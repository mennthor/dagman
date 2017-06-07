import os


class DAGManJobCreator(object):
    """
    DAGManJobCreator

    Create DAGMan job files that can be directly submitted.

    Parameters
    ----------
    max_jobs_submitted : int, optional
        Maximum number of jobs submitted simultaniously. (default: 10000)
    submits_per_interval : int, optional
        New submits per time interval. (default: 100)
    scan_interval : int, optional
        Interval in which is looked, if new jobs can be started.
    """
    def __init__(self, max_jobs_submitted=1000, submits_per_interval=100,
                 scan_interval=5):
        self.max_jobs_submitted = max_jobs_submitted
        self.submits_per_interval = submits_per_interval
        self.scan_interval = scan_interval
        return

    def create_job(self, name, exe, exe_args, njobs, job_args, outfile,
                   local_job_dir, getenv=True, makedirs=False):
        """
        Parameters
        ----------
        name : string
            Name of the process and of a single job. Jobs get appended with
            '_<id>' where '<id>' is an increasing integer identifier in
            [1, ..., njobs].
        exe : string
            Main command that is run in each job.
        exe_args : string
            Arguments to be executed by 'exe' command, without the specific
            arguments. These are pulled from 'job_options'. The to be executed
            program/srcipt must match to all arguments given in 'job_args'.
        njobs : int
            Number of jobs to run in total.
        job_args : dict, {"argname": arg}
            Arguments can be iterable and given explicitely per job, or single
            string, which then get appended by `_<id>` with <id> in
            [1, ..., njobs]. Argument 'outfile' is specified seperately.
        outfile : string
            Full path to the output file path. Output files are written locally
            on the computing node and copied on finish to reduce network load.
            Every other output is written to dirname(outfile)
        local_job_dir : string
            Full path to where the local job submit files are created.
        getenv : bool, optional
            If True, use the current shell environment on the computing nodes.
            (default: True)
        makedirs : bool, optional
            If True, make all non-existing dirs, specified by 'outfile' and
            'local_job_dir'. (default: False)
        """
        # Also handle $HOME etc. in path
        local_job_dir = os.path.abspath(os.path.expandvars(local_job_dir))
        outfile = os.path.abspath(os.path.expandvars(outfile))
        scratch = os.path.dirname(outfile)

        # Check dirs
        if not os.path.isdir(local_job_dir):
            if makedirs:
                os.makedirs(local_job_dir)
            else:
                raise ValueError("Path given in 'local_job_dir' " +
                                 "does not exist.")
        if not os.path.isdir(scratch):
            if makedirs:
                os.makedirs(scratch)
            else:
                raise ValueError("Path to 'outfile' does not exist.")

        # This will always be created
        logdir = os.path.join(scratch, "log")
        if not os.path.isdir(logdir):
            os.makedirs(logdir)

        # Add special argument outfile, which must match in submitter and
        # argument file. Note:
        # 1. The full path is used to get the local scratch dir from it, this is
        #    where all logs and the transfer_output_files are being written to.
        # 2. The basename is used as the argument for the script, which shall
        #    write the outfile to the submitter local directory.
        if "outfile" in job_args.keys():
            raise KeyError("'job_args' must not have special key 'outfile'.")
        job_args["outfile"] = os.path.basename(outfile)

        # Expand the exe_args to match the keys in job_args exactly
        for key in job_args.keys():
            exe_args += " --{}=$({})".format(key, key)

        # Write submit file, dagman options and argument list file
        self._write_one_job_submit(name, exe, exe_args, outfile, local_job_dir,
                                   getenv)
        self._write_job_options(name, njobs, job_args, local_job_dir)
        self._write_dagman_config(local_job_dir)

        # Write a start script: dagman read config and options and gets submit
        # script path from options file.
        startup_f = os.path.join(local_job_dir, "submit_jobs.sh")
        _, job_options_f, dagman_conf_f = self._get_file_paths(local_job_dir)
        with open(startup_f, "w") as sf:
            s = ["condor_submit_dag",
                 "-config",
                 dagman_conf_f,
                 "-notification Complete",
                 job_options_f,
                 ]

            sf.write(" ".join(s))

    def _write_one_job_submit(self, name, exe, exe_args, outfile,
                              local_job_dir, getenv):
        """
        Writes the 'onejob.submit' file which tells dagman how to start a
        single job to 'local_job_dir'.

        For each job dagman runs::

          $ exe exe_args

        Parameters
        ----------
        name : string (See `create_job`)
        exe : string (See `create_job`)
        exe_args : string (See `create_job`)
        outfile : string (See `create_job`)
        local_job_dir : string (See `create_job`)
        getenv : bool (See `create_job`)
        """
        # Set files relativ to local job dir
        oj_submit_f, _, _ = self._get_file_paths(local_job_dir)

        # Scratch dir where results are written to
        scratch = os.path.dirname(outfile)

        # Create submit file
        out = []
        out.append("processname = {}".format(name))
        out.append("executable  = {}".format(exe))

        if getenv:
            getenv = "True"
        else:
            getenv = "False"
        out.append("getenv = {}".format(getenv))
        out.append("")

        logf = os.path.join(scratch, "log/$(processname)_$(Cluster)")
        out.append("output = {}".format(logf + ".out"))
        out.append("error = {}".format(logf + ".err"))
        out.append("log = {}".format(logf + ".log"))
        out.append("")

        out.append("notification = never")
        out.append("universe = vanilla")
        out.append("")

        # out.append("request_cpus = {}".format())  # <num_cpus>
        # out.append("request_memory = {}".format())  # <mem_in_MB>
        # out.append("request_disk = {}".format())  # <disk_in_KB>
        # out.append("")

        out.append("should_transfer_files = {}".format("YES"))
        out.append("when_to_transfer_output = {}".format("ON_EXIT"))
        out.append("")

        # out.append("transfer_input_files={}".format())
        outfile = os.path.join(scratch, "$(outfile)")
        out.append("transfer_output_files = {}".format(outfile))
        out.append("")

        out.append("Arguments = {}".format(exe_args))
        out.append("queue")

        with open(oj_submit_f, "w") as oj_sub_f:
            for line in out:
                oj_sub_f.write(line + "\n")

    def _write_job_options(self, name, njobs, job_args, local_job_dir):
        """
        Writes the 'jobs.options' file which holds the options for each job to
        'local_job_dir' plugged in by dagman for each job.

        Parameters
        ----------
        name : string (See `create_job`)
        njobs : int (See `create_job`)
        job_args : dict, {"argname": arg} (See `create_job`)
        local_job_dir : string (See `create_job`)
        """
        # Set files relativ to local job dir
        oj_submit_f, job_options_f, _ = self._get_file_paths(local_job_dir)

        # Write dagman job argument file
        with open(job_options_f, "w") as job_opts_f:
            for i in range(njobs):
                # Jobname is just for internal use and always gets iterated
                _jobname = name + "_{}".format(i)

                _vars = ""
                for key, val in job_args.items():
                    if njobs == 1:
                        _vars += " {}=\"{}\"".format(key, val)
                    # Otherwise: args must be a single string or an iterable
                    elif type(val) is str:
                        # Single string arguments get appended by the job id
                        n, ext = os.path.splitext(val)
                        val = n.rstrip("/") + "_{}".format(i) + ext
                        _vars += " {}=\"{}\"".format(key, val)
                    elif hasattr(val, "__iter__"):
                        # Object is not string but iterable (given per job),
                        # plug it in the current value
                        _vars += " {}=\"{}\"".format(key, val[i])
                    else:
                        err = ("Job argument '{}' holds a non-".format(key) +
                               "iterable object, that is not of type 'str'. " +
                               "Provide an iterable with one argument per job" +
                               ", or a single string argument.")
                        raise ValueError(err)

                # JOB <jobname> <abs/path/to/OneJob.submit>
                JOB = "JOB {} {}\n".format(_jobname, oj_submit_f)
                # VARS <jobname> arg0_name="arg0" ... argn_name="argn"
                VARS = "VARS {}".format(_jobname) + _vars + "\n"

                job_opts_f.write(JOB)
                job_opts_f.write(VARS)

    def _write_dagman_config(self, local_job_dir):
        """
        Write the 'dagman.config' file, containin steering data for the job
        distribution.

        Parameters
        ----------
            local_job_dir : string (See `create_job`)
        """
        _, _, dagman_conf_f = self._get_file_paths(local_job_dir)

        with open(dagman_conf_f, "w") as dagconf:
            dagconf.write("DAGMAN_MAX_JOBS_SUBMITTED={}\n".format(
                self.max_jobs_submitted))
            dagconf.write("DAGMAN_MAX_SUBMIT_PER_INTERVAL={}\n".format(
                self.submits_per_interval))
            dagconf.write("DAGMAN_USER_LOG_SCAN_INTERVAL={}\n".format(
                self.scan_interval))

    def _get_file_paths(self, local_job_dir):
        """
        Simple DNRY wrapper to get the file paths of the jobdir.

        Parameters
        ----------
            local_job_dir : string (See `create_job`)

        Returns
        -------
        oj_submit_f : string
            Full path to the 'onejob.submit' file in 'local_d'.
        job_options_f : string
            Full path to the 'jobs.options' file in 'local_d'.
        dagman_conf_f : string
            Full path to the 'dagman.config' file in 'local_d'.
        """
        oj_submit_f = os.path.join(local_job_dir, "onejob.submit")
        job_options_f = os.path.join(local_job_dir, "job.options")
        dagman_conf_f = os.path.join(local_job_dir, "dagman.config")
        return oj_submit_f, job_options_f, dagman_conf_f
