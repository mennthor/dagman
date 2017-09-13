# coding: utf8
import os
import math


class DAGManJobCreator(object):
    """
    DAGManJobCreator

    Create DAGMan job files that can be directly submitted:

    - One shell script per job with all job options.
    - One submit file per shell script submitting a single job.
    - Single job options file connecting jobs and submit scripts.
    - Single dagman config file.
    - Single shell script to run at submitter that submits the jobs.

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

    def create_job(self, script, job_args, job_name, job_dir, overwrite=False):
        """
        Parameters
        ----------
        script : string
            Path to the python script that gets ecxcuted in each job.
        job_args : dict
            Dict ``key: arglist`` with arguments in lists for each job, gets
            expanded to: ``--key=arg[i]`` for each argument ``i`` in the
            argument list.
        job_name : string
            Jobname.
        job_dir : string
            Path where the job files get written to.
        overwrite : bool, optional
            If ``True`` use ``job_dir`` even if it already exists.
        """
        script = os.path.abspath(os.path.expandvars(script))
        job_dir = os.path.abspath(os.path.expandvars(job_dir))

        # All job args must be same length lists
        keys = job_args.keys()
        njobs = len(job_args[keys[0]])
        if len(keys) > 1:
            for key in keys[1:]:
                if len(job_args[key]) != njobs:
                    raise ValueError(
                        "Arg list for '{}'".format(key) +
                        " has not the same length as for '{}'.".format(keys))

        # Check jobdir
        if not self._check_and_makedir(job_dir, overwrite):
            raise ValueError("Dir '{}' ".format(job_dir) +
                             "already exists and `overwrite` is False.")

        # Create and write the job, submitter and infrastructure files
        self._write_submit_scripts(job_name, job_dir, njobs)
        self._write_job_shell_scripts(script, job_name, job_dir, job_args,
                                      njobs)

        self._write_job_overview(job_name, job_dir, njobs)
        self._write_dagman_config(job_name, job_dir)
        self._write_start_script(job_name, job_dir)
        return

    def _write_submit_scripts(self, job_name, job_dir, njobs):
        """
        Write a submit script for each job.
        """
        for i in range(njobs):
            job_i = self._append_id(job_name, i, njobs)
            path = os.path.join(job_dir, "{}".format(job_i))

            s = ["processname  = {}".format(job_i)]
            s.append("executable   = /bin/bash")
            s.append("getenv       = True")

            s.append("output       = {}.out".format(path))
            s.append("error        = {}.err".format(path))
            s.append("log          = {}.log".format(path))

            s.append("universe     = vanilla")
            s.append("notification = never")

            s.append("arguments    = {}.sh".format(path))
            s.append("queue")

            with open(path + ".sub", "w") as f:
                f.write("\n".join(s))
        return

    def _write_job_shell_scripts(self, script, job_name, job_dir,
                                 job_args, njobs):
        """
        Write a standalone shell script with all the options per job.
        """
        for i in range(njobs):
            job_i = self._append_id(job_name, i, njobs)
            path_i = os.path.join(job_dir, "{}".format(job_i) + ".sh")

            s = ["echo 'Start: ' `date`"]
            s.append("echo '----- Script output -----'")
            s.append("echo")
            # Write actual command
            exe = "python {}".format(script)
            for key in job_args.keys():
                exe += " --{}={}".format(key, job_args[key][i])
            s.append(exe)

            s.append("echo")
            s.append("echo '----- !Script output -----'")
            s.append("echo 'Finished: ' `date`")

            with open(path_i, "w") as f:
                f.write("\n".join(s))
        return

    def _write_job_overview(self, job_name, job_dir, njobs):
        """
        Write list that includes all submit files and jobnames.
        """
        s = []
        for i in range(njobs):
            job_i = self._append_id(job_name, i, njobs)
            path_i = os.path.join(job_dir, "{}".format(job_i) + ".sub")
            s.append("JOB {} {}".format(job_i, path_i))

        path = os.path.join(job_dir, job_name + ".dag.jobs")
        with open(path, "w") as f:
            f.write("\n".join(s))
        return

    def _write_dagman_config(self, job_name, job_dir):
        """
        Write the 'dagman.config' file, containin steering data for the job
        distribution.
        """
        path = os.path.join(job_dir, job_name + ".dag.config")
        with open(path, "w") as f:
            f.write("DAGMAN_MAX_JOBS_SUBMITTED={}\n".format(
                self.max_jobs_submitted))
            f.write("DAGMAN_MAX_SUBMIT_PER_INTERVAL={}\n".format(
                self.submits_per_interval))
            f.write("DAGMAN_USER_LOG_SCAN_INTERVAL={}\n".format(
                self.scan_interval))
        return

    def _write_start_script(self, job_name, job_dir):
        """
        Writes a script to execute on the submitter to starts all jobs.
        """
        path = os.path.join(job_dir, job_name + ".dag.start.sh")
        dag_conf = os.path.join(job_dir, job_name + ".dag.config")
        dag_jobs = os.path.join(job_dir, job_name + ".dag.jobs")
        with open(path, "w") as f:
            s = ["condor_submit_dag", "-config", dag_conf,
                 "-notification Complete", dag_jobs]
            f.write(" ".join(s))
        return

    def _append_id(self, jobname, i, njobs):
        """
        Append a running job ID string with prepended zeros to ``job_name``.

        Parameters
        ----------
        job_name : str
            Name of the job, getting appended with a job ID.
        i : int
            Current job to get a job ID for.
        njobs : int
            Total number of jobs that get processed.

        Returns
        -------
        jobid : string
            Job ID string in format ``<jobname>_0...00<i>``.
        """
        lead_zeros = int(math.ceil(math.log10(njobs)))
        return "{0:}_{2:0{1:d}d}".format(jobname, lead_zeros, i)

    def _check_and_makedir(self, dirname, overwrite):
        """
        Check if ``dirname`` exists and create if it does not and overwrite is
        ``True``.

        Parameters
        ----------
        dirname : string
            Path to the directory.
        overwrite : bool, optional
            If ``True`` use ``dirname`` even if it already exists.

        Returns
        -------
        was_created : bool
            ``True`` if directory didn't exist and was created, or directory
            existed but ``overwrite`` is ``True``. Otherwise ``False``.
        """
        dirname = os.path.abspath(dirname)
        if not os.path.exists(dirname):
                os.makedirs(dirname)
                print("Created and using dir '{}'".format(dirname))
                return True
        elif overwrite:
            print("Using dir '{}'".format(dirname))
            return True
        else:
            return False
