# coding: utf8
from ._base_job_creator import BaseJobCreator

import os


class DAGManJobCreator(BaseJobCreator):
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
    dag_config : dict, optional
        Dictionary with dagman config options as keys and their corresponding
        values. Available options can be found at
        https://research.cs.wisc.edu/htcondor/manual/v7.6/3_3Configuration.html#sec:DAGMan-Config-File-Entries
        No checking of valid keys is done, invalid keys will be ignored by
        dagman or cause an error on job dispatch using the condor CLI.
        All keys are converted to upper case. (default: `{}`)
    """

    def __init__(self, dag_config={}):
        self._dag_config = dag_config.copy()
        return

    def create_job(self, job_exe, job_args, job_name, job_dir,
                   job_setup_pre=None, job_setup_post=None, extra_sub_args=None,
                   bash_exe="/bin/bash", ram="1GB",
                   overwrite=False, verbose=True):
        """
        Create all necessary jobfiles in a single directory to run the complete
        job on HTCcondor scheduler.

        The jobs are handles as follows:

        - Everything is bundled into ``job_dir``
        - The jobfile is held simple, it only defines a ``/bin/bash`` call to
          the corresponding bash script, which is generated by this method.
          This has the advantage, that each job can also be run standalone and
          all documentation rgarding the arguments used are found in the bash
          scripts too.
        - ``job_exe`` is a list of commands that are equal for each generated
          bash script. The primary command that is executed always starts with
          ``job_exe[0] ... job_exe[N]``
        - After the job excecutables, the ``job_args`` dictionary is used to
          provide different arguments to each job's bash script, so that we
          have ``job_exe[0] ... job_exe[N] --key1=arg1[i] ... --keyM=argM[i]``
          for the i'th job bash script.
        - ``jobname`` is used to name the auto-generated job and shell files in
          a consistent way.
        - ``job_setup_pre`` and ``job_setup_post`` can be used to provide shell
          commands that are equal for each jobs and are placed above (``pre``)
          and below (``post``) the actual command line.

        In the end, the job dir includes for N generated jobs:

        - A single ``jobname.dag.config`` file including global configuration
          for the scheduler (args given at constructor)
        - A single ``jobname.dag.jobs`` file listing all jobs and where to find
          the jobfiles for the scheduler
        - A single ``jobname.dag.start.sh`` shell script containing the common
          command to submit the job to the scheduler (on the submit node). This
          does not have to be used, it's just that I always keep forgetting the
          proper command, so it got bundled too.
          It is made executable and passes all further cmds given on script
          invocation to ``condor_submit_dag``, eg.
          ``./jobname.dag.start.sh -max_idle 100``.
        - N ``jobname_suffix.sh`` shell scripts with the actual command, where
          suffix is zero padded from 1 to N.
        - N ``jobname_suffix.job`` job scripts with the scheduler commands,
          where suffix is zero padded from 1 to N.

        The scheduler reads the global config ``jobname.dag.config``, then looks
        up the required jobs from ``jobname.dag.jobs`` and for each job it
        starts, it uses the ``jobname_suffix.job`` file to load the job
        parameters and then each job file simply states to execute the
        corresponding ``jobname_suffix.sh`` script, in which the actual
        computing command is stated.

        Parameters
        ----------
        job_exe : list
            List of commands that are equal for each job and are placed
            consecutively separated by space before the arguments created from
            ``job_args``. For example to call a python script this would be
            ``job_exe = ['/usr/bin/python', 'script_name.py']``.
        job_args : dict
            Dict ``key: arglist`` with arguments in lists for each job, gets
            expanded to: ``--key=arg[i]`` for each argument ``i`` in the
            argument list. Exception is, when ``'__FLAG__'`` is stored in
            ``arg[i]``, then only ``--key`` is expanded (can be used for boolean
            flag arguments in the programm call, eg. like ``--v`` for verbose).
        job_name : string
            Jobname is used to name all the produced files using
            ``jobname_suffix`` where suffix is just a running number.
        job_setup_pre, job_setup_post : list or None
            These args can be used to provide shell commands that are equal for
            each job and are inserted above (``pre``) and below (``post``)
            the actual command line created with ``job_exe``and ``job_args``
            respectively. Each list entry is written on a new line.
            (default: None)
        extra_sub_args : list or None
            List of extra arguments that get put into the submit files. Each
            item gets written without modification on a new line. Note: It is
            not checked, if any argument overwrites one of the submit arguments
            that are set with the other arguments of this method. This can lead
            to double entries in the resulting submit file. (default: None)
        job_dir : string
            Path where the job files get written to.
        bash_exe : str
            Which bash executable to use to execute each job shell script.
            (default: '/bin/bash')
        ram : str or list, optional
            Requested physical RAM size including unit. Supported units are
            `MB', 'GB'`. Can also be a list, then it has to be as long as the
            job argument lists and is set per job. (default: '1GB')
        overwrite : bool, optional
            If ``True`` use ``job_dir`` even if it already exists.
        verbose : bool, optional
            If `True`, print a small summary and a hint on how to start the DAG.
            Else, nothing is printed. (default: `True`)
        """
        (job_exe, job_args, job_setup_pre, job_setup_post, extra_sub_args,
            job_dir, ram, njobs) = self._check_input(
                job_exe, job_args, job_setup_pre, job_setup_post,
                extra_sub_args, job_dir, ram, overwrite, verbose)

        # Create and write the job, submitter and infrastructure files
        _sn_sub = self._write_submit_scripts(
            job_name, job_dir, extra_sub_args, njobs, bash_exe, ram)
        _sn_sh = self._write_job_shell_scripts(
            job_exe, job_name, job_dir, job_args, njobs,
            job_setup_pre, job_setup_post)

        _sn_job = self._write_job_overview(job_name, job_dir, njobs)
        _sn_cfg = self._write_dagman_config(job_name, job_dir)
        _sn_start = self._write_start_script(job_name, job_dir)

        if verbose:
            off = self._get_lead_zeros(njobs) + 1
            print("Generated files:")
            print(off * " " + "1x submit script '{}'".format(_sn_start))
            print(off * " " + "1x job list '{}'".format(_sn_job))
            print("  {}x job submit scripts '{}'".format(njobs, _sn_sub))
            print("  {}x job compute scripts '{}'".format(njobs, _sn_sh))
            print(off * " " + "1x dagman config '{}'".format(_sn_cfg))
            print("Now login to your submit node and run")
            print("  `{}`".format(os.path.join(job_dir, _sn_start)))
            print("You can also run a single `.sh` file directly or submit a "
                  "single job only via `condor_submit -file <name>.sub`")

        return

    def _write_submit_scripts(
            self, job_name, job_dir, extra_sub_args, njobs, bash_exe, ram):
        """
        Write a submit script for each job.
        """
        for i in range(njobs):
            job_i = self._append_id(job_name, i, njobs)
            job_i_fname = os.path.join(job_dir, "{}".format(job_i))

            s = ["processname    = {}".format(job_i)]
            s.append("executable     = {}".format(bash_exe))
            s.append("getenv         = True")

            s.append("output         = {}.out".format(job_i_fname))
            s.append("error          = {}.err".format(job_i_fname))
            s.append("log            = {}.log".format(job_i_fname))

            s.append("request_memory = {}".format(ram[i]))

            s.append("universe       = vanilla")
            s.append("notification   = never")

            if extra_sub_args:
                for extra_arg in extra_sub_args:
                    s.append(extra_arg)

            s.append("arguments      = {}.sh".format(job_i_fname))
            s.append("queue")

            with open(job_i_fname + ".sub", "w") as f:
                f.write("\n".join(s))
        # Return last filename for summary output
        return os.path.basename(
            job_i_fname + ".sub").replace(
                "{:d}".format(njobs - 1), "[0-{:d}]".format(njobs - 1))

    def _write_job_shell_scripts(self, job_exe, job_name, job_dir, job_args,
                                 njobs, job_setup_pre, job_setup_post):
        """
        Write a standalone shell script with all the options per job.
        """
        for i in range(njobs):
            job_i = self._append_id(job_name, i, njobs)
            path_i = os.path.join(job_dir, "{}".format(job_i) + ".sh")

            s = ["echo 'Start: ' `date`"]
            s.append("echo")

            # Write pre commands if any
            if job_setup_pre:
                s.append("# Pre job command")
                s.append("echo '----- Setup commands -----'")
                for line in job_setup_pre:
                    s.append(line)
                s.append("echo")

            # Write actual computing command with different args per job
            s.append("# Actual main job command")
            s.append("echo '----- Script output -----'")
            exe = " ".join(job_exe)
            for key in job_args.keys():
                # __FLAG__ indicates a bool flag that can't be used explicit
                if job_args[key][i] == "__FLAG__":
                    exe += " --{}".format(key)
                else:
                    exe += " --{}={}".format(key, job_args[key][i])
            s.append(exe)
            s.append("echo '----- !Script output -----'")
            s.append("echo")

            # Write post commands if any
            if job_setup_post:
                s.append("# Post job command")
                s.append("echo '----- Post job commands -----'")
                for line in job_setup_post:
                    s.append(line)
                s.append("echo")

            s.append("echo 'Finished: ' `date`")

            with open(path_i, "w") as f:
                f.write("\n".join(s))
        # Return last filename for summary output
        return os.path.basename(path_i).replace(
            "{:d}".format(njobs - 1), "[0-{:d}]".format(njobs - 1))

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
        return os.path.basename(path)  # Return for summary output

    def _write_dagman_config(self, job_name, job_dir):
        """
        Write the 'dagman.config' file, containin steering data for the job
        distribution.
        """
        path = os.path.join(job_dir, job_name + ".dag.config")
        with open(path, "w") as f:
            for cfg_name, val in self._dag_config.items():
                f.write("{}={}\n".format(cfg_name.upper(), val))
        return os.path.basename(path)  # Return for summary output

    def _write_start_script(self, job_name, job_dir):
        """
        Writes a script to execute on the submitter to starts all jobs.
        """
        path = os.path.join(job_dir, job_name + ".dag.start.sh")
        dag_conf = os.path.join(job_dir, job_name + ".dag.config")
        dag_jobs = os.path.join(job_dir, job_name + ".dag.jobs")
        with open(path, "w") as f:
            # Use a "$@" to inject more params manually if needed later
            s = ["condor_submit_dag", "-config", dag_conf, '"$@"', dag_jobs]
            f.write(" ".join(s))
        # Make it user executable (stackoverflow.com/questions/12791997)
        os.chmod(path, 0o755)
        return os.path.basename(path)  # Return for summary output
