# coding: utf8
import os
import math
import abc


class BaseJobCreator(object):
    """
    BaseJobCreator

    Base class providing some shared code for DAGMan and PBS job creators.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def create_job(self):
        pass

    def _check_input(self, job_exe, job_args,
                     job_setup_pre, job_setup_post, extra_sub_args,
                     job_dir, overwrite):
        """
        Some inoput checks and ensuring we can work with consisten arguments.
        """
        job_dir = os.path.abspath(os.path.expandvars(job_dir))

        # We want to work with list consistently
        if not isinstance(job_exe, list):
            job_exe = [job_exe]
        if not isinstance(job_setup_pre, list):
            job_setup_pre = [job_setup_pre] if job_setup_pre is not None else []
        if not isinstance(job_setup_post, list):
            job_setup_post = [
                job_setup_post] if job_setup_post is not None else []
        if not isinstance(extra_sub_args, list):
            extra_sub_args = [
                extra_sub_args] if extra_sub_args is not None else []

        # All job args must be same length lists
        keys = list(job_args.keys())
        njobs = len(job_args[keys[0]])
        if len(keys) > 1:
            for key in keys[1:]:
                if len(job_args[key]) != njobs:
                    raise ValueError(
                        "Arg list for '{}'".format(key)
                        + " has not the same length as for '{}'.".format(keys))

        # Check jobdir
        if not self._check_and_makedir(job_dir, overwrite):
            raise ValueError("Dir '{}' ".format(job_dir)
                             + "already exists and `overwrite` is False.")

        return (job_exe, job_args, job_setup_pre, job_setup_post,
                extra_sub_args, job_dir, njobs)

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
    max_jobs_submitted : int, optional
        Maximum number of jobs submitted simultaniously. (default: 10000)
    submits_per_interval : int, optional
        New submits per time interval. (default: 100)
    scan_interval : int, optional
        Interval in seconds in which the submitter checks if new jobs can be
        started. (default: 5)
    mem : int, optional
        Requested RAM in GB. (default: 1)
    """
    def __init__(self, max_jobs_submitted=1000, submits_per_interval=100,
                 scan_interval=5, mem=1):
        self._max_jobs_submitted = max_jobs_submitted
        self._submits_per_interval = submits_per_interval
        self._scan_interval = scan_interval
        self._mem = int(mem)
        return

    def create_job(self, job_exe, job_args, job_name, job_dir,
                   job_setup_pre=None, job_setup_post=None, extra_sub_args=None,
                   bash_exe="/bin/bash", overwrite=False):
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
        overwrite : bool, optional
            If ``True`` use ``job_dir`` even if it already exists.
        """
        (job_exe, job_args, job_setup_pre, job_setup_post, extra_sub_args,
            job_dir, njobs) = self._check_input(
                job_exe, job_args, job_setup_pre, job_setup_post,
                extra_sub_args, job_dir, overwrite)

        # Create and write the job, submitter and infrastructure files
        self._write_submit_scripts(
            job_name, job_dir, extra_sub_args, njobs, bash_exe)
        self._write_job_shell_scripts(job_exe, job_name, job_dir, job_args,
                                      njobs, job_setup_pre, job_setup_post)

        self._write_job_overview(job_name, job_dir, njobs)
        self._write_dagman_config(job_name, job_dir)
        self._write_start_script(job_name, job_dir)
        return

    def _write_submit_scripts(
            self, job_name, job_dir, extra_sub_args, njobs, bash_exe):
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

            s.append("request_memory = {:d}gb".format(self._mem))

            s.append("universe       = vanilla")
            s.append("notification   = never")

            if extra_sub_args:
                for extra_arg in extra_sub_args:
                    s.append(extra_arg)

            s.append("arguments      = {}.sh".format(job_i_fname))
            s.append("queue")

            with open(job_i_fname + ".sub", "w") as f:
                f.write("\n".join(s))
        return

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
                self._max_jobs_submitted))
            f.write("DAGMAN_MAX_SUBMIT_PER_INTERVAL={}\n".format(
                self._submits_per_interval))
            f.write("DAGMAN_USER_LOG_SCAN_INTERVAL={}\n".format(
                self._scan_interval))
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


class PBSJobCreator(BaseJobCreator):
    """
    PBSJobCreator <<< DAGManJobCreator

    Create PBS job files that can be directly submitted:

    - One shell script per job with all job options with PBS command header.
    - Single python script that submits the jobs.

    Parameters
    ----------
    max_jobs_submit : int, optional
        Maximum number of jobs to queue at the same time for the current user.
        If not ``>0``, all jobs are queued at once. (default: 0)
    vmem : string, optional
        Virtual memory to request. (default: ``'2GB'``)
    nodes, cores : int, optional
        Requested nodes and cores. (default: 1)
    """
    def __init__(self, max_jobs_submitted=0, vmem="2GB", nodes=1, cores=1):
        self._max_jobs = max_jobs_submitted
        self._nodes = nodes
        self._cores = cores
        self._vmem = vmem
        return

    def create_job(self, script, job_args, job_name, job_dir, queue,
                   exe="/bin/bash/", overwrite=False):
        """
        Parameters
        ----------
        script : string
            Path to the python script that gets ecxcuted in each job.
        job_args : dict
            Dict ``key: arglist`` with arguments in lists for each job, gets
            expanded to: ``--key=arg[i]`` for each argument ``i`` in the
            argument list. Exception is, when ``'__FLAG__'`` is stored in
            ``arg[i]``, then only ``--key`` is expanded.
        job_name : string
            Jobname.
        job_dir : string
            Path where the job files get written to.
        queue : dict
            Must have keys:

            - 'name', string: Name of the queue to use. Available queues can be
              shown using shell command ``qstat -q``.
            - 'walltime', string: Maximum time that the job is allow to run.
              Format: ``'H:MM:SS'``, eg. ``'8:00:00'`` for 8 hours.
        exe : string, optional
            Path to the bash used to excute the script. (default: '/bin/bash')
        overwrite : bool, optional
            If ``True`` use ``job_dir`` even if it already exists.
        """
        _out = self._check_input(script, job_dir, job_args, overwrite)
        script, job_dir, job_args, njobs = _out

        exe = os.path.abspath(exe)

        for k in ["name", "walltime"]:
            if k not in queue.keys():
                raise ValueError("`queue` missing key {}.".format(k))

        # Create and write the job and submit files
        self._write_job_shell_scripts(script, job_name, job_dir, job_args,
                                      njobs, exe, queue)
        self._write_start_script(job_name, job_dir, queue["name"])
        return

    def _write_job_shell_scripts(self, script, job_name, job_dir,
                                 job_args, njobs, exe, queue):
        """
        Write a standalone shell script with all the options per job.
        """
        for i in range(njobs):
            job_i = self._append_id(job_name, i, njobs)
            path = os.path.join(job_dir, "{}".format(job_i))

            # PBS steering information
            s = ["#PBS -N {}".format(job_i)]  # jobname
            s.append("#PBS -M thorben.menne@tu-dortmund.de")
            s.append("#PBS -m abe")  # mail on (a)bortion, (b)egin, (e)nd, (n)o
            s.append("#PBS -o {}.out".format(path))  # Log out
            s.append("#PBS -e {}.err".format(path))  # Error out
            s.append("#PBS -q {}".format(queue["name"]))
            s.append("#PBS -l walltime={}".format(queue["walltime"]))
            # Nodes and cores per node and memory per job
            s.append("#PBS -l nodes={}:ppn={}".format(self._nodes, self._cores))
            s.append("#PBS -l vmem={}".format(self._vmem))
            s.append("#PBS -V")                # Pass environment variables
            s.append("")
            s.append("echo 'Start: ' `date`")
            s.append("echo '----- Script output -----'")
            s.append("echo")
            # Write actual command
            exe = "python {}".format(script)

            for key in job_args.keys():
                # __FLAG__ indicates a bool flag that can't be used explicit
                if job_args[key][i] == "__FLAG__":
                    exe += " --{}".format(key)
                else:
                    exe += " --{}={}".format(key, job_args[key][i])
            s.append(exe)

            s.append("echo")
            s.append("echo '----- !Script output -----'")
            s.append("echo 'Finished: ' `date`")

            with open(path + ".sh", "w") as f:
                f.write("\n".join(s))
        return

    def _write_start_script(self, job_name, job_dir, queue):
        """
        Writes a script to execute on the submitter to starts all jobs.
        """
        path = os.path.join(job_dir, "submit.py")
        s = ['"""']
        s.append('PBS submitter script for')
        s.append('  {}'.format(job_name))
        s.append('"""')
        s.append('')
        s.append('from dagman import pbs_submitter')
        s.append('')
        s.append('pbs_submitter('
                 'path="{}", queue="{}", '.format(job_dir, queue)
                 + 'glob_pat="*.sh", max_jobs={})'.format(self._max_jobs))
        s.append('')
        with open(path, "w") as f:
            f.write('\n'.join(s))
        return
