# coding: utf8
from ._base_job_creator import BaseJobCreator

import os
import sys
import re
import time
from glob import glob
import subprocess
import getpass
import argparse


def _get_n_jobs_in_queue(queue=""):
    """ Parse `qstat` output to see running jobs """
    out = subprocess.check_output(["qstat", queue, "-u", getpass.getuser()])
    regex = re.compile(r"^[0-9]*\.")
    out_jobs = filter(lambda s: re.match(regex, s), out.split('\n'))
    return max(0, len(out_jobs))


def _pbs_submitter_entrypoint():
    """ CLI entrypoint for ``pbs_submitter``. """
    parser = argparse.ArgumentParser(
        description="""
        Utility submit method for PBS queuing systems, where only a maximum number
        of jobs can be submitted at once.

        This method checks in a regular interval if new jobs can be submitted until
        all jobs are queued, where job files are obtained using the provided glob
        pattern (usually this is run in the same directory as the generated job
        files so the default matches correctly).
        """)
    parser.add_argument("queue", help="The desired queue to submit to. "
                        "See available queues with `qstat -Q`")
    parser.add_argument(
        "-p", "--path", help="Path to the jobfile (PBS shell scripts. Under "
        "this path, all `<path>/*.sh` scripts found are put into the queue."
        " Default: './'", default="./")
    parser.add_argument(
        "-m", "--max_jobs", help="Maximum number of jobs to queue at the same "
        "time for the current user. If 0, all jobs are queued at once. "
        "Default: 0", default=0, type=int)
    args = parser.parse_args()

    path = os.path.abspath(os.path.expandvars(os.path.expanduser(args.path)))
    try:
        failed = pbs_submitter(path=path, queue=args.queue,
                               glob_pat="*.sh", max_jobs=args.max_jobs)
    except RuntimeError as err:
        sys.exit("{} Exiting without doing anything.".format(err))

    # Print list of failed queue attempts if any
    if failed:
        lz = BaseJobCreator._get_lead_zeros(len(failed))
        for i, f in enumerate(failed):
            print(" {:{}d} {}".format(lz, i, f))


def pbs_submitter(path, queue, glob_pat="*.sh", max_jobs=0):
    """
    Utility submit method for PBS queuing systems, where only a maximum number
    of jobs can be submitted at once.

    This method checks in a regular interval if new jobs can be submitted until
    all jobs are queued, where job files are obtained using the provided glob
    pattern undern the given path.

    Parameters
    ----------
    path : str
        Absolute path to the jobfiles.
    queue : str
        Which queue we are working in. See available queues with ``qstat -Q``.
    glob_pat : str, optional
        Pattern to match the jobfiles in ``path``. (default: ``'*.sh'``)
    max_jobs : int, optional
        Maximum number of jobs to queue at the same time for the current user.
        If ``0``, all jobs are queued at once. (default: 0)

    Returns
    -------
    failed : list
        List of jobfiles failed to queue.
    """
    jobfiles = sorted(glob(os.path.join(path, glob_pat)))

    if len(jobfiles) == 0:
        raise RuntimeError("No jobfiles found in given path.")

    print("Start submitting to '{}'".format(queue))
    failed = []
    n_job_files = len(jobfiles)
    n_jobs_in_queue = _get_n_jobs_in_queue(queue)
    for i, jf in enumerate(jobfiles):
        print("Queuing jobfile:\n  {}".format(jf))
        if max_jobs > 0:
            # Wait until slots are free
            while n_jobs_in_queue + 1 > max_jobs:
                print("  - User limit reached (max jobs: "
                      "{}). Trying again in 1 min.".format(max_jobs))
                time.sleep(60)
                n_jobs_in_queue = _get_n_jobs_in_queue(queue)
        # Queue the job
        ret = subprocess.call(["qsub", jf])
        if ret == 0:
            print("  - Job {} / {} queued!".format(i, n_job_files))
            n_jobs_in_queue += 1
        else:
            failed.append(jf)
            print("  - FAILED to queue the jobs, SKIPPING!")

    print("All queued. Failed {} / {}".format(len(failed), n_job_files))
    return failed


class PBSJobCreator(BaseJobCreator):
    """
    NOTE: This has not evolved as the DAGMan part has so it is more basic.
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
