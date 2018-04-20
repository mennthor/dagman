# coding: utf8

"""
Submit methods for PBS queuing systems, where only a maximum number of jobs can
be submitted at once. Checks in a regular interval if new jobs can be submitted
until all jobs are queued.
"""

import os
import re
import time
from glob import glob
import subprocess
import getpass


def _get_n_jobs_in_queue(queue=""):
    """ Parse `qstat` output to see running jobs """
    out = subprocess.check_output(["qstat", queue, "-u", getpass.getuser()])
    regex = re.compile("^[0-9]*\.")
    out_jobs = filter(lambda s: re.match(regex, s), out.split('\n'))
    return max(0, len(out_jobs))


def pbs_submitter(path, queue, glob_pat="*.sh", max_jobs=0):
    """
    Queues all jobfiles matching the glob pattern in path until all are queued.

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
        If not ``>0``, all jobs are queued at once. (default: 0)

    Returns
    -------
    failed : list
        List of jobfiles failed to queue.
    """
    jobfiles = sorted(glob(os.path.join(path, glob_pat)))

    if len(jobfiles) == 0:
        raise RuntimeError("No jobfiles foun in given path.")

    print("Start submitting to '{}'".format(queue))
    failed = []
    n_job_files = len(jobfiles)
    n_jobs_in_queue = _get_n_jobs_in_queue(queue)
    for i, jf in enumerate(jobfiles):
        print("Queuing jobfile:\n  {}".format(jf))
        if max_jobs > 0:
            # Wait until slots are free
            while n_jobs_in_queue + 1 > max_jobs:
                print("  - User limit reached (max jobs: " +
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
