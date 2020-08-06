# coding: utf-8

import os

from dagman import dagman


job_creator = dagman.DAGManJobCreator()

job_name = "testjob"  # Used to name generated files
job_dir = "./jobfiles"  # Where to save all generated files
job_exe = ["python", os.path.abspath("./test.py")]  # Main executable
job_setup_pre = ["echo 'Hi there'"]
job_setup_post = ["echo 'Done'", "echo 'with computing'"]
# Arguments for each job
njobs = 10
job_args = {"seed": list(range(100, 100 + njobs)),
            "id": ["run_{:d}".format(i) for i in range(njobs)]}

job_creator.create_job(
    job_exe=job_exe,
    job_args=job_args,
    job_name=job_name,
    job_dir=job_dir,
    job_setup_pre=job_setup_pre,
    job_setup_post=job_setup_post,
    bash_exe="/bin/bash",
    overwrite=False,
    )
