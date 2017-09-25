from dagman import dagman

job_creator = dagman.DAGManJobCreator()

job_name = "testjob"
job_dir = "./jobfiles"
script = "./test.py"
njobs = 10
job_args = {"seed": list(range(100, 100 + njobs)),
            "id": ["run_{:d}".format(i) for i in range(njobs)]}

job_creator.create_job(script=script, job_args=job_args,
                       job_name=job_name, job_dir=job_dir, overwrite=True)
