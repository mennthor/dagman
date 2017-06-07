from dagman import dagman

job_creator = dagman.DAGManJobCreator()


name = "Testjob"
exe = "python"
exe_args = "test.py"
njobs = 10
job_args = {"seed": list(range(njobs)), "id": "run"}
outfile = "./results/outfile.txt"
local_job_dir = "./jobfiles"

job_creator.create_job(
    name=name,
    exe=exe,
    exe_args=exe_args,
    njobs=njobs,
    job_args=job_args,
    outfile=outfile,
    local_job_dir=local_job_dir,
    makedirs=True)
