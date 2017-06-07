# dagman
DAGMan job creator wrapper

This creates dagman job files for submission on a dagman controlled cluster.
This heavily steals from mbrner's implementation [https://github.com/mbrner](https://github.com/mbrner).
But I tried my luck by rewriting it more for my needs, so it's probably not a very flexible solution.

## Fast dagman intro

You need a script/programm you want to run on the cluster with multiple jobs, let's call it 'test.py'.

The dagman system needs three files to run stuff, namely

1. An options file, here 'dagman.options'.
2. A submitter script, which tells dagman how to submit a single job, here 'onejob.config'. In each single job, dagman runs 'test.py'.
3. An argument list file, here 'job.options', telling dagman, which arguments to put in the single run per job, as specified in 'onejob.config'.

Then we can tell the submitter to run the jobs with

```bash
condor_submit_dag -config /path/to/dagman.config -notification Complete /path/to/job.options
```

wihch get's stored in a bash file 'submit.sh' here.

## Module usage

Here we have a simple python script we want to run, called 'test.py', which takes arguments 'seed', 'id' and 'outfile' (sepcial, see below).
Also is must support a special argument outfile (despite it is declared as None n the arguments).
We come to that later.

The first two argument are just given as a dictionary.
Arguments that are different (eg. a random seed) must be given as iterables, single arguments must be string and get appended by the job index automatically.

There is one special argument called 'outfile', which specifies the path to an output file.
This file is special, because it is written directly on the computing node and is copied to it's destination (specified by dirname(outfile)) after a job has fininshed to reduce network write.

We can then simply generate our jobfiles with:

```python
# This is `generator_script.py`
from dagman import dagman

job_creator = dagman.DAGManJobCreator()
job_creator.create_job(
    name="Testjob",
    exe="python",
    exe_args="test.py",
    njobs=10,
    job_args={"seed": 10 + list(range(njobs)), "id": "run"},
    outfile="./results/outfile.txt",
    local_job_dir="./jobfiles",
    makedirs=True)
```

This will create a directory structure as follows:

```
./
 |-- generator_script.py
 |-- results/
 |    |-- log 
 |-- jobfiles/
      |-- dagman.config
      |-- onejob.submit
      |-- job.options
      |-- submit.sh
```

Then put the 'test.py' script (not shown here) in 'jobfiles' and move that to the submitter.
There you can simply run the code in 'submit.sh'.
