# dagman

DAGMan job creator wrapper

This creates dagman job files for submission on a dagman controlled cluster and heavily steals from mbrner's implementation (visit his page: [https://github.com/mbrner](https://github.com/mbrner)).
But I tried my luck by rewriting it more for my needs, so it's probably not a very flexible solution.

## Module usage

In the `example` folder we have a simple python script we want to run, called `test.py`, which takes arguments `seed` and `id`.
These are given to the job generator as a dict using `key: arglist`.
We can generate our jobfiles with:

```python
"""This is `example/generator_script.py`"""
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
```

This will create a directory structure as follows:

```
./jobfiles
   |-- testjob.dag.jobs
   |-- testjob_0.sh
   |-- ...
   |-- testjob_9.sh
   |-- testjob.dag.start.sh
   |-- testjob.dag.config
   |-- testjob_0.sub
   |-- ...
   |-- testjob_9.sub
```

On the submitter node then run `bash testjob.dag.start.sh`.
