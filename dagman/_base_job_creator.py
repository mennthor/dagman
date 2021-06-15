# coding: utf8
import os
import math
import re
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
                     job_dir, ram, overwrite, verbose):
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
        if not self._check_and_makedir(job_dir, overwrite, verbose):
            raise ValueError("Dir '{}' ".format(job_dir)
                             + "already exists and `overwrite` is False.")

        # Check requested RAM format, must fullfill format: start with n digits,
        # then one of 'MB' or 'GB' (after all whitespace removed), then EOL,
        # extra characters raise an error.
        if not isinstance(ram, list):
            ram = [ram]
        else:
            if len(ram) != njobs:
                raise ValueError("`ram` is given as a list but does not have "
                                 "the same length as the job arguments.")
        valid = r"^\d{1,}(?:MB|GB)$"  # 'ram' must fulfill this regex pattern
        for i, ram_i in enumerate(ram):
            # Strip start/end/between whitespaces
            ram_i = ram_i.upper().strip().replace(" ", "")
            match = re.findall(valid, ram_i)
            if len(match) != 1:
                raise ValueError("Given `ram` argument '{}' invalid. Must be "
                                 "of format 'number[MB|GB]'.".format(ram_i))
            else:
                ram[i] = match[0]
        if len(ram) == 1:  # Just to have a consistent arg for each job
            ram = njobs * [ram[0]]

        return (job_exe, job_args, job_setup_pre, job_setup_post,
                extra_sub_args, job_dir, ram, njobs)

    @staticmethod
    def _get_lead_zeros(n):
        """
        Get the number of leading zeros required to cover the range up to ``n``.

        Parameters
        ----------
        n : int
            The maximum occuring number, must be positive.

        Returns
        -------
        leading_zeros : int
            Number of leading zeros required for the range [0, n].
        """
        return int(math.ceil(math.log10(n)))

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
        lead_zeros = self._get_lead_zeros(njobs)
        return "{0:}_{2:0{1:d}d}".format(jobname, lead_zeros, i)

    def _check_and_makedir(self, dirname, overwrite, verbose):
        """
        Check if ``dirname`` exists and create if it does not and overwrite is
        ``True``.

        Parameters
        ----------
        dirname : string
            Path to the directory.
        overwrite : bool, optional
            If ``True`` use ``dirname`` even if it already exists.
        verbose : bool, optional
            If `True`, print a small summary and a hint on how to start the DAG.
            Else, nothing is printed. (default: `True`)

        Returns
        -------
        was_created : bool
            ``True`` if directory didn't exist and was created, or directory
            existed but ``overwrite`` is ``True``. Otherwise ``False``.
        """
        dirname = os.path.abspath(dirname)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
            if verbose:
                print("Created and using dir '{}'.".format(dirname))
            return True
        elif overwrite:
            if verbose:
                print("Using existing dir '{}'.".format(dirname))
            return True
        else:
            return False
