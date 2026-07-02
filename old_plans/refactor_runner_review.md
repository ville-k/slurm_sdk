


src/slurm/_runner_impl.py:

The argument order for update_job_metadata is incorrect. According to result_saver.py lines 109-112, the function signature is update_job_metadata(output_file, job_id, timestamp), but here job_id may be None (from get_job_id_from_env() returning Optional[str]), which would cause issues when used as a dictionary key in the metadata JSON at line 151 of result_saver.py. The call should handle the case where job_id is None, or ensure job_id is never None before calling this function.






src/slurm/runner/__init__.py

There is this comment -- we should not need backward compatibility. Lets remove this backwards compat and make sure everything works with the new names.
 # Legacy underscore-prefixed names (backwards compatibility)
