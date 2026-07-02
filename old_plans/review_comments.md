# Code Review Comments



- Remove mentionts of "submitless execution" from docs and codebase. This used to be an internal name for the feature, but is not the default way of submitting jobs.
- Remove mentions of Slurmfile in documentation and document where slurmfile abstraction is still used in a report called slurmfile_remaining_usage.md . It is an internal, private API now and I also want you to include that in comments for slurmfile. The published docs under docs should not mention it as it will be removed in the near future.
- Ensure only __init__.py files contain __all__ -- do not include those in implementation files
- Ensure all imports are at the top of the file. They should  be within function / class bodies only when they implement some lazy loaded optional feature.
- Do the APIs in src/slurm/context.py need to be public or could they be made private ?  Investigate and ask me what to do based on results.
- Investigate if the integration task related tasks / workflows currenttly placed in under the examples dir in src/slurm/examples/ could be moved outside of the examples into the integration tests or tests dirs. They are not really examples and seem misplaced. Let me know what the effort and obstacles would be and ask me how to proceed.
- Comments, docstrings and docs sometimes reference "fluent API" -- this was an internal name for the functionality while being developed and is no longer relevant. Propose how to proceed with these types of texts and ask for my opinion on how to proceed. Also note that there is integration test task/workflow code under tests/integration/container_test_tasks.py that looks like it might be duplicative.  
- This example in src/slurm/examples/workflow_graph_visualization.py has commented out code - can it be safely re-enabled ? Will the integration tests for it still pass?
- The runner script seems complex : src/slurm/runner.py -- take a deep look at this and propose a simplification / refactor plan and write it to runner_simplification.md .
- Investigate why there are symlinks to files like tests/integration/dev tests/integration/slurm-pyxis-integration and why those are needed. Once you've found out give me options, ask me how to proceed .



