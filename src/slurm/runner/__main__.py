"""Entry point for running the slurm runner as a module.

This allows `python -m slurm.runner` to work the same as before.
"""

from slurm.runner.main import main

if __name__ == "__main__":
    main()
