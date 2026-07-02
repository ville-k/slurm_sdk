# Slurm command and it’s sub-commands:

## Requirements:
- Provide a “slurm” command that gets installed with the sdk. This will provide a CLI interface to SDK functionality and useful functionality/subcommands for working with slurm clusters.
- Use cyclops for CLI arguments / handling:  https://cyclopts.readthedocs.io/en/stable/
- Use rich for formatting output and interactive CLI questions

## Basic Commands / Sub Commands - Basic Command Support:

The slurm command should have the following commands. We will keep adding more commands and sub-commands later so make this convenient to expand.

- jobs [subcommand - default: list]
    - list
    - show [job-id | job-name] 
- cluster  [subcommand - default: list]
    - list — lists all configured clusters
    - show [cluster name]