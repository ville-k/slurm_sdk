# Container Plan

## Requirements

- one docker compose setup used for integration tests and devcontainer. unit tests are always launched inside the development host (host or dev container) while integration tests get always launched inside a container
- devcontainer that runs the main development container with uv and other python tooling required for development of the slurm SDK.
- docker compose manager slurm cluster is available for the developer to use when running inside a devcontainer -- it should be all wired up to look like the developer has access to a full private slurm cluster and container registry
- integration tests are submitted from the containerized host used as the devcontainer when development is done on the host machine (I will later use also for CI). when development is happening inside the devcontainer, the integration tests need to run inside the devcontainer just like the unit tests.
