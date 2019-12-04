#!/usr/bin/env bash

# set is used to set the environment variables.
#  -e: exit immediately when a command returning a non-zero exit code.
#  -u: treat unset variables as an error.
#  -o pipefail: sets the exit code of a pipeline to that of the rightmost command to exit with a non-zero status,
#       or to zero if all commands of the pipeline exit successfully.
set -euo pipefail

# go mod tidy do not support symlink
cd -P .

GO111MODULE=on go mod tidy
