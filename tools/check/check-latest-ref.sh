#!/usr/bin/env bash
# Copyright 2021 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.
#
# set is used to set the environment variables.
#  -e: exit immediately when a command returning a non-zero exit code.
#  -u: treat unset variables as an error.
#  -o pipefail: sets the exit code of a pipeline to that of the rightmost command to exit with a non-zero status,
#       or to zero if all commands of the pipeline exit successfully.
set -euo pipefail

latest_ref=$(git for-each-ref --merged=HEAD --no-contains=HEAD --sort=-committerdate --count=1)
branch=$(echo "$latest_ref" | cut -d' ' -f2)

if [[ ($branch == *"commit"*) && ($branch == *"remotes"*) && ($branch == *"master"*) ]]; then
  echo "$latest_ref" | cut -d' ' -f1
else
  git show HEAD~50 --pretty=format:"%H" | head -n 1
fi
