#!/usr/bin/env bash
# Copyright 2022 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# set is used to set the environment variables.
#  -e: exit immediately when a command returning a non-zero exit code.
#  -u: treat unset variables as an error.
#  -o pipefail: sets the exit code of a pipeline to that of the rightmost command to exit with a non-zero status,
#       or to zero if all commands of the pipeline exit successfully.
set -euo pipefail

before_checksum=`find . -type f \( -name '*.bazel' -o -name '*.bzl' \) -exec md5sum {} \;| sort -k 2`
make bazel_ci_prepare_rbe
after_checksum=`find . -type f \( -name '*.bazel' -o -name '*.bzl' \) -exec md5sum {} \;| sort -k 2`
if [ "$before_checksum" != "$after_checksum" ]
then
  echo "Please run \`make bazel_prepare\` to update \`.bazel\` files, or just apply the following git diff (run \`git apply -\` and paste following contents):"
  git diff
  echo -e "\n\nChecksum diff:"
  diff <(echo "$before_checksum") <(echo "$after_checksum") || true
  exit 1
fi
