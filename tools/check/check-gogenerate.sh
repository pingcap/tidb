#!/usr/bin/env bash
# Copyright 2019 PingCAP, Inc.
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

set -euo pipefail

go generate ./...
set +e
diffline=$(git status -s | awk '{print $2}' | xargs grep '^// Code generated .* DO NOT EDIT\.$' 2>/dev/null)
set -e
if [[ $diffline != "" ]]
then
  echo "Your commit is changed after running go generate ./..., it should not hanppen."
  exit 1
fi
