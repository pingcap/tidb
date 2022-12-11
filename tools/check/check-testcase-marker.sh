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

set -euo pipefail

go run tests/testmarker/walker.go > tests/testmarker/testdata/markdata.yaml
set +e
diffline=$(git status -s tests/testmarker/testdata/markdata.yaml 2>/dev/null)
set -e
if [[ $diffline != "" ]]
then
  echo "Your commit is changed after running go run tests/testmarker/walker.go, it should not happen."
  exit 1
fi
