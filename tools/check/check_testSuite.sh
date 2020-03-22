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

exitCode=0

list=$(find . -name "*_test.go" -not -path "./vendor/*" -print0 | xargs -0 grep -E "type test(.*)Suite"  | awk -F ':| ' '{print $1" "$3}')
while read -r file testSuite; do
  # TODO: ugly regex
  # TODO: check code comment
  dir=$(dirname "$file")
  if ! find "$dir" -name "*_test.go" -print0 | xargs -0 grep -E "_ = (check\.)?(Suite|SerialSuites)\((&?${testSuite}{|new\(${testSuite}\))" > /dev/null
  then
    if find "$dir" -name "*_test.go" -print0 | xargs -0 grep -E "func \((.* )?\*?${testSuite}\) Test" > /dev/null
    then
      echo "${testSuite} in ${dir} is not enabled" && exitCode=1
    fi
  fi
done <<< "$list"
exit ${exitCode}
