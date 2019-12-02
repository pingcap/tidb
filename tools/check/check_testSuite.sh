#!/usr/bin/env bash

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
