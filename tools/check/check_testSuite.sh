#!/bin/bash

set -euo pipefail

exitCode=0
for testSuite in $(find . -name "*_test.go" -print0 | xargs -0 grep -E "type test(.*)Suite" | awk '{print $2}'); do
  # TODO: ugly regex
  # TODO: check code comment
  if ! find . -name "*_test.go" -print0 | xargs -0 grep -E "_ = (check\.)?(Suite|SerialSuites)\((&?${testSuite}{|new\(${testSuite}\))" > /dev/null
  then
    if find . -name "*_test.go" -print0 | xargs -0 grep -E "func \((.* )?\*?${testSuite}\) Test" > /dev/null
    then
      echo "${testSuite} is not enabled" && exitCode=1
    fi
  fi
done
exit ${exitCode}
