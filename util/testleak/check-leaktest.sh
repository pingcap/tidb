#!/bin/sh
#
# Usage: check-leaktest.sh
# It needs to run under the github.com/pingcap/tidb directory.

set -e

pkgs=$(git grep 'Suite' |grep -vE "Godeps|tags" |awk -F: '{print $1}' | xargs -n1 dirname | sort |uniq)
echo $pkgs
for pkg in ${pkgs}; do
  if [ -z "$(ls ${pkg}/*_test.go 2>/dev/null)" ]; then
    continue
  fi
  awk -F'[(]' '
/func \(s .*Suite\) Test.*C\) {/ {
  test = $1"("$2
  next
}

/defer testleak.AfterTest/ {
  test = 0
  next
}

{
    if (test && (FILENAME != "./tidb_test.go")) {
    	printf "%s: %s: missing defer testleak.AfterTest\n", FILENAME, test
    	test = 0
    	code = 1
    }
}

END {
  exit code
}

' ${pkg}/*_test.go
done
