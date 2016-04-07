#!/bin/sh
#
# Usage: add-leaktest.sh pkg/*_test.go

set -eu

sed -i'~' -e '
  /^func (s \*test.*Suite) Test.*(c \*C) {/ {
    n
    /testleak.AfterTest/! i\
		defer testleak.AfterTest(c)()
  }
' $@

for i in $@; do
  if ! cmp -s $i $i~ ; then
    goimports -w $i
  fi
echo $i
  rm -f $i~
done
