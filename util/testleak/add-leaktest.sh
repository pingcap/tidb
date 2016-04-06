#!/bin/sh
#
# Usage: add-testleak.sh pkg/*_test.go

set -eu

sed -i'~' -e '
  /^func (s \*test.*Suite) Test.*(c \*C) {/ {
    # Skip past the test declaration
    n
    # If the next line does not call AfterTest, insert it.
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
