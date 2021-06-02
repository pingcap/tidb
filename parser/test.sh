#!/bin/sh

# If 'check.TestingT' is not used in any of the *_test.go files in a subdir no tests will run.

for f in $(git grep -l 'github.com/pingcap/check' | grep '/' | cut -d/ -f1 | uniq)
do
	if ! grep -r TestingT "$f" > /dev/null
	then
		echo "check.TestingT missing from $f"
		exit 1
	fi
done

GO111MODULE=on go test -p 1 -race -covermode=atomic -coverprofile=coverage.txt -coverpkg=./... ./...
