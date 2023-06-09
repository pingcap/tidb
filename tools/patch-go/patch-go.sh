#!/bin/bash

CWD=$(dirname $(readlink -f "$0"))

# check whether the go is patched, the patched-go can build this file, otherwise not
go build $CWD/check.go 2> /dev/null;

if [ $? -eq 0 ] && [ $# -eq 0 ]  ; then
    echo "go is patched already"
    echo "if you want to reverse the patch, use ./patch-go.sh -R"
    exit 0; # patched already
fi

VERSION=$(go version | grep -o 'go[1-9]\.[0-9]*\.[0-9]*')
# VERSION=$(go env GOVERSION)

if [ ! -f $CWD/$VERSION.patch ]; then
    echo "patch file not exist for version $VERSION"
    exit -1;
fi

echo "patching $VERSION"
GOROOT=$(go env GOROOT)
echo "locate go src at $GOROOT"

cd $GOROOT;
echo "apply patch $1 $CWD/$VERSION.patch"
git apply $1 $CWD/$VERSION.patch

