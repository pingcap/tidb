#!/bin/bash
set -euox pipefail
IFS=$'\n\t'

# This script creates a fake GOPATH, symlinks in the current
# directory as uber-go/atomic and verifies that tests still pass.

WORK_DIR=`mktemp -d`
function cleanup {
	rm -rf "$WORK_DIR"
}
trap cleanup EXIT


export GOPATH="$WORK_DIR"
PKG_PARENT="$WORK_DIR/src/github.com/uber-go"
PKG_DIR="$PKG_PARENT/atomic"

mkdir -p "$PKG_PARENT"
cp -R `pwd` "$PKG_DIR"
cd "$PKG_DIR"

# The example imports go.uber.org, fix the import.
sed -e 's/go.uber.org\/atomic/github.com\/uber-go\/atomic/' -i="" example_test.go

make test
