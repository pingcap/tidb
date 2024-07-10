#!/usr/bin/env bash
# Copyright 2023 PingCAP, Inc.
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

