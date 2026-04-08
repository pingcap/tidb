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

set -xeuo pipefail

# When updating to a new Go version, update all of these variables.
GOVERS=1.20.5
GOLINK=https://go.dev/dl/go$GOVERS.src.tar.gz
SRCSHASUM=9a15c133ba2cfafe79652f4815b62e7cfc267f68df1b9454c6ab2a3ca8b96a88

update-alternatives --install /usr/bin/clang clang /usr/bin/clang-10 100 \
    --slave /usr/bin/clang++ clang++ /usr/bin/clang++-10

# libtapi is required for later versions of MacOSX.
git clone https://github.com/tpoechtrager/apple-libtapi.git
cd apple-libtapi
git checkout a66284251b46d591ee4a0cb4cf561b92a0c138d8
./build.sh
./install.sh
cd ..
rm -rf apple-libtapi

# crosstool-NG 1.24.0
curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/x86_64/20220711-205918/aarch64-unknown-linux-gnu.tar.gz -o aarch64-unknown-linux-gnu.tar.gz
echo '58407f1f3ed490bd0a0a500b23b88503fbcc25f0f69a0b7f8a3e8e7b9237341b aarch64-unknown-linux-gnu.tar.gz' | sha256sum -c -
curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/osxcross/x86_64/20220317-165434/x86_64-apple-darwin21.2.tar.gz -o x86_64-apple-darwin21.2.tar.gz
echo '751365dbfb5db66fe8e9f47fcf82cbbd7d1c176b79112ab91945d1be1d160dd5 x86_64-apple-darwin21.2.tar.gz' | sha256sum -c -
curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/x86_64/20220711-205918/x86_64-unknown-linux-gnu.tar.gz -o x86_64-unknown-linux-gnu.tar.gz
echo '8b0c246c3ebd02aceeb48bb3d70c779a1503db3e99be332ac256d4f3f1c22d47 x86_64-unknown-linux-gnu.tar.gz' | sha256sum -c -
echo *.tar.gz | xargs -n1 tar -xzf
rm *.tar.gz

curl -fsSL $GOLINK -o golang.tar.gz
echo "$SRCSHASUM  golang.tar.gz" | sha256sum -c -
mkdir -p /tmp/go$GOVERS
tar -C /tmp/go$GOVERS -xzf golang.tar.gz
rm golang.tar.gz
cd /tmp/go$GOVERS/go
# we apply a patch to the Go runtime to keep track of running time on a
# per-goroutine basis. See #41574
git apply /bootstrap/diff.patch
cd ..

CONFIGS="linux_amd64 linux_arm64 darwin_amd64 darwin_arm64"

for CONFIG in $CONFIGS; do
    case $CONFIG in
        linux_amd64)
            CC_FOR_TARGET=/x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-cc
            CXX_FOR_TARGET=/x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-c++
        ;;
        linux_arm64)
            CC_FOR_TARGET=/x-tools/aarch64-unknown-linux-gnu/bin/aarch64-unknown-linux-gnu-cc
            CXX_FOR_TARGET=/x-tools/aarch64-unknown-linux-gnu/bin/aarch64-unknown-linux-gnu-c++
        ;;
        darwin_amd64)
            CC_FOR_TARGET=/x-tools/x86_64-apple-darwin21.2/bin/x86_64-apple-darwin21.2-cc
            CXX_FOR_TARGET=/x-tools/x86_64-apple-darwin21.2/bin/x86_64-apple-darwin21.2-c++
            ;;
        darwin_arm64)
            CC_FOR_TARGET=/x-tools/x86_64-apple-darwin21.2/bin/aarch64-apple-darwin21.2-cc
            CXX_FOR_TARGET=/x-tools/x86_64-apple-darwin21.2/bin/aarch64-apple-darwin21.2-c++
        ;;
    esac
    GOOS=$(echo $CONFIG | cut -d_ -f1)
    GOARCH=$(echo $CONFIG | cut -d_ -f2)
    cd go/src
    if [ $GOOS == darwin ]; then
        export LD_LIBRARY_PATH=/x-tools/x86_64-apple-darwin21.2/lib
    fi
    GOOS=$GOOS GOARCH=$GOARCH CC=clang CXX=clang++ CC_FOR_TARGET=$CC_FOR_TARGET CXX_FOR_TARGET=$CXX_FOR_TARGET \
               GOROOT_BOOTSTRAP=$(go env GOROOT) CGO_ENABLED=1 ./make.bash
    if [ $GOOS == darwin ]; then
        unset LD_LIBRARY_PATH
    fi
    cd ../..
    rm -rf /tmp/go$GOVERS/go/pkg/${GOOS}_$GOARCH/cmd
    for OTHER_CONFIG in $CONFIGS; do
        if [ $CONFIG != $OTHER_CONFIG ]; then
            rm -rf /tmp/go$GOVERS/go/pkg/$OTHER_CONFIG
        fi
    done
    if [ $CONFIG != linux_amd64 ]; then
        rm go/bin/go go/bin/gofmt
        mv go/bin/${GOOS}_$GOARCH/* go/bin
        rm -r go/bin/${GOOS}_$GOARCH
    fi
    tar cf - go | gzip -9 > /artifacts/go$GOVERS.$GOOS-$GOARCH.tar.gz
    rm -rf go/bin
done

sha256sum /artifacts/*.tar.gz
