#!/bin/sh
set -e

mkdir -p bin/

# download lightning and sync_diff_inspector
TOOLS_TAG="nightly"
wget https://download.pingcap.com/tidb-toolkit-$TOOLS_TAG-linux-amd64.tar.gz -O tools.tar.gz
tar -xzvf tools.tar.gz
mv tidb-toolkit-$TOOLS_TAG-linux-amd64/bin/* bin/

# MinIO community binaries are no longer published at dl.min.io (HTTP 410).
# Build pinned upstream releases without changing TiDB's module dependencies.
GOBIN="$(pwd)/bin" go install github.com/minio/minio@RELEASE.2025-10-15T17-29-55Z
GOBIN="$(pwd)/bin" go install github.com/minio/mc@RELEASE.2025-08-13T08-35-41Z

go get github.com/ma6174/snappy@15869b0666f67839ecf86cd29ef1452ddcd79cb8
go install github.com/ma6174/snappy@15869b0666f67839ecf86cd29ef1452ddcd79cb8

wget https://github.com/facebook/zstd/releases/download/v1.5.2/zstd-1.5.2.tar.gz
tar xvfz zstd-1.5.2.tar.gz
cd zstd-1.5.2
make
