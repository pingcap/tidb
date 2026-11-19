#!/bin/sh
set -e

mkdir -p bin/

# download lightning and sync_diff_inspector
TOOLS_TAG="nightly"
wget https://download.pingcap.com/tidb-toolkit-$TOOLS_TAG-linux-amd64.tar.gz -O tools.tar.gz
tar -xzvf tools.tar.gz
mv tidb-toolkit-$TOOLS_TAG-linux-amd64/bin/* bin/

# Temporary CI-only pins: dl.min.io no longer serves community binaries.
MINIO_RELEASE="RELEASE.2025-09-07T16-13-09Z"
MC_RELEASE="RELEASE.2025-08-13T08-35-41Z"
MINIO_SHA256="7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f"
MC_SHA256="01f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891"
wget "https://github.com/minio/minio/releases/download/$MINIO_RELEASE/minio.linux-amd64.$MINIO_RELEASE" -O bin/minio
echo "$MINIO_SHA256  bin/minio" | sha256sum -c -
chmod a+x bin/minio

wget "https://github.com/minio/mc/releases/download/$MC_RELEASE/mc.linux-amd64.$MC_RELEASE" -O bin/mc
echo "$MC_SHA256  bin/mc" | sha256sum -c -
chmod a+x bin/mc

go get github.com/ma6174/snappy@15869b0666f67839ecf86cd29ef1452ddcd79cb8
go install github.com/ma6174/snappy@15869b0666f67839ecf86cd29ef1452ddcd79cb8

wget https://github.com/facebook/zstd/releases/download/v1.5.2/zstd-1.5.2.tar.gz
tar xvfz zstd-1.5.2.tar.gz
cd zstd-1.5.2
make
