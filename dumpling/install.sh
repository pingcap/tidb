#!/bin/sh
set -e

mkdir -p bin/

# Detect architecture
ARCH=$(uname -m)
case $ARCH in
    x86_64)
        ARCH_SUFFIX="amd64"
        ;;
    aarch64|arm64)
        ARCH_SUFFIX="arm64"
        ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

# download lightning and sync_diff_inspector
TOOLS_TAG="nightly"
wget http://download.pingcap.com/tidb-toolkit-$TOOLS_TAG-linux-$ARCH_SUFFIX.tar.gz -O tools.tar.gz
tar -xzvf tools.tar.gz
mv tidb-toolkit-$TOOLS_TAG-linux-$ARCH_SUFFIX/bin/* bin/

# Pin official GitHub release assets; the MinIO download sites no longer serve binaries.
MINIO_RELEASE="RELEASE.2025-09-07T16-13-09Z"
MC_RELEASE="RELEASE.2025-08-13T08-35-41Z"
wget "https://github.com/minio/minio/releases/download/$MINIO_RELEASE/minio.linux-$ARCH_SUFFIX.$MINIO_RELEASE" -O bin/minio
chmod a+x bin/minio

wget "https://github.com/minio/mc/releases/download/$MC_RELEASE/mc.linux-$ARCH_SUFFIX.$MC_RELEASE" -O bin/mc
chmod a+x bin/mc

go get github.com/ma6174/snappy@15869b0666f67839ecf86cd29ef1452ddcd79cb8
go install github.com/ma6174/snappy@15869b0666f67839ecf86cd29ef1452ddcd79cb8

wget https://github.com/facebook/zstd/releases/download/v1.5.2/zstd-1.5.2.tar.gz
tar xvfz zstd-1.5.2.tar.gz
cd zstd-1.5.2
make
