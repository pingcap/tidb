#!/bin/sh
set -e

mkdir -p bin/

# Detect architecture
ARCH=$(uname -m)
case $ARCH in
    x86_64)
        ARCH_SUFFIX="amd64"
        MINIO_SHA256="7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f"
        MC_SHA256="01f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891"
        ;;
    aarch64|arm64)
        ARCH_SUFFIX="arm64"
        MINIO_SHA256="5c83cd2cf151717ba0243f73e1c7802ff36e272b67144bdd7f1f7d684fd6f03d"
        MC_SHA256="14c8c9616cfce4636add161304353244e8de383b2e2752c0e9dad01d4c27c12c"
        ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

# download lightning and sync_diff_inspector
TOOLS_TAG="nightly"
wget https://download.pingcap.com/tidb-toolkit-$TOOLS_TAG-linux-$ARCH_SUFFIX.tar.gz -O tools.tar.gz
tar -xzvf tools.tar.gz
mv tidb-toolkit-$TOOLS_TAG-linux-$ARCH_SUFFIX/bin/* bin/

# Temporary CI-only pins: dl.min.io no longer serves community binaries.
MINIO_RELEASE="RELEASE.2025-09-07T16-13-09Z"
MC_RELEASE="RELEASE.2025-08-13T08-35-41Z"
wget "https://github.com/minio/minio/releases/download/$MINIO_RELEASE/minio.linux-$ARCH_SUFFIX.$MINIO_RELEASE" -O bin/minio
echo "$MINIO_SHA256  bin/minio" | sha256sum -c -
chmod a+x bin/minio

wget "https://github.com/minio/mc/releases/download/$MC_RELEASE/mc.linux-$ARCH_SUFFIX.$MC_RELEASE" -O bin/mc
echo "$MC_SHA256  bin/mc" | sha256sum -c -
chmod a+x bin/mc

go get github.com/ma6174/snappy@15869b0666f67839ecf86cd29ef1452ddcd79cb8
go install github.com/ma6174/snappy@15869b0666f67839ecf86cd29ef1452ddcd79cb8

wget https://github.com/facebook/zstd/releases/download/v1.5.2/zstd-1.5.2.tar.gz
tar xvfz zstd-1.5.2.tar.gz
cd zstd-1.5.2
make
