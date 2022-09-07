#!/bin/sh
set -e

mkdir -p bin/

# download lightning and sync_diff_inspector
TOOLS_TAG="nightly"
wget http://download.pingcap.org/tidb-toolkit-$TOOLS_TAG-linux-amd64.tar.gz -O tools.tar.gz
tar -xzvf tools.tar.gz
mv tidb-toolkit-$TOOLS_TAG-linux-amd64/bin/* bin/

# download minio
wget https://dl.min.io/server/minio/release/linux-amd64/minio -O bin/minio
chmod a+x bin/minio

wget https://dl.minio.io/client/mc/release/linux-amd64/mc -O bin/mc
chmod a+x bin/mc
