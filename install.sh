#!/bin/sh

set -e

TAG="nightly"
pwd=$(pwd)

mkdir bin/

# download lightning and sync_diff_inspector
wget http://download.pingcap.org/tidb-toolkit-$TAG-linux-amd64.tar.gz -O tools.tar.gz
tar -xzvf tools.tar.gz
mv tidb-toolkit-$TAG-linux-amd64/bin/* bin/

TIDB_TAG="v4.0.4"
# download tidb-server
git clone -b $TIDB_TAG https://github.com/pingcap/tidb
cd $pwd/tidb && make
cd $pwd
mv tidb/bin/tidb-server bin/
