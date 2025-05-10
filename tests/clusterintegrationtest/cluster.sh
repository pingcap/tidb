#!/bin/bash

cd ../../ || exit 1
echo "building tidb-server..."
make
echo "build successfully"

cd - || exit 1

echo "Starting TiUP Playground in the background..."
tiup playground nightly --db=1 --kv=1 --tiflash=1 --db.binpath=../../bin/tidb-server