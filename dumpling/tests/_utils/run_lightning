#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu

echo "[$(date)] Executing bin/tidb-lightning..."

conf=$1

bin/tidb-lightning -c $1

echo "[$(date)] Executed bin/tidb-lightning"
