#!/usr/bin/env bash

# TiUP's scale-out command does not forward GO_FAILPOINTS to a newly added
# TiDB process. Campaign 25 needs an actual prewritten-secondary fixture, so
# enable the exact client-go barrier at process launch and keep it confined to
# the disposable Go fixture (never the Rust node or TiUP supervisor).

set -euo pipefail

: "${C25_GO_TIDB_SERVER:?C25_GO_TIDB_SERVER must name the failpoint-enabled TiDB binary}"
export GO_FAILPOINTS='github.com/pingcap/tidb/pkg/server/enableTestAPI=return;tikvclient/beforeCommitSecondaries=return("skip")'
exec "${C25_GO_TIDB_SERVER}" "$@"
