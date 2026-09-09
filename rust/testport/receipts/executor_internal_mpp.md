# `pkg/executor/internal/mpp` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains five tracked artifacts and 1,595 lines. Every production
source, test, and Bazel target was read line by line before this receipt was
written. There are no generated sources, platform-specific variants,
benchmarks, fuzz targets, or fixture files.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 59 | `229a8ce50d409070f52d86a8d7d1c486d1b9da41` | `bcaf8f5ec78109420e484c82de14556b3d1edf4056c90395e9ef863b2af6608c` | internal MPP library and two-shard flaky test target |
| `executor_with_retry.go` | 260 | `527993ecc32c4c5e8d76e3171aa8783b43056a39` | `c2e44ae9c9409910952aa6113e659e9638664093b198c5f2391aa0365671cab0` | coordinator setup, retry, recovery, result buffering, and lifecycle |
| `local_mpp_coordinator.go` | 945 | `2ece374dbd769b7e81d6912953576e2d63d7a26e` | `6c3830278a30842953721750f9c48f7eaa3b3cf877b19b6cc8445eb381e30725` | TiFlash task construction, dispatch, stream receive, cancellation, zone routing, and runtime reporting |
| `local_mpp_coordinator_test.go` | 140 | `2ba70761f922074a983c1d60dcfe19450f7d6997` | `52b9c57497132fd0131e35ffed5d0f68707b82d9cff4fb4c33b6a800f7cf79ed` | execution-summary traversal and exchange-zone helper tests |
| `recovery_handler.go` | 191 | `b4ac8cc7540f18f5dcff1389b0484825b7e22565` | `f57d80386adb998efeaab0eb9c3242f8f8717aeb1be3f5e3bb4e4f1837b60125` | bounded result holding, memory tracking, retries, and recovery accounting |

`ExecutorWithRetry` wraps the MPP coordinator, allocates gather IDs, applies
configuration and failpoint-controlled recovery, retries setup and `Next`, and
releases coordinator resources on close. `RecoveryHandler` bounds held result
subsets with a memory tracker and coordinates retry/recovery counters.
`LocalMppCoordinator` builds DAG requests, rewrites physical table IDs,
selects TiFlash stores and zones, dispatches and cancels tasks, receives stream
subsets, reports execution summaries, and exposes node-count/runtime state.
The package tests cover limit-sensitive execution-summary traversal and all
known exchange-zone inference cases (empty, certain, uncertain, root sender,
and receiver paths). The Bazel target is short/flaky and two-sharded.

## Rust ownership and explicit boundary

Rust currently provides MPP protocol metadata and client/coordinator traits in
`tidb-txnkv::mpp`, failed-TiFlash probing in `mpp_probe`, planner MPP property
types, and runtime/slow-log TiFlash statistics. There is no production
implementation of those client or coordinator traits and no dependency-closed
Rust execution path corresponding to `LocalMppCoordinator`, retry/recovery
result holding, TiFlash task dispatch/stream handling, zone routing, or
execution-summary reporting. The SQL parser and planner metadata therefore do
not establish MPP execution parity.

No Rust-only MPP execution behavior was found to remove, and no speculative
coordinator or TiFlash transport implementation was added. This complete Go
package remains an explicit SEED/boundary until a real execution owner and its
full integration/test dependency closure are implemented.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. The package uses
failpoints, so the required wrapper enabled them before testing and disabled
them afterward. No Go source, imports, Bazel metadata, or module files changed;
`make bazel_prepare` and the Ready lint gate are not required for this batch.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/executor/internal/mpp -count=1
# passed: package tests; failpoints enabled before the run and disabled afterward
```

Not verified here: Rust TiFlash/MPP execution (no owner), real TiFlash
cross-node dispatch, Bazel execution, and full workspace tests. Existing
unrelated planner/session worktree changes remain outside this receipt.
