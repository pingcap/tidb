# `pkg/store/gcworker` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`42db2099af50704e424b792626f10a87f4247413` (2026-09-02).

## Complete inventory

The package contains four tracked artifacts and 4,484 lines. Every production
source, test source, BUILD target, and test harness was read in full before
editing. There is no `doc.go`, generated source, platform-specific variant,
fixture directory, benchmark corpus, or nested package in this directory.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 101 | `c2b55212656695829fdc9b8b50e7b471e77ea45c` | `ca8b781970f38ad223226480b58da0192527827c941c8ec13c890c16385a1d7a` | GC worker library and 38-shard race-enabled test target |
| `gc_worker.go` | 1,938 | `f38b5a7c0ac46e9bd6583aa5587f970d4cc54cd1` | `ed7d8bba67a7327508224bdfb848b50591388ab2a250fffbed8b9806acae4538` | leader election, safe-point advancement, lock resolution, range cleanup, and GC lifecycle |
| `gc_worker_test.go` | 2,400 | `5a4b06568d0e23d9305a1e06e3b8af624365ae8f` | `9dadf934bdaf01adcc150b3abef6639d71f3567e0732a7c4b853fdf67afc23f1` | mock TiKV/PD suite, GC lifecycle, keyspace resolution, failure, and placement tests |
| `main_test.go` | 45 | `d267f55f5fb6c7e37534d0e486c4ccdde6d7c82a` | `29eab5cedfd563395f04b0025d11f51429e28c42daf8fad85d001b47644223cd` | failpoint-enabled TestMain and goleak cleanup harness |

The production source contains 55 function declarations covering worker
construction/start/close, status and configuration loading, leader and
safe-point lifecycle, distributed and keyspace GC, lock resolution, range
cleanup, placement/label cleanup, exported test APIs, and mock-worker helpers.
The test sources contain 72 helper/test declarations and 39 top-level tests.
They exercise the worker's mock-store and PD setup, GC enable/lifetime/
concurrency checks, leader transitions, lock boundary handling, keyspace and
multi-batch range partitioning, TiKV request failures, RaftKV2 cleanup,
placement/label rule deletion, pending transactions, exported APIs, and
goleak/failpoint lifecycle. The package has no fixture or generated input to
inventory beyond the checked-in BUILD target and embedded mock setup.

## Go-master delta and implementation

Go `master` adds the external-workload notification after a successful GC
round. `notifyGCV2AfterGC` first requires an enabled manager and keyspace-level
GC, recycles completed work for master/TTL/GCV2 roles, and registers the next
GCV2 task for master/TTL roles using the configured GC lifetime. Notification
and lifetime-load failures are deliberately logged as best effort so a
controller outage does not make an otherwise successful GC round fail. The
worker now invokes this hook after broadcasting the GC safe point. BUILD
metadata includes the `config` and `extworkload` dependencies and the added
test shard.

`TestNotifyGCV2AfterGCForDedicatedWorker` covers keyspace-level recycle,
best-effort recycle failure, and the unified-GC no-op path with a stub manager;
it also asserts that a GCV2 worker does not register a new task. Before the
production hook existed, the focused test failed to compile with
`worker.notifyGCV2AfterGC undefined`; it passes after the implementation.

## Rust ownership and parity result

Rust has configuration constants for external-workload roles and a manager
client, but no dependency-closed `GCWorker` owner that performs TiKV GC,
safe-point broadcasting, lock resolution, and keyspace-level task
notifications. No Rust-only behavior was found to remove. Porting only a
notification call into the Rust config/client crates would omit the GC worker
lifecycle and would create a second, speculative execution path, so the
remaining Rust boundary is explicit.

## Validation and risk

Profile: **Ready** for this package behavior restoration. The package uses
failpoints, so all Go tests were run through the failpoint wrapper and its
state was disabled afterward.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/store/gcworker \
  -run '^TestNotifyGCV2AfterGCForDedicatedWorker$' -count=1
# passed; failpoints enabled and disabled

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/store/gcworker -count=1
# passed; 24.912s, failpoints enabled and disabled

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
make lint
# passed

git diff --check
# passed

make bazel_prepare
# blocked: `make: bazel: No such file or directory`
```

No Rust source changed, so no Rust cargo gate was applicable. Not verified
here: Bazel analysis/sharding, live PD or external-workload controller
services, Windows execution, and full-workspace tests. Correctness risk is
limited to role and keyspace-GC gating around best-effort controller calls;
the focused matrix and full package suite cover those branches. Compatibility
and performance risk are low: successful GC work is unchanged and the added
controller calls occur once per completed keyspace-level round.
