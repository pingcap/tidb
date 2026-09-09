# `pkg/executor/internal/calibrateresource` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package contains four tracked artifacts and 1,613 lines. Every production
source, failpoint/goleak test harness, test, and Bazel target was read line by
line before editing. There are no generated sources, platform-specific
variants, benchmark targets, fuzz targets, or fixture files.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 57 | `4e859141bc6ff3e8e266d963966029e78f30f601` | `bb71ad9a14e2d26d68267f86bd5a84bd6e045323c5a30bd0a025ad94bf1eae17` | internal calibration library and two-shard flaky test target |
| `calibrate_resource.go` | 703 | `e0b586b225f45cb0cdd3362d7479157bd6bda286` | `65b387c265ea3a1dc882f5700a65d9368cbf5877cb3398a4cfd1413bbf592494` | static/dynamic workload calibration executor and metric sampling |
| `calibrate_resource_test.go` | 800 | `1d0890cbaeb2b1619a8bdaa59d61b9acd15f4fd3` | `2f88f5f4b69ec232828036cf74b294a7ffefe5a8bffcda834cb7d4762342c5fe` | resource-control flag, workload, duration, metric, TiFlash, and low-usage tests |
| `main_test.go` | 53 | `daa4c741bb051a4930590d0afd8f0f85614ca455` | `b246b699476bceb696f7d039f876c0252efafd60b41c87e52d5304e5d223418f` | common setup, TiKV failpoints, and goleak harness |

`Executor.Next` rejects disabled resource control, routes static versus
dynamic calibration, and returns one quota row. Static calibration combines
workload-specific TiKV/TiDB CPU, request, byte, and PD resource-group costs;
dynamic calibration parses timestamp/duration expressions, aligns RU and CPU
time series within ten seconds, discards extreme quota samples, and handles
TiFlash CPU/RU metrics. The tests cover all workload types, time-expression
forms, malformed/too-long/too-short windows, low usage, unsynchronized points,
missing metrics, mock HTTP Prometheus responses, and TiFlash fallback.

## Rust ownership and explicit boundary

Rust owns the `CALIBRATE RESOURCE` AST and parser grammar/tests, but no
dependency-closed execution owner exists. There is no Rust session/admin
dispatcher arm, cluster-server metrics query path, resource-group controller
quota calculator, restricted SQL metrics reader, or TiFlash calibration
executor corresponding to this Go package. The existing Rust resource-manager
and statistics modules expose unrelated production primitives; wiring them
into a new calibration command would invent an execution architecture without
the required cluster and HTTP contracts.

No Rust-only calibration behavior was found to remove, and no speculative
implementation was added. The complete Go package is therefore recorded as an
explicit SEED/boundary; parser acceptance must not be reported as execution
parity until the missing owner and its full integration/test dependency closure
are implemented.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. The package uses
failpoints, so the required wrapper enabled them before testing and disabled
them afterward. No Go source, imports, Bazel metadata, or module files changed;
`make bazel_prepare` and the Ready lint gate are not required.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/executor/internal/calibrateresource -count=1
# passed in 22.910s; failpoints enabled before the run and disabled afterward
```

Not verified here: Rust calibration execution (no owner), cluster metrics and
TiFlash integration on the Rust path, Bazel execution, and full workspace
tests. Existing unrelated planner/session worktree changes remain outside this
receipt.

