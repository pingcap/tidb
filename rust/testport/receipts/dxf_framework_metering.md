# `pkg/dxf/framework/metering` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly seven tracked artifacts and 1,411 lines. Every
production, test, and Bazel file was read in full in a detached worktree at the
pinned Go commit before this receipt was written. There is no `doc.go`,
fixture, `testdata`, generated source/input, platform-specific variant,
benchmark, fuzz target, or `OWNERS` file.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 57 | `1c2f498668f184479328eee490fb68c50489d041` | `c873cc02427bd7d0c995f81d3d75a35c3c5022e586a4714217ec548a7cfa33ae` | public metering library and 12-shard flaky test target with SDK/object-store dependencies |
| `data.go` | 120 | `5ef3f617a4b59323d154a837a622d5f8f5405100` | `060e2c39d5c6fd8ffa3ebf30868a039317b33fddef5ae0c47dfae49dba8843e3` | accumulated object-store/cluster counters, delta-item calculation, string rendering, and base metering item fields |
| `data_test.go` | 78 | `81c0396ca08f50b749eb09a5cc56da2d738d3ae7` | `4076f53d79b4fe663d0807f0d29950fcee056ba1f639d486c35c21e17f0712c7` | equality and monotonic delta-table tests |
| `metering.go` | 431 | `6254f637b36ba64e7222590e0a0d04cb4e119ceb` | `00844470bdb3386f873a947353b14a7a062c5728cca400bdbbe7b44d6a4492ee` | classic/next-gen registration gates, recorder lifecycle, flush/retry loops, SDK writer creation, failpoint hooks, metrics, and shutdown |
| `metering_test.go` | 627 | `04247e698dcb9eb5f89ddb8657dacd79a8ce9a1d` | `595c9443a603430e24c24cd5f5f1364d6c2eb99a27131dd8c8fb4f4fd7ad84f3` | config, classic/next-gen registration, flush/retry, virtual-time loop, close, and local object-store round-trip tests |
| `recorder.go` | 59 | `8424142aa4f0b87f73fda7f46c8d85da12686aae` | `d29066e2493a9a57dfda94fb7a7d8f9eab1ebba7c8b791ded06a412a37c5fa12` | atomic object-store/cluster traffic recorder and snapshot conversion |
| `recorder_test.go` | 39 | `d939b61dd7813bd1f876b6cf7fc4cba02a25f3c4` | `c8feef34b2321f58dffd789a952a406445fee4ffae02defaf1fd4fbb250314e8` | recorder counter aggregation test |

The package has 30 production function/method declarations and 16 test/helper
declarations (12 top-level tests plus four test helpers). `RegisterRecorder`,
`UnregisterRecorder`, and `WriteMeterData` are no-ops on classic deployments;
next-gen registration reuses task recorders and preserves a final flush on
unregister. `Meter.flush` computes monotonic deltas, records failed writes by
timestamp, and retries up to ten times; `StartFlushLoop` coordinates flush and
retry goroutines with a final close. Tests cover SDK provider configuration,
recorder re-registration races, no-data and failed-write deltas, retry/drop
policy, virtual-time flush scheduling, writer close errors, and local storage
read-back. The two failpoint hooks exercise minute-boundary timestamps and
final-flush cleanup, while `dxfmetric.ExecuteEventCounter` records write
failures.

## Rust ownership and parity decision

Rust has no dependency-closed owner for this DXF metering package. Rust config
contains only a metering-storage URI field, and executor comments/variables
mention generic metering; no Rust crate owns the Go recorder counters, SDK
writer, object-store format, retry lifecycle, failpoint hooks, or
classic/next-gen registration semantics. No Rust-only DXF metering behavior or
ignored test was found to remove. Adding an external metering facade would be
speculative, so this complete Go package remains an explicit Go-only boundary.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. Because the
package contains failpoint hooks, the prescribed wrapper was used; it enabled
and disabled Go failpoints around the exact suite in the pinned detached
Go-master worktree:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/framework/metering -count=1
# PASS
# ok github.com/pingcap/tidb/pkg/dxf/framework/metering 0.530s
```

Ready repository gates for this receipt batch are
`cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`,
`make lint`, and `git diff --check`. No Go source, import section, test,
Bazel target, or module dependency changed, so `make bazel_prepare` is not
required. Rust tests and a full workspace build are not run because no Rust
source or owning target changed.

The remaining risk is cross-boundary telemetry compatibility: changes to the
metering SDK schema, object-store provider, keyspace labels, flush timing, or
failpoint names must preserve the tested delta/retry behavior and next-gen
registration lifecycle. Rust has no equivalent implementation at this
boundary.
