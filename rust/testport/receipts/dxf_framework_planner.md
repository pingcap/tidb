# `pkg/dxf/framework/planner` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly five tracked artifacts and 320 lines. Every
production, test, and Bazel file was read in full in a detached worktree at the
pinned Go commit before this receipt was written. There is no `doc.go`, fixture,
`testdata`, generated source/input, platform-specific variant, benchmark, fuzz
target, or `OWNERS` file.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 41 | `6096e2c9cf7f73eb1b3ea826a11408c8189ef438` | `b08f9fa2b9cf0bf0c099069a6538742882a94053da40789a2ad08e8ac8887b93` | public planner library and two-shard flaky test target |
| `plan.go` | 123 | `d18de2a06fa05f5ad0b149943eb497c1568663bd` | `f41e3767fbb14c795668c528febaa369bd0bf24e5810c450eaafe9e45173dfd2` | DXF planning context, logical/physical plan contracts, processor/link specs, and subtask-meta conversion |
| `plan_test.go` | 38 | `2390e9e704b2bc517251d9ecbaf5eae4b97244a8` | `c87c090206a8bf4984be313a54fd714906e57047f6fad99c74dd079ae61778b3` | physical-plan filtering and pipeline conversion test |
| `planner.go` | 51 | `dfcd7780b7b55f85168036fcd70e7a3a79625c6f` | `c42d98b4a35140e30d72f42526faea9264a7ee969eea16528a6336523246b03c` | planner construction and task creation through a session-aware storage manager |
| `planner_test.go` | 67 | `f763e0ec99519fe571604b7d3ea48b4bff54b2e6` | `664eabd35fe5d790ef3d169136429b7f3b00d2f353697f46ca382dde792fdcca` | mock-store planner integration test for task metadata, slots, type, and extra params |

The package has four production method/function declarations and two top-level
tests. `PlanCtx` carries session, task, keyspace, node, store, step, and prior
subtask metadata into planning. `PhysicalPlan.ToSubtaskMetas` filters processors
by step and delegates exact pipeline metadata conversion, propagating errors.
`Planner.Run` serializes a logical plan, obtains the kernel target scope, and
creates a task through `TaskManager.CreateTaskWithSession` with thread/node and
extra-parameter settings. Tests cover successful pipeline conversion and the
storage-backed planner path using GoMock and a mock store.

## Rust ownership and parity decision

Rust's `tidb-planner` crate owns SQL logical/physical optimization, which is a
different subsystem. It has no dependency-closed owner for this DXF planner's
`PlanCtx`, processor DAG, pipeline-to-subtask metadata conversion, or
session-aware task creation. No Rust-only DXF planner behavior or ignored test
was found to remove. Adding a second planner facade disconnected from the Go
DXF storage/scheduler contracts would be speculative, so this complete Go
package remains an explicit Go-only boundary.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The exact
Go-master suite passed with the repository's required integration-test build
tag:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test -tags=intest ./pkg/dxf/framework/planner -count=1
# ok github.com/pingcap/tidb/pkg/dxf/framework/planner 2.041s
```

A tagless run is intentionally rejected by the package's testkit guard; the
`-tags=intest` result above is the authoritative validation. Ready repository
gates for this receipt batch are `cargo +nightly-2026-08-22 fmt
--manifest-path rust/Cargo.toml --all -- --check`, `make lint`, and
`git diff --check`. No Go source, import section, test, Bazel target, or module
dependency changed, so `make bazel_prepare` is not required.

The remaining risk is planner contract drift: logical-plan metadata, processor
step filtering, target-scope defaults, and storage-manager task creation must
remain synchronized with scheduler/taskexecutor consumers. Rust's SQL planner
does not satisfy this DXF boundary.
