# `pkg/dxf/framework/scheduler` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every direct Go-master scheduler production, test, and build artifact,
trace the complete state-machine/resource/cleanup contract, and compare it with
Rust owners without fabricating a disconnected scheduler runtime.

## Progress

- [x] (2026-09-02) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-02) Read all 17 direct artifacts and all 6,321 lines: the
  11-shard flaky Bazel target, eight production files, and nine test files.
  Verify the nested `scheduler/mock` package is separate and that no direct
  fixtures, testdata, generated/platform variants, benchmarks, fuzz targets,
  or OWNERS files exist.
- [x] (2026-09-02) Trace all 103 production declarations, 47 top-level tests,
  and 25 test helpers across autoscaling, slots, nodes, balancing, state
  transitions, BaseScheduler, Manager loops, SQL/task contracts, failpoints,
  and cross-keyspace runtime tests. Search Rust `tidb-dxf`/domain owners and
  confirm no dependency-closed scheduler implementation exists.
- [x] (2026-09-02) Align bounded cleanup selection, cleaner capability
  grouping, history-transfer progress, startup draining, and data-error
  metrics with Go master; retain explicit legacy cleanup adapters for
  unmigrated DDL/import-into callers.
- [x] (2026-09-02) Run the failpoint-aware complete package suite and Ready
  repository gates; record the pass, focused regression coverage, and Bazel
  prerequisite limitation in the receipt.
- [ ] Publish this receipt batch to `origin/hparser-integration`, verify the
  remote SHA, pull the branch's latest state, and continue the rolling package
  audit.

## Scope and decision

This package owns the Go DXF distributed scheduler: task-state transitions,
resource estimation/reservation, node liveness, balancing, cleanup, and
keyspace-aware storage interactions. Rust's generic value types do not replace
these runtime loops. Keep the complete package as an explicit Go-only boundary
until dependency-closed Rust scheduler/storage/taskexecutor owners exist.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/framework/scheduler -count=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

Go/Bazel source changed, so `make bazel_prepare` is required; the local gate is
blocked because `bazel` is unavailable. No Rust source or owning target
changed, so a Rust test target is not required.

## Outcome

The complete scheduler inventory, failpoint-aware validation evidence, and Rust
ownership decision are recorded in
`rust/testport/receipts/dxf_framework_scheduler.md`. The rolling audit
continues with the next unrecorded Go package; repository-wide parity is not
claimed.
