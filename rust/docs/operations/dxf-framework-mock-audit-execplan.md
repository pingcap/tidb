# `pkg/dxf/framework/mock` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every direct Go-master artifact in the generated DXF mock package and
compare its planner, scheduler, storage, and taskexecutor test-support
contracts with Rust owners without fabricating disconnected mock APIs.

## Progress

- [x] (2026-09-02) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-02) Read all five direct artifacts and all 1,556 lines: the
  Bazel target plus complete MockGen outputs for `LogicalPlan`, `PipelineSpec`,
  `Scheduler`, `Cleaner`, `TaskManager`, storage `Manager`, `TaskTable`,
  `TaskExecutor`, and taskexecutor `Extension`. Verify the nested execute mock
  is a separate package unit and that no direct tests, fixtures, platform
  variants, benchmarks, fuzz targets, or generator inputs exist.
- [x] (2026-09-02) Trace all 195 generated functions and their source
  interfaces, then search Rust's `tidb-dxf` and test-mock owners for a
  dependency-closed equivalent.
- [x] (2026-09-02) Regenerate the scheduler/task-table mocks for Go master’s
  `Cleaner`/`GetCleanupTasks` contracts and remove stale methods; the package
  compile probe passes with no test files.
- [x] (2026-09-02) Run the package compile probe and Ready repository gates;
  capture the generated-source alignment and Bazel prerequisite limitation in
  the receipt.
- [ ] Publish this receipt batch to `origin/hparser-integration`, verify the
  remote SHA, pull the branch's latest state, and continue the rolling package
  audit.

## Scope and decision

This direct package is one generated test-support unit. Its only behavior is
MockGen forwarding and recorder registration for the nine Go interfaces;
runtime semantics and focused tests live in the parent DXF packages. Rust's
generic task/resource/step types cannot substitute for these Go interfaces or
the GoMock contract. Keep this boundary explicit until dependency-closed Rust
owners exist.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework/mock -count=1 -run '^$'
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

The generated Go interfaces changed, so `make bazel_prepare` is required; the
local command is blocked because `bazel` is unavailable. No Rust source or
owning target changed, so a Rust test target is not required.

## Outcome

The complete generated package inventory, validation evidence, and native
ownership decision are recorded in
`rust/testport/receipts/dxf_framework_mock.md`. The rolling audit continues
with the next unrecorded Go package; repository-wide parity is not claimed.
