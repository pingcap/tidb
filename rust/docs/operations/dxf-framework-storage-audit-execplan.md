# `pkg/dxf/framework/storage` Go-master alignment ExecPlan

This living plan records the package-atomic inventory, restored storage
semantics, and validation evidence.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; read and inventoried all 11
  production/test/Bazel artifacts (4,841 lines) before editing.
- [x] (2026-09-02) Restored canonical VARCHAR task-key conversion, atomic
  task/subtask history transfer, bounded cleanup selection, safe history error
  code/category projection, and upstream table/state regressions.
- [x] (2026-09-02) Added `history_test.go::TestClassifyTaskError`; the first
  storage run exposed the missing proto cleanup-batch dependency, which was
  aligned and covered by `TestTaskCleanupBatchSize`.
- [x] (2026-09-02) Full failpoint-aware storage suite passed in 36.479s.
- [ ] Run `make bazel_prepare`, shared Ready gates, publish one meaningful
  batch commit, verify/pull `origin/hparser-integration`, and continue the
  rolling package audit.

## Scope and integration decision

This package is the Go SQL/session-backed DXF storage owner. Rust owns only
generic DXF records and has no dependency-closed equivalent for TiDB system
tables, transaction/session handling, history cleanup, or the HTTP API. No
Rust-only storage behavior was found to remove; adding a parallel Rust store
would invent schema and transaction semantics. The branch retains the older
`GetTasksInStates` method for existing scheduler/task-executor interfaces while
using Go master’s bounded `GetCleanupTasks` for new cleanup callers.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/framework/storage -count=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

`make bazel_prepare` is required because Go sources/tests and Bazel metadata
changed. No Rust source or target changed, so no Rust test target is needed.

## Outcome

The complete mapping, hashes, risk notes, and test evidence are in
`rust/testport/receipts/dxf_framework_storage.md`. This plan does not claim
repository-wide parity; the rolling audit continues with the next package.
