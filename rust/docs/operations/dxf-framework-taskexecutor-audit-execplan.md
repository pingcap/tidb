# `pkg/dxf/framework/taskexecutor` Go-master alignment ExecPlan

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; inventoried all 12 direct
  artifacts, 4,129 lines, 84 production declarations, and 20 top-level tests
  before editing. The nested `execute` package was audited separately.
- [x] (2026-09-02) Restored Go-master cancellation semantics: direct
  `ErrCancelSubtask` returns are informational, added `GetExecID`, removed the
  stale TaskTable state-query requirement, and locked the behavior with zap
  observer assertions.
- [x] (2026-09-02) Full failpoint-aware package suite passed in 16.116s.
- [ ] Complete Bazel/Ready gates, publish one batch commit, verify/pull
  `origin/hparser-integration`, and continue the rolling audit.

## Scope and integration decision

This package owns the Go task-executor manager and runtime lifecycle. Rust has
no dependency-closed equivalent for SQL-backed task execution, slot/resource
exchange, or StepExecutor orchestration; generic DXF records cannot substitute
for it. Keep this behavior Go-native and avoid speculative Rust-only runtime
code.

## Validation

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/framework/taskexecutor -count=1
```

Changed Go/Bazel files require `make bazel_prepare`; the local command is
blocked because `bazel` is unavailable. Shared Ready checks are `make lint`,
Rust formatting, and `git diff --check`.

## Outcome

The complete inventory and ownership boundary are recorded in
`rust/testport/receipts/dxf_framework_taskexecutor.md`; repository-wide parity
is not claimed and the rolling audit continues.
