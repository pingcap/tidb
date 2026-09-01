# `pkg/dxf/framework/testutil` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly seven tracked artifacts and 1,258 lines. Every
production/support file and the Bazel target was read in full in a detached
worktree at the pinned Go commit before this receipt was written. There is no
`doc.go`, `*_test.go`, fixture, `testdata`, generated source/input,
platform-specific variant, benchmark, fuzz target, or `OWNERS` file.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 41 | `0e4f3102dd4316756e2aa908cee5009fbc1c7d5b` | `bf861ad1a212187a9ec30ad3e0e5c67c60bd542f36f6947ebaaab02bd310adb8` | public helper library and DXF/mock/failpoint/testkit dependencies |
| `context.go` | 452 | `71bee4631bb681160361c56e7c54cc0f3089189f` | `b65ebde728a68b3df763e6271336f53301c82d4af52e3af0c232cc0d9887934b` | multi-node test context, scale-in/out, ownership election, async lifecycle, subtask collection, and interval reduction |
| `disttest_util.go` | 143 | `17eb7078405f261e84958e170d15d23039ef0313` | `1c4b42402f50fe90f0f7152f42f27032f8cfabba2c783d7e08eba17b96471bc8` | mock scheduler/executor/cleaner extensions, registration cleanup, and submit/wait helpers |
| `executor_util.go` | 38 | `8cb90ec9b0e8115370029641ed07073f5b5fd311` | `05c622e4ecea56028cc3820d262bb6809a5ffcd51d9b230c55497b51036a54ae` | registration helper for a common mock task executor |
| `scheduler_util.go` | 211 | `b82236a99a06abbf53467a71c6a118e08502585d` | `28aad532a3373807eeb48f6445d38c1a61a37522ca658b9656cb9f8f4fc82941` | configurable scheduler-extension mocks for normal, HA, retryable, non-retryable, planning-error, and rollback scenarios |
| `table_util.go` | 265 | `71aa1e6b5e354ebab0ee01851162ddd5f8672400` | `9b9302624b458f72ac89641466903876b5b134cfaf1efb429af34b926fead34b` | mock-store/session-pool setup, task/subtask SQL inspection and mutation helpers, end-time/history queries, and resource setup |
| `task_util.go` | 108 | `eea19547638adf6cea6cded985a7195271baa75c` | `687da0196836558c09da3b6dcbf753741eb83807cc83c26285be3a183a05f24e` | direct subtask insertion (with/without summary) and next-gen/system keyspace selection |

The package has 68 function/method declarations. The context helpers cover
randomized node pools, node-ID recycling, owner election, asynchronous
shutdown/change-owner, live executor IDs, cleanup, subtask collection, and
test interval overrides. Disttest and scheduler helpers configure exact
GoMock expectations for step transitions, metadata, retries, planner errors,
rollback, and completion. Table/task helpers execute the canonical DXF SQL
against mock stores and preserve failpoint cleanup. These are test-support
contracts consumed by sibling DXF tests, not production runtime behavior.

## Rust ownership and parity decision

Rust has no dependency-closed owner for this Go DXF test utility package. The
Rust `tidb-dxf` crate provides generic task/step/resource data and unit tests,
but no SQL-backed mock store, session pool, GoMock expectations, failpoint
harness, multi-node scheduler/executor lifecycle, or keyspace-aware test
context. No Rust-only test utility behavior or ignored test was found to
remove. A disconnected Rust helper facade would be speculative, so this
complete support package remains an explicit Go-only boundary.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The exact
Go-master package compile probe passed with no package-local test files:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework/testutil -count=1 -run '^$'
# ? github.com/pingcap/tidb/pkg/dxf/framework/testutil [no test files]
```

Ready repository gates for this receipt batch are
`cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`,
`make lint`, and `git diff --check`. No Go source, import section, test,
Bazel target, or module dependency changed, so `make bazel_prepare` is not
required. Rust tests and a full workspace build are not run because no Rust
source or owning target changed.

The remaining risk is helper drift: changes to DXF scheduler/task-table
interfaces, failpoint names, SQL schemas, or keyspace behavior require
updating these helpers and their consuming Go tests together. Rust parity at
this boundary is intentionally limited to the generic framework types already
owned by `tidb-dxf`.
