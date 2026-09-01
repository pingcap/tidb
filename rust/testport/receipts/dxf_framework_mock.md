# `pkg/dxf/framework/mock` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The direct package contains exactly five tracked artifacts and 1,556 lines.
Every direct file was read in full in a detached worktree at the pinned Go
commit before this receipt was written. The nested
`pkg/dxf/framework/mock/execute` package is a separate package unit and is
inventoried in `dxf_framework_mock_execute.md`. There is no `doc.go`, `OWNERS`,
test file, fixture, benchmark, fuzz target, generator input, or
platform-specific variant; all four Go sources below are generated MockGen
outputs.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 21 | `94704f470f520cb22bef0d5dcc60ab736992438d` | `dbfcca067e0232b95ceeed19cfb9da3a9ec2e7d00dfbb7b57640b5e96a1442b9` | public Bazel library and generated-mock dependency manifest |
| `plan_mock.go` | 147 | `5ea81cc63c36dc394f7ee97e8b4967b46583f0db` | `ca9bbf081c5a0b5c35017306f26292637a275953fd5d9ba3096641da55c1b0f2` | GoMock implementations for planner `LogicalPlan` and `PipelineSpec` |
| `scheduler_mock.go` | 806 | `b27117d8e578ded246fe0be12ad88249c015d51d` | `15d8ab108a225f55ea91a6c6d7b5754cfca1a3e35aa6a5df6d9c67b7d0d5ea52` | GoMock implementations for scheduler `Scheduler`, `Cleaner`, and `TaskManager` |
| `storage_manager_mock.go` | 90 | `74ed36d475147414e4b4efb330fc1fc982b37300` | `58764cb591d798c02453f3e0c4739bc7ddeb38f5ab77f3d24eafc53f317ae2a6` | GoMock implementation for storage `Manager` |
| `task_executor_mock.go` | 492 | `2b2951b8bf05c542fd81bfa30f97d5a541c28649` | `a93f1d8523f6268e8aacc611c9971d0ecf30adeca46f1e46b69241997aa594a8` | GoMock implementations for taskexecutor `TaskTable`, `TaskExecutor`, and `Extension` |

The generated sources contain 195 function declarations including every
constructor, `EXPECT`, `ISGOMOCK`, forwarding method, and recorder method for
the nine source interfaces. The scheduler mock covers scheduler lifecycle,
task transitions, metadata, subtask state/summary queries, node/slot
management, transactional session callbacks, and cleaner operations. The task
executor mock covers task-table metadata/checkpoint/subtask APIs, executor
lifecycle/cancellation, and extension step-executor/idempotence hooks. All
forward calls preserve the exact Go context, proto, storage, execute, and
session context signatures. There are no package-local tests; parent DXF
planner, scheduler, storage, and taskexecutor tests consume these generated
seams.

## Rust ownership and parity decision

Rust has no dependency-closed owner for these generated GoMock packages. The
Rust `tidb-dxf` crate owns generic task/resource/step data but not the Go
planner, scheduler, storage-manager, task-table, or executor interfaces, nor a
GoMock controller/recorder contract. No Rust-only mock behavior or ignored test
was found to remove. Adding disconnected Rust mocks would be speculative, so
this complete generated-support package remains an explicit Go-only boundary.

## Go-master alignment and validation

The generated scheduler mock now exposes the Go-master `Cleaner` and
`GetCleanupTasks` methods while dropping the stale `GetTasksInStates` seam;
the task-table mock likewise follows the parent taskexecutor contract. These
outputs were regenerated from the pinned Go-master interfaces and are consumed
by the scheduler cleanup regressions.

Profile: **Ready** for this generated-support batch. The complete package
compile probe passed with no test files:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework/mock -count=1 -run '^$'
# ? github.com/pingcap/tidb/pkg/dxf/framework/mock [no test files]
```

Ready repository gates for this batch are
`cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`,
`make lint`, and `git diff --check`. `make bazel_prepare` is required because
the generated Go interfaces changed; the local gate is blocked by the
unavailable `bazel` executable. Rust tests and a full workspace build are not
run because no Rust source or owning target changed.

The remaining risk is generated-code drift: any future interface change in the
nine source contracts must regenerate the corresponding MockGen output.
Execution semantics and regression coverage remain owned by the parent DXF
packages that consume these mocks.
