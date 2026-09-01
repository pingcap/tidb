# `pkg/dxf/example` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly six tracked artifacts and 332 lines. Because it
has a package doc, `doc.go` was read first; every remaining production, test,
and build file was then read in full in a detached worktree at the pinned Go
commit. There are no fixtures, `testdata`, generated sources or inputs,
platform variants, benchmarks, fuzz targets, or `OWNERS` files.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 40 | `4c1686fbb4d24494c3b1970d339934e05b473b81` | `c4ab2256d4a7cc893e9a8cd1261820717a4580f9cd6c30885374bf97df19ff9f` | Bazel library/test targets; marks the integration test flaky and wires DXF/testkit dependencies |
| `app_test.go` | 59 | `32de35cb67c41c194be3184cc7b103daf7781ac5` | `3dd69e471daf6692befd12247234847435691f9dc4425f35634cc71e5f9c1e41` | end-to-end mock-store application test registering scheduler, cleaner, and executor factories, submitting a task, and awaiting completion |
| `doc.go` | 27 | `2acd0aa354b6965d116001db36022d5c4430fa04` | `ffed8b829e830e863c9fd4702769251490c94c322601487da3c1f9149c01defd` | package-level DXF integration guide and two-step example contract |
| `proto.go` | 24 | `45d63147c1d09e696a61309b0d010bfe198360bb` | `9c32ca03579031702e64f5d25b0bce1559c804136499f389456f3c5514d75bef` | JSON task/subtask metadata structs |
| `scheduler.go` | 115 | `388753c4cfbb0a5eff7dfb2141cc93e92e29c0fb` | `bf7cd209d3a7ac6a3c46173d7e3ea1f06d5752016ed5c8fc46b11e75bfdf50c4` | example scheduler, step planning, subtask metadata generation, retry policy, and cleaner |
| `task_executor.go` | 67 | `60ade4c4d553649c8cb8d4bb1991f92ed5bd213a` | `08d23bb9e180b77a214e3c084af6e046edf0a06878efb9e89852d1f194090f7a` | example task/step executor, JSON metadata decoding, and idempotent/retry hooks |

The package has 15 function/method declarations (including one top-level
`TestExampleApplication`). The scheduler registers the Example task type,
decodes `taskMeta`, emits one JSON subtask message per configured subtask for
StepOne/StepTwo, advances Init→One→Two→Done, accepts all errors as retryable,
and logs completion. The executor registers the task type, returns a step
executor for each subtask, decodes `subtaskMeta.Message`, logs it, and reports
idempotent/retryable behavior. The test creates a mock store, registers all
factories, submits the task, and waits for completion. No fixture or generated
artifact is involved.

## Rust ownership and parity decision

Rust's `tidb-dxf` crate owns generic task/step vocabulary and includes the
`TASK_TYPE_EXAMPLE`, `STEP_ONE`, and `STEP_TWO` constants as framework enum
parity. It does not own this Go example's scheduler/executor factory
registration, JSON metadata protocol, mock-store integration, or flaky
end-to-end harness. No Rust-only example behavior or ignored test was found to
remove. Porting a second disconnected demonstration runtime would duplicate
the existing Rust framework constants without a dependency-closed SQL-server
test harness, so no speculative implementation was added.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The exact
Go-master package suite passed in the detached worktree:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/example -count=1
# ok github.com/pingcap/tidb/pkg/dxf/example 6.183s
```

The current shared worktree also has an unrelated in-progress
`pkg/lightning/mydump` parser edit; its package build currently fails at
`pkg/lightning/mydump/view_import.go:279` (`undefined: ast.Walk`), so that
working-tree result is not attributed to this example package. Ready repository
gates for this receipt batch are `cargo +nightly-2026-08-22 fmt
--manifest-path rust/Cargo.toml --all -- --check`, `make lint`, and
`git diff --check`. No Go source, import section, test, Bazel target, or module
dependency changed, so `make bazel_prepare` is not required.

The remaining risk is documentation/example drift: changes to DXF factory
registration or task/step metadata must keep the Go example and its single
flaky integration test aligned. Rust framework enum parity does not imply a
Rust application harness at this boundary.
