# `pkg/dxf/framework/mock/execute` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly two tracked artifacts and 253 lines. Every file
was read in full in a detached worktree at the pinned Go commit before this
receipt was written. There is no `doc.go`, `OWNERS`, test file, fixture,
benchmark, fuzz target, generator input, or platform-specific variant. The
single Go source is generated MockGen output and is the package's complete
implementation.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 14 | `c6dfb439a560a539170e4bd39b30036c0aade380` | `d8f93130097d2b61fd4594370f08cae792f7ac72384e58e8161e234b8b8eab94` | public Bazel library exposing the generated StepExecutor mock and its DXF, metering, execute, and GoMock dependencies |
| `execute_mock.go` | 239 | `f29afeeace9ba3f43e013e42f3cc28cf7b864527` | `cecf17ae34374fd067b4fafbdad7ca1995dc109e9ef7b312527a2bf1386d5b5e` | MockGen implementation of `taskexecutor/execute.StepExecutor` |

The generated source contains the complete `MockStepExecutor` and recorder
surface: constructor, `EXPECT`, `ISGOMOCK`, forwarding and recorder methods for
`Cleanup`, checkpoint access/update, metering/resource/step access, `Init`,
summary/reset, resource and task metadata mutation, subtask execution, and the
unexported `restricted` marker. Each method forwards to GoMock with the exact
`StepExecutor` contract, preserving context, proto, metering, and summary
types. There are no package-local tests; parent executor and ImportInto tests
consume this generated seam.

## Rust ownership and parity decision

Rust has no dependency-closed owner for this generated GoMock package. The
nearby `tidb-dxf` code owns generic task/resource/step data but does not expose
the Go `StepExecutor` lifecycle, GoMock controller, or recorder contract. No
Rust-only StepExecutor mock behavior or ignored test was found to remove.
Adding a disconnected Rust mock would be speculative, so this complete
generated-support package remains an explicit Go-only boundary.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The complete
Go-master package compile probe passed with no test files:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework/mock/execute -count=1 -run '^$'
# ? github.com/pingcap/tidb/pkg/dxf/framework/mock/execute [no test files]
```

Ready repository gates for this receipt batch are
`cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`,
`make lint`, and `git diff --check`. No Go source, import section, test,
Bazel target, or module dependency changed, so `make bazel_prepare` is not
required. Rust tests and a full workspace build are not run because no Rust
source or owning target changed.

The remaining risk is generated-code drift: any future `StepExecutor` interface
change must regenerate this MockGen output. Execution semantics and regression
coverage remain owned by the parent packages that consume the mock.
