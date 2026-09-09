# `pkg/dxf/importinto/mock` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly two tracked artifacts and 74 lines. Every file
was read in full in a detached worktree at the pinned Go commit before this
receipt was written. There is no `doc.go`, `OWNERS`, test file, fixture,
benchmark, fuzz target, generator input, generated companion, or
platform-specific variant beyond the generated mock source itself.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 13 | `a1d925d2f29b4feba6aef2105818f1f5b5d8735d` | `8c76dd17d9eb1bc0ebfbaa818ae3d114b976ede4ee65b27d2c51da0e3f789516` | public Bazel library exposing the generated mock and its execute/backend/gomock dependencies |
| `import_mock.go` | 61 | `a5ee61c0fff094c401d8c9acfea7c05e49f189b5` | `8cd22d9cf8d9e551f809cf7a182216a40d1c9a508af3b30ea2ac12c60e55f692` | MockGen output for the parent package's `MiniTaskExecutor` interface |

The generated source contains the complete `MockMiniTaskExecutor` lifecycle:
constructor, `EXPECT`, `ISGOMOCK`, the `Run` forwarding method, and recorder
registration. It delegates calls to GoMock with the exact Go interface
signature (`context.Context`, two Lightning `backend.EngineWriter` values, an
`execute.Collector`, and an error result). There are no package-local tests;
the generated mock is exercised by the parent `pkg/dxf/importinto` tests,
especially encode-and-sort operator tests.

## Rust ownership and parity decision

Rust has no dependency-closed owner for this generated GoMock package. The
nearby `tidb-dxf` code owns runtime task/step concepts, while Rust test mocks
under TiKV, SQL execution, and DDL crates implement unrelated traits and do
not provide the ImportInto `MiniTaskExecutor` contract or GoMock recorder
semantics. No Rust-only ImportInto mock behavior or ignored test was found to
remove. Adding a standalone Rust mock would not connect to the parent Go
operator's writer and collector interfaces, so no speculative port was added;
the complete generated-support package remains an explicit Go-only boundary.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The complete
Go-master package compile/test command passed with no test files:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/importinto/mock -count=1 -run '^$'
# ? github.com/pingcap/tidb/pkg/dxf/importinto/mock [no test files]
```

Ready repository gates for this receipt batch passed:
`cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`,
`make lint`, and `git diff --check`. No Go source, import section, test,
Bazel target, or module dependency changed, so `make bazel_prepare` is not
required. Rust tests and a full workspace build are not run because no Rust
source or owning target changed.

The remaining risk is integration-only: the generated recorder must stay in
lockstep with any future `MiniTaskExecutor` interface change, and the parent
Go operator tests—not this package—own writer/collector behavior. Rust has no
equivalent dependency-closed test seam or parity implementation at this
boundary.
