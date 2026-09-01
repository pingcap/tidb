# `pkg/dxf/framework/scheduler/mock` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

This nested package contains exactly two tracked artifacts and 173 lines.
Every file was read in full in a detached worktree at the pinned Go commit
before this receipt was written. It has no `doc.go`, tests, fixtures,
`testdata`, platform variants, benchmark/fuzz targets, generator inputs, or
`OWNERS` file. The parent `pkg/dxf/framework/scheduler` package is a separate
unit and is inventoried in `dxf_framework_scheduler.md`.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 13 | `4a51e2e542e62f801d1c0fd1a58ae078974be08e` | `3df27e8d438e3d16ec8cfc313741b1b8914a251df769cac04b4d0de79755388c` | public generated mock library and proto/storage/GoMock dependencies |
| `scheduler_mock.go` | 160 | `781535e4907d1aec037e6e07c4a58411b3dc793b` | `ca88d5455ef3c3a124f638a5d33185455a6390cb257de5d918eba904dcd69453` | MockGen implementation of scheduler `Extension` |

The generated source contains 19 function declarations: constructor,
`EXPECT`, `ISGOMOCK`, and forwarding/recorder pairs for `OnTick`,
`OnNextSubtasksBatch`, `OnDone`, `GetEligibleInstances`, `IsRetryableErr`,
`GetNextStep`, `OnPrepare`, and `ModifyMeta`. Every call preserves the exact
context, storage handle, task/proto, step, and metadata signatures of the
parent scheduler interface. There are no package-local tests; scheduler,
testutil, and manager tests consume this generated seam.

## Rust ownership and parity decision

Rust's `tidb-dxf` owns generic task/step/resource values but no Go scheduler
`Extension` lifecycle or GoMock recorder/controller contract. No Rust-only
scheduler mock behavior or ignored test was found to remove. Adding a
disconnected Rust mock would be speculative, so this generated-support package
remains an explicit Go-only boundary.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The exact
Go-master compile probe passed with no tests:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework/scheduler/mock -count=1 -run '^$'
# ? github.com/pingcap/tidb/pkg/dxf/framework/scheduler/mock [no test files]
```

Ready repository gates for this receipt batch are
`cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`,
`make lint`, and `git diff --check`. No Go source, import section, test,
Bazel target, or module dependency changed, so `make bazel_prepare` is not
required. Rust tests and a full workspace build are not run because no Rust
source or owning target changed.

The remaining risk is generated-code drift: any `scheduler.Extension` contract
change must regenerate this mock before parent scheduler tests compile.
