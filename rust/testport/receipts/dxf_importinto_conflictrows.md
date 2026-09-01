# `pkg/dxf/importinto/conflictrows` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly four tracked artifacts and 872 lines. Every file
was read in full in a detached worktree at the pinned Go commit before this
receipt was written. There is no `doc.go`, `OWNERS`, fixture, benchmark,
generated source or generator input, platform-specific variant, or other
checked-in artifact in the package.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 39 | `562b7123ed3d9ad746d37c25a35aa2123fee56df` | `983b1b0a6da37321821173d691a0d18c6bc9ed1c18ea5887c559e7ef1b1cbc3b` | public Go library and four-shard flaky package test target with its complete dependency closure |
| `cleanup.go` | 268 | `2d3fe39e9dc803db9eebe8e36ffaaf4dbaf9d884` | `e1fbf2c5692f4dd9fa6cf9124ff1cc00ea48c9500630b0efee1d0fe4d44e08ef` | retention policy, task-ID parsing and metadata lookup, bounded batches and diagnostics, object deletion, and cloud-store entry point |
| `cleanup_test.go` | 529 | `b35aab9c84e82e482f82c59c3a8d692dafd6fb37` | `d484d0c4aeb8dfd9a9baca6d0f9490a3ae41bc6d17b46891ba3eac7f11597930` | four top-level tests and 38 leaf cases covering paths, retention boundaries, batching, retries, logging, cancellation, storage opening, and credential secrecy |
| `path.go` | 36 | `32e8850547eb939f54c12ea38e9c775fae5ebb39` | `11510af013cfbe743d38d97ffde604a219a737a11157a4d4a5a4c5cdc611a986` | conflict-row storage namespace and UUID-bearing task/subtask file-prefix constructor |

The production inventory contains all 13 functions and methods: the bounded
sample append/record/merge helpers; cleanup-stat diagnostic methods; strict
positive-decimal task-ID parsing; task-state/type retention decision; batched
walk, metadata lookup, deletion, logging, and retry-safe accounting; the
cloud-storage entry point; and unique task/subtask file-prefix creation.

The complete test inventory is `TestParseTaskID` (17 path cases),
`TestShouldDelete` (seven state/time/type cases), `TestCleanFiles` (metadata
diagnostics, empty logging, mixed decisions, both batch bounds, lookup/delete
failure retries, bounded samples, malformed paths, and cancellation), and
`TestCleanConflictRowFiles` (empty URI and credential-safe open failure).

## Rust ownership and parity decision

Rust's `tidb-dxf` crate owns only the `IMPORT INTO` task and step vocabulary.
The workspace has no dependency-closed owner for DXF task cleanup metadata,
the importer sort store, object-storage walking/deletion, conflict-row file
production, cleanup scheduling, or the structured operational log. SQL parser
and session support for the `IMPORT INTO` statement do not make those storage
and lifecycle contracts executable.

No Rust implementation, ignored test, or receipt claimed this package's
cleanup behavior, so there is no Rust-only behavior to remove. Adding only the
path parser, seven-day constant, or UUID name builder would create a detached
second policy without a producer or cleanup caller. No speculative facade was
added; the complete package remains an explicit dependency boundary.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. The exact
Go-master package suite passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/importinto/conflictrows -count=1
# ok github.com/pingcap/tidb/pkg/dxf/importinto/conflictrows 1.054s
```

Repository formatting, lint, and diff hygiene were also run for this receipt
batch (`cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all
-- --check`, `make lint`, and `git diff --check`). No Go source, import
section, test, Bazel target, or module dependency changed, so
`make bazel_prepare` is not required. Rust tests and a full workspace build
were not run because no Rust source or owning target changed. Rust still does
not verify conflict-row retention, cleanup batching/retry behavior, object
storage interoperability, operational logging, or credential secrecy; this
receipt records that risk rather than claiming package parity.
