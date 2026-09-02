# Rust `tidb-domain` RU-statistics GC boundary receipt

Status: bounded Rust-only alignment batch; this receipt does not claim that
the complete Go `pkg/domain` package has been transcreated.

Comparison source: Go `origin/master` at
`a85e0fd5dfa914e73eed97f17af584061252bc3c` (2026-09-02). The source contract
is `pkg/domain/ru_stats.go:238-253`: `GCOutdatedRecords` consumes the first
row of the `count(*)` result with `rows[0].GetInt64(0)` and therefore panics
when an impossible empty result is returned.

## Complete bounded inventory

Before editing, the direct `pkg/domain` boundary was enumerated against the
fetched Go tree: 31 artifacts and 9,140 lines — 16 production files, 13 test
files, `BUILD.bazel`, and `OWNERS`. Nested `pkg/domain/{crossks,infosync,
serverinfo}` directories are separate package boundaries. There are no
direct fixtures, generated outputs, or platform-specific files in this root
boundary.

The Rust owner inventory contains all 17 tracked artifacts under
`rust/crates/tidb-domain` (the manifest plus 16 Rust modules, 10,193 lines).
The owner has no generated or platform-specific source; unit tests are
in-module, including the existing `ru_stats` scripted-dependency suite. The
shared Cargo build script and workspace lockfile were inspected as build
inputs and were not changed.

## Alignment

`RuStatsDeps::query_single_count` continues to return `Option<i64>` so an
implementor can distinguish an absent row from a legitimate zero count. The
writer now directly `.expect`s that option, matching Go's unchecked
`rows[0]` access. The Rust-only `RuStatsError::MissingCountRow` variant and
its fabricated recoverable-error path were removed. The existing missing-row
regression now asserts the Go-compatible panic and verifies that only the
count statement was issued; normal zero-count and delete-error behavior is
unchanged.

## Baseline and validation

The focused regression was applied to a clean pre-fix `2990ecfcc7` worktree
and failed because the old writer returned `Err(MissingCountRow)` rather than
panicking. The same test passes after the change.

Profile: Ready for this bounded Rust package batch.

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all` —
  passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-domain --lib ru_stats::tests::gc_panics_on_a_missing_count_row_like_go -- --exact --nocapture` — focused panic regression passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-domain --all-targets` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`, the repository `make lint` Ready gate, and `git diff --check` are run before commit; their results are recorded with the package commit.

## Risks and boundaries

- Correctness: an empty result from `SELECT count(*)` is malformed for the
  SQL contract; matching Go exposes it as a panic instead of silently
  suppressing garbage collection.
- Compatibility: callers that intentionally relied on the Rust-only
  `MissingCountRow` error must now handle the source-compatible panic. The
  Rust crate is a seed owner and has no in-repository callers of that variant.
- Performance: no normal-path allocation or query behavior changed.
- Unverified: the real session-pool/Domain wiring and the rest of `pkg/domain`
  remain outside this bounded owner; no integration test can exercise those
  absent dependencies locally.
