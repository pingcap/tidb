# `pkg/util/rowDecoder` — Go-master parity audit

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package has exactly four Go artifacts and 499 lines. Every production,
test, harness, and Bazel file was read in full before editing.

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 52 | library/test targets and dependency rows |
| `decoder.go` | 205 | schema-aware row decode/default/generated evaluation |
| `decoder_test.go` | 209 | integer/common-handle and generated-column tests |
| `main_test.go` | 33 | common setup and goleak bootstrap |

The production surface has six functions/methods and the tests have two
behavior tests plus `TestMain`. There is no package `doc.go`, benchmark,
fixture, generated source, or platform-specific Go variant. Go master only
removes the now-unused `pkg/util/codec` test dependency and updates its two
`tablecodec.EncodeRow` callers to the no-encoder API.

## Rust ownership and parity correction

The Rust owner is `rust/crates/tidb-executor/src/kv_table/row_decoder.rs`,
with 887 lines and 11 focused source tests in
`tests/row_decoder_source.rs`. It is the dependency-closed equivalent of the
Go decoder for stored rows, defaults, generated columns, handle columns,
column-type-change phases, and projected/point reads.

The audit found one real mode boundary hidden by the Rust V2 fast path. The
typed rowcodec handle decoder always applies the new-collation
restored-data policy. When a Rust table explicitly uses old-collation mode
and has a common handle, that fast path skipped the handle component and
returned NULL, while Go's `NeedRestoredDataWithCollate`-sensitive path
materializes it. `RowDecoder::build` now keeps the V2 fast path for integer
handles and new-collation tables, but routes old-collation common handles
through the existing map path. The focused source regression is named
`restored_common_handle_value_wins_over_its_lossy_sort_key_even_when_fast_path_is_disabled`.

## Validation

Profile: Ready for this package batch; the repository-wide loop remains in
progress.

- Complete Go-master artifact/function inventory and source diff — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/util/rowDecoder -count=1` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-executor --test all row_decoder_source -- --test-threads=1` — 11 passed, including the old-collation regression.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, repository `make lint`, and `git diff --check` — passed for the batch.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: low; only the old-collation common-handle V2 shape loses the
  direct typed fast path, and it now follows the already-tested map path.
- Compatibility: new-collation and integer-handle fast paths are unchanged;
  old-collation values now match Go's materialized handle semantics.
- Performance: a narrowly scoped fallback adds map allocation only for old
  collation plus common handles, the case where the prior fast path was
  semantically incorrect.
