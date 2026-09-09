# `pkg/tablecodec` — Go-master parity audit

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The root package has exactly six Go artifacts and 3,277 lines. Every
production, test, benchmark, test harness, owner, and Bazel file was read in
full before the parity decision. The nested `pkg/tablecodec/rowindexcodec`
directory is a separate package and is not included here.

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 58 | library/test targets and dependency rows |
| `OWNERS` | 5 | approver metadata |
| `tablecodec.go` | 2,094 | table keys, rows, index values, and temp-index codecs |
| `tablecodec_test.go` | 988 | key, row, index, handle, and value regressions |
| `bench_test.go` | 98 | six key/handle benchmarks and daily harness |
| `main_test.go` | 34 | common test setup and goleak bootstrap |

The source has 97 production functions, 26 `Test*` functions (including
`TestMain` and `TestBenchDaily`), and nine benchmarks. It has no package
`doc.go`, generated Go source, platform-specific variant, or fixture tree.
The Bazel metadata names one production file and the three test files above.

Relative to `origin/hparser-integration`, Go master changes only the OWNERS
metadata and the row API callers: `EncodeRow` and `EncodeOldRow` no longer
accept a `codec.Encoder`, and the old-row path calls package-level
`codec.EncodeValue`. The V2 keyspace metadata test also follows the current
protobuf constructor shape. `GenIndexKey` still intentionally accepts a
key-only encoder, matching Go's live API.

## Rust ownership and parity decision

The Rust owner is `rust/crates/tidb-tablecodec`: nine tracked files and 4,158
lines, including `table_index.rs`, `table_row.rs`, `table_key` re-exports,
the aggregate test harness, source-derived tests/benchmarks, and the
`index_prefix_truncation.hex` fixture. Its row owner already exposes free
`encode_table_row` and `encode_old_table_row` functions with no encoder
argument. The key/index owner retains `Encoder` only where Go retains
collation-aware comparable-key encoding.

No Rust production edit is required for this Go-master delta. The existing
source-derived suite already covers the two row portals, old/new layouts,
handles, restored values, keyspace behavior, V2 keys, and all current index
value variants. The removed Go argument is therefore recorded as a caller
surface cleanup rather than carried as a Rust-only wrapper.

## Rust follow-up: Go-discardable tablecodec returns

The dependency-closed owner carried 43 explicit Rust `#[must_use]` diagnostics
on Go-shaped table-key, row-index, and table-index APIs. Go callers may discard
all of these return values, so the diagnostics were Rust-only and did not
represent a source contract. They were removed from `tidb-codec`'s table-key
and row-index carriers and from `tidb-tablecodec`'s table-index owner without
changing encoded bytes, key classification, handle representation, or index
value behavior.

The source-derived regression
`tablecodec_package_source::return_values_may_be_ignored_like_go` discards all
43 affected API families under `#[deny(unused_must_use)]`. On the pre-fix
owner it failed to compile with 43 unused-return errors; the fixed test passes.

Ready validation for this follow-up:

- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-tablecodec --test all tablecodec_package_source::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-tablecodec --test all -- --test-threads=1` — passed.
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-tablecodec --all-targets` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml -p tidb-tablecodec -- --check`, repository `make lint`, and `git diff --check` — passed.

No Go, generated, fixture, platform, Bazel, or module artifact changed, so
`make bazel_prepare` is not required.

## Validation

Profile: Ready for this package audit; the repository-wide loop remains in
progress.

- Complete Go-master artifact/function inventory and source diff — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/tablecodec -count=1` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-tablecodec --test all -- --test-threads=1` — 55 passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, repository `make lint`, and `git diff --check` — passed for the batch.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: low; the Rust row portals already have the current free
  function shape and their complete source-derived suite passes.
- Compatibility: the removed encoder parameter is a Go-master API cleanup;
  no Rust consumer needs a compatibility shim.
- Performance: unchanged; no production Rust code was modified.
