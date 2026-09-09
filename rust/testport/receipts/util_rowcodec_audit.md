# `pkg/util/rowcodec` — Go-master parity audit

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package has exactly nine Go artifacts and 3,144 lines. All production,
test, benchmark, harness, and Bazel artifacts were read in full before
editing.

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 56 | library/test targets and dependency rows |
| `common.go` | 374 | row framing, compact integers, checksum helpers |
| `decoder.go` | 544 | datum/chunk/byte decoders and old-row conversion |
| `encoder.go` | 264 | typed row encoder and checksum policies |
| `row.go` | 333 | row layout, column lookup, and checksum state |
| `common_test.go` | 67 | keyspace-prefix regression |
| `rowcodec_test.go` | 1,317 | typed rows, defaults, checksums, and edge cases |
| `bench_test.go` | 124 | checksum/encode/decode benchmarks and daily harness |
| `main_test.go` | 65 | common setup, leaktest, and test-only old-row adapter |

The package has 73 production functions, 17 test/harness functions, and four
benchmarks. It has no `doc.go`, generated Go file, platform-specific variant,
or fixture tree. The Go-master diff only removes `codec`/`collate` imports and
passes the new no-encoder `tablecodec.EncodeOldRow` signature in the benchmark
and two source tests.

## Rust ownership and parity decision

The dependency-closed owner is the typed row boundary in
`rust/crates/tidb-codec/src/rowcodec.rs`, with raw framing in
`row_encoder.rs`, `row_decoder.rs`, and `row_layout.rs`. The aggregate test
build script generates `OUT_DIR/all_tests.rs`; no target-specific variant is
present. The source-derived `rowcodec_package_source` module covers all Go
rows plus the malformed-input, non-UTF-8, old-row, checksum, and commit-TS
contracts.

No Rust production edit is required: the owner already uses free row/value
functions and has no obsolete encoder argument. This package's Go-master
changes are downstream caller cleanup in `pkg/tablecodec` and its own tests,
not a new rowcodec behavior. No redundant Rust-only adapter or duplicate
regression was added.

## Validation

Profile: Ready for this package audit; the repository-wide loop remains in
progress.

- Complete Go-master artifact/function inventory and source diff — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/util/rowcodec -count=1` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-codec --test all rowcodec_package_source -- --test-threads=1` — 27 passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, repository `make lint`, and `git diff --check` — passed for the batch.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Rust follow-up: Go-discardable rowcodec returns

The dependency-closed owner carried three explicit Rust `#[must_use]`
diagnostics on the direct Go-shaped `is_new_format`, `is_row_key`, and
`field_type_from_column` helpers. Go permits callers to discard these return
values, so the diagnostics were Rust-only and are now removed without changing
row framing or field metadata behavior.

The focused regression
`rowcodec_package_source::return_values_may_be_ignored_like_go` discards all
three results under `#[deny(unused_must_use)]`. On the pre-fix owner it failed
to compile with three unused-return errors; the fixed test passes.

Ready validation for this follow-up:

- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-codec --test all rowcodec_package_source::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-codec --test all -- --test-threads=1` — passed.
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-codec --all-targets` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml -p tidb-codec -- --check`, repository `make lint`, and `git diff --check` — passed.

No Go, generated, fixture, platform, Bazel, or module artifact changed, so
`make bazel_prepare` is not required.

## Risk

- Correctness: low; no rowcodec production path changed and all source-derived
  row contracts pass.
- Compatibility: the removed `codec.Encoder` caller argument is reflected in
  the current Go API and does not alter encoded bytes.
- Performance: benchmark source remains mapped; no runtime code changed.
