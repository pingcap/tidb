# `pkg/util/serialization` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full: `common_util.go`,
`serialization_util.go`, `deserialization_util.go`, and `BUILD.bazel`. There
is no package doc, test, benchmark, fixture, generated or platform variant,
README, or ownership file. The local Go package is byte-identical to the pin.

Production behavior is same-architecture in-memory serialization for every
primitive, decimal, time, duration, JSON, enum, set, opaque, string, byte
buffer, and supported interface value. Widths and byte order are native. The
decoder advances a positional cursor without validating row width, trailing
bytes, boolean domain, decimal fields, or time fields. Unknown interface tags
panic with the source message.

## Rust ownership and audit result

`rust/crates/tidb-util/src/serialization.rs` owns the complete package.
`Cursor` is the borrowed native equivalent of Go `PosAndBuf`; callers provide
one cell's bytes directly instead of a Go `chunk.Column` and row index. Rust's
closed `InterfaceValue` replaces Go's supported `any` type switch without
adding a wire tag or value family.

The audit removed `SerializationError` and four supplemental package tests,
because the Go package has neither recoverable deserialization errors nor any
tests. It also removed downstream exact-width, trailing-byte, and malformed
boolean recovery from COUNT/AVG/SUM, FIRST_ROW, and GROUP_CONCAT spill paths.
These consumers now decode directly and panic through bounds checks on invalid
input, as Go does. `MyDecimal::from_raw_bytes_like_go` and
`Time::from_go_raw_like_go` preserve the source's unchecked raw field values
instead of introducing validation during deserialization.

The re-audit also removed Rust-only `must_use`, clone/debug/equality behavior,
the unused `Cursor::remaining` and `MY_DECIMAL_LEN` APIs, and public exposure
of the source-private buffer helpers. The FIRST_ROW consumer now calls the
exported `SerializeString`/`DeserializeString` equivalents, matching Go's
package boundary. `Cursor::position` remains the native accessor for Go's
public `PosAndBuf.Pos` field.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

Passed:

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/serialization`
- `cargo test --offline --locked -p tidb-util --lib serialization` (the
  source package has no tests; the Rust package also runs none)
- `cargo test --offline --locked -p tidb-exec partial_result4_count_spill_round_trip_source_values`
- `cargo test --offline --locked -p tidb-exec avg_and_sum_spill_pairs_round_trip_all_original_vectors`
- `cargo test --offline --locked -p tidb-exec source_spill_layout_has_no_rust_only_type_or_collation_tags`
- `cargo test --offline --locked -p tidb-exec base_partial_spill_matches_source_native_shape`
- `cargo check --offline --locked -p tidb-exec --lib`
- `rustfmt --edition 2021 crates/tidb-util/src/serialization.rs
  crates/tidb-exec/src/first_row.rs`
- `git diff --check`

`GOCACHE=/private/tmp/tidb-go-build-cache go test
./pkg/util/serialization -count=1` did not reach this package: the existing
checkout fails first in `pkg/util/hack` (`checkMapABI` is undefined) and the
gRPC transport dependency (`http2.TrailerPrefix` is undefined).

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: scoped Rust compilation and spill round-trip checks pass;
  the local Go test command is blocked before package execution as recorded
  above.
- Compatibility: Rust-only recoverable malformed-spill APIs are intentionally
  removed; valid source-shaped bytes retain their exact layout.
- Performance: removing validation and result mapping makes deserialization
  follow the source's direct native reads.
