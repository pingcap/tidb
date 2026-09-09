# `pkg/util/partialjson` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full: `extract.go`,
`extract_test.go`, and `BUILD.bazel`. There is no package doc, benchmark,
fixture, generated or platform variant, README, or ownership file. The local
Go package is byte-identical to the pin. Bazel exposes the same source once as
the historical `fastjson` alias and once as `partialjson`.

Production behavior includes top-level object validation, decoder-number
preservation, ordered tokens for scalar and compound values, discarded
unrequested members, first-occurrence handling for duplicate requested names,
early termination after all names are found, empty-name short circuiting, and
EOF/error behavior before the last requested member. The package has exactly
one source test, `TestIter`.

## Rust ownership and audit result

`rust/crates/tidb-util/src/partialjson.rs` owns the complete package. The audit
replaced its non-Go `RawValue` public result with `JsonToken`, the native enum
for Go's delimiter, string, number, boolean, and null token types. Numbers
retain their original lexical representation exactly as Go `UseNumber` does.
The existing lossy UTF-8 and unpaired-surrogate normalization remains the
native equivalent of Go `encoding/json` string decoding.

The four Rust tests were reduced to the single Go-owned iterator test.
`tidb-meta` now consumes the returned token counts, variants, and positions
directly in `FastUnmarshalTableNameInfo` and
`ExtractSchemaAndTableNameFromJob`; its separate raw-JSON reparsing and custom
name-object deserializer were removed.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/partialjson` — passed.
- `cargo check -p tidb-util -p tidb-meta --locked` — passed; existing model
  warnings remain outside this change.
- `cargo test -q -p tidb-util partialjson::tests::test_iter --lib --locked -- --exact --test-threads=1` — passed (the one source-owned test).
- `cargo test -q -p tidb-meta --locked -- --test-threads=1` — passed: 58
  integration tests passed, 4 were ignored, and the doc test passed.
- `cargo test -q -p tidb-util --locked -- --test-threads=1` — passed: 610
  unit tests passed, 3 were ignored, and every integration and doc test
  passed.
- Targeted metadata tests `source_range_partial_json_and_filter_boundaries`,
  `table_name_regex_and_fast_decoder_cover_go_escape_vectors`, and
  `job_name_filter_keeps_go_operator_precedence` all passed under the
  `tidb-meta` `all` test target.
- `cargo fmt --all` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: the owner and all known consumers compile and their complete
  serial suites pass; token order/type assertions now match Go directly.
- Compatibility: the repository-unused raw-value return type is intentionally
  replaced by the Go-shaped token API.
- Performance: selected values are tokenized once instead of being returned
  raw and reparsed by each metadata consumer; early-stop behavior is retained.
