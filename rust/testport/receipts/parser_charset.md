# Complete `pkg/parser/charset` package

## Authority and inventory

- Go authority: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (`origin/master`).
- The checkout's `pkg/parser/charset` is byte-identical to that authority.
- Atomic inventory: 14 files and 3,319 Go/Bazel lines: 10 production files,
  the generated GB18030 data
  input, 2 original test files, and `BUILD.bazel`.
- Go executable inventory: 9 `Test*` functions and
  `BenchmarkGetCharsetDesc`; the manifest's 10-function count is exact.

The Rust owner is `tidb-datatype`. The exact Go simple-rune Unicode table is
owned by the dependency-leaf `tidb-mysql` crate, matching the Go package's
dependency on `parser/mysql` and the standard `unicode` table.

## Production mapping

- `charset.go` maps to `src/charset.rs` and the generated
  `src/charset_data/{known_charsets,collations}.rs` catalogs.
- `encoding.go`, the base, binary, ASCII, Latin-1, UTF-8, GBK, and GB18030
  implementations map to `src/encoding_base.rs`, `ascii_encoding.rs`,
  `utf8_encoding.rs`, and `multibyte_encoding.rs`.
- `encoding_gb18030_data.go` and both source special-case tables map through
  `scripts/generate-parser-charset.py` to the four generated GBK/GB18030
  mapping files under `src/charset_data/`.
- `encoding_table.go` maps through the same generator to
  `src/encoding_labels.rs`; `src/encoding_table.rs` supplies the source lookup
  and codec behavior.
- Both Go test files map to
  `tests/parser_charset_package_source.rs` plus leaf regression tests. The Go
  benchmark maps to the `parser_charset` benchmark target.
- No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Closed gaps

- Encoding upper/lower conversion now uses Go's Unicode 15 simple-rune
  mappings instead of Rust full mappings that expanded `ß` to `SS`.
- Registry, collation, and HTML encoding-label normalization use Go simple
  lowercase; encoding labels also use Unicode `strings.TrimSpace` rather than
  an ASCII-only trim policy.
- GB18030 `MbLen` retains Go's short-input return for fewer than two bytes and
  its observable bounds panic for truncated two/three-byte four-byte prefixes.
- The exported TiFlash-supported charset set is present and exact.
- `RemoveCharset` now preserves Go's original-length range/delete behavior,
  including its name comparison and mutation edge cases.

## Validation

Ready profile was used because this receipt updates the complete atomic
package boundary while the repository-wide parity campaign continues.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./charset -count=1` from `pkg/parser` — all source
  package tests passed after the pinned nested-module dependencies were made
  available. The package has no failpoint imports or injections.
- `python3 scripts/generate-parser-charset.py` from `rust` followed by a clean
  generated-table diff — all generated images match the pinned Go sources.
- `cargo +nightly-2026-08-22 test -p tidb-datatype --test all parser_charset -- --test-threads=1` — 11 source-derived tests passed.
- `cargo +nightly-2026-08-22 test -p tidb-datatype --lib encoding -- --test-threads=1` — 21 encoding tests passed.
- `cargo check -p tidb-datatype --benches --locked` — benchmark compiled.
- `cargo check -p tidb-ast -p tidb-protocol -p tidb-expr --lib --locked` — all
  immediate encoding consumers compiled.
- `cargo check -p tidb-executor -p tidb-exec --lib --locked` — both execution
  layers compiled; only pre-existing warnings were emitted.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, pinned-Go `make lint`, and
  `git diff --check` — clean.

No live TiKV/TiFlash service behavior was exercised; this package has no such
direct dependency.
