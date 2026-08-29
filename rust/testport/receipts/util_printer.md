# `pkg/util/printer` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full: `printer.go`,
`printer_test.go`, `main_test.go`, and `BUILD.bazel`. There is no package doc,
README, fixture, benchmark, generated or platform variant, or ownership file.
The local Go package is byte-identical to the pin.

Production behavior consists of classic/next-generation release rendering,
structured startup identity and configuration logging, textual version
identity, and byte-width ASCII-table rendering. The source tests contain
exactly `TestPrintResult`, `TestGetTiDBInfo`, and `TestPrintTiDBInfo`; the test
main only installs the common TiDB test environment and leak checker.

## Rust ownership and audit result

`rust/crates/tidb-util/src/printer.rs` is the production owner. Build and
runtime globals are carried in the server's immutable `VersionInfo` snapshot,
and the already-serialized effective configuration is passed by the startup
owner. `get_print_result_bytes` is the native representation of Go strings'
arbitrary-byte domain; `get_print_result` is the UTF-8 convenience boundary
used by Rust callers. Both use byte lengths and preserve the source output.

The audit removed the supplemental invalid-byte test, Unicode-width branch,
and separate deploy-mode test, then consolidated validation into the source's
three test cases. Both classic and next-generation logging shapes now run
through `print_tidb_info`, including Go's default premium-mode behavior when a
next-generation snapshot has no explicit mode.

## Validation

Profile: WIP; this is one completed package in the continuing package-by-
package audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/printer` — passed.
- `go test ./pkg/util/printer -run '^(TestPrintResult|TestGetTiDBInfo|TestPrintTiDBInfo)$' -count=1` — blocked before package execution by the existing `google.golang.org/grpc/internal/transport` reference to missing `http2.TrailerPrefix`.
- `cargo test -p tidb-util printer::tests --lib --locked` — passed (2 source-mapped unit tests).
- `cargo test -p tidb-util --test printer_contract --locked` — passed (1 source-mapped integration test).
- `cargo test -p tidb-util --locked` — passed (642 unit tests, 3 ignored helpers, all integration and doc tests).
- `cargo fmt --all --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: production behavior is unchanged; the real startup logging
  path now carries the exact source-owned field assertions.
- Compatibility: the raw-byte production API remains because it represents
  Go's string domain; only supplemental Rust test cases were removed.
- Performance: unchanged.
