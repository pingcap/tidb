# `pkg/ttl/sqlbuilder` parity receipt

Status: Ready for this scoped batch. This receipt covers the complete Go
package inventory; it is not a repository-wide parity claim.

Published commit: `8bf78c07e0b82a8738a1e8e5cd1e222a1c032fd3` on
`origin/hparser-integration`; local, tracking, and `git ls-remote` SHAs were
verified equal after the push/pull.

Comparison source: Go `origin/master` at `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
Rust owner: `rust/crates/tidb-ttl/src/sql_builder.rs` and
`rust/crates/tidb-ttl/tests/sql_test.rs`.

## Complete Go inventory

All four tracked artifacts in `pkg/ttl/sqlbuilder` were read in full before
editing: 1,510 lines total. The package has no `doc.go`, fixture or `testdata`
directory, generated source/input, platform/build-tag variant, benchmark, fuzz
target, README, or ownership artifact.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 46 | library/test targets and five-way sharding |
| `main_test.go` | 34 | common test setup and goleak options |
| `sql.go` | 482 | datum formatter, SQL builder, scan generator, delete builder |
| `sql_test.go` | 948 | escaping, formatter, builder, scan, and delete regressions |

The Go production source and BUILD metadata are byte-identical to current Go
master. The Rust owner source (612 lines) and source-derived test (719 lines)
were also read in full; the existing Rust tests cover six source-shaped cases
(`test_escape`, `test_format_sql_datum`, `test_sql_builder`,
`test_scan_query_generator`, `test_build_delete_sql`, and helpers).

## Parity finding and implementation

Go's `ScanQueryGenerator.setStack` distinguishes a `nil` continuation key from
a non-`nil` empty datum slice. A `nil` key selects `keyRangeStart`; an empty
non-`nil` key clears the stack and leaves the configured range start unused.
The Rust owner filtered empty slices into the `None` path, so a result row with
no key datums incorrectly resumed at the range start. `set_stack` now preserves
that distinction exactly.

The Rust owner retains two explicit, dependency-shaped boundaries from the
existing implementation: Go's arbitrary-byte non-binary text output cannot be
represented by the Rust `String`/`RestoreWriter` API, and `ValueExpr`/AST
round-trip validation requires the missing parser-driver node. No lossy
replacement or speculative parser facade was introduced.

## Focused regression coverage

`test_scan_query_generator_preserves_empty_continuation_key` first verifies a
configured `[1,100)` range and then passes three rows whose last key is a
non-`nil` empty vector; it asserts the resumed SQL contains only the upper
bound. Before the fix the test failed with `id > 1`; after the fix it passes
with the Go-shaped `id < 100` query.

## Validation

Profile: **Ready**.

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-ttl --all-targets`
- `OPENSSL_DIR=... DYLD_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-ttl --test sql_test -- --test-threads=1` — 6 passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/ttl/sqlbuilder -count=1` — passed (1.853s).
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — Ready gate.
- `git diff --check` — passed.

No Go source, import section, test target, or Bazel file changed, so
`make bazel_prepare` is not applicable to this Rust-only fix.

## Risks and unverified scope

- Correctness risk is limited to continuation-state handling; the focused
  regression exercises the exact nil/empty distinction and the complete Rust
  SQL-builder suite remains green.
- Compatibility risk is confined to callers that pass an empty continuation
  key; they now match Go and no longer restart at the lower bound.
- Performance is unchanged; the fix removes only an empty-slice filter and
  avoids an incorrect extra predicate.
- Not verified locally: live Rust/Go SQL exchange, arbitrary invalid UTF-8
  text formatting behind the existing writer boundary, non-host platforms, and
  repository-wide integration suites.

The rolling repository audit continues.
