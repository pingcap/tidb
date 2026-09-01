# `pkg/util/dbterror/plannererrors` — complete package parity receipt

Go source: `origin/master`
`db35d47066648fe73abce6318d53fc625df51490`.

Rust comparison branch: `origin/hparser-integration`
`5a005978dda57fbb3373a303660ea0a5f7990b38`.

## Complete Go inventory

The package has exactly three direct artifacts, all read in full:

- `planner_terror.go` — 125 lines including the license and package header,
  one declaration block with 98 exported error prototypes;
- `errors_test.go` — 91 lines, one source test (`TestError`) with 59 listed
  prototypes and no benchmark or fuzz test;
- `BUILD.bazel` — 25 lines, one public `go_library` and one short/flaky
  `go_test` target with the exact source and parser/testify dependencies.

There is no `doc.go`, fixture/testdata, generated source or generator input,
platform/build-tag variant, README, or ownership artifact. The checkout copies
of all three artifacts are byte-identical to the fetched Go source. The
production file has no function or method declarations: all behavior is the
package-level prototype table.

The 98 entries use `ClassOptimizer.NewStd` except `ErrTooBigPrecision`, which
uses `ClassExpression.NewStd`, and the four executor-class entries
`ErrPrepareMulti`, `ErrUnsupportedPs`, `ErrPsManyParam`, and `ErrPrepareDDL`.
`ErrAccessDenied` is the only `NewStdErr` entry; Go deliberately uses the
`ErrAccessDeniedNoPassword` message while retaining code `ErrAccessDenied`.
The table also contains the source's aliases (`ErrUnknownColumn` over
`ErrBadField`, `ErrAmbiguous` over `ErrNonUniq`, and the executor/planner
duplicates) and the window, CTE, lateral-join, temporary-table, and hint
prototypes in their original declaration block.

## Rust ownership and comparison result

`rust/crates/tidb-error/src/plannererrors.rs` is the standalone owner and is
exported by `rust/crates/tidb-error/src/lib.rs`;
`rust/crates/tidb-error/Cargo.toml` is its build artifact. The owner contains
98 public `LazyLock<TerrorError>` statics with Go-name doc links.
`TerrorError::registered_std` resolves the same combined MySQL/TiDB catalog
used by Go's `errno.MySQLErrName`, while the `ERR_ACCESS_DENIED` initializer
preserves the source's special code-1045, code-1698 message selection and
catalog redaction metadata.

The owner test `all_prototypes_resolve` forces every one of the 98 lazy
prototypes, matching Go package initialization's requirement that every
`NewStd` code exist in the catalog. The source-derived `test_error` test
contains exactly the 59 entries from Go `TestError` and asserts the same
non-`ErrUnknown` SQL code identity. A mechanical comparison of the Go
declaration names and Rust Go-name doc links is complete; only the special
`ErrAccessDenied` static is placed last in Rust so its alternate message
initializer stays isolated, which does not affect the registered code map.

No Go-master drift, missing prototype, duplicate Rust table, or Rust-only
execution behavior was found. The existing all-entry initialization guard and
the exact source test are sufficient focused regressions for this unchanged
catalog, so this audit adds no redundant test or production code.

## Validation

Profile: Ready for this atomic package audit. This is not a repository-wide
parity or PR-readiness claim.

Commands run from the repository root:

- `git ls-tree -r --name-only origin/master -- pkg/util/dbterror/plannererrors`
  and full-file reads — confirmed the three-artifact inventory.
- The Go declaration-name/Rust doc-link `diff` — passed: all 98 names match
  (declaration order differs only for `ErrAccessDenied`).
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/dbterror/plannererrors -count=1` — passed, including Go `TestError`.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked -p tidb-error --lib plannererrors` — passed, both owner tests.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --locked -p tidb-error --lib` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

No Go source, import block, test function, module metadata, or Bazel artifact
changed, so `make bazel_prepare` is not required.

## Risk and unverified scope

- Correctness: low. Go `TestError`, Rust's all-entry initialization guard, and
  the complete name/code mapping pass.
- Compatibility: no API or runtime behavior changed. The Rust catalog already
  included all current Go declarations and preserves the special access-denied
  message identity.
- Performance: none; these are lazily initialized error prototypes.
- Not verified locally: no non-host platform variant, generated fixture, or
  Go benchmark exists in this package. Higher-level planner/executor callers
  are separate package claims.
