# `pkg/util/format` — complete package transcreation

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02), unchanged from
extraction pin `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts (318 textual lines), all read in full:

- `format.go` — formatter interface, indentation/flattening state machine, and
  SQL display escaping;
- `format_test.go` — `TestFormat`;
- `main_test.go` — common test setup and leak checking;
- `BUILD.bazel` — one library and one flaky short test target.

There is no `doc.go`, generated/platform source, fixture, testdata, benchmark,
fuzz target, example, or additional harness.

## Rust ownership and audit result

The formatter state machine is shared with the already-transcreated
`pkg/parser/format` owner in `rust/crates/tidb-datatype/src/format.rs`.
`rust/crates/tidb-util/src/format.rs` reexports that owner and supplies util's
additional backslash escape.

The audit removed `IndentFormatter::into_inner` and
`FlatFormatter::into_inner`. Neither Go constructor exposes an equivalent
operation through its returned `Formatter` interface, and no production Rust
consumer used it. Tests now pass borrowed writers and inspect them after the
formatter is dropped. Unused `Clone`, `PartialEq`, and `Eq` implementations on
the Rust-only typed fragment boundary were removed as well.

The authority refresh also removed the Rust-only `#[must_use]` diagnostic from
`tidb-util::format::output_format`. A focused `#[deny(unused_must_use)]`
regression proves callers may discard the return value exactly as Go permits;
the pre-fix test failed with one unused-return error and passes after the
annotation removal.

The stale semantic manifest and historical audit plan were deleted. Retained
tests cover the source state machine, cross-call state, flat behavior, writer
counts/errors, opaque formatted values, Go string bytes, and the complete util
escape set.

## Validation

Profile: **Ready** for this focused parity fix within the continuing repository
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909..c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/format` — passed; no Go package drift.
- `git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/format` — passed; no current-branch Go package drift.
- `git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/format` — passed; exactly the four artifacts listed above.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/format -count=1` — passed in current and exact detached latest-master (`/tmp/tidb-go-latest-c605`) worktrees.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --test format_contract --offline --locked -- --test-threads=1` — passed; five tests including the discard-return regression.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-datatype --test all parser_format_package_source:: --offline --locked -- --test-threads=1` — passed; eight source tests.
- `cd rust && OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: unchanged; the Go and Rust formatter suites pass with borrowed
  writers and retain exact output/state behavior.
- Compatibility: intentionally removes unconsumed Rust-only convenience and
  trait implementations absent from Go.
- Performance: unchanged; rendering and the single underlying writer call are
  untouched.
