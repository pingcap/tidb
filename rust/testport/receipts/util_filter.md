# `pkg/util/filter` — complete package transcreation

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02), unchanged from
extraction pin `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly six artifacts (914 textual lines), all read in full:

- `filter.go` — rule construction, regex/trie selection, schema/table
  precedence, cached matching, `ApplyOn`, and `Apply`;
- `schema.go` — the system-schema predicate and its two package variables;
- `filter_test.go` — six source tests covering both apply paths, precedence,
  case handling, invalid regexes, schema-only statements, and nil rules;
- `schema_test.go` — the complete eleven-row system-schema table;
- `README.md` — MySQL replication-filter behavior and rule priority;
- `BUILD.bazel` — the package library and test target.

There is no `doc.go`, ownership file, generated/platform source, fixture,
benchmark, example test, or additional harness. The checkout is byte-identical
to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/filter` is the sole package owner. Its seven tests
map one-for-one to the six tests in `filter_test.go` and the one test in
`schema_test.go`, including every source table row. `regexpr-router` and the
session plan-cache system-schema check use this owner directly.

The retained implementation preserves Go's rule initialization order,
schema-before-table precedence, case normalization, regex/trie combinations,
schema-only statement handling, synchronized match cache, and the distinct
clone/original behavior of `ApplyOn` and `Apply`. Regex compilation uses the
shared crate-private Go-regexp authority, preserving Go's ASCII Perl classes
and word boundaries.

The audit removed the unused 1,448-line `tidb-exec::filter` implementation and
its duplicate and supplementary tests. It also removed Rust-only exposed error
variants, restored Go's four distinct empty-rule messages, and removed four
supplementary owner tests that had no pinned Go test counterpart. No code
outside the deleted file referenced its module.

The authority refresh removed four Rust-only `#[must_use]` diagnostics from
`is_system_schema`, `Filter::apply_on`, `Filter::apply`, and `Filter::matches`.
A focused `#[deny(unused_must_use)]` regression failed with four errors before
the change and passes afterward, matching Go's discardable return values.

## Validation

Profile: **Ready** for this focused parity fix within the continuing repository
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909..c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/filter` — passed; no Go package drift.
- `git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec..HEAD -- pkg/util/filter` — passed; no current-branch Go package drift.
- `git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/filter` — passed; exactly the six artifacts listed above.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/filter -count=1` — passed in current and exact detached latest-master (`/tmp/tidb-go-latest-c605`) worktrees.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --test table_filter_contract --offline --locked -- --test-threads=1` — passed; four tests including the discard-return regression.
- `cargo test -p tidb-util --locked filter::` — passed.
- `cargo test -p tidb-util --locked` — passed.
- `cargo check -p tidb-exec --lib --locked` — passed.
- `cargo check -p tidb-session --lib --locked` — passed.
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: reduced; one wired implementation owns the package and its
  observable validation errors now distinguish the same four rule classes as
  Go.
- Compatibility: the unused `tidb-exec::filter` public module and Rust-only
  error variants are removed; repository consumers compile against the owner.
- Performance: unchanged; matching still uses the selector, compiled regex
  map, and synchronized result cache.
