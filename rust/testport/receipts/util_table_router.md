# `pkg/util/table-router` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full:

- `router.go` — route/extractor configuration, validation, case folding,
  construction, add/update/remove, routing, capture extraction, and extended
  columns;
- `router_test.go` — `TestRoute`, `TestCaseSensitive`, and
  `TestFetchExtendColumn`;
- `BUILD.bazel` — one library and one test target, depending on the complete
  table-rule-selector package.

There is no `doc.go`, README, ownership file, generated/platform source,
fixture, benchmark, example test, or additional harness. The checkout is
byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/table_router` is the sole package owner. Its three
Go-named tests reproduce every source assertion. Three supplementary tests
retain the source validation/config contract, optional capture-group behavior,
and Go-regexp semantics. The dependent `tidb-util` regexpr-router uses this
owner directly.

The audit found that extractor validation compiled patterns with Rust's
Unicode `\d`/`\w`/`\s` semantics instead of Go's ASCII Perl classes and word
boundaries. A public-path regression failed before the fix because
`table_١` produced the capture `١`; it now produces no capture like Go. All
three extractor types compile through the shared crate-private Go-regexp
authority.

The audit removed Rust-only public constructors for `TableRule` and its three
extractors, plus cloning/value equality on fresh router errors. Source-shaped
public fields remain available for configuration and composite construction;
the compiled-regexp cache remains crate-private. It also removed the unused
1,029-line `tidb-exec` implementation and its duplicate/extra tests. No code
outside that file referenced its module.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/table-router` — passed (3 tests).
- Before the fix,
  `cargo test -p tidb-util --locked table_router::tests::extractors_keep_go_ascii_perl_classes`
  — failed because the extractor captured an Arabic-Indic digit; the same
  command passes after the fix.
- `cargo test -p tidb-util --locked table_router::` — 6 tests passed (3 source
  tests plus 3 supplementary source-contract regressions).
- `cargo test -p tidb-util --locked regexpr_router::` — 10 dependent tests
  passed.
- `cargo test -p tidb-util --locked` — passed.
- `cargo check -p tidb-exec --lib --locked` — passed.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: reduced; extractor regexes now use Go's character-class rules
  and only one table-router implementation remains.
- Compatibility: unused `tidb-exec::table_router` and Rust-only constructors
  are removed; repository consumers compile against the source-shaped owner.
- Performance: regexp rewriting occurs only during rule validation; routing
  still uses the compiled selector and regex caches.
