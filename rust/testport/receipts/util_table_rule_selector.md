# `pkg/util/table-rule-selector` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full:

- `trie_selector.go` — the selector interface, two-level wildcard trie,
  insert/replace/append behavior, matching, removal, rule snapshots, and the
  bounded synchronized match cache;
- `selector_test.go` — `TestSelector` and all five ordered helper phases;
- `BUILD.bazel` — one library and one flaky short test target.

There is no `doc.go`, README, ownership file, generated/platform source,
fixture, benchmark, example test, or additional harness. The checkout is
byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/table_rule_selector.rs` is the sole package owner.
Its `selector` test reproduces the complete source suite, including all rule
tables, match vectors, cache contents, mutation order, and removals. One
regression records a Go behavior that differs from ordinary byte iteration.

The audit used a direct pinned-Go probe to verify that `for i := range s`
advances over UTF-8 rune starts while the trie still indexes `s[i]` bytes. For
input `é`, Go matches two-byte wildcard/range paths but not the literal UTF-8
path. The Rust regression failed before the fix because it also returned the
literal rule; matching now advances by Go string-range indices, including the
invalid suffixes created by recursive `i+1` slicing.

The owner now also preserves Go's nil-versus-non-nil `RuleSet` result and cache
entries, accepts the open integer insert-type domain (unknown values append
like Go), restores the source's observable error text, removes a Rust-only
`Default` constructor and clonable error semantics, and removes three
supplementary tests with no pinned source counterpart.

The audit deleted the unused 1,270-line `tidb-exec` selector duplicate. Its
only consumer was the likewise unused 1,544-line `tidb-exec` duplicate of the
already-audited `pkg/util/column-mapping`; both duplicate public modules and
their tests are removed. All retained filter, router, and column-mapping
consumers use the `tidb-util` owner.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/table-rule-selector` — passed.
- Before the fix,
  `cargo test -p tidb-util --locked table_rule_selector::tests::utf8_matching_follows_go_string_range_indices -- --exact`
  — failed because the literal UTF-8 rule was incorrectly returned; the same
  command passes after the fix.
- `cargo test -p tidb-util --locked table_rule_selector::` — passed.
- `cargo test -p tidb-util --locked` — passed.
- `cargo check -p tidb-exec --lib --locked` — passed.
- `cargo check -p tidb-session --lib --locked` — passed.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: reduced; Unicode matching and nil cache results now follow Go,
  and one synchronized selector owns all retained callers.
- Compatibility: the unused executor selector/column-mapping module paths,
  Rust-only selector default, and clonable errors are removed.
- Performance: the matching loop adds UTF-8 width decoding to reproduce Go's
  range semantics; trie lookup, cache size, locking, and eviction remain the
  same shape as Go.
