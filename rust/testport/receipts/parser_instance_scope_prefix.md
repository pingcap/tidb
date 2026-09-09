# `pkg/parser` `@@instance.` scanner-prefix parity (finding #12)

Go source: `origin/master` at
`d152e4b78d35cfcb771bfabc289f837c2374d4aa` (2026-09-03). This closes finding
#12 of `rust/docs/parser-lexer-divergence.md`.

## Go behavior (the oracle)

`startWithAt` (`pkg/parser/lexer.go:671`) matches exactly
`{"global.", "session.", "local."}` as `@@` scope prefixes. For
`@@instance.x` no prefix matches, so `scanIdentifierOrString` runs an
`isUserVarChar` identifier sweep (`.` is in the table, `misc.go:49`) that
folds `instance.` together with the variable name into one
`doubleAtIdentifier` literal `@@instance.x`. The instance scope is split at
the GRAMMAR layer instead: the `SystemVariable` and `VariableAssignment`
actions (`parser.y:12070-12079`, `:12182-12199`) test
`strings.HasPrefix(v, "@@instance.")` on the literal.

That layering has two observable consequences the old Rust prefix leak got
wrong:

1. `SET @@instance."x" = 1` — Go lexes `@@instance.` (identifier run stops at
   the quote) plus a separate string token, so the SET grammar cannot join
   them: syntax error. The old Rust scanner consumed the quoted body into one
   variable token and parsed it as a variable named `x`.
2. `SELECT @@instance.` — Go's identifier run produces a valid
   `doubleAtIdentifier` literal `@@instance.` and the `SystemVariable` action
   builds `VariableExpr { name: "", IsInstance: true }`: the statement
   parses. The old Rust scanner hit the missing-body arm and returned
   `Invalid`, rejecting the statement.

The plain `@@instance.x` shape is unchanged on both sides (one token, instance
scope via `parse_variable`'s existing `INSTANCE` split — the same split Go's
grammar performs).

## The fix

`tidb-lexer::scan_at` drops `"instance."` from the prefix list, with a comment
citing `lexer.go:671` and the grammar-level split. Everything downstream
(`parse_variable`'s scope split, the SET layer's own empty-name guard) already
matched Go.

## Regressions (both fail-before, verified on the pre-fix tree)

- `tidb-lexer tests::parser_parse_restore_source::instance_scope_is_not_a_scanner_prefix`
  — token shapes for `@@instance.x` (one token), bare `@@instance.` (one
  `UserVar` carrying the whole text; pre-fix: `Invalid`), and
  `@@instance."x"` (`UserVar` + separate `Str`; pre-fix: one `UserVar`).
- `tidb-parser tests::set::instance_scope_quoted_body_and_bare_dot_follow_the_go_scanner`
  — `SET @@instance."x" = 1` refuses (pre-fix: parsed), and
  `SELECT @@instance.` parses to `@@INSTANCE.``` ` `` with the instance scope
  (pre-fix: parse error).

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-lexer -p tidb-parser --no-fail-fast
# 925 run, 925 passed, 1 skipped — including the pre-existing
# @@instance.sql_mode / set @@instance.xx.xx pins, unchanged by design.
# Pre-fix control: both new regressions FAILED against the old scanner.
cargo +nightly-2026-08-22 clippy --offline --locked -p tidb-lexer -p tidb-parser --all-targets
# no diagnostics in the touched code (pre-existing workspace warnings elsewhere)
```

## Risk

- Correctness: low; the plain dotted-name span is byte-identical before and
  after (the identifier run covers the same bytes the prefix loop consumed),
  so only the two edge shapes above change, both toward Go.
- Compatibility: no API change; the one-line prefix-list edit is the whole
  production diff.
