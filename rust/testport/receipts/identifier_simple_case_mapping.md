# Identifier lowercasing — Go `strings.ToLower` simple-mapping parity (#196)

Go source: `origin/master` at
`d152e4b78d35cfcb771bfabc289f837c2374d4aa` (2026-09-03). This closes the
parser-facing half of plan finding #196 (identifier case mapping).

## Go behavior (the oracle)

Every identifier-facing lowercase in the Go parser is `strings.ToLower` — the
SIMPLE Unicode case mapping, no final-sigma rule:

- `pkg/parser/digester.go:227` — every digester literal:
  `currTok := token{tok, strings.ToLower(lit)}`.
- `pkg/parser/parser.y:12219/12223` — `UserIdentity { Hostname:
  strings.ToLower($3) }`.
- `pkg/parser/parser.y:12257/12261` — `RoleIdentity { Hostname:
  strings.ToLower(...) }`.
- `pkg/parser/parser.y:12577` — `AlterJobOption { Name: strings.ToLower($1) }`.

Go's `strings.ToLower("ΟΔΟΣ")` is `οδοσ` (capital sigma → sigma). Rust's
`str::to_lowercase` is the FULL mapping: a trailing capital sigma becomes
final sigma (`U+03C2`), so `SELECT * FROM ΟΔΟΣ` digested as
`` ...οδος`` with ς — different digest bytes for the same SQL on Go and
Rust. That is the concrete, reachable instance of #196: every digest of a
non-ASCII identifier ending in a casing-sensitive letter diverged.

## The fix

The five identifier-facing `str::to_lowercase` call sites now use
`tidb_mysql::to_lowercase` — the crate's existing `strings.ToLower` port
(simple per-rune `CaseRanges` mapping pinned to Go's Unicode version), which
`CiString` already uses:

- `digest.rs` — the digester literal (the high-traffic surface).
- `user.rs` (two sites) and `privilege/role_grant.rs` — the account/role
  `@host` part.
- `admin/ddl_job_alter.rs` — the `ADMIN ALTER DDL JOBS` option name.

`util_parser.rs`'s lowercase copies are ephemeral substring-search keys
transformed identically on both sides of the comparison — not an observable
surface — and are left alone.

## Regressions

- `parser_digester_source::identifier_lowercasing_uses_the_go_simple_case_mapping`
  — FAIL-BEFORE (pre-fix the digest of `SELECT * FROM ΟΔΟΣ` carried final
  sigma ς; post-fix it is `οδοσ`, and the all-caps/all-lower spellings digest
  identically).

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-parser -p tidb-lexer --no-fail-fast
# 925 run, 925 passed, 1 skipped
# Pre-fix control: the new regression FAILED with the final-sigma digest.
cargo +nightly-2026-08-22 clippy --offline --locked -p tidb-parser --all-targets
# no diagnostics in touched code
```

## Planner keys (2026-09-04, same finding)

`pkg/planner/core/schema_table_key.go` keys aliases with `ast.CIStr.L`
(`newTableAliasKey`/`newQualifiedTableAliasKey`), and `ast/model.go:302`
defines `CIStr.L = strings.ToLower(s)` — the same simple mapping. The
planner's `SchemaTableKey`/`TableAliasKey` leaves and the view-recursion,
hint-table, alias-collision, and `USING`-column key sites in
`plan_builder/from.rs` used Rust's full `to_lowercase`: a schema/table/alias
named `ΟΔΟΣ` keyed with final sigma on one side and sigma on the other would
split one identity into two (or hide a collision). All thirteen sites now use
`tidb_mysql::to_lowercase` (tidb-planner gains the `tidb-mysql` dependency;
`Cargo.lock` updated). Fail-before regression:
`schema_table_key::tests::keys_use_the_go_simple_case_mapping` (schema keyed
`οδος` with ς pre-fix, `οδοσ` post-fix).

## Risk

- Correctness: low; the simple mapper is the exact `strings.ToLower` port
  already pinned by `CiString`'s contract and `GO_UNICODE_VERSION`.
- Compatibility: digests of ASCII identifiers are unchanged; only non-ASCII
  identifiers whose full and simple mappings differ (final sigma) change —
  from divergent to Go-identical.
- Coverage note: this closes the parser surface. Any other crate applying
  full `to_lowercase` to Go-`strings.ToLower` values would need the same
  swap; the sweep found none in the parser, lexer, or AST tiers.
