# Port ALTER TABLE charset and collation options structurally

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` current as work proceeds.

Reference: `PLANS.md` at the repository root; this plan follows its required
format.

## Purpose / Big Picture

TiDB's `ALTER TABLE` parser has two adjacent table-option paths that the Rust
port previously rejected: generic repeated charset/collation options and the
distinct `CONVERT TO CHARACTER SET` action. This increment ports the typed Go
AST payload and exact restore behavior, including its surprising two-option
special case, while the seed executor rejects both actions before an implicit
DDL commit because it does not own table charset metadata or value conversion.

## Progress

- [x] (2026-07-14) Traced Go's `parseAlterTableOptions`, `CONVERT` handler,
  and `AlterTableSpec.Restore` special cases.
- [x] (2026-07-14) Added typed actions for ordered table options and conversion
  targets; extracted the shared `TableOption` restore implementation.
- [x] (2026-07-14) Added unit coverage and a source-derived 20-row static
  selector from the checked integration-fixture oracle.
- [x] (2026-07-14) Reviewed the full static distribution and updated it from
  48,578 matches / 2,141 failures / 833 mismatches to 48,608 / 2,111 / 833.
- [ ] Run the combined workspace WIP ring after concurrent waves settle.

## Surprises & Discoveries

- Go restores an exact two-element `CHARSET` then `COLLATE` option list as
  `CHARACTER SET value COLLATE value`, with neither `DEFAULT` nor `=`. Longer
  option sequences restore each ordinary table option independently.
- `CONVERT TO` is not a generic table option: its charset is either a validated
  canonical name or a dedicated `DEFAULT` bit, and it omits `=` on restore.

## Decision Log

- Decision: use typed `Vec<TableOption>` and an explicit conversion action,
  rather than raw SQL or an untyped option string.
  Rationale: this mirrors Go's `AlterTableSpec.Options`, preserves written
  order, and makes the length-sensitive restore rule visible and testable.
  Date/Author: 2026-07-14 / Codex.
- Decision: reject execution before mutation.
  Rationale: updating charset/collation correctly requires catalog metadata
  plus conversion semantics for stored data, neither of which exists in the
  seed executor.
  Date/Author: 2026-07-14 / Codex.

## Outcomes & Retrospective

Twenty checked Go parser-oracle restores now match byte-for-byte. The parser
uses the lexer-generated source charset registry, including `utf8mb3` to
`UTF8` canonicalization. Table-level collation validation remains tied to the
broader future collation registry work; this bounded slice only accepts and
restores fixture-proven values.

## Context and Orientation

The authoritative Go parser branches are `pkg/parser/ddl_alter_handlers.go`
(`parseAlterTableOptions` and `convert`) and their restore behavior is in
`pkg/parser/ast/ddl.go` (`AlterTableSpec.Restore`). Rust's corresponding code
is `crates/tidb-parser/src/ddl.rs`, `crates/tidb-ast/src/ddl.rs`, and the
pre-mutation executor gate in `crates/tidb-exec/src/database.rs`.

The source-derived proof is
`difftests/tests/alter_table_charset_collation_selector.rs`; it selects exactly
the 20 accepted, one-statement fixture rows whose ALTER action begins with this
grammar family. The static oracle remains
`difftests/corpus/coverage/integration_parser_golden.tsv`.

## Validation and Acceptance

- The focused selector must restore all 20 selected rows byte-for-byte.
- `CHAR SET`, `CHARACTER SET`, `CHARSET`, `DEFAULT` prefixes, and `CONVERT TO`
  forms must restore exactly as Go's AST dictates.
- The executor must return an explicit unsupported error before committing or
  mutating a table.
- The full parser oracle snapshot must change only by the reviewed 20 matches.

## Idempotence and Recovery

All Cargo checks are safe to rerun. Do not regenerate the checked Go oracle for
a Rust-only parser change. If a future table-option action cannot be represented
as a typed `TableOption`, leave it rejected rather than accepting raw text.
