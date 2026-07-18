# Preserve typed vector-index constraints in CREATE TABLE

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` current as work proceeds.

Reference: `PLANS.md` at the repository root; this plan follows its required
format.

## Purpose / Big Picture

TiDB accepts `CREATE TABLE` declarations containing `VECTOR INDEX` constraints
such as `VECTOR INDEX ((VEC_L2_DISTANCE(vec)))`. The Rust parser currently
accepts `VECTOR(n)` column types but rejects these table constraints because its
`TableConstraint::Index` carries only plain column names. After this work, Rust
will retain the full vector-index parse tree, restore the exact canonical Go
SQL, and reject execution before catalog mutation until the seed executor owns
vector indexes. The observable proof is that the selected upstream static
oracle rows move from parser failures to byte-for-byte restore matches.

## Progress

- [x] (2026-07-14) Located the parser gap and the reusable typed base:
  `IndexPart` and `SecondaryIndex` already model expression key parts.
- [x] (2026-07-14) Verified the Go contract in
  `pkg/parser/ddl_index_parser.go` and `pkg/parser/ast/ddl.go`:
  `ConstraintVector` restores as `VECTOR INDEX` and carries ordinary index
  parts/options.
- [x] (2026-07-14) Replaced the column-only table `Index` representation with
  `SecondaryIndex`, retaining ordinary-index restore and adding typed vector
  kind plus `IF NOT EXISTS`.
- [x] (2026-07-14) Parsed and restored the bounded no-option `VECTOR INDEX`
  source family, including parenthesized expression key parts. `USING` and
  other unrepresented index options still reject rather than disappear.
- [x] (2026-07-14) Added a source-derived nine-row static selector and
  pre-mutation execution regression; snapshot review remains root-owned.
- [x] (2026-07-14) Reviewed the full static distribution and updated the
  root snapshot from 48,567/2,152/833 to 48,576/2,143/833.
- [x] (2026-07-14) Extended the same typed parser to the two checked
  `ALTER TABLE ADD VECTOR INDEX` rows and added their exact selector.
- [x] (2026-07-14) Reviewed the full static distribution and updated the
  root snapshot from 48,576/2,143/833 to 48,578/2,141/833.
- [ ] Validate the combined workspace WIP differential ring.

## Surprises & Discoveries

- Observation: `VECTOR(n)` is already ported and tested; the 969-row leading
  queue family is vector *indexes*, not vector column types.
  Evidence: `crates/tidb-parser/src/tests/ddl.rs` has vector column coverage,
  while `integration_parser_queue --check` groups failures under `CREATE
  TABLE` from vector fixture statements.
- Observation: Go's `ast.ConstraintVector` uses the same `Keys` and
  `IndexOption` fields as an ordinary index and its restore emits `VECTOR
  INDEX`, not a special execution syntax.
  Evidence: `pkg/parser/ddl_index_parser.go:129-151` and
  `pkg/parser/ast/ddl.go:1014-1050`.
- Observation: nine checked Go-oracle `CREATE TABLE ... VECTOR INDEX` rows
  and two `ALTER TABLE ADD VECTOR INDEX` rows become matches through the
  common typed representation.
  Evidence: full static replay changed from 48,567/2,152/833 to
  48,576/2,143/833 before its reviewed snapshot update.
- Observation: the ALTER-table rows have no index options, so the shared
  expression-part grammar restores them byte-for-byte without accepting
  unrepresented `USING`/`COMMENT` syntax.

## Decision Log

- Decision: promote table-level ordinary and vector indexes onto the existing
  expression-capable `SecondaryIndex` representation rather than add a raw SQL
  string or a vector-only parallel type.
  Rationale: `SecondaryIndex` and `IndexPart` already encode the same Go
  concept for ALTER TABLE, removing the column-only special case and allowing
  parser/restore fidelity without an executor shortcut.
  Date/Author: 2026-07-14 / Codex.
- Decision: parse/restore only in this increment; execution returns a precise
  unsupported error before any catalog or transaction mutation.
  Rationale: a seed in-memory executor cannot model vector-index catalog
  ownership, DML maintenance, vector search, or planner behavior. Pretending
  otherwise would violate the rewrite's source-first contract.
  Date/Author: 2026-07-14 / Codex.

## Outcomes & Retrospective

The bounded CREATE TABLE and ALTER TABLE increments are implemented. They
improve the static parser oracle by eleven restores, retain each vector
constraint structurally, and reject execution before state mutation. Source-
visible index options not represented by `SecondaryIndex` remain explicit
future obligations.

## Context and Orientation

The repository root is `/Users/qiliu/projects/tidb`. Rust lives below `rust/`.
Go is the semantic source of truth. A *vector index* is a table constraint
introduced by `VECTOR INDEX`; its key can be a parenthesized expression such
as `VEC_L2_DISTANCE(vec)`, not merely a named column.

`rust/crates/tidb-ast/src/ddl.rs` defines `IndexPart`, `SecondaryIndex`, and
`TableConstraint`. `IndexPart` already preserves either a column key or an
expression key. `SecondaryIndex` already preserves a name, key parts, comment,
global/invisible options, and a partial-index predicate for `ALTER TABLE ADD
INDEX`. `TableConstraint::Index(TableKeyConstraint)` is the lossy older shape
used only by `CREATE TABLE`.

`rust/crates/tidb-parser/src/ddl.rs::parse_create_table` dispatches table
constraints. Its current `KEY|INDEX` branch parses only plain column lists and
explicitly rejects `VECTOR`. `parse_ordinary_secondary_index` is the reusable
parser for expression parts and index options. `rust/crates/tidb-exec/src/
database.rs` must continue to reject the resulting secondary-index constraint
before mutation.

Go's matching implementation is `pkg/parser/ddl_index_parser.go::
parseConstraint`; its restore contract is in `pkg/parser/ast/ddl.go::
(*Constraint).Restore`. Upstream fixture rows live under
`tests/clusterintegrationtest/t/vector.test` and the static Go oracle at
`rust/difftests/corpus/coverage/integration_parser_golden.tsv`.

## Plan of Work

First introduce a `SecondaryIndexKind` (ordinary versus vector) plus
`if_not_exists` on `SecondaryIndex`, or an equivalently named typed structure
that makes the restore spelling explicit. Change `TableConstraint::Index` to
hold that expression-capable representation, update its canonical restore, and
update every exhaustive match in the executor to reject the new indexed
constraint before state changes. Do not add a stringly `raw_options` field.

Next extract the common table/ALTER secondary-index parser into a helper that
accepts the index kind. It must parse `VECTOR INDEX [IF NOT EXISTS] [name]
(parts)` using `IndexPart::Expr` for the double-parenthesized vector distance
form. Port only options proven by the Go source and focused fixtures; a
remaining unrepresented option must produce a parser error rather than vanish.

Add AST/parser tests covering normal and vector indexes, their canonical
restores, `IF NOT EXISTS`, an expression key part, and malformed syntax. Add a
`difftests/tests/vector_index_selector.rs` static selector that identifies the
bounded upstream vector statements and compares Rust restore bytes directly to
the checked Go oracle. Inspect the complete static-outcome count before editing
`integration_parser_diff.rs`'s `EXPECTED_COUNTS` and `HANDOFF.md`.

## Concrete Steps

Run all commands from `rust/` unless noted otherwise.

1. Read the Go parser/restore sources and source fixtures:

       sed -n '100,170p' ../pkg/parser/ddl_index_parser.go
       sed -n '1000,1050p' ../pkg/parser/ast/ddl.go
       rg -n -i 'vector index|vec_.*distance' ../tests/clusterintegrationtest/t/vector.test

2. Edit `crates/tidb-ast/src/ddl.rs`, `crates/tidb-parser/src/ddl.rs`, and
   only the executor matches necessary to preserve pre-mutation rejection.

3. Add focused parser/AST tests and `difftests/tests/vector_index_selector.rs`.
   The selector must reject drift if its source-derived row count changes.

4. Run:

       cargo fmt --all -- --check
       cargo test -j 12 -p tidb-parser vector -q
       cargo test -j 12 -p difftest --test vector_index_selector -q
       cargo clippy -j 12 -p tidb-ast -p tidb-parser -p tidb-exec -p difftest --all-targets -- -D warnings
       cargo test -j 12 -p difftest --test integration_parser_diff integration_parser_static_go_oracle_reports_rust_outcomes -q -- --exact

5. If and only if the printed full distribution changes exactly as the
   selector predicts, update the root-owned snapshot and rerun the full WIP
   ring in `docs/operations/validation.md`.

## Validation and Acceptance

Acceptance is behavioral, not merely compilation:

- A valid upstream `CREATE TABLE` with a `VECTOR INDEX` parses and restores
  exactly to the pinned Go oracle bytes.
- The parsed vector index retains `if_not_exists`, its name, typed expression
  key parts, and every ported option; tests inspect those fields directly.
- The seed executor reports an unsupported vector-index operation before table
  creation can mutate catalog state.
- The source-derived selector has no failures and the static parser snapshot
  changes only by its reviewed rows.

## Idempotence and Recovery

The parser and selector commands are safe to rerun. If a table index option
cannot be represented faithfully, remove that acceptance path and keep the
statement a parser failure; do not preserve it as raw SQL. If an exhaustive
executor match fails after the AST refactor, add the narrow pre-mutation
unsupported branch before running differentials again. Do not regenerate the
Go oracle or inventory for a Rust-only grammar change.

## Artifacts and Notes

The static oracle currently reports 51,598 inputs. Before this plan starts its
reviewed counts are 48,567 Rust matches, 2,152 raw Rust parse failures
(including 54 matched Go/Rust rejections), and 833 restore mismatches. The
actionable queue therefore reports 2,098 Go-accepted parser failures. Preserve
this distinction when reviewing deltas.

## Interfaces and Dependencies

At milestone completion `tidb_ast` must provide a typed representation
equivalent to:

    enum SecondaryIndexKind { Ordinary, Vector }
    struct SecondaryIndex {
        kind: SecondaryIndexKind,
        if_not_exists: bool,
        name: Option<String>,
        parts: Vec<IndexPart>,
        // only source-proven options represented as typed fields
    }

`TableConstraint` must use this typed index object for table-level secondary
indexes. `tidb_parser::Parser::parse_create_table` must construct it, and
`tidb_exec::Database` must keep its current pre-mutation unsupported boundary.

Plan created 2026-07-14 to convert the high-volume vector queue into a
source-backed structural port rather than a special-case parser allowance.

Updated 2026-07-14 after the CREATE TABLE and ALTER TABLE vector-index
increments; records the eleven-row evidence and remaining options boundary.
