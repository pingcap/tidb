# Batch 27 — ALTER/DROP MATERIALIZED VIEW parity: parser-only statements no-op

## Go reference (pinned master `94a9cbedab`)

Evidence trail (all from the pinned commit):

* Parser: `pkg/parser/parser.y` lines 5868 (`AlterMaterializedViewStmt`),
  5904 (`AlterMaterializedViewLogStmt`), 5940 (`DropMaterializedViewStmt`),
  5951 (`DropMaterializedViewLogStmt`) — all four statements parse into real
  AST nodes; `pkg/parser/ast/ddl.go:39-42` declares all four `DDLNode`s.
* Planner: `pkg/planner/core/planbuilder.go` `buildDDL` (line 5317) has NO
  case for any of the four and no default — the switch falls through to
  `p := &DDL{Statement: node}`. No visitInfo is appended, so no privilege
  check runs.
* Executor: `pkg/executor/ddl.go` `DDLExec.Next` (line 96) likewise has no
  case for any of the four and no default — `err` stays nil, the tail
  (implicit txn commit, `SetInTxn(false)`) runs, and the statement returns
  nil.

Net observable behavior: **each statement answers OK with zero writes, no
job, no schema-version bump, no warning** — a silent no-op (the "unfinished
feature" shape).

## Rust deliverables

* `rust/crates/tidb-exec/src/cluster_ddl.rs`:
  * four new `DdlStatement` variants (`AlterMaterializedViewNoOp`,
    `AlterMaterializedViewLogNoOp`, `DropMaterializedViewNoOp`,
    `DropMaterializedViewLogNoOp`), each carrying only the resolved
    schema/object names;
  * `lower_ddl_with_context` cases for `DdlStmt::AlterMaterializedView`,
    `AlterMaterializedViewLog`, `DropMaterializedView`,
    `DropMaterializedViewLog` — previously the `_` catch-all, which made
    the real node route the statements to the query path (a user-visible
    divergence: Go returns OK, Rust refused);
  * `plan_ddl_with_collation` arms planning each as
    `DdlPlan::AlreadySatisfied { warning: None }` — success, nothing
    written, exactly Go's answer.
* `rust/crates/tidb-exec/tests/cluster_ddl_source.rs`:
  * new `alter_materialized_view_succeeds_as_a_no_op_like_go` covering
    eight statement forms (ALTER with COMMENT / REFRESH NEXT, LOG ALTER
    with PURGE IMMEDIATE / ADD COLUMN, DROP with and without IF EXISTS,
    LOG DROP with and without IF EXISTS): each must LOWER (previously the
    route refused), PLAN as the zero-write success, and carry no warning.

## Validation

```
cargo +nightly-2026-08-22 nextest run -p tidb-exec \
  -E 'test(materialized_view) + test(persisted_materialized_view) +
      test(derives_the_purge_schedule) + test(preserves_text)'
# 13 passed (12 pre-existing + 1 new)
cargo fmt --all -- --check   # clean
git diff --check             # clean
```
