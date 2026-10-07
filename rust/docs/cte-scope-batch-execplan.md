# Share CTE visibility across SQL admission and prepared names

This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture

A common table expression (CTE) is a query-local name declared by WITH. A nested or later declaration must not hide an unrelated physical table. This batch makes privilege checks, PREPARE database pinning and their statement-summary consumers agree on which references name physical tables. For example, an unprivileged user must not read test.t by placing an unrelated WITH t in a sibling derived query; a prepared query must still read its original database after USE.

## Progress

- [x] Inspected live callers and refreshed Go master 7a3dacb52efe58d28db360ae8639d8838c376544; integration starts at e0f28ca86256ee2f68b97422eaccf984c19ecd7a.
- [x] Added six regressions to existing grant, binding, prepared and observation suites.
- [x] Six Rust regressions failed; the live unistore probe reproduced five failures and three passing controls.
- [x] Migrated both consumers to CteScope and removed the global collector, unused alias collector/type and PREPARE pre-scan.
- [x] 101 grouped Rust tests, eight live SQL assertions, affected all-target checking, make lint and the locked server build passed.
- [ ] Update both finding registers and validation receipt; commit with actual hook, fresh build before normal push, verify remote and save cloud checkpoint.

## Surprises & Discoveries

Both required_table_privileges and pin_current_database call a global CTE-name collector. A name declared anywhere suppresses matching physical-table references everywhere. The statement-summary table list uses the same privilege visits, so the omission propagates to observability. The unfiltered collect_table_refs helper has no callers.

## Decision Log

- Decision: share lexical visibility in the existing binding/preprocess boundary; preserve raw binding table-name collection.
  Rationale: Go bindinfo collects syntactic names while preprocess and logical planning distinguish CTE references. Changing binding matching to filter physical tables would conflate those contracts.
  Date/Author: 2026-10-07 / Codex.
- Decision: treat this as existing-owner maintenance across A01, S03 and O18, not complete Go package acceptance.
  Rationale: planner-owned visit ordering, migration tokens and broader observation remain absent.
  Date/Author: 2026-10-07 / Codex.

## Context and Orientation

rust/crates/tidb-session/src/binding.rs owns database pinning used by prepared_statements.rs, prepared_ast.rs and binding_arm.rs. table_privilege.rs collects read-table requirements consumed by admission and observation.rs. Rust's generated AST visitor enters WITH definitions before a query body; SelectStmt and SetOprStmt delimit visibility. Cte itself has no recursive flag, so the enclosing WithClause supplies it. Go pkg/planner/core/preprocess.go Enter/Leave/handleTableName and logical_plan_builder.go buildWith are the reference: earlier definitions are visible, recursive self is visible inside its body, ordinary self and later names are not, qualified names always identify tables. Nested visibility is restored on exit.

## Plan of Work

Introduce a small shared visibility state at the existing session preprocessing boundary. Push the visible-name length at query/CTE entry, add recursive self before visiting its query, truncate on exit, and publish completed CTE names to their enclosing query. Read-table collection filters at each TableRef visit. Database pinning asks that same state in its existing mutable visitor, removing its preliminary full-tree clone and global CTE scan. Leave raw binding matching and generated AST code unchanged.

## Concrete Steps

From /workspace/tidb/rust, activate source /workspace/.cloud-setup/env.sh and export CARGO_BUILD_JOBS=1 before every Cargo invocation. Run cargo test --locked -p tidb-session --lib -- cte_scope_batch_ --test-threads=1 before edits. After edits run the affected existing grant, binding, prepared and observation suites in one process. Run cargo check --locked --all-targets -p tidb-session -p tidb-server and cargo build --locked -p tidb-server. From repository root run make lint with the same environment. Source env and set RUST_MIN_STACK=33554432 for the real-server SQL probe under /workspace/.cloud-setup/cte-scope-batch. Actual hook and immediately pre-push locked build are mandatory; never force push.

## Validation and Acceptance

The new cases must fail on the original production code and pass after migration. Assert exact 1142 denial for nested, nonrecursive-self and forward-name collisions; retained PREPARE reads test.t after USE another database; summary attributes test.cte_observation; recursive, preceding and qualified reference controls keep their meanings. Run the same observable SQL cases on unistore. Do not infer complete planner or Go-package parity from these cases. Record logs, hashes and exact command results in current-audit/cte-scope-batch-validation.json.

## Idempotence and Recovery

Preserve concurrent changes. Do not replay one-use mutation scripts. No lockfile/native dependency changes are needed. Preserve compiler caches; reserve enough disk for the server link by retiring only verified inactive superseded test executables if necessary. Failed publication leaves local commits intact and must not be reported as success.

## Artifacts and Notes

External working receipts are under /workspace/.cloud-setup/cte-scope-batch; durable evidence belongs in rust/docs/parity/current-audit. Cloud draft saving does not publish or verify a fresh restoration.

## Interfaces and Dependencies

Reuse tidb_ast::Visitor/Visitable and existing SelectStmt, SetOprStmt, WithClause, Cte and TableRef types. Both physical read collection and mutable pinning use one CTE scope implementation. No new dependency, public protocol or generated source is required.

## Outcomes & Retrospective

All six fail-before regressions now pass inside one 101-test group covering grant, prepared, binding and observation behavior. All eight real-server assertions pass, including five previously failing cases. All-target checking, lint and locked server build pass. Publication/checkpoint gates follow in the external final handoff to avoid recursively rebuilding for receipt-only commits. Broad parent findings remain partial until their complete owner obligations are verified.
