# Shared DML row policy and scope


This living plan follows PLANS.md. The goal is to repair connected E02/E03/K03 existing-owner behavior across single-table UPDATE, joined UPDATE and duplicate-key UPDATE, using the shared record owner. It does not accept the complete executor, planner, table or session packages.

## Progress


- [x] Refresh master and integration; verify clean starting checkouts and read current ownership evidence.
- [x] Read current Go updateRecord, update row composition and writable-column position construction; add grouped regressions before production changes.
- [x] Confirm ten baseline failures and implement the shared scope and record-policy repair.
- [x] All 209 selected Rust tests, affected all-target checking, lint, locked server build and 20 real MySQL/unistore checks pass.
- [x] Update both finding registers and self-review; prepare publication through the actual hook and fresh locked build. Final commit, remote and Cloud readback results are recorded in /workspace/.cloud-setup/shared-dml-contract-batch/final-handoff.json.

## Context and milestones


Baseline TiDB is ab97d544b62d5e1bc7f59d010931bd9c7df5a08b on hparser-integration; native master is 8b752f9638ad157931725b66ffdc57e0465432a9 and remains unchanged. Fresh Go master is 3ca96b1d5df8da123e7a650512654eedab12c861, exported in /workspace/.cloud-setup/go-master. Read current-audit/README.md, both finding registers and remaining-batches.md for the broader unresolved owners.

First establish three linked contracts from pkg/executor/write.go and update.go and pkg/planner/core/logical_plan_builder.go: IGNORE FK checks see the candidate before bad-NULL substitution; SQL pseudo handles belong to expression scope but not the table's writable row; partitioned heap UPDATE must not skip distinct physical rows sharing a bare handle. Go's writable table range excludes ExtraHandleID, so enabling its UPDATE assignment does not imply an ordinary heap-handle move. The prior Rust comment claiming this move is required is stale. Keep INSERT's distinct explicit-handle behavior.

Second migrate tidb-executor's shared UpdateRecords owner and both single/joined expression-scope producers together. Hidden generated columns remain part of the retained writable row, while the pseudo handle follows visible SQL columns in expression scope. All three record-policy callers must retain the same FK-before-NULL error ordering. Keep useful Go-contract tests and Rust memory/ownership checks. No native, dependency or generated-file changes are planned.

Third validate existing generated/FK/multi-table suites together, update durable evidence and publish through all required gates. Entire package inventories retain their existing unaccepted status; no remaining broad finding is closed by these branch repairs.

## Surprises & Discoveries


Current Go does not cast pseudo-handle UPDATE assignments: their column metadata is nil. TblColPosInfo.End is Start plus WritableCols length. Its expression may be evaluated, but does not change the stored heap identity. This is source evidence, not a new feature to implement based on the old Rust comment.

## Decision Log


Keep one UpdateRecords implementation for ordinary/joined/duplicate updates. Fix producer layouts instead of adding per-query special cases. Move the bad-NULL pass to its source-ordered position, retaining FK rejection before later diagnostics. Generated/constraint and scope regressions are one compile/test selection. Validate source-derived expectations against actual failures before retaining them.

## Validation and recovery


Source /workspace/.cloud-setup/env.sh; export CARGO_BUILD_JOBS=1; run Cargo from /workspace/tidb/rust. Before production edits, run cargo test --locked -p tidb-session --lib -- shared_write_ --test-threads=1. Then run the combined tests_generated_columns::, tests_foreign_key::, tests_multi_table_dml:: and tests_statement_rollback:: owners. Run cargo check --locked --all-targets -p tidb-executor -p tidb-session -p tidb-server; make lint from the repository root. Record exact commands, failures, counts and limits under /workspace/.cloud-setup/shared-dml-contract-batch and current-audit/shared-dml-contract-batch-validation.json.

A commit must invoke executable hooks/pre-commit selected by core.hooksPath=hooks, including cd rust && cargo build --locked -p tidb-server. Repeat that build immediately before each normal push to pingcap/tidb hparser-integration; never force or bypass hooks. Verify remote SHA. Preserve native master and concurrent work. Recover individual files from the baseline without resetting another contributor's changes. Save/read back the reusable environment checkpoint; product Publish and fresh-task restoration are separate, unverified states until actually performed.

## Outcomes & Retrospective


Ten confirmed regressions fail before repair; all 209 selected Rust tests and 20 real MySQL/unistore assertions pass after repair. All-target checking, lint and locked server build pass. Broader source packages remain partial. Four speculative virtual-NULL cases were discarded because their precondition is invalid: the current virtual read owner substitutes zero. An expression-index setup used a nonexistent variable; removing that setup exposed the actual unresolved joined handle binding. Original logs remain. Current Go UpdateExec.prepare explicitly disables repeated-handle skipping for partitioned heap tables; the prior receipt missed this branch and incorrectly dismissed that divergence. This batch restores its regression without changing DELETE's separate deduplication policy. Baseline register: 86 tracked, 30 repaired, 56 unresolved (27 open, 29 partial). No full Go, live multi-node TiKV or performance acceptance is planned or implied.
