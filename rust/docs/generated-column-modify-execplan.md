# Restore generated-column modification semantics

This living plan follows repository PLANS.md. Maintain Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective during implementation.

## Purpose / Big Picture


ALTER TABLE MODIFY/CHANGE must apply the generated-column rules of pinned Go master fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85. In particular, replacing an unindexed virtual expression must update existing and newly inserted rows and SHOW CREATE TABLE. Rejecting every generated-column modification is an implementation gap, not a Go compatibility rule. The parent goal remains all failures from BLOCKER_RESOLUTION.md, not only this category.

## Progress


- [x] (2026-09-11) Replay original catalog gate: 355 compared, 330 matched, 25 divergent, 3480 statements run for effect. Gate fails against the historical 27-divergence expectation.
- [x] (2026-09-11) Replay with temporary strict empty-offset assertion: no chunk panic or empty-offset access; same catalog totals. Remove all diagnostic instrumentation.
- [x] (2026-09-11) Capture Go master successful expression replacement and old/new row values in /tmp/gen-modify-go.out.
- [x] (2026-09-11) Add and run red session regression modifying_virtual_generated_expression_recomputes_existing_rows; /tmp/generated-modify-red.log returns unsupported.
- [x] (2026-09-11) Implement candidate-schema validation and metadata replacement in ddl/generated_modify.rs; carry the auto-increment session flag; virtual changes avoid reading old values.
- [x] (2026-09-11) Twenty-five generated-column tests pass, covering virtual/stored transitions, dependency order, rename/reorder, indexed restrictions, invalid expressions, auto-increment switch and failed-DDL row preservation.
- [x] (2026-09-11) Final priority/partition metadata changes: 25 generated tests, 310 session integrations and make lint pass. Catalog comparison stays 355, matches rise 330 to 332, divergences fall 25 to 23. Untouched baseline and modified tree both have the same four expression-index failures.
- [x] (2026-09-11) Independently committed and pushed as cb76d77b97 to origin/hparser-integration, with remaining catalog and expression-index failure evidence preserved.

## Surprises & Discoveries


The old catalog panic guard in chunk Column.get_bytes claims Go treats an empty offsets vector as an empty cell, but pinned Go Column.GetBytes directly indexes offsets. The current original corpus no longer calls get_bytes with empty offsets even with a strict assertion. This is evidence that the historical panic is not currently reproduced, not proof of its original root cause. Do not claim the guard is a semantic fix. Logs: /tmp/catalog-current-sept11.log and /tmp/catalog-offsets-trace.log.

The catalog mismatch full_name is caused by an explicit unsupported return in ddl/alter_table.rs after validating generated status. Existing tests include a refusal of modifying b in a dependency chain; preserve its underlying dependency rule, do not blindly change every refusal test.

Go checkModifyGeneratedColumn compares !IsVirtualGenerated, not Option<stored>. Thus ordinary and stored-generated columns share stored status; replacing stored with ordinary is not universally forbidden. The current Rust option equality is too restrictive and accounts for another catalog difference.

## Decision Log


Decision (2026-09-11): use Go master source and binary as authority. Retain the full original catalog comparison and do not adjust the historical divergence count simply to obtain a green gate. This category should remove proven mismatches; unrelated catalog differences remain visible.

Decision (2026-09-11): implement validation before mutation and reuse generated_column expression building/materialization. Do not merely remove the unsupported return, since KvTable.modify_column_in currently converts old values, rewrites rows and indexes, and may need different handling for virtual metadata-only changes.

## Context and Orientation


Worktree /tmp/tidb-hparser-current, detached HEAD, target origin/hparser-integration. Oracle /tmp/tidb-go-master-oracle/bin/tidb-server. Root AGENTS.md requires red-before-fix regression and Ready including make lint. No subagents are authorized. Rust commands use RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432.

Rust owners: rust/crates/tidb-executor/src/ddl/alter_table.rs around 3190-3264 (status/refusal), 3590 (dependent-column error), 3660 (new KvColumn currently generated=None); rust/crates/tidb-executor/src/generated_column.rs (builders, dependency checks, materialize); rust/crates/tidb-executor/src/kv_table.rs::modify_column_in (around 2510, row conversion and persistence); rust/crates/tidb-session/src/tests_generated_columns.rs. Prefer a sibling generated-modify module for substantive new validation rather than enlarging the oversized alter_table file.

Go authority: pkg/ddl/generated_column.go::checkModifyGeneratedColumn at 202, checkIndexOrStored at 401, verifyColumnGeneration, checkIllegalFn4Generated and checkAutoIncrementRef. Read pkg/ddl/modify_column.go call ordering before implementing. checkIndexOrStored allows unchanged expression if type unchanged or unindexed; changed expression rejects stored-generated or indexed targets. Its indexed rejection is the exact text Unsupported modification for generated columns covered by an index. Compare normalized GeneratedExprString, not raw SQL spelling.

## Plan of Work


Milestone 1 is complete: reproduce a concrete catalog mismatch with nonempty rows and captured oracle. The red test creates first_name,last_name and virtual full_name, inserts Ada/Lovelace, replaces CONCAT order, reads Lovelace Ada, inserts Grace/Hopper, then expects both new expressions. Go succeeds; Rust currently refuses ALTER.

Milestone 2: implement the full validation surface for MODIFY/CHANGE. Resolve the prospective schema with renamed/repositioned column, verify all generated dependencies and ordering, apply stored-status rule and function/auto-increment checks, and enforce expression/type restrictions for indexed or stored targets. Use existing typed errors. Preserve ordinary-column dependency restrictions where Go applies them. Add negative cases and ensure rejected ALTER leaves metadata and rows unchanged.

Milestone 3: integrate the validated GeneratedColumn into KvColumn replacement. Virtual reads must evaluate the new expression over existing data, new writes must use it, and any allowed stored-to-ordinary conversion must retain correct persisted values. Reordering must update dependency offsets. Do not rebuild indexes from stale generated values. Confirm legal virtual changes do not introduce stricter old-value cast behavior than Go.

Milestone 4: run targeted tests, all generated-column and expression-index tests, session integrations, make lint, and original catalog replay. Compare complete divergence texts before/after. Only verified cases belong in the independent fix commit. Update BLOCKER_RESOLUTION_REPORT.zh-CN.md with real results; preserve unrelated remote changes when fetching/rebasing, never force-push.

## Concrete Steps


From /tmp/tidb-hparser-current, prefix Cargo commands with RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432:

    cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib modifying_virtual_generated_expression_recomputes_existing_rows
    cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_generated_columns
    cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_expression_indexes
    cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
    CATALOG_SHOW_DIVERGENCES=1 cargo test --manifest-path rust/Cargo.toml -p difftest-result-tests --test catalog_diff catalog_reads_match_recorded_tidb_output -- --nocapture
    make lint
    git diff --check

Original catalog gate may remain red because its historical divergence fingerprint/count is stale and genuine differences remain. Report that explicitly; do not weaken assertions or hide skipped reads. Full project quality is not proved by this subset.

## Validation and Acceptance


Require Go-identical row values, metadata and errors for positive and negative modifications. Original source case tests/integrationtest/t/ddl/column_modify.test:14-17 must read back the changed expression. Targeted regression must transition red to green. The catalog compare count and matched count must not regress through additional skips. Restore no diagnostic logs, and do not leave live test clusters after verification.

## Idempotence and Recovery


Use fresh Session objects in tests and dedicated oracle data/ports. Go oracle at 127.0.0.1:4409 has been shut down cleanly. All temporary instrumentation has been removed. Implementation includes ddl/generated_modify.rs, ddl/alter_table.rs, KvTable::modify_column_in, StmtContext, session context and tests_generated_columns.rs. The untouched baseline worktree /tmp/tidb-generated-modify-baseline at 4591dc5485 has finished its expression-index comparison; no background baseline process remains.

## Artifacts and Notes


/tmp/gen-modify-go.out contains successful ALTER, existing Lovelace Ada, newly inserted Hopper Grace and SHOW CREATE. /tmp/gen-modify-oracle.log records the pinned binary. /tmp/generated-modify-red.log records Rust unsupported failure. /tmp/catalog-current-sept11.log contains all 25 catalog divergences; /tmp/catalog-offsets-trace.log identifies every executed statement and verifies no empty-offset access in this replay.

## Interfaces and Dependencies


Reuse GeneratedColumn and existing expression build/validation APIs. Any new helper should return validated optional GeneratedColumn metadata or a typed DriverError before taking a mutable catalog borrow. Keep physical row conversion in KvTable's existing ownership boundary and use existing statement staging for atomicity.

## Outcomes & Retrospective


Implementation passes 25 generated-column tests and removes exactly two original catalog differences without changing comparison scope or expectations. Go /tmp/generated-modify-oracle-cases.out confirms indexed/stored/self-reference/type-reorg errors 3106/3107/8200, auto-increment default error 3109 and enabled success, and stored-to-ordinary preservation of 12 followed by ordinary update to 99. Final logs: /tmp/generated-modify-verified.log 25 pass, /tmp/generated-modify-final-integration.log 310 pass, /tmp/generated-modify-final-lint.log exit 0. /tmp/catalog-generated-modify-after.log has 23 divergences (still red); final repeat uses /tmp/catalog-generated-modify-final.log. Both /tmp/generated-modify-index-tests.log and untouched /tmp/generated-modify-index-baseline.log have identical 32 pass/4 fail: two same-column action checks and two chunk is_null bitmap panics. These remain independent failures for subsequent fixes; this category does not complete the overall quality goal.
