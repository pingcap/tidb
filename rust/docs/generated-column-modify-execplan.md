# Restore generated-column modification semantics

This living plan follows repository PLANS.md. Maintain Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective during implementation.

## Purpose / Big Picture


ALTER TABLE MODIFY/CHANGE must apply the generated-column rules of pinned Go master fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85. In particular, replacing an unindexed virtual expression must update existing and newly inserted rows and SHOW CREATE TABLE. Rejecting every generated-column modification is an implementation gap, not a Go compatibility rule. The parent goal remains all failures from BLOCKER_RESOLUTION.md, not only this category.

## Progress


- [x] (2026-09-11) Replay original catalog gate: 355 compared, 330 matched, 25 divergent, 3480 statements run for effect. Gate fails against the historical 27-divergence expectation.
- [x] (2026-09-11) Replay with temporary strict empty-offset assertion: no chunk panic or empty-offset access; same catalog totals. Remove all diagnostic instrumentation.
- [x] (2026-09-11) Capture Go master successful expression replacement and old/new row values in /tmp/gen-modify-go.out.
- [x] (2026-09-11) Add and run red session regression modifying_virtual_generated_expression_recomputes_existing_rows; /tmp/generated-modify-red.log returns unsupported.
- [ ] Implement Go validation and generated metadata replacement with correct existing-row behavior.
- [ ] Cover virtual/stored transitions, dependency order and renaming, indexed columns, invalid expressions, and statement atomicity.
- [ ] Run Ready gates and original catalog replay, inspect exact remaining divergences, then independently commit/push the completed category.

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


Use fresh Session objects in tests and dedicated oracle data/ports. Prior Go oracle at 127.0.0.1:4409 has been shut down cleanly. All temporary instrumentation has been removed. Current WIP is one red test in tests_generated_columns.rs; no production change yet. Leave that red regression local until this category is implemented and verified.

## Artifacts and Notes


/tmp/gen-modify-go.out contains successful ALTER, existing Lovelace Ada, newly inserted Hopper Grace and SHOW CREATE. /tmp/gen-modify-oracle.log records the pinned binary. /tmp/generated-modify-red.log records Rust unsupported failure. /tmp/catalog-current-sept11.log contains all 25 catalog divergences; /tmp/catalog-offsets-trace.log identifies every executed statement and verifies no empty-offset access in this replay.

## Interfaces and Dependencies


Reuse GeneratedColumn and existing expression build/validation APIs. Any new helper should return validated optional GeneratedColumn metadata or a typed DriverError before taking a mutable catalog borrow. Keep physical row conversion in KvTable's existing ownership boundary and use existing statement staging for atomicity.

## Outcomes & Retrospective


Diagnosis and red test are complete; generated-column modification is not yet fixed. Current catalog panic is not reproduced under stricter checks, but historical root cause remains unproven. The next action is implementing Go's validation and replacement semantics, not re-running readiness or changing catalog fingerprints.
