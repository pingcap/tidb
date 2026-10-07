# Preserve complete cluster ALTER column jobs

This living ExecPlan follows PLANS.md.

## Purpose / Big Picture


A cluster ALTER containing CHANGE or RENAME COLUMN must not silently discard other clauses. Preserve all admitted column/index actions, validate their original-schema dependencies before execution, and publish their identities, index metadata and notifications together. This repairs existing cluster transaction behavior; durable worker/recovery remains unresolved and no complete Go package is accepted.

## Progress


- [x] Verify clean 6a629d0 checkout, refresh Go master and inspect DDL contracts.
- [x] Select connected lowering, original-schema admission, shared column application, reorganization refusal and notification segments.
- [x] Baseline: six of nine Rust cases fail; all eight real MySQL/unistore cases fail after checking complete metadata and notes.
- [x] Implement connected lowering, prepared column application, original-schema admission, shared name-conflict owner and notes. Retire premature returns, unsafe same-family bypass and duplicate index-rename loops. Initial nine regressions pass; final cleanup is under grouped verification.
- [x] Final verification: 123 cluster tests, 56 shared admission tests, eight real MySQL controls, affected all-target check, lint and locked server build.
- [x] Update both registers and prepare publication through the actual hook and fresh locked-build gate. Remote verification and saved Cloud checkpoint results are recorded in `/workspace/.cloud-setup/cluster-column-batch/final-handoff.json`.

## Context and Orientation


rust/crates/tidb-exec/src/cluster_ddl.rs lowers SQL into DdlStatement then plans one metadata transaction. Its multi-action loop returns an entire single-action statement when it sees CHANGE or RENAME, dropping siblings. MODIFY is not admitted there. The single MODIFY planner also bypasses the existing need-reorganization decision for same-family changes, which can reinterpret existing bytes without rewriting them. Existing local executor admission already has Go conflict ordering but cluster planning does not consume it.

## Plan of Work


Lower every supported column spelling into ordered AlterColumnAction values. Extract shared prepared column replacement and add-column values; validate using original TableInfo, then apply by stable column ID against the evolving output. Migrate single-action consumers as well. Share conflict checking via model.MultiSchemaInfo with tidb-executor. Resolve index references and conditional no-ops against original metadata; preserve their notes and notifications. Retire unsupported same-family reinterpretation. Keep unsupported type-reorganization operations refused until a durable rewrite owner exists.

## Milestones


First, reproduce sibling loss, original-name drift, unsafe narrowing, lost notes and index-offset drift together. Then implement the complete selected column composition path before the final grouped checks. Finally verify actual SQL rows, defaults and SHOW metadata over unistore, without claiming multi-node recovery.

## Concrete Steps


Activate /workspace/.cloud-setup/env.sh and export CARGO_BUILD_JOBS=1. Run Cargo from /workspace/tidb/rust:

    cargo test --locked -p tidb-exec --test all -- cluster_column_batch a_multi_action_alter_folds_over_one_evolving_table a_column_and_index_bundle_folds_and_backfills_together a_modify_column_reorganizes_exactly_where_go_says_it_must modify_column_moves_the_column_and_refuses_a_self_anchor --test-threads=1
    cargo test --locked -p tidb-exec --test all -- cluster_ddl_source:: --test-threads=1
    cargo test --locked -p tidb-executor --lib -- multi_schema_change --test-threads=1
    cargo check --locked --all-targets -p tidb-exec -p tidb-executor -p tidb-session -p tidb-server
    cargo build --locked -p tidb-server

Run make lint from repository root. No Go, generated or dependency inputs change; no Bazel/failpoint sweep is needed. Before each push run the mandatory fresh locked build. Use the actual executable hooks/pre-commit selected by core.hooksPath=hooks.

## Validation and Acceptance


All sibling columns must appear or the whole statement must fail. Index names/offsets track unchanged column IDs. Unknown original names and conflicting jobs use Go diagnostics before writes; IF EXISTS notes survive skipped jobs. Single and bundled unsafe narrowing/sign changes must refuse instead of corrupting existing row interpretation. Reuse existing suites and temporary server recipe; no new harness.

## Idempotence and Recovery


Use existing Cloud checkouts and preserve concurrent changes. Logs live outside the checkout under /workspace/.cloud-setup/cluster-column-batch. Never force push or purge caches; preserve a verified recovery bundle before retiring one-shot helpers.

## Surprises & Discoveries


Existing cluster tests include an added-column index success case that contradicts Go original-schema admission. Migrate that fixture to an existing column and keep the missing-original-column refusal as a regression. Existing narrowing tests already describe the required metadata-only refusal but the planner bypasses their predicate. The full cluster sweep also exposed two old CREATE DATABASE fixtures that omitted the required notifier table; their Go serialization/charset obligations are consolidated on the maintained bootstrap helper. A duplicated Unsupported prefix introduced during extraction was corrected after the exact-message CHECK regression caught it.

## Decision Log


- Decision: repair the entire supported column composition path, including single callers and index/notification consumers.
  Rationale: removing early returns alone would retain unsafe type interpretation and evolved-schema admission.
  Date: 2026-10-07.

## Outcomes & Retrospective


Implementation and cleanup are complete; 179 Rust tests and eight wire controls pass, along with affected all-target checking, lint and locked server build. D11 remains partial: this does not implement Go’s durable multi-schema/reorganization worker or complete partition/cache admission.
