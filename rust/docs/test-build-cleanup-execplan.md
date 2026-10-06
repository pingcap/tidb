# Retire disconnected context policy and forwarding harnesses

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove the unused executor ErrorContext/ErrorContextFlags/ErrorDisposition
model and its private tests; use tidb_error::errctx directly in live consumers.
Also remove RU-metrics and dynamic-default forwarding layers in tidb-exec.
Work in /workspace/tidb on hparser-integration from
69cdbf80ff06464f6227b0eba3ba1a62ea6a6123. Go master is
b36c940a4332c866d8b0e2afde88f5e7c2fd7fed. Current Go enables adaptive-limit
scan for every new install; Rust's TiKV-only initial-value guard is stale.

## Progress


- [x] Trace model consumers: only its own tests; identify shared live owners.
- [x] Remove four files and migrate live imports plus useful assertions.
- [x] Reproduce classic-store adaptive-limit default failure against unchanged owner.
- [x] Remove stale store guard to match freshly fetched Go master.
- [x] Validate grouped owner/consumer tests, lint, continuity and diff.
- [ ] Pass actual hook, fresh pre-push build and remote verification.
- [ ] Verify recovery bundle and save/read back Cloud checkpoint.

## Milestones and Plan of Work


Delete executor/src/error_context.rs, exec/src/ruv2_metrics.rs and the two
exec test carriers error_context_source/global_sysvar_initial_source.
Point executor stmt_context and statement_pushdown at tidb_error::errctx.
Point exec execution details, runtime stats and slow-log RU types at tidb_util.
Point bootstrap commit policy at tidb_vardef. Remove corresponding modules,
forwarding exports and obsolete comments. Keep live error handling unchanged.

Preserve group ordering/defaults and all ResolveErrLevel combinations in the
existing errctx owner test. Move the conversion-warning sink test to its
warning_publication implementation. Compact all twenty global-default vectors
into the existing vardef owner test, correcting the one classic adaptive-limit
expectation that conflicts with current Go. Its new ON expectation fails before
the production guard removal. Keep the other nineteen vectors and original
owner cases. Four tests solely exercising the disconnected model are retired;
shared errctx context and live pushdown suites remain the behavior owners.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh and set CARGO_BUILD_JOBS=1. From rust/:

    cargo test --locked -p tidb-vardef -p tidb-exec --lib -- global_sysvar_initial:: exec_details:: runtime_stats:: slow_log_format:: session_commit_protocol:: warning_publication:: --test-threads=1
    cargo test --locked -p tidb-error -p tidb-executor --test all -- errctx_source:: statement_pushdown_source:: --test-threads=1

Require nonzero passing runs for each target; record failures unchanged.
Before the adaptive guard edit, global_system_variable_initial_value_table
must fail with actual OFF versus Go ON; the same test must pass afterward.
Verify all preserved vectors and import-only production changes outside that
single policy correction. From root run make lint and git diff --check.
The real pre-commit hook must pass cd rust && cargo build --locked -p
tidb-server; rerun immediately before authorized normal push to pingcap/tidb
hparser-integration, then verify remote SHA. Never bypass hooks or force push.

## Surprises & Discoveries


The ErrorContext doc claimed it was used by pushdown, but only its re-exported
shared types had live callers. Its private model tests did not exercise SQL.
Go's GlobalSystemVariableInitialValue no longer conditions adaptive-limit scan
on the store. This is a real default-policy fix discovered during cleanup.

## Decision Log


Delete the disconnected model, not the shared errctx owner. Keep source-backed
assertions and Rust warning-sink coverage in actual owners. Fix the stale Go
expectation with fail-before/pass-after evidence. Do not claim complete package
acceptance or mark a broad finding repaired from this bounded maintenance.

## Outcomes & Retrospective


Implementation and validation complete: 35 selected tests pass; lint,
continuity and diff checks pass. The adaptive-limit case fails before and passes
after the correction. Four files and 473 net Rust lines removed. Publication pending. No broad
finding disposition changes. Native client-rust is unchanged.

## Recovery, Artifacts and Dependencies


Use git show 69cdbf80ff:<path> for individual before-images without overwriting
concurrent changes. Logs and inventory live in
/workspace/.cloud-setup/context-owner-cleanup. Durable receipt:
rust/docs/parity/current-audit/context-owner-cleanup-validation.json.
No dependency changes. Cloud draft save, Publish and fresh-task restoration
remain separate. Revision: replace completed join forwarding cleanup with
context/default/metrics ownership and stale adaptive-limit default repair.
