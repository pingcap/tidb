# Share system-index policy and legacy column casting


This living ExecPlan follows root PLANS.md. Start at 9a5877b1555835de42cb29f410fac999167e8186 on hparser-integration in /workspace/tidb; native /workspace/client-rust master stays unchanged. Fresh Go master is 93a01d31f6da205ae4bf376825293903a6899fdb. The user prohibits pushes.

## Purpose / Big Picture


Repair connected remaining T01/K03 contracts in existing table owners. A system-table unique index insert or changed destination must carry a commit-time absence check, rather than silently overwriting another row's index. Public index deletion must assert existence. Legacy generated ENUM/SET reads on a new-collation process must match binary while retaining the declared output collation. This is maintenance of integrated owners, not completion of the whole Go table or rowDecoder packages.

## Progress


- [x] Read root instructions and current audit; safely fetch Go/integration references.
- [x] Add system-index and legacy generated-column regressions.
- [x] Capture six independent failures and implement the shared policies.
- [x] Validate 57 distinct Rust cases, lint, all-target checks and locked server build.
- [x] Prepare receipts and both registers, self-review and one normal hook-gated local commit; final commit/hook/recovery identity is recorded in Cloud table-policy-batch/final-handoff.json.
- [x] Reconcile all 57 remaining IDs into ten structural owner batches and replace per-case compilation with combined target/filter validation policy.

## Context and Orientation


rust/crates/tidb-exec/src/system_row_write.rs encodes system rows and public indexes, with table_write_policy.rs selecting independent KV flags. Existing index writes use unasserted Set/Delete. Go pkg/table/tables/index.go::Create distinguishes unique non-NULL keys (distinct) from handle-qualified keys, and Delete asserts public entries exist. Lazy optimistic inserts assert Unknown, with a duplicate-check flag only on distinct keys. No snapshot absence may be claimed without a read. Non-public index assertions are disabled in Go.

rust/crates/tidb-executor/src/kv_table/row_decoder.rs already receives a captured use_new_collation boolean, but casts still use the process-global collator. Go pkg/table/column.go::CastColumnValue clones an ENUM/SET field type for legacy tasks, sets binary matching, then restores the original datum collation. RowDecodeContext and StmtContext must carry that fact without changing shared catalog metadata or mutating process globals.

## Plan of Work


First run the added lib regressions before production edits. Reuse table_write_policy for index insert flags and public deletion assertions. Preserve unchanged index keys during row rewrites. Carry captured decoder collation mode into its expression context and apply the Go override only in the raw build-context column cast, keeping ordinary session CastValue behavior unchanged. Test ENUM and SET, distinct and nullable/nonunique indexes, deletion and moves.

## Concrete Steps


From /workspace/tidb/rust source /workspace/.cloud-setup/env.sh and run cargo test --locked -p tidb-exec --lib system_ -- --test-threads=1 and cargo test --locked -p tidb-executor --lib legacy_generated_enum_set -- --test-threads=1. Capture logs in /workspace/.cloud-setup/table-policy-batch. After fixes run related column-cast and row-decoder cases and check affected packages including server with --all-targets. From repository root run make lint. Commit normally with core.hooksPath=hooks; its cd rust && cargo build --locked -p tidb-server must pass. No push.

## Validation and Acceptance


Before fixes new tests fail on missing flags/assertions and legacy matching. Afterward all pass; non-distinct keys do not request uniqueness checks, unchanged index rewrites emit only the record, and output ENUM/SET collation stays unchanged. Existing cast/default/generated tests must retain their results and warnings. No whole-package, live multi-node or performance acceptance is implied.

## Idempotence and Recovery


Fetch preserves local commits. Tests create no external data. Never reset the branch or discard concurrent work. Preserve failed diagnostics; prune only positively identified obsolete build outputs if storage fills. Rebuild and verify the unpublished Git bundle before replacing the previous recovery file. Keep cloud repository destinations and unrelated settings intact.

## Interfaces and Dependencies


BufferMutation carries presume_not_exists and AssertionOp independently. The table layer selects them; native transport remains unchanged. StmtContext owns the captured build-context collation mode, RowDecodeContext binds the decoder's existing mode, and cast_table_value_with_flags implements binary ENUM/SET matching on a cloned FieldType. No new dependencies.

## Surprises & Discoveries


Initial fetch failed at the proxy; a turn-scoped network grant allowed the retry. Go master and integration references are unchanged. Recorded audit count is 57 unresolved, not a fresh behavioral claim for all entries.

## Decision Log


Decision: repair T01/K03 together in existing table owners. Rationale: index integrity and generated-value identity are coupled table contracts; adding separate encoders or global mode switches would duplicate ownership. Date: 2026-10-04.

## Outcomes & Retrospective


Six independent Go contracts are repaired. Fifty-seven distinct Rust cases, lint, all-target checking and locked server build pass. T01/K03 remain partial for their named broader owners; other 55 IDs are not newly behaviorally revalidated. The user identified inefficient per-case compilation; the structural batch map and combined validation policy now govern continuation. Source and receipts share one normal local commit, whose actual mandatory hook result and recovery/draft identity are recorded in the Cloud final handoff. No push.

Revision 2026-10-04: completed existing-owner validation and receipts; reconciled the ten structural batches after user feedback. Required hook gates remain mandatory.
