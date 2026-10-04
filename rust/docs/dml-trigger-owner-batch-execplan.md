# Share DML read and foreign-key execution owners

This living ExecPlan follows PLANS.md. The user requests coherent source repair and obsolete-code/test/harness/documentation removal in batches, grouped validation and no push.

## Purpose and context

Single and joined UPDATE/DELETE must consume the physical read child EXPLAIN describes. FK checks/cascades must execute the policy retained by the DML root. Ordinary checks see the final statement buffer; IGNORE checks candidates before mutation. The shared table writer and statement staging own validation/rollback.

Starting Cloud HEAD is 2cecf1e7c2b4907a8c5d891f0288e18dda4d9850 in /workspace/tidb on hparser-integration. Fresh Go master remains 93a01d31f6da205ae4bf376825293903a6899fdb in /workspace/.cloud-setup/go-master; native master remains 19a56ccda1e128218cd33c69709038219aced9bc. Remote integration through 91010d96bb3e32468bd56ea2b07d255248485489 is preserved unmerged. This maintains E02/E03 in B02; it does not accept entire pkg/executor or pkg/planner/core packages. Native source/dependencies are unchanged.

## Progress

- [x] Inspect live Go physicalop FK builders, executor builders/callbacks, ExecStmt.handleForeignKeyTrigger, generated update dependencies, prefix index coverage and plan cache keys.
- [x] Capture original-source session baseline (1 pass/4 fail) and original-server MySQL/unistore baseline (0 pass/4 fail) before replacement build.
- [x] Carry resolved typed FK policy in physical nodes and migrate INSERT/ODKU/REPLACE and single/joined UPDATE/DELETE consumers.
- [x] Remove single-memory-table WHERE/order/limit interpretation and obsolete helpers after shared-read migration; retain hidden adapter positions solely as internal row identity.
- [x] Share DeleteRecords, final-buffer checks and dependent cascade policy; remove redundant REPLACE CHECK prevalidation.
- [x] Delete two obsolete executor harnesses, relocate eleven valid Go cases to Session, retire nine tracked adapter/false-concurrency tests and withdraw two new adapter tests.
- [x] Fix missing Git input invalidation and verify actual build script through loose/packed/loose/detached transitions.
- [x] Run grouped session validation (260 pass), affected all-target checks and make lint; self-review source and preserve truthful intermediate failures.
- [x] Consolidate stale index/register narratives and protocol tables into current authority links; update both registers and batch receipts.
- [ ] Normal local commit must pass the actual locked server-build hook, then final MySQL/unistore regression/control run. Store actual results in external final-handoff.json.
- [ ] Refresh/verify recovery bundle, complete workspace repository survey and save exact local refs/startup instructions in the Cloud draft. No push or publication.

## Implementation and Go ownership

Physical FkTriggerNode now retains kind, child/parent identities, resolved offsets and error constraint. driver/fk_trigger_plan.rs is the sole DML policy builder; foreign_key.rs consumes the retained policy. Dependent cascades compile their policy through that builder. Generated update dependencies include transitive columns and ON UPDATE timestamp dependencies, as Go buildUpdateLists does. Prefix-safe indexes follow model.FindIndexByColumnsForForeignKey. ForeignKeyChecks enters the prepared cache environment because Go NewPlanCacheKey includes it.

UpdateRecords and DeleteRecords share mutation callbacks across entrypoints. Ordinary INSERT checks only accepted rows after writes, while IGNORE checks candidates before insertion. DELETE/REPLACE ordinary checks run after all removals, then cascades; dependent cascade writes also precede their own checks. Each cascade scans the current child image after earlier cascades. Session staging remains the rollback owner. The final source review removes the pre-delete REPLACE CHECK call that existed solely for a direct executor harness; table insertion already validates CHECK.

execute_physical_write_rows now handles stored and memory tables. The memory adapter position emitted by the shared planner survives filtering/reordering; deletion consumes descending positions. SQL stored rows exclude the position. Local memory WHERE/order/limit execution, positional sorting, permutation and limit/predicate helpers are deleted. Final row vectors and stored-handle reconstruction remain explicit E03 limits.

## Surprises & Discoveries

The old direct-executor FK harness expected session rollback without entering a session. Four tests failed after deferred checks; that does not establish four independently new production defects. All eleven valid Go cases now use existing session helpers with original expectations. The twelfth test was a serial 200-insert loop labeled concurrency; it is deleted and Go concurrent locking remains unverified. Eight tracked matrix-only adapter tests and their custom fixture are removed per user direction; two temporary fail-before adapter cases are also withdrawn. Four retained FK regressions cover Go-visible behavior.

The first session migration passes 257 cases and fails two stored-generated-reference cascades: policy selection omitted generated assignments. Extending modified dependencies restores Go's selection and both cases pass. An intermediate Arc<Vec> iteration compile error was corrected. The final grouped retry passes 260 cases, zero failed/ignored.

Actual build-script output watched a nonexistent .git/packed-refs, making Cargo perpetually stale. Existing HEAD/ref watches now handle loose, packed and detached states without freezing version metadata. An isolated actual-script check passes all four transitions; the final grouped retry recompiles executor/session while reusing util/expr. An optional standalone util diagnostic selected another feature graph and was terminated; it is not passing validation.

The audit README and register accumulated contradictory historical current-count claims. Current authority pointers replace duplicate summaries and completed declaration tables. Original receipts, protocol JSON inventories and historical failed validations remain.

## Validation and acceptance

Activate source /workspace/.cloud-setup/env.sh for every shell; run Cargo in /workspace/tidb/rust. Use CARGO_BUILD_JOBS=1 for heavy builds. Exact commands/results/source hashes are in parity/current-audit/dml-trigger-owner-batch-validation.json; logs live at /workspace/.cloud-setup/dml-trigger-owner-batch.

The grouped command is cargo test --locked -p tidb-session --lib -- tests_multi_table_dml tests_foreign_key tests_grants::table_scope tests_mem_quota tests_prepared_statements tests_prepared_plan_cache tests_alter --test-threads=1. It passes 260 cases. cargo check --locked -p tidb-planner -p tidb-executor -p tidb-session --all-targets and root make lint pass. The subsequent narrow REPLACE precheck deletion is compiled by the actual normal commit hook and covered by the final wire rollback control; grouped tests are not falsely attributed to a later source image.

The executable hooks/pre-commit selected by core.hooksPath=hooks must run cd rust && cargo build --locked -p tidb-server on the normal local commit. Do not duplicate that boundary build just to prefill a receipt. Then run the external wire-regressions.py with the rebuilt server: four fail-before cases plus CHECK3819/REPLACE rollback control. Actual SHA/hook/wire results are written after completion to external final-handoff.json.

## Decision Log

- Repair E02/E03 together because their DML root had duplicate policy and read owners; compose callers before deleting them. Date: 2026-10-04.
- Retain meaningful Go SQL expectations in Session; delete direct-harness accommodations and adapter-only tests instead of rebuilding another rollback harness. User explicitly requested this cleanup. Date: 2026-10-04.
- Consolidate current documentation and keep dated evidence separately, avoiding another contradictory headline stack. Date: 2026-10-04.
- Preserve concurrent schema-sync commits unmerged and honor no-push. This batch touches neither concurrent file. Date: 2026-10-04.

## Outcomes & Retrospective

This batch removes 388 net production lines and 277 net test lines while composing the shared DML lifecycle. Both E02/E03 remain partial. The register remains 86 tracked,29 repaired,57 unresolved (39 open,18 partial); other55 unresolved IDs retain prior evidence. Complete indexed FK lookup/locking, physical cascade substatements/runtime stats, final chunk/write streaming and planner handle metadata remain. Existing scan-error handling is inherited and not repaired here. Full Go suites, live multi-node TiKV and workload performance were not verified; prior parser14/expression3 failures remain explicit.

## Recovery and artifacts

Revert only owned hunks if needed; never reset the checkout or force push. After the gated local commit, recreate/verify /workspace/.cloud-setup/tidb-unpublished.bundle. Survey repositories across the complete workspace and save exact local refs/startup instructions in the Cloud configuration draft while preserving unrelated settings. Draft save does not publish or prove fresh-task restoration of unpublished commits. The external final-handoff.json records SHA-dependent outcomes, avoiding another build/commit merely to add its own SHA.
