# Carry coprocessor read policy through the shared SQL path

This living ExecPlan follows root `PLANS.md`.

## Purpose / Big Picture


Ordinary SQL table and index reads must honor Go's adaptive replica threshold, busy-store threshold, read timeout and SELECT execution deadline. The planner supplies estimated response bytes, the statement owns session settings, and the coprocessor adjusts once after region tasks exist. Lookup table requests must estimate their own handle batches. Preserve source contracts and existing meaningful tests.

## Progress


- [x] Refresh both implementation remotes and Go master; both checkouts clean and current.
- [x] Recheck O13/N03 coprocessor producers and consumers against Go master 93a01d31f6da205ae4bf376825293903a6899fdb.
- [x] Capture grouped failing regressions in existing suites.
- [x] Implement connected settings, estimates, request consumers and counters.
- [x] Run grouped tests, all-target checks, lint and self-review; update both registers.
- [ ] Commit through actual locked-build hook, fresh locked build immediately before authorized push, verify remote and save recovery/startup checkpoint.

## Context and Orientation


Cloud checkout `/workspace/tidb` uses hparser-integration at 5259eff918f07518ce7706ee7d706d9d3dc69d73. Native `/workspace/client-rust` master is 06b4ccc2735ecf89ed57136241cb7b7d204d6c07 and needs no edits for this batch. Source owners are Go executor/builder.go newClosestReadAdjuster, planner/cardinality/row_size.go and physicalop reader size methods, sessionctx/variable and store/copr/coprocessor.go Send. Rust owns the corresponding path in tidb-session/src/stmt_ctx.rs, tidb-executor/src/{stmt_context,remote_scan,access_path}.rs and driver/physical_builder.rs, tidb-exec/src/cop_scan.rs, tidb-distsql/src/cop_paging/cop_read_task_runtime.rs. A request adjuster is a callback that decides leader versus local-zone replicas using the estimate divided by actual region-task count.

## Milestones and Plan of Work


First extend existing transport fixtures to reproduce dropped settings and missing adaptive fallback/counters in one baseline run. Then carry immutable statement settings through the existing pushdown context; derive bytes from retained physical statistics with shared row-size arithmetic; distinguish index and table lookup estimates; install the Go callback and count null/hit/miss once at task creation. No private runtime, routing cache or alternate harness is introduced. Run validation together after integration.

## Concrete Steps and Validation


Source `/workspace/.cloud-setup/env.sh` in each shell and use CARGO_BUILD_JOBS=1 for linking. From rust run targeted existing tidb-exec, tidb-executor, tidb-session and tidb-distsql suites. Record exact commands/results in parity/current-audit/cop-read-policy-batch-validation.json and logs under /workspace/.cloud-setup/cop-read-policy-batch. Run affected all-target cargo check, root make lint and git diff --check. Ordinary commit must execute hooks/pre-commit with cd rust && cargo build --locked -p tidb-server; repeat that locked build immediately before push. Never force push. Recheck remote changes before publication.

## Acceptance and Limits


Small adaptive requests fall back to leaders; large requests carry the configured zone; estimates use response columns and cardinality, including each lookup batch. Explicit modes remain unchanged. Session changes apply to new statements without changing retained contexts. Busy thresholds/timeouts reach actual request metadata. Hit/miss/null counters follow Go. This is existing-owner maintenance, not complete package transcreation. Point/batch snapshot routing, local/stale transaction scope lifecycle and complete Go package original-artifact obligations remain unresolved; no full cluster or performance claim.

## Idempotence and Recovery


Preserve concurrent changes. Save exact baseline production contents before edits if needed for fail-before checks; restore only our changes. Reuse shared target cache, do not clean it broadly. On gate failure diagnose before retrying. External logs retain commands, source identities and outcomes; no credentials are recorded.

## Interfaces and Dependencies


Reuse the existing CoprocessorRequestAdjuster trait, RequestBuilder, StmtContext and PushdownStatementContext. Use the existing planner cardinality estimator and retained HistColl, not a new approximate byte model. Keep native dependencies unchanged.

## Surprises & Discoveries


The shared task runtime already invokes an adjuster, but SQL installs none. CopScanSource copies only a subset of DistSQL settings, so valid SET values disappear before dispatch.

## Decision Log


- Decision: repair the complete connected coprocessor request path in one batch; leave independent snapshot routing explicit. Rationale: snapshot consumers have a separate ownership path and cannot be certified by coprocessor tests. Date: 2026-10-05.

## Outcomes & Retrospective


Implementation is complete. All 93 selected tests pass; one existing scaling case is ignored. All-target checks, root lint, changed-region formatting and self-review pass. Publication gates remain in progress. Finding counts remain 86 tracked, 30 repaired, 56 unresolved; O13/N03 partial.


Revision note: completed statement policy, table/covering-index estimates, index lookup table-batch estimates, direct index-merge partial estimates, adaptive callback and request counters. Removed early millisecond duration conversion and its misleading fields; all callers now use nanoseconds until task encoding. Three baseline failures are repaired. The final receipt records exact grouped commands and limits; no whole package is accepted.


Final Go review: GetMaxExecutionTime returns zero outside SELECT, even when a non-SELECT statement carries a hint. The initial unpublished batch accessor exposed the raw value; a new assertion reproduced 919 instead of 0. Publication was stopped before push. The corrected accessor preserves the raw statistics-load value, gates the coprocessor deadline by StatementClass, and passes the expanded session case plus the three affected transport cases. Affected all-target checking and lint pass again; amend and fresh publication gates remain required.
