# Align Rust TPCC and Sysbench Plans with Go TiDB

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan is maintained according to that file and the package-completeness rule in `AGENTS.md`.

## Purpose / Big Picture

The Rust TiDB implementation on `hparser-integration` must make the same optimizer choices as the Go TiDB implementation for the TPCC and Sysbench workloads used by pull request #70403. A user can observe success by starting both servers against the same TiUP nightly storage cluster and running the parity tool: all 12 TPCC check plans and all 13,984 Sysbench plans match after normalization, and the Rust server's median throughput with two clients is no lower than the Go baseline. The correction must come from Go-equivalent statistics, join-reorder, access-path, and physical-plan behavior rather than SQL-specific EXPLAIN substitutions.

## Progress

- [x] (2026-08-15) Preserved the user's original checkout and created `/tmp/tidb-hparser-tpcc-go-align` at `origin/hparser-integration` commit `dcf1ff2471`.
- [x] (2026-08-15) Confirmed the latest remote baseline fails the focused TPCC gate: 0 matched, 9 mismatched, and 3 errors.
- [x] (2026-08-15) Identified a known-good Go-aligned Rust implementation at commit `3d1e96a030` and retained its TPCC, Sysbench, and performance receipts.
- [x] (2026-08-15) Imported the planner substrate, estimates/prepared-path behavior, and compatible executor metadata while retaining current transaction and server APIs.
- [x] (2026-08-15) Restored the eight remaining planner/executor interfaces; `cargo check --offline --locked -j12 -p tidb-server` passes.
- [x] (2026-08-15) Proved the pristine `644927fc21` Rust session baseline is clean: 1,179 passed, 0 failed, and 2 ignored.
- [x] (2026-08-15) Rejected the wholesale known-good planner snapshot because it regresses 97 current session tests even though it remains workload-good on its old base.
- [x] (2026-08-15) Rejected the isolated correlated-aggregate module experiment: it improved two planning errors but produced 0 matches, 11 mismatches, and 1 error without the complete planner substrate.
- [x] (2026-08-15) Restored statement-aware implicit `LIKE` escaping, including `NO_BACKSLASH_ESCAPES`, through the general catalog and range-building APIs.
- [x] (2026-08-15) Restored SQL-killer checks around final executor drains so cancellation reaches non-accounting executor batches.
- [x] (2026-08-15) Restored the full statement context on aggregate output resolution and propagated session `div_precision_increment` through every aggregate-hoisting path.
- [x] (2026-08-15) Reduced the imported planner regression gate from 97 to 52 failures: the latest full run reports 1,127 passed, 52 failed, and 2 ignored.
- [x] (2026-08-15) Reconciled Go join-predicate placement, outer-join simplification, and EXISTS decorrelation; the focused placement gate reports 16 passed and 0 failed, and semi/anti-semi hash execution matches the nested-loop reference.
- [x] (2026-08-15) Restored the complete non-MV IndexMerge path accidentally removed by the planner snapshot; the focused index-hint gate reports 16 passed and 0 failed.
- [x] (2026-08-15) Reconciled TopN placement with fresh Go nightly evidence: direct one-column DISTINCT now builds TopN above its two-phase HashAgg, the grouped aggregate prints Go's output-column mapping, and the focused TopN gate reports 8 passed and 0 failed.
- [x] (2026-08-15) Reconfirmed all TPCC consistency plans against the current working Rust server: 12 matched, 0 mismatched, and 0 errors in `/tmp/tidb-local-parity-20260815-tpcc-check-reconfirmed.json`.
- [x] (2026-08-15) Restored Go's DML reader boundary for consumed handle ranges; `a_consumed_handle_range_update_keeps_its_table_reader` fails without the fix and passes with `Update -> TableReader(root) -> TableRangeScan(cop[tikv])`.
- [x] (2026-08-15) Restored Go's split between DataSource logical rows and chosen-path access rows for residual selections; `residual_selection_uses_logical_rows_over_access_rows` failed with `9.49 != 53.35` before the fix and passes after the logical estimate is carried into `Selection`.
- [x] (2026-08-15) Restored Go's grouped-row estimate for the delivery SUM plan; the cluster regression failed with `1420.00 != 1.00` before the fix and the TPCC run gate improved to 58 matched, 4 mismatched, and 1 error afterward.
- [x] (2026-08-16) Replaced the trace-only ordered IndexLookUp limit with Go's complete pushed-limit execution path and retained focused ordered-limit/cardinality regression receipts.
- [x] (2026-08-16) Reconciled all TPCC transaction plans; the final run receipt reports 63 matched, 0 mismatched, and 0 errors.
- [x] (2026-08-16) Reconciled all TPCC consistency checks and Sysbench statements; the final receipts report 12/12 and 13,984/13,984 respectively.
- [x] (2026-08-16) Rebuilt condition 9's grouped projected point-read at runtime; the focused regression now passes without a projection-width error.
- [x] (2026-08-16) Reconciled post-reorder leaf pruning with physical properties: compact MergeJoin keys resolve by relation-qualified name, truncated index prefixes no longer claim delivery, committed IndexJoin readers become dynamic range scans, and the through-projection gate reports 14/14.
- [x] (2026-08-16) Reconciled the remaining current-package regressions; fresh gates report `tidb-executor --lib` at 711 passed, 0 failed, 4 ignored and `tidb-session --lib` at 1,179 passed, 0 failed, 2 ignored.
- [x] (2026-08-16) Reconciled the complete Go `pkg/planner/core` and `pkg/planner/funcdep` mapping with current remote behavior; all affected executor and session suites pass.
- [x] Build the reconciled release Rust server and obtain TPCC transaction parity of 63/63 plus consistency parity of 12/12 against Go TiDB.
- [x] Obtain Sysbench parity of 13,984/13,984 on the reconciled branch.
- [x] (2026-08-16) Regenerated final release receipts: TPCC transaction 63/63, TPCC consistency 12/12, and Sysbench 13,984/13,984, all with zero mismatches and errors.
- [x] (2026-08-16) Ran five alternating two-client measurements; Go median is 6,381.0 QPS and Rust median is 6,824.0 QPS (Rust/Go 1.0694), with zero errors.
- [x] (2026-08-16) Completed the Ready verification profile: formatting/diff checks, focused and full Rust suites, release build, final workload receipts, and `make lint` all pass.
- [x] (2026-08-16) Completed the cross-crate self-review and focused PD/server gates: timestamp future 1/1, MaxTS/snapshot 20/20, autocommit transaction 21/21, prepared AST 1/1, and loaded-statistics propagation 1/1.
- [x] (2026-08-16) Prepared one coupled package-coherent commit inventory for the complete Go planner/core + funcdep mapping and its required runtime integration; splitting it would leave non-compiling cross-crate interfaces.
- [x] (2026-08-16) Fetched `origin/hparser-integration`; `FETCH_HEAD` matched local baseline `644927fc21`, so no integration or gate-invalidating conflict was required.
- [x] (2026-08-16) Pushed implementation commit `159bbd52e3` to `origin/hparser-integration` by fast-forward with no force.

- [x] (2026-08-23) Re-established the benchmark environment: tiup playground v9.0.0-beta.2.pre-nightly tag `tpccbench` over the preserved 15 GB 100-warehouse dataset; Rust release server from HEAD on :4001 against PD :2379, Go nightly TiDB on :4000.
- [x] (2026-08-23) Fresh alternating A/B baseline (6 x 180 s rounds, 2 threads): Go median 250.63 QPS, Rust median 53.26 QPS, ratio 0.2125. No 9007 write conflicts remain on HEAD; per-round *_ERR counts are end-of-deadline client cutoffs, symmetric across both servers.
- [x] (2026-08-23) Isolated the cost structure: plain in-txn reads are near parity (0.635 vs 0.461 ms), but point UPDATEs pay ~2x (1.871 vs 0.711 ms) because each is a snapshot read plus a separate PessimisticLock round trip where Go fuses both through `lockCtx.InitReturnValues` + `doLockKeys` and reads the value back out of `TxnCtx.PessimisticLockCache`.
- [x] (2026-08-23) Diagnosed multi-region commits: a delivery transaction spans 13 regions / ~250 mutations, declines 1PC and async commit, and paid seven sequential Prewrite round trips (~9 ms) plus sequential secondary Commits where Go admits all batches concurrently (`doBatches`).
- [x] (2026-08-23) Landed concurrent write rounds in tidb-txnkv (`publish_prewrites` / `publish_commits`, prewrite and secondary-commit loops drain into admitted-before-awaited rounds); delivery-shape commit phase 11.3 -> 7.1 -> 6.7 ms.
- [x] (2026-08-23) Wired the resolved `@@tidb_enable_async_commit` / `@@tidb_enable_1pc` protocol through the cluster-session path (TransactionThread arms, SessionTransaction constructors, `commit_staged_buffer`, every caller); live server now reports `commit_protocol=OnePc primary_pubs=0` for single-region pessimistic commits.
- [x] (2026-08-23) Pushed d2aa3c792e and b031e818ac to origin/hparser-integration; tidb-txnkv 541 tests green (one pre-existing lock_resolver_source structure failure belongs to a parallel session's snapshot_read.rs change), tidb-exec 1062 green including the new wiring contract.
- [x] (2026-08-23) Post-fix alternating A/B: Go median 238.13, Rust median 59.41 QPS, ratio 0.2495 (+17% over baseline). Remaining dominant gap: one extra sequential RPC per point DML (read-then-lock vs Go's fused value-returning lock).

- [x] (2026-08-23) Landed the parallel session's prepared-DML fix review receipt plus a complementary regression (`a_prepared_update_the_fast_planner_declines_binds_its_parameters`): composite-key prepared UPDATEs declined by `run_fast_prepared_update` must execute their BOUND tree; the fallback reparsed raw `?` text and answered 1105 on every payment/new-order district update.
- [x] (2026-08-23) Switched the node's global allocator to tikv-jemallocator behind the `jemalloc` feature: per-statement marginal cost 1.19 -> 0.92 ms (-23%), delivery range UPDATE 5.3 -> 3.0 ms, delivery-shape COMMIT 6.7 -> 3.9 ms.
- [x] (2026-08-23) Final alternating A/B (6 x 180 s): Go median 239.57, Rust median 83.61 QPS, ratio 0.3490 — Rust throughput +57% across the session at constant Go throughput. Zero rewriter errors, zero unexpected conflicts; residual *_ERR counts remain end-of-deadline cutoffs on both sides.
- [x] (2026-08-23) Verified no-regression on current HEAD with a controlled mini-A/B after data drift: Go median 246.81, Rust median 89.57 QPS, ratio 0.3629 — the session's best receipt. The earlier smoke deltas that looked like regressions were dataset drift plus one real crash (below).
- [x] (2026-08-23) Tried and REVERTED: extending `run_fast_prepared_update` to composite clustered keys + arithmetic SET expressions (rewrite/eval against the old row). Micro-benchmarks improved (composite point updates -25%), but full-workload medians moved 83.61 -> 81.58 (within noise, no gain), so per the no-regression constraint the tree went back to origin behavior. Two durable findings from the attempt: (1) the first cut answered Error 8112 for row-constructor `(...) IN (...)` UPDATE shapes because the key extractor returned an error instead of declining — ANY generalization of this path must decline with `Ok(None)` for every shape it does not fully cover, or go-tpc delivery crashes; (2) per-execution `TableResolver` construction clones every column name and field type, which makes the fast path pay back much of its planning saving — a borrowed resolver over `&[KvColumn]` is prerequisite for it to ever pay.
- [x] (2026-08-23) Landed row-decode metadata caching keyed on a new schema-metadata epoch (`metadata_version`): DML's over-approximated mutation counter no longer invalidates the TIDB_DECODE_KEY snapshot on every write statement — perf had pinned its rebuild at 1.1% CPU plus allocations inside the memmove/malloc band. Go alignment: the decoder caches hang off the infoschema version, which DDL moves and DML never does; bumps happen at mutator ENTRY so invalidation can never go stale. Receipt after merge: Go median 236.43, Rust median 85.64 QPS (ratio 0.3622), consistent with the pre-rebase 89.57/0.3629 within data-drift noise.
- [x] (2026-08-24) Loop iteration 1 (no-regression loop established): synced to origin tip; fixed two stale pre-existing failures - fix control 52592 now gates ALL SELECT point-get shapes (`plan_fast_point_get` entry per Go `TryFastPlan`'s own first gate `point_get_plan.go:83`, plus the FROM single-point conversion and the join leaf's Point arm per `find_best_task.go:2194`; tidb-session 1385->1387 green), and the overflow-message test was re-pinned to live Go nightly behavior (folded constants carry `(9223372036854775807 + 1)`; runtime column arithmetic remains open divergence #181).
- [x] (2026-08-24) Clean alternating A/B on origin tip + jemalloc feature: Go median 224.70, Rust median 109.54 QPS over 4x150s rounds (rust rounds tightly clustered 96.07..115.51), ratio 0.4875 - Rust throughput up ~106% since the loop baseline (53.26), no regressions observed in any landed commit.

- [x] (2026-08-24) Loop iteration 2: landed the batched batch-point-get read (`KvTable::stored_records_batched` + `HandleSourceExec` prefetch at `open`) — five-key batch read 1.695 -> 0.942 ms, NEW_ORDER-shaped stock lookup 2.799 -> 1.096 ms, full transaction probe -19%. Controlled A/B after landing: Go median 233.82, Rust median 125.84 QPS, ratio 0.5382 — crossed the halfway mark, rust rounds clustered 119.97..127.58, zero unexpected errors.
- [x] (2026-08-24) Incident recorded for the loop's protection rules: mid-receipt, the shared playground's TiKV was SIGKILLed by an outside actor (`tikv-0 quit: signal: killed` in the playground log), collapsing BOTH servers' throughput and producing hundreds of payment errors. The loop must verify playground component health BEFORE a receipt (all of :2379/:20160/:4000 listening, tikv process alive) and discard any receipt window that overlaps a component death.
- [x] (2026-08-24) Diagnosed (open, deferred under the loop's 30-minute rule): `tpcc_condition_six`'s Sort(Build) regression. The enforced-merge-sort delivery checks (`merge_decision::delivers`) all PASS; the Sort is inserted by the INNER derived select's own ORDER BY handling — its `order_satisfied` chain stopped recognizing that the grouped stream-agg output delivers `ORDER BY ol_w_id, ol_d_id, ol_o_id`. Next probe: instrument the `order_satisfied` computation for the derived-select shape and the `AggregationOrder::required_for` re-resolution.
- [x] (2026-08-24) Iteration 3 RPC census (PROBE instrumentation, 20 new_order rounds): each transaction issues ~12 sequential point Gets + ~5 PessimisticLocks + ~3 BatchGets. The five pessimistic locks are the district/stock/insert duplicate-checks (expected); the BatchGet is the batched stock lookup (expected); but ~10 of the Gets are UNACCOUNTED — they do NOT pass through the cluster-session `TransactionRequest::Get` handler at all, so they originate from a read seam outside it (most likely the prepared-point-get / session fast-read path issuing its own per-key storage reads). Locating and batching that seam is the highest-value next probe: at ~0.1-0.2 ms per Get on loopback, ten of them are ~1-2 ms of every NEW_ORDER transaction. into its PessimisticLock via `return_values` + a per-statement lock-value cache (Go `InitReturnValues` / `SetPessimisticLockCache`) — needs executor access to locked values inside `run()` and spans planner/executor/storage/session. Second lever: concurrent region admission for multi-batch pessimistic lock acquisition (same doBatches shape already landed for prewrite/secondary-commit). Third: statement-CPU cost (parse/plan/stage) is now the dominant residual; profile-guided allocation trimming above jemalloc.

## Surprises & Discoveries

- Observation: transplanting the three known-good snapshot commits wholesale is incompatible with the current branch because unrelated session, transaction, and server APIs have moved.
  Evidence: the wholesale attempt produced hundreds of compile errors, while selectively restoring Go-aligned planner behavior and preserving current APIs reduced the result to eight localized errors.

- Observation: the latest remote is not merely different in five plans; three focused statements fail to plan and all nine remaining normalized plans differ.
  Evidence: `/tmp/tidb-local-parity-20260815-tpcc-latest-remote-check.json` reports 0 matches, 9 mismatches, and 3 errors.

- Observation: the old Go-aligned implementation already satisfies all requested behavioral gates on the preserved dataset.
  Evidence: `/tmp/tidb-local-parity-20260815-tpcc-old-current.json` is 12/12, `/tmp/tidb-local-parity-20260815-sysbench-stable.json` is 13,984/13,984, and `/tmp/tidb-local-parity-20260815-bench-alternating/summary.json` reports Go median 5886.67 QPS and Rust median 6406.67 QPS.

- Observation: the current branch compiles after the localized compatibility restoration, but the compiler reports `IndexLookupAggregation::apply` and `IndexLookupPlan::aggregation` as unused.
  Evidence: the successful `cargo check` emits both dead-code warnings, so focused execution tests must prove and likely complete that runtime path before end-to-end parity.

- Observation: the existing TPCC grouped-lookup regression compiles after test harness API reconciliation but fails with a HashJoin plan; the active physical builds report `NoRowSource` because the latest branch does not wire its already-present derived/decorrelation optimizer modules into `driver.rs`.
  Evidence: `cargo test -p tidb-executor tpcc_condition_nine_rebuilds_grouped_history_over_index_lookup -- --nocapture` fails at the plan assertion, while temporary test-only diagnostics showed `Chosen::Refused(NoRowSource)` at both join builds. The diagnostics were removed after recording the result.

- Observation: workload parity is not sufficient package evidence on the latest branch.
  Evidence: pristine `644927fc21` reports `1179 passed; 0 failed; 2 ignored`, while the imported planner snapshot reports `1082 passed; 97 failed; 2 ignored` in the same `tidb-session --lib` gate.

- Observation: the original dirty `e9f9c63fa6` worktree's five remaining TPCC mismatches are not a safe integration base.
  Evidence: pristine `e9f9c63fa6` reports `994 passed; 0 failed; 9 ignored`; the dirty planner reports `942 passed; 52 failed; 9 ignored`.

- Observation: four later session/executor contracts were missing from the imported snapshot but can be restored without weakening the Go-aligned optimizer core.
  Evidence: the focused regressions `no_backslash_escapes_like_default_reaches_a_table_filter`, `query_cancellation_reaches_non_accounting_executor_batches`, `a_folded_last_insert_id_publishes_even_when_a_later_expression_fails_to_resolve`, and `avg_metadata_uses_the_sessions_div_precision_increment` all pass after their general statement-context paths were restored.

- Observation: predicate placement is now package-clean, but two current Rust column-pruning assertions encoded behavior that disagrees with Go's optimizer rules.
  Evidence: `tests_join_predicate_placement::` reports 16 passed and 0 failed; Go `LogicalJoin.PruneColumns` and `LogicalProjection.PruneColumns` recursively narrow derived-join leaves, while `canProjectionBeEliminatedStrict` removes the full-row identity projection. The corresponding Rust gate initially reported 10 passed and 2 failed because it expected a wide derived leaf and matching projection counts.

- Observation: the imported planner snapshot removed IndexMerge across hint parsing, path costing, physical source execution, driver commitment, and EXPLAIN, not just at one selection call.
  Evidence: the current branch initially produced no `IndexMerge` for `tidb_enable_index_merge_controls_automatic_or_paths`; after restoring the coherent chain, `tests_index_hints::` reports 16 passed and 0 failed.

- Observation: the unhinted control in `force_index_constrains_the_access_path` expected obsolete wrappers above a point get.
  Evidence: a fresh Go nightly capture on port 45000 reports only `Point_Get ... table:t ... handle:2` for `SELECT b FROM t WHERE a = 2`, while the hinted statement remains `IndexReader -> Projection -> Selection -> IndexFullScan`. The Rust expectation was narrowed to the one `table:t` access object without changing production behavior.

- Observation: the two remaining TopN failures had different causes despite sharing one test module.
  Evidence: fresh Go nightly captures show `count(1)->Column#N` for grouped aggregation, making one Rust expected string stale, while `SELECT DISTINCT a ... LIMIT 2` uses `TopN -> HashAgg(root) -> TableReader -> HashAgg(cop)`, proving Rust's `Limit -> Sort -> HashAgg` was a production planning gap. After placing the deferred TopN above direct DISTINCT aggregation, `tests_topn::` reports 8 passed and 0 failed.

- Observation: PR #70403's 12 TPCC consistency checks align because they exercise a small set of shared Go planner contracts, not because the implementation recognizes the 12 SQL strings.
  Evidence: the retained regressions group the checks under ordered grouped aggregation, predicate propagation through derived tables, outer-join simplification, grouped index-join reconstruction, unique-group elimination, and correlated aggregate decorrelation; the current check receipt remains 12/12 while transaction-run cardinality and ordering cases still differ.

- Observation: Go preserves separate cardinalities for a DataSource after its complete predicates and for the rows covered by the chosen access path.
  Evidence: the live customer-by-last plan has `Selection 232.05` over `TableRangeScan 3520.00`; the focused synthetic regression failed before the fix with `Selection 9.49` instead of the complete-predicate estimate `53.35`, then passed when `AccessPathCommit.logical_rows` reached the residual Selection.

- Observation: cluster statistics reach the Rust catalog intact, but the specialized partial grouped-SUM trace discarded their group-key NDV and guessed `logical_rows * 0.8`.
  Evidence: `loaded_column_ndv_reaches_grouped_cluster_plans` proved table ID 130 and `ol_d_id` NDV 10 survived loading, then failed with `1420.00 != 1.00`; passing the derived grouped row count into `partial_grouped_sum` made the test pass and fixed `delivery.select_sum_amount` in `/tmp/tidb-local-parity-20260815-tpcc-run-group-ndv.json`.

- Observation: commit `002bb0d379` is useful design evidence for ordered lookup limits but is not a self-contained implementation snapshot.
  Evidence: its `driver.rs` calls `accept_embedded_lookup_limit`, and its `access_path.rs` implements that trait method, while the same tree's `table_access.rs` does not declare the method and therefore cannot compile as a complete interface closure.

- Observation: workload parity is complete, but the complete `tidb-session` package gate still exposes three production contract gaps and seven stale or incomplete observations.
  Evidence: the 2026-08-16 gate reports 1,168 passed, 11 failed, and 2 ignored. Two failures show IndexJoin decoding all visible columns, one omits Go's nullable-key Selection trace, one misses Go's hash-table memory delta and therefore does not raise error 8175, and seven expectations disagree with live Go nightly plans or discard access-range predicate evidence.

- Observation: recursive leaf pruning compacted executor schemas before MergeJoin verified the property offsets captured by its logical promise.
  Evidence: the exact through-projection query fell from two MergeJoins to HashJoins until required keys were re-resolved from `left_required_names` and `right_required_names` against each compact child scope; the 14-test through-projection gate then passed.

- Observation: a covering access path may already have recorded an `IndexReader` before the parent commits IndexJoin.
  Evidence: the expression-key regression left `IndexFullScan` under that reader until `index_join_inner_scan` learned to unwrap the standalone reader, rewrite the scan to `IndexRangeScan`, and rebuild the committed reader boundary.

- Observation: base-table FD metadata cannot be interpreted at physical catalog offsets after leaf pruning has compacted the executor schema.
  Evidence: `fd_u2(b,c,d)` incorrectly carried `lax_keys=[[1,2]]` and `not_null=[1,2]` after pruning original column `a`; rebinding catalog columns by name produces Go's expected `strict_keys=[[0,1]]`, `lax_keys=[[0,2]]`, and `not_null=[0,1]`, and the harvested Go FD regression passes.

- Observation: predicate pushdown must not erase predicates from the logical statement used by `ONLY_FULL_GROUP_BY`.
  Evidence: after `WHERE fd_r.pk IN (7,9)` was consumed by an access path, the checker could not wake the LEFT JOIN's conditional equality FD; retaining the pre-pushdown semantic SELECT restores Go's Selection-before-check behavior and the full FD regression passes.

## Decision Log

- Decision: Benchmark-first loop with per-package landings. The TPC-C gap is measured before and after every landing so each package-level commit carries its own throughput receipt.
  Rationale: AGENTS.md requires verifiable evidence per claim; the alternating A/B median cancels dataset drift between rounds.
  Date/Author: 2026-08-23, ox-alpha.

- Decision: Port the concurrent region-batch admission as one tidb-txnkv change rather than splitting prewrite from secondary commits.
  Rationale: Both loops share the PublishedCommand seam and the regroup-retry contract; splitting would leave one loop paying the other's round trips inside the same receipt window.
  Date/Author: 2026-08-23, ox-alpha.

- Decision: Keep the cluster-session protocol wiring coupled across tidb-exec and tidb-server in one commit.
  Rationale: The constructor signatures change across the crate boundary; splitting would not compile per commit.
  Date/Author: 2026-08-23, ox-alpha.

- Decision: Defer the point-DML read+lock fusion (Go's InitReturnValues shape) until the current landings are verified; it requires executor-level access to locked values inside `run()` plus a session-side pessimistic-lock cache, spanning planner, executor, storage, and session layers.
  Rationale: It remains the largest single remaining lever (~+0.85 ms per point DML, ~12 statements per NEW_ORDER) but its blast radius needs its own verified cycle.
  Date/Author: 2026-08-23, ox-alpha.



- Decision: Restore general optimizer and executor behavior instead of special-casing `condition_02`, `condition_04`, `condition_10`, `condition_11`, or `condition_12`.
  Rationale: the Go implementation is the semantic source of truth, and the Sysbench gate requires general behavior. Query-specific plan rewrites would conceal incorrect estimates and physical properties.
  Date/Author: 2026-08-15, Codex.

- Decision: Finish package correctness before pushing the already workload-clean tree.
  Rationale: `AGENTS.md` makes a complete Go package the minimum source-of-truth unit. IndexJoin decode pruning, residual Selection visibility, and HashJoin memory accounting are observable `pkg/planner/core` and `pkg/executor` contracts, so workload parity alone is not sufficient to claim or push the package.
  Date/Author: 2026-08-16, Codex.

- Decision: Remap catalog functional dependencies by stable column names and retain a separate pre-pushdown semantic SELECT for grouped-query checks.
  Rationale: Go identifies logical columns independently of physical output positions and runs `ONLY_FULL_GROUP_BY` while Selection predicates are still present. Positional repair or SQL-specific exceptions would fail under join reorder and other pruning shapes.
  Date/Author: 2026-08-16, Codex.

- Decision: Use the known-good Rust implementation only as a transcreation receipt and compare every retained behavior with its Go package counterpart.
  Rationale: `AGENTS.md` requires one complete Go package as the minimum claim unit; an old Rust snapshot alone is not source authority.
  Date/Author: 2026-08-15, Codex.

- Decision: Preserve the latest branch's unrelated transaction, session, and server APIs while importing planner behavior selectively.
  Rationale: those APIs changed after the known-good commit and are not part of the plan-parity contract.
  Date/Author: 2026-08-15, Codex.

- Decision: Update the two stale Rust column-pruning expectations instead of restoring their previous production behavior.
  Rationale: preserving a full-width derived leaf or retaining a full-row identity Projection would contradict the authoritative Go implementations in `logical_join.go`, `logical_projection.go`, `logical_datasource.go`, and `rule_eliminate_projection.go`.
  Date/Author: 2026-08-15, Codex.

- Decision: Import the coherent known-good optimizer core (`driver.rs`, `driver/from.rs`, `driver/join_reorder.rs`, and lookup execution in `join.rs`) as one unit, then adapt genuine latest-branch API differences.
  Rationale: isolated compatibility methods compile but leave the newer optimizer modules unreachable and row estimates absent at physical build time. Continuing function-by-function would create a partial Go-package claim and cannot satisfy the package-completeness rule.
  Date/Author: 2026-08-15, Codex.

- Decision: Do not run `make bazel_prepare` while the change remains Rust/protobuf-only.
  Rationale: the Bazel gate in `AGENTS.md` is not triggered by the current path inventory. Reevaluate if Go, Bazel, module, file-layout, or top-level Go test changes appear.
  Date/Author: 2026-08-15, Codex.

- Decision: Reconcile failures by semantic cluster and retain a change only when both a focused regression and the corresponding Go package contract support it.
  Rationale: this prevents a later Rust compatibility restoration from silently undoing the imported optimizer behavior and avoids using historical Rust commits as the source of truth.
  Date/Author: 2026-08-15, Codex.

- Decision: Restore IndexMerge as one coherent planner/executor chain while adapting ordinary access selection to retain the current branch's planner-candidate receipt.
  Rationale: restoring only the old executor or changing the test would leave Go's `USE_INDEX_MERGE`, `NO_INDEX_MERGE`, session switch, cost competition, and runtime semantics internally inconsistent; replacing whole files would discard newer physical-candidate metadata.
  Date/Author: 2026-08-15, Codex.

- Decision: Fuse `ORDER BY ... LIMIT` for direct one-column DISTINCT only after the deduplicating HashAgg is built.
  Rationale: fusing below aggregation can discard duplicate rows and change results, while leaving Sort plus Limit disagrees with Go's physical plan. The post-aggregation TopN matches both Go semantics and its current EXPLAIN shape.
  Date/Author: 2026-08-15, Codex.

- Decision: Treat PR #70403 as localization evidence and reproduce its package-level Go contracts rather than transplanting its Rust snapshot or adding TPCC statement-specific branches.
  Rationale: current 12/12 check parity shows the shared contracts survive independently of the transaction-run gaps, while the old wholesale snapshot caused 97 current-package regressions.
  Date/Author: 2026-08-15, Codex.

- Decision: A residual Selection over a committed single-table access path uses `AccessPathCommit.logical_rows`; the scan retains its access estimate, and join-leaf residuals without an authoritative DataSource estimate keep selectivity scaling.
  Rationale: this matches Go's `DataSource.StatsInfo().RowCount` versus `AccessPath.CountAfterAccess` split and preserves the existing join-leaf fallback.
  Date/Author: 2026-08-15, Codex.

- Decision: A partial grouped aggregate uses the group-key NDV already derived from logical statistics rather than a fixed fraction of its input rows.
  Rationale: this matches Go `cardinality.EstimateColumnNDV`, `property.StatsInfo.Scale`, and `LogicalAggregation.DeriveStats`; the pseudo-statistics fallback remains only when no logical grouped estimate exists.
  Date/Author: 2026-08-15, Codex.

- Decision: Implement ordered IndexLookUp limit sinking as one executor-package data path instead of extending the current trace-only rewrite.
  Rationale: Go `pkg/planner/core.sinkIntoIndexLookUp` removes the root Limit, stores the SQL offset/count on `PhysicalIndexLookUpReader`, and adds a projection when its schema differs; Go `pkg/executor/distsql.go` skips the offset in the index handle stream before table lookup. A trace-only rewrite cannot preserve nonzero-offset semantics or prove the performance benefit.
  Date/Author: 2026-08-16, Codex.

- Decision: Preserve physical ordering only when every column in the chosen index order survives pruning, and resolve merge requirements by stable names after each child is built.
  Rationale: dropping a missing leading index column cannot turn `(b, a)` into an order on `(a)`, and pre-prune offsets cannot identify a compact executor row. These rules match Go's property-prefix and second ColumnPruner contracts without query-specific branches.
  Date/Author: 2026-08-16, Codex.

## Outcomes & Retrospective

The reconciled implementation satisfies the final local workload, package, and Ready gates: TPCC transaction plans are 63/63, TPCC consistency plans are 12/12, Sysbench plans are 13,984/13,984, and Rust's five-pair two-client median is 6,824.0 QPS against Go's 6,381.0 QPS. Full executor and session library suites are clean, as are the focused PD future, MaxTS/snapshot, autocommit transaction, prepared-AST, and statistics-propagation gates. The implementation was committed as one coupled package-coherent unit at `159bbd52e3` and pushed normally to `origin/hparser-integration`.

## Context and Orientation

The active worktree is `/tmp/tidb-hparser-remote-latest`. Its implementation baseline was `644927fc21`, which matched the fetched `origin/hparser-integration` tip before the final commits. Rust implementation code is under `rust/crates`. The primary optimizer bridge is `rust/crates/tidb-executor/src/driver`: `join_reorder.rs` creates a Go-like logical cardinality model, `index_join_decision.rs` and `merge_decision.rs` compare physical join choices, and `from.rs` assembles the committed plan. `rust/crates/tidb-executor/src/access_cost.rs` models analyzed table and index statistics. `rust/crates/tidb-planner` derives cardinalities and costs. Lookup execution crosses `tidb-executor`, `tidb-exec`, `tidb-proto`, and server integration code.

The Go source of truth lives in complete Go packages, principally `pkg/planner/core`, `pkg/planner/cardinality`, and their required statistics/executor dependencies. Before the final claim, inventory every production file, generated or platform variant, original test/support artifact, fixture, and validation gate for each claimed package. Rust may split that behavior across crates, but no partial Go file, function, or feature is to be reported as a completed package.

The shared parity cluster uses PD at `127.0.0.1:43379`, Go TiDB at `127.0.0.1:45000`, and the preserved TiUP data directory `/tmp/tidb-parity-tiup/data/codex-tpcc-plan-fixes-20260814`. Port `45900` is reserved for this branch. The TPCC database is `tpcc_parity_10wh`; the Sysbench database is `sysbench_parity_32x10k` and contains 32 tables with 10,000 rows each.

## Plan of Work

First, classify the 97 session failures by the production module that changed and reproduce one representative failure from each cluster. Compare the current remote implementation, imported planner implementation, and authoritative Go package behavior. Restore the current implementation when the planner snapshot accidentally replaced unrelated later behavior; adapt the planner path only when the Go optimizer contract requires it.

Second, run the smallest focused Rust checks that exercise each restored contract. In particular, cover index lookup row decoding and aggregation because the current branch introduced generic lookup aggregation metadata while preserving newer runtime APIs. Fix any runtime path only when a failing test or end-to-end query shows the metadata is not consumed.

Third, build an optimized Rust server on port `45900` against the existing nightly playground and run the parity tool. Start with TPCC so feedback is bounded, inspect every mismatch against both Go EXPLAIN and the Go implementation, and repeat until all TPCC run and check statements match. Then run the full 13,984-case Sysbench plan check.

Fourth, run alternating Go/Rust two-client measurements on the same data and settings to reduce temporal bias. Accept only if the median Rust QPS is no lower than the median Go baseline.

Finally, run the Ready profile, review the complete diff for unrelated snapshot residue, reorganize temporary WIP commits into package-coherent changes, fetch the latest remote, integrate without rewriting the remote branch, rerun any invalidated gates, and push normally.

## Concrete Steps

Run Rust compile and focused tests from `/tmp/tidb-hparser-remote-latest/rust` with a reusable target directory:

    CARGO_TARGET_DIR=/tmp/tidb-hparser-remote-latest-target cargo check --offline --locked -j12 -p tidb-server
    CARGO_TARGET_DIR=/tmp/tidb-hparser-remote-latest-target cargo test --offline --locked -j12 -p tidb-executor <focused-test-filter>

Build the release server from the same directory:

    CARGO_TARGET_DIR=/tmp/tidb-hparser-remote-latest-target cargo build --offline --locked -j12 --release -p tidb-server

Run the focused TPCC parity check from any directory after the Rust server is ready on port `45900`:

    python3 /tmp/tidb-parity-tools/pr70403-runner/plan-parity.py --manifest /tmp/tidb-parity-tools/local-manifest-10wh-10k.json --go-tpc-root /tmp/tidb-parity-tools/go-tpc-src --sysbench-root /tmp/tidb-parity-tools/sysbench-src --sysbench-contract-root /tmp/tidb-parity-tools/local-contract --go-port 45000 --rust-port 45900 --tpcc-database tpcc_parity_10wh --sysbench-database sysbench_parity_32x10k --suite tpcc --phase check --allow-incomplete-manifest --output /tmp/tidb-local-parity-20260816-tpcc-go-align.json

Use the same tool with the Sysbench suite and a distinct receipt for the full plan gate. Use the preserved alternating benchmark harness and write a new summary under `/tmp` for this branch rather than overwriting known-good evidence.

Before pushing, run from the repository root:

    cd rust && cargo fmt --all -- --check
    make lint
    git fetch origin hparser-integration
    git rebase origin/hparser-integration
    git push origin HEAD:hparser-integration

Expected output includes a clean `cargo check`, focused tests reporting zero failures, TPCC `12/12`, Sysbench `13984/13984`, Rust median QPS greater than or equal to Go median QPS, a successful lint command, and a non-forced push.

## Validation and Acceptance

Compilation alone is not acceptance. The new Rust binary must produce normalized plans identical to the Go server for all 12 TPCC statements and all 13,984 Sysbench cases against the fixed datasets. No statement may be skipped or accepted as an error. The alternating benchmark must use two clients and at least the same repetition and median calculation as the preserved known-good harness; Rust passes when `rust_median_qps >= go_median_qps`.

Focused tests must cover the restored statistics and physical-property behavior and the cross-crate lookup execution path. Formatting and `make lint` are required because this is a Ready claim with code changes. The final diff and commit inventory must allow a reviewer to trace each Rust behavior to complete Go package evidence, with partial work labeled as seed evidence rather than claimed complete.

## Idempotence and Recovery

Cargo checks, tests, release builds, and parity checks are safe to rerun. Use distinct receipt paths so previous evidence remains available. Do not delete or reinitialize `/tmp/tidb-parity-tiup/data/codex-tpcc-plan-fixes-20260814`; the fixed data is part of the comparison contract. If the Rust server fails, resolve and stop only the process listening on port `45900`, retain its log, and restart it on port `45900` after rebuilding. Do not disturb the Go server on `45000`, PD on `43379`, TiUP data, or unrelated Rust servers.

Before rewriting temporary local commits, create a recoverable local backup reference. Never force-push `hparser-integration`. If the remote advances, rebase or merge locally, rebuild, and rerun at least the gates affected by conflict resolution.

## Artifacts and Notes

Feasibility receipts from known-good commit `3d1e96a030`:

    /tmp/tidb-local-parity-20260815-tpcc-old-current.json
    /tmp/tidb-local-parity-20260815-sysbench-stable.json
    /tmp/tidb-local-parity-20260815-bench-alternating/summary.json

Latest-remote failure receipt:

    /tmp/tidb-local-parity-20260815-tpcc-latest-remote-check.json

Latest reconciled TPCC run receipt:

    /tmp/tidb-local-parity-20260816-tpcc-run-final-v3.json  (63/63)
    /tmp/tidb-local-parity-20260816-tpcc-check-final-v3.json  (12/12)
    /tmp/tidb-local-parity-20260816-sysbench-final-current.json  (13,984/13,984)

Final post-regression receipts:

    /tmp/tidb-local-parity-20260816-tpcc-run-ready-v4.json       (63/63)
    /tmp/tidb-local-parity-20260816-tpcc-check-ready-v4.json     (12/12)
    /tmp/tidb-local-parity-20260816-sysbench-ready-v5.json       (13,984/13,984)
    /tmp/tidb-local-parity-20260816-bench-ready/summary.json      (Rust/Go median QPS 1.0694)

Regression baselines:

    /tmp/tidb-hparser-remote-latest-session-lib.log
    /tmp/tidb-replay-current-session-lib.log
    /tmp/tidb-original-dirty-session-lib.log

These files are not substitutes for receipts produced by the final rebased branch.

## Interfaces and Dependencies

`TableStatistics::estimate_column_ndv` in `tidb-executor/src/access_cost.rs` must scale a column histogram's NDV using an analyzed row count from a fully loaded, same-version statistic, matching Go `cardinality.EstimateColumnNDV` and its total-row-count helper.

`RowSource` in `tidb-executor/src/driver/join_reorder.rs` must expose leaf-local filters and whether every WHERE conjunct is either leaf-local or a join equality. `emit` must build `tidb_planner::cardinality::derive_stats::LogicalNode::DataSource` with per-column NDVs and index group NDVs, and `ProjectionExpr` with an optional bare-column `direct_input`.

`merge_join_decision` must receive the optional `RowSource`, and every `MergeDecision` must include required child offsets and names. Those properties drive the same ordered-child decisions as Go `PhysicalMergeJoin.tryToGetChildReqProp`.

Lookup aggregation metadata defined in `tidb-proto` and planned by `tidb-executor` must be consumed by the current lookup runtime. This is a cross-crate contract and must be validated end to end rather than inferred from compilation.

Revision note (2026-08-15): Initial plan created after selective import reduced the current branch to eight compile errors; recorded known-good and failing receipts plus final integration constraints. Updated after resolving all eight errors to record the clean compile and unused lookup-aggregation runtime contract. Updated after clean-baseline comparison rejected both the wholesale planner snapshot and the isolated correlated-aggregate experiment, and moved active reconciliation to latest remote `644927fc21`. Updated after Go-grounded predicate-placement reconciliation reduced the full session gate to 34 failures and exposed two stale Rust column-pruning expectations. Updated after restoring the complete IndexMerge chain and validating the focused index-hint package gate against a fresh Go nightly capture. Updated on 2026-08-16 after auditing the incomplete historical ordered-limit snapshot and selecting the complete Go planner/executor contract as the implementation boundary. Updated again after final TPCC and Sysbench parity receipts narrowed completion work to 11 package regressions and corrected the active Rust port to `45900`.
