# Stabilize preview RU around resource-shaped operator formulas

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan must be maintained according to it.

## Purpose / Big Picture

The current preview RU demo uses an operator-specific matrix of fixed, row, byte, and ordering weights. That matrix is useful for collecting evidence, but it does not express the intended model directly: expression evaluation should consume one shared CPU weight, scans should consume one scan-byte weight, transport should consume network-byte and request weights, and stateful operators should add small hash-table or join-output terms.

After this plan is implemented, `EXPLAIN ANALYZE FORMAT='RU'`, the bounded metrics, the general-log detail, and the statement-summary detail tables will expose the same coefficient-free resource work units. A reader can reproduce every operator's preview RU with seven named weights and can see exactly which current runtime detail supplied each input. The preview remains isolated from production RUv2 charging and resource-control reporting.

This plan first froze the model, field mappings, failure behavior, migration, and validation, and now records the implementation and verification evidence from the implementation worktree.

## Progress

- [x] (2026-07-22 09:00Z) Inspected the current preview RU implementation and its closest tests in `pkg/planner/core/explain_ru.go`, `pkg/planner/core/common_plans_test.go`, and `pkg/executor/explain_test.go`.
- [x] (2026-07-22 09:20Z) Verified current row, byte, scan-detail, cop-task, RUv2 response-byte/RPC, aggregation-output, inline Projection, and reader-child evidence in code.
- [x] (2026-07-22 09:35Z) Resolved the formula, expression-count, reader-attribution, write-unit, failure, and version-migration decisions in this document.
- [x] (2026-07-22 10:05Z) Incorporated the explicit de-duplication constraints for Sort/TopN inline Projection CPU and IndexJoin inner-reader requests.
- [x] (2026-07-22 11:15Z) Incorporated the final ordering clarification: Sort owns `n*log(n)`, TopN owns `n*log(k)` with `k` derived from offset plus count, and expression evaluation remains Projection-owned.
- [x] (2026-07-22 11:40Z) A fresh-context release reviewer re-read the updated user handoff, plan, and code and concluded: “当前版本无需必要修改”.
- [x] (2026-07-22 11:45Z) Applied the Ready profile to this design-only diff: no Go/Bazel trigger was present and the staged Markdown patch passed `git diff --cached --check`.
- [x] (2026-07-22 13:05Z) A later fresh-context completion audit found that explicit-transaction DML write RPCs are statement-local and cannot all be deferred to COMMIT; revised ownership to charge each DML and COMMIT from its own frozen `RUV2Metrics` snapshot.
- [x] (2026-07-22 13:35Z) A new no-inherited-context reviewer inspected the corrected plan and current code and concluded exactly: “当前版本无需必要修改”. Because this iteration changed a substantive ownership rule, the convergence policy still requires another fresh-context iteration before design-loop completion.
- [x] (2026-07-22 14:20Z) A further fresh-context reviewer independently audited the original formula contract, current exec-detail sources, operator ownership, degradation rules, migration, and corrected statement-local write-request lifecycle and concluded exactly: “当前版本无需必要修改”. No substantive design change was made, so the required convergence review is complete.
- [x] (2026-07-22 19:10Z) Implemented Milestone 1: introduced the private six-weight v4 container, semantic work units, centralized expression-slot counts, checked ordering arithmetic, and exact formula tests without changing production RUv2.
- [x] (2026-07-22 19:35Z) Implemented Milestone 2: constructed root/cop/read-transport/write units from existing frozen details and added only the narrow HashJoin `HashTableRows()` runtime interface with V1/V2 state accounting.
- [x] (2026-07-22 20:05Z) Implemented Milestone 3: migrated EXPLAIN, metrics, statement summary, General Log, version labels, legacy-total behavior, documentation, and focused regression tests atomically.
- [x] (2026-07-22 20:55Z) Completed the implementation Ready profile: `make bazel_prepare`, all six prescribed failpoint-wrapped package test sets, the tagged metrics test, `make lint`, and `git diff --check` passed; the HashJoin set additionally ran `TestHashRowContainer` for V1 NAAJ state coverage.
- [x] (2026-07-22 22:10Z) A completion audit found and fixed a v4 migration regression: the write constructor had stopped consulting the existing statement mutation snapshot's `Pipelined` flag. Pipelined DML and COMMIT now retain mutation evidence but fail the write-request component closed with `pipelined_tikv_payload_unsupported`; focused DML and COMMIT regressions cover the restored gate.
- [x] (2026-07-22 04:25+08:00) The final completion audit found that the ordering gate rejected residual scalar functions but did not prove that column `ByItems` belonged to the direct child schema. Sort/TopN now validate the executed flat child, including Projection expression/schema alignment, and a regression was observed failing before the fix and passing afterward.
- [x] (2026-07-22 04:30+08:00) Recompleted the Ready gate after the ordering fix: the full planner/core and executor target sets passed with failpoint cleanup, the earlier same-state join/session/statement-summary/metrics runs remained applicable, `make lint` passed after the code edit, and `git diff --check` plus API/range review found no remaining mismatch.
- [x] (2026-07-22 05:35+08:00) A requirement-by-requirement completion audit found that the private weight container omitted its specified immutable version, so a test-only calibrated set could publish a total under the production uncalibrated label. Added version ownership and validation to the weight container, propagated its active version to every output, and observed the focused regression fail before the fix and pass afterward.
- [x] (2026-07-22 05:45+08:00) Recompleted the full Ready gate from the current worktree: `make bazel_prepare` produced no generated diff; all six failpoint-wrapped target groups, the HashJoin NAAJ coverage, and tagged metrics tests passed; `make lint`, `git diff --check`, API-boundary searches, and the baseline-to-HEAD scope review passed. Failpoint refcount returned to zero after every run.
- [x] (2026-07-22 07:15+08:00) A subsequent output-version audit found that background Prometheus statement, operator-status, base-unit, and row-width series carried only `model_version`. Added the bounded active `weight_version` label to all four families and froze one active version per result recording; the new metric-contract regression failed before the change and passed afterward.
- [x] (2026-07-22 07:20+08:00) Recompleted the Ready gate after the Prometheus provenance fix: `make bazel_prepare` produced no generated diff; all six failpoint-wrapped target groups, HashJoin NAAJ coverage, and tagged metrics tests passed with failpoint refcount returning to zero; `make lint`, `git diff --check`, call-site search, version-label review, and the baseline scope review passed.
- [x] (2026-07-22 15:42+08:00) Applied the final write-unit contract: normalized mutation work now reuses `cpu_work`, is emitted only under a valid calibrated weight snapshot, and remains distinguishable through `site=tidb`, `op_class=kv_mutation`, `operator_kind=memdb_mutation`, `input_source=stmt_memdb_mutation_calls`, and `input_side=all`. The six raw mutation diagnostics remain visible when uncalibrated.
- [x] (2026-07-22 15:53+08:00) Completed the revised write-unit Ready gate: the new regression failed against the old independent-unit behavior and passed after restoration; all six failpoint-wrapped target groups, tagged metrics tests, `make lint`, residual-name search, and `git diff --check` passed with failpoint refcount returning to zero.
- [x] (2026-07-22 16:09+08:00) Split the shared request contract into `read_request_count`/`read_request_weight` and `write_request_count`/`write_request_weight`, preserving the existing statement-scope ownership and fail-closed rules. Updated EXPLAIN, General Log, Prometheus, statement-summary, formula, DML, and COMMIT regressions plus the affected test case maps.
- [x] (2026-07-22 16:14+08:00) Completed the request-split Ready gate: both reader and writer regressions failed when their constructors were temporarily restored to the old shared `request_count` output, then passed with the split implementation. All six failpoint-wrapped target groups, tagged metrics tests, `make lint`, residual-name search, and `git diff --check` passed with failpoint refcount returning to zero. The Bazel gate found no trigger, so `make bazel_prepare` was not required.
- [x] (2026-07-22 18:45+08:00) Corrected TopN ordering work to saturate its legal `offset+count` heap bound at actual input rows and to emit zero work when `count=0`. The focused regression failed against the unsaturated formula and passed after the fix; overflow remains fail-closed for every positive count.
- [x] (2026-07-22 18:47+08:00) Completed the TopN correction Ready gate: the full preview-RU planner target, EXPLAIN/metrics/general-log executor targets, `make lint`, and `git diff --check` passed with failpoint refcount returning to zero. The Bazel gate found no trigger, so `make bazel_prepare` was not required.
- [x] (2026-07-23 23:35+08:00) Reproduced the real-TiKV non-Scan publication failure in code: distsql attaches response ScanDetail to the component's last plan ID while the scan leaf receives only an execution summary, and the v4 estimator counted both the real holder and the leaf's default-zero detail as holders. Three rounds of ownership, fail-closed, and validation review converged on explicit TiDB-side ScanDetail attachment provenance without changing the distsql ownership or TiKV protocol.
- [x] (2026-07-24 00:05+08:00) Added a regression that exactly uses `RecordOneCopTask` for the scan leaf and `RecordCopStats` for the parent holder. It failed before the fix with `ambiguous_cop_scan_width` and passed after `RuntimeStatsColl` began recording ScanDetail attachments and the estimator validated holder/task coverage from one consistent snapshot.
- [x] (2026-07-24 00:20+08:00) Completed the provenance regression matrix: explicit empty detail, repeated same-holder attachments, attachment-without-holder-summary, independent scan-summary and attachment coverage failures, no holder, partial tuple, multi-holder ambiguity, ExecutorId remap, and statement-atomic failure are all covered. Independent implementation and test reviewers both concluded LGTM with no P0/P1/P2.
- [x] (2026-07-24 00:33+08:00) Completed cleanup-safe real-TiKV validation against PD/TiKV `v9.0.0-beta.2.pre-363-g8cabcf1` and the current-worktree TiDB binary. Selection, Projection, Limit, TopN, HashAgg, and ordered StreamAgg all succeeded in `FORMAT='RU'`; the Selection probe emitted `tikv/filter_eval cpu_work=4` and `tikv/kv_range_scan scan_bytes=212.5` identically through General Log, Prometheus, and statement summary. The exact TiUP tag was cleaned and the PD endpoint was unreachable afterward.
- [x] (2026-07-24 00:36+08:00) Completed the final Ready gate after the stable ordered-StreamAgg fixture revision: targeted execdetails, distsql, planner/core, and executor tests passed; `make lint` and `git diff --check` passed; failpoint refcount was zero. The Bazel gate found no added/moved Go file, import change, new top-level test, Bazel edit, or module edit, so `make bazel_prepare` was not required.
- [x] (2026-07-24 01:20+08:00) Added statement-scoped RPC-only preview units for closed-set read-only PointGet and BatchPointGet plans. The implementation emits exactly one synthetic point-lookup operator, retains physical lookup rows as non-billable diagnostics, and keeps locking, DML, mixed cop-reader, and unknown producer shapes fail-closed.
- [x] (2026-07-24 01:35+08:00) Observed the focused EXPLAIN regression fail against the prior `ambiguous_reader_transport_producers` behavior and pass after the implementation. Planner, EXPLAIN, plan-digest, General Log, Prometheus, and statement-summary regressions passed; `make bazel_prepare`, `make lint`, and `git diff --check` passed. A cleanup-safe real-TiKV run confirmed PointGet and BatchPointGet each publish only one statement-scoped `read_request_count=1` unit and no uncalibrated total.
- [x] (2026-07-24 02:05+08:00) Bound UnionScan's intentionally simple CPU formula to its direct child's actual runtime rows: `cpu_work=input_rows`. The focused regression failed while `overlay_reader` was non-billable and passed after the formula was enabled, with no expression-count multiplier or new runtime datum.
- [x] (2026-07-24 02:25+08:00) Completed UnionScan output and Ready verification. A cleanup-safe real-TiKV transaction produced UnionScan output rows 4 over direct-child rows 3 and published exactly `tidb/overlay_reader/unionscan cpu_work=3` from `runtime_child_act_rows`; the focused preview-RU planner suite, `make lint`, and `git diff --check` passed with failpoint refcount zero. The Bazel gate found no import, file, top-level-test, Bazel, or module trigger, so `make bazel_prepare` was not required.

## Surprises & Discoveries

- Observation: root executor byte accounting is logical live chunk bytes, not encoded network bytes.
  Evidence: `pkg/util/execdetails/runtime_stats.go` defines `BasicRuntimeStats.inputBytes/outputBytes`, and `pkg/executor/internal/exec/executor.go` records child/output chunk bytes around `Next`.

- Observation: exact TiKV response bytes and read/write RPC counters already exist, but only at statement scope.
  Evidence: `pkg/util/execdetails/ruv2_metrics.go` exposes `TiKVCoprocessorResponseBytes`, `ResourceManagerReadCnt`, and `ResourceManagerWriteCnt`; none is keyed by reader plan ID.

- Observation: `selectResultRuntimeStats` already counts cop responses internally, but proportional or per-reader network-byte attribution is not available from current exec details.
  Evidence: `pkg/distsql/select_result.go` keeps `copRespTime` and `reqStat` inside the unexported `selectResultRuntimeStats`, while response bytes are drained into statement-level `RUV2Metrics`.

- Observation: HashAgg runtime details do not expose a separate group-count counter, but the physical Agg node's own actual output rows are the number of materialized group states for this simple model.
  Evidence: `RuntimeStatsColl` provides own-plan `GetActRows`; the current preview implementation already publishes Agg output-row shadows from this value and verifies TiKV expected/observed task coverage.

- Observation: HashJoin state is row-backed rather than one-entry-per-distinct-key, and the exact admitted row count exists below the exec-details boundary.
  Evidence: `pkg/executor/join/hash_table_v1.go::Len` counts ordinary inserted row pointers, while NAAJ null-key rows are separately retained in `hashNANullBucket.entries`; `pkg/executor/join/join_row_table.go::validKeyCount` counts v2 rows eligible for the hash lookup structure. Current HashJoin runtime stats expose timing/collision data but not the total admitted lookup-state row count.

- Observation: IndexJoin's inner lookup work is executed by dynamically built reader children, whose physical network requests already enter statement-level read RPC details.
  Evidence: `pkg/executor/join/index_lookup_join.go::fetchInnerResults` drains `task.innerExec`, while the reader paths use DistSQL and feed `RUV2Metrics.ResourceManagerReadCnt`; adding the private inner task count as another request term would charge the same lookup twice.

- Observation: inline Projection materializes scalar Sort/TopN `ByItems`, while ordinary column keys need no scalar evaluation; injection is not universal for pushed cop plans.
  Evidence: `pkg/planner/core/rule_inject_extra_projection.go::InjectProjBelowSort` injects only when a `ByItems` expression is a `ScalarFunction`, and root post-optimization does not rewrite every pushed cop plan. V4 therefore assigns expression evaluation exclusively to an actual Projection, assigns one aggregate sorting-complexity term to Sort/TopN regardless of key count, and fails closed if a scalar ordering expression remains unmaterialized.

- Observation: completed IndexHashJoin plans do not retain the ordinary IndexJoin equality representation.
  Evidence: `pkg/planner/core/exhaust_physical_plans.go::completePhysicalIndexJoin` clears `EqualConditions` after deriving `OuterHashKeys` and `InnerHashKeys`; IndexMergeJoin instead exposes executable `CompareFuncs` and optional `OuterCompareFuncs`. Counting only the embedded IndexJoin fields would undercount these subtypes.

- Observation: zero-valued RUv2 getters do not prove that their payload was present, and the read-RPC counter is broader than cop response bytes.
  Evidence: absent/bypassed `RUV2Metrics` reads as zero; `ResourceManagerReadCnt` covers TiKV read RPC producers including unsupported point/ancillary paths, while `TiKVCoprocessorResponseBytes` covers cop responses. Runtime task coverage and a closed producer set are therefore required before zero or statement-wide totals are attributable.

- Observation: write RPC counters are statement-local even inside an explicit transaction.
  Evidence: `session.executeStmtImpl` installs the current statement's `RUV2Metrics`, `ExecStmt.finalizeStatementRUV2Metrics` drains raw/commit details into it before preview construction, and `TestRUV2MetricsIsolatedPerStatementInExplicitTxn` proves successive statements use distinct instances. A pessimistic DML's write requests therefore cannot be deferred to the later COMMIT snapshot.

- Observation: MockStore does not synthesize resource-manager write RPC detail for pessimistic KV traffic.
  Evidence: the new session lifecycle test observes zero from a real MockStore DML before injection. It therefore injects finalized values into the actual per-statement `RUV2Metrics` objects to prove DML/COMMIT snapshot separation, while private core construction tests independently prove that values 7 and 2 become distinct `write_request_count` units.

- Observation: pipelined completeness is already preserved in the statement-local mutation snapshot and on the COMMIT statement before transaction invalidation.
  Evidence: `stmtctx.PreviewKVMutationSnapshot.Pipelined`, `LazyTxn.previewKVMutationRecorder`, and `session.markPreviewKVMutationTxnPipelined` provide the lifecycle evidence. The v4 write constructor must consume that flag before publishing `ResourceManagerWriteCnt`; a present counter alone does not prove complete pipelined logical-flush attribution.

- Observation: a non-scalar Sort/TopN `ByItems` expression alone does not prove that inline expression evaluation was materialized by the executed child.
  Evidence: `InjectProjBelowSort` rewrites scalar ordering expressions to newly allocated Projection columns, but the preview constructor previously accepted any column without checking the direct child's schema. Column membership and Projection expression/schema alignment are therefore required presence evidence before publishing ordering work.

- Observation: a constant output-side weight version does not prove that the coefficient set used to calculate a total has that version.
  Evidence: the initial v4 implementation kept `readBillingDemoWeightVersion` outside `readBillingDemoWeights`; private calibrated formula fixtures could therefore make `total_preview_ru` available while EXPLAIN, statement summary, and metrics still labeled the result `v3-resource-formula-uncalibrated`.

- Observation: `model_version` alone is insufficient provenance for background Prometheus units whose semantics can change with the weight container.
  Evidence: mutation-derived `cpu_work` depends on the versioned `MutationBytesPerCPUUnit`, while the four `tidb_read_billing_demo_*` families previously omitted `weight_version`; a rolling transition could therefore merge distinct v4 contracts into the same series.

- Observation: distsql response ScanDetail ownership differs from executor-summary ownership.
  Evidence: `pkg/distsql/select_result.go::updateCopRuntimeStats` records each valid execution summary under its exact `copPlanIDs` entry but attaches the response-level ScanDetail only through `RecordCopStats` on the last plan ID. `pkg/distsql/select_result_test.go::TestUpdateCopRuntimeStats` proves that the scan leaf has tasks and zero detail while its parent holds the merged detail.

- Observation: a zero-valued `CopRuntimeStats.GetScanDetail()` does not prove an observed empty scan.
  Evidence: both a summary-only scan leaf and an actually attached all-zero ScanDetail read back as the zero struct. The previous `tasks>0 && detail==zero` sentinel therefore double-counted the leaf beside a real parent holder and could also turn a missing or partial detail into a false observed zero.

- Observation: native EmbedUnistore is not a complete scan-width oracle for this regression.
  Evidence: its cop response supplies processed versions but not the complete total/processed-size tuple required by `readBillingDemoRangeScanInput`. A test-only response enrichment may exercise output wiring, but the corrected Ready evidence must include real TiKV.

- Observation: a bare `stream_agg()` hint did not produce StreamAgg consistently across native EmbedUnistore and real TiKV.
  Evidence: the stable validation shape also orders the result and uses `order_index(table, idx_b)`, making the ordered index property explicit; ordinary EXPLAIN and `FORMAT='RU'` then both show `StreamAgg cop[tikv]`.

- Observation: TiKV client-go already classifies every successfully completed TiKV `CmdGet` and `CmdBatchGet` RPC as one raw read RPC, and TiDB drains that counter into the statement's frozen `ResourceManagerReadCnt`.
  Evidence: `client-go/internal/client/completedTiKVRUV2RPCCount` returns `(1, 0)` for non-write TiKV requests and its regression names both Get and BatchGet; `config.UpdateTiKVRUV2FromExecDetailsV2` adds that count to `kvrpcpb.RUV2.ReadRpcCount`; `ExecStmt.finalizeStatementRUV2Metrics` drains it through `SyncRUV2MetricsFromRUDetails`.

- Observation: PointGet and BatchPointGet have no attributable response-byte counter in the frozen preview details, while their statement read-RPC counter may cover more than one plan node.
  Evidence: `RUV2Metrics.TiKVCoprocessorResponseBytes` covers coprocessor responses rather than Get/BatchGet payloads, and `ResourceManagerReadCnt` is statement-scoped rather than keyed by physical plan ID. Per-node publication would therefore invent byte work and duplicate RPC work for multi-lookup statements.

- Observation: UnionScan merges snapshot rows from its direct child with transaction mem-buffer rows, but the frozen preview details expose only the direct child's exact actual-row count.
  Evidence: `UnionScanExec.getOneRow` merges `getSnapshotRow` and `getAddedRow`; `RuntimeStatsColl` records the direct child and UnionScan output rows but no separate count of mem-buffer rows considered by the merge. Output rows cannot reconstruct input work because deletes, replacements, and predicates can suppress rows.

## Decision Log

- Decision: replace, rather than layer on top of, the v3 fixed/row/byte opclass matrix for new samples.
  Rationale: keeping both billable models would double count and make `cpu_weight` cease to mean one expression-slot evaluation. Historical v3 samples remain queryable under their model version.
  Date/Author: 2026-07-22 / Codex.

- Decision: use seven semantic RU weights: `cpu_weight`, `scan_weight`, `net_weight`, `read_request_weight`, `write_request_weight`, `hash_table_weight`, and `join_weight`.
  Rationale: read and write requests have independent calibration coefficients and externally distinct units. Operator identity remains a diagnostic dimension, not a weight key.
  Date/Author: 2026-07-22 / Codex.

- Decision: Sort CPU work is `rows * log2(max(rows, 2))`; for positive `count`, TopN CPU work is `rows * log2(max(min(rows, k), 2))`, where `k = offset + count`; `count=0` produces zero TopN work.
  Rationale: this is the final requested ownership split. Inline Projection alone charges scalar expression evaluation; the ordering node charges aggregate algorithmic work, with no expression/key-count multiplier. TopN retains at most the rows that actually exist, so a configured heap bound larger than input cardinality cannot increase work. Offset remains part of the legal heap bound. The implementation rejects `offset + count` overflow for positive counts instead of wrapping; zero/one rows use base two so positive-count work is finite and deterministic, while `count=0` matches the executor's no-child-read fast path.
  Date/Author: 2026-07-22 / Codex.

- Decision: count top-level executable expression slots, not recursive AST nodes.
  Rationale: plan fields provide a stable, cheap, deterministic count. Recursive counts would make rewrites of an equivalent scalar expression change billing without runtime evidence and would blur the meaning of one calibrated slot.
  Date/Author: 2026-07-22 / Codex.

- Decision: aggregate reader transport once per statement under `reader_transport`, while retaining TableReader, IndexReader, IndexLookup, and IndexMerge kinds as bounded diagnostics.
  Rationale: the existing byte and RPC details are statement-scoped and all listed reader kinds share the same weights. Algebraically, charging the totals once equals summing per-reader formulas. Proportional allocation by logical output bytes is rejected because it invents attribution and is badly biased for IndexLookup/IndexMerge internal legs.
  Date/Author: 2026-07-22 / Codex.

- Decision: publish PointGet and BatchPointGet read RPCs once per statement under `id=point_lookup@statement`, `site=tikv`, and `op_class=kv_point_lookup`, with only `read_request_count`.
  Rationale: Get/BatchGet RPC counts are exact but statement-scoped, so one synthetic operator preserves the requested lookup dimensions without multiplying the total across multiple plan nodes. Pure PointGet and pure BatchPointGet use their respective operator kind; a closed statement containing both uses `mixed_point_lookup`. No CPU, scan, or network work is inferred. Locking lookups, DML, and statements mixing point lookup with cop readers remain fail-closed because the current counter cannot split ancillary or heterogeneous read RPCs.
  Date/Author: 2026-07-24 / Codex.

- Decision: make root UnionScan weight-bearing with exactly `cpu_work = direct_child_actual_rows`.
  Rationale: this is the requested simple formula and reuses the same stable `runtime_child_act_rows` source as other root unary operators. UnionScan emits no `expression_count` and does not infer mem-buffer input rows from output rows. A future model that charges the second input would require an explicit executor datum and a separate design revision.
  Date/Author: 2026-07-24 / Codex.

- Decision: interpret the requested HashJoin `distinct_rows` term as runtime `hash_state_rows`, the cumulative rows actually admitted into hash lookup state; interpret the HashAgg term as `group_rows = own output rows`.
  Rationale: duplicate join keys still allocate/probe row-backed entries, while null/filter-rejected build rows do not enter that structure. Exact admitted rows are therefore more faithful than all build-child rows, and counting unique keys would require a second hash set solely for billing. This is the one permitted Join-only exec-details addition.
  Date/Author: 2026-07-22 / Codex.

- Decision: IndexJoin has no additional request term at the Join node and requires no request-related runtime datum.
  Rationale: its extra lookups execute through inner reader children and are already included in the statement reader-transport request counter. Charging inner lookup tasks again would duplicate request cost. The optional Join exec-detail permission is therefore not exercised for IndexJoin/request accounting; v4 uses it only for HashJoin `hash_state_rows`.
  Date/Author: 2026-07-22 / Codex.

- Decision: keep calibrated-weight injection private to `pkg/planner/core` formula tests; package-external end-to-end tests exercise the production-default uncalibrated contract.
  Rationale: `pkg/executor/explain_test.go` is `package executor_test`, while the preview weight container intentionally remains private and has no session/global knob. A public or failpoint configuration surface solely for tests would weaken the initial contract.
  Date/Author: 2026-07-22 / Codex.

- Decision: freeze the three legacy statement-summary convenience totals as v3-only and leave them zero for v4 samples.
  Rationale: `fixed_events`, `input_rows`, and `input_bytes` cannot safely represent the new semantic units. V4 consumers must use the versioned detail table; adding a parallel convenience schema and its v1/v2 persistence migration is outside this minimal model change.
  Date/Author: 2026-07-22 / Codex.

- Decision: ship `model_version='v4'` with `weight_version='v3-resource-formula-uncalibrated'` and delete the executable v3 weight map.
  Rationale: historical rows already contain values and versions, while private per-statement results do not survive process upgrade. Keeping a second calculation path would create an unnecessary mixed-model state; an eventual calibrated set must receive another immutable version.
  Date/Author: 2026-07-22 / Codex.

- Decision: combine write mutation count and bytes into `cpu_work = mutation_count + mutation_bytes / mutation_bytes_per_cpu_unit`, then apply `cpu_weight` once. Distinguish mutation CPU from expression CPU through the operator dimensions rather than a separate unit name.
  Rationale: normalization produces expression-equivalent CPU work, so reusing the shared semantic unit preserves one CPU coefficient. `mutation_bytes_per_cpu_unit` is a versioned normalization constant, not an RU coefficient; the complete weight snapshot must be calibrated and valid before mutation-derived `cpu_work` is emitted. Raw mutation count/byte diagnostics remain available without calibration.
  Date/Author: 2026-07-22 / Codex.

- Decision: charge write requests to the statement whose frozen `RUV2Metrics` snapshot contains them; an explicit-transaction DML and the eventual COMMIT each own only their respective snapshots.
  Rationale: TiDB installs a fresh `RUV2Metrics` object for every statement and finalizes raw RU details into that statement before preview construction. Pessimistic DML can issue write RPCs before COMMIT, so deferring all request work to COMMIT would permanently omit those DML-local requests. A cross-statement accumulator is unnecessary and would weaken the existing statement-local contract.
  Date/Author: 2026-07-22 / Codex.

- Decision: expose read and write requests as `read_request_count` and `write_request_count`, with independent `read_request_weight` and `write_request_weight` coefficients.
  Rationale: the counters already have distinct authoritative sources and workload behavior, and independent unit names prevent downstream consumers from collapsing them before calibration. This changes only preview-v4 semantics; statement-scope ownership, fail-closed gates, and production RUv2 remain unchanged.
  Date/Author: 2026-07-22 / Codex.

- Decision: do not guess numerical v4 weights or silently map the heterogeneous v3 opclass weights.
  Rationale: there is no evidence-backed one-to-one mapping. The implementation first publishes v4 base units with `uncalibrated_weights` and no total until an explicit v4 weight set and positive mutation normalization are installed. The preview flag is off by default, so this is safer than presenting arbitrary numbers as RU.
  Date/Author: 2026-07-22 / Codex.

- Decision: validate Sort/TopN ordering columns against the direct executed flat child schema, and additionally require a Projection child's schema width to match its expression list.
  Rationale: this proves the ownership handoff to the child Projection without charging expression CPU at the ordering node, while preserving ordinary column and constant ordering. Relying only on optimizer invariants would violate the explicit fail-closed contract for missing or misaligned Projection schema.
  Date/Author: 2026-07-22 / Codex.

- Decision: make the private weight container own its immutable version and reject calibration under the shipped uncalibrated label.
  Rationale: coefficients and their version are one atomic contract. Deriving all output labels from the active container prevents a future calibrated set from publishing a total under stale metadata, while the production default remains explicitly `v3-resource-formula-uncalibrated` and cannot publish totals.
  Date/Author: 2026-07-22 / Codex.

- Decision: add the active weight version to every background preview Prometheus family, not only to base-unit samples.
  Rationale: keeping statement status, operator status, units, and row-width observations on the same bounded version key makes the Prometheus output an atomic contract and prevents partial cross-version aggregation. The version is immutable per binary configuration, so the extra label remains bounded.
  Date/Author: 2026-07-22 / Codex.

- Decision: record TiDB-internal ScanDetail attachment counts and use them, rather than detail values, to identify the unique component holder.
  Rationale: `RecordCopStats(scan != nil)` already knows whether a response attached ScanDetail, including a legitimate all-zero detail, while `RecordOneCopTask` is summary-only. Counting attachments under the original plan ID preserves the existing distsql/EXPLAIN ownership contract, distinguishes empty from absent evidence, and makes multiple holders fail closed without relying on flat-tree traversal order. This is internal provenance metadata, not a new TiKV protocol field or formula input.
  Date/Author: 2026-07-23 / Codex.

- Decision: validate scan executor summaries and ScanDetail attachments as independent coverage channels.
  Rationale: the scan leaf must have `observed summaries == expected responses > 0`, while the unique holder must have `attachment records == holder expected == scan expected`. The holder's own execution summary is not required for scan bytes because `RecordCopStats` can receive complete ScanDetail with a nil summary. Only after both channels are complete may an all-zero tuple mean an observed empty scan.
  Date/Author: 2026-07-23 / Codex.

- Decision: keep the broader non-Scan direct-child expected-task hardening outside this focused ownership correction.
  Rationale: the existing relative `maxSummaryTasks` gate can miss the case where every executor omits the same response summary, but changing every unary/Agg/DML path is not required to fix the confirmed double-holder failure and needs its own calibrated-release regression matrix.
  Date/Author: 2026-07-23 / Codex.

## Outcomes & Retrospective

The design is stable after the required convergence review. It keeps the requested simple formula family, identifies every current data source, assigns Sort `n*log(n)` and TopN `n*log(min(n, offset+count))` work without repeating Projection expression evaluation, treats zero-count TopN as zero work, validates ordering-column ownership against the executed child schema, removes IndexJoin request double charging, and defines a no-guess migration. The implementation now publishes exactly seven weight-bearing v4 semantic units, reuses dimension-qualified `cpu_work` for normalized mutation preparation, retains the raw mutation diagnostics, and keeps production weights intentionally uncalibrated. It atomically binds any future calibrated coefficients to their immutable output version across EXPLAIN, statement summary, general log, and all background Prometheus families, removes the executable v3 formula from production, gives read and write requests independent unit names and weights while preserving statement-scoped ownership, and preserves the pipelined write-request fail-closed gate. Runtime additions remain narrow: HashJoin exposes actual admitted state rows as a formula datum, while cop stats record only TiDB-side ScanDetail attachment provenance needed to distinguish an observed empty scan from absent detail. MockStore cannot produce real resource-manager write RPC details, so the session lifecycle test injects finalized observations into the actual fresh per-statement snapshots; private construction tests separately verify their exact 7/2 unit values. The ownership correction now passes the complete focused regression matrix and real-TiKV output verification: six non-Scan cop operator classes publish their units, and one Selection sample agrees across EXPLAIN, General Log, Prometheus, and statement summary without publishing an uncalibrated total. The subsequent point-lookup extension consumes the already-frozen TiKV read-RPC counter without introducing a new runtime datum: pure read-only PointGet and BatchPointGet statements now publish one exact RPC-only synthetic operator, while every shape whose statement counter cannot be attributed uniquely remains fail-closed. UnionScan likewise reuses existing evidence and the existing CPU weight: it now publishes direct-child actual rows as `cpu_work` without charging its conditions or inventing a mem-buffer input estimate.

## Context and Orientation

Preview RU is an observational model. It emits coefficient-free work units and optionally multiplies them by preview-only weights. It must not modify the RU charged by TiKV, the RUv2 total reported to resource control, or scheduling decisions.

The current constructor and renderer live in `pkg/planner/core/explain_ru.go`. `buildReadBillingDemoResult` freezes one statement result, `readBillingDemoRootUnits` derives TiDB root units, `readBillingDemoCopUnits` derives TiKV cop units, and `readBillingDemoUnitPreviewRU` applies weights. The same result feeds `EXPLAIN ANALYZE FORMAT='RU'`, metrics, statement summary, and logging. The later implementation must keep one frozen result as the sole source for all outputs.

`RuntimeStatsColl` in `pkg/util/execdetails/runtime_stats.go` maps physical plan IDs to root and cop runtime details. Root `BasicRuntimeStats` gives actual output rows and logical chunk bytes. Cop `CopRuntimeStats` gives executor-produced rows, task count, and `ScanDetail`. `RUV2Metrics` in `pkg/util/execdetails/ruv2_metrics.go` gives statement-level TiKV response bytes and read/write RPC counts. Physical plan structs in `pkg/planner/core/operator/physicalop` give expression lists; these are static plan metadata, not new runtime observations.

In this document, a work unit is a coefficient-free non-negative finite number. A weight converts one work unit into preview RU. A physical operator is one node in the executed physical plan. An expression slot is one top-level expression/function/key comparison that the physical operator evaluates per input row. `cpu_weight` prices one expression-equivalent CPU-work unit; expression evaluation, ordering comparisons, Limit row handling, and normalized mutation preparation can all produce such units. A missing value differs from an observed zero; missing required evidence fails closed, while observed zero remains billable as zero.

## Stable formula contract

For one statement, weighted preview RU is the sum of the following unit families:

    preview_ru =
        cpu_work        * cpu_weight
      + scan_bytes      * scan_weight
      + net_bytes       * net_weight
      + read_request_count  * read_request_weight
      + write_request_count * write_request_weight
      + hash_state_rows * hash_table_weight
      + join_output_rows * join_weight

All arithmetic is float64 with explicit negative, NaN, infinity, and overflow rejection. Integer counters are converted only after validating that they are non-negative. Each pre-aggregation operator result retains `site`, `op_class`, `operator_kind`, `operator_id`, `source`, and, for joins, `input_side`. Physical results use the executed plan's Explain ID. Statement-scoped synthetic results use reserved, non-plan IDs: `reader_transport@statement`, `point_lookup@statement`, `mutation@statement`, and `txn_write@statement`. Statement-summary detail intentionally aggregates away `operator_id`; its remaining bounded dimensions and version still preserve formula provenance.

There is no billable fixed-event term in v4. Setup costs that correlate with remote fanout use the direction-specific request unit; other constant setup costs stay outside this intentionally simple model.

### Operator formulas

| Operator | v4 formula | Required inputs |
|---|---|---|
| Selection | `rows * n_expr * cpu_weight` | direct child actual rows; selection expression slots |
| Projection | `rows * n_expr * cpu_weight` | direct child actual rows; projected expression slots |
| Sort | `rows * log2(max(rows,2)) * cpu_weight` | direct child actual rows; scalar expression evaluation belongs to inline Projection |
| TopN | `count == 0 ? 0 : rows * log2(max(min(rows,k),2)) * cpu_weight` | direct child actual rows; checked `k=offset+count` for positive count; scalar expression evaluation belongs to inline Projection |
| TableScan / IndexScan | `scan_bytes * scan_weight` | attributable TiKV `ScanDetail` |
| TableReader / IndexReader / IndexLookup / IndexMerge transport | `net_bytes * net_weight + read_request_count * read_request_weight` | statement `RUV2Metrics`, emitted once |
| PointGet / BatchPointGet | `read_request_count * read_request_weight` | statement `RUV2Metrics`, emitted once for a closed point-lookup producer set |
| UnionScan | `rows * cpu_weight` | direct child actual rows; no expression-count multiplier |
| StreamAgg | `rows * n_expr * cpu_weight` | direct child actual rows; group and aggregate slots |
| HashAgg | `rows * n_expr * cpu_weight + group_rows * hash_table_weight` | StreamAgg inputs plus own actual output rows |
| MergeJoin | `(left_rows + right_rows) * n_expr * cpu_weight + output_rows * join_weight` | both child rows, join slots, own output rows |
| HashJoin | `(left_rows + right_rows) * n_expr * cpu_weight + hash_state_rows * hash_table_weight + output_rows * join_weight` | both child rows, join slots, one Join runtime state count, own output rows |
| IndexJoin family | `(left_rows + right_rows) * n_expr * cpu_weight + output_rows * join_weight` | both child rows, join slots, own output rows; inner reader already owns requests |
| Limit | `rows * cpu_weight` | direct child actual rows |
| Window | `rows * n_expr * cpu_weight` | direct child rows and the refined Window slot count below |
| Write mutation and commit | `cpu_work * cpu_weight + write_request_count * write_request_weight` | existing mutation recorder and statement/commit RUv2 details; mutation provenance is carried by operator dimensions |

The table is normative. Diagnostic `input_rows`, logical chunk bytes, output bytes, mutation component counters, and operator status may still be emitted, but they have no weight and never enter `preview_ru`.

### Expression-slot count

`n_expr` is a non-negative integer derived from the executed physical plan. It is stored as a diagnostic unit so offline recomputation does not need the original plan object.

Selection uses `len(PhysicalSelection.Conditions)`. Projection uses `len(PhysicalProjection.Exprs)`, including inline ordering expressions materialized for Sort/TopN and column pass-through expressions because the executor still materializes an output column.

Sort and TopN do not emit an expression-count unit and do not multiply algorithmic work by `len(ByItems)` or `len(PartitionBy)`. Ordinary column, constant, and multi-key ordering are covered by the single aggregate sorting term. Before publishing that term, inspect `ByItems`: a remaining `ScalarFunction` proves that expression evaluation was not materialized into a child Projection, so the operator fails closed with `missing_ordering_projection` rather than charging expression CPU at Sort/TopN. If `ByItems` references columns produced by an aligned child `PhysicalProjection`, that Projection's normal formula owns all of its `Exprs` once. A missing/misaligned Projection schema also fails closed. For positive-count TopN, compute `k=offset+count` with checked unsigned addition, saturate it to `effective_k=min(uint64(rows), k)`, and only then convert it to float; do not use `count` alone. A zero-count TopN emits zero ordering work regardless of offset because the executor returns without reading its child.

StreamAgg and HashAgg use `len(GroupByItems) + len(AggFuncs)`. One aggregate function descriptor is one slot regardless of partial/final/complete mode and regardless of argument count. This intentionally avoids a two-phase special model. `COUNT(*)` therefore has one slot rather than zero. Ordered aggregate arguments remain inside their aggregate function slot for v4.

For joins, one join-key pair or executable comparison function is one slot, not two column slots. Add the lengths of the remaining `LeftConditions`, `RightConditions`, and `OtherConditions`. HashJoin key pairs come from `EqualConditions + NAEqualConditions`; MergeJoin key comparisons come from `CompareFuncs`.

The IndexJoin family is counted by concrete subtype, because its completed physical plans do not share one key representation:

- `PhysicalIndexJoin`: count aligned `OuterJoinKeys`/`InnerJoinKeys`, the remaining left/right/other conditions, and `len(CompareFilters.OpType)`.
- `PhysicalIndexHashJoin`: count aligned `OuterHashKeys`/`InnerHashKeys` rather than cleared `EqualConditions`, then the remaining left/right/other conditions and `len(CompareFilters.OpType)`.
- `PhysicalIndexMergeJoin`: count `CompareFuncs`, plus `OuterCompareFuncs` only when `NeedOuterSort` is true, then the remaining left/right/other conditions and `len(CompareFilters.OpType)`.

The implementation must centralize this in one helper. It must reject mismatched aligned key slices, a `NeedOuterSort`/`OuterCompareFuncs` structural inconsistency, or another impossible subtype layout instead of selecting an arbitrary side or falling back to stale embedded fields.

Window refines the original direction without adding a new weight:

    n_expr =
        len(WindowFuncDescs)
      + len(PartitionBy)
      + len(OrderBy)
      + len(Frame.Start.CalcFuncs, if present)
      + len(Frame.End.CalcFuncs, if present)

This covers per-row function evaluation, partition/order comparisons, and dynamic frame-bound calculation. It does not add a partition-size, frame-width, or buffering term because current exec details do not expose those values and the requested model should remain simple.

For every expression-based operator, `n_expr == 0` is valid only when the physical operator genuinely contains no expression slot. The resulting CPU work is zero. The implementation must not silently clamp `n_expr` to one; intrinsic work is represented only where the formula explicitly supplies it, such as Limit.

### Row and state semantics

For a TiDB root operator, `rows` is the sum of direct root children's own actual output rows from `BasicRuntimeStats.GetActRows`. Unary operators normally have one child. A missing child statistic is not zero. Reader-like leaf nodes do not reuse their own output rows as an input for the CPU formulas because reader transport has a separate formula.

For a pushed TiKV unary operator, `rows` is the exact-plan-ID actual output rows of its direct cop child. Existing v3 expected/observed task coverage rules remain: no tasks means missing; negative rows are invalid; fewer observed tasks than the component's known coverage is incomplete. The model must not fall back to optimizer estimates.

HashAgg `group_rows` is the operator's own actual output rows. Each physical partial or final Agg node is charged independently from its own input and output; there is no phase multiplier. TiKV Agg keeps the existing independent expected-response versus observed-summary coverage gate before its own rows are accepted.

HashJoin `hash_state_rows` is the cumulative number of build rows actually admitted into hash lookup structures. Duplicate keys count once per admitted row; rows rejected by build filters and ordinary null-key rows that are not retained for lookup do not count. V1 null-aware anti join is the exception: its null-key rows are stored and probed through `hashNANullBucket.entries`, so they do count. If spilling rebuilds hash state in another round, admissions in that round count again because the state construction work repeats. This value comes from the HashJoin executor, not from build-child output rows.

Expose it through one narrow read-only interface in `pkg/util/execdetails`:

    type HashTableRuntimeStats interface {
        RuntimeStats
        HashTableRows() int64
    }

Both private v1 and v2 HashJoin runtime stats implement the getter. V1 records successful `hashTable.Len() + len(hashNANullBucket.entries)` for NAAJ, with a nil bucket contributing zero; v2 records the sum of `validKeyCount` admitted for each completed build round. Clone/Merge preserve the cumulative counter. Tests cover ordinary null rejection and V1 NAAJ null-bucket inclusion. Missing or negative state rows fail with `missing_hash_state_rows` or `invalid_hash_state_rows`. No unique-key set is introduced.

MergeJoin, HashJoin, and IndexJoin `output_rows` is the join node's own `BasicRuntimeStats.GetActRows`. It is the only permitted output term; output bytes remain diagnostic only. Both join inputs and the output are required even when observed as zero.

### Scan bytes

TableScan and IndexScan use the current v3 scan-byte proxy:

    if TotalKeys == 0 and ProcessedKeys == 0 and ProcessedKeysSize == 0:
        scan_bytes = 0
    otherwise:
        scan_bytes = TotalKeys * ProcessedKeysSize / ProcessedKeys

The all-zero case is an observed zero only when the scan leaf has `observed execution summaries == expected responses > 0` and the unique ScanDetail holder has `attachment records == holder expected == scan expected`. An attachment record is one `RecordCopStats` call with a non-nil ScanDetail under the original plan ID; a summary-only `RecordOneCopTask` is not an attachment. The holder's own execution summary is independent and is not required for scan bytes. Otherwise the value is missing or incomplete. In the nonzero case, all three values must be positive and the result must be finite. This proxy preserves scan work for MVCC/skipped keys while using only current `ScanDetail`; it is labeled `scan_detail_processed_key_avg_estimate`, not presented as encoded response bytes.

Each scan detail must be attributable to exactly one scan component. Multi-scan IndexMerge is supported by evaluating each partial scan component separately; it must not share one detail across siblings. Ambiguous or absent attribution fails the affected statement according to the atomicity rules below.

### Reader transport

The four named reader families share one statement-level transport formula. The constructor identifies all executed TableReader, IndexReader, IndexLookup, and IndexMerge nodes, then emits exactly one `id=reader_transport@statement`, `site=tidb`, `op_class=reader_transport` operator with a bounded `operator_kind` set: `table_reader`, `index_reader`, `index_lookup`, `index_merge`, or `mixed_reader`.

`net_bytes` is `RUV2Metrics.TiKVCoprocessorResponseBytes()`. `read_request_count` is `RUV2Metrics.ResourceManagerReadCnt()`. Both are snapshots from the same frozen statement details used by existing preview outputs. The unit source is `ruv2_metrics`; logical `BasicRuntimeStats.GetOutputBytes` remains a diagnostic and is never substituted for transport bytes.

The statement-wide counters are publishable only when the executed flat plan proves a closed producer set: every possible TiKV read-RPC producer for the statement belongs to those four supported cop-reader families. The initial implementation uses this exact algorithm:

1. Only a read-only `SELECT` can pass the closed-set gate. If a DML flat plan contains any supported reader, its reader-transport component is always `unknown_input/ambiguous_reader_transport_producers`; v4 has no DML allowlist because current details cannot exclude uniqueness, locking, FK, transaction, or other ancillary reads.
2. Walk the complete executed `FlatPhysicalPlan`, including IndexJoin inner plans. Classify `*physicalop.PhysicalTableReader` with `StoreType == kv.TiKV`, `*physicalop.PhysicalIndexReader`, `*physicalop.PhysicalIndexLookUpReader`, and `*physicalop.PhysicalIndexMergeReader` as supported producers.
3. Reject any `*physicalop.PointGetPlan` or `*physicalop.BatchPointGetPlan` from the cop-reader transport component; a separate point-lookup component below may publish it only when the complete statement contains point lookup producers and no cop reader or other open read producer. Also reject any `PhysicalTableReader` whose `StoreType != kv.TiKV`, `*physicalop.PhysicalExchangeReceiver`, and `*physicalop.PhysicalExchangeSender`. Any node that the existing preview classifier marks as a reader/store-access class but that is not one of the four supported cop-reader types also opens the set. This catches TiFlash/MPP and future external reader types without treating a new producer as free.
4. Other already-supported CPU, join, aggregation, wrapper, scan-descendant, UnionScan, MemTable, and TableDual nodes are not independent TiKV read-RPC producers and do not open the set. Any structurally unknown plan node continues to fail through the existing unsupported-operator gate, so transport is never published alongside an unknown tree.

A supported reader mixed with any rejected producer is not partially charged from the total counter: SELECT fails atomically; DML marks only its reader-transport component unknown. This is conservative because the current statement counter cannot subtract unsupported RPCs.

Presence and zero handling are normative:

- A nil, bypassed, or otherwise unavailable RUv2 snapshot is missing, even though public getters return zero.
- Inspect `GetTasks()` and `GetExpectedCopTasks()` for every supported reader/cop descendant. If any observed or expected cop task exists, `net_bytes == 0 && read_request_count == 0` is missing rather than free.
- `net_bytes > 0 && read_request_count == 0` is invalid. `read_request_count > 0 && net_bytes == 0` is valid for an empty cop response.
- `net_bytes == 0 && read_request_count == 0` is an observed zero only when a present, non-bypassed frozen RUv2 snapshot exists, no supported descendant has an observed or expected cop task, every supported reader root has observed zero output rows, and the producer set is closed. This represents an empty range/no-request execution.

If no supported reader executed, no reader-transport operator is emitted. Unsupported producers retain explicit bounded status rows until an attributable mapping is designed. The bounded transport reasons are `missing_reader_transport_details` for presence/coverage failure and `ambiguous_reader_transport_producers` for an open producer set.

### Point lookup RPC transport

Read-only, non-locking PointGet and BatchPointGet plans publish only the exact statement `RUV2Metrics.ResourceManagerReadCnt()` value. The constructor emits it once as `id=point_lookup@statement`, `site=tikv`, `op_class=kv_point_lookup`, `input_source=ruv2_metrics`, `input_side=all`, and unit `read_request_count`. Its bounded `operator_kind` is `point_get`, `batch_point_get`, or `mixed_point_lookup` when both physical kinds occur in one otherwise closed statement. The physical PointGet/BatchPointGet plan rows remain non-billable diagnostics; they do not each copy the statement counter.

A present, non-bypassed frozen RUv2 snapshot is required. Zero is valid because a point lookup can be satisfied without a TiKV RPC, for example from transaction-local state; nonzero plan output therefore does not imply a missing counter. A negative counter is invalid. The component emits no `cpu_work`, `scan_bytes`, or `net_bytes`, because current details expose neither point-lookup executor CPU nor attributable Get/BatchGet response bytes.

The point-lookup producer set is closed only for a read-only statement whose complete flat plan contains one or more non-locking PointGet/BatchPointGet nodes and no cop-reader producer, TiFlash/MPP producer, locking point lookup, or unknown store-access producer. DML remains fail-closed because uniqueness checks, locking, foreign keys, or transaction work can add read RPCs. A statement mixing point lookup and cop readers also remains fail-closed rather than allocating the statement total between components.

### IndexJoin request de-duplication

IndexJoin, IndexHashJoin, and IndexMergeJoin do not emit a Join-local `read_request_count`. Their dynamic inner executors use TableReader, IndexReader, or IndexLookup paths, so the resulting physical read RPCs are already present in the statement-level reader-transport `ResourceManagerReadCnt`. PhysicalIndexMergeReader remains a supported standalone transport producer, but `dataReaderBuilder.BuildExecutorForIndexJoin` does not construct it as an IndexJoin inner path.

This is the explicit de-duplication refinement to the initial simple IndexJoin formula:

    initial: IndexJoin CPU + lookup requests + output rows
    v4:      IndexJoin CPU + output rows
             reader_transport already charges all inner physical requests once

The existing private inner task counter remains an EXPLAIN timing diagnostic and is not converted into request RU. IndexJoin adds no detail; the only v4 runtime extension is HashJoin's state-row getter above.

### Write work

The existing statement-local mutation recorder remains authoritative. Its complete semantics for this plan are:

- Count each attempted foreground `Set`, `SetWithFlags`, `Delete`, or `DeleteWithFlags` once after encoding and before calling MemDB. A failed MemDB call still counts. Set bytes are `len(key)+len(value)`; delete bytes are `len(key)`.
- Same-key overwrites, pessimistic statement retries, and mutations later removed by staging cleanup or ROLLBACK remain counted because their encoding/preparation CPU already occurred. `UpdateFlags`, staging release/cleanup, lock-only operations, commit-time net mutations, and local-temporary-table apply copies do not create another foreground mutation.
- The recorder is statement-local and dynamically follows the current `StatementContext`, including optimistic history replay. Restricted/internal SQL never becomes a foreground sample. Retryable explicit transactions whose already-emitted statements cannot be rewritten are marked `optimistic_replay_attribution_unsupported`/partial rather than pretending exact attribution.
- No-op or zero-match DML has a present recorder with zero count and bytes. Deprecated batch DML keeps one recorder across its internal transaction switches, so every batch attempt is counted once. Local-temporary-table encoding is counted once at its foreground MemDB write.

`docs/design/2026-07-10-preview-ru-tidb-kv-mutation.md` remains background evidence, but the bullets above are the normative v4 contract.

The v4 mutation normalization derives the shared CPU unit:

    cpu_work = mutation_count + mutation_bytes / mutation_bytes_per_cpu_unit

`mutation_bytes_per_cpu_unit` is a positive, finite, versioned normalization constant with units bytes per expression-equivalent CPU-work unit. It is stored alongside the v4 weights and included in output metadata. It is not independently multiplied by RU. If it is unset, zero, negative, NaN, or infinite, mutation base components remain visible but weighted v4 total is unavailable with `uncalibrated_weights`.

The externally weight-bearing semantic units are fixed to `cpu_work`, `scan_bytes`, `net_bytes`, `read_request_count`, `write_request_count`, `hash_state_rows`, and `join_output_rows`. Mutation-derived `cpu_work` uses `site=tidb`, `op_class=kv_mutation`, `operator_kind=memdb_mutation`, `input_source=stmt_memdb_mutation_calls`, and `input_side=all`; consumers must use these dimensions to distinguish it from expression CPU work.

`write_request_count` is `RUV2Metrics.ResourceManagerWriteCnt()` and uses its independent `write_request_weight`; read transport uses `read_request_weight`. The frozen snapshot must exist and be non-bypassed. Every DML statement, whether autocommit or inside an explicit transaction, emits the write-request count present in its own finalized snapshot alongside its statement-local mutation work. This is required for pessimistic transactions, whose DML statements can issue nonzero write RPCs before COMMIT. The eventual COMMIT emits only the write requests in its own fresh snapshot, with empty `dml_kind`; it neither absorbs nor back-attributes earlier DML requests. Thus each physical request is owned once by the statement whose RU details recorded it. A nonzero mutation with a missing request payload is partial, while an observed zero DML request count is valid only when the non-bypassed statement snapshot is present and finalized. A known empty transaction COMMIT likewise emits observed zero request work; an absent lifecycle snapshot is missing, not zero. SQL `ROLLBACK` remains unsupported in v4 and emits neither a zero unit nor a total: current routing has no complete rollback-RPC attribution, and declaring it free would be unsafe.

Pipelined transactions retain valid mutation units but mark the write-request component `pipelined_tikv_payload_unsupported` until current details prove a complete logical flush request count. Deprecated batch DML accumulates available write-request snapshots across internal transaction switches; any missing switch makes the request component partial. Optimistic retry/replay keeps the mutation behavior above and marks unavailable request attribution partial. Thus no retry, pipeline, or batch path publishes a known-incomplete zero.

Mutation count and bytes continue to be emitted as zero-weight diagnostics so calibration can change `mutation_bytes_per_cpu_unit` offline. `CommitDetails.WriteKeys/WriteSize` are not substituted for the TiDB mutation unit.

## Availability, atomicity, and degraded behavior

The existing preview gates remain: the feature is default off; `EXPLAIN ANALYZE FORMAT='RU'` explicitly enables collection; unsupported side-effecting/locking/internal paths are rejected; production resource control is untouched. `*ast.RollbackStmt` is explicitly routed to `unsupported/unsupported_statement` before the ordinary SELECT gate, so it cannot be mistaken for a missing-plan SELECT or an observed-zero write.

For side-effect-free SELECT, billing is statement-atomic. If any executed supported operator lacks a required input, the statement records status and reason but emits no billable v4 units and no `total_preview_ru`. This prevents a partial plan from looking cheap. Diagnostic status rows may still identify every missing operator.

For DML, read-tree, mutation, and statement-local write-request components keep independent status. COMMIT has its own write-request component because explicit transactions separate statement lifetimes. Complete components may retain coefficient-free units for calibration, but a statement-level weighted total is absent unless every component expected at that lifecycle point is complete and the weight set is calibrated.

Observed zero is accepted only with presence evidence: an existing root stat, a cop stat with complete task coverage, a HashJoin state-row runtime stat, a mutation recorder snapshot, or a frozen RUv2 snapshot plus the reader consistency checks above. Missing, negative, overflowed, NaN, and infinite inputs have bounded reasons. New reasons are `missing_expression_count`, `missing_ordering_projection`, `invalid_topn_bound`, `missing_reader_transport_details`, `ambiguous_reader_transport_producers`, `missing_hash_state_rows`, `invalid_hash_state_rows`, and `uncalibrated_weights`.

No fallback may use optimizer estimated rows, schema-estimated widths, plan `netDataSize`, string parsing of `EXPLAIN` runtime text, or proportional allocation of statement counters.

## Weight units and migration

The v4 weight container is preview-only:

    type previewRUWeights struct {
        Version                 string
        CPUPerWorkUnit          float64
        ScanPerByte             float64
        NetworkPerByte          float64
        ReadRequest             float64
        WriteRequest            float64
        HashTablePerRow         float64
        JoinPerOutputRow        float64
        MutationBytesPerCPUUnit float64
        Calibrated              bool
    }

The seven RU fields have units stated by their names. `MutationBytesPerCPUUnit` is a normalization, not RU. Validation requires a nonempty new version, finite non-negative RU weights, a positive finite mutation normalization, and `Calibrated=true` before any weighted total is published. Formula tests in `pkg/planner/core` use the private container directly with small deterministic values. No exported setter, session/global variable, or failpoint is added. Package-external executor tests see the production default and assert `uncalibrated_weights`, coefficient-free units, and no `total_preview_ru` until a later calibration change supplies production values.

Set the exact constants `model_version='v4'` and `weight_version='v3-resource-formula-uncalibrated'`. Do not reuse the old model `v3` or weight `v2` labels. The weight-version string intentionally describes the shipped state; a later calibrated weight set must use another immutable version rather than changing values behind this label. Existing statement-summary detail already carries model and weight versions, so old rows remain self-describing and are not rewritten. Queries that compare workload windows must group by both versions.

The existing `ReadBillingDemoBaseUnitSummary` and its infoschema convenience columns (`fixed_events`, `input_rows`, and `input_bytes`) are frozen legacy-v3 views. V4 samples contribute zero to all three, and v4 consumers use the versioned base-unit detail rows instead. Do not reinterpret an old column as `cpu_work`, and do not merge v3 and v4 in those totals. This avoids a cross-version semantic lie and avoids expanding the v1/v2 statement-summary persistence schema in this milestone. Tests must cover v3 legacy aggregation unchanged, v4 legacy totals zero, and v4 detail surviving memory/history readers.

There is no migration of `config.RUV2`, TiKV client RU coefficients, or resource-group settings. Those configure production RUv2 and have different semantics. Delete the current internal `readBillingDemoWeights` map, `readBillingDemoResolveWeights`, and v3 formula application in the same atomic implementation change. `readBillingDemoResult` is constructed and rendered within one statement in one process; no frozen result crosses a process-upgrade boundary, while historical statement-summary rows already store their unit values and versions and are never recomputed from this map. Therefore no v3 calculation compatibility branch is needed or permitted.

All outputs must switch together: EXPLAIN unit rows, `total_preview_ru`, Prometheus base units, statement-summary detail, and general log. A mixed state where logs use v4 while EXPLAIN uses v3 is not accepted. General-log aggregation must extend its key and serialized object to retain `DMLKind`, `InputSource`, and `InputSide`; otherwise distinct v4 units with the same operator/unit label would collapse and lose their provenance. `operator_id` remains an internal/EXPLAIN identity and is intentionally absent from the bounded statement-summary and general-log aggregation keys.

## Plan of Work

### Milestone 1: represent v4 work without changing collection

In `pkg/planner/core/explain_ru.go`, add bounded v4 unit names (`cpu_work`, `expression_count`, `scan_bytes`, `net_bytes`, `read_request_count`, `write_request_count`, `hash_state_rows`, and `join_output_rows`), the validated weight container, and one formula application function. Replace opclass-specific billable lookup for new results with semantic-unit lookup. Keep diagnostic legacy units zero-weight.

Add physical-plan helpers that return expression-slot count for every supported concrete type. Unit-test exact counts for simple and compound Selection, Projection, Agg, Join, and Window plans, including the distinct ordinary IndexJoin, IndexHashJoin, and IndexMergeJoin key representations. For Sort/TopN, test that a root scalar expression backed by inline Projection is evaluated only at Projection, ordinary-column/multi-key plans still receive exactly one aggregate sorting term, and a TiKV pushed TopN with an unmaterialized scalar `ByItems` fails `missing_ordering_projection`. Test checked `offset+count`, a nonzero offset, saturation when `k` exceeds actual input rows, a legal bound near `MaxUint64`, positive-count overflow rejection, zero-count fast-path work, and zero/one-row boundaries. At this milestone, custom unit fixtures in internal `package core` tests use the private calibrated weight container to prove exact algebra and invalid-number rejection before runtime constructors change.

Acceptance: formula tests with injected weights reproduce hand-calculated totals; no production RUv2 API or configuration changes.

### Milestone 2: construct units from authoritative details

Refactor root and cop constructors in `pkg/planner/core/explain_ru.go` around the field mappings in this document. Preserve current exact child-plan attribution and task coverage code where it satisfies the new rows/scan rules. Add statement-scope reader transport from the frozen `RUV2Metrics` snapshot only after proving the closed producer set and the presence/task gates above; support multiple IndexMerge scan components without allocating transport twice.

Use the existing flat-plan build/probe/left/right labels for join rows and the join node's own rows for output. Do not expose or add an IndexJoin lookup-task counter: its dynamic inner readers are already included by the statement-scope reader transport unit.

Add `HashTableRuntimeStats` in `pkg/util/execdetails/runtime_stats.go`. Implement it for both HashJoin runtime-stat versions in `pkg/executor/join/hash_join_stats.go`, recording successful v1/v2 state admissions at the existing build completion points in `hash_join_v1.go` and `hash_join_v2.go`. Do not add a unique-key collector. The later ScanDetail ownership correction may add only an attachment-presence counter and consistent coverage snapshot to `RuntimeStatsColl`; that metadata does not enter a formula and must not alter distsql ownership.

Change write construction in `pkg/planner/core/explain_ru.go` to derive mutation `cpu_work` only from a valid calibrated normalization/weight snapshot and use the current statement's finalized write RPC count for both DML and COMMIT. Do not carry request counts across statements or defer explicit-transaction DML requests to COMMIT. Retain raw mutation units as diagnostics even when the derived unit is unavailable.

Acceptance: every required formula input can be traced to the source table below; source searches show exactly one new Join-only state-row counter and no other runtime field.

### Milestone 3: output migration and behavioral coverage

Bump the constants and renderer in `pkg/planner/core/explain_ru.go`, including `buildReadBillingDemoStatementStats`, `summarizeReadBillingDemoBaseUnits`, the EXPLAIN row builders, and `recordReadBillingDemoMetrics`. Use `pkg/metrics/explain_ru.go::{RecordReadBillingDemoStatement, RecordReadBillingDemoOperatorStatus, AddReadBillingDemoBaseUnits, ObserveExplainRURow}` for the bounded v4 labels; their public signatures need change only if a required existing provenance dimension is absent.

Update aggregation keys and entry conversion in `pkg/util/stmtsummary/read_billing.go`, statement accumulation plus the legacy-v3-only `ReadBillingDemoBaseUnitSummary` behavior in `pkg/util/stmtsummary/statement_summary.go`, and verify `pkg/util/stmtsummary/v2/record.go` persistence/merge plus `pkg/util/stmtsummary/v2/reader.go::{readBillingDemoRowsFromRecord, readBillingDemoBaseUnitColumnValue}` preserve all v4 detail dimensions. Do not add new convenience columns. In `pkg/infoschema/tables.go`, keep the three legacy columns but change their comments to state v3-only/zero-for-v4 semantics; retain the versioned detail table schema unless the existing columns cannot carry one of the frozen dimensions.

In `pkg/executor/adapter.go`, extend `readBillingDemoGeneralLogUnit`, `buildReadBillingDemoGeneralLogUnits`, and `readBillingDemoGeneralLogUnit.MarshalLogObject` so `DMLKind`, `InputSource`, and `InputSide` participate in aggregation, sorting, and serialization. Apply the exact model/weight versions and output semantics atomically across EXPLAIN, metrics, statement summary, and general log. Update `docs/design/2026-07-01-read-billing-demo-ru-model.md` and `docs/design/2026-07-10-preview-ru-tidb-kv-mutation.md` to point to this v4 contract rather than retaining contradictory current-model claims.

Extend the existing internal suites instead of creating a new planner casetest category. Keep formula and constructor tests near `pkg/planner/core/common_plans_test.go`; extend `pkg/executor/explain_test.go` for end-to-end RU output and de-duplication; keep transaction lifecycle tests in `pkg/session/tidb_test.go`. Update corresponding `.agents/skills/tidb-test-guidelines/references/*-case-map.md` files when test files change.

Acceptance: internal formula tests observe exact calibrated totals. EXPLAIN, metrics hooks, statement summary, and log tests observe identical v4 coefficient-free units and the production-default `uncalibrated_weights`/absent-total state; unsupported and missing-evidence cases also have no weighted total, for their specific bounded reasons.

## Authoritative field map

| Formula input | Source today | Attribution and validation | New runtime data? |
|---|---|---|---|
| root `rows` | `RuntimeStatsColl` direct child `BasicRuntimeStats.GetActRows` | exact child plan ID must exist | no |
| cop `rows` | direct child `CopRuntimeStats.GetActRows/GetTasks` | exact plan ID and coverage checks | no |
| `n_expr` | concrete `physicalop` plan fields | centralized type switch, structural validation | no; immutable plan metadata |
| `scan_bytes` | `CopRuntimeStats.GetScanDetail` plus TiDB attachment provenance | unique holder by non-nil `RecordCopStats` attachment; scan summaries and holder attachments each cover all expected responses | **yes, TiDB-only attachment count; no protocol field or formula term** |
| `net_bytes` | `RUV2Metrics.TiKVCoprocessorResponseBytes` | once per statement; non-bypassed presence, descendant task gate, closed read producer set | no |
| reader `read_request_count` | `RUV2Metrics.ResourceManagerReadCnt` | once per statement only when every read-RPC producer is attributable to supported cop readers | no |
| HashAgg `group_rows` | Agg node own runtime rows | TiKV additionally needs expected/observed coverage | no |
| HashJoin `hash_state_rows` | v1 hash-table `Len` plus NAAJ null-bucket entries; v2 row-table `validKeyCount` | completed build round, cumulative across rebuilds | **yes, Join only** |
| Join `output_rows` | Join node own `BasicRuntimeStats.GetActRows` | executed root stat required | no |
| `mutation_count/bytes` | `StatementContext` preview mutation recorder | current attempted-call semantics | no |
| `write_request_count` | current statement's `RUV2Metrics.ResourceManagerWriteCnt` | finalized, present, non-bypassed snapshot; each DML and COMMIT owns only its own count | no |

## Concrete Steps

The design loop must first commit this file and hand the implementation loop the exact commit hash as `<DESIGN_COMMIT>`. From the original repository root, create the required independent branch/worktree with these commands; the committed plan arrives through Git, so no untracked file copy is permitted:

    preview_ru_design_commit=<DESIGN_COMMIT>
    preview_ru_impl_worktree=/DATA/disk4/yiding/gocode/tidb.worktrees/preview-ru-v4-impl
    git cat-file -e "${preview_ru_design_commit}^{commit}"
    git worktree add -b preview-ru-v4-impl "$preview_ru_impl_worktree" "$preview_ru_design_commit"
    cd "$preview_ru_impl_worktree"
    test "$(git rev-parse HEAD)" = "$preview_ru_design_commit"
    test -f docs/design/2026-07-22-preview-ru-resource-formula-plan.md
    pwd
    git branch --show-current
    git status --short

The final `git status --short` must be empty before implementation begins, `pwd` must be `/DATA/disk4/yiding/gocode/tidb.worktrees/preview-ru-v4-impl`, and `git branch --show-current` must print `preview-ru-v4-impl`. If an external orchestration loop creates the worktree, these three facts plus `<DESIGN_COMMIT>` are mandatory handoff evidence; the implementation loop must stop rather than reuse the design-loop worktree or guess another base revision.

Then inspect local changes and apply the Bazel preparation gate:

    git status --short
    git diff --name-status
    git diff -U0 -- '*.go'

Run `make bazel_prepare` if the actual diff changes a Go import section, adds/moves/removes a Go file, adds a top-level `func TestXxx(t *testing.T)`, changes Bazel targets, or hits another trigger in `AGENTS.md`. The implementation should normally extend existing tests, but it must use the actual diff rather than assume the gate result. If run, review generated `BUILD.bazel`/`.bzl` changes and include only those caused by the implementation.

Implement milestones in order. During WIP, run the smallest targeted tests. The affected packages use failpoints, so use the cleanup-safe wrapper rather than raw `go test` where the package scan finds failpoint use:

    ./tools/check/failpoint-go-test.sh pkg/planner/core -run 'TestExplainRU(PlanFormulaAndOperatorClasses|ComponentSnapshotStatusAndWeights)|TestReadBillingDemo'
    ./tools/check/failpoint-go-test.sh pkg/executor -run 'TestExplainAnalyzeFormatRU|TestReadBillingDemoMetricsHook|TestReadBillingDemoGeneralLogUnits|TestWriteSlowLog'
    ./tools/check/failpoint-go-test.sh pkg/executor/join -run 'Test.*HashJoin.*RuntimeStats'
    ./tools/check/failpoint-go-test.sh pkg/session -run 'TestPreviewKVMutationRecorder|TestRUV2Metrics(IsolatedPerStatementInExplicitTxn|WriteRequestsInPessimisticTxn)'
    ./tools/check/failpoint-go-test.sh pkg/util/stmtsummary -run 'TestReadBillingDemo(BaseUnitsToDatum|StructuredRowsToDatum|AggregationCaps|DMLKindAggregation|ReservedStatusMergeBypassesStatusCap)'
    ./tools/check/failpoint-go-test.sh pkg/util/stmtsummary/v2 -run 'TestStmtRecordReadBillingDemoStructuredStats|TestReadBillingDemo(MemReader|HistoryReader)'
    go test -tags=intest,deadlock ./pkg/metrics -run 'TestExplainRUMetrics|TestExplainRUMetricsIgnoreEmptyLabelsAndMissingValues'

The metrics package currently has no failpoint use, hence the raw targeted command above. If a package scan changes at implementation time, follow `docs/agents/testing-flow.md`, switch to the matching wrapper/raw form, and record that evidence.

At Ready, run the minimum targeted set again after any required `make bazel_prepare`, then run:

    make lint

Do not run `make bazel_lint_changed`. Formula unit tests do not require a live TiKV cluster, but the ScanDetail ownership correction must complete one scoped real-TiKV Ready verification because the defect was observed only with the complete real response tuple and native EmbedUnistore cannot supply that evidence.

## Validation and Acceptance

The implementation is accepted only when all of the following are observable.

With private test weights set to simple values inside `pkg/planner/core`, table-driven formula tests show exact results for every operator row in the formula table, including zero rows, one row, multiple expressions, multi-key joins, all three IndexJoin-family key representations, V1 NAAJ null-bucket state, and Window frame expressions. Sort uses `log2(max(rows,2))`; positive-count TopN uses `log2(max(min(rows,offset+count),2))` with checked addition, while zero-count TopN emits zero work. Cases cover nonzero offset, `k>rows`, a legal bound near `MaxUint64`, and overflow rejection. Neither ordering operator has an expression/key-count multiplier, and unmaterialized scalar ordering expressions fail closed rather than being charged there.

End-to-end `EXPLAIN ANALYZE FORMAT='RU'` cases cover Selection/Projection, Sort/TopN, Table/Index scans, each reader family including IndexMerge, RPC-only PointGet/BatchPointGet, UnionScan, Stream/HashAgg, Merge/Hash/IndexJoin, Limit, Window, autocommit write, explicit DML plus COMMIT, unsupported ROLLBACK, and zero-mutation/zero-row cases. An explicit pessimistic transaction case must prove that a DML-local nonzero `ResourceManagerWriteCnt` is emitted by that DML, the later COMMIT emits only its own fresh-snapshot count, and neither count is lost or duplicated. Each attributable case exposes its coefficient-free units, source, and model/weight versions. Because these tests are package-external and production defaults are not calibrated, they assert `uncalibrated_weights` and absence of `total_preview_ru`; exact weighted totals belong to private core formula tests.

A multi-reader or IndexMerge case proves that statement `net_bytes` and `read_request_count` appear once, while every scan retains its own `scan_bytes`. PointGet and BatchPointGet cases prove that a pure read-only plan publishes the statement `read_request_count` exactly once under the bounded lookup kind, publishes no inferred CPU/scan/network unit, accepts a present zero counter, and rejects locking, DML, bypassed/missing detail, and mixed point/cop-reader producer sets. A UnionScan case proves that `tidb/overlay_reader/unionscan` publishes `cpu_work` equal to its direct child's exact actual rows, carries `runtime_child_act_rows`, and has no expression-count multiplier. An IndexJoin case proves that inner lookup requests appear only in reader transport and no Join-local request unit is emitted. DML and COMMIT cases prove that `write_request_count` remains statement-local and uses a coefficient independent from read requests. Sort/TopN cases prove that root scalar expressions materialized by inline Projection are evaluated only there, ordinary-column/multi-key ordering receives one aggregate complexity term without a key multiplier, TopN offset changes `k`, and an unmaterialized pushed scalar TopN fails closed. Reader-gate cases prove that zero rows with observed/expected cop tasks plus a zero RUv2 payload is missing, `requests > 0 && bytes == 0` is valid, a supported reader mixed with PointGet fails closed, and DML with unexcludable ancillary reads marks only reader transport unknown. A ROLLBACK case proves the explicit unsupported status and absence of units.

Missing root stats, incomplete cop summaries, ambiguous scan details, missing reader transport, invalid expression structure, invalid mutation normalization, negative inputs, overflow, NaN, and infinity all fail closed with bounded reasons. SELECT produces no partial billable total. DML preserves complete independent units but does not claim a complete statement total.

Search and API review prove that runtime additions are limited to HashJoin state rows and the TiDB-only ScanDetail attachment count. The latter is provenance only and never enters a formula. `config.RUV2`, TiKV request charging, `ReportRUV2Consumption`, and resource-control behavior are unchanged.

Statement-summary detail, Prometheus metrics hooks, EXPLAIN rows, and general-log details are built from the same frozen result and agree on unit values. General-log records retain DML kind, input source, and input side. Historical v3 rows remain distinguishable by version; legacy three-column convenience totals keep v3 behavior and remain zero for v4, whose memory/history-reader details remain queryable.

## Idempotence and Recovery

All formula construction is read-only over frozen plan/runtime snapshots and must be safe to call repeatedly. Unit construction must not drain `RUDetails`; use the already synchronized/frozen `RUV2Metrics` snapshot exactly as the current preview path does.

`make bazel_prepare`, formatting, and targeted tests are safe to rerun. The failpoint test wrapper always disables failpoints during cleanup. If a milestone leaves mixed output versions, revert only that milestone's focused changes or finish all output consumers before running behavioral tests; never commit a mixed v3/v4 renderer state.

If the runtime source cannot prove a required input, add a bounded status reason and keep the formula unavailable. Do not recover by parsing runtime strings or by introducing an estimate not recorded in this plan. Any newly discovered formula datum still requires revisiting this design. The ScanDetail attachment count is a provenance exception recorded here after real-TiKV evidence showed that a zero value alone cannot distinguish missing detail from an observed empty scan.

## Artifacts and Notes

Current evidence commands used while drafting this plan included:

    rg -n 'type (BasicRuntimeStats|CopRuntimeStats|RuntimeStatsColl)' pkg/util/execdetails/runtime_stats.go
    rg -n 'TiKVCoprocessorResponseBytes|ResourceManagerReadCnt|ResourceManagerWriteCnt|Bypass' pkg/util/execdetails/ruv2_metrics.go
    rg -n 'innerWorker.task|type indexLookUpJoinRuntimeStats' pkg/executor/join
    rg -n 'type Physical(Selection|Projection|TopN|Sort|HashAgg|StreamAgg|HashJoin|MergeJoin|IndexJoin|Window|UnionScan)' pkg/planner/core/operator/physicalop

The evidence establishes availability and location, not completion of the later implementation.

The final ScanDetail-ownership evidence additionally includes a cleanup-safe real-TiKV run with one current-worktree TiDB, one PD, and one TiKV. It observed `cpu_work=4` for `site=tikv, op_class=filter_eval, operator_kind=selection` and `scan_bytes=212.5` for the corresponding table scan in General Log, Prometheus, and statement summary. The shipped `v3-resource-formula-uncalibrated` version left total preview RU absent as required.

The point-lookup evidence additionally includes a cleanup-safe real-TiKV run using the current-worktree TiDB binary and TiKV isolation. `SELECT ... WHERE primary_key = 1` emitted `id=point_lookup@statement, site=tikv, op_class=kv_point_lookup, operator_kind=point_get, read_request_count=1`; the two-key `IN` form emitted the same dimensions with `operator_kind=batch_point_get` and `read_request_count=1`. Neither row contained CPU, scan, or network work, and the uncalibrated summary contained no total preview RU. The exact TiUP tag and data directory were removed and all dedicated ports were closed afterward.

The UnionScan evidence includes a cleanup-safe real-TiKV transaction over three committed rows plus one uncommitted inserted row. Ordinary `EXPLAIN ANALYZE` reported `UnionScan actRows=4` and its direct `TableReader actRows=3`; `FORMAT='RU'` emitted `site=tidb, op_class=overlay_reader, operator_kind=unionscan, input_rows=3` and `cpu_work=3`, both sourced from `runtime_child_act_rows`, with no expression-count unit and no uncalibrated total. The exact `preview-ru-unionscan-v4` TiUP tag and data directory were removed and ports 25000, 23379, 41160, and 24930 were closed afterward.

## Interfaces and Dependencies

Keep all v4 model types private to `pkg/planner/core` except the narrow `execdetails.HashTableRuntimeStats` read interface and the `RuntimeStatsColl` coverage-snapshot method needed by planner/core. Do not export the preview weight container, concrete executor-private Join stats, or add public session/global variables.

The implementation depends only on existing TiDB packages: `physicalop` for immutable plan expressions, `execdetails` for runtime rows/scan/RUv2 details, the statement mutation recorder for write inputs, and existing statement-summary/metrics/log renderers. It adds no third-party dependency and no protocol field.

At milestone completion, the key internal interfaces should have these conceptual signatures:

    func previewRUExpressionCount(plan base.Plan) (int64, bool)
    func previewRUFormulaUnits(plan base.Plan, details previewRUDetails) ([]previewRUUnit, previewRUStatus)
    func previewRUForUnit(unit previewRUUnit, weights previewRUWeights) (weight, ru float64, ok bool)
    type HashTableRuntimeStats interface {
        RuntimeStats
        HashTableRows() int64
    }

The exact private names may follow nearby conventions, but the semantics, data sources, one-Join-runtime-datum boundary, and de-duplication rules in this plan are mandatory.

Revision note (2026-07-22): first complete design draft created from current branch evidence, then revised for explicit de-duplication and the final ordering contract: inline Projection alone owns scalar Sort/TopN evaluation, Sort owns `n*log(n)`, positive-count TopN owns `n*log(min(n,k))` with checked `k=offset+count`, zero-count TopN owns zero work, and inner readers own IndexJoin request cost. HashJoin exposes actual admitted hash-state rows instead of approximating them with all build rows. A later fresh-context audit corrected write-request ownership: explicit-transaction DML and COMMIT each charge only the write RPCs in their own finalized statement snapshot, preventing pessimistic DML requests from being lost.

Revision note (2026-07-23): real TiKV proved that non-Scan execution summaries were present but all units were suppressed by false ScanDetail ambiguity. The plan now records non-nil ScanDetail attachment counts under the original plan ID, validates scan-summary and attachment coverage independently, preserves true empty scans, and requires a scoped real-TiKV Ready verification. This is a minimal evidence/provenance revision; formulas, weights, protocol, distsql ownership, output dimensions, and production RUv2 remain unchanged.

Revision note (2026-07-24): PointGet and BatchPointGet now consume only the existing statement-scoped TiKV read-RPC counter. The plan adds one synthetic point-lookup publisher with a closed producer set and retains fail-closed handling wherever the counter may include locking, DML, cop-reader, MPP, or unknown work. No CPU or byte estimate, runtime field, protocol change, or new weight-bearing unit was introduced.

Revision note (2026-07-24): UnionScan now reuses the existing `cpu_work` semantic unit with `input_source=runtime_child_act_rows` and value equal to the direct child's actual rows. This intentionally simple first formula does not multiply by UnionScan conditions and does not estimate its transaction mem-buffer input.
