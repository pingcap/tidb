# Stabilize preview RU around resource-shaped operator formulas (v6)

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan must be maintained according to it.

## Purpose / Big Picture

The preview RU demo now exposes eight weight-bearing resource units: CPU work, scan bytes, network bytes, hash-state rows, join-output rows, committed write keys, committed write bytes, and frontend compile bytes. Request counters remain available to existing TiDB diagnostics but are retired from preview RU because a request is not a stable proxy for storage work. PointGet and BatchPointGet consume typed TiKV storage scan details; committed writes consume the current statement's frozen commit details; parser and optimizer work for a non-cached statement uses its original SQL byte length as one deliberately simple combined proxy.

After this revision is implemented, `EXPLAIN ANALYZE FORMAT='RU'`, bounded Prometheus metrics, General Log detail, and statement-summary detail tables expose the same coefficient-free resource work units. A reader can reproduce every modeled operator's preview RU with eight named weights and can see exactly which current runtime detail supplied each input. The preview remains isolated from production RUv2 charging and resource-control reporting.

This living plan preserves earlier v4 and v5 evidence for auditability, but the v6 amendment below is normative. It supersedes every historical statement that assigns a preview-RU unit or weight to request counters or treats remote write work as permanently unmodeled.

## Current v6 amendment (normative)

The demo has not entered external testing, so the amended output pair remains `model_version='v6'` and becomes `weight_version='v6-frontend-compile-work-uncalibrated'`. A weight container is valid only when its embedded model version is exactly `v6`, its weight version is calibrated and differs from the shipped uncalibrated label, all eight RU weights are finite and non-negative, and `MutationBytesPerCPUUnit` is finite and positive. Historical v5 rows remain self-describing and are never recomputed or silently relabeled.

The complete v6 weighted formula is:

    preview_ru =
        cpu_work         * cpu_weight
      + scan_bytes       * scan_weight
      + net_bytes        * net_weight
      + hash_state_rows  * hash_table_weight
      + join_output_rows * join_weight
      + write_keys       * write_key_weight
      + write_bytes      * write_bytes_weight
      + frontend_compile_bytes * frontend_compile_weight

The eight and only eight weight-bearing semantic units are `cpu_work`, `scan_bytes`, `net_bytes`, `hash_state_rows`, `join_output_rows`, `write_keys`, `write_bytes`, and `frontend_compile_bytes`. Read and write request units remain globally retired. The v5 read-side, PointGet/BatchPointGet, request-retirement, and demo fork dependency contracts below carry forward except for the v6 output identity and the explicit local-short-circuit and locking refinements stated below.

Frontend work is one statement-level TiDB component with `site=tidb`, `op_class=sql_frontend`, `operator_kind=parser_optimizer`, `input_source=statement_original_sql`, and `input_side=all`. For a supported successfully completed statement that did not hit plan cache, `frontend_compile_bytes` is the byte length of the resolved target AST's `OriginalText()` and receives its independent `FrontendCompileWeight`. A prepared miss therefore uses the prepared SQL template. If the target AST has no preserved text, as with the ordinary `EXPLAIN ANALYZE` grammar, use the statement context's actual submitted SQL; `StmtNode.Text()` is only the last fallback for programmatically constructed ASTs. If all three sources are empty, omit only the frontend component: preserve the statement status and every already observed execution/storage/write unit, do not publish a zero-byte frontend sample, and do not suppress a calibrated total derived from the remaining components. `FoundInPlanCache=true` uses the same no-component output shape. This demo therefore cannot distinguish cache-hit exclusion from missing frontend text in its output, but neither case becomes partial or fail closed. A cache miss, disabled cache, and cache-ineligible SQL with available text use the same ordinary non-hit formula. This proxy intentionally folds parser and optimizer cost into one coefficient; it does not claim optimizer CPU is intrinsically linear in SQL length. Non-prepared cache-hit parsing plus all cache lookup, parameter adjustment, cached-plan rebuild work, and statements with unavailable source text are deliberate demo-stage undercounts.

Write mutation preparation continues to expose `encoded_mutation_count`, `encoded_mutation_bytes`, `set_count`, `delete_count`, `key_bytes`, and `value_bytes` as raw diagnostics. It derives `cpu_work = encoded_mutation_count + encoded_mutation_bytes / MutationBytesPerCPUUnit` only from a valid calibrated v6 snapshot, and applies `CPUWeight` exactly once. The shipped uncalibrated snapshot exposes the six diagnostics but neither mutation-derived `cpu_work` nor any weight or total preview RU.

Remote write work comes only from the current statement's frozen `RUV2Metrics.WriteKeys()` and `WriteSize()`, whose original provenance is CommitDetails. Autocommit DML owns and publishes its committed pair with `site=tikv`, `op_class=kv_write`, `operator_kind=txn_write`, `input_source=commit_detail`, and `input_side=all`; it retains the DML kind and `txn_prewrite_payload` scope. A non-pipelined DML executing inside an explicit transaction publishes no remote-write component, even when pessimistic locking produced a positive `ResourceManagerWriteCnt`: pessimistic-lock work is intentionally unmodeled and is not final committed write payload. The final COMMIT solely owns the transaction's write pair, leaves `dml_kind` and scope empty, and never inherits mutation diagnostics from earlier statements. Pipelined DML and COMMIT remain partial and publish no write units because their CommitDetails payload is incomplete.

The write pair is fail-closed. Nil or bypassed metrics, either negative field, and a pair with exactly one positive field produce a partial component with no write units. `(0,0)` is a valid observed zero and publishes both zero-valued units; a pair with both values positive publishes both units. `ResourceManagerWriteCnt` is not a v6 formula or coverage input. The outward byte unit is exactly plural `write_bytes`; v6 has no singular `write_byte` alias or semantic-name translation.

All four output surfaces consume the same frozen v6 result. They expose raw valid write and frontend units even while the production weights are uncalibrated, but expose no per-unit weight, mutation-derived CPU, or total preview RU until one valid calibrated eight-weight snapshot is installed.

`EXPLAIN ANALYZE FORMAT='RU'` DML renders after the autocommit transaction has produced CommitDetails but before ordinary statement finalization has merged them into the live `RUV2Metrics`. Its reporting snapshot therefore clones the live metrics and completes only that clone from the statement CommitDetails, without invoking metric-producing `Add*` methods. The later finalization remains the sole merger into live metrics and process-wide RUv2 counters. This reporting bridge neither changes explicit-transaction ownership nor converts a legitimate no-op `(0,0)` pair into a coverage failure.

Point lookup coverage keeps the typed storage-work contract, with one narrow local-short-circuit refinement. A constructed PointGet or BatchPointGet executor must still expose its plan-local typed `SnapshotRuntimeStats`; completed remote responses retain the exact detail-coverage checks below. However, partition pruning can replace the executor with `TableDualExec` after the physical PointGet plan has been flattened. When that plan ID has completed byte-bearing `BasicRuntimeStats` and exactly zero actual rows, absence of the snapshot group is authoritative evidence that the local replacement performed no point storage work; that plan contributes zero to all point work and diagnostics. A missing byte sentinel, nonzero actual rows, or any invalid/incomplete typed detail remains fail-closed. Basic stats are used only as completion evidence and never as unit data.

Locking SELECT is supported without introducing request work. `SelectLock` is a non-billable TiDB wrapper, and the `Lock` flag on PointGet/BatchPointGet does not change the storage-work formula. Pessimistic-lock, lock-wait, retry, and related RPC activity is intentionally ignored because v6 has no request unit or coefficient. If a locking point read is satisfied by the lock cache and its typed snapshot reports zero completed point responses, it publishes zero point work under the existing coverage rule; if point storage responses are present, only their typed `TotalKeys` and `ProcessedKeysSize` are consumed. Lock counters and lock response payloads are never substituted for scan detail, reader network bytes, or committed write work.

### TiDB local Shuffle amendment (normative)

TiDB root `PhysicalShuffle` is weight-bearing under `site=tidb`, `op_class=shuffle`, and bounded `operator_kind=hash_shuffle` or `range_shuffle`. It emits exactly one existing `cpu_work` semantic unit with `input_source=shuffle_data_source_act_rows` and `input_side=all`; it adds no unit, weight, normalization, model version, or weight version. For DataSource index `i`, let `n_i` be the completed runtime output rows under `PhysicalShuffle.DataSources[i].ID()` and `k_i` be `len(PhysicalShuffle.ByItemArrays[i])`. The formula is:

    cpu_work = sum_i n_i * (1 + k_i)

The base `n_i` term owns hash/group boundary selection, worker selection, and in-process row dispatch. The `n_i*k_i` term owns partition-key evaluation and encoding. `Shuffle.actRows`, worker-tail output rows, receiver rows, and concurrency are not formula inputs. A MergeJoin Shuffle therefore aggregates its two DataSources once at the owning Shuffle even though the worker plan contains two receivers. Sort, Window, StreamAgg, MergeJoin, readers, and scans keep their independent existing units.

`PhysicalShuffleReceiverStub` is a transparent non-billable wrapper. Traversal continues through its `DataSource`, so the wrapper neither truncates descendant collection nor republishes Shuffle work. TiFlash MPP `ExchangeSender` and `ExchangeReceiver` remain unsupported and are not reclassified as local Shuffle.

The constructor requires nonempty equal-length `DataSources` and `ByItemArrays`, a positive unique plan ID for every DataSource, exactly one occurrence across the flattened main/CTE/scalar-subquery tree set and in the current tree, and a `BasicRuntimeStats` byte record for every DataSource. The byte record distinguishes build-time registration from successful `Next` evidence and admits an explicitly observed zero rows. Duplicate/aliased IDs, missing evidence, negative rows, checked integer multiply/add overflow, and non-finite converted work fail closed. The resulting unit never emits `net_bytes`, `scan_bytes`, `hash_state_rows`, or `join_output_rows`; TiDB chunk/channel Shuffle is not a network boundary.

## Historical v5 amendment (superseded for version and remote writes)

The immutable output pair is `model_version='v5'` and `weight_version='v5-storage-work-uncalibrated'`. A weight container is valid only when its embedded model version is exactly `v5`, its weight version is calibrated and not the shipped uncalibrated label, all five RU weights are finite and non-negative, and `MutationBytesPerCPUUnit` is finite and positive. EXPLAIN notes include both versions. Historical v4 rows remain self-describing; no v4 result is recomputed or silently relabeled.

The complete v5 weighted formula is:

    preview_ru =
        cpu_work         * cpu_weight
      + scan_bytes       * scan_weight
      + net_bytes        * net_weight
      + hash_state_rows  * hash_table_weight
      + join_output_rows * join_weight

No request counter is a v5 base unit or weight input. `GetCmdRPCCount`, `ResourceManagerReadCnt`, and `ResourceManagerWriteCnt` remain unchanged for existing diagnostics and may be consulted only as non-billable presence/coverage evidence where the implementation already needs it. Reader/Lookup/Merge transport publishes only attributable `net_bytes`. IndexJoin adds no request-like term; its dynamic inner reader transport remains covered by the same statement network-byte publisher.

PointGet and BatchPointGet publish one synthetic `point_lookup@statement` operator with `site=tikv`, `op_class=kv_point_lookup`, `input_source=snapshot_runtime_stats`, and `input_side=all`. The bounded kind is `point_get`, `batch_point_get`, or `mixed_point_lookup`. For every participating plan ID, consume only the typed client-go API:

    GetScanDetailAndCoverage() (detail ScanDetail, detailRecords uint64, completedResponses uint64)

The formulas are `cpu_work = detail.TotalKeys` and `scan_bytes = detail.ProcessedKeysSize`. `TotalKeys` is TiKV storage-operation work, not SQL logical key count. `ProcessedKeysSize` is observed MVCC user-key-plus-value work, not request encoding or network bytes. `ProcessedKeys` remains diagnostic. Also publish zero-weight diagnostics `total_keys`, `processed_keys`, `processed_keys_size`, `detail_records`, and `completed_responses` under the same dimensions.

Point coverage is fail-closed and does not parse `SnapshotRuntimeStats.String()`. Negative detail values and checked-addition overflow are invalid. When `completedResponses > 0`, every completed response must carry typed detail, so `detailRecords == completedResponses`; otherwise the component is `unknown_input/incomplete_point_scan_detail`. When `completedResponses == 0`, `detailRecords` and all three accumulated detail fields must be zero; this is an observed local/cache/MemDB zero and publishes zero-valued work and diagnostics only when the plan-local typed stats object is present. A present all-zero ScanDetail is distinct from missing detail and is accepted. Locking point reads remain unsupported. Read-only and DML point plans share this exact plan-local ownership and never copy a statement request counter.

For the demo-stage dependency, TiDB temporarily replaces `github.com/tikv/client-go/v2` with the reviewed fork backport `github.com/winoros/client-go/v2@v2.0.0-20260802190030-ae98d71ed578`. The backport is based on TiDB's prior client-go baseline and adds typed detail/coverage accumulation, clone/merge, synchronous/asynchronous BatchGet, multi-region, key-error/lock-retry, epoch-not-match, concurrency coverage, and checked arithmetic with an explicit negative invalid sentinel without pulling the incompatible newer PD API. This fork dependency is acceptable for the demo; minimum-cluster-version negotiation is intentionally deferred. A production release must first move the API to the official module and select a new immutable dependency/compatibility contract.

Write mutation preparation continues to publish six raw diagnostics. It derives `cpu_work = encoded_mutation_count + encoded_mutation_bytes / MutationBytesPerCPUUnit` only from a valid calibrated v5 snapshot and applies the shared CPU weight once. Remote write work is not modeled by a request unit: a DML with a present non-bypassed frozen snapshot and zero remote write count is complete for the remote-write component; a positive count is `partial/unmodeled_tikv_write_work`; missing, bypassed, or negative coverage is `partial/missing_tikv_write_coverage`. Pipelined DML is `partial/pipelined_tikv_write_work_unmodeled`. Every COMMIT is partial because v5 does not model its remote work; ordinary and pipelined commits use `unmodeled_tikv_commit_work` and `pipelined_tikv_commit_work_unmodeled`, respectively. DML mutation diagnostics remain visible independently, but any partial component suppresses total preview RU.

All four output surfaces consume the same frozen result. Positive typed point data produces CPU, scan, and five diagnostic units in EXPLAIN, General Log, Prometheus, and statement summary. A backend response missing typed ScanDetail fails closed on every surface; tests must not synthesize detail merely to make MockStore appear compatible. The shipped uncalibrated weights never produce mutation-derived CPU work or `total_preview_ru`.

## Progress

- [x] (2026-08-05 15:50+08:00) Reconstructed local Shuffle planning, executor construction, flat-plan traversal, and Basic runtime evidence. Confirmed that one producer executor exists per `PhysicalShuffle.DataSources` entry, receiver stubs are reused across workers, and the flat tree follows each receiver to its DataSource once.
- [x] (2026-08-05 15:50+08:00) Selected direct DataSource-plan-ID attribution with one aggregate Shuffle `cpu_work` unit, transparent receivers, checked arithmetic, and explicit-zero evidence. Rejected Shuffle output rows, receiver rows, flat direct-child rows, and concurrency multipliers.
- [x] (2026-08-05 16:00+08:00) Added failure-before planner and executor regressions. Before the implementation, the planner classification/formula cases failed and the real StreamAgg plan stopped with `unsupported_operator operator=tidb/reader_receive/shuffle`; the latter also showed DataSource `actRows=838` versus Shuffle `actRows=5`.
- [x] (2026-08-05 16:18+08:00) Implemented bounded hash/range Shuffle classification, one aggregate DataSource-owned CPU unit, transparent receivers, and checked fail-closed arithmetic. Formula, FORMAT RU, real two-source MergeJoin/zero-side, concurrency, forbidden-unit, Prometheus, and statement-summary regressions pass. Ready `make lint`, formatting, and diff checks pass with every failpoint refcount restored to zero. The Bazel gate found no file/import/top-level-test/module/Bazel trigger, so `make bazel_prepare` was intentionally skipped.
- [x] (2026-08-05 14:30+08:00) Reproduced the PointGet boundaries against a cleanup-safe real TiKV playground before changing production code. Ordinary primary-key, unique-index first-leg, and BatchPointGet misses retained typed `TotalKeys`; a partition-pruned PointGet became `partition:dual` with no Snapshot stats and failed `missing_point_scan_stats`. Pessimistic PointGet/BatchPointGet `FOR UPDATE` plans used locking paths with no Get/BatchGet scan detail and were rejected by the syntax gate.
- [x] (2026-08-05 14:35+08:00) Added fail-before regressions for the local completed-zero short circuit and locking SELECT, then implemented the narrow Basic-stats proof, non-billable Lock wrapper, and lock-independent point formula. Focused planner and executor WIP suites pass under the failpoint wrapper with every reference count restored to zero.
- [x] (2026-08-05 14:48+08:00) Reran cleanup-safe TiUP verification with the current TiDB and fixed local PD/TiKV binaries. Primary-key, unique-index-first-leg, and BatchPointGet misses retained exact `cpu_work=1/1/2`; partition-dual PointGet published exact zero work. Pessimistic PointGet hit/miss, BatchPointGet, range SELECT, and UPDATE all completed without lock-derived units or unsupported status, while range scan/CPU/network and mutation diagnostics remained visible. The exact tag, binary, PD/TiDB/TiKV ports, and data directory were cleaned and verified absent.
- [x] (2026-08-05 14:52+08:00) Completed the Ready profile from the final code state. Focused planner and executor suites, including EXPLAIN, DML, General Log, and Prometheus paths, passed under the failpoint wrapper with refcount zero; `make lint`, `gofmt -d`, and `git diff --check` passed. The Bazel gate found no import, Go-file, top-level-test, module, Bazel-file, or target change, so `make bazel_prepare` was intentionally skipped.

- [x] (2026-08-03 15:08+08:00) Reconstructed the frontend lifecycle and compared direct timing, four-counter normalization, fixed hit/miss events, and one-byte-proxy designs. The accepted unreleased-v6 amendment uses one independently weighted `frontend_compile_bytes` unit for non-hits and treats plan-cache hits as a valid no-component policy exclusion.
- [x] (2026-08-03 15:18+08:00) Implemented the statement-level constructor, eighth weight, byte rendering, and exact `FoundInPlanCache` exclusion without changing production RUv2.
- [x] (2026-08-03 15:29+08:00) Added formula, EXPLAIN, General Log, Prometheus, and statement-summary regressions. A cacheable prepared INSERT proves the first miss publishes exactly one template-byte sample while the second execution reports one plan-cache hit and publishes no frontend sample. WIP targets pass individually with every failpoint refcount restored to zero.
- [x] (2026-08-03 15:38+08:00) Fresh implementation review round 1 found that a non-hit with no AST, statement-context, or fallback text was incorrectly published as zero bytes. Changed that shape to atomic `unknown_input/missing_statement_original_sql`, cleared prior units, and added a direct regression; round 2 receives the corrected diff with no inherited conclusion.
- [x] (2026-08-03 15:45+08:00) Fresh implementation review round 2 found that a nil AST bypassed both statement-context fallback and missing-input failure. Refactored the fallback to accept context-only evidence, added nil-AST present/missing tests, and moved pre-existing pure execution-formula fixtures to the extracted execution-only helper. The reviewer's broad planner command then passed all changed frontend/expression cases; its remaining `TestReadBillingDemoHashJoinUnitsUseBuildProbeSides` failure is outside this diff and asserts v3-era `fixed_events`/side diagnostics that current HEAD production no longer emits.
- [x] (2026-08-03 16:05+08:00) Fresh implementation review round 3 approved the complete frontend contract, including nil-AST fallback, atomic missing-text failure, cache-hit omission, prepared hit/miss lifecycle, eight-weight validation, and all four output surfaces. The final scoped Ready matrix, `make lint`, `gofmt -d`, and diff checks passed with every failpoint refcount restored to zero. The Bazel gate found no import, file, top-level-test, module, or Bazel-target trigger, so `make bazel_prepare` was intentionally skipped. The only failure in an additional broad planner sweep was the pre-existing stale `TestReadBillingDemoHashJoinUnitsUseBuildProbeSides` assertion described above and outside this amendment.
- [x] (2026-08-03 16:42+08:00) A follow-up contract correction removed frontend-only atomic failure. A regression first reproduced `unknown_input` and cleared execution units when all SQL-text sources were empty; the implementation now omits only the unavailable frontend component, preserves success and prior units, and never fabricates a zero-byte sample. This deliberately makes missing text observationally identical to the existing cache-hit exclusion.
- [x] (2026-08-03 16:49+08:00) The corrected planner regression and ordinary EXPLAIN RU regression passed under the failpoint wrapper with every refcount restored to zero; `make lint`, `gofmt -d`, and `git diff --check` passed. The Bazel gate found no generated-file trigger, so `make bazel_prepare` remained unnecessary.

- [x] (2026-08-03 13:50+08:00) Rechecked the approved v6 write proposal against the statement lifecycle. Added a regression that failed against v5 because a frozen `(WriteKeys=3, WriteSize=66)` snapshot emitted no write units, then implemented the v6 identity, seven-weight formula, transaction ownership, plural units, and fail-closed write-pair validation.
- [x] (2026-08-03 14:03+08:00) Extended the existing EXPLAIN, General Log, Prometheus, and statement-summary regressions. The scoped planner, executor, metrics, and statement-summary WIP tests passed; every failpoint wrapper returned to `new_refcount=0`. `make bazel_prepare`, `gofmt -d`, and `git diff --check` passed without adding generated-file drift. Ready/lint/real-TiKV verification and fresh implementation review remain assigned to the completion owner.
- [x] (2026-08-03 14:12+08:00) Real TiKV exposed that autocommit `EXPLAIN ANALYZE FORMAT='RU'` rendered a valid nonempty INSERT as `write_keys=0, write_bytes=0`: CommitDetails existed, but the Explain reporting clone preceded the normal live-metrics merge. A fresh reviewer classified this as blocking. Added a no-Prometheus-side-effect reporting clone, proved the new regression fails with an ordinary clone and passes with the bridge, and retained the explicit pessimistic DML no-component rule.
- [x] (2026-08-03 14:36+08:00) Fresh implementation review round 3 approved the reporting bridge and the complete v6 write contract. The final Ready target matrix, `make lint`, `make bazel_prepare`, formatting, and diff checks passed with every failpoint refcount restored to zero. Cleanup-safe real-TiKV rerun produced EXPLAIN INSERT `write_keys=1/write_bytes=60`, ordinary autocommit INSERT `1/76`, no `kv_write` unit or status for a pessimistic UPDATE, and COMMIT-only `1/92`; General Log, Prometheus, and statement summary matched, request units were absent, all ports were closed, and the test tag and binary were removed.

- [x] (2026-08-03 01:20+08:00) Revalidated the request-retirement proposal against production constructors and output consumers. Retired both request units and weights globally, reduced the weight-bearing semantic set to five, and migrated output provenance atomically to model v5 / uncalibrated storage-work weights.
- [x] (2026-08-03 01:40+08:00) Backported the typed PointGet/BatchPointGet scan-detail coverage API onto the compatible client-go baseline, ran its focused synchronous/asynchronous, multi-region, clone/merge, and concurrency regressions, and pushed fork commit `4aeeb1e549a01cb28db3a57247eff56c4a0d1d07` for the demo dependency.
- [x] (2026-08-03 02:05+08:00) Implemented v5 point storage work, raw diagnostics, coverage validation, reader network-only transport, and remote-write partial rules. Formula tests cover hit/miss/partial-hit shapes, present zero versus missing detail, DML ownership, locking rejection, multi-plan accumulation, negative values, and overflow.
- [x] (2026-08-03 02:18+08:00) Verified that MockStore ordinary PointGet responses can complete without ScanDetailV2. The production result correctly reports `unknown_input/incomplete_point_scan_detail`, emits no point base units, and leaves Prometheus CPU counters unchanged; EXPLAIN fixtures and direct renderer/aggregation tests provide positive typed-output coverage without weakening fail-closed behavior.
- [x] (2026-08-03 02:37+08:00) Completed the scoped WIP validation: the v5 planner formula/write suite, six executor output/lifecycle tests, tagged metrics test, and statement-summary aggregation test passed. Every failpoint wrapper returned to `new_refcount=0`; `make bazel_prepare` regenerated only the fork dependency metadata. Ready validation, lint, and real-TiKV verification remain deliberately assigned to the completion owner.
- [x] (2026-08-03 03:21+08:00) Completed the final Ready profile from the reviewed worktree. The planner, executor, metrics, and statement-summary target sets passed; every failpoint wrapper returned to `new_refcount=0`; `make lint`, `git diff --check`, `gofmt -d`, the generated Bazel dependency, and the resolved fork pseudo-version all passed final-state checks.
- [x] (2026-08-03 03:29+08:00) Completed cleanup-safe real-TiKV verification with the current-worktree TiDB, PD `8cabcf154cce072bb1ed8370a6593783625187c7`, and TiKV `6cdd89619503228b0e85adbf278ba21fce516499`. Point hit/miss, BatchPointGet full/partial/all miss, unique-index two-stage PointGet, reader network-only output, and pessimistic explicit-transaction DML/COMMIT all matched v5 through EXPLAIN, General Log, Prometheus, and statement summary. The `preview-ru-v5-930a` tag, temporary binary, and all four ports were cleaned; the PD endpoint was unreachable afterward.
- [x] (2026-08-03 03:00+08:00) Addressed v5 implementation review round 1 in the client-go fork. Point scan-detail record and merge paths now use checked conversion/addition for all three work fields and both coverage counters, propagate invalid state through Clone/Merge, preserve unrelated ScanDetail diagnostics, and return a documented negative sentinel for nil or invalid stats. Deterministic unit tests and real UniStore integration tests cover overflow, epoch-not-match exclusion, and PointGet lock retry accumulation; fork commit `ae98d71ed578167ff6bf69c5dc884f497a386647` was pushed and read back.
- [x] (2026-08-03 03:11+08:00) Completed the review-fix WIP matrix. Both client-go targeted suites passed; TiDB planner, executor, metrics, and statement-summary targets passed against pseudo-version `v2.0.0-20260802190030-ae98d71ed578`; both `make bazel_prepare` runs succeeded and every TiDB failpoint wrapper returned to `new_refcount=0`. Calibrated mutation plus positive/pipelined DML and ordinary/pipelined COMMIT now each prove that a partial remote component suppresses the summary total while statement-local mutation diagnostics remain independent.

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
- [x] (2026-07-22 16:09+08:00) Historically split the then-shared request contract into direction-specific v4 terms. This experiment is retained only as migration history and was globally retired by v5.
- [x] (2026-07-22 16:14+08:00) Completed the historical request-split Ready gate. Both direction regressions caught restoration of the earlier shared term; all focused checks passed. V5 supersedes the resulting request-bearing model.
- [x] (2026-07-22 18:45+08:00) Corrected TopN ordering work to saturate its legal `offset+count` heap bound at actual input rows and to emit zero work when `count=0`. The focused regression failed against the unsaturated formula and passed after the fix; overflow remains fail-closed for every positive count.
- [x] (2026-07-22 18:47+08:00) Completed the TopN correction Ready gate: the full preview-RU planner target, EXPLAIN/metrics/general-log executor targets, `make lint`, and `git diff --check` passed with failpoint refcount returning to zero. The Bazel gate found no trigger, so `make bazel_prepare` was not required.
- [x] (2026-07-23 23:35+08:00) Reproduced the real-TiKV non-Scan publication failure in code: distsql attaches response ScanDetail to the component's last plan ID while the scan leaf receives only an execution summary, and the v4 estimator counted both the real holder and the leaf's default-zero detail as holders. Three rounds of ownership, fail-closed, and validation review converged on explicit TiDB-side ScanDetail attachment provenance without changing the distsql ownership or TiKV protocol.
- [x] (2026-07-24 00:05+08:00) Added a regression that exactly uses `RecordOneCopTask` for the scan leaf and `RecordCopStats` for the parent holder. It failed before the fix with `ambiguous_cop_scan_width` and passed after `RuntimeStatsColl` began recording ScanDetail attachments and the estimator validated holder/task coverage from one consistent snapshot.
- [x] (2026-07-24 00:20+08:00) Completed the provenance regression matrix: explicit empty detail, repeated same-holder attachments, attachment-without-holder-summary, independent scan-summary and attachment coverage failures, no holder, partial tuple, multi-holder ambiguity, ExecutorId remap, and statement-atomic failure are all covered. Independent implementation and test reviewers both concluded LGTM with no P0/P1/P2.
- [x] (2026-07-24 00:33+08:00) Completed cleanup-safe real-TiKV validation against PD/TiKV `v9.0.0-beta.2.pre-363-g8cabcf1` and the current-worktree TiDB binary. Selection, Projection, Limit, TopN, HashAgg, and ordered StreamAgg all succeeded in `FORMAT='RU'`; the Selection probe emitted `tikv/filter_eval cpu_work=4` and `tikv/kv_range_scan scan_bytes=212.5` identically through General Log, Prometheus, and statement summary. The exact TiUP tag was cleaned and the PD endpoint was unreachable afterward.
- [x] (2026-07-24 00:36+08:00) Completed the final Ready gate after the stable ordered-StreamAgg fixture revision: targeted execdetails, distsql, planner/core, and executor tests passed; `make lint` and `git diff --check` passed; failpoint refcount was zero. The Bazel gate found no added/moved Go file, import change, new top-level test, Bazel edit, or module edit, so `make bazel_prepare` was not required.
- [x] (2026-07-24 01:20+08:00) Added statement-scoped RPC-only preview units for closed-set read-only PointGet and BatchPointGet plans. The implementation emits exactly one synthetic point-lookup operator, retains physical lookup rows as non-billable diagnostics, and keeps locking, DML, mixed cop-reader, and unknown producer shapes fail-closed.
- [x] (2026-07-24 01:35+08:00) Observed the focused EXPLAIN regression fail against the prior `ambiguous_reader_transport_producers` behavior and pass after the implementation. A cleanup-safe real-TiKV run confirmed the historical RPC-only point shape; v5 supersedes that output with typed storage work.
- [x] (2026-07-24 02:05+08:00) Bound UnionScan's intentionally simple CPU formula to its direct child's actual runtime rows: `cpu_work=input_rows`. The focused regression failed while `overlay_reader` was non-billable and passed after the formula was enabled, with no expression-count multiplier or new runtime datum.
- [x] (2026-07-24 02:25+08:00) Completed UnionScan output and Ready verification. A cleanup-safe real-TiKV transaction produced UnionScan output rows 4 over direct-child rows 3 and published exactly `tidb/overlay_reader/unionscan cpu_work=3` from `runtime_child_act_rows`; the focused preview-RU planner suite, `make lint`, and `git diff --check` passed with failpoint refcount zero. The Bazel gate found no import, file, top-level-test, Bazel, or module trigger, so `make bazel_prepare` was not required.
- [x] (2026-07-24 12:20+08:00) Enabled DML PointGet and BatchPointGet request publication from their plan-local `SnapshotRuntimeStats.GetCmdRPCCount`. The focused regression failed against the blanket DML rejection and passed after the change; the DML statement producer set remains open, so pessimistic-lock and ancillary requests cannot be copied from the statement RUv2 counter into PointGet.
- [x] (2026-07-24 12:26+08:00) A cleanup-safe real-TiKV run with the current-worktree TiDB binary confirmed historical plan-local point RPC attribution and statement-local pessimistic-write attribution. V5 keeps the plan-local point source but retires both request outputs. The unique TiUP tag was removed and the pre-existing playground tags were preserved.
- [x] (2026-07-24 12:27+08:00) Completed the Ready gate: `make bazel_prepare` generated only the required planner/core TiKV RPC dependencies; the focused planner and EXPLAIN failpoint-wrapped regressions passed with refcount returning to zero; `make server`, `make lint`, and `git diff --check` passed.
- [x] (2026-07-28 23:20+08:00) Reproduced the empty-range failure with the current-worktree TiDB and real TiKV: empty table, nonmatching table range, and nonmatching index range each returned `TotalKeys=1`, `ProcessedKeys=0`, and `ProcessedKeysSize=0`, then failed v4 atomically with `missing_scan_width_evidence`. A scan that processed rows before Selection filtered all output remained calculable.
- [x] (2026-07-28 23:35+08:00) Added the real-TiKV tuple to the focused range-scan regression and observed it fail before the production change. Updated the scan-input contract so a completely observed zero-processed tuple emits `scan_bytes=0` even when `TotalKeys>0`; mismatched processed count/size, missing attachment provenance, and incomplete task coverage remain fail-closed.
- [x] (2026-07-28 23:50+08:00) Completed the zero-processed scan Ready profile. The focused planner regression and the directly related EXPLAIN, General Log, and Prometheus tests passed with failpoint cleanup; `make lint` and `git diff --check` passed. A cleanup-safe real-TiKV run proved zero `scan_bytes` plus nonzero reader transport for empty table, nonmatching table range, and nonmatching index range, and statement summary reported every component `ok`. The Bazel gate found no trigger. A broader preview-RU planner name sweep still finds two pre-existing stale assertions for diagnostic units removed by `1d5da3935a`; this focused correction does not alter those paths.
- [x] (2026-07-29 00:30+08:00) Reproduced the IndexJoin empty-outer failure with real TiKV. Ordinary `EXPLAIN ANALYZE` showed the outer reader completed with zero rows while the dynamically built inner reader had no execution info; `FORMAT='RU'` and the completed statement both failed atomically with `missing_runtime_rows` at `tidb/join_lookup/indexjoin`, and statement-summary base units were empty. HashJoin V1/V2 and MergeJoin controls retained runtime evidence for both direct children and published units successfully.
- [x] (2026-07-29 02:10+08:00) Five proposal revisions and fresh-context reviews converged on a narrow proven-skipped contract. The final two fresh reviewers independently approved the same design: only the three IndexJoin subtypes may prove a dynamically unconstructed inner subtree skipped, all evidence remains fail-closed, and a statement-wide plan-ID ownership gate closes the gap between occurrence-scoped flat plans and plan-ID-aggregated runtime details.
- [x] (2026-07-29 02:35+08:00) Added the first focused three-subtype regression under the existing preview-RU formula test and ran it before the production change. All three cases failed exactly as expected with `status=unknown_input, reason=missing_runtime_rows`; the failpoint wrapper restored its shared refcount to zero.
- [x] (2026-07-29 03:05+08:00) Implemented one immutable statement-wide execution mask and closed every downstream consumer over it: root/coprocessor operator traversal, cop evidence estimation, reader and point transports, DML plan-local request collection, and lookup-Join input accounting. No executor detail, formula unit, weight, protocol field, or resource-control behavior changed.
- [x] (2026-07-29 03:25+08:00) Completed the focused WIP matrix. It covers all three lookup-Join subtypes and both inner-child positions, exact whole-inner subtree masking, Join/outer `HasBytes` sentinels, inner Basic/cop expected/cop observed/ScanDetail/SelectResult/Snapshot blockers, Main/CTE/scalar plan-ID aliases, and preservation of active reader transport.
- [x] (2026-07-29 03:45+08:00) Added a MockStore real-SQL empty-outer `INL_JOIN` regression. `EXPLAIN ANALYZE FORMAT='RU'`, General Log, Prometheus, and statement summary all consume a successful frozen result with Join `cpu_work=0`, `join_output_rows=0`, one executed outer scan, no phantom inner unit/status, and retained outer reader transport.
- [x] (2026-07-29 02:15+08:00) Addressed implementation review round 1. Exact-inner execution evidence now checks Basic byte records plus SelectResult/Snapshot group types for every plan ID, eligible candidates pass exclusive ownership and local runtime proof before pairwise conflict removal, and conflicting candidates are removed symmetrically while unrelated candidates survive. The output regression now fixes the executed outer shape to TableReader/TableFullScan and the skipped inner shape to IndexReader/IndexRangeScan, with presence/absence proof across all four output surfaces and an exact injected read request count of one.
- [x] (2026-07-29 02:40+08:00) Addressed implementation review round 2. Candidate discovery now proves exact-inner preorder closure in one O(n+m) pass before masking; malformed self, skipped, and reversed internal child edges remain visible to the original fail-closed tree analysis. Conflict-domain filtering runs once after local eligibility, and the final ownership gate remains the postcondition. The Prometheus regression uses exact-label before/after deltas on the registered vectors without resetting global metric pointers. Optional private mask arguments now accept only zero or one mask and reject multiple masks through existing fail-closed outcomes.
- [x] (2026-07-29 02:55+08:00) Completed implementation review convergence in four of the allowed five rounds. Rounds 3 and 4 independently approved the final diff with no P0/P1 after the first two rounds' fail-closed, conflict-domain, output-proof, and exact-preorder findings were addressed.
- [x] (2026-07-29 03:04+08:00) Completed the IndexJoin skipped-inner Ready profile. The four focused planner/core and executor failpoint-wrapped targets passed with every wrapper returning to `new_refcount=0`; `make lint`, `gofmt -d`, and `git diff --check` passed. The Bazel gate found no import, top-level-test, file, Bazel, or module trigger, so `make bazel_prepare` was not required.
- [x] (2026-07-29 03:04+08:00) Completed a cleanup-safe real-TiKV verification with the current-worktree TiDB, PD `8cabcf154cce072bb1ed8370a6593783625187c7`, and TiKV `6cdd89619503228b0e85adbf278ba21fce516499`. The empty outer completed while the dynamic inner remained blank in ordinary EXPLAIN; `FORMAT='RU'`, General Log, Prometheus, and statement summary agreed on Join zero work/output plus only the outer TableFullScan/TableReader transport. The exact TiUP tag, temporary TiDB binary, processes, and dedicated ports were removed.
- [x] (2026-08-02 16:10+08:00) Unified read-only and DML PointGet/BatchPointGet request ownership on plan-local `SnapshotRuntimeStats.GetCmdRPCCount`. The focused regression first observed all read-only lookup kinds incorrectly consume an injected statement counter of 99, then passed with exact plan-local `CmdGet=2`, `CmdBatchGet=5`, and mixed total 7.
- [x] (2026-08-02 16:45+08:00) Completed the read-only point-lookup Ready profile. The full preview-RU formula-contract test and the focused EXPLAIN, plan-digest, General Log, Prometheus, and statement-summary tests passed with failpoint refcount returning to zero; `make lint`, `gofmt -d`, and `git diff --check` passed. The Bazel gate found no import, top-level-test, file, Bazel, or module trigger, so `make bazel_prepare` was not required.
- [x] (2026-08-05 13:20+08:00) Confirmed the standalone IndexLookUp zero-handle runtime shape with real TiKV. The IndexLookUp root completed with zero rows, its IndexRangeScan completed one fully covered cop task with zero produced rows and a real zero-byte ScanDetail, and its TableRowIDScan had no runtime evidence because no table task was built. Before the production change, the focused regression reproduced the prior atomic `missing_scan_width_evidence` failure.
- [x] (2026-08-05 13:35+08:00) Added the narrow non-pushdown IndexLookUp table-leg proof to the existing immutable execution mask. The proof requires a completed-zero lookup root, complete zero-row index-root cop summaries, no evidence anywhere in the exact table subtree, exact Build/Probe child identity, and globally unique plan-ID ownership. The regression and post-fix real-TiKV `FORMAT='RU'`/statement-summary checks preserve the real index `scan_bytes=0` and statement reader `net_bytes` while omitting only the unconstructed table subtree; every missing, contradictory, aliased, or malformed case remains fail-closed.
- [x] (2026-08-05 13:50+08:00) A second real-TiKV check established the pushdown boundary: its executed cop root is `LocalIndexLookUp`, which v6 independently rejects as `unsupported_operator`. The short-circuit mask therefore explicitly excludes `IndexLookUpPushDown` instead of manufacturing support from an unrealistic IndexScan-root fixture. The dedicated playground, processes, ports, data, and temporary TiDB binary were removed.
- [x] (2026-08-05 13:59+08:00) Completed the IndexLookUp short-circuit Ready profile. The focused failure-before/pass-after regression, final v6 planner formula pair, and EXPLAIN RU output target passed with every failpoint wrapper returning to refcount zero; `make lint`, `gofmt -d`, and `git diff --check` passed. The Bazel gate found no import, top-level-test, file, Bazel, or module trigger, so `make bazel_prepare` was not required.

## Implementation Review Log

- V5 round 1 (2026-08-03, fresh reviewer): not approved, findings addressed pending rereview.
  - Client-go aggregation could wrap protobuf and accumulated values into plausible non-negative point work. The fork now checked-adds record/Merge work and coverage, propagates a private invalid state, and returns the documented `{-1,-1,-1}` detail sentinel with zero coverage. Tests cover each protobuf conversion, single-stats and Merge overflow, Clone, coverage overflow, nil receiver, and continued unrelated ScanDetail diagnostics.
  - Retry coverage needed deterministic transport evidence. A first epoch-not-match response is now proven excluded from completed responses, while a real prewrite lock followed by cleanup produces two PointGet bodies whose ScanDetailV2 records accumulate.
  - TiDB output coverage needed calibrated mutation plus remote-write partial combinations and a nil promoted snapshot method. Focused regressions now require every DML/COMMIT partial reason to suppress the summary total without suppressing statement-local mutation diagnostics, and require a nil embedded `SnapshotRuntimeStats` to fail closed without panic.

- Round 1/5 (2026-07-29, fresh reviewer): not approved.
  - Finding 1, addressed pending fresh review: Basic `HasBytes` and SelectResult/Snapshot group-type checks now apply to every exact-inner plan ID without invoking an RPC count method. New cop-ID Basic, SelectResult, and Snapshot regressions reject the skip.
  - Finding 2, addressed pending fresh review: local structure, exclusive plan-ID ownership, completed-zero proof, and absence of exact-inner evidence filter candidates first. Pairwise conflict detection then removes both participants for shared involved IDs, non-laminar inner intervals, or a candidate Join/outer occurrence under another candidate's inner; the final ownership gate validates the resulting mask while unrelated survivors remain accepted. Regressions cover two independent legal candidates, one conflict domain beside an independent candidate, symmetric non-laminar removal, and malformed interval failure.
  - Finding 3, addressed pending fresh review: the SQL now forces outer TableReader/TableFullScan and inner IndexReader/IndexRangeScan. EXPLAIN requires exactly one injected read request and only outer plan kinds; General Log and statement summary require outer scan/transport and reject inner kinds; Prometheus verifies exact-label Join/outer increments and zero inner deltas without pre-creating counters or replacing registered vectors.

- Round 2/5 (2026-07-29, fresh reviewer): not approved.
  - Finding 1, addressed pending fresh review (P1): `readBillingDemoFlatSubtreeValid` now validates every exact-inner node and direct edge in O(n+m): bounded monotonic subtree ends, leaf closure, first-child adjacency, strictly increasing child starts, and exact child interval coverage. Self-reference, skipped-child, and reversed-child regressions prove candidate rejection, no mask, and a non-success full-tree result.
  - Finding 2, addressed pending fresh review (P2): the identical second conflict-filter pass and unreachable length guard are removed. One pass follows local eligibility; `readBillingDemoExecutionMaskOwnershipValid` remains the actual post-gate.
  - Finding 3, addressed pending fresh review (P2): the adapter test no longer calls `metrics.InitExplainRUMetrics`. It snapshots exact registered-vector labels before and after the statement, requires statement/Join/outer deltas, and requires zero deltas for the skipped inner scan and reader. The outer reader request supplies a nonzero base-unit delta; the legal zero scan unit remains proven in General Log and statement summary.
  - Finding 4, addressed pending fresh review (P2): the variadic signatures remain to avoid adding `nil` mechanically to dozens of existing private unit-helper calls. A shared parser accepts zero or one mask only; multiple masks cause structural/transport/root-unit failure rather than silently selecting the first, with a focused regression for estimator and transport paths.

- Round 3/5 (2026-07-29, fresh reviewer): approved with no P0/P1.
  - The only P2 documentation issue is addressed: the legacy `Date/Author: 2026-07-22 / Codex.` line now belongs to the original IndexJoin request-de-duplication decision instead of appearing as a second author line under the 2026-07-29 proven-skipped decision. No production code or test changed in this round.

- Round 4/5 (2026-07-29, fresh reviewer): approved with no P0/P1.
  - The final actual diff retained exact-inner evidence and preorder closure, statement-wide ownership, conflict-domain isolation, and mask-consumer closure. The reviewer found no normal-path undercount, malformed fail-open, consumer omission, nil/panic risk, or output-test instability. Round 5 was not needed.

## Surprises & Discoveries

- Observation: local Shuffle has one physical DataSource executor per `PhysicalShuffle.DataSources` entry, while each worker receives the same producer through a reused receiver-stub plan ID.
  Evidence: `pkg/executor/builder.go::buildShuffle` builds `shuffle.dataSources` before the worker loop, allocates one stub per DataSource, then reuses those stubs while building every worker. `pkg/executor/shuffle.go::fetchDataAndSplit` starts one goroutine per DataSource and dispatches each fetched row to one worker.

- Observation: the static flat plan does not duplicate a DataSource merely because it is also referenced by `PhysicalShuffle.DataSources`.
  Evidence: `pkg/planner/core/flat_plan.go::flattenRecursively` has no special DataSource traversal for `PhysicalShuffle`; it reaches the producer once through `PhysicalShuffleReceiverStub.DataSource`. The MergeJoin fixture in `tests/integrationtest/r/executor/merge_join.result` shows one Shuffle, two receivers, and one reader subtree under each receiver.

- Observation: root runtime-stat existence alone cannot distinguish a never-run DataSource from an observed zero, but the preview byte-record sentinel can.
  Evidence: executor construction precreates Basic stats by plan ID; successful `exec.Next` calls record bytes when preview collection is active, including the final empty chunk. `BasicRuntimeStats.HasBytes()` therefore rejects build-only zero state and accepts a completed zero-row producer without adding a Shuffle-specific runtime datum.

- Observation: a physical PointGet can complete without ever constructing a PointGet executor or its Snapshot stats.
  Evidence: partition pruning returns `TableDualExec` for an empty partition before PointGet runtime-stat registration, while the flattened physical plan still contains the PointGet ID. Real TiKV rendered `partition:dual`, no Get/ScanDetail, and completed zero-row Basic stats. This is distinct from a constructed remote point executor whose response omitted typed detail.

- Observation: pessimistic point locking can legitimately leave point Snapshot coverage at zero even when lock RPCs occurred.
  Evidence: PointGet/BatchPointGet lock paths can obtain values through `LockKeys` and the pessimistic lock cache before snapshot Get/BatchGet; real TiKV showed Lock execution but no point scan detail. Since request work is retired, treating the lock activity as missing point storage work would couple the model to an intentionally unweighted RPC.

- Observation: preview construction runs after autocommit commit details are merged, while an explicit-transaction DML is still marked in-transaction.
  Evidence: `session.runStmt` calls `finishStmt` before `ExecStmt.FinishExecuteStmt`; `autoCommitAfterStmt` completes `CommitTxn`, then `FinishExecuteStmt.finalizeStatementRUV2Metrics` freezes CommitDetails before `RecordReadBillingDemoForStatement`. An explicit DML skips `CommitTxn` and retains `SessionVars.InTxn()==true`. This existing timing is sufficient to distinguish autocommit write ownership without adding a cross-layer marker.

- Observation: a pessimistic DML's positive write RPC count is not evidence of committed payload.
  Evidence: pessimistic lock RPCs can populate `ResourceManagerWriteCnt` before final COMMIT, while `WriteKeys` and `WriteSize` are populated from CommitDetails. V6 therefore does not consult the RPC counter and deliberately leaves pessimistic-lock work unmodeled.

- Observation: autocommit EXPLAIN ANALYZE DML has a reporting window after CommitDetails exist but before statement finalization has merged them into the live RUv2 metrics.
  Evidence: the session executes and commits the no-delay DML before the EXPLAIN record set is rendered; `ExplainExec.executeAnalyzeExec` then freezes the context metrics, while `ExecStmt.finalizeStatementRUV2Metrics` runs only when the record set closes. Real TiKV reproduced a nonempty INSERT with a zero write pair before the reporting clone was completed from `StmtCtx.GetExecDetails().CommitDetail`.

- Observation: root executor byte accounting is logical live chunk bytes, not encoded network bytes.
  Evidence: `pkg/util/execdetails/runtime_stats.go` defines `BasicRuntimeStats.inputBytes/outputBytes`, and `pkg/executor/internal/exec/executor.go` records child/output chunk bytes around `Next`.

- Observation: exact TiKV response bytes and chargeable read/write RPC counters exist at statement scope, while PointGet and BatchPointGet additionally retain plan-local RPC counts in their snapshot runtime stats.
  Evidence: `pkg/util/execdetails/ruv2_metrics.go` exposes `TiKVCoprocessorResponseBytes`, `ResourceManagerReadCnt`, and `ResourceManagerWriteCnt`; `pkg/executor/point_get.go` and `pkg/executor/batch_point_get.go` register `SnapshotRuntimeStats` under their plan IDs and query `GetCmdRPCCount` for `CmdGet` and `CmdBatchGet`.

- Observation: `selectResultRuntimeStats` already counts cop responses internally, but proportional or per-reader network-byte attribution is not available from current exec details.
  Evidence: `pkg/distsql/select_result.go` keeps `copRespTime` and `reqStat` inside the unexported `selectResultRuntimeStats`, while response bytes are drained into statement-level `RUV2Metrics`.

- Observation: HashAgg runtime details do not expose a separate group-count counter, but the physical Agg node's own actual output rows are the number of materialized group states for this simple model.
  Evidence: `RuntimeStatsColl` provides own-plan `GetActRows`; the current preview implementation already publishes Agg output-row shadows from this value and verifies TiKV expected/observed task coverage.

- Observation: HashJoin state is row-backed rather than one-entry-per-distinct-key, and the exact admitted row count exists below the exec-details boundary.
  Evidence: `pkg/executor/join/hash_table_v1.go::Len` counts ordinary inserted row pointers, while NAAJ null-key rows are separately retained in `hashNANullBucket.entries`; `pkg/executor/join/join_row_table.go::validKeyCount` counts v2 rows eligible for the hash lookup structure. Current HashJoin runtime stats expose timing/collision data but not the total admitted lookup-state row count.

- Observation: IndexJoin's inner lookup work is executed by dynamically built reader children, whose physical network requests already enter statement-level read RPC details.
  Evidence: `pkg/executor/join/index_lookup_join.go::fetchInnerResults` drains `task.innerExec`, while the reader paths use DistSQL and feed `RUV2Metrics.ResourceManagerReadCnt`; adding the private inner task count as another request term would charge the same lookup twice.

- Observation: an IndexJoin whose completed outer child returns zero rows may never construct or execute its dynamic inner reader, so that physical inner subtree has no runtime-row evidence even though the Join completed successfully.
  Evidence: real TiKV showed `IndexJoin` and its outer `IndexReader` with completed zero-row execution while the inner `IndexReader` and descendants had blank execution info. `pkg/executor/join/index_lookup_join.go::buildTask` returns without producing a lookup task when the outer batch is empty, and `fetchInnerResults` creates the inner executor only for a produced task.

- Observation: a standalone non-pushdown IndexLookUpReader has the same observable absence on its table leg when the completed index phase produces no handles.
  Evidence: `pkg/executor/distsql.go::buildAndDispatchLookupTasks` returns before building a table task when both extracted handles and completed pushdown rows are empty. Real TiKV reported a completed-zero IndexLookUp root and a fully covered zero-row IndexRangeScan while the TableRowIDScan execution-info columns were blank. This is not a zero-byte table scan; it is an unconstructed table task.

- Observation: IndexLookUp pushdown is a distinct unsupported execution shape for the current model, not another instance of the standalone table-leg gap.
  Evidence: real TiKV places `LocalIndexLookUp` at the cop subtree root and reports both its internal IndexRangeScan and TableRowIDScan there. The current supported-operator set rejects that root with `unsupported_operator`; masking the TiDB-side table plan would not make that cop formula supported.

- Observation: root-stat existence and a zero row count do not prove execution, while `BasicRuntimeStats.HasBytes()` distinguishes a successful zero-byte `Next()` from an executor that was only registered.
  Evidence: executor construction obtains zero-valued Basic stats before execution, while `pkg/executor/internal/exec/executor.go` calls `RecordBytes` after each successful `Next()` and `pkg/util/execdetails/runtime_stats.go::HasBytes` reports whether such a byte record exists.

- Observation: the two plan-local RPC-bearing root groups relevant to a dynamic IndexJoin inner are stronger evidence than a zero request count and can be detected without invoking their RPC-count methods.
  Evidence: DistSQL readers register `TpSelectResultRuntimeStats`; PointGet and BatchPointGet register `TpRuntimeStatsWithSnapshot`. A truly unconstructed dynamic inner registers neither group. Treating either type's mere presence as blocking evidence avoids guessing that a constructed producer was free and avoids invoking the snapshot wrapper's promoted method when its embedded pointer is unavailable.

- Observation: `FlatPhysicalPlan` consumers identify individual `FlatOperator` occurrences, but `RuntimeStatsColl` and transport helpers aggregate by plan ID; Main, CTE, scalar-subquery, or shallow-copy trees can therefore make an occurrence-local skip ambiguous.
  Evidence: flat trees retain every occurrence, while `GetBasicRuntimeStats`, cop coverage, root merged groups, and plan-local RPC maps are keyed only by `Origin.ID()`. A masked occurrence and an active occurrence sharing one ID cannot be separated after aggregation.

- Observation: the inherited UnionScan formula test retained a stale diagnostic assertion after production stopped emitting its redundant `input_rows` shadow.
  Evidence: the v5 combined formula sweep failed with expected 4, actual missing while `cpu_work=4` was correct. The regression now asserts the intended absence of the redundant diagnostic and the exact CPU/source contract.

- Observation: inline Projection materializes scalar Sort/TopN `ByItems`, while ordinary column keys need no scalar evaluation; injection is not universal for pushed cop plans.
  Evidence: `pkg/planner/core/rule_inject_extra_projection.go::InjectProjBelowSort` injects only when a `ByItems` expression is a `ScalarFunction`, and root post-optimization does not rewrite every pushed cop plan. V4 therefore assigns expression evaluation exclusively to an actual Projection, assigns one aggregate sorting-complexity term to Sort/TopN regardless of key count, and fails closed if a scalar ordering expression remains unmaterialized.

- Observation: completed IndexHashJoin plans do not retain the ordinary IndexJoin equality representation.
  Evidence: `pkg/planner/core/exhaust_physical_plans.go::completePhysicalIndexJoin` clears `EqualConditions` after deriving `OuterHashKeys` and `InnerHashKeys`; IndexMergeJoin instead exposes executable `CompareFuncs` and optional `OuterCompareFuncs`. Counting only the embedded IndexJoin fields would undercount these subtypes.

- Observation: zero-valued RUv2 getters do not prove that their payload was present, and the read-RPC counter is broader than cop response bytes.
  Evidence: absent/bypassed `RUV2Metrics` reads as zero; `ResourceManagerReadCnt` covers TiKV read RPC producers including unsupported point/ancillary paths, while `TiKVCoprocessorResponseBytes` covers cop responses. Runtime task coverage and a closed producer set are therefore required before zero or statement-wide totals are attributable.

- Observation: write RPC counters are statement-local even inside an explicit transaction.
  Evidence: `session.executeStmtImpl` installs the current statement's `RUV2Metrics`, `ExecStmt.finalizeStatementRUV2Metrics` drains raw/commit details into it before preview construction, and `TestRUV2MetricsIsolatedPerStatementInExplicitTxn` proves successive statements use distinct instances. A pessimistic DML's write requests therefore cannot be deferred to the later COMMIT snapshot.

- Observation: MockStore does not synthesize resource-manager write RPC detail for pessimistic KV traffic.
  Evidence: earlier v4 lifecycle tests injected finalized per-statement `RUV2Metrics` values to prove statement separation. V5 retains the snapshot coverage distinction but treats positive remote DML and every COMMIT as unmodeled partial work rather than emitting a request unit.

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

- Observation: real TiKV can increment `TotalKeys` without producing a processed user record.
  Evidence: an empty or nonmatching range performed one seek and reported `(TotalKeys=1, ProcessedKeys=0, ProcessedKeysSize=0)`; TiKV derives total versions from storage operation counts, while scanners increment processed keys and bytes only after producing a user record. A missing protobuf scalar is decoded to the same zero value as an explicit zero.

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

- Decision: charge TiDB local Shuffle once at `PhysicalShuffle` as `sum_i data_source_rows_i * (1 + partition_expression_count_i)`, using each DataSource plan ID's byte-bearing Basic runtime stats.
  Rationale: the DataSource is where rows enter `fetchDataAndSplit`; Shuffle output and worker-tail rows can reflect Window/Agg/Join cardinality instead. Direct plan-ID ownership avoids multiplying by receiver count or worker concurrency and needs no executor change.
  Date/Author: 2026-08-05 / Codex.

- Decision: classify `PhysicalShuffleReceiverStub` as a transparent wrapper and leave TiFlash Exchange operators under the existing MPP rejection.
  Rationale: receivers are in-process worker input boundaries and carry no independent work formula. The existing flat traversal already reaches their DataSource children, while Exchange operators are distributed network boundaries outside this amendment.
  Date/Author: 2026-08-05 / Codex.

- Decision: accept a missing PointGet/BatchPointGet snapshot group as exact zero only when the same plan ID has completed byte-bearing Basic stats and zero actual rows.
  Rationale: this narrowly recognizes the post-flattening TableDual replacement without weakening typed coverage for constructed executors. Basic stats prove completion only; they supply no key count or byte value. Missing completion evidence and nonzero output remain fail-closed.
  Date/Author: 2026-08-05 / Codex.

- Decision: make locking SELECT and `SelectLock` compatible with v6 while assigning no unit to lock activity.
  Rationale: request counters and weights are globally retired. Lock RPCs therefore must neither add work nor suppress otherwise attributable CPU, scan, network, join, mutation, or committed-write units. Point lookup continues to consume typed storage detail only when such responses exist.
  Date/Author: 2026-08-05 / Codex.

- Decision: keep the unreleased demo on `model_version=v6`, update its uncalibrated weight label, and add independently weighted `frontend_compile_bytes` only when a non-hit has source-text evidence.
  Rationale: one SQL-byte coefficient is the simplest calibration target for combined parser/optimizer work. A cache hit or unavailable text omits the component rather than assigning an arbitrary discount, inventing zero work, or suppressing otherwise valid units and totals. Because no external v6 testing has started, the user explicitly chose to amend v6 instead of creating v7.
  Date/Author: 2026-08-03 / Codex.

- Decision: initially migrate the write-semantic addition to `model_version=v6` and `weight_version=v6-storage-write-work-uncalibrated`, with exactly seven weighted units; this pre-test identity is superseded by the frontend amendment immediately above.
  Rationale: adding committed key and byte terms changes both the formula and output schema. Reusing v5 would silently reinterpret stored rows and calibrated containers.
  Date/Author: 2026-08-03 / Codex.

- Decision: publish autocommit DML write keys/bytes from the frozen statement metrics, omit the remote-write component from non-pipelined explicit DML, and make final COMMIT the sole owner of explicit-transaction units.
  Rationale: this follows the actual commit lifecycle, avoids charging pessimistic locks as final writes, and prevents double counting between DML and COMMIT. COMMIT has no reliable single DML kind.
  Date/Author: 2026-08-03 / Codex.

- Decision: use outward `write_keys` and plural `write_bytes` with `input_source=commit_detail`; accept only `(0,0)` or pairs with both fields positive.
  Rationale: the values are read from the frozen `RUV2Metrics`, but CommitDetails remain their original provenance. Pair validation prevents an omitted or corrupt half from becoming a plausible zero. The plural byte name removes the old semantic alias.
  Date/Author: 2026-08-03 / Codex.

- Decision: complete only the EXPLAIN reporting clone from CommitDetails and leave the live metrics untouched until ordinary statement finalization.
  Rationale: this closes the real autocommit lifecycle gap without changing transaction execution or double-counting process-wide RUv2 counters. Explicit non-pipelined DML still publishes no remote-write component, and a true zero pair remains observable.
  Date/Author: 2026-08-03 / Codex.

- Decision: retire request work globally from preview RU v5 rather than only from PointGet/BatchPointGet.
  Rationale: retaining request weights for readers or writes would keep one unstable proxy in the supposedly storage-shaped model and make identical semantic unit names mean different things by operator. Existing counters remain available outside preview RU for diagnostics.
  Date/Author: 2026-08-03 / Codex.

- Decision: model PointGet/BatchPointGet as `cpu_work=TotalKeys` and `scan_bytes=ProcessedKeysSize`, with `ProcessedKeys` and coverage values diagnostic-only.
  Rationale: TiKV directly reports storage operation count and observed MVCC bytes. Partial misses must not extrapolate hit width to missing keys, and no extra key-count/key-size weight is needed.
  Date/Author: 2026-08-03 / Codex.

- Decision: migrate the destructive semantic-unit change to `model_version=v5` and `weight_version=v5-storage-work-uncalibrated`.
  Rationale: deleting two weighted units and changing point-lookup formulas cannot be published behind the v4 label. Binding the model version into the weight container prevents a calibrated set for another model from producing a mislabeled total.
  Date/Author: 2026-08-03 / Codex.

- Decision: accept a compatible fork backport for the demo and defer minimum-cluster-version negotiation.
  Rationale: the user explicitly scoped this as a demo. The backport keeps TiDB's compatible client-go/PD baseline while supplying typed coverage; official-module integration and backend capability rollout remain release work, not a reason to parse strings or weaken coverage now.
  Date/Author: 2026-08-03 / Codex.

- Decision: leave TiKV DML/COMMIT remote work unmodeled and visible as a partial status instead of retaining write-request billing.
  Rationale: a removed request weight cannot be silently replaced by another proxy. Mutation CPU/raw diagnostics are independently attributable, while the partial remote component prevents a misleading statement total.
  Date/Author: 2026-08-03 / Codex.

The remaining dated decisions document v4 and v5 evolution. Where one conflicts with the current v6 amendment or the decisions above, it is historical and superseded.

- Decision: replace, rather than layer on top of, the v3 fixed/row/byte opclass matrix for new samples.
  Rationale: keeping both billable models would double count and make `cpu_weight` cease to mean one expression-slot evaluation. Historical v3 samples remain queryable under their model version.
  Date/Author: 2026-07-22 / Codex.

- Decision: the historical v4 experiment used seven semantic weights, including two direction-specific request coefficients.
  Rationale: v5 supersedes this decision with the five storage-work weights in the normative amendment.
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

- Decision: the historical v4 point publisher used a plan-local RPC-only term under `point_lookup@statement`.
  Rationale: v5 retains the synthetic ownership and bounded kinds but replaces RPC work with typed `TotalKeys` CPU work and `ProcessedKeysSize` scan bytes.
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

- Decision: recognize an IndexJoin dynamic inner as proven skipped only for `PhysicalIndexJoin`, `PhysicalIndexHashJoin`, or `PhysicalIndexMergeJoin`, and only when the Join and its exact outer direct child each have `BasicRuntimeStats.HasBytes()==true` with zero rows.
  Rationale: these are the three executor families that construct lookup work from outer batches. The byte-record sentinel proves both operators completed rather than merely receiving preallocated zero stats. The original physical child IDs, not the flattened child position, identify inner and outer because `FlattenPhysicalPlan(..., true)` may reorder build and probe children.
  Date/Author: 2026-07-29 / Codex

- Decision: require the exact inner interval to have no Basic byte record, observed or expected cop task, ScanDetail attachment, or plan-local RPC-bearing root group before masking it.
  Rationale: `TpSelectResultRuntimeStats` and `TpRuntimeStatsWithSnapshot` are the actual reader and point-lookup producer groups. Their presence means the producer was at least constructed or registered, so the preview remains fail-closed even if its request count may be zero. The absence proof never calls `GetCmdRPCCount`, adds no executor datum, and cannot panic through a nil promoted snapshot method.
  Date/Author: 2026-07-29 / Codex

- Decision: build one immutable, statement-wide occurrence mask only after every Main, CTE, and scalar-subquery occurrence has passed a global plan-ID ownership gate.
  Rationale: every candidate Join, outer, and inner plan ID must belong to exactly one flat occurrence. Candidates sharing an ID with any active, proof, or other candidate occurrence are rejected; a final ownership inconsistency discards the mask and restores the existing fail-closed path. This lets cop estimation, operator traversal, reader transport, point lookup transport, and DML plan-local request collection consume one closed producer set without hiding active work.
  Date/Author: 2026-07-29 / Codex

- Decision: a proven-skipped inner contributes exact zero input to its owning lookup Join and emits no phantom operator, status, scan, CPU, or transport units.
  Rationale: with an observed empty outer, the Join's expression work and output term are both exactly zero. Actual outer scan and statement transport remain visible, while every missing or contradictory case outside the narrow proof continues to fail closed. General Log, Prometheus, statement summary, and `EXPLAIN ANALYZE FORMAT='RU'` continue to consume the same frozen result.
  Date/Author: 2026-07-29 / Codex

- Decision: recognize the table subtree of a non-pushdown `PhysicalIndexLookUpReader` as proven skipped only when the lookup root has completed byte-bearing zero-row Basic stats, the exact index cop root has complete expected/observed task coverage with zero produced rows, and the exact table subtree has no Basic byte, cop task, ScanDetail, SelectResult, or Snapshot evidence.
  Rationale: these conditions match the executor's zero-handle early return without interpreting absent table statistics as an executed zero-byte scan. Exact Build/Probe identity and globally unique ownership for the lookup, index root, and every table node prevent plan-ID aggregation from borrowing another occurrence's evidence. Only the table subtree is masked, so the real index scan and statement transport remain billable. Pushdown IndexLookUp remains outside this decision.
  Date/Author: 2026-08-05 / Codex

- Decision: keep calibrated-weight injection private to `pkg/planner/core` formula tests; package-external end-to-end tests exercise the production-default uncalibrated contract.
  Rationale: `pkg/executor/explain_test.go` is `package executor_test`, while the preview weight container intentionally remains private and has no session/global knob. A public or failpoint configuration surface solely for tests would weaken the initial contract.
  Date/Author: 2026-07-22 / Codex.

- Decision: freeze the three legacy statement-summary convenience totals as v3-only and leave them zero for v4 samples.
  Rationale: `fixed_events`, `input_rows`, and `input_bytes` cannot safely represent the new semantic units. V4 consumers must use the versioned detail table; adding a parallel convenience schema and its v1/v2 persistence migration is outside this minimal model change.
  Date/Author: 2026-07-22 / Codex.

- Decision: v4 historically shipped with its own model and uncalibrated weight labels.
  Rationale: those rows remain self-describing; v5 uses a new immutable pair and does not relabel history.
  Date/Author: 2026-07-22 / Codex.

- Decision: combine write mutation count and bytes into `cpu_work = mutation_count + mutation_bytes / mutation_bytes_per_cpu_unit`, then apply `cpu_weight` once. Distinguish mutation CPU from expression CPU through the operator dimensions rather than a separate unit name.
  Rationale: normalization produces expression-equivalent CPU work, so reusing the shared semantic unit preserves one CPU coefficient. `mutation_bytes_per_cpu_unit` is a versioned normalization constant, not an RU coefficient; the complete weight snapshot must be calibrated and valid before mutation-derived `cpu_work` is emitted. Raw mutation count/byte diagnostics remain available without calibration.
  Date/Author: 2026-07-22 / Codex.

- Decision: charge write requests to the statement whose frozen `RUV2Metrics` snapshot contains them; an explicit-transaction DML and the eventual COMMIT each own only their respective snapshots.
  Rationale: TiDB installs a fresh `RUV2Metrics` object for every statement and finalizes raw RU details into that statement before preview construction. Pessimistic DML can issue write RPCs before COMMIT, so deferring all request work to COMMIT would permanently omit those DML-local requests. A cross-statement accumulator is unnecessary and would weaken the existing statement-local contract.
  Date/Author: 2026-07-22 / Codex.

- Decision: v4 historically exposed direction-specific request units and coefficients.
  Rationale: v5 globally retires that experiment while leaving the underlying diagnostic counters and production RUv2 unchanged.
  Date/Author: 2026-07-22 / Codex.

- Decision: do not guess numerical v4 weights or silently map the heterogeneous v3 opclass weights.
  Rationale: there is no evidence-backed one-to-one mapping. The implementation first publishes v4 base units with `uncalibrated_weights` and no total until an explicit v4 weight set and positive mutation normalization are installed. The preview flag is off by default, so this is safer than presenting arbitrary numbers as RU.
  Date/Author: 2026-07-22 / Codex.

- Decision: validate Sort/TopN ordering columns against the direct executed flat child schema, and additionally require a Projection child's schema width to match its expression list.
  Rationale: this proves the ownership handoff to the child Projection without charging expression CPU at the ordering node, while preserving ordinary column and constant ordering. Relying only on optimizer invariants would violate the explicit fail-closed contract for missing or misaligned Projection schema.
  Date/Author: 2026-07-22 / Codex.

- Decision: make the private weight container own its immutable version and reject calibration under the shipped uncalibrated label.
  Rationale: coefficients and their version are one atomic contract. Deriving all output labels from the active container prevents a future calibrated set from publishing a total under stale metadata. Historically the v4 production default was `v3-resource-formula-uncalibrated` and the v5 default was `v5-storage-work-uncalibrated`; no uncalibrated label can publish totals.
  Date/Author: 2026-07-22 / Codex.

- Decision: add the active weight version to every background preview Prometheus family, not only to base-unit samples.
  Rationale: keeping statement status, operator status, units, and row-width observations on the same bounded version key makes the Prometheus output an atomic contract and prevents partial cross-version aggregation. The version is immutable per binary configuration, so the extra label remains bounded.
  Date/Author: 2026-07-22 / Codex.

- Decision: record TiDB-internal ScanDetail attachment counts and use them, rather than detail values, to identify the unique component holder.
  Rationale: `RecordCopStats(scan != nil)` already knows whether a response attached ScanDetail, including a legitimate all-zero detail, while `RecordOneCopTask` is summary-only. Counting attachments under the original plan ID preserves the existing distsql/EXPLAIN ownership contract, distinguishes empty from absent evidence, and makes multiple holders fail closed without relying on flat-tree traversal order. This is internal provenance metadata, not a new TiKV protocol field or formula input.
  Date/Author: 2026-07-23 / Codex.

- Decision: validate scan executor summaries and ScanDetail attachments as independent coverage channels.
  Rationale: the scan leaf must have `observed summaries == expected responses > 0`, while the unique holder must have `attachment records == holder expected == scan expected`. The holder's own execution summary is not required for scan bytes because `RecordCopStats` can receive complete ScanDetail with a nil summary. Only after both channels are complete may a zero-processed tuple mean an observed empty scan.
  Date/Author: 2026-07-23 / Codex.

- Decision: treat a completely observed `(TotalKeys>=0, ProcessedKeys=0, ProcessedKeysSize=0)` tuple as zero scan bytes.
  Rationale: `TotalKeys` includes seeks and MVCC operations rather than only produced user records, so it may be positive for an empty result. Without a processed record there is no observed width to multiply by `TotalKeys`; estimating one would invent data. Publishing `scan_bytes=0` preserves the simple byte formula, while independent reader request and network units retain transport cost. A count/size mismatch remains invalid, and the existing attachment plus task-coverage gates still distinguish an observed zero from missing evidence.
  Date/Author: 2026-07-28 / Codex.

- Decision: keep the broader non-Scan direct-child expected-task hardening outside this focused ownership correction.
  Rationale: the existing relative `maxSummaryTasks` gate can miss the case where every executor omits the same response summary, but changing every unary/Agg/DML path is not required to fix the confirmed double-holder failure and needs its own calibrated-release regression matrix.
  Date/Author: 2026-07-23 / Codex.

## Outcomes & Retrospective

The final pre-test v6 amendment adds one independently weighted SQL-byte proxy for combined parser and optimizer work on plan-cache non-hits. Real cache-hit lifecycle coverage proves that the first prepared execution publishes exactly one template-byte sample and the hit publishes no frontend component. A subsequent contract correction made missing text another component-level omission: it neither invents zero work nor suppresses otherwise valid execution units and totals. Three fresh implementation-review rounds converged on the eight-unit contract and four-output implementation before this narrower omission correction. This remains an intentionally simple calibration proxy: cache lookup, non-prepared cache-hit parsing, and unavailable source text are undercounted, and SQL length is not claimed to be causal CPU work.

The v6 revision adds committed key and byte work without reviving request billing. Autocommit DML and final COMMIT have disjoint ownership; explicit DML and pessimistic-lock RPCs cannot produce remote write units, and pipelined payloads remain partial. The uncalibrated production snapshot exposes valid raw write units on every output surface while withholding weights, derived mutation CPU, and totals. A real-TiKV review loop caught and fixed the autocommit EXPLAIN pre-finalization snapshot gap without mutating live metrics or double-counting RUv2 Prometheus counters. Final Ready and cleanup-safe real-TiKV evidence is recorded in `Progress`; the remaining deliberate undercount is pessimistic lock-RPC work, which has no reliable key/byte payload in this model and does not fail the statement closed.

The v5 revision removes request counters from the preview formula, binds five semantic weights to a new immutable model/version pair, and replaces RPC-only point lookup billing with typed TiKV storage work. It keeps raw point and mutation diagnostics, preserves strict coverage semantics, and deliberately reports remote DML/COMMIT work as partial rather than inventing a replacement term. The demo uses a baseline-compatible fork backport; official client-go integration and minimum-version rollout are deferred. Positive renderer/aggregation tests cover all four output surfaces, while MockStore's missing ScanDetailV2 is retained as a negative fail-closed regression. Final real-TiKV verification reproduced the hit, miss, partial-hit, unique-index, reader, and pessimistic-transaction contracts on all required output surfaces, and the Ready profile passed from the reviewed final code state.

The 2026-08-05 v6 refinement closes two zero-work gaps without adding a unit or weight. A partition-pruned PointGet that is replaced locally now uses completed zero-row Basic stats as proof that its absent Snapshot group contributes zero; every constructed point executor still requires typed coverage. Locking SELECT is accepted, `SelectLock` remains a non-billable wrapper, and lock RPC work is ignored rather than converted into a request or storage term. These exceptions preserve attributable units from the rest of the statement and keep missing or incomplete storage detail fail-closed.

The following paragraph is the retained v4 retrospective and is superseded for request units, weights, point formulas, and version labels.

The v4 work established the shared CPU/scan/network/hash/join formula family, exact Sort/TopN ownership, mutation diagnostics, ScanDetail attachment provenance, and proven-skipped IndexJoin handling. Its later direction-specific request and RPC-only point experiments are retained only as historical evidence and are replaced by the v5 storage-work amendment. The non-request formulas and four-output frozen-result architecture remain applicable.

## Context and Orientation

Preview RU is an observational model. It emits coefficient-free work units and optionally multiplies them by preview-only weights. It must not modify the RU charged by TiKV, the RUv2 total reported to resource control, or scheduling decisions.

The current constructor and renderer live in `pkg/planner/core/explain_ru.go`. `buildReadBillingDemoResult` freezes one statement result, `readBillingDemoRootUnits` derives TiDB root units, `readBillingDemoCopUnits` derives TiKV cop units, and `readBillingDemoUnitPreviewRU` applies weights. The same result feeds `EXPLAIN ANALYZE FORMAT='RU'`, metrics, statement summary, and logging. The later implementation must keep one frozen result as the sole source for all outputs.

`RuntimeStatsColl` in `pkg/util/execdetails/runtime_stats.go` maps physical plan IDs to root and cop runtime details. Root `BasicRuntimeStats` gives actual output rows and logical chunk bytes. Cop `CopRuntimeStats` gives executor-produced rows, task count, and `ScanDetail`. `RUV2Metrics` in `pkg/util/execdetails/ruv2_metrics.go` gives statement-level TiKV response bytes and read/write RPC counts. Physical plan structs in `pkg/planner/core/operator/physicalop` give expression lists; these are static plan metadata, not new runtime observations.

In this document, a work unit is a coefficient-free non-negative finite number. A weight converts one work unit into preview RU. A physical operator is one node in the executed physical plan. An expression slot is one top-level expression/function/key comparison that the physical operator evaluates per input row. `cpu_weight` prices one expression-equivalent CPU-work unit; expression evaluation, ordering comparisons, Limit row handling, and normalized mutation preparation can all produce such units. A missing value differs from an observed zero; missing required evidence fails closed, while observed zero remains billable as zero.

## Historical v4 formula contract (superseded by the v5 amendment)

This section is retained as implementation history. Its request-bearing formulas and v4 version statements are not current requirements; the v6 amendment at the top of this document is normative.

For one statement, weighted preview RU is the sum of the following unit families:

    preview_ru =
        cpu_work         * cpu_weight
      + scan_bytes       * scan_weight
      + net_bytes        * net_weight
      + hash_state_rows  * hash_table_weight
      + join_output_rows * join_weight

All arithmetic is float64 with explicit negative, NaN, infinity, and overflow rejection. Integer counters are converted only after validating that they are non-negative. Each pre-aggregation operator result retains `site`, `op_class`, `operator_kind`, `operator_id`, `source`, and, for joins, `input_side`. Physical results use the executed plan's Explain ID. Statement-scoped synthetic results use reserved, non-plan IDs: `reader_transport@statement`, `point_lookup@statement`, `mutation@statement`, and `txn_write@statement`. Statement-summary detail intentionally aggregates away `operator_id`; its remaining bounded dimensions and version still preserve formula provenance.

There is no billable fixed-event term. Remote fanout is not assigned a v5 setup proxy; only attributable storage work and network bytes enter the formula.

### Operator formulas

| Operator | v4 formula | Required inputs |
|---|---|---|
| Selection | `rows * n_expr * cpu_weight` | direct child actual rows; selection expression slots |
| Projection | `rows * n_expr * cpu_weight` | direct child actual rows; projected expression slots |
| Sort | `rows * log2(max(rows,2)) * cpu_weight` | direct child actual rows; scalar expression evaluation belongs to inline Projection |
| TopN | `count == 0 ? 0 : rows * log2(max(min(rows,k),2)) * cpu_weight` | direct child actual rows; checked `k=offset+count` for positive count; scalar expression evaluation belongs to inline Projection |
| TableScan / IndexScan | `scan_bytes * scan_weight` | attributable TiKV `ScanDetail` |
| TableReader / IndexReader / IndexLookup / IndexMerge transport | `net_bytes * net_weight` | statement `RUV2Metrics`, emitted once |
| PointGet / BatchPointGet | `TotalKeys * cpu_weight + ProcessedKeysSize * scan_weight` | typed plan-local `SnapshotRuntimeStats`, aggregated once under the synthetic statement operator |
| UnionScan | `rows * cpu_weight` | direct child actual rows; no expression-count multiplier |
| StreamAgg | `rows * n_expr * cpu_weight` | direct child actual rows; group and aggregate slots |
| HashAgg | `rows * n_expr * cpu_weight + group_rows * hash_table_weight` | StreamAgg inputs plus own actual output rows |
| MergeJoin | `(left_rows + right_rows) * n_expr * cpu_weight + output_rows * join_weight` | both child rows, join slots, own output rows |
| HashJoin | `(left_rows + right_rows) * n_expr * cpu_weight + hash_state_rows * hash_table_weight + output_rows * join_weight` | both child rows, join slots, one Join runtime state count, own output rows |
| IndexJoin family | `(left_rows + right_rows) * n_expr * cpu_weight + output_rows * join_weight` | both child rows, join slots, own output rows; inner reader already owns requests |
| Limit | `rows * cpu_weight` | direct child actual rows |
| Window | `rows * n_expr * cpu_weight` | direct child rows and the refined Window slot count below |
| Write mutation | `cpu_work * cpu_weight` when calibrated | existing mutation recorder; mutation provenance is carried by operator dimensions; remote DML/COMMIT work remains partial |

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

    if ProcessedKeys == 0 and ProcessedKeysSize == 0:
        scan_bytes = 0
    otherwise:
        scan_bytes = TotalKeys * ProcessedKeysSize / ProcessedKeys

The zero-processed case is an observed zero only when `TotalKeys` is non-negative, the scan leaf has `observed execution summaries == expected responses > 0`, and the unique ScanDetail holder has `attachment records == holder expected == scan expected`. TiKV can increment `TotalKeys` for a seek that produces no user record, while an omitted protobuf `ProcessedKeys` field is decoded as zero; therefore `(TotalKeys > 0, ProcessedKeys == 0, ProcessedKeysSize == 0)` is a valid zero-byte scan rather than missing width evidence. An attachment record is one `RecordCopStats` call with a non-nil ScanDetail under the original plan ID; a summary-only `RecordOneCopTask` is not an attachment. The holder's own execution summary is independent and is not required for scan bytes. Otherwise the value is missing or incomplete. When processed keys are nonzero, all three values must be positive and the result must be finite. A zero/nonzero mismatch between `ProcessedKeys` and `ProcessedKeysSize` remains invalid. This proxy preserves scan work for MVCC/skipped keys while using only current `ScanDetail`; it is labeled `scan_detail_processed_key_avg_estimate`, not presented as encoded response bytes.

Each scan detail must be attributable to exactly one scan component. Multi-scan IndexMerge is supported by evaluating each partial scan component separately; it must not share one detail across siblings. Ambiguous or absent attribution fails the affected statement according to the atomicity rules below.

### Reader transport

The four named reader families share one statement-level transport formula. The constructor identifies all executed TableReader, IndexReader, IndexLookup, and IndexMerge nodes, then emits exactly one `id=reader_transport@statement`, `site=tidb`, `op_class=reader_transport` operator with a bounded `operator_kind` set: `table_reader`, `index_reader`, `index_lookup`, `index_merge`, or `mixed_reader`.

`net_bytes` is `RUV2Metrics.TiKVCoprocessorResponseBytes()` from the frozen statement details and uses `input_source=ruv2_metrics`. `ResourceManagerReadCnt()` may remain a non-billable coverage sentinel, but it is never rendered or weighted. Logical `BasicRuntimeStats.GetOutputBytes` remains a diagnostic and is never substituted for transport bytes.

The statement-wide counters are publishable only when the executed flat plan proves a closed producer set: every possible TiKV read-RPC producer for the statement belongs to those four supported cop-reader families. The initial implementation uses this exact algorithm:

1. Only a read-only `SELECT` can pass the closed-set gate. If a DML flat plan contains any supported reader, its reader-transport component is always `unknown_input/ambiguous_reader_transport_producers`; v4 has no DML allowlist because current details cannot exclude uniqueness, locking, FK, transaction, or other ancillary reads.
2. Walk the complete executed `FlatPhysicalPlan`, including IndexJoin inner plans. Classify `*physicalop.PhysicalTableReader` with `StoreType == kv.TiKV`, `*physicalop.PhysicalIndexReader`, `*physicalop.PhysicalIndexLookUpReader`, and `*physicalop.PhysicalIndexMergeReader` as supported producers.
3. Reject any `*physicalop.PointGetPlan` or `*physicalop.BatchPointGetPlan` from the cop-reader transport component; a separate point-lookup component below may publish it only when the complete statement contains point lookup producers and no cop reader or other open read producer. Also reject any `PhysicalTableReader` whose `StoreType != kv.TiKV`, `*physicalop.PhysicalExchangeReceiver`, and `*physicalop.PhysicalExchangeSender`. Any node that the existing preview classifier marks as a reader/store-access class but that is not one of the four supported cop-reader types also opens the set. This catches TiFlash/MPP and future external reader types without treating a new producer as free.
4. Other already-supported CPU, join, aggregation, wrapper, scan-descendant, UnionScan, MemTable, and TableDual nodes are not independent TiKV read-RPC producers and do not open the set. Any structurally unknown plan node continues to fail through the existing unsupported-operator gate, so transport is never published alongside an unknown tree.

A supported reader mixed with any rejected producer is not partially charged from the total counter: SELECT fails atomically; DML marks only its reader-transport component unknown. This is conservative because the current statement counter cannot subtract unsupported RPCs.

Presence and zero handling are normative:

- A nil, bypassed, or otherwise unavailable RUv2 snapshot is missing, even though public getters return zero.
- Inspect `GetTasks()` and `GetExpectedCopTasks()` for every supported reader/cop descendant. If any observed or expected cop task exists, a zero-byte snapshot with no diagnostic read activity is missing rather than free.
- Nonzero network bytes are attributable even when the diagnostic read counter is zero; an empty cop response may have diagnostic read activity and zero network bytes.
- Both network bytes and diagnostic read activity may be observed zero only when a present, non-bypassed frozen RUv2 snapshot exists, no supported descendant has an observed or expected cop task, every supported reader root has observed zero output rows, and the producer set is closed. This represents an empty range/no-request execution.

If no supported reader executed, no reader-transport operator is emitted. Unsupported producers retain explicit bounded status rows until an attributable mapping is designed. The bounded transport reasons are `missing_reader_transport_details` for presence/coverage failure and `ambiguous_reader_transport_producers` for an open producer set.

### Point lookup storage work

PointGet and BatchPointGet plans emit typed storage work once as `id=point_lookup@statement`, `site=tikv`, `op_class=kv_point_lookup`, `input_side=all`. Its bounded `operator_kind` is `point_get`, `batch_point_get`, or `mixed_point_lookup`; the physical plan rows remain non-billable diagnostics. The complete current formula and coverage matrix are in the normative v6 amendment.

For both read-only statements and DML, every constructed PointGet/BatchPointGet executor must have typed snapshot runtime stats and `input_source=snapshot_runtime_stats`. It accumulates `TotalKeys`, `ProcessedKeys`, `ProcessedKeysSize`, detail records, and completed responses with checked arithmetic. It emits `cpu_work` and `scan_bytes` plus the five raw diagnostics only after the coverage contract succeeds. The only absent-group exception is a plan ID with completed byte-bearing Basic stats and exactly zero actual rows, which proves the physical PointGet was locally replaced by a zero-row executor and contributes exact zero. Basic stats are never a key-work or byte-work source.

For a read-only statement, the point-lookup producer set contains one or more PointGet/BatchPointGet nodes, including locking variants. Cop readers remain independently attributable to reader network transport and do not make typed point storage work ambiguous. DML likewise consumes only its plan-local point details; uniqueness, foreign-key, lock, or transaction activity is not assigned to the point operator. `SelectLock` is a supported non-billable wrapper, and its RPCs, waits, retries, and lock-cache payloads do not affect point coverage or add units.

### IndexJoin request de-duplication

IndexJoin, IndexHashJoin, and IndexMergeJoin do not emit a Join-local transport term. Their dynamic inner executors use TableReader, IndexReader, or IndexLookup paths, so attributable response bytes are already present in statement reader transport. PhysicalIndexMergeReader remains a supported standalone producer, but `dataReaderBuilder.BuildExecutorForIndexJoin` does not construct it as an IndexJoin inner path.

This is the explicit de-duplication refinement to the initial simple IndexJoin formula:

    initial: IndexJoin CPU + lookup requests + output rows
    v4:      IndexJoin CPU + output rows
             reader_transport already charges all inner physical requests once

The existing private inner task counter remains an EXPLAIN timing diagnostic and is not converted into request RU. IndexJoin adds no detail; the only v4 runtime extension is HashJoin's state-row getter above.

### IndexLookUpReader table-leg short circuit

For a standalone non-pushdown `PhysicalIndexLookUpReader`, the index phase may complete without producing any handles. In that case the executor does not construct or dispatch a table lookup task. The table cop subtree therefore has no runtime rows or scan detail and must not make otherwise valid index scan and reader-transport units disappear.

The statement-wide execution mask may omit that exact table subtree only when all of the following hold:

- the root IndexLookUp occurrence is a non-pushdown `PhysicalIndexLookUpReader` whose Basic runtime stats have a byte record and exactly zero output rows;
- its original IndexPlan and TablePlan IDs map exactly to the flat Build and Probe children, and both flattened child intervals are structurally valid;
- the index cop root has positive expected task coverage, matching observed summary coverage, and exactly zero produced rows;
- every node in the exact table subtree has no Basic byte record, expected or observed cop task, ScanDetail attachment, SelectResult group, or Snapshot group; and
- the lookup root, index root, and every table-subtree plan ID each own exactly one occurrence across Main, CTE, and scalar-subquery trees.

Only the table subtree is masked. The completed index subtree still emits its actual CPU/scan units, and statement reader transport still emits attributable `net_bytes`. Missing byte sentinels, incomplete index summary coverage, nonzero index output, any table-side evidence, malformed flat intervals, or plan-ID aliases retain the existing fail-closed behavior. `IndexLookUpPushDown` is excluded because its cop root is `LocalIndexLookUp`, which is not a supported v6 operator; this short-circuit rule does not extend that producer set.

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

The externally weight-bearing semantic units are fixed to `cpu_work`, `scan_bytes`, `net_bytes`, `hash_state_rows`, and `join_output_rows`. Mutation-derived `cpu_work` uses `site=tidb`, `op_class=kv_mutation`, `operator_kind=memdb_mutation`, `input_source=stmt_memdb_mutation_calls`, and `input_side=all`; consumers must use these dimensions to distinguish it from expression CPU work.

`RUV2Metrics.ResourceManagerWriteCnt()` remains coverage evidence only. A present, non-bypassed zero DML snapshot proves no unmodeled remote write work; positive activity makes the DML remote component partial, and absent/bypassed/negative coverage is missing. Every COMMIT is partial because this model has no remote commit term. SQL `ROLLBACK` remains unsupported and emits neither a zero unit nor a total.

Pipelined transactions retain valid mutation diagnostics but mark remote write work `pipelined_tikv_write_work_unmodeled`. Optimistic retry/replay keeps the mutation behavior above and marks unavailable remote attribution partial. No retry, pipeline, or batch path publishes a known-incomplete total.

Mutation count and bytes continue to be emitted as zero-weight diagnostics so calibration can change `mutation_bytes_per_cpu_unit` offline. `CommitDetails.WriteKeys/WriteSize` are not substituted for the TiDB mutation unit.

## Availability, atomicity, and degraded behavior

The existing preview gates remain: the feature is default off; `EXPLAIN ANALYZE FORMAT='RU'` explicitly enables collection; unsupported mutating-expression, output-side-effect, and internal paths are rejected; production resource control is untouched. Locking SELECT is the explicit exception: it is allowed, while lock work itself is outside the formula. `*ast.RollbackStmt` is explicitly routed to `unsupported/unsupported_statement` before the ordinary SELECT gate, so it cannot be mistaken for a missing-plan SELECT or an observed-zero write.

For supported SELECT, billing is statement-atomic. If any executed supported operator lacks a required input, the statement records status and reason but emits no billable v4 units and no `total_preview_ru`. This prevents a partial plan from looking cheap. Diagnostic status rows may still identify every missing operator.

For DML, read-tree, mutation, and remote-write coverage components keep independent status. COMMIT has its own remote-work component because explicit transactions separate statement lifetimes. Complete components may retain coefficient-free units for calibration, but a statement-level weighted total is absent unless every component expected at that lifecycle point is complete and the weight set is calibrated.

Observed zero is accepted only with presence evidence: an existing root stat, a cop stat with complete task coverage, a HashJoin state-row runtime stat, a mutation recorder snapshot, or a frozen RUv2 snapshot plus the reader consistency checks above. Missing, negative, overflowed, NaN, and infinite inputs have bounded reasons. New reasons are `missing_expression_count`, `missing_ordering_projection`, `invalid_topn_bound`, `missing_reader_transport_details`, `ambiguous_reader_transport_producers`, `missing_hash_state_rows`, `invalid_hash_state_rows`, and `uncalibrated_weights`.

No fallback may use optimizer estimated rows, schema-estimated widths, plan `netDataSize`, string parsing of `EXPLAIN` runtime text, or proportional allocation of statement counters.

## Weight units and migration

The v4 weight container is preview-only:

    type previewRUWeights struct {
        Version                 string
        CPUPerWorkUnit          float64
        ScanPerByte             float64
        NetworkPerByte          float64
        HashTablePerRow         float64
        JoinPerOutputRow        float64
        MutationBytesPerCPUUnit float64
        Calibrated              bool
    }

The five RU fields have units stated by their names. `MutationBytesPerCPUUnit` is a normalization, not RU. Validation additionally binds the container to model v5 and requires a nonempty calibrated weight version, finite non-negative RU weights, a positive finite mutation normalization, and `Calibrated=true` before any weighted total is published. Formula tests in `pkg/planner/core` use the private container directly with small deterministic values. No exported setter, session/global variable, or failpoint is added. Package-external executor tests see the production default and assert `uncalibrated_weights`, coefficient-free units, and no `total_preview_ru` until a later calibration change supplies production values.

Set the exact constants `model_version='v5'` and `weight_version='v5-storage-work-uncalibrated'`. Do not reuse earlier labels. The weight-version string intentionally describes the shipped state; a later calibrated set must use another immutable version rather than changing values behind this label. Existing statement-summary detail already carries model and weight versions, so old rows remain self-describing and are not rewritten. Queries that compare workload windows must group by both versions.

The existing `ReadBillingDemoBaseUnitSummary` and its infoschema convenience columns (`fixed_events`, `input_rows`, and `input_bytes`) are frozen legacy-v3 views. V4 samples contribute zero to all three, and v4 consumers use the versioned base-unit detail rows instead. Do not reinterpret an old column as `cpu_work`, and do not merge v3 and v4 in those totals. This avoids a cross-version semantic lie and avoids expanding the v1/v2 statement-summary persistence schema in this milestone. Tests must cover v3 legacy aggregation unchanged, v4 legacy totals zero, and v4 detail surviving memory/history readers.

There is no migration of `config.RUV2`, TiKV client RU coefficients, or resource-group settings. Those configure production RUv2 and have different semantics. Delete the current internal `readBillingDemoWeights` map, `readBillingDemoResolveWeights`, and v3 formula application in the same atomic implementation change. `readBillingDemoResult` is constructed and rendered within one statement in one process; no frozen result crosses a process-upgrade boundary, while historical statement-summary rows already store their unit values and versions and are never recomputed from this map. Therefore no v3 calculation compatibility branch is needed or permitted.

All outputs must switch together: EXPLAIN unit rows, `total_preview_ru`, Prometheus base units, statement-summary detail, and general log. A mixed state where logs use v4 while EXPLAIN uses v3 is not accepted. General-log aggregation must extend its key and serialized object to retain `DMLKind`, `InputSource`, and `InputSide`; otherwise distinct v4 units with the same operator/unit label would collapse and lose their provenance. `operator_id` remains an internal/EXPLAIN identity and is intentionally absent from the bounded statement-summary and general-log aggregation keys.

## Plan of Work

### Milestone 4: add TiDB local Shuffle work without changing the model

In `pkg/planner/core/explain_ru.go`, add the bounded Shuffle op class, hash/range kinds, and DataSource-row input source. Classify only root `PhysicalShuffle` as billable and `PhysicalShuffleReceiverStub` as a non-billable wrapper; preserve the existing Exchange rejection. Before the generic root unary path reads an operator's own rows, construct one aggregate Shuffle `cpu_work` unit directly from `PhysicalShuffle.DataSources` and `ByItemArrays`. Require structural alignment, positive unique DataSource IDs, globally exclusive flattened-plan ownership plus one current-tree occurrence per ID, byte-bearing root stats, non-negative rows, and checked multiplication/addition. Do not emit output shadows or any non-CPU Shuffle unit.

Extend the existing preview RU tests rather than adding a top-level suite. Private planner tests prove exact one-key, composite-key, two-source, zero-side, missing-evidence, alias, shape, and overflow outcomes. Executor tests use actual local Shuffle plans to prove FORMAT='RU', concurrency invariance, receiver transparency, descendant preservation, absent forbidden units, unchanged v6 labels, and absent uncalibrated totals. No new top-level test or case-map entry is needed. No executor, runtime-stat, public API, weight, normalization, or Bazel target change is expected.

Acceptance: a single-input `n=3,k=1` Shuffle emits one `cpu_work=6`; `n=3,k=2` emits 9; a two-input `(2,5)` one-key Shuffle emits 14 regardless of Shuffle output rows or concurrency; an explicitly completed zero side contributes zero while the other side remains visible. Any missing or ambiguous DataSource evidence fails closed, receivers emit no units, descendants remain independently collected, and TiFlash Exchange stays unsupported.

### Milestone 1: represent v4 work without changing collection

In `pkg/planner/core/explain_ru.go`, keep the five bounded weight-bearing unit names (`cpu_work`, `scan_bytes`, `net_bytes`, `hash_state_rows`, and `join_output_rows`), the validated model-bound weight container, and one formula application function. Keep expression and raw point/mutation values diagnostic and zero-weight.

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
| point `cpu_work` | typed plan-local `SnapshotRuntimeStats.TotalKeys` | coverage-complete sum across PointGet/BatchPointGet plan IDs | no TiDB field; fork client-go API |
| point `scan_bytes` | typed plan-local `SnapshotRuntimeStats.ProcessedKeysSize` | coverage-complete sum; no partial-miss width extrapolation | no TiDB field; fork client-go API |
| point diagnostics | typed `ProcessedKeys`, detail records, completed responses | checked sum; present-zero and missing-detail remain distinguishable | no TiDB field; fork client-go API |
| HashAgg `group_rows` | Agg node own runtime rows | TiKV additionally needs expected/observed coverage | no |
| HashJoin `hash_state_rows` | v1 hash-table `Len` plus NAAJ null-bucket entries; v2 row-table `validKeyCount` | completed build round, cumulative across rebuilds | **yes, Join only** |
| Join `output_rows` | Join node own `BasicRuntimeStats.GetActRows` | executed root stat required | no |
| local Shuffle `cpu_work` | each `PhysicalShuffle.DataSources[i]` Basic rows plus `len(ByItemArrays[i])` | equal nonempty arrays, unique/owned plan IDs, byte-bearing Basic evidence, checked `sum_i n_i*(1+k_i)` | no |
| `mutation_count/bytes` | `StatementContext` preview mutation recorder | current attempted-call semantics | no |
| remote-write coverage | current statement's `RUV2Metrics.ResourceManagerWriteCnt` | diagnostic sentinel only; zero DML is complete, positive/missing DML and every COMMIT are partial | no |

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

End-to-end `EXPLAIN ANALYZE FORMAT='RU'` cases cover Selection/Projection, Sort/TopN, Table/Index scans, each reader family including IndexMerge, typed PointGet/BatchPointGet storage work, UnionScan, Stream/HashAgg, Merge/Hash/IndexJoin, Limit, Window, autocommit write, explicit DML plus COMMIT, unsupported ROLLBACK, and zero-mutation/zero-row cases. DML/COMMIT cases prove the v5 remote-work partial reasons without emitting request base units. Each attributable case exposes coefficient-free units, source, and model/weight versions. Because package-external tests use uncalibrated production defaults, they assert absent `total_preview_ru`; exact totals belong to private core formula tests.

Local Shuffle coverage additionally proves single and composite partition-key formulas, two differently sized MergeJoin inputs whose Join output differs from their sum, one explicitly empty side, and concurrency invariance. FORMAT='RU' and the statement-summary detail use `tidb/shuffle/{hash_shuffle,range_shuffle}`, `cpu_work`, `shuffle_data_source_act_rows`, and `all`; no Shuffle row appears for `net_bytes`, `scan_bytes`, `hash_state_rows`, or `join_output_rows`. Receiver status is non-billable, descendants remain visible once, and the uncalibrated v6 weight/total contract is unchanged.

A multi-reader or IndexMerge case proves that statement `net_bytes` appears once while every scan retains its own `scan_bytes`. PointGet and BatchPointGet cases prove exact `TotalKeys` CPU work, exact processed bytes, five raw diagnostics, present-zero acceptance, coverage mismatch rejection, completed-zero local replacement, locking acceptance without lock work, multi-plan checked accumulation, and independent coexistence with reader network transport. DML point plans use the same plan-local typed source. A UnionScan case proves `cpu_work` equals direct-child rows. IndexJoin emits no Join-local transport term. Sort/TopN cases retain their materialization, saturation, offset, overflow, and fast-path coverage. Reader gates distinguish missing evidence from attributable zero bytes. ROLLBACK remains explicitly unsupported.

Missing root stats, incomplete cop summaries, ambiguous scan details, missing reader transport, invalid expression structure, invalid mutation normalization, negative inputs, overflow, NaN, and infinity all fail closed with bounded reasons. SELECT produces no partial billable total. DML preserves complete independent units but does not claim a complete statement total.

Search and API review prove that runtime additions are limited to HashJoin state rows and the TiDB-only ScanDetail attachment count. The latter is provenance only and never enters a formula. `config.RUV2`, TiKV request charging, `ReportRUV2Consumption`, and resource-control behavior are unchanged.

Statement-summary detail, Prometheus metrics hooks, EXPLAIN rows, and General Log details are built from the same frozen result and agree on unit values. General Log records retain DML kind, input source, and input side. Historical rows remain distinguishable by version; legacy three-column convenience totals keep v3 behavior and remain zero for v4/v5, whose memory/history-reader details remain queryable.

## Idempotence and Recovery

All formula construction is read-only over frozen plan/runtime snapshots and must be safe to call repeatedly. Unit construction must not drain `RUDetails`; statement-scoped reader/network/write inputs use the already synchronized/frozen `RUV2Metrics` snapshot, while point lookups use their registered plan-local snapshot runtime stats.

`make bazel_prepare`, formatting, and targeted tests are safe to rerun. The failpoint test wrapper always disables failpoints during cleanup. If a milestone leaves mixed output versions, revert only that milestone's focused changes or finish all output consumers before running behavioral tests; never commit a mixed v4/v5 renderer state.

If the runtime source cannot prove a required input, add a bounded status reason and keep the formula unavailable. Do not recover by parsing runtime strings or by introducing an estimate not recorded in this plan. Any newly discovered formula datum still requires revisiting this design. The ScanDetail attachment count is a provenance exception recorded here after real-TiKV evidence showed that a zero value alone cannot distinguish missing detail from an observed empty scan.

## Artifacts and Notes

Current evidence commands used while drafting this plan included:

    rg -n 'type (BasicRuntimeStats|CopRuntimeStats|RuntimeStatsColl)' pkg/util/execdetails/runtime_stats.go
    rg -n 'TiKVCoprocessorResponseBytes|ResourceManagerReadCnt|ResourceManagerWriteCnt|Bypass' pkg/util/execdetails/ruv2_metrics.go
    rg -n 'innerWorker.task|type indexLookUpJoinRuntimeStats' pkg/executor/join
    rg -n 'type Physical(Selection|Projection|TopN|Sort|HashAgg|StreamAgg|HashJoin|MergeJoin|IndexJoin|Window|UnionScan)' pkg/planner/core/operator/physicalop

The evidence establishes availability and location, not completion of the later implementation.

The historical v4 ScanDetail-ownership evidence additionally includes a cleanup-safe real-TiKV run with one then-current-worktree TiDB, one PD, and one TiKV. It observed `cpu_work=4` for `site=tikv, op_class=filter_eval, operator_kind=selection` and `scan_bytes=212.5` for the corresponding table scan in General Log, Prometheus, and statement summary. That run used the then-shipped `v3-resource-formula-uncalibrated` label and left total preview RU absent; the current normative label is `v5-storage-work-uncalibrated`.

Historical real-TiKV point runs established the synthetic `point_lookup@statement` dimensions and plan-local snapshot ownership. Their RPC-only values are superseded; current positive typed output is covered by the fork's real response fixtures plus TiDB EXPLAIN/General-Log and direct Prometheus/statement-summary renderer regressions.

The final v5 real-TiKV run used an isolated `preview-ru-v5-930a` playground on TiDB/PD/TiKV ports 30000/28379/46160, with the TiDB status and metrics endpoint on 36080. The observed point-storage tuples `(cpu_work, scan_bytes, processed_keys, detail_records, completed_responses)` were PointGet hit `(1,43,1,1,1)`, PointGet miss `(1,0,0,1,1)`, BatchPointGet full hit `(2,86,2,1,1)`, partial hit `(2,43,1,1,1)`, all miss `(2,0,0,1,1)`, and unique-index PointGet `(2,87,2,2,2)`. The unique-index result proves index-key plus row-key accumulation. EXPLAIN emitted the same seven point rows with `model_version=v5`, `weight_version=v5-storage-work-uncalibrated`, and no total; General Log contained the same values and dimensions; Prometheus totals reconciled to the executed matrix; statement summary retained the per-digest values. No output surface contained either retired request unit. A real range reader published only `net_bytes=80`. In a pessimistic explicit transaction, the DML kept all six mutation diagnostics and reported `unmodeled_tikv_write_work`, while COMMIT independently reported `unmodeled_tikv_commit_work` and inherited no mutation units. After Ctrl-C shutdown, `ss` showed no listeners, a no-proxy PD health request failed with connection refused, and the tag directory and temporary TiDB binary were removed.

The UnionScan evidence includes a cleanup-safe real-TiKV transaction over three committed rows plus one uncommitted inserted row. Ordinary `EXPLAIN ANALYZE` reported `UnionScan actRows=4` and its direct `TableReader actRows=3`; `FORMAT='RU'` emitted `site=tidb, op_class=overlay_reader, operator_kind=unionscan, input_rows=3` and `cpu_work=3`, both sourced from `runtime_child_act_rows`, with no expression-count unit and no uncalibrated total. The exact `preview-ru-unionscan-v4` TiUP tag and data directory were removed and ports 25000, 23379, 41160, and 24930 were closed afterward.

The zero-processed scan evidence includes a cleanup-safe real-TiKV run. Empty and nonmatching table/index ranges emitted `scan_bytes=0` with independently nonzero `net_bytes`; a control Selection that filtered scanned rows to zero still emitted CPU and nonzero scan bytes. The historical request diagnostic is not part of v5 output.

The IndexJoin skipped-inner WIP evidence used these cleanup-safe commands; every wrapper ended with `new_refcount=0`:

    ./tools/check/failpoint-go-test.sh pkg/planner/core -run '^TestReadBillingDemoV4ExpressionCountsAndOrdering$/lookup_join_(accepts_a_proven_skipped_inner|skipped_inner_proof_is_fail_closed)' -count=1
    ./tools/check/failpoint-go-test.sh pkg/planner/core -run '^TestReadBillingDemoV4FormulaContract$' -count=1
    ./tools/check/failpoint-go-test.sh pkg/executor -run '^TestExplainAnalyzeFormatRUOutput$' -count=1
    ./tools/check/failpoint-go-test.sh pkg/executor -run '^TestFinishExecuteStmtSyncsTiDBRUV2FromRUDetails$/preview_RU_outputs_preserve_a_skipped_lookup_inner' -count=1

The historical IndexJoin Ready run proved a completed zero outer, an unconstructed dynamic inner, and consistent Join/outer scan/network output across all four surfaces. V5 retains those CPU/scan/network semantics and drops the historical request term. The exact playground data and temporary binaries were removed afterward.

The broader related core command is now expected to pass after correcting the inherited UnionScan diagnostic assertion:

    ./tools/check/failpoint-go-test.sh pkg/planner/core -run '^TestReadBillingDemoV5(FormulaContract|ExpressionCountsAndOrdering)$' -count=1

## Interfaces and Dependencies

Keep all v5 model types private to `pkg/planner/core` except the already established narrow `execdetails.HashTableRuntimeStats` and runtime-coverage interfaces. Do not export the preview weight container, concrete executor-private Join stats, or add public session/global variables.

The implementation uses existing TiDB packages plus the demo fork backport of client-go described in the v5 amendment. The typed accessor is consumed through a narrow private TiDB interface; no TiKV protocol field is added. `go.mod`, `go.sum`, and generated Bazel dependency metadata must move atomically to the reviewed pseudo-version.

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

Revision note (2026-07-24): DML PointGet and BatchPointGet no longer fail solely because they execute under DML. Their existing plan-local snapshot runtime stats provide structural `CmdGet`/`CmdBatchGet` counts, while the DML statement producer set remains open and its RUv2 read counter is not assigned to PointGet. This lets the lookup report its own requests without absorbing pessimistic-lock writes or unrelated ancillary reads.

Revision note (2026-07-28): real TiKV empty-range evidence corrected the scan zero contract. A fully covered ScanDetail with zero processed keys and bytes is now an observed zero-byte scan even when `TotalKeys` is positive because the latter includes seek/MVCC operations. This changes no weights, output dimensions, protocol fields, distsql ownership, or production RUv2 behavior.

Revision note (2026-07-29): real TiKV exposed a distinct zero-row gap for IndexJoin. When the completed outer child is empty, the dynamic inner reader is never constructed and therefore has no runtime rows; the old unconditional two-child lookup-Join formula suppressed the whole statement with `missing_runtime_rows`. The accepted revision adds an occurrence-scoped proven-skipped mask for the three IndexJoin subtypes. It requires completed-zero Join and outer Basic byte records, an exact original-child-ID mapping, no inner Basic/cop/ScanDetail or reader/point-producer group evidence, and globally unique plan-ID ownership across Main, CTE, and scalar-subquery trees. Every estimator, operator, and transport consumer uses the same immutable mask. This adds no executor datum or formula weight, never invokes an inner RPC counter, preserves active outer transport, and leaves every ambiguous or aliased case fail-closed.

Revision note (2026-08-02): read-only PointGet and BatchPointGet now use the same plan-local `SnapshotRuntimeStats.GetCmdRPCCount` ownership as DML instead of copying the statement `ResourceManagerReadCnt`. This preserves the single synthetic publisher and existing closed-set/fail-closed boundaries while preventing unrelated statement reads from changing the point-lookup unit. No executor datum, formula weight, or external unit name changed.

Revision note (2026-08-03): preview RU v5 globally retires request units and coefficients, leaving five weight-bearing semantic units. PointGet and BatchPointGet now publish typed TiKV storage-operation CPU work and processed MVCC bytes with explicit response-detail coverage plus raw diagnostics. Reader transport retains only network bytes; remote DML/COMMIT work becomes an explicit partial status while mutation diagnostics remain independent. The destructive semantic change uses model v5 and `v5-storage-work-uncalibrated`; a compatible fork backport is accepted for the demo, and production dependency/capability rollout is deferred.

Revision note (2026-08-03): preview RU v6 initially replaced the v5 remote-write partial with committed `write_keys` and plural `write_bytes`, each with its own weight. Autocommit DML consumes its frozen CommitDetails pair, explicit non-pipelined DML emits no remote-write component, and final COMMIT solely owns the transaction pair. Pessimistic-lock work and pipelined incomplete payloads remain deliberately unmodeled. Its initial pre-test identity was model v6 with `v6-storage-write-work-uncalibrated`.

Revision note (2026-08-03): before external v6 testing began, the same model gained one combined parser/optimizer proxy, `frontend_compile_bytes * FrontendCompileWeight`, and moved to `v6-frontend-compile-work-uncalibrated`. Successful plan-cache hits omit the frontend component entirely; other successful supported statements use original submitted/template SQL bytes. Ordinary EXPLAIN target ASTs do not preserve inner text, so their output uses the statement context's full submitted EXPLAIN SQL rather than inventing restored SQL. The eight-unit amendment supersedes the initial seven-unit v6 identity without changing production RUv2.

Revision note (2026-08-03): missing frontend source text no longer atomically fails the preview result. It omits only `frontend_compile_bytes`, keeps all other units and any calibrated total they produce, and remains distinct from an observed zero-byte sample. The output intentionally does not distinguish this undercount from plan-cache-hit exclusion.

Revision note (2026-08-05): real TiKV exposed a standalone IndexLookUp zero-handle gap analogous to, but structurally distinct from, the earlier IndexJoin empty-outer case. A non-pushdown IndexLookUp may complete its index phase with zero rows and never construct its table task; treating the absent table stats as a required scan suppressed the whole statement. The v6 execution mask now omits only an exactly identified table subtree after completed-zero lookup and complete zero-row index-cop proof, absence of all table execution evidence, and global plan-ID ownership validation. Actual index scan and network units remain visible, formulas and weights do not change, ambiguous cases remain fail-closed, and pushdown `LocalIndexLookUp` stays unsupported.

Revision note (2026-08-05): TiDB-local root `PhysicalShuffle` now reuses `cpu_work` and `CPUWeight` with `sum_i n_i*(1+k_i)`, where byte-bearing Basic runtime stats under each unique DataSource plan ID prove completed rows, including zero. One owning Shuffle aggregates all DataSources; concurrency and receiver multiplicity do not multiply work, `Shuffle.actRows` is never a formula input, and receiver stubs remain transparent non-billable wrappers. TiFlash Exchange, other semantic units, normalization, model v6, and `v6-frontend-compile-work-uncalibrated` remain unchanged.
