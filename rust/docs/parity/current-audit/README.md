# Structural parity audit: current evidence

Current inventory baseline: TiDB Go master `93a01d31f6da205ae4bf376825293903a6899fdb`, client-go
`v2.0.8-0.20260928031501-8edb23f6c7ee`, client-rust
`6f663b396552eec6d1bfad76b65f813e317884a4` (published health-owner repair).
The current review starts at integration `771e62b2871890eeae2296cbeed04a19b1316201`.

This is the list of **currently confirmed findings and explicit review gaps**.
It is not a claim that every semantic mismatch has been discovered or removed.
The coverage inventories enumerate every tracked artifact in 856 TiDB, 41 client-go,
41 kvproto, 24 PD-client and 7 etcd-API package directories, with 83 Rust crates
awaiting current-master acceptance. Original tests, generated/build/platform inputs, fixtures and
unassigned root artifacts are retained. The 2,043 candidate lines are search
evidence, not a defect count. Some are errors Go intentionally returns.

Reproduce the inventory from the repository root with
`python3 rust/scripts/inventory-go-rust-parity.py --go-ref origin/master`.
Receipts and reviewed findings live separately so regeneration cannot certify
unreviewed packages. Other external dependencies still require complete inventories
before acceptance.

The expanded [remaining structural finding register](structural-findings.md)
consolidates 85 tracked ownership/contract findings (77 unresolved, eight repaired), review candidates and the
limits of the review. The [historical protocol comparison](protocol-projections.json)
lists 400 omissions, one PD oneof contract mismatch and 71 deliberate opaque
representations separately. It includes the keyspace-zero wire reproduction.
The [replacement-owner comparison](protocol-contracts-after.json) now reports
zero omissions/contract differences and retains the 71 opaque representations.
See [the removal receipt](complete-protocol-owner-repair.md) for caller and
validation coverage. Neither document claims that every repository semantic
mismatch is known.

The [2026-10-01 full register reconciliation](remaining-structure-review.md) adds
11 previously unregistered source-confirmed boundaries, records the statement-summary
SQL probe, and corrects stale T03 status. [Machine-readable findings](structural-findings.json)
and [source continuity](structural-source-continuity.json) retain every ID and current source pin.
No production code was repaired by this review.

The [full-picture repair sequence](repair-sequence.md) starts from the published
integration audit `4285385fad20855487ec1d8ff113290d48f949a5` and maps all 77 open
IDs exactly once into 12 workstreams. It identifies complete source owners,
caller migrations, removal gates and acceptance behavior. The
[living ExecPlan](../../full-structural-parity-execplan.md) sets the next native
PD package closure, coupled schema/DDL/GC sequence, independent correctness
work, benchmark method and publication gates. Workstreams are not partial
package completion claims; no production code changes in this planning update.

The [PD deadline owner repair](pd-deadline-owner-repair.md) implements the complete
pinned deadline package and its native TSO batch/retirement integration, including
a shared cancellation lost-wakeup correction. Native master is `5928b6e480b441496f9a3cd9bed6a7e8d56215a1`.
P06 is partial; public PD close and the whole parent packages remain open. The
77-unresolved count is unchanged. See the receipt for synchronized-source and
publication checks; earlier source snapshots above retain their original pins.

The [PD connection-context repair](pd-connectionctx-owner-repair.md) implements
the complete pinned connectionctx package and replaces unconditional healthy
TSO stream replacement with URL-keyed ownership. Native master is
`4e3169ed93e433eab38638a1dd92a24f25f7caaf`. Source tests, rejected ownership,
replacement and retained-stream lifecycle are covered; public PD close,
discovery and metadata concurrency remain open. P06 and the 77-ID count do not
advance to complete from this prerequisite.

The [PD batch-controller repair](pd-batch-owner-repair.md) implements the complete
pinned batch package and migrates native TSO to source default collection,
token ownership and buffer reuse. Native master is
`bcf74b7282b01372f93fb814ba601eb4c22b5d12`. The 64-request collector and
65,536-outstanding-batch policy are removed; full dispatcher/options/router
and public close/discovery/concurrency obligations remain open. This is another
complete prerequisite, not closure of P06 or all 77 unresolved findings.

The [PD retry-owner repair](pd-retry-owner-repair.md) implements the complete
pinned retry package and migrates native default initialization through it,
with bounded membership probes. Native master is `2fd0ecebadf0e8274a2b10ebfed729b03284a52b`.
Both transport regressions fail before repair; original Go race/goleak, 95 PD
cases and 1,470 native tests pass (two existing ignored). Full discovery/root/
TSO lifecycle acceptance remains open; P06 and the 77-ID count are unchanged.

The [statistics LFU follow-up](lfu-lifecycle-repair.md) removes synthetic trigger
tables and repairs joined shutdown after reviewing all five Go package artifacts.
C04 remains open: a retained concurrent-pressure reproduction fails in Stretto,
and its admission/metrics contract is not accepted as Ristretto-equivalent.
The [review follow-up](lfu-review-followup.md) closes two missed shutdown paths,
isolates the admission mismatch with a paused worker, and inventories all 91
artifacts of the pinned external module. Both dependency probes remain red.

The [shared cache ownership review](shared-cache-owner-review.md) traces all
four Ristretto consumers on current Go master, including inference, which is
absent from the integration Go checkout. It joins the B01/B02, C03 and C04
dependency work while preserving separate consumer lifetimes and acceptance
gates. Replacing storage alone leaves binding reload and coprocessor
configuration mismatches. This is a design checkpoint, not a runtime repair.

The [restore-utils protocol repair](restore-utils-protocol-repair.md) removes
P04's two handwritten protocol owners after reviewing the complete eight-artifact
Go package at master `93a01d31f6`. Complete generated files retain shared identity
through merging; lookups borrow generated rules. All original Go tests pass with
the race detector, and Rust tests plus all five source merge workloads pass.
Live BRIE execution remains open as E07.

The [range-tree protocol repair](rtree-protocol-repair.md) follows across the
complete seven-artifact `br/pkg/rtree` package. P05 removes generic file payloads,
the narrowed test fixture and local types at generated RPC boundaries. Original
Go tests, Rust identity/error-path cases and fixed-size update/merge workloads
pass. This does not accept metautil, progress-handle aliasing or live BRIE.

The [global-config synchronization repair](global-config-sync-repair.md) implements
O12's complete three-artifact Go package and its production session/factory/PD
integration. Explicit validated writes notify one domain keeper; reloads remain
quiet and failed stores are not retried. Original Go tests and the new Rust
regressions pass. Three broader session failures reproduce unchanged and remain
open. Parent-package and benchmark parity are not claimed.

The [subsystem ownership review](subsystem-structure-review.md) compares integration
`dae65456f9` with the same master and adds 18 findings across optimization,
expression execution, table/write policy, historical schemas, session migration,
worker lifetimes, control-plane services and remaining BR protocol consumers.
The retained probe reproduces strict-mode generated-column overflow and missing
session-state/historical-read/BR job handlers. The [complete scope matrix](structural-coverage.md)
accounts for every Rust crate and inventoried Go package directory, including
explicitly unreviewed queues. Scope accounting does not establish semantic
coverage or package acceptance. No production code is changed in this audit.

The [expanded production-owner review](expanded-ownership-review.md) compares
integration `13689e0b13` with the same current master and adds 13 findings in
privilege/account policy, binding ownership, import, wire/config/admission and
domain workers. Retained SQL and loopback-wire probes reproduce six finding
groups, including unauthorized joined UPDATE, ignored password history/import
options and changed Latin-1 bytes. Passing controls exclude stale keyword
matches. No production behavior or package acceptance is changed.

The [session/executor follow-up](session-ownership-review.md) adds 12 findings
against integration `960fa95b48` and the same Go master. Its retained SQL probe
reproduces lost self-join updates, bypassed multi-update foreign keys,
non-atomic in-process ALTER, missing shared cache eviction/flush, and dynamic
virtual tables served from fixtures or constants. Seven further owner gaps
are source-confirmed with explicit runtime limits. No production fix or new
package acceptance is included in this follow-up.

The [shared session cache repair](shared-session-plan-cache-repair.md) removes
C01's separate physical owners and metadata LRU, including close and flush
callers. C02's instance physical cache and full package acceptance remain open.

| Finding | Evidence and Go ownership | Status |
| --- | --- | --- |
| Partial TiPB declarations and stale dependency selection | Four local projections omitted 157 declarations from master's pin; the old checker validated only locally present declarations against this branch's older pin. `Executor` decode discarded a valid ExplainForConnection body. | Fixed by complete upstream input ownership; individual gaps listed below. Source gate, original Go tests, Rust wire tests and consumers validated. |
| Duplicate coprocessor and MPP contracts | Local projections lost 27 declarations and had two field-contract differences (MPP keyspace oneof/API version enum). Native generation copied four Go SharedBytes fields. | Fixed through complete native package re-exports and source regeneration; native repair published as b2b3783. Individual gaps listed below. |
| MPP dispatch bypassed the native API-context codec | The local literal api_version=1 is V1TTL; classic transactional Go uses V1=0 and the null keyspace from its codec. | Fixed by using native Keyspace and Request setters; the regression failed with V1TTL and now passes with V1. |
| MPP query/task identity has no statement owner | Go executor/mpp_gather.go gets one atomic query ID and UnixNano timestamp from StmtCtx.MPPQueryInfo; builder.go uses domain ServerID. Rust used local_query_id=1, gather_id=1, process ID and Unix seconds. | Fixed for the existing scan path: statement-owned query/task/gather atomics, shared across reader contexts and cluster retry decisions, retired at completed statement/record-set close. Server identity comes from the existing server-info getter; the absent lease allocator is tracked separately below. |
| Domain numeric server-ID lease owner is absent | Go domain.go owns acquireServerID/refreshServerIDTTL and exposes ServerID to executors. Rust node_server_info leaves its numeric identity at zero; sql_node also uses a standalone connection-ID constant. | Open. Sharing the existing getter removes process-ID synthesis from MPP but does not implement cluster-wide server-ID allocation, renewal or loss handling. |
| TiFlash poller had duplicate startup, detached lifetime and a private DDL publisher | Go ddl.Start owns one joined PollTiFlashRoutine, gates work on its owner manager, and publishes through executor.UpdateTableReplicaInfo. | Repaired in the existing classic path: one retained/joined worker, shared owner gate and DDL executor, no poller transaction opener. Required publication gates are recorded in the ExecPlan. This is not full DDL/infosync package acceptance. |
| TiFlash HTTP and status protocols diverged | Raw TCP did not decode chunked responses or bound reads; helper.ComputeTiFlashStatus expects a count plus a space-separated region line. Go uses unique per-store regions, the configured replica count, and errors from live stores. | Repaired with the existing HTTP library and Go-compatible parsing/progress; available tables no longer republish. Six behavior regressions failed before correction. |
| A table update replaced sibling TiFlash placement rules | The invented new_tiflash_bundle put one table into the shared tiflash group; PD partial bundle updates replace named groups. Go infosync.SetPlacementRule uses the individual-rule API with index 120 and group priority. | Removed the bundle constructor and its caller. Individual-rule delivery preserves sibling rules; region-count queries use escaped memcomparable keys. Two-table HTTP regression passes. |
| Classic TiFlash placement lifecycle still belongs to the poller | Go onSetTableFlashReplica configures rules before metadata; GC owns dropped-table cleanup. Rust still repairs/deletes rules and accelerates scheduling every poll. Go runs refreshTiFlashPlacementRules only for NextGen. | Open. Migrate all set/create-like/partition/drop/reset consumers to DDL/GC before deleting reconciliation. Partition IDs and other keyspaces must not be classified as stale by a logical-table-only snapshot. |
| TiFlash DDL status/reset and partition semantics are incomplete | Go preserves existing availability unless ResetAvailable is requested and publishes physical partition availability. Rust SetTiFlashReplica always reconstructs unavailable metadata; UpdateTiFlashReplicaStatus/polling only use logical table IDs. | Open, requiring the complete DDL consumer lifecycle above. |
| TiFlash polling lacks shared progress/backoff and PD HTTP discovery owners | Go refreshTiFlashTicker owns periodic store refresh, unavailable backoff and available-table progress caching; infosync gets PD HTTP store states including Down/Disconnected. Rust calls gRPC discovery every tick and filters legacy Up stores; no shared progress cache or cluster-security HTTP manager. | Open. Errors from selected Up stores are fixed, but that does not certify discovery, offline-store progress, cache, TLS/failover or scheduling parity. |
| Generic mutation constructor chooses table assertion policy | `tidb-txnkv/src/transaction/mutation.rs::BufferMutation::insert` combines presume-not-exists with AssertNotExist. `tidb-exec/src/real_tikv_dml.rs::plan_insert` intentionally does no snapshot check. Go `pkg/table/tables` chooses Unknown for a lazy optimistic miss and NotExist for an eager/pessimistic insert. | Open. Reconcile all callers; blanket Unknown would break eager/pessimistic ownership. Configured WritePlanningSnapshot lacks transaction-mode input; system-row index writes omit assertions. These need the table owner, not another global default. |
| Region/cache/RPC algorithms have competing owners | `tidb-txnkv/src/driver/client_bridge.rs::ClientPd` delegates to TiDB routing/recovery/transport while client-rust also implements these algorithms. DistSQL still needs TiDB capabilities. | Open architecture migration. Duplication alone is not proof of a runtime failure; native RetryBackoffer already owns RegionBackoffBudget. |
| Background lifetime inferred from request type/client mode | The earlier bridge used a TxnHeartBeatRequest exception and native transaction_tasks flag. | Repaired as T03 by native 488bb73 and the synchronized TiDB dependency. Explicit operation lifetimes now cross the bridge; foreground ResolveLock remains cancellable. The prior approval block is historical and resolved. Broader native TSO shutdown remains separately open as P06. |
| Alternate storage session dispatch remains incomplete | The lightweight path's supported SET assignments now use the normal parser, but its transaction mode/autocommit lifecycle is not fully unified with the ordinary session owner. | Review required at session package scope; do not delete a dispatcher before migrating all its callers. |
| Other locally projected protocol packages | Five local PD/TiKV/BR/etcd projections caused 400 omissions and a PD presence mismatch. | Removed. Complete native ownership, descriptor-derived TiKV transport and full pinned etcd inputs eliminate the compared gaps; all 71 opaque representations retain presence. P01/P02 contract ownership repaired; P03 discovery and external package/runtime acceptance remain open. |

The earlier five diff comments (explicit lock retry limits, secondary retry
budget, locked snapshot commit timestamps, mock wake-up semantics and detached
resolver lifetime) have regression receipts in
`../../remove-extra-storage-policies-execplan.md`. They are not reopened solely
because the broader routing/lifetime migration remains unfinished.

## Reproduced baseline failures still requiring root-cause review

The previous embedded suite reproduced these ten failures both before and
after the transaction activation repair. These are failed validations, not yet
ten proven structural root causes. They remain open. The expanded audit reran
`system_table_ddl_does_not_publish_statistics_events_like_go`: it fails at
`p1 exists` after ADD PARTITION. Current DDL already filters system-schema
notifications, so this failure does not establish a missing event filter:

- `add_partition_statistics_follow_global_prune_mode_like_go`
- `cluster_info_reports_this_node`
- `drop_partitions_statistics_match_go`
- `exchange_partition_validates_and_swaps_real_rows_atomically`
- `global_stats_drive_partition_plans_like_go`
- `partition_scoped_analyze_refreshes_global_count_and_modify_count`
- `stats_notifier_uses_a_real_internal_transaction_like_go`
- `system_table_ddl_does_not_publish_statistics_events_like_go`
- `truncate_hash_partition_statistics_match_go`
- `truncate_partitions_refreshes_global_stats_meta_like_go`

## Complete TiPB declaration gap list

All 157 entries below were absent from the four old projections and are now
owned by the complete upstream inputs: 76 messages, 51 fields, 29 enums and
one enum value. The machine-readable before-image retains tags and source
revisions in `tipb-mismatches-before.json`. This fixes declaration loss; it
does not invent SQL executor implementations for previously unsupported types.

| Declaration | Former gap | Status |
| --- | --- | --- |
| `.tipb.TableInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.IndexInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.KeyRange` | missing message | Fixed by complete upstream schema |
| `.tipb.ChecksumRewriteRule` | missing message | Fixed by complete upstream schema |
| `.tipb.ChecksumRequest` | missing message | Fixed by complete upstream schema |
| `.tipb.ChecksumResponse` | missing message | Fixed by complete upstream schema |
| `.tipb.Expr.rpn_args_len` | missing field | Fixed by complete upstream schema |
| `.tipb.Expr.order_by` | missing field | Fixed by complete upstream schema |
| `.tipb.RpnExpr` | missing message | Fixed by complete upstream schema |
| `.tipb.ByItem.rpn_expr` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.exchange_receiver` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.join` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.kill` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.Projection` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.partition_table_scan` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.sort` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.window` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.fine_grained_shuffle_stream_count` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.fine_grained_shuffle_batch_size` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.expand` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.expand2` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.broadcast_query` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.cte_sink` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.cte_source` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.index_lookup` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.explain_for_connection` | missing field | Fixed by complete upstream schema |
| `.tipb.ExchangeSender.partition_keys` | missing field | Fixed by complete upstream schema |
| `.tipb.ExchangeSender.types` | missing field | Fixed by complete upstream schema |
| `.tipb.ExchangeSender.compression` | missing field | Fixed by complete upstream schema |
| `.tipb.ExchangeSender.upstream_cte_task_meta` | missing field | Fixed by complete upstream schema |
| `.tipb.ExchangeSender.same_zone_flag` | missing field | Fixed by complete upstream schema |
| `.tipb.CTESink` | missing message | Fixed by complete upstream schema |
| `.tipb.CTESource` | missing message | Fixed by complete upstream schema |
| `.tipb.IndexLookUp` | missing message | Fixed by complete upstream schema |
| `.tipb.EncodedBytesSlice` | missing message | Fixed by complete upstream schema |
| `.tipb.ExchangeReceiver` | missing message | Fixed by complete upstream schema |
| `.tipb.ANNQueryInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.InvertedQueryInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.FTSBooleanTerm` | missing message | Fixed by complete upstream schema |
| `.tipb.FTSBooleanQuery` | missing message | Fixed by complete upstream schema |
| `.tipb.FTSBooleanNode` | missing message | Fixed by complete upstream schema |
| `.tipb.FTSQueryInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.TiCIVectorQueryInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.ColumnarIndexInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.TableScan.ranges` | missing field | Fixed by complete upstream schema |
| `.tipb.TableScan.pushed_down_filter_conditions` | missing field | Fixed by complete upstream schema |
| `.tipb.TableScan.runtime_filter_list` | missing field | Fixed by complete upstream schema |
| `.tipb.TableScan.deprecated_ann_query` | missing field | Fixed by complete upstream schema |
| `.tipb.TableScan.used_columnar_indexes` | missing field | Fixed by complete upstream schema |
| `.tipb.PartitionTableScan` | missing message | Fixed by complete upstream schema |
| `.tipb.Join` | missing message | Fixed by complete upstream schema |
| `.tipb.RuntimeFilter` | missing message | Fixed by complete upstream schema |
| `.tipb.IndexScan.fts_query_info` | missing field | Fixed by complete upstream schema |
| `.tipb.IndexScan.tici_vector_query_info` | missing field | Fixed by complete upstream schema |
| `.tipb.Selection.rpn_conditions` | missing field | Fixed by complete upstream schema |
| `.tipb.Selection.child` | missing field | Fixed by complete upstream schema |
| `.tipb.Projection` | missing message | Fixed by complete upstream schema |
| `.tipb.Aggregation.rpn_group_by` | missing field | Fixed by complete upstream schema |
| `.tipb.Aggregation.rpn_agg_func` | missing field | Fixed by complete upstream schema |
| `.tipb.Aggregation.child` | missing field | Fixed by complete upstream schema |
| `.tipb.Aggregation.pre_agg_mode` | missing field | Fixed by complete upstream schema |
| `.tipb.TopN.child` | missing field | Fixed by complete upstream schema |
| `.tipb.TopN.partition_by` | missing field | Fixed by complete upstream schema |
| `.tipb.Limit.child` | missing field | Fixed by complete upstream schema |
| `.tipb.Limit.partition_by` | missing field | Fixed by complete upstream schema |
| `.tipb.Kill` | missing message | Fixed by complete upstream schema |
| `.tipb.ExplainForConnection` | missing message | Fixed by complete upstream schema |
| `.tipb.ExecutorExecutionSummary.tiflash_hash_table_stats` | missing field | Fixed by complete upstream schema |
| `.tipb.TiFlashExecutionInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.Sort` | missing message | Fixed by complete upstream schema |
| `.tipb.WindowFrameBound` | missing message | Fixed by complete upstream schema |
| `.tipb.WindowFrame` | missing message | Fixed by complete upstream schema |
| `.tipb.Window` | missing message | Fixed by complete upstream schema |
| `.tipb.GroupingExpr` | missing message | Fixed by complete upstream schema |
| `.tipb.GroupingSet` | missing message | Fixed by complete upstream schema |
| `.tipb.Expand` | missing message | Fixed by complete upstream schema |
| `.tipb.ExprSlice` | missing message | Fixed by complete upstream schema |
| `.tipb.Expand2` | missing message | Fixed by complete upstream schema |
| `.tipb.BroadcastQuery` | missing message | Fixed by complete upstream schema |
| `.tipb.TiFlashHashTableStats` | missing message | Fixed by complete upstream schema |
| `.tipb.InUnionMetadata` | missing message | Fixed by complete upstream schema |
| `.tipb.CompareInMetadata` | missing message | Fixed by complete upstream schema |
| `.tipb.GroupingMark` | missing message | Fixed by complete upstream schema |
| `.tipb.GroupingFunctionMetadata` | missing message | Fixed by complete upstream schema |
| `.tipb.IntermediateOutputChannel` | missing message | Fixed by complete upstream schema |
| `.tipb.DAGRequest.start_ts_fallback` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.collect_range_counts` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.max_warning_count` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.sql_mode` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.max_allowed_packet` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.is_rpn_expr` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.user` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.force_encode_type` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.intermediate_output_channels` | missing field | Fixed by complete upstream schema |
| `.tipb.UserIdentity` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshTeardownRequest` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshSearchRequest` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshEvalContext` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshFilterExprColumn` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshSearchResponse` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshSearchResult` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshSearchRow` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshSearchStats` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshANNStats` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.CreateIndexRequest` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.CreateIndexResponse` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.DropIndexRequest` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.DropIndexResponse` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.TiCITableInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.TiCIColumnInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.TiCIIndexInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.TiCIIndexInfo.OtherParamsEntry` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.ParserInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.ParserInfo.ParserParamsEntry` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.GetIndexProgressRequest` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.GetIndexProgressResponse` | missing message | Fixed by complete upstream schema |
| `.tipb.TopSQLRecord` | missing message | Fixed by complete upstream schema |
| `.tipb.TopRURecord` | missing message | Fixed by complete upstream schema |
| `.tipb.TopRURecordItem` | missing message | Fixed by complete upstream schema |
| `.tipb.TopSQLRecordItem` | missing message | Fixed by complete upstream schema |
| `.tipb.TopSQLRecordItem.StmtKvExecCountEntry` | missing message | Fixed by complete upstream schema |
| `.tipb.SQLMeta` | missing message | Fixed by complete upstream schema |
| `.tipb.PlanMeta` | missing message | Fixed by complete upstream schema |
| `.tipb.EmptyResponse` | missing message | Fixed by complete upstream schema |
| `.tipb.TopRUConfig` | missing message | Fixed by complete upstream schema |
| `.tipb.TopSQLSubRequest` | missing message | Fixed by complete upstream schema |
| `.tipb.TopSQLSubResponse` | missing message | Fixed by complete upstream schema |
| `.tipb.ChecksumScanOn` | missing enum | Fixed by complete upstream schema |
| `.tipb.ChecksumAlgorithm` | missing enum | Fixed by complete upstream schema |
| `.tipb.ExecType.TypeExplainForConnection` | missing or mismatched enum value | Fixed by complete upstream schema |
| `.tipb.CompressionMode` | missing enum | Fixed by complete upstream schema |
| `.tipb.VectorIndexKind` | missing enum | Fixed by complete upstream schema |
| `.tipb.VectorDistanceMetric` | missing enum | Fixed by complete upstream schema |
| `.tipb.ANNQueryType` | missing enum | Fixed by complete upstream schema |
| `.tipb.FTSQueryType` | missing enum | Fixed by complete upstream schema |
| `.tipb.FTSBooleanOccur` | missing enum | Fixed by complete upstream schema |
| `.tipb.FTSBooleanModifier` | missing enum | Fixed by complete upstream schema |
| `.tipb.FTSBooleanTermType` | missing enum | Fixed by complete upstream schema |
| `.tipb.ColumnarIndexType` | missing enum | Fixed by complete upstream schema |
| `.tipb.JoinType` | missing enum | Fixed by complete upstream schema |
| `.tipb.JoinExecType` | missing enum | Fixed by complete upstream schema |
| `.tipb.RuntimeFilterType` | missing enum | Fixed by complete upstream schema |
| `.tipb.RuntimeFilterMode` | missing enum | Fixed by complete upstream schema |
| `.tipb.TiFlashPreAggMode` | missing enum | Fixed by complete upstream schema |
| `.tipb.WindowBoundType` | missing enum | Fixed by complete upstream schema |
| `.tipb.RangeCmpDataType` | missing enum | Fixed by complete upstream schema |
| `.tipb.WindowFrameType` | missing enum | Fixed by complete upstream schema |
| `.tipb.TiFlashHashTableSizeKind` | missing enum | Fixed by complete upstream schema |
| `.tipb.GroupingMode` | missing enum | Fixed by complete upstream schema |
| `.tipb.SPFreshTruncateMode` | missing enum | Fixed by complete upstream schema |
| `.tipb.SPFreshFilterExprColumnSource` | missing enum | Fixed by complete upstream schema |
| `.tipb.SPFreshErrorCode` | missing enum | Fixed by complete upstream schema |
| `.tipb.tici.IndexType` | missing enum | Fixed by complete upstream schema |
| `.tipb.tici.ParserType` | missing enum | Fixed by complete upstream schema |
| `.tipb.CollectorType` | missing enum | Fixed by complete upstream schema |
| `.tipb.ItemInterval` | missing enum | Fixed by complete upstream schema |
| `.tipb.Event` | missing enum | Fixed by complete upstream schema |

## Complete coprocessor and MPP gap list

These 29 gaps are removed by native package ownership. In addition to 10
messages and 17 fields, the old MPP TaskMeta declared a plain keyspace_id
instead of a oneof arm and int32 instead of the APIVersion enum. The same
native response generator now respects all four upstream SharedBytes fields.

| Declaration | Former gap | Status |
| --- | --- | --- |
| `.coprocessor.Request.tasks` | missing field | Fixed through native package ownership |
| `.coprocessor.Request.table_shard_infos` | missing field | Fixed through native package ownership |
| `.coprocessor.Request.versioned_ranges` | missing field | Fixed through native package ownership |
| `.coprocessor.ShardInfo` | missing message | Fixed through native package ownership |
| `.coprocessor.TableShardInfos` | missing message | Fixed through native package ownership |
| `.coprocessor.TiCIEstimateCountRequest` | missing message | Fixed through native package ownership |
| `.coprocessor.TiCIEstimateCountResponse` | missing message | Fixed through native package ownership |
| `.coprocessor.TableRegions` | missing message | Fixed through native package ownership |
| `.coprocessor.BatchRequest.context` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.tp` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.data` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.start_ts` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.schema_ver` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.table_regions` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.log_id` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.connection_id` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.connection_alias` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.table_shard_infos` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchResponse` | missing message | Fixed through native package ownership |
| `.coprocessor.StoreBatchTaskResponse.data_merged_into_response` | missing field | Fixed through native package ownership |
| `.coprocessor.DelegateRequest` | missing message | Fixed through native package ownership |
| `.coprocessor.DelegateResponse` | missing message | Fixed through native package ownership |
| `.mpp.TaskMeta.keyspace_identity` | missing field | Fixed through native package ownership |
| `.mpp.IsAliveRequest` | missing message | Fixed through native package ownership |
| `.mpp.IsAliveResponse` | missing message | Fixed through native package ownership |
| `.mpp.DispatchTaskRequest.table_regions` | missing field | Fixed through native package ownership |
| `.mpp.DispatchTaskRequest.table_shard_infos` | missing field | Fixed through native package ownership |
| `.mpp.TaskMeta.keyspace_id` | field contract differs | Fixed through native package ownership |
| `.mpp.TaskMeta.api_version` | field contract differs | Fixed through native package ownership |


## Persisted DDL lifecycle follow-up (2026-09-30)

Compared worker/scheduler sources at Go master e953a09d9d; the intervening
master change affects numeric COALESCE planning, not these sources or module
pins. Existing persisted CHECK/create schema/create table/create tables/rename
tables/drop schema/drop table paths now share one lifecycle. Removed seven
worker loops, action-owned history writes, premature SYNCED assignments, and
repeated full queue scans for one job. DONE remains active; durable MDL recovery
precedes any next phase or the separate history transaction. Full submitted
table-ID scopes, system-schema owner-column omission, notification failure,
history failure, owner-conditioned MDL cleanup, and pre-commit ownership checks
use shared handling. Batch completion uses Go SetTableInfos rather than a
single-table finish, preserving the full history list and completing empty batches.

Still open: non-CHECK SQL admission usually bypasses persisted jobs; DROP lacks
Go finishDDLJob's delete-range registration/GC owner and typed DropTableArgs;
materialized-view seed action/reorg gaps remain and those actions are not
dispatched (shared completion cleanup is recorded below); general
pause/cancel/reorg scheduling and MDL-disabled operation
are not complete. TiFlash placement creation/deletion, partition handling and
progress/backoff remain open as described above. These are source-backed
maintenance changes, not a complete pkg/ddl or pkg/meta/model acceptance claim.


## Materialized-view seed completion ownership (2026-09-30)

Compared pkg/ddl/mview_worker.go and job_worker.go at master e953a09d9d. Five
remaining private history writers in tidb-exec/src/cluster_ddl.rs are removed.
Both explicit seed entrypoints reuse the shared point lookup, MDL recovery,
active-state lifecycle and history finalizer. DONE/ROLLBACK_DONE remain active
until synchronization; cancellation uses the same finalizer immediately and
persists its error rather than constructing and discarding mutations. Initial
build errors persist in Job.Error/ErrorCount through rollback and history,
replacing an invented success warning. The shared helper also owns CHECK's
error-field update. Seed actions remain excluded from the live dispatcher.

Three red reproductions now pass, along with 93 source planner, 7 commit
classification and 4 embedded DDL tests, affected all-target compilation and
root lint. Exact commands, source decisions, files and limitations are in
[the materialized-view receipt](../../full-structural-parity-execplan.md#materialized-view-seed-completion-cleanup-receipt-2026-09-30).

This does not certify complete materialized-view or pkg/ddl parity. Remaining
seed differences include build/reorg transaction ownership, rollback data GC
and base/log back-references, required-system-table failure transitions, and
worker validation/error identity details. General DDL admission/scheduling,
delete-range GC, MDL-disabled operation and TiFlash placement gaps above remain
open. No full package validation, multi-node interoperability run or benchmark
performance claim accompanies this maintenance repair.


## Persisted action queue ownership (2026-09-30)

Go master e953a09d9d job_worker.go owns updateDDLJob and immediate cancelled-job
finalization after discarding action mutations. Removed 17 action-local queue
writes from existing Rust CHECK/catalog/MV seed planners. The shared worker
now receives borrowed job state, metadata writes, updateRawArgs and any handled
rollback error. It persists one envelope update or finalizes cancellation.
Historical Job.Error no longer cancels successful create/schema/batch/rename
actions. Fresh cancellation errors increment ErrorCount once, including after
an owner-loss retry; cancellation neither announces nor waits for a new schema.
The separate CHECK validation-error state transaction remains explicit.

Red/green regressions and all 95 planner + 7 commit-classification + 4 embedded
DDL tests pass, as do affected compilation and root lint. Files, exact commands,
source decisions and publication gates are in
[the action-state receipt](../../full-structural-parity-execplan.md#persisted-action-state-ownership-receipt-2026-09-30).

Still open: ordinary non-cancelling errors return without Go's persisted
countForError/global error-limit/CANCELLING lifecycle; CHECK missing-object and
constraint validation does not yet consistently select Go's cancelled state.
Other worker validation/error identities and the previously listed admission,
reorg, rollback dependency/GC, scheduler, MDL-disabled and TiFlash ownership
gaps remain. The supported action list is unchanged; no unaccepted seed action
was dispatched and no package-complete parity or performance claim is made.

The [shared UPDATE owner repair](shared-update-owner-repair.md) removes the
UPDATE/ODKU write bypasses and executor undo logs, with fail-before/pass-after
regressions. E01 is repaired; E02 still requires FK plan integration.

The cache-owner continuation additionally reproduced two executor failures on
unchanged integration `5fcc321d48`: `prepared_in_predicate_uses_filtered_stats_for_cache_admission`
and `cached_index_join_compare_filter_rebinds_its_parameter`. Their original
assertions remain enabled. See the [cache repair receipt](shared-session-plan-cache-repair.md)
for the commands and independent baseline evidence. The broader MySQL pipeline
test also reproduces its existing account-name quoting assertion on unchanged
source; this is not counted as a passing protocol suite.

The [progress-ownership follow-up](rtree-progress-ownership-repair.md) rechecks
the same complete range-tree package and removes deep progress/result-tree
clones and detached test snapshots. Inserted, returned and completed progress
handles now share the original record. This closes the recorded P05 ownership
limitation without changing the unresolved-finding count.
