# Structural parity audit: current evidence

Baseline: TiDB Go master `6b2781326b722f217a61852ab403350858549bd0`, client-go
`v2.0.8-0.20260928031501-8edb23f6c7ee`, client-rust
`b2b3783` (published shared-buffer repair). The integration started at
`5503f8860883c6cd80bdd0d487d34c53787daf24`.

This is the list of **currently confirmed findings and explicit review gaps**.
It is not a claim that every semantic mismatch has been discovered or removed.
The coverage inventories enumerate every tracked artifact in 856 TiDB, 41 client-go and 41 kvproto
package directories, with 83 Rust crates awaiting current-master
acceptance. Original tests, generated/build/platform inputs, fixtures and
unassigned root artifacts are retained. The 2,051 candidate lines are search
evidence, not 2,051 defects. Some are errors Go intentionally returns.

Reproduce the inventory from the repository root with
`python3 rust/scripts/inventory-go-rust-parity.py --go-ref origin/master`.
Receipts and reviewed findings live separately so regeneration cannot certify
unreviewed packages. External dependencies beyond client-go, kvproto and TiPB still
require their own complete inventories before acceptance.

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
| Background lifetime still has request-type inference | The bridge retains a TxnHeartBeatRequest exception; additional pipelined/transaction-file cleanup paths need explicit operation scopes like Go's owners. | Open; the concrete native patch was rejected by automatic approval review and remains unapplied pending its separately requested approval. Foreground ResolveLock must remain cancellable. |
| Alternate storage session dispatch remains incomplete | The lightweight path's supported SET assignments now use the normal parser, but its transaction mode/autocommit lifecycle is not fully unified with the ordinary session owner. | Review required at session package scope; do not delete a dispatcher before migrating all its callers. |
| Other locally projected protocol packages | PD, the local TiKV service, etcd and BR inputs remain local projections. | Unreviewed completeness. The TiPB gate does not certify these packages. |

The earlier five diff comments (explicit lock retry limits, secondary retry
budget, locked snapshot commit timestamps, mock wake-up semantics and detached
resolver lifetime) have regression receipts in
`../../remove-extra-storage-policies-execplan.md`. They are not reopened solely
because the broader routing/lifetime migration remains unfinished.

## Reproduced baseline failures still requiring root-cause review

The previous embedded suite reproduced these ten failures both before and
after the transaction activation repair. These are failed validations, not yet
ten proven structural root causes. They remain open:

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
