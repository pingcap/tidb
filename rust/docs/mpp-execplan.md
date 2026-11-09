# MPP execution from the physical plan, ported from Go

This ExecPlan is a living document. Keep Progress, Surprises & Discoveries,
Decision Log and Outcomes & Retrospective current as work proceeds. It follows
`PLANS.md` at the repository root.

## Purpose / Big Picture

Go TiDB runs an analytical query on TiFlash as MPP: the planner's physical
plan below a `TableReader` (read type MPP) is a tree of fragments joined by
`ExchangeSender` / `ExchangeReceiver`; Go cuts it into fragments
(`GenerateRootMPPTasks`), turns each fragment into one DAG per TiFlash task
(`ToPB` on every operator), dispatches every task, and reads the root
fragment's streams (`MPPGather` over the local MPP coordinator).

The Rust planner already chooses Go's MPP plans (EXPLAIN shows Go's
Broadcast and HashPartition exchanges: `unistore_cop.rs`
`tiflash_reads_plan_as_go_mpp_fragments`), but execution cannot follow:
`tidb_executor::mpp_coordinator` hand-builds one fragment for a bare full
scan, and `check_mpp_request` refuses any pushed Selection, TopN,
aggregation, ordered scan or join (register row M01; M04 waits on it). The
embedded store refuses `ExchangeSender`, `ExchangeReceiver`, joins and
`Expand` in its MPP handler (`tidb-unistore-cophandler` `mpp.rs`).

After this plan, a Rust node runs Go's MPP plans end to end on the embedded
store's mock TiFlash: `SELECT ... JOIN ... GROUP BY` with
`tidb_isolation_read_engines='tiflash'` and `tidb_enforce_mpp=1` returns the
rows a Go tidb-server returns, through Go's fragments, Go's task DAGs and Go's
coordinator. Observable: Go's `pkg/executor/test/tiflashtest` tests pass on
the unistore stack.

## Progress

- [ ] Milestone 1: `ToPB` on the physical operators (Go
  `pkg/planner/core/operator/physicalop/*.go` `ToPB`, 17 operators: table and
  index scan, selection, projection, TopN, limit, sort, hash and stream
  aggregation, hash join, exchange sender and receiver, expand, window, CTE
  sink and source, local index lookup). The base refusal already exists
  (`PhysicalPlan::to_pb`). Unit tests copy Go's `ToPB` expectations where Go
  pins them. Done so far (2026-11-10): Go `util.ColumnToProto` /
  `ColumnsToProto` (`tidb-util` `misc.rs`) and `tables.BuildTableScanFromInfos`
  / `BuildPartitionTableScanFromInfos` with the primary-key lookups
  (`tidb-table` `tables/scan_from_infos.rs`), on Go's vectors; the physical
  table scan carries Go's `Table` and `Columns`. `PhysicalPlan::to_pb`
  (`physicalop` `to_pb.rs`, Go's `BuildPBContext`) for Selection,
  Projection, Limit, TopN, TableScan (not the TiFlash partition scan yet),
  ExchangeSender, ExchangeReceiver, HashAgg, StreamAgg (Go `agg_to_pb.go`'s
  plan-to-PB half in `tidb-expression-aggregation`), HashJoin, Sort (partial)
  and Window (`WindowFuncToPBExpr`, `FrameBound.ToPB`); a planned MPP
  fragment's `ToPB` is Go's to the byte for a filter, a bare read, a
  two-phase aggregation, a broadcast join and two window functions
  (`unistore_cop/mpp_to_pb.rs`, Go testkit probe vectors). Left: expand,
  CTE, index scan, local index lookup, the TiFlash partition scan.
- [ ] Milestone 2: Go `fragment.go` (`Fragment`, `GenerateRootMPPTasks`,
  `mppTaskGenerator`: fragments, union-all untwisting, CTE task counts,
  `constructMPPTasksImpl` over `MPPClient.ConstructMPPTasks`, partition
  pruning), with Go's fragment tests. Design (2026-11-10): Go's fragments
  hold pointers into cloned plan trees and fill each sink's `TargetTasks` /
  `Tasks` and each receiver's `Tasks` in place. Here a `Fragment` owns its
  sink tree (`untwistPlanAndRemoveUnionAll` already clones per route); the
  generator owns every fragment in `frags` (bottom-up, as Go appends them)
  and addresses a receiver or a lower fragment's sink by plan id and fragment
  index, so the in-place writes become indexed writes. The sender gains Go's
  `Tasks` (self tasks). The session side (`ConnectionID`, `SessionAlias`,
  `ChooseMppVersion`, `AllocMPPTaskID`, dynamic prune, `TiFlashReplicaRead`,
  the dispatch policy, `AppendWarning`, `GetMPPClient`, the distsql context,
  the infoschema) reaches the generator through one context trait, as Go's
  `sessionctx.Context` does. Steps: `Fragment.init` (Go
  `TestFragmentInitSingleton`) and `constructMPPTasksByChildrenTasks`; the
  untwist; `constructMPPTasksImpl` with `distsql.TableHandleRangesToKVRanges`
  and `partitionPruning`; then the CTE sink/source operators and
  `fillLocalCTECounts` (`TestFillLocalCTECountsUsesLocalTaskCounts`). Done
  (2026-11-10): `Fragment.init`, the untwist, `buildFragments`, and the
  generator (`GenerateRootMPPTasks`, `generateMPPTasksForFragment`,
  `constructMPPTasksByChildrenTasks`, `constructMPPTasksImpl`) over
  `MppTaskGeneratorContext`; the context over the session (2026-11-10,
  `tidb-executor` `local_mpp_coordinator.rs`: `TableHandleRangesToKVRanges`,
  the MPP client, task ids, MPP version, dynamic prune, and Go's fragment
  `partitionPruning` over `partitionpruning.PartitionPruning`, which is the
  planner's pruning helper with Go's explicit inputs). tidb-copr's
  `ConstructMPPTasks` builds a partitioned request's tasks per partition
  (`buildBatchCopTasksCore` over `rangesForEachPhysicalTable`,
  `convertRegionInfosToPartitionTableRegions`) and dispatches them as
  `TableRegions`; a union scan over an MPP gather reads the buffer through
  the fragment's table scan (Go `buildUnionScanFromReader`'s `MPPGather`
  arm). Go `TestReadPartitionTable` and `TestTiflashEmptyDynamicPruneResult`
  pass. Left: the MPP CTE pieces, and the disaggregated (consistent-hash)
  task building for a partitioned request.
- [ ] Milestone 3: the embedded store's MPP. Scope found 2026-11-10: the
  Rust embedded store serves no MPP at all (`tidb-unistore` `rpc.rs` answers
  MPP commands "not support") and its cluster has no TiFlash store, so this
  milestone is: Go `mockstore.WithMockTiFlash(n)` (TiFlash stores labelled
  `engine=tiflash` in the embedded cluster, as Go's `unistore` bootstraps
  them); `rpc.go`'s `DispatchMPPTask`, `EstablishMPPConnection`,
  `CancelMPPTask` and `IsAlive` over `cophandler.HandleMPPDAGReq` and the
  `MPPTaskHandler` tunnel registry (`mpp.go`); and `mpp_exec.go`'s
  `exchSenderExec` (PassThrough, Broadcast, HashPartition), `exchRecvExec`,
  `joinExec`, `expandExec` and `indexLookUpExec`, with `buildMPPExchangeSender`
  / `Receiver` / `Join` / `Expand` / `IndexLookUp` in `mpp.rs`. Done
  (2026-11-10): `MPPTaskHandler`, `ExchangerTunnel`, `HandleMPPDAGReq`, the
  exchange sender/receiver and join executors; the store's MPP task sets
  (`AddStore` / `RemoveStore`, `Create` / `Get` / `RemoveMPPTaskHandler`);
  `rpc.go`'s `DispatchMPPTaskWithStoreID` (the task on its own thread, reading
  the store under a per-scan lock), `EstablishMPPConnectionWithStoreID` (the
  collected packets as client-go's mock stream, a buffered
  `MppStreamResponse::from_packets` in the client fork) and `IsAlive`; the
  RPC client is the tasks' `MppRpcClient`. Mock TiFlash stores
  (`with_mock_tiflash`) already existed. Left: `expandExec`,
  `indexLookUpExec`, `CancelMPPTask` (Go panics "todo" too).
- [ ] Milestone 4: Go's local MPP coordinator and `MPPGather`
  (`pkg/executor/internal/mpp/local_mpp_coordinator.go`,
  `executor_with_retry.go`, `recovery_handler.go`, `pkg/executor/mpp_gather.go`)
  replacing the single-fragment lowering: the reader keeps its physical plan,
  the coordinator dispatches every fragment's tasks and reads the root's.
  Done (2026-11-10): `localMppCoordinator` (`Execute`, `appendMPPDispatchReq`
  over `ConstructDAGReq`, `dispatchAll` / `handleDispatchReq` /
  `receiveResults` on threads, `respChan`, kill checks every 3 s, cancel on
  close) and `UpdateExecutorTableID` in `local_mpp_coordinator.rs`;
  `MPPGather` and `useMPPExecution` in `mpp_gather.rs` under the existing
  `ExecutorWithRetry`; `buildTableReader` routes MPP readers to it; the
  embedded node now has Go mockstore's `GetMPPClient` (copr's `MPPClient`
  over the unistore RPC client). Go `TestMppExecution`, `TestReadUnsigedPK`,
  `TestJoinRace`, `TestInjectExtraProj`, `TestMppEnum`, `TestMppUnionAll`
  (less its task-count failpoint), `TestUnionWithEmptyDualTable`,
  `TestAvgOverflow` and `TestMppApply` pass on mock TiFlash stores
  (`unistore_cop/mpp_execution.rs`); the builder's interim local
  ExchangeSender / ExchangeReceiver arms are gone (Go has none). Left: the coordinator
  manager (`mppcoordmanager`, task-status reports), zone helper, virtual
  columns over the gather, and retiring the single-fragment path
  (`mpp_coordinator.rs`'s `open_mpp_scan`, the builder's interim local
  ExchangeSender / ExchangeReceiver arms).
- [ ] Milestone 5: Go's `tiflashtest` tests on the unistore stack, and the
  register (M01 closed; M04's remainder restated).
- [x] Milestone 6 (register O10, outside M01; resolved 2026-11-10): Go's one `context.Context` for
  internal SQL. Go cancels and carries values in one context; the port split
  it (`tidb_gocontext::Context` cancels, `tikv_client` `TraceContext` holds
  values), so a cancelled DXF caller cannot stop internal SQL. Steps: (1)
  `tidb_gocontext::Context` gains Go `WithValue` / `Value` (done
  2026-11-10); (2) internal SQL reads a Go context's trace value
  (`tidb_txnkv::TraceContextKey`, Go keeps it in `kv`) and returns `ctx.Err()` before each
  statement as Go `executeStmtImpl` does (done 2026-11-10); (3) every DXF
  `TaskManager` method, the scheduler's and task executor's interfaces with
  their mocks, and the handle take Go's `ctx` (`WithNewTxn` with Go's
  internal source, its rollback on a background context) (done
  2026-11-10); (4) Go `TestCancelAndExecIdChanged` and
  `TestWithNewTxnRollbackOnCanceledCtx` pass (done; Go's `sleep` watches
  only the SQL killer, so the latter runs its 10-second statement as Go's
  does); (5) TTL's session takes Go's `ctx` (`ExecuteSQL` under
  `kv.WithInternalSourceType(ctx, ...)`, `RunInTxn`, `NewScanSession`,
  `ExecuteSQLWithCheck`, `EvalExpireTime`), with background contexts where
  Go uses `context.Background()` / `TODO()` (done 2026-11-10).

## Surprises & Discoveries

- 2026-11-10: the first end-to-end run found three port bugs the unit tests
  had not: Rust `HandleMPPDAGReq` went through `buildDAG` and refused a
  receiver-only task's empty ranges (Go never calls `buildDAG` there);
  `aggExec.processAllRows` lacked Go's decimal-to-BIGINT conversion of the
  final count's `sum`; and `getGroupKey` / `useDefaultEncoding` /
  `toTiPBChunk` failed on the decimal truncation Go's `sc.HandleError`
  downgrades (Go's `EncodeValue` answers bytes and error together; the
  codec now has that shape, `encode_value_in_timezone_with_error`).
- 2026-11-10, from the next Go tests: Go's MPP `tableScanExec` reads through
  `dbreader.DBReader.Scan`, which never consults the lock store (only the
  coprocessor path checks locks); the Rust reader went through
  `MVCCStore.Scan`'s lock collection and aborted on an async-commit
  secondary (`MvccStore::db_reader_scan` now). Go casts arithmetic operands
  in the function class (`newBaseBuiltinFuncWithTp`), so the planner's
  `NewFunctionInternal(ast.Div, ...)` (avg -> sum / count) gets
  `cast(case(...) as decimal)`; Rust cast them only in the AST rewriter
  (`wrap_arithmetic_operands`, now shared with `new_function`). And `ToPB`
  must read correlated columns' current values (`conOrCorColToPBExpr`):
  an Apply's inner MPP reader re-dispatches per outer row.

- 2026-11-10: the planner side is already Go's (multi-fragment EXPLAIN);
  `pkg/store/copr/mpp.go` is ported (`tidb-copr` `mpp_client.rs`). The
  missing pieces are `ToPB`, fragments, the embedded store's exchanges and
  the coordinator.
- Rust readers build their DAG from the access path's scan request rather
  than from `ToPB` on the pushed plan (register row E15). Milestone 1 is the
  shared root of M01 and E15; this plan uses it for MPP only, and E15 moves
  the TiKV readers onto it.

- `PhysicalTableScan` had no `Table` / `Columns` (Go's `*model.TableInfo`
  and `[]*model.ColumnInfo`); readers looked the table up again by name. The
  scans built from a `DataSource` now copy both, as Go's
  `getOriginalPhysicalTableScan` does.

- Go's gogoproto non-nullable fields are always encoded, zero included
  (`tipb.Executor`'s fine-grained shuffle fields, `ColumnInfo.pk_handle` /
  `array`, `TableScan.next_read_engine`); prost's `Option` omits `None`, so
  the lowering sets them. A byte comparison against a Go probe is what
  catches these.
- 2026-11-10, a planner divergence found on the way (not this plan's):
  `explain select /*+ read_from_storage(tiflash[t]) */ * from t where a + 1 >
  20 limit 100` on a pseudo-stats table plans Go's shape but Go's scan
  estimate is 125.00 (the limit's expected count over the selection) where
  this port's is 10000.00, and Go's plan ids run two higher
  (`TableFullScan_14` against `_12`). Fixed 2026-11-10: Rust's
  `ExhaustPhysicalPlans4LogicalLimit` never appended Go's MPP candidate
  (its doc assumed no TiFlash), and the TiFlash path built its scan with
  unscaled table stats where Go's `getOriginalPhysicalTableScan` serves both
  paths (`original_table_scan_stats`, `adjust_count_after_access`); the
  estimates are Go's (`mpp_limit_scales_the_tiflash_scan_like_go`). The ids
  were one short of Go's in every MPP plan: Go's `convertToTableScan` draws
  the TiFlash scan's id (`getOriginalPhysicalTableScan`) before refusing the
  path under a cop property; the Rust arm now draws it at the same point,
  and the LIMIT and scalar-aggregate plans are Go's to the id. The MPP
  aggregate plans are Go's to the id too (2026-11-10): Go's DataSource
  prices the unenforced property by re-entering `findBestTask`, where a
  required MPP partitioning is invalid before any scan is built (Rust
  converted the TiFlash scan under it), and Go's `getStreamAggs` refuses an
  MPP property first (Rust built two stream aggregates under it). Go's
  `exploreEnforcedPlan` (fix control 46177) is wired as well.
- 2026-11-10: `inject_extra_projection` skipped a TiFlash reader's table
  plan and the MPP union arm (written when the planner built no MPP plans),
  so an MPP aggregate's `SUM` lacked `WrapCastForAggArgs`' cast and Go's
  projection below it, and an MPP union's child lacked its cast projection.
  Both are Go's now (`mpp_aggregation_injects_gos_projection_below_the_aggregate`).
  Still open: a grouped MPP aggregate's plan numbers run one past Go's (see
  the LIMIT note below), so those tests compare MPP plans with numbers
  blanked.

- The join's bytes exposed three shared mismatches, fixed at their source:
  `NewFunction` kept the caller's `tinyint` for a planner-built `not()`
  (Go keeps the builtin's `bigint(1)`); a numeric-only call recorded no
  collation (Go's is `binary`, which the join's probe types carry); and the
  physical joins kept the logical `Join` TP, so `ExplainID` said `Join_N`.
  The window's bytes exposed a fourth: the pushdown catalog's casts carried
  no `val` (Go encodes each cast's `InUnionMetadata`) and no `has_distinct`.

## Decision Log

- 2026-11-10: M01 after O10 (DXF). O10's last gap, internal SQL honouring a
  cancelled caller, needs the Go-context port unified first (the register
  row explains the split); it is not this plan's.
- `ToPB` first: fragments and the coordinator are meaningless without
  per-operator lowering, and every later milestone's test needs it.

## Outcomes & Retrospective

Nothing yet.

## Context and Orientation

- Planner operators: `rust/crates/tidb-planner-core-operator-physicalop`
  (`PhysicalExchangeSender`, `PhysicalExchangeReceiver`, the base
  `to_pb`). Go: `pkg/planner/core/operator/physicalop`.
- The executor's current MPP path: `rust/crates/tidb-executor/src/mpp_coordinator.rs`
  (single fragment), `dag_request.rs`, `cop_reader.rs`,
  `driver/physical_builder.rs`.
- The MPP client: `rust/crates/tidb-copr/src/mpp_client.rs` (Go
  `pkg/store/copr/mpp.go`); the store interface `rust/crates/tidb-txnkv/src/mpp.rs`
  (Go `kv.MPPClient`).
- The embedded store: `rust/crates/tidb-unistore-cophandler` (`mpp.rs`,
  `mpp_exec.rs`). Go: `pkg/store/mockstore/unistore/cophandler`.
- Tests on the embedded store with a mock TiFlash replica:
  `unistore_cop/tiflash_partition.rs` (`UpdateTiFlashReplicaStatus`), the
  MPP EXPLAIN tests in `unistore_cop.rs`.

## Plan of Work

Each milestone ports whole Go files, keeps Go's names, and copies Go's tests
and vectors. Milestone 1 adds a `to_pb` per operator returning
`tipb::Executor`, the way Go's do, with the expression lowering the pushdown
catalog already provides. Milestone 2 ports `fragment.go` into the planner
operator crate beside the exchanges. Milestone 3 follows Go's cophandler
files. Milestone 4 replaces `mpp_coordinator.rs`'s hand-built fragment with
Go's coordinator over the fragments; `check_mpp_request`'s refusals go away
with it. Milestone 5 ports the tests and updates the register.

## Concrete Steps

From `rust/`, with `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0
CARGO_PROFILE_TEST_DEBUG=0 CARGO_CACHE_AUTO_CLEAN_FREQUENCY=never`:
`cargo check -j12 -p <crate> --tests` while iterating; `cargo nextest run -j12
-p <crate>` per milestone; `cargo build -j12 --locked -p tidb-server` before a
push.

## Validation and Acceptance

Per milestone, Go's own tests for the ported files pass, and a mutant of each
ported decision fails one of them. Acceptance: Go's `tiflashtest` queries
return Go's rows on the unistore stack, and the MPP EXPLAIN tests still
match.

## Idempotence and Recovery

Every step is additive until Milestone 4 swaps the coordinator; until then
the single-fragment path keeps serving what it serves today.

## Interfaces and Dependencies

`to_pb(&self, ctx, store_type) -> Result<tipb::Executor, PlanError>` on each
physical operator; `generate_root_mpp_tasks` returning Go's `Fragment`s;
the coordinator over `tidb_txnkv::MppClient`.
