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
  ExchangeSender, ExchangeReceiver, HashAgg and StreamAgg (Go
  `agg_to_pb.go`'s plan-to-PB half in `tidb-expression-aggregation`); a
  planned MPP fragment's `ToPB` is Go's to the byte
  (`unistore_cop/mpp_to_pb.rs`, Go testkit probe vectors). Left: hash join,
  sort, expand, window, CTE, index scan, local index lookup, the TiFlash
  partition scan.
- [ ] Milestone 2: Go `fragment.go` (`Fragment`, `GenerateRootMPPTasks`,
  `mppTaskGenerator`: fragments, union-all untwisting, CTE task counts,
  `constructMPPTasksImpl` over `MPPClient.ConstructMPPTasks`, partition
  pruning), with Go's fragment tests.
- [ ] Milestone 3: the embedded store's MPP (Go
  `pkg/store/mockstore/unistore/cophandler/mpp.go` / `mpp_exec.go`): tunnels
  between tasks, `ExchangeSender` (PassThrough, Broadcast, HashPartition),
  `ExchangeReceiver`, the MPP joins and `Expand`, and the RPC server's
  `EstablishMPPConnection` over them.
- [ ] Milestone 4: Go's local MPP coordinator and `MPPGather`
  (`pkg/executor/internal/mpp/local_mpp_coordinator.go`,
  `executor_with_retry.go`, `recovery_handler.go`, `pkg/executor/mpp_gather.go`)
  replacing the single-fragment lowering: the reader keeps its physical plan,
  the coordinator dispatches every fragment's tasks and reads the root's.
- [ ] Milestone 5: Go's `tiflashtest` tests on the unistore stack, and the
  register (M01 closed; M04's remainder restated).

## Surprises & Discoveries

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
  (`TableFullScan_14` against `_12`).
- 2026-11-10: `inject_extra_projection` skipped a TiFlash reader's table
  plan and the MPP union arm (written when the planner built no MPP plans),
  so an MPP aggregate's `SUM` lacked `WrapCastForAggArgs`' cast and Go's
  projection below it, and an MPP union's child lacked its cast projection.
  Both are Go's now (`mpp_aggregation_injects_gos_projection_below_the_aggregate`).
  Still open: the MPP exploration numbers plans and columns differently from
  Go (`select a, count(*) from t group by a`: `TableFullScan_21` against
  Go's `_22`), so those tests compare MPP plans with numbers blanked.

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
