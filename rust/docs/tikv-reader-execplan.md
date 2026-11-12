# Read TiKV through Go's reader executors over a ToPB DAG

This ExecPlan is a living document. Keep Progress, Surprises & Discoveries,
Decision Log and Outcomes & Retrospective current as work proceeds. It follows
`PLANS.md` at the repository root.

## Purpose / Big Picture

In Go TiDB a reader is one executor per plan node: `buildTableReader`,
`buildIndexReader` and `buildIndexLookUpReader` (`pkg/executor/builder.go`)
lower the reader's pushed plan with `constructDAGReq` -- one `ToPB` per
pushed node, flattened by `ConstructListBasedDistExec` -- and send it through
`distsql.Select`. A transaction's buffer is a separate `UnionScanExec` over
the reader. Porting a Go pushdown is therefore one `ToPB` method.

The Rust TiKV readers do not have that shape. The physical builder builds a
reader's pushed nodes as local executors and *offers* them to the scan
(`kv_table/table_scan.rs`, `remote_scan.rs`, `cop_reader.rs`), each source
re-deriving the request from the offers it accepted, and a union scan's
buffer is merged inside the scan (`remote_scan.rs`'s staged rows). Every new
pushdown, runtime-stats field or memory charge needs plumbing through the
offers (register row E15); a TiFlash read under a dirty transaction still
lowers to a single MPP fragment (`open_mpp_scan`, row M01).

The TiFlash path already has Go's shape: `table_reader.rs`
(`TiFlashTableReaderExecutor`, `buildNoRangeTableReader`,
`buildRespForGroupedRanges`) over `local_mpp_coordinator::construct_dag_req`
(the tree-based `ToPB`), and `builder_utils.rs` has Go's list-based
`ConstructListBasedDistExec` for the `kv.TiDB` store. After this plan every
TiKV reader is Go's executor over a `ToPB` DAG, the union scan is Go's
`UnionScanExec` over a reader, the offer machinery and `open_mpp_scan` are
deleted, and row E15 leaves the register.

Observable: the session, executor and planner suites and the integration
corpus replay pass unchanged in result and EXPLAIN ANALYZE output, with the
reader built by Go's `buildTableReader` / `buildIndexReader` /
`buildIndexLookUpReader`.

## Progress

- [x] (2026-11-12) Milestone 1: `TableReaderExecutor` serves a TiKV
  `TableReader` with no union scan: `construct_dag_req` for `kv.TiKV`
  (list-based `ToPB`), the reader's ranges, keep order, paging and runtime
  stats through `distsql.Select`; the physical builder routes every such
  reader on a store with a coprocessor client to it.
- [x] (2026-11-12) Milestone 2: `IndexReaderExecutor` likewise (`index_reader.rs`), on
  `PhysicalIndexScan.ToPB` ported with the scan's `Table` and `Index`.
- [x] (2026-11-12) Milestone 3: `IndexLookUpExecutor` (`index_lookup.rs`):
  the index worker (one request a key-range group, merged by the order-by
  columns across groups, one pooled decode chunk), table workers over Go's
  `tableRequest` with the handles' ranges and hints, Go's per-task memory
  charges, index usage and runtime stats.
- [ ] Milestone 4: the index join's inner side is Go's
  `dataReaderBuilder.BuildExecutorForIndexJoin`, and a union scan there is
  `buildUnionScanForIndexJoin`'s `UnionScanExec` over the task's reader; the
  staged-row merge (`ReaderStorage::Merged`) and `open_mpp_scan` are deleted.
  Steps: (a) `TableReaderExecutor` gains Go's `buildTableReaderBase` shape --
  a `dagPB` built once and a request over given KV ranges or handles
  (`buildTableReaderFromKvRanges` / `FromHandles`) -- and the lookup's table
  task reads through it; (b) tidb-executor-join's inner worker calls a
  `BuildExecutorForIndexJoin` builder per task (Go `innerWorker.fetchInnerResults`)
  in place of `IndexLookupSource`'s re-seeded probes; (c) the driver
  implements the builder per Go's `buildExecutorForIndexJoinInternal` arms
  over `buildKvRangesForIndexJoin` (lookup contents into the join's index
  ranges, `ColWithCmpFuncManager.BuildRangesByRow`, `UnionRanges`); (d) the
  probe-offer leaf (`IndexJoinLookupExec`, `SharedIndexJoinProbes`) is
  deleted with its tests' Go-pinned cases moved to the session suite.
- [ ] Milestone 5: the offer machinery is deleted; E15 leaves the register.

## Surprises & Discoveries

- 2026-11-12: the in-process engine (`tidb-kv` `TableStorage` with no
  `client()`), which the executor crate's unit tests and some session tests
  run on, has no coprocessor, so Go's reader cannot serve it. Go has no such
  store (its tests use unistore). Its readers keep the local path until the
  A<->B merge (memory: rust-rewrite-ab-merge-plan) deletes the engine.
- 2026-11-12: the local path had hidden Go divergences elsewhere that the
  Go-shaped reader exposed: the non-prepared plan cache's executors never
  saw its literals (Go `PlanCacheParams`); a generated column's expression
  did not carry the generated columns it reads, so `ExpandVirtualColumn`
  missed their base columns; a hash join took its probe chunks from the
  `AllocPool` where Go's `NewFirstChunk` does not (a Go allocator probe
  under `MustQuery`, which resets the pool per statement, settled it; a
  probe through `tk.Exec`, which does not reset, misleads); and the copr
  runtime refused a request over no range, where Go builds no task.
- 2026-11-12 (M3): the IndexLookUp reader exposed planner fields Go has and
  Rust lacked or dropped: `PhysicalIndexLookUpReader.ExtraHandleCol` (and
  `PhysicalTableScan.AppendExtraHandleCol`, which also appends the column's
  info to `ts.Columns`; `AddExtraPhysTblIDColumn` likewise, after the
  handle); `SortItem`s that named their column by UniqueID alone, so a
  partition merge's ByItems carried no column ID (`SortItem::new` now takes
  the column, as Go's `SortItem{Col}`; only the merge-join enumeration,
  which keeps its join keys as UniqueIDs, still builds ID-only items); and
  the reader's own schema, which in Go is its table plan's schema object, so
  `ResolveIndicesForVirtualColumn` binds both.
- 2026-11-12 (M3): `TableRead` now takes Go's `SetMemTracker` and
  `SetPaging` from its caller. The TableReader and IndexReader still hand
  each select result a fresh operator tracker under the statement's, where
  Go's reader holds one `memTracker` (reset and attached on `Open`) that its
  `kvRanges` are charged to as well; the lookup does it Go's way.

## Decision Log

- 2026-11-12: one reader at a time, each switched wholesale (no per-shape
  fallback to the offer path): a shape the new reader cannot serve is a
  failing test, which is the work list. The one split is by store: a store
  with a coprocessor client takes Go's reader, the in-process engine the
  local path.

## Outcomes & Retrospective

(not started)
