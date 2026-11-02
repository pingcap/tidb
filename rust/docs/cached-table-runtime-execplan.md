# Cached tables read from a leased in-memory copy, as Go's cachedTable

This ExecPlan is a living document. Keep Progress, Surprises & Discoveries,
Decision Log and Outcomes & Retrospective current as work proceeds. It follows
`PLANS.md` at the repository root.

## Purpose / Big Picture

`ALTER TABLE t CACHE` marks a small table as cached. In Go TiDB a cached
table is a `cachedTable` (`pkg/table/tables/cache.go`). It wraps the ordinary
table with three pieces of runtime:

- **Read lease.** A reader takes a READ lock with a lease in
  `mysql.table_cache_meta`. While the lease holds, no writer may commit, so
  the reader can load the whole table into memory once and answer reads from
  that copy. `TryReadFromCache` serves reads; `UpdateLockForRead` takes the
  lock and loads the copy asynchronously; `renewLease` extends the lease near
  its end.
- **Write lock.** A transaction that wrote the table (`AddRecord`,
  `UpdateRecord`, `RemoveRecord` record it in `TxnCtx.CachedTables`) takes
  the WRITE lock before it commits (`cachedTableRenewLease` in
  `session.go`). Taking it means waiting out every READ lease. The commit
  then refuses a commit ts beyond the write lease
  (`kv.CommitTSUpperBoundCheck`).
- **Lock protocol.** `state_remote.go` reads and writes the lock row through
  internal SQL.

The Rust rewrite has the DDL side (`ALTER TABLE ... CACHE/NOCACHE`, the
`table_cache_meta` row, the size admission) but none of this runtime. Rust
reads and writes cached tables as ordinary tables. Writers therefore never
wait for readers' leases: a Go node that serves the table from its cache
reads stale data after a Rust node writes it.

After this plan:

- A read of a cached table under a valid lease reads the in-memory copy.
  `StmtCtx.ReadFromTableCache` is set and the
  `tidb_server_read_from_tablecache_total` counter moves.
- A write waits for the WRITE lock and commits within its lease.
- `mysql.table_cache_meta` shows the READ / INTEND / WRITE states Go writes.

## Progress

- [x] (2026-11-03, d4ccd2bf340) M1: `tidb_table::tables::state_remote`,
  Go `state_remote.go`. Test `state_remote_moves_the_lock_meta_as_go` runs
  over a domain internal session.
- [x] M2: `tidb_table::tables::cache::CachedTable`, Go `cachedTable`.
  It holds the cache data, the lease, the size and the one lock handle (Go
  `tokenLimit`). Its methods:
  - `try_read_from_cache`;
  - `update_lock_for_read`, which loads in the background through
    `tidb_kv::storage::KvStorage`, Go `kv.Storage` with `RunInNewTxn`;
  - `renew_lease`;
  - `write_lock_and_keep_alive`, with `renew` and `lock_for_write`;
  - `check_write_size`.

  `TableCommon.cached` holds it. `cluster_table` (Go `TableFromMeta`)
  creates it for a non-partitioned table whose status is not DISABLE.
- [x] M3: the domain's image builder initializes it, as Go
  `tableFromMeta` does. The lock handle runs over
  `CachedTableSessionFactory` (Go `b.factory`), which takes a domain
  internal session at its first statement: images are built before the
  domain is running. An image built without a factory clears the runtime.
- [x] M4a: reads.
  - The union scan's `cached_table_data`, Go `handleCachedTable` /
    `getCacheTable`. Under the lease it reads the copy beneath the
    buffer, and the reader is an empty `TableDualExec` (Go `setDummy`).
  - Outside the lease it takes the read lock in the background, except
    under UPDATE, DELETE, INSERT or EXPLAIN.
  - `StmtCtx.ReadFromTableCache` is a session cell that each statement
    clears and `finish_statement_state` counts
    (`tidb_server_read_from_tablecache_total`).
  - `tidb_table_cache_lease` is `tidb_vardef::TABLE_CACHE_LEASE`, with its
    SetGlobal hook.
  - The planner's UnionScan condition is now Go's == ENABLE.
- [ ] M4b: point gets.
  - Go `PointGet` / `BatchPointGet` read `cacheTableSnapshot` under the
    lease. Rust point gets still read storage. That is correct, since
    writers hold the write lock, but they neither read the copy nor take
    the read lock.
- [x] M5: writes.
  - `TableCommon`'s three add entry points, its update and its two
    removes record the table in `TxnCtx.CachedTables`, through
    `MutateContext::add_cached_table_handle_to_txn`. Add and update first
    refuse a table over 64 MiB.
  - `txn_manager::CachedTableRenewLease`, Go `cachedTableRenewLease`, takes
    the write locks before each of the three commits and keeps them alive.
    Its `commit_ts_check` reaches the client's existing
    `set_commit_timestamp_upper_bound_check` through
    `SessionTransactionOptions` (autocommit) and
    `OpenClusterTransaction::set_commit_ts_upper_bound_check` (explicit).
  - `StmtCtx.WaitLockLeaseTime` feeds the slow log's `IsWriteCacheTable`.

## Surprises & Discoveries

- The Rust planner already puts Go's `LogicalUnionScan` over a cached table
  (`plan_builder.rs`). It tests `is_cached()` = status != DISABLE, where Go
  tests == ENABLE.
- The union scan's mem reader reads a `MemBufferSnapshot` copy of the
  transaction's buffer. Go's `getSnapIter` unions the cache under the buffer
  (the buffer wins, its tombstones hide cached rows). Overlaying the cache
  under that copy is the same union.

## Decision Log

- The lock handle runs SQL through `tidb_sqlexec::SqlExecutor`, as Go's
  `sqlExec` interface.
- A pooled internal session backs the handle. Go takes it from the domain's
  pool and never returns it.

## Outcomes & Retrospective

Tests: `crates/tidb-session/src/domain/cluster_session_node/tests/cached_table.rs`
(the lock protocol, the image's session, reads from the copy, and a write
waiting out a reader's lease, which fails with stale rows when the write lock
is skipped) and `tidb-table` `tables::cache::tests` (the runtime over fakes).
Not covered: the commit-ts upper bound refusing a commit whose write lease
expired (it needs a commit slower than five seconds), and the `BackoffSleep`
`waitLockLeaseForCacheTable` entry of the execution details.
