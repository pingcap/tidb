# Table storage out of `TableCommon`: one shared InfoSchema

This ExecPlan is a living document. Keep Progress, Surprises & Discoveries,
Decision Log and Outcomes & Retrospective current as work proceeds. It follows
`PLANS.md` at the repository root.

## Purpose / Big Picture

In Go TiDB, a table object (`table.Table`, implemented by
`pkg/table/tables.TableCommon`) is immutable schema metadata. Every read or
write takes the transaction as an argument, for example
`tbl.AddRecord(ctx, txn, row)` and `tbl.RemoveRecord(ctx, txn, h, row)`.
Because tables hold no transaction state, one `infoschema.InfoSchema`, built
once per schema version, is shared by every session.

The Rust rewrite (`rust/`) differs. `tidb_table::tables::TableCommon` owns a
`store: Box<dyn TableStorage>` field (`crates/tidb-table/src/tables.rs`), and
about 90 of its methods read or write through `self.store`. Each session's
uncommitted writes therefore live inside that session's own `TableCommon`
copies. So every session builds its own catalog: `ClusterSession` clones
template tables from `KvTableTemplates` (`crates/tidb-domain-core/src/
cluster_session.rs`) and swaps in its own store with `replace_storage`.

After this plan, `TableCommon` holds no storage. Table operations take the
session transaction's store as a parameter, as in Go. One `InfoSchema` per
schema version can then be shared by all sessions, and the separately loaded
`ClusterCatalog` image can be deleted.

Observable outcome: the integration corpus gate
(`cargo test -p tidb-server --lib integration_corpus_matches_recordings --
--ignored`) still reports `157 topics, 3 divergences (3 stale recordings), 0
refused`. A node with many connections holds one catalog image, not one per
connection, which is visible as lower resident memory on the hzbank-style
schema of about 700 tables.

## Terms

- Store: an implementation of `tidb_kv::storage::TableStorage`, the
  key/value seam a table reads and writes through (`get`, `iter`, `set`,
  `delete`, staging). In a session it is the transaction's buffered view.
- Template: a `TableCommon` built once per schema version and cloned per
  session (`KvTableTemplates`).
- Operation-scoped pair: an immutable `&TableCommon` together with a
  `&mut dyn TableStorage` passed to one call. `PointRead<'a> { table:
  &'a TableCommon, store: Box<dyn TableStorage> }` in `tables.rs` already
  has this shape.

## Plan of Work

The double borrow is the key constraint. A method taking `&self` plus a
store held by `self` cannot compile. So the migration moves each operation
to take `&self` (or `&mut self` only for metadata) plus an explicit
`store: &mut dyn TableStorage`. Meanwhile callers temporarily pass a store
they hold apart from the table. Each milestone converts one operation family
end to end and is merged only with the gate green.

Milestone 1 (prototype, reads): convert the record-read family (point get,
batch get, record scan) to take the store as a parameter. Callers in
`tidb-executor` (`kv_table/table_scan.rs`, `point_get`) obtain the store
from the session transaction rather than from the table. Validate with the
executor and session suites and the corpus gate. Record in Surprises how the
executor reaches the session's store; this decides the shape of the later
milestones.

Milestone 2 (writes): convert `insert_row*`, `update_row*`, `delete_row*` and
index maintenance (`tables/index_entries.rs`, `index_states.rs`,
`nonpublic_columns.rs`, `partition_reorg.rs`) the same way.

Milestone 3 (DDL and maintenance): convert backfill (`tidb-ddl-build/src/
table_backfill.rs`), `cleanup_index`, `drop_index`, `truncate`,
statistics row counts, and the copr and admin users.

Milestone 4a (temporary tables through the transaction, Go
`pkg/table/temptable` + `session.commitTxnWithTemporaryData`): today each
local temporary table owns a `MemTableStorage` and each global temporary
table's rows are a `TemporaryTableTxnData` swapped into the shared table
object around every statement (`TemporaryTableOverlay` in
`crates/tidb-session/src/txn.rs`). Go has neither. Its temporary-table
writes go into the session transaction's membuffer like any other table's.
At commit, `commitTxnWithTemporaryData` copies each modified LOCAL
temporary table's keys from the membuffer into `SessionVars.
TemporaryTableData` (a session MemBuffer with per-table sizes, staged so a
failed commit leaves it unchanged), and `temporaryTableKVFilter` keeps every
temporary key out of 2PC; GLOBAL temporary keys simply die with the
transaction. Reads never reach TiKV: `buildTableReader` makes a temporary
table's reader a dummy, and the reader under `UnionScan` (`mem_reader.go`
around line 627) serves the membuffer merged with `TemporaryTableData` for
a local temporary table; the snapshot interceptor
(`TemporaryTableSnapshotInterceptor`, `interceptor.go`) answers point and
range reads of temporary keys from the session data (local) or nothing
(global). In Rust the interceptor is a `ClusterSnapshot` wrapper
(`crates/tidb-kv/src/cluster_storage.rs` is Go's `kv.Snapshot`), the session
data is a session-level store with per-table sizes, and the commit protocol
gains the filter and the copy. Then `TemporaryTableOverlay`,
`TemporaryTableTxnData`, `swap_temporary_txn_data` and
`truncate_local_temporary_table`'s store replacement all go (TRUNCATE of a
local temporary table deletes its session-data range and takes a new id, as
Go's `TruncateLocalTemporaryTable` does).

Milestone 4: delete the `store` field, `replace_storage`, `swap_storage`,
`store`, `store_mut` and the `TemporaryTableTxnData.store` duplication. A
GLOBAL temporary table's rows then live in the session transaction, as Go's
`TxnCtx.TemporaryTables` does.

Milestone 5: publish one built `InfoSchema` per schema version from the
node's `InfoCache`. Delete `KvTableTemplates` and the per-session catalog
build in `cluster_session.rs`.

Milestone 6: move the readers of `tidb_infoschema::cluster_catalog::
ClusterCatalog` (DDL planner, stats, `tidb-syssession`; about 83 files) onto
the shared `InfoSchema`'s `SchemaByName` / `TableByName`. Then delete
`ClusterCatalog`.

## Validation and Acceptance

For each milestone:
- `cargo check -p <crate> --tests` for each touched crate.
- `cargo test -p tidb-table --lib`, `-p tidb-executor --lib`,
  `-p tidb-executor --test all`.
- `cargo test -p tidb-session --lib`, rerunning any failure alone (this
  suite has known parallel-only flakes).
- The corpus gate above.

Always build with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
CARGO_INCREMENTAL=0`.

## Progress

- [x] Milestone 1: record-read family takes the store as a parameter
  (2026-11-01). `TableCommon::{stored_record, stored_records_batched,
  scan_iterator, point_reader, lookup_unique*, count_keys_read}` take the
  store; the 30 `KvTableReader` methods that reach storage take
  `store: &mut dyn TableStorage`; every reader leaf (`TableScanExec`,
  `HandleSourceExec`, `UniqueIndexPointSourceExec`, `IndexRangeSourceExec`,
  `IndexJoinLookupExec`, `IndexMergeReaderExecutor`) owns a `store` given by
  its builder; `ReaderStorage::table` returns the reader's store instead of
  cloning the table to swap one in; `StatementLocker` holds the statement's
  store, not a table. `TableCommon::lock_keys_for_read` / `do_lock_keys`
  are gone (callers lock through the store).
- [x] Milestone 2: write family (2026-11-02). Every write helper inside
  `tidb-table` (`insert_row_in`, `update_row_in`, `delete_row_in`,
  `delete_old_row_in`, `with_txn_staging`, the index entry writers and
  removers, the non-public index and column paths, the partition-reorg
  double writes, `row_conflicts_reported_as`, the prefetches) takes
  `store: &mut dyn TableStorage` and never reads `self.store`. The public
  entry points (`insert_row*`, `add_record_with_record_id`,
  `update_row_with_*`, `delete_row_with_*`, `row_conflicts*`) are the
  boundary: each lends the table's own store through
  `TableCommon::with_own_store`, which moves it out for the call (a
  `DetachedStorage` that panics stands in) and puts it back even on unwind.
  `KvTablePartitionReorg::reorg_partition_batch` takes the DDL worker's
  store directly, as Go's backfill takes the job's transaction.
- [x] Milestone 3: DDL, backfill and maintenance users (2026-11-02).
  `KvTableBackfill` (index backfill, global-index cleanup, temp-index merge,
  MODIFY COLUMN rewrite, record ranges) and `KvTablePartitionReorg` take
  the DDL worker's store, as Go's backfill workers take the job's
  transaction. The in-process `KvTableDdl` (create/backfill/recover index)
  and `KvTablePartitionMaintenance` take the store; so do
  `TableCommon::{cleanup_index, drop_index, index_entry_records_for_check,
  index_entries_for_check}`. Catalog callers lend the table's own store
  through `with_own_store` until Milestone 4.
- [ ] Milestone 4a: temporary tables through the transaction (design above).
  - [x] (2026-11-02, eea56309e09) Go `TemporaryTableData` ported to
    `tidb_variable::temporary_table_data`; Go
    `TemporaryTableSnapshotInterceptor` ported to
    `tidb_temptable::interceptor` as a `ClusterSnapshot` wrapper, with Go's
    `interceptor_test.go` cases (a limited scan is drained batch by batch
    and must equal the unlimited one).
  - [ ] Session: hold `TemporaryTableData`; wrap the transaction snapshot in
    the interceptor when the infoschema has temporary tables.
    The choke point is `tidb_session::statement_lifecycle::bind_snapshot`
    (every statement snapshot is bound there; Go installs the interceptor
    at the same moment, `sessiontxn/isolation/base.go:251`). Installing it
    alone changes nothing observable while temporary tables still write to
    their own stores, so this step lands together with the write switch.
  - [x] (2026-11-02) Go `temporaryTableKVFilter` ported
    (`session_commit_protocol::TemporaryTableKVFilter`, installed when
    `SessionTransactionOptions.temporary_tables` is set). The id set is a
    live `TemporaryTableIds` the session refreshes, because Rust applies
    transaction options when the transaction begins while Go sets the
    filter in `doCommit`.
  - [ ] The write switch, which must land as one change:
    1. `Session` holds its transaction store (`ClusterTableStorage` clone,
       set where `open_storage_session` builds it) and the live
       `TemporaryTableIds`; `TxnManager::statement_transaction_options`
       adds `with_temporary_tables`.
    2. `TemporaryTableOverlay::run` gives each attached LOCAL table the
       transaction store (`replace_storage`), so its rows go to the buffer.
    3. GLOBAL tables keep the catalog's session store; the overlay swaps
       only Go `TxnCtx.TemporaryTables`' allocators (drop
       `TemporaryTableTxnData.store`).
    4. `statement_lifecycle::bind_snapshot` refreshes the id set and wraps
       the snapshot in `session_snapshot_interceptor` (global ids from the
       session's infoschema, memoized per schema version; local ids from
       `local_temporary_tables`; `temporary_table_data`).
    5. Commit: Go `commitTxnWithTemporaryData` -- before the buffer
       commits, copy each LOCAL table's buffer range into
       `TemporaryTableData` under a stage; release it after a successful
       commit, clean it up otherwise.
    6. Remote scans refuse temporary tables (Go builds a dummy
       `TableReader` for them).
    7. Savepoints keep only the buffer checkpoint (drop `local_temporary`
       and `global_temporary` row images).
    8. `tmp_table_max_size`: Go `checkTempTableSize` over
       `TxnCtx.TemporaryTables[id].GetSize()` (committed size from
       `TemporaryTableData.GetTableSize` plus the transaction's delta,
       `handleTempTableSize` around each `AddRecord`), replacing the
       per-table `store.size_bytes()`.
  - [ ] Delete `TemporaryTableOverlay`, `TemporaryTableTxnData`,
    `swap_temporary_txn_data`.
- [ ] Milestone 4: the `store` field is gone.
- [ ] Milestone 5: one `InfoSchema` per schema version, shared by sessions.
- [ ] Milestone 6: `ClusterCatalog` is gone.

## Surprises & Discoveries

- 2026-11-01: 118 call sites outside `tidb-table` touch a table's store
  directly (`replace_storage`, `store`, `store_mut`). The heaviest are
  `tidb-ddl-build/src/table_backfill.rs` (15),
  `tidb-executor/src/kv_table/table_scan.rs` (7) and
  `tidb-copr/src/cop_store.rs` (7).

- 2026-11-01: In cluster mode a table's store is a clone of the session's
  `ClusterTableStorage` (`crates/tidb-store-driver-txn/src/
  cluster_storage.rs`): the mutation buffer and statement snapshot are
  shared by every table of the session (`cluster_session.rs` binds
  `storage.clone_box()` per table). The one per-table difference is
  `truncated`, a Rust-only flag that refuses statements on a table this
  session truncated. Go instead swaps in a new table with a new id, so that
  flag goes away with the field. So the session-wide store already exists,
  and Milestone 1 readers take it from the session.
- 2026-11-01: Reader leaves that read a different view (snapshot only, or
  buffered entries only) clone the table to swap the store in
  (`ReaderStorage::table` in `crates/tidb-executor/src/driver/
  physical_builder.rs`, `replace_storage`). With storage as a parameter, they
  pass that view directly and stop cloning tables.
- 2026-11-01: The embedded tier's `MemTableStorage` is per table, but it
  stores full TiKV-format keys (`tidb_tablecodec::table_key::
  encode_row_key_with_handle`, `t{tid}_r{handle}`). So one store per node,
  with each session's staging over it, can serve every table, as Go's single
  KV store does. Milestone 1 does not need a key-format change.

- 2026-11-01: `Session::new()` already runs on the mock store
  (`new_on_mock_store`), so production and almost every test session read
  through the session-wide `ClusterTableStorage`. A per-table
  `MemTableStorage` that holds rows survives only in temporary tables,
  memtables and unit fixtures.
- 2026-11-01 (Milestone 1): where a caller holds only a catalog table, the
  transitional store source is `table.store().clone_box()`. For
  `ClusterTableStorage` that is the session's shared handle, exactly what
  the table read before; for a per-table `MemTableStorage` it is a copy,
  which reads see identically. Milestone 4 replaces that one expression with
  the session's store. Write paths inside `tidb-table` still read through
  `stored_record_in_own_store` until Milestone 2 passes them the store.
- 2026-11-01: `count_keys_read` now judges the READER's store
  (`reads_processed_keys`): before, a union scan's mem reader got the
  membuffer store swapped into its table copy, and with the swap gone the
  table's own store would have counted membuffer reads as processed keys.

- 2026-11-02: a `self` / `.store` access split over two lines escapes a
  one-line search. `drop_index` and `index_entry_records_for_check` had
  one, and both would have hit `DetachedStorage` once called inside
  `with_own_store` (none of Milestone 2's write paths reach them). Search
  with `grep -B1 '^\s*\.store\b'` as well as `self\.store`. Remaining
  own-store readers: `partition_row_counts`,
  `stats_row_count_for_physical_id`, `stored_keys`,
  `cache_storage_size_exceeds`, `len`, `is_empty`, `stored_raw_value`,
  `truncate*`, and the boundary accessors.
- 2026-11-02: what is left for Milestone 4, counted by marking the
  own-store methods `#[deprecated]` and compiling: 82 library call sites and
  359 in all (104 of them `insert_row` in test fixtures).

## Decision Log

- 2026-11-01: Storage comes out before the catalog is shared. Sharing first
  is impossible while each session's uncommitted writes live in its own
  `TableCommon` copy.
- 2026-11-01: Convert by operation family, with a store parameter on
  `&self` methods, following `PointRead`'s shape. The alternative, splitting
  `TableCommon` into a metadata struct plus a store-owning wrapper, keeps
  the Go mismatch in the type and only moves it.

## Outcomes & Retrospective

(none yet)
