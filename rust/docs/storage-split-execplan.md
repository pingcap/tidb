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
- [ ] Milestone 2: write family.
- [ ] Milestone 3: DDL, backfill and maintenance users.
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
