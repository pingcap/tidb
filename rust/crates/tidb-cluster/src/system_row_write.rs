// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Writing one row of a `mysql.*` table the way TiDB's own `INSERT`,
//! `UPDATE` and `DELETE` write it.
//!
//! A stored row is several things at once, and a writer that produces fewer
//! leaves it half-visible:
//!
//! 1. the record, under its handle;
//! 2. one entry per public index, since nothing backfills them;
//! 3. for a table with no clustered handle, the row-ID allocator key, so the
//!    next writer does not hand out a handle this one already used.
//!
//! None of it is encoded here. Every write goes through the one table-layer
//! writer, `KvTable` (Go `tables.TableCommon`), opened over the stored
//! `TableInfo` the way Go's `tables.TableFromMeta` opens it: `AddRecord`,
//! `UpdateRecord` and `RemoveRecord` produce the record, the index entries
//! and their assertions exactly as for a user table's `INSERT`, `UPDATE` and
//! `DELETE`, and this module hands their writes to the caller's transaction
//! as mutations. What stays here is what Go's executor does before the table
//! layer: materialising declared defaults and casting the caller's values to
//! their columns (`table.CastValue`).
//!
//! # Two handle shapes, because `mysql.*` has two — and which one is which
//! depends on the cluster's version
//!
//! A table with **no clustered handle** stores its rows under an allocated
//! `_tidb_rowid`, with every declared column in the row value:
//! [`insert_row`], [`update_row`], [`rewrite_rowid_row`], [`delete_row`].
//! Every account table is one of these, and so are the statistics tables on a
//! v8.5 cluster, where `mysql.stats_histograms` is still
//! `UNIQUE INDEX tbl(table_id, is_index, hist_id)` over a row ID.
//!
//! A **clustered** table's primary key *is* its handle, so those columns live
//! in the record key and are absent from the row value — the same rule the
//! reader states in `SystemTableView::project`. Recent TiDB declares
//! `mysql.stats_meta` `PRIMARY KEY (table_id) CLUSTERED` and its histogram
//! tables likewise: [`clustered_record_key`], [`store_clustered_row`],
//! [`delete_clustered_row`].
//!
//! Which set a caller needs is therefore a question about the *cluster's*
//! `TableInfo`, never about the table's name; `tidb_exec::cluster_stats_write`
//! asks [`crate::mysql_system_tables::HandleLayout::of`] per table for exactly
//! that reason.

#[allow(unused_imports)]
pub use tidb_table::origin_default::{
    encode_error, is_time_column, literal_default, materialise, origin_default, unmaterialisable,
    typed_value, RowEncodeError, CURRENT_TIMESTAMP,
};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};

use tidb_codec::table_key::encode_row_key_with_handle;
#[cfg(test)]
use tidb_datatype::{FieldTypeCode, GoString};
use tidb_datatype::{new_collation_enabled, Datum, FieldType, SessionTimeZone, Time};
use tidb_table::kv_table::{KvTable, TableHandle};
use tidb_kv::storage::{MemTableStorage, StorageError, StorageIterator, TableStorage};
use tidb_model::column::ColumnInfo;
use tidb_model::table_info::TableInfo;
#[cfg(test)]
use tidb_model::GoAny;
use tidb_tablecodec::{
    IndexColumn as CodecIndexColumn, IndexInfo as CodecIndexInfo, TableColumn as CodecTableColumn,
    TableInfo as CodecTableInfo,
};
use tidb_txnkv::transaction::{BufferMutation, BufferMutationOp, MutationSetError};
use tidb_txnkv::{AssertionOp, Key};

/// Go's own `Y`/`N` privilege spelling.
pub const YES: &str = "Y";
/// The `N` half of every `ENUM('N','Y')` privilege column.
pub const NO: &str = "N";

/// One row's values, by stored column ID — the shape both the row decoder
/// (`decode_table_row_to_map`) and the row encoder work in, so a row read back
/// from the cluster can be edited and written without a second vocabulary.
pub type RowValues = std::collections::BTreeMap<i64, Datum>;

/// Materialises every column of a row that does not exist yet, from the
/// table's declared `DEFAULT`s.
///
/// This is what an `INSERT` stores for a column the statement does not name.
/// The caller then overwrites the columns it does name. A stored row carries
/// *every* column, because that is what Go's own `INSERT` produces; leaving a
/// column out is what a real TiDB trips on, either as TiKV's "missing data for
/// NOT NULL column" or as a zero time it refuses to convert.
pub fn defaults_row(table: &TableInfo, now: Time) -> Result<RowValues, RowEncodeError> {
    let mut values = RowValues::new();
    for column in table.cols().iter_deref() {
        let column = column.read();
        values.insert(
            column.id,
            declared_default(&column, table_name(table), now)?,
        );
    }
    Ok(values)
}

/// The one table-layer writer (`KvTable`, Go `tables.TableCommon`) opened
/// over one stored `TableInfo` (Go `tables.TableFromMeta`), whose writes are
/// captured as mutations for the caller's transaction instead of a session
/// buffer.
///
/// A system-table row is written by the same `AddRecord`, `UpdateRecord` and
/// `RemoveRecord` a user table's `INSERT`, `UPDATE` and `DELETE` use, so the
/// record key, the row value, every index entry, the restored data of a
/// common handle and the assertions are the table layer's, never a second
/// encoding of them.
struct TableWriter {
    table: KvTable,
    writes: CapturedWrites,
    context: Arc<dyn tidb_table::context::MutateContext>,
    name: String,
}

impl TableWriter {
    fn open(info: &TableInfo, use_new_collation: bool) -> Result<Self, RowEncodeError> {
        Self::open_with(info, use_new_collation, Arc::new(tidb_table::context::StaticMutateContext::new()))
    }

    /// [`Self::open`] under `context`, the statement context the table layer
    /// sees.
    fn open_with(
        info: &TableInfo,
        use_new_collation: bool,
        context: Arc<dyn tidb_table::context::MutateContext>,
    ) -> Result<Self, RowEncodeError> {
        let writes = CapturedWrites::default();
        let table = crate::table_from_meta::cluster_table(
            info,
            &writes,
            &crate::table_from_meta::AutoIdSource::Unavailable,
        )
        .map_err(|reason| {
            encode_error(format!("{} cannot be written: {reason}", table_name(info)))
        })?
        .with_new_collation_mode(use_new_collation);
        // No time zone conversion: every `TIMESTAMP` that reaches this module
        // is ALREADY the UTC wall clock (a fresh value comes from
        // `mysql_bootstrap::utc_now_timestamp`, and an edited row's untouched
        // columns came back from a decode that did not convert either), so
        // the context's zone is UTC.
        Ok(TableWriter {
            table,
            writes,
            context,
            name: table_name(info).to_owned(),
        })
    }

    /// The row in the table's column order, each value cast to its column
    /// the way Go's `INSERT` casts it before `AddRecord` (`table.CastValue`):
    /// an `ENUM`/`SET` as its member(s), a string with the column's collation,
    /// so a new-collation key holds its sort key. `whole` refuses a missing
    /// column; otherwise it reads as NULL (a record key needs only its key
    /// columns).
    fn row(&self, values: &RowValues, whole: bool) -> Result<Vec<Datum>, RowEncodeError> {
        self.table
            .columns
            .iter()
            .map(|column| match values.get(&column.id) {
                Some(value) => Ok(collated_value(
                    typed_value(value, &column.field_type)?,
                    &column.field_type,
                )),
                None if !whole => Ok(Datum::Null),
                None => Err(encode_error(format!(
                    "{}.{} has no value in the row being written",
                    self.name, column.name
                ))),
            })
            .collect()
    }

    /// The handle a clustered table's row lives under; a `_tidb_rowid` table
    /// is refused rather than given some invented key.
    fn clustered_handle(&mut self, row: &[Datum]) -> Result<TableHandle, RowEncodeError> {
        self.table
            .clustered_handle_of_row(row, &SessionTimeZone::utc())
            .map_err(|error| encode_error(format!("{error:?}")))?
            .ok_or_else(|| {
                encode_error(format!(
                    "{} has no clustered handle; its rows are written under an allocated \
                     _tidb_rowid",
                    self.name
                ))
            })
    }

    /// Go `AddRecord` under the duplicate-key mode Go gives a plain `INSERT`
    /// in an optimistic transaction with `tidb_constraint_check_in_place` off
    /// (`optimizeDupKeyCheckForNormalInsert`): `DupKeyCheckLazy`, so the
    /// record and every unique entry are presumed absent and checked at
    /// prewrite.
    fn add(
        mut self,
        values: &RowValues,
        record_id: Option<i64>,
    ) -> Result<Vec<BufferMutation>, RowEncodeError> {
        let row = self.row(values, true)?;
        self.table
            .add_record_with_record_id(&row, record_id, true, &*self.context)
            .map_err(|error| encode_error(format!("{error:?}")))?;
        Ok(self.writes.take())
    }

    /// Go `UpdateRecord`: index entries move only where the indexed value
    /// did; an entry whose key is unchanged is left alone. An optimistic
    /// `UPDATE` checks a moved unique entry in place
    /// (`optimizeDupKeyCheckForUpdate`), so it asserts the destination
    /// absent.
    fn update(
        mut self,
        handle: Option<TableHandle>,
        existing: &RowValues,
        values: &RowValues,
    ) -> Result<Vec<BufferMutation>, RowEncodeError> {
        let old = self.row(existing, true)?;
        let new = self.row(values, true)?;
        let handle = match handle {
            Some(handle) => handle,
            None => self.clustered_handle(&new)?,
        };
        self.table
            .update_row_with_old_context(&handle, Some(&old), &new, &*self.context)
            .map_err(|error| encode_error(format!("{error:?}")))?;
        Ok(self.writes.take())
    }

    /// Go `RemoveRecord`. `whole` is false only for a table with no index,
    /// whose removal reads nothing past the record key.
    fn remove(
        mut self,
        handle: Option<TableHandle>,
        values: &RowValues,
        whole: bool,
    ) -> Result<Vec<BufferMutation>, RowEncodeError> {
        let row = self.row(values, whole)?;
        let handle = match handle {
            Some(handle) => handle,
            None => self.clustered_handle(&row)?,
        };
        self.table
            .delete_row_with_old_context(&handle, &row, &*self.context)
            .map_err(|error| encode_error(format!("{error:?}")))?;
        Ok(self.writes.take())
    }
}

/// A [`TableStorage`] with nothing stored that keeps every write the table
/// layer stages, with its assertion and lazy-check flags, as the mutation the
/// caller's transaction applies.
///
/// Reads answer "absent": a system-table writer has already read the rows it
/// rewrites at its own snapshot, so the table layer's duplicate checks are
/// left to the commit's assertions, exactly as the mutations state them.
#[derive(Clone, Debug, Default)]
struct CapturedWrites {
    writes: Arc<Mutex<Vec<BufferMutation>>>,
    presumed: Arc<Mutex<BTreeSet<Vec<u8>>>>,
}

impl CapturedWrites {
    fn take(&self) -> Vec<BufferMutation> {
        std::mem::take(&mut *self.writes.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn push(&self, mutation: Result<BufferMutation, MutationSetError>) -> Result<(), StorageError> {
        let mutation = mutation.map_err(|error| StorageError::Backend(error.to_string()))?;
        self.writes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(mutation);
        Ok(())
    }

    /// Go `kv.SetPresumeKeyNotExists`, marked before or after the value.
    fn presumed(&self, key: &[u8]) -> bool {
        self.presumed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(key)
    }
}

impl TableStorage for CapturedWrites {
    fn get(&mut self, _key: &Key) -> Result<Vec<u8>, StorageError> {
        Err(StorageError::NotFound)
    }

    fn get_local(&mut self, _key: &Key) -> Result<Vec<u8>, StorageError> {
        Err(StorageError::NotFound)
    }

    fn mark_presume_key_not_exists(&mut self, key: &Key) {
        self.presumed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key.as_bytes().to_vec());
        let mut writes = self.writes.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(position) = writes
            .iter()
            .rposition(|mutation| mutation.key() == key.as_bytes())
        {
            let (op, key, value, _, assertion) = writes[position].clone().into_parts();
            if op == BufferMutationOp::Set {
                if let Ok(marked) = BufferMutation::set_with_flags(key, value, true, assertion) {
                    writes[position] = marked;
                }
            }
        }
    }

    fn mark_presume_key_not_exists_with_hint(&mut self, key: &Key, _value: &str, _index: &str) {
        self.mark_presume_key_not_exists(key);
    }

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), StorageError> {
        self.set_with_assertion(key, value, AssertionOp::AssertNone)
    }

    fn set_with_assertion(
        &mut self,
        key: Key,
        value: Vec<u8>,
        assertion: AssertionOp,
    ) -> Result<(), StorageError> {
        let presumed = self.presumed(key.as_bytes());
        self.push(BufferMutation::set_with_flags(
            key.as_bytes().to_vec(),
            value,
            presumed,
            assertion,
        ))
    }

    fn set_with_constraint_check(
        &mut self,
        key: Key,
        value: Vec<u8>,
        assertion: AssertionOp,
        check_in_prewrite: bool,
    ) -> Result<(), StorageError> {
        if check_in_prewrite {
            // Go's pessimistic `NeedConstraintCheckInPrewrite`; a system-table
            // write never runs in a pessimistic statement.
            return Err(StorageError::Backend(
                "a system-table write has no pessimistic constraint check".to_owned(),
            ));
        }
        self.set_with_assertion(key, value, assertion)
    }

    fn delete(&mut self, key: Key) -> Result<(), StorageError> {
        self.delete_with_assertion(key, AssertionOp::AssertNone)
    }

    fn delete_with_assertion(
        &mut self,
        key: Key,
        assertion: AssertionOp,
    ) -> Result<(), StorageError> {
        let key = key.as_bytes().to_vec();
        self.push(match assertion {
            AssertionOp::AssertExist => BufferMutation::delete_existing(key),
            AssertionOp::AssertNone => BufferMutation::delete(key),
            other => {
                return Err(StorageError::Backend(format!(
                    "a delete carries no {other:?} assertion"
                )))
            }
        })
    }

    fn iter(
        &mut self,
        start: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Box<dyn StorageIterator>, StorageError> {
        MemTableStorage::new().iter(start, upper_bound)
    }

    fn key_count(&self) -> usize {
        0
    }

    fn clear(&mut self) {
        self.take();
    }

    fn clone_box(&self) -> Box<dyn TableStorage> {
        Box::new(self.clone())
    }
}

/// The mutations that write one row that does not exist yet, at `row_id`.
pub fn insert_row(
    table: &TableInfo,
    row_id: i64,
    values: &RowValues,
) -> Result<Vec<BufferMutation>, RowEncodeError> {
    insert_row_with_collation(table, row_id, values, new_collation_enabled())
}

/// [`insert_row`] with the cluster's persisted collation mode already
/// captured.
///
/// Go fixes this mode in the table/index encoder (`TableFromMetaWithCollate`).
/// Accepting it once here keeps the entry key, restored-data decision, and
/// entry value on one format even if the process-level source changes while
/// the mutations are assembled.
pub fn insert_row_with_collation(
    table: &TableInfo,
    row_id: i64,
    values: &RowValues,
    use_new_collation: bool,
) -> Result<Vec<BufferMutation>, RowEncodeError> {
    TableWriter::open(table, use_new_collation)?.add(values, Some(row_id))
}

/// Whether `table`'s rows are keyed by their primary key (an integer primary
/// key as the handle, or a common handle) rather than by an allocated
/// `_tidb_rowid`: Go's `tbl.Meta().PKIsHandle || IsCommonHandle`.
#[must_use]
pub fn is_clustered(table: &TableInfo) -> bool {
    table.pk_is_handle || table.is_common_handle
}

/// The stored row `key`/`value` of `table` as a whole row: the decoded value
/// and, for a clustered table, the primary-key columns that live only in the
/// record key (Go's `tablecodec.DecodeRowKey` handle columns).
pub fn decode_stored_row(
    table: &TableInfo,
    key: &[u8],
    value: &[u8],
    timezone: Option<&tidb_datatype::SessionTimeZone>,
) -> Result<RowValues, RowEncodeError> {
    let types = table
        .cols()
        .iter_deref()
        .map(|column| {
            let column = column.read();
            (column.id, column.field_type.clone())
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut values = tidb_tablecodec::decode_table_row_to_map(value, &types, timezone)
        .map_err(|error| encode_error(error.to_string()))?;
    if is_clustered(table) {
        let view = crate::mysql_system_tables::SystemTableView::project(
            table_name(table),
            table,
            &table
                .cols()
                .iter_deref()
                .map(|column| column.read().name.lowercase().to_owned())
                .collect::<Vec<_>>()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        );
        for (column, datum) in crate::mysql_system_tables::decode_handle_columns(&view, key, timezone)
            .map_err(|error| encode_error(error.to_string()))?
        {
            if let Some(column) = table.find_public_column_by_name(&column) {
                // A restored column is already there, as the original bytes.
                values.entry(column.read().id).or_insert(datum);
            }
        }
    }
    Ok(values)
}

/// The mutations that turn the row stored under `key` from `existing` into
/// `values` (Go `UpdateRecord`), for either handle shape: the record is
/// rewritten in place, and an index entry moves only where its indexed value
/// did.
pub fn update_row(
    table: &TableInfo,
    key: &[u8],
    existing: &RowValues,
    values: &RowValues,
) -> Result<Vec<BufferMutation>, RowEncodeError> {
    let handle = if is_clustered(table) {
        None
    } else {
        Some(TableHandle::Int(row_id_of(key)?))
    };
    TableWriter::open(table, new_collation_enabled())?.update(handle, existing, values)
}

/// The mutations that remove one stored row and every index entry it owns.
pub fn delete_row(
    table: &TableInfo,
    key: &[u8],
    values: &RowValues,
) -> Result<Vec<BufferMutation>, RowEncodeError> {
    let handle = TableHandle::Int(row_id_of(key)?);
    TableWriter::open(table, new_collation_enabled())?.remove(Some(handle), values, true)
}

/// The record key one row of a CLUSTERED table lives under.
///
/// A clustered table's primary key *is* its handle: `mysql.stats_meta` keys
/// on `table_id`, `mysql.stats_histograms` and `mysql.stats_buckets` on their
/// whole `(table_id, is_index, hist_id[, bucket_id])` tuple. Nothing
/// allocates a `_tidb_rowid` for them, and nothing needs to: a row's identity
/// is its own values, so an `ANALYZE` that rewrites a table's statistics
/// overwrites exactly the keys the previous one wrote. Only the key columns
/// need a value.
pub fn clustered_record_key(
    table: &TableInfo,
    values: &RowValues,
) -> Result<Vec<u8>, RowEncodeError> {
    let mut writer = TableWriter::open(table, new_collation_enabled())?;
    let row = writer.row(values, false)?;
    let handle = writer.clustered_handle(&row)?;
    Ok(encode_row_key_with_handle(
        table.id,
        &handle.record_handle(),
    ))
}

/// The mutations that store one row of a clustered table.
///
/// `existing` is the row already living under that key, as this planner's own
/// snapshot read it, or `None` when there is none. It decides two things a
/// caller must not guess:
///
/// * which assertion the record mutation carries -- TiKV rejects an `Insert`
///   over a key that exists and a `Delete`/`Put` over one that does not, so
///   an `ANALYZE` that guessed would fail either the first time or every time
///   after;
/// * which index entries to retract. An index entry's key contains the
///   indexed value, so moving that value (`mysql.stats_meta.idx_ver`, which
///   every `ANALYZE` moves) leaves the old entry pointing at the row unless
///   it is deleted by the value it was written with.
pub fn store_clustered_row(
    table: &TableInfo,
    existing: Option<&RowValues>,
    values: &RowValues,
) -> Result<Vec<BufferMutation>, RowEncodeError> {
    let writer = TableWriter::open(table, new_collation_enabled())?;
    match existing {
        Some(existing) => writer.update(None, existing, values),
        None => writer.add(values, None),
    }
}

/// The mutations that rewrite one row of a `_tidb_rowid` table in place,
/// keeping its handle (Go `UpdateRecord`).
///
/// Keeping the handle is the point. Deleting the row and inserting a
/// replacement under a fresh handle would touch every index entry twice --
/// and for an index whose columns did not move, that is a delete and a put of
/// the *same key* in one transaction, which the mutation set rejects. A
/// rewrite under the same handle leaves those entries alone, exactly as Go's
/// `UPDATE` does.
pub fn rewrite_rowid_row(
    table: &TableInfo,
    key: &[u8],
    existing: &RowValues,
    values: &RowValues,
) -> Result<Vec<BufferMutation>, RowEncodeError> {
    let handle = TableHandle::Int(row_id_of(key)?);
    TableWriter::open(table, new_collation_enabled())?.update(Some(handle), existing, values)
}

/// The mutations that remove one row of a clustered table and every index
/// entry it owns.
pub fn delete_clustered_row(
    table: &TableInfo,
    values: &RowValues,
) -> Result<Vec<BufferMutation>, RowEncodeError> {
    TableWriter::open(table, new_collation_enabled())?.remove(None, values, true)
}

/// Go SQL `REPLACE` for a clustered system table with no secondary indexes.
///
/// The unasserted put is essential for `mysql.tidb_mdl_info`: the first phase
/// normally creates the row, while a later phase may overwrite the stale row
/// left by Go's warning-only cleanup. Refusing indexed tables avoids leaving
/// an old secondary entry when the previous row is not available to retract.
pub fn replace_unindexed_clustered_row(
    table: &TableInfo,
    values: &RowValues,
) -> Result<Vec<BufferMutation>, RowEncodeError> {
    refuse_indexed(table, "replaced")?;
    unasserted(TableWriter::open(table, new_collation_enabled())?.add(values, None)?)
}

/// Go SQL `DELETE` for an unindexed clustered system-table row.
///
/// Missing rows are successful, so this uses an unasserted delete rather
/// than ordinary DML's `AssertExist` mutation.
pub fn delete_unindexed_clustered_row(
    table: &TableInfo,
    values: &RowValues,
) -> Result<Vec<BufferMutation>, RowEncodeError> {
    refuse_indexed(table, "deleted")?;
    unasserted(TableWriter::open(table, new_collation_enabled())?.remove(None, values, false)?)
}

fn refuse_indexed(table: &TableInfo, verb: &str) -> Result<(), RowEncodeError> {
    if !table.indices.is_empty() {
        return Err(encode_error(format!(
            "{} is indexed and cannot be {verb} without its previous row",
            table_name(table)
        )));
    }
    Ok(())
}

/// The table layer's record write without its existence assertion.
fn unasserted(mutations: Vec<BufferMutation>) -> Result<Vec<BufferMutation>, RowEncodeError> {
    mutations
        .into_iter()
        .map(|mutation| {
            let (op, key, value, _, _) = mutation.into_parts();
            match op {
                BufferMutationOp::Set => BufferMutation::set(key, value),
                _ => BufferMutation::delete(key),
            }
            .map_err(|error| encode_error(error.to_string()))
        })
        .collect()
}

/// The stored `_tidb_rowid` a record key carries.
///
/// The key is `t{table_id}_r{handle}` with the handle written by
/// [`tidb_codec`]'s signed-int encoder, so this is that encoder read backwards
/// rather than a second guess at the layout.
pub fn row_id_of(key: &[u8]) -> Result<i64, RowEncodeError> {
    let suffix: [u8; 8] = key
        .get(key.len().saturating_sub(8)..)
        .and_then(|tail| tail.try_into().ok())
        .ok_or_else(|| encode_error("a record key is shorter than its handle"))?;
    // `EncodeInt` flips the sign bit so the big-endian order is the signed
    // order; reading it back flips it again.
    Ok((u64::from_be_bytes(suffix) ^ (1u64 << 63)) as i64)
}

/// The lowercase names of every column a public index of `table` covers.
///
/// A caller that wants to rewrite a row in place asks this first: an update
/// that touches one of these columns is an index move, which
/// [`update_row`] does not perform.
pub fn indexed_columns(table: &TableInfo) -> Vec<String> {
    let mut names = Vec::new();
    for index in table.indices.iter_deref() {
        let index = index.read();
        for column in index.columns.iter_deref() {
            names.push(column.read().name.lowercase().to_owned());
        }
    }
    names
}

/// A string value re-tagged with the collation of the column it belongs to.
///
/// In new-collation mode an index KEY is a SORT KEY, and the key codec takes
/// the collation from the DATUM. A caller states its values as plain bytes,
/// whose collation is `binary`, so without this every index key over a
/// `utf8mb4_general_ci` column would hold the raw bytes where Go holds the
/// case-folded weights -- and a Go server would not find the row through the
/// index at all. In legacy mode the same tagged datum encodes its raw bytes.
/// `mysql.db`, `mysql.tables_priv` and `mysql.columns_priv` all key on such a
/// column.
///
/// Go reaches the same place from the other side: its `INSERT` casts each
/// value to the column type (`table.CastValue`), and a cast result carries the
/// column's collation.
fn collated_value(value: Datum, field_type: &FieldType) -> Datum {
    if !field_type.is_character_string() {
        return value;
    }
    match value {
        Datum::Bytes(bytes) => Datum::new_collation_string(bytes, field_type.collation()),
        Datum::String(string) => {
            Datum::new_collation_string(string.bytes().to_vec(), field_type.collation())
        }
        other => other,
    }
}

fn table_name(table: &TableInfo) -> &str {
    table.name.original()
}

/// The datum one column's declared `DEFAULT` materialises to.
///
/// This is what an `INSERT` stores for a column the statement does not name: a
/// literal default as itself, `CURRENT_TIMESTAMP` as the caller's own `now`,
/// and no default at all as `NULL`. An expression default is refused, because
/// evaluating one is not this path's job and silently storing its unevaluated
/// text writes a row a real TiDB rejects.
pub fn declared_default(
    column: &ColumnInfo,
    table: &str,
    current_timestamp: Time,
) -> Result<Datum, RowEncodeError> {
    if column.default_is_expr {
        return Err(unmaterialisable(column, table));
    }
    // Go `table.GetColDefaultValue` reads `GetDefaultValue()`, whose BIT arm
    // returns the raw `DefaultValueBit` bytes; the JSON-facing string is the
    // same bytes decoded lossily.
    materialise(
        column,
        &column.get_default_value(),
        table,
        Some(current_timestamp),
    )
}

/// The byte-authoritative spelling a value reads back with after column
/// typing. ENUM/SET declarations and stored values are Go strings, so this
/// path deliberately retains invalid UTF-8 instead of manufacturing text.
pub fn canonical_bytes(field_type: &FieldType, bytes: &[u8]) -> Result<Vec<u8>, RowEncodeError> {
    match typed_value(&Datum::Bytes(bytes.to_vec()), field_type)? {
        Datum::Set(members, _) => Ok(members.name_bytes().to_vec()),
        Datum::Enum(member, _) => Ok(member.name_bytes().to_vec()),
        _ => Ok(bytes.to_vec()),
    }
}

/// The spelling a value of this column reads back as once stored.
///
/// A `SET` is why this exists: the row stores it as a bit mask, so it always
/// reads back with its element names in *declaration* order, whatever order
/// the caller wrote them in. A writer comparing a desired value against a
/// stored one has to compare the two in the same spelling or it rewrites the
/// row forever.
pub fn canonical_text(field_type: &FieldType, text: &str) -> Result<String, RowEncodeError> {
    String::from_utf8(canonical_bytes(field_type, text.as_bytes())?).map_err(|_| {
        RowEncodeError("the canonical ENUM/SET spelling is not valid UTF-8".to_owned())
    })
}

/// The tablecodec view of one stored `TableInfo`.
///
/// `tidb-tablecodec` keeps its own minimal metadata shape so it does not depend
/// on the full catalog model; the index encoders need this projection of it.
pub fn codec_table_info(table: &TableInfo) -> CodecTableInfo {
    CodecTableInfo {
        columns: table
            .columns
            .iter_deref()
            .enumerate()
            .map(|(offset, column)| {
                let column = column.read();
                CodecTableColumn {
                    id: column.id,
                    offset,
                    field_type: column.field_type.clone(),
                    primary_key: column
                        .field_type
                        .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY),
                    // Go `ColumnInfo.ChangingFieldType`: the type an in-flight
                    // `MODIFY COLUMN` is moving this column to, which the key
                    // parts flagged `using_changing_type` encode under.
                    changing_field_type: column
                        .changing_field_type
                        .as_ref()
                        .map(|changing| changing.read().clone()),
                }
            })
            .collect(),
        indices: table
            .indices
            .iter_deref()
            .map(|index| {
                let index = index.read();
                CodecIndexInfo {
                    id: index.id,
                    columns: index
                        .columns
                        .iter_deref()
                        .map(|column| {
                            let column = column.read();
                            CodecIndexColumn {
                                offset: usize::try_from(column.offset)
                                    .expect("a column offset is not negative"),
                                length: column.length,
                                use_changing_type: column.use_changing_type,
                            }
                        })
                        .collect(),
                    unique: index.unique,
                    global: index.global,
                    // Go `IndexInfo.GlobalIndexVersion` decides whether the
                    // partition id is part of the index KEY, so it is read from
                    // the schema rather than assumed. Every `mysql.*` table this
                    // crate writes is unpartitioned, so the value is 0 in
                    // practice — but assuming that in the code would be a lie the
                    // first time a partitioned table reaches this path.
                    global_index_version: index.global_index_version,
                    primary: index.primary,
                }
            })
            .collect(),
        pk_is_handle: table.pk_is_handle,
        is_common_handle: table.is_common_handle,
        common_handle_version: u8::try_from(table.common_handle_version).unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};

    /// Go `GetColOriginDefaultValueWithoutStrictSQLMode` casts the stored text
    /// to the column's type. A DATE origin default reached the coprocessor as
    /// the bytes of its printed form, and reading a row written before the
    /// column existed panicked appending bytes to a fixed-length column.
    #[test]
    fn an_origin_default_is_cast_to_its_column_type() {
        for (code, text, expected) in [
            (FieldTypeCode::Date, "2020-03-27", "2020-03-27"),
            (
                FieldTypeCode::Datetime,
                "2020-03-27 20:20:20",
                "2020-03-27 20:20:20",
            ),
            (FieldTypeCode::Duration, "12:23:23", "12:23:23"),
            (FieldTypeCode::NewDecimal, "1.5", "1.5"),
            (FieldTypeCode::Double, "2.25", "2.25"),
        ] {
            let mut column = ColumnInfo {
                id: 1,
                name: tidb_ast::CiString::new("c"),
                field_type: FieldType::new(code),
                ..Default::default()
            };
            column
                .set_origin_default_value(tidb_model::column::ColumnDefaultValue::str(text))
                .expect("a string origin default");
            let datum = origin_default(&column, "t").expect("the default materialises");
            assert!(!matches!(datum, Datum::Bytes(_)), "{code:?}: {datum:?}");
            assert_eq!(
                String::from_utf8_lossy(&datum.sql_bytes().expect("printable")),
                expected,
                "{code:?}"
            );
        }
    }

    fn indexed_fixture(unique: bool, nullable: bool) -> (TableInfo, RowValues) {
        use tidb_model::index::{IndexColumn, IndexInfo};
        let column = ColumnInfo {
            id: 1,
            name: tidb_ast::CiString::new("v"),
            state: tidb_model::SchemaState::PUBLIC,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            ..Default::default()
        };
        let index = IndexInfo {
            id: 2,
            name: tidb_ast::CiString::new("v_idx"),
            unique,
            state: tidb_model::SchemaState::PUBLIC,
            columns: vec![IndexColumn {
                name: tidb_ast::CiString::new("v"),
                offset: 0,
                length: -1,
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        };
        (
            TableInfo {
                id: 42,
                state: tidb_model::SchemaState::PUBLIC,
                columns: vec![column].into(),
                indices: vec![index].into(),
                ..Default::default()
            },
            [(1, if nullable { Datum::Null } else { Datum::Int(7) })].into(),
        )
    }

    /// The one index entry among a row's mutations (Go writes it before the
    /// record).
    fn index_write(mutations: &[BufferMutation]) -> &BufferMutation {
        let prefix = tidb_codec::table_key::encode_table_index_prefix(42, 2);
        let [entry] = mutations
            .iter()
            .filter(|mutation| mutation.key().starts_with(&prefix))
            .collect::<Vec<_>>()[..]
        else {
            panic!("one index entry: {mutations:?}");
        };
        entry
    }

    #[test]
    fn system_unique_index_insert_defers_duplicate_check_to_commit() {
        let (table, values) = indexed_fixture(true, false);
        let mutations = insert_row_with_collation(&table, 1, &values, true).unwrap();
        assert!(index_write(&mutations).presume_not_exists());
        assert_eq!(
            index_write(&mutations).assertion(),
            tidb_txnkv::AssertionOp::AssertUnknown
        );
    }

    #[test]
    fn system_nondistinct_index_insert_asserts_absence_without_duplicate_flag() {
        for (unique, nullable) in [(false, false), (true, true)] {
            let (table, values) = indexed_fixture(unique, nullable);
            let mutations = insert_row_with_collation(&table, 1, &values, true).unwrap();
            assert!(!index_write(&mutations).presume_not_exists());
            assert_eq!(
                index_write(&mutations).assertion(),
                tidb_txnkv::AssertionOp::AssertUnknown
            );
        }
    }

    #[test]
    fn system_public_index_delete_asserts_existing_entry() {
        let (table, values) = indexed_fixture(true, false);
        let key = encode_row_key_with_handle(table.id, &RecordHandle::Int(1));
        let mutations = delete_row(&table, &key, &values).unwrap();
        assert_eq!(
            index_write(&mutations).assertion(),
            tidb_txnkv::AssertionOp::AssertExist
        );
    }

    #[test]
    fn system_index_move_checks_destination_and_preserves_unchanged_keys() {
        let (table, values) = indexed_fixture(true, false);
        let key = encode_row_key_with_handle(table.id, &RecordHandle::Int(1));
        let unchanged = rewrite_rowid_row(&table, &key, &values, &values).unwrap();
        assert_eq!(unchanged.len(), 1);
        let changed = [(1, Datum::Int(8))].into();
        let moved = rewrite_rowid_row(&table, &key, &values, &changed).unwrap();
        assert_eq!(moved.len(), 3);
        // Go's optimistic `UPDATE` checks the moved entry in place
        // (`optimizeDupKeyCheckForUpdate`): no lazy check, and the destination
        // is asserted absent.
        let prefix = tidb_codec::table_key::encode_table_index_prefix(42, 2);
        let entry = |kind: BufferMutationOp| {
            moved
                .iter()
                .find(|mutation| mutation.key().starts_with(&prefix) && mutation.kind() == kind)
                .expect("an index write of that kind")
        };
        let retracted = entry(BufferMutationOp::Delete);
        assert_eq!(retracted.assertion(), tidb_txnkv::AssertionOp::AssertExist);
        let stored = entry(BufferMutationOp::Set);
        assert!(!stored.presume_not_exists());
        assert_eq!(stored.assertion(), tidb_txnkv::AssertionOp::AssertNotExist);
    }

    #[test]
    fn system_nonpublic_index_suppresses_assertions_but_keeps_unique_check() {
        let (table, values) = indexed_fixture(true, false);
        table.indices.get(0).unwrap().write().state = tidb_model::SchemaState::WRITE_ONLY;
        let inserted = insert_row_with_collation(&table, 1, &values, true).unwrap();
        // Go `index.create` for a non-public index: checked like a public
        // unique entry, but asserting nothing about the key's existence.
        assert!(index_write(&inserted).presume_not_exists());
        assert_eq!(
            index_write(&inserted).assertion(),
            tidb_txnkv::AssertionOp::AssertUnknown
        );
        let key = encode_row_key_with_handle(table.id, &RecordHandle::Int(1));
        let deleted = delete_row(&table, &key, &values).unwrap();
        assert_eq!(
            index_write(&deleted).assertion(),
            tidb_txnkv::AssertionOp::AssertNone
        );
    }

    /// `mysql.column_stats_usage`'s shape: `PRIMARY KEY (table_id, column_id)
    /// CLUSTERED`, plus a secondary index so the test also sees an entry that
    /// must be written.
    fn clustered_fixture() -> (TableInfo, RowValues) {
        use tidb_model::index::{IndexColumn, IndexInfo};
        let column = |id: i64, name: &str| ColumnInfo {
            id,
            name: tidb_ast::CiString::new(name),
            offset: id - 1,
            state: tidb_model::SchemaState::PUBLIC,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            ..Default::default()
        };
        let index_column = |name: &str, offset: i64| IndexColumn {
            name: tidb_ast::CiString::new(name),
            offset,
            length: -1,
            ..Default::default()
        };
        let primary = IndexInfo {
            id: 1,
            name: tidb_ast::CiString::new("PRIMARY"),
            primary: true,
            unique: true,
            state: tidb_model::SchemaState::PUBLIC,
            columns: vec![index_column("table_id", 0), index_column("column_id", 1)].into(),
            ..Default::default()
        };
        let secondary = IndexInfo {
            id: 2,
            name: tidb_ast::CiString::new("v_idx"),
            state: tidb_model::SchemaState::PUBLIC,
            columns: vec![index_column("v", 2)].into(),
            ..Default::default()
        };
        (
            TableInfo {
                id: 42,
                state: tidb_model::SchemaState::PUBLIC,
                is_common_handle: true,
                common_handle_version: 1,
                columns: vec![
                    column(1, "table_id"),
                    column(2, "column_id"),
                    column(3, "v"),
                ]
                .into(),
                indices: vec![primary, secondary].into(),
                ..Default::default()
            },
            [(1, Datum::Int(2)), (2, Datum::Int(1)), (3, Datum::Int(7))].into(),
        )
    }

    #[test]
    fn a_clustered_primary_index_owns_no_entry_on_store_or_delete() {
        let (table, values) = clustered_fixture();
        let record_key = clustered_record_key(&table, &values).unwrap();
        for mutations in [
            store_clustered_row(&table, None, &values).unwrap(),
            delete_clustered_row(&table, &values).unwrap(),
        ] {
            // The record itself and the secondary entry; nothing under the
            // primary index's id.
            assert_eq!(mutations.len(), 2, "{mutations:?}");
            assert!(
                mutations
                    .iter()
                    .any(|mutation| mutation.key() == record_key.as_slice()),
                "{mutations:?}"
            );
            let primary_prefix = tidb_codec::table_key::encode_table_index_prefix(42, 1);
            assert!(
                mutations
                    .iter()
                    .all(|mutation| !mutation.key().starts_with(&primary_prefix)),
                "{mutations:?}"
            );
        }
    }

    #[test]
    fn a_record_key_reads_back_the_handle_it_was_written_with() {
        // The delete path derives a row's index entries from its handle, and
        // the only place that handle survives is the record key itself.
        for row_id in [1_i64, 2, 127, 128, 65_536, i64::MAX, -1, i64::MIN] {
            let key = encode_row_key_with_handle(42, &RecordHandle::Int(row_id));
            assert_eq!(row_id_of(&key).expect("the key carries a handle"), row_id);
        }
    }

    #[test]
    fn a_key_too_short_to_carry_a_handle_is_refused_rather_than_guessed() {
        assert!(row_id_of(&[0, 1, 2]).is_err());
    }

    fn declared(code: FieldTypeCode, elements: &[&str]) -> FieldType {
        let mut field_type = FieldType::new(code);
        field_type.set_elems(
            elements
                .iter()
                .map(|e| GoString::from(*e))
                .collect::<Vec<_>>(),
        );
        field_type
    }

    #[test]
    fn a_privilege_enum_is_written_as_its_member_not_as_a_string() {
        // A `Y` stored as `Bytes` decodes as the wrong type on a real TiDB,
        // which is what makes this conversion load-bearing rather than tidy.
        let enum_type = declared(FieldTypeCode::Enum, &[NO, YES]);
        let yes = typed_value(&Datum::Bytes(b"Y".to_vec()), &enum_type).expect("Y is declared");
        let no = typed_value(&Datum::Bytes(b"N".to_vec()), &enum_type).expect("N is declared");
        match (yes, no) {
            (Datum::Enum(yes, _), Datum::Enum(no, _)) => {
                assert_eq!((yes.name_bytes(), yes.value()), (b"Y".as_slice(), 2));
                assert_eq!((no.name_bytes(), no.value()), (b"N".as_slice(), 1));
            }
            other => panic!("the enums did not survive typing: {other:?}"),
        }
    }

    #[test]
    fn enum_and_set_typing_preserve_arbitrary_go_string_bytes() {
        let mut enum_type = FieldType::new(FieldTypeCode::Enum);
        enum_type.set_elems(vec![GoString::from([0xff]), GoString::from([0x15])]);
        let typed = typed_value(&Datum::Bytes(vec![0xff]), &enum_type)
            .expect("the raw ENUM member is declared");
        match typed {
            Datum::Enum(member, _) => {
                assert_eq!(member.name_bytes(), [0xff]);
                assert_eq!(member.value(), 1);
            }
            other => panic!("the raw ENUM did not survive typing: {other:?}"),
        }
        assert_eq!(canonical_bytes(&enum_type, &[0xff]).unwrap(), [0xff]);
        assert!(canonical_text(&enum_type, "\u{fffd}").is_err());

        let mut set_type = FieldType::new(FieldTypeCode::Set);
        set_type.set_elems(vec![GoString::from([0xfe])]);
        let typed = typed_value(&Datum::Bytes(vec![0xfe]), &set_type)
            .expect("the raw SET member is declared");
        match typed {
            Datum::Set(member, _) => {
                assert_eq!(member.name_bytes(), [0xfe]);
                assert_eq!(member.value(), 1);
            }
            other => panic!("the raw SET did not survive typing: {other:?}"),
        }
    }

    #[test]
    fn dynamic_defaults_preserve_go_bytes_and_nil_interface_identity() {
        let mut column = ColumnInfo::default();
        column.set_type(FieldTypeCode::Varchar);

        column
            .set_default_value(tidb_model::ColumnDefaultValue::string_bytes(vec![
                0xff, 0x80, b'x',
            ]))
            .expect("a built-in Go string is a valid VARCHAR default");
        assert_eq!(
            literal_default(&column, "t").expect("the raw string default materialises"),
            Datum::new_collation_string(
                vec![0xff, 0x80, b'x'],
                tidb_datatype::Collation::Utf8Mb4Bin
            )
        );

        column
            .set_default_value(tidb_model::ColumnDefaultValue::Bytes(
                tidb_model::GoAnyBytes::from_vec(vec![0xfe, 0x81]),
            ))
            .expect("a dynamic []byte default is retained");
        assert_eq!(
            literal_default(&column, "t").expect("the raw []byte default materialises"),
            Datum::Bytes(vec![0xfe, 0x81])
        );

        // A typed nil []byte is a non-nil interface and therefore stays a
        // byte datum. Only a nil interface maps to SQL NULL.
        column
            .set_default_value(tidb_model::ColumnDefaultValue::Bytes(
                tidb_model::GoAnyBytes::default(),
            ))
            .expect("a typed nil []byte is retained");
        assert!(!column.default_value.is_nil());
        assert_eq!(
            literal_default(&column, "t").expect("the typed nil []byte materialises"),
            Datum::Bytes(Vec::new())
        );

        column
            .set_default_value(GoAny::nil())
            .expect("a nil interface is a valid nullable default");
        assert!(column.default_value.is_nil());
        assert_eq!(
            literal_default(&column, "t").expect("the nil interface materialises"),
            Datum::Null
        );
    }

    /// Captured from `planner/core/indexjoin`: Go stores and loads a
    /// `BIGINT UNSIGNED DEFAULT '12818995484625953384'`, past `i64::MAX`.
    #[test]
    fn unsigned_integer_defaults_read_past_the_signed_range() {
        let mut column = ColumnInfo::default();
        column.set_type(FieldTypeCode::LongLong);
        column
            .field_type
            .add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
        column
            .set_default_value(tidb_model::ColumnDefaultValue::string_bytes(
                b"12818995484625953384".to_vec(),
            ))
            .expect("a printed unsigned default is valid");
        assert_eq!(
            literal_default(&column, "t").expect("the unsigned default materialises"),
            Datum::UInt(12_818_995_484_625_953_384)
        );
    }

    #[test]
    fn codec_projection_reads_the_current_shared_changing_field_type() {
        let changing = tidb_model::GoShared::new(FieldType::new(FieldTypeCode::Varchar));
        let column = ColumnInfo {
            field_type: FieldType::new(FieldTypeCode::Long),
            changing_field_type: Some(changing.clone()),
            ..Default::default()
        };
        let table = TableInfo {
            columns: vec![column].into(),
            ..Default::default()
        };

        let projected = codec_table_info(&table);
        assert_eq!(
            projected.columns[0]
                .changing_field_type
                .as_ref()
                .expect("the changing type is projected")
                .code(),
            FieldTypeCode::Varchar
        );

        changing.write().set_code(FieldTypeCode::LongLong);
        let projected = codec_table_info(&table);
        assert_eq!(
            projected.columns[0]
                .changing_field_type
                .as_ref()
                .expect("the current shared changing type is projected")
                .code(),
            FieldTypeCode::LongLong
        );
    }

    /// `mysql.tables_priv`.`Table_priv`'s own element list, in declaration
    /// order -- which is what fixes each privilege's bit.
    const TABLE_PRIV_ELEMENTS: &[&str] = &[
        "Select",
        "Insert",
        "Update",
        "Delete",
        "Create",
        "Drop",
        "Grant",
        "Index",
        "Alter",
        "Create View",
        "Show View",
        "Operate View",
        "Trigger",
        "References",
    ];

    #[test]
    fn a_set_column_is_written_as_the_bit_mask_go_stores() {
        // Captured from a real TiDB: after `GRANT SELECT, UPDATE ON test.trows
        // ... WITH GRANT OPTION`, `SELECT Table_priv, Table_priv+0 FROM
        // mysql.tables_priv` answered `Select,Update,Grant` and `69`
        // (`go test -tags=intest ./pkg/executor/ -run TestZZDumpTablesPriv`).
        let set_type = declared(FieldTypeCode::Set, TABLE_PRIV_ELEMENTS);
        let value = typed_value(&Datum::Bytes(b"Select,Update,Grant".to_vec()), &set_type)
            .expect("every named element is declared");
        match value {
            Datum::Set(members, _) => {
                assert_eq!(members.value(), 69);
                assert_eq!(members.name(), "Select,Update,Grant");
            }
            other => panic!("the SET did not survive typing: {other:?}"),
        }

        // An empty SET is the empty mask, not an unknown element -- that is the
        // value a `tables_priv` row carries when only its columns are granted.
        match typed_value(&Datum::Bytes(Vec::new()), &set_type).expect("the empty SET is valid") {
            Datum::Set(members, _) => {
                assert_eq!((members.name_bytes(), members.value()), (b"".as_slice(), 0));
            }
            other => panic!("the empty SET did not survive typing: {other:?}"),
        }
    }

    #[test]
    fn an_element_the_column_does_not_declare_is_refused_rather_than_invented() {
        // A privilege spelled wrong would otherwise be written with a made-up
        // bit, which a Go TiDB would read back as a different privilege.
        let set_type = declared(FieldTypeCode::Set, TABLE_PRIV_ELEMENTS);
        assert!(typed_value(&Datum::Bytes(b"Select,Reload".to_vec()), &set_type).is_err());
        let enum_type = declared(FieldTypeCode::Enum, &[NO, YES]);
        assert!(typed_value(&Datum::Bytes(b"maybe".to_vec()), &enum_type).is_err());
        // And a column whose declaration was never loaded cannot be guessed at.
        assert!(typed_value(
            &Datum::Bytes(b"Select".to_vec()),
            &FieldType::new(FieldTypeCode::Set)
        )
        .is_err());
    }

    /// The writer's own context (Go Lightning's static
    /// `litTableMutateContext` shape) writes, rewrites and removes a row of
    /// every bootstrap table byte-for-byte as the default statement context
    /// it replaced did -- refusals included.
    #[test]
    fn the_system_write_context_writes_as_the_default_statement_did() {
        use tidb_ast::{DdlStmt, Stmt};
        use tidb_datatype::{EvalType, TimeType};
        type Context = Arc<dyn tidb_table::context::MutateContext>;
        let contexts = || -> [Context; 2] {
            [
                Arc::new(tidb_table::context::StaticMutateContext::new()),
                Arc::new(
                    tidb_executor::StmtContext::for_query().with_time_zone(SessionTimeZone::utc()),
                ),
            ]
        };
        let now = Time::from_date_checked(2026, 10, 27, 12, 34, 56, 0, TimeType::Timestamp, 0)
            .expect("a valid timestamp");
        let mut checked = 0;
        for bootstrap in tidb_metadef::BOOTSTRAP_TABLES {
            let Ok(Stmt::Ddl(ddl)) = tidb_parser::parse(bootstrap.create_sql) else {
                continue;
            };
            let DdlStmt::CreateTable(create) = ddl.as_ref() else {
                continue;
            };
            let mut table = tidb_ddl_build::table_info_build::build_table_info_with_context(
                create,
                "utf8mb4",
                "utf8mb4_bin",
                &tidb_expr::metabuild::Context::new_non_strict(),
                false,
            )
            .unwrap_or_else(|error| panic!("mysql.{}: {error:?}", bootstrap.name));
            table.id = bootstrap.id;
            table.state = tidb_model::SchemaState::PUBLIC;
            let mut values = defaults_row(&table, now).expect("the declared defaults");
            let mut changed = values.clone();
            for column in table.columns.iter_deref() {
                let column = column.read();
                let (value, next) = match column.field_type.eval_type() {
                    EvalType::Int => (Datum::Int(3), Datum::Int(4)),
                    EvalType::String => (
                        Datum::Bytes(b"x".to_vec()),
                        Datum::Bytes(b"y".to_vec()),
                    ),
                    _ => continue,
                };
                if matches!(values.get(&column.id), Some(Datum::Null)) {
                    values.insert(column.id, value);
                }
                changed.insert(column.id, next);
            }
            let handle = (!is_clustered(&table)).then_some(TableHandle::Int(5));
            let writes = |context: Context| {
                let open = || TableWriter::open_with(&table, true, Arc::clone(&context));
                let inserted = open().and_then(|writer| {
                    writer.add(&values, handle.is_some().then_some(5))
                });
                let updated = open().and_then(|writer| writer.update(handle.clone(), &values, &changed));
                let removed = open().and_then(|writer| writer.remove(handle.clone(), &values, true));
                format!("{inserted:?}\n{updated:?}\n{removed:?}")
            };
            let [system, statement] = contexts();
            assert_eq!(writes(system), writes(statement), "mysql.{}", bootstrap.name);
            checked += 1;
        }
        assert!(checked > 30, "only {checked} bootstrap tables were compared");
    }
}
