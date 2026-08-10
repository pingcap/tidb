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
//! [`crate::mysql_bootstrap`] proved this encoding against a real TiDB (a Go
//! server reads the rows it seeds); this module is that same encoding lifted
//! out of the bootstrap's one-shot shape so the account path can also update
//! and delete rows that already exist.
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
//! `TableInfo`, never about the table's name; [`crate::cluster_stats_write`]
//! asks [`HandleLayout::of`] per table for exactly that reason.

use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
use tidb_codec::Encoder;
use tidb_datatype::{
    is_bin_collation, new_collation_enabled, parse_enum, parse_set_name, Datum, FieldType,
    FieldTypeCode, GoString, Time,
};
use tidb_model::column::ColumnInfo;
use tidb_model::table_info::TableInfo;
use tidb_model::{GoAny, GoAnyView};
use tidb_tablecodec::{
    encode_table_row, generate_index_key, generate_index_value, truncate_index_value,
    IndexColumn as CodecIndexColumn, IndexInfo as CodecIndexInfo, TableColumn as CodecTableColumn,
    TableInfo as CodecTableInfo,
};
use tidb_txnkv::transaction::OptimisticMutation;
use tidb_txnkv::{CommonHandle, Handle, IntHandle};

use crate::mysql_system_tables::HandleLayout;

/// Go `ast.CurrentTimestamp`, as a column default stores it.
pub(crate) const CURRENT_TIMESTAMP: &str = "CURRENT_TIMESTAMP";

/// Go `types.UnspecifiedLength`: a key part that stores its whole column.
const UNSPECIFIED_LENGTH: i64 = -1;

/// Go's own `Y`/`N` privilege spelling.
pub const YES: &str = "Y";
/// The `N` half of every `ENUM('N','Y')` privilege column.
pub const NO: &str = "N";

/// Why one row could not be encoded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowEncodeError(pub String);

impl std::fmt::Display for RowEncodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RowEncodeError {}

fn encode_error(detail: impl Into<String>) -> RowEncodeError {
    RowEncodeError(detail.into())
}

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

/// The mutations that write one row that does not exist yet, at `row_id`.
pub fn insert_row(
    table: &TableInfo,
    row_id: i64,
    values: &RowValues,
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    insert_row_with_collation(table, row_id, values, new_collation_enabled())
}

/// [`insert_row`] with the cluster's persisted collation mode already
/// captured.
///
/// Go fixes this mode in the table/index encoder. Accepting it once here keeps
/// the entry key, restored-data decision, and entry value on one format even
/// if the process-level source changes while the mutations are assembled.
pub fn insert_row_with_collation(
    table: &TableInfo,
    row_id: i64,
    values: &RowValues,
    use_new_collation: bool,
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    let key = encode_row_key_with_handle(table.id, &RecordHandle::Int(row_id));
    let mut mutations = vec![OptimisticMutation::insert(key, encode_row(table, values)?)
        .map_err(|error| encode_error(error.to_string()))?];
    mutations.extend(index_entries(
        table,
        &Handle::Int(IntHandle::new(row_id)),
        values,
        IndexOp::Put,
        use_new_collation,
    )?);
    Ok(mutations)
}

/// The mutation that rewrites one row already stored under `key`.
///
/// Index entries are deliberately *not* touched: this path is only for a
/// change to columns no public index covers, which every caller checks before
/// calling ([`indexed_columns`]). A rewrite that moved an indexed value would
/// otherwise leave the old entry pointing at the new row.
pub fn update_row(
    table: &TableInfo,
    key: &[u8],
    values: &RowValues,
) -> Result<OptimisticMutation, RowEncodeError> {
    OptimisticMutation::put_existing(key.to_vec(), encode_row(table, values)?)
        .map_err(|error| encode_error(error.to_string()))
}

/// The mutations that remove one stored row and every index entry it owns.
pub fn delete_row(
    table: &TableInfo,
    key: &[u8],
    values: &RowValues,
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    delete_row_with_collation(table, key, values, new_collation_enabled())
}

fn delete_row_with_collation(
    table: &TableInfo,
    key: &[u8],
    values: &RowValues,
    use_new_collation: bool,
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    let row_id = row_id_of(key)?;
    let mut mutations = vec![OptimisticMutation::delete(key.to_vec())
        .map_err(|error| encode_error(error.to_string()))?];
    mutations.extend(index_entries(
        table,
        &Handle::Int(IntHandle::new(row_id)),
        values,
        IndexOp::Delete,
        use_new_collation,
    )?);
    Ok(mutations)
}

/// The record key and handle one row of a CLUSTERED table lives under.
///
/// A clustered table's primary key *is* its handle: `mysql.stats_meta` keys
/// on `table_id`, `mysql.stats_histograms` and `mysql.stats_buckets` on their
/// whole `(table_id, is_index, hist_id[, bucket_id])` tuple. Nothing
/// allocates a `_tidb_rowid` for them, and nothing needs to: a row's identity
/// is its own values, so an `ANALYZE` that rewrites a table's statistics
/// overwrites exactly the keys the previous one wrote.
///
/// A table with no clustered handle is refused rather than given some
/// invented key: [`insert_row`] is that table's writer.
pub fn clustered_record_key(
    table: &TableInfo,
    values: &RowValues,
) -> Result<(Vec<u8>, Handle), RowEncodeError> {
    clustered_record_key_with_collation(table, values, new_collation_enabled())
}

fn clustered_record_key_with_collation(
    table: &TableInfo,
    values: &RowValues,
    use_new_collation: bool,
) -> Result<(Vec<u8>, Handle), RowEncodeError> {
    let layout = HandleLayout::of(table);
    let named = |name: &str| -> Result<&Datum, RowEncodeError> {
        let column = table
            .cols()
            .iter_deref()
            .find(|column| column.read().name.lowercase() == name)
            .ok_or_else(|| encode_error(format!("{} has no column `{name}`", table_name(table))))?;
        let column_id = column.read().id;
        values.get(&column_id).ok_or_else(|| {
            encode_error(format!(
                "{}.{name} has no value in the row being written",
                table_name(table)
            ))
        })
    };
    match &layout {
        HandleLayout::RowId => Err(encode_error(format!(
            "{} has no clustered handle; its rows are written under an allocated _tidb_rowid",
            table_name(table)
        ))),
        HandleLayout::Int(column) => {
            let value = named(column)?;
            let handle = match value {
                Datum::Int(value) => *value,
                Datum::UInt(value) => *value as i64,
                other => {
                    return Err(encode_error(format!(
                        "{}.{column} is the record handle but holds {other:?}",
                        table_name(table)
                    )))
                }
            };
            Ok((
                encode_row_key_with_handle(table.id, &RecordHandle::Int(handle)),
                Handle::Int(IntHandle::new(handle)),
            ))
        }
        HandleLayout::Common(columns) => {
            let mut datums = Vec::with_capacity(columns.len());
            for column in columns {
                datums.push(named(column)?.clone());
            }
            let encoded = Encoder::new(use_new_collation)
                .encode_key(&datums)
                .map_err(|error| encode_error(error.to_string()))?;
            let handle = CommonHandle::new(encoded.clone())
                .map_err(|error| encode_error(error.to_string()))?;
            Ok((
                encode_row_key_with_handle(table.id, &RecordHandle::Common(encoded)),
                Handle::Common(handle),
            ))
        }
    }
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
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    store_clustered_row_with_collation(table, existing, values, new_collation_enabled())
}

fn store_clustered_row_with_collation(
    table: &TableInfo,
    existing: Option<&RowValues>,
    values: &RowValues,
    use_new_collation: bool,
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    let (key, handle) = clustered_record_key_with_collation(table, values, use_new_collation)?;
    match existing {
        Some(existing) => rewrite_row(table, &key, &handle, existing, values, use_new_collation),
        None => {
            let mut mutations = vec![OptimisticMutation::insert(key, encode_row(table, values)?)
                .map_err(|error| encode_error(error.to_string()))?];
            mutations.extend(index_entries(
                table,
                &handle,
                values,
                IndexOp::Put,
                use_new_collation,
            )?);
            Ok(mutations)
        }
    }
}

/// The mutations that rewrite one row of a `_tidb_rowid` table in place,
/// keeping its handle.
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
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    rewrite_rowid_row_with_collation(table, key, existing, values, new_collation_enabled())
}

fn rewrite_rowid_row_with_collation(
    table: &TableInfo,
    key: &[u8],
    existing: &RowValues,
    values: &RowValues,
    use_new_collation: bool,
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    let handle = Handle::Int(IntHandle::new(row_id_of(key)?));
    rewrite_row(table, key, &handle, existing, values, use_new_collation)
}

/// The record put and the index moves that turn `existing` into `values`
/// under one unchanged handle.
fn rewrite_row(
    table: &TableInfo,
    key: &[u8],
    handle: &Handle,
    existing: &RowValues,
    values: &RowValues,
    use_new_collation: bool,
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    let mut mutations =
        vec![
            OptimisticMutation::put_existing(key.to_vec(), encode_row(table, values)?)
                .map_err(|error| encode_error(error.to_string()))?,
        ];
    // Retracting the old entry and writing the new one matters only when the
    // indexed value moved. When it did not, both share a key -- and both a
    // duplicate mutation on one key and a retraction of what the write just
    // stored are wrong -- so the unchanged case emits neither.
    let stale = index_entries(table, handle, existing, IndexOp::Delete, use_new_collation)?;
    let fresh = index_entries(table, handle, values, IndexOp::Put, use_new_collation)?;
    for (retract, store) in stale.into_iter().zip(fresh) {
        if retract.key() == store.key() {
            continue;
        }
        mutations.push(retract);
        mutations.push(store);
    }
    Ok(mutations)
}

/// The mutations that remove one row of a clustered table and every index
/// entry it owns.
pub fn delete_clustered_row(
    table: &TableInfo,
    values: &RowValues,
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    delete_clustered_row_with_collation(table, values, new_collation_enabled())
}

fn delete_clustered_row_with_collation(
    table: &TableInfo,
    values: &RowValues,
    use_new_collation: bool,
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    let (key, handle) = clustered_record_key_with_collation(table, values, use_new_collation)?;
    let mut mutations =
        vec![OptimisticMutation::delete(key).map_err(|error| encode_error(error.to_string()))?];
    mutations.extend(index_entries(
        table,
        &handle,
        values,
        IndexOp::Delete,
        use_new_collation,
    )?);
    Ok(mutations)
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum IndexOp {
    Put,
    Delete,
}

fn encode_row(table: &TableInfo, values: &RowValues) -> Result<Vec<u8>, RowEncodeError> {
    // A clustered handle's columns live in the record key and nowhere else --
    // the same rule the reader states in `SystemTableView::project`. A writer
    // that also put them in the value would store a second copy TiDB never
    // wrote.
    let handle = HandleLayout::of(table);
    let key_columns = handle.columns();
    let mut column_ids = Vec::with_capacity(table.columns.len());
    let mut row = Vec::with_capacity(table.columns.len());
    for column in table.cols().iter_deref() {
        let column = column.read();
        if key_columns.iter().any(|key| key == column.name.lowercase()) {
            continue;
        }
        let value = values.get(&column.id).ok_or_else(|| {
            encode_error(format!(
                "{}.{} has no value in the row being written",
                table_name(table),
                column.name.original()
            ))
        })?;
        column_ids.push(column.id);
        row.push(typed_value(value, &column.field_type)?);
    }
    // No time zone, because every `TIMESTAMP` that reaches this module is
    // ALREADY the UTC wall clock: a fresh value comes from
    // `mysql_bootstrap::utc_now_timestamp`, and an edited row's untouched
    // columns came back from a decode that likewise did not convert. The
    // rowcodec's session->UTC step is for a value still in session time; adding
    // it here would shift every stored timestamp by the session's offset.
    encode_table_row(None, &row, &column_ids, true, None)
        .map_err(|error| encode_error(error.to_string()))
}

fn index_entries(
    table: &TableInfo,
    handle: &Handle,
    values: &RowValues,
    op: IndexOp,
    use_new_collation: bool,
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    let handle = handle.clone();
    let codec_table = codec_table_info(table);
    let mut mutations = Vec::new();
    for (position, index) in table.indices.iter_deref().enumerate() {
        let index = index.read();
        let codec_index = &codec_table.indices[position];
        let mut indexed = Vec::with_capacity(index.columns.len());
        for index_column in index.columns.iter_deref() {
            let index_column = index_column.read();
            let offset =
                usize::try_from(index_column.offset).expect("a column offset is not negative");
            if offset >= table.columns.len() {
                return Err(encode_error(format!(
                    "{}'s index `{}` names an offset the table does not have",
                    table_name(table),
                    index.name.original()
                )));
            }
            let column = table
                .columns
                .get(offset)
                .expect("nil *ColumnInfo in TableInfo.Columns");
            let column = column.read();
            let value = values.get(&column.id).ok_or_else(|| {
                encode_error(format!(
                    "{}.{} has no value for index `{}`",
                    table_name(table),
                    column.name.original(),
                    index.name.original()
                ))
            })?;
            indexed.push(collated_value(
                typed_value(value, &column.field_type)?,
                &column.field_type,
            ));
        }
        let (index_key, distinct) = generate_index_key(
            Encoder::new(use_new_collation),
            None,
            &codec_table,
            codec_index,
            table.id,
            &mut indexed,
            Some(&handle),
        )
        .map_err(|error| encode_error(error.to_string()))?;
        let mutation = match op {
            IndexOp::Delete => OptimisticMutation::index_delete(index_key),
            IndexOp::Put => {
                let index_value = generate_index_value(
                    use_new_collation,
                    None,
                    &codec_table,
                    codec_index,
                    // Go `tables.NeedRestoredData`, computed per index rather
                    // than assumed absent: in new-collation mode the index KEY
                    // is a sort key, so for an indexed VARCHAR (or any column
                    // under a case-insensitive collation) this value is the
                    // only place the original bytes survive. Legacy mode makes
                    // the same predicate false and keeps the raw key instead.
                    needs_restored_data(&codec_table, codec_index, use_new_collation),
                    distinct,
                    false,
                    &indexed,
                    &handle,
                    0,
                    &handle_restored_data(
                        table,
                        &codec_table,
                        codec_index,
                        values,
                        use_new_collation,
                    )?,
                )
                .map_err(|error| encode_error(error.to_string()))?;
                OptimisticMutation::index_put(index_key, index_value)
            }
        };
        mutations.push(mutation.map_err(|error| encode_error(error.to_string()))?);
    }
    Ok(mutations)
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

/// The field type one index key part encodes under, Go
/// `model.GetIdxChangingFieldType`: the column's own type, or the type an
/// in-flight `MODIFY COLUMN` is moving it to.
fn indexed_field_type<'a>(
    codec_table: &'a CodecTableInfo,
    index_column: &CodecIndexColumn,
) -> Option<&'a FieldType> {
    let column = codec_table.columns.get(index_column.offset)?;
    if index_column.use_changing_type {
        if let Some(changing) = column.changing_field_type.as_ref() {
            return Some(changing);
        }
    }
    Some(&column.field_type)
}

/// Go `tables.NeedRestoredData`: whether any of this index's key parts stores
/// a column whose new-collation sort key loses the original bytes.
fn needs_restored_data(
    codec_table: &CodecTableInfo,
    codec_index: &CodecIndexInfo,
    use_new_collation: bool,
) -> bool {
    codec_index.columns.iter().any(|index_column| {
        indexed_field_type(codec_table, index_column).is_some_and(|field_type| {
            field_type.need_restored_data_with_collation(use_new_collation)
        })
    })
}

/// Go `tables.TryGetHandleRestoredDataWrapper`: the clustered PRIMARY KEY's
/// own restored data, which a version-1 common-handle table repeats in EVERY
/// secondary index entry so an index-only read can rebuild the handle columns
/// as well as the indexed ones.
fn handle_restored_data(
    table: &TableInfo,
    codec_table: &CodecTableInfo,
    codec_index: &CodecIndexInfo,
    values: &RowValues,
    use_new_collation: bool,
) -> Result<Vec<Datum>, RowEncodeError> {
    if !codec_table.is_common_handle || codec_table.common_handle_version == 0 {
        return Ok(Vec::new());
    }
    let Some(primary) = codec_table.indices.iter().find(|index| index.primary) else {
        return Ok(Vec::new());
    };
    let mut restored = Vec::new();
    for index_column in &primary.columns {
        let column = codec_table
            .columns
            .get(index_column.offset)
            .ok_or_else(|| {
                encode_error(format!(
                    "{}'s primary key names an offset the table does not have",
                    table_name(table)
                ))
            })?;
        if !column
            .field_type
            .need_restored_data_with_collation(use_new_collation)
        {
            continue;
        }
        let stored = values.get(&column.id).ok_or_else(|| {
            encode_error(format!(
                "{}'s primary key column {} has no value in the row being written",
                table_name(table),
                column.id
            ))
        })?;
        let mut value =
            collated_value(typed_value(stored, &column.field_type)?, &column.field_type);
        // Go `TryTruncateRestoredData`: the restored copy is cut to whichever
        // of the primary key's own prefix and the reading index's prefix keeps
        // MORE of the column, so it always covers what either key lost.
        let target = codec_index
            .columns
            .iter()
            .find(|reading| reading.offset == index_column.offset)
            .map_or(index_column, |reading| longer_prefix(index_column, reading));
        truncate_index_value(&mut value, target, column)
            .map_err(|error| encode_error(error.to_string()))?;
        // Go `ConvertDatumToTailSpaceCount`: a bin collation's sort key
        // differs from the data only by the trailing spaces it trimmed, so the
        // COUNT restores it and the string would be a second copy of the key.
        if is_bin_collation(column.field_type.collation_name()) {
            value = Datum::Int(trailing_spaces(&value) as i64);
        }
        restored.push(value);
    }
    Ok(restored)
}

/// Go `tables.maxIndexLen`: the key part that stores MORE of its column, with
/// "no declared prefix" winning outright because it stores the whole column.
fn longer_prefix<'a>(a: &'a CodecIndexColumn, b: &'a CodecIndexColumn) -> &'a CodecIndexColumn {
    if a.length == UNSPECIFIED_LENGTH || b.length == UNSPECIFIED_LENGTH {
        return if a.length == UNSPECIFIED_LENGTH { a } else { b };
    }
    if a.length > b.length {
        a
    } else {
        b
    }
}

/// Go `stringutil.GetTailSpaceCount`.
fn trailing_spaces(value: &Datum) -> usize {
    value
        .as_raw_bytes()
        .map(|bytes| bytes.iter().rev().take_while(|byte| **byte == b' ').count())
        .unwrap_or(0)
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
    materialise(
        column,
        &column.default_value,
        table,
        Some(current_timestamp),
    )
}

/// The datum one column's declared `DEFAULT` materialises to when the caller
/// has no statement clock to evaluate a functional default against.
///
/// [`declared_default`] answers `CURRENT_TIMESTAMP` with the caller's `now`,
/// which is right for a writer materialising one row at one instant and wrong
/// for a *catalog loader*, which would freeze that instant into every future
/// row. This one therefore refuses every non-literal default -- an expression
/// default, and `CURRENT_TIMESTAMP` -- so a loader can admit exactly the
/// columns whose DEFAULT it can carry across verbatim and refuse the rest by
/// name instead of silently dropping it.
pub fn literal_default(column: &ColumnInfo, table: &str) -> Result<Datum, RowEncodeError> {
    if column.default_is_expr {
        return Err(unmaterialisable(column, table));
    }
    if let Some(GoAnyView::String(bytes)) = column.default_value.view() {
        if bytes
            .as_bytes()
            .eq_ignore_ascii_case(CURRENT_TIMESTAMP.as_bytes())
        {
            return Err(unmaterialisable(column, table));
        }
    }
    materialise(column, &column.default_value, table, None)
}

/// The datum one column's `OriginDefaultValue` materialises to.
///
/// This is what a *read* substitutes for a column the stored row has no entry
/// for at all, which is every row written before an `ALTER TABLE ... ADD
/// COLUMN`: Go encodes it into the coprocessor request
/// (`tables.SetPBColumnsDefaultValue` ->
/// `GetColOriginDefaultValueWithoutStrictSQLMode`), so the scan -- and the
/// `ANALYZE` reading through it -- sees the default rather than NULL.
///
/// A nil interface is Go's nil `OriginDefaultValue`, whose read *is* NULL
/// (`pkg/table/column.go`'s `getColDefaultValueFromNil`). Unlike a declared
/// default this one is always a literal: the DDL that added the column
/// evaluated any expression once, at that moment, and stored the result.
pub fn origin_default(column: &ColumnInfo, table: &str) -> Result<Datum, RowEncodeError> {
    let default = column.get_origin_default_value();
    materialise(column, &default, table, None)
}

fn unmaterialisable(column: &ColumnInfo, table: &str) -> RowEncodeError {
    encode_error(format!(
        "{table}.{} declares a default this path cannot materialise",
        column.name.original()
    ))
}

/// The shared literal-to-datum rule. `current_timestamp` is `Some` only for a
/// declared default, the one kind that may still spell `CURRENT_TIMESTAMP`.
fn materialise(
    column: &ColumnInfo,
    declared: &GoAny,
    table: &str,
    current_timestamp: Option<Time>,
) -> Result<Datum, RowEncodeError> {
    let refuse = || unmaterialisable(column, table);
    // A `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` column stores that very word as
    // its default: an `INSERT` evaluates it, so a writer that stores the word
    // instead writes a row TiDB rejects as an `Incorrect time value`.
    if let (Some(GoAnyView::String(bytes)), Some(now)) = (declared.view(), current_timestamp) {
        if bytes
            .as_bytes()
            .eq_ignore_ascii_case(CURRENT_TIMESTAMP.as_bytes())
        {
            return Ok(Datum::new_time(now));
        }
    }
    let Some(default) = declared.view() else {
        // No declared default: an `INSERT` stores NULL, and the column is
        // nullable or the schema would not have parsed.
        return Ok(Datum::Null);
    };
    let datum = match default {
        GoAnyView::Int(value) => Datum::Int(value),
        GoAnyView::Uint(value) => Datum::UInt(value),
        GoAnyView::Bool(value) => Datum::Int(i64::from(value)),
        GoAnyView::Float(value) => Datum::Real(value),
        GoAnyView::String(bytes) => {
            let text = Datum::Bytes(bytes.as_bytes().to_vec());
            // A numeric column's default is stored as its printed form, so it
            // has to be read back as a number before it is encoded as one.
            match column.get_type() {
                FieldTypeCode::Tiny
                | FieldTypeCode::Short
                | FieldTypeCode::Int24
                | FieldTypeCode::Long
                | FieldTypeCode::LongLong
                | FieldTypeCode::Year => {
                    let printed = std::str::from_utf8(bytes.as_bytes()).map_err(|_| refuse())?;
                    let parsed = printed.trim().parse::<i64>().map_err(|_| refuse())?;
                    if column
                        .field_type
                        .has_flag(tidb_datatype::FieldTypeFlags::UNSIGNED)
                    {
                        Datum::UInt(u64::try_from(parsed).map_err(|_| refuse())?)
                    } else {
                        Datum::Int(parsed)
                    }
                }
                _ => typed_value(&text, &column.field_type)?,
            }
        }
        // `types.NewDatum` accepts a dynamic `[]byte` without converting it
        // through text. Copy the visible Go slice bytes while retaining the
        // nil-interface/typed-nil distinction: a typed nil `[]byte` is an
        // empty byte datum, not SQL NULL.
        GoAnyView::Bytes(bytes) => Datum::Bytes(bytes.header().snapshot()),
        GoAnyView::Byte(_)
        | GoAnyView::DefinedString(_, _)
        | GoAnyView::Slice(_)
        | GoAnyView::Map(_)
        | GoAnyView::Pointer(_)
        | GoAnyView::Array(_)
        | GoAnyView::Struct(_)
        | GoAnyView::Custom => return Err(refuse()),
    };
    Ok(datum)
}

/// Re-types a value for the column it lands in.
///
/// Every value a caller states is spelled as text, because that is how Go's
/// own `INSERT` spells it; a column whose declared type is `ENUM` or `SET`
/// needs it as the member(s) it names, not as a string, or the row decodes as
/// the wrong type. Both carry the element's numeric position as well as its
/// name, and that number comes from the column's own declaration -- so a value
/// naming an element the column does not declare is refused rather than
/// written with an invented position.
pub fn typed_value(value: &Datum, field_type: &FieldType) -> Result<Datum, RowEncodeError> {
    let Datum::Bytes(bytes) = value else {
        return Ok(value.clone());
    };
    let code = field_type.code();
    if !matches!(code, FieldTypeCode::Enum | FieldTypeCode::Set) {
        return Ok(value.clone());
    }
    let diagnostic_name = GoString::from(bytes.as_slice()).to_utf8_lossy_go();
    if field_type.elems().is_empty() {
        return Err(RowEncodeError(format!(
            "a {code:?} column that declares no elements cannot store `{diagnostic_name}`"
        )));
    }
    // The stored spelling is the declared one, so name matching runs under the
    // column's own collation -- which is how Go's `ParseEnum`/`ParseSet` do it.
    let collator = field_type.runtime_collator();
    let datum_collation = field_type.collation();
    if code == FieldTypeCode::Enum {
        let member = field_type
            .with_elems_visible(|elements| parse_enum(elements, bytes.as_slice(), collator))
            .map_err(|error| RowEncodeError(error.to_string()))?;
        Ok(Datum::new_enum(member, datum_collation))
    } else {
        // Go stores a SET as the declaration-ordered bit mask plus the joined
        // names; `parse_set_name` computes both, and answers the empty set for
        // the empty string rather than treating it as an unknown element.
        let members = field_type
            .with_elems_visible(|elements| parse_set_name(elements, bytes.as_slice(), collator))
            .map_err(|error| RowEncodeError(error.to_string()))?;
        Ok(Datum::new_set(members, datum_collation))
    }
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
            Datum::Bytes(vec![0xff, 0x80, b'x'])
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
}
