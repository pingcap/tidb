// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! COMPLETE transcreation of Go `pkg/table/tables/mutation_checker.go`.
//!
//! This is the write-path consistency check: given the mem-buffer staging
//! area a single-row change just produced, it decodes every mutation back and
//! proves the row insertion and the index entries still describe the same
//! row. It is the guard that turns a silent data-index corruption into a
//! refused write, so the comparisons here are the whole point of the file --
//! every one of them is byte-level.
//!
//! Go's three checks, all present:
//!
//! 1. [`check_row_insertion_consistency`] -- the encoded row value decodes
//!    back to the datums the caller asked to insert.
//! 2. [`check_handle_consistency`] -- every index entry names the same row
//!    handle the row insertion does.
//! 3. [`check_index_keys`] -- every index entry's decoded key parts equal the
//!    row's values at the indexed offsets, after prefix truncation.
//!
//! # Narrowings, each named
//!
//! - `TableCommon` / `table.Table` / `table.Column`: Go reaches the table
//!   through the concrete `*TableCommon` (not transcreated here) plus the
//!   `table.Column` wrapper around `model.ColumnInfo`. Both narrow to the
//!   [`MutationCheckTable`] trait. `table.Column`'s only extra members (the
//!   generated-column expression and default-value state) are untouched by
//!   this file.
//! - `model.TableInfo` / `model.ColumnInfo` / `model.IndexInfo` /
//!   `model.IndexColumn`: the transcreated `tidb-model` carries each of these
//!   behind `GoShared`/`GoSharedPointerSlice` handles that reproduce Go's
//!   pointer aliasing under a lock. This file only ever READS metadata, and
//!   taking a read guard per field would bury the comparisons that are the
//!   point of the file, so the four types narrow to the plain-data
//!   [`TableMeta`], [`ColumnMeta`], [`IndexMeta`], and [`IndexColumnMeta`]
//!   below, each declaring only the members `mutation_checker.go` reads.
//! - `kv.Transaction`: [`check_data_consistency`] takes `is_pipelined` and a
//!   caller-owned [`ColumnMapsCache`] instead of the transaction. Go stores
//!   the built maps under the `kv.TableToColumnMaps` transaction option; the
//!   Rust `Transaction::OptionValue` is an owner-closed associated type, so
//!   `getColumnMaps` (the txn-option wrapper) is NOT transcreated and
//!   [`get_or_build_column_maps`] -- the getter/setter form Go already
//!   factored out, and the form Go's own test calls -- carries the whole
//!   behavior.
//! - `types.Context`: only `tc.Location()` is read, so a
//!   [`SessionTimeZone`] is passed directly. Go's `Datum.Compare` also takes
//!   the context; the Rust `Datum::compare` needs none.
//! - `table.IndexesLayout` / `IndexRowLayoutOption`: a
//!   `BTreeMap<i64, Vec<usize>>` and a `&[usize]`.
//! - `collate.Collator`: `Datum::compare` takes a [`Collation`], so
//!   [`comparer_collation`] maps Go's `GetCollatorWithCollate` result onto
//!   one. `binCollator` (new collation disabled) is raw byte order, which is
//!   [`Collation::Binary`].
//! - `zap`/`logutil`: every `logutil.BgLogger().Error(...)` beside a returned
//!   error is DROPPED -- the error itself carries the same payload.
//! - `failpoint`: `injectMutationError` is a pure failpoint hook and
//!   `corruptMutations` is the test-only corruption it injects. Both are
//!   DROPPED with the failpoint mechanism; nothing in production calls them.
//! - `BuildRowcodecColInfoForIndexColumns` and `NeedRestoredData` live in Go
//!   `pkg/table/tables/index.go`, not this file; they are pulled in here as
//!   private helpers because `getOrBuildColumnMaps` and `checkIndexKeys`
//!   cannot run without them.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use tidb_codec::table_key::{decode_index_key, decode_row_key, RecordHandle};
use tidb_codec::ColumnInfo as RowColInfo;
use tidb_codec::{decode_table_id, is_row_key};
use tidb_datatype::{
    get_collator_with_mode, Collation, Collator, Datum, FieldType, FieldTypeFlags, SessionTimeZone,
};
use tidb_tablecodec::{
    decode_column_value, decode_index_handle, decode_index_kv, decode_table_row_to_map,
    decode_temp_index_value, temp_index_key_to_index_key, temp_index_value_is_untouched,
    truncate_index_value, HandleStatus, IndexColumn as CodecIndexColumn,
    TableColumn as CodecTableColumn, INDEX_ID_MASK,
};
use tidb_txnkv::{CommonHandle, Handle, IntHandle, KeyFlags, MemBuffer, StagingHandle};

/// boundary: Go `pkg/meta/model.IndexColumn`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexColumnMeta {
    /// Go `Name.O`.
    pub name: String,
    /// Go `Offset`, the position in `TableInfo.Columns`.
    pub offset: usize,
    /// Go `Length`, the prefix length or `types.UnspecifiedLength`.
    pub length: i64,
    /// Go `UseChangingType`.
    pub use_changing_type: bool,
}

/// boundary: Go `pkg/meta/model.IndexInfo`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexMeta {
    /// Go `ID`.
    pub id: i64,
    /// Go `Name.O`.
    pub name: String,
    /// Go `Columns`.
    pub columns: Vec<IndexColumnMeta>,
    /// Go `Primary`.
    pub primary: bool,
    /// Go `Unique`.
    pub unique: bool,
}

/// boundary: Go `pkg/table.Column`, itself a `*model.ColumnInfo`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnMeta {
    /// Go `ID`.
    pub id: i64,
    /// Go `Name.O`.
    pub name: String,
    /// Go `Offset`.
    pub offset: usize,
    /// Go `FieldType`.
    pub field_type: FieldType,
    /// Go `ChangingFieldType`, set only mid-way through a MODIFY COLUMN.
    pub changing_field_type: Option<FieldType>,
}

/// boundary: Go `pkg/meta/model.TableInfo`, restricted to what this file
/// reads: the name for error messages, the columns and indices, and the two
/// flags that steer the checks.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableMeta {
    /// Go `ID`.
    pub id: i64,
    /// Go `Name.O`.
    pub name: String,
    /// Go `Columns`.
    pub columns: Vec<ColumnMeta>,
    /// Go `Indices`.
    pub indices: Vec<IndexMeta>,
    /// Go `PKIsHandle`.
    pub pk_is_handle: bool,
    /// Go `IsCommonHandle`.
    pub is_common_handle: bool,
    /// Go `GetPartitionInfo() != nil`.
    pub partitioned: bool,
}

/// Go `mutation`: one staged key/value write, plus the index it belongs to.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Mutation {
    /// Go `key`.
    pub key: Vec<u8>,
    /// Go `flags`.
    pub flags: KeyFlags,
    /// Go `value`. An empty value is a deletion.
    pub value: Vec<u8>,
    /// Go `indexID`, zero for row mutations.
    pub index_id: i64,
}

/// Go `columnMaps`: the per-table lookup tables the checks need.
///
/// Go stores raw pointers into the `model.TableInfo`; Rust clones the
/// metadata into the map, which is the same lookup with an owned lifetime.
#[derive(Clone, Debug, Default)]
pub struct ColumnMaps {
    /// Go `ColumnIDToInfo`.
    pub column_id_to_info: BTreeMap<i64, ColumnMeta>,
    /// Go `ColumnIDToFieldType`.
    pub column_id_to_field_type: BTreeMap<i64, FieldType>,
    /// Go `IndexIDToInfo`.
    pub index_id_to_info: BTreeMap<i64, IndexMeta>,
    /// Go `IndexIDToRowColInfos`.
    pub index_id_to_row_col_infos: BTreeMap<i64, Vec<RowColInfo>>,
}

/// boundary: Go's `kv.TableToColumnMaps` transaction option value,
/// `map[int64]columnMaps` keyed by table ID.
pub type ColumnMapsCache = BTreeMap<i64, ColumnMaps>;

/// boundary: Go `pkg/table.IndexesLayout`.
pub type IndexesLayout = BTreeMap<i64, Vec<usize>>;

/// boundary: the `*TableCommon` receiver `mutation_checker.go` reads.
///
/// Only the six members this file touches are declared. `pkg/table.Table` and
/// `pkg/table.Column` are not otherwise transcreated in this crate.
pub trait MutationCheckTable {
    /// Go `t.Meta()`.
    fn meta(&self) -> &TableMeta;
    /// Go `t.tableID`, the logical table the column maps are cached under.
    fn table_id(&self) -> i64;
    /// Go `t.physicalTableID`, the partition a mutation must belong to.
    fn physical_table_id(&self) -> i64;
    /// Go `t.Columns`, the writable columns in table offset order.
    fn columns(&self) -> &[ColumnMeta];
    /// Go `t.Indices()`, the index metadata the table currently writes.
    fn index_metas(&self) -> Vec<&IndexMeta>;
    /// Go `t.encoder.UseNewCollate()`.
    fn use_new_collate(&self) -> bool;
}

/// Why a set of mutations was refused, or could not be decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MutationCheckError {
    /// Go `ErrInconsistentRowValue` (errno 8138).
    InconsistentRowValue {
        /// Table name.
        table: String,
        /// The datum the caller asked to write.
        expected: String,
        /// The datum the encoded row decodes back to.
        record: String,
    },
    /// Go `ErrInconsistentHandle` (errno 8139).
    InconsistentHandle {
        /// Table name.
        table: String,
        /// Index name.
        index: String,
        /// The handle the index entry names.
        index_handle: String,
        /// The handle the row insertion names.
        record_handle: String,
    },
    /// Go `ErrInconsistentIndexedValue` (errno 8140).
    InconsistentIndexedValue {
        /// Table name.
        table: String,
        /// Index name.
        index: String,
        /// Indexed column name.
        column: String,
        /// The value decoded out of the index entry.
        indexed: String,
        /// The value the row carries.
        record: String,
    },
    /// Go `errors.New("index not found")`.
    IndexNotFound(i64),
    /// Go `errors.Errorf("multiple row mutations added/mutated, ...")`.
    MultipleRowMutations,
    /// Any decode failure Go wraps with `errors.Trace`.
    Decode(String),
}

impl MutationCheckError {
    /// The MySQL error number Go's `dbterror.ClassTable.NewStd` attaches, for
    /// the three checks that raise one.
    #[must_use]
    pub const fn code(&self) -> Option<u16> {
        match self {
            Self::InconsistentRowValue { .. } => Some(8138),
            Self::InconsistentHandle { .. } => Some(8139),
            Self::InconsistentIndexedValue { .. } => Some(8140),
            _ => None,
        }
    }
}

impl std::fmt::Display for MutationCheckError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // errno.ErrInconsistentRowValue's template. Go's trailing
            // `record-values` is the decoded datum.
            Self::InconsistentRowValue {
                table,
                expected,
                record,
            } => write!(
                formatter,
                "writing inconsistent data in table: {table}, \
                 expected-values:{{{expected}}} != record-values:{{{record}}}"
            ),
            // errno.ErrInconsistentHandle's template. Go also renders the two
            // raw `mutation` structs with `%#v`; those are Go struct literals
            // with no Rust counterpart and are dropped from the message.
            Self::InconsistentHandle {
                table,
                index,
                index_handle,
                record_handle,
            } => write!(
                formatter,
                "writing inconsistent data in table: {table}, index: {index}, \
                 index-handle:{index_handle} != record-handle:{record_handle}"
            ),
            // errno.ErrInconsistentIndexedValue's template.
            Self::InconsistentIndexedValue {
                table,
                index,
                column,
                indexed,
                record,
            } => write!(
                formatter,
                "writing inconsistent data in table: {table}, index: {index}, col: {column}, \
                 indexed-value:{{{indexed}}} != record-value:{{{record}}}"
            ),
            Self::IndexNotFound(_) => formatter.write_str("index not found"),
            Self::MultipleRowMutations => {
                formatter.write_str("multiple row mutations added/mutated")
            }
            Self::Decode(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for MutationCheckError {}

fn decode_error(error: impl std::fmt::Display) -> MutationCheckError {
    MutationCheckError::Decode(error.to_string())
}

fn datum_text(datum: &Datum) -> String {
    // Go `types.Datum.String()`; `sql_string` is its byte-preserving form.
    datum.sql_string().unwrap_or_else(|_| datum.label())
}

/// Go `CheckDataConsistency` / `checkDataConsistency`.
///
/// Whether the given set of mutations for a single row is consistent: the
/// handle agrees between row and index insertions, and each index entry's
/// key parts agree with the row values.
///
/// Returns `Ok(())` without checking anything when the table is partitioned,
/// the transaction is pipelined, or the mem buffer has no staging area --
/// Go's three early returns.
///
/// `is_pipelined` stands in for Go's `txn.IsPipelined()` and `maps_cache` for
/// the `kv.TableToColumnMaps` transaction option; see the module narrowings.
#[allow(clippy::too_many_arguments)]
pub fn check_data_consistency<T, M>(
    is_pipelined: bool,
    timezone: Option<&SessionTimeZone>,
    table: &T,
    row_to_insert: Option<&[Datum]>,
    row_to_remove: Option<&[Datum]>,
    mem_buffer: &M,
    staging: Option<StagingHandle>,
    maps_cache: &mut ColumnMapsCache,
    extra_indexes_layout: Option<&IndexesLayout>,
) -> Result<(), MutationCheckError>
where
    T: MutationCheckTable + ?Sized,
    M: MemBuffer + ?Sized,
{
    if table.meta().partitioned {
        // Go: "TODO: Support check for partitions as well".
        return Ok(());
    }
    if is_pipelined {
        return Ok(());
    }
    // Go `sh == 0`: some MemBuffer implementations do not support staging.
    let Some(staging) = staging else {
        return Ok(());
    };
    let (index_mutations, row_insertion) =
        collect_table_mutations_from_buffer_stage(table.physical_table_id(), mem_buffer, staging)?;

    let column_maps = get_or_build_column_maps(maps_cache, table);

    // Go disables the row-insertion check here: it "contributes the least to
    // defending data-index consistency, but costs most CPU resources". The
    // call stays commented out in Go and stays absent here;
    // `check_row_insertion_consistency` is still transcreated and tested.

    if let Some(row_insertion) = row_insertion.as_ref() {
        check_handle_consistency(
            row_insertion,
            &index_mutations,
            &column_maps.index_id_to_info,
            table.meta(),
        )?;
    }

    check_index_keys(
        timezone,
        table,
        row_to_insert,
        row_to_remove,
        &index_mutations,
        &column_maps.index_id_to_info,
        &column_maps.index_id_to_row_col_infos,
        extra_indexes_layout,
    )
}

/// Go `checkHandleConsistency`.
///
/// A `PUT_index` implies a `PUT_row` with the same handle. Deletions are not
/// checked, since the values of deletions are unknown.
pub fn check_handle_consistency(
    row_insertion: &Mutation,
    index_mutations: &[Mutation],
    index_id_to_info: &BTreeMap<i64, IndexMeta>,
    table_info: &TableMeta,
) -> Result<(), MutationCheckError> {
    if row_insertion.key.is_empty() {
        return Ok(());
    }
    let insertion_handle =
        record_handle_to_kv_handle(decode_row_key(&row_insertion.key).map_err(decode_error)?)?;

    for mutation in index_mutations {
        if mutation.value.is_empty() {
            continue;
        }

        // Generate correct index id for check.
        let index_id = mutation.index_id & INDEX_ID_MASK;
        let index_info = index_id_to_info
            .get(&index_id)
            .ok_or(MutationCheckError::IndexNotFound(index_id))?;

        // If this is the temporary index data, the trailing version byte has
        // to come off before the value can be read as an ordinary one.
        let index_handle = if index_id == mutation.index_id {
            decode_index_handle(&mutation.key, &mutation.value, index_info.columns.len())
                .map_err(decode_error)?
        } else {
            if temp_index_value_is_untouched(&mutation.value) {
                // We never commit the untouched key values to the storage.
                continue;
            }
            let temp_value = decode_temp_index_value(&mutation.value).map_err(decode_error)?;
            let value = temp_value
                .last()
                .map(|element| element.value.clone())
                .unwrap_or_default();
            if value.is_empty() {
                // Skip the deleted operation values.
                continue;
            }
            let mut original_key = mutation.key.clone();
            temp_index_key_to_index_key(&mut original_key).map_err(decode_error)?;
            decode_index_handle(&original_key, &value, index_info.columns.len())
                .map_err(decode_error)?
        };

        // NOTE: handle type can be different, see issue 29520.
        if index_handle.is_int() == insertion_handle.is_int()
            && index_handle
                .compare(&insertion_handle)
                .map_err(decode_error)?
                != Ordering::Equal
        {
            return Err(MutationCheckError::InconsistentHandle {
                table: table_info.name.clone(),
                index: index_info.name.clone(),
                index_handle: handle_text(&index_handle),
                record_handle: handle_text(&insertion_handle),
            });
        }
    }

    Ok(())
}

/// Go `checkIndexKeys`.
///
/// Assume the set of row values changes from `V1` to `V2`. This proves
/// `V2 - V1 = {added indices}` in the direction Go checks: every index
/// mutation is consistent with the input row key/value. The reverse
/// containment is exactly how the mutations were generated, so Go skips it
/// and so does this.
#[allow(clippy::too_many_arguments)]
pub fn check_index_keys<T>(
    timezone: Option<&SessionTimeZone>,
    table: &T,
    row_to_insert: Option<&[Datum]>,
    row_to_remove: Option<&[Datum]>,
    index_mutations: &[Mutation],
    index_id_to_info: &BTreeMap<i64, IndexMeta>,
    index_id_to_row_col_infos: &BTreeMap<i64, Vec<RowColInfo>>,
    extra_indexes_layout: Option<&IndexesLayout>,
) -> Result<(), MutationCheckError>
where
    T: MutationCheckTable + ?Sized,
{
    let use_new_collate = table.use_new_collate();
    let mut index_data: Vec<Datum> = Vec::new();
    for mutation in index_mutations {
        // Generate correct index id for check.
        let index_id = mutation.index_id & INDEX_ID_MASK;
        let index_info = index_id_to_info
            .get(&index_id)
            .ok_or(MutationCheckError::IndexNotFound(index_id))?;
        let row_col_infos = index_id_to_row_col_infos
            .get(&index_id)
            .ok_or(MutationCheckError::IndexNotFound(index_id))?;

        let mut is_tmp_idx_val_and_deleted = false;
        // If this is temp index data, the trailing version byte comes off.
        let value = if index_id == mutation.index_id {
            mutation.value.clone()
        } else {
            if temp_index_value_is_untouched(&mutation.value) {
                // We never commit the untouched key values to the storage.
                continue;
            }
            let temp_value = decode_temp_index_value(&mutation.value).map_err(decode_error)?;
            match temp_value.last() {
                Some(current) => {
                    is_tmp_idx_val_and_deleted = current.delete;
                    current.value.clone()
                }
                None => Vec::new(),
            }
        };

        // When we cannot decode the key to get the original value.
        if value.is_empty()
            && need_restored_data(use_new_collate, index_info, &table.meta().columns)
        {
            continue;
        }

        let decoded_index_values = decode_index_kv(
            use_new_collate,
            &mutation.key,
            &value,
            index_info.columns.len(),
            HandleStatus::NotNeeded,
            row_col_infos,
        )
        .map_err(decode_error)?;

        // Reuse the underlying memory, save an allocation.
        index_data.clear();
        index_data.reserve(decoded_index_values.len());
        for (position, encoded) in decoded_index_values.iter().enumerate() {
            let offset = index_column_offset(index_info, position)?;
            let column = table
                .columns()
                .get(offset)
                .ok_or(MutationCheckError::IndexNotFound(index_id))?;
            let field_type = column.field_type.array_type();
            index_data
                .push(decode_column_value(encoded, &field_type, timezone).map_err(decode_error)?);
        }

        let extra_index_layout = extra_indexes_layout
            .and_then(|layout| layout.get(&index_id))
            .map(Vec::as_slice);
        // When it is in add index new backfill state, the mutation describes
        // the row being removed rather than the row being written.
        let input = if value.is_empty() || is_tmp_idx_val_and_deleted {
            row_to_remove
        } else {
            row_to_insert
        };
        let Some(input) = input else {
            continue;
        };
        compare_index_data(
            use_new_collate,
            table.columns(),
            &index_data,
            input,
            index_info,
            table.meta(),
            extra_index_layout,
        )?;
    }
    Ok(())
}

/// Go `checkRowInsertionConsistency`.
///
/// Only added data is checked: a deletion does not care about its value, and
/// the value cannot be known anyway.
pub fn check_row_insertion_consistency(
    timezone: Option<&SessionTimeZone>,
    row_to_insert: Option<&[Datum]>,
    row_insertion: &Mutation,
    column_id_to_info: &BTreeMap<i64, ColumnMeta>,
    column_id_to_field_type: &BTreeMap<i64, FieldType>,
    table_name: &str,
) -> Result<(), MutationCheckError> {
    let Some(row_to_insert) = row_to_insert else {
        // It's a deletion.
        return Ok(());
    };

    let decoded_data =
        decode_table_row_to_map(&row_insertion.value, column_id_to_field_type, timezone)
            .map_err(decode_error)?;

    // NOTE: we cannot check that the decoded values contain all columns,
    // since some columns may be skipped -- it can even be empty. Instead we
    // check that the decoded values are consistent with the input row.

    for (column_id, decoded_datum) in &decoded_data {
        let info = column_id_to_info
            .get(column_id)
            .ok_or_else(|| MutationCheckError::Decode(format!("column {column_id} not found")))?;
        let input_datum = row_to_insert.get(info.offset).ok_or_else(|| {
            MutationCheckError::Decode(format!("column offset {} out of row range", info.offset))
        })?;
        let comparer = decoded_datum.collation().unwrap_or(Collation::Utf8Mb4Bin);
        let ordering = decoded_datum
            .compare(input_datum, comparer)
            .map_err(decode_error)?;
        if ordering != Ordering::Equal {
            return Err(MutationCheckError::InconsistentRowValue {
                table: table_name.to_owned(),
                expected: datum_text(input_datum),
                record: datum_text(decoded_datum),
            });
        }
    }
    Ok(())
}

/// Go `collectTableMutationsFromBufferStage`.
///
/// Returns every index mutation for `physical_table_id` plus the single row
/// insertion, if there is one. Multiple row insertions are an error.
pub fn collect_table_mutations_from_buffer_stage<M>(
    physical_table_id: i64,
    mem_buffer: &M,
    staging: StagingHandle,
) -> Result<(Vec<Mutation>, Option<Mutation>), MutationCheckError>
where
    M: MemBuffer + ?Sized,
{
    let mut index_mutations: Vec<Mutation> = Vec::new();
    let mut row_insertion: Option<Mutation> = None;
    let mut failure: Option<MutationCheckError> = None;
    mem_buffer.inspect_stage(staging, &mut |key, flags, data| {
        // Only check the current table.
        if decode_table_id(key.as_bytes()) != physical_table_id {
            return;
        }
        let mut mutation = Mutation {
            key: key.as_bytes().to_vec(),
            flags,
            value: data.to_vec(),
            index_id: 0,
        };
        if is_row_key(key.as_bytes()) {
            if !data.is_empty() {
                if row_insertion.is_none() {
                    row_insertion = Some(mutation);
                } else if failure.is_none() {
                    failure = Some(MutationCheckError::MultipleRowMutations);
                }
            }
            return;
        }
        match decode_index_key(&mutation.key) {
            Ok((_, index_id, _)) => mutation.index_id = index_id,
            Err(error) => {
                if failure.is_none() {
                    failure = Some(decode_error(error));
                }
            }
        }
        index_mutations.push(mutation);
    });
    match failure {
        Some(error) => Err(error),
        None => Ok((index_mutations, row_insertion)),
    }
}

/// Go `compareIndexData`.
///
/// Returns an error unless the decoded index data is a subset of the input
/// row data, comparing after both sides go through the index's prefix
/// truncation.
#[allow(clippy::too_many_arguments)]
pub fn compare_index_data(
    use_new_collate: bool,
    columns: &[ColumnMeta],
    index_data: &[Datum],
    input: &[Datum],
    index_info: &IndexMeta,
    table_info: &TableMeta,
    extra_index_layout: Option<&[usize]>,
) -> Result<(), MutationCheckError> {
    for (position, decoded) in index_data.iter().enumerate() {
        let offset_in_table = index_column_offset(index_info, position)?;
        let offset_in_row = match extra_index_layout {
            Some(layout) if !layout.is_empty() => *layout.get(position).ok_or_else(|| {
                MutationCheckError::Decode("index layout shorter than index".to_owned())
            })?,
            _ => offset_in_table,
        };
        let index_column = index_info.columns.get(position).ok_or_else(|| {
            MutationCheckError::Decode("index column position out of range".to_owned())
        })?;
        let column = columns.get(offset_in_table).ok_or_else(|| {
            MutationCheckError::Decode("index column offset out of table range".to_owned())
        })?;
        let mut decoded_mutation_datum = decoded.clone();
        let mut expected_datum = input
            .get(offset_in_row)
            .ok_or_else(|| {
                MutationCheckError::Decode("index column offset out of row range".to_owned())
            })?
            .clone();

        let codec_index_column = codec_index_column(index_column);
        let codec_table_column = codec_table_column(column);
        truncate_index_value(
            &mut expected_datum,
            &codec_index_column,
            &codec_table_column,
        )
        .map_err(decode_error)?;
        truncate_index_value(
            &mut decoded_mutation_datum,
            &codec_index_column,
            &codec_table_column,
        )
        .map_err(decode_error)?;

        let collator = get_collator_with_mode(
            use_new_collate,
            decoded_mutation_datum
                .collation()
                .unwrap_or(Collation::Binary)
                .name(),
        );
        let compare_mv_index =
            column.field_type.is_array() && matches!(expected_datum, Datum::Json(_));
        let comparison = compare_index_and_val(
            &expected_datum,
            &decoded_mutation_datum,
            collator,
            compare_mv_index,
        )?;

        if comparison != Ordering::Equal {
            return Err(MutationCheckError::InconsistentIndexedValue {
                table: table_info.name.clone(),
                index: index_info.name.clone(),
                column: column.name.clone(),
                indexed: datum_text(&decoded_mutation_datum),
                record: datum_text(&expected_datum),
            });
        }
    }
    Ok(())
}

/// Go `CompareIndexAndVal`: compare an indexed value against a row value.
///
/// With `compare_mv_index`, the row value is a JSON array and the index
/// entry stores one of its elements, so the row value CONTAINS the indexed
/// value when any element compares equal. Go stops at the first match and
/// otherwise keeps the last element's comparison, which this reproduces.
pub fn compare_index_and_val(
    row_val: &Datum,
    idx_val: &Datum,
    collator: Collator,
    compare_mv_index: bool,
) -> Result<Ordering, MutationCheckError> {
    if !compare_mv_index {
        return idx_val
            .compare(row_val, comparer_collation(collator))
            .map_err(decode_error);
    }
    let Datum::Json(json) = row_val else {
        return Err(MutationCheckError::Decode(
            "multi-valued index compared against a non-JSON row value".to_owned(),
        ));
    };
    let count = json.element_count().map_err(decode_error)?;
    // Go's zero-length array leaves `cmpRes` at its zero value, which is the
    // equal comparison.
    let mut comparison = Ordering::Equal;
    for element in 0..count {
        let Some(item) = json.array_get(element).map_err(decode_error)? else {
            continue;
        };
        let json_datum = Datum::new_json(item);
        comparison = json_datum
            .compare(idx_val, Collation::Binary)
            .map_err(decode_error)?;
        if comparison == Ordering::Equal {
            break;
        }
    }
    Ok(comparison)
}

/// Go `getOrBuildColumnMaps`.
///
/// Go passes a getter/setter pair so the maps can be cached on the
/// transaction; here the cache is passed directly, which is the same store
/// with one less indirection.
pub fn get_or_build_column_maps<T>(cache: &mut ColumnMapsCache, table: &T) -> ColumnMaps
where
    T: MutationCheckTable + ?Sized,
{
    if let Some(existing) = cache.get(&table.table_id()) {
        return existing.clone();
    }
    let meta = table.meta();
    let mut maps = ColumnMaps::default();
    for column in &meta.columns {
        maps.column_id_to_info.insert(column.id, column.clone());
        maps.column_id_to_field_type
            .insert(column.id, column.field_type.clone());
    }
    for index in table.index_metas() {
        if index.primary && meta.is_common_handle {
            continue;
        }
        maps.index_id_to_info.insert(index.id, index.clone());
        maps.index_id_to_row_col_infos.insert(
            index.id,
            build_rowcodec_col_info_for_index_columns(index, meta),
        );
    }
    cache.insert(table.table_id(), maps.clone());
    maps
}

/// Go `tables.BuildRowcodecColInfoForIndexColumns` (`index.go`), needed here
/// by [`get_or_build_column_maps`].
fn build_rowcodec_col_info_for_index_columns(
    index_info: &IndexMeta,
    table_info: &TableMeta,
) -> Vec<RowColInfo> {
    let mut infos = Vec::with_capacity(index_info.columns.len());
    for index_column in &index_info.columns {
        let Some(column) = table_info.columns.get(index_column.offset) else {
            continue;
        };
        let field_type = idx_changing_field_type(index_column.use_changing_type, column).clone();
        infos.push(RowColInfo {
            id: column.id,
            is_pk_handle: table_info.pk_is_handle && field_type.has_flag(FieldTypeFlags::PRI_KEY),
            virtual_generated: false,
            field_type,
        });
    }
    infos
}

/// Go `tables.NeedRestoredData` (`index.go`), needed here by
/// [`check_index_keys`].
fn need_restored_data(
    use_new_collate: bool,
    index_info: &IndexMeta,
    columns: &[ColumnMeta],
) -> bool {
    index_info.columns.iter().any(|index_column| {
        columns.get(index_column.offset).is_some_and(|column| {
            idx_changing_field_type(index_column.use_changing_type, column)
                .need_restored_data_with_collation(use_new_collate)
        })
    })
}

/// Go `model.GetIdxChangingFieldType`.
fn idx_changing_field_type(use_changing_type: bool, column: &ColumnMeta) -> &FieldType {
    if use_changing_type {
        if let Some(changing) = column.changing_field_type.as_ref() {
            return changing;
        }
    }
    &column.field_type
}

/// Maps Go's `collate.Collator` onto the collation `Datum::compare` takes.
///
/// `binCollator` -- what `GetCollatorWithCollate` returns when new collation
/// is disabled -- orders raw bytes, which is [`Collation::Binary`].
fn comparer_collation(collator: Collator) -> Collation {
    collator.new_collation().unwrap_or(Collation::Binary)
}

fn codec_index_column(index_column: &IndexColumnMeta) -> CodecIndexColumn {
    CodecIndexColumn {
        offset: index_column.offset,
        length: index_column.length,
        use_changing_type: index_column.use_changing_type,
    }
}

fn codec_table_column(column: &ColumnMeta) -> CodecTableColumn {
    CodecTableColumn {
        id: column.id,
        offset: column.offset,
        primary_key: column.field_type.has_flag(FieldTypeFlags::PRI_KEY),
        field_type: column.field_type.clone(),
        changing_field_type: column.changing_field_type.clone(),
    }
}

fn index_column_offset(
    index_info: &IndexMeta,
    position: usize,
) -> Result<usize, MutationCheckError> {
    index_info
        .columns
        .get(position)
        .map(|index_column| index_column.offset)
        .ok_or_else(|| MutationCheckError::Decode("index column position out of range".to_owned()))
}

/// Bridges the wire-decoding `RecordHandle` onto the canonical KV handle the
/// index side already produces, so the two can be compared at all.
fn record_handle_to_kv_handle(handle: RecordHandle) -> Result<Handle, MutationCheckError> {
    Ok(match handle {
        RecordHandle::Int(value) => Handle::Int(IntHandle::new(value)),
        RecordHandle::Common(encoded) => {
            Handle::Common(CommonHandle::new(encoded).map_err(decode_error)?)
        }
        RecordHandle::Partition { handle, .. } => record_handle_to_kv_handle(*handle)?,
    })
}

fn handle_text(handle: &Handle) -> String {
    match handle.int_value() {
        Some(value) => value.to_string(),
        None => format!("{:?}", handle.encoded()),
    }
}

#[cfg(test)]
mod tests {
    //! Go `pkg/table/tables/mutation_checker_test.go`, all three tests.

    use super::*;
    use tidb_codec::{encode_row_key, Encoder};
    use tidb_datatype::{
        new_collation_enabled, CoreTime, FieldTypeCode, Time, TimeType, UNSPECIFIED_LENGTH,
    };
    use tidb_tablecodec::{
        encode_table_row, generate_index_key, generate_index_value, IndexInfo as CodecIndexInfo,
        TableInfo as CodecTableInfo,
    };

    /// The concrete `*TableCommon` Go's tests build with `TableFromMeta`.
    struct TestTable {
        meta: TableMeta,
        use_new_collate: bool,
    }

    impl MutationCheckTable for TestTable {
        fn meta(&self) -> &TableMeta {
            &self.meta
        }
        fn table_id(&self) -> i64 {
            self.meta.id
        }
        fn physical_table_id(&self) -> i64 {
            self.meta.id
        }
        fn columns(&self) -> &[ColumnMeta] {
            &self.meta.columns
        }
        fn index_metas(&self) -> Vec<&IndexMeta> {
            self.meta.indices.iter().collect()
        }
        fn use_new_collate(&self) -> bool {
            self.use_new_collate
        }
    }

    fn column(id: i64, name: &str, offset: usize, field_type: FieldType) -> ColumnMeta {
        ColumnMeta {
            id,
            name: name.to_owned(),
            offset,
            field_type,
            changing_field_type: None,
        }
    }

    /// Go `TestCompareIndexData`.
    ///
    /// The domain of `compareIndexData` is (1) the column types that influence
    /// truncating values and (2) the comparison of row data against index data.
    #[test]
    fn compare_index_data_matches_go() {
        struct CaseData {
            index_data: Vec<Datum>,
            input_data: Vec<Datum>,
            field_types: Vec<FieldType>,
            index_length: Vec<i64>,
            correct: bool,
        }

        // Assume the index is on all columns.
        let test_data = [
            CaseData {
                index_data: vec![Datum::new_int(1), Datum::new_string("some string")],
                input_data: vec![Datum::new_int(1), Datum::new_string("some string")],
                field_types: vec![
                    FieldType::new(FieldTypeCode::Short),
                    FieldType::new(FieldTypeCode::String),
                ],
                index_length: vec![UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH],
                correct: true,
            },
            CaseData {
                index_data: vec![Datum::new_int(1), Datum::new_string("some string")],
                input_data: vec![Datum::new_int(1), Datum::new_string("some string2")],
                field_types: vec![
                    FieldType::new(FieldTypeCode::Short),
                    FieldType::new(FieldTypeCode::String),
                ],
                index_length: vec![UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH],
                correct: false,
            },
            CaseData {
                index_data: vec![Datum::new_int(1), Datum::new_string("some string")],
                input_data: vec![Datum::new_int(1), Datum::new_string("some string2")],
                field_types: vec![
                    FieldType::new(FieldTypeCode::Short),
                    FieldType::new(FieldTypeCode::String),
                ],
                index_length: vec![UNSPECIFIED_LENGTH, 11],
                correct: true,
            },
        ];

        for (case_id, data) in test_data.iter().enumerate() {
            let mut columns = Vec::new();
            let mut index_columns = Vec::new();
            for (offset, field_type) in data.field_types.iter().enumerate() {
                columns.push(column(0, &format!("c{offset}"), offset, field_type.clone()));
                index_columns.push(IndexColumnMeta {
                    name: String::new(),
                    offset,
                    length: data.index_length[offset],
                    use_changing_type: false,
                });
            }
            let index_info = IndexMeta {
                id: 0,
                name: "i0".to_owned(),
                columns: index_columns,
                primary: false,
                unique: false,
            };
            let table_info = TableMeta {
                name: "t".to_owned(),
                ..TableMeta::default()
            };

            let result = compare_index_data(
                new_collation_enabled(),
                &columns,
                &data.index_data,
                &data.input_data,
                &index_info,
                &table_info,
                None,
            );
            assert_eq!(data.correct, result.is_ok(), "case id = {case_id}");
        }
    }

    /// Go `TestCheckRowInsertionConsistency`.
    #[test]
    fn check_row_insertion_consistency_matches_go() {
        let timezone = SessionTimeZone::utc();
        // Go `rowcodec.Encoder{Enable: true}`: the new row format.
        let mock_row_key_233 = encode_row_key(1, &IntHandle::new(233).encoded());
        let mock_value_233 =
            encode_table_row(Some(&timezone), &[Datum::new_int(233)], &[101], true, None).unwrap();
        let fake_row_insertion = Mutation {
            key: vec![1, 1],
            value: vec![1, 1, 1],
            ..Mutation::default()
        };

        struct CaseData {
            column_id_to_info: BTreeMap<i64, ColumnMeta>,
            column_id_to_field_type: BTreeMap<i64, FieldType>,
            row_to_insert: Option<Vec<Datum>>,
            row_insertion: Mutation,
            correct: bool,
        }

        let short_column = || {
            BTreeMap::from([(
                101,
                ColumnMeta {
                    id: 101,
                    name: String::new(),
                    offset: 0,
                    field_type: FieldType::new(FieldTypeCode::Short),
                    changing_field_type: None,
                },
            )])
        };
        let short_field_type = || BTreeMap::from([(101, FieldType::new(FieldTypeCode::Short))]);

        let test_data = [
            // Expected correct behavior.
            CaseData {
                column_id_to_info: short_column(),
                column_id_to_field_type: short_field_type(),
                row_to_insert: Some(vec![Datum::new_int(233)]),
                row_insertion: Mutation {
                    key: mock_row_key_233.clone(),
                    value: mock_value_233.clone(),
                    ..Mutation::default()
                },
                correct: true,
            },
            // Mismatching mutation.
            CaseData {
                column_id_to_info: short_column(),
                column_id_to_field_type: short_field_type(),
                row_to_insert: Some(vec![Datum::new_int(1)]),
                row_insertion: fake_row_insertion.clone(),
                correct: false,
            },
            // No input row.
            CaseData {
                column_id_to_info: BTreeMap::new(),
                column_id_to_field_type: BTreeMap::new(),
                row_to_insert: None,
                row_insertion: fake_row_insertion,
                correct: true,
            },
            // Invalid value.
            CaseData {
                column_id_to_info: short_column(),
                column_id_to_field_type: short_field_type(),
                row_to_insert: Some(vec![Datum::new_int(233)]),
                row_insertion: Mutation {
                    key: mock_row_key_233,
                    value: vec![0, 1, 2, 3],
                    ..Mutation::default()
                },
                correct: false,
            },
        ];

        for (case_id, data) in test_data.iter().enumerate() {
            let result = check_row_insertion_consistency(
                Some(&timezone),
                data.row_to_insert.as_deref(),
                &data.row_insertion,
                &data.column_id_to_info,
                &data.column_id_to_field_type,
                "t",
            );
            assert_eq!(data.correct, result.is_ok(), "case id = {case_id}");
        }
    }

    fn codec_table_info(meta: &TableMeta) -> CodecTableInfo {
        CodecTableInfo {
            columns: meta.columns.iter().map(codec_table_column).collect(),
            indices: meta.indices.iter().map(codec_index_info).collect(),
            pk_is_handle: meta.pk_is_handle,
            is_common_handle: meta.is_common_handle,
            // Go's test leaves `CommonHandleVersion` at its zero value, so the
            // V0 index-value layout is the one under test.
            common_handle_version: 0,
        }
    }

    fn codec_index_info(index: &IndexMeta) -> CodecIndexInfo {
        CodecIndexInfo {
            id: index.id,
            columns: index.columns.iter().map(codec_index_column).collect(),
            unique: index.unique,
            global: false,
            global_index_version: 0,
            primary: index.primary,
        }
    }

    /// Go's test helper `buildIndexKeyValue`, which reaches through
    /// `index.FetchValues` / `GenIndexKey` / `GenIndexValue` plus
    /// `TryGetHandleRestoredDataWrapper`. The restored-data wrapper returns
    /// `nil` for every case here because the table's `CommonHandleVersion` is
    /// zero, so it is inlined as the empty slice.
    fn build_index_key_value(
        use_new_collate: bool,
        timezone: &SessionTimeZone,
        meta: &TableMeta,
        index: &IndexMeta,
        row: &[Datum],
        handle: &Handle,
    ) -> (Vec<u8>, Vec<u8>) {
        let codec_table = codec_table_info(meta);
        let codec_index = codec_index_info(index);
        let mut indexed_values: Vec<Datum> = index
            .columns
            .iter()
            .map(|index_column| row[index_column.offset].clone())
            .collect();
        let (key, distinct) = generate_index_key(
            Encoder::new(use_new_collate),
            Some(timezone),
            &codec_table,
            &codec_index,
            meta.id,
            &mut indexed_values,
            Some(handle),
        )
        .unwrap();
        let value = generate_index_value(
            use_new_collate,
            Some(timezone),
            &codec_table,
            &codec_index,
            need_restored_data(use_new_collate, index, &meta.columns),
            distinct,
            false,
            &indexed_values,
            handle,
            0,
            &[],
        )
        .unwrap();
        (key, value)
    }

    /// Go's test helper `requireIndexKVDecodeMatchesRow` plus
    /// `requireDecodedIndexValuesMatchRow`. Go's second decoder,
    /// `DecodeIndexKVEx`, is the caller-buffer form; `decode_index_kv_into` is
    /// its Rust counterpart and is asserted to agree byte for byte.
    #[allow(clippy::too_many_arguments)]
    fn require_index_kv_decode_matches_row(
        use_new_collate: bool,
        timezone: &SessionTimeZone,
        table: &TestTable,
        index: &IndexMeta,
        row: &[Datum],
        key: &[u8],
        value: &[u8],
        row_col_infos: &[RowColInfo],
    ) {
        let decoded = decode_index_kv(
            use_new_collate,
            key,
            value,
            index.columns.len(),
            HandleStatus::NotNeeded,
            row_col_infos,
        )
        .unwrap();

        let mut decoded_ex = Vec::with_capacity(index.columns.len() + row_col_infos.len());
        tidb_tablecodec::decode_index_kv_into(
            use_new_collate,
            key,
            value,
            index.columns.len(),
            HandleStatus::NotNeeded,
            row_col_infos,
            &mut decoded_ex,
        )
        .unwrap();
        assert_eq!(decoded, decoded_ex);

        for encoded in [&decoded, &decoded_ex] {
            assert_eq!(encoded.len(), index.columns.len());
            let index_data: Vec<Datum> = encoded
                .iter()
                .enumerate()
                .map(|(position, bytes)| {
                    let offset = index.columns[position].offset;
                    let field_type = table.meta.columns[offset].field_type.array_type();
                    decode_column_value(bytes, &field_type, Some(timezone)).unwrap()
                })
                .collect();
            compare_index_data(
                use_new_collate,
                table.columns(),
                &index_data,
                row,
                index,
                &table.meta,
                None,
            )
            .unwrap();
        }
    }

    /// Go `TestCheckIndexKeysAndCheckHandleConsistency`.
    ///
    /// The domain: session location x (unique / non-unique index) x (clustered
    /// / int handle) x (bin / non-bin string collation), crossed with the
    /// new-collation switch. Primary clustered indexes and int handles are not
    /// covered because they never produce index mutations. The PK is always
    /// the first (string) column.
    #[test]
    fn check_index_keys_and_check_handle_consistency_match_go() {
        let index_infos = vec![
            IndexMeta {
                id: 1,
                name: "idx_unique".to_owned(),
                unique: true,
                primary: false,
                columns: vec![
                    IndexColumnMeta {
                        name: "c2".to_owned(),
                        offset: 1,
                        length: UNSPECIFIED_LENGTH,
                        use_changing_type: false,
                    },
                    IndexColumnMeta {
                        name: "c1".to_owned(),
                        offset: 0,
                        length: UNSPECIFIED_LENGTH,
                        use_changing_type: false,
                    },
                ],
            },
            IndexMeta {
                id: 2,
                name: "idx_non_unique".to_owned(),
                unique: false,
                primary: false,
                columns: vec![
                    IndexColumnMeta {
                        name: "c2".to_owned(),
                        offset: 1,
                        length: UNSPECIFIED_LENGTH,
                        use_changing_type: false,
                    },
                    IndexColumnMeta {
                        name: "c1".to_owned(),
                        offset: 0,
                        length: UNSPECIFIED_LENGTH,
                        use_changing_type: false,
                    },
                ],
            },
        ];
        let column_info_sets = vec![
            vec![
                column(1, "c1", 0, FieldType::new(FieldTypeCode::String)),
                column(2, "c2", 1, FieldType::new(FieldTypeCode::Datetime)),
            ],
            vec![
                column(
                    1,
                    "c1",
                    0,
                    FieldType::new(FieldTypeCode::String).with_collation(Collation::Utf8UnicodeCi),
                ),
                column(2, "c2", 1, FieldType::new(FieldTypeCode::Datetime)),
            ],
        ];
        let locations = [
            SessionTimeZone::utc(),
            SessionTimeZone::Named(chrono_tz::Asia::Shanghai),
        ];

        // Go uses `types.CurrentTime` and `now.Add(24h)`. A wall clock makes
        // nothing here more true, so two fixed datetimes one day apart stand
        // in; the checks only ever compare them against themselves.
        let now = Time::new(
            CoreTime::from_date(2026, 8, 16, 10, 20, 30, 0),
            TimeType::DateTime,
            0,
        )
        .unwrap();
        let another_time = Time::new(
            CoreTime::from_date(2026, 8, 17, 10, 20, 30, 0),
            TimeType::DateTime,
            0,
        )
        .unwrap();
        let row_to_insert = vec![Datum::new_string("some string"), Datum::new_time(now)];
        let row_to_remove = vec![
            Datum::new_string("old string"),
            Datum::new_time(another_time),
        ];

        for use_new_collate in [true, false] {
            for is_common_handle in [true, false] {
                for timezone in &locations {
                    for column_infos in &column_info_sets {
                        let meta = TableMeta {
                            id: 1,
                            name: "t".to_owned(),
                            columns: column_infos.clone(),
                            indices: index_infos.clone(),
                            pk_is_handle: false,
                            is_common_handle,
                            partitioned: false,
                        };
                        let table = TestTable {
                            meta,
                            use_new_collate,
                        };

                        let (handle, corrupted_handle) = if is_common_handle {
                            let encoded = Encoder::new(use_new_collate)
                                .encode_key_in_timezone(timezone, &row_to_insert[..1])
                                .unwrap();
                            let mut corrupted = encoded.clone();
                            let last = corrupted.len() - 1;
                            corrupted[last] ^= 1;
                            (
                                Handle::Common(CommonHandle::new(encoded).unwrap()),
                                Handle::Common(CommonHandle::new(corrupted).unwrap()),
                            )
                        } else {
                            (
                                Handle::Int(IntHandle::new(1)),
                                Handle::Int(IntHandle::new(2)),
                            )
                        };

                        let mut cache = ColumnMapsCache::new();
                        let maps = get_or_build_column_maps(&mut cache, &table);

                        for index_info in &index_infos {
                            // checkIndexKeys.
                            let (insertion_key, insertion_value) = build_index_key_value(
                                use_new_collate,
                                timezone,
                                &table.meta,
                                index_info,
                                &row_to_insert,
                                &handle,
                            );
                            require_index_kv_decode_matches_row(
                                use_new_collate,
                                timezone,
                                &table,
                                index_info,
                                &row_to_insert,
                                &insertion_key,
                                &insertion_value,
                                &maps.index_id_to_row_col_infos[&index_info.id],
                            );
                            let (deletion_key, _) = build_index_key_value(
                                use_new_collate,
                                timezone,
                                &table.meta,
                                index_info,
                                &row_to_remove,
                                &handle,
                            );
                            let index_mutations = vec![
                                Mutation {
                                    key: insertion_key,
                                    value: insertion_value,
                                    index_id: index_info.id,
                                    ..Mutation::default()
                                },
                                Mutation {
                                    key: deletion_key,
                                    index_id: index_info.id,
                                    ..Mutation::default()
                                },
                            ];
                            check_index_keys(
                                Some(timezone),
                                &table,
                                Some(&row_to_insert),
                                Some(&row_to_remove),
                                &index_mutations,
                                &maps.index_id_to_info,
                                &maps.index_id_to_row_col_infos,
                                None,
                            )
                            .unwrap();

                            // checkHandleConsistency.
                            let row_key = encode_row_key(table.meta.id, &handle.encoded());
                            let corrupted_row_key =
                                encode_row_key(table.meta.id, &corrupted_handle.encoded());
                            let row_value = encode_table_row(
                                Some(timezone),
                                &row_to_insert,
                                &[1, 2],
                                true,
                                None,
                            )
                            .unwrap();
                            let row_mutation = Mutation {
                                key: row_key,
                                value: row_value.clone(),
                                ..Mutation::default()
                            };
                            let corrupted_row_mutation = Mutation {
                                key: corrupted_row_key,
                                value: row_value,
                                ..Mutation::default()
                            };
                            check_handle_consistency(
                                &row_mutation,
                                &index_mutations,
                                &maps.index_id_to_info,
                                &table.meta,
                            )
                            .unwrap();
                            assert!(check_handle_consistency(
                                &corrupted_row_mutation,
                                &index_mutations,
                                &maps.index_id_to_info,
                                &table.meta,
                            )
                            .is_err());
                        }
                    }
                }
            }
        }
    }
}
