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
//! # Two handle shapes, because `mysql.*` has two
//!
//! The account tables TiDB declares `NONCLUSTERED`, so their rows live under
//! an allocated `_tidb_rowid` and every declared column is in the row value:
//! [`insert_row`], [`update_row`], [`delete_row`].
//!
//! The statistics tables are `CLUSTERED` — `mysql.stats_meta` on `table_id`,
//! `mysql.stats_histograms` and `mysql.stats_buckets` on their whole
//! `(table_id, is_index, hist_id[, bucket_id])` tuple. Their handle columns
//! live in the record *key* and are absent from the row value, which is the
//! same rule the reader states in `SystemTableView::project`:
//! [`clustered_record_key`], [`store_clustered_row`],
//! [`delete_clustered_row`]. `mysql.stats_top_n` is the odd one out, with no
//! clustered handle at all, and takes the first set.

use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
use tidb_codec::Encoder;
use tidb_datatype::{parse_enum, parse_set_name, Collation, Datum, FieldType, FieldTypeCode, Time};
use tidb_model::column::{ColumnDefaultValue, ColumnInfo};
use tidb_model::table_info::TableInfo;
use tidb_tablecodec::{
    encode_table_row, generate_index_key, generate_index_value, IndexColumn as CodecIndexColumn,
    IndexInfo as CodecIndexInfo, TableColumn as CodecTableColumn, TableInfo as CodecTableInfo,
};
use tidb_txnkv::transaction::OptimisticMutation;
use tidb_txnkv::{CommonHandle, Handle, IntHandle};

use crate::mysql_system_tables::HandleLayout;

/// Go `ast.CurrentTimestamp`, as a column default stores it.
pub(crate) const CURRENT_TIMESTAMP: &str = "CURRENT_TIMESTAMP";

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
    for column in table.cols() {
        values.insert(column.id, declared_default(column, table_name(table), now)?);
    }
    Ok(values)
}

/// The mutations that write one row that does not exist yet, at `row_id`.
pub fn insert_row(
    table: &TableInfo,
    row_id: i64,
    values: &RowValues,
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    let key = encode_row_key_with_handle(table.id, &RecordHandle::Int(row_id));
    let mut mutations = vec![OptimisticMutation::insert(key, encode_row(table, values)?)
        .map_err(|error| encode_error(error.to_string()))?];
    mutations.extend(index_entries(
        table,
        &Handle::Int(IntHandle::new(row_id)),
        values,
        IndexOp::Put,
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
    let row_id = row_id_of(key)?;
    let mut mutations = vec![OptimisticMutation::delete(key.to_vec())
        .map_err(|error| encode_error(error.to_string()))?];
    mutations.extend(index_entries(
        table,
        &Handle::Int(IntHandle::new(row_id)),
        values,
        IndexOp::Delete,
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
    let layout = HandleLayout::of(table);
    let named = |name: &str| -> Result<&Datum, RowEncodeError> {
        let column = table
            .cols()
            .into_iter()
            .find(|column| column.name.lowercase() == name)
            .ok_or_else(|| encode_error(format!("{} has no column `{name}`", table_name(table))))?;
        values.get(&column.id).ok_or_else(|| {
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
            let encoded =
                tidb_codec::encode_key(&datums).map_err(|error| encode_error(error.to_string()))?;
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
    let (key, handle) = clustered_record_key(table, values)?;
    let encoded = encode_row(table, values)?;
    let mut mutations = Vec::new();
    match existing {
        Some(existing) => {
            mutations.push(
                OptimisticMutation::put_existing(key, encoded)
                    .map_err(|error| encode_error(error.to_string()))?,
            );
            // Retracting first and writing second matters only when the value
            // moved; when it did not, the two entries share a key and the
            // retraction would delete what the write just stored, so the
            // unchanged case skips both.
            let stale = index_entries(table, &handle, existing, IndexOp::Delete)?;
            let fresh = index_entries(table, &handle, values, IndexOp::Put)?;
            for (retract, store) in stale.into_iter().zip(fresh) {
                if retract.key() == store.key() {
                    continue;
                }
                mutations.push(retract);
                mutations.push(store);
            }
        }
        None => {
            mutations.push(
                OptimisticMutation::insert(key, encoded)
                    .map_err(|error| encode_error(error.to_string()))?,
            );
            mutations.extend(index_entries(table, &handle, values, IndexOp::Put)?);
        }
    }
    Ok(mutations)
}

/// The mutations that remove one row of a clustered table and every index
/// entry it owns.
pub fn delete_clustered_row(
    table: &TableInfo,
    values: &RowValues,
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    let (key, handle) = clustered_record_key(table, values)?;
    let mut mutations =
        vec![OptimisticMutation::delete(key).map_err(|error| encode_error(error.to_string()))?];
    mutations.extend(index_entries(table, &handle, values, IndexOp::Delete)?);
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
    for index in &table.indices {
        for column in &index.columns {
            names.push(column.name.lowercase().to_owned());
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
    for column in table.cols() {
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
    encode_table_row(None, &row, &column_ids, true, None)
        .map_err(|error| encode_error(error.to_string()))
}

fn index_entries(
    table: &TableInfo,
    handle: &Handle,
    values: &RowValues,
    op: IndexOp,
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    let handle = handle.clone();
    let codec_table = codec_table_info(table);
    let mut mutations = Vec::new();
    for (position, index) in table.indices.iter().enumerate() {
        let codec_index = &codec_table.indices[position];
        let mut indexed = Vec::with_capacity(index.columns.len());
        for index_column in &index.columns {
            let offset =
                usize::try_from(index_column.offset).expect("a column offset is not negative");
            let column = table.columns.get(offset).ok_or_else(|| {
                encode_error(format!(
                    "{}'s index `{}` names an offset the table does not have",
                    table_name(table),
                    index.name.original()
                ))
            })?;
            let value = values.get(&column.id).ok_or_else(|| {
                encode_error(format!(
                    "{}.{} has no value for index `{}`",
                    table_name(table),
                    column.name.original(),
                    index.name.original()
                ))
            })?;
            indexed.push(typed_value(value, &column.field_type)?);
        }
        let (index_key, distinct) = generate_index_key(
            Encoder::new(true),
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
                    true,
                    None,
                    &codec_table,
                    codec_index,
                    false,
                    distinct,
                    false,
                    &indexed,
                    &handle,
                    0,
                    &[],
                )
                .map_err(|error| encode_error(error.to_string()))?;
                OptimisticMutation::index_put(index_key, index_value)
            }
        };
        mutations.push(mutation.map_err(|error| encode_error(error.to_string()))?);
    }
    Ok(mutations)
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
    let refuse = || {
        encode_error(format!(
            "{table}.{} declares a default this path cannot materialise",
            column.name.original()
        ))
    };
    if column.default_is_expr {
        return Err(refuse());
    }
    // A `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` column stores that very word as
    // its default: an `INSERT` evaluates it, so a writer that stores the word
    // instead writes a row TiDB rejects as an `Incorrect time value`.
    if let Some(ColumnDefaultValue::Str(bytes)) = column.default_value.as_ref() {
        if String::from_utf8_lossy(bytes).eq_ignore_ascii_case(CURRENT_TIMESTAMP) {
            return Ok(Datum::new_time(current_timestamp));
        }
    }
    let Some(default) = column.default_value.as_ref() else {
        // No declared default: an `INSERT` stores NULL, and the column is
        // nullable or the schema would not have parsed.
        return Ok(Datum::Null);
    };
    let datum = match default {
        ColumnDefaultValue::Int(value) => Datum::Int(*value),
        ColumnDefaultValue::Uint(value) => Datum::UInt(*value),
        ColumnDefaultValue::Bool(value) => Datum::Int(i64::from(*value)),
        ColumnDefaultValue::Float(value) => Datum::Real(*value),
        ColumnDefaultValue::Str(bytes) => {
            let text = Datum::Bytes(bytes.clone());
            // A numeric column's default is stored as its printed form, so it
            // has to be read back as a number before it is encoded as one.
            match column.get_type() {
                FieldTypeCode::Tiny
                | FieldTypeCode::Short
                | FieldTypeCode::Int24
                | FieldTypeCode::Long
                | FieldTypeCode::LongLong
                | FieldTypeCode::Year => {
                    let printed = String::from_utf8_lossy(bytes);
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
    let name = String::from_utf8_lossy(bytes).into_owned();
    let elements = field_type.elems();
    if elements.is_empty() {
        return Err(RowEncodeError(format!(
            "a {code:?} column that declares no elements cannot store `{name}`"
        )));
    }
    // The stored spelling is the declared one, so name matching runs under the
    // column's own collation -- which is how Go's `ParseEnum`/`ParseSet` do it.
    let collation = Collation::from_name(field_type.collation_name()).unwrap_or(Collation::Binary);
    if code == FieldTypeCode::Enum {
        let member = parse_enum(elements, &name, collation)
            .map_err(|error| RowEncodeError(error.to_string()))?;
        Ok(Datum::new_enum(member, collation))
    } else {
        // Go stores a SET as the declaration-ordered bit mask plus the joined
        // names; `parse_set_name` computes both, and answers the empty set for
        // the empty string rather than treating it as an unknown element.
        let members = parse_set_name(elements, &name, collation)
            .map_err(|error| RowEncodeError(error.to_string()))?;
        Ok(Datum::new_set(members, collation))
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
    match typed_value(&Datum::Bytes(text.as_bytes().to_vec()), field_type)? {
        Datum::Set(members, _) => Ok(members.name().to_owned()),
        Datum::Enum(member, _) => Ok(member.name().to_owned()),
        _ => Ok(text.to_owned()),
    }
}

/// The tablecodec view of one stored `TableInfo`.
///
/// `tidb-tablecodec` keeps its own minimal metadata shape so it does not depend
/// on the full catalog model; the index encoders need this projection of it.
pub fn codec_table_info(table: &TableInfo) -> CodecTableInfo {
    CodecTableInfo {
        columns: table
            .columns
            .iter()
            .enumerate()
            .map(|(offset, column)| CodecTableColumn {
                id: column.id,
                offset,
                field_type: column.field_type.clone(),
                primary_key: column
                    .field_type
                    .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY),
                changing_field_type: None,
            })
            .collect(),
        indices: table
            .indices
            .iter()
            .map(|index| CodecIndexInfo {
                id: index.id,
                columns: index
                    .columns
                    .iter()
                    .map(|column| CodecIndexColumn {
                        offset: usize::try_from(column.offset)
                            .expect("a column offset is not negative"),
                        length: i64::from(column.length),
                        use_changing_type: false,
                    })
                    .collect(),
                unique: index.unique,
                global: index.global,
                global_index_version: 0,
                primary: index.primary,
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
        field_type.set_elems(elements.iter().map(|e| (*e).to_owned()).collect());
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
                assert_eq!((yes.name(), yes.value()), ("Y", 2));
                assert_eq!((no.name(), no.value()), ("N", 1));
            }
            other => panic!("the enums did not survive typing: {other:?}"),
        }
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
            Datum::Set(members, _) => assert_eq!((members.name(), members.value()), ("", 0)),
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
