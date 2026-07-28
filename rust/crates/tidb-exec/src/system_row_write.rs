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
//! Every `mysql.*` table this node writes is *non-clustered* — `pk_is_handle`
//! and `is_common_handle` are both false, because TiDB declares their primary
//! keys `NONCLUSTERED` — so a row is three things at once and a writer that
//! produces fewer leaves the row half-visible:
//!
//! 1. the record, under an implicit `_tidb_rowid` handle;
//! 2. one entry per public index, since nothing backfills them;
//! 3. for an insert, the row-ID allocator key, so the next writer does not
//!    hand out a handle this one already used.
//!
//! [`crate::mysql_bootstrap`] proved this encoding against a real TiDB (a Go
//! server reads the rows it seeds); this module is that same encoding lifted
//! out of the bootstrap's one-shot shape so the account path can also update
//! and delete rows that already exist.

use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
use tidb_codec::Encoder;
use tidb_datatype::{Collation, Datum, FieldTypeCode, MysqlEnum, Time};
use tidb_model::column::{ColumnDefaultValue, ColumnInfo};
use tidb_model::table_info::TableInfo;
use tidb_tablecodec::{
    encode_table_row, generate_index_key, generate_index_value, IndexColumn as CodecIndexColumn,
    IndexInfo as CodecIndexInfo, TableColumn as CodecTableColumn, TableInfo as CodecTableInfo,
};
use tidb_txnkv::transaction::OptimisticMutation;
use tidb_txnkv::{Handle, IntHandle};

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
    mutations.extend(index_entries(table, row_id, values, IndexOp::Put)?);
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
    mutations.extend(index_entries(table, row_id, values, IndexOp::Delete)?);
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
    let mut column_ids = Vec::with_capacity(table.columns.len());
    let mut row = Vec::with_capacity(table.columns.len());
    for column in table.cols() {
        let value = values.get(&column.id).ok_or_else(|| {
            encode_error(format!(
                "{}.{} has no value in the row being written",
                table_name(table),
                column.name.original()
            ))
        })?;
        column_ids.push(column.id);
        row.push(typed_value(value, column.get_type()));
    }
    encode_table_row(None, &row, &column_ids, true, None)
        .map_err(|error| encode_error(error.to_string()))
}

fn index_entries(
    table: &TableInfo,
    row_id: i64,
    values: &RowValues,
    op: IndexOp,
) -> Result<Vec<OptimisticMutation>, RowEncodeError> {
    let handle = Handle::Int(IntHandle::new(row_id));
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
            indexed.push(typed_value(value, column.get_type()));
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
                code => typed_value(&text, code),
            }
        }
    };
    Ok(datum)
}

/// Re-types a value for the column it lands in.
///
/// Every value a caller states is spelled as text, because that is how Go's
/// own `INSERT` spells it; a column whose declared type is `ENUM` needs it as
/// the member it names, not as a string, or the row decodes as the wrong type.
pub fn typed_value(value: &Datum, code: FieldTypeCode) -> Datum {
    match (value, code) {
        (Datum::Bytes(bytes), FieldTypeCode::Enum) => {
            // Go's `mysql.user` privilege enums are `ENUM('N','Y')`, so `N` is
            // member 1 and `Y` is member 2.
            let name = String::from_utf8_lossy(bytes).into_owned();
            let position = if name.eq_ignore_ascii_case(YES) { 2 } else { 1 };
            Datum::new_enum(MysqlEnum::new(name, position), Collation::Binary)
        }
        _ => value.clone(),
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

    #[test]
    fn a_privilege_enum_is_written_as_its_member_not_as_a_string() {
        // A `Y` stored as `Bytes` decodes as the wrong type on a real TiDB,
        // which is what makes this conversion load-bearing rather than tidy.
        let yes = typed_value(&Datum::Bytes(b"Y".to_vec()), FieldTypeCode::Enum);
        let no = typed_value(&Datum::Bytes(b"N".to_vec()), FieldTypeCode::Enum);
        match (yes, no) {
            (Datum::Enum(yes, _), Datum::Enum(no, _)) => {
                assert_eq!((yes.name(), yes.value()), ("Y", 2));
                assert_eq!((no.name(), no.value()), ("N", 1));
            }
            other => panic!("the enums did not survive typing: {other:?}"),
        }
    }
}
