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

//! `ADMIN CHECK TABLE` / `ADMIN CHECK INDEX`: proving that a table's stored
//! rows and its stored index entries describe the same relation.
//!
//! Mirrors Go `pkg/executor/check_table_index.go` (`CheckTableExec`) and the
//! two checks it delegates to in `pkg/util/admin/admin.go`
//! (`CheckIndicesCount`, `CheckRecordAndIndex`), plus the index-side value
//! comparison `pkg/executor/distsql.go`'s `tableWorker.compareData` performs
//! when an `IndexLookUp` is built for an admin check.
//!
//! # Why this exists as a real check and not as an OK
//!
//! `ADMIN CHECK TABLE` is the one statement in the ADMIN family whose whole
//! contract is a *negative*: it produces no output at all and its value is
//! entirely in the errors it does not raise. Answering it with `Ok` without
//! reading a byte would be a success return that is not one -- the exact
//! shape of bug this engine converts into refusals elsewhere. So this module
//! runs TiDB's count-selected direction over the real stored bytes, and
//! [`crate::kv_table::KvTable`] is the only backing it accepts: a table with
//! no stored index entries at all (a `MemTable`) is REFUSED by name rather
//! than passed (see [`AdminCheckError::NotStored`]).
//!
//! # The two directions
//!
//! - ROW -> INDEX (Go `CheckRecordAndIndex`): every row must have the entry
//!   its own values encode to, and a unique index's entry must name it.
//! - INDEX -> ROW (Go `compareData`): every entry's handle must name a row,
//!   and each decoded index datum must compare equal to that row's datum.
//!
//! Go's count check chooses which direction runs: table-heavy mismatches use
//! ROW -> INDEX; index-heavy mismatches and equal counts use INDEX -> ROW.
//! `ADMIN CHECK INDEX` reports a count mismatch as 8003 before either scan.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use tidb_codec::table_key::encode_index_seek_key;
use tidb_datatype::Datum;

use crate::kv_table::{KvTable, RowDecodeContext, TableHandle};

/// Why an `ADMIN CHECK` could not answer, or answered that the table is
/// inconsistent.
#[derive(Debug, Clone)]
pub enum AdminCheckError {
    /// Go `admin.ErrAdminCheckTable` (8003): the number of rows is not the
    /// number of entries in one index. Go's message is
    /// `table count %d != index(%s) count %d`.
    CountMismatch {
        /// Rows in the table's record range.
        table_count: i64,
        /// The index whose entry count differs.
        index: String,
        /// Entries in that index's key range.
        index_count: i64,
    },
    /// Go `consistency.ErrAdminCheckInconsistent` (`ErrDataInconsistent`,
    /// 8223): a row and an index entry do not agree. Go's message is
    /// `data inconsistency in table: %s, index: %s, handle: %s,
    /// index-values:%#v != record-values:%#v`.
    Inconsistent {
        /// The table being checked.
        table: String,
        /// The index the disagreement is in.
        index: String,
        /// The row handle, as Go prints it.
        handle: String,
        /// The index side of the disagreement, or `""` when the entry is
        /// missing entirely -- Go prints an absent side as the empty string.
        index_values: String,
        /// The record side, or `""` when the row is missing.
        record_values: String,
    },
    /// Go `ErrDataInconsistentMismatchIndex` (8134): an index entry names a
    /// live row, but one indexed column differs from that row.
    ValueMismatch(Box<AdminValueMismatch>),
    /// The named table is not stored as index-bearing bytes, so there is
    /// nothing to check it against. Refused rather than answered OK.
    NotStored(String),
    /// `ADMIN CHECK INDEX` named an index the table does not have. Go's
    /// planner returns a plain `errors.Errorf`, which reaches the client as
    /// the generic 1105 error.
    UnknownIndex {
        /// The index name as written.
        index: String,
        /// The table it was looked for in.
        table: String,
    },
    /// A stored key or value could not be decoded at all, which is a
    /// corruption this check reports rather than interprets.
    Decode(String),
}

/// The detail carried by [`AdminCheckError::ValueMismatch`].
#[derive(Debug, Clone)]
pub struct AdminValueMismatch {
    /// The table being checked.
    pub table: String,
    /// The index the disagreement is in.
    pub index: String,
    /// The first mismatching indexed column.
    pub column: String,
    /// The row handle, as Go prints it before the error template quotes it.
    pub handle: String,
    /// Go's debug rendering of the stored index datum.
    pub index_value: String,
    /// Go's debug rendering of the row datum.
    pub record_value: String,
    /// Comparison failure, or `<nil>` for an ordinary unequal comparison.
    pub compare_error: String,
}

/// The value Go prints for one side of an inconsistency: the datums in
/// `%#v` form, which for a slice of datums is its Go debug rendering. This
/// engine prints the values it holds; the *presence* of the two sides is what
/// the error is about, and both are named.
fn render_values(values: &[Datum]) -> String {
    let mut out = String::from("[");
    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&format!("{value:?}"));
    }
    out.push(']');
    out
}

fn render_go_datum(value: &Datum) -> String {
    fn quoted_content(bytes: &[u8]) -> String {
        let quoted = format!("{:?}", String::from_utf8_lossy(bytes));
        quoted[1..quoted.len() - 1].to_owned()
    }

    match value {
        Datum::Null => "KindNull <nil>".to_owned(),
        Datum::Int(value) => format!("KindInt64 {value}"),
        Datum::UInt(value) => format!("KindUint64 {value}"),
        Datum::Float32(value) => format!("KindFloat32 {value}"),
        Datum::Real(value) => format!("KindFloat64 {value}"),
        Datum::String(value) => format!("KindString {}", quoted_content(value.bytes())),
        Datum::Bytes(value) => format!("KindBytes {}", quoted_content(value)),
        Datum::BinaryLiteral(value) => format!("KindBinaryLiteral {value}"),
        Datum::Decimal(value) => format!("KindMysqlDecimal {value}"),
        Datum::Duration(value) => format!("KindMysqlDuration {value}"),
        Datum::Enum(value, _) => format!("KindMysqlEnum {value}"),
        Datum::Bit(value) => format!("KindMysqlBit {value}"),
        Datum::Set(value, _) => format!("KindMysqlSet {value}"),
        Datum::Time(value) => format!("KindMysqlTime {value}"),
        Datum::Json(value) => format!("KindMysqlJSON {value}"),
        Datum::Raw(_) => "KindRaw <nil>".to_owned(),
        Datum::VectorFloat32(value) => format!("KindVectorFloat32 {value}"),
        Datum::MinNotNull => "KindMinNotNull <nil>".to_owned(),
        Datum::MaxValue => "KindMaxValue <nil>".to_owned(),
    }
}

/// Go `consistency.RecordData.String`: both the handle and the indexed
/// values belong to one side of an inconsistency report.
fn render_record(handle: &TableHandle, values: &[Datum]) -> String {
    format!(
        "handle: {}, values: {}",
        render_handle(handle),
        render_values(values)
    )
}

/// How Go prints a row handle in the inconsistency message: an integer handle
/// is its number, a clustered handle its encoded bytes.
fn render_handle(handle: &TableHandle) -> String {
    match handle {
        TableHandle::Int(value) => value.to_string(),
        TableHandle::Common(bytes) => format!("{bytes:?}"),
    }
}

/// One index's stored entries: entry key -> the handle the entry names.
///
/// Read straight out of the index's key range rather than through a range
/// cursor, because the check is over the WHOLE index and a range cursor needs
/// datum bounds it would have to invent.
fn index_entries(
    table: &mut KvTable,
    index_id: i64,
) -> Result<Vec<crate::kv_table::IndexEntryForCheck>, AdminCheckError> {
    table
        .index_entry_records_for_check(index_id)
        .map_err(|error| AdminCheckError::Decode(format!("{error:?}")))
}

/// Runs Go's `CheckTableExec` over one stored table.
///
/// `only_index` is `Some` for `ADMIN CHECK INDEX`, which checks exactly one
/// index and -- as Go's `e.checkIndex` branch does -- reports a row/entry
/// count difference as 8003 instead of drilling into which side is wrong.
///
/// Returns the number of indexes actually checked, which is what a caller
/// asserting the statement did work reads.
pub fn check_table(
    table: &mut KvTable,
    only_index: Option<&str>,
    context: &RowDecodeContext,
) -> Result<usize, AdminCheckError> {
    let indexes = table.index_list_for_check();
    let selected: Vec<_> = match only_index {
        Some(name) => {
            let found = indexes
                .iter()
                .find(|index| index.name.eq_ignore_ascii_case(name))
                .cloned()
                .ok_or_else(|| AdminCheckError::UnknownIndex {
                    index: name.to_owned(),
                    table: table.name.clone(),
                })?;
            vec![found]
        }
        None => indexes,
    };

    let rows = table
        .scan_rows_with_handles_recomputed(context)
        .map_err(|error| AdminCheckError::Decode(format!("{error:?}")))?;
    let table_count = rows.len() as i64;

    // Go `admin.CheckIndicesCount` runs first and, for `ADMIN CHECK INDEX`,
    // is the error the client sees.
    for index in &selected {
        let entries = index_entries(table, index.id)?;
        let index_count = entries.len() as i64;
        if index_count != table_count && only_index.is_some() {
            return Err(AdminCheckError::CountMismatch {
                table_count,
                index: index.name.clone(),
                index_count,
            });
        }
    }

    let table_name = table.name.clone();
    let mut checked = 0usize;
    for index in &selected {
        // Go chooses the direction from the count comparison. Equal counts
        // use IndexLookUp (INDEX -> ROW); only a table-heavy mismatch uses
        // `CheckRecordAndIndex` (ROW -> INDEX).
        let stored = index_entries(table, index.id)?;
        if table_count > stored.len() as i64 {
            let mut expected: BTreeMap<Vec<u8>, (TableHandle, &Vec<Datum>)> = BTreeMap::new();
            for (handle, row) in &rows {
                let (key, _) = table
                    .index_key_for_check(index, row, handle, context.zone())
                    .map_err(|error| AdminCheckError::Decode(format!("{error:?}")))?;
                expected.insert(key, (handle.clone(), row));
            }
            let stored_keys: BTreeMap<Vec<u8>, TableHandle> = stored
                .iter()
                .map(|entry| (entry.key.clone(), entry.handle.clone()))
                .collect();
            for (key, (handle, row)) in &expected {
                let indexed = table.index_values_for_check(index, row);
                match stored_keys.get(key) {
                    Some(stored_handle) if stored_handle == handle => {}
                    Some(stored_handle) => {
                        return Err(AdminCheckError::Inconsistent {
                            table: table_name.clone(),
                            index: index.name.clone(),
                            handle: render_handle(handle),
                            index_values: render_record(stored_handle, &indexed),
                            record_values: render_record(handle, &indexed),
                        });
                    }
                    None => {
                        return Err(AdminCheckError::Inconsistent {
                            table: table_name.clone(),
                            index: index.name.clone(),
                            handle: render_handle(handle),
                            index_values: String::new(),
                            record_values: render_record(handle, &indexed),
                        });
                    }
                }
            }
        } else {
            let rows_by_handle: BTreeMap<_, _> = rows
                .iter()
                .map(|(handle, row)| (handle.clone(), row))
                .collect();
            for entry in &stored {
                let key = &entry.key;
                let value = &entry.value;
                let handle = &entry.handle;
                let Some(row) = rows_by_handle.get(handle) else {
                    let indexed = table
                        .index_entry_values_for_check(index, key, value, context.zone())
                        .map_err(|error| AdminCheckError::Decode(format!("{error:?}")))?;
                    return Err(AdminCheckError::Inconsistent {
                        table: table_name.clone(),
                        index: index.name.clone(),
                        handle: render_handle(handle),
                        index_values: render_record(handle, &indexed),
                        record_values: String::new(),
                    });
                };
                let indexed = table
                    .index_entry_values_for_check(index, key, value, context.zone())
                    .map_err(|error| AdminCheckError::Decode(format!("{error:?}")))?;
                let recorded = table.index_values_for_check(index, row);
                for (position, (index_value, record_value)) in
                    indexed.iter().zip(&recorded).enumerate()
                {
                    let column_offset = index.column_offsets[position];
                    let column = &table.columns[column_offset];
                    match index_value.compare(record_value, column.field_type.collation()) {
                        Ok(Ordering::Equal) => {}
                        comparison => {
                            return Err(AdminCheckError::ValueMismatch(Box::new(
                                AdminValueMismatch {
                                    table: table_name.clone(),
                                    index: index.name.clone(),
                                    column: column.name.clone(),
                                    handle: render_handle(handle),
                                    index_value: render_go_datum(index_value),
                                    record_value: render_go_datum(record_value),
                                    compare_error: comparison.err().map_or_else(
                                        || "<nil>".to_owned(),
                                        |error| error.to_string(),
                                    ),
                                },
                            )));
                        }
                    }
                }
            }
        }
        checked += 1;
    }
    Ok(checked)
}

/// `ADMIN CHECK INDEX t idx (begin, end), ...`: the entries of one index whose
/// row handle falls in one of the half-open intervals, in INDEX order.
///
/// Go builds a `CheckIndexRangeExec` (`pkg/executor/admin.go`), an index scan
/// whose output schema is the indexed columns followed by `extra_handle`.
/// Captured shape, from `tests/integrationtest/r/executor/admin.result`:
///
/// ```text
/// admin check index check_index_test a_b (2, 4);
/// a       b       extra_handle
/// 1       ef      3
/// 2       cd      2
/// ```
///
/// with rows `(3,"ab") (2,"cd") (1,"ef") (-1,"hi")` at handles 1..4 -- the
/// interval bounds the HANDLE, and the rows come back ordered by the index.
///
/// An `ADMIN CHECK INDEX` with no interval is not this statement at all: it is
/// the consistency check, and goes through [`check_table`].
pub fn check_index_ranges(
    table: &mut KvTable,
    index_name: &str,
    ranges: &[tidb_ast::AdminCheckHandleRange],
    context: &RowDecodeContext,
) -> Result<(Vec<String>, Vec<Vec<Datum>>), AdminCheckError> {
    let index = table
        .index_list_for_check()
        .into_iter()
        .find(|index| index.name.eq_ignore_ascii_case(index_name))
        .ok_or_else(|| AdminCheckError::UnknownIndex {
            index: index_name.to_owned(),
            table: table.name.clone(),
        })?;

    let mut columns: Vec<String> = index
        .column_offsets
        .iter()
        .map(|offset| table.columns[*offset].name.clone())
        .collect();
    columns.push("extra_handle".to_owned());

    let rows = table
        .scan_rows_with_handles_recomputed(context)
        .map_err(|error| AdminCheckError::Decode(format!("{error:?}")))?;
    let by_handle: BTreeMap<Vec<u8>, (TableHandle, Vec<Datum>)> = rows
        .iter()
        .map(|(handle, row)| {
            let (key, _) = table
                .index_key_for_check(&index, row, handle, context.zone())
                .map_err(|error| AdminCheckError::Decode(format!("{error:?}")))?;
            Ok((key, (handle.clone(), row.clone())))
        })
        .collect::<Result<_, AdminCheckError>>()?;

    // Iterating the map is iterating the encoded index keys in ascending
    // order, which IS index order -- the same order the stored entries have,
    // because they are those keys.
    let mut out = Vec::new();
    for (handle, row) in by_handle.values() {
        let TableHandle::Int(value) = handle else {
            // A clustered table has no `extra_handle` column, and Go's
            // `CheckIndexRangeExec` takes its bounds as integer handles.
            return Err(AdminCheckError::NotStored(format!(
                "{}: ADMIN CHECK INDEX with handle ranges needs an integer row handle",
                table.name
            )));
        };
        if !ranges
            .iter()
            .any(|range| *value >= range.begin && *value < range.end)
        {
            continue;
        }
        let mut emitted: Vec<Datum> = index
            .column_offsets
            .iter()
            .map(|offset| row.get(*offset).cloned().unwrap_or(Datum::Null))
            .collect();
        emitted.push(Datum::Int(*value));
        out.push(emitted);
    }
    Ok((columns, out))
}

/// The key range one index occupies: `t{tid}_i{iid}` and its `PrefixNext`.
///
/// Kept here rather than in `kv_table` because the whole-index sweep is this
/// check's need; every other reader of an index has datum bounds.
#[must_use]
pub fn index_key_bounds(table_id: i64, index_id: i64) -> (Vec<u8>, Vec<u8>) {
    let low = encode_index_seek_key(table_id, index_id, &[]);
    let high = tidb_txnkv::Key::from_bytes(low.clone())
        .prefix_next()
        .as_bytes()
        .to_vec();
    (low, high)
}

#[cfg(test)]
mod source_tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};

    #[test]
    fn test_compare_index_data() {
        // Direct port of
        // pkg/table/tables/mutation_checker_test.go::TestCompareIndexData.
        // Go compares decoded index values with the row after applying each
        // IndexColumn prefix length to the row side. ADMIN CHECK uses the
        // same cut through KvTable::index_key, so exercise that shared rule.
        let field_types = [
            FieldType::new(FieldTypeCode::Short),
            FieldType::new(FieldTypeCode::String),
        ];
        let cases = [
            (
                vec![Datum::Int(1), Datum::new_string("some string")],
                vec![Datum::Int(1), Datum::new_string("some string")],
                [
                    crate::index_prefix_cut::UNSPECIFIED_LENGTH,
                    crate::index_prefix_cut::UNSPECIFIED_LENGTH,
                ],
                true,
            ),
            (
                vec![Datum::Int(1), Datum::new_string("some string")],
                vec![Datum::Int(1), Datum::new_string("some string2")],
                [
                    crate::index_prefix_cut::UNSPECIFIED_LENGTH,
                    crate::index_prefix_cut::UNSPECIFIED_LENGTH,
                ],
                false,
            ),
            (
                vec![Datum::Int(1), Datum::new_string("some string")],
                vec![Datum::Int(1), Datum::new_string("some string2")],
                [crate::index_prefix_cut::UNSPECIFIED_LENGTH, 11],
                true,
            ),
        ];

        for (index_data, mut row_data, lengths, expected) in cases {
            for ((value, length), field_type) in row_data.iter_mut().zip(lengths).zip(&field_types)
            {
                crate::index_prefix_cut::cut_index_value(value, length, field_type);
            }
            assert_eq!(index_data == row_data, expected);
        }
    }
}
