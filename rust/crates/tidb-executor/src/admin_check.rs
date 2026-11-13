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

//! Go `pkg/util/admin/admin.go`'s `CheckRecordAndIndex` (every row a
//! table holds files the entry of an index that its values encode to, a
//! unique entry naming that row) and the inconsistency errors the admin
//! checks raise (Go `pkg/util/logutil/consistency`'s 8223 and 8134 shapes).
//! The non-fast `ADMIN CHECK` is Go's `CheckTableExec`
//! (`check_table_index.rs`) over checking `IndexLookUpExecutor`s
//! (`index_lookup.rs`'s `compareData`), which calls `CheckRecordAndIndex` when
//! the table holds more rows than an index.


use tidb_datatype::Datum;

use tidb_table::tables::TableHandle;
use tidb_table::tables::RowDecodeContext;
use tidb_table::tables::TableCommon;
use tidb_table::tables::Index;

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
    /// Go `errCheckPartialIndexWithoutFastCheck` (8273): the non-fast
    /// `CheckTableExec` cannot validate a partial index.
    PartialIndexWithoutFastCheck,
    /// Go `CheckRecordAndIndex`'s `errors.Errorf("Column %v define as not
    /// null, but can't find the value where handle is %v")`.
    NotNullWithoutValue {
        /// The column's name.
        column: String,
        /// The row handle, as Go prints it.
        handle: String,
    },
}

impl AdminCheckError {
    /// The Go error a consistency report carries:
    /// `consistency.ErrAdminCheckInconsistent` (8223) or
    /// `ErrAdminCheckInconsistentWithColInfo` (8134).
    ///
    /// # Panics
    ///
    /// On any other kind, which is not a consistency report.
    #[must_use]
    pub(crate) fn terror(self) -> tidb_terror::TerrorError {
        match self {
            AdminCheckError::Inconsistent {
                table,
                index,
                handle,
                index_values,
                record_values,
            } => crate::consistency_errors::ERR_ADMIN_CHECK_INCONSISTENT.gen_with_stack(format!(
                "data inconsistency in table: {table}, index: {index}, handle: {handle}, \
             index-values:{index_values:?} != record-values:{record_values:?}"
            )),
            AdminCheckError::ValueMismatch(mismatch) => {
                let AdminValueMismatch {
                    table,
                    index,
                    column,
                    handle,
                    index_value,
                    record_value,
                    compare_error,
                } = *mismatch;
                crate::consistency_errors::ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO.gen_with_stack(format!(
                    "data inconsistency in table: {table}, index: {index}, col: {column}, handle: \
                 {handle:?}, index-values:{index_value:?} != record-values:{record_value:?}, \
                 compare err:{compare_error}"
                ))
            }
            other => panic!("{other:?} is not a consistency report"),
        }
    }
}

/// Go `kv.Handle` as an inconsistency report names it.
pub(crate) fn table_handle_of(handle: &tidb_txnkv::Handle) -> TableHandle {
    match handle {
        tidb_txnkv::Handle::Int(handle) => TableHandle::Int(handle.value()),
        tidb_txnkv::Handle::Common(handle) => TableHandle::Common(handle.encoded().to_vec()),
        tidb_txnkv::Handle::Partition(handle) => table_handle_of(handle.inner()),
    }
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
/// Go `fmt.Sprint([]types.Datum)`: each datum's `String()`, space-separated.
fn render_values(values: &[Datum]) -> String {
    let rendered: Vec<String> = values.iter().map(render_go_datum).collect();
    format!("[{}]", rendered.join(" "))
}

pub(crate) fn render_go_datum(value: &Datum) -> String {
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

/// Go `consistency.Reporter.ReportAdminCheckInconsistent(handle, idxRow,
/// tblRow)`'s error; an absent side prints as `""`.
pub(crate) fn inconsistent(
    table: &str,
    index: &str,
    handle: &TableHandle,
    index_row: Option<(&TableHandle, &[Datum])>,
    table_row: Option<(&TableHandle, &[Datum])>,
) -> AdminCheckError {
    let render = |row: Option<(&TableHandle, &[Datum])>| row.map_or_else(String::new, |(handle, values)| render_record(handle, values));
    AdminCheckError::Inconsistent {
        table: table.to_owned(),
        index: index.to_owned(),
        handle: render_handle(handle),
        index_values: render(index_row),
        record_values: render(table_row),
    }
}

/// How Go prints a row handle in the inconsistency message: an integer handle
/// is its number, a clustered handle its encoded bytes.
pub(crate) fn render_handle(handle: &TableHandle) -> String {
    match handle {
        TableHandle::Int(value) => value.to_string(),
        TableHandle::Common(bytes) => format!("{bytes:?}"),
    }
}

/// Go `CheckTableExec.checkTableRecord`: `admin.CheckRecordAndIndex` over the
/// table, or over each partition of a partitioned one.
pub(crate) fn check_record_and_index(
    table: &mut TableCommon,
    store: &mut dyn tidb_kv::storage::TableStorage,
    index: &Index,
    context: &RowDecodeContext,
) -> Result<(), AdminCheckError> {
    for physical_id in table.record_physical_ids() {
        check_record_and_index_in(table, store, index, physical_id, context)?;
    }
    Ok(())
}

/// Go `admin.CheckRecordAndIndex` over one physical table: `iterRecords`
/// walks the transaction's record keys (its `Retriever.Iter`), decodes each
/// row with the full row decoder, reads a NULL indexed value as the column's
/// origin default (a NOT NULL column without one is an error), and asks
/// `index.Exist` whether the row's entry is there and names the row.
fn check_record_and_index_in(
    table: &mut TableCommon,
    store: &mut dyn tidb_kv::storage::TableStorage,
    index: &Index,
    physical_id: i64,
    context: &RowDecodeContext,
) -> Result<(), AdminCheckError> {
    let decode = |error: tidb_table::tables::KvTableError| AdminCheckError::Decode(format!("{error:?}"));
    let table_name = table.name();
    let index_name = index.read().name.original().to_owned();
    let decoder = crate::KvTableReader::row_decoder_recomputed(table, context).map_err(decode)?;
    let prefix = tidb_tablecodec::table_key::gen_table_record_prefix(physical_id);
    let upper = tidb_txnkv::Key::from_bytes(prefix.clone()).prefix_next();
    let mut iterator = store
        .iter(Some(&tidb_txnkv::Key::from_bytes(prefix)), Some(&upper))
        .map_err(|error| AdminCheckError::Decode(error.to_string()))?;
    let result = (|| {
        while iterator.valid() {
            let (handle, mut row) = decoder
                .decode_record(iterator.key().as_bytes(), iterator.value())
                .map_err(decode)?;
            for &offset in &index.column_offsets {
                if !matches!(row.get(offset), Some(Datum::Null)) {
                    continue;
                }
                let column = &table.columns()[offset];
                if column.read().field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
                    && column.origin_default.is_none()
                {
                    return Err(AdminCheckError::NotNullWithoutValue {
                        column: column.read().name.original().to_owned(),
                        handle: render_handle(&handle),
                    });
                }
                // NULL value is regarded as its default value.
                row[offset] = column
                    .origin_default_value(context.type_flags(), context.zone())
                    .map_err(|error| AdminCheckError::Decode(error.to_string()))?;
            }
            match table
                .index_exist_for_check(store, index, &row, &handle, physical_id, context.zone())
                .map_err(decode)?
            {
                tidb_table::tables::IndexExistence::Exists => {}
                tidb_table::tables::IndexExistence::Missing => {
                    let values = table.index_values_for_check(index, &row);
                    return Err(inconsistent(&table_name, &index_name, &handle, None, Some((&handle, &values))));
                }
                tidb_table::tables::IndexExistence::OtherHandle(other) => {
                    let values = table.index_values_for_check(index, &row);
                    return Err(inconsistent(
                        &table_name,
                        &index_name,
                        &handle,
                        Some((&other, &values)),
                        Some((&handle, &values)),
                    ));
                }
            }
            iterator.next().map_err(|error| AdminCheckError::Decode(error.to_string()))?;
        }
        Ok(())
    })();
    iterator.close();
    result
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
        // same cut through TableCommon::index_key, so exercise that shared rule.
        let field_types = [
            FieldType::new(FieldTypeCode::Short),
            FieldType::new(FieldTypeCode::String),
        ];
        let cases = [
            (
                vec![Datum::Int(1), Datum::new_string_datum("some string")],
                vec![Datum::Int(1), Datum::new_string_datum("some string")],
                [
                    tidb_datatype::UNSPECIFIED_LENGTH,
                    tidb_datatype::UNSPECIFIED_LENGTH,
                ],
                true,
            ),
            (
                vec![Datum::Int(1), Datum::new_string_datum("some string")],
                vec![Datum::Int(1), Datum::new_string_datum("some string2")],
                [
                    tidb_datatype::UNSPECIFIED_LENGTH,
                    tidb_datatype::UNSPECIFIED_LENGTH,
                ],
                false,
            ),
            (
                vec![Datum::Int(1), Datum::new_string_datum("some string")],
                vec![Datum::Int(1), Datum::new_string_datum("some string2")],
                [tidb_datatype::UNSPECIFIED_LENGTH, 11],
                true,
            ),
        ];

        for (index_data, mut row_data, lengths, expected) in cases {
            for ((value, length), field_type) in row_data.iter_mut().zip(lengths).zip(&field_types)
            {
                tidb_tablecodec::cut_datum_by_prefix_len(value, length, field_type);
            }
            assert_eq!(index_data == row_data, expected);
        }
    }
}
