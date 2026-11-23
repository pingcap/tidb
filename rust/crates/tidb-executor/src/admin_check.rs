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
    /// Go `consistency.Reporter`'s error: `ErrAdminCheckInconsistent`
    /// (8223), a row and an index entry that do not agree, or
    /// `ErrAdminCheckInconsistentWithColInfo` (8134), an index entry whose
    /// indexed column differs from its row. The reporter has logged it.
    Report(tidb_terror::TerrorError),
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

/// Go `kv.Handle` as an inconsistency report names it.
pub(crate) fn table_handle_of(handle: &tidb_txnkv::Handle) -> TableHandle {
    match handle {
        tidb_txnkv::Handle::Int(handle) => TableHandle::Int(handle.value()),
        tidb_txnkv::Handle::Common(handle) => TableHandle::Common(handle.encoded().to_vec()),
        tidb_txnkv::Handle::Partition(handle) => table_handle_of(handle.inner()),
    }
}

/// Go `kv.Handle` of a row handle the table layer names; a clustered
/// handle that does not decode is the corruption Go's `NewCommonHandle`
/// refuses.
pub(crate) fn kv_handle_of(handle: &TableHandle) -> Result<tidb_txnkv::Handle, AdminCheckError> {
    Ok(match handle {
        TableHandle::Int(value) => tidb_txnkv::Handle::Int(tidb_txnkv::IntHandle::new(*value)),
        TableHandle::Common(bytes) => tidb_txnkv::Handle::Common(
            tidb_txnkv::CommonHandle::new(bytes.clone()).map_err(|error| AdminCheckError::Decode(error.to_string()))?,
        ),
    })
}

/// The session state Go's `consistency.Reporter` reads: the redaction mode
/// and the store it asks for MVCC records.
#[derive(Clone)]
pub struct ReportContext {
    /// Go `SessionVars.EnableRedactLog`.
    pub(crate) enable_redact_log: String,
    /// Go `sctx.GetStore()` as a `helper.Storage`.
    pub(crate) storage: Option<std::sync::Arc<dyn tidb_kv::region_map::RegionStore>>,
}

impl ReportContext {
    /// The report context of a statement.
    pub fn of(ctx: &crate::StmtContext) -> Self {
        Self { enable_redact_log: ctx.enable_redact_log().to_owned(), storage: Some(ctx.region_store().clone()) }
    }
}

/// Go `CheckTableExec.checkTableRecord`: `admin.CheckRecordAndIndex` over the
/// table, or over each partition of a partitioned one.
pub(crate) fn check_record_and_index(
    table: &mut TableCommon,
    store: &mut dyn tidb_kv::storage::TableStorage,
    index: &Index,
    context: &RowDecodeContext,
    report: &ReportContext,
) -> Result<(), AdminCheckError> {
    for physical_id in table.record_physical_ids() {
        check_record_and_index_in(table, store, index, physical_id, context, report)?;
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
    report: &ReportContext,
) -> Result<(), AdminCheckError> {
    let decode = |error: tidb_table::tables::KvTableError| AdminCheckError::Decode(format!("{error:?}"));
    let tbl_meta = table.meta();
    let tbl_meta = tbl_meta.read().clone();
    let idx_meta = index.read().clone();
    let index_id = idx_meta.id;
    let zone = context.zone().clone();
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
                        handle: kv_handle_of(&handle)?.to_string(),
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
                    let kv_handle = kv_handle_of(&handle)?;
                    let tbl_row = tidb_logutil_consistency::RecordData { handle: kv_handle.clone(), values };
                    return Err(AdminCheckError::Report(
                        reporter(table, &tbl_meta, &idx_meta, index_id, physical_id, &zone, report, &|reporter| {
                            reporter.report_admin_check_inconsistent(&kv_handle, None, Some(&tbl_row))
                        }),
                    ));
                }
                tidb_table::tables::IndexExistence::OtherHandle(other) => {
                    let values = table.index_values_for_check(index, &row);
                    let kv_handle = kv_handle_of(&handle)?;
                    let idx_row = tidb_logutil_consistency::RecordData { handle: kv_handle_of(&other)?, values: values.clone() };
                    let tbl_row = tidb_logutil_consistency::RecordData { handle: kv_handle.clone(), values };
                    return Err(AdminCheckError::Report(
                        reporter(table, &tbl_meta, &idx_meta, index_id, physical_id, &zone, report, &|reporter| {
                            reporter.report_admin_check_inconsistent(&kv_handle, Some(&idx_row), Some(&tbl_row))
                        }),
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

/// Go `admin.CheckRecordAndIndex`'s `ir()`: the reporter over the table's
/// record keys and the index's keys of a record's values.
#[allow(clippy::too_many_arguments)]
fn reporter(
    table: &TableCommon,
    tbl_meta: &tidb_model::TableInfo,
    idx_meta: &tidb_model::IndexInfo,
    index_id: i64,
    physical_id: i64,
    zone: &tidb_datatype::SessionTimeZone,
    report: &ReportContext,
    run: &dyn Fn(&tidb_logutil_consistency::Reporter<'_>) -> tidb_terror::TerrorError,
) -> tidb_terror::TerrorError {
    let handle_encode = |handle: &tidb_txnkv::Handle| {
        Some(tidb_tablecodec::table_key::encode_row_key_with_handle(physical_id, &table_handle_of(handle).record_handle()))
    };
    let index_encode = |idx_row: &tidb_logutil_consistency::RecordData| {
        let values = idx_row.values[..idx_meta.columns.len().min(idx_row.values.len())].to_vec();
        table
            .gen_index_key(index_id, values, Some(&table_handle_of(&idx_row.handle)), physical_id, zone)
            .ok()
            .map(|(key, _)| key)
    };
    run(&tidb_logutil_consistency::Reporter {
        handle_encode: &handle_encode,
        index_encode: Some(&index_encode),
        tbl: tbl_meta,
        idx: idx_meta,
        enable_redact_log: report.enable_redact_log.clone(),
        storage: report.storage.clone(),
    })
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
