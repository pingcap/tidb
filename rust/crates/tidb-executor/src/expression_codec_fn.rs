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

//! Go `pkg/planner/core/expression_codec_fn.go`'s encode half
//! (`tidbCodecFuncHelper.encodeHandleFromRow` / `encodeIndexKeyFromRow`),
//! which `TIDB_ENCODE_RECORD_KEY` and `TIDB_ENCODE_INDEX_KEY` call.

use tidb_datatype::{Datum, FieldType};
use tidb_exprctx::{Columns, EncodeKeyArgs, EncodeKeyError, EvalError};
use tidb_infoschema::TableEntry;
use tidb_table::tables::{TableCommon, TableHandle};

fn raw(message: impl Into<String>) -> EncodeKeyError {
    EncodeKeyError::Eval(EvalError::Raw(message.into()))
}

/// Go `encodeHandleFromRow` (no index) and `encodeIndexKeyFromRow`.
pub(crate) fn encode_key_from_row(
    is: &crate::TidbDecodeKeySnapshot,
    args: &EncodeKeyArgs<'_>,
    privileged: &dyn Fn(&str, &str) -> bool,
    ctx: &dyn Columns,
) -> Result<Vec<u8>, EncodeKeyError> {
    let (entry, physical_id) = find_common_or_partitioned_table(is, privileged, args.db, args.table)?;
    let TableEntry::Kv(tbl) = &*entry else {
        unreachable!("only a stored table is found")
    };
    let tbl: &TableCommon = tbl;
    let Some(idx_name) = args.index else {
        let record_id = build_handle(ctx, tbl, args.values)?;
        return Ok(record_key(physical_id, &record_id));
    };
    let tbl_info = tbl.meta();
    let tbl_info = tbl_info.read();
    let idx_name = idx_name.to_lowercase();
    let Some(idx_info) = tbl_info
        .indices
        .iter_deref()
        .find(|index| index.read().name.lowercase() == idx_name)
    else {
        return Err(raw("index not found"));
    };
    let idx_info = idx_info.read().clone();
    let pk_len = if tbl_info.is_common_handle { tbl.common_handle_offsets().len() } else { 1 };
    let idx_columns: Vec<_> = idx_info.columns.iter_deref().map(|column| column.read().clone()).collect();
    if idx_columns.len() + pk_len != args.values.len() {
        return Err(raw(format!(
            "column count mismatch, expected {} (index length + pk/rowid length), got {}",
            idx_columns.len() + pk_len,
            args.values.len()
        )));
    }
    let handle = build_handle(ctx, tbl, &args.values[idx_columns.len()..])?;
    let mut idx_dts = Vec::with_capacity(idx_columns.len());
    for (value, idx_col) in args.values.iter().zip(&idx_columns) {
        let col = tbl_info.columns.get(idx_col.offset as usize).expect("an index column's offset");
        let ft = col.read().field_type.clone();
        idx_dts.push(convert_to(ctx, value.clone(), &ft)?);
    }
    let zone = ctx.time_zone();
    tbl.gen_index_key(idx_info.id, idx_dts, Some(&handle), physical_id, &zone)
        .map(|(key, _)| key)
        .map_err(|error| raw(error.into_mysql_error().message))
}

/// Go `findCommonOrPartitionedTable`: the table (or the partition
/// `table(partition)` names) and its physical ID, after Go's any-privilege
/// check on the table. Only a stored table has keys; Go's `TableByName`
/// also returns a view or a sequence, whose `TableInfo` no Rust catalog
/// entry carries, so this answers those as missing.
fn find_common_or_partitioned_table(
    is: &crate::TidbDecodeKeySnapshot,
    privileged: &dyn Fn(&str, &str) -> bool,
    db_name: &str,
    tbl_name: &str,
) -> Result<(std::sync::Arc<TableEntry>, i64), EncodeKeyError> {
    let (tbl_name, part_name) = extract_table_partition(tbl_name);
    let entry = match is.table_by_name(db_name, tbl_name) {
        Some(entry) if matches!(&*entry, TableEntry::Kv(_)) => entry,
        _ => {
            return Err(EncodeKeyError::Eval(EvalError::Conversion(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .gen_with_stack_by_args(&[db_name.to_owned().into(), tbl_name.to_owned().into()]),
            )))
        }
    };
    if !privileged(db_name, tbl_name) {
        // The arguments will be filled by caller.
        return Err(EncodeKeyError::SpecificAccessDenied);
    }
    let TableEntry::Kv(tbl) = &*entry else {
        unreachable!("matched above")
    };
    let tbl_info = tbl.meta();
    let tbl_info = tbl_info.read();
    if !part_name.is_empty() {
        let Some(pi) = tbl_info.get_partition_info() else {
            return Err(raw("not a partitioned table"));
        };
        // Go `tables.FindPartitionByName`.
        let part_name = part_name.to_lowercase();
        let pid = pi.read().definitions.snapshot().iter().find(|def| def.name.lowercase() == part_name).map(|def| def.id);
        return match pid {
            Some(pid) => Ok((std::sync::Arc::clone(&entry), pid)),
            None => Err(EncodeKeyError::Eval(EvalError::Conversion(
                tidb_table::table_errors::ERR_UNKNOWN_PARTITION
                    .gen_with_stack_by_args(&[part_name.into(), tbl_info.name.original().to_owned().into()]),
            ))),
        };
    }
    let id = tbl_info.id;
    drop(tbl_info);
    Ok((entry, id))
}

/// Go `extractTablePartition`: `table(partition)`.
fn extract_table_partition(s: &str) -> (&str, &str) {
    let Some(start) = s.find('(') else {
        return (s, "");
    };
    match s.find(')') {
        Some(end) if end >= start => (&s[..start], &s[start + 1..end]),
        _ => (s, ""),
    }
}

/// Go `buildHandle`: the `_tidb_rowid` (or integer primary key), or the
/// clustered primary key's values converted to its columns' types.
fn build_handle(ctx: &dyn Columns, tbl: &TableCommon, pk_args: &[Datum]) -> Result<TableHandle, EncodeKeyError> {
    let tbl_info = tbl.meta();
    let tbl_info = tbl_info.read();
    if !tbl_info.is_common_handle {
        let Some(arg) = pk_args.first() else {
            return Err(raw("pk column count mismatch, expected 1, got 0"));
        };
        // Go `EvalInt`.
        let handle = convert_to(ctx, arg.clone(), &FieldType::new(tidb_datatype::FieldTypeCode::LongLong))?;
        return Ok(TableHandle::Int(handle.get_int64()));
    }
    let offsets = tbl.common_handle_offsets();
    if offsets.len() != pk_args.len() {
        return Err(raw(format!("pk column count mismatch, expected {}, got {}", offsets.len(), pk_args.len())));
    }
    let mut pk_dts = Vec::with_capacity(offsets.len());
    for (arg, offset) in pk_args.iter().zip(offsets) {
        let ft = tbl.columns()[*offset].read().field_type.clone();
        pk_dts.push(convert_to(ctx, arg.clone(), &ft)?);
    }
    tbl.common_handle_from_values(&pk_dts, &ctx.time_zone()).map_err(|error| raw(error.into_mysql_error().message))
}

/// Go `tablecodec.EncodeRecordKey(tbl.RecordPrefix(), recordID)`.
fn record_key(physical_id: i64, handle: &TableHandle) -> Vec<u8> {
    let handle = match handle {
        TableHandle::Int(value) => tidb_tablecodec::table_key::RecordHandle::Int(*value),
        TableHandle::Common(bytes) => tidb_tablecodec::table_key::RecordHandle::Common(bytes.clone()),
    };
    tidb_tablecodec::table_key::encode_row_key_with_handle(physical_id, &handle)
}

/// Go `Datum.ConvertTo(ctx.TypeCtx(), ft)`.
fn convert_to(ctx: &dyn Columns, value: Datum, target: &FieldType) -> Result<Datum, EncodeKeyError> {
    let zone = ctx.time_zone();
    let context = tidb_datatype::ConversionContext::new(
        ctx.type_flags(),
        tidb_datatype::ConversionLocation::from_time_zone(&zone),
        &tidb_context::IGNORE_CONVERSION_WARNINGS,
    );
    let converted = value.convert_to_in_context(target, &context, &zone).map_err(|error| raw(error.to_string()))?;
    match converted.error {
        Some(error) => Err(EncodeKeyError::Eval(EvalError::Conversion(error))),
        None => Ok(converted.value),
    }
}
