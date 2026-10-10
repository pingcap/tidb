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

//! The `SPLIT [REGION FOR] [PARTITION] TABLE ... [INDEX ...]` statement.
//!
//! Go plans it in `PlanBuilder.buildSplitRegion` (`pkg/planner/core/
//! planbuilder.go`), which validates the clause and converts every value to
//! its column's type, and runs it in `SplitTableRegionExec` /
//! `SplitIndexRegionExec` (`pkg/executor/split.go`) with the split-key
//! arithmetic of `pkg/util/regionsplit` and `util.GetValuesList`. The keys go
//! to the store's `SplitRegions`; the answer is how many regions they created
//! and the fraction that finished scattering.

use crate::kv_table::{KvIndex, KvTable, TableHandle};
use crate::{Catalog, DriverError, ExecError, MysqlError, StmtContext};
use tidb_codec::table_key::{encode_record_key, encode_table_index_prefix, RecordHandle};
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags, SessionTimeZone};
use tidb_error::tidb::errcode;

/// Go `config.SplitRegionMaxNum`'s default.
const SPLIT_REGION_MAX_NUM: i64 = 1000;
/// Go `regionsplit.MinRegionStepValue`.
const MIN_REGION_STEP_VALUE: i64 = 1000;

/// Go `buildSplitRegionsSchema`: `TOTAL_SPLIT_REGION` and
/// `SCATTER_FINISH_RATIO`, both built by `buildColumnWithName`, which marks a
/// numeric column UNSIGNED and binary.
#[must_use]
pub fn split_region_columns() -> Vec<(String, FieldType)> {
    let column = |code: FieldTypeCode, flen: i64| {
        let mut field_type = FieldType::new(code);
        field_type.set_flen(flen);
        field_type.add_flags(FieldTypeFlags::UNSIGNED);
        field_type.set_charset_name("binary");
        field_type.set_collation_name("binary");
        field_type
    };
    vec![
        (
            "TOTAL_SPLIT_REGION".to_owned(),
            column(FieldTypeCode::LongLong, 4),
        ),
        (
            "SCATTER_FINISH_RATIO".to_owned(),
            column(FieldTypeCode::Double, 8),
        ),
    ]
}

/// Plans and runs one SPLIT statement over `table`, answering Go's one row
/// (`appendSplitRegionResultToChunk`). `wait_finish` is
/// `@@tidb_wait_split_region_finish`: without it Go reports no region as
/// scattered.
pub fn split_region(
    stmt: &tidb_ast::SplitRegionStmt,
    table: &KvTable,
    catalog: &Catalog,
    ctx: &StmtContext,
    wait_finish: bool,
) -> Result<Vec<Datum>, DriverError> {
    let table_name = match table.name() {
        "" => stmt.table.last().map_or("", String::as_str),
        name => name,
    };
    if table.is_temporary() {
        return Err(DriverError::OptOnTemporaryTable("split table"));
    }
    if stmt.partition_syntax && table.partition().is_none() {
        return Err(DriverError::PartitionClauseOnNonpartitioned);
    }
    let columns: Vec<(String, FieldType)> = table
        .columns()
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect();
    let converter = ValueConverter {
        resolver: crate::driver::TableResolver {
            table_name,
            database: None,
            columns: &columns,
            constant_context: ctx.clone(),
            zone: ctx.session_zone(),
            no_unsigned_subtraction: ctx.no_unsigned_subtraction(),
            div_precision_increment: ctx.div_precision_increment(),
            // Go's builder rewrites outside any clause (`unknowClause`).
            clause_message: "",
        },
        ctx,
    };
    let keys = match stmt.index.as_deref() {
        Some(index_name) if !index_name.is_empty() => {
            let (index, values) =
                plan_index_split(stmt, index_name, table, table_name, &converter)?;
            let mut keys = Vec::new();
            for physical_id in physical_ids(stmt, table, table_name)? {
                index_split_keys(table, index, physical_id, &values, ctx, &mut keys)?;
            }
            keys
        }
        _ => {
            let values = plan_table_split(stmt, table, &converter)?;
            let mut keys = Vec::new();
            for physical_id in physical_ids(stmt, table, table_name)? {
                table_split_keys(table, table_name, physical_id, &values, ctx, &mut keys)?;
            }
            keys
        }
    };
    let regions = catalog.split_regions(keys);
    // The in-process store scatters nothing, so every created region has
    // "finished" scattering the moment it exists, as unistore reports.
    let finished = if wait_finish { regions } else { 0 };
    let ratio = if finished > 0 && regions > 0 {
        finished as f64 / regions as f64
    } else {
        0.0
    };
    Ok(vec![Datum::Int(regions as i64), Datum::Real(ratio)])
}

/// The converted values of one statement: Go `SplitRegion.ValueLists`, or its
/// `Lower`/`Upper`/`Num`.
enum SplitValues {
    Lists(Vec<Vec<Datum>>),
    Bounds {
        lower: Vec<Datum>,
        upper: Vec<Datum>,
        num: usize,
    },
}

/// Go `buildSplitIndexRegion`.
fn plan_index_split<'a>(
    stmt: &tidb_ast::SplitRegionStmt,
    index_name: &str,
    table: &'a KvTable,
    table_name: &str,
    converter: &ValueConverter<'_>,
) -> Result<(&'a KvIndex, SplitValues), DriverError> {
    let clustered = table.pk_handle_offset().is_some() || !table.common_handle_offsets().is_empty();
    if index_name.eq_ignore_ascii_case("primary") && clustered {
        return Err(coded(
            errcode::ErrKeyDoesNotExist,
            "unable to split clustered index, please split table instead.",
        ));
    }
    let index = table
        .indexes()
        .iter()
        .find(|index| index.name.eq_ignore_ascii_case(index_name))
        .ok_or_else(|| DriverError::KeyNotExists {
            key: index_name.to_owned(),
            table: table_name.to_owned(),
        })?;
    let convert_tuple = |values: &[tidb_ast::Expr]| {
        values
            .iter()
            .zip(&index.column_offsets)
            .map(|(value, offset)| {
                let column = &table.columns()[*offset];
                converter.convert(value, &column.name, &column.field_type)
            })
            .collect::<Result<Vec<_>, _>>()
    };
    match &stmt.option {
        tidb_ast::SplitOption::By(lists) => {
            let mut values = Vec::with_capacity(lists.len());
            for (position, list) in lists.iter().enumerate() {
                if list.len() > index.column_offsets.len() {
                    return Err(DriverError::WrongValueCountOnRow { row: position + 1 });
                }
                values.push(convert_tuple(list)?);
            }
            Ok((index, SplitValues::Lists(values)))
        }
        tidb_ast::SplitOption::Between {
            lower,
            upper,
            regions,
        } => {
            let check_bound = |values: &[tidb_ast::Expr], which: &str| {
                if values.is_empty() {
                    return Err(unknown(format!(
                        "Split index `{}` region {which} value count should more than 0",
                        index.name
                    )));
                }
                if values.len() > index.column_offsets.len() {
                    return Err(unknown(format!(
                        "Split index `{}` region column count doesn't match value count at {which}",
                        index.name
                    )));
                }
                convert_tuple(values)
            };
            let lower = check_bound(lower, "lower")?;
            let upper = check_bound(upper, "upper")?;
            let num = checked_region_num(*regions, "index")?;
            Ok((index, SplitValues::Bounds { lower, upper, num }))
        }
    }
}

/// Go `buildSplitTableRegion` over `buildHandleColumnInfos`.
fn plan_table_split(
    stmt: &tidb_ast::SplitRegionStmt,
    table: &KvTable,
    converter: &ValueConverter<'_>,
) -> Result<SplitValues, DriverError> {
    let handle_columns: Vec<(&str, FieldType)> = if let Some(offset) = table.pk_handle_offset() {
        let column = &table.columns()[offset];
        vec![(column.name.as_str(), column.field_type.clone())]
    } else if !table.common_handle_offsets().is_empty() {
        table
            .common_handle_offsets()
            .iter()
            .map(|offset| {
                let column = &table.columns()[*offset];
                (column.name.as_str(), column.field_type.clone())
            })
            .collect()
    } else {
        // Go `model.NewExtraHandleColInfo`.
        vec![("_tidb_rowid", FieldType::new(FieldTypeCode::LongLong))]
    };
    // Go `convertValueListToData`: a value-count mismatch names the bound, or
    // the row by its ZERO-based position.
    let convert_list = |values: &[tidb_ast::Expr], mismatch: DriverError| {
        if values.len() != handle_columns.len() {
            return Err(mismatch);
        }
        values
            .iter()
            .zip(&handle_columns)
            .map(|(value, (name, field_type))| {
                let converted = converter.convert(value, name, field_type)?;
                if converted.is_null() {
                    return Err(DriverError::ColumnCannotBeNull((*name).to_owned()));
                }
                Ok(converted)
            })
            .collect::<Result<Vec<_>, _>>()
    };
    match &stmt.option {
        tidb_ast::SplitOption::By(lists) => lists
            .iter()
            .enumerate()
            .map(|(position, list)| {
                convert_list(list, DriverError::WrongValueCountOnRow { row: position })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(SplitValues::Lists),
        tidb_ast::SplitOption::Between {
            lower,
            upper,
            regions,
        } => {
            let count_error = |which: &str| {
                unknown(format!(
                    "Split table region {which} value count should be {}",
                    handle_columns.len()
                ))
            };
            let lower = convert_list(lower, count_error("lower"))?;
            let upper = convert_list(upper, count_error("upper"))?;
            let num = checked_region_num(*regions, "table")?;
            Ok(SplitValues::Bounds { lower, upper, num })
        }
    }
}

/// The `REGIONS n` checks both builders make after converting the bounds.
fn checked_region_num(regions: i64, what: &str) -> Result<usize, DriverError> {
    if regions > SPLIT_REGION_MAX_NUM {
        return Err(unknown(format!(
            "Split {what} region num exceeded the limit {SPLIT_REGION_MAX_NUM}"
        )));
    }
    if regions < 1 {
        return Err(unknown(format!(
            "Split {what} region num should more than 0"
        )));
    }
    Ok(regions as usize)
}

/// The physical tables the executor splits: the table itself, every
/// partition, or the named ones (Go `tables.FindPartitionByName`).
fn physical_ids(
    stmt: &tidb_ast::SplitRegionStmt,
    table: &KvTable,
    table_name: &str,
) -> Result<Vec<i64>, DriverError> {
    let Some(partition) = table.partition() else {
        return Ok(vec![table.table_id]);
    };
    if stmt.partitions.is_empty() {
        return Ok(partition
            .definitions
            .iter()
            .map(|definition| definition.id)
            .collect());
    }
    stmt.partitions
        .iter()
        .map(|name| {
            partition
                .definitions
                .iter()
                .find(|definition| definition.name.eq_ignore_ascii_case(name))
                .map(|definition| definition.id)
                .ok_or_else(|| DriverError::UnknownPartition {
                    partition: name.to_lowercase(),
                    table: table_name.to_owned(),
                })
        })
        .collect()
}

/// Go `getSplitIdxPhysicalKeysFromValueList` / `GetSplitIndexKeys` for one
/// physical table.
fn index_split_keys(
    table: &KvTable,
    index: &KvIndex,
    physical_id: i64,
    values: &SplitValues,
    ctx: &StmtContext,
    keys: &mut Vec<Vec<u8>>,
) -> Result<(), DriverError> {
    let zone = ctx.session_zone();
    let start_and_end = |keys: &mut Vec<Vec<u8>>| {
        // Go `GetSplitIdxPhysicalStartAndOtherIdxKeys`: the first index's
        // start key would only cut off the useless `[t{id}, t{id}_i1)`.
        if table
            .indexes()
            .first()
            .is_some_and(|first| first.id != index.id)
        {
            keys.push(encode_table_index_prefix(physical_id, index.id));
        }
        keys.push(encode_table_index_prefix(physical_id, index.id + 1));
    };
    let index_key = |values: &[Datum]| {
        table
            .split_index_key(index, values.to_vec(), physical_id, &zone)
            .map_err(|error| DriverError::from(ExecError::from(error)))
    };
    match values {
        SplitValues::Lists(lists) => {
            start_and_end(keys);
            for list in lists {
                keys.push(index_key(list)?);
            }
        }
        SplitValues::Bounds { lower, upper, num } => {
            start_and_end(keys);
            let lower_key = index_key(lower)?;
            let upper_key = index_key(upper)?;
            if lower_key >= upper_key {
                return Err(invalid_ranges(format!(
                    "Split index `{}` region lower value {} should less than the upper value {}",
                    index.name,
                    datums_text(lower),
                    datums_text(upper)
                )));
            }
            values_list(&lower_key, &upper_key, *num, keys);
        }
    }
    Ok(())
}

/// Go `getSplitTablePhysicalKeysFromValueList` / `GetSplitTableKeys` for one
/// physical table.
fn table_split_keys(
    table: &KvTable,
    table_name: &str,
    physical_id: i64,
    values: &SplitValues,
    ctx: &StmtContext,
    keys: &mut Vec<Vec<u8>>,
) -> Result<(), DriverError> {
    let zone = ctx.session_zone();
    let record_prefix = tidb_codec::gen_table_record_prefix(physical_id);
    let common_handle = !table.common_handle_offsets().is_empty();
    match values {
        SplitValues::Lists(lists) => {
            for list in lists {
                let handle = build_handle(table, common_handle, list, &zone)?;
                keys.push(encode_record_key(&record_prefix, &handle));
            }
        }
        SplitValues::Bounds { lower, upper, num } => {
            // Split a separate region for the indexes, unless the only one
            // is a clustered primary key, which has no entries of its own.
            let indexes = table.indexes().len();
            if indexes > 0 && !(common_handle && indexes == 1) {
                keys.push(record_prefix.clone());
            }
            if !common_handle {
                let (low, step) = int_bound_and_step(table, lower, upper, *num)?;
                let mut record_id = low;
                for _ in 1..*num {
                    record_id = record_id.wrapping_add(step);
                    keys.push(encode_record_key(
                        &record_prefix,
                        &RecordHandle::Int(record_id),
                    ));
                }
                return Ok(());
            }
            let lower_handle = build_handle(table, true, lower, &zone)?;
            let upper_handle = build_handle(table, true, upper, &zone)?;
            if handle_bytes(&lower_handle) >= handle_bytes(&upper_handle) {
                return Err(invalid_ranges(format!(
                    "Split table `{table_name}` region lower value {} should less than the upper value {}",
                    datums_text(lower),
                    datums_text(upper)
                )));
            }
            let low = encode_record_key(&record_prefix, &lower_handle);
            let up = encode_record_key(&record_prefix, &upper_handle);
            values_list(&low, &up, *num, keys);
        }
    }
    Ok(())
}

/// Go `HandleCols.BuildHandleByDatums` for the handle `buildHandleColsForSplit`
/// makes: the first value's int64 bits, or the encoded primary-key tuple.
fn build_handle(
    table: &KvTable,
    common_handle: bool,
    values: &[Datum],
    zone: &SessionTimeZone,
) -> Result<RecordHandle, DriverError> {
    if !common_handle {
        return Ok(RecordHandle::Int(
            values.first().map_or(0, |value| int64_bits(value) as i64),
        ));
    }
    match table
        .common_handle_of_values(values, zone)
        .map_err(|error| DriverError::from(ExecError::from(error)))?
    {
        TableHandle::Common(bytes) => Ok(RecordHandle::Common(bytes)),
        TableHandle::Int(value) => Ok(RecordHandle::Int(value)),
    }
}

fn handle_bytes(handle: &RecordHandle) -> &[u8] {
    match handle {
        RecordHandle::Common(bytes) => bytes,
        _ => &[],
    }
}

/// Go `calculateIntBoundValue`: the lower handle and the step, compared and
/// divided as unsigned when the handle is an UNSIGNED primary key.
fn int_bound_and_step(
    table: &KvTable,
    lower: &[Datum],
    upper: &[Datum],
    num: usize,
) -> Result<(i64, i64), DriverError> {
    let lower_bits = lower.first().map_or(0, int64_bits);
    let upper_bits = upper.first().map_or(0, int64_bits);
    let (low, step) = if table.unsigned_pk_handle() {
        if upper_bits <= lower_bits {
            return Err(invalid_ranges(format!(
                "lower value {lower_bits} should less than the upper value {upper_bits}"
            )));
        }
        (
            lower_bits as i64,
            ((upper_bits - lower_bits) / num as u64) as i64,
        )
    } else {
        let (low, up) = (lower_bits as i64, upper_bits as i64);
        if up <= low {
            return Err(invalid_ranges(format!(
                "lower value {low} should less than the upper value {up}"
            )));
        }
        (low, (up.wrapping_sub(low) as u64 / num as u64) as i64)
    };
    if step < MIN_REGION_STEP_VALUE {
        return Err(invalid_ranges(format!(
            "the region size is too small, expected at least {MIN_REGION_STEP_VALUE}, but got {step}"
        )));
    }
    Ok((low, step))
}

/// Go `Datum.GetInt64` / `GetUint64`: an integer datum's 64 bits.
fn int64_bits(value: &Datum) -> u64 {
    match value {
        Datum::Int(value) => *value as u64,
        Datum::UInt(value) => *value,
        _ => 0,
    }
}

/// Go `util.GetValuesList`: `num - 1` keys evenly spaced between `lower` and
/// `upper` over the eight bytes after their common prefix.
fn values_list(lower: &[u8], upper: &[u8], num: usize, keys: &mut Vec<Vec<u8>>) {
    let common = lower
        .iter()
        .zip(upper)
        .take_while(|(left, right)| left == right)
        .count();
    let start = uint64_from_bytes(&lower[common..], 0);
    let step = uint64_from_bytes(&upper[common..], 0xff).wrapping_sub(start) / num as u64;
    let mut value = start;
    for _ in 1..num {
        value = value.wrapping_add(step);
        let mut key = Vec::with_capacity(common + 8);
        key.extend_from_slice(&lower[..common]);
        key.extend_from_slice(&value.to_be_bytes());
        keys.push(key);
    }
}

/// Go `getUint64FromBytes`: the first eight bytes, padded with `pad`.
fn uint64_from_bytes(bytes: &[u8], pad: u8) -> u64 {
    let mut buffer = [pad; 8];
    let length = bytes.len().min(8);
    buffer[..length].copy_from_slice(&bytes[..length]);
    u64::from_be_bytes(buffer)
}

/// Go `regionsplit.datumSliceToString`.
fn datums_text(values: &[Datum]) -> String {
    let mut parts = Vec::with_capacity(values.len());
    for value in values {
        match value.sql_string() {
            Ok(text) => parts.push(text),
            Err(_) => return format!("{values:?}"),
        }
    }
    format!("({})", parts.join(","))
}

/// Go `PlanBuilder.convertValue` over the statement's table: a constant
/// expression, rewritten over the table's columns (Go's mock
/// `LogicalTableDual`, so a column reference is refused as not constant),
/// evaluated and converted to its column's type under the SPLIT statement's
/// strict type flags.
struct ValueConverter<'a> {
    resolver: crate::driver::TableResolver<'a>,
    ctx: &'a StmtContext,
}

impl ValueConverter<'_> {
    fn convert(
        &self,
        value: &tidb_ast::Expr,
        column_name: &str,
        target: &FieldType,
    ) -> Result<Datum, DriverError> {
        let built = tidb_expr::rewriter::rewrite_expr_resolved(value, &self.resolver)
            .map_err(ExecError::Eval)?;
        if !matches!(built, tidb_expr::expression::Expression::Constant(_)) {
            return Err(unknown("Expect constant values".to_owned()));
        }
        let value = built
            .eval(self.ctx, tidb_chunk::row::Row::empty())
            .map_err(ExecError::Eval)?;
        // Go `ResetContextOfStmt` for a `SplitRegionStmt`: truncation is an
        // error, zero-in-date is ignored, invalid dates follow the mode.
        let flags = tidb_expr::Columns::type_flags(self.ctx)
            .with_ignore_truncate_err(false)
            .with_truncate_as_warning(false)
            .with_ignore_zero_in_date_err(true)
            .with_ignore_invalid_date_err(self.ctx.date_modes().allow_invalid_dates)
            .with_allow_negative_to_unsigned(true);
        let warnings = StatementWarnings(self.ctx);
        let zone = self.ctx.session_zone();
        let context = tidb_datatype::ConversionContext::new(
            flags,
            tidb_datatype::ConversionLocation::from_time_zone(&zone),
            &warnings,
        );
        let converted = value
            .convert_to_in_context(target, &context, &zone)
            .map_err(|error| unknown(error.to_string()))?;
        let Some(error) = converted.error else {
            return Ok(converted.value);
        };
        let reported = error.to_sql_error();
        if !matches!(
            reported.code,
            errcode::WarnDataTruncated | errcode::ErrTruncatedWrongValue | errcode::ErrBadNumber
        ) {
            return Err(coded(reported.code, &reported.message));
        }
        let Ok(text) = value.sql_string() else {
            return Err(coded(reported.code, &reported.message));
        };
        Err(coded(
            errcode::WarnDataTruncated,
            &format!(
                "Incorrect value: '{}' for column '{}'",
                truncate_chars(&text, 128),
                truncate_chars(column_name, 192)
            ),
        ))
    }
}

/// Go's `%-.Ns` verb: at most `limit` characters.
fn truncate_chars(text: &str, limit: usize) -> String {
    text.chars().take(limit).collect()
}

/// The statement's warning buffer as the conversion's warning sink.
struct StatementWarnings<'a>(&'a StmtContext);

impl tidb_datatype::ConversionWarningAppender for StatementWarnings<'_> {
    fn append_conversion_warning(&self, warning: tidb_error::terror::TerrorError) {
        let reported = warning.to_sql_error();
        self.0
            .append_warning_parts(reported.code, &reported.message);
    }
}

fn coded(code: u16, message: &str) -> DriverError {
    DriverError::Mysql(MysqlError::new(code, message))
}

/// Go `errors.Errorf`: an unclassified error, 1105 on the wire.
fn unknown(message: String) -> DriverError {
    coded(errcode::ErrUnknown, &message)
}

/// Go `exeerrors.ErrInvalidSplitRegionRanges` (8212).
fn invalid_ranges(message: String) -> DriverError {
    coded(
        errcode::ErrInvalidSplitRegionRanges,
        &format!("Failed to split region ranges: {message}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_list_spaces_keys_over_the_bytes_after_the_common_prefix() {
        let mut keys = Vec::new();
        values_list(b"t\x00a", b"t\x00b", 2, &mut keys);
        // lower suffix "a" pads to 0x61000000_00000000, upper "b" to
        // 0x62ffffff_ffffffff; half the gap past the lower.
        let expected_value =
            0x6100_0000_0000_0000_u64 + (0x62ff_ffff_ffff_ffff_u64 - 0x6100_0000_0000_0000_u64) / 2;
        let mut expected = b"t\x00".to_vec();
        expected.extend_from_slice(&expected_value.to_be_bytes());
        assert_eq!(keys, vec![expected]);
    }
}
