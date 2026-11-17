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

//! Go `pkg/ddl/index_presplit.go`: an ADD INDEX with `PRE_SPLIT_REGIONS`
//! splits the new index's (or, under ingest and txn-merge, its temporary
//! index's) key range before any entry is written.

use tidb_datatype::{Datum, FieldType};
use tidb_ddl_build::plan_error::DdlPlanError;
use tidb_kv::region_map::RegionStore;
use tidb_log::{Field, Value};
use tidb_model::{GoShared, IndexArg, IndexArgSplitOpt, IndexInfo, ModifyIndexArgs, TableInfo};
use tidb_expr::EvalErrorSql as _;
use tidb_terror::MysqlError;

/// Go `splitArgs`.
struct SplitArgs {
    by_rows: Vec<Vec<Datum>>,
    between_lower: Vec<Datum>,
    between_upper: Vec<Datum>,
    regions_cnt: usize,
}

/// Go `preSplitIndexRegions`.
pub(crate) fn pre_split_index_regions(
    region_store: Option<&dyn RegionStore>,
    tbl_info: &TableInfo,
    all_index_infos: &[GoShared<IndexInfo>],
    reorg_tp: tidb_model::ReorgType,
    args: &ModifyIndexArgs,
    vars: &tidb_ddl_build::session_pool::ReorgSessionVars,
) -> Result<(), DdlPlanError> {
    // Go `newReorgExprCtxWithReorgMeta`.
    let expr_ctx = tidb_ddl_build::table_info_build::default_ddl_statement_context()
        .with_ddl_sql_mode(vars.sql_mode)
        .with_lazy_clock(None, vars.time_zone.clone())
        .for_reorg();
    let split_on_temp_idx = reorg_tp == tidb_model::ReorgType::INGEST || reorg_tp == tidb_model::ReorgType::TXN_MERGE;
    let index_args: Vec<IndexArg> = args.index_args.iter_deref().map(|arg| arg.read().clone()).collect();
    for (i, idx_info) in all_index_infos.iter().enumerate() {
        let idx_info = idx_info.read();
        let Some(split_args) = eval_split_datum_from_args(&expr_ctx, tbl_info, &idx_info, &index_args[i])? else {
            continue;
        };
        let mut split_keys = get_split_idx_keys(tbl_info, &idx_info, &split_args, &vars.time_zone)?;
        if split_on_temp_idx {
            for key in &mut split_keys {
                tidb_tablecodec::index_key_to_temp_index_key(key)
                    .map_err(|error| DdlPlanError::Encode(format!("{error:?}")))?;
            }
        }
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/beforePresplitIndex", &mut split_keys.clone());
        split_index_region_and_wait(region_store, tbl_info, &idx_info, split_keys)?;
    }
    Ok(())
}

/// Go `getSplitIdxKeys`.
fn get_split_idx_keys(
    tbl_info: &TableInfo,
    idx_info: &IndexInfo,
    args: &SplitArgs,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Vec<Vec<u8>>, DdlPlanError> {
    // Split index regions by user specified value lists.
    if !args.by_rows.is_empty() {
        return get_split_idx_keys_from_value_list(tbl_info, idx_info, &args.by_rows, zone);
    }
    get_split_idx_keys_from_bound(tbl_info, idx_info, &args.between_lower, &args.between_upper, args.regions_cnt, zone)
}

/// The physical tables an index's split keys go to: the table, or each
/// partition of a partitioned table unless the index is global.
fn split_physical_ids(tbl_info: &TableInfo, idx_info: &IndexInfo, value_list: bool) -> Vec<i64> {
    match tbl_info.get_partition_info() {
        None => vec![tbl_info.id],
        // Go: only the value-list form special-cases a global index.
        Some(_) if value_list && idx_info.global => vec![tbl_info.id],
        Some(pi) => pi.read().definitions.snapshot().iter().map(|p| p.id).collect(),
    }
}

/// Go `getSplitIdxKeysFromValueList`.
fn get_split_idx_keys_from_value_list(
    tbl_info: &TableInfo,
    idx_info: &IndexInfo,
    by_rows: &[Vec<Datum>],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Vec<Vec<u8>>, DdlPlanError> {
    let mut dest_keys = Vec::new();
    for physical_id in split_physical_ids(tbl_info, idx_info, true) {
        // Go `getSplitIdxPhysicalKeysFromValueList`.
        get_split_idx_physical_start_and_other_idx_keys(tbl_info, idx_info, physical_id, &mut dest_keys);
        for v in by_rows {
            dest_keys.push(gen_index_key(tbl_info, idx_info, physical_id, v.clone(), zone)?);
        }
    }
    Ok(dest_keys)
}

/// Go `getSplitIdxPhysicalStartAndOtherIdxKeys`.
fn get_split_idx_physical_start_and_other_idx_keys(
    tbl_info: &TableInfo,
    idx_info: &IndexInfo,
    physical_id: i64,
    keys: &mut Vec<Vec<u8>>,
) {
    // 1. Split in the start key for the index if the index is not the first index.
    // For the first index, splitting the start key can produce the region [tid, tid_i_1), which is useless.
    if tbl_info.indices.iter_deref().next().is_some_and(|first| first.read().id != idx_info.id) {
        keys.push(tidb_tablecodec::table_key::encode_table_index_prefix(physical_id, idx_info.id));
    }
    // 2. Split in the end key.
    keys.push(tidb_tablecodec::table_key::encode_table_index_prefix(physical_id, idx_info.id + 1));
}

/// Go `getSplitIdxKeysFromBound`.
fn get_split_idx_keys_from_bound(
    tbl_info: &TableInfo,
    idx_info: &IndexInfo,
    lower: &[Datum],
    upper: &[Datum],
    split_num: usize,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Vec<Vec<u8>>, DdlPlanError> {
    let mut keys = Vec::new();
    for physical_id in split_physical_ids(tbl_info, idx_info, false) {
        // Go `getSplitIdxPhysicalKeysFromBound`.
        get_split_idx_physical_start_and_other_idx_keys(tbl_info, idx_info, physical_id, &mut keys);
        // Split index regions by lower, upper value and calculate the step by (upper - lower)/num.
        let lower_idx_key = gen_index_key(tbl_info, idx_info, physical_id, lower.to_vec(), zone)?;
        // Use math.MinInt64 as handle_id for the upper index key to avoid affecting calculate split point.
        let upper_idx_key = gen_index_key(tbl_info, idx_info, physical_id, upper.to_vec(), zone)?;
        if lower_idx_key >= upper_idx_key {
            let err_msg = format!(
                "Split index `{}` region lower value {} should less than the upper value {}",
                idx_info.name.original(),
                datum_slice_to_string(lower),
                datum_slice_to_string(upper)
            );
            return Err(DdlPlanError::from_mysql_error(MysqlError::from_terror(
                &tidb_exeerrors::ERR_INVALID_SPLIT_REGION_RANGES.gen_with_stack_by_args(&[err_msg.into()]),
            )));
        }
        tidb_util::split::get_values_list(&lower_idx_key, &upper_idx_key, split_num, &mut keys);
    }
    Ok(keys)
}

/// Go `index.GenIndexKey(..., kv.IntHandle(math.MinInt64), nil)` over
/// `tables.NewIndex(physicalID, tblInfo, idxInfo)`.
fn gen_index_key(
    tbl_info: &TableInfo,
    idx_info: &IndexInfo,
    physical_id: i64,
    values: Vec<Datum>,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Vec<u8>, DdlPlanError> {
    let encode = |error: String| DdlPlanError::Encode(error);
    let table = tidb_table::table_from_meta::table_from_meta(tbl_info, &tidb_table::table_from_meta::AutoIdSource::Unavailable)
        .map_err(encode)?;
    let index = tidb_table::table_from_meta::kv_index(&GoShared::new(idx_info.clone()), table.deletable_columns()).map_err(encode)?;
    table
        .split_index_key(&index, values, physical_id, zone)
        .map_err(|error| encode(format!("{error:?}")))
}

/// Go `datumSliceToString`.
fn datum_slice_to_string(ds: &[Datum]) -> String {
    let mut parts = Vec::with_capacity(ds.len());
    for d in ds {
        match d.sql_string() {
            Ok(s) => parts.push(s),
            Err(_) => return format!("{ds:?}"),
        }
    }
    format!("({})", parts.join(","))
}

/// Go `splitIndexRegionAndWait`: a store that cannot split skips.
fn split_index_region_and_wait(
    region_store: Option<&dyn RegionStore>,
    tbl_info: &TableInfo,
    idx_info: &IndexInfo,
    split_idx_keys: Vec<Vec<u8>>,
) -> Result<(), DdlPlanError> {
    let Some(store) = region_store else {
        return Ok(());
    };
    let start = std::time::Instant::now();
    let (region_ids, error) = store.split_regions(split_idx_keys, true, Some(tbl_info.id));
    if let Some(error) = error {
        tidb_ddl_logutil::ddl_logger().error(
            "split table index region failed",
            &[
                Field::new("table", Value::Str(tbl_info.name.lowercase().to_owned())),
                Field::new("index", Value::Str(tbl_info.name.lowercase().to_owned())),
                Field::new("error", Value::Str(error.clone())),
            ],
        );
        return Err(DdlPlanError::from_mysql_error(MysqlError::unknown(error)));
    }
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockSplitIndexRegionAndWaitErr").is_some() {
        return Err(DdlPlanError::from_mysql_error(MysqlError::unknown(tidb_gocontext::CtxError::DeadlineExceeded.to_string())));
    }
    let finish_scatter_regions = wait_scatter_region_finish(start, store, &region_ids, tbl_info, idx_info);
    tidb_ddl_logutil::ddl_logger().info(
        "split table index region finished",
        &[
            Field::new("table", Value::Str(tbl_info.name.lowercase().to_owned())),
            Field::new("index", Value::Str(idx_info.name.lowercase().to_owned())),
            Field::new("splitRegions", Value::I64(region_ids.len() as i64)),
            Field::new("scatterRegions", Value::I64(finish_scatter_regions as i64)),
        ],
    );
    Ok(())
}

/// Go `GetSplitRegionTimeout` of the DDL worker's session: the default
/// `tidb_wait_split_region_timeout`.
const SPLIT_REGION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Go `waitScatterRegionFinish`.
fn wait_scatter_region_finish(
    start_time: std::time::Instant,
    store: &dyn RegionStore,
    region_ids: &[u64],
    tbl_info: &TableInfo,
    idx_info: &IndexInfo,
) -> usize {
    let mut finish_scatter_num = 0;
    for &region_id in region_ids {
        let remain = SPLIT_REGION_TIMEOUT.saturating_sub(start_time.elapsed());
        // Do not break on a timeout: check the remaining regions with a
        // very short backoff.
        let remain_millisecond = if remain.is_zero() { 50 } else { remain.as_millis() as i64 };
        match store.wait_scatter_region_finish(region_id, remain_millisecond) {
            Ok(()) => finish_scatter_num += 1,
            Err(error) => tidb_ddl_logutil::ddl_logger().warn(
                "wait scatter region failed",
                &[
                    Field::new("regionID", Value::U64(region_id)),
                    Field::new("table", Value::Str(tbl_info.name.lowercase().to_owned())),
                    Field::new("index", Value::Str(idx_info.name.lowercase().to_owned())),
                    Field::new("error", Value::Str(error)),
                ],
            ),
        }
    }
    finish_scatter_num
}

/// Go `evalSplitDatumFromArgs`.
fn eval_split_datum_from_args(
    build_ctx: &tidb_sessionctx::StmtContext,
    tbl_info: &TableInfo,
    idx_info: &IndexInfo,
    idx_arg: &IndexArg,
) -> Result<Option<SplitArgs>, DdlPlanError> {
    let Some(opt) = idx_arg.split_opt.as_ref() else {
        return Ok(None);
    };
    let opt: IndexArgSplitOpt = opt.read().clone();
    if !opt.value_lists.is_empty() {
        let mut index_values = Vec::with_capacity(opt.value_lists.len());
        for (i, value_list) in opt.value_lists.snapshot().iter().enumerate() {
            let value_list: Vec<String> = value_list.snapshot();
            if value_list.len() > idx_info.columns.len() {
                return Err(DdlPlanError::from_mysql_error(MysqlError::from_terror(
                    &tidb_plannererrors::ERR_WRONG_VALUE_COUNT_ON_ROW.gen_with_stack_by_args(&[(i + 1).into()]),
                )));
            }
            index_values.push(eval_const_expr_nodes(build_ctx, &value_list, tbl_info, idx_info)?);
        }
        return Ok(Some(SplitArgs { by_rows: index_values, between_lower: Vec::new(), between_upper: Vec::new(), regions_cnt: 0 }));
    }

    if opt.lower.is_empty() && opt.upper.is_empty() && opt.num > 0 {
        let lower_vals = vec![Datum::MinNotNull; idx_info.columns.len()];
        let upper_vals = vec![Datum::MaxValue; idx_info.columns.len()];
        return Ok(Some(SplitArgs {
            by_rows: Vec::new(),
            between_lower: lower_vals,
            between_upper: upper_vals,
            regions_cnt: opt.num as usize,
        }));
    }

    // Split index regions by lower, upper value.
    let check_lower_upper_value = |values_item: Vec<String>, name: &str| -> Result<Vec<Datum>, DdlPlanError> {
        if values_item.is_empty() {
            return Err(DdlPlanError::from_mysql_error(MysqlError::unknown(format!(
                "Split index `{}` region {name} value count should be greater than 0",
                idx_info.name.original()
            ))));
        }
        if values_item.len() > idx_info.columns.len() {
            return Err(DdlPlanError::from_mysql_error(MysqlError::unknown(format!(
                "Split index `{}` region column count doesn't match value count at {name}",
                idx_info.name.original()
            ))));
        }
        eval_const_expr_nodes(build_ctx, &values_item, tbl_info, idx_info)
    };
    let lower_values = check_lower_upper_value(opt.lower.snapshot(), "lower")?;
    let upper_values = check_lower_upper_value(opt.upper.snapshot(), "upper")?;
    Ok(Some(SplitArgs {
        by_rows: Vec::new(),
        between_lower: lower_values,
        between_upper: upper_values,
        regions_cnt: opt.num as usize,
    }))
}

/// Go `evalConstExprNodes`: each value's restored SQL, parsed, evaluated and
/// converted to its index column's type.
fn eval_const_expr_nodes(
    build_ctx: &tidb_sessionctx::StmtContext,
    value_list: &[String],
    tbl_info: &TableInfo,
    idx_info: &IndexInfo,
) -> Result<Vec<Datum>, DdlPlanError> {
    let mut values = Vec::with_capacity(value_list.len());
    for (j, value) in value_list.iter().enumerate() {
        let col_offset = idx_info.columns.get(j).expect("checked against the index's columns").read().offset;
        let col = tbl_info.columns.get(col_offset as usize).expect("an index column's offset").read().clone();
        let exp = tidb_expr::simple_expr::parse_simple_expr(&NoColumns, value, &tidb_expr::simple_expr::BuildOptions::default())
            .map_err(|error| DdlPlanError::from_mysql_error(MysqlError::unknown(error.to_string())))?;
        let evaluated_val = exp
            .eval(build_ctx, tidb_chunk::row::Row::empty())
            .map_err(|error| DdlPlanError::from_mysql_error(error.to_mysql_error()))?;
        values.push(convert_to(build_ctx, evaluated_val, &col.field_type, col.name.original())?);
    }
    Ok(values)
}

/// Go `evaluatedVal.ConvertTo(evalCtx.TypeCtx(), &col.FieldType)` and its
/// error mapping.
fn convert_to(
    ctx: &tidb_sessionctx::StmtContext,
    value: Datum,
    target: &FieldType,
    column_name: &str,
) -> Result<Datum, DdlPlanError> {
    let zone = tidb_exprctx::Columns::time_zone(ctx);
    // Go's static warning handler, which nothing reads.
    let context = tidb_datatype::ConversionContext::new(
        tidb_exprctx::Columns::type_flags(ctx),
        tidb_datatype::ConversionLocation::from_time_zone(&zone),
        &tidb_context::IGNORE_CONVERSION_WARNINGS,
    );
    let converted = value
        .convert_to_in_context(target, &context, &zone)
        .map_err(|error| DdlPlanError::from_mysql_error(MysqlError::unknown(error.to_string())))?;
    let Some(error) = converted.error else {
        return Ok(converted.value);
    };
    let reported = error.to_sql_error();
    if !matches!(
        reported.code,
        tidb_errno::errcode::WarnDataTruncated | tidb_errno::errcode::ErrTruncatedWrongValue | tidb_errno::errcode::ErrBadNumber
    ) {
        return Err(DdlPlanError::from_mysql_error(MysqlError::from_sql_error(reported)));
    }
    let Ok(val_str) = value.sql_string() else {
        return Err(DdlPlanError::from_mysql_error(MysqlError::from_sql_error(reported)));
    };
    Err(DdlPlanError::from_mysql_error(MysqlError::from_terror(&tidb_datatype::types_errors::ERR_TRUNCATED.gen_with_stack(format!(
        "Incorrect value: '{}' for column '{}'",
        val_str.chars().take(128).collect::<String>(),
        column_name.chars().take(192).collect::<String>()
    )))))
}

/// The empty column scope Go's DDL expression context gives a split value.
struct NoColumns;

impl tidb_expr::rewriter::ColumnResolver for NoColumns {
    fn resolve(&self, _path: &[String]) -> Option<(usize, tidb_datatype::FieldType, i64)> {
        None
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        tidb_datatype::SessionTimeZone::utc()
    }
}
