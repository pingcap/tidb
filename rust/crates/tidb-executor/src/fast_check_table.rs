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

//! Go `pkg/executor/check_table_index.go`'s `FastCheckTableExec`: with
//! `tidb_enable_fast_table_check` (the default), `ADMIN CHECK` compares each
//! index with the table by SQL on a system session -- a global checksum of
//! `crc32(md5(handle, index columns))`, then bucketed checksums narrowed until
//! few rows differ, then those rows side by side.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::AtomicI64;

use tidb_datatype::Datum;
use tidb_model::{GoShared, IndexInfo, TableInfo};
use tidb_table::tables::TableHandle;

use crate::admin_check::{inconsistent, AdminCheckError};
use crate::DriverError;
use tidb_sqlexec::{ExecutionContext, SqlExecutor};
use tikv_client::trace::TraceContext;
use tikv_client::util::with_internal_source_type;

/// Go `CheckTableFastBucketSize`.
pub static CHECK_TABLE_FAST_BUCKET_SIZE: AtomicI64 = AtomicI64::new(1024);

/// Go `queryToRow`: `se.GetSQLExecutor().ExecuteInternal(ctx, sql)` on the
/// system session `BaseExecutor.GetSysSession` hands the check, drained and
/// closed (none for a write).
fn query_to_row(
    ctx: &dyn ExecutionContext,
    se: &dyn SqlExecutor,
    sql: &str,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    tidb_sqlexec::execute_sql(ctx, se, sql, &[])
        .map_err(crate::memtable_reader::sql_exec_driver_error)
}

/// Go `kv.WithInternalSourceType(ctx, kv.InternalTxnAdmin)`.
fn admin_context() -> TraceContext {
    with_internal_source_type(&TraceContext::new(), tidb_txnkv::INTERNAL_TXN_ADMIN)
}

/// The user session's values Go `applyFastCheckSysSessionVars` copies onto
/// the system session, and its `SnapshotTS`.
#[derive(Clone, Debug, Default)]
pub struct FastCheckSessionVars {
    /// `(name, value)` of `tidb_mem_quota_query`,
    /// `tidb_distsql_scan_concurrency`, `tidb_executor_concurrency`,
    /// `max_execution_time` and `tikv_client_read_timeout`.
    pub copied: Vec<(&'static str, String)>,
    /// Go `SessionVars.SnapshotTS`.
    pub snapshot_ts: u64,
}

/// The variables Go `applyFastCheckSysSessionVars` copies.
pub const FAST_CHECK_COPIED_VARS: [&str; 5] = [
    tidb_vardef::tidb_vars::TIDB_MEM_QUOTA_QUERY,
    tidb_vardef::tidb_vars::TIDB_DIST_SQL_SCAN_CONCURRENCY,
    tidb_vardef::tidb_vars::TIDB_EXECUTOR_CONCURRENCY,
    "max_execution_time",
    "tikv_client_read_timeout",
];

/// Go `FastCheckTableExec.Next`: checks every index in `index_infos`, each on
/// its own system session. Go hands the indexes to three workers and answers
/// the first failure; this checks them in order.
pub fn fast_check_table(
    get_sys_session: &mut dyn FnMut() -> Result<Box<dyn SqlExecutor>, DriverError>,
    vars: &FastCheckSessionVars,
    db_name: &str,
    table: &TableInfo,
    index_infos: &[GoShared<IndexInfo>],
) -> Result<(), DriverError> {
    for idx_info in index_infos {
        let se = get_sys_session()?;
        init_sess_ctx(se.as_ref(), vars)?;
        handle_task(se.as_ref(), db_name, table, &idx_info.read())?;
    }
    Ok(())
}

/// Go `checkIndexWorker.initSessCtx`. The system session is not pooled, so
/// nothing is restored after the check.
fn init_sess_ctx(se: &dyn SqlExecutor, vars: &FastCheckSessionVars) -> Result<(), DriverError> {
    // Go `w.e.contextCtx`: the statement's context, no internal source.
    let ctx = TraceContext::new();
    query_to_row(&ctx, se, &format!("set @@session.{} = 1", tidb_vardef::tidb_vars::TIDB_OPT_USE_INVISIBLE_INDEXES))?;
    for (name, value) in &vars.copied {
        query_to_row(&ctx, se, &format!("set @@session.{name} = {value}"))?;
    }
    if vars.snapshot_ts != 0 {
        if let Err(error) = query_to_row(&ctx, se, &format!("set session tidb_snapshot = {}", vars.snapshot_ts)) {
            tidb_logutil::bg_logger().error(
                "fail to set tidb_snapshot",
                &[
                    tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                    tidb_log::Field::new("snapshot ts", tidb_log::Value::U64(vars.snapshot_ts)),
                ],
            );
        }
    }
    Ok(())
}

/// Go `groupByChecksum`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GroupByChecksum {
    bucket: u64,
    checksum: u64,
    count: i64,
}

/// Go `row.GetUint64` of an integer column: the value's 64 bits.
fn row_u64(datum: &Datum) -> u64 {
    match datum {
        Datum::UInt(value) => *value,
        Datum::Int(value) => *value as u64,
        _ => 0,
    }
}

/// Go `row.GetInt64`.
fn row_i64(datum: &Datum) -> i64 {
    match datum {
        Datum::Int(value) => *value,
        Datum::UInt(value) => *value as i64,
        _ => 0,
    }
}

/// Go `getCheckSum`.
fn get_check_sum(se: &dyn SqlExecutor, sql: &str) -> Result<Vec<GroupByChecksum>, DriverError> {
    Ok(query_to_row(&admin_context(), se, sql)?
        .iter()
        .map(|row| GroupByChecksum {
            bucket: row_u64(&row[1]),
            checksum: row_u64(&row[0]),
            count: row_i64(&row[2]),
        })
        .collect())
}

/// Go `getGlobalCheckSum`.
fn get_global_check_sum(se: &dyn SqlExecutor, sql: &str) -> Result<(u64, i64), DriverError> {
    let rows = query_to_row(&admin_context(), se, sql)?;
    let Some(row) = rows.first() else {
        return Ok((0, 0));
    };
    Ok((row_u64(&row[0]), row_i64(&row[1])))
}

/// Go `TableName`: `schema`.`table`.
fn table_name(schema: &str, table: &str) -> String {
    format!("`{}`.`{}`", escape_name(schema), escape_name(table))
}

/// Go `ColumnName`: `column`.
fn column_name(column: &str) -> String {
    format!("`{}`", escape_name(column))
}

fn escape_name(name: &str) -> String {
    name.replace('`', "``")
}

/// Go `kv.Handle.Compare`: integer handles by value, common handles by their
/// encoded bytes.
fn compare_handle(a: &TableHandle, b: &TableHandle) -> Ordering {
    match (a, b) {
        (TableHandle::Int(a), TableHandle::Int(b)) => a.cmp(b),
        (TableHandle::Common(a), TableHandle::Common(b)) => a.cmp(b),
        (TableHandle::Int(_), TableHandle::Common(_)) => Ordering::Less,
        (TableHandle::Common(_), TableHandle::Int(_)) => Ordering::Greater,
    }
}

/// Go `consistency.RecordData`.
#[derive(Clone)]
struct RecordData {
    handle: TableHandle,
    values: Vec<Datum>,
}

/// Go `checkIndexWorker.HandleTask`.
fn handle_task(
    se: &dyn SqlExecutor,
    db_name: &str,
    tbl_meta: &TableInfo,
    idx_info: &IndexInfo,
) -> Result<(), DriverError> {
    let bucket_size = CHECK_TABLE_FAST_BUCKET_SIZE.load(std::sync::atomic::Ordering::Relaxed) as i64;
    let tbl_name = table_name(db_name, tbl_meta.name.original());
    let column = |offset: i64| tbl_meta.columns.get(offset as usize).expect("index column offset").clone();

    let mut pk_cols = Vec::new();
    let mut pk_types = Vec::new();
    if tbl_meta.is_common_handle {
        let pk = tbl_meta.get_primary_key().expect("a clustered table has a primary key");
        for col_info in pk.read().columns.iter_deref() {
            let col_info = col_info.read();
            pk_cols.push(column_name(col_info.name.original()));
            pk_types.push(column(col_info.offset).read().field_type.clone());
        }
    } else if tbl_meta.pk_is_handle {
        pk_cols.push(column_name(tbl_meta.get_pk_name().original()));
    } else {
        // Support decoding _tidb_rowid.
        pk_cols.push(column_name(tidb_model::column::EXTRA_HANDLE_NAME));
    }
    let handle_columns = pk_cols.join(",");

    let idx_columns: Vec<_> = idx_info.columns.iter_deref().map(|col| col.read().clone()).collect();
    let index_col_names: Vec<String> = idx_columns
        .iter()
        .map(|col| {
            let tbl_col = column(col.offset);
            let tbl_col = tbl_col.read();
            if tbl_col.is_virtual_generated() && tbl_col.hidden {
                tbl_col.generated_expr_string.clone()
            } else {
                column_name(col.name.original())
            }
        })
        .collect();
    let index_columns = index_col_names.join(",");

    // CheckSum of (handle + index columns).
    let md5_handle_and_index_col = format!("crc32(md5(concat_ws(0x2, {handle_columns}, {index_columns})))");
    // Used to group by and order.
    let md5_handle = format!("crc32(md5(concat_ws(0x2, {handle_columns})))");

    let mut table_row_cnt_to_check = 0_i64;
    let mut offset = 0_i64;
    let mut modulus = 1_i64;
    let mut meet_error = false;
    let lookup_check_threshold = 100_i64;
    let mut check_once = false;

    let idx_condition = if idx_info.has_condition() {
        format!("({}) AND ", idx_info.condition_expr_string)
    } else {
        String::new()
    };
    let idx_name = idx_info.name.original();

    let ctx = admin_context();
    query_to_row(&ctx, se, "begin")?;

    // Quick pass: avoid bucketed grouping if table/index checksum already
    // matches (Go `quickPassGlobalChecksum`).
    let table_global_query = format!(
        "select /*+ read_from_storage(tikv[{tbl_name}]), AGG_TO_COP() */ bit_xor({md5_handle_and_index_col}), count(*) from {tbl_name} use index() where {idx_condition}(0 = 0)"
    );
    let index_global_query = format!(
        "select /*+ AGG_TO_COP() */ bit_xor({md5_handle_and_index_col}), count(*) from {tbl_name} use index(`{idx_name}`) where {idx_condition}(0 = 0)"
    );
    let (table_global_checksum, table_global_cnt) = get_global_check_sum(se, &table_global_query)?;
    let (index_global_checksum, index_global_cnt) = get_global_check_sum(se, &index_global_query)?;
    if table_global_cnt == index_global_cnt && table_global_checksum == index_global_checksum {
        return Ok(());
    }

    let mut times = 0;
    const MAX_TIMES: i32 = 10;
    while table_row_cnt_to_check > lookup_check_threshold || !check_once {
        times += 1;
        if times == MAX_TIMES {
            tidb_logutil::bg_logger().warn(
                "compare checksum by group reaches time limit",
                &[tidb_log::Field::new("times", tidb_log::Value::I64(i64::from(times)))],
            );
            break;
        }
        let mut where_key = format!("((cast({md5_handle} as signed) - {offset}) % {modulus})");
        let group_by_key = format!("((cast({md5_handle} as signed) - {offset}) div {modulus} % {bucket_size})");
        if !check_once {
            where_key = "0".to_owned();
        }
        check_once = true;

        let tbl_query = format!(
            "select /*+ read_from_storage(tikv[{tbl_name}]), AGG_TO_COP() */ bit_xor({md5_handle_and_index_col}), {group_by_key}, count(*) from {tbl_name} use index() where {idx_condition}({where_key} = 0) group by {group_by_key}"
        );
        let idx_query = format!(
            "select /*+ AGG_TO_COP() */ bit_xor({md5_handle_and_index_col}), {group_by_key}, count(*) from {tbl_name} use index(`{idx_name}`) where {idx_condition} ({where_key} = 0) group by{group_by_key}"
        );
        tidb_logutil::bg_logger().info(
            "fast check table by group",
            &[
                tidb_log::Field::new("table name", tidb_log::Value::Str(tbl_meta.name.original().to_owned())),
                tidb_log::Field::new("index name", tidb_log::Value::Str(idx_name.to_owned())),
                tidb_log::Field::new("times", tidb_log::Value::I64(i64::from(times))),
                tidb_log::Field::new("current offset", tidb_log::Value::I64(offset)),
                tidb_log::Field::new("current mod", tidb_log::Value::I64(modulus)),
                tidb_log::Field::new("bucket size", tidb_log::Value::I64(bucket_size)),
                tidb_log::Field::new("table sql", tidb_log::Value::Str(tbl_query.clone())),
                tidb_log::Field::new("index sql", tidb_log::Value::Str(idx_query.clone())),
            ],
        );

        // Compute table side checksum.
        let mut table_checksum = get_check_sum(se, &tbl_query)?;
        table_checksum.sort_by_key(|checksum| checksum.bucket);
        // Compute index side checksum.
        let mut index_checksum = get_check_sum(se, &idx_query)?;
        index_checksum.sort_by_key(|checksum| checksum.bucket);

        let table_checksum_map: HashMap<u64, GroupByChecksum> =
            table_checksum.iter().map(|checksum| (checksum.bucket, *checksum)).collect();
        let index_checksum_map: HashMap<u64, GroupByChecksum> =
            index_checksum.iter().map(|checksum| (checksum.bucket, *checksum)).collect();
        for bkt_idx in 0..bucket_size as u64 {
            let message = match (table_checksum_map.get(&bkt_idx), index_checksum_map.get(&bkt_idx)) {
                (Some(tbl), Some(idx)) if tbl.checksum != idx.checksum || tbl.count != idx.count => {
                    "found different checksum in the same bucket"
                }
                (Some(_), None) => "found bucket only exists in table side",
                (None, Some(_)) => "found bucket only exists in index side",
                _ => continue,
            };
            tidb_logutil::bg_logger().info(message, &[tidb_log::Field::new("bucket", tidb_log::Value::U64(bkt_idx))]);
        }

        let mut current_offset = 0_i64;
        meet_error = false;
        // Every checksum in table side should be the same as the index side.
        let mut i = 0;
        while i < table_checksum.len() && i < index_checksum.len() {
            if table_checksum[i].bucket != index_checksum[i].bucket
                || table_checksum[i].checksum != index_checksum[i].checksum
            {
                if table_checksum[i].bucket <= index_checksum[i].bucket {
                    current_offset = table_checksum[i].bucket as i64;
                    table_row_cnt_to_check = table_checksum[i].count;
                } else {
                    current_offset = index_checksum[i].bucket as i64;
                    table_row_cnt_to_check = index_checksum[i].count;
                }
                meet_error = true;
                break;
            }
            i += 1;
        }

        if !meet_error && i < index_checksum.len() && i == table_checksum.len() {
            // Table side has fewer buckets.
            current_offset = index_checksum[i].bucket as i64;
            table_row_cnt_to_check = index_checksum[i].count;
            meet_error = true;
        } else if !meet_error && i < table_checksum.len() && i == index_checksum.len() {
            // Index side has fewer buckets.
            current_offset = table_checksum[i].bucket as i64;
            table_row_cnt_to_check = table_checksum[i].count;
            meet_error = true;
        }

        if !meet_error {
            if times != 1 {
                tidb_logutil::bg_logger().error(
                    "unexpected result, no error detected in this round, but an error is detected in the previous round",
                    &[
                        tidb_log::Field::new("times", tidb_log::Value::I64(i64::from(times))),
                        tidb_log::Field::new("offset", tidb_log::Value::I64(offset)),
                        tidb_log::Field::new("mod", tidb_log::Value::I64(modulus)),
                    ],
                );
            }
            break;
        }

        offset += current_offset * modulus;
        modulus *= bucket_size;
    }

    if !meet_error {
        return Ok(());
    }
    let group_by_key = format!("((cast({md5_handle} as signed) - {offset}) % {modulus})");
    let index_sql = format!(
        "select /*+ AGG_TO_COP() */ {handle_columns}, {index_columns}, {md5_handle_and_index_col} from {tbl_name} use index(`{idx_name}`) where {idx_condition}({group_by_key} = 0) order by {handle_columns}"
    );
    let table_sql = format!(
        "select /*+ read_from_storage(tikv[{tbl_name}]), AGG_TO_COP() */ {handle_columns}, {index_columns}, {md5_handle_and_index_col} from {tbl_name} use index() where {idx_condition}({group_by_key} = 0) order by {handle_columns}"
    );
    let idx_row = query_to_row(&ctx, se, &index_sql)?;
    let tbl_row = query_to_row(&ctx, se, &table_sql)?;

    let get_handle_from_row = |row: &[Datum]| -> Result<TableHandle, DriverError> {
        if tbl_meta.is_common_handle {
            let handle_datum = row[..pk_types.len()].to_vec();
            let handle_bytes = tidb_codec::Encoder::new(tidb_datatype::new_collation_enabled())
                .encode_key(&handle_datum)
                .map_err(|error| DriverError::unsupported(error.to_string()))?;
            return Ok(TableHandle::Common(handle_bytes));
        }
        Ok(TableHandle::Int(row_i64(&row[0])))
    };
    let get_value_from_row = |row: &[Datum]| row[pk_cols.len()..pk_cols.len() + idx_columns.len()].to_vec();
    let get_check_sum = |row: &[Datum]| row_u64(&row[pk_cols.len() + idx_columns.len()]);
    // Go `consistency.Reporter.ReportAdminCheckInconsistent`.
    let report = |handle: &TableHandle, index: Option<&RecordData>, table: Option<&RecordData>| -> DriverError {
        crate::admin::admin_check_error(inconsistent(
            tbl_meta.name.original(),
            idx_name,
            handle,
            index.map(|record| (&record.handle, record.values.as_slice())),
            table.map(|record| (&record.handle, record.values.as_slice())),
        ))
    };

    let mut last_table_record: Option<RecordData> = None;
    let mut i = 0;
    while i < tbl_row.len() || i < idx_row.len() {
        let table_record = if i == tbl_row.len() {
            // No more rows in table side.
            None
        } else {
            Some(RecordData {
                handle: get_handle_from_row(&tbl_row[i])?,
                values: get_value_from_row(&tbl_row[i]),
            })
        };
        let index_record = if i == idx_row.len() {
            // No more rows in index side.
            None
        } else {
            Some(RecordData {
                handle: get_handle_from_row(&idx_row[i])?,
                values: get_value_from_row(&idx_row[i]),
            })
        };

        match (&table_record, &index_record) {
            (None, Some(index_record)) => {
                let table_record = last_table_record
                    .as_ref()
                    .filter(|last| compare_handle(&last.handle, &index_record.handle) == Ordering::Equal);
                return Err(report(&index_record.handle, Some(index_record), table_record));
            }
            (Some(table_record), None) => return Err(report(&table_record.handle, None, Some(table_record))),
            (Some(table_record), Some(index_record)) => {
                let order = compare_handle(&table_record.handle, &index_record.handle);
                if order == Ordering::Equal && get_check_sum(&tbl_row[i]) != get_check_sum(&idx_row[i]) {
                    return Err(report(&table_record.handle, Some(index_record), Some(table_record)));
                }
                if order == Ordering::Less {
                    return Err(report(&table_record.handle, None, Some(table_record)));
                }
                if order == Ordering::Greater {
                    let last = last_table_record
                        .as_ref()
                        .filter(|last| compare_handle(&last.handle, &index_record.handle) == Ordering::Equal);
                    return Err(report(&index_record.handle, Some(index_record), last));
                }
            }
            (None, None) => unreachable!("the loop runs while either side has a row"),
        }
        i += 1;
        last_table_record = table_record;
    }
    Ok(())
}
