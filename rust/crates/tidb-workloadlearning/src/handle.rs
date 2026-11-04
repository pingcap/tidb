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

//! Go `handle.go`: `HandleTableReadCost` and the binary-plan extraction it
//! runs over the workload repository's statement history.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Days, Local, NaiveDateTime};
use prost::Message;
use tidb_ast::CiString;
use tidb_datatype::Datum;
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_log::{Field, Value};
use tidb_proto::tipb::{access_object, AccessObject, ExplainData, ExplainOperator, TaskType};
use tidb_sqlescape::SqlArg;

use crate::metrics::TableReadCostMetrics;
use crate::{Session, SessionPool};

const BATCH_INSERT_SIZE: usize = 1000;

/// Go `defaultPointGetMemUsage`, bytes.
const DEFAULT_POINT_GET_MEM_USAGE: i64 = 1;
/// Go `feedbackCategory`.
pub(crate) const FEEDBACK_CATEGORY: &str = "Feedback";
/// Go `tableReadCost`.
pub(crate) const TABLE_READ_COST: &str = "TableReadCost";

/// Go `defSnapshotInterval`, copied from `pkg/util/workloadrepo`.
const DEF_SNAPSHOT_INTERVAL: u64 = 3600;
/// Go `histSnapshotsTable`.
const HIST_SNAPSHOTS_TABLE: &str = "HIST_SNAPSHOTS";
/// Go `mysql.WorkloadSchema`.
const WORKLOAD_SCHEMA: &str = "workload_schema";

/// Go `workloadQueryTemplate`.
const WORKLOAD_QUERY_TEMPLATE: &str = "select summary_end.DIGEST, summary_end.DIGEST_TEXT, summary_end.BINARY_PLAN, \
    if(summary_start.EXEC_COUNT is NULL, summary_end.EXEC_COUNT, summary_end.EXEC_COUNT - summary_start.EXEC_COUNT) as EXEC_COUNT FROM \
    (SELECT DIGEST, DIGEST_TEXT, BINARY_PLAN, EXEC_COUNT \
    FROM %?.HIST_TIDB_STATEMENTS_STATS WHERE STMT_TYPE = 'Select' AND SNAP_ID = %?) summary_end \
    left join (SELECT DIGEST, DIGEST_TEXT, BINARY_PLAN, EXEC_COUNT \
    FROM %?.HIST_TIDB_STATEMENTS_STATS WHERE STMT_TYPE = 'Select' AND SNAP_ID = %?) summary_start \
    on summary_end.DIGEST = summary_start.DIGEST";

fn warn(message: &str, fields: &[Field]) {
    tidb_logutil::bg_logger().warn(message, fields);
}

fn verbose_warn(message: &str, fields: &[Field]) {
    tidb_logutil::err_verbose_logger().warn(message, fields);
}

fn error_field(error: &str) -> Field {
    Field::new("error", Value::Str(error.to_owned()))
}

/// Go `Handle`: the entry point of the workload-based learning tasks.
pub struct Handle {
    sys_session_pool: Arc<dyn SessionPool>,
}

impl Handle {
    /// Go `NewWorkloadLearningHandle`.
    pub fn new(pool: Arc<dyn SessionPool>) -> Self {
        Self {
            sys_session_pool: pool,
        }
    }

    /// Go `HandleTableReadCost`: one round over the table read history,
    /// saved to `mysql.tidb_workload_values`.
    pub fn handle_table_read_cost(&self, info_schema: &SessionExtendedInfoSchema) {
        let Some((table_id_to_metrics, start_time, end_time)) =
            self.analyze_based_on_statement_stats(info_schema)
        else {
            warn(
                "Failed to analyze table cost metrics from statement stats, skip saving table read cost metrics",
                &[],
            );
            return;
        };
        self.save_table_read_cost_metrics(&table_id_to_metrics, start_time, end_time);
    }

    /// Go `analyzeBasedOnStatementStats`; `None` is Go's nil map.
    #[allow(clippy::type_complexity)]
    fn analyze_based_on_statement_stats(
        &self,
        info_schema: &SessionExtendedInfoSchema,
    ) -> Option<(HashMap<i64, TableReadCostMetrics>, NaiveDateTime, NaiveDateTime)> {
        let end_time = Local::now().naive_local();
        let start_time = end_time - Days::new(7);
        let mut result = None;
        let outcome = self.sys_session_pool.with_session(&mut |session| {
            // Go's deferred Put/Destroy reads the function's `err`: only the
            // snapshot lookups and the history query assign it.
            let start_snapshot_id = find_closest_snapshot_id_by_time(session, start_time).map_err(|error| {
                verbose_warn(
                    "Failed to query HIST_SNAPSHOTS table to find start snapshot id",
                    &[error_field(&error)],
                );
                error
            })?;
            let end_snapshot_id = find_closest_snapshot_id_by_time(session, end_time).map_err(|error| {
                verbose_warn(
                    "Failed to query HIST_SNAPSHOTS table to find end snapshot id",
                    &[error_field(&error)],
                );
                error
            })?;
            let sql = tidb_sqlescape::escape_sql_text(
                WORKLOAD_QUERY_TEMPLATE,
                &[
                    SqlArg::String(WORKLOAD_SCHEMA.as_bytes()),
                    SqlArg::Unsigned(end_snapshot_id),
                    SqlArg::String(WORKLOAD_SCHEMA.as_bytes()),
                    SqlArg::Unsigned(start_snapshot_id),
                ],
            )
            .map_err(|error| {
                verbose_warn(
                    "Failed to construct SQL query for ClusterTableTiDBStatementsStats",
                    &[error_field(&error.to_string())],
                );
                error.to_string()
            })?;
            let rows = session.exec_restricted_sql(&sql, &[]).map_err(|error| {
                verbose_warn("Failed to query CLUSTER_TIDB_STATEMENTS_STATS table", &[error_field(&error)]);
                error
            })?;
            let mut table_id_to_metrics = HashMap::new();
            for row in rows {
                if row.iter().take(4).any(Datum::is_null) || row.len() < 4 {
                    continue;
                }
                let binary_plan = String::from_utf8_lossy(row[2].as_raw_bytes().unwrap_or_default()).into_owned();
                let read_frequency = row[3].get_int64();
                let current_record_metrics = match extract_scan_and_memory_from_binary_plan(&binary_plan) {
                    Ok(metrics) => metrics,
                    Err(error) => {
                        verbose_warn(
                            "failed to abstract scan and memory from binary plan",
                            &[
                                Field::new(
                                    "digest",
                                    Value::Str(String::from_utf8_lossy(row[0].as_raw_bytes().unwrap_or_default()).into_owned()),
                                ),
                                error_field(&error),
                            ],
                        );
                        continue;
                    }
                };
                accumulate_metrics_group_by_table_id(
                    current_record_metrics,
                    read_frequency,
                    &mut table_id_to_metrics,
                    info_schema,
                );
            }
            // Go: `totalScanTime` and `totalMemUsage` wrap like int64.
            let (mut total_scan_time, mut total_mem_usage) = (0_i64, 0_i64);
            for metric in table_id_to_metrics.values() {
                total_mem_usage = total_mem_usage.wrapping_add(metric.table_mem_usage);
                total_scan_time = total_scan_time.wrapping_add(metric.table_scan_time);
            }
            for metric in table_id_to_metrics.values_mut() {
                let scan_time_percentage = if total_scan_time != 0 {
                    metric.table_scan_time as f64 / total_scan_time as f64
                } else {
                    0.0
                };
                let memory_usage_percentage = if total_mem_usage != 0 {
                    metric.table_mem_usage as f64 / total_mem_usage as f64
                } else {
                    0.0
                };
                metric.table_read_cost = scan_time_percentage + memory_usage_percentage;
            }
            result = Some(table_id_to_metrics);
            Ok(())
        });
        if let Err(error) = outcome {
            if result.is_none() && !error.is_empty() {
                // Go logs a failed `Get` here; the other failures were logged
                // where they happened.
                warn(
                    "get system session failed when fetch data from statements_stats",
                    &[error_field(&error)],
                );
            }
        }
        result.map(|metrics| (metrics, start_time, end_time))
    }

    /// Go `SaveTableReadCostMetrics`: one transaction inserting the metrics
    /// in batches of 1000, versioned by its start timestamp.
    pub fn save_table_read_cost_metrics(
        &self,
        metrics: &HashMap<i64, TableReadCostMetrics>,
        _start_time: NaiveDateTime,
        _end_time: NaiveDateTime,
    ) {
        let outcome = self.sys_session_pool.with_session(&mut |session| {
            // Go's deferred Put/Destroy reads the function's `err`: the new
            // transaction, its start timestamp and the commit assign it.
            session.new_txn().map_err(|error| {
                warn("get txn failed when saving table cost metrics", &[error_field(&error)]);
                error
            })?;
            let version = session.txn_start_ts().map_err(|error| {
                warn("failed to get txn when saving table cost metrics", &[error_field(&error)]);
                error
            })?;
            session.enable_non_prepared_plan_cache();
            const INSERT: &str =
                "insert into mysql.tidb_workload_values (version, category, type, table_id, value) values ";
            let mut sql = String::from(INSERT);
            for (i, (table_id, metric)) in metrics.iter().enumerate() {
                let metric_bytes = match tidb_model::serde_helpers::to_go_json(metric) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        warn("marshal table cost metrics failed", &[error_field(&error.to_string())]);
                        continue;
                    }
                };
                sql.push_str(&tidb_sqlescape::escape_sql_text(
                    "(%?, %?, %?, %?, %?)",
                    &[
                        SqlArg::Unsigned(version),
                        SqlArg::String(FEEDBACK_CATEGORY.as_bytes()),
                        SqlArg::String(TABLE_READ_COST.as_bytes()),
                        SqlArg::Signed(*table_id),
                        SqlArg::RawJson(&metric_bytes),
                    ],
                )
                .unwrap_or_else(|error| panic!("{error}")));
                if i % BATCH_INSERT_SIZE == BATCH_INSERT_SIZE - 1 {
                    if let Err(error) = session.exec_restricted_sql(&sql, &[]) {
                        warn("insert new version table cost metrics failed", &[error_field(&error)]);
                        return Ok(());
                    }
                    sql = String::from(INSERT);
                } else {
                    sql.push_str(", ");
                }
            }
            // Go: the last batch, its trailing ", " cut. A batch boundary
            // leaves the bare `INSERT ... values ` prefix, which Go cuts the
            // same way and runs.
            let last = &sql[..sql.len() - 2];
            if let Err(error) = session.exec_restricted_sql(last, &[]) {
                warn("insert new version table cost metrics failed", &[error_field(&error)]);
                return Ok(());
            }
            if let Err(error) = session.commit_txn() {
                warn("commit txn failed when saving table cost metrics", &[error_field(&error)]);
                return Err(error);
            }
            Ok(())
        });
        if let Err(error) = outcome {
            warn("get system session failed when saving table cost metrics", &[error_field(&error)]);
        }
    }
}

/// Go `findClosestSnapshotIDByTime`. The query is Go's text: the schema and
/// table are `%?` string arguments and `END_TIME != NULL` never holds.
fn find_closest_snapshot_id_by_time(session: &dyn Session, time: NaiveDateTime) -> Result<u64, String> {
    let earliest = time - Days::new(DEF_SNAPSHOT_INTERVAL * 2);
    let sql = tidb_sqlescape::escape_sql_text(
        "select SNAPSHOT_ID from %?.%? where BEGIN_TIME > %? and END_TIME != NULL order by (%? - BEGIN_TIME) asc limit 1",
        &[
            SqlArg::String(WORKLOAD_SCHEMA.as_bytes()),
            SqlArg::String(HIST_SNAPSHOTS_TABLE.as_bytes()),
            SqlArg::Time(Some(earliest)),
            SqlArg::Time(Some(time)),
        ],
    )
    .unwrap_or_else(|error| panic!("{error}"));
    let rows = session.exec_restricted_sql(&sql, &[])?;
    match rows.first() {
        Some(row) => Ok(row.first().map_or(0, Datum::get_int64) as u64),
        None => Err(format!(
            "no closest snapshot found by time in hist_snapshot table, time: {time}"
        )),
    }
}

/// Go `extractScanAndMemoryFromBinaryPlan`: one metric per scan operator of
/// the plan. Go seeds the list with one nil entry (`make(..., 1)`), kept here
/// as `None`.
fn extract_scan_and_memory_from_binary_plan(binary_plan: &str) -> Result<Vec<Option<TableReadCostMetrics>>, String> {
    let proto_bytes = tidb_plancodec::decompress(binary_plan.as_bytes()).map_err(|error| error.to_string())?;
    let explain_data = ExplainData::decode(proto_bytes.as_slice()).map_err(|error| error.to_string())?;
    if explain_data.discarded_due_to_too_long {
        return Err("plan is too long, discarded it, failed to convert to ExplainData".to_owned());
    }
    if !explain_data.with_runtime_stats {
        return Err("plan is not with runtime stats, failed to convert to ExplainData".to_owned());
    }
    let mut metrics = vec![None];
    if let Some(main) = &explain_data.main {
        extract_metrics_from_operator_tree(main, &mut metrics)?;
    }
    for cte in &explain_data.ctes {
        extract_metrics_from_operator_tree(cte, &mut metrics)?;
    }
    for sub_query in &explain_data.subqueries {
        extract_metrics_from_operator_tree(sub_query, &mut metrics)?;
    }
    Ok(metrics)
}

/// Go `AccumulateMetricsGroupByTableID`: `previous += current * frequency`,
/// grouped by the table each metric names.
///
/// # Panics
///
/// On a `None` metric, where Go dereferences its nil entry.
pub fn accumulate_metrics_group_by_table_id(
    current_record_metrics: Vec<Option<TableReadCostMetrics>>,
    frequency: i64,
    previous_metrics: &mut HashMap<i64, TableReadCostMetrics>,
    info_schema: &SessionExtendedInfoSchema,
) {
    for operator_extract_metric in current_record_metrics {
        let mut metric = operator_extract_metric.expect("invalid memory address or nil pointer dereference");
        let Some((_, table)) =
            info_schema.table_info_by_name(metric.db_name.original(), metric.table_name.original())
        else {
            warn(
                "failed to find the table id by table name and db name",
                &[
                    Field::new("db_name", Value::Str(metric.db_name.original().to_owned())),
                    Field::new("table_name", Value::Str(metric.table_name.original().to_owned())),
                ],
            );
            continue;
        };
        let table_id = table.read().id;
        metric.table_scan_time = metric.table_scan_time.wrapping_mul(frequency);
        metric.table_mem_usage = metric.table_mem_usage.wrapping_mul(frequency);
        metric.read_frequency = frequency;
        match previous_metrics.get_mut(&table_id) {
            None => {
                previous_metrics.insert(table_id, metric);
            }
            Some(previous) => {
                previous.table_scan_time = previous.table_scan_time.wrapping_add(metric.table_scan_time);
                previous.table_mem_usage = previous.table_mem_usage.wrapping_add(metric.table_mem_usage);
                previous.read_frequency = previous.read_frequency.wrapping_add(metric.read_frequency);
            }
        }
    }
}

/// Go `extractMetricsFromOperatorTree`.
fn extract_metrics_from_operator_tree(
    op: &ExplainOperator,
    operator_metrics: &mut Vec<Option<TableReadCostMetrics>>,
) -> Result<(), String> {
    let operator_type = extract_operator_type_from_name(&op.name)?;
    match operator_type {
        "IndexLookUp" | "IndexReader" => {
            let mem_usage = op.memory_bytes;
            let scan_time = extract_scan_time_from_execution_info(op)
                .map_err(|_| format!("failed to extract scan time from execution info, operator name: {}", op.name))?;
            let (db_name, table_name) = extract_table_name_from_index_scan(op);
            if db_name.is_empty() || table_name.is_empty() {
                return Err(format!(
                    "failed to get table name from children index xxx operator, operator name: {}",
                    op.name
                ));
            }
            operator_metrics.push(Some(scan_metric(&db_name, &table_name, scan_time, mem_usage)));
        }
        "PointGet" | "BatchPointGet" => {
            let Some(access_object) = op.access_objects.first() else {
                return Err(format!(
                    "failed to get table name while access object is empty, operator name: {}",
                    op.name
                ));
            };
            let (db_name, table_name) = extract_table_name_from_access_object(access_object);
            if db_name.is_empty() || table_name.is_empty() {
                return Err(format!(
                    "failed to get table name from access object, operator name: {}, access object: {access_object:?}",
                    op.name
                ));
            }
            let scan_time = extract_scan_time_from_execution_info(op).map_err(|error| {
                format!(
                    "failed to extract scan time from execution info, operator name: {}, err: {error}",
                    op.name
                )
            })?;
            operator_metrics.push(Some(scan_metric(&db_name, &table_name, scan_time, DEFAULT_POINT_GET_MEM_USAGE)));
        }
        "TableReader" => {
            if check_tiflash_operator(op) {
                return Ok(());
            }
            let scan_time = extract_scan_time_from_execution_info(op)
                .map_err(|_| format!("failed to extract scan time from execution info, operator name: {}", op.name))?;
            let mem_usage = op.memory_bytes;
            let (db_name, table_name) = extract_table_name_from_children_table_scan(op);
            if db_name.is_empty() || table_name.is_empty() {
                return Err(format!(
                    "failed to get table name from children table xxx operator, operator name: {}",
                    op.name
                ));
            }
            operator_metrics.push(Some(scan_metric(&db_name, &table_name, scan_time, mem_usage)));
        }
        "IndexMerge" => {
            let total_memory_usage = op.memory_bytes;
            let partial_metrics = match extract_partial_metrics_from_children_index_merge(op) {
                Ok(metrics) if !metrics.is_empty() => metrics,
                _ => {
                    return Err(format!(
                        "failed to get time and table name from children operator of index merge, operator name: {}",
                        op.name
                    ))
                }
            };
            let mem_usage = total_memory_usage / partial_metrics.len() as i64;
            for mut partial_metric in partial_metrics {
                partial_metric.table_mem_usage = mem_usage;
                operator_metrics.push(Some(partial_metric));
            }
        }
        _ => {}
    }
    for child in &op.children {
        extract_metrics_from_operator_tree(child, operator_metrics)?;
    }
    Ok(())
}

fn scan_metric(db_name: &str, table_name: &str, scan_time: i64, mem_usage: i64) -> TableReadCostMetrics {
    TableReadCostMetrics {
        db_name: CiString::new(db_name),
        table_name: CiString::new(table_name),
        table_scan_time: scan_time,
        table_mem_usage: mem_usage,
        ..TableReadCostMetrics::default()
    }
}

/// Go `checkTiFlashOperator`: true when a direct child runs as an MPP task
/// (Go discards its recursive call's answer).
fn check_tiflash_operator(op: &ExplainOperator) -> bool {
    for child in &op.children {
        if child.task_type == TaskType::Mpp as i32 {
            return true;
        }
        check_tiflash_operator(child);
    }
    false
}

/// Go `extractTableNameFromAccessObject`.
fn extract_table_name_from_access_object(access_object: &AccessObject) -> (String, String) {
    match &access_object.access_object {
        Some(access_object::AccessObject::DynamicPartitionObjects(objects)) => objects
            .objects
            .first()
            .map_or_else(Default::default, |access| (access.database.clone(), access.table.clone())),
        Some(access_object::AccessObject::ScanObject(object)) => (object.database.clone(), object.table.clone()),
        _ => (String::new(), String::new()),
    }
}

/// Go `extractTableNameFromChildrenTableScan`.
fn extract_table_name_from_children_table_scan(op: &ExplainOperator) -> (String, String) {
    for child in &op.children {
        let Ok(child_type) = extract_operator_type_from_name(&child.name) else {
            return (String::new(), String::new());
        };
        if child_type == "TableFullScan" || child_type == "TableRangeScan" {
            return extract_table_name_from_access_object(&child.access_objects[0]);
        }
        let (db_name, table_name) = extract_table_name_from_children_table_scan(child);
        if !db_name.is_empty() && !table_name.is_empty() {
            return (db_name, table_name);
        }
    }
    (String::new(), String::new())
}

/// Go `extractTableNameFromIndexScan`.
fn extract_table_name_from_index_scan(op: &ExplainOperator) -> (String, String) {
    for child in &op.children {
        let Ok(child_type) = extract_operator_type_from_name(&child.name) else {
            return (String::new(), String::new());
        };
        if child_type == "IndexRangeScan" || child_type == "IndexFullScan" {
            return extract_table_name_from_access_object(&child.access_objects[0]);
        }
        let (db_name, table_name) = extract_table_name_from_index_scan(child);
        if !db_name.is_empty() && !table_name.is_empty() {
            return (db_name, table_name);
        }
    }
    (String::new(), String::new())
}

/// Go `extractPartialMetricsFromChildrenIndexMerge`.
fn extract_partial_metrics_from_children_index_merge(op: &ExplainOperator) -> Result<Vec<TableReadCostMetrics>, String> {
    let Some(first) = op.children.first() else {
        return Ok(Vec::new());
    };
    let first_type = extract_operator_type_from_name(&first.name)?;
    if first_type == "IndexRangeScan" || first_type == "IndexFullScan" {
        let mut result = Vec::with_capacity(op.children.len());
        for child in &op.children {
            let (db_name, table_name) = extract_table_name_from_access_object(&child.access_objects[0]);
            if db_name.is_empty() || table_name.is_empty() {
                return Err(format!(
                    "failed to get table name from children index xxx operator, operator name: {}",
                    child.name
                ));
            }
            let scan_time = extract_scan_time_from_execution_info(child)
                .map_err(|_| format!("failed to extract scan time from execution info, operator name: {}", child.name))?;
            result.push(scan_metric(&db_name, &table_name, scan_time, 0));
        }
        return Ok(result);
    }
    let mut last = Ok(Vec::new());
    for child in &op.children {
        last = extract_partial_metrics_from_children_index_merge(child);
        if last.as_ref().is_ok_and(|result| !result.is_empty()) {
            return last;
        }
    }
    // Go returns `nil, err` with the last child's error.
    last.map(|_| Vec::new())
}

/// Go `extractOperatorTypeFromName`: `Type_ID` to `Type`.
fn extract_operator_type_from_name(name: &str) -> Result<&str, String> {
    let names: Vec<&str> = name.split('_').collect();
    if names.len() != 2 {
        return Err(format!("failed to extract operator type from operator name: {name}"));
    }
    Ok(names[0])
}

/// Go `extractScanTimeFromExecutionInfo`, nanoseconds.
fn extract_scan_time_from_execution_info(op: &ExplainOperator) -> Result<i64, String> {
    let mut scan_time = 0;
    let mut result = Ok(());
    if !op.root_basic_exec_info.is_empty() {
        match extract_scan_time_from_string(&op.root_basic_exec_info) {
            Ok(time) => scan_time = time,
            Err(error) => result = Err(error),
        }
    }
    if scan_time == 0 {
        if let Some(group) = op.root_group_exec_info.first() {
            match extract_scan_time_from_string(group) {
                Ok(time) => {
                    scan_time = time;
                    result = Ok(());
                }
                Err(error) => result = Err(error),
            }
        }
    }
    if scan_time == 0 && !op.cop_exec_info.is_empty() {
        match extract_scan_time_from_string(&op.cop_exec_info) {
            Ok(time) => {
                scan_time = time;
                result = Ok(());
            }
            Err(error) => result = Err(error),
        }
    }
    result.map(|()| scan_time)
}

/// Go `extractScanTimeFromString`: the duration between `time:` and the next
/// comma.
fn extract_scan_time_from_string(input: &str) -> Result<i64, String> {
    let Some(start) = input.find("time:") else {
        return Err("'time:' not found in input string".to_owned());
    };
    let start = start + "time:".len();
    let Some(end) = input[start..].find(',') else {
        return Err("',' not found after 'time:'".to_owned());
    };
    let time_str = input[start..start + end].trim();
    tidb_config_configtypes::parse_duration(time_str).map_err(|error| format!("failed to parse duration: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operator(name: &str, children: Vec<ExplainOperator>) -> ExplainOperator {
        ExplainOperator {
            name: name.to_owned(),
            children,
            ..ExplainOperator::default()
        }
    }

    fn scan(name: &str, table: &str) -> ExplainOperator {
        ExplainOperator {
            name: name.to_owned(),
            access_objects: vec![AccessObject {
                access_object: Some(access_object::AccessObject::ScanObject(tidb_proto::tipb::ScanAccessObject {
                    database: "test".to_owned(),
                    table: table.to_owned(),
                    ..Default::default()
                })),
            }],
            ..ExplainOperator::default()
        }
    }

    #[test]
    fn scan_time_is_go_parse_duration_of_the_time_field() {
        assert_eq!(extract_scan_time_from_string("time:274.5µs, loops:1, cop_task:"), Ok(274_500));
        assert!(extract_scan_time_from_string("loops:1").is_err());
        assert!(extract_scan_time_from_string("time:1ms").is_err());
        let mut op = operator("TableReader_1", vec![]);
        op.root_basic_exec_info = "time:0s, loops:1".to_owned();
        op.cop_exec_info = "time:2ms, loops:2".to_owned();
        assert_eq!(extract_scan_time_from_execution_info(&op), Ok(2_000_000));
    }

    #[test]
    fn operator_trees_yield_one_metric_per_scan_after_gos_nil_seed() {
        let mut reader = operator("TableReader_3", vec![operator("Selection_2", vec![scan("TableFullScan_1", "t1")])]);
        reader.root_basic_exec_info = "time:1ms, loops:1".to_owned();
        reader.memory_bytes = 64;
        let mut lookup = operator("IndexLookUp_6", vec![scan("IndexRangeScan_4", "t2"), scan("TableRowIDScan_5", "t2")]);
        lookup.root_basic_exec_info = "time:3ms, loops:1".to_owned();
        lookup.memory_bytes = 8;
        let mut metrics = vec![None];
        extract_metrics_from_operator_tree(&operator("HashJoin_7", vec![reader, lookup]), &mut metrics).unwrap();
        assert_eq!(metrics.len(), 3);
        assert!(metrics[0].is_none(), "Go's make([]*T, 1) seed");
        let t1 = metrics[1].as_ref().unwrap();
        assert_eq!((t1.table_name.original(), t1.table_scan_time, t1.table_mem_usage), ("t1", 1_000_000, 64));
        let t2 = metrics[2].as_ref().unwrap();
        assert_eq!((t2.table_name.original(), t2.table_scan_time, t2.table_mem_usage), ("t2", 3_000_000, 8));
        assert!(extract_operator_type_from_name("Table_Reader_1").is_err());
    }

    #[test]
    fn metrics_marshal_as_gos_untagged_struct() {
        let metric = TableReadCostMetrics {
            db_name: CiString::new("test"),
            table_name: CiString::new("T"),
            table_scan_time: 10,
            table_mem_usage: 10,
            read_frequency: 10,
            table_read_cost: 1.0,
        };
        assert_eq!(
            String::from_utf8(tidb_model::serde_helpers::to_go_json(&metric).unwrap()).unwrap(),
            r#"{"DbName":{"O":"test","L":"test"},"TableName":{"O":"T","L":"t"},"TableScanTime":10,"TableMemUsage":10,"ReadFrequency":10,"TableReadCost":1}"#
        );
    }
}
