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

//! Go `ttl.go`: TTL tables and yesterday's deletion histograms.

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use serde::Serialize;
use tidb_log::{Field, Value};
use tidb_model::SchemaState;

use crate::Session;

/// Go `selectDeletedRowsOneDaySQL`: the deleted rows of each table on the
/// last day.
const SELECT_DELETED_ROWS_ONE_DAY_SQL: &str = "SELECT parent_table_id, CAST(SUM(deleted_rows) AS SIGNED)
		FROM
		    mysql.tidb_ttl_job_history
		WHERE
			status != 'running'
		    AND create_time >= CURDATE() - INTERVAL 7 DAY
			AND finish_time >= CURDATE() - INTERVAL 1 DAY
			AND finish_time < CURDATE()
		GROUP BY parent_table_id;";

/// Go `selectDelaySQL`: each table's deletion delay in minutes at the end
/// of the last day.
const SELECT_DELAY_SQL: &str = "SELECT
		parent_table_id, TIMESTAMPDIFF(MINUTE, MIN(tm), CURDATE()) AS ttl_minutes
		FROM
			(
				SELECT
					table_id,
					parent_table_id,
					MAX(ttl_expire) AS tm
				FROM
					mysql.tidb_ttl_job_history
				WHERE
					create_time > CURDATE() - INTERVAL 7 DAY
					AND finish_time < CURDATE()
					AND status = 'finished'
					AND JSON_VALID(summary_text)
					AND summary_text ->> \"$.scan_task_err\" IS NULL
				GROUP BY
					table_id, parent_table_id
			) t
		GROUP BY parent_table_id;";

/// Go `ttlHistItem`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct TtlHistItem {
    /// Set: the item counts `[prevLessThan, LessThan)`; unset, it is not
    /// serialized.
    #[serde(rename = "less_than", skip_serializing_if = "Option::is_none")]
    pub less_than: Option<i64>,
    /// The item counts `[prevLessThan, MAX)`.
    #[serde(rename = "less_than_max", skip_serializing_if = "std::ops::Not::not")]
    pub less_than_max: bool,
    /// The count of items that fit the condition.
    #[serde(rename = "count")]
    pub count: i64,
}

/// Go `ttlUsageCounter`.
#[derive(Debug, Default, Serialize)]
pub struct TtlUsageCounter {
    #[serde(rename = "ttl_job_enabled")]
    pub ttl_job_enabled: bool,
    #[serde(rename = "ttl_table_count")]
    pub ttl_tables: i64,
    #[serde(rename = "ttl_job_enabled_tables")]
    pub ttl_job_enabled_tables: i64,
    #[serde(rename = "ttl_hist_date")]
    pub ttl_hist_date: String,
    #[serde(rename = "table_hist_with_delete_rows")]
    pub table_hist_with_delete_rows: Vec<TtlHistItem>,
    #[serde(rename = "table_hist_with_delay_time")]
    pub table_hist_with_delay_time: Vec<TtlHistItem>,
}

impl TtlUsageCounter {
    /// Go `UpdateTableHistWithDeleteRows`.
    pub(crate) fn update_table_hist_with_delete_rows(&mut self, rows: i64) {
        for item in &mut self.table_hist_with_delete_rows {
            if item.less_than_max || item.less_than.is_some_and(|less_than| rows < less_than) {
                item.count += 1;
                return;
            }
        }
    }

    /// Go `UpdateTableHistWithDelayTime`.
    pub(crate) fn update_table_hist_with_delay_time(&mut self, tbl_cnt: usize, hours: i64) {
        for item in &mut self.table_hist_with_delay_time {
            if item.less_than_max || item.less_than.is_some_and(|less_than| hours < less_than) {
                item.count += i64::try_from(tbl_cnt).unwrap_or(i64::MAX);
                return;
            }
        }
    }
}

const EMPTY_OR_ERROR: &str = "exec sql error or empty rows returned";

fn less_than(value: i64) -> TtlHistItem {
    TtlHistItem {
        less_than: Some(value),
        ..TtlHistItem::default()
    }
}

fn less_than_max() -> TtlHistItem {
    TtlHistItem {
        less_than_max: true,
        ..TtlHistItem::default()
    }
}

fn warn_sql(message: &str, sql: &str, error: Option<&str>) {
    let mut fields = vec![Field::new("SQL", Value::Str(sql.to_owned()))];
    if let Some(error) = error {
        fields.push(Field::new("error", Value::Str(error.to_owned())));
    }
    tidb_logutil::bg_logger().warn(message, &fields);
}

/// Go `getTTLUsageInfo`.
pub(crate) fn get_ttl_usage_info(sctx: &dyn Session) -> TtlUsageCounter {
    let mut counter = TtlUsageCounter {
        ttl_job_enabled: tidb_vardef::ENABLE_TTL_JOB.load(Ordering::Relaxed),
        ttl_hist_date: (chrono::Local::now() - chrono::Duration::hours(24))
            .format("%Y-%m-%d")
            .to_string(),
        table_hist_with_delete_rows: vec![
            less_than(10 * 1000),
            less_than(100 * 1000),
            less_than(1000 * 1000),
            less_than(10000 * 1000),
            less_than_max(),
        ],
        table_hist_with_delay_time: vec![
            less_than(1),
            less_than(6),
            less_than(24),
            less_than(72),
            less_than_max(),
        ],
        ..TtlUsageCounter::default()
    };

    let is = sctx.info_schema();
    let mut ttl_tables = HashMap::new();
    for db in is.stored_schemas() {
        for tbl_info in db.read().deprecated_tables.iter_deref() {
            let tbl_info = tbl_info.read();
            let Some(ttl_info) = tbl_info.ttl_info.as_ref() else {
                continue;
            };
            if tbl_info.state != SchemaState::PUBLIC {
                continue;
            }
            let ttl_info = ttl_info.read();
            counter.ttl_tables += 1;
            if ttl_info.enable {
                counter.ttl_job_enabled_tables += 1;
            }
            ttl_tables.insert(
                tbl_info.id,
                (ttl_info.interval_expr_str.clone(), ttl_info.interval_time_unit),
            );
        }
    }

    match sctx.exec_restricted_sql(SELECT_DELETED_ROWS_ONE_DAY_SQL) {
        Err(error) => warn_sql("exec sql error", SELECT_DELETED_ROWS_ONE_DAY_SQL, Some(&error)),
        Ok(rows) => {
            for row in rows {
                counter.update_table_hist_with_delete_rows(row.get(1).map_or(0, tidb_datatype::Datum::get_int64));
            }
        }
    }

    match sctx.exec_restricted_sql(SELECT_DELAY_SQL) {
        Err(error) => warn_sql("exec sql error", SELECT_DELAY_SQL, Some(&error)),
        Ok(rows) => {
            let mut no_history_tables = ttl_tables.len();
            for row in rows {
                let tbl_id = row.first().map_or(0, tidb_datatype::Datum::get_int64);
                let Some((interval_expr, interval_unit)) = ttl_tables.get(&tbl_id) else {
                    // table not exist, maybe truncated or deleted
                    continue;
                };
                no_history_tables -= 1;

                let eval_interval_sql = format!(
                    "SELECT TIMESTAMPDIFF(HOUR, CURDATE() - INTERVAL {} MINUTE, CURDATE() - INTERVAL {} {})",
                    row.get(1).map_or(0, tidb_datatype::Datum::get_int64),
                    interval_expr,
                    tidb_ttl::TimeUnitType::from_i64(*interval_unit),
                );
                let inner_rows = match sctx.exec_restricted_sql(&eval_interval_sql) {
                    Ok(rows) if !rows.is_empty() => rows,
                    Ok(_) => {
                        warn_sql(EMPTY_OR_ERROR, &eval_interval_sql, None);
                        continue;
                    }
                    Err(error) => {
                        warn_sql(EMPTY_OR_ERROR, &eval_interval_sql, Some(&error));
                        continue;
                    }
                };
                let hours = inner_rows[0].first().map_or(0, tidb_datatype::Datum::get_int64);
                counter.update_table_hist_with_delay_time(1, hours);
            }
            // When no history found for a table, use max delay
            counter.update_table_hist_with_delay_time(no_history_tables, i64::MAX);
        }
    }
    counter
}
