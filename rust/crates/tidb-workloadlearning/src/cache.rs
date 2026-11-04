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

//! Go `cache.go`: the in-memory copy of the latest table read-cost version
//! in `mysql.tidb_workload_values`.

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use tidb_log::{Field, Value};
use tidb_sqlescape::SqlArg;

use crate::handle::{FEEDBACK_CATEGORY, TABLE_READ_COST};
use crate::metrics::TableReadCostMetrics;
use crate::SessionPool;

/// Go `TableReadCostCache`.
#[derive(Debug, Default)]
pub struct TableReadCostCache {
    /// Go `TableReadCostMetrics`, by table ID.
    pub table_read_cost_metrics: HashMap<i64, TableReadCostMetrics>,
    /// Go `Version`.
    pub version: u64,
}

/// Go `WLCacheWorker`.
pub struct WlCacheWorker {
    sys_session_pool: Arc<dyn SessionPool>,
    table_read_cost_cache: RwLock<TableReadCostCache>,
}

impl WlCacheWorker {
    /// Go `NewWLCacheWorker`.
    pub fn new(pool: Arc<dyn SessionPool>) -> Self {
        Self {
            sys_session_pool: pool,
            table_read_cost_cache: RwLock::new(TableReadCostCache::default()),
        }
    }

    /// Go `UpdateTableReadCostCache`: loads the latest stored version when
    /// it is newer than the cached one.
    pub fn update_table_read_cost_cache(&self) {
        let cached_version = self
            .table_read_cost_cache
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .version;
        let mut loaded = None;
        let outcome = self.sys_session_pool.with_session(&mut |session| {
            let rows = match session.exec_restricted_sql(
                "SELECT version FROM mysql.tidb_workload_values
            WHERE category = %? AND type = %?
            ORDER BY version DESC LIMIT 1",
                &[
                    SqlArg::String(FEEDBACK_CATEGORY.as_bytes()),
                    SqlArg::String(TABLE_READ_COST.as_bytes()),
                ],
            ) {
                Ok(rows) => rows,
                Err(error) => {
                    tidb_logutil::err_verbose_logger().warn(
                        "Failed to get the latest table cost version",
                        &[Field::new("error", Value::Str(error.clone()))],
                    );
                    return Err(error);
                }
            };
            if rows.len() != 1 {
                tidb_logutil::bg_logger().warn(
                    "The result of latest table cost version query is not 1",
                    &[Field::new("result_rows", Value::I64(rows.len() as i64))],
                );
                return Ok(());
            }
            let latest_version_in_storage = rows[0].first().map_or(0, tidb_datatype::Datum::get_int64) as u64;
            if latest_version_in_storage <= cached_version {
                tidb_logutil::bg_logger().info(
                    "The latest table cost version in storage is the same as the cached version, no need to update",
                    &[
                        Field::new("latest_version_in_storage", Value::U64(latest_version_in_storage)),
                        Field::new("cached_version", Value::U64(cached_version)),
                    ],
                );
                return Ok(());
            }
            let rows = match session.exec_restricted_sql(
                "SELECT table_id, value FROM mysql.tidb_workload_values
            WHERE category = %? AND type = %? AND version = %?",
                &[
                    SqlArg::String(FEEDBACK_CATEGORY.as_bytes()),
                    SqlArg::String(TABLE_READ_COST.as_bytes()),
                    SqlArg::Unsigned(latest_version_in_storage),
                ],
            ) {
                Ok(rows) => rows,
                Err(error) => {
                    tidb_logutil::err_verbose_logger().warn(
                        "Failed to get the latest table cost metrics",
                        &[Field::new("error", Value::Str(error.clone()))],
                    );
                    return Err(error);
                }
            };
            let mut new_metrics = HashMap::new();
            for row in rows {
                let table_id = row[0].get_int64();
                // Go `row.GetJSON(1).String()`.
                let value = match &row[1] {
                    tidb_datatype::Datum::Json(json) => json.to_string(),
                    _ => String::new(),
                };
                match serde_json::from_str::<TableReadCostMetrics>(&value) {
                    Ok(metric) => {
                        new_metrics.insert(table_id, metric);
                    }
                    Err(error) => tidb_logutil::err_verbose_logger().warn(
                        "Failed to unmarshal table cost metrics",
                        &[
                            Field::new("table_id", Value::I64(table_id)),
                            Field::new("value", Value::Str(value)),
                            Field::new("error", Value::Str(error.to_string())),
                        ],
                    ),
                }
            }
            loaded = Some((new_metrics, latest_version_in_storage));
            Ok(())
        });
        if let Err(error) = outcome {
            if loaded.is_none() {
                tidb_logutil::bg_logger().warn(
                    "Get system session failed when updating table cost cache",
                    &[Field::new("error", Value::Str(error))],
                );
            }
        }
        if let Some((new_metrics, version)) = loaded {
            self.update_table_read_cost_cache_with_metrics(new_metrics, version);
        }
    }

    /// Go `updateTableReadCostCacheWithMetrics`.
    fn update_table_read_cost_cache_with_metrics(&self, new_metrics: HashMap<i64, TableReadCostMetrics>, version: u64) {
        let mut cache = self
            .table_read_cost_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        cache.table_read_cost_metrics = new_metrics;
        cache.version = version;
    }

    /// Go `GetTableReadCostMetrics`: a copy of the four measured fields.
    #[must_use]
    pub fn get_table_read_cost_metrics(&self, table_id: i64) -> Option<TableReadCostMetrics> {
        let cache = self
            .table_read_cost_cache
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        cache.table_read_cost_metrics.get(&table_id).map(|metric| TableReadCostMetrics {
            table_scan_time: metric.table_scan_time,
            table_mem_usage: metric.table_mem_usage,
            read_frequency: metric.read_frequency,
            table_read_cost: metric.table_read_cost,
            ..TableReadCostMetrics::default()
        })
    }
}
