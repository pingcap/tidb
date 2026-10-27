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

//! Go `pkg/infoschema/metrics_schema.go`: the `metrics_schema` tables. Go
//! registers one virtual table per `MetricTableMap` entry when the package
//! initializes (`genColumnInfos` + `buildTableMeta`), numbered after the
//! schema's id in name order; this builds the same objects once per process
//! and shares them with every catalog, so `DESCRIBE`, `SHOW CREATE TABLE` and
//! `information_schema.columns` answer from the objects themselves. A scan
//! reads the absent metric storage and fails (`build_mem_table`).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use super::catalog::{Catalog, TableEntry};

/// The database name Go registers these tables under.
pub const METRICS_SCHEMA: &str = "metrics_schema";

/// Go `autoid.MetricSchemaDBID` (`SystemSchemaIDFlag | 20000`).
pub const METRICS_SCHEMA_DB_ID: i64 = (1 << 62) | 20000;

/// Go `MetricTableDef.genColumnInfos` + `buildTableMeta`, as the `CREATE
/// TABLE` that builds the same table: `time` first, the labels as
/// `varchar(512)`, `quantile` only for a quantile metric (its default is the
/// metric's quantile), then `value`; every table is `utf8mb4_bin` and carries
/// the metric's comment.
fn create_table_sql(name: &str, def: &tidb_metadef::metric_table_def::MetricTableDef) -> String {
    let mut columns = vec!["`time` DATETIME DEFAULT CURRENT_TIMESTAMP".to_owned()];
    for label in def.labels {
        columns.push(format!("`{label}` VARCHAR(512) COLLATE utf8mb4_bin"));
    }
    if def.quantile > 0.0 {
        columns.push(format!("`quantile` DOUBLE DEFAULT '{}'", def.quantile));
    }
    columns.push("`value` DOUBLE".to_owned());
    format!(
        "CREATE TABLE `{name}` ({}) COMMENT='{}' DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
        columns.join(", "),
        def.comment.replace('\\', "\\\\").replace('\'', "''"),
    )
}

/// Every metric table as a catalog entry, keyed by name.
pub(crate) fn tables() -> HashMap<String, Arc<TableEntry>> {
    static TABLES: OnceLock<HashMap<String, Arc<TableEntry>>> = OnceLock::new();
    TABLES
        .get_or_init(|| {
            let mut scratch = Catalog::memory_schema_scratch();
            // Go sorts by name before numbering: "the id is stable across
            // different TiDB instances".
            let mut defs = tidb_metadef::metric_table_def::METRIC_TABLE_MAP
                .iter()
                .collect::<Vec<_>>();
            defs.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (index, (name, def)) in defs.into_iter().enumerate() {
                crate::ddl::run_create_system_table_in(
                    &create_table_sql(name, def),
                    &mut scratch,
                    METRICS_SCHEMA,
                    METRICS_SCHEMA_DB_ID + 1 + index as i64,
                )
                .expect("Go's metric table definitions build in this tier");
            }
            let tables = scratch
                .tables_of(METRICS_SCHEMA)
                .map(|(name, entry)| (name.to_owned(), Arc::clone(entry)))
                .collect();
            Catalog::register_virtual_schema(METRICS_SCHEMA_DB_ID, "METRICS_SCHEMA", &tables);
            tables
        })
        .clone()
}
