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
//! schema's id in name order; this builds the same objects the same way once per process
//! and shares them with every catalog, so `DESCRIBE`, `SHOW CREATE TABLE` and
//! `information_schema.columns` answer from the objects themselves. A scan
//! reads the absent metric storage and fails (`build_mem_table`).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::{Catalog, TableEntry};

/// The database name Go registers these tables under.
pub const METRICS_SCHEMA: &str = "metrics_schema";

/// Go `autoid.MetricSchemaDBID` (`SystemSchemaIDFlag | 20000`).
pub const METRICS_SCHEMA_DB_ID: i64 = (1 << 62) | 20000;

/// Every metric table's `(name, table id)`: Go sorts the names before
/// numbering them from `METRICS_SCHEMA_DB_ID + 1`, so "the id is stable
/// across different TiDB instances".
pub fn table_ids() -> &'static [(&'static str, i64)] {
    static IDS: OnceLock<Vec<(&'static str, i64)>> = OnceLock::new();
    IDS.get_or_init(|| {
        let mut names = crate::metric_table_def::METRIC_TABLE_MAP
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>();
        names.sort_unstable();
        names
            .into_iter()
            .enumerate()
            .map(|(index, name)| (name, METRICS_SCHEMA_DB_ID + 1 + index as i64))
            .collect()
    })
}

/// Every metric table as a catalog entry, keyed by name.
pub fn tables() -> HashMap<String, Arc<TableEntry>> {
    static TABLES: OnceLock<HashMap<String, Arc<TableEntry>>> = OnceLock::new();
    TABLES
        .get_or_init(|| {
            let mut scratch = Catalog::memory_schema_scratch();
            for &(name, id) in table_ids() {
                let def = crate::metric_table_def::METRIC_TABLE_MAP
                    .iter()
                    .find(|(candidate, _)| *candidate == name)
                    .map(|(_, def)| def)
                    .expect("every numbered metric table has a definition");
                let mut info = go_table_info(name, def);
                info.id = id;
                let table = tidb_table::table_from_meta::cluster_table(
                    &info,
                    &tidb_kv::storage::MemTableStorage::new(),
                    &tidb_table::table_from_meta::AutoIdSource::Unavailable,
                )
                .unwrap_or_else(|error| panic!("metrics_schema.{name} cannot load: {error}"));
                scratch
                    .register_kv_in(METRICS_SCHEMA, name, table)
                    .expect("the metrics schema exists in the scratch catalog");
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

/// Go `MetricTableDef.genColumnInfos` + `buildTableMeta` + the comment and
/// counters `metrics_schema.go`'s `init` sets.
fn go_table_info(name: &str, def: &crate::metric_table_def::MetricTableDef) -> tidb_model::TableInfo {
    use crate::infoschema_meta::{build_table_meta, Deflt, InfoColumn};
    use tidb_datatype::FieldTypeCode;
    let column = |name: &'static str, tp, size, deflt| InfoColumn {
        name,
        tp,
        size,
        decimal: 0,
        flag: 0,
        deflt,
        comment: None,
    };
    let mut columns = vec![column("time", FieldTypeCode::Datetime, 19, Some(Deflt::Str("CURRENT_TIMESTAMP")))];
    for label in def.labels {
        columns.push(column(label, FieldTypeCode::Varchar, 512, None));
    }
    if def.quantile > 0.0 {
        let default: &'static str = Box::leak(def.quantile_default().into_boxed_str());
        columns.push(column("quantile", FieldTypeCode::Double, 22, Some(Deflt::Str(default))));
    }
    columns.push(column("value", FieldTypeCode::Double, 22, None));
    let refs: Vec<&InfoColumn> = columns.iter().collect();
    let mut info = build_table_meta(name, &refs);
    info.comment = def.comment.to_owned();
    info.db_id = METRICS_SCHEMA_DB_ID;
    info.max_column_id = i64::try_from(info.columns.len()).unwrap_or(0);
    info.max_index_id = i64::try_from(info.indices.len()).unwrap_or(0);
    info
}
