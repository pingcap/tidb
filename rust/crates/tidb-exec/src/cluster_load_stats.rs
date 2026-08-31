// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Pinned Go `LoadStatsFromJSONNoUpdate` target resolution and conversion.
//!
//! This module stops at the transaction boundary. It resolves the dump's own
//! database/table name, expands partition and `global` entries, and invokes
//! the one canonical `TableStatsFromJSON` conversion. The real-TiKV writer
//! then commits every item, usage slice, and final meta update in the separate
//! transactions the Go statistics handle uses.

use tidb_datatype::FieldTypeFlags;
use tidb_executor::load_stats::{
    statistics_table_from_json_schema, JsonTable, LoadStatsColumnSchema, LoadStatsError,
    LoadStatsIndexSchema, LoadStatsTableSchema, TIDB_GLOBAL_STATS,
};
use tidb_model::table_info::TableInfo;
use tidb_stats::{JsonPredicateColumn, Table};

use crate::cluster_catalog::ClusterCatalog;
use crate::cluster_stats_load::ClusterStatsItem;

/// One parsed LOAD STATS command retained for the cluster route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterLoadStatsStatement {
    /// Decoded path from the SQL string literal.
    pub path: String,
}

/// Parses a statement only when it is LOAD STATS.
pub fn prepare_cluster_load_stats(sql: &str) -> Option<ClusterLoadStatsStatement> {
    let statement = tidb_parser::parse(sql).ok()?;
    let tidb_ast::Stmt::Admin(admin) = statement else {
        return None;
    };
    let tidb_ast::AdminStmt::LoadStats(statement) = admin.as_ref() else {
        return None;
    };
    Some(ClusterLoadStatsStatement {
        path: statement.path.clone(),
    })
}

/// One physical table produced by Go `loadStatsFromJSON` routing.
#[derive(Clone, Debug)]
pub struct LoadedStatsTable {
    /// Logical or partition physical ID.
    pub physical_id: i64,
    /// Dump row count, retained as signed Go `int64`.
    pub count: i64,
    /// Dump modify count.
    pub modify_count: i64,
    /// Columns in stable histogram-ID order.
    pub columns: Vec<ClusterStatsItem>,
    /// Indexes in stable histogram-ID order.
    pub indexes: Vec<ClusterStatsItem>,
    /// Predicate-column usage written in one later transaction.
    pub predicate_columns: Vec<Option<JsonPredicateColumn>>,
}

/// Schema and physical-table tasks selected by Go
/// `LoadStatsFromJSONNoUpdate` before its partition workers start.
pub(crate) struct ResolvedClusterLoadStats {
    pub(crate) schema: LoadStatsTableSchema,
    pub(crate) targets: Vec<ClusterLoadStatsTarget>,
    pub(crate) concurrent: bool,
}

/// One task consumed by Go `LoadStatsFromJSONConcurrently`.
pub(crate) struct ClusterLoadStatsTarget {
    pub(crate) physical_id: i64,
    json: Option<JsonTable>,
}

/// Why a statistics dump could not be resolved against the cluster catalog.
#[derive(Debug)]
pub enum ClusterLoadStatsError {
    /// The dump names a table absent from the statement infoschema.
    MissingTable {
        /// Database named by the dump.
        database: String,
        /// Table named by the dump.
        table: String,
    },
    /// Canonical JSON conversion failed.
    Convert(LoadStatsError),
}

impl std::fmt::Display for ClusterLoadStatsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingTable { database, table } => {
                write!(formatter, "Table '{database}.{table}' doesn't exist")
            }
            Self::Convert(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ClusterLoadStatsError {}

impl From<LoadStatsError> for ClusterLoadStatsError {
    fn from(error: LoadStatsError) -> Self {
        Self::Convert(error)
    }
}

/// Resolves and converts every physical table named by one dump.
#[cfg(test)]
fn lower_cluster_load_stats(
    catalog: &ClusterCatalog,
    json: &JsonTable,
) -> Result<Vec<LoadedStatsTable>, ClusterLoadStatsError> {
    let resolved = resolve_cluster_load_stats(catalog, json)?;
    resolved
        .targets
        .iter()
        .map(|target| load_cluster_stats_target(&resolved.schema, target))
        .collect()
}

pub(crate) fn resolve_cluster_load_stats(
    catalog: &ClusterCatalog,
    json: &JsonTable,
) -> Result<ResolvedClusterLoadStats, ClusterLoadStatsError> {
    let table = catalog
        .databases
        .iter()
        .find(|database| {
            database.info.name.lowercase() == json.database_name.to_lowercase().as_str()
        })
        .and_then(|database| {
            database
                .tables
                .iter()
                .find(|table| table.name.lowercase() == json.table_name.to_lowercase().as_str())
        })
        .ok_or_else(|| ClusterLoadStatsError::MissingTable {
            database: json.database_name.clone(),
            table: json.table_name.clone(),
        })?;
    let schema = load_stats_schema(table);

    let Some(partitions) = json.partitions.as_ref() else {
        return Ok(ResolvedClusterLoadStats {
            schema,
            targets: vec![ClusterLoadStatsTarget {
                physical_id: table.id,
                json: Some(json.clone()),
            }],
            concurrent: false,
        });
    };
    let Some(partition_info) = table.get_partition_info() else {
        return Ok(ResolvedClusterLoadStats {
            schema,
            targets: vec![ClusterLoadStatsTarget {
                physical_id: table.id,
                json: Some(json.clone()),
            }],
            concurrent: false,
        });
    };

    let definitions = partition_info.read().definitions.snapshot();
    let mut targets = Vec::new();
    for definition in definitions {
        let Some(Some(json)) = partitions.get(definition.name.lowercase()) else {
            continue;
        };
        targets.push(ClusterLoadStatsTarget {
            physical_id: definition.id,
            json: Some(json.clone()),
        });
    }
    if let Some(global) = partitions.get(TIDB_GLOBAL_STATS) {
        targets.push(ClusterLoadStatsTarget {
            physical_id: table.id,
            json: global.clone(),
        });
    }
    Ok(ResolvedClusterLoadStats {
        schema,
        targets,
        concurrent: true,
    })
}

pub(crate) fn load_cluster_stats_target(
    schema: &LoadStatsTableSchema,
    target: &ClusterLoadStatsTarget,
) -> Result<LoadedStatsTable, ClusterLoadStatsError> {
    let json = target
        .json
        .as_ref()
        .expect("global statistics JSON must not be null");
    Ok(loaded_table(schema, target.physical_id, json)?)
}

fn load_stats_schema(table: &TableInfo) -> LoadStatsTableSchema {
    LoadStatsTableSchema {
        columns: table
            .columns
            .iter_deref()
            .map(|column| {
                let column = column.read();
                LoadStatsColumnSchema {
                    id: column.id,
                    name: column.name.lowercase().to_owned(),
                    field_type: column.field_type.clone(),
                    primary_key: column.field_type.flags() & FieldTypeFlags::PRI_KEY != 0,
                }
            })
            .collect(),
        indexes: table
            .indices
            .iter_deref()
            .map(|index| {
                let index = index.read();
                LoadStatsIndexSchema {
                    id: index.id,
                    name: index.name.lowercase().to_owned(),
                    columns: index
                        .columns
                        .iter_deref()
                        .map(|column| column.read().name.lowercase().to_owned())
                        .collect(),
                    mv_index: index.mv_index,
                }
            })
            .collect(),
        pk_is_handle: table.pk_is_handle,
    }
}

fn loaded_table(
    schema: &LoadStatsTableSchema,
    physical_id: i64,
    json: &JsonTable,
) -> Result<LoadedStatsTable, LoadStatsError> {
    let table = statistics_table_from_json_schema(schema, physical_id, json)?;
    Ok(LoadedStatsTable {
        physical_id,
        count: json.count,
        modify_count: json.modify_count,
        columns: loaded_columns(&table),
        indexes: loaded_indexes(&table),
        predicate_columns: json.predicate_columns.clone().unwrap_or_default(),
    })
}

fn loaded_columns(table: &Table) -> Vec<ClusterStatsItem> {
    table
        .hist_coll
        .stable_columns()
        .into_iter()
        .map(|column| {
            let column = column
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            ClusterStatsItem {
                id: column.histogram.id,
                is_index: false,
                stats_ver: column.stats_version,
                flag: 0,
                load_status: column.stats_loaded_status,
                histogram: column.histogram.clone(),
                topn: column.top_n.clone(),
                cms: column.cmsketch.clone(),
                fm_sketch: None,
            }
        })
        .collect()
}

fn loaded_indexes(table: &Table) -> Vec<ClusterStatsItem> {
    table
        .hist_coll
        .stable_indices()
        .into_iter()
        .map(|index| {
            let index = index
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            ClusterStatsItem {
                id: index.histogram.id,
                is_index: true,
                stats_ver: index.stats_version,
                flag: 0,
                load_status: index.stats_loaded_status,
                histogram: index.histogram.clone(),
                topn: index.top_n.clone(),
                cms: index.cmsketch.clone(),
                fm_sketch: None,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_model::column::ColumnInfo;
    use tidb_model::db::DBInfo;
    use tidb_model::go_runtime::GoShared;
    use tidb_model::partition::{PartitionDefinition, PartitionInfo};
    use tidb_model::schema_state::SchemaState;
    use tidb_stats::{JsonColumn, JsonHistogram};

    use super::*;
    use crate::cluster_catalog::LoadedDatabase;

    fn table() -> TableInfo {
        TableInfo {
            id: 10,
            name: CiString::new("t"),
            columns: vec![ColumnInfo {
                id: 1,
                name: CiString::new("a"),
                field_type: FieldType::new(FieldTypeCode::LongLong),
                state: SchemaState::PUBLIC,
                ..ColumnInfo::default()
            }]
            .into(),
            partition: Some(GoShared::new(PartitionInfo {
                enable: true,
                definitions: vec![
                    PartitionDefinition {
                        id: 11,
                        name: CiString::new("p0"),
                        ..PartitionDefinition::default()
                    },
                    PartitionDefinition {
                        id: 12,
                        name: CiString::new("p1"),
                        ..PartitionDefinition::default()
                    },
                ]
                .into(),
                ..PartitionInfo::default()
            })),
            ..TableInfo::default()
        }
    }

    fn catalog() -> ClusterCatalog {
        ClusterCatalog {
            schema_version: 1,
            databases: vec![LoadedDatabase {
                info: DBInfo {
                    id: 1,
                    name: CiString::new("test"),
                    ..DBInfo::default()
                },
                tables: vec![table()],
            }],
        }
    }

    fn json(count: i64) -> JsonTable {
        JsonTable {
            database_name: "test".to_owned(),
            table_name: "t".to_owned(),
            count,
            columns: Some(BTreeMap::from([(
                "a".to_owned(),
                Some(JsonColumn {
                    histogram: Some(JsonHistogram {
                        ndv: count,
                        buckets: None,
                    }),
                    stats_ver: Some(2),
                    ..JsonColumn::default()
                }),
            )])),
            ..JsonTable::default()
        }
    }

    #[test]
    fn parser_admits_only_load_stats() {
        assert_eq!(
            prepare_cluster_load_stats("load stats 'x.json'"),
            Some(ClusterLoadStatsStatement {
                path: "x.json".to_owned()
            })
        );
        assert!(prepare_cluster_load_stats("analyze table t").is_none());
        assert!(prepare_cluster_load_stats("load stats").is_none());
    }

    #[test]
    fn nil_partitions_load_the_logical_table() {
        let loaded = lower_cluster_load_stats(&catalog(), &json(-1)).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].physical_id, 10);
        assert_eq!(loaded[0].count, -1);
        assert_eq!(loaded[0].columns[0].id, 1);
    }

    #[test]
    fn present_partitions_load_only_named_entries_and_global() {
        let mut root = json(100);
        root.partitions = Some(BTreeMap::from([
            ("p0".to_owned(), Some(json(40))),
            ("global".to_owned(), Some(json(100))),
        ]));
        let loaded = lower_cluster_load_stats(&catalog(), &root).unwrap();
        assert_eq!(
            loaded
                .iter()
                .map(|table| (table.physical_id, table.count))
                .collect::<Vec<_>>(),
            vec![(11, 40), (10, 100)]
        );
    }

    #[test]
    fn present_but_empty_partitions_load_nothing() {
        let mut root = json(100);
        root.partitions = Some(BTreeMap::new());
        assert!(lower_cluster_load_stats(&catalog(), &root)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn null_global_entry_is_retained_for_worker_panic_recovery() {
        let mut root = json(100);
        root.partitions = Some(BTreeMap::from([(TIDB_GLOBAL_STATS.to_owned(), None)]));
        let resolved = resolve_cluster_load_stats(&catalog(), &root).unwrap();
        assert!(resolved.concurrent);
        assert_eq!(resolved.targets.len(), 1);
        assert_eq!(resolved.targets[0].physical_id, 10);
        assert!(resolved.targets[0].json.is_none());
    }
}
