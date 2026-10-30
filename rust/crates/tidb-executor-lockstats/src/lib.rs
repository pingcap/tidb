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

//! Go `pkg/executor/lockstats`: the `LOCK STATS` / `UNLOCK STATS` statement
//! as the executor holds it -- its targets, with the default schema applied
//! -- and its resolution into the statistics handle's `LockTables` /
//! `LockPartitions` arguments (`populateTableAndPartitionIDs`,
//! `populatePartitionIDAndNames`).

use std::collections::BTreeMap;

use tidb_hack::GoToLower;
use tidb_infoschema::cluster_catalog::ClusterCatalog;
use tidb_model::table_info::TableInfo;
use tidb_stats_handle_lockstats::cluster_stats_lock::{
    ClusterStatsLockError, StatsLockRequest, StatsLockResolver,
};
use tidb_stats_handle_types::StatsLockTable;

/// One table target retained from a parsed lock statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatsLockTarget {
    /// Schema name after default-schema resolution.
    pub schema: String,
    /// Table name as written.
    pub table: String,
    /// Named partitions, if this is a partition-only operation.
    pub partitions: Vec<String>,
}

/// One parsed `LOCK STATS` or `UNLOCK STATS` statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterStatsLockStatement {
    /// `true` for LOCK and `false` for UNLOCK.
    pub lock: bool,
    /// Targets in source order.
    pub targets: Vec<StatsLockTarget>,
}

/// Parses a cluster-routed statistics lock statement.
pub fn prepare_cluster_stats_lock(
    sql: &str,
    default_schema: &str,
) -> Result<Option<ClusterStatsLockStatement>, ClusterStatsLockError> {
    let Ok(statement) = tidb_parser::parse(sql) else {
        return Ok(None);
    };
    prepare_cluster_stats_lock_parsed(&statement, default_schema)
}

/// [`prepare_cluster_stats_lock`] over a statement the caller already parsed.
pub fn prepare_cluster_stats_lock_parsed(
    statement: &tidb_ast::Stmt,
    default_schema: &str,
) -> Result<Option<ClusterStatsLockStatement>, ClusterStatsLockError> {
    let tidb_ast::Stmt::Admin(admin) = statement else {
        return Ok(None);
    };
    let (lock, statement) = match admin.as_ref() {
        tidb_ast::AdminStmt::LockStats(statement) => (true, statement.as_ref()),
        tidb_ast::AdminStmt::UnlockStats(statement) => (false, statement.as_ref()),
        _ => return Ok(None),
    };
    let targets = statement
        .tables
        .iter()
        .map(|target| {
            let (schema, table) = match target.name.as_slice() {
                [table] if !default_schema.is_empty() => (default_schema.to_owned(), table.clone()),
                [_table] => {
                    return Err(tidb_plannererrors::ERR_NO_DB
                        .generate_with_stack_by_args(&[])
                        .into())
                }
                [schema, table] => (schema.clone(), table.clone()),
                _ => {
                    return Err(ClusterStatsLockError::Invalid(
                        "empty table name".to_owned(),
                    ))
                }
            };
            Ok(StatsLockTarget {
                schema,
                table,
                partitions: target.partitions.clone(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(ClusterStatsLockStatement { lock, targets }))
}

impl StatsLockResolver for ClusterStatsLockStatement {
    /// Go `LockExec.Next` / `UnlockExec.Next`: `populatePartitionIDAndNames`
    /// for one table named with partitions, `populateTableAndPartitionIDs`
    /// otherwise.
    fn resolve(&self, catalog: &ClusterCatalog) -> Result<StatsLockRequest, ClusterStatsLockError> {
        if self.targets.is_empty() {
            return Err(ClusterStatsLockError::Invalid(if self.lock {
                "Lock Stats: table should not empty".to_owned()
            } else {
                "Unlock Stats: table should not empty ".to_owned()
            }));
        }
        if let [target] = self.targets.as_slice() {
            if !target.partitions.is_empty() {
                return resolve_partitions(catalog, self.lock, target);
            }
        }
        let mut tables = BTreeMap::new();
        for target in &self.targets {
            let table = user_table(catalog, &target.schema, &target.table)?;
            let partition_info = table.get_partition_info().map(|partition| {
                partition
                    .read()
                    .definitions
                    .map_visible(|definition| {
                        (
                            definition.id,
                            format!(
                                "{}.{} partition ({})",
                                target.schema.go_to_lower(),
                                target.table.go_to_lower(),
                                definition.name.lowercase()
                            ),
                        )
                    })
                    .into_iter()
                    .collect()
            });
            tables.insert(
                table.id,
                StatsLockTable {
                    partition_info,
                    full_name: format!(
                        "{}.{}",
                        target.schema.go_to_lower(),
                        target.table.go_to_lower()
                    ),
                },
            );
        }
        Ok(StatsLockRequest::Tables { lock: self.lock, tables })
    }
}

/// Go `populatePartitionIDAndNames`.
fn resolve_partitions(
    catalog: &ClusterCatalog,
    lock: bool,
    target: &StatsLockTarget,
) -> Result<StatsLockRequest, ClusterStatsLockError> {
    let table = user_table(catalog, &target.schema, &target.table)?;
    let Some(partition) = table.get_partition_info() else {
        return Err(ClusterStatsLockError::Invalid(format!(
            "table {}.{} is not a partition table",
            target.schema.go_to_lower(),
            target.table.go_to_lower()
        )));
    };
    let partition = partition.read();
    let mut partitions = BTreeMap::new();
    for written in &target.partitions {
        let id = partition
            .definitions
            .map_visible(|definition| {
                (definition.name.lowercase() == written.go_to_lower()).then_some(definition.id)
            })
            .into_iter()
            .flatten()
            .next()
            .ok_or_else(|| {
                ClusterStatsLockError::from(
                    tidb_table::table_errors::ERR_UNKNOWN_PARTITION.generate_with_stack_by_args(&[
                        written.go_to_lower().into(),
                        table.name.original().into(),
                    ]),
                )
            })?;
        partitions.insert(id, written.go_to_lower());
    }
    let displayed = if lock {
        format!("{}.{}", target.schema.go_to_lower(), target.table.go_to_lower())
    } else {
        format!("{}.{}", target.schema, target.table)
    };
    Ok(StatsLockRequest::Partitions { lock, table_id: table.id, displayed, partitions })
}

/// Go `is.TableByName` over the stored tables.
fn user_table<'a>(
    catalog: &'a ClusterCatalog,
    schema: &str,
    table: &str,
) -> Result<&'a TableInfo, ClusterStatsLockError> {
    catalog.find_table(schema, table).map(|(_, table)| table).ok_or_else(|| {
        ClusterStatsLockError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .generate_with_stack_by_args(&[schema.into(), table.into()]),
        )
    })
}
