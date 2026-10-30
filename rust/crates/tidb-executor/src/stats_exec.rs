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

//! The statistics statements Go's executors run (`SimpleExec`'s
//! `FLUSH STATS_DELTA` and `DROP STATS`, `LockStatsExec`, `LoadStatsExec`):
//! the name resolution, privileges, warnings and the dump's parse are the
//! executor's, and a [`StatsStatementService`] writes `mysql.stats_*`, as
//! Go's `domain.StatsHandle()` does.

use std::sync::Arc;

use crate::analyze::exec::AnalyzeExec;
use tidb_stats_handle_storage::json::parse_stats_json;
use tidb_stats_util::JsonTable;
use crate::{DriverError, KvTable, TableEntry};
use tidb_hack::go_to_lower;

/// The physical tables a `FLUSH STATS_DELTA` dumps.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FlushStatsDeltaTargets {
    /// `FLUSH STATS_DELTA *.*`: every table.
    All,
    /// The named tables and databases, resolved to physical ids. Empty when
    /// none of them exists, which Go answers with a no-op.
    Tables(Vec<i64>),
}

/// Writes the statistics storage the statistics statements change.
pub trait StatsStatementService {
    /// Go `DumpStatsDeltaToKV` for `target_ids`; empty means every table.
    fn flush_stats_delta(&self, target_ids: &[i64]) -> Result<(), DriverError>;
    /// Go `DeleteTableStatsFromKV(ids, soft = true)` and the cache refresh
    /// after it.
    fn drop_stats(&self, physical_ids: &[i64]) -> Result<(), DriverError>;
    /// One `LOCK`/`UNLOCK STATS` internal transaction; returns the warning
    /// Go appends (empty for none).
    fn lock_stats(
        &self,
        statement: &tidb_executor_lockstats::ClusterStatsLockStatement,
    ) -> Result<String, DriverError>;
    /// Go `LoadStatsFromJSON` for one parsed, non-null dump.
    fn load_stats(
        &self,
        json: &JsonTable,
        historical_stats_enabled: bool,
    ) -> Result<(), DriverError>;
}

/// Go's statistics-statement executors over the session.
pub trait StatsExec: AnalyzeExec {
    /// Go `executeFlushStatsDeltaOnCurrentInstance`'s target resolution: a
    /// missing database warns 1049 and a missing table 1146, each skipped.
    /// `None` is a statement that is not `FLUSH STATS_DELTA`.
    fn flush_stats_delta_targets(
        &mut self,
        stmt: &tidb_ast::Stmt,
    ) -> Option<FlushStatsDeltaTargets> {
        let tidb_ast::Stmt::Admin(admin) = stmt else {
            return None;
        };
        let tidb_ast::AdminStmt::Flush(flush) = admin.as_ref() else {
            return None;
        };
        let tidb_ast::FlushTarget::StatsDelta { objects, .. } = &flush.target else {
            return None;
        };
        Some(self.flush_stats_delta_objects(objects))
    }

    /// Resolves `FLUSH STATS_DELTA`'s objects to physical ids, warning for
    /// each one that does not exist.
    fn flush_stats_delta_objects(
        &mut self,
        objects: &[tidb_ast::StatsObject],
    ) -> FlushStatsDeltaTargets {
        if objects
            .iter()
            .any(|object| matches!(object, tidb_ast::StatsObject::Global))
        {
            return FlushStatsDeltaTargets::All;
        }
        let current_database = self.session_vars().current_db.clone();
        let mut ids = Vec::new();
        let mut warnings = Vec::new();
        {
            let catalog = self.catalog_ignoring_poison();
            // Go `appendStatsDeltaTargetTableIDs`: the table, then its
            // partitions.
            let append_table = |database: &str,
                                table: &str,
                                ids: &mut Vec<i64>,
                                warnings: &mut Vec<(u16, String)>| {
                let Some(TableEntry::Kv(table)) = catalog.table_in(database, table) else {
                    let error = tidb_terror::MysqlError::from_terror(
                        &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                            .generate_with_stack_by_args(&[database.into(), table.into()]),
                    );
                    warnings.push((error.code, error.message));
                    return;
                };
                ids.push(table.table_id());
                if let Some(partition) = table.partition() {
                    ids.extend(partition.definitions.iter().map(|definition| definition.id));
                }
            };
            for object in objects {
                match object {
                    tidb_ast::StatsObject::Global => unreachable!("handled above"),
                    tidb_ast::StatsObject::Database(database) => {
                        if let Some(tables) = catalog.table_names(database) {
                            for table in tables {
                                append_table(database, &table, &mut ids, &mut warnings);
                            }
                        } else {
                            let error = tidb_terror::MysqlError::from_terror(
                                &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                                    .generate_with_stack_by_args(&[database.as_str().into()]),
                            );
                            warnings.push((error.code, error.message));
                        }
                    }
                    tidb_ast::StatsObject::Table { database, table } => {
                        let database = database.as_deref().unwrap_or(&current_database);
                        append_table(database, table, &mut ids, &mut warnings);
                    }
                }
            }
        }
        ids.sort_unstable();
        ids.dedup();
        for (code, message) in warnings {
            self.append_routed_warning(code, message);
        }
        FlushStatsDeltaTargets::Tables(ids)
    }

    /// Runs `FLUSH STATS_DELTA` through `service`, after publishing this
    /// session's own pending deltas.
    fn run_flush_stats_delta(
        &mut self,
        targets: &FlushStatsDeltaTargets,
        service: &dyn StatsStatementService,
    ) -> Result<(), DriverError> {
        // Go: "If all specified databases or tables do not exist, we do
        // nothing."
        if matches!(targets, FlushStatsDeltaTargets::Tables(ids) if ids.is_empty()) {
            return Ok(());
        }
        self.publish_table_delta();
        match targets {
            FlushStatsDeltaTargets::All => service.flush_stats_delta(&[]),
            FlushStatsDeltaTargets::Tables(ids) => service.flush_stats_delta(ids),
        }
    }

    /// Go `SimpleExec.executeDropStats`' id resolution. `None` is a statement
    /// that is not DROP STATS.
    fn drop_stats_ids(&mut self, stmt: &tidb_ast::Stmt) -> Result<Option<Vec<i64>>, DriverError> {
        let tidb_ast::Stmt::Admin(admin) = stmt else {
            return Ok(None);
        };
        let tidb_ast::AdminStmt::DropStats(drop) = admin.as_ref() else {
            return Ok(None);
        };
        self.resolve_drop_stats(drop).map(|(_, ids)| Some(ids))
    }

    /// The tables a DROP STATS names and the statistics ids it drops.
    ///
    /// Without a partition clause every named table drops its partitions and
    /// itself; `GLOBAL` drops the first table's own id; a partition list
    /// drops the first table's named partitions (`GetPhysicalIDsAndPartitionNames`).
    /// Each id is planned once: the soft delete is idempotent per id.
    fn resolve_drop_stats(
        &mut self,
        stmt: &tidb_ast::DropStatsStmt,
    ) -> Result<(Vec<Arc<KvTable>>, Vec<i64>), DriverError> {
        let mut tables = Vec::with_capacity(stmt.tables.len());
        for path in &stmt.tables {
            let (database, name) = match path.as_slice() {
                [name] => (self.session_vars().current_db.clone(), name.clone()),
                [database, name] => (database.clone(), name.clone()),
                _ => {
                    return Err(DriverError::unsupported(
                        "DROP STATS of a malformed table name",
                    ))
                }
            };
            if database.is_empty() {
                return Err(DriverError::from(
                    tidb_plannererrors::ERR_NO_DB.generate_with_stack_by_args(&[]),
                ));
            }
            let table =
                self.with_catalog_mut(|catalog| match catalog.table_in(&database, &name) {
                    Some(TableEntry::Kv(table)) => Ok(Arc::clone(table)),
                    _ => Err(DriverError::from(
                        tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                            .generate_with_stack_by_args(&[(&database).into(), (&name).into()]),
                    )),
                })?;
            tables.push(table);
        }
        let mut ids = Vec::new();
        if stmt.global {
            ids.push(tables[0].table_id());
        } else if stmt.partitions.is_empty() {
            for table in &tables {
                ids.extend(get_physical_ids_and_partition_names(table, &[])?);
                ids.push(table.table_id());
            }
        } else {
            ids = get_physical_ids_and_partition_names(&tables[0], &stmt.partitions)?;
        }
        let mut seen = std::collections::BTreeSet::new();
        ids.retain(|id| seen.insert(*id));
        Ok((tables, ids))
    }

    /// Runs `DROP STATS` through `service`. Go's statistics handle commits
    /// its own transaction, so the user's is not moved.
    fn run_drop_stats(
        &mut self,
        physical_ids: &[i64],
        service: &dyn StatsStatementService,
    ) -> Result<(), DriverError> {
        service.drop_stats(physical_ids)
    }

    /// Runs one `LOCK`/`UNLOCK STATS` through `service`.
    ///
    /// Unlike DDL and ANALYZE, Go does not move the user's transaction: the
    /// stats handle borrows a separate restricted session and commits only
    /// its `mysql.stats_*` changes. `Compile` checked every target's
    /// privileges before any lock row changes.
    fn run_stats_lock(
        &mut self,
        statement: &tidb_executor_lockstats::ClusterStatsLockStatement,
        service: &dyn StatsStatementService,
    ) -> Result<(), DriverError> {
        self.begin_routed_statement_warnings();
        let warning = service.lock_stats(statement)?;
        if !warning.is_empty() {
            self.append_routed_warning(1105, warning);
        }
        Ok(())
    }

    /// Go `LoadStatsInfo.Update` over the bytes the client transferred.
    /// Go's JSON decoder replaces invalid UTF-8 rather than refusing it, and
    /// a dump of `null` is a successful no-op.
    fn run_load_stats(
        &mut self,
        data: &[u8],
        service: &dyn StatsStatementService,
    ) -> Result<(), DriverError> {
        let Some(json) = parse_load_stats_dump(data)? else {
            return Ok(());
        };
        service.load_stats(&json, self.historical_stats_enabled())
    }
}

impl<T: AnalyzeExec> StatsExec for T {}

/// The dump `LOAD STATS` transferred, `None` when there is nothing to load
/// (an empty transfer or JSON `null`).
pub fn parse_load_stats_dump(data: &[u8]) -> Result<Option<JsonTable>, DriverError> {
    if data.is_empty() {
        return Ok(None);
    }
    let json = parse_stats_json(&String::from_utf8_lossy(data))
        .map_err(|error| DriverError::unsupported(error.to_string()))?;
    if json.table_name.is_empty() && json.version == 0 {
        return Ok(None);
    }
    Ok(Some(json))
}

/// Go `GetPhysicalIDsAndPartitionNames`' ids: a partition name matches by
/// its lower-case form (`def.Name.L == name.L`).
pub fn get_physical_ids_and_partition_names(
    table: &KvTable,
    partition_names: &[String],
) -> Result<Vec<i64>, DriverError> {
    let Some(partition) = table.partition() else {
        if !partition_names.is_empty() {
            return Err(DriverError::from(
                tidb_dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED
                    .generate_with_stack_by_args(&[]),
            ));
        }
        return Ok(vec![table.table_id()]);
    };
    if partition_names.is_empty() {
        return Ok(partition
            .definitions
            .iter()
            .map(|definition| definition.id)
            .collect());
    }
    partition_names
        .iter()
        .map(|name| {
            let name_lower = go_to_lower(name);
            partition
                .definitions
                .iter()
                .find(|definition| go_to_lower(&definition.name) == name_lower)
                .map(|definition| definition.id)
                .ok_or_else(|| {
                    DriverError::unsupported(format!(
                        "can not found the specified partition name {name} in the table definition"
                    ))
                })
        })
        .collect()
}
