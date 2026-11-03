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

//! Go `subscriber.go`: each schema change's statistics statements.

use tidb_ddl_notifier::SchemaChangeEvent;
use tidb_log::{Field, Value};
use tidb_model::partition::{PartitionDefinition, PartitionInfo};
use tidb_model::table_info::TableInfo;
use tidb_model::{ActionType, ColumnInfo};
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::SqlExecError;
use tidb_stats_handle_storage::{read, save, update};
use tidb_stats_handle_util::{exec_rows, get_start_ts, StatsSessionContext};
use tidb_variable::table_delta::TableDelta;

/// Go `util.StatsMetaHistorySourceSchemaChange`.
const SCHEMA_CHANGE_HISTORY_SOURCE: &str = "schema change";

/// Go `schemaNotFound`.
const SCHEMA_NOT_FOUND: &str = "Not Found";

/// Go `subscriber`: what it reads of the statistics handle and the schema.
pub struct Subscriber<'a> {
    /// Go `statsCache.Get(id)` holding an initialized table.
    pub stats_initialized: &'a dyn Fn(i64) -> bool,
    /// Go `infoschema.SchemaByTable(is, table)`'s name, for the logs.
    pub schema_name_of: &'a dyn Fn(&TableInfo) -> Option<String>,
}

impl Subscriber<'_> {
    /// Go `subscriber.handle`.
    pub fn handle<C: StatsSessionContext + ?Sized>(
        &self,
        sctx: &C,
        change: &SchemaChangeEvent,
    ) -> Result<(), SqlExecError> {
        match change.action_type() {
            ActionType::ACTION_CREATE_TABLE => {
                let info = change.get_create_table_info();
                for id in get_physical_ids(sctx, info)? {
                    self.insert_stats_4_physical_id(sctx, info, id)?;
                }
            }
            ActionType::ACTION_TRUNCATE_TABLE => {
                let (new_table_info, dropped_table_info) = change.get_truncate_table_info();
                for id in get_physical_ids(sctx, new_table_info)? {
                    self.insert_stats_4_physical_id(sctx, new_table_info, id)?;
                }
                // Remove the old table stats.
                for id in get_physical_ids(sctx, dropped_table_info)? {
                    self.delayed_delete_stats_4_physical_id(sctx, id)?;
                }
            }
            ActionType::ACTION_DROP_TABLE => {
                let dropped_table_info = change.get_drop_table_info();
                for id in get_physical_ids(sctx, dropped_table_info)? {
                    self.delayed_delete_stats_4_physical_id(sctx, id)?;
                }
            }
            ActionType::ACTION_ADD_COLUMN => {
                let (new_table_info, new_column_info) = change.get_add_column_info();
                for id in get_physical_ids(sctx, new_table_info)? {
                    self.insert_stats_4_col(sctx, new_table_info, id, new_column_info)?;
                }
            }
            ActionType::ACTION_MODIFY_COLUMN => {
                let (new_table_info, modified_column_info, analyzed) =
                    change.get_modify_column_info();
                // tidb_stats_update_during_ddl analyzed it in the DDL.
                if analyzed {
                    return Ok(());
                }
                for id in get_physical_ids(sctx, new_table_info)? {
                    self.insert_stats_4_col(sctx, new_table_info, id, modified_column_info)?;
                }
            }
            ActionType::ACTION_ADD_TABLE_PARTITION => {
                let (global_table_info, added_partition_info) = change.get_add_partition_info();
                for def in definitions(added_partition_info) {
                    self.insert_stats_4_physical_id(sctx, global_table_info, def.id)?;
                }
            }
            ActionType::ACTION_TRUNCATE_TABLE_PARTITION => {
                let (global_table_info, added_part_info, dropped_part_info) =
                    change.get_truncate_partition_info();
                // First, the new partitions' stats meta records.
                for def in definitions(added_part_info) {
                    self.insert_stats_4_physical_id(sctx, global_table_info, def.id)?;
                }
                // Second, the dropped partitions leave the global stats meta.
                self.update_global_table_stats_4_truncate_partition(
                    sctx,
                    global_table_info,
                    dropped_part_info,
                )?;
                // Third, the dropped partitions' own stats meta.
                for def in definitions(dropped_part_info) {
                    self.delayed_delete_stats_4_physical_id(sctx, def.id)?;
                }
            }
            ActionType::ACTION_DROP_TABLE_PARTITION => {
                let (global_table_info, dropped_partition_info) = change.get_drop_partition_info();
                update_global_table_stats_4_drop_partition(
                    sctx,
                    global_table_info,
                    dropped_partition_info,
                )?;
                // Reset the partition stats.
                for def in definitions(dropped_partition_info) {
                    self.delayed_delete_stats_4_physical_id(sctx, def.id)?;
                }
            }
            // A partition exchanged with a system table still adjusts the
            // global statistics by the deltas; Go ignores the rest.
            ActionType::ACTION_EXCHANGE_TABLE_PARTITION => {
                let (global_table_info, original_part_info, original_table_info) =
                    change.exchange_partition_info();
                self.update_global_table_stats_4_exchange_partition(
                    sctx,
                    global_table_info,
                    original_part_info,
                    original_table_info,
                )?;
            }
            ActionType::ACTION_REORGANIZE_PARTITION => {
                let (global_table_info, added_part_info, dropped_partition_info) =
                    change.get_reorganize_partition_info();
                // The data does not change: the global stats stay, and the
                // new partitions wait for auto-analyze.
                for def in definitions(added_part_info) {
                    self.insert_stats_4_physical_id(sctx, global_table_info, def.id)?;
                }
                for def in definitions(dropped_partition_info) {
                    self.delayed_delete_stats_4_physical_id(sctx, def.id)?;
                }
            }
            ActionType::ACTION_ALTER_TABLE_PARTITIONING => {
                let (old_single_table_id, global_table_info, added_part_info) =
                    change.get_add_partitioning_info();
                for def in definitions(added_part_info) {
                    self.insert_stats_4_physical_id(sctx, global_table_info, def.id)?;
                }
                // The global stats follow the new ID: the data has not changed.
                update::change_global_stats_id(sctx, old_single_table_id, global_table_info.id)?;
            }
            ActionType::ACTION_REMOVE_PARTITIONING => {
                let (old_tbl_id, new_single_table_info, dropped_part_info) =
                    change.get_remove_partitioning_info();
                update::change_global_stats_id(sctx, old_tbl_id, new_single_table_info.id)?;
                for def in definitions(dropped_part_info) {
                    self.delayed_delete_stats_4_physical_id(sctx, def.id)?;
                }
            }
            ActionType::ACTION_FLASHBACK_CLUSTER => update::update_stats_version(sctx)?,
            // No stats meta changes for an added index.
            ActionType::ACTION_ADD_INDEX => {}
            ActionType::ACTION_DROP_SCHEMA => {
                for table in &change.get_drop_schema_info().tables {
                    // Partition stats are keyed by partition IDs; each is
                    // re-versioned for stats GC, best effort.
                    for partition in &table.partitions {
                        if let Err(error) = self.delayed_delete_stats_4_physical_id(sctx, partition.id) {
                            tidb_stats_handle_logutil::stats_logger().error(
                                "Failed to update stats meta version for gc",
                                &[
                                    Field::new("partitionID", Value::I64(partition.id)),
                                    Field::new("tableID", Value::I64(table.id)),
                                    zap_error(&error),
                                ],
                            );
                        }
                    }
                    if let Err(error) = self.delayed_delete_stats_4_physical_id(sctx, table.id) {
                        tidb_stats_handle_logutil::stats_logger().error(
                            "Failed to update stats meta version for gc",
                            &[Field::new("tableID", Value::I64(table.id)), zap_error(&error)],
                        );
                    }
                }
            }
            _ => {
                tidb_stats_handle_logutil::stats_logger().error(
                    "Unhandled schema change event",
                    &[Field::new("type", Value::Str(change.to_string()))],
                );
            }
        }
        Ok(())
    }

    /// Go `subscriber.insertStats4PhysicalID`.
    fn insert_stats_4_physical_id<C: StatsSessionContext + ?Sized>(
        &self,
        sctx: &C,
        info: &TableInfo,
        id: i64,
    ) -> Result<(), SqlExecError> {
        let start_ts = save::insert_table_stats_2_kv(sctx, info, id)?;
        self.record_historical_stats_meta(sctx, id, start_ts)
    }

    /// Go `subscriber.recordHistoricalStatsMeta`.
    fn record_historical_stats_meta<C: StatsSessionContext + ?Sized>(
        &self,
        sctx: &C,
        id: i64,
        start_ts: u64,
    ) -> Result<(), SqlExecError> {
        if start_ts == 0 || !get_enable_historical_stats(sctx)? || !(self.stats_initialized)(id) {
            return Ok(());
        }
        tidb_stats_handle_history::history_stats::record_historical_stats_meta(
            sctx,
            start_ts,
            SCHEMA_CHANGE_HISTORY_SOURCE,
            id,
        )
    }

    /// Go `subscriber.delayedDeleteStats4PhysicalID`: the statistics stay
    /// for GC, re-versioned.
    fn delayed_delete_stats_4_physical_id<C: StatsSessionContext + ?Sized>(
        &self,
        sctx: &C,
        id: i64,
    ) -> Result<(), SqlExecError> {
        let start_ts = update::update_stats_meta_ver_and_last_hist_update_ver(sctx, id)?;
        self.record_historical_stats_meta(sctx, id, start_ts)
    }

    /// Go `subscriber.insertStats4Col`.
    fn insert_stats_4_col<C: StatsSessionContext + ?Sized>(
        &self,
        sctx: &C,
        table: &TableInfo,
        physical_id: i64,
        col_infos: &[ColumnInfo],
    ) -> Result<(), SqlExecError> {
        let start_ts =
            save::insert_col_stats_2_kv(sctx, table.name.original(), physical_id, col_infos)?;
        self.record_historical_stats_meta(sctx, physical_id, start_ts)
    }

    /// Go `infoschema.SchemaByTable`'s name, or Go's placeholder.
    fn schema_name(&self, table: &TableInfo) -> String {
        (self.schema_name_of)(table).unwrap_or_else(|| {
            tidb_stats_handle_logutil::stats_sample_logger().info(
                "Schema not found for table, it may have been dropped",
                &[Field::new("tableID", Value::I64(table.id))],
            );
            SCHEMA_NOT_FOUND.to_owned()
        })
    }

    /// Go `updateGlobalTableStats4ExchangePartition`.
    fn update_global_table_stats_4_exchange_partition<C: StatsSessionContext + ?Sized>(
        &self,
        sctx: &C,
        global_table_info: &TableInfo,
        original_part_info: &PartitionInfo,
        original_table_info: &TableInfo,
    ) -> Result<(), SqlExecError> {
        let part_def = definitions(original_part_info)
            .into_iter()
            .next()
            .ok_or_else(|| stats_error("exchange event has no partition definition"))?;
        let (part_count, part_modify_count, _) =
            read::stats_meta_count_and_modify_count(sctx, part_def.id, false)?;
        let (table_count, table_modify_count, _) =
            read::stats_meta_count_and_modify_count(sctx, original_table_info.id, false)?;
        let count_delta = table_count - part_count;
        let modify_count_delta = (table_count + part_count) - part_modify_count + table_modify_count;
        if modify_count_delta == 0 && count_delta == 0 {
            return Ok(());
        }
        let global_table_schema_name = self.schema_name(global_table_info);
        let fields = || {
            vec![
                Field::new("globalTableSchema", Value::Str(global_table_schema_name.clone())),
                Field::new("globalTableID", Value::I64(global_table_info.id)),
                Field::new(
                    "globalTableName",
                    Value::Str(global_table_info.name.original().to_owned()),
                ),
                Field::new("countDelta", Value::I64(count_delta)),
                Field::new("modifyCountDelta", Value::I64(modify_count_delta)),
                Field::new("partitionID", Value::I64(part_def.id)),
                Field::new("partitionName", Value::Str(part_def.name.original().to_owned())),
                Field::new("partitionCount", Value::I64(part_count)),
                Field::new("partitionModifyCount", Value::I64(part_modify_count)),
                Field::new("tableID", Value::I64(original_table_info.id)),
                Field::new(
                    "tableName",
                    Value::Str(original_table_info.name.original().to_owned()),
                ),
                Field::new("tableCount", Value::I64(table_count)),
                Field::new("tableModifyCount", Value::I64(table_modify_count)),
            ]
        };
        if let Err(error) = update_stats_with_count_delta_and_modify_count_delta(
            sctx,
            global_table_info.id,
            count_delta,
            modify_count_delta,
        ) {
            let mut fields = fields();
            fields.push(zap_error(&error));
            tidb_stats_handle_logutil::stats_logger()
                .error("Update global stats after exchange partition failed", &fields);
            return Err(error);
        }
        tidb_stats_handle_logutil::stats_logger()
            .info("Update global stats after exchange partition", &fields());
        Ok(())
    }

    /// Go `updateGlobalTableStats4TruncatePartition`.
    fn update_global_table_stats_4_truncate_partition<C: StatsSessionContext + ?Sized>(
        &self,
        sctx: &C,
        global_table_info: &TableInfo,
        dropped_part_info: &PartitionInfo,
    ) -> Result<(), SqlExecError> {
        let dropped = definitions(dropped_part_info);
        let mut count = 0_i64;
        for def in &dropped {
            count += read::stats_meta_count_and_modify_count(sctx, def.id, false)?.0;
        }
        if count == 0 {
            return Ok(());
        }
        let global_table_schema_name = self.schema_name(global_table_info);
        let is_locked = is_table_locked(sctx, global_table_info.id)?;
        let start_ts = get_start_ts(sctx)?;
        let delta = -count;
        let fields = || {
            vec![
                Field::new("schema", Value::Str(global_table_schema_name.clone())),
                Field::new("tableID", Value::I64(global_table_info.id)),
                Field::new(
                    "tableName",
                    Value::Str(global_table_info.name.original().to_owned()),
                ),
                Field::new(
                    "partitionIDs",
                    Value::Str(format!("{:?}", dropped.iter().map(|def| def.id).collect::<Vec<_>>())),
                ),
                Field::new(
                    "partitionNames",
                    Value::Str(format!(
                        "{:?}",
                        dropped.iter().map(|def| def.name.original().to_owned()).collect::<Vec<_>>()
                    )),
                ),
                Field::new("count", Value::I64(count)),
                Field::new("delta", Value::I64(delta)),
                Field::new("startTS", Value::U64(start_ts)),
                Field::new("isLocked", Value::Bool(is_locked)),
            ]
        };
        if let Err(error) = update::update_stats_meta(
            sctx,
            start_ts,
            &[global_delta(global_table_info.id, count, is_locked)],
        ) {
            let mut fields = fields();
            fields.push(zap_error(&error));
            tidb_stats_handle_logutil::stats_logger()
                .error("Update global stats after truncate partition failed", &fields);
            return Err(error);
        }
        tidb_stats_handle_logutil::stats_logger()
            .info("Update global stats after truncate partition", &fields());
        Ok(())
    }
}

/// Go `updateGlobalTableStats4DropPartition`: the dropped partitions' rows
/// leave the global count.
fn update_global_table_stats_4_drop_partition<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    global_table_info: &TableInfo,
    dropped_partition_info: &PartitionInfo,
) -> Result<(), SqlExecError> {
    let mut count = 0_i64;
    for def in definitions(dropped_partition_info) {
        count += read::stats_meta_count_and_modify_count(sctx, def.id, false)?.0;
    }
    if count == 0 {
        return Ok(());
    }
    let is_locked = is_table_locked(sctx, global_table_info.id)?;
    let start_ts = get_start_ts(sctx)?;
    update::update_stats_meta(
        sctx,
        start_ts,
        &[global_delta(global_table_info.id, count, is_locked)],
    )
}

/// Go `storage.NewDeltaUpdate(id, variable.TableDelta{Count: count, Delta:
/// -count}, isLocked)`.
fn global_delta(table_id: i64, count: i64, is_locked: bool) -> update::DeltaUpdate {
    update::DeltaUpdate {
        table_id,
        delta: TableDelta {
            delta: -count,
            count,
            init_time: None,
        },
        is_locked,
    }
}

/// Whether `lockstats.QueryLockedTables` names the table.
fn is_table_locked<C: StatsSessionContext + ?Sized>(sctx: &C, table_id: i64) -> Result<bool, SqlExecError> {
    Ok(tidb_stats_handle_lockstats::session_stats_lock::query_locked_tables(sctx)?.contains(&table_id))
}

/// Go `updateStatsWithCountDeltaAndModifyCountDelta`: a locked table's
/// deltas are stashed; another's count and modify count move by them,
/// never below zero.
fn update_stats_with_count_delta_and_modify_count_delta<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    table_id: i64,
    count_delta: i64,
    modify_count_delta: i64,
) -> Result<(), SqlExecError> {
    let is_locked = is_table_locked(sctx, table_id)?;
    let start_ts = get_start_ts(sctx)?;
    if is_locked {
        exec_rows(
            sctx,
            "INSERT INTO mysql.stats_table_locked (version, count, modify_count, table_id) VALUES (%?, %?, %?, %?) ON DUPLICATE KEY UPDATE version = VALUES(version), count = count + VALUES(count), modify_count = modify_count + VALUES(modify_count)",
            &[
                SqlArg::Unsigned(start_ts),
                SqlArg::Signed(count_delta),
                SqlArg::Signed(modify_count_delta),
                SqlArg::Signed(table_id),
            ],
        )?;
        return Ok(());
    }
    let (count, modify_count, is_null) =
        read::stats_meta_count_and_modify_count(sctx, table_id, true)?;
    if is_null {
        exec_rows(
            sctx,
            "INSERT INTO mysql.stats_meta (version, count, modify_count, table_id) VALUES (%?, GREATEST(0, %?), GREATEST(0, %?), %?)",
            &[
                SqlArg::Unsigned(start_ts),
                SqlArg::Signed(count_delta),
                SqlArg::Signed(modify_count_delta),
                SqlArg::Signed(table_id),
            ],
        )?;
    } else {
        exec_rows(
            sctx,
            "UPDATE mysql.stats_meta SET version = %?, count = GREATEST(0, %?), modify_count = GREATEST(0, %?) WHERE table_id = %?",
            &[
                SqlArg::Unsigned(start_ts),
                SqlArg::Signed(count + count_delta),
                SqlArg::Signed(modify_count + modify_count_delta),
                SqlArg::Signed(table_id),
            ],
        )?;
    }
    Ok(())
}

/// Go `getPhysicalIDs`: a partitioned table's partitions, and the table
/// itself under dynamic pruning.
fn get_physical_ids<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    tbl_info: &TableInfo,
) -> Result<Vec<i64>, SqlExecError> {
    let Some(pi) = tbl_info.get_partition_info() else {
        return Ok(vec![tbl_info.id]);
    };
    let mut ids = definitions(&pi.read()).into_iter().map(|def| def.id).collect::<Vec<_>>();
    if get_current_prune_mode(sctx)? == "dynamic" {
        ids.push(tbl_info.id);
    }
    Ok(ids)
}

/// Go `getCurrentPruneMode`.
fn get_current_prune_mode<C: StatsSessionContext + ?Sized>(sctx: &C) -> Result<String, SqlExecError> {
    sctx.global_system_var(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
}

/// Go `getEnableHistoricalStats`.
fn get_enable_historical_stats<C: StatsSessionContext + ?Sized>(sctx: &C) -> Result<bool, SqlExecError> {
    Ok(tidb_vardef::option_values::tidb_opt_on(
        &sctx.global_system_var(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)?,
    ))
}

fn definitions(info: &PartitionInfo) -> Vec<PartitionDefinition> {
    info.definitions.snapshot()
}

fn stats_error(message: &str) -> SqlExecError {
    Box::new(std::io::Error::other(message.to_owned()))
}

fn zap_error(error: &SqlExecError) -> Field {
    tidb_logutil::zap_error(error)
}
