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

//! Go `pkg/ddl/rollingback.go`'s cleanup of the PD affinity groups a
//! cancelled job may have created.

use super::*;

/// Go `deleteAffinityGroupsOfCancelledJob`: deletes the affinity groups a
/// cancelled job may have created in PD. Rolling back the DDL transaction
/// does not undo PD changes, so groups an earlier attempt created, or the
/// current attempt created before PD answered an error, remain after the
/// cancellation. It reads the metadata the cancellation persists, deletes
/// only groups that metadata does not use, and is best effort.
pub(super) fn delete_affinity_groups_of_cancelled_job(txn: &mut JobTxn<'_>, job: &mut Job) {
    let result = match job.type_ {
        ActionType::ACTION_CREATE_TABLE | ActionType::ACTION_CREATE_MATERIALIZED_VIEW_SHADOW => {
            match tidb_model::get_create_table_args(job) {
                Ok(args) => {
                    let table = args.and_then(|args| args.read().table_info.get());
                    delete_affinity_groups_of_tables_not_created(txn, job, &[table])
                }
                Err(error) => Err(error.to_string()),
            }
        }
        ActionType::ACTION_CREATE_TABLES => match tidb_model::get_batch_create_table_args(job) {
            Ok(args) => {
                let tables = args
                    .map(|args| args.read().tables.get().handles())
                    .unwrap_or_default()
                    .into_iter()
                    .map(|table_args| table_args.and_then(|table_args| table_args.read().table_info.get()))
                    .collect::<Vec<_>>();
                delete_affinity_groups_of_tables_not_created(txn, job, &tables)
            }
            Err(error) => Err(error.to_string()),
        },
        ActionType::ACTION_ALTER_TABLE_AFFINITY => delete_alter_table_affinity_new_groups(txn, job),
        ActionType::ACTION_TRUNCATE_TABLE => delete_truncate_table_replacement_affinity_groups(txn, job),
        ActionType::ACTION_TRUNCATE_TABLE_PARTITION => {
            delete_truncate_partition_replacement_affinity_groups(txn, job)
        }
        _ => return,
    };
    if let Err(error) = result {
        tidb_ddl_logutil::ddl_logger().warn(
            "failed to delete affinity groups of the cancelled job",
            &[
                tidb_log::Field::new("jobID", tidb_log::Value::I64(job.id)),
                tidb_log::Field::new("type", tidb_log::Value::Str(job.type_.to_string())),
                tidb_log::Field::new("error", tidb_log::Value::Str(error)),
            ],
        );
    }
}

/// Go `getTableOfCancelledJob`: the job's table as the cancellation persists
/// it, or `None` when it does not exist.
fn get_table_of_cancelled_job(txn: &mut JobTxn<'_>, job: &Job) -> Result<Option<TableInfo>, String> {
    let meta = txn.meta();
    match meta.table(job.schema_id, job.table_id) {
        Ok(table) => Ok(table),
        Err(tidb_meta::MetaError::DatabaseNotExists) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

/// Go `deleteAffinityGroupsOfTablesNotCreated`: the groups of the tables a
/// cancelled CREATE TABLE job leaves out of the metadata.
fn delete_affinity_groups_of_tables_not_created(
    txn: &mut JobTxn<'_>,
    job: &Job,
    tables: &[Option<GoShared<TableInfo>>],
) -> Result<(), String> {
    let mut ids = Vec::new();
    for table in tables.iter().flatten() {
        let table = table.read().clone_like_go();
        if table.affinity.is_none() {
            continue;
        }
        // CREATE TABLE with foreign keys persists the table before its last
        // step, so a cancelled job can keep the table and its groups.
        let meta = txn.meta();
        let exists = match meta.table(job.schema_id, table.id) {
            Ok(found) => found.is_some(),
            Err(tidb_meta::MetaError::DatabaseNotExists) => false,
            Err(error) => return Err(error.to_string()),
        };
        if exists {
            continue;
        }
        ids.extend(crate::ddl_affinity::build_affinity_group_definitions(&table, None)?.into_keys());
    }
    if ids.is_empty() {
        return Ok(());
    }
    tidb_ddl_logutil::ddl_logger().info(
        "deleting affinity groups of tables the cancelled job did not create",
        &[
            tidb_log::Field::new("jobID", tidb_log::Value::I64(job.id)),
            tidb_log::Field::new("groupIDs", tidb_log::Value::Str(format!("{ids:?}"))),
        ],
    );
    tidb_domain_affinity::delete_groups_with_retry(&ids).map_err(|error| error.to_string())
}

/// Go `deleteAlterTableAffinityNewGroups`: the groups of the new level of a
/// cancelled ALTER TABLE ... AFFINITY job. The groups of an unchanged level
/// are the table's current groups and stay.
fn delete_alter_table_affinity_new_groups(txn: &mut JobTxn<'_>, job: &mut Job) -> Result<(), String> {
    let Some(new_affinity) = tidb_model::get_alter_table_affinity_args(job)
        .map_err(|error| error.to_string())?
        .and_then(|args| args.read().affinity.clone())
    else {
        return Ok(());
    };
    let Some(table) = get_table_of_cancelled_job(txn, job)? else {
        return Ok(());
    };
    if table
        .affinity
        .as_ref()
        .is_some_and(|affinity| affinity.read().level == new_affinity.read().level)
    {
        return Ok(());
    }
    let new_table = TableInfo {
        id: table.id,
        name: table.name.clone(),
        affinity: Some(new_affinity),
        partition: table.partition.clone(),
        ..TableInfo::default()
    };
    crate::ddl_affinity::delete_table_affinity_groups_with_timeout(&new_table, None)
}

/// Go `deleteTruncateTableReplacementAffinityGroups`: the groups of the
/// replacement table of a cancelled TRUNCATE TABLE job, whose ids only the
/// job arguments keep.
fn delete_truncate_table_replacement_affinity_groups(
    txn: &mut JobTxn<'_>,
    job: &mut Job,
) -> Result<(), String> {
    // The original table is still in the metadata, so the replacement ids
    // are unused.
    let Some(table) = get_table_of_cancelled_job(txn, job)? else {
        return Ok(());
    };
    if table.affinity.is_none() {
        return Ok(());
    }
    let Some(args) = tidb_model::get_truncate_table_args(job).map_err(|error| error.to_string())? else {
        return Ok(());
    };
    let (new_table_id, new_partition_ids) = {
        let args = args.read();
        (args.new_table_id.get(), args.new_partition_ids.get().snapshot())
    };
    let replacement = TableInfo {
        id: new_table_id,
        name: table.name.clone(),
        affinity: table.affinity.clone(),
        ..TableInfo::default()
    };
    crate::ddl_affinity::delete_table_affinity_groups_with_timeout(
        &replacement,
        Some(&crate::ddl_affinity::partition_definitions_from_ids(&new_partition_ids)),
    )
}

/// Go `deleteTruncatePartitionReplacementAffinityGroups`: the groups of the
/// replacement partitions of a cancelled TRUNCATE PARTITION job that the
/// metadata does not use.
fn delete_truncate_partition_replacement_affinity_groups(
    txn: &mut JobTxn<'_>,
    job: &mut Job,
) -> Result<(), String> {
    let Some(table) = get_table_of_cancelled_job(txn, job)? else {
        return Ok(());
    };
    if !table
        .affinity
        .as_ref()
        .is_some_and(|affinity| affinity.read().level == "partition")
    {
        return Ok(());
    }
    let Some(args) = tidb_model::get_truncate_table_args(job).map_err(|error| error.to_string())? else {
        return Ok(());
    };
    let new_partition_ids = args.read().new_partition_ids.get().snapshot();
    if new_partition_ids.is_empty() {
        return Ok(());
    }
    // A previous step may already have committed some replacement partitions.
    let committed: std::collections::HashSet<i64> = table
        .get_partition_info()
        .map(|partition| {
            partition
                .read()
                .definitions
                .snapshot()
                .iter()
                .map(|definition| definition.id)
                .collect()
        })
        .unwrap_or_default();
    let unused: Vec<i64> = new_partition_ids
        .into_iter()
        .filter(|id| !committed.contains(id))
        .collect();
    if unused.is_empty() {
        return Ok(());
    }
    crate::ddl_affinity::delete_table_affinity_groups_with_timeout(
        &table,
        Some(&crate::ddl_affinity::partition_definitions_from_ids(&unused)),
    )
}
