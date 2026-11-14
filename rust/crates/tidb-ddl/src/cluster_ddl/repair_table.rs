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

//! `ADMIN REPAIR TABLE` as Go runs it: `executor.RepairTable` builds the
//! table from the statement's `CREATE TABLE`, takes over the IDs of the
//! table `domainutil.RepairInfo` holds and queues `ActionRepairTable`, whose
//! `onRepairTable` writes the table back in one step.

use super::add_index::{action_step, AddIndexSubmission};
use super::*;
use tidb_domainutil::REPAIR_INFO;

/// Go `dbterror.ErrRepairTableFail.GenWithStackByArgs(reason)`.
fn repair_table_fail(reason: String) -> DdlPlanError {
    DdlPlanError::from(tidb_dbterror::ERR_REPAIR_TABLE_FAIL.gen_with_stack_by_args(&[reason.into()]))
}

/// Go `checkAndOverridePartitionID`.
fn check_and_override_partition_id(
    new_table_info: &TableInfo,
    old_table_info: &TableInfo,
) -> Result<(), DdlPlanError> {
    // If any old partitionInfo has lost, that means the partition ID lost
    // too, so did the data, repair failed.
    let Some(new_partition) = new_table_info.partition.as_ref() else {
        return Ok(());
    };
    let Some(old_partition) = old_table_info.partition.as_ref() else {
        return Err(repair_table_fail(
            "Old table doesn't have partitions".to_owned(),
        ));
    };
    let mut new_partition = new_partition.write();
    let old_partition = old_partition.read();
    if new_partition.partition_type != old_partition.partition_type {
        return Err(repair_table_fail(
            "Partition type should be the same".to_owned(),
        ));
    }
    if new_partition.partition_type == tidb_ast::PartitionType::HASH
        && new_partition.num != old_partition.num
    {
        return Err(repair_table_fail(
            "Hash partition num should be the same".to_owned(),
        ));
    }
    let old_definitions = old_partition.definitions.snapshot();
    for index in 0..new_partition.definitions.len() {
        let name = new_partition.definitions.get(index).name.clone();
        let Some(old_one) = old_definitions
            .iter()
            .find(|old_one| old_one.name.lowercase() == name.lowercase())
        else {
            return Err(repair_table_fail(format!(
                "Partition {} has lost",
                name.lowercase()
            )));
        };
        new_partition
            .definitions
            .update(index, |definition| definition.id = old_one.id);
    }
    Ok(())
}

/// Go `getIndexInfoByNameAndColumn` with its `indexColumnSliceEqual`.
fn get_index_info_by_name_and_column(
    old_table_info: &TableInfo,
    new_one: &tidb_model::IndexInfo,
) -> Option<GoShared<tidb_model::IndexInfo>> {
    old_table_info.indices.iter_deref().find(|old_one| {
        let old_one = old_one.read();
        new_one.name.lowercase() == old_one.name.lowercase()
            && new_one.columns.len() == old_one.columns.len()
            && new_one
                .columns
                .iter_deref()
                .zip(old_one.columns.iter_deref())
                .all(|(new_column, old_column)| {
                    new_column.read().name.lowercase() == old_column.read().name.lowercase()
                })
    })
}

/// Go `executor.RepairTable` up to `doDDLJob2`. `None` for any other
/// statement.
pub fn repair_table<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    tiflash_stores: &dyn Fn() -> Result<u64, String>,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let DdlStatement::RepairTable {
        schema,
        table,
        build,
    } = statement
    else {
        return Ok(None);
    };
    repair_table_with_info(
        snapshot,
        schema,
        table,
        build,
        job_context,
        tiflash_stores,
        start_ts,
        upgrading,
    )
}

/// Go `executor.RepairTable`: typed arguments through `doDDLJob2`.
#[allow(clippy::too_many_arguments)]
pub fn repair_table_with_info<S: MetaSnapshot>(
    snapshot: &mut S,
    schema: &str,
    table: &str,
    build: &CreateTableBuild,
    job_context: &dyn DdlSessionContext,
    tiflash_stores: &dyn Fn() -> Result<u64, String>,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    // Existence of DB and table has been checked in the preprocessor, which
    // hands Go's executor both through the session's values.
    let (old_table_info, old_db_info) = REPAIR_INFO.get_repaired_table_info_by_table_name(
        CiString::new(schema.to_owned()).lowercase(),
        CiString::new(table.to_owned()).lowercase(),
    );
    let Some(old_table_info) = old_table_info else {
        return Err(DdlPlanError::from(
            tidb_dbterror::ERR_REPAIR_TABLE_FAIL.gen_with_stack("Failed to get the repaired table"),
        ));
    };
    let Some(old_db_info) = old_db_info else {
        return Err(DdlPlanError::from(
            tidb_dbterror::ERR_REPAIR_TABLE_FAIL
                .gen_with_stack("Failed to get the repaired database"),
        ));
    };
    let old_table_info = old_table_info.read();
    let old_db_info = old_db_info.read();
    // By now only support same DB repair.
    if CiString::new(build.schema.to_owned()).lowercase() != old_db_info.name.lowercase() {
        return Err(DdlPlanError::from(
            tidb_dbterror::ERR_REPAIR_TABLE_FAIL
                .gen_with_stack("Repaired table should in same database with the old one"),
        ));
    }
    // Go `buildTableInfoWithCheck` under the old table's charset, collation
    // and placement policy.
    let mut new_table_info = build
        .for_database(&old_table_info.charset, &old_table_info.collate)
        .map_err(DdlPlanError::Admission)?;
    let written_policy = build
        .create
        .table_options
        .iter()
        .find_map(|option| match option {
            tidb_ast::TableOption::PlacementPolicy(name) => Some(name.clone()),
            _ => None,
        });
    if let Some(name) = written_policy {
        new_table_info.placement_policy_ref =
            Some(tidb_model::GoShared::new(tidb_model::PolicyRefInfo {
                id: 0,
                name: CiString::new(name),
            }));
    } else if new_table_info.temp_table_type == tidb_model::TempTableType::NONE {
        if let Some(reference) = old_table_info.placement_policy_ref.as_ref() {
            new_table_info.placement_policy_ref =
                Some(tidb_model::GoShared::new(reference.read().clone()));
        }
    }
    let mut warnings = Vec::new();
    check_table_info_valid_extra(
        tiflash_stores,
        &build.schema,
        &mut new_table_info,
        &mut warnings,
    )?;
    // Override newTableInfo with oldTableInfo's element necessary.
    new_table_info.id = old_table_info.id;
    check_and_override_partition_id(&new_table_info, &old_table_info)?;
    new_table_info.auto_inc_id = old_table_info.auto_inc_id;
    // If any old columnInfo has lost, that means the old column ID lost too,
    // repair failed.
    for new_one in new_table_info.columns.iter_deref() {
        let mut new_one = new_one.write();
        let Some(old) = old_table_info.find_public_column_by_name(new_one.name.lowercase()) else {
            return Err(repair_table_fail(format!(
                "Column {} has lost",
                new_one.name.lowercase()
            )));
        };
        let old = old.read();
        if new_one.get_type() != old.get_type() {
            return Err(repair_table_fail(format!(
                "Column {} type should be the same",
                new_one.name.lowercase()
            )));
        }
        if new_one.get_flen() != old.get_flen() {
            tidb_ddl_logutil::ddl_logger().warn(
                &format!(
                    "admin repair table : Column {} flen is not equal to the old one",
                    new_one.name.lowercase()
                ),
                &[],
            );
        }
        new_one.id = old.id;
    }
    // If any old indexInfo has lost, that means the index ID lost too, so
    // did the data, repair failed.
    for new_one in new_table_info.indices.iter_deref() {
        let mut new_one = new_one.write();
        let Some(old) = get_index_info_by_name_and_column(&old_table_info, &new_one) else {
            return Err(repair_table_fail(format!(
                "Index {} has lost",
                new_one.name.lowercase()
            )));
        };
        let old = old.read();
        if new_one.tp != old.tp {
            return Err(repair_table_fail(format!(
                "Index {} type should be the same",
                new_one.name.lowercase()
            )));
        }
        new_one.id = old.id;
    }

    new_table_info.state = SchemaState::PUBLIC;
    check_table_info_valid(&new_table_info)?;
    new_table_info.state = SchemaState::NONE;

    let mut job = session_job(job_context, ActionType::ACTION_REPAIR_TABLE);
    job.schema_id = old_db_info.id;
    job.table_id = new_table_info.id;
    job.schema_name = old_db_info.name.lowercase().to_owned().into();
    job.table_name = new_table_info.name.lowercase().to_owned().into();
    // Go's `rewritePartitionQueryString` rewrote `QueryString` before the
    // job copies it.
    job.query = tidb_ddl_build::partition_interval::rewritten_query(
        job_context.ddl_query(),
        build.context.ddl_query_rewrite().as_ref(),
    )
    .into();
    let args = tidb_model::RepairTableArgs {
        table_info: tidb_model::GoField::new(Some(GoShared::new(new_table_info))),
    };
    let built = AddIndexSubmission::Job(
        Box::new(tidb_ddl_jobsubmit::JobSpec {
            job,
            args: JobArgsValue::RepairTable(Some(GoShared::new(args))),
            id_allocated: true,
        }),
        warnings,
    );
    submit_built_job(snapshot, Some(built), start_ts, upgrading)
}

/// Go `onRepairTable`.
pub(super) fn on_repair_table(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let schema_id = active.job.schema_id;
    // Invalid arguments, cancel this job.
    let args = match tidb_model::get_repair_table_args(&mut active.job) {
        Ok(Some(args)) => args,
        Ok(None) => {
            return Err(cancel_ddl_job(
                active,
                DdlPlanError::Encode("repair table job has nil args".to_owned()),
            ))
        }
        Err(error) => {
            return Err(cancel_ddl_job(
                active,
                DdlPlanError::Encode(error.to_string()),
            ))
        }
    };
    let Some(table) = args.read().table_info.get() else {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::Encode("repair table job has a nil table".to_owned()),
        ));
    };
    table.write().state = SchemaState::NONE;

    // Check the old DB and old table exist.
    checked_job_table(txn, active, true)?;

    // When in repair mode, the repaired table in a server is not access to
    // user, the table after repairing will be removed from repair list.
    // Other server left behind alive may need to restart to get the latest
    // schema version.
    let meta = txn.meta();
    let diff = SchemaDiff {
        action_type: active.job.type_,
        schema_id,
        table_id: active.job.table_id,
        ..SchemaDiff::default()
    };
    let (schema_version, diff) = update_schema_version(&meta, active, diff)?;
    // none -> public
    {
        let mut info = table.write();
        info.state = SchemaState::PUBLIC;
        info.update_ts = start_ts;
        // Go `repairTableOrViewWithCheck`.
        if let Err(error) = check_table_info_valid(&info) {
            drop(info);
            return Err(cancel_ddl_job(active, error));
        }
        meta.update_table(schema_id, &mut info)
            .map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))?;
    }
    // Finish this job.
    active.job.finish_table_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        schema_version,
        Some(table),
    );
    let mut write = job_only_write(active.job.id, Vec::new());
    write.schema_version = schema_version;
    write.diff = diff;
    Ok(action_step(write))
}
