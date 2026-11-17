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

//! DROP TABLE / VIEW / SEQUENCE and DROP DATABASE as Go submits them
//! (`executor.dropTableObject`, `executor.DropSchema`): one persisted job per
//! object, which the owner steps through `onDropTableOrView` / `onDropSchema`
//! -- write-only, delete-only, gone -- leaving the rows to the delete range.

use super::add_index::AddIndexSubmission;
use super::*;

fn job_for(
    job_context: &dyn DdlSessionContext,
    action: ActionType,
    schema_id: i64,
    schema_name: &str,
) -> Job {
    let mut job = session_job(job_context, action);
    job.schema_id = schema_id;
    job.schema_name = schema_name.go_to_lower().into();
    job
}

fn submission<S: MetaSnapshot>(
    snapshot: &mut S,
    job: Job,
    args: JobArgsValue,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let mut specs = [tidb_ddl_jobsubmit::JobSpec {
        job,
        args,
        id_allocated: true,
    }];
    tidb_ddl_jobsubmit::prepare_submit_batch(
        snapshot, &mut specs, start_ts, upgrading,
    )?;
    let [spec] = specs;
    Ok(Some(AddIndexSubmission::Job(Box::new(spec), Vec::new())))
}

/// Go `executor.dropTableObject` for DropTable.
#[allow(clippy::too_many_arguments)]
pub fn drop_table<S: MetaSnapshot>(
    snapshot: &mut S,
    schema: &str,
    table: &str,
    if_exists: bool,
    job_context: &dyn DdlSessionContext,
    targets: &[(String, String)],
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_info_schema(snapshot)?;

    let identifiers: Vec<(String, String)> = if targets.is_empty() {
        vec![(schema.to_owned(), table.to_owned())]
    } else {
        targets.to_vec()
    };
    // Go `dropTableObject`: before any table goes, no table the
    // statement names may be cited by a key from a table it does not
    // name.
    for (target_schema, target_table) in &identifiers {
        if let Some(referred) = crate::foreign_key_build::check_table_has_foreign_key_referred(
            &catalog,
            target_schema,
            target_table,
            &identifiers,
            job_context.foreign_key_checks(),
        ) {
            return Err(err_foreign_key_cannot_drop_parent(target_table, &referred));
        }
    }
    let missing = || -> Result<Option<AddIndexSubmission>, DdlPlanError> {
        if if_exists {
            return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                detail: format!("table `{schema}`.`{table}` does not exist"),
                warnings: vec![missing_table_note(schema, table)],
            }));
        }
        Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_DROP_EXISTS
                .gen_with_stack_by_args(&[format!("{schema}.{table}").into()]),
        ))
    };
    let Some(database) = find_database(&catalog, schema) else {
        return missing();
    };
    let Some(stored) = find_table(&catalog, &database.read(), table) else {
        return missing();
    };
    let stored = stored.read();
    // Go `dropTableObject` asks the mode of every object it finds,
    // before it tells a table from a view or a sequence.
    super::rename_table::check_table_mode_is_normal(&stored)?;
    // Go master `checkTableMaterializedViewConstraints`, for a base table
    // before its cache status.
    if stored.is_base_table() {
        super::materialized_view::check_table_materialized_view_constraints(&stored, "DROP TABLE")
            .map_err(DdlPlanError::from_mysql_error)?;
    }
    if !check_cluster_drop_object(schema, table, &stored, false)? {
        return missing();
    }
    let db_id = database.read().id;
    drop_table_like(
        snapshot,
        job_context,
        ActionType::ACTION_DROP_TABLE,
        db_id,
        schema,
        &stored,
        &identifiers,
        (start_ts, upgrading),
    )
}

/// Go master `executor.dropTableObject` for a materialized view log
/// (`materializedViewLogObject`, `action` DROP MATERIALIZED VIEW LOG) or a
/// materialized view (`materializedViewObject`, DROP MATERIALIZED VIEW): its
/// job.
#[allow(clippy::too_many_arguments)]
pub fn drop_materialized_view_object<S: MetaSnapshot>(
    snapshot: &mut S,
    action: ActionType,
    schema: &str,
    mlog: &str,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_info_schema(snapshot)?;
    let missing = || {
        Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_DROP_EXISTS
                .gen_with_stack_by_args(&[format!("{schema}.{mlog}").into()]),
        ))
    };
    let Some(database) = find_database(&catalog, schema) else {
        return missing();
    };
    let Some(stored) = find_table(&catalog, &database.read(), mlog) else {
        return missing();
    };
    let stored = stored.read();
    super::rename_table::check_table_mode_is_normal(&stored)?;
    if !check_cluster_drop_object(schema, mlog, &stored, false)? {
        return missing();
    }
    let db_id = database.read().id;
    drop_table_like(
        snapshot,
        job_context,
        action,
        db_id,
        schema,
        &stored,
        &[(schema.to_owned(), mlog.to_owned())],
        (start_ts, upgrading),
    )
}

/// Go `executor.dropTableObject` for DropView.
#[allow(clippy::too_many_arguments)]
pub fn drop_view<S: MetaSnapshot>(
    snapshot: &mut S,
    schema: &str,
    name: &str,
    if_exists: bool,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_info_schema(snapshot)?;
    let stored = find_database(&catalog, schema)
        .and_then(|db| find_table(&catalog, &db.read(), name).map(|table| (db.read().id, table)));
    let Some((db_id, table)) = stored else {
        if if_exists {
            return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                detail: format!("view `{schema}`.`{name}` does not exist"),
                warnings: vec![missing_table_note(schema, name)],
            }));
        }
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_DROP_EXISTS
                .gen_with_stack_by_args(&[format!("{schema}.{name}").into()]),
        ));
    };
    let table = table.read();
    check_cluster_drop_object(schema, name, &table, true)?;
    drop_table_like(
        snapshot,
        job_context,
        ActionType::ACTION_DROP_VIEW,
        db_id,
        schema,
        &table,
        &[(schema.to_owned(), name.to_owned())],
        (start_ts, upgrading),
    )
}

/// Go `executor.dropTableObject` for DropSequence.
#[allow(clippy::too_many_arguments)]
pub fn drop_sequence<S: MetaSnapshot>(
    snapshot: &mut S,
    schema: &str,
    name: &str,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_info_schema(snapshot)?;

    let found = find_database(&catalog, schema).and_then(|database| {
        find_table(&catalog, &database.read(), name).map(|table| (database.read().id, table))
    });
    let Some((db_id, stored)) = found else {
        return Err(coded_refusal(
            &tidb_dbterror::ERR_UNKNOWN_SEQUENCE,
            format!("Unknown SEQUENCE: '{schema}.{name}'"),
        ));
    };
    let stored = stored.read();
    if !stored.is_sequence() {
        return Err(coded_refusal(
            &tidb_dbterror::ERR_WRONG_OBJECT,
            format!("'{schema}.{name}' is not SEQUENCE"),
        ));
    }
    drop_table_like(
        snapshot,
        job_context,
        ActionType::ACTION_DROP_SEQUENCE,
        db_id,
        schema,
        &stored,
        &[(schema.to_owned(), name.to_owned())],
        (start_ts, upgrading),
    )
}

/// Go `executor.DropSchema` for DropDatabase.
#[allow(clippy::too_many_arguments)]
pub fn drop_schema<S: MetaSnapshot>(
    snapshot: &mut S,
    name: &str,
    if_exists: bool,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_info_schema(snapshot)?;

    let Some(database) = find_database(&catalog, name) else {
        if if_exists {
            return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                detail: format!("database `{name}` does not exist"),
                warnings: Vec::new(),
            }));
        }
        // Go `executor.DropSchema`: `ErrDatabaseDropExists` (1008).
        return Err(DdlPlanError::Admission(DdlAdmissionError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_DATABASE_DROP_EXISTS
                .gen_with_stack_by_args(&[name.into()]),
        )));
    };
    // Go `DropSchema`: `checkDatabaseHasForeignKeyReferred`.
    check_database_has_foreign_key_referred(
        &catalog,
        &database.read(),
        job_context.foreign_key_checks(),
    )?;
    let mut job = job_for(
        job_context,
        ActionType::ACTION_DROP_SCHEMA,
        database.read().id,
        database.read().name.original(),
    );
    job.schema_state = database.read().state;
    job.involving_schema_info = GoSharedSlice::from_vec(vec![tidb_model::InvolvingSchemaInfo {
        database: database.read().name.lowercase().to_owned().into(),
        table: tidb_model::job::INVOLVING_ALL.into(),
        ..tidb_model::InvolvingSchemaInfo::default()
    }]);
    let args = GoShared::new(tidb_model::DropSchemaArgs {
        fk_check: GoField::new(job_context.foreign_key_checks()),
        ..tidb_model::DropSchemaArgs::default()
    });
    submission(
        snapshot,
        job,
        JobArgsValue::DropSchema(Some(args)),
        start_ts,
        upgrading,
    )
}

/// The job of one `dropTableObject` target.
#[allow(clippy::too_many_arguments)]
fn drop_table_like<S: MetaSnapshot>(
    snapshot: &mut S,
    job_context: &dyn DdlSessionContext,
    action: ActionType,
    schema_id: i64,
    schema: &str,
    table: &TableInfo,
    identifiers: &[(String, String)],
    (start_ts, upgrading): (u64, bool),
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    // Go `dropTableObject`: only a table object reads `ForeignKeyChecks`.
    let fk_check = action != ActionType::ACTION_DROP_MATERIALIZED_VIEW_LOG && job_context.foreign_key_checks();
    let mut job = job_for(job_context, action, schema_id, schema);
    job.table_id = table.id;
    job.table_name = table.name.lowercase().to_owned().into();
    job.schema_state = table.state;
    let args = GoShared::new(tidb_model::DropTableArgs {
        identifiers: identifiers
            .iter()
            .map(|(schema, name)| tidb_model::Ident {
                schema: CiString::new(schema.clone()),
                name: CiString::new(name.clone()),
            })
            .collect(),
        fk_check,
        ..tidb_model::DropTableArgs::default()
    });
    submission(
        snapshot,
        job,
        JobArgsValue::DropTable(Some(args)),
        start_ts,
        upgrading,
    )
}

/// Go `ErrForeignKeyCannotDropParent` (3730): `table` (as the statement or
/// the schema names it) is cited by `referred`.
pub(super) fn err_foreign_key_cannot_drop_parent(
    table: &str,
    referred: &tidb_model::table::ReferredFKInfo,
) -> DdlPlanError {
    coded_refusal(
        &tidb_dbterror::ERR_FOREIGN_KEY_CANNOT_DROP_PARENT,
        format!(
            "Cannot drop table '{table}' referenced by a foreign key constraint '{}' on table '{}'.",
            referred.child_fk_name.original(),
            referred.child_table.original()
        ),
    )
}

/// Go `checkDatabaseHasForeignKeyReferred`: no table of `database` is cited
/// by a key from outside it. Off when `fk_check` is.
pub(super) fn check_database_has_foreign_key_referred(
    catalog: &SessionExtendedInfoSchema,
    database: &tidb_model::DBInfo,
    fk_check: bool,
) -> Result<(), DdlPlanError> {
    if !fk_check {
        return Ok(());
    }
    let schema_name = database.name.original().to_owned();
    // Go `is.SchemaSimpleTableInfos`: names only, no table loads under V2.
    let tables: Vec<(String, String)> = catalog
        .schema_simple_table_infos(&schema_name)
        .into_iter()
        .map(|table| (schema_name.clone(), table.table_name.original().to_owned()))
        .collect();
    for (_, table_name) in &tables {
        if let Some(referred) = crate::foreign_key_build::check_table_has_foreign_key_referred(
            catalog,
            &schema_name,
            table_name,
            &tables,
            true,
        ) {
            return Err(err_foreign_key_cannot_drop_parent(table_name, &referred));
        }
    }
    Ok(())
}
