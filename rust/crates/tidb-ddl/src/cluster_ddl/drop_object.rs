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
    catalog: &ClusterCatalog,
    job: Job,
    args: JobArgsValue,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let mut specs = [crate::ddl_job_submit::JobSpec {
        job,
        args,
        id_allocated: true,
    }];
    crate::ddl_job_submit::prepare_submit_batch(
        snapshot, catalog, &mut specs, start_ts, upgrading, min_job_id,
    )?;
    let [spec] = specs;
    Ok(Some(AddIndexSubmission::Job(Box::new(spec), Vec::new())))
}

/// Go `executor.dropTableObject` for one table, view or sequence and
/// `executor.DropSchema`, up to `doDDLJob2`. `None` for any other statement.
/// `targets` are every `(schema, table)` the statement names (empty: just
/// this one), Go's `DropTableArgs.Identifiers`.
pub fn prepare_drop_object_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    targets: &[(String, String)],
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    match statement {
        DdlStatement::DropTable {
            schema,
            table,
            if_exists,
        } => {
            let identifiers: Vec<(String, String)> = if targets.is_empty() {
                vec![(schema.clone(), table.clone())]
            } else {
                targets.to_vec()
            };
            // Go `dropTableObject`: before any table goes, no table the
            // statement names may be cited by a key from a table it does not
            // name.
            for (target_schema, target_table) in &identifiers {
                if let Some(referred) =
                    crate::foreign_key_build::check_table_has_foreign_key_referred(
                        &catalog,
                        target_schema,
                        target_table,
                        &identifiers,
                        job_context.foreign_key_checks(),
                    )
                {
                    return Err(foreign_key_cannot_drop_parent(target_table, &referred));
                }
            }
            let missing = || -> Result<Option<AddIndexSubmission>, DdlPlanError> {
                if *if_exists {
                    return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                        detail: format!("table `{schema}`.`{table}` does not exist"),
                        warnings: vec![missing_table_note(schema, table)],
                    }));
                }
                Err(DdlPlanError::from(
                    tidb_error::catalog::infoschema::ERR_TABLE_DROP_EXISTS
                        .generate_with_stack_by_args(&[format!("{schema}.{table}").into()]),
                ))
            };
            let Some(database) = find_database(&catalog, schema) else {
                return missing();
            };
            let Some(stored) = find_table(database, table) else {
                return missing();
            };
            if !check_cluster_drop_object(schema, table, stored, false)? {
                return missing();
            }
            drop_table_like(
                snapshot,
                &catalog,
                job_context,
                ActionType::ACTION_DROP_TABLE,
                database.info.id,
                schema,
                stored,
                &identifiers,
                (start_ts, upgrading, min_job_id),
            )
        }
        DdlStatement::DropView { names, if_exists } => {
            let [(schema, name)] = names.as_slice() else {
                return Ok(None);
            };
            let stored = find_database(&catalog, schema)
                .and_then(|db| find_table(db, name).map(|table| (db.info.id, table)));
            let Some((db_id, table)) = stored else {
                if *if_exists {
                    return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                        detail: format!("view `{schema}`.`{name}` does not exist"),
                        warnings: vec![missing_table_note(schema, name)],
                    }));
                }
                return Err(DdlPlanError::from(
                    tidb_error::catalog::infoschema::ERR_TABLE_DROP_EXISTS
                        .generate_with_stack_by_args(&[format!("{schema}.{name}").into()]),
                ));
            };
            check_cluster_drop_object(schema, name, table, true)?;
            drop_table_like(
                snapshot,
                &catalog,
                job_context,
                ActionType::ACTION_DROP_VIEW,
                db_id,
                schema,
                table,
                &[(schema.clone(), name.clone())],
                (start_ts, upgrading, min_job_id),
            )
        }
        DdlStatement::DropSequence { schema, name } => {
            let found = find_database(&catalog, schema).and_then(|database| {
                find_table(database, name).map(|table| (database.info.id, table))
            });
            let Some((db_id, stored)) = found else {
                return Err(coded_refusal(
                    &tidb_dbterror::ERR_UNKNOWN_SEQUENCE,
                    format!("Unknown SEQUENCE: '{schema}.{name}'"),
                ));
            };
            if !stored.is_sequence() {
                return Err(coded_refusal(
                    &tidb_dbterror::ERR_WRONG_OBJECT,
                    format!("'{schema}.{name}' is not SEQUENCE"),
                ));
            }
            drop_table_like(
                snapshot,
                &catalog,
                job_context,
                ActionType::ACTION_DROP_SEQUENCE,
                db_id,
                schema,
                stored,
                &[(schema.clone(), name.clone())],
                (start_ts, upgrading, min_job_id),
            )
        }
        DdlStatement::DropDatabase { name, if_exists } => {
            let Some(database) = find_database(&catalog, name) else {
                if *if_exists {
                    return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                        detail: format!("database `{name}` does not exist"),
                        warnings: Vec::new(),
                    }));
                }
                // Go `executor.DropSchema`: `ErrDatabaseDropExists` (1008).
                return Err(DdlPlanError::Admission(DdlAdmissionError::from_terror(
                    &tidb_error::catalog::infoschema::ERR_DATABASE_DROP_EXISTS
                        .generate_with_stack_by_args(&[name.into()]),
                )));
            };
            // Go `DropSchema`: `checkDatabaseHasForeignKeyReferred`.
            check_database_has_foreign_key_referred(
                &catalog,
                &database.info,
                job_context.foreign_key_checks(),
            )?;
            let mut job = job_for(
                job_context,
                ActionType::ACTION_DROP_SCHEMA,
                database.info.id,
                database.info.name.original(),
            );
            job.schema_state = database.info.state;
            job.involving_schema_info =
                GoSharedSlice::from_vec(vec![tidb_model::InvolvingSchemaInfo {
                    database: database.info.name.lowercase().to_owned().into(),
                    table: tidb_model::job::INVOLVING_ALL.into(),
                    ..tidb_model::InvolvingSchemaInfo::default()
                }]);
            let args = GoShared::new(tidb_model::DropSchemaArgs {
                fk_check: GoField::new(job_context.foreign_key_checks()),
                ..tidb_model::DropSchemaArgs::default()
            });
            submission(
                snapshot,
                &catalog,
                job,
                JobArgsValue::DropSchema(Some(args)),
                start_ts,
                upgrading,
                min_job_id,
            )
        }
        _ => Ok(None),
    }
}

/// The job of one `dropTableObject` target.
#[allow(clippy::too_many_arguments)]
fn drop_table_like<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    job_context: &dyn DdlSessionContext,
    action: ActionType,
    schema_id: i64,
    schema: &str,
    table: &TableInfo,
    identifiers: &[(String, String)],
    (start_ts, upgrading, min_job_id): (u64, bool, i64),
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
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
        fk_check: job_context.foreign_key_checks(),
        ..tidb_model::DropTableArgs::default()
    });
    submission(
        snapshot,
        catalog,
        job,
        JobArgsValue::DropTable(Some(args)),
        start_ts,
        upgrading,
        min_job_id,
    )
}

/// Go `ErrForeignKeyCannotDropParent` (3730): `table` (as the statement or
/// the schema names it) is cited by `referred`.
pub(super) fn foreign_key_cannot_drop_parent(
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
    catalog: &ClusterCatalog,
    database: &tidb_model::DBInfo,
    fk_check: bool,
) -> Result<(), DdlPlanError> {
    if !fk_check {
        return Ok(());
    }
    let schema_name = database.name.original().to_owned();
    let tables: Vec<(String, String)> = catalog
        .databases
        .iter()
        .find(|loaded| loaded.info.id == database.id)
        .map(|loaded| {
            loaded
                .tables
                .iter()
                .map(|table| (schema_name.clone(), table.name.original().to_owned()))
                .collect()
        })
        .unwrap_or_default();
    for (_, table_name) in &tables {
        if let Some(referred) = crate::foreign_key_build::check_table_has_foreign_key_referred(
            catalog,
            &schema_name,
            table_name,
            &tables,
            true,
        ) {
            return Err(foreign_key_cannot_drop_parent(table_name, &referred));
        }
    }
    Ok(())
}
