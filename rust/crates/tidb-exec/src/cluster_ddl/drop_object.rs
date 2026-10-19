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
    job_context: &tidb_executor::StmtContext,
    action: ActionType,
    schema_id: i64,
    schema_name: &str,
) -> Job {
    let mut job = Job::default();
    job.version = get_job_ver_in_use();
    job.schema_id = schema_id;
    job.schema_name = schema_name.go_to_lower().into();
    job.type_ = action;
    job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
    job.cdc_write_source = job_context.ddl_cdc_write_source();
    job.sql_mode = job_context.ddl_sql_mode();
    job.query = job_context.ddl_query().into();
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
    job_context: &tidb_executor::StmtContext,
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
            let missing = || -> Result<Option<AddIndexSubmission>, DdlPlanError> {
                if *if_exists {
                    return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                        detail: format!("table `{schema}`.`{table}` does not exist"),
                        warnings: vec![missing_table_note(schema, table)],
                    }));
                }
                Err(DdlPlanError::UnknownTables(vec![format!(
                    "{schema}.{table}"
                )]))
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
            let identifiers: Vec<(String, String)> = if targets.is_empty() {
                vec![(schema.clone(), table.clone())]
            } else {
                targets.to_vec()
            };
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
                return Err(DdlPlanError::UnknownTables(vec![format!(
                    "{schema}.{name}"
                )]));
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
                    tidb_error::tidb::errcode::ErrUnknownSequence,
                    format!("Unknown SEQUENCE: '{schema}.{name}'"),
                ));
            };
            if !stored.is_sequence() {
                return Err(coded_refusal(
                    tidb_error::tidb::errcode::ErrWrongObject,
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
                return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                    1008,
                    format!("Can't drop database '{name}'; database doesn't exist"),
                )));
            };
            let mut job = job_for(
                job_context,
                ActionType::ACTION_DROP_SCHEMA,
                database.info.id,
                database.info.name.original(),
            );
            job.schema_state = database.info.state;
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
    job_context: &tidb_executor::StmtContext,
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
