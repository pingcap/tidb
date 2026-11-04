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

//! CREATE VIEW, CREATE / ALTER SEQUENCE and CREATE TABLE LIKE as Go runs
//! them. The executor builds the table info and `createTableWithInfoJob`
//! queues an `ActionCreateView`, `ActionCreateSequence` or `ActionCreateTable`
//! job whose ids `assignGIDsForJobs` draws; the owner's `onCreateView` and
//! `onCreateSequence` publish the object in one step, and a LIKE table runs
//! through `onCreateTable`. `ALTER SEQUENCE` queues `ActionAlterSequence`,
//! applied by `onAlterSequence`.

use super::add_index::{action_step, update_version_and_table_info, AddIndexSubmission};
use super::*;

/// Go `OnExist`: what `createTableWithInfoJob` does when the name is taken.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum OnExist {
    /// `ErrTableExists`.
    Error,
    /// The error as a note, and no job (`IF NOT EXISTS`).
    Ignore,
    /// Replace an existing view (`CREATE OR REPLACE VIEW`).
    Replace,
}

/// Go's executor halves up to `doDDLJob2` for CREATE VIEW, CREATE / ALTER
/// SEQUENCE and CREATE TABLE LIKE. `None` for any other statement.
pub fn prepare_create_object_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let catalog = load_info_schema(snapshot)?;
    let mut warnings = Vec::new();
    let built = match statement {
        // Go `executor.CreateView`.
        DdlStatement::CreateView {
            schema,
            name,
            or_replace,
            info,
        } => {
            let Some(database) = find_database(&catalog, schema) else {
                return Err(DdlPlanError::from(
                    tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                        .gen_with_stack_by_args(&[(schema.clone()).into()]),
                ));
            };
            tidb_ddl_build::table_lifecycle::check_view_target(
                schema,
                name,
                *or_replace,
                find_table(&database.read(), name).map(|table| table.read().view.is_some()),
            )
            .map_err(|error| match error.code {
                tidb_mysql::errors::errcode::ErrTableExists => DdlPlanError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS.gen_with_stack_by_args(
                        &[format!("{}.{}", (schema.clone()), (name.clone())).into()],
                    ),
                ),
                _ => DdlPlanError::from_mysql_error(error),
            })?;
            let on_exist = if *or_replace {
                OnExist::Replace
            } else {
                OnExist::Error
            };
            create_table_with_info_job(
                snapshot,
                &catalog,
                schema,
                (**info).clone(),
                on_exist,
                job_context,
                &mut warnings,
            )?
        }
        // Go `executor.CreateSequence`.
        DdlStatement::CreateSequence {
            schema,
            if_not_exists,
            info,
            ..
        } => {
            let on_exist = if *if_not_exists {
                OnExist::Ignore
            } else {
                OnExist::Error
            };
            create_table_with_info_job(
                snapshot,
                &catalog,
                schema,
                (**info).clone(),
                on_exist,
                job_context,
                &mut warnings,
            )?
        }
        // Go `executor.CreateTableWithLike`.
        DdlStatement::CreateTableLike {
            schema,
            if_not_exists,
            ..
        } => match build_create_table_like_info(&catalog, statement, job_context)? {
            Ok(info) => {
                let on_exist = if *if_not_exists {
                    OnExist::Ignore
                } else {
                    OnExist::Error
                };
                create_table_with_info_job(
                    snapshot,
                    &catalog,
                    schema,
                    info,
                    on_exist,
                    job_context,
                    &mut warnings,
                )?
            }
            Err(satisfied) => Some(satisfied),
        },
        DdlStatement::AlterSequence { .. } => {
            build_alter_sequence_job(&catalog, statement, job_context)?
        }
        _ => return Ok(None),
    };
    let built = match built {
        Some(AddIndexSubmission::Job(spec, executor_warnings)) => {
            warnings.extend(executor_warnings);
            Some(AddIndexSubmission::Job(spec, warnings))
        }
        Some(AddIndexSubmission::AlreadySatisfied {
            detail,
            warnings: notes,
        }) => {
            warnings.extend(notes);
            Some(AddIndexSubmission::AlreadySatisfied { detail, warnings })
        }
        None => None,
    };
    submit_built_job(snapshot, &catalog, built, start_ts, upgrading, min_job_id)
}

/// Go `executor.createTableWithInfoJob`.
pub(super) fn create_table_with_info_job<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &SessionExtendedInfoSchema,
    schema: &str,
    mut info: TableInfo,
    on_exist: OnExist,
    job_context: &dyn DdlSessionContext,
    warnings: &mut Vec<tidb_context::SqlWarn>,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let Some(database) = find_database(catalog, schema) else {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                .gen_with_stack_by_args(&[(schema.to_owned()).into()]),
        ));
    };
    handle_table_placement(snapshot, job_context, &mut info, warnings)?;
    let mut old_view_id = 0;
    if let Some(existing) = find_table(&database.read(), info.name.original()) {
        let existing = existing.read();
        let exists = || {
            DdlPlanError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS.gen_with_stack_by_args(&[
                    format!(
                        "{}.{}",
                        (schema.to_owned()),
                        (info.name.original().to_owned())
                    )
                    .into(),
                ]),
            )
        };
        match on_exist {
            OnExist::Ignore => {
                return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                    detail: format!("`{schema}`.`{}` already exists", info.name.original()),
                    warnings: vec![crate::real_tikv_ddl::ddl_note(
                        tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS
                            .gen_with_stack_by_args(&[format!(
                                "{}.{}",
                                schema,
                                info.name.original()
                            )
                            .into()]),
                    )],
                }));
            }
            // Only CREATE OR REPLACE VIEW replaces, and only a view.
            OnExist::Replace if info.view.is_some() => {
                if existing.view.is_none() {
                    return Err(coded_refusal(
                        &tidb_dbterror::ERR_WRONG_OBJECT,
                        format!("'{schema}.{}' is not VIEW", info.name.original()),
                    ));
                }
                old_view_id = existing.id;
            }
            _ => return Err(exists()),
        }
    }
    check_table_info_valid_extra(&info, warnings)?;
    let action = if info.view.is_some() {
        ActionType::ACTION_CREATE_VIEW
    } else if info.sequence.is_some() {
        ActionType::ACTION_CREATE_SEQUENCE
    } else {
        ActionType::ACTION_CREATE_TABLE
    };
    let mut job = session_job(job_context, action);
    job.schema_id = database.read().id;
    job.schema_name = database.read().name.lowercase().to_owned().into();
    job.table_name = info.name.lowercase().to_owned().into();
    let shared = create_table::get_shared_involving_schema_info(&info);
    if !shared.is_empty() {
        let mut involving = vec![tidb_model::InvolvingSchemaInfo {
            database: job.schema_name.clone(),
            table: job.table_name.clone(),
            ..tidb_model::InvolvingSchemaInfo::default()
        }];
        involving.extend(shared);
        job.involving_schema_info = GoSharedSlice::from_vec(involving);
    }
    job.session_vars = Some(GoShared::new(std::collections::BTreeMap::from([(
        tidb_vardef::tidb_vars::TIDB_SCATTER_REGION.to_owned(),
        job_context.ddl_reorg_vars().scatter_region.clone().into(),
    )])));
    let args = GoShared::new(tidb_model::CreateTableArgs {
        table_info: GoField::new(Some(GoShared::new(info))),
        on_exist_replace: GoField::new(on_exist == OnExist::Replace),
        old_view_table_id: GoField::new(old_view_id),
        fk_check: GoField::new(job_context.foreign_key_checks()),
    });
    Ok(Some(AddIndexSubmission::Job(
        Box::new(tidb_ddl_jobsubmit::JobSpec {
            job,
            args: JobArgsValue::CreateTable(Some(args)),
            // `assignGIDsForJobs` draws the object's (and partitions') ids.
            id_allocated: false,
        }),
        Vec::new(),
    )))
}

/// Go `BuildTableInfoWithLike` after the preprocessor's and the executor's
/// checks: the source's public definition under the new name, with Go's
/// reset list applied. `Err` is an IF NOT EXISTS over an existing table.
fn build_create_table_like_info(
    catalog: &SessionExtendedInfoSchema,
    statement: &DdlStatement,
    context: &dyn DdlSessionContext,
) -> Result<Result<TableInfo, AddIndexSubmission>, DdlPlanError> {
    let DdlStatement::CreateTableLike {
        schema,
        table,
        source_schema,
        source_table,
        temporary,
        on_commit_delete,
        if_not_exists,
    } = statement
    else {
        unreachable!("called for CREATE TABLE LIKE");
    };
    // Go's preprocessor resolves and validates the LIKE source before the
    // DDL executor checks the target database. A missing source database is
    // consequently reported as a missing table.
    let source = resolve_like_source(catalog, source_schema, source_table, *temporary)?;
    let source = source.read();
    let Some(database) = find_database(catalog, schema) else {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                .gen_with_stack_by_args(&[(schema.clone()).into()]),
        ));
    };
    // Go `BuildTableInfoWithLike` runs after the executor has found the
    // target database, but before target-name collision handling.
    let mut info = tidb_ddl_build::table_info_build::build_table_info_with_like(
        schema,
        table,
        &source,
        *temporary,
        *on_commit_delete,
    )
    .map_err(DdlPlanError::Admission)?;
    // Go `CreateTable` runs `checkTableInfoValidWithStmt` on the copy too,
    // before `createTableWithInfoJob` looks for the name.
    tidb_ddl_build::table_info_build::check_table_info_valid_with_stmt(
        &context.metabuild_context(),
        &info,
    )
    .map_err(DdlPlanError::Admission)?;
    if let Some(existing) = find_table(&database.read(), table) {
        let existing = existing.read();
        if *if_not_exists {
            return Ok(Err(AddIndexSubmission::AlreadySatisfied {
                detail: format!(
                    "table `{schema}`.`{}` already exists",
                    existing.name.original()
                ),
                warnings: vec![crate::real_tikv_ddl::ddl_note(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS.gen_with_stack_by_args(
                        &[format!("{}.{}", schema, existing.name.original()).into()],
                    ),
                )],
            }));
        }
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS.gen_with_stack_by_args(&[
                format!("{}.{}", (schema.clone()), (table.clone())).into(),
            ]),
        ));
    }
    // `assignGIDsForJobs` draws the table's and its partitions' ids.
    info.id = 0;
    info.state = SchemaState::NONE;
    Ok(Ok(info))
}

/// Go's preprocessor resolution of a `CREATE TABLE ... LIKE` source
/// (`checkCreateTableGrammar`, `checkReferInfoForTemporaryTable`): the source
/// must exist, a temporary source is refused, and a temporary copy may not
/// inherit what a temporary table cannot carry.
pub fn resolve_like_source<'catalog>(
    catalog: &'catalog SessionExtendedInfoSchema,
    source_schema: &str,
    source_table: &str,
    temporary: tidb_ast::CreateTableTemporary,
) -> Result<GoShared<TableInfo>, DdlPlanError> {
    // Go `is.TableByName`: a missing source database is reported as a
    // missing table.
    let Some((_, source)) = catalog.table_info_by_name(source_schema, source_table) else {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.gen_with_stack_by_args(&[
                (source_schema.to_owned()).into(),
                (source_table.to_owned()).into(),
            ]),
        ));
    };
    tidb_ddl_build::check_create_table_like_source(&source.read(), temporary)
        .map_err(|error| DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error)))?;
    Ok(source)
}

/// Go `BuildSessionTemporaryTableInfo` (`create_table.go`): a LOCAL
/// temporary table's `TableInfo` in `database` -- copied from `like`'s
/// source (resolved by [`resolve_like_source`]), or built from the statement
/// under the database's charset -- then, for either branch,
/// `checkTableInfoValidWithStmt` and `checkTableInfoValidExtra`. The id is the
/// caller's to draw (`newTemporaryTableFromTableInfo`).
pub fn build_session_temporary_table_info(
    database: &tidb_model::db::DBInfo,
    create: &CreateTableStmt,
    table: &str,
    like: Option<(&str, &TableInfo)>,
    context: &dyn DdlSessionContext,
    warnings: &mut Vec<tidb_context::SqlWarn>,
) -> Result<TableInfo, DdlPlanError> {
    let mb_ctx = context.metabuild_context();
    let info = match like {
        Some((_, source)) => tidb_ddl_build::table_info_build::build_table_info_with_like(
            database.name.original(),
            table,
            source,
            create.temporary,
            create.on_commit_delete,
        )
        .map_err(DdlPlanError::Admission)?,
        None => tidb_ddl_build::table_info_build::build_table_info_with_stmt(
            create,
            database.name.original(),
            &database.charset,
            &database.collate,
            &mb_ctx,
            context.enable_check_constraint(),
        )
        .map_err(DdlPlanError::Admission)?,
    };
    tidb_ddl_build::table_info_build::check_table_info_valid_with_stmt(&mb_ctx, &info)
        .map_err(DdlPlanError::Admission)?;
    check_table_info_valid_extra(&info, warnings)?;
    Ok(info)
}

/// The arguments of a CREATE VIEW / SEQUENCE job.
fn create_table_args(
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
) -> Result<(GoShared<TableInfo>, bool, i64), DdlPlanError> {
    let args = tidb_model::get_create_table_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("CREATE job has nil args".to_owned()),
            )
        })?;
    let args = args.read();
    let info = args
        .table_info
        .get()
        .ok_or_else(|| DdlPlanError::Encode("CREATE job has nil table_info".to_owned()))?;
    Ok((
        info,
        args.on_exist_replace.get(),
        args.old_view_table_id.get(),
    ))
}

/// The job's database, or the job cancelled as Go's
/// `checkSchemaExistAndCancelNotExistJob` cancels it.
fn job_database(
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
) -> Result<GoShared<DBInfo>, DdlPlanError> {
    catalog
        .schema_by_id(active.job.schema_id)
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::from(
                    tidb_dbterror::CLASS_SCHEMA
                        .new_std(tidb_errno::errcode::ErrBadDB)
                        .generate(format!(
                            "Unknown database '(Schema ID {})'",
                            active.job.schema_id
                        )),
                ),
            )
        })
}

fn table_exists_in_owner(schema: &str, name: &str) -> DdlPlanError {
    DdlPlanError::from(
        tidb_dbterror::CLASS_SCHEMA
            .new_std(tidb_errno::errcode::ErrTableExists)
            .generate(format!("Table '{schema}.{name}' already exists")),
    )
}

/// One step publishing a new object under a new version.
fn object_step(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    diff: SchemaDiff,
    info: &TableInfo,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let (schema_version, diff) = update_schema_version(&txn.meta(), active, diff)?;
    active.job.finish_table_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        schema_version,
        Some(GoShared::new(info.clone_like_go())),
    );
    let mut write = job_only_write(active.job.id, Vec::new());
    write.schema_version = schema_version;
    write.diff = diff;
    write.created_id = Some(info.id);
    Ok(action_step(write))
}

/// Go `onCreateView`.
pub(super) fn on_create_view(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let (handle, or_replace, old_view_id) = create_table_args(active)?;
    let database = job_database(catalog, active)?;
    let database_id = database.read().id;
    let schema = database.read().name.original().to_owned();
    let mut info = handle.read().clone_like_go();
    info.state = SchemaState::NONE;
    let old = database
        .read().deprecated_tables
        .iter_deref()
        .find(|table| table.read().name.lowercase() == info.name.lowercase())
        .map(|table| table.read().id);
    if old.is_some() && !or_replace {
        return Err(cancel_ddl_job(
            active,
            table_exists_in_owner(&schema, info.name.original()),
        ));
    }
    info.state = SchemaState::PUBLIC;
    info.update_ts = start_ts;
    let meta = txn.meta();
    let meta_error = |error| DdlPlanError::Meta(job_meta_error(&meta, error));
    if let Some(old) = old.filter(|_| or_replace) {
        // Go `DropTableOrView` and `GetAutoIDAccessors(...).Del()`.
        meta.drop_table_or_view(database_id, old).map_err(meta_error)?;
        meta.auto_ids(database_id, old).delete().map_err(meta_error)?;
    }
    // Go `createTableOrViewWithCheck`.
    check_table_info_valid(&info).map_err(|error| cancel_ddl_job(active, error))?;
    meta.create_table_or_view(database_id, &info).map_err(meta_error)?;
    *handle.write() = info.clone_like_go();
    // Go `SetSchemaDiffForCreateView`.
    let diff = SchemaDiff {
        action_type: active.job.type_,
        schema_id: database_id,
        table_id: info.id,
        old_table_id: if or_replace && old_view_id > 0 {
            old_view_id
        } else {
            0
        },
        ..SchemaDiff::default()
    };
    object_step(txn, active, diff, &info)
}

/// Go `createSequenceWithCheck`, which `onCreateSequence` and
/// `onCreateTables` share.
pub(super) fn create_sequence_with_check(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    database_id: i64,
    info: &mut TableInfo,
    start_ts: u64,
) -> Result<(), DdlPlanError> {
    let state = info.state;
    if !matches!(state, SchemaState::NONE | SchemaState::PUBLIC) {
        return Err(DdlPlanError::from(
            tidb_dbterror::ERR_INVALID_DDL_STATE.gen_with_stack_by_args(&[
                FormatArg::from("sequence"),
                FormatArg::new(state.to_string(), state.0.to_string(), "model.SchemaState"),
            ]),
        ));
    }
    info.state = SchemaState::PUBLIC;
    info.update_ts = start_ts;
    check_table_info_valid(info).map_err(|error| cancel_ddl_job(active, error))?;
    let increment_and_start = info
        .sequence
        .as_ref()
        .map(|sequence| {
            let sequence = sequence.read();
            (sequence.increment, sequence.start)
        })
        .ok_or_else(|| DdlPlanError::Encode("CREATE SEQUENCE has no sequence".to_owned()))?;
    // Go `CreateSequenceAndSetSeqValue`.
    let meta = txn.meta();
    meta.create_sequence_and_set_seq_value(
        database_id,
        info,
        sequence_base_for(increment_and_start.0, increment_and_start.1),
    )
    .map_err(|error| DdlPlanError::Meta(job_meta_error(&meta, error)))
}

/// Go `onCreateSequence`.
pub(super) fn on_create_sequence(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let (handle, _, _) = create_table_args(active)?;
    let database = job_database(catalog, active)?;
    let database_id = database.read().id;
    let schema = database.read().name.original().to_owned();
    let mut info = handle.read().clone_like_go();
    // Go `checkTableNotExists`.
    if database
        .read().deprecated_tables
        .iter_deref()
        .any(|table| table.read().name.lowercase() == info.name.lowercase())
    {
        return Err(cancel_ddl_job(
            active,
            table_exists_in_owner(&schema, info.name.original()),
        ));
    }
    create_sequence_with_check(txn, active, database_id, &mut info, start_ts)?;
    *handle.write() = info.clone_like_go();
    let diff = SchemaDiff {
        action_type: active.job.type_,
        schema_id: database_id,
        table_id: info.id,
        ..SchemaDiff::default()
    };
    object_step(txn, active, diff, &info)
}

/// Go `executor.AlterSequence` up to `doDDLJob2`.
fn build_alter_sequence_job(
    catalog: &SessionExtendedInfoSchema,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let DdlStatement::AlterSequence {
        schema,
        name,
        if_exists,
        options,
    } = statement
    else {
        unreachable!("called for ALTER SEQUENCE");
    };
    let Some(database) = find_database(catalog, schema) else {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                .gen_with_stack_by_args(&[(schema.clone()).into()]),
        ));
    };
    let Some(stored) = find_table(&database.read(), name) else {
        if *if_exists {
            return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                detail: format!("sequence `{schema}`.`{name}` does not exist"),
                warnings: vec![crate::real_tikv_ddl::ddl_note(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .gen_with_stack_by_args(&[schema.into(), name.into()]),
                )],
            }));
        }
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[(schema.clone()).into(), (name.clone()).into()]),
        ));
    };
    let stored = stored.read();
    let Some(old) = stored.sequence.as_ref().map(|shared| shared.read().clone()) else {
        return Err(coded_refusal(
            &tidb_dbterror::ERR_WRONG_OBJECT,
            format!("'{schema}.{name}' is not SEQUENCE"),
        ));
    };
    // Validate the options over a copy of the stored ones.
    tidb_ddl_build::sequence::alter_sequence_options(tidb_model::table::SequenceInfo::clone(&old), options, schema, name)
        .map_err(DdlPlanError::from_mysql_error)?;
    let mut job = session_job(job_context, ActionType::ACTION_ALTER_SEQUENCE);
    job.schema_id = database.read().id;
    job.table_id = stored.id;
    job.schema_name = database.read().name.lowercase().to_owned().into();
    job.table_name = stored.name.lowercase().to_owned().into();
    let args = tidb_model::AlterSequenceArgs {
        ident: tidb_model::Ident {
            schema: CiString::new(schema.clone()),
            name: CiString::new(name.clone()),
        },
        seq_options: options
            .iter()
            .map(tidb_ast::JobSequenceOption::from_option)
            .collect(),
    };
    Ok(Some(AddIndexSubmission::Job(
        Box::new(tidb_ddl_jobsubmit::JobSpec {
            job,
            args: JobArgsValue::AlterSequence(Some(GoShared::new(args))),
            id_allocated: true,
        }),
        Vec::new(),
    )))
}

/// Go `onAlterSequence`.
pub(super) fn on_alter_sequence(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let args = tidb_model::get_alter_sequence_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::Encode("ALTER SEQUENCE job has nil args".to_owned()),
            )
        })?;
    let (ident, options) = {
        let args = args.read();
        let options: Vec<tidb_ast::SequenceOption> = args
            .seq_options
            .iter()
            .filter_map(tidb_ast::JobSequenceOption::to_option)
            .collect();
        (args.ident.clone(), options)
    };
    let mut info = checked_job_table(catalog, active, false)?.read().clone_like_go();
    let database_id = active.job.schema_id;
    let Some(old) = info.sequence.as_ref().map(|shared| shared.read().clone()) else {
        return Err(cancel_ddl_job(
            active,
            coded_refusal(
                &tidb_dbterror::ERR_WRONG_OBJECT,
                format!(
                    "'{}.{}' is not SEQUENCE",
                    ident.schema.original(),
                    ident.name.original()
                ),
            ),
        ));
    };
    let (altered, restart) = tidb_ddl_build::sequence::alter_sequence_options(
        tidb_model::table::SequenceInfo::clone(&old),
        &options,
        ident.schema.original(),
        ident.name.original(),
    )
    .map_err(|error| cancel_ddl_job(active, DdlPlanError::from_mysql_error(error)))?;
    // The options apply over the stored ones, so the comment rides along.
    // Go: nothing changes and nothing restarts -- done, nothing published.
    if altered == old && restart.is_none() {
        active.job.state = JobState::DONE;
        return Ok(job_control_step(&active.job));
    }
    let increment = altered.increment;
    info.sequence = Some(GoShared::new(altered));
    let mut restart_write = Vec::new();
    if let Some(value) = restart {
        // Go `restartSequenceValue`.
        restart_write.push(BufferMutation::set(
            sequence_value_kv_key(database_id, info.id),
            value::encode_int_value(sequence_base_for(increment, value)),
        )?);
    }
    let mut write = update_version_and_table_info(txn, active, database_id, &info, true, start_ts)?;
    write.mutations.extend(restart_write);
    active.job.finish_table_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        write.schema_version,
        Some(GoShared::new(info.clone_like_go())),
    );
    Ok(action_step(write))
}
