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

//! RENAME TABLE as Go runs it: one pair is `ActionRenameTable`
//! (`executor.renameTable`), several are one `ActionRenameTables` job
//! (`renameTables`). The owner moves every table and re-points the foreign
//! keys that name it in one schema version (`onRenameTable(s)`), then
//! finishes the job with a second (`finishJobRenameTable(s)`), as TiCDC
//! requires.

use super::add_index::AddIndexSubmission;
use super::*;

/// Go `ExtractTblInfos` over the `tables` map that carries earlier pairs'
/// moves: the old and new schema ids and the table, or `None` for an
/// ALTER TABLE ... RENAME to its own name.
fn extract_tbl_infos(
    catalog: &ClusterCatalog,
    pair: &RenameTablePair,
    is_alter: bool,
    tables: &mut BTreeMap<String, Option<i64>>,
) -> Result<Option<(i64, CiString, i64, CiString, i64)>, DdlPlanError> {
    let key =
        |schema: &str, table: &str| format!("{}.{}", schema.go_to_lower(), table.go_to_lower());
    let table_id = |tables: &BTreeMap<String, Option<i64>>, schema: &str, table: &str| match tables
        .get(&key(schema, table))
    {
        Some(id) => *id,
        None => catalog.find_table(schema, table).map(|(_, table)| table.id),
    };
    let source_id = table_id(tables, &pair.from_schema, &pair.from_table);
    let target_exists = table_id(tables, &pair.to_schema, &pair.to_table).is_some();
    let old_database = find_database(catalog, &pair.from_schema);
    let new_database = find_database(catalog, &pair.to_schema);
    let source_is_view = catalog
        .find_table(&pair.from_schema, &pair.from_table)
        .is_some_and(|(_, table)| table.view.is_some());
    use tidb_ddl_build::table_lifecycle::{check_rename, RenameAdmission};
    let admitted = check_rename(RenameAdmission {
        source: (&pair.from_schema, &pair.from_table),
        target: (&pair.to_schema, &pair.to_table),
        source_exists: old_database.is_some() && source_id.is_some(),
        target_schema_exists: new_database.is_some(),
        target_exists,
        source_is_view,
        is_alter,
    })
    .map_err(|error| match error.code {
        tidb_mysql::errors::errcode::ErrNoSuchTable => DdlPlanError::from(
            tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS.generate_with_stack_by_args(&[
                (pair.from_schema.clone()).into(),
                (pair.from_table.clone()).into(),
            ]),
        ),
        tidb_mysql::errors::errcode::ErrTableExists => DdlPlanError::from(
            tidb_error::catalog::infoschema::ERR_TABLE_EXISTS.generate_with_stack_by_args(&[
                format!("{}.{}", (pair.to_schema.clone()), (pair.to_table.clone())).into(),
            ]),
        ),
        _ => DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error)),
    })?;
    if !admitted {
        return Ok(None);
    }
    let (old_database, new_database) = (
        old_database.expect("admitted source schema"),
        new_database.expect("admitted target schema"),
    );
    let table_id = source_id.expect("admitted source table");
    tables.insert(key(&pair.from_schema, &pair.from_table), None);
    tables.insert(key(&pair.to_schema, &pair.to_table), Some(table_id));
    Ok(Some((
        old_database.info.id,
        old_database.info.name.clone(),
        new_database.info.id,
        new_database.info.name.clone(),
        table_id,
    )))
}

/// Go's checks on the table a rename moves: not cached, normal mode.
fn check_renamable(
    catalog: &ClusterCatalog,
    table_id: i64,
    operation: &str,
) -> Result<(), DdlPlanError> {
    let Some(table) = catalog
        .databases
        .iter()
        .flat_map(|database| database.tables.iter())
        .find(|table| table.id == table_id)
    else {
        return Ok(());
    };
    if table.table_cache_status_type != tidb_model::TableCacheStatusType::DISABLE {
        return Err(coded_refusal(
            &tidb_dbterror::ERR_OPT_ON_CACHE_TABLE,
            format!("'{operation}' is unsupported on cache tables."),
        ));
    }
    // Go `dbutil.CheckTableModeIsNormal`.
    if table.mode != tidb_model::TableMode::NORMAL {
        return Err(coded_refusal(
            &tidb_error::catalog::infoschema::ERR_PROTECTED_TABLE_MODE,
            format!("Table {} is in mode {}", table.name.original(), table.mode),
        ));
    }
    Ok(())
}

/// Go `executor.RenameTable` up to `doDDLJob2`. `None` for any other
/// statement.
pub fn prepare_rename_table_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let (pairs, is_alter) = match statement {
        DdlStatement::RenameTable {
            is_alter,
            from_schema,
            from_table,
            to_schema,
            to_table,
        } => (
            vec![RenameTablePair {
                from_schema: from_schema.clone(),
                from_table: from_table.clone(),
                to_schema: to_schema.clone(),
                to_table: to_table.clone(),
            }],
            *is_alter,
        ),
        DdlStatement::RenameTables { pairs } => (pairs.clone(), false),
        _ => return Ok(None),
    };
    let catalog = load_cluster_catalog(snapshot)?;
    let mut tables = BTreeMap::new();
    let (job, args) = if pairs.len() == 1 {
        // Go `renameTable`.
        let pair = &pairs[0];
        let Some((old_schema_id, old_schema_name, new_schema_id, new_schema_name, table_id)) =
            extract_tbl_infos(&catalog, pair, is_alter, &mut tables)?
        else {
            return Ok(Some(AddIndexSubmission::AlreadySatisfied {
                detail: format!(
                    "table `{}`.`{}` already has its requested name",
                    pair.from_schema, pair.from_table
                ),
                warnings: Vec::new(),
            }));
        };
        check_renamable(&catalog, table_id, "Rename Table")?;
        let mut job = session_job(job_context, ActionType::ACTION_RENAME_TABLE);
        job.schema_id = new_schema_id;
        job.table_id = table_id;
        job.schema_name = new_schema_name.lowercase().to_owned().into();
        job.table_name = pair.from_table.go_to_lower().into();
        job.involving_schema_info = GoSharedSlice::from_vec(vec![
            tidb_model::InvolvingSchemaInfo {
                database: old_schema_name.lowercase().to_owned().into(),
                table: pair.from_table.go_to_lower().into(),
                ..tidb_model::InvolvingSchemaInfo::default()
            },
            tidb_model::InvolvingSchemaInfo {
                database: new_schema_name.lowercase().to_owned().into(),
                table: pair.to_table.go_to_lower().into(),
                ..tidb_model::InvolvingSchemaInfo::default()
            },
        ]);
        let args = JobArgsValue::RenameTable(Some(GoShared::new(tidb_model::RenameTableArgs {
            old_schema_id,
            old_schema_name,
            new_table_name: CiString::new(pair.to_table.clone()),
            ..tidb_model::RenameTableArgs::default()
        })));
        (job, args)
    } else {
        // Go `renameTables`.
        let mut infos = Vec::with_capacity(pairs.len());
        let mut involving = Vec::with_capacity(pairs.len() * 2);
        let mut last_schema = (0, CiString::default());
        for pair in &pairs {
            let (old_schema_id, old_schema_name, new_schema_id, new_schema_name, table_id) =
                extract_tbl_infos(&catalog, pair, false, &mut tables)?
                    .expect("RENAME TABLE never names its own source as the target");
            check_renamable(&catalog, table_id, "Rename Tables")?;
            involving.push(tidb_model::InvolvingSchemaInfo {
                database: old_schema_name.lowercase().to_owned().into(),
                table: pair.from_table.go_to_lower().into(),
                ..tidb_model::InvolvingSchemaInfo::default()
            });
            involving.push(tidb_model::InvolvingSchemaInfo {
                database: new_schema_name.lowercase().to_owned().into(),
                table: pair.to_table.go_to_lower().into(),
                ..tidb_model::InvolvingSchemaInfo::default()
            });
            infos.push(tidb_model::RenameTableArgs {
                old_schema_id,
                old_schema_name,
                old_table_name: CiString::new(pair.from_table.clone()),
                new_schema_id,
                new_table_name: CiString::new(pair.to_table.clone()),
                table_id,
                ..tidb_model::RenameTableArgs::default()
            });
            last_schema = (new_schema_id, new_schema_name);
        }
        let mut job = session_job(job_context, ActionType::ACTION_RENAME_TABLES);
        job.schema_id = last_schema.0;
        job.table_id = infos[0].table_id;
        job.schema_name = last_schema.1.lowercase().to_owned().into();
        job.involving_schema_info = GoSharedSlice::from_vec(involving);
        let args = JobArgsValue::RenameTables(Some(GoShared::new(tidb_model::RenameTablesArgs {
            rename_table_infos: GoField::new(GoSharedPointerSlice::from(infos)),
        })));
        (job, args)
    };
    let mut specs = [crate::ddl_job_submit::JobSpec {
        job,
        args,
        id_allocated: true,
    }];
    crate::ddl_job_submit::prepare_submit_batch(
        snapshot, &catalog, &mut specs, start_ts, upgrading, min_job_id,
    )?;
    let [spec] = specs;
    Ok(Some(AddIndexSubmission::Job(Box::new(spec), Vec::new())))
}

/// The tables as the job's earlier moves left them, keyed by id, with the
/// meta keys the job writes and deletes.
struct EvolvingTables<'a> {
    catalog: &'a ClusterCatalog,
    tables: BTreeMap<i64, (i64, TableInfo)>,
    written: Vec<i64>,
    deleted: Vec<(i64, i64)>,
}

impl<'a> EvolvingTables<'a> {
    fn new(catalog: &'a ClusterCatalog) -> Self {
        Self {
            catalog,
            tables: BTreeMap::new(),
            written: Vec::new(),
            deleted: Vec::new(),
        }
    }

    /// Go `meta.GetTable(schemaID, tableID)`.
    fn get(&mut self, schema_id: i64, table_id: i64) -> Option<TableInfo> {
        if let Some((schema, table)) = self.tables.get(&table_id) {
            return (*schema == schema_id).then(|| table.clone_like_go());
        }
        let table = self
            .catalog
            .databases
            .iter()
            .find(|database| database.info.id == schema_id)?
            .tables
            .iter()
            .find(|table| table.id == table_id)?
            .clone_like_go();
        self.tables
            .insert(table_id, (schema_id, table.clone_like_go()));
        Some(table)
    }

    fn drop_table(&mut self, schema_id: i64, table_id: i64) {
        self.tables.remove(&table_id);
        self.written.retain(|id| *id != table_id);
        self.deleted.push((schema_id, table_id));
    }

    fn put(&mut self, schema_id: i64, table: TableInfo) {
        let table_id = table.id;
        self.deleted
            .retain(|(schema, id)| !(*schema == schema_id && *id == table_id));
        self.tables.insert(table_id, (schema_id, table));
        if !self.written.contains(&table_id) {
            self.written.push(table_id);
        }
    }

    fn mutations(&self) -> Result<Vec<BufferMutation>, DdlPlanError> {
        let mut mutations = Vec::new();
        for (schema_id, table_id) in &self.deleted {
            mutations.push(BufferMutation::delete(key::table_kv_key(
                *schema_id, *table_id,
            ))?);
        }
        for table_id in &self.written {
            let (schema_id, table) = &self.tables[table_id];
            mutations.push(BufferMutation::set(
                key::table_kv_key(*schema_id, *table_id),
                value::serialize_table_info(table)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?);
        }
        Ok(mutations)
    }
}

/// Go's foreign-key helper: the tables `adjustForeignKeyChildTableInfoAfterRenameTable`
/// loaded, which `updateSchemaVersion` lists as affected.
#[derive(Default)]
struct ForeignKeyHelper {
    loaded: Vec<(String, i64, i64)>,
}

/// Go `GetTableReferredForeignKeys` over the catalog the job started from.
fn referred_foreign_keys(
    catalog: &ClusterCatalog,
    schema: &str,
    table: &str,
) -> Vec<(String, String, String)> {
    let mut referred = Vec::new();
    for database in &catalog.databases {
        for child in &database.tables {
            for fk in child.foreign_keys.iter_deref() {
                let fk = fk.read();
                if fk.version < tidb_model::table::FK_VERSION1 {
                    continue;
                }
                if fk.ref_schema.lowercase() == schema && fk.ref_table.lowercase() == table {
                    referred.push((
                        database.info.name.lowercase().to_owned(),
                        child.name.lowercase().to_owned(),
                        fk.name.lowercase().to_owned(),
                    ));
                }
            }
        }
    }
    referred
}

/// Go `checkAndRenameTables` and `adjustForeignKeyChildTableInfoAfterRenameTable`
/// for one entry.
#[allow(clippy::too_many_arguments)]
fn rename_one(
    catalog: &ClusterCatalog,
    tables: &mut EvolvingTables<'_>,
    helper: &mut ForeignKeyHelper,
    label_rules: &mut LabelRuleSteps,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    info: &tidb_model::RenameTableArgs,
    new_schema_id: i64,
) -> Result<(), DdlPlanError> {
    let not_exists = || {
        DdlPlanError::from(
            tidb_dbterror::CLASS_SCHEMA
                .new_std(tidb_error::tidb::errcode::ErrNoSuchTable)
                .generate(format!(
                    "Table '{}.{}' doesn't exist",
                    info.old_schema_name.original(),
                    info.old_table_name.original()
                )),
        )
    };
    // Go `GetTableInfoAndCancelFaultJob(t, job, oldSchemaID)` by the job's
    // table id.
    let Some(mut table) = tables.get(info.old_schema_id, active.job.table_id) else {
        return Err(cancel_ddl_job(active, not_exists()));
    };
    if table.state != SchemaState::PUBLIC {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_dbterror::ERR_INVALID_DDL_STATE.generate_with_stack_by_args(&[
                    FormatArg::from("table"),
                    FormatArg::new(
                        table.state.to_string(),
                        table.state.0.to_string(),
                        "model.SchemaState",
                    ),
                ]),
            ),
        ));
    }
    tables.drop_table(info.old_schema_id, table.id);
    let old_table_name = table.name.clone();
    let old_rules = label_rules::get_old_label_rules(
        label_rules.manager(),
        &LabelRuleTable::from_table_info(&table),
        info.old_schema_name.lowercase(),
        old_table_name.lowercase(),
    )
    .map_err(|error| {
        cancel_ddl_job(
            active,
            pd_step_error(Some("failed to get old label rules from PD"), error),
        )
    })?;
    if table.auto_id_schema_id == 0 && new_schema_id != info.old_schema_id {
        table.auto_id_schema_id = info.old_schema_id;
    }
    if new_schema_id == table.auto_id_schema_id {
        table.auto_id_schema_id = 0;
    }
    table.name = info.new_table_name.clone();
    let Some(new_database) = catalog
        .databases
        .iter()
        .find(|database| database.info.id == new_schema_id)
    else {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_dbterror::CLASS_SCHEMA
                    .new_std(tidb_error::tidb::errcode::ErrBadDB)
                    .generate(format!("Unknown database '(Schema ID {new_schema_id})'")),
            ),
        ));
    };
    label_rules
        .push(
            label_rules::update_label_rules(
                new_database.info.name.lowercase(),
                &LabelRuleTable::from_table_info(&table),
                &old_rules,
                old_rules.old_rule_ids(),
                table.id,
            ),
            Some("failed to update the label rule to PD"),
        )
        .map_err(|error| cancel_ddl_job(active, error))?;
    tables.put(new_schema_id, table.clone_like_go());
    // Go `adjustForeignKeyChildTableInfoAfterRenameTable`.
    if !tidb_ddl_build::table_lifecycle::rename_changes_fk_reference(
        old_table_name.original(),
        info.new_table_name.original(),
    ) {
        return Ok(());
    }
    let referred = referred_foreign_keys(
        catalog,
        info.old_schema_name.lowercase(),
        old_table_name.lowercase(),
    );
    if referred.is_empty() {
        return Ok(());
    }
    let main_key = format!(
        "{}.{}",
        info.old_schema_name.lowercase(),
        old_table_name.lowercase()
    );
    if !helper.loaded.iter().any(|(key, ..)| *key == main_key) {
        helper.loaded.push((main_key, new_schema_id, table.id));
    }
    for (child_schema, child_table, child_fk) in referred {
        let child_key = format!("{child_schema}.{child_table}");
        let (schema_id, table_id) = match helper.loaded.iter().find(|(key, ..)| *key == child_key) {
            Some((_, schema_id, table_id)) => (*schema_id, *table_id),
            None => {
                let Some((database, child)) = catalog.find_table(&child_schema, &child_table)
                else {
                    continue;
                };
                helper.loaded.push((child_key, database.id, child.id));
                (database.id, child.id)
            }
        };
        let Some(mut child) = tables.get(schema_id, table_id) else {
            continue;
        };
        let Some(fk) = child
            .foreign_keys
            .iter_deref()
            .find(|fk| fk.read().name.lowercase() == child_fk)
        else {
            continue;
        };
        {
            let mut fk = fk.write();
            fk.ref_schema = new_database.info.name.clone();
            fk.ref_table = info.new_table_name.clone();
        }
        let _ = &mut child;
        tables.put(schema_id, child);
    }
    Ok(())
}

/// Go `onRenameTable` / `onRenameTables` and their finishing steps.
pub(super) fn plan_persisted_rename_table_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    _start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let single = active.job.type_ == ActionType::ACTION_RENAME_TABLE;
    let infos: Vec<tidb_model::RenameTableArgs> = if single {
        let args = tidb_model::get_rename_table_args(&mut active.job)
            .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
            .ok_or_else(|| DdlPlanError::Encode("RENAME TABLE job has nil args".to_owned()))?;
        let mut info = args.read().clone();
        info.new_schema_id = active.job.schema_id;
        info.table_id = active.job.table_id;
        vec![info]
    } else {
        let args = tidb_model::get_rename_tables_args(&mut active.job)
            .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
            .ok_or_else(|| DdlPlanError::Encode("RENAME TABLES job has nil args".to_owned()))?;
        let infos = args.read().rename_table_infos.get();
        infos.iter_deref().map(|info| info.read().clone()).collect()
    };
    let schema_version = catalog.schema_version + 1;
    let mut diff = SchemaDiff {
        version: schema_version,
        action_type: active.job.type_,
        schema_id: active.job.schema_id,
        ..SchemaDiff::default()
    };
    let mut mutations = Vec::new();
    let mut label_rules = LabelRuleSteps::new();
    if active.job.schema_state == SchemaState::PUBLIC {
        // Go `finishJobRenameTable(s)`: the old schema ids pinned to the new
        // ones, a second version, done.
        let mut finished = Vec::with_capacity(infos.len());
        for info in &infos {
            let table = catalog
                .databases
                .iter()
                .find(|database| database.info.id == info.new_schema_id)
                .and_then(|database| {
                    database
                        .tables
                        .iter()
                        .find(|table| table.id == info.table_id)
                });
            let Some(table) = table else {
                return Err(cancel_ddl_job(
                    active,
                    DdlPlanError::from(
                        tidb_dbterror::CLASS_SCHEMA
                            .new_std(tidb_error::tidb::errcode::ErrNoSuchTable)
                            .generate(format!(
                                "Table '{}' doesn't exist",
                                info.new_table_name.original()
                            )),
                    ),
                ));
            };
            finished.push(table.clone_like_go());
        }
        set_rename_diff(&mut diff, single, &infos, |info| info.new_schema_id, active);
        push_version(&mut mutations, &diff)?;
        active.job.last_schema_version = schema_version;
        if single {
            active.job.finish_table_job(
                JobState::DONE,
                SchemaState::PUBLIC,
                schema_version,
                Some(GoShared::new(finished.remove(0))),
            );
        } else {
            active.job.finish_multiple_table_job(
                JobState::DONE,
                SchemaState::PUBLIC,
                schema_version,
                &GoSharedPointerSlice::from(finished),
            );
        }
        let mut write = job_only_write(active.job.id, mutations);
        write.schema_version = schema_version;
        write.diff = diff;
        return Ok(PersistedDdlActionStep {
            write,
            update_raw_args: true,
            run_error: None,
            reorg: None,
        });
    }
    if single {
        // Go `checkTableNotExists(newSchemaID, tableName)`.
        let info = &infos[0];
        let Some(database) = catalog
            .databases
            .iter()
            .find(|database| database.info.id == info.new_schema_id)
        else {
            return Err(cancel_ddl_job(
                active,
                DdlPlanError::from(
                    tidb_dbterror::CLASS_SCHEMA
                        .new_std(tidb_error::tidb::errcode::ErrBadDB)
                        .generate(format!(
                            "Unknown database '(Schema ID {})'",
                            info.new_schema_id
                        )),
                ),
            ));
        };
        if database
            .tables
            .iter()
            .any(|table| table.name.lowercase() == info.new_table_name.lowercase())
        {
            return Err(cancel_ddl_job(
                active,
                DdlPlanError::from(
                    tidb_dbterror::CLASS_SCHEMA
                        .new_std(tidb_error::tidb::errcode::ErrTableExists)
                        .generate(format!(
                            "Table '{}' already exists",
                            info.new_table_name.original()
                        )),
                ),
            ));
        }
    }
    let mut tables = EvolvingTables::new(catalog);
    let mut helper = ForeignKeyHelper::default();
    for info in &infos {
        if !single {
            active.job.table_id = info.table_id;
            active.job.table_name = info.old_table_name.lowercase().to_owned().into();
        }
        rename_one(
            catalog,
            &mut tables,
            &mut helper,
            &mut label_rules,
            active,
            info,
            info.new_schema_id,
        )?;
    }
    mutations.extend(tables.mutations()?);
    set_rename_diff(&mut diff, single, &infos, |info| info.old_schema_id, active);
    // Go `SetSchemaDiffForMultiInfos` over the foreign-key helper's tables.
    let mut seen: Vec<i64> = std::iter::once(diff.table_id)
        .chain(
            diff.affected_options
                .iter_deref()
                .map(|affected| affected.read().table_id),
        )
        .collect();
    let mut affected: Vec<AffectedOption> = diff
        .affected_options
        .iter_deref()
        .map(|affected| affected.read().clone())
        .collect();
    for (_, schema_id, table_id) in &helper.loaded {
        if seen.contains(table_id) {
            continue;
        }
        seen.push(*table_id);
        affected.push(AffectedOption {
            schema_id: *schema_id,
            old_schema_id: *schema_id,
            table_id: *table_id,
            old_table_id: *table_id,
            ..AffectedOption::default()
        });
    }
    diff.affected_options = GoSharedPointerSlice::from(affected);
    push_version(&mut mutations, &diff)?;
    active.job.last_schema_version = schema_version;
    active.job.schema_state = SchemaState::PUBLIC;
    let mut write = job_only_write(active.job.id, mutations);
    write.schema_version = schema_version;
    write.diff = diff;
    write.label_rules = label_rules.steps;
    Ok(PersistedDdlActionStep {
        write,
        update_raw_args: true,
        run_error: None,
        reorg: None,
    })
}

/// Go `SetSchemaDiffForRenameTable(s)`, with `old_schema` giving each
/// entry's adjusted old schema id.
fn set_rename_diff(
    diff: &mut SchemaDiff,
    single: bool,
    infos: &[tidb_model::RenameTableArgs],
    old_schema: impl Fn(&tidb_model::RenameTableArgs) -> i64,
    active: &crate::ddl_job_table::ActiveDdlJob,
) {
    if single {
        diff.old_schema_id = old_schema(&infos[0]);
        diff.table_id = active.job.table_id;
        return;
    }
    let affected: Vec<AffectedOption> = infos
        .iter()
        .skip(1)
        .map(|info| AffectedOption {
            schema_id: info.new_schema_id,
            table_id: info.table_id,
            old_table_id: info.table_id,
            old_schema_id: old_schema(info),
            ..AffectedOption::default()
        })
        .collect();
    diff.table_id = infos[0].table_id;
    diff.schema_id = infos[0].new_schema_id;
    diff.old_schema_id = old_schema(&infos[0]);
    diff.affected_options = GoSharedPointerSlice::from(affected);
}

fn push_version(
    mutations: &mut Vec<BufferMutation>,
    diff: &SchemaDiff,
) -> Result<(), DdlPlanError> {
    mutations.push(BufferMutation::set(
        key::schema_version_kv_key(),
        value::encode_int_value(diff.version),
    )?);
    mutations.push(BufferMutation::set(
        key::schema_diff_kv_key(diff.version),
        value::serialize_schema_diff(diff)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
    )?);
    Ok(())
}
