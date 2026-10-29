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

//! A multi-schema ALTER as Go runs it: every specification's executor builds
//! its job against the original table and `appendToSubJobs` keeps it as a
//! sub-job; `multiSchemaChange` checks the set and queues one
//! `ActionMultiSchemaChange` job. The owner's `onMultiSchemaChange` steps
//! each sub-job, run as a proxy job through its own action handler, to its
//! revertible point; then steps them all past it at once under one schema
//! version; then finishes them one by one. A sub-job that fails before that
//! point rolls every sub-job back in reverse order.

use super::add_index::{action_step, table_step, AddIndexSubmission, ReorgOutcome};
use super::*;
use tidb_infoschema::cluster_catalog::MetaPairs;
use tidb_model::{GoAny, JobArgs, MultiSchemaInfo, SubJob};
use tidb_txnkv::transaction::BufferMutationOp;

/// A snapshot that reads one step's earlier sub-job writes first: Go's
/// sub-jobs share the step's transaction, so each sees the metadata the ones
/// before it left.
pub(super) struct MetaOverlay<'a> {
    inner: &'a mut dyn MetaSnapshot,
    staged: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl<'a> MetaOverlay<'a> {
    pub(super) fn new(inner: &'a mut dyn MetaSnapshot) -> Self {
        Self {
            inner,
            staged: BTreeMap::new(),
        }
    }

    fn stage(&mut self, mutations: &[BufferMutation]) {
        for mutation in mutations {
            match mutation.kind() {
                BufferMutationOp::Set => {
                    self.staged
                        .insert(mutation.key().to_vec(), Some(mutation.value().to_vec()));
                }
                BufferMutationOp::Delete => {
                    self.staged.insert(mutation.key().to_vec(), None);
                }
                BufferMutationOp::Lock => {}
            }
        }
    }

    fn merge(&self, pairs: MetaPairs, keep: impl Fn(&[u8]) -> bool) -> MetaPairs {
        let mut rows: BTreeMap<Vec<u8>, Vec<u8>> = pairs.into_iter().collect();
        for (key, value) in &self.staged {
            if !keep(key) {
                continue;
            }
            match value {
                Some(value) => {
                    rows.insert(key.clone(), value.clone());
                }
                None => {
                    rows.remove(key);
                }
            }
        }
        rows.into_iter().collect()
    }
}

impl MetaSnapshot for MetaOverlay<'_> {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        match self.staged.get(key) {
            Some(value) => Ok(value.clone()),
            None => self.inner.get(key),
        }
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        let pairs = self.inner.scan_prefix(prefix)?;
        Ok(self.merge(pairs, |key| key.starts_with(prefix)))
    }

    fn scan_range(&mut self, start: &[u8], end: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        let pairs = self.inner.scan_range(start, end)?;
        Ok(self.merge(pairs, |key| key >= start && key < end))
    }

    fn auto_id_client(&self) -> Option<&std::sync::Arc<dyn tidb_autoid::service_rpc::AutoIdServiceRpc>> {
        self.inner.auto_id_client()
    }
}

/// The standalone statement one specification is, which its executor builds
/// a job for.
fn single_statement(
    schema: &str,
    table: &str,
    action: &AlterColumnAction,
) -> Result<DdlStatement, DdlPlanError> {
    let (schema, table) = (schema.to_owned(), table.to_owned());
    Ok(match action.clone() {
        AlterColumnAction::Add {
            if_not_exists,
            column,
            position,
            context,
        } => DdlStatement::AddColumn {
            schema,
            table,
            if_not_exists,
            column,
            position,
            context,
        },
        AlterColumnAction::Modify {
            if_exists,
            column,
            position,
            context,
            rename_from,
            modify_column_type,
        } => DdlStatement::ModifyColumn {
            schema,
            table,
            column,
            position,
            context,
            rename_from,
            modify_column_type,
            if_exists,
        },
        AlterColumnAction::Rename { from, to } => DdlStatement::RenameColumn {
            schema,
            table,
            from,
            to,
        },
        AlterColumnAction::Drop {
            if_exists,
            column,
            allow_remove_auto_inc,
        } => DdlStatement::DropColumn {
            schema,
            table,
            if_exists,
            column,
            allow_remove_auto_inc,
        },
        AlterColumnAction::AddIndex {
            if_not_exists,
            index,
            expressions,
            condition,
            context,
        } => DdlStatement::CreateIndex {
            schema,
            table,
            if_not_exists,
            index,
            expressions,
            condition,
            context,
        },
        AlterColumnAction::DropIndex { if_exists, name } => DdlStatement::DropIndex {
            hypo: false,
            schema,
            table,
            index: name,
            if_exists,
        },
        AlterColumnAction::DropPrimaryKey => DdlStatement::DropPrimaryKey { schema, table },
        AlterColumnAction::AddForeignKey {
            definition,
            context,
        } => DdlStatement::AddForeignKey {
            schema,
            table,
            definition,
            context,
        },
        AlterColumnAction::DropForeignKey { name } => DdlStatement::DropForeignKey {
            schema,
            table,
            name,
        },
        AlterColumnAction::AddCheck {
            definition,
            context,
        } => DdlStatement::AddCheckConstraint {
            schema,
            table,
            definition,
            context,
        },
        AlterColumnAction::DropCheck { name, context } => DdlStatement::DropCheckConstraint {
            schema,
            table,
            name,
            context,
        },
        AlterColumnAction::AlterCheck {
            name,
            enforced,
            context,
        } => DdlStatement::AlterCheckConstraint {
            schema,
            table,
            name,
            enforced,
            context,
        },
        AlterColumnAction::RenameIndex { from, to } => DdlStatement::RenameIndex {
            schema,
            table,
            from,
            to,
        },
        AlterColumnAction::IndexVisibility { name, invisible } => {
            DdlStatement::AlterIndexVisibility {
                schema,
                table,
                index: name,
                invisible,
            }
        }
        AlterColumnAction::SetDefault {
            column,
            default_value,
            context,
        } => DdlStatement::SetColumnDefault {
            schema,
            table,
            column,
            default_value,
            context,
        },
        AlterColumnAction::RebaseAutoId { new_base, force } => {
            DdlStatement::RebaseAutoIncrementId {
                schema,
                table,
                new_base,
                force,
            }
        }
        AlterColumnAction::TableComment { comment } => DdlStatement::ModifyTableComment {
            schema,
            table,
            comment,
        },
        AlterColumnAction::NoSubJob(statement) => *statement,
        AlterColumnAction::Refused(error) => return Err(DdlPlanError::Admission(error)),
        AlterColumnAction::TableCharset {
            charset,
            collate,
            overwrite_columns,
        } => DdlStatement::ModifyTableCharsetAndCollate {
            schema,
            table,
            charset,
            collate,
            overwrite_columns,
        },
    })
}

/// Go `ErrRunMultiSchemaChanges`.
fn run_multi_schema_changes(action: ActionType) -> DdlPlanError {
    coded_refusal(
        &tidb_util::dbterror::ERR_RUN_MULTI_SCHEMA_CHANGES,
        format!("Unsupported multi schema change for {action}"),
    )
}

/// Go `AlterTable` over more than one specification: each executor's job
/// becomes a sub-job, then `multiSchemaChange` queues them as one.
#[allow(clippy::too_many_arguments)]
/// Go's executor functions append their notes and warnings to the
/// statement context as they run, so a later specification's refusal leaves
/// them in place.
fn publish_warnings(
    job_context: &dyn DdlSessionContext,
    warnings: Vec<tidb_util::context::SqlWarn>,
) {
    for warning in warnings {
        job_context.append_sql_warn(warning);
    }
}

pub fn prepare_multi_schema_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
    store_count: usize,
    tiflash_stores: &dyn Fn() -> Result<u64, String>,
    has_null: &mut dyn FnMut(&TableInfo, i64) -> Result<bool, DdlPlanError>,
) -> Result<Option<AddIndexSubmission>, DdlPlanError> {
    let DdlStatement::MultiSchemaChange {
        schema,
        table,
        actions,
    } = statement
    else {
        return Ok(None);
    };
    let catalog = load_cluster_catalog(snapshot)?;
    let (_, stored) = locate_table(&catalog, schema, table)?;
    let mut specs = Vec::new();
    let mut details = Vec::new();
    let mut anonymous_foreign_keys = 0;
    for action in actions {
        let built = match action {
            AlterColumnAction::Refused(error) => {
                return Err(DdlPlanError::Admission(error.clone()));
            }
            // Go: the specification's executor function runs its checks and
            // builds its job; `DoDDLJob` under `MultiSchemaInfo` refuses a
            // job type it has no sub-job for.
            AlterColumnAction::NoSubJob(statement) => {
                match crate::real_tikv_ddl::prepare_persisted_job_submission(
                    snapshot,
                    statement,
                    job_context,
                    &[],
                    start_ts,
                    upgrading,
                    min_job_id,
                    store_count,
                    tiflash_stores,
                    has_null,
                )? {
                    Some(AddIndexSubmission::Job(spec, _)) => {
                        return Err(run_multi_schema_changes(spec.job.type_));
                    }
                    Some(AddIndexSubmission::AlreadySatisfied {
                        detail,
                        warnings: notes,
                    }) => {
                        publish_warnings(job_context, notes);
                        details.push(detail);
                        continue;
                    }
                    None => {
                        return Err(DdlPlanError::Encode(format!(
                            "no job for the multi-schema specification {statement:?}"
                        )))
                    }
                }
            }
            AlterColumnAction::AddForeignKey {
                definition,
                context,
            } => {
                let name =
                    foreign_key::foreign_key_name(definition, stored, anonymous_foreign_keys);
                if definition.name.as_deref().is_none_or(str::is_empty) {
                    anonymous_foreign_keys += 1;
                }
                specs.extend(foreign_key::build_add_foreign_key_jobs(
                    snapshot,
                    &catalog,
                    schema,
                    table,
                    &name,
                    definition,
                    context,
                    job_context,
                    store_count,
                )?);
                continue;
            }
            // Go `CreateCheckConstraint` builds its job, which
            // `fillMultiSchemaInfo` has no sub-job for.
            AlterColumnAction::AddCheck { .. } => {
                prepare_check_constraint_job_submission(
                    snapshot,
                    &single_statement(schema, table, action)?,
                    start_ts,
                    upgrading,
                    min_job_id,
                )?;
                return Err(run_multi_schema_changes(
                    ActionType::ACTION_ADD_CHECK_CONSTRAINT,
                ));
            }
            AlterColumnAction::DropCheck { .. } | AlterColumnAction::AlterCheck { .. } => {
                prepare_check_constraint_job_submission(
                    snapshot,
                    &single_statement(schema, table, action)?,
                    start_ts,
                    upgrading,
                    min_job_id,
                )?;
                return Err(run_multi_schema_changes(
                    if matches!(action, AlterColumnAction::DropCheck { .. }) {
                        ActionType::ACTION_DROP_CHECK_CONSTRAINT
                    } else {
                        ActionType::ACTION_ALTER_CHECK_CONSTRAINT
                    },
                ));
            }
            AlterColumnAction::Add { .. } => add_column::build_add_column_job(
                snapshot,
                &catalog,
                &single_statement(schema, table, action)?,
                job_context,
            )?,
            AlterColumnAction::Drop { .. } => drop_column::build_drop_column_job(
                snapshot,
                &catalog,
                &single_statement(schema, table, action)?,
                job_context,
            )?,
            AlterColumnAction::Modify { .. } | AlterColumnAction::Rename { .. } => {
                modify_column::build_modify_column_job(
                    snapshot,
                    &catalog,
                    &single_statement(schema, table, action)?,
                    job_context,
                    store_count,
                    has_null,
                )?
            }
            AlterColumnAction::AddIndex { .. } => {
                let statement = single_statement(schema, table, action)?;
                let built =
                    add_index::build_add_index_job(
                        snapshot,
                        &catalog,
                        &statement,
                        store_count,
                        job_context,
                    )?;
                add_index::check_added_primary_key_holds_no_null(&catalog, &statement, has_null)?;
                built
            }
            AlterColumnAction::DropIndex { .. } | AlterColumnAction::DropPrimaryKey => {
                drop_index::build_drop_index_job(
                    snapshot,
                    &catalog,
                    &single_statement(schema, table, action)?,
                    job_context,
                )?
            }
            AlterColumnAction::DropForeignKey { .. }
            | AlterColumnAction::RenameIndex { .. }
            | AlterColumnAction::IndexVisibility { .. }
            | AlterColumnAction::SetDefault { .. }
            | AlterColumnAction::RebaseAutoId { .. }
            | AlterColumnAction::TableComment { .. }
            | AlterColumnAction::TableCharset { .. } => alter_table::build_alter_table_job(
                snapshot,
                &catalog,
                &single_statement(schema, table, action)?,
                job_context,
                true,
                tiflash_stores,
            )?,
        };
        match built {
            Some(AddIndexSubmission::Job(spec, executor_warnings)) => {
                publish_warnings(job_context, executor_warnings);
                specs.push(*spec);
            }
            Some(AddIndexSubmission::AlreadySatisfied {
                detail,
                warnings: notes,
            }) => {
                publish_warnings(job_context, notes);
                details.push(detail);
            }
            None => {
                return Err(DdlPlanError::Encode(format!(
                    "no job for the multi-schema specification {action:?}"
                )))
            }
        }
    }
    // Go `multiSchemaChange`: no sub-job, no job.
    if specs.is_empty() {
        return Ok(Some(AddIndexSubmission::AlreadySatisfied {
            detail: details.join("; "),
            warnings: Vec::new(),
        }));
    }
    let job = build_multi_schema_job(&catalog, schema, table, specs, job_context, store_count)?;
    submit_built_job(
        snapshot,
        &catalog,
        Some(AddIndexSubmission::Job(Box::new(job), Vec::new())),
        start_ts,
        upgrading,
        min_job_id,
    )
}

/// Go `appendToSubJobs` for each built job, then `multiSchemaChange`: the
/// parent job, `checkMultiSchemaInfo` and `mergeAddIndex`.
pub(super) fn build_multi_schema_job(
    catalog: &ClusterCatalog,
    schema: &str,
    table: &str,
    specs: Vec<crate::ddl_job_submit::JobSpec>,
    job_context: &dyn DdlSessionContext,
    store_count: usize,
) -> Result<crate::ddl_job_submit::JobSpec, DdlPlanError> {
    let (schema_id, stored) = locate_table(catalog, schema, table)?;
    let mut info = MultiSchemaInfo::default();
    let mut names = MultiSchemaNames::default();
    let mut sub_jobs = Vec::with_capacity(specs.len());
    let mut involving = Vec::new();
    for spec in specs {
        names.fill(&spec)?;
        if let JobArgsValue::AddForeignKey(Some(args)) = &spec.args {
            if let Some(fk) = &args.read().fk_info {
                let fk = fk.read();
                involving.push(tidb_model::InvolvingSchemaInfo {
                    database: fk.ref_schema.lowercase().to_owned().into(),
                    table: fk.ref_table.lowercase().to_owned().into(),
                    mode: tidb_model::InvolvingSchemaInfoMode::SHARED,
                    ..tidb_model::InvolvingSchemaInfo::default()
                });
            }
        }
        let reorg_type = spec
            .job
            .reorg_meta
            .as_ref()
            .map(|meta| meta.read().reorg_type)
            .unwrap_or_default();
        let mut sub = SubJob::default();
        sub.type_ = spec.job.type_;
        sub.job_args = GoAny::new(spec.args);
        sub.raw_args = spec.job.raw_args.clone();
        sub.schema_state = spec.job.schema_state;
        sub.snapshot_version = spec.job.snapshot_version;
        sub.revertible = true;
        sub.need_reorg = spec.job.need_reorg;
        sub.reorg_type = reorg_type;
        sub_jobs.push(sub);
    }
    names.check(stored)?;
    merge_add_index(&mut sub_jobs);
    let set_reorg_param = sub_jobs.iter().any(|sub| {
        matches!(
            sub.type_,
            ActionType::ACTION_ADD_INDEX
                | ActionType::ACTION_ADD_PRIMARY_KEY
                | ActionType::ACTION_MODIFY_COLUMN
        )
    });
    let set_dist_task_param = sub_jobs.iter().any(|sub| {
        matches!(
            sub.type_,
            ActionType::ACTION_ADD_INDEX | ActionType::ACTION_ADD_PRIMARY_KEY
        ) || (sub.type_ == ActionType::ACTION_MODIFY_COLUMN && sub.need_reorg)
    });
    info.sub_jobs = GoSharedPointerSlice::from_handles(
        sub_jobs.into_iter().map(GoShared::new).map(Some).collect(),
    );

    let mut job = session_job(job_context, ActionType::ACTION_MULTI_SCHEMA_CHANGE);
    job.schema_id = schema_id;
    job.table_id = stored.id;
    job.schema_name = schema.go_to_lower().into();
    job.table_name = stored.name.lowercase().to_owned().into();
    if !involving.is_empty() {
        involving.push(tidb_model::InvolvingSchemaInfo {
            database: schema.go_to_lower().into(),
            table: stored.name.lowercase().to_owned().into(),
            ..tidb_model::InvolvingSchemaInfo::default()
        });
        job.involving_schema_info = GoSharedSlice::from_vec(involving);
    }
    job.multi_schema_info = Some(GoShared::new(info));
    // Go `initJobReorgMetaFromVariables` and `AddSystemVars`.
    job.reorg_meta = Some(GoShared::new(
        add_index::init_job_reorg_meta_from_variables(
            job_context,
            schema,
            store_count,
            set_reorg_param,
            set_dist_task_param,
        ),
    ));
    let vars = job_context.ddl_reorg_vars();
    job.session_vars = Some(GoShared::new(std::collections::BTreeMap::from([
        (
            tidb_vardef::tidb_vars::TIDB_ENABLE_DDL_ANALYZE.to_owned(),
            vars.enable_ddl_analyze.clone().into(),
        ),
        (
            tidb_vardef::tidb_vars::TIDB_ANALYZE_VERSION.to_owned(),
            vars.analyze_version.clone().into(),
        ),
    ])));
    Ok(crate::ddl_job_submit::JobSpec {
        job,
        args: tidb_model::EmptyArgs::into_job_args_value(Some(GoShared::new(
            tidb_model::EmptyArgs::default(),
        ))),
        id_allocated: true,
    })
}

/// Go `MultiSchemaInfo`'s name lists, filled by `fillMultiSchemaInfo`.
#[derive(Default)]
struct MultiSchemaNames {
    add_columns: Vec<CiString>,
    drop_columns: Vec<CiString>,
    modify_columns: Vec<CiString>,
    position_columns: Vec<CiString>,
    relative_columns: Vec<CiString>,
    add_indexes: Vec<CiString>,
    drop_indexes: Vec<CiString>,
    alter_indexes: Vec<CiString>,
    add_foreign_keys: Vec<Vec<CiString>>,
}

impl MultiSchemaNames {
    /// Go `fillMultiSchemaInfo`.
    fn fill(&mut self, spec: &crate::ddl_job_submit::JobSpec) -> Result<(), DdlPlanError> {
        let after = |position: &Option<GoShared<tidb_ast::JobColumnPosition>>| {
            position.as_ref().and_then(|position| {
                let position = position.read();
                (position.tp == tidb_ast::JobColumnPosition::AFTER)
                    .then(|| {
                        position
                            .relative_column
                            .as_ref()
                            .map(|column| column.name.clone())
                    })
                    .flatten()
            })
        };
        match (spec.job.type_, &spec.args) {
            (ActionType::ACTION_ADD_COLUMN, JobArgsValue::TableColumn(Some(args))) => {
                let args = args.read();
                if let Some(column) = &args.col {
                    let column = column.read();
                    self.add_columns.push(column.name.clone());
                    for name in column.dependences.snapshot() {
                        self.relative_columns.push(CiString::new(name.to_string()));
                    }
                }
                if let Some(name) = after(&args.pos) {
                    self.position_columns.push(name);
                }
            }
            (ActionType::ACTION_DROP_COLUMN, JobArgsValue::TableColumn(Some(args))) => {
                self.drop_columns.push(args.read().col_name());
            }
            (
                ActionType::ACTION_DROP_INDEX | ActionType::ACTION_DROP_PRIMARY_KEY,
                JobArgsValue::ModifyIndex(Some(args)),
            ) => {
                if let Some(arg) = args.read().index_args.get(0) {
                    self.drop_indexes.push(arg.read().index_name.clone());
                }
            }
            (
                ActionType::ACTION_ADD_INDEX | ActionType::ACTION_ADD_PRIMARY_KEY,
                JobArgsValue::ModifyIndex(Some(args)),
            ) => {
                if let Some(arg) = args.read().index_args.get(0) {
                    let arg = arg.read();
                    self.add_indexes.push(arg.index_name.clone());
                    for part in arg.index_part_specifications.iter_deref() {
                        if let Some(column) = &part.read().column {
                            self.relative_columns.push(column.name.clone());
                        }
                    }
                    for column in arg.hidden_cols.iter_deref() {
                        for name in column.read().dependences.snapshot() {
                            self.relative_columns.push(CiString::new(name.to_string()));
                        }
                    }
                }
            }
            (ActionType::ACTION_RENAME_INDEX, JobArgsValue::ModifyIndex(Some(args))) => {
                let args = args.read();
                let name = |position: usize| {
                    args.index_args
                        .get(position)
                        .map(|arg| arg.read().index_name.clone())
                        .unwrap_or_default()
                };
                // Go `GetRenameIndexes`, kept in Go's order.
                self.add_indexes.push(name(0));
                self.drop_indexes.push(name(1));
            }
            (ActionType::ACTION_MODIFY_COLUMN, JobArgsValue::ModifyColumn(Some(args))) => {
                let args = args.read();
                let new_name = args
                    .column
                    .as_ref()
                    .map(|column| column.read().name.clone())
                    .unwrap_or_default();
                if new_name.lowercase() != args.old_column_name.lowercase() {
                    self.add_columns.push(new_name);
                    self.drop_columns.push(args.old_column_name.clone());
                } else {
                    self.modify_columns.push(new_name);
                }
                if let Some(name) = after(&args.position) {
                    self.position_columns.push(name);
                }
            }
            (ActionType::ACTION_SET_DEFAULT_VALUE, JobArgsValue::SetDefaultValue(Some(args))) => {
                if let Some(column) = args.read().column.get() {
                    self.modify_columns.push(column.read().name.clone());
                }
            }
            (
                ActionType::ACTION_ALTER_INDEX_VISIBILITY,
                JobArgsValue::AlterIndexVisibility(Some(args)),
            ) => {
                self.alter_indexes.push(args.read().index_name.get());
            }
            (
                ActionType::ACTION_REBASE_AUTO_ID
                | ActionType::ACTION_MODIFY_TABLE_COMMENT
                | ActionType::ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE
                | ActionType::ACTION_MODIFY_ENGINE_ATTRIBUTE,
                _,
            ) => {}
            (ActionType::ACTION_ADD_FOREIGN_KEY, JobArgsValue::AddForeignKey(Some(args))) => {
                if let Some(fk) = &args.read().fk_info {
                    let fk = fk.read();
                    self.add_foreign_keys.push(fk.cols.snapshot());
                }
            }
            // Nothing to verify for `DROP FOREIGN KEY`.
            (ActionType::ACTION_DROP_FOREIGN_KEY, _) => {}
            (action, _) => return Err(run_multi_schema_changes(action)),
        }
        Ok(())
    }

    /// Go `checkMultiSchemaInfo`.
    fn check(&self, stored: &TableInfo) -> Result<(), DdlPlanError> {
        let names = MultiSchemaInfo {
            add_columns: GoSharedSlice::from_vec(self.add_columns.clone()),
            drop_columns: GoSharedSlice::from_vec(self.drop_columns.clone()),
            modify_columns: GoSharedSlice::from_vec(self.modify_columns.clone()),
            position_columns: GoSharedSlice::from_vec(self.position_columns.clone()),
            relative_columns: GoSharedSlice::from_vec(self.relative_columns.clone()),
            add_indexes: GoSharedSlice::from_vec(self.add_indexes.clone()),
            drop_indexes: GoSharedSlice::from_vec(self.drop_indexes.clone()),
            alter_indexes: GoSharedSlice::from_vec(self.alter_indexes.clone()),
            ..MultiSchemaInfo::default()
        };
        // Go `checkOperateSameColAndIdx`.
        tidb_ddl_build::multi_schema_change::check_multi_schema_names(&names)
            .map_err(DdlPlanError::from_mysql_error)?;
        check_visible_column_cnt(stored, self.add_columns.len(), self.drop_columns.len())?;
        check_operate_drop_index_use_by_foreign_key(
            stored,
            &self.drop_indexes,
            &self.add_foreign_keys,
        )?;
        check_add_column_too_many_columns(
            (stored.columns.len() + self.add_columns.len()).saturating_sub(self.drop_columns.len()),
        )
    }
}

/// Go `mergeAddIndex`: more than one ADD INDEX become one sub-job at the
/// end, unless a foreign key needs the order kept.
fn merge_add_index(sub_jobs: &mut Vec<SubJob>) {
    if sub_jobs
        .iter()
        .any(|sub| sub.type_ == ActionType::ACTION_ADD_FOREIGN_KEY)
    {
        return;
    }
    let count = sub_jobs
        .iter()
        .filter(|sub| sub.type_ == ActionType::ACTION_ADD_INDEX)
        .count();
    if count <= 1 {
        return;
    }
    let mut merged: Option<SubJob> = None;
    let mut index_args = Vec::new();
    let mut kept = Vec::with_capacity(sub_jobs.len());
    for sub in sub_jobs.drain(..) {
        if sub.type_ != ActionType::ACTION_ADD_INDEX {
            kept.push(sub);
            continue;
        }
        if let Some(JobArgsValue::ModifyIndex(Some(args))) = sub.job_args_value() {
            index_args.extend(args.read().index_args.iter_handles());
        }
        if merged.is_none() {
            let mut first = sub.clone_without_args();
            first.raw_args = None;
            merged = Some(first);
        }
    }
    let mut merged = merged.expect("more than one ADD INDEX");
    merged.job_args = GoAny::new(JobArgsValue::ModifyIndex(Some(GoShared::new(
        tidb_model::ModifyIndexArgs {
            index_args: GoSharedPointerSlice::from_handles(index_args),
            op_type: tidb_model::IndexOp::ADD_INDEX,
            ..tidb_model::ModifyIndexArgs::default()
        },
    ))));
    kept.push(merged);
    *sub_jobs = kept;
}

/// What one sub-job's step came to (Go `runOneJobStep` on its proxy job).
struct SubJobStep {
    proxy: crate::ddl_job_table::ActiveDdlJob,
    step: PersistedDdlActionStep,
    /// `runOneJobStep`'s returned error.
    run_error: Option<DdlPlanError>,
}

/// Go `runOneJobStep` on a sub-job's proxy job.
fn run_sub_job_step(
    overlay: &mut MetaOverlay<'_>,
    catalog: &ClusterCatalog,
    active: &crate::ddl_job_table::ActiveDdlJob,
    sub: &GoShared<SubJob>,
    seq: usize,
    skip_version: bool,
    start_ts: u64,
    outcomes: &[ReorgOutcome],
    row_ids: Option<&dyn tidb_ddl_build::table_from_meta::TableAutoIds>,
) -> SubJobStep {
    let proxy_job = sub.read().to_proxy_job(&active.job, seq as i64);
    if skip_version {
        if let Some(info) = &proxy_job.multi_schema_info {
            info.write().skip_version = true;
        }
    }
    let mut proxy = active.with_job(proxy_job);
    if proxy.job.real_start_ts == 0 {
        proxy.job.real_start_ts = start_ts;
    }
    // The owner's reorganization outcomes belong to the sub-job that asked
    // for them.
    let outcome = outcomes
        .iter()
        .rev()
        .find(|outcome| outcome.sub_job == Some(seq as i32));
    if proxy.job.is_cancelling() {
        // Go `convertJob2RollbackJob`: the error goes on the proxy, and a
        // sub-job now rolling back or cancelled returns none.
        let ddl_job_id = proxy.job.id;
        let (mut step, error) = match plan_cancel_persisted_ddl_job(catalog, &mut proxy, start_ts) {
            Ok(mut step) => {
                let error = step.run_error.take();
                (step, error)
            }
            Err(error) => (job_control_step_for(ddl_job_id), Some(error)),
        };
        let run_error = error.and_then(|error| {
            record_ddl_cancellation_error(
                &mut proxy.job,
                &error,
                &tidb_vardef::ddl_error_count_limit,
            );
            (!matches!(proxy.job.state, JobState::ROLLINGBACK | JobState::CANCELLED))
                .then_some(error)
        });
        step.update_raw_args = proxy.job.is_rollingback();
        return SubJobStep {
            proxy,
            step,
            run_error,
        };
    }
    if !proxy.job.is_rollingback() {
        proxy.job.state = JobState::RUNNING;
    }
    match plan_persisted_action(
        overlay,
        catalog,
        &mut proxy,
        start_ts,
        outcome.map(std::slice::from_ref).unwrap_or_default(),
        row_ids,
    ) {
        Ok(mut step) => {
            // Go `countForError` on the proxy.
            let run_error = step.run_error.take();
            if let Some(error) = &run_error {
                record_ddl_plan_error(&mut proxy.job, error);
            }
            SubJobStep {
                proxy,
                step,
                run_error,
            }
        }
        Err(error) => {
            record_ddl_plan_error(&mut proxy.job, &error);
            let ddl_job_id = proxy.job.id;
            SubJobStep {
                proxy,
                step: job_control_step_for(ddl_job_id),
                run_error: Some(error),
            }
        }
    }
}

fn job_control_step_for(ddl_job_id: i64) -> PersistedDdlActionStep {
    PersistedDdlActionStep {
        write: job_only_write(ddl_job_id, Vec::new()),
        update_raw_args: false,
        run_error: None,
        reorg: None,
    }
}

/// Go `handleRevertibleException`: a sub-job that turned to rolling back
/// rolls the whole job back.
fn handle_revertible_exception(
    job: &mut Job,
    sub_jobs: &[GoShared<SubJob>],
    sub: &GoShared<SubJob>,
    error: Option<GoShared<TerrorError>>,
) {
    if sub.read().is_normal() {
        return;
    }
    job.state = JobState::ROLLINGBACK;
    job.error = error;
    flush_cancelling(sub_jobs);
}

/// Running sub-jobs turn cancelling and queued ones cancelled.
fn flush_cancelling(sub_jobs: &[GoShared<SubJob>]) {
    for sub in sub_jobs {
        let mut sub = sub.write();
        match sub.state {
            JobState::RUNNING => sub.state = JobState::CANCELLING,
            JobState::NONE | JobState::QUEUEING => sub.state = JobState::CANCELLED,
            _ => {}
        }
    }
}

/// Go `onMultiSchemaChange`.
pub(super) fn plan_persisted_multi_schema_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    outcomes: &[ReorgOutcome],
    row_ids: Option<&dyn tidb_ddl_build::table_from_meta::TableAutoIds>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let Some(info) = active.job.multi_schema_info.clone() else {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::Encode("multi-schema change has no MultiSchemaInfo".to_owned()),
        ));
    };
    let sub_jobs: Vec<GoShared<SubJob>> = info
        .read()
        .sub_jobs
        .iter_handles()
        .map(|sub| sub.expect("nil SubJob in MultiSchemaInfo.SubJobs"))
        .collect();
    let revertible = info.read().revertible;
    let mut overlay = MetaOverlay::new(snapshot);
    if revertible {
        if active.job.is_rollingback() {
            // Roll back or cancel the sub-jobs in reverse order.
            for (seq, sub) in sub_jobs.iter().enumerate().rev() {
                if sub.read().is_finished() {
                    continue;
                }
                let SubJobStep {
                    proxy,
                    mut step,
                    run_error,
                } = run_sub_job_step(
                    &mut overlay,
                    catalog,
                    active,
                    sub,
                    seq,
                    false,
                    start_ts,
                    outcomes,
                    row_ids,
                );
                if step.reorg.is_some() {
                    return Ok(step);
                }
                // Go `handleRollbackException`.
                let error = run_error.or_else(|| {
                    proxy
                        .job
                        .error
                        .as_ref()
                        .filter(|error| {
                            !tidb_util::dbterror::ERR_CANCELLED_DDL_JOB.equal(Some(&*error.read()))
                        })
                        .map(|error| DdlPlanError::from(error.read().clone()))
                });
                if let Some(error) = error {
                    step.run_error = Some(error);
                    step.update_raw_args = false;
                    return Ok(step);
                }
                sub.write()
                    .from_proxy_job(&proxy.job, step.write.schema_version);
                active.job.resume_reason = proxy.job.resume_reason.clone();
                step.update_raw_args = true;
                return Ok(step);
            }
            // The last rolling-back sub-job is done.
            active.job.state = JobState::ROLLBACK_DONE;
            return Ok(action_step(job_only_write(active.job.id, Vec::new())));
        }

        // Run the first sub-job that has not reached its revertible point.
        for (seq, sub) in sub_jobs.iter().enumerate() {
            {
                let sub = sub.read();
                if !sub.revertible || sub.is_finished() {
                    continue;
                }
            }
            let SubJobStep {
                proxy,
                mut step,
                run_error,
            } = run_sub_job_step(
                &mut overlay,
                catalog,
                active,
                sub,
                seq,
                false,
                start_ts,
                outcomes,
                row_ids,
            );
            if step.reorg.is_some() {
                return Ok(step);
            }
            sub.write()
                .from_proxy_job(&proxy.job, step.write.schema_version);
            active.job.resume_reason = proxy.job.resume_reason.clone();
            handle_revertible_exception(&mut active.job, &sub_jobs, sub, proxy.job.error.clone());
            step.update_raw_args = run_error.is_none() || active.job.is_rollingback();
            step.run_error = run_error;
            return Ok(step);
        }

        // Save the table and the sub-jobs for rolling back.
        let saved_info = checked_job_table(catalog, active, true)?.clone_like_go();
        // Go `doAnalyzeWithoutReorg`: no analyze for a job whose
        // `tidb_enable_ddl_analyze` is off, decided in a step of its own.
        if let Some(meta) = &active.job.reorg_meta {
            if meta.read().analyze_state == tidb_model::reorg::analyze_state::NONE {
                meta.write().analyze_state = tidb_model::reorg::analyze_state::SKIPPED;
                let schema_id = active.job.schema_id;
                let write = table_step(catalog, active, schema_id, &saved_info, true, start_ts)?;
                return Ok(action_step(write));
            }
        }
        let saved: Vec<SubJob> = sub_jobs.iter().map(|sub| sub.read().clone()).collect();
        // Step every sub-job past its revertible point at once, under the
        // one schema version the first of them publishes.
        let mut merged = job_only_write(active.job.id, Vec::new());
        let mut version = 0;
        let mut action_types = Vec::new();
        let mut sub_catalog: Option<ClusterCatalog> = None;
        for (seq, sub) in sub_jobs.iter().enumerate() {
            if sub.read().is_finished() {
                continue;
            }
            let step_catalog = sub_catalog.as_ref().unwrap_or(catalog);
            let SubJobStep {
                proxy,
                step,
                run_error,
            } = run_sub_job_step(
                &mut overlay,
                step_catalog,
                active,
                sub,
                seq,
                version != 0,
                start_ts,
                outcomes,
                row_ids,
            );
            if step.reorg.is_some() {
                return Ok(step);
            }
            let proxy_version = step.write.schema_version;
            if version == 0 && proxy_version != 0 {
                version = proxy_version;
            }
            sub.write().from_proxy_job(&proxy.job, proxy_version);
            active.job.resume_reason = proxy.job.resume_reason.clone();
            if run_error.is_some() || proxy.job.error.is_some() {
                for (earlier, saved) in sub_jobs.iter().zip(&saved).take(seq) {
                    *earlier.write() = saved.clone();
                }
                handle_revertible_exception(
                    &mut active.job,
                    &sub_jobs,
                    sub,
                    proxy.job.error.clone(),
                );
                // The table goes back to how it stood, under a new version.
                let schema_id = active.job.schema_id;
                let write = table_step(catalog, active, schema_id, &saved_info, true, start_ts)?;
                return Ok(action_step(write));
            }
            overlay.stage(&step.write.mutations);
            merge_write(&mut merged, step.write);
            action_types.push(sub.read().type_);
            // The next sub-job reads the metadata this one left.
            sub_catalog = Some(load_cluster_catalog(&mut overlay)?);
        }
        if action_types.len() > 1 {
            // Go `SetSchemaDiff` with every sub-job's action.
            let diff = SchemaDiff {
                version,
                action_type: active.job.type_,
                table_id: active.job.table_id,
                schema_id: active.job.schema_id,
                sub_action_types: action_types.into(),
                ..SchemaDiff::default()
            };
            let diff_key = key::schema_diff_kv_key(version);
            merged
                .mutations
                .retain(|mutation| mutation.key() != diff_key.as_slice());
            merged.mutations.push(BufferMutation::set(
                diff_key,
                value::serialize_schema_diff(&diff)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?);
            merged.diff = diff;
        }
        merged.schema_version = version;
        active.job.mark_non_revertible();
        return Ok(action_step(merged));
    }

    // Run the rest of the non-revertible sub-jobs one by one.
    for (seq, sub) in sub_jobs.iter().enumerate() {
        if sub.read().is_finished() {
            continue;
        }
        let SubJobStep {
            proxy,
            mut step,
            run_error,
        } = run_sub_job_step(
            &mut overlay,
            catalog,
            active,
            sub,
            seq,
            false,
            start_ts,
            outcomes,
            row_ids,
        );
        if step.reorg.is_some() {
            return Ok(step);
        }
        sub.write()
            .from_proxy_job(&proxy.job, step.write.schema_version);
        active.job.resume_reason = proxy.job.resume_reason.clone();
        step.update_raw_args = run_error.is_none();
        step.run_error = run_error;
        return Ok(step);
    }
    finish_multi_schema_job(catalog, active, &sub_jobs)
}

/// One sub-job's write joined to the step's: Go's sub-jobs write into the
/// step's one transaction.
fn merge_write(merged: &mut DdlWrite, write: DdlWrite) {
    merged.mutations.extend(write.mutations);
    if merged.diff.version == 0 {
        merged.diff = write.diff;
    }
    merged.label_rules.extend(write.label_rules);
    merged.warnings.extend(write.warnings);
    merged.placement_bundles.extend(write.placement_bundles);
    if merged.check_constraint_validation.is_none() {
        merged.check_constraint_validation = write.check_constraint_validation;
    }
}

/// Go `finishMultiSchemaJob`: done under the newest sub-job version,
/// publishing nothing new.
fn finish_multi_schema_job(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    sub_jobs: &[GoShared<SubJob>],
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let version = sub_jobs
        .iter()
        .map(|sub| sub.read().schema_version)
        .max()
        .unwrap_or(0);
    let info = checked_job_table(catalog, active, false)?.clone_like_go();
    active.job.finish_table_job(
        JobState::DONE,
        SchemaState::NONE,
        version,
        Some(GoShared::new(info)),
    );
    Ok(action_step(job_only_write(active.job.id, Vec::new())))
}

/// Go `rollingBackMultiSchemaChange`: past the revertible point the job runs
/// on; before it, every sub-job turns cancelling.
pub(super) fn plan_cancel_multi_schema(
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let Some(info) = active.job.multi_schema_info.clone() else {
        return Err(cancelled_ddl_job(active));
    };
    if !info.read().revertible {
        active.job.state = JobState::RUNNING;
        return Ok(job_control_step(&active.job));
    }
    let sub_jobs: Vec<GoShared<SubJob>> = info
        .read()
        .sub_jobs
        .iter_handles()
        .map(|sub| sub.expect("nil SubJob in MultiSchemaInfo.SubJobs"))
        .collect();
    flush_cancelling(&sub_jobs);
    active.job.state = JobState::ROLLINGBACK;
    Err(DdlPlanError::from(
        tidb_util::dbterror::ERR_CANCELLED_DDL_JOB
            .generate(tidb_error::tidb::errname::ErrCancelledDDLJob.raw),
    ))
}
