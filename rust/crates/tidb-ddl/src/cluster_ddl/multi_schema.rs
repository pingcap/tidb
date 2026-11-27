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

use super::add_index::{action_step, update_version_and_table_info, ReorgOutcome};
use super::*;
use tidb_model::{GoAny, JobArgs, MultiSchemaInfo, SubJob};

/// Go `ErrRunMultiSchemaChanges`.
fn run_multi_schema_changes(action: ActionType) -> DdlPlanError {
    coded_refusal(
        &tidb_dbterror::ERR_RUN_MULTI_SCHEMA_CHANGES,
        format!("Unsupported multi schema change for {action}"),
    )
}

/// Go `appendToSubJobs`: refuse unsupported sub-jobs when that specification runs.
pub(super) fn append_to_sub_jobs(
    specs: &mut Vec<tidb_ddl_jobsubmit::JobSpec>,
    spec: tidb_ddl_jobsubmit::JobSpec,
) -> Result<(), DdlPlanError> {
    MultiSchemaNames::default().fill(&spec)?;
    specs.push(spec);
    Ok(())
}

/// Go `appendToSubJobs` for each built job, then `multiSchemaChange`: the
/// parent job, `checkMultiSchemaInfo` and `mergeAddIndex`.
pub(super) fn build_multi_schema_job(
    catalog: &SessionExtendedInfoSchema,
    schema: &str,
    table: &str,
    specs: Vec<tidb_ddl_jobsubmit::JobSpec>,
    job_context: &dyn DdlSessionContext,
    store_count: usize,
) -> Result<tidb_ddl_jobsubmit::JobSpec, DdlPlanError> {
    let (schema_id, stored) = locate_table(catalog, schema, table)?;
    let stored = stored.read();
    let mut info = MultiSchemaInfo::default();
    let mut names = MultiSchemaNames::default();
    let mut sub_jobs = Vec::with_capacity(specs.len());
    // Go master `appendToSubJobs`: each sub-job's involving objects.
    let mut sub_involving = Vec::new();
    let mut foreign_key_refs = Vec::new();
    let mut sub_ops = Vec::with_capacity(specs.len());
    for spec in specs {
        names.fill(&spec)?;
        sub_ops.push((spec.job.type_, spec.args.clone()));
        append_involving_schema_info(&mut sub_involving, spec.job.involving_schema_info.snapshot());
        if let JobArgsValue::AddForeignKey(Some(args)) = &spec.args {
            if let Some(fk) = &args.read().fk_info {
                let fk = fk.read();
                foreign_key_refs.push(tidb_model::InvolvingSchemaInfo {
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
        sub.involving_schema_info = spec.job.involving_schema_info.clone();
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
    names.check(&stored)?;
    // Go `checkMultiSchemaInfo`'s `checkOperateBaseTableDependentMViewMinMaxIndexConstraints`.
    check_operate_base_table_dependent_mview_min_max_index_constraints(catalog, job_context, schema, &stored, &sub_ops)?;
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
    let validate_dist_task_at_submit = sub_jobs
        .iter()
        .any(|sub| matches!(sub.type_, ActionType::ACTION_ADD_INDEX | ActionType::ACTION_ADD_PRIMARY_KEY));
    info.sub_jobs = GoSharedPointerSlice::from_handles(
        sub_jobs.into_iter().map(GoShared::new).map(Some).collect(),
    );

    let mut job = session_job(job_context, ActionType::ACTION_MULTI_SCHEMA_CHANGE);
    job.schema_id = schema_id;
    job.table_id = stored.id;
    job.schema_name = schema.go_to_lower().into();
    job.table_name = stored.name.lowercase().to_owned().into();
    // Go master `multiSchemaChange`: the sub-jobs' objects, a log's base
    // table, the referenced tables of added foreign keys, then the table.
    let mut involving = Vec::new();
    append_involving_schema_info(&mut involving, sub_involving.clone());
    append_involving_schema_info(
        &mut involving,
        super::materialized_view::materialized_view_log_base_involving_schema_info(catalog, &schema.go_to_lower(), &stored),
    );
    append_involving_schema_info(&mut involving, foreign_key_refs);
    if !involving.is_empty() {
        append_involving_schema_info(
            &mut involving,
            vec![tidb_model::InvolvingSchemaInfo {
                database: schema.go_to_lower().into(),
                table: stored.name.lowercase().to_owned().into(),
                ..tidb_model::InvolvingSchemaInfo::default()
            }],
        );
        job.involving_schema_info = GoSharedSlice::from_vec(involving);
    }
    info.involving_schema_info = GoSharedSlice::from_vec(sub_involving);
    job.multi_schema_info = Some(GoShared::new(info));
    // Go `initJobReorgMetaFromVariables` and `AddSystemVars`.
    job.reorg_meta = Some(GoShared::new(
        add_index::init_job_reorg_meta_from_variables(
            job_context,
            schema,
            store_count,
            set_reorg_param,
            set_dist_task_param,
            validate_dist_task_at_submit,
        )?,
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
    Ok(tidb_ddl_jobsubmit::JobSpec {
        job,
        args: tidb_model::EmptyArgs::into_job_args_value(Some(GoShared::new(
            tidb_model::EmptyArgs::default(),
        ))),
        id_allocated: true,
    })
}

/// Go `appendInvolvingSchemaInfo`: adds each object not already listed; a
/// listed shared object that is involved exclusively becomes exclusive.
fn append_involving_schema_info(
    dst: &mut Vec<tidb_model::InvolvingSchemaInfo>,
    src: Vec<tidb_model::InvolvingSchemaInfo>,
) {
    for info in src {
        let found = dst.iter_mut().find(|existing| {
            existing.database == info.database
                && existing.table == info.table
                && existing.policy == info.policy
                && existing.resource_group == info.resource_group
        });
        match found {
            Some(existing) => {
                if existing.mode == tidb_model::InvolvingSchemaInfoMode::SHARED
                    && info.mode == tidb_model::InvolvingSchemaInfoMode::EXCLUSIVE
                {
                    existing.mode = tidb_model::InvolvingSchemaInfoMode::EXCLUSIVE;
                }
            }
            None => dst.push(info),
        }
    }
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
    fn fill(&mut self, spec: &tidb_ddl_jobsubmit::JobSpec) -> Result<(), DdlPlanError> {
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
        tidb_ddl_build::multi_schema_change::check_operate_same_col_and_idx(&names)
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

/// Go `buildEffectiveBaseTableInfoForMViewMinMaxIndexConstraints`: the base
/// table's indexes as the multi-schema change leaves them.
fn build_effective_base_table_info_for_mview_min_max_index_constraints(
    base: &TableInfo,
    sub_ops: &[(ActionType, JobArgsValue)],
) -> TableInfo {
    let mut effective = base.clone_like_go();
    let find = |effective: &TableInfo, name: &str| {
        effective.indices.iter_deref().find(|index| index.read().name.lowercase() == name.go_to_lower())
    };
    for (action, args) in sub_ops {
        match (*action, args) {
            (ActionType::ACTION_ADD_INDEX | ActionType::ACTION_ADD_PRIMARY_KEY, JobArgsValue::ModifyIndex(Some(args))) => {
                for arg in args.read().index_args.iter_deref() {
                    let arg = arg.read();
                    let mut index = IndexInfo {
                        name: arg.index_name.clone(),
                        state: SchemaState::PUBLIC,
                        ..IndexInfo::default()
                    };
                    if arg
                        .index_option
                        .as_ref()
                        .is_some_and(|option| option.read().visibility == 2)
                    {
                        index.invisible = true;
                    }
                    let mut columns = Vec::new();
                    let mut usable = true;
                    for spec in arg.index_part_specifications.iter_deref() {
                        let spec = spec.read();
                        let (Some(column), None) = (&spec.column, &spec.expr) else {
                            usable = false;
                            break;
                        };
                        let Some(info) = effective
                            .columns
                            .iter_deref()
                            .find(|info| info.read().name.lowercase() == column.name.lowercase())
                        else {
                            usable = false;
                            break;
                        };
                        columns.push(tidb_model::IndexColumn {
                            name: column.name.clone(),
                            offset: info.read().offset,
                            length: spec.length,
                            ..tidb_model::IndexColumn::default()
                        });
                    }
                    if usable {
                        index.columns = columns.into();
                        effective.indices.push_go(index);
                    }
                }
            }
            (ActionType::ACTION_DROP_INDEX | ActionType::ACTION_DROP_PRIMARY_KEY, JobArgsValue::ModifyIndex(Some(args))) => {
                for arg in args.read().index_args.iter_deref() {
                    let name = arg.read().index_name.lowercase().to_owned();
                    let kept: Vec<IndexInfo> = effective
                        .indices
                        .iter_deref()
                        .filter(|index| index.read().name.lowercase() != name)
                        .map(|index| index.read().clone_like_go())
                        .collect();
                    effective.indices = kept.into();
                }
                if *action == ActionType::ACTION_DROP_PRIMARY_KEY {
                    effective.pk_is_handle = false;
                }
            }
            (ActionType::ACTION_RENAME_INDEX, JobArgsValue::ModifyIndex(Some(args))) => {
                let args = args.read();
                let (Some(from), Some(to)) = (args.index_args.get(0), args.index_args.get(1)) else {
                    continue;
                };
                let (from, to) = (from.read().index_name.clone(), to.read().index_name.clone());
                if let Some(index) = find(&effective, from.original()) {
                    index.write().name = to;
                }
            }
            (ActionType::ACTION_ALTER_INDEX_VISIBILITY, JobArgsValue::AlterIndexVisibility(Some(args))) => {
                let args = args.read();
                if let Some(index) = find(&effective, args.index_name.get().original()) {
                    index.write().invisible = args.invisible.get();
                }
            }
            _ => {}
        }
    }
    effective
}

/// Go `checkOperateBaseTableDependentMViewMinMaxIndexConstraints`.
fn check_operate_base_table_dependent_mview_min_max_index_constraints(
    catalog: &SessionExtendedInfoSchema,
    job_context: &dyn DdlSessionContext,
    schema: &str,
    base: &TableInfo,
    sub_ops: &[(ActionType, JobArgsValue)],
) -> Result<(), DdlPlanError> {
    if base.materialized_view_base.as_ref().is_none_or(|info| info.read().mview_ids.is_empty()) {
        return Ok(());
    }
    let destructive = sub_ops.iter().any(|(action, args)| match (*action, args) {
        (ActionType::ACTION_DROP_INDEX | ActionType::ACTION_DROP_PRIMARY_KEY, _) => true,
        (ActionType::ACTION_ALTER_INDEX_VISIBILITY, JobArgsValue::AlterIndexVisibility(Some(args))) => {
            args.read().invisible.get()
        }
        _ => false,
    });
    if !destructive {
        return Ok(());
    }
    let effective = build_effective_base_table_info_for_mview_min_max_index_constraints(base, sub_ops);
    let check = |index_name: &str, op: &str| {
        crate::worker::executor_mview::check_base_table_dependent_mview_min_max_index_constraints_with_effective_table(
            Some(job_context),
            &|id| catalog.table_info_by_id(id).map(|table| table.read().clone_like_go()),
            schema,
            base,
            &effective,
            "",
            index_name,
            op,
        )
        .map_err(DdlPlanError::from_mysql_error)
    };
    for (action, args) in sub_ops {
        match (*action, args) {
            (ActionType::ACTION_DROP_INDEX | ActionType::ACTION_DROP_PRIMARY_KEY, JobArgsValue::ModifyIndex(Some(args))) => {
                for arg in args.read().index_args.iter_deref() {
                    check(arg.read().index_name.original(), "DROP INDEX")?;
                }
            }
            (ActionType::ACTION_ALTER_INDEX_VISIBILITY, JobArgsValue::AlterIndexVisibility(Some(args))) => {
                let args = args.read();
                if args.invisible.get() {
                    check(args.index_name.get().original(), "ALTER INDEX INVISIBLE")?;
                }
            }
            _ => {}
        }
    }
    Ok(())
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
    proxy: tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    step: PersistedDdlActionStep,
    /// `runOneJobStep`'s returned error.
    run_error: Option<DdlPlanError>,
}

/// Go `runOneJobStep` on a sub-job's proxy job.
/// Go `updateParentJobFromProxy`: what a proxy job's step found that belongs
/// to the parent rather than the sub-job. `ToProxyJob` gives each proxy a
/// copy of the parent's `ReorgMeta`, and `FromProxyJob` does not copy
/// `UseCloudStorage` back, so later proxies would revert to local sort.
fn update_parent_job_from_proxy(parent: &tidb_model::Job, proxy: &tidb_model::Job) {
    if let (Some(parent_meta), Some(proxy_meta)) = (&parent.reorg_meta, &proxy.reorg_meta) {
        if proxy_meta.read().use_cloud_storage {
            parent_meta.write().use_cloud_storage = true;
        }
    }
}

fn run_sub_job_step(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    sub: &GoShared<SubJob>,
    seq: usize,
    skip_version: bool,
    start_ts: u64,
    outcomes: &[ReorgOutcome],
    worker: &DdlStepWorker,
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
        let session = txn.session();
        let region_store = txn.region_store();
        let cancelled = run_in_job_txn_with(txn, start_ts, session, region_store, |txn| {
            plan_cancel_persisted_ddl_job(txn, catalog, &mut proxy, start_ts)
        });
        let (mut step, error) = match cancelled {
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
        txn,
        catalog,
        &mut proxy,
        start_ts,
        outcome.map(std::slice::from_ref).unwrap_or_default(),
        // Go: a sub-job runs on the parent's `w.sess`.
        worker,
    ) {
        Ok(mut step) => {
            // Go `countForError` on the proxy.
            let run_error = step
                .run_error
                .take()
                .and_then(|error| count_for_error_on_proxy(&mut proxy.job, error));
            SubJobStep {
                proxy,
                step,
                run_error,
            }
        }
        Err(error) => {
            // Go `countForError` on the proxy.
            let run_error = count_for_error_on_proxy(&mut proxy.job, error);
            let ddl_job_id = proxy.job.id;
            SubJobStep {
                proxy,
                step: job_control_step_for(ddl_job_id),
                run_error,
            }
        }
    }
}

/// Go `countForError` on a proxy job: the error is recorded on it, and a
/// proxy the action cancelled ("DDL job is cancelled normally") returns no
/// error, so the parent goes on with the sub-job cancelled.
fn count_for_error_on_proxy(job: &mut Job, error: DdlPlanError) -> Option<DdlPlanError> {
    if !record_ddl_action_error(job, &error) {
        return Some(error);
    }
    (job.state != JobState::CANCELLED).then_some(error)
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

/// Go `promoteProxyKVDiskFullPause`: a proxy job the system paused for full
/// TiKV disks pauses the parent job, and the sub-job keeps the state it had
/// before the step so the resumed proxy is not recreated pausing.
fn promote_proxy_kv_disk_full_pause(
    parent_job: &mut Job,
    sub_job: &mut SubJob,
    prev_sub_state: JobState,
    proxy_job: &Job,
) -> bool {
    if !proxy_job.is_pausing_or_paused_by_system_for_kv_disk_full() {
        return false;
    }
    sub_job.state = prev_sub_state;
    parent_job.state = proxy_job.state;
    parent_job.admin_operator = proxy_job.admin_operator;
    parent_job.clear_resume_reason();
    parent_job.pause_reason = proxy_job
        .pause_reason
        .as_ref()
        .map(|reason| GoShared::new(reason.read().clone()));
    parent_job.error = proxy_job.error.clone().or_else(|| {
        let message = parent_job
            .pause_reason
            .as_ref()
            .map_or_else(String::new, |reason| reason.read().message.to_string());
        Some(GoShared::new(
            tidb_dbterror::ERR_DDL_AUTO_PAUSED_BY_KV_DISK_FULL
                .fast_gen_by_args(&[parent_job.id.into(), message.into()]),
        ))
    });
    true
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
pub(super) fn on_multi_schema_change(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    outcomes: &[ReorgOutcome],
    worker: &DdlStepWorker,
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
                    txn,
                    catalog,
                    active,
                    sub,
                    seq,
                    false,
                    start_ts,
                    outcomes,
                    worker,
                );
                update_parent_job_from_proxy(&active.job, &proxy.job);
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
                            !tidb_dbterror::ERR_CANCELLED_DDL_JOB.equal(Some(&*error.read()))
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
            let prev_sub_state = sub.read().state;
            let SubJobStep {
                proxy,
                mut step,
                run_error,
            } = run_sub_job_step(
                txn,
                catalog,
                active,
                sub,
                seq,
                false,
                start_ts,
                outcomes,
                worker,
            );
            update_parent_job_from_proxy(&active.job, &proxy.job);
            if step.reorg.is_some() {
                return Ok(step);
            }
            sub.write()
                .from_proxy_job(&proxy.job, step.write.schema_version);
            active.job.resume_reason = proxy.job.resume_reason.clone();
            if promote_proxy_kv_disk_full_pause(
                &mut active.job,
                &mut sub.write(),
                prev_sub_state,
                &proxy.job,
            ) {
                step.update_raw_args = true;
                return Ok(step);
            }
            handle_revertible_exception(&mut active.job, &sub_jobs, sub, proxy.job.error.clone());
            step.update_raw_args = run_error.is_none() || active.job.is_rollingback();
            step.run_error = run_error;
            return Ok(step);
        }

        // Save the table and the sub-jobs for rolling back.
        let saved_info = checked_job_table(txn, active, true)?.read().clone_like_go();
        let finished = super::analyze::do_analyze_without_reorg(txn, &active.job, &saved_info);
        if !finished {
            let schema_id = active.job.schema_id;
            let write = update_version_and_table_info(txn, active, schema_id, &saved_info, true, start_ts)?;
            return Ok(action_step(write));
        }
        // "Save every sub-job, including finished no-ops, before running the
        // batch."
        let saved: Vec<SubJob> = sub_jobs.iter().map(|sub| sub.read().clone()).collect();
        // Go master: the batch's diff starts from the involving tables, and
        // "execution can discover related tables absent from the submitted
        // involving set. Collect updates even from sub-jobs that skip schema
        // version generation" (`jobCtx.multiSchemaDiff`).
        let involving_infos = collect_affected_table_infos_from_involving(catalog, &active.job);
        let step_ctx = std::sync::Arc::clone(&txn.session().step_ctx);
        *step_ctx.multi_schema_diff.lock().unwrap() = Some(Vec::new());
        let take_multi_schema_infos = || step_ctx.multi_schema_diff.lock().unwrap().take().unwrap_or_default();
        // Step every sub-job past its revertible point at once, under the
        // one schema version the first of them publishes.
        let mut merged = job_only_write(active.job.id, Vec::new());
        let mut version = 0;
        let mut action_types = Vec::new();
        // The sub-jobs' writes, which reach the step only when every one of
        // them stepped (a failed one restores the table alone).
        let session = txn.session();
        let region_store = txn.region_store();
        let mut subs = JobTxn::new(txn, start_ts, session).with_region_store(region_store);
        for (seq, sub) in sub_jobs.iter().enumerate() {
            if sub.read().is_finished() {
                continue;
            }
            let prev_sub_state = sub.read().state;
            let SubJobStep {
                proxy,
                step,
                run_error,
            } = run_sub_job_step(
                &mut subs,
                catalog,
                active,
                sub,
                seq,
                version != 0,
                start_ts,
                outcomes,
                worker,
            );
            // Go's `(job, &proxyJob)`, as a snapshot the callback reads.
            tidb_failpoint::inject_call(
                "github.com/pingcap/tidb/pkg/ddl/beforeBatchedMultiSchemaParentJobUpdate",
                &mut (active.job.clone(), proxy.job.clone()),
            );
            update_parent_job_from_proxy(&active.job, &proxy.job);
            tidb_failpoint::inject_call(
                "github.com/pingcap/tidb/pkg/ddl/afterBatchedMultiSchemaParentJobUpdate",
                &mut (active.job.clone(), proxy.job.clone()),
            );
            if step.reorg.is_some() {
                // Go `onMultiSchemaChange` keeps this loop and its metadata
                // transaction alive while runOneJobStep waits for reorg.
                // Our reorg request unwinds the planner and discards `subs`;
                // restore the earlier sub-jobs too, so the retry replays their
                // writes instead of skipping a now-finished delivered sub-job.
                // The requesting proxy has not been copied back yet.
                for (earlier, saved) in sub_jobs.iter().zip(&saved).take(seq) {
                    *earlier.write() = saved.clone();
                }
                take_multi_schema_infos();
                return Ok(step);
            }
            let proxy_version = step.write.schema_version;
            if version == 0 && proxy_version != 0 {
                version = proxy_version;
            }
            sub.write().from_proxy_job(&proxy.job, proxy_version);
            active.job.resume_reason = proxy.job.resume_reason.clone();
            if promote_proxy_kv_disk_full_pause(
                &mut active.job,
                &mut sub.write(),
                prev_sub_state,
                &proxy.job,
            ) {
                // The step keeps what the sub-jobs wrote so far, under the
                // version the first of them published.
                subs.stage(&step.write.mutations);
                merge_write(&mut merged, step.write);
                let written = subs.into_mutations()?;
                txn.stage(&written);
                merged.schema_version = version;
                take_multi_schema_infos();
                return Ok(action_step(merged));
            }
            if run_error.is_some() || proxy.job.error.is_some() {
                take_multi_schema_infos();
                // Go keeps the failed sub-job's object, whose state decides
                // whether the parent rolls back, apart from the restored list.
                let failed = GoShared::new(sub.read().clone());
                for (earlier, saved) in sub_jobs.iter().zip(&saved).take(seq + 1) {
                    *earlier.write() = saved.clone();
                }
                // "The failed sub-job's state still determines whether to
                // roll back the parent; its persisted state must match the
                // restored transaction."
                handle_revertible_exception(&mut active.job, &sub_jobs, &failed, proxy.job.error.clone());
                // "Earlier sub-jobs may have updated related tables. Discard
                // the entire transaction before persisting the error and
                // restored jobs. External effects such as allocator rebases
                // cannot be undone here."
                drop(subs);
                txn.set_must_rollback_txn_on_error();
                let error = run_error.unwrap_or_else(|| {
                    DdlPlanError::from(proxy.job.error.as_ref().expect("a failed proxy job").read().clone())
                });
                let mut step = action_step(job_only_write(active.job.id, Vec::new()));
                step.update_raw_args = active.job.is_rollingback();
                step.run_error = Some(error);
                return Ok(step);
            }
            // Go's sub-jobs share the step's transaction: the next one reads
            // the metadata this one wrote.
            subs.stage(&step.write.mutations);
            merge_write(&mut merged, step.write);
            action_types.push(sub.read().type_);
        }
        let written = subs.into_mutations()?;
        txn.stage(&written);
        let collected_infos = take_multi_schema_infos();
        if action_types.len() > 1 {
            // "Publish every related table updated under the shared schema
            // version."
            let mut diff = SchemaDiff {
                version,
                action_type: active.job.type_,
                table_id: active.job.table_id,
                schema_id: active.job.schema_id,
                sub_action_types: action_types.into(),
                ..SchemaDiff::default()
            };
            super::materialized_view::set_schema_diff_for_multi_infos(&mut diff, &involving_infos);
            super::materialized_view::set_schema_diff_for_multi_infos(&mut diff, &collected_infos);
            let meta = txn.meta();
            if let Err(error) = meta.set_schema_diff(&diff) {
                let error = job_meta_error(&meta, error);
                // Go master: restore the processed sub-jobs and discard the
                // whole transaction, so the batch retries.
                for (sub, saved) in sub_jobs.iter().zip(&saved) {
                    *sub.write() = saved.clone();
                }
                txn.set_must_rollback_txn_on_error();
                return Err(error);
            }
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
        let prev_sub_state = sub.read().state;
        let SubJobStep {
            proxy,
            mut step,
            run_error,
        } = run_sub_job_step(
            txn,
            catalog,
            active,
            sub,
            seq,
            false,
            start_ts,
            outcomes,
            worker,
        );
        update_parent_job_from_proxy(&active.job, &proxy.job);
        if step.reorg.is_some() {
            return Ok(step);
        }
        sub.write()
            .from_proxy_job(&proxy.job, step.write.schema_version);
        active.job.resume_reason = proxy.job.resume_reason.clone();
        if promote_proxy_kv_disk_full_pause(
            &mut active.job,
            &mut sub.write(),
            prev_sub_state,
            &proxy.job,
        ) {
            step.update_raw_args = true;
            return Ok(step);
        }
        step.update_raw_args = run_error.is_none();
        step.run_error = run_error;
        return Ok(step);
    }
    finish_multi_schema_job(txn, active, &sub_jobs)
}

/// One sub-job's write joined to the step's; its mutations are already the
/// step transaction's (Go's sub-jobs write into the step's one transaction).
fn merge_write(merged: &mut DdlWrite, write: DdlWrite) {
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
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    sub_jobs: &[GoShared<SubJob>],
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let version = sub_jobs
        .iter()
        .map(|sub| sub.read().schema_version)
        .max()
        .unwrap_or(0);
    let info = checked_job_table(txn, active, false)?.read().clone_like_go();
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
pub(super) fn rolling_back_multi_schema_change(
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
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
        tidb_dbterror::ERR_CANCELLED_DDL_JOB
            .generate(tidb_errno::errname::ErrCancelledDDLJob.raw),
    ))
}

#[cfg(test)]
mod promote_proxy_kv_disk_full_pause_tests {
    use super::*;
    use tidb_model::job::{JOB_PAUSE_REASON_KV_DISK_FULL, JOB_RESUME_REASON_KV_DISK_FULL};
    use tidb_model::AdminCommandOperator;

    fn job(id: i64, state: JobState) -> Job {
        let mut job = Job::default();
        job.id = id;
        job.state = state;
        job
    }

    fn sub_job(state: JobState) -> SubJob {
        let mut sub = SubJob::default();
        sub.state = state;
        sub
    }

    fn paused_proxy(state: JobState) -> Job {
        let mut proxy = job(7, state);
        proxy.admin_operator = AdminCommandOperator::BY_SYSTEM;
        proxy.set_pause_reason(JOB_PAUSE_REASON_KV_DISK_FULL, "store 1 disk full");
        proxy
    }

    /// Go `promoteProxyKVDiskFullPause` (pkg/ddl/multi_schema_change.go): the
    /// parent takes the proxy's state, operator, pause reason and error, drops
    /// its resume reason, and the sub-job returns to its pre-step state.
    #[test]
    fn a_kv_disk_full_paused_proxy_pauses_the_parent_like_go() {
        for state in [JobState::PAUSING, JobState::PAUSED] {
            let proxy = paused_proxy(state);
            let mut parent = job(42, JobState::RUNNING);
            parent.set_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL);
            let mut sub = sub_job(state);
            assert!(promote_proxy_kv_disk_full_pause(
                &mut parent,
                &mut sub,
                JobState::RUNNING,
                &proxy
            ));
            assert_eq!(sub.state, JobState::RUNNING);
            assert_eq!(parent.state, state);
            assert_eq!(parent.admin_operator, AdminCommandOperator::BY_SYSTEM);
            assert!(parent.resume_reason.is_none());
            assert!(parent.has_pause_reason(JOB_PAUSE_REASON_KV_DISK_FULL));
            // No proxy error: `ErrDDLAutoPausedByKVDiskFull` over the parent's
            // id and the pause message.
            let error = parent.error.as_ref().unwrap().read().clone();
            assert!(tidb_dbterror::ERR_DDL_AUTO_PAUSED_BY_KV_DISK_FULL.equal(Some(&error)));
            assert_eq!(
                error.to_string(),
                "[ddl:8276]Job [42] has been paused by TiDB because a storage node does not have enough disk space: store 1 disk full"
            );
        }
    }

    #[test]
    fn the_proxys_own_error_is_the_parents_like_go() {
        let mut proxy = paused_proxy(JobState::PAUSED);
        let own = GoShared::new(tidb_dbterror::ERR_CANCELLED_DDL_JOB.fast_gen_by_args(&[]));
        proxy.error = Some(own.clone());
        let mut parent = Job::default();
        let mut sub = SubJob::default();
        assert!(promote_proxy_kv_disk_full_pause(
            &mut parent,
            &mut sub,
            JobState::QUEUEING,
            &proxy
        ));
        assert_eq!(*parent.error.unwrap().read(), *own.read());
        assert_eq!(sub.state, JobState::QUEUEING);
    }

    /// Any other proxy leaves the parent and the sub-job alone.
    #[test]
    fn another_pause_or_state_is_not_promoted_like_go() {
        let mut by_user = paused_proxy(JobState::PAUSED);
        by_user.admin_operator = AdminCommandOperator::BY_END_USER;
        let mut no_reason = paused_proxy(JobState::PAUSED);
        no_reason.clear_pause_reason();
        let running = paused_proxy(JobState::RUNNING);
        for proxy in [by_user, no_reason, running] {
            let mut parent = job(42, JobState::RUNNING);
            let mut sub = sub_job(JobState::DONE);
            assert!(!promote_proxy_kv_disk_full_pause(
                &mut parent,
                &mut sub,
                JobState::RUNNING,
                &proxy
            ));
            assert_eq!(parent.state, JobState::RUNNING);
            assert_eq!(sub.state, JobState::DONE);
            assert!(parent.error.is_none() && parent.pause_reason.is_none());
        }
    }
}

/// Go master `collectAffectedTableInfosFromInvolving`: the tables the job's
/// involving objects name (other than its own), from the latest schema.
fn collect_affected_table_infos_from_involving(catalog: &SessionExtendedInfoSchema, job: &tidb_model::Job) -> Vec<(i64, i64)> {
    let mut seen = vec![job.table_id];
    let mut infos = Vec::new();
    for involving in job.get_involving_schema_info().snapshot() {
        let (database, table) = (involving.database.to_string(), involving.table.to_string());
        if database.is_empty() || table.is_empty() || table == tidb_model::job::INVOLVING_ALL {
            continue;
        }
        let Some((schema, table)) = catalog.table_info_by_name(&database, &table) else {
            continue;
        };
        let table_id = table.read().id;
        if seen.contains(&table_id) {
            continue;
        }
        seen.push(table_id);
        infos.push((schema.read().id, table_id));
    }
    infos
}
