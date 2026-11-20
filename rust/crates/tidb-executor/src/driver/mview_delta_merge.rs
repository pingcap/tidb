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

//! Go master `MViewDeltaMergeAggExec` (`pkg/executor/mviewdeltamergeagg`)
//! and its builder (`mview_delta_merge_agg_builder.go`): FAST refresh's
//! merge source -- the log window's per-group deltas left-joined to the
//! view -- read chunk by chunk, each aggregate merged in dependency order
//! (COUNT(*) first), each group inserted into, updated in or deleted from the
//! view (and its MV log, when it has one) by its new COUNT(*).
//!
//! Go merges on a reader / workers / writer pipeline; this merges and writes
//! each chunk in turn, which writes the same rows.


use tidb_planner::common_plans::MViewDeltaMerge;
use tidb_planner::mview::{AggInfo, AggKind, BuildResult};
use tidb_planner_core_operator_physicalop::PhysicalPlan;

use super::{DriverError, SessionExtendedInfoSchema};

fn plain(message: impl Into<String>) -> DriverError {
    DriverError::Mysql(tidb_terror::MysqlError::unknown(message.into()))
}

/// Go master `mergeWriterStats`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeltaMergeWriterStats {
    /// Chunks written.
    pub chunks: i64,
    /// Row operations planned.
    pub row_ops: i64,
    /// Updates that changed nothing.
    pub noop_rows: i64,
    /// Rows inserted into the view.
    pub insert_rows: i64,
    /// Rows updated in the view.
    pub update_rows: i64,
    /// Rows deleted from the view.
    pub delete_rows: i64,
}

impl DeltaMergeWriterStats {
    /// Go `affectedRows`.
    #[must_use]
    pub fn affected_rows(self) -> u64 {
        u64::try_from(self.insert_rows + self.update_rows + self.delete_rows).unwrap_or_default()
    }

    /// Go `stmtMessage`.
    #[must_use]
    pub fn stmt_message(self) -> String {
        format!("Rows inserted: {}  Updated: {}  Deleted: {}", self.insert_rows, self.update_rows, self.delete_rows)
    }
}

/// Go `mview.Build` over the plan's window, with the catalog's view, base
/// table and log.
///
/// # Errors
///
/// The view is missing or malformed for FAST refresh.
pub fn build_merge_source(
    plan: &MViewDeltaMerge,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<BuildResult, DriverError> {
    let Some((_, mv)) = catalog.table_info_by_name(&plan.database, &plan.name) else {
        return Err(DriverError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .gen_with_stack_by_args(&[(&plan.database).into(), (&plan.name).into()]),
        ));
    };
    let mv = mv.read().clone_like_go();
    // Go master `parseSelectFromSQL`: a new parser (its own `ModeNone` SQL
    // mode) with the session's parser configuration and connection charset.
    let (charset, collation) = ctx.connection_charset_info();
    let parse = |sql: &str| {
        tidb_parser::parse_with_config_and_connection(sql, ctx.parser_config(), charset, collation)
            .map_err(|error| error.compatibility_message(sql))
    };
    tidb_planner::mview::build(&parse, catalog, &mv, plan.options, None).map_err(plain)
}

/// Go master `formatMViewDeltaMergeAggDependency`.
fn format_agg_dependency(info: &AggInfo) -> String {
    let name = match info.kind {
        AggKind::CountStar => "count(*)".to_owned(),
        kind => {
            let kind = match kind {
                AggKind::Count => "count",
                AggKind::Sum => "sum",
                AggKind::Min => "min",
                AggKind::Max => "max",
                AggKind::CountStar => unreachable!(),
            };
            if info.arg_col_name.is_empty() {
                kind.to_owned()
            } else {
                format!("{kind}({})", info.arg_col_name)
            }
        }
    };
    let deps = info.dependencies.iter().map(usize::to_string).collect::<Vec<_>>().join(",");
    format!("{name}@{}->[{deps}]", info.mv_offset)
}

/// Go master `MViewDeltaMerge.ExplainInfo`.
fn explain_info(build: &BuildResult, full_update: bool) -> String {
    let mut info = format!("agg_deps:[{}]", build.agg_infos.iter().map(format_agg_dependency).collect::<Vec<_>>().join(", "));
    if full_update {
        info.push_str(", full_update:index_lookup");
    }
    info
}

/// Go master `mvFullUpdateLookupTemplate`: the MIN / MAX fallback's lookup
/// child, the template IndexJoin it came from, and how its output maps onto
/// the view.
pub struct FullUpdateLookup {
    inner: PhysicalPlan,
    join: tidb_planner_core_operator_physicalop::PhysicalIndexJoin,
    /// Go `KeyResultColIdxes`: each join key's column in the inner output.
    pub(crate) key_result_col_idxes: Vec<usize>,
    /// Go `OutputMVOffsets`: the view offset of each inner output column.
    pub(crate) output_mv_offsets: Vec<usize>,
    /// Go `FullUpdateSnapshot`: an AS OF target's schema and reader.
    snapshot: Option<FullUpdateSnapshot>,
}

impl FullUpdateLookup {
    /// Go `FullUpdateInnerColumnCount`.
    pub(crate) fn inner_column_count(&self) -> usize {
        self.inner.schema().map_or(0, |schema| schema.columns.len())
    }

    /// Go `FullUpdateKeyOff2IdxOff`.
    pub(crate) fn key_off2_idx_off(&self) -> &[i64] {
        &self.join.key_off2_idx_off
    }

    /// Go `FullUpdateSnapshot`.
    pub(crate) fn full_update_snapshot(&self) -> Option<&FullUpdateSnapshot> {
        self.snapshot.as_ref()
    }
}

/// Go `findMVFullUpdateIndexJoinTemplatePlan`: the template's IndexJoin,
/// or the one an IndexHashJoin embeds (the Rust planner has no
/// IndexMergeJoin).
fn find_index_join(plan: &PhysicalPlan) -> Option<&tidb_planner_core_operator_physicalop::PhysicalIndexJoin> {
    match plan {
        PhysicalPlan::IndexJoin(join) => Some(join),
        PhysicalPlan::IndexHashJoin(join) => Some(&join.physical_index_join),
        _ => plan.children().iter().find_map(find_index_join),
    }
}

/// Go master `extractMVFullUpdateLookupTemplate` and
/// `deriveMVFullUpdateOutputMVOffsetsByInnerSchema`.
fn extract_full_update_lookup_template(
    lookup_plan: &PhysicalPlan,
    build: &BuildResult,
) -> Result<FullUpdateLookup, DriverError> {
    let join = find_index_join(lookup_plan)
        .ok_or_else(|| plain("mview full-update lookup template: expected index join plan but not found"))?;
    let inner = join
        .base
        .children()
        .get(join.inner_child_idx)
        .cloned()
        .ok_or_else(|| plain("mview full-update lookup template: index join inner plan is nil"))?;
    let inner_schema = inner.schema().map(|schema| schema.columns.clone()).unwrap_or_default();
    if inner_schema.len() != build.full_update_lookup_column_count {
        return Err(plain(format!(
            "mview full-update lookup template: unexpected inner schema length: got {}, expected {}",
            inner_schema.len(),
            build.full_update_lookup_column_count
        )));
    }
    if join.inner_join_keys.len() != build.group_key_mv_offsets.len() {
        return Err(plain(format!(
            "mview full-update lookup template: unexpected inner join key count: got {}, expected {}",
            join.inner_join_keys.len(),
            build.group_key_mv_offsets.len()
        )));
    }
    let mut output_mv_offsets: Vec<Option<usize>> = vec![None; inner_schema.len()];
    let mut key_result_col_idxes = Vec::with_capacity(join.inner_join_keys.len());
    for (key_pos, key) in join.inner_join_keys.iter().enumerate() {
        let index = inner_schema.iter().position(|column| column.unique_id == key.unique_id).ok_or_else(|| {
            plain(format!("mview full-update lookup template: invalid inner join key at position {key_pos}"))
        })?;
        if output_mv_offsets[index].is_some() {
            return Err(plain(format!("mview full-update lookup template: duplicate key result col idx {index}")));
        }
        output_mv_offsets[index] = Some(build.group_key_mv_offsets[key_pos]);
        key_result_col_idxes.push(index);
    }
    let mut non_key = build
        .full_update_lookup_mv_offsets
        .iter()
        .copied()
        .filter(|offset| !build.group_key_mv_offsets.contains(offset));
    for slot in &mut output_mv_offsets {
        if slot.is_none() {
            *slot = Some(non_key.next().ok_or_else(|| {
                plain("mview full-update lookup template: non-key column mapping mismatch")
            })?);
        }
    }
    if non_key.next().is_some() {
        return Err(plain("mview full-update lookup template: non-key column mapping mismatch"));
    }
    Ok(FullUpdateLookup {
        inner,
        join: join.clone(),
        key_result_col_idxes,
        output_mv_offsets: output_mv_offsets.into_iter().flatten().collect(),
        snapshot: None,
    })
}

/// Go master `mviewRefreshMLogIsEmpty`: the log side of the merge source
/// (projections over the LEFT JOIN's left child, the grouped aggregation of
/// the log) planned to a zero-row dual.
fn mlog_is_empty(plan: &PhysicalPlan) -> bool {
    let mut plan = plan;
    loop {
        let children = plan.children();
        match plan {
            PhysicalPlan::TableDual(dual) => return dual.row_count == 0,
            // Go `base.PhysicalJoin`: every join operator, not only a hash
            // join (a log side folded to a dual can make the planner pick an
            // index or merge join).
            _ if plan.join_type().is_some() => {
                if plan.join_type() != Some(tidb_planner_core_base::LogicalJoinType::LeftOuter) || children.len() != 2 {
                    return false;
                }
                plan = &children[0];
                continue;
            }
            PhysicalPlan::HashAgg(agg) if agg.group_by_items.is_empty() => return false,
            PhysicalPlan::StreamAgg(agg) if agg.group_by_items.is_empty() => return false,
            PhysicalPlan::HashAgg(_) | PhysicalPlan::StreamAgg(_) | PhysicalPlan::Projection(_) | PhysicalPlan::Selection(_) => {}
            _ => return false,
        }
        if children.len() != 1 {
            return false;
        }
        plan = &children[0];
    }
}

/// Go master's `MViewDeltaMerge` physical plan: the operator over its
/// optimized merge source, as EXPLAIN shows it, and whether the log side is
/// empty.
///
/// # Errors
///
/// The merge source fails to plan or has an unexpected width.
pub fn plan_mview_delta_merge(
    plan: &MViewDeltaMerge,
    build: &BuildResult,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    retained_upper_tso: u64,
    full_update_snapshot: &FullUpdateSnapshotProvider<'_>,
) -> Result<(PhysicalPlan, bool, Option<FullUpdateLookup>), DriverError> {
    // Go `buildRefreshMaterializedViewImplement`: the window's upper bound
    // is the statement transaction's TS, which must be set.
    if retained_upper_tso == 0 {
        return Err(plain("RefreshMaterializedViewImplementStmt: invalid transaction start tso"));
    }
    // Go master `WithMLogCommitTSEstimation` around the merge source's
    // optimization: the retained log window the commit-ts filters are
    // estimated over.
    let plan_ctx = ctx.clone().with_mlog_commit_ts_estimation(tidb_exprctx::MLogCommitTSEstimation {
        mlog_table_id: build.mlog_table_id,
        retained_lower_tso: plan.mlog_retained_lower_tso,
        retained_upper_tso,
    });
    // Go optimizes the merge source and the lookup template under one
    // session: both draw from its plan-id and column-id counters.
    let (plan_ids, column_ids) =
        (tidb_planner_core_base::PlanIdAllocator::new(), tidb_planner_core_base::ColumnIdAllocator::new());
    let source = super::planner_bridge::physical_select_plan_with_allocators(
        &build.merge_source_select,
        catalog,
        current_db,
        &plan_ctx,
        &plan_ids,
        &column_ids,
    )
    .map_err(super::planner_error_to_driver)?;
    let width = source.schema().map_or(0, |schema| schema.columns.len());
    if width != build.source_column_count {
        return Err(plain(format!(
            "unexpected merge-source schema length: got {width}, expected {}",
            build.source_column_count
        )));
    }
    let empty = mlog_is_empty(&source);
    // Go `buildRefreshMaterializedViewImplement`'s MIN / MAX fallback: the
    // lookup template planned (IndexJoin over the supporting index) while
    // the log has rows.
    let full_update = match &build.full_update_lookup_template_select {
        Some(template) if !empty => {
            // Go `lookupIS`: an AS OF refresh plans and reads the lookup at
            // its target (the GC safe point checked, the target's schema).
            let snapshot =
                if plan.options.to_ts > 0 { Some(full_update_snapshot(plan.options.to_ts)?) } else { None };
            let lookup_catalog = snapshot.as_ref().map_or(catalog, |snapshot| &snapshot.catalog);
            // Go `validateMVFullUpdateSupportingIndex`.
            let base = lookup_catalog.table_info_by_id(build.base_table_id).ok_or_else(|| {
                plain(format!(
                    "mview full-update lookup template: base table id {} not found in infoschema",
                    build.base_table_id
                ))
            })?;
            let indexes =
                tidb_mviewutil::find_visible_indexes_with_prefix_covering_columns(&base.read(), &build.group_key_base_cols);
            if indexes.is_empty() {
                return Err(plain(
                    "refresh materialized view fast with MIN/MAX requires base table index whose leading columns cover all GROUP BY columns",
                ));
            }
            let mut select = template.clone();
            tidb_planner::mview::set_full_update_lookup_index_hint(&mut select, &indexes).map_err(plain)?;
            let lookup_ctx = plan_ctx.clone().with_inl_join_inner_multi_pattern(true);
            let lookup_plan = super::planner_bridge::physical_select_plan_with_allocators(
                &select,
                lookup_catalog,
                current_db,
                &lookup_ctx,
                &plan_ids,
                &column_ids,
            )
            .map_err(super::planner_error_to_driver)?;
            let width = lookup_plan.schema().map_or(0, |schema| schema.columns.len());
            if width != build.full_update_lookup_column_count {
                return Err(plain(format!(
                    "mview full-update lookup template: unexpected output schema length: got {width}, expected {}",
                    build.full_update_lookup_column_count
                )));
            }
            let mut lookup = extract_full_update_lookup_template(&lookup_plan, build)?;
            lookup.snapshot = snapshot;
            Some(lookup)
        }
        _ => None,
    };
    let root_ids = tidb_planner_core_base::PlanIdAllocator::new();
    Ok((
        PhysicalPlan::Dml(tidb_planner_core_operator_physicalop::PhysicalDmlRoot {
            base: tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(&plan_ids, "MViewDeltaMerge", 0),
            go_operator: "MViewDeltaMerge".to_owned(),
            explain_info: Some(explain_info(build, full_update.is_some())),
            select_plan: Some(Box::new(source)),
            full_update_inner: full_update.as_ref().map(|lookup| Box::new(lookup.inner.clone())),
            ..Default::default()
        }),
        empty,
        full_update,
    ))
}

/// Go master `buildMViewDeltaMergeAggMappings`' order: COUNT(*) first, then
/// every aggregate after the ones whose merged value it reads (Kahn).
pub(crate) fn merge_order(build: &BuildResult) -> Result<Vec<usize>, DriverError> {
    let infos = &build.agg_infos;
    let delta = build.delta_column_count;
    let count_star = infos
        .iter()
        .position(|info| info.kind == AggKind::CountStar)
        .ok_or_else(|| plain("MViewDeltaMerge aggregate mappings require COUNT(*)"))?;
    let output_of = |index: usize| delta + infos[index].mv_offset;
    let mut indegree = vec![0usize; infos.len()];
    let mut adjacent = vec![Vec::new(); infos.len()];
    for (to, info) in infos.iter().enumerate() {
        let mut from_seen = Vec::new();
        for &dep in &info.dependencies {
            if dep < delta {
                continue;
            }
            let from = (0..infos.len()).find(|&from| output_of(from) == dep).ok_or_else(|| {
                plain(format!(
                    "MViewDeltaMerge dependency col id {dep} for mapping {to} (output cols [{}]) does not map to any aggregate output",
                    output_of(to)
                ))
            })?;
            if !from_seen.contains(&from) {
                from_seen.push(from);
                adjacent[from].push(to);
                indegree[to] += 1;
            }
        }
    }
    if indegree[count_star] != 0 {
        return Err(plain(format!(
            "MViewDeltaMerge COUNT(*) mapping at index {count_star} has non-zero indegree {}",
            indegree[count_star]
        )));
    }
    let mut queue = vec![count_star];
    queue.extend((0..infos.len()).filter(|&index| index != count_star && indegree[index] == 0));
    let mut head = 0;
    while head < queue.len() {
        let current = queue[head];
        head += 1;
        for &to in &adjacent[current] {
            indegree[to] -= 1;
            if indegree[to] == 0 {
                queue.push(to);
            }
        }
    }
    if queue.len() != infos.len() {
        let mut unresolved: Vec<usize> = (0..infos.len()).filter(|index| !queue.contains(index)).map(output_of).collect();
        unresolved.sort_unstable();
        return Err(plain(format!(
            "MViewDeltaMerge aggregate dependencies are unresolved or cyclic, unresolved output column ids: {unresolved:?}"
        )));
    }
    Ok(queue)
}

/// Go `plannercore.DataReaderSnapshot` for the MIN / MAX recompute of an AS
/// OF FAST refresh: the target's schema, and the statement reading the base
/// table at the target.
pub struct FullUpdateSnapshot {
    /// Go `TS`: the AS OF target the base table is read at.
    pub ts: u64,
    /// Go `InfoSchema`: the target's schema.
    pub catalog: SessionExtendedInfoSchema,
    pub ctx: crate::StmtContext,
}

/// Opens the [`FullUpdateSnapshot`] at an AS OF target, when the merge needs
/// the MIN / MAX lookup (Go computes it only there).
pub type FullUpdateSnapshotProvider<'a> = dyn Fn(u64) -> Result<FullUpdateSnapshot, DriverError> + 'a;

/// Go master `MViewDeltaMergeAggExec`: merges the window into the view and
/// answers the write counts.
///
/// # Errors
///
/// The merge source fails to plan or run, a merge fails, or a write fails.
pub fn run_mview_delta_merge(
    plan: &MViewDeltaMerge,
    build: &BuildResult,
    catalog: &mut SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    retained_upper_tso: u64,
    full_update_snapshot: &FullUpdateSnapshotProvider<'_>,
) -> Result<DeltaMergeWriterStats, DriverError> {
    let (mut root, mlog_empty, full_update) =
        plan_mview_delta_merge(plan, build, catalog, current_db, ctx, retained_upper_tso, full_update_snapshot)?;
    // Go `newDataReaderBuilderWithSnapshot(v.FullUpdateInnerSource,
    // v.FullUpdateSnapshot)`: an AS OF target's recompute reads the base table
    // at the target, over the target's schema.
    let full_update_reader = full_update
        .as_ref()
        .map(|lookup| match &lookup.snapshot {
            Some(snapshot) => super::physical_builder::full_update_reader_builder(
                &lookup.inner,
                &lookup.join,
                &snapshot.catalog,
                &snapshot.ctx,
            ),
            None => super::physical_builder::full_update_reader_builder(&lookup.inner, &lookup.join, catalog, ctx),
        })
        .transpose()?;
    crate::explain::publish_physical_process_info(ctx, &root, catalog)?;
    let dml = super::physical_builder::DmlExec::new(&root, ctx);
    let root_id = i64::from(root.id());
    if mlog_empty {
        // Go `Open` with `MLogEmpty`: nothing to merge.
        if let Some(runtime) = ctx.executor_runtime_stats_source() {
            runtime.register_stats(root_id, Box::new(tidb_executor_mviewdeltamergeagg::MergeRuntimeStats::new(0)));
        }
        return Ok(DeltaMergeWriterStats::default());
    }
    let delta = build.delta_column_count;
    let tidb_planner_core_operator_physicalop::PhysicalPlan::Dml(root_plan) = &mut root else {
        unreachable!("an MViewDeltaMerge plan is a DML root")
    };
    let physical = root_plan.select_plan.as_deref_mut().expect("the merge source");
    let source = super::physical_builder::build_dml_source(physical, catalog, ctx)?;
    let source_types = source.ret_field_types().to_vec();
    // Go `buildMViewDeltaMerge`.
    let mut agg_mappings = super::mview_delta_merge_agg_builder::build_mview_delta_merge_agg_mappings(build, &source_types)?;
    let min_max_recompute = super::mview_delta_merge_agg_builder::build_mview_delta_merge_min_max_recompute(
        build,
        full_update.as_ref(),
        full_update_reader,
        &mut agg_mappings,
    )?;
    // Go `wrapTableWithMLogIfExists(target, tables.MLogSourceRefresh)`.
    let mlogs = super::dml::MLogTables::wrap(
        catalog,
        [(plan.database.as_str(), plan.name.as_str())],
        tidb_table::tables::mview_log::MLogSourceStmt::Refresh,
    )?;
    let mlog = mlogs.get(&plan.database, &plan.name);
    let target = super::mview_delta_merge_agg_builder::SessionTargetTable::new(catalog, &plan.database, &plan.name, ctx, mlog);
    let mut exec = tidb_executor_mviewdeltamergeagg::Exec::new(source);
    exec.agg_mappings = agg_mappings;
    exec.delta_agg_col_count = delta as i64;
    exec.min_max_recompute = min_max_recompute;
    exec.target_table = Some(Box::new(target));
    exec.target_handle_cols = Some(build.mv_table_pk_cols.clone());

    let collect_runtime_stats = ctx.executor_runtime_stats_source().is_some();
    let mut writer = tidb_executor_mviewdeltamergeagg::MergeWriterStats::default();
    let result = dml.next(|| -> Result<(), DriverError> {
        exec.open(ctx.executor_concurrency() as i64)?;
        writer = exec.next(collect_runtime_stats)?;
        Ok(())
    });
    let runtime = exec.runtime_stats().cloned();
    let closed = dml.close(|| exec.close());
    // Go `defer RuntimeStatsColl.RegisterStats(e.ID(), e.runtimeStats)`.
    if let (Some(source), Some(runtime)) = (ctx.executor_runtime_stats_source(), runtime) {
        source.register_stats(root_id, Box::new(runtime));
    }
    result?;
    closed?;
    Ok(DeltaMergeWriterStats {
        chunks: writer.chunks,
        row_ops: writer.row_ops,
        noop_rows: writer.noop_rows,
        insert_rows: writer.insert_rows,
        update_rows: writer.update_rows,
        delete_rows: writer.delete_rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(kind: AggKind, mv_offset: usize, dependencies: Vec<usize>) -> AggInfo {
        AggInfo { kind, mv_offset, arg_col_name: String::new(), arg_not_null: false, dependencies }
    }

    #[test]
    fn explain_info_as_go() {
        let mut sum = info(AggKind::Sum, 1, vec![1]);
        sum.arg_col_name = "b".to_owned();
        assert_eq!(format_agg_dependency(&sum), "sum(b)@1->[1]");
        assert_eq!(format_agg_dependency(&info(AggKind::CountStar, 2, vec![0])), "count(*)@2->[0]");
    }

}
