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

//! Go `planner.Optimize` for an UPDATE or DELETE (`pkg/planner/optimize.go`,
//! `buildUpdate` / `buildDelete` in `logical_plan_builder.go`, the point
//! plans of `point_get_plan.go`), `OptimizeForForeignKeyCascade` and the
//! foreign-key trigger plans (`foreign_key.go`).

use crate::optimizer::{optimize_built_logical, physical_plan_for_logical_with_update_refs};
use std::collections::BTreeMap;
use tidb_driver_error::DriverError;
use tidb_expr::expression::Expression;
use tidb_hack::{GoCaseCompare, GoToLower};
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_planner_core_base::ColumnIdAllocator;
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_sessionctx::StmtContext;

use crate::catalog_ctx::PlannerCatalogContext as _;
use crate::from_scope::single_table_name;
use crate::optimize::{
    BuildVisits, BuilderParts, QueryBuild, ScalarSubqueryEvalCtx, ScalarSubqueryRegistry,
};
use crate::plan_builder::PlanBuilder;
use crate::point_get_plan::{PointPlanKind, handed_point_plan};
use crate::stmt_ctx::PlannerStmtCtx as _;
use tidb_infoschema::TableEntry;
use tidb_planner_core_base::PlanIdAllocator;
use tidb_planner_core_operator_physicalop::FkTriggerNode;
use tidb_planner_core_operator_physicalop::fk_plan::{FkPlanError, ForeignKeyCatalog};

pub use tidb_planner_core_operator_physicalop::fk_plan::FkPlanSpec;

struct CatalogForeignKeys<'a>(&'a SessionExtendedInfoSchema);

impl ForeignKeyCatalog for CatalogForeignKeys<'_> {
    fn table(
        &self,
        database: &str,
        table: &str,
    ) -> Option<tidb_model::GoShared<tidb_model::table_info::TableInfo>> {
        let Some(TableEntry::Kv(table)) = self.0.get_in(database, table) else {
            return None;
        };
        Some(table.meta())
    }

    fn referring(
        &self,
        database: &str,
        table: &str,
    ) -> Vec<(
        String,
        String,
        tidb_model::GoShared<tidb_model::table::FKInfo>,
    )> {
        referring(self.0, database, table)
    }

    fn lookup_index(
        &self,
        database: &str,
        table: &str,
        columns: &[usize],
    ) -> Option<Option<(i64, String)>> {
        let Some(TableEntry::Kv(table)) = self.0.get_in(database, table) else {
            return None;
        };
        table.foreign_key_lookup_index(columns).map(|index| {
            index.map(|id| {
                let found = table
                    .indexes()
                    .iter()
                    .find(|index| index.read().id == id)
                    .unwrap();
                (id, found.read().name.original().to_owned())
            })
        })
    }
}

/// Select constraints once, following Go's BuildOn{Insert,Update,Delete}FKTriggers.
pub fn build_fk_triggers(
    catalog: &SessionExtendedInfoSchema,
    ctx: &StmtContext,
    operator: &str,
    spec: &FkPlanSpec,
    plan_ids: &PlanIdAllocator,
) -> Result<Vec<FkTriggerNode>, DriverError> {
    // The updated columns are those of Go's write lists: the SET (or ON
    // DUPLICATE) assignments plus the generated assignments the planner
    // appends to them (`Update.OrderedList`, `Insert.OnDuplicate`).
    let mut spec = spec.clone();
    if let Some(TableEntry::Kv(table)) = catalog.get_in(&spec.database, &spec.table) {
        let columns: Vec<_> = table
            .meta()
            .read()
            .cols()
            .iter_handles()
            .flatten()
            .collect();
        use crate::plan_builder::generated_assignments::{
            on_duplicate_generated_columns, update_list_generated_columns,
        };
        if !spec.updated_cols.is_empty() {
            let generated = update_list_generated_columns(&columns, &spec.updated_cols);
            spec.updated_cols.extend(generated);
        }
        if !spec.on_duplicate_cols.is_empty() {
            let generated = on_duplicate_generated_columns(&columns, &spec.on_duplicate_cols);
            spec.on_duplicate_cols.extend(generated);
        }
    }
    tidb_planner_core_operator_physicalop::fk_plan::build_fk_triggers(
        &CatalogForeignKeys(catalog),
        ctx.foreign_key_checks(),
        operator,
        &spec,
        plan_ids,
    )
    .map_err(|error| match error {
        FkPlanError::NoReferencedRow(fk) => DriverError::from(
            tidb_plannererrors::ERR_NO_REFERENCED_ROW2.fast_gen_by_args(&[fk.into()]),
        ),
        FkPlanError::RowIsReferenced(fk) => DriverError::from(
            tidb_plannererrors::ERR_ROW_IS_REFERENCED2.gen_with_stack_by_args(&[fk.into()]),
        ),
        FkPlanError::ProtectedTableMode(table, mode) => DriverError::from(
            tidb_infoschema::check_table_mode_is_normal(&table, mode)
                .expect_err("the child table is not in TableModeNormal"),
        ),
        FkPlanError::Unsupported(message) => DriverError::unsupported(message),
    })
}

/// Go `FKCascadeExec.buildFKCascadePlan`'s planning half: `Preprocess` and
/// `OptimizeForForeignKeyCascade` of the cascade statement, which build the
/// plan with the statement's plan-id counter and without the fast point
/// plans `planner.Optimize` alone tries.
pub fn optimize_for_foreign_key_cascade(
    hooks: &dyn crate::optimize::PlanningHooks,
    stmt: &FKCascadeStmt,
    catalog: &SessionExtendedInfoSchema,
    ctx: &StmtContext,
    plan_ids: &tidb_planner_core_base::PlanIdAllocator,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, DriverError> {
    match stmt {
        FKCascadeStmt::Delete(delete) => {
            let current_db = cascade_database(&delete.kind_table())?;
            let source = delete_source_query(delete);
            physical_dml_plan_with_cache_mode(
                hooks,
                "Delete",
                source.as_ref(),
                Some(MultiDmlRef::Delete(delete)),
                catalog,
                &current_db,
                ctx,
                false,
                &[fk_spec_for_delete(delete, &current_db)?],
                DmlKeep::Auto,
                // Go's `buildFKCascadePlan` checks no privileges.
                BuildVisits::Discard,
                Some(plan_ids),
            )
        }
        FKCascadeStmt::Update(update) => {
            let current_db = cascade_database(&update.kind_table())?;
            let source = update_source_query(update);
            physical_dml_plan_with_cache_mode(
                hooks,
                "Update",
                source.as_ref(),
                Some(MultiDmlRef::Update(update)),
                catalog,
                &current_db,
                ctx,
                false,
                &[build_tbl_2_update_columns(update, &current_db)?],
                DmlKeep::Auto,
                BuildVisits::Discard,
                Some(plan_ids),
            )
        }
    }
}

/// The child schema a cascade statement names (`genTableRefsAST`).
pub fn cascade_database(table: &Option<&tidb_ast::TableRef>) -> Result<String, DriverError> {
    table
        .and_then(|table| (table.name.len() == 2).then(|| table.name[0].clone()))
        .ok_or_else(|| DriverError::unsupported("a cascade statement names no child table"))
}

/// Builds and prepares the same UPDATE or DELETE plan for execution and
/// EXPLAIN, checking the statement's privileges once it is built.
#[allow(clippy::too_many_arguments)]
pub fn physical_dml_plan(
    hooks: &dyn crate::optimize::PlanningHooks,
    operator: &str,
    source: Option<&tidb_ast::QueryStmt>,
    write: MultiDmlRef<'_>,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &StmtContext,
    fk_spec: &FkPlanSpec,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, DriverError> {
    physical_dml_plan_with_cache_mode(
        hooks,
        operator,
        source,
        Some(write),
        catalog,
        current_db,
        ctx,
        false,
        std::slice::from_ref(fk_spec),
        DmlKeep::Auto,
        BuildVisits::Check,
        None,
    )
}

/// [`physical_dml_plan`] with the scalar subqueries its read evaluated, for
/// EXPLAIN.
#[allow(clippy::too_many_arguments)]
pub fn physical_dml_plan_and_scalar_subqueries(
    hooks: &dyn crate::optimize::PlanningHooks,
    operator: &str,
    source: Option<&tidb_ast::QueryStmt>,
    write: MultiDmlRef<'_>,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &StmtContext,
    fk_spec: &FkPlanSpec,
) -> Result<
    (
        tidb_planner_core_operator_physicalop::PhysicalPlan,
        Vec<std::sync::Arc<ScalarSubqueryEvalCtx>>,
    ),
    DriverError,
> {
    physical_dml_plan_and_scalar_subqueries_with_cache_mode(
        hooks,
        operator,
        source,
        Some(write),
        catalog,
        current_db,
        ctx,
        false,
        std::slice::from_ref(fk_spec),
        DmlKeep::Auto,
        BuildVisits::Check,
        None,
    )
}

/// Multi-table callers carry all resolved table policies into the same DML
/// allocator and source builder used by single-table writes.
#[allow(clippy::too_many_arguments)]
pub fn physical_multi_dml_plan(
    hooks: &dyn crate::optimize::PlanningHooks,
    operator: &str,
    source: &tidb_ast::QueryStmt,
    write: MultiDmlRef<'_>,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &StmtContext,
    fk_specs: &[FkPlanSpec],
    keep: DmlKeep,
    visits: BuildVisits<'_>,
) -> Result<
    (
        tidb_planner_core_operator_physicalop::PhysicalPlan,
        Vec<std::sync::Arc<ScalarSubqueryEvalCtx>>,
    ),
    DriverError,
> {
    physical_dml_plan_and_scalar_subqueries_with_cache_mode(
        hooks,
        operator,
        Some(source),
        Some(write),
        catalog,
        current_db,
        ctx,
        false,
        fk_specs,
        keep,
        visits,
        None,
    )
}

/// Shared UPDATE/DELETE construction, following Go's
/// plan-builder allocation order and DML-local plan nodes:
///
/// * Go's single-table point UPDATE/DELETE (`point_get_plan.go:1254/1374`)
///   builds the locked `PointGet` first; the UPDATE path allocates one extra
///   plan before the `Update` root, the DELETE path does not.
/// * Non-point UPDATE/DELETE build the source logically, allocate the
///   `Update`/`Delete` root, then `DoOptimize` — with `buildSelectLock`
///   wrapping the single-table source (`logical_plan_builder.go:6117/6552`).
/// * `BuildOn{Update,Delete}FKTriggers` runs last, allocating the
///   `Foreign_Key_Check` / `Foreign_Key_Cascade` leaves.
#[allow(clippy::too_many_arguments)]
///
/// `write` is the UPDATE or DELETE being planned.
/// `visits` is what becomes of the visit infos Go's one `PlanBuilder` would
/// hold once `buildUpdate` / `buildDelete` returns: `Check`ed
/// here (Go `planner.optimize`'s `CheckPrivilege` after the build, which for a
/// DML includes the read's `DoOptimize`), `Collect`ed for PREPARE, or
/// `Discard`ed for a cached plan's rebuild.
/// A fast point plan is checked as Go's `checkFastPlanPrivilege` checks it,
/// and a denied one leaves the statement to the ordinary build.
#[allow(clippy::too_many_arguments)]
pub fn physical_dml_plan_with_cache_mode(
    hooks: &dyn crate::optimize::PlanningHooks,
    operator: &str,
    source: Option<&tidb_ast::QueryStmt>,
    write: Option<MultiDmlRef<'_>>,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &StmtContext,
    use_plan_cache: bool,
    fk_specs: &[FkPlanSpec],
    keep: DmlKeep,
    visits: BuildVisits<'_>,
    cascade_plan_ids: Option<&tidb_planner_core_base::PlanIdAllocator>,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, DriverError> {
    physical_dml_plan_and_scalar_subqueries_with_cache_mode(
        hooks,
        operator,
        source,
        write,
        catalog,
        current_db,
        ctx,
        use_plan_cache,
        fk_specs,
        keep,
        visits,
        cascade_plan_ids,
    )
    .map(|(plan, _)| plan)
}

/// [`physical_dml_plan_with_cache_mode`] with the scalar subqueries its read
/// evaluated (Go `SessionVars.MapScalarSubQ`), which EXPLAIN renders.
#[allow(clippy::too_many_arguments)]
pub fn physical_dml_plan_and_scalar_subqueries_with_cache_mode(
    hooks: &dyn crate::optimize::PlanningHooks,
    operator: &str,
    source: Option<&tidb_ast::QueryStmt>,
    write: Option<MultiDmlRef<'_>>,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &StmtContext,
    use_plan_cache: bool,
    fk_specs: &[FkPlanSpec],
    keep: DmlKeep,
    visits: BuildVisits<'_>,
    cascade_plan_ids: Option<&tidb_planner_core_base::PlanIdAllocator>,
) -> Result<
    (
        tidb_planner_core_operator_physicalop::PhysicalPlan,
        Vec<std::sync::Arc<ScalarSubqueryEvalCtx>>,
    ),
    DriverError,
> {
    use BuildVisits;
    use tidb_planner_core_operator_physicalop::{BasePhysicalPlan, PhysicalDmlRoot, PhysicalPlan};

    let mut scalar_subqueries = Vec::new();
    // A foreign-key cascade's statement continues the plan ids of the
    // statement that runs it (Go's one `SessionVars.PlanID`).
    let own_plan_ids;
    let plan_ids = match cascade_plan_ids {
        Some(plan_ids) => plan_ids,
        None => {
            own_plan_ids = tidb_planner_core_base::PlanIdAllocator::new();
            &own_plan_ids
        }
    };
    let update = match write {
        Some(MultiDmlRef::Update(update)) => Some(update),
        _ => None,
    };
    let checking = matches!(visits, BuildVisits::Check);
    let collecting = !matches!(visits, BuildVisits::Discard);
    let mut statement_visits = Vec::new();
    let mut fast_plan_checked = false;

    let column_ids = tidb_planner_core_base::ColumnIdAllocator::new();
    let mut update_expressions = Vec::new();
    let mut delete_col_pos = None;
    let mut delete_tables = Vec::new();
    let mut update_tables = Vec::new();
    let mut update_assigns = Vec::new();
    let single_update =
        update.filter(|update| matches!(update.kind, tidb_ast::UpdateKind::Single(_)));
    let mut fast_update_list = false;
    let build_fk_triggers = || -> Result<Vec<FkTriggerNode>, DriverError> {
        let mut fk_triggers = Vec::new();
        for spec in fk_specs {
            fk_triggers.extend(build_fk_triggers(catalog, ctx, operator, spec, plan_ids)?);
        }
        Ok(fk_triggers)
    };
    // Go `buildDelete` builds its triggers once the `Delete` root is
    // initialized, before the pruning projection and `DoOptimize`.
    let delete_fk_triggers = std::cell::RefCell::new(None);
    let build_delete_fk_triggers = || {
        *delete_fk_triggers.borrow_mut() = Some(build_fk_triggers());
    };

    let mut root_base = None;

    let select_plan = match source {
        None => None,
        Some(tidb_ast::QueryStmt::Select(select)) => {
            let multi_update =
                update.filter(|update| matches!(update.kind, tidb_ast::UpdateKind::Multi { .. }));
            // Go's `TryFastPlan` belongs to `planner.Optimize`, which a
            // cascade's `OptimizeForForeignKeyCascade` does not run, and
            // which tries it only when TiKV is an isolation read engine.
            // PREPARE's `destBuilder.Build` is the plan builder alone.
            let allow_fast_plan = multi_update.is_none()
                && !matches!(visits, BuildVisits::Collect(_))
                && cascade_plan_ids.is_none()
                && ctx.isolation_read_contains_tikv()
                && update.is_none_or(|update| try_update_point_plan(update));
            // Go `tryUpdatePointPlan` / `tryDeletePointPlan`'s
            // `checkFastPlanPrivilege(..., SelectPriv, UpdatePriv /
            // DeletePriv)`.
            let write_privilege = if operator.eq_ignore_ascii_case("Update") {
                tidb_mysql::UpdatePriv
            } else {
                tidb_mysql::DeletePriv
            };
            let fast_plan_privilege = |database: &str, table: &str| {
                !checking
                    || crate::privilege_check::check_fast_plan_privilege(
                        ctx,
                        catalog,
                        database,
                        table,
                        &[tidb_mysql::SelectPriv, write_privilege],
                    )
            };
            // Go `Optimize` tries the fast plan whether or not the plan cache
            // builds this plan (`generateNewPlan` -> `OptimizeAstNodeNoCache`);
            // a cached point plan's key is rebuilt from the EXECUTE parameters
            // (`RebuildPlan4CachedPlan` -> `buildRangesForPointGet`).
            let fast = match handed_point_plan(ctx, PointPlanKind::Dml).filter(|_| allow_fast_plan)
            {
                // Go `Optimize`: the point plan a multi-statement query's
                // prefetch already tried (`PointPlanKey`), not `TryFastPlan`
                // again; its plan ids come first.
                Some(handed) => handed.plan.clone().inspect(|_| {
                    for _ in 0..handed.plan_ids {
                        plan_ids.alloc();
                    }
                }),
                None => allow_fast_plan
                    .then(|| {
                        crate::point_get_plan::try_fast_dml_point_physical_plan_with_allocator(
                            select,
                            catalog,
                            current_db,
                            ctx,
                            plan_ids,
                            &fast_plan_privilege,
                        )
                    })
                    .transpose()?
                    .flatten(),
            };
            // Go `tryUpdatePointPlan`: `buildOrderedList` over the point plan,
            // whose `rewriteAstExprWithPlanCtx` inits a `LogicalTableDual` for
            // each assignment; an assignment it cannot resolve there (a
            // column the point plan does not read, such as
            // `_tidb_commit_ts`) leaves no fast plan, and the statement takes
            // the full build.
            let mut fast_update_parts = None;
            let fast = match (fast, update.filter(|_| operator.eq_ignore_ascii_case("Update"))) {
                (Some(point), Some(update)) => {
                    for _ in &update.assignments {
                        let _point_update_filler = plan_ids.alloc();
                    }
                    let parts = update_assignments_for_plan(update, catalog, current_db, ctx).and_then(|assignments| {
                        let assigns = single_update_assigns(&assignments);
                        fast_update_ordered_list(
                            hooks,
                            update,
                            &assignments,
                            &point,
                            catalog,
                            current_db,
                            ctx,
                            &plan_ids,
                            &column_ids,
                        )
                        .map(|expressions| (assigns, expressions))
                    });
                    match parts {
                        Ok(parts) => {
                            fast_update_parts = Some(parts);
                            Some(point)
                        }
                        Err(_) => None,
                    }
                }
                (fast, _) => fast,
            };
            if let Some(mut fast) = fast {
                // Go's `Optimize` returns the fast plan without `optimize`'s
                // `CheckPrivilege`: `checkFastPlanPrivilege` was its check.
                fast_plan_checked = true;
                // Go's point plans carry `Lock` for UPDATE and DELETE, the
                // batch one as well (`tryUpdatePointPlan`/`tryDeletePointPlan`).
                match &mut fast {
                    PhysicalPlan::PointGet(point) => point.lock = ctx.pessimistic_transaction(),
                    PhysicalPlan::BatchPointGet(batch) => {
                        batch.lock = ctx.pessimistic_transaction();
                    }
                    _ => {}
                }
                root_base = Some(BasePhysicalPlan::new(plan_ids, operator, 0));
                if let Some((assigns, expressions)) = fast_update_parts {
                    update_assigns = assigns;
                    update_expressions = expressions;
                    fast_update_list = true;
                }
                Some(fast)
            } else {
                // Go `optimize` past `TryFastPlan`: `PlanBuilder.Build`.
                crate::optimize::record_plan_builder_metric(ctx);
                // Go `buildDelete` prunes the read to what removing a record
                // needs (the planner's `pruneAndBuildColPositionInfoForDelete`);
                // a point plan above keeps every column. Foreign keys other
                // tables reference to this one decline the pruning.
                let (delete_prune_has_fk, delete_multi) = match keep {
                    DmlKeep::Auto => (
                        operator
                            .eq_ignore_ascii_case("Delete")
                            .then(|| delete_has_fk(select, catalog, current_db, ctx)),
                        None,
                    ),
                    DmlKeep::MultiDelete(input) => (None, Some(input)),
                };
                let built = physical_dml_source_plan_with_allocators(
                    hooks,
                    select,
                    single_update,
                    multi_update,
                    catalog,
                    current_db,
                    ctx,
                    use_plan_cache,
                    &plan_ids,
                    &column_ids,
                    operator,
                    delete_prune_has_fk,
                    delete_multi,
                    Some(&build_delete_fk_triggers),
                    write.filter(|_| collecting),
                )
                .map_err(crate::optimize::planner_error_to_driver)?;
                scalar_subqueries = built.scalar_subqueries;
                statement_visits.extend(built.visit_info);
                update_expressions = built.update_expressions;
                delete_col_pos = built.delete_col_pos;
                delete_tables = built.delete_tables;
                update_tables = built.update_tables;
                if multi_update.is_some() {
                    update_assigns = built.update_assigns;
                }
                if let Some(assignments) = &built.single_assignments {
                    update_assigns = single_update_assigns(assignments);
                }
                root_base = Some(built.root);
                Some(built.plan)
            }
        }
        Some(query) => Some(
            physical_query_plan_with_allocators(
                hooks,
                query,
                catalog,
                current_db,
                ctx,
                use_plan_cache,
                &plan_ids,
                &column_ids,
                if collecting {
                    BuildVisits::Collect(&mut statement_visits)
                } else {
                    BuildVisits::Discard
                },
            )
            .map_err(crate::optimize::planner_error_to_driver)?,
        ),
    };

    let base = root_base.ok_or_else(|| DriverError::unsupported("DML build produced no root"))?;
    // Go `BuildOn{Update,Delete}FKTriggers`, last in the builders but
    // `buildDelete`, which runs them before optimizing its source.
    let mut fk_triggers = match delete_fk_triggers.take() {
        Some(built) => built?,
        None => build_fk_triggers()?,
    };
    // Go flattens all checks before all cascades, preserving table order.
    fk_triggers.sort_by_key(|node| node.operator == "Foreign_Key_Cascade");
    // Go `planner.optimize`'s `CheckPrivilege` / `CheckTableLock` once
    // `buildUpdate` / `buildDelete` returned.
    match visits {
        BuildVisits::Check if !fast_plan_checked => {
            crate::privilege_check::check_plan_privilege(ctx, catalog, &statement_visits)?;
        }
        BuildVisits::Collect(collected) => collected.extend(statement_visits),
        BuildVisits::Check | BuildVisits::Discard => {}
    }
    let mut plan = PhysicalPlan::Dml(PhysicalDmlRoot {
        base,
        go_operator: operator.to_owned(),
        explain_info: None,
        select_plan: select_plan.map(Box::new),
        full_update_inner: None,
        update_expressions,
        fk_triggers,
        delete_col_pos,
        delete_tables,
        fast_update_list,
        update_tables,
        update_assigns,
        insert_select: None,
        insert_expressions: None,
        plan_id: plan_ids.current(),
    });
    hooks.prepare_plan(&mut plan, ctx)?;
    Ok((plan, scalar_subqueries))
}

/// Go `buildTbl2UpdateColumns` narrowed to the SET column names.
pub fn build_tbl_2_update_columns(
    update: &tidb_ast::UpdateStmt,
    current_db: &str,
) -> Result<FkPlanSpec, DriverError> {
    let tidb_ast::UpdateKind::Single(table_ref) = &update.kind else {
        return Err(DriverError::unsupported(
            "multi-table UPDATE plans are not supported yet",
        ));
    };
    let (database, table) = tidb_infoschema::split_table_path(&table_ref.name, current_db)?;
    Ok(FkPlanSpec {
        database: database.to_owned(),
        table: table.to_owned(),
        updated_cols: update
            .assignments
            .iter()
            .filter_map(|assignment| assignment.col.last().cloned())
            .collect(),
        replace: false,
        on_duplicate_cols: Vec::new(),
    })
}

/// Go `buildOnDeleteFKTrigger`'s target: the DELETE's single table.
///
/// # Errors
///
/// A multi-table DELETE or a name without a database.
pub fn fk_spec_for_delete(
    delete: &tidb_ast::DeleteStmt,
    current_db: &str,
) -> Result<FkPlanSpec, DriverError> {
    let tidb_ast::DeleteKind::Single(table_ref) = &delete.kind else {
        return Err(DriverError::unsupported(
            "multi-table DELETE plans are not supported yet",
        ));
    };
    let (database, table) = tidb_infoschema::split_table_path(&table_ref.name, current_db)?;
    Ok(FkPlanSpec {
        database: database.to_owned(),
        table: table.to_owned(),
        updated_cols: Vec::new(),
        replace: false,
        on_duplicate_cols: Vec::new(),
    })
}

///
/// The SELECT carries the statement's hints, as Go's `tryUpdatePointPlan`
/// builds its `SelectStmt{TableHints: updateStmt.TableHints}` and
/// `buildUpdate` pushes them for the read.
pub fn update_source_query(update: &tidb_ast::UpdateStmt) -> Option<tidb_ast::QueryStmt> {
    match &update.kind {
        tidb_ast::UpdateKind::Single(table_ref) => Some(multi_dml_select(
            update.with.as_ref(),
            &update.hints,
            &crate::plan_builder::dml_source::single_table_join(table_ref),
            update.where_clause.as_ref(),
            &update.order_by,
            update.limit.as_ref(),
        )),
        tidb_ast::UpdateKind::Multi { .. } => None,
    }
}

/// Go `tryUpdatePointPlan` declines the complete fast DML plan when any SET
/// expression contains a subquery. The ordinary builder must then attach the
/// subquery plan while rewriting the assignment.
pub fn try_update_point_plan(update: &tidb_ast::UpdateStmt) -> bool {
    !update
        .assignments
        .iter()
        .any(|assignment| expr_has_subquery(&assignment.value))
}

/// Go `tryUpdatePointPlan`'s `buildOrderedList`: the assignments of a point
/// UPDATE as expressions over the point plan's row.
pub fn fast_update_ordered_list(
    hooks: &dyn crate::optimize::PlanningHooks,
    update: &tidb_ast::UpdateStmt,
    assignments: &[crate::plan_builder::single_update::SingleUpdateAssignment],
    fast: &tidb_planner_core_operator_physicalop::PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &StmtContext,
    plan_ids: &tidb_planner_core_base::PlanIdAllocator,
    column_ids: &tidb_planner_core_base::ColumnIdAllocator,
) -> Result<Vec<Option<Expression>>, DriverError> {
    let tidb_ast::UpdateKind::Single(table_ref) = &update.kind else {
        return Ok(Vec::new());
    };
    let schema = fast
        .schema()
        .ok_or_else(|| DriverError::unsupported("a fast UPDATE's point plan has no schema"))?;
    let targets: Vec<(Vec<String>, Option<tidb_ast::Expr>)> = update
        .assignments
        .iter()
        .map(|assignment| assignment.col.clone())
        .zip(
            assignments
                .iter()
                .map(|assignment| assignment.value.clone()),
        )
        .collect();
    point_update_ordered_list(
        hooks, table_ref, schema, &targets, catalog, current_db, ctx, plan_ids, column_ids,
    )
    .map_err(crate::optimize::planner_error_to_driver)
}

/// Go `buildOrderedList`'s resolution for a fast point UPDATE: every SET item
/// resolved to its column (see `PlanBuilder::resolve_single_update_assignments`).
pub fn update_assignments_for_plan(
    update: &tidb_ast::UpdateStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &StmtContext,
) -> Result<Vec<crate::plan_builder::single_update::SingleUpdateAssignment>, DriverError> {
    single_update_assignments(update, catalog, current_db, ctx)
        .map_err(crate::optimize::planner_error_to_driver)
}

/// The Update root's assignments of a single-table UPDATE: each SET item with
/// a value, by its place in the list.
pub fn single_update_assigns(
    assignments: &[crate::plan_builder::single_update::SingleUpdateAssignment],
) -> Vec<tidb_planner_core_operator_physicalop::UpdateAssign> {
    assignments
        .iter()
        .enumerate()
        .filter(|(_, assignment)| assignment.value.is_some())
        .map(
            |(index, assignment)| tidb_planner_core_operator_physicalop::UpdateAssign {
                table: 0,
                column: assignment.column,
                expr: index,
            },
        )
        .collect()
}

/// How a DML read decides the columns it keeps (see [`DeletePruning`]).
pub enum DmlKeep {
    /// A single-table statement: the planner prunes a DELETE's read.
    Auto,
    /// A multi-table DELETE: the planner prunes each target's read and lays
    /// out its rows.
    MultiDelete(crate::plan_builder::delete_pruning::DeleteMultiInput),
}

/// Whether a foreign key another table declares references the table a
/// single-table DELETE targets: Go's `len(del.FKCascades) > 0 ||
/// len(del.FKChecks) > 0`, which declines the read's column pruning.
pub fn delete_has_fk(
    select: &tidb_ast::SelectStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &StmtContext,
) -> bool {
    let Some(table_ref) = crate::point_get_plan::sole_table_ref(&select.from) else {
        return false;
    };
    let Ok((database, name)) = single_table_name(table_ref, current_db) else {
        return false;
    };
    ctx.foreign_key_checks() && !referring(catalog, &database, &name).is_empty()
}

/// The read a single-table DELETE plans (`buildDelete`'s source).
#[must_use]
pub fn delete_source_query(delete: &tidb_ast::DeleteStmt) -> Option<tidb_ast::QueryStmt> {
    match &delete.kind {
        tidb_ast::DeleteKind::Single(table_ref) => Some(multi_dml_select(
            delete.with.as_ref(),
            &delete.hints,
            &crate::plan_builder::dml_source::single_table_join(table_ref),
            delete.where_clause.as_ref(),
            &delete.order_by,
            delete.limit.as_ref(),
        )),
        tidb_ast::DeleteKind::Multi { .. } => None,
    }
}

/// Go `buildUpdate`/`buildDelete`'s read: the `FROM` join with the `WHERE`,
/// `ORDER BY` and `LIMIT` above it, as one SELECT for the planner, carrying
/// the statement's `WITH` clause and hints (`pushTableHints(stmt.TableHints,
/// 0)`).
pub fn multi_dml_select(
    with: Option<&tidb_ast::WithClause>,
    hints: &[tidb_ast::Hint],
    from: &tidb_ast::Join,
    where_clause: Option<&tidb_ast::Expr>,
    order_by: &[tidb_ast::OrderItem],
    limit: Option<&tidb_ast::Limit>,
) -> tidb_ast::QueryStmt {
    tidb_ast::QueryStmt::Select(Box::new(
        crate::plan_builder::dml_source::dml_source_select(
            with,
            hints,
            from.clone(),
            where_clause,
            order_by,
            limit,
        ),
    ))
}

/// Plan all writable targets before executing any source. Aliases of one
/// physical table contribute to one FK policy, as Go's tblID2UpdateColumns
/// and per-table FK maps do.
pub fn multi_dml_physical_plan(
    hooks: &dyn crate::optimize::PlanningHooks,
    dml: MultiDmlRef<'_>,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &StmtContext,
    visits: BuildVisits<'_>,
) -> Result<
    (
        tidb_planner_core_operator_physicalop::PhysicalPlan,
        Vec<std::sync::Arc<ScalarSubqueryEvalCtx>>,
    ),
    DriverError,
> {
    use FkPlanSpec;
    let mut specs: BTreeMap<(i64, String, String), FkPlanSpec> = BTreeMap::new();
    let mut add_target = |database: String, table: String, column: Option<String>| {
        let identity = match catalog.get_in(&database, &table) {
            Some(TableEntry::Kv(table)) => (table.table_id(), String::new(), String::new()),
            _ => (0, database.go_to_lower(), table.go_to_lower()),
        };
        let spec = specs.entry(identity).or_insert_with(|| FkPlanSpec {
            database,
            table,
            ..FkPlanSpec::default()
        });
        if let Some(column) = column {
            if !spec
                .updated_cols
                .iter()
                .any(|existing| existing.go_lower_eq(&column))
            {
                spec.updated_cols.push(column);
            }
        }
    };
    let mut delete_multi = None;
    let (operator, select) = match dml {
        MultiDmlRef::Update(update) => {
            let tidb_ast::UpdateKind::Multi { from, .. } = &update.kind else {
                return Err(DriverError::unsupported(
                    "multi-table UPDATE plan requires a join",
                ));
            };
            let select = multi_dml_select(
                update.with.as_ref(),
                &update.hints,
                from,
                update.where_clause.as_ref(),
                &update.order_by,
                update.limit.as_ref(),
            );
            // Go's preprocessor (1248 for an unnamed derived table) runs
            // before the planner builds anything.
            crate::preprocess::validate_query_usage(&select, ctx)?;
            for (database, table, column) in
                update_target_columns(update, catalog, current_db, ctx)?
            {
                add_target(database, table, Some(column));
            }
            ("Update", select)
        }
        MultiDmlRef::Delete(delete) => {
            let tidb_ast::DeleteKind::Multi { from, .. } = &delete.kind else {
                return Err(DriverError::unsupported(
                    "multi-table DELETE plan requires a join",
                ));
            };
            let select = multi_dml_select(
                delete.with.as_ref(),
                &delete.hints,
                from,
                delete.where_clause.as_ref(),
                &[],
                None,
            );
            crate::preprocess::validate_query_usage(&select, ctx)?;
            for target in delete_targets(delete, catalog, current_db, ctx)
                .map_err(crate::optimize::planner_error_to_driver)?
            {
                add_target(target.database, target.table, None);
            }
            delete_multi = Some(delete_multi_input(delete, catalog, current_db, ctx)?);
            ("Delete", select)
        }
    };
    let specs: Vec<_> = specs.into_values().collect();
    let keep = match delete_multi {
        Some(input) => DmlKeep::MultiDelete(input),
        None => DmlKeep::Auto,
    };
    physical_multi_dml_plan(
        hooks, operator, &select, dml, catalog, current_db, ctx, &specs, keep, visits,
    )
}

/// What the planner needs to prune a multi-table DELETE's read and lay out
/// its targets' rows (Go `pruneAndBuildColPositionInfoForDelete`): the
/// resolved target tables, and whether a foreign key references any of them,
/// which declines the pruning for every table.
pub fn delete_multi_input(
    delete: &tidb_ast::DeleteStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &StmtContext,
) -> Result<crate::plan_builder::delete_pruning::DeleteMultiInput, DriverError> {
    use crate::plan_builder::delete_pruning::DeleteMultiInput;
    let targets = delete_targets(delete, catalog, current_db, ctx)
        .map_err(crate::optimize::planner_error_to_driver)?;
    let has_fk = ctx.foreign_key_checks()
        && targets
            .iter()
            .any(|target| !referring(catalog, &target.database, &target.table).is_empty());
    Ok(DeleteMultiInput { targets, has_fk })
}

/// The EXPLAIN plan of a multi-table `UPDATE`/`DELETE`: Go's `Update`/
/// `Delete` root over the optimized join `SelectPlan`. `None` for a
/// single-table statement, which keeps its own builder.
pub fn multi_dml_explain_plan(
    hooks: &dyn crate::optimize::PlanningHooks,
    dml: MultiDmlRef<'_>,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &StmtContext,
) -> Result<
    Option<(
        tidb_planner_core_operator_physicalop::PhysicalPlan,
        Vec<std::sync::Arc<ScalarSubqueryEvalCtx>>,
    )>,
    DriverError,
> {
    match dml {
        MultiDmlRef::Update(update) => {
            let tidb_ast::UpdateKind::Multi { .. } = &update.kind else {
                return Ok(None);
            };
            multi_dml_physical_plan(
                hooks,
                MultiDmlRef::Update(update),
                catalog,
                current_db,
                ctx,
                BuildVisits::Check,
            )
            .map(Some)
        }
        MultiDmlRef::Delete(delete) => {
            let tidb_ast::DeleteKind::Multi { .. } = &delete.kind else {
                return Ok(None);
            };
            multi_dml_physical_plan(
                hooks,
                MultiDmlRef::Delete(delete),
                catalog,
                current_db,
                ctx,
                BuildVisits::Check,
            )
            .map(Some)
        }
    }
}

/// Which multi-table write [`multi_dml_explain_plan`] plans.
#[derive(Clone, Copy)]
pub enum MultiDmlRef<'a> {
    /// An UPDATE.
    Update(&'a tidb_ast::UpdateStmt),
    /// A DELETE.
    Delete(&'a tidb_ast::DeleteStmt),
}

/// Go `buildUpdate`'s assignment targets resolved to their columns.
///
/// # Errors
///
/// A target that does not resolve.
pub fn update_target_columns(
    update: &tidb_ast::UpdateStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &StmtContext,
) -> Result<Vec<(String, String, String)>, DriverError> {
    let source = catalog
        .planner_catalog(current_db, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let targets = PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, ctx.session_zone())
        .for_name_resolution()
        .resolve_update_targets(update)
        .map_err(crate::optimize::planner_error_to_driver)?;
    Ok(targets
        .into_iter()
        .map(|target| (target.database, target.table, target.column))
        .collect())
}

/// Builds a query below a non-query physical root using that statement's
/// plan and column allocators. Go's DML builders allocate the write root and
/// its `SelectPlan` from the same session counters; keeping those counters
/// shared preserves both tree ownership and explain identities.
#[allow(clippy::too_many_arguments)]
pub fn physical_query_plan_with_allocators(
    hooks: &dyn crate::optimize::PlanningHooks,
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    visits: BuildVisits<'_>,
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
    let registry = ScalarSubqueryRegistry::default();
    let parts = BuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
        session_ctx: None,
        registry: &registry,
        hooks,
        on_delete_root: None,
    };
    crate::optimize::optimize_query(&parts, query, visits, QueryBuild::DmlRead)
        .map(|(_, physical)| physical)
}

/// Go `buildUpdateLists`'s resolution of a single-table UPDATE's SET items.
pub fn single_update_assignments(
    update: &tidb_ast::UpdateStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
) -> Result<
    Vec<crate::plan_builder::single_update::SingleUpdateAssignment>,
    tidb_planner_core_base::PlanError,
> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx);
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, ctx.session_zone())
        .for_name_resolution()
        .resolve_single_update_assignments(update, ctx.allow_write_row_id())
}

/// The shared DML-source build, following Go's allocation order:
/// the source builds logically, `buildSelectLock` wraps the single-table
/// UPDATE/DELETE source (`logical_plan_builder.go:6117/6552`, pessimistic
/// mode), the `Update`/`Delete` root allocates, and only then `DoOptimize`
/// lowers the tree. Returns the physical source, the UPDATE assignment
/// expressions, and the allocated DML root base for `PhysicalDmlRoot`.
///
/// `write` is the statement whose `visitInfo` the build collects, Go's
/// `buildUpdate` / `buildDelete` over one `PlanBuilder`: the read's (a
/// non-qualified DELETE pops its last, `popVisitInfo`), then the write's
/// (`UpdatePriv` per assigned table, `DeletePriv` per target); `None` for a
/// cached plan's rebuild, whose statement checks the visit infos PREPARE
/// kept.
#[allow(clippy::too_many_arguments)]
pub fn physical_dml_source_plan_with_allocators(
    hooks: &dyn crate::optimize::PlanningHooks,
    select: &tidb_ast::SelectStmt,
    single_update: Option<&tidb_ast::UpdateStmt>,
    multi_update: Option<&tidb_ast::UpdateStmt>,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    operator: &str,
    delete_prune_has_fk: Option<bool>,
    delete_multi: Option<crate::plan_builder::delete_pruning::DeleteMultiInput>,
    on_delete_root: Option<&dyn Fn()>,
    write: Option<MultiDmlRef<'_>>,
) -> Result<DmlSourcePlan, tidb_planner_core_base::PlanError> {
    // Go builds an UPDATE/DELETE inside `builder.Build` (`buildUpdate` /
    // `buildDelete` run the read plan's `DoOptimize` themselves), so its
    // `DurationRewrite` covers the read plan's optimization too, and the
    // non-logical `Update`/`Delete` never starts `optimize`'s total clock.
    ctx.phase_durations().reset_rewrite_phase_info();
    let begin_rewrite = std::time::Instant::now();
    let registry = ScalarSubqueryRegistry::default();
    let parts = BuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
        session_ctx: None,
        registry: &registry,
        hooks,
        on_delete_root,
    };
    // Go `buildUpdate` / `buildDelete` run inside `PlanBuilder.Build`.
    let built = {
        parts.build(None, None, |builder| {
            let source = builder.source;
            let session_zone = ctx.session_zone();
            builder.delete_prune_has_fk = delete_prune_has_fk;
            builder.delete_multi = delete_multi;
            builder.allocate_delete_root = operator.eq_ignore_ascii_case("Delete");
            // Go `buildUpdate`/`buildDelete` lock the read only for a single-table
            // source (`TableRefs.Right == nil` / `!IsMultiTable`); a multi-table
            // write locks the rows it writes instead.
            let single_source = select.from.as_ref().is_some_and(|from| {
                from.right.is_none() && matches!(from.left, tidb_ast::JoinNode::Table(_))
            });
            let wrap_lock = ctx.pessimistic_transaction() && single_source;
            let mut single_assignments = None;
            let (plan, mut update_expressions, mut plan_flags) = match (single_update, multi_update)
            {
                (Some(update), _) => {
                    let (plan, expressions, assignments, flags) = builder.build_update_dml_source(
                        select,
                        update,
                        ctx.allow_write_row_id(),
                        wrap_lock,
                    )?;
                    single_assignments = Some(assignments);
                    (plan, expressions, flags)
                }
                (None, Some(update)) => builder.build_multi_update_dml_source(
                    select,
                    update,
                    ctx.allow_write_row_id(),
                )?,
                (None, None) => {
                    let (plan, build_flags) = builder.build_dml_source(select, wrap_lock)?;
                    (plan, Vec::new(), build_flags)
                }
            };
            ctx.set_block_as_names(builder.statement_block_as_names());
            // Go `buildUpdate` / `buildDelete`: the write's visit infos follow the
            // read's, before the read is optimized. The write targets resolve on
            // their own allocators: resolving a joined UPDATE's columns walks its
            // table refs again, which must not shift this build's plan ids.
            let mut visit_info = std::mem::take(&mut builder.visit_info);
            if let Some(write) = write {
                let (target_plan_ids, target_column_ids) =
                    (PlanIdAllocator::new(), ColumnIdAllocator::new());
                let mut targets = PlanBuilder::new(
                    source,
                    ctx,
                    &target_plan_ids,
                    &target_column_ids,
                    session_zone.clone(),
                )
                .for_name_resolution();
                match write {
                    MultiDmlRef::Update(update) => {
                        visit_info.extend(targets.update_write_visits(update)?);
                    }
                    MultiDmlRef::Delete(delete) => {
                        // "Only an unqualified single-table delete can omit SELECT
                        // privileges. Joined tables can be read by ON/USING even
                        // without a WHERE clause."
                        if delete.where_clause.is_none()
                            && delete.order_by.is_empty()
                            && deletes_from_a_single_table_name(delete)
                        {
                            visit_info.pop();
                        }
                        visit_info.extend(targets.delete_write_visits(delete)?);
                    }
                }
            }
            // Go `buildUpdate`: "We cannot apply projection elimination when building
            // the subplan, because columns in orderedList cannot be resolved" -- the
            // assignments live outside the plan, so a rewrite that substitutes a
            // projected column never reaches them (`DoOptimize(..., b.optFlag &^
            // rule.FlagEliminateProjection, p)`).
            if operator.eq_ignore_ascii_case("Update") {
                plan_flags &= !tidb_planner_core_rule::flags::ELIMINATE_PROJECTION;
            }
            // Go `buildUpdate`/`buildDelete` allocate the `Update`/`Delete` root
            // after the logical build (which now includes `buildSelectLock` and the
            // freezing projection) and before `DoOptimize`. The optimize pass then
            // allocates the physical projection candidate (Go id 6 in the captured
            // W51 receipt) before the physical `SelectLock` — reproducing Go's
            // exact id sequence without a filler.
            let root = builder.dml_root.take().unwrap_or_else(|| {
                tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(plan_ids, operator, 0)
            });
            let logical = optimize_built_logical(
                plan,
                plan_flags,
                catalog,
                ctx,
                use_plan_cache,
                plan_ids,
                column_ids,
                &session_zone,
            )?;
            // Go `buildUpdateLists`: the columns a non-constant assignment reads
            // join `StmtCtx.ColRefFromUpdatePlan`.
            let col_ref_from_update_plan: std::collections::BTreeSet<i64> = update_expressions
                .iter()
                .flatten()
                .flat_map(tidb_expr::simple_expr::extract_columns)
                .map(|column| column.unique_id)
                .collect();
            let physical = physical_plan_for_logical_with_update_refs(
                &logical,
                plan_ids,
                column_ids,
                catalog,
                ctx,
                &col_ref_from_update_plan,
            )?;
            let schema = physical.schema().ok_or_else(|| {
                tidb_planner_core_base::PlanError::internal("physical DML source has no schema")
            })?;
            for expression in update_expressions.iter_mut().flatten() {
                tidb_expr::simple_expr::resolve_indices_in_place(expression, schema).map_err(
                    |_| {
                        tidb_planner_core_base::PlanError::internal(
                            "UPDATE assignment does not resolve in its physical source",
                        )
                    },
                )?;
            }
            ctx.phase_durations()
                .set_duration_rewrite(begin_rewrite.elapsed());
            Ok(DmlSourcePlan {
                plan: physical,
                update_expressions,
                single_assignments,
                root,
                delete_col_pos: builder.delete_col_pos.take(),
                delete_tables: std::mem::take(&mut builder.delete_tables),
                update_tables: std::mem::take(&mut builder.update_tables),
                update_assigns: std::mem::take(&mut builder.update_assigns),
                visit_info,
                scalar_subqueries: Vec::new(),
            })
        })
    }?;
    let scalar_subqueries = registry.borrow().clone();
    Ok(DmlSourcePlan {
        scalar_subqueries,
        ..built
    })
}

/// What the shared DML-source build produces for the `Update`/`Delete` root.
pub struct DmlSourcePlan {
    /// The optimized physical read.
    pub plan: PhysicalPlan,
    /// The statement's visit infos (Go `builder.GetVisitInfo()`): the read's,
    /// then the write's.
    pub visit_info: Vec<crate::plan_builder::visit_info::VisitInfo>,
    /// The UPDATE assignments' expressions, resolved against the read.
    pub update_expressions: Vec<Option<tidb_expr::expression::Expression>>,
    /// The allocated root base.
    pub root: tidb_planner_core_operator_physicalop::BasePhysicalPlan,
    /// A pruned single-table DELETE's layout.
    pub delete_col_pos: Option<tidb_planner_core_operator_physicalop::DeleteColPos>,
    /// A multi-table DELETE's layouts.
    pub delete_tables: Vec<tidb_planner_core_operator_physicalop::DeleteTablePos>,
    /// A multi-table UPDATE's layouts.
    pub update_tables: Vec<tidb_planner_core_operator_physicalop::UpdateTablePos>,
    /// A multi-table UPDATE's assignments.
    pub update_assigns: Vec<tidb_planner_core_operator_physicalop::UpdateAssign>,
    /// A single-table UPDATE's SET items, resolved over the read (Go
    /// `buildUpdateLists`).
    pub single_assignments: Option<Vec<crate::plan_builder::single_update::SingleUpdateAssignment>>,
    /// The scalar subqueries the read evaluated while it was built (Go
    /// `SessionVars.MapScalarSubQ`), which EXPLAIN renders after the plan.
    pub scalar_subqueries: Vec<std::sync::Arc<ScalarSubqueryEvalCtx>>,
}

/// Go `buildOrderedList` for the fast point UPDATE (`tryUpdatePointPlan`): the
/// assignments rewritten over the point plan's schema and cast to their
/// target columns, resolved to that schema's row.
pub fn point_update_ordered_list(
    hooks: &dyn crate::optimize::PlanningHooks,
    table_ref: &tidb_ast::TableRef,
    schema: &tidb_expr::schema::Schema,
    assignments: &[(Vec<String>, Option<tidb_ast::Expr>)],
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
) -> Result<Vec<Option<Expression>>, tidb_planner_core_base::PlanError> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let defaults = |database: &str, table: &str, column: &str| {
        hooks.field_default(ctx, database, table, column)
    };
    let builder = PlanBuilder::new(&source, ctx, plan_ids, column_ids, ctx.session_zone())
        .with_default_evaluator(&defaults);
    let mut list = builder.build_point_ordered_list(table_ref, schema, assignments)?;
    for expression in list.iter_mut().flatten() {
        tidb_expr::simple_expr::resolve_indices_in_place(expression, schema).map_err(|_| {
            tidb_planner_core_base::PlanError::internal(
                "a fast UPDATE assignment does not resolve in its point plan",
            )
        })?;
    }
    Ok(list)
}

/// Go `buildDelete`'s resolution of a multi-table DELETE's targets.
pub fn delete_targets(
    delete: &tidb_ast::DeleteStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
) -> Result<Vec<crate::plan_builder::delete_pruning::DeleteTarget>, tidb_planner_core_base::PlanError>
{
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, ctx.session_zone())
        .for_name_resolution()
        .resolve_delete_targets(delete)
}

/// Go `buildFKCheckForReferredFK`'s index, computed on demand: every
/// `(schema, table, foreign key)` whose constraint REFERS to `database.table`.
pub fn referring(
    catalog: &SessionExtendedInfoSchema,
    database: &str,
    table: &str,
) -> Vec<(
    String,
    String,
    tidb_model::GoShared<tidb_model::table::FKInfo>,
)> {
    // Sysbench and the normal TiDB bootstrap have no foreign-key declarations.
    // Avoid rebuilding and sorting the complete catalog path list for every
    // UPDATE/DELETE in that case; catalogs that do declare one retain the
    // deterministic scan below.
    if !catalog.has_foreign_keys() {
        return Vec::new();
    }
    if let Some(v2) = catalog.v2() {
        let mut found = Vec::new();
        // Go buildOnDeleteOrUpdateFKTrigger resolves only indexed children
        // and deliberately ignores a TableByName error at this boundary.
        for referred in
            v2.get_table_referred_foreign_keys(&database.go_to_lower(), &table.go_to_lower())
        {
            let Some(TableEntry::Kv(child)) = catalog.get_in(
                referred.child_schema.original(),
                referred.child_table.original(),
            ) else {
                continue;
            };
            if let Some(fk) = child.meta().read().foreign_keys.iter_deref().find(|fk| {
                let fk = fk.read();
                fk.name.lowercase() == referred.child_fk_name.lowercase()
                    && fk.state == tidb_model::SchemaState::PUBLIC
                    && fk.version >= tidb_model::table::FK_VERSION1
            }) {
                found.push((
                    referred.child_schema.lowercase().to_owned(),
                    referred.child_table.lowercase().to_owned(),
                    fk,
                ));
            }
        }
        return found;
    }
    let mut found = Vec::new();
    for (child_db, child_table, entry) in catalog.table_entries() {
        let TableEntry::Kv(child) = entry else {
            continue;
        };
        for foreign_key in child.meta().read().foreign_keys.iter_deref() {
            let key = foreign_key.read();
            if key.state == tidb_model::SchemaState::PUBLIC
                && key.version >= tidb_model::table::FK_VERSION1
                && key.ref_schema.original().go_lower_eq(database)
                && key.ref_table.original().go_lower_eq(table)
            {
                drop(key);
                found.push((child_db.clone(), child_table, foreign_key));
            }
        }
    }
    // Preserve the existing schema/table cascade order and declaration order
    // within each child. Only matching constraints need owned metadata: the
    // cascade mutates the catalog after this borrow ends. Unrelated tables
    // contribute no name, column, or constraint copies.
    found.sort_by(|left, right| (&left.0, left.1).cmp(&(&right.0, right.1)));
    found
        .into_iter()
        .map(|(database, table, key)| (database, table.to_owned(), key))
        .collect()
}

/// The cascade statement Go's `buildFKCascadePlan` generates.
pub enum FKCascadeStmt {
    /// `GenCascadeDeleteAST`.
    Delete(tidb_ast::DeleteStmt),
    /// `GenCascadeSetNullAST` / `GenCascadeUpdateAST`.
    Update(tidb_ast::UpdateStmt),
}

/// Whether `expr` contains a subquery.
#[must_use]
pub fn expr_has_subquery(expr: &tidb_ast::Expr) -> bool {
    use tidb_ast::Expr;
    match expr {
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. } => true,
        Expr::Paren(inner) | Expr::Unary(_, inner) | Expr::Is { expr: inner, .. } => {
            expr_has_subquery(inner)
        }
        Expr::Binary(_, lhs, rhs) => expr_has_subquery(lhs) || expr_has_subquery(rhs),
        Expr::In { expr, list, .. } => {
            expr_has_subquery(expr) || list.iter().any(expr_has_subquery)
        }
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            value.as_deref().is_some_and(expr_has_subquery)
                || when_clauses.iter().any(|(condition, result)| {
                    expr_has_subquery(condition) || expr_has_subquery(result)
                })
                || else_clause.as_deref().is_some_and(expr_has_subquery)
        }
        Expr::Func { args, .. } | Expr::Aggregate { args, .. } => {
            args.iter().any(expr_has_subquery)
        }
        _ => false,
    }
}

/// The single table a cascade statement writes (`genTableRefsAST`).
pub trait CascadeTarget {
    /// The statement's one target table, `None` for a multi-table form.
    fn kind_table(&self) -> Option<&tidb_ast::TableRef>;
}

impl CascadeTarget for tidb_ast::DeleteStmt {
    fn kind_table(&self) -> Option<&tidb_ast::TableRef> {
        match &self.kind {
            tidb_ast::DeleteKind::Single(table) => Some(table),
            tidb_ast::DeleteKind::Multi { .. } => None,
        }
    }
}

impl CascadeTarget for tidb_ast::UpdateStmt {
    fn kind_table(&self) -> Option<&tidb_ast::TableRef> {
        match &self.kind {
            tidb_ast::UpdateKind::Single(table) => Some(table),
            tidb_ast::UpdateKind::Multi { .. } => None,
        }
    }
}

/// Go `getSingleTableNameAndAlias(ds.TableRefs) != nil`: the statement reads
/// one table, named directly (no join, no derived table).
fn deletes_from_a_single_table_name(delete: &tidb_ast::DeleteStmt) -> bool {
    match &delete.kind {
        tidb_ast::DeleteKind::Single(_) => true,
        tidb_ast::DeleteKind::Multi { from, .. } => {
            from.right.is_none() && matches!(from.left, tidb_ast::JoinNode::Table(_))
        }
    }
}
