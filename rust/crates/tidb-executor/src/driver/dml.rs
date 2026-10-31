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

//! The write paths: `INSERT` / `UPDATE` / `DELETE`, plus the column-default,
//! cast, row-ordering and `ON DUPLICATE KEY UPDATE` machinery they share.
//!
//! Mirrors Go's `PlanBuilder.buildInsert` / `buildUpdate` / `buildDelete` and
//! the `executor` package's `InsertExec` / `UpdateExec` / `DeleteExec`.

use tidb_planner::physical_plan_cache::RebuildPlanForCache;
use crate::KvTableReader;
use super::plan_cache::{CachedPhysicalPlan, PhysicalPlanCacheKey, SessionPlanCache};
use super::*;
use tidb_table::kv_table::AutoRandomError;
use tidb_table::kv_table::AutoRandom;
use tidb_table::kv_table::AutoIncrement;
use tidb_autoid::AutoIdError;

mod defaults;
pub(crate) mod delete_record;
pub(crate) mod exchange_partition;
pub(crate) mod update_record;

use delete_record::DeleteRecords;
use tidb_planner_core_operator_physicalop::FkTriggerNode;
use update_record::UpdateRecords;


pub(crate) use defaults::{
    column_default, column_metadata, kv_column_metadata, materialize_column_default,
    stored_column_metadata, ColumnDefaultMeta, DefaultUse, PreparedOnUpdateNow,
    set_value_for_ref_column,
};

/// Parses and runs a plain `INSERT INTO t [(cols)] VALUES (...), ...` against
/// `catalog`, returning the number of inserted rows.
///
/// The shared table writer supports the normal insert
/// forms, including `REPLACE`, `IGNORE`, `ON DUPLICATE KEY UPDATE`, `SET`
/// syntax, query sources, and `PARTITION (...)` destination validation.
/// A `RETURNING` clause is parsed and silently ignored: Go's hand-written
/// parser stores it on the AST but the planner and executor never read it, so
/// the write runs normally and answers with a plain OK packet.
pub fn run_insert_on(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_insert_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_insert_on`] resolving unqualified names in `current_db`.
pub fn run_insert_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_insert_reporting(sql, catalog, current_db, ctx).map(|outcome| outcome.0)
}

/// [`run_insert_in`], also reporting the first auto-increment id the statement
/// allocated, which is what MySQL answers with as `LAST_INSERT_ID`.
///
/// `None` when the statement allocated nothing: an explicit auto value or a
/// table with no auto column leaves the session's value untouched, which is
/// the behavior captured from TiDB.
pub fn run_insert_reporting(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(u64, Option<u64>), DriverError> {
    let stmt = ctx.parse(sql)?;

    let insert = match &stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Insert(insert) => insert,
            _ => return Err(DriverError::unsupported("only INSERT is supported here")),
        },
        _ => return Err(DriverError::unsupported("only INSERT is supported here")),
    };
    run_insert_stmt(insert, catalog, current_db, ctx)
}

/// [`run_insert_reporting`], starting from an already-parsed `InsertStmt`
/// rather than re-parsing a SQL string -- what `EXPLAIN ANALYZE INSERT`
/// needs (it already holds the parsed statement the `EXPLAIN` wraps, and
/// real `EXPLAIN ANALYZE` executes the wrapped statement, captured via
/// `pkg/executor`: an `EXPLAIN ANALYZE INSERT` really inserts the row).
pub fn run_insert_stmt(
    insert: &tidb_ast::InsertStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(u64, Option<u64>), DriverError> {
    run_insert_stmt_with_physical(insert, catalog, current_db, ctx, None)
}

/// The ordinary INSERT executor with an optional already-selected physical
/// source. A fresh statement builds its `Insert.SelectPlan` here; a prepared
/// cache hit passes the child rebuilt by the shared cache visitor.
pub fn run_insert_stmt_with_physical(
    insert: &tidb_ast::InsertStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    physical_plan: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
) -> Result<(u64, Option<u64>), DriverError> {
    run_insert_stmt_with_physical_and_stats(insert, catalog, current_db, ctx, physical_plan, None)
}

pub(crate) fn run_insert_stmt_with_physical_and_stats(
    insert: &tidb_ast::InsertStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    physical_plan: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
) -> Result<(u64, Option<u64>), DriverError> {
    // go resolves the INSERT's target table before planning the source
    // query: `INSERT INTO h1 (a, b) SELECT a, b FROM no_such` over a missing
    // h1 errors `Table 'fdq.h1' doesn't exist`, not the source query's own
    // 1146 (oracle m13).
    resolve_insert_target(insert, catalog, current_db, ctx)?;
    // Go `planner.Optimize`.
    let optimize = super::start_optimize_region();
    let mut fresh = physical_plan
        .is_none()
        .then(|| physical_insert_plan(insert, catalog, current_db, ctx))
        .transpose()?;
    let mut physical_plan = physical_plan.or(fresh.as_mut());
    // Go `buildInsert` evaluates the values' uncorrelated subqueries while
    // it plans: the VALUES lists, then (after an INSERT ... SELECT's source
    // was optimized) the ON DUPLICATE assignments.
    let planned = {
        let select = match physical_plan.as_deref() {
            Some(tidb_planner_core_operator_physicalop::PhysicalPlan::Dml(root)) => root
                .select_plan
                .as_deref()
                .and_then(|plan| plan.schema())
                .zip(root.insert_select.as_ref()),
            _ => None,
        };
        plan_insert(insert, select, catalog, current_db, ctx)?
    };
    optimize.end();
    if let Some(plan) = physical_plan.as_deref_mut() {
        super::physical_builder::prepare_execution_plan(plan, catalog, ctx)?;
        crate::explain::publish_physical_process_info(ctx, plan, catalog);
    }
    // Go `runStmt` -> `handleNoDelayExecutor` -> the DML executor's `Next`.
    let _execution = super::begin_dml_execution(if insert.replace {
        "*executor.ReplaceExec.Next"
    } else {
        "*executor.InsertExec.Next"
    });
    let (physical_source, fk_triggers, _, _) = dml_execution_parts(physical_plan, "Insert")?;
    run_insert_with_physical(
        insert,
        Some(&planned),
        catalog,
        current_db,
        ctx,
        physical_source,
        fk_triggers,
        runtime,
    )
}

/// Builds the physical plan of an INSERT: its source (Go's ON DUPLICATE
/// extension of it for an `INSERT ... SELECT`), the foreign key triggers, and
/// what the planner decided about the source's outputs.
pub(crate) fn physical_insert_plan(
    insert: &tidb_ast::InsertStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, DriverError> {
    let extended = super::planner_bridge::insert_extended_source(insert, catalog, current_db, ctx);
    let (source, actual_col_len) = match &extended {
        Some((query, width)) => (Some(query), Some(*width)),
        None => (insert.source.as_deref(), None),
    };
    physical_dml_plan_with_cache_mode(
        "Insert",
        source,
        None,
        catalog,
        current_db,
        ctx,
        false,
        std::slice::from_ref(&fk_spec_for_insert(insert, catalog, current_db, ctx)?),
        DmlKeep::Auto,
        actual_col_len,
    )
}

fn dml_execution_parts<'a>(
    plan: Option<&'a mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    operator: &str,
) -> Result<
    (
        Option<&'a mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
        &'a [FkTriggerNode],
        Option<&'a tidb_planner_core_operator_physicalop::DeleteColPos>,
        &'a [tidb_planner_core_operator_physicalop::DeleteTablePos],
    ),
    DriverError,
> {
    let Some(tidb_planner_core_operator_physicalop::PhysicalPlan::Dml(root)) = plan else {
        return Err(DriverError::unsupported(
            "DML execution has no physical root",
        ));
    };
    if root.go_operator != operator {
        return Err(DriverError::unsupported(
            "DML execution received the wrong physical root",
        ));
    }
    Ok((
        root.select_plan.as_deref_mut(),
        root.fk_triggers.as_slice(),
        root.delete_col_pos.as_ref(),
        root.delete_tables.as_slice(),
    ))
}

/// Builds and prepares the same DML plan for execution and EXPLAIN.
#[allow(clippy::too_many_arguments)]
pub(crate) fn physical_dml_plan(
    operator: &str,
    source: Option<&tidb_ast::QueryStmt>,
    update: Option<&tidb_ast::UpdateStmt>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    fk_spec: &fk_trigger_plan::FkPlanSpec,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, DriverError> {
    physical_dml_plan_with_cache_mode(
        operator,
        source,
        update,
        catalog,
        current_db,
        ctx,
        false,
        std::slice::from_ref(fk_spec),
        DmlKeep::Auto,
        None,
    )
}

/// Multi-table callers carry all resolved table policies into the same DML
/// allocator and source builder used by single-table writes.
pub(crate) fn physical_multi_dml_plan(
    operator: &str,
    source: &tidb_ast::QueryStmt,
    update: Option<&tidb_ast::UpdateStmt>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    fk_specs: &[fk_trigger_plan::FkPlanSpec],
    keep: DmlKeep,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, DriverError> {
    physical_dml_plan_with_cache_mode(
        operator,
        Some(source),
        update,
        catalog,
        current_db,
        ctx,
        false,
        fk_specs,
        keep,
        None,
    )
}

/// Shared INSERT/UPDATE/DELETE construction, following Go's
/// plan-builder allocation order and DML-local plan nodes:
///
/// * Go `buildInsert` allocates the physical `Insert` root, then the
///   `LogicalTableDual` mock plan (`planbuilder.go:4195`), then the source,
///   then `BuildOnInsertFKTriggers`. The mock plan never renders, but its id
///   shifts everything after it, so the explain path allocates one filler.
/// * Go's single-table point UPDATE/DELETE (`point_get_plan.go:1254/1374`)
///   builds the locked `PointGet` first; the UPDATE path allocates one extra
///   plan before the `Update` root, the DELETE path does not.
/// * Non-point UPDATE/DELETE build the source logically, allocate the
///   `Update`/`Delete` root, then `DoOptimize` — with `buildSelectLock`
///   wrapping the single-table source (`logical_plan_builder.go:6117/6552`).
/// * `BuildOn{Insert,Update,Delete}FKTriggers` runs last, allocating the
///   `Foreign_Key_Check` / `Foreign_Key_Cascade` leaves.
#[allow(clippy::too_many_arguments)]
fn physical_dml_plan_with_cache_mode(
    operator: &str,
    source: Option<&tidb_ast::QueryStmt>,
    update: Option<&tidb_ast::UpdateStmt>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    fk_specs: &[fk_trigger_plan::FkPlanSpec],
    keep: DmlKeep,
    insert_actual_col_len: Option<usize>,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, DriverError> {
    use tidb_planner_core_operator_physicalop::{BasePhysicalPlan, PhysicalDmlRoot, PhysicalPlan};

    for spec in fk_specs {
        if matches!(
            catalog.get_in(&spec.database, &spec.table),
            Some(TableEntry::Mem(_))
        ) {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_table::table_errors::ERR_UNSUPPORTED_OP.generate_with_stack_by_args(&[]),
            )));
        }
    }

    let plan_ids = tidb_planner_core_base::PlanIdAllocator::new();
    let column_ids = tidb_planner_core_base::ColumnIdAllocator::new();
    let mut update_expressions = Vec::new();
    let mut delete_col_pos = None;
    let mut delete_tables = Vec::new();
    let mut update_tables = Vec::new();
    let mut update_assigns = Vec::new();
    let mut insert_select = None;
    // Go `buildUpdateLists`: a single-table UPDATE's SET items, resolved once
    // for the fast point list and the ordinary builder alike.
    let single_assignments = match update {
        Some(update) if matches!(update.kind, tidb_ast::UpdateKind::Single(_)) => {
            Some(update_assignments_for_plan(update, catalog, current_db, ctx)?)
        }
        _ => None,
    };
    if let Some(assignments) = &single_assignments {
        update_assigns = assignments
            .iter()
            .enumerate()
            .filter(|(_, assignment)| assignment.value.is_some())
            .map(|(index, assignment)| tidb_planner_core_operator_physicalop::UpdateAssign {
                table: 0,
                column: assignment.column,
                expr: index,
            })
            .collect();
    }
    let mut fast_update_list = false;

    // Go `buildInsert` builds the target's schema first
    // (`TableInfo2SchemaAndNames`), which draws a plan column id for every
    // public column of the table before the source plan exists.
    if operator.eq_ignore_ascii_case("Insert") {
        if let Some(Some(TableEntry::Kv(table))) = fk_specs
            .first()
            .map(|spec| catalog.get_in(&spec.database, &spec.table))
        {
            for _ in 0..table.columns().len() {
                column_ids.alloc();
            }
        }
    }

    // Go `buildInsert`'s mockTablePlan filler, right after the Insert root.
    let mut root_base = if operator.eq_ignore_ascii_case("Insert") {
        let root = BasePhysicalPlan::new(&plan_ids, operator, 0);
        let _mock_table_plan = plan_ids.alloc();
        Some(root)
    } else {
        None
    };

    let select_plan = match source {
        None => None,
        // INSERT ... SELECT: Go's `buildSelectPlanOfInsert` builds the source
        // without a select lock (only UPDATE/DELETE lock their read), and the
        // Insert root plus its mockTablePlan filler are already allocated
        // above, ahead of the source. The output names are what ON DUPLICATE
        // assignments resolve against.
        Some(query) if operator.eq_ignore_ascii_case("Insert") => {
            let (physical, names) =
                super::planner_bridge::physical_query_plan_and_names_with_allocators(
                    query,
                    catalog,
                    current_db,
                    ctx,
                    use_plan_cache,
                    &plan_ids,
                    &column_ids,
                )
                .map_err(super::planner_error_to_driver)?;
            insert_select = Some(tidb_planner_core_operator_physicalop::InsertSelectOutput {
                names,
                actual_col_len: insert_actual_col_len,
            });
            Some(physical)
        }
        Some(tidb_ast::QueryStmt::Select(select)) => {
            let multi_update = update
                .filter(|update| matches!(update.kind, tidb_ast::UpdateKind::Multi { .. }));
            let allow_fast_plan = multi_update.is_none()
                && update.is_none_or(|update| update_allows_fast_plan(update));
            let fast = (!use_plan_cache && allow_fast_plan)
                .then(|| {
                    super::point_get_plan::try_fast_dml_point_physical_plan_with_allocator(
                        select, catalog, current_db, ctx, &plan_ids,
                    )
                })
                .transpose()?
                .flatten();
            if let Some(mut fast) = fast {
                // Go's point plans carry `Lock` for UPDATE and DELETE, the
                // batch one as well (`tryUpdatePointPlan`/`tryDeletePointPlan`).
                match &mut fast {
                    PhysicalPlan::PointGet(point) => point.lock = ctx.pessimistic_transaction(),
                    PhysicalPlan::BatchPointGet(batch) => {
                        batch.lock = ctx.pessimistic_transaction();
                    }
                    _ => {}
                }
                // Go's point-UPDATE builder allocates one extra plan between
                // the PointGet and the Update root; the DELETE builder does
                // not.
                if operator.eq_ignore_ascii_case("Update") {
                    let _point_update_filler = plan_ids.alloc();
                }
                root_base = Some(BasePhysicalPlan::new(&plan_ids, operator, 0));
                if let Some(update) = update {
                    update_expressions = fast_update_ordered_list(
                        update,
                        single_assignments.as_deref().unwrap_or_default(),
                        &fast,
                        catalog,
                        current_db,
                        ctx,
                        &plan_ids,
                        &column_ids,
                    )?;
                    fast_update_list = true;
                }
                Some(fast)
            } else {
                let update_assignment_values: Option<Vec<Option<tidb_ast::Expr>>> =
                    single_assignments.as_ref().map(|assignments| {
                        assignments
                            .iter()
                            .map(|assignment| assignment.value.clone())
                            .collect()
                    });
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
                let built =
                    super::planner_bridge::physical_dml_source_plan_with_allocators(
                        select,
                        update_assignment_values.as_deref(),
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
                    )
                    .map_err(super::planner_error_to_driver)?;
                update_expressions = built.update_expressions;
                delete_col_pos = built.delete_col_pos;
                delete_tables = built.delete_tables;
                update_tables = built.update_tables;
                if multi_update.is_some() {
                    update_assigns = built.update_assigns;
                }
                root_base = Some(built.root);
                Some(built.plan)
            }
        }
        Some(query) => Some(
            super::planner_bridge::physical_query_plan_with_allocators(
                query,
                catalog,
                current_db,
                ctx,
                use_plan_cache,
                &plan_ids,
                &column_ids,
            )
            .map_err(super::planner_error_to_driver)?,
        ),
    };

    let base = root_base.ok_or_else(|| DriverError::unsupported("DML build produced no root"))?;
    // Go `BuildOn{Insert,Update,Delete}FKTriggers`, last in the builders.
    let mut fk_triggers = Vec::new();
    for spec in fk_specs {
        fk_triggers.extend(fk_trigger_plan::build_fk_triggers(
            catalog, ctx, operator, spec, &plan_ids,
        )?);
    }
    // Go flattens all checks before all cascades, preserving table order.
    fk_triggers.sort_by_key(|node| node.operator == "Foreign_Key_Cascade");
    let mut plan = PhysicalPlan::Dml(PhysicalDmlRoot {
        base,
        go_operator: operator.to_owned(),
        select_plan: select_plan.map(Box::new),
        update_expressions,
        fk_triggers,
        delete_col_pos,
        delete_tables,
        fast_update_list,
        update_tables,
        update_assigns,
        insert_select,
    });
    super::physical_builder::prepare_plan_for_explain(&mut plan, catalog, ctx)?;
    Ok(plan)
}

/// Go `tnW.DBInfo.Name.L` / `tableInfo.Name` for the INSERT target, plus the
/// REPLACE / ON DUPLICATE facts `BuildOnInsertFKTriggers` branches on: the
/// columns the RESOLVED assignments write (`buildOnDuplicateUpdateColumns`).
pub(crate) fn fk_spec_for_insert(
    insert: &tidb_ast::InsertStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<fk_trigger_plan::FkPlanSpec, DriverError> {
    let (database, table) = tidb_infoschema::split_table_path(&insert.table, current_db)?;
    // An assignment the planner cannot resolve is raised by the statement's
    // own planning; the trigger selection falls back to the names as written.
    let on_duplicate_cols =
        super::planner_bridge::insert_on_duplicate_columns(insert, catalog, current_db, ctx)
            .unwrap_or_else(|_| {
                insert
                    .on_duplicate
                    .iter()
                    .filter_map(|assignment| assignment.col.last().cloned())
                    .collect()
            });
    Ok(fk_trigger_plan::FkPlanSpec {
        database: database.to_owned(),
        table: table.to_owned(),
        updated_cols: Vec::new(),
        replace: insert.replace,
        on_duplicate_cols,
    })
}

/// Go `buildTbl2UpdateColumns` narrowed to the SET column names.
pub(crate) fn fk_spec_for_update(
    update: &tidb_ast::UpdateStmt,
    current_db: &str,
) -> Result<fk_trigger_plan::FkPlanSpec, DriverError> {
    let tidb_ast::UpdateKind::Single(table_ref) = &update.kind else {
        return Err(DriverError::unsupported(
            "multi-table UPDATE plans are not supported yet",
        ));
    };
    let (database, table) = tidb_infoschema::split_table_path(&table_ref.name, current_db)?;
    Ok(fk_trigger_plan::FkPlanSpec {
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

pub(crate) fn fk_spec_for_delete(
    delete: &tidb_ast::DeleteStmt,
    current_db: &str,
) -> Result<fk_trigger_plan::FkPlanSpec, DriverError> {
    let tidb_ast::DeleteKind::Single(table_ref) = &delete.kind else {
        return Err(DriverError::unsupported(
            "multi-table DELETE plans are not supported yet",
        ));
    };
    let (database, table) = tidb_infoschema::split_table_path(&table_ref.name, current_db)?;
    Ok(fk_trigger_plan::FkPlanSpec {
        database: database.to_owned(),
        table: table.to_owned(),
        updated_cols: Vec::new(),
        replace: false,
        on_duplicate_cols: Vec::new(),
    })
}

struct InsertTargetLayout {
    database: String,
    table_name: String,
    column_list: Vec<(String, FieldType)>,
    target_offsets: Vec<usize>,
    generated_targets: Vec<bool>,
    column_meta: Vec<ColumnDefaultMeta>,
    /// Where `_tidb_rowid` sits in `column_list`, when the statement named
    /// it. Go appends the same pseudo-column at `len(tCols)` in `fillRow`
    /// and widens the row buffer by one (`initEvalBuffer`), so the value
    /// travels with the row and is taken off it as the record HANDLE rather
    /// than stored as a column.
    extra_handle_offset: Option<usize>,
}

/// Go's message for writing `_tidb_rowid` without `tidb_opt_write_row_id`,
/// raised as a plain error and so reaching the client as 1105.
pub(super) const WRITE_ROW_ID_REFUSED: &str =
    "insert, update and replace statements for _tidb_rowid are not supported";

/// Resolves everything owned by the INSERT target before an INSERT SELECT
/// source is executed. Go plans the target and rejects a view, sequence,
/// unknown field or generated target before opening the source executor; a
/// source with user-variable or sequence side effects must not run first.
fn resolve_insert_target(
    insert: &tidb_ast::InsertStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<InsertTargetLayout, DriverError> {
    let (database, table_name) = split_table_path(&insert.table, current_db)?;
    let (database, table_name) = (database.to_owned(), table_name.to_owned());
    let table = catalog.get_in(&database, &table_name).ok_or_else(|| {
        DriverError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .generate_with_stack_by_args(&[(&database).into(), (&table_name).into()]),
        )
    })?;
    if table.is_view() {
        return Err(DriverError::from(tidb_terror::MysqlError::unknown(
            format!("insert into view {} is not supported now", table_name),
        )));
    }
    if table.is_sequence() {
        return Err(DriverError::from(tidb_terror::MysqlError::unknown(
            format!("insert into sequence {} is not supported now", table_name),
        )));
    }
    let TableEntry::Kv(_) = table else {
        return Err(DriverError::Mysql(MysqlError::from_terror(
            &tidb_table::table_errors::ERR_UNSUPPORTED_OP.generate_with_stack_by_args(&[]),
        )));
    };
    // Go `getAffectCols` (`buildInsert`): the columns the statement writes,
    // `_tidb_rowid` among them for a heap table that names it.
    let planned = super::planner_bridge::insert_target(insert, catalog, current_db, ctx)
        .map_err(super::planner_error_to_driver)?;
    let tidb_planner::plan_builder::insert_values::InsertTarget {
        columns: column_list,
        generated: generated_targets,
        offsets: target_offsets,
        extra_handle_offset,
        ..
    } = planned;
    // Go `initInsertColumns`: `_tidb_rowid` is writable only under
    // `tidb_opt_write_row_id`.
    if extra_handle_offset.is_some() && !ctx.allow_write_row_id() {
        return Err(DriverError::unsupported(WRITE_ROW_ID_REFUSED));
    }
    let mut column_meta = column_metadata(table);
    // The pseudo-column carries the same shape as a stored one so every
    // per-target lookup below stays indexed the same way. It is never
    // generated, and its "default" is the NULL that means "allocate a
    // handle" (Go `adjustImplicitRowID`'s `!hasValue` branch).
    if extra_handle_offset.is_some() {
        column_meta.push(ColumnDefaultMeta {
            default_value: None,
            not_null: false,
            no_default_value: false,
            name: tidb_model::column::EXTRA_HANDLE_NAME.to_owned(),
            field_type: FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            column_info_version: 0,
            generated: false,
        });
    }
    // Go `initInsertColumns`' `table.CheckOnce`, after the planner's checks:
    // a column named twice, in the column list or the SET form.
    for (position, &offset) in target_offsets.iter().enumerate() {
        if target_offsets[..position].contains(&offset) {
            return Err(DriverError::from(
                (&tidb_table::table_errors::ERR_DUPLICATE_COLUMN).generate_with_stack(format!(
                    "Column '{}' specified twice",
                    column_list[offset].0
                )),
            ));
        }
    }
    Ok(InsertTargetLayout {
        database,
        table_name,
        column_list,
        target_offsets,
        generated_targets,
        column_meta,
        extra_handle_offset,
    })
}

/// Go `buildSelectPlanOfInsert`: once the select's width matches the target's,
/// a generated column among the targets is refused. The order is Go's own --
/// `insert into t (b) select * from t` over a generated `b` is a count error,
/// `insert into t select * from t` the generated-column one.
fn refuse_generated_insert_select_targets(layout: &InsertTargetLayout) -> Result<(), DriverError> {
    for &offset in &layout.target_offsets {
        if layout.generated_targets[offset] {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_BAD_GENERATED_COLUMN.generate_with_stack_by_args(&[
                    (layout.column_list[offset].0.clone()).into(),
                    layout.table_name.clone().into(),
                ]),
            ));
        }
    }
    Ok(())
}

/// Go's `buildInsert` checks the target and the width of every row before it
/// plans anything, so `EXPLAIN INSERT` refuses what the statement would. The
/// executor's own check lives in `execute_insert`, after the source ran.
pub(crate) fn validate_insert_for_explain(
    insert: &tidb_ast::InsertStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    select_width: Option<usize>,
) -> Result<(), DriverError> {
    let layout = resolve_insert_target(insert, catalog, current_db, ctx)?;
    let target_width = layout.target_offsets.len();
    if insert.source.is_some() {
        if select_width.is_some_and(|width| width != target_width) {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_WRONG_VALUE_COUNT_ON_ROW
                    .generate_with_stack_by_args(&[1.into()]),
            ));
        }
        return refuse_generated_insert_select_targets(&layout);
    }
    let names_a_column = insert.set_syntax || insert.columns_specified;
    let mut previous_width = target_width;
    for (index, values) in insert.rows.iter().enumerate() {
        let width = values.len();
        let expected = if index == 0 { target_width } else { previous_width };
        if (index > 0 || names_a_column || width > 0) && width != expected {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_WRONG_VALUE_COUNT_ON_ROW
                    .generate_with_stack_by_args(&[(index + 1).into()]),
            ));
        }
        previous_width = width;
    }
    Ok(())
}

/// What `buildInsert` plans besides the source: each VALUES list as the
/// planner's expressions, the ON DUPLICATE KEY UPDATE assignments, and how many
/// of an `INSERT ... SELECT`'s outputs fill the insert.
pub(crate) struct PlannedInsert {
    /// Go `Insert.Lists`: the planner's expressions over the target, in the
    /// order of the statement's columns; a generated column's own DEFAULT has
    /// none.
    lists: Vec<Vec<Option<Expression>>>,
    /// Go `Insert.OnDuplicate`, over `Schema4OnDuplicate`.
    on_duplicate: Vec<tidb_planner::plan_builder::insert_on_duplicate::OnDuplicateAssign>,
    /// Go `actualColLen`.
    actual_col_len: Option<usize>,
    /// Go `Insert.GenCols`: each generated column's expression over the
    /// table's complete stored row, per table column.
    generated: Vec<Option<Expression>>,
}

/// Go `buildInsert`: the VALUES lists, then (for an `INSERT ... SELECT`) the
/// width and generated-target checks of `buildSelectPlanOfInsert`, then
/// `ResolveOnDuplicate` -- all before any executor is built, with every
/// uncorrelated scalar subquery evaluated on the way (`handleScalarSubquery`'s
/// `EvalSubqueryFirstRow`). A value whose rewrite builds an Apply is Go's
/// "complex subqueries" refusal (issue 30626); an ON DUPLICATE assignment's
/// `ErrSubqueryMoreThan1Row` stays on the assignment for the row that meets a
/// duplicate (issue 29260).
fn plan_insert(
    insert: &tidb_ast::InsertStmt,
    select: Option<(
        &tidb_expr::schema::Schema,
        &tidb_planner_core_operator_physicalop::InsertSelectOutput,
    )>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<PlannedInsert, DriverError> {
    let (values, on_duplicate, generated) =
        super::planner_bridge::plan_insert_expressions(insert, select, catalog, current_db, ctx)
            .map_err(super::planner_error_to_driver)?;
    Ok(PlannedInsert {
        lists: values.lists,
        on_duplicate,
        actual_col_len: select.and_then(|(_, output)| output.actual_col_len),
        generated,
    })
}

#[allow(clippy::too_many_arguments)]
fn run_insert_with_physical(
    insert: &tidb_ast::InsertStmt,
    planned: Option<&PlannedInsert>,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    physical_source: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    fk_triggers: &[FkTriggerNode],
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
) -> Result<(u64, Option<u64>), DriverError> {
    run_insert_rows(
        insert,
        planned,
        catalog,
        current_db,
        ctx,
        physical_source,
        fk_triggers,
        runtime,
        None,
    )
}

/// The rows a `LOAD DATA` statement hands Go's `InsertValues`: each already
/// laid out in the statement's insert-column order, to be cast and filled by
/// `getRow` as an `INSERT ... SELECT` row is.
pub struct LoadDataRows<'a> {
    /// Go `encodeWorker.parserData2TableData`: the next row, or `None` at
    /// the end of the batch. It is pulled once per row so that a row's
    /// parse warnings precede its cast warnings, as Go encodes row by row.
    pub next_row: &'a mut dyn FnMut() -> Result<Option<Vec<Datum>>, DriverError>,
    /// Go `LoadDataController.Restrictive`: a strict SQL mode without
    /// `IGNORE`. Otherwise every value-level error is a warning
    /// (`setNonRestrictiveFlags`).
    pub restrictive: bool,
    /// How many file rows earlier batches encoded: Go's `rowCount` names a
    /// row by its place in the whole file.
    pub first_row: usize,
}

/// Go's statement counters a `LOAD DATA` message reports.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LoadDataCounts {
    /// `StmtCtx.RecordRows`.
    pub records: u64,
    /// `StmtCtx.DeletedRows`.
    pub deleted: u64,
    /// `StmtCtx.CopiedRows`.
    pub copied: u64,
    /// `StmtCtx.AffectedRows`.
    pub affected: u64,
}

/// Go `commitWorker.checkAndInsertOneBatch`: writes `LOAD DATA`'s rows
/// through the INSERT executor -- `batchCheckAndInsert` for IGNORE and
/// REPLACE, `addRecord` otherwise -- and answers Go's counters and the
/// first allocated id. `insert` names the table, the insert columns and the
/// duplicate action; it has no values of its own.
pub fn run_load_data_insert(
    insert: &tidb_ast::InsertStmt,
    rows: LoadDataRows<'_>,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(LoadDataCounts, Option<u64>), DriverError> {
    let mut counts = LoadDataCounts::default();
    // Go's LOAD DATA `InsertValues` carries no foreign-key checks.
    let (_, first_allocated) = run_insert_rows(
        insert,
        None,
        catalog,
        current_db,
        ctx,
        None,
        &[],
        None,
        Some((rows, &mut counts)),
    )?;
    Ok((counts, first_allocated))
}

#[allow(clippy::too_many_arguments)]
fn run_insert_rows(
    insert: &tidb_ast::InsertStmt,
    planned: Option<&PlannedInsert>,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    physical_source: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    fk_triggers: &[FkTriggerNode],
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
    load: Option<(LoadDataRows<'_>, &mut LoadDataCounts)>,
) -> Result<(u64, Option<u64>), DriverError> {
    let (mut load_rows, mut load_counts) = match load {
        Some((rows, counts)) => (Some(rows), Some(counts)),
        None => (None, None),
    };
    let load_restrictive = load_rows.as_ref().map(|rows| rows.restrictive);
    let load_first_row = load_rows.as_ref().map_or(0, |rows| rows.first_row);
    // Go reads value-level leniency off IGNORE for an INSERT and off
    // `Restrictive` for LOAD DATA, whose duplicate action is separate.
    let lenient = load_restrictive.map_or(insert.ignore, |restrictive| !restrictive);
    if insert.replace && !insert.on_duplicate.is_empty() {
        return Err(DriverError::unsupported("partitions are not supported yet"));
    }

    let target_layout = resolve_insert_target(insert, catalog, current_db, ctx)?;
    let (planned_lists, planned_on_duplicate, actual_col_len, planned_generated) = match planned {
        Some(planned) => (
            planned.lists.as_slice(),
            planned.on_duplicate.as_slice(),
            planned.actual_col_len,
            Some(planned.generated.as_slice()).filter(|generated| !generated.is_empty()),
        ),
        None => (&[][..], &[][..], None, None),
    };
    let eval_chunk = {
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        chunk
    };
    // The fields Go appended to the SELECT for the assignments, typed as the
    // source plan outputs them.
    let extra_types: Vec<FieldType> = match (physical_source.as_deref(), actual_col_len) {
        (Some(physical), Some(actual)) => physical
            .schema()
            .map(|schema| {
                schema
                    .columns
                    .iter()
                    .skip(actual)
                    .map(|column| {
                        column.ret_type.clone().unwrap_or_else(|| {
                            FieldType::new(tidb_datatype::FieldTypeCode::Null)
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    };

    // Once the target is valid, `INSERT ... SELECT` runs its source over the
    // pre-insert catalog and materializes those rows before the table is
    // borrowed mutably. The size of each chunk the source returned is kept:
    // Go executes the rows chunk by chunk (`insertRowsFromSelect`).
    let mut source_chunks: Vec<usize> = Vec::new();
    let source_rows: Option<Vec<Vec<Datum>>> = match &insert.source {
        // LOAD DATA's rows arrive one at a time below.
        _ if load_rows.is_some() => Some(Vec::new()),
        Some(_) => {
            let physical = physical_source.ok_or_else(|| {
                DriverError::unsupported("INSERT SELECT has no retained physical child")
            })?;
            let mut rows = Vec::new();
            let accountant = ctx
                .statement_memory()
                .write_accountant(mem_quota::label::INSERT);
            let collected = super::physical_builder::execute_dml_source_chunks(
                physical,
                catalog,
                ctx,
                runtime.is_some(),
                |row| {
                    accountant.account_row(&row).map_err(DriverError::from)?;
                    rows.push(row);
                    Ok(())
                },
                |size| source_chunks.push(size),
            )?;
            if let Some(runtime) = runtime {
                runtime.extend(collected);
            }
            Some(rows)
        }
        None => None,
    };
    // The fields Go appended for ON DUPLICATE ride past the row's own values.
    let mut source_rows = source_rows;
    let source_extras: Vec<Vec<Datum>> = match (source_rows.as_mut(), actual_col_len) {
        (Some(rows), Some(actual)) => rows
            .iter_mut()
            .map(|row| row.split_off(actual.min(row.len())))
            .collect(),
        _ => Vec::new(),
    };

    let InsertTargetLayout {
        database,
        table_name,
        column_list,
        extra_handle_offset,
        target_offsets,
        generated_targets,
        column_meta,
    } = target_layout;
    // EXCHANGE PARTITION's write-only state restricts the rows, against the
    // other table (resolved before the target is borrowed for the write).
    let exchange_check =
        exchange_partition::exchange_partition_check(catalog, &database, &table_name)?;
    let TableEntry::Kv(kv) = catalog
        .get_rows_mut_in(&database, &table_name)
        .ok_or(DriverError::unsupported("table not found in catalog"))?
    else {
        unreachable!("the INSERT target was validated before reading its source")
    };

    // Go's planner wraps a partitioned INSERT target in
    // `partitionTableWithGivenSets`: names are resolved once, then every
    // completed row is routed normally and checked against this id set at the
    // table write boundary.  This is deliberately not a read restriction.
    let insert_partition_ids = if insert.partitions.is_empty() {
        None
    } else {
        let Some(spec) = kv.partition() else {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_PARTITION_CLAUSE_ON_NONPARTITIONED
                    .generate_with_stack_by_args(&[]),
            ));
        };
        Some(
            tidb_planner_rule::ids_for_selected_partitions(spec, &insert.partitions)
                .map_err(|partition| {
                    DriverError::from(
                        tidb_table::table_errors::ERR_UNKNOWN_PARTITION
                            .generate_with_stack_by_args(&[
                                partition.into(),
                                table_name.clone().into(),
                            ]),
                    )
                })?,
        )
    };

    let auto_increment_offset = kv.auto_increment_offset();
    let auto_random_offset = kv.auto_random().map(|spec| spec.offset);
    // Until generation makes it a stored row, a row holds the visible
    // columns, where an auto-id column sits at its visible position.
    let visible_offsets = kv.visible_offsets();
    let visible_position =
        |stored: usize| visible_offsets.iter().position(|offset| *offset == stored);
    let stored_meta: Vec<ColumnDefaultMeta> = kv.columns().iter().map(kv_column_metadata).collect();
    // One marker per staged row: after casting, a supplied zero under
    // NO_AUTO_VALUE_ON_ZERO remains zero while NULL and omitted values still
    // take an id. Keeping the supplied-ness beside the row distinguishes the
    // two zero values that the later allocator otherwise cannot tell apart.
    let mut auto_rows: Vec<(usize, bool)> = Vec::new();
    let mut auto_random_rows: Vec<(usize, bool)> = Vec::new();
    let mut first_allocated: Option<u64> = None;

    // A source query supplies already-evaluated values; a VALUES list
    // supplies expressions. Both fill the same target offsets.
    if insert.source.is_none() {
        crate::notify_before_executor_first_run(ctx);
    }
    let value_rows = source_rows.as_deref().unwrap_or_default();
    let row_count = source_rows.as_ref().map_or(insert.rows.len(), Vec::len);
    // Go `ResetContextOfStmt`'s `ast.InsertStmt` arm:
    //   ErrGroupBadNull  = error when !IgnoreErr && (strict || len(stmt.Lists) == 1)
    //   ErrGroupNoDefault = error when strict
    // `stmt.Lists` is the VALUES lists, so an `INSERT ... SELECT` is never
    // "single" no matter how many rows the query returns -- which is why the
    // count below reads the AST and not `row_count`.
    //
    // This is the ONE value-level rule `IGNORE` does not reach through
    // `strict` alone: the single-row promotion holds in every SQL mode, so an
    // `IGNORE` statement has to override it separately. Captured from TiDB,
    // `INSERT IGNORE INTO t(a INT NOT NULL) VALUES (NULL)` under the default
    // strict mode warns 1048 and stores `0`. Go's rule also needs
    // `tidb_enable_strict_not_null_check`, which the statement context
    // carries.
    // LOAD DATA keeps the default error level unless it is not restrictive
    // (`setNonRestrictiveFlags`); neither the one-row rule nor
    // `tidb_enable_strict_not_null_check` reaches it.
    let bad_null_level = crate::bad_null::NullLevel::from_is_error(match load_restrictive {
        Some(restrictive) => restrictive,
        None => {
            !ctx.ignore_err()
                && (ctx.strict() || insert.rows.len() == 1)
                && ctx.strict_not_null_check()
        }
    });

    // Go builds every VALUES list in the planner (`buildValuesListOfInsert`,
    // row-major and before execution), so the expressions -- explicit
    // DEFAULTs included -- arrive already lowered over the target.
    let prepared_value_rows: &[Vec<Option<Expression>>] = planned_lists;
    let mut previous_width = target_offsets.len();
    if source_rows.is_none() {
        if prepared_value_rows.len() != insert.rows.len() {
            return Err(DriverError::unsupported(
                "an INSERT's VALUES lists were not planned",
            ));
        }
    } else if load_restrictive.is_none() {
        // INSERT SELECT still performs the same arity check, but has no AST
        // values to lower.
        for (index, values) in value_rows.iter().enumerate() {
            let width = values.len();
            let expected = if index == 0 {
                target_offsets.len()
            } else {
                previous_width
            };
            if width != expected {
                // Go's INSERT SELECT check (planbuilder.go:4474) always
                // reports row 1.
                return Err(DriverError::from(
                    tidb_plannererrors::ERR_WRONG_VALUE_COUNT_ON_ROW
                        .generate_with_stack_by_args(&[1.into()]),
                ));
            }
            previous_width = width;
        }
    }

    // Go resolves ON DUPLICATE after the VALUES lists and before execution
    // (`ResolveOnDuplicate`), so generated-column and DEFAULT errors were
    // visible at plan time even when no candidate row conflicts.
    let on_duplicate_assignments = planned_on_duplicate
        .iter()
        .map(|assignment| {
            if extra_handle_offset == Some(assignment.offset) {
                return Err(DriverError::unsupported(
                    "UPDATE of _tidb_rowid moves the row, which is not supported yet",
                ));
            }
            Ok(PreparedOnDuplicateAssignment {
                offset: assignment.offset,
                value: assignment.expr.clone(),
                lazy_error: assignment
                    .lazy_error
                    .clone()
                    .map(super::planner_error_to_driver),
            })
        })
        .collect::<Result<Vec<_>, DriverError>>()?;
    let table_types = column_list.iter().map(|(_, field_type)| field_type.clone());
    let prepared_on_duplicate = PreparedOnDuplicate {
        // The duplicate row is a stored row, where each visible column sits
        // at its stored offset.
        on_update_now: PreparedOnUpdateNow::new(
            &stored_meta,
            on_duplicate_assignments
                .iter()
                .map(|assignment| visible_offsets[assignment.offset]),
        )?,
        visible_offsets: visible_offsets.clone(),
        assignments: on_duplicate_assignments,
        extra_handle: extra_handle_offset.is_some(),
        selected_partitions: insert_partition_ids.clone(),
        extra_len: extra_types.len(),
        row_types: table_types
            .clone()
            .chain(extra_types.iter().cloned())
            .chain(table_types)
            .collect(),
    };
    // Go `ResolveOnDuplicate` drops an assignment of DEFAULT to a generated
    // column, and the executor branches on `len(e.OnDuplicate) > 0`: a
    // statement whose every assignment was dropped is a plain INSERT, which
    // reports its duplicate.
    let has_on_duplicate = !prepared_on_duplicate.assignments.is_empty();

    // Go `buildInsert`'s `checkRefColumn`: a VALUES expression that names a
    // column of the row (a subquery's own columns aside) sets
    // `NeedFillDefaultValue`, and `evalRow` then starts every row from
    // `setValueForRefColumn` and evaluates into that buffer, so `SET a = 1,
    // b = a + 1` reads the `a` just written and `VALUES (a)` the default.
    let has_ref_cols = source_rows.is_none()
        && insert
            .rows
            .iter()
            .flatten()
            .any(|value| value.flags() & tidb_ast::FLAG_HAS_REFERENCE != 0);
    let mut eval_buffer = has_ref_cols.then(|| {
        let field_types: Vec<FieldType> = column_meta
            .iter()
            .map(|meta| meta.field_type.clone())
            .collect();
        tidb_chunk::mutrow::MutRow::from_types(&field_types)
    });
    let mut new_rows: Vec<Vec<Datum>> = Vec::with_capacity(row_count);
    // Go `getColDefaultValue`/`lazilyInitColDefaultValBuf`: with more than one
    // VALUES list a non-expression default is computed (and its 1364
    // warning raised) once, then reused for every later row.
    let cache_defaults = insert.source.is_none() && insert.rows.len() > 1;
    let mut default_cache: Vec<Option<Datum>> = vec![None; column_list.len()];
    // Go `buildValuesListOfInsert` checks arity in two steps: the FIRST row
    // against the target columns, and every later row against the one before
    // it. The first check is skipped when both the column list and the first
    // value list are empty, which is what makes `INSERT t VALUES ()` -- a row
    // of nothing but defaults -- legal; chaining the later rows to their
    // predecessor is then what still rejects `VALUES (), (1)`. An empty row
    // assigns nothing, so the default, auto-increment and NOT NULL rules
    // below fill the whole row.
    let mut index = 0;
    loop {
        let load_row = match load_rows.as_mut() {
            Some(load) => match (load.next_row)()? {
                Some(row) => Some(row),
                None => break,
            },
            None if index >= row_count => break,
            None => None,
        };
        let width = match source_rows.as_ref() {
            _ if load_row.is_some() => load_row.as_ref().map_or(0, Vec::len),
            Some(_) => value_rows.get(index).map_or(0, Vec::len),
            None => insert.rows[index].len(),
        };
        let mut row = vec![Datum::Null; column_list.len()];
        let mut assigned = vec![false; column_list.len()];
        if let Some(buffer) = eval_buffer.as_mut() {
            set_value_for_ref_column(
                &column_meta,
                extra_handle_offset,
                &mut row,
                &mut assigned,
                ctx,
                eval_chunk.get_row(0),
            )?;
            buffer.set_datums(&row);
        }
        // Go `evalRow` and `getRow` cast each value as it is produced, in the
        // statement's column order; the cast value is what a later
        // expression of the row reads.
        let mut cast_done = vec![false; column_list.len()];
        for (position, &offset) in target_offsets.iter().enumerate().take(width) {
            let value = match source_rows.as_ref() {
                Some(_) if load_row.is_some() => load_row.as_ref().unwrap()[position].clone(),
                Some(_) => value_rows[index][position].clone(),
                None => match &prepared_value_rows[index][position] {
                    None => continue,
                    Some(expression) => {
                        let input = match eval_buffer.as_ref() {
                            Some(buffer) => buffer.to_row(),
                            None => eval_chunk.get_row(0),
                        };
                        expression
                            .eval(ctx, input)
                            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?
                    }
                },
            };
            // Go `evalRow` re-completes a VALUES row's cast warnings;
            // `getRow` leaves a selected row's as `CastValue` raised them.
            let cast = if load_restrictive.is_some() {
                crate::driver::write_cast::cast_load_data_value_for_column
            } else if source_rows.is_some() {
                crate::driver::write_cast::cast_selected_value_for_column
            } else {
                cast_value_for_column
            };
            let value = match cast(
                value,
                &column_meta[offset].field_type,
                column_meta[offset].name.as_str(),
                load_first_row + new_rows.len(),
                ctx,
                lenient,
            ) {
                Ok(value) => value,
                // Go `InsertValues.handleErr` hands a LOAD DATA row's cast
                // error to the duplicate-key error group
                // (`HandleErrorWithAlias(kv.ErrKeyExists, ...)`), a warning
                // unless the load is restrictive; the value stays the
                // failed conversion's zero datum.
                Err(error) if lenient && load_restrictive.is_some() => {
                    let rendered = error.to_mysql_error();
                    ctx.append_sql_warn(tidb_context::SqlWarn::from_error(
            tidb_context::WarningLevel::Warning,
            rendered.code,
            rendered.message.clone(),
            rendered.identity,
        ));
                    Datum::Null
                }
                Err(error) => return Err(error.into()),
            };
            if let Some(buffer) = eval_buffer.as_mut() {
                buffer.set_datum(offset, &value);
            }
            row[offset] = value;
            assigned[offset] = true;
            cast_done[offset] = true;
        }
        // Go fills the auto-increment column before the default and NOT NULL
        // rules run, so an omitted auto column never looks like a missing
        // value (`adjustAutoIncrementDatum` runs inside the row build).
        if let Some(offset) = auto_increment_offset.and_then(visible_position) {
            // An omitted or explicitly NULL auto column becomes the zero
            // marker, which allocation replaces; Go does this before the
            // NOT NULL check, so a NULL here is never a bad-null error.
            let supplied = assigned[offset] && row[offset] != Datum::Null;
            if !supplied {
                row[offset] = Datum::Int(0);
            }
            assigned[offset] = true;
            auto_rows.push((new_rows.len(), supplied));
        }
        // AUTO_RANDOM uses the same NULL/omitted/zero value boundary as Go's
        // `adjustAutoRandomDatum`, but draws from its own allocator below.
        if let Some(offset) = auto_random_offset.and_then(visible_position) {
            let supplied = assigned[offset] && row[offset] != Datum::Null;
            if !supplied {
                row[offset] = Datum::Int(0);
            }
            assigned[offset] = true;
            auto_random_rows.push((new_rows.len(), supplied));
        }
        // A generated column has a value source of its own, so it is neither
        // defaulted nor NULL-checked here: it counts as supplied, and the
        // expression fills it below. Without this a `NOT NULL` generated
        // column would raise ErrNoDefaultForField for a row Go accepts.
        for (offset, generated) in generated_targets.iter().enumerate() {
            if *generated {
                assigned[offset] = true;
                row[offset] = Datum::Null;
            }
        }
        // Go `fillRow` walks the columns in table order: an omitted column
        // takes its default (`fillColValue`, which can raise
        // ErrNoDefaultForField), then `HandleBadNull` checks the column,
        // before the next column is looked at -- so `INSERT IGNORE INTO t(a)
        // VALUES (NULL)` over `(a NOT NULL, b NOT NULL)` warns 1048-for-a,
        // then 1364-for-b. The statement's own values were cast in
        // `evalRow` before this, which is why their truncation warnings
        // (1406) precede every constraint warning (oracle-captured order).
        for offset in 0..row.len() {
            if offset < column_list.len() && !assigned[offset] {
                let cacheable = cache_defaults
                    && !matches!(
                        &column_meta[offset].default_value,
                        Some(tidb_table::column_default::ColumnDefault::Computed(computed))
                            if computed.is_expr()
                    );
                row[offset] = match &default_cache[offset] {
                    Some(value) if cacheable => value.clone(),
                    _ => {
                        let value =
                            column_default(&column_meta, offset, ctx, eval_chunk.get_row(0))?;
                        if cacheable {
                            default_cache[offset] = Some(value.clone());
                        }
                        value
                    }
                };
                // Go `getColDefaultValueFromNil`'s zero value for a NOT NULL
                // column without a default is final: `fillColValue` stores
                // it uncast (only a stored default goes through `CastValue`,
                // inside `GetColDefaultValue`), so a zero DATETIME raises no
                // 1292 beside its 1364.
                if column_meta[offset].default_value.is_none() && column_meta[offset].not_null {
                    cast_done[offset] = true;
                }
            }
            // Cast what the row filled in (a default or the auto-increment
            // marker); the statement's own values were cast already. The
            // row is one wider than the table when the statement wrote
            // `_tidb_rowid`: Go gives that slot a synthetic column built
            // from `NewExtraHandleColInfo()`, cast as its `TypeLonglong`.
            if !cast_done[offset] {
                let (field_type, name) = (
                    &column_meta[offset].field_type,
                    column_meta[offset].name.as_str(),
                );
                row[offset] = cast_value_for_column(
                    std::mem::replace(&mut row[offset], Datum::Null),
                    field_type,
                    name,
                    new_rows.len(),
                    ctx,
                    lenient,
                )?;
            }
            // Go `Column.HandleBadNull`: an explicit NULL in a NOT NULL
            // column is ErrColumnCantNull, a different error from omitting a
            // column that has no default; `bad_null_level` decides whether
            // it fails the statement. A generated column's value is not
            // built yet, so the NULL standing in for it is not the user's.
            if assigned[offset] && !generated_targets[offset] {
                if load_restrictive.is_some() {
                    crate::bad_null::handle_bad_null_in_load_data(
                        &mut row[offset],
                        &column_meta[offset].field_type,
                        &column_list[offset].0,
                        bad_null_level,
                        load_first_row + new_rows.len() + 1,
                        ctx,
                    )?;
                } else {
                    crate::bad_null::handle_bad_null(
                        &mut row[offset],
                        &column_meta[offset].field_type,
                        &column_list[offset].0,
                        bad_null_level,
                        ctx,
                    )?;
                }
            }
        }
        // Go `getRow`: an exchanged standalone table's row is checked before
        // its generated columns are filled.
        if let Some(check) = &exchange_check {
            check.check_standalone_row(&row, ctx)?;
        }
        {
            // The generated columns are computed from the finished row, so
            // the conflict lookup and the foreign-key check below see the
            // same values the write will store.
            // Go appends the extra handle after all writable columns. Keep it
            // outside generation, then append it after the complete hidden tail.
            let extra_handle = extra_handle_offset.map(|_| row.pop().expect("extra handle slot"));
            materialize_generated_for_write(
                kv,
                &mut row,
                ctx,
                GeneratedWrite::Insert {
                    row_index: new_rows.len(),
                    null_level: bad_null_level,
                },
                planned_generated,
            )?;
            if let Some(handle) = extra_handle {
                row.push(handle);
            }
        }
        // Go `partitionedTableAddRecord`: a row into the exchanged partition
        // passes the standalone table's constraints.
        if let Some(check) = &exchange_check {
            check.check_partitioned_row(kv, &row, ctx)?;
        }
        new_rows.push(row);
        index += 1;
    }
    // A LOAD DATA batch's size is what its source produced.
    let row_count = if load_restrictive.is_some() {
        index
    } else {
        row_count
    };
    let extra_handle_offset = extra_handle_offset.map(|_| kv.columns().len());
    // Go `InsertValues.insertRows`/`insertRowsFromSelect`, the consume that
    // sits immediately before `base.exec(ctx, rows)`:
    // `types.EstimatedMemUsage(rows[0], len(rows))` over the staged rows.
    // Accounting HERE, before a single row is written, is what makes a
    // cancelled INSERT leave the table exactly as it found it.
    ctx.statement_memory()
        .write_accountant(mem_quota::label::INSERT)
        .account_rows(&new_rows)
        .map_err(DriverError::from)?;
    {
        let kv = std::sync::Arc::make_mut(kv);
        if let Some(auto_offset) = auto_random_offset {
            for (index, supplied) in &auto_random_rows {
                if *supplied
                    && ctx.auto_increment_zero_is_explicit()
                    && matches!(
                        new_rows[*index][auto_offset],
                        Datum::Int(0) | Datum::UInt(0)
                    )
                {
                    continue;
                }
                let outcome = kv
                    .apply_auto_random(
                        &mut new_rows[*index],
                        ctx.auto_increment_step(),
                        ctx.allow_auto_random_explicit_insert(),
                        || ctx.reuse_auto_random_id(),
                        ctx.next_row_id_shard(1),
                    )
                    .map_err(|error| match error {
                        AutoRandomError::ExplicitInsertDisabled => DriverError::from(tidb_dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[("Explicit insertion on auto_random column is disabled. Try to set @@allow_auto_random_explicit_insert = true."
                                .to_owned()).into()])),
                        AutoRandomError::AutoId(AutoIdError::Exhausted) => {
                            DriverError::from(tidb_autoid::autoid_errors::ERR_AUTO_RAND_READ_FAILED.generate_with_stack_by_args(&[]))
                        }
                        AutoRandomError::AutoId(AutoIdError::OutOfRange {
                            value,
                            type_name,
                        }) => DriverError::from(tidb_datatype::types_errors::ERR_OVERFLOW.generate_with_stack(format!("constant {} overflows {}", value, type_name))),
                        AutoRandomError::AutoId(AutoIdError::Store(detail)) => {
                            DriverError::from(tidb_terror::MysqlError::unknown(detail.0))
                        }
                        AutoRandomError::NotApplicable
                        | AutoRandomError::RebaseOverflow { .. }
                        | AutoRandomError::InvalidDefinition(_) => {
                            unreachable!("row allocation does not rebase a table option")
                        }
                    })?;
                if let Some(placed) = outcome.placed() {
                    ctx.record_auto_random_id(placed);
                }
                match outcome {
                    AutoRandom::Given(given) => ctx.record_given_insert_id(given),
                    AutoRandom::Allocated(id) if first_allocated.is_none() => {
                        first_allocated = Some(id);
                    }
                    AutoRandom::Absent | AutoRandom::Reused(_) | AutoRandom::Allocated(_) => {}
                }
            }
        }
        if let Some(auto_offset) = auto_increment_offset {
            // The allocator lives on the table, so the ids are handed out here
            // rather than while the rows were being built.
            for (index, supplied) in &auto_rows {
                if *supplied
                    && ctx.auto_increment_zero_is_explicit()
                    && matches!(
                        new_rows[*index][auto_offset],
                        Datum::Int(0) | Datum::UInt(0)
                    )
                {
                    continue;
                }
                // A full domain is Go's 1467; a counter whose home could not
                // be reached is NOT that, and saying 1467 for it would report
                // a table that has run out of ids when the ids are all still
                // there.
                // Go's replay hands the row back the id its losing attempt
                // gave it (`RetryInfo`); outside a replay there is nothing to
                // hand back and the counter is drawn from as usual. The cursor
                // is read lazily so that a row carrying its OWN id does not
                // consume from it -- see `apply_auto_increment`.
                // Go fills a selected row's id as it reads the row
                // (`adjustAutoIncrementDatum`, `lazyFillAutoID` off):
                // `table.AllocAutoIncrementValue` per row. A VALUES list
                // allocates its batch at once, with no such region.
                let _alloc = (insert.source.is_some() && load_restrictive.is_none()).then(|| {
                    tidb_tracing::start_thread_region_ex("table.AllocAutoIncrementValue")
                });
                let outcome = match kv.apply_auto_increment_in(
                    &mut new_rows[*index],
                    ctx.auto_increment_step(),
                    || ctx.reuse_auto_increment_id(),
                    &tidb_autoid::AutoIdCall::statement(&ctx.statement_memory()),
                ) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        if let Err(killed) = ctx.statement_memory().check().map_err(ExecError::from) {
                            return Err(DriverError::Exec(killed));
                        }
                        match error {
                            AutoIdError::Exhausted => {
                                return Err(DriverError::Mysql(kv.auto_id_exhausted_error()))
                            }
                            // An id that does not fit the COLUMN is not a full
                            // domain: Go `setDatumAutoIDAndCast` casts the
                            // allocated id, and the cast's 1690 names the value
                            // and type. Under IGNORE or a non-strict mode the
                            // cast warns and clamps instead; the clamped id may
                            // duplicate an existing one, which only an ON
                            // DUPLICATE KEY UPDATE may go on with (issue
                            // 38950). Otherwise it is 1467, which IGNORE's
                            // error context makes a warning that ends the
                            // statement (insert.go `insertRows`'s caller).
                            AutoIdError::OutOfRange { value, type_name } => {
                                if ctx.strict() && !insert.ignore {
                                    return Err(DriverError::from(
                                        tidb_datatype::types_errors::ERR_OVERFLOW
                                            .generate_with_stack(format!(
                                                "constant {} overflows {}",
                                                value, type_name
                                            )),
                                    ));
                                }
                                ctx.append_sql_warn(tidb_context::SqlWarn::raised(
                                    tidb_context::WarningLevel::Warning,
                                    tidb_datatype::types_errors::ERR_OVERFLOW.generate_with_stack(
                                        format!("constant {value} overflows {type_name}"),
                                    ),
                                ));
                                if !has_on_duplicate {
                                    if !insert.ignore {
                                        return Err(DriverError::from(
                                            tidb_autoid::autoid_errors::ERR_AUTOINC_READ_FAILED
                                                .generate_with_stack_by_args(&[]),
                                        ));
                                    }
                                    ctx.append_sql_warn(tidb_context::SqlWarn::raised(
                                        tidb_context::WarningLevel::Warning,
                                        tidb_autoid::autoid_errors::ERR_AUTOINC_READ_FAILED
                                            .generate_with_stack_by_args(&[]),
                                    ));
                                    return Ok((0, None));
                                }
                                kv.clamp_auto_increment(&mut new_rows[*index]);
                                AutoIncrement::Allocated(value.parse::<i64>().unwrap_or_else(
                                    |_| value.parse::<u64>().map_or(0, |id| id as i64),
                                ))
                            }
                            AutoIdError::Store(detail) => {
                                return Err(DriverError::from(
                                    tidb_terror::MysqlError::unknown(detail.0),
                                ));
                            }
                        }
                    }
                };
                // Recorded whether it was drawn, handed back, or supplied by
                // the row, so the NEXT attempt replays this attempt's
                // assignment exactly. Go records in all three arms
                // (`insert_common.go:902`, `:946`), and an explicit id left
                // out of the list is what desynchronised the cursor.
                if let Some(placed) = outcome.placed() {
                    ctx.record_auto_increment_id(placed);
                }
                match outcome {
                    AutoIncrement::Given(given) => {
                        // Go records the explicit value as `StmtCtx.InsertID`,
                        // and the LAST row's wins; the OK packet falls back to
                        // it when the statement published nothing.
                        ctx.record_given_insert_id(given);
                    }
                    // Go keeps the FIRST id the statement ALLOCATED. A reused
                    // one is deliberately not it: the replay's consume loop
                    // returns before `lastInsertID` is ever assigned, so the
                    // value a client read after the losing attempt is the one
                    // that survives.
                    AutoIncrement::Allocated(id) if first_allocated.is_none() => {
                        first_allocated = Some(id as u64);
                    }
                    AutoIncrement::Absent
                    | AutoIncrement::Reused(_)
                    | AutoIncrement::Allocated(_) => {}
                }
            }
        }
    }
    // The table is re-borrowed per use rather than held across the loop,
    // because REPLACE's row removal runs the PARENT-side referential
    // operators, and those write the DEPENDENT tables the statement never
    // named -- reachable only from the catalog, not from this table.
    fn target<'a>(
        catalog: &'a mut Catalog,
        database: &str,
        table_name: &str,
    ) -> &'a mut tidb_table::kv_table::KvTable {
        match catalog.get_rows_mut_in(database, table_name) {
            Some(TableEntry::Kv(kv)) => std::sync::Arc::make_mut(kv),
            _ => unreachable!("INSERT through a view is refused above"),
        }
    }
    // Go `optimizeDupKeyCheckForNormalInsert` (`pkg/executor/insert.go`):
    // only a NORMAL insert -- no REPLACE, no ON DUPLICATE KEY, no IGNORE --
    // may defer its duplicate key check to the pessimistic lock / prewrite
    // constraint check. The moment a statement must RESOLVE a conflict rather
    // than report one, every prior read stays eager, exactly as Go keeps the
    // in-place mode for those statements.
    let lazy_dup_check = (!ctx.constraint_check_in_place() || ctx.pessimistic_lazy_dup_check())
        && !insert.replace
        && !has_on_duplicate
        && !insert.ignore;
    // Go resolves a conflict per row, before the row is written, only when
    // the statement needs the conflicting handle: REPLACE deletes every row
    // it collides with, ON DUPLICATE KEY UPDATE applies its assignments to
    // the first one, and IGNORE skips the row with the duplicate reported as
    // a warning. A normal INSERT does not consume that handle. Its
    // authoritative `addRecord`/index writes below perform the same one
    // existence check and retain the duplicate error, so probing here would
    // issue a redundant remote read for every row in a batch.
    let resolves_conflicts = insert.replace || has_on_duplicate || insert.ignore;
    // A normal INSERT into a clustered table with no secondary indexes can
    // prove all record keys absent in one BatchGet. Keep the proof narrow: a
    // heap handle allocation, partition routing, or a unique secondary key
    // has additional duplicate semantics that must stay on the row path.
    let skip_primary_duplicate_check =
        if !resolves_conflicts && !lazy_dup_check && extra_handle_offset.is_none() {
            match target(catalog, &database, &table_name)
                .all_clustered_insert_keys_absent(&new_rows, ctx)
                .map_err(kv_write_error)?
            {
                Some(absent) => absent,
                None => false,
            }
        } else {
            false
        };
    let mut inserted = 0;
    // Go `StmtCtx.CopiedRows`/`UpdatedRows`/`TouchedRows`, which the
    // `Records: ...` message reads.
    let mut copied: u64 = 0;
    let mut duplicates_updated: u64 = 0;
    let mut duplicates_touched: u64 = 0;
    let mut load_deleted: u64 = 0;
    // Go's LOAD DATA sets `ErrGroupNoMatchedPartition` to a warning, which
    // its IGNORE and REPLACE batch check (`getKeysNeedCheck`) honours.
    let partition_warns = insert.ignore || (load_restrictive.is_some() && insert.replace);
    let mut updates = UpdateRecords::new(fk_triggers);
    let mut removals = DeleteRecords::new(fk_triggers);
    let mut inserted_rows = Vec::new();
    // Go `InsertExec.exec` / `ReplaceExec.exec` run once per batch: the whole
    // VALUES list, or each chunk an INSERT ... SELECT's source returned.
    let batch_ends: Vec<usize> = {
        let selected: usize = source_chunks.iter().sum();
        if insert.source.is_some() && !source_chunks.is_empty() && selected == new_rows.len() {
            source_chunks
                .iter()
                .scan(0, |end, size| {
                    *end += size;
                    Some(*end)
                })
                .collect()
        } else {
            vec![new_rows.len()]
        }
    };
    let mut batches = batch_ends.iter().copied().peekable();
    let mut batch_start = 0;
    let mut batch_check: Option<tidb_tracing::ThreadRegion> = None;
    // Generated columns belong to the writable row; the extra handle is
    // candidate identity and must not affect row equality or FK callbacks.
    fn split_candidate<'r>(
        candidate: &'r [Datum],
        extra_handle_offset: Option<usize>,
        ctx: &crate::StmtContext,
    ) -> (&'r [Datum], Option<i64>) {
        match extra_handle_offset {
            Some(offset) => (
                &candidate[..offset],
                candidate
                    .get(offset)
                    .and_then(|value| written_row_id(value, ctx)),
            ),
            None => (candidate, None),
        }
    }
    for (position, candidate) in new_rows.iter().enumerate() {
        if load_restrictive.is_none() && position == batch_start {
            if let Some(end) = batches.next() {
                drop(batch_check.take());
                let rows = new_rows[batch_start..end]
                    .iter()
                    .map(|candidate| split_candidate(candidate, extra_handle_offset, ctx))
                    .collect::<Vec<_>>();
                batch_start = end;
                // Go `InsertExec.exec`'s event; REPLACE logs none.
                if !insert.replace {
                    tidb_tracing::thread_event(|| {
                        format!(
                            "insert {} rows into table `{}`",
                            rows.len(),
                            table_name.to_lowercase()
                        )
                    });
                }
                if resolves_conflicts && !lazy_dup_check {
                    let table = target(catalog, &database, &table_name);
                    if has_on_duplicate || insert.replace {
                        // Go `prefetchDataCache` (none for a temporary table).
                        if !table.is_temporary() {
                            let _prefetch =
                                tidb_tracing::start_thread_region("prefetchDataCache");
                            let values = table
                                .prefetch_unique_indices(&rows, ctx)
                                .map_err(kv_write_error)?;
                            table
                                .prefetch_conflicted_old_rows(&rows, &values, ctx)
                                .map_err(kv_write_error)?;
                        }
                    } else {
                        // Go `batchCheckAndInsert`, which IGNORE runs over
                        // the batch's rows.
                        batch_check = Some(tidb_tracing::start_thread_region(
                            "InsertValues.batchCheckAndInsert",
                        ));
                        if !table.is_temporary() {
                            table
                                .prefetch_unique_indices(&rows, ctx)
                                .map_err(kv_write_error)?;
                        }
                    }
                }
            }
        }
        let (row, written_row_id): (&[Datum], Option<i64>) =
            split_candidate(candidate, extra_handle_offset, ctx);
        // Go's partition-qualified INSERT target is a table wrapper, so the
        // completed candidate is routed through the selected partition set
        // before duplicate-key resolution.  This prevents a row for p0 from
        // finding and updating a conflicting row when the statement names p1.
        if let Some(partitions) = &insert_partition_ids {
            if let Err(error) = target(catalog, &database, &table_name)
                .validate_insert_partitions(row, partitions, ctx)
            {
                handle_partition_write_error(kv_write_error(error), partition_warns, ctx)?;
                continue;
            }
        }
        // A lazy normal insert reads nothing here -- Go's addRecord never
        // resolves conflicts it only reports, and the deferred check needs no
        // old-row handles.
        let conflicts = if lazy_dup_check || !resolves_conflicts {
            Vec::new()
        } else {
            match target(catalog, &database, &table_name).row_conflicts_with_row_id(
                row,
                written_row_id,
                ctx,
            ) {
                Ok(conflicts) => conflicts,
                Err(error) => {
                    handle_partition_write_error(kv_write_error(error), partition_warns, ctx)?;
                    continue;
                }
            }
        };
        if !conflicts.is_empty() {
            if insert.replace {
                // Go `InsertValues.removeRow` (`insert_common.go`): a
                // conflicting row IDENTICAL to the one being written is left
                // in place -- not deleted and not rewritten -- and counts
                // ONE, not the two a delete-plus-insert would. This is also
                // the site `tidb_lock_unchanged_keys` governs, which is why
                // Go's `TestInsertLockUnchangedKeys` drives it with
                // `replace into t values (1)` over the same row.
                let mut unchanged = false;
                for conflict in &conflicts {
                    let handle = &conflict.handle;
                    let existing = target(catalog, &database, &table_name)
                        .get_row_by_handle(handle, &ctx.session_zone())
                        .map_err(|e| kv_read_error("row read failed", e))?;
                    if existing.as_deref() == Some(row) {
                        // Go `batchCheckAndInsert`: an identical row behind the
                        // record key counts as copied, one behind a unique key
                        // (`removeRow(inReplace=true)`) as affected.
                        if load_restrictive.is_some() && conflict.on_record_key {
                            copied += 1;
                        } else {
                            inserted += 1;
                        }
                        unchanged = true;
                        break;
                    }
                    removals.write(
                        catalog,
                        &database,
                        &table_name,
                        &handle,
                        existing.as_deref().expect("conflicting row exists"),
                        insert.ignore,
                        ctx,
                    )?;
                    // Go's LOAD DATA removes a record-key duplicate with
                    // `removeRow(inReplace=false)`, a deleted row.
                    if load_restrictive.is_some() && conflict.on_record_key {
                        load_deleted += 1;
                    } else {
                        inserted += 1;
                    }
                }
                if unchanged {
                    continue;
                }
            } else if has_on_duplicate {
                // `VALUES(col)` and the assignments read the statement's
                // visible columns, then the extra handle it wrote.
                let sql_candidate = visible_offsets
                    .iter()
                    .map(|offset| &row[*offset])
                    .chain(extra_handle_offset.map(|offset| &candidate[offset]))
                    .cloned()
                    .collect::<Vec<_>>();
                let candidate_values = sql_candidate.as_slice();
                let applied = apply_on_duplicate(
                    catalog,
                    &database,
                    &conflicts[0].handle,
                    candidate_values,
                    source_extras.get(position).map_or(&[][..], Vec::as_slice),
                    &prepared_on_duplicate,
                    &column_list,
                    position,
                    &table_name,
                    insert.ignore,
                    bad_null_level,
                    &mut updates,
                    ctx,
                )?;
                inserted += applied.affected;
                duplicates_updated += u64::from(applied.changed);
                duplicates_touched += u64::from(applied.touched);
                // Go `doDupRowUpdate` (issue #55965): the OK packet's insert
                // id is the updated row's AUTO_INCREMENT value when the
                // statement has affected rows, and 0 otherwise.
                if let Some(id) = applied.auto_increment {
                    ctx.record_given_insert_id(if inserted > 0 { id } else { 0 });
                }
                continue;
            } else if insert.ignore {
                let conflict = conflicts.into_iter().next().expect("a conflict was found");
                let warning = kv_write_error(conflict.error).to_mysql_error();
                ctx.append_sql_warn(tidb_context::SqlWarn::from_error(
            tidb_context::WarningLevel::Warning,
            warning.code,
            warning.message.clone(),
            warning.identity,
        ));
                continue;
            }
        }
        // IGNORE checks this candidate before writing; ordinary INSERT
        // collects only accepted rows and checks the final statement buffer.
        // ODKU candidates redirected to UPDATE have already continued above.
        if insert.ignore {
            if let Err(error) = crate::foreign_key::require_child_rows(
                catalog,
                fk_triggers,
                &database,
                &table_name,
                &[row.to_vec()],
                ctx,
            ) {
                if error.equal(&tidb_plannererrors::ERR_NO_REFERENCED_ROW2) {
                    let warning = error.to_mysql_error();
                    ctx.append_sql_warn(tidb_context::SqlWarn::from_error(
            tidb_context::WarningLevel::Warning,
            warning.code,
            warning.message.clone(),
            warning.identity,
        ));
                    continue;
                }
                return Err(error);
            }
        }
        // Go publishes the statement's first allocated id the moment a row is
        // ACCEPTED for insertion (`addRecord` -> `SetLastInsertID`), which is
        // why a hard duplicate publishes -- its deferred unique-key check
        // fails the statement only afterwards -- while an IGNORE-skipped row
        // and a row redirected into ON DUPLICATE KEY UPDATE never reach here
        // and so publish nothing.
        if let Some(allocated) = first_allocated {
            ctx.publish_last_insert_id(allocated);
        }
        // Go `AllocHandleIDs` asks the SESSION's row-id shard generator for
        // the shard covering the next `n` ids, and only when the table
        // declares shard bits -- asking otherwise would advance a generator
        // whose run length is observable (`tidb_shard_allocate_step`).
        let shard = if target(catalog, &database, &table_name).shard_row_id_bits() > 0 {
            ctx.next_row_id_shard(1) as i64
        } else {
            0
        };
        let insert_result = if skip_primary_duplicate_check {
            target(catalog, &database, &table_name)
                .insert_row_with_row_id_checked_without_primary_duplicate_check(
                    row,
                    written_row_id,
                    shard,
                    ctx,
                    lazy_dup_check,
                )
        } else {
            target(catalog, &database, &table_name).insert_row_with_row_id_checked(
                row,
                written_row_id,
                shard,
                ctx,
                lazy_dup_check,
            )
        };
        match insert_result {
            Ok(_) => {
                inserted += 1;
                copied += 1;
                if !insert.ignore
                    && crate::foreign_key::has_triggers(fk_triggers, &database, &table_name)
                {
                    inserted_rows.push(position);
                }
            }
            Err(error) => {
                let rendered = kv_write_error(error);
                // Go's `batchCheckAndInsert` counts the row copied before
                // `addRecord` and, in LOAD DATA, skips a violated CHECK
                // without a warning.
                if load_restrictive.is_some()
                    && insert.ignore
                    && rendered.equal(&tidb_table::table_errors::ERR_CHECK_CONSTRAINT_VIOLATED)
                {
                    copied += 1;
                    continue;
                }
                // Under IGNORE a skipped row counts in NEITHER the stored
                // rows nor the affected count -- Go's per-row skip writes
                // nothing and rewinds nothing; earlier conforming rows of
                // the same statement survive.
                handle_partition_write_error(rendered, insert.ignore, ctx)?;
            }
        }
    }
    if !inserted_rows.is_empty() {
        let written: Vec<_> = inserted_rows
            .into_iter()
            .map(|index| {
                let row = &new_rows[index];
                row[..extra_handle_offset.unwrap_or(row.len())].to_vec()
            })
            .collect();
        crate::foreign_key::require_child_rows(
            catalog,
            fk_triggers,
            &database,
            &table_name,
            &written,
            ctx,
        )?;
    }
    updates.finish(catalog, ctx)?;
    removals.finish(catalog, ctx)?;
    // Go `InsertExec.setMessage` / `ReplaceExec.setMessage`: an
    // `INSERT ... SELECT` or a multi-row VALUES list reports
    // `Records: N  Duplicates: D  Warnings: W`.
    let records = row_count as u64;
    if let Some(counts) = load_counts.as_deref_mut() {
        *counts = LoadDataCounts {
            records,
            deleted: load_deleted,
            copied,
            affected: inserted,
        };
        return Ok((inserted, first_allocated));
    }
    if insert.source.is_some() || records > 1 {
        let duplicates = if insert.replace {
            inserted.saturating_sub(records)
        } else if insert.ignore {
            records.saturating_sub(copied)
        } else if ctx.client_found_rows() {
            duplicates_touched
        } else {
            duplicates_updated
        };
        ctx.set_message(format!(
            "Records: {records}  Duplicates: {duplicates}  Warnings: {}",
            ctx.warning_count()
        ));
    }
    Ok((inserted, first_allocated))
}

/// Go `adjustImplicitRowID`'s reading of a written `_tidb_rowid`: the handle
/// the statement asked for, or `None` for "allocate one".
///
/// Go decides it in two steps. A non-zero value is always the handle. A NULL
/// or a ZERO is normally "allocate", EXCEPT that the zero is kept when
/// `NO_AUTO_VALUE_ON_ZERO` is in the `sql_mode` -- Go's condition is
/// `d.IsNull() || SQLMode&ModeNoAutoValueOnZero == 0`, so with the mode set a
/// written zero falls past the allocation branch and is stored as handle 0.
/// The corpus asserts exactly that pair: the same
/// `insert t (a, _tidb_rowid) values (n, 0)` answers masked row id 0 with the
/// mode and 8 without it.
fn written_row_id(value: &Datum, ctx: &crate::StmtContext) -> Option<i64> {
    let written = match value {
        Datum::Int(value) => *value,
        Datum::UInt(value) => *value as i64,
        _ => return None,
    };
    (written != 0 || ctx.auto_increment_zero_is_explicit()).then_some(written)
}

/// The one rendering of a byte-backed write failure, so every write path
/// reports the same statement error for the same cause.
///
/// The generation arm is the reason this is shared rather than repeated: a
/// generated column's expression fails with an evaluation error that already
/// carries its own MySQL code (1365 for a zero divisor under
/// `ERROR_FOR_DIVISION_BY_ZERO`), and rendering it as a generic parse failure
/// would replace the code an application branches on.
pub fn kv_write_error(error: tidb_table::kv_table::KvTableError) -> DriverError {
    match error {
        tidb_table::kv_table::KvTableError::Storage(error) => error.into(),
        tidb_table::kv_table::KvTableError::Mysql(error) => {
            DriverError::Exec(crate::ExecError::Mysql(error))
        }
        tidb_table::kv_table::KvTableError::Generation {
            eval: Some(eval), ..
        } => DriverError::Exec(crate::ExecError::Eval(eval)),
        tidb_table::kv_table::KvTableError::PartitionEval(eval) => {
            DriverError::Exec(crate::ExecError::Eval(eval))
        }
        tidb_table::kv_table::KvTableError::CheckConstraint {
            eval: Some(eval), ..
        } => DriverError::Exec(crate::ExecError::Eval(eval)),
        other => DriverError::from(tidb_util::misc::syntax_error(format!(
            "row encode failed: {other:?}"
        ))),
    }
}

/// Applies Go's `ErrCtx.HandleError` rule for partition-routing failures on
/// `INSERT/UPDATE IGNORE`: report one warning and skip the row. Other write
/// failures retain their normal error identity.
fn handle_partition_write_error(
    error: DriverError,
    ignore: bool,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    match error {
        // Go's `batchCheckAndInsert` (the IGNORE path) downgrades a violated
        // CHECK to a warning and skips the row, exactly like a duplicate key
        // (`insert_common.go:1364-1370`); a plain insert still fails the
        // statement from `addRecord`.
        error
            if ignore
                && (error.equal(&tidb_table::table_errors::ERR_NO_PARTITION_FOR_GIVEN_VALUE)
                    || error.equal(
                        &tidb_table::table_errors::ERR_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET,
                    )
                    || error.equal(&tidb_table::table_errors::ERR_CHECK_CONSTRAINT_VIOLATED)) =>
        {
            let warning = error.to_mysql_error();
            ctx.append_sql_warn(tidb_context::SqlWarn::from_error(
                tidb_context::WarningLevel::Warning,
                warning.code,
                warning.message.clone(),
                warning.identity,
            ));
            Ok(())
        }
        error => Err(error),
    }
}

/// Renders a table read failure while preserving the storage layer's
/// retryable-region signal as a transaction error instead of a parse error.
pub(crate) fn kv_read_error(operation: &str, error: tidb_table::kv_table::KvTableError) -> DriverError {
    match error {
        tidb_table::kv_table::KvTableError::Storage(error) => error.into(),
        // A row that could not be READ is a runtime storage/decode failure;
        // Go surfaces it through its generic 1105 path -- never a 1064, which
        // would tell the client its SQL TEXT was at fault.
        other => DriverError::Exec(ExecError::Internal(
            format!("{operation}: {other:?}").into(),
        )),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedOnDuplicateAssignment {
    offset: usize,
    /// Built over Go's `Schema4OnDuplicate`.
    value: Expression,
    /// Go `Assignment.LazyErr`: a scalar subquery's 1242 raised while the
    /// assignment was built, which only a row meeting a duplicate reports.
    lazy_error: Option<DriverError>,
}

struct PreparedOnDuplicate {
    extra_handle: bool,
    /// The stored offsets of the visible columns the assignments name.
    visible_offsets: Vec<usize>,
    assignments: Vec<PreparedOnDuplicateAssignment>,
    on_update_now: PreparedOnUpdateNow,
    selected_partitions: Option<Vec<i64>>,
    /// How many fields Go appended to the SELECT for the assignments.
    extra_len: usize,
    /// The types of Go's `row4Update`: stored row, appended fields, would-be
    /// row.
    row_types: Vec<FieldType>,
}

/// Go `ON DUPLICATE KEY UPDATE`: applies the assignments to the row already
/// stored, and reports what the statement counts as affected.
///
/// Captured from TiDB: the assignments read the EXISTING row (`c = c + 1` on
/// a stored 10 gives 11, not the rejected value plus one), `VALUES(col)`
/// reads the row that would have been inserted, an update that changes
/// nothing counts 0 (or 1 under `CLIENT_FOUND_ROWS`), and one that changes
/// something counts 2.
fn apply_on_duplicate(
    catalog: &mut Catalog,
    database: &str,
    handle: &tidb_table::kv_table::TableHandle,
    candidate: &[Datum],
    extras: &[Datum],
    prepared: &PreparedOnDuplicate,
    column_list: &[(String, FieldType)],
    row_index: usize,
    target_table_name: &str,
    ignore: bool,
    null_level: crate::bad_null::NullLevel,
    updates: &mut UpdateRecords<'_>,
    ctx: &crate::StmtContext,
) -> Result<OnDuplicateApplied, DriverError> {
    let Some(TableEntry::Kv(table)) = catalog.get_rows_mut_in(database, target_table_name) else {
        unreachable!("ODKU targets a byte-backed table")
    };
    let auto_increment_offset = table.auto_increment_column_offset();
    let Some(existing) = std::sync::Arc::make_mut(table)
        .get_row_by_handle(handle, &ctx.session_zone())
        .map_err(|e| kv_read_error("row read failed", e))?
    else {
        return Ok(OnDuplicateApplied::default());
    };
    let field_types: Vec<FieldType> = column_list.iter().map(|(_, ft)| ft.clone()).collect();
    let extra_handle = prepared.extra_handle;
    let sql_row = |row: &[Datum]| {
        let mut values: Vec<Datum> =
            prepared.visible_offsets.iter().map(|offset| row[*offset].clone()).collect();
        if extra_handle {
            values.push(Datum::Int(
                extra_handle_value(handle).expect("heap row has an integer handle"),
            ));
        }
        values
    };
    // Go `doDupRowUpdate`: `VALUES(col)` reads `CurrInsertValues`, the
    // would-be row, and each assignment evaluates over `row4Update` -- the
    // stored row as the earlier assignments left it, the appended fields,
    // then the would-be row.
    let new_row = &candidate[..column_list.len().min(candidate.len())];
    ctx.set_current_insert_values(new_row.to_vec());
    let mut updated = existing.clone();
    let assigned = (|| {
        for assignment in &prepared.assignments {
            if let Some(error) = &assignment.lazy_error {
                return Err(error.clone());
            }
            let mut row4_update = sql_row(&updated);
            row4_update.extend(
                extras
                    .iter()
                    .cloned()
                    .chain(std::iter::repeat(Datum::Null))
                    .take(prepared.extra_len),
            );
            row4_update.extend_from_slice(new_row);
            let chunk = row_chunk(&row4_update, &prepared.row_types)?;
            let value = assignment
                .value
                .eval(ctx, chunk.get_row(0))
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
            updated[prepared.visible_offsets[assignment.offset]] = cast_value_for_assignment(
                value,
                &field_types[assignment.offset],
                &column_list[assignment.offset].0,
                row_index,
                ctx,
            )?;
        }
        Ok::<(), DriverError>(())
    })();
    ctx.clear_current_insert_values();
    assigned?;
    let updated_chunk = row_chunk(&sql_row(&updated), &field_types)?;
    prepared
        .on_update_now
        .apply(&existing, &mut updated, ctx, updated_chunk.get_row(0))?;
    let outcome = updates.write(
        catalog,
        database,
        target_table_name,
        handle,
        &existing,
        &mut updated,
        prepared.selected_partitions.as_deref(),
        ignore,
        GeneratedWrite::OnDuplicate {
            row_index,
            null_level,
        },
        ctx,
    )?;
    Ok(OnDuplicateApplied {
        affected: if outcome.changed() {
            2
        } else {
            u64::from(outcome.unchanged() && ctx.client_found_rows())
        },
        changed: outcome.changed(),
        touched: outcome.touched(),
        // Go `doDupRowUpdate` returns before its insert-id step when
        // `updateRecord` ignored the row.
        auto_increment: if outcome.touched() {
            auto_increment_offset
                .and_then(|offset| updated.get(offset))
                .map(datum_bits_as_u64)
        } else {
            None
        },
    })
}

/// What one ON DUPLICATE KEY UPDATE row did.
#[derive(Default)]
struct OnDuplicateApplied {
    /// Its contribution to the affected-row count.
    affected: u64,
    /// Go `UpdatedRows`: the update changed the row.
    changed: bool,
    /// Go `TouchedRows`: the row was found (changed or not).
    touched: bool,
    /// The updated row's AUTO_INCREMENT value, when the table has one.
    auto_increment: Option<u64>,
}

/// Go `Datum.GetUint64` on an integer datum: its bits.
fn datum_bits_as_u64(value: &Datum) -> u64 {
    match value {
        Datum::Int(value) => *value as u64,
        Datum::UInt(value) => *value,
        _ => 0,
    }
}

/// Runs a single-table `UPDATE`, returning MySQL's affected-row count.
///
/// Go `executor.UpdateExec` + `updateRecord`: each row the `WHERE` selects is
/// re-evaluated with the `SET` assignments applied, and a row is written back
/// only when a column actually changed. The affected-row count is the number
/// of CHANGED rows, not the number matched -- an unchanged row is "touched"
/// instead, and a client that negotiated `CLIENT_FOUND_ROWS` sees those
/// successfully matched rows counted too.
///
/// Assignments are evaluated against the row's ORIGINAL values. Go constructs
/// the complete replacement row before it writes any assignment, so one `SET`
/// item cannot observe a previous item's new value.
///
/// Multi-table `UPDATE` lives in `multi_dml`, which reads a joined row
/// source carrying each target's row identity.
///
/// A changed primary-key handle moves the row and rewrites its secondary-index
/// entries. Single-table `ORDER BY`/`LIMIT` is supported (see
/// the retained physical read child).
pub fn run_update_on(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_update_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_update_on`] resolving unqualified names in `current_db`.
pub fn run_update_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    let stmt = ctx.parse(sql)?;
    let update = match &stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Update(update) => update,
            _ => return Err(DriverError::unsupported("only UPDATE is supported here")),
        },
        _ => return Err(DriverError::unsupported("only UPDATE is supported here")),
    };
    run_update_stmt(update, catalog, current_db, ctx)
}

/// [`run_update_in`]'s body, taking the already-parsed AST directly so
/// `explain::explain_analyze_update_stmt` (which already holds a parsed
/// `UpdateStmt` from the `EXPLAIN ANALYZE` wrapper) can execute the SAME
/// write path real `EXPLAIN ANALYZE UPDATE` runs, rather than re-deriving
/// it or re-parsing the statement text (which `explain`'s callers do not
/// keep around).
pub fn run_update_stmt(
    update: &tidb_ast::UpdateStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_update_stmt_with_physical(update, catalog, current_db, ctx, None)
}

/// The ordinary UPDATE executor, optionally consuming the cached
/// `Update.SelectPlan` selected by the shared physical planner.
pub fn run_update_stmt_with_physical(
    update: &tidb_ast::UpdateStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    physical_plan: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
) -> Result<u64, DriverError> {
    run_update_stmt_with_physical_and_stats(update, catalog, current_db, ctx, physical_plan, None)
}

pub(crate) fn run_update_stmt_with_physical_and_stats(
    update: &tidb_ast::UpdateStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    physical_plan: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
) -> Result<u64, DriverError> {
    // Go `buildUpdateLists` resolves a CTE target's assignment columns and
    // then refuses it (1288); `update_target_columns` takes the same steps.
    if let tidb_ast::UpdateKind::Single(table) = &update.kind {
        if super::planner_bridge::is_cte(update.with.as_ref(), table) {
            super::planner_bridge::update_target_columns(update, catalog, current_db, ctx)?;
            return Err(DriverError::from(
                tidb_plannererrors::ERR_NON_UPDATABLE_TABLE
                    .generate_with_stack_by_args(&[(table.name.join(".")).into(), "UPDATE".into()]),
            ));
        }
    }
    let source = update_source_query(update);
    // Go `planner.Optimize`.
    let optimize = super::start_optimize_region();
    let mut fresh = physical_plan
        .is_none()
        .then(|| match &update.kind {
            tidb_ast::UpdateKind::Single(_) => physical_dml_plan(
                "Update",
                source.as_ref(),
                Some(update),
                catalog,
                current_db,
                ctx,
                &fk_spec_for_update(update, current_db)?,
            ),
            tidb_ast::UpdateKind::Multi { .. } => super::multi_dml::multi_dml_physical_plan(
                super::multi_dml::MultiDmlRef::Update(update),
                catalog,
                current_db,
                ctx,
            ),
        })
        .transpose()?;
    optimize.end();
    let mut physical_plan = physical_plan.or(fresh.as_mut());
    if let Some(plan) = physical_plan.as_deref_mut() {
        mark_dml_point_source_lock(plan, ctx.pessimistic_transaction());
        super::physical_builder::prepare_execution_plan(plan, catalog, ctx)?;
        crate::explain::publish_physical_process_info(ctx, plan, catalog);
    }
    // Go `runStmt` -> `handleNoDelayExecutor` -> the DML executor's `Next`.
    let _execution = super::begin_dml_execution("*executor.UpdateExec.Next");
    let (physical_source, update_expressions, fast_update_list, fk_triggers, multi_layout) = match physical_plan {
        Some(plan) => {
            let tidb_planner_core_operator_physicalop::PhysicalPlan::Dml(root) = plan else {
                return Err(DriverError::unsupported(
                    "UPDATE execution received a non-DML physical root",
                ));
            };
            if !root.go_operator.eq_ignore_ascii_case("Update") {
                return Err(DriverError::unsupported(format!(
                    "Update execution received a {} physical root",
                    root.go_operator
                )));
            }
            (
                root.select_plan.as_deref_mut(),
                Some(root.update_expressions.as_slice()),
                root.fast_update_list,
                root.fk_triggers.as_slice(),
                (root.update_tables.as_slice(), root.update_assigns.as_slice()),
            )
        }
        None => (None, None, false, &[][..], (&[][..], &[][..])),
    };
    run_update_with_physical(
        update,
        catalog,
        current_db,
        ctx,
        physical_source,
        update_expressions,
        fast_update_list,
        fk_triggers,
        multi_layout,
        runtime,
    )
}

///
/// The SELECT carries the statement's hints, as Go's `tryUpdatePointPlan`
/// builds its `SelectStmt{TableHints: updateStmt.TableHints}` and
/// `buildUpdate` pushes them for the read.
pub(crate) fn update_source_query(update: &tidb_ast::UpdateStmt) -> Option<tidb_ast::QueryStmt> {
    match &update.kind {
        tidb_ast::UpdateKind::Single(table_ref) => Some(super::multi_dml::multi_dml_select(
            update.with.as_ref(),
            &update.hints,
            &tidb_planner::plan_builder::dml_source::single_table_join(table_ref),
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
pub(crate) fn update_allows_fast_plan(update: &tidb_ast::UpdateStmt) -> bool {
    !update
        .assignments
        .iter()
        .any(|assignment| super::subquery::expr_has_subquery(&assignment.value))
}

/// Go `tryUpdatePointPlan`'s `buildOrderedList`: the assignments of a point
/// UPDATE as expressions over the point plan's row.
fn fast_update_ordered_list(
    update: &tidb_ast::UpdateStmt,
    assignments: &[tidb_planner::plan_builder::single_update::SingleUpdateAssignment],
    fast: &tidb_planner_core_operator_physicalop::PhysicalPlan,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
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
        .zip(assignments.iter().map(|assignment| assignment.value.clone()))
        .collect();
    super::planner_bridge::point_update_ordered_list(
        table_ref,
        schema,
        &targets,
        catalog,
        current_db,
        ctx,
        plan_ids,
        column_ids,
    )
    .map_err(super::planner_error_to_driver)
}

/// Go `buildUpdateLists` for a single-table UPDATE: every SET item resolved to
/// its column, a bare DEFAULT named, a generated column's own DEFAULT left to
/// its expression. A view or a sequence is no UPDATE target.
fn update_assignments_for_plan(
    update: &tidb_ast::UpdateStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<tidb_planner::plan_builder::single_update::SingleUpdateAssignment>, DriverError> {
    let tidb_ast::UpdateKind::Single(table_ref) = &update.kind else {
        return Ok(Vec::new());
    };
    let (database, name) = single_table_name(table_ref, current_db)?;
    let table = catalog.get_in(&database, &name).ok_or_else(|| {
        DriverError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .generate_with_stack_by_args(&[(&database).into(), (&name).into()]),
        )
    })?;
    match table {
        TableEntry::View(_) => {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_NON_UPDATABLE_TABLE
                    .generate_with_stack_by_args(&[(name.clone()).into(), "UPDATE".into()]),
            ))
        }
        TableEntry::Sequence(_) => {
            return Err(DriverError::unsupported(
                "UPDATE of a sequence is not a statement TiDB accepts",
            ))
        }
        _ => {}
    }
    super::planner_bridge::single_update_assignments(update, catalog, current_db, ctx)
        .map_err(super::planner_error_to_driver)
}

/// The retained definition and cache key for a prepared DML statement.
///
/// Go caches an ordinary `Insert`, `Update`, or `Delete` plan and builds
/// the same executor for a cache hit and a miss. Rust keeps the same lifecycle
/// contract here: this object owns only immutable PREPARE input and cache-key
/// state. The bound statement is executed by the ordinary session DML funnel;
/// there is no cache-only write executor.
#[derive(Debug)]
pub struct PreparedDmlPlan {
    current_database: String,
    table_keys: Vec<CatalogTableKey>,
    /// The tables whose statistics versions key a cache entry (Go
    /// `PlanCacheStmt.tables`), when `tidb_plan_cache_invalidation_on_fresh_stats`
    /// is on.
    stats_table_keys: Vec<CatalogTableKey>,
    parameter_count: usize,
    limit_parameter_orders: Vec<usize>,
    stmt_info: super::access::PlanCacheStmtInfo,
    statement: Stmt,
    cache_key: String,
    last_limit_values: std::sync::Mutex<Vec<u64>>,
}

#[derive(Debug)]
pub(super) struct CachedDmlPlan {
    statement: Stmt,
    physical: tidb_planner_core_operator_physicalop::PhysicalPlan,
    generation: u64,
}

impl CachedDmlPlan {
    pub(super) fn clone_for_execution(&self) -> Self {
        Self {
            statement: self.statement.clone(),
            physical: self.physical.deep_clone(),
            generation: self.generation,
        }
    }

    pub(super) fn memory_usage(&self) -> i64 {
        tidb_planner::physical_plan_cache::cached_plan_memory_usage(&self.physical) as i64
    }

    fn bind(
        &mut self,
        values: &[Datum],
        ctx: Option<&crate::StmtContext>,
    ) -> Result<u64, super::planner_bridge::CachedPlanBindFailure> {
        use super::planner_bridge::CachedPlanBindFailure;
        super::bind_prepared_statement_in_place(&mut self.statement, values)
            .map_err(|_| CachedPlanBindFailure::unbound())?;
        let needs_statement = std::sync::atomic::AtomicBool::new(false);
        let statement = super::planner_bridge::deferred_rebuild_context(ctx, values);
        let parameters = tidb_planner::physical_plan_cache::CachedPlanRebuildContext::new(values);
        let evaluator = |expression: &tidb_expr::expression::Expression| {
            super::planner_bridge::rebuild_evaluate(
                expression,
                statement.as_ref(),
                &parameters,
                &needs_statement,
            )
        };
        let rebuilt = self.physical.rebuild_plan_for_cache_in_place(
            &tidb_planner::physical_plan_cache::CachedPlanRebuildContext::new(values)
                .with_deferred_evaluator(&evaluator),
        );
        if needs_statement.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(CachedPlanBindFailure::NeedsStatement);
        }
        rebuilt.map_err(CachedPlanBindFailure::rebuild_failed)?;
        self.generation = self.generation.wrapping_add(1);
        Ok(self.generation)
    }

    fn execution_mut(
        &mut self,
        generation: u64,
    ) -> Option<(&Stmt, &mut tidb_planner_core_operator_physicalop::PhysicalPlan)> {
        (self.generation == generation).then_some((&self.statement, &mut self.physical))
    }
}

/// One bound execution rebuilt from a retained prepared DML definition.
#[derive(Debug)]
pub struct PreparedDmlExecution {
    parameters: Arc<[Datum]>,
    planning_warnings: std::sync::Mutex<Vec<tidb_context::SqlWarn>>,
    plan: Arc<PreparedDmlPlan>,
    cached_plan: Arc<std::sync::Mutex<CachedDmlPlan>>,
    generation: u64,
    cache_hit: bool,
}

impl PreparedDmlPlan {
    /// Go's non-prepared plan cache collects its statistics tables from the
    /// parameterized statement, where an unqualified table name resolves to
    /// no table: only a name written with its database invalidates the
    /// entry on fresh statistics.
    #[must_use]
    pub fn with_non_prepared_stats_tables(mut self) -> Self {
        self.stats_table_keys = prepared_dml_table_names(&self.statement, "")
            .iter()
            .map(|(database, table)| CatalogTableKey::new(database, table))
            .collect();
        self
    }

    /// Retain Go's original/parameterized SQL identity alongside PREPARE input.
    #[must_use]
    pub fn with_sql(mut self, sql: &str) -> Self {
        self.cache_key = super::plan_cache::statement_key(&self.current_database, sql);
        self
    }

    /// Go DEALLOCATE/COM_STMT_CLOSE deletes the current environment's key,
    /// including every parameter-type variant, unless the session retains it.
    pub fn discard_cached_plan(
        &self,
        cache: &SessionPlanCache,
        catalog: &Catalog,
        environment: &PreparedPlanCacheEnvironment,
    ) {
        cache.delete(&PhysicalPlanCacheKey {
            statement: self.cache_key.clone(),
            schema_version: catalog.schema_version(),
            stats_version_hash: self.stats_version_hash(catalog, environment),
            environment: environment.clone(),
            limit_values: self
                .last_limit_values
                .lock()
                .expect("prepared limits poisoned")
                .clone(),
            dirty_tables: Vec::new(),
        });
    }

    /// The immutable statement retained at PREPARE time.
    #[must_use]
    pub const fn statement(&self) -> &Stmt {
        &self.statement
    }

    /// Binds one EXECUTE's values and checks Go's schema, session, and
    /// parameter-type cache key. A different key is a cache miss, not a
    /// reason to choose a different executor implementation.
    #[must_use]
    pub fn bind(
        self: &Arc<Self>,
        cache: &SessionPlanCache,
        params: &[Datum],
        catalog: &Catalog,
        current_database: &str,
        environment: &PreparedPlanCacheEnvironment,
    ) -> Option<PreparedDmlExecution> {
        let ctx = crate::StmtContext::for_query();
        self.bind_for_statement(
            cache,
            params,
            catalog,
            current_database,
            &ctx,
            &[],
            environment,
            &self.statement,
        )
    }

    /// Binds the statement after the SQL binding selected for this EXECUTE
    /// has replaced its hints. The matching `BindSQL` is carried by
    /// `environment`, so changing a binding cannot hit an older entry.
    #[must_use]
    pub fn bind_for_statement(
        self: &Arc<Self>,
        cache: &SessionPlanCache,
        params: &[Datum],
        catalog: &Catalog,
        current_database: &str,
        ctx: &crate::StmtContext,
        dirty_ids: &[i64],
        environment: &PreparedPlanCacheEnvironment,
        statement: &Stmt,
    ) -> Option<PreparedDmlExecution> {
        self.bind_inner(
            cache,
            params,
            catalog,
            current_database,
            Some(ctx),
            dirty_ids,
            environment,
            statement,
        )
    }

    /// Rebuilds an existing physical DML root without constructing a planner
    /// statement context. A miss leaves physical enumeration to
    /// [`Self::bind_for_statement`].
    #[must_use]
    pub fn bind_cached_for_statement(
        self: &Arc<Self>,
        cache: &SessionPlanCache,
        params: &[Datum],
        catalog: &Catalog,
        current_database: &str,
        dirty_ids: &[i64],
        environment: &PreparedPlanCacheEnvironment,
        statement: &Stmt,
    ) -> Option<PreparedDmlExecution> {
        self.bind_inner(
            cache,
            params,
            catalog,
            current_database,
            None,
            dirty_ids,
            environment,
            statement,
        )
    }

    fn bind_inner(
        self: &Arc<Self>,
        cache: &SessionPlanCache,
        params: &[Datum],
        catalog: &Catalog,
        current_database: &str,
        ctx: Option<&crate::StmtContext>,
        dirty_ids: &[i64],
        environment: &PreparedPlanCacheEnvironment,
        statement: &Stmt,
    ) -> Option<PreparedDmlExecution> {
        if !super::plan_cache::tables_cacheable(catalog, &self.table_keys) {
            return None;
        }
        // Keyed by the PREPARE-time database, as a cached SELECT is.
        let parameter_types: Arc<[PreparedParameterType]> =
            params.iter().map(PreparedParameterType::of).collect();
        let limit_values = self
            .limit_parameter_orders
            .iter()
            .map(|order| match params.get(*order) {
                Some(Datum::Int(value)) if (0..=10_000).contains(value) => Some(*value as u64),
                Some(Datum::UInt(value)) if *value <= 10_000 => Some(*value),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        self.last_limit_values
            .lock()
            .ok()?
            .clone_from(&limit_values);
        let schema_version = catalog.schema_version();
        let stats_version_hash = self.stats_version_hash(catalog, environment);
        let cache_key = PhysicalPlanCacheKey {
            statement: self.cache_key.clone(),
            schema_version,
            stats_version_hash,
            environment: environment.clone(),
            limit_values,
            dirty_tables: super::plan_cache::dirty_statement_tables(catalog, &self.table_keys, dirty_ids),
        };
        let refusal = environment.refusal(self.stmt_info);
        let admitted = refusal.is_none();
        let cached = match admitted.then(|| cache.get(&cache_key, &parameter_types)).flatten() {
            Some(CachedPhysicalPlan::Dml(plan)) => Some(plan),
            _ => None,
        };
        // Go `GetPlanFromPlanCache` refuses with `SetSkipPlanCache`, which
        // warns why.
        if let (Some(reason), Some(ctx)) = (refusal, ctx) {
            ctx.start_prepared_range_tracking();
            ctx.set_skip_plan_cache(reason);
        }
        if cached.is_none() {
            ctx?;
        }
        let parameters: Arc<[Datum]> = Arc::from(params);
        let rebound = match cached {
            Some(plan) => {
                let generation = {
                    let mut cached = plan.lock().ok()?;
                    cached.bind(params, ctx)
                };
                match generation {
                    Ok(generation) => Some((plan, generation)),
                    // A context-free lookup leaves the entry for the bind
                    // that carries the statement.
                    Err(failure) if failure.replan(ctx) => None,
                    Err(_) => return None,
                }
            }
            None => None,
        };
        let (cached_plan, generation, cache_hit) = match rebound {
            Some((plan, generation)) => (plan, generation, true),
            None => {
                let ctx = ctx?.clone().with_prepared_params(Arc::clone(&parameters));
                let bound = super::bind_prepared_statement(statement, params).ok()?;
                let (physical, cacheable) = cached_dml_physical_plan(
                    &bound,
                    catalog,
                    current_database,
                    &ctx,
                    environment.plan_cacheability(self.parameter_count),
                )?;
                // Keep the marker-bearing statement beside the root just as
                // Go keeps PreparedAst and PlanCacheValue.Plan. The physical
                // tree was generated from the current marker values; `bind`
                // below performs the one recursive rebuild used to publish
                // this retained entry.
                let mut plan = CachedDmlPlan {
                    statement: bound,
                    physical,
                    generation: 0,
                };
                // A rejected candidate executes once without a cache rebuild or insertion.
                let cacheable = cacheable && admitted;
                // Go `generateNewPlan` caches the tree it just optimized
                // without rebuilding it; a rebuild it cannot pass fails on
                // the next hit instead, which then replans.
                let generation = if cacheable {
                    plan.bind(params, Some(&ctx)).unwrap_or(0)
                } else {
                    0
                };
                let plan = Arc::new(std::sync::Mutex::new(plan));
                if cacheable {
                    cache.put(
                        cache_key,
                        parameter_types,
                        CachedPhysicalPlan::Dml(Arc::clone(&plan)),
                    );
                }
                (plan, generation, false)
            }
        };
        Some(PreparedDmlExecution {
            parameters,
            planning_warnings: std::sync::Mutex::new(if cache_hit {
                Vec::new()
            } else {
                ctx.map_or_else(Vec::new, crate::StmtContext::take_warnings)
            }),
            plan: Arc::clone(self),
            cached_plan,
            generation,
            cache_hit,
        })
    }

    fn stats_version_hash(
        &self,
        catalog: &Catalog,
        environment: &PreparedPlanCacheEnvironment,
    ) -> u64 {
        environment.stats_version_hash(catalog, &self.stats_table_keys)
    }
}

impl PreparedDmlExecution {
    /// Values owned by this execution, independent of later cache rebuilds.
    #[must_use]
    pub fn parameters(&self) -> Arc<[Datum]> {
        Arc::clone(&self.parameters)
    }
    /// Takes warnings produced by this execution's cache-miss planning once.
    pub fn take_planning_warnings(&self) -> Vec<tidb_context::SqlWarn> {
        std::mem::take(
            &mut *self
                .planning_warnings
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// The immutable PREPARE-time definition used for schema ownership.
    #[must_use]
    pub fn plan(&self) -> &PreparedDmlPlan {
        &self.plan
    }

    /// Whether this execution's full Go cache key matches the last successful
    /// execution.
    #[must_use]
    pub const fn cache_hit(&self) -> bool {
        self.cache_hit
    }

    /// Runs a callback while the cache-owned DML root is pinned to the
    /// generation rebuilt for this EXECUTE. The callback is the ordinary
    /// session statement funnel; this type does not own another executor.
    pub fn with_plan<R>(
        &self,
        callback: impl FnOnce(&Stmt, &mut tidb_planner_core_operator_physicalop::PhysicalPlan) -> R,
    ) -> Option<R> {
        let mut cached = self.cached_plan.lock().ok()?;
        let (statement, physical) = cached.execution_mut(self.generation)?;
        Some(callback(statement, physical))
    }
}

/// Builds the retained definition for an AST-cacheable prepared write.
///
/// Shape-specific physical decisions deliberately do not happen here. Go
/// builds a normal DML root and uses the common executor builder; the session
/// cacheability checker owns admission before this function is called.
pub fn build_prepared_dml_plan(
    statement: &Stmt,
    parameter_count: usize,
    _catalog: &Catalog,
    current_db: &str,
) -> Result<Option<PreparedDmlPlan>, DriverError> {
    if parsed_parameter_count(statement) != parameter_count
        || !matches!(statement, Stmt::Dml(dml) if prepared_dml_kind(dml))
    {
        return Ok(None);
    }
    let table_keys: Vec<CatalogTableKey> = prepared_dml_table_names(statement, current_db)
        .iter()
        .map(|(database, table)| CatalogTableKey::new(database, table))
        .collect();
    Ok(Some(PreparedDmlPlan {
        current_database: current_db.to_owned(),
        stats_table_keys: table_keys.clone(),
        table_keys,
        parameter_count,
        limit_parameter_orders: super::access::prepared_limit_parameter_orders(statement),
        stmt_info: super::access::PlanCacheStmtInfo::of(statement),
        statement: statement.clone(),
        last_limit_values: std::sync::Mutex::default(),
        cache_key: super::plan_cache::statement_key(current_db, &statement.restore()),
    }))
}

fn prepared_dml_kind(dml: &tidb_ast::DmlStmt) -> bool {
    matches!(
        dml,
        tidb_ast::DmlStmt::Insert(_) | tidb_ast::DmlStmt::Update(_) | tidb_ast::DmlStmt::Delete(_)
    )
}

fn cached_dml_physical_plan(
    statement: &Stmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    cacheability: tidb_planner::physical_plan_cache::PlanCacheabilityContext,
) -> Option<(tidb_planner_core_operator_physicalop::PhysicalPlan, bool)> {
    let Stmt::Dml(dml) = statement else {
        return None;
    };
    let (operator, source, update, insert_actual_col_len) = match dml.as_ref() {
        tidb_ast::DmlStmt::Insert(insert) => {
            let extended =
                super::planner_bridge::insert_extended_source(insert, catalog, current_database, ctx);
            let actual = extended.as_ref().map(|(_, width)| *width);
            let source = extended
                .map(|(query, _)| query)
                .or_else(|| insert.source.as_deref().cloned());
            ("Insert", source, None, actual)
        }
        tidb_ast::DmlStmt::Update(update) => (
            "Update",
            Some(update_source_query(update)?),
            Some(update.as_ref()),
            None,
        ),
        tidb_ast::DmlStmt::Delete(delete) => {
            ("Delete", Some(delete_source_query(delete)?), None, None)
        }
        _ => return None,
    };
    let fk_spec = match dml.as_ref() {
        tidb_ast::DmlStmt::Insert(insert) => {
            fk_spec_for_insert(insert, catalog, current_database, ctx)
        }
        tidb_ast::DmlStmt::Update(update) => fk_spec_for_update(update, current_database),
        tidb_ast::DmlStmt::Delete(delete) => fk_spec_for_delete(delete, current_database),
        _ => return None,
    }
    .ok()?;
    ctx.start_prepared_range_tracking();
    let mut root = physical_dml_plan_with_cache_mode(
        operator,
        source.as_ref(),
        update,
        catalog,
        current_database,
        ctx,
        true,
        std::slice::from_ref(&fk_spec),
        DmlKeep::Auto,
        insert_actual_col_len,
    )
    .ok()?;
    // Keep a cached single-row UPDATE/DELETE source as a point access path.
    // The DML root stores its source in `select_plan`, outside the common
    // physical child list, so the helper explicitly descends through it.
    if matches!(operator, "Update" | "Delete") {
        promote_cached_dml_point_source(&mut root);
    }
    if ctx.skip_plan_cache() {
        return Some((root, false));
    }
    tidb_planner::physical_plan_cache::plan_cacheable(&root, cacheability).ok()?;
    Some((root, true))
}

/// Go `tryUpdatePointPlan` / `tryDeletePointPlan`: an UPDATE's or DELETE's
/// point source carries `Lock` in a pessimistic transaction, so its executor
/// locks the row inside `Next` before reading it (`PointGetExecutor.getAndLock`).
/// A cached plan is marked per execution, because the transaction mode of the
/// execution, not of the caching, decides it.
fn mark_dml_point_source_lock(plan: &mut tidb_planner_core_operator_physicalop::PhysicalPlan, pessimistic: bool) {
    use tidb_planner_core_operator_physicalop::PhysicalPlan;
    match plan {
        PhysicalPlan::Dml(root) => {
            if let Some(child) = root.select_plan.as_deref_mut() {
                mark_dml_point_source_lock(child, pessimistic);
            }
        }
        PhysicalPlan::TableReader(reader) => {
            if let Some(child) = reader.table_plan.as_deref_mut() {
                mark_dml_point_source_lock(child, pessimistic);
            }
        }
        PhysicalPlan::PointGet(point) if point.select_lock.is_none() => point.lock = pessimistic,
        PhysicalPlan::BatchPointGet(batch) if batch.select_lock.is_none() => {
            batch.lock = pessimistic;
        }
        _ => {}
    }
}

fn promote_cached_dml_point_source(plan: &mut tidb_planner_core_operator_physicalop::PhysicalPlan) {
    use tidb_planner_core_operator_physicalop::{PhysicalPlan, PhysicalPointGet};
    use tidb_planner_core_operator_physicalop::range_rebuild::PointRangeRebuild;

    if let PhysicalPlan::Dml(dml) = plan {
        if let Some(child) = dml.select_plan.as_deref_mut() {
            promote_cached_dml_point_source(child);
        }
        return;
    }
    if let PhysicalPlan::TableScan(scan) = plan {
        let Some(rebuild) = scan.range_rebuild.clone() else {
            return;
        };
        if scan.ranges.len() != 1 || !scan.ranges[0].is_point_nullable() {
            return;
        }
        *plan = PhysicalPlan::PointGet(PhysicalPointGet {
            base: scan.base.clone(),
            table_id: scan.table_id,
            partition: None,
            index_id: None,
            access_cols: Some(scan.cost_columns.clone()),
            ranges: scan.ranges.clone(),
            range_rebuild: Some(PointRangeRebuild::Table(rebuild)),
            lock: false,
            select_lock: None,
        });
        return;
    }
    if let PhysicalPlan::TableReader(reader) = plan {
        if let Some(child) = reader.table_plan.as_deref_mut() {
            promote_cached_dml_point_source(child);
        }
    }
    for child in plan.base_mut().children_mut() {
        promote_cached_dml_point_source(child);
    }
}

fn physical_plan_contains_point_get(plan: &tidb_planner_core_operator_physicalop::PhysicalPlan) -> bool {
    use tidb_planner_core_operator_physicalop::PhysicalPlan;

    if let PhysicalPlan::Dml(dml) = plan {
        return dml
            .select_plan
            .as_deref()
            .is_some_and(physical_plan_contains_point_get);
    }
    if matches!(
        plan,
        PhysicalPlan::PointGet(_) | PhysicalPlan::BatchPointGet(_)
    ) {
        return true;
    }
    if let PhysicalPlan::TableReader(reader) = plan {
        if reader
            .table_plan
            .as_deref()
            .is_some_and(physical_plan_contains_point_get)
        {
            return true;
        }
    }
    plan.base()
        .children()
        .iter()
        .any(physical_plan_contains_point_get)
}

fn prepared_dml_table_names(statement: &Stmt, current_database: &str) -> Vec<(String, String)> {
    struct Collector<'a> {
        current_database: &'a str,
        names: Vec<(String, String)>,
    }

    impl tidb_ast::Visitor for Collector<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(table_ref) = node.downcast_ref::<tidb_ast::TableRef>() else {
                return false;
            };
            if let Ok((database, table)) = split_table_path(&table_ref.name, self.current_database)
            {
                let name = (database.to_owned(), table.to_owned());
                if !self.names.contains(&name) {
                    self.names.push(name);
                }
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut statement = statement.clone();
    let mut collector = Collector {
        current_database,
        names: Vec::new(),
    };
    use tidb_ast::Visitable as _;
    statement.accept(&mut collector);
    if let Stmt::Dml(dml) = statement {
        if let tidb_ast::DmlStmt::Insert(insert) = dml.as_ref() {
            if let Ok((database, table)) = split_table_path(&insert.table, current_database) {
                let target = (database.to_owned(), table.to_owned());
                if !collector.names.contains(&target) {
                    collector.names.push(target);
                }
            }
        }
    }
    collector.names
}
/// [`run_update_stmt`], recording the plan it builds into `trace`.
///
/// The read plan is the one this function performs -- the `Point_Get`,
/// `TableRangeScan` or `TableFullScan` [`super::access::write_read_path`]
/// chose, with a `Selection` above it for the `WHERE`. Its `actRows` are
/// counted off the very read and predicate the update runs. The one access
/// path a write is still never offered is a non-unique INDEX; see `explain`'s
/// divergence 8.
fn run_update_with_physical(
    update: &tidb_ast::UpdateStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    physical_source: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    planned_update_expressions: Option<&[Option<Expression>]>,
    fast_update_list: bool,
    fk_triggers: &[FkTriggerNode],
    multi_layout: (
        &[tidb_planner_core_operator_physicalop::UpdateTablePos],
        &[tidb_planner_core_operator_physicalop::UpdateAssign],
    ),
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
) -> Result<u64, DriverError> {
    // A `RETURNING` clause is parsed and silently ignored, matching Go: the
    // planner and executor never read `UpdateStmt.Returning`. `StmtContext`
    // already carries `IgnoreErr`, so casts, bad NULLs, and other value-level
    // write checks use the same warning-and-coercion path as INSERT IGNORE.
    let table_ref = match &update.kind {
        tidb_ast::UpdateKind::Single(table_ref) => table_ref,
        // A multi-table write reads a joined row source that carries every
        // target's row identity, which is a different read path -- see
        // `multi_dml`'s shared metadata and row-identity owners.
        tidb_ast::UpdateKind::Multi { .. } => {
            return super::multi_dml::run_multi_update(
                update,
                catalog,
                ctx,
                physical_source,
                multi_layout,
                planned_update_expressions.unwrap_or_default(),
                fk_triggers,
                runtime,
            );
        }
    };
    let (database, name) = single_table_name(table_ref, current_db)?;
    let table = catalog.get_in(&database, &name).ok_or_else(|| {
        DriverError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .generate_with_stack_by_args(&[(&database).into(), (&name).into()]),
        )
    })?;
    if !table_ref.partitions.is_empty() && !matches!(table, TableEntry::Kv(_)) {
        return Err(DriverError::from(
            tidb_table::table_errors::ERR_UNKNOWN_PARTITION.generate_with_stack_by_args(&[
                (table_ref.partitions[0].clone()).into(),
                name.clone().into(),
            ]),
        ));
    }
    let physical_kv_source = matches!(table, TableEntry::Kv(_));
    match table {
        TableEntry::View(_) => {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_NON_UPDATABLE_TABLE
                    .generate_with_stack_by_args(&[(name.clone()).into(), "UPDATE".into()]),
            ))
        }
        TableEntry::Sequence(_) => {
            return Err(DriverError::unsupported(
                "UPDATE of a sequence is not a statement TiDB accepts",
            ))
        }
        _ => {}
    }
    let mut column_list = table.column_list();
    let column_meta = column_metadata(table);
    // Go exposes the heap handle to expressions and assignment admission,
    // independently of whether the WHERE or a value expression names it.
    // It remains outside the table's writable row.
    let extra_handle_slot = crate::driver::from::extra_handle_column(table).map(|column| {
        column_list.push(column);
        column_list.len() - 1
    });
    let column_list = column_list;
    // The rows are stored rows and the planner's assignment offsets address
    // them (`TableInfo.Columns`, hidden columns included), with the extra
    // handle one past the last column; the statement's expressions read the
    // visible columns.
    let stored_meta = stored_column_metadata(table);
    let stored_handle_slot = extra_handle_slot.map(|_| stored_meta.len());
    let visible_offsets: Vec<usize> = match table {
        TableEntry::Kv(kv) => kv.visible_offsets(),
        _ => (0..column_meta.len()).collect(),
    };
    let handle_type = extra_handle_slot.map(|slot| column_list[slot].1.clone());
    let assignment_type = |offset: usize| -> FieldType {
        match stored_meta.get(offset) {
            Some(meta) => meta.field_type.clone(),
            None => handle_type.clone().expect("only the extra handle follows the columns"),
        }
    };

    // Go `buildUpdateLists` resolved every SET item at plan time: the planner's
    // assignments name the target column of each expression.
    let assignments: Vec<(usize, usize)> = multi_layout
        .1
        .iter()
        .map(|assign| (assign.expr, assign.column))
        .collect();
    // Go builds every ordered-list expression in the planner
    // (`buildUpdateLists`, or the fast plan's `buildOrderedList`).
    let mut set_exprs = Vec::with_capacity(assignments.len());
    for (assignment_index, offset) in &assignments {
        let planned = planned_update_expressions
            .and_then(|expressions| expressions.get(*assignment_index))
            .and_then(Option::as_ref)
            .ok_or_else(|| DriverError::unsupported("an UPDATE assignment was not planned"))?;
        set_exprs.push((*offset, planned.clone()));
    }
    let on_update_now =
        PreparedOnUpdateNow::new(&stored_meta, set_exprs.iter().map(|(offset, _)| *offset))?;

    let field_types: Vec<FieldType> = column_list.iter().map(|(_, ft)| ft.clone()).collect();
    // Finish the physical read before evaluating the predicate. The Apply's
    // inner query needs an immutable view of the complete statement snapshot,
    // including the target table itself, and no write is applied until every
    // replacement has been staged.
    // Prepared DML plans intentionally retain the ordinary Go-shaped DML
    // root.  The generic cache builder cannot use the small point child
    // builder because a point plan has to retain the execute-time key.  A
    // cached UPDATE whose predicate is a single primary-key equality would
    // therefore otherwise fall back to a full TableScan on every EXECUTE.
    // Rebuild that one-row child from the already-bound AST here.  This is
    // the same fast path used by a non-prepared point UPDATE and keeps the
    // restored physical root (and its cache key) intact while avoiding a
    // 10-million-row scan.  Non-point writes continue through the retained
    // physical child unchanged.
    // Keep the temporary query alive while the fast plan is built; the
    // helper borrows the SELECT AST while constructing the owned plan.
    let retained_point_source = physical_source
        .as_deref()
        .is_some_and(physical_plan_contains_point_get);
    let fast_source_query =
        if physical_kv_source && update_allows_fast_plan(update) && !retained_point_source {
            update_source_query(update)
        } else {
            None
        };
    let mut fast_point_source = fast_source_query.as_ref().and_then(|source| {
        let tidb_ast::QueryStmt::Select(select) = source else {
            return None;
        };
        // The statement was planned already, and that planning reported the
        // fast plan's conversion warnings (Go `getNameValuePairs`) once. This
        // rebuild repeats the decision under the same type context -- so a
        // pair the planning refused is refused again -- but must not report
        // it a second time.
        let warning_bookmark = ctx.warning_count();
        let fast = super::point_get_plan::try_fast_dml_point_physical_plan_with_allocator(
            select,
            catalog,
            current_db,
            ctx,
            &tidb_planner_core_base::PlanIdAllocator::new(),
        );
        ctx.truncate_warnings(warning_bookmark);
        fast.ok().flatten().map(|mut fast| {
            mark_dml_point_source_lock(&mut fast, ctx.pessimistic_transaction());
            fast
        })
    });
    let mut physical_source = fast_point_source.as_mut().or(physical_source);
    // Go's fast plan (`tryUpdatePointPlan`) reads through a bare point plan,
    // and its `buildOrderedList` casts every assignment to the target column.
    if !fast_update_list
        && update_allows_fast_plan(update)
        && physical_source.as_deref().is_some_and(|plan| {
            matches!(
                plan,
                tidb_planner_core_operator_physicalop::PhysicalPlan::PointGet(_)
                    | tidb_planner_core_operator_physicalop::PhysicalPlan::BatchPointGet(_)
            )
        })
    {
        for (offset, expression) in &mut set_exprs {
            *expression = tidb_planner::plan_builder::ordered_list::cast_point_update_assignment(
                expression.clone(),
                &assignment_type(*offset),
                ctx,
            )
            .map_err(|error| DriverError::Exec(ExecError::Eval(error)))?;
        }
    }
    let partition_ids = match catalog.get_in(&database, &name) {
        Some(TableEntry::Kv(kv)) if table_ref.partitions.is_empty() => None,
        Some(TableEntry::Kv(kv)) => {
            let Some(spec) = kv.partition() else {
                return Err(DriverError::from(
                    tidb_table::table_errors::ERR_UNKNOWN_PARTITION.generate_with_stack_by_args(
                        &[
                            (table_ref.partitions[0].clone()).into(),
                            name.clone().into(),
                        ],
                    ),
                ));
            };
            Some(
                tidb_planner_rule::ids_for_selected_partitions(spec, &table_ref.partitions)
                    .map_err(|partition| {
                        DriverError::from(
                            tidb_table::table_errors::ERR_UNKNOWN_PARTITION
                                .generate_with_stack_by_args(&[
                                    partition.into(),
                                    name.clone().into(),
                                ]),
                        )
                    })?,
            )
        }
        _ => None,
    };
    let physical = physical_source
        .as_deref_mut()
        .ok_or_else(|| DriverError::unsupported("UPDATE has no retained physical child"))?;
    let mut matched = 0u64;
    let mut touched = 0u64;
    let mut changed = 0u64;
    let mut rewrites = Vec::new();
    let mut records = UpdateRecords::new(fk_triggers);
    let writes_stored_columns = set_exprs
        .iter()
        .any(|(offset, _)| Some(*offset) != stored_handle_slot);
    let accountant = ctx
        .statement_memory()
        .write_accountant(mem_quota::label::UPDATE);
    // Go `UpdateExec.updateRows` composes each row and writes it before the
    // next, and asks its source for the next chunk only afterwards: row 1's
    // write error or `HandleBadNull` warning comes before row 2's cast
    // warning, and the source's `SelectLockExec` locks after the last chunk's
    // writes. Assignments that read the catalog (a correlated subquery)
    // instead see the whole statement snapshot first.
    if writes_stored_columns {
        let mut row_index = 0usize;
        stream_physical_write_rows(
            physical,
            catalog,
            &database,
            &name,
            ctx,
            runtime,
            mem_quota::label::UPDATE,
            None,
            |catalog, row, physical_field_types| {
                let handle = extra_handle_value(&row.id);
                let mut new_row = UpdateRowEvaluator {
                    field_types: &field_types,
                    stored_meta: &stored_meta,
                    visible_offsets: &visible_offsets,
                    set_exprs: &set_exprs,
                    ctx,
                    on_update_now: &on_update_now,
                    stored_handle_slot,
                }
                .compute(
                    &row.stored,
                    handle,
                    Some((&row.output, physical_field_types)),
                    &mut matched,
                )?;
                accountant
                    .account_row(&new_row)
                    .map_err(DriverError::from)?;
                let outcome = records.write(
                    catalog,
                    &database,
                    &name,
                    &row.id,
                    &row.stored,
                    &mut new_row,
                    partition_ids.as_deref(),
                    update.ignore,
                    GeneratedWrite::Update { row_index },
                    ctx,
                )?;
                row_index += 1;
                touched += u64::from(outcome.touched());
                changed += u64::from(outcome.changed());
                Ok(())
            },
        )?;
        records.finish(catalog, ctx)?;
        ctx.set_message(format!(
            "Rows matched: {matched}  Changed: {changed}  Warnings: {}",
            ctx.warning_count()
        ));
        return Ok(if ctx.client_found_rows() {
            touched
        } else {
            changed
        });
    }
    let PhysicalWriteRows {
        rows,
        field_types: physical_field_types,
    } = execute_physical_write_rows(
        physical,
        catalog,
        &database,
        &name,
        ctx,
        runtime,
        mem_quota::label::UPDATE,
        None,
    )?;
    let row_evaluator = UpdateRowEvaluator {
        field_types: &field_types,
        stored_meta: &stored_meta,
        visible_offsets: &visible_offsets,
        set_exprs: &set_exprs,
        ctx,
        on_update_now: &on_update_now,
        stored_handle_slot,
    };
    for row in rows {
        let handle = extra_handle_value(&row.id);
        let new_row = row_evaluator.compute(
            &row.stored,
            handle,
            Some((&row.output, &physical_field_types)),
            &mut matched,
        )?;
        accountant
            .account_row(&new_row)
            .map_err(DriverError::from)?;
        rewrites.push((row.id, row.stored, new_row));
    }
    if !writes_stored_columns {
        // Go assignFlag excludes the extra handle from each writable table range.
        matched = 0;
        rewrites.clear();
    }
    for (row_index, (id, old_row, mut new_row)) in rewrites.into_iter().enumerate() {
        let outcome = records.write(
            catalog,
            &database,
            &name,
            &id,
            &old_row,
            &mut new_row,
            partition_ids.as_deref(),
            update.ignore,
            GeneratedWrite::Update { row_index },
            ctx,
        )?;
        touched += u64::from(outcome.touched());
        changed += u64::from(outcome.changed());
    }
    records.finish(catalog, ctx)?;
    ctx.set_message(format!(
        "Rows matched: {matched}  Changed: {changed}  Warnings: {}",
        ctx.warning_count()
    ));
    Ok(if ctx.client_found_rows() {
        touched
    } else {
        changed
    })
}

struct UpdateRowEvaluator<'a> {
    /// The statement's visible columns, then Go's extra handle column when
    /// the table has one.
    field_types: &'a [FieldType],
    /// Every column of the stored row, which `set_exprs` offsets address.
    stored_meta: &'a [ColumnDefaultMeta],
    /// The stored offsets of the visible columns, in order.
    visible_offsets: &'a [usize],
    set_exprs: &'a [(usize, Expression)],
    ctx: &'a crate::StmtContext,
    on_update_now: &'a PreparedOnUpdateNow,
    /// The extra handle's assignment offset, one past the stored row: Go
    /// evaluates it but stages no column for it.
    stored_handle_slot: Option<usize>,
}

impl UpdateRowEvaluator<'_> {
    /// Evaluates one selected row. The shared record owner decides whether
    /// it changed and retains unchanged selected rows for locking.
    fn compute(
        &self,
        row: &[Datum],
        handle: Option<i64>,
        physical_input: Option<(&[Datum], &[FieldType])>,
        matched: &mut u64,
    ) -> Result<Vec<Datum>, DriverError> {
        let mut evaluated: Vec<Datum> =
            self.visible_offsets.iter().map(|offset| row[*offset].clone()).collect();
        if self.stored_handle_slot.is_some() {
            evaluated.push(handle.map_or(Datum::Null, Datum::Int));
        }
        let sql_row = evaluated.as_slice();
        let chunk = row_chunk(sql_row, self.field_types)?;
        let physical_chunk = physical_input
            .map(|(values, field_types)| row_chunk(values, field_types))
            .transpose()?;
        let physical_row = physical_chunk.as_ref().map(|chunk| chunk.get_row(0));
        // Matched rows include unchanged rows; the record owner separately
        // counts touched and affected rows.
        let row_index = *matched as usize;
        *matched += 1;
        // Every assignment reads the row as the statement found it, so
        // `SET a = 100, b = a` stores the ORIGINAL `a` in `b`, and
        // `SET c = a, a = b, b = c` rotates the three original values in one step.
        // Go builds the whole new row from the old one before writing any of it
        // (`executor.UpdateExec` composes `newRowData` off the fetched row), which
        // is why an earlier assignment is invisible to a later one. Evaluating
        // against the single unmodified `chunk` makes that the only reading there
        // is, rather than a rule the loop has to re-establish per assignment.
        let mut new_row = row.to_vec();
        for (offset, expr) in self.set_exprs {
            let value = expr
                .eval(
                    self.ctx,
                    physical_row.ok_or_else(|| {
                        DriverError::unsupported(
                            "a planned UPDATE expression has no physical input row",
                        )
                    })?,
                )
                .map_err(|error| DriverError::Exec(ExecError::Eval(error)))?;
            // ExtraHandleID has no column metadata in Go composeNewRow:
            // evaluate it, but neither cast it nor put it in the writable row.
            if self.stored_handle_slot == Some(*offset) {
                continue;
            }
            new_row[*offset] = cast_value_for_update_assignment(
                value,
                &self.stored_meta[*offset].field_type,
                &self.stored_meta[*offset].name,
                row_index,
                self.ctx,
            )?;
        }
        self.on_update_now
            .apply(row, &mut new_row, self.ctx, chunk.get_row(0))?;
        Ok(new_row)
    }
}

/// Runs a single-table `DELETE`, returning the number of removed rows.
///
/// Go `executor.DeleteExec`: every row the `WHERE` selects is removed, and the
/// affected-row count is simply that count.
///
/// `DELETE IGNORE` checks restrictions before each candidate row and
/// downgrades violations to warnings; ordinary checks see the final buffer.
///
/// `QUICK` is a parser-only storage hint in Go: neither its planner nor its
/// executor reads `DeleteStmt.Quick`, so it has the same row/count behavior
/// as plain `DELETE` here. Single-table `ORDER BY`/`LIMIT` is supported (see
/// the retained physical read child). A `RETURNING` clause is parsed and
/// silently ignored, matching Go, where the planner and executor never read
/// `DeleteStmt.Returning`.
pub fn run_delete_on(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_delete_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_delete_on`] resolving unqualified names in `current_db`.
pub fn run_delete_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    let stmt = ctx.parse(sql)?;
    let delete = match &stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Delete(delete) => delete,
            _ => return Err(DriverError::unsupported("only DELETE is supported here")),
        },
        _ => return Err(DriverError::unsupported("only DELETE is supported here")),
    };
    run_delete_stmt(delete, catalog, current_db, ctx)
}

/// [`run_delete_in`]'s body, taking the already-parsed AST directly -- see
/// [`run_update_stmt`]'s doc for why `explain::explain_analyze_delete_stmt`
/// needs this split.
pub fn run_delete_stmt(
    delete: &tidb_ast::DeleteStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_delete_stmt_with_physical(delete, catalog, current_db, ctx, None)
}

/// The ordinary DELETE executor, optionally consuming the cached
/// `Delete.SelectPlan` selected by the shared physical planner.
pub fn run_delete_stmt_with_physical(
    delete: &tidb_ast::DeleteStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    physical_plan: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
) -> Result<u64, DriverError> {
    run_delete_stmt_with_physical_and_stats(delete, catalog, current_db, ctx, physical_plan, None)
}

pub(crate) fn run_delete_stmt_with_physical_and_stats(
    delete: &tidb_ast::DeleteStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    physical_plan: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
) -> Result<u64, DriverError> {
    // Go's `PlanBuilder.buildDelete` resolves the target and refuses a view or
    // sequence before it builds the read child.  A sequence is not a row
    // source, so letting the physical planner inspect it first would turn the
    // intended plain 1105 refusal into a misleading 1146 unknown-table error.
    // Keep this preflight limited to known read-only objects; ordinary table
    // and missing-table ordering remains owned by the existing planner path.
    if let tidb_ast::DeleteKind::Single(table_ref) = &delete.kind {
        // `isCTE(tblW)` comes first: a CTE shadows any table of its name.
        if super::planner_bridge::is_cte(delete.with.as_ref(), table_ref) {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_NON_UPDATABLE_TABLE.generate_with_stack_by_args(&[
                    (table_ref.name.join(".")).into(),
                    "DELETE".into(),
                ]),
            ));
        }
        let (database, name) = single_table_name(table_ref, current_db)?;
        match catalog.get_in(&database, &name) {
            Some(TableEntry::View(_)) => {
                return Err(DriverError::from(tidb_terror::MysqlError::unknown(
                    format!("delete view {} is not supported now", name),
                )));
            }
            Some(TableEntry::Sequence(_)) => {
                return Err(DriverError::from(tidb_terror::MysqlError::unknown(
                    format!("delete sequence {} is not supported now", name),
                )));
            }
            _ => {}
        }
    }
    let source = delete_source_query(delete);
    // Go `planner.Optimize`.
    let optimize = super::start_optimize_region();
    let mut fresh = physical_plan
        .is_none()
        .then(|| match &delete.kind {
            tidb_ast::DeleteKind::Single(_) => physical_dml_plan(
                "Delete",
                source.as_ref(),
                None,
                catalog,
                current_db,
                ctx,
                &fk_spec_for_delete(delete, current_db)?,
            ),
            tidb_ast::DeleteKind::Multi { .. } => super::multi_dml::multi_dml_physical_plan(
                super::multi_dml::MultiDmlRef::Delete(delete),
                catalog,
                current_db,
                ctx,
            ),
        })
        .transpose()?;
    optimize.end();
    let mut physical_plan = physical_plan.or(fresh.as_mut());
    if let Some(plan) = physical_plan.as_deref_mut() {
        mark_dml_point_source_lock(plan, ctx.pessimistic_transaction());
        super::physical_builder::prepare_execution_plan(plan, catalog, ctx)?;
        crate::explain::publish_physical_process_info(ctx, plan, catalog);
    }
    // Go `runStmt` -> `handleNoDelayExecutor` -> the DML executor's `Next`.
    let _execution = super::begin_dml_execution("*executor.DeleteExec.Next");
    let (physical_source, fk_triggers, delete_col_pos, delete_tables) =
        dml_execution_parts(physical_plan, "Delete")?;
    run_delete_with_physical(
        delete,
        catalog,
        current_db,
        ctx,
        physical_source,
        fk_triggers,
        delete_col_pos,
        delete_tables,
        runtime,
    )
}

/// How a DML read decides the columns it keeps (see [`DeletePruning`]).
pub(crate) enum DmlKeep {
    /// A single-table statement: the planner prunes a DELETE's read.
    Auto,
    /// A multi-table DELETE: the planner prunes each target's read and lays
    /// out its rows.
    MultiDelete(tidb_planner::plan_builder::delete_pruning::DeleteMultiInput),
}

/// Whether a foreign key another table declares references the table a
/// single-table DELETE targets: Go's `len(del.FKCascades) > 0 ||
/// len(del.FKChecks) > 0`, which declines the read's column pruning.
fn delete_has_fk(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> bool {
    let Some(table_ref) = super::point_get_plan::sole_table_ref(&select.from) else {
        return false;
    };
    let Ok((database, name)) = single_table_name(table_ref, current_db) else {
        return false;
    };
    ctx.foreign_key_checks() && !crate::foreign_key::referring(catalog, &database, &name).is_empty()
}

pub(crate) fn delete_source_query(delete: &tidb_ast::DeleteStmt) -> Option<tidb_ast::QueryStmt> {
    match &delete.kind {
        tidb_ast::DeleteKind::Single(table_ref) => Some(super::multi_dml::multi_dml_select(
            delete.with.as_ref(),
            &delete.hints,
            &tidb_planner::plan_builder::dml_source::single_table_join(table_ref),
            delete.where_clause.as_ref(),
            &delete.order_by,
            delete.limit.as_ref(),
        )),
        tidb_ast::DeleteKind::Multi { .. } => None,
    }
}

fn run_delete_with_physical(
    delete: &tidb_ast::DeleteStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut physical_source: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    fk_triggers: &[FkTriggerNode],
    delete_col_pos: Option<&tidb_planner_core_operator_physicalop::DeleteColPos>,
    delete_tables: &[tidb_planner_core_operator_physicalop::DeleteTablePos],
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
) -> Result<u64, DriverError> {
    // `DELETE IGNORE` differs from a plain `DELETE` only in what it does with
    // a referential violation: Go downgrades it from a statement error to a
    // per-row skip with a warning. `QUICK` is parser-only and needs no branch.
    let table_ref = match &delete.kind {
        tidb_ast::DeleteKind::Single(table_ref) => table_ref,
        // See `multi_dml`'s shared metadata and row-identity owners.
        tidb_ast::DeleteKind::Multi { .. } => {
            return super::multi_dml::run_multi_delete(
                delete,
                catalog,
                ctx,
                physical_source,
                delete_tables,
                fk_triggers,
                runtime,
            );
        }
    };
    let (database, name) = single_table_name(table_ref, current_db)?;
    if !table_ref.partitions.is_empty()
        && !matches!(catalog.get_in(&database, &name), Some(TableEntry::Kv(_)))
    {
        return Err(DriverError::from(
            tidb_table::table_errors::ERR_UNKNOWN_PARTITION.generate_with_stack_by_args(&[
                (table_ref.partitions[0].clone()).into(),
                name.clone().into(),
            ]),
        ));
    }
    let physical = physical_source
        .as_deref_mut()
        .ok_or_else(|| DriverError::unsupported("DELETE has no retained physical child"))?;
    // Go `DeleteExec.deleteSingleTableByChunk` removes each chunk's rows
    // before asking its source for the next chunk.
    let mut records = DeleteRecords::new(fk_triggers);
    let mut deleted = 0;
    stream_physical_write_rows(
        physical,
        catalog,
        &database,
        &name,
        ctx,
        runtime,
        mem_quota::label::DELETE,
        delete_col_pos,
        |catalog, row, _| {
            deleted += u64::from(records.write(
                catalog,
                &database,
                &name,
                &row.id,
                &row.stored,
                delete.ignore,
                ctx,
            )?);
            Ok(())
        },
    )?;
    records.finish(catalog, ctx)?;
    Ok(deleted)
}

/// Fetches the records a single-table write will filter, through the read
/// path chosen for it.
///
/// One function for `UPDATE` and `DELETE` both, because the two statements
/// differ in what they do with a record and not in how they find one -- and
/// because a second copy of this dispatch is exactly how a write path comes
/// to read a different record set than the plan it printed.
///
/// A point get reads ONE key. `get_row_by_handle` is the same read
/// `HandleSourceExec` performs for a `SELECT`'s `Point_Get`, and it answers
/// `None` for a key no record carries -- Go's point get that finds nothing.
struct PhysicalWriteRow {
    id: TableHandle,
    stored: Vec<Datum>,
    output: Vec<Datum>,
}

struct PhysicalWriteRows {
    rows: Vec<PhysicalWriteRow>,
    field_types: Vec<FieldType>,
}

/// How a row of a single-table write's source becomes the record the write
/// changes: its handle, the stored columns, and the source's own row.
///
/// It owns what it needs of the table, so a write can apply each row to the
/// catalog while the source executor is still delivering the next ones.
struct PhysicalWriteShape {
    field_types: Vec<FieldType>,
    table: Arc<tidb_table::kv_table::KvTable>,
    stored_width: usize,
    expected_width: usize,
    /// Where `_tidb_rowid` sits in a row of this read.
    extra_position: usize,
    /// The stored-column offsets a pruned DELETE read keeps (see
    /// [`DeletePruning`]); `None` when the read carries every column.
    kept: Option<Vec<usize>>,
}

impl PhysicalWriteShape {
    fn new(
        physical: &tidb_planner_core_operator_physicalop::PhysicalPlan,
        catalog: &Catalog,
        database: &str,
        name: &str,
        pruning: Option<&tidb_planner_core_operator_physicalop::DeleteColPos>,
    ) -> Result<Self, DriverError> {
        let field_types = physical
            .schema()
            .ok_or_else(|| DriverError::unsupported("a physical write child has no schema"))?
            .columns
            .iter()
            .map(|column| {
                column.ret_type.clone().ok_or_else(|| {
                    DriverError::unsupported("a physical write child has an untyped column")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let table = catalog.get_in(database, name).ok_or_else(|| {
            DriverError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .generate_with_stack_by_args(&[(&database).into(), (&name).into()]),
            )
        })?;
        let TableEntry::Kv(table) = table else {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_table::table_errors::ERR_UNSUPPORTED_OP.generate_with_stack_by_args(&[]),
            )));
        };
        let stored_width = table.columns().len();
        let has_extra_handle =
            table.pk_handle_offset().is_none() && table.common_handle_offsets().is_empty();
        // A pruned DELETE read carries only the kept columns (see
        // [`DeletePruning`]); a plan cached before the decision changed carries
        // them all, which the row width tells apart.
        let pruning = pruning.filter(|pruning| field_types.len() == pruning.width());
        let expected_width =
            pruning.map_or(stored_width + usize::from(has_extra_handle), |p| p.width());
        let extra_position = pruning.map_or(stored_width, |p| p.kept.len());
        Ok(Self {
            field_types,
            table: table.clone(),
            stored_width,
            expected_width,
            extra_position,
            kept: pruning.map(|pruning| pruning.kept.clone()),
        })
    }

    /// The cell holding the stored column at `offset`.
    fn cell(&self, row: &[Datum], offset: usize) -> Option<Datum> {
        match &self.kept {
            None => row.get(offset).cloned(),
            Some(kept) => kept
                .iter()
                .position(|kept| *kept == offset)
                .and_then(|position| row.get(position).cloned()),
        }
    }

    fn row(&self, row: Vec<Datum>, ctx: &crate::StmtContext) -> Result<PhysicalWriteRow, DriverError> {
        let table = &self.table;
        if row.len() < self.expected_width {
            return Err(DriverError::unsupported(format!(
                "a physical write child returned {} columns, expected at least {}",
                row.len(),
                self.expected_width
            )));
        }
        let id = if let Some(offset) = table.pk_handle_offset() {
            match self.cell(&row, offset) {
                Some(Datum::Int(value)) => tidb_table::kv_table::TableHandle::Int(value),
                Some(Datum::UInt(value)) => tidb_table::kv_table::TableHandle::Int(value as i64),
                _ => {
                    return Err(DriverError::unsupported(
                        "a physical write child returned an invalid integer handle",
                    ));
                }
            }
        } else if !table.common_handle_offsets().is_empty() {
            let values = table
                .common_handle_offsets()
                .iter()
                .map(|offset| {
                    self.cell(&row, *offset).ok_or_else(|| {
                        DriverError::unsupported(
                            "a physical write child dropped a common-handle column",
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            table
                .common_handle_of_values(&values, &ctx.session_zone())
                .map_err(kv_write_error)?
        } else {
            match row.get(self.extra_position) {
                Some(Datum::Int(value)) => tidb_table::kv_table::TableHandle::Int(*value),
                Some(Datum::UInt(value)) => tidb_table::kv_table::TableHandle::Int(*value as i64),
                _ => {
                    return Err(DriverError::unsupported(
                        "a physical write child returned no _tidb_rowid handle",
                    ));
                }
            }
        };
        // A pruned column is NULL here: removing a record reads only the
        // handle's and the indexes' columns.
        let stored = match &self.kept {
            None => row[..self.stored_width].to_vec(),
            Some(kept) => {
                let mut stored = vec![Datum::Null; self.stored_width];
                for (position, offset) in kept.iter().enumerate() {
                    stored[*offset] = row[position].clone();
                }
                stored
            }
        };
        Ok(PhysicalWriteRow {
            id,
            stored,
            output: row,
        })
    }
}

/// The rows a single-table write changes, all read before the first write.
/// Only an assignment that reads the catalog needs this; every other write
/// applies each row as the source delivers it ([`stream_physical_write_rows`]).
fn execute_physical_write_rows(
    physical: &mut tidb_planner_core_operator_physicalop::PhysicalPlan,
    catalog: &mut Catalog,
    database: &str,
    name: &str,
    ctx: &crate::StmtContext,
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
    memory_label: i64,
    pruning: Option<&tidb_planner_core_operator_physicalop::DeleteColPos>,
) -> Result<PhysicalWriteRows, DriverError> {
    let mut rows = Vec::new();
    let field_types = stream_physical_write_rows(
        physical,
        catalog,
        database,
        name,
        ctx,
        runtime,
        memory_label,
        pruning,
        |_, row, _| {
            rows.push(row);
            Ok(())
        },
    )?;
    Ok(PhysicalWriteRows { rows, field_types })
}

/// Go `UpdateExec.updateRows` / `DeleteExec.deleteSingleTableByChunk`'s
/// loop: `apply` writes each row the source delivers before the source is
/// asked for the next chunk, so what the source does when it is drained (a
/// `SelectLockExec`'s lock) comes after the last chunk's writes. Returns the
/// source's column types.
fn stream_physical_write_rows(
    physical: &mut tidb_planner_core_operator_physicalop::PhysicalPlan,
    catalog: &mut Catalog,
    database: &str,
    name: &str,
    ctx: &crate::StmtContext,
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
    memory_label: i64,
    pruning: Option<&tidb_planner_core_operator_physicalop::DeleteColPos>,
    mut apply: impl FnMut(&mut Catalog, PhysicalWriteRow, &[FieldType]) -> Result<(), DriverError>,
) -> Result<Vec<FieldType>, DriverError> {
    let shape = PhysicalWriteShape::new(physical, catalog, database, name, pruning)?;
    let accountant = ctx.statement_memory().write_accountant(memory_label);
    let collected = super::physical_builder::execute_dml_source_writing(
        physical,
        catalog,
        ctx,
        runtime.is_some(),
        |catalog, row| {
            let row = shape.row(row, ctx)?;
            // Both representations stay live until the write.
            accountant
                .account_row(&row.output)
                .map_err(DriverError::from)?;
            accountant
                .account_row(&row.stored)
                .map_err(DriverError::from)?;
            apply(catalog, row, &shape.field_types)
        },
    )?;
    if let Some(runtime) = runtime {
        runtime.extend(collected);
    }
    Ok(shape.field_types)
}

/// A one-row chunk holding `row`, so an expression can be evaluated over it.
///
/// `field_types` is the SCHEMA the expression was built against, and it is
/// what decides the chunk's width. A row read straight from storage is wider
/// than that when the table has hidden expression-index columns; those are
/// the TAIL (see `tidb_ddl_build::expression_index`), and no expression a statement
/// can write is able to name one, so the visible prefix is exactly the row
/// the expression means.
pub(crate) fn row_chunk(
    row: &[Datum],
    field_types: &[FieldType],
) -> Result<tidb_chunk::chunk::Chunk, DriverError> {
    let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(field_types, 1);
    for (i, value) in row.iter().take(field_types.len()).enumerate() {
        chunk.append_datum(i, value);
    }
    // A row SHORTER than the schema (a partially built one) still has to
    // present every column, or a reference to a trailing one reads off the
    // end.
    for i in row.len()..field_types.len() {
        chunk.append_datum(i, &Datum::Null);
    }
    Ok(chunk)
}

/// The integer a record handle reports as `_tidb_rowid`.
///
/// A partitioned heap table keys its rows by `(partition_id, handle)` and Go
/// reports the handle, which is why the same rowid recurs across partitions.
/// A common handle has no rowid at all, and the leaf never offers the column
/// for such a table.
fn extra_handle_value(handle: &tidb_table::kv_table::TableHandle) -> Option<i64> {
    fn integer(handle: &tidb_tablecodec::table_key::RecordHandle) -> Option<i64> {
        match handle {
            tidb_tablecodec::table_key::RecordHandle::Int(value) => Some(*value),
            tidb_tablecodec::table_key::RecordHandle::Partition { handle, .. } => integer(handle),
            tidb_tablecodec::table_key::RecordHandle::Common(_) => None,
        }
    }
    integer(&handle.record_handle())
}
