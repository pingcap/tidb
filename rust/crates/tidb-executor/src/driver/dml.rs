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

use super::plan_cache::{CachedPhysicalPlan, PhysicalPlanCacheKey, SessionPlanCache};
use super::*;
use crate::kv_table::{AutoIdError, AutoIncrement, AutoRandom, AutoRandomError};

mod correlated;
mod defaults;
pub(crate) mod delete_record;
pub(crate) mod update_record;

use delete_record::DeleteRecords;
use tidb_planner::physical::FkTriggerNode;
use update_record::UpdateRecords;

use correlated::{dml_table_scope, insert_table_scope, DmlExpression, UpdateExpression};

pub(crate) use defaults::{
    column_default, column_metadata, materialize_column_default, prepare_named_defaults,
    rewrite_with_prepared_defaults, ColumnDefaultMeta, DefaultColumnIdentity, DefaultUse,
    PreparedOnUpdateNow, ResolvedDefaultColumn, set_value_for_ref_column,
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
    physical_plan: Option<&mut tidb_planner::physical::PhysicalPlan>,
) -> Result<(u64, Option<u64>), DriverError> {
    run_insert_stmt_with_physical_and_stats(insert, catalog, current_db, ctx, physical_plan, None)
}

pub(crate) fn run_insert_stmt_with_physical_and_stats(
    insert: &tidb_ast::InsertStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    physical_plan: Option<&mut tidb_planner::physical::PhysicalPlan>,
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
) -> Result<(u64, Option<u64>), DriverError> {
    // go resolves the INSERT's target table before planning the source
    // query: `INSERT INTO h1 (a, b) SELECT a, b FROM no_such` over a missing
    // h1 errors `Table 'fdq.h1' doesn't exist`, not the source query's own
    // 1146 (oracle m13).
    resolve_insert_target(insert, catalog, current_db, ctx)?;
    let extended_source = insert_plan_source(insert, catalog, current_db);
    let mut fresh = physical_plan
        .is_none()
        .then(|| {
            physical_dml_plan(
                "Insert",
                extended_source.as_ref().or(insert.source.as_deref()),
                None,
                catalog,
                current_db,
                ctx,
                &fk_spec_for_insert(insert, current_db)?,
            )
        })
        .transpose()?;
    let mut physical_plan = physical_plan.or(fresh.as_mut());
    if let Some(plan) = physical_plan.as_deref_mut() {
        super::physical_builder::prepare_execution_plan(plan, catalog, ctx)?;
        ctx.publish_physical_process_info(plan, catalog);
    }
    let (physical_source, fk_triggers) = dml_execution_parts(physical_plan, "Insert")?;
    run_insert_with_physical(
        insert,
        catalog,
        current_db,
        ctx,
        physical_source,
        fk_triggers,
        runtime,
    )
}

/// The query an `INSERT ... SELECT` plans: its own source, or Go's
/// ON DUPLICATE extension of it (see [`super::on_duplicate_scope`]).
pub(crate) fn insert_plan_source(
    insert: &tidb_ast::InsertStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Option<tidb_ast::QueryStmt> {
    super::on_duplicate_scope::extended_source(insert, catalog, current_db).map(|(query, _)| query)
}

fn dml_execution_parts<'a>(
    plan: Option<&'a mut tidb_planner::physical::PhysicalPlan>,
    operator: &str,
) -> Result<
    (
        Option<&'a mut tidb_planner::physical::PhysicalPlan>,
        &'a [FkTriggerNode],
    ),
    DriverError,
> {
    let Some(tidb_planner::physical::PhysicalPlan::Dml(root)) = plan else {
        return Err(DriverError::unsupported(
            "DML execution has no physical root",
        ));
    };
    if root.go_operator != operator {
        return Err(DriverError::unsupported(
            "DML execution received the wrong physical root",
        ));
    }
    Ok((root.select_plan.as_deref_mut(), root.fk_triggers.as_slice()))
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
) -> Result<tidb_planner::physical::PhysicalPlan, DriverError> {
    physical_dml_plan_with_cache_mode(
        operator,
        source,
        update,
        catalog,
        current_db,
        ctx,
        false,
        std::slice::from_ref(fk_spec),
    )
}

/// Multi-table callers carry all resolved table policies into the same DML
/// allocator and source builder used by single-table writes.
pub(crate) fn physical_multi_dml_plan(
    operator: &str,
    source: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    fk_specs: &[fk_trigger_plan::FkPlanSpec],
) -> Result<tidb_planner::physical::PhysicalPlan, DriverError> {
    physical_dml_plan_with_cache_mode(
        operator,
        Some(source),
        None,
        catalog,
        current_db,
        ctx,
        false,
        fk_specs,
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
) -> Result<tidb_planner::physical::PhysicalPlan, DriverError> {
    use tidb_planner::physical::{BasePhysicalPlan, PhysicalDmlRoot, PhysicalPlan};

    for spec in fk_specs {
        if matches!(
            catalog.get_in(&spec.database, &spec.table),
            Some(TableEntry::Mem(_))
        ) {
            return Err(DriverError::Mysql(MysqlError::new(
                tidb_error::tidb::errcode::ErrUnsupportedOp,
                "operation not supported",
            )));
        }
    }

    let plan_ids = tidb_planner::plan_base::PlanIdAllocator::new();
    let column_ids = tidb_planner::expression_rewriter::ColumnIdAllocator::new();
    let mut update_expressions = Vec::new();

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
        // above, ahead of the source.
        Some(tidb_ast::QueryStmt::Select(select)) if operator.eq_ignore_ascii_case("Insert") => {
            Some(
                super::planner_bridge::physical_query_plan_with_allocators(
                    &tidb_ast::QueryStmt::Select(select.clone()),
                    catalog,
                    current_db,
                    ctx,
                    use_plan_cache,
                    &plan_ids,
                    &column_ids,
                )
                .map_err(super::planner_error_to_driver)?,
            )
        }
        Some(tidb_ast::QueryStmt::Select(select)) => {
            let allow_fast_plan = update.is_none_or(|update| update_allows_fast_plan(update));
            let fast = (!use_plan_cache && allow_fast_plan)
                .then(|| {
                    super::access::try_fast_dml_point_physical_plan_with_allocator(
                        select, catalog, current_db, ctx, &plan_ids,
                    )
                })
                .transpose()?
                .flatten();
            if let Some(mut fast) = fast {
                // Go's point plans carry `Lock` for UPDATE and DELETE.
                if let PhysicalPlan::PointGet(point) = &mut fast {
                    point.lock = ctx.pessimistic_transaction();
                }
                // Go's point-UPDATE builder allocates one extra plan between
                // the PointGet and the Update root; the DELETE builder does
                // not.
                if operator.eq_ignore_ascii_case("Update") {
                    let _point_update_filler = plan_ids.alloc();
                }
                root_base = Some(BasePhysicalPlan::new(&plan_ids, operator, 0));
                Some(fast)
            } else {
                let update_assignment_values = update
                    .map(|update| {
                        update_assignment_values_for_plan(update, catalog, current_db, ctx)
                    })
                    .transpose()?;
                let (plan, expressions, root) =
                    super::planner_bridge::physical_dml_source_plan_with_allocators(
                        select,
                        update_assignment_values.as_deref(),
                        catalog,
                        current_db,
                        ctx,
                        use_plan_cache,
                        &plan_ids,
                        &column_ids,
                        operator,
                    )
                    .map_err(super::planner_error_to_driver)?;
                update_expressions = expressions;
                root_base = Some(root);
                Some(plan)
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
    });
    super::physical_builder::prepare_execution_plan(&mut plan, catalog, ctx)?;
    Ok(plan)
}

/// Go `tnW.DBInfo.Name.L` / `tableInfo.Name` for the INSERT target, plus the
/// REPLACE / ON DUPLICATE facts `BuildOnInsertFKTriggers` branches on.
pub(crate) fn fk_spec_for_insert(
    insert: &tidb_ast::InsertStmt,
    current_db: &str,
) -> Result<fk_trigger_plan::FkPlanSpec, DriverError> {
    let (database, table) = super::catalog::split_table_path(&insert.table, current_db)?;
    Ok(fk_trigger_plan::FkPlanSpec {
        database: database.to_owned(),
        table: table.to_owned(),
        updated_cols: Vec::new(),
        replace: insert.replace,
        on_duplicate_cols: insert
            .on_duplicate
            .iter()
            .filter_map(|assignment| assignment.col.last().cloned())
            .collect(),
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
    let (database, table) = super::catalog::split_table_path(&table_ref.name, current_db)?;
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
    let (database, table) = super::catalog::split_table_path(&table_ref.name, current_db)?;
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
        DriverError::Schema(crate::SchemaErrorKind::UnknownTable(format!(
            "{database}.{table_name}"
        )))
    })?;
    if table.is_view() {
        return Err(DriverError::InsertIntoViewUnsupported(table_name));
    }
    if table.is_sequence() {
        return Err(DriverError::InsertIntoSequenceUnsupported(table_name));
    }
    let TableEntry::Kv(kv) = table else {
        return Err(DriverError::Mysql(MysqlError::new(
            tidb_error::tidb::errcode::ErrUnsupportedOp,
            "operation not supported",
        )));
    };
    let mut column_list = table.column_list();
    let stored_width = column_list.len();
    let named_columns: Vec<String> = if insert.set_syntax {
        insert
            .set_columns
            .iter()
            .map(|path| path.last().cloned().unwrap_or_default())
            .collect()
    } else {
        insert.columns.clone()
    };
    // Go `initInsertColumns`: `_tidb_rowid` among the named columns is the
    // extra handle, which only `tidb_opt_write_row_id` admits writing. A
    // table with a real handle has no such column at all, so the name falls
    // through to the ordinary 1054 there -- which is what TiDB answers for
    // `insert s (a, _tidb_rowid) values (1, 2)` on a table with a primary
    // key.
    let extra_handle_offset = ((insert.set_syntax || insert.columns_specified)
        && crate::driver::from::extra_handle_column(table).is_some()
        && named_columns
            .iter()
            .any(|name| name.eq_ignore_ascii_case(tidb_model::column::EXTRA_HANDLE_NAME)))
    .then(|| {
        column_list.push((
            tidb_model::column::EXTRA_HANDLE_NAME.to_owned(),
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        ));
        stored_width
    });
    if extra_handle_offset.is_some() && !ctx.allow_write_row_id() {
        return Err(DriverError::unsupported(WRITE_ROW_ID_REFUSED));
    }
    let target_offsets = if insert.set_syntax || insert.columns_specified {
        named_columns
            .iter()
            .map(|name| {
                column_list
                    .iter()
                    .position(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                    .ok_or_else(|| DriverError::UnknownColumnInClause {
                        column: name.clone(),
                        clause: "field list".to_owned(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        (0..column_list.len()).collect()
    };
    let mut generated_targets: Vec<bool> = kv
        .visible_columns()
        .iter()
        .map(|column| column.generated.is_some())
        .collect();
    let mut column_meta = column_metadata(table);
    // The pseudo-column carries the same shape as a stored one so every
    // per-target lookup below stays indexed the same way. It is never
    // generated, and its "default" is the NULL that means "allocate a
    // handle" (Go `adjustImplicitRowID`'s `!hasValue` branch).
    if extra_handle_offset.is_some() {
        generated_targets.push(false);
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
    if insert.source.is_some() {
        for &offset in &target_offsets {
            if generated_targets[offset] {
                return Err(DriverError::BadGeneratedColumn {
                    column: column_list[offset].0.clone(),
                    table: table_name,
                });
            }
        }
    }
    // Go `initInsertColumns`' `table.CheckOnce`, after the planner's checks:
    // a column named twice, in the column list or the SET form.
    for (position, &offset) in target_offsets.iter().enumerate() {
        if target_offsets[..position].contains(&offset) {
            return Err(DriverError::DdlCoded {
                errno: 1110,
                message: format!("Column '{}' specified twice", column_list[offset].0),
            });
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

/// Go evaluates an uncorrelated subquery in a VALUES list or an ON
/// DUPLICATE KEY UPDATE assignment while it plans the INSERT
/// (`handleScalarSubquery`'s `EvalSubqueryFirstRow`): before any row is
/// written, and whether or not a row conflicts. One that reads the target's
/// columns stays in the statement, for the rewriter to refuse.
fn fold_insert_subqueries(
    insert: &tidb_ast::InsertStmt,
    layout: &InsertTargetLayout,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<tidb_ast::InsertStmt>, DriverError> {
    use super::subquery::{expr_has_subquery, fold_subqueries};
    let values = insert
        .rows
        .iter()
        .flatten()
        .chain(insert.on_duplicate.iter().map(|assignment| &assignment.value));
    if !values.clone().any(expr_has_subquery) {
        return Ok(None);
    }
    // Go rewrites each value against a dual; a subquery whose built plan
    // carries a correlated column anywhere stays an Apply, and the value
    // list refuses it (`buildValuesListOfInsert`, issue 30626).
    for value in values.clone() {
        for query in outermost_subqueries(value) {
            if super::planner_bridge::query_builds_correlated_columns(
                &query, catalog, current_db, ctx,
            )
            .unwrap_or(false)
            {
                return Err(DriverError::unsupported(
                    "Insert's SET operation or VALUES_LIST doesn't support complex subqueries now",
                ));
            }
        }
    }
    let scope = insert_table_scope(
        &layout.database,
        &layout.table_name,
        layout.column_list.clone(),
        ctx,
    );
    let mut folded = insert.clone();
    let values = folded
        .rows
        .iter_mut()
        .flatten()
        .chain(folded.on_duplicate.iter_mut().map(|assignment| &mut assignment.value));
    for value in values {
        if expr_has_subquery(value) {
            *value = fold_subqueries(value, &scope, catalog, current_db, ctx)?;
        }
    }
    Ok(Some(folded))
}

/// The subqueries `expr` contains, outermost only.
fn outermost_subqueries(expr: &tidb_ast::Expr) -> Vec<tidb_ast::QueryStmt> {
    struct Collector(Vec<tidb_ast::QueryStmt>);
    impl tidb_ast::Visitor for Collector {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_ref::<tidb_ast::Expr>() else {
                return false;
            };
            let query = match expr {
                tidb_ast::Expr::Subquery(query)
                | tidb_ast::Expr::Exists {
                    subquery: query, ..
                }
                | tidb_ast::Expr::InSubquery {
                    subquery: query, ..
                }
                | tidb_ast::Expr::CompareSubquery {
                    subquery: query, ..
                } => query,
                _ => return false,
            };
            self.0.push((**query).clone());
            true
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut collector = Collector(Vec::new());
    tidb_ast::Visitable::accept(&mut expr.clone(), &mut collector);
    collector.0
}

fn run_insert_with_physical(
    insert: &tidb_ast::InsertStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    physical_source: Option<&mut tidb_planner::physical::PhysicalPlan>,
    fk_triggers: &[FkTriggerNode],
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
) -> Result<(u64, Option<u64>), DriverError> {
    if insert.replace && !insert.on_duplicate.is_empty() {
        return Err(DriverError::unsupported("partitions are not supported yet"));
    }

    let target_layout = resolve_insert_target(insert, catalog, current_db, ctx)?;
    let folded_insert = fold_insert_subqueries(insert, &target_layout, catalog, current_db, ctx)?;
    let insert = folded_insert.as_ref().unwrap_or(insert);
    let eval_chunk = {
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        chunk
    };
    // Go `buildInsert`'s `Names4OnDuplicate` and `ResolveOnDuplicate`: the
    // assignment values resolve at plan time, conflict or not.
    let on_duplicate_scope = super::on_duplicate_scope::OnDuplicateScope::build(
        insert,
        catalog,
        current_db,
        &target_layout.column_list,
        &target_layout.database,
        &target_layout.table_name,
        &target_layout.target_offsets,
    );
    on_duplicate_scope.validate(&insert.on_duplicate)?;
    // The fields Go appended to the SELECT for the assignments, typed as the
    // source plan outputs them.
    let extra_types: Vec<FieldType> =
        match (physical_source.as_deref(), on_duplicate_scope.actual_col_len()) {
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
    let select_on_duplicate = if insert.source.is_some() {
        Some(prepare_on_duplicate_assignments(
            &insert.on_duplicate,
            &target_layout.column_list,
            &target_layout.column_meta,
            target_layout.extra_handle_offset,
            &target_layout.table_name,
            Some(&target_layout.database),
            &on_duplicate_scope,
            &extra_types,
            ctx,
            eval_chunk.get_row(0),
        )?)
    } else {
        None
    };

    // Once the target is valid, `INSERT ... SELECT` runs its source over the
    // pre-insert catalog and materializes those rows before the table is
    // borrowed mutably.
    let source_rows: Option<Vec<Vec<Datum>>> = match &insert.source {
        Some(_) => {
            let physical = physical_source.ok_or_else(|| {
                DriverError::unsupported("INSERT SELECT has no retained physical child")
            })?;
            let mut rows = Vec::new();
            let accountant = ctx
                .statement_memory()
                .write_accountant(mem_quota::label::INSERT);
            let collected = super::physical_builder::execute_dml_source(
                physical,
                catalog,
                ctx,
                runtime.is_some(),
                |row| {
                    accountant.account_row(&row).map_err(DriverError::from)?;
                    rows.push(row);
                    Ok(())
                },
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
    let source_extras: Vec<Vec<Datum>> = match (
        source_rows.as_mut(),
        on_duplicate_scope.actual_col_len(),
    ) {
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
    let TableEntry::Kv(kv) = catalog
        .get_mut_in(&database, &table_name)
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
            return Err(DriverError::PartitionClauseOnNonpartitioned);
        };
        Some(
            crate::partition_pruning::ids_for_selected_partitions(spec, &insert.partitions)
                .map_err(|partition| DriverError::UnknownPartition {
                    partition,
                    table: table_name.clone(),
                })?,
        )
    };

    let auto_increment_offset = kv.auto_increment_offset();
    let auto_random_offset = kv.auto_random().map(|spec| spec.offset);
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
        ctx.notify_before_executor_first_run();
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
    let bad_null_level = crate::bad_null::NullLevel::from_is_error(
        !ctx.ignore_err()
            && (ctx.strict() || insert.rows.len() == 1)
            && ctx.strict_not_null_check(),
    );

    enum PreparedInsertValue {
        Generated,
        Expression(Box<Expression>),
    }

    // Go lowers every explicit VALUES default while building the insert plan,
    // row-major and before an ordinary VALUES expression is evaluated. This
    // ordering is observable for computed defaults such as RAND(), and is
    // deliberately distinct from omitted columns, whose defaults are filled
    // later while each runtime row is built.
    let mut prepared_value_rows: Vec<Vec<PreparedInsertValue>> = Vec::new();
    let names_a_column = insert.set_syntax || insert.columns_specified;
    let mut previous_width = target_offsets.len();
    if source_rows.is_none() {
        let resolver = TableResolver {
            database: Some(&database),
            table_name: &table_name,
            columns: &column_list,
            constant_context: ctx.clone(),
            zone: ctx.session_zone(),
            no_unsigned_subtraction: ctx.no_unsigned_subtraction(),
            div_precision_increment: ctx.div_precision_increment(),
            clause_message: "field list",
        };
        for (index, values) in insert.rows.iter().enumerate() {
            let width = values.len();
            let expected = if index == 0 {
                target_offsets.len()
            } else {
                previous_width
            };
            let arity_is_checked = index > 0 || names_a_column || width > 0;
            if arity_is_checked && width != expected {
                // Go's planner check (planbuilder.go:4349/:4361): row 1 answers
                // to the column list, later rows to the first row's width --
                // both report `ErrWrongValueCountOnRow` with the 1-based row
                // number.
                return Err(DriverError::WrongValueCountOnRow { row: index + 1 });
            }
            previous_width = width;

            let mut prepared = Vec::with_capacity(width);
            for (position, value) in values.iter().enumerate() {
                let target_offset = target_offsets[position];
                let target = ResolvedDefaultColumn {
                    identity: DefaultColumnIdentity {
                        table: 0,
                        column: target_offset,
                    },
                    meta: column_meta[target_offset].clone(),
                };
                let resolve_root_default =
                    |path: &[String]| -> Result<ResolvedDefaultColumn, DriverError> {
                        let name = path
                            .last()
                            .ok_or(DriverError::unsupported("DEFAULT() needs a column"))?;
                        let column = column_meta
                            .iter()
                            .position(|meta| meta.name.eq_ignore_ascii_case(name))
                            .ok_or_else(|| DriverError::UnknownColumnInClause {
                                column: name.clone(),
                                clause: "field list".to_owned(),
                            })?;
                        Ok(ResolvedDefaultColumn {
                            identity: DefaultColumnIdentity { table: 0, column },
                            meta: column_meta[column].clone(),
                        })
                    };

                let expression = match value {
                    tidb_ast::Expr::Default(None) if target.meta.generated => {
                        prepared.push(PreparedInsertValue::Generated);
                        continue;
                    }
                    tidb_ast::Expr::Default(None) => {
                        let datum = materialize_column_default(
                            &target.meta,
                            DefaultUse::Insert,
                            ctx,
                            eval_chunk.get_row(0),
                        )?;
                        Expression::Constant(tidb_expr::constant::Constant::new(
                            datum,
                            target.meta.field_type,
                        ))
                    }
                    tidb_ast::Expr::Default(Some(path)) => {
                        let source = resolve_root_default(path)?;
                        if target.identity != source.identity
                            && (target.meta.generated || source.meta.generated)
                        {
                            return Err(DriverError::BadGeneratedColumn {
                                column: target.meta.name,
                                table: table_name.clone(),
                            });
                        }
                        if target.meta.generated {
                            prepared.push(PreparedInsertValue::Generated);
                            continue;
                        }
                        let datum = materialize_column_default(
                            &source.meta,
                            DefaultUse::Insert,
                            ctx,
                            eval_chunk.get_row(0),
                        )?;
                        Expression::Constant(tidb_expr::constant::Constant::new(
                            datum,
                            source.meta.field_type,
                        ))
                    }
                    _ if target.meta.generated => {
                        return Err(DriverError::BadGeneratedColumn {
                            column: target.meta.name,
                            table: table_name.clone(),
                        });
                    }
                    _ => {
                        let defaults = prepare_named_defaults(
                            value,
                            ctx,
                            eval_chunk.get_row(0),
                            DefaultUse::Expression,
                            |path| {
                                let (column, _, _) = resolver.resolve(path).ok_or_else(|| {
                                    DriverError::UnknownColumnInClause {
                                        column: path.last().cloned().unwrap_or_default(),
                                        clause: "field list".to_owned(),
                                    }
                                })?;
                                Ok(ResolvedDefaultColumn {
                                    identity: DefaultColumnIdentity { table: 0, column },
                                    meta: column_meta[column].clone(),
                                })
                            },
                        )?;
                        rewrite_with_prepared_defaults(value, &resolver, &defaults)?
                    }
                };
                prepared.push(PreparedInsertValue::Expression(Box::new(expression)));
            }
            prepared_value_rows.push(prepared);
        }
    } else {
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
                return Err(DriverError::WrongValueCountOnRow { row: 1 });
            }
            previous_width = width;
        }
    }

    // Go resolves ON DUPLICATE after the VALUES lists and before execution.
    // Keeping that placement makes generated-column and DEFAULT errors visible
    // even when no candidate row ends up conflicting.
    let on_duplicate_assignments = match select_on_duplicate {
        Some(prepared) => prepared,
        None => prepare_on_duplicate_assignments(
            &insert.on_duplicate,
            &column_list,
            &column_meta,
            extra_handle_offset,
            &table_name,
            Some(&database),
            &on_duplicate_scope,
            &extra_types,
            ctx,
            eval_chunk.get_row(0),
        )?,
    };
    let table_types = column_list.iter().map(|(_, field_type)| field_type.clone());
    let prepared_on_duplicate = PreparedOnDuplicate {
        on_update_now: PreparedOnUpdateNow::new(
            &column_meta,
            on_duplicate_assignments
                .iter()
                .map(|assignment| assignment.offset),
        )?,
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
        let field_types: Vec<FieldType> =
            column_meta.iter().map(|meta| meta.field_type.clone()).collect();
        tidb_chunk::mutrow::MutRow::from_types(&field_types)
    });
    let mut new_rows: Vec<Vec<Datum>> = Vec::with_capacity(row_count);
    // Go `buildValuesListOfInsert` checks arity in two steps: the FIRST row
    // against the target columns, and every later row against the one before
    // it. The first check is skipped when both the column list and the first
    // value list are empty, which is what makes `INSERT t VALUES ()` -- a row
    // of nothing but defaults -- legal; chaining the later rows to their
    // predecessor is then what still rejects `VALUES (), (1)`. An empty row
    // assigns nothing, so the default, auto-increment and NOT NULL rules
    // below fill the whole row.
    for index in 0..row_count {
        let width = match source_rows.as_ref() {
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
                Some(_) => value_rows[index][position].clone(),
                None => match &prepared_value_rows[index][position] {
                    PreparedInsertValue::Generated => continue,
                    PreparedInsertValue::Expression(expression) => {
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
            let value = cast_value_for_column(
                value,
                &column_meta[offset].field_type,
                column_meta[offset].name.as_str(),
                new_rows.len(),
                ctx,
                insert.ignore,
            )?;
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
        if let Some(offset) = auto_increment_offset {
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
        if let Some(offset) = auto_random_offset {
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
        // Only a column the statement omits takes its default, and only such
        // a column can raise ErrNoDefaultForField (Go `fillColValue`).
        for offset in 0..column_list.len() {
            if !assigned[offset] {
                row[offset] = column_default(&column_meta, offset, ctx, eval_chunk.get_row(0))?;
            }
        }
        // Go casts each value to its column's type BEFORE the row is
        // written, which is what rounds a decimal to the column's scale and
        // parses a numeric string. The statement's values were cast above;
        // this pass casts what the row filled in (defaults and the
        // auto-increment marker). It runs before `HandleBadNull`: go's row
        // build emits the truncation warnings (1406) before the constraint
        // warnings (1048), so `INSERT IGNORE INTO t VALUES (NULL, 'abc')`
        // warns 1406-for-b and only then 1048-for-a (oracle-captured order).
        {
            for (offset, value) in row.iter_mut().enumerate() {
                if cast_done[offset] {
                    continue;
                }
                // The row is one wider than the table when the statement
                // wrote `_tidb_rowid`. Go gives that slot a synthetic
                // `table.Column` built from `NewExtraHandleColInfo()`
                // (`fillRow`) so every per-column step has an entry; the cast
                // it performs is `setDatumAutoIDAndCast` against that
                // column's `TypeLonglong`.
                // SQL input metadata includes the extra handle after visible
                // columns; its offset may overlap the table's hidden tail.
                let (field_type, name) = (
                    &column_meta[offset].field_type,
                    column_meta[offset].name.as_str(),
                );
                *value = cast_value_for_column(
                    std::mem::replace(value, Datum::Null),
                    field_type,
                    name,
                    new_rows.len(),
                    ctx,
                    insert.ignore,
                )?;
            }
        }
        // Go `Column.HandleBadNull`: an explicit NULL in a NOT NULL column is
        // ErrColumnCantNull, which is a different error from omitting a
        // column that has no default. Whether it FAILS the statement is
        // `bad_null_level` above, not the SQL mode alone.
        for (offset, value) in row.iter_mut().enumerate() {
            // A generated column's value is not built yet at this point, so
            // the NULL standing in for it is not the user's NULL.
            if assigned[offset] && !generated_targets[offset] {
                crate::bad_null::handle_bad_null(
                    value,
                    &column_meta[offset].field_type,
                    &column_list[offset].0,
                    bad_null_level,
                    ctx,
                )?;
            }
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
            )?;
            if let Some(handle) = extra_handle {
                row.push(handle);
            }
        }
        new_rows.push(row);
    }
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
                        AutoRandomError::ExplicitInsertDisabled => DriverError::InvalidAutoRandom(
                            "Explicit insertion on auto_random column is disabled. Try to set @@allow_auto_random_explicit_insert = true."
                                .to_owned(),
                        ),
                        AutoRandomError::AutoId(AutoIdError::Exhausted) => {
                            DriverError::AutoRandReadFailed
                        }
                        AutoRandomError::AutoId(AutoIdError::OutOfRange {
                            value,
                            type_name,
                        }) => DriverError::ConstantOverflows { value, type_name },
                        AutoRandomError::AutoId(AutoIdError::Store(detail)) => {
                            DriverError::AutoIdUnavailable(detail.0)
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
                let outcome = match kv.apply_auto_increment_in(
                    &mut new_rows[*index],
                    ctx.auto_increment_step(),
                    || ctx.reuse_auto_increment_id(),
                    &crate::kv_table::AutoIdCall::statement(&ctx.statement_memory()),
                ) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        if let Err(killed) = ctx.statement_memory().check() {
                            return Err(DriverError::Exec(killed));
                        }
                        match error {
                            AutoIdError::Exhausted => return Err(DriverError::AutoincReadFailed),
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
                                    return Err(DriverError::ConstantOverflows { value, type_name });
                                }
                                ctx.append_warning_parts(
                                    1690,
                                    &format!("constant {value} overflows {type_name}"),
                                );
                                if !has_on_duplicate {
                                    if !insert.ignore {
                                        return Err(DriverError::AutoincReadFailed);
                                    }
                                    ctx.append_warning_parts(
                                        1467,
                                        "Failed to read auto-increment value from storage engine",
                                    );
                                    return Ok((0, None));
                                }
                                kv.clamp_auto_increment(&mut new_rows[*index]);
                                AutoIncrement::Allocated(value.parse::<i64>().unwrap_or_else(
                                    |_| value.parse::<u64>().map_or(0, |id| id as i64),
                                ))
                            }
                            AutoIdError::Store(detail) => {
                                return Err(DriverError::AutoIdUnavailable(detail.0));
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
    ) -> &'a mut crate::kv_table::KvTable {
        match catalog.get_mut_in(database, table_name) {
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
    let mut updates = UpdateRecords::new(fk_triggers);
    let mut removals = DeleteRecords::new(fk_triggers);
    let mut inserted_rows = Vec::new();
    for (position, candidate) in new_rows.iter().enumerate() {
        // Generated columns belong to the writable row; the extra handle is
        // candidate identity and must not affect row equality or FK callbacks.
        let (row, written_row_id): (&[Datum], Option<i64>) = match extra_handle_offset {
            Some(offset) => (
                &candidate[..offset],
                candidate
                    .get(offset)
                    .and_then(|value| written_row_id(value, ctx)),
            ),
            None => (candidate.as_slice(), None),
        };
        // Go's partition-qualified INSERT target is a table wrapper, so the
        // completed candidate is routed through the selected partition set
        // before duplicate-key resolution.  This prevents a row for p0 from
        // finding and updating a conflicting row when the statement names p1.
        if let Some(partitions) = &insert_partition_ids {
            if let Err(error) = target(catalog, &database, &table_name)
                .validate_insert_partitions(row, partitions, ctx)
            {
                handle_partition_write_error(kv_write_error(error), insert.ignore, ctx)?;
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
                    handle_partition_write_error(kv_write_error(error), insert.ignore, ctx)?;
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
                        inserted += 1;
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
                    inserted += 1;
                }
                if unchanged {
                    continue;
                }
            } else if has_on_duplicate {
                let sql_candidate;
                let candidate_values = if let Some(offset) = extra_handle_offset {
                    sql_candidate = candidate[..column_list.len() - 1]
                        .iter()
                        .chain(std::iter::once(&candidate[offset]))
                        .cloned()
                        .collect::<Vec<_>>();
                    sql_candidate.as_slice()
                } else {
                    row
                };
                inserted += apply_on_duplicate(
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
                continue;
            } else if insert.ignore {
                let conflict = conflicts.into_iter().next().expect("a conflict was found");
                let warning = kv_write_error(conflict.error).to_mysql_error();
                ctx.append_warning_parts(warning.code, &warning.message);
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
                if matches!(error, DriverError::ForeignKeyNoReferencedRow { .. }) {
                    let warning = error.to_mysql_error();
                    ctx.append_warning_parts(warning.code, &warning.message);
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
                if !insert.ignore
                    && crate::foreign_key::has_triggers(fk_triggers, &database, &table_name)
                {
                    inserted_rows.push(position);
                }
            }
            Err(error) => {
                let rendered = kv_write_error(error);
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
pub(crate) fn kv_write_error(error: crate::kv_table::KvTableError) -> DriverError {
    match error {
        crate::kv_table::KvTableError::Storage(error) => error.into(),
        crate::kv_table::KvTableError::ColumnCast(error) => {
            DriverError::Exec(crate::ExecError::Mysql(error))
        }
        crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
            DriverError::DuplicateEntry { value, key }
        }
        crate::kv_table::KvTableError::Generation {
            eval: Some(eval), ..
        } => DriverError::Exec(crate::ExecError::Eval(eval)),
        crate::kv_table::KvTableError::CheckConstraintViolated(name) => {
            DriverError::CheckConstraintViolated(name)
        }
        crate::kv_table::KvTableError::CheckConstraint {
            eval: Some(eval), ..
        } => DriverError::Exec(crate::ExecError::Eval(eval)),
        // A RANGE table with no `MAXVALUE` partition rejects the row rather
        // than storing it somewhere; 1526 is the code an application sees.
        crate::kv_table::KvTableError::NoPartitionForValue(value) => {
            DriverError::NoPartitionForValue(value)
        }
        crate::kv_table::KvTableError::RowDoesNotMatchGivenPartitionSet => {
            DriverError::RowDoesNotMatchGivenPartitionSet
        }
        // A HASH partition value with no signed reading is Go's own
        // `ConvertTo` error surfacing out of `locateHashPartition`: 1690,
        // naming the value and `bigint`, the type it did not fit.
        crate::kv_table::KvTableError::PartitionValueOverflowsBigint(value) => {
            DriverError::ConstantOverflows {
                value,
                type_name: "bigint".to_owned(),
            }
        }
        // The `_tidb_rowid` a non-clustered row needs comes off the same
        // counter the AUTO_INCREMENT column does, so its exhaustion is the
        // same 1467 an allocated column value would have reported.
        crate::kv_table::KvTableError::AutoIdExhausted => DriverError::AutoincReadFailed,
        other => DriverError::Parse(format!("row encode failed: {other:?}")),
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
        error @ (DriverError::NoPartitionForValue(_)
        | DriverError::RowDoesNotMatchGivenPartitionSet
        // Go's `batchCheckAndInsert` (the IGNORE path) downgrades a violated
        // CHECK to a warning and skips the row, exactly like a duplicate key
        // (`insert_common.go:1364-1370`); a plain insert still fails the
        // statement from `addRecord`.
        | DriverError::CheckConstraintViolated(_))
            if ignore =>
        {
            let warning = error.to_mysql_error();
            ctx.append_warning_parts(warning.code, &warning.message);
            Ok(())
        }
        error => Err(error),
    }
}

/// Renders a table read failure while preserving the storage layer's
/// retryable-region signal as a transaction error instead of a parse error.
pub(crate) fn kv_read_error(operation: &str, error: crate::kv_table::KvTableError) -> DriverError {
    match error {
        crate::kv_table::KvTableError::Storage(error) => error.into(),
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
    /// Built over Go's `Schema4OnDuplicate` (see
    /// [`super::on_duplicate_scope::OnDuplicateResolver`]).
    value: Expression,
}

struct PreparedOnDuplicate {
    extra_handle: bool,
    assignments: Vec<PreparedOnDuplicateAssignment>,
    on_update_now: PreparedOnUpdateNow,
    selected_partitions: Option<Vec<i64>>,
    /// How many fields Go appended to the SELECT for the assignments.
    extra_len: usize,
    /// The types of Go's `row4Update`: stored row, appended fields, would-be
    /// row.
    row_types: Vec<FieldType>,
}

/// Go `ResolveOnDuplicate`: every assignment is rewritten once, at plan
/// time, whether or not an inserted row eventually conflicts, its DEFAULT
/// leaves already typed statement constants.
#[allow(clippy::too_many_arguments)]
fn prepare_on_duplicate_assignments(
    assignments: &[tidb_ast::Assignment],
    column_list: &[(String, FieldType)],
    column_meta: &[ColumnDefaultMeta],
    extra_handle_offset: Option<usize>,
    table_name: &str,
    database: Option<&str>,
    scope: &super::on_duplicate_scope::OnDuplicateScope,
    extra_types: &[FieldType],
    ctx: &crate::StmtContext,
    row: tidb_chunk::row::Row<'_>,
) -> Result<Vec<PreparedOnDuplicateAssignment>, DriverError> {
    let resolver = TableResolver {
        database: database,
        table_name,
        columns: column_list,
        constant_context: ctx.clone(),
        zone: ctx.session_zone(),
        no_unsigned_subtraction: ctx.no_unsigned_subtraction(),
        div_precision_increment: ctx.div_precision_increment(),
        clause_message: "field list",
    };
    let value_resolver = super::on_duplicate_scope::OnDuplicateResolver {
        base: TableResolver {
            database: database,
            table_name,
            columns: column_list,
            constant_context: ctx.clone(),
            zone: ctx.session_zone(),
            no_unsigned_subtraction: ctx.no_unsigned_subtraction(),
            div_precision_increment: ctx.div_precision_increment(),
            clause_message: "field list",
        },
        scope,
        extra_types,
    };
    let mut prepared = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let (offset, _, _) = resolver.resolve(&assignment.col).ok_or_else(|| {
            DriverError::UnknownColumnInClause {
                column: assignment.col.last().cloned().unwrap_or_default(),
                clause: "field list".to_owned(),
            }
        })?;
        if extra_handle_offset == Some(offset) {
            return Err(DriverError::unsupported(
                "UPDATE of _tidb_rowid moves the row, which is not supported yet",
            ));
        }
        let target_identity = DefaultColumnIdentity {
            table: 0,
            column: offset,
        };
        let target_meta = &column_meta[offset];

        if target_meta.generated {
            let is_own_default = match &assignment.value {
                tidb_ast::Expr::Default(None) => true,
                tidb_ast::Expr::Default(Some(path)) => {
                    resolver.resolve(path).is_some_and(|(column, _, _)| {
                        target_identity == (DefaultColumnIdentity { table: 0, column })
                    })
                }
                _ => false,
            };
            if is_own_default {
                continue;
            }
            return Err(DriverError::BadGeneratedColumn {
                column: target_meta.name.clone(),
                table: table_name.to_owned(),
            });
        }

        let value = match &assignment.value {
            tidb_ast::Expr::Default(None) => {
                let datum =
                    materialize_column_default(target_meta, DefaultUse::Expression, ctx, row)?;
                Expression::Constant(tidb_expr::constant::Constant::new(
                    datum,
                    target_meta.field_type.clone(),
                ))
            }
            value => {
                let defaults =
                    prepare_named_defaults(value, ctx, row, DefaultUse::Expression, |path| {
                        let (column, _, _) = resolver.resolve(path).ok_or_else(|| {
                            DriverError::UnknownColumnInClause {
                                column: path.last().cloned().unwrap_or_default(),
                                clause: "field list".to_owned(),
                            }
                        })?;
                        Ok(ResolvedDefaultColumn {
                            identity: DefaultColumnIdentity { table: 0, column },
                            meta: column_meta[column].clone(),
                        })
                    })?;
                rewrite_with_prepared_defaults(value, &value_resolver, &defaults)?
            }
        };
        prepared.push(PreparedOnDuplicateAssignment { offset, value });
    }
    Ok(prepared)
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
    handle: &crate::kv_table::TableHandle,
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
) -> Result<u64, DriverError> {
    let Some(TableEntry::Kv(table)) = catalog.get_mut_in(database, target_table_name) else {
        unreachable!("ODKU targets a byte-backed table")
    };
    let Some(existing) = std::sync::Arc::make_mut(table)
        .get_row_by_handle(handle, &ctx.session_zone())
        .map_err(|e| kv_read_error("row read failed", e))?
    else {
        return Ok(0);
    };
    let field_types: Vec<FieldType> = column_list.iter().map(|(_, ft)| ft.clone()).collect();
    let extra_handle = prepared.extra_handle;
    let sql_row = |row: &[Datum]| {
        let visible = column_list.len() - usize::from(extra_handle);
        let mut values = row[..visible].to_vec();
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
            updated[assignment.offset] = cast_value_for_assignment(
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
    Ok(if outcome.changed() {
        2
    } else {
        u64::from(outcome.unchanged() && ctx.client_found_rows())
    })
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
    physical_plan: Option<&mut tidb_planner::physical::PhysicalPlan>,
) -> Result<u64, DriverError> {
    run_update_stmt_with_physical_and_stats(update, catalog, current_db, ctx, physical_plan, None)
}

pub(crate) fn run_update_stmt_with_physical_and_stats(
    update: &tidb_ast::UpdateStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    physical_plan: Option<&mut tidb_planner::physical::PhysicalPlan>,
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
) -> Result<u64, DriverError> {
    let source = update_source_query(update);
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
    let mut physical_plan = physical_plan.or(fresh.as_mut());
    if let Some(plan) = physical_plan.as_deref_mut() {
        super::physical_builder::prepare_execution_plan(plan, catalog, ctx)?;
        ctx.publish_physical_process_info(plan, catalog);
    }
    let (physical_source, update_expressions, fk_triggers) = match physical_plan {
        Some(plan) => {
            let tidb_planner::physical::PhysicalPlan::Dml(root) = plan else {
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
                root.fk_triggers.as_slice(),
            )
        }
        None => (None, None, &[][..]),
    };
    run_update_with_physical(
        update,
        catalog,
        current_db,
        ctx,
        physical_source,
        update_expressions,
        fk_triggers,
        runtime,
    )
}

///
/// The SELECT carries the statement's hints, as Go's `tryUpdatePointPlan`
/// builds its `SelectStmt{TableHints: updateStmt.TableHints}` and
/// `buildUpdate` pushes them for the read.
pub(crate) fn update_source_query(update: &tidb_ast::UpdateStmt) -> Option<tidb_ast::QueryStmt> {
    match &update.kind {
        tidb_ast::UpdateKind::Single(table_ref) => super::access::PointPlanStmt::of_write(
            update.where_clause.as_ref(),
            &update.order_by,
            update.limit.as_ref(),
            table_ref,
        )
        .write_select(),
        tidb_ast::UpdateKind::Multi { .. } => None,
    }
    .map(|mut select| {
        select.hints.clone_from(&update.hints);
        tidb_ast::QueryStmt::Select(Box::new(select))
    })
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

fn update_assignment_values_for_plan(
    update: &tidb_ast::UpdateStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<Option<tidb_ast::Expr>>, DriverError> {
    let tidb_ast::UpdateKind::Single(table_ref) = &update.kind else {
        return Ok(Vec::new());
    };
    let (database, name) = single_table_name(table_ref, current_db)?;
    let table = catalog.get_in(&database, &name).ok_or_else(|| {
        DriverError::Schema(crate::SchemaErrorKind::UnknownTable(format!(
            "{database}.{name}"
        )))
    })?;
    let mut columns = table.column_list();
    if let Some(column) = crate::driver::from::extra_handle_column(table) {
        columns.push(column);
    }
    let scope = dml_table_scope(table_ref, &database, &name, columns, ctx);
    update
        .assignments
        .iter()
        .map(|assignment| {
            let folded = super::subquery::fold_subqueries(
                &assignment.value,
                &scope,
                catalog,
                current_db,
                ctx,
            )?;
            Ok(super::subquery::expr_has_subquery(&folded).then_some(folded))
        })
        .collect()
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
    physical: tidb_planner::physical::PhysicalPlan,
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
    ) -> Option<(&Stmt, &mut tidb_planner::physical::PhysicalPlan)> {
        (self.generation == generation).then_some((&self.statement, &mut self.physical))
    }
}

/// One bound execution rebuilt from a retained prepared DML definition.
#[derive(Debug)]
pub struct PreparedDmlExecution {
    parameters: Arc<[Datum]>,
    planning_warnings: std::sync::Mutex<Vec<(crate::WarnLevel, u16, String)>>,
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
        environment: &PreparedPlanCacheEnvironment,
        statement: &Stmt,
    ) -> Option<PreparedDmlExecution> {
        self.bind_inner(
            cache,
            params,
            catalog,
            current_database,
            Some(ctx),
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
        environment: &PreparedPlanCacheEnvironment,
        statement: &Stmt,
    ) -> Option<PreparedDmlExecution> {
        self.bind_inner(
            cache,
            params,
            catalog,
            current_database,
            None,
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
        if !environment.hashes_fresh_statistics() {
            return 0;
        }
        self.stats_table_keys.iter().fold(0, |hash, key| {
            let version = match catalog.get_by_key(key) {
                Some(TableEntry::Kv(table)) => catalog
                    .table_statistics(table.stats_physical_id())
                    .map_or(0, |statistics| statistics.version),
                _ => 0,
            };
            hash.wrapping_add(version)
        })
    }
}

impl PreparedDmlExecution {
    /// Values owned by this execution, independent of later cache rebuilds.
    #[must_use]
    pub fn parameters(&self) -> Arc<[Datum]> {
        Arc::clone(&self.parameters)
    }
    /// Takes warnings produced by this execution's cache-miss planning once.
    pub fn take_planning_warnings(&self) -> Vec<(crate::WarnLevel, u16, String)> {
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
        callback: impl FnOnce(&Stmt, &mut tidb_planner::physical::PhysicalPlan) -> R,
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
) -> Option<(tidb_planner::physical::PhysicalPlan, bool)> {
    let Stmt::Dml(dml) = statement else {
        return None;
    };
    let (operator, source, update) = match dml.as_ref() {
        tidb_ast::DmlStmt::Insert(insert) => {
            let source = insert_plan_source(insert, catalog, current_database)
                .or_else(|| insert.source.as_deref().cloned());
            ("Insert", source, None)
        }
        tidb_ast::DmlStmt::Update(update) => {
            ("Update", Some(update_source_query(update)?), Some(update.as_ref()))
        }
        tidb_ast::DmlStmt::Delete(delete) => ("Delete", Some(delete_source_query(delete)?), None),
        _ => return None,
    };
    let fk_spec = match dml.as_ref() {
        tidb_ast::DmlStmt::Insert(insert) => fk_spec_for_insert(insert, current_database),
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

fn promote_cached_dml_point_source(plan: &mut tidb_planner::physical::PhysicalPlan) {
    use tidb_planner::physical::{PhysicalPlan, PhysicalPointGet};
    use tidb_planner::physical_plan_cache::PointRangeRebuild;

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

fn physical_plan_contains_point_get(plan: &tidb_planner::physical::PhysicalPlan) -> bool {
    use tidb_planner::physical::PhysicalPlan;

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
    physical_source: Option<&mut tidb_planner::physical::PhysicalPlan>,
    planned_update_expressions: Option<&[Option<Expression>]>,
    fk_triggers: &[FkTriggerNode],
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
        tidb_ast::UpdateKind::Multi { from, .. } => {
            return super::multi_dml::run_multi_update(
                update,
                from,
                catalog,
                current_db,
                ctx,
                physical_source,
                fk_triggers,
                runtime,
            );
        }
    };
    let (database, name) = single_table_name(table_ref, current_db)?;
    let table = catalog.get_in(&database, &name).ok_or_else(|| {
        DriverError::Schema(crate::SchemaErrorKind::UnknownTable(format!(
            "{database}.{name}"
        )))
    })?;
    if !table_ref.partitions.is_empty() && !matches!(table, TableEntry::Kv(_)) {
        return Err(DriverError::UnknownPartition {
            partition: table_ref.partitions[0].clone(),
            table: name.clone(),
        });
    }
    let physical_kv_source = matches!(table, TableEntry::Kv(_));
    match table {
        TableEntry::View(_) => return Err(DriverError::TableNotUpdatable(name.clone())),
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

    // An alias REPLACES the table name as the only usable qualifier, in the
    // SET list as much as the WHERE: `UPDATE u AS x SET x.v = 1` resolves and
    // `SET u.v = 1` is Go's unknown-column error. Resolving both sides through
    // the one resolver is what makes those two cases the same case.
    let resolver = TableResolver {
        database: Some(&database),
        table_name: table_ref.alias.as_deref().unwrap_or(&name),
        columns: &column_list,
        constant_context: ctx.clone(),
        zone: ctx.session_zone(),
        no_unsigned_subtraction: ctx.no_unsigned_subtraction(),
        div_precision_increment: ctx.div_precision_increment(),
        clause_message: "field list",
    };
    let mut assignments = Vec::with_capacity(update.assignments.len());
    for (assignment_index, assignment) in update.assignments.iter().enumerate() {
        let (offset, _, _) = resolver.resolve(&assignment.col).ok_or_else(|| {
            // go `getColumns` answers the assignment's unknown target with
            // the ordinary `ErrBadField` (1054) 'field list' form.
            DriverError::UnknownColumnInClause {
                column: assignment.col.join("."),
                clause: "field list".to_owned(),
            }
        })?;
        if extra_handle_slot == Some(offset) && !ctx.allow_write_row_id() {
            return Err(DriverError::unsupported(WRITE_ROW_ID_REFUSED));
        }
        // Go `IsDefaultExprSameColumn`: a generated target accepts only bare
        // DEFAULT or DEFAULT(the same resolved column). DEFAULT(other), even
        // though it has the same AST variant, is error 3105.
        if column_meta.get(offset).is_some_and(|meta| meta.generated) {
            let own_default = match &assignment.value {
                tidb_ast::Expr::Default(None) => true,
                tidb_ast::Expr::Default(Some(path)) => resolver
                    .resolve(path)
                    .is_some_and(|(source, _, _)| source == offset),
                _ => false,
            };
            if !own_default {
                return Err(DriverError::BadGeneratedColumn {
                    column: column_meta[offset].name.clone(),
                    table: name.clone(),
                });
            }
            // The generation expression remains the value source.
            continue;
        }
        assignments.push((assignment_index, offset, assignment.value.clone()));
    }
    let dml_scope = dml_table_scope(table_ref, &database, &name, column_list.clone(), ctx);
    let default_row = {
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        chunk
    };
    let mut set_exprs = Vec::with_capacity(assignments.len());
    for (assignment_index, offset, value) in &assignments {
        let planned = planned_update_expressions
            .and_then(|expressions| expressions.get(*assignment_index))
            .and_then(Option::as_ref);
        let expression = if let Some(planned) = planned {
            UpdateExpression::physical(planned.clone())
        } else {
            match value {
                // Go fills a bare update DEFAULT with the target column name and
                // resolves it through GetColDefaultValue while building the
                // assignment. Do the same once here, before row iteration, so a
                // computed default and any warning are statement-scoped.
                tidb_ast::Expr::Default(None) => {
                    let value = materialize_column_default(
                        column_meta.get(*offset).ok_or_else(|| {
                            DriverError::UnknownColumnInClause {
                                column: column_list[*offset].0.clone(),
                                clause: "field_list".to_owned(),
                            }
                        })?,
                        DefaultUse::Expression,
                        ctx,
                        default_row.get_row(0),
                    )?;
                    UpdateExpression::scalar(Expression::Constant(
                        tidb_expr::constant::Constant::new(value, column_list[*offset].1.clone()),
                    ))
                }
                _ => {
                    let defaults = prepare_named_defaults(
                        value,
                        ctx,
                        default_row.get_row(0),
                        DefaultUse::Expression,
                        |path| {
                            let (column, _, _) = resolver.resolve(path).ok_or_else(|| {
                                DriverError::UnknownColumnInClause {
                                    column: path.last().cloned().unwrap_or_default(),
                                    clause: "field list".to_owned(),
                                }
                            })?;
                            Ok(ResolvedDefaultColumn {
                                identity: DefaultColumnIdentity { table: 0, column },
                                meta: column_meta.get(column).cloned().ok_or_else(|| {
                                    DriverError::UnknownColumnInClause {
                                        column: column_list[column].0.clone(),
                                        clause: "field_list".to_owned(),
                                    }
                                })?,
                            })
                        },
                    )?;
                    if expr_has_subquery(value) {
                        UpdateExpression::applied(DmlExpression::build_with_prepared_defaults(
                            value,
                            dml_scope.clone(),
                            catalog,
                            current_db,
                            ctx,
                            &defaults,
                        )?)
                    } else {
                        UpdateExpression::scalar(rewrite_with_prepared_defaults(
                            value, &resolver, &defaults,
                        )?)
                    }
                }
            }
        };
        set_exprs.push((*offset, expression));
    }
    let on_update_now =
        PreparedOnUpdateNow::new(&column_meta, set_exprs.iter().map(|(offset, _)| *offset))?;

    let field_types: Vec<FieldType> = column_list.iter().map(|(_, ft)| ft.clone()).collect();
    let column_names: Vec<String> = column_list.iter().map(|(name, _)| name.clone()).collect();
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
        super::access::try_fast_dml_point_physical_plan_with_allocator(
            select,
            catalog,
            current_db,
            ctx,
            &tidb_planner::plan_base::PlanIdAllocator::new(),
        )
        .ok()
        .flatten()
    });
    let mut physical_source = fast_point_source.as_mut().or(physical_source);
    let partition_ids = match catalog.get_in(&database, &name) {
        Some(TableEntry::Kv(kv)) if table_ref.partitions.is_empty() => None,
        Some(TableEntry::Kv(kv)) => {
            let Some(spec) = kv.partition() else {
                return Err(DriverError::UnknownPartition {
                    partition: table_ref.partitions[0].clone(),
                    table: name.clone(),
                });
            };
            Some(
                crate::partition_pruning::ids_for_selected_partitions(spec, &table_ref.partitions)
                    .map_err(|partition| DriverError::UnknownPartition {
                        partition,
                        table: name.clone(),
                    })?,
            )
        }
        _ => None,
    };
    let physical = physical_source
        .as_deref_mut()
        .ok_or_else(|| DriverError::unsupported("UPDATE has no retained physical child"))?;
    let source_rows = execute_physical_write_rows(
        physical,
        catalog,
        &database,
        &name,
        ctx,
        runtime,
        mem_quota::label::UPDATE,
    )?;
    let mut matched = 0u64;
    let mut touched = 0u64;
    let mut changed = 0u64;
    let mut rewrites = Vec::new();
    let mut records = UpdateRecords::new(fk_triggers);
    let row_evaluator = UpdateRowEvaluator {
        field_types: &field_types,
        column_names: &column_names,
        set_exprs: &set_exprs,
        catalog,
        current_db,
        ctx,
        on_update_now: &on_update_now,
        extra_handle: extra_handle_slot.is_some(),
    };
    let accountant = ctx
        .statement_memory()
        .write_accountant(mem_quota::label::UPDATE);
    let PhysicalWriteRows {
        rows,
        field_types: physical_field_types,
    } = source_rows;
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
    let writes_stored_columns = set_exprs
        .iter()
        .any(|(offset, _)| Some(*offset) != extra_handle_slot);
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
    field_types: &'a [FieldType],
    column_names: &'a [String],
    set_exprs: &'a [(usize, UpdateExpression)],
    catalog: &'a Catalog,
    current_db: &'a str,
    ctx: &'a crate::StmtContext,
    on_update_now: &'a PreparedOnUpdateNow,
    /// Whether `field_types` ends in Go's extra handle column, so the row
    /// this evaluates has one more slot than the row it stages.
    extra_handle: bool,
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
        let visible = self.field_types.len() - usize::from(self.extra_handle);
        let mut evaluated = row[..visible].to_vec();
        if self.extra_handle {
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
            let value = expr.eval(
                sql_row,
                chunk.get_row(0),
                physical_row,
                self.catalog,
                self.current_db,
                self.ctx,
            )?;
            // ExtraHandleID has no column metadata in Go composeNewRow:
            // evaluate it, but neither cast it nor put it in the writable row.
            if self.extra_handle && *offset == visible {
                continue;
            }
            new_row[*offset] = cast_value_for_update_assignment(
                value,
                &self.field_types[*offset],
                &self.column_names[*offset],
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
    physical_plan: Option<&mut tidb_planner::physical::PhysicalPlan>,
) -> Result<u64, DriverError> {
    run_delete_stmt_with_physical_and_stats(delete, catalog, current_db, ctx, physical_plan, None)
}

pub(crate) fn run_delete_stmt_with_physical_and_stats(
    delete: &tidb_ast::DeleteStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    physical_plan: Option<&mut tidb_planner::physical::PhysicalPlan>,
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
) -> Result<u64, DriverError> {
    // Go's `PlanBuilder.buildDelete` resolves the target and refuses a view or
    // sequence before it builds the read child.  A sequence is not a row
    // source, so letting the physical planner inspect it first would turn the
    // intended plain 1105 refusal into a misleading 1146 unknown-table error.
    // Keep this preflight limited to known read-only objects; ordinary table
    // and missing-table ordering remains owned by the existing planner path.
    if let tidb_ast::DeleteKind::Single(table_ref) = &delete.kind {
        let (database, name) = single_table_name(table_ref, current_db)?;
        match catalog.get_in(&database, &name) {
            Some(TableEntry::View(_)) => {
                return Err(DriverError::DeleteViewUnsupported(name));
            }
            Some(TableEntry::Sequence(_)) => {
                return Err(DriverError::DeleteSequenceUnsupported(name));
            }
            _ => {}
        }
    }
    let source = delete_source_query(delete);
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
    let mut physical_plan = physical_plan.or(fresh.as_mut());
    if let Some(plan) = physical_plan.as_deref_mut() {
        super::physical_builder::prepare_execution_plan(plan, catalog, ctx)?;
        ctx.publish_physical_process_info(plan, catalog);
    }
    let (physical_source, fk_triggers) = dml_execution_parts(physical_plan, "Delete")?;
    run_delete_with_physical(
        delete,
        catalog,
        current_db,
        ctx,
        physical_source,
        fk_triggers,
        runtime,
    )
}

pub(crate) fn delete_source_query(delete: &tidb_ast::DeleteStmt) -> Option<tidb_ast::QueryStmt> {
    match &delete.kind {
        tidb_ast::DeleteKind::Single(table_ref) => super::access::PointPlanStmt::of_write(
            delete.where_clause.as_ref(),
            &delete.order_by,
            delete.limit.as_ref(),
            table_ref,
        )
        .write_select(),
        tidb_ast::DeleteKind::Multi { .. } => None,
    }
    .map(|mut select| {
        select.hints.clone_from(&delete.hints);
        tidb_ast::QueryStmt::Select(Box::new(select))
    })
}

fn run_delete_with_physical(
    delete: &tidb_ast::DeleteStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut physical_source: Option<&mut tidb_planner::physical::PhysicalPlan>,
    fk_triggers: &[FkTriggerNode],
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
) -> Result<u64, DriverError> {
    // `DELETE IGNORE` differs from a plain `DELETE` only in what it does with
    // a referential violation: Go downgrades it from a statement error to a
    // per-row skip with a warning. `QUICK` is parser-only and needs no branch.
    let table_ref = match &delete.kind {
        tidb_ast::DeleteKind::Single(table_ref) => table_ref,
        // See `multi_dml`'s shared metadata and row-identity owners.
        tidb_ast::DeleteKind::Multi { targets, from, .. } => {
            return super::multi_dml::run_multi_delete(
                delete,
                targets,
                from,
                catalog,
                current_db,
                ctx,
                physical_source,
                fk_triggers,
                runtime,
            );
        }
    };
    let (database, name) = single_table_name(table_ref, current_db)?;
    if !table_ref.partitions.is_empty()
        && !matches!(catalog.get_in(&database, &name), Some(TableEntry::Kv(_)))
    {
        return Err(DriverError::UnknownPartition {
            partition: table_ref.partitions[0].clone(),
            table: name.clone(),
        });
    }
    let physical = physical_source
        .as_deref_mut()
        .ok_or_else(|| DriverError::unsupported("DELETE has no retained physical child"))?;
    let rows = execute_physical_write_rows(
        physical,
        catalog,
        &database,
        &name,
        ctx,
        runtime,
        mem_quota::label::DELETE,
    )?
    .rows;
    let mut records = DeleteRecords::new(fk_triggers);
    let mut deleted = 0;
    for row in rows {
        deleted += u64::from(records.write(
            catalog,
            &database,
            &name,
            &row.id,
            &row.stored,
            delete.ignore,
            ctx,
        )?);
    }
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

fn execute_physical_write_rows(
    physical: &mut tidb_planner::physical::PhysicalPlan,
    catalog: &Catalog,
    database: &str,
    name: &str,
    ctx: &crate::StmtContext,
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
    memory_label: i64,
) -> Result<PhysicalWriteRows, DriverError> {
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
        DriverError::Schema(crate::SchemaErrorKind::UnknownTable(format!(
            "{database}.{name}"
        )))
    })?;
    let TableEntry::Kv(table) = table else {
        return Err(DriverError::Mysql(MysqlError::new(
            tidb_error::tidb::errcode::ErrUnsupportedOp,
            "operation not supported",
        )));
    };
    let stored_width = table.columns().len();
    let has_extra_handle =
        table.pk_handle_offset().is_none() && table.common_handle_offsets().is_empty();
    let expected_width = stored_width + usize::from(has_extra_handle);
    let accountant = ctx.statement_memory().write_accountant(memory_label);
    let mut rows = Vec::new();
    let collected = super::physical_builder::execute_dml_source(
        physical,
        catalog,
        ctx,
        runtime.is_some(),
        |row| {
            if row.len() < expected_width {
                return Err(DriverError::unsupported(format!(
                    "a physical write child returned {} columns, expected at least {expected_width}",
                    row.len()
                )));
            }
            let id = if let Some(offset) = table.pk_handle_offset() {
                match row.get(offset) {
                    Some(Datum::Int(value)) => crate::kv_table::TableHandle::Int(*value),
                    Some(Datum::UInt(value)) => crate::kv_table::TableHandle::Int(*value as i64),
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
                    .map(|offset| row[*offset].clone())
                    .collect::<Vec<_>>();
                table
                    .common_handle_of_values(&values, &ctx.session_zone())
                    .map_err(kv_write_error)?
            } else {
                match row.get(stored_width) {
                    Some(Datum::Int(value)) => crate::kv_table::TableHandle::Int(*value),
                    Some(Datum::UInt(value)) => crate::kv_table::TableHandle::Int(*value as i64),
                    _ => {
                        return Err(DriverError::unsupported(
                            "a physical write child returned no _tidb_rowid handle",
                        ));
                    }
                }
            };
            let stored = row[..stored_width].to_vec();
            // Both representations stay live until the write phase.
            accountant.account_row(&row).map_err(DriverError::from)?;
            accountant.account_row(&stored).map_err(DriverError::from)?;
            rows.push(PhysicalWriteRow {
                id,
                stored,
                output: row,
            });
            Ok(())
        },
    )?;
    if let Some(runtime) = runtime {
        runtime.extend(collected);
    }
    Ok(PhysicalWriteRows { rows, field_types })
}

/// A one-row chunk holding `row`, so an expression can be evaluated over it.
///
/// `field_types` is the SCHEMA the expression was built against, and it is
/// what decides the chunk's width. A row read straight from storage is wider
/// than that when the table has hidden expression-index columns; those are
/// the TAIL (see `crate::expression_index`), and no expression a statement
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
fn extra_handle_value(handle: &crate::kv_table::TableHandle) -> Option<i64> {
    fn integer(handle: &tidb_codec::table_key::RecordHandle) -> Option<i64> {
        match handle {
            tidb_codec::table_key::RecordHandle::Int(value) => Some(*value),
            tidb_codec::table_key::RecordHandle::Partition { handle, .. } => integer(handle),
            tidb_codec::table_key::RecordHandle::Common(_) => None,
        }
    }
    integer(&handle.record_handle())
}
