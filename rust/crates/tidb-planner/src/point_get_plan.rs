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

//! Go `pkg/planner/core/point_get_plan.go`: `TryFastPlan` -- the point and
//! batch point plans a statement whose whole predicate is a key's equality
//! gets before the ordinary optimizer runs.

use tidb_ast::{JoinNode, SelectField};
use tidb_datatype::{Datum, FieldType};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::{rewrite_expr_resolved, ColumnResolver};
use tidb_expr::schema::Schema;
use tidb_infoschema::{split_table_path, Catalog, TableEntry};
use crate::plan_builder::field_name::default_field_display_name;

use crate::from_scope::{single_table_scope, FromScope, ScopeResolver};
use crate::point_get_key::point_get_value;
use tidb_driver_error::DriverError;
use tidb_table::kv_table::{KvTable, TableHandle};
use tidb_hack::GoCaseCompare;

#[derive(Clone, Debug)]
pub struct FastPointOutput {
    pub offsets: Vec<usize>,
    pub columns: Vec<(String, FieldType)>,
    /// The table's visible name (its alias when it has one): every output
    /// column's `TableAsName`.
    pub table_alias: String,
}

/// Go `ast.TiDBRowChecksum`.
const TIDB_ROW_CHECKSUM: &str = "tidb_row_checksum";

/// The [`FastPointOutput::offsets`] entry of the checksum column: it is no
/// stored column's offset, so every reader that requires one declines it.
const ROW_CHECKSUM_OUTPUT_OFFSET: usize = usize::MAX;

/// Go `tryExtractRowChecksumColumn`'s type: `TypeString` in the default
/// charset and collation for that type, `MaxBlobWidth` wide, no decimals.
fn row_checksum_field_type() -> FieldType {
    FieldType::new(tidb_datatype::FieldTypeCode::String)
        .with_charset_name(tidb_mysql::charset::DefaultCharset)
        .with_collation_name(tidb_mysql::charset::DefaultCollationName)
        .with_flen(tidb_mysql::consts::MaxBlobWidth as i64)
        .with_decimal(0)
}

/// Go `planner.optimize` calls `TryFastPlan` before constructing the ordinary
/// logical plan. Keep that ordering for a complete point-read SELECT: only a
/// source-column projection whose whole predicate is owned by the point key is
/// returned here. Every residual operator or unsupported table shape declines
/// to the ordinary planner below.
pub fn fast_point_schema(table: &KvTable, output: &FastPointOutput) -> Schema {
    let columns = output
        .offsets
        .iter()
        .zip(&output.columns)
        .enumerate()
        .map(|(position, (offset, (name, field_type)))| {
            // Go `tryExtractRowChecksumColumn`: ID and UniqueID are both
            // `ExtraRowChecksumID`, and OrigName is the lowercase call text.
            if *offset == ROW_CHECKSUM_OUTPUT_OFFSET {
                let id = tidb_model::column::EXTRA_ROW_CHECKSUM_ID;
                let mut column = Column::new(id, field_type.clone());
                column.id = id;
                column.index = position as i64;
                column.orig_name = format!("{TIDB_ROW_CHECKSUM}()");
                return column;
            }
            let id = table
                .columns()
                .get(*offset)
                .map_or(tidb_model::column::EXTRA_HANDLE_ID, |column| column.id);
            let mut column = Column::new(id, field_type.clone());
            column.id = id;
            column.index = position as i64;
            column.orig_name.clone_from(name);
            column
        })
        .collect();
    Schema::new(columns)
}

fn closed_point_ranges(keys: &[Vec<Datum>]) -> tidb_ranger::types::Ranges {
    keys.iter()
        .map(|values| tidb_ranger::types::Range {
            low_val: values.clone(),
            high_val: values.clone(),
            collators: vec![tidb_charset::Collation::Binary; values.len()],
            low_exclude: false,
            high_exclude: false,
        })
        .collect()
}

/// Go `TryFastPlan` for a query statement, producing the same complete
/// physical root later consumed by `executorBuilder.build`. This is the
/// optimizer stage; it never drains a source or constructs a separate
/// executor pipeline.
pub fn try_fast_point_physical_plan(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &tidb_sessionctx::StmtContext,
) -> Result<Option<crate::physical::PhysicalPlan>, DriverError> {
    let plan = try_fast_point_physical_plan_with_allocator(
        select,
        catalog,
        current_db,
        ctx,
        &tidb_planner_core_base::PlanIdAllocator::new(),
    )?;
    // Go `TryFastPlan`'s deferred check on a SELECT: a session
    // `sql_select_limit` discards the fast plan so the ordinary planner can
    // apply the limit.
    if plan.is_some() && ctx.select_limit() != u64::MAX {
        ctx.append_warning_parts(
            1105,
            "sql_select_limit is set, so point get plan is not activated",
        );
        return Ok(None);
    }
    Ok(plan)
}

/// Go's fast-plan builder using the enclosing statement's plan-id counter.
/// DML tries its point child before allocating the write root, while a plain
/// query starts from a fresh counter; both call sites therefore share the
/// same implementation without assigning fixed IDs by hand.
pub fn try_fast_point_physical_plan_with_allocator(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &tidb_sessionctx::StmtContext,
    plan_ids: &tidb_planner_core_base::PlanIdAllocator,
) -> Result<Option<crate::physical::PhysicalPlan>, DriverError> {
    try_fast_point_physical_plan_with_allocator_mode(
        select, catalog, current_db, ctx, plan_ids, false,
    )
}

/// Go's `tryUpdatePointPlan` / `tryDeletePointPlan` retain the complete table
/// row and its handle on the PointGet child instead of applying a SELECT-list
/// projection.
pub fn try_fast_dml_point_physical_plan_with_allocator(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &tidb_sessionctx::StmtContext,
    plan_ids: &tidb_planner_core_base::PlanIdAllocator,
) -> Result<Option<crate::physical::PhysicalPlan>, DriverError> {
    try_fast_point_physical_plan_with_allocator_mode(
        select, catalog, current_db, ctx, plan_ids, true,
    )
}

/// Go `checkLockClauseTables` runs in `Preprocess`, before `tryPointGetPlan`:
/// an `OF` target that names an aliased table by its base name is accepted
/// with warning 1105, and a target that names no FROM table is "Unknown table
/// ... in locking clause". The fast plan has no `SelectLock` to carry the
/// clause, so the targets are checked here; `None` hands an unresolvable target
/// to the ordinary planner, which raises the error.
fn fast_point_lock_of_warnings(
    select: &tidb_ast::SelectStmt,
    current_db: &str,
) -> Option<Vec<String>> {
    let Some(lock) = select.lock.as_ref().filter(|lock| !lock.of.is_empty()) else {
        return Some(Vec::new());
    };
    let table_ref = sole_table_ref(&select.from)?;
    let (database, name) = split_table_path(&table_ref.name, current_db).ok()?;
    let mut warned = std::collections::BTreeSet::new();
    let mut warnings = Vec::new();
    for target in &lock.of {
        let (target_database, target_table) = match target.as_slice() {
            [table] => (None, table),
            [target_database, table] => (Some(target_database), table),
            _ => return None,
        };
        if target_database.is_some_and(|target| !target.go_lower_eq(database)) {
            return None;
        }
        match table_ref.alias.as_deref() {
            Some(alias) if alias.go_lower_eq(target_table) => {}
            Some(alias) if name.go_lower_eq(target_table) => {
                if warned.insert(target.clone()) {
                    warnings.push(format!(
                        "FOR UPDATE OF references the base table name while the table is aliased. Use the alias '{alias}' in OF to make the lock target explicit."
                    ));
                }
            }
            None if name.go_lower_eq(target_table) => {}
            _ => return None,
        }
    }
    Some(warnings)
}

fn try_fast_point_physical_plan_with_allocator_mode(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &tidb_sessionctx::StmtContext,
    plan_ids: &tidb_planner_core_base::PlanIdAllocator,
    dml_source: bool,
) -> Result<Option<crate::physical::PhysicalPlan>, DriverError> {
    let Some(of_warnings) = fast_point_lock_of_warnings(select, current_db) else {
        return Ok(None);
    };
    let plan =
        build_fast_point_physical_plan(select, catalog, current_db, ctx, plan_ids, dml_source)?;
    if plan.is_some() {
        for warning in &of_warnings {
            ctx.append_warning_parts(1105, warning);
        }
    }
    Ok(plan)
}

fn build_fast_point_physical_plan(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &tidb_sessionctx::StmtContext,
    plan_ids: &tidb_planner_core_base::PlanIdAllocator,
    dml_source: bool,
) -> Result<Option<crate::physical::PhysicalPlan>, DriverError> {
    if ctx.stable_result_mode()
        || ctx
            .optimizer_fix_control()
            .get_bool_with_default(tidb_fixcontrol::FIX_52592, false)
        || select.into_outfile.is_some()
        // Go `tryPointGetPlan` accepts a locking read and `getLockWaitTime`
        // sets `Lock` only when `PessimisticLockEligible` (autocommit off, or
        // a transaction open): an autocommit `FOR UPDATE` is a plain
        // `Point_Get` with no `SelectLock` above it, and inside a transaction
        // the point plan itself carries `lock`. Shared locks and `SKIP
        // LOCKED` keep the ordinary plan.
        || select.lock.as_ref().is_some_and(|lock| {
            lock.kind != tidb_ast::LockKind::Update
                || lock.wait == tidb_ast::LockWait::SkipLocked
        })
    {
        return Ok(None);
    }
    // Go `getLockWaitTime`: the lock a locking SELECT takes inside a
    // transaction. A write's point plan locks its rows through the statement.
    let select_lock = (!dml_source
        && (ctx.pessimistic_transaction() || ctx.optimistic_transaction()))
    .then_some(select.lock.as_ref())
    .flatten()
    .map(|lock| match lock.wait {
        tidb_ast::LockWait::NoWait => (crate::logical::SelectLockType::ForUpdateNoWait, 0),
        tidb_ast::LockWait::Wait(seconds) => {
            (crate::logical::SelectLockType::ForUpdateWaitN, seconds)
        }
        _ => (crate::logical::SelectLockType::ForUpdate, 0),
    });
    // The PARTITION clause is decided below, against the table and pin.
    let Some(table_ref) = sole_table_ref(&select.from) else {
        return Ok(None);
    };
    if table_ref.as_of.is_some() || table_ref.sample.is_some() {
        return Ok(None);
    }
    // Go `handleTableName` leaves an unqualified name that is a usable CTE
    // unresolved -- no current database is demanded and no table of that
    // name is looked up -- so `tryPointGetPlan` finds no `TableNameW` for
    // it (`resolveCtx.GetTableName(tblName) == nil`) and declines; the
    // ordinary planner then reads the CTE.
    if is_cte(select.with.as_ref(), table_ref) {
        return Ok(None);
    }
    let (database, name) = split_table_path(&table_ref.name, current_db)?;
    let Some(entry @ TableEntry::Kv(table)) = catalog.get_in(database, name) else {
        return Ok(None);
    };
    // A PARTITION clause survives only on a partitioned table's global-index
    // pin below (Go `PointGetPlan.PartitionNames`); the ordinary planner
    // answers every other shape, including the clause on a plain table.
    if !table_ref.partitions.is_empty() && table.partition().is_none() {
        return Ok(None);
    }
    let columns = entry.column_list();
    let visible = table_ref.alias.as_deref().unwrap_or(name);
    let mut scope = single_table_scope(
        visible,
        table_ref.alias.is_none().then(|| database.to_owned()),
        columns.clone(),
    );
    scope.zone = ctx.session_zone();
    let output = if dml_source {
        fast_dml_point_output(table)
    } else {
        let Some(output) = fast_point_output(select, &scope) else {
            return Ok(None);
        };
        output
    };
    let schema = fast_point_schema(table, &output);

    let mut batch = try_batch_point_get(select, table, &columns, &scope.zone, Some(ctx))?;
    if batch.as_ref().is_some_and(|batch| !batch.ignores_hints()) {
        batch = batch.filter(|batch| {
            let index_name = match batch.index.as_ref() {
                Some((index_id, _)) => match table
                    .plan_indexes()
                    .find(|index| index.id == *index_id)
                    .map(|index| index.name.as_str())
                {
                    Some(name) => Some(name),
                    None => return false,
                },
                None => None,
            };
            crate::access_path::fast_index_is_available_by_hints(
                current_db,
                database,
                visible,
                index_name,
                &select.hints,
                &table_ref.hints,
            )
        });
    }
    if let Some(mut batch) = batch {
        // Go `newBatchPointGetPlan` keeps a partitioned table's global unique
        // index: each entry records its row's partition, which the reader
        // checks against the PARTITION clause (`matchPartitionNames`).
        let global_index = batch
            .index
            .as_ref()
            .and_then(|(id, _)| table.plan_indexes().find(|index| index.id == *id))
            .is_some_and(|index| index.global);
        if !global_index && !table_ref.partitions.is_empty() {
            return Ok(None);
        }
        let partition_ids = if global_index {
            None
        } else if let Some(partition) = table.partition() {
            // The routed partitions follow this execution's keys; a cached
            // plan rebuilding parameter keys could not route them again.
            if batch.constants.is_some() {
                return Ok(None);
            }
            // Go newBatchPointGetPlan requires a bare partition column
            // (`PartitionExpr().Expr` is a `*expression.Column`), which the
            // COLUMNS and KEY forms do not carry.
            if !matches!(
                partition.kind,
                tidb_table::partition_routing::PartitionKind::Hash
                    | tidb_table::partition_routing::PartitionKind::Range { .. }
                    | tidb_table::partition_routing::PartitionKind::List { .. }
            ) || !matches!(partition.expr, Expression::Column(_))
            {
                return Ok(None);
            }
            // Go `PrunePartitionsAndValues`: an integer handle routes as the
            // handle column's value, a common handle or unique index by its
            // `IndexValues` placed at their columns (`getPartitionIdxs`).
            let routes = if batch.index.is_none() && !batch.common_handle {
                if table.pk_handle_offset().is_none() {
                    return Ok(None);
                }
                let handles = batch
                    .key_values
                    .iter()
                    .map(|values| match values.as_slice() {
                        [Datum::Int(value)] => Ok(TableHandle::Int(*value)),
                        [Datum::UInt(value)] => Ok(TableHandle::Int(*value as i64)),
                        _ => Err(DriverError::unsupported("invalid partition batch handle")),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                table.handle_partition_routes(&handles, &scope.zone, ctx)
            } else {
                let offsets = match &batch.index {
                    Some((index_id, _)) => {
                        let Some(index) = table.plan_indexes().find(|index| index.id == *index_id)
                        else {
                            return Ok(None);
                        };
                        index.column_offsets.clone()
                    }
                    None => table.common_handle_offsets().to_vec(),
                };
                batch
                    .key_values
                    .iter()
                    .map(|values| {
                        let mut row = vec![Datum::Null; table.columns().len()];
                        for (offset, value) in offsets.iter().zip(values) {
                            *row.get_mut(*offset)? = value.clone();
                        }
                        table.row_partition_route(&row, ctx)
                    })
                    .collect()
            };
            let mut ids = Vec::with_capacity(routes.len());
            let mut keys = Vec::with_capacity(routes.len());
            for (key, route) in batch.key_values.into_iter().zip(routes) {
                if let Some((_, id)) = route {
                    keys.push(key);
                    ids.push(id);
                }
            }
            batch.key_values = keys;
            Some(ids)
        } else {
            None
        };
        if batch.key_values.is_empty() {
            let mut base = crate::physical::BasePhysicalPlan::new(plan_ids, "TableDual", 0);
            base.base
                .set_stats(Some(tidb_planner_property::stats_info::StatsInfo::new(0.0, [])));
            base.base.set_schema(Some(schema));
            return Ok(Some(crate::physical::PhysicalPlan::TableDual(
                crate::physical::PhysicalTableDual::new(base, 0),
            )));
        }
        let mut base =
            crate::physical::BasePhysicalPlan::new(plan_ids, "Batch_Point_Get", 0);
        base.base
            .set_stats(Some(tidb_planner_property::stats_info::StatsInfo::new(
                batch.plan_rows as f64,
                [],
            )));
        base.base.set_schema(Some(schema));
        let ranges = closed_point_ranges(&batch.key_values);
        return Ok(Some(crate::physical::PhysicalPlan::BatchPointGet(
            crate::physical::PhysicalBatchPointGet {
                base,
                table_id: table.table_id(),
                index_id: batch.index.as_ref().map(|(id, _)| *id),
                access_cols: None,
                unsigned_handle: table.unsigned_pk_handle(),
                ranges,
                partition_ids,
                partition_names: if global_index {
                    table_ref.partitions.clone()
                } else {
                    Vec::new()
                },
                range_rebuild: batch
                    .constants
                    .map(crate::physical_plan_cache::PointRangeRebuild::Constants),
                keep_order: false,
                desc: false,
                lock: select_lock.is_some(),
                select_lock,
            },
        )));
    }

    let mut table_dual = false;
    let Some(point) = try_point_get(
        &PointPlanStmt::of_select(select),
        table,
        &columns,
        &scope.zone,
        Some(ctx),
        &mut table_dual,
    )?
    else {
        if table_dual
            && overflow_plans_table_dual(
                select,
                table,
                &columns,
                &scope.zone,
                current_db,
                database,
                visible,
                table_ref,
            )
        {
            // Go `tryPointGetPlan`'s `isTableDual` arm: the predicate can
            // equal no stored row, so the answer is empty and no read runs.
            let mut base = crate::physical::BasePhysicalPlan::new(plan_ids, "TableDual", 0);
            base.base
                .set_stats(Some(tidb_planner_property::stats_info::StatsInfo::new(0.0, [])));
            base.base.set_schema(Some(schema));
            return Ok(Some(crate::physical::PhysicalPlan::TableDual(
                crate::physical::PhysicalTableDual::new(base, 0),
            )));
        }
        return Ok(None);
    };
    if !point_get_consumes_where(select, table, &columns, &scope.zone) {
        return Ok(None);
    }
    // Go `tryPointGetPlan` plans partitioned tables too. This fast path
    // keeps the global unique index, whose entry records the row's
    // partition, and an integer handle pin, which the executor routes by the
    // partition expression (`point_partition_id`); common-handle, local-index
    // and PARTITION-clause pins take the ordinary planner.
    let global_index = point
        .index_id
        .and_then(|index_id| table.plan_indexes().find(|index| index.id == index_id))
        .is_some_and(|index| index.global);
    let routed_int_handle = point.index_id.is_none()
        && table.common_handle_offsets().is_empty()
        && table_ref.partitions.is_empty();
    if table.partition().is_some() && !global_index && !routed_int_handle {
        return Ok(None);
    }
    let index_name = point.index_id.and_then(|index_id| {
        table
            .plan_indexes()
            .find(|index| index.id == index_id)
            .map(|index| index.name.as_str())
    });
    if point.index_id.is_some() && index_name.is_none() {
        return Ok(None);
    }
    if !crate::access_path::fast_index_is_available_by_hints(
        current_db,
        database,
        visible,
        index_name,
        &select.hints,
        &table_ref.hints,
    ) {
        return Ok(None);
    }
    let mut base = crate::physical::BasePhysicalPlan::new(plan_ids, "Point_Get", 0);
    base.base
        .set_stats(Some(tidb_planner_property::stats_info::StatsInfo::new(1.0, [])));
    base.base.set_schema(Some(schema));
    // Go `PointGetPlan.HandleConstant` / `IndexConstants`.
    let constants = crate::physical_plan_cache::PointConstantsRebuild {
        rows: vec![point
            .key_markers
            .iter()
            .zip(&point.key_values)
            .map(
                |(marker, value)| crate::physical_plan_cache::PointKeyConstant {
                    marker: *marker,
                    value: value.clone(),
                },
            )
            .collect()],
        field_types: point.key_types.clone(),
        handle_batch: false,
    };
    let range_rebuild = constants
        .has_parameters()
        .then_some(crate::physical_plan_cache::PointRangeRebuild::Constants(constants));
    Ok(Some(crate::physical::PhysicalPlan::PointGet(
        crate::physical::PhysicalPointGet {
            base,
            table_id: table.table_id(),
            // Go `PointGetPlan.PartitionNames`: a global index applies them
            // to the partition its entry records.
            partition: ((global_index && !table_ref.partitions.is_empty())
                || (routed_int_handle && table.partition().is_some()))
            .then(|| crate::physical::PointGetPartition {
                names: table_ref.partitions.clone(),
                physical_table_id: None,
            }),
            index_id: point.index_id,
            access_cols: None,
            ranges: closed_point_ranges(&[point.key_values]),
            range_rebuild,
            lock: select_lock.is_some(),
            select_lock,
        },
    )))
}

/// Whether Go's `tryPointGetPlan` turns a `getNameValuePairs` walk that ended
/// on `isTableDual` (a constant overflowing its column) into a `TableDual`.
///
/// The walk keeps its pairs only when the `WHERE` is that one equality;
/// through an `AND` it returns no pairs at all. Go then plans the dual when
/// the single pair is the integer handle (`findPKHandle`, with
/// `len(pairs) == 1` and the hints admitting the handle), refuses outright
/// when the handle is pinned otherwise, and else hands the dual to the first
/// unique index `checkTblIndexForPointPlan` may use -- whatever its columns
/// -- declining when there is none. A declined statement is planned by the
/// ordinary planner.
#[allow(clippy::too_many_arguments)]
fn overflow_plans_table_dual(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
    current_db: &str,
    database: &str,
    visible: &str,
    table_ref: &tidb_ast::TableRef,
) -> bool {
    let index_available = |index_name: Option<&str>| {
        crate::access_path::fast_index_is_available_by_hints(
            current_db,
            database,
            visible,
            index_name,
            &select.hints,
            &table_ref.hints,
        )
    };
    let mut where_clause = select.where_clause.as_ref();
    while let Some(tidb_ast::Expr::Paren(inner)) = where_clause {
        where_clause = Some(inner);
    }
    let mut pairs = Vec::new();
    let single_pair = where_clause.is_some_and(|expr| {
        matches!(expr, tidb_ast::Expr::Binary(tidb_ast::BinaryOp::Eq, _, _))
            && name_value_pairs(expr, &mut pairs, zone)
    });
    if let Some(handle_offset) = table.pk_handle_offset() {
        let handle_column = &columns[handle_offset].0;
        if single_pair && pairs[0].column.go_lower_eq(handle_column) {
            return index_available(None);
        }
    }
    (!table.common_handle_offsets().is_empty() && index_available(None))
        || table
            .point_get_indexes()
            .any(|index| index.unique && index_available(Some(&index.name)))
}

fn fast_dml_point_output(table: &KvTable) -> FastPointOutput {
    let mut offsets = (0..table.columns().len()).collect::<Vec<_>>();
    let mut columns = table
        .columns()
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect::<Vec<_>>();
    if table.pk_handle_offset().is_none() && table.common_handle_offsets().is_empty() {
        offsets.push(table.columns().len());
        columns.push((
            tidb_model::column::EXTRA_HANDLE_NAME.to_owned(),
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
                .with_flags(tidb_datatype::FieldTypeFlags::NOT_NULL),
        ));
    }
    FastPointOutput {
        offsets,
        columns,
        table_alias: table.name(),
    }
}

/// The part of a column name Go's point-get fast path reads:
/// `buildSchemaFromFields` / `getNameValuePairs` check only `Name.Table`
/// against the table and look the column up by `Name.Name`, never reading
/// `Name.Schema` -- so `SELECT nosuch.t.a FROM t WHERE id = 1` is a point get
/// that answers, where the ordinary planner would refuse the qualifier
/// (1054).
pub fn point_get_column_path(path: &[String]) -> &[String] {
    &path[path.len().saturating_sub(2)..]
}

pub fn fast_point_output(select: &tidb_ast::SelectStmt, scope: &FromScope) -> Option<FastPointOutput> {
    let resolver = ScopeResolver { scope };
    let mut offsets = Vec::new();
    let mut columns = Vec::new();
    for (field_index, field) in select.fields.fields().iter().enumerate() {
        match field {
            SelectField::Expr {
                expr: expr @ tidb_ast::Expr::Column(path),
                alias,
            } => {
                let (offset, field_type, _) = resolver.resolve(point_get_column_path(path))?;
                let name = alias.clone().unwrap_or_else(|| {
                    default_field_display_name(&select.fields, field_index, expr)
                });
                offsets.push(offset);
                columns.push((name, field_type));
            }
            // Go `buildSchemaFromFields` asks `tryExtractRowChecksumColumn`
            // before requiring a column name: a bare zero-argument
            // `TIDB_ROW_CHECKSUM()` is the point get's checksum column.
            SelectField::Expr {
                expr: tidb_ast::Expr::Func { name, args, .. },
                alias,
            } if args.is_empty() && name.eq_ignore_ascii_case(TIDB_ROW_CHECKSUM) => {
                // `origName` is `FnName` plus "()", with the name as written.
                let written = select
                    .fields
                    .original_text(field_index)
                    .and_then(|text| text.split(|byte| *byte == b'(').next())
                    .map(|text| String::from_utf8_lossy(text.trim_ascii_end()).into_owned())
                    .unwrap_or_else(|| name.clone());
                let name = alias.clone().unwrap_or_else(|| format!("{written}()"));
                offsets.push(ROW_CHECKSUM_OUTPUT_OFFSET);
                columns.push((name, row_checksum_field_type()));
            }
            SelectField::Expr { .. } => return None,
            SelectField::Wildcard(qualifier) => {
                if qualifier.last().is_none() {
                    for (offset, name, field_type) in scope.star_columns() {
                        offsets.push(offset);
                        columns.push((name, field_type));
                    }
                    continue;
                }
                let table_name = qualifier.last()?;
                let mut matched = false;
                for table in scope
                    .tables
                    .iter()
                    .filter(|table| table.name.go_lower_eq(table_name))
                {
                    matched = true;
                    for (local_offset, (name, field_type)) in table.columns.iter().enumerate() {
                        offsets.push(table.offset + local_offset);
                        columns.push((name.clone(), field_type.clone()));
                    }
                }
                if !matched {
                    return None;
                }
            }
        }
    }
    Some(FastPointOutput {
        offsets,
        columns,
        table_alias: scope.tables.first()?.name.clone(),
    })
}

fn point_get_consumes_where(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    point_get_predicate_is_consumed(
        select.where_clause.as_ref(),
        table,
        columns,
        zone,
        sole_table_ref(&select.from).map_or(&[][..], |table_ref| table_ref.partitions.as_slice()),
    )
}

pub fn point_get_predicate_is_consumed(
    where_clause: Option<&tidb_ast::Expr>,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
    named_partitions: &[String],
) -> bool {
    let Some(where_clause) = where_clause else {
        return false;
    };
    let mut pairs = Vec::new();
    if !name_value_pairs(where_clause, &mut pairs, zone) || pairs.is_empty() {
        return false;
    }
    let key_matches = |offsets: &[usize]| {
        offsets.len() == pairs.len()
            && offsets.iter().all(|offset| {
                columns
                    .get(*offset)
                    .is_some_and(|(name, _)| pairs.iter().any(|pair| pair.column.go_lower_eq(name)))
            })
    };
    if table
        .pk_handle_offset()
        .is_some_and(|offset| key_matches(std::slice::from_ref(&offset)))
        || key_matches(table.common_handle_offsets())
    {
        return true;
    }
    // The extra handle pins a row as completely as an integer primary key
    // does, so Go's point plan over `_tidb_rowid = c` carries no `Selection`
    // either -- its recorded plan for `select * from t where _tidb_rowid = 0`
    // is a bare `Point_Get table:t handle:0`.
    if table.pk_handle_offset().is_none()
        && table.common_handle_offsets().is_empty()
        && (table.partition().is_none() || named_partitions.len() == 1)
        && pairs.len() == 1
        && pairs[0]
            .column
            .go_lower_eq(tidb_model::column::EXTRA_HANDLE_NAME)
    {
        return true;
    }
    let matches_unique_index = table
        .point_get_indexes()
        .any(|index| index.unique && !index.has_prefix() && key_matches(&index.column_offsets));
    matches_unique_index
}

/// The partitions a single-table `SELECT` still reads, named as declared and
/// in definition order -- Go's `PartitionProcessor` output, which is the list
/// `EXPLAIN` fans a static-mode plan out over.
///
/// Two narrowings compose, in either order and both cumulative, exactly as
/// [`crate::KvTable::restrict_read_to_partitions`] composes them for the read
/// itself: the statement's own `PARTITION (p, ...)` list, and whatever the
/// `WHERE` pruned. Empty for an unpartitioned table.
///
/// An unresolvable `PARTITION (p)` name answers the FULL list rather than
/// failing here: the read has already raised 1735 for it, and this is only
/// ever asked for a plan that got built.
pub fn single_table_ref(from: &Option<tidb_ast::Join>) -> Option<&tidb_ast::TableRef> {
    let table_ref = sole_table_ref(from)?;
    // A `PARTITION (...)` restriction is refused by `build_from`; declining
    // the fast path here too keeps a point get from answering a statement the
    // scan would have rejected.
    if !table_ref.partitions.is_empty() {
        return None;
    }
    Some(table_ref)
}

/// [`single_table_ref`] WITHOUT its fast-path refusal: the one table a `FROM`
/// names, whether or not the statement narrowed it with `PARTITION (...)`.
///
/// The refusal above is about which ACCESS PATHS may be chosen. Callers that
/// only want to know which table -- and which partitions of it -- the
/// statement reads want this one, so that a `PARTITION (p)` narrowing is
/// reported rather than silently read as "no single table".
pub fn sole_table_ref(from: &Option<tidb_ast::Join>) -> Option<&tidb_ast::TableRef> {
    let join = from.as_ref()?;
    if join.right.is_some() {
        return None;
    }
    let JoinNode::Table(table_ref) = &join.left else {
        return None;
    };
    Some(table_ref)
}

/// Go `tryWhereIn2BatchPointGet`: a single-table `SELECT` whose whole `WHERE`
/// is `column IN (constants)` over the handle or a single-column unique index
/// reads those rows directly instead of scanning.
///
/// Go rejects the fast plan when `ORDER BY`, `GROUP BY`, `LIMIT`, `HAVING`,
/// `DISTINCT` or a window spec is present, when the `IN` is negated, and when
/// its list is empty. The handle path applies when the table's primary key IS
/// the handle and the column names it; otherwise a unique index whose only
/// column it is.
///
/// The row form, `(a, b) IN ((1, 2), (3, 4))`, is a composite-key
/// `Batch_Point_Get` when the tuples pin every column of a unique index or a
/// clustered common handle.
pub struct BatchPointLookup {
    index: Option<(i64, String)>,
    common_handle: bool,
    plan_rows: usize,
    key_values: Vec<Vec<Datum>>,
    /// Go `HandleParams` / `IndexValueParams`: every candidate key with the
    /// EXECUTE parameters it reads, present when any part is a parameter.
    constants: Option<crate::physical_plan_cache::PointConstantsRebuild>,
}

/// One IN-list item a batch point plan keys by. Go's `newBatchPointGetPlan`
/// accepts only a literal (`ValueExpr`) or a marker (`ParamMarkerExpr`),
/// whose parameter order a cached plan reads again.
fn batch_point_constant(
    item: &tidb_ast::Expr,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<(Datum, Option<usize>)> {
    let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(
        item,
        &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
    ) else {
        return None;
    };
    if constant.deferred_expr.is_some() {
        return None;
    }
    let marker = constant
        .param_marker
        .and_then(|marker| usize::try_from(marker.order).ok());
    // A marker's constant holds the value its parameter carried; evaluating
    // it would read the parameter again from a statement this has none of.
    let value = match marker {
        Some(_) => constant.value.clone(),
        None => constant.eval().ok()?,
    };
    Some((value, marker))
}

impl BatchPointLookup {
    fn handle(handles: Vec<TableHandle>, plan_rows: usize) -> Self {
        let key_values = handles
            .iter()
            .filter_map(|handle| match handle {
                TableHandle::Int(value) => Some(vec![Datum::Int(*value)]),
                TableHandle::Common(_) => None,
            })
            .collect();
        Self {
            index: None,
            common_handle: false,
            plan_rows,
            key_values,
            constants: None,
        }
    }

    fn common_handle(key_values: Vec<Vec<Datum>>, plan_rows: usize) -> Self {
        Self {
            index: None,
            common_handle: true,
            plan_rows,
            key_values,
            constants: None,
        }
    }

    /// Keeps the candidate keys a cached plan rebuilds, when any of them
    /// reads an EXECUTE parameter.
    fn with_constants(
        mut self,
        rows: Vec<Vec<crate::physical_plan_cache::PointKeyConstant>>,
        field_types: Vec<FieldType>,
        handle_batch: bool,
    ) -> Self {
        let rebuild = crate::physical_plan_cache::PointConstantsRebuild {
            rows,
            field_types,
            handle_batch,
        };
        self.constants = rebuild.has_parameters().then_some(rebuild);
        self
    }

    fn index(
        plan_rows: usize,
        table: &KvTable,
        columns: &[(String, FieldType)],
        index: &tidb_table::kv_table::KvIndex,
        key_values: Vec<Vec<Datum>>,
    ) -> Self {
        let index_columns = index
            .column_offsets
            .iter()
            .map(|offset| {
                columns.get(*offset).map_or_else(
                    || index_key_part_name(table, *offset),
                    |column| column.0.clone(),
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        Self {
            index: Some((index.id, format!("index:{}({index_columns})", index.name))),
            common_handle: false,
            plan_rows,
            key_values,
            constants: None,
        }
    }

    fn ignores_hints(&self) -> bool {
        self.index.is_none() && !self.common_handle
    }
}

/// Go `tryWhereIn2BatchPointGet`. `statement` is the statement whose type
/// context converts the list (see [`batch_key_value`]); `None` applies the
/// detached rule.
pub fn try_batch_point_get(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
    statement: Option<&dyn tidb_exprctx::Columns>,
) -> Result<Option<BatchPointLookup>, DriverError> {
    let stmt = PointPlanStmt::of_select(select);
    try_batch_point_get_stmt(&stmt, table, columns, zone, statement)
}

/// Go `newBatchPointGetPlan`'s `getPointGetValue` for one `IN` list element:
/// under the statement's type context while the fast plan is built, which
/// reports each element's conversion through the statement in list order
/// (`id IN ('a', 'b')` warns for `'a'` twice, then for `'b'` twice), or
/// silently by the detached rule for a caller outside planning.
fn batch_key_value(
    statement: Option<&dyn tidb_exprctx::Columns>,
    column: &FieldType,
    value: &Datum,
) -> Option<Datum> {
    match statement {
        Some(ctx) => crate::point_get_value::statement_point_get_value(ctx, column, value),
        None => point_get_value(column, value),
    }
}

pub fn try_batch_point_get_stmt(
    select: &PointPlanStmt<'_>,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
    statement: Option<&dyn tidb_exprctx::Columns>,
) -> Result<Option<BatchPointLookup>, DriverError> {
    if select.having.is_some()
        || !select.order_by.is_empty()
        || !select.group_by.is_empty()
        || select.limit.is_some()
        || select.distinct
    {
        return Ok(None);
    }
    let Some(where_clause) = select.where_clause else {
        return Ok(None);
    };
    // The WHERE must be exactly the IN, as Go requires a PatternInExpr.
    let tidb_ast::Expr::In { expr, list, not } = where_clause else {
        return Ok(None);
    };
    if *not || list.is_empty() {
        return Ok(None);
    }
    // The row form is Go's composite-key Batch_Point_Get. Each tuple value
    // is converted into the indexed column's domain before the key lookup;
    // any value that cannot round-trip exactly declines the fast path and
    // leaves the ordinary scan to preserve the written predicate's answer.
    if let tidb_ast::Expr::Row(left) = &**expr {
        let mut names = Vec::with_capacity(left.len());
        for column in left {
            let tidb_ast::Expr::Column(path) = column else {
                return Ok(None);
            };
            let Some(name) = path.last() else {
                return Ok(None);
            };
            names.push(name);
        }
        // A clustered composite primary key is represented by the common
        // handle offsets, not by a KvIndex. Its encoded datum key is the
        // record handle itself, so it can use the same direct lookup source as
        // a unique index without manufacturing a redundant index entry.
        let common_offsets = table.common_handle_offsets().to_vec();
        if common_offsets.len() == names.len() {
            let mut positions = Vec::with_capacity(common_offsets.len());
            for offset in &common_offsets {
                let Some((column_name, _)) = columns.get(*offset) else {
                    positions.clear();
                    break;
                };
                let Some(position) = names.iter().position(|name| column_name.go_lower_eq(name))
                else {
                    positions.clear();
                    break;
                };
                positions.push(position);
            }
            if positions.len() == common_offsets.len() {
                let mut handles = Vec::with_capacity(list.len());
                let mut all_key_values = Vec::with_capacity(list.len());
                let mut constant_rows = Vec::with_capacity(list.len());
                for candidate in list {
                    let tidb_ast::Expr::Row(values) = candidate else {
                        return Ok(None);
                    };
                    if values.len() != left.len() {
                        return Ok(None);
                    }
                    let mut key_values = Vec::with_capacity(common_offsets.len());
                    let mut constant_row = Vec::with_capacity(common_offsets.len());
                    for (offset, position) in common_offsets.iter().zip(&positions) {
                        let Some((value, marker)) = batch_point_constant(&values[*position], zone)
                        else {
                            return Ok(None);
                        };
                        let Some(value) = batch_key_value(statement, &columns[*offset].1, &value)
                        else {
                            return Ok(None);
                        };
                        constant_row.push(crate::physical_plan_cache::PointKeyConstant {
                            marker,
                            value: value.clone(),
                        });
                        key_values.push(value);
                    }
                    constant_rows.push(constant_row);
                    let encoded =
                        tidb_codec::encode_key_in_timezone(zone, &key_values).map_err(|e| {
                            DriverError::from(tidb_util::misc::syntax_error(format!(
                                "common handle encode failed: {e:?}"
                            )))
                        })?;
                    let handle = tidb_txnkv::CommonHandle::new(encoded).map_err(|e| {
                        DriverError::from(tidb_util::misc::syntax_error(format!(
                            "common handle build failed: {e:?}"
                        )))
                    })?;
                    let handle = TableHandle::Common(handle.encoded().to_vec());
                    if !handles.contains(&handle) {
                        handles.push(handle);
                        all_key_values.push(key_values);
                    }
                }
                let key_types = common_offsets
                    .iter()
                    .map(|offset| columns[*offset].1.clone())
                    .collect();
                return Ok(Some(
                    BatchPointLookup::common_handle(all_key_values, list.len()).with_constants(
                        constant_rows,
                        key_types,
                        true,
                    ),
                ));
            }
        }
        for index in table.point_get_indexes().cloned().collect::<Vec<_>>() {
            if !index.unique || index.has_prefix() || index.column_offsets.len() != names.len() {
                continue;
            }
            let mut positions = Vec::with_capacity(index.column_offsets.len());
            for offset in &index.column_offsets {
                let Some((column_name, _)) = columns.get(*offset) else {
                    positions.clear();
                    break;
                };
                let Some(position) = names.iter().position(|name| column_name.go_lower_eq(name))
                else {
                    positions.clear();
                    break;
                };
                positions.push(position);
            }
            if positions.len() != index.column_offsets.len() {
                continue;
            }
            let mut index_values = Vec::with_capacity(list.len());
            let mut constant_rows = Vec::with_capacity(list.len());
            for candidate in list {
                let tidb_ast::Expr::Row(values) = candidate else {
                    return Ok(None);
                };
                if values.len() != left.len() {
                    return Ok(None);
                }
                let mut key_values = Vec::with_capacity(index.column_offsets.len());
                let mut constant_row = Vec::with_capacity(index.column_offsets.len());
                for (offset, position) in index.column_offsets.iter().zip(&positions) {
                    let Some((value, marker)) = batch_point_constant(&values[*position], zone)
                    else {
                        return Ok(None);
                    };
                    let Some(value) = batch_key_value(statement, &columns[*offset].1, &value)
                    else {
                        return Ok(None);
                    };
                    constant_row.push(crate::physical_plan_cache::PointKeyConstant {
                        marker,
                        value: value.clone(),
                    });
                    key_values.push(value);
                }
                index_values.push(key_values);
                constant_rows.push(constant_row);
            }
            let key_types = index
                .column_offsets
                .iter()
                .map(|offset| columns[*offset].1.clone())
                .collect();
            return Ok(Some(
                BatchPointLookup::index(list.len(), table, columns, &index, index_values)
                    .with_constants(constant_rows, key_types, false),
            ));
        }
        return Ok(None);
    }
    let tidb_ast::Expr::Column(path) = &**expr else {
        return Ok(None);
    };
    let Some(name) = path.last() else {
        return Ok(None);
    };

    // Every list element must be a constant, or this is not a point plan.
    let mut values = Vec::with_capacity(list.len());
    let mut markers = Vec::with_capacity(list.len());
    for item in list {
        let Some((value, marker)) = batch_point_constant(item, zone) else {
            return Ok(None);
        };
        values.push(value);
        markers.push(marker);
    }

    // The handle path.
    if let Some(offset) = table.pk_handle_offset() {
        if columns[offset].0.go_lower_eq(name) {
            // Go `newBatchPointGetPlan` runs every list element through
            // `getPointGetValue` and returns `nil` -- no batch plan at all --
            // as soon as one of them is not exactly representable, so a list
            // mixing `1.0` with `1.5` still answers from a scan rather than
            // silently dropping the element it cannot key.
            let mut handles = Vec::with_capacity(values.len());
            let mut constant_rows = Vec::with_capacity(values.len());
            for (value, marker) in values.iter().zip(&markers) {
                let handle = match batch_key_value(statement, &columns[offset].1, value) {
                    Some(Datum::Int(v)) => v,
                    Some(Datum::UInt(v)) => v as i64,
                    _ => return Ok(None),
                };
                constant_rows.push(vec![crate::physical_plan_cache::PointKeyConstant {
                    marker: *marker,
                    value: Datum::Int(handle),
                }]);
                let handle = TableHandle::Int(handle);
                if !handles.contains(&handle) {
                    handles.push(handle);
                }
            }
            return Ok(Some(
                BatchPointLookup::handle(handles, list.len()).with_constants(
                    constant_rows,
                    vec![columns[offset].1.clone()],
                    true,
                ),
            ));
        }
    }

    // The unique-index path.
    for index in table.point_get_indexes().cloned().collect::<Vec<_>>() {
        if !index.unique || index.column_offsets.len() != 1 {
            continue;
        }
        // Go `point_get_plan.go` declines an index with `HasPrefixIndex()`:
        // an entry found by a CUT value does not prove the row matches, and a
        // point get has no residual predicate to catch that. Skipping the
        // index here is load-bearing, not defensive -- `lookup_unique` fails
        // closed with `None`, which this loop would otherwise read as "no
        // such row" and answer zero rows for a row that exists.
        if index.has_prefix() {
            continue;
        }
        // Resolved through `get` for the same reason the single point get
        // does: an EXPRESSION key part's hidden generated column sits past
        // the end of the scope's visible columns, and no `IN` list names it.
        let Some((index_column, field_type)) = columns.get(index.column_offsets[0]) else {
            continue;
        };
        if !index_column.go_lower_eq(name) {
            continue;
        }
        let mut converted = Vec::with_capacity(values.len());
        for value in &values {
            let Some(value) = batch_key_value(statement, field_type, value) else {
                return Ok(None);
            };
            converted.push(value);
        }
        let constant_rows = converted
            .iter()
            .zip(&markers)
            .map(|(value, marker)| {
                vec![crate::physical_plan_cache::PointKeyConstant {
                    marker: *marker,
                    value: value.clone(),
                }]
            })
            .collect();
        let index_values = converted
            .into_iter()
            .map(|value| vec![value])
            .collect::<Vec<_>>();
        let field_type = field_type.clone();
        return Ok(Some(
            BatchPointLookup::index(list.len(), table, columns, &index, index_values)
                .with_constants(constant_rows, vec![field_type], false),
        ));
    }
    Ok(None)
}

/// Moves every pair's constant into its column's domain, in place.
///
/// Returns false when any pair names an unknown column or holds a constant
/// the column cannot represent exactly, which is Go's "no point plan; let the
/// scan decide" answer.
pub fn convert_pairs_to_column_domain(
    pairs: &mut [NameValuePair],
    columns: &[(String, FieldType)],
) -> bool {
    convert_pairs_to_column_domain_by(pairs, |wanted| {
        columns
            .iter()
            .find(|(name, _)| name.go_lower_eq(wanted))
            .map(|(_, field_type)| field_type)
    })
}

/// [`convert_pairs_to_column_domain`] over any column lookup, so a caller
/// holding the table's own column list need not copy it first.
pub fn convert_pairs_to_column_domain_by<'a>(
    pairs: &mut [NameValuePair],
    field_type_of: impl Fn(&str) -> Option<&'a FieldType>,
) -> bool {
    for pair in pairs {
        let Some(field_type) = field_type_of(&pair.column) else {
            return false;
        };
        let Some(value) = point_get_value(field_type, &pair.value) else {
            return false;
        };
        pair.value = value;
    }
    true
}

/// One `column = constant` equality from a `WHERE`, Go's `nameValuePair`.
pub struct NameValuePair {
    column: String,
    /// The table qualifier the column was written with (`t` in `t.a = 1`).
    table: Option<String>,
    value: Datum,
    /// Go `nameValuePair.con`: the EXECUTE parameter the value came from,
    /// which a cached plan reads again on every execution.
    marker: Option<usize>,
}

impl NameValuePair {
    /// The column this equality pinned.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// The value it pinned the column to, already moved into that column's
    /// domain by [`convert_pairs_to_column_domain`].
    pub const fn value(&self) -> &Datum {
        &self.value
    }
}

fn index_key_part_name(table: &KvTable, offset: usize) -> String {
    let Some(column) = table.columns().get(offset) else {
        return String::new();
    };
    match &column.generated {
        Some(generated) if table.is_hidden(offset) => generated.expr_text.clone(),
        _ => column.name.clone(),
    }
}

/// Go `getNameValuePairs`: flattens a `WHERE` that is a conjunction of
/// `column = constant` equalities into pairs, returning `None` for any other
/// shape.
///
/// Go accepts the constant on either side of the `=`, and recurses only
/// through `AND`; anything else (an `OR`, a comparison, a function call)
/// makes the statement ineligible for a point get, which is what returning
/// `None` means here.
pub fn name_value_pairs(
    expr: &tidb_ast::Expr,
    pairs: &mut Vec<NameValuePair>,
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    point_equal_pairs(expr, pairs, zone, &[])
}

/// [`Self::name_value_pairs`]-shaped walker that ALSO resolves `?` markers
/// against EXECUTE parameters, so one implementation decides the point-pin
/// rule for a bound tree and for a PREPARE template alike. A template whose
/// marker position is unfilled refuses, exactly like a non-literal.
pub fn point_equal_pairs(
    expr: &tidb_ast::Expr,
    pairs: &mut Vec<NameValuePair>,
    zone: &tidb_datatype::SessionTimeZone,
    params: &[Datum],
) -> bool {
    point_equal_pairs_visiting(expr, pairs, zone, params, &mut |_| true)
}

/// [`point_equal_pairs`] that hands each pair to `visit` as the walk reaches
/// it, before the next conjunct is looked at, and stops the walk when `visit`
/// refuses. This is the order Go's `getNameValuePairs` checks and converts a
/// pair in, which is observable: the conversion of an earlier pair reports
/// its warnings even when a later conjunct declines the point plan.
fn point_equal_pairs_visiting(
    expr: &tidb_ast::Expr,
    pairs: &mut Vec<NameValuePair>,
    zone: &tidb_datatype::SessionTimeZone,
    params: &[Datum],
    visit: &mut dyn FnMut(&mut NameValuePair) -> bool,
) -> bool {
    use tidb_ast::{BinaryOp, Expr};
    fn unparenthesized(expr: &Expr) -> &Expr {
        match expr {
            Expr::Paren(inner) => unparenthesized(inner),
            other => other,
        }
    }
    match expr {
        Expr::Paren(inner) => point_equal_pairs_visiting(inner, pairs, zone, params, visit),
        Expr::Binary(BinaryOp::LogicAnd, lhs, rhs) => {
            point_equal_pairs_visiting(lhs, pairs, zone, params, visit)
                && point_equal_pairs_visiting(rhs, pairs, zone, params, visit)
        }
        Expr::Binary(BinaryOp::Eq, lhs, rhs) => {
            // Stripped on both sides for the reason the arm above recurses
            // through `Expr::Paren`: parentheses are syntax, and Go has
            // unwrapped them before a point-get key is ever looked for, so
            // `(a)=1` names the same key that `a=1` does.
            let (column, value_expr) = match (unparenthesized(lhs), unparenthesized(rhs)) {
                (Expr::Column(path), other) => (path, other),
                (other, Expr::Column(path)) => (path, other),
                _ => return false,
            };
            let Some(name) = column.last() else {
                return false;
            };
            // A `?` marker IS a literal once its parameter arrived (Go
            // `ParamMarkerExpression` reads the value the marker carries);
            // every other shape must resolve to a constant exactly as
            // [`name_value_pairs`] demands — anything needing evaluation
            // against a row is not a point-get key.
            let (value, marker) = if let Expr::ParamMarker {
                order,
                in_execute,
                value,
                ..
            } = unparenthesized(value_expr)
            {
                let value = value.as_ref().and_then(|value| value.downcast_ref::<Datum>());
                let datum = match (params.get(*order), in_execute, value) {
                    (Some(datum), _, _) => datum.clone(),
                    (None, true, Some(datum)) => datum.clone(),
                    _ => return false,
                };
                (datum, Some(*order))
            } else {
                let Ok(value) = rewrite_expr_resolved(
                    value_expr,
                    &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
                ) else {
                    return false;
                };
                let Expression::Constant(constant) = value else {
                    return false;
                };
                // Go keys only a literal or a marker: a constant built from a
                // parameter or a deferred function (`NOW()`) is an
                // expression, whose value a cached point plan cannot replay.
                if constant.param_marker.is_some() || constant.deferred_expr.is_some() {
                    return false;
                }
                let Ok(value) = constant.eval() else {
                    return false;
                };
                (value, None)
            };
            let mut pair = NameValuePair {
                column: name.clone(),
                table: column
                    .len()
                    .checked_sub(2)
                    .map(|position| column[position].clone()),
                value,
                marker,
            };
            if !visit(&mut pair) {
                return false;
            }
            pairs.push(pair);
            true
        }
        _ => false,
    }
}

/// The clauses [`try_point_get`] decides a point plan from.
///
/// This exists because Go decides a WRITE's point plan from the SAME
/// function as a read's: `tryUpdatePointPlan`/`tryDeletePointPlan`
/// (`pkg/planner/core/point_get_plan.go`) build an `ast.SelectStmt` out of
/// the write's own `TableRefs`/`Where`/`Order`/`Limit` and hand it to
/// `tryPointGetPlan`. This struct IS that synthesis, expressed as the field
/// copy Go performs rather than as a second point-plan builder -- there is
/// one rule here and one implementation of it, and a write cannot drift from
/// a read about which statements are point plans.
pub struct PointPlanStmt<'a> {
    where_clause: Option<&'a tidb_ast::Expr>,
    order_by: &'a [tidb_ast::OrderItem],
    limit: Option<&'a tidb_ast::Limit>,
    /// Go's synthesized statement carries no select list, so it has neither
    /// of these; only a real `SELECT` can.
    having: Option<&'a tidb_ast::Expr>,
    group_by: &'a [tidb_ast::GroupByItem],
    /// `DISTINCT` is present only on a real `SELECT`; writes set this false.
    distinct: bool,
    /// Go's `DataSource.PartitionNames`: the statement's own
    /// `PARTITION (p, ...)` list, EXACTLY as written and before any pruning.
    ///
    /// It is a point-plan input rather than a read restriction because
    /// `find_best_task.go`'s point-get conversion tests its LENGTH: "Partition
    /// table can't use `_tidb_rowid` to generate PointGet Plan unless one
    /// partition is explicitly specified" -- `len(ds.PartitionNames) != 1`
    /// disables the conversion. What the `WHERE` happened to prune is NOT
    /// that list, which is why the restricted `KvTable` cannot answer this.
    named_partitions: &'a [String],
    /// The single write target Go copies into the SELECT-shaped fast-plan
    /// input. The ordinary write path uses the same table reference when it
    /// enters the shared logical planner, preserving alias, partition, and
    /// index-hint identity.
    table_ref: Option<&'a tidb_ast::TableRef>,
}

impl<'a> PointPlanStmt<'a> {
    /// A `SELECT`'s own clauses.
    pub fn of_select(select: &'a tidb_ast::SelectStmt) -> Self {
        PointPlanStmt {
            where_clause: select.where_clause.as_ref(),
            order_by: &select.order_by,
            limit: select.limit.as_ref(),
            having: select.having.as_ref(),
            group_by: &select.group_by,
            distinct: select.distinct,
            named_partitions: sole_table_ref(&select.from)
                .map_or(&[][..], |table_ref| table_ref.partitions.as_slice()),
            table_ref: sole_table_ref(&select.from),
        }
    }
}

/// The row a point get reads, when the statement qualifies for one.
///
/// Go `TryFastPlan`/`tryPointGetPlan`: a single-table statement with no
/// `HAVING` and no `ORDER BY`, whose `WHERE` is a conjunction of equalities
/// that pins either the handle or every column of a unique index, reads one
/// row directly instead of scanning. `LIMIT` is allowed only when it cannot
/// remove the row (`count > 0` and `offset == 0`), matching Go's check.
///
/// Returns `Ok(None)` when the statement does not qualify, so the caller
/// falls back to the ordinary scan.
/// What pinned a point get: Go `PointGetPlan`'s split between a HANDLE plan
/// and an INDEX plan, which is exactly what its `AccessObject` prints --
/// `table:t handle:N` for the first, `table:t, index:idx(cols)` for the
/// second, never both.
#[derive(Clone, Debug)]
pub struct PointGetPin {
    /// The exact handle or unique-index values retained by Go's point plan.
    pub key_values: Vec<Datum>,
    /// The selected unique-index id, or `None` for a record-handle lookup.
    pub index_id: Option<i64>,
    /// Go `HandleConstant` / `IndexConstants`: the EXECUTE parameter each
    /// key part came from, aligned with [`Self::key_values`].
    pub key_markers: Vec<Option<usize>>,
    /// The key columns' types, which a cached plan moves each execution's
    /// parameters into.
    pub key_types: Vec<FieldType>,
}

/// Go `tryPointGetPlan`'s pin (see the doc above [`PointGetPin`]).
///
/// With `statement`, the constants are converted the way Go's fast plan
/// converts them: under the statement's type context, pair by pair as
/// `getNameValuePairs` walks the `WHERE`, reporting through the statement
/// (see [`crate::point_get_value::name_value_pair_value`]), and
/// `table_dual` is set when that walk ended on Go's `isTableDual`
/// (`ErrOverflow`). Without it the detached rule converts every pair
/// silently.
pub fn try_point_get(
    select: &PointPlanStmt<'_>,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
    statement: Option<&dyn tidb_exprctx::Columns>,
    table_dual: &mut bool,
) -> Result<Option<PointGetPin>, DriverError> {
    if select.having.is_some() || !select.order_by.is_empty() || !select.group_by.is_empty() {
        return Ok(None);
    }
    if let Some(limit) = select.limit {
        let count = eval_limit_bound(&limit.count)?;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)?,
            None => 0,
        };
        if count == 0 || offset > 0 {
            return Ok(None);
        }
    }
    let Some(where_clause) = select.where_clause else {
        return Ok(None);
    };
    let mut pairs = Vec::new();
    let walked = match statement {
        Some(ctx) => {
            let visible = select.table_ref.map(|table_ref| {
                table_ref
                    .alias
                    .as_deref()
                    .or_else(|| table_ref.name.last().map(String::as_str))
                    .unwrap_or_default()
            });
            point_equal_pairs_visiting(where_clause, &mut pairs, zone, &[], &mut |pair| {
                // Go declines a foreign qualifier before converting the pair.
                if let (Some(visible), Some(qualifier)) = (visible, pair.table.as_deref()) {
                    if !qualifier.go_lower_eq(visible) {
                        return false;
                    }
                }
                // A pair whose column Go does not find stays unconverted
                // (`types.NewFieldType(mysql.TypeLonglong)`); the handle arm
                // below reads `_tidb_rowid` from it, and any other unknown
                // column declines after the walk.
                let Some((_, field_type)) = columns
                    .iter()
                    .find(|(name, _)| name.go_lower_eq(&pair.column))
                else {
                    return true;
                };
                match crate::point_get_value::name_value_pair_value(
                    ctx,
                    field_type,
                    &pair.value,
                ) {
                    crate::point_get_value::NameValuePairValue::Key(value) => {
                        pair.value = value;
                        true
                    }
                    crate::point_get_value::NameValuePairValue::TableDual => {
                        *table_dual = true;
                        false
                    }
                    crate::point_get_value::NameValuePairValue::Decline => false,
                }
            })
        }
        None => name_value_pairs(where_clause, &mut pairs, zone),
    };
    if !walked || pairs.is_empty() {
        return Ok(None);
    }
    // Go `findPKHandle`'s `!tblInfo.PKIsHandle` branch: when the table has no
    // primary-key handle, the pair naming `_tidb_rowid` IS the handle pair,
    // and its type is `TypeLonglong` rather than any stored column's -- which
    // is why this runs BEFORE the column-domain conversion below, where
    // `_tidb_rowid` names nothing to convert against.
    //
    // Go refuses it for a PARTITIONED table (`point_get_plan.go`: "Partition
    // table can't use `_tidb_rowid` to generate PointGet Plan"), because a row
    // id alone does not say which partition holds the row -- UNLESS the
    // statement said which one. `find_best_task.go`'s point-get conversion
    // carries the exception verbatim:
    //
    // ```go
    // // Partition table can't use `_tidb_rowid` to generate PointGet Plan
    // // unless one partition is explicitly specified.
    // if canConvertPointGet && path.IsIntHandlePath &&
    //     !ds.Table.Meta().PKIsHandle && len(ds.PartitionNames) != 1 {
    //     canConvertPointGet = false
    // }
    // ```
    //
    // The test is on the WRITTEN list's length, not on how many partitions
    // survived: TiDB's own recording gives `Point_Get table:t, partition:p0`
    // for `select *,_tidb_rowid from t partition(p0) where _tidb_rowid=1`,
    // and a `TableRangeScan` for both the bare form and `partition(p0,p1)`.
    // Go `getNameValuePairs`: a column qualified by any name other than the
    // table's alias (or its name when it has none) declines the fast plan,
    // and the ordinary planner then reports the unknown column (1054).
    if let Some(table_ref) = select.table_ref {
        let visible = table_ref
            .alias
            .as_deref()
            .or_else(|| table_ref.name.last().map(String::as_str))
            .unwrap_or_default();
        if pairs.iter().any(|pair| {
            pair.table
                .as_deref()
                .is_some_and(|qualifier| !qualifier.go_lower_eq(visible))
        }) {
            return Ok(None);
        }
    }
    let single_named_partition = select.named_partitions.len() == 1;
    if table.pk_handle_offset().is_none()
        && table.common_handle_offsets().is_empty()
        && (table.partition().is_none() || single_named_partition)
        && pairs.len() == 1
        && pairs[0]
            .column
            .go_lower_eq(tidb_model::column::EXTRA_HANDLE_NAME)
    {
        let handle_type = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let Some(value) = point_get_value(&handle_type, &pairs[0].value) else {
            return Ok(None);
        };
        return Ok(Some(PointGetPin {
            key_values: vec![value],
            index_id: None,
            key_markers: vec![pairs[0].marker],
            key_types: vec![handle_type],
        }));
    }

    // Go `getNameValuePairs` moves every constant into its column's domain
    // before the pair is usable as a key, and abandons the whole point plan
    // when one of them will not survive the round trip. Doing it once for
    // every pair is what keeps the handle arm below dealing only in integers
    // and the unique-index arm dealing only in column-typed values; the
    // statement walk above already did it.
    if statement.is_some() {
        if pairs.iter().any(|pair| {
            !columns
                .iter()
                .any(|(name, _)| name.go_lower_eq(&pair.column))
        }) {
            return Ok(None);
        }
    } else if !convert_pairs_to_column_domain(&mut pairs, columns) {
        return Ok(None);
    }

    // The handle path: the primary key pinned by exactly one equality, which
    // is Go's `len(pairs) == 1` condition on the handle pair.
    if let Some(handle_offset) = table.pk_handle_offset() {
        let handle_column = &columns[handle_offset].0;
        if pairs.len() == 1 && pairs[0].column.go_lower_eq(handle_column) {
            return Ok(Some(PointGetPin {
                key_values: vec![pairs[0].value.clone()],
                index_id: None,
                key_markers: vec![pairs[0].marker],
                key_types: vec![columns[handle_offset].1.clone()],
            }));
        }
        // Go's `else if handlePair.value.Kind() != KindNull { return nil }`:
        // once a HANDLE pair exists among the conjuncts, the unique-index arm
        // is never tried -- the fast point plan is refused outright and the
        // ordinary planner takes over, whose `convertToPointGet` prints the
        // bare handle plan (`Point_Get table:t`) with the extra conjunct as a
        // filter. Falling through to the unique index here instead printed
        // `index:i(i, j)` where TiDB's recorded plan names no index.
        if pairs
            .iter()
            .any(|pair| pair.column.go_lower_eq(handle_column))
        {
            return Ok(None);
        }
    }

    // A clustered composite primary key is encoded directly as a common
    // handle rather than materialized as a secondary `KvIndex`. When every
    // handle column is pinned, it is the same one-row lookup as an integer
    // point get; extra equalities remain in the filter above the source.
    let common_offsets = table.common_handle_offsets().to_vec();
    // Go `getIndexValues` declines the clustered primary key too when it has
    // a prefix column: its handle holds only the leading part of the value.
    let prefix_common_handle = table
        .common_handle_prefix_lengths()
        .iter()
        .any(|length| *length != tidb_datatype::UNSPECIFIED_LENGTH);
    if !common_offsets.is_empty() && !prefix_common_handle {
        let mut values = Vec::with_capacity(common_offsets.len());
        let mut markers = Vec::with_capacity(common_offsets.len());
        let mut types = Vec::with_capacity(common_offsets.len());
        for offset in common_offsets {
            let Some((name, field_type)) = columns.get(offset) else {
                values.clear();
                break;
            };
            let Some(pair) = pairs.iter().find(|pair| pair.column.go_lower_eq(name)) else {
                values.clear();
                break;
            };
            values.push(pair.value.clone());
            markers.push(pair.marker);
            types.push(field_type.clone());
        }
        if values.len() == table.common_handle_offsets().len() {
            // A clustered common handle IS the record key, so it prints as
            // a handle plan, not an index one.
            return Ok(Some(PointGetPin {
                key_values: values,
                index_id: None,
                key_markers: markers,
                key_types: types,
            }));
        }
    }

    // The unique-index path: every column of some unique index is pinned.
    for index in table.point_get_indexes() {
        if !index.unique {
            continue;
        }
        // Go `point_get_plan.go` declines an index with `HasPrefixIndex()`:
        // an entry found by a CUT value does not prove the row matches, and a
        // point get has no residual predicate to catch that. Skipping the
        // index here is load-bearing, not defensive -- `lookup_unique` fails
        // closed with `None`, which this loop would otherwise read as "no
        // such row" and answer zero rows for a row that exists.
        if index.has_prefix() {
            continue;
        }
        let mut values = Vec::with_capacity(index.column_offsets.len());
        let mut markers = Vec::with_capacity(index.column_offsets.len());
        let mut types = Vec::with_capacity(index.column_offsets.len());
        for offset in &index.column_offsets {
            // Go `getIndexValues` resolves each key part by NAME against the
            // `WHERE`'s pairs, so a key part the statement cannot name
            // declines the whole index. The hidden generated column an
            // EXPRESSION key part was rewritten into is exactly such a part:
            // it lives past the end of the scope's visible column list, and
            // `tidb_shard(a)` is not a name any `WHERE` writes. Resolving the
            // name through `get` makes "no visible column at that offset" and
            // "not pinned by the WHERE" the same answer -- without it the
            // offset indexes past the end and panics, which is what
            // `explain_shard_index`'s `where a=100` reached.
            let Some((pair, field_type)) = columns.get(*offset).and_then(|(name, field_type)| {
                pairs
                    .iter()
                    .find(|pair| pair.column.go_lower_eq(name))
                    .map(|pair| (pair, field_type))
            }) else {
                values.clear();
                break;
            };
            values.push(pair.value.clone());
            markers.push(pair.marker);
            types.push(field_type.clone());
        }
        if values.len() != index.column_offsets.len() {
            continue;
        }
        return Ok(Some(PointGetPin {
            key_values: values,
            index_id: Some(index.id),
            key_markers: markers,
            key_types: types,
        }));
    }
    Ok(None)
}

/// Evaluates a `LIMIT` bound, which must be a non-negative integer literal or
/// an execute-time integer parameter. Go keeps a `ParamMarkerExpr` in the
/// prepared AST and resolves its value during execution; the Rust prepared
/// path installs that same datum on [`tidb_ast::Expr::ParamMarker`].
pub fn eval_limit_bound(expr: &tidb_ast::Expr) -> Result<u64, DriverError> {
    match expr {
        tidb_ast::Expr::Int(text) => text
            .parse::<u64>()
            .map_err(|_| DriverError::unsupported("LIMIT bound must be a non-negative integer")),
        tidb_ast::Expr::ParamMarker {
            value: Some(value), ..
        } => match value.downcast_ref::<Datum>() {
            Some(Datum::Int(value)) if *value >= 0 => Ok(*value as u64),
            Some(Datum::UInt(value)) => Ok(*value),
            _ => Err(DriverError::unsupported(
                "LIMIT bound must be an integer literal",
            )),
        },
        _ => Err(DriverError::unsupported(
            "LIMIT bound must be an integer literal",
        )),
    }
}

/// Go `isCTE(tblW)` for a write statement's table ref: `buildDataSource`
/// resolves an unqualified name to a CTE of the statement's `WITH` before any
/// table of that name.
pub fn is_cte(with: Option<&tidb_ast::WithClause>, table: &tidb_ast::TableRef) -> bool {
    let [name] = table.name.as_slice() else {
        return false;
    };
    with.is_some_and(|with| with.ctes.iter().any(|cte| cte.name.go_lower_eq(name)))
}
