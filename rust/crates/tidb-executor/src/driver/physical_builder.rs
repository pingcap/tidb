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

//! Go `executorBuilder.build`: instantiate a retained physical SELECT tree.
//!
//! A prepared-plan cache hit has already run logical optimization, physical
//! enumeration, task attachment, and `RebuildPlan4CachedPlan`. This module is
//! deliberately a constructor switch over that resulting tree. It must not
//! inspect the SQL AST to choose access, aggregation, join, sort, or reader
//! policy; the AST is used only for client-visible result-column labels.

use crate::executor::{Executor, ExecutorMeta};
use crate::hash_agg::{AggFunc, AggKind, GroupedStreamAggExec, HashAggExec, StreamAggExec};
use crate::join::{JoinExec, JoinKind};
use crate::kv_table::{IndexRange, RowDecodeContext, TableScanExec};
use crate::limit::LimitExec;
use crate::projection::ProjectionExec;
use crate::remote_scan::PushdownStatementContext;
use crate::selection::SelectionExec;
use crate::sort::{SortByItem, SortExec};
use crate::table_access::TableAccess;
use crate::table_dual::TableDualExec;
use crate::topn::TopNExec;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::aggregation::{AggFuncDesc, AggFunctionMode};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;
use tidb_expr::schema::Schema;
use tidb_planner::find_best_task::LogicalJoinType;
use tidb_planner::physical::{PhysicalPlan, PhysicalTableScan};

use super::{Catalog, DriverError, SelectMeta, INIT_CAP, MAX_CHUNK_SIZE};

fn plan_schema(plan: &PhysicalPlan) -> Result<Schema, DriverError> {
    plan.base()
        .base
        .schema()
        .cloned()
        .ok_or_else(|| DriverError::unsupported("a cached physical operator has no schema"))
}

fn unary_schema(plan: &PhysicalPlan, child: &dyn Executor) -> Schema {
    plan.base()
        .base
        .schema()
        .cloned()
        .unwrap_or_else(|| child.schema().clone())
}

fn meta(plan: &PhysicalPlan, schema: Schema) -> ExecutorMeta {
    ExecutorMeta::new(
        schema,
        i64::from(plan.base().base.id()),
        INIT_CAP,
        MAX_CHUNK_SIZE,
    )
}

fn only_child(plan: &PhysicalPlan) -> Result<&PhysicalPlan, DriverError> {
    let [child] = plan.children() else {
        return Err(DriverError::unsupported(
            "a cached unary physical operator has the wrong child count",
        ));
    };
    Ok(child)
}

fn resolve_expression(
    mut expression: Expression,
    schema: &Schema,
) -> Result<Expression, DriverError> {
    super::planner_bridge::materialize_cached_expression(&mut expression);
    tidb_expr::simple_expr::resolve_indices_in_place(&mut expression, schema).map_err(|_| {
        DriverError::unsupported("a cached physical expression does not resolve in its child")
    })?;
    Ok(expression)
}

fn resolve_expressions(
    expressions: &[Expression],
    schema: &Schema,
) -> Result<Vec<Expression>, DriverError> {
    expressions
        .iter()
        .cloned()
        .map(|expression| resolve_expression(expression, schema))
        .collect()
}

fn executor_ranges(ranges: &tidb_planner::ranger::types::Ranges) -> Vec<IndexRange> {
    ranges
        .iter()
        .map(|range| IndexRange {
            low: range.low_val.clone(),
            high: range.high_val.clone(),
            low_exclusive: range.low_exclude,
            high_exclusive: range.high_exclude,
        })
        .collect()
}

fn table_scan_schema(
    scan: &PhysicalTableScan,
    table: &crate::KvTable,
) -> Result<(Schema, Vec<usize>), DriverError> {
    let output = scan
        .base
        .base
        .schema()
        .ok_or_else(|| DriverError::unsupported("a cached table scan has no schema"))?;
    let mut full = Vec::with_capacity(table.visible_column_count());
    for (index, column) in table.visible_columns().iter().enumerate() {
        let mut planned = scan
            .cost_columns
            .iter()
            .find(|planned| planned.id == column.id)
            .cloned()
            .unwrap_or_else(|| {
                let mut planned = Column::new(column.id, column.field_type.clone());
                planned.id = column.id;
                planned
            });
        planned.index = index as i64;
        full.push(planned);
    }
    let keep = output
        .columns
        .iter()
        .map(|wanted| {
            full.iter()
                .position(|column| {
                    column.unique_id == wanted.unique_id
                        || (wanted.id != 0 && column.id == wanted.id)
                })
                .ok_or_else(|| {
                    DriverError::unsupported(
                        "a cached table-scan output column is absent from its table",
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((Schema::new(full), keep))
}

fn build_table_scan(
    plan: &PhysicalPlan,
    scan: &PhysicalTableScan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let table = catalog
        .kv_table_by_id(scan.table_id)
        .ok_or_else(|| DriverError::unsupported("cached table ID is absent from the catalog"))?;
    let (schema, keep) = table_scan_schema(scan, table)?;
    let mut source = TableScanExec::new_with_context(
        meta(plan, schema),
        table.clone(),
        RowDecodeContext::for_query(ctx),
        PushdownStatementContext::from_stmt(ctx),
    );
    if keep.len() != table.visible_column_count()
        || keep.iter().copied().ne(0..table.visible_column_count())
    {
        if !source.accept_column_prune(&keep) {
            return Err(DriverError::unsupported(
                "the cached table scan cannot apply its physical projection",
            ));
        }
    }
    if scan.keep_order && !source.accept_keep_order(scan.desc) {
        return Err(DriverError::unsupported(
            "the cached table scan cannot preserve its physical order",
        ));
    }
    if !scan.ranges.is_empty() {
        let ranges = executor_ranges(&scan.ranges);
        if !source.accept_handle_ranges(&ranges) {
            return Err(DriverError::unsupported(
                "the cached table scan cannot apply its rebuilt ranges",
            ));
        }
    }
    if let Some(rows) = scan.base.base.stats_info().map(|stats| stats.row_count()) {
        source.accept_scan_estimate(rows);
    }
    Ok(Box::new(source))
}

fn aggregate_function(
    descriptor: &AggFuncDesc,
    child_schema: &Schema,
    ctx: &crate::StmtContext,
) -> Result<AggFunc, DriverError> {
    let mut args = resolve_expressions(&descriptor.base.args, child_schema)?;
    let upper = descriptor.base.name.to_ascii_uppercase();
    let mut kind = if matches!(upper.as_str(), "FIRST_ROW" | "FIRSTROW") {
        AggKind::FirstRow
    } else {
        super::agg_build::agg_kind_and_type_with_div_precision(
            &upper,
            &args,
            ctx.div_precision_increment(),
        )
        .map_err(|_| {
            DriverError::unsupported(format!(
                "cached physical aggregate `{upper}` is not executable"
            ))
        })?
        .0
    };
    if descriptor.mode == AggFunctionMode::Final && matches!(kind, AggKind::Count) {
        kind = AggKind::FinalCount;
    }
    let arg = (!args.is_empty()).then(|| args.remove(0));
    Ok(AggFunc {
        kind,
        arg,
        extra_args: args,
        order_by: descriptor
            .order_by_items
            .iter()
            .map(|item| {
                resolve_expression(item.expr.clone(), child_schema)
                    .map(|expression| (expression, item.desc))
            })
            .collect::<Result<Vec<_>, _>>()?,
        distinct: descriptor.has_distinct,
        arg_orig_name: String::new(),
    })
}

fn build_aggregation(
    plan: &PhysicalPlan,
    descriptors: &[AggFuncDesc],
    group_by: &[Expression],
    stream: bool,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let child = build(only_child(plan)?, catalog, ctx)?;
    let child_schema = child.schema().clone();
    let group_by = resolve_expressions(group_by, &child_schema)?;
    let functions = descriptors
        .iter()
        .map(|descriptor| aggregate_function(descriptor, &child_schema, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let executor_meta = meta(plan, plan_schema(plan)?);
    if stream {
        if group_by.is_empty() {
            Ok(Box::new(StreamAggExec::new(
                executor_meta,
                functions,
                child,
                ctx.clone(),
            )))
        } else {
            let output_positions = (0..functions.len()).collect();
            Ok(Box::new(GroupedStreamAggExec::new(
                executor_meta,
                group_by,
                functions,
                output_positions,
                child,
                ctx.clone(),
            )))
        }
    } else {
        Ok(Box::new(HashAggExec::new(
            executor_meta,
            group_by,
            functions,
            child,
            ctx.clone(),
            ctx.statement_memory(),
        )))
    }
}

fn build_reader(
    embedded: Option<&PhysicalPlan>,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    build(
        embedded.ok_or_else(|| DriverError::unsupported("a cached reader has no pushed plan"))?,
        catalog,
        ctx,
    )
}

fn join_kind(join_type: LogicalJoinType) -> Result<JoinKind, DriverError> {
    match join_type {
        LogicalJoinType::Inner => Ok(JoinKind::Inner),
        LogicalJoinType::LeftOuter => Ok(JoinKind::Left),
        LogicalJoinType::RightOuter => Ok(JoinKind::Right),
        LogicalJoinType::Semi => Ok(JoinKind::Semi),
        LogicalJoinType::AntiSemi => Ok(JoinKind::AntiSemi),
        LogicalJoinType::LeftOuterSemi => Ok(JoinKind::LeftOuterSemi),
        LogicalJoinType::AntiLeftOuterSemi => Err(DriverError::unsupported(
            "cached anti-left-outer-semi join execution is not implemented",
        )),
    }
}

fn filtered_join_child(
    plan: &PhysicalPlan,
    child: Box<dyn Executor>,
    conditions: &[Expression],
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    if conditions.is_empty() {
        return Ok(child);
    }
    let schema = child.schema().clone();
    let filters = resolve_expressions(conditions, &schema)?;
    Ok(Box::new(SelectionExec::new(
        meta(plan, schema),
        filters,
        child,
        ctx.clone(),
        ctx.statement_memory(),
    )))
}

fn join_equality(
    left: &Column,
    right: &Column,
    schema: &Schema,
) -> Result<Expression, DriverError> {
    let expression = Expression::ScalarFunction(ScalarFunction::new(
        tidb_ast::CiString::new("eq"),
        FieldType::new(FieldTypeCode::Tiny),
        vec![
            Expression::Column(left.clone()),
            Expression::Column(right.clone()),
        ],
    ));
    resolve_expression(expression, schema)
}

#[allow(clippy::too_many_arguments)]
fn build_join(
    plan: &PhysicalPlan,
    join_type: LogicalJoinType,
    left_join_keys: &[Column],
    right_join_keys: &[Column],
    left_conditions: &[Expression],
    right_conditions: &[Expression],
    other_conditions: &[Expression],
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<JoinExec<crate::StmtContext>, DriverError> {
    let [left_plan, right_plan] = plan.children() else {
        return Err(DriverError::unsupported(
            "a cached physical join has the wrong child count",
        ));
    };
    let left = filtered_join_child(plan, build(left_plan, catalog, ctx)?, left_conditions, ctx)?;
    let right = filtered_join_child(
        plan,
        build(right_plan, catalog, ctx)?,
        right_conditions,
        ctx,
    )?;
    let condition_schema =
        tidb_expr::schema::merge_schema(Some(left.schema()), Some(right.schema()))
            .ok_or_else(|| DriverError::unsupported("a cached join has no condition schema"))?;
    let mut conditions = left_join_keys
        .iter()
        .zip(right_join_keys)
        .map(|(left, right)| join_equality(left, right, &condition_schema))
        .collect::<Result<Vec<_>, _>>()?;
    conditions.extend(resolve_expressions(other_conditions, &condition_schema)?);
    Ok(JoinExec::new(
        meta(plan, plan_schema(plan)?),
        join_kind(join_type)?,
        conditions,
        left,
        right,
        ctx.clone(),
        ctx.statement_memory(),
    ))
}

fn join_key_offsets(
    left_keys: &[Column],
    right_keys: &[Column],
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<Vec<crate::merge_join_plan::MergeJoinKey>, DriverError> {
    left_keys
        .iter()
        .zip(right_keys)
        .map(|(left, right)| {
            let left = left_schema
                .columns
                .iter()
                .position(|column| column.unique_id == left.unique_id)
                .ok_or_else(|| {
                    DriverError::unsupported("a cached merge key is absent on the left")
                })?;
            let right = right_schema
                .columns
                .iter()
                .position(|column| column.unique_id == right.unique_id)
                .ok_or_else(|| {
                    DriverError::unsupported("a cached merge key is absent on the right")
                })?;
            Ok(crate::merge_join_plan::MergeJoinKey { left, right })
        })
        .collect()
}

/// Recursively instantiates one retained physical operator tree.
pub(super) fn build(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    match plan {
        PhysicalPlan::TableScan(scan) => build_table_scan(plan, scan, catalog, ctx),
        PhysicalPlan::TableReader(reader) => {
            build_reader(reader.table_plan.as_deref(), catalog, ctx)
        }
        PhysicalPlan::Projection(projection) => {
            let child = build(only_child(plan)?, catalog, ctx)?;
            let expressions = resolve_expressions(&projection.exprs, child.schema())?;
            Ok(Box::new(ProjectionExec::new(
                meta(plan, plan_schema(plan)?),
                expressions,
                child,
                ctx.clone(),
            )))
        }
        PhysicalPlan::Selection(selection) => {
            let child = build(only_child(plan)?, catalog, ctx)?;
            let filters = resolve_expressions(&selection.conditions, child.schema())?;
            let schema = unary_schema(plan, child.as_ref());
            Ok(Box::new(SelectionExec::new(
                meta(plan, schema),
                filters,
                child,
                ctx.clone(),
                ctx.statement_memory(),
            )))
        }
        PhysicalPlan::Limit(limit) => {
            let child = build(only_child(plan)?, catalog, ctx)?;
            let schema = unary_schema(plan, child.as_ref());
            Ok(Box::new(LimitExec::new(
                meta(plan, schema),
                limit.offset,
                limit.count,
                child,
            )))
        }
        PhysicalPlan::Sort(sort) => {
            let child = build(only_child(plan)?, catalog, ctx)?;
            let by_items = sort
                .by_items
                .iter()
                .map(|item| {
                    child
                        .schema()
                        .columns
                        .iter()
                        .find(|column| column.unique_id == item.col)
                        .cloned()
                        .map(|column| SortByItem {
                            expr: Expression::Column(column),
                            desc: item.desc,
                        })
                        .ok_or_else(|| {
                            DriverError::unsupported(
                                "a cached physical sort column is absent from its child",
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let schema = unary_schema(plan, child.as_ref());
            Ok(Box::new(
                SortExec::new(
                    meta(plan, schema),
                    by_items,
                    child,
                    ctx.clone(),
                    ctx.statement_memory(),
                )
                .with_parallelism(ctx.executor_concurrency()),
            ))
        }
        PhysicalPlan::TopN(topn) => {
            let child = build(only_child(plan)?, catalog, ctx)?;
            let by_items = topn
                .by_items
                .iter()
                .map(|item| {
                    resolve_expression(item.expr.clone(), child.schema()).map(|expr| SortByItem {
                        expr,
                        desc: item.desc,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let schema = unary_schema(plan, child.as_ref());
            Ok(Box::new(
                TopNExec::new(
                    meta(plan, schema),
                    by_items,
                    child,
                    ctx.clone(),
                    topn.offset,
                    topn.count,
                    ctx.statement_memory(),
                )
                .with_parallelism(ctx.executor_concurrency()),
            ))
        }
        PhysicalPlan::HashAgg(aggregation) => build_aggregation(
            plan,
            &aggregation.agg_funcs,
            &aggregation.group_by_items,
            false,
            catalog,
            ctx,
        ),
        PhysicalPlan::StreamAgg(aggregation) => build_aggregation(
            plan,
            &aggregation.agg_funcs,
            &aggregation.group_by_items,
            true,
            catalog,
            ctx,
        ),
        PhysicalPlan::HashJoin(join) => {
            let mut executor = build_join(
                plan,
                join.join_type,
                &join.left_join_keys,
                &join.right_join_keys,
                &join.left_conditions,
                &join.right_conditions,
                &join.other_conditions,
                catalog,
                ctx,
            )?;
            let build_child_idx = if join.use_outer_to_build {
                1usize.checked_sub(join.inner_child_idx).ok_or_else(|| {
                    DriverError::unsupported("a cached hash join has an invalid inner child")
                })?
            } else {
                join.inner_child_idx
            };
            executor.set_hash_build_is_left(build_child_idx == 0);
            Ok(Box::new(executor))
        }
        PhysicalPlan::MergeJoin(join) => {
            let [left, right] = plan.children() else {
                return Err(DriverError::unsupported(
                    "a cached merge join has the wrong child count",
                ));
            };
            let keys = join_key_offsets(
                &join.left_join_keys,
                &join.right_join_keys,
                &plan_schema(left)?,
                &plan_schema(right)?,
            )?;
            let mut executor = build_join(
                plan,
                join.join_type,
                &join.left_join_keys,
                &join.right_join_keys,
                &join.left_conditions,
                &join.right_conditions,
                &join.other_conditions,
                catalog,
                ctx,
            )?;
            executor.set_merge_plan(crate::merge_join_plan::MergeJoinPlan {
                keys,
                desc: join.desc,
            });
            Ok(Box::new(executor))
        }
        PhysicalPlan::TableDual(dual) => Ok(Box::new(TableDualExec::new(
            meta(plan, plan_schema(plan)?),
            dual.row_count,
        ))),
        PhysicalPlan::NominalSort(_) => build(only_child(plan)?, catalog, ctx),
        _ => Err(DriverError::unsupported(format!(
            "cached physical executor construction for {} is not implemented",
            plan.base().base.tp()
        ))),
    }
}

fn schema_column_name(column: &Column, ordinal: usize) -> String {
    column
        .orig_name
        .rsplit('.')
        .find(|part| !part.is_empty())
        .map_or_else(|| format!("Column#{}", ordinal + 1), str::to_owned)
}

fn result_columns(select: &tidb_ast::SelectStmt, schema: &Schema) -> Vec<(String, FieldType)> {
    let expression_names = select
        .fields
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| match field {
            tidb_ast::SelectField::Expr { expr, alias } => {
                Some(alias.clone().unwrap_or_else(|| {
                    super::default_field_display_name(&select.fields, index, expr)
                }))
            }
            tidb_ast::SelectField::Wildcard(_) => None,
        })
        .collect::<Vec<_>>();
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let name = if expression_names.len() == schema.len() {
                expression_names[index].clone()
            } else {
                schema_column_name(column, index)
            };
            let field_type = column
                .ret_type
                .clone()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            (name, field_type)
        })
        .collect()
}

/// Builds and drains the rebuilt physical tree without entering the AST
/// optimizer/lowering pipeline.
pub(super) fn run_cached_select(
    select: &tidb_ast::SelectStmt,
    physical: &PhysicalPlan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    let root = build(physical, catalog, ctx)?;
    let columns = result_columns(select, root.schema());
    super::drain_root_executor(root, columns, ctx)
}
