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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go-shaped `EXPLAIN` and `EXPLAIN ANALYZE` over retained physical plans.
//!
//! Fresh queries, prepared cache hits, and DML all render the same physical
//! tree that `physical_builder` lowers through the ordinary executor switch.
//! `EXPLAIN ANALYZE` attaches row counters while draining that executor tree;
//! plain `EXPLAIN` only renders it and never executes the target statement.

use std::cell::Cell;
use std::rc::Rc;

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_planner::explain::{
    Explain as PlannerExplain, ExplainContext as PlannerExplainContext,
    ExplainFormat as PlannerExplainFormat, ExplainOperator, ExplainTask,
};
use tidb_planner::physical::{PhysicalPlan, RedactMode};

use crate::driver::{
    run_delete_stmt_with_physical_and_stats, run_insert_stmt_with_physical_and_stats,
    run_update_stmt_with_physical_and_stats, Catalog, DriverError, SelectMeta,
};
/// The `EXPLAIN FORMAT = '...'` this tier accepts. Go's `'row'` (the
/// default, also the explicit spelling) and `'brief'` render the identical
/// five-column tree; `'brief'` merely drops each operator's `_N` build-order
/// suffix. `'plan_tree'` is Go's four-column tree shape (it omits `estRows`),
/// which is the format used by the TPC-DS source test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainFormat {
    /// Go's default: every operator id carries its `_N` build-order suffix.
    Row,
    /// Same tree, ids printed without the `_N` suffix.
    Brief,
    /// Go's four-column tree format, without estimates or id suffixes.
    PlanTree,
}

impl ExplainFormat {
    /// Parses Go's `ExplainStmt.Format` string, case-insensitively as Go's
    /// preprocessor does. `None` for a format this tier does not recognize
    /// -- the caller reports Go's own "Unknown EXPLAIN format name" error
    /// (captured verbatim from `explain format = 'bogus' ...`).
    pub fn parse(format: &str) -> Option<Self> {
        if format.eq_ignore_ascii_case("row") {
            Some(Self::Row)
        } else if format.eq_ignore_ascii_case("brief") {
            Some(Self::Brief)
        } else if format.eq_ignore_ascii_case("plan_tree") {
            Some(Self::PlanTree)
        } else {
            None
        }
    }
}

/// The plan a finished trace recorded, or the refusal a build site left in it.
fn planner_explain_format(format: ExplainFormat) -> PlannerExplainFormat {
    match format {
        ExplainFormat::Row => PlannerExplainFormat::Row,
        ExplainFormat::Brief => PlannerExplainFormat::Brief,
        ExplainFormat::PlanTree => PlannerExplainFormat::PlanTree,
    }
}

fn expression_text(expression: &tidb_expr::expression::Expression) -> String {
    crate::plan_trace::physical_expression_text_with_columns(expression, &[]).unwrap_or_default()
}

/// Go appends `stats:pseudo` only when a scan's `StatsInfo.StatsVersion` is
/// `statistics.PseudoVersion` (and no statement-specific `UsedStatsInfo`
/// replaces it). Rust carries that same bit on the retained histogram
/// collection.
fn scan_uses_pseudo_statistics(base: &tidb_planner::physical::BasePhysicalPlan) -> bool {
    base.base
        .stats_info()
        .and_then(tidb_planner::stats_info::StatsInfo::hist_coll)
        .is_none_or(tidb_planner::stats_info::HistColl::pseudo)
}

fn expressions_text(expressions: &[tidb_expr::expression::Expression]) -> String {
    expressions
        .iter()
        .map(expression_text)
        .collect::<Vec<_>>()
        .join(", ")
}

fn by_items_text(items: &[tidb_expr::aggregation::ByItems]) -> String {
    items
        .iter()
        .map(|item| {
            let expression = expression_text(&item.expr);
            if item.desc {
                format!("{expression}:desc")
            } else {
                expression
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn join_type_text(join_type: tidb_planner::find_best_task::LogicalJoinType) -> &'static str {
    use tidb_planner::find_best_task::LogicalJoinType;
    match join_type {
        LogicalJoinType::Inner => "inner join",
        LogicalJoinType::LeftOuter => "left outer join",
        LogicalJoinType::RightOuter => "right outer join",
        LogicalJoinType::Semi => "semi join",
        LogicalJoinType::AntiSemi => "anti semi join",
        LogicalJoinType::LeftOuterSemi => "left outer semi join",
        LogicalJoinType::AntiLeftOuterSemi => "anti left outer semi join",
    }
}

fn join_info(
    join_type: tidb_planner::find_best_task::LogicalJoinType,
    left_keys: &[tidb_expr::column::Column],
    right_keys: &[tidb_expr::column::Column],
    left_conditions: &[tidb_expr::expression::Expression],
    right_conditions: &[tidb_expr::expression::Expression],
    other_conditions: &[tidb_expr::expression::Expression],
) -> String {
    let mut parts = vec![join_type_text(join_type).to_owned()];
    let equal = left_keys
        .iter()
        .zip(right_keys)
        .map(|(left, right)| {
            format!(
                "eq({}, {})",
                expression_text(&tidb_expr::expression::Expression::Column(left.clone())),
                expression_text(&tidb_expr::expression::Expression::Column(right.clone()))
            )
        })
        .collect::<Vec<_>>();
    if !equal.is_empty() {
        parts.push(format!("equal:[{}]", equal.join(" ")));
    }
    for (name, conditions) in [
        ("left cond", left_conditions),
        ("right cond", right_conditions),
        ("other cond", other_conditions),
    ] {
        if !conditions.is_empty() {
            parts.push(format!("{name}:[{}]", expressions_text(conditions)));
        }
    }
    parts.join(", ")
}

fn aggregate_info(
    functions: &[tidb_expr::aggregation::AggFuncDesc],
    group_by: &[tidb_expr::expression::Expression],
    schema: Option<&tidb_expr::schema::Schema>,
) -> String {
    let mut parts = Vec::new();
    if !group_by.is_empty() {
        parts.push(format!("group by:{}", expressions_text(group_by)));
    }
    if !functions.is_empty() {
        let functions = functions
            .iter()
            .enumerate()
            .map(|(index, function)| {
                let distinct = if function.has_distinct {
                    "distinct "
                } else {
                    ""
                };
                let output = schema
                    .and_then(|schema| schema.columns.get(index))
                    .map(|column| {
                        expression_text(&tidb_expr::expression::Expression::Column(column.clone()))
                    })
                    .unwrap_or_else(|| format!("Column#{index}"));
                format!(
                    "funcs:{}({}{})->{output}",
                    function.base.name,
                    distinct,
                    expressions_text(&function.base.args)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        parts.push(functions);
    }
    parts.join(", ")
}

fn scan_ranges_text(ranges: &tidb_planner::ranger::types::Ranges) -> String {
    ranges
        .iter()
        .map(tidb_planner::ranger::types::Range::to_display_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn table_name(catalog: &Catalog, table_id: i64, alias: Option<&str>) -> String {
    alias.map_or_else(
        || {
            catalog
                .kv_table_by_id(table_id)
                .map_or_else(|| table_id.to_string(), |table| table.name.clone())
        },
        str::to_owned,
    )
}

fn table_access(catalog: &Catalog, table_id: i64, alias: Option<&str>) -> String {
    format!("table:{}", table_name(catalog, table_id, alias))
}

fn index_access(
    catalog: &Catalog,
    table_id: i64,
    alias: Option<&str>,
    index_id: i64,
    index_name: Option<&str>,
) -> String {
    let table_access = table_access(catalog, table_id, alias);
    let Some(table) = catalog.kv_table_by_id(table_id) else {
        return index_name.map_or(table_access.clone(), |name| {
            format!("{table_access}, index:{name}")
        });
    };
    let Some(index) = table.plan_indexes().find(|index| index.id == index_id) else {
        return index_name.map_or(table_access.clone(), |name| {
            format!("{table_access}, index:{name}")
        });
    };
    let columns = index
        .column_offsets
        .iter()
        .filter_map(|offset| table.columns.get(*offset))
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    format!("{table_access}, index:{}({columns})", index.name)
}

fn point_access(catalog: &Catalog, table_id: i64, index_id: Option<i64>) -> String {
    match index_id {
        Some(index_id) => index_access(catalog, table_id, None, index_id, None),
        None => {
            let access = table_access(catalog, table_id, None);
            let Some(table) = catalog.kv_table_by_id(table_id) else {
                return access;
            };
            if table.common_handle_offsets().is_empty() {
                access
            } else {
                let columns = table
                    .common_handle_offsets()
                    .iter()
                    .filter_map(|offset| table.columns.get(*offset))
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{access}, clustered index:PRIMARY({columns})")
            }
        }
    }
}

fn physical_operator_name(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::TableScan(scan) => scan.scan_kind().map_or_else(
            || "TableScan".to_owned(),
            |kind| kind.plan_type().to_owned(),
        ),
        PhysicalPlan::IndexScan(scan) => {
            if scan.ranges.is_empty()
                || tidb_planner::ranger::types::has_full_range(&scan.ranges, false)
            {
                "IndexFullScan".to_owned()
            } else {
                "IndexRangeScan".to_owned()
            }
        }
        PhysicalPlan::PointGet(_) => "Point_Get".to_owned(),
        PhysicalPlan::BatchPointGet(_) => "Batch_Point_Get".to_owned(),
        _ => plan.tp().to_owned(),
    }
}

fn physical_access(plan: &PhysicalPlan, catalog: &Catalog) -> String {
    match plan {
        PhysicalPlan::TableScan(scan) => {
            table_access(catalog, scan.table_id, scan.table_as_name.as_deref())
        }
        PhysicalPlan::MemTable(scan) => format!("table:{}", scan.table_name),
        PhysicalPlan::IndexScan(scan) => index_access(
            catalog,
            scan.table_id,
            scan.table_as_name.as_deref(),
            scan.index_id,
            Some(&scan.index_name),
        ),
        PhysicalPlan::PointGet(point) => point_access(catalog, point.table_id, point.index_id),
        PhysicalPlan::BatchPointGet(point) => point_access(catalog, point.table_id, point.index_id),
        PhysicalPlan::CTE(cte) => {
            if cte.cte_name.eq_ignore_ascii_case(&cte.cte_as_name) {
                format!("CTE:{}", cte.cte_name.to_ascii_lowercase())
            } else {
                format!(
                    "CTE:{} AS {}",
                    cte.cte_name.to_ascii_lowercase(),
                    cte.cte_as_name.to_ascii_lowercase()
                )
            }
        }
        _ => String::new(),
    }
}

fn point_handle_text(range: &tidb_planner::ranger::types::Range) -> String {
    range
        .low_val
        .first()
        .and_then(|value| value.sql_string().ok())
        .unwrap_or_default()
}

fn physical_operator_info(plan: &PhysicalPlan, catalog: &Catalog) -> String {
    match plan {
        PhysicalPlan::Selection(selection) => expressions_text(&selection.conditions),
        PhysicalPlan::Projection(projection) => expressions_text(&projection.exprs),
        PhysicalPlan::HashJoin(join) => join_info(
            join.join_type,
            &join.left_join_keys,
            &join.right_join_keys,
            &join.left_conditions,
            &join.right_conditions,
            &join.other_conditions,
        ),
        PhysicalPlan::MergeJoin(join) => join_info(
            join.join_type,
            &join.left_join_keys,
            &join.right_join_keys,
            &join.left_conditions,
            &join.right_conditions,
            &join.other_conditions,
        ),
        PhysicalPlan::IndexJoin(join) => join_info(
            join.join_type,
            &join.left_join_keys,
            &join.right_join_keys,
            &join.left_conditions,
            &join.right_conditions,
            &join.other_conditions,
        ),
        PhysicalPlan::Apply(apply) => join_info(
            apply.hash_join.join_type,
            &apply.hash_join.left_join_keys,
            &apply.hash_join.right_join_keys,
            &apply.hash_join.left_conditions,
            &apply.hash_join.right_conditions,
            &apply.hash_join.other_conditions,
        ),
        PhysicalPlan::Sort(sort) => by_items_text(&sort.by_items),
        PhysicalPlan::Limit(limit) => limit.explain_info(RedactMode::Disable),
        PhysicalPlan::TableScan(scan) => {
            let mut parts = Vec::new();
            if scan
                .scan_kind()
                .is_some_and(|kind| kind.plan_type() == "TableRangeScan")
                && !scan.ranges.is_empty()
            {
                parts.push(format!("range:{}", scan_ranges_text(&scan.ranges)));
            }
            parts.push(format!("keep order:{}", scan.keep_order));
            if scan_uses_pseudo_statistics(&scan.base) {
                parts.push("stats:pseudo".to_owned());
            }
            parts.join(", ")
        }
        PhysicalPlan::TableDual(dual) => dual.explain_info(),
        PhysicalPlan::MemTable(_) => String::new(),
        PhysicalPlan::CTE(cte) => cte.operator_info(),
        PhysicalPlan::CTETable(table) => table.explain_info(),
        PhysicalPlan::Lock(lock) => lock.explain_info(),
        PhysicalPlan::Sequence(_) => {
            tidb_planner::physical::PhysicalSequence::explain_info().to_owned()
        }
        PhysicalPlan::TableReader(reader) => reader.explain_info(),
        PhysicalPlan::IndexScan(scan) => {
            let mut parts = Vec::new();
            if !scan.ranges.is_empty()
                && !tidb_planner::ranger::types::has_full_range(&scan.ranges, false)
            {
                parts.push(format!("range:{}", scan_ranges_text(&scan.ranges)));
            }
            parts.push(format!("keep order:{}", scan.keep_order));
            if scan_uses_pseudo_statistics(&scan.base) {
                parts.push("stats:pseudo".to_owned());
            }
            parts.join(", ")
        }
        PhysicalPlan::IndexReader(reader) => format!(
            "index:{}",
            reader
                .index_plan
                .as_deref()
                .map_or_else(String::new, |plan| plan.explain_id(false))
        ),
        PhysicalPlan::IndexLookUpReader(reader) => format!(
            "index:{}, table:{}",
            reader
                .index_plan
                .as_deref()
                .map_or_else(String::new, |plan| plan.explain_id(false)),
            reader
                .table_plan
                .as_deref()
                .map_or_else(String::new, |plan| plan.explain_id(false))
        ),
        PhysicalPlan::PointGet(point) => {
            let common_handle = catalog
                .kv_table_by_id(point.table_id)
                .is_some_and(|table| !table.common_handle_offsets().is_empty());
            if point.index_id.is_some() || common_handle {
                String::new()
            } else {
                point.ranges.first().map_or_else(String::new, |range| {
                    format!("handle:{}", point_handle_text(range))
                })
            }
        }
        PhysicalPlan::BatchPointGet(batch) => {
            let common_handle = catalog
                .kv_table_by_id(batch.table_id)
                .is_some_and(|table| !table.common_handle_offsets().is_empty());
            let prefix = if batch.index_id.is_none() && !common_handle {
                let handles = batch
                    .ranges
                    .iter()
                    .map(point_handle_text)
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("handle:[{handles}]")
            } else {
                String::new()
            };
            let order = format!("keep order:{}, desc:{}", batch.keep_order, batch.desc);
            if prefix.is_empty() {
                order
            } else {
                format!("{prefix}, {order}")
            }
        }
        PhysicalPlan::IndexMergeReader(reader) => format!(
            "type:{}, keep order:{}",
            if reader.is_intersection_type {
                "intersection"
            } else {
                "union"
            },
            reader.keep_order
        ),
        PhysicalPlan::Dml(_) => "N/A".to_owned(),
        PhysicalPlan::TopN(topn) => format!(
            "{}, offset:{}, count:{}",
            by_items_text(&topn.by_items),
            topn.offset,
            topn.count
        ),
        PhysicalPlan::HashAgg(aggregation) => aggregate_info(
            &aggregation.agg_funcs,
            &aggregation.group_by_items,
            aggregation.base.base.schema(),
        ),
        PhysicalPlan::StreamAgg(aggregation) => aggregate_info(
            &aggregation.agg_funcs,
            &aggregation.group_by_items,
            aggregation.base.base.schema(),
        ),
        PhysicalPlan::MaxOneRow(_)
        | PhysicalPlan::NominalSort(_)
        | PhysicalPlan::Show(_)
        | PhysicalPlan::ShowDDLJobs(_)
        | PhysicalPlan::UnionAll(_) => String::new(),
    }
}

fn runtime_rows(
    plan: &PhysicalPlan,
    runtime: Option<&crate::driver::physical_builder::PhysicalRuntimeStats>,
) -> Option<u64> {
    runtime?
        .get(&crate::driver::physical_builder::runtime_plan_key(plan))
        .map(|counter| counter.get())
}

fn physical_explain_operator(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    task: ExplainTask,
    label: &str,
    runtime: Option<&crate::driver::physical_builder::PhysicalRuntimeStats>,
) -> ExplainOperator {
    let mut children = match plan {
        PhysicalPlan::TableReader(reader) => reader
            .table_plan
            .as_deref()
            .map(|child| {
                vec![physical_explain_operator(
                    child,
                    catalog,
                    ExplainTask::Cop {
                        request: "cop".to_owned(),
                        store: "tikv".to_owned(),
                    },
                    "",
                    runtime,
                )]
            })
            .unwrap_or_default(),
        PhysicalPlan::IndexReader(reader) => reader
            .index_plan
            .as_deref()
            .map(|child| {
                vec![physical_explain_operator(
                    child,
                    catalog,
                    ExplainTask::Cop {
                        request: "cop".to_owned(),
                        store: "tikv".to_owned(),
                    },
                    "",
                    runtime,
                )]
            })
            .unwrap_or_default(),
        PhysicalPlan::IndexLookUpReader(reader) => [
            reader.index_plan.as_deref().map(|child| (child, "(Build)")),
            reader.table_plan.as_deref().map(|child| (child, "(Probe)")),
        ]
        .into_iter()
        .flatten()
        .map(|(child, label)| {
            physical_explain_operator(
                child,
                catalog,
                ExplainTask::Cop {
                    request: "cop".to_owned(),
                    store: "tikv".to_owned(),
                },
                label,
                runtime,
            )
        })
        .collect(),
        PhysicalPlan::IndexMergeReader(reader) => reader
            .partial_plans_raw
            .iter()
            .map(|child| (child, "(Build)"))
            .chain(reader.table_plan.as_deref().map(|child| (child, "(Probe)")))
            .map(|(child, label)| {
                physical_explain_operator(
                    child,
                    catalog,
                    ExplainTask::Cop {
                        request: "cop".to_owned(),
                        store: "tikv".to_owned(),
                    },
                    label,
                    runtime,
                )
            })
            .collect(),
        PhysicalPlan::Dml(root) => root
            .select_plan
            .as_deref()
            .map(|child| {
                vec![physical_explain_operator(
                    child,
                    catalog,
                    ExplainTask::Root,
                    "",
                    runtime,
                )]
            })
            .unwrap_or_default(),
        _ => plan
            .children()
            .iter()
            .map(|child| physical_explain_operator(child, catalog, task.clone(), "", runtime))
            .collect(),
    };

    let labels = match plan {
        PhysicalPlan::HashJoin(join) => {
            let build = if join.use_outer_to_build {
                1usize.saturating_sub(join.inner_child_idx)
            } else {
                join.inner_child_idx
            };
            Some(build)
        }
        PhysicalPlan::Apply(apply) => Some(1usize.saturating_sub(apply.hash_join.inner_child_idx)),
        PhysicalPlan::IndexJoin(join) => Some(1usize.saturating_sub(join.inner_child_idx)),
        PhysicalPlan::MergeJoin(join) => Some(
            if join.join_type == tidb_planner::find_best_task::LogicalJoinType::RightOuter {
                0
            } else {
                1
            },
        ),
        _ => None,
    };
    if let Some(build) = labels.filter(|_| children.len() == 2) {
        children[build].label = "(Build)".to_owned();
        children[1 - build].label = "(Probe)".to_owned();
        if build == 1 {
            children.swap(0, 1);
        }
    }

    let mut operator = ExplainOperator::new(physical_operator_name(plan), plan.id())
        .with_task(task)
        .with_access_object(physical_access(plan, catalog))
        .with_operator_info(physical_operator_info(plan, catalog))
        .with_children(children);
    operator.label = label.to_owned();
    if let Some(rows) = plan
        .stats_info()
        .map(tidb_planner::stats_info::StatsInfo::row_count)
    {
        operator = operator.with_estimated_rows(rows);
    }
    if let Some(rows) = runtime_rows(plan, runtime) {
        operator = operator.with_actual_rows(rows);
    }
    operator
}

fn cte_definitions(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    runtime: Option<&crate::driver::physical_builder::PhysicalRuntimeStats>,
    seen: &mut std::collections::BTreeSet<i32>,
    out: &mut Vec<ExplainOperator>,
) {
    if let PhysicalPlan::CTE(cte) = plan {
        if seen.insert(cte.id_for_storage) {
            let mut children = vec![physical_explain_operator(
                &cte.seed_plan,
                catalog,
                ExplainTask::Root,
                "(Seed Part)",
                runtime,
            )];
            if let Some(recursive) = cte.recursive_plan.as_deref() {
                children.push(physical_explain_operator(
                    recursive,
                    catalog,
                    ExplainTask::Root,
                    "(Recursive Part)",
                    runtime,
                ));
            }
            let mut definition = ExplainOperator::new("CTE", cte.id_for_storage)
                .with_operator_info(if cte.recursive_plan.is_some() {
                    "Recursive CTE"
                } else {
                    "Non-Recursive CTE"
                })
                .with_children(children);
            definition.estimated_rows = plan
                .stats_info()
                .map(tidb_planner::stats_info::StatsInfo::row_count);
            out.push(definition);
        }
        cte_definitions(&cte.seed_plan, catalog, runtime, seen, out);
        if let Some(recursive) = cte.recursive_plan.as_deref() {
            cte_definitions(recursive, catalog, runtime, seen, out);
        }
    }
    for child in plan.children() {
        cte_definitions(child, catalog, runtime, seen, out);
    }
    match plan {
        PhysicalPlan::TableReader(reader) => {
            if let Some(child) = reader.table_plan.as_deref() {
                cte_definitions(child, catalog, runtime, seen, out);
            }
        }
        PhysicalPlan::IndexReader(reader) => {
            if let Some(child) = reader.index_plan.as_deref() {
                cte_definitions(child, catalog, runtime, seen, out);
            }
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            if let Some(child) = reader.index_plan.as_deref() {
                cte_definitions(child, catalog, runtime, seen, out);
            }
            if let Some(child) = reader.table_plan.as_deref() {
                cte_definitions(child, catalog, runtime, seen, out);
            }
        }
        PhysicalPlan::Dml(root) => {
            if let Some(child) = root.select_plan.as_deref() {
                cte_definitions(child, catalog, runtime, seen, out);
            }
        }
        _ => {}
    }
}

fn render_physical_plan(
    physical: &PhysicalPlan,
    catalog: &Catalog,
    format: ExplainFormat,
    analyze: bool,
    runtime: Option<&crate::driver::physical_builder::PhysicalRuntimeStats>,
) -> Result<SelectMeta, DriverError> {
    let mut roots = vec![physical_explain_operator(
        physical,
        catalog,
        ExplainTask::Root,
        "",
        runtime,
    )];
    cte_definitions(
        physical,
        catalog,
        runtime,
        &mut std::collections::BTreeSet::new(),
        &mut roots,
    );

    let mut rows = Vec::new();
    let mut columns = Vec::new();
    for root in roots {
        let mut explain = PlannerExplain::new(planner_explain_format(format), analyze, root);
        let mut explain_context = PlannerExplainContext::default();
        let schema = explain
            .prepare_schema(&mut explain_context)
            .map_err(|error| DriverError::unsupported(error.to_string()))?;
        let rendered = explain
            .render_result(&mut explain_context)
            .map_err(|error| DriverError::unsupported(error.to_string()))?
            .to_vec();
        if columns.is_empty() {
            let field_type = FieldType::new(FieldTypeCode::VarString);
            columns = schema
                .field_names
                .into_iter()
                .map(|name| (name.to_owned(), field_type.clone()))
                .collect();
        }
        rows.extend(
            rendered
                .iter()
                .map(|row| row.iter().map(|value| text(value)).collect::<Vec<_>>()),
        );
    }
    Ok((columns, rows))
}

fn render_physical_query(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
    analyze: bool,
) -> Result<SelectMeta, DriverError> {
    crate::driver::set_opr::validate_query_usage(query)?;
    let mut physical = crate::driver::optimize_query_stmt(query, catalog, current_db, ctx)?;
    let runtime = analyze
        .then(|| crate::driver::physical_builder::execute_for_explain(&mut physical, catalog, ctx))
        .transpose()?;
    render_physical_plan(&physical, catalog, format, analyze, runtime.as_ref())
}

/// Plans `select` and reports the plan as EXPLAIN rows, executing nothing:
/// the driver builds the pipeline, the trace records it, and it is dropped
/// undrained.
pub fn explain_select_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    render_physical_query(
        &tidb_ast::QueryStmt::Select(Box::new(select.clone())),
        catalog,
        current_db,
        ctx,
        format,
        false,
    )
}

/// Plain `EXPLAIN` over a supported set operation.
pub fn explain_set_opr_stmt(
    set_opr: &tidb_ast::SetOprStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    render_physical_query(
        &tidb_ast::QueryStmt::SetOpr(Box::new(set_opr.clone())),
        catalog,
        current_db,
        ctx,
        format,
        false,
    )
}

/// `EXPLAIN ANALYZE <select>`: the same plan [`explain_select_stmt`] records,
/// but the query actually RUNS (real `EXPLAIN ANALYZE` executes the wrapped
/// statement to gather its runtime counters, confirmed by capture), and each
/// operator's `actRows` is the REAL row count that stage produced -- metered
/// by the trace on the executor it recorded, during that one execution.
/// Nothing is estimated, and nothing is run a second time to be counted.
pub fn explain_analyze_select_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    render_physical_query(
        &tidb_ast::QueryStmt::Select(Box::new(select.clone())),
        catalog,
        current_db,
        ctx,
        format,
        true,
    )
}

/// `EXPLAIN ANALYZE` over a supported set operation.
pub fn explain_analyze_set_opr_stmt(
    set_opr: &tidb_ast::SetOprStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    render_physical_query(
        &tidb_ast::QueryStmt::SetOpr(Box::new(set_opr.clone())),
        catalog,
        current_db,
        ctx,
        format,
        true,
    )
}

/// Plans an `INSERT` and reports the plan as EXPLAIN rows, executing nothing:
/// the driver returns before it writes. Go's `Insert_N` row carries none of
/// the estimate/access/info a read operator would (captured:
/// `[Insert_1 N/A root  N/A]`, both for a plain `VALUES` insert and for the
/// `Insert ... SELECT` form, where the select's own plan appears as
/// `Insert`'s one child).
pub fn explain_insert_stmt(
    insert: &tidb_ast::InsertStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let physical = crate::driver::physical_dml_plan(
        "Insert",
        insert.source.as_deref(),
        true,
        None,
        catalog,
        current_db,
        ctx,
    )?;
    render_physical_plan(&physical, catalog, format, false, None)
}

/// `EXPLAIN ANALYZE <insert>`: unlike [`explain_insert_stmt`], this really
/// inserts the row(s) -- real `EXPLAIN ANALYZE INSERT` executes the
/// statement (captured: the table has the new row afterward). The `Insert_N`
/// node's `actRows` is always `0`: Go's own `Insert_1` row shows `actRows`
/// `0` too (captured), because the insert executor's `Next()` produces no
/// rows of its own -- the write is a side effect, not this operator's
/// row-producing interface.
///
/// An `INSERT ... SELECT` source's `actRows` is metered on the SAME run that
/// feeds the insert, so a row this statement is about to write can never be
/// counted as one it read, and the source is read exactly once.
pub fn explain_analyze_insert_stmt(
    insert: &tidb_ast::InsertStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let mut physical = crate::driver::physical_dml_plan(
        "Insert",
        insert.source.as_deref(),
        true,
        None,
        catalog,
        current_db,
        ctx,
    )?;
    let root_key = crate::driver::physical_builder::runtime_plan_key(&physical);
    let mut runtime = crate::driver::physical_builder::PhysicalRuntimeStats::new();
    run_insert_stmt_with_physical_and_stats(
        insert,
        catalog,
        current_db,
        ctx,
        Some(&mut physical),
        Some(&mut runtime),
    )?;
    runtime.insert(root_key, Rc::new(Cell::new(0)));
    render_physical_plan(&physical, catalog, format, true, Some(&runtime))
}

/// Plans an `UPDATE` and reports the plan as EXPLAIN rows, executing nothing.
/// `Update_N`'s one child is the read the driver performs to find the rows to
/// update -- a `Point_Get`, a `TableRangeScan` or a `TableFullScan`, whichever
/// `driver::access::write_read_path` chose -- with a `Selection` above it for
/// the `WHERE` (divergences 7 and 8).
pub fn explain_update_stmt(
    update: &tidb_ast::UpdateStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let source = crate::driver::update_source_query(update).ok_or_else(|| {
        DriverError::unsupported("multi-table UPDATE plans are not supported yet")
    })?;
    let physical = crate::driver::physical_dml_plan(
        "Update",
        Some(&source),
        false,
        Some(update),
        catalog,
        current_db,
        ctx,
    )?;
    render_physical_plan(&physical, catalog, format, false, None)
}

/// Plans a `DELETE` and reports the plan as EXPLAIN rows, executing nothing.
/// See [`explain_update_stmt`]: `Delete_N`'s child is the same read plan.
pub fn explain_delete_stmt(
    delete: &tidb_ast::DeleteStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let source = crate::driver::delete_source_query(delete).ok_or_else(|| {
        DriverError::unsupported("multi-table DELETE plans are not supported yet")
    })?;
    let physical = crate::driver::physical_dml_plan(
        "Delete",
        Some(&source),
        false,
        None,
        catalog,
        current_db,
        ctx,
    )?;
    render_physical_plan(&physical, catalog, format, false, None)
}

/// `EXPLAIN ANALYZE <update>`: unlike [`explain_update_stmt`], this really
/// runs the `UPDATE` (captured: the table's rows change afterward, both for a
/// primary-key `WHERE` and an ordinary-column one). The read child's
/// `actRows` come off that same run: the read node's is the number of records
/// the update's own read examined, and `Selection`'s the number its own
/// `WHERE` passed -- both confirmed by capture. Like `Insert_1`, `Update_N`'s
/// own `actRows` is always `0` (captured): the write is a side effect, not a
/// row this operator's `Next()` produces.
pub fn explain_analyze_update_stmt(
    update: &tidb_ast::UpdateStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let source = crate::driver::update_source_query(update).ok_or_else(|| {
        DriverError::unsupported("multi-table UPDATE plans are not supported yet")
    })?;
    let mut physical = crate::driver::physical_dml_plan(
        "Update",
        Some(&source),
        false,
        Some(update),
        catalog,
        current_db,
        ctx,
    )?;
    let root_key = crate::driver::physical_builder::runtime_plan_key(&physical);
    let mut runtime = crate::driver::physical_builder::PhysicalRuntimeStats::new();
    run_update_stmt_with_physical_and_stats(
        update,
        catalog,
        current_db,
        ctx,
        Some(&mut physical),
        Some(&mut runtime),
    )?;
    runtime.insert(root_key, Rc::new(Cell::new(0)));
    render_physical_plan(&physical, catalog, format, true, Some(&runtime))
}

/// `EXPLAIN ANALYZE <delete>`: see [`explain_analyze_update_stmt`] -- the
/// same real read-then-write shape, over the delete driver.
pub fn explain_analyze_delete_stmt(
    delete: &tidb_ast::DeleteStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let source = crate::driver::delete_source_query(delete).ok_or_else(|| {
        DriverError::unsupported("multi-table DELETE plans are not supported yet")
    })?;
    let mut physical = crate::driver::physical_dml_plan(
        "Delete",
        Some(&source),
        false,
        None,
        catalog,
        current_db,
        ctx,
    )?;
    let root_key = crate::driver::physical_builder::runtime_plan_key(&physical);
    let mut runtime = crate::driver::physical_builder::PhysicalRuntimeStats::new();
    run_delete_stmt_with_physical_and_stats(
        delete,
        catalog,
        current_db,
        ctx,
        Some(&mut physical),
        Some(&mut runtime),
    )?;
    runtime.insert(root_key, Rc::new(Cell::new(0)));
    render_physical_plan(&physical, catalog, format, true, Some(&runtime))
}

fn text(value: &str) -> Datum {
    Datum::Bytes(value.as_bytes().to_vec())
}
