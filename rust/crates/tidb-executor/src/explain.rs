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

use prost::Message;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_planner::access::{AccessObject, IndexAccess, OtherAccessObject, ScanAccessObject};
use tidb_planner::explain::{
    Explain as PlannerExplain, ExplainContext as PlannerExplainContext,
    ExplainFormat as PlannerExplainFormat, ExplainOperator, ExplainTask,
};
use tidb_planner::physical::{PhysicalPlan, RedactMode};
use tidb_proto::tipb::{ExplainData, ExplainOperator as PbExplainOperator, OperatorLabel};

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
    crate::plan_trace::physical_expression_text_with_columns(
        expression,
        &[],
        crate::plan_trace::ExpressionTextStyle::Explain,
    )
    .unwrap_or_default()
}

/// Go `Expression.StringWithCtx`: the renderer `ExplainExpressionList` uses
/// for a Projection's own expressions, where a nested string constant prints
/// bare instead of quoted.
fn expression_string_text(expression: &tidb_expr::expression::Expression) -> String {
    crate::plan_trace::physical_expression_text_with_columns(
        expression,
        &[],
        crate::plan_trace::ExpressionTextStyle::StringWithCtx,
    )
    .unwrap_or_default()
}

/// Go appends `stats:pseudo` only when a scan's `StatsInfo.StatsVersion` is
/// `statistics.PseudoVersion` (and no statement-specific `UsedStatsInfo`
/// replaces it).
fn scan_uses_pseudo_statistics(base: &tidb_planner::physical::BasePhysicalPlan) -> bool {
    base.base
        .stats_info()
        .is_none_or(|stats| stats.stats_version() == tidb_stats::PSEUDO_VERSION)
}

fn expressions_text(expressions: &[tidb_expr::expression::Expression]) -> String {
    expressions
        .iter()
        .map(expression_text)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Go `expression.ExplainExpressionList` (`explain.go:188`), the Projection's
/// operator text: every expression is rendered with its OUTPUT column as
/// `expr->Column#N`, EXCEPT a direct column whose own text already equals the
/// output column's.
fn projection_text(
    expressions: &[tidb_expr::expression::Expression],
    schema: Option<&tidb_expr::schema::Schema>,
) -> String {
    expressions
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            let rendered = expression_string_text(expression);
            let Some(output) = schema.and_then(|schema| schema.columns.get(index)) else {
                return rendered;
            };
            let output =
                expression_string_text(&tidb_expr::expression::Expression::Column(output.clone()));
            match expression {
                // A column projected under the SAME identity prints once;
                // a re-projected column with a different UniqueID prints both.
                tidb_expr::expression::Expression::Column(_)
                | tidb_expr::expression::Expression::CorrelatedColumn(_)
                    if rendered == output =>
                {
                    rendered
                }
                _ => format!("{rendered}->{output}"),
            }
        })
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

/// Go `Plan.TP()` (normalized) / `Plan.ExplainID().String()` (row) for a
/// physical child. The base plan's own `tp` is the logical operator name
/// ("Join"), so the child's PHYSICAL name has to come from
/// [`physical_operator_name`].
fn plan_explain_id(plan: &PhysicalPlan, ignore_suffix: bool) -> String {
    let name = physical_operator_name(plan, None);
    if ignore_suffix {
        name
    } else {
        format!("{}_{}", name, plan.base().base.id())
    }
}

/// Go `expression.SortedExplainExpressionList`: render each condition, SORT
/// the rendered strings, and join with `", "`. `PhysicalHashJoin`'s and
/// `PhysicalMergeJoin`'s `right cond`/`other cond` both use it, and so does
/// MergeJoin's `left cond`.
fn sorted_expressions_text(expressions: &[tidb_expr::expression::Expression]) -> String {
    let mut rendered = expressions.iter().map(expression_text).collect::<Vec<_>>();
    rendered.sort();
    rendered.join(", ")
}

/// Go `PhysicalHashJoin`/`PhysicalMergeJoin`'s operator text.
///
/// `explainJoinLeftSide` (`physical_index_join.go:135`) appends
/// `, left side:<child>` for every join that is NOT an inner join, using the
/// child's plan TYPE under a normalized (brief) explain and its explain id
/// otherwise. A merge join lists its keys as `left key:`/`right key:`; a hash
/// join renders its equal conditions, and only the HASH join brackets
/// `left cond`.
fn join_info(
    join_type: tidb_planner::find_best_task::LogicalJoinType,
    left_child: Option<&PhysicalPlan>,
    ignore_explain_id_suffix: bool,
    merge: bool,
    left_keys: &[tidb_expr::column::Column],
    right_keys: &[tidb_expr::column::Column],
    is_null_eq: &[bool],
    left_conditions: &[tidb_expr::expression::Expression],
    right_conditions: &[tidb_expr::expression::Expression],
    other_conditions: &[tidb_expr::expression::Expression],
) -> String {
    let mut parts = vec![join_type_text(join_type).to_owned()];
    if join_type != tidb_planner::find_best_task::LogicalJoinType::Inner {
        if let Some(child) = left_child {
            parts.push(format!(
                "left side:{}",
                plan_explain_id(child, ignore_explain_id_suffix)
            ));
        }
    }
    if merge {
        if !left_keys.is_empty() {
            parts.push(format!("left key:{}", columns_text(left_keys)));
        }
        if !right_keys.is_empty() {
            parts.push(format!("right key:{}", columns_text(right_keys)));
        }
    } else {
        let equal = left_keys
            .iter()
            .zip(right_keys)
            .enumerate()
            .map(|(index, (left, right))| {
                // Go renders each `EqualCondition`'s OWN function name; a
                // set-operator semi join keys on `<=>` (`nulleq`).
                let operator = if is_null_eq.get(index).copied().unwrap_or(false) {
                    "nulleq"
                } else {
                    "eq"
                };
                format!(
                    "{operator}({}, {})",
                    expression_text(&tidb_expr::expression::Expression::Column(left.clone())),
                    expression_text(&tidb_expr::expression::Expression::Column(right.clone()))
                )
            })
            .collect::<Vec<_>>();
        if !equal.is_empty() {
            parts.push(format!("equal:[{}]", equal.join(" ")));
        }
    }
    if !left_conditions.is_empty() {
        if merge {
            parts.push(format!(
                "left cond:{}",
                sorted_expressions_text(left_conditions)
            ));
        } else {
            // Go's `PhysicalHashJoin` non-normalized `left cond` is the ONLY
            // bracketed condition list, and it keeps the original order.
            let rendered = left_conditions
                .iter()
                .map(expression_text)
                .collect::<Vec<_>>()
                .join(" ");
            parts.push(format!("left cond:[{rendered}]"));
        }
    }
    if !right_conditions.is_empty() {
        parts.push(format!(
            "right cond:{}",
            sorted_expressions_text(right_conditions)
        ));
    }
    if !other_conditions.is_empty() {
        parts.push(format!(
            "other cond:{}",
            sorted_expressions_text(other_conditions)
        ));
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
        // Go `BasePhysicalAgg.explainInfo` renders the GROUP BY items with
        // `expression.SortedExplainExpressionList`, which SORTS the rendered
        // strings; the aggregate list below keeps its own order.
        parts.push(format!("group by:{}", sorted_expressions_text(group_by)));
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

/// Go `PhysicalWindow.ExplainInfo` (`physical_window.go:167`) and
/// `FormatWindowFuncDescs` (`:218`): the window functions' own renderings
/// (each consuming its trailing schema column) followed by
/// `over(partition by ... order by ... <frame>)`.
fn window_info(window: &tidb_planner::physical::PhysicalWindow) -> String {
    use tidb_expr::expression::Expression;
    let schema = window.base.base.schema();
    let result_start = schema
        .map(|schema| {
            schema
                .columns
                .len()
                .saturating_sub(window.window_func_descs.len())
        })
        .unwrap_or(0);
    let functions = window
        .window_func_descs
        .iter()
        .enumerate()
        .map(|(index, descriptor)| {
            let output = schema
                .and_then(|schema| schema.columns.get(result_start + index))
                .map(|column| expression_text(&Expression::Column(column.clone())))
                .unwrap_or_else(|| format!("Column#{}", result_start + index));
            format!(
                "{}({})->{output}",
                descriptor.base.name,
                expressions_text(&descriptor.base.args)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut parts = Vec::new();
    if !window.partition_by.is_empty() {
        parts.push(format!(
            "partition by {}",
            window
                .partition_by
                .iter()
                .map(|item| expression_text(&Expression::Column(item.col.clone())))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !window.order_by.is_empty() {
        parts.push(format!(
            "order by {}",
            window
                .order_by
                .iter()
                .map(|item| {
                    let text = expression_text(&Expression::Column(item.col.clone()));
                    if item.desc {
                        format!("{text} desc")
                    } else {
                        text
                    }
                })
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(frame) = &window.frame {
        let kind = match frame.frame_type {
            tidb_planner::logical::window::FrameType::Rows => "rows",
            tidb_planner::logical::window::FrameType::Ranges => "range",
            tidb_planner::logical::window::FrameType::Groups => "groups",
        };
        parts.push(format!(
            "{kind} between {} and {}",
            frame_bound_text(frame.start.as_ref()),
            frame_bound_text(frame.end.as_ref())
        ));
    }
    format!("{functions} over({})", parts.join(" "))
}

fn frame_bound_text(bound: Option<&tidb_planner::logical::window::FrameBound>) -> String {
    use tidb_planner::logical::window::{BoundType, FrameBound};
    let Some(bound) = bound else {
        return String::new();
    };
    let _: &FrameBound = bound;
    if bound.bound_type == BoundType::CurrentRow {
        return "current row".to_owned();
    }
    let direction = if bound.bound_type == BoundType::Preceding {
        "preceding"
    } else {
        "following"
    };
    if bound.unbounded {
        format!("unbounded {direction}")
    } else {
        format!("{} {direction}", bound.num)
    }
}

fn scan_ranges_text(ranges: &tidb_planner::ranger::types::Ranges) -> String {
    ranges
        .iter()
        .map(tidb_planner::ranger::types::Range::to_display_string)
        // Go's EXPLAIN separates multiple intervals with ", " —
        // `range:[-inf,5), (5,+inf]`.
        .collect::<Vec<_>>()
        .join(", ")
}

/// Go `PhysicalTableScan/PhysicalIndexScan.OperatorInfo`'s order clause:
/// `keep order:<bool>` followed by `, desc` only when the scan walks
/// backwards (`physical_table_scan.go:512`, `physical_index_scan.go:296`).
fn scan_keep_order_text(keep_order: bool, desc: bool) -> String {
    if desc {
        format!("keep order:{keep_order}, desc")
    } else {
        format!("keep order:{keep_order}")
    }
}

fn table_name(catalog: &Catalog, table_id: i64, alias: Option<&str>) -> String {
    alias.map_or_else(
        || {
            catalog
                .physical_kv_table_by_id(table_id)
                .map_or_else(|| table_id.to_string(), |table| table.name.clone())
        },
        str::to_owned,
    )
}

fn table_access(catalog: &Catalog, table_id: i64, alias: Option<&str>) -> ScanAccessObject {
    let mut access = ScanAccessObject {
        database: catalog
            .physical_kv_table_database_by_id(table_id)
            .unwrap_or_default()
            .to_owned(),
        table: table_name(catalog, table_id, alias),
        ..ScanAccessObject::default()
    };
    if let Some(table) = catalog.physical_kv_table_by_id(table_id) {
        if let Some(name) = table.partition().and_then(|partition| {
            partition
                .definitions
                .iter()
                .find(|definition| definition.id == table_id)
                .map(|definition| definition.name.as_str())
        }) {
            access.partitions.push(name.to_owned());
        }
    }
    access
}

fn index_access(
    catalog: &Catalog,
    table_id: i64,
    alias: Option<&str>,
    index_id: i64,
    index_name: Option<&str>,
) -> ScanAccessObject {
    let mut table_access = table_access(catalog, table_id, alias);
    let Some(table) = catalog.physical_kv_table_by_id(table_id) else {
        if let Some(name) = index_name {
            table_access.indexes.push(IndexAccess {
                name: name.to_owned(),
                ..IndexAccess::default()
            });
        }
        return table_access;
    };
    let Some(index) = table.plan_indexes().find(|index| index.id == index_id) else {
        if let Some(name) = index_name {
            table_access.indexes.push(IndexAccess {
                name: name.to_owned(),
                ..IndexAccess::default()
            });
        }
        return table_access;
    };
    let cols = index
        .column_offsets
        .iter()
        .filter_map(|offset| table.columns.get(*offset))
        .map(|column| column.name.clone())
        .collect();
    table_access.indexes.push(IndexAccess {
        name: index.name.clone(),
        cols,
        is_clustered_index: false,
    });
    table_access
}

fn point_access(catalog: &Catalog, table_id: i64, index_id: Option<i64>) -> ScanAccessObject {
    match index_id {
        Some(index_id) => index_access(catalog, table_id, None, index_id, None),
        None => {
            let mut access = table_access(catalog, table_id, None);
            let Some(table) = catalog.physical_kv_table_by_id(table_id) else {
                return access;
            };
            if table.common_handle_offsets().is_empty() {
                access
            } else {
                let columns = table
                    .common_handle_offsets()
                    .iter()
                    .filter_map(|offset| table.columns.get(*offset))
                    .map(|column| column.name.clone())
                    .collect();
                access.indexes.push(IndexAccess {
                    name: "PRIMARY".to_owned(),
                    cols: columns,
                    is_clustered_index: true,
                });
                access
            }
        }
    }
}

#[derive(Clone, Copy)]
struct IndexJoinExplainContext<'a> {
    table_id: i64,
    /// The inner access object: `None` is the clustered table range, `Some`
    /// is a secondary index. Go renders `range: decided by [...]` on whichever
    /// scan the runtime join keys probe.
    index_id: Option<i64>,
    /// Go `indexJoinResult.chosenPath.IdxCols` at each matched key offset: the
    /// INNER index columns the outer keys probe.
    inner_keys: &'a [tidb_expr::column::Column],
    /// Go `prop.IndexJoinProp.OuterJoinKeys`, paired with `inner_keys`.
    outer_keys: &'a [tidb_expr::column::Column],
    /// Go `indexJoinResult.chosenAccess`: the conditions that extend the
    /// per-probe range past the equality keys.
    access_conditions: &'a [tidb_expr::expression::Expression],
}

fn is_index_join_table_range(
    plan: &PhysicalPlan,
    context: Option<IndexJoinExplainContext<'_>>,
) -> bool {
    let PhysicalPlan::TableScan(scan) = plan else {
        return false;
    };
    context.is_some_and(|context| {
        context.index_id.is_none()
            && context.table_id == scan.table_id
            && !context.outer_keys.is_empty()
    })
}

/// Go `PhysicalIndexScan.RangeInfo` for an index-join probe: the secondary
/// index the runtime join keys decide.
fn is_index_join_index_range(
    plan: &PhysicalPlan,
    context: Option<IndexJoinExplainContext<'_>>,
) -> bool {
    let PhysicalPlan::IndexScan(scan) = plan else {
        return false;
    };
    context.is_some_and(|context| {
        context.index_id == Some(scan.index_id)
            && context.table_id == scan.table_id
            && !context.outer_keys.is_empty()
    })
}

/// Go `indexJoinPathRangeInfo`: `eq(inner_idx_col, outer_key)` for every
/// matched key, then `chosenAccess`.
fn index_join_decided_by_text(context: IndexJoinExplainContext<'_>) -> String {
    let mut decided = context
        .inner_keys
        .iter()
        .zip(context.outer_keys)
        .map(|(inner, outer)| {
            format!(
                "eq({}, {})",
                expression_text(&tidb_expr::expression::Expression::Column(inner.clone())),
                expression_text(&tidb_expr::expression::Expression::Column(outer.clone()))
            )
        })
        .collect::<Vec<_>>();
    decided.extend(context.access_conditions.iter().map(expression_text));
    format!("range: decided by [{}]", decided.join(" "))
}

fn columns_text(columns: &[tidb_expr::column::Column]) -> String {
    columns
        .iter()
        .map(|column| expression_text(&tidb_expr::expression::Expression::Column(column.clone())))
        .collect::<Vec<_>>()
        .join(", ")
}

fn physical_operator_name(
    plan: &PhysicalPlan,
    index_join_context: Option<IndexJoinExplainContext<'_>>,
) -> String {
    match plan {
        PhysicalPlan::TableScan(scan) => {
            if is_index_join_table_range(plan, index_join_context) {
                "TableRangeScan".to_owned()
            } else {
                scan.scan_kind().map_or_else(
                    || "TableScan".to_owned(),
                    |kind| kind.plan_type().to_owned(),
                )
            }
        }
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
        // Join physical operators share the logical `Join` base type, but
        // Go's plancodec names each executor family distinctly.
        PhysicalPlan::HashJoin(_) => "HashJoin".to_owned(),
        PhysicalPlan::MergeJoin(_) => "MergeJoin".to_owned(),
        PhysicalPlan::IndexJoin(join) => match join.kind {
            tidb_planner::plan_cost_ver2::IndexJoinKind::IndexJoin => "IndexJoin",
            tidb_planner::plan_cost_ver2::IndexJoinKind::IndexHashJoin => "IndexHashJoin",
            tidb_planner::plan_cost_ver2::IndexJoinKind::IndexMergeJoin => "IndexMergeJoin",
        }
        .to_owned(),
        _ => plan.tp().to_owned(),
    }
}

fn physical_access(plan: &PhysicalPlan, catalog: &Catalog) -> Option<AccessObject> {
    match plan {
        // Go reader AccessObject reports dynamic pruning; scan AccessObject
        // reports a partition only when it scans a static physical table ID.
        PhysicalPlan::TableReader(reader) => reader
            .table_plan
            .as_deref()
            .and_then(dynamic_partition_access),
        PhysicalPlan::IndexReader(reader) => reader
            .index_plan
            .as_deref()
            .and_then(dynamic_partition_access),
        PhysicalPlan::IndexLookUpReader(reader) => reader
            .index_plan
            .as_deref()
            .and_then(dynamic_partition_access),
        PhysicalPlan::IndexMergeReader(reader) => reader
            .table_plan
            .as_deref()
            .and_then(dynamic_partition_access),
        PhysicalPlan::TableScan(scan) => Some(AccessObject::Scan(table_access(
            catalog,
            scan.table_id,
            scan.table_as_name.as_deref(),
        ))),
        PhysicalPlan::TableSample(sample) => Some(AccessObject::Scan(table_access(
            catalog,
            sample.physical_table_id,
            None,
        ))),
        PhysicalPlan::MemTable(scan) => Some(AccessObject::Scan(ScanAccessObject {
            database: scan.db_name.clone(),
            table: scan.table_name.clone(),
            ..ScanAccessObject::default()
        })),
        PhysicalPlan::IndexScan(scan) => Some(AccessObject::Scan(index_access(
            catalog,
            scan.table_id,
            scan.table_as_name.as_deref(),
            scan.index_id,
            Some(&scan.index_name),
        ))),
        PhysicalPlan::PointGet(point) => {
            let id = point.partition.as_ref().and_then(|p| p.physical_table_id);
            let mut access = point_access(catalog, id.unwrap_or(point.table_id), point.index_id);
            if point.partition.is_some() && id.is_none() {
                access.partitions = vec!["dual".to_owned()];
            }
            Some(AccessObject::Scan(access))
        }
        PhysicalPlan::BatchPointGet(point) => {
            let mut access = point_access(catalog, point.table_id, point.index_id);
            if let Some(ids) = &point.partition_ids {
                if let Some(table) = catalog.physical_kv_table_by_id(point.table_id) {
                    if let Some(partition) = table.partition() {
                        access.partitions = partition
                            .definitions
                            .iter()
                            .filter(|definition| ids.contains(&definition.id))
                            .map(|definition| definition.name.clone())
                            .collect();
                    }
                }
            }
            Some(AccessObject::Scan(access))
        }
        PhysicalPlan::CTE(cte) => {
            if cte.cte_name.eq_ignore_ascii_case(&cte.cte_as_name) {
                Some(AccessObject::Other(OtherAccessObject(format!(
                    "CTE:{}",
                    cte.cte_name.to_ascii_lowercase()
                ))))
            } else {
                Some(AccessObject::Other(OtherAccessObject(format!(
                    "CTE:{} AS {}",
                    cte.cte_name.to_ascii_lowercase(),
                    cte.cte_as_name.to_ascii_lowercase()
                ))))
            }
        }
        _ => None,
    }
}

fn dynamic_partition_access(plan: &PhysicalPlan) -> Option<AccessObject> {
    let access = match plan {
        PhysicalPlan::TableScan(scan) => scan.dynamic_partition_access.clone(),
        PhysicalPlan::IndexScan(scan) => scan.dynamic_partition_access.clone(),
        _ => None,
    };
    access
        .map(|object| {
            AccessObject::DynamicPartitions(tidb_planner::access::DynamicPartitionAccessObjects(
                vec![object],
            ))
        })
        .or_else(|| plan.children().iter().find_map(dynamic_partition_access))
}

fn point_handle_text(range: &tidb_planner::ranger::types::Range) -> String {
    range
        .low_val
        .first()
        .and_then(|value| value.sql_string().ok())
        .unwrap_or_default()
}

/// Go `physical_batch_point_get.go:206`: with `UnsignedHandle`, the batch's
/// handle values print as their uint64 reading (`strconv.FormatUint`) — a
/// `BIGINT UNSIGNED` handle's `u64::MAX` is `18446744073709551615`, never the
/// signed reinterpretation `-1`.
fn point_handle_text_unsigned(range: &tidb_planner::ranger::types::Range) -> String {
    match range.low_val.first() {
        Some(tidb_datatype::Datum::Int(value)) => format!("{}", *value as u64),
        Some(value) => value.sql_string().unwrap_or_default(),
        None => String::new(),
    }
}

fn physical_operator_info(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    ignore_explain_id_suffix: bool,
    index_join_context: Option<IndexJoinExplainContext<'_>>,
) -> String {
    match plan {
        PhysicalPlan::Selection(selection) => expressions_text(&selection.conditions),
        PhysicalPlan::Projection(projection) => {
            projection_text(&projection.exprs, projection.base.base.schema())
        }
        PhysicalPlan::HashJoin(join) => {
            let prefix = if join.left_join_keys.is_empty() && join.equal_conditions.is_empty() {
                if join.na_equal_conditions.is_empty() {
                    "CARTESIAN "
                } else {
                    "Null-aware "
                }
            } else {
                ""
            };
            let info = join_info(
                join.join_type,
                join.base.children().first(),
                ignore_explain_id_suffix,
                false,
                &join.left_join_keys,
                &join.right_join_keys,
                &join.is_null_eq,
                &join.left_conditions,
                &join.right_conditions,
                &join.other_conditions,
            );
            format!("{prefix}{info}")
        }
        PhysicalPlan::MergeJoin(join) => join_info(
            join.join_type,
            join.base.children().first(),
            ignore_explain_id_suffix,
            true,
            &join.left_join_keys,
            &join.right_join_keys,
            &join.is_null_eq,
            &join.left_conditions,
            &join.right_conditions,
            &join.other_conditions,
        ),
        PhysicalPlan::IndexJoin(join) => {
            let mut parts = vec![join_type_text(join.join_type).to_owned()];
            // Go `PhysicalIndexJoin.ExplainInfoInternal`
            // (`physical_index_join.go:159-165`): the `inner:` field comes
            // BEFORE `explainJoinLeftSide`'s `left side:` field.
            if let Some(inner) = join.base.children().get(join.inner_child_idx) {
                parts.push(format!(
                    "inner:{}",
                    inner.explain_id(ignore_explain_id_suffix)
                ));
            }
            if join.join_type != tidb_planner::find_best_task::LogicalJoinType::Inner {
                if let Some(left) = join.base.children().first() {
                    parts.push(format!(
                        "left side:{}",
                        plan_explain_id(left, ignore_explain_id_suffix)
                    ));
                }
            }
            if !join.outer_join_keys.is_empty() {
                parts.push(format!("outer key:{}", columns_text(&join.outer_join_keys)));
            }
            if !join.inner_join_keys.is_empty() {
                parts.push(format!("inner key:{}", columns_text(&join.inner_join_keys)));
            }
            if !join.outer_hash_keys.is_empty()
                && join.kind != tidb_planner::plan_cost_ver2::IndexJoinKind::IndexMergeJoin
            {
                let mut equal = join
                    .outer_hash_keys
                    .iter()
                    .zip(&join.inner_hash_keys)
                    .enumerate()
                    .map(|(index, (outer, inner))| {
                        let operator = if join.is_null_eq.get(index).copied().unwrap_or(false) {
                            "nulleq"
                        } else {
                            "eq"
                        };
                        format!(
                            "{operator}({}, {})",
                            expression_text(&tidb_expr::expression::Expression::Column(
                                outer.clone()
                            )),
                            expression_text(&tidb_expr::expression::Expression::Column(
                                inner.clone()
                            )),
                        )
                    })
                    .collect::<Vec<_>>();
                // Go `sortedExplainExpressionList`: sorted, `", "`-joined.
                equal.sort();
                parts.push(format!("equal cond:{}", equal.join(", ")));
            }
            if !join.left_conditions.is_empty() {
                parts.push(format!(
                    "left cond:{}",
                    sorted_expressions_text(&join.left_conditions)
                ));
            }
            if !join.right_conditions.is_empty() {
                parts.push(format!(
                    "right cond:{}",
                    sorted_expressions_text(&join.right_conditions)
                ));
            }
            if !join.other_conditions.is_empty() {
                parts.push(format!(
                    "other cond:{}",
                    sorted_expressions_text(&join.other_conditions)
                ));
            }
            parts.join(", ")
        }
        PhysicalPlan::Apply(apply) => join_info(
            apply.hash_join.join_type,
            apply.hash_join.base.children().first(),
            ignore_explain_id_suffix,
            false,
            &apply.hash_join.left_join_keys,
            &apply.hash_join.right_join_keys,
            &apply.hash_join.is_null_eq,
            &apply.hash_join.left_conditions,
            &apply.hash_join.right_conditions,
            &apply.hash_join.other_conditions,
        ),
        PhysicalPlan::Sort(sort) => by_items_text(&sort.by_items),
        PhysicalPlan::Limit(limit) => limit.explain_info(RedactMode::Disable),
        PhysicalPlan::TableScan(scan) => {
            let mut parts = Vec::new();
            if is_index_join_table_range(plan, index_join_context) {
                parts.push(index_join_decided_by_text(
                    index_join_context.expect("index join context"),
                ));
            } else if scan
                .scan_kind()
                .is_some_and(|kind| kind.plan_type() == "TableRangeScan")
                && !scan.ranges.is_empty()
            {
                parts.push(format!("range:{}", scan_ranges_text(&scan.ranges)));
            }
            parts.push(scan_keep_order_text(scan.keep_order, scan.desc));
            if scan_uses_pseudo_statistics(&scan.base) {
                parts.push("stats:pseudo".to_owned());
            }
            parts.join(", ")
        }
        PhysicalPlan::TableDual(dual) => dual.explain_info(),
        PhysicalPlan::TableSample(_) => String::new(),
        PhysicalPlan::MemTable(_) => String::new(),
        PhysicalPlan::CTE(cte) => cte.operator_info(),
        PhysicalPlan::CTETable(table) => table.explain_info(),
        PhysicalPlan::Lock(lock) => lock.explain_info(),
        PhysicalPlan::Sequence(_) => {
            tidb_planner::physical::PhysicalSequence::explain_info().to_owned()
        }
        PhysicalPlan::TableReader(reader) => reader.explain_info(ignore_explain_id_suffix),
        PhysicalPlan::IndexScan(scan) => {
            let mut parts = Vec::new();
            if is_index_join_index_range(plan, index_join_context) {
                parts.push(index_join_decided_by_text(
                    index_join_context.expect("index join context"),
                ));
            } else if !scan.ranges.is_empty()
                && !tidb_planner::ranger::types::has_full_range(&scan.ranges, false)
            {
                parts.push(format!("range:{}", scan_ranges_text(&scan.ranges)));
            }
            parts.push(scan_keep_order_text(scan.keep_order, scan.desc));
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
                .map_or_else(String::new, |plan| {
                    plan.explain_id(ignore_explain_id_suffix)
                })
        ),
        PhysicalPlan::IndexLookUpReader(reader) => reader.pushed_limit.map_or_else(
            String::new,
            // Go `PhysicalIndexLookUpReader.ExplainInfo`
            // (`physical_indexlookup_reader.go:189`): the children are implied
            // by the relation symbol, so the reader prints only its embedded
            // index-side limit.
            |limit| {
                format!(
                    "limit embedded(offset:{}, count:{})",
                    limit.offset, limit.count
                )
            },
        ),
        PhysicalPlan::LocalIndexLookUp(lookup) => {
            format!("index handle offsets:{:?}", lookup.index_handle_offsets)
        }
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
                let handle_text = |range: &tidb_planner::ranger::types::Range| {
                    if batch.unsigned_handle {
                        point_handle_text_unsigned(range)
                    } else {
                        point_handle_text(range)
                    }
                };
                let handles = batch
                    .ranges
                    .iter()
                    .map(handle_text)
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
        PhysicalPlan::Window(window) => window_info(window),
        PhysicalPlan::Expand(expand) => format!(
            "level-projection:{}; schema: [{}]",
            expand
                .level_exprs
                .iter()
                .map(|level| format!("[{}]", expressions_text(level)))
                .collect::<Vec<_>>()
                .join(","),
            plan.schema()
                .map(|schema| columns_text(&schema.columns))
                .unwrap_or_default()
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
    ignore_explain_id_suffix: bool,
    index_join_context: Option<IndexJoinExplainContext<'_>>,
    // Go `GetEstimatedProbeCntFromProbeParents`: the product of the outer
    // child's row count for every index-join (or apply) parent this operator
    // sits inside. `EXPLAIN` displays `StatsInfo.RowCount * probeCount`.
    probe_count: f64,
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
                    ignore_explain_id_suffix,
                    index_join_context,
                    probe_count,
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
                    ignore_explain_id_suffix,
                    index_join_context,
                    probe_count,
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
                ignore_explain_id_suffix,
                index_join_context,
                probe_count,
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
                    ignore_explain_id_suffix,
                    index_join_context,
                    probe_count,
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
                    ignore_explain_id_suffix,
                    index_join_context,
                    probe_count,
                )]
            })
            .unwrap_or_default(),
        _ => plan
            .children()
            .iter()
            .enumerate()
            .map(|(index, child)| {
                let child_context = match plan {
                    PhysicalPlan::IndexJoin(join) if index == join.inner_child_idx => join
                        .inner_access_table_id
                        .map(|table_id| IndexJoinExplainContext {
                            table_id,
                            index_id: join.inner_access_index_id,
                            inner_keys: &join.inner_join_keys,
                            outer_keys: &join.outer_join_keys,
                            access_conditions: &join.other_conditions,
                        }),
                    PhysicalPlan::IndexJoin(_) => None,
                    _ => index_join_context,
                };
                // Go `propagateProbeParents`: the INNER child of an index
                // join (or apply) carries that join as a probe parent, so its
                // display row count is multiplied by the join's OUTER row
                // count. The outer child does not.
                let inner_child_idx = match plan {
                    PhysicalPlan::IndexJoin(join) => Some(join.inner_child_idx),
                    PhysicalPlan::Apply(apply) => Some(apply.hash_join.inner_child_idx),
                    _ => None,
                };
                let child_probe_count = match inner_child_idx {
                    Some(inner) if index == inner => {
                        let outer_rows = plan
                            .children()
                            .get(1 - inner)
                            .and_then(|outer| outer.stats_info())
                            .map_or(1.0, tidb_planner::stats_info::StatsInfo::row_count);
                        probe_count * outer_rows
                    }
                    _ => probe_count,
                };
                physical_explain_operator(
                    child,
                    catalog,
                    task.clone(),
                    "",
                    runtime,
                    ignore_explain_id_suffix,
                    child_context,
                    child_probe_count,
                )
            })
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

    let mut operator =
        ExplainOperator::new(physical_operator_name(plan, index_join_context), plan.id())
            .with_task(task)
            .with_operator_info(physical_operator_info(
                plan,
                catalog,
                ignore_explain_id_suffix,
                index_join_context,
            ))
            .with_children(children);
    if let Some(access_object) = physical_access(plan, catalog) {
        operator = operator.with_access_object(access_object);
    }
    operator.label = label.to_owned();
    if let Some(rows) = plan
        .stats_info()
        .map(tidb_planner::stats_info::StatsInfo::row_count)
    {
        operator = operator.with_estimated_rows(rows * probe_count);
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
    ignore_explain_id_suffix: bool,
) {
    if let PhysicalPlan::CTE(cte) = plan {
        if seen.insert(cte.id_for_storage) {
            let mut children = vec![physical_explain_operator(
                &cte.seed_plan,
                catalog,
                ExplainTask::Root,
                "(Seed Part)",
                runtime,
                ignore_explain_id_suffix,
                None,
                1.0,
            )];
            if let Some(recursive) = cte.recursive_plan.as_deref() {
                children.push(physical_explain_operator(
                    recursive,
                    catalog,
                    ExplainTask::Root,
                    "(Recursive Part)",
                    runtime,
                    ignore_explain_id_suffix,
                    None,
                    1.0,
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
        cte_definitions(
            &cte.seed_plan,
            catalog,
            runtime,
            seen,
            out,
            ignore_explain_id_suffix,
        );
        if let Some(recursive) = cte.recursive_plan.as_deref() {
            cte_definitions(
                recursive,
                catalog,
                runtime,
                seen,
                out,
                ignore_explain_id_suffix,
            );
        }
    }
    for child in plan.children() {
        cte_definitions(child, catalog, runtime, seen, out, ignore_explain_id_suffix);
    }
    match plan {
        PhysicalPlan::TableReader(reader) => {
            if let Some(child) = reader.table_plan.as_deref() {
                cte_definitions(child, catalog, runtime, seen, out, ignore_explain_id_suffix);
            }
        }
        PhysicalPlan::IndexReader(reader) => {
            if let Some(child) = reader.index_plan.as_deref() {
                cte_definitions(child, catalog, runtime, seen, out, ignore_explain_id_suffix);
            }
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            if let Some(child) = reader.index_plan.as_deref() {
                cte_definitions(child, catalog, runtime, seen, out, ignore_explain_id_suffix);
            }
            if let Some(child) = reader.table_plan.as_deref() {
                cte_definitions(child, catalog, runtime, seen, out, ignore_explain_id_suffix);
            }
        }
        PhysicalPlan::Dml(root) => {
            if let Some(child) = root.select_plan.as_deref() {
                cte_definitions(child, catalog, runtime, seen, out, ignore_explain_id_suffix);
            }
        }
        _ => {}
    }
}

fn physical_explain_roots(
    physical: &PhysicalPlan,
    catalog: &Catalog,
    runtime: Option<&crate::driver::physical_builder::PhysicalRuntimeStats>,
    ignore_explain_id_suffix: bool,
    scalar_subqueries: &[crate::driver::planner_bridge::RegisteredScalarSubquery],
) -> Vec<ExplainOperator> {
    let mut roots = vec![physical_explain_operator(
        physical,
        catalog,
        ExplainTask::Root,
        "",
        runtime,
        ignore_explain_id_suffix,
        None,
        1.0,
    )];
    cte_definitions(
        physical,
        catalog,
        runtime,
        &mut std::collections::BTreeSet::new(),
        &mut roots,
        ignore_explain_id_suffix,
    );
    for subquery in scalar_subqueries {
        roots.push(scalar_subquery_root(
            subquery,
            catalog,
            runtime,
            ignore_explain_id_suffix,
        ));
    }
    roots
}

/// Go `FlatPhysicalPlan.flattenScalarSubQRecursively` (`flat_plan.go:553`):
/// the registered `ScalarSubqueryEvalCtx` is its own EXPLAIN root, whose
/// `ExplainInfo` lists the output column ids the folded constants carry and
/// whose single child is the optimized subquery plan.
fn scalar_subquery_root(
    subquery: &crate::driver::planner_bridge::RegisteredScalarSubquery,
    catalog: &Catalog,
    runtime: Option<&crate::driver::physical_builder::PhysicalRuntimeStats>,
    ignore_explain_id_suffix: bool,
) -> ExplainOperator {
    let output = subquery
        .output_col_ids
        .iter()
        .map(|id| format!("ScalarQueryCol#{id}"))
        .collect::<Vec<_>>()
        .join(", ");
    ExplainOperator::new("ScalarSubQuery", subquery.block_offset)
        .with_operator_info(format!("Output: {output}"))
        .with_children(vec![physical_explain_operator(
            &subquery.physical,
            catalog,
            ExplainTask::Root,
            "",
            runtime,
            ignore_explain_id_suffix,
            None,
            1.0,
        )])
}

fn binary_operator(full: ExplainOperator, brief: ExplainOperator) -> PbExplainOperator {
    let labels = match full.label.as_str() {
        "(Build)" => vec![OperatorLabel::BuildSide as i32],
        "(Probe)" => vec![OperatorLabel::ProbeSide as i32],
        "(Seed Part)" => vec![OperatorLabel::SeedPart as i32],
        "(Recursive Part)" => vec![OperatorLabel::RecursivePart as i32],
        _ => Vec::new(),
    };
    let task = full.task.to_string();
    let (task_type, store_type) = if task == "root" {
        (
            tidb_proto::tipb::TaskType::Root as i32,
            tidb_proto::tipb::StoreType::Tidb as i32,
        )
    } else {
        (
            tidb_proto::tipb::TaskType::Cop as i32,
            tidb_proto::tipb::StoreType::Tikv as i32,
        )
    };
    let children = full
        .children
        .into_iter()
        .zip(brief.children)
        .map(|(full, brief)| binary_operator(full, brief))
        .collect();
    let brief_operator_info = matches!(
        full.operator.as_str(),
        "TableReader" | "IndexReader" | "HashJoin" | "IndexJoin" | "IndexHashJoin" | "MergeJoin"
    )
    .then_some(brief.operator_info)
    .unwrap_or_default();
    let mut operator = PbExplainOperator {
        name: format!("{}_{}", full.operator, full.id),
        children,
        labels,
        est_rows: full.estimated_rows.unwrap_or_default(),
        task_type,
        store_type,
        operator_info: full.operator_info,
        memory_bytes: -1,
        disk_bytes: -1,
        brief_name: brief.operator,
        brief_operator_info,
        ..Default::default()
    };
    if let Some(access) = full.access_object {
        access.set_into_pb(&mut operator);
    }
    operator
}

/// Go `GetBriefBinaryPlan`: encodes the retained ordinary physical tree with
/// build-side-first traversal and brief operator metadata.
#[must_use]
pub fn brief_binary_plan(physical: &PhysicalPlan, catalog: &Catalog) -> String {
    let mut full = physical_explain_roots(physical, catalog, None, false, &[]);
    let mut brief = physical_explain_roots(physical, catalog, None, true, &[]);
    if full.is_empty() || brief.is_empty() {
        return String::new();
    }
    let main = binary_operator(full.remove(0), brief.remove(0));
    let ctes = full
        .into_iter()
        .zip(brief)
        .map(|(full, brief)| binary_operator(full, brief))
        .collect();
    let data = ExplainData {
        main: Some(main),
        ctes,
        ..Default::default()
    };
    tidb_util::plancodec::compress(&data.encode_to_vec())
}

fn first_table_scan(plan: &PhysicalPlan) -> Option<&tidb_planner::physical::PhysicalTableScan> {
    match plan {
        PhysicalPlan::TableScan(scan) => Some(scan),
        PhysicalPlan::TableReader(reader) => {
            reader.table_plan.as_deref().and_then(first_table_scan)
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            reader.table_plan.as_deref().and_then(first_table_scan)
        }
        PhysicalPlan::IndexMergeReader(reader) => {
            reader.table_plan.as_deref().and_then(first_table_scan)
        }
        PhysicalPlan::Dml(root) => root.select_plan.as_deref().and_then(first_table_scan),
        _ => plan.children().iter().find_map(first_table_scan),
    }
}

fn first_index_scan(plan: &PhysicalPlan) -> Option<&tidb_planner::physical::PhysicalIndexScan> {
    match plan {
        PhysicalPlan::IndexScan(scan) => Some(scan),
        PhysicalPlan::IndexReader(reader) => {
            reader.index_plan.as_deref().and_then(first_index_scan)
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            reader.index_plan.as_deref().and_then(first_index_scan)
        }
        PhysicalPlan::IndexMergeReader(reader) => {
            reader.partial_plans_raw.iter().find_map(first_index_scan)
        }
        PhysicalPlan::Dml(root) => root.select_plan.as_deref().and_then(first_index_scan),
        _ => plan.children().iter().find_map(first_index_scan),
    }
}

fn logical_table_id(catalog: &Catalog, physical_id: i64) -> i64 {
    catalog
        .physical_kv_table_by_id(physical_id)
        .map_or(physical_id, |table| table.table_id)
}

fn index_process_name(
    catalog: &Catalog,
    scan: &tidb_planner::physical::PhysicalIndexScan,
) -> String {
    format!(
        "{}:{}",
        table_name(catalog, scan.table_id, None),
        scan.index_name
    )
}

fn collect_executor_process_fields(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    table_ids: &mut Vec<i64>,
    index_names: &mut Vec<String>,
) {
    match plan {
        PhysicalPlan::TableReader(reader) => {
            if let Some(scan) = reader.table_plan.as_deref().and_then(first_table_scan) {
                table_ids.push(logical_table_id(catalog, scan.table_id));
            }
        }
        PhysicalPlan::IndexReader(reader) => {
            if let Some(scan) = reader.index_plan.as_deref().and_then(first_index_scan) {
                index_names.push(index_process_name(catalog, scan));
            }
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            if let Some(scan) = reader.index_plan.as_deref().and_then(first_index_scan) {
                index_names.push(index_process_name(catalog, scan));
            }
            if let Some(scan) = reader.table_plan.as_deref().and_then(first_table_scan) {
                table_ids.push(logical_table_id(catalog, scan.table_id));
            }
        }
        PhysicalPlan::IndexMergeReader(reader) => {
            for partial in &reader.partial_plans_raw {
                if let Some(scan) = first_index_scan(partial) {
                    index_names.push(index_process_name(catalog, scan));
                } else if let Some(scan) = first_table_scan(partial) {
                    if let Some(table) = catalog.physical_kv_table_by_id(scan.table_id) {
                        if let Some(primary) =
                            table.indexes().iter().find(|index| index.clustered_primary)
                        {
                            index_names.push(format!("{}:{}", table.name, primary.name));
                        }
                    }
                }
            }
            if let Some(scan) = reader.table_plan.as_deref().and_then(first_table_scan) {
                table_ids.push(logical_table_id(catalog, scan.table_id));
            }
        }
        PhysicalPlan::PointGet(point) => {
            if let Some(index_id) = point.index_id {
                if let Some(table) = catalog.physical_kv_table_by_id(point.table_id) {
                    if let Some(index) = table.indexes().iter().find(|index| index.id == index_id) {
                        index_names.push(format!("{}:{}", table.name, index.name));
                    }
                }
            }
        }
        PhysicalPlan::BatchPointGet(point) => {
            if let Some(index_id) = point.index_id {
                if let Some(table) = catalog.physical_kv_table_by_id(point.table_id) {
                    if let Some(index) = table.indexes().iter().find(|index| index.id == index_id) {
                        index_names.push(format!("{}:{}", table.name, index.name));
                    }
                }
            }
        }
        PhysicalPlan::TableScan(scan) => {
            table_ids.push(logical_table_id(catalog, scan.table_id));
        }
        PhysicalPlan::IndexScan(scan) => index_names.push(index_process_name(catalog, scan)),
        PhysicalPlan::TableSample(sample) => {
            table_ids.push(logical_table_id(catalog, sample.physical_table_id));
        }
        PhysicalPlan::Dml(root) => {
            if let Some(select) = root.select_plan.as_deref() {
                collect_executor_process_fields(select, catalog, table_ids, index_names);
            }
        }
        _ => {
            for child in plan.children() {
                collect_executor_process_fields(child, catalog, table_ids, index_names);
            }
        }
    }
}

fn collect_stats_info(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    stats_info: &mut std::collections::HashMap<String, u64>,
) {
    for child in plan.children() {
        collect_stats_info(child, catalog, stats_info);
    }
    match plan {
        PhysicalPlan::TableReader(reader) => {
            if let Some(inner) = reader.table_plan.as_deref() {
                collect_stats_info(inner, catalog, stats_info);
            }
        }
        PhysicalPlan::IndexReader(reader) => {
            if let Some(inner) = reader.index_plan.as_deref() {
                collect_stats_info(inner, catalog, stats_info);
            }
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            if let Some(inner) = reader.index_plan.as_deref() {
                collect_stats_info(inner, catalog, stats_info);
            }
        }
        PhysicalPlan::IndexScan(scan) => {
            let name = table_name(catalog, scan.table_id, None);
            let version = catalog
                .table_statistics(logical_table_id(catalog, scan.table_id))
                .map_or(0, |statistics| statistics.version);
            stats_info.insert(name, version);
        }
        PhysicalPlan::TableScan(scan) => {
            let name = table_name(catalog, scan.table_id, None);
            let version = catalog
                .table_statistics(logical_table_id(catalog, scan.table_id))
                .map_or(0, |statistics| statistics.version);
            stats_info.insert(name, version);
        }
        PhysicalPlan::Dml(root) => {
            if let Some(select) = root.select_plan.as_deref() {
                collect_stats_info(select, catalog, stats_info);
            }
        }
        _ => {}
    }
}

/// The plan-derived fields Go publishes while the ordinary executor is
/// constructed.
#[must_use]
pub fn process_plan_info(physical: &PhysicalPlan, catalog: &Catalog) -> crate::ProcessPlanInfo {
    process_plan_info_with_brief(physical, catalog, true)
}

/// [`process_plan_info`] with the brief binary plan rendered only when
/// `with_brief` is set; the table, index, and statistics fields are always
/// collected, as Go's `StmtCtx.TableIDs`/`IndexNames` are.
#[must_use]
pub fn process_plan_info_with_brief(
    physical: &PhysicalPlan,
    catalog: &Catalog,
    with_brief: bool,
) -> crate::ProcessPlanInfo {
    let mut info = crate::ProcessPlanInfo {
        brief_binary_plan: if with_brief {
            brief_binary_plan(physical, catalog)
        } else {
            String::new()
        },
        ..crate::ProcessPlanInfo::default()
    };
    collect_executor_process_fields(
        physical,
        catalog,
        &mut info.table_ids,
        &mut info.index_names,
    );
    collect_stats_info(physical, catalog, &mut info.stats_info);
    info
}

fn render_physical_plan(
    physical: &PhysicalPlan,
    catalog: &Catalog,
    format: ExplainFormat,
    analyze: bool,
    runtime: Option<&crate::driver::physical_builder::PhysicalRuntimeStats>,
    scalar_subqueries: &[crate::driver::planner_bridge::RegisteredScalarSubquery],
) -> Result<SelectMeta, DriverError> {
    let ignore_explain_id_suffix = matches!(format, ExplainFormat::Brief | ExplainFormat::PlanTree);
    let roots = physical_explain_roots(
        physical,
        catalog,
        runtime,
        ignore_explain_id_suffix,
        scalar_subqueries,
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
    crate::driver::set_opr::validate_query_usage(query, ctx)?;
    let (mut physical, scalar_subqueries) =
        crate::driver::optimize_query_stmt_with_scalar_subqueries(query, catalog, current_db, ctx)?;
    crate::driver::physical_builder::prepare_execution_plan(&mut physical, catalog, ctx)?;
    let runtime = analyze
        .then(|| crate::driver::physical_builder::execute_for_explain(&mut physical, catalog, ctx))
        .transpose()?;
    render_physical_plan(
        &physical,
        catalog,
        format,
        analyze,
        runtime.as_ref(),
        &scalar_subqueries,
    )
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
    render_physical_plan(&physical, catalog, format, false, None, &[])
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
    render_physical_plan(&physical, catalog, format, true, Some(&runtime), &[])
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
    render_physical_plan(&physical, catalog, format, false, None, &[])
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
    render_physical_plan(&physical, catalog, format, false, None, &[])
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
    render_physical_plan(&physical, catalog, format, true, Some(&runtime), &[])
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
    render_physical_plan(&physical, catalog, format, true, Some(&runtime), &[])
}

fn text(value: &str) -> Datum {
    Datum::Bytes(value.as_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_partitions_belong_to_readers_not_scans() {
        use tidb_planner::physical::*;
        let catalog = Catalog::default();
        for (all_partitions, names, expected) in [
            (true, Vec::new(), "partition:all"),
            (
                false,
                vec!["p0".to_owned(), "p2".to_owned()],
                "partition:p0,p2",
            ),
        ] {
            let access = tidb_planner::access::DynamicPartitionAccessObject {
                database: "test".to_owned(),
                table: "t".to_owned(),
                all_partitions,
                partitions: names,
                error: String::new(),
            };
            let table = PhysicalPlan::TableScan(PhysicalTableScan {
                dynamic_partition_access: Some(access.clone()),
                ..Default::default()
            });
            let index = PhysicalPlan::IndexScan(PhysicalIndexScan {
                dynamic_partition_access: Some(access),
                ..Default::default()
            });
            let readers = [
                PhysicalPlan::TableReader(PhysicalTableReader {
                    table_plan: Some(Box::new(table.clone())),
                    ..Default::default()
                }),
                PhysicalPlan::IndexReader(PhysicalIndexReader {
                    index_plan: Some(Box::new(index.clone())),
                    ..Default::default()
                }),
                PhysicalPlan::IndexLookUpReader(PhysicalIndexLookUpReader {
                    index_plan: Some(Box::new(index.clone())),
                    table_plan: Some(Box::new(table.clone())),
                    ..Default::default()
                }),
                PhysicalPlan::IndexMergeReader(PhysicalIndexMergeReader {
                    partial_plans_raw: vec![index.clone()],
                    table_plan: Some(Box::new(table.clone())),
                    ..Default::default()
                }),
            ];
            for reader in readers {
                assert_eq!(
                    physical_access(&reader, &catalog).unwrap().to_string(),
                    expected
                );
            }
            for scan in [table, index] {
                let Some(AccessObject::Scan(access)) = physical_access(&scan, &catalog) else {
                    panic!("scan has a scan access object");
                };
                assert!(access.partitions.is_empty());
            }
        }
    }

    #[test]
    fn physical_join_names_match_go_plancodec_types() {
        assert_eq!(
            physical_operator_name(&PhysicalPlan::HashJoin(Default::default()), None),
            "HashJoin"
        );
        assert_eq!(
            physical_operator_name(&PhysicalPlan::MergeJoin(Default::default()), None),
            "MergeJoin"
        );

        let mut index = tidb_planner::physical::PhysicalIndexJoin::default();
        for (kind, expected) in [
            (
                tidb_planner::plan_cost_ver2::IndexJoinKind::IndexJoin,
                "IndexJoin",
            ),
            (
                tidb_planner::plan_cost_ver2::IndexJoinKind::IndexHashJoin,
                "IndexHashJoin",
            ),
            (
                tidb_planner::plan_cost_ver2::IndexJoinKind::IndexMergeJoin,
                "IndexMergeJoin",
            ),
        ] {
            index.kind = kind;
            assert_eq!(
                physical_operator_name(&PhysicalPlan::IndexJoin(index.clone()), None),
                expected
            );
        }
    }

    #[test]
    fn physical_index_join_explain_uses_null_safe_equal_condition() {
        let column =
            |id| tidb_expr::column::Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        let mut index = tidb_planner::physical::PhysicalIndexJoin::default();
        index.outer_hash_keys = vec![column(1), column(2)];
        index.inner_hash_keys = vec![column(3), column(4)];
        index.is_null_eq = vec![true, false];
        let info = physical_operator_info(
            &PhysicalPlan::IndexJoin(index),
            &Catalog::default(),
            false,
            None,
        );
        assert!(info.contains("equal cond:eq(Column#2, Column#4), nulleq(Column#1, Column#3)"));
    }

    #[test]
    fn merge_join_lists_keys_and_unbracketed_conditions_like_go() {
        let column =
            |id| tidb_expr::column::Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        let condition = |name: &str, left: i64, right: i64| {
            tidb_expr::expression::Expression::ScalarFunction(
                tidb_expr::scalar_function::ScalarFunction::new(
                    tidb_ast::CiString::new(name),
                    FieldType::new(FieldTypeCode::LongLong),
                    vec![
                        tidb_expr::expression::Expression::Column(column(left)),
                        tidb_expr::expression::Expression::Column(column(right)),
                    ],
                ),
            )
        };
        let mut merge = tidb_planner::physical::PhysicalMergeJoin::default();
        merge.join_type = tidb_planner::find_best_task::LogicalJoinType::Inner;
        merge.left_join_keys = vec![column(1), column(2)];
        merge.right_join_keys = vec![column(3), column(4)];
        // Go sorts the rendered conditions; `eq` precedes `gt` here.
        merge.other_conditions = vec![condition("gt", 1, 5), condition("eq", 2, 6)];
        let info = physical_operator_info(
            &PhysicalPlan::MergeJoin(merge),
            &Catalog::default(),
            true,
            None,
        );
        assert!(
            info.starts_with(
                "inner join, left key:Column#1, Column#2, right key:Column#3, Column#4, other cond:eq(Column#2, Column#6), gt(Column#1, Column#5)"
            ),
            "{info}"
        );
    }

    #[test]
    fn a_non_inner_join_explains_its_left_side_like_go() {
        let column =
            |id| tidb_expr::column::Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        let child = || PhysicalPlan::HashJoin(tidb_planner::physical::PhysicalHashJoin::default());

        let mut outer = tidb_planner::physical::PhysicalHashJoin::default();
        outer.join_type = tidb_planner::find_best_task::LogicalJoinType::LeftOuterSemi;
        outer.left_join_keys = vec![column(1)];
        outer.right_join_keys = vec![column(2)];
        outer.base.set_children(vec![child()]);
        let info = physical_operator_info(
            &PhysicalPlan::HashJoin(outer),
            &Catalog::default(),
            true,
            None,
        );
        assert!(
            info.starts_with(
                "left outer semi join, left side:HashJoin, equal:[eq(Column#1, Column#2)]"
            ),
            "{info}"
        );

        // Go's `explainJoinLeftSide` writes nothing for an INNER join.
        let mut inner = tidb_planner::physical::PhysicalHashJoin::default();
        inner.join_type = tidb_planner::find_best_task::LogicalJoinType::Inner;
        inner.left_join_keys = vec![column(1)];
        inner.right_join_keys = vec![column(2)];
        inner.base.set_children(vec![child()]);
        let info = physical_operator_info(
            &PhysicalPlan::HashJoin(inner),
            &Catalog::default(),
            true,
            None,
        );
        assert!(
            info.starts_with("inner join, equal:[eq(Column#1, Column#2)]"),
            "{info}"
        );
    }

    #[test]
    fn hash_join_without_equality_explains_as_cartesian() {
        let mut join = tidb_planner::physical::PhysicalHashJoin::default();
        join.join_type = tidb_planner::find_best_task::LogicalJoinType::Inner;
        assert_eq!(
            physical_operator_info(&PhysicalPlan::HashJoin(join), &Catalog::default(), true, None),
            "CARTESIAN inner join"
        );
    }

    #[test]
    fn a_null_safe_join_key_explains_as_nulleq() {
        let column =
            |id| tidb_expr::column::Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        let mut join = tidb_planner::physical::PhysicalHashJoin::default();
        join.join_type = tidb_planner::find_best_task::LogicalJoinType::Semi;
        join.left_join_keys = vec![column(1), column(2)];
        join.right_join_keys = vec![column(3), column(4)];
        join.is_null_eq = vec![true, false];
        join.base.set_children(vec![PhysicalPlan::HashJoin(
            tidb_planner::physical::PhysicalHashJoin::default(),
        )]);
        let info = physical_operator_info(
            &PhysicalPlan::HashJoin(join),
            &Catalog::default(),
            true,
            None,
        );
        assert!(
            info.starts_with("semi join, left side:HashJoin, equal:[nulleq(Column#1, Column#3) eq(Column#2, Column#4)]"),
            "{info}"
        );
    }
}
