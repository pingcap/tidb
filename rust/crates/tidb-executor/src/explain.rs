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


mod exec;
pub use exec::*;
use tidb_planner::stmt_ctx::PlannerStmtCtx as _;
use prost::Message;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_hack::GoToLower;
use tidb_planner_core_access::{AccessObject, IndexAccess, OtherAccessObject, ScanAccessObject};
use tidb_planner::explain::{
    Explain as PlannerExplain, ExplainContext as PlannerExplainContext,
    ExplainFormat as PlannerExplainFormat, ExplainOperator, ExplainTask,
};
use tidb_planner_core_operator_physicalop::{PhysicalPlan, RedactMode};
use tidb_proto::tipb::{ExplainData, ExplainOperator as PbExplainOperator, OperatorLabel};

use tidb_planner::find_best_task::coster::Ver2Coster;

use crate::driver::{run_delete_stmt_with_physical_and_stats, run_insert_stmt_with_physical_and_stats, run_update_stmt_with_physical_and_stats, DriverError, SelectMeta};
use tidb_infoschema::Catalog;
use tidb_sessionctx::explain_format::ExplainFormat;

/// The plan a finished trace recorded, or the refusal a build site left in it.
fn planner_explain_format(format: ExplainFormat) -> PlannerExplainFormat {
    match format {
        ExplainFormat::Row => PlannerExplainFormat::Row,
        ExplainFormat::Verbose => PlannerExplainFormat::Verbose,
        ExplainFormat::Brief => PlannerExplainFormat::Brief,
        ExplainFormat::PlanTree => PlannerExplainFormat::PlanTree,
        // Dot renders through `explain_dot_rows`, never the row planner.
        ExplainFormat::Dot => PlannerExplainFormat::Row,
        // Hint renders through the empty hint pair, never the row planner.
        ExplainFormat::Hint => PlannerExplainFormat::Row,
        ExplainFormat::CostTrace => PlannerExplainFormat::CostTrace,
        ExplainFormat::Json => PlannerExplainFormat::Json,
        ExplainFormat::PlanCache => PlannerExplainFormat::Row,
        // Both render through their own encoders in `render_physical_plan`.
        ExplainFormat::TidbJson | ExplainFormat::Binary => PlannerExplainFormat::Row,
    }
}

fn expression_text(
    eval_ctx: &dyn tidb_exprctx::Columns,
    expression: &tidb_expr::expression::Expression,
) -> String {
    tidb_expr::explain::physical_expression_text_with_columns(
        eval_ctx,
        expression,
        &[],
        tidb_expr::explain::ExpressionTextStyle::Explain,
    )
    .unwrap_or_default()
}

/// Go `Expression.StringWithCtx`: the renderer `ExplainExpressionList` uses
/// for a Projection's own expressions, where a nested string constant prints
/// bare instead of quoted.
fn expression_string_text(
    eval_ctx: &dyn tidb_exprctx::Columns,
    expression: &tidb_expr::expression::Expression,
) -> String {
    tidb_expr::explain::physical_expression_text_with_columns(
        eval_ctx,
        expression,
        &[],
        tidb_expr::explain::ExpressionTextStyle::StringWithCtx,
    )
    .unwrap_or_default()
}

/// Go appends `stats:pseudo` only when a scan's `StatsInfo.StatsVersion` is
/// `statistics.PseudoVersion` (and no statement-specific `UsedStatsInfo`
/// replaces it).
fn scan_uses_pseudo_statistics(base: &tidb_planner_core_operator_physicalop::BasePhysicalPlan) -> bool {
    base.base
        .stats_info()
        .is_none_or(|stats| stats.stats_version() == tidb_stats::PSEUDO_VERSION)
}

/// Go scan `ExplainInfoInternal`: use the statement's estimator observations,
/// falling back to the plan's pseudo version only when no used-stat record
/// exists for this physical table.
fn used_stats_suffix_for_scan(
    context: &crate::StmtContext,
    physical_table_id: i64,
    base: &tidb_planner_core_operator_physicalop::BasePhysicalPlan,
    catalog: &Catalog,
    selected_index_id: Option<i64>,
) -> Option<String> {
    // A correlated IndexJoin can select its inner index after datasource
    // candidate estimation. Go records that index when its row-count
    // estimator runs; if this planning path did not estimate the selected
    // lookup index, retain its status here so the physical scan still reports
    // the statistic it actually uses.
    if let (Some(index_id), Some(table)) = (
        selected_index_id,
        catalog.physical_kv_table_by_id(physical_table_id),
    ) {
        // Go's estimator reads the table `GetStatsTable` planned from, so a
        // pseudo or determinate copy reports what the planner saw.
        let statistics = crate::access_cost::get_stats_table(context, catalog, physical_table_id);
        crate::driver::planner_bridge::record_used_item_stats_status(
            context,
            &table.name(),
            physical_table_id,
            statistics.as_deref(),
            table.indexes().iter().any(|index| index.id == index_id),
            index_id,
            true,
        );
    }
    if let Some(used_stats) = context.used_stats_info(physical_table_id) {
        let (column_names, index_names) = catalog
            .physical_kv_table_by_id(physical_table_id)
            .map(|table| {
                (
                    table
                        .columns()
                        .iter()
                        .map(|column| (column.id, column.name.clone()))
                        .collect(),
                    table
                        .indexes()
                        .iter()
                        .map(|index| (index.id, index.name.clone()))
                        .collect(),
                )
            })
            .unwrap_or_default();
        // Go prints the used-stats text alone once a record exists -- nothing
        // when every statistic it used is fully loaded -- and falls back to
        // the plan's pseudo version only without one.
        let formatted = used_stats.format_for_explain(&column_names, &index_names);
        return (!formatted.is_empty()).then_some(formatted);
    }
    scan_uses_pseudo_statistics(base).then(|| "stats:pseudo".to_owned())
}

fn expressions_text(
    eval_ctx: &dyn tidb_exprctx::Columns,
    expressions: &[tidb_expr::expression::Expression],
) -> String {
    expressions
        .iter()
        .map(|expression| expression_text(eval_ctx, expression))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Go `expression.ExplainExpressionList` (`explain.go:188`), the Projection's
/// operator text: every expression is rendered with its OUTPUT column as
/// `expr->Column#N`, EXCEPT a direct column whose own text already equals the
/// output column's.
fn projection_text(
    eval_ctx: &dyn tidb_exprctx::Columns,
    expressions: &[tidb_expr::expression::Expression],
    schema: Option<&tidb_expr::schema::Schema>,
) -> String {
    expressions
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            let rendered = expression_string_text(eval_ctx, expression);
            let Some(output) = schema.and_then(|schema| schema.columns.get(index)) else {
                return rendered;
            };
            let output = expression_string_text(
                eval_ctx,
                &tidb_expr::expression::Expression::Column(output.clone()),
            );
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

fn by_items_text(
    eval_ctx: &dyn tidb_exprctx::Columns,
    items: &[tidb_expression_aggregation::ByItems],
) -> String {
    items
        .iter()
        .map(|item| {
            let expression = expression_text(eval_ctx, &item.expr);
            if item.desc {
                format!("{expression}:desc")
            } else {
                expression
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Go `PhysicalTopN.ExplainInfo` (`physical_topn.go:134`): the partition
/// columns, `order by` only after a partition, the by-items, then the
/// partial-order prefix column a prefix index supplies.
fn topn_info(eval_ctx: &dyn tidb_exprctx::Columns, topn: &tidb_planner_core_operator_physicalop::PhysicalTopN) -> String {
    let column_text = |column: &tidb_expr::column::Column| {
        expression_text(eval_ctx, &tidb_expr::expression::Expression::Column(column.clone()))
    };
    let mut text = String::new();
    if !topn.partition_by.is_empty() {
        text.push_str("partition by ");
        text.push_str(
            &topn
                .partition_by
                .iter()
                .map(|item| column_text(&item.col))
                .collect::<Vec<_>>()
                .join(", "),
        );
        text.push(' ');
    }
    if !topn.by_items.is_empty() {
        if !topn.partition_by.is_empty() {
            text.push_str("order by ");
        }
        text.push_str(&by_items_text(eval_ctx, &topn.by_items));
    }
    text.push_str(&format!(", offset:{}, count:{}", topn.offset, topn.count));
    if let Some(prefix_col) = topn.prefix_col {
        let name = topn
            .base
            .base
            .schema()
            .and_then(|schema| schema.columns.iter().find(|column| column.unique_id == prefix_col))
            .map_or_else(|| "?".to_owned(), column_text);
        text.push_str(&format!(", prefix_col:{name}, prefix_len:{}", topn.prefix_len));
    }
    text
}

fn join_type_text(join_type: tidb_planner_core_base::LogicalJoinType) -> &'static str {
    use tidb_planner_core_base::LogicalJoinType;
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
fn sorted_expressions_text(
    eval_ctx: &dyn tidb_exprctx::Columns,
    expressions: &[tidb_expr::expression::Expression],
) -> String {
    let mut rendered = expressions
        .iter()
        .map(|expression| expression_text(eval_ctx, expression))
        .collect::<Vec<_>>();
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
    eval_ctx: &dyn tidb_exprctx::Columns,
    join_type: tidb_planner_core_base::LogicalJoinType,
    left_child: Option<&PhysicalPlan>,
    ignore_explain_id_suffix: bool,
    merge: bool,
    left_keys: &[tidb_expr::column::Column],
    right_keys: &[tidb_expr::column::Column],
    is_null_eq: &[bool],
    equal_conditions: &[tidb_expr::scalar_function::ScalarFunction],
    na_equal_conditions: &[tidb_expr::scalar_function::ScalarFunction],
    left_conditions: &[tidb_expr::expression::Expression],
    right_conditions: &[tidb_expr::expression::Expression],
    other_conditions: &[tidb_expr::expression::Expression],
) -> String {
    let mut parts = vec![join_type_text(join_type).to_owned()];
    if join_type != tidb_planner_core_base::LogicalJoinType::Inner {
        if let Some(child) = left_child {
            parts.push(format!(
                "left side:{}",
                plan_explain_id(child, ignore_explain_id_suffix)
            ));
        }
    }
    if merge {
        if !left_keys.is_empty() {
            parts.push(format!("left key:{}", columns_text(eval_ctx, left_keys)));
        }
        if !right_keys.is_empty() {
            parts.push(format!("right key:{}", columns_text(eval_ctx, right_keys)));
        }
    } else if !equal_conditions.is_empty() {
        // Go `PhysicalHashJoin.explainInfo` (`physical_hash_join.go:243-256`):
        // `equal:[...]` renders `EqualConditions` -- the ORIGINAL eq scalar
        // functions whose argument order is the SQL text order -- not the
        // child-aligned `left_keys`/`right_keys`, so a reordered join keeps
        // the direction the query wrote.
        let equal = equal_conditions
            .iter()
            .map(|condition| {
                expression_text(
                    eval_ctx,
                    &tidb_expr::expression::Expression::ScalarFunction(condition.clone()),
                )
            })
            .collect::<Vec<_>>();
        parts.push(format!("equal:[{}]", equal.join(" ")));
    } else if !na_equal_conditions.is_empty() {
        // Go `PhysicalHashJoin.explainInfo` (`physical_hash_join.go:257-271`):
        // a NAAJ's `NAEqualConditions` render like `EqualConditions` --
        // `equal:[...]`, each condition printed as its own scalar function
        // -- but the two are mutually exclusive (`updateEQCond` only
        // produces `NAEQConditions` when `EqualConditions` is empty), so
        // this never doubles up with the `left_keys`/`right_keys` branch
        // below.
        let equal = na_equal_conditions
            .iter()
            .map(|condition| {
                expression_text(
                    eval_ctx,
                    &tidb_expr::expression::Expression::ScalarFunction(condition.clone()),
                )
            })
            .collect::<Vec<_>>();
        parts.push(format!("equal:[{}]", equal.join(" ")));
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
                    expression_text(
                        eval_ctx,
                        &tidb_expr::expression::Expression::Column(left.clone())
                    ),
                    expression_text(
                        eval_ctx,
                        &tidb_expr::expression::Expression::Column(right.clone())
                    )
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
                sorted_expressions_text(eval_ctx, left_conditions)
            ));
        } else {
            // Go's `PhysicalHashJoin` non-normalized `left cond` is the ONLY
            // bracketed condition list, and it keeps the original order.
            let rendered = left_conditions
                .iter()
                .map(|expression| expression_text(eval_ctx, expression))
                .collect::<Vec<_>>()
                .join(" ");
            parts.push(format!("left cond:[{rendered}]"));
        }
    }
    if !right_conditions.is_empty() {
        parts.push(format!(
            "right cond:{}",
            sorted_expressions_text(eval_ctx, right_conditions)
        ));
    }
    if !other_conditions.is_empty() {
        parts.push(format!(
            "other cond:{}",
            sorted_expressions_text(eval_ctx, other_conditions)
        ));
    }
    parts.join(", ")
}

fn aggregate_info(
    eval_ctx: &dyn tidb_exprctx::Columns,
    functions: &[tidb_expression_aggregation::AggFuncDesc],
    group_by: &[tidb_expr::expression::Expression],
    schema: Option<&tidb_expr::schema::Schema>,
) -> String {
    // Go `BasePhysicalAgg.explainInfo` (`base_physical_agg.go:888-916`)
    // writes the two sections into ONE builder: the group-by branch itself
    // ends with ", ", and each funcs entry is followed by ", " except the
    // last. A cop-level dedup whose `RemoveUnnecessaryFirstRow` removed its
    // only funcs therefore prints `group by:test.t2.a, ` WITH the trailing
    // separator -- joining separate parts here would trim it.
    let mut text = String::new();
    if !group_by.is_empty() {
        // The GROUP BY items render with
        // `expression.SortedExplainExpressionList`, which SORTS the rendered
        // strings; the aggregate list below keeps its own order.
        text.push_str("group by:");
        text.push_str(&sorted_expressions_text(eval_ctx, group_by));
        text.push_str(", ");
    }
    for (index, function) in functions.iter().enumerate() {
        if index > 0 {
            text.push_str(", ");
        }
        let output = schema
            .and_then(|schema| schema.columns.get(index))
            .map(|column| {
                expression_text(
                    eval_ctx,
                    &tidb_expr::expression::Expression::Column(column.clone()),
                )
            })
            .unwrap_or_else(|| format!("Column#{index}"));
        text.push_str(&format!("funcs:{}->{output}", agg_func_text(eval_ctx, function)));
    }
    text
}

/// Go `aggregation.ExplainAggFunc(ctx, agg, false)` (`explain.go:26`): a
/// GROUP_CONCAT's last argument is its separator, printed after the
/// aggregate's own ORDER BY.
fn agg_func_text(
    eval_ctx: &dyn tidb_exprctx::Columns,
    function: &tidb_expression_aggregation::AggFuncDesc,
) -> String {
    let mut text = format!("{}(", function.base.name);
    if function.has_distinct {
        text.push_str("distinct ");
    }
    let count = function.base.args.len();
    for (index, argument) in function.base.args.iter().enumerate() {
        if function.base.name == tidb_expression_aggregation::names::GROUP_CONCAT && index + 1 == count {
            if !function.order_by_items.is_empty() {
                text.push_str(" order by ");
                let items = function
                    .order_by_items
                    .iter()
                    .map(|item| {
                        let rendered = expression_text(eval_ctx, &item.expr);
                        if item.desc {
                            format!("{rendered} desc")
                        } else {
                            rendered
                        }
                    })
                    .collect::<Vec<_>>();
                text.push_str(&items.join(", "));
            }
            text.push_str(" separator ");
        } else if index != 0 {
            text.push_str(", ");
        }
        text.push_str(&expression_text(eval_ctx, argument));
    }
    text.push(')');
    text
}

/// Go `PhysicalWindow.ExplainInfo` (`physical_window.go:167`) and
/// `FormatWindowFuncDescs` (`:218`): the window functions' own renderings
/// (each consuming its trailing schema column) followed by
/// `over(partition by ... order by ... <frame>)`.
fn window_info(
    eval_ctx: &dyn tidb_exprctx::Columns,
    window: &tidb_planner_core_operator_physicalop::PhysicalWindow,
) -> String {
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
                .map(|column| expression_text(eval_ctx, &Expression::Column(column.clone())))
                .unwrap_or_else(|| format!("Column#{}", result_start + index));
            format!(
                "{}({})->{output}",
                descriptor.base.name,
                expressions_text(eval_ctx, &descriptor.base.args)
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
                .map(|item| expression_text(eval_ctx, &Expression::Column(item.col.clone())))
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
                    let text = expression_text(eval_ctx, &Expression::Column(item.col.clone()));
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
            tidb_planner_core_operator_logicalop::window::FrameType::Rows => "rows",
            tidb_planner_core_operator_logicalop::window::FrameType::Ranges => "range",
            tidb_planner_core_operator_logicalop::window::FrameType::Groups => "groups",
        };
        parts.push(format!(
            "{kind} between {} and {}",
            frame_bound_text(eval_ctx, frame.start.as_ref()),
            frame_bound_text(eval_ctx, frame.end.as_ref())
        ));
    }
    format!("{functions} over({})", parts.join(" "))
}

/// Go `PhysicalWindow.formatFrameBound`: a RANGE bound prints the operand of
/// its calc function (`interval "2:30" "MINUTE_SECOND"` for date arithmetic,
/// the offset constant for plus/minus), a ROWS bound its row count.
fn frame_bound_text(
    eval_ctx: &dyn tidb_exprctx::Columns,
    bound: Option<&tidb_planner_core_operator_logicalop::window::FrameBound>,
) -> String {
    use tidb_expr::expression::Expression;
    use tidb_planner_core_operator_logicalop::window::BoundType;
    let Some(bound) = bound else {
        return String::new();
    };
    if bound.bound_type == BoundType::CurrentRow {
        return "current row".to_owned();
    }
    let direction = if bound.bound_type == BoundType::Preceding {
        "preceding"
    } else {
        "following"
    };
    let operand = if bound.unbounded {
        "unbounded".to_owned()
    } else if let Some(calc) = bound.calc_funcs.first() {
        match calc {
            Expression::ScalarFunction(function) => {
                let args = function.get_args();
                match function.func_name.lowercase() {
                    // Go `FrameBound.String`: `interval <value> <unit>`, the
                    // unit being the function's third argument.
                    "date_add" | "date_sub" => format!(
                        "interval {} {}",
                        args.get(1)
                            .map(|arg| expression_text(eval_ctx, arg))
                            .unwrap_or_default(),
                        args.get(2)
                            .map(|arg| expression_text(eval_ctx, arg))
                            .unwrap_or_default(),
                    ),
                    "plus" | "minus" => args
                        .get(1)
                        .map(|arg| expression_text(eval_ctx, arg))
                        .unwrap_or_default(),
                    _ => String::new(),
                }
            }
            _ => String::new(),
        }
    } else {
        bound.num.to_string()
    };
    format!("{operand} {direction}")
}

fn scan_ranges_text(ranges: &tidb_ranger::types::Ranges) -> String {
    ranges
        .iter()
        .map(tidb_ranger::types::Range::to_display_string)
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
                .map_or_else(|| table_id.to_string(), |table| table.name())
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
    hypo_indexes: Option<&tidb_variable::hypo_index::HypoIndexes>,
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
    // Go prints the plan's own `p.Index`, which for an EXPLAIN may be one of
    // the session's hypothetical indexes rather than one the table stores.
    let hypo_index = || {
        let database = catalog.physical_kv_table_database_by_id(table_id)?;
        tidb_table::kv_table::hypo_kv_indexes(hypo_indexes?, database, &table)
            .into_iter()
            .find(|index| index.id == index_id)
    };
    let Some(index) = table
        .plan_indexes()
        .find(|index| index.id == index_id)
        .cloned()
        .or_else(hypo_index)
    else {
        if let Some(name) = index_name {
            table_access.indexes.push(IndexAccess {
                name: name.to_owned(),
                ..IndexAccess::default()
            });
        }
        return table_access;
    };
    // Go `PhysicalIndexScan.AccessObject`: a hidden expression-index column
    // prints its `GeneratedExprString`.
    let cols = index
        .column_offsets
        .iter()
        .filter_map(|offset| table.columns().get(*offset).map(|column| (*offset, column)))
        .map(|(offset, column)| match &column.generated {
            Some(generated) if table.is_hidden(offset) => generated.expr_text.clone(),
            _ => column.name.clone(),
        })
        .collect();
    table_access.indexes.push(IndexAccess {
        name: index.name.clone(),
        cols,
        is_clustered_index: false,
    });
    table_access
}

fn point_access(
    catalog: &Catalog,
    hypo_indexes: Option<&tidb_variable::hypo_index::HypoIndexes>,
    table_id: i64,
    index_id: Option<i64>,
) -> ScanAccessObject {
    match index_id {
        Some(index_id) => index_access(catalog, hypo_indexes, table_id, None, index_id, None),
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
                    .filter_map(|offset| table.columns().get(*offset))
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
    key_offsets: &'a [i64],
    /// Go `indexJoinResult.chosenAccess`: the conditions that extend the
    /// per-probe range past the equality keys.
    access_conditions: &'a [tidb_expr::expression::Expression],
    /// Master's `RangeInfo` spelling for this probe: the integer-handle
    /// branch renders bare outer keys, every other probe renders
    /// `eq(inner, outer)` pairs.
    range_bare: bool,
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
fn index_join_decided_by_text(
    eval_ctx: &dyn tidb_exprctx::Columns,
    context: IndexJoinExplainContext<'_>,
) -> String {
    let mut keys = context
        .inner_keys
        .iter()
        .zip(context.outer_keys)
        .enumerate()
        .collect::<Vec<_>>();
    keys.sort_by_key(|(key, _)| {
        context
            .key_offsets
            .get(*key)
            .copied()
            .unwrap_or(*key as i64)
    });
    let mut decided = keys
        .into_iter()
        .map(|(_, (inner, outer))| {
            format!(
                "eq({}, {})",
                expression_text(
                    eval_ctx,
                    &tidb_expr::expression::Expression::Column(inner.clone())
                ),
                expression_text(
                    eval_ctx,
                    &tidb_expr::expression::Expression::Column(outer.clone())
                )
            )
        })
        .collect::<Vec<_>>();
    decided.extend(
        context
            .access_conditions
            .iter()
            .map(|expression| expression_string_text(eval_ctx, expression)),
    );
    format!("range: decided by [{}]", decided.join(" "))
}

/// Go `indexJoinIntPKRangeInfo` (`index_join_path.go:597`): the INT-PK probe
/// scan's `decided by` text is the OUTER join keys rendered alone --
/// `indexJoinIntPKRangeInfo` writes each outer key with no eq wrapping.
fn index_join_int_pk_decided_by_text(
    eval_ctx: &dyn tidb_exprctx::Columns,
    context: &IndexJoinExplainContext<'_>,
) -> String {
    let decided = context
        .outer_keys
        .iter()
        .map(|outer| {
            expression_text(
                eval_ctx,
                &tidb_expr::expression::Expression::Column(outer.clone()),
            )
        })
        .collect::<Vec<_>>()
        .join(" ");
    format!("range: decided by [{}]", decided)
}

/// Integer table handles use Go's `indexJoinIntPKRangeInfo`; common handles
/// use the equality pairs for the matched index columns.
fn index_join_decided_by_text_for_scan(
    eval_ctx: &dyn tidb_exprctx::Columns,
    context: &IndexJoinExplainContext<'_>,
) -> String {
    // Master picks the integer-PK spelling when the join probes the table's
    // integer handle (`constructDS2TableScanTask`'s int-PK branch records the
    // bare `indexJoinIntPKRangeInfo`); every other probe (index path,
    // common handle, rowid mutable ranges) records the `eq(inner, outer)`
    // pairs of `indexJoinPathRangeInfo`. The join construction records which
    // branch selected the inner access.
    if context.range_bare {
        return index_join_int_pk_decided_by_text(eval_ctx, context);
    }
    index_join_decided_by_text(eval_ctx, *context)
}

fn columns_text(
    eval_ctx: &dyn tidb_exprctx::Columns,
    columns: &[tidb_expr::column::Column],
) -> String {
    columns
        .iter()
        .map(|column| {
            expression_text(
                eval_ctx,
                &tidb_expr::expression::Expression::Column(column.clone()),
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Go `PhysicalTableScan/PhysicalIndexScan.haveCorCol`: the access
/// conditions, when one of them reads a correlated column -- the scan's
/// ranges are rebuilt per outer row, so it is a range scan "decided by"
/// them.
fn correlated_access_conditions(plan: &PhysicalPlan) -> Option<&[tidb_expr::expression::Expression]> {
    let conditions = match plan {
        PhysicalPlan::TableScan(scan) => scan
            .range_rebuild
            .as_ref()
            .map(|rebuild| rebuild.access_conditions.as_slice()),
        PhysicalPlan::IndexScan(scan) => scan
            .range_rebuild
            .as_ref()
            .map(|rebuild| rebuild.access_conditions.as_slice()),
        _ => None,
    }?;
    conditions
        .iter()
        .any(|condition| !tidb_expr::simple_expr::extract_cor_columns(condition).is_empty())
        .then_some(conditions)
}

/// Go's `range: decided by [...]` over correlated access conditions, each
/// rendered with `StringWithCtx` and separated by a space.
fn correlated_decided_by_text(
    eval_ctx: &dyn tidb_exprctx::Columns,
    conditions: &[tidb_expr::expression::Expression],
) -> String {
    format!(
        "range: decided by [{}]",
        conditions
            .iter()
            .map(|condition| expression_string_text(eval_ctx, condition))
            .collect::<Vec<_>>()
            .join(" ")
    )
}

fn physical_operator_name(
    plan: &PhysicalPlan,
    index_join_context: Option<IndexJoinExplainContext<'_>>,
) -> String {
    match plan {
        // Go `PhysicalTableScan.TP`: a cluster table's scan is a memory
        // table scan whatever its ranges.
        PhysicalPlan::TableScan(scan)
            if scan.store_type == tidb_kv::store_type::StoreType::TiDb =>
        {
            "MemTableScan".to_owned()
        }
        PhysicalPlan::TableScan(scan) => {
            if is_index_join_table_range(plan, index_join_context)
                || correlated_access_conditions(plan).is_some()
            {
                "TableRangeScan".to_owned()
            } else {
                scan.scan_kind().map_or_else(
                    || "TableScan".to_owned(),
                    |kind| kind.plan_type().to_owned(),
                )
            }
        }
        PhysicalPlan::IndexScan(scan) => {
            let static_range = !scan.ranges.is_empty()
                && !tidb_ranger::types::has_full_range(&scan.ranges, false);
            // Go names an index scan `IndexRangeScan` when it carries either
            // static ranges or a runtime `RangeInfo`: the IndexJoin's inner
            // probe owns only the latter (the ranges are decided by the join
            // keys at run time), so the probe must not fall back to
            // `IndexFullScan` just because its static range list is empty.
            if static_range
                || is_index_join_index_range(plan, index_join_context)
                || correlated_access_conditions(plan).is_some()
            {
                "IndexRangeScan".to_owned()
            } else {
                "IndexFullScan".to_owned()
            }
        }
        PhysicalPlan::PointGet(_) => "Point_Get".to_owned(),
        PhysicalPlan::BatchPointGet(_) => "Batch_Point_Get".to_owned(),
        // Join physical operators share the logical `Join` base type, but
        // Go's plancodec names each executor family distinctly.
        PhysicalPlan::HashJoin(_) => "HashJoin".to_owned(),
        PhysicalPlan::MergeJoin(_) => "MergeJoin".to_owned(),
        PhysicalPlan::IndexJoin(join) => match join.kind {
            tidb_planner_core_operator_physicalop::IndexJoinKind::IndexJoin => "IndexJoin",
            tidb_planner_core_operator_physicalop::IndexJoinKind::IndexHashJoin => "IndexHashJoin",
            tidb_planner_core_operator_physicalop::IndexJoinKind::IndexMergeJoin => "IndexMergeJoin",
        }
        .to_owned(),
        _ => plan.tp().to_owned(),
    }
}

fn physical_access(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    hypo_indexes: Option<&tidb_variable::hypo_index::HypoIndexes>,
) -> Option<AccessObject> {
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
        // Go `PhysicalTableScan.AccessObject` names the scan's own `Table.Name.O`
        // (or its alias); a cluster table has no stored table to look up.
        PhysicalPlan::TableScan(scan)
            if scan.store_type == tidb_kv::store_type::StoreType::TiDb =>
        {
            Some(AccessObject::Scan(ScanAccessObject {
                database: scan.db_name.clone(),
                table: scan
                    .table_as_name
                    .as_deref()
                    .filter(|alias| !alias.is_empty())
                    .unwrap_or(&scan.table_name)
                    .to_owned(),
                ..ScanAccessObject::default()
            }))
        }
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
            hypo_indexes,
            scan.table_id,
            scan.table_as_name.as_deref(),
            scan.index_id,
            Some(&scan.index_name),
        ))),
        PhysicalPlan::PointGet(point) => {
            let id = point.partition.as_ref().and_then(|p| p.physical_table_id);
            let mut access = point_access(
                catalog,
                hypo_indexes,
                id.unwrap_or(point.table_id),
                point.index_id,
            );
            if point.partition.is_some() && id.is_none() {
                access.partitions = vec!["dual".to_owned()];
            }
            Some(AccessObject::Scan(access))
        }
        PhysicalPlan::BatchPointGet(point) => {
            let mut access = point_access(catalog, hypo_indexes, point.table_id, point.index_id);
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
                    cte.cte_name.go_to_lower()
                ))))
            } else {
                Some(AccessObject::Other(OtherAccessObject(format!(
                    "CTE:{} AS {}",
                    cte.cte_name.go_to_lower(),
                    cte.cte_as_name.go_to_lower()
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
            AccessObject::DynamicPartitions(tidb_planner_core_access::DynamicPartitionAccessObjects(
                vec![object],
            ))
        })
        .or_else(|| plan.children().iter().find_map(dynamic_partition_access))
}

fn point_handle_text(range: &tidb_ranger::types::Range) -> String {
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
fn point_handle_text_unsigned(range: &tidb_ranger::types::Range) -> String {
    match range.low_val.first() {
        Some(tidb_datatype::Datum::Int(value)) => format!("{}", *value as u64),
        Some(value) => value.sql_string().unwrap_or_default(),
        None => String::new(),
    }
}

/// The session facts a memory-table extractor's `ExplainInfo` reads.
struct MemTableExplainSession<'a> {
    eval_ctx: &'a dyn tidb_exprctx::Columns,
}

impl MemTableExplainSession<'_> {
    fn sysvar_int(&self, name: &str, default: i64) -> i64 {
        self.eval_ctx
            .sysvar(None, name)
            .and_then(|value| value.sql_string().ok())
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }
}

impl tidb_planner::memtable_predicate_extractor::MemTableExplainEnv for MemTableExplainSession<'_> {
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.eval_ctx.time_zone()
    }

    fn slow_query_file(&self) -> String {
        // Go's `SessionVars.SlowQueryFile` starts as the configured
        // `log.slow-query-file`, `tidb-slow.log` by default.
        self.eval_ctx
            .sysvar(None, tidb_vardef::tidb_vars::TIDB_SLOW_QUERY_FILE)
            .and_then(|value| value.sql_string().ok())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "tidb-slow.log".to_owned())
    }

    fn metric_schema_step(&self) -> i64 {
        self.sysvar_int(
            tidb_vardef::tidb_vars::TIDB_METRIC_SCHEMA_STEP,
            tidb_vardef::defaults::DEF_TIDB_METRIC_SCHEMA_STEP,
        )
    }

    fn metric_schema_range_duration(&self) -> i64 {
        self.sysvar_int(
            tidb_vardef::tidb_vars::TIDB_METRIC_SCHEMA_RANGE_DURATION,
            tidb_vardef::defaults::DEF_TIDB_METRIC_SCHEMA_RANGE_DURATION,
        )
    }
}

fn physical_operator_info(
    eval_ctx: &dyn tidb_exprctx::Columns,
    statement_context: &crate::StmtContext,
    plan: &PhysicalPlan,
    catalog: &Catalog,
    ignore_explain_id_suffix: bool,
    index_join_context: Option<IndexJoinExplainContext<'_>>,
) -> String {
    match plan {
        PhysicalPlan::Selection(selection) => {
            // Go `PhysicalSelection.ExplainInfo` renders through
            // `expression.SortedExplainExpressionList`: the conditions are
            // sorted by their rendered text, so the operator text is stable
            // regardless of the CNF extraction order.
            sorted_expressions_text(eval_ctx, &selection.conditions)
        }
        // Go `PhysicalUnionScan.ExplainInfo`: the sorted conditions.
        PhysicalPlan::UnionScan(union_scan) => {
            sorted_expressions_text(eval_ctx, &union_scan.conditions)
        }
        PhysicalPlan::Projection(projection) => {
            projection_text(eval_ctx, &projection.exprs, projection.base.base.schema())
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
                eval_ctx,
                join.join_type,
                join.base.children().first(),
                ignore_explain_id_suffix,
                false,
                &join.left_join_keys,
                &join.right_join_keys,
                &join.is_null_eq,
                &join.equal_conditions,
                &join.na_equal_conditions,
                &join.left_conditions,
                &join.right_conditions,
                &join.other_conditions,
            );
            format!("{prefix}{info}")
        }
        PhysicalPlan::MergeJoin(join) => join_info(
            eval_ctx,
            join.join_type,
            join.base.children().first(),
            ignore_explain_id_suffix,
            true,
            &join.left_join_keys,
            &join.right_join_keys,
            &join.is_null_eq,
            &[],
            &[],
            &join.left_conditions,
            &join.right_conditions,
            &join.other_conditions,
        ),
        PhysicalPlan::IndexJoin(join) => {
            let mut parts = vec![join_type_text(join.join_type).to_owned()];
            // Go `PhysicalIndexJoin.ExplainInfoInternal`
            // (`physical_index_join.go:159-165`): the `inner:` field comes
            // BEFORE `explainJoinLeftSide`'s `left side:` field. Go's
            // `ExplainID()` always reflects the child's own concrete
            // physical type because Go stores that type's own constant at
            // construction (`plancodec.TypeHashJoin`, etc). This tier's
            // `HashJoin`/`MergeJoin`/`IndexJoin` share one generic base type
            // string ("Join", see `physical_operator_name`'s own comment on
            // why it re-derives the name from the enum variant instead of
            // trusting it), so naming this cross-reference the same way
            // `plan_explain_id` (which DOES re-derive it) already names
            // `left side:` below keeps the two fields consistent instead of
            // one trusting the shared, generic stored name.
            if let Some(inner) = join.base.children().get(join.inner_child_idx) {
                parts.push(format!(
                    "inner:{}",
                    plan_explain_id(inner, ignore_explain_id_suffix)
                ));
            }
            if join.join_type != tidb_planner_core_base::LogicalJoinType::Inner {
                if let Some(left) = join.base.children().first() {
                    parts.push(format!(
                        "left side:{}",
                        plan_explain_id(left, ignore_explain_id_suffix)
                    ));
                }
            }
            if !join.outer_join_keys.is_empty() {
                parts.push(format!(
                    "outer key:{}",
                    columns_text(eval_ctx, &join.outer_join_keys)
                ));
            }
            if !join.inner_join_keys.is_empty() {
                parts.push(format!(
                    "inner key:{}",
                    columns_text(eval_ctx, &join.inner_join_keys)
                ));
            }
            if !join.outer_hash_keys.is_empty()
                && join.kind != tidb_planner_core_operator_physicalop::IndexJoinKind::IndexMergeJoin
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
                            expression_text(
                                eval_ctx,
                                &tidb_expr::expression::Expression::Column(outer.clone())
                            ),
                            expression_text(
                                eval_ctx,
                                &tidb_expr::expression::Expression::Column(inner.clone())
                            ),
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
                    sorted_expressions_text(eval_ctx, &join.left_conditions)
                ));
            }
            if !join.right_conditions.is_empty() {
                parts.push(format!(
                    "right cond:{}",
                    sorted_expressions_text(eval_ctx, &join.right_conditions)
                ));
            }
            if !join.other_conditions.is_empty() {
                parts.push(format!(
                    "other cond:{}",
                    sorted_expressions_text(eval_ctx, &join.other_conditions)
                ));
            }
            parts.join(", ")
        }
        PhysicalPlan::Apply(apply) => {
            // Go `BasePhysicalJoin` renders through the shared join explain:
            // a cartesian apply (no equal or null-aware conditions) prefixes
            // `CARTESIAN ` (physical_hash_join.go:231).
            let prefix = if apply.hash_join.equal_conditions.is_empty() {
                if apply.hash_join.na_equal_conditions.is_empty() {
                    "CARTESIAN "
                } else {
                    "Null-aware "
                }
            } else {
                ""
            };
            let info = join_info(
                eval_ctx,
                apply.hash_join.join_type,
                apply.hash_join.base.children().first(),
                ignore_explain_id_suffix,
                false,
                &apply.hash_join.left_join_keys,
                &apply.hash_join.right_join_keys,
                &apply.hash_join.is_null_eq,
                &apply.hash_join.equal_conditions,
                &apply.hash_join.na_equal_conditions,
                &apply.hash_join.left_conditions,
                &apply.hash_join.right_conditions,
                &apply.hash_join.other_conditions,
            );
            format!("{prefix}{info}")
        }
        PhysicalPlan::Sort(sort) => by_items_text(eval_ctx, &sort.by_items),
        PhysicalPlan::Limit(limit) => limit.explain_info(RedactMode::Disable),
        // Go `PhysicalTableScan.OperatorInfo`: nothing for a cluster table.
        PhysicalPlan::TableScan(scan)
            if scan.store_type == tidb_kv::store_type::StoreType::TiDb =>
        {
            String::new()
        }
        PhysicalPlan::TableScan(scan) => {
            let mut parts = Vec::new();
            if is_index_join_table_range(plan, index_join_context) {
                let ctx = index_join_context.expect("index join context");
                // Table handle metadata selects Go's integer-PK or common
                // handle range description.
                let parts_text = index_join_decided_by_text_for_scan(eval_ctx, &ctx);
                parts.push(parts_text);
            } else if let Some(conditions) = correlated_access_conditions(plan) {
                parts.push(correlated_decided_by_text(eval_ctx, conditions));
            } else if scan
                .scan_kind()
                .is_some_and(|kind| kind.plan_type() == "TableRangeScan")
                && !scan.ranges.is_empty()
            {
                parts.push(format!("range:{}", scan_ranges_text(&scan.ranges)));
            }
            parts.push(scan_keep_order_text(scan.keep_order, scan.desc));
            if let Some(stats) = used_stats_suffix_for_scan(
                statement_context,
                scan.table_id,
                &scan.base,
                catalog,
                None,
            ) {
                parts.push(stats);
            }
            parts.join(", ")
        }
        PhysicalPlan::TableDual(dual) => dual.explain_info(),
        PhysicalPlan::TableSample(_) => String::new(),
        PhysicalPlan::MemTable(scan) => tidb_planner::memtable_predicate_extractor::extractor_of(scan).map_or_else(String::new, |e| {
            e.explain_info(&scan.table_name, &MemTableExplainSession { eval_ctx })
        }),
        PhysicalPlan::CTE(cte) => cte.operator_info(),
        PhysicalPlan::CTETable(table) => table.explain_info(),
        PhysicalPlan::Lock(lock) => lock.explain_info(),
        PhysicalPlan::Sequence(_) => {
            tidb_planner_core_operator_physicalop::PhysicalSequence::explain_info().to_owned()
        }
        // Go `PhysicalTableReader.ExplainInfo`: the table plan's ExplainID,
        // which names a scan with runtime or correlated ranges
        // `TableRangeScan` exactly as the scan's own row does.
        PhysicalPlan::TableReader(reader) => match reader.table_plan.as_deref() {
            Some(scan @ PhysicalPlan::TableScan(_))
                if reader.read_req_type != tidb_planner_core_operator_physicalop::physical_table_reader::ReadReqType::Mpp =>
            {
                let name = physical_operator_name(scan, index_join_context);
                if ignore_explain_id_suffix {
                    format!("data:{name}")
                } else {
                    format!("data:{name}_{}", scan.base().base.id())
                }
            }
            _ => reader.explain_info(ignore_explain_id_suffix),
        },
        PhysicalPlan::IndexScan(scan) => {
            let mut parts = Vec::new();
            if is_index_join_index_range(plan, index_join_context) {
                parts.push(index_join_decided_by_text(
                    eval_ctx,
                    index_join_context.expect("index join context"),
                ));
            } else if let Some(conditions) = correlated_access_conditions(plan) {
                parts.push(correlated_decided_by_text(eval_ctx, conditions));
            } else if !scan.ranges.is_empty()
                && !tidb_ranger::types::has_full_range(&scan.ranges, false)
            {
                parts.push(format!("range:{}", scan_ranges_text(&scan.ranges)));
            }
            parts.push(scan_keep_order_text(scan.keep_order, scan.desc));
            if let Some(stats) = used_stats_suffix_for_scan(
                statement_context,
                scan.table_id,
                &scan.base,
                catalog,
                Some(scan.index_id),
            ) {
                parts.push(stats);
            }
            parts.join(", ")
        }
        // Go `PhysicalIndexReader.ExplainInfo`: the index plan's ExplainID,
        // which names an index-join probe `IndexRangeScan` from its runtime
        // RangeInfo exactly as the scan's own row does.
        PhysicalPlan::IndexReader(reader) => format!(
            "index:{}",
            reader
                .index_plan
                .as_deref()
                .map_or_else(String::new, |plan| match plan {
                    PhysicalPlan::IndexScan(_) => {
                        let name = physical_operator_name(plan, index_join_context);
                        if ignore_explain_id_suffix {
                            name
                        } else {
                            format!("{name}_{}", plan.base().base.id())
                        }
                    }
                    _ => plan.explain_id(ignore_explain_id_suffix),
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
        PhysicalPlan::ExchangeSender(sender) => sender.explain_info(ignore_explain_id_suffix),
        PhysicalPlan::ExchangeReceiver(receiver) => receiver.explain_info(),
        PhysicalPlan::PointGet(point) => {
            let common_handle = catalog
                .kv_table_by_id(point.table_id)
                .is_some_and(|table| !table.common_handle_offsets().is_empty());
            let mut info = if point.index_id.is_some() || common_handle {
                String::new()
            } else {
                point.ranges.first().map_or_else(String::new, |range| {
                    format!("handle:{}", point_handle_text(range))
                })
            };
            // Go `PointGetPlan.ExplainInfo` renders `lock` for the
            // UPDATE/DELETE read: `OperatorInfo` appends it whenever
            // `p.Lock` is set — which is the case for the harness's
            // transactional (autocommit-off) sessions (W4/W6).
            if point.lock {
                if info.is_empty() {
                    info = "lock".to_owned();
                } else {
                    info.push_str(", lock");
                }
            }
            info
        }
        PhysicalPlan::BatchPointGet(batch) => {
            let common_handle = catalog
                .kv_table_by_id(batch.table_id)
                .is_some_and(|table| !table.common_handle_offsets().is_empty());
            let prefix = if batch.index_id.is_none() && !common_handle {
                let handle_text = |range: &tidb_ranger::types::Range| {
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
            let mut order = format!("keep order:{}, desc:{}", batch.keep_order, batch.desc);
            if batch.lock {
                order.push_str(", lock");
            }
            if prefix.is_empty() {
                order
            } else {
                format!("{prefix}, {order}")
            }
        }
        PhysicalPlan::IndexMergeReader(reader) => {
            // Go `PhysicalIndexMergeReader.ExplainInfo`: the merge type, then
            // the embedded limit when one was pushed. KeepOrder is NOT
            // printed on this operator.
            let mut text = format!(
                "type: {}",
                if reader.is_intersection_type {
                    "intersection"
                } else {
                    "union"
                }
            );
            if let Some(limit) = reader.pushed_limit.as_ref() {
                text.push_str(
                    format!(
                        ", limit embedded(offset:{}, count:{})",
                        limit.offset, limit.count
                    )
                    .as_str(),
                );
            }
            text
        }
        PhysicalPlan::Dml(_) => "N/A".to_owned(),
        PhysicalPlan::TopN(topn) => topn_info(eval_ctx, topn),
        PhysicalPlan::HashAgg(aggregation) => aggregate_info(
            eval_ctx,
            &aggregation.agg_funcs,
            &aggregation.group_by_items,
            aggregation.base.base.schema(),
        ),
        PhysicalPlan::StreamAgg(aggregation) => aggregate_info(
            eval_ctx,
            &aggregation.agg_funcs,
            &aggregation.group_by_items,
            aggregation.base.base.schema(),
        ),
        PhysicalPlan::Shuffle(shuffle) => shuffle
            .explain_info(ignore_explain_id_suffix)
            .unwrap_or_else(|error| format!("{error:?}")),
        PhysicalPlan::Window(window) => window_info(eval_ctx, window),
        // Go `PhysicalExpand.explainInfoV2`: each level is an
        // `ExplainExpressionList` over the Expand's schema, and the schema
        // columns are joined without a space.
        PhysicalPlan::Expand(expand) => format!(
            "level-projection:{}; schema: [{}]",
            expand
                .level_exprs
                .iter()
                .map(|level| format!("[{}]", projection_text(eval_ctx, level, plan.schema())))
                .collect::<Vec<_>>()
                .join(","),
            plan.schema()
                .map(|schema| {
                    schema
                        .columns
                        .iter()
                        .map(|column| {
                            expression_text(
                                eval_ctx,
                                &tidb_expr::expression::Expression::Column(column.clone()),
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_default()
        ),
        PhysicalPlan::MaxOneRow(_)
        | PhysicalPlan::ShuffleReceiver(_)
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
    costs: Option<&Ver2Coster>,
    eval_ctx: &dyn tidb_exprctx::Columns,
    statement_context: &crate::StmtContext,
    brief_binary: bool,
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
    // Price parents before children so scans retain their reader/task context,
    // matching Go's cached GetPlanCostVer2 during top-down EXPLAIN rendering.
    let priced = costs.filter(|_| !matches!(plan, PhysicalPlan::Dml(_))).map(|coster| {
        match coster.cost_option() {
            Some(option) => coster.plan_cost_with_option(
                plan,
                tidb_planner_property::task_type::TaskType::Root,
                false,
                *option,
            ),
            None => coster.plan_cost(plan, tidb_planner_property::task_type::TaskType::Root, false),
        }
    });
    let estimated_cost = priced.as_ref().map(|cost| cost.value());
    let cost_formula = priced
        .as_ref()
        .and_then(|cost| cost.trace().map(|trace| trace.formula().to_owned()));
    let mut children = match plan {
        PhysicalPlan::ShuffleReceiver(receiver) => vec![physical_explain_operator(
            costs,
            eval_ctx,
            statement_context,
            brief_binary,
            &receiver.data_source,
            catalog,
            ExplainTask::Root,
            "",
            runtime,
            ignore_explain_id_suffix,
            index_join_context,
            probe_count,
        )],
        PhysicalPlan::TableReader(reader) => reader
            .table_plan
            .as_deref()
            .map(|child| {
                // Go `explainChildOperator` renders the pushed-down tier's
                // task from the reader's request type and store: an MPP
                // reader (TiFlash store over an ExchangeSender,
                // `physical_table_reader.go:299` adjustReadReqType) labels
                // the fragment `mpp[tiflash]`; everything else so far is a
                // plain cop request.
                let task = match reader.read_req_type {
                    tidb_planner_core_operator_physicalop::physical_table_reader::ReadReqType::Mpp => ExplainTask::Cop {
                        request: "mpp".to_owned(),
                        store: "tiflash".to_owned(),
                    },
                    tidb_planner_core_operator_physicalop::physical_table_reader::ReadReqType::BatchCop
                    | tidb_planner_core_operator_physicalop::physical_table_reader::ReadReqType::Cop
                    | tidb_planner_core_operator_physicalop::physical_table_reader::ReadReqType::Unknown(_) => {
                        ExplainTask::Cop {
                            request: "cop".to_owned(),
                            store: match reader.store_type {
                                tidb_kv::store_type::StoreType::TiFlash => {
                                    "tiflash".to_owned()
                                }
                                tidb_kv::store_type::StoreType::TiDb => {
                                    "tidb".to_owned()
                                }
                                tidb_kv::store_type::StoreType::TiKv
                                | tidb_kv::store_type::StoreType::Unspecified => {
                                    "tikv".to_owned()
                                }
                            },
                        }
                    }
                };
                vec![physical_explain_operator(
                    costs,
                    eval_ctx,
                    statement_context,
                    brief_binary,
                    child,
                    catalog,
                    task,
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
                    costs,
                    eval_ctx,
                    statement_context,
                    brief_binary,
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
                costs,
                eval_ctx,
                statement_context,
                brief_binary,
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
                    costs,
                    eval_ctx,
                    statement_context,
                    brief_binary,
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
        PhysicalPlan::Dml(root) => {
            let mut children = root
                .select_plan
                .as_deref()
                .map(|child| {
                    vec![physical_explain_operator(
                        costs,
                        eval_ctx,
                        statement_context,
                        brief_binary,
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
                .unwrap_or_default();
            // Go `flat_plan.go`: the FK checks and cascades flatten as the
            // DML root's remaining children, after the select source.
            for trigger in &root.fk_triggers {
                children.push(ExplainOperator {
                    operator: trigger.operator.to_owned(),
                    id: trigger.id,
                    label: String::new(),
                    estimated_rows: Some(0.0),
                    estimated_cost: None,
            cost_formula: None,
                    actual_rows: None,
                    execution_info: None,
                    memory_info: None,
                    disk_info: None,
                    task: ExplainTask::Root,
                    access_object: Some(AccessObject::Other(OtherAccessObject(
                        trigger.access.clone(),
                    ))),
                    operator_info: trigger.info.clone(),
                    children: Vec::new(),
                    fixed_id: None,
                    build_side_swapped: false,
                });
            }
            children
        }
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
                            key_offsets: &join.key_off2_idx_off,
                            access_conditions: &join.inner_access_conditions,
                            range_bare: join.inner_range_bare,
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
                            .map_or(1.0, tidb_planner_property::stats_info::StatsInfo::row_count);
                        probe_count * outer_rows
                    }
                    _ => probe_count,
                };
                physical_explain_operator(
                    costs,
                    eval_ctx,
                    statement_context,
                    brief_binary,
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
            if join.join_type == tidb_planner_core_base::LogicalJoinType::RightOuter {
                0
            } else {
                1
            },
        ),
        // Go `flat_plan.go`: the index subtree builds, the cloned table scan
        // probes.
        PhysicalPlan::LocalIndexLookUp(_) => Some(0),
        _ => None,
    };
    let mut build_side_swapped = false;
    if let Some(build) = labels.filter(|_| children.len() == 2) {
        children[build].label = "(Build)".to_owned();
        children[1 - build].label = "(Probe)".to_owned();
        if build == 1 {
            children.swap(0, 1);
            build_side_swapped = true;
        }
    }

    let name = physical_operator_name(plan, index_join_context);
    let info = if brief_binary && !has_brief_operator_info(&name) {
        String::new()
    } else {
        physical_operator_info(
            eval_ctx,
            statement_context,
            plan,
            catalog,
            ignore_explain_id_suffix,
            index_join_context,
        )
    };
    let mut operator = ExplainOperator::new(name, plan.id())
        .with_task(task)
        .with_operator_info(info)
        .with_children(children);
    if let Some(access_object) = physical_access(plan, catalog, statement_context.hypo_indexes()) {
        operator = operator.with_access_object(access_object);
    }
    operator.estimated_cost = estimated_cost;
    operator.cost_formula = cost_formula;
    operator.label = label.to_owned();
    operator.build_side_swapped = build_side_swapped;
    if let Some(rows) = plan
        .stats_info()
        .map(tidb_planner_property::stats_info::StatsInfo::row_count)
    {
        operator = operator.with_estimated_rows(rows * probe_count);
    }
    if let Some(rows) = runtime_rows(plan, runtime) {
        operator = operator.with_actual_rows(rows);
    }
    // Go `getRuntimeInfo`: `StmtCtx.MemTracker.SearchTrackerWithoutLock(p.ID())`
    // and the same on the disk tracker, formatted by `FormatBytes`.
    // A pushed operator has no tracker in Go (a local fallback's is not
    // Go's).
    if runtime.is_some() && matches!(operator.task, ExplainTask::Root) {
        let memory = statement_context.statement_memory();
        let label = i64::from(plan.id());
        operator.memory_info = memory
            .stmt_tracker()
            .search_tracker(label)
            .map(|tracker| tidb_memory::format_bytes(tracker.max_consumed()));
        operator.disk_info = memory
            .stmt_disk_tracker()
            .search_tracker(label)
            .map(|tracker| tidb_memory::format_bytes(tracker.max_consumed()));
    }
    // Go `getRuntimeInfoStr`: a pushed operator reports what the cop tasks
    // returned in their execution summaries. A local fallback for a cop plan
    // is not a TiKV timing measurement, so it reports nothing.
    if !matches!(operator.task, ExplainTask::Root) {
        if let Some(cop) = statement_context
            .executor_runtime_stats_source()
            .and_then(|source| source.cop_runtime_info(i64::from(plan.id())))
        {
            // `rootStats.String() + ", " + copStats.String()`.
            let groups = statement_context
                .executor_runtime_stats_source()
                .and_then(|source| source.root_runtime_info(i64::from(plan.id())));
            let info = match groups {
                Some(groups) if !cop.info.is_empty() => format!("{groups}, {}", cop.info),
                Some(groups) => groups,
                None => cop.info,
            };
            operator = operator
                .with_actual_rows(u64::try_from(cop.act_rows).unwrap_or_default())
                .with_execution_info(info);
        }
    }
    // Only publish the runtime of the actual root executor at this seam.
    if matches!(operator.task, ExplainTask::Root) {
        if let Some(info) = runtime
            .and_then(|stats| stats.get(&crate::driver::physical_builder::runtime_plan_key(plan)))
            .and_then(|counter| counter.execution_info())
        {
            // Go `RootRuntimeStats.String()`: the basic stats, then the
            // group stats registered on the plan (a reader's select result).
            let info = match statement_context
                .executor_runtime_stats_source()
                .and_then(|source| source.root_group_runtime_info(i64::from(plan.id())))
            {
                Some(groups) => format!("{info}, {groups}"),
                None => info,
            };
            operator = operator.with_execution_info(info);
        }
    }
    operator
}

fn cte_definitions(
    costs: Option<&Ver2Coster>,
    eval_ctx: &dyn tidb_exprctx::Columns,
    statement_context: &crate::StmtContext,
    brief_binary: bool,
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
                costs,
                eval_ctx,
                statement_context,
                brief_binary,
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
                    costs,
                    eval_ctx,
                    statement_context,
                    brief_binary,
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
                .with_fixed_id(format!("CTE_{}", cte.id_for_storage))
                .with_operator_info(if cte.recursive_plan.is_some() {
                    "Recursive CTE"
                } else {
                    "Non-Recursive CTE"
                })
                .with_children(children);
            definition.estimated_rows = plan
                .stats_info()
                .map(tidb_planner_property::stats_info::StatsInfo::row_count);
            out.push(definition);
        }
        cte_definitions(
            costs,
            eval_ctx,
            statement_context,
            brief_binary,
            &cte.seed_plan,
            catalog,
            runtime,
            seen,
            out,
            ignore_explain_id_suffix,
        );
        if let Some(recursive) = cte.recursive_plan.as_deref() {
            cte_definitions(
                costs,
                eval_ctx,
                statement_context,
                brief_binary,
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
        cte_definitions(
            costs,
            eval_ctx,
            statement_context,
            brief_binary,
            child,
            catalog,
            runtime,
            seen,
            out,
            ignore_explain_id_suffix,
        );
    }
    match plan {
        PhysicalPlan::ShuffleReceiver(receiver) => cte_definitions(
            costs,
            eval_ctx,
            statement_context,
            brief_binary,
            &receiver.data_source,
            catalog,
            runtime,
            seen,
            out,
            ignore_explain_id_suffix,
        ),
        PhysicalPlan::TableReader(reader) => {
            if let Some(child) = reader.table_plan.as_deref() {
                cte_definitions(
                    costs,
                    eval_ctx,
                    statement_context,
                    brief_binary,
                    child,
                    catalog,
                    runtime,
                    seen,
                    out,
                    ignore_explain_id_suffix,
                );
            }
        }
        PhysicalPlan::IndexReader(reader) => {
            if let Some(child) = reader.index_plan.as_deref() {
                cte_definitions(
                    costs,
                    eval_ctx,
                    statement_context,
                    brief_binary,
                    child,
                    catalog,
                    runtime,
                    seen,
                    out,
                    ignore_explain_id_suffix,
                );
            }
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            if let Some(child) = reader.index_plan.as_deref() {
                cte_definitions(
                    costs,
                    eval_ctx,
                    statement_context,
                    brief_binary,
                    child,
                    catalog,
                    runtime,
                    seen,
                    out,
                    ignore_explain_id_suffix,
                );
            }
            if let Some(child) = reader.table_plan.as_deref() {
                cte_definitions(
                    costs,
                    eval_ctx,
                    statement_context,
                    brief_binary,
                    child,
                    catalog,
                    runtime,
                    seen,
                    out,
                    ignore_explain_id_suffix,
                );
            }
        }
        PhysicalPlan::Dml(root) => {
            if let Some(child) = root.select_plan.as_deref() {
                cte_definitions(
                    costs,
                    eval_ctx,
                    statement_context,
                    brief_binary,
                    child,
                    catalog,
                    runtime,
                    seen,
                    out,
                    ignore_explain_id_suffix,
                );
            }
        }
        _ => {}
    }
}

fn physical_explain_roots(
    costs: Option<&Ver2Coster>,
    eval_ctx: &dyn tidb_exprctx::Columns,
    statement_context: &crate::StmtContext,
    brief_binary: bool,
    physical: &PhysicalPlan,
    catalog: &Catalog,
    runtime: Option<&crate::driver::physical_builder::PhysicalRuntimeStats>,
    ignore_explain_id_suffix: bool,
    scalar_subqueries: &[crate::driver::planner_bridge::RegisteredScalarSubquery],
) -> Vec<ExplainOperator> {
    let mut roots = vec![physical_explain_operator(
        costs,
        eval_ctx,
        statement_context,
        brief_binary,
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
        costs,
        eval_ctx,
        statement_context,
        brief_binary,
        physical,
        catalog,
        runtime,
        &mut std::collections::BTreeSet::new(),
        &mut roots,
        ignore_explain_id_suffix,
    );
    for subquery in scalar_subqueries {
        roots.push(scalar_subquery_root(
            costs,
            eval_ctx,
            statement_context,
            brief_binary,
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
    costs: Option<&Ver2Coster>,
    eval_ctx: &dyn tidb_exprctx::Columns,
    statement_context: &crate::StmtContext,
    brief_binary: bool,
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
    ExplainOperator::new("ScalarSubQuery", subquery.plan_id)
        .with_operator_info(format!("Output: {output}"))
        .with_children(vec![physical_explain_operator(
            costs,
            eval_ctx,
            statement_context,
            brief_binary,
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

// Go binaryOpTreeFromFlatOps rerenders only operators with ID-sensitive
// brief metadata. Rerendering a Selection would evaluate its constants twice.
fn has_brief_operator_info(operator: &str) -> bool {
    matches!(
        operator,
        "TableReader" | "IndexReader" | "HashJoin" | "IndexJoin" | "IndexHashJoin" | "MergeJoin"
    )
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
    let brief_operator_info = has_brief_operator_info(&full.operator)
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
pub fn brief_binary_plan(
    statement_context: &crate::StmtContext,
    physical: &PhysicalPlan,
    catalog: &Catalog,
) -> String {
    let full = physical_explain_roots(
        None,
        statement_context,
        statement_context,
        false,
        physical,
        catalog,
        None,
        false,
        &[],
    );
    binary_plan_from_roots(statement_context, physical, catalog, full)
}

fn binary_plan_from_roots(
    statement_context: &crate::StmtContext,
    physical: &PhysicalPlan,
    catalog: &Catalog,
    mut full: Vec<ExplainOperator>,
) -> String {
    let mut brief = physical_explain_roots(
        None,
        statement_context,
        statement_context,
        true,
        physical,
        catalog,
        None,
        true,
        &[],
    );
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
    tidb_plancodec::compress(&data.encode_to_vec())
}

fn first_table_scan(plan: &PhysicalPlan) -> Option<&tidb_planner_core_operator_physicalop::PhysicalTableScan> {
    match plan {
        PhysicalPlan::ShuffleReceiver(receiver) => first_table_scan(&receiver.data_source),
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

fn first_index_scan(plan: &PhysicalPlan) -> Option<&tidb_planner_core_operator_physicalop::PhysicalIndexScan> {
    match plan {
        PhysicalPlan::ShuffleReceiver(receiver) => first_index_scan(&receiver.data_source),
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
        .map_or(physical_id, |table| table.table_id())
}

fn index_process_name(
    catalog: &Catalog,
    scan: &tidb_planner_core_operator_physicalop::PhysicalIndexScan,
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
        PhysicalPlan::ShuffleReceiver(receiver) => {
            collect_executor_process_fields(&receiver.data_source, catalog, table_ids, index_names)
        }
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
                            index_names.push(format!("{}:{}", table.name(), primary.name));
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
                        index_names.push(format!("{}:{}", table.name(), index.name));
                    }
                }
            }
        }
        PhysicalPlan::BatchPointGet(point) => {
            if let Some(index_id) = point.index_id {
                if let Some(table) = catalog.physical_kv_table_by_id(point.table_id) {
                    if let Some(index) = table.indexes().iter().find(|index| index.id == index_id) {
                        index_names.push(format!("{}:{}", table.name(), index.name));
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

/// Go `CollectPlanStatsVersion`: each scan's `StatsInfo().StatsVersion`, which
/// `initStats` takes from the table `GetStatsTable` planned it from --
/// `statistics.PseudoVersion` (zero) for a pseudo table.
fn collect_stats_info(
    plan: &PhysicalPlan,
    ctx: &crate::StmtContext,
    catalog: &Catalog,
    stats_info: &mut std::collections::HashMap<String, u64>,
) {
    let stats_version = |physical_table_id| {
        crate::access_cost::get_stats_table(ctx, catalog, physical_table_id)
            .filter(|statistics| !statistics.hist_coll.pseudo)
            .map_or(tidb_stats::PSEUDO_VERSION, |statistics| statistics.version)
    };
    for child in plan.children() {
        collect_stats_info(child, ctx, catalog, stats_info);
    }
    match plan {
        PhysicalPlan::ShuffleReceiver(receiver) => {
            collect_stats_info(&receiver.data_source, ctx, catalog, stats_info);
        }
        PhysicalPlan::TableReader(reader) => {
            if let Some(inner) = reader.table_plan.as_deref() {
                collect_stats_info(inner, ctx, catalog, stats_info);
            }
        }
        PhysicalPlan::IndexReader(reader) => {
            if let Some(inner) = reader.index_plan.as_deref() {
                collect_stats_info(inner, ctx, catalog, stats_info);
            }
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            if let Some(inner) = reader.index_plan.as_deref() {
                collect_stats_info(inner, ctx, catalog, stats_info);
            }
        }
        PhysicalPlan::IndexScan(scan) => {
            let name = table_name(catalog, scan.table_id, None);
            stats_info.insert(name, stats_version(scan.table_id));
        }
        PhysicalPlan::TableScan(scan) => {
            let name = table_name(catalog, scan.table_id, None);
            stats_info.insert(name, stats_version(scan.table_id));
        }
        PhysicalPlan::Dml(root) => {
            if let Some(select) = root.select_plan.as_deref() {
                collect_stats_info(select, ctx, catalog, stats_info);
            }
        }
        _ => {}
    }
}

// Go EncodeFlatPlan's build-side-first tree, using the existing codec rather
// than serializing display rows. Access objects belong to ExplainInfo in this
// format; the binary format keeps them in its typed field.
fn encode_observed_operator(operator: &ExplainOperator, depth: isize, output: &mut String) {
    let task = match &operator.task {
        ExplainTask::Root => tidb_plancodec::encode_task_type(true, 0),
        ExplainTask::Cop { store, .. } => tidb_plancodec::encode_task_type(
            false,
            match store.as_str() {
                "tiflash" => 1,
                "tidb" => 2,
                _ => 0,
            },
        ),
    };
    let mut info = operator
        .access_object
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    if !operator.operator_info.is_empty() {
        if !info.is_empty() {
            info.push_str(", ");
        }
        info.push_str(&operator.operator_info);
    }
    tidb_plancodec::encode_plan_node(
        depth,
        &format!("{}{}", operator.id, operator.label),
        &operator.operator,
        operator.estimated_rows.unwrap_or_default(),
        &task,
        &info,
        "",
        "",
        "",
        "",
        output,
    );
    for child in &operator.children {
        encode_observed_operator(child, depth + 1, output);
    }
}

/// Go `normalizeStore`: the store a pushed-down plan runs on, as
/// `kv.StoreType` numbers it (TiKV 0, TiFlash 1, TiDB 2, UnSpecified 255).
fn reader_store_code(store: tidb_kv::store_type::StoreType) -> u8 {
    match store {
        tidb_kv::store_type::StoreType::TiFlash => 1,
        tidb_kv::store_type::StoreType::TiDb => 2,
        tidb_kv::store_type::StoreType::TiKv => 0,
        tidb_kv::store_type::StoreType::Unspecified => 255,
    }
}

/// The context Go's index join hands the probe side of its inner child (see
/// `physical_explain_operator`'s `_` arm): the join's own keys and ranges.
fn index_join_child_context(
    join: &tidb_planner_core_operator_physicalop::PhysicalIndexJoin,
) -> Option<IndexJoinExplainContext<'_>> {
    join.inner_access_table_id
        .map(|table_id| IndexJoinExplainContext {
            table_id,
            index_id: join.inner_access_index_id,
            inner_keys: &join.inner_join_keys,
            outer_keys: &join.outer_join_keys,
            key_offsets: &join.key_off2_idx_off,
            access_conditions: &join.inner_access_conditions,
            range_bare: join.inner_range_bare,
        })
}

/// Go `Expression.ExplainNormalizedInfo` over a column.
fn normalized_column_text(column: &tidb_expr::column::Column) -> String {
    tidb_expr::expr_util::explain_normalized_info(&tidb_expr::expression::Expression::Column(
        column.clone(),
    ))
}

/// Go `PhysicalSelection`/`PhysicalProjection.ExplainNormalizedInfo`: the
/// expressions sorted by their normalized text, with IN lists collapsed while
/// `tidb_ignore_inlist_plan_digest` is on.
fn normalized_expression_list(
    statement_context: &crate::StmtContext,
    expressions: &[tidb_expr::expression::Expression],
) -> String {
    if statement_context.ignore_inlist_plan_digest() {
        tidb_expr::expr_util::sorted_explain_expression_list_ignore_inlist(expressions)
    } else {
        tidb_expr::expr_util::sorted_explain_normalized_expression_list(expressions)
    }
}

/// Go `explainJoinLeftSide` with `normalized`: the left child by plan TYPE.
fn normalized_left_side(
    join_type: tidb_planner_core_base::LogicalJoinType,
    left_child: Option<&PhysicalPlan>,
) -> Option<String> {
    if join_type == tidb_planner_core_base::LogicalJoinType::Inner {
        return None;
    }
    left_child.map(|child| format!("left side:{}", plan_explain_id(child, true)))
}

/// Go `PhysicalHashJoin.explainInfo(true)` (`physical_hash_join.go:221`).
fn hash_join_normalized_info(
    statement_context: &crate::StmtContext,
    join: &tidb_planner_core_operator_physicalop::PhysicalHashJoin,
) -> String {
    let _ = statement_context;
    let mut text = String::new();
    if join.equal_conditions.is_empty() {
        text.push_str(if join.na_equal_conditions.is_empty() {
            "CARTESIAN "
        } else {
            "Null-aware "
        });
    }
    text.push_str(join_type_text(join.join_type));
    if let Some(side) = normalized_left_side(join.join_type, join.base.children().first()) {
        text.push_str(", ");
        text.push_str(&side);
    }
    if !join.equal_conditions.is_empty() {
        text.push_str(", equal:");
        text.push_str(
            &tidb_expr::expr_util::sorted_explain_normalized_scalar_func_list(
                &join.equal_conditions,
            ),
        );
    }
    if !join.na_equal_conditions.is_empty() {
        text.push_str(", equal:");
        text.push_str(
            &tidb_expr::expr_util::sorted_explain_normalized_scalar_func_list(
                &join.na_equal_conditions,
            ),
        );
    }
    for (label, conditions) in [
        ("left cond", &join.left_conditions),
        ("right cond", &join.right_conditions),
        ("other cond", &join.other_conditions),
    ] {
        if !conditions.is_empty() {
            text.push_str(&format!(
                ", {label}:{}",
                tidb_expr::expr_util::sorted_explain_normalized_expression_list(conditions)
            ));
        }
    }
    text
}

/// Go `PhysicalMergeJoin.explainInfo(true)` (`physical_merge_join.go:292`).
fn merge_join_normalized_info(
    eval_ctx: &dyn tidb_exprctx::Columns,
    join: &tidb_planner_core_operator_physicalop::PhysicalMergeJoin,
) -> String {
    let mut text = join_type_text(join.join_type).to_owned();
    if let Some(side) = normalized_left_side(join.join_type, join.base.children().first()) {
        text.push_str(", ");
        text.push_str(&side);
    }
    if !join.left_join_keys.is_empty() {
        text.push_str(&format!(
            ", left key:{}",
            columns_text(eval_ctx, &join.left_join_keys)
        ));
    }
    if !join.right_join_keys.is_empty() {
        text.push_str(&format!(
            ", right key:{}",
            columns_text(eval_ctx, &join.right_join_keys)
        ));
    }
    for (label, conditions) in [
        ("left cond", &join.left_conditions),
        ("right cond", &join.right_conditions),
        ("other cond", &join.other_conditions),
    ] {
        if !conditions.is_empty() {
            text.push_str(&format!(
                ", {label}:{}",
                tidb_expr::expr_util::sorted_explain_normalized_expression_list(conditions)
            ));
        }
    }
    text
}

/// Go `PhysicalIndexJoin.ExplainInfoInternal(true, isIndexMergeJoin)`
/// (`physical_index_join.go:147`).
fn index_join_normalized_info(
    eval_ctx: &dyn tidb_exprctx::Columns,
    join: &tidb_planner_core_operator_physicalop::PhysicalIndexJoin,
) -> String {
    let mut parts = vec![join_type_text(join.join_type).to_owned()];
    if let Some(inner) = join.base.children().get(join.inner_child_idx) {
        parts.push(format!("inner:{}", plan_explain_id(inner, true)));
    }
    if let Some(side) = normalized_left_side(join.join_type, join.base.children().first()) {
        parts.push(side);
    }
    if !join.outer_join_keys.is_empty() {
        parts.push(format!(
            "outer key:{}",
            columns_text(eval_ctx, &join.outer_join_keys)
        ));
    }
    if !join.inner_join_keys.is_empty() {
        parts.push(format!(
            "inner key:{}",
            columns_text(eval_ctx, &join.inner_join_keys)
        ));
    }
    if !join.outer_hash_keys.is_empty()
        && join.kind != tidb_planner_core_operator_physicalop::IndexJoinKind::IndexMergeJoin
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
                    normalized_column_text(outer),
                    normalized_column_text(inner)
                )
            })
            .collect::<Vec<_>>();
        equal.sort();
        parts.push(format!("equal cond:{}", equal.join(", ")));
    }
    for (label, conditions) in [
        ("left cond", &join.left_conditions),
        ("right cond", &join.right_conditions),
        ("other cond", &join.other_conditions),
    ] {
        if !conditions.is_empty() {
            parts.push(format!(
                "{label}:{}",
                tidb_expr::expr_util::sorted_explain_normalized_expression_list(conditions)
            ));
        }
    }
    parts.join(", ")
}

/// Go `BasePhysicalAgg.explainInfo(true)` (`base_physical_agg.go:869`).
fn aggregate_normalized_info(
    functions: &[tidb_expression_aggregation::AggFuncDesc],
    group_by: &[tidb_expr::expression::Expression],
    schema: Option<&tidb_expr::schema::Schema>,
) -> String {
    let mut text = String::new();
    if !group_by.is_empty() {
        text.push_str("group by:");
        text.push_str(&tidb_expr::expr_util::sorted_explain_normalized_expression_list(group_by));
        text.push_str(", ");
    }
    for (index, function) in functions.iter().enumerate() {
        let output = schema
            .and_then(|schema| schema.columns.get(index))
            .map_or_else(String::new, normalized_column_text);
        text.push_str("funcs:");
        text.push_str(&tidb_expression_aggregation::explain_agg_func_normalized(
            function, false,
        ));
        text.push_str("->");
        text.push_str(&output);
        if index + 1 < functions.len() {
            text.push_str(", ");
        }
    }
    text
}

/// Go `PhysicalTopN.ExplainNormalizedInfo` (`physical_topn.go:173`).
fn topn_normalized_info(topn: &tidb_planner_core_operator_physicalop::PhysicalTopN) -> String {
    let mut text = String::new();
    if !topn.partition_by.is_empty() {
        text.push_str("partition by ");
        text.push_str(
            &topn
                .partition_by
                .iter()
                .map(|item| tidb_expr::expr_util::column_explain_info_normalized(&item.col))
                .collect::<Vec<_>>()
                .join(", "),
        );
        text.push(' ');
    }
    if !topn.by_items.is_empty() {
        if !topn.partition_by.is_empty() {
            text.push_str("order by ");
        }
        text.push_str(
            &topn
                .by_items
                .iter()
                .map(|item| {
                    let expression = tidb_expr::expr_util::explain_normalized_info(&item.expr);
                    if item.desc {
                        format!("{expression}:desc")
                    } else {
                        expression
                    }
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    text
}

/// Go `PhysicalPlan.ExplainNormalizedInfo()`: fifteen operators render a
/// normalized body (constants as `?`, no ranges or statistics, children by
/// plan type); every other operator inherits the empty base implementation.
fn physical_normalized_info(
    eval_ctx: &dyn tidb_exprctx::Columns,
    statement_context: &crate::StmtContext,
    plan: &PhysicalPlan,
    catalog: &Catalog,
    index_join_context: Option<IndexJoinExplainContext<'_>>,
) -> String {
    let access = |plan: &PhysicalPlan| {
        physical_access(plan, catalog, statement_context.hypo_indexes())
            .map(|object| object.normalized_string())
            .unwrap_or_default()
    };
    // Go `Table/IndexScan.OperatorInfo(true)`'s range section: nothing for a
    // runtime-decided range, the normalized access conditions for a
    // correlated one, and `range:[?,?]` for any static range list.
    let scan_ranges = |decided_by_join: bool, has_ranges: bool| {
        if decided_by_join {
            String::new()
        } else if let Some(conditions) = correlated_access_conditions(plan) {
            format!(
                "range: decided by {}, ",
                tidb_expr::expr_util::sorted_explain_normalized_expression_list(conditions)
            )
        } else if has_ranges {
            "range:[?,?], ".to_owned()
        } else {
            String::new()
        }
    };
    match plan {
        PhysicalPlan::Selection(selection) => {
            normalized_expression_list(statement_context, &selection.conditions)
        }
        PhysicalPlan::Projection(projection) => {
            normalized_expression_list(statement_context, &projection.exprs)
        }
        PhysicalPlan::TopN(topn) => topn_normalized_info(topn),
        PhysicalPlan::HashAgg(aggregation) => aggregate_normalized_info(
            &aggregation.agg_funcs,
            &aggregation.group_by_items,
            aggregation.base.base.schema(),
        ),
        PhysicalPlan::StreamAgg(aggregation) => aggregate_normalized_info(
            &aggregation.agg_funcs,
            &aggregation.group_by_items,
            aggregation.base.base.schema(),
        ),
        PhysicalPlan::HashJoin(join) => hash_join_normalized_info(statement_context, join),
        // `PhysicalApply` embeds `PhysicalHashJoin`, so it inherits the method.
        PhysicalPlan::Apply(apply) => {
            hash_join_normalized_info(statement_context, &apply.hash_join)
        }
        PhysicalPlan::MergeJoin(join) => merge_join_normalized_info(eval_ctx, join),
        PhysicalPlan::IndexJoin(join) => index_join_normalized_info(eval_ctx, join),
        PhysicalPlan::IndexReader(reader) => format!(
            "index:{}",
            reader
                .index_plan
                .as_deref()
                .map_or_else(String::new, |plan| physical_operator_name(
                    plan,
                    index_join_context
                ))
        ),
        PhysicalPlan::TableScan(scan) => {
            let ranges = scan_ranges(
                is_index_join_table_range(plan, index_join_context),
                !scan.ranges.is_empty(),
            );
            format!(
                "{}, {ranges}{}",
                access(plan),
                scan_keep_order_text(scan.keep_order, scan.desc)
            )
        }
        PhysicalPlan::IndexScan(scan) => {
            let ranges = scan_ranges(
                is_index_join_index_range(plan, index_join_context),
                !scan.ranges.is_empty(),
            );
            format!(
                "{}, {ranges}{}",
                access(plan),
                scan_keep_order_text(scan.keep_order, scan.desc)
            )
        }
        PhysicalPlan::PointGet(point) => {
            let common_handle = catalog
                .kv_table_by_id(point.table_id)
                .is_some_and(|table| !table.common_handle_offsets().is_empty());
            let handle = point.index_id.is_none() && !common_handle && !point.ranges.is_empty();
            let mut info = if handle {
                "handle:?".to_owned()
            } else {
                String::new()
            };
            if point.lock {
                info.push_str(if handle { ", lock" } else { "lock" });
            }
            if info.is_empty() {
                access(plan)
            } else {
                format!("{}, {info}", access(plan))
            }
        }
        PhysicalPlan::BatchPointGet(batch) => {
            let common_handle = catalog
                .kv_table_by_id(batch.table_id)
                .is_some_and(|table| !table.common_handle_offsets().is_empty());
            let mut info = String::new();
            if batch.index_id.is_none() && !common_handle {
                info.push_str("handle:?, ");
            }
            info.push_str(&format!(
                "keep order:{}, desc:{}",
                batch.keep_order, batch.desc
            ));
            if batch.lock {
                info.push_str(", lock");
            }
            format!("{}, {info}", access(plan))
        }
        _ => String::new(),
    }
}

/// Go `NormalizeFlatPlan` over `FlattenPhysicalPlan(plan, false)`: one line per
/// operator of the SELECT part of the plan, in natural child order, then the
/// sha256 of the text. A DML contributes its select plan only; a plan without
/// one, or an empty rendering, has no digest.
#[must_use]
pub fn normalize_physical_plan(
    statement_context: &crate::StmtContext,
    physical: &PhysicalPlan,
    catalog: &Catalog,
) -> (String, String) {
    struct Walk<'a> {
        statement_context: &'a crate::StmtContext,
        catalog: &'a Catalog,
        output: String,
    }
    impl Walk<'_> {
        fn node(
            &mut self,
            plan: &PhysicalPlan,
            is_root: bool,
            store: u8,
            depth: isize,
            in_probe: bool,
            index_join_context: Option<IndexJoinExplainContext<'_>>,
        ) {
            // The recorded plan's `PhysicalShuffleReceiverStub` carries neither
            // a type nor an id (`flattenSingle` drops such operators), so its
            // data source takes the stub's place in the digest text.
            if let PhysicalPlan::ShuffleReceiver(receiver) = plan {
                self.node(
                    &receiver.data_source,
                    true,
                    2,
                    depth,
                    in_probe,
                    index_join_context,
                );
                return;
            }
            let mut name = physical_operator_name(plan, index_join_context);
            // Go `PhysicalTableScan.TP(isChildOfIndexLookUp)`.
            if in_probe && matches!(plan, PhysicalPlan::TableScan(_)) && name.starts_with("Table") {
                name = "TableRowIDScan".to_owned();
            }
            let info = physical_normalized_info(
                self.statement_context,
                self.statement_context,
                plan,
                self.catalog,
                index_join_context,
            );
            tidb_plancodec::normalize_plan_node(
                depth,
                &name,
                &tidb_plancodec::encode_task_type_for_normalize(is_root, store),
                &info,
                &mut self.output,
            );
            let next = depth + 1;
            match plan {
                PhysicalPlan::TableReader(reader) => {
                    if let Some(child) = reader.table_plan.as_deref() {
                        self.node(
                            child,
                            false,
                            reader_store_code(reader.store_type),
                            next,
                            false,
                            index_join_context,
                        );
                    }
                }
                PhysicalPlan::IndexReader(reader) => {
                    if let Some(child) = reader.index_plan.as_deref() {
                        self.node(child, false, 0, next, false, index_join_context);
                    }
                }
                PhysicalPlan::IndexLookUpReader(reader) => {
                    if let Some(child) = reader.index_plan.as_deref() {
                        self.node(child, false, 0, next, in_probe, index_join_context);
                    }
                    if let Some(child) = reader.table_plan.as_deref() {
                        self.node(child, false, 0, next, true, index_join_context);
                    }
                }
                PhysicalPlan::IndexMergeReader(reader) => {
                    for child in &reader.partial_plans_raw {
                        self.node(child, false, 0, next, in_probe, index_join_context);
                    }
                    if let Some(child) = reader.table_plan.as_deref() {
                        self.node(child, false, 0, next, true, index_join_context);
                    }
                }
                PhysicalPlan::CTE(_) => {}
                _ => {
                    for (index, child) in plan.children().iter().enumerate() {
                        let child_context = match plan {
                            PhysicalPlan::IndexJoin(join) if index == join.inner_child_idx => {
                                index_join_child_context(join)
                            }
                            PhysicalPlan::IndexJoin(_) => None,
                            _ => index_join_context,
                        };
                        // Go `PhysicalLocalIndexLookUp`: the cloned table scan
                        // is the index-lookup probe.
                        let probe = in_probe
                            || (matches!(plan, PhysicalPlan::LocalIndexLookUp(_)) && index == 1);
                        self.node(child, is_root, store, next, probe, child_context);
                    }
                }
            }
        }
    }
    let select_plan = match physical {
        PhysicalPlan::Dml(root) => root.select_plan.as_deref(),
        other => Some(other),
    };
    let Some(select_plan) = select_plan else {
        return (String::new(), String::new());
    };
    let mut walk = Walk {
        statement_context,
        catalog,
        output: String::new(),
    };
    walk.node(select_plan, true, 2, 0, false, None);
    if walk.output.is_empty() {
        return (String::new(), String::new());
    }
    let digest = tidb_parser::digest_normalized(&walk.output)
        .as_str()
        .to_owned();
    (walk.output, digest)
}

/// The plan-derived fields Go publishes while the ordinary executor is
/// constructed.
#[must_use]
pub fn process_plan_info(
    statement_context: &crate::StmtContext,
    physical: &PhysicalPlan,
    catalog: &Catalog,
) -> crate::ProcessPlanInfo {
    process_plan_info_with_brief(statement_context, physical, catalog, true)
}

/// The encoded and binary plans of `physical`: the ordinary tree rendered once
/// for both codecs.
fn plan_encodings(
    statement_context: &crate::StmtContext,
    physical: &PhysicalPlan,
    catalog: &Catalog,
) -> (String, String) {
    let full = physical_explain_roots(
        None,
        statement_context,
        statement_context,
        false,
        physical,
        catalog,
        None,
        false,
        &[],
    );
    let mut encoded = String::new();
    for root in &full {
        encode_observed_operator(root, 0, &mut encoded);
    }
    let encoded_plan = if encoded.is_empty() {
        String::new()
    } else {
        tidb_plancodec::compress(encoded.as_bytes())
    };
    (
        encoded_plan,
        binary_plan_from_roots(statement_context, physical, catalog, full),
    )
}

/// Derives and publishes the process-info fields for `physical`, rendering
/// the brief binary plan only when this statement publishes one.
pub fn publish_physical_process_info(
    ctx: &crate::StmtContext,
    physical: &tidb_planner_core_operator_physicalop::PhysicalPlan,
    catalog: &tidb_infoschema::Catalog,
) {
    let Some(execution_started) = ctx.begin_physical_process_info() else {
        return;
    };
    let plan = process_plan_info_with_brief(
        ctx,
        physical,
        catalog,
        ctx.publishes_brief_binary_plan(),
    );
    ctx.finish_physical_process_info(plan, Some(execution_started));
}

/// [`process_plan_info`] with the brief binary plan published only when
/// `with_brief` is set. Summary plans and table/index/statistics metadata remain
/// independent of binary prepared process-list visibility.
#[must_use]
pub fn process_plan_info_with_brief(
    statement_context: &crate::StmtContext,
    physical: &PhysicalPlan,
    catalog: &Catalog,
    with_brief: bool,
) -> crate::ProcessPlanInfo {
    let stored = std::sync::Arc::new(physical.deep_clone());
    // The process list reads the brief binary plan while the statement runs,
    // so a statement that publishes one renders its encodings now. Every
    // other reader is the statement's summary and slow log, which render them
    // only when they sample or write the plan (Go `LazyInfo`).
    let (encoded_plan, binary, plan_source) = if with_brief {
        let (encoded, binary) = plan_encodings(statement_context, physical, catalog);
        (encoded, binary, None)
    } else {
        let ctx = statement_context.clone().without_process_plan_info();
        let catalog = catalog.clone();
        let plan = std::sync::Arc::clone(&stored);
        let source = crate::PlanSource(std::sync::Arc::new(move || {
            Some(plan_encodings(&ctx, &plan, &catalog))
        }));
        (String::new(), String::new(), Some(source))
    };
    let (normalized_plan, plan_digest) = match statement_context.cached_plan_digest() {
        Some(cached) => cached
            .get_or_init(|| normalize_physical_plan(statement_context, physical, catalog))
            .clone(),
        None => normalize_physical_plan(statement_context, physical, catalog),
    };
    let fast_candidate = match physical {
        PhysicalPlan::Projection(projection) => {
            projection.base.children().first().unwrap_or(physical)
        }
        _ => physical,
    };
    let mut info = crate::ProcessPlanInfo {
        brief_binary_plan: if with_brief {
            binary.clone()
        } else {
            String::new()
        },
        encoded_plan,
        summary_binary_plan: binary,
        normalized_plan,
        plan_digest,
        plan_hints: tidb_planner::plan_hint::gen_hints_from_physical_plan(
            physical,
            &CatalogHints(catalog),
            &statement_context.block_as_names(),
        ),
        is_fast_plan: matches!(
            fast_candidate,
            PhysicalPlan::PointGet(_) | PhysicalPlan::TableDual(_)
        ),
        plan: Some(crate::StoredPlan::new(stored)),
        plan_source,
        ..crate::ProcessPlanInfo::default()
    };
    collect_executor_process_fields(
        physical,
        catalog,
        &mut info.table_ids,
        &mut info.index_names,
    );
    collect_stats_info(physical, statement_context, catalog, &mut info.stats_info);
    info
}

/// The table facts Go's hint generation reads off a plan's `TableInfo`.
struct CatalogHints<'a>(&'a Catalog);

impl tidb_planner::plan_hint::HintCatalog for CatalogHints<'_> {
    fn table_name(&self, table_id: i64) -> Option<String> {
        self.0
            .physical_kv_table_by_id(table_id)
            .map(|table| table.name())
    }

    fn has_clustered_primary_key(&self, table_id: i64) -> bool {
        self.0.physical_kv_table_by_id(table_id).is_some_and(|table| {
            table.pk_handle_offset().is_some() || !table.common_handle_offsets().is_empty()
        })
    }
}

/// Go `ExplainExec.executeAnalyzeExec`'s last step: once the analyzed
/// statement has run, the statement's `RUDetails` (cloned) are registered on
/// the target plan as `RURuntimeStats`, under the RU version of Go's
/// `domain.GetRUVersion` -- `rmclient.DefaultRUVersion` here, Go's answer
/// when no domain resource-group controller is reachable from the session.
/// This node keeps no statement-level RU v2 metrics (Go
/// `SyncRUV2MetricsFromContext`), so `Metrics` is nil.
fn register_ru_runtime_stats(
    statement_context: &crate::StmtContext,
    physical: &PhysicalPlan,
    runtime: &crate::driver::physical_builder::PhysicalRuntimeStats,
) -> crate::driver::physical_builder::PhysicalRuntimeStats {
    let mut runtime = runtime.clone();
    if let Some(ru_details) = statement_context.ru_details() {
        runtime
            .entry(crate::driver::physical_builder::runtime_plan_key(physical))
            .or_insert_with(|| tidb_executor_internal_exec::RowCount::default().into())
            .register_ru_runtime_stats(tidb_execdetails::ruv2_metrics::RuRuntimeStats {
                ru_details: Some(ru_details.cloned()),
                metrics: None,
                weights: tidb_execdetails::ruv2_metrics::RuV2Weights::default(),
                // Go `do.GetRUVersion()`.
                ru_version: tidb_topsql_stmtstats::bound_ru_version(),
            });
    }
    runtime
}

fn render_physical_plan(
    eval_ctx: &dyn tidb_exprctx::Columns,
    statement_context: &crate::StmtContext,
    physical: &PhysicalPlan,
    catalog: &Catalog,
    format: ExplainFormat,
    analyze: bool,
    runtime: Option<&crate::driver::physical_builder::PhysicalRuntimeStats>,
    scalar_subqueries: &[crate::driver::planner_bridge::RegisteredScalarSubquery],
) -> Result<SelectMeta, DriverError> {
    if matches!(format, ExplainFormat::Dot) {
        return Ok(explain_dot_rows(physical));
    }
    let runtime_with_ru;
    let runtime = match runtime {
        Some(runtime) if analyze => {
            runtime_with_ru = register_ru_runtime_stats(statement_context, physical, runtime);
            Some(&runtime_with_ru)
        }
        runtime => runtime,
    };
    if matches!(format, ExplainFormat::TidbJson | ExplainFormat::Binary) {
        // Go's binary operator carries `getPlanCost` (cost model ver2).
        let coster = matches!(format, ExplainFormat::Binary).then(|| {
            Ver2Coster::from_env(statement_context.optimizer_cost_env()).with_explain_cache()
        });
        let roots = physical_explain_roots(
            coster.as_ref(),
            eval_ctx,
            statement_context,
            false,
            physical,
            catalog,
            runtime,
            false,
            scalar_subqueries,
        );
        let field_type = FieldType::new(FieldTypeCode::VarString);
        return Ok(if matches!(format, ExplainFormat::TidbJson) {
            (
                vec![("TiDB_JSON".to_owned(), field_type)],
                vec![vec![text(&explain_tidb_json(&roots, analyze))]],
            )
        } else {
            (
                vec![("binary plan".to_owned(), field_type)],
                vec![vec![text(&explain_binary_plan(roots, analyze))]],
            )
        });
    }
    if matches!(format, ExplainFormat::Hint) {
        // Go `ExplainFormatHint`: the hints that reproduce the plan, then the
        // hints the statement itself wrote.
        let mut hints = tidb_planner::plan_hint::gen_hints_from_physical_plan(
            physical,
            &CatalogHints(catalog),
            &statement_context.block_as_names(),
        );
        hints.extend(statement_context.explain_table_hints().iter().cloned());
        let varchar = FieldType::new(FieldTypeCode::VarString);
        return Ok((
            vec![("hint".to_owned(), varchar)],
            vec![vec![text(&tidb_hint::restore_optimizer_hints(&hints))]],
        ));
    }
    let ignore_explain_id_suffix = matches!(format, ExplainFormat::Brief | ExplainFormat::PlanTree);
    let coster = (matches!(
        format,
        ExplainFormat::Verbose | ExplainFormat::CostTrace
    ))
    .then(|| {
        let coster = Ver2Coster::from_env(statement_context.optimizer_cost_env())
            .with_explain_cache();
        if matches!(format, ExplainFormat::CostTrace) {
            // go prices `ExplainFormatCostTrace` with
            // `CostFlagTrace` so every operator's `CostVer2` carries
            // its formula decomposition.
            let mut option = tidb_planner_costusage::PlanCostOption::new();
            option.with_cost_flag(tidb_planner_costusage::COST_FLAG_TRACE);
            coster.with_cost_option(option)
        } else {
            coster
        }
    });
    let roots = physical_explain_roots(
        coster.as_ref(),
        eval_ctx,
        statement_context,
        false,
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

/// Go `ExplainInfoForEncode`: the fields in Go's declaration order, with its
/// `omitempty` tags (`estRows` and `taskType` are always present).
#[derive(serde::Serialize)]
struct ExplainInfoForEncode {
    id: String,
    #[serde(rename = "estRows")]
    est_rows: String,
    #[serde(rename = "actRows", skip_serializing_if = "String::is_empty")]
    act_rows: String,
    #[serde(rename = "taskType")]
    task_type: String,
    #[serde(rename = "accessObject", skip_serializing_if = "String::is_empty")]
    access_object: String,
    #[serde(rename = "executeInfo", skip_serializing_if = "String::is_empty")]
    execute_info: String,
    #[serde(rename = "operatorInfo", skip_serializing_if = "String::is_empty")]
    operator_info: String,
    #[serde(rename = "memoryInfo", skip_serializing_if = "String::is_empty")]
    memory_info: String,
    #[serde(rename = "diskInfo", skip_serializing_if = "String::is_empty")]
    disk_info: String,
    #[serde(rename = "subOperators", skip_serializing_if = "Vec::is_empty")]
    sub_operators: Vec<ExplainInfoForEncode>,
}

/// Go `Explain.explainOpRecursivelyInJSONFormat` over one operator: the
/// full (suffixed) explain id plus its label, and the runtime columns only
/// under ANALYZE (`prepareOperatorInfoForJSONFormat`).
fn explain_info_for_encode(operator: &ExplainOperator, analyze: bool) -> ExplainInfoForEncode {
    let (act_rows, execute_info, memory_info, disk_info) = if analyze {
        (
            operator
                .actual_rows
                .map_or_else(|| "N/A".to_owned(), |rows| rows.to_string()),
            operator
                .execution_info
                .clone()
                .unwrap_or_else(|| "N/A".to_owned()),
            operator
                .memory_info
                .clone()
                .unwrap_or_else(|| "N/A".to_owned()),
            operator
                .disk_info
                .clone()
                .unwrap_or_else(|| "N/A".to_owned()),
        )
    } else {
        Default::default()
    };
    ExplainInfoForEncode {
        id: operator.explain_id(false),
        est_rows: operator
            .estimated_rows
            .map_or_else(|| "N/A".to_owned(), |rows| format!("{rows:.2}")),
        act_rows,
        task_type: operator.task.to_string(),
        access_object: operator
            .access_object
            .as_ref()
            .map_or_else(String::new, ToString::to_string),
        execute_info,
        operator_info: operator.operator_info.clone(),
        memory_info,
        disk_info,
        sub_operators: operator
            .children
            .iter()
            .map(|child| explain_info_for_encode(child, analyze))
            .collect(),
    }
}

/// Go `JSONToString`: `json.Encoder` with a four-space indent and HTML
/// escaping off, so the text ends in the encoder's newline. Go also escapes
/// U+2028 and U+2029, which `serde_json` leaves raw.
fn explain_tidb_json(roots: &[ExplainOperator], analyze: bool) -> String {
    use serde::Serialize;
    let encodes: Vec<ExplainInfoForEncode> = roots
        .iter()
        .map(|root| explain_info_for_encode(root, analyze))
        .collect();
    let mut buffer = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
    let mut serializer = serde_json::Serializer::with_formatter(&mut buffer, formatter);
    encodes
        .serialize(&mut serializer)
        .expect("explain operators serialize to JSON");
    let mut text = String::from_utf8(buffer).expect("serde_json writes UTF-8");
    text = text
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029");
    text.push('\n');
    text
}

/// Undoes the build-side-first swap, giving Go's `FlattenPhysicalPlan(plan,
/// false)` child order, which the binary format encodes.
fn into_plan_order(mut operator: ExplainOperator) -> ExplainOperator {
    if operator.build_side_swapped && operator.children.len() == 2 {
        operator.children.swap(0, 1);
    }
    operator.children = operator.children.into_iter().map(into_plan_order).collect();
    operator
}

/// Go `BinaryPlanStrFromFlatPlan(flat, false)`: the main tree and its CTEs
/// in plan order, each operator with its ver2 cost and no brief fields.
///
/// Go builds the target's executors even for a plain EXPLAIN ("to get
/// partition pruning", `buildExplain`), and each registers an empty basic
/// runtime record, so every root operator encodes `time:0s, open:0s,
/// close:0s, loops:0` with zero actual rows and the data says it carries
/// runtime stats. Under ANALYZE the operators carry what the run measured.
fn explain_binary_plan(roots: Vec<ExplainOperator>, analyze: bool) -> String {
    let mut roots = roots.into_iter().map(into_plan_order);
    let Some(main) = roots.next() else {
        return String::new();
    };
    let encode = |operator: ExplainOperator| {
        let mut encoded = binary_operator(operator.clone(), operator.clone());
        fill_binary_operator(&mut encoded, &operator, analyze);
        encoded
    };
    let data = ExplainData {
        main: Some(encode(main)),
        ctes: roots.map(encode).collect(),
        with_runtime_stats: true,
        ..Default::default()
    };
    tidb_plancodec::compress(&data.encode_to_vec())
}

const EMPTY_ROOT_BASIC_EXEC_INFO: &str = "time:0s, open:0s, close:0s, loops:0";

fn fill_binary_operator(
    encoded: &mut PbExplainOperator,
    operator: &ExplainOperator,
    analyze: bool,
) {
    encoded.brief_name = String::new();
    encoded.brief_operator_info = String::new();
    encoded.cost = operator.estimated_cost.unwrap_or_default();
    let root = matches!(operator.task, ExplainTask::Root);
    if analyze {
        encoded.act_rows = operator.actual_rows.unwrap_or_default();
        let info = operator.execution_info.clone().unwrap_or_default();
        if root {
            encoded.root_basic_exec_info = info;
        } else {
            encoded.cop_exec_info = info;
        }
    } else if root {
        encoded.root_basic_exec_info = EMPTY_ROOT_BASIC_EXEC_INFO.to_owned();
    }
    for (child, operator) in encoded.children.iter_mut().zip(&operator.children) {
        fill_binary_operator(child, operator, analyze);
    }
}

/// go `Explain.prepareDotInfo` (`pkg/planner/core/common_plans.go:1341`): the
/// plan rendered as one GraphViz `digraph` document in a single TEXT cell.
/// Each task opens a `cluster<N>` subgraph labelled with its task type, then
/// walks its operators breadth-first writing one `"parent" -> "child"` edge
/// per child; reader/cop splits (go's `copTasks`/`pipelines` queues) do not
/// arise at this tier, where a reader is an ordinary node of the same tree.
fn explain_dot_rows(physical: &PhysicalPlan) -> SelectMeta {
    let mut buffer = String::new();
    buffer.push_str(&format!("\ndigraph {} {{\n", physical.explain_id(false)));
    prepare_task_dot(physical, "root", &mut buffer);
    buffer.push_str("}\n");
    let field_type = FieldType::new(FieldTypeCode::VarString);
    (
        vec![("dot contents".to_owned(), field_type)],
        vec![vec![text(&buffer)]],
    )
}

/// go `Explain.prepareTaskDot`: one cluster per task, breadth-first edges
/// inside it.
fn prepare_task_dot(plan: &PhysicalPlan, task_tp: &str, buffer: &mut String) {
    buffer.push_str(&format!("subgraph cluster{}{{\n", plan.id()));
    buffer.push_str("node [style=filled, color=lightgrey]\n");
    buffer.push_str("color=black\n");
    buffer.push_str(&format!("label = \"{}\"\n", task_tp));
    if plan.children().is_empty() {
        buffer.push_str(&format!("\"{}\"\n", plan.explain_id(false)));
    }
    let mut queue = std::collections::VecDeque::new();
    queue.push_back(plan);
    while let Some(current) = queue.pop_front() {
        for child in current.children() {
            buffer.push_str(&format!(
                "\"{}\" -> \"{}\"\n",
                current.explain_id(false),
                child.explain_id(false)
            ));
            queue.push_back(child);
        }
    }
    buffer.push_str("}\n");
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
    if analyze {
        crate::driver::physical_builder::prepare_execution_plan(&mut physical, catalog, ctx)?;
    } else {
        crate::driver::physical_builder::prepare_plan_for_explain(&mut physical, catalog, ctx)?;
    }
    let runtime = analyze
        .then(|| crate::driver::physical_builder::execute_for_explain(&mut physical, catalog, ctx))
        .transpose()?;
    render_physical_plan(
        ctx,
        ctx,
        &physical,
        catalog,
        format,
        analyze,
        runtime.as_ref(),
        &scalar_subqueries,
    )
}

/// Reports a plan the statement already has -- EXPLAIN FORMAT =
/// 'plan_cache' over the plan the non-prepared cache gave it -- as EXPLAIN
/// rows, executing nothing.
pub fn explain_physical_plan(
    physical: &PhysicalPlan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let mut physical = physical.deep_clone();
    crate::driver::physical_builder::prepare_plan_for_explain(&mut physical, catalog, ctx)?;
    render_physical_plan(ctx, ctx, &physical, catalog, format, false, None, &[])
}

/// `EXPLAIN [FORMAT = f] FOR CONNECTION`: Go renders the target's retained
/// plan with the runtime statistics the target collected, so the result has
/// the analyze columns. This tier keeps no per-operator counters of an
/// ordinary statement, so those cells report what an unexecuted plan does.
pub fn explain_connection_plan(
    physical: &PhysicalPlan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let mut physical = physical.deep_clone();
    crate::driver::physical_builder::prepare_plan_for_explain(&mut physical, catalog, ctx)?;
    render_physical_plan(ctx, ctx, &physical, catalog, format, true, None, &[])
}

/// `EXPLAIN ANALYZE FORMAT = 'plan_cache'` over the plan the non-prepared
/// cache gave the statement: that plan RUNS, and each operator reports what
/// it produced, as [`explain_analyze_select_stmt`] does for a fresh plan.
pub fn explain_analyze_physical_plan(
    physical: &PhysicalPlan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let mut physical = physical.deep_clone();
    crate::driver::physical_builder::prepare_execution_plan(&mut physical, catalog, ctx)?;
    let runtime =
        crate::driver::physical_builder::execute_for_explain(&mut physical, catalog, ctx)?;
    render_physical_plan(
        ctx,
        ctx,
        &physical,
        catalog,
        format,
        true,
        Some(&runtime),
        &[],
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

/// The Index Advisor's `queryPlanCost` (`pkg/planner/optimize.go`): the
/// root cost of the plan the optimizer picks for `query`, with the
/// `tidb_opt_fix_control` 43817 rule the advisor turns on -- a plan that would
/// evaluate an uncorrelated scalar subquery while planning is refused.
pub fn query_plan_cost(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<f64, DriverError> {
    crate::driver::set_opr::validate_query_usage(query, ctx)?;
    let (mut physical, scalar_subqueries) =
        crate::driver::optimize_query_stmt_with_scalar_subqueries(query, catalog, current_db, ctx)?;
    if !scalar_subqueries.is_empty() {
        return Err(DriverError::Mysql(crate::driver::MysqlError::unknown("evaluate non-correlated sub-queries during optimization phase is not allowed by fix-control 43817"
                .to_owned(),
        )));
    }
    crate::driver::physical_builder::prepare_plan_for_explain(&mut physical, catalog, ctx)?;
    let coster = Ver2Coster::from_env(ctx.optimizer_cost_env()).with_explain_cache();
    Ok(coster
        .plan_cost(&physical, tidb_planner_property::task_type::TaskType::Root, false)
        .value())
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
    // Go `buildInsert` resolves the target columns and checks a VALUES list's
    // width before it builds an INSERT ... SELECT's source.
    crate::driver::validate_insert_for_explain(insert, catalog, current_db, ctx, None)?;
    let fk_spec = crate::driver::fk_spec_for_insert(insert, catalog, current_db, ctx)?;
    let _ = fk_spec;
    let physical = crate::driver::physical_insert_plan(insert, catalog, current_db, ctx)?;
    // The source's own width: Go appends ON DUPLICATE fields past it.
    let select_width = match &physical {
        PhysicalPlan::Dml(root) => root.select_plan.as_deref().and_then(|select| {
            root.insert_select
                .as_ref()
                .and_then(|output| output.actual_col_len)
                .or_else(|| select.schema().map(|schema| schema.columns.len()))
        }),
        _ => None,
    };
    crate::driver::validate_insert_for_explain(insert, catalog, current_db, ctx, select_width)?;
    render_physical_plan(ctx, ctx, &physical, catalog, format, false, None, &[])
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
    let mut physical = crate::driver::physical_insert_plan(insert, catalog, current_db, ctx)?;
    let root_key = crate::driver::physical_builder::runtime_plan_key(&physical);
    let mut runtime = crate::driver::physical_builder::PhysicalRuntimeStats::new();
    let (affected, _) = run_insert_stmt_with_physical_and_stats(
        insert,
        catalog,
        current_db,
        ctx,
        Some(&mut physical),
        Some(&mut runtime),
    )?;
    ctx.add_affected_rows(affected);
    runtime.insert(root_key, tidb_executor_internal_exec::RowCount::default().into());
    render_physical_plan(
        ctx,
        ctx,
        &physical,
        catalog,
        format,
        true,
        Some(&runtime),
        &[],
    )
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
    if let Some(physical) = crate::driver::multi_dml_explain_plan(
        crate::driver::MultiDmlRef::Update(update),
        catalog,
        current_db,
        ctx,
    )? {
        return render_physical_plan(ctx, ctx, &physical, catalog, format, false, None, &[]);
    }
    let source = crate::driver::update_source_query(update).ok_or_else(|| {
        DriverError::unsupported("UPDATE has no single-table source")
    })?;
    let fk_spec = crate::driver::fk_spec_for_update(update, current_db)?;
    let physical = crate::driver::physical_dml_plan(
        "Update",
        Some(&source),
        Some(update),
        catalog,
        current_db,
        ctx,
        &fk_spec,
    )?;
    render_physical_plan(ctx, ctx, &physical, catalog, format, false, None, &[])
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
    if let Some(physical) = crate::driver::multi_dml_explain_plan(
        crate::driver::MultiDmlRef::Delete(delete),
        catalog,
        current_db,
        ctx,
    )? {
        return render_physical_plan(ctx, ctx, &physical, catalog, format, false, None, &[]);
    }
    let source = crate::driver::delete_source_query(delete).ok_or_else(|| {
        DriverError::unsupported("DELETE has no single-table source")
    })?;
    let fk_spec = crate::driver::fk_spec_for_delete(delete, current_db)?;
    let physical = crate::driver::physical_dml_plan(
        "Delete",
        Some(&source),
        None,
        catalog,
        current_db,
        ctx,
        &fk_spec,
    )?;
    render_physical_plan(ctx, ctx, &physical, catalog, format, false, None, &[])
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
    let mut physical = match crate::driver::multi_dml_explain_plan(
        crate::driver::MultiDmlRef::Update(update),
        catalog,
        current_db,
        ctx,
    )? {
        Some(physical) => physical,
        None => {
            let source = crate::driver::update_source_query(update).ok_or_else(|| {
                DriverError::unsupported("UPDATE has no single-table source")
            })?;
            crate::driver::physical_dml_plan(
                "Update",
                Some(&source),
                Some(update),
                catalog,
                current_db,
                ctx,
                &crate::driver::fk_spec_for_update(update, current_db)?,
            )?
        }
    };
    let root_key = crate::driver::physical_builder::runtime_plan_key(&physical);
    let mut runtime = crate::driver::physical_builder::PhysicalRuntimeStats::new();
    let affected = run_update_stmt_with_physical_and_stats(
        update,
        catalog,
        current_db,
        ctx,
        Some(&mut physical),
        Some(&mut runtime),
    )?;
    ctx.add_affected_rows(affected);
    runtime.insert(root_key, tidb_executor_internal_exec::RowCount::default().into());
    render_physical_plan(
        ctx,
        ctx,
        &physical,
        catalog,
        format,
        true,
        Some(&runtime),
        &[],
    )
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
    let mut physical = match crate::driver::multi_dml_explain_plan(
        crate::driver::MultiDmlRef::Delete(delete),
        catalog,
        current_db,
        ctx,
    )? {
        Some(physical) => physical,
        None => {
            let source = crate::driver::delete_source_query(delete).ok_or_else(|| {
                DriverError::unsupported("DELETE has no single-table source")
            })?;
            crate::driver::physical_dml_plan(
                "Delete",
                Some(&source),
                None,
                catalog,
                current_db,
                ctx,
                &crate::driver::fk_spec_for_delete(delete, current_db)?,
            )?
        }
    };
    let root_key = crate::driver::physical_builder::runtime_plan_key(&physical);
    let mut runtime = crate::driver::physical_builder::PhysicalRuntimeStats::new();
    let affected = run_delete_stmt_with_physical_and_stats(
        delete,
        catalog,
        current_db,
        ctx,
        Some(&mut physical),
        Some(&mut runtime),
    )?;
    ctx.add_affected_rows(affected);
    runtime.insert(root_key, tidb_executor_internal_exec::RowCount::default().into());
    render_physical_plan(
        ctx,
        ctx,
        &physical,
        catalog,
        format,
        true,
        Some(&runtime),
        &[],
    )
}

fn text(value: &str) -> Datum {
    Datum::Bytes(value.as_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_partitions_belong_to_readers_not_scans() {
        use tidb_planner_core_operator_physicalop::*;
        let catalog = tidb_infoschema_perfschema::mock_catalog();
        for (all_partitions, names, expected) in [
            (true, Vec::new(), "partition:all"),
            (
                false,
                vec!["p0".to_owned(), "p2".to_owned()],
                "partition:p0,p2",
            ),
        ] {
            let access = tidb_planner_core_access::DynamicPartitionAccessObject {
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
                    physical_access(&reader, &catalog, None)
                        .unwrap()
                        .to_string(),
                    expected
                );
            }
            for scan in [table, index] {
                let Some(AccessObject::Scan(access)) = physical_access(&scan, &catalog, None)
                else {
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

        let mut index = tidb_planner_core_operator_physicalop::PhysicalIndexJoin::default();
        for (kind, expected) in [
            (
                tidb_planner_core_operator_physicalop::IndexJoinKind::IndexJoin,
                "IndexJoin",
            ),
            (
                tidb_planner_core_operator_physicalop::IndexJoinKind::IndexHashJoin,
                "IndexHashJoin",
            ),
            (
                tidb_planner_core_operator_physicalop::IndexJoinKind::IndexMergeJoin,
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

    /// Task 65's secondary note: an `IndexJoin`'s `inner:` field named its
    /// child `Join` where Go names it `HashJoin`. Root cause: every join
    /// strategy `exhaust_physical_plans` considers for one `LogicalJoin`
    /// (Hash/Merge/Index) is built from one shared `BasePhysicalPlan` whose
    /// stored type string comes from the LOGICAL join's own type ("Join") --
    /// exactly why `physical_operator_name` above exists, to re-derive each
    /// variant's real name instead of trusting that shared, generic string.
    /// `left side:` (`join_info`, above) already calls `plan_explain_id`,
    /// which does that re-derivation; `inner:` called `.explain_id()`
    /// directly instead, trusting the stored (generic) name.
    #[test]
    fn index_join_inner_field_names_its_child_like_go_regardless_of_the_childs_stored_base_type() {
        use tidb_planner_core_operator_physicalop::{BasePhysicalPlan, PhysicalIndexJoin};
        use tidb_planner_core_base::BasePlan;

        let mut inner_base = BasePhysicalPlan::default();
        inner_base.base = BasePlan::with_id(7, "Join", 0);
        let inner = PhysicalPlan::HashJoin(tidb_planner_core_operator_physicalop::PhysicalHashJoin {
            base: inner_base,
            ..Default::default()
        });

        let mut index_base = BasePhysicalPlan::default();
        index_base.base = BasePlan::with_id(9, "IndexJoin", 0);
        index_base.set_children(vec![PhysicalPlan::TableReader(Default::default()), inner]);
        let index_join = PhysicalPlan::IndexJoin(PhysicalIndexJoin {
            base: index_base,
            inner_child_idx: 1,
            join_type: tidb_planner_core_base::LogicalJoinType::Inner,
            ..Default::default()
        });

        let info = physical_operator_info(
            &crate::StmtContext::default(),
            &crate::StmtContext::default(),
            &index_join,
            &tidb_infoschema_perfschema::mock_catalog(),
            false,
            None,
        );
        assert!(
            info.contains("inner:HashJoin_7"),
            "the inner: field must name the child by its real physical type, \
             matching Go's ExplainID (always its own plancodec constant), not \
             the shared generic base string this tier's Hash/Merge/Index \
             candidates share:\n{info}"
        );
        assert!(
            !info.contains("inner:Join_7"),
            "must not regress to the mislabelled 'Join' name:\n{info}"
        );
    }

    #[test]
    fn physical_index_join_explain_uses_null_safe_equal_condition() {
        let column =
            |id| tidb_expr::column::Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        let mut index = tidb_planner_core_operator_physicalop::PhysicalIndexJoin::default();
        index.outer_hash_keys = vec![column(1), column(2)];
        index.inner_hash_keys = vec![column(3), column(4)];
        index.is_null_eq = vec![true, false];
        let info = physical_operator_info(
            &crate::StmtContext::default(),
            &crate::StmtContext::default(),
            &PhysicalPlan::IndexJoin(index),
            &tidb_infoschema_perfschema::mock_catalog(),
            false,
            None,
        );
        assert!(info.contains("equal cond:eq(Column#2, Column#4), nulleq(Column#1, Column#3)"));
    }

    #[test]
    fn index_join_range_info_uses_the_go_spelling_for_the_chosen_probe() {
        use tidb_planner_core_operator_physicalop::PhysicalTableScan;

        let column =
            |id| tidb_expr::column::Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        let inner_keys = [column(1)];
        let outer_keys = [column(2)];
        let key_offsets = [0];
        let access_conditions: [tidb_expr::expression::Expression; 0] = [];
        let eval_ctx = crate::StmtContext::default();
        let scan = PhysicalTableScan::default();
        let catalog = tidb_infoschema_perfschema::mock_catalog();

        for (range_bare, expected) in [
            (true, "range: decided by [Column#2]"),
            (false, "range: decided by [eq(Column#1, Column#2)]"),
        ] {
            let context = IndexJoinExplainContext {
                table_id: 1,
                index_id: None,
                inner_keys: &inner_keys,
                outer_keys: &outer_keys,
                key_offsets: &key_offsets,
                access_conditions: &access_conditions,
                range_bare,
            };
            assert_eq!(
                index_join_decided_by_text_for_scan(&eval_ctx, &context),
                expected
            );
        }
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
        let mut merge = tidb_planner_core_operator_physicalop::PhysicalMergeJoin::default();
        merge.join_type = tidb_planner_core_base::LogicalJoinType::Inner;
        merge.left_join_keys = vec![column(1), column(2)];
        merge.right_join_keys = vec![column(3), column(4)];
        // Go sorts the rendered conditions; `eq` precedes `gt` here.
        merge.other_conditions = vec![condition("gt", 1, 5), condition("eq", 2, 6)];
        let info = physical_operator_info(
            &crate::StmtContext::default(),
            &crate::StmtContext::default(),
            &PhysicalPlan::MergeJoin(merge),
            &tidb_infoschema_perfschema::mock_catalog(),
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
        let child = || PhysicalPlan::HashJoin(tidb_planner_core_operator_physicalop::PhysicalHashJoin::default());

        let mut outer = tidb_planner_core_operator_physicalop::PhysicalHashJoin::default();
        outer.join_type = tidb_planner_core_base::LogicalJoinType::LeftOuterSemi;
        outer.left_join_keys = vec![column(1)];
        outer.right_join_keys = vec![column(2)];
        outer.base.set_children(vec![child()]);
        let info = physical_operator_info(
            &crate::StmtContext::default(),
            &crate::StmtContext::default(),
            &PhysicalPlan::HashJoin(outer),
            &tidb_infoschema_perfschema::mock_catalog(),
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
        let mut inner = tidb_planner_core_operator_physicalop::PhysicalHashJoin::default();
        inner.join_type = tidb_planner_core_base::LogicalJoinType::Inner;
        inner.left_join_keys = vec![column(1)];
        inner.right_join_keys = vec![column(2)];
        inner.base.set_children(vec![child()]);
        let info = physical_operator_info(
            &crate::StmtContext::default(),
            &crate::StmtContext::default(),
            &PhysicalPlan::HashJoin(inner),
            &tidb_infoschema_perfschema::mock_catalog(),
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
        let mut join = tidb_planner_core_operator_physicalop::PhysicalHashJoin::default();
        join.join_type = tidb_planner_core_base::LogicalJoinType::Inner;
        assert_eq!(
            physical_operator_info(
                &crate::StmtContext::default(),
                &crate::StmtContext::default(),
                &PhysicalPlan::HashJoin(join),
                &tidb_infoschema_perfschema::mock_catalog(),
                true,
                None
            ),
            "CARTESIAN inner join"
        );
    }

    #[test]
    fn a_null_safe_join_key_explains_as_nulleq() {
        let column =
            |id| tidb_expr::column::Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        let mut join = tidb_planner_core_operator_physicalop::PhysicalHashJoin::default();
        join.join_type = tidb_planner_core_base::LogicalJoinType::Semi;
        join.left_join_keys = vec![column(1), column(2)];
        join.right_join_keys = vec![column(3), column(4)];
        join.is_null_eq = vec![true, false];
        join.base.set_children(vec![PhysicalPlan::HashJoin(
            tidb_planner_core_operator_physicalop::PhysicalHashJoin::default(),
        )]);
        let info = physical_operator_info(
            &crate::StmtContext::default(),
            &crate::StmtContext::default(),
            &PhysicalPlan::HashJoin(join),
            &tidb_infoschema_perfschema::mock_catalog(),
            true,
            None,
        );
        assert!(
            info.starts_with("semi join, left side:HashJoin, equal:[nulleq(Column#1, Column#3) eq(Column#2, Column#4)]"),
            "{info}"
        );
    }
    #[test]
    fn brief_binary_plan_renders_bound_selection_parameters() {
        let ctx = crate::StmtContext::for_query()
            .with_prepared_params(vec![tidb_datatype::Datum::Int(42)].into());
        let mut parameter = tidb_expr::constant::Constant::new(
            tidb_datatype::Datum::Int(0),
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        parameter.param_marker = Some(tidb_expr::constant::ParamMarker { order: 0 });
        let plan = PhysicalPlan::Selection(tidb_planner_core_operator_physicalop::PhysicalSelection {
            conditions: vec![tidb_expr::expression::Expression::Constant(parameter)],
            ..Default::default()
        });
        assert!(!super::brief_binary_plan(&ctx, &plan, &tidb_infoschema_perfschema::mock_catalog()).is_empty());
    }
}
