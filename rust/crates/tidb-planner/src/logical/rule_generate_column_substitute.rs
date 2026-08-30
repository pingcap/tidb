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

//! Go `pkg/planner/core/rule_generate_column_substitute.go`.

use tidb_datatype::EvalType;
use tidb_expr::column::Column;
use tidb_expr::expr_util::predicates::maybe_over_optimized_4_plan_cache;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::extract_columns;

use super::rule::{LogicalOptRule, RuleContext};
use super::{LogicalPlan, PlanError};

/// Go `GcSubstituter`.
pub struct GcSubstituter;

type Candidate = (Expression, Column);

fn collect_candidates(
    plan: &LogicalPlan,
    candidates: &mut Vec<Candidate>,
    enable_unsafe_substitute: bool,
) {
    if matches!(plan, LogicalPlan::CTE(_)) {
        return;
    }
    for child in plan.base().children() {
        collect_candidates(child, candidates, enable_unsafe_substitute);
    }
    let LogicalPlan::DataSource(source) = plan else {
        return;
    };
    if source.prefer_store_type & super::data_source::PREFER_TIFLASH != 0 {
        return;
    }
    let Some(schema) = source.base.base.schema() else {
        return;
    };
    for path in &source.enumerated_paths {
        let crate::access_path::PossiblePath::Index { index } = path else {
            continue;
        };
        let Some(index) = source.indexes.get(*index) else {
            continue;
        };
        for part in &index.columns {
            let Some(column) = schema.columns.get(part.offset) else {
                continue;
            };
            let Some(generated) = column.virtual_expr.as_deref() else {
                continue;
            };
            let (Some(column_type), Some(generated_type)) =
                (column.ret_type.as_ref(), generated.static_type())
            else {
                continue;
            };
            if !column_type.partial_equal(generated_type, enable_unsafe_substitute)
                || extract_columns(generated).is_empty()
            {
                continue;
            }
            candidates.push((generated.clone(), column.clone()));
        }
    }
}

fn try_substitute(
    expression: &mut Expression,
    desired_type: EvalType,
    schema: &Schema,
    candidates: &[Candidate],
    context: &RuleContext<'_>,
) -> bool {
    for (candidate, column) in candidates {
        if expression.equal(candidate)
            && candidate
                .static_type()
                .is_some_and(|field_type| field_type.eval_type() == desired_type)
            && schema.column_index(column) >= 0
        {
            if maybe_over_optimized_4_plan_cache(
                context.use_plan_cache,
                std::slice::from_ref(expression),
            ) {
                if let Some(marker) = context.plan_cache_marker {
                    marker.set_skip_plan_cache(
                        "generated column substitution with mutable constants can affect index selection",
                    );
                }
            }
            *expression = Expression::Column(column.clone());
            return true;
        }
    }
    false
}

fn substitute_condition(
    condition: &mut Expression,
    schema: &Schema,
    candidates: &[Candidate],
    context: &RuleContext<'_>,
) -> bool {
    let Expression::ScalarFunction(function) = condition else {
        return false;
    };
    let mut changed = false;
    match function.func_name.lowercase() {
        "eq" | "lt" | "le" | "gt" | "ge" if function.args.len() == 2 => {
            let left_type = function.args[0].static_type().map(|ty| ty.eval_type());
            let right_type = function.args[1].static_type().map(|ty| ty.eval_type());
            if let Some(left_type) = left_type {
                changed |= try_substitute(
                    &mut function.args[1],
                    left_type,
                    schema,
                    candidates,
                    context,
                );
            }
            if let Some(right_type) = right_type {
                changed |= try_substitute(
                    &mut function.args[0],
                    right_type,
                    schema,
                    candidates,
                    context,
                );
            }
        }
        "in" if function.args.len() >= 2 => {
            let right_type = function.args[1].static_type().map(|ty| ty.eval_type());
            if let Some(right_type) = right_type {
                let same_type = function.args[1..].iter().all(|argument| {
                    argument
                        .static_type()
                        .is_some_and(|field_type| field_type.eval_type() == right_type)
                });
                if same_type {
                    changed |= try_substitute(
                        &mut function.args[0],
                        right_type,
                        schema,
                        candidates,
                        context,
                    );
                }
            }
        }
        "like" if function.args.len() >= 2 => {
            if let Some(right_type) = function.args[1].static_type().map(|ty| ty.eval_type()) {
                changed |= try_substitute(
                    &mut function.args[0],
                    right_type,
                    schema,
                    candidates,
                    context,
                );
            }
        }
        "or" | "and" if function.args.len() == 2 => {
            changed |= substitute_condition(&mut function.args[0], schema, candidates, context);
            changed |= substitute_condition(&mut function.args[1], schema, candidates, context);
        }
        "not" if function.args.len() == 1 => {
            changed |= substitute_condition(&mut function.args[0], schema, candidates, context);
        }
        _ => {}
    }
    if changed {
        function.invalidate_cached_arguments();
    }
    changed
}

fn substitute_plan(plan: &mut LogicalPlan, candidates: &[Candidate], context: &RuleContext<'_>) {
    match plan {
        LogicalPlan::Selection(selection) => {
            let schema = selection
                .base
                .base
                .schema()
                .or_else(|| {
                    selection
                        .base
                        .children()
                        .first()
                        .and_then(LogicalPlan::schema)
                })
                .cloned()
                .unwrap_or_default();
            for condition in &mut selection.conditions {
                substitute_condition(condition, &schema, candidates, context);
            }
        }
        LogicalPlan::Projection(projection) => {
            let schema = projection
                .base
                .children()
                .first()
                .and_then(LogicalPlan::schema)
                .cloned()
                .unwrap_or_default();
            for expression in &mut projection.exprs {
                if let Some(eval_type) = expression.static_type().map(|ty| ty.eval_type()) {
                    try_substitute(expression, eval_type, &schema, candidates, context);
                }
            }
        }
        LogicalPlan::Sort(sort) => {
            let schema = sort
                .base
                .base
                .schema()
                .or_else(|| sort.base.children().first().and_then(LogicalPlan::schema))
                .cloned()
                .unwrap_or_default();
            for item in &mut sort.by_items {
                if let Some(eval_type) = item.expr.static_type().map(|ty| ty.eval_type()) {
                    try_substitute(&mut item.expr, eval_type, &schema, candidates, context);
                }
            }
        }
        LogicalPlan::Aggregation(aggregation) => {
            let schema = aggregation
                .base
                .base
                .schema()
                .or_else(|| {
                    aggregation
                        .base
                        .children()
                        .first()
                        .and_then(LogicalPlan::schema)
                })
                .cloned()
                .unwrap_or_default();
            for function in &mut aggregation.agg_funcs {
                for argument in &mut function.base.args {
                    if let Some(eval_type) = argument.static_type().map(|ty| ty.eval_type()) {
                        try_substitute(argument, eval_type, &schema, candidates, context);
                    }
                }
            }
            for item in &mut aggregation.group_by_items {
                if let Some(eval_type) = item.static_type().map(|ty| ty.eval_type()) {
                    try_substitute(item, eval_type, &schema, candidates, context);
                }
            }
        }
        _ => {}
    }
    for child in plan.base_mut().children_mut() {
        substitute_plan(child, candidates, context);
    }
}

impl LogicalOptRule for GcSubstituter {
    fn optimize(
        &self,
        context: &RuleContext<'_>,
        mut plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        let mut candidates = Vec::new();
        collect_candidates(&plan, &mut candidates, context.enable_unsafe_substitute);
        if !candidates.is_empty() {
            substitute_plan(&mut plan, &candidates, context);
        }
        // Pinned Go never changes `planChanged` from false.
        Ok((plan, false))
    }

    fn name(&self) -> &'static str {
        "generate_column_substitute"
    }
}
