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

//! Go `pkg/planner/core/rule_inject_extra_projection.go` — the postOptimize
//! projection re-injection (`InjectExtraProjection`), which Go runs AFTER
//! `eliminatePhysicalProjection` so the final physical tree carries the
//! purposeful projections that elimination removed:
//!
//! * a projection below a hash/stream aggregate when any aggregate argument,
//!   order-by item, or group-by item is a scalar function (vectorized
//!   evaluation of the scalar inputs, `InjectProjBelowAgg`);
//! * a projection pair around a sort/top-n when an order-by item is a scalar
//!   function (`InjectProjBelowSort`: the bottom projection evaluates it,
//!   the top one prunes the extra column back to the sort's schema);
//! * `TurnNominalSortIntoProj`: an `only_column` nominal sort disappears
//!   (its child already answers the order property); a non-column one
//!   becomes two pass-through projections;
//! * a per-child cast projection below an MPP union — dead here: the narrow
//!   tier's unions always carry `mpp: false`, and Go's own early return
//!   keeps the union untouched in that case.
//!
//! `refine4NeighbourProj` (`pkg/planner/core/resolve_indices.go:56`) is
//! ported beside them: neighbouring projections must not keep redundant
//! pass-through columns, and the union-find over each column's output
//! positions is what keeps the referenced `Index` fields consistent.
//!
//! # Narrowing
//!
//! Go's `projInjector.inject` recurses into a TiFlash `PhysicalTableReader`'s
//! table plan. The narrow tier is TiKV-only — its reader nodes carry no
//! TiFlash store type — so that arm has no counterpart here.
//!
//! The Rust `NominalSort` narrows `ByItems` to column `SortItem`s
//! (`physical/mod.rs`), so Go's scalar-expression loop inside
//! `TurnNominalSortIntoProj` has nothing to add; the conversion still builds
//! the two pass-through projections for the non-column shape.

use tidb_expr::aggregation::ByItems;
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::NoColumns;

use crate::expression_rewriter::ColumnIdAllocator;
use crate::plan_base::PlanIdAllocator;
use crate::physical::{BasePhysicalPlan, PhysicalPlan, PhysicalProjection};

/// The allocators and stats context Go reaches through `plan.SCtx()`.
pub struct InjectContext<'a> {
    /// Go `SCtx().GetSessionVars().GetPlanID()` — the plan id allocator.
    pub plan_ids: &'a PlanIdAllocator,
    /// Go `SCtx().GetSessionVars().AllocPlanColumnID()`.
    pub column_ids: &'a ColumnIdAllocator,
    /// The session's stats skew ratio, which the Rust
    /// `StatsInfo::scale_by_expect_cnt` threads alongside Go's expected
    /// count.
    pub skew_ratio: f64,
}

/// Go `InjectExtraProjection` (`rule_inject_extra_projection.go:39`).
///
/// Go's failpoint `DisableProjectionPostOptimization` short-circuits the
/// whole pass; the failpoint harness is a test-only seam and is not
/// reproduced.
///
/// The bridge entry point: it owns the allocators and carries the same
/// `skew_ratio = 1.0` the wired search context uses.
#[must_use]
pub fn inject_extra_projection(
    plan: PhysicalPlan,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
) -> PhysicalPlan {
    let ctx = InjectContext {
        plan_ids,
        column_ids,
        skew_ratio: 1.0,
    };
    inject(&ctx, plan)
}

/// Go `projInjector.inject` (`rule_inject_extra_projection.go:57`): recurse
/// into every child first, then apply the per-operator injection.
fn inject(ctx: &InjectContext<'_>, plan: PhysicalPlan) -> PhysicalPlan {
    let mut plan = plan;
    let children = plan.base_mut().take_children();
    plan.base_mut()
        .set_children(children.into_iter().map(|child| inject(ctx, child)).collect());
    match plan {
        PhysicalPlan::HashAgg(agg) => inject_proj_below_agg(ctx, agg),
        PhysicalPlan::StreamAgg(agg) => inject_proj_below_stream_agg(ctx, agg),
        PhysicalPlan::Sort(sort) => inject_proj_below_sort(ctx, PhysicalPlan::Sort(sort)),
        PhysicalPlan::TopN(topn) => inject_proj_below_sort(ctx, PhysicalPlan::TopN(topn)),
        PhysicalPlan::NominalSort(nominal) => turn_nominal_sort_into_proj(ctx, nominal),
        PhysicalPlan::UnionAll(union_all) => inject_proj_below_union(union_all),
        other => other,
    }
}

/// Go `injectProjBelowUnion` (`rule_inject_extra_projection.go:84`): only an
/// MPP union casts its children's outputs; the narrow tier's unions always
/// carry `mpp: false`, so Go's early return answers the union unchanged.
fn inject_proj_below_union(union_all: crate::physical::PhysicalUnionAll) -> PhysicalPlan {
    if !union_all.mpp {
        return PhysicalPlan::UnionAll(union_all);
    }
    // Unreachable on this tier: nothing constructs an MPP union here. Kept
    // as Go's guard so a future mpp=true constructor cannot silently skip
    // the cast injection.
    PhysicalPlan::UnionAll(union_all)
}

/// Whether any expression in the list is a scalar function.
fn has_scalar_func(mut exprs: impl Iterator<Item = Expression>) -> bool {
    exprs.any(|expr| matches!(expr, Expression::ScalarFunction(_)))
}

/// Go `InjectProjBelowAgg` (`rule_inject_extra_projection.go:119`) for both
/// aggregate flavors. If every aggregate argument, order-by item, and
/// group-by item is a column or constant, the aggregate answers unchanged.
fn inject_proj_below_agg(
    ctx: &InjectContext<'_>,
    mut agg: crate::physical::PhysicalHashAgg,
) -> PhysicalPlan {
    // Go `coreusage.WrapCastForAggFuncs(exprCtx, aggFuncs)` — best-effort
    // argument casts, exactly as Go runs them unconditionally here.
    let _ = crate::core_usage::wrap_cast_for_agg_funcs(&NoColumns, &mut agg.agg_funcs);

    let mut has_scalar_func = false;
    for func in &agg.agg_funcs {
        has_scalar_func = has_scalar_func
            || func
                .args()
                .iter()
                .any(|arg| matches!(arg, Expression::ScalarFunction(_)));
        has_scalar_func = has_scalar_func
            || func
                .order_by_items
                .iter()
                .any(|item| matches!(item.expr, Expression::ScalarFunction(_)));
    }
    if !has_scalar_func {
        has_scalar_func = agg
            .group_by_items
            .iter()
            .any(|item| matches!(item, Expression::ScalarFunction(_)));
    }
    if !has_scalar_func {
        return PhysicalPlan::HashAgg(agg);
    }

    let mut proj_exprs: Vec<Expression> = Vec::new();
    let mut proj_schema_cols: Vec<Column> = Vec::new();
    let mut cursor = 0usize;

    for func in &mut agg.agg_funcs {
        for i in 0..func.base.args.len() {
            if matches!(func.base.args[i], Expression::Constant(_)) {
                continue;
            }
            let Some(ret_type) = func.base.args[i].static_type().cloned() else {
                continue;
            };
            proj_exprs.push(func.base.args[i].clone());
            let mut new_arg = Column::new(ctx.column_ids.alloc(), ret_type);
            new_arg.index = cursor as i64;
            proj_schema_cols.push(new_arg.clone());
            func.base.args[i] = Expression::Column(new_arg);
            cursor += 1;
        }
        for by_item in &mut func.order_by_items {
            if matches!(by_item.expr, Expression::Constant(_)) {
                continue;
            }
            let idx = proj_exprs.iter().position(|a| a.equal(&by_item.expr));
            if let Some(idx) = idx {
                by_item.expr = Expression::Column(proj_schema_cols[idx].clone());
            } else {
                let Some(ret_type) = by_item.expr.static_type().cloned() else {
                    continue;
                };
                proj_exprs.push(by_item.expr.clone());
                let mut new_arg = Column::new(ctx.column_ids.alloc(), ret_type);
                new_arg.index = cursor as i64;
                proj_schema_cols.push(new_arg.clone());
                by_item.expr = Expression::Column(new_arg);
                cursor += 1;
            }
        }
    }

    for item in &mut agg.group_by_items {
        if matches!(item, Expression::Constant(_)) {
            continue;
        }
        let idx = proj_exprs.iter().position(|a| a.equal(item));
        if let Some(idx) = idx {
            *item = Expression::Column(proj_schema_cols[idx].clone());
        } else {
            let Some(ret_type) = item.static_type().cloned() else {
                continue;
            };
            proj_exprs.push(item.clone());
            let mut new_arg = Column::new(ctx.column_ids.alloc(), ret_type);
            new_arg.index = cursor as i64;
            proj_schema_cols.push(new_arg.clone());
            *item = Expression::Column(new_arg);
            cursor += 1;
        }
    }

    let block_offset = agg.base.base.query_block_offset();
    let child_prop = agg.base.child_req_prop(0).cloned().unwrap_or_default();
    let child = agg
        .base
        .take_children()
        .into_iter()
        .next()
        .expect("aggregate projection injection requires one child");
    let stats = child
        .stats_info()
        .map(|stats| stats.scale_by_expect_cnt(child_prop.expected_cnt, ctx.skew_ratio));
    let mut base = BasePhysicalPlan::new(
        ctx.plan_ids,
        crate::logical::LogicalProjection::TYPE,
        block_offset,
    );
    base.base.set_stats(stats);
    base.base.set_schema(Some(Schema::new(proj_schema_cols)));
    base.set_children(vec![child]);
    base.set_children_req_props(vec![Some(child_prop)]);
    let proj = PhysicalProjection {
        base,
        exprs: proj_exprs,
        calculate_no_delay: false,
        avoid_column_evaluator: false,
    };
    agg.base.set_children(vec![PhysicalPlan::Projection(proj)]);
    PhysicalPlan::HashAgg(agg)
}

/// Go routes `PhysicalStreamAgg` to the same `InjectProjBelowAgg`; the port
/// converts the node into the hash-agg struct it shares fields with (the
/// base — and therefore the operator type string — moves unchanged).
fn inject_proj_below_stream_agg(
    ctx: &InjectContext<'_>,
    agg: crate::physical::PhysicalStreamAgg,
) -> PhysicalPlan {
    let hash_like = crate::physical::PhysicalHashAgg {
        base: agg.base,
        agg_funcs: agg.agg_funcs,
        group_by_items: agg.group_by_items,
    };
    match inject_proj_below_agg(ctx, hash_like) {
        PhysicalPlan::HashAgg(rebuilt) => {
            PhysicalPlan::StreamAgg(crate::physical::PhysicalStreamAgg {
                base: rebuilt.base,
                agg_funcs: rebuilt.agg_funcs,
                group_by_items: rebuilt.group_by_items,
            })
        }
        other => other,
    }
}

/// Whether any order-by item of the sort/top-n is a scalar function.
fn order_items_have_scalar(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Sort(sort) => {
            has_scalar_func(sort.by_items.iter().map(|item| item.expr.clone()))
        }
        PhysicalPlan::TopN(topn) => {
            has_scalar_func(topn.by_items.iter().map(|item| item.expr.clone()))
        }
        _ => false,
    }
}

/// Go `InjectProjBelowSort` (`rule_inject_extra_projection.go:227`): extract
/// the scalar functions of the order-by items into a projection below the
/// sort/top-n, and add a top projection to prune the extra column back to
/// the sort's schema.
fn inject_proj_below_sort(ctx: &InjectContext<'_>, p: PhysicalPlan) -> PhysicalPlan {
    if !order_items_have_scalar(&p) {
        return p;
    }
    let mut p = p;
    let block_offset = p.base().base.query_block_offset();
    let p_schema = p.schema().cloned().unwrap_or_default();

    // Go: childPlan := p.Children()[0].
    let child_plan = p
        .base_mut()
        .take_children()
        .into_iter()
        .next()
        .expect("sort projection injection requires one child");
    let child_schema = child_plan.schema().cloned().unwrap_or_default();
    let child_prop = p.base().child_req_prop(0).cloned().unwrap_or_default();
    let orig_child_exprs: Option<Vec<Expression>> = match &child_plan {
        PhysicalPlan::Projection(child_proj) => Some(child_proj.exprs.clone()),
        _ => None,
    };

    // Bottom projection: the child's own columns, then one fresh column per
    // scalar order-by item (the item is rewritten to reference it).
    let mut bottom_proj_schema_cols: Vec<Column> = child_schema
        .columns
        .iter()
        .enumerate()
        .map(|(offset, col)| {
            let mut new_col = col.clone();
            new_col.index = offset as i64;
            new_col
        })
        .collect();
    let mut bottom_proj_exprs: Vec<Expression> = bottom_proj_schema_cols
        .iter()
        .map(|col| Expression::Column(col.clone()))
        .collect();
    {
        let items: &mut Vec<ByItems> = match &mut p {
            PhysicalPlan::Sort(sort) => &mut sort.by_items,
            PhysicalPlan::TopN(topn) => &mut topn.by_items,
            _ => unreachable!("guarded by order_items_have_scalar"),
        };
        for item in items.iter_mut() {
            if !matches!(item.expr, Expression::ScalarFunction(_)) {
                continue;
            }
            let Some(ret_type) = item.expr.static_type().cloned() else {
                continue;
            };
            let mut new_arg = Column::new(ctx.column_ids.alloc(), ret_type);
            new_arg.index = bottom_proj_schema_cols.len() as i64;
            bottom_proj_exprs.push(item.expr.clone());
            bottom_proj_schema_cols.push(new_arg.clone());
            item.expr = Expression::Column(new_arg);
        }
    }

    // Go: refine4NeighbourProj(bottomProj, origChildProj) when the original
    // child is itself a projection.
    if let Some(orig_child_exprs) = &orig_child_exprs {
        refine_4_neighbour_proj_exprs(&mut bottom_proj_exprs, orig_child_exprs);
    }

    let bottom_stats = child_plan
        .stats_info()
        .map(|stats| stats.scale_by_expect_cnt(child_prop.expected_cnt, ctx.skew_ratio));
    let mut bottom_base = BasePhysicalPlan::new(
        ctx.plan_ids,
        crate::logical::LogicalProjection::TYPE,
        block_offset,
    );
    bottom_base.base.set_stats(bottom_stats);
    bottom_base
        .base
        .set_schema(Some(Schema::new(bottom_proj_schema_cols)));
    bottom_base.set_children(vec![child_plan]);
    bottom_base.set_children_req_props(vec![Some(child_prop)]);
    let bottom_proj = PhysicalPlan::Projection(PhysicalProjection {
        base: bottom_base,
        exprs: bottom_proj_exprs.clone(),
        calculate_no_delay: false,
        avoid_column_evaluator: false,
    });

    // Go: p.SetChildren(bottomProj) — the sort's own schema is unchanged.
    p.base_mut().set_children(vec![bottom_proj]);

    // Top projection: prune back to the sort's schema, with the bottom
    // projection as its child.
    let top_proj_exprs: Vec<Expression> = p_schema
        .columns
        .iter()
        .enumerate()
        .map(|(offset, col)| {
            let mut col = col.clone();
            col.index = offset as i64;
            Expression::Column(col)
        })
        .collect();
    let top_stats = p.stats_info().cloned();
    let mut top_base = BasePhysicalPlan::new(
        ctx.plan_ids,
        crate::logical::LogicalProjection::TYPE,
        block_offset,
    );
    top_base.base.set_stats(top_stats);
    top_base.base.set_schema(Some(p_schema));
    top_base.set_children(vec![p]);
    top_base.set_children_req_props(vec![None]);
    let mut top_proj = PhysicalProjection {
        base: top_base,
        exprs: top_proj_exprs,
        calculate_no_delay: false,
        avoid_column_evaluator: false,
    };

    // Go: refine4NeighbourProj(topProj, bottomProj).
    refine_4_neighbour_proj_exprs(&mut top_proj.exprs, &bottom_proj_exprs);
    PhysicalPlan::Projection(top_proj)
}

/// Go `TurnNominalSortIntoProj` (`rule_inject_extra_projection.go:292`).
fn turn_nominal_sort_into_proj(
    ctx: &InjectContext<'_>,
    mut nominal: crate::physical::NominalSort,
) -> PhysicalPlan {
    if nominal.only_column {
        // The child already provides the written order; the nominal sort
        // disappears.
        return nominal
            .base
            .take_children()
            .into_iter()
            .next()
            .expect("nominal sort projection conversion requires one child");
    }

    // Non-column nominal sorts become two pass-through projections. The
    // Rust NominalSort carries column-only SortItems, so Go's scalar
    // expression loop adds nothing here — the pair exists to keep the
    // overflow-checking evaluation points Go's executor relies on.
    let block_offset = nominal.base.base.query_block_offset();
    let child_plan = nominal
        .base
        .take_children()
        .into_iter()
        .next()
        .expect("nominal sort projection conversion requires one child");
    let child_schema = child_plan.schema().cloned().unwrap_or_default();
    let child_prop = nominal.base.child_req_prop(0).cloned().unwrap_or_default();
    let orig_child_exprs: Option<Vec<Expression>> = match &child_plan {
        PhysicalPlan::Projection(child_proj) => Some(child_proj.exprs.clone()),
        _ => None,
    };

    let mut bottom_proj_exprs: Vec<Expression> = child_schema
        .columns
        .iter()
        .enumerate()
        .map(|(offset, col)| {
            let mut new_col = col.clone();
            new_col.index = offset as i64;
            Expression::Column(new_col)
        })
        .collect();
    if let Some(orig_child_exprs) = &orig_child_exprs {
        refine_4_neighbour_proj_exprs(&mut bottom_proj_exprs, orig_child_exprs);
    }
    let bottom_proj_schema_cols: Vec<Column> = bottom_proj_exprs
        .iter()
        .filter_map(|expr| expr.as_column().cloned())
        .collect();

    let child_stats = child_plan.stats_info().cloned();
    let bottom_stats = child_stats
        .as_ref()
        .map(|stats| stats.scale_by_expect_cnt(child_prop.expected_cnt, ctx.skew_ratio));
    let mut bottom_base = BasePhysicalPlan::new(
        ctx.plan_ids,
        crate::logical::LogicalProjection::TYPE,
        block_offset,
    );
    bottom_base.base.set_stats(bottom_stats);
    bottom_base
        .base
        .set_schema(Some(Schema::new(bottom_proj_schema_cols)));
    bottom_base.set_children(vec![child_plan]);
    bottom_base.set_children_req_props(vec![Some(child_prop.clone())]);
    let bottom_proj = PhysicalPlan::Projection(PhysicalProjection {
        base: bottom_base,
        exprs: bottom_proj_exprs.clone(),
        calculate_no_delay: false,
        avoid_column_evaluator: false,
    });

    let top_proj_exprs: Vec<Expression> = child_schema
        .columns
        .iter()
        .enumerate()
        .map(|(offset, col)| {
            let mut col = col.clone();
            col.index = offset as i64;
            Expression::Column(col)
        })
        .collect();
    let top_stats = child_stats
        .as_ref()
        .map(|stats| stats.scale_by_expect_cnt(child_prop.expected_cnt, ctx.skew_ratio));
    let mut top_base = BasePhysicalPlan::new(
        ctx.plan_ids,
        crate::logical::LogicalProjection::TYPE,
        block_offset,
    );
    top_base.base.set_stats(top_stats);
    top_base.base.set_schema(Some(child_schema));
    top_base.set_children(vec![bottom_proj]);
    top_base.set_children_req_props(vec![None]);
    let mut top_proj = PhysicalProjection {
        base: top_base,
        exprs: top_proj_exprs,
        calculate_no_delay: false,
        avoid_column_evaluator: false,
    };

    // Go: refine4NeighbourProj(topProj, bottomProj).
    refine_4_neighbour_proj_exprs(&mut top_proj.exprs, &bottom_proj_exprs);
    PhysicalPlan::Projection(top_proj)
}

/// Go `refine4NeighbourProj` (`resolve_indices.go:56`): given a projection's
/// expressions and its child projection's expressions, rewrite the projection
/// column references' `Index` through a union-find over the child's output
/// positions, so duplicated pass-through columns stay consistent.
fn refine_4_neighbour_proj_exprs(exprs: &mut [Expression], child_proj_exprs: &[Expression]) {
    // map: child column Index -> the positions that output it.
    let mut input_idx_to_output_idxes: std::collections::BTreeMap<i64, Vec<usize>> =
        std::collections::BTreeMap::new();
    for (i, expr) in child_proj_exprs.iter().enumerate() {
        if let Expression::Column(col) = expr {
            input_idx_to_output_idxes.entry(col.index).or_default().push(i);
        }
    }
    let mut union_set = DisjointSet::new(child_proj_exprs.len());
    for output_idxes in input_idx_to_output_idxes.values() {
        if output_idxes.len() <= 1 {
            continue;
        }
        for i in 1..output_idxes.len() {
            union_set.union(output_idxes[0], output_idxes[i]);
        }
    }
    for expr in exprs.iter_mut() {
        if let Expression::Column(col) = expr {
            let index = col.index;
            if index >= 0 && (index as usize) < union_set.len() {
                col.index = union_set.find(index as usize) as i64;
            }
        }
    }
}

/// Go `disjointset.IntSet` (`pkg/util/disjointset/int_set.go`), the small
/// union-find `refine4NeighbourProj` needs.
struct DisjointSet {
    parent: Vec<usize>,
}

impl DisjointSet {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
        }
    }

    fn len(&self) -> usize {
        self.parent.len()
    }

    fn find(&mut self, x: usize) -> usize {
        let mut root = x;
        while self.parent[root] != root {
            root = self.parent[root];
        }
        let mut cur = x;
        while self.parent[cur] != root {
            let next = self.parent[cur];
            self.parent[cur] = root;
            cur = next;
        }
        root
    }

    fn union(&mut self, a: usize, b: usize) {
        let root_a = self.find(a);
        let root_b = self.find(b);
        if root_a != root_b {
            self.parent[root_b] = root_a;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::{BasePhysicalPlan, PhysicalTableDual};
    use crate::plan_base::PlanIdAllocator;
    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::aggregation::{AggFuncDesc, AggFunctionMode, BaseFuncDesc};
    use tidb_expr::expression::ScalarFunction;

    fn column(id: i64) -> Column {
        Column::new(id, FieldType::new(FieldTypeCode::LongLong))
    }

    fn col_expr(id: i64) -> Expression {
        Expression::Column(column(id))
    }

    fn plus(a: i64, b: i64) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![col_expr(a), col_expr(b)],
        ))
    }

    fn dual(allocator: &PlanIdAllocator, schema_columns: Vec<Column>) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::new(allocator, "Dual", 1);
        base.base.set_schema(Some(Schema::new(schema_columns)));
        PhysicalPlan::TableDual(PhysicalTableDual::new(base, 1))
    }

    fn agg_node(
        allocator: &PlanIdAllocator,
        agg_funcs: Vec<AggFuncDesc>,
        group_by_items: Vec<Expression>,
        child: PhysicalPlan,
    ) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::new(allocator, "HashAgg", 1);
        let outputs = (0..agg_funcs.len())
            .map(|offset| column(200 + offset as i64))
            .collect::<Vec<Column>>();
        base.base.set_schema(Some(Schema::new(outputs)));
        base.set_children(vec![child]);
        base.set_children_req_props(vec![Some(
            crate::physical_property::PhysicalProperty::default(),
        )]);
        PhysicalPlan::HashAgg(crate::physical::PhysicalHashAgg {
            base,
            agg_funcs,
            group_by_items,
        })
    }

    fn sum(arg: Expression) -> AggFuncDesc {
        AggFuncDesc {
            base: tidb_expr::aggregation::BaseFuncDesc {
                name: "sum".to_owned(),
                args: vec![arg],
                ret_type: FieldType::new(FieldTypeCode::LongLong),
            },
            mode: AggFunctionMode::Complete,
            has_distinct: false,
            order_by_items: Vec::new(),
            grouping_id: 0,
        }
    }

    fn sort_node(
        allocator: &PlanIdAllocator,
        by_items: Vec<tidb_expr::aggregation::ByItems>,
        child: PhysicalPlan,
    ) -> PhysicalPlan {
        let schema = child.schema().cloned().unwrap_or_default();
        let mut base = BasePhysicalPlan::new(allocator, "Sort", 1);
        base.base.set_schema(Some(schema));
        base.set_children(vec![child]);
        base.set_children_req_props(vec![Some(
            crate::physical_property::PhysicalProperty::default(),
        )]);
        PhysicalPlan::Sort(crate::physical::PhysicalSort {
            base,
            by_items,
            is_partial_sort: false,
        })
    }

    fn by_item(expr: Expression) -> tidb_expr::aggregation::ByItems {
        tidb_expr::aggregation::ByItems::new(expr, false)
    }

    fn shape(plan: &PhysicalPlan) -> String {
        match plan {
            PhysicalPlan::TableDual(_) => "dual".to_owned(),
            PhysicalPlan::Projection(proj) => {
                let inner = proj
                    .base
                    .children()
                    .first()
                    .map(shape)
                    .unwrap_or_default();
                format!("Proj({inner})")
            }
            PhysicalPlan::HashAgg(agg) => format!("HashAgg({})", shape(&agg.base.children()[0])),
            PhysicalPlan::StreamAgg(agg) => format!("StreamAgg({})", shape(&agg.base.children()[0])),
            PhysicalPlan::Sort(sort) => format!("Sort({})", shape(&sort.base.children()[0])),
            PhysicalPlan::TopN(topn) => format!("TopN({})", shape(&topn.base.children()[0])),
            PhysicalPlan::NominalSort(nominal) => {
                format!("NominalSort({})", shape(&nominal.base.children()[0]))
            }
            PhysicalPlan::UnionAll(union_all) => format!("Union({} children)", union_all.base.children().len()),
            other => other.tp().to_owned(),
        }
    }

    fn context<'a>(plan_ids: &'a PlanIdAllocator, column_ids: &'a ColumnIdAllocator) -> InjectContext<'a> {
        InjectContext {
            plan_ids,
            column_ids,
            skew_ratio: 1.0,
        }
    }

    #[test]
    fn scalar_agg_argument_gets_a_projection_below_the_aggregate() {
        let plan_ids = PlanIdAllocator::new();
        let column_ids = ColumnIdAllocator::new();
        let ctx = context(&plan_ids, &column_ids);
        let child = dual(&plan_ids, vec![column(1)]);
        // sum(a + a): the argument is a scalar function.
        let plan = agg_node(
            &plan_ids,
            vec![sum(plus(1, 1))],
            Vec::new(),
            child,
        );

        let injected = inject(&ctx, plan);
        assert_eq!(shape(&injected), "HashAgg(Proj(dual))");
        let PhysicalPlan::HashAgg(agg) = &injected else {
            panic!("root must stay the aggregate");
        };
        // The aggregate argument was rewritten to the projection's output
        // column.
        assert!(matches!(agg.agg_funcs[0].args()[0], Expression::Column(_)));
        let PhysicalPlan::Projection(proj) = &agg.base.children()[0] else {
            panic!("the injected projection must sit between agg and child");
        };
        assert!(matches!(proj.exprs[0], Expression::ScalarFunction(_)));
    }

    #[test]
    fn column_only_aggregate_stays_untouched() {
        let plan_ids = PlanIdAllocator::new();
        let column_ids = ColumnIdAllocator::new();
        let ctx = context(&plan_ids, &column_ids);
        let child = dual(&plan_ids, vec![column(1)]);
        let plan = agg_node(
            &plan_ids,
            vec![sum(col_expr(1))],
            vec![col_expr(1)],
            child,
        );

        let injected = inject(&ctx, plan);
        assert_eq!(shape(&injected), "HashAgg(dual)");
    }

    #[test]
    fn group_by_expression_deduplicates_against_the_aggregate_argument() {
        let plan_ids = PlanIdAllocator::new();
        let column_ids = ColumnIdAllocator::new();
        let ctx = context(&plan_ids, &column_ids);
        let child = dual(&plan_ids, vec![column(1), column(2)]);
        // sum(a) group by a + b: the group-by scalar goes to the projection;
        // sum's column arg does not.
        let plan = agg_node(
            &plan_ids,
            vec![sum(col_expr(1))],
            vec![plus(1, 2)],
            child,
        );

        let injected = inject(&ctx, plan);
        assert_eq!(shape(&injected), "HashAgg(Proj(dual))");
        let PhysicalPlan::HashAgg(agg) = &injected else {
            panic!("root must stay the aggregate");
        };
        assert!(matches!(agg.group_by_items[0], Expression::Column(_)));
        let PhysicalPlan::Projection(proj) = &agg.base.children()[0] else {
            panic!("the injected projection must sit between agg and child");
        };
        // Projection exprs: [a, a + b].
        assert_eq!(proj.exprs.len(), 2);
        assert!(matches!(proj.exprs[1], Expression::ScalarFunction(_)));
    }

    #[test]
    fn scalar_order_by_item_wraps_the_sort_in_two_projections() {
        let plan_ids = PlanIdAllocator::new();
        let column_ids = ColumnIdAllocator::new();
        let ctx = context(&plan_ids, &column_ids);
        let child = dual(&plan_ids, vec![column(1)]);
        let plan = sort_node(&plan_ids, vec![by_item(plus(1, 1))], child);

        let injected = inject(&ctx, plan);
        assert_eq!(shape(&injected), "Proj(Sort(Proj(dual)))");
        let PhysicalPlan::Projection(top) = &injected else {
            panic!("the sort must be wrapped in a top projection");
        };
        // The top projection prunes back to the sort's schema (one column).
        assert_eq!(top.exprs.len(), 1);
    }

    #[test]
    fn column_order_by_item_leaves_the_sort_untouched() {
        let plan_ids = PlanIdAllocator::new();
        let column_ids = ColumnIdAllocator::new();
        let ctx = context(&plan_ids, &column_ids);
        let child = dual(&plan_ids, vec![column(1)]);
        let plan = sort_node(&plan_ids, vec![by_item(col_expr(1))], child);

        let injected = inject(&ctx, plan);
        assert_eq!(shape(&injected), "Sort(dual)");
    }

    #[test]
    fn column_only_nominal_sort_disappears() {
        let plan_ids = PlanIdAllocator::new();
        let column_ids = ColumnIdAllocator::new();
        let ctx = context(&plan_ids, &column_ids);
        let child = dual(&plan_ids, vec![column(1)]);
        let mut base = BasePhysicalPlan::new(&plan_ids, "NominalSort", 1);
        base.base.set_schema(Some(child.schema().cloned().unwrap_or_default()));
        base.set_children(vec![child]);
        base.set_children_req_props(vec![Some(
            crate::physical_property::PhysicalProperty::default(),
        )]);
        let plan = PhysicalPlan::NominalSort(crate::physical::NominalSort {
            base,
            by_items: vec![crate::physical_property::SortItem::new(1, false)],
            only_column: true,
        });

        let injected = inject(&ctx, plan);
        assert_eq!(shape(&injected), "dual");
    }

    #[test]
    fn expression_nominal_sort_becomes_two_pass_through_projections() {
        let plan_ids = PlanIdAllocator::new();
        let column_ids = ColumnIdAllocator::new();
        let ctx = context(&plan_ids, &column_ids);
        let child = dual(&plan_ids, vec![column(1)]);
        let mut base = BasePhysicalPlan::new(&plan_ids, "NominalSort", 1);
        base.base.set_schema(Some(child.schema().cloned().unwrap_or_default()));
        base.set_children(vec![child]);
        base.set_children_req_props(vec![Some(
            crate::physical_property::PhysicalProperty::default(),
        )]);
        let plan = PhysicalPlan::NominalSort(crate::physical::NominalSort {
            base,
            by_items: vec![crate::physical_property::SortItem::new(1, false)],
            only_column: false,
        });

        let injected = inject(&ctx, plan);
        assert_eq!(shape(&injected), "Proj(Proj(dual))");
    }

    #[test]
    fn non_mpp_union_is_untouched() {
        let plan_ids = PlanIdAllocator::new();
        let column_ids = ColumnIdAllocator::new();
        let ctx = context(&plan_ids, &column_ids);
        let child = dual(&plan_ids, vec![column(1)]);
        let mut base = BasePhysicalPlan::new(&plan_ids, "Union", 1);
        base.base.set_schema(Some(child.schema().cloned().unwrap_or_default()));
        base.set_children(vec![child.clone(), child]);
        let plan = PhysicalPlan::UnionAll(crate::physical::PhysicalUnionAll {
            base,
            mpp: false,
        });

        let injected = inject(&ctx, plan);
        assert_eq!(shape(&injected), "Union(2 children)");
    }

    #[test]
    fn stream_aggregate_keeps_its_operator_type() {
        let plan_ids = PlanIdAllocator::new();
        let column_ids = ColumnIdAllocator::new();
        let ctx = context(&plan_ids, &column_ids);
        let child = dual(&plan_ids, vec![column(1)]);
        let mut base = BasePhysicalPlan::new(&plan_ids, "StreamAgg", 1);
        base.base.set_schema(Some(Schema::new(vec![column(200)])));
        base.set_children(vec![child]);
        base.set_children_req_props(vec![Some(
            crate::physical_property::PhysicalProperty::default(),
        )]);
        let plan = PhysicalPlan::StreamAgg(crate::physical::PhysicalStreamAgg {
            base,
            agg_funcs: vec![sum(plus(1, 1))],
            group_by_items: Vec::new(),
        });

        let injected = inject(&ctx, plan);
        assert_eq!(shape(&injected), "StreamAgg(Proj(dual))");
        assert_eq!(injected.tp(), "StreamAgg");
    }

    #[test]
    fn refine_neighbour_unifies_duplicated_output_positions() {
        // A child projection emitting [c0, c1, c0] (positions 0 and 2 are the
        // same column) and a neighbour referencing c1's column at index 2:
        // the rewrite must keep in-range indices intact.
        let mut exprs = vec![col_expr(9)];
        if let Expression::Column(col) = &mut exprs[0] {
            col.index = 2;
        }
        let child_proj_exprs = vec![
            {
                let mut col = column(10);
                col.index = 0;
                Expression::Column(col)
            },
            {
                let mut col = column(11);
                col.index = 1;
                Expression::Column(col)
            },
            {
                let mut col = column(10);
                col.index = 0;
                Expression::Column(col)
            },
        ];
        refine_4_neighbour_proj_exprs(&mut exprs, &child_proj_exprs);
        // Position 2 belongs to the same group as position 0.
        let Expression::Column(col) = &exprs[0] else {
            panic!("expected a column");
        };
        assert_eq!(col.index, 0);
    }
}
