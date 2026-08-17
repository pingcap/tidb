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

//! Go `DeriveTopNFromWindow`
//! (`pkg/planner/core/rule_derive_topn_from_window.go`), Go rule #19, plus the
//! only operator body it dispatches to,
//! `LogicalSelection.DeriveTopN` (`logical_selection.go:167`) and its
//! `windowIsTopN` predicate (`logical_selection.go:349`).
//!
//! # What is derived
//!
//! `SELECT * FROM (SELECT ..., row_number() OVER (...) rn FROM t) WHERE rn <= 5`
//! only ever needs the first five rows of each partition, so a `LogicalTopN`
//! is planted UNDER the window:
//!
//! ```text
//! Selection(rn <= 5)          Selection(rn <= 5)
//!   └─ Window                   └─ Window
//!        └─ DataSource     ⇒          └─ TopN(count=5, by=window ORDER BY)
//!                                          └─ DataSource
//! ```
//!
//! Go's own comment for that shape is "return select->datasource->topN->window".
//!
//! # Every guard in `windowIsTopN`, and why each is load-bearing
//!
//! * the child is a `LogicalWindow` and the selection has EXACTLY one
//!   condition;
//! * that condition is an upper bound `col < n` / `col <= n` with `n > 0`
//!   (`expression.FindUpperBound`);
//! * the bounded column is the window's ONLY result column — bounding anything
//!   else says nothing about how many rows the window must see;
//! * the window's child is a `DataSource`;
//! * the window function is a single `row_number()` over a `ROWS BETWEEN
//!   CURRENT ROW AND CURRENT ROW` frame — `row_number` is the only function
//!   whose value is monotone in the frame's row position, which is what makes
//!   the bound a row COUNT;
//! * `checkPartitionBy`: the `PARTITION BY` prefix is a prefix of the data
//!   source's clustered-index order, so the partitions arrive contiguously.
//!
//! # Narrowings, by name
//!
//! * `sessionVars.AllowDeriveTopN` gates Go's `BaseLogicalPlan.DeriveTopN`
//!   recursion (`base_logical_plan.go:169`). It is a [`RuleContext`] field
//!   here, threaded rather than read off the plan.
//! * `dataSource.AllPossibleAccessPaths[i].StoreType == kv.TiFlash` refuses the
//!   derivation ("Pushing down window aggregation is good enough in this
//!   case"). [`super::super::access_path::DataSourceAccessPath`] does not carry
//!   `util.AccessPath.StoreType`, so [`super::DataSource::has_tiflash_replica`]
//!   stands in: a table with a TiFlash replica is where such a path can come
//!   from, so the test refuses a SUPERSET of what Go refuses. Refusing is the
//!   safe direction — the plan keeps the window without a derived TopN, which
//!   is what Go produces whenever the guard fires.
//! * `expression.Column.Equal(evalCtx, other)` compares by `UniqueID` for
//!   columns, which is what [`tidb_expr::column::Column::equal_column`] does.

use tidb_expr::aggregation::ByItems;
use tidb_expr::expr_util::extract::find_upper_bound;
use tidb_expr::expression::Expression;

use crate::plan_base::PlanError;

use super::fold::{fold_owned, Descend, OwnedRewrite};
use super::rule::{LogicalOptRule, RuleContext};
use super::topn::LogicalTopN;
use super::window::{BoundType, FrameType, LogicalWindow};
use super::{BaseLogicalPlan, LogicalPlan};

/// Go's `"row_number"`, the only window function this rule accepts.
const ROW_NUMBER: &str = "row_number";

/// Go `checkPartitionBy(p, d)` (`logical_selection.go:394`): the window's
/// `PARTITION BY` must be a prefix of the data source's handle columns.
fn check_partition_by(window: &LogicalWindow, handle_cols: &[tidb_expr::column::Column]) -> bool {
    if window.partition_by.is_empty() {
        return true;
    }
    // Go: "Table not clustered and window has partition by" — `d.HandleCols ==
    // nil` — and the too-long case, both refusals.
    if handle_cols.is_empty() || window.partition_by.len() > handle_cols.len() {
        return false;
    }
    window
        .partition_by
        .iter()
        .zip(handle_cols)
        .all(|(item, handle)| item.col.unique_id == handle.unique_id)
}

/// Go `(*LogicalSelection).windowIsTopN()` (`logical_selection.go:349`).
///
/// Returns Go's `(true, limitValue)` as `Some(limit)`.
fn window_is_topn(selection: &super::LogicalSelection, child: &LogicalPlan) -> Option<u64> {
    let LogicalPlan::Window(window) = child else {
        return None;
    };
    if selection.conditions.len() != 1 {
        return None;
    }
    let (column, limit_value) = find_upper_bound(&selection.conditions[0])?;
    if limit_value <= 0 {
        return None;
    }
    // Go: the bounded column must be the window's single result column.
    let window_schema = child.schema()?;
    let window_columns = window.get_window_result_columns(window_schema);
    if window_columns.len() != 1 || window_columns[0].unique_id != column.unique_id {
        return None;
    }
    let LogicalPlan::DataSource(data_source) = child.children().first()? else {
        return None;
    };
    // Go: "Give up if TiFlash is one possible access path of all." See this
    // module's narrowings.
    if data_source.has_tiflash_replica {
        return None;
    }
    let frame = window.frame.as_ref()?;
    let is_single_row_number =
        window.window_func_descs.len() == 1 && window.window_func_descs[0].base.name == ROW_NUMBER;
    let is_current_row_frame = frame.frame_type == FrameType::Rows
        && frame
            .start
            .as_ref()
            .is_some_and(|bound| bound.bound_type == BoundType::CurrentRow)
        && frame
            .end
            .as_ref()
            .is_some_and(|bound| bound.bound_type == BoundType::CurrentRow);
    if is_single_row_number
        && is_current_row_frame
        && check_partition_by(window, &data_source.handle_cols)
    {
        // Go's `uint64(limitValue)`, reached only with `limitValue > 0`.
        return u64::try_from(limit_value).ok();
    }
    None
}

struct DeriveTopN<'a, 'ctx> {
    ctx: &'a RuleContext<'ctx>,
}

impl OwnedRewrite for DeriveTopN<'_, '_> {
    type Down = ();
    type Up = ();

    fn descend(&mut self, node: &mut LogicalPlan, (): ()) -> Descend<(), ()> {
        Descend::Children((0..node.children().len()).map(|_| ()).collect())
    }

    fn ascend(&mut self, mut node: LogicalPlan, _child_ups: Vec<()>) -> (LogicalPlan, ()) {
        // Go's ONLY override of `DeriveTopN` is `LogicalSelection`'s; every
        // other operator takes the base body, which just recurses.
        let LogicalPlan::Selection(selection) = &node else {
            return (node, ());
        };
        let Some(child) = node.children().first() else {
            return (node, ());
        };
        let Some(count) = window_is_topn(selection, child) else {
            return (node, ());
        };
        let LogicalPlan::Window(window) = child else {
            unreachable!("window_is_topn answered Some only for a LogicalWindow child")
        };
        // Go builds the ByItems from the window's ORDER BY, keeping direction.
        let by_items: Vec<ByItems> = window
            .order_by
            .iter()
            .map(|item| ByItems {
                expr: Expression::Column(item.col.clone()),
                desc: item.desc,
            })
            .collect();
        let partition_by = window.get_partition_by().to_vec();

        // Go: `LogicalTopN{...}.Init(grandChild.SCtx(),
        // grandChild.QueryBlockOffset())`, i.e. the DATA SOURCE's offset.
        let mut window_node = node
            .base_mut()
            .take_children()
            .pop()
            .unwrap_or_else(|| unreachable!("the child was just observed"));
        let grand_child = window_node
            .base_mut()
            .take_children()
            .pop()
            .unwrap_or_else(|| unreachable!("window_is_topn saw the window's DataSource child"));
        let base = BaseLogicalPlan::new(
            self.ctx.allocator,
            LogicalTopN::TYPE,
            grand_child.base().base.query_block_offset(),
        );
        let mut derived = LogicalTopN::new(base, by_items, 0, count);
        derived.partition_by = partition_by
            .iter()
            .map(|item| crate::physical_property::SortItem::new(item.col.unique_id, item.desc))
            .collect();
        let mut topn = LogicalPlan::TopN(derived);
        topn.set_children(vec![grand_child]);
        window_node.set_children(vec![topn]);
        node.set_children(vec![window_node]);
        (node, ())
    }
}

/// Go `p.DeriveTopN()` (`base_logical_plan.go:169` plus
/// `logical_selection.go:167`), as one [`fold_owned`].
///
/// The whole walk is gated on Go's `AllowDeriveTopN`, exactly as the base body
/// gates its own recursion.
#[must_use]
pub fn derive_topn(ctx: &RuleContext<'_>, plan: LogicalPlan) -> LogicalPlan {
    if !ctx.allow_derive_topn {
        return plan;
    }
    let mut rewrite = DeriveTopN { ctx };
    fold_owned(&mut rewrite, plan, ()).0
}

/// Go `DeriveTopNFromWindow` (`rule_derive_topn_from_window.go:24`), Go rule
/// #19.
///
/// [`crate::derive_topn_from_window`] is the crate's trait-shaped seed of the
/// same rule and is KEPT — `difftests/planner-tests` consumes it from outside
/// this crate.
#[derive(Debug)]
pub struct DeriveTopNFromWindow;

impl LogicalOptRule for DeriveTopNFromWindow {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        // Go hard-codes `planChanged := false`.
        Ok((derive_topn(ctx, plan), false))
    }

    fn name(&self) -> &'static str {
        "derive_topn_from_window"
    }
}
