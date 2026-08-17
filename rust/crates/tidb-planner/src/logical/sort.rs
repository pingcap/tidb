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

//! Go `pkg/planner/core/operator/logicalop/logical_sort.go`: `LogicalSort`,
//! the `ORDER BY` operator, plus the `ByItems` pruning and property extraction
//! that `logical_top_n.go` shares with it
//! (`pkg/planner/core/operator/logicalop/logical_plans_misc.go`).
//!
//! SEED of `pkg/planner/core`. The operator was previously a SKELETON in
//! [`crate::logical`] carrying only its `ByItems`; this file gives it its real
//! member bodies. The `ByItems` element type moves from an anonymous
//! `(Expression, bool)` pair to [`ByItems`], which IS Go's
//! `pkg/planner/util.ByItems` as `tidb-expr` already ports it.
//!
//! # Narrowings, by name
//!
//! * `ExplainInfo` is `util.ExplainByItems(evalCtx, buffer, ls.ByItems)`,
//!   which renders each expression through `Expression.StringWithCtx` and so
//!   needs an `EvalContext`. [`LogicalSort::explain_info`] reports the item
//!   COUNT rather than silently dropping the list, the same shape
//!   [`crate::logical::LogicalJoin::explain_info`] uses.
//! * `ReplaceExprColumns` calls `ruleutil.ResolveExprAndReplace`, which is not
//!   transcreated; it is absent rather than approximated.
//! * `PushDownTopN` is a RECURSION into `Children()[0]`, so its local decision
//!   lands here as [`LogicalSort::push_down_topn_decision`] and the walk
//!   belongs to the enum-level driver.

use std::collections::BTreeSet;

use tidb_datatype::FieldTypeCode;
use tidb_expr::aggregation::ByItems;
use tidb_expr::column::Column;
use tidb_expr::expr_util::predicates::is_runtime_const_expr;
use tidb_expr::expression::CorrelatedColumn;
use tidb_expr::simple_expr::{extract_columns, extract_cor_columns};

use crate::logical::BaseLogicalPlan;
use crate::plan_base::PossiblePropertiesInfo;

/// Go `logicalop.LogicalSort` (`logical_sort.go:28`).
#[derive(Clone, Debug, Default)]
pub struct LogicalSort {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `ByItems []*util.ByItems`.
    pub by_items: Vec<ByItems>,
}

/// What [`LogicalSort::push_down_topn_decision`] tells the driver to do with
/// the `LogicalTopN` a parent is pushing through this sort.
///
/// Go `LogicalSort.PushDownTopN` (`logical_sort.go:84`) expresses the same
/// three outcomes by mutating `topN` and returning a child walk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortTopNPushDown {
    /// Go's `topN == nil` branch: no TopN to push, fall through to the base
    /// body, which pushes `nil` into the child and keeps this sort.
    KeepSort,
    /// Go's `topN.IsLimit()` branch: the pushed node is a bare limit, so it
    /// ADOPTS this sort's `ByItems` and replaces the sort entirely.
    AdoptByItemsAndDropSort,
    /// A real TopN was pushed, so "this sort is useless" — Go's own comment —
    /// and the TopN goes into the child unchanged.
    DropSort,
}

impl LogicalSort {
    /// Go `plancodec.TypeSort`.
    pub const TYPE: &'static str = "Sort";

    /// Go `LogicalSort.Init(ctx, offset)` (`logical_sort.go:35`).
    #[must_use]
    pub fn new(base: BaseLogicalPlan, by_items: Vec<ByItems>) -> Self {
        Self { base, by_items }
    }

    /// Go `LogicalSort.ExtractCorrelatedCols()` (`logical_sort.go:133`): every
    /// correlated column under every `ByItem`, appended in order and NOT
    /// deduplicated.
    #[must_use]
    pub fn extract_correlated_cols(&self) -> Vec<CorrelatedColumn> {
        let mut cor_cols = Vec::with_capacity(self.by_items.len());
        for item in &self.by_items {
            cor_cols.extend(extract_cor_columns(&item.expr));
        }
        cor_cols
    }

    /// Go `LogicalSort.GetUsedCols()` (`logical_sort.go:162`): every column any
    /// `ByItem` reads, without deduplication across items.
    #[must_use]
    pub fn get_used_cols(&self) -> Vec<Column> {
        let mut used = Vec::new();
        for item in &self.by_items {
            used.extend(extract_columns(&item.expr));
        }
        used
    }

    /// Go `LogicalSort.PruneColumns(parentUsedCols)`'s LOCAL half
    /// (`logical_sort.go:67`): drop the `ByItems` that cannot affect the order,
    /// then report the column set the child must still produce.
    ///
    /// The recursion into `children[0]` belongs to the enum-level driver; see
    /// [`crate::logical::LogicalPlan::prune_columns`].
    pub fn prune_columns_local(&mut self, parent_used_cols: &[Column]) -> Vec<Column> {
        let (kept, from_items) = prune_by_items(&self.by_items);
        self.by_items = kept;
        let mut used = parent_used_cols.to_vec();
        used.extend(from_items);
        used
    }

    /// Go `LogicalSort.PreparePossibleProperties(_, infos...)`
    /// (`logical_sort.go:114`): a sort OFFERS its own order, provided the
    /// leading `ByItems` are bare columns.
    ///
    /// Note that this DISCARDS the child's orders, unlike the base body: an
    /// explicit sort establishes the order regardless of what arrived.
    pub fn prepare_possible_properties(
        &mut self,
        child: Option<&PossiblePropertiesInfo>,
    ) -> PossiblePropertiesInfo {
        let has_tiflash = child.is_some_and(|info| info.has_tiflash);
        self.base.set_has_tiflash(has_tiflash);
        let prop_cols = get_possible_property_from_by_items(&self.by_items);
        PossiblePropertiesInfo {
            orders: if prop_cols.is_empty() {
                Vec::new()
            } else {
                vec![prop_cols]
            },
            has_tiflash,
        }
    }

    /// Go `LogicalSort.PushDownTopN(topNLogicalPlan)`
    /// (`logical_sort.go:84`)'s local decision; see [`SortTopNPushDown`].
    #[must_use]
    pub const fn push_down_topn_decision(pushed_is_limit: Option<bool>) -> SortTopNPushDown {
        match pushed_is_limit {
            None => SortTopNPushDown::KeepSort,
            Some(true) => SortTopNPushDown::AdoptByItemsAndDropSort,
            Some(false) => SortTopNPushDown::DropSort,
        }
    }

    /// Go `LogicalSort.ExplainInfo()` (`logical_sort.go:43`).
    ///
    /// # Blocked
    ///
    /// `util.ExplainByItems(evalCtx, buffer, ls.ByItems)` renders each item as
    /// `Expression.StringWithCtx(evalCtx, ...)` plus `" desc"`; there is no
    /// `EvalContext` in this crate. The item count is reported so the string
    /// is never silently empty.
    #[must_use]
    pub fn explain_info(&self) -> String {
        if self.by_items.is_empty() {
            return String::new();
        }
        format!("{} by items", self.by_items.len())
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            by_items: self.by_items.clone(),
        }
    }
}

/// Go `getPossiblePropertyFromByItems(items)` (`logical_sort.go:169`): the
/// LEADING run of items that are bare column references.
///
/// Go `break`s at the first non-column item, so a trailing expression truncates
/// the offered order rather than dropping it.
#[must_use]
pub fn get_possible_property_from_by_items(items: &[ByItems]) -> Vec<Column> {
    let mut cols = Vec::with_capacity(items.len());
    for item in items {
        match item.expr.as_column() {
            Some(column) => cols.push(column.clone()),
            None => break,
        }
    }
    cols
}

/// Go `pruneByItems(p, old)` (`logical_plans_misc.go:139`): the `ByItems` that
/// can still affect the order, and the child columns they read.
///
/// An item is pruned when it
/// * repeats an earlier item's `HashCode` — a duplicate sort key is a no-op;
/// * reads no column and is a runtime constant — the same value for every row;
/// * has result type `NULL` — every row compares equal.
///
/// # Narrowing
///
/// Go reads the type through `byItem.Expr.GetType(evalCtx)` and would nil-deref
/// on an absent `RetType`; [`Expression::static_type`] can report `None`, and an
/// item with no static type is KEPT, which is the direction that cannot silently
/// drop an ordering.
#[must_use]
pub fn prune_by_items(old: &[ByItems]) -> (Vec<ByItems>, Vec<Column>) {
    let mut by_items = Vec::with_capacity(old.len());
    let mut parent_used_cols = Vec::new();
    let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
    for item in old {
        let hash = item.expr.clone().hash_code().to_vec();
        // Go records the hash BEFORE testing, so the first occurrence survives
        // and every later one is pruned.
        let hash_match = !seen.insert(hash);
        if hash_match {
            continue;
        }
        let cols = extract_columns(&item.expr);
        if cols.is_empty() {
            if !is_runtime_const_expr(&item.expr) {
                by_items.push(item.clone());
            }
        } else if item
            .expr
            .static_type()
            .is_none_or(|ty| ty.code() != FieldTypeCode::Null)
        {
            parent_used_cols.extend(cols);
            by_items.push(item.clone());
        }
    }
    (by_items, parent_used_cols)
}
