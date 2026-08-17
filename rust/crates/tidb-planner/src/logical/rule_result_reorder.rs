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

//! Go `ResultReorder` (`pkg/planner/core/rule_result_reorder.go`), Go rule #2.
//!
//! Go's own note, kept because it bounds the rule: "it's not a common rule for
//! all queries, it's specially implemented for a few customers". `select a from
//! t` may return `1 2` or `2 1`; this rule makes the answer deterministic by
//! putting a total order on the result.
//!
//! Go's recipe, verbatim from its comment:
//!
//! 1. walk down from the root ignoring every INPUT-ORDER KEEPER
//!    (`LogicalSelection`, `LogicalProjection`, `LogicalLimit`,
//!    `LogicalTableDual`);
//! 2. at the first operator that is not one:
//!    * a `LogicalSort` absorbs the missing columns into its `ByItems`;
//!    * anything else gets a fresh `LogicalSort` INJECTED above it.
//!
//! # One walk, not Go's two
//!
//! Go runs `completeSort` and then, only if it returned false, `injectSort` —
//! and both walk the SAME keeper chain from the root by the same test. The
//! second walk therefore stops at exactly the node the first walk stopped at,
//! so the two collapse into one descent whose terminal case branches on Sort
//! versus not-Sort. Go's third outcome — `completeSort` returning true because
//! a keeper ran out of children — is the branch that does nothing at all.
//!
//! The walk is the ONE spine from the root: [`super::fold::Descend::Stop`]
//! ends it at the terminal node, and a keeper hands the carried bit to its
//! FIRST child only, so no other subtree is entered. That is Go, which only
//! ever indexes `Children()[0]`.
//!
//! # Ordering by the handle instead of by every column
//!
//! `extractHandleCol` is a best-effort search for the row's handle: a total
//! order on the handle is a total order on the rows, and it is one `ByItem`
//! instead of one per output column. Go walks down through `LogicalSelection`
//! and `LogicalLimit` to a `DataSource`, and on the way back up requires each
//! level's schema to still CONTAIN the column, because an inlined projection
//! may have dropped it.

use tidb_expr::aggregation::ByItems;
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;

use crate::base_arms;
use crate::plan_base::PlanError;

use super::fold::{fold_owned, Descend, OwnedRewrite};
use super::rule::{LogicalOptRule, RuleContext};
use super::sort::LogicalSort;
use super::{BaseLogicalPlan, LogicalPlan};

/// Go `(*ResultReorder).isInputOrderKeeper(lp)`
/// (`rule_result_reorder.go:100`).
fn is_input_order_keeper(node: &LogicalPlan) -> bool {
    matches!(
        node,
        LogicalPlan::Selection(_)
            | LogicalPlan::Projection(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::TableDual(_)
    )
}

/// Go `(*ResultReorder).extractHandleCol(lp)`
/// (`rule_result_reorder.go:109`), iteratively.
///
/// Go recurses down the `LogicalSelection`/`LogicalLimit` chain and checks each
/// level's schema on the way back up; the chain is collected here and then
/// checked in reverse, which is the same order of tests without the recursion
/// this module bans.
///
/// # Narrowing
///
/// Go's `DataSource` arm refuses a common-handle table
/// (`x.TableInfo.IsCommonHandle`, "deliberately don't support common handle
/// case for simplicity"). [`super::DataSource`] does not carry
/// `model.TableInfo`; its `common_handle_cols` is non-empty exactly for such a
/// table, and that is what is tested. The direction is the refusing one: a
/// table with common-handle columns yields no handle, so the rule falls back
/// to ordering by every output column, which is a STRONGER order than Go's.
fn extract_handle_col(node: &LogicalPlan) -> Option<Column> {
    let mut chain: Vec<&LogicalPlan> = Vec::new();
    let mut current = node;
    loop {
        match current {
            LogicalPlan::Selection(_) | LogicalPlan::Limit(_) => {
                chain.push(current);
                // Go indexes `lp.Children()[0]` and would panic on a childless
                // selection or limit; `None` is that refusal without the panic.
                current = current.children().first()?;
            }
            LogicalPlan::DataSource(data_source) => {
                if !data_source.common_handle_cols.is_empty() {
                    return None;
                }
                let schema = current.schema()?;
                let handle = data_source.get_pk_is_handle_col(schema)?.clone();
                // Go: "some Projection Operator might be inlined, so check the
                // column again here", at every level that was descended.
                for level in chain.iter().rev() {
                    if !level
                        .schema()
                        .is_some_and(|schema| schema.contains(&handle))
                    {
                        return None;
                    }
                }
                return Some(handle);
            }
            _ => return None,
        }
    }
}

/// Go's `cols` selection, shared by both terminal arms: the handle column
/// alone when one can be extracted, otherwise every output column.
fn ordering_columns(node: &LogicalPlan, handle_source: &LogicalPlan) -> Vec<Column> {
    if let Some(handle) = extract_handle_col(handle_source) {
        return vec![handle];
    }
    node.schema()
        .map(|schema| schema.columns.clone())
        .unwrap_or_default()
}

struct ResultReorderRewrite<'a, 'ctx> {
    ctx: &'a RuleContext<'ctx>,
}

impl OwnedRewrite for ResultReorderRewrite<'_, '_> {
    /// Whether this node is still on the keeper spine Go walks down.
    type Down = bool;
    type Up = ();

    fn descend(&mut self, node: &mut LogicalPlan, on_spine: bool) -> Descend<bool, ()> {
        if !on_spine {
            // Go never looks at anything off the spine.
            return Descend::Stop(());
        }
        let child_count = node.children().len();
        if is_input_order_keeper(node) {
            if child_count == 0 {
                // Go `completeSort`: "if len(lp.Children()) == 0 { return true }"
                // — a childless keeper such as a `LogicalTableDual` is already
                // deterministic.
                return Descend::Stop(());
            }
            let mut downs = vec![false; child_count];
            downs[0] = true;
            return Descend::Children(downs);
        }
        let query_block_offset = node.base().base.query_block_offset();
        match node {
            // Go `completeSort`'s `else if sort, ok := lp.(*LogicalSort)` arm:
            // append every ordering column the sort does not already carry.
            LogicalPlan::Sort(_) => {
                let handle_source = node.children().first().unwrap_or(node);
                let cols = ordering_columns(node, handle_source);
                if let LogicalPlan::Sort(sort) = node {
                    for col in cols {
                        let exists = sort
                            .by_items
                            .iter()
                            .any(|item| col.equal_column(&item.expr));
                        if !exists {
                            sort.by_items.push(ByItems {
                                expr: Expression::Column(col),
                                desc: false,
                            });
                        }
                    }
                }
                Descend::Stop(())
            }
            // Go `injectSort`'s tail: a brand new `LogicalSort` over this node.
            base_arms![
                Join,
                Apply,
                Aggregation,
                TopN,
                UnionAll,
                PartitionUnionAll,
                Window,
                CTE,
                CTETable,
                MaxOneRow,
                Lock,
                Sequence,
                UnionScan,
                TiKVSingleGather,
                TableScan,
                IndexScan,
                DataSource,
                Expand,
                MemTable,
                Show,
                ShowDDLJobs,
                Todo,
                // The four keepers are answered above, before this match; they
                // are listed so the arm set stays exhaustive when a keeper
                // stops being one.
                Selection,
                Projection,
                Limit,
                TableDual,
            ] => {
                let by_items = ordering_columns(node, node)
                    .into_iter()
                    .map(|col| ByItems {
                        expr: Expression::Column(col),
                        desc: false,
                    })
                    .collect();
                let base =
                    BaseLogicalPlan::new(self.ctx.allocator, LogicalSort::TYPE, query_block_offset);
                let placeholder = LogicalPlan::Sort(LogicalSort::new(base.clone(), Vec::new()));
                let child = std::mem::replace(node, placeholder);
                let mut sort = LogicalPlan::Sort(LogicalSort::new(base, by_items));
                sort.set_children(vec![child]);
                *node = sort;
                Descend::Stop(())
            }
        }
    }

    fn ascend(&mut self, node: LogicalPlan, _child_ups: Vec<()>) -> (LogicalPlan, ()) {
        // Only keepers ever reach here, and Go leaves them untouched.
        (node, ())
    }
}

/// Go `(*ResultReorder).Optimize`'s body — `completeSort` followed by
/// `injectSort` — as one [`fold_owned`].
#[must_use]
pub fn result_reorder(ctx: &RuleContext<'_>, plan: LogicalPlan) -> LogicalPlan {
    let mut rewrite = ResultReorderRewrite { ctx };
    fold_owned(&mut rewrite, plan, true).0
}

/// Go `ResultReorder` (`rule_result_reorder.go:41`), Go rule #2.
#[derive(Debug)]
pub struct ResultReorder;

impl LogicalOptRule for ResultReorder {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        // Go hard-codes `planChanged := false`.
        Ok((result_reorder(ctx, plan), false))
    }

    fn name(&self) -> &'static str {
        "result_reorder"
    }
}
