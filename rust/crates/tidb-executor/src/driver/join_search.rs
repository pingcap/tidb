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

//! WHICH JOIN STRATEGY THIS SITE MAY USE, asked of
//! [`tidb_planner::find_best_task`] rather than of a structural rule.
//!
//! # What this wires, and to what
//!
//! [`tidb_planner::find_best_task::exhaust_join`] is
//! `exhaustPhysicalPlans4LogicalJoin` for a root task: given a `LogicalJoin`
//! and the property its parent requires, it returns the physical candidates Go
//! ENUMERATES there. This module builds that `LogicalJoin` out of what the
//! driver already knows at `build_join` -- the equality keys
//! ([`crate::hash_join::split_equi`]), the two sides' widths, and the orders
//! each side can provide ([`crate::driver::merge_decision`]'s
//! `preparePossibleProperties`) -- and reads the answer.
//!
//! Column identity is the JOINED ROW OFFSET, which is the same identity
//! [`crate::driver::merge_decision::child_required_prop`] writes into the
//! property this site is asked for, so the two are one numbering rather than
//! two that can drift.
//!
//! # The rule this landing takes from the enumeration, and the one it refuses
//!
//! `getHashJoins` opens with "hash join doesn't promise any orders" and
//! returns NOTHING under a non-empty `prop.SortItems`; `GetMergeJoin` needs a
//! `LeftProperties` entry covering every left join key, which no index
//! provides for a projected expression. A join under a parent merge join whose
//! key is such an expression therefore has exactly ONE family of candidates
//! left, and no cost can change which family wins:
//!
//! * INDEX BY ELIMINATION -- taken here.
//! * INDEX FOR A SINGLE OUTER ROW -- taken when the estimate says one outer
//!   row probes strictly fewer rows than reading the inner side whole. This is
//!   the one cost choice the available row source can settle without pricing
//!   a candidate tree.
//! * anything else -- REFUSED here, and refused fail-closed. Choosing between
//!   two families is `findBestTask`'s costing layer, which needs a priced
//!   `Candidate` tree per side ([`tidb_planner::candidate_cost`]); this tier
//!   builds executors bottom-up and has no physical-plan IR to price. NAMED
//!   RESIDUE, and the one it hides is "an index join Go picks ON COST under an
//!   EMPTY property". The measurement in [`super::index_join_decision`] says
//!   what refusing it costs.
//!
//! # Why the rows are read at all
//!
//! A candidate the search cannot PRICE is a candidate it cannot choose, so a
//! site whose row counts this tier cannot derive is refused here rather than
//! decided by the structural rule underneath. The rows come from
//! [`crate::driver::join_reorder::RowSource`] -- `derive_stats` over the
//! statement, the catalog and the statistics -- and NOT from
//! [`crate::plan_trace::PlanTrace`], which exists only under `EXPLAIN`. That
//! is the whole reason the source was built: a chooser reading the trace would
//! make `EXPLAIN` print a strategy the bare statement does not run. See
//! `crate::tests_join_search`.

use tidb_ast::{Join, JoinNode};
use tidb_planner::find_best_task::{
    exhaust_join, JoinStrategy, LogicalJoin, LogicalJoinType, LogicalNode,
};
use tidb_planner::physical_property::PhysicalProperty;

use crate::driver::join_reorder::RowSource;
use crate::hash_join::EquiKey;

/// What the enumeration leaves standing at one join site.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Chosen {
    /// Index joins are the only enumerated candidates.
    Index,
    /// A one-row outer side can probe a strict subset of the inner side. The
    /// caller still verifies that the physical object is a non-primary
    /// secondary index before committing this cost-shaped choice.
    IndexForSingleOuterRow,
    /// Refused, and why. See [`Refusal`].
    Refused(Refusal),
}

/// Why a site was refused, one variant per MEASURED population.
///
/// Over the 106-topic replay every site this tier reaches falls in one of
/// these, and `Index` is reached by NONE of them -- see the census in the
/// module doc.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Refusal {
    /// A cross join: no equality to probe an index with.
    NoEquiKeys,
    /// This `FROM`'s shape has no estimate owner
    /// ([`crate::driver::join_reorder::row_source`] declined it).
    NoRowSource,
    /// A side exposes a relation under no name.
    NoRelationNames,
    /// The estimate owner does not hold these relations.
    RowsDeclined,
    /// A side whose provided orders
    /// [`crate::driver::merge_decision::possible_properties`] declined, which
    /// would understate the merge-join candidates.
    NoChildOrders,
    /// The property this site was asked for is EMPTY, so `getHashJoins`
    /// answers too and the choice needs the costing layer this tier refuses.
    HashAlsoEnumerated,
    /// The property is non-empty -- no hash join -- but a merge join is still
    /// a candidate, so the choice again needs the costing layer.
    MergeAlsoEnumerated,
    /// No index-join candidate: `prop` reads a column the inner side owns.
    NoIndexCandidate,
}

/// Go's `p.LeftProperties` / `p.RightProperties`: the column orders each
/// child's output already carries, in that child's OWN row offsets.
pub(crate) type ChildOrders<'a> = (&'a [Vec<usize>], &'a [Vec<usize>]);

/// Everything one join site hands the search.
pub(crate) struct SearchInput<'a> {
    /// The `FROM` node being built.
    pub(crate) join: &'a Join,
    /// `base.JoinType`.
    pub(crate) join_type: LogicalJoinType,
    /// The equality conjuncts, as [`crate::hash_join::split_equi`] split them.
    pub(crate) keys: &'a [EquiKey],
    /// Where the right child's columns start in the joined row.
    pub(crate) left_width: usize,
    /// The joined row's width.
    pub(crate) width: usize,
    /// The orders each child's output already carries, in its OWN row
    /// offsets -- Go's `p.LeftProperties` / `p.RightProperties`. `None` is a
    /// side [`crate::driver::merge_decision::possible_properties`] declined,
    /// which would understate the merge-join candidates and so is refused.
    pub(crate) orders: Option<ChildOrders<'a>>,
    /// The property this join's parent requires of it.
    pub(crate) required: &'a PhysicalProperty,
    /// The estimate owner, or `None` when this `FROM` has none.
    pub(crate) rows: Option<&'a RowSource>,
}

// Every site this chooser answered for, in the order it was asked. The
// recorder is the ONLY way to observe the chooser's answer for a statement
// that is not being explained, which is exactly what
// `crate::tests_join_search` has to compare against the explained one.
#[cfg(test)]
thread_local! {
    /// See above.
    pub(crate) static ANSWERS: std::cell::RefCell<Vec<Answer>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// One recorded answer.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Answer {
    /// The relations the left side reads.
    pub(crate) left: Vec<String>,
    /// The relations the right side reads.
    pub(crate) right: Vec<String>,
    /// What the estimate owner said, when it was reached.
    pub(crate) rows: Option<crate::driver::join_reorder::JoinRows>,
    /// Whether the property this site was asked for carries an order.
    pub(crate) ordered: bool,
    /// The answer.
    pub(crate) chosen: Chosen,
}

/// Asks [`exhaust_join`] which strategies this site may use.
pub(crate) fn choose(input: &SearchInput<'_>) -> Chosen {
    #[cfg(test)]
    let mut record = Answer {
        left: Vec::new(),
        right: Vec::new(),
        rows: None,
        ordered: !input.required.is_sort_item_empty(),
        chosen: Chosen::Refused(Refusal::NoEquiKeys),
    };
    #[cfg(not(test))]
    let mut record = ();
    let chosen = decide(input, &mut record);
    #[cfg(test)]
    {
        record.chosen = chosen;
        ANSWERS.with(|answers| answers.borrow_mut().push(record));
    }
    chosen
}

#[cfg(test)]
type Record = Answer;
#[cfg(not(test))]
type Record = ();

fn decide(input: &SearchInput<'_>, record: &mut Record) -> Chosen {
    let _ = &record;
    if input.keys.is_empty() {
        return Chosen::Refused(Refusal::NoEquiKeys);
    }
    // A site the estimator cannot answer for is a site the search cannot
    // price. Fail closed rather than decide it structurally.
    let Some(rows) = input.rows else {
        return Chosen::Refused(Refusal::NoRowSource);
    };
    let Some((left_names, right_names)) = side_names(input.join) else {
        return Chosen::Refused(Refusal::NoRelationNames);
    };
    let Some(counts) = rows.rows_of_join(&left_names, &right_names) else {
        return Chosen::Refused(Refusal::RowsDeclined);
    };
    #[cfg(test)]
    {
        record.left = left_names;
        record.right = right_names;
        record.rows = Some(counts);
    }
    #[cfg(not(test))]
    let _ = counts;
    let Some(orders) = input.orders else {
        return Chosen::Refused(Refusal::NoChildOrders);
    };
    let candidates = exhaust_join(&logical_join(input, orders), input.required);
    let mut index = false;
    let mut hash = false;
    let mut merge = false;
    let mut single_outer_row_dominates = false;
    for candidate in &candidates {
        match candidate.strategy {
            JoinStrategy::Index { outer_idx, .. } => {
                index = true;
                let (outer_rows, inner_rows) = if outer_idx == 0 {
                    (counts.left, counts.right)
                } else {
                    (counts.right, counts.left)
                };
                // With exactly one driver row, the index alternative performs
                // one lookup. When that lookup is expected to return fewer
                // rows than the inner side contains, it avoids a strict
                // subset of the whole-side read required by hash and merge.
                // This is deliberately the only cost-shaped comparison made
                // without a complete candidate tree.
                single_outer_row_dominates |= outer_rows == 1.0 && counts.joined < inner_rows;
            }
            JoinStrategy::Hash(_) => hash = true,
            JoinStrategy::Merge { .. } => merge = true,
        }
    }
    match (index, hash, merge) {
        (true, false, false) => Chosen::Index,
        (true, _, _) if single_outer_row_dominates => Chosen::IndexForSingleOuterRow,
        (true, true, _) => Chosen::Refused(Refusal::HashAlsoEnumerated),
        (true, false, true) => Chosen::Refused(Refusal::MergeAlsoEnumerated),
        (false, ..) => Chosen::Refused(Refusal::NoIndexCandidate),
    }
}

/// The `LogicalJoin` the enumeration reads, in joined-row-offset identity.
///
/// The two children are `Leaf`s with no alternatives: `exhaust_join` never
/// descends, so what a child could be planned as is not its input. Only
/// [`tidb_planner::find_best_task::find_best_task`] reads the alternatives,
/// and reaching it is this module's named residue.
fn logical_join(input: &SearchInput<'_>, orders: (&[Vec<usize>], &[Vec<usize>])) -> LogicalJoin {
    let shift = input.left_width as i64;
    LogicalJoin {
        join_type: input.join_type,
        left: Box::new(LogicalNode::Leaf(Vec::new())),
        right: Box::new(LogicalNode::Leaf(Vec::new())),
        left_keys: input.keys.iter().map(|key| key.left as i64).collect(),
        right_keys: input
            .keys
            .iter()
            .map(|key| key.right as i64 + shift)
            .collect(),
        left_schema: (0..shift).collect(),
        right_schema: (shift..input.width as i64).collect(),
        left_properties: orders
            .0
            .iter()
            .map(|order| order.iter().map(|at| *at as i64).collect())
            .collect(),
        right_properties: orders
            .1
            .iter()
            .map(|order| order.iter().map(|at| *at as i64 + shift).collect())
            .collect(),
    }
}

/// The relations each side of `join` reads, by the name a column reference
/// reaches them under -- the key [`RowSource`] answers to.
fn side_names(join: &Join) -> Option<(Vec<String>, Vec<String>)> {
    let mut left = Vec::new();
    let mut right = Vec::new();
    leaf_names(&join.left, &mut left)?;
    leaf_names(join.right.as_ref()?, &mut right)?;
    (!left.is_empty() && !right.is_empty()).then_some((left, right))
}

/// Returns the statement-owned estimates for one join in left/right order.
///
/// This is the same answer [`choose`] records and lets the committed index
/// strategy carry its outer/probe cardinalities into the physical-plan cost
/// comparison without consulting EXPLAIN-only state.
pub(crate) fn estimated_rows(
    join: &Join,
    rows: Option<&RowSource>,
) -> Option<crate::driver::join_reorder::JoinRows> {
    let (left, right) = side_names(join)?;
    rows?.rows_of_join(&left, &right)
}

/// Every relation a `FROM` subtree exposes, in row order.
fn leaf_names(node: &JoinNode, out: &mut Vec<String>) -> Option<()> {
    match node {
        JoinNode::Table(table_ref) => {
            let name = table_ref
                .alias
                .clone()
                .or_else(|| table_ref.name.last().cloned())?;
            out.push(name);
            Some(())
        }
        JoinNode::Derived { alias, .. } => {
            out.push(alias.clone().filter(|alias| !alias.is_empty())?);
            Some(())
        }
        JoinNode::Join(inner) => {
            leaf_names(&inner.left, out)?;
            match &inner.right {
                Some(right) => leaf_names(right, out),
                None => Some(()),
            }
        }
    }
}
