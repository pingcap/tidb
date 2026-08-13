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

//! Go's `findBestTask` over `(logical plan, required property)` pairs, for the
//! JOIN-STRATEGY choice.
//!
//! # Why a cost evaluator was not enough
//!
//! [`crate::candidate_cost`] prices a whole candidate tree and is validated
//! node by node against `EXPLAIN FORMAT='cost_trace'`. Asking it "hash or
//! index at THIS join" still reproduces neither of the two recorded choices
//! the corpus pins, because Go never asks that question. Go asks
//! `findBestTask(join, prop)`, and the required property decides WHICH
//! candidates are enumerated at all before any of them is costed.
//!
//! This module is that layer, and only that layer: the `(prop, candidate,
//! enforcer)` rules of `pkg/planner/core/find_best_task.go` and
//! `exhaust_physical_plans.go` for `LogicalJoin`, with costing delegated to
//! [`crate::candidate_cost`] through [`JoinCostModel`].
//!
//! # The rule table, read off the source
//!
//! For a `LogicalJoin` under a required property `prop` on a root task, with
//! no MPP:
//!
//! | candidate | emitted when | child properties |
//! | --- | --- | --- |
//! | `PhysicalHashJoin` | `prop.SortItems` is EMPTY, and only then -- `getHashJoins` opens with "hash join doesn't promise any orders" and returns nothing otherwise | both empty |
//! | `PhysicalMergeJoin` | some `LeftProperties` entry covers ALL left join keys, the matching right keys are a `RightProperties` prefix, and (`prop` empty, or `prop` is compatible with the left or the right keys and all one direction) | left: the left join keys; right: the right join keys |
//! | enforced `PhysicalMergeJoin` (`Sort` under each side) | a `MERGE_JOIN` hint. NEVER in an unhinted enumeration | both join-key orders, with enforcers enabled |
//! | `PhysicalIndexJoin` / `PhysicalIndexHashJoin` | every `prop` column comes from the OUTER child's schema and `prop` is all one direction; two outer sides for an inner join, one for an outer join; times `TableRangeScan` and index | outer: `prop.SortItems` PRESERVED; inner: empty plus the index-join runtime prop |
//!
//! and the enforcer branch of `findBestTask` runs only when
//! `prop.CanAddEnforcer`, which for a join reached through a parent
//! `PhysicalMergeJoin`'s child property is FALSE -- `tryToGetChildReqProp`
//! builds it with `property.NewPhysicalProperty(..., enforced: false)`. The
//! other trigger, `!hintWorksWithProp`, cannot fire on an unhinted join:
//! `exhaustPhysicalPlans4LogicalJoin` returns `hintCanWork = true` whenever
//! `p.PreferJoinType == 0`.
//!
//! Three consequences are load-bearing, and each is pinned by a test below:
//!
//! * under a non-empty order property a join has NO hash-join candidate, so
//!   the comparison the corpus appeared to demand never happens at that site;
//! * an index join under such a property re-plans its OUTER side under the
//!   SAME property, which is what keeps the parent merge joins alive;
//! * the `Sort`-enforced merge join that would be cheaper on some statements
//!   is unreachable without a hint, so reproducing Go's CHOICE and minimising
//!   Go's COST are different objectives -- fidelity is the objective here.
//!
//! # What this module does NOT own
//!
//! Row counts. Go reads `p.StatsInfo().RowCount` at every node, derived long
//! before costing. Here that stays with the caller, through
//! [`JoinCostModel::attach`], which is handed the children's tasks and returns
//! the [`Candidate`] to price. The estimator is
//! [`crate::cardinality::derive_stats`]; wiring it to a live driver is a
//! separate seam and deliberately absent here rather than approximated.

use crate::candidate_cost::{self, Candidate, CostEnv, CostedNode};
use crate::physical_property::{PhysicalProperty, SortItem, TaskType};
use crate::plan_cost_ver2::IndexJoinKind;
/// The cost model's own task enum, which [`crate::candidate_cost`] reads.
/// Every candidate a join chooser compares sits on the root task.
use crate::task_type::TaskType as CostTaskType;

/// `base.JoinType`, the subset a root-task join enumeration branches on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalJoinType {
    /// `InnerJoin`.
    Inner,
    /// `LeftOuterJoin`.
    LeftOuter,
    /// `RightOuterJoin`.
    RightOuter,
    /// `SemiJoin`.
    Semi,
    /// `AntiSemiJoin`.
    AntiSemi,
    /// `LeftOuterSemiJoin`.
    LeftOuterSemi,
    /// `AntiLeftOuterSemiJoin`.
    AntiLeftOuterSemi,
}

impl LogicalJoinType {
    /// `tryToEnumerateIndexJoin`: which side may be the OUTER one.
    ///
    /// An index join reads its inner side once per outer row, so a preserved
    /// side can never be the inner one.
    #[must_use]
    pub const fn index_join_outer_sides(self) -> &'static [usize] {
        match self {
            Self::Inner => &[0, 1],
            Self::Semi
            | Self::AntiSemi
            | Self::LeftOuterSemi
            | Self::AntiLeftOuterSemi
            | Self::LeftOuter => &[0],
            Self::RightOuter => &[1],
        }
    }
}

/// One `(inner_idx, use_outer_to_build)` hash-join shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HashJoinShape {
    /// `BasePhysicalJoin.InnerChildIdx`.
    pub inner_idx: usize,
    /// `PhysicalHashJoin.UseOuterToBuild`.
    pub use_outer_to_build: bool,
}

/// The physical alternatives one enumerated candidate stands for.
#[derive(Clone, Debug, PartialEq)]
pub enum JoinStrategy {
    /// `getHashJoin(ge, p, prop, inner_idx, use_outer_to_build)`.
    Hash(HashJoinShape),
    /// One `PhysicalMergeJoin` from one `LeftProperties` entry.
    Merge {
        /// The left keys this candidate joins on, in the child's order.
        left_keys: Vec<i64>,
        /// The right keys, in the matching order.
        right_keys: Vec<i64>,
        /// `PhysicalMergeJoin.Desc`, which is `prop.AllSameOrder()`'s answer.
        desc: bool,
    },
    /// `constructIndexJoinStatic` / `constructIndexHashJoinStatic`.
    Index {
        /// Which child drives, and is read once.
        outer_idx: usize,
        /// `IndexJoinRuntimeProp.TableRangeScan`: the clustered handle rather
        /// than a secondary index.
        table_range_scan: bool,
        /// Which executor, which decides the cost formula's build term.
        kind: IndexJoinKind,
        /// `PhysicalIndexHashJoin.KeepOuterOrder`: `!prop.IsSortItemEmpty()`.
        keep_outer_order: bool,
    },
}

/// One enumerated physical candidate together with the properties it demands
/// of its children -- Go's `pp` plus `pp.GetChildReqProps(j)`.
#[derive(Clone, Debug, PartialEq)]
pub struct EnumeratedJoin {
    /// What the candidate is.
    pub strategy: JoinStrategy,
    /// `GetChildReqProps(0)` and `GetChildReqProps(1)`.
    pub child_props: [PhysicalProperty; 2],
    /// `PhysicalProperty.IndexJoinProp` on each child, which this port carries
    /// beside the property rather than inside it -- see [`LeafRole`].
    pub child_roles: [LeafRole; 2],
}

/// A `LogicalJoin` reduced to what the enumeration reads about it.
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalJoin {
    /// `p.JoinType`.
    pub join_type: LogicalJoinType,
    /// The left child.
    pub left: Box<LogicalNode>,
    /// The right child.
    pub right: Box<LogicalNode>,
    /// `p.GetJoinKeys()`'s left half, by `UniqueID`.
    pub left_keys: Vec<i64>,
    /// `p.GetJoinKeys()`'s right half, positionally paired with `left_keys`.
    pub right_keys: Vec<i64>,
    /// The left child's output columns, by `UniqueID`.
    pub left_schema: Vec<i64>,
    /// The right child's output columns, by `UniqueID`.
    pub right_schema: Vec<i64>,
    /// `p.LeftProperties`: every column order the LEFT child can provide.
    pub left_properties: Vec<Vec<i64>>,
    /// `p.RightProperties`.
    pub right_properties: Vec<Vec<i64>>,
    /// `p.PreferJoinType&PreferMergeJoin > 0`.
    ///
    /// A forced merge join differs from an ordinary merge candidate in one
    /// load-bearing way: each child property permits a Sort enforcer. That is
    /// how `getEnforcedMergeJoin` remains buildable when neither access path
    /// already provides the join-key order.
    pub force_merge: bool,
}

/// A node of the logical tree the search runs over.
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalNode {
    /// A subtree the caller has already physicalised into a fixed set of
    /// alternatives, each with the order it provides -- Go's `DataSource`
    /// access paths, reduced to what the join search reads.
    Leaf(Vec<LeafAlternative>),
    /// A join, whose candidates this module enumerates.
    Join(Box<LogicalJoin>),
}

/// One physical alternative for a leaf subtree.
#[derive(Clone, Debug, PartialEq)]
pub struct LeafAlternative {
    /// The plan to price.
    pub plan: Candidate,
    /// The order its rows come out in, outermost first. An unordered read is
    /// the empty order, which satisfies only the empty property.
    pub order: Vec<SortItem>,
    /// Which caller may read this alternative.
    pub role: LeafRole,
}

/// Which parent an access path answers to.
///
/// Go carries this as `PhysicalProperty.IndexJoinProp`: a non-nil one makes
/// `DataSource.findBestTask` answer with
/// `buildDataSource2IndexScanByIndexJoinProp` /
/// `buildDataSource2TableScanByIndexJoinProp` -- a RANGE scan built from the
/// outer join keys -- instead of an ordinary access path. The two answers are
/// different plans with different row counts, so they are different
/// alternatives here rather than one plan read two ways.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeafRole {
    /// An ordinary access path, which any parent may read.
    Plain,
    /// The INNER side of an index join, costed for ONE outer row.
    IndexJoinProbe {
        /// `IndexJoinRuntimeProp.TableRangeScan`: the clustered handle rather
        /// than a secondary index.
        table_range_scan: bool,
    },
}

/// A physical task: a plan, the order it provides, and its cost.
///
/// `base.Task` reduced to the three fields the join search reads.
#[derive(Clone, Debug, PartialEq)]
pub struct Task {
    /// The plan.
    pub plan: Candidate,
    /// The order the plan's rows come out in.
    pub order: Vec<SortItem>,
    /// `GetPlanCostVer2` over `plan`, per node.
    pub costed: CostedNode,
}

impl Task {
    /// `getTaskPlanCost`.
    #[must_use]
    pub fn cost(&self) -> f64 {
        self.costed.est_cost()
    }
}

/// How a caller turns an enumerated strategy into a plan to price.
///
/// This is the seam Go does not have, because Go's physical operators carry
/// their own stats. Everything the formulas read that is NOT a property --
/// row counts, row sizes, filter lists -- arrives through here.
pub trait JoinCostModel {
    /// `pp.Attach2Task(childTasks...)`: the candidate plan, or `None` for Go's
    /// invalid task.
    fn attach(
        &self,
        join: &LogicalJoin,
        strategy: &JoinStrategy,
        children: [&Task; 2],
    ) -> Option<Candidate>;

    /// `EnforceProperty`: the `Sort` that makes `task` satisfy `prop`.
    ///
    /// `None` refuses the enforced branch, which is what a caller with no row
    /// estimate for the sort must answer rather than guess one.
    fn enforce(&self, prop: &PhysicalProperty, task: &Task) -> Option<Candidate>;
}

/// `util.GetMaxSortPrefix(sort_cols, all_cols)`: each sort column's offset in
/// `all_cols`, stopping at the first one that is not there.
#[must_use]
pub fn max_sort_prefix(sort_cols: &[i64], all_cols: &[i64]) -> Vec<usize> {
    let mut offsets = Vec::with_capacity(sort_cols.len());
    for col in sort_cols {
        let Some(offset) = all_cols.iter().position(|candidate| candidate == col) else {
            return offsets;
        };
        offsets.push(offset);
    }
    offsets
}

/// `findMaxPrefixLen(candidates, keys)`: the longest prefix of `keys` any
/// candidate order starts with.
#[must_use]
pub fn max_prefix_len(candidates: &[Vec<i64>], keys: &[i64]) -> usize {
    candidates
        .iter()
        .map(|candidate| {
            keys.iter()
                .zip(candidate)
                .take_while(|(key, col)| key == col)
                .count()
        })
        .max()
        .unwrap_or(0)
}

/// `isSortPropCompatibleWithJoinKeys`, with no constant columns.
///
/// The `constantCols` set comes from the join's functional dependencies, which
/// this tier does not derive; leaving it empty makes the check STRICTER than
/// Go's, so a property Go would accept may be refused here. NAMED RESIDUE.
#[must_use]
pub fn sort_prop_compatible_with_join_keys(sort_items: &[SortItem], join_keys: &[i64]) -> bool {
    // With no constant column to skip over, Go's walk advances one key per
    // sort item and fails on the first mismatch -- which is exactly "the sort
    // items are a prefix of the join keys".
    sort_items.len() <= join_keys.len()
        && sort_items
            .iter()
            .zip(join_keys)
            .all(|(item, key)| item.col == *key)
}

/// The child properties `PhysicalMergeJoin.tryToGetChildReqProp` builds, or
/// `None` when the merge join cannot satisfy `prop` at all.
fn merge_join_child_props(
    join_type: LogicalJoinType,
    left_keys: &[i64],
    right_keys: &[i64],
    prop: &PhysicalProperty,
) -> Option<[PhysicalProperty; 2]> {
    let (all, desc) = prop.all_same_order();
    if !prop.is_sort_item_empty() {
        if !all {
            return None;
        }
        let match_left = sort_prop_compatible_with_join_keys(&prop.sort_items, left_keys);
        let match_right = sort_prop_compatible_with_join_keys(&prop.sort_items, right_keys);
        if !match_left && !match_right {
            return None;
        }
        if match_right && join_type == LogicalJoinType::LeftOuter {
            return None;
        }
        if match_left && join_type == LogicalJoinType::RightOuter {
            return None;
        }
    }
    Some([
        PhysicalProperty::new(TaskType::Root, left_keys, desc, f64::MAX, false),
        PhysicalProperty::new(TaskType::Root, right_keys, desc, f64::MAX, false),
    ])
}

/// `getHashJoins`'s per-join-type shapes, with no build/probe hints.
fn hash_join_shapes(join_type: LogicalJoinType) -> Vec<HashJoinShape> {
    let shape = |inner_idx, use_outer_to_build| HashJoinShape {
        inner_idx,
        use_outer_to_build,
    };
    match join_type {
        // Hash join v1 for a semi join builds the right side only.
        LogicalJoinType::Semi
        | LogicalJoinType::AntiSemi
        | LogicalJoinType::LeftOuterSemi
        | LogicalJoinType::AntiLeftOuterSemi => vec![shape(1, false)],
        LogicalJoinType::LeftOuter => vec![shape(1, false), shape(1, true)],
        LogicalJoinType::RightOuter => vec![shape(0, true), shape(0, false)],
        LogicalJoinType::Inner => vec![shape(1, false), shape(0, false)],
    }
}

/// `exhaustPhysicalPlans4LogicalJoin` for a root task with no hints: every
/// physical candidate this join may become UNDER `prop`, in Go's enumeration
/// order -- merge joins, then index joins, then hash joins.
///
/// The order is not cosmetic: `compareTaskCost` replaces the incumbent only on
/// a strict `<`, so an exact tie is broken by whichever candidate Go reached
/// first.
#[must_use]
pub fn exhaust_join(join: &LogicalJoin, prop: &PhysicalProperty) -> Vec<EnumeratedJoin> {
    let mut out = Vec::new();
    out.extend(merge_join_candidates(join, prop));
    if join.force_merge {
        out.extend(enforced_merge_join_candidates(join, prop));
        if !out.is_empty() {
            return out;
        }
    }
    out.extend(index_join_candidates(join, prop));
    out.extend(hash_join_candidates(join, prop));
    out
}

/// `getEnforcedMergeJoin`: reorder the join keys so a required output order
/// is their prefix, then let both children add Sort enforcers.
fn enforced_merge_join_candidates(
    join: &LogicalJoin,
    prop: &PhysicalProperty,
) -> Vec<EnumeratedJoin> {
    if join.left_keys.is_empty() || join.left_keys.len() != join.right_keys.len() {
        return Vec::new();
    }
    let (all, desc) = prop.all_same_order();
    if !all {
        return Vec::new();
    }

    let mut offsets = Vec::with_capacity(join.left_keys.len());
    for item in &prop.sort_items {
        let left_at = join.left_keys.iter().position(|key| *key == item.col);
        let right_at = join.right_keys.iter().position(|key| *key == item.col);
        let Some(at) = left_at.or(right_at) else {
            return Vec::new();
        };
        if join.join_type == LogicalJoinType::LeftOuter && right_at.is_some() {
            return Vec::new();
        }
        if join.join_type == LogicalJoinType::RightOuter && left_at.is_some() {
            return Vec::new();
        }
        if !offsets.contains(&at) {
            offsets.push(at);
        }
    }
    for at in 0..join.left_keys.len() {
        if !offsets.contains(&at) {
            offsets.push(at);
        }
    }
    let left_keys: Vec<i64> = offsets.iter().map(|at| join.left_keys[*at]).collect();
    let right_keys: Vec<i64> = offsets.iter().map(|at| join.right_keys[*at]).collect();
    let child_prop = |keys: &[i64]| PhysicalProperty {
        sort_items: keys
            .iter()
            .map(|col| SortItem { col: *col, desc })
            .collect(),
        task_tp: TaskType::Root,
        expected_cnt: f64::MAX,
        can_add_enforcer: true,
    };
    vec![EnumeratedJoin {
        strategy: JoinStrategy::Merge {
            left_keys: left_keys.clone(),
            right_keys: right_keys.clone(),
            desc,
        },
        child_props: [child_prop(&left_keys), child_prop(&right_keys)],
        child_roles: [LeafRole::Plain, LeafRole::Plain],
    }]
}

/// `physicalop.GetMergeJoin` without the enforced branch, which
/// `getEnforcedMergeJoin` reaches only under a `MERGE_JOIN` hint or with hash
/// join disabled.
fn merge_join_candidates(join: &LogicalJoin, prop: &PhysicalProperty) -> Vec<EnumeratedJoin> {
    let mut out = Vec::new();
    for lhs_property in &join.left_properties {
        let offsets = max_sort_prefix(lhs_property, &join.left_keys);
        if offsets.len() < join.left_keys.len() || join.left_keys.is_empty() {
            continue;
        }
        let left_keys: Vec<i64> = lhs_property[..offsets.len()].to_vec();
        let right_keys: Vec<i64> = offsets.iter().map(|at| join.right_keys[*at]).collect();
        let prefix_len = max_prefix_len(&join.right_properties, &right_keys);
        if prefix_len < offsets.len() || prefix_len == 0 {
            continue;
        }
        let left_keys = left_keys[..prefix_len].to_vec();
        let right_keys = right_keys[..prefix_len].to_vec();
        let Some(child_props) =
            merge_join_child_props(join.join_type, &left_keys, &right_keys, prop)
        else {
            continue;
        };
        let (_, desc) = prop.all_same_order();
        out.push(EnumeratedJoin {
            strategy: JoinStrategy::Merge {
                left_keys,
                right_keys,
                desc,
            },
            child_props,
            child_roles: [LeafRole::Plain, LeafRole::Plain],
        });
    }
    out
}

/// `tryToEnumerateIndexJoin` -> `enumerateIndexJoinByOuterIdx`.
fn index_join_candidates(join: &LogicalJoin, prop: &PhysicalProperty) -> Vec<EnumeratedJoin> {
    let mut out = Vec::new();
    let (all, _) = prop.all_same_order();
    if !all {
        return out;
    }
    for outer_idx in join.join_type.index_join_outer_sides().iter().copied() {
        let outer_schema = if outer_idx == 0 {
            &join.left_schema
        } else {
            &join.right_schema
        };
        // `prop.AllColsFromSchema(outerSchema)`: an index join cannot promise
        // an order over a column the inner side owns.
        if !prop
            .sort_items
            .iter()
            .all(|item| outer_schema.contains(&item.col))
        {
            continue;
        }
        let mut child_props = [PhysicalProperty::default(), PhysicalProperty::default()];
        // The OUTER side is re-planned under the SAME property. This is the
        // line that keeps a parent merge join alive above an index join.
        child_props[outer_idx] = PhysicalProperty {
            sort_items: prop.sort_items.clone(),
            task_tp: TaskType::Root,
            expected_cnt: prop.expected_cnt,
            can_add_enforcer: false,
        };
        // The inner side is planned under an empty property plus the index-join
        // runtime prop, which this port carries as the strategy's own
        // `table_range_scan` flag rather than as a property field.
        for table_range_scan in [true, false] {
            for kind in [IndexJoinKind::IndexJoin, IndexJoinKind::IndexHashJoin] {
                let mut child_roles = [LeafRole::Plain, LeafRole::Plain];
                child_roles[1 - outer_idx] = LeafRole::IndexJoinProbe { table_range_scan };
                out.push(EnumeratedJoin {
                    strategy: JoinStrategy::Index {
                        outer_idx,
                        table_range_scan,
                        kind,
                        keep_outer_order: !prop.is_sort_item_empty(),
                    },
                    child_props: child_props.clone(),
                    child_roles,
                });
            }
        }
    }
    // Go emits both `IndexJoin` variants before both `IndexHashJoin` variants;
    // reorder to match, since the enumeration order breaks exact ties.
    out.sort_by_key(|candidate| match &candidate.strategy {
        JoinStrategy::Index {
            kind: IndexJoinKind::IndexHashJoin,
            ..
        } => 1,
        _ => 0,
    });
    out
}

/// `getHashJoins`, whose first line is the whole rule: "hash join doesn't
/// promise any orders".
fn hash_join_candidates(join: &LogicalJoin, prop: &PhysicalProperty) -> Vec<EnumeratedJoin> {
    if !prop.is_sort_item_empty() {
        return Vec::new();
    }
    let child_prop = || PhysicalProperty {
        sort_items: Vec::new(),
        task_tp: TaskType::Root,
        expected_cnt: f64::MAX,
        can_add_enforcer: false,
    };
    hash_join_shapes(join.join_type)
        .into_iter()
        .map(|shape| EnumeratedJoin {
            strategy: JoinStrategy::Hash(shape),
            child_props: [child_prop(), child_prop()],
            child_roles: [LeafRole::Plain, LeafRole::Plain],
        })
        .collect()
}

/// Whether a task providing `order` satisfies `prop`.
///
/// Go answers this by construction -- a child is planned UNDER the property --
/// so this is the leaf-side check only, where the caller declares what each
/// alternative provides.
#[must_use]
pub fn order_satisfies(order: &[SortItem], prop: &PhysicalProperty) -> bool {
    prop.sort_items.len() <= order.len()
        && prop
            .sort_items
            .iter()
            .zip(order)
            .all(|(required, provided)| required == provided)
}

/// `findBestTask(lp, prop)`: the cheapest task for this subtree that satisfies
/// `prop`, or `None` for Go's invalid task.
#[must_use]
pub fn find_best_task(
    node: &LogicalNode,
    prop: &PhysicalProperty,
    role: LeafRole,
    model: &dyn JoinCostModel,
    env: &CostEnv,
) -> Option<Task> {
    match node {
        LogicalNode::Leaf(alternatives) => best_leaf(alternatives, prop, role, model, env),
        LogicalNode::Join(join) => {
            // Go's `admitIndexJoinInnerChildPattern` decides which operators
            // may sit under an index join's runtime property. A JOIN is one of
            // them in Go, which pushes the property on to one of ITS children;
            // no recorded plan this tier reaches has that shape, so it is
            // refused rather than half-implemented. NAMED RESIDUE.
            if role != LeafRole::Plain {
                return None;
            }
            best_join(join, prop, model, env)
        }
    }
}

fn best_leaf(
    alternatives: &[LeafAlternative],
    prop: &PhysicalProperty,
    role: LeafRole,
    model: &dyn JoinCostModel,
    env: &CostEnv,
) -> Option<Task> {
    let mut best: Option<Task> = None;
    for alternative in alternatives {
        if alternative.role != role || !order_satisfies(&alternative.order, prop) {
            continue;
        }
        let costed = candidate_cost::evaluate(&alternative.plan, env, CostTaskType::Root);
        let task = Task {
            plan: alternative.plan.clone(),
            order: alternative.order.clone(),
            costed,
        };
        keep_cheaper(&mut best, task);
    }
    if prop.can_add_enforcer {
        // Go's `DataSource.findBestTask` clears the sort items, finds the best
        // unordered path and enforces the order on top of it.
        let mut unordered = prop.clone();
        unordered.sort_items = Vec::new();
        unordered.can_add_enforcer = false;
        unordered.expected_cnt = f64::MAX;
        if let Some(task) = best_leaf(alternatives, &unordered, role, model, env) {
            if let Some(enforced) = enforce(model, prop, &task, env) {
                keep_cheaper(&mut best, enforced);
            }
        }
    }
    best
}

fn best_join(
    join: &LogicalJoin,
    prop: &PhysicalProperty,
    model: &dyn JoinCostModel,
    env: &CostEnv,
) -> Option<Task> {
    let candidates = exhaust_join(join, prop);
    if join.force_merge
        && !prop.is_sort_item_empty()
        && !candidates
            .iter()
            .any(|candidate| matches!(candidate.strategy, JoinStrategy::Merge { .. }))
    {
        // `hintWorksWithProp == false`: Go retries the hint under an empty
        // property, then adds one Sort above the hinted join. If that retry
        // works, the non-hinted candidates from the original property are
        // discarded rather than allowed to defeat the hint on cost.
        let mut empty = prop.clone();
        empty.sort_items.clear();
        empty.expected_cnt = f64::MAX;
        let under_empty = exhaust_join(join, &empty);
        if under_empty
            .iter()
            .any(|candidate| matches!(candidate.strategy, JoinStrategy::Merge { .. }))
        {
            return enumerate(join, &under_empty, prop, true, model, env);
        }
    }

    let mut best = enumerate(join, &candidates, prop, false, model, env);
    if prop.can_add_enforcer {
        // `findBestTask`'s enforced branch: exhaust under the EMPTY property,
        // then put the enforcer on each resulting task.
        let mut empty = prop.clone();
        empty.sort_items = Vec::new();
        empty.expected_cnt = f64::MAX;
        let enforced = enumerate(join, &exhaust_join(join, &empty), prop, true, model, env);
        if let Some(task) = enforced {
            keep_cheaper(&mut best, task);
        }
    }
    best
}

fn enumerate(
    join: &LogicalJoin,
    candidates: &[EnumeratedJoin],
    prop: &PhysicalProperty,
    add_enforcer: bool,
    model: &dyn JoinCostModel,
    env: &CostEnv,
) -> Option<Task> {
    let mut best: Option<Task> = None;
    for candidate in candidates {
        let Some(left) = find_best_task(
            &join.left,
            &candidate.child_props[0],
            candidate.child_roles[0],
            model,
            env,
        ) else {
            continue;
        };
        let Some(right) = find_best_task(
            &join.right,
            &candidate.child_props[1],
            candidate.child_roles[1],
            model,
            env,
        ) else {
            continue;
        };
        let Some(plan) = model.attach(join, &candidate.strategy, [&left, &right]) else {
            continue;
        };
        let order = provided_order(&candidate.strategy, [&left, &right]);
        let costed = candidate_cost::evaluate(&plan, env, CostTaskType::Root);
        let task = Task {
            plan,
            order,
            costed,
        };
        let task = if add_enforcer {
            match enforce(model, prop, &task, env) {
                Some(enforced) => enforced,
                None => continue,
            }
        } else {
            task
        };
        keep_cheaper(&mut best, task);
    }
    best
}

/// `EnforceProperty`: a `Sort` on top, which then provides exactly `prop`.
fn enforce(
    model: &dyn JoinCostModel,
    prop: &PhysicalProperty,
    task: &Task,
    env: &CostEnv,
) -> Option<Task> {
    let plan = model.enforce(prop, task)?;
    let costed = candidate_cost::evaluate(&plan, env, CostTaskType::Root);
    Some(Task {
        plan,
        order: prop.sort_items.clone(),
        costed,
    })
}

/// The order a strategy's output comes out in.
fn provided_order(strategy: &JoinStrategy, children: [&Task; 2]) -> Vec<SortItem> {
    match strategy {
        // "hash join doesn't promise any orders".
        JoinStrategy::Hash(_) => Vec::new(),
        // A merge join emits its left child's order, which its child property
        // already fixed to the left join keys.
        JoinStrategy::Merge { .. } => children[0].order.clone(),
        // An index join emits the OUTER child's order, unchanged: the inner
        // side is read once per outer row and never reorders it.
        JoinStrategy::Index { outer_idx, .. } => children[*outer_idx].order.clone(),
    }
}

/// `compareTaskCost`'s strict `<`: an exactly equal alternative never
/// displaces the incumbent, so enumeration order breaks the tie.
fn keep_cheaper(best: &mut Option<Task>, task: Task) {
    match best {
        Some(incumbent) if !candidate_cost::prefer(&task.costed, &incumbent.costed) => {}
        _ => *best = Some(task),
    }
}

#[cfg(test)]
mod tests;
