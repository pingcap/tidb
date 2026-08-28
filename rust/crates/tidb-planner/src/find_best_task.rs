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

//! Go's physical join enumeration under a required property.
//!
//! The production recursive search is [`dispatch::find_best_task`], which
//! operates directly on the shared [`LogicalPlan`] and [`PhysicalPlan`](crate::physical::PhysicalPlan)
//! trees. This parent module retains only the join candidate/property rules
//! that dispatch consumes. The former reduced candidate tree, duplicate
//! recursive cost search, and executor-facing decision tree were removed
//! once the shared planner became the production authority.
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
//! Three consequences are load-bearing:
//!
//! * under a non-empty order property a join has NO hash-join candidate, so
//!   the comparison the corpus appeared to demand never happens at that site;
//! * an index join under such a property re-plans its OUTER side under the
//!   SAME property, which is what keeps the parent merge joins alive;
//! * the `Sort`-enforced merge join that would be cheaper on some statements
//!   is unreachable without a hint, so reproducing Go's CHOICE and minimising
//!   Go's COST are different objectives -- fidelity is the objective here.
//!
use crate::logical::LogicalPlan;
use crate::physical_property::{PhysicalProperty, SortItem, TaskType};
use crate::plan_base::PlanError;
use crate::plan_cost_ver2::IndexJoinKind;

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
    /// `GetJoinKeys()`'s `hasNullEQ`: some key is `<=>`.
    ///
    /// Go refuses BOTH merge-join forms for it — `GetMergeJoin`
    /// (`physical_merge_join.go:70`) and `getEnforcedMergeJoin` (`:150`),
    /// each under the same `TODO: support null equal join keys for merge
    /// join`.
    pub has_null_eq: bool,
    /// Whether some join key's type is ENUM or SET.
    ///
    /// Go refuses the PLAIN merge-join enumeration for these
    /// (`physical_merge_join.go:57-66`, issues 24473/25669: merge join
    /// conflicts with index order for them) — but NOT the hint-enforced
    /// form, which checks only `hasNullEQ`. That asymmetry is Go's, and it
    /// is reproduced, not repaired.
    pub keys_contain_enum_or_set: bool,
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
    let child_prop = |keys| {
        let mut child = PhysicalProperty::new(TaskType::Root, keys, desc, f64::MAX, false);
        child.cte_producer_status = prop.cte_producer_status;
        child.no_cop_push_down = prop.no_cop_push_down;
        child
    };
    Some([child_prop(left_keys), child_prop(right_keys)])
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
    // Go `exhaustPhysicalPlans4LogicalJoin` enumerates only hash joins while
    // this join is itself inside an index-join probe. Each hash candidate
    // forwards `IndexJoinProp` through one child so the eventual data source
    // can return `IndexJoinInfo`; merge and nested index joins are excluded.
    if prop.index_join_prop.is_some() {
        return hash_join_candidates(join, prop);
    }
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
    // `getEnforcedMergeJoin` refuses null-eq keys (`physical_merge_join.go:150`)
    // — and ONLY those; the ENUM/SET refusal guards the plain form alone.
    if join.has_null_eq {
        return Vec::new();
    }
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
        sort_items_for_partition: Vec::new(),
        cte_producer_status: prop.cte_producer_status,
        no_cop_push_down: prop.no_cop_push_down,
        index_join_prop: None,
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
    // `GetMergeJoin`'s two up-front refusals, in Go's order: ENUM/SET keys
    // (`physical_merge_join.go:57-66`), then null-eq keys (`:70`).
    if join.keys_contain_enum_or_set || join.has_null_eq {
        return Vec::new();
    }
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
            sort_items_for_partition: Vec::new(),
            cte_producer_status: prop.cte_producer_status,
            no_cop_push_down: prop.no_cop_push_down,
            index_join_prop: None,
        };
        child_props[1 - outer_idx] = PhysicalProperty {
            cte_producer_status: prop.cte_producer_status,
            no_cop_push_down: prop.no_cop_push_down,
            ..PhysicalProperty::default()
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
        sort_items_for_partition: Vec::new(),
        cte_producer_status: prop.cte_producer_status,
        no_cop_push_down: prop.no_cop_push_down,
        index_join_prop: None,
    };
    let mut candidates = Vec::new();
    for shape in hash_join_shapes(join.join_type) {
        if let Some(runtime) = &prop.index_join_prop {
            // Go `getHashJoin`: for a parent index-join runtime property,
            // enumerate one candidate per child that may contain the target
            // data source. Exactly one child receives the property.
            for child_idx in 0..2 {
                let mut child_props = [child_prop(), child_prop()];
                child_props[child_idx].index_join_prop = Some(runtime.clone());
                candidates.push(EnumeratedJoin {
                    strategy: JoinStrategy::Hash(shape.clone()),
                    child_props,
                    child_roles: [LeafRole::Plain, LeafRole::Plain],
                });
            }
        } else {
            candidates.push(EnumeratedJoin {
                strategy: JoinStrategy::Hash(shape),
                child_props: [child_prop(), child_prop()],
                child_roles: [LeafRole::Plain, LeafRole::Plain],
            });
        }
    }
    candidates
}

/// Go `hint.PreferMergeJoin`: bit 7 of the `1 << iota` block opened by
/// `PreferINLJ` (`pkg/util/hint/hint.go:141`).
pub const PREFER_MERGE_JOIN: u32 = 1 << 7;

/// Reduce one real logical join to the fields its physical enumeration reads.
///
/// * keys through `LogicalJoin::get_join_keys` (Go `GetJoinKeys`,
///   `logical_join.go:1011`), which also answers `has_null_eq`;
/// * `keys_contain_enum_or_set` from the key columns' own types, which is
///   what lets [`merge_join_candidates`] apply `GetMergeJoin`'s refusal
///   (`physical_merge_join.go:57-66`);
/// * `force_merge` as `PreferJoinType & PreferMergeJoin`, Go's own test;
/// * schemas and provided orders from the operator's fields.
pub(crate) fn project_one_join(
    join: &crate::logical::LogicalJoin,
    node: &LogicalPlan,
) -> Result<LogicalJoin, PlanError> {
    let (left_key_cols, right_key_cols, _, has_null_eq) = join.get_join_keys();
    // `GetMergeJoin` reads `RetType.GetType()` on every key of BOTH sides
    // (`physical_merge_join.go:57-66`).
    let is_enum_or_set = |col: &tidb_expr::column::Column| {
        col.ret_type.as_ref().is_some_and(|ty| {
            matches!(
                ty.code(),
                tidb_datatype::FieldTypeCode::Enum | tidb_datatype::FieldTypeCode::Set
            )
        })
    };
    let keys_contain_enum_or_set =
        left_key_cols.iter().any(&is_enum_or_set) || right_key_cols.iter().any(&is_enum_or_set);
    let schema_ids = |child: &LogicalPlan| -> Vec<i64> {
        // Builder output carries a schema on or near every join child; the
        // lookup recurses only through schema-less pass-throughs.
        child
            .schema()
            .map(|schema| schema.columns.iter().map(|col| col.unique_id).collect())
            .unwrap_or_default()
    };
    let children = node.children();
    let ids = |cols: &[tidb_expr::column::Column]| -> Vec<i64> {
        cols.iter().map(|col| col.unique_id).collect()
    };
    Ok(LogicalJoin {
        join_type: join.join_type,
        left_keys: ids(&left_key_cols),
        right_keys: ids(&right_key_cols),
        left_schema: children.first().map(|c| schema_ids(c)).unwrap_or_default(),
        right_schema: children.get(1).map(|c| schema_ids(c)).unwrap_or_default(),
        left_properties: join.left_properties.iter().map(|p| ids(p)).collect(),
        right_properties: join.right_properties.iter().map(|p| ids(p)).collect(),
        force_merge: join.prefer_join_type & PREFER_MERGE_JOIN != 0,
        has_null_eq,
        keys_contain_enum_or_set,
    })
}

pub mod coster;
pub mod dispatch;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_property::CteProducerStatus;

    #[test]
    fn every_join_candidate_preserves_cte_and_no_cop_requirements() {
        let join = LogicalJoin {
            join_type: LogicalJoinType::Inner,
            left_keys: vec![1],
            right_keys: vec![2],
            left_schema: vec![1],
            right_schema: vec![2],
            left_properties: vec![vec![1]],
            right_properties: vec![vec![2]],
            force_merge: false,
            has_null_eq: false,
            keys_contain_enum_or_set: false,
        };
        let prop = PhysicalProperty {
            cte_producer_status: CteProducerStatus::AllCteCanMpp,
            no_cop_push_down: true,
            ..PhysicalProperty::default()
        };
        let candidates = exhaust_join(&join, &prop);
        assert!(!candidates.is_empty());
        for candidate in candidates {
            for child in candidate.child_props {
                assert_eq!(child.cte_producer_status, CteProducerStatus::AllCteCanMpp);
                assert!(child.no_cop_push_down);
            }
        }
    }
}
