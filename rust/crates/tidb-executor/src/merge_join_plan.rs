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

//! Whether a join's two sides ALREADY produce the order a merge join needs,
//! and what the required order of each side then is.
//!
//! # What this is a port of
//!
//! Go decides a merge join in `physicalop.GetMergeJoin`
//! (`pkg/planner/core/operator/physicalop/physical_merge_join.go:50`), which
//! reads two caches on the `LogicalJoin`: `LeftProperties` and
//! `RightProperties`, the column orders each CHILD can provide. For a base
//! table those come from `DataSource.PreparePossibleProperties`
//! (`pkg/planner/core/operator/logicalop/logical_datasource.go:343`), which
//! offers one order per access path: the int-handle column for a
//! `IsIntHandlePath`, and the index columns for an index path.
//!
//! `GetMergeJoin` then requires the join keys to be FULLY covered by one such
//! order on the left (`util.GetMaxSortPrefix`, and `len(offsets) <
//! len(leftJoinKeys)` skips) and by a matching prefix on the right
//! (`findMaxPrefixLen`). Only then does it build a `PhysicalMergeJoin` and
//! hand each child a required property over its own join keys
//! (`tryToGetChildReqProp`).
//!
//! # What this port covers
//!
//! BOTH branches of `PreparePossibleProperties`.
//!
//! The int-handle order is one a table read ALREADY produces. This tier's
//! base-table source streams a key-ordered snapshot (see
//! [`crate::table_access`]'s staged-row promise), and for an int handle the
//! record key's order IS the handle column's order, so demanding that order
//! changes NOTHING about the access path -- the same `TableFullScan` over the
//! same object, with `keep order:true` where Go prints `keep order:true`.
//!
//! The INDEX branch is different in kind: satisfying an index order means
//! CHOOSING an index path where this tier would otherwise read the table,
//! which changes the printed access object. It landed with the two pieces that
//! make it safe, and NOT before:
//!
//!  * the leaf can BUILD that walk -- Go's `convertToIndexScan` under a
//!    non-empty property, which is the order filter in
//!    [`crate::driver::access::leaf_index_path`] -- and reports the index
//!    order only when it was built with `keep order:true`, so the promise and
//!    the delivery are two separate statements;
//!  * the candidate list is gated by the statement's join-method HINTS before
//!    any cost is compared ([`crate::driver::join_method_hints`]), which is
//!    what `exhaustPhysicalPlans4LogicalJoin` does first.
//!
//! An earlier attempt at the index branch WITHOUT either measured 61 -> 101
//! divergences; a second, with the leaf still walking in handle order,
//! measured a row DROP (see [`table_scan_order`]).
//!
//! Unsigned int handles are excluded for the same reason
//! [`crate::handle_range`]'s `handle_column` excludes them: TiDB stores an
//! unsigned handle in an `int64`, so above `i64::MAX` the key order and the
//! column's unsigned order disagree, and an order this tier cannot actually
//! produce must not be promised.

use crate::kv_table::KvTable;
use tidb_datatype::FieldTypeCode;

/// A join key as a pair of column offsets, one in each side's own row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MergeJoinKey {
    /// Column offset within the LEFT child's row.
    pub(crate) left: usize,
    /// Column offset within the RIGHT child's row.
    pub(crate) right: usize,
}

/// A merge join this tier can run: the ordered key pairs, and the direction
/// both sides must be read in.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MergeJoinPlan {
    /// The key pairs, in the order both sides are sorted by.
    pub(crate) keys: Vec<MergeJoinKey>,
    /// Whether both sides must run descending. Go's `PhysicalMergeJoin.Desc`,
    /// taken from `prop.AllSameOrder()`.
    pub(crate) desc: bool,
}

/// The column orders a base-table read of `table` already produces.
///
/// `DataSource.PreparePossibleProperties`, int-handle branch only: Go's
/// `if path.IsIntHandlePath { col := ds.GetPKIsHandleCol(); if col != nil {
/// result = append(result, []*expression.Column{col}) } }`.
///
/// Returns at most one order because a table has at most one int handle. The
/// empty answer is the correct one for a table without a clustered integer
/// primary key, and it makes every caller below decline a merge join.
pub(crate) fn provided_orders(table: &KvTable) -> Vec<Vec<usize>> {
    let mut orders = table_scan_order(table);
    orders.extend(index_orders(table));
    orders
}

/// The INDEX branch of the same Go loop:
///
/// ```text
/// if len(path.IdxCols) == 0 { continue }
/// result = append(result, make([]*expression.Column, len(path.IdxCols)))
/// copy(result[len(result)-1], path.IdxCols)
/// ```
///
/// One order per access path, which for an index path is its key parts in
/// key order. Go's `EqCondCount` suffixes are NOT produced here: they are the
/// orders left over once a leading key part is pinned to a constant by a
/// pushed-down `=`, and no condition reaches a join leaf in this tier (see
/// [`crate::driver::access::leaf_index_path`]'s "why no `WHERE` reaches it"),
/// so `path.EqCondCount` is zero at every site this answers for.
///
/// A key part with a declared PREFIX length is where the list stops:
/// [`crate::kv_table::KvIndex::ordered_column_offsets`] cuts it, because the
/// entry holds `'abc'` where the row holds `'abcdef'` and the index does not
/// order by that column at all. Go reaches the same answer one layer later,
/// in `matchProperty`'s `idxColLens[colIdx] == types.UnspecifiedLength`.
///
/// This is the PROMISE half only. Whether a leaf can be BUILT to walk one of
/// these orders is [`crate::driver::access::leaf_ordered_index_path`], and
/// whether it actually was is what the leaf reports back -- see
/// [`table_scan_order`] for the row drop that conflating the two once hid.
fn index_orders(table: &KvTable) -> Vec<Vec<usize>> {
    table
        .plan_indexes()
        .filter_map(|index| {
            let ordered = index.ordered_column_offsets();
            (!ordered.is_empty()).then(|| ordered.to_vec())
        })
        .collect()
}

/// The orders a whole-table scan of `table` ACTUALLY walks in -- the record
/// key's own order, which for an integer handle is that column's order.
///
/// This is deliberately a DIFFERENT function from [`provided_orders`] even
/// though the two agree today, and the difference is the whole point.
/// [`provided_orders`] is Go's `PreparePossibleProperties`: a claim about the
/// orders SOME access path of this table could produce, which is what a merge
/// join is offered. This one is a statement about the executor
/// [`crate::driver::from::build_from`] actually builds for a leaf, and it can
/// never say more than a `TableFullScan` delivers.
///
/// MEASURED, and the reason the two are separated: growing
/// [`provided_orders`] into Go's index branch while the leaf's DELIVERED
/// orders were read from that same function made a merge join form over an
/// index column while the leaf walked in handle order, and rows were silently
/// DROPPED -- see
/// [`crate::driver::tests::joins::a_leaf_delivers_only_the_order_its_scan_walks_in`].
/// The promise/verify contract in [`crate::driver::merge_decision`] can only
/// catch that if the verify side reads the BUILD, so the build side gets its
/// own name here rather than sharing the promise's.
pub(crate) fn table_scan_order(table: &KvTable) -> Vec<Vec<usize>> {
    let Some(offset) = table.pk_handle_offset() else {
        return Vec::new();
    };
    let Some(column) = table.columns.get(offset) else {
        return Vec::new();
    };
    if column.field_type.is_unsigned() {
        return Vec::new();
    }
    if !matches!(
        column.field_type.code(),
        FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
    ) {
        return Vec::new();
    }
    vec![vec![offset]]
}

/// `util.GetMaxSortPrefix`: for each column of `sort_cols` in turn, its
/// position in `all_cols`, stopping at the first column that is absent.
///
/// The answer is the join-key POSITIONS in the order the child provides them,
/// which is what lets the caller reorder both key lists into merge order.
pub(crate) fn max_sort_prefix(sort_cols: &[usize], all_cols: &[usize]) -> Vec<usize> {
    let mut offsets = Vec::with_capacity(sort_cols.len());
    for sort_col in sort_cols {
        let Some(offset) = all_cols.iter().position(|col| col == sort_col) else {
            return offsets;
        };
        offsets.push(offset);
    }
    offsets
}

/// `findMaxPrefixLen`: the longest prefix of `keys` that some candidate order
/// begins with.
pub(crate) fn find_max_prefix_len(candidates: &[Vec<usize>], keys: &[usize]) -> usize {
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

/// The merge join `GetMergeJoin` would build for these keys over these two
/// sides, or `None` when no provided order covers them.
///
/// `equal_keys` are the join's equal conditions as `(left offset, right
/// offset)` pairs, in `ON` order -- Go's `p.GetJoinKeys()`.
///
/// `desc` is `prop.AllSameOrder()`'s second answer for the property this join
/// is being asked for; an unordered parent asks for ascending, which is what
/// [`tidb_planner::physical_property::PhysicalProperty::all_same_order`]
/// returns for the empty property.
pub(crate) fn get_merge_join(
    equal_keys: &[MergeJoinKey],
    left_orders: &[Vec<usize>],
    right_orders: &[Vec<usize>],
    desc: bool,
) -> Option<MergeJoinPlan> {
    if equal_keys.is_empty() {
        // Go: `len(leftJoinKeys) == 0` skips every candidate, and a merge
        // join with no key is only ever produced by the ENFORCED path.
        return None;
    }
    let left_keys: Vec<usize> = equal_keys.iter().map(|key| key.left).collect();
    let right_keys: Vec<usize> = equal_keys.iter().map(|key| key.right).collect();
    for order in left_orders {
        let offsets = max_sort_prefix(order, &left_keys);
        // "If not all equal conditions hit properties. We ban merge join
        // heuristically": a partially ordered side would make the executor
        // compare groups the order does not separate.
        if offsets.len() < left_keys.len() {
            continue;
        }
        // The keys REORDERED into the order the left side provides them in.
        let ordered_right: Vec<usize> = offsets.iter().map(|&at| right_keys[at]).collect();
        let prefix_len = find_max_prefix_len(right_orders, &ordered_right);
        if prefix_len < offsets.len() || prefix_len == 0 {
            continue;
        }
        let keys = offsets[..prefix_len]
            .iter()
            .map(|&at| equal_keys[at])
            .collect();
        return Some(MergeJoinPlan { keys, desc });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{find_max_prefix_len, get_merge_join, max_sort_prefix, MergeJoinKey};

    fn key(left: usize, right: usize) -> MergeJoinKey {
        MergeJoinKey { left, right }
    }

    /// `GetMaxSortPrefix` stops at the FIRST provided column the key list does
    /// not contain, so a side ordered by `(a, b)` whose join key is only `b`
    /// answers with the empty prefix rather than with `b`'s position.
    #[test]
    fn max_sort_prefix_stops_at_the_first_absent_column() {
        assert_eq!(max_sort_prefix(&[0], &[0]), vec![0]);
        assert_eq!(max_sort_prefix(&[0, 1], &[1, 0]), vec![1, 0]);
        assert_eq!(max_sort_prefix(&[0, 1], &[1]), Vec::<usize>::new());
        assert_eq!(max_sort_prefix(&[1, 0], &[1]), vec![0]);
    }

    /// `findMaxPrefixLen` takes the BEST candidate, and a candidate that
    /// starts elsewhere contributes zero.
    #[test]
    fn find_max_prefix_len_takes_the_best_candidate() {
        let candidates = vec![vec![3usize], vec![1, 2]];
        assert_eq!(find_max_prefix_len(&candidates, &[1, 2]), 2);
        assert_eq!(find_max_prefix_len(&candidates, &[3]), 1);
        assert_eq!(find_max_prefix_len(&candidates, &[2]), 0);
        assert_eq!(find_max_prefix_len(&[], &[1]), 0);
    }

    /// The target shape: `t2 join t3 on t2.a = t3.a` where `a` is column 0 and
    /// the int handle on both sides. Both sides provide `[0]`, the single key
    /// is fully covered, and the merge join is available.
    #[test]
    fn both_sides_ordered_by_the_single_join_key_merge() {
        let plan = get_merge_join(&[key(0, 0)], &[vec![0]], &[vec![0]], false)
            .expect("both sides provide the key's order");
        assert_eq!(plan.keys, vec![key(0, 0)]);
        assert!(!plan.desc);
    }

    /// One side without a clustered integer primary key provides NO order, and
    /// that alone declines the merge join -- this is the gate that keeps every
    /// heap-ordered table on the hash path.
    #[test]
    fn a_side_that_provides_no_order_declines() {
        assert!(get_merge_join(&[key(0, 0)], &[], &[vec![0]], false).is_none());
        assert!(get_merge_join(&[key(0, 0)], &[vec![0]], &[], false).is_none());
    }

    /// A join on a NON-handle column declines even when both tables have a
    /// clustered integer primary key: the order they provide is over the
    /// handle, not over the key being joined.
    #[test]
    fn a_join_on_a_non_handle_column_declines() {
        assert!(get_merge_join(&[key(1, 1)], &[vec![0]], &[vec![0]], false).is_none());
    }

    /// A cartesian join has no equal condition and is never a property-driven
    /// merge join.
    #[test]
    fn a_join_without_equal_conditions_declines() {
        assert!(get_merge_join(&[], &[vec![0]], &[vec![0]], false).is_none());
    }

    /// Go bans the merge join when only SOME equal conditions hit the provided
    /// order: `len(offsets) < len(leftJoinKeys)` skips the candidate.
    #[test]
    fn a_partially_covered_key_list_declines() {
        assert!(get_merge_join(&[key(0, 0), key(1, 1)], &[vec![0]], &[vec![0]], false).is_none());
    }

    /// The required direction is carried straight through from the parent's
    /// property, which is what `PhysicalMergeJoin.Desc` is.
    #[test]
    fn the_required_direction_is_carried_through() {
        let plan = get_merge_join(&[key(0, 0)], &[vec![0]], &[vec![0]], true).expect("available");
        assert!(plan.desc);
    }
}
