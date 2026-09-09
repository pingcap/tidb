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

//! Legacy AST-driver order discovery. Go's sources are DataSource's
//! PreparePossibleProperties and physicalop.GetMergeJoin. Candidate orders
//! describe available access paths; table_scan_order describes the selected
//! record walk. The native physical plan lives in tidb-planner.

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

/// Available int-handle and index orders from PreparePossibleProperties.
pub(crate) fn provided_orders(table: &KvTable) -> Vec<Vec<usize>> {
    let mut orders = table_scan_order(table);
    orders.extend(index_orders(table));
    orders
}

/// Full-length index key prefixes; a prefix-index part cannot order a full column.
fn index_orders(table: &KvTable) -> Vec<Vec<usize>> {
    table
        .plan_indexes()
        .filter_map(|index| {
            let ordered = index.ordered_column_offsets();
            (!ordered.is_empty()).then(|| ordered.to_vec())
        })
        .collect()
}

/// Orders delivered by the selected record walk, not other available indexes.
pub(crate) fn table_scan_order(table: &KvTable) -> Vec<Vec<usize>> {
    if !table.common_handle_offsets().is_empty() {
        // A common handle is the mem-comparable key encoding of these datums
        // in exactly this order. Unlike a prefix secondary index, every
        // clustered-primary part is stored in full, so the record walk really
        // delivers the whole tuple order it promises.
        return vec![table.common_handle_offsets().to_vec()];
    }
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

/// Ordinary GetMergeJoin: both children must provide the complete key order.
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
