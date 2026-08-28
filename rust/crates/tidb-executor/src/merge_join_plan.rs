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

//! Executor order receipts used to lower a shared-planner merge join.
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
//! Physical merge candidate enumeration now lives only in `tidb-planner`.
//! This module retains the executor receipt types and the distinction between
//! orders an access path can provide and the order a selected table scan
//! actually delivers.
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
//!  * the shared planner applies the statement's join-method hints during
//!    `findBestTask`, before the executor lowers its selected receipt.
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
/// key's own order. For an integer handle that is the one handle column; for
/// a clustered common handle it is the complete primary-key tuple in encoded
/// datum order.
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
