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

//! Go `pkg/planner/core/operator/logicalop/logical_lock.go`: `LogicalLock`,
//! the `SELECT ... FOR UPDATE` / `FOR SHARE` operator.
//!
//! SEED of `pkg/planner/core`. This operator was a
//! [`crate::logical::TodoLogicalOp`] before this batch.
//!
//! # Narrowings, by name
//!
//! * `Lock *ast.SelectLockInfo` carries the lock type and a `WaitSec`. The AST
//!   node is not transcreated, so the DECISION-bearing half is [`SelectLockType`]
//!   here — every body below branches on the type and on nothing else.
//! * `TblID2Handle map[int64][]util.HandleCols` becomes a table-id to COLUMNS
//!   map, the same narrowing [`crate::logical::DataSource`] already makes for
//!   its own `HandleCols`.

use std::collections::BTreeMap;

use tidb_expr::column::Column;

use crate::logical::BaseLogicalPlan;

/// Go `ast.SelectLockType`, in the variants `logical_lock.go` distinguishes.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum SelectLockType {
    /// Go `ast.SelectLockNone`.
    #[default]
    None,
    /// Go `ast.SelectLockForUpdate`.
    ForUpdate,
    /// Go `ast.SelectLockForUpdateNoWait`.
    ForUpdateNoWait,
    /// Go `ast.SelectLockForUpdateWaitN`.
    ForUpdateWaitN,
    /// Go `ast.SelectLockForShare`.
    ForShare,
    /// Go `ast.SelectLockForShareNoWait`.
    ForShareNoWait,
}

/// Go `isSelectForUpdateLockType(lockType)` (`logical_lock.go:131`).
#[must_use]
pub const fn is_select_for_update_lock_type(lock_type: SelectLockType) -> bool {
    matches!(
        lock_type,
        SelectLockType::ForUpdate
            | SelectLockType::ForUpdateNoWait
            | SelectLockType::ForUpdateWaitN
    )
}

/// Go `isSelectForShareLockType(lockType)` (`logical_lock.go:141`).
#[must_use]
pub const fn is_select_for_share_lock_type(lock_type: SelectLockType) -> bool {
    matches!(
        lock_type,
        SelectLockType::ForShare | SelectLockType::ForShareNoWait
    )
}

/// Go `logicalop.IsSupportedSelectLockType(lockType)`
/// (`logical_lock.go:151`): whether this lock type acquires pessimistic locks.
///
/// This is the gate on the whole handle-column widening in
/// [`LogicalLock::prune_columns_local`]: a lock TiDB does not implement reads no
/// handle, so it must not force one to survive pruning.
#[must_use]
pub const fn is_supported_select_lock_type(lock_type: SelectLockType) -> bool {
    is_select_for_update_lock_type(lock_type) || is_select_for_share_lock_type(lock_type)
}

/// Go `logicalop.LogicalLock` (`logical_lock.go:27`).
#[derive(Clone, Debug, Default)]
pub struct LogicalLock {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `Lock.LockType`; see this module's header.
    pub lock_type: SelectLockType,
    /// Go `Lock.WaitSec`, which `ForUpdateWaitN` carries.
    pub wait_sec: u64,
    /// Go `TblID2Handle`, as the handle COLUMNS per table id.
    pub tbl_id_to_handle_cols: BTreeMap<i64, Vec<Column>>,
    /// Go `TblID2PhysTblIDCol`: for a partitioned table, "the child executor
    /// need to return an extra column containing the Physical Table ID (i.e.
    /// from which partition the row came from)".
    pub tbl_id_to_phys_tbl_id_col: BTreeMap<i64, Column>,
}

impl LogicalLock {
    /// Go `plancodec.TypeLock`.
    pub const TYPE: &'static str = "SelectLock";

    /// Go `LogicalLock.Init(ctx)` (`logical_lock.go:42`), which fixes the
    /// query-block offset at 0.
    #[must_use]
    pub fn new(base: BaseLogicalPlan, lock_type: SelectLockType) -> Self {
        Self {
            base,
            lock_type,
            wait_sec: 0,
            tbl_id_to_handle_cols: BTreeMap::new(),
            tbl_id_to_phys_tbl_id_col: BTreeMap::new(),
        }
    }

    /// Go `LogicalLock.PruneColumns(parentUsedCols)`'s LOCAL half
    /// (`logical_lock.go:52`): every locked table's handle columns — and its
    /// partition-id column, when it has one — must survive pruning, because the
    /// lock is taken on the ROW and the row is named by its handle.
    ///
    /// An unsupported lock type widens nothing; see
    /// [`is_supported_select_lock_type`].
    #[must_use]
    pub fn prune_columns_local(&self, parent_used_cols: &[Column]) -> Vec<Column> {
        let mut used = parent_used_cols.to_vec();
        if !is_supported_select_lock_type(self.lock_type) {
            return used;
        }
        for (table_id, handle_cols) in &self.tbl_id_to_handle_cols {
            used.extend(handle_cols.iter().cloned());
            if let Some(phys_col) = self.tbl_id_to_phys_tbl_id_col.get(table_id) {
                used.push(phys_col.clone());
            }
        }
        used
    }

    /// Go `LogicalLock.PushDownTopN(topN)` (`logical_lock.go:85`): a TopN is
    /// pushed THROUGH the lock into its child, and the lock stays where it is.
    ///
    /// It never collapses: the lock must still be taken on whichever rows
    /// survive. Returns whether the driver should push into `children[0]`.
    #[must_use]
    pub const fn pushes_topn_into_child(has_topn: bool) -> bool {
        has_topn
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            lock_type: self.lock_type,
            wait_sec: self.wait_sec,
            tbl_id_to_handle_cols: self.tbl_id_to_handle_cols.clone(),
            tbl_id_to_phys_tbl_id_col: self.tbl_id_to_phys_tbl_id_col.clone(),
        }
    }
}
