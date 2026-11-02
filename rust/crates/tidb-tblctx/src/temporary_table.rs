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

//! Go `pkg/table/tblctx/table.go`'s `TemporaryTableHandler`, over the
//! transaction's temporary-table state (Go `tableutil.TempTable`, an entry of
//! `TxnCtx.TemporaryTables`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use tidb_variable::temporary_table_data::SessionData;

/// Go `tableutil.TempTable`'s size and modified state: what one transaction
/// wrote to one temporary table.
#[derive(Debug, Default)]
pub struct TempTable {
    size: AtomicI64,
    modified: AtomicBool,
}

impl TempTable {
    /// Go `GetSize`.
    pub fn get_size(&self) -> i64 {
        self.size.load(Ordering::Relaxed)
    }

    /// Go `SetSize`.
    pub fn set_size(&self, size: i64) {
        self.size.store(size, Ordering::Relaxed);
    }

    /// Go `GetModified`.
    pub fn get_modified(&self) -> bool {
        self.modified.load(Ordering::Relaxed)
    }

    /// Go `SetModified`.
    pub fn set_modified(&self, modified: bool) {
        self.modified.store(modified, Ordering::Relaxed);
    }
}

/// Go `TxnCtx.TemporaryTables`: the transaction's temporary tables by id,
/// built fresh for each transaction.
pub type TxnTemporaryTables = Arc<Mutex<HashMap<i64, Arc<TempTable>>>>;

/// Go `TxnCtx.CachedTables`: the cached tables this transaction wrote, by id
/// (Go `map[int64]any`, the table's handle), built fresh for each
/// transaction.
pub type TxnCachedTables = Arc<Mutex<std::collections::BTreeMap<i64, Arc<dyn std::any::Any + Send + Sync>>>>;

/// Go `TemporaryTableHandler`.
#[derive(Clone, Debug)]
pub struct TemporaryTableHandler {
    tbl_in_txn: Arc<TempTable>,
    table_id: i64,
    data: Option<SessionData>,
}

impl TemporaryTableHandler {
    /// Go `NewTemporaryTableHandler`.
    #[must_use]
    pub fn new(tbl_in_txn: Arc<TempTable>, table_id: i64, data: Option<SessionData>) -> Self {
        Self {
            tbl_in_txn,
            table_id,
            data,
        }
    }

    /// Go `GetDirtySize`.
    #[must_use]
    pub fn get_dirty_size(&self) -> i64 {
        self.tbl_in_txn.get_size()
    }

    /// Go `GetCommittedSize`.
    #[must_use]
    pub fn get_committed_size(&self) -> i64 {
        self.data.as_ref().map_or(0, |data| {
            data.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_table_size(self.table_id)
        })
    }

    /// Go `UpdateTxnDeltaSize`.
    pub fn update_txn_delta_size(&self, delta: i64) {
        self.tbl_in_txn.set_size(self.tbl_in_txn.get_size() + delta);
    }
}

/// Go `tblsession.MutateContext.AddTemporaryTableToTxn`: the transaction's
/// entry for temporary table `table_id` (created at its first write), marked
/// modified, with the session data its committed size is read from.
#[must_use]
pub fn add_temporary_table_to_txn(
    tables: &TxnTemporaryTables,
    table_id: i64,
    data: Option<&SessionData>,
) -> TemporaryTableHandler {
    let tbl = Arc::clone(
        tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(table_id)
            .or_default(),
    );
    tbl.set_modified(true);
    TemporaryTableHandler::new(tbl, table_id, data.cloned())
}
