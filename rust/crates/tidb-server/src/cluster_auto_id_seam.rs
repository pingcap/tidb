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

//! The node's auto-increment allocators: one per table, over the cluster's
//! own meta keys, kept alive for as long as the node is.
//!
//! Go puts the allocator on the domain's table cache -- one per
//! `tidb-server`, not one per session and not one per schema reload -- and
//! that placement is a correctness property, not a cache. A reserved range
//! belongs to whoever reserved it, so an allocator rebuilt alongside its
//! `KvTable` would abandon the ids it still held and reserve a fresh range
//! from the meta key, leaving a `DEFAULT_AUTO_ID_STEP`-sized hole in the ids
//! every time the schema version moved. This registry is where the allocator
//! outlives the table objects, and [`ClusterSessionFactory`] holds exactly
//! one.
//!
//! [`ClusterSessionFactory`]: crate::cluster_session_node::ClusterSessionFactory
//! [`DEFAULT_AUTO_ID_STEP`]: tidb_executor::kv_table::DEFAULT_AUTO_ID_STEP

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use tidb_exec::cluster_auto_id::ClusterAutoIdStore;
use tidb_executor::kv_table::{TableAutoId, DEFAULT_AUTO_ID_STEP};
use tidb_model::TableInfo;
use tidb_txnkv::transaction::RealOptimisticTransactionOpener;

use crate::cluster_session::TableAutoIds;

/// This node's allocators, keyed by table id.
///
/// A table id is unique cluster-wide and is REPLACED rather than reused when
/// a table is truncated or recreated, which is what makes it the right key:
/// the entry a dropped table leaves behind can never be handed to a different
/// table, and a truncated table gets a new id and so a new allocator whose
/// counter key has never been written.
pub struct ClusterTableAutoIds {
    opener: RealOptimisticTransactionOpener,
    timeout: Duration,
    allocators: Mutex<HashMap<(i64, bool), (i64, TableAutoId)>>,
}

impl std::fmt::Debug for ClusterTableAutoIds {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let held = self.allocators.lock().map_or(0, |map| map.len());
        formatter
            .debug_struct("ClusterTableAutoIds")
            .field("tables", &held)
            .finish_non_exhaustive()
    }
}

impl ClusterTableAutoIds {
    /// Allocators reserving through `opener`, giving each meta-key
    /// transaction `timeout` to finish.
    #[must_use]
    pub fn new(opener: RealOptimisticTransactionOpener, timeout: Duration) -> Self {
        ClusterTableAutoIds {
            opener,
            timeout,
            allocators: Mutex::new(HashMap::new()),
        }
    }
}

impl TableAutoIds for ClusterTableAutoIds {
    fn allocator_for(&self, db_id: i64, table: &TableInfo) -> TableAutoId {
        self.allocator(db_id, table, false)
    }

    fn random_allocator_for(&self, db_id: i64, table: &TableInfo) -> TableAutoId {
        self.allocator(db_id, table, true)
    }
}

impl ClusterTableAutoIds {
    fn allocator(&self, db_id: i64, table: &TableInfo, random: bool) -> TableAutoId {
        let mut allocators = self
            .allocators
            .lock()
            .expect("cluster auto id registry poisoned");
        if let Some((cache, allocator)) = allocators.get(&(table.id, random)) {
            if *cache == table.auto_id_cache {
                return allocator.clone();
            }
            let step = if table.auto_id_cache > 1 {
                table.auto_id_cache as u64
            } else {
                DEFAULT_AUTO_ID_STEP
            };
            let allocator = allocator.with_step(step);
            allocators.insert((table.id, random), (table.auto_id_cache, allocator.clone()));
            return allocator;
        }
        let store = if random {
            ClusterAutoIdStore::new_random(self.opener.clone(), db_id, table, self.timeout).shared()
        } else {
            ClusterAutoIdStore::new(self.opener.clone(), db_id, table, self.timeout).shared()
        };
        let step = if table.auto_id_cache > 1 {
            table.auto_id_cache as u64
        } else {
            DEFAULT_AUTO_ID_STEP
        };
        let allocator = TableAutoId::over(store, step);
        allocators.insert((table.id, random), (table.auto_id_cache, allocator.clone()));
        allocator
    }
}
