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

//! The catalog a connection follows.
//!
//! A connection's tables are built once from the node's catalog, so a schema
//! that moves afterwards is picked up by rebuilding them over the same
//! storage handles -- which leaves the snapshot slot and the staged writes
//! where they are. Go does the same per statement: it resolves against the
//! latest infoschema (`GetLatestInfoSchema`) unless a transaction has pinned
//! one. The follower owns exactly what that rebuild needs, apart from the
//! session, so a statement lifecycle that holds the session can still ask it
//! to follow.

use std::sync::Arc;

use tidb_kv::cluster_storage::ClusterTableStorage;
use crate::txn_manager::TxnManager;
use crate::Session;

use super::{
    cluster_session_catalog_with_templates,
    SharedClusterCatalog, SkippedTable, TableAutoIds,
};

/// See the module documentation.
pub(super) struct CatalogFollower {
    /// The node's catalog, which this connection follows.
    catalog: Arc<SharedClusterCatalog>,
    /// The handles every table of the session was built over, kept so the
    /// connection's catalog can be rebuilt after a DDL without disturbing the
    /// snapshot slot or the staged writes.
    storage: ClusterTableStorage,
    /// The node's auto-increment allocators, needed on every rebuild so the
    /// rebuilt tables keep the ranges the old ones had reserved.
    auto_ids: Arc<dyn TableAutoIds>,
    /// The schema version the session's tables were built from. A move in
    /// `catalog` past this is what makes the connection rebuild them.
    schema_version: i64,
    /// Tables of the cluster the connection's catalog could not include,
    /// answered by name when a statement names one. Rebuilt with the catalog.
    pub(super) skipped: Vec<SkippedTable>,
}

impl CatalogFollower {
    pub(super) fn new(
        catalog: Arc<SharedClusterCatalog>,
        storage: ClusterTableStorage,
        auto_ids: Arc<dyn TableAutoIds>,
        schema_version: i64,
        skipped: Vec<SkippedTable>,
    ) -> Self {
        Self {
            catalog,
            storage,
            auto_ids,
            schema_version,
            skipped,
        }
    }

    /// Rebinds the connection's tables to the node's current catalog.
    ///
    /// Not inside an open transaction: its statements read at one timestamp,
    /// and a schema that moved under them would describe rows that timestamp
    /// cannot see. Go holds the transaction's `InfoSchema` for the same reason.
    pub(super) fn rebuild_if_stale(&mut self, session: &mut Session, txn: &TxnManager) {
        if txn.explicit.is_some() || session.in_transaction() {
            return;
        }
        self.rebuild(session);
    }

    /// The unguarded rebuild: `BEGIN` calls this directly because the session
    /// already reports in-transaction by the time the transaction opens, and
    /// Go pins the LATEST schema at transaction start
    /// (`domain.GetSnapshotInfoSchema(startTS)`) -- a table committed before
    /// `BEGIN` is visible to every statement of the new transaction.
    pub(super) fn rebuild(&mut self, session: &mut Session) {
        let loaded = self.catalog.load();
        // Go pins InfoSchema independently of Domain.StatsHandle. Statistics
        // reads already see current publications, including inside a transaction.
        if loaded.schema_version == self.schema_version {
            return;
        }
        let mut built = cluster_session_catalog_with_templates(
            &loaded,
            &self.storage,
            None,
            self.auto_ids.as_ref(),
            &self.storage,
            None,
        );
        let shared = session.shared_catalog();
        let mut catalog = shared.lock().unwrap_or_else(|poison| poison.into_inner());
        *catalog = built.catalog;
        drop(catalog);
        self.schema_version = loaded.schema_version;
        session.set_plan_cache_schema_version(loaded.schema_version as u64);
        session.set_info_schema(Arc::clone(&loaded));
        self.skipped = built.skipped;
    }
}

impl crate::statement_lifecycle::CatalogFollowing for CatalogFollower {
    fn schema_version(&self) -> i64 {
        self.schema_version
    }

    fn rebuild_if_stale(&mut self, session: &mut Session, txn: &TxnManager) {
        Self::rebuild_if_stale(self, session, txn);
    }

    fn rebuild(&mut self, session: &mut Session) {
        Self::rebuild(self, session);
    }
}
