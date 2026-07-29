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

//! The DDL seam: this node's one route to the cluster's stored schema. Split
//! out of `cluster_session_node` because it is one of the independent seams
//! that accreted there; see that module's doc comment for how a DDL
//! statement is routed here and what happens to the connection's own
//! catalog afterwards.

use std::sync::Arc;
use std::time::Duration;

use tidb_exec::catalog_reload::ReloadedCatalog;
use tidb_exec::catalog_watch::SharedCatalog as SharedClusterCatalog;
use tidb_exec::cluster_ddl::DdlStatement;
use tidb_exec::real_tikv_catalog::reload_catalog_from_cluster;
use tidb_exec::real_tikv_ddl::{commit_cluster_ddl, ClusterDdlReport, SchemaVersionNotifier};
use tidb_exec::real_tikv_read::RealOptimisticTransactionOpener;
use tidb_pd_client::EtcdClient;

/// This node's one route to the cluster's stored schema.
///
/// The seam exists for the same reason `ClusterTransactions` does: the
/// routing decision -- which statements become catalog changes, what happens
/// to an open transaction, when the connection's tables are rebuilt -- is
/// exercised without a cluster. The production implementation is
/// [`RealClusterDdl`].
pub trait ClusterDdl: Send + Sync {
    /// Publishes one admitted catalog change, then brings this node's own
    /// catalog up to it before answering.
    ///
    /// The two halves are one method because a caller that published without
    /// refreshing would answer the next statement from a catalog it knows to
    /// be stale.
    fn execute(&self, statement: &DdlStatement) -> Result<ClusterDdlReport, String>;
}

/// The production catalog writer: the optimistic 2PC over the node's one
/// process authority, followed by an inline reload of the node's own catalog.
pub struct RealClusterDdl {
    opener: Arc<RealOptimisticTransactionOpener>,
    catalog: Arc<SharedClusterCatalog>,
    timeout: Duration,
    /// The etcd client this node announces its catalog changes through, so
    /// peers' watches fire promptly. `None` leaves them to their lease tick;
    /// a failed announcement is a warning, never a failed DDL.
    notifier: Option<Arc<EtcdClient>>,
}

impl RealClusterDdl {
    /// Binds the writer to an already-connected authority and the catalog slot
    /// the reload thread publishes into.
    #[must_use]
    pub fn new(
        opener: RealOptimisticTransactionOpener,
        catalog: Arc<SharedClusterCatalog>,
        timeout: Duration,
        notifier: Option<Arc<EtcdClient>>,
    ) -> Self {
        Self {
            opener: Arc::new(opener),
            catalog,
            timeout,
            notifier,
        }
    }

    /// Runs one reload pass inline, on the statement's own thread.
    ///
    /// Go's DDL owner PUTs the new version to etcd so every *other* node's
    /// watch fires; this node is the one that just wrote the change, so it
    /// needs no notification -- it reloads at once instead of waiting up to
    /// `lease/2` for the reload thread's tick. Both publishers replace the
    /// catalog whole in the same slot, so neither can observe the other
    /// half-applied.
    ///
    /// A failed reload is not a failed DDL: the change is committed in the
    /// cluster, and the lease tick will pick it up. Reporting the statement as
    /// failed would be a lie about what the cluster now holds, so the failure
    /// is emitted and the statement stands.
    fn refresh_catalog(&self) {
        let current = self.catalog.load();
        match reload_catalog_from_cluster(&self.opener, self.timeout, &current) {
            Ok(ReloadedCatalog::Unchanged { .. }) => {}
            Ok(ReloadedCatalog::Diffs { catalog, .. } | ReloadedCatalog::Full { catalog, .. }) => {
                self.catalog.store(catalog);
            }
            Err(error) => eprintln!(
                "{{\"event\":\"catalog_reload_after_ddl_failed\",\"schema_version\":{},\"error\":{:?}}}",
                current.schema_version,
                error.to_string()
            ),
        }
    }
}

impl ClusterDdl for RealClusterDdl {
    fn execute(&self, statement: &DdlStatement) -> Result<ClusterDdlReport, String> {
        let notifier = self
            .notifier
            .as_ref()
            .map(|client| Arc::as_ref(client) as &dyn SchemaVersionNotifier);
        let report = commit_cluster_ddl(&self.opener, statement, self.timeout, notifier)
            .map_err(|error| error.to_string())?;
        self.refresh_catalog();
        Ok(report)
    }
}
