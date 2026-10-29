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

//! What this node's sessions run with beside the domain: Go's
//! `createSessionWithOpt` and `cmd/tidb-server`'s main wire these into each
//! session, and Go's `Domain` holds none of them. The domain carries them for
//! the session layer ([`Domain::set_session_services`]), which owns the type.

use std::sync::{Arc, RwLock};

use tidb_executor::cluster_analyze_seam::ClusterAnalyze;
use tidb_executor::cluster_config::ClusterConfigClient;
use tidb_executor::cluster_diagnostics::ClusterDiagnosticsClient;
use tidb_executor::cluster_peer::ClusterPeerClient;
use tidb_executor::PlanCacheInvalidation;

use super::Domain;

/// The node's session-layer services.
pub struct NodeServices {
    /// The route an `ANALYZE TABLE` takes; see
    /// `tidb_executor::cluster_analyze_seam`.
    analyze: RwLock<Arc<dyn ClusterAnalyze>>,
    /// The instance plan cache, for the node's lifetime: independent of each
    /// connection's rebuilt catalog.
    pub plan_cache_invalidation: Arc<PlanCacheInvalidation>,
    /// Go `fetchClusterConfig`'s internal HTTP client, shared by every session.
    pub cluster_config: Option<Arc<ClusterConfigClient>>,
    /// The diagnostics gRPC client the server-info and log cluster tables
    /// read through.
    pub cluster_diagnostics: Option<Arc<ClusterDiagnosticsClient>>,
    /// The outgoing peer fleet, when the node has one.
    pub cluster_peer: Option<Arc<ClusterPeerClient>>,
}

impl NodeServices {
    /// Services over `analyze`, with no cluster clients.
    #[must_use]
    pub fn new(analyze: Arc<dyn ClusterAnalyze>) -> Self {
        Self {
            analyze: RwLock::new(analyze),
            plan_cache_invalidation: Arc::default(),
            cluster_config: None,
            cluster_diagnostics: None,
            cluster_peer: None,
        }
    }

    /// Binds outgoing cluster administration to the process RPC owner.
    #[must_use]
    pub fn with_cluster_peer_client(mut self, client: Arc<ClusterPeerClient>) -> Self {
        self.cluster_peer = Some(client);
        self
    }

    /// Shares the internal HTTP configuration retriever with every SQL session.
    #[must_use]
    pub fn with_cluster_config_client(mut self, client: Arc<ClusterConfigClient>) -> Self {
        self.cluster_config = Some(client);
        self
    }

    /// Shares the diagnostics gRPC client with every SQL session.
    #[must_use]
    pub fn with_cluster_diagnostics_client(mut self, client: Arc<ClusterDiagnosticsClient>) -> Self {
        self.cluster_diagnostics = Some(client);
        self
    }

    /// The node's ANALYZE client.
    pub fn analyze(&self) -> Arc<dyn ClusterAnalyze> {
        Arc::clone(&self.analyze.read().unwrap_or_else(std::sync::PoisonError::into_inner))
    }

    /// Replaces the node's ANALYZE client.
    #[cfg(any(test, feature = "test-util"))]
    pub fn set_analyze(&self, analyze: Arc<dyn ClusterAnalyze>) {
        *self.analyze.write().unwrap_or_else(std::sync::PoisonError::into_inner) = analyze;
    }

    /// Binds these services to `domain`; a node binds them once, as it is built.
    pub fn bind(self, domain: &Domain) {
        if domain.set_session_services(Arc::new(self)).is_err() {
            panic!("a node's session services are bound once");
        }
    }
}

impl Drop for NodeServices {
    fn drop(&mut self) {
        self.plan_cache_invalidation.stop_maintenance();
    }
}

/// The services `domain`'s node was built with.
#[must_use]
pub fn node_services(domain: &Domain) -> Arc<NodeServices> {
    domain
        .session_services::<NodeServices>()
        .expect("a node binds its session services as it is built")
}
