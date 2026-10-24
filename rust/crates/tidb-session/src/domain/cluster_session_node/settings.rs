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

//! Internal SQL ownership for HTTP GLOBAL settings and the peer coprocessor:
//! the pieces of the status port's handlers that run SQL on this node. The
//! status port itself is the server's.
use super::*;

impl ClusterSessionFactory {
    /// Fresh metadata and the existing usage collector for a peer's internal
    /// session, without retaining a client connection or transaction.
    pub fn peer_session_builder(self: &Arc<Self>) -> Arc<dyn Fn() -> Session + Send + Sync> {
        let catalog = Arc::clone(&self.catalog);
        let stats = Arc::clone(&self.stats);
        let auto_ids = Arc::clone(&self.auto_ids);
        let index_usage = self.stats_usage.index_usage_collector();
        let templates = Arc::clone(&self.session_kv_cache);
        Arc::new(move || {
            let loaded = catalog.load();
            let storage = detached_storage();
            let mut templates = templates
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            templates.reuse(&loaded);
            let built = cluster_session_catalog_with_templates(
                &loaded,
                &storage,
                Some(&stats.load()),
                auto_ids.as_ref(),
                &storage,
                Some(&mut templates),
            );
            let mut session = Session::with_catalog(Arc::new(Mutex::new(built.catalog)));
            session.set_index_usage_collector(Arc::clone(&index_usage));
            session
        })
    }

    /// The outgoing peer fleet, when the node has one.
    pub fn cluster_peer(&self) -> Option<Arc<tidb_exec::cluster_peer::ClusterPeerClient>> {
        self.cluster_peer.clone()
    }

    /// The node's privilege registry.
    pub fn privileges(&self) -> PrivilegeRegistry {
        self.privileges.clone()
    }

    /// The node's server-info syncer.
    pub fn server_info(&self) -> Option<Arc<tidb_domain::serverinfo_syncer::Syncer>> {
        self.server_info.clone()
    }

    /// Go `IsFromRemote` plans run on this instance alone; this node's
    /// statements never broadcast, so an internal session runs a received
    /// broadcast statement as-is.
    pub fn broadcast_query_runner(
        self: &Arc<Self>,
    ) -> Arc<dyn Fn(&str) -> Result<(), String> + Send + Sync> {
        let broadcast = Arc::downgrade(self);
        Arc::new(move |query: &str| {
            let factory = broadcast
                .upgrade()
                .ok_or("broadcast session factory is stopped")?;
            factory
                .advanced_sys_session_pool()
                .with_session(|lease| {
                    lease.with_session_context(|context| {
                        let mut slot = context
                            .state
                            .session
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        let session = slot.as_mut().ok_or_else(|| {
                            tidb_syssession::SysSessionError::new("broadcast session is closed")
                        })?;
                        let result = (|| -> Result<(), String> {
                            let mut result = session.execute(query).map_err(|e| e.message)?;
                            let source = result.source();
                            source.finish().map_err(|e| e.to_string())?;
                            source.close().map_err(|e| e.to_string())?;
                            Ok(())
                        })();
                        if result.is_err() {
                            lease.avoid_reuse();
                        }
                        Ok(result)
                    })
                })
                .map_err(|e| e.to_string())?
        })
    }

    /// The node's GLOBAL variable cache.
    pub fn global_vars(&self) -> GlobalSysvars {
        self.global_vars.clone()
    }

    /// The HTTP status port's GLOBAL setter: a fixed handler name and a
    /// validated ON/OFF value become a `SET GLOBAL` on an internal session.
    pub fn http_global_setter(
        self: &Arc<Self>,
    ) -> Arc<dyn Fn(&str, &str) -> Result<(), String> + Send + Sync> {
        let factory = Arc::downgrade(self);
        Arc::new(move |name: &str, value: &str| {
                // Only the fixed handler names and validated ON/OFF values enter SQL.
                if !matches!(
                    name,
                    "tidb_enable_async_commit" | "tidb_enable_1pc" | "tidb_enable_mutation_checker"
                ) || !matches!(value, "ON" | "OFF")
                {
                    return Err("invalid HTTP global setting".into());
                }
                let factory = factory
                    .upgrade()
                    .ok_or("settings session factory is stopped")?;
                factory
                    .advanced_sys_session_pool()
                    .with_session(|lease| {
                        lease.with_session_context(|context| {
                            let mut slot = context
                                .state
                                .session
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            let session = slot.as_mut().ok_or_else(|| {
                                tidb_syssession::SysSessionError::new("settings session is closed")
                            })?;
                            let result = (|| -> Result<(), String> {
                                let mut result = session
                                    .execute(&format!("SET GLOBAL {name} = '{value}'"))
                                    .map_err(|e| e.message)?;
                                let source = result.source();
                                source.finish().map_err(|e| e.to_string())?;
                                source.close().map_err(|e| e.to_string())?;
                                Ok(())
                            })();
                            if result.is_err() {
                                lease.avoid_reuse();
                            }
                            Ok(result)
                        })
                    })
                    .map_err(|e| e.to_string())?
            })
    }
}
