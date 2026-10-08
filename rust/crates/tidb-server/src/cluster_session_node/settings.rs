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

//! Internal SQL ownership for HTTP GLOBAL settings.
use super::*;

impl ClusterSessionFactory {
    pub(crate) fn peer_service(&self) -> crate::peer_rpc::PeerService {
        let catalog = Arc::clone(&self.catalog);
        let peer = self.cluster_peer.clone();
        let stats = Arc::clone(&self.stats);
        let auto_ids = Arc::clone(&self.auto_ids);
        let index_usage = self.stats_usage.index_usage_collector();
        let templates = Arc::clone(&self.session_kv_cache);
        crate::peer_rpc::PeerService::new(
            self.processes.clone(),
            self.privileges.clone(),
            self.server_info.clone(),
        )
        .with_session_factory(Arc::new(move || {
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
            if let Some(peer) = &peer {
                session.set_cluster_peer_client(Arc::clone(peer));
            }
            session
        }))
    }

    pub(crate) fn status_settings(self: &Arc<Self>) -> crate::http_settings::Settings {
        let factory = Arc::downgrade(self);
        crate::http_settings::Settings::new(
            self.global_vars.clone(),
            Arc::new(move |name, value| {
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
            }),
        )
    }
}
