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

/// The domain's settings and peer hooks that run on the session package's sessions.
pub trait DomainSessionSettings {
    /// Go `rpcServer.createSession`: a session on this node's store for
    /// each peer coprocessor request, holding no client connection.
    fn peer_session_builder(
        self: &Arc<Self>,
    ) -> Arc<dyn Fn() -> Result<Session, String> + Send + Sync>;

    /// Go `IsFromRemote` plans run on this instance alone; this node's
    /// statements never broadcast, so an internal session runs a received
    /// broadcast statement as-is.
    fn broadcast_query_runner(
        self: &Arc<Self>,
    ) -> Arc<dyn Fn(&str) -> Result<(), String> + Send + Sync>;

    /// The HTTP status port's GLOBAL setter: a fixed handler name and a
    /// validated ON/OFF value become a `SET GLOBAL` on an internal session.
    fn http_global_setter(
        self: &Arc<Self>,
    ) -> Arc<dyn Fn(&str, &str) -> Result<(), String> + Send + Sync>;
}

impl DomainSessionSettings for Domain {
    fn peer_session_builder(
        self: &Arc<Self>,
    ) -> Arc<dyn Fn() -> Result<Session, String> + Send + Sync> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_PEER_CONNECTION_ID: AtomicU64 = AtomicU64::new(3_u64 << 61);
        let domain = Arc::downgrade(self);
        Arc::new(move || {
            let domain = domain.upgrade().ok_or("the node's domain is stopped")?;
            domain
                .open_storage_session(NEXT_PEER_CONNECTION_ID.fetch_add(1, Ordering::Relaxed))
                .map_err(|error| error.message)
        })
    }

    fn broadcast_query_runner(
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
                        let mut slot = cluster_stats_session(context)
                            .state
                            .session
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        let session = slot.as_mut().ok_or_else(|| {
                            tidb_syssession::SysSessionError::new("broadcast session is closed")
                        })?;
                        let result = (|| -> Result<(), String> {
                            let mut result = session.node_conn().execute(query).map_err(|e| e.message)?;
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

    fn http_global_setter(
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
                            let mut slot = cluster_stats_session(context)
                                .state
                                .session
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            let session = slot.as_mut().ok_or_else(|| {
                                tidb_syssession::SysSessionError::new("settings session is closed")
                            })?;
                            let result = (|| -> Result<(), String> {
                                let mut result = session
                                    .node_conn()
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
