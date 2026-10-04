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
