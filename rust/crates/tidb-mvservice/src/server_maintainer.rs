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

//! Go master `pkg/mvservice/server_maintainer.go`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_logutil::bg_logger;

use crate::consistenthash::ConsistentHash;
use crate::time_proxy::mvs_after;

/// Go `serverInfo`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ServerInfo {
    /// Go `ID`.
    pub id: String,
    /// Go `IP`.
    pub ip: String,
    /// Go `Port`.
    pub port: u64,
    /// Go `StartTimestamp`.
    pub start_timestamp: i64,
}

/// Go `int64KeyToBinaryBytes`: the key's big-endian 8 bytes.
#[must_use]
pub fn int64_key_to_binary_bytes(key: i64) -> [u8; 8] {
    (key as u64).to_be_bytes()
}

/// The key types Go's `Available(key any)` supports.
#[derive(Clone, Copy, Debug)]
pub enum AvailableKey<'a> {
    /// A string key.
    Str(&'a str),
    /// A byte key.
    Bytes(&'a [u8]),
    /// An int64 key, hashed by its fixed-width binary encoding.
    Int64(i64),
}

/// Go `ServerDiscovery`: server discovery for the consistent-hash view.
pub trait ServerDiscovery: Send + Sync {
    /// Go `serverFilter`.
    fn server_filter(&self, server: &ServerInfo) -> bool;
    /// Go `getServerInfo`.
    ///
    /// # Errors
    /// The local server's identity is not available yet.
    fn get_server_info(&self) -> Result<ServerInfo, String>;
    /// Go `getAllServerInfo`.
    ///
    /// # Errors
    /// The cluster's servers could not be listed.
    fn get_all_server_info(&self, ctx: &Context) -> Result<HashMap<String, ServerInfo>, String>;
}

/// The state Go's `ServerConsistentHash.mu` guards.
pub(crate) struct HashState {
    pub(crate) servers: HashMap<String, ServerInfo>,
    pub(crate) server_ids: Vec<String>,
    /// Go `chash`: consistent hash to reduce task movements after nodes change.
    pub(crate) chash: ConsistentHash,
    /// Go `ID`: the current server's exec ID.
    pub(crate) id: String,
}

/// Go `ServerConsistentHash`: TiDB membership and ownership by exec ID.
pub struct ServerConsistentHash {
    ctx: Context,
    pub(crate) mu: RwLock<HashState>,
    helper: Arc<dyn ServerDiscovery>,
}

impl ServerConsistentHash {
    /// Go `NewServerConsistentHash`.
    #[must_use]
    pub fn new(ctx: Context, replicas: i64, helper: Arc<dyn ServerDiscovery>) -> Self {
        Self {
            ctx,
            mu: RwLock::new(HashState {
                servers: HashMap::new(),
                server_ids: Vec::new(),
                chash: ConsistentHash::new(replicas),
                id: String::new(),
            }),
            helper,
        }
    }

    pub(crate) fn rebuild_locked(state: &mut HashState, servers: HashMap<String, ServerInfo>) {
        state.server_ids = servers.keys().filter(|id| !id.is_empty()).cloned().collect();
        state.server_ids.sort();
        state.chash.rebuild(&servers);
        state.servers = servers;
    }

    /// Go `ID`.
    #[must_use]
    pub fn id(&self) -> String {
        self.mu.read().unwrap().id.clone()
    }

    /// Go `init`: loads the local server info with retry and sets the exec ID.
    pub(crate) fn init(&self) -> bool {
        const SECOND: i64 = tidb_timer::go_time::SECOND;
        let mut backoff = SECOND;
        let mut attempt = 0i64;
        loop {
            attempt += 1;
            // Retry until local server identity is available; otherwise this node
            // cannot be part of the hash ring and will receive no tasks.
            match self.helper.get_server_info() {
                Ok(info) => {
                    self.mu.write().unwrap().id = info.id;
                    return true;
                }
                Err(err) => {
                    let wait_dur = backoff.min(5 * SECOND);
                    bg_logger().warn(
                        "get local TiDB server info failed, retrying after backoff",
                        &[
                            Field::new("attempt", Value::I64(attempt)),
                            Field::new("backoff", Value::Duration(wait_dur)),
                            Field::new("error", Value::Str(err)),
                        ],
                    );
                    crossbeam_channel::select! {
                        recv(self.ctx.done()) -> _ => {
                            bg_logger().warn(
                                "stop initializing server consistent hash because context is done",
                                &[
                                    Field::new("attempt", Value::I64(attempt)),
                                    Field::new(
                                        "context_error",
                                        Value::Str(self.ctx.err().map(|err| err.to_string()).unwrap_or_default()),
                                    ),
                                ],
                            );
                            return false;
                        }
                        recv(mvs_after(wait_dur)) -> _ => backoff = wait_dur * 2,
                    }
                }
            }
        }
    }

    /// Go `refresh`: reloads membership and rebuilds the ring when it
    /// changed; answers whether the membership set changed.
    pub(crate) fn refresh(&self) -> Result<bool, String> {
        let old_srv_cnt = self.mu.read().unwrap().servers.len();
        let mut new_infos = match self.helper.get_all_server_info(&self.ctx) {
            Ok(infos) => infos,
            Err(err) => {
                bg_logger().warn(
                    "get available TiDB nodes failed",
                    &[
                        Field::new("old_server_count", Value::I64(old_srv_cnt as i64)),
                        Field::new(
                            "context_error",
                            Value::Str(self.ctx.err().map(|err| err.to_string()).unwrap_or_default()),
                        ),
                        Field::new("error", Value::Str(err.clone())),
                    ],
                );
                return Err(err);
            }
        };
        // Filter servers by helper policy.
        new_infos.retain(|_, info| self.helper.server_filter(info));
        {
            // Return early when there is no membership change.
            let state = self.mu.read().unwrap();
            let unchanged =
                state.servers.len() == new_infos.len() && new_infos.keys().all(|id| state.servers.contains_key(id));
            if unchanged {
                return Ok(false);
            }
        }
        let mut state = self.mu.write().unwrap();
        Self::rebuild_locked(&mut state, new_infos);
        Ok(true)
    }

    /// Go `Available`: whether this server owns `key` (string, bytes or int64).
    #[must_use]
    pub fn available(&self, key: AvailableKey<'_>) -> bool {
        match key {
            AvailableKey::Str(key) => self.available_string(key),
            AvailableKey::Bytes(key) => self.available_bytes(key),
            AvailableKey::Int64(key) => self.available_int64(key),
        }
    }

    /// Go `AvailableString`: whether this server owns the string key.
    #[must_use]
    pub fn available_string(&self, key: &str) -> bool {
        self.available_bytes(key.as_bytes())
    }

    /// Go `AvailableBytes`: whether this server owns the byte key.
    #[must_use]
    pub fn available_bytes(&self, key: &[u8]) -> bool {
        let state = self.mu.read().unwrap();
        state.chash.get_node(key) == state.id
    }

    /// Go `AvailableInt64`: whether this server owns the int64 key, hashed
    /// by its fixed-width binary encoding.
    #[must_use]
    pub fn available_int64(&self, key: i64) -> bool {
        self.available_bytes(&int64_key_to_binary_bytes(key))
    }
}
