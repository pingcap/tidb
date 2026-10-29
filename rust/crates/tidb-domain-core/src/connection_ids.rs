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

//! Go `Domain.connIDAllocator`.

use std::sync::Arc;

use tidb_globalconn::{Allocator, GlobalAllocator, SimpleAllocator};

/// Go `Domain`'s connection-ID allocator (`domain.go`, "initialize the global
/// kill"): with global kill on, a global allocator whose IDs name this node's
/// server ID -- the leased one, or `serverIDForStandalone` without etcd --
/// so `KILL` can route by ID; otherwise the simple allocator. One instance
/// serves a node: client connections and internal process IDs such as
/// auto-analyze's (Go `NextConnID`) draw from it.
pub fn new_connection_id_allocator(
    config: &tidb_config::config_tree::Config,
    identity: Option<Arc<tidb_domain::server_id::ServerIdAuthority>>,
) -> Arc<dyn Allocator + Send + Sync> {
    if config.enable_global_kill {
        let owner = identity;
        Arc::new(GlobalAllocator::new(
            move || {
                owner
                    .as_ref()
                    .map_or(tidb_globalconn::SERVER_ID_FOR_STANDALONE, |owner| owner.id())
            },
            config.enable_32bits_connection_id,
        ))
    } else {
        Arc::new(SimpleAllocator::new())
    }
}
