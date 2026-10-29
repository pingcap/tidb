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

//! The status port's handlers over this node's domain: the peer coprocessor
//! service and the HTTP GLOBAL settings, assembled from what the domain's
//! session factory supplies.

use tidb_session::domain::cluster_session_node::DomainSessionSettings as _;
use std::sync::Arc;

use tidb_domain_core::domain::Domain;

/// The peer coprocessor service of this node (Go `tikvHandlerTool`'s
/// coprocessor over the node's own sessions).
pub(crate) fn peer_service(factory: &Arc<Domain>) -> crate::peer_rpc::PeerService {
    crate::peer_rpc::PeerService::new(
        factory.peer_session_builder(),
        factory.processes(),
        factory.privileges(),
        factory.server_info(),
    )
    .with_cluster_peer_client(factory.cluster_peer())
    .with_broadcast_query_runner(factory.broadcast_query_runner())
}

/// The HTTP GLOBAL settings handler of this node.
pub(crate) fn status_settings(factory: &Arc<Domain>) -> crate::http_settings::Settings {
    crate::http_settings::Settings::new(factory.global_vars(), factory.http_global_setter())
}
