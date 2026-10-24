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

//! Fixtures the server's tests of the domain share: a unistore stack, a
//! session context and a statement runner, as the domain's own tests build
//! them (`tidb_session::domain::cluster_session_node::tests::node_fixture`).

use std::net::SocketAddr;
use std::sync::Arc;

use sha1::{Digest, Sha1};
use tidb_datatype::Datum;
use tidb_session::domain::cluster_session_node::ClusterServerSession;
use tidb_session::domain::configured_user_store::ConfiguredUserStore;
use tidb_session::domain::resultset_source::ResultSetSource;
use tidb_session::domain::session_api::{
    ConnectionCancellation, ConnectionClose, QuerySession, SessionContext,
};
use tidb_session::domain::unistore_node::{unistore_cluster_session_stack, UnistoreClusterStack};

pub(crate) const ABC_HASH: &str = "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E";
pub(crate) const SALT: [u8; 20] = [7; 20];

pub(crate) fn scramble(password: &[u8], salt: &[u8]) -> [u8; 20] {
    let stage_one = Sha1::digest(password);
    let stage_two = Sha1::digest(stage_one);
    let mut hasher = Sha1::new();
    hasher.update(salt);
    hasher.update(stage_two);
    let challenge = hasher.finalize();
    let mut response = [0; 20];
    for ((destination, stage_one), challenge) in response
        .iter_mut()
        .zip(stage_one.iter())
        .zip(challenge.iter())
    {
        *destination = stage_one ^ challenge;
    }
    response
}

pub(crate) fn session_context(connection_id: u64) -> SessionContext {
    let users =
        ConfiguredUserStore::parse(&format!("root\t%\tmysql_native_password\t{ABC_HASH}\n"))
            .expect("configured user store");
    let identity = users
        .authenticate_native("root", "127.0.0.1", &SALT, &scramble(b"abc", &SALT))
        .expect("authenticated identity");
    session_context_with_identity(connection_id, identity)
}

pub(crate) fn session_context_with_identity(
    connection_id: u64,
    identity: tidb_session::domain::configured_user_store::AuthenticatedIdentity,
) -> SessionContext {
    let peer_addr: SocketAddr = "127.0.0.1:4000".parse().expect("peer address");
    SessionContext {
        connection_id,
        peer_addr,
        identity,
        client_found_rows: false,
        secure_transport: false,
        tls_status: None,
        cancellation: ConnectionCancellation::default(),
        close: ConnectionClose::default(),
    }
}

pub(crate) fn rows(session: &mut ClusterServerSession, sql: &str) -> Vec<Vec<Datum>> {
    let mut result = session.execute(sql).expect("the query runs");
    let source = result.source();
    let mut rows = Vec::new();
    loop {
        let batch = source.next_batch(8).expect("batch");
        if batch.is_empty() {
            break;
        }
        rows.extend(batch);
    }
    source.finish().expect("finish");
    source.close().expect("close");
    rows
}

pub(crate) fn cop_backed_stack() -> (UnistoreClusterStack, Arc<ConfiguredUserStore>) {
    cop_backed_stack_with_stats_lease(None)
}

pub(crate) fn cop_backed_stack_with_stats_lease(
    stats_lease: Option<tidb_session::domain::node_config::StatsLease>,
) -> (UnistoreClusterStack, Arc<ConfiguredUserStore>) {
    let mut config = tidb_session::domain::node_config::NodeConfig::parse([
        "tidb-server",
        "--store",
        "unistore",
        "--port",
        "0",
        // Parse-time requirement only: the test passes its own user store
        // below, so the flag never has to name real rows.
        "--auth-file",
        "/dev/null",
    ])
    .expect("node config");
    if let Some(stats_lease) = stats_lease {
        config.stats_lease = stats_lease;
    }
    let users = Arc::new(
        ConfiguredUserStore::parse(&format!("root\t%\tmysql_native_password\t{ABC_HASH}\n"))
            .expect("configured user store"),
    );
    let stack =
        unistore_cluster_session_stack(&config, &users, None, None).expect("unistore stack");
    (stack, users)
}
