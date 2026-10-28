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

use sha1::{Digest, Sha1};
use tidb_session::domain::configured_user_store::ConfiguredUserStore;
use tidb_session::domain::session_api::{
    ConnectionCancellation, ConnectionClose, SessionContext,
};

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
        peer_addr: peer_addr.into(),
        identity,
        client_found_rows: false,
        tls_state: None,
        compression_algorithm: tidb_mysql::consts::CompressionNone,
        compression_level: 0,
        cancellation: ConnectionCancellation::default(),
        close: ConnectionClose::default(),
    }
}

