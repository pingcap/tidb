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

//! Where a client connected from, as the session records it (Go
//! `clientConn.peerHost` / `RemoteAddr` and `isUnixSocket`, which the server
//! hands the session at `OpenCtx`).

use std::net::SocketAddr;

/// Where a client connected from: Go's `RemoteAddr`, with `isUnixSocket`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerAddress {
    /// A TCP peer.
    Tcp(SocketAddr),
    /// A Unix-domain socket peer, which has no address.
    Unix,
}

impl PeerAddress {
    /// Go `clientConn.PeerHost`: the peer's IP, and `localhost`
    /// (`vardef.DefHostname`) for a Unix-domain socket.
    #[must_use]
    pub fn host(&self) -> String {
        match self {
            Self::Tcp(address) => address.ip().to_string(),
            Self::Unix => tidb_vardef::defaults::DEF_HOSTNAME.to_owned(),
        }
    }

    /// Go `RemoteAddr().String()`: `ip:port`, and an unnamed Unix peer's
    /// empty name.
    #[must_use]
    pub fn remote_addr(&self) -> String {
        match self {
            Self::Tcp(address) => address.to_string(),
            Self::Unix => String::new(),
        }
    }

    /// The process list's `Host`: Go's `peerHost:peerPort`, and a Unix
    /// socket's `localhost`, which has no port.
    #[must_use]
    pub fn process_host(&self) -> String {
        match self {
            Self::Tcp(address) => address.to_string(),
            Self::Unix => tidb_vardef::defaults::DEF_HOSTNAME.to_owned(),
        }
    }

    /// Go `clientConn.isUnixSocket`.
    #[must_use]
    pub const fn is_unix_socket(&self) -> bool {
        matches!(self, Self::Unix)
    }
}

impl From<SocketAddr> for PeerAddress {
    fn from(address: SocketAddr) -> Self {
        Self::Tcp(address)
    }
}

impl std::fmt::Display for PeerAddress {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.remote_addr())
    }
}
