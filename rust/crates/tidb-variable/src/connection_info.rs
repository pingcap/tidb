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

//! Go `pkg/sessionctx/variable/session.go`: connection information.

/// Go `ConnectionInfo`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConnectionInfo {
    /// Go `ConnectionID`.
    pub connection_id: u64,
    /// Go `ConnectionType`.
    pub connection_type: String,
    /// Go `Host`.
    pub host: String,
    /// Go `ClientIP`.
    pub client_ip: String,
    /// Go `ClientPort`.
    pub client_port: String,
    /// Go `ServerID`.
    pub server_id: i64,
    /// Go `ServerIP`.
    pub server_ip: String,
    /// Go `ServerPort`.
    pub server_port: i64,
    /// Go `Duration`.
    pub duration: f64,
    /// Go `User`.
    pub user: String,
    /// Go `ServerOSLoginUser`.
    pub server_os_login_user: String,
    /// Go `OSVersion`.
    pub os_version: String,
    /// Go `ClientVersion`.
    pub client_version: String,
    /// Go `ServerVersion`.
    pub server_version: String,
    /// Go `SSLVersion`.
    pub ssl_version: String,
    /// Go `PID`.
    pub pid: i64,
    /// Go `DB`.
    pub db: String,
    /// Go `AuthMethod`.
    pub auth_method: String,
    /// Go `Attributes`.
    pub attributes: std::collections::BTreeMap<String, String>,
}
/// Go `ConnTypeSocket`.
pub const CONN_TYPE_SOCKET: &str = "TCP";
/// Go `ConnTypeUnixSocket`.
pub const CONN_TYPE_UNIX_SOCKET: &str = "UnixSocket";
/// Go `ConnTypeTLS`.
pub const CONN_TYPE_TLS: &str = "SSL/TLS";
impl ConnectionInfo {
    /// Go `ConnectionInfo.IsSecureTransport`.
    pub fn is_secure_transport(&self) -> bool {
        matches!(
            self.connection_type.as_str(),
            CONN_TYPE_UNIX_SOCKET | CONN_TYPE_TLS
        )
    }
}
