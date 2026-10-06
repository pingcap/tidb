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

//! All topology-independent `tidb-server` integration tests in one process.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod auth_exchange_source;
mod auth_identity_source;
mod auth_plugin_registry_source;
mod concurrent_mysql_sessions_source;
mod configured_user_store_source;
mod distsql_streaming_response_source;
mod fallible_process_shutdown_source;
mod grants_wire_protocol_source;
mod handshake_response_package_source;
mod handshake_source;
mod hashing_plugin_auth_source;
mod initial_database_tcp_source;
mod mysql_client_lifecycle_source;
mod mysql_native_auth_lifecycle_source;
mod mysql_tls_source;
mod native_password_source;
mod node_config_source;
mod panic_recovery_source;
mod parse_go_source;
mod pipeline_mysql_client_source;
mod require_ssl_login_source;
mod resultset_writer_source;
mod secure_transport_source;
mod server_internal_packetio_source;
mod sql_node_lifecycle_source;
