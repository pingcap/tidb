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

//! The cluster tier below the executor: Go's infoschema loader and syncer
//! over TiKV meta (`pkg/infoschema`, `pkg/domain`'s schema validator),
//! `pkg/meta/autoid`'s cluster half, the `mysql.*` system tables Go reaches
//! through internal SQL, and the transaction helpers they share. Go's
//! `pkg/ddl` is `tidb-ddl`, above this crate; `tidb-exec` re-exports every
//! module for its callers.

pub mod auto_id_client;
pub mod catalog_reload;
pub mod catalog_watch;
pub mod cluster_account_write;
pub mod cluster_auto_id;
pub mod cluster_catalog;
pub use tidb_txnkv::cluster_http;
pub mod cluster_privilege_load;
pub mod cluster_sysvar_load;
pub mod cluster_sysvar_write;
pub mod cluster_table_storage;
pub mod mdl_info_load;
pub mod mysql_system_tables;
pub mod pessimistic_lock_error;
pub mod real_tikv_catalog;
pub mod real_tikv_read;
pub mod session_commit_protocol;
pub mod sql_query_error;
pub mod system_row_write;

