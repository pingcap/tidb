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

//! What the executor's instance-wide memory-table readers read of this
//! session (`viewer_state`: the process registry, the viewer's privileges,
//! the node's cluster clients), and the SQL-digest text retriever -- the
//! state Go's readers take from `sctx`.
//!
//! This is distinct from the `process` module, which owns the process
//! registry and kill-target trait a server front end wires in.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use crate::*;
use tidb_executor::memtable_reader::SqlDigestTextRetriever;

impl Session {
    // Go `SimpleExec.executeKillStmt`.
    /// [`Self::sql_digest_text_retriever`] as `TIDB_DECODE_SQL_DIGESTS` runs
    /// it from the statement context (Go `RetrieveGlobal`).
    pub(crate) fn sql_digest_text_retrieval(&self) -> tidb_sessionctx::SqlDigestTextRetrieval {
        let retriever = self.sql_digest_text_retriever();
        std::sync::Arc::new(move |ctx, digests| {
            retriever
                .retrieve(ctx, digests.iter().cloned(), true)
                .map_err(|error| error.to_string())
        })
    }

    /// An owned [`SqlDigestTextRetriever`] over this session's node, for a
    /// builtin that retrieves while evaluating with the session out of reach.
    pub(crate) fn sql_digest_text_retriever(&self) -> SqlDigestTextRetriever {
        SqlDigestTextRetriever {
            zone: self.session_time_zone(),
            scan_concurrency: self.dist_sql_scan_concurrency(),
            server_info_syncer: self.server_info_syncer.clone(),
            cluster_peer: self.cluster_peer.clone(),
        }
    }

    /// The `User` column: Go reports the bare user name, while this session
    /// stores the login identity as `user@host`.
    pub(crate) fn process_list_user(&self) -> String {
        match &self.vars.login_user {
            Some(user) => user.split('@').next().unwrap_or_default().to_owned(),
            None => String::new(),
        }
    }

    /// What the instance-wide `information_schema` tables read of this
    /// session: see [`tidb_executor::memtable_reader::ViewerState`].
    pub(crate) fn viewer_state(&self) -> tidb_executor::memtable_reader::ViewerState {
        tidb_executor::memtable_reader::ViewerState {
            has_process: self.has_process_privilege(),
            registry: self.process.as_ref().map(|guard| guard.registry().clone()),
            connection_id: self.vars.connection_id.unwrap_or(0),
            own_process_id: self.process.as_ref().map(|guard| guard.id()),
            process_user: self.process_list_user(),
            current_db: self.vars.current_db.clone(),
            status: self.status_text(),
            resource_group: self.active_resource_group.clone(),
            login_user: self.vars.login_user.clone(),
            auth_identity: self
                .current_identity()
                .map(|(user, host)| (user.to_owned(), host.to_owned())),
            scan_concurrency: self.dist_sql_scan_concurrency(),
            visibility: self.schema_visibility(),
            index_usage: std::sync::Arc::clone(&self.index_usage_collector),
            process_scoped: self.has_scoped_privilege("", "", tidb_mysql::ProcessPriv),
            restricted_sql: self.restricted_sql_executor(),
            slow_query_file: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_SLOW_QUERY_FILE)
                .unwrap_or_default(),
            ddl_job_history_store: self.ddl_job_history_store.clone(),
            analyze_status: self.analyze_status.clone(),
            digest_retriever: self.sql_digest_text_retriever(),
            data_lock_waits: self.data_lock_waits.clone(),
            cluster_topology: self.cluster_topology.clone(),
            cluster_config: self.cluster_config.clone(),
            cluster_diagnostics: self.cluster_diagnostics.clone(),
            cluster_peer: self.cluster_peer.clone(),
            server_info_syncer: self.server_info_syncer.clone(),
            config_scoped: self.has_scoped_privilege("", "", tidb_mysql::ConfigPriv),
            hides_cluster_metadata: self.sem_hides_cluster_metadata(),
        }
    }

}

impl Session {
    /// Whether this session may inspect process-wide diagnostic state.
    pub(crate) fn has_process_privilege(&self) -> bool {
        self.privilege_checks_bypassed()
            || self.has_process_priv
            || self.privileges.as_ref().is_some_and(|registry| {
                self.current_identity().is_some_and(|(user, host)| {
                    registry.has_global_priv_with_roles(
                        user,
                        host,
                        self.active_roles(),
                        tidb_mysql::ProcessPriv,
                    )
                })
            })
            || self.vars.login_user.is_none()
    }

}
