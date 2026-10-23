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

//! Go's `memTableRetriever`s for the `information_schema` tables that read
//! instance-wide state through the viewing session's identity: the process
//! list, the transaction and statement summaries, the client-error summaries,
//! the memory-usage tables and the index-usage table.
//!
//! The executor owns these retrievers (a scan calls `retrieve` at its first
//! `Next`), so what they need of the viewing session is a [`ViewerState`]
//! the session snapshots when a scan first asks for one of them.

use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_executor::mem_table_retrieval::{MemTableRead, MemTableRetriever};
use tidb_executor::{Catalog, DriverError};
use tidb_hack::GoCaseCompare;

use crate::infoschema::SchemaVisibility;
use crate::process;

/// What the instance-wide tables read of the session viewing them.
#[derive(Clone)]
pub(crate) struct ViewerState {
    /// Go's `hasPriv(sctx, mysql.ProcessPriv)`, which `PROCESS` privileged
    /// viewers and internal sessions pass.
    pub(crate) has_process: bool,
    /// The instance's connection registry, absent for a session no server
    /// front end owns.
    pub(crate) registry: Option<process::ProcessRegistry>,
    /// This connection's id, user name, current schema, status text and
    /// resource group, which stand for it when there is no registry.
    pub(crate) connection_id: u64,
    pub(crate) process_user: String,
    pub(crate) current_db: String,
    pub(crate) status: String,
    pub(crate) resource_group: String,
    /// The login (`user@host`) and the authenticated account.
    pub(crate) login_user: Option<String>,
    pub(crate) auth_identity: Option<(String, String)>,
    /// `@@tidb_distsql_scan_concurrency`, the statement-summary history
    /// reader's worker count.
    pub(crate) scan_concurrency: usize,
    pub(crate) visibility: SchemaVisibility,
    pub(crate) index_usage: Arc<tidb_stats_handle_usage_indexusage::Collector>,
}

impl ViewerState {
    /// Go `setDataForProcessList`: "If you have the PROCESS privilege, you
    /// can see all threads. Otherwise, you can see only your own threads";
    /// an internal session with no login user is not filtered at all.
    pub(crate) fn filter_process_rows(
        &self,
        rows: Vec<process::ProcessRow>,
    ) -> Vec<process::ProcessRow> {
        if self.has_process {
            return rows;
        }
        rows.into_iter()
            .filter(|row| row.user == self.process_user)
            .collect()
    }

    /// Every connection this session is allowed to see for `SHOW
    /// PROCESSLIST` / `information_schema.PROCESSLIST`.
    pub(crate) fn visible_process_rows(&self, full: bool) -> Vec<process::ProcessRow> {
        let rows: Vec<process::ProcessRow> = match &self.registry {
            Some(registry) => registry.snapshot(),
            None => vec![process::ProcessRow {
                id: self.connection_id,
                user: self.process_user.clone(),
                host: String::new(),
                db: self.current_db.clone(),
                command: "Query".to_owned(),
                time: 0,
                state: self.status.clone(),
                info: Some(if full {
                    "show full processlist".to_owned()
                } else {
                    "show processlist".to_owned()
                }),
                resource_group: self.resource_group.clone(),
                ..process::ProcessRow::default()
            }],
        };
        self.filter_process_rows(rows)
    }

    /// Go `setDataForTrxSummary`: the transaction history, for a viewer that
    /// holds PROCESS.
    pub(crate) fn transaction_summary_rows(
        &self,
        registry: Option<&process::ProcessRegistry>,
    ) -> Vec<Vec<Datum>> {
        if !self.has_process {
            return Vec::new();
        }
        registry.or(self.registry.as_ref()).map_or_else(
            || tidb_exec::txn_summary::RECORDER.rows(),
            |registry| registry.transaction_history_rows(),
        )
    }

    /// Go `stmtSummaryRetriever`: the statement summary tables for this
    /// viewer, or every user's rows for an `internal` reader.
    pub(crate) fn statement_summary_rows(
        &self,
        table_name: &str,
        columns: &[(String, tidb_datatype::FieldType)],
        zone: &tidb_datatype::SessionTimeZone,
        internal: bool,
        digests: Option<&std::collections::HashSet<String>>,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        use tidb_parser::auth::UserIdentity;
        let has_process = internal || self.has_process;
        let user = self
            .login_user
            .as_deref()
            .filter(|_| !internal)
            .map(|login| {
                let (username, hostname) = login.split_once('@').unwrap_or((login, ""));
                let (auth_username, auth_hostname) = self
                    .auth_identity
                    .as_ref()
                    .map_or(("", ""), |(user, host)| (user.as_str(), host.as_str()));
                UserIdentity {
                    username: username.to_owned(),
                    hostname: hostname.to_owned(),
                    current_user: false,
                    auth_username: auth_username.to_owned(),
                    auth_hostname: auth_hostname.to_owned(),
                    auth_plugin: String::new(),
                }
            });
        crate::dispatch::statement_summary_rows(
            table_name,
            columns,
            zone,
            user,
            has_process,
            digests,
            self.scan_concurrency,
        )
    }
}

/// The retriever for the tables [`ViewerState`] answers.
pub(crate) struct InstanceStateRetriever {
    viewer: ViewerState,
}

impl InstanceStateRetriever {
    pub(crate) fn new(viewer: ViewerState) -> Self {
        Self { viewer }
    }

    fn is_statement_summary(table: &str) -> bool {
        [
            "TIDB_STATEMENTS_STATS",
            "STATEMENTS_SUMMARY",
            "STATEMENTS_SUMMARY_HISTORY",
            "STATEMENTS_SUMMARY_EVICTED",
        ]
        .iter()
        .any(|name| table.go_lower_eq(name))
    }

    fn is_client_errors_summary(table: &str) -> bool {
        [
            "CLIENT_ERRORS_SUMMARY_GLOBAL",
            "CLIENT_ERRORS_SUMMARY_BY_USER",
            "CLIENT_ERRORS_SUMMARY_BY_HOST",
        ]
        .iter()
        .any(|name| table.go_lower_eq(name))
    }
}

impl MemTableRetriever for InstanceStateRetriever {
    fn serves(&self, read: &MemTableRead) -> bool {
        // The cluster tables (`CLUSTER_PROCESSLIST`, ...) fan out to peers and
        // are the session's.
        if read.is_cluster_scan
            || !read
                .db_name
                .go_lower_eq(tidb_executor::driver::infoschema_meta::INFORMATION_SCHEMA)
        {
            return false;
        }
        let table = read.table_name.as_str();
        [
            "PROCESSLIST",
            "TRX_SUMMARY",
            "MEMORY_USAGE",
            "MEMORY_USAGE_OPS_HISTORY",
            "TIDB_INDEX_USAGE",
        ]
        .iter()
        .any(|name| table.go_lower_eq(name))
            || Self::is_statement_summary(table)
            || Self::is_client_errors_summary(table)
    }

    fn retrieve(
        &self,
        read: &MemTableRead,
        catalog: &Catalog,
        ctx: &tidb_executor::StmtContext,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let table = read.table_name.as_str();
        if table.go_lower_eq("PROCESSLIST") {
            return Ok(crate::process_arm::format_process_rows(
                self.viewer.visible_process_rows(true),
                &ctx.session_zone(),
            ));
        }
        if table.go_lower_eq("TRX_SUMMARY") {
            return Ok(self.viewer.transaction_summary_rows(None));
        }
        if table.go_lower_eq("MEMORY_USAGE") {
            return Ok(crate::dispatch::memory_usage_table_rows());
        }
        if table.go_lower_eq("MEMORY_USAGE_OPS_HISTORY") {
            return Ok(tidb_util::servermemorylimit::GLOBAL_MEMORY_OPS_HISTORY_MANAGER.get_rows());
        }
        if table.go_lower_eq("TIDB_INDEX_USAGE") {
            return Ok(crate::infoschema::tidb_index_usage_rows(
                catalog,
                &self.viewer.visibility,
                self.viewer.index_usage.as_ref(),
            ));
        }
        if Self::is_client_errors_summary(table) {
            return crate::Session::client_errors_summary_rows(
                table,
                self.viewer.has_process,
                self.viewer.login_user.as_deref(),
            );
        }
        let columns = crate::infoschema::table_schema(table).ok_or_else(|| {
            DriverError::unsupported("a statement summary table has no declared schema")
        })?;
        self.viewer
            .statement_summary_rows(table, &columns, &ctx.session_zone(), false, None)
    }
}
