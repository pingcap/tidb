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

//! The `SHOW [FULL] PROCESSLIST` / `information_schema.PROCESSLIST` rows and
//! `KILL`: the arms `Session::dispatch_admin_stmt` delegates to, plus the
//! helpers `Session::retrieve_memory_table` (in `dispatch.rs`) calls to build the
//! virtual `PROCESSLIST` table.
//!
//! This is distinct from the `process` module, which owns the process
//! registry and kill-target trait a server front end wires in; this file is
//! the Session-side use of that registry.

use crate::*;

impl Session {
    // Go `SimpleExec.executeKillStmt`.
    pub(crate) fn kill_stmt(
        &mut self,
        kill: &tidb_ast::KillStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        const INVALID_OPERATION: &str = "Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] [connectionID | CONNECTION_ID()]' instead";
        let registry = self.process.as_ref().map(|guard| guard.registry().clone());
        let target = match &kill.target {
            tidb_ast::KillTarget::ConnectionId(id) => *id,
            tidb_ast::KillTarget::Expr(tidb_ast::Expr::Func { name, args, .. })
                if name.eq_ignore_ascii_case("connection_id") && args.is_empty() =>
            {
                // Go executes this local expression before numeric ID/config gates.
                if let Some(registry) = registry {
                    registry.kill(self.vars.connection_id.unwrap_or(0), kill.query);
                }
                return Ok(Some(StmtOutput::Affected(0)));
            }
            tidb_ast::KillTarget::Expr(_) => {
                return Err(DriverError::unsupported(INVALID_OPERATION));
            }
        };

        let config = tidb_config::config_tree::config::get_global_config();
        if !config.enable_global_kill {
            if kill.tidb_extension || config.compatible_kill_query {
                if let Some(registry) = registry {
                    registry.kill(target, kill.query);
                }
            } else {
                self.append_warning(
                    crate::WarningLevel::Warning,
                    1105,
                    INVALID_OPERATION.to_owned(),
                );
            }
            return Ok(Some(StmtOutput::Affected(0)));
        }
        let Some(registry) = registry else {
            return Ok(Some(StmtOutput::Affected(0)));
        };
        match tidb_util::globalconn::parse_conn_id(target) {
            Err(error) => {
                self.append_warning(
                    crate::WarningLevel::Warning,
                    1105,
                    format!("Parse ConnectionID failed: {error}"),
                );
            }
            Ok((_, true)) => {
                self.append_warning(crate::WarningLevel::Warning, 1105, "Kill failed: Received a 32bits truncated ConnectionID, expect 64bits. Please execute 'KILL [CONNECTION | QUERY] ConnectionID' to send a Kill without truncating ConnectionID.".to_owned());
            }
            Ok((id, false)) => {
                if id.server_id == (self.server_id_getter)() {
                    registry.kill(target, kill.query);
                } else if let Err(error) = self.kill_remote_connection(id, kill.query) {
                    self.append_warning(
                        WarningLevel::Warning,
                        1105,
                        format!("KILL remote connection failed: {error}"),
                    );
                }
            }
        }
        Ok(Some(StmtOutput::Affected(0)))
    }

    fn kill_remote_connection(
        &self,
        id: tidb_util::globalconn::Gcid,
        query: bool,
    ) -> Result<(), String> {
        if id.server_id == 0 {
            return Err("Unexpected ZERO ServerID. Please file a bug to the TiDB Team".into());
        }
        let client = self
            .cluster_peer
            .as_ref()
            .ok_or("TiDB peer RPC client is not installed")?;
        let syncer = self
            .server_info_syncer
            .as_ref()
            .ok_or("TiDB peer discovery is not installed")?;
        let servers: Vec<_> = syncer.all_server_info()?.into_values().collect();
        client.kill(
            &servers,
            id,
            query,
            &self.statement_context(false),
            &self.session_time_zone(),
        )
    }

    /// [`Self::sql_digest_text_retriever`] as `TIDB_DECODE_SQL_DIGESTS` runs
    /// it from the statement context (Go `RetrieveGlobal`).
    pub(crate) fn sql_digest_text_retrieval(&self) -> tidb_executor::SqlDigestTextRetrieval {
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

    /// The rows of `SHOW [FULL] PROCESSLIST`.
    ///
    /// With a server front end this is the whole live connection list. A
    /// session with NO front end (in-process tests, the embedded driver) has
    /// no peers to report, so it lists exactly one row: itself, with the
    /// values it honestly knows -- its own connection id (0 when the front
    /// end never assigned one), no client host, its current schema, and the
    /// statement it is running, which is this SHOW.
    ///
    /// Filtered by the `PROCESS` privilege the same way Go's
    /// `setDataForProcessList` / `fetchShowProcessList` both filter: a
    /// session without it sees only its own connections.
    /// Go `ShowExec.fetchShowProcessList`: one row per connection the
    /// session may see (`FULL` keeps the statement text whole).
    pub(crate) fn process_list_rows(&self, full: bool) -> Vec<Vec<Datum>> {
        let rows = self.visible_process_rows(full);
        let nullable_text = |value: String| {
            if value.is_empty() {
                Datum::Null
            } else {
                Datum::Bytes(value.into_bytes())
            }
        };
        rows
                .into_iter()
                .map(|row| {
                    vec![
                        Datum::UInt(row.id),
                        Datum::Bytes(row.user.into_bytes()),
                        Datum::Bytes(row.host.into_bytes()),
                        // Go reports an unselected schema as SQL NULL.
                        nullable_text(row.db),
                        Datum::Bytes(row.command.into_bytes()),
                        Datum::Int(i64::try_from(row.time).unwrap_or(i64::MAX)),
                        Datum::Bytes(row.state.into_bytes()),
                        // Go reports an idle connection's statement as NULL,
                        // and truncates a running one to 100 runes without
                        // FULL.
                        // The text carries a byte that is not UTF-8 as its
                        // `raw_bytes` escape; Go's string holds the byte.
                        match row.info {
                            Some(info) => Datum::Bytes(
                                tidb_hack::raw_bytes::str_to_bytes(
                                    &process::truncate_process_info(&info, full),
                                )
                                .into_owned(),
                            ),
                            None => Datum::Null,
                        },
                    ]
                })
            .collect()
    }

    /// The `User` column: Go reports the bare user name, while this session
    /// stores the login identity as `user@host`.
    pub(crate) fn process_list_user(&self) -> String {
        match &self.vars.login_user {
            Some(user) => user.split('@').next().unwrap_or_default().to_owned(),
            None => String::new(),
        }
    }

    /// Every connection this session is allowed to see for `SHOW
    /// PROCESSLIST` / `information_schema.PROCESSLIST`.
    ///
    /// Go (`setDataForProcessList`, `fetchShowProcessList`): "If you have the
    /// PROCESS privilege, you can see all threads. Otherwise, you can see
    /// only your own threads" -- and an internal session with no login user
    /// is not filtered at all, since there is nothing to compare against.
    pub(crate) fn visible_process_rows(&self, full: bool) -> Vec<process::ProcessRow> {
        self.viewer_state().visible_process_rows(full)
    }

    /// What the instance-wide `information_schema` tables read of this
    /// session: see [`crate::instance_state::ViewerState`].
    pub(crate) fn viewer_state(&self) -> crate::instance_state::ViewerState {
        crate::instance_state::ViewerState {
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

/// Go `ProcessInfo.ToRow` for each row: the columns of
/// `information_schema.PROCESSLIST`.
pub(crate) fn format_process_rows(
    rows: Vec<process::ProcessRow>,
    zone: &tidb_datatype::SessionTimeZone,
) -> Vec<Vec<Datum>> {
    rows.into_iter()
        .map(|row| {
            let txn_start = if row.cur_txn_start_ts == 0 {
                String::new()
            } else {
                let time = tidb_expr::sessionexpr::get_time_from_ts(row.cur_txn_start_ts)
                    .with_timezone(zone);
                format!(
                    "{}({})",
                    time.format("%m-%d %H:%M:%S%.3f"),
                    row.cur_txn_start_ts
                )
            };
            vec![
                Datum::UInt(row.id),
                Datum::Bytes(row.user.into_bytes()),
                Datum::Bytes(row.host.into_bytes()),
                if row.db.is_empty() {
                    Datum::Null
                } else {
                    Datum::Bytes(row.db.into_bytes())
                },
                Datum::Bytes(row.command.into_bytes()),
                Datum::Int(i64::try_from(row.time).unwrap_or(i64::MAX)),
                if row.state.is_empty() {
                    Datum::Null
                } else {
                    Datum::Bytes(row.state.into_bytes())
                },
                match row.info {
                    Some(info) => {
                        Datum::Bytes(tidb_hack::raw_bytes::str_to_bytes(&info).into_owned())
                    }
                    None => Datum::Null,
                },
                Datum::Bytes(row.digest.into_bytes()),
                Datum::Int(row.mem_bytes),
                // MEM_ARBITRATION
                Datum::Null,
                // MEM_WAIT_ARBITRATE_START
                Datum::Null,
                // MEM_WAIT_ARBITRATE_BYTES
                Datum::Null,
                Datum::Int(row.disk_bytes),
                Datum::Bytes(txn_start.into_bytes()),
                Datum::Bytes(row.resource_group.into_bytes()),
                Datum::Bytes(row.session_alias.into_bytes()),
                row.affected_rows.map_or(Datum::Null, Datum::UInt),
                // TIDB_CPU
                Datum::Int(0),
                // TIKV_CPU
                Datum::Int(0),
            ]
        })
        .collect()
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

/// Go `expression.SQLDigestTextRetriever`: the normalized statement text of
/// each requested digest, read the way Go's `ExecRestrictedSQL` reads it --
/// from this node's `statements_summary` and its history, then, for
/// `RetrieveGlobal`, from the peers' cluster tables for the digests still
/// unknown. Owned, so `TIDB_DECODE_SQL_DIGESTS` can retrieve while it
/// evaluates.
#[derive(Clone)]
pub(crate) struct SqlDigestTextRetriever {
    zone: tidb_datatype::SessionTimeZone,
    scan_concurrency: usize,
    server_info_syncer: Option<std::sync::Arc<tidb_domain::serverinfo_syncer::Syncer>>,
    cluster_peer: Option<std::sync::Arc<tidb_exec::cluster_peer::ClusterPeerClient>>,
}

impl SqlDigestTextRetriever {
    /// Go `RetrieveLocal`, then with `global` `RetrieveGlobal`'s peer pass.
    /// A digest the summaries do not know maps to the empty text.
    pub(crate) fn retrieve(
        &self,
        ctx: &tidb_executor::StmtContext,
        digests: impl IntoIterator<Item = String>,
        global: bool,
    ) -> Result<std::collections::HashMap<String, String>, DriverError> {
        use std::collections::{HashMap, HashSet};
        let mut texts: HashMap<String, String> = digests
            .into_iter()
            .map(|digest| (digest, String::new()))
            .collect();
        if texts.is_empty() {
            return Ok(texts);
        }
        let requested: HashSet<_> = texts.keys().cloned().collect();
        // Go limits the WHERE IN list to512 digests, otherwise fetching all.
        let filter = (requested.len() <= 512).then_some(&requested);
        let columns = tidb_executor::infoschema_reader::table_schema("STATEMENTS_SUMMARY")
            .expect("summary schema");
        let columns: Vec<_> = ["DIGEST", "DIGEST_TEXT"]
            .iter()
            .map(|name| {
                columns
                    .iter()
                    .find(|(column, _)| column == name)
                    .expect("digest column")
                    .clone()
            })
            .collect();
        fn merge(texts: &mut HashMap<String, String>, rows: Vec<Vec<Datum>>) {
            for row in rows {
                let (Some(digest), Some(text)) = (row[0].as_raw_bytes(), row[1].as_raw_bytes())
                else {
                    continue;
                };
                if let Some(known) = texts.get_mut(String::from_utf8_lossy(digest).as_ref()) {
                    if known.is_empty() {
                        *known = String::from_utf8_lossy(text).into_owned();
                    }
                }
            }
        }
        for table in ["STATEMENTS_SUMMARY", "STATEMENTS_SUMMARY_HISTORY"] {
            merge(
                &mut texts,
                crate::dispatch::statement_summary_rows(
                    table,
                    &columns,
                    &self.zone,
                    None,
                    true,
                    filter,
                    self.scan_concurrency,
                )?,
            );
        }
        let unknown: HashSet<_> = texts
            .iter()
            .filter(|(_, text)| text.is_empty())
            .map(|(digest, _)| digest.clone())
            .collect();
        if !global || unknown.is_empty() {
            return Ok(texts);
        }
        let filter = (requested.len() <= 512).then_some(&unknown);
        for (table, source) in [
            ("CLUSTER_STATEMENTS_SUMMARY", "STATEMENTS_SUMMARY"),
            (
                "CLUSTER_STATEMENTS_SUMMARY_HISTORY",
                "STATEMENTS_SUMMARY_HISTORY",
            ),
        ] {
            let schema = tidb_executor::infoschema_reader::table_schema(table)
                .expect("cluster summary schema");
            let offsets: Vec<_> = ["DIGEST", "DIGEST_TEXT"]
                .iter()
                .map(|name| {
                    schema
                        .iter()
                        .position(|(column, _)| column == name)
                        .expect("digest column")
                })
                .collect();
            let local = crate::dispatch::statement_summary_rows(
                source,
                &columns,
                &self.zone,
                None,
                true,
                filter,
                self.scan_concurrency,
            )?;
            // Go reads the peers through `ExecRestrictedSQL`, whose warnings
            // stay on the internal session.
            let (rows, _) = peer_cluster_table_rows(
                self.server_info_syncer.as_deref(),
                self.cluster_peer.as_deref(),
                ctx,
                None,
                &self.zone,
                table,
                &schema,
                &offsets,
                local,
                filter,
            )?;
            merge(&mut texts, rows);
        }
        Ok(texts)
    }
}

/// The rows the discovered peers answer for a cluster memory table, after
/// `rows` (this node's own, dropped when discovery does not list this node).
/// Without a server-info syncer there are no peers and `rows` is the answer.
/// The second value is the warnings the peers reported.
#[allow(clippy::too_many_arguments)]
pub(crate) fn peer_cluster_table_rows(
    syncer: Option<&tidb_domain::serverinfo_syncer::Syncer>,
    peer: Option<&tidb_exec::cluster_peer::ClusterPeerClient>,
    ctx: &tidb_executor::StmtContext,
    user: Option<(&str, &str)>,
    zone: &tidb_datatype::SessionTimeZone,
    table_name: &str,
    columns: &[(String, FieldType)],
    offsets: &[usize],
    mut rows: Vec<Vec<Datum>>,
    digests: Option<&std::collections::HashSet<String>>,
) -> Result<(Vec<Vec<Datum>>, Vec<(u16, String)>), DriverError> {
    let Some(syncer) = syncer else {
        return Ok((rows, Vec::new()));
    };
    let local_id = syncer.local_server_info().static_info.id;
    let discovered = syncer.all_server_info().map_err(DriverError::unsupported)?;
    if !discovered
        .values()
        .any(|server| server.static_info.id == local_id && server.static_info.ip != "<nil>")
    {
        rows.clear();
    }
    let servers: Vec<_> = discovered
        .into_values()
        .filter(|server| server.static_info.id != local_id && server.static_info.ip != "<nil>")
        .collect();
    if servers.is_empty() {
        return Ok((rows, Vec::new()));
    }
    let client =
        peer.ok_or_else(|| DriverError::unsupported("TiDB peer RPC client is not installed"))?;
    let result = client.scan_projected(
        &servers,
        tidb_executor::infoschema_reader::memory_table_id(table_name)
            .expect("registered memory table"),
        columns,
        offsets,
        user,
        ctx,
        zone,
        ctx.dist_sql_scan_concurrency() as usize,
        digests,
    )?;
    rows.extend(result.rows);
    Ok((rows, result.warnings))
}
