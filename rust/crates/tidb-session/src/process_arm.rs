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
//! helpers `open_information_schema_query` (in `dispatch.rs`) calls to build the
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
                    registry.kill(self.connection_id.unwrap_or(0), kill.query);
                }
                return Ok(Some(StmtOutput::Affected(0)));
            }
            tidb_ast::KillTarget::Expr(_) => {
                return Err(DriverError::unsupported(INVALID_OPERATION));
            }
        };

        // Go's planner checks the local target before executeKillStmt applies
        // config or ID warnings. Same username is exempt regardless of host.
        if let Some(owner) = registry
            .as_ref()
            .and_then(|registry| registry.snapshot().into_iter().find(|row| row.id == target))
        {
            if owner.user != self.process_list_user() {
                if !self.has_dynamic_privilege("CONNECTION_ADMIN", false) {
                    return Err(DriverError::KillAccessDenied);
                }
                // The front end stores the display address (Go Host + Port).
                // Verification needs the presented host without its port.
                let host = owner
                    .host
                    .rsplit_once(':')
                    .filter(|(host, port)| {
                        port.parse::<u16>().is_ok()
                            && (!host.contains(':')
                                || (host.starts_with('[') && host.ends_with(']')))
                    })
                    .map_or(owner.host.as_str(), |(host, _)| {
                        host.trim_start_matches('[').trim_end_matches(']')
                    });
                if tidb_util::sem::is_enabled()
                    && self.target_has_dynamic_privilege(&owner.user, host, "RESTRICTED_USER_ADMIN")
                    && !self.has_dynamic_privilege("RESTRICTED_CONNECTION_ADMIN", false)
                {
                    return Err(DriverError::SpecificAccessDenied(
                        "RESTRICTED_CONNECTION_ADMIN".to_owned(),
                    ));
                }
            }
        } else if registry.is_some()
            && tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.contains(target)
            && !self.has_dynamic_privilege("CONNECTION_ADMIN", false)
        {
            return Err(DriverError::KillAccessDenied);
        }

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

    /// Local formatting and visibility stay with PROCESSLIST; discovered peers
    /// execute the same cluster table with the originating user's identity.
    pub(crate) fn cluster_process_list_table_rows(
        &mut self,
        columns: &[(String, FieldType)],
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let instance = self.cluster_instance_address();
        let mut rows: Vec<_> = self
            .process_list_table_rows()
            .into_iter()
            .map(|mut row| {
                row.insert(0, Datum::Bytes(instance.clone().into_bytes()));
                row
            })
            .collect();
        let Some(syncer) = &self.server_info_syncer else {
            return Ok(rows);
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
            return Ok(rows);
        }
        let client = self
            .cluster_peer
            .as_ref()
            .ok_or_else(|| DriverError::unsupported("TiDB peer RPC client is not installed"))?;
        let ctx = self.statement_context(false);
        // Go carries Username/Hostname (the presented login), not AuthHostname.
        let user = self
            .login_user
            .as_deref()
            .and_then(|identity| identity.split_once('@'));
        let result = client.scan(
            &servers,
            infoschema::memory_table_id("CLUSTER_PROCESSLIST").expect("registered memory table"),
            columns,
            user,
            &ctx,
            &self.session_time_zone(),
            ctx.dist_sql_scan_concurrency() as usize,
        )?;
        for (code, message) in result.warnings {
            self.append_warning(WarningLevel::Warning, code, message);
        }
        rows.extend(result.rows);
        Ok(rows)
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
    pub(crate) fn process_list_output(&self, full: bool) -> StmtOutput {
        let rows = self.visible_process_rows(full);
        let text = || FieldType::new(tidb_datatype::FieldTypeCode::Varchar);
        let nullable_text = |value: String| {
            if value.is_empty() {
                Datum::Null
            } else {
                Datum::Bytes(value.into_bytes())
            }
        };
        StmtOutput::Rows {
            columns: vec![
                (
                    "Id".to_owned(),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                ),
                ("User".to_owned(), text()),
                ("Host".to_owned(), text()),
                ("db".to_owned(), text()),
                ("Command".to_owned(), text()),
                (
                    "Time".to_owned(),
                    FieldType::new(tidb_datatype::FieldTypeCode::Long),
                ),
                ("State".to_owned(), text()),
                (
                    "Info".to_owned(),
                    FieldType::new(tidb_datatype::FieldTypeCode::String),
                ),
            ],
            rows: rows
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
                        match row.info {
                            Some(info) => Datum::Bytes(
                                process::truncate_process_info(&info, full).into_bytes(),
                            ),
                            None => Datum::Null,
                        },
                    ]
                })
                .collect(),
        }
    }

    /// The `User` column: Go reports the bare user name, while this session
    /// stores the login identity as `user@host`.
    pub(crate) fn process_list_user(&self) -> String {
        match &self.login_user {
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
        let rows: Vec<process::ProcessRow> = match &self.process {
            Some(guard) => guard.registry().snapshot(),
            None => vec![process::ProcessRow {
                id: self.connection_id.unwrap_or(0),
                user: self.process_list_user(),
                host: String::new(),
                db: self.current_db.clone(),
                command: "Query".to_owned(),
                time: 0,
                state: self.status_text(),
                info: Some(if full {
                    "show full processlist".to_owned()
                } else {
                    "show processlist".to_owned()
                }),
                resource_group: self.active_resource_group.clone(),
                ..process::ProcessRow::default()
            }],
        };
        if self.has_process_privilege() {
            return rows;
        }
        let me = self.process_list_user();
        rows.into_iter().filter(|row| row.user == me).collect()
    }

    /// `SELECT * FROM information_schema.PROCESSLIST` rows, in the exact
    /// column order Go's `tableProcesslistCols` / `ProcessInfo.ToRow` build
    /// (CAPTURED: `ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO, DIGEST,
    /// MEM, MEM_ARBITRATION, MEM_WAIT_ARBITRATE_START,
    /// MEM_WAIT_ARBITRATE_BYTES, DISK, TxnStart, RESOURCE_GROUP,
    /// SESSION_ALIAS, ROWS_AFFECTED, TIDB_CPU, TIKV_CPU`).
    ///
    /// `ToRow` builds on `ToRowForShow(true)`, i.e. `INFO` is never truncated
    /// here (unlike `SHOW PROCESSLIST` without `FULL`).
    ///
    /// Memory arbitration and SQL CPU timing still need their statement
    /// owners. The other values are snapshots of the target's published state.
    pub(crate) fn process_list_table_rows(&self) -> Vec<Vec<Datum>> {
        self.visible_process_rows(true)
            .into_iter()
            .map(|row| {
                let txn_start = if row.cur_txn_start_ts == 0 {
                    String::new()
                } else {
                    let time = tidb_expr::sessionexpr::get_time_from_ts(row.cur_txn_start_ts)
                        .with_timezone(&self.session_time_zone());
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
                        Some(info) => Datum::Bytes(info.into_bytes()),
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
                        privilege::GlobalPriv::Process,
                    )
                })
            })
            || self.login_user.is_none()
    }

    /// Executor-owned rows of `INFORMATION_SCHEMA.DEADLOCKS`.
    pub(crate) fn deadlock_history_table_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        use tidb_datatype::{Collation, StringDatum};
        use tidb_executor::deadlock_history::{
            COL_CURRENT_SQL_DIGEST, COL_DEADLOCK_ID, COL_KEY, COL_OCCUR_TIME, COL_RETRYABLE,
            COL_TRX_HOLDING_LOCK, COL_TRY_LOCK_TRX_ID, GLOBAL_DEADLOCK_HISTORY,
        };
        use tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP;

        let records = GLOBAL_DEADLOCK_HISTORY.get_all();
        self.with_catalog_mut(|catalog| {
            Ok(records
                .into_iter()
                .flat_map(|record| {
                    (0..record.wait_chain.len())
                        .map(|idx| {
                            let item = &record.wait_chain[idx];
                            let digest_text = STMT_SUMMARY_BY_DIGEST_MAP
                                .normalized_sql_for_digest(&item.sql_digest)
                                .map(Datum::new_string)
                                .unwrap_or(Datum::Null);
                            let key_info = if item.key.is_empty() {
                                Datum::Null
                            } else {
                                tidb_executor::keydecoder::decode_key(&item.key, catalog)
                                    .ok()
                                    .and_then(|decoded| serde_json::to_vec(&decoded).ok())
                                    .map(|json| {
                                        Datum::String(StringDatum::new(json, Collation::DEFAULT))
                                    })
                                    .unwrap_or(Datum::Null)
                            };
                            vec![
                                record.to_datum(idx, COL_DEADLOCK_ID),
                                record.to_datum(idx, COL_OCCUR_TIME),
                                record.to_datum(idx, COL_RETRYABLE),
                                record.to_datum(idx, COL_TRY_LOCK_TRX_ID),
                                record.to_datum(idx, COL_CURRENT_SQL_DIGEST),
                                digest_text,
                                record.to_datum(idx, COL_KEY),
                                key_info,
                                record.to_datum(idx, COL_TRX_HOLDING_LOCK),
                            ]
                        })
                        .collect::<Vec<_>>()
                })
                .collect())
        })
    }
}
