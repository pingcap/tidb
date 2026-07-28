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
//! helpers `run_information_schema_select` (in `lib.rs`) calls to build the
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
        let target = match &kill.target {
                        tidb_ast::KillTarget::ConnectionId(id) => *id,
                        // Go accepts `KILL CONNECTION_ID()` (kill my own
                        // connection) and rejects every other expression with
                        // this exact message.
                        tidb_ast::KillTarget::Expr(tidb_ast::Expr::Func { name, args, .. })
                            if name.eq_ignore_ascii_case("connection_id") && args.is_empty() =>
                        {
                            self.connection_id.unwrap_or(0)
                        }
                        tidb_ast::KillTarget::Expr(_) => {
                            return Err(DriverError::Unsupported(
                                "Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] [connectionID | CONNECTION_ID()]' instead",
                            ))
                        }
                    };
        // Captured from TiDB: KILL of an id this server does not
        // hold is NOT an error -- it answers OK, having done
        // nothing. (1094 `Unknown thread id` belongs to EXPLAIN
        // FOR CONNECTION, not to KILL.) A session with no server
        // front holds no connection at all, which is Go's
        // `sm == nil` early return: also a silent no-op.
        if let Some(guard) = &self.process {
            // Go `planbuilder.go`'s `*ast.KillStmt` case: everyone
            // may KILL their own connection regardless of
            // privilege; killing anyone else's requires the
            // DYNAMIC `CONNECTION_ADMIN`, reported as
            // `ErrSpecificAccessDenied.GenWithStackByArgs("SUPER
            // or CONNECTION_ADMIN")` (1227) -- NOT the unused
            // 1095 `ErrKillDenied` errno entry, which no code
            // path in current Go ever raises. SUPER still passes
            // because it is the fallback for every dynamic
            // privilege, which is exactly why Go's message names
            // both.
            //
            // Go additionally requires `RESTRICTED_CONNECTION_ADMIN`
            // to kill a connection owned by a
            // `RESTRICTED_USER_ADMIN` user, but only under SEM
            // (`appendVisitInfoIsRestrictedUser` returns early
            // when `sem.IsEnabled()` is false); with no SEM in
            // this tier that branch is unreachable, so it is
            // deliberately absent rather than half-modelled.
            let is_self = self.connection_id == Some(target);
            if !is_self {
                let owner = guard
                    .registry()
                    .snapshot()
                    .into_iter()
                    .find(|row| row.id == target)
                    .map(|row| row.user);
                // Go compares the process's USERNAME against the
                // logged-in username, ignoring host.
                let same_user = owner.as_deref() == Some(self.process_list_user().as_str());
                let may_kill = self.privileges.as_ref().is_some_and(|registry| {
                    self.current_identity().is_some_and(|(user, host)| {
                        registry.has_dynamic_priv_with_roles(
                            user,
                            host,
                            self.active_roles(),
                            "CONNECTION_ADMIN",
                            false,
                        )
                    })
                });
                if owner.is_some() && !same_user && !may_kill {
                    return Err(DriverError::KillAccessDenied);
                }
            }
            guard.registry().kill(target, kill.query);
        }
        Ok(Some(StmtOutput::Affected(0)))
    }

    /// `SHOW WARNINGS` / `SHOW ERRORS` output: one row per buffered warning,
    /// or the count when the source wrote `SHOW COUNT(*) WARNINGS`.
    ///
    /// Captured from TiDB: the columns are `Level`, `Code`, `Message`; the
    /// count form returns a single `@@session.warning_count` column; and
    /// `SHOW ERRORS` shows only the `Error`-level rows.
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
            }],
        };
        let has_process_via_registry = self.privileges.as_ref().is_some_and(|registry| {
            self.current_identity().is_some_and(|(user, host)| {
                registry.has_global_priv_with_roles(
                    user,
                    host,
                    self.active_roles(),
                    privilege::GlobalPriv::Process,
                )
            })
        });
        if self.has_process_priv || has_process_via_registry || self.login_user.is_none() {
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
    /// NOT MODELLED (this tier tracks none of these per connection, so each
    /// is Go's own value for a connection with no live statement context --
    /// `RefCountOfStmtCtx` fails to increase -- rather than an invented one):
    /// `DIGEST` is `""`, `MEM`/`DISK`/`TIDB_CPU`/`TIKV_CPU` are `0`,
    /// `MEM_ARBITRATION`/`MEM_WAIT_ARBITRATE_START`/
    /// `MEM_WAIT_ARBITRATE_BYTES`/`ROWS_AFFECTED` are `NULL`, and
    /// `TxnStart`/`RESOURCE_GROUP`/`SESSION_ALIAS` are `""`.
    pub(crate) fn process_list_table_rows(&self) -> Vec<Vec<Datum>> {
        self.visible_process_rows(true)
            .into_iter()
            .map(|row| {
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
                    // DIGEST
                    Datum::Bytes(Vec::new()),
                    // MEM
                    Datum::UInt(0),
                    // MEM_ARBITRATION
                    Datum::Null,
                    // MEM_WAIT_ARBITRATE_START
                    Datum::Null,
                    // MEM_WAIT_ARBITRATE_BYTES
                    Datum::Null,
                    // DISK
                    Datum::UInt(0),
                    // TxnStart
                    Datum::Bytes(Vec::new()),
                    // RESOURCE_GROUP
                    Datum::Bytes(Vec::new()),
                    // SESSION_ALIAS
                    Datum::Bytes(Vec::new()),
                    // ROWS_AFFECTED
                    Datum::Null,
                    // TIDB_CPU
                    Datum::Int(0),
                    // TIKV_CPU
                    Datum::Int(0),
                ]
            })
            .collect()
    }
}
