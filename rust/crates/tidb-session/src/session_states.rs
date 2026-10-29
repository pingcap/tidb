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

//! Go `session.EncodeStates` / `DecodeStates` over the typed
//! `sessionstates.SessionStates`, with the parts `SessionVars`' own
//! `EncodeSessionStates` / `DecodeSessionStates` hold, and the session token
//! `SHOW SESSION_STATES` signs (`tidb_sessionstates::create_session_token`).

use tidb_executor::compiler::PreparedExec;
use crate::Session;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_executor::{DriverError, MysqlError};
use tidb_sessionstates::{PreparedStmtInfo, SessionStates};

/// A snapshot supplied by the connection owner before exporting session state.
#[derive(Default)]
pub struct ProtocolSessionStates {
    /// Binary prepared handles, excluding physical plans.
    pub prepared: BTreeMap<u32, PreparedStmtInfo>,
    /// A connection-owned resource that prevents migration.
    pub cannot_migrate: Option<&'static str>,
}

fn invalid_state(error: impl std::fmt::Display) -> DriverError {
    DriverError::Mysql(MysqlError::unknown(error.to_string()))
}

impl Session {
    /// Resolves migration commands executed through SQL PREPARE using the retained AST.
    pub fn session_migration_statement<'a>(
        &'a self,
        stmt: &'a tidb_ast::Stmt,
    ) -> Option<&'a tidb_ast::Stmt> {
        let statement = match stmt {
            tidb_ast::Stmt::Session(session) => match &**session {
                tidb_ast::SessionStmt::Execute { name, .. } => {
                    &self.prepared_statements.get(name)?.statement
                }
                _ => stmt,
            },
            _ => stmt,
        };
        match statement {
            tidb_ast::Stmt::Session(session)
                if matches!(&**session, tidb_ast::SessionStmt::SetSessionStates(_)) =>
            {
                Some(statement)
            }
            tidb_ast::Stmt::Admin(admin) if matches!(&**admin, tidb_ast::AdminStmt::Show(show) if show.tp == tidb_ast::ShowStmtType::SessionStates) => {
                Some(statement)
            }
            _ => None,
        }
    }

    /// Allocates in the namespace shared by SQL PREPARE and COM_STMT_PREPARE.
    pub fn allocate_prepared_statement_id(&mut self) -> u32 {
        self.vars.prepared_statement_id = self.vars.prepared_statement_id.wrapping_add(1);
        self.vars.prepared_statement_id
    }

    /// Installs the protocol owner's current portable state and admission result.
    pub fn set_protocol_session_states(&mut self, states: ProtocolSessionStates) {
        self.protocol_session_states = states;
    }

    /// Temporarily selects the prepare-time database without running a USE statement.
    pub fn replace_migration_database(&mut self, database: String) -> String {
        std::mem::replace(&mut self.vars.current_db, database)
    }

    /// Exports the state owned by this SQL session and its connection handlers.
    pub fn encode_session_states(&self) -> Result<SessionStates, DriverError> {
        if self.in_transaction() {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_error::catalog::sessionstates::ERR_CANNOT_MIGRATE_SESSION
                    .generate_with_stack_by_args(&["session has an active transaction".into()]),
            )));
        }
        if !self.local_temporary_tables.is_empty() {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_error::catalog::sessionstates::ERR_CANNOT_MIGRATE_SESSION
                    .generate_with_stack_by_args(&["session has local temporary tables".into()]),
            )));
        }
        if self.advisory_locks.has_locks() {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_error::catalog::sessionstates::ERR_CANNOT_MIGRATE_SESSION
                    .generate_with_stack_by_args(&["session has advisory locks".into()]),
            )));
        }
        // The `TableLockTpInfo` names this session and server, so the locks
        // cannot move with it.
        if self.has_locked_tables() {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_error::catalog::sessionstates::ERR_CANNOT_MIGRATE_SESSION
                    .generate_with_stack_by_args(&["session has locked tables".into()]),
            )));
        }
        if self.vars.sandbox_mode {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_error::catalog::sessionstates::ERR_CANNOT_MIGRATE_SESSION
                    .generate_with_stack_by_args(&["session is in sandbox mode".into()]),
            )));
        }
        if let Some(reason) = self.protocol_session_states.cannot_migrate {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_error::catalog::sessionstates::ERR_CANNOT_MIGRATE_SESSION
                    .generate_with_stack_by_args(&[reason.to_string().into()]),
            )));
        }
        let mut system_vars = BTreeMap::new();
        for definition in tidb_variable::sysvar::SYS_VARS.iter() {
            if !definition.has_session_scope() || definition.is_read_only() {
                continue;
            }
            let (value, keep) = self
                .vars
                .get_session_states_system_var(definition.name)
                .map_err(crate::variables::var_error)?;
            if !keep {
                continue;
            }
            if self.sem_hides_sysvar(definition.name) {
                let default = if definition.has_global_scope() {
                    self.vars
                        .get_global(definition.name)
                        .map_err(crate::variables::var_error)?
                } else {
                    definition.value.to_owned()
                };
                if value != default {
                    return Err(DriverError::Mysql(MysqlError::from_terror(
                        &tidb_error::catalog::sessionstates::ERR_CANNOT_MIGRATE_SESSION
                            .generate_with_stack_by_args(&[(format!(
                                "session has set invisible variable '{}'",
                                definition.name
                            ))
                            .to_string()
                            .into()]),
                    )));
                }
                continue;
            }
            system_vars.insert(definition.name, value);
        }
        system_vars.insert("rand_seed1", self.rand.get_seed1().to_string());
        system_vars.insert("rand_seed2", self.rand.get_seed2().to_string());
        let mut states = SessionStates {
            resource_group_name: self.vars.resource_group.clone(),
            ..SessionStates::default()
        };
        system_vars.insert("rand_seed1", self.rand.get_seed1().to_string());
        system_vars.insert("rand_seed2", self.rand.get_seed2().to_string());
        states.system_vars = system_vars
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect();
        // Go `SessionVars.EncodeSessionStates`.
        let (values, user_types) = self.user_vars.snapshot();
        states.user_vars = values
            .into_iter()
            .map(|(name, datum)| (name, tidb_sessionstates::JsonDatum(datum)))
            .collect();
        states.user_var_types = user_types.into_iter().collect();
        states.prepared_stmt_id = self.vars.prepared_statement_id;
        states.status = if self.is_autocommit() { 2 } else { 0 };
        states.current_db = self.vars.current_db.clone();
        states.last_txn_info = self.vars.last_txn_info.clone();
        if self.vars.last_query_info.start_ts != 0 {
            states.last_query_info = Some(self.vars.last_query_info.clone());
        }
        if self.vars.last_ddl_info.seq_num != 0 {
            states.last_ddl_info = Some(self.vars.last_ddl_info.clone());
        }
        states.last_found_rows = self.vars.last_found_rows;
        states.sequence_latest_values = self
            .sequence_last_values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(&id, &value)| (id, value))
            .collect();
        states.found_in_plan_cache = self.vars.prev_found_in_plan_cache;
        states.found_in_binding = self.vars.prev_found_in_binding;
        states.hypo_indexes = self.hypo_indexes.session_state().clone();
        states.hypo_tiflash_replicas = self.hypo_tiflash_replicas.session_state();
        states.last_affected_rows = self.vars.prev_row_count;
        states.last_insert_id = self.vars.last_insert_id;
        states.warnings = self.warnings.clone();
        // The connection's handlers: prepared statements and bindings.
        states.prepared_stmts = self.protocol_session_states.prepared.clone();
        for (name, statement) in &self.prepared_statements {
            states.prepared_stmts.insert(
                statement.id,
                PreparedStmtInfo {
                    name: name.clone(),
                    stmt_text: statement.original_sql.clone(),
                    stmt_db: statement.database.clone(),
                    param_types: Vec::new(),
                },
            );
        }
        let bindings = self.session_bindings.all_sorted();
        if !bindings.is_empty() {
            let bindings: Vec<_> = bindings.into_iter().map(|b| json!({
                "OriginalSQL": b.original_sql, "Db": b.db, "BindSQL": b.bind_sql,
                "Status": b.status, "CreateTime": binding_time_to_json(&b.create_time), "UpdateTime": binding_time_to_json(&b.update_time),
                "Charset": b.charset, "Collation": b.collation, "Source": b.source,
                "SQLDigest": b.sql_digest, "PlanDigest": b.plan_digest,
            })).collect();
            states.bindings = serde_json::to_string(&bindings).map_err(invalid_state)?;
        }
        Ok(states)
    }

    /// Go `ShowExec.fetchShowSessionStates`: the encoded session state, and
    /// a session token for the logged-in user.
    pub(crate) fn show_session_states_rows(&self) -> Result<Vec<Vec<Datum>>, DriverError> {
        let text = serde_json::to_string(&self.encode_session_states()?).map_err(invalid_state)?;
        let state = tidb_datatype::BinaryJSON::parse(&text).map_err(invalid_state)?;
        // A session with no user has no token. A failure to sign is only
        // logged: a deployment may want session states (traffic replay)
        // without the signing certificate a migration needs.
        let token = self.vars.login_user.as_ref().and_then(|_| {
            tidb_sessionstates::create_session_token(&self.process_list_user())
                .map_err(|error| {
                    tidb_log::warn(
                        "create session token failed",
                        &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
                    );
                })
                .ok()
        });
        let token = match token {
            Some(token) => Datum::Json(
                tidb_datatype::BinaryJSON::parse(&tidb_util::stringutil::go_json_marshal(&token))
                    .map_err(invalid_state)?,
            ),
            None => Datum::Null,
        };
        Ok(vec![vec![Datum::Json(state), token]])
    }

    /// Restores portable SQL state. Connection handlers rebuild binary handles first.
    pub fn decode_session_states(&mut self, text: &str) -> Result<(), DriverError> {
        let states = parse_session_states(text)?;
        let old_id = self.vars.prepared_statement_id;
        let old_db = self.vars.current_db.clone();
        let prepare_result = (|| {
            for (&id, statement) in &states.prepared_stmts {
                if statement.name.is_empty() {
                    continue;
                }
                self.vars.prepared_statement_id = id.wrapping_sub(1);
                self.vars.current_db = statement.stmt_db.clone();
                self.prepare_statement(
                    &statement.name,
                    &tidb_ast::PrepareSource::Sql(statement.stmt_text.clone()),
                )?;
            }
            Ok::<_, DriverError>(())
        })();
        self.vars.prepared_statement_id = old_id;
        self.vars.current_db = old_db;
        prepare_result?;
        if !states.bindings.is_empty() {
            let records: Vec<Value> =
                serde_json::from_str(&states.bindings).map_err(invalid_state)?;
            for record in records {
                let nested = record
                    .get("Bindings")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_else(|| vec![record.clone()]);
                for mut binding in nested {
                    for key in ["OriginalSQL", "Db"] {
                        if binding.get(key).is_none() {
                            binding[key] = record
                                .get(key)
                                .cloned()
                                .unwrap_or(Value::String(String::new()));
                        }
                    }
                    for key in ["CreateTime", "UpdateTime"] {
                        if let Some(time) = binding.get(key).filter(|time| time.is_number()) {
                            binding[key] = Value::String(
                                tidb_datatype::Time::from_go_json(&time.to_string())
                                    .map_err(invalid_state)?
                                    .to_string(),
                            );
                        }
                    }
                    let row: Vec<_> = [
                        "OriginalSQL",
                        "BindSQL",
                        "Db",
                        "Status",
                        "CreateTime",
                        "UpdateTime",
                        "Charset",
                        "Collation",
                        "Source",
                        "SQLDigest",
                        "PlanDigest",
                    ]
                    .iter()
                    .map(|key| {
                        Datum::Bytes(
                            binding
                                .get(*key)
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .as_bytes()
                                .to_vec(),
                        )
                    })
                    .collect();
                    let mut binding = tidb_bindinfo::binding_utils::new_binding_from_storage(&row)
                        .ok_or_else(|| invalid_state("invalid session binding"))?;
                    binding.sql_digest =
                        tidb_parser::digest_normalized(&binding.original_sql).to_string();
                    self.session_bindings.create(binding);
                }
            }
        }
        {
            let mut vars: Vec<_> = states.system_vars.iter().collect();
            // Go OrderByDependency partitions depended variables before their consumers.
            vars.sort_by_key(|(name, _)| {
                !matches!(
                    name.as_str(),
                    "tidb_allow_mpp"
                        | "tidb_enable_noop_functions"
                        | "tidb_enable_historical_stats"
                        | "tidb_enable_local_txn"
                )
            });
            for (name, value) in vars {
                // Go logs and continues when versions differ in variable availability or validation.
                if let Err(error) = self.vars.set_system(name, value.to_owned()) {
                    tidb_util::logutil::bg_logger().warn(
                        "restoring session variable failed",
                        &[
                            tidb_log::Field::new("variable", tidb_log::Value::Str(name.clone())),
                            tidb_log::Field::new(
                                "error",
                                tidb_log::Value::Str(format!("{error:?}")),
                            ),
                        ],
                    );
                } else {
                    self.seed_rand_from_sysvar(name)?;
                }
            }
        }
        if !states.resource_group_name.is_empty() {
            let group = &states.resource_group_name;
            if !self.vars.resource_control_strict_mode()
                || (self.privilege_bypassed
                    || self.privileges.is_none()
                    || self.vars.current_user.is_none())
                || self.has_dynamic_privilege("RESOURCE_GROUP_ADMIN", false)
                || self.has_dynamic_privilege("RESOURCE_GROUP_USER", false)
            {
                self.vars.resource_group = group.to_owned();
            }
        }
        // Go `SessionVars.DecodeSessionStates`.
        for (name, value) in states.user_vars {
            self.user_vars.set_user_var_val(&name, value.0);
        }
        for (name, field_type) in states.user_var_types {
            self.user_vars.set_user_var_type(&name, field_type);
        }
        self.vars.restore_autocommit_status(states.status);
        self.vars.prepared_statement_id = states.prepared_stmt_id;
        self.vars.current_db = states.current_db;
        self.vars.last_txn_info = states.last_txn_info;
        if let Some(info) = states.last_query_info {
            self.vars.last_query_info = info;
        }
        if let Some(info) = states.last_ddl_info {
            self.vars.last_ddl_info = info;
        }
        self.vars.last_found_rows = states.last_found_rows;
        *self
            .sequence_last_values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            states.sequence_latest_values.into_iter().collect();
        self.vars.found_in_plan_cache = states.found_in_plan_cache;
        self.vars.found_in_binding = states.found_in_binding;
        self.hypo_indexes = std::sync::Arc::new(
            tidb_table::hypo_index::HypoIndexes::from_session_state(states.hypo_indexes),
        );
        self.hypo_tiflash_replicas = std::sync::Arc::new(
            tidb_table::hypo_index::HypoTiFlashReplicas::from_session_state(
                states.hypo_tiflash_replicas,
            ),
        );
        self.vars.prev_row_count = states.last_affected_rows;
        self.vars.last_insert_id = states.last_insert_id;
        self.warnings = states.warnings;
        Ok(())
    }
}

fn binding_time_to_json(text: &str) -> Value {
    tidb_datatype::parse_datetime(text, &chrono::Utc, true, true)
        .map(|parsed| Value::from(parsed.time.go_raw()))
        .unwrap_or(Value::from(0u64))
}

/// Go `executeSetSessionStates`' decode of the statement's JSON into
/// `sessionstates.SessionStates`, before any handler restores from it.
///
/// # Errors
///
/// The decoder's error when the text is not Go's shape.
pub fn parse_session_states(text: &str) -> Result<SessionStates, DriverError> {
    tidb_sessionstates::decode_session_states(text).map_err(invalid_state)
}

fn columns() -> Vec<(String, FieldType)> {
    ["Session_states", "Session_token"]
        .into_iter()
        .map(|name| (name.to_owned(), FieldType::new(FieldTypeCode::Json)))
        .collect()
}

pub(crate) fn prepared_columns(statement: &tidb_ast::Stmt) -> Option<Vec<(String, FieldType)>> {
    matches!(statement, tidb_ast::Stmt::Admin(admin) if matches!(&**admin, tidb_ast::AdminStmt::Show(show) if show.tp == tidb_ast::ShowStmtType::SessionStates)).then(columns)
}
