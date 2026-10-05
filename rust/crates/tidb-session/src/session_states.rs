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

//! Composition of session migration state over existing SQL and protocol owners.
//! This is repair evidence, not complete acceptance of Go's sessionstates package:
//! signing certificates and token authentication retain their separate obligations.

use crate::{Session, SqlWarning, StmtOutput, WarningLevel};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_executor::{DriverError, MysqlError};

/// Go's prepared-statement migration input. Plans are rebuilt at the destination.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PreparedStmtInfo {
    /// SQL PREPARE name; empty denotes a binary protocol statement.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Original statement text.
    pub text: String,
    /// Database selected when preparing.
    #[serde(rename = "db", skip_serializing_if = "String::is_empty")]
    pub database: String,
    /// Go []byte JSON spelling of the remembered binary parameter types.
    #[serde(rename = "types", skip_serializing_if = "String::is_empty")]
    pub parameter_types: String,
}

/// A snapshot supplied by the connection owner before exporting session state.
#[derive(Default)]
pub struct ProtocolSessionStates {
    /// Binary prepared handles, excluding physical plans.
    pub prepared: BTreeMap<u32, PreparedStmtInfo>,
    /// A connection-owned resource that prevents migration.
    pub cannot_migrate: Option<&'static str>,
}

/// Go's nonportable-session diagnostic (8146).
pub fn cannot_migrate(reason: impl std::fmt::Display) -> DriverError {
    DriverError::Mysql(MysqlError::new(
        8146,
        format!("cannot migrate the current session: {reason}"),
    ))
}

fn invalid_state(error: impl std::fmt::Display) -> DriverError {
    DriverError::Mysql(MysqlError::new(1105, error.to_string()))
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
            tidb_ast::Stmt::Admin(admin) if matches!(&**admin, tidb_ast::AdminStmt::ShowInspection(show) if show.kind == tidb_ast::ShowInspectionKind::SessionStates) => {
                Some(statement)
            }
            _ => None,
        }
    }

    /// Allocates in the namespace shared by SQL PREPARE and COM_STMT_PREPARE.
    pub fn allocate_prepared_statement_id(&mut self) -> u32 {
        self.prepared_statement_id = self.prepared_statement_id.wrapping_add(1);
        self.prepared_statement_id
    }

    /// Installs the protocol owner's current portable state and admission result.
    pub fn set_protocol_session_states(&mut self, states: ProtocolSessionStates) {
        self.protocol_session_states = states;
    }

    /// Temporarily selects the prepare-time database without running a USE statement.
    pub fn replace_migration_database(&mut self, database: String) -> String {
        std::mem::replace(&mut self.current_db, database)
    }

    /// Exports the state owned by this SQL session and its connection handlers.
    pub fn encode_session_states(&self) -> Result<Value, DriverError> {
        if self.in_transaction() {
            return Err(cannot_migrate("session has an active transaction"));
        }
        if !self.local_temporary_tables.is_empty() {
            return Err(cannot_migrate("session has local temporary tables"));
        }
        if self.advisory_locks.has_locks() {
            return Err(cannot_migrate("session has advisory locks"));
        }
        if self.sandbox_mode {
            return Err(cannot_migrate("session is in sandbox mode"));
        }
        if let Some(reason) = self.protocol_session_states.cannot_migrate {
            return Err(cannot_migrate(reason));
        }
        let mut system_vars = BTreeMap::new();
        for definition in crate::sysvar::SYS_VARS.iter() {
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
                    return Err(cannot_migrate(format!(
                        "session has set invisible variable '{}'",
                        definition.name
                    )));
                }
                continue;
            }
            system_vars.insert(definition.name, value);
        }
        system_vars.insert("rand_seed1", self.rand.get_seed1().to_string());
        system_vars.insert("rand_seed2", self.rand.get_seed2().to_string());
        let (values, user_types) = self.user_vars.snapshot();
        let mut user_vars = BTreeMap::new();
        for (name, datum) in values {
            user_vars.insert(
                name,
                serde_json::from_slice::<Value>(&datum.marshal_json().map_err(invalid_state)?)
                    .map_err(invalid_state)?,
            );
        }
        let mut prepared = self.protocol_session_states.prepared.clone();
        for (name, statement) in &self.prepared_statements {
            prepared.insert(
                statement.id,
                PreparedStmtInfo {
                    name: name.clone(),
                    text: statement.original_sql.clone(),
                    database: statement.database.clone(),
                    parameter_types: String::new(),
                },
            );
        }
        let warnings: Vec<_> = self.warnings.iter().map(|warning| json!({"level": match warning.level { WarningLevel::Warning => "Warning", WarningLevel::Error => "Error", WarningLevel::Note => "Note" }, "err": {"class": 0, "code": warning.code, "message": warning.message}})).collect();
        let mut state = json!({
            "user-var-values": user_vars, "user-var-types": user_types,
            "sys-vars": system_vars, "prepared-stmts": prepared,
            "prepared-stmt-id": self.prepared_statement_id,
            "status": if self.is_autocommit() { 2u32 } else { 0 },
            "current-db": self.current_db, "txn-info": *self.last_txn_info.borrow(),
            "found-rows": self.last_found_rows, "in-plan-cache": self.prev_found_in_plan_cache,
            "in-binding": self.prev_found_in_binding,
            "seq-values": *self.sequence_last_values.lock().unwrap_or_else(std::sync::PoisonError::into_inner),
            "affected-rows": self.prev_row_count, "last-insert-id": self.last_insert_id,
            "warnings": warnings, "rs-group": self.resource_group,
        });
        let query: Value =
            serde_json::from_str(&self.last_query_info.borrow()).unwrap_or(Value::Null);
        if query.get("start_ts").and_then(Value::as_u64).unwrap_or(0) != 0 {
            state["query-info"] = query;
        }
        let bindings = self.session_bindings.all_sorted();
        if !bindings.is_empty() {
            let bindings: Vec<_> = bindings.into_iter().map(|b| json!({
                "OriginalSQL": b.original_sql, "Db": b.db, "BindSQL": b.bind_sql,
                "Status": b.status, "CreateTime": binding_time_to_json(&b.create_time), "UpdateTime": binding_time_to_json(&b.update_time),
                "Charset": b.charset, "Collation": b.collation, "Source": b.source,
                "SQLDigest": b.sql_digest, "PlanDigest": b.plan_digest,
            })).collect();
            state["bindings"] =
                Value::String(serde_json::to_string(&bindings).map_err(invalid_state)?);
        }
        state
            .as_object_mut()
            .expect("state object")
            .retain(|_, value| match value {
                Value::Null => false,
                Value::Bool(value) => *value,
                Value::Number(value) => value.as_i64() != Some(0),
                Value::String(value) => !value.is_empty(),
                Value::Array(value) => !value.is_empty(),
                Value::Object(value) => !value.is_empty(),
            });
        Ok(state)
    }

    pub(crate) fn show_session_states(&self) -> Result<StmtOutput, DriverError> {
        let json = tidb_datatype::BinaryJSON::parse(&self.encode_session_states()?.to_string())
            .map_err(invalid_state)?;
        Ok(StmtOutput::Rows {
            columns: columns(),
            rows: vec![vec![Datum::Json(json), Datum::Null]],
        })
    }

    /// Restores portable SQL state. Connection handlers rebuild binary handles first.
    pub fn decode_session_states(&mut self, text: &str) -> Result<(), DriverError> {
        let state = parse_session_states(text)?;
        let prepared: BTreeMap<u32, PreparedStmtInfo> =
            serde_json::from_value(state.get("prepared-stmts").cloned().unwrap_or(json!({})))
                .map_err(invalid_state)?;
        let old_id = self.prepared_statement_id;
        let old_db = self.current_db.clone();
        let prepare_result = (|| {
            for (id, statement) in prepared {
                if statement.name.is_empty() {
                    continue;
                }
                self.prepared_statement_id = id.wrapping_sub(1);
                self.current_db = statement.database;
                self.prepare_statement(
                    &statement.name,
                    &tidb_ast::PrepareSource::Sql(statement.text),
                )?;
            }
            Ok::<_, DriverError>(())
        })();
        self.prepared_statement_id = old_id;
        self.current_db = old_db;
        prepare_result?;
        if let Some(bindings) = state
            .get("bindings")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            let records: Vec<Value> = serde_json::from_str(bindings).map_err(invalid_state)?;
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
                    let mut binding = crate::binding_utils::new_binding_from_storage(&row)
                        .ok_or_else(|| invalid_state("invalid session binding"))?;
                    binding.sql_digest =
                        tidb_parser::digest_normalized(&binding.original_sql).to_string();
                    self.session_bindings.create(binding);
                }
            }
        }
        if let Some(vars) = state.get("sys-vars").and_then(Value::as_object) {
            let mut vars: Vec<_> = vars.iter().collect();
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
                let Some(value) = value.as_str() else {
                    return Err(invalid_state("system variable state must be a string"));
                };
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
        if let Some(group) = state
            .get("rs-group")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            if !self.vars.resource_control_strict_mode()
                || (self.privilege_bypassed
                    || self.privileges.is_none()
                    || self.current_user.is_none())
                || self.has_dynamic_privilege("RESOURCE_GROUP_ADMIN", false)
                || self.has_dynamic_privilege("RESOURCE_GROUP_USER", false)
            {
                self.resource_group = group.to_owned();
            }
        }
        if let Some(vars) = state.get("user-var-values").and_then(Value::as_object) {
            for (name, value) in vars {
                self.user_vars.set_user_var_val(
                    name,
                    Datum::unmarshal_json(value.to_string().as_bytes()).map_err(invalid_state)?,
                );
            }
        }
        if let Some(types) = state.get("user-var-types").and_then(Value::as_object) {
            for (name, field_type) in types {
                self.user_vars.set_user_var_type(
                    name,
                    serde_json::from_value(field_type.clone()).map_err(invalid_state)?,
                );
            }
        }
        self.vars.restore_autocommit_status(
            state.get("status").and_then(Value::as_u64).unwrap_or(0) as u32,
        );
        self.prepared_statement_id = state
            .get("prepared-stmt-id")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32;
        self.current_db = state
            .get("current-db")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        *self.last_txn_info.borrow_mut() = state
            .get("txn-info")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if let Some(query) = state.get("query-info").filter(|q| !q.is_null()) {
            *self.last_query_info.borrow_mut() = query.to_string();
        }
        self.last_found_rows = state.get("found-rows").and_then(Value::as_u64).unwrap_or(0);
        self.found_in_plan_cache = state
            .get("in-plan-cache")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        self.found_in_binding = state
            .get("in-binding")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        self.prev_row_count = state
            .get("affected-rows")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        self.last_insert_id = state
            .get("last-insert-id")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        *self
            .sequence_last_values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            serde_json::from_value(state.get("seq-values").cloned().unwrap_or(json!({})))
                .map_err(invalid_state)?;
        self.warnings.clear();
        if let Some(warnings) = state.get("warnings").and_then(Value::as_array) {
            for warning in warnings {
                let error = warning.get("err").filter(|e| !e.is_null());
                self.warnings.push(SqlWarning {
                    level: match warning.get("level").and_then(Value::as_str) {
                        Some("Error") => WarningLevel::Error,
                        Some("Note") => WarningLevel::Note,
                        _ => WarningLevel::Warning,
                    },
                    code: error
                        .and_then(|e| e.get("code"))
                        .and_then(Value::as_u64)
                        .unwrap_or(1105) as u16,
                    message: error
                        .and_then(|e| e.get("message"))
                        .or_else(|| warning.get("msg"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                });
            }
        }
        Ok(())
    }
}

fn binding_time_to_json(text: &str) -> Value {
    tidb_datatype::parse_datetime(text, &chrono::Utc, true, true)
        .map(|parsed| Value::from(parsed.time.go_raw()))
        .unwrap_or(Value::from(0u64))
}

/// Parses and validates all supported state values before any restore handler mutates a session.
pub fn parse_session_states(text: &str) -> Result<Value, DriverError> {
    let mut decoder = serde_json::Deserializer::from_str(text);
    let mut state = Value::deserialize(&mut decoder).map_err(invalid_state)?;
    if state.is_null() {
        state = json!({});
    }
    let object = state
        .as_object_mut()
        .ok_or_else(|| invalid_state("session states must be a JSON object"))?;
    // Go encoding/json ignores null for scalar fields and accepts nil maps/slices.
    object.retain(|_, value| !value.is_null());
    for (name, value) in object.iter() {
        let valid = match name.as_str() {
            "prepared-stmt-id" | "status" => {
                value.as_u64().is_some_and(|n| u32::try_from(n).is_ok())
            }
            "found-rows" | "last-insert-id" => value.as_u64().is_some(),
            "affected-rows" => value.as_i64().is_some(),
            "in-plan-cache" | "in-binding" => value.is_boolean(),
            "current-db" | "txn-info" | "bindings" | "rs-group" => value.is_string(),
            "sys-vars" => serde_json::from_value::<BTreeMap<String, String>>(value.clone()).is_ok(),
            "prepared-stmts" => {
                serde_json::from_value::<BTreeMap<u32, PreparedStmtInfo>>(value.clone()).is_ok()
            }
            "seq-values" => serde_json::from_value::<BTreeMap<i64, i64>>(value.clone()).is_ok(),
            "user-var-types" => {
                serde_json::from_value::<BTreeMap<String, FieldType>>(value.clone()).is_ok()
            }
            "user-var-values" => value.as_object().is_some_and(|values| {
                values
                    .values()
                    .all(|datum| Datum::unmarshal_json(datum.to_string().as_bytes()).is_ok())
            }),
            "warnings" => value.is_array(),
            _ => true,
        };
        if !valid {
            return Err(invalid_state(format!(
                "invalid session state field '{name}'"
            )));
        }
    }
    Ok(state)
}

fn columns() -> Vec<(String, FieldType)> {
    ["Session_states", "Session_token"]
        .into_iter()
        .map(|name| (name.to_owned(), FieldType::new(FieldTypeCode::Json)))
        .collect()
}

pub(crate) fn prepared_columns(statement: &tidb_ast::Stmt) -> Option<Vec<(String, FieldType)>> {
    matches!(statement, tidb_ast::Stmt::Admin(admin) if matches!(&**admin, tidb_ast::AdminStmt::ShowInspection(show) if show.kind == tidb_ast::ShowInspectionKind::SessionStates)).then(columns)
}
