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

//! Go `pkg/executor/plan_replayer.go`'s `PlanReplayerLoadInfo.Update`: a
//! `PLAN REPLAYER LOAD` archive the client transferred rebuilds its
//! variables, schemas, TiFlash replicas, views, statistics and bindings on
//! this session.

use std::collections::BTreeSet;

use tidb_context::WarningLevel;
use tidb_executor::analyze::exec::AnalyzeExec as _;
use tidb_executor::sessionctx::SessionContext as _;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

use super::{map_error, ClusterServerSession};

/// Go `domain.PlanReplayerSchemaMetaFile`.
const PLAN_REPLAYER_SCHEMA_META_FILE: &str = "schema_meta.txt";
/// Go `domain.PlanReplayerTiFlashReplicasFile`.
const PLAN_REPLAYER_TIFLASH_REPLICAS_FILE: &str = "table_tiflash_replica.txt";
/// Go `domain.PlanReplayerVariablesFile`.
const PLAN_REPLAYER_VARIABLES_FILE: &str = "variables.toml";
/// Go `domain.PlanReplayerSessionBindingFile`.
const PLAN_REPLAYER_SESSION_BINDING_FILE: &str = "session_bindings.sql";
/// Go `domain.PlanReplayerGlobalBindingFile`.
const PLAN_REPLAYER_GLOBAL_BINDING_FILE: &str = "global_bindings.sql";

/// Go `errors.New` / `errors.NewNoStackErrorf` through `AppendWarning`.
const ER_UNKNOWN_ERROR: u16 = 1105;

fn unknown(message: impl Into<String>) -> SqlQueryError {
    SqlQueryError::unknown(message)
}

impl ClusterServerSession<'_> {
    /// Go `PlanReplayerLoadInfo.Update` updates the data of the
    /// corresponding table.
    pub(super) fn plan_replayer_load_update(&mut self, data: &[u8]) -> Result<(), SqlQueryError> {
        let z = tidb_replayer::zip::read_zip(data).map_err(|error| unknown(error.to_string()))?;

        // load variable
        self.load_variables(&z)?;
        // Explicitly disable auto-analyze after restore so imported stats stay
        // stable. Users can re-enable it manually when they no longer need a
        // frozen replay env.
        self.disable_auto_analyze_for_plan_replayer_load()?;
        // build schema and table first
        let database_sets = self.create_table(&z)?;

        // set tiflash replica if exists
        self.load_set_tiflash_replica(&z);

        // build view next
        for (name, contents) in &z {
            let path: Vec<&str> = name.split('/').collect();
            if path.len() == 2 && path[0] == "view" {
                self.create_schema_and_items(contents)?;
            }
        }

        // load stats
        for (name, contents) in &z {
            let path: Vec<&str> = name.split('/').collect();
            if path.len() == 2 && path[0] == "stats" {
                self.load_stats(name, contents)?;
            }
        }

        self.load_all_bindings(&z, &database_sets);

        // Notify users that PLAN REPLAYER LOAD disables auto-analyze to keep
        // restored stats stable.
        self.session.append_warning(
            WarningLevel::Warning,
            ER_UNKNOWN_ERROR,
            format!(
                "`PLAN REPLAYER LOAD` sets @@global.{}=OFF to keep restored statistics stable; re-enable it manually if needed",
                tidb_vardef::tidb_vars::TIDB_ENABLE_AUTO_ANALYZE
            ),
        );
        Ok(())
    }

    /// Go `loadVariables`.
    fn load_variables(&mut self, z: &[(String, Vec<u8>)]) -> Result<(), SqlQueryError> {
        let mut un_load_vars = Vec::new();
        for (name, contents) in z {
            if name == PLAN_REPLAYER_VARIABLES_FILE {
                un_load_vars = tidb_util_config::load_config_for_plan_replayer_load(
                    &mut self.session.vars,
                    contents.as_slice(),
                )
                .map_err(|error| unknown(error.to_string()))?;
            }
        }
        if !un_load_vars.is_empty() {
            self.session.append_warning(
                WarningLevel::Warning,
                ER_UNKNOWN_ERROR,
                format!("variables set failed:{}", un_load_vars.join(",")),
            );
        }
        Ok(())
    }

    /// Go `disableAutoAnalyzeForPlanReplayerLoad`: plan replayer loads
    /// recorded statistics, which an auto-analyze right after the restore
    /// would overwrite.
    fn disable_auto_analyze_for_plan_replayer_load(&mut self) -> Result<(), SqlQueryError> {
        self.session
            .set_global_sys_var(tidb_vardef::tidb_vars::TIDB_ENABLE_AUTO_ANALYZE, "OFF".to_owned())
            .map_err(map_error)
    }

    /// Go `PlanReplayerLoadInfo.createTable`: the dumped schemas, with
    /// foreign key checks off and placement ignored, since their order is not
    /// guaranteed; answers the dump's databases.
    fn create_table(&mut self, z: &[(String, Vec<u8>)]) -> Result<BTreeSet<String>, SqlQueryError> {
        let read = |vars: &tidb_variable::vars::SessionVars, name: &str| vars.get_system(name).unwrap_or_default();
        let origin_foreign_key_checks = read(&self.session.vars, tidb_vardef::FOREIGN_KEY_CHECKS);
        let origin_placement_mode = read(&self.session.vars, tidb_vardef::tidb_vars::TIDB_PLACEMENT_MODE);
        let set = |vars: &mut tidb_variable::vars::SessionVars, name: &str, value: String| {
            let _ = vars.set_system(name, value);
        };
        set(&mut self.session.vars, tidb_vardef::FOREIGN_KEY_CHECKS, "OFF".to_owned());
        set(&mut self.session.vars, tidb_vardef::tidb_vars::TIDB_PLACEMENT_MODE, "IGNORE".to_owned());
        let outcome = (|| {
            let mut database_sets = BTreeSet::new();
            for (name, contents) in z {
                if *name == format!("schema/{PLAN_REPLAYER_SCHEMA_META_FILE}") {
                    let text = String::from_utf8_lossy(contents);
                    for row in text.split('\n') {
                        for m in row.split(';') {
                            if m.is_empty() {
                                continue;
                            }
                            let s = m.split('.').next().unwrap_or_default();
                            database_sets.insert(s.to_owned());
                        }
                    }
                    continue;
                }
                let path: Vec<&str> = name.split('/').collect();
                if path.len() == 2 && path[0] == "schema" {
                    self.create_schema_and_items(contents)?;
                }
            }
            Ok(database_sets)
        })();
        set(&mut self.session.vars, tidb_vardef::FOREIGN_KEY_CHECKS, origin_foreign_key_checks);
        set(&mut self.session.vars, tidb_vardef::tidb_vars::TIDB_PLACEMENT_MODE, origin_placement_mode);
        outcome
    }

    /// Go `createSchemaAndItems` creates schema and tables or views.
    fn create_schema_and_items(&mut self, contents: &[u8]) -> Result<(), SqlQueryError> {
        let origin_text = String::from_utf8_lossy(contents).into_owned();
        let stmts = tidb_parser::parse_multi_with_sql_mode_and_config(
            &origin_text,
            tidb_sessionctx::scanner_sql_mode(self.session.vars.sql_mode()),
            self.session.vars.build_parser_config(),
        )
        .map_err(|error| {
            map_error(match error.terror {
                Some(proto) => tidb_driver_error::DriverError::from(proto.gen_with_stack(error.message)),
                None => tidb_driver_error::DriverError::from(tidb_util::misc::syntax_error(
                    error.compatibility_message(&origin_text),
                )),
            })
        })?;
        if stmts.is_empty() {
            return Err(unknown("plan replayer: empty schema file"));
        }
        for (i, stmt) in stmts.iter().enumerate() {
            let sql_text = String::from_utf8_lossy(stmt.text()).into_owned();
            if sql_text.trim().is_empty() {
                continue;
            }
            let outcome = self.execute_nested(&sql_text);
            if i == 0 {
                // create database if not exists
                continue;
            }
            if let Err(error) = outcome {
                if error.code == tidb_errno::errcode::ErrTableExists {
                    continue;
                }
                return Err(error);
            }
        }
        Ok(())
    }

    /// Go `ctx.GetSQLExecutor().Execute(ctx, sql)`: one statement on this
    /// session, as the connection runs one.
    fn execute_nested(&mut self, sql: &str) -> Result<(), SqlQueryError> {
        let stmt = self.session.parse_at_statement_boundary(sql).map_err(map_error)?;
        self.reborrow().execute_stmt(sql, &stmt).map(drop)
    }

    /// Go `loadSetTiFlashReplica`; every failure is skipped.
    fn load_set_tiflash_replica(&mut self, z: &[(String, Vec<u8>)]) {
        for (name, contents) in z {
            if name != PLAN_REPLAYER_TIFLASH_REPLICAS_FILE {
                continue;
            }
            let text = String::from_utf8_lossy(contents).into_owned();
            for row in text.split('\n') {
                if row.is_empty() {
                    continue;
                }
                let r: Vec<&str> = row.split('\t').collect();
                if r.len() < 3 {
                    continue;
                }
                let (db_name, table_name, replica_count) = (r[0], r[1], r[2].trim());
                let sql = format!("alter table {db_name}.{table_name} set tiflash replica {replica_count}");
                if let Err(error) = self.execute_nested(&sql) {
                    // Without TiFlash stores, use one hypothetical replica so
                    // the optimizer can still reproduce TiFlash access paths.
                    if error.error_string().contains("total tiflash server count: 0") {
                        let sql = format!("alter table {db_name}.{table_name} set hypo tiflash replica 1");
                        let _ = self.execute_nested(&sql);
                    }
                }
            }
        }
    }

    /// Go `loadStats`: a file that does not decode is a warning, a dump of
    /// a table without statistics is a no-op.
    fn load_stats(&mut self, name: &str, contents: &[u8]) -> Result<(), SqlQueryError> {
        let json = match tidb_stats_handle_storage::json::parse_stats_json(&String::from_utf8_lossy(contents)) {
            Ok(json) => json,
            Err(error) => {
                self.session.append_warning(
                    WarningLevel::Warning,
                    ER_UNKNOWN_ERROR,
                    format!("fail to unmarshal stats JSON for file {name}\n{error}"),
                );
                return Ok(());
            }
        };
        // Keep plan replayer consistent with LOAD STATS: a dumped stats file
        // can be null when the source table has no stats.
        if json.table_name.is_empty() && json.version == 0 {
            return Ok(());
        }
        let service = self.stats_statement_service();
        service
            .load_stats(&json, self.session.historical_stats_enabled())
            .map_err(map_error)
    }

    /// Go `loadAllBindings`.
    fn load_all_bindings(&mut self, z: &[(String, Vec<u8>)], database_sets: &BTreeSet<String>) {
        for (name, contents) in z {
            if name == PLAN_REPLAYER_SESSION_BINDING_FILE {
                self.load_bindings(contents, database_sets, true);
            } else if name == PLAN_REPLAYER_GLOBAL_BINDING_FILE {
                self.load_bindings(contents, database_sets, false);
            }
        }
    }

    /// Go `loadBindings`: the bindings recreated `USING` their bind SQL,
    /// since the original SQL is normalized and cannot run.
    fn load_bindings(&mut self, contents: &[u8], database_sets: &BTreeSet<String>, is_session: bool) {
        let text = String::from_utf8_lossy(contents).into_owned();
        if text.is_empty() {
            return;
        }
        for binding in text.split('\n') {
            let cols: Vec<&str> = binding.split('\t').collect();
            if cols.len() < 3 {
                continue;
            }
            let binding_sql = cols[1];
            let default_db = cols[2];
            // Go reads `cols[3]` unguarded, which a three-column row panics on.
            let enabled = cols[3];
            // An empty defaultDB is a universal binding, which doesn't need
            // to check databaseSets.
            if !default_db.is_empty() && !database_sets.contains(default_db) {
                continue;
            }
            if enabled == "enabled" {
                let sql = format!(
                    "CREATE {} BINDING USING {binding_sql}",
                    if is_session { "SESSION" } else { "GLOBAL" }
                );
                if let Err(error) = self.execute_nested(&sql) {
                    tidb_logutil::bg_logger().warn(
                        "load bindings failed",
                        &[
                            tidb_logutil::zap_error(error.error_string()),
                            tidb_log::Field::new("sql", tidb_log::Value::Str(sql)),
                        ],
                    );
                }
            }
        }
    }
}
