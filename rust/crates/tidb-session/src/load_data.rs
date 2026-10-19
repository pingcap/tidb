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

//! `LOAD DATA` in the session: Go's planner privilege check
//! (`buildLoadData`'s visit info), `ResetContextOfStmt`'s LOAD DATA arm and
//! `LoadDataExec`, over [`tidb_executor::load_data`].
//!
//! A LOCAL load is two steps on the wire. The connection first asks
//! [`crate::Session::accept_local_infile`], which runs everything Go runs
//! before `LoadDataExec.Open` sends the `0xfb` file request; it then hands
//! the client's bytes to [`crate::Session::set_local_infile_data`] and runs
//! the statement, whose dispatch arm takes them.

use tidb_ast::{LoadDataOnDuplicate, LoadDataStmt, Stmt};
use tidb_executor::load_data::{LoadDataEnv, PreparedLoadData};

use crate::privilege::GlobalPriv;
use crate::{DriverError, StmtOutput};

impl crate::Session {
    /// The store's regions this session's catalog reaches (Go
    /// `kv.SplittableStore`).
    pub fn region_store(
        &mut self,
    ) -> Result<std::sync::Arc<dyn tidb_executor::region_map::RegionStore>, DriverError> {
        self.with_catalog_mut(|catalog| Ok(catalog.region_store()))
    }

    fn load_data_env(&self) -> LoadDataEnv {
        LoadDataEnv {
            charset_database: self
                .vars
                .get_system("character_set_database")
                .unwrap_or_else(|_| "utf8mb4".to_owned()),
            strict_sql_mode: self.vars.sql_mode().has_strict_mode(),
            allow_write_row_id: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_OPT_WRITE_ROW_ID)
                .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value)),
        }
    }

    /// Go `buildLoadData`'s visit info: INSERT on the table, and DELETE too
    /// for REPLACE, named by the table as written.
    fn load_data_privileges(
        &self,
        schema: &str,
        table: &str,
        on_duplicate: LoadDataOnDuplicate,
    ) -> Result<(), DriverError> {
        let Some((_, user, host)) = self.privilege_context() else {
            return Ok(());
        };
        let mut required = vec![GlobalPriv::Insert];
        if on_duplicate == LoadDataOnDuplicate::Replace {
            required.push(GlobalPriv::Delete);
        }
        for privilege in required {
            let granted = crate::table_privilege::mem_db_verdict_mask(schema, privilege.bit())
                .unwrap_or_else(|| self.has_scoped_privilege(schema, table, privilege));
            if !granted {
                return Err(DriverError::TableAccessDenied {
                    privilege: privilege.print_name(),
                    user: user.to_owned(),
                    host: host.to_owned(),
                    table: table.to_owned(),
                });
            }
        }
        Ok(())
    }

    /// Everything Go checks before `LoadDataExec.Open`: the table, the
    /// privileges, then the executor builder and `NewLoadDataWorker`.
    pub(crate) fn prepare_load_data(
        &mut self,
        load: &LoadDataStmt,
    ) -> Result<PreparedLoadData, DriverError> {
        let current_db = self.current_db.clone();
        let env = self.load_data_env();
        let privileges = match load.table.as_slice() {
            [schema, table] => self.load_data_privileges(schema, table, load.on_duplicate),
            [table] if !current_db.is_empty() => {
                self.load_data_privileges(&current_db, table, load.on_duplicate)
            }
            _ => Ok(()),
        };
        self.with_catalog_mut(|catalog| {
            tidb_executor::load_data::prepare_load_data(load, catalog, &current_db, &env, |_, _| {
                privileges
            })
        })
    }

    /// Go `clientConn.preprocessLoadDataLocal` accepted the statement (the
    /// client can send files) and the executor was built: on success the
    /// connection may request the file. A failure is the statement's error,
    /// recorded as Go records one after its statement context reset.
    pub fn accept_local_infile(&mut self, stmt: &Stmt) -> Result<(), DriverError> {
        let Stmt::Dml(dml) = stmt else {
            return Ok(());
        };
        let tidb_ast::DmlStmt::LoadData(load) = dml.as_ref() else {
            return Ok(());
        };
        self.local_infile_accepted = true;
        // The statement context resets now that the connection accepted the
        // load (the parse kept the previous statement's warnings in case it
        // refused).
        self.warnings.clear();
        let prepared = self.prepare_load_data(load);
        if let Err(error) = &prepared {
            self.local_infile_accepted = false;
            let reported = error.clone().to_mysql_error();
            self.record_parse_failure_coded(reported.code, reported.message);
        }
        prepared.map(|_| ())
    }

    /// The client's file for the accepted LOCAL load, which the statement
    /// reads when it runs (each attempt, if the statement is retried).
    pub fn set_local_infile_data(&mut self, data: Vec<u8>) {
        self.local_infile_data = Some(data);
    }

    /// Ends the LOCAL load's file exchange once its statement finished.
    pub fn clear_local_infile(&mut self) {
        self.local_infile_data = None;
        self.local_infile_accepted = false;
    }

    /// Go `LoadDataExec` under `ResetContextOfStmt`'s `*ast.LoadDataStmt` arm.
    pub(crate) fn run_load_data_stmt(
        &mut self,
        load: &LoadDataStmt,
        stmt: &Stmt,
    ) -> Result<StmtOutput, DriverError> {
        let data = self.local_infile_data.clone();
        // Go `handleLoadData` refuses a LOCAL load the connection did not
        // accept before anything else runs.
        if load.local && (!self.local_infile_accepted || data.is_none()) {
            return Err(DriverError::DdlCoded {
                errno: 1148,
                message: "The used command is not allowed with this MySQL version".to_owned(),
            });
        }
        let prepared = self.prepare_load_data(load)?;
        if !load.local {
            // Go `InitDataFiles`: only a remote URL is read; a server path is
            // refused once the executor is built.
            return Err(DriverError::DdlCoded {
                errno: 8154,
                message: format!(
                    "Don't support load data from tidb-server's disk. Or if you want to load local data via client, the path of INFILE '{}' needs to specify the clause of LOCAL first",
                    load.path
                ),
            });
        }
        let data = data.unwrap_or_default();
        let env = self.load_data_env();
        let restrictive = env.strict_sql_mode && load.on_duplicate != LoadDataOnDuplicate::Ignore;
        let ctx = self
            .statement_context_ignoring(true, !restrictive)
            .with_statement_priority(crate::classify::statement_priority_of(stmt))
            .with_statement_class(tidb_executor::StatementClass::LoadData);
        let current_db = self.current_db.clone();
        let result = self.with_staged_catalog_for_path(&load.table, &current_db, |catalog| {
            tidb_executor::load_data::run_load_data(
                &prepared,
                &data,
                catalog,
                &current_db,
                &ctx,
                &env,
            )
        });
        self.drain_eval_warnings(&ctx);
        self.statement_insert_id = ctx
            .published_last_insert_id()
            .unwrap_or_else(|| ctx.given_insert_id());
        self.statement_message = ctx.message();
        let (affected, _) = result?;
        Ok(StmtOutput::Affected(affected))
    }
}
