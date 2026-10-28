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

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_ast::{LoadDataOnDuplicate, LoadDataStmt, Stmt};
use tidb_executor::load_data::LoadDataExec;

use crate::{DriverError, StmtOutput};

impl LoadDataExec for crate::Session {}

impl crate::Session {
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
            self.record_parse_failure_coded(reported.code, reported.message, reported.identity);
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
            return Err(DriverError::from(
                (&tidb_error::catalog::servererr::ERR_NOT_ALLOWED_COMMAND).generate_with_stack(
                    "The used command is not allowed with this MySQL version".to_owned(),
                ),
            ));
        }
        let prepared = self.prepare_load_data(load)?;
        if !load.local {
            // Go `InitDataFiles`: only a remote URL is read; a server path is
            // refused once the executor is built.
            return Err(DriverError::from((&tidb_util::dbterror::exeerrors::ERR_LOAD_DATA_FROM_SERVER_DISK).generate_with_stack(format!(
                    "Don't support load data from tidb-server's disk. Or if you want to load local data via client, the path of INFILE '{}' needs to specify the clause of LOCAL first",
                    load.path
                ))));
        }
        let data = data.unwrap_or_default();
        let env = self.load_data_env();
        let restrictive = env.strict_sql_mode && load.on_duplicate != LoadDataOnDuplicate::Ignore;
        let ctx = self
            .statement_context_ignoring(true, !restrictive)
            .with_statement_priority(crate::classify::statement_priority_of(stmt))
            .with_statement_class(tidb_executor::StatementClass::LoadData);
        let current_db = self.vars.current_db.clone();
        let result = self.with_staged_catalog(|catalog| {
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
        self.vars.statement_insert_id = ctx
            .published_last_insert_id()
            .unwrap_or_else(|| ctx.given_insert_id());
        self.statement_message = ctx.message();
        let (affected, _) = result?;
        Ok(StmtOutput::Affected(affected))
    }
}
