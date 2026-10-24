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

//! Go `HandleNonTransactionalDML` on the node: the shard SELECT runs as an
//! internal statement (`ExecuteInternal`) and every job as a statement of its
//! own (`ExecuteStmt`), so an auto-committed job publishes before the next
//! job reads. Run inside the BATCH statement, the jobs shared its buffer: a
//! later job's lookup met the earlier jobs' unpublished rows, and one failed
//! job discarded every job's writes.

use super::{map_error, ClusterServerSession, MaterializedResultSetSource, QueryResult};
use crate::domain::resultset_source::ResultSetSource;
use crate::domain::session_api::{QuerySession, SqlQueryError};
use tidb_datatype::Datum;
use tidb_executor::DriverError;
use crate::{NonTransactionalInput, NonTransactionalStep, StmtOutput};

fn driver_error(error: SqlQueryError) -> DriverError {
    DriverError::Mysql(tidb_executor::MysqlError::from_parts(
        error.code,
        error.state,
        error.message,
    ))
}

impl ClusterServerSession {
    pub(super) fn run_non_transactional_dml<'a>(
        &'a mut self,
        batch: &tidb_ast::BatchDmlStmt,
    ) -> Result<QueryResult<'a>, SqlQueryError> {
        let mut dml = self.store.session.begin_non_transactional_dml(batch);
        let mut input = NonTransactionalInput::Start;
        let outcome = loop {
            input = match self.store.session.non_transactional_step(&mut dml, input) {
                Err(error) => break Err(error),
                Ok(NonTransactionalStep::Done(output)) => break Ok(output),
                Ok(NonTransactionalStep::ShardSelect(select)) => {
                    NonTransactionalInput::ShardRows(self.non_transactional_rows(&select))
                }
                Ok(NonTransactionalStep::Job(job)) => {
                    NonTransactionalInput::JobDone(self.non_transactional_job(&job))
                }
            };
        };
        self.store.session.finish_non_transactional_dml(dml);
        let (columns, rows) = match outcome.map_err(map_error)? {
            StmtOutput::Rows { columns, rows } => (columns, rows),
            StmtOutput::Affected(_) | StmtOutput::Done(_) => {
                unreachable!("a BATCH statement answers with a result set")
            }
        };
        let field_types = columns.iter().map(|(_, field)| field.clone()).collect();
        let authority = self.store.session.result_materialization_authority();
        Ok(QueryResult::new(Box::new(MaterializedResultSetSource::new(
            crate::domain::pipeline_session::select_columns(&columns),
            rows,
        )))
        .with_cursor_materialization(field_types, authority)
        .with_statement_status(
            self.store.session.wire_warning_count(),
            crate::domain::wire_status::WireStatus::of_session(&self.store.session),
        ))
    }

    /// The shard SELECT's rows, read through the node's own statement path.
    fn non_transactional_rows(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>, DriverError> {
        let mut result = self.execute(sql).map_err(driver_error)?;
        let source = result.source();
        let mut rows = Vec::new();
        loop {
            let batch = source
                .next_batch(256)
                .map_err(|error| DriverError::unsupported(error.to_string()))?;
            if batch.is_empty() {
                break;
            }
            rows.extend(batch);
        }
        source
            .finish()
            .map_err(|error| DriverError::unsupported(error.to_string()))?;
        source
            .close()
            .map_err(|error| DriverError::unsupported(error.to_string()))?;
        Ok(rows)
    }

    /// One job's DML as a statement of its own, published on success when
    /// the session is auto-committing.
    fn non_transactional_job(&mut self, sql: &str) -> Result<(), DriverError> {
        if self.execute_write(sql).map_err(driver_error)?.is_some() {
            return Ok(());
        }
        self.non_transactional_rows(sql).map(|_| ())
    }
}
