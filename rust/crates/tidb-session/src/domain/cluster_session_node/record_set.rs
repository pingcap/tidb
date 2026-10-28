// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use super::*;
use crate::domain::resultset_source::{ResultSetSource, StatementStatus};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use crate::{OpenedStatement, StatementRecordSet};

/// The connection lends itself to the result until Close. Executor Finish
/// precedes snapshot teardown; statement Close follows transaction completion.
struct ClusterRecordSet<'a> {
    owner: ClusterServerSession<'a>,
    state: StatementRecordSet,
    field_types: Vec<FieldType>,
    snapshot: Option<ReadStatement>,
    finished: bool,
    closed: bool,
    failed: bool,
}


impl<'s> ClusterServerSession<'s> {



    pub(super) fn open_statement_result(
        mut self,
        shape: StatementReadShape,
        resource_group: &str,
        mut run: impl FnMut(&mut Session) -> Result<OpenedStatement, SqlQueryError>,
    ) -> Result<QueryResult<'s>, SqlQueryError> {
        // Only a replayable locking/write statement drains before returning.
        // Its retained chunks escape after the existing lock/retry loop succeeds.
        // Go `TiDBContext.ExecuteStmt` (`pkg/server/driver_tidb.go:376`)
        // appends every error `session.ExecuteStmt` returns to the statement
        // context. The session files its own planning and execution errors;
        // the transaction activation, snapshot and lock steps below are this
        // node's, so their failures are filed here.
        self.prepare_statement_context(
            resource_group,
            shape != StatementReadShape::AutocommitWrite,
        )
        .map_err(|error| self.record_statement_failure(error))?;
        let buffers_rows = shape == StatementReadShape::LockingRead
            && self
                .node
                .txn
                .explicit
                .as_ref()
                .is_some_and(|transaction| transaction.is_pessimistic());
        let (opened, snapshot) = if buffers_rows || shape == StatementReadShape::AutocommitWrite {
            // The drained record set of the latest attempt stays here, outside
            // the attempt loop, until the statement's outcome is known: a step
            // after the drain (the pessimistic lock) can still fail the
            // statement, and only this owner can close it with that error.
            let mut drained: Option<StatementRecordSet> = None;
            let opened = self.with_bound_statement(shape, resource_group, true, false, |session| {
                // A lock-conflict retry re-executes: Go closes the losing
                // attempt's executor and finishes the statement once.
                drained = None;
                match run(session)? {
                    OpenedStatement::Rows(mut state) => {
                        if let Err(error) = state.retain_chunks(session) {
                            let _ = state.close(session);
                            return Err(map_error(error));
                        }
                        drained = Some(state);
                        Ok(None)
                    }
                    opened => Ok(Some(opened)),
                }
            });
            let opened = match opened {
                Ok(Some(opened)) => opened,
                Ok(None) => OpenedStatement::Rows(
                    drained
                        .take()
                        .expect("a drained attempt left its record set"),
                ),
                // Go `handlePessimisticSelectForUpdate` drains and locks
                // inside `ExecStmt.Exec`, so a lock failure (3572 NOWAIT, 1205
                // timeout, 1213 deadlock) is an `ExecuteStmt` error: `runStmt`
                // sees no record set and finishes the statement with it
                // (`FinishExecuteStmt(txnStartTS, err, false)`), and Go's
                // `TiDBContext.ExecuteStmt` appends it.
                Err(error) if buffers_rows => {
                    if let Some(mut state) = drained.take() {
                        state.record_error(tidb_executor::DriverError::Mysql(
                            tidb_executor::MysqlError::from_parts(
                                error.code,
                                error.state,
                                error.message.clone(),
                            )
                            .with_identity_opt(error.identity),
                        ));
                        let _ = state.close(&mut *self.session);
                    }
                    return Err(self.record_statement_failure(error));
                }
                Err(error) => return Err(error),
            };
            (opened, None)
        } else {
            let snapshot = self
                .begin_read_statement(shape, resource_group)
                .map_err(|error| self.record_statement_failure(error))?;
            match run(&mut self.session) {
                Ok(opened) => (opened, Some(snapshot)),
                Err(error) => {
                    let _ = self.end_read_statement(snapshot, false);
                    return Err(error);
                }
            }
        };
        match opened {
            OpenedStatement::Rows(state) => {
                let field_types: Vec<_> = state
                    .columns()
                    .iter()
                    .map(|(_, field)| field.clone())
                    .collect();
                let authority = self.session.result_materialization_authority();
                Ok(QueryResult::new(Box::new(ClusterRecordSet {
                    owner: self,
                    state,
                    field_types: field_types.clone(),
                    snapshot,
                    finished: false,
                    closed: false,
                    failed: false,
                }))
                .with_cursor_materialization(field_types, authority))
            }
            OpenedStatement::Complete(output) => {
                if let Some(snapshot) = snapshot {
                    self.end_read_statement(snapshot, true)?;
                }
                let source = match output {
                    StmtOutput::Rows { columns, rows } => MaterializedResultSetSource::new(
                        crate::domain::pipeline_session::select_columns(&columns),
                        rows,
                    ),
                    StmtOutput::Affected(count) => {
                        crate::domain::pipeline_session::affected_rows_source(count)
                    }
                    StmtOutput::Done(_) => crate::domain::pipeline_session::affected_rows_source(0),
                };
                Ok(QueryResult::new(Box::new(source))
                    .with_statement_status(
                        self.session.wire_warning_count(),
                        WireStatus::of_session(&self.session),
                    )
                    .with_statement_output(
                        self.session.statement_affected_rows(),
                        self.session.statement_insert_id(),
                        self.session.statement_message().as_bytes().to_vec(),
                    ))
            }
        }
    }
}

impl ResultSetSource for ClusterRecordSet<'_> {
    fn statement_status(&self) -> Option<StatementStatus<'_>> {
        let session = &self.owner.session;
        Some(StatementStatus {
            warnings: session.wire_warning_count(),
            status: WireStatus::of_session(session),
            affected_rows: session.statement_affected_rows(),
            last_insert_id: session.statement_insert_id(),
            info: session.statement_message().as_bytes(),
        })
    }
    fn new_chunk(&self) -> Option<Chunk> {
        Some(self.state.new_chunk())
    }
    fn field_types(&self) -> &[FieldType] {
        &self.field_types
    }
    fn next_chunk(&mut self, chunk: &mut Chunk) -> Result<(), tidb_executor::MysqlError> {
        let result = self.state.next(&mut *self.owner.session, chunk);
        self.failed |= result.is_err();
        result.map_err(|error| error.to_mysql_error())
    }
    fn next_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_executor::MysqlError> {
        let mut chunk = self.state.new_chunk();
        chunk.set_required_rows(max_rows as isize, max_rows);
        self.next_chunk(&mut chunk)?;
        Ok((0..chunk.num_rows())
            .map(|index| chunk.get_row(index).get_datum_row(&self.field_types))
            .collect())
    }
    fn columns(&mut self) -> Result<Vec<tidb_protocol::ColumnInfo>, tidb_executor::MysqlError> {
        Ok(crate::domain::pipeline_session::result_set_columns(
            self.state.columns(),
            self.state.output_names(),
        ))
    }
    fn finish(&mut self) -> Result<(), tidb_executor::MysqlError> {
        if std::mem::replace(&mut self.finished, true) {
            return Ok(());
        }
        // The result set's field origins, as the in-process collect records
        // them for the statement that produced them.
        let (names, current_db) = self.state.output_names();
        let session = &mut *self.owner.session;
        session.last_result_output_names = names.to_vec();
        current_db.clone_into(&mut session.last_result_current_db);
        let result = self
            .state
            .finish(&mut *self.owner.session)
            .map_err(|error| error.to_mysql_error());
        self.failed |= result.is_err();
        let ended = self.snapshot.take().map_or(Ok(()), |snapshot| {
            self.owner
                .end_read_statement(snapshot, !self.failed)
                .map_err(|error| {
                    tidb_executor::MysqlError::from_parts(error.code, error.state, error.message)
                        .with_identity_opt(error.identity)
                })
        });
        if let Err(error) = &ended {
            self.state
                .record_error(tidb_executor::DriverError::Mysql(error.clone()));
        }
        result.and(ended)
    }
    fn close(&mut self) -> Result<(), tidb_executor::MysqlError> {
        if std::mem::replace(&mut self.closed, true) {
            return Ok(());
        }
        let finished = self.finish();
        let closed = self
            .state
            .close(&mut *self.owner.session)
            .map_err(|error| error.to_mysql_error());
        finished.and(closed)
    }
    fn add_write_sql_resp_duration(&mut self, duration: std::time::Duration) {
        self.owner.session.add_write_sql_resp_duration(duration);
    }
    fn add_result_chunk_cells(&mut self, cells: i64) {
        self.owner.session.add_result_chunk_cells(cells);
    }
}

impl Drop for ClusterRecordSet<'_> {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
