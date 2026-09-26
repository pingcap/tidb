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

use super::*;
use crate::resultset_source::{ResultSetSource, StatementStatus};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_session::{OpenedStatement, StatementRecordSet};

/// The connection lends itself to the result until Close. Executor Finish
/// precedes snapshot teardown; statement Close follows transaction completion.
struct ClusterRecordSet<'a> {
    owner: &'a mut ClusterServerSession,
    state: StatementRecordSet,
    field_types: Vec<FieldType>,
    snapshot: Option<ReadStatement>,
    finished: bool,
    closed: bool,
    failed: bool,
}

struct ReadStatement {
    _pin: schema_sync::SchemaPinGuard,
    read_ts: transactions::StatementReadTs,
    savepoint: BufferCheckpoint,
    autocommit: bool,
    resource_group: String,
}

impl ClusterServerSession {
    pub(super) fn open_read_snapshot(
        &self,
        shape: StatementReadShape,
        prelock_keys: &[Vec<u8>],
        retry_read_ts: Option<u64>,
        read_ts: &transactions::StatementReadTs,
        resource_group: &str,
    ) -> Result<Box<dyn ClusterSnapshot>, SqlQueryError> {
        if let Some(transaction) = &self.explicit {
            let locking = shape == StatementReadShape::LockingRead || !prelock_keys.is_empty();
            return match retry_read_ts {
                Some(ts) => transaction.snapshot_at_for(ts, locking),
                None if shape == StatementReadShape::LockingRead
                    && prelock_keys.is_empty()
                    && transaction.is_pessimistic() =>
                {
                    transaction.fresh_locking_snapshot()
                }
                None => transaction.snapshot_for(locking),
            }
            .map_err(SqlQueryError::unknown);
        }
        if shape == StatementReadShape::AutocommitPointGet {
            return self
                .transactions
                .open_max_ts_snapshot(resource_group)
                .map_err(SqlQueryError::unknown);
        }
        Ok(transactions::deferred_snapshot(
            Arc::clone(&self.transactions),
            read_ts.clone(),
            Arc::<str>::from(resource_group),
        ))
    }

    fn begin_read_statement(
        &mut self,
        shape: StatementReadShape,
        resource_group: &str,
    ) -> Result<ReadStatement, SqlQueryError> {
        if let Some(transaction) = &self.explicit {
            transaction
                .set_resource_group_name(resource_group)
                .map_err(SqlQueryError::unknown)?;
        }
        let autocommit = self.explicit.is_none();
        if autocommit {
            self.session.current_tso().clear();
        }
        let statement = ReadStatement {
            _pin: self
                .schema_pins
                .hold(self.connection_id, self.schema_version),
            read_ts: transactions::StatementReadTs::new(self.session.current_tso()),
            savepoint: self.buffer.checkpoint(),
            autocommit,
            resource_group: resource_group.to_owned(),
        };
        self.session.begin_external_executor_breakpoint_scope(true);
        let setup = self
            .open_read_snapshot(shape, &[], None, &statement.read_ts, resource_group)
            .and_then(|snapshot| {
                drop(self.bind(snapshot));
                self.declare_read_shape(shape);
                self.prepare_snapshot()
            });
        if let Err(error) = setup {
            let _ = self.end_read_statement(statement, false);
            return Err(error);
        }
        Ok(statement)
    }

    fn end_read_statement(
        &mut self,
        statement: ReadStatement,
        success: bool,
    ) -> Result<(), SqlQueryError> {
        let finished = self.finish_snapshot();
        let result = if success {
            finished
                .and_then(|()| self.commit_if_session_left_transaction())
                .and_then(|()| {
                    self.flush_if_autocommit(
                        statement.read_ts.get(),
                        None,
                        &statement.resource_group,
                    )
                })
        } else {
            self.buffer.restore(statement.savepoint);
            finished
        };
        self.session.end_external_executor_breakpoint_scope();
        if statement.autocommit {
            self.session.current_tso().clear();
        }
        self.session.set_selected_lock_keys(None);
        self.session
            .retry_auto_ids()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clean();
        self.session
            .publish_transaction_buffer_metrics(self.buffer.len(), self.buffer.memory_footprint());
        result
    }

    pub(super) fn open_statement_result<'a>(
        &'a mut self,
        shape: StatementReadShape,
        bind_prelock_keys: impl FnOnce(&Session) -> Vec<Vec<u8>>,
        resource_group: &str,
        mut run: impl FnMut(&mut Session) -> Result<OpenedStatement, SqlQueryError>,
    ) -> Result<QueryResult<'a>, SqlQueryError> {
        // Only a replayable locking/write statement drains before returning.
        // Its retained chunks escape after the existing lock/retry loop succeeds.
        self.prepare_statement_context(resource_group)?;
        let buffers_rows = shape == StatementReadShape::LockingRead
            && self
                .explicit
                .as_ref()
                .is_some_and(|transaction| transaction.is_pessimistic());
        let (opened, snapshot) = if buffers_rows || shape == StatementReadShape::AutocommitWrite {
            let prelock_keys = self.bind_statement_prelocks(shape, bind_prelock_keys);
            let opened =
                self.with_bound_statement(shape, &prelock_keys, resource_group, true, |session| {
                    let mut opened = run(session)?;
                    if let OpenedStatement::Rows(state) = &mut opened {
                        if let Err(error) = state.retain_chunks(session) {
                            let _ = state.close(session);
                            return Err(map_error(error));
                        }
                    }
                    Ok(opened)
                })?;
            (opened, None)
        } else {
            let snapshot = self.begin_read_statement(shape, resource_group)?;
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
                        crate::pipeline_session::select_columns(&columns),
                        rows,
                    ),
                    StmtOutput::Affected(count) => {
                        crate::pipeline_session::affected_rows_source(count)
                    }
                    StmtOutput::Done(_) => crate::pipeline_session::affected_rows_source(0),
                };
                Ok(QueryResult::new(Box::new(source))
                    .with_statement_status(
                        self.session.wire_warning_count(),
                        WireStatus::of_session(&self.session),
                    )
                    .with_statement_output(
                        0,
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
            affected_rows: 0,
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
        let result = self.state.next(&mut self.owner.session, chunk);
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
        Ok(crate::pipeline_session::select_columns(
            self.state.columns(),
        ))
    }
    fn finish(&mut self) -> Result<(), tidb_executor::MysqlError> {
        if std::mem::replace(&mut self.finished, true) {
            return Ok(());
        }
        let result = self
            .state
            .finish(&mut self.owner.session)
            .map_err(|error| error.to_mysql_error());
        self.failed |= result.is_err();
        let ended = self.snapshot.take().map_or(Ok(()), |snapshot| {
            self.owner
                .end_read_statement(snapshot, !self.failed)
                .map_err(|error| {
                    tidb_executor::MysqlError::from_parts(error.code, error.state, error.message)
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
            .close(&mut self.owner.session)
            .map_err(|error| error.to_mysql_error());
        finished.and(closed)
    }
}

impl Drop for ClusterRecordSet<'_> {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
