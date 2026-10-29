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

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use crate::{driver::QueryRecordSet, DriverError, StmtContext, StmtOutput};

use crate::stmt_ctx_build::StatementCompletion;

/// A statement either returns an opened query or has already executed.
pub enum StatementExecution<'a, S: RecordSetSession> {
    /// Query execution and its statement state remain owned by this result.
    Rows(SessionRecordSet<'a, S>),
    /// Statements that execute before returning a result, such as DML/SHOW.
    Complete(StmtOutput),
}


pub enum PendingExecution {
    Query(PendingQuery),
    Complete(StmtOutput),
}

impl PendingExecution {
    pub fn collect<S: RecordSetSession>(self, session: &mut S) -> Result<StmtOutput, DriverError> {
        match self {
            Self::Complete(output) => Ok(output),
            Self::Query(query) => query.collect(session),
        }
    }
}

#[derive(Default)]
pub enum QueryTransactionEnd {
    #[default]
    None,
    StaleRead,
    SnapshotRead,
}

impl QueryTransactionEnd {
    fn finish<S: RecordSetSession>(self, session: &mut S) -> Result<(), DriverError> {
        match self {
            // A one-statement stale transaction must end even after Next/Close fails.
            Self::StaleRead => session.discard_stale_statement_transaction(),
            Self::SnapshotRead => session.clear_statement_snapshot(),
            _ => {}
        }
        Ok(())
    }
}

pub struct PendingQuery {
    record_set: QueryRecordSet,
    context: StmtContext,
    pub transaction_end: QueryTransactionEnd,
    /// Go `ExecStmt.startExecute`: the moment the executor was opened; the
    /// run-duration observation at Finish/Close measures to here.
    execute_opened_at: std::time::Instant,
}

impl PendingQuery {
    pub fn new(record_set: QueryRecordSet, context: StmtContext) -> Self {
        Self {
            record_set,
            context,
            transaction_end: QueryTransactionEnd::None,
            execute_opened_at: std::time::Instant::now(),
        }
    }

    fn finish<S: RecordSetSession>(&mut self, session: &mut S) -> Result<(), DriverError> {
        S::observe_execute_duration(
            self.execute_opened_at.elapsed().as_secs_f64(),
            false,
        );
        session.session_vars_mut().use_chunk_alloc = self.context.is_chunk_alloc_valid();
        let result = self.record_set.finish();
        self.context.drain_fold_warnings();
        session.drain_eval_warnings(&self.context);
        let finished = std::mem::take(&mut self.transaction_end).finish(session);
        result.and(finished)
    }

    fn collect<S: RecordSetSession>(self, session: &mut S) -> Result<StmtOutput, DriverError> {
        S::observe_execute_duration(
            self.execute_opened_at.elapsed().as_secs_f64(),
            false,
        );
        // Go's result writer draws every chunk from the statement's
        // allocator, which marks `StmtCtx.useChunkAlloc` while it is valid.
        session.session_vars_mut().use_chunk_alloc = self.context.is_chunk_alloc_valid();
        let Self {
            record_set,
            context,
            transaction_end,
            execute_opened_at: _,
        } = self;
        session.set_last_result_names(record_set.output_names().to_vec(), record_set.current_db());
        let result = record_set.collect();
        context.drain_fold_warnings();
        session.drain_eval_warnings(&context);
        let finished = transaction_end.finish(session);
        let (columns, rows) = result?;
        finished?;
        Ok(StmtOutput::Rows { columns, rows })
    }
}

/// Owned execution state for one statement. A front-end result owner keeps
/// this together with its originating session and calls Close on every exit.
/// It contains no borrow into the session, so the outer owner can also finish
/// the transaction and snapshot without a self-reference.
pub struct StatementRecordSet {
    query: PendingQuery,
    retained: Option<std::collections::VecDeque<Chunk>>,
    retained_offset: usize,
    found_rows: u64,
    exhausted: bool,
    finished: bool,
    closed: bool,
    last_error: Option<DriverError>,
}

impl StatementRecordSet {
    pub fn new(query: PendingQuery) -> Self {
        Self {
            query,
            retained: None,
            retained_offset: 0,
            found_rows: 0,
            exhausted: false,
            finished: false,
            closed: false,
            last_error: None,
        }
    }

    /// Metadata remains available after Finish or Close, as in Go.
    pub fn columns(&self) -> &[(String, FieldType)] {
        self.query.record_set.columns()
    }

    /// Go `ExecStmt.OutputNames`, one per column, and the `CurrentDB` the
    /// statement opened under: the inputs of `colNames2ResultFields`.
    pub fn output_names(&self) -> (&[tidb_datatype::FieldNameMetadata], &str) {
        let record_set = &self.query.record_set;
        (record_set.output_names(), record_set.current_db())
    }

    /// Allocates the result's configured chunk shape.
    pub fn new_chunk(&self) -> Chunk {
        self.query.record_set.new_chunk()
    }

    /// Retains a failure from an outer transaction's Finish for CloseRecordSet.
    pub fn record_error(&mut self, error: DriverError) {
        self.last_error.get_or_insert(error);
    }

    /// Fills a reusable chunk and transfers warnings from this execution.
    pub fn next<S: RecordSetSession>(&mut self, session: &mut S, req: &mut Chunk) -> Result<(), DriverError> {
        let retained = self.retained.is_some();
        let result = if self.finished {
            Err(DriverError::Mysql(crate::MysqlError::from_terror(
                &tidb_dbterror::exeerrors::ERR_QUERY_INTERRUPTED
                    .generate_with_stack_by_args(&[]),
            )))
        } else if let Some(chunks) = &mut self.retained {
            req.reset();
            while req.num_rows() < req.required_rows() {
                let Some(chunk) = chunks.front() else { break };
                let end = (self.retained_offset + req.required_rows() - req.num_rows())
                    .min(chunk.num_rows());
                req.append_range_from(chunk, self.retained_offset, end);
                self.retained_offset = end;
                if end == chunk.num_rows() {
                    chunks.pop_front();
                    self.retained_offset = 0;
                }
            }
            Ok(())
        } else {
            self.query.record_set.next(req)
        };
        session.drain_eval_warnings(&self.query.context);
        match &result {
            // Go chunkRowRecordSet does not update LastFoundRows while replaying retained rows.
            Ok(()) if retained => {}
            Ok(()) if req.num_rows() == 0 => {
                self.exhausted = true;
                session.session_vars_mut().last_found_rows = self.found_rows;
            }
            Ok(()) => self.found_rows += req.num_rows() as u64,
            Err(error) => {
                self.last_error.get_or_insert_with(|| error.clone());
            }
        }
        result
    }

    /// Go runPessimisticSelectForUpdate retains chunks before locks/retries
    /// complete. Do not publish rows to a client from inside a replayable attempt.
    pub fn retain_chunks<S: RecordSetSession>(&mut self, session: &mut S) -> Result<(), DriverError> {
        let mut chunks = std::collections::VecDeque::new();
        let mut req = self.new_chunk();
        let result = (|| {
            loop {
                self.query.record_set.next(&mut req)?;
                if req.num_rows() == 0 {
                    break;
                }
                let next = req.renew(req.required_rows());
                chunks.push_back(std::mem::replace(&mut req, next));
            }
            self.query.record_set.finish()
        })();
        session.drain_eval_warnings(&self.query.context);
        if let Err(error) = &result {
            self.last_error.get_or_insert_with(|| error.clone());
        }
        result?;
        self.retained = Some(chunks);
        Ok(())
    }

    /// Finishes execution before the writer emits its terminal packet.
    pub fn finish<S: RecordSetSession>(&mut self, session: &mut S) -> Result<(), DriverError> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        let result = self.query.finish(session);
        if let Err(error) = &result {
            self.last_error.get_or_insert_with(|| error.clone());
        }
        result
    }

    /// Publishes the completed statement after execution has been finished.
    pub fn close<S: RecordSetSession>(&mut self, session: &mut S) -> Result<(), DriverError> {
        if self.closed {
            return Ok(());
        }
        let result = self.finish(session);
        self.closed = true;
        let completion = match self.last_error.clone() {
            Some(error) => Err(error),
            None => Ok(StatementCompletion::Rows(
                self.exhausted.then_some(self.found_rows),
            )),
        };
        session.finish_statement_state(&completion);
        result
    }
}

/// An opened statement whose outer owner will retain the session and snapshot.
#[must_use]
pub enum OpenedStatement {
    /// Execution is deferred to Next.
    Rows(StatementRecordSet),
    /// A statement that has already executed and published its effects.
    Complete(StmtOutput),
}

impl OpenedStatement {
    /// Keeps the originating session borrowed until the result closes.
    pub fn attach<S: RecordSetSession>(self, session: &mut S) -> StatementExecution<'_, S> {
        match self {
            Self::Rows(state) => StatementExecution::Rows(SessionRecordSet { session, state }),
            Self::Complete(output) => StatementExecution::Complete(output),
        }
    }
}

/// A session-borrowing result owner for callers without an outer transaction
/// wrapper. Drop and explicit Close use the same completion path.
pub struct SessionRecordSet<'a, S: RecordSetSession> {
    session: &'a mut S,
    state: StatementRecordSet,
}

impl<S: RecordSetSession> SessionRecordSet<'_, S> {
    /// Result metadata, including after Finish.
    pub fn columns(&self) -> &[(String, FieldType)] {
        self.state.columns()
    }
    /// See [`StatementRecordSet::output_names`].
    pub fn output_names(&self) -> (&[tidb_datatype::FieldNameMetadata], &str) {
        self.state.output_names()
    }
    /// Allocates a chunk with this statement's schema and sizing.
    pub fn new_chunk(&self) -> Chunk {
        self.state.new_chunk()
    }
    /// Current statement state while execution remains borrowed.
    pub fn session(&self) -> &S {
        self.session
    }
    /// Fills a reusable chunk.
    pub fn next(&mut self, req: &mut Chunk) -> Result<(), DriverError> {
        self.state.next(self.session, req)
    }
    /// Finishes the executor once.
    pub fn finish(&mut self) -> Result<(), DriverError> {
        self.state.finish(self.session)
    }
    /// See `RecordSetSession::add_write_sql_resp_duration`.
    pub fn add_write_sql_resp_duration(&mut self, duration: std::time::Duration) {
        self.session.add_write_sql_resp_duration(duration);
    }
    /// See `RecordSetSession::add_result_chunk_cells`.
    pub fn add_result_chunk_cells(&mut self, cells: i64) {
        self.session.add_result_chunk_cells(cells);
    }
    /// Publishes completion once.
    pub fn close(&mut self) -> Result<(), DriverError> {
        self.state.close(self.session)
    }
}

impl<S: RecordSetSession> Drop for SessionRecordSet<'_, S> {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// The session a record set finishes its statement through: Go's
/// `ExecStmt.FinishExecuteStmt` / `CloseRecordSet` reach back into `sctx`.
pub trait RecordSetSession: crate::stmt_ctx_build::ResetContextOfStmt {
    /// Ends a one-statement stale-read transaction.
    fn discard_stale_statement_transaction(&mut self);
    /// Drops the statement's pinned `tidb_snapshot` read.
    fn clear_statement_snapshot(&mut self);
    /// Moves the statement context's warnings onto the session.
    fn drain_eval_warnings(&mut self, ctx: &StmtContext);
    /// The result's column names and current database, which the next
    /// statement's `SHOW FIELDS`-style readers report.
    fn set_last_result_names(&mut self, names: Vec<tidb_datatype::FieldNameMetadata>, current_db: &str);
    /// Go `ExecStmt.FinishExecuteStmt`: the statement's completion state.
    fn finish_statement_state(&mut self, result: &Result<StatementCompletion, DriverError>);
    /// Go `StmtCtx`'s write-response phase duration.
    fn add_write_sql_resp_duration(&mut self, duration: std::time::Duration);
    /// The cells the result writer sent, for the statement's observation.
    fn add_result_chunk_cells(&mut self, cells: i64);
    /// Go `ExecStmt.finishExecutor`'s run observation.
    fn observe_execute_duration(seconds: f64, internal: bool);
}
