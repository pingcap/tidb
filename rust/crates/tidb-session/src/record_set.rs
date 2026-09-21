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
use tidb_executor::{driver::QueryRecordSet, DriverError, StmtContext};

use crate::{Session, StmtOutput};

/// A statement either returns an opened query or has already executed.
pub enum StatementExecution<'a> {
    /// Query execution and its statement state remain owned by this result.
    Rows(SessionRecordSet<'a>),
    /// Statements that execute before returning a result, such as DML/SHOW.
    Complete(StmtOutput),
}

impl Session {
    pub(crate) fn return_opened_record_set(
        &mut self,
        result: Result<PendingExecution, DriverError>,
    ) -> Result<OpenedStatement, DriverError> {
        match result {
            Ok(PendingExecution::Query(query)) => {
                Ok(OpenedStatement::Rows(StatementRecordSet::new(query)))
            }
            Ok(PendingExecution::Complete(output)) => self
                .finish_statement_execution(Ok(output))
                .map(OpenedStatement::Complete),
            Err(error) => self
                .finish_statement_execution(Err(error))
                .map(OpenedStatement::Complete),
        }
    }
}

pub(crate) enum PendingExecution {
    Query(PendingQuery),
    Complete(StmtOutput),
}

impl PendingExecution {
    pub(crate) fn collect(self, session: &mut Session) -> Result<StmtOutput, DriverError> {
        match self {
            Self::Complete(output) => Ok(output),
            Self::Query(query) => query.collect(session),
        }
    }
}

#[derive(Default)]
pub(crate) enum QueryTransactionEnd {
    #[default]
    None,
    AutocommitRead,
    StaleRead,
}

impl QueryTransactionEnd {
    fn finish(self, session: &mut Session, succeeded: bool) -> Result<(), DriverError> {
        match self {
            Self::AutocommitRead if succeeded && !session.in_transaction() => {
                let start_ts = session.lock_catalog()?.allocate_tso();
                session.set_last_txn_info_started(start_ts);
            }
            // A one-statement stale transaction must end even after Next/Close fails.
            Self::StaleRead => session.discard_stale_statement_transaction(),
            _ => {}
        }
        Ok(())
    }
}

pub(crate) struct PendingQuery {
    record_set: QueryRecordSet,
    context: StmtContext,
    pub(crate) transaction_end: QueryTransactionEnd,
    /// Go `ExecStmt.startExecute`: the moment the executor was opened; the
    /// run-duration observation at Finish/Close measures to here.
    execute_opened_at: std::time::Instant,
}

impl PendingQuery {
    pub(crate) fn new(record_set: QueryRecordSet, context: StmtContext) -> Self {
        Self {
            record_set,
            context,
            transaction_end: QueryTransactionEnd::None,
            execute_opened_at: std::time::Instant::now(),
        }
    }

    fn finish(&mut self, session: &mut Session) -> Result<(), DriverError> {
        crate::metrics::observe_execute_duration(
            self.execute_opened_at.elapsed().as_secs_f64(),
            false,
        );
        let result = self.record_set.finish();
        session.drain_eval_warnings(&self.context);
        let finished = std::mem::take(&mut self.transaction_end).finish(session, result.is_ok());
        result.and(finished)
    }

    fn collect(self, session: &mut Session) -> Result<StmtOutput, DriverError> {
        crate::metrics::observe_execute_duration(
            self.execute_opened_at.elapsed().as_secs_f64(),
            false,
        );
        let Self {
            record_set,
            context,
            transaction_end,
            execute_opened_at: _,
        } = self;
        let result = record_set.collect();
        session.drain_eval_warnings(&context);
        let finished = transaction_end.finish(session, result.is_ok());
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
    pub(crate) fn new(query: PendingQuery) -> Self {
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

    /// Allocates the result's configured chunk shape.
    pub fn new_chunk(&self) -> Chunk {
        self.query.record_set.new_chunk()
    }

    /// Retains a failure from an outer transaction's Finish for CloseRecordSet.
    pub fn record_error(&mut self, error: DriverError) {
        self.last_error.get_or_insert(error);
    }

    /// Fills a reusable chunk and transfers warnings from this execution.
    pub fn next(&mut self, session: &mut Session, req: &mut Chunk) -> Result<(), DriverError> {
        let retained = self.retained.is_some();
        let result = if self.finished {
            Err(DriverError::Mysql(tidb_executor::MysqlError::new(
                1317,
                "Query execution was interrupted",
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
                session.last_found_rows = self.found_rows;
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
    pub fn retain_chunks(&mut self, session: &mut Session) -> Result<(), DriverError> {
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
    pub fn finish(&mut self, session: &mut Session) -> Result<(), DriverError> {
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
    pub fn close(&mut self, session: &mut Session) -> Result<(), DriverError> {
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
    pub fn attach(self, session: &mut Session) -> StatementExecution<'_> {
        match self {
            Self::Rows(state) => StatementExecution::Rows(SessionRecordSet { session, state }),
            Self::Complete(output) => StatementExecution::Complete(output),
        }
    }
}

/// A session-borrowing result owner for callers without an outer transaction
/// wrapper. Drop and explicit Close use the same completion path.
pub struct SessionRecordSet<'a> {
    session: &'a mut Session,
    state: StatementRecordSet,
}

impl SessionRecordSet<'_> {
    /// Result metadata, including after Finish.
    pub fn columns(&self) -> &[(String, FieldType)] {
        self.state.columns()
    }
    /// Allocates a chunk with this statement's schema and sizing.
    pub fn new_chunk(&self) -> Chunk {
        self.state.new_chunk()
    }
    /// Current statement state while execution remains borrowed.
    pub fn session(&self) -> &Session {
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
    /// Publishes completion once.
    pub fn close(&mut self) -> Result<(), DriverError> {
        self.state.close(self.session)
    }
}

impl Drop for SessionRecordSet<'_> {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

pub(crate) enum StatementCompletion {
    Rows(Option<u64>),
    Affected(u64),
    Done,
}

impl From<&StmtOutput> for StatementCompletion {
    fn from(output: &StmtOutput) -> Self {
        match output {
            StmtOutput::Rows { rows, .. } => Self::Rows(Some(rows.len() as u64)),
            StmtOutput::Affected(rows) => Self::Affected(*rows),
            StmtOutput::Done(_) => Self::Done,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests_support::query_text;

    // Go pkg/executor/adapter_internal_test.go::TestRecordSetNewChunkAfterFinish
    // and TestRecordSetNextAfterFinish, through the session-owned wrapper.
    #[test]
    fn record_set_after_finish() {
        let mut session = Session::new();
        let stmt = session.parse_statement("SELECT 1").unwrap();
        let StatementExecution::Rows(mut result) =
            session.execute_record_set_parsed(stmt, "SELECT 1").unwrap()
        else {
            panic!("SELECT must return an opened executor");
        };
        result.finish().unwrap();
        let mut req = result.new_chunk();
        assert_eq!(req.num_cols(), 1);
        assert_eq!(result.columns().len(), 1);
        assert_eq!(
            result.next(&mut req).unwrap_err().to_mysql_error().code,
            1317
        );
        result.close().unwrap();
        result.close().unwrap();
    }

    // Go pkg/expression/integration_test/integration_test.go::TestInfoBuiltin,
    // the found_rows sequence, using Next/Finish/Close rather than eager Run.
    #[test]
    fn info_builtin_found_rows() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a INT)").unwrap();
        query_text(&mut session, "SELECT * FROM t");
        assert_eq!(query_text(&mut session, "SELECT FOUND_ROWS()").1, [["0"]]);
        assert_eq!(query_text(&mut session, "SELECT FOUND_ROWS()").1, [["1"]]);
        session.run("INSERT INTO t VALUES (1),(2),(2)").unwrap();
        for (sql, count) in [
            ("SELECT * FROM t", "3"),
            ("SELECT * FROM t WHERE a=0", "0"),
            ("SELECT * FROM t WHERE a=1", "1"),
            ("SELECT * FROM t WHERE a LIKE '2'", "2"),
            ("SHOW TABLES LIKE 't'", "1"),
            ("SELECT COUNT(*) FROM t", "1"),
        ] {
            query_text(&mut session, sql);
            assert_eq!(query_text(&mut session, "SELECT FOUND_ROWS()").1, [[count]]);
        }
    }
}
