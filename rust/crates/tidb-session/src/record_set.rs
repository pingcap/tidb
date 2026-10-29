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


//! The session side of `tidb_executor::record_set`: opening a statement's
//! record set and the session primitives it finishes through.

use crate::*;
use tidb_executor::record_set::{OpenedStatement, StatementRecordSet};
pub(crate) use tidb_executor::record_set::{PendingExecution, PendingQuery, QueryTransactionEnd};
pub(crate) use tidb_executor::stmt_ctx_build::StatementCompletion;

/// A statement either returns an opened query or has already executed.
pub type StatementExecution<'a> = tidb_executor::record_set::StatementExecution<'a, Session>;
/// An opened query over this session.
pub type SessionRecordSet<'a> = tidb_executor::record_set::SessionRecordSet<'a, Session>;

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

impl tidb_executor::record_set::RecordSetSession for Session {
    fn discard_stale_statement_transaction(&mut self) {
        Session::discard_stale_statement_transaction(self)
    }

    fn clear_statement_snapshot(&mut self) {
        self.statement_snapshot = None;
    }

    fn drain_eval_warnings(&mut self, ctx: &tidb_sessionctx::StmtContext) {
        Session::drain_eval_warnings(self, ctx);
    }

    fn set_last_result_names(
        &mut self,
        names: Vec<tidb_datatype::FieldNameMetadata>,
        current_db: &str,
    ) {
        self.last_result_output_names = names;
        current_db.clone_into(&mut self.last_result_current_db);
    }

    fn finish_statement_state(&mut self, result: &Result<StatementCompletion, DriverError>) {
        Session::finish_statement_state(self, result);
    }

    fn add_write_sql_resp_duration(&mut self, duration: std::time::Duration) {
        Session::add_write_sql_resp_duration(self, duration);
    }

    fn add_result_chunk_cells(&mut self, cells: i64) {
        Session::add_result_chunk_cells(self, cells);
    }

    fn observe_execute_duration(seconds: f64, internal: bool) {
        tidb_metrics::session::observe_execute_duration(seconds, internal);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::resultset_source::ResultSetSource;
    use crate::tests_support::query_text;

    // Go pkg/executor/adapter_internal_test.go::TestRecordSetNewChunkAfterFinish
    // and TestRecordSetNextAfterFinish, through the session-owned wrapper.
    #[test]
    fn record_set_after_finish() {
        let mut session = Session::new();
        let mut result = crate::tests_support::execute_stmt(&mut session, "SELECT 1")
            .expect("SELECT must return an opened executor");
        let result = result.source();
        result.finish().unwrap();
        let mut req = result.new_chunk().expect("an executor's chunk");
        assert_eq!(req.num_cols(), 1);
        assert_eq!(result.fields().unwrap().len(), 1);
        assert_eq!(result.next_chunk(&mut req).unwrap_err().code, 1317);
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
