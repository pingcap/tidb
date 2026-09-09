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

//! Go `pkg/util/sqlexec/mock`.

use std::collections::VecDeque;
use std::sync::Mutex;

use tidb_ast::Stmt;
use tidb_datatype::Datum;
use tidb_resolve::ResultFieldRef;
use tidb_sqlexec::{ExecutionContext, OptionFuncAlias, RestrictedSqlExecutor, Result as SqlResult};
use tidb_util::sqlescape::SqlArg;

type RowsAndFields = (Vec<Vec<Datum>>, Vec<ResultFieldRef>);
type ParseCall =
    Box<dyn for<'a> FnOnce(&dyn ExecutionContext, &str, &[SqlArg<'a>]) -> SqlResult<Stmt> + Send>;
type StatementCall = Box<
    dyn FnOnce(&dyn ExecutionContext, &Stmt, &[OptionFuncAlias]) -> SqlResult<RowsAndFields> + Send,
>;
type SqlCall = Box<
    dyn for<'a> FnOnce(
            &dyn ExecutionContext,
            &[OptionFuncAlias],
            &str,
            &[SqlArg<'a>],
        ) -> SqlResult<RowsAndFields>
        + Send,
>;

/// Go `RestrictedSQLExecutorKey`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct RestrictedSqlExecutorKey;

impl std::fmt::Display for RestrictedSqlExecutorKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("__MockRestrictedSQLExecutor")
    }
}

/// Native recorder for Go's generated `MockRestrictedSQLExecutor`.
///
/// Each recorded callback is one expected call. Calls of the same method are
/// consumed in registration order; an unrecorded call panics as GoMock does.
pub struct MockRestrictedSqlExecutor {
    parse_calls: Mutex<VecDeque<ParseCall>>,
    statement_calls: Mutex<VecDeque<StatementCall>>,
    sql_calls: Mutex<VecDeque<SqlCall>>,
}

impl Default for MockRestrictedSqlExecutor {
    fn default() -> Self {
        Self {
            parse_calls: Mutex::new(VecDeque::new()),
            statement_calls: Mutex::new(VecDeque::new()),
            sql_calls: Mutex::new(VecDeque::new()),
        }
    }
}

impl MockRestrictedSqlExecutor {
    /// Go `NewMockRestrictedSQLExecutor` without a language-specific GoMock
    /// controller.
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `EXPECT`.
    pub const fn expect(&self) -> MockRestrictedSqlExecutorRecorder<'_> {
        MockRestrictedSqlExecutorRecorder { mock: self }
    }

    /// Go `ISGOMOCK`, represented as the same zero-sized marker result.
    pub const fn is_mock(&self) {}

    /// Fails if an expected call was not consumed, as `Controller.Finish`
    /// does for the generated Go mock.
    pub fn verify(&self) {
        let pending = self.parse_calls.lock().unwrap().len()
            + self.statement_calls.lock().unwrap().len()
            + self.sql_calls.lock().unwrap().len();
        assert_eq!(
            pending, 0,
            "missing {pending} restricted SQL executor call(s)"
        );
    }
}

impl Drop for MockRestrictedSqlExecutor {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.verify();
        }
    }
}

/// Recorder returned by [`MockRestrictedSqlExecutor::expect`].
pub struct MockRestrictedSqlExecutorRecorder<'a> {
    mock: &'a MockRestrictedSqlExecutor,
}

impl MockRestrictedSqlExecutorRecorder<'_> {
    /// Records one expected Go `ParseWithParams` call.
    pub fn parse_with_params(
        self,
        call: impl for<'a> FnOnce(&dyn ExecutionContext, &str, &[SqlArg<'a>]) -> SqlResult<Stmt>
            + Send
            + 'static,
    ) {
        self.mock
            .parse_calls
            .lock()
            .unwrap()
            .push_back(Box::new(call));
    }

    /// Records one expected Go `ExecRestrictedStmt` call.
    pub fn exec_restricted_stmt(
        self,
        call: impl FnOnce(&dyn ExecutionContext, &Stmt, &[OptionFuncAlias]) -> SqlResult<RowsAndFields>
            + Send
            + 'static,
    ) {
        self.mock
            .statement_calls
            .lock()
            .unwrap()
            .push_back(Box::new(call));
    }

    /// Records one expected Go `ExecRestrictedSQL` call.
    pub fn exec_restricted_sql(
        self,
        call: impl for<'a> FnOnce(
                &dyn ExecutionContext,
                &[OptionFuncAlias],
                &str,
                &[SqlArg<'a>],
            ) -> SqlResult<RowsAndFields>
            + Send
            + 'static,
    ) {
        self.mock
            .sql_calls
            .lock()
            .unwrap()
            .push_back(Box::new(call));
    }
}

impl RestrictedSqlExecutor for MockRestrictedSqlExecutor {
    fn parse_with_params(
        &self,
        context: &dyn ExecutionContext,
        sql: &str,
        arguments: &[SqlArg<'_>],
    ) -> SqlResult<Stmt> {
        let call = self
            .parse_calls
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected ParseWithParams call");
        call(context, sql, arguments)
    }

    fn exec_restricted_stmt(
        &self,
        context: &dyn ExecutionContext,
        statement: &Stmt,
        options: &[OptionFuncAlias],
    ) -> SqlResult<RowsAndFields> {
        let call = self
            .statement_calls
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected ExecRestrictedStmt call");
        call(context, statement, options)
    }

    fn exec_restricted_sql(
        &self,
        context: &dyn ExecutionContext,
        options: &[OptionFuncAlias],
        sql: &str,
        arguments: &[SqlArg<'_>],
    ) -> SqlResult<RowsAndFields> {
        let call = self
            .sql_calls
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected ExecRestrictedSQL call");
        call(context, options, sql, arguments)
    }
}

#[cfg(test)]
mod tests {
    use tidb_ast::{NodeBox, SessionStmt, Stmt};
    use tidb_sqlexec::{BackgroundContext, RestrictedSqlExecutor};

    use super::{MockRestrictedSqlExecutor, RestrictedSqlExecutorKey};

    #[test]
    fn key_and_generated_mock_contract() {
        assert_eq!(
            RestrictedSqlExecutorKey.to_string(),
            "__MockRestrictedSQLExecutor"
        );

        let mock = MockRestrictedSqlExecutor::new();
        let parsed = Stmt::Session(NodeBox::new(SessionStmt::Use("test".to_owned())));
        let expected_parse = parsed.clone();
        mock.expect().parse_with_params(move |_, sql, arguments| {
            assert_eq!(sql, "use test");
            assert!(arguments.is_empty());
            Ok(expected_parse)
        });
        let actual = mock
            .parse_with_params(&BackgroundContext, "use test", &[])
            .unwrap();
        assert_eq!(actual, parsed);

        let expected_statement = parsed.clone();
        mock.expect()
            .exec_restricted_stmt(move |_, statement, options| {
                assert_eq!(statement, &expected_statement);
                assert!(options.is_empty());
                Ok((Vec::new(), Vec::new()))
            });
        let result = mock.exec_restricted_stmt(&BackgroundContext, &parsed, &[]);
        assert!(result.is_ok());

        mock.expect()
            .exec_restricted_sql(|_, options, sql, arguments| {
                assert!(options.is_empty());
                assert_eq!(sql, "select 1");
                assert!(arguments.is_empty());
                Ok((Vec::new(), Vec::new()))
            });
        let result = mock.exec_restricted_sql(&BackgroundContext, &[], "select 1", &[]);
        assert!(result.is_ok());
        mock.verify();
    }

    #[deny(unused_must_use)]
    #[test]
    fn generated_constructor_and_expect_result_may_be_ignored_like_go() {
        MockRestrictedSqlExecutor::new();
        let mock = MockRestrictedSqlExecutor::new();
        mock.expect();
    }

    #[test]
    #[should_panic(expected = "missing 1 restricted SQL executor call")]
    fn an_unconsumed_expectation_is_rejected() {
        let mock = MockRestrictedSqlExecutor::new();
        mock.expect()
            .exec_restricted_sql(|_, _, _, _| Ok((Vec::new(), Vec::new())));
    }

    #[test]
    #[should_panic(expected = "unexpected ExecRestrictedSQL call")]
    fn an_unrecorded_call_is_rejected() {
        let mock = MockRestrictedSqlExecutor::new();
        let _ = mock.exec_restricted_sql(&BackgroundContext, &[], "select 1", &[]);
    }
}
