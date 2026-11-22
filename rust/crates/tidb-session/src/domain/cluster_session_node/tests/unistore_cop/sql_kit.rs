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


//! The statement helpers the tests moved off the executor's in-memory tier
//! share (X11): a session in `test`, displayed rows, the error a statement
//! fails with, and Go's `explain format = 'brief'` with plan ids stripped.

use super::*;
use crate::domain::resultset_source::ResultSetSource as _;

pub(super) fn session(stack: &UnistoreClusterStack, connection: u64) -> Session {
    let mut session = stack.factory.open_session(session_context(connection)).expect("session opens");
    rows(&mut session, "use test");
    session
}

pub(super) fn q(tk: &mut Session, sql: &str) -> Vec<Vec<String>> {
    displayed(rows(tk, sql))
}

/// The error a statement fails with, whether it fails while compiling or
/// while its rows are fetched (Go's `tk.ExecToErr` / `QueryToErr`).
pub(super) struct StatementError {
    pub(super) code: u16,
    pub(super) message: String,
}

pub(super) fn error(tk: &mut Session, sql: &str) -> StatementError {
    let mut result = match tk.node_conn().execute(sql) {
        Ok(result) => result,
        Err(error) => return StatementError { code: error.code, message: error.message },
    };
    let source = result.source();
    loop {
        match source.next_batch(64) {
            Ok(batch) if batch.is_empty() => break,
            Ok(_) => {}
            Err(error) => {
                let _ = source.close();
                return StatementError { code: error.code, message: error.message };
            }
        }
    }
    if let Err(error) = source.finish() {
        let _ = source.close();
        return StatementError { code: error.code, message: error.message };
    }
    source.close().expect("close");
    panic!("{sql}: the statement succeeded")
}

pub(super) fn exec_all(tk: &mut Session, sqls: &[&str]) {
    for sql in sqls {
        rows(tk, sql);
    }
}

pub(super) fn explain_brief(tk: &mut Session, sql: &str) -> Vec<Vec<String>> {
    displayed(rows(tk, &format!("explain format = 'brief' {sql}")))
        .into_iter()
        .map(|mut row| {
            if let Some((name, id)) = row[0].rsplit_once('_') {
                if !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()) {
                    row[0] = name.to_owned();
                }
            }
            row
        })
        .collect()
}

/// The brief plan's lines joined by tabs, for substring checks.
pub(super) fn plan_lines(tk: &mut Session, sql: &str) -> Vec<String> {
    explain_brief(tk, sql).into_iter().map(|row| row.join("\t")).collect()
}

/// Runs `sql` and answers the rows it affected.
pub(super) fn affected(tk: &mut Session, sql: &str) -> u64 {
    rows(tk, sql);
    tk.affected_rows()
}

/// The codes of the statement's warnings, from `SHOW WARNINGS`.
pub(super) fn warning_codes(tk: &mut Session) -> Vec<String> {
    q(tk, "SHOW WARNINGS").into_iter().map(|row| row[1].clone()).collect()
}

/// The result set's field metadata as the wire would carry it.
pub(super) fn fields(tk: &mut Session, sql: &str) -> Vec<tidb_executor::adapter::ResultFieldMetadata> {
    let mut result = tk.node_conn().execute(sql).expect(sql);
    let source = result.source();
    let fields = source.fields().expect("fields");
    while !source.next_batch(64).expect("batch").is_empty() {}
    source.finish().expect("finish");
    source.close().expect("close");
    fields
}

/// The result set's column names as the wire would carry them.
pub(super) fn field_names(tk: &mut Session, sql: &str) -> Vec<String> {
    fields(tk, sql).into_iter().map(|field| field.name).collect()
}
