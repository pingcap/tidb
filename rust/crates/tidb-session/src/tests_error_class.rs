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

//! The Go error class (`[<class>:<code>]`) that `err.Error()` prints.
//!
//! Go keeps the class on the `*terror.Error` value. The wire message drops it
//! (`terror.ToSQLError`), but `@@tidb_last_query_info`'s `error` field, the
//! non-transactional DML job failure text and every other embedded
//! `err.Error()` carry it. Every expectation below was CAPTURED from the Go
//! oracle (`select @@tidb_last_query_info` right after the failing statement).
#![cfg(test)]

use crate::tests_support::scalar_text;
use crate::Session;

/// The `error` field of `@@tidb_last_query_info`, `None` when the last
/// recorded statement succeeded.
fn last_error(session: &mut Session) -> Option<String> {
    let info = scalar_text(session, "SELECT @@tidb_last_query_info").unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&info).unwrap();
    parsed
        .get("error")
        .and_then(|error| error.as_str())
        .map(str::to_owned)
}

fn session_with_table() -> Session {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE u1 (a INT PRIMARY KEY, b VARCHAR(3) NOT NULL, c INT UNIQUE, \
             d TINYINT UNSIGNED, e DECIMAL(5,2), f DATE, g JSON, h ENUM('x','y'))",
        )
        .unwrap();
    session
        .run("INSERT INTO u1 VALUES (1,'a',1,1,1.5,'2020-01-01','{}','x')")
        .unwrap();
    session
}

/// Each statement fails at execution time under a different Go class; the
/// text `last_query_info` records starts with that class and code.
#[test]
fn last_query_info_error_carries_the_go_class() {
    let mut session = session_with_table();
    for (sql, expected) in [
        (
            "INSERT INTO u1 VALUES (1,'b',2,1,1.5,'2020-01-01','{}','x')",
            "[kv:1062]Duplicate entry '1' for key 'u1.PRIMARY'",
        ),
        (
            "INSERT INTO u1 VALUES (2,'b',1,1,1.5,'2020-01-01','{}','x')",
            "[kv:1062]Duplicate entry '1' for key 'u1.c'",
        ),
        (
            "INSERT INTO u1 VALUES (3,NULL,3,1,1.5,'2020-01-01','{}','x')",
            "[table:1048]Column 'b' cannot be null",
        ),
        (
            "INSERT INTO u1 VALUES (3,'abcd',3,1,1.5,'2020-01-01','{}','x')",
            "[types:1406]Data too long for column 'b' at row 1",
        ),
        (
            "INSERT INTO u1 VALUES (3,'a',3,300,1.5,'2020-01-01','{}','x')",
            "[types:1264]Out of range value for column 'd' at row 1",
        ),
        (
            "INSERT INTO u1 VALUES (3,'a',3,1,1.5,'2020-13-01','{}','x')",
            "[table:1292]Incorrect date value: '2020-13-01' for column 'f' at row 1",
        ),
        (
            "INSERT INTO u1 VALUES (3,'a',3,1,1.5,'2020-01-01','{x','x')",
            "[json:3140]Invalid JSON text: The document root must not be followed by other \
             values.",
        ),
        (
            "INSERT INTO u1 VALUES (3,'a',3,1,1.5,'2020-01-01','{}','z')",
            "[types:1265]Data truncated for column 'h' at row 1",
        ),
        (
            "INSERT INTO u1 VALUES ('abc','a',3,1,1.5,'2020-01-01','{}','x')",
            "[table:1366]Incorrect int value: 'abc' for column 'a' at row 1",
        ),
        (
            "INSERT INTO u1 VALUES (4,'a',1/0,1,1.5,'2020-01-01','{}','x')",
            "[expression:1365]Division by 0",
        ),
        (
            "SELECT 9223372036854775807+1 FROM u1",
            "[types:1690]BIGINT value is out of range in '(9223372036854775807 + 1)'",
        ),
    ] {
        assert!(session.run(sql).is_err(), "{sql}");
        assert_eq!(last_error(&mut session).as_deref(), Some(expected), "{sql}");
    }
}

/// Go `session.NonTransactionalDML`: a later job's failure is
/// `ErrNonTransactionalJobFailure` whose last argument is `job.err.Error()`,
/// so the inner error's `[kv:1062]` prefix is part of the client message.
#[test]
fn non_transactional_job_failure_embeds_the_inner_error_class() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE nt1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("CREATE TABLE nt2 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("INSERT INTO nt1 VALUES (1,1),(2,2),(3,3),(4,4)")
        .unwrap();
    session.run("INSERT INTO nt2 VALUES (3,0)").unwrap();
    let error = session
        .run("BATCH ON id LIMIT 1 INSERT INTO nt2 SELECT * FROM nt1")
        .unwrap_err();
    let message = error.to_mysql_error().message;
    assert!(
        message.ends_with(", err: [kv:1062]Duplicate entry '3' for key 'nt2.PRIMARY'"),
        "{message}"
    );
}
