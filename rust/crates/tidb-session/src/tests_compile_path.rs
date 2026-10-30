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

//! The checks Go's `session.ExecuteStmt` makes before `Compile`.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use crate::Session;

/// Go `validateStatementReadOnlyInStaleness` (`session.go:2822`) switches on
/// the statement node itself: a top-level SELECT's lock, the read-only
/// question for EXPLAIN, a set operation and the like, and any DML. A lock
/// inside a derived table or a view body, and every DDL, pass it.
#[test]
fn the_staleness_check_reads_only_the_statement_node() {
    let mut session = Session::new();
    session
        .run("SET @@tidb_enable_external_ts_read = ON")
        .unwrap();
    let check = |sql: &str| {
        let parsed = session.parse_statement(sql).unwrap();
        session
            .validate_statement_read_only_in_staleness(&parsed)
            .map_err(|error| error.to_mysql_error().message)
    };
    assert_eq!(
        check("SELECT * FROM t FOR UPDATE"),
        Err("select lock hasn't been supported in stale read yet".to_owned())
    );
    assert_eq!(
        check("INSERT INTO t VALUES (1)"),
        Err("only support read-only statement during read-only staleness transactions".to_owned())
    );
    for sql in [
        "SELECT * FROM (SELECT * FROM t FOR UPDATE) x",
        "EXPLAIN SELECT * FROM t FOR UPDATE",
        "CREATE VIEW v AS SELECT * FROM t FOR UPDATE",
        "CREATE TABLE t2 (a INT)",
    ] {
        assert_eq!(check(sql), Ok(()), "{sql}");
    }
}
