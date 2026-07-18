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

//! Protocol-neutral SQL execution at the shared-session boundary.
//!
//! The source test exercises a MySQL packet dispatcher, so this module keeps
//! only the SQL slice that can be proven without inventing packet framing,
//! authentication, COM_* command routing, or result encoding.

use super::*;

fn assert_done(session: &mut Session, sql: &str) {
    assert_eq!(session.execute_sql(sql), Ok(Outcome::Done), "SQL: {sql}");
}

#[test]
fn go_server_test_dispatch_sql_path_source_slice() {
    let cluster = Cluster::new();
    let mut session = cluster.session();

    // The COM_QUERY case in Go's TestDispatch reaches handleQuery after the
    // packet byte is removed.  This is the same SQL path without MySQL
    // transport/authentication or OK-packet encoding.
    assert_done(
        &mut session,
        "create table test_dispatch (id int primary key, value int)",
    );
    assert_done(&mut session, "insert into test_dispatch values (1, 7)");
    assert_eq!(
        session.execute_sql("select value from test_dispatch where id = 1"),
        Ok(Outcome::Rows(ResultSet {
            rows: vec![vec![tidb_datatype::Datum::Int(7)]],
            ordered: false,
        }))
    );

    // Go's TestDispatch also proves commands outside COM_QUERY (PING,
    // INIT_DB, prepared statements, and user change).  They remain outside
    // this SQL-only API and are intentionally not accepted here.
    assert_eq!(
        session.execute_sql("do 1"),
        Err(ExecError::Unsupported("shared-session capability"))
    );
}

#[test]
fn execute_sql_rejects_multi_statement_before_shared_mutation() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    assert_done(&mut session, "create table execute_sql_gate (id int)");
    let version_before = cluster.catalog_version();

    let error = session
        .execute_sql(
            "insert into execute_sql_gate values (1); insert into execute_sql_gate values (2)",
        )
        .expect_err("strict SQL entrypoint must reject trailing statements");
    assert!(matches!(error, ExecError::Parse { .. }));
    assert_eq!(cluster.catalog_version(), version_before);
    assert_eq!(
        session.execute_sql("select id from execute_sql_gate"),
        Ok(Outcome::Rows(ResultSet {
            rows: Vec::new(),
            ordered: false,
        }))
    );
}

#[test]
fn execute_sql_rejects_unsupported_command_after_parse_without_mutation() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    assert_done(
        &mut session,
        "create table execute_sql_unsupported (id int)",
    );
    let version_before = cluster.catalog_version();

    assert_eq!(
        session.execute_sql("begin"),
        Err(ExecError::Unsupported("shared-session capability"))
    );
    assert_eq!(cluster.catalog_version(), version_before);
    assert_done(
        &mut session,
        "insert into execute_sql_unsupported values (3)",
    );
    assert_eq!(
        session.execute_sql("select id from execute_sql_unsupported"),
        Ok(Outcome::Rows(ResultSet {
            rows: vec![vec![tidb_datatype::Datum::Int(3)]],
            ordered: false,
        }))
    );
}
