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

use crate::tests_support::{authenticated_session, row_text, session_as};
use crate::Session;

fn one(session: &mut Session, sql: &str) -> String {
    row_text(session.run(sql))[0][0].clone()
}

fn error_text(session: &mut Session, sql: &str) -> String {
    session.run(sql).err().unwrap_or_else(|| panic!("{sql} was accepted")).to_mysql_error().message
}

fn id_of(session: &mut Session, sql: &str) -> i64 {
    one(session, sql).parse().unwrap()
}

/// Go `TestTiDBEncodeKey` (`pkg/expression/integration_test`), but for its
/// one `TIDB_MVCC_INFO` read of a record, which needs the mock store to
/// answer `GetMvccByEncodedKey`.
#[test]
fn test_tidb_encode_key() {
    let registry = tidb_privilege::PrivilegeRegistry::default();
    let mut tk = authenticated_session(&registry, "root", "%");
    tk.run("use test").unwrap();
    tk.run("create table t (a int primary key, b int);").unwrap();
    tk.run("insert into t values (1, 1);").unwrap();
    let tbl_id = id_of(&mut tk, "select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 't'");
    let err = error_text(&mut tk, "select tidb_encode_record_key('test', 't1', 0);");
    assert!(err.contains("doesn't exist"), "{err}");
    assert_eq!(
        one(&mut tk, "select tidb_encode_record_key('test', 't', 1);"),
        format!("74800000000000{tbl_id:04x}5f728000000000000001")
    );

    tk.run("alter table t add index i(b);").unwrap();
    let err = error_text(&mut tk, "select tidb_encode_index_key('test', 't', 'i1', 1);");
    assert!(err.contains("index not found"), "{err}");
    assert_eq!(
        one(&mut tk, "select tidb_encode_index_key('test', 't', 'i', 1, 1);"),
        format!("74800000000000{tbl_id:04x}5f698000000000000001038000000000000001038000000000000001")
    );

    tk.run("create table t1 (a int primary key, b int) partition by hash(a) partitions 4;").unwrap();
    tk.run("insert into t1 values (1, 1);").unwrap();
    let p1 = id_of(
        &mut tk,
        "select tidb_partition_id from information_schema.partitions where table_schema = 'test' and table_name = 't1' and partition_name = 'p1'",
    );
    assert_eq!(
        one(&mut tk, "select tidb_encode_record_key('test', 't1(p1)', 1);"),
        format!("74800000000000{p1:04x}5f728000000000000001")
    );

    tk.run("create user 'alice'@'%';").unwrap();
    tk.run("flush privileges;").unwrap();
    let mut tk2 = session_as(&registry, tk.shared_catalog(), "alice", "%");
    let err = error_text(&mut tk2, &format!("select tidb_mvcc_info('74800000000000{p1:04x}5f728000000000000001');"));
    assert!(err.contains("Access denied"), "{err}");
    let err = error_text(&mut tk2, "select tidb_encode_record_key('test', 't1(p1)', 1);");
    assert!(err.contains("SELECT command denied"), "{err}");
    let err = error_text(&mut tk2, "select tidb_encode_index_key('test', 't', 'i1', 1);");
    assert!(err.contains("SELECT command denied"), "{err}");
    tk.run("grant select on test.t1 to 'alice'@'%';").unwrap();
    assert_eq!(
        one(&mut tk2, "select tidb_encode_record_key('test', 't1(p1)', 1);"),
        format!("74800000000000{p1:04x}5f728000000000000001")
    );
}
