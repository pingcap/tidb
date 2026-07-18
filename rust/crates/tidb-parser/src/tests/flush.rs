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

//! Standalone `FLUSH` tests mapped from Go's parser and AST restore suites.

use super::*;

#[test]
fn flush_tables_restore_and_shape_match_go() {
    for (sql, restored) in [
        ("flush table", "FLUSH TABLES"),
        ("flush tables", "FLUSH TABLES"),
        ("flush tables tbl1", "FLUSH TABLES `tbl1`"),
        ("flush table with read lock", "FLUSH TABLES WITH READ LOCK"),
        (
            "flush tables tbl1, tbl2, tbl3",
            "FLUSH TABLES `tbl1`, `tbl2`, `tbl3`",
        ),
        (
            "flush tables db1.t1, t2 with read lock",
            "FLUSH TABLES `db1`.`t1`, `t2` WITH READ LOCK",
        ),
        ("flush tables read", "FLUSH TABLES `read`"),
    ] {
        assert_eq!(r(sql), restored, "{sql}");
    }

    let statement = parse("flush tables t1,t2 with read lock").expect("FLUSH TABLES parses");
    assert!(matches!(
        statement,
        tidb_ast::Stmt::Admin(admin)
            if matches!(admin.as_ref(), tidb_ast::AdminStmt::Flush(flush)
                if matches!(flush.as_ref(), tidb_ast::FlushStmt::Tables { tables, read_lock }
                    if tables.len() == 2 && *read_lock))
    ));
}

#[test]
fn flush_status_and_privileges_keep_distinct_payloads() {
    assert_eq!(r("flush status"), "FLUSH STATUS");
    assert_eq!(r("flush privileges"), "FLUSH PRIVILEGES");
    assert!(matches!(
        parse("flush privileges"),
        Ok(tidb_ast::Stmt::Admin(admin))
            if matches!(admin.as_ref(), tidb_ast::AdminStmt::Flush(flush)
                if matches!(flush.as_ref(), tidb_ast::FlushStmt::Privileges))
    ));
}

#[test]
fn unrepresented_go_flush_payloads_remain_explicit_gaps() {
    for sql in [
        "flush no_write_to_binlog tables t",
        "flush local tables t",
        "flush tidb plugins audit",
        "flush hosts",
        "flush logs",
        "flush binary logs",
        "flush engine logs",
        "flush error logs",
        "flush general logs",
        "flush slow logs",
        "flush client_errors_summary",
        "flush stats_delta *.*",
    ] {
        assert!(parse(sql).is_err(), "unrepresented payload accepted: {sql}");
    }
}
