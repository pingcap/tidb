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
                if matches!(&flush.target, tidb_ast::FlushTarget::Tables { tables, read_lock }
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
                if matches!(flush.target, tidb_ast::FlushTarget::Privileges))
    ));
}

#[test]
fn complete_go_flush_payloads_restore_source_rows() {
    for (sql, expected) in [
        (
            "flush no_write_to_binlog tables tbl1 with read lock",
            "FLUSH NO_WRITE_TO_BINLOG TABLES `tbl1` WITH READ LOCK",
        ),
        (
            "flush no_write_to_binlog tables tbl1",
            "FLUSH NO_WRITE_TO_BINLOG TABLES `tbl1`",
        ),
        (
            "flush local tables tbl1",
            "FLUSH NO_WRITE_TO_BINLOG TABLES `tbl1`",
        ),
        ("flush tidb plugins plugin1", "FLUSH TIDB PLUGINS plugin1"),
        (
            "flush tidb plugins plugin1, plugin2",
            "FLUSH TIDB PLUGINS plugin1, plugin2",
        ),
        ("flush hosts", "FLUSH HOSTS"),
        ("flush logs", "FLUSH LOGS"),
        ("flush binary logs", "FLUSH BINARY LOGS"),
        ("flush engine logs", "FLUSH ENGINE LOGS"),
        ("flush error logs", "FLUSH ERROR LOGS"),
        ("flush general logs", "FLUSH GENERAL LOGS"),
        ("flush slow logs", "FLUSH SLOW LOGS"),
        ("flush client_errors_summary", "FLUSH CLIENT_ERRORS_SUMMARY"),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn test_flush_stats_delta_scoped() {
    for (sql, restored, object_count, cluster) in [
        ("FLUSH STATS_DELTA *.*", "FLUSH STATS_DELTA *.*", 1, false),
        (
            "FLUSH STATS_DELTA *.* CLUSTER",
            "FLUSH STATS_DELTA *.* CLUSTER",
            1,
            true,
        ),
        (
            "FLUSH STATS_DELTA db1.*",
            "FLUSH STATS_DELTA `db1`.*",
            1,
            false,
        ),
        (
            "FLUSH STATS_DELTA db1.t1",
            "FLUSH STATS_DELTA `db1`.`t1`",
            1,
            false,
        ),
        (
            "FLUSH STATS_DELTA db1.t1 CLUSTER",
            "FLUSH STATS_DELTA `db1`.`t1` CLUSTER",
            1,
            true,
        ),
        (
            "FLUSH STATS_DELTA table1",
            "FLUSH STATS_DELTA `table1`",
            1,
            false,
        ),
        (
            "FLUSH STATS_DELTA db1.t1, db2.*, *.*",
            "FLUSH STATS_DELTA `db1`.`t1`, `db2`.*, *.*",
            3,
            false,
        ),
        (
            "FLUSH STATS_DELTA db1.t1, db2.* CLUSTER",
            "FLUSH STATS_DELTA `db1`.`t1`, `db2`.* CLUSTER",
            2,
            true,
        ),
    ] {
        let statement = parse(sql).expect("FLUSH STATS_DELTA parses");
        assert_eq!(statement.restore(), restored, "{sql}");
        assert!(matches!(
            statement,
            tidb_ast::Stmt::Admin(admin)
                if matches!(admin.as_ref(), tidb_ast::AdminStmt::Flush(flush)
                    if matches!(&flush.target, tidb_ast::FlushTarget::StatsDelta { objects, cluster: actual_cluster }
                        if objects.len() == object_count && *actual_cluster == cluster))
        ));
    }

    for (sql, restored) in [
        (
            "FLUSH STATS_DELTA table1, db1.t1, *.*, db2.t2",
            "FLUSH STATS_DELTA *.*",
        ),
        (
            "FLUSH STATS_DELTA db1.t1, db2.t1, db1.*, db2.t2",
            "FLUSH STATS_DELTA `db2`.`t1`, `db1`.*, `db2`.`t2`",
        ),
        (
            "FLUSH STATS_DELTA db1.t1, db1.T1, db2.t1",
            "FLUSH STATS_DELTA `db1`.`t1`, `db2`.`t1`",
        ),
        (
            "FLUSH STATS_DELTA `a.b`.`c`, `a`.`b.c`",
            "FLUSH STATS_DELTA `a.b`.`c`, `a`.`b.c`",
        ),
    ] {
        let tidb_ast::Stmt::Admin(admin) = parse(sql).expect("FLUSH STATS_DELTA parses") else {
            panic!("expected admin statement");
        };
        let tidb_ast::AdminStmt::Flush(mut flush) = admin.into_inner() else {
            panic!("expected flush statement");
        };
        flush.dedup_stats_objects();
        assert_eq!(
            tidb_ast::Stmt::Admin(tidb_ast::NodeBox::new(tidb_ast::AdminStmt::Flush(flush)))
                .restore(),
            restored,
            "{sql}"
        );
    }
}
