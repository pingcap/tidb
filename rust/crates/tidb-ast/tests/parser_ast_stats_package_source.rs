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

//! Ports of `pkg/parser/ast/stats_test.go` (origin/master).
//!
//! Go parses SQL then restores with `format.DefaultRestoreFlags`. This crate
//! owns the AST/restore/dedup contract, so the cases are constructed as AST
//! nodes matching those parse results and restored through `Stmt::restore`.

use tidb_ast::{
    AdminStmt, FlushStmt, FlushTarget, NodeBox, RefreshStatsMode, RefreshStatsStmt, StatsObject,
    Stmt,
};

fn refresh(objects: Vec<StatsObject>, mode: Option<RefreshStatsMode>, cluster_wide: bool) -> Stmt {
    Stmt::Admin(NodeBox::new(AdminStmt::RefreshStats(Box::new(
        RefreshStatsStmt {
            objects,
            mode,
            cluster_wide,
        },
    ))))
}

fn flush_stats(objects: Vec<StatsObject>, cluster: bool) -> Stmt {
    Stmt::Admin(NodeBox::new(AdminStmt::Flush(Box::new(FlushStmt {
        no_write_to_binlog: false,
        target: FlushTarget::StatsDelta { objects, cluster },
    }))))
}

fn table(name: &str) -> StatsObject {
    StatsObject::Table {
        database: None,
        table: name.to_string(),
    }
}

fn qualified(database: &str, name: &str) -> StatsObject {
    StatsObject::Table {
        database: Some(database.to_string()),
        table: name.to_string(),
    }
}

fn database(name: &str) -> StatsObject {
    StatsObject::Database(name.to_string())
}

/// `pkg/parser/ast/stats_test.go::TestRefreshStatsStmt`.
#[test]
fn refresh_stats_stmt() {
    let cases: Vec<(Stmt, &str, Option<RefreshStatsMode>)> = vec![
        (
            refresh(vec![StatsObject::Global], None, false),
            "REFRESH STATS *.*",
            None,
        ),
        (
            refresh(vec![database("db1")], None, false),
            "REFRESH STATS `db1`.*",
            None,
        ),
        (
            refresh(vec![qualified("db1", "t1")], None, false),
            "REFRESH STATS `db1`.`t1`",
            None,
        ),
        (
            refresh(vec![table("table1")], None, false),
            "REFRESH STATS `table1`",
            None,
        ),
        (
            refresh(vec![table("table1"), table("table2")], None, false),
            "REFRESH STATS `table1`, `table2`",
            None,
        ),
        (
            refresh(
                vec![
                    StatsObject::Global,
                    database("db1"),
                    qualified("db2", "t1"),
                    table("table1"),
                    table("table2"),
                ],
                None,
                false,
            ),
            "REFRESH STATS *.*, `db1`.*, `db2`.`t1`, `table1`, `table2`",
            None,
        ),
        (
            refresh(vec![table("table1")], Some(RefreshStatsMode::Full), false),
            "REFRESH STATS `table1` FULL",
            Some(RefreshStatsMode::Full),
        ),
        (
            refresh(vec![table("table1")], None, true),
            "REFRESH STATS `table1` CLUSTER",
            None,
        ),
        (
            refresh(vec![database("db1")], Some(RefreshStatsMode::Lite), true),
            "REFRESH STATS `db1`.* LITE CLUSTER",
            Some(RefreshStatsMode::Lite),
        ),
    ];

    for (stmt, want, mode) in cases {
        let Stmt::Admin(admin) = &stmt else {
            panic!("expected admin statement");
        };
        let AdminStmt::RefreshStats(refresh) = admin.as_ref() else {
            panic!("expected refresh stats");
        };
        assert_eq!(refresh.mode, mode, "{want}");
        assert_eq!(stmt.restore(), want);
    }
}

/// `pkg/parser/ast/stats_test.go::TestFlushStatsDeltaScoped` parse/restore
/// cases (object count, CLUSTER flag, restore text).
#[test]
fn flush_stats_delta_scoped() {
    let cases: Vec<(Stmt, &str, usize, bool)> = vec![
        (
            flush_stats(vec![StatsObject::Global], false),
            "FLUSH STATS_DELTA *.*",
            1,
            false,
        ),
        (
            flush_stats(vec![StatsObject::Global], true),
            "FLUSH STATS_DELTA *.* CLUSTER",
            1,
            true,
        ),
        (
            flush_stats(vec![database("db1")], false),
            "FLUSH STATS_DELTA `db1`.*",
            1,
            false,
        ),
        (
            flush_stats(vec![qualified("db1", "t1")], false),
            "FLUSH STATS_DELTA `db1`.`t1`",
            1,
            false,
        ),
        (
            flush_stats(vec![qualified("db1", "t1")], true),
            "FLUSH STATS_DELTA `db1`.`t1` CLUSTER",
            1,
            true,
        ),
        (
            flush_stats(vec![table("table1")], false),
            "FLUSH STATS_DELTA `table1`",
            1,
            false,
        ),
        (
            flush_stats(
                vec![qualified("db1", "t1"), database("db2"), StatsObject::Global],
                false,
            ),
            "FLUSH STATS_DELTA `db1`.`t1`, `db2`.*, *.*",
            3,
            false,
        ),
        (
            flush_stats(vec![qualified("db1", "t1"), database("db2")], true),
            "FLUSH STATS_DELTA `db1`.`t1`, `db2`.* CLUSTER",
            2,
            true,
        ),
    ];

    for (stmt, want, objects, cluster) in cases {
        let Stmt::Admin(admin) = &stmt else {
            panic!("expected admin statement");
        };
        let AdminStmt::Flush(flush) = admin.as_ref() else {
            panic!("expected flush");
        };
        let FlushTarget::StatsDelta {
            objects: flush_objects,
            cluster: flush_cluster,
        } = &flush.target
        else {
            panic!("expected STATS_DELTA");
        };
        assert_eq!(flush_objects.len(), objects, "{want}");
        assert_eq!(*flush_cluster, cluster, "{want}");
        assert_eq!(stmt.restore(), want);
    }
}

/// `pkg/parser/ast/stats_test.go::TestFlushStatsDeltaScoped` DedupFlushObjects
/// cases.
#[test]
fn flush_stats_delta_scoped_dedup() {
    let cases: Vec<(&str, Vec<StatsObject>, &str)> = vec![
        (
            "global overrides all",
            vec![
                table("table1"),
                qualified("db1", "t1"),
                StatsObject::Global,
                qualified("db2", "t2"),
            ],
            "FLUSH STATS_DELTA *.*",
        ),
        (
            "database removes prior tables",
            vec![
                qualified("db1", "t1"),
                qualified("db2", "t1"),
                database("db1"),
                qualified("db2", "t2"),
            ],
            "FLUSH STATS_DELTA `db2`.`t1`, `db1`.*, `db2`.`t2`",
        ),
        (
            "table duplicates case insensitive",
            vec![
                qualified("db1", "t1"),
                qualified("db1", "T1"),
                qualified("db2", "t1"),
            ],
            "FLUSH STATS_DELTA `db1`.`t1`, `db2`.`t1`",
        ),
        (
            "quoted dotted names are distinct",
            vec![qualified("a.b", "c"), qualified("a", "b.c")],
            "FLUSH STATS_DELTA `a.b`.`c`, `a`.`b.c`",
        ),
    ];

    for (name, objects, want) in cases {
        let mut stmt = flush_stats(objects, false);
        let Stmt::Admin(admin) = &mut stmt else {
            panic!("expected admin statement");
        };
        let AdminStmt::Flush(flush) = admin.as_mut() else {
            panic!("expected flush");
        };
        flush.dedup_stats_objects();
        assert_eq!(stmt.restore(), want, "{name}");
    }
}

/// `pkg/parser/ast/stats_test.go::TestRefreshStatsStmtDedup`.
#[test]
fn refresh_stats_stmt_dedup() {
    let cases: Vec<(&str, Vec<StatsObject>, &str)> = vec![
        (
            "global overrides all",
            vec![
                table("table1"),
                qualified("db1", "t1"),
                StatsObject::Global,
                qualified("db2", "t2"),
            ],
            "REFRESH STATS *.*",
        ),
        (
            "database removes prior tables",
            vec![
                qualified("db1", "t1"),
                qualified("db2", "t1"),
                database("db1"),
                qualified("db2", "t2"),
            ],
            "REFRESH STATS `db2`.`t1`, `db1`.*, `db2`.`t2`",
        ),
        (
            "table duplicates case insensitive",
            vec![
                qualified("db1", "t1"),
                qualified("db1", "T1"),
                qualified("db2", "t1"),
            ],
            "REFRESH STATS `db1`.`t1`, `db2`.`t1`",
        ),
        (
            "table duplicates without database",
            vec![table("table1"), table("table1"), table("table2")],
            "REFRESH STATS `table1`, `table2`",
        ),
        (
            "database duplicates case insensitive",
            vec![database("db1"), database("DB1"), qualified("db2", "t1")],
            "REFRESH STATS `db1`.*, `db2`.`t1`",
        ),
        (
            "quoted dotted names are distinct",
            vec![qualified("a.b", "c"), qualified("a", "b.c")],
            "REFRESH STATS `a.b`.`c`, `a`.`b.c`",
        ),
    ];

    for (name, objects, want) in cases {
        let mut stmt = refresh(objects, None, false);
        let Stmt::Admin(admin) = &mut stmt else {
            panic!("expected admin statement");
        };
        let AdminStmt::RefreshStats(refresh) = admin.as_mut() else {
            panic!("expected refresh stats");
        };
        refresh.dedup();
        assert_eq!(stmt.restore(), want, "{name}");
    }
}
