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

//! Go master `eca4674a6f`'s materialized view parser tests (with
//! `6aec16d0234`'s IF NOT EXISTS rows)
//! (`parser_test.go`: `TestMaterializedViewDDLStatements`,
//! `TestRefreshMaterializedViewStatements`,
//! `TestMaterializedViewDuplicateOptionsErrMsg`,
//! `TestMaterializedViewCreateOptionOrder`,
//! `TestMaterializedViewLogCreatePurgeClauseSyntax`, and the SHOW, CANCEL
//! and COMPARE rows of `TestDMLStmt` / `TestAsOfClause`).

use crate::{parse, tests::assert_full_visitor_traversal};
use tidb_ast::{
    AdminStmt, CancelMaterializedViewJobType, DdlStmt, RefreshMaterializedViewImplementStmt,
    RefreshMaterializedViewMode, RefreshMaterializedViewObserveType, RefreshMaterializedViewStmt,
    Stmt,
};

/// Go `RunTest`: a statement that parses restores to `restore`, and the
/// restored text parses back to the same restore; one that must not parse
/// is refused.
fn run_test(cases: &[(&str, bool, &str)]) {
    for (sql, ok, restore) in cases {
        let parsed = parse(sql);
        if !ok {
            assert!(parsed.is_err(), "source SQL unexpectedly parsed: {sql}");
            continue;
        }
        let restored = parsed.unwrap_or_else(|error| panic!("{sql}: {error:?}")).restore();
        assert_eq!(&restored, restore, "source SQL: {sql}");
        let again = parse(&restored).unwrap_or_else(|error| panic!("{restored}: {error:?}")).restore();
        assert_eq!(&again, restore, "restored SQL: {restored}");
        assert_full_visitor_traversal(sql);
    }
}

fn kind(sql: &str) -> &'static str {
    match parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}")) {
        Stmt::Ddl(ddl) => match ddl.into_inner() {
            DdlStmt::CreateMaterializedView(_) => "CreateMaterializedViewStmt",
            DdlStmt::CreateMaterializedViewLog(_) => "CreateMaterializedViewLogStmt",
            DdlStmt::AlterMaterializedView(_) => "AlterMaterializedViewStmt",
            DdlStmt::AlterMaterializedViewLog(_) => "AlterMaterializedViewLogStmt",
            DdlStmt::DropMaterializedView(_) => "DropMaterializedViewStmt",
            DdlStmt::DropMaterializedViewLog(_) => "DropMaterializedViewLogStmt",
            other => panic!("{sql}: {other:?}"),
        },
        Stmt::Admin(admin) => match admin.into_inner() {
            AdminStmt::PurgeMaterializedViewLog(_) => "PurgeMaterializedViewLogStmt",
            other => panic!("{sql}: {other:?}"),
        },
        other => panic!("{sql}: {other:?}"),
    }
}

#[test]
fn materialized_view_ddl_statements() {
    let table = [
        ("CREATE MATERIALIZED VIEW mv (a) AS SELECT 1", true, "CREATE MATERIALIZED VIEW `mv` (`a`) AS SELECT 1"),
        (
            "CREATE MATERIALIZED VIEW mv (a) COMMENT = 'c1' SHARD_ROW_ID_BITS = 2 PRE_SPLIT_REGIONS = 3 REFRESH FAST NEXT 300 ATTRIBUTES = 'x' AS SELECT 1",
            true,
            "CREATE MATERIALIZED VIEW `mv` (`a`) COMMENT = 'c1' SHARD_ROW_ID_BITS = 2 PRE_SPLIT_REGIONS = 3 REFRESH FAST NEXT 300 ATTRIBUTES = 'x' AS SELECT 1",
        ),
        ("CREATE MATERIALIZED VIEW mv (a) COMMENT 'c1' AS SELECT 1", true, "CREATE MATERIALIZED VIEW `mv` (`a`) COMMENT = 'c1' AS SELECT 1"),
        (
            "CREATE MATERIALIZED VIEW LOG ON t (a,b) PURGE IMMEDIATE ALERT ROWS 10",
            true,
            "CREATE MATERIALIZED VIEW LOG ON `t` (`a`, `b`) PURGE IMMEDIATE ALERT ROWS 10",
        ),
        ("CREATE MATERIALIZED VIEW LOG ON t (a) PURGE NEXT 300", true, "CREATE MATERIALIZED VIEW LOG ON `t` (`a`) PURGE NEXT 300"),
        (
            "ALTER MATERIALIZED VIEW mv COMMENT = 'c2', REFRESH START WITH now() NEXT 300, ATTRIBUTES = 'y'",
            true,
            "ALTER MATERIALIZED VIEW `mv` COMMENT = 'c2', REFRESH START WITH NOW() NEXT 300, ATTRIBUTES = 'y'",
        ),
        ("ALTER MATERIALIZED VIEW mv COMMENT 'c2'", true, "ALTER MATERIALIZED VIEW `mv` COMMENT = 'c2'"),
        ("ALTER MATERIALIZED VIEW mv REFRESH", true, "ALTER MATERIALIZED VIEW `mv` REFRESH"),
        (
            "ALTER MATERIALIZED VIEW LOG ON t PURGE, ADD COLUMN (b,c)",
            true,
            "ALTER MATERIALIZED VIEW LOG ON `t` PURGE, ADD COLUMN (`b`, `c`)",
        ),
        ("ALTER MATERIALIZED VIEW LOG ON t PURGE", true, "ALTER MATERIALIZED VIEW LOG ON `t` PURGE"),
        ("PURGE MATERIALIZED VIEW LOG ON t", true, "PURGE MATERIALIZED VIEW LOG ON `t`"),
        ("PURGE MATERIALIZED VIEW LOG ON test.t", true, "PURGE MATERIALIZED VIEW LOG ON `test`.`t`"),
        ("DROP MATERIALIZED VIEW IF EXISTS mv", true, "DROP MATERIALIZED VIEW IF EXISTS `mv`"),
        ("DROP MATERIALIZED VIEW LOG IF EXISTS ON t", true, "DROP MATERIALIZED VIEW LOG IF EXISTS ON `t`"),
        // Go master `6aec16d0234`: CREATE ... IF NOT EXISTS.
        ("CREATE MATERIALIZED VIEW IF NOT EXISTS test.mv (a) AS SELECT 1", true, "CREATE MATERIALIZED VIEW IF NOT EXISTS `test`.`mv` (`a`) AS SELECT 1"),
        (
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv (a) COMMENT = 'c1' SHARD_ROW_ID_BITS = 4 REFRESH FAST ATTRIBUTES = 'x' AS SELECT 1",
            true,
            "CREATE MATERIALIZED VIEW IF NOT EXISTS `mv` (`a`) COMMENT = 'c1' SHARD_ROW_ID_BITS = 4 REFRESH FAST ATTRIBUTES = 'x' AS SELECT 1",
        ),
        ("CREATE MATERIALIZED VIEW LOG IF NOT EXISTS ON test.t (a,b)", true, "CREATE MATERIALIZED VIEW LOG IF NOT EXISTS ON `test`.`t` (`a`, `b`)"),
        (
            "CREATE MATERIALIZED VIEW LOG IF NOT EXISTS ON t (a) SHARD_ROW_ID_BITS = 4 PURGE START WITH now() NEXT 300 ALERT ROWS 100",
            true,
            "CREATE MATERIALIZED VIEW LOG IF NOT EXISTS ON `t` (`a`) SHARD_ROW_ID_BITS = 4 PURGE START WITH NOW() NEXT 300 ALERT ROWS 100",
        ),
        ("CREATE MATERIALIZED VIEW IF NOT EXISTS log (a) AS SELECT 1", true, "CREATE MATERIALIZED VIEW IF NOT EXISTS `log` (`a`) AS SELECT 1"),
        ("CREATE MATERIALIZED VIEW mv IF NOT EXISTS (a) AS SELECT 1", false, ""),
        ("CREATE MATERIALIZED VIEW LOG ON IF NOT EXISTS t (a)", false, ""),
    ];
    run_test(&table);
    let want = [
        "CreateMaterializedViewStmt",
        "CreateMaterializedViewStmt",
        "CreateMaterializedViewStmt",
        "CreateMaterializedViewLogStmt",
        "CreateMaterializedViewLogStmt",
        "AlterMaterializedViewStmt",
        "AlterMaterializedViewStmt",
        "AlterMaterializedViewStmt",
        "AlterMaterializedViewLogStmt",
        "AlterMaterializedViewLogStmt",
        "PurgeMaterializedViewLogStmt",
        "PurgeMaterializedViewLogStmt",
        "DropMaterializedViewStmt",
        "DropMaterializedViewLogStmt",
    ];
    for ((sql, _, _), want) in table.iter().zip(want) {
        assert_eq!(kind(sql), want, "{sql}");
    }
}

#[test]
fn refresh_materialized_view_statements() {
    run_test(&[
        ("REFRESH MATERIALIZED VIEW mv FAST", true, "REFRESH MATERIALIZED VIEW `mv` FAST"),
        ("REFRESH MATERIALIZED VIEW mv WITH ASYNC MODE FAST", true, "REFRESH MATERIALIZED VIEW `mv` WITH ASYNC MODE FAST"),
        (
            "REFRESH MATERIALIZED VIEW mv FAST AS OF TIMESTAMP '2021-04-15 00:00:00' WITH PROFILE",
            true,
            "REFRESH MATERIALIZED VIEW `mv` FAST AS OF TIMESTAMP _UTF8MB4'2021-04-15 00:00:00' WITH PROFILE",
        ),
        ("REFRESH MATERIALIZED VIEW mv COMPLETE", false, ""),
        ("REFRESH MATERIALIZED VIEW mv COMPLETE IN PLACE", true, "REFRESH MATERIALIZED VIEW `mv` COMPLETE IN PLACE"),
        (
            "REFRESH MATERIALIZED VIEW mv WITH ASYNC MODE COMPLETE OUT OF PLACE DRY RUN",
            true,
            "REFRESH MATERIALIZED VIEW `mv` WITH ASYNC MODE COMPLETE OUT OF PLACE DRY RUN",
        ),
        (
            "REFRESH MATERIALIZED VIEW mv COMPLETE DELTA APPLY WITH PROFILE",
            true,
            "REFRESH MATERIALIZED VIEW `mv` COMPLETE DELTA APPLY WITH PROFILE",
        ),
        ("REFRESH MATERIALIZED VIEW mv FAST OUT OF PLACE", false, ""),
        ("REFRESH MATERIALIZED VIEW mv COMPLETE OUT OF PLACE DELTA APPLY", false, ""),
        ("CANCEL MATERIALIZED VIEW REFRESH JOB", false, ""),
        ("CANCEL MATERIALIZED VIEW REFRESH JOB 42", true, "CANCEL MATERIALIZED VIEW REFRESH JOB 42"),
        // `TestDMLStmt`.
        ("cancel materialized view log purge job", false, ""),
        ("cancel materialized view log purge job 1", true, "CANCEL MATERIALIZED VIEW LOG PURGE JOB 1"),
    ]);

    let Stmt::Admin(admin) = parse("REFRESH MATERIALIZED VIEW mv COMPLETE DELTA APPLY").unwrap() else {
        panic!("admin statement");
    };
    let AdminStmt::RefreshMaterializedView(refresh) = admin.into_inner() else {
        panic!("refresh statement");
    };
    assert_eq!(refresh.mode, RefreshMaterializedViewMode::CompleteDeltaApply);

    let Stmt::Admin(admin) = parse("CANCEL MATERIALIZED VIEW REFRESH JOB 42").unwrap() else {
        panic!("admin statement");
    };
    let AdminStmt::CancelMaterializedViewJob(cancel) = admin.into_inner() else {
        panic!("cancel statement");
    };
    assert_eq!((cancel.tp, cancel.job_id), (CancelMaterializedViewJobType::Refresh, 42));

    // Go's restore cases. A FAST refresh carries no complete type here, and a
    // COMPLETE one without its mode cannot be built.
    let view_name = vec!["test".to_owned(), "mv".to_owned()];
    let implement = AdminStmt::RefreshMaterializedViewImplement(Box::new(RefreshMaterializedViewImplementStmt {
        refresh_stmt: RefreshMaterializedViewStmt {
            view_name: view_name.clone(),
            with_async_mode: false,
            mode: RefreshMaterializedViewMode::Fast,
            observe_type: RefreshMaterializedViewObserveType::None,
            as_of: None,
        },
        last_successful_refresh_read_tso: 4242,
        target_refresh_read_tso: 5252,
        mlog_retained_lower_tso: 3131,
        registered_mlog_ids: vec![1234],
    }));
    assert_eq!(
        Stmt::Admin(tidb_ast::NodeBox::new(implement)).restore(),
        "IMPLEMENT FOR REFRESH MATERIALIZED VIEW `test`.`mv` FAST USING TIMESTAMP 4242 UP TO TIMESTAMP 5252 MLOG RETAINED LOWER TIMESTAMP 3131"
    );
}

#[test]
fn materialized_view_duplicate_options_err_msg() {
    for (sql, substring) in [
        (
            "CREATE MATERIALIZED VIEW mv (a) COMMENT = 'c1' COMMENT = 'c2' AS SELECT 1",
            "Duplicate COMMENT specified in CREATE MATERIALIZED VIEW",
        ),
        (
            "CREATE MATERIALIZED VIEW mv (a) SHARD_ROW_ID_BITS = 1 SHARD_ROW_ID_BITS = 2 AS SELECT 1",
            "Duplicate SHARD_ROW_ID_BITS specified in CREATE MATERIALIZED VIEW",
        ),
        (
            "CREATE MATERIALIZED VIEW mv (a) PRE_SPLIT_REGIONS = 1 PRE_SPLIT_REGIONS = 2 AS SELECT 1",
            "Duplicate PRE_SPLIT_REGIONS specified in CREATE MATERIALIZED VIEW",
        ),
        (
            "CREATE MATERIALIZED VIEW LOG ON t (a) SHARD_ROW_ID_BITS = 1 SHARD_ROW_ID_BITS = 2",
            "Duplicate SHARD_ROW_ID_BITS specified in CREATE MATERIALIZED VIEW LOG",
        ),
        (
            "CREATE MATERIALIZED VIEW LOG ON t (a) PRE_SPLIT_REGIONS = 1 PRE_SPLIT_REGIONS = 2",
            "Duplicate PRE_SPLIT_REGIONS specified in CREATE MATERIALIZED VIEW LOG",
        ),
    ] {
        let error = parse(sql).expect_err(sql);
        assert!(format!("{error:?}").contains(substring), "{sql}: {error:?}");
    }
}

#[test]
fn materialized_view_create_option_order_and_purge_syntax() {
    for sql in [
        "CREATE MATERIALIZED VIEW mv (a) REFRESH FAST START WITH now() AS SELECT 1",
        "CREATE MATERIALIZED VIEW mv (a) REFRESH FAST SHARD_ROW_ID_BITS = 4 AS SELECT 1",
        "CREATE MATERIALIZED VIEW mv (a) ATTRIBUTES = 'x' REFRESH FAST AS SELECT 1",
        "CREATE MATERIALIZED VIEW mv (a) REFRESH FAST REFRESH FAST AS SELECT 1",
        "CREATE MATERIALIZED VIEW mv (a) ATTRIBUTES = 'x' ATTRIBUTES = 'y' AS SELECT 1",
        "CREATE MATERIALIZED VIEW LOG ON t (a) PURGE START WITH now()",
        "CREATE MATERIALIZED VIEW LOG ON t (a) PURGE",
    ] {
        assert!(parse(sql).is_err(), "source SQL unexpectedly parsed: {sql}");
    }
}

/// Go master's `show materialized views from 'test'` and `show
/// materialized view logs from 'test'` rows expect a syntax error: its yacc
/// `DBName` takes an identifier. This branch's hand-written parser shares
/// `parseShowDatabaseNameOpt` across every SHOW and accepts a string there
/// (`SHOW TABLES FROM 'test'` parses on this branch), and so do these.
#[test]
fn show_database_name_opt_is_the_branchs_shared_one() {
    for sql in ["show materialized views from 'test'", "show materialized view logs from 'test'"] {
        assert!(parse(sql).is_ok(), "{sql}");
    }
}

#[test]
fn show_and_compare_materialized_view_statements() {
    run_test(&[
        ("show create materialized view test.t", true, "SHOW CREATE MATERIALIZED VIEW `test`.`t`"),
        ("show create materialized view t", true, "SHOW CREATE MATERIALIZED VIEW `t`"),
        ("show create materialized view log on test.t", true, "SHOW CREATE MATERIALIZED VIEW LOG ON `test`.`t`"),
        ("show create materialized view log on t", true, "SHOW CREATE MATERIALIZED VIEW LOG ON `t`"),
        ("show materialized views", true, "SHOW MATERIALIZED VIEWS"),
        ("show materialized views from test", true, "SHOW MATERIALIZED VIEWS IN `test`"),
        ("show materialized views in test like 'mv%'", true, "SHOW MATERIALIZED VIEWS IN `test` LIKE _UTF8MB4'mv%'"),
        ("show materialized views where mview_id = 1", true, "SHOW MATERIALIZED VIEWS WHERE `mview_id`=1"),
        ("show materialized view mv remain_logs", true, "SHOW MATERIALIZED VIEW `mv` REMAIN_LOGS"),
        ("show materialized view test.mv remain_logs", true, "SHOW MATERIALIZED VIEW `test`.`mv` REMAIN_LOGS"),
        ("show materialized view mv", false, ""),
        ("show materialized view mv like 'mv%'", false, ""),
        ("show materialized view mv where mview_id = 1", false, ""),
        ("show materialized view logs", true, "SHOW MATERIALIZED VIEW LOGS"),
        ("show materialized view logs from test", true, "SHOW MATERIALIZED VIEW LOGS IN `test`"),
        (
            "show materialized view logs in test like '$mlog$%'",
            true,
            "SHOW MATERIALIZED VIEW LOGS IN `test` LIKE _UTF8MB4'$mlog$%'",
        ),
        ("show materialized view logs where mlog_id = 1", true, "SHOW MATERIALIZED VIEW LOGS WHERE `mlog_id`=1"),
        ("show materialized view log on t wait_purge", true, "SHOW MATERIALIZED VIEW LOG ON `t` WAIT_PURGE"),
        ("show materialized view log on test.t wait_purge", true, "SHOW MATERIALIZED VIEW LOG ON `test`.`t` WAIT_PURGE"),
        ("show materialized view log on t", false, ""),
        ("show materialized view log on t like '$mlog$%'", false, ""),
        ("show materialized view log on t where mlog_id = 1", false, ""),
        (
            "COMPARE MATERIALIZED VIEW mv AS OF TIMESTAMP '2026-01-01 00:00:00'",
            true,
            "COMPARE MATERIALIZED VIEW `mv` AS OF TIMESTAMP _UTF8MB4'2026-01-01 00:00:00'",
        ),
        (
            "COMPARE MATERIALIZED VIEW test.mv AS OF TIMESTAMP '2026-01-01 00:00:00' OUTPUT INTO TABLE test.diff",
            true,
            "COMPARE MATERIALIZED VIEW `test`.`mv` AS OF TIMESTAMP _UTF8MB4'2026-01-01 00:00:00' OUTPUT INTO TABLE `test`.`diff`",
        ),
        ("COMPARE MATERIALIZED VIEW mv", false, ""),
        ("COMPARE MATERIALIZED VIEW mv OUTPUT INTO TABLE diff", false, ""),
    ]);
}

/// The 1064 text a Go master `eca4674a6f` node answers for the
/// materialized view statements it refuses (after its fixed "You have an
/// error in your SQL syntax; ... to use " prefix), captured over the wire.
#[test]
fn materialized_view_syntax_errors_answer_go_masters_text() {
    for (sql, go) in [
        ("CREATE MATERIALIZED VIEW mv (a) COMMENT = 'c1' COMMENT = 'c2' AS SELECT 1", "line 1 column 64 near \"AS SELECT 1\"Duplicate COMMENT specified in CREATE MATERIALIZED VIEW "),
        ("CREATE MATERIALIZED VIEW mv (a) SHARD_ROW_ID_BITS = 1 SHARD_ROW_ID_BITS = 2 AS SELECT 1", "line 1 column 78 near \"AS SELECT 1\"Duplicate SHARD_ROW_ID_BITS specified in CREATE MATERIALIZED VIEW "),
        ("CREATE MATERIALIZED VIEW LOG ON t (a) PRE_SPLIT_REGIONS = 1 PRE_SPLIT_REGIONS = 2", "line 1 column 81 near \"\"Duplicate PRE_SPLIT_REGIONS specified in CREATE MATERIALIZED VIEW LOG "),
        ("CREATE MATERIALIZED VIEW mv (a) REFRESH FAST START WITH now() AS SELECT 1", "line 1 column 64 near \"AS SELECT 1\" "),
        ("CREATE MATERIALIZED VIEW LOG ON t (a) PURGE", "line 1 column 43 near \"\" "),
        ("REFRESH MATERIALIZED VIEW mv COMPLETE", "line 1 column 37 near \"\" "),
        ("CANCEL MATERIALIZED VIEW REFRESH JOB", "line 1 column 36 near \"\" "),
        ("COMPARE MATERIALIZED VIEW mv", "line 1 column 28 near \"\" "),
        ("show materialized view mv", "line 1 column 25 near \"\" "),
        ("show materialized view mv foo", "line 1 column 29 near \"\"syntax error: expected REMAIN_LOGS "),
        ("show materialized view log on t bar", "line 1 column 35 near \"\"syntax error: expected WAIT_PURGE "),
        ("create materialized view mv (a) with recursive c as (select 1) select * from c", "line 1 column 36 near \"with recursive c as (select 1) select * from c\" "),
        ("create materialized view mv as select 1", "line 1 column 30 near \"as select 1\" "),
        ("drop materialized view", "line 1 column 22 near \"\" "),
    ] {
        let error = parse(sql).expect_err(sql);
        assert_eq!(error.compatibility_message(sql), go, "{sql}");
    }
}

/// Go `parseExplainStmt` parses a statement only after an explainable token,
/// and Go `parseFrameBound` takes only a number, a `?` or `INTERVAL`: the
/// 1064 text Go master answers over unistore (`tools/xtake/go_x0607.jsonl`).
#[test]
fn explain_and_frame_bound_syntax_errors_answer_go_masters_text() {
    for (sql, go) in [
        ("EXPLAIN SHOW TABLES", "line 1 column 12 near \"SHOW TABLES\" "),
        ("EXPLAIN SET @a = 1", "line 1 column 11 near \"SET @a = 1\" "),
        ("EXPLAIN ANALYZE SHOW TABLES", "line 1 column 20 near \"SHOW TABLES\" "),
        ("EXPLAIN LOAD DATA INFILE 'x' INTO TABLE t", "line 1 column 12 near \"LOAD DATA INFILE 'x' INTO TABLE t\" "),
        ("EXPLAIN CALL p()", "line 1 column 12 near \"CALL p()\" "),
        ("EXPLAIN FORMAT='plan_tree' ANALYZE SELECT 1", "line 1 column 34 near \"ANALYZE SELECT 1\" "),
        // This repository's Go parser (`parseAdminShow`, probed directly).
        ("ADMIN SHOW TELEMETRY", "line 1 column 5 near \"ADMIN SHOW TELEMETRY\" "),
        ("ADMIN SHOW NEXT_ROW_ID", "line 1 column 5 near \"ADMIN SHOW NEXT_ROW_ID\" "),
        ("ADMIN SHOW ALL JOBS", "line 1 column 14 near \"ALL JOBS\" "),
        (
            "select sum(a) over (order by a rows between 1+0 preceding and current row) from t",
            "line 1 column 46 near \"+0 preceding and current row) from t\" ",
        ),
        (
            "select sum(a) over (order by a rows between a preceding and current row) from t",
            "line 1 column 45 near \"a preceding and current row) from t\" ",
        ),
    ] {
        let error = parse(sql).expect_err(sql);
        assert_eq!(error.compatibility_message(sql), go, "{sql}");
    }
    assert!(parse("select sum(a) over (order by a rows between 1 preceding and current row) from t").is_ok());
    assert!(parse("select sum(a) over (order by a range between interval 1 day preceding and current row) from t").is_ok());
    assert!(parse("select sum(a) over (order by a rows between ? preceding and current row) from t").is_ok());
}

/// Go master `parser_test.go`'s `MAX_COUNT` / `MIN_COUNT` rows of
/// `TestBuiltin` (the internal aggregates of materialized view maintenance,
/// also spelled in SQL).
#[test]
fn max_count_and_min_count_aggregates() {
    run_test(&[
        ("select max_count(c1,c2) from t;", false, ""),
        ("select max_count(distinct c1) from t;", false, ""),
        ("select max_count(c2) from t;", true, "SELECT MAX_COUNT(`c2`) FROM `t`"),
        ("select max_count(all c1) from t;", true, "SELECT MAX_COUNT(`c1`) FROM `t`"),
        ("select min_count(c1,c2) from t;", false, ""),
        ("select min_count(distinct c1) from t;", false, ""),
        ("select min_count(c2) from t;", true, "SELECT MIN_COUNT(`c2`) FROM `t`"),
        ("select min_count(all c1) from t;", true, "SELECT MIN_COUNT(`c1`) FROM `t`"),
    ]);
}
