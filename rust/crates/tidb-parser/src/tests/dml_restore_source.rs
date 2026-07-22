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

//! Source-owned SELECT-node restore rows from `pkg/parser/ast/dml_test.go`.
//! These tests keep LIMIT and select-field ownership in one leaf so changes to
//! AST restore formatting remain isolated from unrelated DML grammar tests.

use super::*;

#[test]
fn statement_priority_and_select_modifier_source_rows() {
    for (sql, expected) in [
        (
            "select high_priority * from t",
            "SELECT HIGH_PRIORITY * FROM `t`",
        ),
        (
            "select low_priority * from t",
            "SELECT LOW_PRIORITY * FROM `t`",
        ),
        ("select delayed * from t", "SELECT DELAYED * FROM `t`"),
        (
            "select sql_small_result sql_big_result sql_buffer_result sql_no_cache sql_calc_found_rows straight_join distinct a from t",
            "SELECT SQL_SMALL_RESULT SQL_BIG_RESULT SQL_BUFFER_RESULT SQL_NO_CACHE SQL_CALC_FOUND_ROWS DISTINCT STRAIGHT_JOIN `a` FROM `t`",
        ),
        (
            "select distinct sql_calc_found_rows high_priority straight_join a from t",
            "SELECT HIGH_PRIORITY SQL_CALC_FOUND_ROWS DISTINCT STRAIGHT_JOIN `a` FROM `t`",
        ),
        (
            "insert high_priority into t values (1)",
            "INSERT HIGH_PRIORITY INTO `t` VALUES (1)",
        ),
        (
            "insert low_priority into t values (1)",
            "INSERT LOW_PRIORITY INTO `t` VALUES (1)",
        ),
        (
            "insert delayed into t values (1)",
            "INSERT DELAYED INTO `t` VALUES (1)",
        ),
        (
            "replace high_priority into t values (1)",
            "REPLACE HIGH_PRIORITY INTO `t` VALUES (1)",
        ),
        (
            "update low_priority ignore t set a=2",
            "UPDATE LOW_PRIORITY IGNORE `t` SET `a`=2",
        ),
        (
            "update high_priority t set a=2",
            "UPDATE HIGH_PRIORITY `t` SET `a`=2",
        ),
        (
            "update delayed t set a=2",
            "UPDATE DELAYED `t` SET `a`=2",
        ),
        (
            "delete low_priority quick ignore from t where a=2",
            "DELETE LOW_PRIORITY QUICK IGNORE FROM `t` WHERE `a`=2",
        ),
        (
            "delete high_priority from t where a=2",
            "DELETE HIGH_PRIORITY FROM `t` WHERE `a`=2",
        ),
        (
            "delete delayed from t where a=2",
            "DELETE DELAYED FROM `t` WHERE `a`=2",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

fn field_list(sql: &str) -> String {
    let statement = parse(&format!("SELECT {sql}")).expect("parse select field list");
    let tidb_ast::Stmt::Query(query) = statement else {
        panic!("expected query statement");
    };
    let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
        panic!("expected plain SELECT statement");
    };
    select.restore_field_list()
}

/// Go source: `TestLimitRestore` (`dml_test.go:134`).
#[test]
fn limit_restore_source_rows() {
    for (sql, expected) in [
        ("limit 10", "LIMIT 10"),
        ("limit 10,20", "LIMIT 10,20"),
        ("limit 20 offset 10", "LIMIT 10,20"),
    ] {
        let statement = format!("SELECT 1 {sql}");
        assert_eq!(r(&statement), format!("SELECT 1 {expected}"), "{statement}");
    }
}

/// Go source: `TestWildCardFieldRestore` (`dml_test.go:146`).
#[test]
fn wildcard_field_restore_source_rows() {
    for (sql, expected) in [
        ("*", "*"),
        ("t.*", "`t`.*"),
        ("testdb.t.*", "`testdb`.`t`.*"),
    ] {
        let statement = format!("SELECT {sql}");
        assert_eq!(r(&statement), format!("SELECT {expected}"), "{statement}");
    }
}

/// Go source: `TestSelectFieldRestore` (`dml_test.go:158`).
#[test]
fn select_field_restore_source_rows() {
    for (sql, expected) in [
        ("*", "*"),
        ("t.*", "`t`.*"),
        ("testdb.t.*", "`testdb`.`t`.*"),
        ("col as a", "`col` AS `a`"),
        ("col + 1 a", "`col`+1 AS `a`"),
    ] {
        let statement = format!("SELECT {sql}");
        assert_eq!(r(&statement), format!("SELECT {expected}"), "{statement}");
    }
}

/// Go source: `TestFieldListRestore` (`dml_test.go:172`).
#[test]
fn field_list_restore_source_rows() {
    for (sql, expected) in [
        ("*", "*"),
        ("t.*", "`t`.*"),
        ("testdb.t.*", "`testdb`.`t`.*"),
        ("col as a", "`col` AS `a`"),
        ("`t`.*, s.col as a", "`t`.*, `s`.`col` AS `a`"),
    ] {
        assert_eq!(field_list(sql), expected, "field list {sql}");
    }
}

#[test]
fn test_table_refs_clause_restore() {
    assert_dml_restore_rows(&[
        ("select * from t", "SELECT * FROM `t`"),
        ("select * from t1 join t2", "SELECT * FROM `t1` JOIN `t2`"),
        ("select * from t1, t2", "SELECT * FROM (`t1`) JOIN `t2`"),
    ]);
}

#[test]
fn test_delete_table_list_restore() {
    assert_dml_restore_rows(&[
        (
            "DELETE t1,t2 FROM t1, t2",
            "DELETE `t1`,`t2` FROM (`t1`) JOIN `t2`",
        ),
        (
            "DELETE FROM t1,t2 USING t1, t2",
            "DELETE FROM `t1`,`t2` USING (`t1`) JOIN `t2`",
        ),
    ]);
}

#[test]
fn test_delete_table_index_hint_restore() {
    assert_dml_restore_rows(&[
        (
            "DELETE FROM t1 USE key (`fld1`) WHERE fld=1",
            "DELETE FROM `t1` USE INDEX (`fld1`) WHERE `fld`=1",
        ),
        (
            "DELETE FROM t1 as tbl USE key (`fld1`) WHERE tbl.fld=2",
            "DELETE FROM `t1` AS `tbl` USE INDEX (`fld1`) WHERE `tbl`.`fld`=2",
        ),
    ]);
}

#[test]
fn test_by_item_restore() {
    assert_dml_restore_rows(&[
        (
            "select * from t order by a desc",
            "SELECT * FROM `t` ORDER BY `a` DESC",
        ),
        (
            "select * from t order by NULL",
            "SELECT * FROM `t` ORDER BY NULL",
        ),
    ]);
}

#[test]
fn test_group_by_clause_restore() {
    assert_dml_restore_rows(&[
        (
            "select * from t GROUP BY a,b desc",
            "SELECT * FROM `t` GROUP BY `a`,`b` DESC",
        ),
        (
            "select * from t GROUP BY 1 desc,b",
            "SELECT * FROM `t` GROUP BY 1 DESC,`b`",
        ),
    ]);
}

#[test]
fn test_order_by_clause_restore() {
    assert_dml_restore_rows(&[
        (
            "SELECT 1 FROM t1 ORDER BY a,b",
            "SELECT 1 FROM `t1` ORDER BY `a`,`b`",
        ),
        (
            "SELECT 1 FROM t1 UNION SELECT 2 FROM t2 ORDER BY a,b",
            "SELECT 1 FROM `t1` UNION SELECT 2 FROM `t2` ORDER BY `a`,`b`",
        ),
    ]);
}

#[test]
fn test_assignment_restore() {
    assert_dml_restore_rows(&[
        ("UPDATE t1 SET a=1", "UPDATE `t1` SET `a`=1"),
        ("UPDATE t1 SET b=1+2", "UPDATE `t1` SET `b`=1+2"),
    ]);
}

#[test]
fn test_having_clause_restore() {
    assert_dml_restore_rows(&[
        (
            "select 1 from t1 group by 1 HAVING a>b",
            "SELECT 1 FROM `t1` GROUP BY 1 HAVING `a`>`b`",
        ),
        (
            "select 1 from t1 group by 1 HAVING NULL",
            "SELECT 1 FROM `t1` GROUP BY 1 HAVING NULL",
        ),
    ]);
}

fn assert_dml_restore_rows(rows: &[(&str, &str)]) {
    for (sql, expected) in rows {
        let actual = parse(sql)
            .unwrap_or_else(|error| panic!("parse {sql}: {error:?}"))
            .restore();
        assert_eq!(actual, *expected, "{sql}");
    }
}

#[test]
fn test_frame_bound_restore() {
    for (bound, expected) in [
        ("CURRENT ROW", "CURRENT ROW"),
        ("UNBOUNDED PRECEDING", "UNBOUNDED PRECEDING"),
        ("1 PRECEDING", "1 PRECEDING"),
        ("? PRECEDING", "? PRECEDING"),
        ("INTERVAL 5 DAY PRECEDING", "INTERVAL 5 DAY PRECEDING"),
        ("UNBOUNDED FOLLOWING", "UNBOUNDED FOLLOWING"),
        ("1 FOLLOWING", "1 FOLLOWING"),
        ("? FOLLOWING", "? FOLLOWING"),
        (
            "INTERVAL '2:30' MINUTE_SECOND FOLLOWING",
            "INTERVAL _UTF8MB4'2:30' MINUTE_SECOND FOLLOWING",
        ),
    ] {
        assert_eq!(
            r(&format!(
                "select avg(val) over (rows between {bound} and current row) from t"
            )),
            format!("SELECT AVG(`val`) OVER (ROWS BETWEEN {expected} AND CURRENT ROW) FROM `t`"),
            "{bound}"
        );
    }
}

#[test]
fn test_frame_clause_restore() {
    for (frame, expected) in [
        ("ROWS CURRENT ROW", "ROWS BETWEEN CURRENT ROW AND CURRENT ROW"),
        (
            "ROWS UNBOUNDED PRECEDING",
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
        ),
        (
            "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
            "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
        ),
        (
            "RANGE BETWEEN ? PRECEDING AND ? FOLLOWING",
            "RANGE BETWEEN ? PRECEDING AND ? FOLLOWING",
        ),
        (
            "RANGE BETWEEN INTERVAL 5 DAY PRECEDING AND INTERVAL '2:30' MINUTE_SECOND FOLLOWING",
            "RANGE BETWEEN INTERVAL 5 DAY PRECEDING AND INTERVAL _UTF8MB4'2:30' MINUTE_SECOND FOLLOWING",
        ),
    ] {
        assert_eq!(
            r(&format!("select avg(val) over ({frame}) from t")),
            format!("SELECT AVG(`val`) OVER ({expected}) FROM `t`"),
            "{frame}"
        );
    }
}

#[test]
fn test_window_spec_restore_named_definitions() {
    for (spec, expected) in [
        ("w as ()", "`w` AS ()"),
        ("w as (w1)", "`w` AS (`w1`)"),
        (
            "w as (w1 order by country)",
            "`w` AS (`w1` ORDER BY `country`)",
        ),
        (
            "w as (partition by a order by b rows current row)",
            "`w` AS (PARTITION BY `a` ORDER BY `b` ROWS BETWEEN CURRENT ROW AND CURRENT ROW)",
        ),
    ] {
        assert_eq!(
            r(&format!("select rank() over w from t window {spec}")),
            format!("SELECT RANK() OVER `w` FROM `t` WINDOW {expected}"),
            "{spec}"
        );
    }
}

#[test]
fn test_partition_by_clause_restore() {
    for (partition, expected) in [
        ("PARTITION BY a", "PARTITION BY `a`"),
        ("PARTITION BY NULL", "PARTITION BY NULL"),
        ("PARTITION BY a, b", "PARTITION BY `a`, `b`"),
    ] {
        assert_eq!(
            r(&format!(
                "select avg(val) over ({partition} rows current row) from t"
            )),
            format!(
                "SELECT AVG(`val`) OVER ({expected} ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM `t`"
            ),
            "{partition}"
        );
    }
}

#[test]
fn test_window_spec_restore() {
    for (spec, expected) in [
        ("w", "`w`"),
        ("()", "()"),
        ("(w)", "(`w`)"),
        ("(w PARTITION BY country)", "(`w` PARTITION BY `country`)"),
        (
            "(PARTITION BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
            "(PARTITION BY `a` ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
        ),
    ] {
        assert_eq!(
            r(&format!(
                "select rank() over {spec} from t window w as (order by a)"
            )),
            format!("SELECT RANK() OVER {expected} FROM `t` WINDOW `w` AS (ORDER BY `a`)"),
            "{spec}"
        );
    }
}

#[test]
fn test_import_into_restore() {
    for (sql, expected) in [
        (
            "IMPORT INTO t from '/file.csv'",
            "IMPORT INTO `t` FROM '/file.csv'",
        ),
        (
            "IMPORT INTO t (a, @1, c) from '/file.csv'",
            "IMPORT INTO `t` (`a`,@`1`,`c`) FROM '/file.csv'",
        ),
        (
            "IMPORT INTO t set a=100 from '/file.csv'",
            "IMPORT INTO `t` SET `a`=100 FROM '/file.csv'",
        ),
        (
            "IMPORT INTO t (b, c) set a=100 from '/file.csv'",
            "IMPORT INTO `t` (`b`,`c`) SET `a`=100 FROM '/file.csv'",
        ),
        (
            "IMPORT INTO t from '/file.csv' format 'csv'",
            "IMPORT INTO `t` FROM '/file.csv' FORMAT 'csv'",
        ),
        (
            "IMPORT INTO `t` from '/file.csv' with detached",
            "IMPORT INTO `t` FROM '/file.csv' WITH detached",
        ),
        (
            "IMPORT INTO `t` from '/file.csv' with detached, thread=1",
            "IMPORT INTO `t` FROM '/file.csv' WITH detached, thread=1",
        ),
        (
            "IMPORT INTO `t` from '/file.csv' with fields_terminated_by=_UTF8MB4'\t', detached",
            "IMPORT INTO `t` FROM '/file.csv' WITH fields_terminated_by=_UTF8MB4'\t', detached",
        ),
        (
            "IMPORT INTO `t` from '/file.csv' with fields_terminated_by=_UTF8MB4'\t', detached, thread=1",
            "IMPORT INTO `t` FROM '/file.csv' WITH fields_terminated_by=_UTF8MB4'\t', detached, thread=1",
        ),
        (
            "IMPORT INTO `t` from select * from xx",
            "IMPORT INTO `t` FROM SELECT * FROM `xx`",
        ),
        (
            "IMPORT INTO `t` from with `c` as (select * from `xx`) select * from `c` with thread=1",
            "IMPORT INTO `t` FROM WITH `c` AS (SELECT * FROM `xx`) SELECT * FROM `c` WITH thread=1",
        ),
        (
            "IMPORT INTO `t` from select * from `xx` union select * from `yy` with thread=1",
            "IMPORT INTO `t` FROM SELECT * FROM `xx` UNION SELECT * FROM `yy` WITH thread=1",
        ),
        (
            "IMPORT INTO `t` from with `c` as (select * from `xx`) select * from `c` union select * from `c` with thread=1",
            "IMPORT INTO `t` FROM WITH `c` AS (SELECT * FROM `xx`) SELECT * FROM `c` UNION SELECT * FROM `c` WITH thread=1",
        ),
        (
            "IMPORT INTO `t` from (select * from xx)",
            "IMPORT INTO `t` FROM (SELECT * FROM `xx`)",
        ),
    ] {
        let actual = parse(sql)
            .unwrap_or_else(|error| panic!("parse {sql}: {error:?}"))
            .restore();
        assert_eq!(actual, expected, "{sql}");
    }
}

#[test]
fn test_import_actions() {
    for (sql, expected) in [
        ("cancel import job 123", "CANCEL IMPORT JOB 123"),
        ("show import jobs", "SHOW IMPORT JOBS"),
        ("show import job 123", "SHOW IMPORT JOB 123"),
        ("show raw import jobs", "SHOW RAW IMPORT JOBS"),
        ("show raw import job 123", "SHOW RAW IMPORT JOB 123"),
        (
            "show raw import jobs where group_key = 'g'",
            "SHOW RAW IMPORT JOBS WHERE `group_key`=_UTF8MB4'g'",
        ),
        (
            "show import jobs where aa > 1",
            "SHOW IMPORT JOBS WHERE `aa`>1",
        ),
        ("show import groups", "SHOW IMPORT GROUPS"),
        ("show import group '123'", "SHOW IMPORT GROUP '123'"),
    ] {
        let actual = parse(sql)
            .unwrap_or_else(|error| panic!("parse {sql}: {error:?}"))
            .restore();
        assert_eq!(actual, expected, "{sql}");
    }
}

#[test]
fn test_fulltext_search_modifier() {
    use tidb_ast::MatchModifier;

    assert!(!MatchModifier::None.is_boolean_mode());
    assert!(MatchModifier::None.is_natural_language_mode());
    assert!(!MatchModifier::None.with_query_expansion());
}

#[test]
fn test_import_into_secure_text() {
    for (sql, expected) in [
        (
            "import into t from 's3://bucket/prefix?access-key=aaaaa&secret-access-key=bbbbb'",
            "IMPORT INTO `t` FROM 's3://bucket/prefix?access-key=xxxxxx&secret-access-key=xxxxxx'",
        ),
        (
            "import into t from 'gcs://bucket/prefix?access-key=aaaaa&secret-access-key=bbbbb'",
            "IMPORT INTO `t` FROM 'gcs://bucket/prefix?access-key=aaaaa&secret-access-key=bbbbb'",
        ),
        (
            "import into t from 's3://bucket/prefix?access-key=aaaaa&secret-access-key=bbbbb' with CLOUD_STORAGE_uri='s3://bucket/prefix?access-key=cccccc&secret-access-key=dddddd'",
            "IMPORT INTO `t` FROM 's3://bucket/prefix?access-key=xxxxxx&secret-access-key=xxxxxx' WITH cloud_storage_uri='s3://bucket/prefix?access-key=xxxxxx&secret-access-key=xxxxxx'",
        ),
    ] {
        let statement = parse(sql).expect("parse IMPORT INTO");
        let tidb_ast::Stmt::Dml(dml) = statement else {
            panic!("expected DML statement");
        };
        let tidb_ast::DmlStmt::ImportInto(import) = dml.as_ref() else {
            panic!("expected IMPORT INTO statement");
        };
        assert_eq!(import.secure_text(), expected, "{sql}");
    }
}

#[test]
fn test_import_into_from_select_invalid_stmt() {
    for (sql, message) in [
        (
            "IMPORT INTO t1(a, @1) FROM select * from t2;",
            "Cannot use user variable(1) in IMPORT INTO FROM SELECT statement",
        ),
        (
            "IMPORT INTO t1(a, @b) FROM select * from t2;",
            "Cannot use user variable(b) in IMPORT INTO FROM SELECT statement",
        ),
        (
            "IMPORT INTO t1(a) set a=1 FROM select a from t2;",
            "Cannot use SET clause in IMPORT INTO FROM SELECT statement.",
        ),
    ] {
        let error = parse(sql).expect_err("IMPORT INTO FROM SELECT must reject mappings");
        assert!(error.message.contains(message), "{sql}: {error:?}");
    }
}

#[test]
fn returning_and_insert_row_alias_source_rows() {
    for (sql, expected) in [
        ("INSERT INTO t (a) VALUES (1) RETURNING *", "INSERT INTO `t` (`a`) VALUES (1) RETURNING *"),
        ("INSERT INTO t (a) VALUES (1) RETURNING id", "INSERT INTO `t` (`a`) VALUES (1) RETURNING `id`"),
        ("INSERT INTO t (a) VALUES (1) RETURNING id, name", "INSERT INTO `t` (`a`) VALUES (1) RETURNING `id`, `name`"),
        ("INSERT INTO t2(id,animal) VALUES (1,'Dog'),(2,'Lion'),(3,'Tiger'),(4,'Leopard') RETURNING id,id+id,id&id,id||id", "INSERT INTO `t2` (`id`,`animal`) VALUES (1,_UTF8MB4'Dog'),(2,_UTF8MB4'Lion'),(3,_UTF8MB4'Tiger'),(4,_UTF8MB4'Leopard') RETURNING `id`, `id`+`id`, `id`&`id`, `id` OR `id`"),
        ("INSERT INTO t (a) VALUES (1) ON DUPLICATE KEY UPDATE a=2 RETURNING id", "INSERT INTO `t` (`a`) VALUES (1) ON DUPLICATE KEY UPDATE `a`=2 RETURNING `id`"),
        ("UPDATE t SET a=1 RETURNING *", "UPDATE `t` SET `a`=1 RETURNING *"),
        ("UPDATE t SET a=1 WHERE id=1 RETURNING id, a", "UPDATE `t` SET `a`=1 WHERE `id`=1 RETURNING `id`, `a`"),
        ("UPDATE t SET a=1 LIMIT 1 RETURNING *", "UPDATE `t` SET `a`=1 LIMIT 1 RETURNING *"),
        ("DELETE FROM t RETURNING *", "DELETE FROM `t` RETURNING *"),
        ("DELETE FROM t WHERE id=1 RETURNING id", "DELETE FROM `t` WHERE `id`=1 RETURNING `id`"),
        ("DELETE FROM t ORDER BY id LIMIT 1 RETURNING *", "DELETE FROM `t` ORDER BY `id` LIMIT 1 RETURNING *"),
        ("INSERT INTO t (a,b,c) VALUES (1,2,3) AS new ON DUPLICATE KEY UPDATE c=new.a+new.b", "INSERT INTO `t` (`a`,`b`,`c`) VALUES (1,2,3) AS `new` ON DUPLICATE KEY UPDATE `c`=`new`.`a`+`new`.`b`"),
        ("INSERT INTO t (a,b,c) VALUES (1,2,3),(4,5,6) AS new(m,n,p) ON DUPLICATE KEY UPDATE c=m+n", "INSERT INTO `t` (`a`,`b`,`c`) VALUES (1,2,3),(4,5,6) AS `new`(`m`, `n`, `p`) ON DUPLICATE KEY UPDATE `c`=`m`+`n`"),
        ("INSERT INTO t VALUES (1,2) AS new ON DUPLICATE KEY UPDATE b=new.b", "INSERT INTO `t` VALUES (1,2) AS `new` ON DUPLICATE KEY UPDATE `b`=`new`.`b`"),
        ("INSERT INTO t SET a=1,b=2 AS new ON DUPLICATE KEY UPDATE b=new.a+new.b", "INSERT INTO `t` SET `a`=1,`b`=2 AS `new` ON DUPLICATE KEY UPDATE `b`=`new`.`a`+`new`.`b`"),
        ("INSERT INTO t SET a=1,b=2 AS new(m,n) ON DUPLICATE KEY UPDATE b=m+n", "INSERT INTO `t` SET `a`=1,`b`=2 AS `new`(`m`, `n`) ON DUPLICATE KEY UPDATE `b`=`m`+`n`"),
        ("INSERT INTO t VALUES (1,2) AS new", "INSERT INTO `t` VALUES (1,2) AS `new`"),
        ("INSERT INTO t VALUES (1,2) AS new(a,b)", "INSERT INTO `t` VALUES (1,2) AS `new`(`a`, `b`)"),
    ] {
        let actual = parse(sql)
            .unwrap_or_else(|error| panic!("Go TestDMLStmt row {sql}: {error:?}"))
            .restore();
        assert_eq!(actual, expected, "Go TestDMLStmt row: {sql}");
    }
    for sql in [
        "REPLACE INTO t VALUES (1,2) AS new",
        "REPLACE INTO t SET a=1,b=2 AS new",
    ] {
        assert!(
            parse(sql).is_err(),
            "Go rejects row aliases for REPLACE: {sql}"
        );
    }
}

#[test]
fn insert_table_result_source_rows() {
    for (sql, expected) in [
        ("INSERT INTO ta TABLE tb", "INSERT INTO `ta` TABLE `tb`"),
        (
            "INSERT INTO t.a TABLE t.b",
            "INSERT INTO `t`.`a` TABLE `t`.`b`",
        ),
    ] {
        assert_eq!(r(sql), expected, "Go TestDMLStmt row: {sql}");
    }
}

#[test]
fn insert_explicit_empty_column_list_source_row() {
    assert_eq!(
        r("INSERT INTO foo () VALUES ()"),
        "INSERT INTO `foo` () VALUES ()"
    );
}

#[test]
fn distribute_table_source_rows() {
    for (sql, expected) in [
        ("distribute table t1 rule = 'leader-scatter' engine = 'tikv'", "DISTRIBUTE TABLE `t1` RULE = 'leader-scatter' ENGINE = 'tikv'"),
        ("distribute table t1 rule = \"leader-scatter\" engine = \"tikv\"", "DISTRIBUTE TABLE `t1` RULE = 'leader-scatter' ENGINE = 'tikv'"),
        ("distribute table t1 partition(p0,p1) rule = 'learner-scatter' engine = 'tikv'", "DISTRIBUTE TABLE `t1` PARTITION(`p0`, `p1`) RULE = 'learner-scatter' ENGINE = 'tikv'"),
        ("distribute table t1 partition(p0) rule = 'peer-scatter' engine = 'tiflash'", "DISTRIBUTE TABLE `t1` PARTITION(`p0`) RULE = 'peer-scatter' ENGINE = 'tiflash'"),
        ("distribute table t1 partition(p0) rule = 'peer-scatter' engine = 'tiflash' timeout = '30m'", "DISTRIBUTE TABLE `t1` PARTITION(`p0`) RULE = 'peer-scatter' ENGINE = 'tiflash' TIMEOUT = '30m'"),
    ] {
        assert_eq!(r(sql), expected, "Go TestDMLStmt row: {sql}");
    }
    for sql in [
        "distribute table t1",
        "distribute table t1 partition(p0)",
        "distribute table t1 partition(p0,p1)",
        "distribute table t1 partition(p0,p1) engine = tikv",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}
