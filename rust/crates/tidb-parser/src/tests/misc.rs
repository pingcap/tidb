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

//! Transcreation of `pkg/parser/ast/misc_test.go`.

use super::*;

/// Exact rows from Go `pkg/parser/parser_test.go:TestSignedInt64OutOfRange`.
#[test]
fn signed_int64_fields_reject_uint64_only_values() {
    for sql in [
        "recover table by job 18446744073709551612",
        "recover table t 18446744073709551612",
        "admin check index t idx (0, 18446744073709551612)",
        "create user abc@def with max_queries_per_hour 18446744073709551612",
    ] {
        let error = parse(sql).expect_err("signed field must reject a uint64-only value");
        assert!(
            error.message.contains("out of range"),
            "source SQL: {sql}; error: {error:?}"
        );
    }
}

/// `pkg/parser/ast/misc_test.go::TestMiscVisitorCover`.
#[test]
fn test_misc_visitor_cover() {
    for sql in [
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "DO 1",
        "EXPLAIN SELECT 1",
        "PREPARE s FROM @sql",
        "EXECUTE s USING @a",
        "DEALLOCATE PREPARE s",
        "SET @a = 1",
        "USE d",
        "FLUSH TABLES",
        "KILL 1",
        "SHUTDOWN",
    ] {
        assert_full_visitor_traversal(sql);
    }
}

/// `pkg/parser/ast/misc_test.go::TestDDLVisitorCoverMisc`.
#[test]
fn test_ddl_visitor_cover_misc() {
    for sql in [
        "CREATE TABLE t (c1 SMALLINT UNSIGNED, c2 INT UNSIGNED)",
        "ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED AFTER b",
        "ALTER TABLE t ADD COLUMN (a INT, CONSTRAINT c CHECK (a > 0))",
        "CREATE INDEX t_i ON t (id)",
        "CREATE DATABASE test CHARACTER SET utf8",
        "DROP DATABASE test",
        "DROP INDEX t_i ON t",
        "DROP TABLE t",
        "TRUNCATE TABLE t",
    ] {
        assert_full_visitor_traversal(sql);
    }
}

/// `pkg/parser/ast/misc_test.go::TestDMLVistorCover`.
#[test]
fn test_dml_vistor_cover() {
    for sql in [
        "DELETE FROM somelog WHERE user = 'jcole' ORDER BY timestamp_column LIMIT 1",
        "DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id",
        "SELECT * FROM t WHERE EXISTS(SELECT * FROM t k WHERE t.c = k.c HAVING SUM(c) = 1)",
        "INSERT INTO t_copy SELECT * FROM t WHERE t.x > 5",
        "UPDATE t1 SET col1 = col1 + 1, col2 = col1",
        "SHOW CREATE TABLE t",
        "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE t FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b'",
        "IMPORT INTO t FROM '/file.csv'",
    ] {
        assert_full_visitor_traversal(sql);
    }
}

#[test]
fn test_sensitive_statement() {
    for sql in [
        "set password = 'secret'",
        "create user u identified by 'secret'",
        "alter user u identified by 'secret'",
        "grant select on *.* to u",
    ] {
        let statement = parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert!(statement.is_sensitive(), "{sql}");
    }
    for sql in [
        "drop user u",
        "revoke select on *.* from u",
        "alter table t add column a int",
        "create database d",
        "create index i on t(a)",
        "create table t(a int)",
        "drop database d",
        "drop index i on t",
        "drop table t",
        "rename table t to u",
        "truncate table t",
    ] {
        let statement = parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert!(!statement.is_sensitive(), "{sql}");
    }
}

#[test]
fn remaining_misc_statement_restore_source_rows() {
    for (sql, expected) in [
        (
            "trace format = 'json' select 1",
            "TRACE FORMAT = 'json' SELECT 1",
        ),
        (
            "trace plan target = 'estimation' select 1",
            "TRACE PLAN TARGET = 'estimation' SELECT 1",
        ),
        (
            "explain format = 'brief' for connection 42",
            "EXPLAIN FORMAT = 'brief' FOR CONNECTION 42",
        ),
        ("binlog 'abc'", "BINLOG 'abc'"),
        ("kill tidb query 42", "KILL TIDB QUERY 42"),
        ("kill query @id", "KILL QUERY @`id`"),
        (
            "set config tikv log.level = 'debug'",
            "SET CONFIG TIKV LOG.LEVEL = _UTF8MB4'debug'",
        ),
        (
            "set config '127.0.0.1:20160' raftstore.store-pool-size = 4",
            "SET CONFIG '127.0.0.1:20160' RAFTSTORE.STORE-POOL-SIZE = 4",
        ),
        (
            "create statistics if not exists s (correlation) on db.t(a,b)",
            "CREATE STATISTICS IF NOT EXISTS `s` (CORRELATION) ON `db`.`t`(`a`, `b`)",
        ),
        ("drop statistics s", "DROP STATISTICS `s`"),
        ("shutdown", "SHUTDOWN"),
        ("restart", "RESTART"),
        ("help 'contents'", "HELP 'contents'"),
        ("cancel distribution job 7", "CANCEL DISTRIBUTION JOB 7"),
        (
            "calibrate resource workload tpcc",
            "CALIBRATE RESOURCE WORKLOAD TPCC",
        ),
        (
            "calibrate resource start_time = '2024-01-01' end_time = '2024-01-02'",
            "CALIBRATE RESOURCE START_TIME _UTF8MB4'2024-01-01' END_TIME _UTF8MB4'2024-01-02'",
        ),
        (
            "calibrate resource duration = '10m'",
            "CALIBRATE RESOURCE DURATION '10m'",
        ),
        (
            "calibrate resource duration interval 5 minute",
            "CALIBRATE RESOURCE DURATION INTERVAL 5 MINUTE",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn statistics_zero_value_and_name_boundaries_match_go_source() {
    assert_eq!(
        r("create statistics 1 (bogus) on a.1(c)"),
        "CREATE STATISTICS `1` (CARDINALITY) ON `a`.`1`(`c`)"
    );
    assert_eq!(
        r("create statistics 'x' (dependency) on 't'(@c)"),
        "CREATE STATISTICS `x` (DEPENDENCY) ON `t`(`c`)"
    );
    assert_eq!(r("drop statistics"), "DROP STATISTICS ``");
    assert_eq!(r("drop statistics @x"), "DROP STATISTICS `x`");
}

#[test]
fn recommend_index_restore_source_rows() {
    for (sql, expected) in [
        ("recommend index run", "RECOMMEND INDEX RUN"),
        (
            "recommend index run with A = 1",
            "RECOMMEND INDEX RUN WITH A = 1",
        ),
        (
            "recommend index run with A = 1, B = 2",
            "RECOMMEND INDEX RUN WITH A = 1, B = 2",
        ),
        (
            "recommend index run for 'select * from t where a=1'",
            "RECOMMEND INDEX RUN FOR 'select * from t where a=1'",
        ),
        (
            "recommend index run for 'select * from t where a=1' with A = 1, B = 2",
            "RECOMMEND INDEX RUN FOR 'select * from t where a=1' WITH A = 1, B = 2",
        ),
        ("recommend index show option", "RECOMMEND INDEX SHOW OPTION"),
        ("recommend index apply 1", "RECOMMEND INDEX APPLY 1"),
        ("recommend index ignore 1", "RECOMMEND INDEX IGNORE 1"),
        (
            "recommend index set A = 1, B = 2, C = 3",
            "RECOMMEND INDEX SET A = 1, B = 2, C = 3",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn grant_proxy_restore_source_rows() {
    for (sql, expected) in [
        (
            "GRANT PROXY ON 'localuser'@'localhost' TO 'externaluser'@'somehost'",
            "GRANT PROXY ON `localuser`@`localhost` TO `externaluser`@`somehost`",
        ),
        (
            "GRANT PROXY ON ''@'' TO 'root'@'localhost' WITH GRANT OPTION",
            "GRANT PROXY ON ``@`` TO `root`@`localhost` WITH GRANT OPTION",
        ),
        (
            "GRANT PROXY ON 'proxied_user' TO 'proxy_user1', 'proxy_user2'",
            "GRANT PROXY ON `proxied_user`@`%` TO `proxy_user1`@`%`, `proxy_user2`@`%`",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn test_table_optimizer_hint_restore() {
    let rows = [
        ("USE_INDEX(t1 c1)", "USE_INDEX(`t1` `c1`)"),
        ("USE_INDEX(test.t1 c1)", "USE_INDEX(`test`.`t1` `c1`)"),
        ("USE_INDEX(@sel_1 t1 c1)", "USE_INDEX(@`sel_1` `t1` `c1`)"),
        ("USE_INDEX(t1@sel_1 c1)", "USE_INDEX(`t1`@`sel_1` `c1`)"),
        (
            "USE_INDEX(test.t1@sel_1 c1)",
            "USE_INDEX(`test`.`t1`@`sel_1` `c1`)",
        ),
        (
            "USE_INDEX(test.t1@sel_1 partition(p0) c1)",
            "USE_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)",
        ),
        ("FORCE_INDEX(t1 c1)", "FORCE_INDEX(`t1` `c1`)"),
        ("FORCE_INDEX(test.t1 c1)", "FORCE_INDEX(`test`.`t1` `c1`)"),
        (
            "FORCE_INDEX(@sel_1 t1 c1)",
            "FORCE_INDEX(@`sel_1` `t1` `c1`)",
        ),
        ("FORCE_INDEX(t1@sel_1 c1)", "FORCE_INDEX(`t1`@`sel_1` `c1`)"),
        (
            "FORCE_INDEX(test.t1@sel_1 c1)",
            "FORCE_INDEX(`test`.`t1`@`sel_1` `c1`)",
        ),
        (
            "FORCE_INDEX(test.t1@sel_1 partition(p0) c1)",
            "FORCE_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)",
        ),
        ("IGNORE_INDEX(t1 c1)", "IGNORE_INDEX(`t1` `c1`)"),
        (
            "IGNORE_INDEX(@sel_1 t1 c1)",
            "IGNORE_INDEX(@`sel_1` `t1` `c1`)",
        ),
        (
            "IGNORE_INDEX(t1@sel_1 c1)",
            "IGNORE_INDEX(`t1`@`sel_1` `c1`)",
        ),
        (
            "IGNORE_INDEX(t1@sel_1 partition(p0, p1) c1)",
            "IGNORE_INDEX(`t1`@`sel_1` PARTITION(`p0`, `p1`) `c1`)",
        ),
        ("ORDER_INDEX(t1 c1)", "ORDER_INDEX(`t1` `c1`)"),
        ("ORDER_INDEX(test.t1 c1)", "ORDER_INDEX(`test`.`t1` `c1`)"),
        (
            "ORDER_INDEX(@sel_1 t1 c1)",
            "ORDER_INDEX(@`sel_1` `t1` `c1`)",
        ),
        ("ORDER_INDEX(t1@sel_1 c1)", "ORDER_INDEX(`t1`@`sel_1` `c1`)"),
        (
            "ORDER_INDEX(test.t1@sel_1 c1)",
            "ORDER_INDEX(`test`.`t1`@`sel_1` `c1`)",
        ),
        (
            "ORDER_INDEX(test.t1@sel_1 partition(p0) c1)",
            "ORDER_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)",
        ),
        ("NO_ORDER_INDEX(t1 c1)", "NO_ORDER_INDEX(`t1` `c1`)"),
        (
            "NO_ORDER_INDEX(test.t1 c1)",
            "NO_ORDER_INDEX(`test`.`t1` `c1`)",
        ),
        (
            "NO_ORDER_INDEX(@sel_1 t1 c1)",
            "NO_ORDER_INDEX(@`sel_1` `t1` `c1`)",
        ),
        (
            "NO_ORDER_INDEX(t1@sel_1 c1)",
            "NO_ORDER_INDEX(`t1`@`sel_1` `c1`)",
        ),
        (
            "NO_ORDER_INDEX(test.t1@sel_1 c1)",
            "NO_ORDER_INDEX(`test`.`t1`@`sel_1` `c1`)",
        ),
        (
            "NO_ORDER_INDEX(test.t1@sel_1 partition(p0) c1)",
            "NO_ORDER_INDEX(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)",
        ),
        (
            "INDEX_LOOKUP_PUSHDOWN(t1 c1)",
            "INDEX_LOOKUP_PUSHDOWN(`t1` `c1`)",
        ),
        (
            "INDEX_LOOKUP_PUSHDOWN(test.t1 c1)",
            "INDEX_LOOKUP_PUSHDOWN(`test`.`t1` `c1`)",
        ),
        (
            "INDEX_LOOKUP_PUSHDOWN(@sel_1 t1 c1)",
            "INDEX_LOOKUP_PUSHDOWN(@`sel_1` `t1` `c1`)",
        ),
        (
            "INDEX_LOOKUP_PUSHDOWN(t1@sel_1 c1)",
            "INDEX_LOOKUP_PUSHDOWN(`t1`@`sel_1` `c1`)",
        ),
        (
            "INDEX_LOOKUP_PUSHDOWN(test.t1@sel_1 c1)",
            "INDEX_LOOKUP_PUSHDOWN(`test`.`t1`@`sel_1` `c1`)",
        ),
        (
            "INDEX_LOOKUP_PUSHDOWN(test.t1@sel_1 partition(p0) c1)",
            "INDEX_LOOKUP_PUSHDOWN(`test`.`t1`@`sel_1` PARTITION(`p0`) `c1`)",
        ),
        ("TIDB_SMJ(`t1`)", "TIDB_SMJ(`t1`)"),
        ("TIDB_SMJ(t1)", "TIDB_SMJ(`t1`)"),
        ("TIDB_SMJ(t1,t2)", "TIDB_SMJ(`t1`, `t2`)"),
        ("TIDB_SMJ(@sel1 t1,t2)", "TIDB_SMJ(@`sel1` `t1`, `t2`)"),
        (
            "TIDB_SMJ(t1@sel1,t2@sel2)",
            "TIDB_SMJ(`t1`@`sel1`, `t2`@`sel2`)",
        ),
        ("TIDB_INLJ(t1,t2)", "TIDB_INLJ(`t1`, `t2`)"),
        ("TIDB_INLJ(@sel1 t1,t2)", "TIDB_INLJ(@`sel1` `t1`, `t2`)"),
        (
            "TIDB_INLJ(t1@sel1,t2@sel2)",
            "TIDB_INLJ(`t1`@`sel1`, `t2`@`sel2`)",
        ),
        ("TIDB_HJ(t1,t2)", "TIDB_HJ(`t1`, `t2`)"),
        ("TIDB_HJ(@sel1 t1,t2)", "TIDB_HJ(@`sel1` `t1`, `t2`)"),
        (
            "TIDB_HJ(t1@sel1,t2@sel2)",
            "TIDB_HJ(`t1`@`sel1`, `t2`@`sel2`)",
        ),
        ("MERGE_JOIN(t1,t2)", "MERGE_JOIN(`t1`, `t2`)"),
        ("BROADCAST_JOIN(t1,t2)", "BROADCAST_JOIN(`t1`, `t2`)"),
        ("INL_HASH_JOIN(t1,t2)", "INL_HASH_JOIN(`t1`, `t2`)"),
        ("INL_MERGE_JOIN(t1,t2)", "INL_MERGE_JOIN(`t1`, `t2`)"),
        ("INL_JOIN(t1,t2)", "INL_JOIN(`t1`, `t2`)"),
        ("HASH_JOIN(t1,t2)", "HASH_JOIN(`t1`, `t2`)"),
        ("HASH_JOIN_BUILD(t1)", "HASH_JOIN_BUILD(`t1`)"),
        ("HASH_JOIN_PROBE(t1)", "HASH_JOIN_PROBE(`t1`)"),
        ("LEADING(t1)", "LEADING(`t1`)"),
        ("LEADING(t1, c1)", "LEADING(`t1`, `c1`)"),
        ("LEADING((t1, c1), t2)", "LEADING((`t1`, `c1`), `t2`)"),
        ("LEADING(t1, (c1, t2))", "LEADING(`t1`, (`c1`, `t2`))"),
        (
            "LEADING(((t1, c1), t2), t3)",
            "LEADING(((`t1`, `c1`), `t2`), `t3`)",
        ),
        (
            "LEADING(t1, (c1, (t2, t3)))",
            "LEADING(`t1`, (`c1`, (`t2`, `t3`)))",
        ),
        ("LEADING(t1, c1, t2)", "LEADING(`t1`, `c1`, `t2`)"),
        ("LEADING(@sel1 t1, c1)", "LEADING(@`sel1` `t1`, `c1`)"),
        ("LEADING(@sel1 t1)", "LEADING(@`sel1` `t1`)"),
        (
            "LEADING(@sel1 t1, c1, t2)",
            "LEADING(@`sel1` `t1`, `c1`, `t2`)",
        ),
        (
            "LEADING(@sel1 t1, (c1, t2))",
            "LEADING(@`sel1` `t1`, (`c1`, `t2`))",
        ),
        (
            "LEADING(@sel1 t1, (c1, t2), d3)",
            "LEADING(@`sel1` `t1`, (`c1`, `t2`), `d3`)",
        ),
        ("LEADING(t1@sel1)", "LEADING(`t1`@`sel1`)"),
        ("LEADING(t1@sel1, c1)", "LEADING(`t1`@`sel1`, `c1`)"),
        (
            "LEADING(t1@sel1, c1, t2)",
            "LEADING(`t1`@`sel1`, `c1`, `t2`)",
        ),
        (
            "LEADING((t1@sel1, c1), t2)",
            "LEADING((`t1`@`sel1`, `c1`), `t2`)",
        ),
        (
            "LEADING(t1@sel1, (c1, t2))",
            "LEADING(`t1`@`sel1`, (`c1`, `t2`))",
        ),
        (
            "LEADING(t1@sel1, c1, t2, d3)",
            "LEADING(`t1`@`sel1`, `c1`, `t2`, `d3`)",
        ),
        (
            "LEADING(t1@sel1, (c1, t2), d3)",
            "LEADING(`t1`@`sel1`, (`c1`, `t2`), `d3`)",
        ),
        ("MAX_EXECUTION_TIME(3000)", "MAX_EXECUTION_TIME(3000)"),
        (
            "MAX_EXECUTION_TIME(@sel1 3000)",
            "MAX_EXECUTION_TIME(@`sel1` 3000)",
        ),
        ("USE_INDEX_MERGE(t1 c1)", "USE_INDEX_MERGE(`t1` `c1`)"),
        (
            "USE_INDEX_MERGE(@sel1 t1 c1)",
            "USE_INDEX_MERGE(@`sel1` `t1` `c1`)",
        ),
        (
            "USE_INDEX_MERGE(t1@sel1 c1)",
            "USE_INDEX_MERGE(`t1`@`sel1` `c1`)",
        ),
        ("USE_TOJA(TRUE)", "USE_TOJA(TRUE)"),
        ("USE_TOJA(FALSE)", "USE_TOJA(FALSE)"),
        ("USE_TOJA(@sel1 TRUE)", "USE_TOJA(@`sel1` TRUE)"),
        ("USE_CASCADES(TRUE)", "USE_CASCADES(TRUE)"),
        ("USE_CASCADES(FALSE)", "USE_CASCADES(FALSE)"),
        ("USE_CASCADES(@sel1 TRUE)", "USE_CASCADES(@`sel1` TRUE)"),
        ("QUERY_TYPE(OLAP)", "QUERY_TYPE(OLAP)"),
        ("QUERY_TYPE(OLTP)", "QUERY_TYPE(OLTP)"),
        ("QUERY_TYPE(@sel1 OLTP)", "QUERY_TYPE(@`sel1` OLTP)"),
        ("NTH_PLAN(10)", "NTH_PLAN(10)"),
        ("NTH_PLAN(@sel1 30)", "NTH_PLAN(@`sel1` 30)"),
        ("MEMORY_QUOTA(1 GB)", "MEMORY_QUOTA(1024 MB)"),
        ("MEMORY_QUOTA(@sel1 1 GB)", "MEMORY_QUOTA(@`sel1` 1024 MB)"),
        ("HASH_AGG()", "HASH_AGG()"),
        ("HASH_AGG(@sel1)", "HASH_AGG(@`sel1`)"),
        ("STREAM_AGG()", "STREAM_AGG()"),
        ("STREAM_AGG(@sel1)", "STREAM_AGG(@`sel1`)"),
        ("AGG_TO_COP()", "AGG_TO_COP()"),
        ("AGG_TO_COP(@sel_1)", "AGG_TO_COP(@`sel_1`)"),
        ("LIMIT_TO_COP()", "LIMIT_TO_COP()"),
        ("MERGE()", "MERGE()"),
        ("STRAIGHT_JOIN()", "STRAIGHT_JOIN()"),
        ("NO_INDEX_MERGE()", "NO_INDEX_MERGE()"),
        ("NO_INDEX_MERGE(@sel1)", "NO_INDEX_MERGE(@`sel1`)"),
        ("READ_CONSISTENT_REPLICA()", "READ_CONSISTENT_REPLICA()"),
        (
            "READ_CONSISTENT_REPLICA(@sel1)",
            "READ_CONSISTENT_REPLICA(@`sel1`)",
        ),
        ("QB_NAME(sel1)", "QB_NAME(`sel1`)"),
        (
            "READ_FROM_STORAGE(@sel TIFLASH[t1, t2])",
            "READ_FROM_STORAGE(@`sel` TIFLASH[`t1`, `t2`])",
        ),
        (
            "READ_FROM_STORAGE(@sel TIFLASH[t1 partition(p0)])",
            "READ_FROM_STORAGE(@`sel` TIFLASH[`t1` PARTITION(`p0`)])",
        ),
        (
            "TIME_RANGE('2020-02-02 10:10:10','2020-02-02 11:10:10')",
            "TIME_RANGE('2020-02-02 10:10:10', '2020-02-02 11:10:10')",
        ),
        ("RESOURCE_GROUP(rg1)", "RESOURCE_GROUP(`rg1`)"),
        ("RESOURCE_GROUP(`default`)", "RESOURCE_GROUP(`default`)"),
    ];
    assert_eq!(rows.len(), 109, "source row count drifted");
    for (hint, expected_hint) in rows {
        let sql = format!("select /*+ {hint} */ * from t1 join t2");
        let expected = format!("SELECT /*+ {expected_hint}*/ * FROM `t1` JOIN `t2`");
        let actual = parse(&sql)
            .unwrap_or_else(|error| panic!("{hint}: {error:?}"))
            .restore();
        assert_eq!(actual, expected, "{hint}");
    }
}

#[test]
fn test_brie_secure_text() {
    for (sql, expected) in [
        (
            "restore database * from 'local:///tmp/br01' snapshot = 23333",
            "RESTORE DATABASE * FROM 'local:///tmp/br01' SNAPSHOT = 23333",
        ),
        (
            "backup database * to 's3://bucket/prefix?region=us-west-2'",
            "BACKUP DATABASE * TO 's3://bucket/prefix?region=us-west-2'",
        ),
        (
            "backup database * to 's3://bucket/prefix?access-key=abcdefghi&secret-access-key=123&force-path-style=true'",
            "BACKUP DATABASE * TO 's3://bucket/prefix?access-key=xxxxxx&force-path-style=true&secret-access-key=xxxxxx'",
        ),
        (
            "backup database * to 'gcs://bucket/prefix?access-key=irrelevant&credentials-file=/home/user/secrets.txt'",
            "BACKUP DATABASE * TO 'gcs://bucket/prefix?access-key=irrelevant&credentials-file=/home/user/secrets.txt'",
        ),
    ] {
        let statement = parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        let Stmt::Admin(admin) = statement else {
            panic!("{sql}: expected admin statement");
        };
        let tidb_ast::AdminStmt::Brie(brie) = admin.as_ref() else {
            panic!("{sql}: expected BRIE statement");
        };
        assert_eq!(brie.secure_text(), expected, "{sql}");
    }
}

#[test]
fn test_add_query_watch_stmt_restore() {
    for (sql, expected) in [
        (
            "QUERY WATCH ADD ACTION KILL SQL TEXT EXACT TO 'select * from test.t2'",
            "QUERY WATCH ADD ACTION = KILL SQL TEXT EXACT TO _UTF8MB4'select * from test.t2'",
        ),
        (
            "QUERY WATCH ADD RESOURCE GROUP rg1 SQL TEXT SIMILAR TO 'select * from test.t2'",
            "QUERY WATCH ADD RESOURCE GROUP `rg1` SQL TEXT SIMILAR TO _UTF8MB4'select * from test.t2'",
        ),
        (
            "QUERY WATCH ADD RESOURCE GROUP rg1 ACTION COOLDOWN PLAN DIGEST 'd08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57'",
            "QUERY WATCH ADD RESOURCE GROUP `rg1` ACTION = COOLDOWN PLAN DIGEST _UTF8MB4'd08bc323a934c39dc41948b0a073725be3398479b6fa4f6dd1db2a9b115f7f57'",
        ),
        (
            "QUERY WATCH ADD ACTION SWITCH_GROUP(rg1) SQL TEXT EXACT TO 'select * from test.t1'",
            "QUERY WATCH ADD ACTION = SWITCH_GROUP(`rg1`) SQL TEXT EXACT TO _UTF8MB4'select * from test.t1'",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn test_compact_table_stmt_restore() {
    for (sql, expected) in [
        (
            "alter table abc compact tiflash replica",
            "ALTER TABLE `abc` COMPACT TIFLASH REPLICA",
        ),
        ("alter table abc compact", "ALTER TABLE `abc` COMPACT"),
        (
            "alter table test.abc compact",
            "ALTER TABLE `test`.`abc` COMPACT",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

#[test]
fn test_plan_replayer_stmt_restore() {
    for (sql, expected) in [
        (
            "plan replayer dump with stats as of timestamp '2023-06-28 12:34:00' explain select * from t where a > 10",
            "PLAN REPLAYER DUMP WITH STATS AS OF TIMESTAMP _UTF8MB4'2023-06-28 12:34:00' EXPLAIN SELECT * FROM `t` WHERE `a`>10",
        ),
        (
            "plan replayer dump explain analyze select * from t where a > 10",
            "PLAN REPLAYER DUMP EXPLAIN ANALYZE SELECT * FROM `t` WHERE `a`>10",
        ),
        (
            "plan replayer dump with stats as of timestamp 12345 explain analyze select * from t where a > 10",
            "PLAN REPLAYER DUMP WITH STATS AS OF TIMESTAMP 12345 EXPLAIN ANALYZE SELECT * FROM `t` WHERE `a`>10",
        ),
        (
            "plan replayer dump explain analyze 'test'",
            "PLAN REPLAYER DUMP EXPLAIN ANALYZE 'test'",
        ),
        (
            "plan replayer dump with stats as of timestamp '12345' explain analyze 'test2'",
            "PLAN REPLAYER DUMP WITH STATS AS OF TIMESTAMP _UTF8MB4'12345' EXPLAIN ANALYZE 'test2'",
        ),
        (
            "plan replayer dump explain ('SELECT * FROM t1', 'SELECT * FROM t2')",
            "PLAN REPLAYER DUMP EXPLAIN ('SELECT * FROM t1', 'SELECT * FROM t2')",
        ),
        (
            "plan replayer dump explain analyze ('SELECT * FROM t1')",
            "PLAN REPLAYER DUMP EXPLAIN ANALYZE ('SELECT * FROM t1')",
        ),
        (
            "plan replayer capture '123' '123'",
            "PLAN REPLAYER CAPTURE '123' '123'",
        ),
        (
            "plan replayer capture remove '123' '123'",
            "PLAN REPLAYER CAPTURE REMOVE '123' '123'",
        ),
        (
            "plan replayer load '/tmp/sdfaalskdjf.zip'",
            "PLAN REPLAYER LOAD '/tmp/sdfaalskdjf.zip'",
        ),
    ] {
        assert_eq!(r(sql), expected, "{sql}");
    }
}

/// Top-level dispatch for the complete
/// `admin_query_parser.go::parseSlowQueryStmt` production.
#[test]
fn top_level_slow_query_uses_plan_replayer_container() {
    assert_eq!(r("slow query"), "PLAN REPLAYER DUMP EXPLAIN SLOW QUERY");
    assert_eq!(
        r("slow query where a=1 order by b limit 2"),
        "PLAN REPLAYER DUMP EXPLAIN SLOW QUERY WHERE `a`=1 ORDER BY `b` LIMIT 2"
    );
    assert_eq!(
        r("plan replayer dump explain slow query where a=1 order by b limit 2"),
        "PLAN REPLAYER DUMP EXPLAIN SLOW QUERY WHERE `a`=1 ORDER BY `b` LIMIT 2"
    );
}

#[test]
fn test_redact_url() {
    for (input, expected) in [
        ("", ""),
        (":", ":"),
        ("~/file", "~/file"),
        ("gs://bucket/file", "gs://bucket/file"),
        (
            "gs://bucket/file?access-key=123",
            "gs://bucket/file?access-key=123",
        ),
        (
            "gs://bucket/file?secret-access-key=123",
            "gs://bucket/file?secret-access-key=123",
        ),
        ("s3://bucket/file", "s3://bucket/file"),
        (
            "s3://bucket/file?other-key=123",
            "s3://bucket/file?other-key=123",
        ),
        (
            "s3://bucket/file?access-key=123",
            "s3://bucket/file?access-key=xxxxxx",
        ),
        (
            "s3://bucket/file?secret-access-key=123",
            "s3://bucket/file?secret-access-key=xxxxxx",
        ),
        (
            "ks3://bucket/file?access-key=123",
            "ks3://bucket/file?access-key=xxxxxx",
        ),
        (
            "ks3://bucket/file?secret-access-key=123",
            "ks3://bucket/file?secret-access-key=xxxxxx",
        ),
        (
            "oss://bucket/file?access-key=123",
            "oss://bucket/file?access-key=xxxxxx",
        ),
        (
            "oss://bucket/file?secret-access-key=123",
            "oss://bucket/file?secret-access-key=xxxxxx",
        ),
        (
            "s3://bucket/file?access_key=123",
            "s3://bucket/file?access_key=xxxxxx",
        ),
        (
            "s3://bucket/file?secret_access_key=123",
            "s3://bucket/file?secret_access_key=xxxxxx",
        ),
        (
            "azure://bucket/file?sas-token=123",
            "azure://bucket/file?sas-token=xxxxxx",
        ),
        (
            "azblob://container/file?sas-token=123",
            "azblob://container/file?sas-token=xxxxxx",
        ),
        (
            "azure://container/file?account-name=test&sas_token=123",
            "azure://container/file?account-name=test&sas_token=xxxxxx",
        ),
        (
            "azure://container/file?account-name=test&account-key=123",
            "azure://container/file?account-key=xxxxxx&account-name=test",
        ),
        (
            "azblob://container/file?encryption-key=123",
            "azblob://container/file?encryption-key=xxxxxx",
        ),
        (
            "azure://container/file?account_key=123&encryption_key=456",
            "azure://container/file?account_key=xxxxxx&encryption_key=xxxxxx",
        ),
    ] {
        assert_eq!(tidb_ast::redact_url(input), expected, "{input}");
    }
}

#[test]
fn test_set_pwd_stmt_secure_text() {
    use tidb_ast::{SetPasswordStmt, UserSpec};

    for (statement, expected) in [
        (
            SetPasswordStmt {
                user: None,
                password: "x".to_string(),
                retain_current_password: false,
            },
            "set password",
        ),
        (
            SetPasswordStmt {
                user: None,
                password: "x".to_string(),
                retain_current_password: true,
            },
            "set password RETAIN CURRENT PASSWORD",
        ),
        (
            SetPasswordStmt {
                user: Some(UserSpec {
                    current_user: false,
                    user: "u".to_string(),
                    host: "%".to_string(),
                }),
                password: "x".to_string(),
                retain_current_password: false,
            },
            "set password for user u@%",
        ),
        (
            SetPasswordStmt {
                user: Some(UserSpec {
                    current_user: false,
                    user: "u".to_string(),
                    host: "%".to_string(),
                }),
                password: "x".to_string(),
                retain_current_password: true,
            },
            "set password for user u@% RETAIN CURRENT PASSWORD",
        ),
    ] {
        assert_eq!(statement.secure_text(), expected);
    }
}
