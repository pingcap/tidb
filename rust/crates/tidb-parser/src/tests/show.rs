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

//! Ordinary SHOW inspection parser/restore tests.

use super::*;

/// Direct `TestParseShowOpenTables` rows from `pkg/parser/parser_test.go`,
/// including the shared filter payload owned by Go's `ShowStmt`.
#[test]
fn show_open_tables_restores_complete_source_shape() {
    for (sql, expected) in [
        ("show open tables", "SHOW OPEN TABLES"),
        ("show open tables in test", "SHOW OPEN TABLES IN `test`"),
        ("show open tables from test", "SHOW OPEN TABLES IN `test`"),
        (
            "show open tables from test like 't%'",
            "SHOW OPEN TABLES IN `test` LIKE _UTF8MB4't%'",
        ),
        (
            "show open tables where In_use > 0",
            "SHOW OPEN TABLES WHERE `In_use`>0",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }

    let tidb_ast::Stmt::Admin(admin) =
        parse("show open tables from executor__show").expect("parse Go source form")
    else {
        panic!("expected administrative statement");
    };
    let tidb_ast::AdminStmt::ShowOpenTables(show) = admin.as_ref() else {
        panic!("expected typed SHOW OPEN TABLES statement");
    };
    assert_eq!(show.database.as_deref(), Some("executor__show"));
    assert!(show.filter.is_none());

    assert!(parse("show open").is_err());
}

#[test]
fn show_create() {
    assert_eq!(r("show create table t"), "SHOW CREATE TABLE `t`");
    assert_eq!(r("show create view v"), "SHOW CREATE VIEW `v`");
    assert_eq!(r("show create sequence s"), "SHOW CREATE SEQUENCE `s`");
    assert_eq!(r("show create database db"), "SHOW CREATE DATABASE `db`");
    assert_eq!(
        r("show create database if not exists db"),
        "SHOW CREATE DATABASE IF NOT EXISTS `db`"
    );
    assert_eq!(r("show create schema db"), "SHOW CREATE DATABASE `db`");
    assert_eq!(r("show create table db.t"), "SHOW CREATE TABLE `db`.`t`");
    assert_eq!(
        r("show create placement policy x"),
        "SHOW CREATE PLACEMENT POLICY `x`"
    );
    assert_eq!(
        r("show create resource group rg1"),
        "SHOW CREATE RESOURCE GROUP `rg1`"
    );
}

#[test]
fn show_variables() {
    assert_eq!(
        r("show variables like 'character_set_server'"),
        "SHOW SESSION VARIABLES LIKE _UTF8MB4'character_set_server'"
    );
    assert_eq!(r("show variables"), "SHOW SESSION VARIABLES");
    assert_eq!(r("show session variables"), "SHOW SESSION VARIABLES");
    assert_eq!(
        r("show global variables like 'x'"),
        "SHOW GLOBAL VARIABLES LIKE _UTF8MB4'x'"
    );
    // Exact Go `TestDBAStmt` row at `pkg/parser/parser_test.go:1270`.
    assert_eq!(
        r("show global variables where Variable_name = 'autocommit'"),
        "SHOW GLOBAL VARIABLES WHERE `Variable_name`=_UTF8MB4'autocommit'"
    );
    assert_eq!(
        r("show session variables where Variable_name like 'tidb_%' and Value != '0'"),
        "SHOW SESSION VARIABLES WHERE `Variable_name` LIKE _UTF8MB4'tidb_%' AND `Value`!=_UTF8MB4'0'"
    );

    let statement = parse("show global variables where Variable_name = 'autocommit'")
        .expect("SHOW GLOBAL VARIABLES with WHERE parses");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::ShowVariables(show) = admin.as_ref() else {
        panic!("expected ShowVariables");
    };
    assert!(show.global);
    assert!(show.like.is_none());
    assert!(show.where_clause.is_some());

    for sql in [
        "show variables where",
        "show global variables where",
        "show session variables where",
        "show variables like 'x' where Variable_name = 'x'",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}

/// Exact Go `TestDBAStmt` status rows at `pkg/parser/parser_test.go:1271-1276`.
#[test]
fn show_status_preserves_scope_and_filters() {
    assert_eq!(r("show status"), "SHOW SESSION STATUS");
    assert_eq!(r("show global status"), "SHOW GLOBAL STATUS");
    assert_eq!(r("show session status"), "SHOW SESSION STATUS");
    assert_eq!(
        r("show status like 'Up%'"),
        "SHOW SESSION STATUS LIKE _UTF8MB4'Up%'"
    );
    assert_eq!(
        r("show status where Variable_name"),
        "SHOW SESSION STATUS WHERE `Variable_name`"
    );
    assert_eq!(
        r("show status where Variable_name LIKE 'Up%'"),
        "SHOW SESSION STATUS WHERE `Variable_name` LIKE _UTF8MB4'Up%'"
    );

    let statement = parse("show global status where Variable_name = 'Threads_connected'")
        .expect("SHOW GLOBAL STATUS with WHERE parses");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::ShowStatus(show) = admin.as_ref() else {
        panic!("expected typed ShowStatus");
    };
    assert!(show.global);
    assert!(matches!(
        &show.filter,
        Some(tidb_ast::ShowStatusFilter::Where(_))
    ));

    for sql in [
        "show status like",
        "show status where",
        "show global status like",
        "show session status where",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}

#[test]
fn show_warnings_restore_and_scope() {
    assert_eq!(r("show warnings"), "SHOW WARNINGS");
    assert_eq!(r("show count(*) warnings"), "SHOW WARNINGS");
    assert_eq!(r("show count(*) errors"), "SHOW ERRORS");
    assert_eq!(
        r("show warnings like 'Warn%'"),
        "SHOW WARNINGS LIKE _UTF8MB4'Warn%'"
    );
    assert_eq!(
        r("show warnings where Level in ('Warning', 'Error')"),
        "SHOW WARNINGS WHERE `Level` IN (_UTF8MB4'Warning',_UTF8MB4'Error')"
    );
    assert_eq!(
        r("show warnings where Message not like '%disable dynamic pruning%'"),
        "SHOW WARNINGS WHERE `Message` NOT LIKE _UTF8MB4'%disable dynamic pruning%'"
    );

    assert_eq!(r("show errors"), "SHOW ERRORS");
    assert_eq!(
        r("show errors like 'Err%'"),
        "SHOW ERRORS LIKE _UTF8MB4'Err%'"
    );
    assert_eq!(
        r("show count(*) errors where Code = 1"),
        "SHOW ERRORS WHERE `Code`=1"
    );
    let errors = parse("show count(*) errors").expect("SHOW COUNT(*) ERRORS parses");
    let tidb_ast::Stmt::Admin(admin) = errors else {
        panic!("SHOW ERRORS must use Admin envelope")
    };
    assert!(matches!(
        admin.as_ref(),
        tidb_ast::AdminStmt::ShowErrors(show) if show.count_only
    ));
    let warnings = parse("show count(*) warnings").expect("SHOW COUNT(*) WARNINGS parses");
    let tidb_ast::Stmt::Admin(admin) = warnings else {
        panic!("SHOW WARNINGS must use Admin envelope")
    };
    assert!(matches!(
        admin.as_ref(),
        tidb_ast::AdminStmt::ShowWarnings(show) if show.count_only
    ));
}

#[test]
fn show_collation_preserves_its_filter_grammar() {
    assert_eq!(r("show collation"), "SHOW COLLATION");
    assert_eq!(
        r("show collation like 'utf8%'"),
        "SHOW COLLATION LIKE _UTF8MB4'utf8%'"
    );
    assert_eq!(
        r("show collation where Charset = 'utf8' and Collation = 'utf8_bin'"),
        "SHOW COLLATION WHERE `Charset`=_UTF8MB4'utf8' AND `Collation`=_UTF8MB4'utf8_bin'"
    );
}

#[test]
fn show_stats_histograms_preserves_shared_show_filters() {
    assert_eq!(r("show stats_histograms"), "SHOW STATS_HISTOGRAMS");
    assert_eq!(
        r("show stats_histograms like 'col%'"),
        "SHOW STATS_HISTOGRAMS LIKE _UTF8MB4'col%'"
    );
    assert_eq!(
        r("show stats_histograms where db_name = 'test' and column_name = 'a'"),
        "SHOW STATS_HISTOGRAMS WHERE `db_name`=_UTF8MB4'test' AND `column_name`=_UTF8MB4'a'"
    );
}

/// Exact Go `TestDBAStmt` TopN rows at `pkg/parser/parser_test.go:1361-1362`.
/// `WHERE` is part of the source grammar through `parseShowLikeOrWhere`.
#[test]
fn show_stats_topn_preserves_its_own_filter_payload() {
    assert_eq!(r("show stats_topn"), "SHOW STATS_TOPN");
    assert_eq!(
        r("show stats_topn where table_name = 't'"),
        "SHOW STATS_TOPN WHERE `table_name`=_UTF8MB4't'"
    );
    assert_eq!(
        r("show stats_topn like 'table_%'"),
        "SHOW STATS_TOPN LIKE _UTF8MB4'table_%'"
    );

    let statement = parse("show stats_topn where db_name = 'test' and is_index = 1")
        .expect("SHOW STATS_TOPN with WHERE parses");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::ShowStatsTopN(topn) = admin.as_ref() else {
        panic!("expected typed ShowStatsTopN");
    };
    assert!(matches!(
        &topn.filter,
        Some(tidb_ast::ShowStatsTopNFilter::Where(_))
    ));

    for sql in [
        "show stats_topn where",
        "show stats_topn like",
        "show stats_topn like 't%' where table_name = 't'",
    ] {
        assert!(parse(sql).is_err(), "outside this TopN leaf: {sql}");
    }
}

#[test]
fn show_databases_restore_and_scope() {
    assert_eq!(r("show databases"), "SHOW DATABASES");
    assert_eq!(
        r("show databases like 'test2'"),
        "SHOW DATABASES LIKE _UTF8MB4'test2'"
    );
    assert_eq!(
        r("show databases where Database like 'test_%'"),
        "SHOW DATABASES WHERE `Database` LIKE _UTF8MB4'test_%'"
    );

    let statement =
        parse("show databases where Database='mysql'").expect("SHOW DATABASES with WHERE parses");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::ShowDatabases(show) = admin.as_ref() else {
        panic!("expected typed ShowDatabases");
    };
    assert!(matches!(
        &show.filter,
        Some(tidb_ast::ShowDatabasesFilter::Where(_))
    ));
    assert!(parse("show database").is_err());
}

#[test]
fn show_tables_restore_and_scope() {
    assert_eq!(r("show tables"), "SHOW TABLES");
    assert_eq!(
        r("show tables like 'table%'"),
        "SHOW TABLES LIKE _UTF8MB4'table%'"
    );
    assert_eq!(r("show tables like T"), "SHOW TABLES LIKE `T`");

    assert_eq!(r("show full tables"), "SHOW FULL TABLES");
    assert_eq!(r("show tables from test"), "SHOW TABLES IN `test`");
    assert_eq!(
        r("show tables where tables_in_test = 't'"),
        "SHOW TABLES WHERE `tables_in_test`=_UTF8MB4't'"
    );
}

/// Exact duplicated `SHOW TABLE STATUS LIKE` rows from Go
/// `TestLockUnlockTables` at `pkg/parser/parser_test.go:5810,5820`, plus the
/// optional database and WHERE state owned by `parseShowTable`.
#[test]
fn show_table_status_preserves_its_distinct_go_payload() {
    for sql in ["show table status like 't'", "show table status like 't'"] {
        assert_eq!(r(sql), "SHOW TABLE STATUS LIKE _UTF8MB4't'");
    }
    assert_eq!(r("show table status"), "SHOW TABLE STATUS");
    assert_eq!(
        r("show table status from test"),
        "SHOW TABLE STATUS IN `test`"
    );
    assert_eq!(
        r("show table status in test where Name = 't'"),
        "SHOW TABLE STATUS IN `test` WHERE `Name`=_UTF8MB4't'"
    );

    let statement = parse("show table status from test like table_name")
        .expect("SHOW TABLE STATUS with database and LIKE parses");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::ShowTableStatus(show) = admin.as_ref() else {
        panic!("expected typed ShowTableStatus");
    };
    assert_eq!(show.database.as_deref(), Some("test"));
    assert!(matches!(
        &show.filter,
        Some(tidb_ast::ShowTableStatusFilter::Like(_))
    ));

    assert_eq!(r("show table status from"), "SHOW TABLE STATUS");
    for sql in [
        "show table status like",
        "show table status from test like",
        "show table status where",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}

/// Exact Go `TestDMLStmt` rows at `pkg/parser/parser_test.go:1141-1142`.
#[test]
fn show_table_next_row_id_is_not_admin_show_or_show_tables() {
    assert_eq!(
        r("show table t1.t1 next_row_id"),
        "SHOW TABLE `t1`.`t1` NEXT_ROW_ID"
    );
    assert_eq!(
        r("show table t1 next_row_id"),
        "SHOW TABLE `t1` NEXT_ROW_ID"
    );
    for sql in [
        "show table t1",
        "show table t1 partition (p0) next_row_id",
        "show table t1 next_row_id where a=1",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}

#[test]
fn show_columns_restore_and_scope() {
    assert_eq!(
        r("show columns from schema_name.table_name"),
        "SHOW COLUMNS IN `schema_name`.`table_name`"
    );
    assert_eq!(r("show fields from City"), "SHOW COLUMNS IN `City`");
    assert_eq!(r("show full columns in t"), "SHOW FULL COLUMNS IN `t`");
    assert_eq!(
        r("show extended full fields from City"),
        "SHOW EXTENDED FULL COLUMNS IN `City`"
    );
    assert_eq!(
        r("show columns from t from test"),
        "SHOW COLUMNS IN `t` IN `test`"
    );
    assert_eq!(
        r("show columns in t like id"),
        "SHOW COLUMNS IN `t` LIKE `id`"
    );
    assert_eq!(
        r("show fields in t like '%ime'"),
        "SHOW COLUMNS IN `t` LIKE _UTF8MB4'%ime'"
    );
    assert_eq!(
        r("show columns from t where field in (select 'b')"),
        "SHOW COLUMNS IN `t` WHERE `field` IN (SELECT _UTF8MB4'b')"
    );
    assert_eq!(
        r("show columns from t where field < all (select a from t)"),
        "SHOW COLUMNS IN `t` WHERE `field`<ALL (SELECT `a` FROM `t`)"
    );
    assert_eq!(
        r("show fields from t where field = 'abctime'"),
        "SHOW COLUMNS IN `t` WHERE `field`=_UTF8MB4'abctime'"
    );

    let stmt =
        parse("show columns from t where field = 'name'").expect("SHOW COLUMNS with WHERE parses");
    let tidb_ast::Stmt::Admin(admin) = stmt else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::ShowColumns(show) = admin.as_ref() else {
        panic!("expected ShowColumns");
    };
    assert_eq!(show.table, vec!["t".to_string()]);
    assert!(matches!(
        &show.filter,
        Some(tidb_ast::ShowColumnsFilter::Where(_))
    ));

    let stmt = parse("show columns from t from test")
        .expect("SHOW COLUMNS with a separate database parses");
    let tidb_ast::Stmt::Admin(admin) = stmt else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::ShowColumns(show) = admin.as_ref() else {
        panic!("expected ShowColumns");
    };
    assert_eq!(show.table, vec!["t".to_string()]);
    assert_eq!(show.database.as_deref(), Some("test"));

    assert!(parse("show columns t").is_err());
}

#[test]
fn show_index_restore_and_scope() {
    assert_eq!(r("show index from t"), "SHOW INDEX IN `t`");
    assert_eq!(r("show keys from t"), "SHOW INDEX IN `t`");
    assert_eq!(r("show index in t"), "SHOW INDEX IN `t`");
    assert_eq!(r("show keys in t"), "SHOW INDEX IN `t`");
    assert_eq!(
        r("show keys from t from test where true"),
        "SHOW INDEX IN `test`.`t` WHERE TRUE"
    );
    assert_eq!(
        r("show indexes in t where true"),
        "SHOW INDEX IN `t` WHERE TRUE"
    );
    assert_eq!(
        r("show index from performance_schema.events_statements_summary_by_digest"),
        "SHOW INDEX IN `performance_schema`.`events_statements_summary_by_digest`"
    );
    assert_eq!(
        r("show index from t like 'idx%'"),
        "SHOW INDEX IN `t` LIKE _UTF8MB4'idx%'"
    );

    let stmt =
        parse("show index from t where Key_name='PRIMARY'").expect("SHOW INDEX with WHERE parses");
    let tidb_ast::Stmt::Admin(admin) = stmt else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::ShowIndex(show) = admin.as_ref() else {
        panic!("expected ShowIndex");
    };
    assert_eq!(show.table, vec!["t".to_string()]);
    assert!(matches!(
        &show.filter,
        Some(tidb_ast::ShowIndexFilter::Where(_))
    ));

    assert!(parse("show index t").is_err());
}

#[test]
fn common_show_inspection_family_matches_go_source_rows() {
    for (sql, expected) in [
        ("show triggers like 't'", "SHOW TRIGGERS LIKE _UTF8MB4't'"),
        (
            "show procedure status where Db='test'",
            "SHOW PROCEDURE STATUS WHERE `Db`=_UTF8MB4'test'",
        ),
        (
            "show function status where Db='test'",
            "SHOW FUNCTION STATUS WHERE `Db`=_UTF8MB4'test'",
        ),
        (
            "show events from test_db where definer='current_user'",
            "SHOW EVENTS IN `test_db` WHERE `definer`=_UTF8MB4'current_user'",
        ),
        (
            "show plugins like 'Validate%'",
            "SHOW PLUGINS LIKE _UTF8MB4'Validate%'",
        ),
        (
            "show stats_extended where table_name='t'",
            "SHOW STATS_EXTENDED WHERE `table_name`=_UTF8MB4't'",
        ),
        (
            "show stats_meta where table_name='t'",
            "SHOW STATS_META WHERE `table_name`=_UTF8MB4't'",
        ),
        (
            "show stats_healthy where table_name='t'",
            "SHOW STATS_HEALTHY WHERE `table_name`=_UTF8MB4't'",
        ),
        ("show histograms_in_flight", "SHOW HISTOGRAMS_IN_FLIGHT"),
        (
            "show column_stats_usage where table_name='t'",
            "SHOW COLUMN_STATS_USAGE WHERE `table_name`=_UTF8MB4't'",
        ),
        ("show binding_cache status", "SHOW BINDING_CACHE STATUS"),
        (
            "show analyze status where table_name like '%'",
            "SHOW ANALYZE STATUS WHERE `table_name` LIKE _UTF8MB4'%'",
        ),
        (
            "show backups where start_time > now() - interval 10 hour",
            "SHOW BACKUPS WHERE `start_time`>DATE_SUB(NOW(), INTERVAL 10 HOUR)",
        ),
        (
            "show restores like 'r0001'",
            "SHOW RESTORES LIKE _UTF8MB4'r0001'",
        ),
        (
            "show config where type='tidb'",
            "SHOW CONFIG WHERE `type`=_UTF8MB4'tidb'",
        ),
        ("show replica status", "SHOW REPLICA STATUS"),
        ("show slave status", "SHOW REPLICA STATUS"),
        ("show binary log status", "SHOW BINARY LOG STATUS"),
        ("show profiles", "SHOW PROFILES"),
        ("show session_states", "SHOW SESSION_STATES"),
        ("show processlist", "SHOW PROCESSLIST"),
        ("show full processlist", "SHOW FULL PROCESSLIST"),
        ("show affinity", "SHOW AFFINITY"),
    ] {
        assert_eq!(r(sql), expected, "Go TestDMLStmt row: {sql}");
    }
}

#[test]
fn show_masking_policies_matches_go_rows() {
    assert_eq!(
        r("show masking policies for t"),
        "SHOW MASKING POLICIES FOR `t`"
    );
    assert_eq!(
        r("show masking policies for t where column_name = 'c'"),
        "SHOW MASKING POLICIES FOR `t` WHERE `column_name`=_UTF8MB4'c'"
    );
}

#[test]
fn table_region_distribution_and_job_show_rows_match_go() {
    for (sql, expected) in [
        ("show table t1 regions", "SHOW TABLE `t1` REGIONS"),
        (
            "show table t1 index idx1 regions where a=2",
            "SHOW TABLE `t1` INDEX `idx1` REGIONS WHERE `a`=2",
        ),
        (
            "show table t1 partition (p0,p1) regions",
            "SHOW TABLE `t1` PARTITION(`p0`, `p1`) REGIONS",
        ),
        (
            "show table t1 partition (p0,p1) index idx1 regions where a=2",
            "SHOW TABLE `t1` PARTITION(`p0`, `p1`) INDEX `idx1` REGIONS WHERE `a`=2",
        ),
        (
            "show table t1 distributions where a=1",
            "SHOW TABLE `t1` DISTRIBUTIONS WHERE `a`=1",
        ),
        (
            "show table t1 partition (p0,p1) distributions",
            "SHOW TABLE `t1` PARTITION(`p0`, `p1`) DISTRIBUTIONS",
        ),
        ("show distribution jobs", "SHOW DISTRIBUTION JOBS"),
        (
            "show distribution jobs where id > 0",
            "SHOW DISTRIBUTION JOBS WHERE `id`>0",
        ),
        ("show distribution job 1", "SHOW DISTRIBUTION JOB 1"),
    ] {
        assert_eq!(r(sql), expected, "Go TestDMLStmt row: {sql}");
    }
}

#[test]
fn placement_show_family_matches_go_rows() {
    for (sql, expected) in [
        ("show placement", "SHOW PLACEMENT"),
        (
            "show placement like 'POLICY foo%'",
            "SHOW PLACEMENT LIKE _UTF8MB4'POLICY foo%'",
        ),
        (
            "show placement where Target='TABLE test.t1'",
            "SHOW PLACEMENT WHERE `Target`=_UTF8MB4'TABLE test.t1'",
        ),
        (
            "show placement for schema db1",
            "SHOW PLACEMENT FOR DATABASE `db1`",
        ),
        (
            "show placement for table db1.tb1",
            "SHOW PLACEMENT FOR TABLE `db1`.`tb1`",
        ),
        (
            "show placement for table db1.tb1 partition p1",
            "SHOW PLACEMENT FOR TABLE `db1`.`tb1` PARTITION `p1`",
        ),
        ("show placement labels", "SHOW PLACEMENT LABELS"),
        (
            "show placement labels like '%zone%'",
            "SHOW PLACEMENT LABELS LIKE _UTF8MB4'%zone%'",
        ),
    ] {
        assert_eq!(r(sql), expected, "Go TestDMLStmt row: {sql}");
    }
    for sql in [
        "show placement for",
        "show placement database db1",
        "show placement for db db1",
        "show placement for database db1 table tb1",
        "show placement for partition p1",
        "show placement for database db1 like '%'",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}

#[test]
fn show_profile_payload_matches_go_rows() {
    for (sql, expected) in [
        ("show profile", "SHOW PROFILE"),
        ("show profile for query 1", "SHOW PROFILE FOR QUERY 1"),
        (
            "show profile cpu for query 2 limit 1,1",
            "SHOW PROFILE CPU FOR QUERY 2 LIMIT 1,1",
        ),
        (
            "show profile cpu, memory, block io, context switches, page faults, ipc, swaps, source for query 1 limit 100",
            "SHOW PROFILE CPU, MEMORY, BLOCK IO, CONTEXT SWITCHES, PAGE FAULTS, IPC, SWAPS, SOURCE FOR QUERY 1 LIMIT 100",
        ),
    ] {
        assert_eq!(r(sql), expected, "Go TestDMLStmt row: {sql}");
    }
}

#[test]
fn show_builtins_restores_the_source_row() {
    assert_eq!(r("show builtins"), "SHOW BUILTINS");

    let statement = parse("show builtins").expect("parse SHOW BUILTINS");
    let Stmt::Admin(admin) = statement else {
        panic!("expected administrative statement");
    };
    assert!(matches!(admin.as_ref(), tidb_ast::AdminStmt::ShowBuiltins));
}

#[test]
fn show_builtins_has_no_filter_or_trailing_payload() {
    for sql in ["show builtins like 'x'", "show builtins where 1 = 1"] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}

#[test]
fn show_full_tables_restores_full_scope_and_filter() {
    for (sql, expected) in [
        (
            "show full tables like '%lmn'",
            "SHOW FULL TABLES LIKE _UTF8MB4'%lmn'",
        ),
        (
            "show full tables from demo like 't%'",
            "SHOW FULL TABLES IN `demo` LIKE _UTF8MB4't%'",
        ),
        (
            "show tables where Table_type = 'BASE TABLE'",
            "SHOW TABLES WHERE `Table_type`=_UTF8MB4'BASE TABLE'",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

#[test]
fn show_tables_keeps_full_and_filter_typed() {
    let statement = parse("show full tables from demo where Table_type = 'BASE TABLE'")
        .expect("parse full SHOW TABLES");
    let Stmt::Admin(admin) = statement else {
        panic!("expected administrative statement");
    };
    let tidb_ast::AdminStmt::ShowTables(show) = admin.as_ref() else {
        panic!("expected typed SHOW TABLES");
    };
    assert!(show.full);
    assert_eq!(show.database.as_deref(), Some("demo"));
    assert!(matches!(
        show.filter,
        Some(tidb_ast::ShowTablesFilter::Where(_))
    ));
}

#[test]
fn show_tables_rejects_incomplete_filters() {
    for sql in [
        "show full",
        "show full tables like",
        "show full tables where",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
    assert_eq!(r("show tables from"), "SHOW TABLES");
}

/// Exact `TestDBAStmt` rows at `pkg/parser/parser_test.go:1314-1315`.
#[test]
fn show_character_set_restores_go_aliases() {
    for sql in ["show character set", "show char set", "show charset"] {
        assert_eq!(r(sql), "SHOW CHARSET", "source spelling: {sql}");
    }
}

#[test]
fn show_character_set_preserves_filter_payload() {
    assert_eq!(
        r("show character set like '%utf8mb4%'"),
        "SHOW CHARSET LIKE _UTF8MB4'%utf8mb4%'"
    );
    assert_eq!(
        r("show charset where Charset = 'utf8'"),
        "SHOW CHARSET WHERE `Charset`=_UTF8MB4'utf8'"
    );

    let statement = parse("show character set where Charset = 'utf8'")
        .expect("SHOW CHARACTER SET with WHERE parses");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::ShowCharset(show) = admin.as_ref() else {
        panic!("expected typed SHOW CHARSET");
    };
    assert!(matches!(
        &show.filter,
        Some(tidb_ast::ShowCharsetFilter::Where(_))
    ));

    for sql in [
        "show character set like",
        "show character set where",
        "show charset like 'x%' where Charset = 'utf8'",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}

/// The differential parser corpus's source row is the executor `TestShow`
/// statement at `tests/integrationtest/t/executor/executor.test:1660`.
#[test]
fn show_engines_restores_go_source_row() {
    assert_eq!(r("show engines"), "SHOW ENGINES");
}

#[test]
fn show_engines_preserves_the_shared_filter_payload() {
    assert_eq!(
        r("show engines like 'innodb%'"),
        "SHOW ENGINES LIKE _UTF8MB4'innodb%'"
    );
    assert_eq!(
        r("show engines where Engine = 'InnoDB'"),
        "SHOW ENGINES WHERE `Engine`=_UTF8MB4'InnoDB'"
    );

    let statement = parse("show engines where Engine = 'InnoDB'").expect("parse SHOW ENGINES");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::ShowEngines(show) = admin.as_ref() else {
        panic!("expected typed SHOW ENGINES");
    };
    assert!(matches!(
        &show.filter,
        Some(tidb_ast::ShowEnginesFilter::Where(_))
    ));

    for sql in [
        "show engines like",
        "show engines where",
        "show engines like 'x%' where Engine = 'InnoDB'",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}

#[test]
fn show_master_status_and_privileges_restore_source_rows() {
    assert_eq!(r("show master status"), "SHOW MASTER STATUS");
    assert_eq!(r("show privileges"), "SHOW PRIVILEGES");
}

#[test]
fn show_master_status_and_privileges_have_distinct_typed_leaves() {
    let master = parse("show master status").expect("SHOW MASTER STATUS parses");
    let Stmt::Admin(master) = master else {
        panic!("SHOW MASTER STATUS must use Admin envelope");
    };
    assert!(matches!(
        master.as_ref(),
        tidb_ast::AdminStmt::ShowMasterStatus
    ));

    let privileges = parse("show privileges").expect("SHOW PRIVILEGES parses");
    let Stmt::Admin(privileges) = privileges else {
        panic!("SHOW PRIVILEGES must use Admin envelope");
    };
    assert!(matches!(
        privileges.as_ref(),
        tidb_ast::AdminStmt::ShowPrivileges
    ));
}

#[test]
fn show_master_status_and_privileges_reject_trailing_or_missing_payload() {
    for sql in [
        "show master",
        "show master status like 'x'",
        "show privileges like 'x'",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}

/// Exact `TestDBAStmt` rows at `pkg/parser/parser_test.go:1355-1356`.
#[test]
fn show_stats_buckets_restores_original_go_rows() {
    assert_eq!(r("show stats_buckets"), "SHOW STATS_BUCKETS");
    assert_eq!(
        r("show stats_buckets where col_name = 'a'"),
        "SHOW STATS_BUCKETS WHERE `col_name`=_UTF8MB4'a'"
    );
}

#[test]
fn show_stats_buckets_preserves_its_own_filter_payload() {
    assert_eq!(
        r("show stats_buckets like 'col%'"),
        "SHOW STATS_BUCKETS LIKE _UTF8MB4'col%'"
    );
    let tidb_ast::Stmt::Admin(admin) =
        parse("show stats_buckets where db_name = 'test'").expect("parse")
    else {
        panic!("expected SHOW administrative envelope");
    };
    let tidb_ast::AdminStmt::ShowStatsBuckets(buckets) = admin.as_ref() else {
        panic!("expected typed SHOW STATS_BUCKETS");
    };
    assert!(matches!(
        &buckets.filter,
        Some(tidb_ast::ShowStatsBucketsFilter::Where(_))
    ));
    for sql in [
        "show stats_buckets like",
        "show stats_buckets where",
        "show stats_buckets like 'x%' where col_name = 'a'",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}

/// Exact `TestDBAStmt` rows at `pkg/parser/parser_test.go:1349-1350`.
#[test]
fn show_stats_locked_restores_original_go_rows() {
    assert_eq!(r("show stats_locked"), "SHOW STATS_LOCKED");
    assert_eq!(
        r("show stats_locked where table_name = 't'"),
        "SHOW STATS_LOCKED WHERE `table_name`=_UTF8MB4't'"
    );
}

/// Go's shared `parseShowLikeOrWhere` gives LIKE a simple expression and
/// WHERE a full expression, while retaining a distinct typed payload.
#[test]
fn show_stats_locked_preserves_shared_show_filters() {
    assert_eq!(
        r("show stats_locked like 'table_%'"),
        "SHOW STATS_LOCKED LIKE _UTF8MB4'table_%'"
    );
    assert_eq!(
        r("show stats_locked where table_name like '%'"),
        "SHOW STATS_LOCKED WHERE `table_name` LIKE _UTF8MB4'%'"
    );

    let tidb_ast::Stmt::Admin(admin) =
        parse("show stats_locked where table_name = 't'").expect("parse")
    else {
        panic!("expected SHOW administrative envelope");
    };
    let tidb_ast::AdminStmt::ShowStatsLocked(locked) = admin.as_ref() else {
        panic!("expected typed SHOW STATS_LOCKED");
    };
    assert!(matches!(
        &locked.filter,
        Some(tidb_ast::ShowStatsLockedFilter::Where(_))
    ));

    for sql in [
        "show stats_locked like",
        "show stats_locked where",
        "show stats_locked like 't%' where table_name = 't'",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}

#[test]
fn show_ident_based_dispatch_uses_go_decoded_literals() {
    for (sql, expected) in [
        ("show 'databases'", "SHOW DATABASES"),
        ("show @engines", "SHOW ENGINES"),
        ("show 'grants'", "SHOW GRANTS"),
        ("show 'bindings'", "SHOW SESSION BINDINGS"),
        ("show 'placement' 'labels'", "SHOW PLACEMENT LABELS"),
        ("show 'profile' cpu", "SHOW PROFILE CPU"),
        ("show 'open' tables", "SHOW OPEN TABLES"),
        ("show 'table' status", "SHOW TABLE STATUS"),
        (
            "show 'extended' columns from t",
            "SHOW EXTENDED COLUMNS IN `t`",
        ),
        ("show 'slave' status", "SHOW REPLICA STATUS"),
        ("show 'backup' logs status", "SHOW BACKUP LOGS STATUS"),
        ("show 'br' job 1", "SHOW BR JOB 1"),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
    assert!(parse("show profile cpu,").is_err());
    assert!(parse("show 'placement' for table t").is_err());
}

#[test]
fn show_create_uses_source_owned_name_boundaries() {
    for (sql, expected) in [
        ("show create table 't'", "SHOW CREATE TABLE `t`"),
        ("show create database 123", "SHOW CREATE DATABASE `123`"),
        ("show create database", "SHOW CREATE DATABASE ``"),
        (
            "show create 'placement' 'policy' @p",
            "SHOW CREATE PLACEMENT POLICY `p`",
        ),
        (
            "show create 'resource' 'group' 123",
            "SHOW CREATE RESOURCE GROUP `123`",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
    assert!(parse("show create table a.b.c").is_err());
}

#[test]
fn show_parser_preserves_shared_go_semantics() {
    for (sql, expected) in [
        ("show variables like 1", "SHOW SESSION VARIABLES LIKE 1"),
        ("show variables like @x", "SHOW SESSION VARIABLES LIKE @`x`"),
        ("show distribution job 1", "SHOW DISTRIBUTION JOB 1"),
        ("show distribution job -1", "SHOW DISTRIBUTION JOBS"),
        ("show distribution job 1+2", "SHOW DISTRIBUTION JOBS"),
        ("show tables from 123", "SHOW TABLES IN `123`"),
        (
            "show columns from t from 123",
            "SHOW COLUMNS IN `t` IN `123`",
        ),
        ("show index from t from 123", "SHOW INDEX IN `123`.`t`"),
        (
            "show table t index i distributions",
            "SHOW TABLE `t` DISTRIBUTIONS",
        ),
        ("show binary 'log' status", "SHOW BINARY LOG STATUS"),
        ("show count(*) 'errors'", "SHOW ERRORS"),
        ("show global 'bindings'", "SHOW GLOBAL BINDINGS"),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }

    for sql in [
        "show table a.b.c regions",
        "show table t partition ('p') regions",
        "show binary 'log' 'status'",
        "show 'extended' 'columns' from t",
        "show table t regions like 'x'",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}
