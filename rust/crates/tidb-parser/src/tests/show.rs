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
    let tidb_ast::AdminStmt::ShowVariables {
        global,
        like,
        where_clause,
    } = admin.as_ref()
    else {
        panic!("expected ShowVariables");
    };
    assert!(*global);
    assert!(like.is_none());
    assert!(where_clause.is_some());

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
    assert!(parse("show count(*) warnings").is_err());
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

    for sql in [
        "show table status from",
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

    assert!(parse("show columns t").is_err());
    assert!(parse("show full columns from t").is_err());
    assert!(parse("show full fields from t").is_err());
    assert!(parse("show extended columns from t").is_err());
    assert!(parse("show fields from t from test").is_err());
}

#[test]
fn show_index_restore_and_scope() {
    assert_eq!(r("show index from t"), "SHOW INDEX IN `t`");
    assert_eq!(r("show keys from t"), "SHOW INDEX IN `t`");
    assert_eq!(r("show index in t"), "SHOW INDEX IN `t`");
    assert_eq!(r("show keys in t"), "SHOW INDEX IN `t`");
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
    assert!(parse("show keys from t from test").is_err());
}
