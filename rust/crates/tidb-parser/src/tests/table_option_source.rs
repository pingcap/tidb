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

//! Direct table-option rows from `pkg/parser/ddl_table_option_parser.go`.

use super::*;
use tidb_ast::{DdlStmt, Stmt};

#[test]
fn table_option_source_rows_use_the_physical_option_leaf() {
    // `pkg/parser/parser_test.go:TestDDL` exercises these distinct helper
    // paths: bare, string, normalized row-format, and TTL interval values.
    for (sql, expected) in [
        (
            "create table t (a int) engine=InnoDB comment='source'",
            "CREATE TABLE `t` (`a` INT) ENGINE = InnoDB COMMENT = 'source'",
        ),
        (
            "create table t (a int) row_format=dynamic",
            "CREATE TABLE `t` (`a` INT) ROW_FORMAT = DYNAMIC",
        ),
        (
            "create table t (a int) ttl_job_interval='1h'",
            "CREATE TABLE `t` (`a` INT) TTL_JOB_INTERVAL = '1h'",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }

    type TypedAssertion = (&'static str, fn(&[TableOption]) -> bool);
    let typed: [TypedAssertion; 7] = [
        (
            "create table a (id int primary key) autoextend_size=4M",
            |options: &[TableOption]| matches!(options, [TableOption::AutoextendSize(value)] if value == "4M"),
        ),
        (
            "create table a (id int primary key) page_checksum=1",
            |options: &[TableOption]| matches!(options, [TableOption::PageChecksum(value)] if value == "1"),
        ),
        (
            "create table b (id int primary key) page_compressed=1",
            |options: &[TableOption]| matches!(options, [TableOption::PageCompressed(value)] if value == "1"),
        ),
        (
            "create table c (id int primary key) page_compression_level=1",
            |options: &[TableOption]| matches!(options, [TableOption::PageCompressionLevel(value)] if value == "1"),
        ),
        (
            "create table d (id int primary key) transactional=0",
            |options: &[TableOption]| matches!(options, [TableOption::Transactional(value)] if value == "0"),
        ),
        (
            "create table e (id int primary key) ietf_quotes=YES",
            |options: &[TableOption]| matches!(options, [TableOption::IetfQuotes(value)] if value == "YES"),
        ),
        (
            "create table f (id int primary key) sequence=1",
            |options: &[TableOption]| matches!(options, [TableOption::Sequence(value)] if value == "1"),
        ),
    ];
    for (sql, is_expected) in typed {
        let Stmt::Ddl(ddl) = parse(sql).expect("parse typed compatibility option") else {
            panic!("not a DDL statement: {sql}");
        };
        let DdlStmt::CreateTable(table) = ddl.into_inner() else {
            panic!("not a CREATE TABLE statement: {sql}");
        };
        assert!(is_expected(&table.table_options), "typed AST drift: {sql}");
    }
}

/// All CREATE rows from Go `TestTableAffinityOption`, including the accepted
/// arbitrary level. The parser owns only the literal boundary; TiDB's DDL
/// layer validates the known affinity levels later.
#[test]
fn table_option_affinity_source_rows_keep_go_literal_and_restore_contract() {
    for (sql, expected) in [
        (
            "create table t (a int) affinity = 'table'",
            "CREATE TABLE `t` (`a` INT) AFFINITY = 'table'",
        ),
        (
            "create table t (a int) affinity 'TABLE'",
            "CREATE TABLE `t` (`a` INT) AFFINITY = 'TABLE'",
        ),
        (
            "create table t (a int) affinity 'partition'",
            "CREATE TABLE `t` (`a` INT) AFFINITY = 'partition'",
        ),
        (
            "create table t (a int) affinity = ''",
            "CREATE TABLE `t` (`a` INT) AFFINITY = ''",
        ),
        (
            "create table t (a int) affinity 'none'",
            "CREATE TABLE `t` (`a` INT) AFFINITY = 'none'",
        ),
        (
            "create table t (a int) affinity 'PARTITION' partition by hash (a) partitions 1",
            "CREATE TABLE `t` (`a` INT) AFFINITY = 'PARTITION' PARTITION BY HASH (`a`) PARTITIONS 1",
        ),
        (
            "create table t (a int) /*T![affinity] affinity = 'table' */",
            "CREATE TABLE `t` (`a` INT) AFFINITY = 'table'",
        ),
        (
            "create table t (a int) affinity 'abcd'",
            "CREATE TABLE `t` (`a` INT) AFFINITY = 'abcd'",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }

    for sql in [
        "create table t (a int) affinity 1",
        "create table t (a int) affinity = 1",
        "create table t (a int) affinity",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}

/// MariaDB compatibility options are accepted by TiDB's shared
/// `parseTableOption` production, warned as ignored by storage engines, and
/// restored as bare values. Keep every source fixture in one direct parser
/// test so a future table-option refactor cannot silently drop an option
/// family that the Go parser still accepts.
#[test]
fn table_option_mariadb_compatibility_source_rows_restore_bare_values() {
    for (sql, expected) in [
        (
            "create table a (id int primary key) autoextend_size=4M",
            "CREATE TABLE `a` (`id` INT PRIMARY KEY) AUTOEXTEND_SIZE = 4M",
        ),
        (
            "create table a (id int primary key) page_checksum=1",
            "CREATE TABLE `a` (`id` INT PRIMARY KEY) PAGE_CHECKSUM = 1",
        ),
        (
            "create table b (id int primary key) page_compressed=1",
            "CREATE TABLE `b` (`id` INT PRIMARY KEY) PAGE_COMPRESSED = 1",
        ),
        (
            "create table c (id int primary key) page_compression_level=1",
            "CREATE TABLE `c` (`id` INT PRIMARY KEY) PAGE_COMPRESSION_LEVEL = 1",
        ),
        (
            "create table d (id int primary key) transactional=0",
            "CREATE TABLE `d` (`id` INT PRIMARY KEY) TRANSACTIONAL = 0",
        ),
        (
            "create table e (id int primary key) ietf_quotes=YES",
            "CREATE TABLE `e` (`id` INT PRIMARY KEY) IETF_QUOTES = YES",
        ),
        (
            "create table f (id int primary key) sequence=1",
            "CREATE TABLE `f` (`id` INT PRIMARY KEY) SEQUENCE = 1",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

/// `ENGINE = MERGE UNION = (...)` is parsed by the same shared Go table
/// option production. The option keeps every table name path and restores
/// the comma-tight list exactly as `ast.TableOption.Restore` does.
#[test]
fn table_option_merge_union_source_row_is_typed_and_restored() {
    let sql = "create table z (a int) engine = MERGE union = (x, y)";
    assert_eq!(
        r(sql),
        "CREATE TABLE `z` (`a` INT) ENGINE = MERGE UNION = (`x`,`y`)"
    );
    let Stmt::Ddl(ddl) = parse(sql).expect("parse MERGE UNION") else {
        panic!("not a DDL statement");
    };
    let DdlStmt::CreateTable(table) = ddl.into_inner() else {
        panic!("not a CREATE TABLE statement");
    };
    assert!(matches!(
        table.table_options.as_slice(),
        [
            TableOption::Engine(engine),
            TableOption::Union(tables),
        ] if engine == "MERGE"
            && tables == &vec![vec!["x".to_string()], vec!["y".to_string()]]
    ));
}
