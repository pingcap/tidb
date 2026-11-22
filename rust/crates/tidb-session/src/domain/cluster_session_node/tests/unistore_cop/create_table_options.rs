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


//! `CREATE TABLE` options and refusals through Go's DDL job, as the executor's
//! session-less DDL used to check them (X11): TTL options, DEFAULT / ON UPDATE
//! validity, partitioning column types, SHARD_ROW_ID_BITS, and table options.

use super::sql_kit::*;
use super::*;

fn show_create(tk: &mut Session, table: &str) -> String {
    q(tk, &format!("SHOW CREATE TABLE {table}"))[0][1].clone()
}

#[test]
fn get_ttl_info_in_options_without_options_builds_nothing() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 860);
    rows(&mut tk, "create table ttl_t (test_column datetime)");
    assert!(!show_create(&mut tk, "ttl_t").contains("TTL"));
}

#[test]
fn get_ttl_info_in_options_ttl_clause_defaults_enable_and_job_interval() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 861);
    rows(&mut tk, "create table ttl_t (test_column datetime) TTL `test_column` + INTERVAL 5 YEAR");
    let text = show_create(&mut tk, "ttl_t");
    assert!(text.contains("TTL=`test_column` + INTERVAL 5 YEAR") && text.contains("TTL_ENABLE='ON'") && text.contains("TTL_JOB_INTERVAL='24h'"), "{text}");
}

#[test]
fn get_ttl_info_in_options_ttl_enable_off_overrides_the_default() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 862);
    rows(&mut tk, "create table ttl_t (test_column datetime) TTL_ENABLE = 'OFF' TTL `test_column` + INTERVAL 5 YEAR");
    assert!(show_create(&mut tk, "ttl_t").contains("TTL_ENABLE='OFF'"));
}

#[test]
fn get_ttl_info_in_options_last_ttl_enable_wins() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 863);
    rows(&mut tk, "create table ttl_t (test_column datetime) TTL_ENABLE = 'OFF' TTL `test_column` + INTERVAL 5 YEAR TTL_ENABLE = 'ON'");
    assert!(show_create(&mut tk, "ttl_t").contains("TTL_ENABLE='ON'"));
}

#[test]
fn get_ttl_info_in_options_ttl_job_interval_overrides_the_default() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 864);
    rows(&mut tk, "create table ttl_t (test_column datetime) TTL `test_column` + INTERVAL 5 YEAR TTL_JOB_INTERVAL = '25h'");
    assert!(show_create(&mut tk, "ttl_t").contains("TTL_JOB_INTERVAL='25h'"));
}

/// Go `checkColumnDefaultValue` / `checkColumnOnUpdate`.
#[test]
fn defaults_and_on_update_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 865);
    assert_eq!(error(&mut tk, "create table t (a int default 'abc')").message, "Invalid default value for 'a'");
    assert_eq!(error(&mut tk, "create table t (a int on update current_timestamp)").message, "Invalid ON UPDATE clause for 'a' column");
    error(&mut tk, "create table t (a int default (now()))");
    exec_all(&mut tk, &[
        "create table t1 (a datetime default current_timestamp)",
        "create table t2 (a datetime default current_timestamp on update current_timestamp)",
        "create table t3 (a int default (1+2))",
    ]);
}

const COLS: &str = "(i int, f float, c char(20), b bit(2), b32 bit(32), b64 bit(64), d date, dt datetime, dt6 datetime(6), ts timestamp, ts6 timestamp(6), j json)";

/// Go `TestReorgPartitionFailures`' create half: which column types each
/// partitioning method admits (1659), and a timezone-dependent expression
/// (1486).
#[test]
fn partition_by_column_checks_create_half_matches_go_per_clause() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 866);
    rows(&mut tk, &format!("create table t {COLS}"));
    let cases: Vec<(&str, Result<(), (u16, &str)>)> = vec![
        ("key (c) partitions 2", Ok(())),
        ("key (j) partitions 2", Err((1659, "j"))),
        ("list (b) (partition pDef default)", Ok(())),
        ("list (f) (partition pDef default)", Err((1659, "f"))),
        ("list (j) (partition pDef default)", Err((1659, "j"))),
        ("list columns (b) (partition pDef default)", Err((1659, "b"))),
        ("list columns (f) (partition pDef default)", Err((1659, "f"))),
        ("list columns (ts) (partition pDef default)", Err((1659, "ts"))),
        ("list columns (j) (partition pDef default)", Err((1659, "j"))),
        ("hash (year(ts)) partitions 2", Err((1486, ""))),
        ("hash (ts) partitions 2", Err((1659, "ts"))),
        ("hash (ts6) partitions 2", Err((1659, "ts6"))),
        ("hash (d) partitions 2", Err((1659, "d"))),
        ("hash (f) partitions 2", Err((1659, "f"))),
        ("range (c) (partition pMax values less than (maxvalue))", Err((1659, "c"))),
        ("range (f) (partition pMax values less than (maxvalue))", Err((1659, "f"))),
        ("range (d) (partition pMax values less than (maxvalue))", Err((1659, "d"))),
        ("range (dt) (partition pMax values less than (maxvalue))", Err((1659, "dt"))),
        ("range (dt6) (partition pMax values less than (maxvalue))", Err((1659, "dt6"))),
        ("range (ts) (partition pMax values less than (maxvalue))", Err((1659, "ts"))),
        ("range (ts6) (partition pMax values less than (maxvalue))", Err((1659, "ts6"))),
        ("range (j) (partition pMax values less than (maxvalue))", Err((1659, "j"))),
        ("range columns (b) (partition pMax values less than (maxvalue))", Err((1659, "b"))),
        ("range columns (b64) (partition pMax values less than (maxvalue))", Err((1659, "b64"))),
        ("range columns (c) (partition pMax values less than (maxvalue))", Ok(())),
        ("range columns (f) (partition pMax values less than (maxvalue))", Err((1659, "f"))),
        ("range columns (d) (partition pMax values less than (maxvalue))", Ok(())),
        ("range columns (dt) (partition pMax values less than (maxvalue))", Ok(())),
        ("range columns (dt6) (partition pMax values less than (maxvalue))", Ok(())),
        ("range columns (ts) (partition pMax values less than (maxvalue))", Err((1659, "ts"))),
        ("range columns (ts6) (partition pMax values less than (maxvalue))", Err((1659, "ts6"))),
        ("range columns (j) (partition pMax values less than (maxvalue))", Err((1659, "j"))),
    ];
    for (clause, expected) in cases {
        let sql = format!("create table tt {COLS} partition by {clause}");
        match expected {
            Ok(()) => exec_all(&mut tk, &[&sql, "drop table tt"]),
            Err((code, column)) => {
                let refused = error(&mut tk, &sql);
                let message = if code == 1659 {
                    format!("Field '{column}' is of a not allowed type for this type of partitioning")
                } else {
                    "Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed".to_owned()
                };
                assert_eq!((refused.code, refused.message), (code, message), "{clause}");
            }
        }
    }
}

/// Go clamps SHARD_ROW_ID_BITS on a `_tidb_rowid` table and refuses it (8200)
/// on a clustered one.
#[test]
fn shard_row_id_bits_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 867);
    exec_all(&mut tk, &["create table t1 (a int) shard_row_id_bits = 20", "create table t2 (a int) shard_row_id_bits = 4"]);
    assert_eq!(error(&mut tk, "create table t3 (a int primary key) shard_row_id_bits = 4").message, "Unsupported shard_row_id_bits for table with primary key as row id");
}

/// Go `checkTableOptions`: known engines pass, unknown ones are refused, and
/// UNION / INSERT_METHOD are 8232 / 8233.
#[test]
fn table_options_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 868);
    rows(&mut tk, "create table t (a int) engine = innodb");
    assert_eq!(error(&mut tk, "create table t1 (a int) engine = fizzbuzz").message, "Unknown storage engine 'fizzbuzz'");
    let union = error(&mut tk, "create table t2 (a int) union = (t)");
    assert_eq!((union.code, union.message.as_str()), (8232, "CREATE/ALTER table with union option is not supported"));
    let insert_method = error(&mut tk, "create table t3 (a int) insert_method = first");
    assert_eq!((insert_method.code, insert_method.message.as_str()), (8233, "CREATE/ALTER table with insert method option is not supported"));
}
