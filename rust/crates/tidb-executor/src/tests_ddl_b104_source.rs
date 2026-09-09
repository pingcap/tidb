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

//! Ports of the deterministic b104 window (Go declarations 241--300) from
//! `pkg/ddl` at master `25050b53f84fd14c4cfa97a7bb3826876c333c29`.
//!
//! This window is mostly the Go DDL coordinator's private test surface:
//! failpoint-controlled schema states, the DDL job/history tables, metadata
//! locks, domain schema versions, and the running-job scheduler. Those are
//! above this crate's synchronous catalog/DDL driver and are therefore kept as
//! explicit ignored mappings rather than replaced by weaker tests. The one
//! portable SQL contract, `TestDropTables`, runs against the same lifecycle
//! entry point that the executor exposes.

/// Go `pkg/ddl/db_table_test.go:876::TestDropTables` is the one synchronous
/// SQL contract in this window. Go drops existing names even when another
/// name is missing, and reports the missing names after the mutation.
#[test]
fn drop_tables() {
    let mut catalog = crate::Catalog::default();
    let drop = |sql: &str, catalog: &mut crate::Catalog| {
        crate::run_drop_table_in(sql, catalog, "test", tidb_parser::SqlMode::default(), true)
    };

    let error = drop("drop table t1", &mut catalog).expect_err("missing table errors");
    assert_eq!(error.to_mysql_error().code, 1051, "Go: errno.ErrBadTable");
    let error = drop("drop table test2.t1", &mut catalog).expect_err("missing table errors");
    assert_eq!(error.to_mysql_error().code, 1051, "Go: errno.ErrBadTable");

    crate::run_create_table_on("create table t1 (a int)", &mut catalog).unwrap();
    assert_eq!(
        drop("drop table if exists t1, t2", &mut catalog).unwrap(),
        vec!["test.t2"]
    );
    assert!(!catalog.contains_in("test", "t1"));

    crate::run_create_table_on("create table t1 (a int)", &mut catalog).unwrap();
    assert_eq!(
        drop("drop table if exists t2, t1", &mut catalog).unwrap(),
        vec!["test.t2"]
    );
    assert!(!catalog.contains_in("test", "t1"));

    crate::run_create_table_on("create table t1 (a int)", &mut catalog).unwrap();
    let error = drop("drop table t1, t2", &mut catalog).expect_err("missing table errors");
    assert_eq!(error.to_mysql_error().code, 1051, "Go: errno.ErrBadTable");
    assert!(!catalog.contains_in("test", "t1"));

    crate::run_create_table_on("create table t1 (a int)", &mut catalog).unwrap();
    let error = drop("drop table t2, t1", &mut catalog).expect_err("missing table errors");
    assert_eq!(error.to_mysql_error().code, 1051, "Go: errno.ErrBadTable");
    assert!(!catalog.contains_in("test", "t1"));
}
