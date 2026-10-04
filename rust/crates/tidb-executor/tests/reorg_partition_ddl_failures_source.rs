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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

use tidb_executor::{run_create_table_on, Catalog, DriverError};

/// The column set Go `serial_test.go:970` builds:
/// `i int, f float, c char(20), b bit(2), b32 bit(32), b64 bit(64), d date,
/// dt datetime, dt6 datetime(6), ts timestamp, ts6 timestamp(6), j json`.
const COLS: &str = "(i int, f float, c char(20), b bit(2), b32 bit(32), b64 bit(64), d date, \
dt datetime, dt6 datetime(6), ts timestamp, ts6 timestamp(6), j json)";

fn create_error(catalog: &mut Catalog, sql: &str) -> DriverError {
    run_create_table_on(sql, catalog)
        .map(|_| panic!("{sql} was expected to fail"))
        .expect_err("expected error")
}

/// Go `reorg_partition_test.go:981-1015`, the `create table tt <cols>
/// partition by <clause>` half of `TestPartitionByColumnChecks`: each clause
/// either builds or answers `dbterror.ErrNotAllowedTypeInPartition` (1659,
/// "Field '<col>' is of a not allowed type for this type of partitioning") or
/// `dbterror.ErrWrongExprInPartitionFunc` (1486, the timezone-dependent
/// expression message). The one divergence — Go accepts `list (c)` — is the
/// `#[ignore]` test below.
#[test]
fn partition_by_column_checks_create_half_matches_go_per_clause() {
    // (clause, expected): Err(None) = accepted, Err(1659, col) / Err(1486) =
    // Go's expected dbterror.
    let cases: Vec<(&str, Result<(), (u16, &str)>)> = vec![
        ("key (c) partitions 2", Ok(())),
        ("key (j) partitions 2", Err((1659, "j"))),
        // {"list (c) ...", nil} is the one diverging row; see the gap test.
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
    assert_eq!(cases.len(), 32, "Go's table minus the diverging list (c) row");

    for (clause, expected) in cases {
        // Go builds `tt` once and `tt`-copies per clause; the tier's
        // statement names must be unique per catalog, so each clause runs on
        // a fresh catalog with both tables.
        let mut catalog = Catalog::default();
        run_create_table_on(&format!("create table t {COLS}"), &mut catalog)
            .expect("the plain column set builds");
        let sql = format!("create table tt {COLS} partition by {clause}");
        match expected {
            Ok(()) => {
                run_create_table_on(&sql, &mut catalog)
                    .unwrap_or_else(|error| panic!("{clause} should build: {error:?}"));
            }
            Err((code, column)) => {
                let error = create_error(&mut catalog, &sql);
                let rendered = error.clone().to_mysql_error();
                assert_eq!(rendered.code, code, "{clause}");
                let expected_message = match code {
                    1659 => format!(
                        "Field '{column}' is of a not allowed type for this type of partitioning"
                    ),
                    _ => "Constant, random or timezone-dependent expressions in (sub)partitioning \
                          function are not allowed"
                        .to_owned(),
                };
                assert_eq!(rendered.message, expected_message, "{clause}");
            }
        }
    }
}
