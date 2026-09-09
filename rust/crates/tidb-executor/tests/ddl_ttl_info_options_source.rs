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

//! Port of Go `pkg/ddl/ttl_test.go:25::Test_getTTLInfoInOptions`, which feeds
//! five `[]*ast.TableOption` rows to the unexported `getTTLInfoInOptions`
//! (`pkg/ddl/ttl.go:176-212`) and asserts the `(ttlInfo, ttlEnable,
//! ttlCronJobSchedule, err)` it returns.
//!
//! This tier folds that function into the CREATE TABLE path:
//! `ttl_info_from_options` (`crates/tidb-executor/src/ddl.rs:488`, Go
//! `handleTableOptions`' TTL block included) builds the `TTLInfo` that
//! `run_create_table_on` stores on the table (`ddl.rs:1427`). The Go test's
//! `ttlInfo` return is therefore pinned here through the public carrier, with
//! the same five option rows; the two raw pointer returns (`ttlEnable`,
//! `ttlCronJobSchedule`) have no Rust counterpart — Go folds them into
//! `ttlInfo.Enable`/`ttlInfo.JobInterval` before anything observable happens
//! (`ttl.go:203-210`), and exactly that fold is what each row asserts.
//! Defaults are Go's: `TTL_ENABLE` defaults ON and the job interval to
//! `model.DefaultTTLJobInterval` = `"24h"` (`pkg/meta/model/table.go:1433`).

use tidb_executor::{run_create_table_on, Catalog, TableEntry};

/// Creates the Go test's option row over SQL on a `datetime` column (Go
/// builds the `ast.TableOption` values by hand against no table; a
/// time-typed column keeps the create accepted by both implementations) and
/// returns the `TTLInfo` the table settled with.
fn ttl_info_of(options_sql: &str) -> Option<tidb_model::TTLInfo> {
    let mut catalog = Catalog::default();
    run_create_table_on(
        &format!("create table ttl_t (test_column datetime) {options_sql}"),
        &mut catalog,
    )
    .expect("create table succeeds, like Go's err == nil rows");
    match catalog.table_in("test", "ttl_t") {
        Some(TableEntry::Kv(table)) => table.ttl_info().cloned(),
        _ => panic!("ttl_t should exist"),
    }
}

fn go_row(options_sql: &str) -> tidb_model::TTLInfo {
    ttl_info_of(options_sql).expect("a TTL= clause always builds a TTLInfo")
}

/// Go `ttl_test.go:32-38` (row 1): no options at all — `getTTLInfoInOptions`
/// returns a nil `TTLInfo` (and nil pointers) with no error.
#[test]
fn get_ttl_info_in_options_without_options_builds_nothing() {
    assert_eq!(ttl_info_of(""), None, "no TTL clause, no TTLInfo");
}

/// Go `ttl_test.go:39-52` (row 2): a bare `TTL test_column + INTERVAL 5 YEAR`
/// builds `TTLInfo{ColumnName: test_column, IntervalExprStr: "5",
/// IntervalTimeUnit: int(ast.TimeUnitYear), Enable: true, JobInterval:
/// DefaultTTLJobInterval}`. `ast.TimeUnitYear` is 9 (Go
/// `pkg/parser/ast/functions.go:1055` counts it from `TimeUnitInvalid = 0`,
/// one past MICROSECOND..QUARTER), the value
/// `tidb_model::time_unit_type_from_keyword("YEAR")` also answers. The
/// interval magnitude restores as `5`: the function restores with
/// `RestoreStringSingleQuotes | RestoreNameBackQuotes` only (`ttl.go:181`),
/// and a number carries no quotes under that flag set.
#[test]
fn get_ttl_info_in_options_ttl_clause_defaults_enable_and_job_interval() {
    let info = go_row("TTL `test_column` + INTERVAL 5 YEAR");
    assert_eq!(
        info.column_name.to_string(), "test_column",
        "the TTL column is Go's `ColumnName: ast.NewCIStr(\"test_column\")`"
    );
    assert_eq!(info.interval_expr_str, "5", "the restored magnitude");
    assert_eq!(
        info.interval_time_unit,
        tidb_model::time_unit_type_from_keyword("YEAR").unwrap(),
        "the parsed unit keyword"
    );
    // The Go literal `int(ast.TimeUnitYear)` this row pins, spelled out.
    assert_eq!(info.interval_time_unit, 9);
    assert!(info.enable, "Enable defaults true");
    assert_eq!(
        info.job_interval,
        tidb_model::DEFAULT_TTL_JOB_INTERVAL,
        "JobInterval defaults to DefaultTTLJobInterval"
    );
}

/// Go `ttl_test.go:53-68` (row 3): `TTL_ENABLE = 'OFF'` BEFORE the `TTL=`
/// clause still lands — the fold scans every option regardless of order, so
/// `TTLInfo.Enable` becomes false while the pointer return is `&false`.
#[test]
fn get_ttl_info_in_options_ttl_enable_off_overrides_the_default() {
    assert_eq!(
        go_row("TTL_ENABLE = 'OFF' TTL `test_column` + INTERVAL 5 YEAR").enable,
        false,
        "TTL_ENABLE=OFF before TTL= wins over the ON default"
    );
}

/// Go `ttl_test.go:69-85` (row 4): two `TTL_ENABLE` options — the LAST one
/// wins (`ttl.go:196-197` keeps overwriting the pointer; the fold applies the
/// final value), so `Enable` is true again and the pointer return is `&true`.
#[test]
fn get_ttl_info_in_options_last_ttl_enable_wins() {
    assert_eq!(
        go_row(
            "TTL_ENABLE = 'OFF' TTL `test_column` + INTERVAL 5 YEAR TTL_ENABLE = 'ON'"
        )
        .enable,
        true,
        "the later TTL_ENABLE=ON overrides the earlier OFF"
    );
}

/// Go `ttl_test.go:86-101` (row 5): `TTL_JOB_INTERVAL = '25h'` lands in
/// `TTLInfo.JobInterval` (and the `ttlCronJobSchedule` pointer return), again
/// independent of the order the options were written in.
#[test]
fn get_ttl_info_in_options_ttl_job_interval_overrides_the_default() {
    assert_eq!(
        go_row("TTL `test_column` + INTERVAL 5 YEAR TTL_JOB_INTERVAL = '25h'").job_interval,
        "25h",
        "TTL_JOB_INTERVAL replaces the 24h default"
    );
}
