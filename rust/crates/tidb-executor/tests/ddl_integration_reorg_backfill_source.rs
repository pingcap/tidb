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

use tidb_executor::ddl::{self, CreateTableSettings};
use tidb_executor::{Catalog, StmtContext, TableEntry};

/// Focused shape/type slice of Go `TestPartialIndex`: unsupported conditions
/// are rejected with the dedicated 8200 errno, while the accepted type-family
/// combinations remain executable through both CREATE TABLE and ALTER TABLE.
#[test]
fn partial_index_condition_validation_matches_go() {
    let ctx = StmtContext::for_query();
    let assert_create = |sql: &str, allowed: bool| {
        let mut catalog = Catalog::default();
        let result = ddl::run_create_table_in(
            sql,
            &mut catalog,
            "test",
            CreateTableSettings::default(),
            &ctx,
        );
        if allowed {
            result.unwrap_or_else(|error| panic!("{sql} should be accepted: {error:?}"));
        } else {
            let error = result.expect_err("Go rejects this partial-index condition");
            assert_eq!(error.clone().to_mysql_error().code, 8200, "{sql}: {error:?}");
        }
    };

    assert_create("create table t (a int, b int, key idx (b) where a = 1)", true);
    assert_create("create table t (a int, b int, key idx (b) where a = '1')", false);
    assert_create("create table t (a float, b int, key idx (b) where a = 1.0)", true);
    assert_create("create table t (a int, b int, key idx (b) where a = 1.0)", false);
    assert_create("create table t (a binary(8), b int, key idx (b) where a = 0x01)", true);
    assert_create("create table t (a varchar(8), b int, key idx (b) where a = 0x01)", false);
    assert_create("create table t (a text, b int, key idx (b) where a = '1')", true);
    assert_create(
        "create table t (a char(8) collate binary, b int, key idx (b) where a = 0x01)",
        true,
    );
    assert_create(
        "create table t (a char(8) collate binary, b int, key idx (b) where a = '1')",
        false,
    );
    assert_create(
        "create table t (a datetime, b int, key idx (b) where a = '2025-07-28')",
        true,
    );
    assert_create("create table t (a datetime, b int, key idx (b) where a = 1)", false);
    assert_create(
        "create table t (a enum('a','b'), b int, key idx (b) where a = 'a')",
        true,
    );
    assert_create("create table t (a enum('a','b'), b int, key idx (b) where a = null)", false);
    assert_create("create table t (a int, b int, key idx (b) where missing = 1)", false);
    assert_create("create table t (a int, b int, primary key (b) where a = 1)", false);
    assert_create("create table t (a int, b int, key idx (b) where a > b)", false);
    assert_create("create table t (a int, b int, key idx (b) where a like '1')", false);
    assert_create("create table t (a int, b int, key idx (b) where a is true)", false);
    assert_create(
        "create table t (a int, c int as (a + 1), b int, key idx (b) where c = 1)",
        false,
    );

    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int, b int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    ddl::run_alter_table_in(
        "alter table t add index idx (b) where a = 1",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    let error = ddl::run_alter_table_in(
        "alter table t add index bad (b) where a = '1'",
        &mut catalog,
        "test",
        &ctx,
    )
    .expect_err("Go rejects the ALTER partial-index type mismatch");
    assert_eq!(error.to_mysql_error().code, 8200);
}

/// GO PORT of `pkg/ddl/integration_test.go:209 TestMaintainAffectColumns`.
///
/// Re-derived contract: a partial index over `col2` records
/// `AffectColumn[0].Offset` = col2's position, and that offset is
/// maintained as `add column col1 int first` shifts it to 1,
/// `add column col3 int after col1` shifts it to 2, and
/// `drop column col1` brings it back to 1 — index metadata offsets track
/// column insertion/removal positions.
#[test]
fn maintain_affect_columns_tracks_offsets_across_column_changes() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    ddl::run_create_table_in(
        "create table t (col2 int, key idx_col2 (col2) where col2 > 0)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    let index_offset = |catalog: &Catalog| match catalog.table_in("test", "t") {
        Some(TableEntry::Kv(table)) => table.indexes()[0].column_offsets[0],
        _ => panic!("expected a storage-backed table"),
    };
    assert_eq!(index_offset(&catalog), 0);
    ddl::run_alter_table_in(
        "alter table t add column col1 int first",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    assert_eq!(index_offset(&catalog), 1);
    ddl::run_alter_table_in(
        "alter table t add column col3 int after col1",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    assert_eq!(index_offset(&catalog), 2);
    ddl::run_alter_table_in(
        "alter table t drop column col1",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    assert_eq!(index_offset(&catalog), 1);

    let error = ddl::run_alter_table_in(
        "alter table t drop column col2",
        &mut catalog,
        "test",
        &ctx,
    )
    .expect_err("Go protects columns referenced by a partial predicate");
    assert_eq!(error.to_mysql_error().code, 8272);
}
