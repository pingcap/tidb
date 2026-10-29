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

use crate::support::testkit::TestKit;
use tidb_infoschema::TableEntry;

/// Focused shape/type slice of Go `TestPartialIndex`: unsupported conditions
/// are rejected with the dedicated 8200 errno, while the accepted type-family
/// combinations remain executable through both CREATE TABLE and ALTER TABLE.
#[test]
fn partial_index_condition_validation_matches_go() {
    let mut tk = TestKit::new();
    // Go `checkColumnTypes` (pkg/ddl/integration_test.go:109): one kit, the
    // table dropped before each attempt.
    let mut assert_create = |sql: &str, allowed: bool| {
        tk.must_exec("drop table if exists t");
        if allowed {
            tk.must_exec(sql);
        } else {
            tk.must_get_err_code(sql, 8200);
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

    // Adding a partial index to an existing table needs Go's fast-reorg
    // (ingest) backfill, which a mock store does not initialize
    // (`InitGlobalLightningEnv` needs TiKV), so Go refuses it after the
    // condition checks pass (`index.go:1363`, captured with `gorun`); a
    // condition that fails them is refused first.
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (a int, b int)");
    let error = tk.must_get_err_code("alter table t add index idx (b) where a = 1", 8200);
    assert_eq!(
        error.error_string(),
        "[ddl:8200]Unsupported add partial index: add partial index without fast reorg is not supported"
    );
    let error = tk.must_get_err_code("alter table t add index bad (b) where a = '1'", 8200);
    assert!(
        error.error_string().contains("in partial index condition is not compatible"),
        "{}",
        error.error_string()
    );
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
    let mut tk = TestKit::new();
    tk.must_exec("create table t (col2 int, key idx_col2 (col2) where col2 > 0)");
    // Go `dom.InfoSchema().TableByName(...)`, re-read after every DDL.
    let index_offset = |tk: &TestKit| match tk.get_table_by_name("test", "t") {
        TableEntry::Kv(table) => table.indexes()[0].column_offsets[0],
        _ => panic!("expected a storage-backed table"),
    };
    assert_eq!(index_offset(&tk), 0);
    tk.must_exec("alter table t add column col1 int first");
    assert_eq!(index_offset(&tk), 1);
    tk.must_exec("alter table t add column col3 int after col1");
    assert_eq!(index_offset(&tk), 2);
    tk.must_exec("alter table t drop column col1");
    assert_eq!(index_offset(&tk), 1);

    // Go protects columns referenced by a partial predicate.
    tk.must_get_err_code("alter table t drop column col2", 8272);
}
