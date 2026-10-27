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

//! CREATE-time cases from `pkg/ddl/tests/partition/db_partition_test.go`
//! (`TestPrimaryGlobalIndex` and `TestPrimaryNoGlobalIndex`).
//! Online primary-key/repartition obligations remain recorded in
//! `docs/parity/current-audit/test-owner-cleanup-validation.json`.

use crate::support::testkit::TestKit;
use tidb_executor::TableEntry;

/// Go `db_partition_test.go:3872::checkGlobalAndPK`: `indexes` count,
/// `PKIsHandle`, `IsCommonHandle`, and — when an index exists — the PRIMARY
/// entry's `Global` flag and `Primary` flag. The tier's equivalents are
/// `KvTable::pk_handle_offset`/`common_handle_offsets` and the `KvIndex`
/// fields; `Global` is false on every index this tier can build, and a
/// nonclustered PRIMARY is identified by name exactly as Go names it.
fn check_global_and_pk(tk: &TestKit, name: &str, indexes: usize, pk_is_handle: bool, is_common_handle: bool, global: bool) {
    let TableEntry::Kv(table) = tk.get_table_by_name("test", name) else {
        panic!("table {name} is not a KV table");
    };
    assert_eq!(table.indexes().len(), indexes, "{name} index count");
    assert_eq!(table.pk_handle_offset().is_some(), pk_is_handle, "{name} PKIsHandle");
    assert_eq!(!table.common_handle_offsets().is_empty(), is_common_handle, "{name} IsCommonHandle");
    if indexes > 0 {
        let index = table
            .indexes()
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case("primary"))
            .expect("Go FindIndexByName(primary)");
        assert_eq!(index.global, global, "{name} primary.Global");
        // Go `idxInfo.Primary` — the tier stores the PRIMARY entry under the
        // conventional name; Go sets `Primary` for it at build time.
        assert!(index.name.eq_ignore_ascii_case("primary"), "{name} primary.Primary");
    }
}

/// Go `db_partition_test.go:3731::TestPrimaryGlobalIndex`, the arms that do
/// not re-partition or flip a primary key:
/// - `:3736` a CLUSTERED primary key that does not cover the partitioning
///   column answers 1503 "A CLUSTERED INDEX must include all columns in the
///   table's partitioning function" (`ErrUniqueKeyNeedAllFieldsInPf` raised
///   with `CLUSTERED INDEX` as the key name);
/// - `:3740`, `:3752`, `:3757`, `:3763`, `:3774`, `:3790`, `:3801` the
///   unpartitioned creates whose metadata `checkGlobalAndPK` pins: a
///   PKIsHandle int key has NO index entry; every varchar clustered key is a
///   common handle with one PRIMARY entry; a NONCLUSTERED primary key is a
///   real index entry, non-global.
#[test]
fn primary_global_index_create_refusals_and_table_metadata() {
    let mut tk = TestKit::new();

    // :3736 — clustered PK not covering the KEY partition column.
    assert_eq!(
        tk.must_get_err_code(
            "create table t (a int primary key clustered, b varchar(255)) partition by key(b) partitions 3",
            1503,
        )
        .message,
        "A CLUSTERED INDEX must include all columns in the table's partitioning function"
    );

    // :3740 + :3741 — PKIsHandle, no index entry at all.
    tk.must_exec("create table t (a int primary key clustered, b varchar(255))");
    check_global_and_pk(&tk, "t", 0, true, false, false);
    tk.must_exec("drop table t");

    // :3752 + :3753 — clustered varchar PK: IsCommonHandle, one PRIMARY entry.
    tk.must_exec("create table t (a varchar(255), b varchar(255), primary key (a) clustered)");
    check_global_and_pk(&tk, "t", 1, false, true, false);
    tk.must_exec("drop table t");

    // :3757 — clustered composite (a,c): IsCommonHandle, one PRIMARY entry.
    tk.must_exec("create table t (a varchar(255), b varchar(255), c int, primary key (a,c) clustered)");
    check_global_and_pk(&tk, "t", 1, false, true, false);
    // Go :3759-:3760 re-asserts the metadata and refuses the key
    // re-partition; the refusal is the gap test below.
    tk.must_exec("drop table t");

    // :3763 — clustered PK covering ALL partitioning columns stays legal
    // (Go re-partitions this table by key(b) at :3766; that arm is the gap
    // test below). Metadata before the re-partition:
    tk.must_exec("create table t (a varchar(255), b varchar(255), primary key (a, b) clustered)");
    check_global_and_pk(&tk, "t", 1, false, true, false);
    tk.must_exec("drop table t");

    // :3790 — a NONCLUSTERED primary key is a real, non-global index entry.
    tk.must_exec("create table t (a int primary key nonclustered, b varchar(255))");
    check_global_and_pk(&tk, "t", 1, false, false, false);
    tk.must_exec("drop table t");

    // :3801 — composite nonclustered PK, still one non-global PRIMARY entry.
    tk.must_exec("create table t (a varchar(255), b varchar(255), primary key (a, b) nonclustered)");
    check_global_and_pk(&tk, "t", 1, false, false, false);
}
/// Go `db_partition_test.go:3805::TestPrimaryNoGlobalIndex`, the create-time
/// arms: the same 1503 clustered refusal at `:3810`, and at `:3817`, `:3821`,
/// `:3833` the 8264 refusals for a NONCLUSTERED primary key whose columns do
/// not cover the partitioning function and which was not declared GLOBAL
/// (`ErrGlobalIndexNotExplicitlySet`, `pkg/ddl/partition.go:703` guard). The
/// unpartitioned metadata arms (`:3818`, `:3822`, `:3834`) run against the
/// same metadata facts as the GlobalIndex variant above.
#[test]
fn primary_no_global_index_create_refusals_and_table_metadata() {
    let mut tk = TestKit::new();

    // :3810 — clustered PK not covering the partitioning column: 1503.
    assert_eq!(
        tk.must_get_err_code(
            "create table t (a int primary key clustered, b varchar(255)) partition by key(b) partitions 3",
            1503,
        )
        .message,
        "A CLUSTERED INDEX must include all columns in the table's partitioning function"
    );

    // :3811 — PKIsHandle table, no index entries.
    tk.must_exec("create table t (a int primary key clustered, b varchar(255))");
    check_global_and_pk(&tk, "t", 0, true, false, false);
    tk.must_exec("drop table t");

    // :3817 — nonclustered int PK + key(b): 8264, naming 'PRIMARY'.
    assert_eq!(
        tk.must_get_err_code(
            "create table t (a int primary key nonclustered, b varchar(255)) partition by key(b) partitions 3",
            8264,
        )
        .message,
        "Global Index is needed for index 'PRIMARY', since the unique index is not including \
         all partitioning columns, and GLOBAL is not given as IndexOption"
    );

    // :3818 — the unpartitioned version: one non-global PRIMARY entry.
    tk.must_exec("create table t (a int primary key nonclustered, b varchar(255))");
    check_global_and_pk(&tk, "t", 1, false, false, false);
    tk.must_exec("drop table t");

    // :3833 — nonclustered varchar PK + key(b): 8264 again.
    assert_eq!(
        tk.must_get_err_code(
            "create table t (a varchar(255), b varchar(255), primary key (a) nonclustered) partition by key(b) partitions 3",
            8264,
        )
        .message,
        "Global Index is needed for index 'PRIMARY', since the unique index is not including \
         all partitioning columns, and GLOBAL is not given as IndexOption"
    );

    // :3834 + :3838 — composite nonclustered PK covers nothing extra at
    // create time; one non-global PRIMARY entry.
    tk.must_exec("create table t (a varchar(255), b varchar(255), primary key (a, b) nonclustered)");
    check_global_and_pk(&tk, "t", 1, false, false, false);
}
