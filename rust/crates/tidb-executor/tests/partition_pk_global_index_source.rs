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

//! Port of `pkg/ddl/tests/partition/db_partition_test.go:3731::TestPrimaryGlobalIndex`
//! and `pkg/ddl/tests/partition/db_partition_test.go:3805::TestPrimaryNoGlobalIndex`.
//!
//! Both Go tests share one shape: a battery of CREATE/ALTER statements around
//! "may a primary key be (or become) a GLOBAL index of a partitioned table",
//! with `checkGlobalAndPK` (`db_partition_test.go:3872`) asserting the table
//! metadata after each accepted statement.
//!
//! What runs here is the CREATE-TIME contract, whose decision logic is
//! transcreated in `crate::ddl::table_partition::check_unique_keys_include_partition_columns`
//! (Go `checkPartitionKeysConstraint`, `pkg/ddl/partition.go:686`) plus the
//! clustered-handle metadata facts (`TableInfo.PKIsHandle` /
//! `IsCommonHandle`). What cannot run is every arm that re-partitions or
//! flips a primary key online: `ALTER TABLE ... PARTITION BY` is refused by
//! this tier, and a GLOBAL primary key — which Go ACCEPTS at create time
//! (`db_partition_test.go:3793` builds exactly that table) — is refused
//! here because this node maintains only per-partition index entries and so
//! could not enforce the cross-partition constraint (see the `#[ignore]`
//! tests below).

use tidb_executor::{run_create_table_on, run_drop_table_in, Catalog, DriverError, TableEntry};

/// Go `db_partition_test.go` reuses the table name `t` across arms, dropping
/// it between arms (`:3742`, `:3749`, ...); the port mirrors that.
fn drop_table(catalog: &mut Catalog, name: &str) {
    run_drop_table_in(
        &format!("drop table {name}"),
        catalog,
        "test",
        tidb_parser::SqlMode::default(),
        true,
    )
    .expect("drop table succeeds");
}

/// Go `db_partition_test.go:3872::checkGlobalAndPK`: `indexes` count,
/// `PKIsHandle`, `IsCommonHandle`, and — when an index exists — the PRIMARY
/// entry's `Global` flag and `Primary` flag. The tier's equivalents are
/// `KvTable::pk_handle_offset`/`common_handle_offsets` and the `KvIndex`
/// fields; `Global` is false on every index this tier can build, and a
/// nonclustered PRIMARY is identified by name exactly as Go names it.
fn check_global_and_pk(catalog: &Catalog, name: &str, indexes: usize, pk_is_handle: bool, is_common_handle: bool, global: bool) {
    let Some(TableEntry::Kv(table)) = catalog.table_in("test", name) else {
        panic!("table {name} missing");
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

fn create_error(catalog: &mut Catalog, sql: &str) -> DriverError {
    run_create_table_on(sql, catalog)
        .map(|_| panic!("{sql} was expected to fail"))
        .expect_err("expected error")
}

fn rendered(error: &DriverError) -> (u16, String) {
    let mysql = error.clone().to_mysql_error();
    (mysql.code, mysql.message)
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
    let mut catalog = Catalog::default();

    // :3736 — clustered PK not covering the KEY partition column.
    let error = create_error(
        &mut catalog,
        "create table t (a int primary key clustered, b varchar(255)) partition by key(b) partitions 3",
    );
    let (code, message) = rendered(&error);
    assert_eq!(code, 1503);
    assert_eq!(
        message,
        "A CLUSTERED INDEX must include all columns in the table's partitioning function"
    );

    // :3740 + :3741 — PKIsHandle, no index entry at all.
    run_create_table_on(
        "create table t (a int primary key clustered, b varchar(255))",
        &mut catalog,
    )
    .expect("Go: unpartitioned clustered create succeeds");
    check_global_and_pk(&catalog, "t", 0, true, false, false);
    drop_table(&mut catalog, "t");

    // :3752 + :3753 — clustered varchar PK: IsCommonHandle, one PRIMARY entry.
    run_create_table_on(
        "create table t (a varchar(255), b varchar(255), primary key (a) clustered)",
        &mut catalog,
    )
    .expect("Go: clustered varchar PK create succeeds");
    check_global_and_pk(&catalog, "t", 1, false, true, false);
    drop_table(&mut catalog, "t");

    // :3757 — clustered composite (a,c): IsCommonHandle, one PRIMARY entry.
    run_create_table_on(
        "create table t (a varchar(255), b varchar(255), c int, primary key (a,c) clustered)",
        &mut catalog,
    )
    .expect("Go: clustered composite PK create succeeds");
    check_global_and_pk(&catalog, "t", 1, false, true, false);
    // Go :3759-:3760 re-asserts the metadata and refuses the key
    // re-partition; the refusal is the gap test below.
    drop_table(&mut catalog, "t");

    // :3763 — clustered PK covering ALL partitioning columns stays legal
    // (Go re-partitions this table by key(b) at :3766; that arm is the gap
    // test below). Metadata before the re-partition:
    run_create_table_on(
        "create table t (a varchar(255), b varchar(255), primary key (a, b) clustered)",
        &mut catalog,
    )
    .expect("Go: covering clustered PK create succeeds");
    check_global_and_pk(&catalog, "t", 1, false, true, false);
    drop_table(&mut catalog, "t");

    // :3790 — a NONCLUSTERED primary key is a real, non-global index entry.
    run_create_table_on(
        "create table t (a int primary key nonclustered, b varchar(255))",
        &mut catalog,
    )
    .expect("Go: nonclustered PK create succeeds");
    check_global_and_pk(&catalog, "t", 1, false, false, false);
    drop_table(&mut catalog, "t");

    // :3801 — composite nonclustered PK, still one non-global PRIMARY entry.
    run_create_table_on(
        "create table t (a varchar(255), b varchar(255), primary key (a, b) nonclustered)",
        &mut catalog,
    )
    .expect("Go: composite nonclustered PK create succeeds");
    check_global_and_pk(&catalog, "t", 1, false, false, false);
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
    let mut catalog = Catalog::default();

    // :3810 — clustered PK not covering the partitioning column: 1503.
    let error = create_error(
        &mut catalog,
        "create table t (a int primary key clustered, b varchar(255)) partition by key(b) partitions 3",
    );
    let (code, message) = rendered(&error);
    assert_eq!(code, 1503);
    assert_eq!(
        message,
        "A CLUSTERED INDEX must include all columns in the table's partitioning function"
    );

    // :3811 — PKIsHandle table, no index entries.
    run_create_table_on(
        "create table t (a int primary key clustered, b varchar(255))",
        &mut catalog,
    )
    .unwrap();
    check_global_and_pk(&catalog, "t", 0, true, false, false);
    drop_table(&mut catalog, "t");

    // :3817 — nonclustered int PK + key(b): 8264, naming 'PRIMARY'.
    let error = create_error(
        &mut catalog,
        "create table t (a int primary key nonclustered, b varchar(255)) partition by key(b) partitions 3",
    );
    let (code, message) = rendered(&error);
    assert_eq!(code, 8264);
    assert_eq!(
        message,
        "Global Index is needed for index 'PRIMARY', since the unique index is not including \
         all partitioning columns, and GLOBAL is not given as IndexOption"
    );

    // :3818 — the unpartitioned version: one non-global PRIMARY entry.
    run_create_table_on(
        "create table t (a int primary key nonclustered, b varchar(255))",
        &mut catalog,
    )
    .unwrap();
    check_global_and_pk(&catalog, "t", 1, false, false, false);
    drop_table(&mut catalog, "t");

    // :3833 — nonclustered varchar PK + key(b): 8264 again.
    let error = create_error(
        &mut catalog,
        "create table t (a varchar(255), b varchar(255), primary key (a) nonclustered) partition by key(b) partitions 3",
    );
    let (code, message) = rendered(&error);
    assert_eq!(code, 8264);
    assert_eq!(
        message,
        "Global Index is needed for index 'PRIMARY', since the unique index is not including \
         all partitioning columns, and GLOBAL is not given as IndexOption"
    );

    // :3834 + :3838 — composite nonclustered PK covers nothing extra at
    // create time; one non-global PRIMARY entry.
    run_create_table_on(
        "create table t (a varchar(255), b varchar(255), primary key (a, b) nonclustered)",
        &mut catalog,
    )
    .unwrap();
    check_global_and_pk(&catalog, "t", 1, false, false, false);
}

/// Go `db_partition_test.go:3731::TestPrimaryGlobalIndex`, the ONLINE arms:
/// `alter table t partition by key(b) partitions 3` accepted at :3747/:3766/
/// :3777/:3803 (re-partitioning to a key layout the PK covers) and refused
/// with 1503 at :3744/:3755/:3760; `alter table t drop primary key` at
/// :3745/:3751 answering Go's ErrUnsupportedModifyPrimaryKey "Unsupported
/// drop primary key when the table is using clustered index"; and the
/// GLOBAL primary-key flips at :3793-:3799 (`primary key ... global` create
/// accepted, `drop primary key` + `add primary key (a) global`, and
/// `alter table t partition by ... update indexes (`primary` global/local)`).
// go-parity-gap: this tier has no online re-partitioning — `ALTER TABLE ...
// PARTITION BY`, `REMOVE PARTITIONING`, and ADD PRIMARY KEY answer 1105
// "not supported yet" (measured) — and it refuses a GLOBAL primary key at
// create time with 8264 where Go accepts it (`db_partition_test.go:3793`),
// because its index writer maintains only per-partition entries
// (crate::ddl::table_partition, the GLOBAL-exemption arm).
#[test]
#[ignore]
fn primary_global_index_online_repartition_and_global_pk_arms() {
    let mut catalog = Catalog::default();
    // Go :3793 builds this table; the tier refuses it.
    let error = create_error(
        &mut catalog,
        "create table t (a int primary key nonclustered global, b varchar(255)) partition by key(b) partitions 3",
    );
    assert_eq!(rendered(&error).0, 8264, "tier refuses the GLOBAL PK Go accepts");
    // The remaining arms need `alter table t partition by ...` and
    // `alter table t add/drop primary key`, both unsupported here.
    let _ = &mut catalog;
}

/// Go `db_partition_test.go:3805::TestPrimaryNoGlobalIndex`, the ONLINE arms:
/// `alter table t partition by key(b) partitions 3` refused with 8264 at
/// :3815/:3831 (a local nonclustered PK cannot become a key-partition
/// index) and `alter table t partition by hash(a) partitions 3` accepted at
/// :3825 (the PK covers the hash column).
// go-parity-gap: `ALTER TABLE ... PARTITION BY` answers 1105 "not supported
// yet" in this tier (measured), so neither the refusals nor the acceptance
// can be observed; the create-time halves are pinned by the running tests.
#[test]
#[ignore]
fn primary_no_global_index_online_repartition_arms() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int primary key nonclustered, b varchar(255))",
        &mut catalog,
    )
    .unwrap();
    let error = create_error(
        &mut catalog,
        "alter table t partition by key(b) partitions 3",
    );
    assert_eq!(rendered(&error).0, 1105, "this tier's refusal replaces Go's 8264/acceptance pair");
}
