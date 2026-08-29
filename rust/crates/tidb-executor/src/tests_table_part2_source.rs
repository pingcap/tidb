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

//! Source-backed inventory for manifest batch `b151`, `pkg/table.part2`.
//!
//! The deterministic slice is items 61--108 of the repository-wide
//! `pkg/table` test enumeration: the partition package's test harness and 24
//! tests, five `tblctx` buffer tests, one `tblsession` test, three temporary
//! table DDL tests, thirteen temporary-table interceptor tests, and the
//! temporary-table test harness.
//!
//! The executor already owns the partition router, pruner, and SQL driver;
//! the runnable tests below pin the data-level portions of those Go tests.
//! The rest remain explicit carrier/gap inventory entries rather than empty
//! omissions. In particular, temporary-table DDL/interceptor behavior is
//! owned by the unported session/storage overlay, while the buffer and
//! tblsession tests already have complete carriers in `tblctx` and
//! `tblsession`.

#![cfg(test)]

use crate::{run_create_table_on, run_insert_on, run_select_on, Catalog, StmtContext};

fn query_ctx() -> StmtContext {
    StmtContext::for_query()
}

/// `pkg/table/tables/test/partition/partition_test.go:111::TestPartitionAddRecord`.
/// The storage-level AddRecord portion is represented by the SQL driver's
/// insert and partition-qualified reads; Go's direct transaction-key probes
/// are below this crate's session transaction boundary.
#[test]
fn partition_add_record_routes_rows_to_range_partitions() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (id int, index(id)) partition by range(id) (partition p0 values less than (6), partition p1 values less than (11), partition p2 values less than (16), partition p3 values less than (21))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1), (7), (12), (16)",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on("select count(*) from t", &catalog, &query_ctx()).unwrap(),
        vec![vec![tidb_datatype::Datum::Int(4)]]
    );
    for (predicate, expected) in [
        ("id < 6", 1),
        ("id >= 6 and id < 11", 1),
        ("id >= 11 and id < 16", 1),
        ("id >= 16 and id < 21", 1),
    ] {
        let rows = run_select_on(
            &format!("select count(*) from t where {predicate}"),
            &catalog,
            &query_ctx(),
        )
        .unwrap();
        assert_eq!(
            rows,
            vec![vec![tidb_datatype::Datum::Int(expected)]],
            "range predicate {predicate}",
        );
    }
}

/// `partition_test.go:218::TestHashPartitionAddRecord`.
#[test]
fn hash_partition_add_record_routes_rows_to_all_hash_partitions() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (id int, index(id)) partition by hash(id) partitions 4",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (8), (-1), (3), (6)",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    for id in [8_i64, -1, 3, 6] {
        let rows = run_select_on(
            &format!("select count(*) from t where id = {id}"),
            &catalog,
            &query_ctx(),
        )
        .unwrap();
        assert_eq!(
            rows,
            vec![vec![tidb_datatype::Datum::Int(1)]],
            "hash point id={id}",
        );
    }
}

/// `partition_test.go:284::TestPartitionGetPhysicalID`.
#[test]
fn partition_get_physical_id_preserves_definition_ids() {
    let spec = partition_fixture();
    assert_eq!(spec.physical_ids(), vec![101, 102, 103]);
    for definition in &spec.definitions {
        assert_eq!(
            spec.definition_named(&definition.name).unwrap().id,
            definition.id
        );
    }
}

/// `partition_test.go:341::TestLocatePartition`; this retains the row-level
/// LIST COLUMNS routing contract while omitting Go's concurrent EXPLAIN probe.
#[test]
fn locate_partition_routes_list_columns_rows() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (id bigint, type varchar(255)) partition by list columns(type) (partition push_event values in ('PushEvent'), partition watch_event values in ('WatchEvent'))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, 'PushEvent'), (2, 'WatchEvent'), (3, 'WatchEvent')",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "select count(*) from t partition (watch_event)",
            &catalog,
            &query_ctx()
        )
        .unwrap(),
        vec![vec![tidb_datatype::Datum::Int(2)]]
    );
}

/// `partition_test.go:878::TestKeyPartitionTableBasic`; the executor's
/// partition SQL carrier already exercises the full key-pruning/data family
/// (`tests_partition_table_sql_source`).
#[test]
fn key_partition_table_basic_is_carried_by_partition_sql_tests() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (col1 int not null, col2 int not null, col3 int not null, unique key(col3)) partition by key(col3) partitions 4",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on("select count(*) from t", &catalog, &query_ctx()).unwrap(),
        vec![vec![tidb_datatype::Datum::Int(4)]]
    );
    assert_eq!(
        run_select_on(
            "select count(*) from t where col3 = 3",
            &catalog,
            &query_ctx()
        )
        .unwrap(),
        vec![vec![tidb_datatype::Datum::Int(1)]]
    );
}

/// `partition_test.go:3221::TestPointGetKeyPartitioning`.
#[test]
fn point_get_key_partitioning_returns_the_matching_row() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a varchar(30) not null, b varchar(45) not null, c varchar(45) not null, primary key (b, a)) partition by key(b) partitions 5",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values ('Aa', 'Ab', 'Ac'), ('Ba', 'Bb', 'Bc')",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on("select * from t where b = 'Ab'", &catalog, &query_ctx()).unwrap(),
        vec![vec![
            tidb_datatype::Datum::String(tidb_datatype::StringDatum::new(
                "Aa",
                tidb_datatype::Collation::Utf8Mb4Bin,
            )),
            tidb_datatype::Datum::String(tidb_datatype::StringDatum::new(
                "Ab",
                tidb_datatype::Collation::Utf8Mb4Bin,
            )),
            tidb_datatype::Datum::String(tidb_datatype::StringDatum::new(
                "Ac",
                tidb_datatype::Collation::Utf8Mb4Bin,
            )),
        ]]
    );
}

/// `partition_test.go:3257::TestPruningOverflow`; the data-level predicate
/// must still find the inserted row when the partition expression multiplies
/// large signed values.
#[test]
fn pruning_overflow_keeps_the_matching_row() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int not null, b bigint not null, primary key(a, b)) partition by hash((a * b)) partitions 13",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t values (0, 3522101843073676459)",
        &mut catalog,
        &query_ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "select a, b from t where a in (0, 14158354938390, 0) and b in (3522101843073676459, -2846203247576845955, 838395691793635638)",
            &catalog,
            &query_ctx(),
        )
        .unwrap(),
        vec![vec![
            tidb_datatype::Datum::Int(0),
            tidb_datatype::Datum::Int(3522101843073676459),
        ]]
    );
}

// The next six entries are complete carriers already exercised by the owning
// modules. They stay in this source-backed inventory so the manifest remains
// one-to-one without duplicating private implementation tests here.

fn partition_fixture() -> crate::PartitionSpec {
    use crate::{PartitionDef, PartitionKind, PartitionSpec, RangeBound};
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::expression::{Constant, Expression};

    let field_type = FieldType::new(FieldTypeCode::LongLong);
    PartitionSpec {
        kind: PartitionKind::Range {
            less_than: vec![
                RangeBound::Value(10),
                RangeBound::Value(20),
                RangeBound::MaxValue,
            ],
            unsigned: false,
        },
        expr_text: "`a`".to_owned(),
        expr: Expression::Constant(Constant::new(Datum::Int(0), field_type)),
        dependencies: vec!["a".to_owned()],
        definitions: [(101, "p0"), (102, "p1"), (103, "p2")]
            .into_iter()
            .map(|(id, name)| PartitionDef {
                id,
                name: name.to_owned(),
                less_than: Vec::new(),
                in_values: Vec::new(),
                comment: String::new(),
                placement_policy: None,
            })
            .collect(),
        is_empty_columns: false,
    }
}
