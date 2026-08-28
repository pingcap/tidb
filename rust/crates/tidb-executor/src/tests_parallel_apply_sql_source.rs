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

//! Data-level ports of Go `pkg/executor/parallel_apply_test.go`: the scalar /
//! EXISTS / IN correlated-subquery (apply) contracts those tests pin.
//!
//! SCOPE NOTE. Go's `parallel_apply_test.go` runs every query twice -- once
//! serial, once with `tidb_enable_parallel_apply=true` -- and additionally
//! asserts `explain analyze` plan text (`Concurrency:`/`cacheHitRatio:`
//! lines), failpoint-injected worker panics, SQL-killer cancellation, and
//! cancel-in-flight latency. This tier has ONE apply implementation -- the
//! sequential `crate::apply` operator driven through the statement driver --
//! and no `explain analyze` text, failpoint, or kill surface. The DATA
//! assertions (which Go proves identical across both modes) are ported here
//! as running tests with Go's exact fixtures and expectations; every
//! execution-mode assertion is recorded as an `#[ignore]` gap test in this
//! module or the sibling gap modules. Nothing was weakened into a pass.

use crate::{run_create_table_on, run_insert_on, run_select_on, Catalog, StmtContext};
use tidb_datatype::{Collation, Datum, StringDatum};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

/// testkit's `result.Sort()`: rows compared as an ordered multiset. The key
/// orders by kind (NULL first, Go's `KindNull` encoding), then numerically
/// for numeric kinds and by bytes for strings, so `6` sorts below `10` the
/// way Go's encoded-row comparison does.
fn datum_sort_key(datum: &Datum) -> (u8, String) {
    const NULL: u8 = 0;
    const NUMBER: u8 = 1;
    const TEXT: u8 = 2;
    match datum {
        Datum::Null => (NULL, String::new()),
        Datum::Int(value) => (NUMBER, format!("{value:024}")),
        Datum::UInt(value) => (NUMBER, format!("{value:024}")),
        Datum::Decimal(value) => (NUMBER, format!("{:024}", value)),
        Datum::Real(value) => (NUMBER, format!("{value:024}")),
        Datum::Float32(value) => (NUMBER, format!("{value:024}")),
        Datum::String(text) => (TEXT, String::from_utf8_lossy(text.bytes()).into_owned()),
        Datum::Bytes(bytes) => (TEXT, String::from_utf8_lossy(bytes).into_owned()),
        other => (TEXT, format!("{other:?}")),
    }
}

fn sorted(mut rows: Vec<Vec<Datum>>) -> Vec<Vec<Datum>> {
    rows.sort_by_key(|row| row.iter().map(datum_sort_key).collect::<Vec<_>>());
    rows
}

fn select_sorted(catalog: &Catalog, sql: &str) -> Vec<Vec<Datum>> {
    match run_select_on(sql, catalog, &ctx()) {
        Ok(rows) => sorted(rows),
        Err(error) => panic!("query {sql:?} failed: {error:?}"),
    }
}

fn text(value: &str) -> Datum {
    Datum::String(StringDatum::new(value, Collation::Utf8Mb4Bin))
}

fn int(value: i64) -> Datum {
    Datum::Int(value)
}

/// Go `pkg/executor/parallel_apply_test.go:50::TestParallelApplyPlan`, data
/// arms: `q1` returns b-values strictly above the max inner b over rows with
/// a smaller a, and `q3` (the same with ORDER BY) returns them in a order --
/// rows `1..9` for Go's fixture (a,b = 0..9 plus a NULL row that never
/// qualifies). Go additionally checks the `explain analyze` line carries an
/// `Apply` with `Concurrency:`; that plan-text contract is the
/// `parallel_apply_plan_explain_gap` test below.
#[test]
fn parallel_apply_plan_q1_and_q3_data() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int, b int)", &mut catalog).unwrap();
    run_insert_on(
        "insert into t values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(null,null)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    let q1 = "select t1.b from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a)";
    assert_eq!(
        select_sorted(&catalog, q1),
        vec![vec![int(1)], vec![int(2)], vec![int(3)], vec![int(4)], vec![int(5)],
             vec![int(6)], vec![int(7)], vec![int(8)], vec![int(9)]],
    );
    // q3: identical rows, delivered in a order. Go compares against the
    // ordered result and requires `show warnings` to be empty; the warning
    // surface is part of the session layer and is covered by the explain gap.
    let q3 = "select t1.b from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a) order by t1.a";
    assert_eq!(
        run_select_on(q3, &catalog, &ctx()).unwrap(),
        vec![vec![int(1)], vec![int(2)], vec![int(3)], vec![int(4)], vec![int(5)],
             vec![int(6)], vec![int(7)], vec![int(8)], vec![int(9)]],
    );
}

/// Go `pkg/executor/parallel_apply_test.go:79::TestApplyColumnType`, the int /
/// varchar / bit / char / double arms. Each pins the apply over a different
/// inner/outer column type: Go's expected rows are cited inline.
#[test]
fn apply_column_type_matrix() {
    // int: `5 5` is the only row above the inner minimum for a larger a.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t(a int, b int)", &mut catalog).unwrap();
    run_create_table_on("create table t1(a int, b int)", &mut catalog).unwrap();
    run_insert_on("insert into t values(1,1), (2,2), (5,5), (2, 4), (5, 2), (9, 4)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t1 values(2, 3), (4, 9), (10, 4), (1, 10)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select * from t where t.b > (select min(t1.b) from t1 where t1.a > t.a)"),
        vec![vec![int(5), int(5)]],
    );

    // varchar: `bb` for the two `aa` rows, `dd` for the two `bb` rows.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a varchar(255), b varchar(255))", &mut catalog).unwrap();
    run_create_table_on("create table t2(a varchar(255))", &mut catalog).unwrap();
    run_insert_on("insert into t1 values ('aa', 'bb'), ('aa', 'tikv'), ('bb', 'cc'), ('bb', 'ee')", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values ('kk'), ('aa'), ('dd'), ('bb')", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select (select min(t2.a) from t2 where t2.a > t1.a) from t1"),
        vec![vec![text("bb")], vec![text("bb")], vec![text("dd")], vec![text("dd")]],
    );

    // bit: rows with b above the inner minimum over smaller bit keys; Go's
    // duplicated fixture yields each qualifying value twice.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a bit(10), b int)", &mut catalog).unwrap();
    run_create_table_on("create table t2(a bit(10), b int)", &mut catalog).unwrap();
    run_insert_on("insert into t1 values ('1', 1), ('2', 2), ('3', 3), ('4', 4), ('1', 1), ('2', 2), ('3', 3), ('4', 4)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values ('1', 1), ('2', 2), ('3', 3), ('4', 4), ('1', 1), ('2', 2), ('3', 3), ('4', 4)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select b from t1 where t1.b > (select min(t2.b) from t2 where t2.a < t1.a)"),
        vec![vec![int(2)], vec![int(2)], vec![int(3)], vec![int(3)], vec![int(4)], vec![int(4)]],
    );

    // char: the inner side is EMPTY (a single all-NULL row), so Go expects no
    // rows -- the max over an empty inner group is NULL and the comparison
    // filters every outer row.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a char(25), b int)", &mut catalog).unwrap();
    run_create_table_on("create table t2(a char(10), b int)", &mut catalog).unwrap();
    run_insert_on("insert into t1 values ('abc', 1), ('abc', '5'), ('fff', 4), ('fff', 9), ('tidb', 6), ('tidb', 5)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values ()", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select t1.b from t1 where t1.b > (select max(t2.b) from t2 where t2.a > t1.a)"),
        Vec::<Vec<Datum>>::new(),
    );

    // double: rows whose a is below the average inner a over rows with a
    // larger b; Go expects `1 1.11`, `1 2.12`, `2 3`, `2 4.56`.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a int, b double)", &mut catalog).unwrap();
    run_create_table_on("create table t2(a int, b double)", &mut catalog).unwrap();
    run_insert_on("insert into t1 values(1, 2.12), (1, 1.11), (2, 3), (2, 4.56), (5, 55), (5, -4)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values(1, 3.22), (3, 4.5), (5, 2.3), (4, 5.55)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select * from t1 where t1.a < (select avg(t2.a) from t2 where t2.b > t1.b)"),
        vec![
            vec![int(1), Datum::Real(1.11)],
            vec![int(1), Datum::Real(2.12)],
            vec![int(2), Datum::Real(3.0)],
            vec![int(2), Datum::Real(4.56)],
        ],
    );
}

/// Go `pkg/executor/parallel_apply_test.go:135-155` (the date, datetime and
/// timestamp arms of `TestApplyColumnType`): a scalar apply whose correlated
/// column is a `Time`-typed value. The driver rejects the composed form.
#[test]
#[ignore = "go-parity-gap: scalar apply over DATE/DATETIME/TIMESTAMP correlated columns is rejected by the driver (DriverError Exec Unsupported \"this subquery result kind is not supported yet\"), measured on this engine this session with Go's exact fixtures; the Time-typed correlated comparison is not transcreated"]
fn apply_column_type_time_arms() {}

/// Go `pkg/executor/parallel_apply_test.go:164::TestApplyMultiColumnType`.
/// Two-column correlated applies over every type pairing Go exercises whose
/// comparison shape this tier supports; the enum pairing is the
/// `apply_multi_column_enum_gap` test below.
#[test]
fn apply_multi_column_type_matrix() {
    // int & int: `4` six times and `6` four times.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a int, b int)", &mut catalog).unwrap();
    run_create_table_on("create table t2(a int, b int)", &mut catalog).unwrap();
    run_insert_on("insert into t1 values (1, 1), (1, 1), (2, 2), (2, 3), (2, 3), (1, 1), (1, 1), (2, 2), (2, 3), (2, 3)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values (2, 2), (3,3), (-1, 1), (5, 4), (2, 2), (3,3), (-1, 1), (5, 4)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select (select count(*) from t2 where t2.a > t1.a and t2.b > t1.a) from t1"),
        vec![vec![int(4)]; 6].into_iter().chain(vec![vec![int(6)]; 4]).collect::<Vec<_>>(),
    );

    // int & char: sum per outer row -- Go's rows are `10 10 6 6 8 8` sorted;
    // each distinct outer a contributes one sum, duplicated by the duplicated
    // fixture rows.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a int, b char(20))", &mut catalog).unwrap();
    run_create_table_on("create table t2(a int, b char(20))", &mut catalog).unwrap();
    run_insert_on("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c'), (1, 'a'), (2, 'b'), (3, 'c')", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values (1, 'a'), (2, 'b'), (3, 'c'), (1, 'a'), (2, 'b'), (3, 'c')", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select (select sum(t2.a) from t2 where t2.a > t1.a or t2.b < t1.b) from t1"),
        // Go's rows `10 10 6 6 8 8` (sorted); each distinct outer a contributes
        // one sum, duplicated by the duplicated fixture rows.
        sorted(vec![
            vec![Datum::Decimal(tidb_datatype::Decimal::from_int(10))],
            vec![Datum::Decimal(tidb_datatype::Decimal::from_int(10))],
            vec![Datum::Decimal(tidb_datatype::Decimal::from_int(6))],
            vec![Datum::Decimal(tidb_datatype::Decimal::from_int(6))],
            vec![Datum::Decimal(tidb_datatype::Decimal::from_int(8))],
            vec![Datum::Decimal(tidb_datatype::Decimal::from_int(8))],
        ]),
    );

    // char & char: every outer row qualifies (`count(*)` = 6).
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a char(20), b varchar(255))", &mut catalog).unwrap();
    run_create_table_on("create table t2(a char(20), b varchar(255))", &mut catalog).unwrap();
    run_insert_on("insert into t1 values ('7', '7'), ('8', '8'), ('9', '9'), ('7', '7'), ('8', '8'), ('9', '9')", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values ('7', '7'), ('8', '8'), ('9', '9'), ('7', '7'), ('8', '8'), ('9', '9')", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select count(*) from t1 where (select sum(t2.a) from t2 where t2.a >= t1.a and t2.b >= t1.b) > 4"),
        vec![vec![int(6)]],
    );

    // char & bit: string literals inserted into BIT(10) store the BYTE value
    // ('1' -> 49), so t2.b < t1.b holds for every inner row until a='4'.
    // Go expects `1`,`1`,`2`,`2`,`3`,`3`.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a varchar(20), b bit(10))", &mut catalog).unwrap();
    run_create_table_on("create table t2(a varchar(20), b int)", &mut catalog).unwrap();
    run_insert_on("insert into t1 values ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4'), ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4')", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values ('1', 1), ('2', 2), ('3', 3), ('4', 4), ('1', 1), ('2', 2), ('3', 3), ('4', 4)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select a from t1 where (select sum(t2.b) from t2 where t2.a > t1.a and t2.b < t1.b) > 4"),
        vec![vec![text("1")], vec![text("1")], vec![text("2")], vec![text("2")], vec![text("3")], vec![text("3")]],
    );

    // int & double: `3 3.3` and `4 4.4` each twice.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1 (a int, b double)", &mut catalog).unwrap();
    run_create_table_on("create table t2 (a int, b double)", &mut catalog).unwrap();
    run_insert_on("insert into t1 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select * from t1 where (select min(t2.a) from t2 where t2.a < t1.a and t2.a > 1 and t2.b < t1.b) > 0"),
        vec![vec![int(3), Datum::Real(3.3)]; 2].into_iter()
            .chain(vec![vec![int(4), Datum::Real(4.4)]; 2]).collect::<Vec<_>>(),
    );

    // int & int & char: `2`,`2`,`3`,`3` then two NULLs (no inner row above
    // a=3).
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a int, b int, c varchar(20))", &mut catalog).unwrap();
    run_create_table_on("create table t2(a int, b int, c varchar(20))", &mut catalog).unwrap();
    run_insert_on("insert into t1 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3')", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3')", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select (select min(t2.a) from t2 where t2.a > t1.a and t2.b > t1.b and t2.c > t1.c) from t1"),
        sorted(vec![
            vec![int(2)],
            vec![int(2)],
            vec![int(3)],
            vec![int(3)],
            vec![Datum::Null],
            vec![Datum::Null],
        ]),
    );
}

/// Go `pkg/executor/parallel_apply_test.go:196-203` (the enum & char arm of
/// `TestApplyMultiColumnType`): `t2.b * 2 > t1.b` compares an INT against an
/// ENUM correlated column. Go expects rows `1 a`, `1 a`, `2 b`, `2 b`,
/// `3 c`, `3 c`.
#[test]
#[ignore = "go-parity-gap: correlated comparison between INT and ENUM columns inside an apply is rejected by the driver (DriverError Exec Unsupported \"this subquery result kind is not supported yet\"), measured on this engine this session with Go's fixture"]
fn apply_multi_column_enum_gap() {}

/// Go `pkg/executor/parallel_apply_test.go:260::TestMultipleApply`, arms 2-4:
/// two applies composed by multiplication (arm 2), two applies conjuncted
/// with a constant threshold over varchar+bit columns (arm 3), and multiple
/// apply fields in the SELECT list gated by a COUNT apply in WHERE (arm 4).
#[test]
fn multiple_apply_arms_source() {
    // Arm 2: `(select min(...) ...) * (select min(...) ...) > 1` -> `3 3.3`,
    // `3 3.3`, `4 4.4`, `4 4.4`.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a int, b double)", &mut catalog).unwrap();
    run_create_table_on("create table t2(a int, b double)", &mut catalog).unwrap();
    run_insert_on("insert into t1 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select * from t1 where (select min(t2.a) from t2 where t2.a < t1.a and t2.a > 1) * (select min(t2.a) from t2 where t2.b < t1.b) > 1"),
        vec![vec![int(3), Datum::Real(3.3)]; 2].into_iter()
            .chain(vec![vec![int(4), Datum::Real(4.4)]; 2]).collect::<Vec<_>>(),
    );

    // Arm 3: two applies ANDed -> `1`,`1`,`2`,`2`,`3`,`3`.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a varchar(20), b bit(10))", &mut catalog).unwrap();
    run_create_table_on("create table t2(a varchar(20), b int)", &mut catalog).unwrap();
    run_insert_on("insert into t1 values ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4'), ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4')", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values ('1', 1111), ('2', 2222), ('3', 3333), ('4', 4444), ('1', 1111), ('2', 2222), ('3', 3333), ('4', 4444)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select a from t1 where (select sum(t2.b) from t2 where t2.a > t1.a) > 4 and (select sum(t2.b) from t2 where t2.b > t1.b) > 4"),
        vec![vec![text("1")], vec![text("1")], vec![text("2")], vec![text("2")], vec![text("3")], vec![text("3")]],
    );

    // Arm 4: `(select min(...)), (select max(...))` in the SELECT list with a
    // COUNT apply in WHERE -> `2 4`, `2 4`, `3 4`, `3 4`.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a int, b int, c varchar(20))", &mut catalog).unwrap();
    run_create_table_on("create table t2(a int, b int, c varchar(20))", &mut catalog).unwrap();
    run_insert_on("insert into t1 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4')", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4')", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select (select min(t2.a) from t2 where t2.a > t1.a and t2.b > t1.b), (select max(t2.a) from t2 where t2.a > t1.a and t2.b > t1.b) from t1 where (select count(*) from t2 where t2.c > t1.c) > 3"),
        vec![vec![int(2), int(4)], vec![int(2), int(4)], vec![int(3), int(4)], vec![int(3), int(4)]],
    );
}

/// Go `pkg/executor/parallel_apply_test.go:271-275` (arm 1 of
/// `TestMultipleApply`): two SUM applies COMPARED TO EACH OTHER
/// (`(select sum ...) >= (select sum ...)`), the outer `t1.b` being an ENUM.
#[test]
#[ignore = "go-parity-gap: comparing two scalar subqueries to each other is rejected by the driver (DriverError Exec Unsupported \"this subquery result kind is not supported yet\"), measured on this engine this session with Go's fixture; Go expects rows `1 a` x2, `2 b` x2, `3 c` x2"]
fn multiple_apply_scalar_vs_scalar_gap() {}

/// Go `pkg/executor/parallel_apply_test.go:307::TestApplyWithOtherOperators`:
/// an apply in the SELECT list above every join method Go forces with a hint.
/// The multiset `0 0 0 0 2 2 2 2 4 4 4 4` (or `0 1 2` for the unique-index
/// arms) is Go's expectation, reached here with Go's fixtures.
#[test]
fn apply_with_other_operators_source() {
    // hash_join: pairs (1,1),(2,2),(3,3) with counts over duplicated data.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a int, b int)", &mut catalog).unwrap();
    run_create_table_on("create table t2(a int, b int)", &mut catalog).unwrap();
    run_insert_on("insert into t1 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select /*+ hash_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a"),
        vec![vec![int(0)]; 4].into_iter()
            .chain(vec![vec![int(2)]; 4]).chain(vec![vec![int(4)]; 4]).collect::<Vec<_>>(),
    );

    // merge_join over a double/int pairing.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a double, b int)", &mut catalog).unwrap();
    run_create_table_on("create table t2(a int, b int)", &mut catalog).unwrap();
    run_insert_on("insert into t1 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select /*+ merge_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a"),
        vec![vec![int(0)]; 4].into_iter()
            .chain(vec![vec![int(2)]; 4]).chain(vec![vec![int(4)]; 4]).collect::<Vec<_>>(),
    );

    // index merge join: Go's fixture has t1 = (1,1),(2,2),(3,3) and t2 with
    // every row DUPLICATED, so each outer row's apply count is 0/2/4 twice.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a int primary key, b int)", &mut catalog).unwrap();
    run_create_table_on("create table t2(a int, b int, index idx(a))", &mut catalog).unwrap();
    run_insert_on("insert into t1 values (1, 1), (2, 2), (3, 3)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)", &mut catalog, &ctx()).unwrap();
    for hint in ["inl_merge_join(t1)", "inl_merge_join(t2)"] {
        let sql = format!(
            "select /*+ {hint} */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a"
        );
        assert_eq!(
            select_sorted(&catalog, &sql),
            vec![vec![int(0)], vec![int(0)], vec![int(2)], vec![int(2)], vec![int(4)], vec![int(4)]],
            "hint {hint}",
        );
    }

    // index hash join: the FULLY duplicated fixture from Go (6+6 rows) gives
    // 0 x4, 2 x4, 4 x4.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a int, b int, index idx(a, b))", &mut catalog).unwrap();
    run_create_table_on("create table t2(a int, b int, index idx(a))", &mut catalog).unwrap();
    run_insert_on("insert into t1 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)", &mut catalog, &ctx()).unwrap();
    for hint in ["inl_hash_join(t1)", "inl_hash_join(t2)"] {
        let sql = format!(
            "select /*+ {hint} */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a"
        );
        assert_eq!(
            select_sorted(&catalog, &sql),
            vec![vec![int(0)]; 4].into_iter()
                .chain(vec![vec![int(2)]; 4]).chain(vec![vec![int(4)]; 4]).collect::<Vec<_>>(),
            "hint {hint}",
        );
    }

    // index join over UNIQUE keys: counts 0/1/2.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1(a int , b int, unique index idx(a))", &mut catalog).unwrap();
    run_create_table_on("create table t2(a int, b int, unique index idx(a))", &mut catalog).unwrap();
    run_insert_on("insert into t1 values (1, 1), (2, 2), (3, 3)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values (1, 1), (2, 2), (3, 3)", &mut catalog, &ctx()).unwrap();
    for hint in ["inl_join(t1)", "inl_join(t2)"] {
        let sql = format!(
            "select /*+ {hint} */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a"
        );
        assert_eq!(
            select_sorted(&catalog, &sql),
            vec![vec![int(0)], vec![int(1)], vec![int(2)]],
            "hint {hint}",
        );
    }

    // index merge: Go expects the six rows with a in 1..3 duplicated pairs.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t(a int, b int, c int, index idxa(a), unique index idxb(b))", &mut catalog).unwrap();
    run_insert_on("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (1, 5, 1), (2, 6, 2), (3, 7, 3), (4, 8, 4)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select /*+ use_index_merge(t) */ * from t where (a > 0 or b < 0) and (select count(*) from t t1 where t1.c > t.a) > 0"),
        vec![
            vec![int(1), int(1), int(1)],
            vec![int(1), int(5), int(1)],
            vec![int(2), int(2), int(2)],
            vec![int(2), int(6), int(2)],
            vec![int(3), int(3), int(3)],
            vec![int(3), int(7), int(3)],
        ],
    );
}

/// Go `pkg/executor/parallel_apply_test.go:392::TestApplyConcurrency`, data
/// arm: `select sum(a) from t where t.a >= (select max(a) from t t1 where
/// t1.a <= t.a)` over t = 1..100 must be 5050 at every concurrency setting.
/// The `tidb_executor_concurrency` SET arms are execution-mode surface and
/// have no tier analog (the executor is sequential); the VALUE is the
/// engine-visible contract.
#[test]
fn apply_concurrency_sum_over_prefix_source() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (a int)", &mut catalog).unwrap();
    let values: String = (1..=100i64).map(|i| format!("({i})")).collect::<Vec<_>>().join(",");
    run_insert_on(&format!("insert into t values {values}"), &mut catalog, &ctx()).unwrap();
    assert_eq!(
        run_select_on(
            "select sum(a) from t where t.a >= (select max(a) from t t1 where t1.a <= t.a)",
            &catalog,
            &ctx(),
        )
        .unwrap(),
        vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_int(5050))]],
    );
}

/// Go `pkg/executor/parallel_apply_test.go:427::TestApplyCacheRatio`:
/// `explain analyze` output must report `cacheHitRatio:10.000%` / `20.000%` /
/// `50.000%` for fixtures with the matching duplicate ratio, and nothing when
/// `tidb_mem_quota_apply_cache = 0`.
#[test]
#[ignore = "go-parity-gap: apply-cache hit-ratio reporting lives in explain-analyze execution stats; this tier has no explain-analyze text surface and the apply cache exposes no ratio (crate::apply_cache is bounded by the quota but unmeasured)"]
fn apply_cache_ratio_gap() {}

/// Go `pkg/executor/parallel_apply_test.go:469::TestApplyGoroutinePanic`: with
/// failpoints `parallelApplyInnerWorkerPanic`, `parallelApplyOuterWorkerPanic`,
/// `parallelApplyGetCachePanic`, `parallelApplySetCachePanic` enabled the
/// query must ERROR, and without them return `4`x6 / `6`x4.
#[test]
#[ignore = "go-parity-gap: the parallel apply worker pool and its failpoint hooks (pkg/executor/parallel_apply.go parallelApplyInnerWorkerPanic et al) are not transcreated; this tier's apply is the sequential operator in crate::apply with no panic-injection surface"]
fn apply_goroutine_panic_gap() {}

/// Go `pkg/executor/parallel_apply_test.go:499::TestParallelApplyCorrectness`:
/// NO_DECORRELATE sum apply over `t1.c3 = alias.c3` for `alias.c1 = 1`
/// yields `1` and `3` whether or not the parallel flag is on -- on this tier
/// there is one apply implementation, so the rows are pinned once.
#[test]
fn parallel_apply_correctness_source() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1 (c1 bigint, c2 int, c3 int, c4 int, primary key(c1, c2), index (c3))", &mut catalog).unwrap();
    run_insert_on("insert into t1 values(1, 1, 1, 1), (1, 2, 3, 3), (2, 1, 4, 4), (2, 2, 2, 2)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select (select /*+ NO_DECORRELATE() */ sum(c4) from t1 where t1.c3 = alias.c3) from t1 alias where alias.c1 = 1;"),
        vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_int(1))],
             vec![Datum::Decimal(tidb_datatype::Decimal::from_int(3))]],
    );
}

/// Go `pkg/executor/parallel_apply_test.go:515::TestParallelApplyCancelInflight`:
/// with `parallelApplySlowInner` sleeping 300ms per inner execution, a
/// `LIMIT 1` query must return one row in well under the serial time because
/// `Close()` cancels in-flight inner workers via context cancellation.
#[test]
#[ignore = "go-parity-gap: cancel-in-flight timing requires the parallel worker pool, the parallelApplySlowInner failpoint and wall-clock accounting; the sequential apply (crate::apply) has no in-flight workers to cancel"]
fn parallel_apply_cancel_inflight_gap() {}

/// Go `pkg/executor/parallel_apply_test.go:560::TestOrderedParallelApply`,
/// data arms 1-5: ORDER BY with a scalar correlated subquery preserves row
/// order, ORDER BY + LIMIT, EXISTS semi-join with ORDER BY, count apply at
/// every concurrency, and LIMIT/OFFSET. Go proves parallel == serial; the
/// serial values themselves are the pinned contract below.
#[test]
fn ordered_parallel_apply_source() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1 (a int, b int, index idx_a(a))", &mut catalog).unwrap();
    run_create_table_on("create table t2 (a int, b int)", &mut catalog).unwrap();
    run_insert_on("insert into t1 values (1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80),(9,90),(10,100)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)", &mut catalog, &ctx()).unwrap();

    // Arm 1: ordered scalar apply.
    assert_eq!(
        run_select_on(
            "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a",
            &catalog,
            &ctx(),
        )
        .unwrap(),
        (1..=10i64).map(|i| vec![int(i), int(i)]).collect::<Vec<_>>(),
    );

    // Arm 2: ORDER BY + LIMIT -- inner min over a >= outer a is always the
    // outer key itself, so every row survives and LIMIT truncates to five.
    assert_eq!(
        run_select_on(
            "select t1.a, t1.b from t1 where t1.b > (select min(t2.b) from t2 where t2.a >= t1.a) order by t1.a limit 5",
            &catalog,
            &ctx(),
        )
        .unwrap(),
        vec![vec![int(1), int(10)], vec![int(2), int(20)], vec![int(3), int(30)], vec![int(4), int(40)], vec![int(5), int(50)]],
    );

    // Arm 3: EXISTS semi-join with ORDER BY -- t2.b = t2.a, so b > 3 keeps
    // a in 4..10 (Go's serial rows).
    assert_eq!(
        run_select_on(
            "select t1.a from t1 where exists (select 1 from t2 where t2.a = t1.a and t2.b > 3) order by t1.a",
            &catalog,
            &ctx(),
        )
        .unwrap(),
        (4..=10i64).map(|i| vec![int(i)]).collect::<Vec<_>>(),
    );

    // Arm 4: count apply -- 9, 8, ..., 0.
    assert_eq!(
        run_select_on(
            "select t1.a, (select count(*) from t2 where t2.a > t1.a) from t1 order by t1.a",
            &catalog,
            &ctx(),
        )
        .unwrap(),
        (1..=10i64).map(|i| vec![int(i), int(10 - i)]).collect::<Vec<_>>(),
    );

    // Arm 5: LIMIT/OFFSET over the ordered apply.
    assert_eq!(
        run_select_on(
            "select t1.a, t1.b from t1 where t1.b > (select min(t2.b) from t2 where t2.a >= t1.a) order by t1.a limit 3 offset 2",
            &catalog,
            &ctx(),
        )
        .unwrap(),
        vec![vec![int(3), int(30)], vec![int(4), int(40)], vec![int(5), int(50)]],
    );
}

/// Go `pkg/executor/parallel_apply_test.go:664::TestOrderedParallelApplyEdgeCases`,
/// data arms 1-7: outer filter, NOT EXISTS anti-semi-join, LIMIT 1 eager
/// flush, empty outer side, single outer row, left-outer-semi IN with
/// NO_DECORRELATE, and a 2000-row outer side.
#[test]
fn ordered_parallel_apply_edge_cases_source() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1 (a int, b int, index idx_a(a))", &mut catalog).unwrap();
    run_create_table_on("create table t2 (a int, b int)", &mut catalog).unwrap();
    run_insert_on("insert into t1 values (1,1),(2,2),(3,3),(4,4),(5,5)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values (1,10),(2,20),(3,30)", &mut catalog, &ctx()).unwrap();

    // Arm 1: outer filter leaves a in 3..5; the inner max over a <= 3 is 30.
    assert_eq!(
        select_sorted(&catalog, "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 where t1.a > 2 order by t1.a"),
        vec![vec![int(3), int(30)], vec![int(4), int(30)], vec![int(5), int(30)]],
    );

    // Arm 2: NOT EXISTS -- only a=4,5 lack an inner match.
    assert_eq!(
        select_sorted(&catalog, "select t1.a from t1 where not exists (select 1 from t2 where t2.a = t1.a) order by t1.a"),
        vec![vec![int(4)], vec![int(5)]],
    );

    // Arm 3: ORDER BY + LIMIT 1.
    assert_eq!(
        run_select_on(
            "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a limit 1",
            &catalog,
            &ctx(),
        )
        .unwrap(),
        vec![vec![int(1), int(10)]],
    );

    // Arm 4: empty outer side.
    assert_eq!(
        run_select_on(
            "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 where t1.a > 999 order by t1.a",
            &catalog,
            &ctx(),
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new(),
    );

    // Arm 5: single outer row.
    assert_eq!(
        run_select_on(
            "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 where t1.a = 3 order by t1.a",
            &catalog,
            &ctx(),
        )
        .unwrap(),
        vec![vec![int(3), int(30)]],
    );

    // Arm 6: left-outer-semi IN with NO_DECORRELATE -> 0/1 flags.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t3 (a int, index idx_a(a))", &mut catalog).unwrap();
    run_create_table_on("create table t4 (a int)", &mut catalog).unwrap();
    run_insert_on("insert into t3 values (1),(2),(3),(4),(5)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t4 values (2),(4)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select a, a in (select /*+ NO_DECORRELATE() */ a from t4 where t4.a = t3.a) from t3 order by a"),
        vec![vec![int(1), int(0)], vec![int(2), int(1)], vec![int(3), int(0)], vec![int(4), int(1)], vec![int(5), int(0)]],
    );

    // Arm 7: 2000-row outer side over a 3-row inner: the count is
    // min(a, 3).
    let mut catalog = Catalog::default();
    run_create_table_on("create table t5 (a int, index idx_a(a))", &mut catalog).unwrap();
    run_create_table_on("create table t6 (a int)", &mut catalog).unwrap();
    let values: String = (1..=2000i64).map(|i| format!("({i})")).collect::<Vec<_>>().join(",");
    run_insert_on(&format!("insert into t5 values {values}"), &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t6 values (1),(2),(3)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        run_select_on(
            "select t5.a, (select count(*) from t6 where t6.a <= t5.a) from t5 order by t5.a",
            &catalog,
            &ctx(),
        )
        .unwrap(),
        (1..=2000i64).map(|i| vec![int(i), int(i.min(3))]).collect::<Vec<_>>(),
    );
}

/// Go `pkg/executor/parallel_apply_test.go:764::TestOrderedParallelApplyLargeInner`:
/// a 2000-row inner keyed a = i%3+1 (counts 666/667/667) and a cartesian-sum
/// arm over t4 = 1..500 (sums 5050 / 20100 / 45150).
#[test]
fn ordered_parallel_apply_large_inner_source() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1 (a int, index idx_a(a))", &mut catalog).unwrap();
    run_create_table_on("create table t2 (a int, b int)", &mut catalog).unwrap();
    run_insert_on("insert into t1 values (1),(2),(3)", &mut catalog, &ctx()).unwrap();
    let values: String = (1..=2000i64)
        .map(|i| format!("({}, {})", i % 3 + 1, i))
        .collect::<Vec<_>>()
        .join(",");
    run_insert_on(&format!("insert into t2 values {values}"), &mut catalog, &ctx()).unwrap();
    assert_eq!(
        run_select_on(
            "select t1.a, (select /*+ NO_DECORRELATE() */ count(*) from t2 where t2.a = t1.a) from t1 order by t1.a",
            &catalog,
            &ctx(),
        )
        .unwrap(),
        vec![vec![int(1), int(666)], vec![int(2), int(667)], vec![int(3), int(667)]],
    );

    // Cartesian-style: every outer row pairs with the whole inner.
    let mut catalog = Catalog::default();
    run_create_table_on("create table t3 (a int, index idx_a(a))", &mut catalog).unwrap();
    run_create_table_on("create table t4 (b int)", &mut catalog).unwrap();
    run_insert_on("insert into t3 values (1),(2),(3)", &mut catalog, &ctx()).unwrap();
    let values: String = (1..=500i64).map(|i| format!("({i})")).collect::<Vec<_>>().join(",");
    run_insert_on(&format!("insert into t4 values {values}"), &mut catalog, &ctx()).unwrap();
    assert_eq!(
        run_select_on(
            "select t3.a, (select /*+ NO_DECORRELATE() */ sum(t4.b) from t4 where t4.b <= t3.a * 100) from t3 order by t3.a",
            &catalog,
            &ctx(),
        )
        .unwrap(),
        vec![
            vec![int(1), Datum::Decimal(tidb_datatype::Decimal::from_int(5050))],
            vec![int(2), Datum::Decimal(tidb_datatype::Decimal::from_int(20100))],
            vec![int(3), Datum::Decimal(tidb_datatype::Decimal::from_int(45150))],
        ],
    );
}

/// Go `pkg/executor/parallel_apply_test.go:823::TestOrderedParallelApplyLeftOuterSemiJoin`,
/// data arms q1-q5: IN-as-boolean over an equality apply, NOT IN anti-semi,
/// a scalar apply that returns NULL for unmatched rows, and both over
/// filtered/nullable outer sides.
#[test]
fn ordered_parallel_apply_left_outer_semi_source() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t1 (a int, b int, index idx_a(a))", &mut catalog).unwrap();
    run_create_table_on("create table t2 (a int, b int)", &mut catalog).unwrap();
    run_insert_on("insert into t1 values (1,10),(2,20),(3,30),(4,40),(5,50)", &mut catalog, &ctx()).unwrap();
    run_insert_on("insert into t2 values (2,2),(4,4)", &mut catalog, &ctx()).unwrap();

    // q1: IN over the equality apply.
    assert_eq!(
        select_sorted(&catalog, "select a, a in (select /*+ NO_DECORRELATE() */ a from t2 where t2.a = t1.a) from t1 order by a"),
        vec![vec![int(1), int(0)], vec![int(2), int(1)], vec![int(3), int(0)], vec![int(4), int(1)], vec![int(5), int(0)]],
    );

    // q2: NOT IN anti-semi -- a=1,3,5 survive.
    assert_eq!(
        select_sorted(&catalog, "select a from t1 where a not in (select /*+ NO_DECORRELATE() */ a from t2 where t2.b < t1.b) order by a"),
        vec![vec![int(1)], vec![int(3)], vec![int(5)]],
    );

    // q3: scalar apply returns the inner b, NULL for unmatched.
    assert_eq!(
        select_sorted(&catalog, "select t1.a, (select t2.b from t2 where t2.a = t1.a) from t1 order by t1.a"),
        vec![vec![int(1), Datum::Null], vec![int(2), int(2)], vec![int(3), Datum::Null], vec![int(4), int(4)], vec![int(5), Datum::Null]],
    );

    // q4/q5: filtered outer sides (adds (null,null) and (6,null) first).
    run_insert_on("insert into t1 values (null, null), (6, null)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        select_sorted(&catalog, "select a, a in (select /*+ NO_DECORRELATE() */ a from t2 where t2.a = t1.a) from t1 where t1.b is not null order by a"),
        vec![vec![int(1), int(0)], vec![int(2), int(1)], vec![int(3), int(0)], vec![int(4), int(1)], vec![int(5), int(0)]],
    );
    assert_eq!(
        select_sorted(&catalog, "select a, a in (select /*+ NO_DECORRELATE() */ a from t2 where t2.a = t1.a) from t1 where t1.a > 2 order by a"),
        vec![vec![int(3), int(0)], vec![int(4), int(1)], vec![int(5), int(0)], vec![int(6), int(0)]],
    );
}

/// Go `pkg/executor/parallel_apply_test.go:887::TestOrderedParallelApplyGoroutinePanic`
/// and `:917::TestOrderedParallelApplyKillSignal`: baseline rows
/// `1 10`..`5 30` are covered by `ordered_parallel_apply_source`; the panic
/// and kill contracts are the gap.
#[test]
#[ignore = "go-parity-gap: ordered-worker panic propagation (parallelApplyInnerWorkerOrderedPanic / parallelApplyOuterWorkerPanic failpoints) and SQLKiller kill-signal timing are parallel-execution surfaces with no tier analog"]
fn ordered_parallel_apply_panic_and_kill_gaps() {}

/// Go `pkg/executor/parallel_apply_test.go:969::TestOrderedParallelApplyNested`.
/// Case 1 stacks TWO NO_DECORRELATE max applies in the SELECT list
/// (`max(t2.b)` and `max(t3.b)` under `t?.a = t1.a`); case 2 nests one apply
/// inside another's inner expression. Both are rejected by this engine:
/// case 1 hits a chunk column-index panic (max/min scalar apply under
/// EQUALITY correlation, observed at crates/tidb-chunk/src/chunk.rs:213 this
/// session), case 2 fails with Exec Unsupported "the correlated subquery
/// failed".
#[test]
#[ignore = "go-parity-gap: two stacked max applies with equality correlation panic in the chunk writer (tidb-chunk chunk.rs:213 index out of bounds), and an apply nested inside another apply's inner expression is rejected (Exec Unsupported \"the correlated subquery failed\"); both measured on this engine this session with Go's fixtures"]
fn ordered_parallel_apply_nested_gap() {}

/// Go `checkApplyPlan` (`pkg/executor/parallel_apply_test.go:31`), applied by
/// every test of the file: `explain analyze <sql>` must contain an `Apply`
/// operator whose execution info reports `Concurrency:<n>` (or the flag when
/// parallel == 1). Also Go `TestParallelApplyPlan`'s `show warnings` arm.
#[test]
#[ignore = "go-parity-gap: explain-analyze plan text (operator ids, Concurrency:/concurrency:OFF execution info) has no surface in this tier; the driver produces rows, not plan strings"]
fn parallel_apply_plan_explain_gap() {}
