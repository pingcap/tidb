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

//! Direct source tests for the live variance and standard-deviation runtime.

use tidb_ast::{Expr, WindowDef, WindowOver};
use tidb_datatype::{Collation, Datum, FieldTypeCode};
use tidb_exec::aggregate::runtime::{fold_values, VarianceState};
use tidb_exec::{resolve_result_fields, ResultFieldSpec};
use tidb_exec::{Database, Outcome, Row};
use tidb_planner::aggregation_descriptor::AggregateKind;

const KINDS: [AggregateKind; 4] = [
    AggregateKind::VarPop,
    AggregateKind::VarSamp,
    AggregateKind::StddevPop,
    AggregateKind::StddevSamp,
];

fn state(kind: AggregateKind, distinct: bool, values: &[Option<f64>]) -> VarianceState {
    let mut state = VarianceState::new(kind, distinct).expect("variance kind");
    for value in values {
        state.update_real(*value).expect("finite source value");
    }
    state
}

fn assert_close(actual: Option<f64>, expected: f64) {
    let actual = actual.expect("non-NULL variance result");
    assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
}

fn run(database: &mut Database, sql: &str) -> Outcome {
    database
        .run(&tidb_parser::parse(sql).expect("variance SQL parses"))
        .expect("variance SQL executes")
}

fn rows(database: &mut Database, sql: &str) -> Vec<Row> {
    match run(database, sql) {
        Outcome::Rows(result) => result.rows,
        Outcome::Done => panic!("expected query rows"),
    }
}

fn assert_real(value: &Datum, expected: f64) {
    let Datum::Real(actual) = value else {
        panic!("expected real result, got {value:?}");
    };
    assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
}

#[test]
fn ordinary_update_and_finalization_match_all_four_go_functions() {
    // Direct Go sources: func_varpop.go:58-82, func_varsamp.go:24-33,
    // func_stddevpop.go:26-35, and func_stddevsamp.go:26-35.
    let values = [None, Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0)];
    assert_close(state(AggregateKind::VarPop, false, &values).result(), 2.0);
    assert_close(state(AggregateKind::VarSamp, false, &values).result(), 2.5);
    assert_close(
        state(AggregateKind::StddevPop, false, &values).result(),
        2.0_f64.sqrt(),
    );
    assert_close(
        state(AggregateKind::StddevSamp, false, &values).result(),
        2.5_f64.sqrt(),
    );

    for kind in KINDS {
        assert_eq!(state(kind, false, &[None]).result(), None);
    }
    assert_close(
        state(AggregateKind::VarPop, false, &[Some(7.0)]).result(),
        0.0,
    );
    assert_close(
        state(AggregateKind::StddevPop, false, &[Some(7.0)]).result(),
        0.0,
    );
    assert_eq!(
        state(AggregateKind::VarSamp, false, &[Some(7.0)]).result(),
        None
    );
    assert_eq!(
        state(AggregateKind::StddevSamp, false, &[Some(7.0)]).result(),
        None
    );
}

#[test]
fn ordinary_partial_merge_uses_go_source_order_and_zero_branches() {
    // Direct Go source/test: func_varpop.go:84-118 and each
    // TestMergePartialResult4{Varpop,Varsamp,Stddevpop,Stddevsamp}.
    let expected = [
        (AggregateKind::VarPop, 2.0, 2.0 / 3.0, 1.734375),
        (AggregateKind::VarSamp, 2.5, 1.0, 1.982_142_857_142_857_2),
        (
            AggregateKind::StddevPop,
            std::f64::consts::SQRT_2,
            0.816_496_580_927_726,
            1.316_956_719_106_592_3,
        ),
        (
            AggregateKind::StddevSamp,
            1.581_138_830_084_189_8,
            1.0,
            1.407_885_953_173_359,
        ),
    ];
    for (kind, first_expected, second_expected, merged_expected) in expected {
        // This is the Go harness's exact shape: first update 0..4, merge;
        // reset the source, update 2..4, merge into the same destination.
        let first = state(
            kind,
            false,
            &[Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0)],
        );
        let second = state(kind, false, &[Some(2.0), Some(3.0), Some(4.0)]);
        assert_close(first.result(), first_expected);
        assert_close(second.result(), second_expected);
        let mut destination = VarianceState::new(kind, false).unwrap();
        destination.merge_from(&first).expect("first partial");
        destination.merge_from(&second).expect("second partial");
        assert_close(destination.result(), merged_expected);

        let mut empty_destination = VarianceState::new(kind, false).unwrap();
        empty_destination
            .merge_from(&destination)
            .expect("copy into empty destination");
        assert_close(
            empty_destination.result(),
            destination.result().expect("merged rows"),
        );
        destination
            .merge_from(&VarianceState::new(kind, false).unwrap())
            .expect("empty source is ignored");
        assert_close(destination.result(), merged_expected);
    }
}

#[test]
fn dedicated_distinct_float_state_deduplicates_updates_and_partial_merges() {
    // Direct Go source: partialResult4VarPopDistinctFloat64 and
    // calculateDistinctFloat64Variance in func_varpop.go:129-203.
    // Direct Go test: TestParallelDistinctVarAndStddev and its generated
    // empty/NULL/non-NULL split-and-merge cases.
    for kind in KINDS {
        let source = state(kind, true, &[None, Some(0.0), Some(1.0), Some(1.0)]);
        let mut destination = state(kind, true, &[Some(-0.0), Some(2.0), Some(2.0)]);
        destination.merge_from(&source).expect("distinct set union");
        assert_eq!(destination.distinct_len(), 3);

        let expected = match kind {
            AggregateKind::VarPop => 2.0 / 3.0,
            AggregateKind::VarSamp => 1.0,
            AggregateKind::StddevPop => (2.0_f64 / 3.0).sqrt(),
            AggregateKind::StddevSamp => 1.0,
            _ => unreachable!(),
        };
        assert_close(destination.result(), expected);
    }
}

#[test]
fn live_fold_routes_variance_distinct_around_generic_datum_checker() {
    let values = vec![
        Datum::Real(0.0),
        Datum::Real(-0.0),
        Datum::Real(1.0),
        Datum::Real(1.0),
        Datum::Real(2.0),
        Datum::Null,
    ];
    assert_eq!(
        fold_values(AggregateKind::VarPop, true, &values, 4).unwrap(),
        Datum::Real(2.0 / 3.0)
    );
    assert_eq!(
        fold_values(AggregateKind::VarSamp, true, &values, 4).unwrap(),
        Datum::Real(1.0)
    );
    assert_eq!(
        fold_values(AggregateKind::StddevSamp, true, &[Datum::Real(1.0)], 4).unwrap(),
        Datum::Null
    );
    assert_eq!(
        fold_values(AggregateKind::StddevPop, false, &[], 4).unwrap(),
        Datum::Null
    );
}

#[test]
fn database_and_window_consumers_reach_the_canonical_real_state() {
    let mut database = Database::new();
    assert_eq!(
        run(
            &mut database,
            "create table variance_live (id int, v double)"
        ),
        Outcome::Done
    );
    assert_eq!(
        run(
            &mut database,
            "insert into variance_live values \
             (1, cast(0 as double)), (2, cast(1 as double)), \
             (3, cast(2 as double)), (4, cast(3 as double)), \
             (5, cast(4 as double)), (6, cast(4 as double)), (7, null)"
        ),
        Outcome::Done
    );

    let ordinary = rows(
        &mut database,
        "select var_pop(v), var_samp(v), stddev_pop(v), stddev_samp(v) \
         from variance_live where id <= 5",
    );
    assert_eq!(ordinary.len(), 1);
    assert_real(&ordinary[0][0], 2.0);
    assert_real(&ordinary[0][1], 2.5);
    assert_real(&ordinary[0][2], 2.0_f64.sqrt());
    assert_real(&ordinary[0][3], 2.5_f64.sqrt());

    let distinct = rows(
        &mut database,
        "select var_pop(distinct v), var_samp(distinct v), \
         stddev_pop(distinct v), stddev_samp(distinct v) from variance_live",
    );
    assert_eq!(distinct.len(), 1);
    assert_real(&distinct[0][0], 2.0);
    assert_real(&distinct[0][1], 2.5);
    assert_real(&distinct[0][2], 2.0_f64.sqrt());
    assert_real(&distinct[0][3], 2.5_f64.sqrt());

    // The window coordinator routes these four names through the same
    // fold_aggregate_values seam. Five physical rows therefore repeat the
    // same full-partition result rather than creating another state family.
    let windowed = rows(
        &mut database,
        "select id, var_pop(v) over (), var_samp(v) over (), \
         stddev_pop(v) over (), stddev_samp(v) over () \
         from variance_live where id <= 5 order by id",
    );
    assert_eq!(windowed.len(), 5);
    for (index, row) in windowed.iter().enumerate() {
        assert_eq!(row[0], Datum::Int(index as i64 + 1));
        assert_real(&row[1], 2.0);
        assert_real(&row[2], 2.5);
        assert_real(&row[3], 2.0_f64.sqrt());
        assert_real(&row[4], 2.5_f64.sqrt());
    }

    // The parser canonicalizes VARIANCE/STD/STDDEV before both ordinary
    // aggregate and window dispatch. Exercise the SQL surface rather than
    // relying only on AggregateKind::from_name's unit test.
    let aliases = rows(
        &mut database,
        "select variance(v), std(v), stddev(v) \
         from variance_live where id <= 5",
    );
    assert_eq!(aliases.len(), 1);
    assert_real(&aliases[0][0], 2.0);
    assert_real(&aliases[0][1], 2.0_f64.sqrt());
    assert_real(&aliases[0][2], 2.0_f64.sqrt());

    let alias_windowed = rows(
        &mut database,
        "select id, variance(v) over (), std(v) over () \
         from variance_live where id <= 5 order by id",
    );
    assert_eq!(alias_windowed.len(), 5);
    for (index, row) in alias_windowed.iter().enumerate() {
        assert_eq!(row[0], Datum::Int(index as i64 + 1));
        assert_real(&row[1], 2.0);
        assert_real(&row[2], 2.0_f64.sqrt());
    }
}

#[test]
fn reset_memory_boundary_and_fallible_domain_are_explicit() {
    // Direct Go test: TestMemVarpop. The fixed ordinary tuple is exact; Go's
    // MemAwareMap bucket accounting is intentionally not claimed by Rust's
    // HashMap-backed DISTINCT set.
    assert_eq!(VarianceState::ordinary_partial_state_size(), 24);
    let mut distinct = state(
        AggregateKind::VarPop,
        true,
        &[Some(1.0), Some(1.0), Some(2.0)],
    );
    assert_eq!(distinct.distinct_len(), 2);
    distinct.reset();
    assert_eq!(distinct.distinct_len(), 0);
    assert_eq!(distinct.result(), None);

    assert!(VarianceState::new(AggregateKind::Sum, false).is_err());
    assert!(state(AggregateKind::VarPop, false, &[Some(1.0)])
        .merge_from(&state(AggregateKind::VarSamp, false, &[Some(1.0)]))
        .is_err());
    assert!(state(AggregateKind::VarPop, false, &[Some(1.0)])
        .merge_from(&state(AggregateKind::VarPop, true, &[Some(1.0)]))
        .is_err());
    assert!(VarianceState::new(AggregateKind::VarPop, false)
        .unwrap()
        .update(&Datum::Int(1))
        .is_err());
    assert!(VarianceState::new(AggregateKind::VarPop, false)
        .unwrap()
        .update(&Datum::Real(f64::NAN))
        .is_err());
}

#[test]
fn variance_result_metadata_is_always_double_23_with_unspecified_scale() {
    // Direct Go source: base_func.go::typeInfer4PopOrSamp.
    for name in [
        "VAR_POP",
        "VARIANCE",
        "VAR_SAMP",
        "STDDEV_POP",
        "STDDEV",
        "STD",
        "STDDEV_SAMP",
    ] {
        for expression in [
            Expr::Aggregate {
                name: name.to_owned(),
                distinct: false,
                args: vec![Expr::Int("1".to_owned())],
            },
            Expr::Window {
                name: name.to_owned(),
                args: vec![Expr::Int("1".to_owned())],
                distinct: false,
                ignore_nulls: false,
                from_last: false,
                over: WindowOver::Def(WindowDef::default()),
            },
        ] {
            let fields =
                resolve_result_fields(&[ResultFieldSpec::new(expression)], Collation::Utf8Mb4Bin)
                    .expect("variance metadata");
            let field = &fields[0].field_type;
            assert_eq!(field.code, FieldTypeCode::Double);
            assert_eq!(field.flen, Some(23));
            assert_eq!(field.decimal, None);
            assert_eq!(field.flags, 0);
            assert_eq!(field.collation, Collation::Binary);
        }
    }
}
