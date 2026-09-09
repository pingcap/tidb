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
// See the License for the specific language governing permissions and
// limitations under the License.

//! GO PORTS of the vectorized-harness slices assigned to this batch:
//! `builtin_time_vec_generated_test.go` (:11718), `builtin_time_vec_test.go`
//! (:567-:610 VecMonth), `builtin_vec_vec_test.go` (:201), and
//! `builtin_vectorized_test.go` (:104-:194 Benchmarks, :564 DoubleRow2Vec,
//! :589 DoubleVec2Row, :744/:758 MockDouble Benchmarks, :775 VectorizedCheck,
//! :804 Float32ColVec, :836 VecEvalBool, :857 RowBasedFilterAndVectorizedFilter).
//!
//! Go drives these through `vecExprBenchCase` tables filled by random data
//! generators and compared across Go's SEPARATE vectorized/row evaluators.
//! This crate has one evaluator per tier, so the same contracts are pinned
//! deterministically: columnar input chunks agree cell-by-cell with the
//! scalar answers every source row demands, and null bits travel with them.

use super::*;
use crate::column::Column;
use crate::constant::Constant;
use crate::evaluator::{vectorizable, EvaluatorSuite};
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use crate::time_fn::dispatch;
use std::cell::RefCell;
use tidb_ast::CiString;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{CoreTime, FieldType, FieldTypeCode, MySqlDuration, Time, TimeType};

/// Evaluates one builtin through the time family's dispatch seam and unwraps
/// both the dispatch hit and the evaluation result.
fn dispatched(name: &str, vals: &[Datum], cols: &dyn Columns) -> Datum {
    dispatch(name, vals, cols)
        .expect("the name belongs to the time family")
        .expect("source row must evaluate")
}

/// GO PORT of `pkg/expression/builtin_time_vec_generated_test.go:11718
/// TestVectorizedBuiltinTimeEvalOneVecGenerated` and `:11722
/// TestVectorizedBuiltinTimeFuncGenerated`, restricted to the generated
/// case-table FAMILIES whose values this evaluator can derive independently:
/// the ADDDATE/SUBDATE composite-unit constant shapes (`DAY_MINUTE`,
/// `DAY_HOUR`, `YEAR_MONTH`) that make up the table's tail
/// (`ast.AddDate` arm starting at :287 of the source file). The generated
/// table asserts vector results equal per-row evaluation; both of this
/// crate's tiers answering identically pins the same contract.
#[test]
fn vectorized_generated_time_unit_families_match_the_row_answers() {
    // Field-splitting rule of Go's `parseTimeValue`
    // (`pkg/types/time.go:2577`): digit RUNS are matched, then right-assigned
    // to unit fields from the last -- so a single-run integer amount fills
    // ONLY the trailing field ("1441 DAY_MINUTE" is +1441 minutes, i.e.
    // +24h01m; decimal amounts instead arrive pre-padded through
    // `getIntervalFromDecimal`). 25 DAY_HOUR therefore stays +25 hours.
    for (sql, want) in [
        (
            "date_add('2011-11-11 10:00:00', interval 1441 day_minute)",
            "2011-11-12 10:01:00",
        ),
        (
            "date_sub('2011-11-11 10:00:00', interval 1441 day_minute)",
            "2011-11-10 09:59:00",
        ),
        (
            "date_add('2011-11-11 10:00:00', interval 25 day_hour)",
            "2011-11-12 11:00:00",
        ),
        // YEAR_MONTH's month field wraps past December into the next year,
        // with the month-end clamp landing on January 30.
        (
            "date_add('2011-11-30 10:00:00', interval 14 year_month)",
            "2013-01-30 10:00:00",
        ),
    ] {
        assert_eq!(e(sql), format!("STR:{want}"), "{sql} (row tier)");
        assert_eq!(chunk_e(sql), format!("STR:{want}"), "{sql} (chunk tier)");
    }
}

/// GO PORT of `pkg/expression/builtin_time_vec_test.go:567
/// TestVectorizedBuiltinTimeEvalOneVec` and `:571
/// TestVectorizedBuiltinTimeFunc`: boundary vectors (valid pairs, invalid-zero
/// pairs, NULL cadence) for three `vecBuiltinTimeCases` families -- `DateDiff`,
/// `TimeDiff`, and `SecToTime` -- whose column-wide answers equal each row's
/// own scalar answer.
#[test]
fn vectorized_time_harness_representative_cases_match_scalar_answers() {
    fn unary_vec(rows: &[&str], name: &str) -> Vec<String> {
        rows.iter()
            .map(|text| {
                if *text == "<NULL>" {
                    dispatched(name, &[Datum::Null], &NoColumns)
                } else {
                    dispatched(name, &[Datum::new_string(*text)], &NoColumns)
                }
                .label()
            })
            .collect()
    }

    fn binary_vec(
        eval: impl Fn(&[Datum]) -> Result<Datum, EvalError>,
        pairs: &[(&str, &str)],
    ) -> Vec<String> {
        pairs
            .iter()
            .map(|(left, right)| {
                eval(&[Datum::new_string(*left), Datum::new_string(*right)])
                    .expect("source pair must evaluate")
                    .label()
            })
            .collect()
    }

    // ast.SecToTime over signed clamps plus a null cadence; a STRING argument
    // carries MaxFsp, hence the six-digit fractions (`time_fn::tests::sec_to_time_source_vectors`).
    assert_eq!(
        unary_vec(&["3863999", "-3863999", "<NULL>"], "SEC_TO_TIME"),
        ["STR:838:59:59.000000", "STR:-838:59:59.000000", "NULL"]
    );

    // ast.DateDiff (calendar::date_diff; DATEDIFF is not in the time-family
    // dispatch): master's valid pair and an invalid-zero pair.
    assert_eq!(
        binary_vec(
            crate::time_fn::calendar::date_diff,
            &[
                ("2004-05-21", "2004:01:02"),
                ("2007-00-31 23:59:59", "2016-01-13"),
            ],
        ),
        ["INT:140", "NULL"]
    );

    // ast.TimeDiff pairs, including the INVALID pair that is NULL.
    assert_eq!(
        binary_vec(
            |args| dispatch("TIMEDIFF", args, &NoColumns).unwrap(),
            &[
                ("2008-12-31 23:59:59.000001", "2008-12-30 01:01:01.000002",),
                ("2016-12-00 12:00:00", "2016-12-01 12:00:00"),
                ("2016-12-00 12:00:00", "10:9:0"),
            ],
        ),
        ["STR:46:58:57.999999", "STR:-24:00:00", "NULL"]
    );
}

/// GO PORT of `pkg/expression/builtin_time_vec_test.go:575
/// TestVectorizedTimeFormatEmptyFormatReturnsNull`: a TIME_FORMAT vector
/// whose FORMAT column carries an empty string in some rows answers NULL for
/// those rows and formats the others -- `12:34:56 => '' => NULL` is issue
/// #59445's recorded shape.
#[test]
fn vectorized_time_format_empty_format_returns_null() {
    let noon = MySqlDuration::new(12, 34, 56, 0, 0).expect("a valid duration");
    let early = MySqlDuration::new(1, 2, 3, 0, 0).expect("a valid duration");
    let rows = [
        (Datum::Duration(noon.clone()), Datum::new_string(""), "NULL"),
        (
            Datum::Duration(early),
            Datum::new_string("%H:%i:%s"),
            "STR:01:02:03",
        ),
        (Datum::Duration(noon), Datum::Null, "NULL"),
    ];
    for (value, format, want) in rows {
        assert_eq!(
            dispatched("TIME_FORMAT", &[value.clone(), format.clone()], &NoColumns).label(),
            want,
            "TIME_FORMAT({value:?}, {format:?})"
        );
    }
}

/// GO PORT of `pkg/expression/builtin_time_vec_test.go:610 TestVecMonth`:
/// MONTH evaluated over `[zero-date, NULL, zero-date]` emits ZERO warnings
/// under `TruncateAsWarning` and keeps answering under the insert-mode flag
/// combination (`InInsertStmt=true` with `TruncateAsWarning=false`, i.e. the
/// error truncate level here). Stored-component reads (`0`) never become
/// rejections on either mode.
#[test]
fn vec_month_zero_dates_stay_warning_free_in_both_flag_modes() {
    struct WarnSink {
        warnings: RefCell<Vec<(u16, String)>>,
        level: ErrorLevel,
    }

    impl Columns for WarnSink {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_string()));
        }

        fn truncate_level(&self) -> ErrorLevel {
            self.level
        }
    }

    let zero =
        Datum::Time(Time::new(CoreTime::default(), TimeType::DateTime, 0).expect("zero datetime"));
    for level in [ErrorLevel::Warn, ErrorLevel::Error] {
        let sink = WarnSink {
            warnings: RefCell::new(Vec::new()),
            level,
        };
        for row in [&zero, &Datum::Null, &zero] {
            assert_eq!(
                dispatched("MONTH", std::slice::from_ref(row), &sink),
                if matches!(row, Datum::Null) {
                    Datum::Null
                } else {
                    Datum::Int(0)
                },
                "MONTH({row:?}) at truncate level {level:?}"
            );
        }
        assert!(
            sink.warnings.borrow().is_empty(),
            "MONTH vectors emit no warnings at {level:?}: {:?}",
            sink.warnings.borrow()
        );
    }
}

/// GO PORT of `pkg/expression/builtin_vectorized_test.go:104
/// TestMockVecPlusInt`'s buffer contract via the plus operator over two
/// LongLong input columns: evaluating elementwise across a 1024-row chunk --
/// twice, as `genMockRowDouble` repeats its evaluations -- yields `i*2` with
/// no spurious null bits. Go additionally toggles `enableAlloc`; Rust has no
/// result-buffer allocator toggle to model ([`mock_vec_plus_int_parallel_allocator_race`]).
#[test]
fn elementwise_plus_over_columns_matches_the_mock_contract() {
    const ROWS: usize = 1024;
    let long_long = || FieldType::new(FieldTypeCode::LongLong);

    let mut input = Chunk::new_with_capacity(&[long_long(), long_long()], ROWS);
    for i in 0..ROWS {
        input.append_datum(0, &Datum::Int(i as i64));
        input.append_datum(1, &Datum::Int(i as i64));
    }

    let side = |index: i64| {
        let mut column = Column::new(index, long_long());
        column.index = index;
        Expression::Column(column)
    };
    let plus = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("plus"),
        long_long(),
        vec![side(0), side(1)],
    ));

    for repeat in 0..2 {
        let suite = EvaluatorSuite::new(vec![plus.clone()], false);
        let mut output = Chunk::new_with_capacity(&[long_long()], ROWS);
        suite
            .run(&NoColumns, &mut input.clone(), &mut output)
            .expect("elementwise plus succeeds");
        for i in 0..ROWS {
            let row = output.get_row(i);
            assert!(!row.is_null(0), "repeat {repeat} row {i} must not be null");
            assert_eq!(
                row.get_datum(0, &long_long()),
                Datum::Int(i as i64 * 2),
                "repeat {repeat}: buffer carries i*2"
            );
        }
    }
}

/// GO PORT of `pkg/expression/builtin_vectorized_test.go:122
/// TestMockVecPlusIntParallel`: Go races five threads against the shared
/// buffer allocator to prove concurrency safety.
#[test]
#[ignore = "go-parity-gap: the enableAlloc result-buffer allocator (and therefore its concurrency-safety race) has no counterpart; elementwise correctness is pinned serially"]
fn mock_vec_plus_int_parallel_allocator_race() {}

/// GO PORT of `pkg/expression/builtin_vectorized_test.go:564/:589
/// TestDoubleRow2Vec/TestDoubleVec2Row`'s essence: every eval type projects a
/// bare Column through BOTH evaluation directions (Go's transfer path and its
/// cell-by-cell recalculation) with byte-equal outputs and matching null bits.
/// The seven-type sweep lives in
/// `tests/vectorizable_and_chunk_eval_source::test_eval_expr_column_projected_through_both_modes_agrees`;
/// this adds Go's DOUBLE-EVAL leg -- running the SAME suite twice over the
/// same input must reproduce identical chunks.
#[test]
fn double_evaluation_reproduces_the_projected_column_exactly() {
    use tidb_datatype::Decimal;

    const ROWS: usize = 256;
    let decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&decimal_type), ROWS);
    for row in 0..ROWS {
        if row % 7 == 3 {
            input.append_null(0);
        } else {
            input.append_datum(0, &Datum::Decimal(Decimal::from_int(row as i64 * 13 - 6)));
        }
    }

    let mut projection = Column::new(9, decimal_type.clone());
    projection.index = 0;
    let expression = Expression::Column(projection);

    let transfer_suite = EvaluatorSuite::new(vec![expression.clone()], false);
    let cell_by_cell_suite = EvaluatorSuite::new(vec![expression], true);

    let mut first = Chunk::new_with_capacity(std::slice::from_ref(&decimal_type), ROWS);
    let mut second = Chunk::new_with_capacity(std::slice::from_ref(&decimal_type), ROWS);
    let mut recalculated = Chunk::new_with_capacity(std::slice::from_ref(&decimal_type), ROWS);
    transfer_suite
        .run(&NoColumns, &mut input.clone(), &mut first)
        .expect("first transfer");
    transfer_suite
        .run(&NoColumns, &mut input.clone(), &mut second)
        .expect("second transfer (the DoubleRow2Vec leg)");
    cell_by_cell_suite
        .run(&NoColumns, &mut input, &mut recalculated)
        .expect("cell-by-cell recalculation");

    for row in 0..ROWS {
        for (name, other) in [
            ("second-transfer", &second),
            ("cell-by-cell", &recalculated),
        ] {
            assert_eq!(
                first.get_row(row).is_null(0),
                other.get_row(row).is_null(0),
                "{name} null bit differs at {row}"
            );
            if !first.get_row(row).is_null(0) {
                assert_eq!(
                    first.get_row(row).get_raw(0).as_ref(),
                    other.get_row(row).get_raw(0).as_ref(),
                    "{name} bytes differ at {row}"
                );
            }
        }
    }
}

/// GO PORT of `pkg/expression/builtin_vectorized_test.go:744/:758
/// BenchmarkMockDoubleRow/BenchmarkMockDoubleVec`.
#[test]
#[ignore = "skipped-reason: Go testing.B microbenchmark, excluded by the gate"]
fn benchmark_mock_double_row_and_vec() {}

/// GO PORT of `pkg/expression/builtin_vectorized_test.go:775
/// TestVectorizedCheck`: Constants, Columns, and a column-backed correlated
/// column all answer Vectorized()=true through this crate's list predicate,
/// while the stateful setvar list flips it exactly like Go's list predicates.
/// Go's `ScalarFunction{Function: rowF}.Vectorized()` split has no analogue:
/// one evaluator serves both directions here.
#[test]
fn vectorized_check_predicates_over_constants_columns_and_correlated() {
    let constant = Expression::Constant(Constant::new(
        Datum::Int(1),
        FieldType::new(FieldTypeCode::LongLong),
    ));
    assert!(vectorizable(std::slice::from_ref(&constant)));

    let column = Expression::Column(Column::new(0, FieldType::new(FieldTypeCode::LongLong)));
    assert!(vectorizable(std::slice::from_ref(&column)));

    // The Column arm of the predicate covers Go's CorrelatedColumn{Column}
    // variant: the crate models correlation as a Column bound to an outer
    // value (`expression.rs Expression::CorrelatedColumn`), so its ownership
    // is a plain column's.
    let correlated_expression = Expression::CorrelatedColumn(crate::column::CorrelatedColumn {
        column: Column::new(0, FieldType::new(FieldTypeCode::LongLong)),
        data: None,
    });
    assert!(vectorizable(std::slice::from_ref(&correlated_expression)));

    let raw_setvar = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("setvar"),
        FieldType::new(FieldTypeCode::LongLong),
        vec![
            Expression::Column(Column::new(1, FieldType::new(FieldTypeCode::VarString))),
            Expression::Column(Column::new(2, FieldType::new(FieldTypeCode::VarString))),
        ],
    ));
    assert!(!vectorizable(std::slice::from_ref(&raw_setvar)));
}

/// GO PORT of `pkg/expression/builtin_vec_vec_test.go:201
/// TestVectorizedBuiltinVecFunc`: every vector-distance family in
/// `vecBuiltinVecCases` behaves uniformly over FIXED vectors and NULL-vector
/// vectors. Pre-existing carriers live in `builtin_ext/vec.rs`; the rows here
/// carry the master table's own shapes: dimension counts, L2/norm magnitudes,
/// and AS_TEXT round-trips.
#[test]
fn vectorized_builtin_vec_families_match_master_shapes() {
    let vector_of = |values: &[f32]| {
        Datum::new_vector_float32(tidb_datatype::VectorFloat32::must_create(values.to_vec()))
    };
    let rows = vec![
        ("VEC_DIMS", vec![vector_of(&[1.0, 2.0, 3.0])], "INT:3"),
        ("VEC_DIMS", vec![Datum::Null], "NULL"),
        ("VEC_L2_NORM", vec![vector_of(&[3.0, 4.0])], "FLOAT:5"),
        ("VEC_L2_NORM", vec![Datum::Null], "NULL"),
        ("VEC_AS_TEXT", vec![vector_of(&[1.0, 2.0])], "STR:[1,2]"),
    ];
    for (name, args, want) in rows {
        assert_eq!(
            crate::builtin_ext::vec::dispatch(name, &args)
                .expect("a VEC_ family name")
                .expect("vector shapes evaluate")
                .label(),
            want,
            "{name}{args:?}"
        );
    }
    // The L1/L2/Cosine/NegativeInnerProduct magnitudes and the different-
    // dimension domain error are pinned by `builtin_ext/vec.rs`'s own tests;
    // the CosineDistance `[0,0,0]` NaN-origin NULL shape rides the same
    // distance path those tests carry.
}

/// GO PORT of `pkg/expression/builtin_vectorized_test.go:804
/// TestFloat32ColVec`.
#[test]
#[ignore = "go-parity-gap: FLOAT32 columnar storage (mysql.TypeFloat chunks) is not modeled; vector floats exist only as scalar datums"]
fn float32_col_vectorization() {}

/// GO PORT of `pkg/expression/builtin_vectorized_test.go:836 TestVecEvalBool`.
#[test]
#[ignore = "go-parity-gap: VecEvalBool/EvalBool selection machinery (selected/nulls buffers over expression conjunctions) is not modeled in this crate"]
fn vec_eval_bool_matches_row_eval_bool() {}

/// GO PORT of `pkg/expression/builtin_vectorized_test.go:857
/// TestRowBasedFilterAndVectorizedFilter`.
#[test]
#[ignore = "go-parity-gap: the rowBasedFilter/vectorizedFilter equivalence harness needs the missing VecEvalBool layer"]
fn row_based_filter_and_vectorized_filter_agree() {}

/// GO PORT of the batch slice's Benchmark* harnesses
/// (`builtin_time_vec_generated_test.go:11726/:11730`,
/// `builtin_time_vec_test.go:602/:606`, `builtin_vec_vec_test.go:205`,
/// `builtin_vectorized_test.go:148/:158/:170/:181/:194`).
#[test]
#[ignore = "skipped-reason: Go testing.B microbenchmarks, excluded by the gate"]
fn benchmark_vectorized_harnesses() {}
