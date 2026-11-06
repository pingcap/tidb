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

//! Go `pkg/executor/aggfuncs/aggfunc_test.go`'s harness and the tests of
//! the ported functions (`func_count_test.go`, ...), with Go's expected
//! results and memory deltas.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, Decimal, FieldType, FieldTypeCode, MySqlDuration};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expression_aggregation::AggFuncDesc;
use tidb_exprctx::context::NoColumns;
use tidb_goruntime::GoString;

use crate::agg_builder::build;
use crate::agg_func::AggFunc;

/// Go `aggTest`.
struct AggTest {
    key_type: FieldType,
    num_rows: usize,
    func_name: &'static str,
    results: Vec<Datum>,
}

/// Go's `&expression.Constant{Value: types.NewStringDatum(separator), ...}`.
fn separator_constant(sep: &str) -> Expression {
    Expression::Constant(tidb_expr::constant::Constant::new(Datum::new_string_datum(sep), FieldType::new(FieldTypeCode::String)))
}

/// Go `getDataGenFunc`'s enum and set elements.
const GEN_ELEMS: [&str; 5] = ["e", "d", "c", "b", "a"];

/// Go `ft.GetCollate()`.
fn collation_of(field_type: &FieldType) -> tidb_charset::Collation {
    tidb_charset::Collation::from_name(field_type.collation_name()).unwrap_or(tidb_charset::Collation::DEFAULT)
}

/// Go `getDataGenFunc`.
fn data_gen(field_type: &FieldType, i: usize) -> Datum {
    let i = i as i64;
    match field_type.code() {
        FieldTypeCode::LongLong if field_type.is_unsigned() => Datum::UInt(i as u64),
        FieldTypeCode::LongLong => Datum::Int(i),
        FieldTypeCode::Float => Datum::Float32(i as f32 as f64),
        FieldTypeCode::Double => Datum::Real(i as f64),
        FieldTypeCode::Date => Datum::Time(tidb_datatype::time_from_days(i + 365)),
        FieldTypeCode::NewDecimal => Datum::Decimal(Decimal::from_int(i)),
        FieldTypeCode::String => Datum::new_string_datum(i.to_string()),
        FieldTypeCode::Duration => {
            Datum::Duration(MySqlDuration::from_nanoseconds(i, 0).expect("a valid duration"))
        }
        FieldTypeCode::Json => Datum::Json(tidb_datatype::BinaryJSON::parse(&i.to_string()).expect("an integer document")),
        // Go ignores `ParseEnumValue`'s error past the fifth element and
        // stores the zero enum (and set).
        FieldTypeCode::Enum => Datum::Enum(
            tidb_datatype::parse_enum_value(&GEN_ELEMS, i as u64 + 1).unwrap_or_default(),
            collation_of(field_type),
        ),
        FieldTypeCode::Set => Datum::Set(
            tidb_datatype::parse_set_value(&GEN_ELEMS, i as u64 + 1).unwrap_or_default(),
            collation_of(field_type),
        ),
        other => panic!("no data generator for {other:?}"),
    }
}

impl AggTest {
    /// Go `buildAggTester`.
    fn new(func_name: &'static str, key_tp: FieldTypeCode, num_rows: usize, results: Vec<Datum>) -> Self {
        Self::with_field_type(func_name, FieldType::new(key_tp), num_rows, results)
    }

    /// Go `buildAggTesterWithFieldType`.
    fn with_field_type(func_name: &'static str, key_type: FieldType, num_rows: usize, results: Vec<Datum>) -> Self {
        Self {
            key_type,
            num_rows,
            func_name,
            results,
        }
    }

    /// Go `genSrcChk`: `num_rows` generated values and a trailing NULL.
    fn gen_src_chk(&self) -> Chunk {
        let mut src_chk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&self.key_type), self.num_rows);
        for i in 0..self.num_rows {
            src_chk.append_datum(0, &data_gen(&self.key_type, i));
        }
        src_chk.append_datum(0, &Datum::Null);
        src_chk
    }

    fn args(&self) -> Vec<Expression> {
        let mut column = Column::new(1, self.key_type.clone());
        column.index = 0;
        let mut args = vec![Expression::Column(column)];
        if self.func_name == "group_concat" {
            args.push(separator_constant(" "));
        }
        if self.func_name == "approx_percentile" {
            args.push(Expression::Constant(tidb_expr::constant::Constant::new(Datum::Int(50), FieldType::new(FieldTypeCode::Long))));
        }
        args
    }

    fn desc(&self, has_distinct: bool) -> AggFuncDesc {
        AggFuncDesc::new(&NoColumns, self.func_name, self.args(), has_distinct).expect("descriptor builds")
    }
}

/// Go's `dt.Compare(..., &p.results[i], ctor)` is 0.
#[track_caller]
fn assert_datum_eq(actual: &Datum, expected: &Datum, what: &str) {
    // Go `Duration.Compare` compares the nanoseconds alone: a column read
    // under an unspecified fsp carries `-1`, which the numeric comparison
    // refuses.
    if let (Datum::Duration(actual), Datum::Duration(expected)) = (actual, expected) {
        assert_eq!(actual.nanoseconds(), expected.nanoseconds(), "{what}");
        return;
    }
    let ordering = tidb_expr::compare_datums(actual, expected).expect("comparable datums");
    assert_eq!(ordering, std::cmp::Ordering::Equal, "{what}: {actual:?} != {expected:?}");
}

fn update_each_row(func: &dyn AggFunc, src_chk: &Chunk, pr: &mut crate::agg_func::PartialResult) -> Vec<i64> {
    (0..src_chk.num_rows())
        .map(|i| {
            func.update_partial_result(&NoColumns, &[src_chk.get_row(i)], pr)
                .expect("update succeeds")
        })
        .collect()
}

fn final_datum(func: &dyn AggFunc, pr: &mut crate::agg_func::PartialResult, ret_type: &FieldType) -> Datum {
    let mut result_chk = Chunk::new_chunk_with_capacity(std::slice::from_ref(ret_type), 1);
    func.append_final_result_2_chunk(&NoColumns, pr, &mut result_chk)
        .expect("final result appends");
    result_chk.get_row(0).get_datum(0, ret_type)
}

/// Go `testAggFunc`: the result over the rows, over no rows, and with
/// `DISTINCT` over the rows twice.
fn test_agg_func(p: &AggTest) {
    let src_chk = p.gen_src_chk();
    let desc = p.desc(false);
    let final_func = build(&NoColumns, &desc, 0).expect("the function is implemented");
    let (mut final_pr, _) = final_func.alloc_partial_result();
    update_each_row(final_func.as_ref(), &src_chk, &mut final_pr);
    assert_datum_eq(&final_datum(final_func.as_ref(), &mut final_pr, desc.ret_type()), &p.results[1], p.func_name);

    // test the empty input
    final_func.reset_partial_result(&mut final_pr);
    assert_datum_eq(&final_datum(final_func.as_ref(), &mut final_pr, desc.ret_type()), &p.results[0], p.func_name);

    // test the agg func with distinct
    let desc = p.desc(true);
    let final_func = build(&NoColumns, &desc, 0).expect("the function is implemented");
    let (mut final_pr, _) = final_func.alloc_partial_result();
    update_each_row(final_func.as_ref(), &p.gen_src_chk(), &mut final_pr);
    update_each_row(final_func.as_ref(), &p.gen_src_chk(), &mut final_pr);
    assert_datum_eq(&final_datum(final_func.as_ref(), &mut final_pr, desc.ret_type()), &p.results[1], p.func_name);
}

/// Go `testMergePartialResult`: a partial phase over all rows and over all
/// but the first, each merged into the final phase.
fn test_merge_partial_result(p: &AggTest) {
    let desc = p.desc(false);
    let (partial_desc, final_desc) = desc.split(&[0, 1]);
    let partial_func = build(&NoColumns, &partial_desc, 0).expect("the partial function is implemented");
    let (mut partial_result, _) = partial_func.alloc_partial_result();
    let final_func = build(&NoColumns, &final_desc, 0).expect("the final function is implemented");
    let (mut final_pr, _) = final_func.alloc_partial_result();
    // Go reads the partial results as the key type, an
    // APPROX_COUNT_DISTINCT one as a string and its final one as a
    // BIGINT, a JSON_ARRAYAGG one as JSON.
    let (partial_type, final_type) = match p.func_name {
        "approx_count_distinct" => (FieldType::new(FieldTypeCode::String), FieldType::new(FieldTypeCode::LongLong)),
        "json_arrayagg" => (FieldType::new(FieldTypeCode::Json), FieldType::new(FieldTypeCode::Json)),
        _ => (p.key_type.clone(), p.key_type.clone()),
    };

    update_each_row(partial_func.as_ref(), &p.gen_src_chk(), &mut partial_result);
    assert_datum_eq(&final_datum(partial_func.as_ref(), &mut partial_result, &partial_type), &p.results[0], p.func_name);
    final_func
        .merge_partial_result(&NoColumns, &mut partial_result, &mut final_pr)
        .expect("merge succeeds");
    partial_func.reset_partial_result(&mut partial_result);

    // Go's `iter.Begin(); iter.Next()` before the loop's own `iter.Next()`:
    // the first two rows are skipped.
    let src_chk = p.gen_src_chk();
    for i in 2..src_chk.num_rows() {
        partial_func
            .update_partial_result(&NoColumns, &[src_chk.get_row(i)], &mut partial_result)
            .expect("update succeeds");
    }
    assert_datum_eq(&final_datum(partial_func.as_ref(), &mut partial_result, &partial_type), &p.results[1], p.func_name);
    final_func
        .merge_partial_result(&NoColumns, &mut partial_result, &mut final_pr)
        .expect("merge succeeds");
    assert_datum_eq(&final_datum(final_func.as_ref(), &mut final_pr, &final_type), &p.results[2], p.func_name);
}

/// Go `aggMemTest` + `testAggMemFunc`: the bytes the partial result is
/// allocated with and each row's update reports.
fn test_agg_mem_func(p: &AggTest, alloc_mem_delta: i64, update_mem_deltas: &[i64], is_distinct: bool) {
    let desc = p.desc(is_distinct);
    let final_func = build(&NoColumns, &desc, 0).expect("the function is implemented");
    let (mut final_pr, mem_delta) = final_func.alloc_partial_result();
    assert_eq!(mem_delta, alloc_mem_delta, "{} alloc", p.func_name);
    let deltas = update_each_row(final_func.as_ref(), &p.gen_src_chk(), &mut final_pr);
    assert_eq!(deltas, update_mem_deltas, "{} updates", p.func_name);
}

/// Go `defaultUpdateMemDeltaGens`: no row grows the partial result.
fn default_update_mem_deltas(p: &AggTest) -> Vec<i64> {
    vec![0; p.num_rows + 1]
}

/// Go `distinctUpdateMemDeltaGens`: a new key adds the bytes Go stores
/// beside the set (a decimal hash key's or a string's length, and a
/// `MyDecimal` for a `valType` decimal map), a repeat or a NULL nothing;
/// five keys never grow the set's first bucket.
fn distinct_update_mem_deltas(p: &AggTest) -> Vec<i64> {
    distinct_update_mem_deltas_with_value(p, 0)
}

fn distinct_update_mem_deltas_with_value(p: &AggTest, value_bytes: i64) -> Vec<i64> {
    let src_chk = p.gen_src_chk();
    (0..src_chk.num_rows())
        .map(|i| {
            let row = src_chk.get_row(i);
            if row.is_null(0) {
                return 0;
            }
            let key_bytes = match p.key_type.code() {
                FieldTypeCode::NewDecimal => {
                    let Datum::Decimal(value) = row.get_datum(0, &p.key_type) else {
                        unreachable!("a decimal column")
                    };
                    value.to_hash_key().expect("a hash key").0.len() as i64
                }
                FieldTypeCode::String => GoString::from(row.get_bytes(0).to_vec()).len() as i64,
                _ => 0,
            };
            key_bytes + value_bytes
        })
        .collect()
}

const COUNT_TYPES: [FieldTypeCode; 5] = [
    FieldTypeCode::LongLong,
    FieldTypeCode::Double,
    FieldTypeCode::NewDecimal,
    FieldTypeCode::String,
    FieldTypeCode::Duration,
];

/// Go `TestCount`.
#[test]
fn test_count() {
    for tp in COUNT_TYPES {
        test_agg_func(&AggTest::new("count", tp, 5, vec![Datum::Int(0), Datum::Int(5)]));
    }
}

/// Go `TestMergePartialResult4Count`.
#[test]
fn test_merge_partial_result_4_count() {
    test_merge_partial_result(&AggTest::new(
        "count",
        FieldTypeCode::LongLong,
        5,
        vec![Datum::Int(5), Datum::Int(3), Datum::Int(8)],
    ));
}

/// Go `TestMemCount`.
#[test]
fn test_mem_count() {
    use crate::func_count::DEF_PARTIAL_RESULT_4_COUNT_SIZE;
    use crate::func_count_distinct::*;
    use tidb_hack::{DEF_BUCKET_MEMORY_USAGE_FOR_SET_FLOAT64, DEF_BUCKET_MEMORY_USAGE_FOR_SET_INT64, DEF_BUCKET_MEMORY_USAGE_FOR_SET_STRING};
    for tp in COUNT_TYPES {
        let p = AggTest::new("count", tp, 5, Vec::new());
        test_agg_mem_func(&p, DEF_PARTIAL_RESULT_4_COUNT_SIZE, &default_update_mem_deltas(&p), false);
    }
    for (tp, alloc) in [
        (FieldTypeCode::LongLong, DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_INT_SIZE + DEF_BUCKET_MEMORY_USAGE_FOR_SET_INT64 as i64),
        (FieldTypeCode::Double, DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_REAL_SIZE + DEF_BUCKET_MEMORY_USAGE_FOR_SET_FLOAT64 as i64),
        (FieldTypeCode::NewDecimal, DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_DECIMAL_SIZE + DEF_BUCKET_MEMORY_USAGE_FOR_SET_STRING as i64),
        (FieldTypeCode::String, DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_STRING_SIZE + DEF_BUCKET_MEMORY_USAGE_FOR_SET_STRING as i64),
        (FieldTypeCode::Duration, DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_DURATION_SIZE + DEF_BUCKET_MEMORY_USAGE_FOR_SET_INT64 as i64),
    ] {
        let p = AggTest::new("count", tp, 5, Vec::new());
        test_agg_mem_func(&p, alloc, &distinct_update_mem_deltas(&p), true);
    }
}

/// Every Rust size constant is Go's `unsafe.Sizeof` on a 64-bit build
/// (`testdata/go_def_sizes.txt`, printed from `pkg/executor/aggfuncs`).
#[test]
fn def_sizes_are_gos() {
    let go: std::collections::HashMap<&str, i64> = include_str!("../testdata/go_def_sizes.txt")
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace().skip(1);
            Some((parts.next()?, parts.next()?.parse().ok()?))
        })
        .collect();
    for (name, value) in [
        ("DefUint32Size", crate::agg_func::DEF_UINT32_SIZE),
        ("DefUint64Size", crate::agg_func::DEF_UINT64_SIZE),
        ("DefInt64Size", crate::agg_func::DEF_INT64_SIZE),
        ("DefFloat64Size", crate::agg_func::DEF_FLOAT64_SIZE),
        ("DefTimeSize", crate::agg_func::DEF_TIME_SIZE),
        ("DefRowSize", crate::agg_func::DEF_ROW_SIZE),
        ("DefBoolSize", crate::agg_func::DEF_BOOL_SIZE),
        ("DefInterfaceSize", crate::agg_func::DEF_INTERFACE_SIZE),
        ("DefMyDecimalSize", crate::agg_func::DEF_MY_DECIMAL_SIZE),
        ("DefDurationSize", crate::agg_func::DEF_DURATION_SIZE),
        ("DefPartialResult4CountSize", crate::func_count::DEF_PARTIAL_RESULT_4_COUNT_SIZE),
        ("DefPartialResult4CountDistinctIntSize", crate::func_count_distinct::DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_INT_SIZE),
        ("DefPartialResult4CountDistinctRealSize", crate::func_count_distinct::DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_REAL_SIZE),
        ("DefPartialResult4CountDistinctDecimalSize", crate::func_count_distinct::DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_DECIMAL_SIZE),
        ("DefPartialResult4CountDistinctDurationSize", crate::func_count_distinct::DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_DURATION_SIZE),
        ("DefPartialResult4CountDistinctStringSize", crate::func_count_distinct::DEF_PARTIAL_RESULT_4_COUNT_DISTINCT_STRING_SIZE),
        ("DefPartialResult4CountWithDistinctSize", crate::func_count_distinct::DEF_PARTIAL_RESULT_4_COUNT_WITH_DISTINCT_SIZE),
        ("DefPartialResult4SumFloat64Size", crate::func_sum::DEF_PARTIAL_RESULT_4_SUM_FLOAT64_SIZE),
        ("DefPartialResult4SumDecimalSize", crate::func_sum::DEF_PARTIAL_RESULT_4_SUM_DECIMAL_SIZE),
        ("DefPartialResult4SumDistinctFloat64Size", crate::func_sum::DEF_PARTIAL_RESULT_4_SUM_DISTINCT_FLOAT64_SIZE),
        ("DefPartialResult4SumDistinctDecimalSize", crate::func_sum::DEF_PARTIAL_RESULT_4_SUM_DISTINCT_DECIMAL_SIZE),
        ("DefPartialResult4SumInt64Size", crate::func_sum_int::DEF_PARTIAL_RESULT_4_SUM_INT64_SIZE),
        ("DefPartialResult4AvgDecimalSize", crate::func_avg::DEF_PARTIAL_RESULT_4_AVG_DECIMAL_SIZE),
        ("DefPartialResult4FirstRowIntSize", crate::func_first_row::DEF_PARTIAL_RESULT_4_FIRST_ROW_INT_SIZE),
        ("DefPartialResult4FirstRowFloat32Size", crate::func_first_row::DEF_PARTIAL_RESULT_4_FIRST_ROW_FLOAT32_SIZE),
        ("DefPartialResult4FirstRowFloat64Size", crate::func_first_row::DEF_PARTIAL_RESULT_4_FIRST_ROW_FLOAT64_SIZE),
        ("DefPartialResult4FirstRowStringSize", crate::func_first_row::DEF_PARTIAL_RESULT_4_FIRST_ROW_STRING_SIZE),
        ("DefPartialResult4FirstRowTimeSize", crate::func_first_row::DEF_PARTIAL_RESULT_4_FIRST_ROW_TIME_SIZE),
        ("DefPartialResult4FirstRowDurationSize", crate::func_first_row::DEF_PARTIAL_RESULT_4_FIRST_ROW_DURATION_SIZE),
        ("DefPartialResult4FirstRowJSONSize", crate::func_first_row::DEF_PARTIAL_RESULT_4_FIRST_ROW_JSON_SIZE),
        ("DefPartialResult4FirstRowVectorFloat32Size", crate::func_first_row::DEF_PARTIAL_RESULT_4_FIRST_ROW_VECTOR_FLOAT32_SIZE),
        ("DefPartialResult4FirstRowDecimalSize", crate::func_first_row::DEF_PARTIAL_RESULT_4_FIRST_ROW_DECIMAL_SIZE),
        ("DefPartialResult4FirstRowEnumSize", crate::func_first_row::DEF_PARTIAL_RESULT_4_FIRST_ROW_ENUM_SIZE),
        ("DefPartialResult4FirstRowSetSize", crate::func_first_row::DEF_PARTIAL_RESULT_4_FIRST_ROW_SET_SIZE),
        ("DefPartialResult4AvgDistinctDecimalSize", crate::func_avg::DEF_PARTIAL_RESULT_4_AVG_DISTINCT_DECIMAL_SIZE),
        ("DefPartialResult4AvgFloat64Size", crate::func_avg::DEF_PARTIAL_RESULT_4_AVG_FLOAT64_SIZE),
        ("DefPartialResult4AvgDistinctFloat64Size", crate::func_avg::DEF_PARTIAL_RESULT_4_AVG_DISTINCT_FLOAT64_SIZE),
        ("DefPartialResult4SumDistinctInt64Size", crate::func_sum_int::DEF_PARTIAL_RESULT_4_SUM_DISTINCT_INT64_SIZE),
        ("DefPartialResult4SumUint64Size", crate::func_sum_int::DEF_PARTIAL_RESULT_4_SUM_UINT64_SIZE),
        ("DefPartialResult4SumDistinctUint64Size", crate::func_sum_int::DEF_PARTIAL_RESULT_4_SUM_DISTINCT_UINT64_SIZE),
        ("DefPartialResult4MaxMinIntSize", crate::func_max_min::DEF_PARTIAL_RESULT_4_MAX_MIN_INT_SIZE),
        ("DefPartialResult4MaxMinUintSize", crate::func_max_min::DEF_PARTIAL_RESULT_4_MAX_MIN_UINT_SIZE),
        ("DefPartialResult4MaxMinDecimalSize", crate::func_max_min::DEF_PARTIAL_RESULT_4_MAX_MIN_DECIMAL_SIZE),
        ("DefPartialResult4MaxMinFloat32Size", crate::func_max_min::DEF_PARTIAL_RESULT_4_MAX_MIN_FLOAT32_SIZE),
        ("DefPartialResult4MaxMinFloat64Size", crate::func_max_min::DEF_PARTIAL_RESULT_4_MAX_MIN_FLOAT64_SIZE),
        ("DefPartialResult4MaxMinTimeSize", crate::func_max_min::DEF_PARTIAL_RESULT_4_MAX_MIN_TIME_SIZE),
        ("DefPartialResult4MaxMinDurationSize", crate::func_max_min::DEF_PARTIAL_RESULT_4_MAX_MIN_DURATION_SIZE),
        ("DefPartialResult4MaxMinStringSize", crate::func_max_min::DEF_PARTIAL_RESULT_4_MAX_MIN_STRING_SIZE),
        ("DefPartialResult4MaxMinJSONSize", crate::func_max_min::DEF_PARTIAL_RESULT_4_MAX_MIN_JSON_SIZE),
        ("DefPartialResult4MaxMinVectorFloat32Size", crate::func_max_min::DEF_PARTIAL_RESULT_4_MAX_MIN_VECTOR_FLOAT32_SIZE),
        ("DefPartialResult4MaxMinEnumSize", crate::func_max_min::DEF_PARTIAL_RESULT_4_MAX_MIN_ENUM_SIZE),
        ("DefPartialResult4MaxMinSetSize", crate::func_max_min::DEF_PARTIAL_RESULT_4_MAX_MIN_SET_SIZE),
        ("DefMaxMinDequeSize", crate::func_max_min::DEF_MAX_MIN_DEQUE_SIZE),
        ("DefPartialResult4BitFuncSize", crate::func_bitfuncs::DEF_PARTIAL_RESULT_4_BIT_FUNC_SIZE),
        ("DefPartialResult4VarPopFloat64Size", crate::func_varpop::DEF_PARTIAL_RESULT_4_VAR_POP_FLOAT64_SIZE),
        ("DefPartialResult4VarPopDistinctFloat64Size", crate::func_varpop::DEF_PARTIAL_RESULT_4_VAR_POP_DISTINCT_FLOAT64_SIZE),
    ] {
        assert_eq!(Some(&value), go.get(name), "{name}");
    }
}

fn unsigned_longlong() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong).with_unsigned(true)
}

fn decimal(value: i64) -> Datum {
    Datum::Decimal(Decimal::from_int(value))
}

/// Go `TestMergePartialResult4Sum`.
#[test]
fn test_merge_partial_result_4_sum() {
    for test in [
        AggTest::new("sum", FieldTypeCode::NewDecimal, 5, vec![decimal(10), decimal(9), decimal(19)]),
        AggTest::new("sum", FieldTypeCode::Double, 5, vec![Datum::Real(10.0), Datum::Real(9.0), Datum::Real(19.0)]),
        AggTest::new("sum_int", FieldTypeCode::LongLong, 5, vec![Datum::Int(10), Datum::Int(9), Datum::Int(19)]),
        AggTest::with_field_type("sum_int", unsigned_longlong(), 5, vec![Datum::UInt(10), Datum::UInt(9), Datum::UInt(19)]),
    ] {
        test_merge_partial_result(&test);
    }
}

/// Go `TestSum`.
#[test]
fn test_sum() {
    for test in [
        AggTest::new("sum", FieldTypeCode::NewDecimal, 5, vec![Datum::Null, decimal(10)]),
        AggTest::new("sum", FieldTypeCode::Double, 5, vec![Datum::Null, Datum::Real(10.0)]),
        AggTest::new("sum_int", FieldTypeCode::LongLong, 5, vec![Datum::Null, Datum::Int(10)]),
        AggTest::with_field_type("sum_int", unsigned_longlong(), 5, vec![Datum::Null, Datum::UInt(10)]),
    ] {
        test_agg_func(&test);
    }
}

/// Go `TestMemSum`.
#[test]
fn test_mem_sum() {
    use crate::func_sum::*;
    use crate::func_sum_int::*;
    use tidb_hack::{DEF_BUCKET_MEMORY_USAGE_FOR_SET_FLOAT64, DEF_BUCKET_MEMORY_USAGE_FOR_SET_INT64, DEF_BUCKET_MEMORY_USAGE_FOR_SET_STRING};
    for (name, tp, alloc) in [
        ("sum", FieldTypeCode::Double, DEF_PARTIAL_RESULT_4_SUM_FLOAT64_SIZE),
        ("sum", FieldTypeCode::NewDecimal, DEF_PARTIAL_RESULT_4_SUM_DECIMAL_SIZE),
        ("sum_int", FieldTypeCode::LongLong, DEF_PARTIAL_RESULT_4_SUM_INT64_SIZE),
    ] {
        let p = AggTest::new(name, tp, 5, Vec::new());
        test_agg_mem_func(&p, alloc, &default_update_mem_deltas(&p), false);
    }
    let p = AggTest::new("sum", FieldTypeCode::Double, 5, Vec::new());
    test_agg_mem_func(&p, DEF_PARTIAL_RESULT_4_SUM_DISTINCT_FLOAT64_SIZE + DEF_BUCKET_MEMORY_USAGE_FOR_SET_FLOAT64 as i64, &distinct_update_mem_deltas(&p), true);
    // Go's `valType` decimal: each new key also stores a `MyDecimal`.
    let p = AggTest::new("sum", FieldTypeCode::NewDecimal, 5, Vec::new());
    test_agg_mem_func(
        &p,
        DEF_PARTIAL_RESULT_4_SUM_DISTINCT_DECIMAL_SIZE + DEF_BUCKET_MEMORY_USAGE_FOR_SET_STRING as i64,
        &distinct_update_mem_deltas_with_value(&p, tidb_datatype::MYDECIMAL_STRUCT_SIZE as i64),
        true,
    );
    let p = AggTest::new("sum_int", FieldTypeCode::LongLong, 5, Vec::new());
    test_agg_mem_func(&p, DEF_PARTIAL_RESULT_4_SUM_DISTINCT_INT64_SIZE + DEF_BUCKET_MEMORY_USAGE_FOR_SET_INT64 as i64, &distinct_update_mem_deltas(&p), true);
}

fn decimal_text(text: &str) -> Datum {
    Datum::Decimal(Decimal::parse_mysql(text).0)
}

/// Go `TestMergePartialResult4Avg`.
#[test]
fn test_merge_partial_result_4_avg() {
    test_merge_partial_result(&AggTest::new("avg", FieldTypeCode::NewDecimal, 5, vec![decimal_text("2"), decimal_text("3"), decimal_text("2.375")]));
    test_merge_partial_result(&AggTest::new("avg", FieldTypeCode::Double, 5, vec![Datum::Real(2.0), Datum::Real(3.0), Datum::Real(2.375)]));
}

/// Go `TestAvg`.
#[test]
fn test_avg() {
    test_agg_func(&AggTest::new("avg", FieldTypeCode::NewDecimal, 5, vec![Datum::Null, decimal_text("2")]));
    test_agg_func(&AggTest::new("avg", FieldTypeCode::Double, 5, vec![Datum::Null, Datum::Real(2.0)]));
}

/// Go `TestMemAvg`.
#[test]
fn test_mem_avg() {
    use crate::func_avg::*;
    use tidb_hack::{DEF_BUCKET_MEMORY_USAGE_FOR_SET_FLOAT64, DEF_BUCKET_MEMORY_USAGE_FOR_SET_STRING};
    let p = AggTest::new("avg", FieldTypeCode::NewDecimal, 5, Vec::new());
    test_agg_mem_func(&p, DEF_PARTIAL_RESULT_4_AVG_DECIMAL_SIZE, &default_update_mem_deltas(&p), false);
    test_agg_mem_func(
        &p,
        DEF_PARTIAL_RESULT_4_AVG_DISTINCT_DECIMAL_SIZE + DEF_BUCKET_MEMORY_USAGE_FOR_SET_STRING as i64,
        &distinct_update_mem_deltas_with_value(&p, tidb_datatype::MYDECIMAL_STRUCT_SIZE as i64),
        true,
    );
    let p = AggTest::new("avg", FieldTypeCode::Double, 5, Vec::new());
    test_agg_mem_func(&p, DEF_PARTIAL_RESULT_4_AVG_FLOAT64_SIZE, &default_update_mem_deltas(&p), false);
    test_agg_mem_func(&p, DEF_PARTIAL_RESULT_4_AVG_DISTINCT_FLOAT64_SIZE + DEF_BUCKET_MEMORY_USAGE_FOR_SET_FLOAT64 as i64, &distinct_update_mem_deltas(&p), true);
}

/// Go `TestMergePartialResult4FirstRow` (types the generator covers).
#[test]
fn test_merge_partial_result_4_first_row() {
    let date = |days| Datum::Time(tidb_datatype::time_from_days(days));
    let duration = |nanos| Datum::Duration(MySqlDuration::from_nanoseconds(nanos, 0).expect("a duration"));
    for test in [
        AggTest::new("firstrow", FieldTypeCode::LongLong, 5, vec![Datum::Int(0), Datum::Int(2), Datum::Int(0)]),
        AggTest::new("firstrow", FieldTypeCode::Float, 5, vec![Datum::Float32(0.0), Datum::Float32(2.0), Datum::Float32(0.0)]),
        AggTest::new("firstrow", FieldTypeCode::Double, 5, vec![Datum::Real(0.0), Datum::Real(2.0), Datum::Real(0.0)]),
        AggTest::new("firstrow", FieldTypeCode::NewDecimal, 5, vec![decimal(0), decimal(2), decimal(0)]),
        AggTest::new("firstrow", FieldTypeCode::String, 5, vec![Datum::new_string_datum("0"), Datum::new_string_datum("2"), Datum::new_string_datum("0")]),
        AggTest::new("firstrow", FieldTypeCode::Date, 5, vec![date(365), date(367), date(365)]),
        AggTest::new("firstrow", FieldTypeCode::Duration, 5, vec![duration(0), duration(2), duration(0)]),
    ] {
        test_merge_partial_result(&test);
    }
}

/// Go `TestMemFirstRow` (types the generator covers): a string's first value
/// adds its bytes, later rows nothing.
#[test]
fn test_mem_first_row() {
    use crate::func_first_row::*;
    for (tp, alloc) in [
        (FieldTypeCode::LongLong, DEF_PARTIAL_RESULT_4_FIRST_ROW_INT_SIZE),
        (FieldTypeCode::Float, DEF_PARTIAL_RESULT_4_FIRST_ROW_FLOAT32_SIZE),
        (FieldTypeCode::Double, DEF_PARTIAL_RESULT_4_FIRST_ROW_FLOAT64_SIZE),
        (FieldTypeCode::NewDecimal, DEF_PARTIAL_RESULT_4_FIRST_ROW_DECIMAL_SIZE),
        (FieldTypeCode::Date, DEF_PARTIAL_RESULT_4_FIRST_ROW_TIME_SIZE),
        (FieldTypeCode::Duration, DEF_PARTIAL_RESULT_4_FIRST_ROW_DURATION_SIZE),
    ] {
        let p = AggTest::new("firstrow", tp, 5, Vec::new());
        test_agg_mem_func(&p, alloc, &default_update_mem_deltas(&p), false);
    }
    let p = AggTest::new("firstrow", FieldTypeCode::String, 5, Vec::new());
    test_agg_mem_func(&p, DEF_PARTIAL_RESULT_4_FIRST_ROW_STRING_SIZE, &[1, 0, 0, 0, 0, 0], false);
}

/// Every `firstRow4X` reads back what it spilled (Go's
/// `serializePartialResult4FirstRowX` / `deserializePartialResult4FirstRowX`).
#[test]
fn first_row_spills_and_restores() {
    for tp in [FieldTypeCode::LongLong, FieldTypeCode::Double, FieldTypeCode::NewDecimal, FieldTypeCode::String, FieldTypeCode::Date, FieldTypeCode::Duration] {
        let p = AggTest::new("firstrow", tp, 5, Vec::new());
        let desc = p.desc(false);
        let func = build(&NoColumns, &desc, 0).expect("firstrow is implemented");
        let (mut pr, _) = func.alloc_partial_result();
        update_each_row(func.as_ref(), &p.gen_src_chk(), &mut pr);
        let blob = FieldType::new(FieldTypeCode::Blob);
        let mut spill = Chunk::new_chunk_with_capacity(std::slice::from_ref(&blob), 1);
        func.serialize_partial_result(&mut pr, &mut spill, &mut crate::spill_serialize_helper::SerializeHelper::new());
        let (mut restored, mem_delta) = func.deserialize_partial_result(&spill);
        assert_eq!(mem_delta, func.alloc_partial_result().1, "{tp:?}");
        assert_datum_eq(&final_datum(func.as_ref(), &mut restored[0], &p.key_type), &final_datum(func.as_ref(), &mut pr, &p.key_type), "first row spill");
    }
}

fn json(value: i64) -> Datum {
    Datum::Json(tidb_datatype::BinaryJSON::parse(&value.to_string()).expect("an integer document"))
}

fn gen_enum(name: &str) -> Datum {
    let collation = tidb_charset::Collation::DEFAULT;
    Datum::Enum(tidb_datatype::parse_enum(&GEN_ELEMS, name, collation).expect("an element"), collation)
}

fn gen_set(name: &str) -> Datum {
    let collation = tidb_charset::Collation::DEFAULT;
    Datum::Set(tidb_datatype::parse_set(&GEN_ELEMS, name, collation).expect("elements"), collation)
}

/// Go `TestMergePartialResult4MaxMin`.
#[test]
fn test_merge_partial_result_4_max_min() {
    let date = |days| Datum::Time(tidb_datatype::time_from_days(days));
    let duration = |nanos| Datum::Duration(MySqlDuration::from_nanoseconds(nanos, 0).expect("a duration"));
    let string = |text: &str| Datum::new_string_datum(text);
    for test in [
        AggTest::new("max", FieldTypeCode::LongLong, 5, vec![Datum::Int(4), Datum::Int(4), Datum::Int(4)]),
        AggTest::with_field_type("max", unsigned_longlong(), 5, vec![Datum::UInt(4), Datum::UInt(4), Datum::UInt(4)]),
        AggTest::new("max", FieldTypeCode::Float, 5, vec![Datum::Float32(4.0), Datum::Float32(4.0), Datum::Float32(4.0)]),
        AggTest::new("max", FieldTypeCode::Double, 5, vec![Datum::Real(4.0), Datum::Real(4.0), Datum::Real(4.0)]),
        AggTest::new("max", FieldTypeCode::NewDecimal, 5, vec![decimal(4), decimal(4), decimal(4)]),
        AggTest::new("max", FieldTypeCode::String, 5, vec![string("4"), string("4"), string("4")]),
        AggTest::new("max", FieldTypeCode::Date, 5, vec![date(369), date(369), date(369)]),
        AggTest::new("max", FieldTypeCode::Duration, 5, vec![duration(4), duration(4), duration(4)]),
        AggTest::new("max", FieldTypeCode::Json, 5, vec![json(4), json(4), json(4)]),
        AggTest::new("max", FieldTypeCode::Enum, 5, vec![gen_enum("e"), gen_enum("c"), gen_enum("e")]),
        AggTest::new("max", FieldTypeCode::Set, 5, vec![gen_set("e,d"), gen_set("e,d"), gen_set("e,d")]),
        AggTest::new("min", FieldTypeCode::LongLong, 5, vec![Datum::Int(0), Datum::Int(2), Datum::Int(0)]),
        AggTest::with_field_type("min", unsigned_longlong(), 5, vec![Datum::UInt(0), Datum::UInt(2), Datum::UInt(0)]),
        AggTest::new("min", FieldTypeCode::Float, 5, vec![Datum::Float32(0.0), Datum::Float32(2.0), Datum::Float32(0.0)]),
        AggTest::new("min", FieldTypeCode::Double, 5, vec![Datum::Real(0.0), Datum::Real(2.0), Datum::Real(0.0)]),
        AggTest::new("min", FieldTypeCode::NewDecimal, 5, vec![decimal(0), decimal(2), decimal(0)]),
        AggTest::new("min", FieldTypeCode::String, 5, vec![string("0"), string("2"), string("0")]),
        AggTest::new("min", FieldTypeCode::Date, 5, vec![date(365), date(367), date(365)]),
        AggTest::new("min", FieldTypeCode::Duration, 5, vec![duration(0), duration(2), duration(0)]),
        AggTest::new("min", FieldTypeCode::Json, 5, vec![json(0), json(2), json(0)]),
        AggTest::new("min", FieldTypeCode::Enum, 5, vec![gen_enum("a"), gen_enum("a"), gen_enum("a")]),
        AggTest::new("min", FieldTypeCode::Set, 5, vec![gen_set("c"), gen_set("c"), gen_set("c")]),
    ] {
        test_merge_partial_result(&test);
    }
}

/// Go `TestMaxMin`.
#[test]
fn test_max_min() {
    let date = |days| Datum::Time(tidb_datatype::time_from_days(days));
    let duration = |nanos| Datum::Duration(MySqlDuration::from_nanoseconds(nanos, 0).expect("a duration"));
    for test in [
        AggTest::new("max", FieldTypeCode::LongLong, 5, vec![Datum::Null, Datum::Int(4)]),
        AggTest::with_field_type("max", unsigned_longlong(), 5, vec![Datum::Null, Datum::UInt(4)]),
        AggTest::new("max", FieldTypeCode::Float, 5, vec![Datum::Null, Datum::Float32(4.0)]),
        AggTest::new("max", FieldTypeCode::Double, 5, vec![Datum::Null, Datum::Real(4.0)]),
        AggTest::new("max", FieldTypeCode::NewDecimal, 5, vec![Datum::Null, decimal(4)]),
        AggTest::new("max", FieldTypeCode::String, 5, vec![Datum::Null, Datum::new_string_datum("4"), Datum::new_string_datum("4")]),
        AggTest::new("max", FieldTypeCode::Date, 5, vec![Datum::Null, date(369)]),
        AggTest::new("max", FieldTypeCode::Duration, 5, vec![Datum::Null, duration(4)]),
        AggTest::new("max", FieldTypeCode::Json, 5, vec![Datum::Null, json(4)]),
        AggTest::new("min", FieldTypeCode::LongLong, 5, vec![Datum::Null, Datum::Int(0)]),
        AggTest::with_field_type("min", unsigned_longlong(), 5, vec![Datum::Null, Datum::UInt(0)]),
        AggTest::new("min", FieldTypeCode::Float, 5, vec![Datum::Null, Datum::Float32(0.0)]),
        AggTest::new("min", FieldTypeCode::Double, 5, vec![Datum::Null, Datum::Real(0.0)]),
        AggTest::new("min", FieldTypeCode::NewDecimal, 5, vec![Datum::Null, decimal(0)]),
        AggTest::new("min", FieldTypeCode::String, 5, vec![Datum::Null, Datum::new_string_datum("0")]),
        AggTest::new("min", FieldTypeCode::Date, 5, vec![Datum::Null, date(365)]),
        AggTest::new("min", FieldTypeCode::Duration, 5, vec![Datum::Null, duration(0)]),
        AggTest::new("min", FieldTypeCode::Json, 5, vec![Datum::Null, json(0)]),
    ] {
        test_agg_func(&test);
    }
}

/// Go `maxMinUpdateMemDeltaGens`: the first value adds its bytes, a
/// replacement the difference -- Go orders the expected values by their
/// raw bytes (a JSON document's `Value`, an enum's or set's name).
fn max_min_update_mem_deltas(p: &AggTest, is_max: bool) -> Vec<i64> {
    let src_chk = p.gen_src_chk();
    let mut pre: Vec<u8> = Vec::new();
    (0..src_chk.num_rows())
        .map(|i| {
            let row = src_chk.get_row(i);
            if row.is_null(0) {
                return 0;
            }
            let cur = match row.get_datum(0, &p.key_type) {
                Datum::String(text) => text.bytes().to_vec(),
                Datum::Bytes(bytes) => bytes,
                Datum::Json(value) => value.value().to_vec(),
                Datum::Enum(value, _) => value.name_bytes().to_vec(),
                Datum::Set(value, _) => value.name_bytes().to_vec(),
                other => panic!("no variable-size value in {other:?}"),
            };
            if i == 0 {
                let delta = cur.len() as i64;
                pre = cur;
                return delta;
            }
            if (is_max && cur > pre) || (!is_max && cur < pre) {
                let delta = cur.len() as i64 - pre.len() as i64;
                pre = cur;
                return delta;
            }
            0
        })
        .collect()
}

/// Go `TestMemMaxMin`.
#[test]
fn test_mem_max_min() {
    use crate::func_max_min::*;
    for name in ["max", "min"] {
        for (key_type, alloc) in [
            (FieldType::new(FieldTypeCode::LongLong), DEF_PARTIAL_RESULT_4_MAX_MIN_INT_SIZE),
            (unsigned_longlong(), DEF_PARTIAL_RESULT_4_MAX_MIN_UINT_SIZE),
            (FieldType::new(FieldTypeCode::NewDecimal), DEF_PARTIAL_RESULT_4_MAX_MIN_DECIMAL_SIZE),
            (FieldType::new(FieldTypeCode::Float), DEF_PARTIAL_RESULT_4_MAX_MIN_FLOAT32_SIZE),
            (FieldType::new(FieldTypeCode::Double), DEF_PARTIAL_RESULT_4_MAX_MIN_FLOAT64_SIZE),
            (FieldType::new(FieldTypeCode::Date), DEF_PARTIAL_RESULT_4_MAX_MIN_TIME_SIZE),
            (FieldType::new(FieldTypeCode::Duration), DEF_PARTIAL_RESULT_4_MAX_MIN_DURATION_SIZE),
        ] {
            let p = AggTest::with_field_type(name, key_type, 5, Vec::new());
            test_agg_mem_func(&p, alloc, &default_update_mem_deltas(&p), false);
        }
        for (tp, alloc) in [
            (FieldTypeCode::String, DEF_PARTIAL_RESULT_4_MAX_MIN_STRING_SIZE),
            (FieldTypeCode::Json, DEF_PARTIAL_RESULT_4_MAX_MIN_JSON_SIZE),
            (FieldTypeCode::Enum, DEF_PARTIAL_RESULT_4_MAX_MIN_ENUM_SIZE),
            (FieldTypeCode::Set, DEF_PARTIAL_RESULT_4_MAX_MIN_SET_SIZE),
        ] {
            let p = AggTest::new(name, tp, 99, Vec::new());
            test_agg_mem_func(&p, alloc, &max_min_update_mem_deltas(&p, name == "max"), false);
        }
    }
}

/// Every `maxMin4X` but the vector one reads back what it spilled (Go's
/// `serializePartialResult4MaxMinX` / `deserializePartialResult4MaxMinX`).
#[test]
fn max_min_spills_and_restores() {
    for name in ["max", "min"] {
        for key_type in [
            FieldType::new(FieldTypeCode::LongLong),
            unsigned_longlong(),
            FieldType::new(FieldTypeCode::Float),
            FieldType::new(FieldTypeCode::Double),
            FieldType::new(FieldTypeCode::NewDecimal),
            FieldType::new(FieldTypeCode::String),
            FieldType::new(FieldTypeCode::Date),
            FieldType::new(FieldTypeCode::Duration),
            FieldType::new(FieldTypeCode::Json),
            FieldType::new(FieldTypeCode::Enum),
            FieldType::new(FieldTypeCode::Set),
        ] {
            let p = AggTest::with_field_type(name, key_type, 5, Vec::new());
            let func = build(&NoColumns, &p.desc(false), 0).expect("max/min is implemented");
            let blob = FieldType::new(FieldTypeCode::Blob);
            for rows in [0, 5] {
                let (mut pr, _) = func.alloc_partial_result();
                let src_chk = p.gen_src_chk();
                for i in 0..rows {
                    func.update_partial_result(&NoColumns, &[src_chk.get_row(i)], &mut pr).expect("update succeeds");
                }
                let mut spill = Chunk::new_chunk_with_capacity(std::slice::from_ref(&blob), 1);
                func.serialize_partial_result(&mut pr, &mut spill, &mut crate::spill_serialize_helper::SerializeHelper::new());
                let (mut restored, mem_delta) = func.deserialize_partial_result(&spill);
                assert_eq!(mem_delta, func.alloc_partial_result().1, "{name} {:?}", p.key_type.code());
                let (expected, actual) = (final_datum(func.as_ref(), &mut pr, &p.key_type), final_datum(func.as_ref(), &mut restored[0], &p.key_type));
                if rows == 0 {
                    assert!(expected.is_null() && actual.is_null(), "{name} {:?}", p.key_type.code());
                } else {
                    assert_datum_eq(&actual, &expected, "max/min spill");
                }
            }
        }
    }
}

/// Go `TestMergePartialResult4BitFuncs`.
#[test]
fn test_merge_partial_result_4_bit_funcs() {
    for test in [
        AggTest::new("bit_and", FieldTypeCode::LongLong, 5, vec![Datum::UInt(0), Datum::UInt(0), Datum::UInt(0)]),
        AggTest::new("bit_or", FieldTypeCode::LongLong, 5, vec![Datum::UInt(7), Datum::UInt(7), Datum::UInt(7)]),
        AggTest::new("bit_xor", FieldTypeCode::LongLong, 5, vec![Datum::UInt(4), Datum::UInt(5), Datum::UInt(1)]),
    ] {
        test_merge_partial_result(&test);
    }
}

/// Go `TestMemBitFunc`.
#[test]
fn test_mem_bit_func() {
    for name in ["bit_and", "bit_or", "bit_xor"] {
        let p = AggTest::new(name, FieldTypeCode::LongLong, 5, Vec::new());
        test_agg_mem_func(&p, crate::func_bitfuncs::DEF_PARTIAL_RESULT_4_BIT_FUNC_SIZE, &default_update_mem_deltas(&p), false);
    }
}

/// `BIT_AND` starts from all ones, so over no rows (or only NULLs) it
/// answers `18446744073709551615` (Go `math.MaxUint64`).
#[test]
fn bit_funcs_over_no_rows_answer_their_starting_values() {
    for (name, expected) in [("bit_and", u64::MAX), ("bit_or", 0), ("bit_xor", 0)] {
        let p = AggTest::new(name, FieldTypeCode::LongLong, 0, Vec::new());
        let func = build(&NoColumns, &p.desc(false), 0).expect("implemented");
        let (mut pr, _) = func.alloc_partial_result();
        update_each_row(func.as_ref(), &p.gen_src_chk(), &mut pr);
        let ret_type = unsigned_longlong();
        assert_datum_eq(&final_datum(func.as_ref(), &mut pr, &ret_type), &Datum::UInt(expected), name);
    }
}

/// Go `TestMergePartialResult4Varpop`, `...4Varsamp`, `...4Stddevpop` and
/// `...4Stddevsamp`.
#[test]
fn test_merge_partial_result_4_variances() {
    for test in [
        AggTest::new("var_pop", FieldTypeCode::Double, 5, vec![Datum::Real(2.0), Datum::Real(2.0 / 3.0), Datum::Real(59.0 / 8.0 - (19.0 * 19.0) / (8.0 * 8.0))]),
        AggTest::new("var_samp", FieldTypeCode::Double, 5, vec![Datum::Real(2.5), Datum::Real(1.0), Datum::Real(1.9821428571428572)]),
        AggTest::new("stddev_pop", FieldTypeCode::Double, 5, vec![Datum::Real(1.4142135623730951), Datum::Real(0.816496580927726), Datum::Real(1.3169567191065923)]),
        AggTest::new("stddev_samp", FieldTypeCode::Double, 5, vec![Datum::Real(1.5811388300841898), Datum::Real(1.0), Datum::Real(1.407885953173359)]),
    ] {
        test_merge_partial_result(&test);
    }
}

/// Go `TestVarpop`, `TestVarsamp`, `TestStddevpop` and `TestStddevsamp`.
#[test]
fn test_variances() {
    for test in [
        AggTest::new("var_pop", FieldTypeCode::Double, 5, vec![Datum::Null, Datum::Real(2.0)]),
        AggTest::new("var_samp", FieldTypeCode::Double, 5, vec![Datum::Null, Datum::Real(2.5)]),
        AggTest::new("stddev_pop", FieldTypeCode::Double, 5, vec![Datum::Null, Datum::Real(1.4142135623730951)]),
        AggTest::new("stddev_samp", FieldTypeCode::Double, 5, vec![Datum::Null, Datum::Real(1.5811388300841898)]),
    ] {
        test_agg_func(&test);
    }
}

/// Go `TestMemVarpop`.
#[test]
fn test_mem_varpop() {
    use crate::func_varpop::*;
    let p = AggTest::new("var_pop", FieldTypeCode::Double, 5, Vec::new());
    test_agg_mem_func(&p, DEF_PARTIAL_RESULT_4_VAR_POP_FLOAT64_SIZE, &default_update_mem_deltas(&p), false);
    test_agg_mem_func(&p, DEF_PARTIAL_RESULT_4_VAR_POP_DISTINCT_FLOAT64_SIZE + tidb_hack::DEF_BUCKET_MEMORY_USAGE_FOR_SET_FLOAT64 as i64, &distinct_update_mem_deltas(&p), true);
}

/// A sample variance over one row is NULL, a population one `0` (Go's
/// `p.count <= 1` against `p.count == 0`).
#[test]
fn a_sample_variance_over_one_row_is_null() {
    for (name, expected) in [("var_pop", Datum::Real(0.0)), ("stddev_pop", Datum::Real(0.0)), ("var_samp", Datum::Null), ("stddev_samp", Datum::Null)] {
        for distinct in [false, true] {
            let p = AggTest::new(name, FieldTypeCode::Double, 1, Vec::new());
            let func = build(&NoColumns, &p.desc(distinct), 0).expect("implemented");
            let (mut pr, _) = func.alloc_partial_result();
            update_each_row(func.as_ref(), &p.gen_src_chk(), &mut pr);
            let actual = final_datum(func.as_ref(), &mut pr, &p.key_type);
            if expected.is_null() {
                assert!(actual.is_null(), "{name} distinct={distinct}: {actual:?}");
            } else {
                assert_datum_eq(&actual, &expected, name);
            }
        }
    }
}

/// Go `TestMergePartialResult4GroupConcat`.
#[test]
fn test_merge_partial_result_4_group_concat() {
    let s = Datum::new_string_datum;
    test_merge_partial_result(&AggTest::new("group_concat", FieldTypeCode::String, 5, vec![s("0 1 2 3 4"), s("2 3 4"), s("0 1 2 3 4 2 3 4")]));
}

/// A statement context for GROUP_CONCAT: its `group_concat_max_len`, its
/// truncation policy (Go `mock.NewContext()` has neither truncation flag,
/// so a cut fails), and the warnings it keeps.
struct GroupConcatCtx {
    max_len: u64,
    level: tidb_exprctx::ErrorLevel,
    warnings: std::cell::RefCell<Vec<u16>>,
}

impl GroupConcatCtx {
    fn new(max_len: u64) -> Self {
        Self { max_len, level: tidb_exprctx::ErrorLevel::Error, warnings: std::cell::RefCell::new(Vec::new()) }
    }
}

impl tidb_exprctx::Columns for GroupConcatCtx {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn sysvar(&self, _scope: Option<tidb_ast::SysVarScope>, name: &str) -> Option<Datum> {
        (name == "group_concat_max_len").then_some(Datum::UInt(self.max_len))
    }

    fn truncate_level(&self) -> tidb_exprctx::ErrorLevel {
        self.level
    }

    fn append_warning(&self, code: u16, _message: &str) {
        self.warnings.borrow_mut().push(code);
    }
}

/// One GROUP_CONCAT over string columns: its alloc delta, each row's delta
/// (and whether it failed), and the result. `None` cells are NULL.
fn run_group_concat(ctx: &GroupConcatCtx, rows: &[Vec<Option<&str>>], sep: &str, distinct: bool, order: bool) -> (i64, Vec<(i64, bool)>, Option<String>) {
    let string = FieldType::new(FieldTypeCode::String);
    let tps = vec![string.clone(); rows[0].len()];
    let mut chk = Chunk::new_chunk_with_capacity(&tps, rows.len());
    for row in rows {
        for (j, cell) in row.iter().enumerate() {
            chk.append_datum(j, &cell.map_or(Datum::Null, Datum::new_string_datum));
        }
    }
    let mut args: Vec<Expression> = (0..tps.len())
        .map(|k| {
            let mut column = Column::new(k as i64 + 1, string.clone());
            column.index = k as i64;
            Expression::Column(column)
        })
        .collect();
    args.push(separator_constant(sep));
    let mut desc = AggFuncDesc::new(ctx, "group_concat", args.clone(), distinct).expect("descriptor builds");
    if order {
        desc.order_by_items = vec![tidb_expression_aggregation::ByItems::new(args[0].clone(), true)];
    }
    let func = build(ctx, &desc, 0).expect("group_concat is implemented");
    let (mut pr, alloc) = func.alloc_partial_result();
    let deltas = (0..chk.num_rows())
        .map(|i| match func.update_partial_result(ctx, &[chk.get_row(i)], &mut pr) {
            Ok(delta) => (delta, false),
            Err(_) => (go_error_delta(func.as_ref(), ctx, &chk, i), true),
        })
        .collect::<Vec<_>>();
    let ret_type = desc.base.ret_type.clone();
    let mut res = Chunk::new_chunk_with_capacity(std::slice::from_ref(&ret_type), 1);
    func.append_final_result_2_chunk(ctx, &mut pr, &mut res).expect("final result appends");
    let row = res.get_row(0);
    let out = (!row.is_null(0)).then(|| String::from_utf8(row.get_bytes(0).to_vec()).expect("utf8"));
    (alloc, deltas, out)
}

/// Go returns `(memDelta, err)` together; a Rust error carries no delta,
/// so a failed update's delta is not comparable. The probes below record a
/// failure by its flag alone.
fn go_error_delta(_func: &dyn AggFunc, _ctx: &GroupConcatCtx, _chk: &Chunk, _row: usize) -> i64 {
    -1
}

fn gc_rows(values: &[&[&'static str]]) -> Vec<Vec<Option<&'static str>>> {
    values.iter().map(|row| row.iter().map(|v| (*v != "NULL").then_some(*v)).collect()).collect()
}

fn ok(deltas: &[i64]) -> Vec<(i64, bool)> {
    deltas.iter().map(|d| (*d, false)).collect()
}

/// Go's GROUP_CONCAT over two string columns, captured from Go's
/// `aggfuncs.Build` (alloc, per-row deltas, result) -- Go
/// `TestMemGroupConcat` and `TestGroupConcat` with exact values.
#[test]
fn group_concat_charges_and_answers_like_go() {
    let ctx = GroupConcatCtx::new(1024);
    let rows = gc_rows(&[&["0", "0"], &["1", "1"], &["2", "2"], &["3", "3"], &["4", "4"], &["NULL", "NULL"]]);
    let cases: [(bool, bool, i64, &[i64], &str); 4] = [
        (false, false, 56, &[168, 0, 0, 0, 0, 0], "00 11 22 33 44"),
        (false, true, 144, &[148, 148, 148, 148, 148, 0], "44 33 22 11 00"),
        (true, false, 424, &[102, 20, 20, 20, 20, 0], "00 11 22 33 44"),
        (true, true, 456, &[184, 166, 166, 166, 166, 0], "44 33 22 11 00"),
    ];
    for (distinct, order, alloc, deltas, result) in cases {
        let (got_alloc, got_deltas, got) = run_group_concat(&ctx, &rows, " ", distinct, order);
        assert_eq!((got_alloc, got_deltas), (alloc, ok(deltas)), "distinct={distinct} order={order}");
        let got = got.expect("a result");
        if distinct && !order {
            // Go iterates its map in random order.
            let mut parts: Vec<&str> = got.split(' ').collect();
            parts.sort_unstable();
            assert_eq!(parts.join(" "), result);
        } else {
            assert_eq!(got, result, "distinct={distinct} order={order}");
        }
    }
}

/// Forty values grow the buffers past one size class (Go's deltas).
#[test]
fn a_long_group_concat_grows_like_go() {
    let ctx = GroupConcatCtx::new(1024);
    let values: Vec<String> = (0..40).map(|i| format!("v{}", i * 7)).collect();
    let rows: Vec<Vec<Option<&str>>> = values.iter().map(|v| vec![Some(v.as_str())]).collect();
    let mut plain = vec![0; 40];
    plain[0] = 168;
    plain[16] = 64;
    plain[29] = 128;
    let order: Vec<i64> = (0..40).map(|i| match i { 0 | 1 => 149, 2..=14 => 150, _ => 151 }).collect();
    let distinct: Vec<i64> = (0..40).map(|i| match i { 0 => 84, 1 => 11, 2..=14 => 12, 15 => 562, 31 => 875, _ => 13 }).collect();
    let distinct_order: Vec<i64> = (0..40).map(|i| match i { 0 => 167, 1 => 158, 2..=14 => 159, 15 => 564, 31 => 813, _ => 160 }).collect();
    let ordered = "v98,v91,v84,v77,v70,v7,v63,v56,v49,v42,v35,v28,v273,v266,v259,v252,v245,v238,v231,v224,v217,v210,v21,v203,v196,v189,v182,v175,v168,v161,v154,v147,v140,v14,v133,v126,v119,v112,v105,v0";
    let in_order = values.join(",");
    for (distinct_flag, order_flag, alloc, deltas, result) in [
        (false, false, 56, plain, in_order.as_str()),
        (false, true, 144, order, ordered),
        (true, false, 424, distinct, in_order.as_str()),
        (true, true, 456, distinct_order, ordered),
    ] {
        let (got_alloc, got_deltas, got) = run_group_concat(&ctx, &rows, ",", distinct_flag, order_flag);
        assert_eq!((got_alloc, got_deltas), (alloc, ok(&deltas)), "distinct={distinct_flag} order={order_flag}");
        let got = got.expect("a result");
        if distinct_flag && !order_flag {
            let mut parts: Vec<&str> = got.split(',').collect();
            let mut expected: Vec<&str> = result.split(',').collect();
            parts.sort_unstable();
            expected.sort_unstable();
            assert_eq!(parts, expected);
        } else {
            assert_eq!(got, result, "distinct={distinct_flag} order={order_flag}");
        }
    }
}

/// `group_concat_max_len` 4..7 (Go `TestGroupConcat`'s loop, with Go's
/// deltas): the first cut fails the strict statement once, and the ORDER
/// BY result keeps the first rows in order, then part of a separator.
#[test]
fn a_group_concat_past_its_max_len_is_cut_like_go() {
    let rows = gc_rows(&[&["0", "0"], &["1", "1"], &["2", "2"], &["3", "3"], &["4", "4"], &["NULL", "NULL"]]);
    let failed = |deltas: &[i64], at: usize| -> Vec<(i64, bool)> {
        deltas.iter().enumerate().map(|(i, d)| if i == at { (-1, true) } else { (*d, false) }).collect()
    };
    // Go's lines: a delta marked `!` came back with the error.
    for (max_len, order_deltas, order_at, order_result, distinct_deltas, distinct_at, plain_at, plain_result) in [
        (4, [148, 148, 0, 0, 0, 0], 1, "44 3", [184, 166, 18, 18, 18, 0], 1, 1, "00 1"),
        (5, [148, 148, 0, 0, 0, 0], 2, "44 33", [184, 166, 18, 18, 18, 0], 2, 2, "00 11"),
        (6, [148, 148, 0, 0, 0, 0], 2, "44 33 ", [184, 166, 18, 18, 18, 0], 2, 2, "00 11 "),
        (7, [148, 148, 148, 0, 0, 0], 2, "44 33 2", [184, 166, 166, 18, 18, 0], 2, 2, "00 11 2"),
    ] {
        let ctx = GroupConcatCtx::new(max_len);
        let (_, deltas, got) = run_group_concat(&ctx, &rows, " ", false, true);
        assert_eq!(deltas, failed(&order_deltas, order_at), "order max_len={max_len}");
        assert_eq!(got.as_deref(), Some(order_result), "order max_len={max_len}");
        let (_, deltas, got) = run_group_concat(&ctx, &rows, " ", true, true);
        assert_eq!(deltas, failed(&distinct_deltas, distinct_at), "distinct order max_len={max_len}");
        assert_eq!(got.as_deref(), Some(order_result), "distinct order max_len={max_len}");
        let (_, deltas, got) = run_group_concat(&ctx, &rows, " ", false, false);
        assert_eq!(deltas, failed(&[168, 0, 0, 0, 0, 0], plain_at), "plain max_len={max_len}");
        assert_eq!(got.as_deref(), Some(plain_result), "plain max_len={max_len}");
    }
}

/// Go's `topNRows` comment: with `group_concat_max_len` 10 and '---', the
/// popped row leaves one '-' (`isSepTruncated`).
#[test]
fn a_cut_separator_is_kept_like_go() {
    let ctx = GroupConcatCtx::new(10);
    let (_, deltas, got) = run_group_concat(&ctx, &gc_rows(&[&["aaa"], &["bbb"], &["ccc"]]), "---", false, false);
    assert_eq!((deltas, got.as_deref()), (vec![(168, false), (0, false), (-1, true)], Some("aaa---bbb-")));
    let (_, deltas, got) = run_group_concat(&ctx, &gc_rows(&[&["ccc"], &["bbb"], &["aaa"]]), "---", false, true);
    assert_eq!((deltas, got.as_deref()), (vec![(150, false), (150, false), (-1, true)], Some("ccc---bbb-")));
    let (_, deltas, got) = run_group_concat(&ctx, &gc_rows(&[&["ccc"], &["bbb"], &["aaa"], &["ddd"]]), "---", true, true);
    assert_eq!((deltas, got.as_deref()), (vec![(168, false), (159, false), (-1, true), (9, false)], Some("ddd---ccc-")));
}

/// Where truncation is a warning, the first cut warns 1260 once per
/// function, and later cuts are silent.
#[test]
fn a_cut_warns_once_where_truncation_is_a_warning() {
    let mut ctx = GroupConcatCtx::new(4);
    ctx.level = tidb_exprctx::ErrorLevel::Warn;
    let rows = gc_rows(&[&["0", "0"], &["1", "1"], &["2", "2"], &["3", "3"], &["4", "4"], &["NULL", "NULL"]]);
    let (_, deltas, got) = run_group_concat(&ctx, &rows, " ", false, false);
    assert!(deltas.iter().all(|(_, failed)| !failed), "{deltas:?}");
    assert_eq!(got.as_deref(), Some("00 1"));
    assert_eq!(*ctx.warnings.borrow(), vec![1260]);
}

/// A GROUP_CONCAT partial result reads back what it spilled, a nil buffer
/// as nil.
#[test]
fn group_concat_spills_and_restores() {
    let p = AggTest::new("group_concat", FieldTypeCode::String, 5, Vec::new());
    let func = build(&NoColumns, &p.desc(false), 0).expect("group_concat is implemented");
    let blob = FieldType::new(FieldTypeCode::Blob);
    let ret_type = p.desc(false).base.ret_type.clone();
    for rows in [0, 5] {
        let (mut pr, _) = func.alloc_partial_result();
        let src_chk = p.gen_src_chk();
        for i in 0..rows {
            func.update_partial_result(&NoColumns, &[src_chk.get_row(i)], &mut pr).expect("update succeeds");
        }
        let mut spill = Chunk::new_chunk_with_capacity(std::slice::from_ref(&blob), 1);
        func.serialize_partial_result(&mut pr, &mut spill, &mut crate::spill_serialize_helper::SerializeHelper::new());
        let (mut restored, _) = func.deserialize_partial_result(&spill);
        let (expected, actual) = (final_datum(func.as_ref(), &mut pr, &ret_type), final_datum(func.as_ref(), &mut restored[0], &ret_type));
        if rows == 0 {
            assert!(expected.is_null() && actual.is_null());
        } else {
            assert_datum_eq(&actual, &expected, "group_concat spill");
        }
    }
}

/// Go `getJSONValue` of the generator's `i`th value, as `CreateBinaryJSON`
/// encodes it.
fn go_json_value(tp: FieldTypeCode, i: i64) -> tidb_datatype::BinaryJSONValue {
    use tidb_datatype::BinaryJSONValue as V;
    match tp {
        FieldTypeCode::LongLong => V::Int64(i),
        FieldTypeCode::Double | FieldTypeCode::Float => V::Float64(i as f64),
        FieldTypeCode::String => V::String(i.to_string()),
        FieldTypeCode::Json => V::Binary(tidb_datatype::BinaryJSON::parse(&i.to_string()).expect("a document")),
        FieldTypeCode::Date => V::Time(tidb_datatype::time_from_days(i + 365)),
        FieldTypeCode::Duration => V::Duration(MySqlDuration::from_nanoseconds(i, 0).expect("a duration")),
        FieldTypeCode::NewDecimal => V::Float64(i as f64),
        other => panic!("no JSON value for {other:?}"),
    }
}

fn json_array(values: Vec<tidb_datatype::BinaryJSONValue>) -> Datum {
    Datum::Json(tidb_datatype::BinaryJSON::from_typed_value(&tidb_datatype::BinaryJSONValue::Array(values)).expect("an array"))
}

const JSON_ARRAYAGG_TYPES: [FieldTypeCode; 7] = [
    FieldTypeCode::LongLong,
    FieldTypeCode::Double,
    FieldTypeCode::Float,
    FieldTypeCode::String,
    FieldTypeCode::Json,
    FieldTypeCode::Date,
    FieldTypeCode::Duration,
];

/// Go `TestMergePartialResult4JsonArrayagg`: the trailing NULL row lands
/// in each array as JSON `null`.
#[test]
fn test_merge_partial_result_4_json_arrayagg() {
    use tidb_datatype::BinaryJSONValue as V;
    for tp in JSON_ARRAYAGG_TYPES {
        let mut entries1: Vec<V> = (0..5).map(|m| go_json_value(tp, m)).collect();
        entries1.push(V::Null);
        let mut entries2: Vec<V> = (2..5).map(|m| go_json_value(tp, m)).collect();
        entries2.push(V::Null);
        let entries3: Vec<V> = entries1.iter().chain(entries2.iter()).cloned().collect();
        test_merge_partial_result(&AggTest::new("json_arrayagg", tp, 5, vec![json_array(entries1), json_array(entries2), json_array(entries3)]));
    }
}

/// Go `TestJsonArrayagg` (`testAggFuncWithoutDistinct`).
#[test]
fn test_json_arrayagg() {
    use tidb_datatype::BinaryJSONValue as V;
    for tp in JSON_ARRAYAGG_TYPES {
        let mut entries: Vec<V> = (0..5).map(|m| go_json_value(tp, m)).collect();
        entries.push(V::Null);
        let p = AggTest::new("json_arrayagg", tp, 5, Vec::new());
        let desc = p.desc(false);
        let func = build(&NoColumns, &desc, 0).expect("json_arrayagg is implemented");
        let (mut pr, _) = func.alloc_partial_result();
        update_each_row(func.as_ref(), &p.gen_src_chk(), &mut pr);
        assert_datum_eq(&final_datum(func.as_ref(), &mut pr, &desc.base.ret_type), &json_array(entries), "json_arrayagg");
        func.reset_partial_result(&mut pr);
        assert!(final_datum(func.as_ref(), &mut pr, &desc.base.ret_type).is_null());
    }
}

/// Go `TestMemJsonArrayagg`: each row adds an interface and its value --
/// a NULL row the interface alone.
#[test]
fn test_mem_json_arrayagg() {
    use crate::func_json_arrayagg::{DEF_PARTIAL_RESULT_4_JSON_ARRAYAGG, DEF_SLICE_SIZE};
    for tp in [FieldTypeCode::LongLong, FieldTypeCode::Double, FieldTypeCode::String, FieldTypeCode::Json, FieldTypeCode::Duration, FieldTypeCode::NewDecimal, FieldTypeCode::Date] {
        let p = AggTest::new("json_arrayagg", tp, 5, Vec::new());
        let src_chk = p.gen_src_chk();
        let deltas: Vec<i64> = (0..src_chk.num_rows())
            .map(|i| {
                let row = src_chk.get_row(i);
                if row.is_null(0) {
                    return 16;
                }
                16 + match tp {
                    FieldTypeCode::String => row.get_bytes(0).len() as i64,
                    FieldTypeCode::Json => match row.get_datum(0, &p.key_type) {
                        Datum::Json(value) => value.value().len() as i64 + 1,
                        other => panic!("{other:?}"),
                    },
                    FieldTypeCode::Duration => 16,
                    _ => 8,
                }
            })
            .collect();
        test_agg_mem_func(&p, DEF_PARTIAL_RESULT_4_JSON_ARRAYAGG + DEF_SLICE_SIZE, &deltas, false);
    }
}

/// A JSON_ARRAYAGG of non-NULL rows reads back what it spilled.
#[test]
fn json_aggs_spill_and_restore() {
    let p = AggTest::new("json_arrayagg", FieldTypeCode::LongLong, 5, Vec::new());
    let desc = p.desc(false);
    let func = build(&NoColumns, &desc, 0).expect("json_arrayagg is implemented");
    let (mut pr, _) = func.alloc_partial_result();
    let src_chk = p.gen_src_chk();
    for i in 0..5 {
        func.update_partial_result(&NoColumns, &[src_chk.get_row(i)], &mut pr).expect("update succeeds");
    }
    let blob = FieldType::new(FieldTypeCode::Blob);
    let mut spill = Chunk::new_chunk_with_capacity(std::slice::from_ref(&blob), 1);
    func.serialize_partial_result(&mut pr, &mut spill, &mut crate::spill_serialize_helper::SerializeHelper::new());
    let (mut restored, _) = func.deserialize_partial_result(&spill);
    assert_datum_eq(&final_datum(func.as_ref(), &mut restored[0], &desc.base.ret_type), &final_datum(func.as_ref(), &mut pr, &desc.base.ret_type), "json_arrayagg spill");

    let (desc, chk) = json_objectagg_input(FieldTypeCode::LongLong, FieldTypeCode::String);
    let func = build(&NoColumns, &desc, 0).expect("json_objectagg is implemented");
    let (mut pr, alloc) = func.alloc_partial_result();
    let mut charged = 0;
    for i in 0..5 {
        charged += func.update_partial_result(&NoColumns, &[chk.get_row(i)], &mut pr).expect("update succeeds");
    }
    let mut spill = Chunk::new_chunk_with_capacity(std::slice::from_ref(&blob), 1);
    func.serialize_partial_result(&mut pr, &mut spill, &mut crate::spill_serialize_helper::SerializeHelper::new());
    let (mut restored, mem_delta) = func.deserialize_partial_result(&spill);
    // Go's deserializer charges the entries as the updates did.
    assert_eq!(mem_delta, alloc + charged);
    assert_datum_eq(&final_datum(func.as_ref(), &mut restored[0], &desc.base.ret_type), &final_datum(func.as_ref(), &mut pr, &desc.base.ret_type), "json_objectagg spill");
}

/// A JSON_OBJECTAGG descriptor over a generated key column and value
/// column (Go `buildMultiArgsAggTester`), with the trailing NULL row.
fn json_objectagg_input(key_tp: FieldTypeCode, value_tp: FieldTypeCode) -> (AggFuncDesc, Chunk) {
    let tps = vec![FieldType::new(key_tp), FieldType::new(value_tp)];
    let mut chk = Chunk::new_chunk_with_capacity(&tps, 6);
    for i in 0..5 {
        for (j, tp) in tps.iter().enumerate() {
            chk.append_datum(j, &data_gen(tp, i));
        }
    }
    for j in 0..tps.len() {
        chk.append_datum(j, &Datum::Null);
    }
    let args: Vec<Expression> = tps
        .iter()
        .enumerate()
        .map(|(k, tp)| {
            let mut column = Column::new(k as i64 + 1, tp.clone());
            column.index = k as i64;
            Expression::Column(column)
        })
        .collect();
    (AggFuncDesc::new(&NoColumns, "json_objectagg", args, false).expect("descriptor builds"), chk)
}

/// Go `TestJsonObjectagg` and `TestMemJsonObjectagg` over Go's key and
/// value types: the object keys each row's key text (`Datum.ToString`), a
/// new key charges its bytes, an interface and the value; the NULL key
/// fails (Go `ErrJSONDocumentNULLKey`).
#[test]
fn test_json_objectagg() {
    use tidb_datatype::BinaryJSONValue as V;
    let types = [FieldTypeCode::LongLong, FieldTypeCode::Double, FieldTypeCode::Float, FieldTypeCode::String, FieldTypeCode::Json, FieldTypeCode::Date, FieldTypeCode::Duration, FieldTypeCode::NewDecimal];
    for key_tp in types {
        for value_tp in types {
            let (desc, chk) = json_objectagg_input(key_tp, value_tp);
            let func = build(&NoColumns, &desc, 0).expect("json_objectagg is implemented");
            let (mut pr, alloc) = func.alloc_partial_result();
            assert_eq!(alloc, crate::func_json_objectagg::DEF_PARTIAL_RESULT_4_JSON_OBJECT_AGG + tidb_hack::DEF_BUCKET_MEMORY_USAGE_FOR_MAP_STRING_TO_ANY as i64);
            let mut entries = std::collections::BTreeMap::new();
            for i in 0..5 {
                let key = data_gen(&FieldType::new(key_tp), i as usize);
                let key_text = String::from_utf8(key.sql_bytes().expect("a key")).expect("utf8");
                let fresh = !entries.contains_key(&key_text);
                let value_bytes = match value_tp {
                    FieldTypeCode::String => chk.get_row(i as usize).get_bytes(1).len() as i64,
                    FieldTypeCode::Json => match chk.get_row(i as usize).get_datum(1, &FieldType::new(value_tp)) {
                        Datum::Json(value) => value.value().len() as i64 + 1,
                        other => panic!("{other:?}"),
                    },
                    FieldTypeCode::Duration => 16,
                    _ => 8,
                };
                entries.insert(key_text.clone(), go_json_value(value_tp, i));
                let delta = func.update_partial_result(&NoColumns, &[chk.get_row(i as usize)], &mut pr).expect("update succeeds");
                assert_eq!(delta, if fresh { key_text.len() as i64 + 16 + value_bytes } else { 0 }, "{key_tp:?} {value_tp:?} row {i}");
            }
            assert!(func.update_partial_result(&NoColumns, &[chk.get_row(5)], &mut pr).is_err(), "a NULL key fails");
            let expected = Datum::Json(tidb_datatype::BinaryJSON::from_typed_value(&V::Object(entries)).expect("an object"));
            assert_datum_eq(&final_datum(func.as_ref(), &mut pr, &desc.base.ret_type), &expected, "json_objectagg");
            func.reset_partial_result(&mut pr);
            assert!(final_datum(func.as_ref(), &mut pr, &desc.base.ret_type).is_null());
        }
    }
}

/// Go `TestMergePartialResult4JsonObjectagg`: merging rows 2..5 into rows
/// 0..5 keeps the first object.
#[test]
fn test_merge_partial_result_4_json_objectagg() {
    let (desc, chk) = json_objectagg_input(FieldTypeCode::LongLong, FieldTypeCode::Double);
    let func = build(&NoColumns, &desc, 0).expect("json_objectagg is implemented");
    let (mut p1, _) = func.alloc_partial_result();
    let (mut p2, _) = func.alloc_partial_result();
    for i in 0..5 {
        func.update_partial_result(&NoColumns, &[chk.get_row(i)], &mut p1).expect("update succeeds");
    }
    for i in 2..5 {
        func.update_partial_result(&NoColumns, &[chk.get_row(i)], &mut p2).expect("update succeeds");
    }
    let first = final_datum(func.as_ref(), &mut p1, &desc.base.ret_type);
    func.merge_partial_result(&NoColumns, &mut p2, &mut p1).expect("merge succeeds");
    assert_datum_eq(&final_datum(func.as_ref(), &mut p1, &desc.base.ret_type), &first, "merged");
}

/// A repeated JSON_OBJECTAGG key replaces the value and charges nothing
/// (Go `SetExt`'s `insert` is false); the object keeps the last value.
#[test]
fn a_repeated_json_objectagg_key_charges_nothing() {
    let string = FieldType::new(FieldTypeCode::String);
    let long = FieldType::new(FieldTypeCode::LongLong);
    let mut chk = Chunk::new_chunk_with_capacity(&[string.clone(), long.clone()], 2);
    for value in [1, 2] {
        chk.append_datum(0, &Datum::new_string_datum("k"));
        chk.append_datum(1, &Datum::Int(value));
    }
    let args: Vec<Expression> = [string, long]
        .into_iter()
        .enumerate()
        .map(|(k, tp)| {
            let mut column = Column::new(k as i64 + 1, tp);
            column.index = k as i64;
            Expression::Column(column)
        })
        .collect();
    let desc = AggFuncDesc::new(&NoColumns, "json_objectagg", args, false).expect("descriptor builds");
    let func = build(&NoColumns, &desc, 0).expect("json_objectagg is implemented");
    let (mut pr, _) = func.alloc_partial_result();
    let deltas: Vec<i64> = (0..2).map(|i| func.update_partial_result(&NoColumns, &[chk.get_row(i)], &mut pr).expect("update succeeds")).collect();
    assert_eq!(deltas, vec![1 + 16 + 8, 0]);
    let expected = Datum::Json(tidb_datatype::BinaryJSON::parse(r#"{"k": 2}"#).expect("an object"));
    assert_datum_eq(&final_datum(func.as_ref(), &mut pr, &desc.base.ret_type), &expected, "last value");
}

/// Go `genApproxDistinctMergePartialResult`: the serialized sketch of the
/// little-endian `begin..end`.
fn gen_approx_distinct_merge_partial_result(begin: u64, end: u64) -> Datum {
    let mut o = crate::approx_count_distinct::PartialResult4ApproxCountDistinct::new();
    for i in begin..end {
        o.insert(&i.to_le_bytes());
    }
    Datum::new_string_datum(o.serialize())
}

/// Go `TestMergePartialResult4Count`'s APPROX_COUNT_DISTINCT case.
#[test]
fn test_merge_partial_result_4_approx_count_distinct() {
    test_merge_partial_result(&AggTest::new(
        "approx_count_distinct",
        FieldTypeCode::LongLong,
        5,
        vec![gen_approx_distinct_merge_partial_result(0, 5), gen_approx_distinct_merge_partial_result(2, 5), Datum::Int(5)],
    ));
}

/// Go `TestCount`'s APPROX_COUNT_DISTINCT cases (one and two arguments):
/// five distinct tuples estimate 5, no rows 0.
#[test]
fn test_approx_count_distinct() {
    for tp in COUNT_TYPES.into_iter().chain([FieldTypeCode::Float, FieldTypeCode::Date, FieldTypeCode::Json]) {
        test_agg_func(&AggTest::new("approx_count_distinct", tp, 5, vec![Datum::Int(0), Datum::Int(5)]));
        let field_type = FieldType::new(tp);
        let mut chk = Chunk::new_chunk_with_capacity(&[field_type.clone(), field_type.clone()], 6);
        for i in 0..5 {
            chk.append_datum(0, &data_gen(&field_type, i));
            chk.append_datum(1, &data_gen(&field_type, i));
        }
        chk.append_datum(0, &Datum::Null);
        chk.append_datum(1, &Datum::Null);
        let args: Vec<Expression> = (0..2)
            .map(|k| {
                let mut column = Column::new(k + 1, field_type.clone());
                column.index = k;
                Expression::Column(column)
            })
            .collect();
        let desc = AggFuncDesc::new(&NoColumns, "approx_count_distinct", args, false).expect("descriptor builds");
        let func = build(&NoColumns, &desc, 0).expect("approx_count_distinct is implemented");
        let (mut pr, _) = func.alloc_partial_result();
        update_each_row(func.as_ref(), &chk, &mut pr);
        assert_datum_eq(&final_datum(func.as_ref(), &mut pr, &desc.base.ret_type), &Datum::Int(5), "two arguments");
    }
}

/// Go `TestMemCount`'s APPROX_COUNT_DISTINCT cases: five values never grow
/// the sketch's 16-slot buffer.
#[test]
fn test_mem_approx_count_distinct() {
    for tp in [FieldTypeCode::LongLong, FieldTypeCode::String] {
        let p = AggTest::new("approx_count_distinct", tp, 5, Vec::new());
        test_agg_mem_func(&p, crate::func_count_distinct::DEF_PARTIAL_RESULT_4_APPROX_COUNT_DISTINCT_SIZE, &default_update_mem_deltas(&p), true);
    }
    // Past half of its buffer the sketch doubles: Go charges the new
    // buffer's `uint32`s.
    let p = AggTest::new("approx_count_distinct", FieldTypeCode::LongLong, 20, Vec::new());
    let func = build(&NoColumns, &p.desc(false), 0).expect("implemented");
    let (mut pr, _) = func.alloc_partial_result();
    let total: i64 = update_each_row(func.as_ref(), &p.gen_src_chk(), &mut pr).iter().sum();
    let mut sketch = crate::approx_count_distinct::PartialResult4ApproxCountDistinct::new();
    let start = sketch.mem_usage();
    for i in 0..20u64 {
        sketch.insert(&(i as i64).to_le_bytes());
    }
    assert!(sketch.mem_usage() > start, "twenty values grow the sketch");
    assert_eq!(total, sketch.mem_usage() - start);
}

/// Go `TestPercentile`: the median of 0..5 is 2.
#[test]
fn test_percentile() {
    for (tp, expected) in [
        (FieldTypeCode::LongLong, Datum::Int(2)),
        (FieldTypeCode::Float, Datum::Float32(2.0)),
        (FieldTypeCode::Double, Datum::Real(2.0)),
        (FieldTypeCode::NewDecimal, decimal(2)),
        (FieldTypeCode::Date, Datum::Time(tidb_datatype::time_from_days(367))),
        (FieldTypeCode::Duration, Datum::Duration(MySqlDuration::from_nanoseconds(2, 0).expect("a duration"))),
    ] {
        test_agg_func(&AggTest::new("approx_percentile", tp, 5, vec![Datum::Null, expected]));
    }
}

/// Go `TestFix26807`: the 100th percentile of 1..=28 is 28, whatever the
/// random pivots.
#[test]
fn test_fix_26807() {
    let p = AggTest::new("approx_percentile", FieldTypeCode::LongLong, 0, Vec::new());
    let mut desc = p.desc(false);
    desc.base.args[1] = Expression::Constant(tidb_expr::constant::Constant::new(Datum::Int(100), FieldType::new(FieldTypeCode::Long)));
    let func = build(&NoColumns, &desc, 0).expect("implemented");
    let long = FieldType::new(FieldTypeCode::LongLong);
    let mut chk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&long), 28);
    for i in 1..=28 {
        chk.append_datum(0, &Datum::Int(i));
    }
    for _ in 0..10 {
        let (mut pr, _) = func.alloc_partial_result();
        update_each_row(func.as_ref(), &chk, &mut pr);
        assert_datum_eq(&final_datum(func.as_ref(), &mut pr, &long), &Datum::Int(28), "p100");
    }
}

/// Go `TestFix40463`: an ENUM or SET argument answers NULL.
#[test]
fn test_fix_40463() {
    for tp in [FieldTypeCode::Enum, FieldTypeCode::Set] {
        let p = AggTest::new("approx_percentile", tp, 5, Vec::new());
        let desc = p.desc(false);
        let func = build(&NoColumns, &desc, 0).expect("implemented");
        let (mut pr, alloc) = func.alloc_partial_result();
        assert_eq!(alloc, 0);
        update_each_row(func.as_ref(), &p.gen_src_chk(), &mut pr);
        assert!(final_datum(func.as_ref(), &mut pr, &desc.base.ret_type).is_null());
    }
}

/// Go `MemSize` deltas: each kept value adds its element size (a decimal
/// `MyDecimal`'s 40 bytes, the rest 8); a NULL adds nothing.
#[test]
fn test_mem_percentile() {
    for (tp, elem) in [(FieldTypeCode::LongLong, 8), (FieldTypeCode::Double, 8), (FieldTypeCode::NewDecimal, 40), (FieldTypeCode::Date, 8), (FieldTypeCode::Duration, 8)] {
        let p = AggTest::new("approx_percentile", tp, 5, Vec::new());
        let mut deltas = vec![elem; 5];
        deltas.push(0);
        test_agg_mem_func(&p, 24, &deltas, false);
    }
}

/// Go `windowTest`.
struct WindowTest {
    data_type: FieldType,
    num_rows: usize,
    func_name: &'static str,
    args: Vec<Expression>,
    order_by_cols: Vec<Column>,
    results: Vec<Datum>,
}

fn column_0(tp: &FieldType) -> Column {
    let mut column = Column::new(1, tp.clone());
    column.index = 0;
    column
}

impl WindowTest {
    /// Go `buildWindowTesterWithArgs`.
    fn with_args(func_name: &'static str, tp: FieldTypeCode, args: Vec<Expression>, order_by_cols: usize, num_rows: usize, results: Vec<Datum>) -> Self {
        let data_type = FieldType::new(tp);
        let mut all_args = Vec::new();
        if func_name != "ntile" {
            all_args.push(Expression::Column(column_0(&data_type)));
        }
        all_args.extend(args);
        let order_by_cols = if order_by_cols > 0 { vec![column_0(&data_type)] } else { Vec::new() };
        Self { data_type, num_rows, func_name, args: all_args, order_by_cols, results }
    }

    /// Go `buildWindowTester`: a positive constant argument is an unsigned
    /// constant.
    fn new(func_name: &'static str, tp: FieldTypeCode, constant_arg: u64, order_by_cols: usize, num_rows: usize, results: Vec<Datum>) -> Self {
        let args = if constant_arg > 0 { vec![uint_constant(constant_arg)] } else { Vec::new() };
        Self::with_args(func_name, tp, args, order_by_cols, num_rows, results)
    }

    /// Go `windowTest.genSrcChk`.
    fn gen_src_chk(&self) -> Chunk {
        let mut src_chk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&self.data_type), self.num_rows);
        for i in 0..self.num_rows {
            src_chk.append_datum(0, &data_gen(&self.data_type, i));
        }
        src_chk
    }

    fn build(&self) -> (AggFuncDesc, Box<dyn AggFunc>) {
        let desc = AggFuncDesc::new(&NoColumns, self.func_name, self.args.clone(), false).expect("descriptor builds");
        let func = crate::agg_builder::build_window_functions(&NoColumns, &desc, 0, &self.order_by_cols).expect("the window function is implemented");
        (desc, func)
    }
}

fn uint_constant(value: u64) -> Expression {
    Expression::Constant(tidb_expr::constant::Constant::new(Datum::UInt(value), FieldType::new(FieldTypeCode::LongLong).with_unsigned(true)))
}

fn int_constant(value: i64, tp: FieldTypeCode) -> Expression {
    Expression::Constant(tidb_expr::constant::Constant::new(Datum::Int(value), FieldType::new(tp)))
}

/// Go `testWindowFunc`: each row updates the partial result, then each
/// final result is the next row's.
fn test_window_func(p: &WindowTest) {
    let src_chk = p.gen_src_chk();
    let (desc, func) = p.build();
    let (mut pr, _) = func.alloc_partial_result();
    update_each_row(func.as_ref(), &src_chk, &mut pr);
    assert_eq!(p.results.len(), p.num_rows);
    for (i, expected) in p.results.iter().enumerate() {
        let actual = final_datum(func.as_ref(), &mut pr, &desc.base.ret_type);
        if expected.is_null() {
            assert!(actual.is_null(), "{} row {i}: {actual:?}", p.func_name);
        } else {
            assert_datum_eq(&actual, expected, &format!("{} row {i}", p.func_name));
        }
    }
    func.reset_partial_result(&mut pr);
}

/// Go `testWindowAggMemFunc`.
fn test_window_agg_mem_func(p: &WindowTest, alloc_mem_delta: i64, update_mem_deltas: &[i64]) {
    let (_, func) = p.build();
    let (mut pr, mem_delta) = func.alloc_partial_result();
    assert_eq!(mem_delta, alloc_mem_delta, "{} alloc", p.func_name);
    assert_eq!(update_each_row(func.as_ref(), &p.gen_src_chk(), &mut pr), update_mem_deltas, "{} updates", p.func_name);
}

/// Go `TestWindowFunctions`.
#[test]
fn test_window_functions() {
    let i = |v: i64| Datum::Int(v);
    let r = |v: f64| Datum::Real(v);
    let date = |days| Datum::Time(tidb_datatype::time_from_days(days));
    let zero_duration = Datum::Duration(MySqlDuration::from_nanoseconds(0, 0).expect("a duration"));
    use FieldTypeCode as T;
    for test in [
        WindowTest::new("cume_dist", T::LongLong, 0, 1, 1, vec![r(1.0)]),
        WindowTest::new("cume_dist", T::LongLong, 0, 0, 2, vec![r(1.0), r(1.0)]),
        WindowTest::new("cume_dist", T::LongLong, 0, 1, 4, vec![r(0.25), r(0.5), r(0.75), r(1.0)]),
        WindowTest::new("dense_rank", T::LongLong, 0, 0, 2, vec![i(1), i(1)]),
        WindowTest::new("dense_rank", T::LongLong, 0, 1, 4, vec![i(1), i(2), i(3), i(4)]),
        WindowTest::new("first_value", T::LongLong, 0, 1, 2, vec![i(0), i(0)]),
        WindowTest::new("first_value", T::Float, 0, 1, 2, vec![Datum::Float32(0.0), Datum::Float32(0.0)]),
        WindowTest::new("first_value", T::Double, 0, 1, 2, vec![r(0.0), r(0.0)]),
        WindowTest::new("first_value", T::NewDecimal, 0, 1, 2, vec![decimal(0), decimal(0)]),
        WindowTest::new("first_value", T::String, 0, 1, 2, vec![Datum::new_string_datum("0"), Datum::new_string_datum("0")]),
        WindowTest::new("first_value", T::Date, 0, 1, 2, vec![date(365), date(365)]),
        WindowTest::new("first_value", T::Duration, 0, 1, 2, vec![zero_duration.clone(), zero_duration]),
        WindowTest::new("first_value", T::Json, 0, 1, 2, vec![json(0), json(0)]),
        WindowTest::new("last_value", T::LongLong, 1, 0, 2, vec![i(1), i(1)]),
        WindowTest::new("nth_value", T::LongLong, 2, 0, 3, vec![i(1), i(1), i(1)]),
        WindowTest::new("nth_value", T::LongLong, 5, 0, 3, vec![Datum::Null, Datum::Null, Datum::Null]),
        WindowTest::new("ntile", T::LongLong, 3, 0, 4, vec![i(1), i(1), i(2), i(3)]),
        WindowTest::new("ntile", T::LongLong, 5, 0, 3, vec![i(1), i(2), i(3)]),
        WindowTest::new("percent_rank", T::LongLong, 0, 1, 1, vec![r(0.0)]),
        WindowTest::new("percent_rank", T::LongLong, 0, 0, 3, vec![r(0.0), r(0.0), r(0.0)]),
        WindowTest::new("percent_rank", T::LongLong, 0, 1, 4, vec![r(0.0), r(0.3333333333333333), r(0.6666666666666666), r(1.0)]),
        WindowTest::new("rank", T::LongLong, 0, 1, 1, vec![i(1)]),
        WindowTest::new("rank", T::LongLong, 0, 0, 3, vec![i(1), i(1), i(1)]),
        WindowTest::new("rank", T::LongLong, 0, 1, 4, vec![i(1), i(2), i(3), i(4)]),
        WindowTest::new("row_number", T::LongLong, 0, 0, 4, vec![i(1), i(2), i(3), i(4)]),
    ] {
        test_window_func(&test);
    }
}

/// Go `TestLeadLag`.
#[test]
fn test_lead_lag() {
    use FieldTypeCode as T;
    let zero = || int_constant(0, T::LongLong);
    let one = || int_constant(1, T::LongLong);
    let two = || int_constant(2, T::Tiny);
    let three = || int_constant(3, T::Tiny);
    let million = || int_constant(1_000_000, T::Long);
    let default_arg = || Expression::Column(column_0(&FieldType::new(T::LongLong)));
    let v = |values: [Option<i64>; 3]| values.iter().map(|x| x.map_or(Datum::Null, Datum::Int)).collect::<Vec<_>>();
    const M: Option<i64> = Some(1_000_000);
    let cases: Vec<(&'static str, Vec<Expression>, [Option<i64>; 3])> = vec![
        ("lag", vec![zero()], [Some(0), Some(1), Some(2)]),
        ("lag", vec![one()], [None, Some(0), Some(1)]),
        ("lag", vec![two()], [None, None, Some(0)]),
        ("lag", vec![three()], [None, None, None]),
        ("lag", vec![million()], [None, None, None]),
        ("lag", vec![zero(), million()], [Some(0), Some(1), Some(2)]),
        ("lag", vec![one(), million()], [M, Some(0), Some(1)]),
        ("lag", vec![two(), million()], [M, M, Some(0)]),
        ("lag", vec![three(), million()], [M, M, M]),
        ("lag", vec![million(), million()], [M, M, M]),
        ("lag", vec![zero(), default_arg()], [Some(0), Some(1), Some(2)]),
        ("lag", vec![one(), default_arg()], [Some(0), Some(0), Some(1)]),
        ("lag", vec![two(), default_arg()], [Some(0), Some(1), Some(0)]),
        ("lag", vec![three(), default_arg()], [Some(0), Some(1), Some(2)]),
        ("lag", vec![million(), default_arg()], [Some(0), Some(1), Some(2)]),
        ("lead", vec![zero()], [Some(0), Some(1), Some(2)]),
        ("lead", vec![one()], [Some(1), Some(2), None]),
        ("lead", vec![two()], [Some(2), None, None]),
        ("lead", vec![three()], [None, None, None]),
        ("lead", vec![million()], [None, None, None]),
        ("lead", vec![zero(), million()], [Some(0), Some(1), Some(2)]),
        ("lead", vec![one(), million()], [Some(1), Some(2), M]),
        ("lead", vec![two(), million()], [Some(2), M, M]),
        ("lead", vec![three(), million()], [M, M, M]),
        ("lead", vec![million(), million()], [M, M, M]),
        ("lead", vec![zero(), default_arg()], [Some(0), Some(1), Some(2)]),
        ("lead", vec![one(), default_arg()], [Some(1), Some(2), Some(2)]),
        ("lead", vec![two(), default_arg()], [Some(2), Some(1), Some(2)]),
        ("lead", vec![three(), default_arg()], [Some(0), Some(1), Some(2)]),
        ("lead", vec![million(), default_arg()], [Some(0), Some(1), Some(2)]),
    ];
    for (name, args, results) in cases {
        test_window_func(&WindowTest::with_args(name, T::LongLong, args, 0, 3, v(results)));
    }
}

/// Go `TestMemRank`, `TestMemCumeDist`, `TestMemPercentRank`,
/// `TestMemNtile`, `TestMemRowNumber` and `TestMemLeadLag`: a function
/// that keeps the partition's rows charges `DefRowSize` per row.
#[test]
fn test_mem_window_functions() {
    use crate::func_rank::{DEF_PARTIAL_RESULT_4_RANK_SIZE, DEF_ROW_SIZE};
    use FieldTypeCode as T;
    for (name, constant, alloc, row_bytes) in [
        ("rank", 0, DEF_PARTIAL_RESULT_4_RANK_SIZE, DEF_ROW_SIZE),
        ("cume_dist", 0, crate::func_cume_dist::DEF_PARTIAL_RESULT_4_CUME_DIST_SIZE, DEF_ROW_SIZE),
        ("percent_rank", 0, DEF_PARTIAL_RESULT_4_RANK_SIZE, DEF_ROW_SIZE),
        ("ntile", 1, crate::func_ntile::DEF_PARTIAL_RESULT_4_NTILE, 0),
    ] {
        for (num_rows, order_by) in [(1, 1), (3, 0), (4, 1)] {
            let p = WindowTest::new(name, T::LongLong, constant, order_by, num_rows, Vec::new());
            test_window_agg_mem_func(&p, alloc, &vec![row_bytes; num_rows]);
        }
    }
    let p = WindowTest::new("row_number", T::LongLong, 0, 4, 0, Vec::new());
    test_window_agg_mem_func(&p, crate::row_number::DEF_PARTIAL_RESULT_4_ROW_NUMBER_SIZE, &[]);
    for name in ["lag", "lead"] {
        for offset in [int_constant(0, T::LongLong), int_constant(1, T::LongLong), int_constant(2, T::Tiny), int_constant(1_000_000, T::Long)] {
            let p = WindowTest::with_args(name, T::LongLong, vec![offset], 0, 3, Vec::new());
            test_window_agg_mem_func(&p, crate::func_lead_lag::DEF_PARTIAL_RESULT_4_LEAD_LAG_SIZE, &[DEF_ROW_SIZE; 3]);
        }
    }
}

/// Go `TestMemValue`: the value evaluator charges a string's or a JSON
/// document's bytes when it evaluates a row.
#[test]
fn test_mem_value() {
    use crate::func_value::*;
    use FieldTypeCode as T;
    let value_bytes = |tp: T, row: usize| -> i64 {
        match tp {
            T::String => data_gen(&FieldType::new(tp), row).sql_bytes().expect("bytes").len() as i64,
            T::Json => 8,
            _ => 0,
        }
    };
    for (tp, size) in [(T::LongLong, 16), (T::Float, 8), (T::Double, 16), (T::NewDecimal, 16), (T::String, 24), (T::Date, 16), (T::Duration, 24), (T::Json, 40)] {
        let p = WindowTest::new("first_value", tp, 0, 1, 2, Vec::new());
        test_window_agg_mem_func(&p, DEF_PARTIAL_RESULT_4_FIRST_VALUE_SIZE + size, &[value_bytes(tp, 0), 0]);
    }
    for (tp, size) in [(T::LongLong, 16), (T::String, 24), (T::Json, 40)] {
        let p = WindowTest::new("last_value", tp, 1, 0, 2, Vec::new());
        test_window_agg_mem_func(&p, DEF_PARTIAL_RESULT_4_LAST_VALUE_SIZE + size, &[value_bytes(tp, 0), value_bytes(tp, 1) - value_bytes(tp, 0)]);
    }
    for (tp, size, nth) in [(T::LongLong, 16, 2), (T::LongLong, 16, 5), (T::Json, 40, 2), (T::String, 24, 5)] {
        let p = WindowTest::new("nth_value", tp, nth, 0, 3, Vec::new());
        let mut deltas = vec![0; 3];
        if (nth as usize) < 3 {
            deltas[nth as usize - 1] = value_bytes(tp, nth as usize - 1);
        }
        test_window_agg_mem_func(&p, DEF_PARTIAL_RESULT_4_NTH_VALUE_SIZE + size, &deltas);
    }
}

/// Ties: over ordered 1, 1, 2, RANK skips (1, 1, 3), DENSE_RANK does not
/// (1, 1, 2), PERCENT_RANK is (0, 0, 1) and CUME_DIST (2/3, 2/3, 1).
#[test]
fn ranks_treat_peers_like_go() {
    let long = FieldType::new(FieldTypeCode::LongLong);
    let mut chk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&long), 3);
    for v in [1, 1, 2] {
        chk.append_datum(0, &Datum::Int(v));
    }
    for (name, expected) in [
        ("rank", vec![Datum::Int(1), Datum::Int(1), Datum::Int(3)]),
        ("dense_rank", vec![Datum::Int(1), Datum::Int(1), Datum::Int(2)]),
        ("percent_rank", vec![Datum::Real(0.0), Datum::Real(0.0), Datum::Real(1.0)]),
        ("cume_dist", vec![Datum::Real(2.0 / 3.0), Datum::Real(2.0 / 3.0), Datum::Real(1.0)]),
    ] {
        let p = WindowTest::new(name, FieldTypeCode::LongLong, 0, 1, 3, Vec::new());
        let (desc, func) = p.build();
        let (mut pr, _) = func.alloc_partial_result();
        update_each_row(func.as_ref(), &chk, &mut pr);
        for (i, expected) in expected.iter().enumerate() {
            assert_datum_eq(&final_datum(func.as_ref(), &mut pr, &desc.base.ret_type), expected, &format!("{name} row {i}"));
        }
    }
}
