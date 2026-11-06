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
        vec![Expression::Column(column)]
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

    update_each_row(partial_func.as_ref(), &p.gen_src_chk(), &mut partial_result);
    assert_datum_eq(&final_datum(partial_func.as_ref(), &mut partial_result, &p.key_type), &p.results[0], p.func_name);
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
    assert_datum_eq(&final_datum(partial_func.as_ref(), &mut partial_result, &p.key_type), &p.results[1], p.func_name);
    final_func
        .merge_partial_result(&NoColumns, &mut partial_result, &mut final_pr)
        .expect("merge succeeds");
    assert_datum_eq(&final_datum(final_func.as_ref(), &mut final_pr, &p.key_type), &p.results[2], p.func_name);
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
