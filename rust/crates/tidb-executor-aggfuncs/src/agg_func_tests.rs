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

/// Go `getDataGenFunc`.
fn data_gen(field_type: &FieldType, i: usize) -> Datum {
    let i = i as i64;
    match field_type.code() {
        FieldTypeCode::LongLong => Datum::Int(i),
        FieldTypeCode::Double => Datum::Real(i as f64),
        FieldTypeCode::NewDecimal => Datum::Decimal(Decimal::from_int(i)),
        FieldTypeCode::String => Datum::new_string_datum(i.to_string()),
        FieldTypeCode::Duration => {
            Datum::Duration(MySqlDuration::from_nanoseconds(i, 0).expect("a valid duration"))
        }
        other => panic!("no data generator for {other:?}"),
    }
}

impl AggTest {
    /// Go `buildAggTester`.
    fn new(func_name: &'static str, key_tp: FieldTypeCode, num_rows: usize, results: Vec<Datum>) -> Self {
        Self {
            key_type: FieldType::new(key_tp),
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
    let final_func = build(&desc, 0).expect("the function is implemented");
    let (mut final_pr, _) = final_func.alloc_partial_result();
    update_each_row(final_func.as_ref(), &src_chk, &mut final_pr);
    assert_eq!(final_datum(final_func.as_ref(), &mut final_pr, desc.ret_type()), p.results[1], "{}", p.func_name);

    // test the empty input
    final_func.reset_partial_result(&mut final_pr);
    assert_eq!(final_datum(final_func.as_ref(), &mut final_pr, desc.ret_type()), p.results[0], "{}", p.func_name);

    // test the agg func with distinct
    let desc = p.desc(true);
    let final_func = build(&desc, 0).expect("the function is implemented");
    let (mut final_pr, _) = final_func.alloc_partial_result();
    update_each_row(final_func.as_ref(), &p.gen_src_chk(), &mut final_pr);
    update_each_row(final_func.as_ref(), &p.gen_src_chk(), &mut final_pr);
    assert_eq!(final_datum(final_func.as_ref(), &mut final_pr, desc.ret_type()), p.results[1], "{} distinct", p.func_name);
}

/// Go `testMergePartialResult`: a partial phase over all rows and over all
/// but the first, each merged into the final phase.
fn test_merge_partial_result(p: &AggTest) {
    let desc = p.desc(false);
    let (partial_desc, final_desc) = desc.split(&[0, 1]);
    let partial_func = build(&partial_desc, 0).expect("the partial function is implemented");
    let (mut partial_result, _) = partial_func.alloc_partial_result();
    let final_func = build(&final_desc, 0).expect("the final function is implemented");
    let (mut final_pr, _) = final_func.alloc_partial_result();

    update_each_row(partial_func.as_ref(), &p.gen_src_chk(), &mut partial_result);
    assert_eq!(final_datum(partial_func.as_ref(), &mut partial_result, &p.key_type), p.results[0]);
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
    assert_eq!(final_datum(partial_func.as_ref(), &mut partial_result, &p.key_type), p.results[1]);
    final_func
        .merge_partial_result(&NoColumns, &mut partial_result, &mut final_pr)
        .expect("merge succeeds");
    assert_eq!(final_datum(final_func.as_ref(), &mut final_pr, &p.key_type), p.results[2]);
}

/// Go `aggMemTest` + `testAggMemFunc`: the bytes the partial result is
/// allocated with and each row's update reports.
fn test_agg_mem_func(p: &AggTest, alloc_mem_delta: i64, update_mem_deltas: &[i64], is_distinct: bool) {
    let desc = p.desc(is_distinct);
    let final_func = build(&desc, 0).expect("the function is implemented");
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
/// beside the set (a decimal hash key's or a string's length), a repeat or
/// a NULL nothing; five keys never grow the set's first bucket.
fn distinct_update_mem_deltas(p: &AggTest) -> Vec<i64> {
    let src_chk = p.gen_src_chk();
    (0..src_chk.num_rows())
        .map(|i| {
            let row = src_chk.get_row(i);
            if row.is_null(0) {
                return 0;
            }
            match p.key_type.code() {
                FieldTypeCode::NewDecimal => {
                    let Datum::Decimal(value) = row.get_datum(0, &p.key_type) else {
                        unreachable!("a decimal column")
                    };
                    value.to_hash_key().expect("a hash key").0.len() as i64
                }
                FieldTypeCode::String => GoString::from(row.get_bytes(0).to_vec()).len() as i64,
                _ => 0,
            }
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
    ] {
        assert_eq!(Some(&value), go.get(name), "{name}");
    }
}
