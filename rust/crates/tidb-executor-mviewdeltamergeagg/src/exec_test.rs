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

//! Go master `pkg/executor/mviewdeltamergeagg/exec_test.go`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags, MyDecimal};
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_expr::constant::Constant;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expression_aggregation::{names, AggFuncDesc, AggFunctionMode, BaseFuncDesc};

use crate::exec::{ChunkResult, Exec, Mapping, MergeRuntimeStats, MergeWriterStats, ResultWriter, RowOpType};
use crate::merge::error_text;

/// Go `mockSource`: hands out its chunks, then nothing.
struct MockSource {
    meta: ExecutorMeta,
    chunks: Vec<Chunk>,
    idx: usize,
}

/// Go `newMockSource`.
fn new_mock_source(fts: &[FieldType], chks: Vec<Chunk>) -> Box<dyn Executor> {
    let cols = fts.iter().enumerate().map(|(i, ft)| tidb_expr::column::Column::new(i as i64 + 1, ft.clone())).collect();
    Box::new(MockSource { meta: ExecutorMeta::new(Schema::new(cols), 0, 32, 1024), chunks: chks, idx: 0 })
}

impl Executor for MockSource {
    fn open(&mut self) -> Result<(), ExecError> {
        self.idx = 0;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.idx >= self.chunks.len() {
            return Ok(());
        }
        req.swap_columns(&mut self.chunks[self.idx]);
        self.idx += 1;
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

/// Go `collectWriter`: keeps a copy of every result.
#[derive(Clone, Default)]
struct CollectWriter(Arc<Mutex<Vec<ChunkResult>>>);

impl ResultWriter for CollectWriter {
    fn write_chunk(&mut self, result: &ChunkResult, _: &mut MergeWriterStats) -> Result<(), ExecError> {
        self.0.lock().unwrap().push(ChunkResult {
            input: result.input.copy_construct(),
            computed_cols: result.computed_cols.iter().map(|col| col.as_ref().map(|c| c.copy_construct())).collect(),
            row_ops: result.row_ops.clone(),
            update_touched_bitmap: result.update_touched_bitmap.clone(),
            update_touched_stride: result.update_touched_stride,
            update_touched_bit_cnt: result.update_touched_bit_cnt,
        });
        Ok(())
    }
}

fn ft(code: FieldTypeCode) -> FieldType {
    FieldType::new(code)
}

fn ft_flags(code: FieldTypeCode, flags: u32) -> FieldType {
    let mut tp = FieldType::new(code);
    tp.add_flags(flags);
    tp
}

/// Go `aggregation.NewAggFuncDesc(ctx, name, args, false)`.
fn agg_desc(name: &str, args: Vec<Expression>) -> AggFuncDesc {
    let ret_type = match name {
        names::COUNT => ft(FieldTypeCode::LongLong),
        _ => match args.first() {
            Some(Expression::Column(column)) => column.ret_type.clone().unwrap_or_else(|| ft(FieldTypeCode::LongLong)),
            _ => ft(FieldTypeCode::LongLong),
        },
    };
    AggFuncDesc {
        base: BaseFuncDesc { name: name.to_owned(), args, ret_type },
        mode: AggFunctionMode::Complete,
        has_distinct: false,
        order_by_items: Vec::new(),
        grouping_id: 0,
    }
}

/// Go `COUNT(1)`.
fn count_star() -> AggFuncDesc {
    agg_desc(names::COUNT, vec![Expression::Constant(Constant::new_one())])
}

/// Go `&expression.Column{Index: index, RetType: tp}`.
fn col_arg(index: i64, tp: FieldType) -> Expression {
    Expression::Column(tidb_expr::column::Column::new(index + 1, tp))
}

fn mapping(agg: AggFuncDesc, col_id: &[i64], dependency_col_id: &[i64]) -> Mapping {
    Mapping { agg_func: Some(agg), col_id: col_id.to_vec(), dependency_col_id: dependency_col_id.to_vec(), ..Mapping::default() }
}

/// Go `Open`, `Next` x2, `Close` with `ExecutorConcurrency = 2`; the collected
/// results.
fn run(mut exec: Exec) -> Result<Vec<ChunkResult>, ExecError> {
    let writer = CollectWriter::default();
    exec.writer = Some(Box::new(writer.clone()));
    exec.open(2)?;
    exec.next(false)?;
    exec.next(false)?;
    exec.close()?;
    let results = std::mem::take(&mut *writer.0.lock().unwrap());
    Ok(results)
}

fn open_err(mut exec: Exec) -> String {
    error_text(&exec.open(2).expect_err("Open fails"))
}

#[test]
fn test_merge_runtime_stats() {
    let ms = Duration::from_millis;
    let mut stats = MergeRuntimeStats::new(3);
    stats.reader_time = ms(15);
    stats.writer_time = ms(8);
    stats.merge_worker_time = vec![ms(10), ms(20), Duration::ZERO];
    let s = stats.to_string();
    for want in [
        "mview_delta_merge_agg",
        "total:3",
        "active:2",
        "min:10ms",
        "max:20ms",
        "avg:15ms",
        "reader:15ms",
        "writer:{time:8ms",
        "chunks:0",
        "row_ops:0",
    ] {
        assert!(s.contains(want), "{s} lacks {want}");
    }

    let mut stats = MergeRuntimeStats::new(2);
    stats.reader_time = ms(3);
    stats.writer_time = ms(4);
    stats.merge_worker_time = vec![ms(5), ms(7)];
    stats.writer_detail =
        MergeWriterStats { chunks: 1, row_ops: 11, noop_rows: 1, insert_rows: 3, update_rows: 5, delete_rows: 2 };
    let mut other = MergeRuntimeStats::new(3);
    other.reader_time = ms(2);
    other.writer_time = ms(1);
    other.merge_worker_time = vec![ms(1), ms(2), ms(9)];
    other.writer_detail =
        MergeWriterStats { chunks: 2, row_ops: 9, noop_rows: 2, insert_rows: 1, update_rows: 4, delete_rows: 2 };
    stats.merge(&other);
    assert_eq!(ms(5), stats.reader_time);
    assert_eq!(ms(5), stats.writer_time);
    assert_eq!(vec![ms(6), ms(9), ms(9)], stats.merge_worker_time);
    assert_eq!(
        MergeWriterStats { chunks: 3, row_ops: 20, noop_rows: 3, insert_rows: 4, update_rows: 9, delete_rows: 4 },
        stats.writer_detail
    );
    let clone = stats.clone();
    assert_eq!(stats, clone);
    assert_eq!(stats.to_string(), clone.to_string());
}

#[test]
fn test_count_and_nullable_sum() {
    // Schema: [0] delta_count(*), [1] delta_sum(b), [2] delta_count(b), [3] group_key,
    // [4] mv_count(*), [5] mv_sum(b), [6] mv_count(b).
    let ft_int = ft(FieldTypeCode::LongLong);
    let ft_dec = ft(FieldTypeCode::NewDecimal);
    let fts = vec![ft_int.clone(), ft_dec.clone(), ft_int.clone(), ft_int.clone(), ft_int.clone(), ft_dec.clone(), ft_int.clone()];
    let mut chk = Chunk::new_chunk_with_capacity(&fts, 3);
    chk.append_int64(0, 0);
    chk.append_my_decimal(1, &MyDecimal::from_int(-10));
    chk.append_int64(2, -1);
    chk.append_int64(3, 1);
    chk.append_int64(4, 1);
    chk.append_my_decimal(5, &MyDecimal::from_int(10));
    chk.append_int64(6, 1);

    chk.append_int64(0, -1);
    chk.append_my_decimal(1, &MyDecimal::from_int(-5));
    chk.append_int64(2, -1);
    chk.append_int64(3, 2);
    chk.append_int64(4, 1);
    chk.append_my_decimal(5, &MyDecimal::from_int(5));
    chk.append_int64(6, 1);

    chk.append_int64(0, 2);
    chk.append_null(1);
    chk.append_int64(2, 0);
    chk.append_int64(3, 3);
    chk.append_null(4);
    chk.append_null(5);
    chk.append_null(6);

    let mut exec = Exec::new(new_mock_source(&fts, vec![chk]));
    exec.agg_mappings = vec![
        mapping(count_star(), &[4], &[0]),
        mapping(agg_desc(names::COUNT, vec![col_arg(0, ft_int.clone())]), &[6], &[2]),
        mapping(agg_desc(names::SUM, vec![col_arg(1, ft_dec.clone())]), &[5], &[1, 6]),
    ];
    exec.delta_agg_col_count = 3;
    exec.worker_cnt = 2;
    let results = run(exec).unwrap();
    assert_eq!(1, results.len());
    let res = &results[0];
    assert_eq!(
        vec![RowOpType::Update, RowOpType::Delete, RowOpType::Insert],
        res.row_ops.iter().map(|op| op.tp).collect::<Vec<_>>()
    );
    let count_star_col = res.computed_cols[4].as_ref().unwrap();
    assert_eq!([1, 0, 2], [0, 1, 2].map(|row| count_star_col.get_int64(row)));
    let sum_col = res.computed_cols[5].as_ref().unwrap();
    assert!((0..3).all(|row| sum_col.is_null(row)));
}

#[test]
fn test_count_and_non_null_sum_real() {
    // Schema: [0] delta_count(*), [1] delta_sum(x), [2] group_key, [3] mv_count(*), [4] mv_sum(x).
    let ft_int = ft(FieldTypeCode::LongLong);
    let ft_real = ft(FieldTypeCode::Double);
    let fts = vec![ft_int.clone(), ft_real.clone(), ft_int.clone(), ft_int.clone(), ft_real.clone()];
    let mut chk = Chunk::new_chunk_with_capacity(&fts, 3);
    chk.append_int64(0, 1);
    chk.append_float64(1, 2.0);
    chk.append_int64(2, 1);
    chk.append_int64(3, 2);
    chk.append_float64(4, 3.0);

    chk.append_int64(0, 1);
    chk.append_float64(1, 1.5);
    chk.append_int64(2, 2);
    chk.append_null(3);
    chk.append_null(4);

    chk.append_int64(0, -1);
    chk.append_float64(1, -8.0);
    chk.append_int64(2, 3);
    chk.append_int64(3, 1);
    chk.append_float64(4, 8.0);

    let mut exec = Exec::new(new_mock_source(&fts, vec![chk]));
    exec.agg_mappings = vec![
        mapping(count_star(), &[3], &[0]),
        mapping(
            agg_desc(names::SUM, vec![col_arg(1, ft_flags(FieldTypeCode::Double, FieldTypeFlags::NOT_NULL))]),
            &[4],
            &[1],
        ),
    ];
    exec.delta_agg_col_count = 2;
    exec.worker_cnt = 2;
    let results = run(exec).unwrap();
    assert_eq!(1, results.len());
    let res = &results[0];
    assert_eq!(
        vec![RowOpType::Update, RowOpType::Insert, RowOpType::Delete],
        res.row_ops.iter().map(|op| op.tp).collect::<Vec<_>>()
    );
    assert_eq!(1, res.update_touched_stride);
    assert_eq!(2, res.update_touched_bit_cnt);
    assert_eq!(vec![0x3_u8], res.update_touched_bitmap);
    let sum_col = res.computed_cols[4].as_ref().unwrap();
    for (row, want) in [(0, 5.0), (1, 1.5), (2, 0.0)] {
        assert!((sum_col.get_float64(row) - want).abs() < 1e-9, "row {row}");
    }
}

#[test]
fn test_reject_sum_unsigned_delta() {
    // Schema: [0] delta_count(*), [1] delta_sum(x unsigned), [2] mv_count(*), [3] mv_sum(x signed).
    let ft_int = ft(FieldTypeCode::LongLong);
    let ft_uint = ft_flags(FieldTypeCode::LongLong, FieldTypeFlags::UNSIGNED);
    let fts = vec![ft_int.clone(), ft_uint, ft_int.clone(), ft_int.clone()];
    let mut chk = Chunk::new_chunk_with_capacity(&fts, 1);
    chk.append_int64(0, 0);
    chk.append_uint64(1, 1 << 63);
    chk.append_int64(2, 1);
    chk.append_int64(3, i64::MIN + 1);
    let mut exec = Exec::new(new_mock_source(&fts, vec![chk]));
    exec.agg_mappings = vec![
        mapping(count_star(), &[2], &[0]),
        mapping(
            agg_desc(names::SUM, vec![col_arg(1, ft_flags(FieldTypeCode::LongLong, FieldTypeFlags::NOT_NULL))]),
            &[3],
            &[1],
        ),
    ];
    exec.delta_agg_col_count = 2;
    exec.worker_cnt = 2;
    assert!(open_err(exec).contains("requires signed delta dependency"));
}

#[test]
fn test_reject_nullable_sum_count_dependency_from_delta() {
    let ft_int = ft(FieldTypeCode::LongLong);
    let fts = vec![ft_int.clone(); 6];
    let mut exec = Exec::new(new_mock_source(&fts, Vec::new()));
    exec.agg_mappings = vec![
        mapping(count_star(), &[3], &[0]),
        mapping(agg_desc(names::SUM, vec![col_arg(1, ft_int)]), &[4], &[1, 2]),
    ];
    exec.delta_agg_col_count = 3;
    assert!(open_err(exec).contains("must come from previously computed columns"));
}

/// One cell of a test chunk row.
enum Cell {
    I(i64),
    U(u64),
    N,
}

/// Appends one row of `cells` to `chk`.
fn push_row(chk: &mut Chunk, cells: &[Cell]) {
    for (col, cell) in cells.iter().enumerate() {
        match cell {
            Cell::I(v) => chk.append_int64(col, *v),
            Cell::U(v) => chk.append_uint64(col, *v),
            Cell::N => chk.append_null(col),
        }
    }
}

fn sum_not_null_int() -> AggFuncDesc {
    agg_desc(names::SUM, vec![col_arg(1, ft_flags(FieldTypeCode::LongLong, FieldTypeFlags::NOT_NULL))])
}

#[test]
fn test_count_and_non_null_sum_unsigned_int() {
    use Cell::{I, N, U};
    // Schema: [0] delta_count(*), [1] delta_sum(x), [2] mv_count(*), [3] mv_sum(x unsigned).
    let ft_int = ft(FieldTypeCode::LongLong);
    let ft_uint = ft_flags(FieldTypeCode::LongLong, FieldTypeFlags::UNSIGNED);
    let fts = vec![ft_int.clone(), ft_int.clone(), ft_int.clone(), ft_uint];
    let mut chk = Chunk::new_chunk_with_capacity(&fts, 3);
    push_row(&mut chk, &[I(0), I(-3), I(1), U(10)]);
    push_row(&mut chk, &[I(1), I(5), N, N]);
    push_row(&mut chk, &[I(-1), I(-2), I(1), U(2)]);
    let mut exec = Exec::new(new_mock_source(&fts, vec![chk]));
    exec.agg_mappings = vec![mapping(count_star(), &[2], &[0]), mapping(sum_not_null_int(), &[3], &[1])];
    exec.delta_agg_col_count = 2;
    exec.worker_cnt = 2;
    let results = run(exec).unwrap();
    assert_eq!(1, results.len());
    let res = &results[0];
    assert_eq!(
        vec![RowOpType::Update, RowOpType::Insert, RowOpType::Delete],
        res.row_ops.iter().map(|op| op.tp).collect::<Vec<_>>()
    );
    let sum_col = res.computed_cols[3].as_ref().unwrap();
    assert_eq!([7, 5, 0], [0, 1, 2].map(|row| sum_col.get_uint64(row)));
}

#[test]
fn test_count_and_nullable_sum_unsigned_int() {
    use Cell::{I, N, U};
    // Schema: [0] delta_count(*), [1] delta_sum(b), [2] delta_count(b), [3] group_key,
    // [4] mv_count(*), [5] mv_sum(b unsigned), [6] mv_count(b).
    let ft_int = ft(FieldTypeCode::LongLong);
    let ft_uint = ft_flags(FieldTypeCode::LongLong, FieldTypeFlags::UNSIGNED);
    let fts = vec![ft_int.clone(), ft_int.clone(), ft_int.clone(), ft_int.clone(), ft_int.clone(), ft_uint, ft_int.clone()];
    let mut chk = Chunk::new_chunk_with_capacity(&fts, 3);
    push_row(&mut chk, &[I(0), I(-3), I(-1), I(1), I(1), U(10), I(2)]);
    push_row(&mut chk, &[I(0), I(0), I(-1), I(2), I(1), U(5), I(1)]);
    push_row(&mut chk, &[I(1), I(5), I(2), I(3), N, N, N]);
    let mut exec = Exec::new(new_mock_source(&fts, vec![chk]));
    exec.agg_mappings = vec![
        mapping(count_star(), &[4], &[0]),
        mapping(agg_desc(names::COUNT, vec![col_arg(0, ft_int.clone())]), &[6], &[2]),
        mapping(agg_desc(names::SUM, vec![col_arg(1, ft_int)]), &[5], &[1, 6]),
    ];
    exec.delta_agg_col_count = 3;
    exec.worker_cnt = 2;
    let results = run(exec).unwrap();
    assert_eq!(1, results.len());
    let res = &results[0];
    assert_eq!(
        vec![RowOpType::Update, RowOpType::Update, RowOpType::Insert],
        res.row_ops.iter().map(|op| op.tp).collect::<Vec<_>>()
    );
    let sum_col = res.computed_cols[5].as_ref().unwrap();
    assert_eq!(7, sum_col.get_uint64(0));
    assert!(sum_col.is_null(1));
    assert_eq!(5, sum_col.get_uint64(2));
}

/// The four-column `count(*)` / `sum` schema the rejection tests open over.
fn open_err_with(fts: Vec<FieldType>, mappings: Vec<Mapping>, delta_agg_col_count: i64) -> String {
    let mut exec = Exec::new(new_mock_source(&fts, Vec::new()));
    exec.agg_mappings = mappings;
    exec.delta_agg_col_count = delta_agg_col_count;
    open_err(exec)
}

#[test]
fn test_reject_sum_delta_dependency_from_computed() {
    let ft_int = ft(FieldTypeCode::LongLong);
    let got = open_err_with(
        vec![ft_int.clone(); 4],
        vec![mapping(count_star(), &[2], &[0]), mapping(agg_desc(names::SUM, vec![col_arg(1, ft_int)]), &[3], &[2])],
        2,
    );
    assert!(got.contains("SUM mapping delta dependency col 2: must come from delta aggregation input"), "{got}");
}

#[test]
fn test_reject_sum_type_mismatch() {
    let ft_int = ft(FieldTypeCode::LongLong);
    let got = open_err_with(
        vec![ft_int.clone(), ft_int.clone(), ft_int.clone(), ft(FieldTypeCode::Double)],
        vec![mapping(count_star(), &[2], &[0]), mapping(agg_desc(names::SUM, vec![col_arg(1, ft_int)]), &[3], &[1])],
        2,
    );
    assert!(got.contains("SUM mapping type mismatch"), "{got}");
}

#[test]
fn test_reject_count_type_mismatch() {
    let got = open_err_with(
        vec![ft(FieldTypeCode::Double), ft(FieldTypeCode::LongLong)],
        vec![mapping(count_star(), &[1], &[0])],
        1,
    );
    assert!(got.contains("COUNT mapping dependency: eval type must be int"), "{got}");
}

#[test]
fn test_reject_count_unsigned_type() {
    let got = open_err_with(
        vec![ft_flags(FieldTypeCode::LongLong, FieldTypeFlags::UNSIGNED), ft(FieldTypeCode::LongLong)],
        vec![mapping(count_star(), &[1], &[0])],
        1,
    );
    assert!(got.contains("COUNT mapping dependency: type must be signed integer"), "{got}");
}

#[test]
fn test_reject_count_dependency_from_computed() {
    let ft_int = ft(FieldTypeCode::LongLong);
    let got = open_err_with(
        vec![ft_int.clone(); 4],
        vec![mapping(count_star(), &[2], &[0]), mapping(agg_desc(names::COUNT, vec![col_arg(1, ft_int)]), &[3], &[2])],
        2,
    );
    assert!(got.contains("COUNT mapping dependency col 2: must come from delta aggregation input"), "{got}");
}

#[test]
fn test_reject_nil_agg_func() {
    let ft_int = ft(FieldTypeCode::LongLong);
    let got = open_err_with(
        vec![ft_int.clone(), ft_int],
        vec![Mapping { agg_func: None, col_id: vec![1], dependency_col_id: vec![0], ..Mapping::default() }],
        0,
    );
    assert!(got.contains("requires AggFunc"), "{got}");
}

#[test]
fn test_reject_first_count_not_all_rows() {
    let ft_int = ft(FieldTypeCode::LongLong);
    let got = open_err_with(
        vec![ft_int.clone(), ft_int.clone()],
        vec![mapping(agg_desc(names::COUNT, vec![col_arg(0, ft_int)]), &[1], &[0])],
        0,
    );
    assert!(got.contains("must be COUNT(*)/COUNT(non-NULL constant)"), "{got}");
}

#[test]
fn test_allow_first_count_const_two() {
    let ft_int = ft(FieldTypeCode::LongLong);
    let fts = vec![ft_int.clone(), ft_int.clone()];
    let count_two = agg_desc(
        names::COUNT,
        vec![Expression::Constant(Constant::new(tidb_datatype::Datum::Int(2), ft_int))],
    );
    let mut exec = Exec::new(new_mock_source(&fts, Vec::new()));
    exec.agg_mappings = vec![mapping(count_two, &[1], &[0])];
    exec.delta_agg_col_count = 1;
    exec.open(2).unwrap();
    exec.close().unwrap();
}

#[test]
fn test_reject_dependency_from_mv_side() {
    let ft_int = ft(FieldTypeCode::LongLong);
    let got = open_err_with(
        vec![ft_int.clone(); 3],
        // Dependency col 2 is on MView side (DeltaAggColCount = 1).
        vec![mapping(count_star(), &[1], &[0]), mapping(sum_not_null_int(), &[2], &[2])],
        1,
    );
    assert!(got.contains("expect delta agg range"), "{got}");
}

/// Go `executil.MarkTouchedRowsByColumn(rows, bitmap, stride, bit, chk.Column(0), chk.Column(1), ft, notNull, ...)`.
fn mark(chk: &Chunk, rows: &[usize], bitmap: &mut [u8], stride: usize, bit_pos: usize, tp: &FieldType, not_null: bool) {
    tidb_executor_internal_util::mark_touched_rows_by_column(
        rows,
        bitmap,
        stride,
        bit_pos,
        &chk.column(0),
        &chk.column(1),
        tp,
        not_null,
        "aggregate change",
    )
    .unwrap();
}

#[test]
fn test_mark_update_touched_rows_by_column_string_collation() {
    let mut tp = ft(FieldTypeCode::VarString);
    tp.set_charset_name("utf8mb4");
    tp.set_collation_name("utf8mb4_general_ci");
    let mut chk = Chunk::new_chunk_with_capacity(&[tp.clone(), tp.clone()], 4);
    chk.append_string(0, "A");
    chk.append_string(1, "a");
    chk.append_string(0, "Ab");
    chk.append_string(1, "ac");
    chk.append_null(0);
    chk.append_null(1);
    chk.append_null(0);
    chk.append_string(1, "x");
    let mut bitmap = vec![0_u8; 4];
    mark(&chk, &[0, 1, 2, 3], &mut bitmap, 1, 0, &tp, false);
    assert_eq!(vec![1, 1, 0, 1], bitmap);
}

#[test]
fn test_mark_update_touched_rows_by_column_decimal_value_comparison() {
    let mut tp = ft(FieldTypeCode::NewDecimal);
    tp.set_flen(32);
    tp.set_decimal(0);
    // Stored values and aggregate results can use different internal precisions.
    let compact = MyDecimal::from_string(b"50").0;
    let compact_dec = tidb_datatype::Decimal::from_my_decimal(&compact);
    let (encoded, _) = compact_dec.to_bin(32, 0).unwrap();
    let padded = tidb_datatype::Decimal::from_bin(&encoded, 32, 0).unwrap().0.to_my_decimal().unwrap();
    assert_eq!(std::cmp::Ordering::Equal, padded.compare(&compact));
    let fifty_one = MyDecimal::from_string(b"51").0;
    let rows: Vec<[Option<&MyDecimal>; 2]> = vec![
        [Some(&padded), Some(&compact)],
        [Some(&compact), Some(&padded)],
        [Some(&compact), Some(&compact)],
        [Some(&padded), Some(&padded)],
        [Some(&padded), Some(&fifty_one)],
        [None, None],
        [None, Some(&compact)],
        [Some(&compact), None],
    ];
    let mut chk = Chunk::new_chunk_with_capacity(&[tp.clone(), tp.clone()], rows.len());
    for row in &rows {
        for (col, value) in row.iter().enumerate() {
            match value {
                Some(value) => chk.append_my_decimal(col, value),
                None => chk.append_null(col),
            }
        }
    }
    let mut bitmap = vec![0_u8; rows.len()];
    mark(&chk, &[0, 1, 2, 3, 4, 5, 6, 7], &mut bitmap, 1, 0, &tp, false);
    assert_eq!(vec![0, 0, 0, 0, 1, 0, 1, 1], bitmap);
}

#[test]
fn test_mark_update_touched_rows_by_column_float_value_comparison() {
    let negative_zero = -0.0_f64;
    // (old, new, old_null, new_null)
    let rows = [
        (0.0, negative_zero, false, false),
        (1.0, 9.0, false, false),
        (negative_zero, 0.0, false, false),
        (2.0, 3.0, false, false),
        (4.0, 4.0, false, false),
        (5.0, 5.0, true, true),
        (6.0, 7.0, true, false),
        (7.0, 8.0, false, true),
    ];
    for code in [FieldTypeCode::Float, FieldTypeCode::Double] {
        for not_null in [false, true] {
            let tp = if not_null { ft_flags(code, FieldTypeFlags::NOT_NULL) } else { ft(code) };
            let mut chk = Chunk::new_chunk_with_capacity(&[tp.clone(), tp.clone()], rows.len());
            for &(old, new, old_null, new_null) in &rows {
                for (col, value, is_null) in [(0, old, old_null), (1, new, new_null)] {
                    if is_null && !not_null {
                        chk.append_null(col);
                    } else if code == FieldTypeCode::Float {
                        chk.append_float32(col, value as f32);
                    } else {
                        chk.append_float64(col, value);
                    }
                }
            }
            for (stride, bit_pos, expected) in [
                (1, 3, vec![0x48_u8, 0x40, 0x48, 0x40, 0x40, 0x48]),
                (2, 11, vec![0x40, 0x48, 0x40, 0x40, 0x40, 0x48, 0x40, 0x40, 0x40, 0x40, 0x40, 0x48]),
            ] {
                let update_rows = [7, 0, 3, 2, 5, 6];
                let mut bitmap = vec![0x40_u8; update_rows.len() * stride];
                mark(&chk, &update_rows, &mut bitmap, stride, bit_pos, &tp, not_null);
                assert_eq!(expected, bitmap, "{code:?} not_null={not_null} stride={stride}");
            }
        }
    }
}

#[test]
fn test_mark_update_touched_rows_by_column_bit_value_comparison() {
    for (flen, old_value, new_value) in [
        (8, vec![2_u8], vec![3_u8]),
        (64, vec![0x80, 0, 0, 0, 0, 0, 0, 2], vec![0x80, 0, 0, 0, 0, 0, 0, 3]),
    ] {
        for not_null in [false, true] {
            let mut tp = ft(FieldTypeCode::Bit);
            tp.set_flen(flen);
            let (old, new) = (Some(old_value.as_slice()), Some(new_value.as_slice()));
            let (rows, want): (Vec<[Option<&[u8]>; 2]>, Vec<u8>) = if not_null {
                tp.add_flags(FieldTypeFlags::NOT_NULL);
                (vec![[old, new], [old, old], [new, new], [new, old]], vec![1, 0, 0, 1])
            } else {
                (vec![[None, None], [old, new], [old, old], [None, old], [old, None], [None, None], [old, new]], vec![0, 1, 0, 1, 1, 0, 1])
            };
            let mut chk = Chunk::new_chunk_with_capacity(&[tp.clone(), tp.clone()], rows.len());
            for row in &rows {
                for (col, value) in row.iter().enumerate() {
                    match value {
                        Some(value) => chk.append_bytes(col, value),
                        None => chk.append_null(col),
                    }
                }
            }
            let update_rows: Vec<usize> = (0..rows.len()).collect();
            let mut bitmap = vec![0_u8; rows.len()];
            mark(&chk, &update_rows, &mut bitmap, 1, 0, &tp, not_null);
            assert_eq!(want, bitmap, "bit{flen} not_null={not_null}");
        }
    }
    // "fixed integer projection"
    let mut tp = ft(FieldTypeCode::Bit);
    tp.set_flen(64);
    tp.add_flags(FieldTypeFlags::UNSIGNED | FieldTypeFlags::NOT_NULL);
    let physical = ft_flags(FieldTypeCode::LongLong, FieldTypeFlags::UNSIGNED | FieldTypeFlags::NOT_NULL);
    let mut chk = Chunk::new_chunk_with_capacity(&[physical.clone(), physical], 3);
    for [old, new] in [[0_u64, 0], [1 << 63, 1 << 63], [1 << 63, (1 << 63) + 1]] {
        chk.append_uint64(0, old);
        chk.append_uint64(1, new);
    }
    let mut bitmap = vec![0_u8; 3];
    mark(&chk, &[0, 1, 2], &mut bitmap, 1, 0, &tp, true);
    assert_eq!(vec![0, 0, 1], bitmap);
}

#[test]
fn test_mark_update_touched_rows_by_column_enum_set_use_binary_name_compare() {
    let mut enum_tp = ft(FieldTypeCode::Enum);
    enum_tp.set_charset_name("utf8mb4");
    enum_tp.set_collation_name("utf8mb4_general_ci");
    let mut enum_chk = Chunk::new_chunk_with_capacity(&[enum_tp.clone(), enum_tp.clone()], 1);
    enum_chk.append_enum(0, &tidb_datatype::MysqlEnum::new("x", 1));
    enum_chk.append_enum(1, &tidb_datatype::MysqlEnum::new("x", 2));
    let mut enum_bitmap = vec![0_u8; 1];
    mark(&enum_chk, &[0], &mut enum_bitmap, 1, 0, &enum_tp, true);
    assert_eq!(vec![0], enum_bitmap);

    let mut set_tp = ft(FieldTypeCode::Set);
    set_tp.set_charset_name("utf8mb4");
    set_tp.set_collation_name("utf8mb4_general_ci");
    let mut set_chk = Chunk::new_chunk_with_capacity(&[set_tp.clone(), set_tp.clone()], 1);
    set_chk.append_set(0, &tidb_datatype::MysqlSet::new("a,b", 3));
    set_chk.append_set(1, &tidb_datatype::MysqlSet::new("a,b", 7));
    let mut set_bitmap = vec![0_u8; 1];
    mark(&set_chk, &[0], &mut set_bitmap, 1, 0, &set_tp, true);
    assert_eq!(vec![0], set_bitmap);
}

#[test]
fn test_no_op_when_agg_value_unchanged() {
    let ft_int = ft(FieldTypeCode::LongLong);
    let ft_real = ft(FieldTypeCode::Double);
    let fts = vec![ft_int.clone(), ft_real.clone(), ft_int.clone(), ft_real];
    let mut chk = Chunk::new_chunk_with_capacity(&fts, 1);
    chk.append_int64(0, 0);
    chk.append_float64(1, 0.0);
    chk.append_int64(2, 7);
    chk.append_float64(3, 3.5);
    let mut exec = Exec::new(new_mock_source(&fts, vec![chk]));
    exec.agg_mappings = vec![
        mapping(count_star(), &[2], &[0]),
        mapping(
            agg_desc(names::SUM, vec![col_arg(1, ft_flags(FieldTypeCode::Double, FieldTypeFlags::NOT_NULL))]),
            &[3],
            &[1],
        ),
    ];
    exec.delta_agg_col_count = 2;
    let results = run(exec).unwrap();
    assert_eq!(1, results.len());
    let res = &results[0];
    assert_eq!(vec![RowOpType::Update], res.row_ops.iter().map(|op| op.tp).collect::<Vec<_>>());
    assert_eq!(1, res.update_touched_stride);
    assert_eq!(2, res.update_touched_bit_cnt);
    assert_eq!(0, res.update_touched_bitmap[0]);

    let mut table_writer = crate::writer::TableResultWriter::for_touched_test(vec![0, 1]);
    let changed = table_writer.build_touched_from_bitmap(&res.update_touched_bitmap, res.update_touched_stride, 0);
    assert!(!changed);
    assert_eq!(vec![false, false], table_writer.touched);
}

/// Go `tables.MockTableFromMeta` of a two-column view.
struct MockTargetTable(Vec<crate::writer::TargetColumn>);

impl crate::writer::TargetTable for MockTargetTable {
    fn writable_cols(&self) -> Vec<crate::writer::TargetColumn> {
        self.0.clone()
    }

    fn public_col_count(&self) -> usize {
        self.0.len()
    }

    fn add_record(&mut self, _: &[tidb_datatype::Datum], _: usize) -> Result<(), ExecError> {
        Ok(())
    }

    fn update_record(&mut self, _: &tidb_txnkv::Handle, _: &[tidb_datatype::Datum], _: &[tidb_datatype::Datum], _: &[bool]) -> Result<(), ExecError> {
        Ok(())
    }

    fn remove_record(&mut self, _: &tidb_txnkv::Handle, _: &[tidb_datatype::Datum]) -> Result<(), ExecError> {
        Ok(())
    }

    fn may_flush(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    fn shard_allocate_step(&self) -> i64 {
        1
    }

    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        tidb_datatype::SessionTimeZone::utc()
    }
}

#[test]
fn test_table_result_writer_column_offsets() {
    for (name, offsets, want_err) in [
        ("dense", [0, 1], ""),
        ("reordered", [1, 0], "column `a` offset 1 != writable ordinal 0"),
        ("gapped", [0, 2], "column `b` offset 2 != writable ordinal 1"),
    ] {
        let tp = ft(FieldTypeCode::LongLong);
        let target = MockTargetTable(vec![
            crate::writer::TargetColumn { name: "a".to_owned(), offset: offsets[0], field_type: tp.clone() },
            crate::writer::TargetColumn { name: "b".to_owned(), offset: offsets[1], field_type: tp.clone() },
        ]);
        let mut handle_col = tidb_expr::column::Column::new(2, tp.clone());
        handle_col.index = 1;
        let writer = crate::writer::build_table_result_writer(
            Box::new(target),
            tidb_planner_util::handle_cols::new_int_handle_cols(handle_col),
            &[tp.clone(), tp.clone(), tp],
            1,
            &[2],
        );
        match writer {
            Err(error) => {
                assert!(!want_err.is_empty(), "{name}: {}", error_text(&error));
                assert!(error_text(&error).contains(want_err), "{name}: {}", error_text(&error));
            }
            Ok(_) => assert!(want_err.is_empty(), "{name}"),
        }
    }
}

use crate::recompute::{
    MinMaxBatchBuildRequest, MinMaxBatchExecBuilder, MinMaxRecomputeExec, MinMaxRecomputeSingleRowExec,
    MinMaxRecomputeSingleRowWorker, MinMaxRecomputeSpec, MinMaxRecomputeStrategy,
};

/// An executor over fixed rows of int64 cells (`None` = NULL), one batch
/// per `Next` call after `Open`; Go's mock recompute executors' shape.
struct RowsExec {
    meta: ExecutorMeta,
    rows: Vec<Vec<Option<i64>>>,
    idx: usize,
}

fn rows_exec(col_cnt: usize, rows: Vec<Vec<Option<i64>>>) -> RowsExec {
    let cols = (0..col_cnt).map(|i| tidb_expr::column::Column::new(i as i64 + 1, ft(FieldTypeCode::LongLong))).collect();
    RowsExec { meta: ExecutorMeta::new(Schema::new(cols), 0, 32, 1024), rows, idx: 0 }
}

impl Executor for RowsExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.idx = 0;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        while self.idx < self.rows.len() && req.num_rows() < req.capacity() {
            for (col, cell) in self.rows[self.idx].iter().enumerate() {
                match cell {
                    Some(v) => req.append_int64(col, *v),
                    None => req.append_null(col),
                }
            }
            self.idx += 1;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

/// Go `mockSingleRowRecomputeExec` (`multi`: `mockSingleRowRecomputeExecMulti`):
/// on each `Open`, the rows for the bound key.
struct MockSingleRowRecomputeExec {
    inner: RowsExec,
    key: Arc<std::sync::RwLock<tidb_datatype::Datum>>,
    values: std::collections::HashMap<i64, Vec<i64>>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl Executor for MockSingleRowRecomputeExec {
    fn open(&mut self) -> Result<(), ExecError> {
        let key = match &*self.key.read().unwrap() {
            tidb_datatype::Datum::Int(v) => *v,
            other => panic!("unexpected key {other:?}"),
        };
        self.inner.rows = match self.values.get(&key) {
            Some(values) => values.iter().map(|v| vec![Some(*v)]).collect(),
            // Go's single mock answers NULL for an unknown key.
            None => vec![vec![None]],
        };
        self.inner.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if self.inner.idx == 0 {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner.next(req)
    }

    fn close(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.inner.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.inner.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.inner.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.inner.new_chunk()
    }
}

/// Go `mockBatchRecomputeBuilder`: rows `(key, value)` for the lookup keys
/// (NULL values for `null_keys`), `extra_rows` after them, all reversed
/// when `reverse_output`.
#[derive(Clone, Default)]
struct MockBatchRecomputeBuilder {
    values: std::collections::HashMap<i64, i64>,
    null_keys: std::collections::HashSet<i64>,
    reverse_output: bool,
    extra_rows: Vec<[i64; 2]>,
    /// Go `retTp`: the result column types (default two BIGINTs).
    ret_tp: Vec<FieldType>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl MinMaxBatchExecBuilder for MockBatchRecomputeBuilder {
    fn build(&self, req: &MinMaxBatchBuildRequest) -> Result<Box<dyn Executor>, ExecError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut rows: Vec<Vec<Option<i64>>> = req
            .lookup_keys
            .iter()
            .map(|key| {
                let tidb_datatype::Datum::Int(k) = key.keys[0] else { panic!("int keys") };
                let value = if self.null_keys.contains(&k) { None } else { Some(self.values.get(&k).copied().unwrap_or(0)) };
                vec![Some(k), value]
            })
            .collect();
        rows.extend(self.extra_rows.iter().map(|[k, v]| vec![Some(*k), Some(*v)]));
        if self.reverse_output {
            rows.reverse();
        }
        let mut exec = rows_exec(2, rows);
        if !self.ret_tp.is_empty() {
            let cols = self.ret_tp.iter().enumerate().map(|(i, tp)| tidb_expr::column::Column::new(i as i64 + 1, tp.clone())).collect();
            exec.meta = ExecutorMeta::new(Schema::new(cols), 0, 32, 1024);
        }
        Ok(Box::new(exec))
    }
}

/// One single-row worker slot over `values` per key.
fn single_row_worker(
    values: &[(i64, Vec<i64>)],
    calls: &Arc<std::sync::atomic::AtomicUsize>,
) -> MinMaxRecomputeSingleRowWorker {
    let key_col = tidb_expr::column::CorrelatedColumn::new(tidb_expr::column::Column::new(100, ft(FieldTypeCode::LongLong)));
    let exec = MockSingleRowRecomputeExec {
        inner: rows_exec(1, Vec::new()),
        key: key_col.data.clone().expect("bound"),
        values: values.iter().cloned().collect(),
        calls: Arc::clone(calls),
    };
    MinMaxRecomputeSingleRowWorker { key_cols: vec![key_col], exec: Mutex::new(Box::new(exec)) }
}

fn not_null_int_arg(index: i64) -> Expression {
    col_arg(index, ft_flags(FieldTypeCode::LongLong, FieldTypeFlags::NOT_NULL))
}

#[test]
fn test_reject_min_max_delta_dependency_from_computed() {
    let ft_int = ft(FieldTypeCode::LongLong);
    for dep_idx in 0..4 {
        let mut deps = vec![1, 2, 3, 4];
        // Force the dependency to come from the computed output col.
        deps[dep_idx] = 6;
        let mut exec = Exec::new(new_mock_source(&vec![ft_int.clone(); 8], Vec::new()));
        exec.agg_mappings = vec![
            mapping(count_star(), &[6], &[0]),
            Mapping {
                min_max_recompute: Some(MinMaxRecomputeSpec {
                    strategy: MinMaxRecomputeStrategy::Batch,
                    batch_result_col_idxes: vec![1],
                    ..MinMaxRecomputeSpec::default()
                }),
                ..mapping(agg_desc(names::MIN, vec![not_null_int_arg(1)]), &[7], &deps)
            },
        ];
        exec.delta_agg_col_count = 6;
        exec.min_max_recompute = Some(MinMaxRecomputeExec {
            key_input_col_ids: vec![5],
            key_result_col_idxes: vec![0],
            batch_builder: Some(Box::new(MockBatchRecomputeBuilder::default())),
        });
        let got = open_err(exec);
        assert!(got.contains("must come from delta aggregation input"), "dep {dep_idx}: {got}");
    }
}

#[test]
fn test_reject_min_max_for_stage1() {
    let ft_int = ft(FieldTypeCode::LongLong);
    let got = open_err_with(
        vec![ft_int.clone(); 4],
        vec![mapping(count_star(), &[1], &[0]), mapping(agg_desc(names::MIN, vec![col_arg(0, ft_int)]), &[2, 3], &[0])],
        1,
    );
    assert!(got.contains("requires MinMaxRecompute metadata"), "{got}");
}

/// Appends rows of int64 cells (`None` = NULL) to a new chunk of `fts`.
fn int_chunk(fts: &[FieldType], rows: &[&[Option<i64>]]) -> Chunk {
    let mut chk = Chunk::new_chunk_with_capacity(fts, rows.len());
    for row in rows {
        for (col, cell) in row.iter().enumerate() {
            match cell {
                Some(v) => chk.append_int64(col, *v),
                None => chk.append_null(col),
            }
        }
    }
    chk
}

#[test]
fn test_max_fast_path_and_single_row_recompute() {
    // Schema:
    // [0] delta_count(*), [1] delta_max_added(v), [2] delta_max_added_cnt(v),
    // [3] delta_max_removed(v), [4] delta_max_removed_cnt(v), [5] group_key,
    // [6] mv_count(*), [7] mv_max(v).
    let ft_int = ft(FieldTypeCode::LongLong);
    let fts = vec![ft_int.clone(); 8];
    let chk = int_chunk(
        &fts,
        &[
            // row0: fast update max 10 -> 12
            &[Some(0), Some(12), Some(1), None, Some(0), Some(1), Some(3), Some(10)],
            // row1: fallback (remove old max 10), single-row recompute returns 9
            &[Some(0), None, Some(0), Some(10), Some(1), Some(2), Some(3), Some(10)],
            // row2: no change
            &[Some(0), None, Some(0), None, Some(0), Some(3), Some(3), Some(5)],
        ],
    );
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut exec = Exec::new(new_mock_source(&fts, vec![chk]));
    exec.agg_mappings = vec![
        mapping(count_star(), &[6], &[0]),
        Mapping {
            min_max_recompute: Some(MinMaxRecomputeSpec {
                strategy: MinMaxRecomputeStrategy::SingleRow,
                single_row: Some(MinMaxRecomputeSingleRowExec { workers: vec![single_row_worker(&[(2, vec![9])], &calls)] }),
                ..MinMaxRecomputeSpec::default()
            }),
            ..mapping(agg_desc(names::MAX, vec![not_null_int_arg(1)]), &[7], &[1, 2, 3, 4])
        },
    ];
    exec.delta_agg_col_count = 6;
    exec.worker_cnt = 1;
    exec.min_max_recompute =
        Some(MinMaxRecomputeExec { key_input_col_ids: vec![5], key_result_col_idxes: vec![0], batch_builder: None });
    let results = run(exec).unwrap();
    assert_eq!(1, results.len());
    let res = &results[0];
    assert_eq!(vec![RowOpType::Update; 3], res.row_ops.iter().map(|op| op.tp).collect::<Vec<_>>());
    let max_col = res.computed_cols[7].as_ref().unwrap();
    assert_eq!([12, 9, 5], [0, 1, 2].map(|row| max_col.get_int64(row)));
    assert_eq!(1, calls.load(std::sync::atomic::Ordering::SeqCst));
}

/// Go's two-row MAX fallback input of the batch tests: rows of
/// `[delta_count, added, added_cnt, removed, removed_cnt, key, mv_count, mv_max]`.
fn max_schema() -> Vec<FieldType> {
    vec![ft(FieldTypeCode::LongLong); 8]
}

/// The `count(*)` + batch `MAX` exec over `chk`.
fn max_batch_exec(chk: Chunk, builder: MockBatchRecomputeBuilder) -> Exec {
    let mut exec = Exec::new(new_mock_source(&max_schema(), vec![chk]));
    exec.agg_mappings = vec![
        mapping(count_star(), &[6], &[0]),
        Mapping {
            min_max_recompute: Some(MinMaxRecomputeSpec {
                strategy: MinMaxRecomputeStrategy::Batch,
                batch_result_col_idxes: vec![1],
                ..MinMaxRecomputeSpec::default()
            }),
            ..mapping(agg_desc(names::MAX, vec![not_null_int_arg(1)]), &[7], &[1, 2, 3, 4])
        },
    ];
    exec.delta_agg_col_count = 6;
    exec.worker_cnt = 1;
    exec.min_max_recompute = Some(MinMaxRecomputeExec {
        key_input_col_ids: vec![5],
        key_result_col_idxes: vec![0],
        batch_builder: Some(Box::new(builder)),
    });
    exec
}

/// Go: `Open` succeeds, `Next` fails, `Close` succeeds; the failure's text.
fn next_err(mut exec: Exec) -> String {
    exec.open(1).unwrap();
    let error = exec.next(false).expect_err("Next fails");
    exec.close().unwrap();
    error_text(&error)
}

/// One fallback row removing the old max `10` for key 1.
const FALLBACK_KEY1: [Option<i64>; 8] = [Some(0), None, Some(0), Some(10), Some(1), Some(1), Some(3), Some(10)];

#[test]
fn test_max_batch_recompute() {
    let chk = int_chunk(
        &max_schema(),
        &[
            // row0: fallback, batch recompute returns 8
            &FALLBACK_KEY1,
            // row1: fast update max 10 -> 12
            &[Some(0), Some(12), Some(1), None, Some(0), Some(2), Some(3), Some(10)],
        ],
    );
    let builder = MockBatchRecomputeBuilder { values: [(1, 8), (2, 12)].into(), ..MockBatchRecomputeBuilder::default() };
    let results = run(max_batch_exec(chk, builder)).unwrap();
    assert_eq!(1, results.len());
    let res = &results[0];
    assert_eq!(vec![RowOpType::Update; 2], res.row_ops.iter().map(|op| op.tp).collect::<Vec<_>>());
    let max_col = res.computed_cols[7].as_ref().unwrap();
    assert_eq!([8, 12], [0, 1].map(|row| max_col.get_int64(row)));
}

#[test]
fn test_max_batch_recompute_with_out_of_order_result_rows() {
    let chk = int_chunk(
        &max_schema(),
        &[&FALLBACK_KEY1, &[Some(0), None, Some(0), Some(20), Some(1), Some(2), Some(4), Some(20)]],
    );
    let builder = MockBatchRecomputeBuilder {
        reverse_output: true,
        values: [(1, 8), (2, 15)].into(),
        ..MockBatchRecomputeBuilder::default()
    };
    let results = run(max_batch_exec(chk, builder)).unwrap();
    let res = &results[0];
    assert_eq!(vec![RowOpType::Update; 2], res.row_ops.iter().map(|op| op.tp).collect::<Vec<_>>());
    let max_col = res.computed_cols[7].as_ref().unwrap();
    assert_eq!([8, 15], [0, 1].map(|row| max_col.get_int64(row)));
}

#[test]
fn test_reject_max_batch_recompute_unexpected_key() {
    let chk = int_chunk(&max_schema(), &[&FALLBACK_KEY1]);
    let builder = MockBatchRecomputeBuilder {
        values: [(1, 8)].into(),
        extra_rows: vec![[999, 1]],
        ..MockBatchRecomputeBuilder::default()
    };
    let got = next_err(max_batch_exec(chk, builder));
    assert!(got.contains("returns a key outside lookup set"), "{got}");
}

#[test]
fn test_reject_max_batch_recompute_key_type_mismatch() {
    let chk = int_chunk(&max_schema(), &[&FALLBACK_KEY1]);
    let builder = MockBatchRecomputeBuilder {
        ret_tp: vec![ft(FieldTypeCode::Double), ft(FieldTypeCode::LongLong)],
        values: [(1, 8)].into(),
        ..MockBatchRecomputeBuilder::default()
    };
    let got = next_err(max_batch_exec(chk, builder));
    assert!(got.contains("min/max batch key type mismatch"), "{got}");
}

#[test]
fn test_reject_max_batch_recompute_result_type_mismatch() {
    let chk = int_chunk(&max_schema(), &[&FALLBACK_KEY1]);
    let builder = MockBatchRecomputeBuilder {
        ret_tp: vec![ft(FieldTypeCode::LongLong), ft(FieldTypeCode::Double)],
        values: [(1, 8)].into(),
        ..MockBatchRecomputeBuilder::default()
    };
    let got = next_err(max_batch_exec(chk, builder));
    assert!(got.contains("min/max batch result type mismatch"), "{got}");
}

#[test]
fn test_reject_max_single_row_recompute_multiple_rows() {
    let chk = int_chunk(&max_schema(), &[&FALLBACK_KEY1]);
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut exec = Exec::new(new_mock_source(&max_schema(), vec![chk]));
    exec.agg_mappings = vec![
        mapping(count_star(), &[6], &[0]),
        Mapping {
            min_max_recompute: Some(MinMaxRecomputeSpec {
                strategy: MinMaxRecomputeStrategy::SingleRow,
                single_row: Some(MinMaxRecomputeSingleRowExec { workers: vec![single_row_worker(&[(1, vec![8, 9])], &calls)] }),
                ..MinMaxRecomputeSpec::default()
            }),
            ..mapping(agg_desc(names::MAX, vec![not_null_int_arg(1)]), &[7], &[1, 2, 3, 4])
        },
    ];
    exec.delta_agg_col_count = 6;
    exec.worker_cnt = 1;
    exec.min_max_recompute =
        Some(MinMaxRecomputeExec { key_input_col_ids: vec![5], key_result_col_idxes: vec![0], batch_builder: None });
    let got = next_err(exec);
    assert!(got.contains("returns more than one row"), "{got}");
}

/// Go `types.NewBinaryLiteralFromUint(v, byteSize)`: big-endian bytes.
fn binary_literal(v: u64, byte_size: usize) -> Vec<u8> {
    v.to_be_bytes()[8 - byte_size..].to_vec()
}

#[test]
fn test_min_max_bit_merge() {
    for (name, flen, low, high) in [
        ("bit1", 1_i64, 0_u64, 1_u64),
        ("bit8", 8, 127, 128),
        ("bit16", 16, 255, 256),
        ("bit64", 64, i64::MAX as u64, 1 << 63),
    ] {
        for agg_name in [names::MIN, names::MAX] {
            let mut ft_bit = ft(FieldTypeCode::Bit);
            ft_bit.set_flen(flen);
            ft_bit.set_collation_name("binary");
            ft_bit.add_flags(FieldTypeFlags::UNSIGNED);
            let ft_int = ft(FieldTypeCode::LongLong);
            let fts = vec![ft_bit.clone(), ft_int.clone(), ft_bit.clone(), ft_int.clone(), ft_bit.clone(), ft_int];
            let byte_size = ((flen + 7) / 8) as usize;
            let low = binary_literal(low, byte_size);
            let high = binary_literal(high, byte_size);
            let max_value = binary_literal(u64::MAX >> (64 - flen), byte_size);
            // (old, added, removed, count)
            let rows: Vec<(Option<&Vec<u8>>, Option<&Vec<u8>>, Option<&Vec<u8>>, i64)> = vec![
                (Some(&low), Some(&high), None, 2),
                (Some(&high), Some(&low), None, 2),
                (None, Some(&high), None, 1),
                (Some(&high), None, Some(&high), 1), // Recompute the removed extremum.
                (None, None, None, 1),               // An all-NULL group.
                (Some(&low), None, None, 0),         // A deleted group.
                (Some(&low), None, None, 2),
                (Some(&high), Some(&max_value), None, 2),
            ];
            let mut input = Chunk::new_chunk_with_capacity(&fts, rows.len());
            for (old, added, removed, count) in &rows {
                for (i, value) in [added, removed, old].into_iter().enumerate() {
                    match value {
                        None => {
                            input.append_null(2 * i);
                            if i < 2 {
                                input.append_int64(2 * i + 1, 0);
                            }
                        }
                        Some(value) => {
                            input.append_bytes(2 * i, value);
                            if i < 2 {
                                input.append_int64(2 * i + 1, 1);
                            }
                        }
                    }
                }
                input.append_int64(5, *count);
            }
            let desc = agg_desc(agg_name, vec![col_arg(0, ft_bit.clone())]);
            let mut exec = Exec::new(new_mock_source(&[], Vec::new()));
            exec.agg_mappings = vec![Mapping { col_id: vec![5], ..Mapping::default() }];
            exec.delta_agg_col_count = 4;
            let merger = exec
                .build_min_max_merger(
                    0,
                    &mapping(desc, &[4], &[0, 1, 2, 3]),
                    &std::collections::HashMap::from([(5, 0)]),
                    &fts,
                )
                .unwrap();
            let mut worker_data = crate::exec::MergeWorkerData::default();
            worker_data.prepare_min_max_recompute(1);
            let mut output = vec![None];
            merger
                .merge_chunk(&input, &[input.column(5).copy_construct()], &mut output, &mut worker_data)
                .unwrap();
            assert_eq!(vec![vec![3_usize]], worker_data.min_max_mapping_idx_to_rows, "{name} {agg_name}");
            let mut want: Vec<Option<&Vec<u8>>> =
                vec![Some(&low), Some(&low), Some(&high), None, None, None, Some(&low), Some(&high)];
            if agg_name == names::MAX {
                want[0] = Some(&high);
                want[1] = Some(&high);
                want[7] = Some(&max_value);
            }
            let out = output[0].as_ref().unwrap();
            for (row_idx, value) in want.into_iter().enumerate() {
                match value {
                    None => assert!(out.is_null(row_idx), "{name} {agg_name} row {row_idx}"),
                    Some(value) => assert_eq!(value.as_slice(), out.get_raw(row_idx).as_ref(), "{name} {agg_name} row {row_idx}"),
                }
            }
        }
    }
}
