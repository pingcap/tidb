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

#![allow(missing_docs)]

//! Go `pkg/executor/windows`: `WindowExec`, `PipelinedWindowExec` and
//! `OrderedWindowExec` built by `windows.Build` / `BuildOrdered` over the
//! `AggFunc`s `aggfuncs.BuildWindowFunctions` builds.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_driver_error::ExecError;
use tidb_executor::mem_table::MemTableSourceExec;
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_executor_windows::WindowPlan;
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expression_aggregation::WindowFuncDesc;
use tidb_exprctx::{Columns, NoColumns};
use tidb_planner_core_operator_logicalop::window::{BoundType, FrameBound, FrameType, WindowFrame};

fn long() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn schema(width: usize) -> Schema {
    Schema::new((0..width).map(col).collect())
}

fn col(index: usize) -> Column {
    let mut column = Column::new(index as i64 + 1, long());
    column.index = index as i64;
    column
}

fn column(index: usize) -> Expression {
    Expression::Column(col(index))
}

fn int(value: i64) -> Expression {
    Expression::Constant(Constant::new(Datum::Int(value), long()))
}

fn uint(value: u64) -> Expression {
    let mut ty = long();
    ty.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
    Expression::Constant(Constant::new(Datum::UInt(value), ty))
}

/// Go `aggregation.NewWindowFuncDesc` on a prepared plan's path
/// (`skipCheckArgs`), so a constant argument the planner would refuse
/// (`ntile(0)`) still reaches the builder.
fn desc(name: &str, args: Vec<Expression>) -> WindowFuncDesc {
    WindowFuncDesc::new(&NoColumns, name, args, true)
        .unwrap()
        .unwrap_or_else(|| panic!("{name} has no window descriptor"))
}

fn bound(bound_type: BoundType, num: u64) -> FrameBound {
    FrameBound { bound_type, num, ..FrameBound::default() }
}

fn unbounded(bound_type: BoundType) -> FrameBound {
    FrameBound { bound_type, unbounded: true, ..FrameBound::default() }
}

fn current_row() -> FrameBound {
    bound(BoundType::CurrentRow, 0)
}

/// A ROWS frame bound `offset` rows from the current one.
fn offset(offset: i64) -> FrameBound {
    match offset {
        0 => current_row(),
        n if n < 0 => bound(BoundType::Preceding, n.unsigned_abs()),
        n => bound(BoundType::Following, n as u64),
    }
}

fn rows_frame(start: FrameBound, end: FrameBound) -> WindowFrame {
    WindowFrame { frame_type: FrameType::Rows, start: Some(start), end: Some(end) }
}

fn unbounded_rows_frame() -> WindowFrame {
    rows_frame(unbounded(BoundType::Preceding), unbounded(BoundType::Following))
}

/// Go `windows.Build(sctx, v, childExec, forcePipelined)`.
fn window<C: Columns + Send + 'static>(
    meta: ExecutorMeta,
    ctx: C,
    partition_by: Vec<Expression>,
    order_by: Vec<usize>,
    window_func_descs: Vec<WindowFuncDesc>,
    frame: Option<WindowFrame>,
    child: Box<dyn Executor>,
    pipelined: bool,
) -> Box<dyn Executor> {
    tidb_executor_windows::build(
        meta,
        ctx,
        WindowPlan {
            partition_by,
            order_by: order_by.into_iter().map(|index| (col(index), false)).collect(),
            window_func_descs,
            frame,
        },
        child,
        pipelined,
        false,
    )
    .unwrap()
}

struct ChunkedSource {
    inner: MemTableSourceExec,
    calls: usize,
    input: Option<Chunk>,
    chunk_size: usize,
    fail_after: Option<usize>,
}

impl Executor for ChunkedSource {
    fn open(&mut self) -> Result<(), ExecError> {
        self.calls = 0;
        self.input = None;
        self.inner.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if self.fail_after.is_some_and(|limit| self.calls >= limit) {
            return Err(ExecError::internal("later child failure"));
        }
        if self.input.is_none() {
            let mut input = self.inner.new_chunk();
            self.inner.next(&mut input)?;
            self.input = Some(input);
        }
        req.reset();
        let input = self.input.as_ref().unwrap();
        let start = self.calls * self.chunk_size;
        for row in start..(start + self.chunk_size).min(input.num_rows()) {
            req.append_row(input.get_row(row));
        }
        self.calls += 1;
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.inner.close()
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


#[test]
fn completed_output_chunk_precedes_later_child_error() {
    // Go WindowExec.Next returns resultChunks[0] only when every row in it
    // is ready. The second single-row chunk establishes the first partition's
    // end, so the first chunk is returned before fetching the failing third.
    let child = ChunkedSource {
        inner: MemTableSourceExec::new(
            ExecutorMeta::new(schema(2), 2, 1, 1),
            vec![
                vec![Datum::Int(1), Datum::Int(10)],
                vec![Datum::Int(2), Datum::Int(20)],
            ],
        ),
        calls: 0,
        input: None,
        chunk_size: 1,
        fail_after: Some(2),
    };
    let mut executor = window(
        ExecutorMeta::new(schema(3), 1, 1, 1),
        NoColumns,
        vec![column(0)],
        vec![],
        vec![desc("row_number", vec![])],
        Some(unbounded_rows_frame()),
        Box::new(child),
        false,
    );
    executor.open().unwrap();
    let mut output = executor.new_chunk();
    executor.next(&mut output).unwrap();
    assert_eq!(
        output.get_row(0).get_datum_row(executor.ret_field_types()),
        vec![Datum::Int(1), Datum::Int(10), Datum::Int(1)]
    );
    assert!(executor.next(&mut output).is_err());
}

#[test]
fn incomplete_output_chunk_does_not_return_a_finished_partition() {
    let child = ChunkedSource {
        inner: MemTableSourceExec::new(
            ExecutorMeta::new(schema(2), 1, 2, 2),
            vec![
                vec![Datum::Int(1), Datum::Int(10)],
                vec![Datum::Int(2), Datum::Int(20)],
            ],
        ),
        calls: 0,
        input: None,
        chunk_size: 2,
        fail_after: Some(1),
    };
    let mut executor = window(
        ExecutorMeta::new(schema(3), 2, 1, 1),
        NoColumns,
        vec![column(0)],
        vec![],
        vec![desc("row_number", vec![])],
        Some(unbounded_rows_frame()),
        Box::new(child),
        false,
    );
    executor.open().unwrap();
    let mut output = executor.new_chunk();
    assert!(executor.next(&mut output).is_err());
    assert_eq!(output.num_rows(), 0);
    executor.close().unwrap();
}

#[test]
fn window_chunks_preserve_partitions_peers_projection_and_reopen() {
    // The first partition crosses a chunk boundary, the next two share one.
    // Go copyChk follows schema column indexes, including repeated aliases.
    let rows = vec![
        vec![Datum::Null, Datum::Int(10)],
        vec![Datum::Null, Datum::Int(10)],
        vec![Datum::Null, Datum::Int(20)],
        vec![Datum::Int(1), Datum::Int(5)],
        vec![Datum::Int(1), Datum::Int(7)],
        vec![Datum::Int(1), Datum::Int(7)],
        vec![Datum::Int(2), Datum::Int(0)],
    ];
    for (chunk_size, pipelined) in [1, 2, 3, 5, 20]
        .into_iter()
        .flat_map(|n| [(n, false), (n, true)])
    {
        let child = ChunkedSource {
            inner: MemTableSourceExec::new(
                ExecutorMeta::new(schema(2), 1, 1, chunk_size),
                rows.clone(),
            ),
            calls: 0,
            input: None,
            chunk_size,
            fail_after: None,
        };
        let mut output_schema = schema(6);
        for (column, index) in output_schema.columns[..3].iter_mut().zip([1, 0, 1]) {
            column.index = index;
        }
        let mut executor = window(
            ExecutorMeta::new(output_schema, 2, 1, 1),
            NoColumns,
            vec![column(0)],
            vec![1],
            vec![
                desc("row_number", vec![]),
                desc("rank", vec![]),
                desc("lag", vec![column(1), int(1)]),
            ],
            // Go's planner gives a function that needs no frame none.
            None,
            Box::new(child),
            pipelined,
        );
        for _ in 0..2 {
            executor.open().unwrap();
            let mut output = executor.new_chunk();
            let mut actual = Vec::new();
            loop {
                executor.next(&mut output).unwrap();
                if output.num_rows() == 0 {
                    break;
                }
                assert_eq!(output.num_rows(), chunk_size.min(rows.len() - actual.len()));
                for index in 0..output.num_rows() {
                    actual.push(
                        output
                            .get_row(index)
                            .get_datum_row(executor.ret_field_types()),
                    );
                }
                // Resetting a returned chunk must not corrupt a buffered partition.
                output.reset();
            }
            executor.close().unwrap();
            let row_numbers = [1, 2, 3, 1, 2, 3, 1];
            let ranks = [1, 1, 3, 1, 2, 2, 1];
            for (index, row) in rows.iter().enumerate() {
                let lag = if row_numbers[index] == 1 {
                    Datum::Null
                } else {
                    rows[index - 1][1].clone()
                };
                assert_eq!(
                    actual[index],
                    vec![
                        row[1].clone(),
                        row[0].clone(),
                        row[1].clone(),
                        Datum::Int(row_numbers[index]),
                        Datum::Int(ranks[index]),
                        lag,
                    ],
                    "chunk_size={chunk_size}, row={index}"
                );
            }
            assert_eq!(actual.len(), rows.len());
        }
    }
}

#[test]
fn moving_count_matches_rows_frame_results() {
    let rows = (0..100)
        .map(|value| vec![Datum::Int(1), Datum::Int(value)])
        .collect();
    let child = MemTableSourceExec::new(ExecutorMeta::new(schema(2), 7, 7, 7), rows);
    let mut executor = window(
        ExecutorMeta::new(schema(3), 8, 8, 8),
        NoColumns,
        vec![column(0)],
        vec![1],
        vec![desc("count", vec![column(1)])],
        Some(rows_frame(offset(-9), current_row())),
        Box::new(child),
        false,
    );
    executor.open().unwrap();
    let types = executor.ret_field_types().to_vec();
    let mut counts = Vec::new();
    loop {
        let mut output = executor.new_chunk();
        executor.next(&mut output).unwrap();
        if output.num_rows() == 0 {
            break;
        }
        counts
            .extend((0..output.num_rows()).map(|row| output.get_row(row).get_datum(2, &types[2])));
    }
    assert_eq!(counts.len(), 100);
    assert_eq!(
        counts[..12],
        [
            Datum::Int(1),
            Datum::Int(2),
            Datum::Int(3),
            Datum::Int(4),
            Datum::Int(5),
            Datum::Int(6),
            Datum::Int(7),
            Datum::Int(8),
            Datum::Int(9),
            Datum::Int(10),
            Datum::Int(10),
            Datum::Int(10),
        ]
    );
    assert!(counts[12..].iter().all(|count| *count == Datum::Int(10)));
}

/// A parameter marker whose every evaluation is counted.
fn counted_parameter(order: i64) -> Expression {
    let mut parameter = Constant::new(Datum::Int(1), long());
    parameter.param_marker = Some(tidb_expr::constant::ParamMarker { order });
    Expression::Constant(parameter)
}

struct CountedParameters(std::sync::Arc<std::sync::Mutex<Vec<usize>>>);

impl Columns for CountedParameters {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn param_value(&self, order: usize) -> Result<Datum, tidb_exprctx::EvalError> {
        self.0.lock().unwrap().push(order);
        Ok(Datum::Int(1))
    }
}

#[test]
fn moving_count_evaluates_only_entering_and_leaving_rows() {
    let evaluations = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let child = MemTableSourceExec::new(
        ExecutorMeta::new(schema(1), 1, 100, 100),
        (0..100).map(|i| vec![Datum::Int(i)]).collect(),
    );
    let mut executor = window(
        ExecutorMeta::new(schema(2), 2, 100, 100),
        CountedParameters(evaluations.clone()),
        vec![],
        vec![],
        vec![desc("count", vec![counted_parameter(0)])],
        Some(rows_frame(offset(-9), current_row())),
        Box::new(child),
        false,
    );
    executor.open().unwrap();
    let mut output = executor.new_chunk();
    executor.next(&mut output).unwrap();
    assert_eq!(output.num_rows(), 100);
    for row in 0..100 {
        assert_eq!(
            output.get_row(row).get_datum(1, &long()),
            Datum::Int((row + 1).min(10) as i64)
        );
    }
    // Go countOriginal*.Slide removes expired rows before adding new rows.
    // 100 rows enter, 90 leave; rescanning every frame evaluates 955 inputs.
    assert_eq!(evaluations.lock().unwrap().len(), 190);
    executor.close().unwrap();
}

#[test]
fn moving_aggregates_nulls_empty_frames_chunks_and_partitions() {
    let rows: Vec<_> = (0..18)
        .map(|i| {
            vec![
                Datum::Int(i / 9),
                if i % 3 == 0 {
                    Datum::Null
                } else {
                    Datum::Int(i)
                },
            ]
        })
        .collect();
    // Inclusive offsets relative to the current row. Include empty frames
    // at both partition edges and nonoverlapping single-row frames.
    for (lower, upper) in [(-3_i64, -1_i64), (-2, 2), (0, 0), (2, 2), (1, 3)] {
        for (chunk_size, pipelined) in [1, 4, 7, 32]
            .into_iter()
            .flat_map(|n| [(n, false), (n, true)])
        {
            let child = ChunkedSource {
                inner: MemTableSourceExec::new(
                    ExecutorMeta::new(schema(2), 1, chunk_size, chunk_size),
                    rows.clone(),
                ),
                calls: 0,
                input: None,
                chunk_size,
                fail_after: None,
            };
            let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
            decimal.set_decimal(4);
            let mut output_schema = schema(9);
            for column in &mut output_schema.columns[4..6] {
                column.ret_type = Some(decimal.clone());
            }
            let mut unsigned = long();
            unsigned.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
            output_schema.columns[8].ret_type = Some(unsigned.clone());
            let descs = vec![
                desc("count", vec![column(1)]),
                desc("count", vec![int(1)]),
                desc("sum", vec![column(1)]),
                desc("avg", vec![column(1)]),
                desc("min", vec![column(1)]),
                desc("max", vec![column(1)]),
                desc("bit_xor", vec![column(1)]),
            ];
            let mut executor = window(
                ExecutorMeta::new(output_schema, 2, chunk_size, chunk_size),
                NoColumns,
                vec![column(0)],
                vec![],
                descs,
                Some(rows_frame(offset(lower), offset(upper))),
                Box::new(child),
                pipelined,
            );
            for _ in 0..2 {
                executor.open().unwrap();
                let mut output = executor.new_chunk();
                let mut index = 0;
                loop {
                    executor.next(&mut output).unwrap();
                    if output.num_rows() == 0 {
                        break;
                    }
                    for i in 0..output.num_rows() {
                        let current = (index % 9) as i64;
                        let selected: Vec<_> = (0..9)
                            .filter(|&candidate| {
                                candidate >= current + lower && candidate <= current + upper
                            })
                            .collect();
                        let count = selected
                            .iter()
                            .filter(|&&candidate| candidate % 3 != 0)
                            .count();
                        assert_eq!(
                            output.get_row(i).get_datum(2, &long()),
                            Datum::Int(count as i64),
                            "row={index}, frame=({lower},{upper}), chunk={chunk_size}"
                        );
                        assert_eq!(
                            output.get_row(i).get_datum(3, &long()),
                            Datum::Int(selected.len() as i64)
                        );
                        let sum: i64 = selected
                            .iter()
                            .filter(|&&candidate| candidate % 3 != 0)
                            .map(|&candidate| candidate + (index / 9 * 9) as i64)
                            .sum();
                        for (position, average) in [(4, false), (5, true)] {
                            let expected = if count == 0 {
                                Datum::Null
                            } else {
                                Datum::Decimal(if average {
                                    tidb_datatype::Decimal::from_scaled_i128(
                                        (i128::from(sum) * 10_000 + count as i128 / 2)
                                            / count as i128,
                                        4,
                                    )
                                } else {
                                    tidb_datatype::Decimal::from_int(sum)
                                })
                            };
                            assert_eq!(
                                output.get_row(i).get_datum(position, &decimal),
                                expected,
                                "row={index}, frame=({lower},{upper}), chunk={chunk_size}, average={average}"
                            );
                        }
                        let values: Vec<_> = selected
                            .iter()
                            .filter(|&&candidate| candidate % 3 != 0)
                            .map(|&candidate| candidate + (index / 9 * 9) as i64)
                            .collect();
                        for (position, value) in
                            [(6, values.iter().min()), (7, values.iter().max())]
                        {
                            assert_eq!(
                                output.get_row(i).get_datum(position, &long()),
                                value.map_or(Datum::Null, |v| Datum::Int(*v))
                            );
                        }
                        assert_eq!(
                            output.get_row(i).get_datum(8, &unsigned),
                            Datum::UInt(values.iter().fold(0, |acc, &value| acc ^ value as u64))
                        );
                        index += 1;
                    }
                }
                assert_eq!(index, rows.len());
                executor.close().unwrap();
            }
        }
    }
}

#[test]
fn pipelined_ready_chunk_precedes_later_same_partition_error() {
    for (ordered, unbounded_start) in [(false, false), (true, false), (false, true), (true, true)] {
        let child = ChunkedSource {
            inner: MemTableSourceExec::new(
                ExecutorMeta::new(schema(1), 1, 1, 1),
                (1..=4).map(|i| vec![Datum::Int(i)]).collect(),
            ),
            calls: 0,
            input: None,
            chunk_size: 1,
            fail_after: Some(3),
        };
        let meta = ExecutorMeta::new(schema(2), 2, 1, 1);
        let ctx = tidb_sessionctx::StmtContext::for_query().with_pipelined_window_exec(false);
        let start = if unbounded_start { unbounded(BoundType::Preceding) } else { current_row() };
        let plan = WindowPlan {
            partition_by: vec![],
            order_by: vec![],
            window_func_descs: vec![desc("row_number", vec![])],
            frame: Some(rows_frame(start, current_row())),
        };
        let mut executor: Box<dyn Executor> = if ordered {
            Box::new(tidb_executor_windows::build_ordered(meta, ctx, plan, Box::new(child)).unwrap())
        } else {
            tidb_executor_windows::build(meta, ctx, plan, Box::new(child), true, false).unwrap()
        };
        executor.open().unwrap();
        let mut output = executor.new_chunk();
        // Go requires lookahead AND release of the frame's last reference to the
        // first input chunk, but does not need the rest of this partition.
        if unbounded_start {
            // Go cannot release any input referenced by an unbounded start,
            // even when the first output chunk's values are already computed.
            assert!(executor.next(&mut output).is_err());
            assert_eq!(output.num_rows(), 0);
            executor.close().unwrap();
            continue;
        }
        executor.next(&mut output).unwrap();
        assert_eq!(
            output.get_row(0).get_datum_row(executor.ret_field_types()),
            [Datum::Int(1), Datum::Int(1)]
        );
        output.reset();
        assert!(executor.next(&mut output).is_err());
        assert_eq!(output.num_rows(), 0);
        executor.close().unwrap();
    }
}

/// Go windows.TestBuildOrderedWindowExec: the explicit ordered constructor
/// processes partitioned row numbers even when ordinary pipelining is disabled.
#[test]
fn upstream_ordered_window_partitioned_row_number() {
    let child = MemTableSourceExec::new(
        ExecutorMeta::new(schema(2), 1, 4, 4),
        [[1, 1], [1, 2], [2, 1], [2, 2]]
            .into_iter()
            .map(|row| row.into_iter().map(Datum::Int).collect())
            .collect(),
    );
    let mut executor = tidb_executor_windows::build_ordered(
        ExecutorMeta::new(schema(3), 2, 4, 4),
        tidb_sessionctx::StmtContext::for_query().with_pipelined_window_exec(false),
        WindowPlan {
            partition_by: vec![column(0)],
            order_by: vec![(col(1), false)],
            window_func_descs: vec![desc("row_number", vec![])],
            frame: Some(rows_frame(current_row(), current_row())),
        },
        Box::new(child),
    )
    .unwrap();
    executor.open().unwrap();
    let mut output = executor.new_chunk();
    let mut rows = Vec::new();
    loop {
        executor.next(&mut output).unwrap();
        if output.num_rows() == 0 {
            break;
        }
        for i in 0..output.num_rows() {
            rows.push(output.get_row(i).get_datum_row(executor.ret_field_types()));
        }
    }
    executor.close().unwrap();
    let expected: Vec<Vec<_>> = [[1, 1, 1], [1, 2, 2], [2, 1, 1], [2, 2, 2]]
        .into_iter()
        .map(|row| row.into_iter().map(Datum::Int).collect())
        .collect();
    assert_eq!(rows, expected);
}

/// Go aggWindowProcessor consumes the partition once, then repeats its partial
/// results across output chunks; an explicit ROWS processor evaluates per row.
#[test]
fn partition_aggregates_are_evaluated_once_across_chunks() {
    for explicit_frame in [false, true] {
        let evaluations = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let child = ChunkedSource {
            inner: MemTableSourceExec::new(
                ExecutorMeta::new(schema(1), 1, 12, 12),
                (0..12).map(|i| vec![Datum::Int(i / 4)]).collect(),
            ),
            calls: 0,
            input: None,
            chunk_size: 3,
            fail_after: None,
        };
        let mut executor = window(
            ExecutorMeta::new(schema(3), 2, 3, 3),
            CountedParameters(evaluations.clone()),
            vec![column(0)],
            vec![],
            vec![
                desc("bit_or", vec![counted_parameter(0)]),
                desc("first_value", vec![counted_parameter(0)]),
            ],
            explicit_frame.then(unbounded_rows_frame),
            Box::new(child),
            false,
        );
        for run in 1..=2 {
            executor.open().unwrap();
            let mut output = executor.new_chunk();
            let mut count = 0;
            loop {
                executor.next(&mut output).unwrap();
                if output.num_rows() == 0 {
                    break;
                }
                assert_eq!(output.num_rows(), 3);
                for i in 0..output.num_rows() {
                    assert_eq!(
                        output.get_row(i).get_datum(0, &long()),
                        Datum::Int(count / 4)
                    );
                    assert_eq!(output.get_row(i).get_datum(1, &long()), Datum::Int(1));
                    assert_eq!(output.get_row(i).get_datum(2, &long()), Datum::Int(1));
                    count += 1;
                }
            }
            assert_eq!(count, 12);
            assert_eq!(
                evaluations.lock().unwrap().len(),
                run * if explicit_frame { 60 } else { 15 },
                "explicit_frame={explicit_frame}"
            );
            executor.close().unwrap();
        }
    }
}

/// Go aggWindowProcessor updates every function before appending any result;
/// pipelined produce instead updates/appends one function at a time.
#[test]
fn partition_update_precedes_lead_lag_result_evaluation() {
    for pipelined in [false, true] {
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let child = MemTableSourceExec::new(
            ExecutorMeta::new(schema(1), 1, 2, 2),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]],
        );
        let mut executor = window(
            ExecutorMeta::new(schema(4), 2, 2, 2),
            CountedParameters(calls.clone()),
            vec![],
            vec![],
            vec![
                desc("lead", vec![counted_parameter(0), int(0)]),
                desc("first_value", vec![counted_parameter(1)]),
                desc("bit_or", vec![counted_parameter(2)]),
            ],
            None,
            Box::new(child),
            pipelined,
        );
        executor.open().unwrap();
        let mut output = executor.new_chunk();
        executor.next(&mut output).unwrap();
        assert_eq!(output.num_rows(), 2);
        assert_eq!(
            *calls.lock().unwrap(),
            if pipelined {
                vec![0, 1, 2, 2, 0]
            } else {
                vec![1, 2, 2, 0, 0]
            },
            "pipelined={pipelined}"
        );
        executor.close().unwrap();
    }
}

#[test]
fn partition_update_error_precedes_aggregate_finalization_error() {
    use tidb_datatype::Decimal;
    use tidb_exprctx::EvalError;
    struct FailingParameter;
    impl Columns for FailingParameter {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn param_value(&self, _: usize) -> Result<Datum, EvalError> {
            Err(EvalError::UnknownColumn("later window update".into()))
        }
    }
    let mut ty = FieldType::new(FieldTypeCode::NewDecimal);
    ty.set_decimal(0);
    ty.set_flen(65);
    let mut input = Column::new(1, ty.clone());
    input.index = 0;
    for pipelined in [false, true] {
        // The retained SUM fits nine decimal words; AVG division reports
        // truncation only when AppendFinalResult2Chunk is reached.
        let child = MemTableSourceExec::new(
            ExecutorMeta::new(Schema::new(vec![input.clone()]), 1, 1, 1),
            vec![vec![Datum::Decimal(Decimal::from_literal(&format!(
                "5{}",
                "0".repeat(80)
            )))]],
        );
        let mut output_schema = schema(3);
        output_schema.columns[0].ret_type = Some(ty.clone());
        let avg = desc("avg", vec![Expression::Column(input.clone())]);
        output_schema.columns[1].ret_type = Some(avg.base.ret_type.clone());
        let mut executor = window(
            ExecutorMeta::new(output_schema, 2, 1, 1),
            FailingParameter,
            vec![],
            vec![],
            vec![avg, desc("first_value", vec![counted_parameter(0)])],
            None,
            Box::new(child),
            pipelined,
        );
        executor.open().unwrap();
        let mut output = executor.new_chunk();
        let error = executor.next(&mut output).unwrap_err();
        if pipelined {
            assert!(
                matches!(error, ExecError::Eval(EvalError::Conversion(_))),
                "{error:?}"
            );
        } else {
            assert!(
                matches!(error, ExecError::Eval(EvalError::UnknownColumn(ref name)) if name == "later window update"),
                "{error:?}"
            );
        }
        assert_eq!(output.num_rows(), 0);
        executor.close().unwrap();
    }
}

#[test]
fn go_window_leaf_vectors_use_the_live_chunk_executor() {
    // Go aggfuncs TestWindowFunctions/TestLeadLag and func_value.go, through
    // the executor that drives them.
    fn check(input: &[Option<i64>], function: impl Fn() -> WindowFuncDesc, ordered: bool, expected: &[Datum]) {
        for chunk_size in [1, 2] {
            for pipelined in [false, true] {
                let function = function();
                let child = ChunkedSource {
                    inner: MemTableSourceExec::new(
                        ExecutorMeta::new(schema(1), 1, chunk_size, chunk_size),
                        input
                            .iter()
                            .map(|v| vec![v.map_or(Datum::Null, Datum::Int)])
                            .collect(),
                    ),
                    calls: 0,
                    input: None,
                    chunk_size,
                    fail_after: None,
                };
                let mut output_schema = schema(2);
                output_schema.columns[1].ret_type = Some(function.base.ret_type.clone());
                let mut executor = window(
                    ExecutorMeta::new(output_schema, 2, chunk_size, chunk_size),
                    NoColumns,
                    vec![],
                    if ordered { vec![0] } else { vec![] },
                    vec![function],
                    None,
                    Box::new(child),
                    pipelined,
                );
                for _ in 0..2 {
                    executor.open().unwrap();
                    let mut output = executor.new_chunk();
                    let mut actual = Vec::new();
                    loop {
                        executor.next(&mut output).unwrap();
                        if output.num_rows() == 0 {
                            break;
                        }
                        for i in 0..output.num_rows() {
                            actual.push(
                                output
                                    .get_row(i)
                                    .get_datum(1, &executor.ret_field_types()[1]),
                            );
                        }
                    }
                    assert_eq!(
                        actual, expected,
                        "chunk={chunk_size}, pipelined={pipelined}, input={input:?}"
                    );
                    executor.close().unwrap();
                }
            }
        }
    }

    for (keys, expected) in [
        (vec![], vec![]),
        (vec![1], vec![1.0]),
        (vec![1, 1], vec![1.0, 1.0]),
        (vec![1, 2, 3, 4], vec![0.25, 0.5, 0.75, 1.0]),
        (vec![1, 1, 2, 3], vec![0.5, 0.5, 0.75, 1.0]),
        (vec![4, 4, 7], vec![2.0 / 3.0, 2.0 / 3.0, 1.0]),
    ] {
        check(
            &keys.into_iter().map(Some).collect::<Vec<_>>(),
            || desc("cume_dist", vec![]),
            true,
            &expected.into_iter().map(Datum::Real).collect::<Vec<_>>(),
        );
    }
    for (buckets, expected) in [
        (0, vec![None, None, None]),
        (3, vec![Some(1), Some(1), Some(2), Some(3)]),
        (5, vec![Some(1), Some(2), Some(3)]),
        (
            3,
            vec![
                Some(1),
                Some(1),
                Some(1),
                Some(1),
                Some(2),
                Some(2),
                Some(2),
                Some(2),
                Some(3),
                Some(3),
                Some(3),
            ],
        ),
    ] {
        check(
            &vec![Some(1); expected.len()],
            || desc("ntile", vec![int(buckets)]),
            false,
            &expected
                .into_iter()
                .map(|v| v.map_or(Datum::Null, Datum::UInt))
                .collect::<Vec<_>>(),
        );
    }

    // The explicit index vectors include Go's uint64 LEAD addition wrap.
    for (lead, offset, targets) in [
        (false, 0, [Some(0), Some(1), Some(2)]),
        (false, 1, [None, Some(0), Some(1)]),
        (false, 2, [None, None, Some(0)]),
        (false, 3, [None, None, None]),
        (false, 1_000_000, [None, None, None]),
        (true, 0, [Some(0), Some(1), Some(2)]),
        (true, 1, [Some(1), Some(2), None]),
        (true, 2, [Some(2), None, None]),
        (true, 3, [None, None, None]),
        (true, 1_000_000, [None, None, None]),
        (true, u64::MAX, [None, Some(0), Some(1)]),
        (true, u64::MAX - 1, [None, None, Some(0)]),
    ] {
        let input = [7, 3, 11];
        for current_row_default in [false, true] {
            let expected: Vec<_> = targets
                .iter()
                .enumerate()
                .map(|(i, target)| {
                    target.map(|n| Datum::Int(input[n])).unwrap_or_else(|| {
                        if current_row_default {
                            Datum::Int(input[i])
                        } else {
                            Datum::Null
                        }
                    })
                })
                .collect();
            check(
                &input.map(Some),
                || {
                    let mut args = vec![column(0), uint(offset)];
                    if current_row_default {
                        args.push(column(0));
                    }
                    desc(if lead { "lead" } else { "lag" }, args)
                },
                false,
                &expected,
            );
        }
    }
    for (input, nth, last, expected) in [
        (vec![], Some(1), false, None),
        (vec![None, Some(2), Some(9)], Some(1), false, None),
        (vec![Some(7)], Some(1), false, Some(7)),
        (vec![Some(1), Some(2)], None, true, Some(2)),
        (vec![Some(1), Some(2), None], None, true, None),
        (vec![Some(10), Some(20), Some(30)], Some(2), false, Some(20)),
        (vec![Some(10), None], Some(2), false, None),
        (vec![Some(1), Some(2), Some(3)], Some(5), false, None),
        (vec![Some(1), Some(2)], Some(0), false, None),
    ] {
        check(
            &input,
            || match (nth, last) {
                (_, true) => desc("last_value", vec![column(0)]),
                (Some(1), false) => desc("first_value", vec![column(0)]),
                (Some(n), false) => desc("nth_value", vec![column(0), int(n)]),
                (None, false) => unreachable!("a value function names its row"),
            },
            false,
            &vec![expected.map_or(Datum::Null, Datum::Int); input.len()],
        );
    }
}
