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

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_executor::executor::{ExecError, Executor, ExecutorMeta};
use tidb_executor::hash_agg::{AggFunc, AggKind, BitOp};
use tidb_executor::mem_table::MemTableSourceExec;
use tidb_executor::window::{
    WindowBound, WindowExec, WindowFrameSpec, WindowFuncSpec, WindowFunction,
};
use tidb_expr::NoColumns;
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

fn long() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn schema(width: usize) -> Schema {
    Schema::new(
        (0..width)
            .map(|index| {
                let mut column = Column::new(index as i64 + 1, long());
                column.index = index as i64;
                column
            })
            .collect(),
    )
}

fn column(index: usize) -> Expression {
    let mut column = Column::new(index as i64 + 1, long());
    column.index = index as i64;
    Expression::Column(column)
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
    let mut executor = WindowExec::new(
        ExecutorMeta::new(schema(3), 1, 1, 1),
        vec![WindowFuncSpec {
            func: WindowFunction::RowNumber,
            output_type: long(),
        }],
        vec![column(0)],
        vec![],
        WindowFrameSpec {
            start: WindowBound::Unbounded,
            end: WindowBound::Unbounded,
            range: None,
            range_desc: false,
        },
        Box::new(child),
        NoColumns,
        2,
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
    let mut executor = WindowExec::new(
        ExecutorMeta::new(schema(3), 2, 1, 1),
        vec![WindowFuncSpec {
            func: WindowFunction::RowNumber,
            output_type: long(),
        }],
        vec![column(0)],
        vec![],
        WindowFrameSpec {
            start: WindowBound::Unbounded,
            end: WindowBound::Unbounded,
            range: None,
            range_desc: false,
        },
        Box::new(child),
        NoColumns,
        2,
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
        let executor = WindowExec::new(
            ExecutorMeta::new(output_schema, 2, 1, 1),
            vec![
                WindowFuncSpec {
                    func: WindowFunction::RowNumber,
                    output_type: long(),
                },
                WindowFuncSpec {
                    func: WindowFunction::Rank { dense: false },
                    output_type: long(),
                },
                WindowFuncSpec {
                    func: WindowFunction::Relative {
                        arg: column(1),
                        offset: 1,
                        default: None,
                        lead: false,
                    },
                    output_type: long(),
                },
            ],
            vec![column(0)],
            vec![column(1)],
            WindowFrameSpec {
                start: WindowBound::Unbounded,
                end: WindowBound::Unbounded,
                range: None,
                range_desc: false,
            },
            Box::new(child),
            NoColumns,
            3,
        );
        let mut executor: Box<dyn Executor> = if pipelined {
            Box::new(tidb_executor::window::PipelinedWindowExec::new(executor))
        } else {
            Box::new(executor)
        };
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
    let frame = WindowFrameSpec {
        start: WindowBound::Offset {
            num: 9,
            preceding: true,
        },
        end: WindowBound::CurrentRow,
        range: None,
        range_desc: false,
    };
    let mut executor = WindowExec::new(
        ExecutorMeta::new(schema(3), 8, 8, 8),
        vec![WindowFuncSpec {
            func: WindowFunction::Aggregate(AggFunc::new(AggKind::Count, Some(column(1)))),
            output_type: long(),
        }],
        vec![column(0)],
        vec![column(1)],
        frame,
        Box::new(child),
        NoColumns,
        2,
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

#[test]
fn moving_count_evaluates_only_entering_and_leaving_rows() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tidb_expr::constant::{Constant, ParamMarker};
    use tidb_expr::{Columns, EvalError};

    struct Parameters(Arc<AtomicUsize>);
    impl Columns for Parameters {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn param_value(&self, _: usize) -> Result<Datum, EvalError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(Datum::Int(1))
        }
    }
    let evaluations = Arc::new(AtomicUsize::new(0));
    let mut parameter = Constant::new(Datum::Int(1), long());
    parameter.param_marker = Some(ParamMarker { order: 0 });
    let child = MemTableSourceExec::new(
        ExecutorMeta::new(schema(1), 1, 100, 100),
        (0..100).map(|i| vec![Datum::Int(i)]).collect(),
    );
    let mut executor = WindowExec::new(
        ExecutorMeta::new(schema(2), 2, 100, 100),
        vec![WindowFuncSpec {
            func: WindowFunction::Aggregate(AggFunc::new(
                AggKind::Count,
                Some(Expression::Constant(parameter)),
            )),
            output_type: long(),
        }],
        vec![],
        vec![],
        WindowFrameSpec {
            start: WindowBound::Offset {
                num: 9,
                preceding: true,
            },
            end: WindowBound::CurrentRow,
            range: None,
            range_desc: false,
        },
        Box::new(child),
        Parameters(evaluations.clone()),
        1,
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
    assert_eq!(evaluations.load(Ordering::Relaxed), 190);
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
            let bound = |offset: i64| {
                if offset == 0 {
                    WindowBound::CurrentRow
                } else {
                    WindowBound::Offset {
                        num: offset.unsigned_abs(),
                        preceding: offset < 0,
                    }
                }
            };
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
            let mut funcs: Vec<_> = vec![Some(column(1)), None]
                .into_iter()
                .map(|arg| WindowFuncSpec {
                    func: WindowFunction::Aggregate(AggFunc::new(AggKind::Count, arg)),
                    output_type: long(),
                })
                .collect();
            for kind in [AggKind::Sum, AggKind::Avg] {
                funcs.push(WindowFuncSpec {
                    func: WindowFunction::Aggregate(AggFunc::new(kind, Some(column(1)))),
                    output_type: decimal.clone(),
                });
            }
            for kind in [AggKind::Min, AggKind::Max, AggKind::Bit(BitOp::Xor)] {
                let output_type = if matches!(kind, AggKind::Bit(_)) {
                    unsigned.clone()
                } else {
                    long()
                };
                funcs.push(WindowFuncSpec {
                    func: WindowFunction::Aggregate(AggFunc::new(kind, Some(column(1)))),
                    output_type,
                });
            }
            let executor = WindowExec::new(
                ExecutorMeta::new(output_schema, 2, chunk_size, chunk_size),
                funcs,
                vec![column(0)],
                vec![],
                WindowFrameSpec {
                    start: bound(lower),
                    end: bound(upper),
                    range: None,
                    range_desc: false,
                },
                Box::new(child),
                NoColumns,
                2,
            );
            let mut executor: Box<dyn Executor> = if pipelined {
                Box::new(tidb_executor::window::PipelinedWindowExec::new(executor))
            } else {
                Box::new(executor)
            };
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
    for (ordered, unbounded) in [(false, false), (true, false), (false, true), (true, true)] {
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
        let base = WindowExec::new(
            ExecutorMeta::new(schema(2), 2, 1, 1),
            vec![WindowFuncSpec {
                func: WindowFunction::RowNumber,
                output_type: long(),
            }],
            vec![],
            vec![],
            WindowFrameSpec {
                start: if unbounded {
                    WindowBound::Unbounded
                } else {
                    WindowBound::CurrentRow
                },
                end: WindowBound::CurrentRow,
                range: None,
                range_desc: false,
            },
            Box::new(child),
            tidb_executor::StmtContext::for_query().with_pipelined_window_exec(false),
            1,
        );
        let mut executor: Box<dyn Executor> = if ordered {
            Box::new(tidb_executor::window::OrderedWindowExec::new(base))
        } else {
            Box::new(tidb_executor::window::PipelinedWindowExec::new(base))
        };
        executor.open().unwrap();
        let mut output = executor.new_chunk();
        // Go requires lookahead AND release of the frame's last reference to the
        // first input chunk, but does not need the rest of this partition.
        if unbounded {
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
    let base = WindowExec::new(
        ExecutorMeta::new(schema(3), 2, 4, 4),
        vec![WindowFuncSpec {
            func: WindowFunction::RowNumber,
            output_type: long(),
        }],
        vec![column(0)],
        vec![column(1)],
        WindowFrameSpec {
            start: WindowBound::CurrentRow,
            end: WindowBound::CurrentRow,
            range: None,
            range_desc: false,
        },
        Box::new(child),
        tidb_executor::StmtContext::for_query().with_pipelined_window_exec(false),
        2,
    );
    let mut executor = tidb_executor::window::OrderedWindowExec::new(base);
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
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tidb_expr::constant::{Constant, ParamMarker};
    use tidb_expr::{Columns, EvalError};
    struct Parameters(Arc<AtomicUsize>);
    impl Columns for Parameters {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn param_value(&self, _: usize) -> Result<Datum, EvalError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(Datum::Int(1))
        }
    }
    for explicit_frame in [false, true] {
        let evaluations = Arc::new(AtomicUsize::new(0));
        let mut parameter = Constant::new(Datum::Int(1), long());
        parameter.param_marker = Some(ParamMarker { order: 0 });
        let arg = Expression::Constant(parameter);
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
        let mut executor = WindowExec::new(
            ExecutorMeta::new(schema(3), 2, 3, 3),
            vec![
                WindowFuncSpec {
                    func: WindowFunction::Aggregate(AggFunc::new(
                        AggKind::Bit(BitOp::Or),
                        Some(arg.clone()),
                    )),
                    output_type: long(),
                },
                WindowFuncSpec {
                    func: WindowFunction::Value {
                        arg,
                        nth: Some(1),
                        last: false,
                    },
                    output_type: long(),
                },
            ],
            vec![column(0)],
            vec![],
            explicit_frame.then_some(WindowFrameSpec {
                start: WindowBound::Unbounded,
                end: WindowBound::Unbounded,
                range: None,
                range_desc: false,
            }),
            Box::new(child),
            Parameters(evaluations.clone()),
            1,
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
                evaluations.load(Ordering::Relaxed),
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
    use std::sync::{Arc, Mutex};
    use tidb_expr::constant::{Constant, ParamMarker};
    use tidb_expr::{Columns, EvalError};
    struct Parameters(Arc<Mutex<Vec<usize>>>);
    impl Columns for Parameters {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
            self.0.lock().unwrap().push(order);
            Ok(Datum::Int(1))
        }
    }
    fn parameter(order: usize) -> Expression {
        let mut value = Constant::new(Datum::Int(1), long());
        value.param_marker = Some(ParamMarker {
            order: order as i64,
        });
        Expression::Constant(value)
    }
    for pipelined in [false, true] {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let child = MemTableSourceExec::new(
            ExecutorMeta::new(schema(1), 1, 2, 2),
            vec![vec![Datum::Int(1)], vec![Datum::Int(2)]],
        );
        let base = WindowExec::new(
            ExecutorMeta::new(schema(4), 2, 2, 2),
            vec![
                WindowFuncSpec {
                    func: WindowFunction::Relative {
                        arg: parameter(0),
                        offset: 0,
                        default: None,
                        lead: true,
                    },
                    output_type: long(),
                },
                WindowFuncSpec {
                    func: WindowFunction::Value {
                        arg: parameter(1),
                        nth: Some(1),
                        last: false,
                    },
                    output_type: long(),
                },
                WindowFuncSpec {
                    func: WindowFunction::Aggregate(AggFunc::new(
                        AggKind::Bit(BitOp::Or),
                        Some(parameter(2)),
                    )),
                    output_type: long(),
                },
            ],
            vec![],
            vec![],
            None,
            Box::new(child),
            Parameters(calls.clone()),
            1,
        );
        let mut executor: Box<dyn Executor> = if pipelined {
            Box::new(tidb_executor::window::PipelinedWindowExec::new(base))
        } else {
            Box::new(base)
        };
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
    use tidb_expr::constant::{Constant, ParamMarker};
    use tidb_expr::{Columns, EvalError};
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
    let mut parameter = Constant::new(Datum::Int(1), long());
    parameter.param_marker = Some(ParamMarker { order: 0 });
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
        output_schema.columns[1].ret_type = Some(ty.clone());
        let base = WindowExec::new(
            ExecutorMeta::new(output_schema, 2, 1, 1),
            vec![
                WindowFuncSpec {
                    func: WindowFunction::Aggregate(AggFunc::new(
                        AggKind::Avg,
                        Some(Expression::Column(input.clone())),
                    )),
                    output_type: ty.clone(),
                },
                WindowFuncSpec {
                    func: WindowFunction::Value {
                        arg: Expression::Constant(parameter.clone()),
                        nth: Some(1),
                        last: false,
                    },
                    output_type: long(),
                },
            ],
            vec![],
            vec![],
            None,
            Box::new(child),
            FailingParameter,
            1,
        );
        let mut executor: Box<dyn Executor> = if pipelined {
            Box::new(tidb_executor::window::PipelinedWindowExec::new(base))
        } else {
            Box::new(base)
        };
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

/// Go passes ORDER BY columns to ranking comparers, but COUNT/ROW_NUMBER
/// do not evaluate them. Ordinary windows must not build an unused peer table.
#[test]
fn nonranking_windows_do_not_compare_order_keys() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tidb_expr::constant::{Constant, ParamMarker};
    use tidb_expr::{Columns, EvalError};
    struct Parameters(Arc<AtomicUsize>);
    impl Columns for Parameters {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn param_value(&self, _: usize) -> Result<Datum, EvalError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(Datum::Int(1))
        }
    }
    for pipelined in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut key = Constant::new(Datum::Int(1), long());
        key.param_marker = Some(ParamMarker { order: 0 });
        let child = MemTableSourceExec::new(
            ExecutorMeta::new(schema(1), 1, 100, 100),
            (0..100).map(|i| vec![Datum::Int(i / 50)]).collect(),
        );
        let base = WindowExec::new(
            ExecutorMeta::new(schema(3), 2, 100, 100),
            vec![
                WindowFuncSpec {
                    func: WindowFunction::Aggregate(AggFunc::new(AggKind::Count, None)),
                    output_type: long(),
                },
                WindowFuncSpec {
                    func: WindowFunction::RowNumber,
                    output_type: long(),
                },
            ],
            vec![column(0)],
            vec![Expression::Constant(key)],
            None,
            Box::new(child),
            Parameters(calls.clone()),
            1,
        );
        let mut executor: Box<dyn Executor> = if pipelined {
            Box::new(tidb_executor::window::PipelinedWindowExec::new(base))
        } else {
            Box::new(base)
        };
        executor.open().unwrap();
        let mut output = executor.new_chunk();
        executor.next(&mut output).unwrap();
        assert_eq!(output.num_rows(), 100);
        for i in 0..100 {
            assert_eq!(output.get_row(i).get_datum(1, &long()), Datum::Int(50));
            assert_eq!(
                output.get_row(i).get_datum(2, &long()),
                Datum::Int((i % 50 + 1) as i64)
            );
        }
        assert_eq!(calls.load(Ordering::Relaxed), 0, "pipelined={pipelined}");
        executor.close().unwrap();
    }
}

#[test]
fn ranking_comparisons_follow_result_evaluation_order() {
    use std::sync::{Arc, Mutex};
    use tidb_expr::constant::{Constant, ParamMarker};
    use tidb_expr::{Columns, EvalError};
    struct Parameters(Arc<Mutex<Vec<usize>>>);
    impl Columns for Parameters {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
            self.0.lock().unwrap().push(order);
            Ok(Datum::Int(1))
        }
    }
    let parameter = |order| {
        let mut value = Constant::new(Datum::Int(1), long());
        value.param_marker = Some(ParamMarker { order });
        Expression::Constant(value)
    };
    for pipelined in [false, true] {
        for function in [
            WindowFunction::Rank { dense: false },
            WindowFunction::Rank { dense: true },
            WindowFunction::PercentRank,
            WindowFunction::CumeDist,
        ] {
            let cume = matches!(function, WindowFunction::CumeDist);
            let floating = matches!(
                function,
                WindowFunction::PercentRank | WindowFunction::CumeDist
            );
            let calls = Arc::new(Mutex::new(Vec::new()));
            let child = MemTableSourceExec::new(
                ExecutorMeta::new(schema(1), 1, 2, 2),
                vec![vec![Datum::Int(1)], vec![Datum::Int(1)]],
            );
            let base = WindowExec::new(
                ExecutorMeta::new(schema(3), 2, 2, 2),
                vec![
                    WindowFuncSpec {
                        func: function,
                        output_type: if floating {
                            FieldType::new(FieldTypeCode::Double)
                        } else {
                            long()
                        },
                    },
                    WindowFuncSpec {
                        func: WindowFunction::Relative {
                            arg: parameter(1),
                            offset: 0,
                            default: None,
                            lead: true,
                        },
                        output_type: long(),
                    },
                ],
                vec![],
                vec![parameter(0)],
                None,
                Box::new(child),
                Parameters(calls.clone()),
                1,
            );
            let mut executor: Box<dyn Executor> = if pipelined {
                Box::new(tidb_executor::window::PipelinedWindowExec::new(base))
            } else {
                Box::new(base)
            };
            executor.open().unwrap();
            let mut output = executor.new_chunk();
            executor.next(&mut output).unwrap();
            assert_eq!(output.num_rows(), 2);
            let expected = if cume {
                vec![0, 0, 0, 0, 1, 1]
            } else {
                vec![1, 0, 0, 1]
            };
            assert_eq!(
                *calls.lock().unwrap(),
                expected,
                "pipelined={pipelined} cume={cume}"
            );
            executor.close().unwrap();
            calls.lock().unwrap().clear();
            executor.open().unwrap();
            executor.next(&mut output).unwrap();
            assert_eq!(output.num_rows(), 2);
            assert_eq!(
                *calls.lock().unwrap(),
                expected,
                "reopen pipelined={pipelined}"
            );
            executor.close().unwrap();
        }
    }
}
