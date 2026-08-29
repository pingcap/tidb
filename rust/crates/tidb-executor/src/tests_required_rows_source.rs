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

//! Go `pkg/executor/executor_required_rows_test.go`: the chunk `requiredRows`
//! contract of every pull operator, pinned through the same instrumented data
//! source Go's suite uses.
//!
//! Go's `requiredRowsDataSource` (pkg/executor/executor_required_rows_test.go:53) is a
//! `[Double, Long]` source that fills `min(req.RequiredRows(), remaining)`
//! rows per `Next` and PANICS unless the batch size equals the per-call
//! expectation recorded in `expectedRowsRet` (`pkg/executor/executor_required_rows_test.go
// :73-86`); `checkNumNextCalled` requires the child to be read exactly as
//! often as the recorded pattern. Both halves are ported verbatim, so every
//! assertion below also pins how many rows the operator pulls from its child
//! per call -- not merely what it emits.
//!
//! Go builds `defaultCtx()` with `DefInitChunkSize`/`DefMaxChunkSize`
//! (pkg/executor/executor_required_rows_test.go:246); this port uses the same shipped
//! 1024 (`tidb-vardef::defaults::DEF_MAX_CHUNK_SIZE`).

use tidb_ast::CiString;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::expression::{Constant, Expression, ScalarFunction};
use tidb_expr::schema::Schema;

use tidb_expr::NoColumns;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::limit::LimitExec;
use crate::mem_quota::StatementMemory;
use crate::projection::ProjectionExec;
use crate::selection::SelectionExec;
use crate::sort::SortByItem;

use crate::topn::TopNExec;

const MAX_CHUNK_SIZE: usize = 1024;

fn double() -> FieldType {
    FieldType::new(FieldTypeCode::Double)
}

fn long() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

/// The fixed `[Double, Long]` output schema of Go's data source
/// (pkg/executor/executor_required_rows_test.go:56).
fn source_schema() -> Schema {
    let mut first = Column::new(1, double());
    first.index = 0;
    let mut second = Column::new(2, long());
    second.index = 1;
    Schema::new(vec![first, second])
}

fn source_meta(init_cap: usize) -> ExecutorMeta {
    ExecutorMeta::new(source_schema(), 1, init_cap, MAX_CHUNK_SIZE)
}

fn by_items_over(column_offsets: &[usize]) -> Vec<SortByItem> {
    column_offsets
        .iter()
        .map(|&offset| {
            let mut column = Column::new(
                offset as i64 + 1,
                if offset == 0 { double() } else { long() },
            );
            column.index = offset as i64;
            SortByItem {
                expr: Expression::Column(column),
                desc: false,
            }
        })
        .collect()
}

/// Go `requiredRowsDataSource.generator` implementations. Each produces one
/// `[Double, Long]` row per call in Go's column order (executor
/// _required_rows_test.go:108-116: the generator runs per column, Double
/// first).
enum RowGenerator {
    /// Go `defaultGenerator`: arbitrary values; every case using it asserts
    /// row counts only.
    Default { next: u64 },
    /// Go `gen01` (pkg/executor/executor_required_rows_test.go:441): the Long column
    /// alternates 0/1 per row starting at 0; the Double column is unused.
    Alternating01 { count: u64 },
    /// Go `divGenerator(factor)` (pkg/executor/executor_required_rows_test.go:695): both
    /// columns carry `integer_count / factor`; only the integer counter
    /// advances per row.
    Divided { factor: u64, count: u64 },
}

/// Go `requiredRowsDataSource` (pkg/executor/executor_required_rows_test.go:53-130).
struct RequiredRowsDataSource {
    meta: ExecutorMeta,
    total_rows: usize,
    count: usize,
    expected_rows_ret: Option<Vec<usize>>,
    num_next_called: usize,
    generator: RowGenerator,
}

impl RequiredRowsDataSource {
    fn new(total_rows: usize, expected_rows_ret: Option<Vec<usize>>) -> Box<Self> {
        Self::with_generator(
            total_rows,
            expected_rows_ret,
            RowGenerator::Default { next: 0 },
        )
    }

    fn with_generator(
        total_rows: usize,
        expected_rows_ret: Option<Vec<usize>>,
        generator: RowGenerator,
    ) -> Box<Self> {
        Box::new(Self {
            meta: source_meta(MAX_CHUNK_SIZE),
            total_rows,
            count: 0,
            expected_rows_ret,
            num_next_called: 0,
            generator,
        })
    }

    fn append_one_row(&mut self, req: &mut Chunk) {
        match &mut self.generator {
            RowGenerator::Default { next } => {
                let n = *next;
                *next += 1;
                req.append_float64(0, n as f64);
                req.append_int64(1, n as i64);
            }
            RowGenerator::Alternating01 { count } => {
                let c = *count;
                *count += 1;
                req.append_float64(0, c as f64);
                req.append_int64(1, (c % 2) as i64);
            }
            RowGenerator::Divided { factor, count } => {
                let c = *count;
                *count += 1;
                req.append_float64(0, (c / *factor) as f64);
                req.append_int64(1, (c / *factor) as i64);
            }
        }
    }
}

impl Executor for RequiredRowsDataSource {
    fn open(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    /// Go `Next` (pkg/executor/executor_required_rows_test.go:88-106): fill
    /// `min(requiredRows, remaining)` rows, then verify the batch size
    /// against the per-call expectation (Go panics in the deferred check).
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let required = req.required_rows().min(self.total_rows - self.count);
        for _ in 0..required {
            self.append_one_row(req);
        }
        self.count += required;
        let call = self.num_next_called;
        if let Some(expected) = &self.expected_rows_ret {
            match expected.get(call) {
                Some(&expected_rows) => assert_eq!(
                    req.num_rows(),
                    expected_rows,
                    "unexpected number of rows returned, obtain: {}, expected: {}",
                    req.num_rows(),
                    expected_rows,
                ),
                None => panic!(
                    "data source Next called {} times; the recorded pattern has only {} calls",
                    call + 1,
                    expected.len(),
                ),
            }
        }
        self.num_next_called += 1;
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

/// One `chk.SetRequiredRows`/`Next`/row-count round driven the way every Go
/// case in the suite drives its executor (pkg/executor/executor_required_rows_test.go:194
/// -202).
fn next_batch(executor: &mut dyn Executor, required: usize) -> usize {
    let mut req = executor.new_chunk();
    req.set_required_rows(required as isize, executor.max_chunk_size());
    executor.next(&mut req).unwrap();
    req.num_rows()
}

/// Go `pkg/executor/executor_required_rows_test.go:131::TestLimitRequiredRows`.
///
/// Go's `buildLimitExec` (pkg/executor/executor_required_rows_test.go:206) caps the child
/// chunk at `min(count, maxChunkSize)`; the relay contract itself is
/// `LimitExec.adjustRequiredRows` (`pkg/executor/select.go`): skip rows plus
/// requested rows, capped by the remaining window and the chunk maximum.
#[test]
fn limit_required_rows() {
    struct Case {
        total_rows: usize,
        limit_offset: u64,
        limit_count: u64,
        required_rows: Vec<usize>,
        expected_rows: Vec<usize>,
        expected_rows_ds: Vec<usize>,
    }
    let cases = [
        // pkg/executor/executor_required_rows_test.go:141-149
        Case {
            total_rows: 20,
            limit_offset: 0,
            limit_count: 10,
            required_rows: vec![3, 5, 1, 500, 500],
            expected_rows: vec![3, 5, 1, 1, 0],
            expected_rows_ds: vec![3, 5, 1, 1],
        },
        // pkg/executor/executor_required_rows_test.go:150-156
        Case {
            total_rows: 20,
            limit_offset: 0,
            limit_count: 25,
            required_rows: vec![9, 500],
            expected_rows: vec![9, 11],
            expected_rows_ds: vec![9, 11],
        },
        // pkg/executor/executor_required_rows_test.go:157-164: the first child batch must
        // carry the rows still to skip (60 = 50 offset + 10 requested).
        Case {
            total_rows: 100,
            limit_offset: 50,
            limit_count: 30,
            required_rows: vec![10, 5, 10, 20],
            expected_rows: vec![10, 5, 10, 5],
            expected_rows_ds: vec![60, 5, 10, 5],
        },
        // pkg/executor/executor_required_rows_test.go:165-170
        Case {
            total_rows: 100,
            limit_offset: 101,
            limit_count: 10,
            required_rows: vec![10],
            expected_rows: vec![0],
            expected_rows_ds: vec![100, 0],
        },
        // pkg/executor/executor_required_rows_test.go:171-178: the offset spans a whole
        // chunk, so the second child batch carries exactly the 4 window rows.
        Case {
            total_rows: MAX_CHUNK_SIZE + 20,
            limit_offset: (MAX_CHUNK_SIZE + 1) as u64,
            limit_count: 10,
            required_rows: vec![3, 3, 3, 100],
            expected_rows: vec![3, 3, 3, 1],
            expected_rows_ds: vec![MAX_CHUNK_SIZE, 4, 3, 3, 1],
        },
    ];
    for case in &cases {
        let ds = RequiredRowsDataSource::new(case.total_rows, Some(case.expected_rows_ds.clone()));
        let init_cap = case.limit_count.min(MAX_CHUNK_SIZE as u64) as usize;
        let mut limit = LimitExec::new(
            source_meta(init_cap),
            case.limit_offset,
            case.limit_count,
            ds as Box<dyn Executor>,
        );
        limit.open().unwrap();
        for (call, &required) in case.required_rows.iter().enumerate() {
            assert_eq!(
                next_batch(&mut limit, required),
                case.expected_rows[call],
                "case offset={} count={} call {call}",
                case.limit_offset,
                case.limit_count,
            );
        }
        limit.close().unwrap();
    }
}

/// Go `pkg/executor/executor_required_rows_test.go:212::TestDMLChildChunkInitCapByRowWidth`.
// go-parity-gap: Go sizes a `DeleteExec` DML child chunk through
// `newDMLChildChunk` (`pkg/executor/delete.go`: capacity 1 when the row is
// wider than the quota, else the child's init cap); this tier has no
// `DeleteExec`/`newDMLChildChunk` seam -- DML runs through
// `crate::driver::dml` with no per-width chunk sizing to pin.

/// Go `pkg/executor/executor_required_rows_test.go:251::TestSortRequiredRows`.
///
/// Go's `buildSortExec` (pkg/executor/executor_required_rows_test.go:293) sorts by the
/// named source columns ascending; Go's `SortExec.Next`
/// (`pkg/executor/sortexec/sort_exec.go`, `onePartitionSorting`) fills each
/// parent request exactly (`req.IsFull()`), so its case table pins both the
/// OUTPUT batching ([1,5,3,1]-shaped windows) and the drain batches the child
/// recorded (`expectedRowsDS` like [10, 0] or [1024, 1, 0]).
// go-parity-gap: the ported `crate::sort::SortExec::next` appends sorted rows
// up to `max_chunk_size` per call (`append_sorted_rows_into(req, batch)`)
// without consulting the parent request's required rows, so none of Go's
// per-request output expectations can be pinned on it.

/// Go `pkg/executor/executor_required_rows_test.go:321::TestTopNRequiredRows`.
///
/// Go's `buildTopNExec` (pkg/executor/executor_required_rows_test.go:411) is a `TopNExec`
/// with `Concurrency: 5`; concurrency only changes Go's post-spill worker
/// scheduling, never the serial child fetch the `expectedRowsDS` patterns
/// record, so this port pins the same patterns against the default
/// (single-threaded) fetch.
#[test]
fn topn_required_rows() {
    struct Case {
        total_rows: usize,
        topn_offset: u64,
        topn_count: u64,
        group_by: Vec<usize>,
        required_rows: Vec<usize>,
        expected_rows: Vec<usize>,
        expected_rows_ds: Vec<usize>,
    }
    let cases = [
        // pkg/executor/executor_required_rows_test.go:330-339
        Case {
            total_rows: 10,
            topn_offset: 0,
            topn_count: 10,
            group_by: vec![0],
            required_rows: vec![1, 1, 1, 1, 10],
            expected_rows: vec![1, 1, 1, 1, 6],
            expected_rows_ds: vec![10, 0],
        },
        // pkg/executor/executor_required_rows_test.go:340-349
        Case {
            total_rows: 100,
            topn_offset: 15,
            topn_count: 11,
            group_by: vec![0],
            required_rows: vec![1, 1, 1, 1, 10],
            expected_rows: vec![1, 1, 1, 1, 7],
            expected_rows_ds: vec![100, 0],
        },
        // pkg/executor/executor_required_rows_test.go:350-357: only 5 window rows exist.
        Case {
            total_rows: 100,
            topn_offset: 95,
            topn_count: 10,
            group_by: vec![0],
            required_rows: vec![1, 2, 3, 10],
            expected_rows: vec![1, 2, 2, 0],
            expected_rows_ds: vec![100, 0, 0],
        },
        // pkg/executor/executor_required_rows_test.go:358-366
        Case {
            total_rows: MAX_CHUNK_SIZE + 20,
            topn_offset: 1,
            topn_count: 5,
            group_by: vec![0, 1],
            required_rows: vec![1, 3, 7, 10],
            expected_rows: vec![1, 3, 1, 0],
            expected_rows_ds: vec![MAX_CHUNK_SIZE, 20, 0],
        },
        // pkg/executor/executor_required_rows_test.go:367-375
        Case {
            total_rows: 2 * MAX_CHUNK_SIZE + 20,
            topn_offset: (MAX_CHUNK_SIZE + 10) as u64,
            topn_count: 8,
            group_by: vec![0, 1],
            required_rows: vec![1, 2, 3, 5, 7],
            expected_rows: vec![1, 2, 3, 2, 0],
            expected_rows_ds: vec![MAX_CHUNK_SIZE, MAX_CHUNK_SIZE, 20, 0],
        },
        // pkg/executor/executor_required_rows_test.go:376-385: the offset is past the last
        // row, yet every row is still fetched (the skip is only known after
        // the full scan).
        Case {
            total_rows: 5 * MAX_CHUNK_SIZE + 10,
            topn_offset: (5 * MAX_CHUNK_SIZE + 20) as u64,
            topn_count: 10,
            group_by: vec![0, 1],
            required_rows: vec![1, 2, 3],
            expected_rows: vec![0, 0, 0],
            expected_rows_ds: vec![
                MAX_CHUNK_SIZE,
                MAX_CHUNK_SIZE,
                MAX_CHUNK_SIZE,
                MAX_CHUNK_SIZE,
                MAX_CHUNK_SIZE,
                10,
                0,
                0,
            ],
        },
        // pkg/executor/executor_required_rows_test.go:386-394: `count = MaxInt64`.
        Case {
            total_rows: 2 * MAX_CHUNK_SIZE + 10,
            topn_offset: 10,
            topn_count: i64::MAX as u64,
            group_by: vec![0, 1],
            required_rows: vec![1, 2, 3, MAX_CHUNK_SIZE, MAX_CHUNK_SIZE],
            expected_rows: vec![1, 2, 3, MAX_CHUNK_SIZE, MAX_CHUNK_SIZE - 1 - 2 - 3],
            expected_rows_ds: vec![MAX_CHUNK_SIZE, MAX_CHUNK_SIZE, 10, 0, 0],
        },
    ];
    for case in &cases {
        let ds = RequiredRowsDataSource::new(case.total_rows, Some(case.expected_rows_ds.clone()));
        let mut executor = TopNExec::new(
            source_meta(MAX_CHUNK_SIZE),
            by_items_over(&case.group_by),
            ds as Box<dyn Executor>,
            NoColumns,
            case.topn_offset,
            case.topn_count,
            StatementMemory::default(),
        );
        executor.open().unwrap();
        for (call, &required) in case.required_rows.iter().enumerate() {
            assert_eq!(
                next_batch(&mut executor, required),
                case.expected_rows[call],
                "topn case offset={} count={} call {call}",
                case.topn_offset,
                case.topn_count,
            );
        }
        executor.close().unwrap();
    }
}

/// Go `pkg/executor/executor_required_rows_test.go:432::TestSelectionRequiredRows`.
///
/// Go's `buildSelectionExec` (pkg/executor/executor_required_rows_test.go:511) filters
/// with `eq(col1, <constant>)` when the case carries a generator; the batched
/// selection drains its child in max-chunk-size batches, so the child pattern
/// is the drain pattern, while the OUTPUT pattern is what survives the filter
/// (`pkg/executor/selection.go`, Go `SelectionExec.Next`).
#[test]
fn selection_required_rows() {
    struct Case {
        total_rows: usize,
        filters_of_col1: Option<i64>,
        required_rows: Vec<usize>,
        expected_rows: Vec<usize>,
        expected_rows_ds: Vec<usize>,
    }
    let cases = [
        // pkg/executor/executor_required_rows_test.go:455-461: no filter at all.
        Case {
            total_rows: 20,
            filters_of_col1: None,
            required_rows: vec![1, 2, 3, 4, 5, 20],
            expected_rows: vec![1, 2, 3, 4, 5, 5],
            expected_rows_ds: vec![20, 0],
        },
        // pkg/executor/executor_required_rows_test.go:462-470: keep the col1 == 0 half.
        Case {
            total_rows: 20,
            filters_of_col1: Some(0),
            required_rows: vec![1, 3, 5, 7, 9],
            expected_rows: vec![1, 3, 5, 1, 0],
            expected_rows_ds: vec![20, 0, 0],
        },
        // pkg/executor/executor_required_rows_test.go:471-479: keep the col1 == 1 half of
        // 1044 alternating rows (522 surviving rows in total).
        Case {
            total_rows: MAX_CHUNK_SIZE + 20,
            filters_of_col1: Some(1),
            required_rows: vec![1, 3, 5, MAX_CHUNK_SIZE],
            expected_rows: vec![1, 3, 5, MAX_CHUNK_SIZE / 2 - 1 - 3 - 5 + 10],
            expected_rows_ds: vec![MAX_CHUNK_SIZE, 20, 0],
        },
    ];
    for case in &cases {
        let (ds, filters) = match case.filters_of_col1 {
            None => (
                RequiredRowsDataSource::new(case.total_rows, Some(case.expected_rows_ds.clone()))
                    as Box<dyn Executor>,
                Vec::new(),
            ),
            Some(value) => {
                let ds = RequiredRowsDataSource::with_generator(
                    case.total_rows,
                    Some(case.expected_rows_ds.clone()),
                    RowGenerator::Alternating01 { count: 0 },
                );
                // Go: expression.NewFunction(sctx.GetExprCtx(), ast.EQ, ETInt,
                // ds.Schema().Columns[1], Constant(filtersOfCol1, TypeTiny))
                // (pkg/executor/executor_required_rows_test.go:482-490).
                let mut column = Column::new(2, long());
                column.index = 1;
                let filter = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("eq"),
                    long(),
                    vec![
                        Expression::Column(column),
                        Expression::Constant(Constant::new(
                            Datum::Int(value),
                            FieldType::new(FieldTypeCode::Tiny),
                        )),
                    ],
                ));
                (ds as Box<dyn Executor>, vec![filter])
            }
        };
        let mut executor = SelectionExec::new(
            source_meta(MAX_CHUNK_SIZE),
            filters,
            ds,
            NoColumns,
            StatementMemory::default(),
        );
        executor.open().unwrap();
        for (call, &required) in case.required_rows.iter().enumerate() {
            assert_eq!(
                next_batch(&mut executor, required),
                case.expected_rows[call],
                "selection case filter={:?} call {call}",
                case.filters_of_col1,
            );
        }
        executor.close().unwrap();
    }
}

/// Go `pkg/executor/executor_required_rows_test.go:572::TestProjectionParallelRequiredRows`
/// is skipped in Go itself (`t.Skip("not stable because of goroutine
/// schedule")`, pkg/executor/executor_required_rows_test.go:573), and parallel projection
/// is documented as deferred for this tier (see `lib.rs`); nothing to port.

/// Go `pkg/executor/executor_required_rows_test.go:521::TestProjectionUnparallelRequiredRows`.
///
/// Go's `buildProjectionExec` (pkg/executor/executor_required_rows_test.go:630) projects
/// the identity of both source columns; an unparallel projection forwards the
/// parent's request to its child one-for-one
/// (`pkg/executor/projection.go`, Go `ProjectionExec.Next`), so the child
/// pattern equals the output pattern in every case.
#[test]
fn projection_unparallel_required_rows() {
    struct Case {
        total_rows: usize,
        required_rows: Vec<usize>,
        expected_rows: Vec<usize>,
    }
    let cases = [
        // pkg/executor/executor_required_rows_test.go:530-537
        Case {
            total_rows: 20,
            required_rows: vec![1, 3, 5, 7, 9],
            expected_rows: vec![1, 3, 5, 7, 4],
        },
        // pkg/executor/executor_required_rows_test.go:538-545
        Case {
            total_rows: MAX_CHUNK_SIZE + 10,
            required_rows: vec![1, 3, 5, 7, 9, MAX_CHUNK_SIZE],
            expected_rows: vec![1, 3, 5, 7, 9, MAX_CHUNK_SIZE - 1 - 3 - 5 - 7 - 9 + 10],
        },
        // pkg/executor/executor_required_rows_test.go:546-553
        Case {
            total_rows: 2 * MAX_CHUNK_SIZE + 10,
            required_rows: vec![1, 7, 9, MAX_CHUNK_SIZE, MAX_CHUNK_SIZE + 10],
            expected_rows: vec![1, 7, 9, MAX_CHUNK_SIZE, MAX_CHUNK_SIZE + 10 - 1 - 7 - 9],
        },
    ];
    for case in &cases {
        let ds = RequiredRowsDataSource::new(case.total_rows, Some(case.expected_rows.clone()));
        let mut first = Column::new(1, double());
        first.index = 0;
        let mut second = Column::new(2, long());
        second.index = 1;
        let exprs = vec![Expression::Column(first), Expression::Column(second)];
        let mut executor = ProjectionExec::new(
            source_meta(MAX_CHUNK_SIZE),
            exprs,
            ds as Box<dyn Executor>,
            NoColumns,
        );
        executor.open().unwrap();
        for (call, &required) in case.required_rows.iter().enumerate() {
            assert_eq!(
                next_batch(&mut executor, required),
                case.expected_rows[call],
                "projection case {} call {call}",
                case.total_rows,
            );
        }
        executor.close().unwrap();
    }
}
