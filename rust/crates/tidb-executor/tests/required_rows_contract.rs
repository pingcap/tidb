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

//! Go-derived required-row contracts at executor boundaries.

use std::cell::RefCell;
use std::rc::Rc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_executor::{ExecError, Executor, ExecutorMeta, LimitExec, ProjectionExec};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::NoColumns;

const MAX_CHUNK_SIZE: usize = 8;

fn long() -> FieldType {
    FieldType::new(FieldTypeCode::Long)
}

fn one_long_column_schema() -> Schema {
    let mut column = Column::new(1, long());
    column.index = 0;
    Schema::new(vec![column])
}

/// A source that makes chunk-size negotiation observable: each call returns
/// no more rows than the incoming chunk's `required_rows` value.
struct RequiredRowsSource {
    meta: ExecutorMeta,
    next_value: i64,
    end_value: i64,
    requests: Rc<RefCell<Vec<usize>>>,
}

impl RequiredRowsSource {
    fn new(end_value: i64, requests: Rc<RefCell<Vec<usize>>>) -> Self {
        RequiredRowsSource {
            meta: ExecutorMeta::new(one_long_column_schema(), 0, MAX_CHUNK_SIZE, MAX_CHUNK_SIZE),
            next_value: 1,
            end_value,
            requests,
        }
    }
}

impl Executor for RequiredRowsSource {
    fn open(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        let required_rows = req.required_rows();
        self.requests.borrow_mut().push(required_rows);
        req.reset();

        let remaining = usize::try_from((self.end_value - self.next_value + 1).max(0))
            .expect("remaining non-negative row count fits usize");
        for _ in 0..required_rows.min(remaining) {
            req.append_int64(0, self.next_value);
            self.next_value += 1;
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

fn int64_values(chunk: &Chunk) -> Vec<i64> {
    (0..chunk.num_rows())
        .map(|row| chunk.get_row(row).get_int64(0))
        .collect()
}

#[test]
fn projection_propagates_parent_required_rows_to_child() {
    // Source: pkg/executor/projection.go unParallelExecute and
    // pkg/executor/executor_required_rows_test.go TestProjectionUnparallelRequiredRows.
    let requests = Rc::new(RefCell::new(Vec::new()));
    let source = RequiredRowsSource::new(20, Rc::clone(&requests));
    let mut input_column = Column::new(1, long());
    input_column.index = 0;
    let mut projection = ProjectionExec::new(
        ExecutorMeta::new(one_long_column_schema(), 1, MAX_CHUNK_SIZE, MAX_CHUNK_SIZE),
        vec![Expression::Column(input_column)],
        Box::new(source),
        NoColumns,
    );

    projection.open().expect("projection opens");
    let mut output = projection.new_chunk();
    output.set_required_rows(2, MAX_CHUNK_SIZE);
    projection.next(&mut output).expect("projection next");

    assert_eq!(
        (int64_values(&output), requests.borrow().clone()),
        (vec![1, 2], vec![2]),
        "projection must forward the parent's row demand before pulling its child"
    );
}

#[test]
fn limit_preserves_parent_batches_while_skipping_offset() {
    // Go adjustRequiredRows asks for max=8, then offset remainder + parent=3,
    // then the next parent demand=2. This prevents both overproduction and
    // reading rows past the LIMIT window.
    let requests = Rc::new(RefCell::new(Vec::new()));
    let source = RequiredRowsSource::new(20, Rc::clone(&requests));
    let mut limit = LimitExec::new(
        ExecutorMeta::new(one_long_column_schema(), 1, 4, MAX_CHUNK_SIZE),
        9,
        4,
        Box::new(source),
    );

    limit.open().expect("limit opens");
    let mut output = limit.new_chunk();
    let mut batches = Vec::new();
    for _ in 0..2 {
        output.set_required_rows(2, MAX_CHUNK_SIZE);
        limit.next(&mut output).expect("limit next");
        batches.push(int64_values(&output));
    }

    assert_eq!(
        (batches, requests.borrow().clone()),
        (vec![vec![10, 11], vec![12, 13]], vec![8, 3, 2]),
        "LIMIT must preserve two-row parent batches and negotiate only the rows needed"
    );
}
