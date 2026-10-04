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

use super::*;
use crate::joiner::{new_joiner, JoinType, JoinerChunkSizes};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use tidb_datatype::{Datum, FieldTypeCode};
use tidb_expr::column::{Column, CorrelatedColumn};

struct Gate {
    entered: Mutex<usize>,
    ready: Condvar,
    maximum: AtomicUsize,
    width: usize,
}
impl Gate {
    fn enter(&self) -> Result<(), ExecError> {
        let mut entered = self.entered.lock().unwrap();
        *entered += 1;
        self.maximum.fetch_max(*entered, SeqCst);
        self.ready.notify_all();
        let (entered, timeout) = self
            .ready
            .wait_timeout_while(entered, std::time::Duration::from_secs(3), |n| {
                *n < self.width
            })
            .unwrap();
        if timeout.timed_out() && *entered < self.width {
            return Err(ExecError::internal("Apply workers did not overlap"));
        }
        Ok(())
    }
}
struct Inner {
    meta: ExecutorMeta,
    correlation: CorrelatedColumn,
    emitted: usize,
    rows: usize,
    gate: Option<Arc<Gate>>,
    opens: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
    fail: u8,
}
impl Executor for Inner {
    fn open(&mut self) -> Result<(), ExecError> {
        self.emitted = 0;
        self.opens.fetch_add(1, SeqCst);
        Ok(())
    }
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.emitted == self.rows {
            return Ok(());
        }
        if self.emitted == 0 {
            if let Some(gate) = &self.gate {
                gate.enter()?;
            }
            if self.fail == 1 {
                return Err(ExecError::internal("original inner error"));
            }
            if self.fail == 2 {
                panic!("original inner panic");
            }
        }
        let datum = self
            .correlation
            .data
            .as_ref()
            .unwrap()
            .read()
            .unwrap()
            .clone();
        req.append_datum(0, &datum);
        self.emitted += 1;
        Ok(())
    }
    fn close(&mut self) -> Result<(), ExecError> {
        self.closes.fetch_add(1, SeqCst);
        Ok(())
    }
    fn schema(&self) -> &Schema {
        self.meta.schema()
    }
    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }
    fn init_cap(&self) -> usize {
        1
    }
    fn max_chunk_size(&self) -> usize {
        1
    }
    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}
fn fixture(
    ordered: bool,
    cache: bool,
    inner_rows: usize,
    failure: u8,
    gate: Option<Arc<Gate>>,
) -> (
    ParallelNestedLoopApplyExec,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let ty = FieldType::new(FieldTypeCode::LongLong);
    let column = Column::new(1, ty.clone());
    let schema = Schema::new(vec![column.clone()]);
    let outer_meta = ExecutorMeta::new(schema.clone(), 1, 1, 4);
    let mut input = outer_meta.new_chunk();
    for n in [1, 2, 3, 4] {
        input.append_datum(0, &Datum::Int(n));
    }
    let outer = WorkerOuter::new(
        outer_meta.clone(),
        Arc::new(Mutex::new((Some(input), false))),
    );
    let context = StmtContext::for_query()
        .with_apply_cache_capacity(4096)
        .with_coprocessor_worker_scope();
    let output_meta = ExecutorMeta::new(
        Schema::new(vec![column.clone(), Column::new(2, ty.clone())]),
        2,
        1,
        1,
    );
    let stop = Arc::new(AtomicBool::new(false));
    let opens = Arc::new(AtomicUsize::new(0));
    let closes = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::new();
    for id in 0..4 {
        let correlation = CorrelatedColumn::new(column.clone());
        let feed = Arc::new(Mutex::new((None, false)));
        let inner = Inner {
            meta: ExecutorMeta::new(schema.clone(), 3, 1, 1),
            correlation: correlation.clone(),
            emitted: 0,
            rows: inner_rows,
            gate: gate.clone(),
            opens: opens.clone(),
            closes: closes.clone(),
            fail: if id == 0 { failure } else { 0 },
        };
        let joiner = new_joiner(
            context.clone(),
            JoinType::Inner,
            false,
            &[Datum::Null],
            Vec::new(),
            &[ty.clone()],
            &[ty.clone()],
            None,
            false,
            JoinerChunkSizes {
                vectorized: true,
                init_chunk_size: 1,
                max_chunk_size: 1,
            },
        );
        let serial = NestedLoopApplyExec::new(
            output_meta.clone(),
            Box::new(WorkerOuter::new(outer_meta.clone(), feed.clone())),
            Box::new(CancellableInner::new(Box::new(inner), stop.clone())),
            Vec::new(),
            Vec::new(),
            vec![correlation],
            joiner,
            false,
            cache,
            context.clone(),
        );
        workers.push(ApplyWorker {
            executor: Arc::new(Mutex::new(serial)),
            feed,
        });
    }
    (
        ParallelNestedLoopApplyExec::new(
            output_meta,
            Box::new(outer),
            Vec::new(),
            workers,
            ordered,
            cache,
            context,
            stop,
            None,
        ),
        opens,
        closes,
    )
}
fn drain(executor: &mut ParallelNestedLoopApplyExec) -> Result<Vec<Vec<Datum>>, ExecError> {
    let mut rows = Vec::new();
    let mut output = executor.new_chunk();
    loop {
        executor.next(&mut output)?;
        if output.num_rows() == 0 {
            break;
        }
        for i in 0..output.num_rows() {
            rows.push(
                executor
                    .ret_field_types()
                    .iter()
                    .enumerate()
                    .map(|(column, ty)| output.get_row(i).get_datum(column, ty))
                    .collect(),
            );
        }
    }
    Ok(rows)
}
#[test]
fn native_parallel_apply_overlaps_isolated_inner_bindings_and_preserves_order() {
    let gate = Arc::new(Gate {
        entered: Mutex::new(0),
        ready: Condvar::new(),
        maximum: AtomicUsize::new(0),
        width: 4,
    });
    let (mut executor, opens, closes) = fixture(true, false, 2, 0, Some(gate.clone()));
    executor.open().unwrap();
    assert_eq!(
        drain(&mut executor).unwrap(),
        (1..=4)
            .flat_map(|n| vec![vec![Datum::Int(n), Datum::Int(n)]; 2])
            .collect::<Vec<_>>()
    );
    assert_eq!(gate.maximum.load(SeqCst), 4);
    executor.close().unwrap();
    assert_eq!(opens.load(SeqCst), closes.load(SeqCst));
    assert_eq!(executor.memory.bytes_consumed(), 0);
}
#[test]
fn native_parallel_apply_unordered_results_and_shared_cache_retire() {
    let (mut executor, opens, closes) = fixture(false, true, 2, 0, None);
    executor.open().unwrap();
    let mut rows = drain(&mut executor).unwrap();
    rows.sort_by_key(|row| format!("{row:?}"));
    assert_eq!(
        rows,
        (1..=4)
            .flat_map(|n| vec![vec![Datum::Int(n), Datum::Int(n)]; 2])
            .collect::<Vec<_>>()
    );
    executor.close().unwrap();
    assert_eq!(opens.load(SeqCst), closes.load(SeqCst));
    assert_eq!(executor.memory.bytes_consumed(), 0);
}
#[test]
fn native_parallel_apply_early_close_joins_full_output_queues_and_reopens() {
    let (mut executor, opens, closes) = fixture(true, true, 100, 0, None);
    for _ in 0..3 {
        let mut input = executor.outer.new_chunk();
        for n in [1, 2, 3, 4] {
            input.append_datum(0, &Datum::Int(n));
        }
        // Replenish the reusable source fixture, just as a storage Open does.
        let feed = Arc::new(Mutex::new((Some(input), false)));
        executor.outer = Box::new(WorkerOuter::new(
            ExecutorMeta::new(executor.outer.schema().clone(), 1, 1, 4),
            feed,
        ));
        executor.open().unwrap();
        executor.next(&mut executor.new_chunk()).unwrap();
        executor.close().unwrap();
        assert_eq!(opens.load(SeqCst), closes.load(SeqCst));
        assert_eq!(executor.memory.bytes_consumed(), 0);
    }
}
#[test]
fn native_parallel_apply_preserves_errors_and_panic_messages_and_joins() {
    for (failure, message) in [(1, "original inner error"), (2, "original inner panic")] {
        let (mut executor, opens, closes) = fixture(false, false, 10, failure, None);
        executor.open().unwrap();
        let error = drain(&mut executor).unwrap_err();
        assert!(format!("{error:?}").contains(message), "{error:?}");
        executor.close().unwrap();
        assert_eq!(opens.load(SeqCst), closes.load(SeqCst));
        assert_eq!(executor.memory.bytes_consumed(), 0);
    }
}

#[test]
fn apply_close_cancels_request_children_without_killing_the_statement_and_reopen_rotates() {
    let (mut executor, _, _) = fixture(true, true, 1, 0, None);
    executor.open().unwrap();
    let request = executor.memory.coprocessor_request_cancellation();
    let sibling = executor.memory.coprocessor_request_cancellation();
    request.cancel();
    assert!(
        !sibling.is_cancelled(),
        "response Close cannot cancel sibling requests"
    );
    executor.close().unwrap();
    assert!(
        sibling.is_cancelled(),
        "Apply Close must retire in-flight requests"
    );
    assert!(
        executor.memory.check().is_ok(),
        "child scope cannot poison the statement SQL killer"
    );
    executor.open().unwrap();
    let next = executor.memory.coprocessor_request_cancellation();
    assert!(
        !next.is_cancelled(),
        "new Open owns a fresh cancellation generation"
    );
    executor.close().unwrap();
    assert!(next.is_cancelled());
}
