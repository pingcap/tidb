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
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Condvar,
};
use tidb_datatype::{Datum, FieldTypeCode};
use tidb_executor_join::joiner::{new_joiner, JoinType, JoinerChunkSizes};
use tidb_expr::column::{Column, CorrelatedColumn};

// One-chunk outer fixture; production no longer composes serial lanes.
struct WorkerOuter {
    meta: ExecutorMeta,
    feed: Arc<Mutex<(Option<Chunk>, bool)>>,
}
impl WorkerOuter {
    fn new(meta: ExecutorMeta, feed: Arc<Mutex<(Option<Chunk>, bool)>>) -> Self {
        Self { meta, feed }
    }
}
impl Executor for WorkerOuter {
    fn open(&mut self) -> Result<(), ExecError> {
        Ok(())
    }
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if let Some(mut chk) = lock(&self.feed).0.take() {
            req.swap_columns(&mut chk);
        }
        Ok(())
    }
    fn close(&mut self) -> Result<(), ExecError> {
        lock(&self.feed).0 = None;
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
    let opens = Arc::new(AtomicUsize::new(0));
    let closes = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::new();
    for id in 0..4 {
        let correlation = CorrelatedColumn::new(column.clone());
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
        workers.push(ApplyWorker::new(
            Box::new(inner),
            Vec::new(),
            vec![correlation],
            joiner,
        ));
    }
    (
        ParallelNestedLoopApplyExec::new(
            output_meta,
            Box::new(outer),
            Vec::new(),
            workers,
            false,
            ordered,
            cache,
            context,
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
        let mut input = lock(&executor.outer_exec).new_chunk();
        for n in [1, 2, 3, 4] {
            input.append_datum(0, &Datum::Int(n));
        }
        // Replenish the reusable source fixture, just as a storage Open does.
        let feed = Arc::new(Mutex::new((Some(input), false)));
        let schema = lock(&executor.outer_exec).schema().clone();
        *lock(&executor.outer_exec) = Box::new(WorkerOuter::new(
            ExecutorMeta::new(schema.clone(), 1, 1, 4),
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

/// Go `TestApplyGoroutinePanic` in `pkg/executor/parallel_apply_test.go`.
#[test]
fn test_apply_goroutine_panic() {
    for name in [
        "parallelApplyInnerWorkerPanic",
        "parallelApplyOuterWorkerPanic",
        "parallelApplyGetCachePanic",
        "parallelApplySetCachePanic",
    ] {
        let (mut executor, _, _) = fixture(false, true, 2, 0, None);
        let _guard = tidb_failpoint::enable_for_test(
            &format!("github.com/pingcap/tidb/pkg/executor/{name}"),
            "panic",
        );
        executor.open().unwrap();
        assert!(
            drain(&mut executor).is_err(),
            "Go {name} must reach the consumer"
        );
        executor.close().unwrap();
    }
}

/// Go `TestOrderedParallelApplyGoroutinePanic` in `pkg/executor/parallel_apply_test.go`.
#[test]
fn test_ordered_parallel_apply_goroutine_panic() {
    for name in [
        "parallelApplyInnerWorkerOrderedPanic",
        "parallelApplyOuterWorkerPanic",
    ] {
        let (mut executor, _, _) = fixture(true, true, 2, 0, None);
        let _guard = tidb_failpoint::enable_for_test(
            &format!("github.com/pingcap/tidb/pkg/executor/{name}"),
            "panic",
        );
        executor.open().unwrap();
        assert!(
            drain(&mut executor).is_err(),
            "Go {name} must reach the consumer"
        );
        executor.close().unwrap();
    }
}

/// Go `ParallelNestedLoopApplyExec.Next`'s `context.WithCancel(ctx)` and
/// `TestOrderedParallelApplyNested`: a nested Apply's requests inherit its
/// parent's cancellation even across executor reopens.
#[test]
fn nested_apply_cancellation_follows_the_worker_context() {
    let parent = StatementMemory::default().with_coprocessor_worker_scope();
    let nested = parent.clone().with_coprocessor_worker_scope();
    for _ in 0..2 {
        parent.renew_coprocessor_worker_scope();
        nested.renew_coprocessor_worker_scope();
        let outer_request = parent.coprocessor_request_cancellation();
        let inner_request = nested.coprocessor_request_cancellation();
        assert!(!inner_request.is_cancelled());
        nested.cancel_coprocessor_workers();
        assert!(inner_request.is_cancelled());
        assert!(!outer_request.is_cancelled());
        nested.renew_coprocessor_worker_scope();
        let inner_request = nested.coprocessor_request_cancellation();
        parent.cancel_coprocessor_workers();
        assert!(outer_request.is_cancelled());
        assert!(inner_request.is_cancelled());
        assert!(parent.check().is_ok());
    }
}

/// Go `executorBuilder.buildApply.constructSerialExec` keeps the caller's
/// context when cloning falls back to serial execution; `Next` has not
/// created any worker context. Pinned by zzprobe/c02.TestC02ApplyContexts.
#[test]
fn serial_apply_fallback_keeps_the_live_parent_context() {
    let parent = StatementMemory::default().with_coprocessor_worker_scope();
    let fallback = parent.clone().with_coprocessor_worker_scope();
    for _ in 0..2 {
        parent.renew_coprocessor_worker_scope();
        let request = fallback.coprocessor_request_cancellation();
        assert!(!request.is_cancelled());
        parent.cancel_coprocessor_workers();
        assert!(request.is_cancelled(), "serial fallback retains the current parent");
        assert!(parent.check().is_ok());
    }
}
