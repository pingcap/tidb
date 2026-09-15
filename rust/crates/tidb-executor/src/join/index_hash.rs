// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go indexHashJoinInnerWorker: build outer rows, probe inner rows, and
//! optionally retain match pointers to preserve the required outer order.

use super::*;

type OuterHash = std::collections::HashMap<
    u64,
    Vec<usize>,
    std::hash::BuildHasherDefault<crate::hash_join::IdentityU64Hasher>,
>;

#[derive(Clone)]
pub(super) struct IndexHashOutput {
    pub kind: JoinKind,
    pub outer_is_left: bool,
    pub ordered: bool,
    pub output: JoinOutput,
    pub conditions: Vec<Expression>,
    pub condition_types: Vec<FieldType>,
    pub left_types: Vec<FieldType>,
    pub right_types: Vec<FieldType>,
    pub output_types: Vec<FieldType>,
}

pub(super) struct IndexHashState {
    buckets: OuterHash,
    ordered: Option<Vec<Vec<RowPtr>>>,
    matched: Vec<bool>,
    keys: Vec<EquiKey>,
    inner_chunk: usize,
    inner_row: usize,
    candidate: usize,
    probe_hash: Option<Option<u64>>,
    outer_row: usize,
    scratch: Chunk,
    pub evaluations: u64,
    pub done: bool,
    tracker: Arc<Tracker>,
    bytes: i64,
}

impl Drop for IndexHashState {
    fn drop(&mut self) {
        self.tracker.consume(-self.bytes);
    }
}

impl IndexHashOutput {
    fn outer_types(&self) -> &[FieldType] {
        if self.outer_is_left {
            &self.left_types
        } else {
            &self.right_types
        }
    }

    fn inner_types(&self) -> &[FieldType] {
        if self.outer_is_left {
            &self.right_types
        } else {
            &self.left_types
        }
    }

    fn equal(&self, keys: &[EquiKey], outer: Row<'_>, inner: Row<'_>) -> Result<bool, ExecError> {
        let (left, right) = if self.outer_is_left {
            (outer, inner)
        } else {
            (inner, outer)
        };
        equi_keys_equal_chunk_rows(keys, left, &self.left_types, right, &self.right_types)
            .map_err(key_error)
    }

    fn matches<C: Columns>(
        &self,
        ctx: &C,
        outer: Row<'_>,
        inner: Row<'_>,
        state: &mut IndexHashState,
    ) -> Result<bool, ExecError> {
        if self.conditions.is_empty() {
            return Ok(true);
        }
        state.evaluations += 1;
        state.scratch.reset();
        let (left, right) = if self.outer_is_left {
            (outer, inner)
        } else {
            (inner, outer)
        };
        state.scratch.append_partial_row(0, left);
        state.scratch.append_partial_row(left.len(), right);
        let (matched, has_null) =
            crate::joiner::eval_bool(ctx, &self.conditions, state.scratch.get_row(0))?;
        // Preserve the existing anti-semi NOT-IN/NULL residual contract.
        Ok(matched || self.kind == JoinKind::AntiSemi && has_null)
    }

    fn matched(&self, req: &mut Chunk, outer: Row<'_>, inner: Row<'_>) {
        match self.kind {
            JoinKind::Inner | JoinKind::Left | JoinKind::Right => {
                self.output.chunks(req, self.outer_is_left, outer, inner)
            }
            JoinKind::Semi => self.output.preserved(req, outer),
            JoinKind::LeftOuterSemi => {
                self.output.preserved(req, outer);
                req.append_datum(self.output.width(), &Datum::Int(1));
            }
            JoinKind::AntiSemi => {}
        }
    }

    fn unmatched(&self, req: &mut Chunk, outer: Row<'_>) {
        match self.kind {
            JoinKind::Left | JoinKind::Right => {
                self.output.unmatched(req, self.outer_is_left, outer, 0)
            }
            JoinKind::AntiSemi => self.output.preserved(req, outer),
            JoinKind::LeftOuterSemi => {
                self.output.preserved(req, outer);
                req.append_datum(self.output.width(), &Datum::Int(0));
            }
            JoinKind::Inner | JoinKind::Semi => {}
        }
    }

    fn semi(&self) -> bool {
        matches!(
            self.kind,
            JoinKind::Semi | JoinKind::AntiSemi | JoinKind::LeftOuterSemi
        )
    }
}

impl IndexHashState {
    pub fn new(
        output: &IndexHashOutput,
        keys: &[EquiKey],
        outer: &OuterBatch,
        inner: &List,
        tracker: &Arc<Tracker>,
        memory: &StatementMemory,
    ) -> Result<Self, ExecError> {
        let mut state = Self::build(output, keys, outer, tracker, memory)?;
        state.prepare_ordered(output, outer, inner, memory)?;
        Ok(state)
    }

    fn build(
        output: &IndexHashOutput,
        keys: &[EquiKey],
        outer: &OuterBatch,
        tracker: &Arc<Tracker>,
        memory: &StatementMemory,
    ) -> Result<Self, ExecError> {
        let mut buckets = OuterHash::with_capacity_and_hasher(outer.len(), Default::default());
        for index in 0..outer.len() {
            if let Some(hash) =
                row_hash_chunk(keys, outer.row(index), output.outer_types(), |key| {
                    if output.outer_is_left {
                        key.left
                    } else {
                        key.right
                    }
                })
                .map_err(key_error)?
            {
                buckets.entry(hash).or_default().push(index);
            }
        }
        let mut state = Self {
            buckets,
            ordered: output.ordered.then(|| vec![Vec::new(); outer.len()]),
            matched: vec![false; outer.len()],
            keys: keys.to_vec(),
            inner_chunk: 0,
            inner_row: 0,
            candidate: 0,
            probe_hash: None,
            outer_row: 0,
            scratch: Chunk::new_with_capacity(&output.condition_types, 1),
            evaluations: 0,
            done: false,
            tracker: Arc::clone(tracker),
            bytes: 0,
        };
        state.account(memory)?;
        Ok(state)
    }

    fn prepare_ordered(
        &mut self,
        output: &IndexHashOutput,
        outer: &OuterBatch,
        inner: &List,
        memory: &StatementMemory,
    ) -> Result<(), ExecError> {
        if output.ordered {
            for chunk in 0..inner.num_chunks() {
                for index in 0..inner.num_rows_of_chunk(chunk) {
                    let ptr = RowPtr::new(chunk as u32, index as u32);
                    let row = inner.get_row(ptr);
                    let Some(hash) = row_hash_chunk(&self.keys, row, output.inner_types(), |key| {
                        if output.outer_is_left {
                            key.right
                        } else {
                            key.left
                        }
                    })
                    .map_err(key_error)?
                    else {
                        continue;
                    };
                    for &at in self.buckets.get(&hash).into_iter().flatten() {
                        if output.equal(&self.keys, outer.row(at), row)? {
                            self.ordered.as_mut().expect("ordered variant")[at].push(ptr);
                        }
                    }
                }
                self.account(memory)?;
            }
            // Ordered probes need only their per-outer match lists now.
            self.buckets = OuterHash::default();
            self.account(memory)?;
        }
        Ok(())
    }

    fn account(&mut self, memory: &StatementMemory) -> Result<(), ExecError> {
        let bytes = self.buckets.capacity() * (size_of::<(u64, Vec<usize>)>() + 1)
            + self
                .buckets
                .values()
                .map(|v| v.capacity() * size_of::<usize>())
                .sum::<usize>()
            + self.ordered.as_ref().map_or(0, |v| {
                v.capacity() * size_of::<Vec<RowPtr>>()
                    + v.iter()
                        .map(|v| v.capacity() * size_of::<RowPtr>())
                        .sum::<usize>()
            })
            + self.matched.capacity()
            + self.keys.capacity() * size_of::<EquiKey>();
        let bytes = bytes as i64 + self.scratch.memory_usage();
        self.tracker.consume(bytes - self.bytes);
        self.bytes = bytes;
        memory.check()
    }

    pub fn drain<C: Columns>(
        &mut self,
        output: &IndexHashOutput,
        ctx: &C,
        outer: &OuterBatch,
        inner: &List,
        req: &mut Chunk,
    ) -> Result<(), ExecError> {
        if self.ordered.is_some() {
            while self.outer_row < outer.len() && !req.is_full() {
                let at = self.outer_row;
                let row = outer.row(at);
                let ptr = self.ordered.as_ref().expect("ordered variant")[at]
                    .get(self.candidate)
                    .copied();
                if let Some(ptr) = ptr {
                    let inner_row = inner.get_row(ptr);
                    if output.matches(ctx, row, inner_row, self)? {
                        self.matched[at] = true;
                        output.matched(req, row, inner_row);
                        if output.semi() {
                            self.outer_row += 1;
                            self.candidate = 0;
                            continue;
                        }
                    }
                    self.candidate += 1;
                } else {
                    if !self.matched[at] {
                        output.unmatched(req, row);
                    }
                    self.outer_row += 1;
                    self.candidate = 0;
                }
            }
            self.done = self.outer_row == outer.len();
            return Ok(());
        }

        while self.inner_chunk < inner.num_chunks() && !req.is_full() {
            if self.inner_row == inner.num_rows_of_chunk(self.inner_chunk) {
                self.inner_chunk += 1;
                self.inner_row = 0;
                continue;
            }
            let row = inner.get_row(RowPtr::new(self.inner_chunk as u32, self.inner_row as u32));
            let hash = match self.probe_hash {
                Some(hash) => hash,
                None => {
                    let hash = row_hash_chunk(&self.keys, row, output.inner_types(), |key| {
                        if output.outer_is_left {
                            key.right
                        } else {
                            key.left
                        }
                    })
                    .map_err(key_error)?;
                    self.probe_hash = Some(hash);
                    hash
                }
            };
            let at = hash
                .and_then(|hash| self.buckets.get(&hash))
                .and_then(|v| v.get(self.candidate))
                .copied();
            if let Some(at) = at {
                if !(output.semi() && self.matched[at])
                    && output.equal(&self.keys, outer.row(at), row)?
                    && output.matches(ctx, outer.row(at), row, self)?
                {
                    self.matched[at] = true;
                    output.matched(req, outer.row(at), row);
                }
                self.candidate += 1;
            } else {
                self.inner_row += 1;
                self.candidate = 0;
                self.probe_hash = None;
            }
        }
        if self.inner_chunk == inner.num_chunks() {
            while self.outer_row < outer.len() && !req.is_full() {
                if !self.matched[self.outer_row] {
                    output.unmatched(req, outer.row(self.outer_row));
                }
                self.outer_row += 1;
            }
            self.done = self.outer_row == outer.len();
        }
        Ok(())
    }
}

pub(super) struct IndexHashChunk {
    pub chunk: Chunk,
    pub row: usize,
    bytes: i64,
    tracker: Arc<Tracker>,
    recycle: Option<std::sync::mpsc::SyncSender<Chunk>>,
}

impl IndexHashChunk {
    pub fn new(chunk: Chunk, tracker: &Arc<Tracker>) -> Self {
        let bytes = chunk.memory_usage();
        tracker.consume(bytes);
        Self {
            chunk,
            row: 0,
            bytes,
            tracker: Arc::clone(tracker),
            recycle: None,
        }
    }
}

impl Drop for IndexHashChunk {
    fn drop(&mut self) {
        self.tracker.consume(-self.bytes);
        if let Some(sender) = self.recycle.take() {
            let mut chunk = std::mem::replace(&mut self.chunk, Chunk::new_with_capacity(&[], 0));
            chunk.reset();
            let _ = sender.try_send(chunk);
        }
    }
}

/// A bounded result channel transfers each filled chunk instead of retaining
/// an entire task's fanout. Dropping the consumer releases blocked senders.
pub(super) fn send_index_task<C: Columns + Send + Sync + 'static>(
    shared: &Arc<IndexTaskShared<C>>,
    outer: OuterBatch,
    sender: IndexResultSender,
) {
    if shared.hash_output.is_none() || shared.template.is_none() {
        send_prepared_index_task(shared, run_index_task(shared, outer), sender);
        return;
    }
    // Go handleTask overlaps outer hash construction with fetching inner
    // results. CPU work uses the existing pool, never a blocking I/O lane.
    let outer = Arc::new(outer);
    let build_outer = Arc::clone(&outer);
    let build_shared = Arc::clone(shared);
    let build = crate::worker_pool::spawn(move || {
        let result = IndexHashState::build(
            build_shared.hash_output.as_ref().expect("hash variant"),
            &build_shared.keys,
            &build_outer,
            &build_shared.tracker,
            &build_shared.memory,
        );
        drop(build_outer);
        result
    });
    let fetched = fetch_index_task(shared, &outer);
    let built = build.recv().unwrap_or_else(|_| {
        Err(ExecError::internal(
            "index-hash build worker stopped without a result",
        ))
    });
    let outer = Arc::into_inner(outer).expect("build completion releases its outer rows");
    let outcome = fetched_index_task_outcome(outer, fetched);
    send_prepared_with_hash(shared, outcome, sender, Some(built));
}

pub(super) fn send_prepared_index_task<C: Columns>(
    shared: &IndexTaskShared<C>,
    outcome: IndexTaskOutcome,
    sender: IndexResultSender,
) {
    send_prepared_with_hash(shared, outcome, sender, None);
}

fn send_prepared_with_hash<C: Columns>(
    shared: &IndexTaskShared<C>,
    outcome: IndexTaskOutcome,
    sender: IndexResultSender,
    built: Option<Result<IndexHashState, ExecError>>,
) {
    let Some(output) = &shared.hash_output else {
        let _ = sender.send(outcome);
        return;
    };
    let IndexTaskOutcome::Prepared {
        outer,
        inner,
        inner_rows,
    } = outcome
    else {
        let _ = sender.send(outcome);
        return;
    };
    let result = (|| {
        let mut hash = match built {
            Some(built) => built?,
            None => IndexHashState::build(
                output,
                &shared.keys,
                &outer,
                &shared.tracker,
                &shared.memory,
            )?,
        };
        hash.prepare_ordered(output, &outer, &inner.rows, &shared.memory)?;
        let (recycle, recycled) = std::sync::mpsc::sync_channel(1);
        let mut buffer = Chunk::new(
            &output.output_types,
            shared.max_chunk_size,
            shared.max_chunk_size,
        );
        while !hash.done {
            hash.drain(output, &shared.ctx, &outer, &inner.rows, &mut buffer)?;
            if buffer.num_rows() > 0 {
                let mut chunk = IndexHashChunk::new(buffer, &shared.tracker);
                chunk.recycle = Some(recycle.clone());
                shared.memory.check()?;
                if sender.send(IndexTaskOutcome::HashChunk(chunk)).is_err() {
                    return Ok(None);
                }
                buffer = recycled
                    .recv()
                    .map_err(|_| ExecError::internal("index-hash output resource closed"))?;
            }
        }
        Ok::<_, ExecError>(Some(hash.evaluations))
    })();
    match result {
        Ok(Some(evaluations)) => {
            let _ = sender.send(IndexTaskOutcome::HashFinished {
                inner_rows,
                evaluations,
            });
        }
        Ok(None) => {}
        Err(error) => {
            let _ = sender.send(IndexTaskOutcome::Failed {
                outer,
                error: Box::new(error),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go runUnordered consumes resultCh, not the head task's resultCh.
    #[test]
    fn unordered_index_hash_receives_ready_task_without_waiting_for_first() {
        let memory = StatementMemory::default();
        let tracker = memory.operator_tracker(1);
        let mut tasks = IndexUnordered::new(2, &tracker);
        let _first = tasks.admit(100);
        let second = tasks.admit(200);
        assert!(second
            .send(IndexTaskOutcome::HashFinished {
                inner_rows: 7,
                evaluations: 3
            })
            .is_ok());
        let ready = tasks.recv_timeout(std::time::Duration::from_millis(20));
        assert!(
            ready.is_ok(),
            "a ready later task must not wait for the first task"
        );
        let result = ready.unwrap();
        assert_eq!(tasks.active, 1);
        assert_eq!(tracker.bytes_consumed(), 100);
        assert!(matches!(
            result,
            IndexTaskOutcome::HashFinished {
                inner_rows: 7,
                evaluations: 3
            }
        ));
    }

    /// Go worker recovery sends an error to resultCh. Other live senders
    /// must not turn a failed worker into an indefinite wait.
    #[test]
    fn unordered_index_hash_worker_exit_and_close_release_pending_tasks() {
        let memory = StatementMemory::default();
        let tracker = memory.operator_tracker(1);
        let mut tasks = IndexUnordered::new(2, &tracker);
        let failed = tasks.admit(100);
        let pending = tasks.admit(200);
        drop(failed);
        assert!(tasks
            .recv_timeout(std::time::Duration::from_millis(20))
            .is_err());
        assert_eq!(tasks.active, 1);
        assert_eq!(tracker.bytes_consumed(), 200);
        drop(tasks);
        assert_eq!(tracker.bytes_consumed(), 0);
        // Dropping the last worker after its consumer must not block.
        drop(pending);
    }

    #[test]
    fn unordered_index_hash_refused_fork_transfers_outer_charge() {
        let memory = StatementMemory::default();
        let tracker = memory.operator_tracker(1);
        let ty = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let mut outer = OuterBatch::new(&[ty.clone()], 1, 1);
        let mut chunk = Chunk::new_with_capacity(&[ty], 1);
        chunk.append_int64(0, 1);
        outer.push(chunk.get_row(0));
        let bytes = outer.settle_bytes();
        let mut tasks = IndexUnordered::new(1, &tracker);
        let sender = tasks.admit(bytes);
        assert!(sender
            .send(IndexTaskOutcome::Unforked {
                outer,
                probes: Vec::new()
            })
            .is_ok());
        let IndexTaskOutcome::Unforked { outer, .. } = tasks.recv().unwrap() else {
            panic!("expected synchronous ownership");
        };
        drop(tasks);
        assert_eq!(tracker.bytes_consumed(), bytes);
        tracker.consume(-outer.bytes);
        assert_eq!(tracker.bytes_consumed(), 0);
    }

    /// Go getMatchedOuterRows verifies equality after hashing, including
    /// NULL-safe keys, and suppresses duplicate semi-join matches.
    #[test]
    fn index_hash_null_safe_keys_and_hash_collisions() {
        for ordered in [false, true] {
            for null_safe in [false, true] {
                let ty = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                let types = vec![ty.clone()];
                let output = IndexHashOutput {
                    kind: JoinKind::LeftOuterSemi,
                    outer_is_left: true,
                    ordered,
                    output: JoinOutput::all(JoinKind::LeftOuterSemi, 1, 1),
                    conditions: Vec::new(),
                    condition_types: vec![ty.clone(), ty.clone()],
                    left_types: types.clone(),
                    right_types: types.clone(),
                    output_types: vec![ty.clone(), ty],
                };
                let keys = [EquiKey {
                    left: 0,
                    right: 0,
                    class: KeyClass::Int,
                    null_safe,
                }];
                let memory = StatementMemory::default();
                let tracker = memory.operator_tracker(1);
                let mut source = Chunk::new_with_capacity(&types, 4);
                source.append_null(0);
                for key in [1, 1, 9] {
                    source.append_int64(0, key);
                }
                let mut outer = OuterBatch::new(&types, 4, 1024);
                for i in 0..4 {
                    outer.push(source.get_row(i));
                }
                let mut inner = List::new(&types, 4, 1024);
                let mut chunk = Chunk::new_with_capacity(&types, 4);
                chunk.append_null(0);
                for key in [1, 1, 2] {
                    chunk.append_int64(0, key);
                }
                inner.add(chunk);
                let mut state =
                    IndexHashState::build(&output, &keys, &outer, &tracker, &memory).unwrap();
                let hash = row_hash_chunk(&keys, outer.row(1), &types, |k| k.left)
                    .unwrap()
                    .unwrap();
                // A deliberately colliding unequal row must not be matched.
                state.buckets.get_mut(&hash).unwrap().push(3);
                state
                    .prepare_ordered(&output, &outer, &inner, &memory)
                    .unwrap();
                let mut values = Vec::new();
                while !state.done {
                    let mut result = Chunk::new(&output.output_types, 1, 1);
                    state
                        .drain(&output, &tidb_expr::NoColumns, &outer, &inner, &mut result)
                        .unwrap();
                    for i in 0..result.num_rows() {
                        let row = result.get_row(i);
                        values.push((
                            (!row.is_null(0)).then(|| row.get_int64(0)),
                            row.get_int64(1),
                        ));
                    }
                }
                values.sort();
                assert_eq!(
                    values,
                    vec![
                        (None, i64::from(null_safe)),
                        (Some(1), 1),
                        (Some(1), 1),
                        (Some(9), 0)
                    ]
                );
                drop(state);
                assert_eq!(tracker.bytes_consumed(), 0);
            }
        }
    }
}

/// Go's shared resultCh. Task completion, not chunk delivery, releases an
/// admission slot and its outer-row charge.
pub(super) struct IndexUnordered {
    results: std::sync::mpsc::Receiver<(i64, IndexTaskOutcome)>,
    sender: std::sync::mpsc::SyncSender<(i64, IndexTaskOutcome)>,
    pub active: usize,
    bytes: i64,
    tracker: Arc<Tracker>,
}

impl IndexUnordered {
    pub fn new(capacity: usize, tracker: &Arc<Tracker>) -> Self {
        let (sender, results) = std::sync::mpsc::sync_channel(capacity);
        Self {
            results,
            sender,
            active: 0,
            bytes: 0,
            tracker: Arc::clone(tracker),
        }
    }

    pub fn admit(&mut self, bytes: i64) -> IndexResultSender {
        self.active += 1;
        self.bytes += bytes;
        self.tracker.consume(bytes);
        IndexResultSender {
            target: IndexResultTarget::Ready {
                sender: self.sender.clone(),
                bytes,
            },
            terminal: Cell::new(false),
        }
    }

    pub fn finish(&mut self, bytes: i64) {
        debug_assert!(self.active > 0 && self.bytes >= bytes);
        self.active -= 1;
        self.bytes -= bytes;
    }

    pub fn recv(&mut self) -> Result<IndexTaskOutcome, ExecError> {
        let (bytes, result) = self
            .results
            .recv()
            .map_err(|_| ExecError::internal("index-hash result channel closed"))?;
        self.accept(bytes, result)
    }

    #[cfg(test)]
    fn recv_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<IndexTaskOutcome, ExecError> {
        let (bytes, result) = self
            .results
            .recv_timeout(timeout)
            .map_err(|error| ExecError::internal(error.to_string()))?;
        self.accept(bytes, result)
    }

    fn accept(
        &mut self,
        bytes: i64,
        result: IndexTaskOutcome,
    ) -> Result<IndexTaskOutcome, ExecError> {
        if matches!(result, IndexTaskOutcome::HashChunk(_)) {
            return Ok(result);
        }
        self.finish(bytes);
        if let IndexTaskOutcome::Unforked { ref outer, .. } = result {
            // Transfer the existing charge back to the synchronous consumer.
            debug_assert_eq!(outer.bytes, bytes);
        } else {
            self.tracker.consume(-bytes);
        }
        match result {
            IndexTaskOutcome::Failed { error, .. } => Err(*error),
            IndexTaskOutcome::Prepared { .. } => Err(ExecError::internal(
                "unordered hash task returned an ordinary lookup map",
            )),
            _ => Ok(result),
        }
    }
}

impl Drop for IndexUnordered {
    fn drop(&mut self) {
        self.tracker.consume(-self.bytes);
    }
}

enum IndexResultTarget {
    Ordered(std::sync::mpsc::SyncSender<IndexTaskOutcome>),
    Ready {
        sender: std::sync::mpsc::SyncSender<(i64, IndexTaskOutcome)>,
        bytes: i64,
    },
}

pub(super) struct IndexResultSender {
    target: IndexResultTarget,
    terminal: Cell<bool>,
}

impl From<std::sync::mpsc::SyncSender<IndexTaskOutcome>> for IndexResultSender {
    fn from(sender: std::sync::mpsc::SyncSender<IndexTaskOutcome>) -> Self {
        Self {
            target: IndexResultTarget::Ordered(sender),
            terminal: Cell::new(false),
        }
    }
}

impl IndexResultSender {
    pub fn send(
        &self,
        result: IndexTaskOutcome,
    ) -> Result<(), std::sync::mpsc::SendError<IndexTaskOutcome>> {
        if !matches!(&result, IndexTaskOutcome::HashChunk(_)) {
            self.terminal.set(true);
        }
        match &self.target {
            IndexResultTarget::Ordered(sender) => sender.send(result),
            IndexResultTarget::Ready { sender, bytes } => sender
                .send((*bytes, result))
                .map_err(|error| std::sync::mpsc::SendError(error.0 .1)),
        }
    }
}

impl Drop for IndexResultSender {
    fn drop(&mut self) {
        if !self.terminal.get() && matches!(self.target, IndexResultTarget::Ready { .. }) {
            // Other workers keep the shared channel open after a panic. A
            // missing terminal message must not leave Next waiting forever.
            let _ = self.send(IndexTaskOutcome::Failed {
                outer: OuterBatch::new(&[], 1, 1),
                error: Box::new(ExecError::internal(
                    "index-hash worker stopped without completion",
                )),
            });
        }
    }
}
