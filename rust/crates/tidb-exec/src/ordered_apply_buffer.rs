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

//! Dependency-closed ordered-result buffering from
//! `pkg/executor/parallel_apply.go`.
//!
//! Ordered parallel Apply workers may finish outer rows out of order. The Go
//! reorder worker buffers those results by sequence, drains only consecutive
//! rows, batches them at chunk capacity, and terminates immediately on worker
//! error or cancellation. This leaf keeps that state machine generic over row
//! values. Executor/chunk/channel/context lifecycle, correlated evaluation,
//! joiner mismatch handling, and runtime error construction remain external.

use std::collections::BTreeMap;

/// One ordered worker result. An error result terminates the reorder stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedApplyResult<T> {
    /// Outer-row sequence number.
    pub sequence: u64,
    /// Rows produced for this outer row; empty is a valid result.
    pub rows: Vec<T>,
    /// Worker failure, if any.
    pub error: Option<String>,
}

impl<T> OrderedApplyResult<T> {
    /// Creates a successful result, including a valid empty result.
    #[must_use]
    pub fn success(sequence: u64, rows: Vec<T>) -> Self {
        Self {
            sequence,
            rows,
            error: None,
        }
    }

    /// Creates a terminal worker-error result.
    #[must_use]
    pub fn failure(sequence: u64, error: impl Into<String>) -> Self {
        Self {
            sequence,
            rows: Vec::new(),
            error: Some(error.into()),
        }
    }
}

/// Terminal states reported by the ordered buffer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReorderError {
    /// A worker reported an execution error.
    Worker(String),
    /// The executor was cancelled before the ordered stream completed.
    Cancelled,
    /// A result was submitted after terminal completion.
    Finished,
    /// A zero-sized output batch is not a valid chunk capacity.
    InvalidBatchCapacity,
}

/// Ordered sequence buffer with source-shaped batch flushing.
#[derive(Debug)]
pub struct OrderedApplyBuffer<T> {
    pending: BTreeMap<u64, Vec<T>>,
    next_sequence: u64,
    batch_capacity: usize,
    current_batch: Vec<T>,
    finished: bool,
    cancelled: bool,
}

impl<T> OrderedApplyBuffer<T> {
    /// Creates a reorder buffer. Capacity matches the output chunk limit.
    pub fn new(batch_capacity: usize) -> Result<Self, ReorderError> {
        if batch_capacity == 0 {
            return Err(ReorderError::InvalidBatchCapacity);
        }
        Ok(Self {
            pending: BTreeMap::new(),
            next_sequence: 0,
            batch_capacity,
            current_batch: Vec::with_capacity(batch_capacity),
            finished: false,
            cancelled: false,
        })
    }

    /// Buffers one result and emits any newly completed full batches.
    pub fn push(&mut self, result: OrderedApplyResult<T>) -> Result<Vec<Vec<T>>, ReorderError> {
        self.ensure_active()?;
        if let Some(error) = result.error {
            self.finished = true;
            return Err(ReorderError::Worker(error));
        }
        // Source pending-map insertion replaces a duplicate sequence. The
        // producer contract normally guarantees unique sequence numbers.
        self.pending.insert(result.sequence, result.rows);
        self.drain_ready()
    }

    /// Flushes the final partial batch and marks the stream complete.
    pub fn finish(&mut self) -> Result<Vec<Vec<T>>, ReorderError> {
        self.ensure_active()?;
        self.finished = true;
        let mut batches = self.drain_ready()?;
        if !self.current_batch.is_empty() {
            batches.push(std::mem::take(&mut self.current_batch));
        }
        // A closed source channel is expected only after every sequence has
        // arrived; retain no stale out-of-order rows after terminal EOF.
        self.pending.clear();
        Ok(batches)
    }

    /// Flushes a non-empty partial batch when the source channel has no
    /// immediately available result. This models the reorder worker's idle
    /// `default` branch while retaining full-batch opportunistic draining.
    pub fn flush_idle(&mut self) -> Result<Option<Vec<T>>, ReorderError> {
        self.ensure_active()?;
        if self.current_batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(std::mem::take(&mut self.current_batch)))
        }
    }

    /// Cancels the stream without flushing pending or partial rows.
    pub fn cancel(&mut self) {
        self.cancelled = true;
        self.pending.clear();
        self.current_batch.clear();
    }

    /// Returns the next sequence that can be emitted.
    #[must_use]
    pub const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Returns the number of out-of-order results waiting for a gap.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Returns whether the stream has reached EOF or worker error.
    #[must_use]
    pub const fn is_finished(&self) -> bool {
        self.finished
    }

    fn ensure_active(&self) -> Result<(), ReorderError> {
        if self.cancelled {
            Err(ReorderError::Cancelled)
        } else if self.finished {
            Err(ReorderError::Finished)
        } else {
            Ok(())
        }
    }

    fn drain_ready(&mut self) -> Result<Vec<Vec<T>>, ReorderError> {
        let mut batches = Vec::new();
        while let Some(rows) = self.pending.remove(&self.next_sequence) {
            self.next_sequence += 1;
            for row in rows {
                self.current_batch.push(row);
                if self.current_batch.len() == self.batch_capacity {
                    batches.push(std::mem::take(&mut self.current_batch));
                    self.current_batch = Vec::with_capacity(self.batch_capacity);
                }
            }
        }
        Ok(batches)
    }
}
