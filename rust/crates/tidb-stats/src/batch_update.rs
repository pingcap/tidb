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

//! Statistics-cache batch update state from
//! `pkg/statistics/handle/cache/statscache.go`.
//!
//! The source batches updates and deletes independently, flushing both lists
//! whenever either reaches the configured size. This leaf keeps that state
//! transition generic: callers own the table/value type and the operation that
//! consumes each flushed batch.

/// A source-shaped batch of caller-owned updates and table-ID deletions.
pub struct BatchUpdate<T, F> {
    operation: F,
    updates: Vec<T>,
    deletes: Vec<i64>,
    batch_size: usize,
}

impl<T, F> BatchUpdate<T, F>
where
    F: FnMut(&[T], &[i64]),
{
    /// Creates an empty batch with the source capacity and flush callback.
    #[must_use]
    pub fn new(batch_size: usize, operation: F) -> Self {
        Self {
            operation,
            updates: Vec::with_capacity(batch_size),
            deletes: Vec::with_capacity(batch_size),
            batch_size,
        }
    }

    /// Adds one update, flushing before append when the update list is full.
    pub fn add_update(&mut self, update: T) {
        if self.updates.len() == self.batch_size {
            self.internal_flush();
        }
        self.updates.push(update);
    }

    /// Adds one deletion, flushing before append when the delete list is full.
    pub fn add_delete(&mut self, table_id: i64) {
        if self.deletes.len() == self.batch_size {
            self.internal_flush();
        }
        self.deletes.push(table_id);
    }

    /// Flushes a nonempty batch and leaves both pending lists empty.
    pub fn flush(&mut self) {
        if !self.updates.is_empty() || !self.deletes.is_empty() {
            self.internal_flush();
        }
    }

    /// Returns the updates waiting for the next flush.
    #[must_use]
    pub fn pending_updates(&self) -> &[T] {
        &self.updates
    }

    /// Returns the table IDs waiting for the next flush.
    #[must_use]
    pub fn pending_deletes(&self) -> &[i64] {
        &self.deletes
    }

    fn internal_flush(&mut self) {
        (self.operation)(&self.updates, &self.deletes);
        self.updates.clear();
        self.deletes.clear();
    }
}
