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

//! Monotonic max/min deque helper from
//! `pkg/executor/aggfuncs/func_max_min.go`.
//!
//! The Go helper stores `(item,index)` pairs, supports double-ended pushes and
//! pops, removes expired front indices, and maintains a monotonic queue for
//! max/min sliding windows. This leaf ports that dependency-closed helper for
//! a caller-provided comparator. Typed MAX/MIN partial states, evaluator
//! coercion, window callbacks, chunk output, memory accounting, and spill
//! serialization remain external.

use std::cmp::Ordering;
use std::fmt;

use crate::ExecError;

/// A value and its absolute source-row index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pair<T> {
    /// Stored aggregate value.
    pub item: T,
    /// Source-row position used by sliding-window expiry.
    pub idx: u64,
}

/// Errors returned by invalid direct pop operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DequeError {
    /// `PopFront` was requested for an empty deque.
    EmptyFront,
    /// `PopBack` was requested for an empty deque.
    EmptyBack,
}

impl fmt::Display for DequeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyFront => f.write_str("Pop front when MinMaxDeque is empty"),
            Self::EmptyBack => f.write_str("Pop back when MinMaxDeque is empty"),
        }
    }
}

/// Array-backed monotonic double-ended queue for max/min sliding windows.
pub struct MinMaxDeque<T> {
    items: Vec<Pair<T>>,
    is_max: bool,
    compare: fn(&T, &T) -> Ordering,
}

impl<T> fmt::Debug for MinMaxDeque<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MinMaxDeque")
            .field("items", &self.items)
            .field("is_max", &self.is_max)
            .finish_non_exhaustive()
    }
}

impl<T> MinMaxDeque<T> {
    /// Creates an empty deque with max or min mode and a source comparator.
    #[must_use]
    pub const fn new(is_max: bool, compare: fn(&T, &T) -> Ordering) -> Self {
        Self {
            items: Vec::new(),
            is_max,
            compare,
        }
    }

    /// Appends a pair without monotonic filtering.
    pub fn push_back(&mut self, idx: u64, item: T) {
        self.items.push(Pair { item, idx });
    }

    /// Removes the front pair.
    pub fn pop_front(&mut self) -> Result<(), DequeError> {
        if self.items.is_empty() {
            return Err(DequeError::EmptyFront);
        }
        self.items.remove(0);
        Ok(())
    }

    /// Removes the back pair.
    pub fn pop_back(&mut self) -> Result<(), DequeError> {
        if self.items.pop().is_none() {
            return Err(DequeError::EmptyBack);
        }
        Ok(())
    }

    /// Returns the back pair, if any.
    #[must_use]
    pub fn back(&self) -> Option<&Pair<T>> {
        self.items.last()
    }

    /// Returns the front pair, if any.
    #[must_use]
    pub fn front(&self) -> Option<&Pair<T>> {
        self.items.first()
    }

    /// Returns whether the deque contains no pairs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns whether this deque tracks a maximum.
    #[must_use]
    pub const fn is_max(&self) -> bool {
        self.is_max
    }

    /// Returns the current number of pairs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Clears all pairs while retaining the allocation.
    pub fn reset(&mut self) {
        self.items.clear();
    }

    /// Removes front pairs whose indices are at or before `boundary`.
    pub fn dequeue(&mut self, boundary: u64) -> Result<(), DequeError> {
        while let Some(pair) = self.front() {
            if pair.idx > boundary {
                break;
            }
            self.pop_front()?;
        }
        Ok(())
    }

    /// Adds a pair while preserving the source monotonic max/min invariant.
    /// Equal values evict the older back pair, matching Go's `>=`/`<=` test.
    pub fn enqueue(&mut self, idx: u64, item: T) -> Result<(), DequeError> {
        while let Some(back) = self.back() {
            let ordering = (self.compare)(&item, &back.item);
            let evict = if self.is_max {
                ordering != Ordering::Less
            } else {
                ordering != Ordering::Greater
            };
            if !evict {
                break;
            }
            self.pop_back()?;
        }
        self.push_back(idx, item);
        Ok(())
    }
}

impl<T> MinMaxDeque<T>
where
    T: Clone,
{
    /// Returns a cloned snapshot for source-backed assertions.
    #[must_use]
    pub fn items(&self) -> Vec<Pair<T>> {
        self.items.clone()
    }
}

/// A value and all source-row positions tied at that value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CountPair<T> {
    /// Stored aggregate value.
    pub item: T,
    /// Source-row positions that compare equal to `item`.
    pub idxs: Vec<u64>,
}

/// Count-aware monotonic deque used by Go's MAX_COUNT/MIN_COUNT sliding
/// evaluators. Unlike [`MinMaxDeque`], equal values share one item and retain
/// every source index so expiry can decrement the tie count without losing
/// the remaining equal rows.
pub struct MinMaxCountDeque<T> {
    items: Vec<CountPair<T>>,
    is_max: bool,
    compare: fn(&T, &T) -> Result<Ordering, ExecError>,
}

impl<T> fmt::Debug for MinMaxCountDeque<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MinMaxCountDeque")
            .field("items", &self.items)
            .field("is_max", &self.is_max)
            .finish_non_exhaustive()
    }
}

impl<T> MinMaxCountDeque<T> {
    /// Creates an empty count-aware deque with a fallible source comparator.
    #[must_use]
    pub const fn new(is_max: bool, compare: fn(&T, &T) -> Result<Ordering, ExecError>) -> Self {
        Self {
            items: Vec::new(),
            is_max,
            compare,
        }
    }

    /// Returns whether this deque tracks a maximum.
    #[must_use]
    pub const fn is_max(&self) -> bool {
        self.is_max
    }

    /// Returns whether the deque contains no values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Clears all values while retaining the allocation.
    pub fn reset(&mut self) {
        self.items.clear();
    }

    /// Returns the front value and its live source positions.
    #[must_use]
    pub fn front(&self) -> Option<&CountPair<T>> {
        self.items.first()
    }

    /// Returns the number of live rows tied at the current extreme.
    #[must_use]
    pub fn front_count(&self) -> usize {
        self.front().map_or(0, |pair| pair.idxs.len())
    }

    /// Returns a cloned snapshot for source-shaped assertions.
    #[must_use]
    pub fn items(&self) -> Vec<CountPair<T>>
    where
        T: Clone,
    {
        self.items.clone()
    }

    /// Adds one value, retaining all equal source positions in one group.
    pub fn enqueue(&mut self, idx: u64, item: T) -> Result<(), ExecError> {
        loop {
            let Some(back) = self.items.last_mut() else {
                self.items.push(CountPair {
                    item,
                    idxs: vec![idx],
                });
                return Ok(());
            };
            let ordering = (self.compare)(&item, &back.item)?;
            if ordering == Ordering::Equal {
                back.idxs.push(idx);
                return Ok(());
            }
            let evict = if self.is_max {
                ordering == Ordering::Greater
            } else {
                ordering == Ordering::Less
            };
            if evict {
                self.items.pop();
                continue;
            }
            self.items.push(CountPair {
                item,
                idxs: vec![idx],
            });
            return Ok(());
        }
    }

    /// Removes every source position at or before `boundary` from the front.
    pub fn dequeue(&mut self, boundary: u64) {
        while let Some(front) = self.items.first_mut() {
            let first_alive = front
                .idxs
                .iter()
                .position(|idx| *idx > boundary)
                .unwrap_or(front.idxs.len());
            if first_alive == 0 {
                return;
            }
            front.idxs.drain(..first_alive);
            if !front.idxs.is_empty() {
                return;
            }
            self.items.remove(0);
        }
    }
}
