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

//! Canonical signed partial state shared by every typed `COUNT` evaluator.

use std::collections::BTreeSet;

use tidb_datatype::Datum;

/// Go `partialResult4Count` is one signed 64-bit counter for every input type.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CountState {
    value: i64,
}

/// Source typed-int `COUNT(DISTINCT)` set, colocated with the count runtime.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CountDistinctIntState {
    values: BTreeSet<i64>,
}

impl CountDistinctIntState {
    /// Creates an empty exact-integer DISTINCT set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            values: BTreeSet::new(),
        }
    }
    /// Inserts every non-NULL integer into the exact DISTINCT set.
    pub fn update(&mut self, values: &[Option<i64>]) {
        self.values.extend(values.iter().flatten().copied());
    }
    /// Unions another exact DISTINCT set into this state.
    pub fn merge_from(&mut self, source: &Self) {
        self.values.extend(source.values.iter().copied());
    }
    /// Drops all remembered values without replacing the state owner.
    pub fn reset(&mut self) {
        self.values.clear();
    }
    /// Returns the number of distinct non-NULL integers seen.
    #[must_use]
    pub fn result(&self) -> i64 {
        self.values.len() as i64
    }
    /// Returns the current exact-set cardinality as a collection size.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }
    /// Reports whether the exact DISTINCT set contains no values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    /// Returns the fixed structural width, excluding set allocations.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

impl CountState {
    /// Creates the source zero state.
    #[must_use]
    pub const fn new() -> Self {
        Self { value: 0 }
    }

    /// Resets the partial result.
    pub fn reset(&mut self) {
        self.value = 0;
    }

    /// Adds one for a non-NULL original value.
    pub fn update(&mut self, value: &Datum) {
        if !value.is_null() {
            self.value = self.value.wrapping_add(1);
        }
    }

    /// Adds a count produced by a partial executor.
    pub fn add_partial(&mut self, value: i64) {
        self.value = self.value.wrapping_add(value);
    }

    /// Merges a source partial result into this destination.
    pub fn merge_from(&mut self, source: &Self) {
        self.add_partial(source.value);
    }

    /// Slides by removing outgoing values before adding incoming values.
    pub fn slide(&mut self, outgoing: &[Datum], incoming: &[Datum]) {
        for value in outgoing {
            if !value.is_null() {
                self.value = self.value.wrapping_sub(1);
            }
        }
        for value in incoming {
            self.update(value);
        }
    }

    /// Returns the signed result.
    #[must_use]
    pub const fn result(self) -> i64 {
        self.value
    }

    /// Returns Go's fixed partial-result width.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        std::mem::size_of::<i64>()
    }
}
