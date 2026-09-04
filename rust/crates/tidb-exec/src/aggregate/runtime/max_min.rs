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

//! Canonical nullable MAX/MIN partial state.

use std::cmp::Ordering;

use tidb_datatype::Datum;
use tidb_planner::aggregation_descriptor::AggregateKind;

use super::super::value_cmp;
use crate::minmax_deque::MinMaxCountDeque;
use crate::ExecError;

/// One nullable partial state shared by MAX and MIN update, merge, and result.
#[derive(Clone, Debug, PartialEq)]
pub struct MaxMinState {
    kind: AggregateKind,
    value: Option<Datum>,
}

/// Nullable MAX_COUNT/MIN_COUNT partial state.
///
/// Go's pair evaluator tracks the winning value and the number of rows tied
/// at that value. NULL arguments are ignored; an empty or all-NULL group
/// returns the count-shaped zero value.
#[derive(Clone, Debug, PartialEq)]
pub struct MaxMinCountState {
    kind: AggregateKind,
    value: Option<Datum>,
    count: i64,
}

/// Sliding MAX_COUNT/MIN_COUNT state from Go's
/// `partialResult4MaxMinCountSliding`.
///
/// The caller supplies already-evaluated values and absolute source-row
/// indices. `update` builds the initial frame; `slide` follows Go's order by
/// enqueueing incoming rows before expiring the outgoing boundary. NULLs are
/// ignored, and an empty/all-NULL frame returns the count-shaped zero.
#[derive(Debug)]
pub struct MaxMinCountSlidingState {
    kind: AggregateKind,
    deque: MinMaxCountDeque<Datum>,
    count: i64,
    is_null: bool,
}

impl PartialEq for MaxMinCountSlidingState {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.count == other.count
            && self.is_null == other.is_null
            && self.deque.items() == other.deque.items()
    }
}

fn compare_sliding_datums(left: &Datum, right: &Datum) -> Result<Ordering, ExecError> {
    value_cmp(left, right)
}

impl MaxMinCountSlidingState {
    /// Creates an empty sliding state for MAX_COUNT or MIN_COUNT.
    #[must_use]
    pub fn new(kind: AggregateKind) -> Option<Self> {
        matches!(kind, AggregateKind::MaxCount | AggregateKind::MinCount).then_some(Self {
            kind,
            deque: MinMaxCountDeque::new(kind == AggregateKind::MaxCount, compare_sliding_datums),
            count: 0,
            is_null: true,
        })
    }

    /// Adds values to the current frame, using absolute source indices that
    /// start at `start`.
    pub fn update(&mut self, start: u64, values: &[Datum]) -> Result<(), ExecError> {
        for (offset, value) in values.iter().enumerate() {
            if value.is_null() {
                continue;
            }
            self.deque
                .enqueue(start.wrapping_add(offset as u64), value.clone())?;
        }
        self.refresh();
        Ok(())
    }

    /// Advances the frame by enqueueing incoming values and then expiring all
    /// rows at or before `boundary`, matching Go's `Slide` implementation.
    pub fn slide(
        &mut self,
        incoming_start: u64,
        incoming: &[Datum],
        boundary: Option<u64>,
    ) -> Result<(), ExecError> {
        for (offset, value) in incoming.iter().enumerate() {
            if value.is_null() {
                continue;
            }
            self.deque
                .enqueue(incoming_start.wrapping_add(offset as u64), value.clone())?;
        }
        if let Some(boundary) = boundary {
            self.deque.dequeue(boundary);
        }
        self.refresh();
        Ok(())
    }

    /// Resets the state to an empty frame.
    pub fn reset(&mut self) {
        self.deque.reset();
        self.count = 0;
        self.is_null = true;
    }

    /// Returns the count tied at the current extreme, or zero for an empty
    /// frame.
    #[must_use]
    pub const fn result(&self) -> i64 {
        self.count
    }

    /// Returns whether the current frame has no non-NULL values.
    #[must_use]
    pub const fn is_null(&self) -> bool {
        self.is_null
    }

    /// Returns the selected MAX_COUNT/MIN_COUNT direction.
    #[must_use]
    pub const fn kind(&self) -> AggregateKind {
        self.kind
    }

    fn refresh(&mut self) {
        self.count = self.deque.front_count() as i64;
        self.is_null = self.deque.is_empty();
    }
}

impl MaxMinCountState {
    /// Creates an empty state for MAX_COUNT or MIN_COUNT.
    #[must_use]
    pub fn new(kind: AggregateKind) -> Option<Self> {
        matches!(kind, AggregateKind::MaxCount | AggregateKind::MinCount).then_some(Self {
            kind,
            value: None,
            count: 0,
        })
    }

    /// Updates the winner and tie count with one input value.
    pub fn update(&mut self, value: &Datum) -> Result<(), ExecError> {
        if value.is_null() {
            return Ok(());
        }
        let Some(current) = &self.value else {
            value_cmp(value, value)?;
            self.value = Some(value.clone());
            self.count = 1;
            return Ok(());
        };
        match value_cmp(value, current)? {
            ordering if ordering == self.wanted_ordering() => {
                self.value = Some(value.clone());
                self.count = 1;
            }
            std::cmp::Ordering::Equal => {
                self.count += 1;
            }
            _ => {}
        }
        Ok(())
    }

    /// Merges a partial winner/count pair into this state.
    pub fn merge_from(&mut self, source: &Self) -> Result<(), ExecError> {
        if self.kind != source.kind {
            return Err(ExecError::Unsupported(
                "MAX_COUNT/MIN_COUNT aggregate kind mismatch",
            ));
        }
        let Some(source_value) = &source.value else {
            return Ok(());
        };
        let Some(current) = &self.value else {
            self.value = Some(source_value.clone());
            self.count = source.count;
            return Ok(());
        };
        match value_cmp(source_value, current)? {
            ordering if ordering == self.wanted_ordering() => {
                self.value = Some(source_value.clone());
                self.count = source.count;
            }
            std::cmp::Ordering::Equal => {
                self.count += source.count;
            }
            _ => {}
        }
        Ok(())
    }

    /// Resets the state to an empty group.
    pub fn reset(&mut self) {
        self.value = None;
        self.count = 0;
    }

    /// Returns the count of rows tied at the selected extreme.
    #[must_use]
    pub const fn result(&self) -> i64 {
        self.count
    }

    fn wanted_ordering(&self) -> std::cmp::Ordering {
        match self.kind {
            AggregateKind::MaxCount => std::cmp::Ordering::Greater,
            AggregateKind::MinCount => std::cmp::Ordering::Less,
            _ => unreachable!("constructor accepts only MAX_COUNT/MIN_COUNT"),
        }
    }
}

impl MaxMinState {
    /// Creates an empty state for MAX or MIN, rejecting unrelated kinds.
    #[must_use]
    pub fn new(kind: AggregateKind) -> Option<Self> {
        matches!(kind, AggregateKind::Max | AggregateKind::Min)
            .then_some(Self { kind, value: None })
    }

    /// Ignores NULL and retains the first value unless a strict winner arrives.
    pub fn update(&mut self, value: &Datum) -> Result<(), ExecError> {
        if value.is_null() {
            return Ok(());
        }
        let Some(current) = &self.value else {
            // Validate even the first value. Otherwise a range sentinel or
            // unordered real would become the result without ever reaching
            // the comparator.
            value_cmp(value, value)?;
            self.value = Some(value.clone());
            return Ok(());
        };
        if value_cmp(value, current)? == self.wanted_ordering() {
            self.value = Some(value.clone());
        }
        Ok(())
    }

    /// Merges a source partial into this destination using the same strict rule.
    pub fn merge_from(&mut self, source: &Self) -> Result<(), ExecError> {
        if self.kind != source.kind {
            return Err(ExecError::Unsupported("MAX/MIN aggregate kind mismatch"));
        }
        if let Some(value) = &source.value {
            self.update(value)?;
        }
        Ok(())
    }

    /// Resets the partial to SQL NULL without changing its aggregate kind.
    pub fn reset(&mut self) {
        self.value = None;
    }

    /// Returns SQL NULL for an empty/all-NULL input, otherwise the retained value.
    #[must_use]
    pub fn result(&self) -> Datum {
        self.value.clone().unwrap_or(Datum::Null)
    }

    fn wanted_ordering(&self) -> Ordering {
        match self.kind {
            AggregateKind::Max => Ordering::Greater,
            AggregateKind::Min => Ordering::Less,
            _ => unreachable!("constructor accepts only MAX/MIN"),
        }
    }
}
