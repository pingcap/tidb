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

use std::cmp::Ordering;

use super::GroupConcatState;

#[derive(Clone, Debug, Eq, PartialEq)]
struct OrderedRow {
    keys: Vec<Vec<u8>>,
    rendered: Vec<u8>,
}

/// Source-shaped ordered state using caller-resolved immutable collation keys.
///
/// Unlike final-prefix truncation, Go's `topNRows.tryToAdd` bounds the state
/// during every update: it evicts the current worst row, truncates that row
/// when only part of it is over budget, and remembers when a removed row's
/// separator must be represented by a partial separator in the final result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedGroupConcatState {
    rows: Vec<OrderedRow>,
    descending: Vec<bool>,
    separator: Vec<u8>,
    max_len: u64,
    current_size: u64,
    separator_truncated: bool,
    truncated: bool,
    distinct_keys: Option<Vec<Vec<u8>>>,
    output: GroupConcatState,
}

impl OrderedGroupConcatState {
    /// Creates an empty ordered state.
    #[must_use]
    pub fn new(separator: impl AsRef<[u8]>, max_len: u64, descending: Vec<bool>) -> Self {
        Self::with_distinct(separator, max_len, descending, false)
    }

    /// Creates an ordered DISTINCT state.
    #[must_use]
    pub fn new_distinct(separator: impl AsRef<[u8]>, max_len: u64, descending: Vec<bool>) -> Self {
        Self::with_distinct(separator, max_len, descending, true)
    }

    fn with_distinct(
        separator: impl AsRef<[u8]>,
        max_len: u64,
        descending: Vec<bool>,
        distinct: bool,
    ) -> Self {
        let separator = separator.as_ref().to_vec();
        Self {
            rows: Vec::new(),
            descending,
            output: GroupConcatState::new(&separator, 0),
            separator,
            max_len,
            current_size: 0,
            separator_truncated: false,
            truncated: false,
            distinct_keys: distinct.then(Vec::new),
        }
    }

    /// Adds one rendered row and reports the aggregate's first truncation.
    pub fn update(&mut self, keys: Vec<Vec<u8>>, rendered: Vec<u8>) -> bool {
        self.update_inner(keys, rendered)
    }

    /// Adds one rendered row only when its encoded argument tuple is unseen.
    pub fn update_distinct(
        &mut self,
        distinct_key: Vec<u8>,
        keys: Vec<Vec<u8>>,
        rendered: Vec<u8>,
    ) -> bool {
        let Some(seen) = self.distinct_keys.as_mut() else {
            return self.update(keys, rendered);
        };
        if seen.iter().any(|key| key == &distinct_key) {
            return false;
        }
        seen.push(distinct_key);
        self.update_inner(keys, rendered)
    }

    fn update_inner(&mut self, keys: Vec<Vec<u8>>, rendered: Vec<u8>) -> bool {
        self.current_size = self.current_size.saturating_add(rendered.len() as u64);
        if !self.rows.is_empty() {
            self.current_size = self
                .current_size
                .saturating_add(self.separator.len() as u64);
        }
        self.rows.push(OrderedRow { keys, rendered });
        if self.max_len == 0 || self.current_size <= self.max_len {
            return false;
        }

        while self.current_size > self.max_len {
            let debt = self.current_size - self.max_len;
            let worst = self
                .rows
                .iter()
                .enumerate()
                .max_by(|(_, left), (_, right)| self.compare(left, right))
                .map(|(index, _)| index)
                .expect("the just-inserted row makes the top-N non-empty");
            if self.rows[worst].rendered.len() as u64 > debt {
                let keep = self.rows[worst].rendered.len() - debt as usize;
                self.rows[worst].rendered.truncate(keep);
                self.current_size -= debt;
            } else {
                let removed = self.rows.remove(worst);
                self.current_size = self
                    .current_size
                    .saturating_sub(removed.rendered.len() as u64 + self.separator.len() as u64);
                self.separator_truncated = true;
            }
        }
        let first = !self.truncated;
        self.truncated = true;
        first
    }

    /// Sorts retained top-N rows, builds final bytes, and reports truncation.
    pub fn finalize(&mut self) -> bool {
        let descending = &self.descending;
        self.rows
            .sort_unstable_by(|left, right| compare(left, right, descending));
        let mut result = Vec::with_capacity(self.current_size as usize);
        for (index, row) in self.rows.iter().enumerate() {
            if index != 0 {
                result.extend_from_slice(&self.separator);
            }
            result.extend_from_slice(&row.rendered);
        }
        if self.separator_truncated {
            result.extend_from_slice(&self.separator);
            if self.max_len > 0 && result.len() as u64 > self.max_len {
                result.truncate(usize::try_from(self.max_len).unwrap_or(usize::MAX));
            }
        }
        self.output
            .restore_buffer((!self.rows.is_empty()).then_some(result));
        self.truncated
    }

    /// Returns finalized bytes.
    #[must_use]
    pub fn finish(&self) -> Option<&[u8]> {
        self.output.finish()
    }

    /// Reports whether this aggregate state has ever crossed its bound.
    #[must_use]
    pub const fn was_truncated(&self) -> bool {
        self.truncated
    }

    /// Resets group rows while retaining Go's aggregate-lifetime sentinel.
    pub fn reset(&mut self) {
        self.rows.clear();
        self.current_size = 0;
        self.output.reset();
        if let Some(keys) = &mut self.distinct_keys {
            keys.clear();
        }
        // Go topNRows.reset does not clear isSepTruncated. Preserve that
        // observable source state, as well as the aggregate warning sentinel.
    }

    /// Rejects merge structurally: Go forbids parallel ordered GROUP_CONCAT.
    pub fn merge_from(&mut self, _: &Self) -> Result<(), &'static str> {
        Err("groupConcatOrder.MergePartialResult should not be called")
    }

    fn compare(&self, left: &OrderedRow, right: &OrderedRow) -> Ordering {
        compare(left, right, &self.descending)
    }
}

fn compare(left: &OrderedRow, right: &OrderedRow, descending: &[bool]) -> Ordering {
    for (index, (left, right)) in left.keys.iter().zip(&right.keys).enumerate() {
        let order = left.cmp(right);
        if !order.is_eq() {
            return if descending.get(index).copied().unwrap_or(false) {
                order.reverse()
            } else {
                order
            };
        }
    }
    left.keys.len().cmp(&right.keys.len())
}
