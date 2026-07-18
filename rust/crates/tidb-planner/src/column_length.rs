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

//! Index-column length comparison from `pkg/planner/util/path.go`.
//!
//! This leaf ports the dependency-closed `Col2Len` dominance and comparison
//! rules. Expression extraction, index metadata, and ranger/session context
//! remain external planner boundaries.

use std::collections::BTreeMap;

/// Source sentinel for an unspecified (full) column length.
pub const UNSPECIFIED_LENGTH: i64 = -1;

/// Maps a stable column identity to its index-prefix length.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Col2Len(BTreeMap<i64, i64>);

impl Col2Len {
    /// Creates a column-length map from source `(column_id, length)` pairs.
    #[must_use]
    pub fn from_pairs(pairs: impl IntoIterator<Item = (i64, i64)>) -> Self {
        Self(pairs.into_iter().collect())
    }

    /// Returns the number of tracked columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether no columns are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

fn compare_length(left: i64, right: i64) -> i8 {
    if left == right {
        return 0;
    }
    if left == UNSPECIFIED_LENGTH {
        return 1;
    }
    if right == UNSPECIFIED_LENGTH || left < right {
        return -1;
    }
    1
}

fn dominates(left: &Col2Len, right: &Col2Len) -> bool {
    if right.len() > left.len() {
        return false;
    }
    right.0.iter().all(|(&column, &right_len)| {
        left.0
            .get(&column)
            .is_some_and(|&left_len| compare_length(right_len, left_len) != 1)
    })
}

/// Compares two column-length maps and reports source comparability.
///
/// The first result is `-1`, `0`, or `1` for the source ordering. The second
/// result is false when the maps are incomparable and another planner
/// criterion must decide the winner.
#[must_use]
pub fn compare_col2_len(left: &Col2Len, right: &Col2Len) -> (i8, bool) {
    match left.len().cmp(&right.len()) {
        std::cmp::Ordering::Greater => (1, dominates(left, right)),
        std::cmp::Ordering::Less => (-1, dominates(right, left)),
        std::cmp::Ordering::Equal => {
            for (&column, &right_len) in &right.0 {
                let Some(&left_len) = left.0.get(&column) else {
                    return (0, false);
                };
                if left_len != right_len {
                    return (if left_len > right_len { 1 } else { -1 }, false);
                }
            }
            (0, true)
        }
    }
}
