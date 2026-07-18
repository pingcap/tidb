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

use super::GroupConcatState;

/// DISTINCT partial state keyed by caller-provided collation keys.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistinctGroupConcatState {
    values: Vec<(Vec<u8>, Vec<u8>)>,
    output: GroupConcatState,
}

impl DistinctGroupConcatState {
    /// Creates an empty DISTINCT state.
    #[must_use]
    pub fn new(separator: impl AsRef<[u8]>, max_len: u64) -> Self {
        Self {
            values: Vec::new(),
            output: GroupConcatState::new(separator, max_len),
        }
    }

    /// Inserts one encoded tuple and rendered value, retaining its first key.
    pub fn update(&mut self, encoded_key: &[u8], rendered: &[u8]) -> bool {
        if self.values.iter().any(|(key, _)| key == encoded_key) {
            return false;
        }
        self.values.push((encoded_key.to_vec(), rendered.to_vec()));
        false
    }

    /// Merges unseen source keys.
    pub fn merge_from(&mut self, source: &Self) {
        for (key, value) in &source.values {
            self.update(key, value);
        }
    }

    /// Finalizes in retained insertion order and returns first truncation.
    ///
    /// Go ranges over a map, so unordered DISTINCT output has no promised
    /// order. Retaining insertion order is deterministic but is not exposed as
    /// a source ordering guarantee; callers and tests must compare membership.
    pub fn finalize(&mut self) -> bool {
        self.output.reset();
        self.output
            .update_distinct_values(self.values.iter().map(|(_, value)| value.as_slice()))
    }

    /// Returns finalized bytes.
    #[must_use]
    pub fn finish(&self) -> Option<&[u8]> {
        self.output.finish()
    }

    /// Resets keys and output while retaining the truncation sentinel.
    pub fn reset(&mut self) {
        self.values.clear();
        self.output.reset();
    }
}
