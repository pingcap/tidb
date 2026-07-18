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

use std::mem::size_of;

/// Source-shaped non-DISTINCT `GROUP_CONCAT` partial state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GroupConcatState {
    separator: Vec<u8>,
    max_len: u64,
    buffer: Option<Vec<u8>>,
    truncated: bool,
}

impl GroupConcatState {
    /// Creates an empty state with the source separator and maximum byte length.
    #[must_use]
    pub fn new(separator: impl AsRef<[u8]>, max_len: u64) -> Self {
        Self {
            separator: separator.as_ref().to_vec(),
            max_len,
            buffer: None,
            truncated: false,
        }
    }

    /// Appends already-evaluated rows; NULL rows are skipped.
    pub fn update(&mut self, values: &[Option<&str>]) -> bool {
        self.update_bytes(
            &values
                .iter()
                .map(|value| value.map(str::as_bytes))
                .collect::<Vec<_>>(),
        )
    }

    /// Appends raw byte rows and reports the first truncation transition.
    pub fn update_bytes(&mut self, values: &[Option<&[u8]>]) -> bool {
        let mut newly_truncated = false;
        for value in values.iter().flatten() {
            if let Some(buffer) = &mut self.buffer {
                buffer.extend_from_slice(&self.separator);
            }
            self.buffer
                .get_or_insert_with(Vec::new)
                .extend_from_slice(value);
        }
        self.truncate_if_needed(&mut newly_truncated);
        newly_truncated
    }

    /// Appends values from Go's unordered DISTINCT map finalizer.
    ///
    /// That path decides whether to write the separator from `buffer.Len() >
    /// 0`, not from buffer presence. Consequently an empty value followed by
    /// a non-empty value does not acquire a leading separator.
    pub(crate) fn update_distinct_values<'a>(
        &mut self,
        values: impl IntoIterator<Item = &'a [u8]>,
    ) -> bool {
        let mut newly_truncated = false;
        for value in values {
            let exceeded = {
                let buffer = self.buffer.get_or_insert_with(Vec::new);
                if !buffer.is_empty() {
                    buffer.extend_from_slice(&self.separator);
                }
                buffer.extend_from_slice(value);
                self.max_len > 0 && buffer.len() as u64 > self.max_len
            };
            if exceeded {
                self.truncate_if_needed(&mut newly_truncated);
                break;
            }
        }
        newly_truncated
    }

    /// Merges a source state into the destination in source order.
    pub fn merge_from(&mut self, source: &Self) -> bool {
        let Some(source_buffer) = source.buffer.as_deref() else {
            return false;
        };
        match &mut self.buffer {
            Some(destination) => {
                destination.extend_from_slice(&self.separator);
                destination.extend_from_slice(source_buffer);
            }
            None => self.buffer = Some(source_buffer.to_vec()),
        }
        let mut newly_truncated = false;
        self.truncate_if_needed(&mut newly_truncated);
        newly_truncated
    }

    /// Resets group data while retaining the aggregate-lifetime warning sentinel.
    pub fn reset(&mut self) {
        self.buffer = None;
    }

    /// Returns the final bytes, or NULL for an empty/all-NULL group.
    #[must_use]
    pub fn finish(&self) -> Option<&[u8]> {
        self.buffer.as_deref()
    }

    /// Returns the final result when it is valid UTF-8.
    #[must_use]
    pub fn finish_str(&self) -> Option<&str> {
        self.finish()
            .and_then(|value| std::str::from_utf8(value).ok())
    }

    /// Replaces the partial buffer when restoring a spill row.
    pub(crate) fn restore_buffer(&mut self, buffer: Option<Vec<u8>>) {
        self.buffer = buffer;
    }

    /// Reports whether any group crossed the maximum length.
    #[must_use]
    pub const fn was_truncated(&self) -> bool {
        self.truncated
    }

    /// Returns this Rust state's fixed allocation size.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        size_of::<Self>()
    }

    fn truncate_if_needed(&mut self, newly_truncated: &mut bool) {
        let Some(buffer) = self.buffer.as_mut() else {
            return;
        };
        if self.max_len == 0 || buffer.len() as u64 <= self.max_len {
            return;
        }
        buffer.truncate(usize::try_from(self.max_len).unwrap_or(usize::MAX));
        if !self.truncated {
            self.truncated = true;
            *newly_truncated = true;
        }
    }
}
