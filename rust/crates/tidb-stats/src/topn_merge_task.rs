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

//! TopN merge task range from `pkg/statistics/handle/globalstats/merge_worker.go`.
//!
//! The Go global-statistics worker passes a start/end partition range through
//! a task channel. This leaf owns only that immutable range descriptor;
//! channel scheduling, TopN/histogram values, cancellation, and merge logic
//! remain external.

/// Identifies the half-open range of TopN partitions handled by one worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TopnStatsMergeTask {
    start: isize,
    end: isize,
}

impl TopnStatsMergeTask {
    /// Creates a task descriptor without validating the caller's range.
    ///
    /// The Go constructor stores both integers directly; range validation is
    /// intentionally left to the eventual worker/slice owner.
    #[must_use]
    pub const fn new(start: isize, end: isize) -> Self {
        Self { start, end }
    }

    /// Returns the inclusive start boundary used by the worker slice.
    #[must_use]
    pub const fn start(self) -> isize {
        self.start
    }

    /// Returns the exclusive end boundary used by the worker slice.
    #[must_use]
    pub const fn end(self) -> isize {
        self.end
    }
}
