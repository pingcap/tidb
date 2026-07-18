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

//! Statistics test-fixture shape from
//! `pkg/statistics/handle/cache/internal/testutil/testutil.go`.
//!
//! The Go benchmark fixture creates a table with caller-selected column/index
//! counts and optional CMSketch, TopN, and histogram payloads. This leaf keeps
//! only that dependency-closed shape; `statistics.Table` allocation, payload
//! construction, field types, memory accounting, and cache/benchmark runtime
//! remain external.

/// Caller-selected shape for a mock statistics table fixture.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MockStatisticsTableShape {
    /// Number of mock columns.
    pub columns: usize,
    /// Number of mock indexes.
    pub indices: usize,
    /// Whether each item receives a CMSketch.
    pub with_cms: bool,
    /// Whether each item receives a TopN payload.
    pub with_top_n: bool,
    /// Whether each item receives a histogram payload.
    pub with_hist: bool,
}

impl MockStatisticsTableShape {
    /// Creates a fixture shape matching `NewMockStatisticsTable` arguments.
    #[must_use]
    pub const fn new(
        columns: usize,
        indices: usize,
        with_cms: bool,
        with_top_n: bool,
        with_hist: bool,
    ) -> Self {
        Self {
            columns,
            indices,
            with_cms,
            with_top_n,
            with_hist,
        }
    }

    /// Returns the total number of mock column/index items.
    #[must_use]
    pub const fn item_count(self) -> usize {
        self.columns + self.indices
    }
}
