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

//! Column/index memory-usage value objects from `pkg/statistics/table.go`.
//!
//! These structs report already-measured component sizes. They do not measure
//! CMSketch, TopN, histogram, or FM-sketch allocations and do not own cache
//! eviction or LFU accounting.

/// Measured memory usage for a column's statistics components.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ColumnMemUsage {
    /// Column metadata ID.
    pub column_id: i64,
    /// Histogram bytes.
    pub histogram_mem_usage: i64,
    /// CMSketch bytes.
    pub cmsketch_mem_usage: i64,
    /// FMSketch bytes (included in total, not tracking usage).
    pub fmsketch_mem_usage: i64,
    /// TopN bytes.
    pub topn_mem_usage: i64,
    /// Total measured bytes.
    pub total_mem_usage: i64,
}

impl ColumnMemUsage {
    /// Returns the measured total.
    #[must_use]
    pub const fn total_memory_usage(self) -> i64 {
        self.total_mem_usage
    }

    /// Returns the column ID.
    #[must_use]
    pub const fn item_id(self) -> i64 {
        self.column_id
    }

    /// Returns cache-tracked bytes (histogram + CMSketch + TopN).
    #[must_use]
    pub const fn tracking_mem_usage(self) -> i64 {
        self.cmsketch_mem_usage
            .wrapping_add(self.topn_mem_usage)
            .wrapping_add(self.histogram_mem_usage)
    }

    /// Returns measured histogram bytes.
    #[must_use]
    pub const fn hist_mem_usage(self) -> i64 {
        self.histogram_mem_usage
    }

    /// Returns measured TopN bytes.
    #[must_use]
    pub const fn topn_mem_usage(self) -> i64 {
        self.topn_mem_usage
    }

    /// Returns measured CMSketch bytes.
    #[must_use]
    pub const fn cms_mem_usage(self) -> i64 {
        self.cmsketch_mem_usage
    }
}

/// Measured memory usage for an index's statistics components.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IndexMemUsage {
    /// Index metadata ID.
    pub index_id: i64,
    /// Histogram bytes.
    pub histogram_mem_usage: i64,
    /// CMSketch bytes.
    pub cmsketch_mem_usage: i64,
    /// TopN bytes.
    pub topn_mem_usage: i64,
    /// Total measured bytes.
    pub total_mem_usage: i64,
}

impl IndexMemUsage {
    /// Returns the measured total.
    #[must_use]
    pub const fn total_memory_usage(self) -> i64 {
        self.total_mem_usage
    }

    /// Returns the index ID.
    #[must_use]
    pub const fn item_id(self) -> i64 {
        self.index_id
    }

    /// Returns cache-tracked bytes (histogram + CMSketch + TopN).
    #[must_use]
    pub const fn tracking_mem_usage(self) -> i64 {
        self.cmsketch_mem_usage
            .wrapping_add(self.topn_mem_usage)
            .wrapping_add(self.histogram_mem_usage)
    }

    /// Returns measured histogram bytes.
    #[must_use]
    pub const fn hist_mem_usage(self) -> i64 {
        self.histogram_mem_usage
    }

    /// Returns measured TopN bytes.
    #[must_use]
    pub const fn topn_mem_usage(self) -> i64 {
        self.topn_mem_usage
    }

    /// Returns measured CMSketch bytes.
    #[must_use]
    pub const fn cms_mem_usage(self) -> i64 {
        self.cmsketch_mem_usage
    }
}
