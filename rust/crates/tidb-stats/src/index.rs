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

//! Aggregate index-statistics behavior from `pkg/statistics/index.go`.

use crate::histogram::Histogram;
use crate::{
    query_index_bytes, CmsSketch, FmSketch, IndexMemUsage, StatsLoadedStatus, TopN, ALL_EVICTED,
    ALL_LOADED,
};

/// The index metadata used directly by `index.go` and `table.go`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexInfo {
    pub id: i64,
    pub name: String,
    /// Lower-cased indexed-column names in index order.
    pub columns: Vec<String>,
    pub mv_index: bool,
}

/// An index histogram and its optional sketches.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Index {
    pub cmsketch: Option<CmsSketch>,
    pub top_n: Option<TopN>,
    pub fm_sketch: Option<FmSketch>,
    pub info: Option<IndexInfo>,
    pub histogram: Histogram,
    pub stats_loaded_status: StatsLoadedStatus,
    pub stats_version: i64,
    pub physical_id: i64,
}

impl Index {
    #[must_use]
    pub fn copy(&self) -> Self {
        self.clone()
    }

    #[must_use]
    pub fn item_id(&self) -> i64 {
        self.info.as_ref().expect("index has no metadata").id
    }

    #[must_use]
    pub const fn is_all_evicted(&self) -> bool {
        self.stats_loaded_status.is_all_evicted()
    }

    #[must_use]
    pub const fn evicted_status(&self) -> i32 {
        self.stats_loaded_status.evicted_status()
    }

    pub fn drop_unnecessary_data(&mut self) {
        if self.stats_version < 2 {
            self.cmsketch = None;
        }
        self.top_n = None;
        self.histogram.buckets.clear();
        self.stats_loaded_status =
            StatsLoadedStatus::new(self.stats_loaded_status.stats_initialized(), ALL_EVICTED);
    }

    #[must_use]
    pub const fn stats_version(&self) -> i64 {
        self.stats_version
    }

    #[must_use]
    pub const fn is_cms_exist(&self) -> bool {
        self.cmsketch.is_some()
    }

    /// Go `IsEvicted` intentionally tests only the integer status. The zero
    /// value therefore behaves as `AllLoaded` even when uninitialized.
    #[must_use]
    pub const fn is_evicted(&self) -> bool {
        self.stats_loaded_status.evicted_status() != ALL_LOADED
    }

    #[must_use]
    pub const fn is_full_load(&self) -> bool {
        self.stats_loaded_status.is_full_load()
    }

    #[must_use]
    pub fn total_row_count(&self) -> f64 {
        let histogram = self.histogram.total_row_count();
        if self.stats_version >= 2 {
            histogram
                + self
                    .top_n
                    .as_ref()
                    .expect("v2 index has no TopN")
                    .total_count() as f64
        } else {
            histogram
        }
    }

    /// Go test-only `EvictAllStats`.
    pub fn evict_all_stats(&mut self) {
        self.histogram.buckets.clear();
        self.cmsketch = None;
        self.top_n = None;
        self.stats_loaded_status =
            StatsLoadedStatus::new(self.stats_loaded_status.stats_initialized(), ALL_EVICTED);
    }

    /// Go `MemoryUsage`; FM sketch memory is intentionally not included.
    #[must_use]
    pub fn memory_usage(&self) -> IndexMemUsage {
        let mut usage = IndexMemUsage {
            index_id: self.item_id(),
            histogram_mem_usage: self.histogram.memory_usage(),
            ..IndexMemUsage::default()
        };
        let mut total = usage.histogram_mem_usage;
        if let Some(cmsketch) = &self.cmsketch {
            usage.cmsketch_mem_usage = cmsketch.memory_usage() as i64;
            total = total.wrapping_add(usage.cmsketch_mem_usage);
        }
        if let Some(top_n) = &self.top_n {
            usage.topn_mem_usage = top_n.memory_usage() as i64;
            total = total.wrapping_add(usage.topn_mem_usage);
        }
        usage.total_mem_usage = total;
        usage
    }

    /// Go `QueryBytes` using the existing source-owned histogram fallback
    /// value. TopN and CMS are resolved here in the original precedence.
    #[must_use]
    pub fn query_bytes(&self, encoded: &[u8], histogram_count: u64) -> u64 {
        query_index_bytes(
            self.top_n
                .as_ref()
                .and_then(|top_n| top_n.query_bytes(encoded)),
            self.cmsketch
                .as_ref()
                .map(|cmsketch| cmsketch.query_bytes(encoded)),
            histogram_count,
        )
    }

    #[must_use]
    pub fn increase_factor(&self, realtime_row_count: i64) -> f64 {
        let index_count = self.total_row_count();
        if index_count == 0.0 {
            1.0
        } else {
            realtime_row_count as f64 / index_count
        }
    }

    #[must_use]
    pub const fn histogram(&self) -> &Histogram {
        &self.histogram
    }

    #[must_use]
    pub const fn top_n(&self) -> Option<&TopN> {
        self.top_n.as_ref()
    }

    #[must_use]
    pub const fn is_analyzed(&self) -> bool {
        self.stats_version > 0
    }
}

#[must_use]
pub fn copy_index(index: Option<&Index>) -> Option<Index> {
    index.map(Index::copy)
}

#[must_use]
pub fn index_is_all_evicted(index: Option<&Index>) -> bool {
    index.is_none_or(Index::is_all_evicted)
}
