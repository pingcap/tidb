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

//! Index-usage sample metadata from
//! `pkg/statistics/handle/usage/indexusage/collector.go`.
//!
//! This leaf owns the source's seven percentage-access buckets and one-sample
//! construction. Session/global collectors, worker channels, persistence,
//! and schema-based garbage collection remain outside this crate.

use std::time::SystemTime;

/// Percentage boundaries used by index-usage samples.
pub const INDEX_USAGE_BUCKET_BOUNDS: [f64; 6] = [0.0, 0.01, 0.1, 0.2, 0.5, 1.0];

/// Number of percentage-access buckets in an index-usage sample.
pub const INDEX_USAGE_BUCKET_COUNT: usize = INDEX_USAGE_BUCKET_BOUNDS.len() + 1;

/// A single index-usage observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexUsageSample {
    /// Wall-clock time at which the observation was created.
    pub last_used_at: SystemTime,
    /// Number of queries attributed to the index by this observation.
    pub query_total: u64,
    /// Number of KV requests attributed to the index.
    pub kv_req_total: u64,
    /// Number of rows scanned through the index.
    pub row_access_total: u64,
    /// One-hot percentage-access bucket for this observation.
    pub percentage_access: [u64; INDEX_USAGE_BUCKET_COUNT],
}

impl IndexUsageSample {
    /// Merges another observation into this one.
    ///
    /// This is the source `indexUsage.updateByKey` value merge without the
    /// surrounding map, mutex, or collector channel.
    pub fn merge(&mut self, other: &Self) {
        self.query_total = self.query_total.wrapping_add(other.query_total);
        self.kv_req_total = self.kv_req_total.wrapping_add(other.kv_req_total);
        self.row_access_total = self.row_access_total.wrapping_add(other.row_access_total);
        for (current, incoming) in self
            .percentage_access
            .iter_mut()
            .zip(other.percentage_access.iter())
        {
            *current = current.wrapping_add(*incoming);
        }
        if self.last_used_at < other.last_used_at {
            self.last_used_at = other.last_used_at;
        }
    }
}

/// Maps a scanned-row percentage to TiDB's percentage-access bucket.
///
/// Values outside the source's explicit ranges intentionally retain the Go
/// zero-value bucket behavior. In particular, `NaN` and percentages greater
/// than one are not clamped or assigned a new bucket.
#[must_use]
pub fn index_usage_access_bucket(percentage: f64) -> usize {
    if percentage == 0.0 {
        return 0;
    }

    let mut bucket = 0;
    for index in 1..INDEX_USAGE_BUCKET_BOUNDS.len() {
        if percentage >= INDEX_USAGE_BUCKET_BOUNDS[index - 1]
            && percentage < INDEX_USAGE_BUCKET_BOUNDS[index]
        {
            bucket = index;
            break;
        }
    }
    if percentage == 1.0 {
        bucket = INDEX_USAGE_BUCKET_BOUNDS.len();
    }
    bucket
}

/// Constructs one index-usage observation and records its scan-percentage
/// bucket.
#[must_use]
pub fn new_index_usage_sample(
    query_total: u64,
    kv_req_total: u64,
    row_access: u64,
    table_total_rows: u64,
) -> IndexUsageSample {
    let mut percentage_access = [0; INDEX_USAGE_BUCKET_COUNT];
    let bucket = if table_total_rows > 0 {
        index_usage_access_bucket(row_access as f64 / table_total_rows as f64)
    } else {
        INDEX_USAGE_BUCKET_BOUNDS.len()
    };
    percentage_access[bucket] = 1;

    IndexUsageSample {
        last_used_at: SystemTime::now(),
        query_total,
        kv_req_total,
        row_access_total: row_access,
        percentage_access,
    }
}
