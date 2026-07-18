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

//! Statistics-health bucket metadata from
//! `pkg/statistics/handle/metrics/metrics.go`.
//!
//! The Go source owns the scalar bucket indexes, upper bounds, and labels used
//! when publishing Prometheus gauges. This leaf keeps those values available
//! without importing Prometheus, statistics-table traversal, or gauge state.

/// Healthy statistics bucket for values in `[0, 50)`.
pub const STATS_HEALTHY_BUCKET_0_TO_50: usize = 0;
/// Healthy statistics bucket for values in `[50, 55)`.
pub const STATS_HEALTHY_BUCKET_50_TO_55: usize = 1;
/// Healthy statistics bucket for values in `[55, 60)`.
pub const STATS_HEALTHY_BUCKET_55_TO_60: usize = 2;
/// Healthy statistics bucket for values in `[60, 70)`.
pub const STATS_HEALTHY_BUCKET_60_TO_70: usize = 3;
/// Healthy statistics bucket for values in `[70, 80)`.
pub const STATS_HEALTHY_BUCKET_70_TO_80: usize = 4;
/// Healthy statistics bucket for values in `[80, 100)`.
pub const STATS_HEALTHY_BUCKET_80_TO_100: usize = 5;
/// Healthy statistics bucket for values in `[100, 100]`.
pub const STATS_HEALTHY_BUCKET_100_TO_100: usize = 6;
/// Aggregate healthy statistics bucket.
pub const STATS_HEALTHY_BUCKET_TOTAL: usize = 7;
/// Bucket for tables that need analysis.
pub const STATS_HEALTHY_BUCKET_UNNEEDED_ANALYZE: usize = 8;
/// Bucket for pseudo statistics.
pub const STATS_HEALTHY_BUCKET_PSEUDO: usize = 9;
/// Number of configured healthy statistics buckets.
pub const STATS_HEALTHY_BUCKET_COUNT: usize = 10;

/// One source healthy-bucket index, upper-bound, and Prometheus label.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HealthyBucketConfig {
    /// Stable bucket index.
    pub index: usize,
    /// Exclusive numeric upper bound; zero denotes a special category.
    pub upper_bound: i64,
    /// Stable source label.
    pub label: &'static str,
}

/// Source-ordered healthy-bucket configurations.
pub const HEALTHY_BUCKET_CONFIGS: [HealthyBucketConfig; STATS_HEALTHY_BUCKET_COUNT] = [
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_0_TO_50,
        upper_bound: 50,
        label: "[0,50)",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_50_TO_55,
        upper_bound: 55,
        label: "[50,55)",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_55_TO_60,
        upper_bound: 60,
        label: "[55,60)",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_60_TO_70,
        upper_bound: 70,
        label: "[60,70)",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_70_TO_80,
        upper_bound: 80,
        label: "[70,80)",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_80_TO_100,
        upper_bound: 100,
        label: "[80,100)",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_100_TO_100,
        upper_bound: 101,
        label: "[100,100]",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_TOTAL,
        upper_bound: 0,
        label: "[0,100]",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_UNNEEDED_ANALYZE,
        upper_bound: 0,
        label: "unneeded analyze",
    },
    HealthyBucketConfig {
        index: STATS_HEALTHY_BUCKET_PSEUDO,
        upper_bound: 0,
        label: "pseudo",
    },
];

/// Returns the source-ordered healthy-bucket catalog.
#[must_use]
pub const fn healthy_bucket_configs() -> &'static [HealthyBucketConfig] {
    &HEALTHY_BUCKET_CONFIGS
}
