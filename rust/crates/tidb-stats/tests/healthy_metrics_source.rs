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

//! Source-backed tests for statistics-health bucket metadata.

use tidb_stats::{
    healthy_bucket_configs, HealthyBucketConfig, STATS_HEALTHY_BUCKET_COUNT,
    STATS_HEALTHY_BUCKET_PSEUDO, STATS_HEALTHY_BUCKET_TOTAL,
};

#[test]
fn source_healthy_bucket_catalog_has_stable_indices_and_labels() {
    let configs = healthy_bucket_configs();
    assert_eq!(configs.len(), STATS_HEALTHY_BUCKET_COUNT);
    assert_eq!(
        configs
            .iter()
            .map(|config| config.index)
            .collect::<Vec<_>>(),
        (0..STATS_HEALTHY_BUCKET_COUNT).collect::<Vec<_>>()
    );
    assert_eq!(
        configs
            .iter()
            .map(|config| config.label)
            .collect::<Vec<_>>(),
        vec![
            "[0,50)",
            "[50,55)",
            "[55,60)",
            "[60,70)",
            "[70,80)",
            "[80,100)",
            "[100,100]",
            "[0,100]",
            "unneeded analyze",
            "pseudo",
        ]
    );
}

#[test]
fn source_healthy_bucket_bounds_keep_special_categories_explicit() {
    let configs = healthy_bucket_configs();
    assert_eq!(
        configs
            .iter()
            .map(|config| config.upper_bound)
            .collect::<Vec<_>>(),
        vec![50, 55, 60, 70, 80, 100, 101, 0, 0, 0]
    );
    assert_eq!(configs[STATS_HEALTHY_BUCKET_TOTAL].label, "[0,100]");
    assert_eq!(configs[STATS_HEALTHY_BUCKET_PSEUDO].label, "pseudo");
}

#[test]
fn source_healthy_bucket_config_is_copyable_value_metadata() {
    let config = healthy_bucket_configs()[0];
    let copied: HealthyBucketConfig = config;
    assert_eq!(copied, config);
}
