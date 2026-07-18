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

//! Source-backed tests for pseudo-statistics cache admission.

use tidb_stats::{should_cache_pseudo_stats, PSEUDO_CACHE_PARTITION_LIMIT};

#[test]
fn source_pseudo_cache_policy_matches_partition_threshold() {
    assert!(should_cache_pseudo_stats(false, 10_000, false));
    assert!(should_cache_pseudo_stats(
        true,
        PSEUDO_CACHE_PARTITION_LIMIT - 1,
        false
    ));
    assert!(!should_cache_pseudo_stats(
        true,
        PSEUDO_CACHE_PARTITION_LIMIT,
        false
    ));
    assert!(!should_cache_pseudo_stats(true, 10_000, false));
}

#[test]
fn source_pseudo_cache_policy_rejects_temporary_tables() {
    assert!(!should_cache_pseudo_stats(false, 0, true));
    assert!(!should_cache_pseudo_stats(true, 0, true));
    assert!(!should_cache_pseudo_stats(
        false,
        PSEUDO_CACHE_PARTITION_LIMIT,
        true
    ));
}
