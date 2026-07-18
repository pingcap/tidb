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

//! Source-backed tests for the InfoSchema partition-to-table cache contract.

use tidb_stats::PartitionTableIdCache;

#[test]
fn partition_cache_resolves_parent_and_missing_ids() {
    let mut cache = PartitionTableIdCache::new();
    let pairs = [(101, 10), (102, 10)];

    assert_eq!(cache.lookup(1, &pairs, 101), Some(10));
    assert_eq!(cache.lookup(1, &pairs, 1 << 60), None);
    assert_eq!(cache.schema_version(), 1);
    assert_eq!(cache.len(), 2);
}

#[test]
fn partition_cache_rebuilds_only_after_schema_version_changes() {
    let mut cache = PartitionTableIdCache::new();
    assert_eq!(cache.lookup(7, &[(101, 10)], 101), Some(10));

    // Same schema version keeps the cached map even if the caller's source
    // slice is stale or has been replaced.
    assert_eq!(cache.lookup(7, &[(101, 11)], 101), Some(10));
    assert_eq!(cache.lookup(8, &[(101, 11)], 101), Some(11));
    assert_eq!(cache.len(), 1);
}

#[test]
fn partition_cache_duplicate_pairs_follow_last_assignment() {
    let mut cache = PartitionTableIdCache::new();
    assert_eq!(cache.lookup(1, &[(101, 10), (101, 12)], 101), Some(12));
    assert!(!cache.is_empty());
}
