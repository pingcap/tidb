// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Immutable region bucket metadata and half-open lookup rules.

use super::RegionLocation;

/// Per-bucket counters from pinned `metapb.BucketStats` fields 1 through 6.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BucketStats {
    /// Total bytes read from every bucket.
    pub read_bytes: Vec<u64>,
    /// Total bytes written to every bucket.
    pub write_bytes: Vec<u64>,
    /// Read queries per second for every bucket.
    pub read_qps: Vec<u64>,
    /// Write queries per second for every bucket.
    pub write_qps: Vec<u64>,
    /// Keys read from every bucket.
    pub read_keys: Vec<u64>,
    /// Keys written to every bucket.
    pub write_keys: Vec<u64>,
}

/// Transport-neutral projection of pinned `metapb.Buckets` fields 1 through 5.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BucketMetadata {
    /// Region identity that owns this bucket version.
    pub region_id: u64,
    /// Monotonic bucket-layout version.
    pub version: u64,
    /// Ordered split keys. They may be stale and outside the current region.
    pub keys: Vec<Vec<u8>>,
    /// Optional source counters.
    pub stats: Option<BucketStats>,
    /// Source collection period in milliseconds.
    pub period_in_ms: u64,
}

/// One half-open bucket after clamping to the current region.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Bucket {
    /// Inclusive bucket start.
    pub start_key: Vec<u8>,
    /// Exclusive bucket end. Empty means positive infinity.
    pub end_key: Vec<u8>,
}

impl Bucket {
    /// Whether `key` belongs to this half-open bucket.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.start_key.as_slice() <= key
            && (self.end_key.is_empty() || key < self.end_key.as_slice())
    }
}

impl RegionLocation {
    /// Returns zero when PD supplied no bucket metadata.
    #[must_use]
    pub fn bucket_version(&self) -> u64 {
        self.buckets.as_ref().map_or(0, |buckets| buckets.version)
    }

    /// Locates `key` using possibly stale bucket keys, then clamps the result
    /// to this region exactly like client-go `KeyLocation.LocateBucket`.
    #[must_use]
    pub fn locate_bucket(&self, key: &[u8]) -> Option<Bucket> {
        if !self.contains_key(key) {
            return None;
        }
        let metadata = self.buckets.as_ref()?;

        if let Some(bucket) = raw_bucket(&metadata.keys, key) {
            return Some(self.clamp_bucket(bucket));
        }

        let Some(first) = metadata.keys.first() else {
            return Some(self.region_bucket());
        };
        if key < first.as_slice() {
            return Some(self.clamp_bucket(Bucket {
                start_key: self.start_key.clone(),
                end_key: first.clone(),
            }));
        }
        let last = metadata.keys.last().expect("first bucket key exists");
        if last.as_slice() <= key {
            return Some(self.clamp_bucket(Bucket {
                start_key: last.clone(),
                end_key: self.end_key.clone(),
            }));
        }

        // Ordered keys make every interior key reachable. Treat malformed
        // metadata as one region bucket instead of publishing an invalid range.
        Some(self.region_bucket())
    }

    fn region_bucket(&self) -> Bucket {
        Bucket {
            start_key: self.start_key.clone(),
            end_key: self.end_key.clone(),
        }
    }

    fn clamp_bucket(&self, bucket: Bucket) -> Bucket {
        let mut start_key = bucket.start_key;
        let mut end_key = bucket.end_key;
        if start_key < self.start_key {
            start_key.clone_from(&self.start_key);
        }
        if !self.end_key.is_empty() && (end_key.is_empty() || end_key > self.end_key) {
            end_key.clone_from(&self.end_key);
        }
        if !end_key.is_empty() && start_key >= end_key {
            return self.region_bucket();
        }
        Bucket { start_key, end_key }
    }
}

fn raw_bucket(keys: &[Vec<u8>], key: &[u8]) -> Option<Bucket> {
    if keys.len() < 2 {
        return None;
    }
    let search_len = keys.len() - 1;
    let upper = keys[..search_len].partition_point(|candidate| key >= candidate.as_slice());
    if upper == 0
        || upper == search_len && !keys[search_len].is_empty() && key >= keys[search_len].as_slice()
    {
        return None;
    }
    Some(Bucket {
        start_key: keys[upper - 1].clone(),
        end_key: keys[upper].clone(),
    })
}
