// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use derive_new::new;
use std::fmt;

use crate::proto::metapb;
use crate::proto::pdpb;
use crate::Error;
use crate::Key;
use crate::Result;

/// The ID of a region
pub type RegionId = u64;
/// The ID of a store
pub type StoreId = u64;

/// A single bucket range inside a region.
///
/// Bucket end keys follow TiKV range conventions: an empty end is positive
/// infinity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Bucket {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

impl Bucket {
    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        self.start_key.as_slice() <= key
            && (self.end_key.is_empty() || key < self.end_key.as_slice())
    }
}

/// The ID and version information of a region.
#[derive(Eq, PartialEq, Hash, Clone, Default, Debug)]
pub struct RegionVerId {
    /// The ID of the region
    pub id: RegionId,
    /// Conf change version, auto increment when add or remove peer
    pub conf_ver: u64,
    /// Region version, auto increment when split or merge
    pub ver: u64,
}

impl fmt::Display for RegionVerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{{ region id: {}, ver: {}, confVer: {} }}",
            self.id, self.ver, self.conf_ver
        )
    }
}

/// Information about a TiKV region and its leader.
///
/// In TiKV all data is partitioned by range. Each partition is called a region.
#[derive(new, Clone, Default, Debug, PartialEq)]
pub struct RegionWithLeader {
    pub region: metapb::Region,
    pub leader: Option<metapb::Peer>,
    /// PD bucket metadata associated with this region, when bucket-aware
    /// routing requested it or TiKV refreshed it after a version mismatch.
    #[new(default)]
    pub buckets: Option<metapb::Buckets>,
    /// Peers PD reports as not yet caught up with the leader. TiFlash batch
    /// work prefers other replicas when they are available.
    #[new(default)]
    pub pending_peers: Vec<metapb::Peer>,
    /// Peers PD reports as down. Source region construction removes them from
    /// every access mode before a selector can choose a route.
    #[new(default)]
    pub down_peers: Vec<pdpb::PeerStats>,
}

impl Eq for RegionWithLeader {}

impl RegionWithLeader {
    pub fn contains(&self, key: &Key) -> bool {
        let key: &[u8] = key.into();
        let start_key = &self.region.start_key;
        let end_key = &self.region.end_key;
        key >= start_key.as_slice() && (key < end_key.as_slice() || end_key.is_empty())
    }

    pub fn start_key(&self) -> Key {
        self.region.start_key.to_vec().into()
    }

    pub fn end_key(&self) -> Key {
        self.region.end_key.to_vec().into()
    }

    pub fn range(&self) -> (Key, Key) {
        (self.start_key(), self.end_key())
    }

    pub fn ver_id(&self) -> RegionVerId {
        let region = &self.region;
        let epoch = region.region_epoch.as_ref();
        RegionVerId {
            id: region.id,
            conf_ver: epoch.map_or(0, |epoch| epoch.conf_ver),
            ver: epoch.map_or(0, |epoch| epoch.version),
        }
    }

    pub fn id(&self) -> RegionId {
        self.region.id
    }

    /// The bucket version carried in TiKV's request context. Regions without
    /// bucket metadata retain the source zero-value (bucket feature disabled).
    pub fn buckets_version(&self) -> u64 {
        self.buckets.as_ref().map_or(0, |buckets| buckets.version)
    }

    /// Returns the precise bucket that contains `key`, if the cached bucket
    /// boundaries cover it. Stale bucket metadata deliberately returns `None`;
    /// callers that only need a region-contained fallback should use
    /// [`Self::locate_bucket`].
    pub(crate) fn locate_cached_bucket(&self, key: &[u8]) -> Option<Bucket> {
        let buckets = self.buckets.as_ref()?;
        let keys = buckets.keys.as_slice();
        let search_len = keys.len().checked_sub(1)?;

        // This is Go's `sort.Search(len(keys)-1, key < keys[i])`. The last
        // key is the candidate bucket end, so it is intentionally excluded
        // from the search predicate.
        let index = keys[..search_len].partition_point(|bucket_key| key >= bucket_key.as_slice());
        if index == 0
            || (index == search_len
                && !keys[search_len].is_empty()
                && key >= keys[search_len].as_slice())
        {
            return None;
        }

        Some(Bucket {
            start_key: keys[index - 1].clone(),
            end_key: keys[index].clone(),
        })
    }

    /// Returns the bucket containing `key`, including client-go's defensive
    /// fallback for holes at either end of stale bucket metadata. A key outside
    /// this region is never assigned a bucket.
    pub(crate) fn locate_bucket(&self, key: &[u8]) -> Option<Bucket> {
        if let Some(bucket) = self.locate_cached_bucket(key) {
            return Some(self.clamp_bucket_to_region(bucket));
        }
        if !self.contains(&key.to_vec().into()) {
            return None;
        }

        let keys = self.buckets.as_ref()?.keys.as_slice();
        if keys.is_empty() {
            return Some(Bucket {
                start_key: self.region.start_key.clone(),
                end_key: self.region.end_key.clone(),
            });
        }
        if key < keys[0].as_slice() {
            return Some(Bucket {
                start_key: self.region.start_key.clone(),
                end_key: keys[0].clone(),
            });
        }
        if keys.last().is_some_and(|last| last.as_slice() <= key) {
            return Some(Bucket {
                start_key: keys.last().unwrap().clone(),
                end_key: self.region.end_key.clone(),
            });
        }

        None
    }

    fn clamp_bucket_to_region(&self, bucket: Bucket) -> Bucket {
        let mut start_key = bucket.start_key;
        let mut end_key = bucket.end_key;
        if start_key < self.region.start_key {
            start_key.clone_from(&self.region.start_key);
        }
        if !self.region.end_key.is_empty() && (end_key.is_empty() || end_key > self.region.end_key)
        {
            end_key.clone_from(&self.region.end_key);
        }
        if !end_key.is_empty() && start_key >= end_key {
            return Bucket {
                start_key: self.region.start_key.clone(),
                end_key: self.region.end_key.clone(),
            };
        }
        Bucket { start_key, end_key }
    }

    pub fn get_store_id(&self) -> Result<StoreId> {
        self.leader
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::LeaderNotFound {
                region: self.ver_id(),
            })
            .map(|s| s.store_id)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn source_missing_region_epoch_uses_protobuf_zero_values() {
        let region = RegionWithLeader {
            region: metapb::Region {
                id: 42,
                region_epoch: None,
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(
            region.ver_id(),
            RegionVerId {
                id: 42,
                conf_ver: 0,
                ver: 0,
            }
        );
    }

    fn region_with_buckets(
        start_key: &[u8],
        end_key: &[u8],
        bucket_keys: &[&[u8]],
    ) -> RegionWithLeader {
        RegionWithLeader {
            region: metapb::Region {
                start_key: start_key.to_vec(),
                end_key: end_key.to_vec(),
                ..Default::default()
            },
            buckets: Some(metapb::Buckets {
                keys: bucket_keys.iter().map(|key| key.to_vec()).collect(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn source_region_version_display_preserves_field_order() {
        assert_eq!(
            RegionVerId {
                id: 7,
                conf_ver: 9,
                ver: 8,
            }
            .to_string(),
            "{ region id: 7, ver: 8, confVer: 9 }"
        );
    }

    #[test]
    fn source_bucket_lookup_distinguishes_stale_holes_from_region_fallback() {
        let region = region_with_buckets(b"", b"", &[b"", b"a", b"b", b""]);
        for (key, expected) in [
            (
                b"".as_slice(),
                Bucket {
                    start_key: vec![],
                    end_key: b"a".to_vec(),
                },
            ),
            (
                b"a0".as_slice(),
                Bucket {
                    start_key: b"a".to_vec(),
                    end_key: b"b".to_vec(),
                },
            ),
            (
                b"c".as_slice(),
                Bucket {
                    start_key: b"b".to_vec(),
                    end_key: vec![],
                },
            ),
        ] {
            let bucket = region.locate_cached_bucket(key).unwrap();
            assert_eq!(bucket, expected);
            assert!(bucket.contains(key));
        }

        let stale = region_with_buckets(b"", b"", &[b"b", b"c", b"d"]);
        assert!(stale.locate_cached_bucket(b"a").is_none());
        assert!(stale.locate_cached_bucket(b"d").is_none());
        assert_eq!(
            stale.locate_bucket(b"a"),
            Some(Bucket {
                start_key: vec![],
                end_key: b"b".to_vec()
            })
        );
        assert_eq!(
            stale.locate_bucket(b"e"),
            Some(Bucket {
                start_key: b"d".to_vec(),
                end_key: vec![]
            })
        );
    }

    #[test]
    fn source_bucket_lookup_clamps_stale_boundaries_to_region() {
        let region = region_with_buckets(b"f", b"m", &[b"a", b"z"]);
        assert_eq!(
            region.locate_bucket(b"g"),
            Some(Bucket {
                start_key: b"f".to_vec(),
                end_key: b"m".to_vec()
            })
        );

        let before_region = region_with_buckets(b"m", b"z", &[b"a", b"f"]);
        assert_eq!(
            before_region.locate_bucket(b"n"),
            Some(Bucket {
                start_key: b"f".to_vec(),
                end_key: b"z".to_vec()
            })
        );

        let outside = region_with_buckets(b"f", b"m", &[b"a", b"z"]);
        assert_eq!(outside.locate_bucket(b"z"), None);
    }

    #[test]
    fn source_bucket_clamp_handles_empty_and_invalid_bounds() {
        for (region_start, region_end, bucket_start, bucket_end, expected_start, expected_end) in [
            (
                b"a".as_slice(),
                b"z".as_slice(),
                b"f".as_slice(),
                b"m".as_slice(),
                b"f".as_slice(),
                b"m".as_slice(),
            ),
            (b"f", b"z", b"a", b"m", b"f", b"m"),
            (b"a", b"m", b"f", b"z", b"f", b"m"),
            (b"", b"m", b"a", b"z", b"a", b"m"),
            (b"f", b"", b"a", b"z", b"f", b"z"),
            (b"m", b"z", b"a", b"f", b"m", b"z"),
            (b"f", b"z", b"", b"m", b"f", b"m"),
            (b"a", b"m", b"f", b"", b"f", b"m"),
            (b"", b"", b"a", b"z", b"a", b"z"),
        ] {
            let region = region_with_buckets(region_start, region_end, &[]);
            assert_eq!(
                region.clamp_bucket_to_region(Bucket {
                    start_key: bucket_start.to_vec(),
                    end_key: bucket_end.to_vec(),
                }),
                Bucket {
                    start_key: expected_start.to_vec(),
                    end_key: expected_end.to_vec(),
                }
            );
        }
    }
}
