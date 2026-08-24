// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Common control surface for simulated TiKV clusters.
//!
//! This is the Rust test-support equivalent of client-go's
//! `internal/mockstore/cluster.Cluster`. Concrete mock behavior is owned by the
//! mock-store implementation rather than this interface.

use std::time::Duration;

use crate::proto::metapb::{Buckets, Peer, Region, Store, StoreLabel};

/// Changes simulated TiKV cluster state for tests.
pub(crate) trait Cluster {
    /// Allocates an ID usable as a store, region, or peer ID.
    fn alloc_id(&self) -> u64;

    /// Returns the region, leader, buckets, and down peers containing `key`.
    fn region_by_key(
        &self,
        key: &[u8],
    ) -> (Option<Region>, Option<Peer>, Option<Buckets>, Vec<Peer>);

    /// Returns metadata for every store.
    fn all_stores(&self) -> Vec<Store>;

    /// Schedules a delay for a transaction on one region.
    fn schedule_delay(&self, start_ts: u64, region_id: u64, duration: Duration);

    /// Splits a region at an encoded key.
    fn split(
        &self,
        region_id: u64,
        new_region_id: u64,
        key: &[u8],
        peer_ids: &[u64],
        leader_peer_id: u64,
    );

    /// Splits a region at an unencoded key and returns the new region, if any.
    fn split_raw(
        &self,
        region_id: u64,
        new_region_id: u64,
        raw_key: &[u8],
        peer_ids: &[u64],
        leader_peer_id: u64,
    ) -> Option<Region>;

    /// Evenly splits a key range into `count` regions.
    ///
    /// `count` stays signed because the source interface accepts a Go `int`.
    fn split_keys(&self, start: &[u8], end: &[u8], count: isize);

    /// Adds a store and its labels.
    fn add_store(&self, store_id: u64, address: &str, labels: Vec<StoreLabel>);

    /// Removes a store.
    fn remove_store(&self, store_id: u64);
}

#[cfg(test)]
mod tests {
    use super::*;

    struct InterfaceFixture;

    impl Cluster for InterfaceFixture {
        fn alloc_id(&self) -> u64 {
            1
        }

        fn region_by_key(
            &self,
            _key: &[u8],
        ) -> (Option<Region>, Option<Peer>, Option<Buckets>, Vec<Peer>) {
            (None, None, None, Vec::new())
        }

        fn all_stores(&self) -> Vec<Store> {
            Vec::new()
        }

        fn schedule_delay(&self, _start_ts: u64, _region_id: u64, _duration: Duration) {}

        fn split(
            &self,
            _region_id: u64,
            _new_region_id: u64,
            _key: &[u8],
            _peer_ids: &[u64],
            _leader_peer_id: u64,
        ) {
        }

        fn split_raw(
            &self,
            _region_id: u64,
            _new_region_id: u64,
            _raw_key: &[u8],
            _peer_ids: &[u64],
            _leader_peer_id: u64,
        ) -> Option<Region> {
            None
        }

        fn split_keys(&self, _start: &[u8], _end: &[u8], _count: isize) {}

        fn add_store(&self, _store_id: u64, _address: &str, _labels: Vec<StoreLabel>) {}

        fn remove_store(&self, _store_id: u64) {}
    }

    #[test]
    fn cluster_interface_is_object_safe_and_complete() {
        let cluster: &dyn Cluster = &InterfaceFixture;

        assert_eq!(cluster.alloc_id(), 1);
        assert_eq!(
            cluster.region_by_key(b"key"),
            (None, None, None, Vec::new())
        );
        assert!(cluster.all_stores().is_empty());
        cluster.schedule_delay(1, 2, Duration::from_millis(3));
        cluster.split(1, 2, b"encoded", &[3], 3);
        assert!(cluster.split_raw(1, 2, b"raw", &[3], 3).is_none());
        cluster.split_keys(b"a", b"z", -1);
        cluster.add_store(1, "store", Vec::new());
        cluster.remove_store(1);
    }
}
