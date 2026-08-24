// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::iter::Iterator;

use crate::proto::kvrpcpb;
use crate::proto::pdpb::Timestamp;
/// This module provides constructor functions for requests which take arguments as high-level
/// types (i.e., the types from the client crate) and converts these to the types used in the
/// generated protobuf code, then calls the low-level ctor functions in the requests module.
use crate::timestamp::TimestampExt;
/// This module provides constructor functions for requests which take arguments as high-level
/// types (i.e., the types from the client crate) and converts these to the types used in the
/// generated protobuf code, then calls the low-level ctor functions in the requests module.
use crate::transaction::requests;
/// This module provides constructor functions for requests which take arguments as high-level
/// types (i.e., the types from the client crate) and converts these to the types used in the
/// generated protobuf code, then calls the low-level ctor functions in the requests module.
use crate::BoundRange;
/// This module provides constructor functions for requests which take arguments as high-level
/// types (i.e., the types from the client crate) and converts these to the types used in the
/// generated protobuf code, then calls the low-level ctor functions in the requests module.
use crate::Key;

pub fn new_get_request(key: Key, timestamp: Timestamp) -> kvrpcpb::GetRequest {
    requests::new_get_request(key.into(), timestamp.version())
}

pub fn new_batch_get_request(
    keys: impl Iterator<Item = Key>,
    timestamp: Timestamp,
) -> kvrpcpb::BatchGetRequest {
    requests::new_batch_get_request(keys.map(Into::into).collect(), timestamp.version())
}

pub fn new_scan_request(
    range: BoundRange,
    timestamp: Timestamp,
    limit: u32,
    key_only: bool,
    reverse: bool,
    sample_step: u32,
) -> kvrpcpb::ScanRequest {
    let (start_key, end_key) = range.into_keys();
    requests::new_scan_request(
        start_key.into(),
        end_key.unwrap_or_default().into(),
        timestamp.version(),
        limit,
        key_only,
        reverse,
        sample_step,
    )
}

pub fn new_prewrite_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_lock: Key,
    start_version: Timestamp,
    lock_ttl: u64,
) -> kvrpcpb::PrewriteRequest {
    requests::new_prewrite_request(
        mutations,
        primary_lock.into(),
        start_version.version(),
        lock_ttl,
    )
}

pub fn new_pessimistic_prewrite_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_lock: Key,
    start_version: Timestamp,
    lock_ttl: u64,
    for_update_ts: Timestamp,
) -> kvrpcpb::PrewriteRequest {
    requests::new_pessimistic_prewrite_request(
        mutations,
        primary_lock.into(),
        start_version.version(),
        lock_ttl,
        for_update_ts.version(),
    )
}

pub fn new_commit_request(
    keys: impl Iterator<Item = Key>,
    start_version: Timestamp,
    commit_version: Timestamp,
) -> kvrpcpb::CommitRequest {
    requests::new_commit_request(
        keys.map(Into::into).collect(),
        start_version.version(),
        commit_version.version(),
    )
}

pub fn new_batch_rollback_request(
    keys: impl Iterator<Item = Key>,
    start_version: Timestamp,
) -> kvrpcpb::BatchRollbackRequest {
    requests::new_batch_rollback_request(keys.map(Into::into).collect(), start_version.version())
}

pub fn new_pessimistic_rollback_request(
    keys: impl Iterator<Item = Key>,
    start_version: Timestamp,
    for_update_ts: Timestamp,
) -> kvrpcpb::PessimisticRollbackRequest {
    requests::new_pessimistic_rollback_request(
        keys.map(Into::into).collect(),
        start_version.version(),
        for_update_ts.version(),
    )
}

pub trait PessimisticLock: Clone {
    fn key(self) -> Key;

    fn assertion(&self) -> kvrpcpb::Assertion;
}

impl PessimisticLock for Key {
    fn key(self) -> Key {
        self
    }

    fn assertion(&self) -> kvrpcpb::Assertion {
        kvrpcpb::Assertion::None
    }
}

impl PessimisticLock for (Key, kvrpcpb::Assertion) {
    fn key(self) -> Key {
        self.0
    }

    fn assertion(&self) -> kvrpcpb::Assertion {
        self.1
    }
}

pub fn new_pessimistic_lock_request(
    locks: impl Iterator<Item = impl PessimisticLock>,
    primary_lock: Key,
    start_version: Timestamp,
    lock_ttl: u64,
    for_update_ts: Timestamp,
    need_value: bool,
) -> kvrpcpb::PessimisticLockRequest {
    requests::new_pessimistic_lock_request(
        locks
            .map(|pl| {
                let mut mutation = kvrpcpb::Mutation::default();
                mutation.op = kvrpcpb::Op::PessimisticLock.into();
                mutation.assertion = pl.assertion().into();
                mutation.key = pl.key().into();
                mutation
            })
            .collect(),
        primary_lock.into(),
        start_version.version(),
        lock_ttl,
        for_update_ts.version(),
        need_value,
    )
}

pub fn new_scan_lock_request(
    range: BoundRange,
    safepoint: &Timestamp,
    limit: u32,
) -> kvrpcpb::ScanLockRequest {
    let (start_key, end_key) = range.into_keys();
    requests::new_scan_lock_request(
        start_key.into(),
        end_key.unwrap_or_default().into(),
        safepoint.version(),
        limit,
    )
}

pub fn new_heart_beat_request(
    start_ts: Timestamp,
    primary_lock: Key,
    ttl: u64,
) -> kvrpcpb::TxnHeartBeatRequest {
    requests::new_heart_beat_request(start_ts.version(), primary_lock.into(), ttl)
}

pub fn new_unsafe_destroy_range_request(range: BoundRange) -> kvrpcpb::UnsafeDestroyRangeRequest {
    let (start_key, end_key) = range.into_keys();
    requests::new_unsafe_destroy_range_request(start_key.into(), end_key.unwrap_or_default().into())
}

pub fn new_delete_range_request(range: BoundRange) -> kvrpcpb::DeleteRangeRequest {
    let (start_key, end_key) = range.into_keys();
    requests::new_delete_range_request(start_key.into(), end_key.unwrap_or_default().into())
}

pub fn new_prepare_flashback_to_version_request(
    range: BoundRange,
    start_ts: Timestamp,
    version: Timestamp,
) -> kvrpcpb::PrepareFlashbackToVersionRequest {
    let (start_key, end_key) = range.into_keys();
    requests::new_prepare_flashback_to_version_request(
        start_key.into(),
        end_key.unwrap_or_default().into(),
        start_ts.version(),
        version.version(),
    )
}

pub fn new_flashback_to_version_request(
    range: BoundRange,
    version: Timestamp,
    start_ts: Timestamp,
    commit_ts: Timestamp,
) -> kvrpcpb::FlashbackToVersionRequest {
    let (start_key, end_key) = range.into_keys();
    requests::new_flashback_to_version_request(
        start_key.into(),
        end_key.unwrap_or_default().into(),
        version.version(),
        start_ts.version(),
        commit_ts.version(),
    )
}

pub fn new_flush_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_key: Key,
    start_ts: Timestamp,
    min_commit_ts: Timestamp,
    generation: u64,
    lock_ttl: u64,
) -> kvrpcpb::FlushRequest {
    requests::new_flush_request(
        mutations,
        primary_key.into(),
        start_ts.version(),
        min_commit_ts.version(),
        generation,
        lock_ttl,
    )
}

pub fn new_buffer_batch_get_request(
    keys: impl Iterator<Item = Key>,
    version: Timestamp,
) -> kvrpcpb::BufferBatchGetRequest {
    requests::new_buffer_batch_get_request(keys.map(Into::into).collect(), version.version())
}

pub fn new_physical_scan_lock_request(
    max_ts: Timestamp,
    start_key: Key,
    limit: u32,
) -> kvrpcpb::PhysicalScanLockRequest {
    requests::new_physical_scan_lock_request(max_ts.version(), start_key.into(), limit)
}

pub fn new_mvcc_get_by_key_request(key: Key) -> kvrpcpb::MvccGetByKeyRequest {
    requests::new_mvcc_get_by_key_request(key.into())
}

pub fn new_mvcc_get_by_start_ts_request(start_ts: Timestamp) -> kvrpcpb::MvccGetByStartTsRequest {
    requests::new_mvcc_get_by_start_ts_request(start_ts.version())
}

pub fn new_check_lock_observer_request(max_ts: Timestamp) -> kvrpcpb::CheckLockObserverRequest {
    requests::new_check_lock_observer_request(max_ts.version())
}

pub fn new_get_lock_wait_info_request() -> kvrpcpb::GetLockWaitInfoRequest {
    requests::new_get_lock_wait_info_request()
}

pub fn new_split_region_request(
    split_keys: impl Iterator<Item = Key>,
    is_raw_kv: bool,
) -> kvrpcpb::SplitRegionRequest {
    requests::new_split_region_request(split_keys.map(Into::into).collect(), is_raw_kv)
}

pub fn new_store_safe_ts_request(range: Option<BoundRange>) -> kvrpcpb::StoreSafeTsRequest {
    let key_range = range.map(|range| {
        let (start_key, end_key) = range.into_keys();
        kvrpcpb::KeyRange {
            start_key: start_key.into(),
            end_key: end_key.unwrap_or_default().into(),
        }
    });
    requests::new_store_safe_ts_request(key_range)
}
