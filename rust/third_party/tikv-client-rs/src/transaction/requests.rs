// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::iter;
use std::sync::Arc;

use either::Either;
use futures::stream::BoxStream;
use futures::stream::{self};
use futures::StreamExt;

use super::transaction::TXN_COMMIT_BATCH_SIZE;
use crate::collect_single;
use crate::common::Error::PessimisticLockError;
use crate::pd::PdClient;
use crate::proto::kvrpcpb::Action;
use crate::proto::kvrpcpb::LockInfo;
use crate::proto::kvrpcpb::TxnHeartBeatResponse;
use crate::proto::kvrpcpb::TxnInfo;
use crate::proto::kvrpcpb::{self};
use crate::proto::pdpb::Timestamp;
use crate::range_request;
use crate::region::RegionWithLeader;
use crate::request::Collect;
use crate::request::CollectSingle;
use crate::request::CollectWithShard;
use crate::request::DefaultProcessor;
use crate::request::HasNextBatch;
use crate::request::KvRequest;
use crate::request::Merge;
use crate::request::NextBatch;
use crate::request::Process;
use crate::request::RangeRequest;
use crate::request::ResponseWithShard;
use crate::request::Shardable;
use crate::request::SingleKey;
use crate::request::{Batchable, StoreRequest};
use crate::reversible_range_request;
use crate::shardable_key;
use crate::shardable_keys;
use crate::shardable_range;
use crate::store::RegionStore;
use crate::store::Request;
use crate::store::Store;
use crate::store::{region_stream_for_keys, region_stream_for_range};
use crate::timestamp::TimestampExt;
use crate::transaction::requests::kvrpcpb::prewrite_request::PessimisticAction;
use crate::transaction::HasLocks;
use crate::util::iter::FlatMapOkIterExt;
use crate::KvPair;
use crate::Result;
use crate::Value;

// implement HasLocks for a response type that has a `pairs` field,
// where locks can be extracted from both the `pairs` and `error` fields
macro_rules! pair_locks {
    ($response_type:ty) => {
        impl HasLocks for $response_type {
            fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
                if self.pairs.is_empty() {
                    self.error
                        .as_mut()
                        .and_then(|error| error.locked.take())
                        .into_iter()
                        .collect()
                } else {
                    self.pairs
                        .iter_mut()
                        .filter_map(|pair| {
                            pair.error.as_mut().and_then(|error| error.locked.take())
                        })
                        .collect()
                }
            }
        }
    };
}

// implement HasLocks for a response type that does not have a `pairs` field,
// where locks are only extracted from the `error` field
macro_rules! error_locks {
    ($response_type:ty) => {
        impl HasLocks for $response_type {
            fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
                self.error
                    .as_mut()
                    .and_then(|error| error.locked.take())
                    .into_iter()
                    .collect()
            }
        }
    };
}

macro_rules! impl_txn_v2_response {
    ($request:ty, $response:ty, $decode:expr) => {
        impl KvRequest for $request {
            type Response = $response;

            fn key_mode(&self) -> Option<crate::request::KeyMode> {
                Some(crate::request::KeyMode::Txn)
            }

            fn decode_response(
                &self,
                response: &mut Self::Response,
                codec: Option<&crate::request::ApiV2Codec>,
            ) -> Result<()> {
                let Some(codec) = codec else {
                    return Ok(());
                };
                $decode(codec, response)
            }

            fn decode_v1_response(
                &self,
                response: &mut Self::Response,
                codec: Option<&crate::request::ApiV1Codec>,
            ) -> Result<()> {
                if let (Some(codec), Some(region_error)) = (codec, &mut response.region_error) {
                    codec.decode_region_error(region_error)?;
                }
                Ok(())
            }
        }
    };
}

/// API V2-only commands added after the V1 response-codec command matrix.
///
/// `codec_v1.go` intentionally leaves these responses untouched, even when
/// they carry a `region_error`; do not reuse `impl_txn_v2_response` here.
macro_rules! impl_txn_v2_only_response {
    ($request:ty, $response:ty, $decode:expr) => {
        impl KvRequest for $request {
            type Response = $response;

            fn key_mode(&self) -> Option<crate::request::KeyMode> {
                Some(crate::request::KeyMode::Txn)
            }

            fn decode_response(
                &self,
                response: &mut Self::Response,
                codec: Option<&crate::request::ApiV2Codec>,
            ) -> Result<()> {
                let Some(codec) = codec else {
                    return Ok(());
                };
                $decode(codec, response)
            }
        }
    };
}

pub fn new_get_request(key: Vec<u8>, timestamp: u64) -> kvrpcpb::GetRequest {
    let mut req = kvrpcpb::GetRequest::default();
    req.key = key;
    req.version = timestamp;
    req
}

impl KvRequest for kvrpcpb::GetRequest {
    type Response = kvrpcpb::GetResponse;

    fn key_mode(&self) -> Option<crate::request::KeyMode> {
        Some(crate::request::KeyMode::Txn)
    }

    fn decode_response(
        &self,
        response: &mut Self::Response,
        codec: Option<&crate::request::ApiV2Codec>,
    ) -> Result<()> {
        let Some(codec) = codec else {
            return Ok(());
        };
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        if let Some(key_error) = &mut response.error {
            codec.decode_key_error(key_error)?;
        }
        Ok(())
    }

    fn decode_v1_response(
        &self,
        response: &mut Self::Response,
        codec: Option<&crate::request::ApiV1Codec>,
    ) -> Result<()> {
        if let (Some(codec), Some(region_error)) = (codec, &mut response.region_error) {
            codec.decode_region_error(region_error)?;
        }
        Ok(())
    }
}

shardable_key!(kvrpcpb::GetRequest);
collect_single!(kvrpcpb::GetResponse);
impl SingleKey for kvrpcpb::GetRequest {
    fn key(&self) -> &Vec<u8> {
        &self.key
    }
}

impl Process<kvrpcpb::GetResponse> for DefaultProcessor {
    type Out = Option<Value>;

    fn process(&self, input: Result<kvrpcpb::GetResponse>) -> Result<Self::Out> {
        let input = input?;
        Ok(if input.not_found {
            None
        } else {
            Some(input.value)
        })
    }
}

impl_txn_v2_response!(
    kvrpcpb::CleanupRequest,
    kvrpcpb::CleanupResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::CleanupResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        if let Some(error) = &mut response.error {
            codec.decode_key_error(error)?;
        }
        Ok(())
    }
);

shardable_key!(kvrpcpb::CleanupRequest);
collect_single!(kvrpcpb::CleanupResponse);
error_locks!(kvrpcpb::CleanupResponse);

pub fn new_batch_get_request(keys: Vec<Vec<u8>>, timestamp: u64) -> kvrpcpb::BatchGetRequest {
    let mut req = kvrpcpb::BatchGetRequest::default();
    req.keys = keys;
    req.version = timestamp;
    req
}

impl_txn_v2_response!(
    kvrpcpb::BatchGetRequest,
    kvrpcpb::BatchGetResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::BatchGetResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        codec.decode_pairs(&mut response.pairs)?;
        if let Some(error) = &mut response.error {
            codec.decode_key_error(error)?;
        }
        Ok(())
    }
);

shardable_keys!(kvrpcpb::BatchGetRequest);

impl Merge<kvrpcpb::BatchGetResponse> for Collect {
    type Out = Vec<KvPair>;

    fn merge(&self, input: Vec<Result<kvrpcpb::BatchGetResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .flat_map_ok(|resp| resp.pairs.into_iter().map(Into::into))
            .collect()
    }
}

pub fn new_scan_request(
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    timestamp: u64,
    limit: u32,
    key_only: bool,
    reverse: bool,
) -> kvrpcpb::ScanRequest {
    let mut req = kvrpcpb::ScanRequest::default();
    if !reverse {
        req.start_key = start_key;
        req.end_key = end_key;
    } else {
        req.start_key = end_key;
        req.end_key = start_key;
    }
    req.limit = limit;
    req.key_only = key_only;
    req.version = timestamp;
    req.reverse = reverse;
    req
}

impl_txn_v2_response!(
    kvrpcpb::ScanRequest,
    kvrpcpb::ScanResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::ScanResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        codec.decode_pairs(&mut response.pairs)?;
        if let Some(error) = &mut response.error {
            codec.decode_key_error(error)?;
        }
        Ok(())
    }
);

reversible_range_request!(kvrpcpb::ScanRequest);
shardable_range!(kvrpcpb::ScanRequest);

impl Merge<kvrpcpb::ScanResponse> for Collect {
    type Out = Vec<KvPair>;

    fn merge(&self, input: Vec<Result<kvrpcpb::ScanResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .flat_map_ok(|resp| resp.pairs.into_iter().map(Into::into))
            .collect()
    }
}

pub fn new_resolve_lock_request(
    start_version: u64,
    commit_version: u64,
    is_txn_file: bool,
) -> kvrpcpb::ResolveLockRequest {
    let mut req = kvrpcpb::ResolveLockRequest::default();
    req.start_version = start_version;
    req.commit_version = commit_version;
    req.is_txn_file = is_txn_file;
    req
}

pub fn new_batch_resolve_lock_request(txn_infos: Vec<TxnInfo>) -> kvrpcpb::ResolveLockRequest {
    let mut req = kvrpcpb::ResolveLockRequest::default();
    req.txn_infos = txn_infos;
    req
}

// Note: ResolveLockRequest is a special one: it can be sent to a specified
// region without keys. So it's not Shardable. And we don't automatically retry
// on its region errors (in the Plan level). The region error must be manually
// handled (in the upper level).
impl_txn_v2_response!(
    kvrpcpb::ResolveLockRequest,
    kvrpcpb::ResolveLockResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::ResolveLockResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        if let Some(error) = &mut response.error {
            codec.decode_key_error(error)?;
        }
        Ok(())
    }
);

pub fn new_prewrite_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_lock: Vec<u8>,
    start_version: u64,
    lock_ttl: u64,
) -> kvrpcpb::PrewriteRequest {
    let mut req = kvrpcpb::PrewriteRequest::default();
    req.mutations = mutations;
    req.primary_lock = primary_lock;
    req.start_version = start_version;
    req.lock_ttl = lock_ttl;
    // FIXME: Lite resolve lock is currently disabled
    req.txn_size = u64::MAX;

    req
}

pub fn new_pessimistic_prewrite_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_lock: Vec<u8>,
    start_version: u64,
    lock_ttl: u64,
    for_update_ts: u64,
) -> kvrpcpb::PrewriteRequest {
    let len = mutations.len();
    let mut req = new_prewrite_request(mutations, primary_lock, start_version, lock_ttl);
    req.for_update_ts = for_update_ts;
    req.pessimistic_actions =
        iter::repeat_n(PessimisticAction::DoPessimisticCheck.into(), len).collect();
    req
}

impl_txn_v2_response!(
    kvrpcpb::PrewriteRequest,
    kvrpcpb::PrewriteResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::PrewriteResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        codec.decode_key_errors(&mut response.errors)
    }
);

impl Shardable for kvrpcpb::PrewriteRequest {
    type Shard = Vec<kvrpcpb::Mutation>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let mut mutations = self.mutations.clone();
        mutations.sort_by(|a, b| a.key.cmp(&b.key));

        region_stream_for_keys(mutations.into_iter(), pd_client.clone())
            .flat_map(|result| match result {
                Ok((mutations, region)) => stream::iter(kvrpcpb::PrewriteRequest::batches(
                    mutations,
                    TXN_COMMIT_BATCH_SIZE,
                ))
                .map(move |batch| Ok((batch, region.clone())))
                .boxed(),
                Err(e) => stream::iter(Err(e)).boxed(),
            })
            .boxed()
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        // Only need to set secondary keys if we're sending the primary key.
        if self.use_async_commit && !self.mutations.iter().any(|m| m.key == self.primary_lock) {
            self.secondaries = vec![];
        }

        // Only if there is only one request to send
        if self.try_one_pc && shard.len() != self.secondaries.len() + 1 {
            self.try_one_pc = false;
        }

        self.mutations = shard;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.request_region())?;
        self.set_replica_read(store.is_replica_read());
        self.set_stale_read(store.stale_read);
        Ok(())
    }
}

impl Batchable for kvrpcpb::PrewriteRequest {
    type Item = kvrpcpb::Mutation;

    fn item_size(item: &Self::Item) -> u64 {
        let mut size = item.key.len() as u64;
        size += item.value.len() as u64;
        size
    }
}

pub fn new_commit_request(
    keys: Vec<Vec<u8>>,
    start_version: u64,
    commit_version: u64,
) -> kvrpcpb::CommitRequest {
    let mut req = kvrpcpb::CommitRequest::default();
    req.keys = keys;
    req.start_version = start_version;
    req.commit_version = commit_version;

    req
}

impl_txn_v2_response!(
    kvrpcpb::CommitRequest,
    kvrpcpb::CommitResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::CommitResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        if let Some(error) = &mut response.error {
            codec.decode_key_error(error)?;
        }
        Ok(())
    }
);

impl Shardable for kvrpcpb::CommitRequest {
    type Shard = Vec<Vec<u8>>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let mut keys = self.keys.clone();
        keys.sort();

        region_stream_for_keys(keys.into_iter(), pd_client.clone())
            .flat_map(|result| match result {
                Ok((keys, region)) => {
                    stream::iter(kvrpcpb::CommitRequest::batches(keys, TXN_COMMIT_BATCH_SIZE))
                        .map(move |batch| Ok((batch, region.clone())))
                        .boxed()
                }
                Err(e) => stream::iter(Err(e)).boxed(),
            })
            .boxed()
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.keys = shard;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.request_region())?;
        self.set_replica_read(store.is_replica_read());
        self.set_stale_read(store.stale_read);
        Ok(())
    }
}

impl Batchable for kvrpcpb::CommitRequest {
    type Item = Vec<u8>;

    fn item_size(item: &Self::Item) -> u64 {
        item.len() as u64
    }
}

pub fn new_batch_rollback_request(
    keys: Vec<Vec<u8>>,
    start_version: u64,
) -> kvrpcpb::BatchRollbackRequest {
    let mut req = kvrpcpb::BatchRollbackRequest::default();
    req.keys = keys;
    req.start_version = start_version;

    req
}

impl_txn_v2_response!(
    kvrpcpb::BatchRollbackRequest,
    kvrpcpb::BatchRollbackResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::BatchRollbackResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        if let Some(error) = &mut response.error {
            codec.decode_key_error(error)?;
        }
        Ok(())
    }
);

shardable_keys!(kvrpcpb::BatchRollbackRequest);

pub fn new_pessimistic_rollback_request(
    keys: Vec<Vec<u8>>,
    start_version: u64,
    for_update_ts: u64,
) -> kvrpcpb::PessimisticRollbackRequest {
    let mut req = kvrpcpb::PessimisticRollbackRequest::default();
    req.keys = keys;
    req.start_version = start_version;
    req.for_update_ts = for_update_ts;

    req
}

impl_txn_v2_response!(
    kvrpcpb::PessimisticRollbackRequest,
    kvrpcpb::PessimisticRollbackResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::PessimisticRollbackResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        codec.decode_key_errors(&mut response.errors)
    }
);

shardable_keys!(kvrpcpb::PessimisticRollbackRequest);

pub fn new_pessimistic_lock_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_lock: Vec<u8>,
    start_version: u64,
    lock_ttl: u64,
    for_update_ts: u64,
    need_value: bool,
) -> kvrpcpb::PessimisticLockRequest {
    let mut req = kvrpcpb::PessimisticLockRequest::default();
    req.mutations = mutations;
    req.primary_lock = primary_lock;
    req.start_version = start_version;
    req.lock_ttl = lock_ttl;
    req.for_update_ts = for_update_ts;
    // FIXME: make them configurable
    req.is_first_lock = false;
    req.wait_timeout = 0;
    req.return_values = need_value;
    // FIXME: support large transaction
    req.min_commit_ts = 0;

    req
}

impl_txn_v2_response!(
    kvrpcpb::PessimisticLockRequest,
    kvrpcpb::PessimisticLockResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::PessimisticLockResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        codec.decode_key_errors(&mut response.errors)
    }
);

impl Shardable for kvrpcpb::PessimisticLockRequest {
    type Shard = Vec<kvrpcpb::Mutation>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let mut mutations = self.mutations.clone();
        mutations.sort_by(|a, b| a.key.cmp(&b.key));
        region_stream_for_keys(mutations.into_iter(), pd_client.clone())
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.mutations = shard;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.request_region())?;
        self.set_replica_read(store.is_replica_read());
        self.set_stale_read(store.stale_read);
        Ok(())
    }
}

// PessimisticLockResponse returns values that preserves the order with keys in request, thus the
// kvpair result should be produced by zipping the keys in request and the values in respponse.
impl Merge<ResponseWithShard<kvrpcpb::PessimisticLockResponse, Vec<kvrpcpb::Mutation>>>
    for CollectWithShard
{
    type Out = Vec<KvPair>;

    fn merge(
        &self,
        input: Vec<
            Result<ResponseWithShard<kvrpcpb::PessimisticLockResponse, Vec<kvrpcpb::Mutation>>>,
        >,
    ) -> Result<Self::Out> {
        if input.iter().any(Result::is_err) {
            let (success, mut errors): (Vec<_>, Vec<_>) =
                input.into_iter().partition(Result::is_ok);
            let first_err = errors.pop().unwrap();
            let success_keys = success
                .into_iter()
                .map(Result::unwrap)
                .flat_map(|ResponseWithShard(_resp, mutations)| {
                    mutations.into_iter().map(|m| m.key)
                })
                .collect();
            Err(PessimisticLockError {
                inner: Box::new(first_err.unwrap_err()),
                success_keys,
            })
        } else {
            Ok(input
                .into_iter()
                .map(Result::unwrap)
                .flat_map(|ResponseWithShard(resp, mutations)| {
                    let values: Vec<Vec<u8>> = resp.values;
                    let values_len = values.len();
                    let not_founds = resp.not_founds;
                    let kvpairs = mutations
                        .into_iter()
                        .map(|m| m.key)
                        .zip(values)
                        .map(KvPair::from);
                    assert_eq!(kvpairs.len(), values_len);
                    if not_founds.is_empty() {
                        // Legacy TiKV does not distinguish not existing key and existing key
                        // that with empty value. We assume that key does not exist if value
                        // is empty.
                        Either::Left(kvpairs.filter(|kvpair| !kvpair.value().is_empty()))
                    } else {
                        assert_eq!(kvpairs.len(), not_founds.len());
                        Either::Right(kvpairs.zip(not_founds).filter_map(|(kvpair, not_found)| {
                            if not_found {
                                None
                            } else {
                                Some(kvpair)
                            }
                        }))
                    }
                })
                .collect())
        }
    }
}

pub fn new_scan_lock_request(
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    safepoint: u64,
    limit: u32,
) -> kvrpcpb::ScanLockRequest {
    let mut req = kvrpcpb::ScanLockRequest::default();
    req.start_key = start_key;
    req.end_key = end_key;
    req.max_version = safepoint;
    req.limit = limit;
    req
}

impl_txn_v2_response!(
    kvrpcpb::ScanLockRequest,
    kvrpcpb::ScanLockResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::ScanLockResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        if let Some(error) = &mut response.error {
            codec.decode_key_error(error)?;
        }
        for lock in &mut response.locks {
            codec.decode_lock_info(lock)?;
        }
        Ok(())
    }
);

impl Shardable for kvrpcpb::ScanLockRequest {
    type Shard = (Vec<u8>, Vec<u8>);

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        region_stream_for_range(
            (self.start_key.clone(), self.end_key.clone()),
            pd_client.clone(),
        )
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.start_key = shard.0;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.request_region())?;
        self.set_replica_read(store.is_replica_read());
        self.set_stale_read(store.stale_read);
        Ok(())
    }
}

impl HasNextBatch for kvrpcpb::ScanLockResponse {
    fn has_next_batch(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.locks.last().map(|lock| {
            // TODO: if last key is larger or equal than ScanLockRequest.end_key, return None.
            let mut start_key: Vec<u8> = lock.key.clone();
            start_key.push(0);
            (start_key, vec![])
        })
    }
}

impl NextBatch for kvrpcpb::ScanLockRequest {
    fn next_batch(&mut self, range: (Vec<u8>, Vec<u8>)) {
        self.start_key = range.0;
    }
}

impl Merge<kvrpcpb::ScanLockResponse> for Collect {
    type Out = Vec<kvrpcpb::LockInfo>;

    fn merge(&self, input: Vec<Result<kvrpcpb::ScanLockResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .flat_map_ok(|mut resp| resp.take_locks().into_iter())
            .collect()
    }
}

pub fn new_heart_beat_request(
    start_ts: u64,
    primary_lock: Vec<u8>,
    ttl: u64,
) -> kvrpcpb::TxnHeartBeatRequest {
    let mut req = kvrpcpb::TxnHeartBeatRequest::default();
    req.start_version = start_ts;
    req.primary_lock = primary_lock;
    req.advise_lock_ttl = ttl;
    req
}

impl_txn_v2_response!(
    kvrpcpb::TxnHeartBeatRequest,
    kvrpcpb::TxnHeartBeatResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::TxnHeartBeatResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        if let Some(error) = &mut response.error {
            codec.decode_key_error(error)?;
        }
        Ok(())
    }
);

impl Shardable for kvrpcpb::TxnHeartBeatRequest {
    type Shard = Vec<Vec<u8>>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        region_stream_for_keys(std::iter::once(self.key().clone()), pd_client.clone())
    }

    fn apply_shard(&mut self, mut shard: Self::Shard) {
        assert!(shard.len() == 1);
        self.primary_lock = shard.pop().unwrap();
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.request_region())?;
        self.set_replica_read(store.is_replica_read());
        self.set_stale_read(store.stale_read);
        Ok(())
    }
}

collect_single!(TxnHeartBeatResponse);

impl SingleKey for kvrpcpb::TxnHeartBeatRequest {
    fn key(&self) -> &Vec<u8> {
        &self.primary_lock
    }
}

impl Process<kvrpcpb::TxnHeartBeatResponse> for DefaultProcessor {
    type Out = u64;

    fn process(&self, input: Result<kvrpcpb::TxnHeartBeatResponse>) -> Result<Self::Out> {
        Ok(input?.lock_ttl)
    }
}

#[allow(clippy::too_many_arguments)]
pub fn new_check_txn_status_request(
    primary_key: Vec<u8>,
    lock_ts: u64,
    caller_start_ts: u64,
    current_ts: u64,
    rollback_if_not_exist: bool,
    force_sync_commit: bool,
    resolving_pessimistic_lock: bool,
    is_txn_file: bool,
) -> kvrpcpb::CheckTxnStatusRequest {
    let mut req = kvrpcpb::CheckTxnStatusRequest::default();
    req.primary_key = primary_key;
    req.lock_ts = lock_ts;
    req.caller_start_ts = caller_start_ts;
    req.current_ts = current_ts;
    req.rollback_if_not_exist = rollback_if_not_exist;
    req.force_sync_commit = force_sync_commit;
    req.resolving_pessimistic_lock = resolving_pessimistic_lock;
    req.verify_is_primary = true;
    req.is_txn_file = is_txn_file;
    req
}

impl_txn_v2_response!(
    kvrpcpb::CheckTxnStatusRequest,
    kvrpcpb::CheckTxnStatusResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::CheckTxnStatusResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        if let Some(error) = &mut response.error {
            codec.decode_key_error(error)?;
        }
        if let Some(lock) = &mut response.lock_info {
            codec.decode_lock_info(lock)?;
        }
        Ok(())
    }
);

impl Shardable for kvrpcpb::CheckTxnStatusRequest {
    type Shard = Vec<Vec<u8>>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        region_stream_for_keys(std::iter::once(self.key().clone()), pd_client.clone())
    }

    fn apply_shard(&mut self, mut shard: Self::Shard) {
        assert!(shard.len() == 1);
        self.primary_key = shard.pop().unwrap();
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.request_region())?;
        self.set_replica_read(store.is_replica_read());
        self.set_stale_read(store.stale_read);
        Ok(())
    }
}

impl SingleKey for kvrpcpb::CheckTxnStatusRequest {
    fn key(&self) -> &Vec<u8> {
        &self.primary_key
    }
}

collect_single!(kvrpcpb::CheckTxnStatusResponse);

impl Process<kvrpcpb::CheckTxnStatusResponse> for DefaultProcessor {
    type Out = TransactionStatus;

    fn process(&self, input: Result<kvrpcpb::CheckTxnStatusResponse>) -> Result<Self::Out> {
        Ok(input?.into())
    }
}

#[derive(Debug, Clone)]
pub struct TransactionStatus {
    pub kind: TransactionStatusKind,
    pub action: kvrpcpb::Action,
    pub is_expired: bool, // Available only when kind is Locked.
}

impl From<kvrpcpb::CheckTxnStatusResponse> for TransactionStatus {
    fn from(mut resp: kvrpcpb::CheckTxnStatusResponse) -> TransactionStatus {
        TransactionStatus {
            action: Action::try_from(resp.action).unwrap(),
            kind: (resp.commit_version, resp.lock_ttl, resp.lock_info.take()).into(),
            is_expired: false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TransactionStatusKind {
    Committed(Timestamp),
    RolledBack,
    Locked(u64, kvrpcpb::LockInfo), // None of ttl means expired.
}

impl TransactionStatus {
    pub fn check_ttl(&mut self, current: Timestamp) {
        if let TransactionStatusKind::Locked(ref ttl, ref lock_info) = self.kind {
            if current.physical - Timestamp::from_version(lock_info.lock_version).physical
                >= *ttl as i64
            {
                self.is_expired = true
            }
        }
    }

    // is_cacheable checks whether the transaction status is certain.
    // If transaction is already committed, the result could be cached.
    // Otherwise:
    //   If l.LockType is pessimistic lock type:
    //       - if its primary lock is pessimistic too, the check txn status result should not be cached.
    //       - if its primary lock is prewrite lock type, the check txn status could be cached.
    //   If l.lockType is prewrite lock type:
    //       - always cache the check txn status result.
    // For prewrite locks, their primary keys should ALWAYS be the correct one and will NOT change.
    pub fn is_cacheable(&self) -> bool {
        match &self.kind {
            TransactionStatusKind::RolledBack | TransactionStatusKind::Committed(..) => true,
            TransactionStatusKind::Locked(..) if self.is_expired => matches!(
                self.action,
                kvrpcpb::Action::NoAction
                    | kvrpcpb::Action::LockNotExistRollback
                    | kvrpcpb::Action::TtlExpireRollback
            ),
            _ => false,
        }
    }
}

impl From<(u64, u64, Option<kvrpcpb::LockInfo>)> for TransactionStatusKind {
    fn from((ts, ttl, info): (u64, u64, Option<kvrpcpb::LockInfo>)) -> TransactionStatusKind {
        match (ts, ttl, info) {
            (0, 0, None) => TransactionStatusKind::RolledBack,
            (ts, 0, None) => TransactionStatusKind::Committed(Timestamp::from_version(ts)),
            (0, ttl, Some(info)) => TransactionStatusKind::Locked(ttl, info),
            _ => unreachable!(),
        }
    }
}

pub fn new_check_secondary_locks_request(
    keys: Vec<Vec<u8>>,
    start_version: u64,
) -> kvrpcpb::CheckSecondaryLocksRequest {
    let mut req = kvrpcpb::CheckSecondaryLocksRequest::default();
    req.keys = keys;
    req.start_version = start_version;
    req
}

impl_txn_v2_response!(
    kvrpcpb::CheckSecondaryLocksRequest,
    kvrpcpb::CheckSecondaryLocksResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::CheckSecondaryLocksResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        if let Some(error) = &mut response.error {
            codec.decode_key_error(error)?;
        }
        for lock in &mut response.locks {
            codec.decode_lock_info(lock)?;
        }
        Ok(())
    }
);

shardable_keys!(kvrpcpb::CheckSecondaryLocksRequest);

impl Merge<kvrpcpb::CheckSecondaryLocksResponse> for Collect {
    type Out = SecondaryLocksStatus;

    fn merge(&self, input: Vec<Result<kvrpcpb::CheckSecondaryLocksResponse>>) -> Result<Self::Out> {
        let mut out = SecondaryLocksStatus {
            commit_ts: None,
            min_commit_ts: 0,
            fallback_2pc: false,
        };
        for resp in input {
            let resp = resp?;
            for lock in resp.locks.into_iter() {
                if !lock.use_async_commit {
                    out.fallback_2pc = true;
                    return Ok(out);
                }
                out.min_commit_ts = cmp::max(out.min_commit_ts, lock.min_commit_ts);
            }
            out.commit_ts = match (
                out.commit_ts.take(),
                Timestamp::try_from_version(resp.commit_ts),
            ) {
                (Some(a), Some(b)) => {
                    assert_eq!(a, b);
                    Some(a)
                }
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
        }
        Ok(out)
    }
}

pub struct SecondaryLocksStatus {
    pub commit_ts: Option<Timestamp>,
    pub min_commit_ts: u64,
    pub fallback_2pc: bool,
}

pair_locks!(kvrpcpb::BatchGetResponse);
pair_locks!(kvrpcpb::ScanResponse);
error_locks!(kvrpcpb::GetResponse);
error_locks!(kvrpcpb::ResolveLockResponse);
error_locks!(kvrpcpb::CommitResponse);
error_locks!(kvrpcpb::BatchRollbackResponse);
error_locks!(kvrpcpb::TxnHeartBeatResponse);
error_locks!(kvrpcpb::CheckTxnStatusResponse);
error_locks!(kvrpcpb::CheckSecondaryLocksResponse);

impl HasLocks for kvrpcpb::ScanLockResponse {
    fn take_locks(&mut self) -> Vec<LockInfo> {
        std::mem::take(&mut self.locks)
    }
}

impl HasLocks for kvrpcpb::PessimisticRollbackResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.errors
            .iter_mut()
            .filter_map(|error| error.locked.take())
            .collect()
    }
}

impl HasLocks for kvrpcpb::PessimisticLockResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.errors
            .iter_mut()
            .filter_map(|error| error.locked.take())
            .collect()
    }
}

impl HasLocks for kvrpcpb::PrewriteResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.errors
            .iter_mut()
            .filter_map(|error| error.locked.take())
            .collect()
    }
}

impl_txn_v2_response!(
    kvrpcpb::GcRequest,
    kvrpcpb::GcResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::GcResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        if let Some(error) = &mut response.error {
            codec.decode_key_error(error)?;
        }
        Ok(())
    }
);

impl StoreRequest for kvrpcpb::GcRequest {
    fn apply_store(&mut self, _store: &Store) {}
}

error_locks!(kvrpcpb::GcResponse);

impl Merge<kvrpcpb::GcResponse> for Collect {
    type Out = ();

    fn merge(&self, input: Vec<Result<kvrpcpb::GcResponse>>) -> Result<Self::Out> {
        let _: Vec<kvrpcpb::GcResponse> = input.into_iter().collect::<Result<Vec<_>>>()?;
        Ok(())
    }
}

pub fn new_delete_range_request(
    start_key: Vec<u8>,
    end_key: Vec<u8>,
) -> kvrpcpb::DeleteRangeRequest {
    kvrpcpb::DeleteRangeRequest {
        start_key,
        end_key,
        ..Default::default()
    }
}

impl_txn_v2_response!(
    kvrpcpb::DeleteRangeRequest,
    kvrpcpb::DeleteRangeResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::DeleteRangeResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        Ok(())
    }
);

range_request!(kvrpcpb::DeleteRangeRequest);
shardable_range!(kvrpcpb::DeleteRangeRequest);

impl HasLocks for kvrpcpb::DeleteRangeResponse {}

impl Merge<kvrpcpb::DeleteRangeResponse> for Collect {
    type Out = usize;

    fn merge(&self, input: Vec<Result<kvrpcpb::DeleteRangeResponse>>) -> Result<Self::Out> {
        let responses = input.into_iter().collect::<Result<Vec<_>>>()?;
        for response in &responses {
            if !response.error.is_empty() {
                return Err(crate::Error::StringError(format!(
                    "unexpected delete range err: {}",
                    response.error
                )));
            }
        }
        Ok(responses.len())
    }
}

pub fn new_prepare_flashback_to_version_request(
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    start_ts: u64,
    version: u64,
) -> kvrpcpb::PrepareFlashbackToVersionRequest {
    kvrpcpb::PrepareFlashbackToVersionRequest {
        start_key,
        end_key,
        start_ts,
        version,
        ..Default::default()
    }
}

impl_txn_v2_only_response!(
    kvrpcpb::PrepareFlashbackToVersionRequest,
    kvrpcpb::PrepareFlashbackToVersionResponse,
    |codec: &crate::request::ApiV2Codec,
     response: &mut kvrpcpb::PrepareFlashbackToVersionResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        Ok(())
    }
);

range_request!(kvrpcpb::PrepareFlashbackToVersionRequest);
shardable_range!(kvrpcpb::PrepareFlashbackToVersionRequest);
impl HasLocks for kvrpcpb::PrepareFlashbackToVersionResponse {}

impl Merge<kvrpcpb::PrepareFlashbackToVersionResponse> for Collect {
    type Out = usize;

    fn merge(
        &self,
        input: Vec<Result<kvrpcpb::PrepareFlashbackToVersionResponse>>,
    ) -> Result<Self::Out> {
        input
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .map(|responses| responses.len())
    }
}

pub fn new_flashback_to_version_request(
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    version: u64,
    start_ts: u64,
    commit_ts: u64,
) -> kvrpcpb::FlashbackToVersionRequest {
    kvrpcpb::FlashbackToVersionRequest {
        start_key,
        end_key,
        version,
        start_ts,
        commit_ts,
        ..Default::default()
    }
}

impl_txn_v2_only_response!(
    kvrpcpb::FlashbackToVersionRequest,
    kvrpcpb::FlashbackToVersionResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::FlashbackToVersionResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        Ok(())
    }
);

range_request!(kvrpcpb::FlashbackToVersionRequest);
shardable_range!(kvrpcpb::FlashbackToVersionRequest);
impl HasLocks for kvrpcpb::FlashbackToVersionResponse {}

impl Merge<kvrpcpb::FlashbackToVersionResponse> for Collect {
    type Out = usize;

    fn merge(&self, input: Vec<Result<kvrpcpb::FlashbackToVersionResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .map(|responses| responses.len())
    }
}

pub fn new_flush_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_key: Vec<u8>,
    start_ts: u64,
    min_commit_ts: u64,
    generation: u64,
    lock_ttl: u64,
) -> kvrpcpb::FlushRequest {
    kvrpcpb::FlushRequest {
        mutations,
        primary_key,
        start_ts,
        min_commit_ts,
        generation,
        lock_ttl,
        ..Default::default()
    }
}

impl_txn_v2_only_response!(
    kvrpcpb::FlushRequest,
    kvrpcpb::FlushResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::FlushResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        codec.decode_key_errors(&mut response.errors)
    }
);

impl Shardable for kvrpcpb::FlushRequest {
    type Shard = Vec<kvrpcpb::Mutation>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let mut mutations = self.mutations.clone();
        mutations.sort_by(|a, b| a.key.cmp(&b.key));
        region_stream_for_keys(mutations.into_iter(), pd_client.clone())
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.mutations = shard;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.request_region())?;
        self.set_replica_read(store.is_replica_read());
        self.set_stale_read(store.stale_read);
        Ok(())
    }
}

impl HasLocks for kvrpcpb::FlushResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.errors
            .iter_mut()
            .filter_map(|error| error.locked.take())
            .collect()
    }
}

pub fn new_buffer_batch_get_request(
    keys: Vec<Vec<u8>>,
    version: u64,
) -> kvrpcpb::BufferBatchGetRequest {
    kvrpcpb::BufferBatchGetRequest {
        keys,
        version,
        ..Default::default()
    }
}

impl_txn_v2_only_response!(
    kvrpcpb::BufferBatchGetRequest,
    kvrpcpb::BufferBatchGetResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::BufferBatchGetResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        codec.decode_pairs(&mut response.pairs)?;
        if let Some(error) = &mut response.error {
            codec.decode_key_error(error)?;
        }
        Ok(())
    }
);

shardable_keys!(kvrpcpb::BufferBatchGetRequest);
pair_locks!(kvrpcpb::BufferBatchGetResponse);

impl Merge<kvrpcpb::BufferBatchGetResponse> for Collect {
    type Out = Vec<KvPair>;

    fn merge(&self, input: Vec<Result<kvrpcpb::BufferBatchGetResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .flat_map_ok(|response| response.pairs.into_iter().map(Into::into))
            .collect()
    }
}

pub fn new_physical_scan_lock_request(
    max_ts: u64,
    start_key: Vec<u8>,
    limit: u32,
) -> kvrpcpb::PhysicalScanLockRequest {
    kvrpcpb::PhysicalScanLockRequest {
        max_ts,
        start_key,
        limit,
        ..Default::default()
    }
}

impl KvRequest for kvrpcpb::PhysicalScanLockRequest {
    type Response = kvrpcpb::PhysicalScanLockResponse;

    fn key_mode(&self) -> Option<crate::request::KeyMode> {
        Some(crate::request::KeyMode::Txn)
    }

    fn decode_response(
        &self,
        response: &mut Self::Response,
        codec: Option<&crate::request::ApiV2Codec>,
    ) -> Result<()> {
        let Some(codec) = codec else {
            return Ok(());
        };
        for lock in &mut response.locks {
            codec.decode_lock_info(lock)?;
        }
        Ok(())
    }
}

impl HasLocks for kvrpcpb::PhysicalScanLockResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        std::mem::take(&mut self.locks)
    }
}

pub fn new_mvcc_get_by_key_request(key: Vec<u8>) -> kvrpcpb::MvccGetByKeyRequest {
    kvrpcpb::MvccGetByKeyRequest {
        key,
        ..Default::default()
    }
}

impl_txn_v2_only_response!(
    kvrpcpb::MvccGetByKeyRequest,
    kvrpcpb::MvccGetByKeyResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::MvccGetByKeyResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        if let Some(info) = &mut response.info {
            codec.decode_mvcc_info(info)?;
        }
        Ok(())
    }
);

shardable_key!(kvrpcpb::MvccGetByKeyRequest);
collect_single!(kvrpcpb::MvccGetByKeyResponse);
impl SingleKey for kvrpcpb::MvccGetByKeyRequest {
    fn key(&self) -> &Vec<u8> {
        &self.key
    }
}
impl HasLocks for kvrpcpb::MvccGetByKeyResponse {}

pub fn new_mvcc_get_by_start_ts_request(start_ts: u64) -> kvrpcpb::MvccGetByStartTsRequest {
    kvrpcpb::MvccGetByStartTsRequest {
        start_ts,
        ..Default::default()
    }
}

impl_txn_v2_only_response!(
    kvrpcpb::MvccGetByStartTsRequest,
    kvrpcpb::MvccGetByStartTsResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::MvccGetByStartTsResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        if !response.key.is_empty() {
            response.key = codec.decode_key(&response.key)?;
        }
        if let Some(info) = &mut response.info {
            codec.decode_mvcc_info(info)?;
        }
        Ok(())
    }
);

impl HasLocks for kvrpcpb::MvccGetByStartTsResponse {}

pub fn new_check_lock_observer_request(max_ts: u64) -> kvrpcpb::CheckLockObserverRequest {
    kvrpcpb::CheckLockObserverRequest {
        max_ts,
        ..Default::default()
    }
}

impl KvRequest for kvrpcpb::CheckLockObserverRequest {
    type Response = kvrpcpb::CheckLockObserverResponse;

    fn key_mode(&self) -> Option<crate::request::KeyMode> {
        Some(crate::request::KeyMode::Txn)
    }

    fn decode_response(
        &self,
        response: &mut Self::Response,
        codec: Option<&crate::request::ApiV2Codec>,
    ) -> Result<()> {
        let Some(codec) = codec else {
            return Ok(());
        };
        for lock in &mut response.locks {
            codec.decode_lock_info(lock)?;
        }
        Ok(())
    }
}

impl HasLocks for kvrpcpb::CheckLockObserverResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        std::mem::take(&mut self.locks)
    }
}

pub fn new_get_lock_wait_info_request() -> kvrpcpb::GetLockWaitInfoRequest {
    kvrpcpb::GetLockWaitInfoRequest::default()
}

impl_txn_v2_only_response!(
    kvrpcpb::GetLockWaitInfoRequest,
    kvrpcpb::GetLockWaitInfoResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::GetLockWaitInfoResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        for entry in &mut response.entries {
            entry.key = codec.decode_key(&entry.key)?;
        }
        Ok(())
    }
);

impl HasLocks for kvrpcpb::GetLockWaitInfoResponse {}

pub fn new_split_region_request(
    split_keys: Vec<Vec<u8>>,
    is_raw_kv: bool,
) -> kvrpcpb::SplitRegionRequest {
    kvrpcpb::SplitRegionRequest {
        split_keys,
        is_raw_kv,
        ..Default::default()
    }
}

impl_txn_v2_only_response!(
    kvrpcpb::SplitRegionRequest,
    kvrpcpb::SplitRegionResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::SplitRegionResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        codec.decode_regions(&mut response.regions)?;
        codec.decode_key_errors(&mut response.errors)
    }
);

impl HasLocks for kvrpcpb::SplitRegionResponse {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.errors
            .iter_mut()
            .filter_map(|error| error.locked.take())
            .collect()
    }
}

pub fn new_store_safe_ts_request(
    key_range: Option<kvrpcpb::KeyRange>,
) -> kvrpcpb::StoreSafeTsRequest {
    kvrpcpb::StoreSafeTsRequest { key_range }
}

impl KvRequest for kvrpcpb::StoreSafeTsRequest {
    type Response = kvrpcpb::StoreSafeTsResponse;
}

impl HasLocks for kvrpcpb::StoreSafeTsResponse {}

impl KvRequest for crate::proto::coprocessor::Request {
    type Response = crate::proto::coprocessor::Response;

    fn is_batched_coprocessor_read(&self) -> bool {
        !self.tasks.is_empty()
    }

    fn key_mode(&self) -> Option<crate::request::KeyMode> {
        Some(crate::request::KeyMode::Txn)
    }

    fn decode_response(
        &self,
        response: &mut Self::Response,
        codec: Option<&crate::request::ApiV2Codec>,
    ) -> Result<()> {
        let Some(codec) = codec else {
            return Ok(());
        };
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        if let Some(lock) = &mut response.locked {
            codec.decode_lock_info(lock)?;
        }
        if let Some(range) = &mut response.range {
            codec.decode_cop_range(range)?;
        }
        Ok(())
    }
}

impl HasLocks for crate::proto::coprocessor::Response {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.locked.take().into_iter().collect()
    }
}

pub fn new_unsafe_destroy_range_request(
    start_key: Vec<u8>,
    end_key: Vec<u8>,
) -> kvrpcpb::UnsafeDestroyRangeRequest {
    let mut req = kvrpcpb::UnsafeDestroyRangeRequest::default();
    req.start_key = start_key;
    req.end_key = end_key;
    req
}

impl KvRequest for kvrpcpb::UnsafeDestroyRangeRequest {
    type Response = kvrpcpb::UnsafeDestroyRangeResponse;

    fn key_mode(&self) -> Option<crate::request::KeyMode> {
        Some(crate::request::KeyMode::Txn)
    }

    fn decode_response(
        &self,
        response: &mut Self::Response,
        codec: Option<&crate::request::ApiV2Codec>,
    ) -> Result<()> {
        if let (Some(codec), Some(region_error)) = (codec, &mut response.region_error) {
            codec.decode_region_error(region_error)?;
        }
        Ok(())
    }

    fn decode_v1_response(
        &self,
        response: &mut Self::Response,
        codec: Option<&crate::request::ApiV1Codec>,
    ) -> Result<()> {
        if let (Some(codec), Some(region_error)) = (codec, &mut response.region_error) {
            codec.decode_region_error(region_error)?;
        }
        Ok(())
    }
}

impl StoreRequest for kvrpcpb::UnsafeDestroyRangeRequest {
    fn apply_store(&mut self, _store: &Store) {}
}

impl HasLocks for kvrpcpb::UnsafeDestroyRangeResponse {}

impl Merge<kvrpcpb::UnsafeDestroyRangeResponse> for Collect {
    type Out = ();

    fn merge(&self, input: Vec<Result<kvrpcpb::UnsafeDestroyRangeResponse>>) -> Result<Self::Out> {
        let _: Vec<kvrpcpb::UnsafeDestroyRangeResponse> =
            input.into_iter().collect::<Result<Vec<_>>>()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::common::Error::PessimisticLockError;
    use crate::common::Error::ResolveLockError;
    use crate::proto::errorpb;
    use crate::proto::kvrpcpb;
    use crate::request::plan::Merge;
    use crate::request::Collect;
    use crate::request::CollectWithShard;
    use crate::request::ResponseWithShard;
    use crate::request::{ApiV1Codec, ApiV2Codec, KeyMode, KvRequest};
    use crate::store::Request;
    use crate::KvPair;

    #[test]
    fn source_delete_range_server_error_is_terminal() {
        let error = Collect
            .merge(vec![Ok(kvrpcpb::DeleteRangeResponse {
                error: "delete failed".to_owned(),
                ..Default::default()
            })])
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "unexpected delete range err: delete failed"
        );
    }

    #[test]
    fn api_v2_decoder_runs_before_pessimistic_lock_errors_are_extracted() {
        let codec = ApiV2Codec::new(KeyMode::Txn, 7).unwrap();
        let request = kvrpcpb::PessimisticLockRequest::default();
        let mut response = kvrpcpb::PessimisticLockResponse {
            errors: vec![kvrpcpb::KeyError {
                locked: Some(kvrpcpb::LockInfo {
                    key: codec.encode_key(b"locked"),
                    primary_lock: codec.encode_key(b"primary"),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        request
            .decode_response(&mut response, Some(&codec))
            .unwrap();
        let lock = response.errors[0].locked.as_ref().unwrap();
        assert_eq!(lock.key, b"locked");
        assert_eq!(lock.primary_lock, b"primary");
    }

    #[test]
    fn api_v2_decoder_decodes_transaction_batch_and_scan_pair_keys() {
        let codec = ApiV2Codec::new(KeyMode::Txn, 7).unwrap();
        let batch_get_request = kvrpcpb::BatchGetRequest::default();
        let scan_request = kvrpcpb::ScanRequest::default();
        let pair = kvrpcpb::KvPair {
            key: codec.encode_key(b"key"),
            value: b"value".to_vec(),
            ..Default::default()
        };
        let mut batch_get_response = kvrpcpb::BatchGetResponse {
            pairs: vec![pair.clone()],
            ..Default::default()
        };
        let mut scan_response = kvrpcpb::ScanResponse {
            pairs: vec![pair],
            ..Default::default()
        };

        batch_get_request
            .decode_response(&mut batch_get_response, Some(&codec))
            .unwrap();
        scan_request
            .decode_response(&mut scan_response, Some(&codec))
            .unwrap();
        assert_eq!(batch_get_response.pairs[0].key, b"key");
        assert_eq!(scan_response.pairs[0].key, b"key");
    }

    #[test]
    fn api_v2_decoder_covers_pipelined_and_flashback_commands() {
        let codec = ApiV2Codec::new(KeyMode::Txn, 7).unwrap();

        let flush_request = super::new_flush_request(
            vec![kvrpcpb::Mutation {
                key: codec.encode_key(b"flush-key"),
                ..Default::default()
            }],
            codec.encode_key(b"flush-primary"),
            11,
            12,
            13,
            14,
        );
        assert_eq!(
            flush_request.mutations[0].key,
            codec.encode_key(b"flush-key")
        );
        assert_eq!(
            flush_request.primary_key,
            codec.encode_key(b"flush-primary")
        );

        let mut flush_response = kvrpcpb::FlushResponse {
            errors: vec![kvrpcpb::KeyError {
                locked: Some(kvrpcpb::LockInfo {
                    key: codec.encode_key(b"flush-locked"),
                    primary_lock: codec.encode_key(b"flush-primary"),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        flush_request
            .decode_response(&mut flush_response, Some(&codec))
            .unwrap();
        let flush_lock = flush_response.errors[0].locked.as_ref().unwrap();
        assert_eq!(flush_lock.key, b"flush-locked");
        assert_eq!(flush_lock.primary_lock, b"flush-primary");

        let buffer_batch_get_request =
            super::new_buffer_batch_get_request(vec![codec.encode_key(b"buffer-key")], 15);
        assert_eq!(
            buffer_batch_get_request.keys,
            vec![codec.encode_key(b"buffer-key")]
        );
        let mut buffer_batch_get_response = kvrpcpb::BufferBatchGetResponse {
            pairs: vec![kvrpcpb::KvPair {
                key: codec.encode_key(b"buffer-key"),
                value: b"value".to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        };
        buffer_batch_get_request
            .decode_response(&mut buffer_batch_get_response, Some(&codec))
            .unwrap();
        assert_eq!(buffer_batch_get_response.pairs[0].key, b"buffer-key");

        let (start_key, end_key) = codec.encode_region_range(b"start", b"end");
        let region_error = errorpb::Error {
            key_not_in_region: Some(errorpb::KeyNotInRegion {
                key: codec.encode_key(b"region-key"),
                start_key,
                end_key,
                ..Default::default()
            }),
            ..Default::default()
        };
        let prepare_request = super::new_prepare_flashback_to_version_request(
            codec.encode_key(b"start"),
            codec.encode_key(b"end"),
            16,
            17,
        );
        let mut prepare_response = kvrpcpb::PrepareFlashbackToVersionResponse {
            region_error: Some(region_error.clone()),
            ..Default::default()
        };
        prepare_request
            .decode_response(&mut prepare_response, Some(&codec))
            .unwrap();
        assert_eq!(
            prepare_response
                .region_error
                .unwrap()
                .key_not_in_region
                .unwrap()
                .key,
            b"region-key"
        );

        let flashback_request = super::new_flashback_to_version_request(
            codec.encode_key(b"start"),
            codec.encode_key(b"end"),
            18,
            19,
            20,
        );
        let mut flashback_response = kvrpcpb::FlashbackToVersionResponse {
            region_error: Some(region_error),
            ..Default::default()
        };
        flashback_request
            .decode_response(&mut flashback_response, Some(&codec))
            .unwrap();
        assert_eq!(
            flashback_response
                .region_error
                .unwrap()
                .key_not_in_region
                .unwrap()
                .key,
            b"region-key"
        );
    }

    #[test]
    fn api_v2_decoder_covers_physical_scan_lock_and_mvcc_by_key() {
        let codec = ApiV2Codec::new(KeyMode::Txn, 7).unwrap();

        let physical_scan_lock_request =
            super::new_physical_scan_lock_request(21, codec.encode_key(b"scan-start"), 22);
        let mut physical_scan_lock_response = kvrpcpb::PhysicalScanLockResponse {
            locks: vec![kvrpcpb::LockInfo {
                key: codec.encode_key(b"lock-key"),
                primary_lock: codec.encode_key(b"lock-primary"),
                ..Default::default()
            }],
            ..Default::default()
        };
        physical_scan_lock_request
            .decode_response(&mut physical_scan_lock_response, Some(&codec))
            .unwrap();
        assert_eq!(physical_scan_lock_response.locks[0].key, b"lock-key");
        assert_eq!(
            physical_scan_lock_response.locks[0].primary_lock,
            b"lock-primary"
        );

        let mvcc_request = super::new_mvcc_get_by_key_request(codec.encode_key(b"mvcc-key"));
        let mut mvcc_response = kvrpcpb::MvccGetByKeyResponse {
            info: Some(kvrpcpb::MvccInfo {
                lock: Some(kvrpcpb::MvccLock {
                    primary: codec.encode_key(b"mvcc-primary"),
                    secondaries: vec![codec.encode_key(b"mvcc-secondary")],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        mvcc_request
            .decode_response(&mut mvcc_response, Some(&codec))
            .unwrap();
        let lock = mvcc_response.info.unwrap().lock.unwrap();
        assert_eq!(lock.primary, b"mvcc-primary");
        assert_eq!(lock.secondaries, vec![b"mvcc-secondary".to_vec()]);
    }

    #[test]
    fn api_v2_decoder_covers_lock_observer_mvcc_start_ts_and_wait_info() {
        let codec = ApiV2Codec::new(KeyMode::Txn, 7).unwrap();

        let check_lock_observer_request = super::new_check_lock_observer_request(23);
        let mut check_lock_observer_response = kvrpcpb::CheckLockObserverResponse {
            locks: vec![kvrpcpb::LockInfo {
                key: codec.encode_key(b"observer-key"),
                primary_lock: codec.encode_key(b"observer-primary"),
                ..Default::default()
            }],
            ..Default::default()
        };
        check_lock_observer_request
            .decode_response(&mut check_lock_observer_response, Some(&codec))
            .unwrap();
        assert_eq!(check_lock_observer_response.locks[0].key, b"observer-key");

        let mvcc_start_ts_request = super::new_mvcc_get_by_start_ts_request(24);
        let mut mvcc_start_ts_response = kvrpcpb::MvccGetByStartTsResponse {
            key: codec.encode_key(b"mvcc-start-key"),
            info: Some(kvrpcpb::MvccInfo {
                lock: Some(kvrpcpb::MvccLock {
                    primary: codec.encode_key(b"mvcc-start-primary"),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        mvcc_start_ts_request
            .decode_response(&mut mvcc_start_ts_response, Some(&codec))
            .unwrap();
        assert_eq!(mvcc_start_ts_response.key, b"mvcc-start-key");
        assert_eq!(
            mvcc_start_ts_response.info.unwrap().lock.unwrap().primary,
            b"mvcc-start-primary"
        );

        let wait_info_request = super::new_get_lock_wait_info_request();
        let mut wait_info_response = kvrpcpb::GetLockWaitInfoResponse {
            entries: vec![crate::proto::deadlock::WaitForEntry {
                key: codec.encode_key(b"wait-key"),
                ..Default::default()
            }],
            ..Default::default()
        };
        wait_info_request
            .decode_response(&mut wait_info_response, Some(&codec))
            .unwrap();
        assert_eq!(wait_info_response.entries[0].key, b"wait-key");
    }

    #[test]
    #[allow(deprecated)]
    fn api_v2_decoder_covers_split_region_response_regions_only() {
        let codec = ApiV2Codec::new(KeyMode::Txn, 7).unwrap();
        let split_request = super::new_split_region_request(
            vec![codec.encode_key(b"split-a"), codec.encode_key(b"split-b")],
            false,
        );
        assert_eq!(
            split_request.split_keys,
            vec![codec.encode_key(b"split-a"), codec.encode_key(b"split-b")]
        );

        let (start_key, end_key) = codec.encode_region_range(b"region-start", b"region-end");
        let mut split_response = kvrpcpb::SplitRegionResponse {
            regions: vec![crate::proto::metapb::Region {
                start_key,
                end_key,
                ..Default::default()
            }],
            // `codec_v2.go` deliberately transforms `Regions`, not deprecated
            // `Left`/`Right`; keep that source distinction explicit.
            left: Some(crate::proto::metapb::Region {
                start_key: codec.encode_region_key(b"legacy-start"),
                end_key: codec.encode_region_key(b"legacy-end"),
                ..Default::default()
            }),
            ..Default::default()
        };
        split_request
            .decode_response(&mut split_response, Some(&codec))
            .unwrap();
        assert_eq!(split_response.regions[0].start_key, b"region-start");
        assert_eq!(split_response.regions[0].end_key, b"region-end");
        assert_eq!(
            split_response.left.unwrap().start_key,
            codec.encode_region_key(b"legacy-start")
        );
    }

    #[test]
    fn store_safe_ts_keeps_its_contextless_api_v2_key_range_shape() {
        let codec = ApiV2Codec::new(KeyMode::Txn, 7).unwrap();
        let key_range = codec.encode_key_range(&kvrpcpb::KeyRange {
            start_key: b"safe-start".to_vec(),
            end_key: b"safe-end".to_vec(),
        });
        let mut request = super::new_store_safe_ts_request(Some(key_range));
        request.set_api_version(kvrpcpb::ApiVersion::V2);

        assert_eq!(request.label(), "store_safe_ts");
        assert_eq!(
            request.key_range.unwrap(),
            kvrpcpb::KeyRange {
                start_key: codec.encode_key(b"safe-start"),
                end_key: codec.encode_key(b"safe-end"),
            }
        );
    }

    #[test]
    fn api_v2_codec_covers_coprocessor_ranges_tasks_and_response_fields() {
        let codec = ApiV2Codec::new(KeyMode::Txn, 7).unwrap();
        let request = crate::proto::coprocessor::Request {
            ranges: vec![crate::proto::coprocessor::KeyRange {
                start: b"top-start".to_vec(),
                end: b"top-end".to_vec(),
            }],
            tasks: vec![crate::proto::coprocessor::StoreBatchTask {
                ranges: vec![crate::proto::coprocessor::KeyRange {
                    start: b"task-start".to_vec(),
                    end: b"task-end".to_vec(),
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let request = codec.encode_coprocessor_request(&request);
        assert_eq!(request.ranges[0].start, codec.encode_key(b"top-start"));
        assert_eq!(request.ranges[0].end, codec.encode_key(b"top-end"));
        assert_eq!(
            request.tasks[0].ranges[0].start,
            codec.encode_key(b"task-start")
        );
        assert_eq!(
            request.tasks[0].ranges[0].end,
            codec.encode_key(b"task-end")
        );

        let mut response = crate::proto::coprocessor::Response {
            locked: Some(kvrpcpb::LockInfo {
                key: codec.encode_key(b"locked"),
                primary_lock: codec.encode_key(b"primary"),
                ..Default::default()
            }),
            range: Some(
                codec.encode_cop_range(&crate::proto::coprocessor::KeyRange {
                    start: b"resume-start".to_vec(),
                    end: b"resume-end".to_vec(),
                }),
            ),
            ..Default::default()
        };
        request
            .decode_response(&mut response, Some(&codec))
            .unwrap();
        assert_eq!(response.locked.as_ref().unwrap().key, b"locked");
        assert_eq!(response.locked.as_ref().unwrap().primary_lock, b"primary");
        let range = response.range.unwrap();
        assert_eq!(range.start, b"resume-start");
        assert_eq!(range.end, b"resume-end");
    }

    #[test]
    fn api_v2_codec_covers_tiflash_batch_and_mpp_region_descriptors() {
        let codec = ApiV2Codec::new(KeyMode::Txn, 7).unwrap();
        let region = crate::proto::coprocessor::RegionInfo {
            ranges: vec![crate::proto::coprocessor::KeyRange {
                start: b"region-start".to_vec(),
                end: b"region-end".to_vec(),
            }],
            ..Default::default()
        };
        let table_region = crate::proto::coprocessor::TableRegions {
            regions: vec![region.clone()],
            ..Default::default()
        };

        let batch =
            codec.encode_batch_coprocessor_request(&crate::proto::coprocessor::BatchRequest {
                regions: vec![region.clone()],
                table_regions: vec![table_region.clone()],
                ..Default::default()
            });
        assert_eq!(
            batch.regions[0].ranges[0].start,
            codec.encode_key(b"region-start")
        );
        assert_eq!(
            batch.regions[0].ranges[0].end,
            codec.encode_key(b"region-end")
        );
        assert_eq!(
            batch.table_regions[0].regions[0].ranges[0].start,
            codec.encode_key(b"region-start")
        );

        let mpp = codec.encode_mpp_dispatch_task_request(&crate::proto::mpp::DispatchTaskRequest {
            meta: Some(crate::proto::mpp::TaskMeta::default()),
            regions: vec![region],
            table_regions: vec![table_region],
            ..Default::default()
        });
        assert_eq!(
            mpp.regions[0].ranges[0].end,
            codec.encode_key(b"region-end")
        );
        assert_eq!(
            mpp.table_regions[0].regions[0].ranges[0].end,
            codec.encode_key(b"region-end")
        );
        let meta = mpp.meta.unwrap();
        assert_eq!(meta.keyspace_id, 7);
        assert_eq!(meta.api_version, kvrpcpb::ApiVersion::V2 as i32);

        let mut dispatch_request = crate::proto::mpp::DispatchTaskRequest {
            meta: Some(crate::proto::mpp::TaskMeta::default()),
            ..Default::default()
        };
        dispatch_request.set_api_version(kvrpcpb::ApiVersion::V2);
        dispatch_request.set_keyspace_id(Some(9));
        assert_eq!(dispatch_request.label(), "dispatch_mpp_task");
        let meta = dispatch_request.meta.unwrap();
        assert_eq!(meta.keyspace_id, 9);
        assert_eq!(meta.api_version, kvrpcpb::ApiVersion::V2 as i32);
    }

    #[test]
    fn v1_decoder_only_transforms_the_client_go_v1_command_matrix() {
        let codec = ApiV1Codec::new(KeyMode::Txn);
        let encoded_start = codec.encode_region_key(b"start");
        let encoded_end = codec.encode_region_key(b"end");

        let mut get_response = kvrpcpb::GetResponse {
            region_error: Some(errorpb::Error {
                key_not_in_region: Some(errorpb::KeyNotInRegion {
                    start_key: encoded_start.clone(),
                    end_key: encoded_end.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        kvrpcpb::GetRequest::default()
            .decode_v1_response(&mut get_response, Some(&codec))
            .unwrap();
        let decoded = get_response
            .region_error
            .unwrap()
            .key_not_in_region
            .unwrap();
        assert_eq!(decoded.start_key, b"start");
        assert_eq!(decoded.end_key, b"end");

        let mut flush_response = kvrpcpb::FlushResponse {
            region_error: Some(errorpb::Error {
                key_not_in_region: Some(errorpb::KeyNotInRegion {
                    start_key: encoded_start.clone(),
                    end_key: encoded_end.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        kvrpcpb::FlushRequest::default()
            .decode_v1_response(&mut flush_response, Some(&codec))
            .unwrap();
        let untouched = flush_response
            .region_error
            .unwrap()
            .key_not_in_region
            .unwrap();
        assert_eq!(untouched.start_key, encoded_start);
        assert_eq!(untouched.end_key, encoded_end);

        let mut cop_response = crate::proto::coprocessor::Response {
            region_error: Some(errorpb::Error {
                key_not_in_region: Some(errorpb::KeyNotInRegion {
                    start_key: codec.encode_region_key(b"cop-start"),
                    end_key: codec.encode_region_key(b"cop-end"),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        crate::proto::coprocessor::Request::default()
            .decode_v1_response(&mut cop_response, Some(&codec))
            .unwrap();
        let untouched = cop_response
            .region_error
            .unwrap()
            .key_not_in_region
            .unwrap();
        assert_eq!(untouched.start_key, codec.encode_region_key(b"cop-start"));
        assert_eq!(untouched.end_key, codec.encode_region_key(b"cop-end"));
    }

    #[test]
    fn api_v2_decoder_decodes_cleanup_gc_and_delete_range_errors() {
        let codec = ApiV2Codec::new(KeyMode::Txn, 7).unwrap();

        let cleanup_request = kvrpcpb::CleanupRequest::default();
        let mut cleanup_response = kvrpcpb::CleanupResponse {
            error: Some(kvrpcpb::KeyError {
                locked: Some(kvrpcpb::LockInfo {
                    key: codec.encode_key(b"cleanup-locked"),
                    primary_lock: codec.encode_key(b"cleanup-primary"),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        cleanup_request
            .decode_response(&mut cleanup_response, Some(&codec))
            .unwrap();
        let lock = cleanup_response.error.unwrap().locked.unwrap();
        assert_eq!(lock.key, b"cleanup-locked");
        assert_eq!(lock.primary_lock, b"cleanup-primary");

        let gc_request = kvrpcpb::GcRequest::default();
        let mut gc_response = kvrpcpb::GcResponse {
            error: Some(kvrpcpb::KeyError {
                locked: Some(kvrpcpb::LockInfo {
                    key: codec.encode_key(b"locked"),
                    primary_lock: codec.encode_key(b"primary"),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        gc_request
            .decode_response(&mut gc_response, Some(&codec))
            .unwrap();
        let lock = gc_response.error.unwrap().locked.unwrap();
        assert_eq!(lock.key, b"locked");
        assert_eq!(lock.primary_lock, b"primary");

        let delete_range_request =
            super::new_delete_range_request(b"start".to_vec(), b"end".to_vec());
        let (start_key, end_key) = codec.encode_region_range(b"start", b"end");
        let mut delete_range_response = kvrpcpb::DeleteRangeResponse {
            region_error: Some(errorpb::Error {
                key_not_in_region: Some(errorpb::KeyNotInRegion {
                    key: codec.encode_key(b"region-key"),
                    start_key,
                    end_key,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        delete_range_request
            .decode_response(&mut delete_range_response, Some(&codec))
            .unwrap();
        let key_not_in_region = delete_range_response
            .region_error
            .unwrap()
            .key_not_in_region
            .unwrap();
        assert_eq!(key_not_in_region.key, b"region-key");
        assert_eq!(key_not_in_region.start_key, b"start");
        assert_eq!(key_not_in_region.end_key, b"end");
    }

    #[tokio::test]
    async fn test_merge_pessimistic_lock_response() {
        let (key1, key2, key3, key4) = (b"key1", b"key2", b"key3", b"key4");
        let (value1, value4) = (b"value1", b"value4");
        let value_empty = b"";

        let resp1 = ResponseWithShard(
            kvrpcpb::PessimisticLockResponse {
                values: vec![value1.to_vec()],
                ..Default::default()
            },
            vec![kvrpcpb::Mutation {
                op: kvrpcpb::Op::PessimisticLock.into(),
                key: key1.to_vec(),
                ..Default::default()
            }],
        );

        let resp_empty_value = ResponseWithShard(
            kvrpcpb::PessimisticLockResponse {
                values: vec![value_empty.to_vec()],
                ..Default::default()
            },
            vec![kvrpcpb::Mutation {
                op: kvrpcpb::Op::PessimisticLock.into(),
                key: key2.to_vec(),
                ..Default::default()
            }],
        );

        let resp_not_found = ResponseWithShard(
            kvrpcpb::PessimisticLockResponse {
                values: vec![value_empty.to_vec(), value4.to_vec()],
                not_founds: vec![true, false],
                ..Default::default()
            },
            vec![
                kvrpcpb::Mutation {
                    op: kvrpcpb::Op::PessimisticLock.into(),
                    key: key3.to_vec(),
                    ..Default::default()
                },
                kvrpcpb::Mutation {
                    op: kvrpcpb::Op::PessimisticLock.into(),
                    key: key4.to_vec(),
                    ..Default::default()
                },
            ],
        );

        let merger = CollectWithShard {};
        {
            // empty values & not founds are filtered.
            let input = vec![
                Ok(resp1.clone()),
                Ok(resp_empty_value.clone()),
                Ok(resp_not_found.clone()),
            ];
            let result = merger.merge(input);

            assert_eq!(
                result.unwrap(),
                vec![
                    KvPair::new(key1.to_vec(), value1.to_vec()),
                    KvPair::new(key4.to_vec(), value4.to_vec()),
                ]
            );
        }
        {
            let input = vec![
                Ok(resp1),
                Ok(resp_empty_value),
                Err(ResolveLockError(vec![])),
                Ok(resp_not_found),
            ];
            let result = merger.merge(input);

            if let PessimisticLockError {
                inner,
                success_keys,
            } = result.unwrap_err()
            {
                assert!(matches!(*inner, ResolveLockError(_)));
                assert_eq!(
                    success_keys,
                    vec![key1.to_vec(), key2.to_vec(), key3.to_vec(), key4.to_vec()]
                );
            } else {
                panic!();
            }
        }
    }
}
