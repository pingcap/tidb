// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::iter;
use std::sync::Arc;

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
use crate::store::{HasKeyErrors, HasRegionError};
use crate::timestamp::TimestampExt;
use crate::transaction::requests::kvrpcpb::prewrite_request::PessimisticAction;
use crate::transaction::HasLocks;
use crate::util::iter::FlatMapOkIterExt;
use crate::Error;
use crate::Key;
use crate::KvPair;
use crate::Result;
use crate::Value;

// implement HasLocks for a response type that has a `pairs` field,
// where locks can be extracted from both the `pairs` and `error` fields
macro_rules! pair_locks {
    ($response_type:ty) => {
        impl HasLocks for $response_type {
            fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
                // A response-level key error means TiKV returned an
                // incomplete `pairs` list. client-go resolves that lock and
                // retries the original request; pair-level locks are only
                // meaningful when the response itself succeeded.
                if let Some(lock) = self.error.as_mut().and_then(|error| error.locked.take()) {
                    return vec![lock];
                }
                self.pairs
                    .iter_mut()
                    .filter_map(|pair| pair.error.as_mut().and_then(|error| error.locked.take()))
                    .collect()
            }

            fn take_response_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
                self.error
                    .as_mut()
                    .and_then(|error| error.locked.take())
                    .into_iter()
                    .collect()
            }

            fn take_clean_result_for_lock_retry(&mut self) -> Option<Self> {
                if self.error.is_some() {
                    return None;
                }
                let mut clean = Self::default();
                clean.pairs = std::mem::take(&mut self.pairs)
                    .into_iter()
                    .filter(|pair| pair.error.is_none())
                    .collect();
                Some(clean)
            }

            fn merge_clean_lock_retry_result(&mut self, mut clean: Self) {
                clean.pairs.append(&mut self.pairs);
                self.pairs = clean.pairs;
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

            fn take_response_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
                self.take_locks()
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

// client-go `txnkv/txnsnapshot.batchGetKeysByRegions` limits each physical
// BatchGet request to this many keys after grouping them by region.
const SNAPSHOT_BATCH_GET_SIZE: usize = 5120;

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

impl Shardable for kvrpcpb::BatchGetRequest {
    type Shard = Vec<Vec<u8>>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let mut keys = self.keys.clone();
        keys.sort();
        region_stream_for_keys(keys.into_iter(), pd_client.clone())
            .flat_map(|result| match result {
                Ok((keys, region)) => stream::iter(
                    keys.chunks(SNAPSHOT_BATCH_GET_SIZE)
                        .map(move |batch| Ok((batch.to_vec(), region.clone())))
                        .collect::<Vec<_>>(),
                )
                .boxed(),
                Err(error) => stream::iter(Err(error)).boxed(),
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
        self.set_busy_threshold_ms(store.busy_threshold_ms);
        Ok(())
    }
}

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
    sample_step: u32,
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
    req.sample_step = sample_step;
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

/// Scan response whose pair errors remain available to the snapshot scanner.
/// Region retry may inspect only the response-level error; pair errors are
/// source-owned iterator entries and are recovered with point reads.
#[derive(Clone)]
pub(crate) struct ScannerBatchResponse {
    pub(crate) pairs: Vec<kvrpcpb::KvPair>,
    region_error: Option<crate::proto::errorpb::Error>,
    error: Option<kvrpcpb::KeyError>,
}

#[derive(Clone, Copy)]
pub(crate) struct PreserveScannerPairErrors;

impl Process<kvrpcpb::ScanResponse> for PreserveScannerPairErrors {
    type Out = ScannerBatchResponse;

    fn process(&self, input: Result<kvrpcpb::ScanResponse>) -> Result<Self::Out> {
        let response = input?;
        Ok(ScannerBatchResponse {
            pairs: response.pairs,
            region_error: response.region_error,
            error: response.error,
        })
    }
}

impl HasKeyErrors for ScannerBatchResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        self.error.take().map(|error| vec![error.into()])
    }
}

impl HasRegionError for ScannerBatchResponse {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
        self.region_error.take()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct CollectScannerPairs;

impl Merge<ScannerBatchResponse> for CollectScannerPairs {
    type Out = Vec<kvrpcpb::KvPair>;

    fn merge(&self, input: Vec<Result<ScannerBatchResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .flat_map_ok(|response| response.pairs)
            .collect()
    }
}

/// One source scanner refill together with the exact region shard selected by
/// the retry owner. The shard drives continuation across empty regions.
pub(crate) struct ScannerRegionBatch {
    pub(crate) pairs: Vec<kvrpcpb::KvPair>,
    pub(crate) range: (Vec<u8>, Vec<u8>),
}

#[derive(Clone, Copy)]
pub(crate) struct CollectScannerRegionBatch;

impl Merge<ResponseWithShard<ScannerBatchResponse, (Vec<u8>, Vec<u8>)>>
    for CollectScannerRegionBatch
{
    type Out = ScannerRegionBatch;

    fn merge(
        &self,
        input: Vec<Result<ResponseWithShard<ScannerBatchResponse, (Vec<u8>, Vec<u8>)>>>,
    ) -> Result<Self::Out> {
        let mut input = input.into_iter();
        let response = input
            .next()
            .ok_or_else(|| Error::StringError("scanner selected no region".to_owned()))??;
        if input.next().is_some() {
            return Err(Error::StringError(
                "scanner selected more than one region".to_owned(),
            ));
        }
        Ok(ScannerRegionBatch {
            pairs: response.0.pairs,
            range: response.1,
        })
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
    req.txn_size = req.mutations.len() as u64;

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
        let full_txn_size = self.txn_size;
        let has_primary = shard
            .iter()
            .any(|mutation| mutation.key == self.primary_lock);
        // Only need to set secondary keys if we're sending the primary key.
        if self.use_async_commit && !has_primary {
            self.secondaries = vec![];
        }

        // Only if there is only one request to send
        if self.try_one_pc && (!has_primary || shard.len() as u64 != full_txn_size) {
            self.try_one_pc = false;
        }

        self.txn_size = shard.len() as u64;
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
    // The transaction owner fills call-specific first-lock, wait, wake-up,
    // existence, and minimum-commit fields after lowering the typed keys.
    req.is_first_lock = false;
    req.wait_timeout = 0;
    req.return_values = need_value;
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

#[derive(Clone, Copy)]
pub(crate) struct CollectPessimisticLock;

pub(crate) struct PessimisticLockOutput {
    pub(crate) pairs: Vec<KvPair>,
    pub(crate) returned_values: Vec<(Key, crate::ReturnedValue)>,
    pub(crate) max_locked_with_conflict_ts: u64,
}

fn collect_pessimistic_lock(
    input: Vec<Result<ResponseWithShard<kvrpcpb::PessimisticLockResponse, Vec<kvrpcpb::Mutation>>>>,
) -> Result<PessimisticLockOutput> {
    if input.iter().any(Result::is_err) {
        let (success, mut errors): (Vec<_>, Vec<_>) = input.into_iter().partition(Result::is_ok);
        let first_err = errors.pop().unwrap();
        let success_keys = success
            .into_iter()
            .map(Result::unwrap)
            .flat_map(|ResponseWithShard(_resp, mutations)| {
                mutations.into_iter().map(|mutation| mutation.key)
            })
            .collect();
        return Err(PessimisticLockError {
            inner: Box::new(first_err.unwrap_err()),
            success_keys,
        });
    }

    let mut pairs = Vec::new();
    let mut returned_values = Vec::new();
    let mut max_locked_with_conflict_ts = 0;
    for ResponseWithShard(response, mutations) in input.into_iter().map(Result::unwrap) {
        if !response.results.is_empty() {
            assert_eq!(response.results.len(), mutations.len());
            for (mutation, result) in mutations.into_iter().zip(response.results) {
                max_locked_with_conflict_ts =
                    max_locked_with_conflict_ts.max(result.locked_with_conflict_ts);
                returned_values.push((
                    Key::from(mutation.key.clone()),
                    crate::ReturnedValue {
                        value: result.value.clone(),
                        exists: result.existence,
                        locked_with_conflict_ts: result.locked_with_conflict_ts,
                        already_locked: false,
                    },
                ));
                if result.existence {
                    pairs.push(KvPair::new(Key::from(mutation.key), result.value));
                }
            }
            continue;
        }

        if response.values.is_empty() && response.not_founds.is_empty() {
            continue;
        }

        assert!(response.values.is_empty() || response.values.len() == mutations.len());
        if !response.not_founds.is_empty() {
            assert_eq!(response.not_founds.len(), mutations.len());
        }
        for (index, mutation) in mutations.into_iter().enumerate() {
            let value = response.values.get(index).cloned().unwrap_or_default();
            let not_found = response
                .not_founds
                .get(index)
                .copied()
                .unwrap_or_else(|| value.is_empty());
            returned_values.push((
                Key::from(mutation.key.clone()),
                crate::ReturnedValue {
                    value: value.clone(),
                    exists: !not_found,
                    locked_with_conflict_ts: 0,
                    already_locked: false,
                },
            ));
            if !not_found {
                pairs.push(KvPair::new(Key::from(mutation.key), value));
            }
        }
    }
    Ok(PessimisticLockOutput {
        pairs,
        returned_values,
        max_locked_with_conflict_ts,
    })
}

// PessimisticLockResponse preserves request order in both the legacy
// values/not_founds representation and ForceLock's per-key results.
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
        collect_pessimistic_lock(input).map(|output| output.pairs)
    }
}

impl Merge<ResponseWithShard<kvrpcpb::PessimisticLockResponse, Vec<kvrpcpb::Mutation>>>
    for CollectPessimisticLock
{
    type Out = PessimisticLockOutput;

    fn merge(
        &self,
        input: Vec<
            Result<ResponseWithShard<kvrpcpb::PessimisticLockResponse, Vec<kvrpcpb::Mutation>>>,
        >,
    ) -> Result<Self::Out> {
        collect_pessimistic_lock(input)
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

    /// Whether this is a source-determined transaction status suitable for
    /// lock-resolver caching. A locked response remains mutable even if its
    /// TTL looks expired locally, so client-go never retains it.
    pub fn is_cacheable(&self) -> bool {
        matches!(
            &self.kind,
            TransactionStatusKind::RolledBack | TransactionStatusKind::Committed(..)
        )
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

// CheckSecondaryLocks only returns locks that are still present. Keep each
// request shard so the resolver can distinguish an all-present response from
// a missing lock, which means TiKV has already determined the transaction.
impl Merge<ResponseWithShard<kvrpcpb::CheckSecondaryLocksResponse, Vec<Vec<u8>>>>
    for CollectWithShard
{
    type Out = SecondaryLocksStatus;

    fn merge(
        &self,
        input: Vec<Result<ResponseWithShard<kvrpcpb::CheckSecondaryLocksResponse, Vec<Vec<u8>>>>>,
    ) -> Result<Self::Out> {
        let mut out = SecondaryLocksStatus {
            locks: Vec::new(),
            missing_lock: false,
            missing_commit_ts: 0,
            fallback_2pc: false,
        };
        for response in input {
            let ResponseWithShard(resp, keys) = response?;
            if resp.locks.len() < keys.len() {
                if out.missing_lock && out.missing_commit_ts != resp.commit_ts {
                    return Err(Error::InternalError {
                        message: format!(
                            "commit TS mismatch in async commit recovery: {} and {}",
                            out.missing_commit_ts, resp.commit_ts
                        ),
                    });
                }
                out.missing_lock = true;
                out.missing_commit_ts = resp.commit_ts;
                // client-go does not retain locks from a partial response:
                // TiKV resolves the remaining locks once the outcome is known.
                continue;
            }
            out.locks.extend(resp.locks);
        }
        Ok(out)
    }
}

#[derive(Debug)]
pub struct SecondaryLocksStatus {
    locks: Vec<LockInfo>,
    missing_lock: bool,
    missing_commit_ts: u64,
    pub fallback_2pc: bool,
}

impl SecondaryLocksStatus {
    pub(crate) fn empty() -> Self {
        Self {
            locks: Vec::new(),
            missing_lock: false,
            missing_commit_ts: 0,
            fallback_2pc: false,
        }
    }

    pub(crate) fn merge_from(&mut self, mut other: Self) -> Result<()> {
        if other.missing_lock {
            if self.missing_lock && self.missing_commit_ts != other.missing_commit_ts {
                return Err(Error::InternalError {
                    message: format!(
                        "commit TS mismatch in async commit recovery: {} and {}",
                        self.missing_commit_ts, other.missing_commit_ts
                    ),
                });
            }
            self.missing_lock = true;
            self.missing_commit_ts = other.missing_commit_ts;
        }
        self.fallback_2pc |= other.fallback_2pc;
        self.locks.append(&mut other.locks);
        Ok(())
    }

    /// Returns `None` when a returned secondary is not an async-commit lock,
    /// in which case client-go retries CheckTxnStatus in forced 2PC mode.
    pub fn determine_commit_ts(
        &mut self,
        txn_id: u64,
        primary_min_commit_ts: u64,
    ) -> Result<Option<u64>> {
        let mut min_commit_ts = primary_min_commit_ts;
        for lock in &self.locks {
            if lock.lock_version != txn_id {
                return Err(Error::InternalError {
                    message: format!(
                        "unexpected timestamp, expected: {txn_id}, found: {}",
                        lock.lock_version
                    ),
                });
            }
            if !lock.use_async_commit {
                self.fallback_2pc = true;
                return Ok(None);
            }
            min_commit_ts = cmp::max(min_commit_ts, lock.min_commit_ts);
        }

        if self.missing_lock {
            if self.missing_commit_ts != 0 && self.missing_commit_ts < min_commit_ts {
                return Err(Error::InternalError {
                    message: format!(
                        "commit TS must be greater or equal to min commit TS: commit ts: {}, min commit ts: {}",
                        self.missing_commit_ts, min_commit_ts
                    ),
                });
            }
            Ok(Some(self.missing_commit_ts))
        } else {
            Ok(Some(min_commit_ts))
        }
    }

    /// The source resolver only sends ResolveLock for returned secondaries
    /// when every requested lock is present. A missing lock has already been
    /// resolved by TiKV according to its returned commit timestamp, so only
    /// the primary needs an explicit resolve request.
    pub fn keys_to_resolve(&self, primary: &[u8]) -> Vec<Vec<u8>> {
        if self.missing_lock {
            return vec![primary.to_vec()];
        }

        self.locks
            .iter()
            .map(|lock| lock.key.clone())
            .chain(std::iter::once(primary.to_vec()))
            .collect()
    }
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
            .flat_map(|lock| {
                if lock.shared_lock_infos.is_empty() {
                    vec![lock]
                } else {
                    lock.shared_lock_infos
                }
            })
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

impl Shardable for kvrpcpb::BufferBatchGetRequest {
    type Shard = Vec<Vec<u8>>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let mut keys = self.keys.clone();
        keys.sort();
        region_stream_for_keys(keys.into_iter(), pd_client.clone())
            .flat_map(|result| match result {
                Ok((keys, region)) => stream::iter(
                    keys.chunks(SNAPSHOT_BATCH_GET_SIZE)
                        .map(move |batch| Ok((batch.to_vec(), region.clone())))
                        .collect::<Vec<_>>(),
                )
                .boxed(),
                Err(error) => stream::iter(Err(error)).boxed(),
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
        self.set_busy_threshold_ms(store.busy_threshold_ms);
        Ok(())
    }
}
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

impl KvRequest for kvrpcpb::BroadcastTxnStatusRequest {
    type Response = kvrpcpb::BroadcastTxnStatusResponse;
}

impl StoreRequest for kvrpcpb::BroadcastTxnStatusRequest {
    fn apply_store(&mut self, _store: &Store) {}
}

impl crate::store::HasKeyErrors for kvrpcpb::BroadcastTxnStatusResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        None
    }
}

impl crate::store::HasRegionError for kvrpcpb::BroadcastTxnStatusResponse {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
        None
    }
}

impl HasLocks for kvrpcpb::BroadcastTxnStatusResponse {}

impl Merge<kvrpcpb::BroadcastTxnStatusResponse> for Collect {
    type Out = ();

    fn merge(&self, input: Vec<Result<kvrpcpb::BroadcastTxnStatusResponse>>) -> Result<Self::Out> {
        let _: Vec<kvrpcpb::BroadcastTxnStatusResponse> =
            input.into_iter().collect::<Result<Vec<_>>>()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        new_prewrite_request, SecondaryLocksStatus, TransactionStatus, TransactionStatusKind,
    };
    use crate::common::Error::PessimisticLockError;
    use crate::common::Error::ResolveLockError;
    use crate::proto::errorpb;
    use crate::proto::kvrpcpb;
    use crate::request::plan::Merge;
    use crate::request::Collect;
    use crate::request::CollectWithShard;
    use crate::request::ResponseWithShard;
    use crate::request::Shardable;
    use crate::request::{ApiV1Codec, ApiV2Codec, KeyMode, KvRequest};
    use crate::store::Request;
    use crate::transaction::HasLocks;
    use crate::KvPair;
    use crate::Timestamp;
    use crate::TimestampExt;
    use std::any::Any;
    use std::sync::Arc;
    use std::sync::Mutex;

    #[test]
    fn source_lock_resolver_caches_only_determined_statuses() {
        let locked = TransactionStatus {
            kind: TransactionStatusKind::Locked(
                1,
                kvrpcpb::LockInfo {
                    lock_version: 1 << 18,
                    ..Default::default()
                },
            ),
            action: kvrpcpb::Action::TtlExpireRollback,
            is_expired: true,
        };
        assert!(!locked.is_cacheable());

        for kind in [
            TransactionStatusKind::RolledBack,
            TransactionStatusKind::Committed(Timestamp::from_version(42)),
        ] {
            assert!(TransactionStatus {
                kind,
                action: kvrpcpb::Action::NoAction,
                is_expired: false,
            }
            .is_cacheable());
        }
    }

    #[tokio::test]
    async fn source_snapshot_batch_get_batches_each_region_at_5120_keys() -> crate::Result<()> {
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let captured_sizes = Arc::clone(&batch_sizes);
        let client = Arc::new(crate::mock::MockPdClient::new(
            crate::mock::MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
                let request = request.downcast_ref::<kvrpcpb::BatchGetRequest>().unwrap();
                captured_sizes.lock().unwrap().push(request.keys.len());
                Ok(Box::new(kvrpcpb::BatchGetResponse::default()) as Box<dyn Any>)
            }),
        ));

        let request = super::new_batch_get_request(vec![vec![11]; 5121], 1);
        let plan =
            crate::request::PlanBuilder::new(client, crate::request::Keyspace::Disable, request)
                .retry_multi_region(crate::backoff::DEFAULT_REGION_BACKOFF)
                .merge(crate::request::Collect)
                .plan();
        let _: Vec<KvPair> = crate::request::Plan::execute(&plan).await?;

        let mut sizes = batch_sizes.lock().unwrap().clone();
        sizes.sort_unstable();
        assert_eq!(sizes, [1, 5120]);
        Ok(())
    }

    fn secondary_lock(txn_id: u64, min_commit_ts: u64) -> kvrpcpb::LockInfo {
        kvrpcpb::LockInfo {
            lock_version: txn_id,
            min_commit_ts,
            use_async_commit: true,
            ..Default::default()
        }
    }

    #[test]
    fn source_async_commit_secondary_status_uses_missing_lock_commit_ts() {
        let merger = CollectWithShard {};
        let mut status: SecondaryLocksStatus = merger
            .merge(vec![
                Ok(ResponseWithShard(
                    kvrpcpb::CheckSecondaryLocksResponse {
                        // A non-zero response commit ts is ignored while all
                        // locks in this shard remain present, as in client-go.
                        commit_ts: 5,
                        locks: vec![secondary_lock(7, 100)],
                        ..Default::default()
                    },
                    vec![b"a".to_vec()],
                )),
                Ok(ResponseWithShard(
                    kvrpcpb::CheckSecondaryLocksResponse {
                        commit_ts: 120,
                        locks: Vec::new(),
                        ..Default::default()
                    },
                    vec![b"b".to_vec()],
                )),
            ])
            .unwrap();

        assert_eq!(status.determine_commit_ts(7, 80).unwrap(), Some(120));
    }

    #[test]
    fn source_async_commit_secondary_status_uses_max_min_commit_ts_when_all_present() {
        let merger = CollectWithShard {};
        let mut status: SecondaryLocksStatus = merger
            .merge(vec![
                Ok(ResponseWithShard(
                    kvrpcpb::CheckSecondaryLocksResponse {
                        commit_ts: 5,
                        locks: vec![secondary_lock(7, 100)],
                        ..Default::default()
                    },
                    vec![b"a".to_vec()],
                )),
                Ok(ResponseWithShard(
                    kvrpcpb::CheckSecondaryLocksResponse {
                        commit_ts: 6,
                        locks: vec![secondary_lock(7, 110)],
                        ..Default::default()
                    },
                    vec![b"b".to_vec()],
                )),
            ])
            .unwrap();

        assert_eq!(status.determine_commit_ts(7, 80).unwrap(), Some(110));
    }

    #[test]
    fn source_async_commit_secondary_status_rejects_inconsistent_missing_commit_ts() {
        let merger = CollectWithShard {};
        let error = merger
            .merge(vec![
                Ok(ResponseWithShard(
                    kvrpcpb::CheckSecondaryLocksResponse {
                        commit_ts: 120,
                        ..Default::default()
                    },
                    vec![b"a".to_vec()],
                )),
                Ok(ResponseWithShard(
                    kvrpcpb::CheckSecondaryLocksResponse {
                        commit_ts: 121,
                        ..Default::default()
                    },
                    vec![b"b".to_vec()],
                )),
            ])
            .unwrap_err();

        assert!(matches!(error, crate::Error::InternalError { .. }));
    }

    #[test]
    fn source_async_commit_secondary_status_requests_forced_2pc_for_non_async_lock() {
        let merger = CollectWithShard {};
        let mut status: SecondaryLocksStatus = merger
            .merge(vec![Ok(ResponseWithShard(
                kvrpcpb::CheckSecondaryLocksResponse {
                    locks: vec![kvrpcpb::LockInfo {
                        lock_version: 7,
                        use_async_commit: false,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                vec![b"a".to_vec()],
            ))])
            .unwrap();

        assert_eq!(status.determine_commit_ts(7, 1).unwrap(), None);
        assert!(status.fallback_2pc);
    }

    #[test]
    fn source_prewrite_txn_size_tracks_the_physical_request_shard() {
        let mut request = new_prewrite_request(
            vec![
                kvrpcpb::Mutation {
                    key: b"a".to_vec(),
                    ..Default::default()
                },
                kvrpcpb::Mutation {
                    key: b"b".to_vec(),
                    ..Default::default()
                },
            ],
            b"a".to_vec(),
            1,
            10,
        );
        assert_eq!(request.txn_size, 2);

        <kvrpcpb::PrewriteRequest as Shardable>::apply_shard(
            &mut request,
            vec![kvrpcpb::Mutation {
                key: b"b".to_vec(),
                ..Default::default()
            }],
        );
        assert_eq!(request.txn_size, 1);
    }

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
    fn source_delete_range_counts_successful_region_responses() {
        let completed = Collect
            .merge(vec![
                Ok(kvrpcpb::DeleteRangeResponse::default()),
                Ok(kvrpcpb::DeleteRangeResponse::default()),
            ])
            .unwrap();
        assert_eq!(completed, 2);
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
    fn pair_response_lock_takes_precedence_over_incomplete_pairs() {
        let response_lock = kvrpcpb::LockInfo {
            key: b"response-lock".to_vec(),
            ..Default::default()
        };
        let pair_lock = kvrpcpb::LockInfo {
            key: b"pair-lock".to_vec(),
            ..Default::default()
        };
        let mut response = kvrpcpb::ScanResponse {
            error: Some(kvrpcpb::KeyError {
                locked: Some(response_lock.clone()),
                ..Default::default()
            }),
            pairs: vec![kvrpcpb::KvPair {
                error: Some(kvrpcpb::KeyError {
                    locked: Some(pair_lock),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert_eq!(response.take_locks(), vec![response_lock]);
        assert!(response.pairs[0]
            .error
            .as_ref()
            .and_then(|error| error.locked.as_ref())
            .is_some());
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
        assert_eq!(
            meta.keyspace,
            Some(crate::proto::mpp::task_meta::Keyspace::KeyspaceId(7))
        );
        assert_eq!(meta.api_version, kvrpcpb::ApiVersion::V2 as i32);

        let mut dispatch_request = crate::proto::mpp::DispatchTaskRequest {
            meta: Some(crate::proto::mpp::TaskMeta::default()),
            ..Default::default()
        };
        dispatch_request.set_api_version(kvrpcpb::ApiVersion::V2);
        dispatch_request.set_keyspace_id(Some(9));
        assert_eq!(dispatch_request.label(), "dispatch_mpp_task");
        let meta = dispatch_request.meta.unwrap();
        assert_eq!(
            meta.keyspace,
            Some(crate::proto::mpp::task_meta::Keyspace::KeyspaceId(9))
        );
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
