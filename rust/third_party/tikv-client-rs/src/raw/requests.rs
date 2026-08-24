// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::RawRpcRequest;
use crate::collect_single;
use crate::kv::KvPairTTL;
use crate::pd::PdClient;
use crate::proto::kvrpcpb;
use crate::proto::metapb;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::range_request;
use crate::region::RegionWithLeader;
use crate::request::plan::ResponseWithShard;
use crate::request::CollectSingle;
use crate::request::DefaultProcessor;
use crate::request::KvRequest;
use crate::request::Merge;
use crate::request::Process;
use crate::request::RangeRequest;
use crate::request::Shardable;
use crate::request::SingleKey;
use crate::request::{key_batches, Batchable, Collect};
use crate::shardable_key;
use crate::shardable_range;
use crate::store::region_stream_for_keys;
use crate::store::region_stream_for_range;
use crate::store::region_stream_for_ranges;
use crate::store::RegionStore;
use crate::store::Request;
use crate::transaction::HasLocks;
use crate::util::iter::FlatMapOkIterExt;
use crate::ColumnFamily;
use crate::Key;
use crate::KvPair;
use crate::RawChecksum;
use crate::Result;
use crate::Value;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{stream, StreamExt};
use std::any::Any;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;

const RAW_KV_REQUEST_BATCH_SIZE: u64 = 16 * 1024; // 16 KB
const RAW_KV_REQUEST_KEY_BATCH_LIMIT: isize = 512;

macro_rules! impl_raw_v2_response {
    ($request:ty, $response:ty, $decode:expr) => {
        impl KvRequest for $request {
            type Response = $response;

            fn key_mode(&self) -> Option<crate::request::KeyMode> {
                Some(crate::request::KeyMode::Raw)
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

pub fn new_raw_get_request(key: Vec<u8>, cf: Option<ColumnFamily>) -> kvrpcpb::RawGetRequest {
    let mut req = kvrpcpb::RawGetRequest::default();
    req.key = key;
    req.maybe_set_cf(cf);

    req
}

impl KvRequest for kvrpcpb::RawGetRequest {
    type Response = kvrpcpb::RawGetResponse;

    fn key_mode(&self) -> Option<crate::request::KeyMode> {
        Some(crate::request::KeyMode::Raw)
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

shardable_key!(kvrpcpb::RawGetRequest);
collect_single!(kvrpcpb::RawGetResponse);

impl SingleKey for kvrpcpb::RawGetRequest {
    fn key(&self) -> &Vec<u8> {
        &self.key
    }
}

impl Process<kvrpcpb::RawGetResponse> for DefaultProcessor {
    type Out = Option<Value>;

    fn process(&self, input: Result<kvrpcpb::RawGetResponse>) -> Result<Self::Out> {
        let input = input?;
        Ok(if input.not_found {
            None
        } else {
            Some(input.value)
        })
    }
}

pub fn new_raw_batch_get_request(
    keys: Vec<Vec<u8>>,
    cf: Option<ColumnFamily>,
) -> kvrpcpb::RawBatchGetRequest {
    let mut req = kvrpcpb::RawBatchGetRequest::default();
    req.keys = keys;
    req.maybe_set_cf(cf);

    req
}

impl KvRequest for kvrpcpb::RawBatchGetRequest {
    type Response = kvrpcpb::RawBatchGetResponse;

    fn key_mode(&self) -> Option<crate::request::KeyMode> {
        Some(crate::request::KeyMode::Raw)
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
        codec.decode_pairs(&mut response.pairs)
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

impl Shardable for kvrpcpb::RawBatchGetRequest {
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
                    stream::iter(key_batches(keys, RAW_KV_REQUEST_KEY_BATCH_LIMIT))
                        .map(move |batch| Ok((batch, region.clone())))
                        .boxed()
                }
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
        Ok(())
    }
}

impl Merge<kvrpcpb::RawBatchGetResponse> for Collect {
    type Out = Vec<KvPair>;

    fn merge(&self, input: Vec<Result<kvrpcpb::RawBatchGetResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .flat_map_ok(|resp| resp.pairs.into_iter().map(Into::into))
            .collect()
    }
}

pub fn new_raw_get_key_ttl_request(
    key: Vec<u8>,
    cf: Option<ColumnFamily>,
) -> kvrpcpb::RawGetKeyTtlRequest {
    let mut req = kvrpcpb::RawGetKeyTtlRequest::default();
    req.key = key;
    req.maybe_set_cf(cf);

    req
}

impl_raw_v2_response!(
    kvrpcpb::RawGetKeyTtlRequest,
    kvrpcpb::RawGetKeyTtlResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::RawGetKeyTtlResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        Ok(())
    }
);

shardable_key!(kvrpcpb::RawGetKeyTtlRequest);
collect_single!(kvrpcpb::RawGetKeyTtlResponse);

impl SingleKey for kvrpcpb::RawGetKeyTtlRequest {
    fn key(&self) -> &Vec<u8> {
        &self.key
    }
}

impl Process<kvrpcpb::RawGetKeyTtlResponse> for DefaultProcessor {
    type Out = Option<u64>;

    fn process(&self, input: Result<kvrpcpb::RawGetKeyTtlResponse>) -> Result<Self::Out> {
        let input = input?;
        Ok(if input.not_found {
            None
        } else {
            Some(input.ttl)
        })
    }
}

pub fn new_raw_put_request(
    key: Vec<u8>,
    value: Vec<u8>,
    ttl: u64,
    cf: Option<ColumnFamily>,
    atomic: bool,
) -> kvrpcpb::RawPutRequest {
    let mut req = kvrpcpb::RawPutRequest::default();
    req.key = key;
    req.value = value;
    req.ttl = ttl;
    req.maybe_set_cf(cf);
    req.for_cas = atomic;

    req
}

impl_raw_v2_response!(
    kvrpcpb::RawPutRequest,
    kvrpcpb::RawPutResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::RawPutResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        Ok(())
    }
);

shardable_key!(kvrpcpb::RawPutRequest);
collect_single!(kvrpcpb::RawPutResponse);
impl SingleKey for kvrpcpb::RawPutRequest {
    fn key(&self) -> &Vec<u8> {
        &self.key
    }
}

#[allow(deprecated)]
pub fn new_raw_batch_put_request(
    pairs: Vec<kvrpcpb::KvPair>,
    ttls: Vec<u64>,
    cf: Option<ColumnFamily>,
    atomic: bool,
) -> kvrpcpb::RawBatchPutRequest {
    let mut req = kvrpcpb::RawBatchPutRequest::default();
    req.pairs = pairs;
    // Keep the legacy single-TTL field in sync with client-go. TiKV uses the
    // per-pair `ttls` field for mixed TTLs, but older servers still observe
    // `ttl`, which client-go sets to the first batch item's value.
    req.ttl = ttls.first().copied().unwrap_or_default();
    req.ttls = ttls;
    req.maybe_set_cf(cf);
    req.for_cas = atomic;

    req
}

impl_raw_v2_response!(
    kvrpcpb::RawBatchPutRequest,
    kvrpcpb::RawBatchPutResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::RawBatchPutResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        Ok(())
    }
);

impl Batchable for kvrpcpb::RawBatchPutRequest {
    type Item = (kvrpcpb::KvPair, u64);

    fn item_size(item: &Self::Item) -> u64 {
        (item.0.key.len() + item.0.value.len()) as u64
    }
}

impl Shardable for kvrpcpb::RawBatchPutRequest {
    type Shard = Vec<(kvrpcpb::KvPair, u64)>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let kvs = self.pairs.clone();
        let ttls = self.ttls.clone();
        let mut kv_ttl: Vec<KvPairTTL> = kvs
            .into_iter()
            .zip(ttls)
            .map(|(kv, ttl)| KvPairTTL(kv, ttl))
            .collect();
        kv_ttl.sort_by(|a, b| a.0.key.cmp(&b.0.key));
        region_stream_for_keys(kv_ttl.into_iter(), pd_client.clone())
            .flat_map(|result| match result {
                Ok((keys, region)) => stream::iter(kvrpcpb::RawBatchPutRequest::batches(
                    keys,
                    RAW_KV_REQUEST_BATCH_SIZE,
                ))
                .map(move |batch| Ok((batch, region.clone())))
                .boxed(),
                Err(e) => stream::iter(Err(e)).boxed(),
            })
            .boxed()
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        let (pairs, ttls) = shard.into_iter().unzip();
        self.pairs = pairs;
        self.ttls = ttls;
        #[allow(deprecated)]
        {
            self.ttl = self.ttls.first().copied().unwrap_or_default();
        }
    }

    fn clone_then_apply_shard(&self, shard: Self::Shard) -> Self
    where
        Self: Sized + Clone,
    {
        let mut cloned = Self::default();
        cloned.context = self.context.clone();
        cloned.cf = self.cf.clone();
        cloned.for_cas = self.for_cas;
        cloned.apply_shard(shard);
        cloned
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.request_region())?;
        self.set_replica_read(store.is_replica_read());
        self.set_stale_read(store.stale_read);
        Ok(())
    }
}

pub fn new_raw_delete_request(
    key: Vec<u8>,
    cf: Option<ColumnFamily>,
    atomic: bool,
) -> kvrpcpb::RawDeleteRequest {
    let mut req = kvrpcpb::RawDeleteRequest::default();
    req.key = key;
    req.maybe_set_cf(cf);
    req.for_cas = atomic;

    req
}

impl_raw_v2_response!(
    kvrpcpb::RawDeleteRequest,
    kvrpcpb::RawDeleteResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::RawDeleteResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        Ok(())
    }
);

shardable_key!(kvrpcpb::RawDeleteRequest);
collect_single!(kvrpcpb::RawDeleteResponse);
impl SingleKey for kvrpcpb::RawDeleteRequest {
    fn key(&self) -> &Vec<u8> {
        &self.key
    }
}

pub fn new_raw_batch_delete_request(
    keys: Vec<Vec<u8>>,
    cf: Option<ColumnFamily>,
    atomic: bool,
) -> kvrpcpb::RawBatchDeleteRequest {
    let mut req = kvrpcpb::RawBatchDeleteRequest::default();
    req.keys = keys;
    req.maybe_set_cf(cf);
    req.for_cas = atomic;

    req
}

impl_raw_v2_response!(
    kvrpcpb::RawBatchDeleteRequest,
    kvrpcpb::RawBatchDeleteResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::RawBatchDeleteResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        Ok(())
    }
);

impl Shardable for kvrpcpb::RawBatchDeleteRequest {
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
                    stream::iter(key_batches(keys, RAW_KV_REQUEST_KEY_BATCH_LIMIT))
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

    fn clone_then_apply_shard(&self, shard: Self::Shard) -> Self
    where
        Self: Sized + Clone,
    {
        let mut cloned = Self::default();
        cloned.context = self.context.clone();
        cloned.cf = self.cf.clone();
        cloned.for_cas = self.for_cas;
        cloned.apply_shard(shard);
        cloned
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.request_region())?;
        self.set_replica_read(store.is_replica_read());
        self.set_stale_read(store.stale_read);
        Ok(())
    }
}

pub fn new_raw_delete_range_request(
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    cf: Option<ColumnFamily>,
) -> kvrpcpb::RawDeleteRangeRequest {
    let mut req = kvrpcpb::RawDeleteRangeRequest::default();
    req.start_key = start_key;
    req.end_key = end_key;
    req.maybe_set_cf(cf);

    req
}

impl_raw_v2_response!(
    kvrpcpb::RawDeleteRangeRequest,
    kvrpcpb::RawDeleteRangeResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::RawDeleteRangeResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        Ok(())
    }
);

range_request!(kvrpcpb::RawDeleteRangeRequest);
shardable_range!(kvrpcpb::RawDeleteRangeRequest);

pub fn new_raw_checksum_request(
    start_key: Vec<u8>,
    end_key: Vec<u8>,
) -> kvrpcpb::RawChecksumRequest {
    kvrpcpb::RawChecksumRequest {
        algorithm: kvrpcpb::ChecksumAlgorithm::Crc64Xor.into(),
        ranges: vec![kvrpcpb::KeyRange { start_key, end_key }],
        ..Default::default()
    }
}

impl_raw_v2_response!(
    kvrpcpb::RawChecksumRequest,
    kvrpcpb::RawChecksumResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::RawChecksumResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        Ok(())
    }
);

impl Shardable for kvrpcpb::RawChecksumRequest {
    type Shard = (Vec<u8>, Vec<u8>);

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let range = self
            .ranges
            .first()
            .expect("RawChecksumRequest must contain one range");
        region_stream_for_range(
            (range.start_key.clone(), range.end_key.clone()),
            pd_client.clone(),
        )
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.ranges = vec![kvrpcpb::KeyRange {
            start_key: shard.0,
            end_key: shard.1,
        }];
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.request_region())?;
        self.set_replica_read(store.is_replica_read());
        self.set_stale_read(store.stale_read);
        Ok(())
    }
}

impl Merge<kvrpcpb::RawChecksumResponse> for Collect {
    type Out = RawChecksum;

    fn merge(&self, input: Vec<Result<kvrpcpb::RawChecksumResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .try_fold(RawChecksum::default(), |mut checksum, response| {
                let response = response?;
                checksum.crc64_xor ^= response.checksum;
                checksum.total_kvs += response.total_kvs;
                checksum.total_bytes += response.total_bytes;
                Ok(checksum)
            })
    }
}

pub fn new_raw_scan_request(
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    limit: u32,
    key_only: bool,
    reverse: bool,
    cf: Option<ColumnFamily>,
) -> kvrpcpb::RawScanRequest {
    let mut req = kvrpcpb::RawScanRequest::default();
    if !reverse {
        req.start_key = start_key;
        req.end_key = end_key;
    } else {
        req.start_key = end_key;
        req.end_key = start_key;
    }
    req.limit = limit;
    req.key_only = key_only;
    req.reverse = reverse;
    req.maybe_set_cf(cf);

    req
}

impl_raw_v2_response!(
    kvrpcpb::RawScanRequest,
    kvrpcpb::RawScanResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::RawScanResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        codec.decode_pairs(&mut response.kvs)
    }
);

range_request!(kvrpcpb::RawScanRequest);
shardable_range!(kvrpcpb::RawScanRequest);

impl Merge<kvrpcpb::RawScanResponse> for Collect {
    type Out = Vec<KvPair>;

    fn merge(&self, input: Vec<Result<kvrpcpb::RawScanResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .flat_map_ok(|resp| resp.kvs.into_iter().map(Into::into))
            .collect()
    }
}

pub fn new_raw_batch_scan_request(
    ranges: Vec<kvrpcpb::KeyRange>,
    each_limit: u32,
    key_only: bool,
    cf: Option<ColumnFamily>,
) -> kvrpcpb::RawBatchScanRequest {
    let mut req = kvrpcpb::RawBatchScanRequest::default();
    req.ranges = ranges;
    req.each_limit = each_limit;
    req.key_only = key_only;
    req.maybe_set_cf(cf);

    req
}

impl_raw_v2_response!(
    kvrpcpb::RawBatchScanRequest,
    kvrpcpb::RawBatchScanResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::RawBatchScanResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        codec.decode_pairs(&mut response.kvs)
    }
);

impl Shardable for kvrpcpb::RawBatchScanRequest {
    type Shard = Vec<kvrpcpb::KeyRange>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        region_stream_for_ranges(self.ranges.clone(), pd_client.clone())
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.ranges = shard;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.request_region())?;
        self.set_replica_read(store.is_replica_read());
        self.set_stale_read(store.stale_read);
        Ok(())
    }
}

impl Merge<kvrpcpb::RawBatchScanResponse> for Collect {
    type Out = Vec<KvPair>;

    fn merge(&self, input: Vec<Result<kvrpcpb::RawBatchScanResponse>>) -> Result<Self::Out> {
        input
            .into_iter()
            .flat_map_ok(|resp| resp.kvs.into_iter().map(Into::into))
            .collect()
    }
}

pub fn new_cas_request(
    key: Vec<u8>,
    value: Vec<u8>,
    previous_value: Option<Vec<u8>>,
    cf: Option<ColumnFamily>,
) -> kvrpcpb::RawCasRequest {
    let mut req = kvrpcpb::RawCasRequest::default();
    req.key = key;
    req.value = value;
    match previous_value {
        Some(v) => req.previous_value = v,
        None => req.previous_not_exist = true,
    }
    req.maybe_set_cf(cf);
    req
}

impl_raw_v2_response!(
    kvrpcpb::RawCasRequest,
    kvrpcpb::RawCasResponse,
    |codec: &crate::request::ApiV2Codec, response: &mut kvrpcpb::RawCasResponse| {
        if let Some(region_error) = &mut response.region_error {
            codec.decode_region_error(region_error)?;
        }
        Ok(())
    }
);

shardable_key!(kvrpcpb::RawCasRequest);
collect_single!(kvrpcpb::RawCasResponse);
impl SingleKey for kvrpcpb::RawCasRequest {
    fn key(&self) -> &Vec<u8> {
        &self.key
    }
}

impl Process<kvrpcpb::RawCasResponse> for DefaultProcessor {
    type Out = (Option<Value>, bool); // (previous_value, swapped)

    fn process(&self, input: Result<kvrpcpb::RawCasResponse>) -> Result<Self::Out> {
        let input = input?;
        if input.previous_not_exist {
            Ok((None, input.succeed))
        } else {
            Ok((Some(input.previous_value), input.succeed))
        }
    }
}

type RawCoprocessorRequestDataBuilder =
    Arc<dyn Fn(metapb::Region, Vec<kvrpcpb::KeyRange>) -> Vec<u8> + Send + Sync>;

pub fn new_raw_coprocessor_request(
    copr_name: String,
    copr_version_req: String,
    ranges: Vec<kvrpcpb::KeyRange>,
    data_builder: RawCoprocessorRequestDataBuilder,
) -> RawCoprocessorRequest {
    let mut inner = kvrpcpb::RawCoprocessorRequest::default();
    inner.copr_name = copr_name;
    inner.copr_version_req = copr_version_req;
    inner.ranges = ranges;
    RawCoprocessorRequest {
        inner,
        data_builder,
    }
}

#[derive(Clone)]
pub struct RawCoprocessorRequest {
    inner: kvrpcpb::RawCoprocessorRequest,
    data_builder: RawCoprocessorRequestDataBuilder,
}

#[async_trait]
impl Request for RawCoprocessorRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        self.inner.dispatch(client, timeout).await
    }

    async fn dispatch_with_forwarded_host(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        self.inner
            .dispatch_with_forwarded_host(client, timeout, forwarded_host)
            .await
    }

    fn label(&self) -> &'static str {
        self.inner.label()
    }

    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }

    fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()> {
        self.inner.set_leader(leader)
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        self.inner.set_api_version(api_version);
    }

    fn set_is_retry_request(&mut self) {
        self.inner.set_is_retry_request();
    }

    fn set_keyspace_id(&mut self, keyspace_id: Option<u32>) {
        self.inner.set_keyspace_id(keyspace_id);
    }

    fn set_keyspace_name(&mut self, keyspace_name: Option<&str>) {
        self.inner.set_keyspace_name(keyspace_name);
    }

    fn set_max_execution_duration_ms(&mut self, duration_ms: u64) {
        self.inner.set_max_execution_duration_ms(duration_ms);
    }

    fn set_priority(&mut self, priority: kvrpcpb::CommandPri) {
        self.inner.set_priority(priority);
    }
}

impl KvRequest for RawCoprocessorRequest {
    type Response = kvrpcpb::RawCoprocessorResponse;

    fn key_mode(&self) -> Option<crate::request::KeyMode> {
        Some(crate::request::KeyMode::Raw)
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
}

impl Shardable for RawCoprocessorRequest {
    type Shard = Vec<kvrpcpb::KeyRange>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        region_stream_for_ranges(self.inner.ranges.clone(), pd_client.clone())
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.inner.ranges = shard;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.request_region())?;
        self.set_replica_read(store.is_replica_read());
        self.set_stale_read(store.stale_read);
        self.inner.data = (self.data_builder)(
            store.region_with_leader.region.clone(),
            self.inner.ranges.clone(),
        );
        Ok(())
    }
}

#[allow(clippy::type_complexity)]
impl
    Process<Vec<Result<ResponseWithShard<kvrpcpb::RawCoprocessorResponse, Vec<kvrpcpb::KeyRange>>>>>
    for DefaultProcessor
{
    type Out = Vec<(Vec<Range<Key>>, Vec<u8>)>;

    fn process(
        &self,
        input: Result<
            Vec<Result<ResponseWithShard<kvrpcpb::RawCoprocessorResponse, Vec<kvrpcpb::KeyRange>>>>,
        >,
    ) -> Result<Self::Out> {
        input?
            .into_iter()
            .map(|shard_resp| {
                shard_resp.map(|ResponseWithShard(resp, ranges)| {
                    (
                        ranges
                            .into_iter()
                            .map(|range| range.start_key.into()..range.end_key.into())
                            .collect(),
                        resp.data,
                    )
                })
            })
            .collect::<Result<Vec<_>>>()
    }
}

macro_rules! impl_raw_rpc_request {
    ($name: ident) => {
        impl RawRpcRequest for kvrpcpb::$name {
            fn set_cf(&mut self, cf: String) {
                self.cf = cf;
            }
        }
    };
}

impl_raw_rpc_request!(RawGetRequest);
impl_raw_rpc_request!(RawBatchGetRequest);
impl_raw_rpc_request!(RawGetKeyTtlRequest);
impl_raw_rpc_request!(RawPutRequest);
impl_raw_rpc_request!(RawBatchPutRequest);
impl_raw_rpc_request!(RawDeleteRequest);
impl_raw_rpc_request!(RawBatchDeleteRequest);
impl_raw_rpc_request!(RawScanRequest);
impl_raw_rpc_request!(RawBatchScanRequest);
impl_raw_rpc_request!(RawDeleteRangeRequest);
impl_raw_rpc_request!(RawCasRequest);

impl HasLocks for kvrpcpb::RawGetResponse {}

impl HasLocks for kvrpcpb::RawBatchGetResponse {}

impl HasLocks for kvrpcpb::RawGetKeyTtlResponse {}

impl HasLocks for kvrpcpb::RawPutResponse {}

impl HasLocks for kvrpcpb::RawBatchPutResponse {}

impl HasLocks for kvrpcpb::RawDeleteResponse {}

impl HasLocks for kvrpcpb::RawBatchDeleteResponse {}

impl HasLocks for kvrpcpb::RawScanResponse {}

impl HasLocks for kvrpcpb::RawBatchScanResponse {}

impl HasLocks for kvrpcpb::RawDeleteRangeResponse {}

impl HasLocks for kvrpcpb::RawCasResponse {}

impl HasLocks for kvrpcpb::RawCoprocessorResponse {}

impl HasLocks for kvrpcpb::RawChecksumResponse {}

#[cfg(test)]
mod test {
    use std::any::Any;
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::sync::Mutex;

    use super::*;
    use crate::backoff::DEFAULT_REGION_BACKOFF;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::kvrpcpb;
    use crate::request::Plan;
    use crate::request::{ApiV2Codec, KeyMode, Keyspace, KvRequest};

    #[test]
    fn api_v2_decoder_decodes_raw_batch_pair_errors_before_plan_extraction() {
        let codec = ApiV2Codec::new(KeyMode::Raw, 7).unwrap();
        let request = kvrpcpb::RawBatchGetRequest::default();
        let mut response = kvrpcpb::RawBatchGetResponse {
            pairs: vec![kvrpcpb::KvPair {
                key: codec.encode_key(b"successful-key"),
                error: Some(kvrpcpb::KeyError {
                    already_exist: Some(kvrpcpb::AlreadyExist {
                        key: codec.encode_key(b"duplicate"),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        request
            .decode_response(&mut response, Some(&codec))
            .unwrap();
        assert_eq!(
            response.pairs[0]
                .error
                .as_ref()
                .unwrap()
                .already_exist
                .as_ref()
                .unwrap()
                .key,
            b"duplicate"
        );
        assert_eq!(response.pairs[0].key, b"successful-key");
    }

    #[test]
    fn api_v2_decoder_decodes_raw_scan_and_batch_scan_pair_keys_before_merge() {
        let codec = ApiV2Codec::new(KeyMode::Raw, 7).unwrap();
        let scan_request = kvrpcpb::RawScanRequest::default();
        let batch_scan_request = kvrpcpb::RawBatchScanRequest::default();
        let pair = kvrpcpb::KvPair {
            key: codec.encode_key(b"scan-key"),
            value: b"value".to_vec(),
            ..Default::default()
        };
        let mut scan_response = kvrpcpb::RawScanResponse {
            kvs: vec![pair.clone()],
            ..Default::default()
        };
        let mut batch_scan_response = kvrpcpb::RawBatchScanResponse {
            kvs: vec![pair],
            ..Default::default()
        };

        scan_request
            .decode_response(&mut scan_response, Some(&codec))
            .unwrap();
        batch_scan_request
            .decode_response(&mut batch_scan_response, Some(&codec))
            .unwrap();
        assert_eq!(scan_response.kvs[0].key, b"scan-key");
        assert_eq!(batch_scan_response.kvs[0].key, b"scan-key");
    }

    #[test]
    fn raw_checksum_uses_crc64_xor_and_decodes_api_v2_region_bounds() {
        let codec = ApiV2Codec::new(KeyMode::Raw, 7).unwrap();
        let request = new_raw_checksum_request(b"start".to_vec(), b"end".to_vec());
        assert_eq!(
            request.algorithm,
            kvrpcpb::ChecksumAlgorithm::Crc64Xor as i32
        );
        assert_eq!(request.ranges.len(), 1);
        assert_eq!(request.ranges[0].start_key, b"start");
        assert_eq!(request.ranges[0].end_key, b"end");

        let (start_key, end_key) = codec.encode_region_range(b"start", b"end");
        let mut response = kvrpcpb::RawChecksumResponse {
            region_error: Some(crate::proto::errorpb::Error {
                key_not_in_region: Some(crate::proto::errorpb::KeyNotInRegion {
                    key: codec.encode_key(b"region-key"),
                    start_key,
                    end_key,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            checksum: 0x0f,
            total_kvs: 2,
            total_bytes: 20,
            ..Default::default()
        };
        request
            .decode_response(&mut response, Some(&codec))
            .unwrap();
        let key_not_in_region = response
            .region_error
            .as_ref()
            .unwrap()
            .key_not_in_region
            .as_ref()
            .unwrap();
        assert_eq!(key_not_in_region.key, b"region-key");
        assert_eq!(key_not_in_region.start_key, b"start");
        assert_eq!(key_not_in_region.end_key, b"end");

        let combined = Collect
            .merge(vec![
                Ok(response),
                Ok(kvrpcpb::RawChecksumResponse {
                    checksum: 0xf0,
                    total_kvs: 3,
                    total_bytes: 30,
                    ..Default::default()
                }),
            ])
            .unwrap();
        assert_eq!(combined.crc64_xor, 0xff);
        assert_eq!(combined.total_kvs, 5);
        assert_eq!(combined.total_bytes, 50);
    }

    #[tokio::test]
    async fn test_raw_scan() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                let req: &kvrpcpb::RawScanRequest = req.downcast_ref().unwrap();
                assert!(req.key_only);
                assert_eq!(req.limit, 10);

                let mut resp = kvrpcpb::RawScanResponse::default();
                for i in req.start_key[0]..req.end_key[0] {
                    let kv = kvrpcpb::KvPair {
                        key: vec![i],
                        ..Default::default()
                    };
                    resp.kvs.push(kv);
                }

                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let start: Key = vec![1].into();
        let end: Key = vec![50].into();
        let scan = kvrpcpb::RawScanRequest {
            start_key: start.into(),
            end_key: end.into(),
            limit: 10,
            key_only: true,
            ..Default::default()
        };
        let plan = crate::request::PlanBuilder::new(client, Keyspace::Disable, scan)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .merge(Collect)
            .plan();
        let scan = plan.execute().await.unwrap();

        assert_eq!(scan.len(), 49);
        // FIXME test the keys returned.
    }

    #[tokio::test]
    async fn test_raw_batch_put() -> Result<()> {
        let region1_kvs = vec![KvPair(vec![9].into(), vec![12])];
        let region1_ttls = vec![0];
        let region2_kvs = vec![
            KvPair(vec![11].into(), vec![12]),
            KvPair("FFF".to_string().as_bytes().to_vec().into(), vec![12]),
        ];
        let region2_ttls = vec![0, 1];

        let expected_map = HashMap::from([
            (region1_kvs.clone(), region1_ttls.clone()),
            (region2_kvs.clone(), region2_ttls.clone()),
        ]);

        let pairs: Vec<kvrpcpb::KvPair> = [region1_kvs, region2_kvs]
            .concat()
            .into_iter()
            .map(|kv| kv.into())
            .collect();
        let ttls = [region1_ttls, region2_ttls].concat();
        let cf = ColumnFamily::Default;

        let actual_map: Arc<Mutex<HashMap<Vec<KvPair>, Vec<u64>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let fut_actual_map = actual_map.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req: &kvrpcpb::RawBatchPutRequest = req.downcast_ref().unwrap();
                let kv_pair = req
                    .pairs
                    .clone()
                    .into_iter()
                    .map(|p| p.into())
                    .collect::<Vec<KvPair>>();
                let ttls = req.ttls.clone();
                fut_actual_map.lock().unwrap().insert(kv_pair, ttls);
                let resp = kvrpcpb::RawBatchPutResponse::default();
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let batch_put_request =
            new_raw_batch_put_request(pairs.clone(), ttls.clone(), Some(cf), false);
        let keyspace = Keyspace::Enable { keyspace_id: 0 };
        let plan = crate::request::PlanBuilder::new(client, keyspace, batch_put_request)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .plan();
        let _ = plan.execute().await;
        assert_eq!(actual_map.lock().unwrap().deref(), &expected_map);
        Ok(())
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn raw_batch_put_uses_client_go_payload_boundary_and_preserves_ttls() -> Result<()> {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = Arc::clone(&captured);
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req: &kvrpcpb::RawBatchPutRequest = req.downcast_ref().unwrap();
                captured_requests.lock().unwrap().push((
                    req.ttl,
                    req.ttls.clone(),
                    req.pairs.len(),
                ));
                Ok(Box::new(kvrpcpb::RawBatchPutResponse::default()) as Box<dyn Any>)
            },
        )));

        // Each item is exactly 8 KiB including its key. client-go appends an
        // item before testing the accumulated size, so 16 KiB is sent as one
        // batch and the third item starts the second batch.
        let pairs = vec![1_u8, 2, 3]
            .into_iter()
            .map(|key| kvrpcpb::KvPair {
                key: vec![key],
                value: vec![key; 8191],
                ..Default::default()
            })
            .collect();
        let request = new_raw_batch_put_request(pairs, vec![7, 11, 13], None, false);
        let plan = crate::request::PlanBuilder::new(client, Keyspace::Disable, request)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .plan();
        plan.execute().await?;

        let mut captured = captured.lock().unwrap().clone();
        captured.sort_by_key(|(ttl, _, _)| *ttl);
        assert_eq!(captured, [(7, vec![7, 11], 2), (13, vec![13], 1)]);
        Ok(())
    }

    #[tokio::test]
    async fn raw_batch_get_uses_client_go_key_count_boundary() -> Result<()> {
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let captured_sizes = batch_sizes.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req: &kvrpcpb::RawBatchGetRequest = req.downcast_ref().unwrap();
                captured_sizes.lock().unwrap().push(req.keys.len());
                Ok(Box::new(kvrpcpb::RawBatchGetResponse::default()) as Box<dyn Any>)
            },
        )));

        let request = new_raw_batch_get_request(vec![vec![11]; 514], None);
        let plan = crate::request::PlanBuilder::new(client, Keyspace::Disable, request)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .merge(Collect)
            .plan();
        let _: Vec<KvPair> = plan.execute().await?;

        let mut sizes = batch_sizes.lock().unwrap().clone();
        sizes.sort_unstable();
        assert_eq!(sizes, [1, 513]);
        Ok(())
    }
}
