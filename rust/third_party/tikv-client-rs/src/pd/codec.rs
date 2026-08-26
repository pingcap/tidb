// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Key encoding at the PD client boundary.
//!
//! PD stores region boundaries in TiKV's memcomparable physical-key format.
//! The native Rust request plans retain API V2 prefixes while sharding, so the
//! region cache also retains physical keys and this wrapper owns only the PD
//! memcomparable layer. Client-go's logical V2 codec remains the source of
//! truth for request/response fields and is applied at those boundaries.

use std::sync::Arc;

use async_trait::async_trait;

use crate::pd::retry::RetryClientTrait;
use crate::proto::{keyspacepb, metapb, pdpb};
use crate::region::{RegionId, RegionWithLeader, StoreId};
use crate::request::{ApiV1Codec, ApiV2Codec, KeyMode};
use crate::{Result, Timestamp};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PdRegionCodec {
    V1(ApiV1Codec),
    V2(ApiV2Codec),
}

impl PdRegionCodec {
    pub const fn v1(mode: KeyMode) -> Self {
        Self::V1(ApiV1Codec::new(mode))
    }

    pub fn v2(mode: KeyMode, keyspace_id: u32) -> Result<Self> {
        Ok(Self::V2(ApiV2Codec::new(mode, keyspace_id)?))
    }

    /// Codec for the representation actually stored by the Rust region cache.
    ///
    /// API V2 request keys already include their four-byte physical prefix in
    /// this client. PD adds only TiKV's memcomparable region-key encoding; using
    /// `ApiV2Codec::encode_region_key` here would prepend the keyspace twice.
    const fn cache_wire_codec(self) -> ApiV1Codec {
        match self {
            Self::V1(codec) => codec,
            Self::V2(_) => ApiV1Codec::new(KeyMode::Txn),
        }
    }

    pub(crate) fn encode_region_key(self, key: &[u8]) -> Vec<u8> {
        self.cache_wire_codec().encode_region_key(key)
    }

    pub(crate) fn encode_region_range(self, start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
        self.cache_wire_codec().encode_region_range(start, end)
    }

    fn decode_region_range(self, start: &[u8], end: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        self.cache_wire_codec().decode_region_range(start, end)
    }

    fn decode_bucket_keys(self, keys: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        self.cache_wire_codec().decode_bucket_keys(keys)
    }

    fn decode_region(self, mut region: RegionWithLeader) -> Result<RegionWithLeader> {
        (region.region.start_key, region.region.end_key) =
            self.decode_region_range(&region.region.start_key, &region.region.end_key)?;
        if let Some(buckets) = &mut region.buckets {
            buckets.keys = self.decode_bucket_keys(&buckets.keys)?;
        }
        Ok(region)
    }
}

/// A PD client view that owns region-key encoding while delegating all
/// keyless PD operations unchanged.
#[derive(Clone)]
pub struct CodecPdClient<C> {
    inner: Arc<C>,
    codec: PdRegionCodec,
}

impl<C> CodecPdClient<C> {
    pub const fn new(inner: Arc<C>, codec: PdRegionCodec) -> Self {
        Self { inner, codec }
    }

    pub const fn codec(&self) -> PdRegionCodec {
        self.codec
    }
}

#[async_trait]
impl<C> RetryClientTrait for CodecPdClient<C>
where
    C: RetryClientTrait + Send + Sync + 'static,
{
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        let key = self.codec.encode_region_key(&key);
        let region = self.inner.clone().get_region(key).await?;
        self.codec.decode_region(region)
    }

    async fn get_region_with_buckets(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        let key = self.codec.encode_region_key(&key);
        let region = self.inner.clone().get_region_with_buckets(key).await?;
        self.codec.decode_region(region)
    }

    async fn get_prev_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        let key = self.codec.encode_region_key(&key);
        let region = self.inner.clone().get_prev_region(key).await?;
        self.codec.decode_region(region)
    }

    async fn get_prev_region_with_buckets(
        self: Arc<Self>,
        key: Vec<u8>,
    ) -> Result<RegionWithLeader> {
        let key = self.codec.encode_region_key(&key);
        let region = self.inner.clone().get_prev_region_with_buckets(key).await?;
        self.codec.decode_region(region)
    }

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader> {
        let region = self.inner.clone().get_region_by_id(region_id).await?;
        self.codec.decode_region(region)
    }

    async fn get_region_by_id_with_buckets(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<RegionWithLeader> {
        let region = self
            .inner
            .clone()
            .get_region_by_id_with_buckets(region_id)
            .await?;
        self.codec.decode_region(region)
    }

    async fn scan_regions(
        self: Arc<Self>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<RegionWithLeader>> {
        let (start_key, end_key) = self.codec.encode_region_range(&start_key, &end_key);
        self.inner
            .clone()
            .scan_regions(start_key, end_key, limit)
            .await?
            .into_iter()
            .map(|region| self.codec.decode_region(region))
            .collect()
    }

    async fn batch_scan_regions(
        self: Arc<Self>,
        ranges: Vec<pdpb::KeyRange>,
        limit: usize,
        options: super::retry::RegionScanOptions,
    ) -> Result<Vec<RegionWithLeader>> {
        let encoded_ranges = ranges
            .into_iter()
            .map(|range| {
                let (start_key, end_key) = self
                    .codec
                    .encode_region_range(&range.start_key, &range.end_key);
                pdpb::KeyRange { start_key, end_key }
            })
            .collect();
        self.inner
            .clone()
            .batch_scan_regions(encoded_ranges, limit, options)
            .await?
            .into_iter()
            .map(|region| self.codec.decode_region(region))
            .collect()
    }

    async fn split_regions(
        self: Arc<Self>,
        split_keys: Vec<Vec<u8>>,
        retry_limit: u64,
    ) -> Result<pdpb::SplitRegionsResponse> {
        let split_keys = split_keys
            .into_iter()
            .map(|key| self.codec.encode_region_key(&key))
            .collect();
        self.inner
            .clone()
            .split_regions(split_keys, retry_limit)
            .await
    }

    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<Option<metapb::Store>> {
        self.inner.clone().get_store(id).await
    }

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
        self.inner.clone().get_all_stores().await
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        self.inner.clone().get_timestamp().await
    }

    async fn get_min_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        self.inner.clone().get_min_timestamp().await
    }

    async fn set_external_timestamp(self: Arc<Self>, timestamp: u64) -> Result<()> {
        self.inner.clone().set_external_timestamp(timestamp).await
    }

    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        self.inner.clone().get_external_timestamp().await
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool> {
        self.inner.clone().update_safepoint(safepoint).await
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        self.inner.load_keyspace(keyspace).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::pd::RegionScanOptions;

    #[derive(Clone, Debug, PartialEq)]
    enum PdCall {
        Get(Vec<u8>),
        GetWithBuckets(Vec<u8>),
        Prev(Vec<u8>),
        PrevWithBuckets(Vec<u8>),
        ById(RegionId),
        ByIdWithBuckets(RegionId),
        Scan(Vec<u8>, Vec<u8>, usize),
        BatchScan(Vec<pdpb::KeyRange>, usize, RegionScanOptions),
        Split(Vec<Vec<u8>>, u64),
    }

    struct RecordingPdClient {
        calls: Mutex<Vec<PdCall>>,
        region: RegionWithLeader,
    }

    impl RecordingPdClient {
        fn new(region: RegionWithLeader) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                region,
            }
        }

        fn calls(&self) -> Vec<PdCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl RetryClientTrait for RecordingPdClient {
        async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
            self.calls.lock().unwrap().push(PdCall::Get(key));
            Ok(self.region.clone())
        }

        async fn get_region_with_buckets(
            self: Arc<Self>,
            key: Vec<u8>,
        ) -> Result<RegionWithLeader> {
            self.calls.lock().unwrap().push(PdCall::GetWithBuckets(key));
            Ok(self.region.clone())
        }

        async fn get_prev_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
            self.calls.lock().unwrap().push(PdCall::Prev(key));
            Ok(self.region.clone())
        }

        async fn get_prev_region_with_buckets(
            self: Arc<Self>,
            key: Vec<u8>,
        ) -> Result<RegionWithLeader> {
            self.calls
                .lock()
                .unwrap()
                .push(PdCall::PrevWithBuckets(key));
            Ok(self.region.clone())
        }

        async fn get_region_by_id(
            self: Arc<Self>,
            region_id: RegionId,
        ) -> Result<RegionWithLeader> {
            self.calls.lock().unwrap().push(PdCall::ById(region_id));
            Ok(self.region.clone())
        }

        async fn get_region_by_id_with_buckets(
            self: Arc<Self>,
            region_id: RegionId,
        ) -> Result<RegionWithLeader> {
            self.calls
                .lock()
                .unwrap()
                .push(PdCall::ByIdWithBuckets(region_id));
            Ok(self.region.clone())
        }

        async fn scan_regions(
            self: Arc<Self>,
            start_key: Vec<u8>,
            end_key: Vec<u8>,
            limit: usize,
        ) -> Result<Vec<RegionWithLeader>> {
            self.calls
                .lock()
                .unwrap()
                .push(PdCall::Scan(start_key, end_key, limit));
            Ok(vec![self.region.clone()])
        }

        async fn batch_scan_regions(
            self: Arc<Self>,
            ranges: Vec<pdpb::KeyRange>,
            limit: usize,
            options: RegionScanOptions,
        ) -> Result<Vec<RegionWithLeader>> {
            self.calls
                .lock()
                .unwrap()
                .push(PdCall::BatchScan(ranges, limit, options));
            Ok(vec![self.region.clone()])
        }

        async fn split_regions(
            self: Arc<Self>,
            split_keys: Vec<Vec<u8>>,
            retry_limit: u64,
        ) -> Result<pdpb::SplitRegionsResponse> {
            self.calls
                .lock()
                .unwrap()
                .push(PdCall::Split(split_keys, retry_limit));
            Ok(pdpb::SplitRegionsResponse {
                finished_percentage: 100,
                regions_id: vec![9],
                ..Default::default()
            })
        }

        async fn get_store(self: Arc<Self>, id: StoreId) -> Result<Option<metapb::Store>> {
            Ok(Some(metapb::Store {
                id,
                ..Default::default()
            }))
        }

        async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
            Ok(Vec::new())
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
            Ok(Timestamp::default())
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
            Ok(true)
        }

        async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Ok(keyspacepb::KeyspaceMeta {
                name: keyspace.to_owned(),
                ..Default::default()
            })
        }
    }

    fn physical_region(codec: PdRegionCodec) -> RegionWithLeader {
        let (start_key, end_key) = codec.encode_region_range(b"a", b"z");
        let (_, keyspace_end) = codec.encode_region_range(b"", b"");
        RegionWithLeader {
            region: metapb::Region {
                id: 9,
                start_key,
                end_key,
                ..Default::default()
            },
            buckets: Some(metapb::Buckets {
                region_id: 9,
                version: 2,
                keys: vec![Vec::new(), codec.encode_region_key(b"middle"), keyspace_end],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn assert_decoded_region(region: &RegionWithLeader) {
        assert_eq!(region.region.start_key, b"a");
        assert_eq!(region.region.end_key, b"z");
        assert_eq!(
            region.buckets.as_ref().unwrap().keys,
            [Vec::new(), b"middle".to_vec(), Vec::new()]
        );
    }

    #[tokio::test]
    async fn native_codec_pd_client_covers_v2_region_operations() {
        let codec = PdRegionCodec::v2(KeyMode::Txn, 7).unwrap();
        let inner = Arc::new(RecordingPdClient::new(physical_region(codec)));
        let client = Arc::new(CodecPdClient::new(inner.clone(), codec));
        assert_eq!(client.codec(), codec);

        assert_decoded_region(&client.clone().get_region(b"key".to_vec()).await.unwrap());
        assert_decoded_region(
            &client
                .clone()
                .get_region_with_buckets(b"bucket-key".to_vec())
                .await
                .unwrap(),
        );
        assert_decoded_region(
            &client
                .clone()
                .get_prev_region(b"prev".to_vec())
                .await
                .unwrap(),
        );
        assert_decoded_region(
            &client
                .clone()
                .get_prev_region_with_buckets(b"prev-bucket".to_vec())
                .await
                .unwrap(),
        );
        assert_decoded_region(&client.clone().get_region_by_id(9).await.unwrap());
        assert_decoded_region(
            &client
                .clone()
                .get_region_by_id_with_buckets(9)
                .await
                .unwrap(),
        );

        let scanned = client
            .clone()
            .scan_regions(b"scan".to_vec(), Vec::new(), 8)
            .await
            .unwrap();
        assert_decoded_region(&scanned[0]);

        let ranges = vec![
            pdpb::KeyRange {
                start_key: b"a".to_vec(),
                end_key: b"b".to_vec(),
            },
            pdpb::KeyRange {
                start_key: b"c".to_vec(),
                end_key: Vec::new(),
            },
        ];
        let original_ranges = ranges.clone();
        let options = RegionScanOptions {
            need_buckets: true,
            contain_all_key_range: true,
        };
        let batch = client
            .clone()
            .batch_scan_regions(ranges, 16, options)
            .await
            .unwrap();
        assert_decoded_region(&batch[0]);
        assert_eq!(
            original_ranges[1].end_key,
            Vec::<u8>::new(),
            "the caller's ranges remain logical and unmodified"
        );

        let split = client
            .clone()
            .split_regions(vec![b"left".to_vec(), b"right".to_vec()], 3)
            .await
            .unwrap();
        assert_eq!(split.finished_percentage, 100);
        assert_eq!(split.regions_id, [9]);

        let (scan_start, scan_end) = codec.encode_region_range(b"scan", b"");
        let expected_ranges = original_ranges
            .iter()
            .map(|range| {
                let (start_key, end_key) =
                    codec.encode_region_range(&range.start_key, &range.end_key);
                pdpb::KeyRange { start_key, end_key }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            inner.calls(),
            vec![
                PdCall::Get(codec.encode_region_key(b"key")),
                PdCall::GetWithBuckets(codec.encode_region_key(b"bucket-key")),
                PdCall::Prev(codec.encode_region_key(b"prev")),
                PdCall::PrevWithBuckets(codec.encode_region_key(b"prev-bucket")),
                PdCall::ById(9),
                PdCall::ByIdWithBuckets(9),
                PdCall::Scan(scan_start, scan_end, 8),
                PdCall::BatchScan(expected_ranges, 16, options),
                PdCall::Split(
                    vec![
                        codec.encode_region_key(b"left"),
                        codec.encode_region_key(b"right"),
                    ],
                    3,
                ),
            ]
        );
    }

    #[tokio::test]
    async fn v2_pd_boundary_memencodes_an_already_physical_key_exactly_once() {
        let codec = PdRegionCodec::v2(KeyMode::Txn, 0).unwrap();
        let api = ApiV2Codec::new(KeyMode::Txn, 0).unwrap();
        let wire = ApiV1Codec::new(KeyMode::Txn);
        let physical_key = api.encode_key(b"foo");
        let physical_start = api.encode_key(b"a");
        let physical_end = api.encode_key(b"z");
        let inner = Arc::new(RecordingPdClient::new(RegionWithLeader {
            region: metapb::Region {
                id: 16,
                start_key: wire.encode_region_key(&physical_start),
                end_key: wire.encode_region_key(&physical_end),
                ..Default::default()
            },
            ..Default::default()
        }));
        let client = Arc::new(CodecPdClient::new(inner.clone(), codec));

        let region = client
            .clone()
            .get_region(physical_key.clone())
            .await
            .unwrap();

        assert_eq!(region.region.start_key, physical_start);
        assert_eq!(region.region.end_key, physical_end);
        assert_eq!(
            inner.calls(),
            [PdCall::Get(wire.encode_region_key(&physical_key))]
        );
        assert_ne!(
            wire.encode_region_key(&physical_key),
            api.encode_region_key(&physical_key),
            "the latter would prepend x\\0\\0\\0 a second time"
        );
    }

    #[tokio::test]
    async fn source_v1_raw_and_txn_pd_modes_keep_distinct_region_formats() {
        let raw = PdRegionCodec::v1(KeyMode::Raw);
        let raw_inner = Arc::new(RecordingPdClient::new(physical_region(raw)));
        let raw_client = Arc::new(CodecPdClient::new(raw_inner.clone(), raw));
        assert_decoded_region(
            &raw_client
                .clone()
                .get_region(b"raw-key".to_vec())
                .await
                .unwrap(),
        );
        assert_eq!(raw_inner.calls(), [PdCall::Get(b"raw-key".to_vec())]);

        let txn = PdRegionCodec::v1(KeyMode::Txn);
        let txn_inner = Arc::new(RecordingPdClient::new(physical_region(txn)));
        let txn_client = Arc::new(CodecPdClient::new(txn_inner.clone(), txn));
        assert_decoded_region(
            &txn_client
                .clone()
                .get_region(b"txn-key".to_vec())
                .await
                .unwrap(),
        );
        assert_eq!(
            txn_inner.calls(),
            [PdCall::Get(txn.encode_region_key(b"txn-key"))]
        );
        assert_ne!(
            raw.encode_region_key(b"same"),
            txn.encode_region_key(b"same")
        );
    }
}
