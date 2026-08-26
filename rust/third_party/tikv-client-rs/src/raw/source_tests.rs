// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::mock::{MockKvClient, MockPdClient};
use crate::proto::{kvrpcpb, metapb};
use crate::region::RegionWithLeader;
use crate::request::Keyspace;
use crate::{Error, KvPair, Result};

use super::{Client, MAX_RAW_KV_SCAN_LIMIT, RAW_BATCH_PUT_SIZE};

#[derive(Clone, Debug)]
struct Entry {
    value: Vec<u8>,
    expires_at: Option<u64>,
}

#[derive(Clone, Debug)]
struct RegionBounds {
    id: u64,
    start: Vec<u8>,
    end: Vec<u8>,
}

#[derive(Debug)]
struct StatefulRawStore {
    now: AtomicU64,
    dispatches: AtomicUsize,
    data: Mutex<BTreeMap<(String, Vec<u8>), Entry>>,
    regions: RwLock<Vec<RegionBounds>>,
}

impl StatefulRawStore {
    fn new(regions: Vec<RegionBounds>) -> Self {
        Self {
            now: AtomicU64::new(0),
            dispatches: AtomicUsize::new(0),
            data: Mutex::new(BTreeMap::new()),
            regions: RwLock::new(regions),
        }
    }

    fn set_now(&self, now: u64) {
        self.now.store(now, Ordering::SeqCst);
    }

    fn dispatches(&self) -> usize {
        self.dispatches.load(Ordering::SeqCst)
    }

    fn replace_regions(&self, regions: Vec<RegionBounds>) {
        *self.regions.write().unwrap() = regions;
    }

    fn purge_expired(&self, data: &mut BTreeMap<(String, Vec<u8>), Entry>) {
        let now = self.now.load(Ordering::SeqCst);
        data.retain(|_, entry| entry.expires_at.is_none_or(|expires_at| expires_at > now));
    }

    fn put(&self, cf: &str, key: Vec<u8>, value: Vec<u8>, ttl: u64) {
        let expires_at = (ttl != 0).then(|| self.now.load(Ordering::SeqCst) + ttl);
        self.data
            .lock()
            .unwrap()
            .insert((cf.to_owned(), key), Entry { value, expires_at });
    }

    fn get(&self, cf: &str, key: &[u8]) -> Option<Entry> {
        let mut data = self.data.lock().unwrap();
        self.purge_expired(&mut data);
        data.get(&(cf.to_owned(), key.to_vec())).cloned()
    }

    fn region_contains(&self, region_id: u64, key: &[u8]) -> bool {
        self.regions
            .read()
            .unwrap()
            .iter()
            .find(|region| region.id == region_id)
            .is_none_or(|region| {
                region.start.as_slice() <= key
                    && (region.end.is_empty() || key < region.end.as_slice())
            })
    }

    fn scan_region_id(&self, request: &kvrpcpb::RawScanRequest) -> u64 {
        self.regions
            .read()
            .unwrap()
            .iter()
            .find(|region| {
                if request.reverse {
                    region.start.as_slice() < request.start_key.as_slice()
                        && (region.end.is_empty()
                            || request.start_key.as_slice() <= region.end.as_slice())
                } else {
                    region.start.as_slice() <= request.start_key.as_slice()
                        && (region.end.is_empty()
                            || request.start_key.as_slice() < region.end.as_slice())
                }
            })
            .map_or(0, |region| region.id)
    }

    fn dispatch(&self, request: &dyn Any) -> Result<Box<dyn Any>> {
        self.dispatches.fetch_add(1, Ordering::SeqCst);

        if let Some(request) = request.downcast_ref::<kvrpcpb::RawGetRequest>() {
            return Ok(Box::new(match self.get(&request.cf, &request.key) {
                Some(entry) => kvrpcpb::RawGetResponse {
                    value: entry.value,
                    ..Default::default()
                },
                None => kvrpcpb::RawGetResponse {
                    not_found: true,
                    ..Default::default()
                },
            }));
        }

        if let Some(request) = request.downcast_ref::<kvrpcpb::RawBatchGetRequest>() {
            let pairs = request
                .keys
                .iter()
                .filter_map(|key| {
                    self.get(&request.cf, key).map(|entry| kvrpcpb::KvPair {
                        key: key.clone(),
                        value: entry.value,
                        ..Default::default()
                    })
                })
                .collect();
            return Ok(Box::new(kvrpcpb::RawBatchGetResponse {
                pairs,
                ..Default::default()
            }));
        }

        if let Some(request) = request.downcast_ref::<kvrpcpb::RawPutRequest>() {
            self.put(
                &request.cf,
                request.key.clone(),
                request.value.clone(),
                request.ttl,
            );
            return Ok(Box::new(kvrpcpb::RawPutResponse::default()));
        }

        if let Some(request) = request.downcast_ref::<kvrpcpb::RawBatchPutRequest>() {
            #[allow(deprecated)]
            let fallback_ttl = request.ttl;
            for (index, pair) in request.pairs.iter().enumerate() {
                let ttl = match request.ttls.as_slice() {
                    [] => fallback_ttl,
                    [ttl] => *ttl,
                    ttls => ttls[index],
                };
                self.put(&request.cf, pair.key.clone(), pair.value.clone(), ttl);
            }
            return Ok(Box::new(kvrpcpb::RawBatchPutResponse::default()));
        }

        if let Some(request) = request.downcast_ref::<kvrpcpb::RawGetKeyTtlRequest>() {
            let now = self.now.load(Ordering::SeqCst);
            return Ok(Box::new(match self.get(&request.cf, &request.key) {
                Some(entry) => kvrpcpb::RawGetKeyTtlResponse {
                    ttl: entry.expires_at.map_or(0, |expires_at| expires_at - now),
                    ..Default::default()
                },
                None => kvrpcpb::RawGetKeyTtlResponse {
                    not_found: true,
                    ..Default::default()
                },
            }));
        }

        if let Some(request) = request.downcast_ref::<kvrpcpb::RawDeleteRequest>() {
            self.data
                .lock()
                .unwrap()
                .remove(&(request.cf.clone(), request.key.clone()));
            return Ok(Box::new(kvrpcpb::RawDeleteResponse::default()));
        }

        if let Some(request) = request.downcast_ref::<kvrpcpb::RawBatchDeleteRequest>() {
            let mut data = self.data.lock().unwrap();
            for key in &request.keys {
                data.remove(&(request.cf.clone(), key.clone()));
            }
            return Ok(Box::new(kvrpcpb::RawBatchDeleteResponse::default()));
        }

        if let Some(request) = request.downcast_ref::<kvrpcpb::RawDeleteRangeRequest>() {
            let mut data = self.data.lock().unwrap();
            data.retain(|(cf, key), _| {
                cf != &request.cf
                    || key < &request.start_key
                    || (!request.end_key.is_empty() && key >= &request.end_key)
            });
            return Ok(Box::new(kvrpcpb::RawDeleteRangeResponse::default()));
        }

        if let Some(request) = request.downcast_ref::<kvrpcpb::RawScanRequest>() {
            let mut data = self.data.lock().unwrap();
            self.purge_expired(&mut data);
            let region_id = self.scan_region_id(request);
            let in_range = |key: &[u8]| {
                if request.reverse {
                    request.end_key.as_slice() <= key && key < request.start_key.as_slice()
                } else {
                    request.start_key.as_slice() <= key
                        && (request.end_key.is_empty() || key < request.end_key.as_slice())
                }
            };
            let pair = |key: &Vec<u8>, entry: &Entry| kvrpcpb::KvPair {
                key: key.clone(),
                value: if request.key_only {
                    Vec::new()
                } else {
                    entry.value.clone()
                },
                ..Default::default()
            };
            let mut kvs = data
                .iter()
                .filter(|((cf, key), _)| {
                    cf == &request.cf && in_range(key) && self.region_contains(region_id, key)
                })
                .map(|((_, key), entry)| pair(key, entry))
                .collect::<Vec<_>>();
            if request.reverse {
                kvs.reverse();
            }
            kvs.truncate(request.limit as usize);
            return Ok(Box::new(kvrpcpb::RawScanResponse {
                kvs,
                ..Default::default()
            }));
        }

        if let Some(request) = request.downcast_ref::<kvrpcpb::RawCasRequest>() {
            let previous = self.get(&request.cf, &request.key);
            let succeeds = if request.previous_not_exist {
                previous.is_none()
            } else {
                previous
                    .as_ref()
                    .is_some_and(|entry| entry.value == request.previous_value)
            };
            if succeeds {
                if request.delete {
                    self.data
                        .lock()
                        .unwrap()
                        .remove(&(request.cf.clone(), request.key.clone()));
                } else {
                    self.put(
                        &request.cf,
                        request.key.clone(),
                        request.value.clone(),
                        request.ttl,
                    );
                }
            }
            return Ok(Box::new(kvrpcpb::RawCasResponse {
                succeed: succeeds,
                previous_not_exist: previous.is_none(),
                previous_value: previous.map_or_else(Vec::new, |entry| entry.value),
                ..Default::default()
            }));
        }

        if let Some(request) = request.downcast_ref::<kvrpcpb::RawChecksumRequest>() {
            let mut data = self.data.lock().unwrap();
            self.purge_expired(&mut data);
            let range = request.ranges.first().expect("raw checksum range");
            let mut response = kvrpcpb::RawChecksumResponse::default();
            for ((cf, key), entry) in data.iter() {
                if !cf.is_empty()
                    || key < &range.start_key
                    || (!range.end_key.is_empty() && key >= &range.end_key)
                {
                    continue;
                }
                let checksum_key =
                    if request.context.as_ref().is_some_and(|context| {
                        context.api_version == kvrpcpb::ApiVersion::V2 as i32
                    }) {
                        key.get(4..).expect("API V2 raw key prefix").to_vec()
                    } else {
                        key.clone()
                    };
                let mut pair = checksum_key;
                pair.extend_from_slice(&entry.value);
                response.checksum ^= crc64_ecma(&pair);
                response.total_kvs += 1;
                response.total_bytes += (key.len() + entry.value.len()) as u64;
            }
            return Ok(Box::new(response));
        }

        panic!("unexpected stateful RawKV request type")
    }
}

fn crc64_ecma(bytes: &[u8]) -> u64 {
    const POLYNOMIAL: u64 = 0x42f0_e1eb_a9ea_3693;
    let mut crc = 0_u64;
    for byte in bytes {
        crc ^= u64::from(*byte) << 56;
        for _ in 0..8 {
            crc = if crc & (1 << 63) != 0 {
                (crc << 1) ^ POLYNOMIAL
            } else {
                crc << 1
            };
        }
    }
    crc
}

fn region(bounds: &RegionBounds, store_id: u64) -> RegionWithLeader {
    let mut region = RegionWithLeader::default();
    region.region.id = bounds.id;
    region.region.start_key = bounds.start.clone();
    region.region.end_key = bounds.end.clone();
    region.region.region_epoch = Some(metapb::RegionEpoch {
        conf_ver: 1,
        version: 1,
    });
    region.leader = Some(metapb::Peer {
        id: bounds.id + 100,
        store_id,
        ..Default::default()
    });
    region
}

fn source_regions() -> Vec<RegionBounds> {
    regions_with_splits(&[b"k3", b"k6"])
}

fn regions_with_splits(split_keys: &[&[u8]]) -> Vec<RegionBounds> {
    let mut start = Vec::new();
    let mut regions = Vec::with_capacity(split_keys.len() + 1);
    for (index, split_key) in split_keys.iter().enumerate() {
        regions.push(RegionBounds {
            id: index as u64 + 1,
            start: start.clone(),
            end: split_key.to_vec(),
        });
        start = split_key.to_vec();
    }
    regions.push(RegionBounds {
        id: regions.len() as u64 + 1,
        start,
        end: Vec::new(),
    });
    regions
}

fn region_metas(regions: &[RegionBounds]) -> Vec<RegionWithLeader> {
    regions
        .iter()
        .enumerate()
        .map(|(index, bounds)| region(bounds, index as u64 + 41))
        .collect()
}

fn stateful_fixture_with(
    regions: Vec<RegionBounds>,
    keyspace: Keyspace,
) -> (
    Client<MockPdClient>,
    Arc<StatefulRawStore>,
    Arc<MockPdClient>,
) {
    let state = Arc::new(StatefulRawStore::new(regions.clone()));
    let dispatch_state = state.clone();
    let kv_client =
        MockKvClient::with_dispatch_hook(move |request| dispatch_state.dispatch(request));
    let pd_client = Arc::new(MockPdClient::with_client_and_regions(
        kv_client,
        region_metas(&regions),
    ));
    let client = Client::from_test_rpc(
        pd_client.clone(),
        keyspace,
        matches!(keyspace, Keyspace::Enable { .. }).then(|| "DEFAULT".to_owned()),
    );
    (client, state, pd_client)
}

fn stateful_client_with(
    regions: Vec<RegionBounds>,
    keyspace: Keyspace,
) -> (Client<MockPdClient>, Arc<StatefulRawStore>) {
    let (client, state, _) = stateful_fixture_with(regions, keyspace);
    (client, state)
}

fn stateful_client() -> (Client<MockPdClient>, Arc<StatefulRawStore>) {
    stateful_client_with(source_regions(), Keyspace::Disable)
}

fn replace_stateful_regions(
    state: &StatefulRawStore,
    pd_client: &MockPdClient,
    regions: Vec<RegionBounds>,
) {
    state.replace_regions(regions.clone());
    pd_client.replace_regions(region_metas(&regions));
}

#[tokio::test]
async fn source_delete_range_walks_regions_in_order_and_stops_at_the_first_error() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded_calls = calls.clone();
    let kv_client = MockKvClient::with_dispatch_hook(move |request| {
        let request = request
            .downcast_ref::<kvrpcpb::RawDeleteRangeRequest>()
            .expect("delete-range request");
        let region_id = request.context.as_ref().expect("request context").region_id;
        recorded_calls.lock().unwrap().push(region_id);
        Ok(Box::new(kvrpcpb::RawDeleteRangeResponse {
            error: (region_id == 2)
                .then_some("delete range failed".to_owned())
                .unwrap_or_default(),
            ..Default::default()
        }))
    });
    let regions = source_regions();
    let pd_client = MockPdClient::with_client_and_regions(
        kv_client,
        regions
            .iter()
            .enumerate()
            .map(|(index, bounds)| region(bounds, index as u64 + 41))
            .collect(),
    );
    let client = Client::from_test_rpc(Arc::new(pd_client), Keyspace::Disable, None);

    let error = client
        .delete_range(b"a".to_vec()..b"z".to_vec())
        .await
        .unwrap_err();

    assert_eq!(*calls.lock().unwrap(), vec![1, 2]);
    assert_eq!(error.to_string(), "delete range failed");
}

#[tokio::test]
async fn source_checksum_walks_regions_in_order_and_ignores_response_error_text() -> Result<()> {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded_calls = calls.clone();
    let kv_client = MockKvClient::with_dispatch_hook(move |request| {
        let request = request
            .downcast_ref::<kvrpcpb::RawChecksumRequest>()
            .expect("checksum request");
        let region_id = request.context.as_ref().expect("request context").region_id;
        recorded_calls.lock().unwrap().push(region_id);
        Ok(Box::new(kvrpcpb::RawChecksumResponse {
            checksum: region_id,
            total_kvs: 1,
            total_bytes: region_id,
            // Pinned client-go never reads RawChecksumResponse.Error.
            error: "ignored by client-go".to_owned(),
            ..Default::default()
        }))
    });
    let regions = source_regions();
    let pd_client = MockPdClient::with_client_and_regions(
        kv_client,
        regions
            .iter()
            .enumerate()
            .map(|(index, bounds)| region(bounds, index as u64 + 41))
            .collect(),
    );
    let client = Client::from_test_rpc(Arc::new(pd_client), Keyspace::Disable, None);

    let checksum = client.checksum(b"a".to_vec()..b"z".to_vec()).await?;

    assert_eq!(checksum.crc64_xor, 1 ^ 2 ^ 3);
    assert_eq!(checksum.total_kvs, 3);
    assert_eq!(checksum.total_bytes, 1 + 2 + 3);
    assert_eq!(*calls.lock().unwrap(), vec![1, 2, 3]);
    Ok(())
}

#[tokio::test]
async fn source_batch_get_and_scan_ignore_legacy_pair_errors() -> Result<()> {
    let kv_client = MockKvClient::with_dispatch_hook(move |request| {
        if let Some(request) = request.downcast_ref::<kvrpcpb::RawBatchGetRequest>() {
            return Ok(Box::new(kvrpcpb::RawBatchGetResponse {
                pairs: vec![kvrpcpb::KvPair {
                    key: request.keys[0].clone(),
                    value: b"batch-value".to_vec(),
                    error: Some(kvrpcpb::KeyError {
                        abort: "ignored by client-go RawBatchGet".to_owned(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }));
        }
        let request = request
            .downcast_ref::<kvrpcpb::RawScanRequest>()
            .expect("raw scan request");
        Ok(Box::new(kvrpcpb::RawScanResponse {
            kvs: vec![kvrpcpb::KvPair {
                key: request.start_key.clone(),
                value: b"scan-value".to_vec(),
                error: Some(kvrpcpb::KeyError {
                    abort: "ignored by client-go RawScan".to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }))
    });
    let client = Client::from_test_rpc(
        Arc::new(MockPdClient::new(kv_client)),
        Keyspace::Disable,
        None,
    );

    assert_eq!(
        client.batch_get([b"batch-key".to_vec()]).await?,
        vec![Some(b"batch-value".to_vec())]
    );
    assert_eq!(
        pairs_bytes(client.scan(b"scan-key".to_vec()..b"z".to_vec(), 1).await?),
        vec![(b"scan-key".to_vec(), b"scan-value".to_vec())]
    );
    Ok(())
}

#[tokio::test]
async fn source_scan_treats_the_request_limit_as_server_enforced() -> Result<()> {
    let kv_client = MockKvClient::with_dispatch_hook(move |request| {
        let request = request
            .downcast_ref::<kvrpcpb::RawScanRequest>()
            .expect("raw scan request");
        assert_eq!(request.limit, 1);
        Ok(Box::new(kvrpcpb::RawScanResponse {
            // client-go appends the complete response and does not truncate a
            // malformed server response that exceeds the requested limit.
            kvs: vec![
                kvrpcpb::KvPair {
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                    ..Default::default()
                },
                kvrpcpb::KvPair {
                    key: b"b".to_vec(),
                    value: b"2".to_vec(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }))
    });
    let client = Client::from_test_rpc(
        Arc::new(MockPdClient::new(kv_client)),
        Keyspace::Disable,
        None,
    );

    assert_eq!(
        pairs_bytes(client.scan(b"a".to_vec()..b"z".to_vec(), 1).await?),
        expected_pairs(&[("a", "1"), ("b", "2")])
    );
    Ok(())
}

fn pairs_bytes(pairs: Vec<KvPair>) -> Vec<(Vec<u8>, Vec<u8>)> {
    pairs
        .into_iter()
        .map(|pair| (pair.0.into(), pair.1))
        .collect()
}

fn expected_pairs(pairs: &[(&str, &str)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    pairs
        .iter()
        .map(|(key, value)| (key.as_bytes().to_vec(), value.as_bytes().to_vec()))
        .collect()
}

fn owned_pairs(pairs: &[(&str, &str)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    expected_pairs(pairs)
}

async fn assert_source_forward_scan_table(client: &Client<MockPdClient>) -> Result<()> {
    assert_eq!(
        pairs_bytes(client.scan(Vec::<u8>::new().., 1).await?),
        expected_pairs(&[("k1", "v1")])
    );
    assert_eq!(
        pairs_bytes(client.scan(b"k1".to_vec().., 2).await?),
        expected_pairs(&[("k1", "v1"), ("k3", "v3")])
    );
    assert_eq!(
        pairs_bytes(client.scan(Vec::<u8>::new().., 10).await?),
        expected_pairs(&[("k1", "v1"), ("k3", "v3"), ("k5", "v5"), ("k7", "v7")])
    );
    assert_eq!(
        pairs_bytes(client.scan(b"k2".to_vec().., 2).await?),
        expected_pairs(&[("k3", "v3"), ("k5", "v5")])
    );
    assert_eq!(
        pairs_bytes(client.scan(b"k2".to_vec().., 3).await?),
        expected_pairs(&[("k3", "v3"), ("k5", "v5"), ("k7", "v7")])
    );
    assert!(client
        .scan(Vec::<u8>::new()..b"k1".to_vec(), 1)
        .await?
        .is_empty());
    assert_eq!(
        pairs_bytes(client.scan(b"k1".to_vec()..b"k3".to_vec(), 2).await?),
        expected_pairs(&[("k1", "v1")])
    );
    assert_eq!(
        pairs_bytes(client.scan(b"k1".to_vec()..b"k5".to_vec(), 10).await?),
        expected_pairs(&[("k1", "v1"), ("k3", "v3")])
    );
    assert_eq!(
        pairs_bytes(client.scan(b"k1".to_vec()..b"k5\0".to_vec(), 10).await?),
        expected_pairs(&[("k1", "v1"), ("k3", "v3"), ("k5", "v5")])
    );
    assert!(client
        .scan(b"k5\0".to_vec()..b"k5\0\0".to_vec(), 10)
        .await?
        .is_empty());
    Ok(())
}

async fn assert_source_reverse_scan_table(client: &Client<MockPdClient>) -> Result<()> {
    assert!(client
        .scan_reverse(Vec::<u8>::new().., 10)
        .await?
        .is_empty());
    assert_eq!(
        pairs_bytes(
            client
                .scan_reverse(Vec::<u8>::new()..b"z".to_vec(), 1)
                .await?
        ),
        expected_pairs(&[("k7", "v7")])
    );
    assert_eq!(
        pairs_bytes(
            client
                .scan_reverse(Vec::<u8>::new()..b"z".to_vec(), 2)
                .await?
        ),
        expected_pairs(&[("k7", "v7"), ("k5", "v5")])
    );
    assert_eq!(
        pairs_bytes(
            client
                .scan_reverse(Vec::<u8>::new()..b"z".to_vec(), 10)
                .await?
        ),
        expected_pairs(&[("k7", "v7"), ("k5", "v5"), ("k3", "v3"), ("k1", "v1")])
    );
    assert_eq!(
        pairs_bytes(
            client
                .scan_reverse(Vec::<u8>::new()..b"k2".to_vec(), 10)
                .await?
        ),
        expected_pairs(&[("k1", "v1")])
    );
    assert_eq!(
        pairs_bytes(
            client
                .scan_reverse(Vec::<u8>::new()..b"k6".to_vec(), 2)
                .await?
        ),
        expected_pairs(&[("k5", "v5"), ("k3", "v3")])
    );
    assert_eq!(
        pairs_bytes(
            client
                .scan_reverse(Vec::<u8>::new()..b"k5".to_vec(), 1)
                .await?
        ),
        expected_pairs(&[("k3", "v3")])
    );
    assert_eq!(
        pairs_bytes(
            client
                .scan_reverse(Vec::<u8>::new()..b"k5\0".to_vec(), 1)
                .await?
        ),
        expected_pairs(&[("k5", "v5")])
    );
    assert_eq!(
        pairs_bytes(
            client
                .scan_reverse(Vec::<u8>::new()..b"k6".to_vec(), 3)
                .await?
        ),
        expected_pairs(&[("k5", "v5"), ("k3", "v3"), ("k1", "v1")])
    );
    assert_eq!(
        pairs_bytes(
            client
                .scan_reverse(b"k3".to_vec()..b"z".to_vec(), 10)
                .await?
        ),
        expected_pairs(&[("k7", "v7"), ("k5", "v5"), ("k3", "v3")])
    );
    assert_eq!(
        pairs_bytes(
            client
                .scan_reverse(b"k3\0".to_vec()..b"k7".to_vec(), 10)
                .await?
        ),
        expected_pairs(&[("k5", "v5")])
    );
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_rawkv_TestColumnFamilyForClient() -> Result<()> {
    let (mut client, _) = stateful_client();
    client.set_column_family("cf1");
    client
        .put(b"test_key_cf1".to_vec(), b"test_value_cf1".to_vec())
        .await?;
    client.set_column_family("cf2");
    client
        .put(b"test_key_cf2".to_vec(), b"test_value_cf2".to_vec())
        .await?;

    client.set_column_family("cf1");
    assert_eq!(
        client.get(b"test_key_cf1".to_vec()).await?,
        Some(b"test_value_cf1".to_vec())
    );
    assert_eq!(client.get(b"test_key_cf2".to_vec()).await?, None);
    client.set_column_family("cf2");
    assert_eq!(
        client.get(b"test_key_cf2".to_vec()).await?,
        Some(b"test_value_cf2".to_vec())
    );
    assert_eq!(client.get(b"test_key_cf1".to_vec()).await?, None);
    client.set_column_family("");
    assert_eq!(client.get(b"test_key_cf1".to_vec()).await?, None);
    assert_eq!(client.get(b"test_key_cf2".to_vec()).await?, None);

    client.set_column_family("cf1");
    client.delete(b"test_key_cf1".to_vec()).await?;
    assert_eq!(client.get(b"test_key_cf1".to_vec()).await?, None);
    client.set_column_family("cf2");
    client.delete(b"test_key_cf2".to_vec()).await?;
    assert_eq!(client.get(b"test_key_cf2".to_vec()).await?, None);
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_rawkv_TestColumnFamilyForOptions() -> Result<()> {
    let (client, _) = stateful_client();
    let cf1 = client.with_cf_name("cf1");
    let cf2 = client.with_cf_name("cf2");
    cf1.put(b"db".to_vec(), b"TiDB".to_vec()).await?;
    cf2.put(b"kv".to_vec(), b"TiKV".to_vec()).await?;
    assert_eq!(cf1.get(b"db".to_vec()).await?, Some(b"TiDB".to_vec()));
    assert_eq!(cf1.get(b"kv".to_vec()).await?, None);
    assert_eq!(cf2.get(b"kv".to_vec()).await?, Some(b"TiKV".to_vec()));
    assert_eq!(cf2.get(b"db".to_vec()).await?, None);
    cf1.delete(b"db".to_vec()).await?;
    cf2.delete(b"kv".to_vec()).await?;
    assert_eq!(cf1.get(b"db".to_vec()).await?, None);
    assert_eq!(cf2.get(b"kv".to_vec()).await?, None);
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_integration_raw_api_mock_TestScan() -> Result<()> {
    let (client, state, pd_client) =
        stateful_fixture_with(regions_with_splits(&[]), Keyspace::Disable);
    client
        .batch_put(owned_pairs(&[
            ("k1", "v1"),
            ("k3", "v3"),
            ("k5", "v5"),
            ("k7", "v7"),
        ]))
        .await?;
    assert_source_forward_scan_table(&client).await?;
    replace_stateful_regions(&state, &pd_client, regions_with_splits(&[b"k2"]));
    assert_source_forward_scan_table(&client).await?;
    replace_stateful_regions(&state, &pd_client, regions_with_splits(&[b"k2", b"k5"]));
    assert_source_forward_scan_table(&client).await?;
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_rawkv_TestScan() -> Result<()> {
    let (client, _) = stateful_client();
    let cf = client.with_cf_name("test_cf");
    cf.batch_put(owned_pairs(&[
        ("db", "TiDB"),
        ("key2", "value2"),
        ("key1", "value1"),
        ("key4", "value4"),
        ("key3", "value3"),
        ("kv", "TiKV"),
    ]))
    .await?;
    assert_eq!(
        pairs_bytes(cf.scan(b"key1".to_vec()..b"keyz".to_vec(), 3).await?),
        expected_pairs(&[("key1", "value1"), ("key2", "value2"), ("key3", "value3"),])
    );
    assert_eq!(
        cf.scan_keys_reverse(Vec::<u8>::new()..b"key3".to_vec(), 10)
            .await?,
        vec![
            b"key2".to_vec().into(),
            b"key1".to_vec().into(),
            b"db".to_vec().into(),
        ]
    );
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_integration_raw_api_mock_TestSplit() -> Result<()> {
    let (client, state, pd_client) =
        stateful_fixture_with(regions_with_splits(&[]), Keyspace::Disable);
    client.put(b"k1".to_vec(), b"v1".to_vec()).await?;
    client.put(b"k3".to_vec(), b"v3".to_vec()).await?;
    replace_stateful_regions(&state, &pd_client, regions_with_splits(&[b"k2"]));
    assert_eq!(client.get(b"k1".to_vec()).await?, Some(b"v1".to_vec()));
    assert_eq!(client.get(b"k3".to_vec()).await?, Some(b"v3".to_vec()));
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_integration_raw_api_mock_TestReverseScan() -> Result<()> {
    let (client, state, pd_client) =
        stateful_fixture_with(regions_with_splits(&[]), Keyspace::Disable);
    client
        .batch_put(owned_pairs(&[
            ("k1", "v1"),
            ("k3", "v3"),
            ("k5", "v5"),
            ("k7", "v7"),
        ]))
        .await?;
    assert_source_reverse_scan_table(&client).await?;
    replace_stateful_regions(&state, &pd_client, regions_with_splits(&[b"k2"]));
    assert_source_reverse_scan_table(&client).await?;
    replace_stateful_regions(&state, &pd_client, regions_with_splits(&[b"k2", b"k5"]));
    assert_source_reverse_scan_table(&client).await?;
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_integration_raw_api_TestReverseScan() -> Result<()> {
    let (client, _) = stateful_client();
    let prefix = b"test_reverse_scan";
    let pairs = (0..10)
        .map(|index| {
            (
                [prefix.as_slice(), format!("key:{index}").as_bytes()].concat(),
                format!("value:{index}").into_bytes(),
            )
        })
        .collect::<Vec<_>>();
    client.batch_put(pairs).await?;

    let source_pairs = pairs_bytes(
        client
            .scan_reverse(prefix.to_vec()..[prefix.as_slice(), b"key:"].concat(), 5)
            .await?,
    );
    for (index, (key, value)) in source_pairs.iter().rev().enumerate() {
        assert_eq!(key, &format!("test_reverse_scankey:{index}").into_bytes());
        assert_eq!(value, &format!("value:{index}").into_bytes());
    }

    // The pinned source bounds are empty in lexical order, making its loop
    // vacuous. Keep the exact call above and add the intended non-empty
    // reverse-limit check over the same data.
    assert_eq!(
        pairs_bytes(
            client
                .scan_reverse(prefix.to_vec()..[prefix.as_slice(), b"key;"].concat(), 5)
                .await?
        ),
        (5..10)
            .rev()
            .map(|index| {
                (
                    format!("test_reverse_scankey:{index}").into_bytes(),
                    format!("value:{index}").into_bytes(),
                )
            })
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_rawkv_TestBatch() -> Result<()> {
    let (client, _) = stateful_client();
    let cf = client.with_cf_name("test_cf");
    let pairs = [
        ("db", "TiDB"),
        ("key2", "value2"),
        ("key1", "value1"),
        ("key3", "value3"),
        ("kv", "TiKV"),
    ];
    cf.batch_put(owned_pairs(&pairs)).await?;
    assert_eq!(
        cf.batch_get(pairs.iter().map(|(key, _)| key.as_bytes().to_vec()))
            .await?,
        pairs
            .iter()
            .map(|(_, value)| Some(value.as_bytes().to_vec()))
            .collect::<Vec<_>>()
    );
    cf.batch_delete(pairs.iter().map(|(key, _)| key.as_bytes().to_vec()))
        .await?;
    assert_eq!(cf.get(b"db".to_vec()).await?, None);
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_rawkv_TestCompareAndSwap() -> Result<()> {
    let (client, _) = stateful_client();
    let mut cas = client.with_cf_name("my_cf");
    cas.put(b"kv".to_vec(), b"TiDB".to_vec()).await?;
    assert_eq!(
        cas.compare_and_swap(b"kv".to_vec(), Some(b"TiDB".to_vec()), b"TiKV".to_vec(),)
            .await
            .unwrap_err()
            .to_string(),
        "using CompareAndSwap without enable atomic mode"
    );
    cas.set_atomic_for_cas(true);
    assert_eq!(
        cas.compare_and_swap(b"kv".to_vec(), Some(b"TiKV".to_vec()), b"TiKV".to_vec(),)
            .await?,
        (Some(b"TiDB".to_vec()), false)
    );
    assert_eq!(
        cas.compare_and_swap(b"kv".to_vec(), Some(b"TiDB".to_vec()), b"TiKV".to_vec(),)
            .await?,
        (Some(b"TiDB".to_vec()), true)
    );
    assert_eq!(cas.get(b"kv".to_vec()).await?, Some(b"TiKV".to_vec()));
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_integration_raw_api_mock_TestDeleteRange() -> Result<()> {
    let (client, state, pd_client) =
        stateful_fixture_with(regions_with_splits(&[]), Keyspace::Disable);
    let mut expected = BTreeMap::new();
    for prefix in b'a'..=b'd' {
        for suffix in b'0'..=b'9' {
            let key = vec![prefix, suffix];
            let value = vec![b'v', prefix, suffix];
            expected.insert(key, value);
        }
    }
    client
        .batch_put(
            expected
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        )
        .await?;
    replace_stateful_regions(&state, &pd_client, regions_with_splits(&[b"b", b"c", b"d"]));

    let cases = [
        (b"b".to_vec(), b"c0".to_vec()),
        (b"c11".to_vec(), b"c12".to_vec()),
        (b"d0".to_vec(), b"d0".to_vec()),
        (b"c5".to_vec(), b"d5".to_vec()),
        (b"a".to_vec(), b"z".to_vec()),
    ];
    for (start, end) in cases {
        client.delete_range(start.clone()..end.clone()).await?;
        expected.retain(|key, _| key < &start || key >= &end);
        assert_eq!(
            pairs_bytes(client.scan(Vec::<u8>::new().., 100).await?),
            expected
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>()
        );
    }

    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_rawkv_TestDeleteRange() -> Result<()> {
    let (client, _) = stateful_client();
    let cf = client.with_cf_name("test_cf");
    cf.batch_put(owned_pairs(&[
        ("db", "TiDB"),
        ("key2", "value2"),
        ("key1", "value1"),
        ("key4", "value4"),
        ("key3", "value3"),
        ("kv", "TiKV"),
    ]))
    .await?;
    cf.delete_range(b"key3".to_vec()..).await?;
    assert!(cf.scan(b"key3".to_vec().., 10).await?.is_empty());
    assert_eq!(cf.scan(Vec::<u8>::new().., 10).await?.len(), 3);
    cf.delete_range(Vec::<u8>::new()..).await?;
    assert!(cf.scan(Vec::<u8>::new().., 10).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_rawkv_TestDeleteRangeEmptyKeysMultiRegion() -> Result<()> {
    let (client, _) = stateful_client_with(regions_with_splits(&[b"key3"]), Keyspace::Disable);
    let cf = client.with_cf_name("test_cf");
    cf.batch_put(owned_pairs(&[
        ("db", "TiDB"),
        ("key2", "value2"),
        ("key1", "value1"),
        ("key4", "value4"),
        ("key3", "value3"),
        ("kv", "TiKV"),
    ]))
    .await?;
    assert_eq!(cf.scan(Vec::<u8>::new().., 10).await?.len(), 6);
    cf.delete_range(Vec::<u8>::new()..).await?;
    assert!(cf.scan(Vec::<u8>::new().., 10).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_rawkv_TestRawChecksum() -> Result<()> {
    let (client, _) = stateful_client();
    let pairs = [
        ("db", "TiDB"),
        ("key2", "value2"),
        ("key1", "value1"),
        ("key4", "value4"),
        ("key3", "value3"),
        ("kv", "TiKV"),
    ];
    client.batch_put(owned_pairs(&pairs)).await?;
    let expected_crc = pairs.iter().fold(0, |checksum, (key, value)| {
        let mut pair = key.as_bytes().to_vec();
        pair.extend_from_slice(value.as_bytes());
        checksum ^ crc64_ecma(&pair)
    });
    let checksum = client.checksum(b"db".to_vec()..).await?;
    assert_eq!(checksum.crc64_xor, expected_crc);
    assert_eq!(checksum.total_kvs, pairs.len() as u64);
    assert_eq!(
        checksum.total_bytes,
        pairs
            .iter()
            .map(|(key, value)| (key.len() + value.len()) as u64)
            .sum::<u64>()
    );
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_integration_raw_api_mock_TestRawBatch() -> Result<()> {
    let (client, state, pd_client) =
        stateful_fixture_with(regions_with_splits(&[]), Keyspace::Disable);
    let mut pairs = Vec::new();
    let mut size = 0_usize;
    let mut index = 0_usize;
    while size / (RAW_BATCH_PUT_SIZE as usize) < 4 {
        let key = format!("key{index}");
        let value = format!("value{index}");
        size += key.len() + value.len();
        assert_eq!(client.get(key.as_bytes().to_vec()).await?, None);
        pairs.push((key.into_bytes(), value.into_bytes()));
        index += 1;
    }
    let split_key = format!("key{}", index.saturating_sub(1) / 2).into_bytes();
    replace_stateful_regions(
        &state,
        &pd_client,
        regions_with_splits(&[split_key.as_slice()]),
    );
    let before_put = state.dispatches();
    client.batch_put(pairs.clone()).await?;
    assert!(state.dispatches() - before_put >= 4);
    assert_eq!(
        client
            .batch_get(pairs.iter().map(|(key, _)| key.clone()))
            .await?,
        pairs
            .iter()
            .map(|(_, value)| Some(value.clone()))
            .collect::<Vec<_>>()
    );
    client
        .batch_delete(pairs.iter().map(|(key, _)| key.clone()))
        .await?;
    assert!(client
        .batch_get(pairs.iter().map(|(key, _)| key.clone()))
        .await?
        .into_iter()
        .all(|value| value.is_none()));
    Ok(())
}

fn scale_pairs(count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|index| {
            (
                format!("key@{index}").into_bytes(),
                format!("value@{index}").into_bytes(),
            )
        })
        .collect()
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_integration_raw_api_TestScan() -> Result<()> {
    const SOURCE_SCALE: usize = 20_480;
    let (scan_client, state, pd_client) =
        stateful_fixture_with(regions_with_splits(&[]), Keyspace::Disable);
    let pairs = scale_pairs(SOURCE_SCALE);
    scan_client.batch_put(pairs.clone()).await?;
    let mut split_keys = (0..SOURCE_SCALE)
        .step_by(1_024)
        .map(|index| format!("key@{index}").into_bytes())
        .collect::<Vec<_>>();
    split_keys.sort();
    let split_refs = split_keys.iter().map(Vec::as_slice).collect::<Vec<_>>();
    replace_stateful_regions(&state, &pd_client, regions_with_splits(&split_refs));
    let scanned = scan_client
        .scan(
            Vec::<u8>::new()..,
            MAX_RAW_KV_SCAN_LIMIT.load(Ordering::Relaxed),
        )
        .await?;
    assert_eq!(scanned.len(), 10_240);
    for pair in scanned {
        let key: &[u8] = pair.key().as_ref();
        assert!(key.starts_with(b"key@"));
        assert!(pair.value().starts_with(b"value@"));
    }

    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_integration_raw_api_TestDeleteRange() -> Result<()> {
    const SOURCE_SCALE: usize = 20_480;
    let (client, state, pd_client) =
        stateful_fixture_with(regions_with_splits(&[]), Keyspace::Disable);
    client.batch_put(scale_pairs(SOURCE_SCALE)).await?;
    replace_stateful_regions(&state, &pd_client, regions_with_splits(&[b"key@4096"]));
    client.delete_range(Vec::<u8>::new()..).await?;
    for key in [b"key@0".as_slice(), b"key@1", b"key@2"] {
        assert_eq!(client.get(key.to_vec()).await?, None);
    }
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_integration_raw_api_TestTTL() -> Result<()> {
    let (client, state) = stateful_client();
    state.set_now(100);
    client
        .put_with_ttl(b"key".to_vec(), b"value".to_vec(), 2)
        .await?;
    state.set_now(101);
    assert_eq!(client.get_key_ttl_secs(b"key".to_vec()).await?, Some(1));
    state.set_now(102);
    assert_eq!(client.get(b"key".to_vec()).await?, None);
    assert_eq!(client.get_key_ttl_secs(b"key".to_vec()).await?, None);
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_integration_raw_api_TestEmptyValue() -> Result<()> {
    let (client, _) = stateful_client();
    let mut atomic = client.clone();
    atomic.set_atomic_for_cas(true);

    assert_eq!(client.get(b"key".to_vec()).await?, None);
    client.put(b"key".to_vec(), Vec::new()).await?;
    assert_eq!(client.get(b"key".to_vec()).await?, Some(Vec::new()));
    assert_eq!(
        client
            .batch_get([b"key".to_vec(), b"key1".to_vec()])
            .await?,
        vec![Some(Vec::new()), None]
    );
    assert_eq!(
        pairs_bytes(client.scan(b"key".to_vec()..b"keyz".to_vec(), 10).await?),
        vec![(b"key".to_vec(), Vec::new())]
    );
    assert_eq!(
        pairs_bytes(
            client
                .scan_reverse(b"key".to_vec()..b"keyz".to_vec(), 10)
                .await?
        ),
        vec![(b"key".to_vec(), Vec::new())]
    );

    client.delete(b"key".to_vec()).await?;
    assert_eq!(
        client
            .batch_get([b"key".to_vec(), b"key1".to_vec()])
            .await?,
        vec![None, None]
    );
    assert!(client
        .scan(b"key".to_vec()..b"keyz".to_vec(), 10)
        .await?
        .is_empty());
    assert!(client
        .scan_reverse(b"key".to_vec()..b"keyz".to_vec(), 10)
        .await?
        .is_empty());

    client.batch_put([(b"key".to_vec(), Vec::new())]).await?;
    assert_eq!(client.get(b"key".to_vec()).await?, Some(Vec::new()));
    client.delete(b"key".to_vec()).await?;
    assert_eq!(
        atomic
            .compare_and_swap(b"key".to_vec(), None, Vec::new())
            .await?,
        (None, true)
    );
    assert_eq!(client.get(b"key".to_vec()).await?, Some(Vec::new()));
    assert_eq!(
        atomic
            .compare_and_swap(b"key".to_vec(), Some(Vec::new()), b"val".to_vec())
            .await?,
        (Some(Vec::new()), true)
    );
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_integration_raw_api_TestRawChecksum() -> Result<()> {
    const SOURCE_SCALE: usize = 20_480;
    let pairs = scale_pairs(SOURCE_SCALE);
    for keyspace in [Keyspace::Disable, Keyspace::Enable { keyspace_id: 7 }] {
        let (client, _) = stateful_client_with(regions_with_splits(&[]), keyspace);
        client.batch_put(pairs.clone()).await?;
        let checksum = client.checksum(Vec::<u8>::new()..).await?;
        let expected_crc = pairs.iter().fold(0_u64, |checksum, (key, value)| {
            let mut pair = key.clone();
            pair.extend_from_slice(value);
            checksum ^ crc64_ecma(&pair)
        });
        let prefix_bytes = if matches!(keyspace, Keyspace::Enable { .. }) {
            4
        } else {
            0
        };
        assert_eq!(checksum.crc64_xor, expected_crc);
        assert_eq!(checksum.total_kvs, SOURCE_SCALE as u64);
        assert_eq!(
            checksum.total_bytes,
            pairs
                .iter()
                .map(|(key, value)| (key.len() + value.len() + prefix_bytes) as u64)
                .sum::<u64>()
        );
    }
    Ok(())
}

#[tokio::test]
async fn source_batch_duplicate_ttl_and_cf_matrix() -> Result<()> {
    let (client, _) = stateful_client();

    assert_eq!(client.get(b"missing".to_vec()).await?, None);
    client.put(b"empty".to_vec(), Vec::new()).await?;
    assert_eq!(client.get(b"empty".to_vec()).await?, Some(Vec::new()));
    client.delete(b"empty".to_vec()).await?;
    assert_eq!(client.get(b"empty".to_vec()).await?, None);

    client
        .batch_put_with_ttl(
            vec![
                (b"dup".to_vec(), b"first".to_vec()),
                (b"dup".to_vec(), b"last".to_vec()),
                (b"k4".to_vec(), Vec::new()),
                (b"z".to_vec(), b"last-region".to_vec()),
            ],
            [3, 7, 0, 0],
        )
        .await?;
    assert_eq!(client.get(b"dup".to_vec()).await?, Some(b"last".to_vec()));
    assert_eq!(client.get_key_ttl_secs(b"dup".to_vec()).await?, Some(7));
    assert_eq!(
        client
            .batch_get(vec![
                b"missing".to_vec(),
                b"k4".to_vec(),
                b"dup".to_vec(),
                b"dup".to_vec(),
                b"z".to_vec(),
            ])
            .await?,
        vec![
            None,
            Some(Vec::new()),
            Some(b"last".to_vec()),
            Some(b"last".to_vec()),
            Some(b"last-region".to_vec()),
        ]
    );
    client
        .batch_delete([b"dup".to_vec(), b"z".to_vec()])
        .await?;
    assert_eq!(client.get(b"dup".to_vec()).await?, None);

    let cf1 = client.with_cf_name("cf1");
    let cf2 = client.with_cf_name("cf2");
    cf1.put(b"same".to_vec(), b"one".to_vec()).await?;
    cf2.put(b"same".to_vec(), b"two".to_vec()).await?;
    assert_eq!(cf1.get(b"same".to_vec()).await?, Some(b"one".to_vec()));
    assert_eq!(cf2.get(b"same".to_vec()).await?, Some(b"two".to_vec()));
    assert_eq!(client.get(b"same".to_vec()).await?, None);

    assert!(matches!(
        client
            .compare_and_swap(b"cas".to_vec(), None, Vec::new())
            .await,
        Err(Error::UnsupportedMode)
    ));
    let mut atomic = client.clone();
    atomic.set_atomic_for_cas(true);
    assert_eq!(
        atomic
            .compare_and_swap(b"cas".to_vec(), None, Vec::new())
            .await?,
        (None, true)
    );
    assert_eq!(
        atomic
            .compare_and_swap(b"cas".to_vec(), Some(Vec::new()), b"value".to_vec())
            .await?,
        (Some(Vec::new()), true)
    );
    assert_eq!(
        atomic
            .compare_and_swap(b"cas".to_vec(), Some(b"wrong".to_vec()), b"other".to_vec(),)
            .await?,
        (Some(b"value".to_vec()), false)
    );
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_integration_raw_api_mock_TestSimple() -> Result<()> {
    let (client, _) = stateful_client();
    assert_eq!(client.get(b"key".to_vec()).await?, None);
    client.put(b"key".to_vec(), b"value".to_vec()).await?;
    assert_eq!(client.get(b"key".to_vec()).await?, Some(b"value".to_vec()));
    client.delete(b"key".to_vec()).await?;
    assert_eq!(client.get(b"key".to_vec()).await?, None);
    client.put(b"key".to_vec(), Vec::new()).await?;
    assert_eq!(client.get(b"key".to_vec()).await?, Some(Vec::new()));
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_integration_raw_api_TestSimple() -> Result<()> {
    let (client, _) = stateful_client();
    assert_eq!(client.get(b"key".to_vec()).await?, None);
    client.put(b"key".to_vec(), b"value".to_vec()).await?;
    assert_eq!(client.get(b"key".to_vec()).await?, Some(b"value".to_vec()));
    client.delete(b"key".to_vec()).await?;
    assert_eq!(client.get(b"key".to_vec()).await?, None);
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_integration_raw_api_TestBatchOp() -> Result<()> {
    let (client, _) = stateful_client();
    client
        .batch_put(owned_pairs(&[("k1", "v1"), ("k2", "v2")]))
        .await?;
    assert_eq!(
        client.batch_get([b"k1".to_vec(), b"k2".to_vec()]).await?,
        vec![Some(b"v1".to_vec()), Some(b"v2".to_vec())]
    );
    client
        .batch_delete([b"k1".to_vec(), b"k2".to_vec()])
        .await?;
    assert_eq!(client.get(b"k1".to_vec()).await?, None);
    assert_eq!(client.get(b"k2".to_vec()).await?, None);
    Ok(())
}

#[tokio::test]
#[allow(non_snake_case)]
async fn source_go_integration_raw_api_TestCAS() -> Result<()> {
    let (mut client, _) = stateful_client();
    client.set_atomic_for_cas(true);
    assert_eq!(
        client
            .compare_and_swap(b"key".to_vec(), None, b"hello world".to_vec())
            .await?,
        (None, true)
    );
    assert_eq!(
        client
            .compare_and_swap(b"key".to_vec(), Some(b"hello".to_vec()), b"world".to_vec(),)
            .await?,
        (Some(b"hello world".to_vec()), false)
    );
    assert_eq!(
        client
            .compare_and_swap(
                b"key".to_vec(),
                Some(b"hello world".to_vec()),
                b"world".to_vec(),
            )
            .await?,
        (Some(b"hello world".to_vec()), true)
    );
    assert_eq!(client.get(b"key".to_vec()).await?, Some(b"world".to_vec()));
    Ok(())
}

#[tokio::test]
async fn source_multi_region_scan_reverse_delete_and_checksum_matrix() -> Result<()> {
    let (client, state) = stateful_client();
    let entries = [("k1", "v1"), ("k3", "v3"), ("k5", "v5"), ("k7", "v7")];
    client
        .batch_put(entries.map(|(key, value)| (key.to_owned(), value.to_owned())))
        .await?;
    assert_eq!(client.get(b"k1".to_vec()).await?, Some(b"v1".to_vec()));

    assert_eq!(
        pairs_bytes(client.scan(Vec::<u8>::new().., 10).await?),
        entries
            .iter()
            .map(|(key, value)| (key.as_bytes().to_vec(), value.as_bytes().to_vec()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        pairs_bytes(client.scan(b"k2".to_vec()..b"k6".to_vec(), 2).await?),
        vec![
            (b"k3".to_vec(), b"v3".to_vec()),
            (b"k5".to_vec(), b"v5".to_vec()),
        ]
    );
    assert_eq!(
        pairs_bytes(
            client
                .scan_reverse(b"k2".to_vec()..b"z".to_vec(), 3)
                .await?
        ),
        vec![
            (b"k7".to_vec(), b"v7".to_vec()),
            (b"k5".to_vec(), b"v5".to_vec()),
            (b"k3".to_vec(), b"v3".to_vec()),
        ]
    );
    assert!(client.scan_reverse(b"k2".to_vec().., 10).await?.is_empty());
    assert_eq!(
        client.scan_keys(b"k1".to_vec()..b"k6".to_vec(), 10).await?,
        vec![
            b"k1".to_vec().into(),
            b"k3".to_vec().into(),
            b"k5".to_vec().into()
        ]
    );

    let checksum = client.checksum(Vec::<u8>::new()..).await?;
    let expected_crc = entries.iter().fold(0_u64, |checksum, (key, value)| {
        let mut pair = key.as_bytes().to_vec();
        pair.extend_from_slice(value.as_bytes());
        checksum ^ crc64_ecma(&pair)
    });
    assert_eq!(checksum.crc64_xor, expected_crc);
    assert_eq!(checksum.total_kvs, 4);
    assert_eq!(checksum.total_bytes, 16);

    let before_empty = state.dispatches();
    client
        .delete_range(b"same".to_vec()..b"same".to_vec())
        .await?;
    assert_eq!(state.dispatches(), before_empty);

    client.delete_range(b"k3".to_vec()..b"k7".to_vec()).await?;
    assert_eq!(
        pairs_bytes(client.scan(Vec::<u8>::new().., 10).await?),
        vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k7".to_vec(), b"v7".to_vec()),
        ]
    );
    client.delete_range(Vec::<u8>::new()..).await?;
    assert!(client.scan(Vec::<u8>::new().., 10).await?.is_empty());
    Ok(())
}

fn histogram_count(metric: &str, label: &str) -> u64 {
    crate::metrics::global_metrics()
        .histogram_vec(metric)
        .expect("RawKV histogram")
        .with_label_values(&[label])
        .get_sample_count()
}

#[tokio::test]
async fn source_ttl_scan_limit_and_high_level_metric_matrix() -> Result<()> {
    let (client, state) = stateful_client();
    state.set_now(10);

    let command_before = histogram_count("TiKVRawkvCmdHistogram", "batch_put");
    let key_before = histogram_count("TiKVRawkvSizeHistogram", "key");
    client
        .put_with_ttl(b"ttl".to_vec(), b"value".to_vec(), 2)
        .await?;
    assert!(histogram_count("TiKVRawkvCmdHistogram", "batch_put") > command_before);
    assert!(histogram_count("TiKVRawkvSizeHistogram", "key") > key_before);
    assert_eq!(client.get_key_ttl_secs(b"ttl".to_vec()).await?, Some(2));
    state.set_now(11);
    assert_eq!(client.get_key_ttl_secs(b"ttl".to_vec()).await?, Some(1));
    state.set_now(12);
    assert_eq!(client.get(b"ttl".to_vec()).await?, None);
    assert_eq!(client.get_key_ttl_secs(b"ttl".to_vec()).await?, None);

    let max_scan_limit = MAX_RAW_KV_SCAN_LIMIT.load(Ordering::Relaxed);
    assert_eq!(max_scan_limit, 10_240);
    assert!(matches!(
        client.scan(Vec::<u8>::new().., max_scan_limit + 1).await,
        Err(Error::MaxScanLimitExceeded {
            limit: 10_241,
            max_limit: 10_240,
        })
    ));
    assert!(matches!(
        client
            .scan_reverse(b"a".to_vec()..b"z".to_vec(), max_scan_limit + 1)
            .await,
        Err(Error::MaxScanLimitExceeded { .. })
    ));

    client.put(b"metric".to_vec(), b"value".to_vec()).await?;
    let checksum_before = histogram_count("TiKVRawkvSizeHistogram", "raw_checksum");
    client.checksum(Vec::<u8>::new()..).await?;
    assert!(histogram_count("TiKVRawkvSizeHistogram", "raw_checksum") > checksum_before);
    Ok(())
}

#[tokio::test]
async fn source_command_metric_label_and_delete_range_error_matrix() -> Result<()> {
    let (client, _) = stateful_client();
    let labels = [
        "get",
        "batch_get",
        "batch_put",
        "delete",
        "batch_delete",
        "raw_scan",
        "raw_reverse_scan",
        "delete_range",
    ];
    let before = labels.map(|label| histogram_count("TiKVRawkvCmdHistogram", label));

    client.get(b"metric".to_vec()).await?;
    client.batch_get([b"metric".to_vec()]).await?;
    client.put(b"metric".to_vec(), b"value".to_vec()).await?;
    client.batch_delete([b"missing-metric".to_vec()]).await?;
    client.delete(b"metric".to_vec()).await?;
    client.scan(Vec::<u8>::new().., 1).await?;
    client
        .scan_reverse(Vec::<u8>::new()..b"z".to_vec(), 1)
        .await?;
    client.delete_range(b"a".to_vec()..b"b".to_vec()).await?;

    for (label, before) in labels.into_iter().zip(before) {
        assert!(histogram_count("TiKVRawkvCmdHistogram", label) > before);
    }

    let error_before = histogram_count("TiKVRawkvCmdHistogram", "delete_range_error");
    let failing_pd = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
        |request| {
            assert!(request.is::<kvrpcpb::RawDeleteRangeRequest>());
            Ok(Box::new(kvrpcpb::RawDeleteRangeResponse {
                error: "delete range failed".to_owned(),
                ..Default::default()
            }) as Box<dyn Any>)
        },
    )));
    let failing = Client::from_test_rpc(failing_pd, Keyspace::Disable, None);
    assert!(failing
        .delete_range(b"a".to_vec()..b"b".to_vec())
        .await
        .is_err());
    assert!(histogram_count("TiKVRawkvCmdHistogram", "delete_range_error") > error_before);
    Ok(())
}

#[test]
fn crc64_test_backend_matches_the_ecma_check_value() {
    assert_eq!(crc64_ecma(b"123456789"), 0x6c40_df5f_0b49_7347);
}
