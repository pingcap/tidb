// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::mock::{MockKvClient, MockPdClient};
use crate::proto::{kvrpcpb, metapb};
use crate::region::RegionWithLeader;
use crate::request::Keyspace;
use crate::{Error, KvPair, Result};

use super::{Client, MAX_RAW_KV_SCAN_LIMIT};

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
    regions: Vec<RegionBounds>,
}

impl StatefulRawStore {
    fn new(regions: Vec<RegionBounds>) -> Self {
        Self {
            now: AtomicU64::new(0),
            dispatches: AtomicUsize::new(0),
            data: Mutex::new(BTreeMap::new()),
            regions,
        }
    }

    fn set_now(&self, now: u64) {
        self.now.store(now, Ordering::SeqCst);
    }

    fn dispatches(&self) -> usize {
        self.dispatches.load(Ordering::SeqCst)
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
            .iter()
            .find(|region| region.id == region_id)
            .is_none_or(|region| {
                region.start.as_slice() <= key
                    && (region.end.is_empty() || key < region.end.as_slice())
            })
    }

    fn scan_region_id(&self, request: &kvrpcpb::RawScanRequest) -> u64 {
        self.regions
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
                let mut pair = key.clone();
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

fn stateful_client() -> (Client<MockPdClient>, Arc<StatefulRawStore>) {
    let regions = vec![
        RegionBounds {
            id: 1,
            start: Vec::new(),
            end: b"k3".to_vec(),
        },
        RegionBounds {
            id: 2,
            start: b"k3".to_vec(),
            end: b"k6".to_vec(),
        },
        RegionBounds {
            id: 3,
            start: b"k6".to_vec(),
            end: Vec::new(),
        },
    ];
    let state = Arc::new(StatefulRawStore::new(regions.clone()));
    let dispatch_state = state.clone();
    let kv_client =
        MockKvClient::with_dispatch_hook(move |request| dispatch_state.dispatch(request));
    let pd_client = MockPdClient::with_client_and_regions(
        kv_client,
        regions
            .iter()
            .enumerate()
            .map(|(index, bounds)| region(bounds, index as u64 + 41))
            .collect(),
    );
    (
        Client::from_test_rpc(Arc::new(pd_client), Keyspace::Disable, None),
        state,
    )
}

fn pairs_bytes(pairs: Vec<KvPair>) -> Vec<(Vec<u8>, Vec<u8>)> {
    pairs
        .into_iter()
        .map(|pair| (pair.0.into(), pair.1))
        .collect()
}

#[tokio::test]
async fn source_simple_batch_column_family_cas_and_empty_value_matrix() -> Result<()> {
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
