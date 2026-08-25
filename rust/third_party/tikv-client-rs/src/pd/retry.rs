// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! A utility module for managing and retrying PD requests.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::pd::Cluster;
use crate::pd::Connection;
use crate::proto::keyspacepb;
use crate::proto::metapb;
use crate::proto::pdpb::Timestamp;
use crate::proto::pdpb::{self};
use crate::region::RegionId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::stats::pd_stats;
use crate::Error;
use crate::Result;
use crate::SecurityManager;

// FIXME: these numbers and how they are used are all just cargo-culted in, there
// may be more optimal values.
const RECONNECT_INTERVAL_SEC: u64 = 1;
const MAX_REQUEST_COUNT: usize = 5;
const LEADER_CHANGE_RETRY: usize = 10;

/// Options carried by PD's `BatchScanRegions` request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RegionScanOptions {
    pub need_buckets: bool,
    pub contain_all_key_range: bool,
}

#[async_trait]
pub trait RetryClientTrait {
    // These get_* functions will try multiple times to make a request, reconnecting as necessary.
    // It does not know about encoding. Caller should take care of it.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader>;

    /// Requests PD bucket metadata for a key lookup. Custom clients that do
    /// not model PD options retain their normal region result.
    async fn get_region_with_buckets(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        self.get_region(key).await
    }

    async fn get_prev_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader>;

    async fn get_prev_region_with_buckets(
        self: Arc<Self>,
        key: Vec<u8>,
    ) -> Result<RegionWithLeader> {
        self.get_prev_region(key).await
    }

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader>;

    async fn get_region_by_id_with_buckets(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<RegionWithLeader> {
        self.get_region_by_id(region_id).await
    }

    /// Source `PDClient.ScanRegions`, used to refresh a contiguous region
    /// range in one request. The default keeps custom/mock clients compatible
    /// by deriving the same bounded sequence from point lookups.
    async fn scan_regions(
        self: Arc<Self>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<RegionWithLeader>> {
        let mut next: crate::Key = start_key.into();
        let end: crate::Key = end_key.into();
        let mut regions = Vec::with_capacity(limit);
        while regions.len() < limit {
            let region = self.clone().get_region(next.clone().into()).await?;
            let region_end = region.end_key();
            if !region_end.is_empty() && region_end <= next {
                return Err(Error::StringError(
                    "PD returned a region that does not advance ScanRegions".to_owned(),
                ));
            }
            regions.push(region);
            if region_end.is_empty() || (!end.is_empty() && region_end >= end) {
                break;
            }
            next = region_end;
        }
        Ok(regions)
    }

    /// PD's multi-range region scan. Custom clients can explicitly retain an
    /// unsupported result so callers can fall back to `ScanRegions`, as
    /// client-go's region cache does for older PD servers.
    async fn batch_scan_regions(
        self: Arc<Self>,
        _ranges: Vec<pdpb::KeyRange>,
        _limit: usize,
        _options: RegionScanOptions,
    ) -> Result<Vec<RegionWithLeader>> {
        Err(Error::Unimplemented)
    }

    /// Requests PD to split at the supplied physical keys.
    async fn split_regions(
        self: Arc<Self>,
        _split_keys: Vec<Vec<u8>>,
        _retry_limit: u64,
    ) -> Result<pdpb::SplitRegionsResponse> {
        Err(Error::Unimplemented)
    }

    /// Returns `None` when PD has no store for the requested ID. Client-go
    /// treats that outcome like a tombstone; retaining it explicitly avoids
    /// panicking on an empty `GetStoreResponse`.
    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<Option<metapb::Store>>;

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>>;

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp>;

    async fn get_min_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        Err(Error::StringError(
            "PD minimum timestamp is not supported by this client".to_owned(),
        ))
    }

    async fn set_external_timestamp(self: Arc<Self>, _timestamp: u64) -> Result<()> {
        Err(Error::StringError(
            "PD external timestamp is not supported by this client".to_owned(),
        ))
    }

    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        Err(Error::StringError(
            "PD external timestamp is not supported by this client".to_owned(),
        ))
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool>;

    async fn update_safepoint_value(self: Arc<Self>, _safepoint: u64) -> Result<u64> {
        Err(Error::Unimplemented)
    }

    /// Loads PD's modern transaction/GC safe-point state for one keyspace.
    async fn get_gc_state(self: Arc<Self>, _keyspace_id: u32) -> Result<pdpb::GetGcStateResponse> {
        Err(Error::Unimplemented)
    }

    async fn advance_txn_safe_point(
        self: Arc<Self>,
        _keyspace_id: u32,
        _target: u64,
    ) -> Result<pdpb::AdvanceTxnSafePointResponse> {
        Err(Error::Unimplemented)
    }

    async fn advance_gc_safe_point(
        self: Arc<Self>,
        _keyspace_id: u32,
        _target: u64,
    ) -> Result<pdpb::AdvanceGcSafePointResponse> {
        Err(Error::Unimplemented)
    }

    async fn scatter_regions(
        self: Arc<Self>,
        _region_ids: Vec<u64>,
        _group: String,
    ) -> Result<pdpb::ScatterRegionResponse> {
        Err(Error::Unimplemented)
    }

    async fn get_operator(self: Arc<Self>, _region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        Err(Error::Unimplemented)
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta>;
}
/// Client for communication with a PD cluster. Has the facility to reconnect to the cluster.
pub struct RetryClient<Cl = Cluster> {
    // Tuple is the cluster and the time of the cluster's last reconnect.
    cluster: RwLock<(Cl, Instant)>,
    connection: Connection,
    timeout: Duration,
}

#[cfg(any(test, feature = "internal-tests"))]
impl<Cl> RetryClient<Cl> {
    pub fn new_with_cluster(
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
        cluster: Cl,
    ) -> RetryClient<Cl> {
        let connection = Connection::new(security_mgr);
        RetryClient {
            cluster: RwLock::new((cluster, Instant::now())),
            connection,
            timeout,
        }
    }
}

macro_rules! retry_core {
    ($self: ident, $tag: literal, $call: expr) => {{
        let stats = pd_stats($tag);
        let mut last_err = Ok(());
        for _ in 0..LEADER_CHANGE_RETRY {
            let res = $call;

            match stats.done(res) {
                Ok(r) => return Ok(r),
                Err(Error::Unimplemented) => return Err(Error::Unimplemented),
                Err(e) => last_err = Err(e),
            }

            let mut reconnect_count = MAX_REQUEST_COUNT;
            while let Err(e) = $self.reconnect(RECONNECT_INTERVAL_SEC).await {
                reconnect_count -= 1;
                if reconnect_count == 0 {
                    return Err(e);
                }
                sleep(Duration::from_secs(RECONNECT_INTERVAL_SEC)).await;
            }
        }

        last_err?;
        unreachable!();
    }};
}

macro_rules! retry_mut {
    ($self: ident, $tag: literal, |$cluster: ident| $call: expr) => {{
        retry_core!($self, $tag, {
            // use the block here to drop the guard of the lock,
            // otherwise `reconnect` will try to acquire the write lock and results in a deadlock
            let $cluster = &mut $self.cluster.write().await.0;
            $call.await
        })
    }};
}

macro_rules! retry {
    ($self: ident, $tag: literal, |$cluster: ident| $call: expr) => {{
        retry_core!($self, $tag, {
            // use the block here to drop the guard of the lock,
            // otherwise `reconnect` will try to acquire the write lock and results in a deadlock
            let $cluster = &$self.cluster.read().await.0;
            $call.await
        })
    }};
}

impl RetryClient<Cluster> {
    pub async fn connect(
        endpoints: &[String],
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
    ) -> Result<RetryClient> {
        let connection = Connection::new(security_mgr);
        let cluster = RwLock::new((
            connection.connect_cluster(endpoints, timeout).await?,
            Instant::now(),
        ));
        Ok(RetryClient {
            cluster,
            connection,
            timeout,
        })
    }

    pub async fn cluster_id(&self) -> u64 {
        self.cluster.read().await.0.id()
    }
}

#[async_trait]
impl RetryClientTrait for RetryClient<Cluster> {
    // These get_* functions will try multiple times to make a request, reconnecting as necessary.
    // It does not know about encoding. Caller should take care of it.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        retry_mut!(self, "get_region", |cluster| {
            let key = key.clone();
            async {
                cluster
                    .get_region(key.clone(), self.timeout)
                    .await
                    .and_then(|resp| {
                        region_from_response(resp, || Error::RegionForKeyNotFound { key })
                    })
            }
        })
    }

    async fn get_region_with_buckets(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        retry_mut!(self, "get_region_with_buckets", |cluster| {
            let key = key.clone();
            async {
                cluster
                    .get_region_with_buckets(key.clone(), self.timeout, true)
                    .await
                    .and_then(|resp| {
                        region_from_response(resp, || Error::RegionForKeyNotFound { key })
                    })
            }
        })
    }

    async fn get_prev_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        retry_mut!(self, "get_prev_region", |cluster| {
            let key = key.clone();
            async {
                cluster
                    .get_prev_region(key.clone(), self.timeout)
                    .await
                    .and_then(|resp| {
                        region_from_response(resp, || Error::RegionForKeyNotFound { key })
                    })
            }
        })
    }

    async fn get_prev_region_with_buckets(
        self: Arc<Self>,
        key: Vec<u8>,
    ) -> Result<RegionWithLeader> {
        retry_mut!(self, "get_prev_region_with_buckets", |cluster| {
            let key = key.clone();
            async {
                cluster
                    .get_prev_region_with_buckets(key.clone(), self.timeout, true)
                    .await
                    .and_then(|resp| {
                        region_from_response(resp, || Error::RegionForKeyNotFound { key })
                    })
            }
        })
    }

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader> {
        retry_mut!(self, "get_region_by_id", |cluster| async {
            cluster
                .get_region_by_id(region_id, self.timeout)
                .await
                .and_then(|resp| {
                    region_from_response(resp, || Error::RegionNotFoundInResponse { region_id })
                })
        })
    }

    async fn get_region_by_id_with_buckets(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<RegionWithLeader> {
        retry_mut!(self, "get_region_by_id_with_buckets", |cluster| async {
            cluster
                .get_region_by_id_with_buckets(region_id, self.timeout, true)
                .await
                .and_then(|resp| {
                    region_from_response(resp, || Error::RegionNotFoundInResponse { region_id })
                })
        })
    }

    async fn scan_regions(
        self: Arc<Self>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<RegionWithLeader>> {
        retry_mut!(self, "scan_regions", |cluster| {
            let start_key = start_key.clone();
            let end_key = end_key.clone();
            async {
                cluster
                    .scan_regions(start_key, end_key, limit, self.timeout)
                    .await
                    .and_then(regions_from_scan_response)
            }
        })
    }

    async fn batch_scan_regions(
        self: Arc<Self>,
        ranges: Vec<pdpb::KeyRange>,
        limit: usize,
        options: RegionScanOptions,
    ) -> Result<Vec<RegionWithLeader>> {
        retry_mut!(self, "batch_scan_regions", |cluster| {
            let ranges = ranges.clone();
            async {
                cluster
                    .batch_scan_regions(ranges, limit, options, self.timeout)
                    .await
                    .and_then(regions_from_batch_scan_response)
            }
        })
    }

    async fn split_regions(
        self: Arc<Self>,
        split_keys: Vec<Vec<u8>>,
        retry_limit: u64,
    ) -> Result<pdpb::SplitRegionsResponse> {
        retry_mut!(self, "split_regions", |cluster| {
            let split_keys = split_keys.clone();
            cluster.split_regions(split_keys, retry_limit, self.timeout)
        })
    }

    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<Option<metapb::Store>> {
        retry_mut!(self, "get_store", |cluster| async {
            cluster
                .get_store(id, self.timeout)
                .await
                .map(|resp| resp.store)
        })
    }

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
        retry_mut!(self, "get_all_stores", |cluster| async {
            cluster
                .get_all_stores(self.timeout)
                .await
                .map(|resp| resp.stores)
        })
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        retry!(self, "get_timestamp", |cluster| cluster.get_timestamp())
    }

    async fn get_min_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        retry_mut!(self, "get_min_timestamp", |cluster| {
            cluster.get_min_timestamp(self.timeout)
        })
    }

    async fn set_external_timestamp(self: Arc<Self>, timestamp: u64) -> Result<()> {
        retry_mut!(self, "set_external_timestamp", |cluster| {
            cluster.set_external_timestamp(timestamp, self.timeout)
        })
    }

    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        retry_mut!(self, "get_external_timestamp", |cluster| {
            cluster.get_external_timestamp(self.timeout)
        })
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool> {
        Ok(self.clone().update_safepoint_value(safepoint).await? == safepoint)
    }

    async fn update_safepoint_value(self: Arc<Self>, safepoint: u64) -> Result<u64> {
        retry_mut!(self, "update_gc_safepoint", |cluster| async {
            cluster
                .update_safepoint(safepoint, self.timeout)
                .await
                .map(|resp| resp.new_safe_point)
        })
    }

    async fn get_gc_state(self: Arc<Self>, keyspace_id: u32) -> Result<pdpb::GetGcStateResponse> {
        retry_mut!(self, "get_gc_state", |cluster| {
            cluster.get_gc_state(keyspace_id, self.timeout)
        })
    }

    async fn advance_txn_safe_point(
        self: Arc<Self>,
        keyspace_id: u32,
        target: u64,
    ) -> Result<pdpb::AdvanceTxnSafePointResponse> {
        retry_mut!(self, "advance_txn_safe_point", |cluster| {
            cluster.advance_txn_safe_point(keyspace_id, target, self.timeout)
        })
    }

    async fn advance_gc_safe_point(
        self: Arc<Self>,
        keyspace_id: u32,
        target: u64,
    ) -> Result<pdpb::AdvanceGcSafePointResponse> {
        retry_mut!(self, "advance_gc_safe_point", |cluster| {
            cluster.advance_gc_safe_point(keyspace_id, target, self.timeout)
        })
    }

    async fn scatter_regions(
        self: Arc<Self>,
        region_ids: Vec<u64>,
        group: String,
    ) -> Result<pdpb::ScatterRegionResponse> {
        retry_mut!(self, "scatter_regions", |cluster| {
            cluster.scatter_regions(region_ids.clone(), group.clone(), self.timeout)
        })
    }

    async fn get_operator(self: Arc<Self>, region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        retry_mut!(self, "get_operator", |cluster| {
            cluster.get_operator(region_id, self.timeout)
        })
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        retry_mut!(self, "load_keyspace", |cluster| async {
            cluster.load_keyspace(keyspace, self.timeout).await
        })
    }
}

impl fmt::Debug for RetryClient {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("pd::RetryClient")
            .field("timeout", &self.timeout)
            .finish()
    }
}

fn region_from_response(
    mut resp: pdpb::GetRegionResponse,
    err: impl FnOnce() -> Error,
) -> Result<RegionWithLeader> {
    let region = resp.region.take().ok_or_else(err)?;
    let mut region = RegionWithLeader::new(region, resp.leader.take());
    region.buckets = resp.buckets.take();
    region.pending_peers = std::mem::take(&mut resp.pending_peers);
    region.down_peers = std::mem::take(&mut resp.down_peers);
    Ok(region)
}

fn regions_from_scan_response(resp: pdpb::ScanRegionsResponse) -> Result<Vec<RegionWithLeader>> {
    if !resp.regions.is_empty() {
        return resp
            .regions
            .into_iter()
            .map(|mut entry| {
                let region = entry.region.take().ok_or_else(|| {
                    Error::StringError("PD ScanRegions response has no region metadata".to_owned())
                })?;
                Ok(RegionWithLeader {
                    region,
                    leader: entry.leader.take(),
                    buckets: entry.buckets.take(),
                    pending_peers: std::mem::take(&mut entry.pending_peers),
                    down_peers: std::mem::take(&mut entry.down_peers),
                })
            })
            .collect();
    }

    Ok(resp
        .region_metas
        .into_iter()
        .enumerate()
        .map(|(index, region)| RegionWithLeader::new(region, resp.leaders.get(index).cloned()))
        .collect())
}

fn regions_from_batch_scan_response(
    resp: pdpb::BatchScanRegionsResponse,
) -> Result<Vec<RegionWithLeader>> {
    resp.regions
        .into_iter()
        .map(|mut entry| {
            let region = entry.region.take().ok_or_else(|| {
                Error::StringError("PD BatchScanRegions response has no region metadata".to_owned())
            })?;
            Ok(RegionWithLeader {
                region,
                leader: entry.leader.take(),
                buckets: entry.buckets.take(),
                pending_peers: std::mem::take(&mut entry.pending_peers),
                down_peers: std::mem::take(&mut entry.down_peers),
            })
        })
        .collect()
}

// A node-like thing that can be connected to.
#[async_trait]
trait Reconnect {
    type Cl;
    async fn reconnect(&self, interval_sec: u64) -> Result<()>;
}

#[async_trait]
impl Reconnect for RetryClient<Cluster> {
    type Cl = Cluster;

    async fn reconnect(&self, interval_sec: u64) -> Result<()> {
        let reconnect_begin = Instant::now();
        let mut lock = self.cluster.write().await;
        let (cluster, last_connected) = &mut *lock;
        // If `last_connected + interval_sec` is larger or equal than reconnect_begin,
        // a concurrent reconnect is just succeed when this thread trying to get write lock
        let should_connect = reconnect_begin > *last_connected + Duration::from_secs(interval_sec);
        if should_connect {
            self.connection.reconnect(cluster, self.timeout).await?;
            *last_connected = Instant::now();
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;

    use futures::executor;
    use futures::future::ready;

    use super::*;
    use crate::internal_err;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_reconnect() {
        struct MockClient {
            reconnect_count: AtomicUsize,
            cluster: RwLock<((), Instant)>,
        }

        #[async_trait]
        impl Reconnect for MockClient {
            type Cl = ();

            async fn reconnect(&self, _: u64) -> Result<()> {
                self.reconnect_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Not actually unimplemented, we just don't care about the error.
                Err(Error::Unimplemented)
            }
        }

        async fn retry_err(client: Arc<MockClient>) -> Result<()> {
            retry_mut!(client, "test", |_c| ready(Err(internal_err!("whoops"))))
        }

        async fn retry_ok(client: Arc<MockClient>) -> Result<()> {
            retry!(client, "test", |_c| ready(Ok::<_, Error>(())))
        }

        executor::block_on(async {
            let client = Arc::new(MockClient {
                reconnect_count: AtomicUsize::new(0),
                cluster: RwLock::new(((), Instant::now())),
            });

            assert!(retry_err(client.clone()).await.is_err());
            assert_eq!(
                client
                    .reconnect_count
                    .load(std::sync::atomic::Ordering::SeqCst),
                MAX_REQUEST_COUNT
            );

            client
                .reconnect_count
                .store(0, std::sync::atomic::Ordering::SeqCst);
            assert!(retry_ok(client.clone()).await.is_ok());
            assert_eq!(
                client
                    .reconnect_count
                    .load(std::sync::atomic::Ordering::SeqCst),
                0
            );
        })
    }

    #[test]
    fn test_retry() {
        struct MockClient {
            cluster: RwLock<(AtomicUsize, Instant)>,
        }

        #[async_trait]
        impl Reconnect for MockClient {
            type Cl = Mutex<usize>;

            async fn reconnect(&self, _: u64) -> Result<()> {
                Ok(())
            }
        }

        async fn retry_max_err(
            client: Arc<MockClient>,
            max_retries: Arc<AtomicUsize>,
        ) -> Result<()> {
            retry_mut!(client, "test", |c| {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                let max_retries = max_retries.fetch_sub(1, Ordering::SeqCst) - 1;
                if max_retries == 0 {
                    ready(Ok(()))
                } else {
                    ready(Err(internal_err!("whoops")))
                }
            })
        }

        async fn retry_max_ok(
            client: Arc<MockClient>,
            max_retries: Arc<AtomicUsize>,
        ) -> Result<()> {
            retry!(client, "test", |c| {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                let max_retries = max_retries.fetch_sub(1, Ordering::SeqCst) - 1;
                if max_retries == 0 {
                    ready(Ok(()))
                } else {
                    ready(Err(internal_err!("whoops")))
                }
            })
        }

        executor::block_on(async {
            let client = Arc::new(MockClient {
                cluster: RwLock::new((AtomicUsize::new(0), Instant::now())),
            });
            let max_retries = Arc::new(AtomicUsize::new(1000));

            assert!(retry_max_err(client.clone(), max_retries).await.is_err());
            assert_eq!(
                client.cluster.read().await.0.load(Ordering::SeqCst),
                LEADER_CHANGE_RETRY
            );

            let client = Arc::new(MockClient {
                cluster: RwLock::new((AtomicUsize::new(0), Instant::now())),
            });
            let max_retries = Arc::new(AtomicUsize::new(2));

            assert!(retry_max_ok(client.clone(), max_retries).await.is_ok());
            assert_eq!(client.cluster.read().await.0.load(Ordering::SeqCst), 2);
        })
    }

    #[test]
    fn source_scan_regions_decodes_extended_and_legacy_shapes() {
        let extended = pdpb::ScanRegionsResponse {
            regions: vec![pdpb::Region {
                region: Some(metapb::Region {
                    id: 1,
                    start_key: b"a".to_vec(),
                    end_key: b"z".to_vec(),
                    ..Default::default()
                }),
                leader: Some(metapb::Peer {
                    id: 7,
                    store_id: 8,
                    ..Default::default()
                }),
                pending_peers: vec![metapb::Peer {
                    id: 9,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let regions = regions_from_scan_response(extended).unwrap();
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].id(), 1);
        assert_eq!(regions[0].leader.as_ref().unwrap().store_id, 8);
        assert_eq!(regions[0].pending_peers[0].id, 9);

        let legacy = pdpb::ScanRegionsResponse {
            region_metas: vec![metapb::Region {
                id: 2,
                start_key: b"z".to_vec(),
                ..Default::default()
            }],
            leaders: vec![metapb::Peer {
                id: 10,
                store_id: 11,
                ..Default::default()
            }],
            ..Default::default()
        };
        let regions = regions_from_scan_response(legacy).unwrap();
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].id(), 2);
        assert_eq!(regions[0].leader.as_ref().unwrap().store_id, 11);
    }

    #[test]
    fn source_scan_regions_rejects_an_extended_entry_without_metadata() {
        let error = regions_from_scan_response(pdpb::ScanRegionsResponse {
            regions: vec![pdpb::Region::default()],
            ..Default::default()
        })
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "PD ScanRegions response has no region metadata"
        );
    }
}
