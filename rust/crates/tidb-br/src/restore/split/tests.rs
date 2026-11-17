// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `br/pkg/restore/split/{split,client}_test.go`, over a port of Go's
//! `MockPDClientForSplit`.
//!
//! Go's tests that run with `isRawKv: true` "to make tests more readable"
//! run here with memcomparable-encoded region boundaries: raw KV split has
//! no SQL caller and is not ported.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_proto::{kvrpcpb, metapb, pdpb};

use super::client::{is_scatter_region_finished, MockSplit, MAX_BATCH_SPLIT_SIZE};
use super::*;
use crate::errors::{self, Result};
use crate::pdclient::{PdClient, PdRegion};
use crate::utils::backoff::BackoffStrategy;
use crate::utils::retry::with_retry;

/// Go tests run one at a time; these share the package's tunables.
pub(crate) static SERIAL: Mutex<()> = Mutex::new(());

pub(crate) fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Restores a tunable when the test ends (Go `t.Cleanup`).
struct Restore<'a>(&'a std::sync::atomic::AtomicI32, i32);

impl Drop for Restore<'_> {
    fn drop(&mut self) {
        self.0.store(self.1, Ordering::Relaxed);
    }
}

fn set_tunable(t: &'static std::sync::atomic::AtomicI32, v: i32) -> Restore<'static> {
    Restore(t, t.swap(v, Ordering::Relaxed))
}

fn enc(k: &[u8]) -> Vec<u8> {
    let mut b = Vec::new();
    tidb_codec::encode_bytes(&mut b, k);
    b
}

/// Go `encodeBytes`: every non-empty key encoded.
fn encode_keys(keys: &[&[u8]]) -> Vec<Vec<u8>> {
    keys.iter()
        .map(|k| if k.is_empty() { Vec::new() } else { enc(k) })
        .collect()
}

/// The mock's regions with their leaders.
type Regions = Vec<(metapb::Region, metapb::Peer)>;
/// Go `scanRegions.beforeHook`.
type ScanHook = Box<dyn FnMut(&mut Regions) + Send>;

#[derive(Default)]
struct MockState {
    stores: HashMap<u64, metapb::Store>,
    regions: Regions,
    last_region_id: u64,
    scan_errors: Vec<Option<errors::Error>>,
    scan_before_hook: Option<ScanHook>,
    split_count: usize,
    scatter_region_fail_before: usize,
    scatter_region_count: HashMap<u64, usize>,
    scatter_regions_not_implemented: bool,
    scatter_regions_region_count: usize,
    scatter_regions_failed_count: usize,
    scatter_regions_finished_percentage: usize,
    get_operator_responses: Option<HashMap<u64, Vec<pdpb::GetOperatorResponse>>>,
    /// Go `nilRegionByIDMockPDClient.regionID`.
    nil_region_id: Option<u64>,
}

/// Go `MockPDClientForSplit`.
#[derive(Clone)]
struct MockPd(Arc<Mutex<MockState>>);

fn overlap(a: &metapb::Region, start: &[u8], end: &[u8]) -> bool {
    !(!end.is_empty() && end <= a.start_key.as_slice())
        && !(!a.end_key.is_empty() && a.end_key.as_slice() <= start)
}

impl MockPd {
    /// Go `NewMockPDClientForSplit`.
    fn new() -> Self {
        Self(Arc::new(Mutex::new(MockState {
            scatter_regions_finished_percentage: 100,
            ..MockState::default()
        })))
    }

    fn state(&self) -> MutexGuard<'_, MockState> {
        self.0.lock().unwrap()
    }

    /// Go `SetRegions`.
    fn set_regions(&self, boundaries: &[Vec<u8>]) -> Vec<metapb::Region> {
        set_regions(&mut self.state(), boundaries)
    }

    /// Go `MockPDClientForSplit.SplitRegion`.
    fn split_region(&self, region: &RegionInfo, keys: &[Vec<u8>]) -> kvrpcpb::SplitRegionResponse {
        let mut st = self.state();
        st.split_count += 1;
        let mut boundaries = vec![region.region.start_key.clone()];
        boundaries.extend(keys.iter().map(|k| enc(k)));
        boundaries.push(region.region.end_key.clone());
        let mut new_regions = set_regions(&mut st, &boundaries);
        let first_id = new_regions[0].id;
        new_regions[0].id = region.region.id;
        for (r, _) in &mut st.regions {
            if r.id == first_id {
                r.id = region.region.id;
            }
        }
        kvrpcpb::SplitRegionResponse {
            regions: new_regions,
            ..Default::default()
        }
    }
}

/// Go `RegionTree.SetRegion` over Go `setRegions`.
fn set_regions(st: &mut MockState, boundaries: &[Vec<u8>]) -> Vec<metapb::Region> {
    let mut ret = Vec::new();
    for w in boundaries.windows(2) {
        st.last_region_id += 1;
        let r = metapb::Region {
            id: st.last_region_id,
            start_key: w[0].clone(),
            end_key: w[1].clone(),
            ..Default::default()
        };
        let p = metapb::Peer {
            id: st.last_region_id,
            store_id: 1,
            ..Default::default()
        };
        st.regions
            .retain(|(old, _)| !overlap(old, &r.start_key, &r.end_key));
        st.regions.push((r.clone(), p));
        ret.push(r);
    }
    ret
}

/// Go `RegionTree.ScanRange`.
fn scan_range(
    regions: &mut [(metapb::Region, metapb::Peer)],
    start: &[u8],
    end: &[u8],
    limit: usize,
) -> Vec<PdRegion> {
    regions.sort_by(|a, b| a.0.start_key.cmp(&b.0.start_key));
    regions
        .iter()
        .filter(|(r, _)| overlap(r, start, end))
        .take(if limit == 0 { usize::MAX } else { limit })
        .map(|(r, p)| PdRegion {
            meta: r.clone(),
            leader: Some(*p),
            ..Default::default()
        })
        .collect()
}

impl PdClient for MockPd {
    fn get_cluster_id(&self, _: &Context) -> u64 {
        1
    }
    fn get_all_stores(&self, _: &Context, _: bool) -> Result<Vec<metapb::Store>> {
        Ok(self.state().stores.values().cloned().collect())
    }
    fn get_store(&self, _: &Context, id: u64) -> Result<metapb::Store> {
        Ok(self.state().stores.get(&id).cloned().unwrap_or_default())
    }
    fn get_ts(&self, _: &Context) -> Result<(i64, i64)> {
        Ok((1, 2))
    }
    fn update_gc_safe_point(&self, _: &Context, _: u64) -> Result<u64> {
        Ok(0)
    }
    fn update_service_gc_safe_point(&self, _: &Context, _: &str, _: i64, _: u64) -> Result<u64> {
        Ok(0)
    }
    fn get_keyspace_gc_safe_point(&self, _: &Context, _: u32) -> Result<u64> {
        Ok(0)
    }
    fn set_gc_barrier(
        &self,
        _: &Context,
        _: u32,
        id: &str,
        ts: u64,
        ttl: Duration,
    ) -> Result<(String, u64, Duration)> {
        Ok((id.to_owned(), ts, ttl))
    }
    fn delete_gc_barrier(&self, _: &Context, _: u32, _: &str) -> Result<()> {
        Ok(())
    }
    fn get_region_by_id(&self, _: &Context, id: u64) -> Result<Option<PdRegion>> {
        let st = self.state();
        if st.nil_region_id == Some(id) {
            return Ok(None);
        }
        match st.regions.iter().find(|(r, _)| r.id == id) {
            Some((r, p)) => Ok(Some(PdRegion {
                meta: r.clone(),
                leader: Some(*p),
                ..Default::default()
            })),
            None => Err(errors::errorf("region not found")),
        }
    }
    fn scan_regions(
        &self,
        _: &Context,
        key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Result<Vec<PdRegion>> {
        let mut st = self.state();
        if !st.scan_errors.is_empty() {
            if let Some(err) = st.scan_errors.remove(0) {
                return Err(err);
            }
            return Ok(Vec::new());
        }
        if let Some(mut hook) = st.scan_before_hook.take() {
            hook(&mut st.regions);
            st.scan_before_hook = Some(hook);
        }
        Ok(scan_range(&mut st.regions, key, end_key, limit))
    }
    fn scatter_region(&self, _: &Context, id: u64) -> Result<()> {
        let mut st = self.state();
        let n = st.scatter_region_count.entry(id).or_default();
        *n += 1;
        if *n > st.scatter_region_fail_before {
            return Ok(());
        }
        Err(errors::grpc(tonic::Status::unknown(format!(
            "region {id} is not fully replicated"
        ))))
    }
    fn scatter_regions(&self, _: &Context, ids: &[u64]) -> Result<pdpb::ScatterRegionResponse> {
        let mut st = self.state();
        if st.scatter_regions_not_implemented {
            return Err(errors::grpc(tonic::Status::unimplemented("Ah, yep")));
        }
        if st.scatter_regions_failed_count > 0 {
            st.scatter_regions_failed_count -= 1;
            return Ok(pdpb::ScatterRegionResponse {
                finished_percentage: 0,
                failed_regions_id: ids.to_vec(),
                ..Default::default()
            });
        }
        st.scatter_regions_region_count += ids.len() * st.scatter_regions_finished_percentage / 100;
        Ok(pdpb::ScatterRegionResponse {
            finished_percentage: st.scatter_regions_finished_percentage as u64,
            ..Default::default()
        })
    }
    fn get_operator(&self, _: &Context, id: u64) -> Result<pdpb::GetOperatorResponse> {
        let mut st = self.state();
        match &mut st.get_operator_responses {
            None => Ok(pdpb::GetOperatorResponse {
                desc: b"scatter-region".to_vec(),
                status: pdpb::OperatorStatus::Success as i32,
                ..Default::default()
            }),
            Some(m) => Ok(m.get_mut(&id).unwrap().remove(0)),
        }
    }
}

fn runtime() -> Arc<tokio::runtime::Runtime> {
    Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
    )
}

/// Go `&pdClient{client: mock, ...}` with the mock's split.
fn pd_client(mock: &MockPd, split_concurrency: usize, split_batch_key_cnt: usize) -> PdSplitClient {
    let mut c = new_client(
        Arc::new(mock.clone()),
        None,
        tidb_pd_client::ClusterSecurity::default(),
        runtime(),
        split_batch_key_cnt,
        split_concurrency,
        None,
    );
    let m = mock.clone();
    let split: MockSplit =
        Box::new(move |region, keys| (false, Ok(Some(m.split_region(region, keys)))));
    c.mock_split = Some(split);
    c
}

/// Go `pdClient{needScatterVal: true, ...}` with `needScatterInit` done.
fn scattering_client(mock: &MockPd) -> PdSplitClient {
    let c = pd_client(mock, 1, 100);
    c.need_scatter_val.set(true).unwrap();
    c
}

fn region(id: u64) -> RegionInfo {
    RegionInfo {
        region: metapb::Region {
            id,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Go `checkRegionsBoundaries`.
fn check_regions_boundaries(regions: &[RegionInfo], expected: &[Vec<u8>]) {
    assert_eq!(regions.len(), expected.len() - 1, "{regions:?}");
    for i in 1..expected.len() {
        assert_eq!(
            expected[i - 1],
            regions[i - 1].region.start_key,
            "region {i}"
        );
        assert_eq!(expected[i], regions[i - 1].region.end_key, "region {i}");
    }
}

fn bg() -> Context {
    Context::background()
}

#[test]
fn scan_region_backoffer_with_success() {
    let _s = serial();
    let mut counter = 0;
    let res = with_retry(
        &bg(),
        || {
            counter += 1;
            if counter == 4 {
                return Ok(());
            }
            Err(errors::new(&errors::ERR_PD_BATCH_SCAN_REGION))
        },
        &mut WaitRegionOnlineBackoffer::new(),
    );
    assert!(res.is_ok());
    assert_eq!(counter, 4);
}

#[test]
fn scan_region_backoffer_with_fail() {
    let _s = serial();
    let _fp = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/br/pkg/restore/split/hint-scan-region-backoff",
        "return(true)",
    );
    let mut counter = 0;
    let res = with_retry(
        &bg(),
        || {
            counter += 1;
            Err(errors::new(&errors::ERR_PD_BATCH_SCAN_REGION))
        },
        &mut WaitRegionOnlineBackoffer::new(),
    );
    assert!(res.is_err());
    assert_eq!(
        counter,
        WAIT_REGION_ONLINE_ATTEMPT_TIMES.load(Ordering::Relaxed)
    );
}

#[test]
fn scan_region_backoffer_with_stop_retry() {
    let _s = serial();
    let _fp = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/br/pkg/restore/split/hint-scan-region-backoff",
        "return(true)",
    );
    let mut counter = 0;
    let res = with_retry(
        &bg(),
        || {
            counter += 1;
            if counter <= 5 {
                return Err(errors::new(&errors::ERR_PD_BATCH_SCAN_REGION));
            }
            Err(errors::new(&errors::ERR_KV_UNKNOWN))
        },
        &mut WaitRegionOnlineBackoffer::new(),
    );
    assert!(res.is_err());
    assert_eq!(counter, 6);
}

/// Go `recordCntBackoffer`.
struct RecordCntBackoffer {
    already: usize,
}

impl BackoffStrategy for RecordCntBackoffer {
    fn next_backoff(&mut self, _: &errors::Error) -> Duration {
        self.already += 1;
        Duration::ZERO
    }
    fn remaining_attempts(&self) -> i32 {
        100
    }
}

#[test]
fn scatter_sequentially_retry_cnt() {
    let _s = serial();
    let mock = MockPd::new();
    mock.state().scatter_region_fail_before = 7;
    let client = scattering_client(&mock);
    let mut bo = RecordCntBackoffer { already: 0 };
    client.scatter_regions_sequentially(&bg(), &[region(1), region(2)], &mut bo);
    assert_eq!(bo.already, 7);
}

#[test]
fn batch_scatter_regions_retry_cnt() {
    let _s = serial();
    let mock = MockPd::new();
    mock.state().scatter_regions_failed_count = 7;
    let client = scattering_client(&mock);
    client
        .scatter_regions(&bg(), &[region(1), region(2)])
        .unwrap();
}

#[test]
fn scatter_backward_compatibility() {
    let _s = serial();
    let mock = MockPd::new();
    mock.state().scatter_regions_not_implemented = true;
    let client = scattering_client(&mock);
    client
        .scatter_regions(&bg(), &[region(1), region(2)])
        .unwrap();
    assert_eq!(
        mock.state().scatter_region_count,
        HashMap::from([(1, 1), (2, 1)])
    );
}

fn op(desc: &str, status: pdpb::OperatorStatus) -> pdpb::GetOperatorResponse {
    pdpb::GetOperatorResponse {
        desc: desc.as_bytes().to_vec(),
        status: status as i32,
        ..Default::default()
    }
}

fn op_err(ty: pdpb::ErrorType) -> pdpb::GetOperatorResponse {
    pdpb::GetOperatorResponse {
        header: Some(pdpb::ResponseHeader {
            error: Some(pdpb::Error {
                r#type: ty as i32,
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn wait_for_scatter_regions() {
    use pdpb::OperatorStatus::{Cancel, Replace, Running, Success, Timeout};
    let _s = serial();
    let mock = MockPd::new();
    mock.state().scatter_regions_not_implemented = true;
    let client = scattering_client(&mock);
    let regions: Vec<RegionInfo> = (1..=6).map(region).collect();
    let drained = |mock: &MockPd| {
        for (id, rs) in mock.state().get_operator_responses.as_ref().unwrap() {
            assert!(rs.is_empty(), "region {id} has responses left");
        }
    };

    mock.state().get_operator_responses = Some(HashMap::from([
        (1, vec![op_err(pdpb::ErrorType::RegionNotFound)]),
        (2, vec![op("not-scatter-region", Success)]),
        (3, vec![op("scatter-region", Success)]),
        (
            4,
            vec![
                op("scatter-region", Running),
                op("scatter-region", Timeout),
                op("scatter-region", Success),
            ],
        ),
        (
            5,
            vec![
                op("scatter-region", Cancel),
                op("scatter-region", Cancel),
                op("scatter-region", Cancel),
                op("scatter-region", Running),
                op("scatter-region", Running),
                op("not-scatter-region", Success),
            ],
        ),
        (
            6,
            vec![op("scatter-region", Replace), op("scatter-region", Success)],
        ),
    ]));
    let (left, res) = client.wait_regions_scattered(&bg(), &regions);
    res.unwrap();
    assert_eq!(left, 0);
    {
        let st = mock.state();
        for i in 1..=3 {
            assert_eq!(st.scatter_region_count.get(&i).copied().unwrap_or(0), 0);
        }
        assert_eq!(st.scatter_region_count[&4], 1);
        assert_eq!(st.scatter_region_count[&5], 3);
        assert_eq!(st.scatter_region_count[&6], 1);
    }
    drained(&mock);

    // A non-retryable error.
    mock.state().scatter_region_count.clear();
    mock.state().get_operator_responses = Some(HashMap::from([
        (1, vec![op_err(pdpb::ErrorType::RegionNotFound)]),
        (2, vec![op("not-scatter-region", Success)]),
        (3, vec![op_err(pdpb::ErrorType::DataCompacted)]),
    ]));
    let (left, res) = client.wait_regions_scattered(&bg(), &regions);
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("get operator error: DATA_COMPACTED"));
    assert_eq!(left, 4);
    drained(&mock);
    assert!(mock.state().scatter_region_count.is_empty());

    // The backoff times out.
    let _restore = set_tunable(&WAIT_REGION_ONLINE_ATTEMPT_TIMES, 2);
    mock.state().get_operator_responses = Some(HashMap::from([
        (1, vec![op_err(pdpb::ErrorType::RegionNotFound)]),
        (2, vec![op("not-scatter-region", Success)]),
        (3, vec![op("scatter-region", Success)]),
        (
            4,
            vec![
                op("scatter-region", Running),
                op("scatter-region", Running),
                op("scatter-region", Running),
            ],
        ),
        (5, vec![op("not-scatter-region", Success)]),
        (6, vec![op("scatter-region", Success)]),
    ]));
    let (left, res) = client.wait_regions_scattered(&bg(), &regions);
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("the first unfinished region: {id:4}"));
    assert_eq!(left, 1);
    drained(&mock);
    assert!(mock.state().scatter_region_count.is_empty());
}

#[test]
fn backoff_may_not_count_backoffer() {
    let _s = serial();
    let mut b = BackoffMayNotCountBackoffer::new();
    let init = b.remaining_attempts();
    b.next_backoff(&ERR_BACKOFF_AND_DONT_COUNT);
    assert_eq!(b.remaining_attempts(), init);
    // Annotated, as the callers do.
    b.next_backoff(&errors::annotate(
        Arc::clone(&ERR_BACKOFF_AND_DONT_COUNT),
        "caller message",
    ));
    assert_eq!(b.remaining_attempts(), init);
    b.next_backoff(&ERR_BACKOFF);
    assert_eq!(b.remaining_attempts(), init - 1);
    b.next_backoff(&errors::errorf("test"));
    assert_eq!(b.remaining_attempts(), 0);
}

#[test]
fn split_ctx_cancel() {
    let _s = serial();
    let (ctx, cancel) = Context::with_cancel(&bg());
    let mock = MockPd::new();
    let mut client = pd_client(&mock, 1, 100);
    let split: MockSplit = Box::new(move |_, _| {
        cancel.cancel();
        let regions = vec![
            metapb::Region {
                id: 1,
                ..Default::default()
            },
            metapb::Region {
                id: 2,
                ..Default::default()
            },
        ];
        (
            false,
            Ok(Some(kvrpcpb::SplitRegionResponse {
                regions,
                ..Default::default()
            })),
        )
    });
    client.mock_split = Some(split);
    let err = client
        .split_wait_and_scatter(&ctx, &RegionInfo::default(), &[vec![1]])
        .unwrap_err();
    assert!(errors::is_context_canceled(err.as_ref()), "{err}");
}

#[test]
fn get_split_key_per_region() {
    let _s = serial();
    // Moved from BR: raw region bounds against encoded keys, as Go's.
    let sorted_keys: Vec<Vec<u8>> = ["b", "d", "g", "j", "l", "m"]
        .iter()
        .map(|k| k.as_bytes().to_vec())
        .collect();
    let r = |id, s: &str, e: &str| RegionInfo {
        region: metapb::Region {
            id,
            start_key: s.as_bytes().to_vec(),
            end_key: e.as_bytes().to_vec(),
            ..Default::default()
        },
        ..Default::default()
    };
    let sorted_regions = vec![r(1, "a", "g"), r(2, "g", "k"), r(3, "k", "m")];
    let result: HashMap<u64, Vec<Vec<u8>>> =
        get_split_keys_of_regions(&sorted_keys, &sorted_regions)
            .into_iter()
            .map(|(r, k)| (r.region.id, k))
            .collect();
    assert_eq!(result.len(), 3);
    assert_eq!(result[&1], vec![b"b".to_vec(), b"d".to_vec()]);
    assert_eq!(result[&2], vec![b"g".to_vec(), b"j".to_vec()]);
    assert_eq!(result[&3], vec![b"l".to_vec()]);

    // Moved from lightning.
    let table_id = 1;
    let row = |h: i64| {
        tidb_tablecodec::table_key::encode_row_key_with_handle(
            table_id,
            &tidb_tablecodec::table_key::RecordHandle::Int(h),
        )
    };
    let ends = [1_i64, 10, 100, 1000, 10000, -1];
    let mut sorted_regions = Vec::new();
    let mut region_start = enc(&row(0));
    for (i, end) in ends.iter().enumerate() {
        let region_end = if *end >= 0 {
            enc(&row(*end))
        } else {
            Vec::new()
        };
        sorted_regions.push(RegionInfo {
            region: metapb::Region {
                id: i as u64,
                start_key: region_start,
                end_key: region_end.clone(),
                ..Default::default()
            },
            ..Default::default()
        });
        region_start = region_end;
    }
    let check_keys: [(i64, i64); 9] = [
        (0, -1),
        (5, 1),
        (6, 1),
        (7, 1),
        (50, 2),
        (60, 2),
        (70, 2),
        (100, -1),
        (50000, 5),
    ];
    let mut expected: HashMap<u64, Vec<Vec<u8>>> = HashMap::new();
    let mut sorted_keys = Vec::new();
    for (hdl, idx) in check_keys {
        let key = row(hdl);
        sorted_keys.push(key.clone());
        if idx >= 0 {
            expected.entry(idx as u64).or_default().push(key);
        }
    }
    sorted_keys.sort();
    for v in expected.values_mut() {
        v.sort();
    }
    let got = get_split_keys_of_regions(&sorted_keys, &sorted_regions);
    assert_eq!(got.len(), expected.len());
    for (region, keys) in got {
        assert_eq!(expected[&region.region.id], keys);
    }
}

#[test]
fn paginate_scan_region_retries_and_paginates() {
    let _s = serial();
    let _restore = set_tunable(&WAIT_REGION_ONLINE_ATTEMPT_TIMES, 3);
    let mock = MockPd::new();
    let client = pd_client(&mock, 1, 100);
    let ctx = bg();

    // No region.
    let err = paginate_scan_region(&ctx, &client, &[], &[], 3).unwrap_err();
    assert!(errors::is(err.as_ref(), &errors::ERR_PD_BATCH_SCAN_REGION));
    assert!(
        err.to_string().contains("scan region return empty result"),
        "{err}"
    );

    // Retry on error.
    mock.state().scan_errors = vec![Some(errors::grpc(tonic::Status::unavailable("not leader")))];
    mock.set_regions(&[vec![], vec![]]);
    let got = paginate_scan_region(&ctx, &client, &[], &[], 3).unwrap();
    check_regions_boundaries(&got, &[vec![], vec![]]);

    // Paginate.
    let boundaries: Vec<Vec<u8>> = vec![
        vec![],
        vec![1],
        vec![2],
        vec![3],
        vec![4],
        vec![5],
        vec![6],
        vec![7],
        vec![8],
        vec![],
    ];
    mock.set_regions(&boundaries);
    check_regions_boundaries(
        &paginate_scan_region(&ctx, &client, &[], &[], 3).unwrap(),
        &boundaries,
    );
    check_regions_boundaries(
        &paginate_scan_region(&ctx, &client, &[1], &[], 3).unwrap(),
        &boundaries[1..],
    );
    check_regions_boundaries(
        &paginate_scan_region(&ctx, &client, &[], &[2], 8).unwrap(),
        &boundaries[..3],
    );
    check_regions_boundaries(
        &paginate_scan_region(&ctx, &client, &[4], &[5], 1).unwrap(),
        &[vec![4], vec![5]],
    );

    // start == end.
    let err = paginate_scan_region(&ctx, &client, &[4], &[4], 1).unwrap_err();
    assert!(
        err.to_string().contains("scan region return empty result"),
        "{err}"
    );

    // start > end.
    let err = paginate_scan_region(&ctx, &client, &[5], &[4], 5).unwrap_err();
    assert!(errors::is(err.as_ref(), &errors::ERR_INVALID_RANGE));
    assert!(err.to_string().contains("startKey > endKey"), "{err}");

    // Retries exhausted.
    mock.state().scan_errors = (0..3)
        .map(|_| Some(errors::grpc(tonic::Status::unavailable("not leader"))))
        .collect();
    let err = paginate_scan_region(&ctx, &client, &[4], &[5], 1).unwrap_err();
    assert!(err.to_string().contains("not leader"), "{err}");

    // Regions not continuous.
    let reg = |id: u64, s: u8, e: u8| {
        (
            metapb::Region {
                id,
                start_key: vec![s],
                end_key: vec![e],
                ..Default::default()
            },
            metapb::Peer {
                id,
                store_id: 1,
                ..Default::default()
            },
        )
    };
    mock.state().regions = vec![reg(1, 1, 2), reg(4, 4, 5)];
    let err = paginate_scan_region(&ctx, &client, &[1], &[5], 3).unwrap_err();
    assert!(errors::is(err.as_ref(), &errors::ERR_PD_BATCH_SCAN_REGION));
    assert!(
        err.to_string()
            .contains("region 1's endKey not equal to next region 4's startKey"),
        "{err}"
    );

    // Regions become continuous slowly.
    let mut to_add = vec![reg(2, 2, 3), reg(3, 3, 4)];
    mock.state().scan_before_hook = Some(Box::new(move |regions| {
        if !to_add.is_empty() {
            regions.push(to_add.remove(0));
        }
    }));
    let got = paginate_scan_region(&ctx, &client, &[1], &[5], 100).unwrap();
    check_regions_boundaries(&got, &[vec![1], vec![2], vec![3], vec![4], vec![5]]);
}

#[test]
fn region_consistency_errors() {
    let _s = serial();
    let e = |k: &str| enc(k.as_bytes());
    let info = |id: u64, s: &str, en: &str, leader: Option<(u64, u64)>| RegionInfo {
        region: metapb::Region {
            id,
            start_key: e(s),
            end_key: e(en),
            ..Default::default()
        },
        leader: leader.map(|(id, store_id)| metapb::Peer {
            id,
            store_id,
            ..Default::default()
        }),
        ..Default::default()
    };
    type Case<'a> = (Vec<u8>, Vec<u8>, &'a str, Vec<RegionInfo>);
    let cases: Vec<Case<'_>> = vec![
        (
            e("a"),
            e("a"),
            "scan region return empty result, startKey: ",
            vec![],
        ),
        (
            e("a"),
            e("a"),
            "first region 1's startKey(",
            vec![info(1, "b", "d", None)],
        ),
        (
            e("b"),
            e("e"),
            "last region 100's endKey(",
            vec![info(100, "b", "d", None)],
        ),
        (
            e("c"),
            e("e"),
            "region 6's endKey not equal to next region 8's startKey",
            vec![
                info(6, "b", "d", Some((6, 1))),
                info(8, "e", "f", Some((8, 1))),
            ],
        ),
        (
            e("c"),
            e("e"),
            "region 6's leader is nil",
            vec![info(6, "c", "d", None), info(8, "d", "e", None)],
        ),
        (
            e("c"),
            e("e"),
            "region 6's leader's store id is 0",
            vec![
                info(6, "c", "d", Some((6, 0))),
                info(8, "d", "e", Some((6, 0))),
            ],
        ),
    ];
    for (start, end, want, regions) in cases {
        let err = check_region_consistency(&start, &end, &regions, false).unwrap_err();
        assert!(err.to_string().contains(want), "{err} lacks {want}");
    }
}

#[test]
fn scan_empty_region() {
    let _s = serial();
    let mock = MockPd::new();
    mock.set_regions(&[vec![], enc(&[12]), enc(&[34]), vec![]]);
    let splitter = RegionSplitter::new(pd_client(&mock, 4, 100), 0);
    // One key only: Go `initKeys()[0:1]`.
    splitter
        .execute_sorted_keys(&bg(), &[b"aae".to_vec()])
        .unwrap();
}

#[test]
fn split_empty_region() {
    let _s = serial();
    let mock = MockPd::new();
    mock.set_regions(&[vec![], enc(&[12]), enc(&[34]), vec![]]);
    let splitter = RegionSplitter::new(pd_client(&mock, 4, 100), 0);
    splitter.execute_sorted_keys(&bg(), &[]).unwrap();
}

/// Go `recordingSplitClient`.
#[derive(Default)]
struct RecordingSplitClient {
    calls: Mutex<Vec<(Vec<Vec<u8>>, bool)>>,
}

impl SplitClient for RecordingSplitClient {
    fn get_store(&self, _: &Context, _: u64) -> Result<metapb::Store> {
        unimplemented!()
    }
    fn get_region(&self, _: &Context, _: &[u8]) -> Result<Option<RegionInfo>> {
        unimplemented!()
    }
    fn get_region_by_id(&self, _: &Context, _: u64) -> Result<Option<RegionInfo>> {
        unimplemented!()
    }
    fn split_keys_and_scatter(&self, _: &Context, keys: &[Vec<u8>]) -> Result<Vec<RegionInfo>> {
        self.calls.lock().unwrap().push((keys.to_vec(), true));
        Ok(Vec::new())
    }
    fn split_keys(&self, _: &Context, keys: &[Vec<u8>]) -> Result<Vec<RegionInfo>> {
        self.calls.lock().unwrap().push((keys.to_vec(), false));
        Ok(Vec::new())
    }
    fn split_wait_and_scatter(
        &self,
        _: &Context,
        _: &RegionInfo,
        _: &[Vec<u8>],
    ) -> Result<Vec<RegionInfo>> {
        unimplemented!()
    }
    fn get_operator(&self, _: &Context, _: u64) -> Result<pdpb::GetOperatorResponse> {
        unimplemented!()
    }
    fn scan_regions(&self, _: &Context, _: &[u8], _: &[u8], _: usize) -> Result<Vec<RegionInfo>> {
        unimplemented!()
    }
    fn get_placement_rule(
        &self,
        _: &Context,
        _: &str,
        _: &str,
    ) -> Result<tikv_client::pd_http::Rule> {
        unimplemented!()
    }
    fn set_placement_rule(&self, _: &Context, _: &tikv_client::pd_http::Rule) -> Result<()> {
        unimplemented!()
    }
    fn delete_placement_rule(&self, _: &Context, _: &str, _: &str) -> Result<()> {
        unimplemented!()
    }
    fn set_stores_label(&self, _: &Context, _: &[u64], _: &str, _: &str) -> Result<()> {
        unimplemented!()
    }
    fn wait_regions_scattered(&self, _: &Context, _: &[RegionInfo]) -> (usize, Result<()>) {
        (0, Ok(()))
    }
}

#[test]
fn region_splitter_rough_split_uses_configured_region_index_step() {
    let _s = serial();
    let keys: Vec<Vec<u8>> = b"abcdef".iter().map(|k| vec![*k]).collect();
    for (coarse, scatter_by_calls) in [(false, [true, true]), (true, [true, false])] {
        let mut splitter = RegionSplitter::new(RecordingSplitClient::default(), 2);
        splitter.set_coarse_scatter(coarse);
        splitter.execute_sorted_keys(&bg(), &keys).unwrap();
        let calls = splitter.client().calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0, vec![vec![b'c'], vec![b'e']]);
        assert_eq!(calls[1].0, keys);
        assert_eq!([calls[0].1, calls[1].1], scatter_by_calls);
    }
}

#[test]
fn batch_split() {
    let _s = serial();
    let backup = MAX_BATCH_SPLIT_SIZE.swap(7, Ordering::Relaxed);
    let mock = MockPd::new();
    let set = mock.set_regions(&[vec![], vec![]]);
    assert_eq!(set.len(), 1);
    let split_region = RegionInfo {
        region: set[0].clone(),
        ..Default::default()
    };
    let client = pd_client(&mock, 1, 100);
    let split_keys: Vec<Vec<u8>> = ["ba", "bb", "bc", "bd", "be", "bf", "bg", "bh"]
        .iter()
        .map(|k| k.as_bytes().to_vec())
        .collect();
    // Go's 2-byte raw keys fit 3 to a 7-byte batch; their 9-byte encodings
    // here go one per batch, so the batch limit counts the raw keys.
    client
        .split_wait_and_scatter(&bg(), &split_region, &split_keys)
        .unwrap();
    MAX_BATCH_SPLIT_SIZE.store(backup, Ordering::Relaxed);
    let regions = paginate_scan_region(&bg(), &client, &enc(b"b"), &enc(b"c"), 5).unwrap();
    let mut expected = vec![vec![]];
    expected.extend(split_keys.iter().map(|k| enc(k)));
    expected.push(vec![]);
    check_regions_boundaries(&regions, &expected);
    let st = mock.state();
    assert_eq!(st.split_count, 3);
    assert_eq!(st.scatter_regions_region_count, split_keys.len());
}

#[test]
fn split_scatter_empty_end_key() {
    let _s = serial();
    let mock = MockPd::new();
    let keys = encode_keys(&[b"", b"aay", b"bba", b"bbh", b"cca", b""]);
    mock.set_regions(&keys);
    let client = pd_client(&mock, 10, 100);
    // Go's trailing empty key only works in raw mode: encoded, an empty
    // last key makes the scan end sort before its start.
    client
        .split_keys_and_scatter(&bg(), &[b"b".to_vec(), b"c".to_vec()])
        .unwrap();
    let regions = paginate_scan_region(&bg(), &client, &[], &[], 5).unwrap();
    let result = encode_keys(&[b"", b"aay", b"b", b"bba", b"bbh", b"c", b"cca", b""]);
    check_regions_boundaries(&regions, &result);
}

#[test]
fn scan_region_empty_result() {
    let _s = serial();
    let _r1 = set_tunable(&WAIT_REGION_ONLINE_ATTEMPT_TIMES, 2);
    let _r2 = set_tunable(&SPLIT_RETRY_TIMES, 2);
    let mock = MockPd::new();
    mock.set_regions(&[vec![], vec![]]);
    mock.state().scan_errors = vec![None; 8];
    let client = pd_client(&mock, 4, 100);
    let err = client
        .split_keys_and_scatter(&bg(), &[b"ba".to_vec(), b"bb".to_vec()])
        .unwrap_err();
    assert!(
        err.to_string().contains("scan region return empty result"),
        "{err}"
    );
}

#[test]
fn split_meet_error_and_retry() {
    let _s = serial();
    let mock = MockPd::new();
    mock.set_regions(&encode_keys(&[b"", b"a", b""]));
    let mut client = pd_client(&mock, 1, 100);
    let hijack = |client: &mut PdSplitClient, msg: &'static str, once: bool| {
        let (m, fired) = (mock.clone(), Arc::new(Mutex::new(false)));
        let split: MockSplit = Box::new(move |region, keys| {
            let mut fired = fired.lock().unwrap();
            if once && *fired {
                return (false, Ok(Some(m.split_region(region, keys))));
            }
            *fired = true;
            m.state().split_count += 1;
            (false, Err(errors::errorf(msg)))
        });
        client.mock_split = Some(split);
    };

    // The epoch changes between the scan and the split.
    hijack(&mut client, "epoch not match", true);
    client
        .split_keys_and_scatter(&bg(), &[b"b".to_vec()])
        .unwrap();
    let regions = paginate_scan_region(&bg(), &client, &enc(b"a"), &[], 5).unwrap();
    check_regions_boundaries(&regions, &encode_keys(&[b"a", b"b", b""]));
    assert_eq!(mock.state().split_count, 2);

    // "no valid key".
    hijack(&mut client, "no valid key", true);
    mock.state().split_count = 0;
    client
        .split_keys_and_scatter(&bg(), &[b"c".to_vec()])
        .unwrap();
    let regions = paginate_scan_region(&bg(), &client, &enc(b"b"), &[], 5).unwrap();
    check_regions_boundaries(&regions, &encode_keys(&[b"b", b"c", b""]));
    assert_eq!(mock.state().split_count, 2);

    // The retries fail too.
    let _r = set_tunable(&SPLIT_RETRY_TIMES, 2);
    hijack(&mut client, "no valid key", false);
    let err = client
        .split_keys_and_scatter(&bg(), &[b"d".to_vec()])
        .unwrap_err();
    assert!(err.to_string().contains("no valid key"), "{err}");
}

#[test]
fn pd_error_can_retry_matches_go() {
    let _s = serial();
    assert!(!pd_error_can_retry(&errors::errorf("random failure")));
    let st = |m: &str| errors::grpc(tonic::Status::unknown(m));
    assert!(pd_error_can_retry(&st("region 42 is not fully replicated")));
    assert!(pd_error_can_retry(&st(
        "operator canceled because cannot add an operator to the execute queue"
    )));
    assert!(pd_error_can_retry(&st(
        "unable to create operator, failed to create scatter region operator for region 13813282"
    )));
    assert!(!pd_error_can_retry(&st("should be false")));
}

#[test]
fn regions_not_fully_scatter() {
    let _s = serial();
    let mock = MockPd::new();
    let client = scattering_client(&mock);
    let regions = [region(1), region(2)];
    client.scatter_regions(&bg(), &regions).unwrap();
    assert_eq!(mock.state().scatter_regions_region_count, 2);
    assert!(mock.state().scatter_region_count.is_empty());

    // One region is not scattered by the batch.
    mock.state().scatter_regions_finished_percentage = 50;
    client.scatter_regions(&bg(), &regions).unwrap();
    assert_eq!(mock.state().scatter_regions_region_count, 3);
    assert_eq!(
        mock.state().scatter_region_count,
        HashMap::from([(1, 1), (2, 1)])
    );

    // The one-region scatter fails too, 7 times each.
    mock.state().scatter_region_fail_before = 7;
    client.scatter_regions(&bg(), &regions).unwrap();
    assert_eq!(mock.state().scatter_regions_region_count, 4);
    assert_eq!(
        mock.state().scatter_region_count,
        HashMap::from([(1, 8), (2, 8)])
    );
}

/// Go `TestSplitAndScatter`.
#[test]
fn split_and_scatter_rewritten_ranges() {
    let _s = serial();
    let mock = MockPd::new();
    mock.set_regions(&encode_keys(&[b"", b"aay", b"bba", b"bbh", b"cca", b""]));
    let splitter = RegionSplitter::new(pd_client(&mock, 4, 100), 0);
    // Go `initRanges` and `initRewriteRules`.
    let ranges = [
        ("aaa", "aae"),
        ("aae", "aaz"),
        ("ccd", "ccf"),
        ("ccf", "ccj"),
    ];
    let rules = crate::restore_utils::RewriteRules {
        data: vec![
            tidb_proto::import_sstpb::RewriteRule {
                old_key_prefix: b"aa".to_vec(),
                new_key_prefix: b"xx".to_vec(),
                ..Default::default()
            },
            tidb_proto::import_sstpb::RewriteRule {
                old_key_prefix: b"cc".to_vec(),
                new_key_prefix: b"bb".to_vec(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let mut split_keys: Vec<Vec<u8>> = ranges
        .iter()
        .map(|(s, e)| {
            crate::restore_utils::rewrite_range(
                crate::rtree::Range::new(s.as_bytes(), e.as_bytes()),
                Some(&rules),
            )
            .unwrap()
            .end_key()
            .to_vec()
        })
        .collect();
    split_keys.sort();
    splitter.execute_sorted_keys(&bg(), &split_keys).unwrap();
    let regions = scan_range(&mut mock.state().regions, &[], &[], 100);
    let expected = encode_keys(&[
        b"", b"aay", b"bba", b"bbf", b"bbh", b"bbj", b"cca", b"xxe", b"xxz", b"",
    ]);
    assert_eq!(regions.len(), expected.len() - 1);
    for (i, r) in regions.iter().enumerate() {
        assert_eq!(expected[i], r.meta.start_key, "region {i}");
        assert_eq!(expected[i + 1], r.meta.end_key, "region {i}");
    }
}

/// Go `TestSplitAndScatter` "not leader with missing region by ID returns
/// epoch not match".
#[test]
fn split_not_leader_with_missing_region_returns_epoch_not_match() {
    let _s = serial();
    let _fp = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/br/pkg/restore/split/not-leader-error",
        "return(false)",
    );
    let mock = MockPd::new();
    let regions = mock.set_regions(&[vec![], b"m".to_vec(), vec![]]);
    mock.state().stores.insert(
        1,
        metapb::Store {
            id: 1,
            address: "127.0.0.1:1".into(),
            ..Default::default()
        },
    );
    mock.state().nil_region_id = Some(regions[0].id);
    let mut client = pd_client(&mock, 4, 100);
    client.mock_split = None;
    let split_region = RegionInfo {
        region: regions[0].clone(),
        leader: Some(metapb::Peer {
            id: regions[0].id,
            store_id: 1,
            ..Default::default()
        }),
        ..Default::default()
    };
    let err = client
        .split_wait_and_scatter(&bg(), &split_region, &[b"a".to_vec()])
        .unwrap_err();
    // Go `containsKVEpochNotMatch`: in the error or one of a multierr's.
    let errs = crate::utils::retry::multierr_errors(&err);
    assert!(
        errs.iter()
            .any(|e| errors::is(e.as_ref(), &errors::ERR_KV_EPOCH_NOT_MATCH)),
        "{err}"
    );
}

#[test]
fn is_scatter_region_finished_matches_go() {
    let _s = serial();
    assert_eq!(
        is_scatter_region_finished(&op_err(pdpb::ErrorType::RegionNotFound)).unwrap(),
        (true, false)
    );
    assert_eq!(
        is_scatter_region_finished(&op("x", pdpb::OperatorStatus::Running)).unwrap(),
        (true, false)
    );
    assert_eq!(
        is_scatter_region_finished(&op("scatter-region", pdpb::OperatorStatus::Running)).unwrap(),
        (false, false)
    );
    assert_eq!(
        is_scatter_region_finished(&op("scatter-region", pdpb::OperatorStatus::Cancel)).unwrap(),
        (false, true)
    );
}
