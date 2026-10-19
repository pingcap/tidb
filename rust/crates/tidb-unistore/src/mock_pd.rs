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

//! Go unistore's `MockPD` (`tikv/mock_region.go`) with its
//! `gcStatesManagerSimulator`: the embedded store's PD -- region and store
//! lookups over the region manager, the oracle, and PD's GC-state API (txn
//! and GC safe points, GC barriers, the service safe point compatibility
//! call).
//!
//! # Narrowings, by name
//!
//! * `GetTS`'s process-global `tsMu`: every store here owns one oracle
//!   ([`crate::tso::Tso`]) that its MVCC store shares, which keeps one store's
//!   timestamps strictly increasing, the property the global gives Go.
//! * GC barrier TTLs: unimplemented in Go too (`TTLNeverExpire`).
//! * `AskSplit` / `AskBatchSplit` / `ReportBatchSplit` /
//!   `SetRegionHeartbeatResponseHandler`, `SetGlobalGCBarrier` /
//!   `DeleteGlobalGCBarrier` / `GetAllKeyspacesGCStates`: Go panics
//!   "unimplemented"; they are absent.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_proto::metapb;

use crate::region_manager::InProcessRegionManager;
use crate::tso::Tso;

/// PD's `constants.NullKeyspaceID`.
pub const NULL_KEYSPACE_ID: u32 = 0xFFFF_FFFF;

/// The cluster id Go's `NewMock(conf, 1)` gives the region manager.
pub const CLUSTER_ID: u64 = 1;

/// The GC-state errors the simulator raises, each rendering Go's message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GcStatesError {
    /// `[PD:gc:ErrDecreasingTxnSafePoint]`.
    DecreasingTxnSafePoint {
        /// The current txn safe point.
        current: u64,
        /// The target given.
        target: u64,
    },
    /// Go `errDecreasingGCSafePoint`.
    DecreasingGcSafePoint {
        /// The current GC safe point.
        current: u64,
        /// The target given.
        target: u64,
    },
    /// The GC safe point would pass the txn safe point.
    GcSafePointExceedsTxnSafePoint {
        /// The current GC safe point.
        current: u64,
        /// The target given.
        target: u64,
        /// The current txn safe point.
        txn_safe_point: u64,
    },
    /// Go `errGCBarrierTSBehindTxnSafePoint`.
    GcBarrierTsBehindTxnSafePoint {
        /// The barrier ts attempted.
        attempted_barrier_ts: u64,
        /// The current txn safe point.
        txn_safe_point: u64,
    },
    /// Go's `errors.New("invalid arguments")`.
    InvalidArguments,
    /// The "gc_worker" service safe point is set with a TTL other than
    /// `math.MaxInt64`.
    GcWorkerTtl,
}

impl std::fmt::Display for GcStatesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DecreasingTxnSafePoint { current, target } => write!(
                f,
                "[PD:gc:ErrDecreasingTxnSafePoint] trying to update txn safe point to a smaller value, current value: {current}, given: {target}"
            ),
            Self::DecreasingGcSafePoint { current, target } => write!(
                f,
                "trying to update gc safe point to a smaller value, current value: {current}, given: {target}"
            ),
            Self::GcSafePointExceedsTxnSafePoint {
                current,
                target,
                txn_safe_point,
            } => write!(
                f,
                "trying to update GC safe point to a too large value that exceeds the txn safe point, current value: {current}, given: {target}, current txn safe point: {txn_safe_point}"
            ),
            Self::GcBarrierTsBehindTxnSafePoint {
                attempted_barrier_ts,
                txn_safe_point,
            } => write!(
                f,
                "trying to set a GC barrier on ts {attempted_barrier_ts} which is already behind the txn safe point {txn_safe_point}"
            ),
            Self::InvalidArguments => f.write_str("invalid arguments"),
            Self::GcWorkerTtl => {
                f.write_str("ttl of gc_worker's service safe point must be math.MaxInt64")
            }
        }
    }
}

impl std::error::Error for GcStatesError {}

/// pd-client `gc.AdvanceTxnSafePointResult`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdvanceTxnSafePointResult {
    /// `OldTxnSafePoint`.
    pub old_txn_safe_point: u64,
    /// `Target`.
    pub target: u64,
    /// `NewTxnSafePoint`.
    pub new_txn_safe_point: u64,
    /// `BlockerDescription`.
    pub blocker_description: String,
}

/// pd-client `gc.AdvanceGCSafePointResult`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdvanceGcSafePointResult {
    /// `OldGCSafePoint`.
    pub old_gc_safe_point: u64,
    /// `Target`.
    pub target: u64,
    /// `NewGCSafePoint`.
    pub new_gc_safe_point: u64,
}

/// pd-client `gc.GCBarrierInfo`, the TTL never expiring.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcBarrierInfo {
    /// `BarrierID`.
    pub barrier_id: String,
    /// `BarrierTS`.
    pub barrier_ts: u64,
}

/// pd-client `gc.GCState`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcState {
    /// `KeyspaceID`.
    pub keyspace_id: u32,
    /// `TxnSafePoint`.
    pub txn_safe_point: u64,
    /// `GCSafePoint`.
    pub gc_safe_point: u64,
    /// The barriers, `None` when the call excluded them (Go
    /// `HasGCBarriers() == false`).
    pub gc_barriers: Option<Vec<GcBarrierInfo>>,
}

/// Go `gcState`.
#[derive(Debug, Default)]
struct KeyspaceGcState {
    txn_safe_point: u64,
    gc_safe_point: u64,
    /// Barrier id -> barrier ts.
    gc_barriers: BTreeMap<String, u64>,
}

/// Go `gcStatesManagerSimulator`: PD's GC states, one per keyspace id,
/// every keyspace id taken as valid.
#[derive(Debug, Default)]
pub struct GcStatesManagerSimulator {
    keyspace_gc_states: Mutex<HashMap<u32, KeyspaceGcState>>,
}

/// Go `fmt.Sprintf("%+q", s)`: a double-quoted string with every non-ASCII
/// or non-printable character escaped.
fn go_quote_ascii(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\x07' => out.push_str("\\a"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x0b' => out.push_str("\\v"),
            ' '..='~' => out.push(c),
            c if (c as u32) < 0x80 => out.push_str(&format!("\\x{:02x}", c as u32)),
            c if (c as u32) <= 0xffff => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push_str(&format!("\\U{:08x}", c as u32)),
        }
    }
    out.push('"');
    out
}

impl GcStatesManagerSimulator {
    /// Go `newGCStatesManager`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn states(&self) -> std::sync::MutexGuard<'_, HashMap<u32, KeyspaceGcState>> {
        self.keyspace_gc_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `UpdateServiceGCSafePoint`, PD's compatibility wrapper: the
    /// "gc_worker" service advances the txn safe point; any other service
    /// sets (positive TTL) or deletes its GC barrier. Answers the txn safe
    /// point, as if "gc_worker" held the minimal service safe point.
    pub fn update_service_gc_safe_point(
        &self,
        service_id: &str,
        ttl_secs: i64,
        safe_point: u64,
    ) -> Result<u64, GcStatesError> {
        if service_id == "gc_worker" {
            if ttl_secs != i64::MAX {
                return Err(GcStatesError::GcWorkerTtl);
            }
            let res = self
                .gc_internal_controller(NULL_KEYSPACE_ID)
                .advance_txn_safe_point(safe_point)?;
            return Ok(res.new_txn_safe_point);
        }
        let mut states = self.states();
        if ttl_secs > 0 {
            let ttl = Duration::from_secs(u64::try_from(ttl_secs).unwrap_or(u64::MAX));
            match set_gc_barrier_impl(&mut states, NULL_KEYSPACE_ID, service_id, safe_point, ttl) {
                Err(error @ GcStatesError::GcBarrierTsBehindTxnSafePoint { .. }) => {
                    return Err(error)
                }
                // Go returns only the behind-txn-safe-point error here.
                Ok(_) | Err(_) => {}
            }
        } else {
            delete_gc_barrier_impl(&mut states, NULL_KEYSPACE_ID, service_id);
        }
        Ok(get_gc_state(&mut states, NULL_KEYSPACE_ID).txn_safe_point)
    }

    /// Go `GetGCInternalController`.
    #[must_use]
    pub fn gc_internal_controller(&self, keyspace_id: u32) -> GcInternalController<'_> {
        GcInternalController {
            inner: self,
            keyspace_id,
        }
    }

    /// Go `GetGCStatesClient`.
    #[must_use]
    pub fn gc_states_client(&self, keyspace_id: u32) -> GcStatesClient<'_> {
        GcStatesClient {
            inner: self,
            keyspace_id,
        }
    }
}

/// Go `getGCState`: the keyspace's state, created on first use.
fn get_gc_state(
    states: &mut HashMap<u32, KeyspaceGcState>,
    keyspace_id: u32,
) -> &mut KeyspaceGcState {
    states.entry(keyspace_id).or_default()
}

/// Go `setGCBarrierImpl`.
fn set_gc_barrier_impl(
    states: &mut HashMap<u32, KeyspaceGcState>,
    keyspace_id: u32,
    barrier_id: &str,
    barrier_ts: u64,
    ttl: Duration,
) -> Result<GcBarrierInfo, GcStatesError> {
    let state = get_gc_state(states, keyspace_id);
    if barrier_ts == 0 || barrier_id.is_empty() || ttl.is_zero() {
        return Err(GcStatesError::InvalidArguments);
    }
    if barrier_ts < state.txn_safe_point {
        return Err(GcStatesError::GcBarrierTsBehindTxnSafePoint {
            attempted_barrier_ts: barrier_ts,
            txn_safe_point: state.txn_safe_point,
        });
    }
    state.gc_barriers.insert(barrier_id.to_owned(), barrier_ts);
    Ok(GcBarrierInfo {
        barrier_id: barrier_id.to_owned(),
        barrier_ts,
    })
}

/// Go `deleteGCBarrierImpl`: the deleted barrier, `None` when none existed.
fn delete_gc_barrier_impl(
    states: &mut HashMap<u32, KeyspaceGcState>,
    keyspace_id: u32,
    barrier_id: &str,
) -> Option<GcBarrierInfo> {
    let state = get_gc_state(states, keyspace_id);
    state
        .gc_barriers
        .remove(barrier_id)
        .map(|barrier_ts| GcBarrierInfo {
            barrier_id: barrier_id.to_owned(),
            barrier_ts,
        })
}

/// Go `gcInternalController`: the GC worker's half of the GC-state API.
#[derive(Clone, Copy, Debug)]
pub struct GcInternalController<'a> {
    inner: &'a GcStatesManagerSimulator,
    keyspace_id: u32,
}

impl GcInternalController<'_> {
    /// Go `AdvanceTxnSafePoint`: the txn safe point moves to `target`, held
    /// back by the lowest GC barrier but never moved backward.
    pub fn advance_txn_safe_point(
        &self,
        target: u64,
    ) -> Result<AdvanceTxnSafePointResult, GcStatesError> {
        let mut states = self.inner.states();
        let state = get_gc_state(&mut states, self.keyspace_id);
        if target < state.txn_safe_point {
            return Err(GcStatesError::DecreasingTxnSafePoint {
                current: state.txn_safe_point,
                target,
            });
        }
        let mut res = AdvanceTxnSafePointResult {
            old_txn_safe_point: state.txn_safe_point,
            target,
            new_txn_safe_point: target,
            blocker_description: String::new(),
        };
        let mut min_barrier: Option<(&String, u64)> = None;
        for (name, &ts) in &state.gc_barriers {
            assert!(ts != 0, "found 0 in barrier ts of GC barriers");
            if min_barrier.is_none_or(|(_, min_ts)| ts < min_ts) {
                min_barrier = Some((name, ts));
            }
        }
        if let Some((name, ts)) = min_barrier {
            if ts < res.new_txn_safe_point {
                res.new_txn_safe_point = ts;
                res.blocker_description = format!(
                    "GCBarrier {{ BarrierID: {}, BarrierTS: {}, ExpirationTime: <nil> }}",
                    go_quote_ascii(name),
                    res.new_txn_safe_point
                );
                tidb_log::info(
                    "txn safe point blocked",
                    &[
                        tidb_log::Field::new(
                            "oldTxnSafePoint",
                            tidb_log::Value::U64(res.old_txn_safe_point),
                        ),
                        tidb_log::Field::new(
                            "newTxnSafePoint",
                            tidb_log::Value::U64(res.new_txn_safe_point),
                        ),
                        tidb_log::Field::new(
                            "blocker",
                            tidb_log::Value::Str(res.blocker_description.clone()),
                        ),
                    ],
                );
            }
        }
        if res.new_txn_safe_point < res.old_txn_safe_point {
            res.new_txn_safe_point = res.old_txn_safe_point;
            tidb_log::info(
                "txn safe point unable to be blocked",
                &[
                    tidb_log::Field::new(
                        "oldTxnSafePoint",
                        tidb_log::Value::U64(res.old_txn_safe_point),
                    ),
                    tidb_log::Field::new(
                        "newTxnSafePoint",
                        tidb_log::Value::U64(res.new_txn_safe_point),
                    ),
                    tidb_log::Field::new(
                        "blocker",
                        tidb_log::Value::Str(res.blocker_description.clone()),
                    ),
                ],
            );
        }
        state.txn_safe_point = res.new_txn_safe_point;
        Ok(res)
    }

    /// Go `AdvanceGCSafePoint`: never backward, never past the txn safe
    /// point.
    pub fn advance_gc_safe_point(
        &self,
        target: u64,
    ) -> Result<AdvanceGcSafePointResult, GcStatesError> {
        let mut states = self.inner.states();
        let state = get_gc_state(&mut states, self.keyspace_id);
        if target < state.gc_safe_point {
            return Err(GcStatesError::DecreasingGcSafePoint {
                current: state.gc_safe_point,
                target,
            });
        }
        if target > state.txn_safe_point {
            return Err(GcStatesError::GcSafePointExceedsTxnSafePoint {
                current: state.gc_safe_point,
                target,
                txn_safe_point: state.txn_safe_point,
            });
        }
        let res = AdvanceGcSafePointResult {
            old_gc_safe_point: state.gc_safe_point,
            target,
            new_gc_safe_point: target,
        };
        state.gc_safe_point = res.new_gc_safe_point;
        Ok(res)
    }
}

/// Go `gcStatesClient`: the GC-state API's half for every other component.
#[derive(Clone, Copy, Debug)]
pub struct GcStatesClient<'a> {
    inner: &'a GcStatesManagerSimulator,
    keyspace_id: u32,
}

impl GcStatesClient<'_> {
    /// Go `SetGCBarrier`.
    pub fn set_gc_barrier(
        &self,
        barrier_id: &str,
        barrier_ts: u64,
        ttl: Duration,
    ) -> Result<GcBarrierInfo, GcStatesError> {
        let mut states = self.inner.states();
        set_gc_barrier_impl(&mut states, self.keyspace_id, barrier_id, barrier_ts, ttl)
    }

    /// Go `DeleteGCBarrier`: the deleted barrier, `None` when none existed.
    pub fn delete_gc_barrier(&self, barrier_id: &str) -> Option<GcBarrierInfo> {
        let mut states = self.inner.states();
        delete_gc_barrier_impl(&mut states, self.keyspace_id, barrier_id)
    }

    /// Go `GetGCState`: barriers included unless `exclude_gc_barriers`
    /// (PD's default excludes them).
    #[must_use]
    pub fn get_gc_state(&self, exclude_gc_barriers: bool) -> GcState {
        let mut states = self.inner.states();
        let state = get_gc_state(&mut states, self.keyspace_id);
        GcState {
            keyspace_id: self.keyspace_id,
            txn_safe_point: state.txn_safe_point,
            gc_safe_point: state.gc_safe_point,
            gc_barriers: (!exclude_gc_barriers).then(|| {
                state
                    .gc_barriers
                    .iter()
                    .map(|(barrier_id, &barrier_ts)| GcBarrierInfo {
                        barrier_id: barrier_id.clone(),
                        barrier_ts,
                    })
                    .collect()
            }),
        }
    }
}

/// Go `MockPD`: the embedded store's PD.
#[derive(Debug)]
pub struct MockPd {
    rm: Arc<InProcessRegionManager>,
    gc_states: GcStatesManagerSimulator,
    external_timestamp: AtomicU64,
    tso: Arc<Tso>,
}

impl MockPd {
    /// Go `NewMockPD`, over the store's oracle.
    #[must_use]
    pub fn new(rm: Arc<InProcessRegionManager>, tso: Arc<Tso>) -> Self {
        Self {
            rm,
            gc_states: GcStatesManagerSimulator::new(),
            external_timestamp: AtomicU64::new(0),
            tso,
        }
    }

    /// The region manager beneath.
    #[must_use]
    pub fn region_manager(&self) -> &Arc<InProcessRegionManager> {
        &self.rm
    }

    /// The embedded `gcStatesManagerSimulator`.
    #[must_use]
    pub fn gc_states(&self) -> &GcStatesManagerSimulator {
        &self.gc_states
    }

    /// Go `GetClusterID`.
    #[must_use]
    pub fn get_cluster_id(&self) -> u64 {
        CLUSTER_ID
    }

    /// Go `AllocID`.
    #[must_use]
    pub fn alloc_id(&self) -> u64 {
        self.rm.alloc_id()
    }

    /// Go `Bootstrap`.
    pub fn bootstrap(&self, store: metapb::Store, region: metapb::Region) {
        self.rm.bootstrap(vec![store], region);
    }

    /// Go `IsBootstrapped`.
    #[must_use]
    pub fn is_bootstrapped(&self) -> bool {
        self.rm.is_bootstrapped()
    }

    /// Go `PutStore`.
    pub fn put_store(&self, store: metapb::Store) {
        self.rm
            .add_store(store.id, &store.address, store.labels.clone());
    }

    /// Go `GetStore`.
    #[must_use]
    pub fn get_store(&self, store_id: u64) -> Option<metapb::Store> {
        self.rm
            .get_all_stores()
            .into_iter()
            .find(|store| store.id == store_id)
    }

    /// Go `GetRegion`: the region holding the encoded `key` and its leader.
    #[must_use]
    pub fn get_region(&self, key: &[u8]) -> Option<(metapb::Region, metapb::Peer)> {
        self.rm.get_region_by_key(key)
    }

    /// Go `GetRegionByID`.
    #[must_use]
    pub fn get_region_by_id(&self, region_id: u64) -> Option<(metapb::Region, metapb::Peer)> {
        self.rm.get_region(region_id).map(|region| {
            let leader = region.peers[0].clone();
            (region, leader)
        })
    }

    /// Go `GetPrevRegion`: the region whose `(start, end]` holds the encoded
    /// `key`.
    #[must_use]
    pub fn get_prev_region(&self, key: &[u8]) -> Option<(metapb::Region, metapb::Peer)> {
        self.rm.get_region_by_end_key(key)
    }

    /// Go `GetGCSafePoint`: the null keyspace's GC safe point.
    #[must_use]
    pub fn get_gc_safe_point(&self) -> u64 {
        self.gc_states
            .gc_states_client(NULL_KEYSPACE_ID)
            .get_gc_state(true)
            .gc_safe_point
    }

    /// Go `UpdateGCSafePoint`: advances the null keyspace's GC safe point and
    /// answers the new one; a smaller target fails silently with the current
    /// value.
    pub fn update_gc_safe_point(&self, target: u64) -> Result<u64, GcStatesError> {
        match self
            .gc_states
            .gc_internal_controller(NULL_KEYSPACE_ID)
            .advance_gc_safe_point(target)
        {
            Ok(res) => Ok(res.new_gc_safe_point),
            Err(GcStatesError::DecreasingGcSafePoint { current, .. }) => Ok(current),
            Err(error) => Err(error),
        }
    }

    /// Go `GetExternalTimestamp`.
    #[must_use]
    pub fn get_external_timestamp(&self) -> u64 {
        self.external_timestamp.load(Ordering::Acquire)
    }

    /// Go `SetExternalTimestamp`: never past the current TSO, never
    /// backward.
    pub fn set_external_timestamp(&self, new_timestamp: u64) -> Result<(), String> {
        let current_tso = self.tso.get_composed_ts();
        if new_timestamp > current_tso {
            return Err("external timestamp is greater than global tso".to_owned());
        }
        loop {
            let external = self.external_timestamp.load(Ordering::Acquire);
            if external > new_timestamp {
                return Err("cannot decrease the external timestamp".to_owned());
            }
            if self
                .external_timestamp
                .compare_exchange(external, new_timestamp, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    /// Go `GetTS`: `(physical, logical)`.
    #[must_use]
    pub fn get_ts(&self) -> (i64, i64) {
        self.tso.get_ts()
    }

    /// The oracle `GetTS` draws from, shared with the store.
    #[must_use]
    pub fn oracle(&self) -> &Arc<Tso> {
        &self.tso
    }

    /// Go `GetAllStores`.
    #[must_use]
    pub fn get_all_stores(&self) -> Vec<metapb::Store> {
        self.rm.get_all_stores()
    }

    /// Go `ScanRegions`.
    #[must_use]
    pub fn scan_regions(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Vec<(metapb::Region, metapb::Peer)> {
        self.rm.scan_regions(start_key, end_key, limit)
    }

    /// Go `BatchScanRegions`: the regions of each encoded `(start, end)`
    /// range in turn, a range's part already covered by the last region
    /// skipped, at most `limit` regions in all.
    #[must_use]
    pub fn batch_scan_regions(
        &self,
        key_ranges: &[(Vec<u8>, Vec<u8>)],
        mut limit: i64,
    ) -> Vec<(metapb::Region, metapb::Peer)> {
        let mut regions: Vec<(metapb::Region, metapb::Peer)> = Vec::with_capacity(key_ranges.len());
        for (start, end) in key_ranges {
            let mut start = start.clone();
            if let Some((last, _)) = regions.last() {
                let last_end = &last.end_key;
                if last_end.is_empty() {
                    return regions;
                }
                if last_end >= end {
                    continue;
                } else if *last_end > start {
                    start = last_end.clone();
                }
            }
            let range_regions =
                self.rm
                    .scan_regions(&start, end, usize::try_from(limit).unwrap_or(0));
            if range_regions.is_empty() {
                continue;
            }
            limit -= i64::try_from(range_regions.len()).unwrap_or(i64::MAX);
            regions.extend(range_regions);
            if limit <= 0 {
                break;
            }
        }
        regions
    }

    /// Go `ScatterRegion`.
    pub fn scatter_region(&self, _region_id: u64) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestMockGCStatesManager` (`tikv/mock_pd_test.go`), whole.
    #[test]
    fn test_mock_gc_states_manager() {
        let m = GcStatesManagerSimulator::new();
        let never = Duration::MAX;
        for keyspace_id in [NULL_KEYSPACE_ID, 0, 1] {
            let ctl = m.gc_internal_controller(keyspace_id);
            let cli = m.gc_states_client(keyspace_id);

            let s = cli.get_gc_state(false);
            assert_eq!((s.gc_safe_point, s.txn_safe_point), (0, 0));
            assert_eq!(s.gc_barriers, Some(Vec::new()));

            // Test excluding GC barriers
            let s = cli.get_gc_state(true);
            assert_eq!((s.gc_safe_point, s.txn_safe_point), (0, 0));
            assert!(s.gc_barriers.is_none());

            let tsp_res = ctl.advance_txn_safe_point(10).unwrap();
            assert_eq!(
                tsp_res,
                AdvanceTxnSafePointResult {
                    old_txn_safe_point: 0,
                    target: 10,
                    new_txn_safe_point: 10,
                    blocker_description: String::new(),
                }
            );
            let gsp_res = ctl.advance_gc_safe_point(9).unwrap();
            assert_eq!(
                gsp_res,
                AdvanceGcSafePointResult {
                    old_gc_safe_point: 0,
                    target: 9,
                    new_gc_safe_point: 9,
                }
            );

            // No decreasing
            assert!(ctl.advance_txn_safe_point(9).is_err());
            assert!(ctl.advance_gc_safe_point(8).is_err());
            assert!(ctl.advance_gc_safe_point(11).is_err());

            let s = cli.get_gc_state(false);
            assert_eq!((s.gc_safe_point, s.txn_safe_point), (9, 10));
            assert_eq!(s.gc_barriers, Some(Vec::new()));

            // Invalid arguments
            assert!(cli.set_gc_barrier("", 20, never).is_err());
            assert!(cli.set_gc_barrier("b1", 0, never).is_err());
            assert!(cli.set_gc_barrier("b1", 20, Duration::ZERO).is_err());

            // Disallow being less than txn safe point
            assert!(cli.set_gc_barrier("b1", 9, never).is_err());

            let b = cli.set_gc_barrier("b2", 25, never).unwrap();
            assert_eq!((b.barrier_id.as_str(), b.barrier_ts), ("b2", 25));

            let s = cli.get_gc_state(false);
            assert_eq!((s.gc_safe_point, s.txn_safe_point), (9, 10));
            let barriers = s.gc_barriers.unwrap();
            assert_eq!(barriers.len(), 1);
            assert_eq!(
                (barriers[0].barrier_id.as_str(), barriers[0].barrier_ts),
                ("b2", 25)
            );

            // Test excluding GC barriers
            let s = cli.get_gc_state(true);
            assert_eq!((s.gc_safe_point, s.txn_safe_point), (9, 10));
            assert!(s.gc_barriers.is_none());

            let tsp_res = ctl.advance_txn_safe_point(30).unwrap();
            assert_eq!(tsp_res.new_txn_safe_point, 25);
            assert_eq!(tsp_res.target, 30);
            assert_eq!(tsp_res.old_txn_safe_point, 10);
            assert!(tsp_res.blocker_description.contains("b2"));

            let s = cli.get_gc_state(false);
            assert_eq!(s.txn_safe_point, 25);

            let b = cli.delete_gc_barrier("b2").unwrap();
            assert_eq!((b.barrier_id.as_str(), b.barrier_ts), ("b2", 25));

            let tsp_res = ctl.advance_txn_safe_point(30).unwrap();
            assert_eq!(
                tsp_res,
                AdvanceTxnSafePointResult {
                    old_txn_safe_point: 25,
                    target: 30,
                    new_txn_safe_point: 30,
                    blocker_description: String::new(),
                }
            );

            let s = cli.get_gc_state(false);
            assert_eq!((s.gc_safe_point, s.txn_safe_point), (9, 30));
            assert_eq!(s.gc_barriers, Some(Vec::new()));
        }
    }

    /// Go's blocker description: `%+q` quoting of the barrier id.
    #[test]
    fn the_blocker_description_quotes_the_barrier_id_as_go_does() {
        let m = GcStatesManagerSimulator::new();
        m.gc_states_client(NULL_KEYSPACE_ID)
            .set_gc_barrier("b\"é", 5, Duration::MAX)
            .unwrap();
        let res = m
            .gc_internal_controller(NULL_KEYSPACE_ID)
            .advance_txn_safe_point(9)
            .unwrap();
        assert_eq!(
            res.blocker_description,
            "GCBarrier { BarrierID: \"b\\\"\\u00e9\", BarrierTS: 5, ExpirationTime: <nil> }"
        );
    }

    /// Go `UpdateServiceGCSafePoint`: "gc_worker" advances the txn safe
    /// point; other services set or delete barriers; the answer is the txn
    /// safe point.
    #[test]
    fn update_service_gc_safe_point_is_pds_compatibility_wrapper() {
        let m = GcStatesManagerSimulator::new();
        assert_eq!(
            m.update_service_gc_safe_point("gc_worker", i64::MAX, 10),
            Ok(10)
        );
        assert_eq!(
            m.update_service_gc_safe_point("gc_worker", 100, 20),
            Err(GcStatesError::GcWorkerTtl)
        );
        assert_eq!(m.update_service_gc_safe_point("br", 100, 15), Ok(10));
        assert_eq!(
            m.update_service_gc_safe_point("gc_worker", i64::MAX, 20),
            Ok(15)
        );
        assert!(matches!(
            m.update_service_gc_safe_point("cdc", 100, 5),
            Err(GcStatesError::GcBarrierTsBehindTxnSafePoint { .. })
        ));
        assert_eq!(m.update_service_gc_safe_point("br", 0, 0), Ok(15));
        assert_eq!(
            m.update_service_gc_safe_point("gc_worker", i64::MAX, 20),
            Ok(20)
        );
    }

    /// Go `MockPD.UpdateGCSafePoint`: a smaller target answers the current
    /// value without an error.
    #[test]
    fn update_gc_safe_point_fails_silently_on_a_smaller_target() {
        let pd = MockPd::new(
            Arc::new(InProcessRegionManager::new()),
            Arc::new(Tso::new()),
        );
        pd.gc_states()
            .gc_internal_controller(NULL_KEYSPACE_ID)
            .advance_txn_safe_point(100)
            .unwrap();
        assert_eq!(pd.update_gc_safe_point(50), Ok(50));
        assert_eq!(pd.update_gc_safe_point(40), Ok(50));
        assert_eq!(pd.get_gc_safe_point(), 50);
        assert!(pd.update_gc_safe_point(200).is_err());
    }
}
