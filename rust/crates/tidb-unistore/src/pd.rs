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

//! Go `pkg/store/mockstore/unistore/pd.go`: the PD client over the embedded
//! store's [`MockPd`], with the keyspace manager the cluster is configured
//! with. Go's is client-go's `pd.Client`; here it is client-rust's
//! [`RetryClientTrait`], under which client-rust's own [`PdRpcClient`]
//! keeps the region cache, store clients and routing, as client-go's
//! `KVStore` keeps them over Go's.
//!
//! Regions are answered as PD holds them, with memcomparable-encoded bounds;
//! client-rust's codec PD client encodes the keys it asks with and decodes
//! the answers, as client-go's `CodecPDClient` does.
//!
//! [`PdRpcClient`]: tikv_client::PdRpcClient
//!
//! # Narrowings, by name
//!
//! * Global config, member listing, service discovery, resource groups and
//!   meta storage: client-rust's PD trait has no such calls.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tikv_client::proto::keyspacepb::{self, keyspace_meta, KeyspaceMeta};
use tikv_client::proto::{metapb, pdpb};
use tikv_client::{
    Error, RegionId, RegionScanOptions, RegionWithLeader, Result, RetryClientTrait, StoreId,
    Timestamp,
};

use tidb_unistore_tikv::mock_pd::{GcState, GcStatesError, MockPd};

/// PD's `constants.MaxKeyspaceID`.
pub const MAX_KEYSPACE_ID: u32 = 0x00FF_FFFF;

/// Go `keyspace.GetId()`: the v1/v2 id, zero otherwise.
fn keyspace_id(meta: &KeyspaceMeta) -> u32 {
    match meta.keyspace {
        Some(keyspace_meta::Keyspace::Id(id)) => id,
        _ => 0,
    }
}

/// Go `mockKeyspaceManager`: the cluster's keyspaces, sorted by id.
#[derive(Debug, Default)]
pub struct MockKeyspaceManager {
    keyspaces: Vec<KeyspaceMeta>,
    keyspace_names_map: std::collections::HashMap<String, u32>,
}

impl MockKeyspaceManager {
    /// Go `newMockKeyspaceManager`: ids must be valid (at most
    /// `MaxKeyspaceID`) and distinct, names distinct.
    pub fn new(mut keyspaces: Vec<KeyspaceMeta>) -> std::result::Result<Self, String> {
        keyspaces.sort_by_key(keyspace_id);
        let mut keyspace_names_map = std::collections::HashMap::with_capacity(keyspaces.len());
        for (i, keyspace) in keyspaces.iter().enumerate() {
            let id = keyspace_id(keyspace);
            if id > MAX_KEYSPACE_ID {
                return Err(format!(
                    "invalid keyspace ID (note that null keyspace won't have meta), got keyspace meta: {keyspace:?}"
                ));
            }
            if i > 0 && id == keyspace_id(&keyspaces[i - 1]) {
                return Err(format!(
                    "keyspace ID {id} duplicated: keyspace meta {keyspace:?}, keyspace meta {:?}",
                    keyspaces[i - 1]
                ));
            }
            if keyspace_names_map.contains_key(&keyspace.name) {
                return Err(format!(
                    "keyspace name {} duplicated: keyspace meta {keyspace:?}",
                    keyspace.name
                ));
            }
            keyspace_names_map.insert(keyspace.name.clone(), id);
        }
        Ok(Self {
            keyspaces,
            keyspace_names_map,
        })
    }

    /// The position of keyspace `id`, or where it would be.
    fn search(&self, id: u32) -> std::result::Result<usize, usize> {
        self.keyspaces.binary_search_by_key(&id, keyspace_id)
    }

    /// Go `LoadKeyspace`.
    pub fn load_keyspace(&self, name: &str) -> std::result::Result<KeyspaceMeta, String> {
        match self.keyspace_names_map.get(name) {
            Some(&id) => {
                let index = self.search(id).unwrap_or_else(|_| {
                    panic!("keyspace meta list and name map mismatches, id: {id}")
                });
                Ok(self.keyspaces[index].clone())
            }
            None => Err(pdpb::ErrorType::EntryNotFound.as_str_name().to_owned()),
        }
    }

    /// Go `LoadKeyspaceByID`.
    pub fn load_keyspace_by_id(&self, id: u32) -> std::result::Result<KeyspaceMeta, String> {
        self.search(id)
            .map(|index| self.keyspaces[index].clone())
            .map_err(|_| pdpb::ErrorType::EntryNotFound.as_str_name().to_owned())
    }

    /// Go `GetAllKeyspaces`: from the first keyspace at or after `start_id`,
    /// at most `limit` (zero: all the rest).
    #[must_use]
    pub fn get_all_keyspaces(&self, start_id: u32, limit: u32) -> Vec<KeyspaceMeta> {
        let start = self.search(start_id).unwrap_or_else(|index| index);
        let rest = &self.keyspaces[start..];
        let limit = if limit == 0 {
            rest.len()
        } else {
            usize::try_from(limit).unwrap_or(usize::MAX)
        };
        rest.iter().take(limit).cloned().collect()
    }
}

/// Go `pdClient`: the PD client over the embedded store's mock PD.
#[derive(Debug)]
pub struct PdClient {
    mock_pd: Arc<MockPd>,
    keyspaces: MockKeyspaceManager,
    external_timestamp: AtomicU64,
}

/// A GC-state error as client-rust carries PD's: the message, Go's text.
fn gc_error(error: GcStatesError) -> Error {
    Error::StringError(error.to_string())
}

fn region_with_leader((region, leader): (metapb::Region, metapb::Peer)) -> RegionWithLeader {
    RegionWithLeader::new(region, Some(leader))
}

impl PdClient {
    /// Go `newPDClient`.
    pub fn new(
        mock_pd: Arc<MockPd>,
        cluster_keyspaces: Vec<KeyspaceMeta>,
    ) -> std::result::Result<Self, String> {
        let keyspaces = MockKeyspaceManager::new(cluster_keyspaces)
            .map_err(|error| format!("failed to create mock keyspace manager, err: {error}"))?;
        Ok(Self {
            mock_pd,
            keyspaces,
            external_timestamp: AtomicU64::new(0),
        })
    }

    /// The mock PD beneath.
    #[must_use]
    pub fn mock_pd(&self) -> &Arc<MockPd> {
        &self.mock_pd
    }

    fn gc_state(&self, keyspace_id: u32) -> GcState {
        self.mock_pd
            .gc_states()
            .gc_states_client(keyspace_id)
            .get_gc_state(false)
    }
}

#[async_trait]
impl RetryClientTrait for PdClient {
    /// Go `GetClusterID`.
    async fn cluster_id(&self) -> u64 {
        self.mock_pd.get_cluster_id()
    }

    /// Go `GetRegion`.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        self.mock_pd
            .get_region(&key)
            .map(region_with_leader)
            .ok_or_else(|| Error::StringError(format!("region not found for key {key:?}")))
    }

    /// Go `GetPrevRegion`; an empty key is the last region.
    async fn get_prev_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        let found = if key.is_empty() {
            self.mock_pd.scan_regions(b"", b"", 0).pop()
        } else {
            self.mock_pd.get_prev_region(&key)
        };
        found
            .map(region_with_leader)
            .ok_or_else(|| Error::StringError(format!("region not found for end key {key:?}")))
    }

    /// Go `GetRegionByID`.
    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader> {
        self.mock_pd
            .get_region_by_id(region_id)
            .map(region_with_leader)
            .ok_or(Error::RegionNotFoundInResponse { region_id })
    }

    /// Go `ScanRegions`.
    async fn scan_regions(
        self: Arc<Self>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<RegionWithLeader>> {
        Ok(self
            .mock_pd
            .scan_regions(&start_key, &end_key, limit)
            .into_iter()
            .map(region_with_leader)
            .collect())
    }

    /// Go `BatchScanRegions`: unistore's `pdClient` does not override it, so
    /// client-go falls back to scanning, as client-rust does on
    /// `Unimplemented`.
    async fn batch_scan_regions(
        self: Arc<Self>,
        _ranges: Vec<pdpb::KeyRange>,
        _limit: usize,
        _options: RegionScanOptions,
    ) -> Result<Vec<RegionWithLeader>> {
        Err(Error::Unimplemented)
    }

    /// Go `GetStore`.
    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<Option<metapb::Store>> {
        Ok(self.mock_pd.get_store(id))
    }

    /// Go `GetAllStores`.
    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
        Ok(self.mock_pd.get_all_stores())
    }

    /// Go `GetTS`.
    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        let (physical, logical) = self.mock_pd.get_ts();
        Ok(Timestamp {
            physical,
            logical,
            ..Timestamp::default()
        })
    }

    /// Go `GetMinTS`: zero.
    async fn get_min_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        Ok(Timestamp::default())
    }

    /// Go `pdClient.SetExternalTimestamp`: never past the current TSO, never
    /// backward; setting the same value again succeeds.
    async fn set_external_timestamp(self: Arc<Self>, new_timestamp: u64) -> Result<()> {
        let current_tso = self.mock_pd.oracle().get_composed_ts();
        if new_timestamp > current_tso {
            return Err(Error::StringError(
                "external timestamp is greater than global tso".to_owned(),
            ));
        }
        loop {
            let external = self.external_timestamp.load(Ordering::Acquire);
            if external > new_timestamp {
                return Err(Error::StringError(
                    "cannot decrease the external timestamp".to_owned(),
                ));
            } else if external == new_timestamp {
                return Ok(());
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

    /// Go `pdClient.GetExternalTimestamp`.
    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        Ok(self.external_timestamp.load(Ordering::Acquire))
    }

    /// Go `UpdateGCSafePoint`: whether the safe point became `safe_point`.
    async fn update_safepoint(self: Arc<Self>, safe_point: u64) -> Result<bool> {
        Ok(self
            .mock_pd
            .update_gc_safe_point(safe_point)
            .map_err(gc_error)?
            == safe_point)
    }

    /// Go `UpdateGCSafePoint`: the safe point after the update.
    async fn update_safepoint_value(self: Arc<Self>, safe_point: u64) -> Result<u64> {
        self.mock_pd
            .update_gc_safe_point(safe_point)
            .map_err(gc_error)
    }

    /// Go `UpdateServiceGCSafePoint`, the simulator's compatibility wrapper.
    async fn update_service_gc_safe_point(
        self: Arc<Self>,
        service_id: String,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        self.mock_pd
            .gc_states()
            .update_service_gc_safe_point(&service_id, ttl, safe_point)
            .map_err(gc_error)
    }

    /// Go `GetAllKeyspaces`.
    async fn get_all_keyspaces(&self, start_id: u32, limit: u32) -> Result<Vec<KeyspaceMeta>> {
        Ok(self.keyspaces.get_all_keyspaces(start_id, limit))
    }

    /// Go `GetGCStatesClient(keyspace).GetGCState`, barriers included.
    async fn get_gc_state(self: Arc<Self>, keyspace_id: u32) -> Result<pdpb::GetGcStateResponse> {
        let state = self.gc_state(keyspace_id);
        Ok(pdpb::GetGcStateResponse {
            header: None,
            gc_state: Some(pdpb::GcState {
                keyspace_scope: Some(pdpb::KeyspaceScope {
                    keyspace: Some(pdpb::keyspace_scope::Keyspace::KeyspaceId(
                        state.keyspace_id,
                    )),
                }),
                // The simulator's `NewGCStateWithGCBarriers` never sets it.
                is_keyspace_level_gc: false,
                txn_safe_point: state.txn_safe_point,
                gc_safe_point: state.gc_safe_point,
                gc_barriers: state
                    .gc_barriers
                    .unwrap_or_default()
                    .into_iter()
                    .map(|barrier| pdpb::GcBarrierInfo {
                        barrier_id: barrier.barrier_id,
                        barrier_ts: barrier.barrier_ts,
                        // `TTLNeverExpire`, which PD's client saturates back.
                        ttl_seconds: i64::MAX,
                    })
                    .collect(),
            }),
            // The simulator keeps no global barriers.
            global_gc_barriers: None,
        })
    }

    /// Go `GetGCInternalController(keyspace).AdvanceTxnSafePoint`.
    async fn advance_txn_safe_point(
        self: Arc<Self>,
        keyspace_id: u32,
        target: u64,
    ) -> Result<pdpb::AdvanceTxnSafePointResponse> {
        let res = self
            .mock_pd
            .gc_states()
            .gc_internal_controller(keyspace_id)
            .advance_txn_safe_point(target)
            .map_err(gc_error)?;
        Ok(pdpb::AdvanceTxnSafePointResponse {
            header: None,
            old_txn_safe_point: res.old_txn_safe_point,
            new_txn_safe_point: res.new_txn_safe_point,
            blocker_description: res.blocker_description,
        })
    }

    /// Go `GetGCInternalController(keyspace).AdvanceGCSafePoint`.
    async fn advance_gc_safe_point(
        self: Arc<Self>,
        keyspace_id: u32,
        target: u64,
    ) -> Result<pdpb::AdvanceGcSafePointResponse> {
        let res = self
            .mock_pd
            .gc_states()
            .gc_internal_controller(keyspace_id)
            .advance_gc_safe_point(target)
            .map_err(gc_error)?;
        Ok(pdpb::AdvanceGcSafePointResponse {
            header: None,
            old_gc_safe_point: res.old_gc_safe_point,
            new_gc_safe_point: res.new_gc_safe_point,
        })
    }

    /// Go `ScatterRegions`: nothing to scatter.
    async fn scatter_regions(
        self: Arc<Self>,
        _region_ids: Vec<u64>,
        _group: String,
    ) -> Result<pdpb::ScatterRegionResponse> {
        Ok(pdpb::ScatterRegionResponse::default())
    }

    /// Go `GetOperator`: every operator has succeeded.
    async fn get_operator(self: Arc<Self>, _region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        Ok(pdpb::GetOperatorResponse {
            status: pdpb::OperatorStatus::Success as i32,
            ..pdpb::GetOperatorResponse::default()
        })
    }

    /// Go `SplitRegions`: no regions.
    async fn split_regions(
        self: Arc<Self>,
        _split_keys: Vec<Vec<u8>>,
        _retry_limit: u64,
    ) -> Result<pdpb::SplitRegionsResponse> {
        Ok(pdpb::SplitRegionsResponse::default())
    }

    /// Go `LoadKeyspace`.
    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        self.keyspaces
            .load_keyspace(keyspace)
            .map_err(Error::StringError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_unistore_tikv::mock_pd::NULL_KEYSPACE_ID;

    fn keyspace_meta(id: u32, name: &str) -> KeyspaceMeta {
        KeyspaceMeta {
            keyspace: Some(keyspace_meta::Keyspace::Id(id)),
            name: name.to_owned(),
            ..KeyspaceMeta::default()
        }
    }

    fn check_elements(m: &MockKeyspaceManager, ids: &[u32], names: &[&str]) {
        assert_eq!(ids.len(), names.len());
        assert_eq!(ids.len(), m.keyspaces.len());
        assert_eq!(ids.len(), m.keyspace_names_map.len());
        for (i, keyspace) in m.keyspaces.iter().enumerate() {
            if i > 0 {
                assert!(keyspace_id(keyspace) > keyspace_id(&m.keyspaces[i - 1]));
            }
            assert_eq!(ids[i], keyspace_id(keyspace));
            assert_eq!(names[i], keyspace.name);
            assert_eq!(m.keyspace_names_map[&keyspace.name], keyspace_id(keyspace));
        }
    }

    fn must_load_keyspace(m: &MockKeyspaceManager, name: &str, expected: Option<u32>) {
        match (m.load_keyspace(name), expected) {
            (Ok(meta), Some(id)) => {
                assert_eq!(meta.name, name);
                assert_eq!(keyspace_id(&meta), id);
            }
            (Err(error), None) => assert!(error.contains("ENTRY_NOT_FOUND"), "{error}"),
            (got, want) => panic!("load {name}: got {got:?}, want {want:?}"),
        }
    }

    fn must_list_keyspaces(
        m: &MockKeyspaceManager,
        start_id: u32,
        limit: u32,
        ids: &[u32],
        names: &[&str],
    ) {
        let keyspaces = m.get_all_keyspaces(start_id, limit);
        let got: Vec<_> = keyspaces
            .iter()
            .map(|keyspace| (keyspace_id(keyspace), keyspace.name.as_str()))
            .collect();
        let want: Vec<_> = ids.iter().copied().zip(names.iter().copied()).collect();
        assert_eq!(got, want);
    }

    /// Go `TestMockKeyspaceManager` (`unistore/pd_test.go`), whole.
    #[test]
    fn test_mock_keyspace_manager() {
        let m = MockKeyspaceManager::new(Vec::new()).unwrap();
        check_elements(&m, &[], &[]);
        must_load_keyspace(&m, "DEFAULT", None);
        must_list_keyspaces(&m, 0, 0, &[], &[]);

        let m = MockKeyspaceManager::new(vec![keyspace_meta(0, "DEFAULT")]).unwrap();
        check_elements(&m, &[0], &["DEFAULT"]);
        must_load_keyspace(&m, "DEFAULT", Some(0));
        must_load_keyspace(&m, "ks1", None);
        must_list_keyspaces(&m, 0, 0, &[0], &["DEFAULT"]);
        must_list_keyspaces(&m, 1, 0, &[], &[]);

        let m = MockKeyspaceManager::new(vec![
            keyspace_meta(1, "ks1"),
            keyspace_meta(4, "ks4"),
            keyspace_meta(2, "ks2"),
            keyspace_meta(5, "ks5"),
            keyspace_meta(3, "ks3"),
        ])
        .unwrap();
        let all = ["ks1", "ks2", "ks3", "ks4", "ks5"];
        check_elements(&m, &[1, 2, 3, 4, 5], &all);
        for (i, name) in all.iter().enumerate() {
            must_load_keyspace(&m, name, Some(u32::try_from(i).unwrap() + 1));
        }
        must_load_keyspace(&m, "ks6", None);
        must_list_keyspaces(&m, 0, 0, &[1, 2, 3, 4, 5], &all);
        must_list_keyspaces(&m, 0, 3, &[1, 2, 3], &["ks1", "ks2", "ks3"]);
        must_list_keyspaces(&m, 1, 0, &[1, 2, 3, 4, 5], &all);
        must_list_keyspaces(&m, 3, 0, &[3, 4, 5], &["ks3", "ks4", "ks5"]);
        must_list_keyspaces(&m, 3, 2, &[3, 4], &["ks3", "ks4"]);
        must_list_keyspaces(&m, 5, 0, &[5], &["ks5"]);

        let m = MockKeyspaceManager::new(vec![
            keyspace_meta(100, "ks100"),
            keyspace_meta(1, "ks1"),
            keyspace_meta(MAX_KEYSPACE_ID, "lastks"),
            keyspace_meta(10, "ks10"),
        ])
        .unwrap();
        let ids = [1, 10, 100, MAX_KEYSPACE_ID];
        let names = ["ks1", "ks10", "ks100", "lastks"];
        check_elements(&m, &ids, &names);
        must_list_keyspaces(&m, 0, 0, &ids, &names);
        must_list_keyspaces(&m, 5, 0, &ids[1..], &names[1..]);
        must_list_keyspaces(&m, 5, 1, &[10], &["ks10"]);
        must_list_keyspaces(&m, 10, 0, &ids[1..], &names[1..]);
        must_list_keyspaces(&m, 11, 0, &ids[2..], &names[2..]);
        must_list_keyspaces(&m, 99, 0, &ids[2..], &names[2..]);
        must_list_keyspaces(&m, 101, 0, &ids[3..], &names[3..]);
        must_list_keyspaces(&m, MAX_KEYSPACE_ID, 0, &ids[3..], &names[3..]);

        // Rejects duplicated ID.
        assert!(MockKeyspaceManager::new(vec![
            keyspace_meta(1, "ks1"),
            keyspace_meta(2, "ks2"),
            keyspace_meta(3, "ks3"),
            keyspace_meta(1, "ks4"),
        ])
        .is_err());
        // Rejects duplicated name.
        assert!(MockKeyspaceManager::new(vec![
            keyspace_meta(1, "ks1"),
            keyspace_meta(2, "ks2"),
            keyspace_meta(3, "ks3"),
            keyspace_meta(4, "ks1"),
        ])
        .is_err());
        // Exceeds the current value of max allowed keyspace id
        assert!(MockKeyspaceManager::new(vec![keyspace_meta(0x0100_0000, "illegal")]).is_err());
        // Null keyspace id is not allowed
        assert!(MockKeyspaceManager::new(vec![keyspace_meta(NULL_KEYSPACE_ID, "")]).is_err());
    }
}
