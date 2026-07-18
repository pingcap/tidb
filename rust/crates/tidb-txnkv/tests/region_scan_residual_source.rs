// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Direct transition of client-go ScanRegions start, limit, filtering, and
//! leader-only retry semantics.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tidb_txnkv::region::{
    KeyRange, RegionCache, RegionLoadError, RegionLoader, RegionLocation, RegionQuery,
    RegionQueryBackoff, RegionQueryLoader, RegionQueryOptions, RegionQueryRetryReason,
    RegionQueryRoute, RegionRouteError, RegionVerId, StoreMetadata,
};

type ScanCall = (KeyRange, usize, RegionQueryOptions);

struct Loader {
    replies: VecDeque<Vec<RegionLocation>>,
    calls: Arc<Mutex<Vec<ScanCall>>>,
}

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-unary",
            "scan test requires the request-shaped path",
        ))
    }
}

impl RegionQueryLoader for Loader {
    fn query_region(
        &mut self,
        _query: RegionQuery<'_>,
        _options: RegionQueryOptions,
    ) -> Result<RegionLocation, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-query",
            "scan test requires ScanRegions",
        ))
    }

    fn scan_regions_once(
        &mut self,
        range: &KeyRange,
        limit: usize,
        options: RegionQueryOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        self.calls
            .lock()
            .unwrap()
            .push((range.clone(), limit, options));
        self.replies
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("missing-reply", "scan script exhausted"))
    }

    fn load_store(&mut self, _store_id: u64) -> Result<Option<StoreMetadata>, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-store",
            "scan test does not resolve stores",
        ))
    }
}

#[derive(Default)]
struct Backoff {
    reasons: Vec<RegionQueryRetryReason>,
}

impl RegionQueryBackoff for Backoff {
    fn backoff(&mut self, reason: RegionQueryRetryReason) -> Result<(), RegionRouteError> {
        self.reasons.push(reason);
        Ok(())
    }
}

fn region(id: u64, start: &[u8], end: &[u8], has_leader: bool) -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(id, 1, 1),
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        leader_peer_id: has_leader.then_some(id * 10),
        ..RegionLocation::default()
    }
}

fn cache(replies: Vec<Vec<RegionLocation>>) -> (RegionCache<Loader>, Arc<Mutex<Vec<ScanCall>>>) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    (
        RegionCache::new(Loader {
            replies: replies.into(),
            calls: Arc::clone(&calls),
        }),
        calls,
    )
}

#[test]
fn zero_limit_is_empty_without_a_pd_attempt() {
    let (mut cache, calls) = cache(Vec::new());
    let regions = cache
        .scan_regions(&KeyRange::new(b"a", b"z"), 0, &mut Backoff::default())
        .unwrap();
    assert!(regions.is_empty());
    assert!(calls.lock().unwrap().is_empty());
}

#[test]
fn scan_starts_with_the_region_containing_start_and_preserves_limit() {
    let (mut cache, calls) = cache(vec![vec![
        region(1, b"a", b"b", true),
        region(2, b"b", b"d", true),
    ]]);
    let range = KeyRange::new(b"a1", b"d");
    let regions = cache
        .scan_regions(&range, 2, &mut Backoff::default())
        .unwrap();
    assert_eq!(
        regions.iter().map(|r| r.region.id).collect::<Vec<_>>(),
        [1, 2]
    );
    let calls = calls.lock().unwrap();
    assert_eq!(calls[0].0, range);
    assert_eq!(calls[0].1, 2);
}

#[test]
fn mixed_leaderless_regions_are_filtered_after_raw_coverage_validation() {
    let (mut cache, _) = cache(vec![vec![
        region(1, b"", b"a", true),
        region(2, b"a", b"b", false),
        region(3, b"b", b"c", true),
    ]]);
    let mut backoff = Backoff::default();
    let regions = cache
        .scan_regions(&KeyRange::new(b"", b"c"), 3, &mut backoff)
        .unwrap();
    assert_eq!(
        regions.iter().map(|r| r.region.id).collect::<Vec<_>>(),
        [1, 3]
    );
    assert!(backoff.reasons.is_empty());
}

#[test]
fn empty_and_all_leaderless_retries_are_pd_leader_only() {
    let (mut cache, calls) = cache(vec![
        Vec::new(),
        vec![region(1, b"a", b"z", false)],
        vec![region(1, b"a", b"z", true)],
    ]);
    let mut backoff = Backoff::default();
    let regions = cache
        .scan_regions(&KeyRange::new(b"a", b"z"), 1, &mut backoff)
        .unwrap();
    assert_eq!(regions[0].region.id, 1);
    assert_eq!(
        backoff.reasons,
        [
            RegionQueryRetryReason::EmptyReply,
            RegionQueryRetryReason::MissingLeader,
        ]
    );
    let calls = calls.lock().unwrap();
    assert_eq!(calls[0].2.route, RegionQueryRoute::AllowFollowerOrRouter);
    for call in &calls[1..] {
        assert_eq!(call.2.route, RegionQueryRoute::LeaderOnly);
    }
}
