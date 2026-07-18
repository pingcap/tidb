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

#![allow(missing_docs)]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use tidb_codec::encode_bytes;
use tidb_proto::{errorpb, metapb};
use tidb_txnkv::region::{
    Peer, PeerRole, RegionAttempt, RegionBackoffBudget, RegionBackoffKind, RegionCache,
    RegionErrorDisposition, RegionLoadError, RegionLoader, RegionLocation, RegionMetadata,
    RegionRebuildAction, RegionRecoveryError, RegionRecoveryLoader, RegionTerminalError,
    RegionVerId, ReplicaReadMode, SelectorRecovery, Store, StoreLiveness,
};

type RecordedMetadata = Rc<RefCell<Vec<(RegionMetadata, u64)>>>;

struct Loader {
    initial: VecDeque<RegionLocation>,
    hydrated: VecDeque<Result<RegionLocation, RegionLoadError>>,
    metadata: RecordedMetadata,
}

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.initial
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("empty", "no initial region"))
    }
}

impl RegionRecoveryLoader for Loader {
    fn hydrate_region(
        &mut self,
        metadata: &RegionMetadata,
        leader_store_id: u64,
    ) -> Result<RegionLocation, RegionLoadError> {
        self.metadata
            .borrow_mut()
            .push((metadata.clone(), leader_store_id));
        let hydrated = self
            .hydrated
            .pop_front()
            .unwrap_or_else(|| Err(RegionLoadError::new("empty", "no hydrated region")))?;
        let expected_start = encoded_boundary(&hydrated.start_key);
        let expected_end = encoded_boundary(&hydrated.end_key);
        if hydrated.region != metadata.region
            || expected_start != metadata.encoded_start_key
            || expected_end != metadata.encoded_end_key
        {
            return Err(RegionLoadError::new(
                "dishonest_hydration",
                "scripted hydrated region disagrees with TiKV metadata",
            ));
        }
        Ok(hydrated)
    }
}

fn location(id: u64, conf_ver: u64, version: u64, start: &[u8], end: &[u8]) -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(id, conf_ver, version),
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        peers: vec![
            Peer {
                id: 11,
                store_id: 101,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 7,
            },
            Peer {
                id: 12,
                store_id: 102,
                role: PeerRole::Unknown(99),
                is_witness: false,
                store_epoch: 9,
            },
        ],
        leader_peer_id: Some(11),
        stores: vec![
            Store {
                id: 101,
                address: "store-101".to_owned(),
                epoch: 7,
            },
            Store {
                id: 102,
                address: "store-102".to_owned(),
                epoch: 9,
            },
        ],
        ..RegionLocation::default()
    }
}

fn cache(
    first: RegionLocation,
    hydrated: impl IntoIterator<Item = Result<RegionLocation, RegionLoadError>>,
) -> (RegionCache<Loader>, RecordedMetadata) {
    let metadata = Rc::new(RefCell::new(Vec::new()));
    let loader = Loader {
        initial: VecDeque::from([first]),
        hydrated: hydrated.into_iter().collect(),
        metadata: Rc::clone(&metadata),
    };
    (RegionCache::new(loader), metadata)
}

fn attempt(region: RegionVerId) -> RegionAttempt {
    RegionAttempt {
        region,
        peer_id: 11,
        store_id: 101,
        address: "store-101".to_owned(),
        store_epoch: 7,
    }
}

fn seed(cache: &mut RegionCache<Loader>) -> RegionVerId {
    cache.locate_key(b"k").unwrap().region
}

fn current_region(
    id: u64,
    conf_ver: u64,
    version: u64,
    start: &[u8],
    end: &[u8],
) -> metapb::Region {
    metapb::Region {
        id,
        start_key: encoded_boundary(start),
        end_key: encoded_boundary(end),
        region_epoch: Some(metapb::RegionEpoch { conf_ver, version }),
        peers: vec![
            metapb::Peer {
                id: 11,
                store_id: 101,
                role: 0,
                is_witness: false,
            },
            metapb::Peer {
                id: 12,
                store_id: 102,
                role: 99,
                is_witness: false,
            },
        ],
    }
}

fn encoded_boundary(key: &[u8]) -> Vec<u8> {
    if key.is_empty() {
        return Vec::new();
    }
    let mut encoded = Vec::new();
    encode_bytes(&mut encoded, key);
    encoded
}

#[test]
fn pinned_handler_order_checks_undetermined_before_not_leader() {
    let (mut cache, _) = cache(location(7, 3, 4, b"", b""), []);
    let region = seed(&mut cache);
    let error = errorpb::Error {
        undetermined_result: Some(errorpb::UndeterminedResult {
            message: "ambiguous".to_owned(),
        }),
        not_leader: Some(errorpb::NotLeader {
            region_id: 7,
            leader: Some(metapb::Peer {
                id: 12,
                store_id: 102,
                role: 99,
                is_witness: false,
            }),
        }),
        ..Default::default()
    };

    assert_eq!(
        cache
            .on_region_error(
                &error,
                attempt(region),
                &mut RegionBackoffBudget::campaign_default(),
            )
            .unwrap(),
        RegionErrorDisposition::ReturnRegionError
    );
    assert_eq!(cache.locate_key(b"k").unwrap().leader_peer_id, Some(11));
}

#[test]
fn bucket_version_mismatch_publishes_only_strictly_newer_metadata() {
    let (mut cache, _) = cache(location(7, 3, 4, b"", b""), []);
    let region = seed(&mut cache);
    let mut budget = RegionBackoffBudget::campaign_default();

    let error = errorpb::Error {
        bucket_version_not_match: Some(errorpb::BucketVersionNotMatch {
            version: 5,
            keys: vec![b"a".to_vec(), b"m".to_vec(), b"z".to_vec()],
        }),
        ..Default::default()
    };
    assert_eq!(
        cache
            .on_region_error(&error, attempt(region), &mut budget)
            .unwrap(),
        RegionErrorDisposition::ReturnRegionError
    );
    let published = cache.locate_key(b"k").unwrap().buckets.clone().unwrap();
    assert_eq!(published.region_id, region.id);
    assert_eq!(published.version, 5);
    assert_eq!(
        published.keys,
        vec![b"a".to_vec(), b"m".to_vec(), b"z".to_vec()]
    );
    assert_eq!(published.stats, None);
    assert_eq!(published.period_in_ms, 0);

    for version in [4, 5] {
        let stale = errorpb::Error {
            bucket_version_not_match: Some(errorpb::BucketVersionNotMatch {
                version,
                keys: vec![b"stale".to_vec()],
            }),
            ..Default::default()
        };
        assert_eq!(
            cache
                .on_region_error(&stale, attempt(region), &mut budget)
                .unwrap(),
            RegionErrorDisposition::ReturnRegionError
        );
    }
    let retained = cache.locate_key(b"k").unwrap().buckets.as_ref().unwrap();
    assert_eq!(retained.version, 5);
    assert_eq!(
        retained.keys,
        vec![b"a".to_vec(), b"m".to_vec(), b"z".to_vec()]
    );
}

#[test]
fn bucket_version_mismatch_does_not_synthesize_a_missing_region() {
    let (mut cache, _) = cache(location(7, 3, 4, b"", b""), []);
    let region = seed(&mut cache);
    let observed = attempt(region);
    assert!(cache.invalidate(region));

    let error = errorpb::Error {
        bucket_version_not_match: Some(errorpb::BucketVersionNotMatch {
            version: 5,
            keys: vec![b"a".to_vec()],
        }),
        ..Default::default()
    };
    assert_eq!(
        cache.on_region_error(
            &error,
            observed.clone(),
            &mut RegionBackoffBudget::campaign_default(),
        ),
        Err(RegionRecoveryError::StaleObservation(observed))
    );
    assert!(cache.is_empty());
}

#[test]
fn known_leader_updates_snapshot_then_returns_through_reselection() {
    let (mut cache, _) = cache(location(7, 3, 4, b"", b""), []);
    let region = seed(&mut cache);
    let error = errorpb::Error {
        not_leader: Some(errorpb::NotLeader {
            region_id: 7,
            leader: Some(metapb::Peer {
                id: 12,
                store_id: 102,
                role: 99,
                is_witness: false,
            }),
        }),
        ..Default::default()
    };
    let mut budget = RegionBackoffBudget::campaign_default();

    assert_eq!(
        cache
            .on_region_error(&error, attempt(region), &mut budget)
            .unwrap(),
        RegionErrorDisposition::RetrySelector {
            attempt: attempt(region),
            transition: SelectorRecovery::FollowKnownLeader { leader_peer_id: 12 },
            delay: Duration::ZERO,
        }
    );
    assert_eq!(budget.total_sleep(), Duration::ZERO);
    assert_eq!(cache.locate_key(b"k").unwrap().leader_peer_id, Some(12));
}

#[test]
fn newly_named_leader_is_retried_after_a_follower_attempt() {
    let mut candidate = location(7, 3, 4, b"", b"");
    candidate.peers[1].role = PeerRole::Voter;
    let (mut cache, _) = cache(candidate, []);
    let region = seed(&mut cache);
    let mut selector = cache
        .request_selector(
            region,
            tidb_txnkv::region::ReadPolicy {
                mode: ReplicaReadMode::Follower,
                ..tidb_txnkv::region::ReadPolicy::default()
            },
        )
        .unwrap();
    let tidb_txnkv::region::RequestSelection::Attempt(alternate) =
        cache.select_request(&mut selector).unwrap()
    else {
        panic!("expected follower")
    };
    assert_eq!(alternate.attempt.peer_id, 12);
    assert!(selector.record_attempt_result(&alternate.attempt, Duration::from_millis(1)));

    let error = errorpb::Error {
        not_leader: Some(errorpb::NotLeader {
            region_id: 7,
            leader: Some(metapb::Peer {
                id: 12,
                store_id: 102,
                role: 0,
                is_witness: false,
            }),
        }),
        ..Default::default()
    };
    let disposition = cache
        .on_region_error(
            &error,
            alternate.attempt.clone(),
            &mut RegionBackoffBudget::campaign_default(),
        )
        .unwrap();
    let RegionErrorDisposition::RetrySelector {
        attempt,
        transition,
        ..
    } = disposition
    else {
        panic!("known leader must transition the same selector")
    };
    assert!(selector.apply_recovery(&attempt, transition));
    assert!(!selector.apply_recovery(&attempt, transition));

    let tidb_txnkv::region::RequestSelection::Attempt(retried) =
        cache.select_request(&mut selector).unwrap()
    else {
        panic!("new leader must receive one fresh leader-semantics attempt")
    };
    assert_eq!(retried.attempt.peer_id, 12);
    assert!(retried.cached_leader);
    assert!(!retried.replica_read);
    assert!(!retried.stale_read);
}

#[test]
fn stale_data_not_ready_is_an_exact_single_use_selector_transition() {
    let mut candidate = location(7, 3, 4, b"", b"");
    candidate.peers[1].role = PeerRole::Voter;
    let (mut cache, _) = cache(candidate, []);
    let region = seed(&mut cache);
    let mut selector = cache
        .request_selector(
            region,
            tidb_txnkv::region::ReadPolicy {
                mode: ReplicaReadMode::Mixed,
                stale_read: true,
                selection_seed: 1,
                ..tidb_txnkv::region::ReadPolicy::default()
            },
        )
        .unwrap();
    let tidb_txnkv::region::RequestSelection::Attempt(stale) =
        cache.select_request(&mut selector).unwrap()
    else {
        panic!("expected stale attempt")
    };
    assert!(stale.stale_read);
    assert!(selector.record_attempt_result(&stale.attempt, Duration::from_millis(1)));
    let disposition = cache
        .on_region_error(
            &errorpb::Error {
                data_is_not_ready: Some(errorpb::DataIsNotReady::default()),
                ..errorpb::Error::default()
            },
            stale.attempt.clone(),
            &mut RegionBackoffBudget::campaign_default(),
        )
        .unwrap();
    let RegionErrorDisposition::RetrySelector {
        attempt,
        transition,
        ..
    } = disposition
    else {
        panic!("data-not-ready must stay selector-owned")
    };
    assert!(selector.apply_recovery(&attempt, transition));
    assert!(!selector.apply_recovery(&attempt, transition));

    let tidb_txnkv::region::RequestSelection::Attempt(leader) =
        cache.select_request(&mut selector).unwrap()
    else {
        panic!("stale retry must try the unattempted leader")
    };
    assert_eq!(leader.attempt.peer_id, 11);
    assert!(!leader.replica_read);
    assert!(!leader.stale_read);
}

#[test]
fn known_unreachable_leader_never_bypasses_reselection() {
    let first = location(7, 3, 4, b"", b"");
    let refreshed = first.clone();
    let metadata = Rc::new(RefCell::new(Vec::new()));
    let mut cache = RegionCache::new(Loader {
        initial: VecDeque::from([first, refreshed]),
        hydrated: VecDeque::new(),
        metadata,
    });
    let region = seed(&mut cache);
    let unreachable = RegionAttempt {
        region,
        peer_id: 12,
        store_id: 102,
        address: "store-102".to_owned(),
        store_epoch: 9,
    };
    cache
        .on_send_failure(&unreachable, StoreLiveness::Unreachable)
        .unwrap();
    cache.invalidate(region);
    let region = seed(&mut cache);
    let error = errorpb::Error {
        not_leader: Some(errorpb::NotLeader {
            region_id: 7,
            leader: Some(metapb::Peer {
                id: 12,
                store_id: 102,
                role: 0,
                is_witness: false,
            }),
        }),
        ..Default::default()
    };

    assert_eq!(
        cache
            .on_region_error(
                &error,
                attempt(region),
                &mut RegionBackoffBudget::campaign_default(),
            )
            .unwrap(),
        RegionErrorDisposition::RetrySelector {
            attempt: attempt(region),
            transition: SelectorRecovery::FollowKnownLeader { leader_peer_id: 12 },
            delay: Duration::ZERO,
        }
    );
    let mut selector = cache
        .request_selector(region, tidb_txnkv::region::ReadPolicy::default())
        .unwrap();
    selector.reject_peer(11);
    assert_eq!(
        cache.select_request(&mut selector).unwrap(),
        tidb_txnkv::region::RequestSelection::ReloadRegion { region }
    );
}

#[test]
fn missing_leader_retries_peers_while_unknown_named_leader_rebuilds() {
    for leader in [
        None,
        Some(metapb::Peer {
            id: 99,
            store_id: 999,
            role: 0,
            is_witness: false,
        }),
    ] {
        let (mut cache, _) = cache(location(7, 3, 4, b"", b""), []);
        let region = seed(&mut cache);
        let error = errorpb::Error {
            not_leader: Some(errorpb::NotLeader {
                region_id: 7,
                leader,
            }),
            ..Default::default()
        };
        let disposition = cache
            .on_region_error(
                &error,
                attempt(region),
                &mut RegionBackoffBudget::campaign_default(),
            )
            .unwrap();
        let deferred = error.not_leader.as_ref().unwrap().leader.is_none();
        if deferred {
            assert_eq!(cache.len(), 1, "nil leader keeps topology for peer probing");
            assert_eq!(
                disposition,
                RegionErrorDisposition::RetrySelector {
                    attempt: attempt(region),
                    transition: SelectorRecovery::RejectPeer,
                    delay: Duration::from_millis(2),
                }
            );
        } else {
            assert_eq!(
                disposition,
                RegionErrorDisposition::RebuildRanges {
                    delay: Duration::ZERO,
                    action: RegionRebuildAction::CacheReady,
                }
            );
        }
        assert_eq!(cache.is_empty(), !deferred);
    }
}

#[test]
fn delayed_route_observation_is_rejected_before_cache_mutation() {
    let (mut cache, _) = cache(location(7, 3, 4, b"", b""), []);
    let region = seed(&mut cache);
    let mut stale = attempt(region);
    stale.address = "old-address".to_owned();
    let error = errorpb::Error {
        region_not_found: Some(errorpb::RegionNotFound { region_id: 7 }),
        ..Default::default()
    };

    assert!(matches!(
        cache.on_region_error(&error, stale, &mut RegionBackoffBudget::campaign_default()),
        Err(RegionRecoveryError::StaleObservation(_))
    ));
    assert_eq!(cache.len(), 1);
}

#[test]
fn epoch_ahead_retries_old_route_but_empty_current_regions_rebuilds() {
    let (mut cache, _) = cache(location(7, 3, 4, b"", b""), []);
    let region = seed(&mut cache);
    let ahead = errorpb::Error {
        epoch_not_match: Some(errorpb::EpochNotMatch {
            current_regions: vec![current_region(7, 2, 4, b"", b"")],
        }),
        ..Default::default()
    };
    let RegionErrorDisposition::RetryRoute { delay, .. } = cache
        .on_region_error(
            &ahead,
            attempt(region),
            &mut RegionBackoffBudget::campaign_default(),
        )
        .unwrap()
    else {
        panic!("an epoch-ahead response must retry the old route")
    };
    assert_eq!(delay, Duration::from_millis(2));
    assert_eq!(cache.len(), 1);

    let empty = errorpb::Error {
        epoch_not_match: Some(errorpb::EpochNotMatch::default()),
        ..Default::default()
    };
    assert_eq!(
        cache
            .on_region_error(
                &empty,
                attempt(region),
                &mut RegionBackoffBudget::campaign_default(),
            )
            .unwrap(),
        RegionErrorDisposition::RebuildRanges {
            delay: Duration::ZERO,
            action: RegionRebuildAction::CacheReady,
        }
    );
    assert!(cache.is_empty());
}

#[test]
fn split_hydrates_every_region_then_replaces_old_snapshot_atomically() {
    let left = location(8, 4, 5, b"", b"m");
    let right = location(9, 4, 5, b"m", b"");
    let (mut cache, recorded) = cache(
        location(7, 3, 4, b"", b""),
        [Ok(left.clone()), Ok(right.clone())],
    );
    let region = seed(&mut cache);
    let error = errorpb::Error {
        epoch_not_match: Some(errorpb::EpochNotMatch {
            current_regions: vec![
                current_region(8, 4, 5, b"", b"m"),
                current_region(9, 4, 5, b"m", b""),
            ],
        }),
        ..Default::default()
    };

    assert_eq!(
        cache
            .on_region_error(
                &error,
                attempt(region),
                &mut RegionBackoffBudget::campaign_default(),
            )
            .unwrap(),
        RegionErrorDisposition::RebuildRanges {
            delay: Duration::ZERO,
            action: RegionRebuildAction::CacheReady,
        }
    );
    assert_eq!(cache.locate_key(b"a").unwrap(), &left);
    assert_eq!(cache.locate_key(b"z").unwrap(), &right);
    let recorded = recorded.borrow();
    assert_eq!(recorded.len(), 2);
    assert!(recorded
        .iter()
        .all(|(_, leader_store)| *leader_store == 101));
    assert_eq!(recorded[0].0.peers[1].role, PeerRole::Unknown(99));
}

#[test]
fn hydration_failure_preserves_the_complete_old_snapshot() {
    let left = location(8, 4, 5, b"", b"m");
    let (mut cache, _) = cache(
        location(7, 3, 4, b"", b""),
        [
            Ok(left),
            Err(RegionLoadError::new("removed_store", "store 102 removed")),
        ],
    );
    let region = seed(&mut cache);
    let error = errorpb::Error {
        epoch_not_match: Some(errorpb::EpochNotMatch {
            current_regions: vec![
                current_region(8, 4, 5, b"", b"m"),
                current_region(9, 4, 5, b"m", b""),
            ],
        }),
        ..Default::default()
    };

    assert!(matches!(
        cache.on_region_error(
            &error,
            attempt(region),
            &mut RegionBackoffBudget::campaign_default()
        ),
        Err(RegionRecoveryError::Loader(_))
    ));
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.locate_key(b"z").unwrap().region, region);
}

#[test]
fn available_sibling_subset_replaces_old_snapshot_without_requiring_full_coverage() {
    let left = location(8, 4, 5, b"", b"g");
    let right = location(9, 4, 5, b"m", b"");
    let (mut cache, _) = cache(
        location(7, 3, 4, b"", b""),
        [Ok(left.clone()), Ok(right.clone())],
    );
    let region = seed(&mut cache);
    let error = errorpb::Error {
        epoch_not_match: Some(errorpb::EpochNotMatch {
            current_regions: vec![
                current_region(8, 4, 5, b"", b"g"),
                current_region(9, 4, 5, b"m", b""),
            ],
        }),
        ..Default::default()
    };

    assert!(matches!(
        cache
            .on_region_error(
                &error,
                attempt(region),
                &mut RegionBackoffBudget::campaign_default(),
            )
            .unwrap(),
        RegionErrorDisposition::RebuildRanges { .. }
    ));
    assert_eq!(cache.locate_key(b"a").unwrap(), &left);
    assert_eq!(cache.locate_key(b"z").unwrap(), &right);
}

#[test]
fn terminal_and_backoff_branches_are_typed_and_budgeted_once() {
    let (mut terminal_cache, _) = cache(location(7, 3, 4, b"", b""), []);
    let region = seed(&mut terminal_cache);
    let terminal = errorpb::Error {
        raft_entry_too_large: Some(errorpb::RaftEntryTooLarge {
            region_id: 7,
            entry_size: 4096,
        }),
        ..Default::default()
    };
    assert_eq!(
        terminal_cache
            .on_region_error(
                &terminal,
                attempt(region),
                &mut RegionBackoffBudget::campaign_default(),
            )
            .unwrap(),
        RegionErrorDisposition::Terminal(RegionTerminalError::RaftEntryTooLarge {
            region_id: 7,
            entry_size: 4096,
        })
    );

    let busy = errorpb::Error {
        server_is_busy: Some(errorpb::ServerIsBusy::default()),
        ..Default::default()
    };
    let mut budget = RegionBackoffBudget::with_jitter_seed(Duration::from_secs(20), 1);
    let first = terminal_cache
        .on_region_error(&busy, attempt(region), &mut budget)
        .unwrap();
    let RegionErrorDisposition::RetryRoute { delay, .. } = first else {
        panic!("busy must retry the observed route")
    };
    assert!((Duration::from_millis(1_000)..Duration::from_millis(2_000)).contains(&delay));
    assert_eq!(budget.remaining(), Duration::from_secs(20));

    let mut tiny = RegionBackoffBudget::with_jitter_seed(Duration::from_millis(1), 1);
    let no_leader = errorpb::Error {
        not_leader: Some(errorpb::NotLeader {
            region_id: 7,
            leader: None,
        }),
        ..Default::default()
    };
    let (mut first_cache, _) = cache(location(7, 3, 4, b"", b""), []);
    let region = seed(&mut first_cache);
    assert!(matches!(
        first_cache
            .on_region_error(&no_leader, attempt(region), &mut tiny)
            .unwrap(),
        RegionErrorDisposition::RetrySelector {
            transition: SelectorRecovery::RejectPeer,
            ..
        }
    ));
    let (mut second_cache, _) = cache(location(7, 3, 4, b"", b""), []);
    let region = seed(&mut second_cache);
    assert_eq!(
        second_cache
            .on_region_error(&no_leader, attempt(region), &mut tiny)
            .unwrap(),
        RegionErrorDisposition::Terminal(RegionTerminalError::BackoffExhausted {
            kind: RegionBackoffKind::RegionScheduling,
            max_sleep: Duration::from_millis(1),
        })
    );
}

#[test]
fn backoff_arithmetic_preserves_strict_exponential_equal_jitter_and_busy_exclusion() {
    let mut strict = RegionBackoffBudget::with_jitter_seed(Duration::from_secs(20), 1);
    assert_eq!(
        (0..4)
            .map(|_| strict.next_delay(RegionBackoffKind::RegionMiss).unwrap())
            .collect::<Vec<_>>(),
        [2, 4, 8, 16].map(Duration::from_millis)
    );

    let mut busy = RegionBackoffBudget::with_jitter_seed(Duration::from_secs(20), 7);
    let mut sleeps = Vec::new();
    loop {
        match busy.next_delay(RegionBackoffKind::TikvServerBusy) {
            Ok(delay) => {
                assert!(
                    (Duration::from_millis(1_000)..Duration::from_millis(10_000)).contains(&delay),
                    "busy delay escaped its equal-jitter bounds after {} attempts: {delay:?}",
                    sleeps.len(),
                );
                sleeps.push(delay);
            }
            Err(exhausted) => {
                assert_eq!(exhausted.kind, RegionBackoffKind::TikvServerBusy);
                break;
            }
        }
    }
    assert!(busy.total_sleep() >= Duration::from_secs(600));
    assert_eq!(busy.remaining(), Duration::from_secs(20));
    assert!(sleeps.len() > 60);
    assert_eq!(
        busy.next_delay(RegionBackoffKind::RegionMiss).unwrap(),
        Duration::from_millis(2),
        "the excluded busy cap applies only to another excluded backoff"
    );
}

#[test]
fn tikv_rpc_backoff_uses_equal_jitter_and_the_shared_effective_budget() {
    let mut budget = RegionBackoffBudget::with_jitter_seed(Duration::from_secs(20), 7);
    let first = budget.next_delay(RegionBackoffKind::TikvRpc).unwrap();
    let second = budget.next_delay(RegionBackoffKind::TikvRpc).unwrap();
    assert!((Duration::from_millis(50)..Duration::from_millis(100)).contains(&first));
    assert!((Duration::from_millis(100)..Duration::from_millis(200)).contains(&second));
    assert_eq!(budget.remaining(), Duration::from_secs(20) - first - second);

    let mut capped = RegionBackoffBudget::with_jitter_seed(Duration::from_secs(60), 11);
    for _ in 0..20 {
        assert!(capped.next_delay(RegionBackoffKind::TikvRpc).unwrap() < Duration::from_secs(2));
    }
}

#[test]
fn exhausted_disk_full_returns_to_the_outer_region_miss_owner() {
    let disk_full = errorpb::Error {
        disk_full: Some(errorpb::DiskFull::default()),
        ..Default::default()
    };
    let (mut cache, _) = cache(location(7, 3, 4, b"", b""), []);
    let region = seed(&mut cache);
    let mut budget = RegionBackoffBudget::with_jitter_seed(Duration::from_millis(1), 1);

    let RegionErrorDisposition::RetryRoute { delay, .. } = cache
        .on_region_error(&disk_full, attempt(region), &mut budget)
        .unwrap()
    else {
        panic!("the first disk-full response must retry its route")
    };
    assert_eq!(delay, Duration::from_millis(500));
    assert_eq!(
        cache
            .on_region_error(&disk_full, attempt(region), &mut budget)
            .unwrap(),
        RegionErrorDisposition::RebuildRanges {
            delay: Duration::ZERO,
            action: RegionRebuildAction::CacheReady,
        }
    );
    assert_eq!(cache.len(), 1, "disk-full does not invalidate topology");
    let exhausted = budget
        .next_delay(RegionBackoffKind::RegionMiss)
        .unwrap_err();
    assert_eq!(exhausted.kind, RegionBackoffKind::TikvDiskFull);
}

#[test]
fn every_outer_region_error_branch_has_a_typed_source_action() {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Expected {
        Retry,
        Rebuild,
        Return,
        FlashProgress,
        FlashNotPrepared,
        RaftTooLarge,
        InvalidMaxTs,
    }

    let cases = vec![
        (
            "undetermined",
            errorpb::Error {
                undetermined_result: Some(errorpb::UndeterminedResult::default()),
                ..Default::default()
            },
            Expected::Return,
            false,
        ),
        (
            "known leader",
            errorpb::Error {
                not_leader: Some(errorpb::NotLeader {
                    region_id: 7,
                    leader: Some(metapb::Peer {
                        id: 12,
                        store_id: 102,
                        role: 99,
                        is_witness: false,
                    }),
                }),
                ..Default::default()
            },
            Expected::Retry,
            false,
        ),
        (
            "disk full",
            errorpb::Error {
                disk_full: Some(errorpb::DiskFull::default()),
                ..Default::default()
            },
            Expected::Retry,
            false,
        ),
        (
            "recovery",
            errorpb::Error {
                recovery_in_progress: Some(errorpb::RecoveryInProgress::default()),
                ..Default::default()
            },
            Expected::Rebuild,
            true,
        ),
        (
            "witness",
            errorpb::Error {
                is_witness: Some(errorpb::IsWitness::default()),
                ..Default::default()
            },
            Expected::Rebuild,
            true,
        ),
        (
            "flashback progress",
            errorpb::Error {
                flashback_in_progress: Some(errorpb::FlashbackInProgress::default()),
                ..Default::default()
            },
            Expected::FlashProgress,
            false,
        ),
        (
            "flashback not prepared",
            errorpb::Error {
                flashback_not_prepared: Some(errorpb::FlashbackNotPrepared::default()),
                ..Default::default()
            },
            Expected::FlashNotPrepared,
            false,
        ),
        (
            "region not found",
            errorpb::Error {
                region_not_found: Some(errorpb::RegionNotFound::default()),
                ..Default::default()
            },
            Expected::Rebuild,
            true,
        ),
        (
            "key not in region",
            errorpb::Error {
                key_not_in_region: Some(errorpb::KeyNotInRegion::default()),
                ..Default::default()
            },
            Expected::Rebuild,
            true,
        ),
        (
            "empty epoch",
            errorpb::Error {
                epoch_not_match: Some(errorpb::EpochNotMatch::default()),
                ..Default::default()
            },
            Expected::Rebuild,
            true,
        ),
        (
            "bucket mismatch",
            errorpb::Error {
                bucket_version_not_match: Some(errorpb::BucketVersionNotMatch::default()),
                ..Default::default()
            },
            Expected::Return,
            false,
        ),
        (
            "server busy",
            errorpb::Error {
                server_is_busy: Some(errorpb::ServerIsBusy::default()),
                ..Default::default()
            },
            Expected::Retry,
            false,
        ),
        (
            "stale command",
            errorpb::Error {
                stale_command: Some(errorpb::StaleCommand::default()),
                ..Default::default()
            },
            Expected::Retry,
            false,
        ),
        (
            "store mismatch",
            errorpb::Error {
                store_not_match: Some(errorpb::StoreNotMatch::default()),
                ..Default::default()
            },
            Expected::Rebuild,
            true,
        ),
        (
            "raft too large",
            errorpb::Error {
                raft_entry_too_large: Some(errorpb::RaftEntryTooLarge::default()),
                ..Default::default()
            },
            Expected::RaftTooLarge,
            false,
        ),
        (
            "max timestamp",
            errorpb::Error {
                max_timestamp_not_synced: Some(errorpb::MaxTimestampNotSynced::default()),
                ..Default::default()
            },
            Expected::Retry,
            false,
        ),
        (
            "not initialized",
            errorpb::Error {
                region_not_initialized: Some(errorpb::RegionNotInitialized::default()),
                ..Default::default()
            },
            Expected::Retry,
            false,
        ),
        (
            "read index",
            errorpb::Error {
                read_index_not_ready: Some(errorpb::ReadIndexNotReady::default()),
                ..Default::default()
            },
            Expected::Retry,
            false,
        ),
        (
            "merging",
            errorpb::Error {
                proposal_in_merging_mode: Some(errorpb::ProposalInMergingMode::default()),
                ..Default::default()
            },
            Expected::Retry,
            false,
        ),
        (
            "stale read data",
            errorpb::Error {
                data_is_not_ready: Some(errorpb::DataIsNotReady::default()),
                ..Default::default()
            },
            Expected::Retry,
            false,
        ),
        (
            "peer mismatch",
            errorpb::Error {
                mismatch_peer_id: Some(errorpb::MismatchPeerId::default()),
                ..Default::default()
            },
            Expected::Rebuild,
            true,
        ),
        (
            "invalid max timestamp update",
            errorpb::Error {
                message: "invalid max_ts update: stale tso".to_owned(),
                ..Default::default()
            },
            Expected::InvalidMaxTs,
            false,
        ),
        (
            "unsupported configurable deadline",
            errorpb::Error {
                message: "Deadline is exceeded".to_owned(),
                ..Default::default()
            },
            Expected::Rebuild,
            true,
        ),
        (
            "unknown",
            errorpb::Error::default(),
            Expected::Rebuild,
            true,
        ),
    ];

    for (name, error, expected, cache_empty) in cases {
        let (mut cache, _) = cache(location(7, 3, 4, b"", b""), []);
        let region = seed(&mut cache);
        let disposition = cache
            .on_region_error(
                &error,
                attempt(region),
                &mut RegionBackoffBudget::with_jitter_seed(Duration::from_secs(20), 3),
            )
            .unwrap();
        let actual = match disposition {
            RegionErrorDisposition::RetryRoute { .. } => Expected::Retry,
            RegionErrorDisposition::RetrySelector { .. } => Expected::Retry,
            RegionErrorDisposition::RebuildRanges { .. } => Expected::Rebuild,
            RegionErrorDisposition::ReturnRegionError => Expected::Return,
            RegionErrorDisposition::Terminal(RegionTerminalError::FlashbackInProgress {
                ..
            }) => Expected::FlashProgress,
            RegionErrorDisposition::Terminal(RegionTerminalError::FlashbackNotPrepared {
                ..
            }) => Expected::FlashNotPrepared,
            RegionErrorDisposition::Terminal(RegionTerminalError::RaftEntryTooLarge { .. }) => {
                Expected::RaftTooLarge
            }
            RegionErrorDisposition::Terminal(RegionTerminalError::InvalidMaxTimestampUpdate {
                ..
            }) => Expected::InvalidMaxTs,
            RegionErrorDisposition::Terminal(RegionTerminalError::BackoffExhausted { .. }) => {
                panic!("{name}: fresh budget unexpectedly exhausted")
            }
        };
        assert_eq!(actual, expected, "{name}");
        assert_eq!(cache.is_empty(), cache_empty, "{name}");
    }
}

#[test]
fn adjacent_multi_populated_fields_preserve_pinned_handler_precedence() {
    let precedence = [
        (
            errorpb::Error {
                disk_full: Some(errorpb::DiskFull::default()),
                recovery_in_progress: Some(errorpb::RecoveryInProgress::default()),
                ..Default::default()
            },
            false,
        ),
        (
            errorpb::Error {
                flashback_in_progress: Some(errorpb::FlashbackInProgress::default()),
                flashback_not_prepared: Some(errorpb::FlashbackNotPrepared::default()),
                ..Default::default()
            },
            false,
        ),
        (
            errorpb::Error {
                server_is_busy: Some(errorpb::ServerIsBusy::default()),
                stale_command: Some(errorpb::StaleCommand::default()),
                ..Default::default()
            },
            false,
        ),
        (
            errorpb::Error {
                store_not_match: Some(errorpb::StoreNotMatch::default()),
                raft_entry_too_large: Some(errorpb::RaftEntryTooLarge::default()),
                ..Default::default()
            },
            true,
        ),
    ];

    for (index, (error, invalidated)) in precedence.into_iter().enumerate() {
        let (mut cache, _) = cache(location(7, 3, 4, b"", b""), []);
        let region = seed(&mut cache);
        let disposition = cache
            .on_region_error(
                &error,
                attempt(region),
                &mut RegionBackoffBudget::with_jitter_seed(Duration::from_secs(20), 5),
            )
            .unwrap();
        match index {
            0 | 2 => assert!(matches!(
                disposition,
                RegionErrorDisposition::RetryRoute { .. }
            )),
            1 => assert!(matches!(
                disposition,
                RegionErrorDisposition::Terminal(RegionTerminalError::FlashbackInProgress { .. })
            )),
            3 => assert!(matches!(
                disposition,
                RegionErrorDisposition::RebuildRanges { .. }
            )),
            _ => unreachable!(),
        }
        assert_eq!(cache.is_empty(), invalidated);
    }
}
