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

//! What one direct-unary dispatch actually sends, and which authority each
//! field comes from: the lazy, address-directed, logically ordered dispatch
//! shape client-go has; the transaction event a locked response publishes; the
//! replica-read policies and request flags production metadata drives; and the
//! store labels and load inputs the live selector consumes.

#![allow(missing_docs)]

use crate::direct_unary_client_fixture::*;

fn completion_order_transport(
    calls: Rc<RefCell<Vec<ObservedCall>>>,
    ready_on_try: impl IntoIterator<Item = bool>,
) -> DirectUnaryQueryTransport<ScriptedClient, ScriptedLoader> {
    batch_first_transport(
        calls,
        [Ok(response(b"left")), Ok(response(b"right"))],
        [
            location(1, "a", "m", "tikv-1:20160"),
            location(2, "m", "z", "tikv-2:20160"),
        ],
        ready_on_try,
    )
}

#[test]
fn unordered_regions_publish_first_completed_response() {
    // pkg/store/copr/coprocessor.go:1184-1192, 1416-1465. Unordered
    // workers share respChan, so the first completed region reaches Next.
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.keep_order = false;
    request_metadata.concurrency = 2;
    let mut runtime = InjectedQueryRuntime::new(completion_order_transport(
        Rc::clone(&calls),
        [false, true],
    ));
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));

    assert_eq!(result.next_raw().unwrap(), Some(b"right".to_vec()));
    assert_eq!(calls.borrow().len(), 2, "prefetch stays bounded by concurrency");
}

#[test]
fn ordered_regions_retain_logical_range_order() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.concurrency = 2;
    let mut runtime = InjectedQueryRuntime::new(completion_order_transport(
        Rc::clone(&calls),
        [false, true],
    ));
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));

    assert_eq!(result.next_raw().unwrap(), Some(b"left".to_vec()));
    assert_eq!(calls.borrow().len(), 2, "ordered reads still prefetch regions");
}

#[test]
fn unordered_region_window_is_bounded_and_results_are_not_lost() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.keep_order = false;
    request_metadata.concurrency = 2;
    let mut runtime = InjectedQueryRuntime::new(batch_first_transport(
        Rc::clone(&calls),
        [
            Ok(response(b"left")),
            Ok(response(b"right")),
            Ok(response(b"third")),
        ],
        [
            location(1, "a", "m", "tikv-1:20160"),
            location(2, "m", "t", "tikv-2:20160"),
            location(3, "t", "z", "tikv-3:20160"),
        ],
        [false, true, true],
    ));
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));

    assert_eq!(result.next_raw().unwrap(), Some(b"right".to_vec()));
    assert_eq!(calls.borrow().len(), 2);
    assert_eq!(result.next_raw().unwrap(), Some(b"third".to_vec()));
    assert_eq!(calls.borrow().len(), 3);

    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.keep_order = false;
    request_metadata.concurrency = 2;
    let mut runtime = InjectedQueryRuntime::new(completion_order_transport(
        calls,
        [true, true],
    ));
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));
    assert_eq!(result.next_raw().unwrap(), Some(b"left".to_vec()));
    assert_eq!(result.next_raw().unwrap(), Some(b"right".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
}

#[test]
fn client_go_shaped_dispatch_is_lazy_address_directed_and_logically_ordered() {
    // client-go/internal/client/client.go:96-105 Client.SendRequest
    // pkg/store/copr/coprocessor.go:1723 handleTaskOnce
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [Ok(response(b"left")), Ok(response(b"right"))],
        [
            location(1, "a", "m", "tikv-1:20160"),
            location(2, "m", "z", "tikv-2:20160"),
        ],
    ));
    let request = transport_request(metadata("a", "z"));
    let mut result = select_result(&mut runtime, &request);

    assert!(calls.borrow().is_empty(), "send must stay response-lazy");
    assert_eq!(result.next_raw().unwrap(), Some(b"left".to_vec()));
    assert_eq!(calls.borrow().len(), 1);
    // Go handleTaskOnce creates a fresh RPC timeout for the next region;
    // time spent consuming the previous result is not charged to that RPC.
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(result.next_raw().unwrap(), Some(b"right".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);

    let mut normalized_calls = calls.borrow().clone();
    assert!(normalized_calls.iter().all(|call| {
        call.timeout <= Duration::from_millis(777) && call.timeout > Duration::from_millis(700)
    }));
    for call in &mut normalized_calls {
        call.timeout = Duration::from_millis(777);
    }
    assert_eq!(
        normalized_calls.as_slice(),
        [
            ObservedCall {
                address: "tikv-1:20160".to_owned(),
                forwarded_host: None,
                timeout: Duration::from_millis(777),
                region_id: 1,
                data: b"dag-read".to_vec(),
                paging_size: 0,
                is_cache_enabled: false,
                cache_if_match_version: 0,
                predicted_read_bytes: 4096,
                cluster_id: 9001,
                conf_ver: 1,
                version: 2,
                peer_id: 101,
                store_id: 201,
                peer_role: 0,
                is_witness: false,
                task_id: 29,
                request_source: "internal_ddl".to_owned(),
                not_fill_cache: true,
                replica_read_type: ClientReplicaReadType::Leader,
                replica_read: false,
                stale_read: false,
            },
            ObservedCall {
                address: "tikv-2:20160".to_owned(),
                forwarded_host: None,
                timeout: Duration::from_millis(777),
                region_id: 2,
                data: b"dag-read".to_vec(),
                paging_size: 0,
                is_cache_enabled: false,
                cache_if_match_version: 0,
                predicted_read_bytes: 4096,
                cluster_id: 9001,
                conf_ver: 1,
                version: 2,
                peer_id: 102,
                store_id: 202,
                peer_role: 0,
                is_witness: false,
                task_id: 29,
                request_source: "internal_ddl".to_owned(),
                not_fill_cache: true,
                replica_read_type: ClientReplicaReadType::Leader,
                replica_read: false,
                stale_read: false,
            },
        ]
    );
}

#[test]
fn locked_response_publishes_the_exact_transaction_event_before_recovery() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let lock = KvrpcLockInfo {
        key: b"locked-key".to_vec(),
        primary_lock: b"primary-key".to_vec(),
        lock_version: 42,
        ..KvrpcLockInfo::default()
    };
    let transport = transport(
        Rc::clone(&calls),
        [Ok(locked_response(lock.clone()))],
        [location(1, "a", "z", "tikv-1:20160")],
    );
    let observed = Arc::new(Mutex::new(None));
    let callback_observation = Arc::clone(&observed);
    let callback: tidb_txnkv::EventCallback = Arc::new(move |event| {
        *callback_observation.lock().expect("lock event observation") = event
            .get_cop_meet_lock()
            .and_then(|event| event.lock_info.clone());
    });
    let mut runtime = InjectedQueryRuntime::new(transport);
    let request = transport_request(metadata("a", "z"));
    let mut result = runtime
        .select_with_runtime_stats(
            &request,
            SelectInput::default(),
            QueryResultContext::new(Vec::<FieldType>::new(), WarningCollector::new())
                .with_event_callback(callback),
            vec![1],
            2,
            true,
        )
        .expect("lazy locked response");

    let _ = result
        .next_raw()
        .expect_err("scripted lock recovery cannot complete");
    assert_eq!(
        observed
            .lock()
            .expect("lock event observation")
            .as_deref(),
        Some(&lock)
    );
}

#[test]
fn alive_scan_lock_uses_fast_backoff_capped_by_ttl() {
    use tidb_distsql::cop_paging::{
        LockedResponseAction, LockedResponseDelegate, LockedResponseObservation,
    };
    use tidb_txnkv::{lock::LockRecoveryResult, SharedReadRuntime};

    #[derive(Debug)]
    struct AliveLock;
    impl LockedResponseDelegate<ScriptedClient, ScriptedLoader> for AliveLock {
        fn handle_locked_response(
            &self,
            _: &SharedReadRuntime<ScriptedClient, ScriptedLoader>,
            observation: LockedResponseObservation,
        ) -> Result<LockedResponseAction, String> {
            Ok(LockedResponseAction::RetrySameTask {
                recovered: LockRecoveryResult {
                    ttl: Duration::from_millis(observation.lock.lock_ttl),
                    ..LockRecoveryResult::default()
                },
            })
        }
    }

    let calls = Rc::new(RefCell::new(Vec::new()));
    let client = ScriptedClient {
        calls: Rc::clone(&calls),
        responses: [20_000, 1, 0]
            .into_iter()
            .map(|ttl| {
                Ok(locked_response(KvrpcLockInfo {
                    key: b"locked-key".to_vec(),
                    primary_lock: b"primary-key".to_vec(),
                    lock_version: 42,
                    lock_ttl: ttl,
                    ..KvrpcLockInfo::default()
                }))
            })
            .chain([Ok(response(b"unblocked"))])
            .collect(),
        events: Rc::new(RefCell::new(Vec::new())),
        liveness: RefCell::new(VecDeque::new()),
        batch_errors: RefCell::new(VecDeque::new()),
        batch_ready_immediately: RefCell::new(VecDeque::new()),
        batch_begin_count: None,
    };
    let shared = SharedReadRuntime::new_injected(
        client,
        RegionCache::new(ScriptedLoader {
            cluster_id: 9001,
            calls: Rc::new(RefCell::new(Vec::new())),
            regions: [location(1, "a", "z", "tikv-1:20160")]
                .into_iter()
                .collect(),
        }),
    );
    let waiter = Arc::new(RecordingRetryControl::default());
    let transport = DirectUnaryQueryTransport::with_locked_response_delegate(
        shared,
        DirectUnaryRuntimeConfig {
            region_retry_waiter: waiter.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
        Arc::new(AliveLock),
    )
    .unwrap();
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));
    assert_eq!(result.next_raw().unwrap(), Some(b"unblocked".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    let waits = waiter.sleeps.lock().unwrap();
    assert_eq!(waits.len(), 2, "each live lock needs one backoff draw");
    assert!(waits[0] > Duration::ZERO && waits[0] <= Duration::from_millis(2));
    assert_eq!(waits[1], Duration::from_millis(1));
    assert_eq!(calls.borrow().len(), 4);
}

#[test]
fn ignored_request_lock_hints_back_off_before_resolve_and_stop_at_the_budget() {
    use tidb_distsql::cop_paging::{
        LockedResponseAction, LockedResponseDelegate, LockedResponseObservation,
    };
    use tidb_txnkv::{lock::LockRecoveryResult, SharedReadRuntime};

    #[derive(Debug)]
    struct Resolve {
        committed: bool,
        waits: Arc<RecordingRetryControl>,
        observations: Arc<Mutex<Vec<(usize, tidb_proto::KvrpcContext)>>>,
    }
    impl LockedResponseDelegate<ScriptedClient, ScriptedLoader> for Resolve {
        fn handle_locked_response(
            &self,
            _: &SharedReadRuntime<ScriptedClient, ScriptedLoader>,
            observation: LockedResponseObservation,
        ) -> Result<LockedResponseAction, String> {
            self.observations.lock().unwrap().push((
                self.waits.sleeps.lock().unwrap().len(),
                observation.request_context,
            ));
            Ok(LockedResponseAction::RetrySameTask {
                recovered: LockRecoveryResult {
                    ignore_locks: if self.committed { vec![] } else { vec![42] },
                    access_locks: if self.committed { vec![42] } else { vec![] },
                    ..LockRecoveryResult::default()
                },
            })
        }
    }
    for committed in [false, true] {
        for shared in [false, true] {
            for ordered in [false, true] {
                for outcome in ["success", "exhaust", "cancel", "unhinted"] {
                    let exhaust = outcome == "exhaust";
                    let cancel = outcome == "cancel";
                    let unhinted = outcome == "unhinted";
                    for asynchronous in [false, true] {
                        let calls = Rc::new(RefCell::new(Vec::new()));
                        let observations = Arc::new(Mutex::new(Vec::new()));
                        let waiter = Arc::new(RecordingRetryControl::default());
                        waiter.fail_next_sleep.store(cancel, Ordering::SeqCst);
                        let lock = KvrpcLockInfo {
                            key: b"locked-key".to_vec(),
                            primary_lock: b"primary".to_vec(),
                            lock_version: 42,
                            lock_type: 5,
                            ..KvrpcLockInfo::default()
                        };
                        let lock = if shared {
                            KvrpcLockInfo {
                                shared_lock_infos: vec![
                                    KvrpcLockInfo {
                                        lock_version: 41,
                                        ..lock.clone()
                                    },
                                    lock,
                                ],
                                ..KvrpcLockInfo::default()
                            }
                        } else {
                            lock
                        };
                        let client = ScriptedClient {
                            calls: Rc::clone(&calls),
                            responses: (0..3)
                                .map(|index| {
                                    let mut lock = lock.clone();
                                    if unhinted && index != 0 {
                                        lock.lock_version = 43;
                                        for child in &mut lock.shared_lock_infos {
                                            child.lock_version = 43;
                                        }
                                    }
                                    Ok(locked_response(lock))
                                })
                                .chain([Ok(response(b"unblocked"))])
                                .collect(),
                            events: Rc::default(),
                            liveness: RefCell::default(),
                            batch_errors: RefCell::default(),
                            batch_ready_immediately: RefCell::new([true; 4].into()),
                            batch_begin_count: None,
                        };
                        let shared_runtime = SharedReadRuntime::new_injected(
                            client,
                            RegionCache::new(ScriptedLoader {
                                cluster_id: 9001,
                                calls: Rc::default(),
                                regions: [location(1, "a", "z", "tikv-1:20160")].into(),
                            }),
                        );
                        let transport = DirectUnaryQueryTransport::with_locked_response_delegate(
                            shared_runtime,
                            DirectUnaryRuntimeConfig {
                                region_retry_waiter: waiter.clone(),
                                region_retry_max_sleep: Duration::from_millis(if exhaust {
                                    1
                                } else {
                                    100
                                }),
                                ..DirectUnaryRuntimeConfig::default()
                            },
                            Arc::new(Resolve {
                                committed,
                                waits: Arc::clone(&waiter),
                                observations: Arc::clone(&observations),
                            }),
                        )
                        .unwrap();
                        let transport = if asynchronous {
                            transport.with_async_dispatch()
                        } else {
                            transport
                        };
                        let mut runtime = InjectedQueryRuntime::new(transport);
                        let mut request = metadata("a", "z");
                        request.keep_order = ordered;
                        let mut result = select_result(&mut runtime, &transport_request(request));
                        if cancel {
                            assert_eq!(
                                result.next_raw().unwrap_err(),
                                tidb_distsql::QueryResponseError::Cancelled
                            );
                            assert_eq!(calls.borrow().len(), 2);
                        } else if exhaust {
                            let error = result
                                .next_raw()
                                .expect_err("repeated ignored hints must exhaust the lock budget");
                            assert_eq!(
                                error,
                                tidb_distsql::QueryResponseError::Sql {
                                    code: 9004,
                                    message: "[tikv:9004]Resolve lock timeout".to_owned()
                                }
                            );
                            assert_eq!(calls.borrow().len(), 3);
                        } else {
                            assert_eq!(result.next_raw().unwrap(), Some(b"unblocked".to_vec()));
                            assert_eq!(result.next_raw().unwrap(), None);
                            assert_eq!(calls.borrow().len(), 4);
                        }
                        let observations = observations.lock().unwrap();
                        let waits = waiter.sleeps.lock().unwrap();
                        assert_eq!(
                            waits.len(),
                            if unhinted {
                                0
                            } else if exhaust || cancel {
                                1
                            } else {
                                2
                            }
                        );
                        assert_eq!(
                            observations.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
                            if cancel {
                                vec![0]
                            } else if unhinted {
                                vec![0, 0, 0]
                            } else if exhaust {
                                vec![0, 1]
                            } else {
                                vec![0, 1, 2]
                            },
                            "hint backoff precedes resolution"
                        );
                        assert!(observations[0].1.resolved_locks.is_empty());
                        assert!(observations[0].1.committed_locks.is_empty());
                        if !cancel {
                            assert_eq!(
                                if committed {
                                    &observations[1].1.committed_locks
                                } else {
                                    &observations[1].1.resolved_locks
                                },
                                &[42]
                            );
                        }
                        if !unhinted {
                            assert_eq!(waits[0], Duration::from_millis(1));
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn pd_peer_role_witness_and_cluster_fields_have_one_context_authority() {
    for (role, encoded) in [
        (PeerRole::Voter, 0),
        (PeerRole::IncomingVoter, 2),
        (PeerRole::DemotingVoter, 3),
    ] {
        let calls = Rc::new(RefCell::new(Vec::new()));
        let mut candidate = location(7, "a", "z", "tikv-7:20160");
        candidate.peers[0].role = role;
        let mut runtime = InjectedQueryRuntime::new(transport(
            Rc::clone(&calls),
            [Ok(response(b"ok"))],
            [candidate],
        ));
        let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));
        assert!(calls.borrow().is_empty());
        assert_eq!(result.next_raw().unwrap(), Some(b"ok".to_vec()));

        let calls = calls.borrow();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].cluster_id, 9001);
        assert_eq!(calls[0].conf_ver, 1);
        assert_eq!(calls[0].version, 2);
        assert_eq!(calls[0].peer_id, 107);
        assert_eq!(calls[0].store_id, 207);
        assert_eq!(calls[0].peer_role, encoded);
        assert!(!calls[0].is_witness);
        assert_eq!(calls[0].task_id, 29);
        assert_eq!(calls[0].request_source, "internal_ddl");
        assert!(calls[0].not_fill_cache);
        assert!(!calls[0].replica_read);
        assert!(!calls[0].stale_read);
    }
}

#[test]
fn production_metadata_drives_supported_replica_policies_and_exact_request_flags() {
    struct Case {
        source: ReplicaReadType,
        address_suffix: &'static str,
        request_type: ClientReplicaReadType,
        replica_read: bool,
    }

    for case in [
        Case {
            source: ReplicaReadType::Leader,
            address_suffix: "-leader:20160",
            request_type: ClientReplicaReadType::Leader,
            replica_read: false,
        },
        Case {
            source: ReplicaReadType::Follower,
            address_suffix: "-learner:20160",
            request_type: ClientReplicaReadType::Follower,
            replica_read: true,
        },
        Case {
            source: ReplicaReadType::Mixed,
            address_suffix: "-follower:20160",
            request_type: ClientReplicaReadType::Mixed,
            replica_read: true,
        },
        Case {
            source: ReplicaReadType::PreferLeader,
            address_suffix: "-leader:20160",
            request_type: ClientReplicaReadType::PreferLeader,
            replica_read: false,
        },
        Case {
            source: ReplicaReadType::Learner,
            address_suffix: "-learner:20160",
            request_type: ClientReplicaReadType::Learner,
            replica_read: true,
        },
        Case {
            source: ReplicaReadType::Closest,
            address_suffix: "-follower:20160",
            request_type: ClientReplicaReadType::Mixed,
            replica_read: true,
        },
        Case {
            source: ReplicaReadType::ClosestAdaptive,
            address_suffix: "-follower:20160",
            request_type: ClientReplicaReadType::Mixed,
            replica_read: true,
        },
    ] {
        let calls = Rc::new(RefCell::new(Vec::new()));
        let mut metadata = metadata("a", "z");
        metadata.replica_read = case.source;
        let mut runtime = InjectedQueryRuntime::new(transport(
            Rc::clone(&calls),
            [Ok(response(b"ok"))],
            [location_with_three_peers(1, "a", "z", "tikv-policy")],
        ));
        let mut result = select_result(&mut runtime, &transport_request(metadata));
        assert_eq!(result.next_raw().unwrap(), Some(b"ok".to_vec()));

        let calls = calls.borrow();
        assert_eq!(calls.len(), 1);
        assert!(
            calls[0].address.ends_with(case.address_suffix),
            "{:?} selected {}",
            case.source,
            calls[0].address
        );
        assert_eq!(calls[0].replica_read_type, case.request_type);
        assert_eq!(calls[0].replica_read, case.replica_read);
        assert!(!calls[0].stale_read);
    }
}

#[test]
fn labels_and_load_inputs_are_consumed_by_the_live_selector() {
    let configurations: [fn(&mut KvRequestMetadata); 2] = [
        |metadata: &mut KvRequestMetadata| {
            metadata.match_store_labels.push(tidb_distsql::StoreLabel {
                key: "zone".to_owned(),
                value: "z1".to_owned(),
            });
        },
        |metadata: &mut KvRequestMetadata| {
            metadata.store_busy_threshold_ns = 1_000_000;
        },
    ];
    for configure in configurations {
        let mut metadata = metadata("a", "z");
        metadata.replica_read = ReplicaReadType::Mixed;
        configure(&mut metadata);
        let calls = Rc::new(RefCell::new(Vec::new()));
        let mut runtime = InjectedQueryRuntime::new(transport(
            Rc::clone(&calls),
            [Ok(response(b"selected"))],
            [location_with_three_peers(1, "a", "z", "tikv-policy")],
        ));
        let mut result = runtime
            .select_with_runtime_stats(
                &transport_request(metadata),
                SelectInput::default(),
                QueryResultContext::new(Vec::new(), WarningCollector::new()),
                vec![1],
                2,
                true,
            )
            .expect("Campaign 14 selector metadata is supported");
        assert_eq!(result.next_raw().unwrap(), Some(b"selected".to_vec()));
        assert_eq!(calls.borrow().len(), 1);
    }
}

#[test]
fn closest_replica_policy_with_labels_reaches_the_live_selector() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let mut runtime = InjectedQueryRuntime::new(transport_with_loader_calls(
        Rc::clone(&calls),
        [Ok(response(b"closest"))],
        [location(1, "a", "z", "one")],
        9001,
        Rc::clone(&loader_calls),
    ));
    let mut closest_with_labels = metadata("a", "z");
    closest_with_labels.replica_read = tidb_distsql::ReplicaReadType::Closest;
    closest_with_labels
        .match_store_labels
        .push(tidb_distsql::StoreLabel {
            key: "zone".to_owned(),
            value: "z1".to_owned(),
        });

    let mut result = runtime
        .select_with_runtime_stats(
            &transport_request(closest_with_labels),
            SelectInput::default(),
            QueryResultContext::new(Vec::new(), WarningCollector::new()),
            vec![1],
            2,
            true,
        )
        .expect("closest label policy is supported");
    assert_eq!(result.next_raw().unwrap(), Some(b"closest".to_vec()));
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(loader_calls.borrow().as_slice(), &[b"a".to_vec()]);
}
