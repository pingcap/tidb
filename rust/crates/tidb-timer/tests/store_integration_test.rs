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

//! Transcreation of Go `pkg/timer/store_intergartion_test.go` over the
//! in-memory timer store and notifier (Go `TestMemTimerStore`,
//! `TestMemNotifier`, and the memory-store half of `TestTimerStoreWithTimeZone`).
//!
//! Skipped, with exactly what each would need:
//! - `TestTableTimerStore`, the table-store halves of
//!   `TestTimerStoreWithTimeZone`, and `TestTableStoreManualTrigger` run CRUD,
//!   watch and the scheduling runtime against a live SQL backend through a
//!   session pool (`testkit.CreateMockStoreAndDomain`); no live TiDB exists on
//!   this side of the port.
//! - `TestEtcdNotifier` needs an embedded etcd integration cluster
//!   (`go.etcd.io/etcd/tests/v3/integration`); its `multiNotifier` shape — one
//!   notifier notifying while another watches — also presumes that shared
//!   etcd transport, so two in-memory notifiers cannot stand in for it.

use std::collections::HashMap;
use std::sync::mpsc::TryRecvError;
use std::time::Duration;

use tidb_timer::go_time::{GoTime, MINUTE};
use tidb_timer::mem_store::{new_mem_timer_watch_event_notifier, new_memory_timer_store};
use tidb_timer::store::{
    and, not, or, Context, OptionalVal, TimerCond, TimerStore, TimerUpdate,
    TimerWatchEventNotifier, WatchTimerEvent, WatchTimerEventType, WatchTimerResponse,
};
use tidb_timer::timer::{
    EventExtra, ManualRequest, SchedEventStatus, SchedPolicyType, TimerRecord, TimerSpec,
};
use tidb_timer::uuid::new_uuid_hex;
use tidb_timer::TimerError;
use tidb_util::timeutil::{load_location, set_system_tz, system_location, zone_name, TimeZone};

const ONE_MINUTE: Duration = Duration::from_secs(60);

/// Go `TestMemTimerStore`.
#[test]
fn test_mem_timer_store() {
    set_system_tz("Asia/Shanghai");

    let store = new_memory_timer_store();
    run_timer_store_test(&store);
    drop(store);

    let store = new_memory_timer_store();
    run_timer_store_watch_test(&store);
    store.close();
}

/// Go `TestMemNotifier`.
#[test]
fn test_mem_notifier() {
    let notifier = new_mem_timer_watch_event_notifier();
    run_notifier_test(notifier.clone());
    notifier.close();

}

fn run_timer_store_test(store: &TimerStore) {
    let ctx = Context::background();
    let timer = run_timer_store_insert_and_get(&ctx, store);
    run_timer_store_update(&ctx, store, &timer);
    run_timer_store_delete(&ctx, store, &timer);
    run_timer_store_insert_and_list(&ctx, store);
}

/// Go `runTimerStoreInsertAndGet`; returns the template record with its
/// assigned id/version/create-time filled in.
fn run_timer_store_insert_and_get(ctx: &Context, store: &TimerStore) -> TimerRecord {
    let records = store.list(ctx, None).unwrap();
    assert!(records.is_empty());

    let record_tpl = TimerSpec {
        namespace: "n1".to_string(),
        key: "/path/to/key".to_string(),
        time_zone: "".to_string(),
        sched_policy_type: SchedPolicyType::interval(),
        sched_policy_expr: "1h".to_string(),
        data: b"data1".to_vec(),
        ..Default::default()
    };

    // normal insert
    let record = TimerRecord {
        spec: record_tpl.clone_spec(),
        ..Default::default()
    };
    let id = store.create(ctx, &record).unwrap();
    assert!(!id.is_empty());
    assert_eq!(record.spec.namespace, record_tpl.namespace);
    let mut tpl = TimerRecord {
        spec: record_tpl.clone_spec(),
        id: id.clone(),
        location: Some(system_location()),
        event_status: SchedEventStatus::idle(),
        ..Default::default()
    };

    // get by id
    let got = store.get_by_id(ctx, &id).unwrap();
    let record = got;
    assert_eq!(record.id, tpl.id);
    assert_ne!(record.version, 0);
    tpl.version = record.version;
    assert!(!record.create_time.is_zero());
    tpl.create_time = record.create_time.clone();
    assert_eq!(tpl, record);

    // id not exist
    let err = store.get_by_id(ctx, "noexist").unwrap_err();
    assert_eq!(err, TimerError::TimerNotExist);

    // get by key
    let record = store.get_by_key(ctx, "n1", "/path/to/key").unwrap();
    assert_eq!(tpl, record);

    // key not exist
    let err = store.get_by_key(ctx, "n1", "noexist").unwrap_err();
    assert_eq!(err, TimerError::TimerNotExist);
    let err = store.get_by_key(ctx, "n2", "/path/to/ke").unwrap_err();
    assert_eq!(err, TimerError::TimerNotExist);

    // invalid insert
    let invalid = TimerRecord::default();
    let err = store.create(ctx, &invalid).unwrap_err();
    assert_eq!(err.to_string(), "field 'Namespace' should not be empty");

    let mut invalid = TimerRecord {
        spec: TimerSpec {
            namespace: "n1".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    let err = store.create(ctx, &invalid).unwrap_err();
    assert_eq!(err.to_string(), "field 'Key' should not be empty");

    invalid.spec.key = "k1".to_string();
    let err = store.create(ctx, &invalid).unwrap_err();
    assert_eq!(
        err.to_string(),
        "field 'SchedPolicyType' should not be empty"
    );

    invalid.spec.sched_policy_type = SchedPolicyType::interval();
    invalid.spec.sched_policy_expr = "1x".to_string();
    let err = store.create(ctx, &invalid).unwrap_err();
    assert_eq!(
        err.to_string(),
        "schedule event configuration is not valid: invalid schedule event expr '1x': unknown unit x"
    );

    invalid.spec.sched_policy_expr = "1h".to_string();
    invalid.spec.time_zone = "tidb".to_string();
    let err = store.create(ctx, &invalid).unwrap_err();
    assert!(
        err.to_string().contains("Unknown or incorrect time zone: 'tidb'"),
        "unexpected error: {err}"
    );

    tpl
}

/// Go `runTimerStoreUpdate`.
fn run_timer_store_update(ctx: &Context, store: &TimerStore, tpl: &TimerRecord) {
    // normal update
    let org_record = store.get_by_id(ctx, &tpl.id).unwrap();
    assert_eq!("1h", tpl.spec.sched_policy_expr);
    let event_id = new_uuid_hex();
    let event_start = GoTime::from_unix(1_234_567, 0);
    let watermark = GoTime::from_unix(7_890_123, 0);
    store
        .update(
            ctx,
            &tpl.id,
            &TimerUpdate {
                tags: OptionalVal::new(vec!["l1".to_string(), "l2".to_string()]),
                time_zone: OptionalVal::new("UTC".to_string()),
                sched_policy_expr: OptionalVal::new("2h".to_string()),
                manual_request: OptionalVal::new(ManualRequest {
                    manual_request_id: "req1".to_string(),
                    manual_request_time: GoTime::from_unix(123, 0),
                    manual_timeout: MINUTE,
                    manual_processed: true,
                    manual_event_id: "event1".to_string(),
                }),
                event_status: OptionalVal::new(SchedEventStatus::trigger()),
                event_id: OptionalVal::new(event_id.clone()),
                event_data: OptionalVal::new(b"eventdata1".to_vec()),
                event_start: OptionalVal::new(event_start.clone()),
                event_extra: OptionalVal::new(EventExtra {
                    event_manual_request_id: "req2".to_string(),
                    event_watermark: GoTime::from_unix(456, 0),
                }),
                watermark: OptionalVal::new(watermark.clone()),
                summary_data: OptionalVal::new(b"summary1".to_vec()),
                check_version: OptionalVal::new(org_record.version),
                check_event_id: OptionalVal::new(String::new()),
                ..Default::default()
            },
        )
        .unwrap();

    let record = store.get_by_id(ctx, &tpl.id).unwrap();
    assert!(record.version > tpl.version);
    let mut tpl = TimerRecord {
        spec: TimerSpec {
            time_zone: "UTC".to_string(),
            sched_policy_expr: "2h".to_string(),
            tags: vec!["l1".to_string(), "l2".to_string()],
            watermark: record.spec.watermark.clone(),
            ..tpl.spec.clone_spec()
        },
        version: record.version,
        event_status: SchedEventStatus::trigger(),
        event_id: event_id.clone(),
        event_data: b"eventdata1".to_vec(),
        event_start: record.event_start.clone(),
        summary_data: b"summary1".to_vec(),
        manual_request: ManualRequest {
            manual_request_id: "req1".to_string(),
            manual_request_time: GoTime::from_unix(123, 0),
            manual_timeout: MINUTE,
            manual_processed: true,
            manual_event_id: "event1".to_string(),
        },
        event_extra: EventExtra {
            event_manual_request_id: "req2".to_string(),
            event_watermark: GoTime::from_unix(456, 0),
        },
        create_time: record.create_time.clone(),
        location: Some(utc_zone()),
        ..tpl.clone_record()
    };
    assert_eq!(event_start.unix(), record.event_start.unix());
    assert_eq!(watermark.unix(), record.spec.watermark.unix());
    assert_eq!(tpl, record);

    // tags full update again
    store
        .update(
            ctx,
            &tpl.id,
            &TimerUpdate {
                tags: OptionalVal::new(vec!["l3".to_string()]),
                ..Default::default()
            },
        )
        .unwrap();
    let record = store.get_by_id(ctx, &tpl.id).unwrap();
    tpl.version = record.version;
    tpl.spec.tags = vec!["l3".to_string()];
    assert_eq!(tpl, record);

    // update manual request
    store
        .update(
            ctx,
            &tpl.id,
            &TimerUpdate {
                manual_request: OptionalVal::new(ManualRequest {
                    manual_request_id: "req3".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
    let record = store.get_by_id(ctx, &tpl.id).unwrap();
    tpl.version = record.version;
    tpl.manual_request = ManualRequest {
        manual_request_id: "req3".to_string(),
        ..Default::default()
    };
    assert_eq!(tpl, record);

    // update event extra
    store
        .update(
            ctx,
            &tpl.id,
            &TimerUpdate {
                event_extra: OptionalVal::new(EventExtra {
                    event_manual_request_id: "req4".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
    let record = store.get_by_id(ctx, &tpl.id).unwrap();
    tpl.version = record.version;
    tpl.event_extra = EventExtra {
        event_manual_request_id: "req4".to_string(),
        ..Default::default()
    };
    assert_eq!(tpl, record);

    // set some to empty
    let zero_time = GoTime::zero();
    store
        .update(
            ctx,
            &tpl.id,
            &TimerUpdate {
                time_zone: OptionalVal::new(String::new()),
                tags: OptionalVal::new(Vec::new()),
                manual_request: OptionalVal::new(ManualRequest::default()),
                event_status: OptionalVal::new(SchedEventStatus::idle()),
                event_id: OptionalVal::new(String::new()),
                event_data: OptionalVal::new(Vec::new()),
                event_start: OptionalVal::new(zero_time.clone()),
                event_extra: OptionalVal::new(EventExtra::default()),
                watermark: OptionalVal::new(zero_time.clone()),
                summary_data: OptionalVal::new(Vec::new()),
                ..Default::default()
            },
        )
        .unwrap();
    let record = store.get_by_id(ctx, &tpl.id).unwrap();
    tpl.spec.time_zone = String::new();
    tpl.location = Some(system_location());
    tpl.version = record.version;
    tpl.spec.tags = Vec::new();
    tpl.manual_request = ManualRequest::default();
    tpl.event_status = SchedEventStatus::idle();
    tpl.event_id = String::new();
    tpl.event_data = Vec::new();
    tpl.event_start = zero_time.clone();
    tpl.event_extra = EventExtra::default();
    tpl.spec.watermark = zero_time;
    tpl.summary_data = Vec::new();
    tpl.create_time = record
        .create_time
        .in_location(tpl.location.as_ref().unwrap());
    assert_eq!(tpl, record);

    // err check version
    let err = store
        .update(
            ctx,
            &tpl.id,
            &TimerUpdate {
                sched_policy_expr: OptionalVal::new("2h".to_string()),
                check_version: OptionalVal::new(record.version + 1),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(err.to_string(), "timer version not match");
    let record = store.get_by_id(ctx, &tpl.id).unwrap();
    assert_eq!(tpl, record);

    // err check event ID
    let err = store
        .update(
            ctx,
            &tpl.id,
            &TimerUpdate {
                sched_policy_expr: OptionalVal::new("2h".to_string()),
                check_event_id: OptionalVal::new("aabb".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(err.to_string(), "timer event id not match");
    let record = store.get_by_id(ctx, &tpl.id).unwrap();
    assert_eq!(tpl, record);

    // err update
    let err = store
        .update(
            ctx,
            &tpl.id,
            &TimerUpdate {
                sched_policy_expr: OptionalVal::new("2x".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "schedule event configuration is not valid: invalid schedule event expr '2x': unknown unit x"
    );
    let record = store.get_by_id(ctx, &tpl.id).unwrap();
    assert_eq!(tpl, record);

    let err = store
        .update(
            ctx,
            &tpl.id,
            &TimerUpdate {
                time_zone: OptionalVal::new("invalid".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert!(err.to_string().contains("Unknown or incorrect time zone: 'invalid'"));
    let record = store.get_by_id(ctx, &tpl.id).unwrap();
    assert_eq!(tpl, record);

    let err = store
        .update(
            ctx,
            &tpl.id,
            &TimerUpdate {
                time_zone: OptionalVal::new("tidb".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert!(err.to_string().contains("Unknown or incorrect time zone: 'tidb'"));
    let record = store.get_by_id(ctx, &tpl.id).unwrap();
    assert_eq!(tpl, record);
}

/// Go `runTimerStoreDelete`.
fn run_timer_store_delete(ctx: &Context, store: &TimerStore, tpl: &TimerRecord) {
    let exist = store.delete(ctx, &tpl.id).unwrap();
    assert!(exist);

    let err = store.get_by_id(ctx, &tpl.id).unwrap_err();
    assert_eq!(err, TimerError::TimerNotExist);

    let exist = store.delete(ctx, &tpl.id).unwrap();
    assert!(!exist);
}

/// Go `runTimerStoreInsertAndList`.
fn run_timer_store_insert_and_list(ctx: &Context, store: &TimerStore) {
    let records = store.list(ctx, None).unwrap();
    assert!(records.is_empty());

    let mut record_tpl1 = TimerRecord {
        spec: TimerSpec {
            namespace: "n1".to_string(),
            key: "/path/to/key1".to_string(),
            sched_policy_type: SchedPolicyType::interval(),
            sched_policy_expr: "1h".to_string(),
            ..Default::default()
        },
        event_status: SchedEventStatus::idle(),
        ..Default::default()
    };
    let mut record_tpl2 = TimerRecord {
        spec: TimerSpec {
            namespace: "n1".to_string(),
            key: "/path/to/key2".to_string(),
            sched_policy_type: SchedPolicyType::interval(),
            sched_policy_expr: "2h".to_string(),
            tags: vec!["tag1".to_string(), "tag2".to_string()],
            ..Default::default()
        },
        event_status: SchedEventStatus::idle(),
        ..Default::default()
    };
    let mut record_tpl3 = TimerRecord {
        spec: TimerSpec {
            namespace: "n2".to_string(),
            key: "/path/to/another".to_string(),
            sched_policy_type: SchedPolicyType::interval(),
            sched_policy_expr: "3h".to_string(),
            tags: vec!["tag2".to_string(), "tag3".to_string()],
            ..Default::default()
        },
        event_status: SchedEventStatus::idle(),
        ..Default::default()
    };

    for tpl in [&mut record_tpl1, &mut record_tpl2, &mut record_tpl3] {
        let id = store.create(ctx, tpl).unwrap();
        let got = store.get_by_id(ctx, &id).unwrap();
        tpl.id = got.id.clone();
        if tpl.spec.namespace == "n1" && tpl.spec.key.ends_with("key1") {
            tpl.location = Some(system_location());
        } else {
            tpl.location = got.location.clone();
        }
        tpl.version = got.version;
        tpl.create_time = got.create_time.clone();
        tpl.event_status = got.event_status.clone();
    }
    // The mem store stamps every created record with the system location.
    record_tpl2.location = Some(system_location());
    record_tpl3.location = Some(system_location());

    let check_list = |expected: &[&TimerRecord], list: &[TimerRecord]| {
        let mut expected_map: HashMap<&str, &TimerRecord> =
            expected.iter().map(|r| (r.id.as_str(), *r)).collect();
        for record in list {
            let tpl = expected_map
                .remove(record.id.as_str())
                .expect("unexpected record id");
            assert_eq!(tpl, record);
        }
        assert!(
            expected_map.is_empty(),
            "missing records: {:?}",
            expected_map.keys()
        );
    };

    let timers = store.list(ctx, None).unwrap();
    check_list(&[&record_tpl1, &record_tpl2, &record_tpl3], &timers);

    let cond = TimerCond {
        key: OptionalVal::new("/path/to/k".to_string()),
        key_prefix: true,
        ..Default::default()
    };
    let timers = store.list(ctx, Some(&cond)).unwrap();
    check_list(&[&record_tpl1, &record_tpl2], &timers);

    let timers = store
        .list(
            ctx,
            Some(&TimerCond {
                key: OptionalVal::new("/path/to/k".to_string()),
                ..Default::default()
            }),
        )
        .unwrap();
    check_list(&[], &timers);

    let timers = store
        .list(
            ctx,
            Some(&TimerCond {
                namespace: OptionalVal::new("n2".to_string()),
                key: OptionalVal::new("/path/to/key2".to_string()),
                ..Default::default()
            }),
        )
        .unwrap();
    check_list(&[], &timers);

    let timers = store
        .list(
            ctx,
            Some(&TimerCond {
                namespace: OptionalVal::new("n1".to_string()),
                key: OptionalVal::new("/path/to/key2".to_string()),
                ..Default::default()
            }),
        )
        .unwrap();
    check_list(&[&record_tpl2], &timers);

    let timers = store
        .list(
            ctx,
            Some(&TimerCond {
                tags: OptionalVal::new(vec!["tag2".to_string()]),
                ..Default::default()
            }),
        )
        .unwrap();
    check_list(&[&record_tpl2, &record_tpl3], &timers);

    let timers = store
        .list(
            ctx,
            Some(&TimerCond {
                tags: OptionalVal::new(vec!["tag1".to_string(), "tag3".to_string()]),
                ..Default::default()
            }),
        )
        .unwrap();
    check_list(&[], &timers);

    let timers = store
        .list(
            ctx,
            Some(&TimerCond {
                tags: OptionalVal::new(vec!["tag2".to_string(), "tag3".to_string()]),
                ..Default::default()
            }),
        )
        .unwrap();
    check_list(&[&record_tpl3], &timers);

    let timers = store
        .list(
            ctx,
            Some(&and(vec![
                std::sync::Arc::new(TimerCond {
                    namespace: OptionalVal::new("n1".to_string()),
                    ..Default::default()
                }),
                std::sync::Arc::new(TimerCond {
                    tags: OptionalVal::new(vec!["tag2".to_string()]),
                    ..Default::default()
                }),
            ])),
        )
        .unwrap();
    check_list(&[&record_tpl2], &timers);

    let timers = store
        .list(
            ctx,
            Some(&not(std::sync::Arc::new(and(vec![
                std::sync::Arc::new(TimerCond {
                    namespace: OptionalVal::new("n1".to_string()),
                    ..Default::default()
                }),
                std::sync::Arc::new(TimerCond {
                    tags: OptionalVal::new(vec!["tag2".to_string()]),
                    ..Default::default()
                }),
            ])))),
        )
        .unwrap();
    check_list(&[&record_tpl1, &record_tpl3], &timers);

    let timers = store
        .list(
            ctx,
            Some(&or(vec![
                std::sync::Arc::new(TimerCond {
                    key: OptionalVal::new("/path/to/key2".to_string()),
                    ..Default::default()
                }),
                std::sync::Arc::new(TimerCond {
                    tags: OptionalVal::new(vec!["tag3".to_string()]),
                    ..Default::default()
                }),
            ])),
        )
        .unwrap();
    check_list(&[&record_tpl2, &record_tpl3], &timers);

    let timers = store
        .list(
            ctx,
            Some(&not(std::sync::Arc::new(or(vec![
                std::sync::Arc::new(TimerCond {
                    key: OptionalVal::new("/path/to/key2".to_string()),
                    ..Default::default()
                }),
                std::sync::Arc::new(TimerCond {
                    tags: OptionalVal::new(vec!["tag3".to_string()]),
                    ..Default::default()
                }),
            ])))),
        )
        .unwrap();
    check_list(&[&record_tpl1], &timers);
}

/// Go `runTimerStoreWatchTest`.
fn run_timer_store_watch_test(store: &TimerStore) {
    assert!(store.watch_supported());
    let (ctx, cancel_fn) = Context::with_cancel();

    let timer = TimerRecord {
        spec: TimerSpec {
            namespace: "n1".to_string(),
            key: "/path/to/key".to_string(),
            sched_policy_type: SchedPolicyType::interval(),
            sched_policy_expr: "1h".to_string(),
            data: b"data1".to_vec(),
            ..Default::default()
        },
        ..Default::default()
    };

    let ch = store.watch(&ctx);
    let assert_watch_event = |ch: &std::sync::mpsc::Receiver<WatchTimerResponse>,
                              tp: Option<WatchTimerEventType>,
                              id: &str| {
        let resp = ch.recv_timeout(ONE_MINUTE).expect("no response");
        match tp {
            None => panic!("expected channel close, got {resp:?}"),
            Some(tp) => {
                assert_eq!(1, resp.events.len());
                assert_eq!(tp, resp.events[0].tp);
                assert_eq!(id, resp.events[0].timer_id);
            }
        }
    };

    let id = store.create(&ctx, &timer).unwrap();
    assert_watch_event(&ch, Some(WatchTimerEventType::Create), &id);

    store
        .update(
            &ctx,
            &id,
            &TimerUpdate {
                sched_policy_expr: OptionalVal::new("2h".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
    assert_watch_event(&ch, Some(WatchTimerEventType::Update), &id);

    let exit = store.delete(&ctx, &id).unwrap();
    assert!(exit);
    assert_watch_event(&ch, Some(WatchTimerEventType::Delete), &id);

    cancel_fn.cancel();
    // Go's per-watcher relay goroutine closes the channel as soon as the
    // watcher context is done. The Rust notifier drops a cancelled watcher's
    // sender on its next notification, so one more store mutation closes the
    // channel without ever delivering an event to it.
    let probe = TimerRecord {
        spec: TimerSpec {
            namespace: "probe".to_string(),
            key: "/path/to/probe".to_string(),
            sched_policy_type: SchedPolicyType::interval(),
            sched_policy_expr: "1h".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    let probe_id = store.create(&Context::background(), &probe).unwrap();
    match ch.try_recv() {
        Err(TryRecvError::Disconnected) => {}
        other => panic!("expected closed channel after cancel, got {other:?}"),
    }
    assert!(store.delete(&Context::background(), &probe_id).unwrap());
}

/// Go `runNotifierTest`.
fn run_notifier_test(notifier: std::sync::Arc<dyn TimerWatchEventNotifier>) {
    let check_watcher_events = |ch: &std::sync::mpsc::Receiver<WatchTimerResponse>,
                                events: &[WatchTimerEvent]| {
        let mut got_events: Vec<WatchTimerEvent> = Vec::with_capacity(events.len());
        loop {
            match ch.recv_timeout(Duration::from_secs(60)) {
                Ok(resp) => {
                    assert!(!resp.events.is_empty());
                    for event in resp.events {
                        got_events.push(event);
                    }
                    if got_events.len() >= events.len() {
                        break;
                    }
                }
                Err(_) => {
                    assert_eq!(events, got_events, "wait events timeout");
                    return;
                }
            }
        }
        assert_eq!(events, got_events);
    };

    let check_watcher_closed = |ch: &std::sync::mpsc::Receiver<WatchTimerResponse>, expect_no_data: bool| {
        // Reads until the channel closes; any buffered data drains first and
        // fails only when `expect_no_data` forbids it (Go's `checkNoData`).
        loop {
            match ch.recv_timeout(Duration::from_secs(60)) {
                Ok(resp) => {
                    assert!(!expect_no_data, "unexpected data: {resp:?}");
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    panic!("wait closed timeout")
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
    };

    let (ctx1, cancel_fn1) = Context::with_cancel();
    let watcher1 = notifier.watch(&ctx1);

    let (ctx2, cancel_fn2) = Context::with_cancel();
    let watcher2 = notifier.watch(&ctx2);

    std::thread::sleep(Duration::from_secs(1));
    notifier.notify(WatchTimerEventType::Create, "1");
    notifier.notify(WatchTimerEventType::Create, "2");
    notifier.notify(WatchTimerEventType::Update, "1");
    notifier.notify(WatchTimerEventType::Delete, "2");

    let expected_events = vec![
        WatchTimerEvent {
            tp: WatchTimerEventType::Create,
            timer_id: "1".to_string(),
        },
        WatchTimerEvent {
            tp: WatchTimerEventType::Create,
            timer_id: "2".to_string(),
        },
        WatchTimerEvent {
            tp: WatchTimerEventType::Update,
            timer_id: "1".to_string(),
        },
        WatchTimerEvent {
            tp: WatchTimerEventType::Delete,
            timer_id: "2".to_string(),
        },
    ];
    check_watcher_events(&watcher1, &expected_events);
    check_watcher_events(&watcher2, &expected_events);
    notifier.notify(WatchTimerEventType::Create, "3");
    notifier.notify(WatchTimerEventType::Update, "3");
    cancel_fn1.cancel();
    notifier.notify(WatchTimerEventType::Delete, "3");
    notifier.notify(WatchTimerEventType::Create, "4");
    let expected_events = vec![
        WatchTimerEvent {
            tp: WatchTimerEventType::Create,
            timer_id: "3".to_string(),
        },
        WatchTimerEvent {
            tp: WatchTimerEventType::Update,
            timer_id: "3".to_string(),
        },
        WatchTimerEvent {
            tp: WatchTimerEventType::Delete,
            timer_id: "3".to_string(),
        },
        WatchTimerEvent {
            tp: WatchTimerEventType::Create,
            timer_id: "4".to_string(),
        },
    ];
    check_watcher_closed(&watcher1, false);
    check_watcher_events(&watcher2, &expected_events);
    notifier.notify(WatchTimerEventType::Create, "5");
    notifier.close();
    let watcher3 = notifier.watch(&Context::background());
    std::thread::sleep(Duration::from_secs(1));
    notifier.notify(WatchTimerEventType::Delete, "4");
    let watcher4 = notifier.watch(&Context::background());
    std::thread::sleep(Duration::from_secs(1));
    check_watcher_closed(&watcher2, false);
    check_watcher_closed(&watcher3, true);
    check_watcher_closed(&watcher4, true);
    cancel_fn2.cancel();
}

/// Go `TestTimerStoreWithTimeZone`'s memory-store half; the table-store halves
/// need a live SQL session pool.
#[test]
fn test_timer_store_with_time_zone() {
    set_system_tz("Asia/Shanghai");
    let default_tz = zone_name(&system_location());

    // mem store
    let store = new_memory_timer_store();
    test_timer_store_with_time_zone_case(&store, &default_tz);
    store.close();
}

/// Go `testTimerStoreWithTimeZone`.
fn test_timer_store_with_time_zone_case(timer_store: &TimerStore, default_tz: &str) {
    let ctx = Context::background();
    // 2024-11-03 09:30:00 UTC is 2024-11-03 01:30:00 -08:00 in
    // `America/Los_Angeles`. It must NOT be read back as -07:00: DST makes
    // both spellings share the zone name.
    let time1 = GoTime::date(2024, 11, 3, 9, 30, 0, 0, &utc_zone());
    let time2 = GoTime::date(2024, 11, 3, 8, 30, 0, 0, &utc_zone());

    let id1 = timer_store
        .create(
            &ctx,
            &TimerRecord {
                spec: TimerSpec {
                    namespace: "default".to_string(),
                    key: "test1".to_string(),
                    sched_policy_type: SchedPolicyType::interval(),
                    sched_policy_expr: "1h".to_string(),
                    watermark: time1.clone(),
                    ..Default::default()
                },
                event_status: SchedEventStatus::trigger(),
                event_start: time2.clone(),
                ..Default::default()
            },
        )
        .unwrap();

    let id2 = timer_store
        .create(
            &ctx,
            &TimerRecord {
                spec: TimerSpec {
                    namespace: "default".to_string(),
                    key: "test2".to_string(),
                    sched_policy_type: SchedPolicyType::interval(),
                    sched_policy_expr: "1h".to_string(),
                    watermark: time2.clone(),
                    ..Default::default()
                },
                event_status: SchedEventStatus::trigger(),
                event_start: time1.clone(),
                ..Default::default()
            },
        )
        .unwrap();

    // create case
    let timer1 = timer_store.get_by_id(&ctx, &id1).unwrap();
    assert_eq!(time1.unix(), timer1.spec.watermark.unix());
    assert_eq!(time2.unix(), timer1.event_start.unix());
    check_timer_record_location(&timer1, default_tz);

    let timer2 = timer_store.get_by_id(&ctx, &id2).unwrap();
    assert_eq!(time2.unix(), timer2.spec.watermark.unix());
    assert_eq!(time1.unix(), timer2.event_start.unix());
    check_timer_record_location(&timer2, default_tz);

    // update time
    timer_store
        .update(
            &ctx,
            &id1,
            &TimerUpdate {
                watermark: OptionalVal::new(time2.clone()),
                event_start: OptionalVal::new(time1.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    timer_store
        .update(
            &ctx,
            &id2,
            &TimerUpdate {
                watermark: OptionalVal::new(time1.clone()),
                event_start: OptionalVal::new(time2.clone()),
                ..Default::default()
            },
        )
        .unwrap();

    let timer1 = timer_store.get_by_id(&ctx, &id1).unwrap();
    assert_eq!(time2.unix(), timer1.spec.watermark.unix());
    assert_eq!(time1.unix(), timer1.event_start.unix());
    check_timer_record_location(&timer1, default_tz);

    let timer2 = timer_store.get_by_id(&ctx, &id2).unwrap();
    assert_eq!(time1.unix(), timer2.spec.watermark.unix());
    assert_eq!(time2.unix(), timer2.event_start.unix());
    check_timer_record_location(&timer2, default_tz);

    // update timezone
    timer_store
        .update(
            &ctx,
            &id1,
            &TimerUpdate {
                time_zone: OptionalVal::new("Europe/Berlin".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
    let timer1 = timer_store.get_by_id(&ctx, &id1).unwrap();
    assert_eq!(time2.unix(), timer1.spec.watermark.unix());
    assert_eq!(time1.unix(), timer1.event_start.unix());
    check_timer_record_location(&timer1, "Europe/Berlin");

    timer_store
        .update(
            &ctx,
            &id1,
            &TimerUpdate {
                time_zone: OptionalVal::new(String::new()),
                ..Default::default()
            },
        )
        .unwrap();
    let timer1 = timer_store.get_by_id(&ctx, &id1).unwrap();
    assert_eq!(time2.unix(), timer1.spec.watermark.unix());
    assert_eq!(time1.unix(), timer1.event_start.unix());
    check_timer_record_location(&timer1, default_tz);
}

/// Go `checkTimerRecordLocation`. Go compares location pointers
/// (`require.Same`); Rust compares zone values.
fn check_timer_record_location(record: &TimerRecord, tz: &str) {
    let location = record.location.as_ref().expect("location is set");
    assert_eq!(tz, zone_name(location));
    assert_eq!(location, record.spec.watermark.location());
    assert_eq!(location, record.create_time.location());
    if !record.event_start.is_zero() {
        assert_eq!(location, record.event_start.location());
    }
}

/// Go's `time.UTC` location.
fn utc_zone() -> TimeZone {
    load_location("UTC").unwrap()
}
