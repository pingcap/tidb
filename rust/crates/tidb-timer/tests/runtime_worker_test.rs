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

//! Transcreation of Go `pkg/timer/runtime/worker_test.go`.
//!
//! Go's `sendWorkerRequestAndCheckResp` uses a non-blocking send into the
//! worker's buffered channel and then waits one second on an unbuffered
//! response channel; both spellings survive verbatim on
//! `std::sync::mpsc::sync_channel`.

#[path = "common/mod.rs"]
mod common;

use std::collections::VecDeque;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_timer::client::{new_default_timer_client, with_set_summary_data, with_set_watermark};
use tidb_timer::go_time::{GoTime, MINUTE, SECOND};
use tidb_timer::hook::{Hook, PreSchedEventResult};
use tidb_timer::mem_store::new_memory_timer_store;
use tidb_timer::runtime::worker::{
    new_hook_worker, new_hook_worker_with_retry, new_rendezvous_resp_channel, HookFn, HookWorker,
    TriggerEventRequest, TriggerEventResponse, WORKER_EVENT_DEFAULT_RETRY_INTERVAL,
};
use tidb_timer::runtime::WaitGroup;
use tidb_timer::store::{Context, OptionalVal, TimerStore, TimerUpdate};
use tidb_timer::timer::{
    EventExtra, ManualRequest, SchedEventStatus, SchedPolicyType, TimerRecord, TimerSpec,
};
use tidb_timer::uuid::new_uuid_hex;
use tidb_timer::TimerClient;

use common::{MockHook, MockStoreCore, Outcome};

const ONE_SECOND: Duration = Duration::from_secs(1);
const FIVE_SECONDS: Duration = Duration::from_secs(5);

/// Go `onlyOnceNewHook`.
fn only_once_new_hook(hook: Arc<MockHook>) -> HookFn {
    let slot = Mutex::new(Some(hook as Arc<dyn Hook>));
    Arc::new(move || {
        slot.lock()
            .unwrap()
            .take()
            .or_else(|| panic!("hook function called more than once"))
    })
}

/// One queued `newHookFn` result: the hook to hand back, or a panic.
type HookOutcome = Outcome<Option<Arc<dyn Hook>>>;

/// Go's `newHookFn` mock: a queue of "return this hook" / "panic" outcomes.
struct MockHookFn {
    outcomes: Mutex<VecDeque<HookOutcome>>,
    standing: Mutex<Option<&'static str>>,
    calls: Mutex<usize>,
    first_call: Arc<common::Event>,
}

impl MockHookFn {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            outcomes: Mutex::new(VecDeque::new()),
            standing: Mutex::new(None),
            calls: Mutex::new(0),
            first_call: Arc::new(common::Event::default()),
        })
    }

    fn queue_panic(&self, message: &'static str) {
        self.outcomes
            .lock()
            .unwrap()
            .push_back(Outcome::Panic(message));
    }

    fn queue_hook(&self, hook: Arc<MockHook>) {
        self.outcomes
            .lock()
            .unwrap()
            .push_back(Outcome::Ok(Some(hook as Arc<dyn Hook>)));
    }

    fn always_panic(&self, message: &'static str) {
        *self.standing.lock().unwrap() = Some(message);
    }

    fn assert_drained(&self) {
        assert!(
            self.outcomes.lock().unwrap().is_empty(),
            "hook function has unfulfilled expectations"
        );
    }

    fn into_fn(self: &Arc<Self>) -> HookFn {
        let this = Arc::clone(self);
        Arc::new(move || {
            *this.calls.lock().unwrap() += 1;
            this.first_call.signal();
            let queued = this.outcomes.lock().unwrap().pop_front();
            match queued {
                Some(Outcome::Ok(hook)) => hook,
                Some(Outcome::Panic(message)) => panic!("{message}"),
                Some(_) => None,
                None => match *this.standing.lock().unwrap() {
                    Some(message) => panic!("{message}"),
                    None => None,
                },
            }
        })
    }
}

/// Go `prepareTimer`.
fn prepare_timer(cli: &dyn TimerClient) -> TimerRecord {
    let ctx = Context::background();
    let now = GoTime::now();
    let timer = cli
        .create_timer(
            &ctx,
            TimerSpec {
                key: "key1".to_string(),
                data: b"data1".to_vec(),
                sched_policy_type: SchedPolicyType::interval(),
                sched_policy_expr: "1m".to_string(),
                hook_class: "h1".to_string(),
                enable: true,
                ..Default::default()
            },
        )
        .unwrap();

    let watermark = now.add(-MINUTE);
    cli.update_timer(
        &ctx,
        &timer.id,
        &[
            with_set_watermark(watermark.clone()),
            with_set_summary_data(b"summary1".to_vec()),
        ],
    )
    .unwrap();

    let timer = cli.get_timer_by_id(&ctx, &timer.id).unwrap();
    assert!(!timer.id.is_empty());
    assert_eq!(timer.spec.data, b"data1");
    assert_eq!(timer.spec.sched_policy_type, SchedPolicyType::interval());
    assert_eq!(timer.spec.sched_policy_expr, "1m");
    assert_eq!(timer.spec.hook_class, "h1");
    assert!(timer.spec.enable);
    assert_eq!(timer.spec.watermark.unix(), watermark.unix());
    assert_eq!(timer.summary_data, b"summary1");
    assert!(timer.version > 0);
    assert!(timer.event_id.is_empty());
    assert_eq!(timer.event_status, SchedEventStatus::idle());
    assert_eq!(timer.event_data.len(), 0);
    assert!(timer.event_start.is_zero());
    timer
}

/// Go `getAndCheckTriggeredTimer`.
fn get_and_check_triggered_timer(
    cli: &dyn TimerClient,
    old_timer: &TimerRecord,
    event_id: &str,
    event_data: &[u8],
) -> TimerRecord {
    let timer = cli
        .get_timer_by_id(&Context::background(), &old_timer.id)
        .unwrap();
    assert_eq!(timer.event_status, SchedEventStatus::trigger());
    assert_eq!(timer.event_id, event_id);
    assert_eq!(timer.event_data, event_data);
    assert!(timer.version > old_timer.version);

    let mut expected = old_timer.clone_record();
    expected.event_id = timer.event_id.clone();
    expected.event_data = timer.event_data.clone();
    expected.event_status = timer.event_status.clone();
    expected.event_start = timer.event_start.clone();
    expected.version = timer.version;
    expected.event_extra = EventExtra {
        event_watermark: expected.spec.watermark.clone(),
        ..Default::default()
    };
    assert_eq!(expected, timer);
    timer
}

/// Go `sendWorkerRequestAndCheckResp`.
fn send_worker_request_and_check_resp(
    worker: &HookWorker,
    request: TriggerEventRequest,
    resp_rx: &Receiver<TriggerEventResponse>,
    check: impl FnOnce(&TriggerEventResponse),
) {
    worker
        .ch
        .try_send(Some(request))
        .expect("worker channel should accept the request");
    let response = resp_rx
        .recv_timeout(ONE_SECOND)
        .expect("worker should answer within a second");
    check(&response);
    assert!(resp_rx.try_recv().is_err(), "only one response is expected");
}

fn check_worker_counters(worker: &HookWorker, expected: [u64; 6]) {
    let counters = &worker.counters;
    assert_eq!(
        [
            counters.trigger_request.val(),
            counters.on_pre_sched_event.val(),
            counters.on_pre_sched_event_err.val(),
            counters.on_pre_sched_event_delay.val(),
            counters.on_sched_event.val(),
            counters.on_sched_event_err.val(),
        ],
        expected
    );
}

#[test]
fn test_worker_start_stop() {
    let wait_group = Arc::new(WaitGroup::new());
    let (ctx, cancel) = Context::with_cancel();

    let hook = MockHook::new();
    let _worker = new_hook_worker(
        &ctx,
        &wait_group,
        "g1",
        "h1",
        Some(only_once_new_hook(Arc::clone(&hook))),
        None,
    );
    hook.started.wait_done(ONE_SECOND);
    assert_eq!(hook.on_start.call_count(), 1);

    hook.stopped.check_not_done(Duration::from_millis(100));
    cancel.cancel();
    hook.stopped.wait_done(ONE_SECOND);
    wait_group.wait();
    assert_eq!(hook.on_stop.call_count(), 1);
}

#[test]
fn test_worker_process_idle_timer_success() {
    let wait_group = Arc::new(WaitGroup::new());
    let (ctx, cancel) = Context::with_cancel();

    let store = new_memory_timer_store();
    let cli = new_default_timer_client(store.clone());
    let (resp_tx, resp_rx) = new_rendezvous_resp_channel();
    let timer = prepare_timer(&cli);

    let hook = MockHook::new();
    let worker = new_hook_worker(
        &ctx,
        &wait_group,
        "g1",
        "h1",
        Some(only_once_new_hook(Arc::clone(&hook))),
        None,
    );

    let event_id = new_uuid_hex();
    hook.on_pre_sched_event
        .once(Outcome::Ok(PreSchedEventResult {
            event_data: b"eventdata".to_vec(),
            ..Default::default()
        }));
    hook.on_sched_event.once(Outcome::Ok(()));

    let request = TriggerEventRequest {
        event_id: event_id.clone(),
        timer: Some(timer.clone_record()),
        store: store.clone(),
        resp: Some(resp_tx),
    };

    send_worker_request_and_check_resp(&worker, request, &resp_rx, |resp| {
        assert!(resp.success);
        assert_eq!(resp.timer_id, timer.id);
        assert_eq!(resp.event_id, event_id);
        assert!(!resp.retry_after.present());
        let new_timer = resp.new_timer_record.get().unwrap().as_ref().unwrap();
        let final_timer = get_and_check_triggered_timer(&cli, &timer, &event_id, b"eventdata");
        assert_eq!(new_timer, &final_timer);
    });

    // the hook saw the pre-trigger record and then the triggered one
    let pre = hook.pre_sched_events.lock().unwrap();
    assert_eq!(pre.len(), 1);
    assert_eq!(pre[0].0, event_id);
    assert_eq!(pre[0].1, timer);
    drop(pre);
    let sched = hook.sched_events.lock().unwrap();
    assert_eq!(sched.len(), 1);
    assert_eq!(sched[0].0, event_id);
    drop(sched);

    check_worker_counters(&worker, [1, 1, 0, 0, 1, 0]);
    cancel.cancel();
    hook.stopped.wait_done(ONE_SECOND);
    wait_group.wait();
    hook.assert_expectations();
    store.close();
}

#[test]
fn test_worker_process_triggered_timer_success() {
    let wait_group = Arc::new(WaitGroup::new());
    let (ctx, cancel) = Context::with_cancel();

    let store = new_memory_timer_store();
    let cli = new_default_timer_client(store.clone());
    let (resp_tx, resp_rx) = new_rendezvous_resp_channel();
    let timer = prepare_timer(&cli);
    let event_start = GoTime::now();
    let event_id = new_uuid_hex();
    store
        .update(
            &ctx,
            &timer.id,
            &TimerUpdate {
                event_id: OptionalVal::new(event_id.clone()),
                event_status: OptionalVal::new(SchedEventStatus::trigger()),
                event_data: OptionalVal::new(b"eventdata".to_vec()),
                event_start: OptionalVal::new(event_start),
                event_extra: OptionalVal::new(EventExtra {
                    event_watermark: timer.spec.watermark.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
    let timer = get_and_check_triggered_timer(&cli, &timer, &event_id, b"eventdata");

    let hook = MockHook::new();
    let worker = new_hook_worker(
        &ctx,
        &wait_group,
        "g1",
        "h1",
        Some(only_once_new_hook(Arc::clone(&hook))),
        None,
    );
    hook.on_sched_event.once(Outcome::Ok(()));

    let request = TriggerEventRequest {
        event_id: event_id.clone(),
        timer: Some(timer.clone_record()),
        store: store.clone(),
        resp: Some(resp_tx),
    };

    send_worker_request_and_check_resp(&worker, request, &resp_rx, |resp| {
        assert!(resp.success);
        assert_eq!(resp.timer_id, timer.id);
        assert_eq!(resp.event_id, event_id);
        assert!(!resp.retry_after.present());
        assert_eq!(
            resp.new_timer_record.get().unwrap().as_ref().unwrap(),
            &timer
        );
    });

    check_worker_counters(&worker, [1, 0, 0, 0, 1, 0]);
    cancel.cancel();
    hook.stopped.wait_done(ONE_SECOND);
    wait_group.wait();
    hook.assert_expectations();
    store.close();
}

#[test]
fn test_worker_process_delay_or_err() {
    let wait_group = Arc::new(WaitGroup::new());
    let (ctx, cancel) = Context::with_cancel();

    let store = new_memory_timer_store();
    let cli = new_default_timer_client(store.clone());
    let (resp_tx, resp_rx) = new_rendezvous_resp_channel();
    let mut timer = prepare_timer(&cli);

    let hook = MockHook::new();
    let worker = new_hook_worker(
        &ctx,
        &wait_group,
        "g1",
        "h1",
        Some(only_once_new_hook(Arc::clone(&hook))),
        None,
    );

    let event_id = new_uuid_hex();
    // Go builds `request` once and only reassigns `request.timer` at the two
    // points marked below, so the stale record keeps being sent while the
    // stored one moves on; this mirror keeps that distinction.
    let mut request_timer = timer.clone_record();
    let make_request = |store: TimerStore, timer: &TimerRecord| TriggerEventRequest {
        event_id: event_id.clone(),
        timer: Some(timer.clone_record()),
        store,
        resp: Some(resp_tx.clone()),
    };

    // invalid requests should be discarded
    worker.ch.try_send(None).unwrap();
    let mut invalid = make_request(store.clone(), &request_timer);
    invalid.timer = None;
    worker.ch.try_send(Some(invalid)).unwrap();
    let mut invalid = make_request(store.clone(), &request_timer);
    invalid.resp = None;
    worker.ch.try_send(Some(invalid)).unwrap();

    // Delay 5 seconds
    hook.on_pre_sched_event
        .once(Outcome::Ok(PreSchedEventResult {
            delay: 5 * SECOND,
            ..Default::default()
        }));
    send_worker_request_and_check_resp(
        &worker,
        make_request(store.clone(), &request_timer),
        &resp_rx,
        |resp| {
            assert!(!resp.success);
            assert_eq!(resp.timer_id, timer.id);
            assert_eq!(resp.event_id, event_id);
            assert_eq!(resp.retry_after.get(), Some(&(5 * SECOND)));
            assert!(!resp.new_timer_record.present());
        },
    );
    check_worker_counters(&worker, [1, 1, 0, 1, 0, 0]);

    // OnPreSchedEvent error
    hook.on_pre_sched_event.once(Outcome::Err("mockErr"));
    send_worker_request_and_check_resp(
        &worker,
        make_request(store.clone(), &request_timer),
        &resp_rx,
        |resp| {
            assert!(!resp.success);
            assert_eq!(
                resp.retry_after.get(),
                Some(&WORKER_EVENT_DEFAULT_RETRY_INTERVAL)
            );
            assert!(!resp.new_timer_record.present());
        },
    );
    check_worker_counters(&worker, [2, 2, 1, 1, 0, 0]);

    assert_eq!(cli.get_timer_by_id(&ctx, &timer.id).unwrap(), timer);

    // update timer unknown error
    let (mock_core, mock_store) = MockStoreCore::new();
    hook.on_pre_sched_event
        .once(Outcome::Ok(PreSchedEventResult::default()));
    mock_core.on_update.once(Outcome::Err("mockErr"));
    send_worker_request_and_check_resp(
        &worker,
        make_request(mock_store.clone(), &request_timer),
        &resp_rx,
        |resp| {
            assert!(!resp.success);
            assert_eq!(
                resp.retry_after.get(),
                Some(&WORKER_EVENT_DEFAULT_RETRY_INTERVAL)
            );
            assert!(!resp.new_timer_record.present());
        },
    );
    check_worker_counters(&worker, [3, 3, 1, 1, 0, 0]);

    // timer meta changed then get record error
    hook.on_pre_sched_event
        .once(Outcome::Ok(PreSchedEventResult::default()));
    mock_core
        .on_update
        .once(Outcome::Sentinel(tidb_timer::TimerError::VersionNotMatch));
    mock_core.on_list.once(Outcome::Err("mockErr"));
    send_worker_request_and_check_resp(
        &worker,
        make_request(mock_store.clone(), &request_timer),
        &resp_rx,
        |resp| {
            assert!(!resp.success);
            assert_eq!(
                resp.retry_after.get(),
                Some(&WORKER_EVENT_DEFAULT_RETRY_INTERVAL)
            );
            assert!(!resp.new_timer_record.present());
        },
    );
    check_worker_counters(&worker, [4, 4, 1, 1, 0, 0]);

    // timer event updated then get record error
    hook.on_pre_sched_event
        .once(Outcome::Ok(PreSchedEventResult::default()));
    mock_core.on_update.once(Outcome::Ok(()));
    mock_core.on_list.once(Outcome::Err("mockErr"));
    send_worker_request_and_check_resp(
        &worker,
        make_request(mock_store.clone(), &request_timer),
        &resp_rx,
        |resp| {
            assert!(!resp.success);
            assert_eq!(
                resp.retry_after.get(),
                Some(&WORKER_EVENT_DEFAULT_RETRY_INTERVAL)
            );
            assert!(!resp.new_timer_record.present());
        },
    );
    check_worker_counters(&worker, [5, 5, 1, 1, 0, 0]);

    // timer event updated then get record return nil
    hook.on_pre_sched_event
        .once(Outcome::Ok(PreSchedEventResult::default()));
    mock_core.on_update.once(Outcome::Ok(()));
    mock_core.on_list.once(Outcome::Ok(Vec::new()));
    send_worker_request_and_check_resp(
        &worker,
        make_request(mock_store.clone(), &request_timer),
        &resp_rx,
        |resp| {
            assert!(!resp.success);
            assert!(!resp.retry_after.present());
            assert_eq!(resp.new_timer_record.get(), Some(&None));
        },
    );
    check_worker_counters(&worker, [6, 6, 1, 1, 0, 0]);

    // timer event updated then get record return different eventID
    let mut another = timer.clone_record();
    another.version += 2;
    another.event_status = SchedEventStatus::trigger();
    another.event_id = "anothereventid".to_string();
    another.event_start = GoTime::now();
    hook.on_pre_sched_event
        .once(Outcome::Ok(PreSchedEventResult::default()));
    mock_core.on_update.once(Outcome::Ok(()));
    mock_core
        .on_list
        .once(Outcome::Ok(vec![another.clone_record()]));
    send_worker_request_and_check_resp(
        &worker,
        make_request(mock_store.clone(), &request_timer),
        &resp_rx,
        |resp| {
            assert!(!resp.success);
            assert!(!resp.retry_after.present());
            assert_eq!(
                resp.new_timer_record.get().unwrap().as_ref().unwrap(),
                &another
            );
        },
    );
    check_worker_counters(&worker, [7, 7, 1, 1, 0, 0]);
    mock_core.assert_expectations();

    // timer meta changed
    cli.update_timer(
        &ctx,
        &timer.id,
        &[tidb_timer::with_set_sched_expr(
            SchedPolicyType::interval(),
            "2m",
        )],
    )
    .unwrap();
    let updated = cli.get_timer_by_id(&ctx, &timer.id).unwrap();
    assert_eq!(updated.spec.sched_policy_expr, "2m");
    assert!(updated.version > timer.version);
    timer = updated;

    hook.on_pre_sched_event
        .once(Outcome::Ok(PreSchedEventResult::default()));
    send_worker_request_and_check_resp(
        &worker,
        make_request(store.clone(), &request_timer),
        &resp_rx,
        |resp| {
            assert!(!resp.success);
            assert!(!resp.retry_after.present());
            assert_eq!(
                resp.new_timer_record.get().unwrap().as_ref().unwrap(),
                &timer
            );
        },
    );
    check_worker_counters(&worker, [8, 8, 1, 1, 0, 0]);

    // Go: `request.timer = timer`
    request_timer = timer.clone_record();

    // OnSchedEvent error
    hook.on_pre_sched_event
        .once(Outcome::Ok(PreSchedEventResult {
            event_data: b"eventdata".to_vec(),
            ..Default::default()
        }));
    hook.on_sched_event.once(Outcome::Err("mockErr"));
    let mut final_timer = None;
    send_worker_request_and_check_resp(
        &worker,
        make_request(store.clone(), &request_timer),
        &resp_rx,
        |resp| {
            assert!(!resp.success);
            assert_eq!(
                resp.retry_after.get(),
                Some(&WORKER_EVENT_DEFAULT_RETRY_INTERVAL)
            );
            final_timer = resp.new_timer_record.get().unwrap().clone();
        },
    );
    timer = get_and_check_triggered_timer(&cli, &timer, &event_id, b"eventdata");
    assert_eq!(final_timer.as_ref(), Some(&timer));
    // Go: `request.timer = timer`
    request_timer = timer.clone_record();
    check_worker_counters(&worker, [9, 9, 1, 1, 1, 1]);

    // Event closed before trigger
    cli.close_timer_event(&ctx, &timer.id, &event_id, &[])
        .unwrap();
    let mut final_timer = None;
    send_worker_request_and_check_resp(
        &worker,
        make_request(store.clone(), &request_timer),
        &resp_rx,
        |resp| {
            assert!(!resp.success);
            assert!(!resp.retry_after.present());
            final_timer = resp.new_timer_record.get().unwrap().clone();
        },
    );
    timer = cli.get_timer_by_id(&ctx, &timer.id).unwrap();
    assert!(timer.event_id.is_empty());
    assert_eq!(final_timer.as_ref(), Some(&timer));

    // Timer deleted
    assert!(cli.delete_timer(&ctx, &timer.id).unwrap());
    send_worker_request_and_check_resp(
        &worker,
        make_request(store.clone(), &request_timer),
        &resp_rx,
        |resp| {
            assert!(!resp.success);
            assert!(!resp.retry_after.present());
            assert_eq!(resp.new_timer_record.get(), Some(&None));
        },
    );

    // Timer deleted after OnPreSchedEvent
    let timer = prepare_timer(&cli);
    request_timer = timer.clone_record();
    assert!(cli.delete_timer(&ctx, &timer.id).unwrap());
    hook.on_pre_sched_event
        .once(Outcome::Ok(PreSchedEventResult {
            event_data: b"eventdata".to_vec(),
            ..Default::default()
        }));
    send_worker_request_and_check_resp(
        &worker,
        make_request(store.clone(), &request_timer),
        &resp_rx,
        |resp| {
            assert!(!resp.success);
            assert!(!resp.retry_after.present());
            assert_eq!(resp.new_timer_record.get(), Some(&None));
        },
    );

    cancel.cancel();
    hook.stopped.wait_done(ONE_SECOND);
    wait_group.wait();
    hook.assert_expectations();
    store.close();
}

#[test]
fn test_worker_process_manual_request() {
    let wait_group = Arc::new(WaitGroup::new());
    let (ctx, cancel) = Context::with_cancel();

    let store = new_memory_timer_store();
    let cli = new_default_timer_client(store.clone());
    let (resp_tx, resp_rx) = new_rendezvous_resp_channel();
    let timer = prepare_timer(&cli);
    store
        .update(
            &ctx,
            &timer.id,
            &TimerUpdate {
                manual_request: OptionalVal::new(ManualRequest {
                    manual_request_id: "req1".to_string(),
                    manual_request_time: GoTime::now().add(-MINUTE),
                    manual_timeout: 59 * SECOND,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
    let mut timer = cli.get_timer_by_id(&ctx, &timer.id).unwrap();

    let hook = MockHook::new();
    let worker = new_hook_worker(
        &ctx,
        &wait_group,
        "g1",
        "h1",
        Some(only_once_new_hook(Arc::clone(&hook))),
        None,
    );

    // manual trigger timeout and update api returns error
    let (mock_core, mock_store) = MockStoreCore::new();
    mock_core.on_update.once(Outcome::Err("mockErr"));
    let mut event_id = new_uuid_hex();
    send_worker_request_and_check_resp(
        &worker,
        TriggerEventRequest {
            event_id: event_id.clone(),
            timer: Some(timer.clone_record()),
            store: mock_store.clone(),
            resp: Some(resp_tx.clone()),
        },
        &resp_rx,
        |resp| {
            assert!(!resp.success);
            assert_eq!(resp.timer_id, timer.id);
            assert_eq!(resp.event_id, event_id);
            assert_eq!(
                resp.retry_after.get(),
                Some(&WORKER_EVENT_DEFAULT_RETRY_INTERVAL)
            );
            assert!(!resp.new_timer_record.present());
        },
    );
    mock_core.assert_expectations();

    // manual trigger timeout and list api returns error
    event_id = new_uuid_hex();
    mock_core.on_update.once(Outcome::Ok(()));
    mock_core.on_list.once(Outcome::Err("mockErr"));
    send_worker_request_and_check_resp(
        &worker,
        TriggerEventRequest {
            event_id: event_id.clone(),
            timer: Some(timer.clone_record()),
            store: mock_store.clone(),
            resp: Some(resp_tx.clone()),
        },
        &resp_rx,
        |resp| {
            assert!(!resp.success);
            assert_eq!(
                resp.retry_after.get(),
                Some(&WORKER_EVENT_DEFAULT_RETRY_INTERVAL)
            );
            assert!(!resp.new_timer_record.present());
        },
    );
    mock_core.assert_expectations();

    // manual trigger timeout
    event_id = new_uuid_hex();
    send_worker_request_and_check_resp(
        &worker,
        TriggerEventRequest {
            event_id: event_id.clone(),
            timer: Some(timer.clone_record()),
            store: store.clone(),
            resp: Some(resp_tx.clone()),
        },
        &resp_rx,
        |resp| {
            assert!(!resp.success);
            assert!(!resp.retry_after.present());
            let record = resp.new_timer_record.get().unwrap().as_ref().unwrap();
            let got = cli.get_timer_by_id(&ctx, &timer.id).unwrap();
            assert_eq!(&got, record);

            let mut expected = timer.clone_record();
            expected.version = got.version;
            expected.manual_request.manual_processed = true;
            assert_eq!(got, expected);
        },
    );

    // manual trigger success
    let request_id = cli.manual_trigger_event(&ctx, &timer.id).unwrap();
    timer = cli.get_timer_by_id(&ctx, &timer.id).unwrap();
    event_id = new_uuid_hex();
    hook.on_pre_sched_event
        .once(Outcome::Ok(PreSchedEventResult::default()));
    hook.on_sched_event.once(Outcome::Ok(()));
    send_worker_request_and_check_resp(
        &worker,
        TriggerEventRequest {
            event_id: event_id.clone(),
            timer: Some(timer.clone_record()),
            store: store.clone(),
            resp: Some(resp_tx.clone()),
        },
        &resp_rx,
        |resp| {
            assert!(resp.success);
            assert!(!resp.retry_after.present());
            let record = resp.new_timer_record.get().unwrap().as_ref().unwrap();
            let got = cli.get_timer_by_id(&ctx, &timer.id).unwrap();
            assert_eq!(&got, record);

            let mut expected = timer.clone_record();
            expected.version = got.version;
            expected.manual_request.manual_processed = true;
            expected.manual_request.manual_event_id = event_id.clone();
            expected.event_id = event_id.clone();
            expected.event_start = got.event_start.clone();
            expected.event_status = SchedEventStatus::trigger();
            expected.event_extra = EventExtra {
                event_manual_request_id: request_id.clone(),
                event_watermark: expected.spec.watermark.clone(),
            };
            assert_eq!(got, expected);
        },
    );

    cancel.cancel();
    hook.stopped.wait_done(ONE_SECOND);
    wait_group.wait();
    hook.assert_expectations();
    store.close();
}

#[test]
fn test_hook_worker_loop_panic_recover() {
    let wait_group = Arc::new(WaitGroup::new());
    let (ctx, cancel) = Context::with_cancel();

    let hook_fn = MockHookFn::new();

    // create hook function panic
    hook_fn.queue_panic("hook func panic1");
    hook_fn.queue_panic("hook func panic2");

    // hook start panic
    let hook1 = MockHook::new();
    hook1.on_start.once(Outcome::Panic("hook1 start panic"));
    hook_fn.queue_hook(Arc::clone(&hook1));

    // create hook panic again
    hook_fn.queue_panic("hook func panic3");

    // hook start and stop panic
    let hook2 = MockHook::new();
    hook2.on_start.once(Outcome::Panic("hook2 start panic"));
    hook_fn.queue_hook(Arc::clone(&hook2));

    // hook start normal and process request
    let hook3 = MockHook::new();
    hook_fn.queue_hook(Arc::clone(&hook3));
    hook3
        .on_pre_sched_event
        .once(Outcome::Ok(PreSchedEventResult::default()));
    hook3.on_sched_event.once(Outcome::Ok(()));

    let worker = new_hook_worker_with_retry(
        &ctx,
        &wait_group,
        "g1",
        "h1",
        Some(hook_fn.into_fn()),
        None,
        1_000_000,
        5 * SECOND,
    );
    hook3.started.wait_done(FIVE_SECONDS);

    // check can process request normally after send request
    let timer1 = TimerRecord {
        id: "1".to_string(),
        version: 1,
        event_status: SchedEventStatus::idle(),
        ..Default::default()
    };
    let mut timer2 = timer1.clone_record();
    timer2.version = 2;
    timer2.event_id = "event1".to_string();
    timer2.event_status = SchedEventStatus::trigger();

    let (mock_core, mock_store) = MockStoreCore::new();
    mock_core.on_update.once(Outcome::Ok(()));
    mock_core
        .on_list
        .once(Outcome::Ok(vec![timer2.clone_record()]));
    let (resp_tx, resp_rx) = new_rendezvous_resp_channel();
    send_worker_request_and_check_resp(
        &worker,
        TriggerEventRequest {
            event_id: timer2.event_id.clone(),
            timer: Some(timer1.clone_record()),
            store: mock_store,
            resp: Some(resp_tx),
        },
        &resp_rx,
        |resp| {
            assert!(resp.success);
            assert_eq!(resp.timer_id, timer1.id);
            assert_eq!(resp.event_id, "event1");
            assert_eq!(
                resp.new_timer_record.get().unwrap().as_ref().unwrap(),
                &timer2
            );
        },
    );
    hook_fn.assert_drained();
    assert_eq!(hook1.on_stop.call_count(), 1);
    hook3.assert_expectations();

    // hook3 stop panic but worker can still stop
    hook3.on_stop.once(Outcome::Panic("hook3 stop panic"));
    cancel.cancel();
    wait_group.wait();

    // continues to panic will not affect worker stop immediately
    let wait_group = Arc::new(WaitGroup::new());
    let (ctx, cancel) = Context::with_cancel();
    let hook_fn = MockHookFn::new();
    hook_fn.always_panic("hook func panic");
    let first_call = Arc::clone(&hook_fn.first_call);
    new_hook_worker_with_retry(
        &ctx,
        &wait_group,
        "g1",
        "h1",
        Some(hook_fn.into_fn()),
        None,
        MINUTE,
        5 * SECOND,
    );
    first_call.wait_done(FIVE_SECONDS);
    std::thread::sleep(Duration::from_millis(1));
    cancel.cancel();
    wait_group.wait();
}

#[test]
fn test_hook_worker_loop_handle_request_panic_recover() {
    let wait_group = Arc::new(WaitGroup::new());
    let (ctx, cancel) = Context::with_cancel();

    // we set the request retry wait with a long delay to check the fail message
    // is responded immediately, ignoring the delay, when a panic happens.
    let hook = MockHook::new();
    let hook_fn = MockHookFn::new();
    hook_fn.queue_hook(Arc::clone(&hook));

    let worker = new_hook_worker_with_retry(
        &ctx,
        &wait_group,
        "g1",
        "h1",
        Some(hook_fn.into_fn()),
        None,
        10 * SECOND,
        MINUTE,
    );
    hook.started.wait_done(FIVE_SECONDS);
    hook_fn.assert_drained();

    let timer1 = TimerRecord {
        id: "1".to_string(),
        version: 1,
        event_status: SchedEventStatus::idle(),
        ..Default::default()
    };
    let mut timer2 = timer1.clone_record();
    timer2.version = 2;
    timer2.event_id = "event1".to_string();
    timer2.event_status = SchedEventStatus::trigger();

    let (mock_core, mock_store) = MockStoreCore::new();
    mock_core.on_update.once(Outcome::Ok(()));
    mock_core
        .on_list
        .once(Outcome::Ok(vec![timer2.clone_record()]));
    let (resp_tx, resp_rx) = new_rendezvous_resp_channel();
    let make_request = || TriggerEventRequest {
        event_id: timer2.event_id.clone(),
        timer: Some(timer1.clone_record()),
        store: mock_store.clone(),
        resp: Some(resp_tx.clone()),
    };

    // OnPreSchedEvent panicked
    hook.on_pre_sched_event
        .once(Outcome::Panic("OnPreSchedEvent panic"));
    send_worker_request_and_check_resp(&worker, make_request(), &resp_rx, |resp| {
        assert!(!resp.success);
        assert_eq!(resp.timer_id, timer1.id);
        assert_eq!(resp.event_id, "event1");
        assert!(!resp.new_timer_record.present());
        assert_eq!(
            resp.retry_after.get(),
            Some(&WORKER_EVENT_DEFAULT_RETRY_INTERVAL)
        );
    });

    // OnSchedEvent panicked
    hook.on_pre_sched_event
        .once(Outcome::Ok(PreSchedEventResult::default()));
    hook.on_sched_event
        .once(Outcome::Panic("OnSchedEvent panic"));
    send_worker_request_and_check_resp(&worker, make_request(), &resp_rx, |resp| {
        assert!(!resp.success);
        assert_eq!(resp.event_id, "event1");
        assert!(!resp.new_timer_record.present());
        assert_eq!(
            resp.retry_after.get(),
            Some(&WORKER_EVENT_DEFAULT_RETRY_INTERVAL)
        );
    });

    cancel.cancel();
    wait_group.wait();
    hook.assert_expectations();
}
