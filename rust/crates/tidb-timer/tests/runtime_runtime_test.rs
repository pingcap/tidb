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

//! Transcreation of Go `pkg/timer/runtime/runtime_test.go`.
//!
//! Go's package-level interval variables are process-global here too, and Go
//! runs a package's tests one at a time while Rust runs them on parallel
//! threads. Every test in this file therefore takes [`SERIAL`] first, which
//! restores Go's one-at-a-time guarantee without a `--test-threads=1` flag.

mod common;

use std::collections::HashSet;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tidb_timer::client::TimerClient;
use tidb_timer::go_time::{GoTime, MINUTE, SECOND};
use tidb_timer::hook::Hook;
use tidb_timer::mem_store::new_memory_timer_store;
use tidb_timer::runtime::cache::RuntimeProcStatus;
use tidb_timer::runtime::worker::{HookWorker, TriggerEventRequest, TriggerEventResponse};
use tidb_timer::runtime::{
    max_trigger_event_interval, min_trigger_event_interval, new_timer_runtime_builder,
    re_watch_interval, retry_busy_worker_interval, set_batch_process_watch_resp_interval,
    set_in_test_intervals, set_max_trigger_event_interval, set_min_trigger_event_interval,
    set_re_watch_interval, TimerGroupRuntime,
};
use tidb_timer::store::{
    Context, OperatorTp, OptionalVal, TimerCond, WatchTimerEvent, WatchTimerEventType,
    WatchTimerResponse,
};
use tidb_timer::timer::{SchedEventStatus, SchedPolicyType, TimerRecord, TimerSpec};

use common::{new_test_timer, CondShape, Event, MockHook, MockStoreCore, Outcome};

/// Serializes the tests, as Go's per-package test runner does.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|err| err.into_inner())
}

/// Restores the package tunables Go's tests reset with `defer`.
struct RestoreIntervals {
    min_trigger: i64,
    max_trigger: i64,
    re_watch: i64,
    batch: i64,
}

impl RestoreIntervals {
    fn capture() -> Self {
        Self {
            min_trigger: min_trigger_event_interval(),
            max_trigger: max_trigger_event_interval(),
            re_watch: re_watch_interval(),
            batch: tidb_timer::runtime::batch_process_watch_resp_interval(),
        }
    }
}

impl Drop for RestoreIntervals {
    fn drop(&mut self) {
        set_min_trigger_event_interval(self.min_trigger);
        set_max_trigger_event_interval(self.max_trigger);
        set_re_watch_interval(self.re_watch);
        set_batch_process_watch_resp_interval(self.batch);
    }
}

fn channel_only_worker(
    runtime: &TimerGroupRuntime,
    capacity: usize,
) -> Receiver<Option<TriggerEventRequest>> {
    let (sender, receiver): (SyncSender<Option<TriggerEventRequest>>, _) = sync_channel(capacity);
    runtime.workers().insert(
        "hook1".to_string(),
        Arc::new(HookWorker::channel_only(sender)),
    );
    receiver
}

fn fixed_now(now: GoTime) -> Arc<dyn Fn() -> GoTime + Send + Sync> {
    Arc::new(move || now.clone())
}

#[test]
fn test_runtime_start_stop() {
    let _guard = serial();
    let _restore = RestoreIntervals::capture();
    set_in_test_intervals();

    let ctx = Context::background();
    let store = new_memory_timer_store();
    let cli = tidb_timer::client::new_default_timer_client(store.clone());
    cli.create_timer(
        &ctx,
        TimerSpec {
            namespace: "n1".to_string(),
            key: "k1".to_string(),
            sched_policy_type: SchedPolicyType::interval(),
            sched_policy_expr: "1m".to_string(),
            enable: true,
            hook_class: "hook1".to_string(),
            ..Default::default()
        },
    )
    .unwrap();

    let timer_processed = Arc::new(Event::default());
    let hook = MockHook::new();
    hook.set_sched_done(Arc::clone(&timer_processed));
    hook.on_pre_sched_event
        .always(Outcome::Ok(Default::default()));
    hook.on_sched_event.always(Outcome::Ok(()));

    let factory_calls = Arc::new(Mutex::new(Vec::<String>::new()));
    let factory_hook = Arc::clone(&hook);
    let recorded = Arc::clone(&factory_calls);
    let runtime = new_timer_runtime_builder("g1", store.clone())
        .register_hook_factory(
            "hook1",
            Box::new(move |hook_class, _cli| {
                recorded.lock().unwrap().push(hook_class.to_string());
                Arc::clone(&factory_hook) as Arc<dyn Hook>
            }),
        )
        .build();

    runtime.start();
    assert!(runtime.running());
    timer_processed.wait_done(Duration::from_secs(60));
    runtime.stop();
    assert!(!runtime.running());
    assert_eq!(factory_calls.lock().unwrap().as_slice(), ["hook1"]);
    assert_eq!(hook.on_start.call_count(), 1);
    assert_eq!(hook.on_stop.call_count(), 1);
    store.close();
}

#[test]
fn test_ensure_worker() {
    let _guard = serial();

    let store = new_memory_timer_store();
    let hook = MockHook::new();
    let factory_hook = Arc::clone(&hook);
    let runtime = new_timer_runtime_builder("g1", store.clone())
        .register_hook_factory(
            "hook1",
            Box::new(move |_class, _cli| Arc::clone(&factory_hook) as Arc<dyn Hook>),
        )
        .build();
    runtime.init_ctx();

    let worker1 = runtime.ensure_worker("hook1").expect("hook1 is registered");
    hook.started.wait_done(Duration::from_secs(60));

    let worker2 = runtime.ensure_worker("hook1").expect("hook1 is registered");
    assert!(Arc::ptr_eq(&worker1, &worker2));

    assert!(runtime.ensure_worker("hook2").is_none());

    runtime.stop();
    assert_eq!(hook.on_start.call_count(), 1);
    assert_eq!(hook.on_stop.call_count(), 1);
    store.close();
}

#[test]
fn test_try_trigger_timer() {
    let _guard = serial();

    let now = GoTime::now();
    let store = new_memory_timer_store();
    let runtime = new_timer_runtime_builder("g1", store.clone()).build();
    runtime.set_now_func(fixed_now(now.clone()));
    runtime.init_ctx();

    // t1: idle timer
    let t1 = new_test_timer("t1", "1m", now.add(-60 * MINUTE));
    runtime.cache().update_timer(&t1);

    // t2: not idle timer, it will be triggered even if the timer is disabled
    let mut t2 = new_test_timer("t2", "1h", now.clone());
    t2.event_status = SchedEventStatus::trigger();
    t2.event_id = "event2".to_string();
    t2.event_start = now.add(-60 * MINUTE);
    t2.spec.enable = false;
    runtime.cache().update_timer(&t2);

    // t3: next event time after now
    let mut t3 = new_test_timer("t3", "10m", now.clone());
    runtime.cache().update_timer(&t3);
    runtime
        .cache()
        .update_next_try_trigger_time(&t3.id, now.add(-10 * MINUTE));

    // t4: next try trigger time after now
    let t4 = new_test_timer("t4", "1m", now.add(-60 * MINUTE));
    runtime.cache().update_timer(&t4);
    runtime
        .cache()
        .update_next_try_trigger_time(&t4.id, now.add(SECOND));

    let t5 = new_test_timer("t5", "5m", now.add(-10 * MINUTE));
    runtime.cache().update_timer(&t5);

    // t6/t7: the worker channel is full when these are emitted
    let t6 = new_test_timer("t6", "6m", now.add(-10 * MINUTE));
    runtime.cache().update_timer(&t6);
    let t7 = new_test_timer("t7", "6m", now.add(-10 * MINUTE));
    runtime.cache().update_timer(&t7);

    // t8: triggering
    let t8 = new_test_timer("t8", "1m", now.add(-120 * MINUTE));
    runtime.cache().update_timer(&t8);
    runtime
        .cache()
        .set_timer_proc_status(&t8.id, RuntimeProcStatus::Triggering, "event8");

    // t9: wait close
    let mut t9 = new_test_timer("t9", "1m", now.add(-120 * MINUTE));
    t9.event_status = SchedEventStatus::trigger();
    t9.event_id = "event9".to_string();
    t9.event_start = now.add(-120 * MINUTE);
    runtime.cache().update_timer(&t9);
    runtime
        .cache()
        .set_timer_proc_status(&t9.id, RuntimeProcStatus::WaitTriggerClose, "event9");

    let receiver = channel_only_worker(&runtime, 3);
    runtime.try_trigger_timer_events();

    {
        let cache = runtime.cache();
        assert_eq!(cache.items["t1"].proc_status, RuntimeProcStatus::Triggering);
        assert!(!cache.items["t1"].trigger_event_id.is_empty());

        assert_eq!(cache.items["t2"].proc_status, RuntimeProcStatus::Triggering);
        assert_eq!(cache.items["t2"].trigger_event_id, "event2");

        assert_eq!(cache.items["t3"].proc_status, RuntimeProcStatus::Idle);
        assert!(cache.items["t3"].trigger_event_id.is_empty());

        assert_eq!(cache.items["t4"].proc_status, RuntimeProcStatus::Idle);
        assert!(cache.items["t4"].trigger_event_id.is_empty());

        assert_eq!(cache.items["t5"].proc_status, RuntimeProcStatus::Triggering);
        assert!(!cache.items["t5"].trigger_event_id.is_empty());

        for id in ["t6", "t7"] {
            assert_eq!(cache.items[id].proc_status, RuntimeProcStatus::Idle);
            assert!(cache.items[id].trigger_event_id.is_empty());
            assert_eq!(
                cache.items[id].next_try_trigger_time,
                now.add(retry_busy_worker_interval())
            );
        }

        assert_eq!(cache.items["t8"].proc_status, RuntimeProcStatus::Triggering);
        assert_eq!(
            cache.items["t9"].proc_status,
            RuntimeProcStatus::WaitTriggerClose
        );
    }

    let consume_and_verify = |expected: Option<&TimerRecord>| match receiver.try_recv() {
        Ok(request) => {
            let expected = expected.expect("should not reach here");
            let request = request.expect("a request, never nil");
            assert_eq!(request.timer.as_ref().unwrap(), expected);
            assert_eq!(
                runtime.cache().items[&expected.id].trigger_event_id,
                request.event_id
            );
        }
        Err(_) => assert!(expected.is_none(), "should not reach here"),
    };

    consume_and_verify(Some(&t2));
    consume_and_verify(Some(&t1));
    consume_and_verify(Some(&t5));
    consume_and_verify(None);

    // t3: has a processed manual request
    t3 = t3.clone_record();
    t3.version += 1;
    t3.manual_request = tidb_timer::timer::ManualRequest {
        manual_request_id: "req1".to_string(),
        manual_request_time: now.clone(),
        manual_timeout: MINUTE,
        manual_processed: true,
        manual_event_id: "event1".to_string(),
    };
    runtime.cache().update_timer(&t3);
    runtime.try_trigger_timer_events();
    consume_and_verify(None);

    // t3: has a not processed manual request but the timer is disabled
    t3.spec.enable = false;
    t3.manual_request = tidb_timer::timer::ManualRequest {
        manual_request_id: "req2".to_string(),
        manual_request_time: now.clone(),
        manual_timeout: MINUTE,
        ..Default::default()
    };
    t3.version += 1;
    runtime.cache().update_timer(&t3);
    runtime.try_trigger_timer_events();
    consume_and_verify(None);

    // t3: has a not processed manual request
    t3.spec.enable = true;
    t3.version += 1;
    runtime.cache().update_timer(&t3);
    runtime.try_trigger_timer_events();
    consume_and_verify(Some(&t3));

    store.close();
}

#[test]
fn test_try_trigger_time_priority() {
    let _guard = serial();

    let now = GoTime::now();
    let store = new_memory_timer_store();
    let runtime = new_timer_runtime_builder("g1", store.clone()).build();
    runtime.set_now_func(fixed_now(now.clone()));
    runtime.init_ctx();
    let _receiver = channel_only_worker(&runtime, 2);

    let t1 = new_test_timer("t1", "1m", now.add(-60 * MINUTE));
    runtime.cache().update_timer(&t1);
    runtime
        .cache()
        .update_next_try_trigger_time(&t1.id, now.add(-3 * MINUTE));

    let t2 = new_test_timer("t2", "1m", now.add(-120 * MINUTE));
    runtime.cache().update_timer(&t2);
    runtime
        .cache()
        .update_next_try_trigger_time(&t2.id, now.add(-2 * MINUTE));

    let mut t3 = new_test_timer("t3", "1h", now.clone());
    t3.event_status = SchedEventStatus::trigger();
    t3.event_id = "event2".to_string();
    t3.event_start = now.add(-MINUTE);
    t3.spec.enable = false;
    runtime.cache().update_timer(&t3);

    let t4 = new_test_timer("t4", "1m", now.add(-600 * MINUTE));
    runtime.cache().update_timer(&t4);
    runtime
        .cache()
        .update_next_try_trigger_time(&t4.id, now.add(MINUTE));

    // nextEventTime: t3 (nil) < t4 < t2 < t1; the priority is by nextEventTime
    // so the most delayed timer is triggered first. t4 is not scheduled because
    // its next try trigger time is after now, so with a capacity of 2 the
    // channel takes t3 and t2.
    runtime.try_trigger_timer_events();
    let cache = runtime.cache();
    assert_eq!(cache.items["t2"].proc_status, RuntimeProcStatus::Triggering);
    assert_eq!(cache.items["t3"].proc_status, RuntimeProcStatus::Triggering);
    assert_eq!(cache.items["t1"].proc_status, RuntimeProcStatus::Idle);
    assert_eq!(cache.items["t4"].proc_status, RuntimeProcStatus::Idle);
    drop(cache);
    store.close();
}

#[test]
fn test_handle_hook_worker_response() {
    let _guard = serial();

    let now = GoTime::now();
    let store = new_memory_timer_store();
    let runtime = new_timer_runtime_builder("g1", store.clone()).build();
    runtime.set_now_func(fixed_now(now.clone()));
    runtime.init_ctx();

    let t1 = new_test_timer("t1", "1m", now.add(-60 * MINUTE));
    runtime.cache().update_timer(&t1);
    runtime
        .cache()
        .set_timer_proc_status(&t1.id, RuntimeProcStatus::Triggering, "event1");

    // success response
    runtime.cache().remove_timer(&t1.id);
    runtime.cache().update_timer(&t1);
    let mut triggered = t1.clone_record();
    triggered.event_id = "event1".to_string();
    triggered.event_status = SchedEventStatus::trigger();
    triggered.event_start = now.clone();
    triggered.event_data = b"data1".to_vec();
    triggered.version += 1;
    runtime.handle_worker_response(&TriggerEventResponse {
        success: true,
        timer_id: t1.id.clone(),
        event_id: "event1".to_string(),
        new_timer_record: OptionalVal::new(Some(triggered.clone_record())),
        retry_after: OptionalVal::default(),
    });
    {
        let cache = runtime.cache();
        let item = &cache.items[&t1.id];
        assert_eq!(item.timer.as_ref().unwrap(), &triggered);
        assert_eq!(item.proc_status, RuntimeProcStatus::WaitTriggerClose);
        assert_eq!(item.trigger_event_id, "event1");
        assert_eq!(cache.wait_close_timer_ids.len(), 1);
        assert!(cache.wait_close_timer_ids.contains(&t1.id));
    }

    // not success response with timer removed
    runtime.cache().remove_timer(&t1.id);
    runtime.cache().update_timer(&t1);
    runtime.handle_worker_response(&TriggerEventResponse {
        success: false,
        timer_id: t1.id.clone(),
        event_id: "event1".to_string(),
        new_timer_record: OptionalVal::new(None),
        retry_after: OptionalVal::default(),
    });
    assert!(!runtime.cache().has_timer(&t1.id));
    assert_eq!(runtime.cache().wait_close_timer_ids.len(), 0);

    // not success response with timer changed
    runtime.cache().remove_timer(&t1.id);
    runtime.cache().update_timer(&t1);
    let mut new_timer = t1.clone_record();
    new_timer.version += 1;
    new_timer.spec.watermark = now.add(SECOND);
    runtime.handle_worker_response(&TriggerEventResponse {
        success: false,
        timer_id: t1.id.clone(),
        event_id: "event1".to_string(),
        new_timer_record: OptionalVal::new(Some(new_timer.clone_record())),
        retry_after: OptionalVal::default(),
    });
    {
        let cache = runtime.cache();
        let item = &cache.items[&t1.id];
        assert_eq!(item.timer.as_ref().unwrap(), &new_timer);
        assert_eq!(item.proc_status, RuntimeProcStatus::Idle);
        assert_eq!(item.trigger_event_id, "");
        assert_eq!(cache.wait_close_timer_ids.len(), 0);
    }

    // not success response with retry after
    runtime.cache().remove_timer(&t1.id);
    runtime.cache().update_timer(&t1);
    runtime.handle_worker_response(&TriggerEventResponse {
        success: false,
        timer_id: t1.id.clone(),
        event_id: "event1".to_string(),
        new_timer_record: OptionalVal::default(),
        retry_after: OptionalVal::new(12 * SECOND),
    });
    {
        let cache = runtime.cache();
        let item = &cache.items[&t1.id];
        assert_eq!(item.timer.as_ref().unwrap(), &t1);
        assert_eq!(item.proc_status, RuntimeProcStatus::Idle);
        assert_eq!(item.trigger_event_id, "");
        assert_eq!(item.next_try_trigger_time, now.add(12 * SECOND));
        assert_eq!(cache.wait_close_timer_ids.len(), 0);
    }
    store.close();
}

#[test]
fn test_next_try_trigger_duration() {
    let _guard = serial();
    let _restore = RestoreIntervals::capture();
    set_min_trigger_event_interval(SECOND);

    let base = GoTime::now();
    let now = Arc::new(Mutex::new(base.clone()));
    let store = new_memory_timer_store();
    let runtime = new_timer_runtime_builder("g1", store.clone()).build();
    let now_reader = Arc::clone(&now);
    runtime.set_now_func(Arc::new(move || now_reader.lock().unwrap().clone()));
    runtime.init_ctx();

    let t1 = new_test_timer("t1", "0.1m", base.clone());
    runtime.cache().update_timer(&t1);
    runtime
        .cache()
        .set_timer_proc_status(&t1.id, RuntimeProcStatus::Triggering, "event1");

    let t2 = new_test_timer("t2", "1.5m", base.clone());
    runtime.cache().update_timer(&t2);

    let t3 = new_test_timer("t3", "2m", base.clone());
    runtime.cache().update_timer(&t3);

    let current = || now.lock().unwrap().clone();
    let set_now = |value: GoTime| *now.lock().unwrap() = value;

    assert_eq!(runtime.get_next_try_trigger_duration(&base), 60 * SECOND);

    set_now(base.add(70 * SECOND));
    assert_eq!(
        runtime.get_next_try_trigger_duration(&current()),
        20 * SECOND
    );

    set_now(current().add(19 * SECOND + 500_000_000));
    assert_eq!(
        runtime.get_next_try_trigger_duration(&current().add(-SECOND)),
        500_000_000
    );
    assert_eq!(runtime.get_next_try_trigger_duration(&current()), SECOND);
    assert_eq!(
        runtime.get_next_try_trigger_duration(&current().add(100_000_000)),
        SECOND
    );

    set_now(current().add(60 * MINUTE));
    assert_eq!(
        runtime.get_next_try_trigger_duration(&GoTime::from_unix_milli(0)),
        0
    );
    store.close();
}

fn build_refresh_fixture(
    runtime: &TimerGroupRuntime,
    trigger_at: &[usize],
    count: usize,
) -> Vec<TimerRecord> {
    let mut timers = Vec::with_capacity(count);
    for index in 0..count {
        let mut timer = new_test_timer(&format!("t{index}"), "1m", GoTime::now());
        let mut proc_status = RuntimeProcStatus::Idle;
        if trigger_at.contains(&index) {
            timer.event_status = SchedEventStatus::trigger();
            timer.event_start = GoTime::now();
            timer.event_id = format!("event{}", index + 1);
            proc_status = RuntimeProcStatus::WaitTriggerClose;
        }
        if index == 6 {
            proc_status = RuntimeProcStatus::Triggering;
        }
        runtime.cache().update_timer(&timer);
        let event_id = timer.event_id.clone();
        runtime
            .cache()
            .set_timer_proc_status(&timer.id, proc_status, &event_id);
        timers.push(timer);
    }
    timers
}

#[test]
fn test_full_refresh_timers() {
    let _guard = serial();

    let (mock_core, mock_store) = MockStoreCore::new();
    let runtime = new_timer_runtime_builder("g1", mock_store).build();
    let cond = Arc::new(TimerCond {
        namespace: OptionalVal::new("n1".to_string()),
        ..Default::default()
    });
    runtime.set_cond(cond.clone());
    runtime.init_ctx();

    let timers = build_refresh_fixture(&runtime, &[2, 4], 7);

    let mut t0_new = timers[0].clone_record();
    t0_new.version += 1;
    let mut t2_new = timers[2].clone_record();
    t2_new.version += 1;
    let mut t4_new = timers[4].clone_record();
    t4_new.event_status = SchedEventStatus::idle();
    t4_new.event_id = String::new();
    t4_new.version += 1;
    let mut t6_new = timers[6].clone_record();
    t6_new.version += 1;

    mock_core.on_list.once(Outcome::Err("mockErr"));
    assert_eq!(runtime.full_refresh_timer_counter().val(), 0);
    runtime.full_refresh_timers();
    assert_eq!(runtime.full_refresh_timer_counter().val(), 1);
    assert_eq!(runtime.cache().items.len(), 7);

    mock_core.on_list.once(Outcome::Ok(vec![
        t0_new.clone_record(),
        timers[1].clone_record(),
        t2_new.clone_record(),
        t4_new.clone_record(),
        t6_new.clone_record(),
    ]));
    runtime.full_refresh_timers();
    assert_eq!(runtime.full_refresh_timer_counter().val(), 2);
    mock_core.assert_expectations();

    let cache = runtime.cache();
    assert_eq!(cache.items.len(), 5);
    assert_eq!(cache.items["t0"].timer.as_ref().unwrap(), &t0_new);
    assert_eq!(cache.items["t1"].timer.as_ref().unwrap(), &timers[1]);
    assert_eq!(cache.items["t2"].timer.as_ref().unwrap(), &t2_new);
    assert_eq!(
        cache.items["t2"].proc_status,
        RuntimeProcStatus::WaitTriggerClose
    );
    assert_eq!(cache.items["t4"].timer.as_ref().unwrap(), &t4_new);
    assert_eq!(cache.items["t4"].proc_status, RuntimeProcStatus::Idle);
    assert_eq!(cache.items["t6"].timer.as_ref().unwrap(), &t6_new);
    assert_eq!(cache.items["t6"].proc_status, RuntimeProcStatus::Triggering);
    drop(cache);

    // Go asserts the store saw the runtime's own condition, unwrapped.
    let conds = mock_core.list_conds.lock().unwrap();
    assert_eq!(
        conds[0],
        Some(CondShape::Timer(TimerCond {
            namespace: OptionalVal::new("n1".to_string()),
            ..Default::default()
        }))
    );
}

/// Checks the `AND(cond, OR(id-conds...))` shape Go inspects inside its `Run`
/// hook, and returns the ids the `OR` arm carried.
fn assert_timer_ids_cond(shape: &CondShape, runtime_cond: &TimerCond) -> HashSet<String> {
    let CondShape::Op { op, not, children } = shape else {
        panic!("expected an operator");
    };
    assert_eq!(*op, OperatorTp::And);
    assert!(!*not);
    assert_eq!(children.len(), 2);
    assert_eq!(children[0], CondShape::Timer(runtime_cond.clone()));

    let CondShape::Op {
        op,
        not,
        children: id_children,
    } = &children[1]
    else {
        panic!("expected an operator");
    };
    assert_eq!(*op, OperatorTp::Or);
    assert!(!*not);

    let mut ids = HashSet::new();
    for child in id_children {
        let CondShape::Timer(cond) = child else {
            panic!("expected a timer cond");
        };
        let id = cond.id.get().expect("the id is set").clone();
        assert!(cond.fields_set(&["ID"]).is_empty());
        ids.insert(id);
    }
    assert_eq!(ids.len(), id_children.len());
    ids
}

#[test]
fn test_batch_handler_watch_responses() {
    let _guard = serial();

    let (mock_core, mock_store) = MockStoreCore::new();
    let runtime = new_timer_runtime_builder("g1", mock_store).build();
    let cond = TimerCond {
        namespace: OptionalVal::new("n1".to_string()),
        ..Default::default()
    };
    runtime.set_cond(Arc::new(cond.clone()));
    runtime.init_ctx();

    let timers = build_refresh_fixture(&runtime, &[2], 7);

    let t10 = new_test_timer("t10", "1m", GoTime::now());
    let mut t2_new = timers[2].clone_record();
    t2_new.event_status = SchedEventStatus::idle();
    t2_new.event_id = String::new();
    t2_new.version += 1;
    let mut t6_new = timers[6].clone_record();
    t6_new.version += 1;

    mock_core.on_list.once(Outcome::Ok(vec![
        t2_new.clone_record(),
        t6_new.clone_record(),
        t10.clone_record(),
    ]));

    assert_eq!(runtime.partial_refresh_timer_counter().val(), 0);
    runtime.batch_handle_watch_responses(&[
        WatchTimerResponse {
            events: vec![
                WatchTimerEvent {
                    tp: WatchTimerEventType::Delete,
                    timer_id: "t0".to_string(),
                },
                WatchTimerEvent {
                    tp: WatchTimerEventType::Create,
                    timer_id: "t10".to_string(),
                },
            ],
        },
        WatchTimerResponse {
            events: vec![
                WatchTimerEvent {
                    tp: WatchTimerEventType::Update,
                    timer_id: "t2".to_string(),
                },
                WatchTimerEvent {
                    tp: WatchTimerEventType::Delete,
                    timer_id: "t5".to_string(),
                },
            ],
        },
    ]);
    assert_eq!(runtime.partial_refresh_timer_counter().val(), 1);
    mock_core.assert_expectations();

    let conds = mock_core.list_conds.lock().unwrap();
    let ids = assert_timer_ids_cond(conds[0].as_ref().unwrap(), &cond);
    assert_eq!(ids, HashSet::from(["t10".to_string(), "t2".to_string()]));
    drop(conds);

    let cache = runtime.cache();
    assert_eq!(cache.items.len(), 6);
    assert!(!cache.has_timer("t0"));
    assert!(!cache.has_timer("t5"));
    assert_eq!(cache.items["t10"].timer.as_ref().unwrap(), &t10);
    assert_eq!(cache.items["t10"].proc_status, RuntimeProcStatus::Idle);
    assert_eq!(cache.items["t2"].timer.as_ref().unwrap(), &t2_new);
    assert_eq!(cache.items["t2"].proc_status, RuntimeProcStatus::Idle);
    assert_eq!(cache.items["t6"].timer.as_ref().unwrap(), &t6_new);
    assert_eq!(cache.items["t6"].proc_status, RuntimeProcStatus::Triggering);
}

#[test]
fn test_close_waiting_close_timers() {
    let _guard = serial();

    let (mock_core, mock_store) = MockStoreCore::new();
    let runtime = new_timer_runtime_builder("g1", mock_store).build();
    let cond = TimerCond {
        namespace: OptionalVal::new("n1".to_string()),
        ..Default::default()
    };
    runtime.set_cond(Arc::new(cond.clone()));
    runtime.init_ctx();

    assert!(!runtime.try_close_triggering_timers());

    let mut timers = Vec::with_capacity(5);
    for index in 0..5 {
        let mut timer = new_test_timer(&format!("t{index}"), "1m", GoTime::now());
        timer.event_status = SchedEventStatus::trigger();
        timer.event_start = GoTime::now();
        timer.event_id = format!("event{index}");
        runtime.cache().update_timer(&timer);
        let event_id = timer.event_id.clone();
        runtime.cache().set_timer_proc_status(
            &timer.id,
            RuntimeProcStatus::WaitTriggerClose,
            &event_id,
        );
        timers.push(timer);
    }

    mock_core.on_list.once(Outcome::Ok(
        timers.iter().map(TimerRecord::clone_record).collect(),
    ));
    assert!(!runtime.try_close_triggering_timers());
    mock_core.assert_expectations();
    assert_eq!(runtime.cache().wait_close_timer_ids.len(), timers.len());
    assert_eq!(runtime.cache().items.len(), timers.len());
    assert_eq!(runtime.cache().sorted.len(), timers.len());

    {
        let conds = mock_core.list_conds.lock().unwrap();
        let ids = assert_timer_ids_cond(conds[0].as_ref().unwrap(), &cond);
        assert_eq!(ids.len(), timers.len());
        for index in 0..timers.len() {
            assert!(ids.contains(&format!("t{index}")));
        }
    }

    let mut t1_new = timers[1].clone_record();
    t1_new.event_status = SchedEventStatus::idle();
    t1_new.event_id = String::new();
    t1_new.version += 1;

    let mut t4_new = timers[4].clone_record();
    t4_new.event_id = "event_next".to_string();
    t4_new.version += 1;

    mock_core.on_list.once(Outcome::Ok(vec![
        timers[0].clone_record(),
        t1_new.clone_record(),
        timers[2].clone_record(),
        t4_new.clone_record(),
    ]));
    assert!(runtime.try_close_triggering_timers());
    mock_core.assert_expectations();

    let cache = runtime.cache();
    assert_eq!(cache.wait_close_timer_ids.len(), 2);
    assert_eq!(cache.items.len(), 4);
    assert_eq!(cache.sorted.len(), 4);
    assert!(cache.wait_close_timer_ids.contains("t0"));
    assert!(cache.wait_close_timer_ids.contains("t2"));
    assert_eq!(cache.items["t0"].timer.as_ref().unwrap(), &timers[0]);
    assert_eq!(
        cache.items["t0"].proc_status,
        RuntimeProcStatus::WaitTriggerClose
    );
    assert_eq!(cache.items["t1"].timer.as_ref().unwrap(), &t1_new);
    assert_eq!(cache.items["t1"].proc_status, RuntimeProcStatus::Idle);
    assert_eq!(cache.items["t2"].timer.as_ref().unwrap(), &timers[2]);
    assert_eq!(
        cache.items["t2"].proc_status,
        RuntimeProcStatus::WaitTriggerClose
    );
    assert_eq!(cache.items["t4"].timer.as_ref().unwrap(), &t4_new);
    assert_eq!(cache.items["t4"].proc_status, RuntimeProcStatus::Idle);
}

#[test]
fn test_create_watch_timer_chan() {
    let _guard = serial();

    let (mock_core, mock_store) = MockStoreCore::new();
    let runtime = new_timer_runtime_builder("g1", mock_store).build();

    let (sender, receiver) = sync_channel(1);
    sender
        .send(WatchTimerResponse {
            events: vec![WatchTimerEvent {
                tp: WatchTimerEventType::Create,
                timer_id: "AAA".to_string(),
            }],
        })
        .unwrap();
    mock_core.queue_watch(receiver);
    mock_core.on_watch_supported.once(Outcome::Ok(true));

    let got = runtime.create_watch_timer_chan(&Context::background());
    let got = got.expect("not the idle watch chan");
    let response = got.try_recv().expect("a queued response");
    assert_eq!(response.events.len(), 1);
    assert_eq!(response.events[0].timer_id, "AAA");
    mock_core.assert_expectations();

    mock_core.on_watch_supported.once(Outcome::Ok(false));
    assert!(runtime
        .create_watch_timer_chan(&Context::background())
        .is_none());
    mock_core.assert_expectations();
}

#[test]
fn test_watch_timer_retry() {
    let _guard = serial();
    let _restore = RestoreIntervals::capture();
    set_in_test_intervals();
    set_re_watch_interval(100_000_000);

    let (mock_core, mock_store) = MockStoreCore::new();

    // a closed channel: the sender is dropped straight away
    let (closed_tx, closed_rx) = sync_channel::<WatchTimerResponse>(0);
    drop(closed_tx);
    // a channel that stays open
    let (normal_tx, normal_rx) = sync_channel::<WatchTimerResponse>(0);

    mock_core.on_watch_supported.always(Outcome::Ok(true));
    mock_core.on_list.always(Outcome::Ok(Vec::new()));
    mock_core.queue_watch(closed_rx);
    mock_core.queue_watch(normal_rx);

    let watched_twice = Arc::new(Event::default());
    *mock_core.watch_signal.lock().unwrap() = Some(Arc::clone(&watched_twice));

    let runtime = new_timer_runtime_builder("g1", mock_store).build();
    runtime.start();

    // wait until both watch calls have happened
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while mock_core
        .watch_calls
        .load(std::sync::atomic::Ordering::SeqCst)
        < 2
    {
        assert!(std::time::Instant::now() < deadline, "wait done timeout");
        std::thread::sleep(Duration::from_millis(1));
    }
    runtime.stop();
    drop(normal_tx);
    watched_twice.wait_done(Duration::from_secs(1));

    let times = mock_core.watch_times.lock().unwrap();
    assert!(times.len() >= 2);
    assert!(times[1].duration_since(times[0]) >= Duration::from_millis(100));
}

#[test]
fn test_timer_full_process() {
    let _guard = serial();
    let _restore = RestoreIntervals::capture();
    set_batch_process_watch_resp_interval(1_000_000);
    set_min_trigger_event_interval(1_000_000);
    set_max_trigger_event_interval(10_000_000);

    let ctx = Context::background();
    let now = Arc::new(Mutex::new(GoTime::from_unix_milli(0)));
    let store = new_memory_timer_store();
    let cli = tidb_timer::client::new_default_timer_client(store.clone());
    let hook = MockHook::new();
    let on_sched_done = Arc::new(Event::default());
    hook.set_sched_done(Arc::clone(&on_sched_done));

    let factory_hook = Arc::clone(&hook);
    let runtime = new_timer_runtime_builder("g1", store.clone())
        .register_hook_factory(
            "h1",
            Box::new(move |hook_class, _cli| {
                assert_eq!(hook_class, "h1");
                Arc::clone(&factory_hook) as Arc<dyn Hook>
            }),
        )
        .build();
    let now_reader = Arc::clone(&now);
    runtime.set_now_func(Arc::new(move || now_reader.lock().unwrap().clone()));

    hook.on_pre_sched_event
        .once(Outcome::Ok(tidb_timer::hook::PreSchedEventResult {
            event_data: b"eventdata1".to_vec(),
            ..Default::default()
        }));
    hook.on_sched_event.once(Outcome::Ok(()));

    runtime.start();

    let timer = cli
        .create_timer(
            &ctx,
            TimerSpec {
                key: "key1".to_string(),
                data: b"timer1data".to_vec(),
                sched_policy_type: SchedPolicyType::interval(),
                sched_policy_expr: "1m".to_string(),
                hook_class: "h1".to_string(),
                enable: true,
                ..Default::default()
            },
        )
        .unwrap();
    let timer_id = timer.id.clone();

    on_sched_done.wait_done(Duration::from_secs(5));
    let mut timer = cli.get_timer_by_id(&ctx, &timer_id).unwrap();
    assert_eq!(timer.event_status, SchedEventStatus::trigger());
    assert_eq!(timer.event_data, b"eventdata1");
    let sched_seen = hook.sched_events.lock().unwrap().last().unwrap().1.clone();
    assert_eq!(sched_seen, timer);
    drop(sched_seen);

    // should not trigger again before closing the previous event
    let second_round = Arc::new(Event::default());
    hook.set_sched_done(Arc::clone(&second_round));
    let advanced = now.lock().unwrap().add(2 * MINUTE);
    *now.lock().unwrap() = advanced;
    second_round.check_not_done(Duration::from_secs(1));
    assert_eq!(cli.get_timer_by_id(&ctx, &timer_id).unwrap(), timer);

    // close the event
    let watermark = now.lock().unwrap().clone();
    cli.close_timer_event(
        &ctx,
        &timer_id,
        &timer.event_id,
        &[
            tidb_timer::with_set_watermark(watermark.clone()),
            tidb_timer::with_set_summary_data(b"summary1".to_vec()),
        ],
    )
    .unwrap();
    timer = cli.get_timer_by_id(&ctx, &timer_id).unwrap();
    assert_eq!(timer.event_status, SchedEventStatus::idle());
    assert!(timer.event_id.is_empty());
    assert!(timer.event_start.is_zero());
    assert!(timer.event_data.is_empty());
    assert_eq!(timer.summary_data, b"summary1");
    second_round.check_not_done(Duration::from_secs(1));

    // trigger again after one minute
    hook.on_pre_sched_event
        .once(Outcome::Ok(tidb_timer::hook::PreSchedEventResult {
            event_data: b"eventdata2".to_vec(),
            ..Default::default()
        }));
    hook.on_sched_event.once(Outcome::Ok(()));
    let next_trigger_time = watermark.add(MINUTE);
    *now.lock().unwrap() = next_trigger_time.clone();
    second_round.wait_done(Duration::from_secs(5));

    let final_timer = cli.get_timer_by_id(&ctx, &timer_id).unwrap();
    assert_eq!(final_timer.event_data, b"eventdata2");
    assert_eq!(final_timer.event_start, next_trigger_time);
    let sched_seen = hook.sched_events.lock().unwrap().last().unwrap().1.clone();
    assert_eq!(sched_seen, final_timer);

    runtime.stop();
    store.close();
}

#[test]
fn test_timer_runtime_loop_panic_recover() {
    let _guard = serial();
    let _restore = RestoreIntervals::capture();
    set_in_test_intervals();

    let (mock_core, mock_store) = MockStoreCore::new();
    let runtime = new_timer_runtime_builder("g1", mock_store.clone()).build();

    // start and panic two times, then normal
    mock_core.on_watch_supported.always(Outcome::Ok(false));
    mock_core.on_list.once(Outcome::Panic("store panic"));
    mock_core.on_list.once(Outcome::Panic("store panic"));
    mock_core.on_list.always(Outcome::Ok(Vec::new()));
    runtime.set_retry_loop_wait(1_000_000);
    runtime.start();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while mock_core.on_list.call_count() < 3 {
        assert!(std::time::Instant::now() < deadline, "wait done timeout");
        std::thread::sleep(Duration::from_millis(1));
    }
    mock_core.assert_expectations();

    // normal stop
    runtime.stop();

    // start and panic always
    let (mock_core, mock_store) = MockStoreCore::new();
    let runtime = new_timer_runtime_builder("g1", mock_store).build();
    mock_core.on_watch_supported.always(Outcome::Ok(false));
    mock_core.on_list.always(Outcome::Panic("store panic"));
    runtime.set_retry_loop_wait(1_000_000);
    runtime.start();
    std::thread::sleep(Duration::from_millis(10));

    // can also stop
    runtime.stop();

    // stop should stop immediately even with a long retry wait
    let (mock_core, mock_store) = MockStoreCore::new();
    let runtime = new_timer_runtime_builder("g1", mock_store).build();
    mock_core.on_watch_supported.always(Outcome::Ok(false));
    mock_core.on_list.always(Outcome::Panic("store panic"));
    runtime.set_retry_loop_wait(MINUTE);
    runtime.start();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while mock_core.on_list.call_count() < 1 {
        assert!(std::time::Instant::now() < deadline, "wait done timeout");
        std::thread::sleep(Duration::from_millis(1));
    }
    std::thread::sleep(Duration::from_millis(1));

    let stopped = Arc::new(Event::default());
    let stopping = Arc::clone(&stopped);
    let stopping_runtime = runtime.clone();
    std::thread::spawn(move || {
        stopping_runtime.stop();
        stopping.signal();
    });
    stopped.wait_done(Duration::from_secs(5));
}
