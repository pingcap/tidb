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

//! Go `pkg/timer/api/client_test.go`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tidb_timer::client::{
    with_id, with_key, with_key_prefix, with_set_enable, with_set_sched_expr,
    with_set_summary_data, with_set_tags, with_set_time_zone, with_set_watermark, with_tag,
    DefaultTimerClient, GetTimerOption, TimerClient, UpdateTimerOption,
};
use tidb_timer::go_time::{GoTime, HOUR, MINUTE, SECOND};
use tidb_timer::mem_store::new_memory_timer_store;
use tidb_timer::store::{
    Cond, Context, OptionalVal, TimerCond, TimerStore, TimerStoreCore, TimerUpdate, WatchTimerChan,
};
use tidb_timer::timer::{
    EventExtra, ManualRequest, SchedEventStatus, SchedPolicyType, TimerRecord, TimerSpec,
};
use tidb_timer::TimerError;

/// Go `TestGetTimerOption`.
#[test]
fn test_get_timer_option() {
    let mut cond = TimerCond::default();
    assert!(cond.fields_set(&[]).is_empty());

    // test 'Key' field
    assert!(!cond.key.present());

    with_key("k1")(&mut cond);
    let (key, ok) = cond.key.get_or_zero();
    assert!(ok);
    assert_eq!(key, "k1");
    assert!(!cond.key_prefix);
    assert_eq!(cond.fields_set(&[]), vec!["Key"]);

    with_key_prefix("k2")(&mut cond);
    let (key, ok) = cond.key.get_or_zero();
    assert!(ok);
    assert_eq!(key, "k2");
    assert!(cond.key_prefix);
    assert_eq!(cond.fields_set(&[]), vec!["Key"]);

    with_key("k3")(&mut cond);
    let (key, ok) = cond.key.get_or_zero();
    assert!(ok);
    assert_eq!(key, "k3");
    assert!(!cond.key_prefix);
    assert_eq!(cond.fields_set(&[]), vec!["Key"]);

    // test 'ID' field
    assert!(!cond.id.present());

    with_id("id1")(&mut cond);
    let (id, ok) = cond.id.get_or_zero();
    assert!(ok);
    assert_eq!(id, "id1");
    assert_eq!(cond.fields_set(&[]), vec!["ID", "Key"]);

    // test 'Tags' field
    assert!(!cond.tags.present());
    with_tag(&["l1", "l2"])(&mut cond);
    let (tags, ok) = cond.tags.get_or_zero();
    assert!(ok);
    assert_eq!(tags, vec!["l1".to_string(), "l2".to_string()]);
    assert_eq!(cond.fields_set(&[]), vec!["ID", "Key", "Tags"]);
}

/// Go `TestUpdateTimerOption`.
#[test]
fn test_update_timer_option() {
    let mut update = TimerUpdate::default();
    assert_eq!(update, TimerUpdate::default());

    // test 'Enable' field
    assert!(!update.enable.present());

    with_set_enable(true)(&mut update);
    let (set_enable, ok) = update.enable.get_or_zero();
    assert!(ok);
    assert!(set_enable);
    assert_eq!(update.fields_set(&[]), vec!["Enable"]);

    with_set_enable(false)(&mut update);
    let (set_enable, ok) = update.enable.get_or_zero();
    assert!(ok);
    assert!(!set_enable);
    assert_eq!(update.fields_set(&[]), vec!["Enable"]);

    // test schedule policy
    assert!(!update.sched_policy_type.present());
    assert!(!update.sched_policy_expr.present());

    with_set_sched_expr(SchedPolicyType::interval(), "3h")(&mut update);
    let (stp, ok) = update.sched_policy_type.get_or_zero();
    assert!(ok);
    assert_eq!(stp, SchedPolicyType::interval());
    let (expr, ok) = update.sched_policy_expr.get_or_zero();
    assert!(ok);
    assert_eq!(expr, "3h");
    assert_eq!(
        update.fields_set(&[]),
        vec!["Enable", "SchedPolicyType", "SchedPolicyExpr"]
    );

    with_set_sched_expr(SchedPolicyType::interval(), "1h")(&mut update);
    let (stp, ok) = update.sched_policy_type.get_or_zero();
    assert!(ok);
    assert_eq!(stp, SchedPolicyType::interval());
    let (expr, ok) = update.sched_policy_expr.get_or_zero();
    assert!(ok);
    assert_eq!(expr, "1h");
    assert_eq!(
        update.fields_set(&[]),
        vec!["Enable", "SchedPolicyType", "SchedPolicyExpr"]
    );

    // test 'Watermark' field
    assert!(!update.watermark.present());

    with_set_watermark(GoTime::from_unix(1234, 5678))(&mut update);
    let (watermark, ok) = update.watermark.get_or_zero();
    assert!(ok);
    assert_eq!(watermark, GoTime::from_unix(1234, 5678));
    assert_eq!(
        update.fields_set(&[]),
        vec!["Enable", "SchedPolicyType", "SchedPolicyExpr", "Watermark"]
    );

    // test 'SummaryData' field
    assert!(!update.summary_data.present());

    with_set_summary_data(b"hello".to_vec())(&mut update);
    let (summary, ok) = update.summary_data.get_or_zero();
    assert!(ok);
    assert_eq!(summary, b"hello".to_vec());
    assert_eq!(
        update.fields_set(&[]),
        vec![
            "Enable",
            "SchedPolicyType",
            "SchedPolicyExpr",
            "Watermark",
            "SummaryData"
        ]
    );

    // test 'Tags' field
    assert!(!update.tags.present());
    with_set_tags(Vec::new())(&mut update);
    let (tags, ok) = update.tags.get_or_zero();
    assert!(ok);
    assert_eq!(tags.len(), 0);
    with_set_tags(vec!["l1".to_string(), "l2".to_string()])(&mut update);
    let (tags, ok) = update.tags.get_or_zero();
    assert!(ok);
    assert_eq!(tags, vec!["l1".to_string(), "l2".to_string()]);
    assert_eq!(
        update.fields_set(&[]),
        vec![
            "Tags",
            "Enable",
            "SchedPolicyType",
            "SchedPolicyExpr",
            "Watermark",
            "SummaryData"
        ]
    );

    // test 'TimeZone' field
    assert!(!update.time_zone.present());
    with_set_time_zone("UTC")(&mut update);
    assert!(update.time_zone.present());
    let (tz, ok) = update.time_zone.get_or_zero();
    assert!(ok);
    assert_eq!(tz, "UTC");
    assert_eq!(
        update.fields_set(&[]),
        vec![
            "Tags",
            "Enable",
            "TimeZone",
            "SchedPolicyType",
            "SchedPolicyExpr",
            "Watermark",
            "SummaryData"
        ]
    );
}

/// Go `TestDefaultClient`.
#[test]
fn test_default_client() {
    let store = new_memory_timer_store();
    let cli = DefaultTimerClient::new(store.clone());
    let ctx = Context::background();
    let mut spec = TimerSpec {
        key: "k1".to_string(),
        sched_policy_type: SchedPolicyType::interval(),
        sched_policy_expr: "1h".to_string(),
        time_zone: "Asia/Shanghai".to_string(),
        data: b"data1".to_vec(),
        tags: vec!["l1".to_string(), "l2".to_string()],
        ..Default::default()
    };

    // create
    let mut timer = cli.create_timer(&ctx, spec.clone()).unwrap();
    spec.namespace = "default".to_string();
    assert!(!timer.id.is_empty());
    assert_eq!(timer.spec, spec);
    assert_eq!(timer.spec.time_zone, "Asia/Shanghai");
    assert_eq!(timer.event_status, SchedEventStatus::idle());
    assert_eq!(timer.event_id, "");
    assert!(timer.event_data.is_empty());
    assert!(timer.summary_data.is_empty());

    // get by id
    let got = cli.get_timer_by_id(&ctx, &timer.id).unwrap();
    assert_eq!(timer, got);

    // get by key
    let got = cli.get_timer_by_key(&ctx, &timer.spec.key).unwrap();
    assert_eq!(timer, got);

    // get by key prefix
    let opts: Vec<GetTimerOption> = vec![with_key_prefix("k")];
    let tms = cli.get_timers(&ctx, &opts).unwrap();
    assert_eq!(tms.len(), 1);
    assert_eq!(timer, tms[0]);

    // get by tag
    let opts: Vec<GetTimerOption> = vec![with_tag(&["l1"])];
    let tms = cli.get_timers(&ctx, &opts).unwrap();
    assert_eq!(tms.len(), 1);
    assert_eq!(timer, tms[0]);

    let opts: Vec<GetTimerOption> = vec![with_tag(&["l1", "l2"])];
    let tms = cli.get_timers(&ctx, &opts).unwrap();
    assert_eq!(tms.len(), 1);
    assert_eq!(timer, tms[0]);

    let opts: Vec<GetTimerOption> = vec![with_tag(&["l3"])];
    let tms = cli.get_timers(&ctx, &opts).unwrap();
    assert_eq!(tms.len(), 0);

    // update
    let opts: Vec<UpdateTimerOption> = vec![with_set_sched_expr(SchedPolicyType::interval(), "3h")];
    cli.update_timer(&ctx, &timer.id, &opts).unwrap();
    timer.spec.sched_policy_type = SchedPolicyType::interval();
    timer.spec.sched_policy_expr = "3h".to_string();
    let got = cli.get_timer_by_id(&ctx, &timer.id).unwrap();
    assert!(got.version > timer.version);
    timer.version = got.version;
    assert_eq!(timer, got);

    // close event
    let event_start = GoTime::now().add(-SECOND);
    store
        .update(
            &ctx,
            &timer.id,
            &TimerUpdate {
                event_status: OptionalVal::new(SchedEventStatus::trigger()),
                event_id: OptionalVal::new("event1".to_string()),
                event_data: OptionalVal::new(b"d1".to_vec()),
                summary_data: OptionalVal::new(b"s1".to_vec()),
                event_start: OptionalVal::new(event_start.clone()),
                event_extra: OptionalVal::new(EventExtra {
                    event_manual_request_id: "req1".to_string(),
                    event_watermark: GoTime::from_unix(456, 0),
                }),
                ..Default::default()
            },
        )
        .unwrap();
    let err = cli
        .close_timer_event(&ctx, &timer.id, "event2", &[])
        .unwrap_err();
    assert!(err.error_equal(&TimerError::EventIDNotMatch));

    let opts: Vec<UpdateTimerOption> = vec![with_set_sched_expr(SchedPolicyType::interval(), "1h")];
    let err = cli
        .close_timer_event(&ctx, &timer.id, "event2", &opts)
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "The field(s) [SchedPolicyType, SchedPolicyExpr] are not allowed to update when close event"
    );

    cli.close_timer_event(&ctx, &timer.id, "event1", &[])
        .unwrap();
    timer = cli.get_timer_by_id(&ctx, &timer.id).unwrap();
    assert_eq!(timer.event_status, SchedEventStatus::idle());
    assert!(timer.event_id.is_empty());
    assert!(timer.event_data.is_empty());
    assert!(timer.event_start.is_zero());
    assert_eq!(timer.summary_data, b"s1".to_vec());
    assert_eq!(timer.spec.watermark.unix(), event_start.unix());
    assert_eq!(timer.event_extra, EventExtra::default());

    // close event with option
    store
        .update(
            &ctx,
            &timer.id,
            &TimerUpdate {
                event_id: OptionalVal::new("event1".to_string()),
                event_data: OptionalVal::new(b"d1".to_vec()),
                summary_data: OptionalVal::new(b"s1".to_vec()),
                ..Default::default()
            },
        )
        .unwrap();

    let watermark = GoTime::now().add(HOUR);
    let opts: Vec<UpdateTimerOption> = vec![
        with_set_watermark(watermark.clone()),
        with_set_summary_data(b"s2".to_vec()),
    ];
    cli.close_timer_event(&ctx, &timer.id, "event1", &opts)
        .unwrap();
    timer = cli.get_timer_by_id(&ctx, &timer.id).unwrap();
    assert_eq!(timer.event_status, SchedEventStatus::idle());
    assert!(timer.event_id.is_empty());
    assert!(timer.event_data.is_empty());
    assert!(timer.event_start.is_zero());
    assert_eq!(timer.summary_data, b"s2".to_vec());
    assert_eq!(timer.spec.watermark.unix(), watermark.unix());

    // manual trigger
    store
        .update(
            &ctx,
            &timer.id,
            &TimerUpdate {
                event_id: OptionalVal::new("event1".to_string()),
                event_data: OptionalVal::new(b"d1".to_vec()),
                summary_data: OptionalVal::new(b"s1".to_vec()),
                ..Default::default()
            },
        )
        .unwrap();
    let err = cli.manual_trigger_event(&ctx, &timer.id).unwrap_err();
    assert_eq!(
        err.to_string(),
        "manual trigger is not allowed when event is not closed"
    );

    cli.close_timer_event(&ctx, &timer.id, "event1", &[])
        .unwrap();
    let opts: Vec<UpdateTimerOption> = vec![with_set_enable(false)];
    cli.update_timer(&ctx, &timer.id, &opts).unwrap();
    let err = cli.manual_trigger_event(&ctx, &timer.id).unwrap_err();
    assert_eq!(
        err.to_string(),
        "manual trigger is not allowed when timer is disabled"
    );

    let opts: Vec<UpdateTimerOption> = vec![with_set_enable(true)];
    cli.update_timer(&ctx, &timer.id, &opts).unwrap();
    let now = GoTime::now();
    let req_id = cli.manual_trigger_event(&ctx, &timer.id).unwrap();
    assert!(!req_id.is_empty());
    timer = cli.get_timer_by_id(&ctx, &timer.id).unwrap();
    assert!(!timer.manual_request.manual_request_id.is_empty());
    assert!(timer.manual_request.manual_request_time.unix() >= now.unix());
    assert!(timer.manual_request.manual_request_time.sub(&now) < 10 * SECOND);
    assert_eq!(
        timer.manual_request,
        ManualRequest {
            manual_request_id: req_id,
            manual_request_time: timer.manual_request.manual_request_time.clone(),
            manual_timeout: 2 * MINUTE,
            ..Default::default()
        }
    );

    // close manual triggered event
    let manual_request = timer.manual_request.set_processed("event1");
    store
        .update(
            &ctx,
            &timer.id,
            &TimerUpdate {
                manual_request: OptionalVal::new(manual_request.clone()),
                event_extra: OptionalVal::new(EventExtra {
                    event_manual_request_id: manual_request.manual_request_id.clone(),
                    event_watermark: timer.spec.watermark.clone(),
                }),
                event_id: OptionalVal::new("event1".to_string()),
                event_start: OptionalVal::new(GoTime::now()),
                event_status: OptionalVal::new(SchedEventStatus::trigger()),
                ..Default::default()
            },
        )
        .unwrap();
    cli.close_timer_event(&ctx, &timer.id, "event1", &[])
        .unwrap();
    timer = cli.get_timer_by_id(&ctx, &timer.id).unwrap();
    assert_eq!(timer.manual_request, manual_request);
    assert_eq!(timer.event_extra, EventExtra::default());

    // delete
    assert!(cli.delete_timer(&ctx, &timer.id).unwrap());

    // delete no exist
    assert!(!cli.delete_timer(&ctx, &timer.id).unwrap());
}

/// Go's `injectedTimerStore`.
///
/// Go's hook closes over the wrapper itself so it can re-enter the inner
/// store; Rust cannot form that self-reference in a closure, so the inner
/// store is handed to the hook as an argument instead.
type BeforeUpdate = Box<dyn Fn(&TimerStore) + Send + Sync>;

struct InjectedTimerStore {
    store: TimerStore,
    before_update: Mutex<Option<BeforeUpdate>>,
}

impl InjectedTimerStore {
    fn set_before_update(&self, hook: BeforeUpdate) {
        *self.before_update.lock().unwrap() = Some(hook);
    }
}

impl TimerStoreCore for InjectedTimerStore {
    fn create(&self, ctx: &Context, record: &TimerRecord) -> Result<String, TimerError> {
        self.store.create(ctx, record)
    }

    fn list(&self, ctx: &Context, cond: Option<&dyn Cond>) -> Result<Vec<TimerRecord>, TimerError> {
        self.store.list(ctx, cond)
    }

    fn update(
        &self,
        ctx: &Context,
        timer_id: &str,
        update: &TimerUpdate,
    ) -> Result<(), TimerError> {
        if let Some(hook) = self.before_update.lock().unwrap().as_ref() {
            hook(&self.store);
        }
        self.store.update(ctx, timer_id, update)
    }

    fn delete(&self, ctx: &Context, timer_id: &str) -> Result<bool, TimerError> {
        self.store.delete(ctx, timer_id)
    }

    fn watch_supported(&self) -> bool {
        self.store.watch_supported()
    }

    fn watch(&self, ctx: &Context) -> WatchTimerChan {
        self.store.watch(ctx)
    }

    fn close(&self) {
        self.store.close();
    }
}

/// Go `TestDefaultClientManualTriggerRetry`.
#[test]
fn test_default_client_manual_trigger_retry() {
    let inject = Arc::new(InjectedTimerStore {
        store: new_memory_timer_store(),
        before_update: Mutex::new(None),
    });

    let store = TimerStore::new(Arc::clone(&inject) as Arc<dyn TimerStoreCore>);
    let cli = DefaultTimerClient::new(store);
    cli.set_retry_backoff(1);
    let ctx = Context::background();
    let spec = TimerSpec {
        key: "k1".to_string(),
        sched_policy_type: SchedPolicyType::interval(),
        sched_policy_expr: "1h".to_string(),
        data: b"data1".to_vec(),
        tags: vec!["l1".to_string(), "l2".to_string()],
        enable: true,
        ..Default::default()
    };

    let timer = cli.create_timer(&ctx, spec).unwrap();
    let timer_id = timer.id.clone();

    // retry and success
    let update_times = Arc::new(AtomicUsize::new(0));
    {
        let update_times = Arc::clone(&update_times);
        let timer_id = timer_id.clone();
        inject.set_before_update(Box::new(move |store| {
            let round = update_times.fetch_add(1, Ordering::SeqCst) + 1;
            if round < 3 {
                store
                    .update(
                        &Context::background(),
                        &timer_id,
                        &TimerUpdate {
                            watermark: OptionalVal::new(GoTime::now()),
                            ..Default::default()
                        },
                    )
                    .unwrap();
            }
        }));
    }
    let req_id = cli.manual_trigger_event(&ctx, &timer_id).unwrap();
    assert!(!req_id.is_empty());
    assert_eq!(update_times.load(Ordering::SeqCst), 3);

    // max retry
    {
        let timer_id = timer_id.clone();
        inject.set_before_update(Box::new(move |store| {
            store
                .update(
                    &Context::background(),
                    &timer_id,
                    &TimerUpdate {
                        watermark: OptionalVal::new(GoTime::now()),
                        ..Default::default()
                    },
                )
                .unwrap();
        }));
    }
    let err = cli.manual_trigger_event(&ctx, &timer_id).unwrap_err();
    assert_eq!(err.to_string(), "timer version not match");

    // retry to other error
    update_times.store(0, Ordering::SeqCst);
    {
        let update_times = Arc::clone(&update_times);
        let timer_id = timer_id.clone();
        inject.set_before_update(Box::new(move |store| {
            let round = update_times.fetch_add(1, Ordering::SeqCst) + 1;
            let update = if round < 3 {
                TimerUpdate {
                    watermark: OptionalVal::new(GoTime::now()),
                    ..Default::default()
                }
            } else {
                TimerUpdate {
                    enable: OptionalVal::new(false),
                    ..Default::default()
                }
            };
            store
                .update(&Context::background(), &timer_id, &update)
                .unwrap();
        }));
    }
    let err = cli.manual_trigger_event(&ctx, &timer_id).unwrap_err();
    assert_eq!(
        err.to_string(),
        "manual trigger is not allowed when timer is disabled"
    );
    assert_eq!(update_times.load(Ordering::SeqCst), 3);
}
