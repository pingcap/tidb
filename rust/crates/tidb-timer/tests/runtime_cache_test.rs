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

//! Transcreation of Go `pkg/timer/runtime/cache_test.go`.

mod common;

use std::sync::Arc;

use tidb_timer::cron::parse_standard;
use tidb_timer::go_time::{GoTime, MINUTE, SECOND};
use tidb_timer::runtime::cache::{
    location_changed, never_try_trigger_time, RuntimeProcStatus, TimersCache,
};
use tidb_timer::timer::{ManualRequest, SchedEventStatus, SchedPolicyType, TimerRecord};
use tidb_util::timeutil::{load_location, TimeZone};

use common::new_test_timer;

fn utc() -> TimeZone {
    TimeZone::Named(chrono_tz::Tz::UTC)
}

/// Go `time.FixedZone(name, offset)`.
fn fixed_zone(name: &str, offset_secs: i32) -> TimeZone {
    TimeZone::Fixed {
        name: name.to_string(),
        offset_secs,
    }
}

/// Go `checkSortedCache`.
fn check_sorted_cache(cache: &TimersCache, sorted: &[(&TimerRecord, GoTime)]) {
    let mut index = 0usize;
    cache.iter_try_trigger_timers(|timer, try_trigger_time, next_event_time| {
        let (expected_timer, expected_try) = &sorted[index];
        assert_eq!(**expected_timer, *timer, "case {index}");
        let item = cache.items.get(&timer.id).expect("item present");
        assert_eq!(**expected_timer, *item.timer.as_ref().unwrap());

        if timer.is_manual_requesting() {
            assert_eq!(try_trigger_time, next_event_time.unwrap());
        } else {
            match timer.next_event_time() {
                Ok((time, ok)) => {
                    if !timer.spec.enable {
                        assert!(time.is_zero());
                        assert!(!ok);
                    } else {
                        assert!(ok);
                        assert_eq!(&time, next_event_time.unwrap());
                    }
                }
                Err(_) => assert!(next_event_time.is_none()),
            }
        }

        assert_eq!(expected_try, try_trigger_time, "case {index}");
        index += 1;
        true
    });
    assert_eq!(sorted.len(), index);
}

#[test]
fn test_cache_update() {
    let now = GoTime::now().in_location(&utc());
    let now_for_func = now.clone();
    let mut cache = TimersCache::new();
    cache.now_func = Arc::new(move || now_for_func.clone());

    // update
    let mut t1 = new_test_timer("t1", "10m", now.clone());
    assert!(cache.update_timer(&t1));
    check_sorted_cache(&cache, &[(&t1, now.add(10 * MINUTE))]);
    assert_eq!(cache.items.len(), 1);

    // dup update with same version
    assert!(!cache.update_timer(&t1.clone_record()));
    check_sorted_cache(&cache, &[(&t1, now.add(10 * MINUTE))]);
    assert_eq!(cache.items.len(), 1);

    // policy changed
    t1.spec.sched_policy_type = SchedPolicyType::cron();
    t1.spec.sched_policy_expr = "* 1 * * *".to_string();
    t1.version += 1;
    assert!(cache.update_timer(&t1));
    let schedule = parse_standard(&t1.spec.sched_policy_expr).unwrap();
    check_sorted_cache(&cache, &[(&t1, schedule.next(&now))]);
    assert_eq!(cache.items.len(), 1);

    // update with same version but loc changed
    let fixed = fixed_zone("name1", 2 * 60 * 60);
    t1.location = Some(fixed.clone());
    assert!(cache.update_timer(&t1));
    check_sorted_cache(&cache, &[(&t1, schedule.next(&now.in_location(&fixed)))]);
    assert_eq!(cache.items.len(), 1);

    // invalid policy
    t1.location = Some(now.location().clone());
    t1.spec.sched_policy_type = SchedPolicyType::interval();
    t1.spec.sched_policy_expr = "invalid".to_string();
    t1.version += 1;
    assert!(cache.update_timer(&t1));
    check_sorted_cache(&cache, &[(&t1, never_try_trigger_time())]);
    assert_eq!(cache.items.len(), 1);

    // manual set next try trigger time for invalid timer
    cache.update_next_try_trigger_time(&t1.id, now.add(7 * SECOND));
    check_sorted_cache(&cache, &[(&t1, never_try_trigger_time())]);
    assert_eq!(cache.items.len(), 1);

    // not enable
    t1.spec.sched_policy_expr = "1m".to_string();
    t1.spec.enable = false;
    t1.version += 1;
    assert!(cache.update_timer(&t1));
    check_sorted_cache(&cache, &[(&t1, never_try_trigger_time())]);
    assert_eq!(cache.items.len(), 1);

    // manual set next try trigger time but before nextEventTime
    t1.spec.enable = true;
    t1.version += 1;
    assert!(cache.update_timer(&t1));
    check_sorted_cache(&cache, &[(&t1, now.add(MINUTE))]);
    cache.update_next_try_trigger_time(&t1.id, now.add(MINUTE - SECOND));
    check_sorted_cache(&cache, &[(&t1, now.add(MINUTE))]);

    // manual set next try trigger
    cache.update_next_try_trigger_time(&t1.id, now.add(MINUTE + SECOND));
    check_sorted_cache(&cache, &[(&t1, now.add(MINUTE + SECOND))]);

    // should not change procTriggering state
    t1.spec.enable = true;
    t1.version += 1;
    cache.set_timer_proc_status(&t1.id, RuntimeProcStatus::Triggering, "event1");
    assert!(cache.update_timer(&t1));
    check_sorted_cache(&cache, &[]);
    assert_eq!(cache.items.len(), 1);
    assert_eq!(
        cache.items[&t1.id].proc_status,
        RuntimeProcStatus::Triggering
    );
    assert_eq!(cache.items[&t1.id].trigger_event_id, "event1");

    // test SchedEventTrigger but procIdle
    t1.spec.sched_policy_expr = "1m".to_string();
    t1.event_status = SchedEventStatus::trigger();
    t1.event_start = now.add(-10 * SECOND);
    t1.event_id = "event1".to_string();
    t1.version += 1;
    assert!(cache.update_timer(&t1));
    cache.set_timer_proc_status(&t1.id, RuntimeProcStatus::Idle, "event1");
    check_sorted_cache(&cache, &[(&t1, now.add(-10 * SECOND))]);
    assert_eq!(cache.items.len(), 1);
    assert_eq!(cache.wait_close_timer_ids.len(), 0);

    // should reset procWaitTriggerClose to procIdle
    cache.set_timer_proc_status(&t1.id, RuntimeProcStatus::WaitTriggerClose, "event1");
    check_sorted_cache(&cache, &[]);
    assert_eq!(cache.items.len(), 1);
    assert_eq!(cache.wait_close_timer_ids.len(), 1);
    assert!(cache.wait_close_timer_ids.contains(&t1.id));

    t1.version += 1;
    assert!(cache.update_timer(&t1));
    assert_eq!(cache.items.len(), 1);
    assert_eq!(cache.wait_close_timer_ids.len(), 1);
    assert!(cache.wait_close_timer_ids.contains(&t1.id));
    assert_eq!(
        cache.items[&t1.id].proc_status,
        RuntimeProcStatus::WaitTriggerClose
    );

    t1.event_status = SchedEventStatus::idle();
    t1.event_id = String::new();
    t1.version += 1;
    assert!(cache.update_timer(&t1));
    assert_eq!(cache.items[&t1.id].proc_status, RuntimeProcStatus::Idle);
    assert_eq!(cache.items[&t1.id].trigger_event_id, "");
    assert_eq!(cache.wait_close_timer_ids.len(), 0);

    t1.version += 1;
    t1.manual_request = ManualRequest {
        manual_request_id: "req1".to_string(),
        manual_request_time: now.clone(),
        manual_timeout: MINUTE,
        manual_processed: true,
        ..Default::default()
    };
    assert!(cache.update_timer(&t1));
    assert_eq!(cache.items[&t1.id].proc_status, RuntimeProcStatus::Idle);
    assert_eq!(cache.items[&t1.id].trigger_event_id, "");
    assert_eq!(cache.wait_close_timer_ids.len(), 0);
    check_sorted_cache(&cache, &[(&t1, now.add(MINUTE))]);

    t1.version += 1;
    t1.manual_request = ManualRequest {
        manual_request_id: "req2".to_string(),
        manual_request_time: now.clone(),
        manual_timeout: MINUTE,
        ..Default::default()
    };
    assert!(cache.update_timer(&t1));
    assert_eq!(cache.items[&t1.id].proc_status, RuntimeProcStatus::Idle);
    assert_eq!(cache.items[&t1.id].trigger_event_id, "");
    assert_eq!(cache.wait_close_timer_ids.len(), 0);
    check_sorted_cache(&cache, &[(&t1, now.clone())]);
}

#[test]
fn test_cache_sort() {
    let now = GoTime::now().in_location(&utc());
    let now_for_func = now.clone();
    let mut cache = TimersCache::new();
    cache.now_func = Arc::new(move || now_for_func.clone());

    check_sorted_cache(&cache, &[]);

    let mut t1 = new_test_timer("t1", "10m", now.clone());
    assert!(cache.update_timer(&t1));
    check_sorted_cache(&cache, &[(&t1, now.add(10 * MINUTE))]);

    let mut t2 = new_test_timer("t2", "20m", now.clone());
    assert!(cache.update_timer(&t2));
    check_sorted_cache(
        &cache,
        &[(&t1, now.add(10 * MINUTE)), (&t2, now.add(20 * MINUTE))],
    );

    let mut t3 = new_test_timer("t3", "5m", now.clone());
    assert!(cache.update_timer(&t3));
    check_sorted_cache(
        &cache,
        &[
            (&t3, now.add(5 * MINUTE)),
            (&t1, now.add(10 * MINUTE)),
            (&t2, now.add(20 * MINUTE)),
        ],
    );

    let mut t4 = new_test_timer("t4", "3m", now.clone());
    assert!(cache.update_timer(&t4));
    check_sorted_cache(
        &cache,
        &[
            (&t4, now.add(3 * MINUTE)),
            (&t3, now.add(5 * MINUTE)),
            (&t1, now.add(10 * MINUTE)),
            (&t2, now.add(20 * MINUTE)),
        ],
    );

    // move left 1
    t3.spec.sched_policy_expr = "1m".to_string();
    t3.version += 1;
    assert!(cache.update_timer(&t3));
    check_sorted_cache(
        &cache,
        &[
            (&t3, now.add(MINUTE)),
            (&t4, now.add(3 * MINUTE)),
            (&t1, now.add(10 * MINUTE)),
            (&t2, now.add(20 * MINUTE)),
        ],
    );

    // move left 2
    t2.spec.sched_policy_expr = "2m".to_string();
    t2.version += 1;
    assert!(cache.update_timer(&t2));
    check_sorted_cache(
        &cache,
        &[
            (&t3, now.add(MINUTE)),
            (&t2, now.add(2 * MINUTE)),
            (&t4, now.add(3 * MINUTE)),
            (&t1, now.add(10 * MINUTE)),
        ],
    );

    // move right 1
    t4.spec.sched_policy_expr = "15m".to_string();
    t4.version += 1;
    assert!(cache.update_timer(&t4));
    check_sorted_cache(
        &cache,
        &[
            (&t3, now.add(MINUTE)),
            (&t2, now.add(2 * MINUTE)),
            (&t1, now.add(10 * MINUTE)),
            (&t4, now.add(15 * MINUTE)),
        ],
    );

    // move right 2
    t3.spec.sched_policy_expr = "12m".to_string();
    t3.version += 1;
    assert!(cache.update_timer(&t3));
    check_sorted_cache(
        &cache,
        &[
            (&t2, now.add(2 * MINUTE)),
            (&t1, now.add(10 * MINUTE)),
            (&t3, now.add(12 * MINUTE)),
            (&t4, now.add(15 * MINUTE)),
        ],
    );

    // unchanged
    t2.spec.sched_policy_expr = "1m".to_string();
    t2.version += 1;
    assert!(cache.update_timer(&t2));
    check_sorted_cache(
        &cache,
        &[
            (&t2, now.add(MINUTE)),
            (&t1, now.add(10 * MINUTE)),
            (&t3, now.add(12 * MINUTE)),
            (&t4, now.add(15 * MINUTE)),
        ],
    );

    t1.spec.sched_policy_expr = "11m".to_string();
    t1.version += 1;
    assert!(cache.update_timer(&t1));
    check_sorted_cache(
        &cache,
        &[
            (&t2, now.add(MINUTE)),
            (&t1, now.add(11 * MINUTE)),
            (&t3, now.add(12 * MINUTE)),
            (&t4, now.add(15 * MINUTE)),
        ],
    );

    t4.spec.sched_policy_expr = "16m".to_string();
    t4.version += 1;
    assert!(cache.update_timer(&t4));
    check_sorted_cache(
        &cache,
        &[
            (&t2, now.add(MINUTE)),
            (&t1, now.add(11 * MINUTE)),
            (&t3, now.add(12 * MINUTE)),
            (&t4, now.add(16 * MINUTE)),
        ],
    );

    // test updateNextTryTriggerTime
    cache.update_next_try_trigger_time(&t2.id, now.add(20 * MINUTE));
    check_sorted_cache(
        &cache,
        &[
            (&t1, now.add(11 * MINUTE)),
            (&t3, now.add(12 * MINUTE)),
            (&t4, now.add(16 * MINUTE)),
            (&t2, now.add(20 * MINUTE)),
        ],
    );

    cache.update_next_try_trigger_time(&t2.id, now.add(14 * MINUTE));
    check_sorted_cache(
        &cache,
        &[
            (&t1, now.add(11 * MINUTE)),
            (&t3, now.add(12 * MINUTE)),
            (&t2, now.add(14 * MINUTE)),
            (&t4, now.add(16 * MINUTE)),
        ],
    );

    cache.update_next_try_trigger_time(&t3.id, now.add(15 * MINUTE));
    check_sorted_cache(
        &cache,
        &[
            (&t1, now.add(11 * MINUTE)),
            (&t2, now.add(14 * MINUTE)),
            (&t3, now.add(15 * MINUTE)),
            (&t4, now.add(16 * MINUTE)),
        ],
    );

    // test version update should reset updateNextTryTriggerTime
    t3.version += 1;
    assert!(cache.update_timer(&t3));
    check_sorted_cache(
        &cache,
        &[
            (&t1, now.add(11 * MINUTE)),
            (&t3, now.add(12 * MINUTE)),
            (&t2, now.add(14 * MINUTE)),
            (&t4, now.add(16 * MINUTE)),
        ],
    );
}

#[test]
fn test_full_update_cache() {
    let now = GoTime::now().in_location(&utc());
    let now_for_func = now.clone();
    let mut cache = TimersCache::new();
    cache.now_func = Arc::new(move || now_for_func.clone());

    let mut t1 = new_test_timer("t1", "10m", now.clone());
    let t2 = new_test_timer("t2", "20m", now.clone());
    let mut t3 = new_test_timer("t3", "30m", now.clone());
    let t4 = new_test_timer("t4", "40m", now.clone());

    assert!(cache.update_timer(&t1));
    assert!(cache.update_timer(&t2));
    assert!(cache.update_timer(&t3));
    assert!(cache.update_timer(&t4));
    check_sorted_cache(
        &cache,
        &[
            (&t1, now.add(10 * MINUTE)),
            (&t2, now.add(20 * MINUTE)),
            (&t3, now.add(30 * MINUTE)),
            (&t4, now.add(40 * MINUTE)),
        ],
    );

    t1.spec.sched_policy_expr = "15m".to_string();
    t1.version += 1;
    t3.spec.sched_policy_expr = "1m".to_string();
    t3.version += 1;
    let t5 = new_test_timer("t5", "25m", now.clone());
    cache.full_update_timers(&[t1.clone_record(), t3.clone_record(), t5.clone_record()]);
    check_sorted_cache(
        &cache,
        &[
            (&t3, now.add(MINUTE)),
            (&t1, now.add(15 * MINUTE)),
            (&t5, now.add(25 * MINUTE)),
        ],
    );
    assert_eq!(cache.items.len(), 3);
}

#[test]
fn test_location_changed() {
    let loc1 = load_location("America/New_York").unwrap();
    let loc2 = load_location("America/Los_Angeles").unwrap();
    let loc3 = load_location("America/New_York").unwrap();
    let loc4 = fixed_zone("name1", 2 * 60 * 60);
    let loc5 = fixed_zone("name2", 2 * 60 * 60);
    let loc6 = fixed_zone("name1", 60 * 60);

    let cases: Vec<(Option<&TimeZone>, Option<&TimeZone>, bool)> = vec![
        (None, None, false),
        (Some(&loc1), None, true),
        (None, Some(&loc1), true),
        (Some(&loc1), Some(&loc2), true),
        (Some(&loc1), Some(&loc3), false),
        (Some(&loc4), Some(&loc5), false),
        (Some(&loc4), Some(&loc6), true),
    ];

    for (index, (a, b, changed)) in cases.into_iter().enumerate() {
        assert_eq!(changed, location_changed(a, b), "case {index}");
    }
}
