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

//! Direct transit of the pure scheduler tests in client-go `client_test.go`.

#[path = "../src/rpc/batch/mod.rs"]
mod batch;

use std::time::Duration;

use batch::{
    BatchEntry, BatchPolicyOptions, BatchScheduler, BatchTrigger, BATCH_POLICY_BASIC,
    BATCH_POLICY_CUSTOM, BATCH_POLICY_POSITIVE, BATCH_POLICY_STANDARD, DEFAULT_BATCH_POLICY,
    HIGH_TASK_PRIORITY,
};

#[test]
fn test_batch_commands_builder() {
    let mut scheduler = BatchScheduler::new();

    for payload in 0..10 {
        scheduler.push(BatchEntry::new(payload));
        assert_eq!(scheduler.len(), payload + 1);
    }
    let groups = scheduler.build_with_limit(usize::MAX);
    let direct = groups.direct().expect("direct group");
    assert_eq!(direct.len(), 10);
    assert!(groups.forwarded().is_empty());
    for (index, item) in direct.entries().iter().enumerate() {
        assert_eq!(item.request_id(), (index + 1) as u64);
        assert_eq!(*item.entry().payload(), index);
    }
    assert_eq!(scheduler.id_alloc(), 10);

    scheduler.reset();
    let forwarded_hosts = [
        None,
        Some("127.0.0.1:6666"),
        Some("127.0.0.1:7777"),
        Some("127.0.0.1:8888"),
    ];
    let mut payload = 0;
    for start in 0..forwarded_hosts.len() {
        for host in &forwarded_hosts[start..] {
            let entry = BatchEntry::new(payload);
            scheduler.push(match host {
                Some(host) => entry.with_forwarded_host(*host),
                None => entry,
            });
            payload += 1;
        }
    }
    let groups = scheduler.build_with_limit(usize::MAX);
    assert_eq!(groups.direct().expect("direct group").len(), 1);
    assert_eq!(groups.forwarded().len(), 3);
    for (index, host) in forwarded_hosts[1..].iter().enumerate() {
        let group = &groups.forwarded()[host.expect("forwarded host")];
        assert_eq!(group.len(), index + 2);
        assert!(group
            .entries()
            .iter()
            .all(|item| { item.entry().forwarded_host() == Some(host.expect("forwarded host")) }));
    }
    assert_eq!(scheduler.id_alloc(), 20);

    scheduler.reset();
    for (payload, canceled) in [true, false, true, true, false].into_iter().enumerate() {
        let entry = BatchEntry::new(payload);
        let state = entry.state();
        if canceled {
            state.cancel();
        }
        scheduler.push(entry);
    }
    let groups = scheduler.build_with_limit(usize::MAX);
    let direct = groups.direct().expect("direct group");
    assert_eq!(direct.len(), 2);
    assert!(direct
        .entries()
        .iter()
        .all(|item| !item.entry().state().is_canceled()));

    scheduler.reset();
    let mut cancellation_states = Vec::new();
    for payload in 0..3 {
        let entry = BatchEntry::new(payload);
        cancellation_states.push(entry.state());
        scheduler.push(entry);
    }
    assert_eq!(scheduler.cancel_all("error").len(), 3);
    assert!(cancellation_states
        .iter()
        .all(|state| state.failure().as_deref() == Some("error")));
    scheduler.reset();
    assert_eq!(scheduler.len(), 0);
    assert_ne!(scheduler.id_alloc(), 0);
}

#[test]
fn test_limit_concurrency() {
    let mut scheduler = BatchScheduler::new();

    scheduler.push(BatchEntry::new(1));
    assert_eq!(
        scheduler
            .build_with_limit(1)
            .direct()
            .expect("direct group")
            .len(),
        1
    );
    assert_eq!(scheduler.len(), 0);
    scheduler.reset();

    scheduler.push(BatchEntry::new(2).with_priority(HIGH_TASK_PRIORITY));
    scheduler.push(BatchEntry::new(3).with_priority(HIGH_TASK_PRIORITY - 1));
    assert_eq!(
        scheduler
            .build_with_limit(0)
            .direct()
            .expect("direct group")
            .len(),
        1
    );
    scheduler.reset();
    assert_eq!(scheduler.len(), 1);

    scheduler.push(BatchEntry::new(4));
    scheduler.push(BatchEntry::new(5));
    assert_eq!(
        scheduler
            .build_with_limit(2)
            .direct()
            .expect("direct group")
            .len(),
        2
    );
    assert_eq!(scheduler.len(), 1);
    scheduler.reset();

    for payload in 6..=7 {
        let entry = BatchEntry::new(payload);
        entry.state().cancel();
        scheduler.push(entry);
    }
    scheduler.reset();
    assert_eq!(scheduler.len(), 1);
}

#[test]
fn test_batch_policy() {
    let (basic, valid) = BatchTrigger::from_policy(BATCH_POLICY_BASIC);
    assert!(valid);
    assert_eq!(basic.turbo_wait_time(), Duration::ZERO);

    let (mut positive, valid) = BatchTrigger::from_policy(BATCH_POLICY_POSITIVE);
    assert!(valid);
    assert_eq!(positive.turbo_wait_time(), Duration::from_micros(100));
    assert!(positive.need_fetch_more(Duration::from_secs(3600)));
    assert!(positive.need_fetch_more(Duration::from_millis(1)));
    for average in [1.0, 1.2, 1.8] {
        assert_eq!(positive.preferred_batch_wait_size(average, 8), 8);
    }

    let (mut standard, valid) = BatchTrigger::from_policy(BATCH_POLICY_STANDARD);
    assert!(valid);
    assert_eq!(standard.preferred_batch_wait_size(1.0, 8), 1);
    assert_eq!(standard.preferred_batch_wait_size(1.2, 8), 1);
    assert_eq!(standard.preferred_batch_wait_size(1.8, 8), 2);
    assert_eq!(standard.turbo_wait_time(), Duration::from_micros(100));
    assert!(!standard.need_fetch_more(Duration::from_micros(100)));
    assert!(!standard.need_fetch_more(Duration::from_micros(80)));
    assert!(standard.need_fetch_more(Duration::from_micros(10)));
    assert!(standard.need_fetch_more(Duration::from_micros(80)));
    assert!(!standard.need_fetch_more(Duration::from_micros(90)));
    for _ in 0..50 {
        standard.need_fetch_more(Duration::from_secs(3600));
    }
    assert!(standard.estimated_arrival_interval() < standard.max_arrival_interval());
    for _ in 0..8 {
        assert!(!standard.need_fetch_more(Duration::from_micros(10)));
    }
    assert!(standard.need_fetch_more(Duration::from_micros(10)));

    let (custom_basic, valid) = BatchTrigger::from_policy(&format!("{BATCH_POLICY_CUSTOM} {{}} "));
    assert!(valid);
    assert_eq!(custom_basic.options(), basic.options());
    let (custom_positive, valid) = BatchTrigger::from_policy(r#"{"t":0.0001}"#);
    assert!(valid);
    assert_eq!(custom_positive.options(), positive.options());
    let (custom_standard, valid) =
        BatchTrigger::from_policy(r#"{"v":1,"t":0.0001,"n":5,"w":0.2,"p":0.8,"q":0.8}"#);
    assert!(valid);
    let (standard_options, _) = BatchTrigger::from_policy(BATCH_POLICY_STANDARD);
    assert_eq!(custom_standard.options(), standard_options.options());

    let (mut probabilistic, valid) =
        BatchTrigger::from_policy(r#"{"v":2,"t":0.001,"w":0.2,"p":0.5}"#);
    assert!(valid);
    assert_eq!(probabilistic.preferred_batch_wait_size(1.0, 8), 2);
    assert_eq!(probabilistic.preferred_batch_wait_size(1.2, 8), 2);
    assert_eq!(probabilistic.turbo_wait_time(), Duration::from_millis(1));
    for expected in [false, false, false, true] {
        assert_eq!(
            probabilistic.need_fetch_more(Duration::from_micros(999)),
            expected
        );
    }
    assert!(!probabilistic.need_fetch_more(Duration::from_millis(1)));

    let (_, default_valid) = BatchTrigger::from_policy(DEFAULT_BATCH_POLICY);
    assert!(default_valid);
    for invalid in ["", "invalid", "custom", "custom {x:1}"] {
        let (trigger, valid) = BatchTrigger::from_policy(invalid);
        assert!(!valid, "{invalid}");
        assert_eq!(trigger.options(), standard_options.options(), "{invalid}");
    }

    // Keep the exact source shape visible without exporting preset constructors.
    assert_eq!(BatchPolicyOptions::default(), basic.options());
}
