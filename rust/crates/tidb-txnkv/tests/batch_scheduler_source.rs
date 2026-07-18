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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use batch::{
    BatchEntry, BatchEntryCompletion, BatchPolicyOptions, BatchScheduler, BatchTrigger,
    BATCH_POLICY_BASIC, BATCH_POLICY_CUSTOM, BATCH_POLICY_POSITIVE, BATCH_POLICY_STANDARD,
    DEFAULT_BATCH_POLICY, HIGH_TASK_PRIORITY,
};

#[derive(Debug, Default)]
struct TestCompletion {
    canceled: AtomicBool,
    failure: Mutex<Option<String>>,
}

impl TestCompletion {
    fn cancel(&self) {
        self.canceled.store(true, Ordering::Release);
    }

    fn failure(&self) -> Option<String> {
        self.failure.lock().unwrap().clone()
    }
}

impl BatchEntryCompletion for TestCompletion {
    fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::Acquire)
    }

    fn fail(&self, reason: &str) {
        *self.failure.lock().unwrap() = Some(reason.to_owned());
    }
}

fn entry<T>(payload: T) -> (BatchEntry<T>, Arc<TestCompletion>) {
    let completion = Arc::new(TestCompletion::default());
    let scheduler_completion: Arc<dyn BatchEntryCompletion> = completion.clone();
    (BatchEntry::new(payload, scheduler_completion), completion)
}

#[test]
fn test_batch_commands_builder() {
    let mut scheduler = BatchScheduler::new();

    for payload in 0..10 {
        scheduler.push(entry(payload).0);
        assert_eq!(scheduler.len(), payload + 1);
    }
    let groups = scheduler.build_with_limit(usize::MAX);
    let direct = groups.direct().expect("direct group");
    assert_eq!(direct.len(), 10);
    assert!(groups.forwarded().is_empty());
    for (index, item) in direct.entries().iter().enumerate() {
        assert_eq!(item.request_id(), (index + 1) as u64);
        assert_eq!(*item.entry().payload(), index);
        assert_eq!(item.entry().progress().request_id(), item.request_id());
        assert!(item
            .entry()
            .progress()
            .batch_selected_after_arrival()
            .is_some_and(|duration| duration > Duration::ZERO));
        let progress_state = item.entry().progress().batch_state().unwrap();
        assert!(progress_state.shares_state_with(&direct.state()));
        assert_eq!(progress_state.batch_size(), 10);
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
            let entry = entry(payload).0;
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
        assert_eq!(group.state().batch_size(), index + 2);
        assert!(group.entries().iter().all(|item| item
            .entry()
            .progress()
            .batch_state()
            .unwrap()
            .shares_state_with(&group.state())));
    }
    assert_eq!(scheduler.id_alloc(), 20);

    scheduler.reset();
    for (payload, canceled) in [true, false, true, true, false].into_iter().enumerate() {
        let (entry, completion) = entry(payload);
        if canceled {
            completion.cancel();
        }
        scheduler.push(entry);
    }
    let groups = scheduler.build_with_limit(usize::MAX);
    let direct = groups.direct().expect("direct group");
    assert_eq!(direct.len(), 2);
    assert!(direct
        .entries()
        .iter()
        .all(|item| !item.entry().completion().is_canceled()));

    scheduler.reset();
    let mut cancellation_states = Vec::new();
    for payload in 0..3 {
        let (entry, completion) = entry(payload);
        cancellation_states.push(completion);
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

    scheduler.push(entry(1).0);
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

    scheduler.push(entry(2).0.with_priority(HIGH_TASK_PRIORITY));
    scheduler.push(entry(3).0.with_priority(HIGH_TASK_PRIORITY - 1));
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

    scheduler.push(entry(4).0);
    scheduler.push(entry(5).0);
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
        let (entry, completion) = entry(payload);
        completion.cancel();
        scheduler.push(entry);
    }
    scheduler.reset();
    assert_eq!(scheduler.len(), 1);

    let mut mixed = BatchScheduler::new();
    mixed.push(entry(10).0.with_priority(HIGH_TASK_PRIORITY));
    for payload in 11..=13 {
        mixed.push(entry(payload).0);
    }
    // client-go takes `limit` entries per pass. The first pass contains the
    // high-priority entry plus one normal, then the second full chunk takes
    // both remaining normals even though the normal count overshoots `limit`.
    assert_eq!(
        mixed
            .build_with_limit(2)
            .direct()
            .expect("mixed priority group")
            .len(),
        4
    );
    assert_eq!(mixed.len(), 0);
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

    for invalid_json in [
        r#"{"t":+1}"#,
        r#"{"v":-1}"#,
        r#"{"w":1.1}"#,
        r#"{"t":1e308}"#,
    ] {
        let (_, valid) = BatchTrigger::from_policy(invalid_json);
        assert!(!valid, "{invalid_json}");
    }
    let (unknown_fields, valid) =
        BatchTrigger::from_policy(r#"{"t":0.0001,"label":"ignored","nested":{"enabled":true}}"#);
    assert!(valid);
    assert_eq!(unknown_fields.options(), positive.options());

    // Keep the exact source shape visible without exporting preset constructors.
    assert_eq!(BatchPolicyOptions::default(), basic.options());
}
