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

//! Go `runtime/pprof`'s profiler labels (`pprof.Labels`, `WithLabels`,
//! `SetGoroutineLabels`) for threads.
//!
//! Go's CPU profiler copies the running goroutine's labels into each sample.
//! A pprof-rs sample carries its thread and time instead, so setting a
//! thread's labels appends to that thread's timeline, and the profile encoder
//! reads the labels a sample's thread held at the sample's time.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, SystemTime};

/// Go `pprof.LabelSet`: label keys and their values.
pub type LabelSet = Arc<BTreeMap<String, String>>;

/// How long a thread keeps a replaced label set: a profile window (one
/// second, Go `DefProfileDuration`) is encoded within this time of its
/// samples.
const TIMELINE_RETENTION: Duration = Duration::from_secs(10);

thread_local! {
    static CURRENT: RefCell<Option<LabelSet>> = const { RefCell::new(None) };
}

type Timeline = VecDeque<(SystemTime, Option<LabelSet>)>;

static TIMELINES: LazyLock<Mutex<HashMap<u64, Timeline>>> = LazyLock::new(Mutex::default);

/// The thread identity pprof-rs stores in each sample (`pthread_self`).
#[must_use]
pub fn current_thread_id() -> u64 {
    nix::sys::pthread::pthread_self() as u64
}

/// Go `pprof.WithLabels(ctx, pprof.Labels(pairs...))`: `parent`'s labels
/// with `pairs` added, a later key replacing an earlier one.
#[must_use]
pub fn with_labels(parent: Option<&LabelSet>, pairs: &[(&str, String)]) -> LabelSet {
    let mut labels = parent.map(|labels| (**labels).clone()).unwrap_or_default();
    for (key, value) in pairs {
        labels.insert((*key).to_owned(), value.clone());
    }
    Arc::new(labels)
}

/// Go `pprof.SetGoroutineLabels(ctx)`: the labels this thread's samples carry
/// from now on.
pub fn set_goroutine_labels(labels: Option<LabelSet>) {
    CURRENT.with(|current| *current.borrow_mut() = labels.clone());
    let now = SystemTime::now();
    let mut timelines = TIMELINES.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let timeline = timelines.entry(current_thread_id()).or_default();
    timeline.push_back((now, labels));
    // Keep the newest replaced set an old sample may still need.
    while timeline.len() > 1 && timeline[1].0 + TIMELINE_RETENTION < now {
        timeline.pop_front();
    }
}

/// The labels this thread set last (Go `ctx`'s labels), which a thread doing
/// work for it carries over (Go goroutines inherit their creator's labels).
#[must_use]
pub fn goroutine_labels() -> Option<LabelSet> {
    CURRENT.with(|current| current.borrow().clone())
}

/// Go's `go f()`: a goroutine starts with its creator's labels. Wraps `f`
/// to run, on whatever thread runs it, with the calling thread's labels.
pub fn inherit<R>(f: impl FnOnce() -> R) -> impl FnOnce() -> R {
    let labels = goroutine_labels();
    move || {
        if labels.is_some() {
            set_goroutine_labels(labels);
        }
        f()
    }
}

/// [`inherit`] for a task: each poll runs with the creating thread's labels
/// and gives the polling thread its own back after.
pub fn inherit_future<F: std::future::Future>(future: F) -> impl std::future::Future<Output = F::Output> {
    let labels = goroutine_labels();
    let mut future = Box::pin(future);
    std::future::poll_fn(move |cx| {
        let Some(labels) = &labels else {
            return future.as_mut().poll(cx);
        };
        let polling = goroutine_labels();
        set_goroutine_labels(Some(Arc::clone(labels)));
        let poll = future.as_mut().poll(cx);
        set_goroutine_labels(polling);
        poll
    })
}

/// The labels thread `thread_id` held at `at`.
#[must_use]
pub fn labels_at(thread_id: u64, at: SystemTime) -> Option<LabelSet> {
    let timelines = TIMELINES.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    timelines
        .get(&thread_id)?
        .iter()
        .rev()
        .find(|(since, _)| *since <= at)
        .and_then(|(_, labels)| labels.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sample_reads_the_labels_its_thread_held_at_its_time() {
        let thread = current_thread_id();
        let before = SystemTime::now();
        let sql = with_labels(None, &[("sql_digest", "aa".to_owned())]);
        set_goroutine_labels(Some(Arc::clone(&sql)));
        let with_sql = SystemTime::now();
        let plan = with_labels(Some(&sql), &[("plan_digest", "bb".to_owned()), ("sql_digest", "cc".to_owned())]);
        std::thread::sleep(Duration::from_millis(2));
        set_goroutine_labels(Some(Arc::clone(&plan)));
        let with_plan = SystemTime::now();

        assert_eq!(labels_at(thread, before - Duration::from_secs(1)), None);
        assert_eq!(labels_at(thread, with_sql), Some(sql));
        assert_eq!(labels_at(thread, with_plan).unwrap()["sql_digest"], "cc");
        assert_eq!(labels_at(thread, with_plan).unwrap()["plan_digest"], "bb");
        assert_eq!(goroutine_labels(), Some(plan));
        assert_eq!(labels_at(thread + 1, with_plan), None);
    }

    /// Go goroutines inherit their creator's labels; so do a spawned
    /// thread's closure and a task's polls, which leave the polling thread's
    /// own labels in place afterwards.
    #[test]
    fn spawned_work_inherits_the_creators_labels() {
        let sql = with_labels(None, &[("sql_digest", "aa".to_owned())]);
        set_goroutine_labels(Some(Arc::clone(&sql)));
        let worker = std::thread::spawn(inherit(goroutine_labels)).join().unwrap();
        assert_eq!(worker, Some(Arc::clone(&sql)));
        let unlabeled = std::thread::spawn(goroutine_labels).join().unwrap();
        assert_eq!(unlabeled, None);

        let task = inherit_future(async { goroutine_labels() });
        let polled = std::thread::spawn(move || {
            let own = with_labels(None, &[("sql_digest", "bb".to_owned())]);
            set_goroutine_labels(Some(Arc::clone(&own)));
            let waker = std::task::Waker::noop();
            let mut task = std::pin::pin!(task);
            let std::task::Poll::Ready(seen) = std::future::Future::poll(task.as_mut(), &mut std::task::Context::from_waker(waker)) else {
                panic!("ready")
            };
            (seen, goroutine_labels() == Some(own))
        })
        .join()
        .unwrap();
        assert_eq!(polled, (Some(sql), true));
    }
}
