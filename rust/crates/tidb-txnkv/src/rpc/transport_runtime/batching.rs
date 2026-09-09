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

//! Go conn_batch.go collection state, retained per store by the transport loop.

use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;

use super::super::batch::{
    BatchSubmission, BatchTrigger, DEFAULT_BATCH_POLICY, MAX_BATCH_COMMANDS,
};
use super::WorkerCommand;

const DEFAULT_BATCH_WAIT_SIZE: usize = 8;

pub(super) enum Admission {
    Publish(Vec<BatchSubmission>),
    Pending,
    Yield,
}

struct PendingBatch {
    submissions: Vec<BatchSubmission>,
    entries: usize,
    target: usize,
    deadline: Option<Instant>,
}

struct StoreCollector {
    trigger: BatchTrigger,
    latest_arrival: Option<Instant>,
    average_size: f64,
    pending: Option<PendingBatch>,
}

impl Default for StoreCollector {
    fn default() -> Self {
        Self {
            trigger: BatchTrigger::from_policy(DEFAULT_BATCH_POLICY).0,
            latest_arrival: None,
            average_size: DEFAULT_BATCH_WAIT_SIZE as f64,
            pending: None,
        }
    }
}

impl StoreCollector {
    fn record_size(&mut self, mut count: usize) {
        // Caller vectors can exceed Go's one-entry submission API. Account
        // for each bounded collection, not one oversized synthetic batch.
        while count > 0 {
            let size = count.min(MAX_BATCH_COMMANDS);
            self.average_size = 0.2 * size as f64 + 0.8 * self.average_size;
            count -= size;
        }
    }

    fn finish(&mut self) -> Option<Vec<BatchSubmission>> {
        let batch = self.pending.take()?;
        self.record_size(batch.entries);
        Some(batch.submissions)
    }
}

/// Waiting batches retain data, not the transport thread. The event loop can
/// serve another address or a lifecycle command until the earliest deadline.
#[derive(Default)]
pub(super) struct Collectors {
    stores: HashMap<String, StoreCollector>,
}

impl Collectors {
    pub(super) fn push(
        &mut self,
        address: &str,
        submissions: Vec<BatchSubmission>,
        now: Instant,
    ) -> Admission {
        let count = submissions.iter().map(|s| s.entries.len()).sum::<usize>();
        let mut arrivals = submissions
            .iter()
            .flat_map(|s| &s.entries)
            .map(|entry| entry.arrived_at());
        let Some(head_arrival) = arrivals.next() else {
            return Admission::Publish(submissions);
        };
        let latest = arrivals.fold(head_arrival, Instant::max);
        let store = self.stores.entry(address.to_owned()).or_default();
        let interval = store
            .latest_arrival
            .map(|previous| head_arrival.saturating_duration_since(previous))
            .unwrap_or_default();
        store.latest_arrival = Some(
            store
                .latest_arrival
                .map_or(latest, |previous| previous.max(latest)),
        );
        if let Some(batch) = store.pending.as_mut() {
            batch.entries = batch.entries.saturating_add(count);
            batch.submissions.extend(submissions);
        } else {
            let wait = store.trigger.turbo_wait_time();
            if count >= MAX_BATCH_COMMANDS
                || wait.is_zero()
                || interval.is_zero()
                || !store.trigger.need_fetch_more(interval)
            {
                store.record_size(count);
                return Admission::Publish(submissions);
            }
            store.pending = Some(PendingBatch {
                submissions,
                entries: count,
                target: store
                    .trigger
                    .preferred_batch_wait_size(store.average_size, DEFAULT_BATCH_WAIT_SIZE),
                deadline: Some(now + wait),
            });
        }
        let batch = store.pending.as_mut().expect("pending collection");
        if batch.entries >= MAX_BATCH_COMMANDS
            || batch.submissions.len() >= MAX_BATCH_COMMANDS
            || batch.deadline.is_some_and(|deadline| now >= deadline)
        {
            return Admission::Publish(store.finish().expect("pending collection"));
        }
        // Go stops its timer before yielding once after reaching its target.
        if batch.entries >= batch.target && batch.deadline.take().is_some() {
            Admission::Yield
        } else {
            Admission::Pending
        }
    }

    pub(super) fn finish_turn(
        &mut self,
        address: &str,
        receiver: &mut mpsc::UnboundedReceiver<WorkerCommand>,
        pending: &mut Option<WorkerCommand>,
    ) -> Option<Vec<BatchSubmission>> {
        let store = self.stores.get_mut(address)?;
        let batch = store.pending.take()?;
        let submissions = collect_more(address, batch.submissions, receiver, pending);
        // Go resumes the nonblocking drain after yielding, and updates the
        // estimate with the final collected length before selecting a channel.
        for entry in submissions.iter().flat_map(|s| &s.entries) {
            let arrived = entry.arrived_at();
            store.latest_arrival = Some(
                store
                    .latest_arrival
                    .map_or(arrived, |previous| previous.max(arrived)),
            );
        }
        store.record_size(submissions.iter().map(|s| s.entries.len()).sum());
        Some(submissions)
    }

    pub(super) fn finish_address(&mut self, address: &str) -> Option<Vec<BatchSubmission>> {
        self.stores.get_mut(address)?.finish()
    }

    pub(super) fn remove(&mut self, address: &str) {
        debug_assert!(self.stores.get(address).is_none_or(|s| s.pending.is_none()));
        self.stores.remove(address);
    }

    pub(super) fn next_deadline(&self) -> Option<Instant> {
        self.stores
            .values()
            .filter_map(|s| s.pending.as_ref().and_then(|p| p.deadline))
            .min()
    }

    pub(super) fn take_due(&mut self, now: Instant) -> Option<(String, Vec<BatchSubmission>)> {
        let (address, store) = self.stores.iter_mut().find(|(_, s)| {
            s.pending
                .as_ref()
                .and_then(|p| p.deadline)
                .is_some_and(|deadline| deadline <= now)
        })?;
        Some((address.clone(), store.finish().expect("due collection")))
    }

    pub(super) fn finish_all(&mut self) -> Vec<(String, Vec<BatchSubmission>)> {
        self.stores
            .iter_mut()
            .filter_map(|(address, store)| {
                store
                    .finish()
                    .map(|submissions| (address.clone(), submissions))
            })
            .collect()
    }
}

pub(super) fn collect(
    address: &str,
    first: BatchSubmission,
    receiver: &mut mpsc::UnboundedReceiver<WorkerCommand>,
    pending: &mut Option<WorkerCommand>,
) -> Vec<BatchSubmission> {
    collect_more(address, vec![first], receiver, pending)
}

fn collect_more(
    address: &str,
    mut submissions: Vec<BatchSubmission>,
    receiver: &mut mpsc::UnboundedReceiver<WorkerCommand>,
    pending: &mut Option<WorkerCommand>,
) -> Vec<BatchSubmission> {
    let mut entries = submissions.iter().map(|s| s.entries.len()).sum::<usize>();
    // Bound empty submissions too, so a producer cannot starve lifecycle work.
    // Publication separately splits oversized caller vectors into wire packets.
    while pending.is_none()
        && entries < MAX_BATCH_COMMANDS
        && submissions.len() < MAX_BATCH_COMMANDS
    {
        match receiver.try_recv() {
            Ok(WorkerCommand::BatchSubmit {
                address: next,
                entries: next_entries,
                call,
                reply,
            }) if next == address => {
                entries = entries.saturating_add(next_entries.len());
                submissions.push(BatchSubmission {
                    entries: next_entries,
                    call,
                    reply,
                });
            }
            Ok(command) => *pending = Some(command),
            Err(_) => break,
        }
    }
    submissions
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn submission() -> BatchSubmission {
        use crate::rpc::batch::{BatchCommandEntry, BatchCommandTag, OpaqueBatchCommand};
        use crate::rpc::{completion_pair, CompletionRunLoop};
        let (completion, _) = completion_pair(CompletionRunLoop::new(), || {});
        BatchSubmission {
            entries: vec![BatchCommandEntry::new(
                OpaqueBatchCommand::new(BatchCommandTag::Empty, vec![]),
                completion.into(),
            )],
            call: None,
            reply: None,
        }
    }

    #[test]
    fn test_batch_policy_collection() {
        // client-go client_test.go TestBatchPolicy/standard: the same arrival
        // sequence must drive the live collector, not only the pure trigger.
        let mut collectors = Collectors::default();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let mut queued = None;
        for (micros, waits) in [
            (100, false),
            (80, false),
            (10, true),
            (80, true),
            (90, false),
        ] {
            let request = submission();
            let arrived = request.entries[0].arrived_at();
            let store = collectors.stores.entry("store".to_owned()).or_default();
            store.latest_arrival = Some(arrived - Duration::from_micros(micros));
            store.average_size = 1.8;
            let now = Instant::now();
            let result = collectors.push("store", vec![request], now);
            assert_eq!(
                matches!(result, Admission::Pending | Admission::Yield),
                waits
            );
            if waits {
                assert!(matches!(result, Admission::Pending));
                assert_eq!(
                    collectors.next_deadline(),
                    Some(now + Duration::from_micros(100))
                );
                assert!(collectors
                    .take_due(now + Duration::from_micros(99))
                    .is_none());
                // Another store's first request has no arrival history. It
                // publishes without waiting behind this store's timer.
                assert!(matches!(
                    collectors.push("other", vec![submission()], now),
                    Admission::Publish(_)
                ));
                collectors.finish_address("other");
                collectors.remove("other");
                let Admission::Yield = collectors.push("store", vec![submission()], now) else {
                    panic!("reaching target two must yield once");
                };
                // conn_batch.go stops its timer before the final drain/yield.
                assert!(collectors.next_deadline().is_none());
                assert!(collectors.take_due(now + Duration::from_secs(1)).is_none());
                let BatchSubmission {
                    entries,
                    call,
                    reply,
                } = submission();
                sender
                    .send(WorkerCommand::BatchSubmit {
                        address: "store".to_owned(),
                        entries,
                        call,
                        reply,
                    })
                    .unwrap();
                assert_eq!(
                    collectors
                        .finish_turn("store", &mut receiver, &mut queued)
                        .unwrap()
                        .len(),
                    3
                );
                assert!(collectors
                    .finish_turn("store", &mut receiver, &mut queued)
                    .is_none());
                assert!(collectors.next_deadline().is_none());
            }
        }
        // A sparse head after idle suppresses waiting, as the source trigger
        // caps long intervals and lets the estimate recover gradually.
        let request = submission();
        collectors.stores.get_mut("store").unwrap().latest_arrival =
            Some(request.entries[0].arrived_at() - Duration::from_secs(1));
        assert!(matches!(
            collectors.push("store", vec![request], Instant::now()),
            Admission::Publish(_)
        ));
    }

    #[test]
    fn collection_timeout_and_lifecycle_finish_once() {
        let mut collectors = Collectors::default();
        let now = Instant::now();
        let request = submission();
        let mut state = StoreCollector::default();
        state.latest_arrival = Some(request.entries[0].arrived_at() - Duration::from_micros(1));
        collectors.stores.insert("store".to_owned(), state);
        assert!(matches!(
            collectors.push("store", vec![request], now),
            Admission::Pending
        ));
        assert_eq!(
            collectors
                .take_due(now + Duration::from_micros(100))
                .unwrap()
                .1
                .len(),
            1
        );
        assert!(collectors
            .take_due(now + Duration::from_micros(100))
            .is_none());
        let request = submission();
        collectors.stores.get_mut("store").unwrap().latest_arrival =
            Some(request.entries[0].arrived_at() - Duration::from_micros(1));
        assert!(matches!(
            collectors.push("store", vec![request], now),
            Admission::Pending | Admission::Yield
        ));
        assert_eq!(collectors.finish_address("store").unwrap().len(), 1);
        collectors.remove("store");
        assert!(collectors.finish_all().is_empty());
        assert!(collectors.next_deadline().is_none());
    }

    fn empty_submission() -> BatchSubmission {
        BatchSubmission {
            entries: Vec::new(),
            call: None,
            reply: None,
        }
    }

    #[test]
    fn empty_submissions_are_bounded_and_do_not_wait_for_live_senders() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let mut pending = None;
        assert_eq!(
            collect("store", empty_submission(), &mut receiver, &mut pending).len(),
            1
        );
        for _ in 0..MAX_BATCH_COMMANDS {
            let BatchSubmission {
                entries,
                call,
                reply,
            } = empty_submission();
            sender
                .send(WorkerCommand::BatchSubmit {
                    address: "store".to_owned(),
                    entries,
                    call,
                    reply,
                })
                .unwrap();
        }
        assert_eq!(
            collect("store", empty_submission(), &mut receiver, &mut pending).len(),
            MAX_BATCH_COMMANDS
        );
        assert!(pending.is_none());
        assert!(matches!(
            receiver.try_recv(),
            Ok(WorkerCommand::BatchSubmit { .. })
        ));
    }
}
