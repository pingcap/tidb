// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! The pinned PD client's `pkg/deadline` owner. Deadlines start before bounded
//! admission, are watched in submission order, and cancel only on expiry.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::async_util::Cancellation;

struct Deadline {
    at: Instant,
    done: Cancellation,
    cancel: Box<dyn FnOnce() + Send>,
}

/// Completion of one accepted deadline, corresponding to closing Go's done channel.
/// Dropping this handle does not complete the operation or disarm its deadline.
pub struct DeadlineDone(Cancellation);

impl DeadlineDone {
    /// Disarms the deadline when its operation finishes, successfully or with an error.
    pub fn complete(self) {
        self.0.cancel();
    }
}

#[derive(Default)]
struct QueueState {
    entries: VecDeque<Deadline>,
    receiving: bool,
}

struct Queue {
    state: Mutex<QueueState>,
    changed: watch::Sender<()>,
    capacity: usize,
}

impl Queue {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(QueueState::default()),
            changed: watch::channel(()).0,
            capacity,
        }
    }

    async fn receive(&self, cancellation: &Cancellation) -> Option<Deadline> {
        let mut changed = self.changed.subscribe();
        loop {
            if cancellation.is_cancelled() {
                return None;
            }
            {
                let mut state = self.state.lock().unwrap();
                if let Some(deadline) = state.entries.pop_front() {
                    state.receiving = false;
                    self.changed.send_replace(());
                    return Some(deadline);
                }
                if !state.receiving {
                    // This also admits a rendezvous sender when capacity is zero.
                    state.receiving = true;
                    self.changed.send_replace(());
                }
            }
            tokio::select! {
                _ = cancellation.cancelled() => return None,
                _ = changed.changed() => {}
            }
        }
    }
}

struct Inner {
    cancellation: Cancellation,
    queue: Arc<Queue>,
    worker: AsyncMutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

/// A bounded, serial deadline watcher with a parent-owned cancellation lifetime.
#[derive(Clone)]
pub struct Watcher {
    inner: Arc<Inner>,
}

impl Watcher {
    /// Starts the watcher, preserving Go's buffered and zero-capacity admission.
    pub fn new(parent: &Cancellation, capacity: usize, source: impl Into<String>) -> Self {
        let cancellation = parent.child();
        let queue = Arc::new(Queue::new(capacity));
        let worker_cancellation = cancellation.clone();
        let worker_queue = queue.clone();
        let source = source.into();
        let worker = tokio::spawn(async move {
            log::info!("[pd] start the deadline watcher, source={source}");
            while let Some(deadline) = worker_queue.receive(&worker_cancellation).await {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline.at) => {
                        log::error!("[pd] the deadline is reached, source={source}");
                        (deadline.cancel)();
                    }
                    _ = deadline.done.cancelled() => {}
                    _ = worker_cancellation.cancelled() => break,
                }
            }
            // Rust can release queued callbacks as soon as their owner ends;
            // there is no Go timer/channel object that needs pooling or draining.
            worker_queue.state.lock().unwrap().entries.clear();
            log::info!("[pd] exit the deadline watcher, source={source}");
        });
        Self {
            inner: Arc::new(Inner {
                cancellation,
                queue,
                worker: AsyncMutex::new(Some(worker)),
            }),
        }
    }

    /// Starts the timer before waiting for queue capacity. Caller cancellation
    /// governs admission only; after admission, completion or the watcher's
    /// parent lifetime governs the deadline, as in `deadline.Watcher.Start`.
    pub async fn start(
        &self,
        caller: &Cancellation,
        timeout: Duration,
        cancel: impl FnOnce() + Send + 'static,
    ) -> Option<DeadlineDone> {
        if self.inner.cancellation.is_cancelled() || caller.is_cancelled() {
            return None;
        }
        let done = Cancellation::default();
        let mut deadline = Some(Deadline {
            at: Instant::now() + timeout,
            done: done.clone(),
            cancel: Box::new(cancel),
        });
        let queue = &self.inner.queue;
        let mut changed = queue.changed.subscribe();
        loop {
            if self.inner.cancellation.is_cancelled() || caller.is_cancelled() {
                return None;
            }
            {
                let mut state = queue.state.lock().unwrap();
                let has_capacity = if queue.capacity == 0 {
                    state.receiving && state.entries.is_empty()
                } else {
                    state.entries.len() < queue.capacity
                };
                if has_capacity {
                    state.entries.push_back(deadline.take().unwrap());
                    queue.changed.send_replace(());
                    return Some(DeadlineDone(done));
                }
            }
            tokio::select! {
                _ = self.inner.cancellation.cancelled() => return None,
                _ = caller.cancelled() => return None,
                _ = changed.changed() => {}
            }
        }
    }

    /// Cancels and joins the watcher without invoking queued timeout callbacks.
    pub async fn close(&self) {
        self.inner.cancellation.cancel();
        let mut worker = self.inner.worker.lock().await;
        if let Some(handle) = worker.as_mut() {
            // Keep the handle until joining finishes, even if this close future
            // is dropped. Concurrent close callers wait for the same completion.
            if let Err(error) = handle.await {
                if error.is_panic() {
                    std::panic::resume_unwind(error.into_panic());
                }
                log::error!("[pd] deadline watcher cancelled: {error}");
            }
            worker.take();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn next_tick() {
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    #[allow(non_snake_case)]
    async fn source_go_deadline_TestWatcher() {
        let parent = Cancellation::default();
        let watcher = Watcher::new(&parent, 10, "test");
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let done = watcher
            .start(&parent, Duration::from_millis(1), move || {
                count.fetch_add(1, Ordering::SeqCst);
            })
            .await;
        assert!(done.is_some());
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let count = calls.clone();
        watcher
            .start(&parent, Duration::from_millis(500), move || {
                count.fetch_add(1, Ordering::SeqCst);
            })
            .await
            .unwrap()
            .complete();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let dead = parent.child();
        dead.cancel();
        assert!(watcher
            .start(&dead, Duration::from_millis(1), || panic!(
                "cancelled admission"
            ))
            .await
            .is_none());
        tokio::time::sleep(Duration::from_millis(5)).await;
        watcher.close().await;
        assert!(watcher
            .start(&parent, Duration::ZERO, || panic!("closed watcher"))
            .await
            .is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn queued_deadline_uses_start_time_and_preserves_serial_watch_order() {
        let parent = Cancellation::default();
        let watcher = Watcher::new(&parent, 1, "queue");
        let first = watcher
            .start(&parent, Duration::from_secs(100), || {
                panic!("first completed")
            })
            .await
            .unwrap();
        next_tick().await;
        let expired = Cancellation::default();
        let callback = expired.clone();
        let _done = watcher
            .start(&parent, Duration::from_millis(10), move || {
                callback.cancel()
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !expired.is_cancelled(),
            "the watcher must finish the first deadline before processing the queue"
        );
        first.complete();
        next_tick().await;
        assert!(
            expired.is_cancelled(),
            "the queued deadline must not restart its timer when dequeued"
        );
        watcher.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_admission_cancels_and_does_not_enqueue() {
        for capacity in [0, 1] {
            let parent = Cancellation::default();
            let watcher = Watcher::new(&parent, capacity, "capacity");
            let first = watcher
                .start(&parent, Duration::from_secs(100), || panic!("completed"))
                .await
                .unwrap();
            next_tick().await;
            let queued = if capacity == 1 {
                Some(
                    watcher
                        .start(&parent, Duration::from_secs(100), || panic!("completed"))
                        .await
                        .unwrap(),
                )
            } else {
                None
            };
            let caller = parent.child();
            let waiting = watcher.start(&caller, Duration::ZERO, || panic!("unadmitted callback"));
            tokio::pin!(waiting);
            assert!(futures::poll!(waiting.as_mut()).is_pending());
            caller.cancel();
            assert!(waiting.await.is_none());
            first.complete();
            if let Some(done) = queued {
                done.complete();
            }
            watcher.close().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_after_admission_does_not_change_the_deadline_owner() {
        let parent = Cancellation::default();
        let caller = parent.child();
        let watcher = Watcher::new(&parent, 0, "rendezvous");
        let expired = Cancellation::default();
        let callback = expired.clone();
        let _done = watcher
            .start(&caller, Duration::from_millis(10), move || {
                callback.cancel()
            })
            .await
            .unwrap();
        caller.cancel();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(expired.is_cancelled());
        watcher.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn parent_cancellation_releases_active_queued_and_blocked_work() {
        let parent = Cancellation::default();
        let watcher = Watcher::new(&parent, 1, "close");
        let _active = watcher
            .start(&parent, Duration::from_secs(100), || {
                panic!("shutdown is not expiry")
            })
            .await
            .unwrap();
        next_tick().await;
        let _queued = watcher
            .start(&parent, Duration::from_secs(100), || {
                panic!("shutdown is not expiry")
            })
            .await
            .unwrap();
        let waiting = watcher.start(&parent, Duration::ZERO, || panic!("unadmitted callback"));
        tokio::pin!(waiting);
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        parent.cancel();
        assert!(waiting.await.is_none());
        tokio::join!(watcher.close(), watcher.close());
        assert!(watcher.inner.queue.state.lock().unwrap().entries.is_empty());
    }

    #[tokio::test]
    async fn dropping_last_watcher_stops_worker_and_releases_callbacks() {
        let parent = Cancellation::default();
        let watcher = Watcher::new(&parent, 1, "drop");
        let retained = Arc::new(());
        let callback = retained.clone();
        let _done = watcher
            .start(&parent, Duration::from_secs(100), move || drop(callback))
            .await
            .unwrap();
        drop(watcher);
        tokio::time::timeout(Duration::from_secs(1), async {
            while Arc::strong_count(&retained) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!parent.is_cancelled());
    }
    #[tokio::test(start_paused = true)]
    async fn blocked_admission_keeps_the_original_deadline() {
        let parent = Cancellation::default();
        let watcher = Watcher::new(&parent, 1, "blocked-time");
        let first = watcher
            .start(&parent, Duration::from_secs(100), || panic!("completed"))
            .await
            .unwrap();
        next_tick().await;
        let second = watcher
            .start(&parent, Duration::from_secs(100), || panic!("completed"))
            .await
            .unwrap();
        let expired = Cancellation::default();
        let callback = expired.clone();
        let third = watcher.start(&parent, Duration::from_millis(10), move || {
            callback.cancel()
        });
        tokio::pin!(third);
        assert!(futures::poll!(third.as_mut()).is_pending());
        tokio::time::sleep(Duration::from_millis(20)).await;
        first.complete();
        let _third_done = third.await.unwrap();
        second.complete();
        next_tick().await;
        assert!(
            expired.is_cancelled(),
            "admission must not restart an elapsed timeout"
        );
        watcher.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_completion_handle_does_not_disarm_zero_timeout() {
        let parent = Cancellation::default();
        let watcher = Watcher::new(&parent, 1, "zero");
        let expired = Cancellation::default();
        let callback = expired.clone();
        drop(
            watcher
                .start(&parent, Duration::ZERO, move || callback.cancel())
                .await
                .unwrap(),
        );
        next_tick().await;
        assert!(expired.is_cancelled());
        watcher.close().await;
    }
}
