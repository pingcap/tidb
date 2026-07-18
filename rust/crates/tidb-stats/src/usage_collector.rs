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

//! Bounded statistics usage collection from
//! `pkg/statistics/handle/usage/collector/collector.go`.
//!
//! The source keeps separate normal and high-priority channels, gives normal
//! sends a non-blocking path, blocks synchronous sends until the high-priority
//! queue accepts them, and drains pending values when the worker closes. This
//! leaf preserves that queue/worker boundary while leaving statistics-specific
//! merge maps, persistence, and session lifecycle to their callers.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Source default queue capacity for both normal and high-priority updates.
pub const DEFAULT_CHANNEL_SIZE: usize = 10;

/// Source timeout after which a normal send becomes synchronous.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5 * 60);

struct QueueState<T> {
    normal: VecDeque<T>,
    high_priority: VecDeque<T>,
    closed: bool,
}

struct Shared<T> {
    state: Mutex<QueueState<T>>,
    changed: Condvar,
}

/// Global worker-backed collector for caller-owned statistics deltas.
pub struct GlobalCollector<T> {
    shared: Arc<Shared<T>>,
    merge: Arc<dyn Fn(T) + Send + Sync>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl<T: Send + 'static> GlobalCollector<T> {
    /// Creates a collector whose merge callback runs serially on its worker.
    #[must_use]
    pub fn new<F>(merge: F) -> Self
    where
        F: Fn(T) + Send + Sync + 'static,
    {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(QueueState {
                    normal: VecDeque::with_capacity(DEFAULT_CHANNEL_SIZE),
                    high_priority: VecDeque::with_capacity(DEFAULT_CHANNEL_SIZE),
                    closed: false,
                }),
                changed: Condvar::new(),
            }),
            merge: Arc::new(merge),
            worker: Mutex::new(None),
        }
    }

    /// Creates a session sender attached to this collector.
    #[must_use]
    pub fn spawn_session(&self) -> SessionCollector<T> {
        SessionCollector {
            shared: Arc::clone(&self.shared),
            last_update: Mutex::new(Instant::now()),
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Starts the single serial merge worker. Repeated calls are no-ops.
    pub fn start_worker(&self) {
        let mut worker = self.worker.lock().expect("collector worker lock poisoned");
        if worker.is_some() {
            return;
        }
        let shared = Arc::clone(&self.shared);
        let merge = Arc::clone(&self.merge);
        *worker = Some(thread::spawn(move || loop {
            let item = {
                let mut state = shared.state.lock().expect("collector state lock poisoned");
                loop {
                    if let Some(item) = state.high_priority.pop_front() {
                        shared.changed.notify_all();
                        break Some(item);
                    }
                    if let Some(item) = state.normal.pop_front() {
                        shared.changed.notify_all();
                        break Some(item);
                    }
                    if state.closed {
                        break None;
                    }
                    state = shared
                        .changed
                        .wait(state)
                        .expect("collector state lock poisoned while waiting");
                }
            };

            match item {
                Some(item) => merge(item),
                None => break,
            }
        }));
    }

    /// Closes the queues, drains values already accepted, and joins the worker.
    pub fn close(&self) {
        {
            let mut state = self
                .shared
                .state
                .lock()
                .expect("collector state lock poisoned");
            if state.closed {
                return;
            }
            state.closed = true;
            self.shared.changed.notify_all();
        }
        if let Some(worker) = self
            .worker
            .lock()
            .expect("collector worker lock poisoned")
            .take()
        {
            worker.join().expect("collector worker panicked");
        }
    }
}

impl<T> Drop for GlobalCollector<T> {
    fn drop(&mut self) {
        {
            let mut state = self
                .shared
                .state
                .lock()
                .expect("collector state lock poisoned");
            state.closed = true;
            self.shared.changed.notify_all();
        }
        if let Some(worker) = self
            .worker
            .get_mut()
            .expect("collector worker lock poisoned")
            .take()
        {
            let _ = worker.join();
        }
    }
}

/// Session-local sender for a [`GlobalCollector`].
pub struct SessionCollector<T> {
    shared: Arc<Shared<T>>,
    last_update: Mutex<Instant>,
    timeout: Duration,
}

impl<T: Send + 'static> SessionCollector<T> {
    /// Sends a normal-priority delta without blocking on a full queue.
    pub fn send_delta(&self, data: T) -> bool {
        let expired = self
            .last_update
            .lock()
            .expect("session timestamp lock poisoned")
            .elapsed()
            > self.timeout;
        if expired {
            return self.send_delta_sync(data);
        }

        let mut state = self
            .shared
            .state
            .lock()
            .expect("collector state lock poisoned");
        if state.closed || state.normal.len() >= DEFAULT_CHANNEL_SIZE {
            return false;
        }
        state.normal.push_back(data);
        *self
            .last_update
            .lock()
            .expect("session timestamp lock poisoned") = Instant::now();
        self.shared.changed.notify_one();
        true
    }

    /// Sends a high-priority delta, waiting for queue capacity or closure.
    pub fn send_delta_sync(&self, data: T) -> bool {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("collector state lock poisoned");
        loop {
            if state.closed {
                return false;
            }
            if state.high_priority.len() < DEFAULT_CHANNEL_SIZE {
                state.high_priority.push_back(data);
                *self
                    .last_update
                    .lock()
                    .expect("session timestamp lock poisoned") = Instant::now();
                self.shared.changed.notify_one();
                return true;
            }
            state = self
                .shared
                .changed
                .wait(state)
                .expect("collector state lock poisoned while waiting");
        }
    }
}
