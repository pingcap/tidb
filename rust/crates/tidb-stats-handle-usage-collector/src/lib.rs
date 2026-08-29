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

//! Go `pkg/statistics/handle/usage/collector`.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const DEFAULT_CHANNEL_SIZE: usize = 10;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5 * 60);

struct QueueState<T> {
    normal: VecDeque<T>,
    high_priority: VecDeque<T>,
    closed: bool,
}

struct Shared<T> {
    state: Mutex<QueueState<T>>,
    changed: Condvar,
}

/// Go `GlobalCollector` and `globalCollector`.
pub struct GlobalCollector<T> {
    shared: Arc<Shared<T>>,
    merge: Arc<dyn Fn(T) + Send + Sync>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    close_once: Mutex<bool>,
}

impl<T: Send + 'static> GlobalCollector<T> {
    /// Go `NewGlobalCollector`.
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
            workers: Mutex::new(Vec::new()),
            close_once: Mutex::new(false),
        }
    }

    /// Go `GlobalCollector.SpawnSession`.
    #[must_use]
    pub fn spawn_session(&self) -> SessionCollector<T> {
        SessionCollector {
            shared: Arc::clone(&self.shared),
            last_update: Mutex::new(Instant::now()),
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Go `GlobalCollector.StartWorker`.
    pub fn start_worker(&self) {
        let shared = Arc::clone(&self.shared);
        let merge = Arc::clone(&self.merge);
        self.workers
            .lock()
            .expect("collector worker lock poisoned")
            .push(thread::spawn(move || loop {
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

    /// Go `GlobalCollector.Close`.
    pub fn close(&self) {
        let mut closed = self
            .close_once
            .lock()
            .expect("collector close lock poisoned");
        if *closed {
            return;
        }
        {
            let mut state = self
                .shared
                .state
                .lock()
                .expect("collector state lock poisoned");
            state.closed = true;
            self.shared.changed.notify_all();
        }
        let workers =
            std::mem::take(&mut *self.workers.lock().expect("collector worker lock poisoned"));
        for worker in workers {
            worker.join().expect("collector worker panicked");
        }
        *closed = true;
    }
}

/// Go `SessionCollector` and `sessionCollector`.
pub struct SessionCollector<T> {
    shared: Arc<Shared<T>>,
    last_update: Mutex<Instant>,
    timeout: Duration,
}

impl<T: Send + 'static> SessionCollector<T> {
    /// Go `SessionCollector.SendDelta`.
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
        if state.normal.len() >= DEFAULT_CHANNEL_SIZE {
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

    /// Go `SessionCollector.SendDeltaSync`.
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
