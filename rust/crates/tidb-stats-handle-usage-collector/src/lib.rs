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

use crossbeam_channel::{bounded, select, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const DEFAULT_CHANNEL_SIZE: usize = 10;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5 * 60);

struct Shared<T> {
    normal: (Sender<T>, Receiver<T>),
    high_priority: (Sender<T>, Receiver<T>),
    closed: Receiver<()>,
    close_once: Mutex<Option<Sender<()>>>,
}

/// Go `GlobalCollector` and `globalCollector`.
pub struct GlobalCollector<T> {
    shared: Arc<Shared<T>>,
    merge: Arc<dyn Fn(T) + Send + Sync>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl<T: Send + 'static> GlobalCollector<T> {
    /// Go `NewGlobalCollector`.
    pub fn new<F>(merge: F) -> Self
    where
        F: Fn(T) + Send + Sync + 'static,
    {
        let (close, closed) = bounded(0);
        Self {
            shared: Arc::new(Shared {
                normal: bounded(DEFAULT_CHANNEL_SIZE),
                high_priority: bounded(DEFAULT_CHANNEL_SIZE),
                closed,
                close_once: Mutex::new(Some(close)),
            }),
            merge: Arc::new(merge),
            workers: Mutex::new(Vec::new()),
        }
    }

    /// Go `GlobalCollector.SpawnSession`.
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
            .push(thread::spawn(move || {
                loop {
                    // Go's outer select prioritizes high-priority data over
                    // entering the ordinary, blocking three-way select.
                    select! {
                        recv(shared.high_priority.1) -> item => merge(item.expect("retained sender")),
                        recv(shared.closed) -> _ => break,
                        default => {
                            select! {
                                recv(shared.normal.1) -> item => merge(item.expect("retained sender")),
                                recv(shared.high_priority.1) -> item => merge(item.expect("retained sender")),
                                recv(shared.closed) -> _ => break,
                            }
                        }
                    }
                }
                // Close stops admission to the worker, not the data channels.
                // Like Go flush, drain accepted deltas before joining.
                loop {
                    select! {
                        recv(shared.high_priority.1) -> item => merge(item.expect("retained sender")),
                        recv(shared.normal.1) -> item => merge(item.expect("retained sender")),
                        default => break,
                    }
                }
            }));
    }

    /// Go `GlobalCollector.Close`.
    pub fn close(&self) {
        let mut closed = self
            .shared
            .close_once
            .lock()
            .expect("collector close lock poisoned");
        if closed.is_none() {
            return;
        }
        // Disconnecting the stop channel broadcasts to every worker. Keep
        // the close lock until they join, matching Go sync.Once + WaitGroup.
        drop(closed.take());
        let workers =
            std::mem::take(&mut *self.workers.lock().expect("collector worker lock poisoned"));
        for worker in workers {
            worker.join().expect("collector worker panicked");
        }
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
        if self.shared.normal.0.try_send(data).is_err() {
            return false;
        }
        *self
            .last_update
            .lock()
            .expect("session timestamp lock poisoned") = Instant::now();
        true
    }

    /// Go `SessionCollector.SendDeltaSync`.
    pub fn send_delta_sync(&self, data: T) -> bool {
        // Pinned Go `SpawnSession` leaves `sessionCollector.closeCh` nil, so
        // this synchronous path cannot observe `GlobalCollector.Close` and
        // still enqueues while the high-priority channel has capacity.
        self.shared
            .high_priority
            .0
            .send(data)
            .expect("retained receiver");
        *self
            .last_update
            .lock()
            .expect("session timestamp lock poisoned") = Instant::now();
        true
    }
}
