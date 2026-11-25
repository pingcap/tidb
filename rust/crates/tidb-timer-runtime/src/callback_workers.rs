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

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crossbeam_channel::{Sender, bounded};

use super::CallbackDone;

struct Callback {
    body: Box<dyn FnOnce() + Send>,
    done: CallbackDone,
}

// Only idle workers count toward this limit. Blocking a callback must not
// prevent another callback, including one it starts, from running.
const MAX_IDLE_WORKERS: usize = 8;
const IDLE_TIMEOUT: Duration = Duration::from_secs(1);
const IDLE_SPIN: Duration = Duration::from_micros(24);
static IDLE_WORKERS: Mutex<Vec<Sender<Callback>>> = Mutex::new(Vec::new());

fn idle_workers() -> MutexGuard<'static, Vec<Sender<Callback>>> {
    IDLE_WORKERS.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(super) fn dispatch(body: impl FnOnce() + Send + 'static, done: CallbackDone) {
    let callback = Callback {
        body: Box::new(body),
        done,
    };
    {
        let mut idle = idle_workers();
        if let Some(sender) = idle.pop() {
            // Publish the callback before releasing the same lock the idle
            // worker uses to retire. A worker missing from the cache has work.
            callback.done.set_dispatch_pending(true);
            assert!(sender.try_send(callback).is_ok(), "an idle timer worker accepts one callback");
            return;
        }
    }
    drop(std::thread::spawn(move || run_worker(callback)));
}

fn run_worker(mut callback: Callback) {
    let (sender, receiver) = bounded(1);
    let mut reused = false;
    loop {
        let Callback { body, done } = callback;
        if reused {
            done.set_dispatch_pending(false);
        }
        // A panic exits the thread and drops the completion guard. Only a
        // worker that returned from its callback is eligible for reuse.
        body();
        {
            let mut idle = idle_workers();
            if idle.len() == MAX_IDLE_WORKERS {
                return;
            }
            idle.push(sender.clone());
        }
        // A waiter can send the next callback immediately after completion.
        drop(done);

        let start = Instant::now();
        callback = loop {
            if let Ok(next) = receiver.try_recv() {
                break next;
            }
            if start.elapsed() >= IDLE_SPIN {
                if let Ok(next) = receiver.recv_timeout(IDLE_TIMEOUT) {
                    break next;
                }
                let mut idle = idle_workers();
                if let Some(index) = idle.iter().position(|worker| worker.same_channel(&sender)) {
                    idle.swap_remove(index);
                    return;
                }
                break receiver.try_recv().expect("a claimed timer worker has a callback");
            }
            std::hint::spin_loop();
        };
        reused = true;
    }
}
