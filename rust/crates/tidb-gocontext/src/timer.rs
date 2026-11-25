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

//! Shared deadline scheduler for Go context's runtime timers.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, LazyLock, Mutex, Weak};
use std::time::Instant;

use super::{CtxError, Inner};

#[derive(Default)]
struct State {
    timers: BTreeMap<(Instant, usize), Weak<Inner>>,
    // The worker is due to wake at this instant or earlier, even if the
    // corresponding timer has since been canceled.
    wake_at: Option<Instant>,
}

#[derive(Default)]
struct Scheduler {
    state: Mutex<State>,
    changed: Condvar,
}

static SCHEDULER: LazyLock<Scheduler> = LazyLock::new(|| {
    std::thread::Builder::new()
        .name("context-deadlines".to_owned())
        .spawn(|| SCHEDULER.run())
        .expect("context deadline scheduler thread spawns");
    Scheduler::default()
});

pub(super) fn insert(deadline: Instant, ctx: &Arc<Inner>) {
    let scheduler = &*SCHEDULER;
    let mut state = scheduler.state.lock().unwrap();
    state
        .timers
        .insert((deadline, ctx.id()), Arc::downgrade(ctx));
    let wake = state.wake_at.is_none_or(|current| deadline < current);
    if wake {
        state.wake_at = Some(deadline);
        drop(state);
        scheduler.changed.notify_one();
    }
}

pub(super) fn remove(deadline: Instant, id: usize) {
    SCHEDULER
        .state
        .lock()
        .unwrap()
        .timers
        .remove(&(deadline, id));
}

#[cfg(test)]
pub(super) fn contains(deadline: Instant, id: usize) -> bool {
    SCHEDULER
        .state
        .lock()
        .unwrap()
        .timers
        .contains_key(&(deadline, id))
}

impl Scheduler {
    fn run(&self) {
        loop {
            let ctx = {
                let mut state = self.state.lock().unwrap();
                loop {
                    let Some((&(deadline, _), _)) = state.timers.first_key_value() else {
                        state.wake_at = None;
                        state = self.changed.wait(state).unwrap();
                        continue;
                    };
                    let wait = deadline.saturating_duration_since(Instant::now());
                    if wait.is_zero() {
                        break state.timers.pop_first().unwrap().1;
                    }
                    state.wake_at = Some(deadline);
                    state = self.changed.wait_timeout(state, wait).unwrap().0;
                }
            };
            // Cancellation can also remove timers, so run it outside the
            // scheduler's lock.
            if let Some(ctx) = ctx.upgrade() {
                ctx.cancel(CtxError::DeadlineExceeded, None);
            }
        }
    }
}
