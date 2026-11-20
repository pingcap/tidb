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

//! Go master `pkg/mvservice/time_proxy.go` and its `intest` build,
//! `time_proxy_intest.go`: the package's clock, which a test replaces with
//! a [`MockTimeModule`]. Rust has no build tags, so the mock is always
//! compiled and the real clock answers until a test installs one.
//!
//! Go durations are `i64` nanoseconds here, as `tidb_timer::go_time`
//! keeps them.

use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use crossbeam_channel::{bounded, never, Receiver, Sender};
use tidb_timer::go_time::GoTime;

/// Go `MockTimeModule`: a controllable clock for tests, with virtual
/// Now/After/Sleep/Since/Until advanced by hand.
pub struct MockTimeModule {
    inner: Mutex<MockInner>,
}

struct MockInner {
    now: GoTime,
    auto_advance_on_sleep: bool,
    after_waiters: Vec<(GoTime, Sender<Instant>)>,
}

impl MockTimeModule {
    /// Go `NewMockTimeModule`.
    #[must_use]
    pub fn new(initial: GoTime) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(MockInner { now: initial, auto_advance_on_sleep: true, after_waiters: Vec::new() }),
        })
    }

    /// Go `SetAutoAdvanceOnSleep`.
    pub fn set_auto_advance_on_sleep(&self, v: bool) {
        self.inner.lock().unwrap().auto_advance_on_sleep = v;
    }

    /// Go `Now`.
    #[must_use]
    pub fn now(&self) -> GoTime {
        self.inner.lock().unwrap().now.clone()
    }

    /// Go `Since`.
    #[must_use]
    pub fn since(&self, t: &GoTime) -> i64 {
        self.now().sub(t)
    }

    /// Go `Until`.
    #[must_use]
    pub fn until(&self, t: &GoTime) -> i64 {
        t.sub(&self.now())
    }

    /// Go `Set`: sets virtual time and fires due waiters.
    pub fn set(&self, now: GoTime) {
        self.set_now(now);
    }

    /// Go `Advance`: moves virtual time forward and fires due waiters.
    pub fn advance(&self, d: i64) {
        let next = self.now().add(d);
        self.set_now(next);
    }

    fn set_now(&self, now: GoTime) {
        let mut inner = self.inner.lock().unwrap();
        inner.now = now;
        Self::fire_due_waiters_locked(&mut inner);
    }

    /// Go `After`: a channel that receives once virtual time reaches now+d.
    #[must_use]
    pub fn after(&self, d: i64) -> Receiver<Instant> {
        let mut inner = self.inner.lock().unwrap();
        let (tx, rx) = bounded(1);
        if d <= 0 {
            let _ = tx.send(Instant::now());
            return rx;
        }
        let deadline = inner.now.add(d);
        inner.after_waiters.push((deadline, tx));
        rx
    }

    /// Go `Sleep`: auto-advances the clock or blocks on `After(d)`.
    pub fn sleep(&self, d: i64) {
        if d <= 0 {
            return;
        }
        let auto_advance = self.inner.lock().unwrap().auto_advance_on_sleep;
        if auto_advance {
            self.advance(d);
            return;
        }
        let _ = self.after(d).recv();
    }

    /// Go `len(module.afterWaiters)`.
    #[cfg(test)]
    pub(crate) fn after_waiters_len(&self) -> usize {
        self.inner.lock().unwrap().after_waiters.len()
    }

    fn fire_due_waiters_locked(inner: &mut MockInner) {
        let now = inner.now.clone();
        inner.after_waiters.retain(|(deadline, tx)| {
            if !deadline.after(&now) {
                let _ = tx.send(Instant::now());
                return false;
            }
            true
        });
    }
}

static ACTIVE_MOCK_TIME_MODULE: RwLock<Option<Arc<MockTimeModule>>> = RwLock::new(None);

/// Go `InstallMockTimeModuleForTest`: installs `module` into the time proxy;
/// the returned guard restores the previous clock.
#[must_use]
pub fn install_mock_time_module_for_test(module: Arc<MockTimeModule>) -> MockTimeGuard {
    let previous = ACTIVE_MOCK_TIME_MODULE.write().unwrap().replace(module);
    MockTimeGuard(Some(previous))
}

/// Go `activeMockTimeModule.Store(nil)` for a test: no mock clock until the
/// guard restores the previous one.
#[cfg(test)]
pub(crate) fn uninstall_mock_time_module_for_test() -> MockTimeGuard {
    let previous = ACTIVE_MOCK_TIME_MODULE.write().unwrap().take();
    MockTimeGuard(Some(previous))
}

/// Restores the previous clock on drop.
pub struct MockTimeGuard(Option<Option<Arc<MockTimeModule>>>);

impl Drop for MockTimeGuard {
    fn drop(&mut self) {
        if let Some(previous) = self.0.take() {
            *ACTIVE_MOCK_TIME_MODULE.write().unwrap() = previous;
        }
    }
}

fn current_mock_time_module() -> Option<Arc<MockTimeModule>> {
    ACTIVE_MOCK_TIME_MODULE.read().unwrap().clone()
}

fn std_duration(d: i64) -> std::time::Duration {
    std::time::Duration::from_nanos(u64::try_from(d).unwrap_or(0))
}

/// Go `mvsNow`.
pub fn mvs_now() -> GoTime {
    match current_mock_time_module() {
        Some(m) => m.now(),
        None => GoTime::now(),
    }
}

/// Go `mvsAfter`.
pub(crate) fn mvs_after(d: i64) -> Receiver<Instant> {
    match current_mock_time_module() {
        Some(m) => m.after(d),
        None => crossbeam_channel::after(std_duration(d)),
    }
}

/// Go `mvsSince`.
pub fn mvs_since(t: &GoTime) -> i64 {
    mvs_now().sub(t)
}

/// Go `mvsUntil`.
pub(crate) fn mvs_until(t: &GoTime) -> i64 {
    t.sub(&mvs_now())
}

/// Go `mvsSleep`.
pub fn mvs_sleep(d: i64) {
    match current_mock_time_module() {
        Some(m) => m.sleep(d),
        None => std::thread::sleep(std_duration(d)),
    }
}

/// Go `mvsUnix`.
pub fn mvs_unix(sec: i64, nsec: i64) -> GoTime {
    GoTime::from_unix(sec, nsec)
}

/// Go `mvsUnixMilli`. Go's time range reaches far past chrono's, so a
/// millisecond beyond chrono's latest instant (the scheduler's
/// `maxNextScheduleTs` sentinel, 9e18) reads as that instant: still later
/// than any real time, and Go's `Until` of either saturates alike.
pub(crate) fn mvs_unix_milli(ms: i64) -> GoTime {
    let max = chrono::DateTime::<chrono::Utc>::MAX_UTC.timestamp_millis();
    let min = chrono::DateTime::<chrono::Utc>::MIN_UTC.timestamp_millis();
    GoTime::from_unix_milli(ms.clamp(min, max))
}

/// Go `time.Time.UnixMilli`.
pub(crate) fn unix_milli(t: &GoTime) -> i64 {
    t.instant().timestamp_millis()
}

/// Go `mvsTimer`: a timer on the package clock. Go's timer keeps one `C`
/// and fires into it; this one hands out the channel of its current arming
/// ([`Self::c`]), which a stop disarms, so a stale firing never reaches a
/// later `select`.
pub(crate) struct MvsTimer {
    inner: Mutex<TimerInner>,
}

struct TimerInner {
    rx: Receiver<Instant>,
    deadline: Option<GoTime>,
}

impl MvsTimer {
    /// Go `mvsNewTimer`.
    pub(crate) fn new(d: i64) -> Self {
        let timer = Self { inner: Mutex::new(TimerInner { rx: never(), deadline: None }) };
        timer.reset(d);
        timer
    }

    /// Go `C` of the current arming.
    pub(crate) fn c(&self) -> Receiver<Instant> {
        self.inner.lock().unwrap().rx.clone()
    }

    fn active(inner: &TimerInner) -> bool {
        inner.deadline.as_ref().is_some_and(|deadline| mvs_now().before(deadline))
    }

    /// Go `Stop`: whether the timer was still armed.
    pub(crate) fn stop(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let was_active = Self::active(&inner);
        inner.rx = never();
        inner.deadline = None;
        was_active
    }

    /// Go `Reset`: whether the timer was still armed.
    pub(crate) fn reset(&self, d: i64) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let was_active = Self::active(&inner);
        inner.deadline = Some(mvs_now().add(d.max(0)));
        inner.rx = mvs_after(d);
        was_active
    }
}
