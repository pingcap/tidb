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

//! `go.uber.org/mock/gomock`'s `Controller`, for the ports of Go's
//! generated mocks.
//!
//! One [`Controller`] holds every mock's expectations, in the order they were
//! set. A call is keyed `Method|arg|arg`; an expectation's `*` argument is
//! `gomock.Any()`. The first expectation in order that matches and has calls
//! left answers a call, as gomock's call set does; an answer is a closure of
//! the call's input (`DoAndReturn`). An unexpected call is recorded, so a
//! mock called on a thread whose panic is only logged still fails
//! [`Controller::finish`].

use std::any::Any;
use std::sync::{Arc, Mutex};

/// gomock's call counts.
#[derive(Clone, Copy, Debug)]
pub enum Times {
    /// `Times(n)`; the default is once.
    N(usize),
    /// `AnyTimes()`.
    Any,
}

/// gomock's default count.
pub const ONCE: Times = Times::N(1);

type AnswerFn = Arc<dyn Fn(&dyn Any) -> Box<dyn Any + Send> + Send + Sync>;

struct Expectation {
    pattern: Vec<String>,
    remaining: Option<usize>,
    answer: AnswerFn,
}

/// gomock `Controller`.
#[derive(Default)]
pub struct Controller {
    expected: Mutex<Vec<Expectation>>,
    failures: Mutex<Vec<String>>,
}

fn matches(pattern: &[String], key: &[&str]) -> bool {
    pattern.len() == key.len() && pattern.iter().zip(key).all(|(want, got)| want == "*" || want == got)
}

impl Controller {
    /// gomock `NewController`.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// `EXPECT().Method(args).DoAndReturn(answer).Times(times)`.
    pub fn expect<T: Send + 'static>(
        &self,
        key: impl AsRef<str>,
        times: Times,
        answer: impl Fn(&dyn Any) -> T + Send + Sync + 'static,
    ) {
        self.expected.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(Expectation {
            pattern: key.as_ref().split('|').map(str::to_owned).collect(),
            remaining: match times {
                Times::N(n) => Some(n),
                Times::Any => None,
            },
            answer: Arc::new(move |input| Box::new(answer(input))),
        });
    }

    /// `EXPECT().Method(args).Return(value)`, once.
    pub fn ret<T: Clone + Send + Sync + 'static>(&self, key: impl AsRef<str>, value: T) {
        self.expect(key, ONCE, move |_| value.clone());
    }

    /// `EXPECT().Method(args).Return(value).Times(times)`.
    pub fn ret_times<T: Clone + Send + Sync + 'static>(&self, key: impl AsRef<str>, times: Times, value: T) {
        self.expect(key, times, move |_| value.clone());
    }

    /// A mock's call.
    ///
    /// # Panics
    /// No expectation answers it (recorded first), or the answer is of
    /// another type.
    pub fn call<T: 'static>(&self, key: &str, input: &dyn Any) -> T {
        let parts: Vec<&str> = key.split('|').collect();
        let answer = {
            let mut expected = self.expected.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            expected
                .iter_mut()
                .find(|expectation| expectation.remaining != Some(0) && matches(&expectation.pattern, &parts))
                .map(|expectation| {
                    if let Some(remaining) = expectation.remaining.as_mut() {
                        *remaining -= 1;
                    }
                    Arc::clone(&expectation.answer)
                })
        };
        let Some(answer) = answer else {
            self.failures.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(format!("unexpected call {key}"));
            panic!("unexpected call {key}");
        };
        *answer(input).downcast::<T>().unwrap_or_else(|_| panic!("{key} answered with the wrong type"))
    }

    /// gomock `Satisfied`: every expected call was made.
    #[must_use]
    pub fn satisfied(&self) -> bool {
        self.expected
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .all(|expectation| expectation.remaining.is_none_or(|remaining| remaining == 0))
    }

    /// gomock `Finish`: no unexpected call and no missing call.
    ///
    /// # Panics
    /// A call was unexpected or is missing.
    pub fn finish(&self) {
        let failures = self.failures.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        assert!(failures.is_empty(), "{failures:?}");
        let missing: Vec<String> = self
            .expected
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|expectation| expectation.remaining.is_some_and(|remaining| remaining > 0))
            .map(|expectation| expectation.pattern.join("|"))
            .collect();
        assert!(missing.is_empty(), "missing calls: {missing:?}");
    }
}

/// A mock's receiver: gomock matches a call against the expectations of the
/// mock it was made on, so each mock prefixes its keys with its own ID.
pub struct Receiver {
    ctrl: Arc<Controller>,
    prefix: String,
}

static NEXT_RECEIVER_ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl Receiver {
    /// A new mock's receiver on `ctrl`.
    #[must_use]
    pub fn new(ctrl: &Arc<Controller>) -> Self {
        let id = NEXT_RECEIVER_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self { ctrl: Arc::clone(ctrl), prefix: format!("mock{id}.") }
    }

    fn key(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }

    /// `EXPECT().Method(args).DoAndReturn(answer).Times(times)` on this mock.
    pub fn expect<T: Send + 'static>(
        &self,
        key: impl AsRef<str>,
        times: Times,
        answer: impl Fn(&dyn Any) -> T + Send + Sync + 'static,
    ) {
        self.ctrl.expect(self.key(key.as_ref()), times, answer);
    }

    /// `EXPECT().Method(args).Return(value)` on this mock, once.
    pub fn ret<T: Clone + Send + Sync + 'static>(&self, key: impl AsRef<str>, value: T) {
        self.ctrl.ret(self.key(key.as_ref()), value);
    }

    /// `EXPECT().Method(args).Return(value).Times(times)` on this mock.
    pub fn ret_times<T: Clone + Send + Sync + 'static>(&self, key: impl AsRef<str>, times: Times, value: T) {
        self.ctrl.ret_times(self.key(key.as_ref()), times, value);
    }

    /// A call on this mock (see [`Controller::call`]).
    pub fn call<T: 'static>(&self, key: &str, input: &dyn Any) -> T {
        self.ctrl.call(&self.key(key), input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calls_match_in_order_skipping_exhausted() {
        let ctrl = Controller::new();
        ctrl.ret("Get|1", 10);
        ctrl.ret_times("Get|*", Times::Any, 20);
        ctrl.ret("Get|1", 30);
        assert_eq!(ctrl.call::<i32>("Get|1", &()), 10);
        // the any-times expectation now answers before the later one.
        assert_eq!(ctrl.call::<i32>("Get|1", &()), 20);
        assert_eq!(ctrl.call::<i32>("Get|2", &()), 20);
        assert!(!ctrl.satisfied());
    }

    #[test]
    fn receivers_match_their_own_expectations() {
        let ctrl = Controller::new();
        let (a, b) = (Receiver::new(&ctrl), Receiver::new(&ctrl));
        a.ret_times("GetStep", Times::Any, 1);
        b.ret_times("GetStep", Times::Any, 2);
        assert_eq!(b.call::<i32>("GetStep", &()), 2);
        assert_eq!(a.call::<i32>("GetStep", &()), 1);
    }

    #[test]
    fn unexpected_calls_fail_finish() {
        let ctrl = Controller::new();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| ctrl.call::<()>("Run", &())));
        assert!(result.is_err());
        let finish = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| ctrl.finish()));
        assert!(finish.is_err());
    }
}
