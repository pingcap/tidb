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

//! Complete transcreation of `pkg/util/intest`.
//!
//! Go's `intest` and `enableassert` build tags map to same-named Cargo
//! features. Unit tests also behave like the repository's canonical
//! `-tags=intest,deadlock` invocation. Exported mutable booleans become atomic
//! booleans so callers retain the source toggles without introducing a Rust
//! data race. The package's init-time failpoint is represented by the same
//! `GO_FAILPOINTS=/enableInternalCheck=return(true)` environment contract.
//!
//! Go's variadic `fmt.Sprintf` arguments become preformatted Rust strings:
//! callers use `format!(...)` before the `_with_message` variants. This removes
//! an untyped variadic edge while preserving every observable panic message.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Once;

/// Whether this build is running with TiDB's `intest` behavior.
pub const IN_TEST: bool = cfg!(any(feature = "intest", test));

/// Whether ordinary assertions are enabled.
pub static ENABLE_ASSERT: AtomicBool = AtomicBool::new(cfg!(any(
    feature = "intest",
    feature = "enableassert",
    test
)));

/// General runtime switch for internal checks.
pub static ENABLE_INTERNAL_CHECK: AtomicBool = AtomicBool::new(cfg!(any(
    feature = "intest",
    feature = "enableassert",
    test
)));

static ENVIRONMENT_INIT: Once = Once::new();

fn initialize_environment_failpoint() {
    ENVIRONMENT_INIT.call_once(|| {
        let enabled = std::env::var_os("GO_FAILPOINTS").is_some_and(|value| {
            let value = value.to_string_lossy();
            value.split(';').map(str::trim).any(|entry| {
                entry == "/enableInternalCheck=return(true)"
                    || entry == "enableInternalCheck=return(true)"
            })
        });
        if enabled {
            ENABLE_INTERNAL_CHECK.store(true, Ordering::Relaxed);
            ENABLE_ASSERT.store(true, Ordering::Relaxed);
        }
    });
}

fn assertions_enabled() -> bool {
    initialize_environment_failpoint();
    ENABLE_ASSERT.load(Ordering::Relaxed) || ENABLE_INTERNAL_CHECK.load(Ordering::Relaxed)
}

fn assertion_failed_message(extra: &str, user_message: Option<String>) -> String {
    let mut message = String::from("assert failed");
    if let Some(user_message) = user_message {
        message.push_str(", ");
        message.push_str(&user_message);
    }
    if !extra.is_empty() {
        message.push_str(", ");
        message.push_str(extra);
    }
    message
}

#[cold]
#[track_caller]
fn assertion_panic(extra: &str, user_message: Option<String>) -> ! {
    panic!("{}", assertion_failed_message(extra, user_message));
}

fn do_assert(condition: bool, message: Option<String>) {
    if !condition {
        assertion_panic("", message);
    }
}

/// Asserts that `condition` is true when internal assertions are enabled.
#[track_caller]
pub fn assert(condition: bool) {
    if assertions_enabled() {
        do_assert(condition, None);
    }
}

/// Asserts with a caller-formatted diagnostic message.
#[track_caller]
pub fn assert_with_message(condition: bool, message: impl Into<String>) {
    if assertions_enabled() {
        do_assert(condition, Some(message.into()));
    }
}

/// Asserts that no error is present.
#[track_caller]
pub fn assert_no_error(error: Option<&dyn fmt::Display>) {
    if assertions_enabled() {
        do_assert_no_error(error, None);
    }
}

/// Asserts that no error is present, with a caller-formatted message.
#[track_caller]
pub fn assert_no_error_with_message(error: Option<&dyn fmt::Display>, message: impl Into<String>) {
    if assertions_enabled() {
        do_assert_no_error(error, Some(message.into()));
    }
}

fn do_assert_no_error(error: Option<&dyn fmt::Display>, message: Option<String>) {
    if let Some(error) = error {
        assertion_panic(&format!("error is not nil: {error}"), message);
    }
}

/// Asserts that an optional Rust value represents a non-nil Go value.
#[track_caller]
pub fn assert_not_nil<T>(value: Option<T>) {
    if assertions_enabled() {
        do_assert(value.is_some(), None);
    }
}

/// Asserts that an optional value is present, with a caller-formatted message.
#[track_caller]
pub fn assert_not_nil_with_message<T>(value: Option<T>, message: impl Into<String>) {
    if assertions_enabled() {
        do_assert(value.is_some(), Some(message.into()));
    }
}

/// Asserts that a function exists and returns true.
#[track_caller]
pub fn assert_func<F>(function: Option<F>)
where
    F: FnOnce() -> bool,
{
    if assertions_enabled() {
        do_assert_func(function, None);
    }
}

/// Asserts a function result with a caller-formatted message.
#[track_caller]
pub fn assert_func_with_message<F>(function: Option<F>, message: impl Into<String>)
where
    F: FnOnce() -> bool,
{
    if assertions_enabled() {
        do_assert_func(function, Some(message.into()));
    }
}

fn do_assert_func<F>(function: Option<F>, message: Option<String>)
where
    F: FnOnce() -> bool,
{
    let Some(function) = function else {
        assertion_panic("", message);
    };
    do_assert(function(), message);
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]

    use super::*;
    use std::any::Any;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    #[derive(Debug)]
    struct Foo;

    fn panic_text(payload: &(dyn Any + Send)) -> Option<&str> {
        payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
    }

    fn check(operation: impl FnOnce(), expected: Option<&str>) {
        let result = catch_unwind(AssertUnwindSafe(operation));
        match expected {
            None => assert!(result.is_ok()),
            Some(expected) => {
                let payload = result.expect_err("assertion must panic");
                assert_eq!(panic_text(payload.as_ref()), Some(expected));
            }
        }
    }

    #[test]
    fn TestAssert() {
        const { assert!(IN_TEST) };

        check(|| super::assert(true), None);
        check(|| super::assert(false), Some("assert failed"));
        check(
            || assert_with_message(false, "msg1"),
            Some("assert failed, msg1"),
        );
        check(
            || assert_with_message(false, format!("msg2 {} {} {}", "a", "b", 1)),
            Some("assert failed, msg2 a b 1"),
        );
        check(
            || assert_with_message(false, 123.to_string()),
            Some("assert failed, 123"),
        );

        check(|| assert_not_nil(Some("")), None);
        check(|| assert_not_nil(Some("abc")), None);
        check(|| assert_not_nil(Some(0)), None);
        check(|| assert_not_nil(Some(123)), None);
        check(|| assert_not_nil(Some(Foo)), None);
        check(|| assert_not_nil(Some(Box::new(Foo))), None);
        check(|| assert_not_nil(None::<Foo>), Some("assert failed"));
        check(|| assert_not_nil(Some(true)), None);
        check(|| assert_not_nil(Some(false)), None);
        check(|| assert_not_nil(None::<Box<Foo>>), Some("assert failed"));
        check(|| assert_not_nil(Some(|| true)), None);
        check(|| assert_not_nil(Some(|| false)), None);
        check(|| assert_not_nil(Some(|_: &str| true)), None);
        check(
            || assert_not_nil_with_message(None::<Foo>, "msg1"),
            Some("assert failed, msg1"),
        );
        check(
            || {
                assert_not_nil_with_message(None::<Foo>, format!("msg2 {} {} {}", "a", "b", 1));
            },
            Some("assert failed, msg2 a b 1"),
        );

        check(
            || assert_func(Some(|| panic!("inner panic1"))),
            Some("inner panic1"),
        );
        check(|| assert_func(Some(|| true)), None);
        check(|| assert_func(Some(|| false)), Some("assert failed"));
        check(
            || assert_func_with_message(Some(|| false), "msg3"),
            Some("assert failed, msg3"),
        );
        check(
            || {
                assert_func_with_message(Some(|| false), format!("msg4 {} {} {}", "c", "d", 2));
            },
            Some("assert failed, msg4 c d 2"),
        );
        check(
            || assert_func(Some(|| panic!("inner panic2"))),
            Some("inner panic2"),
        );
        check(|| assert_func(None::<fn() -> bool>), Some("assert failed"));

        check(|| assert_no_error(None), None);
        let error = std::io::Error::other("mock err1");
        check(
            || assert_no_error(Some(&error)),
            Some("assert failed, error is not nil: mock err1"),
        );
        let no_error: Option<&dyn fmt::Display> = None;
        check(|| assert_no_error(no_error), None);

        // Exercise the `!intest && !enableassert` source file without adding a
        // second implementation authority: disabled assertions must not even
        // evaluate a deferred function. The independent internal-check switch
        // must still reactivate the same assertion body.
        ENABLE_ASSERT.store(false, Ordering::Relaxed);
        ENABLE_INTERNAL_CHECK.store(false, Ordering::Relaxed);
        let called = AtomicBool::new(false);
        assert_func(Some(|| {
            called.store(true, Ordering::Relaxed);
            false
        }));
        assert!(!called.load(Ordering::Relaxed));
        ENABLE_INTERNAL_CHECK.store(true, Ordering::Relaxed);
        check(|| super::assert(false), Some("assert failed"));
        ENABLE_ASSERT.store(true, Ordering::Relaxed);
    }
}
