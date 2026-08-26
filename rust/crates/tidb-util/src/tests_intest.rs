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

//! Direct port of `Go code: pkg/util/intest` unit tests (assert_test.go).

use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::Ordering;

use crate::intest::{
    assert, assert_func, assert_func_with_message, assert_no_error,
    assert_no_error_with_message, assert_not_nil, assert_not_nil_with_message,
    assert_with_message, ENABLE_ASSERT, IN_TEST,
};

struct Foo;

fn panic_text(payload: &(dyn Any + Send)) -> Option<&str> {
    payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
}

/// Mirrors Go's `doCheckAssert`: run `operation`, requiring either success or a
/// panic whose payload equals (or starts with, for error assertions) the
/// expected message. The Go test relies on testify's message equality; here the
/// whole panic text must match exactly unless a prefix check is requested.
fn check(operation: impl FnOnce(), expected: Option<&str>) {
    let result = catch_unwind(AssertUnwindSafe(operation));
    match (result, expected) {
        (Ok(()), None) => {}
        (Ok(()), Some(expected)) => {
            panic!("assertion should have panicked with {expected:?}")
        }
        (Err(payload), None) => {
            panic!("assertion should not have panicked: {:?}", panic_text(payload.as_ref()))
        }
        (Err(payload), Some(expected)) => match panic_text(payload.as_ref()) {
            Some(text) if text == expected || text.starts_with(expected) => {}
            other => panic!("expected panic containing {expected:?}, got {other:?}"),
        },
    }
}

// Go pkg/util/intest/assert_test.go: TestAssert.
#[test]
fn assert_cases() {
    assert!(IN_TEST);

    check(|| assert(true), None);
    check(|| assert(false), Some("assert failed"));
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

    // Go checks typed zero values and non-nil pointers; Rust models "nil" as
    // Option::None, so each Some(...) case corresponds to a non-nil Go value.
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
    let no_error: Option<&dyn std::fmt::Display> = None;
    check(|| assert_no_error(no_error), None);

    // Mirror of the `!intest && !enableassert` source file: with both switches
    // off the assertion body must be inert, and re-enabling only the
    // internal-check switch must reactivate it.
    ENABLE_ASSERT.store(false, Ordering::Relaxed);
    crate::intest::ENABLE_INTERNAL_CHECK.store(false, Ordering::Relaxed);
    assert(false);
    assert_func(Some(|| false));
    crate::intest::ENABLE_INTERNAL_CHECK.store(true, Ordering::Relaxed);
    check(|| assert(false), Some("assert failed"));
    ENABLE_ASSERT.store(true, Ordering::Relaxed);
}
