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

//! Public package contract for Go `pkg/util/intest`.

use std::any::Any;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::process::Command;
use std::sync::atomic::Ordering;

use tidb_util::intest;

const CHILD_MODE: &str = "TIDB_INTEST_CONTRACT_CHILD";
#[cfg(not(any(feature = "intest", feature = "enableassert")))]
const INTERNAL_CHECK_FAILPOINT: &str = "/enableInternalCheck=return(true)";

fn panic_text(payload: &(dyn Any + Send)) -> Option<&str> {
    payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
}

fn caught_panic(action: impl FnOnce()) -> Option<String> {
    catch_unwind(AssertUnwindSafe(action)).err().map(|payload| {
        panic_text(payload.as_ref())
            .expect("assertion panic must carry text")
            .to_owned()
    })
}

fn expect_panic(action: impl FnOnce(), expected: &str) {
    assert_eq!(caught_panic(action).as_deref(), Some(expected));
}

fn run_child(mode: &str, failpoint: Option<&str>) {
    let mut command = Command::new(std::env::current_exe().expect("locate contract test binary"));
    command
        .arg("contract_child")
        .arg("--exact")
        .arg("--nocapture")
        .env(CHILD_MODE, mode);
    if let Some(failpoint) = failpoint {
        command.env("GO_FAILPOINTS", failpoint);
    } else {
        command.env_remove("GO_FAILPOINTS");
    }

    let output = command.output().expect("launch contract child");
    assert!(
        output.status.success(),
        "contract child failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[cfg(not(any(feature = "intest", feature = "enableassert")))]
fn check_startup_failpoint() {
    const { assert!(!intest::IN_TEST) };
    assert!(intest::ENABLE_ASSERT.load(Ordering::Relaxed));
    assert!(intest::ENABLE_INTERNAL_CHECK.load(Ordering::Relaxed));
    expect_panic(|| intest::assert(false), "assert failed");
}

#[cfg(any(feature = "intest", feature = "enableassert"))]
fn check_startup_failpoint() {
    panic!("startup failpoint child only applies to the default build");
}

#[test]
fn contract_child() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };

    match mode.as_str() {
        "build-shape" => {
            let enabled = cfg!(any(feature = "intest", feature = "enableassert"));
            assert_eq!(intest::IN_TEST, cfg!(feature = "intest"));
            assert_eq!(intest::ENABLE_ASSERT.load(Ordering::Relaxed), enabled);
            assert_eq!(
                intest::ENABLE_INTERNAL_CHECK.load(Ordering::Relaxed),
                enabled
            );

            let called = std::sync::atomic::AtomicBool::new(false);
            let panic = caught_panic(|| {
                intest::assert_func(Some(|| {
                    called.store(true, Ordering::Relaxed);
                    false
                }));
            });
            assert_eq!(called.load(Ordering::Relaxed), enabled);
            assert_eq!(panic.is_some(), enabled);
        }
        "startup-failpoint" => check_startup_failpoint(),
        other => panic!("unknown child mode: {other}"),
    }
}

#[test]
fn build_shape_matches_go_tags() {
    run_child("build-shape", None);
}

#[cfg(not(any(feature = "intest", feature = "enableassert")))]
#[test]
fn startup_failpoint_initializes_exported_switches() {
    run_child("startup-failpoint", Some(INTERNAL_CHECK_FAILPOINT));
}

#[test]
fn public_assertion_surface_matches_go() {
    intest::ENABLE_ASSERT.store(true, Ordering::Relaxed);
    intest::ENABLE_INTERNAL_CHECK.store(true, Ordering::Relaxed);

    assert_eq!(intest::IN_TEST, cfg!(feature = "intest"));
    assert_eq!(caught_panic(|| intest::assert(true)), None);
    expect_panic(|| intest::assert(false), "assert failed");
    expect_panic(
        || intest::assert_with_message(false, "msg1"),
        "assert failed, msg1",
    );
    expect_panic(
        || intest::assert_with_message(false, format!("msg2 {} {} {}", "a", "b", 1)),
        "assert failed, msg2 a b 1",
    );
    expect_panic(
        || intest::assert_with_message(false, 123.to_string()),
        "assert failed, 123",
    );

    assert_eq!(caught_panic(|| intest::assert_not_nil(Some(0))), None);
    assert_eq!(caught_panic(|| intest::assert_not_nil(Some(false))), None);
    expect_panic(|| intest::assert_not_nil(None::<Box<()>>), "assert failed");
    expect_panic(
        || intest::assert_not_nil_with_message(None::<()>, "msg1"),
        "assert failed, msg1",
    );

    assert_eq!(caught_panic(|| intest::assert_func(Some(|| true))), None);
    expect_panic(|| intest::assert_func(Some(|| false)), "assert failed");
    expect_panic(
        || intest::assert_func_with_message(Some(|| false), "msg3"),
        "assert failed, msg3",
    );
    expect_panic(
        || intest::assert_func(Some(|| panic!("inner panic"))),
        "inner panic",
    );
    expect_panic(
        || intest::assert_func(None::<fn() -> bool>),
        "assert failed",
    );

    let no_error: Option<&dyn fmt::Display> = None;
    assert_eq!(caught_panic(|| intest::assert_no_error(no_error)), None);
    let error = std::io::Error::other("mock err1");
    expect_panic(
        || intest::assert_no_error(Some(&error)),
        "assert failed, error is not nil: mock err1",
    );
    expect_panic(
        || intest::assert_no_error_with_message(Some(&error), "msg4"),
        "assert failed, msg4, error is not nil: mock err1",
    );

    intest::ENABLE_ASSERT.store(false, Ordering::Relaxed);
    intest::ENABLE_INTERNAL_CHECK.store(false, Ordering::Relaxed);
    let called = std::sync::atomic::AtomicBool::new(false);
    intest::assert_func(Some(|| {
        called.store(true, Ordering::Relaxed);
        false
    }));
    assert!(!called.load(Ordering::Relaxed));
    assert_eq!(caught_panic(|| intest::assert(false)), None);

    intest::ENABLE_INTERNAL_CHECK.store(true, Ordering::Relaxed);
    expect_panic(|| intest::assert(false), "assert failed");

    let source_default = cfg!(any(feature = "intest", feature = "enableassert"));
    intest::ENABLE_ASSERT.store(source_default, Ordering::Relaxed);
    intest::ENABLE_INTERNAL_CHECK.store(source_default, Ordering::Relaxed);
}
