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

//! Go master `pkg/mvservice/time_proxy_test.go`. Go's
//! `TestTimeProxyInstallPanicWhenModuleNil` has no Rust form: the installer
//! takes an `Arc<MockTimeModule>`, which cannot be nil.

use std::time::Duration;

use crossbeam_channel::Receiver;
use tidb_timer::go_time::{GoTime, SECOND};

use crate::test_helpers_test::{eventually, serial, TEST_EVENTUALLY_TICK, TEST_EVENTUALLY_WAIT};
use crate::time_proxy::{
    install_mock_time_module_for_test, mvs_after, mvs_now, mvs_since, mvs_sleep, mvs_unix, mvs_unix_milli, mvs_until,
    uninstall_mock_time_module_for_test, MockTimeModule, MvsTimer,
};

/// Go `time.Date(2026, 2, 10, h, m, s, 0, time.UTC)`.
fn date(epoch_seconds: i64) -> GoTime {
    GoTime::from_unix(epoch_seconds, 0)
}

/// Go `waitValueNoTime` / `waitSignalNoTime`.
fn wait_value_no_time<T>(ch: &Receiver<T>) -> T {
    ch.recv_timeout(TEST_EVENTUALLY_WAIT).expect("value not received before timeout")
}

fn not_ready<T>(ch: &Receiver<T>, message: &str) {
    assert!(ch.try_recv().is_err(), "{message}");
}

#[test]
fn test_time_proxy_mock_module_clock_ops() {
    let _serial = serial();
    let start = date(1_770_714_487); // 2026-02-10 09:08:07 UTC
    let module = MockTimeModule::new(start.clone());
    let _restore = install_mock_time_module_for_test(module.clone());

    assert_eq!(start, mvs_now());
    assert_eq!(0, mvs_since(&start));
    assert_eq!(3 * SECOND, mvs_until(&start.add(3 * SECOND)));

    module.advance(5 * SECOND);
    assert_eq!(start.add(5 * SECOND), mvs_now());
    assert_eq!(5 * SECOND, mvs_since(&start));
    assert_eq!(2 * SECOND, mvs_until(&start.add(7 * SECOND)));

    assert_eq!(GoTime::from_unix(123, 456), mvs_unix(123, 456));
    assert_eq!(GoTime::from_unix_milli(789), mvs_unix_milli(789));
}

#[test]
fn test_time_proxy_mock_module_after_and_sleep() {
    let _serial = serial();
    let start = date(1_770_717_600); // 2026-02-10 10:00:00 UTC
    let module = MockTimeModule::new(start.clone());
    let _restore = install_mock_time_module_for_test(module.clone());

    let ch = mvs_after(2 * SECOND);
    not_ready(&ch, "after should not fire before due");
    module.advance(SECOND);
    not_ready(&ch, "after should not fire before due");
    module.advance(SECOND);
    wait_value_no_time(&ch);
    assert_eq!(start.add(2 * SECOND), mvs_now());

    module.set(start.clone());
    mvs_sleep(3 * SECOND);
    assert_eq!(start.add(3 * SECOND), mvs_now());

    module.set(start.clone());
    module.set_auto_advance_on_sleep(false);
    let (done_tx, done) = crossbeam_channel::bounded::<()>(1);
    let sleeper = std::thread::spawn(move || {
        mvs_sleep(4 * SECOND);
        let _ = done_tx.send(());
    });
    // "Sleep must register its waiter before virtual time advances."
    eventually(|| module.after_waiters_len() == 1, TEST_EVENTUALLY_WAIT, TEST_EVENTUALLY_TICK);

    module.advance(3 * SECOND);
    not_ready(&done, "sleep should still be blocked before due");
    module.advance(SECOND);
    wait_value_no_time(&done);
    sleeper.join().unwrap();
}

#[test]
fn test_time_proxy_mock_module_timer() {
    let _serial = serial();
    let start = date(1_770_721_200); // 2026-02-10 11:00:00 UTC
    let module = MockTimeModule::new(start);
    let _restore = install_mock_time_module_for_test(module.clone());

    let timer = MvsTimer::new(2 * SECOND);
    not_ready(&timer.c(), "timer should not fire before due");
    module.advance(SECOND);
    not_ready(&timer.c(), "timer should not fire before due");
    module.advance(SECOND);
    wait_value_no_time(&timer.c());

    assert!(!timer.reset(3 * SECOND));
    module.advance(2 * SECOND);
    not_ready(&timer.c(), "timer should not fire before reset due");
    module.advance(SECOND);
    wait_value_no_time(&timer.c());

    assert!(!timer.stop());
    assert!(!timer.reset(5 * SECOND));
    assert!(timer.reset(5 * SECOND));
    assert!(timer.stop());
    module.advance(10 * SECOND);
    not_ready(&timer.c(), "timer should not fire after stop");
}

#[test]
fn test_time_proxy_fallback_when_module_not_installed() {
    let _serial = serial();
    let _restore = uninstall_mock_time_module_for_test();

    let now = GoTime::now();
    let got = mvs_now();
    assert!(got.sub(&now).abs() < 100 * 1_000_000, "mvs_now falls back to the real clock");

    let ch = mvs_after(10 * 1_000_000);
    assert!(ch.recv_timeout(Duration::from_secs(1)).is_ok(), "expected mvsAfter fallback to real timer");

    let timer = MvsTimer::new(10 * 1_000_000);
    assert!(timer.c().recv_timeout(Duration::from_secs(1)).is_ok(), "expected mvsNewTimer fallback to real timer");
    timer.stop();
}
