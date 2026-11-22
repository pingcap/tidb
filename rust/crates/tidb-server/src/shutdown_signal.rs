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

//! Signal-discriminating graceful shutdown: Go
//! `signal.SetupSignalHandler` + `exitCodeForSignal`
//! (`cmd/tidb-server/main.go:498-529`).
//!
//! Go's handler closes the server on the FIRST of SIGINT/SIGTERM/SIGHUP/
//! SIGQUIT and remembers which one fired, because the process exit code
//! depends on it: SIGINT is the standby force-shutdown path and exits
//! `128+SIGINT` "so deployment scripts can identify this force-shutdown
//! path"; every other signal exits 0. The former `ctrlc` registration
//! could not say which signal fired, which left `exit_code_for_signal` an
//! unwired mapping — this module is the wiring.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use signal_hook::consts::{SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1};

/// Which signal started the shutdown; 0 until one fires.
pub type LastSignal = Arc<AtomicI32>;

/// Registers the four Go-handled signals; the first one recorded starts
/// `on_signal` (the node's shutdown) and later ones are ignored, as Go's
/// one-shot handler behaves.
pub fn install(on_signal: impl Fn() + Send + 'static) -> std::io::Result<LastSignal> {
    let last = Arc::new(AtomicI32::new(0));
    let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM, SIGHUP, SIGQUIT])?;
    let recorded = Arc::clone(&last);
    std::thread::Builder::new()
        .name("tidb-shutdown-signal".to_owned())
        .spawn(move || {
            if let Some(signal) = signals.forever().next() {
                recorded.store(signal, Ordering::SeqCst);
                on_signal();
            }
        })?;
    Ok(last)
}

/// Go `getGoroutineStacks` (`pkg/util/signal/signal_posix.go`): the stacks
/// of the process, truncated at Go's 64 MiB limit.
fn get_goroutine_stacks() -> String {
    let mut buf = Vec::with_capacity(1 << 20);
    // Writing into a Vec cannot fail; a read failure leaves what was dumped.
    let _ = tidb_memoryusagealarm::write_native_thread_stacks(&mut buf);
    buf.truncate(64 << 20);
    String::from_utf8_lossy(&buf).into_owned()
}

/// Go `SetupUSR1Handler`'s dump line (`log.Printf`, whose standard logger
/// prefixes the local date and time). Go's `syscall.SIGUSR1.String()` is
/// "user defined signal 1".
fn usr1_dump_message(now: chrono::DateTime<chrono::Local>, stacks: &str) -> String {
    format!(
        "{} \n=== Got signal [user defined signal 1] to dump goroutine stack. ===\n{stacks}\n=== Finished dumping goroutine stack. ===\n",
        now.format("%Y/%m/%d %H:%M:%S"),
    )
}

/// Go `signal.SetupUSR1Handler` (`pkg/util/signal/signal_posix.go`):
/// every SIGUSR1 dumps the process's stacks to the standard error instead
/// of terminating the server (the signal's default action).
pub fn setup_usr1_handler() -> std::io::Result<()> {
    let mut signals = signal_hook::iterator::Signals::new([SIGUSR1])?;
    std::thread::Builder::new()
        .name("tidb-usr1-signal".to_owned())
        .spawn(move || {
            for _ in signals.forever() {
                eprint!("{}", usr1_dump_message(chrono::Local::now(), &get_goroutine_stacks()));
            }
        })?;
    Ok(())
}

/// Go `exitCodeForSignal` (`main.go:522`) over the recorded signal.
#[must_use]
pub fn exit_code_for_recorded(last: &LastSignal) -> u8 {
    if last.load(Ordering::SeqCst) == SIGINT {
        // `exitCodeInt = 128 + int(syscall.SIGINT)`.
        130
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_code_for_signal_matches_gos_table() {
        // `TestExitCodeForSignal` (`cmd/tidb-server/main_test.go:56`),
        // transcreated over the recorded-signal form: SIGINT is the
        // force-shutdown path (128+2); SIGTERM, SIGHUP, SIGQUIT and
        // nothing-recorded all exit 0.
        for (signal, want) in [
            (SIGINT, 130_u8),
            (SIGTERM, 0),
            (SIGHUP, 0),
            (SIGQUIT, 0),
            (0, 0), // Go's nil signal
        ] {
            let last: LastSignal = Arc::new(AtomicI32::new(signal));
            assert_eq!(exit_code_for_recorded(&last), want, "{signal}");
        }
    }

    #[test]
    fn usr1_dumps_stacks_instead_of_terminating() {
        // Go `SetupUSR1Handler`: SIGUSR1's default action terminates the
        // process; with the handler set it only dumps the stacks.
        setup_usr1_handler().expect("SetupUSR1Handler");
        signal_hook::low_level::raise(SIGUSR1).expect("raise SIGUSR1");
        std::thread::sleep(std::time::Duration::from_millis(200));
        let message = usr1_dump_message(
            chrono::Local::now(),
            "thread main:",
        );
        assert!(message.starts_with(&chrono::Local::now().format("%Y/%m/%d ").to_string()), "{message}");
        assert!(message.contains(" \n=== Got signal [user defined signal 1] to dump goroutine stack. ===\nthread main:\n=== Finished dumping goroutine stack. ===\n"), "{message}");
        assert!(!get_goroutine_stacks().is_empty());
    }
}
