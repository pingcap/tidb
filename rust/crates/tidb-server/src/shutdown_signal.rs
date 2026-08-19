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

use signal_hook::consts::{SIGHUP, SIGINT, SIGQUIT, SIGTERM};

/// Which signal started the shutdown; 0 until one fires.
pub type LastSignal = Arc<AtomicI32>;

/// Registers the four Go-handled signals; the first one recorded starts
/// `on_signal` (the node's shutdown) and later ones are ignored, as Go's
/// one-shot handler behaves.
pub fn install(
    on_signal: impl Fn() + Send + 'static,
) -> std::io::Result<LastSignal> {
    let last = Arc::new(AtomicI32::new(0));
    let mut signals =
        signal_hook::iterator::Signals::new([SIGINT, SIGTERM, SIGHUP, SIGQUIT])?;
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
}
