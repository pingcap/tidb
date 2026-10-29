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

//! The process start Go's runtime records (`runtime.startNano`), from which
//! `time.Now()` measures its monotonic reading -- the `m=+0.001` suffix of
//! `time.Time.String()`.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

static PROCESS_START: OnceLock<Instant> = OnceLock::new();

/// Records the process start. `main` calls it first; a later call keeps the
/// first instant.
pub fn mark_process_start() {
    PROCESS_START.get_or_init(Instant::now);
}

/// The monotonic time since the process started (since the first use, when
/// `main` did not mark it).
pub fn since_process_start() -> Duration {
    PROCESS_START.get_or_init(Instant::now).elapsed()
}

/// Go `time.Time.String()`'s monotonic suffix for a time taken now:
/// ` m=+<seconds>.<nanoseconds>`.
pub fn monotonic_suffix() -> String {
    let elapsed = since_process_start();
    format!(" m=+{}.{:09}", elapsed.as_secs(), elapsed.subsec_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_suffix_is_gos_signed_seconds_with_nine_fraction_digits() {
        mark_process_start();
        let suffix = monotonic_suffix();
        let digits = suffix.strip_prefix(" m=+").expect("Go's m=+ prefix");
        let (seconds, nanos) = digits.split_once('.').expect("a fraction");
        assert!(seconds.parse::<u64>().is_ok(), "{suffix}");
        assert_eq!(9, nanos.len(), "{suffix}");
    }
}
