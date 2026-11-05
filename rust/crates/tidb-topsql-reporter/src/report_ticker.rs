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

//! Go `pkg/util/topsql/reporter/report_ticker.go`.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use tidb_topsql_state::DEF_TIDB_TOP_SQL_REPORT_INTERVAL_SECONDS;

/// Go `defaultReportTickerInterval`.
const DEFAULT_REPORT_TICKER_INTERVAL: Duration =
    Duration::from_secs(DEF_TIDB_TOP_SQL_REPORT_INTERVAL_SECONDS as u64);

/// Go `reportTickerInterval`, guarded as Go's `reportTickerIntervalMu` does.
static REPORT_TICKER_INTERVAL: Mutex<Duration> = Mutex::new(DEFAULT_REPORT_TICKER_INTERVAL);

/// Go `newReportTicker`.
pub(crate) fn new_report_ticker() -> crossbeam_channel::Receiver<Instant> {
    let interval = *REPORT_TICKER_INTERVAL.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    crossbeam_channel::tick(interval)
}

/// Go `SetReportTickerIntervalSecondsForTest` overrides report ticker
/// interval in tests. A non-positive value resets to the default interval.
/// The returned function restores the previous override.
pub fn set_report_ticker_interval_seconds_for_test(seconds: i64) -> impl FnOnce() {
    let interval = if seconds > 0 {
        Duration::from_secs(seconds as u64)
    } else {
        DEFAULT_REPORT_TICKER_INTERVAL
    };
    let prev = std::mem::replace(
        &mut *REPORT_TICKER_INTERVAL.lock().unwrap_or_else(std::sync::PoisonError::into_inner),
        interval,
    );
    move || {
        *REPORT_TICKER_INTERVAL.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = prev;
    }
}
