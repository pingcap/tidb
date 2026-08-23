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

//! Inner-transaction start-timestamp tracking translated from `pkg/kv/txn.go`.
//!
//! This owner keeps the synchronized process-global set, minimum selection,
//! timestamp conversion, and long-running diagnostic side effect together.

use std::collections::BTreeSet;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Duration after which an internal transaction is considered long-running.
pub const TIME_TO_PRINT_LONG_INTERNAL_TXN: Duration = Duration::from_secs(5 * 60);

/// Process-global inner-transaction timestamp registry.
pub static GLOBAL_INNER_TXN_START_TS: LazyLock<InnerTxnStartTsBox> =
    LazyLock::new(InnerTxnStartTsBox::new);

/// Synchronized set of inner-transaction start timestamps.
///
/// `pkg/kv/txn.go` stores timestamps in a mutex-protected map. A set is the
/// source-shaped representation because the map values are all empty structs;
/// ordering the set makes minimum selection explicit without changing the
/// returned value.
#[derive(Debug, Default)]
pub struct InnerTxnStartTsBox {
    timestamps: Mutex<BTreeSet<u64>>,
}

impl InnerTxnStartTsBox {
    /// Creates an empty timestamp box.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an inner transaction start timestamp.
    pub fn store_inner_txn_ts(&self, start_ts: u64) {
        self.timestamps
            .lock()
            .expect("inner transaction timestamp mutex poisoned")
            .insert(start_ts);
    }

    /// Removes an inner transaction start timestamp.
    pub fn delete_inner_txn_ts(&self, start_ts: u64) {
        self.timestamps
            .lock()
            .expect("inner transaction timestamp mutex poisoned")
            .remove(&start_ts);
    }

    /// Reports whether a timestamp is currently tracked.
    #[must_use]
    pub fn contains(&self, start_ts: u64) -> bool {
        self.timestamps
            .lock()
            .expect("inner transaction timestamp mutex poisoned")
            .contains(&start_ts)
    }

    /// Returns the smallest tracked timestamp strictly above `lower_limit`
    /// and strictly below `current_min`.
    ///
    /// Every tracked transaction is also checked for the source long-running
    /// diagnostic, including timestamps outside the returned range.
    #[must_use]
    pub fn get_min_start_ts(&self, now: SystemTime, lower_limit: u64, current_min: u64) -> u64 {
        let timestamps = self
            .timestamps
            .lock()
            .expect("inner transaction timestamp mutex poisoned");
        let mut minimum = current_min;
        for start_ts in timestamps.iter().copied() {
            let _ = print_long_time_internal_txn(now, start_ts, true);
            if start_ts > lower_limit && start_ts < minimum {
                minimum = start_ts;
            }
        }
        minimum
    }
}

/// One long-running inner transaction observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LongRunningInnerTxn {
    /// Transaction start timestamp.
    pub start_ts: u64,
    /// Elapsed wall-clock time.
    pub elapsed: Duration,
    /// Whether `RunInNewTxn` owns the transaction.
    pub run_by_function: bool,
}

/// Converts a TSO to its physical wall-clock time.
fn tso_time(start_ts: u64) -> SystemTime {
    // TiDB TSO reserves 18 logical bits below the physical milliseconds.
    UNIX_EPOCH + Duration::from_millis(start_ts >> 18)
}

/// Returns a long-running observation when the source five-minute threshold is
/// exceeded.
#[must_use]
pub fn long_running_inner_txn(
    now: SystemTime,
    start_ts: u64,
    run_by_function: bool,
) -> Option<LongRunningInnerTxn> {
    if start_ts == 0 {
        return None;
    }
    let elapsed = now.duration_since(tso_time(start_ts)).ok()?;
    (elapsed > TIME_TO_PRINT_LONG_INTERNAL_TXN).then_some(LongRunningInnerTxn {
        start_ts,
        elapsed,
        run_by_function,
    })
}

/// Emits the source long-running diagnostic and returns its structured value.
#[must_use]
pub fn print_long_time_internal_txn(
    now: SystemTime,
    start_ts: u64,
    run_by_function: bool,
) -> Option<LongRunningInnerTxn> {
    let observation = long_running_inner_txn(now, start_ts, run_by_function)?;
    let owner = if run_by_function {
        "RunInNewTxn"
    } else {
        "internal session"
    };
    eprintln!(
        "An internal transaction running by {owner} lasts long time: time={:?} startTS={} start_time={:?}",
        observation.elapsed,
        observation.start_ts,
        tso_time(observation.start_ts)
    );
    Some(observation)
}

/// Returns long-running observations for every currently tracked transaction.
#[must_use]
pub fn long_running_inner_txns(
    timestamps: &InnerTxnStartTsBox,
    now: SystemTime,
) -> Vec<LongRunningInnerTxn> {
    timestamps
        .timestamps
        .lock()
        .expect("inner transaction timestamp mutex poisoned")
        .iter()
        .filter_map(|start_ts| long_running_inner_txn(now, *start_ts, true))
        .collect()
}

/// Source-shaped `GetMinInnerTxnStartTS` with the registry made explicit.
#[must_use]
pub fn get_min_inner_txn_start_ts(
    timestamps: &InnerTxnStartTsBox,
    now: SystemTime,
    lower_limit: u64,
    current_min: u64,
) -> u64 {
    timestamps.get_min_start_ts(now, lower_limit, current_min)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    /// Go `oracle.GoTimeToTS`: physical milliseconds in the high bits.
    fn go_time_to_ts(unix_millis: u64) -> u64 {
        unix_millis << 18
    }

    /// Go `oracle.GoTimeToLowerLimitStartTS`.
    fn go_time_to_lower_limit_start_ts(now_unix_millis: u64, max_txn_time_use_ms: u64) -> u64 {
        go_time_to_ts(now_unix_millis - max_txn_time_use_ms)
    }

    /// Go `TestInnerTxnStartTsBox` (`txn_test.go:72`): store and delete track
    /// membership; the minimum scan returns the smallest tracked timestamp
    /// strictly above the lower limit.
    #[test]
    fn go_test_inner_txn_start_ts_box() {
        // case1: store and delete
        let box_ = InnerTxnStartTsBox::new();
        box_.store_inner_txn_ts(5);
        assert!(box_.contains(5));

        box_.delete_inner_txn_ts(5);
        assert!(!box_.contains(5));

        // case2: GetMinInnerTxnStartTS. The source times are 2022-03-08/10
        // UTC; expressed here as their Unix milliseconds.
        let ts0 = go_time_to_ts(1_646_740_201_000); // 2022-03-08 12:10:01 UTC
        let ts1 = go_time_to_ts(1_646_920_201_000); // 2022-03-10 12:10:01 UTC
        let ts2 = go_time_to_ts(1_646_922_243_000); // 2022-03-10 12:14:03 UTC
        let ts3 = go_time_to_ts(1_646_922_245_000); // 2022-03-10 12:14:05 UTC
        let now_millis = 1_646_922_900_000u64; // 2022-03-10 12:15:00 UTC
        let low_limit = go_time_to_lower_limit_start_ts(now_millis, 24 * 60 * 60 * 1000);
        let min_start_ts = go_time_to_ts(now_millis);

        box_.store_inner_txn_ts(ts0);
        box_.store_inner_txn_ts(ts1);
        box_.store_inner_txn_ts(ts2);
        box_.store_inner_txn_ts(ts3);

        let now = UNIX_EPOCH + Duration::from_millis(now_millis);
        let new_min_start_ts = get_min_inner_txn_start_ts(&box_, now, low_limit, min_start_ts);
        assert_eq!(new_min_start_ts, ts1);

        box_.delete_inner_txn_ts(ts0);
        box_.delete_inner_txn_ts(ts1);
        box_.delete_inner_txn_ts(ts2);
        box_.delete_inner_txn_ts(ts3);
        assert!(!box_.contains(ts0));
        assert!(!box_.contains(ts1));
        assert!(!box_.contains(ts2));
        assert!(!box_.contains(ts3));
    }
}
