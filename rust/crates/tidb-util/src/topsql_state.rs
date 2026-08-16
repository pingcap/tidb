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

//! Complete transcreation of Go `pkg/util/topsql/state` (`state.go`): the
//! process-global on/off state for Top-SQL and Top-RU.
//!
//! Top-SQL is a plain flag. Top-RU is reference counted instead — it is on
//! while at least one subscriber holds it — and the item interval resets to
//! its default only when the last subscriber leaves.
//!
//! `tipb.ItemInterval` is a protobuf enum whose numeric values *are* the
//! interval in seconds (15, 30, 60), with 0 meaning unspecified; Go relies on
//! that by casting the enum straight to an `int64`. `tidb-proto` does not
//! generate this enum yet, so [`ItemInterval`] carries the same wire numbers
//! and the same cast.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

/// Go `DefTiDBTopSQLEnable`.
pub const DEF_TIDB_TOP_SQL_ENABLE: bool = false;
/// Go `DefTiDBTopSQLPrecisionSeconds`.
pub const DEF_TIDB_TOP_SQL_PRECISION_SECONDS: i64 = 1;
/// Go `DefTiDBTopSQLMaxTimeSeriesCount`.
pub const DEF_TIDB_TOP_SQL_MAX_TIME_SERIES_COUNT: i64 = 100;
/// Go `DefTiDBTopSQLMaxMetaCount`.
pub const DEF_TIDB_TOP_SQL_MAX_META_COUNT: i64 = 5000;
/// Go `DefTiDBTopSQLReportIntervalSeconds`.
pub const DEF_TIDB_TOP_SQL_REPORT_INTERVAL_SECONDS: i64 = 60;
/// Go `DefTiDBTopRUItemIntervalSeconds`.
pub const DEF_TIDB_TOP_RU_ITEM_INTERVAL_SECONDS: i64 = 60;

/// Go `tipb.ItemInterval`, whose numeric values are the interval in seconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ItemInterval(pub i32);

impl ItemInterval {
    /// Go `ITEM_INTERVAL_UNSPECIFIED`, which normalizes to the default.
    pub const UNSPECIFIED: Self = Self(0);
    /// Go `ITEM_INTERVAL_15S`.
    pub const FIFTEEN_SECONDS: Self = Self(15);
    /// Go `ITEM_INTERVAL_30S`.
    pub const THIRTY_SECONDS: Self = Self(30);
    /// Go `ITEM_INTERVAL_60S`.
    pub const SIXTY_SECONDS: Self = Self(60);
}

/// Go `ErrInvalidTopRUItemInterval`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidTopRuItemInterval(pub i32);

impl std::fmt::Display for InvalidTopRuItemInterval {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid top ru item interval: {}", self.0)
    }
}

impl std::error::Error for InvalidTopRuItemInterval {}

/// Go `state.State`.
#[derive(Debug)]
pub struct State {
    enable: AtomicBool,
    /// Go `PrecisionSeconds`: the Top-SQL refresh interval.
    pub precision_seconds: AtomicI64,
    /// Go `MaxStatementCount`: statements kept in memory.
    pub max_statement_count: AtomicI64,
    /// Go `MaxCollect`: capacity of the collect map.
    pub max_collect: AtomicI64,
    ru_consumer_count: AtomicI64,
    /// Go `TopRUItemIntervalSeconds`.
    pub top_ru_item_interval_seconds: AtomicI64,
}

impl State {
    const fn new() -> Self {
        Self {
            enable: AtomicBool::new(DEF_TIDB_TOP_SQL_ENABLE),
            precision_seconds: AtomicI64::new(DEF_TIDB_TOP_SQL_PRECISION_SECONDS),
            max_statement_count: AtomicI64::new(DEF_TIDB_TOP_SQL_MAX_TIME_SERIES_COUNT),
            max_collect: AtomicI64::new(DEF_TIDB_TOP_SQL_MAX_META_COUNT),
            ru_consumer_count: AtomicI64::new(0),
            top_ru_item_interval_seconds: AtomicI64::new(DEF_TIDB_TOP_RU_ITEM_INTERVAL_SECONDS),
        }
    }

    /// The number of active Top-RU subscribers, for tests that reset it.
    pub fn set_ru_consumer_count(&self, count: i64) {
        self.ru_consumer_count.store(count, Ordering::SeqCst);
    }
}

/// Go `GlobalState`.
pub static GLOBAL_STATE: State = State::new();

/// Go `EnableTopSQL`.
pub fn enable_top_sql() {
    GLOBAL_STATE.enable.store(true, Ordering::SeqCst);
}

/// Go `DisableTopSQL`.
pub fn disable_top_sql() {
    GLOBAL_STATE.enable.store(false, Ordering::SeqCst);
}

/// Go `TopSQLEnabled`.
#[must_use]
pub fn top_sql_enabled() -> bool {
    GLOBAL_STATE.enable.load(Ordering::SeqCst)
}

/// Go `TopProfilingEnabled`: true when either Top-SQL or Top-RU is on.
#[must_use]
pub fn top_profiling_enabled() -> bool {
    top_sql_enabled() || top_ru_enabled()
}

/// Go `EnableTopRU`: adds one subscriber.
pub fn enable_top_ru() {
    GLOBAL_STATE
        .ru_consumer_count
        .fetch_add(1, Ordering::SeqCst);
}

/// Go `DisableTopRU`: removes one subscriber, resetting the item interval
/// when the last one leaves.
///
/// Go guards against underflow with a compare-and-swap loop rather than a
/// bare decrement, so a surplus disable is a no-op instead of driving the
/// count negative.
pub fn disable_top_ru() {
    loop {
        let previous = GLOBAL_STATE.ru_consumer_count.load(Ordering::SeqCst);
        if previous <= 0 {
            return;
        }
        if GLOBAL_STATE
            .ru_consumer_count
            .compare_exchange(previous, previous - 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            if previous == 1 {
                reset_top_ru_item_interval();
            }
            return;
        }
    }
}

/// Go `TopRUEnabled`: on while any subscriber remains.
#[must_use]
pub fn top_ru_enabled() -> bool {
    GLOBAL_STATE.ru_consumer_count.load(Ordering::SeqCst) > 0
}

/// Go's private `normalizeTopRUItemIntervalSeconds`.
fn normalize_top_ru_item_interval_seconds(
    interval: ItemInterval,
) -> Result<i64, InvalidTopRuItemInterval> {
    match interval {
        ItemInterval::UNSPECIFIED => Ok(DEF_TIDB_TOP_RU_ITEM_INTERVAL_SECONDS),
        ItemInterval::FIFTEEN_SECONDS
        | ItemInterval::THIRTY_SECONDS
        | ItemInterval::SIXTY_SECONDS => Ok(i64::from(interval.0)),
        other => Err(InvalidTopRuItemInterval(other.0)),
    }
}

/// Go `SetTopRUItemInterval`: 15, 30, and 60 are accepted, unspecified falls
/// back to the default, and anything else is rejected and changes nothing.
pub fn set_top_ru_item_interval(interval: ItemInterval) -> Result<(), InvalidTopRuItemInterval> {
    let current = get_top_ru_item_interval();
    let subscribers = GLOBAL_STATE.ru_consumer_count.load(Ordering::SeqCst);
    match normalize_top_ru_item_interval_seconds(interval) {
        Err(error) => {
            tracing::warn!(
                current_interval_seconds = current,
                active_subscribers = subscribers,
                %error,
                "[top-sql] top ru item interval invalid"
            );
            Err(error)
        }
        Ok(seconds) => {
            tracing::info!(
                current_interval_seconds = current,
                new_interval_seconds = seconds,
                active_subscribers = subscribers,
                "[top-sql] top ru item interval overridden by later subscription"
            );
            GLOBAL_STATE
                .top_ru_item_interval_seconds
                .store(seconds, Ordering::SeqCst);
            Ok(())
        }
    }
}

/// Go `GetTopRUItemInterval`.
#[must_use]
pub fn get_top_ru_item_interval() -> i64 {
    GLOBAL_STATE
        .top_ru_item_interval_seconds
        .load(Ordering::SeqCst)
}

/// Go `ResetTopRUItemInterval`.
pub fn reset_top_ru_item_interval() {
    GLOBAL_STATE
        .top_ru_item_interval_seconds
        .store(DEF_TIDB_TOP_RU_ITEM_INTERVAL_SECONDS, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // The state is process-global. Go runs a package's tests sequentially;
    // Rust runs them in parallel, so these serialize.
    static GLOBAL: Mutex<()> = Mutex::new(());

    fn reset() {
        GLOBAL_STATE.set_ru_consumer_count(0);
        reset_top_ru_item_interval();
    }

    // Go `TestTopRUEnableDisableAndResetInterval`.
    #[test]
    fn top_ru_is_reference_counted_and_resets_on_the_last_leave() {
        let _guard = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
        reset();

        assert!(!top_ru_enabled());
        enable_top_ru();
        assert!(top_ru_enabled());
        enable_top_ru();
        assert!(top_ru_enabled());

        assert!(set_top_ru_item_interval(ItemInterval::FIFTEEN_SECONDS).is_ok());
        assert_eq!(get_top_ru_item_interval(), 15);

        // One subscriber remains, so the interval stands.
        disable_top_ru();
        assert!(top_ru_enabled());
        assert_eq!(get_top_ru_item_interval(), 15);

        // The last one leaving resets it.
        disable_top_ru();
        assert!(!top_ru_enabled());
        assert_eq!(
            get_top_ru_item_interval(),
            DEF_TIDB_TOP_RU_ITEM_INTERVAL_SECONDS
        );

        // A surplus disable does not underflow.
        disable_top_ru();
        assert!(!top_ru_enabled());
        assert_eq!(GLOBAL_STATE.ru_consumer_count.load(Ordering::SeqCst), 0);
    }

    // Go `TestTopRUItemIntervalLastWriteWins`.
    #[test]
    fn the_latest_valid_interval_wins() {
        let _guard = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
        reset();

        for interval in [
            ItemInterval::THIRTY_SECONDS,
            ItemInterval::SIXTY_SECONDS,
            ItemInterval::FIFTEEN_SECONDS,
        ] {
            assert!(set_top_ru_item_interval(interval).is_ok());
            assert_eq!(get_top_ru_item_interval(), i64::from(interval.0));
        }
    }

    // Go `TestTopRUItemIntervalRejectsInvalid`.
    #[test]
    fn invalid_intervals_are_rejected_and_change_nothing() {
        let _guard = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
        reset();

        assert_eq!(
            set_top_ru_item_interval(ItemInterval(1)),
            Err(InvalidTopRuItemInterval(1))
        );
        assert_eq!(
            get_top_ru_item_interval(),
            DEF_TIDB_TOP_RU_ITEM_INTERVAL_SECONDS
        );

        // Unspecified is valid and normalizes to the default.
        assert!(set_top_ru_item_interval(ItemInterval::UNSPECIFIED).is_ok());
        assert_eq!(
            get_top_ru_item_interval(),
            DEF_TIDB_TOP_RU_ITEM_INTERVAL_SECONDS
        );

        assert!(set_top_ru_item_interval(ItemInterval::FIFTEEN_SECONDS).is_ok());
        assert_eq!(get_top_ru_item_interval(), 15);

        // A rejected value leaves the previous one in place.
        assert_eq!(
            set_top_ru_item_interval(ItemInterval(99)),
            Err(InvalidTopRuItemInterval(99))
        );
        assert_eq!(get_top_ru_item_interval(), 15);

        assert!(set_top_ru_item_interval(ItemInterval::UNSPECIFIED).is_ok());
        assert_eq!(
            get_top_ru_item_interval(),
            DEF_TIDB_TOP_RU_ITEM_INTERVAL_SECONDS
        );
    }

    // Top-SQL is a plain flag, and top profiling is on when either is.
    #[test]
    fn top_profiling_follows_either_feature() {
        let _guard = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
        reset();
        disable_top_sql();

        assert!(!top_sql_enabled());
        assert!(!top_profiling_enabled());

        enable_top_sql();
        assert!(top_sql_enabled());
        assert!(top_profiling_enabled());

        disable_top_sql();
        assert!(!top_profiling_enabled());

        // Top-RU alone also counts.
        enable_top_ru();
        assert!(top_profiling_enabled());
        disable_top_ru();
        assert!(!top_profiling_enabled());
    }
}
