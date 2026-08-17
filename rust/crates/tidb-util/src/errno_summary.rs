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

//! Complete transcreation of Go `pkg/errno/infoschema.go`: the mutex-guarded
//! error/warning counters backing `information_schema.CLIENT_ERRORS_SUMMARY_
//! {GLOBAL,BY_USER,BY_HOST}`.
//!
//! Go guards one package-global `instanceStatistics` with a `sync.Mutex` and
//! exposes it only through `IncrementError`/`IncrementWarning`/`FlushStats`
//! and the three `*Stats` readers, each of which deep-copies its map before
//! returning so callers can never observe (or corrupt) the live counters.
//! This module reproduces that shape as a `Stats` struct behind a
//! `std::sync::Mutex`, with the same four-map layout (global, per-user,
//! per-host) and the same copy-on-read contract. Go's `time.Time` becomes
//! `std::time::SystemTime`, which is `Copy` like `time.Time` and preserves
//! the same "record wall-clock instants, copy them by value" semantics.
//!
//! Go keeps `stats` as a single hidden package variable seeded by an `init()`
//! call to `FlushStats`; this module exposes the equivalent as [`Stats::new`]
//! plus a process-wide [`global`] instance so callers that want the Go
//! package-level behavior (a single shared instance) still get it, while
//! callers that want an isolated instance (e.g. tests) can construct their
//! own with [`Stats::new`].

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::SystemTime;

/// Go `ErrorSummary`: summarizes errors and warnings for one error code.
#[derive(Debug, Clone, Copy)]
pub struct ErrorSummary {
    /// Go `ErrorCount`.
    pub error_count: i64,
    /// Go `WarningCount`.
    pub warning_count: i64,
    /// Go `FirstSeen`.
    pub first_seen: SystemTime,
    /// Go `LastSeen`.
    pub last_seen: SystemTime,
}

impl ErrorSummary {
    /// Go's `&ErrorSummary{FirstSeen: seen}` literal used in `initCounters`:
    /// a fresh entry with both counters at zero and `LastSeen` at its Rust
    /// zero value (`UNIX_EPOCH`) until the first increment sets it, exactly
    /// mirroring Go's zero-value `time.Time` for an unset `LastSeen`.
    fn seen_at(first_seen: SystemTime) -> Self {
        ErrorSummary {
            error_count: 0,
            warning_count: 0,
            first_seen,
            last_seen: SystemTime::UNIX_EPOCH,
        }
    }
}

/// Go's private `instanceStatistics`: per-instance error/warning counters,
/// keyed by MySQL error code, and mirrored globally / per-user / per-host.
///
/// Go embeds `sync.Mutex` directly in the struct; this module holds the
/// three maps inside a `Mutex` instead, which gives the same "one lock guards
/// all three maps" invariant without unsafe embedding tricks.
pub struct Stats {
    inner: Mutex<StatsInner>,
}

struct StatsInner {
    global: HashMap<u16, ErrorSummary>,
    users: HashMap<String, HashMap<u16, ErrorSummary>>,
    hosts: HashMap<String, HashMap<u16, ErrorSummary>>,
}

impl StatsInner {
    fn empty() -> Self {
        StatsInner {
            global: HashMap::new(),
            users: HashMap::new(),
            hosts: HashMap::new(),
        }
    }
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}

impl Stats {
    /// Go's implicit zero-value `instanceStatistics` immediately reset by the
    /// package `init()` calling `FlushStats` — this constructor folds both
    /// steps into one, since there is no separate "uninitialized" state to
    /// observe here.
    pub fn new() -> Self {
        Stats {
            inner: Mutex::new(StatsInner::empty()),
        }
    }

    /// Go `FlushStats`: resets errors and warnings across global/users/hosts.
    pub fn flush_stats(&self) {
        let mut inner = self.lock();
        *inner = StatsInner::empty();
    }

    /// Go `GlobalStats`: summarizes errors and warnings across all
    /// users/hosts. Returns a deep copy so the caller cannot observe later
    /// mutations of the live map.
    pub fn global_stats(&self) -> HashMap<u16, ErrorSummary> {
        let inner = self.lock();
        copy_map(&inner.global)
    }

    /// Go `UserStats`: summarizes per-user. Deep-copies both map levels.
    pub fn user_stats(&self) -> HashMap<String, HashMap<u16, ErrorSummary>> {
        let inner = self.lock();
        inner
            .users
            .iter()
            .map(|(k, v)| (k.clone(), copy_map(v)))
            .collect()
    }

    /// Go `HostStats`: summarizes per remote-host. Deep-copies both map
    /// levels.
    pub fn host_stats(&self) -> HashMap<String, HashMap<u16, ErrorSummary>> {
        let inner = self.lock();
        inner
            .hosts
            .iter()
            .map(|(k, v)| (k.clone(), copy_map(v)))
            .collect()
    }

    /// Go's private `initCounters`: ensures the global/user/host entries for
    /// `err_code` exist, seeding freshly-created entries' `FirstSeen` with
    /// `seen`. Go takes its own lock separately from the caller's later lock
    /// acquisition (`IncrementError`/`IncrementWarning` call `initCounters`
    /// then lock again); this module takes one lock for the whole
    /// init-then-increment sequence per public method instead, which is
    /// observationally equivalent for a `Mutex` (no other Go goroutine can
    /// interleave between the two Go lock/unlock pairs at any point this
    /// crate's callers can distinguish) and avoids acquiring the same mutex
    /// twice per call.
    fn init_counters_locked(
        inner: &mut StatsInner,
        err_code: u16,
        user: &str,
        host: &str,
        seen: SystemTime,
    ) {
        inner
            .global
            .entry(err_code)
            .or_insert_with(|| ErrorSummary::seen_at(seen));
        inner
            .users
            .entry(user.to_string())
            .or_default()
            .entry(err_code)
            .or_insert_with(|| ErrorSummary::seen_at(seen));
        inner
            .hosts
            .entry(host.to_string())
            .or_default()
            .entry(err_code)
            .or_insert_with(|| ErrorSummary::seen_at(seen));
    }

    /// Go `IncrementError`: increments the global/user/host statistics for an
    /// `errCode`.
    pub fn increment_error(&self, err_code: u16, user: &str, host: &str) {
        let seen = SystemTime::now();
        let mut inner = self.lock();
        Self::init_counters_locked(&mut inner, err_code, user, host, seen);

        inner.global.get_mut(&err_code).unwrap().error_count += 1;
        inner.global.get_mut(&err_code).unwrap().last_seen = seen;

        let user_entry = inner
            .users
            .get_mut(user)
            .unwrap()
            .get_mut(&err_code)
            .unwrap();
        user_entry.error_count += 1;
        user_entry.last_seen = seen;

        let host_entry = inner
            .hosts
            .get_mut(host)
            .unwrap()
            .get_mut(&err_code)
            .unwrap();
        host_entry.error_count += 1;
        host_entry.last_seen = seen;
    }

    /// Go `IncrementWarning`: increments the global/user/host statistics for
    /// an `errCode`.
    pub fn increment_warning(&self, err_code: u16, user: &str, host: &str) {
        let seen = SystemTime::now();
        let mut inner = self.lock();
        Self::init_counters_locked(&mut inner, err_code, user, host, seen);

        inner.global.get_mut(&err_code).unwrap().warning_count += 1;
        inner.global.get_mut(&err_code).unwrap().last_seen = seen;

        let user_entry = inner
            .users
            .get_mut(user)
            .unwrap()
            .get_mut(&err_code)
            .unwrap();
        user_entry.warning_count += 1;
        user_entry.last_seen = seen;

        let host_entry = inner
            .hosts
            .get_mut(host)
            .unwrap()
            .get_mut(&err_code)
            .unwrap();
        host_entry.warning_count += 1;
        host_entry.last_seen = seen;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, StatsInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Go's private `copyMap`: deep-copies one error-code-keyed map.
fn copy_map(old: &HashMap<u16, ErrorSummary>) -> HashMap<u16, ErrorSummary> {
    old.iter().map(|(k, v)| (*k, *v)).collect()
}

/// Go's package-level `stats` variable: one process-wide instance, seeded
/// (like Go's `init()`) on first use. Go's `init()` runs unconditionally at
/// program start; `OnceLock` gives the same "exists and is flushed before
/// any other access" guarantee lazily, which is observationally identical
/// since nothing can read `stats` before Go's `init()` completes either.
pub fn global() -> &'static Stats {
    static GLOBAL: OnceLock<Stats> = OnceLock::new();
    GLOBAL.get_or_init(Stats::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go `TestCopySafety` (pkg/errno/infoschema_test.go). Ported onto a
    // private `Stats` instance instead of the package-global `stats` so this
    // test does not interfere with any other test that touches `global()`;
    // Go had only one test in the package and could safely use the shared
    // package variable.
    #[test]
    fn test_copy_safety() {
        let stats = Stats::new();
        stats.increment_error(123, "user", "host");
        stats.increment_error(321, "user2", "host2");
        stats.increment_warning(123, "user", "host");
        stats.increment_warning(999, "user", "host");
        stats.increment_warning(222, "u", "h");

        let global_copy = stats.global_stats();
        let user_copy = stats.user_stats();
        let host_copy = stats.host_stats();

        stats.increment_error(123, "user", "host");
        stats.increment_error(999, "user2", "host2");
        stats.increment_error(123, "user3", "host");
        stats.increment_warning(123, "user", "host");
        stats.increment_warning(222, "u", "h");
        stats.increment_warning(222, "a", "b");
        stats.increment_warning(333, "c", "d");

        // global stats
        assert_eq!(stats.inner.lock().unwrap().global[&123].error_count, 3);
        assert_eq!(global_copy[&123].error_count, 1);

        // user stats
        assert_eq!(stats.inner.lock().unwrap().users.len(), 6);
        assert_eq!(user_copy.len(), 3);
        assert_eq!(
            stats.inner.lock().unwrap().users["user"][&123].error_count,
            2
        );
        assert_eq!(
            stats.inner.lock().unwrap().users["user"][&123].warning_count,
            2
        );
        assert_eq!(user_copy["user"][&123].error_count, 1);
        assert_eq!(user_copy["user"][&123].warning_count, 1);

        // ensure there is no user3 in userCopy
        assert!(!user_copy.contains_key("user3"));
        assert!(stats.inner.lock().unwrap().users.contains_key("user3"));
        assert!(!user_copy.contains_key("a"));
        assert!(stats.inner.lock().unwrap().users.contains_key("a"));

        // host stats
        assert_eq!(stats.inner.lock().unwrap().hosts.len(), 5);
        assert_eq!(host_copy.len(), 3);

        stats.increment_error(123, "user3", "newhost");
        assert_eq!(stats.inner.lock().unwrap().hosts.len(), 6);
        assert_eq!(host_copy.len(), 3);

        // ensure there is no newhost in hostCopy
        assert!(!host_copy.contains_key("newhost"));
        assert!(stats.inner.lock().unwrap().hosts.contains_key("newhost"));
        assert!(!host_copy.contains_key("b"));
        assert!(stats.inner.lock().unwrap().hosts.contains_key("b"));
    }

    // New coverage (no Go equivalent): pins FlushStats's reset contract,
    // which Go's own test suite never exercises.
    #[test]
    fn flush_stats_resets_all_three_maps() {
        let stats = Stats::new();
        stats.increment_error(1, "u", "h");
        stats.increment_warning(2, "u2", "h2");
        assert!(!stats.global_stats().is_empty());
        assert!(!stats.user_stats().is_empty());
        assert!(!stats.host_stats().is_empty());

        stats.flush_stats();

        assert!(stats.global_stats().is_empty());
        assert!(stats.user_stats().is_empty());
        assert!(stats.host_stats().is_empty());
    }

    // New coverage: pins that a fresh entry's `first_seen` is set once and
    // never moves on subsequent increments, while `last_seen` advances every
    // time — the "seed on first touch, then only bump last_seen" contract
    // `initCounters` + the increment methods implement together in Go.
    #[test]
    fn first_seen_is_stable_and_last_seen_advances() {
        let stats = Stats::new();
        stats.increment_error(7, "u", "h");
        let first = stats.global_stats()[&7].first_seen;
        let last_after_first = stats.global_stats()[&7].last_seen;

        std::thread::sleep(std::time::Duration::from_millis(2));
        stats.increment_error(7, "u", "h");
        let snapshot = stats.global_stats();
        assert_eq!(snapshot[&7].first_seen, first);
        assert!(snapshot[&7].last_seen >= last_after_first);
        assert_eq!(snapshot[&7].error_count, 2);
    }

    // New coverage: increment_error and increment_warning both bump
    // last_seen and are independently counted, matching Go's separate
    // ErrorCount/WarningCount fields updated by separate functions.
    #[test]
    fn error_and_warning_counters_are_independent() {
        let stats = Stats::new();
        stats.increment_error(5, "u", "h");
        stats.increment_error(5, "u", "h");
        stats.increment_warning(5, "u", "h");

        let snapshot = stats.global_stats();
        assert_eq!(snapshot[&5].error_count, 2);
        assert_eq!(snapshot[&5].warning_count, 1);
    }

    // New coverage: the process-wide `global()` accessor returns the same
    // instance across calls, matching Go's single package-level `stats`
    // variable.
    #[test]
    fn global_accessor_is_a_singleton() {
        global().flush_stats();
        global().increment_error(42, "singleton-user", "singleton-host");
        assert_eq!(global().global_stats()[&42].error_count, 1);
        global().flush_stats();
    }
}
