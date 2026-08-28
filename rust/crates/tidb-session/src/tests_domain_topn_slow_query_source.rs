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

//! Port of `pkg/domain/topn_slow_query_test.go` (origin/master):
//! `TestPush`, `TestRemoveExpired`, and `TestQueue`, driving
//! `tidb_domain::topn_slow_query::TopNSlowQueries` — the transcreation of
//! `pkg/domain/topn_slow_query.go`.
//!
//! The Go test reads the user heap's internal array (`slowQuery.user.data[0]`,
//! the MIN-duration entry, i.e. the heap TOP) and runs the `checkHeap` helper
//! over that array. Those internals are private to the transcreation, so the
//! observable used here is `query_top(top_n, Default)`: Go's `Query` sorts the
//! same array ascending and returns the last `count` descending, so the LAST
//! element it returns is exactly Go's `data[0]`, and its full length is
//! exactly `len(data)`. The internal array layout (sift order) additionally
//! stays pinned by `tidb-domain`'s own whitebox test
//! (`rust/crates/tidb-domain/src/topn_slow_query.rs`, `mod tests`), which
//! transcribes `checkHeap` directly.

#![cfg(test)]

use std::time::{Duration, SystemTime};

use tidb_domain::topn_slow_query::{SlowQueryInfo, TopNSlowQueries};

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn with_duration(duration: Duration) -> SlowQueryInfo {
    SlowQueryInfo {
        duration,
        ..SlowQueryInfo::default()
    }
}

/// The kept user-heap entries, slowest first, exactly the array Go's
/// `Query` would answer from.
fn user_kept(q: &mut TopNSlowQueries, top_n: usize) -> Vec<Duration> {
    q.query_top(top_n, tidb_ast::AdminShowSlowTopScope::Default)
        .into_iter()
        .map(|info| info.duration)
        .collect()
}

/// Go `pkg/domain/topn_slow_query_test.go:24::TestPush`: fill the heap, then
/// each slower append evicts the top in Go's exact order; entries at or below
/// the top never enter.
#[test]
fn push_replaces_the_heap_top_exactly_when_slower() {
    let mut slow_query = TopNSlowQueries::new(10, Duration::ZERO, 10);
    // Insert data into the heap.
    for millis in [300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200] {
        slow_query.append(with_duration(ms(millis)));
    }
    assert_eq!(user_kept(&mut slow_query, 10).last(), Some(&ms(300)));

    // Update all data in the heap.
    for (pushed, expected_top) in [
        (1300, 400),
        (1400, 500),
        (1500, 600),
        (1500, 700),
        (1600, 800),
        (1700, 900),
        (1800, 1000),
        (1900, 1100),
        (2000, 1200),
        (2100, 1300),
    ] {
        slow_query.append(with_duration(ms(pushed)));
        assert_eq!(user_kept(&mut slow_query, 10).last(), Some(&ms(expected_top)));
    }

    // Data smaller than heap top will not be inserted.
    slow_query.append(with_duration(ms(1200)));
    assert_eq!(user_kept(&mut slow_query, 10).last(), Some(&ms(1300)));
    slow_query.append(with_duration(ms(666)));
    assert_eq!(user_kept(&mut slow_query, 10).last(), Some(&ms(1300)));
}

/// Go `pkg/domain/topn_slow_query_test.go:70::TestRemoveExpired`: entries
/// whose `start + period` has passed leave the heap; survivors keep the heap
/// answering min-first.
#[test]
fn remove_expired_drops_by_start_plus_period() {
    let now = SystemTime::now();
    let mut slow_query = TopNSlowQueries::new(6, Duration::from_secs(3), 10);
    let at = |secs: u64| Some(now + Duration::from_secs(secs));
    let entry = |start: Option<SystemTime>, nanos: u64| SlowQueryInfo {
        start,
        duration: Duration::from_nanos(nanos),
        ..SlowQueryInfo::default()
    };

    slow_query.append(entry(at(0), 6));
    slow_query.append(entry(at(1), 5));
    slow_query.append(entry(at(2), 4));
    slow_query.append(entry(at(3), 3));
    slow_query.append(entry(at(4), 2));
    assert_eq!(user_kept(&mut slow_query, 6).last(), Some(&Duration::from_nanos(2)));

    slow_query.remove_expired(now + Duration::from_secs(5));
    let kept = user_kept(&mut slow_query, 6);
    assert_eq!(kept.len(), 2);
    assert_eq!(kept.last(), Some(&Duration::from_nanos(2)));

    slow_query.append(entry(at(3), 3));
    slow_query.append(entry(at(4), 2));
    slow_query.append(entry(at(5), 1));
    slow_query.append(entry(at(6), 0));
    let kept = user_kept(&mut slow_query, 6);
    assert_eq!(kept.len(), 6);
    assert_eq!(kept.last(), Some(&Duration::from_nanos(0)));

    slow_query.remove_expired(now + Duration::from_secs(6));
    let kept = user_kept(&mut slow_query, 6);
    // (start=now+3s, period=3s) expires exactly at `now+6s`, so the two
    // now+3s entries leave together with the now+2s one; now+4s..now+6s stay.
    assert_eq!(kept, vec![
        Duration::from_nanos(2),
        Duration::from_nanos(2),
        Duration::from_nanos(1),
        Duration::from_nanos(0),
    ]);
    assert_eq!(kept.last(), Some(&Duration::from_nanos(0)));
}

/// Go `pkg/domain/topn_slow_query_test.go:97::TestQueue`: the recent FIFO
/// evicts oldest-first and answers newest-first, truncating to `count`.
#[test]
fn the_recent_queue_answers_newest_first() {
    let mut q = TopNSlowQueries::new(10, Duration::from_secs(60), 5);
    for sql in ["aaa", "bbb", "ccc"] {
        q.append(SlowQueryInfo {
            sql: sql.to_owned(),
            ..SlowQueryInfo::default()
        });
    }

    let texts = |rows: Vec<SlowQueryInfo>| -> Vec<String> {
        rows.into_iter().map(|row| row.sql).collect()
    };

    assert_eq!(texts(q.query_recent(1)), ["ccc"]);
    assert_eq!(texts(q.query_recent(2)), ["ccc", "bbb"]);
    assert_eq!(texts(q.query_recent(6)), ["ccc", "bbb", "aaa"]);

    q.append(SlowQueryInfo { sql: "ddd".to_owned(), ..SlowQueryInfo::default() });
    q.append(SlowQueryInfo { sql: "eee".to_owned(), ..SlowQueryInfo::default() });
    q.append(SlowQueryInfo { sql: "fff".to_owned(), ..SlowQueryInfo::default() });
    q.append(SlowQueryInfo { sql: "ggg".to_owned(), ..SlowQueryInfo::default() });

    assert_eq!(texts(q.query_recent(3)), ["ggg", "fff", "eee"]);
    assert_eq!(
        texts(q.query_recent(6)),
        ["ggg", "fff", "eee", "ddd", "ccc"]
    );
}
