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

//! Source-backed tests for `pkg/domain/topn_slow_query.go`, ported from
//! `pkg/domain/topn_slow_query_test.go` (`TestPush`, `TestRemoveExpired`,
//! `TestQueue`) with byte-exact expectations. `checkHeap` is ported as
//! [`check_heap`].

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tidb_exec::topn_slow_query::{SlowQueryHeap, SlowQueryInfo, TopNSlowQueries};

/// Go `topn_slow_query_test.go:133` `checkHeap`.
fn check_heap(heap: &SlowQueryHeap) {
    let data = heap.data();
    for i in 0..data.len() {
        let left = 2 * i + 1;
        let right = 2 * i + 2;
        if left < data.len() {
            assert!(data[i].duration <= data[left].duration);
        }
        if right < data.len() {
            assert!(data[i].duration <= data[right].duration);
        }
    }
}

fn millis(value: u64) -> Arc<SlowQueryInfo> {
    Arc::new(SlowQueryInfo {
        duration: Duration::from_millis(value),
        ..SlowQueryInfo::default()
    })
}

fn started(start: SystemTime, nanos: u64) -> Arc<SlowQueryInfo> {
    Arc::new(SlowQueryInfo {
        start: Some(start),
        duration: Duration::from_nanos(nanos),
        ..SlowQueryInfo::default()
    })
}

fn sql(text: &str) -> Arc<SlowQueryInfo> {
    Arc::new(SlowQueryInfo {
        sql: text.to_owned(),
        ..SlowQueryInfo::default()
    })
}

/// Go `topn_slow_query_test.go:24` `TestPush`.
#[test]
fn push_matches_source() {
    let mut slow_query = TopNSlowQueries::new(10, Duration::ZERO, 10);
    // Insert data into the heap.
    for value in [300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200] {
        slow_query.append(millis(value));
    }
    assert_eq!(
        slow_query.user().data()[0].duration,
        Duration::from_millis(300)
    );
    check_heap(slow_query.user());

    // Update all data in the heap.
    for (appended, expected_top) in [
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
        slow_query.append(millis(appended));
        assert_eq!(
            slow_query.user().data()[0].duration,
            Duration::from_millis(expected_top),
            "after appending {appended}ms"
        );
    }
    check_heap(slow_query.user());

    // Data smaller than heap top will not be inserted.
    slow_query.append(millis(1200));
    assert_eq!(
        slow_query.user().data()[0].duration,
        Duration::from_millis(1300)
    );
    slow_query.append(millis(666));
    assert_eq!(
        slow_query.user().data()[0].duration,
        Duration::from_millis(1300)
    );
}

/// Go `topn_slow_query_test.go:70` `TestRemoveExpired`.
#[test]
fn remove_expired_matches_source() {
    let now = SystemTime::now();
    let after = |seconds: u64| now + Duration::from_secs(seconds);
    let mut slow_query = TopNSlowQueries::new(6, Duration::from_secs(3), 10);

    slow_query.append(started(now, 6));
    slow_query.append(started(after(1), 5));
    slow_query.append(started(after(2), 4));
    slow_query.append(started(after(3), 3));
    slow_query.append(started(after(4), 2));
    assert_eq!(slow_query.user().data()[0].duration, Duration::from_nanos(2));

    slow_query.remove_expired(after(5));
    assert_eq!(slow_query.user().data().len(), 2);
    assert_eq!(slow_query.user().data()[0].duration, Duration::from_nanos(2));

    slow_query.append(started(after(3), 3));
    slow_query.append(started(after(4), 2));
    slow_query.append(started(after(5), 1));
    slow_query.append(started(after(6), 0));
    assert_eq!(slow_query.user().data().len(), 6);
    assert_eq!(slow_query.user().data()[0].duration, Duration::from_nanos(0));

    slow_query.remove_expired(after(6));
    assert_eq!(slow_query.user().data().len(), 4);
    assert_eq!(slow_query.user().data()[0].duration, Duration::from_nanos(0));
}

/// Go `topn_slow_query_test.go:97` `TestQueue`.
#[test]
fn queue_matches_source() {
    let mut q = TopNSlowQueries::new(10, Duration::from_secs(60), 5);
    q.append(sql("aaa"));
    q.append(sql("bbb"));
    q.append(sql("ccc"));

    let query = q.recent().query(1);
    assert_eq!(query[0].sql, "ccc");

    let query = q.recent().query(2);
    assert_eq!(query[0].sql, "ccc");
    assert_eq!(query[1].sql, "bbb");

    let query = q.recent().query(6);
    assert_eq!(query[0].sql, "ccc");
    assert_eq!(query[1].sql, "bbb");
    assert_eq!(query[2].sql, "aaa");

    q.append(sql("ddd"));
    q.append(sql("eee"));
    q.append(sql("fff"));
    q.append(sql("ggg"));

    let query = q.recent().query(3);
    assert_eq!(query[0].sql, "ggg");
    assert_eq!(query[1].sql, "fff");
    assert_eq!(query[2].sql, "eee");

    let query = q.recent().query(6);
    assert_eq!(query[0].sql, "ggg");
    assert_eq!(query[1].sql, "fff");
    assert_eq!(query[2].sql, "eee");
    assert_eq!(query[3].sql, "ddd");
    assert_eq!(query[4].sql, "ccc");
}
