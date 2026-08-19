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

//! Go `pkg/domain/topn_slow_query.go`: the recent/top slow-query store
//! behind `ADMIN SHOW SLOW`.
//!
//! Two min-heaps (user and internal statements) keep the top-N slowest
//! queries of the last period — the heap TOP is the smallest kept duration,
//! so a new slow query replaces it exactly when slower — and a fixed-size
//! FIFO keeps the most recent ones regardless of duration. N = 30 and
//! period = 7 days in the domain's defaults.
//!
//! Boundaries, named: the `ch`/`msgCh` channel pair belongs to
//! `Domain.topNSlowQueryLoop` (the serving goroutine), which arrives with
//! the domain run loop; [`TopNSlowQueries::close`] keeps the observable
//! `closed` latch. `SlowQueryInfo.Detail` (`execdetails.ExecDetails`) rides
//! with the unported execdetails surface.

use std::time::{Duration, SystemTime};

use tidb_ast::AdminShowSlowTopScope;

/// Go `SlowQueryInfo` (without `Detail`, module header).
#[derive(Clone, Debug, Default)]
pub struct SlowQueryInfo {
    /// Go `SQL`.
    pub sql: String,
    /// Go `Start`.
    pub start: Option<SystemTime>,
    /// Go `Duration`.
    pub duration: Duration,
    /// Go `ConnID`.
    pub conn_id: u64,
    /// Go `SessAlias`.
    pub sess_alias: String,
    /// Go `TxnTS`.
    pub txn_ts: u64,
    /// Go `User`.
    pub user: String,
    /// Go `DB`.
    pub db: String,
    /// Go `TableIDs`.
    pub table_ids: String,
    /// Go `IndexNames`.
    pub index_names: String,
    /// Go `Digest`.
    pub digest: String,
    /// Go `Internal`.
    pub internal: bool,
    /// Go `Succ`.
    pub succ: bool,
}

/// Go `slowQueryHeap`: a MIN-heap on duration, kept as Go's
/// `container/heap` array so the sift orders (and therefore the observable
/// `data[0]`) match exactly.
#[derive(Debug, Default)]
struct SlowQueryHeap {
    data: Vec<SlowQueryInfo>,
}

impl SlowQueryHeap {
    fn less(&self, i: usize, j: usize) -> bool {
        self.data[i].duration < self.data[j].duration
    }

    /// Go `heap.Push` + `up`.
    fn push(&mut self, info: SlowQueryInfo) {
        self.data.push(info);
        self.up(self.data.len() - 1);
    }

    /// Go `heap.Pop`: swap root and last, sift down, pop.
    fn pop(&mut self) -> Option<SlowQueryInfo> {
        if self.data.is_empty() {
            return None;
        }
        let last = self.data.len() - 1;
        self.data.swap(0, last);
        let popped = self.data.pop();
        if !self.data.is_empty() {
            self.down(0, self.data.len());
        }
        popped
    }

    fn up(&mut self, mut j: usize) {
        while j > 0 {
            let i = (j - 1) / 2;
            if i == j || !self.less(j, i) {
                break;
            }
            self.data.swap(i, j);
            j = i;
        }
    }

    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n {
                break;
            }
            let mut j = j1;
            let j2 = j1 + 1;
            if j2 < n && self.less(j2, j1) {
                j = j2;
            }
            if !self.less(j, i) {
                break;
            }
            self.data.swap(i, j);
            i = j;
        }
        i > i0
    }

    /// Go `heap.Init`.
    fn init(&mut self) {
        let n = self.data.len();
        for i in (0..n / 2).rev() {
            self.down(i, n);
        }
    }

    /// Go `RemoveExpired`: keep the entries whose `start + period` is still
    /// in the future, then rebuild the heap.
    fn remove_expired(&mut self, now: SystemTime, period: Duration) {
        let before = self.data.len();
        self.data.retain(|info| {
            info.start
                .map_or(false, |start| start + period > now)
        });
        if self.data.len() == before {
            return;
        }
        self.init();
    }

    /// Go `Query`: sort ascending (the sorted array still satisfies the
    /// heap property) and answer the LAST `count`, slowest first.
    fn query(&mut self, count: usize) -> Vec<SlowQueryInfo> {
        self.data
            .sort_by(|left, right| left.duration.cmp(&right.duration));
        take_last_n(&self.data, count)
    }
}

/// Go `slowQueryQueue`: a fixed-size FIFO.
#[derive(Debug, Default)]
struct SlowQueryQueue {
    data: Vec<SlowQueryInfo>,
    size: usize,
}

impl SlowQueryQueue {
    fn enqueue(&mut self, info: SlowQueryInfo) {
        self.data.push(info);
        if self.data.len() > self.size {
            self.data.remove(0);
        }
    }

    fn query(&self, count: usize) -> Vec<SlowQueryInfo> {
        // Queue is empty.
        take_last_n(&self.data, count)
    }
}

/// Go `takeLastN`: the last `count` entries, newest/largest first.
fn take_last_n(data: &[SlowQueryInfo], count: usize) -> Vec<SlowQueryInfo> {
    data.iter().rev().take(count).cloned().collect()
}

/// Go `topNSlowQueries` (its data core; channels are the run loop's, module
/// header).
#[derive(Debug)]
pub struct TopNSlowQueries {
    recent: SlowQueryQueue,
    user: SlowQueryHeap,
    internal: SlowQueryHeap,
    top_n: usize,
    period: Duration,
    closed: bool,
}

impl TopNSlowQueries {
    /// Go `newTopNSlowQueries`.
    #[must_use]
    pub fn new(top_n: usize, period: Duration, queue_size: usize) -> Self {
        Self {
            recent: SlowQueryQueue {
                data: Vec::with_capacity(queue_size),
                size: queue_size,
            },
            user: SlowQueryHeap {
                data: Vec::with_capacity(top_n),
            },
            internal: SlowQueryHeap {
                data: Vec::with_capacity(top_n),
            },
            top_n,
            period,
            closed: false,
        }
    }

    /// Go `Append`: always into the recent FIFO; into the matching heap
    /// while it has room, else replacing the heap top exactly when slower.
    pub fn append(&mut self, info: SlowQueryInfo) {
        self.recent.enqueue(info.clone());
        let heap = if info.internal {
            &mut self.internal
        } else {
            &mut self.user
        };
        if heap.data.len() < self.top_n {
            heap.push(info);
            return;
        }
        if info.duration > heap.data[0].duration {
            heap.pop();
            heap.push(info);
        }
    }

    /// Go `QueryAll`.
    #[must_use]
    pub fn query_all(&self) -> &[SlowQueryInfo] {
        &self.recent.data
    }

    /// Go `RemoveExpired`.
    pub fn remove_expired(&mut self, now: SystemTime) {
        self.user.remove_expired(now, self.period);
        self.internal.remove_expired(now, self.period);
    }

    /// Go `QueryRecent`.
    #[must_use]
    pub fn query_recent(&self, count: usize) -> Vec<SlowQueryInfo> {
        self.recent.query(count)
    }

    /// Go `QueryTop` over `ast.ShowSlowKind`
    /// ([`AdminShowSlowTopScope`] here): user by default, internal, or both
    /// merged and re-sorted.
    #[must_use]
    pub fn query_top(&mut self, count: usize, kind: AdminShowSlowTopScope) -> Vec<SlowQueryInfo> {
        match kind {
            AdminShowSlowTopScope::Default => self.user.query(count),
            AdminShowSlowTopScope::Internal => self.internal.query(count),
            AdminShowSlowTopScope::All => {
                let mut merged: Vec<SlowQueryInfo> = self
                    .user
                    .data
                    .iter()
                    .chain(self.internal.data.iter())
                    .cloned()
                    .collect();
                merged.sort_by(|left, right| left.duration.cmp(&right.duration));
                take_last_n(&merged, count)
            }
        }
    }

    /// Go `Close`: latch the closed flag (the channel close belongs to the
    /// run loop, module header).
    pub fn close(&mut self) {
        self.closed = true;
    }

    /// Whether [`Self::close`] ran, Go's `mu.closed`.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    fn with_duration(duration: Duration) -> SlowQueryInfo {
        SlowQueryInfo {
            duration,
            ..SlowQueryInfo::default()
        }
    }

    /// Go `checkHeap`.
    fn check_heap(heap: &SlowQueryHeap) {
        for i in 0..heap.data.len() {
            let left = 2 * i + 1;
            let right = 2 * i + 2;
            if left < heap.data.len() {
                assert!(heap.data[i].duration <= heap.data[left].duration);
            }
            if right < heap.data.len() {
                assert!(heap.data[i].duration <= heap.data[right].duration);
            }
        }
    }

    /// Go `TestPush` (`topn_slow_query_test.go:26`), verbatim: fill the
    /// heap, then each faster-than-none append replaces the top in Go's
    /// exact order; entries at or below the top never enter.
    #[test]
    fn push_replaces_the_heap_top_exactly_when_slower() {
        let mut slow_query = TopNSlowQueries::new(10, Duration::ZERO, 10);
        for millis in [300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200] {
            slow_query.append(with_duration(ms(millis)));
        }
        assert_eq!(slow_query.user.data[0].duration, ms(300));
        check_heap(&slow_query.user);

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
            assert_eq!(slow_query.user.data[0].duration, ms(expected_top));
        }
        check_heap(&slow_query.user);

        // Data smaller than the heap top will not be inserted.
        slow_query.append(with_duration(ms(1200)));
        assert_eq!(slow_query.user.data[0].duration, ms(1300));
        slow_query.append(with_duration(ms(666)));
        assert_eq!(slow_query.user.data[0].duration, ms(1300));
    }

    /// Go `TestRemoveExpired` (`:70`).
    #[test]
    fn remove_expired_drops_by_start_plus_period() {
        let now = SystemTime::now();
        let mut slow_query = TopNSlowQueries::new(6, Duration::from_secs(3), 10);
        let at = |secs: u64| Some(now + Duration::from_secs(secs));

        for (start, nanos) in [(0, 6), (1, 5), (2, 4), (3, 3), (4, 2)] {
            slow_query.append(SlowQueryInfo {
                start: at(start),
                duration: Duration::from_nanos(nanos),
                ..SlowQueryInfo::default()
            });
        }
        assert_eq!(slow_query.user.data[0].duration, Duration::from_nanos(2));

        slow_query.remove_expired(now + Duration::from_secs(5));
        assert_eq!(slow_query.user.data.len(), 2);
        assert_eq!(slow_query.user.data[0].duration, Duration::from_nanos(2));

        for (start, nanos) in [(3, 3), (4, 2), (5, 1), (6, 0)] {
            slow_query.append(SlowQueryInfo {
                start: at(start),
                duration: Duration::from_nanos(nanos),
                ..SlowQueryInfo::default()
            });
        }
        assert_eq!(slow_query.user.data.len(), 6);
        assert_eq!(slow_query.user.data[0].duration, Duration::from_nanos(0));

        slow_query.remove_expired(now + Duration::from_secs(6));
        assert_eq!(slow_query.user.data.len(), 4);
        assert_eq!(slow_query.user.data[0].duration, Duration::from_nanos(0));
    }

    /// Go `TestQueue` (`:98`): the FIFO evicts oldest-first and answers
    /// newest-first.
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

        for sql in ["ddd", "eee", "fff", "ggg"] {
            q.append(SlowQueryInfo {
                sql: sql.to_owned(),
                ..SlowQueryInfo::default()
            });
        }
        assert_eq!(texts(q.query_recent(3)), ["ggg", "fff", "eee"]);
        assert_eq!(
            texts(q.query_recent(6)),
            ["ggg", "fff", "eee", "ddd", "ccc"]
        );
    }

    /// `QueryTop`'s three scopes over one mixed load, plus the close latch.
    #[test]
    fn query_top_scopes_split_user_and_internal() {
        let mut q = TopNSlowQueries::new(10, Duration::from_secs(60), 5);
        q.append(SlowQueryInfo {
            duration: ms(100),
            ..SlowQueryInfo::default()
        });
        q.append(SlowQueryInfo {
            duration: ms(300),
            internal: true,
            ..SlowQueryInfo::default()
        });
        q.append(SlowQueryInfo {
            duration: ms(200),
            ..SlowQueryInfo::default()
        });

        let durations = |rows: Vec<SlowQueryInfo>| -> Vec<Duration> {
            rows.into_iter().map(|row| row.duration).collect()
        };
        assert_eq!(
            durations(q.query_top(10, AdminShowSlowTopScope::Default)),
            [ms(200), ms(100)]
        );
        assert_eq!(
            durations(q.query_top(10, AdminShowSlowTopScope::Internal)),
            [ms(300)]
        );
        assert_eq!(
            durations(q.query_top(2, AdminShowSlowTopScope::All)),
            [ms(300), ms(200)]
        );

        assert!(!q.is_closed());
        q.close();
        assert!(q.is_closed());
    }
}
