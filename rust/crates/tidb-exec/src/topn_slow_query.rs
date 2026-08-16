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

//! COMPLETE port of Go `pkg/domain/topn_slow_query.go`: the `ADMIN SHOW SLOW`
//! backing store. Every declaration in that file is here -- `slowQueryHeap`
//! (its `heap.Interface` methods, `RemoveExpired`, `Query`), `slowQueryQueue`
//! (`Enqueue`, `Query`), `takeLastN`, `topNSlowQueries` (`newTopNSlowQueries`,
//! `Append`, `QueryAll`, `RemoveExpired`, `QueryRecent`, `QueryTop`, `Close`),
//! `showSlowMessage`, and `SlowQueryInfo`.
//!
//! It complements this crate's slow-log family ([`crate::slow_log_format`],
//! [`crate::slow_log_parse`]): those render and re-read the on-disk slow log,
//! while this keeps the in-memory top-N heaps and the recent FIFO ring that
//! `ADMIN SHOW SLOW RECENT/TOP` reads back.
//!
//! Go's `container/heap` is reproduced here verbatim (`up`, `down`, `init`,
//! `push`, `pop`) rather than reached for through [`std::collections::BinaryHeap`],
//! because the direct tests assert the exact array layout after each
//! `Append` and after `RemoveExpired`'s rebuild; `BinaryHeap`'s sift order is
//! not Go's.
//!
//! Boundaries and narrowings, each named:
//! - `topNSlowQueries.ch` (`chan *SlowQueryInfo`, cap 1000) and `.msgCh`
//!   (`chan *showSlowMessage`, cap 10) are dropped. Both are consumed only by
//!   `domain.topNSlowQueryLoop` in `pkg/domain/domain.go`, which is not
//!   ported; the value here is the store those channels feed, driven by
//!   direct method calls. Consequently `Close` records only the
//!   `mu.closed` flag and has no channel to close.
//! - `showSlowMessage`'s embedded `sync.WaitGroup` is dropped for the same
//!   reason: it is the loop's request/response handoff, not store state.
//!   [`ShowSlowMessage`] keeps the `request`/`result` pair.
//! - Go `ast.ShowSlow` / `ast.ShowSlowKind` are this workspace's
//!   [`tidb_ast::AdminShowSlowStmt`] / [`tidb_ast::AdminShowSlowTopScope`]
//!   (`Default`/`Internal`/`All` are Go's `ShowSlowKindDefault`/
//!   `ShowSlowKindInternal`/`ShowSlowKindAll`).
//! - Go's `*SlowQueryInfo` pointers, shared between the recent queue and the
//!   two heaps, become [`Arc<SlowQueryInfo>`]; the record is written once and
//!   only read afterwards, so shared ownership is the faithful shape.
//! - `SlowQueryInfo.Start` (`time.Time`) is [`SystemTime`], matching
//!   [`crate::stmt_cache`]'s narrowing of the same Go type; `Duration`
//!   (signed `time.Duration`) is [`Duration`], which is unsigned. Go never
//!   stores a negative slow-query duration, and `Less` is a plain `<`, so the
//!   ordering is unchanged.
//! - `slowQueryHeap.Query` calls Go's `sort.Sort`, which is unstable
//!   (pdqsort); [`SlowQueryHeap::query`] uses `sort_unstable_by_key` on the same
//!   key. Records that tie on `Duration` may therefore land in a different
//!   relative order than a given Go build produces -- Go does not promise one
//!   either.

use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use tidb_ast::{AdminShowSlowStmt, AdminShowSlowTopScope};

use crate::exec_details::ExecDetails;

/// Go `SlowQueryInfo`: one recorded slow query.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SlowQueryInfo {
    /// Go `SlowQueryInfo.SQL`.
    pub sql: String,
    /// Go `SlowQueryInfo.Start` (`time.Time`).
    pub start: Option<SystemTime>,
    /// Go `SlowQueryInfo.Duration`.
    pub duration: Duration,
    /// Go `SlowQueryInfo.Detail` (`execdetails.ExecDetails`).
    pub detail: ExecDetails,
    /// Go `SlowQueryInfo.ConnID`.
    pub conn_id: u64,
    /// Go `SlowQueryInfo.SessAlias`.
    pub sess_alias: String,
    /// Go `SlowQueryInfo.TxnTS`.
    pub txn_ts: u64,
    /// Go `SlowQueryInfo.User`.
    pub user: String,
    /// Go `SlowQueryInfo.DB`.
    pub db: String,
    /// Go `SlowQueryInfo.TableIDs`.
    pub table_ids: String,
    /// Go `SlowQueryInfo.IndexNames`.
    pub index_names: String,
    /// Go `SlowQueryInfo.Digest`.
    pub digest: String,
    /// Go `SlowQueryInfo.Internal`.
    pub internal: bool,
    /// Go `SlowQueryInfo.Succ`.
    pub succ: bool,
}

impl SlowQueryInfo {
    /// The `Start.Add(period)` deadline Go compares against `now`.
    ///
    /// Go's zero `time.Time` is a real instant far in the past; a record built
    /// without a start (Go's zero value) is therefore always expired, which
    /// [`None`] reproduces.
    fn outdate_time(&self, period: Duration) -> Option<SystemTime> {
        self.start.and_then(|start| start.checked_add(period))
    }
}

/// Go `showSlowMessage`: one `ADMIN SHOW SLOW` request and its answer.
///
/// The embedded `sync.WaitGroup` is not carried; see the module header.
#[derive(Clone, Debug)]
pub struct ShowSlowMessage {
    /// Go `showSlowMessage.request` (`*ast.ShowSlow`).
    pub request: AdminShowSlowStmt,
    /// Go `showSlowMessage.result`.
    pub result: Vec<Arc<SlowQueryInfo>>,
}

/// Go `takeLastN`: the last `count` records, newest first.
#[must_use]
pub fn take_last_n(data: &[Arc<SlowQueryInfo>], count: usize) -> Vec<Arc<SlowQueryInfo>> {
    let count = count.min(data.len());
    data.iter().rev().take(count).cloned().collect()
}

/// Go `slowQueryHeap`: a min-heap on `Duration`, so the top is the record the
/// next larger one evicts.
#[derive(Clone, Debug, Default)]
pub struct SlowQueryHeap {
    data: Vec<Arc<SlowQueryInfo>>,
}

impl SlowQueryHeap {
    /// Creates a heap with Go's `make([]*SlowQueryInfo, 0, topN)` capacity.
    #[must_use]
    pub fn with_capacity(top_n: usize) -> Self {
        Self {
            data: Vec::with_capacity(top_n),
        }
    }

    /// Go `slowQueryHeap.Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Reports whether the heap holds no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// The heap array in its current layout, in Go's `h.data` order.
    #[must_use]
    pub fn data(&self) -> &[Arc<SlowQueryInfo>] {
        &self.data
    }

    /// Go `slowQueryHeap.Less`: strictly by `Duration`.
    fn less(&self, i: usize, j: usize) -> bool {
        self.data[i].duration < self.data[j].duration
    }

    /// Go `slowQueryHeap.Swap`.
    fn swap(&mut self, i: usize, j: usize) {
        self.data.swap(i, j);
    }

    /// `container/heap.up`.
    fn sift_up(&mut self, mut j: usize) {
        loop {
            // Go computes `i := (j - 1) / 2` on a signed int, so `j == 0`
            // yields `i == j == 0` and breaks; index 0 is spelled out here
            // because unsigned arithmetic cannot reproduce that.
            if j == 0 {
                break;
            }
            let i = (j - 1) / 2;
            if !self.less(j, i) {
                break;
            }
            self.swap(i, j);
            j = i;
        }
    }

    /// `container/heap.down`, bounded by `n`.
    fn sift_down(&mut self, i0: usize, n: usize) {
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
            self.swap(i, j);
            i = j;
        }
    }

    /// `container/heap.Init`.
    fn heap_init(&mut self) {
        let n = self.data.len();
        for i in (0..n / 2).rev() {
            self.sift_down(i, n);
        }
    }

    /// `container/heap.Push` over Go `slowQueryHeap.Push`.
    fn heap_push(&mut self, info: Arc<SlowQueryInfo>) {
        self.data.push(info);
        self.sift_up(self.data.len() - 1);
    }

    /// `container/heap.Pop` over Go `slowQueryHeap.Pop`.
    fn heap_pop(&mut self) -> Option<Arc<SlowQueryInfo>> {
        let n = self.data.len().checked_sub(1)?;
        self.swap(0, n);
        self.sift_down(0, n);
        self.data.pop()
    }

    /// Go `slowQueryHeap.RemoveExpired`: drop records whose
    /// `Start + period` is not after `now`, then rebuild the heap.
    pub fn remove_expired(&mut self, now: SystemTime, period: Duration) {
        let mut idx = 0;
        for i in 0..self.data.len() {
            let live = self.data[i]
                .outdate_time(period)
                .is_some_and(|outdate| outdate > now);
            if live {
                self.data.swap(idx, i);
                idx += 1;
            }
        }
        if self.data.len() == idx {
            return;
        }

        // Rebuild the heap.
        self.data.truncate(idx);
        self.heap_init();
    }

    /// Go `slowQueryHeap.Query`: sort in place, then take the slowest `count`
    /// in decreasing order.
    pub fn query(&mut self, count: usize) -> Vec<Arc<SlowQueryInfo>> {
        // The sorted array still maintains the heap property.
        self.data.sort_unstable_by_key(|a| a.duration);

        // The result should be in decrease order.
        take_last_n(&self.data, count)
    }
}

/// Go `slowQueryQueue`: a bounded FIFO of the most recent records.
#[derive(Clone, Debug, Default)]
pub struct SlowQueryQueue {
    data: Vec<Arc<SlowQueryInfo>>,
    size: usize,
}

impl SlowQueryQueue {
    /// Creates a queue holding at most `size` records.
    #[must_use]
    pub fn with_size(size: usize) -> Self {
        Self {
            data: Vec::with_capacity(size),
            size,
        }
    }

    /// The queue contents, oldest first, as Go's `q.data`.
    #[must_use]
    pub fn data(&self) -> &[Arc<SlowQueryInfo>] {
        &self.data
    }

    /// Go `slowQueryQueue.Enqueue`: append, dropping the oldest once full.
    ///
    /// Go's `append(q.data, info)[1:]` reslices, so a zero-sized queue keeps
    /// nothing; the `remove(0)` here is the same observable result.
    pub fn enqueue(&mut self, info: Arc<SlowQueryInfo>) {
        self.data.push(info);
        if self.data.len() > self.size {
            self.data.remove(0);
        }
    }

    /// Go `slowQueryQueue.Query`: the newest `count` records, newest first.
    #[must_use]
    pub fn query(&self, count: usize) -> Vec<Arc<SlowQueryInfo>> {
        // Queue is empty.
        if self.data.is_empty() {
            return Vec::new();
        }
        take_last_n(&self.data, count)
    }
}

/// Go `topNSlowQueries`: two top-N heaps -- one for user statements, one for
/// internal ones -- plus a recent FIFO queue. N = 30 and period = 7 days by
/// default at the call site in `pkg/domain/domain.go`.
#[derive(Debug)]
pub struct TopNSlowQueries {
    recent: SlowQueryQueue,
    user: SlowQueryHeap,
    internal: SlowQueryHeap,
    top_n: usize,
    period: Duration,
    /// Go's `mu struct { sync.RWMutex; closed bool }`.
    mu: RwLock<bool>,
}

impl TopNSlowQueries {
    /// Go `newTopNSlowQueries`.
    #[must_use]
    pub fn new(top_n: usize, period: Duration, queue_size: usize) -> Self {
        Self {
            recent: SlowQueryQueue::with_size(queue_size),
            user: SlowQueryHeap::with_capacity(top_n),
            internal: SlowQueryHeap::with_capacity(top_n),
            top_n,
            period,
            mu: RwLock::new(false),
        }
    }

    /// The user-statement heap, Go's `q.user`.
    #[must_use]
    pub fn user(&self) -> &SlowQueryHeap {
        &self.user
    }

    /// The internal-statement heap, Go's `q.internal`.
    #[must_use]
    pub fn internal(&self) -> &SlowQueryHeap {
        &self.internal
    }

    /// The recent FIFO queue, Go's `q.recent`.
    #[must_use]
    pub fn recent(&self) -> &SlowQueryQueue {
        &self.recent
    }

    /// Go `topNSlowQueries.Append`.
    pub fn append(&mut self, info: Arc<SlowQueryInfo>) {
        // Put into the recent queue.
        self.recent.enqueue(Arc::clone(&info));

        let top_n = self.top_n;
        let heap = if info.internal {
            &mut self.internal
        } else {
            &mut self.user
        };

        // Heap is not full.
        if heap.data.len() < top_n {
            heap.heap_push(info);
            return;
        }

        // Replace the heap top.
        //
        // Go indexes `h.data[0]` unguarded; that is safe there only because a
        // zero `topN` never reaches this branch with an empty heap, and it is
        // guarded here for the same reason without changing behavior.
        if heap
            .data
            .first()
            .is_some_and(|top| info.duration > top.duration)
        {
            heap.heap_pop();
            heap.heap_push(info);
        }
    }

    /// Go `topNSlowQueries.QueryAll`: the whole recent queue, oldest first.
    #[must_use]
    pub fn query_all(&self) -> &[Arc<SlowQueryInfo>] {
        self.recent.data()
    }

    /// Go `topNSlowQueries.RemoveExpired`: expire both heaps at `q.period`.
    pub fn remove_expired(&mut self, now: SystemTime) {
        self.user.remove_expired(now, self.period);
        self.internal.remove_expired(now, self.period);
    }

    /// Go `topNSlowQueries.QueryRecent`.
    #[must_use]
    pub fn query_recent(&self, count: usize) -> Vec<Arc<SlowQueryInfo>> {
        self.recent.query(count)
    }

    /// Go `topNSlowQueries.QueryTop`.
    pub fn query_top(
        &mut self,
        count: usize,
        kind: AdminShowSlowTopScope,
    ) -> Vec<Arc<SlowQueryInfo>> {
        match kind {
            AdminShowSlowTopScope::Default => self.user.query(count),
            AdminShowSlowTopScope::Internal => self.internal.query(count),
            AdminShowSlowTopScope::All => {
                let mut tmp = Vec::with_capacity(self.user.data.len() + self.internal.data.len());
                tmp.extend(self.user.data.iter().cloned());
                tmp.extend(self.internal.data.iter().cloned());
                let mut tmp1 = SlowQueryHeap { data: tmp };
                tmp1.data.sort_unstable_by_key(|a| a.duration);
                take_last_n(&tmp1.data, count)
            }
        }
    }

    /// Go `topNSlowQueries.Close`: mark the store closed.
    ///
    /// Go additionally does `close(q.ch)`; there is no channel here (see the
    /// module header), so only the flag is recorded.
    pub fn close(&self) {
        *self.mu.write().expect("topNSlowQueries mutex") = true;
    }

    /// Reads Go's `q.mu.closed` under the read lock.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        *self.mu.read().expect("topNSlowQueries mutex")
    }
}
