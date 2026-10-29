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

//! Go `pkg/executor/distsql.go`'s `IndexLookUpRunTimeStats` and
//! `pkg/executor/index_merge_reader.go`'s `IndexMergeRuntimeStat`: what the
//! double readers register on their plan when they close.

use std::any::Any;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tidb_model::go_duration::format_go_duration;

use tidb_util::execdetails::{
    format_duration, RuntimeStats, TP_INDEX_LOOK_UP_RUN_TIME_STATS, TP_INDEX_MERGE_RUN_TIME_STATS,
};
use crate::executor::{ExecutorRuntimeStats, ExecutorRuntimeStatsSource};

fn nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

fn fmt(nanos: i64) -> String {
    format_duration(Duration::from_nanos(u64::try_from(nanos).unwrap_or(0)))
}

/// Go `IndexLookUpRunTimeStats` while the lookup runs.
#[derive(Debug, Default)]
pub struct IndexLookUpRunTimeStats {
    fetch_handle_total: AtomicI64,
    fetch_handle: AtomicI64,
    task_wait: AtomicI64,
    table_row_scan: AtomicI64,
    table_task_num: AtomicI64,
    concurrency: usize,
    next_wait_index_scan: AtomicI64,
    next_wait_table_lookup_build: AtomicI64,
    next_wait_table_lookup_resp: AtomicI64,
}

impl IndexLookUpRunTimeStats {
    /// Go `initRuntimeStats`' `Concurrency: e.indexLookupConcurrency`.
    #[must_use]
    pub fn new(concurrency: usize) -> Self {
        Self {
            concurrency,
            ..Self::default()
        }
    }

    /// Go `indexWorker.fetchHandles`' per-task `FetchHandle`,
    /// `FetchHandleTotal` and `TaskWait`.
    pub fn record_index_task(&self, fetch_handle: Duration, total: Duration, wait: Duration) {
        self.fetch_handle
            .fetch_add(nanos(fetch_handle), Ordering::Relaxed);
        self.fetch_handle_total
            .fetch_add(nanos(total), Ordering::Relaxed);
        self.task_wait.fetch_add(nanos(wait), Ordering::Relaxed);
    }

    /// Go `tableWorker.executeTask`'s `TableRowScan` and `TableTaskNum`.
    pub fn record_table_task(&self, elapsed: Duration) {
        self.table_row_scan
            .fetch_add(nanos(elapsed), Ordering::Relaxed);
        self.table_task_num.fetch_add(1, Ordering::Relaxed);
    }

    /// Go `IndexLookUpExecutor.Next`'s `NextWaitIndexScan`,
    /// `NextWaitTableLookUpBuild` and `NextWaitTableLookUpResp`.
    pub fn record_next_wait(&self, index_scan: Duration, build: Duration, resp: Duration) {
        self.next_wait_index_scan
            .fetch_add(nanos(index_scan), Ordering::Relaxed);
        self.next_wait_table_lookup_build
            .fetch_add(nanos(build), Ordering::Relaxed);
        self.next_wait_table_lookup_resp
            .fetch_add(nanos(resp), Ordering::Relaxed);
    }

    /// The stats as Close registers them.
    #[must_use]
    pub fn snapshot(&self) -> IndexLookUpRunTimeStatsSnapshot {
        IndexLookUpRunTimeStatsSnapshot {
            fetch_handle_total: self.fetch_handle_total.load(Ordering::Relaxed),
            fetch_handle: self.fetch_handle.load(Ordering::Relaxed),
            task_wait: self.task_wait.load(Ordering::Relaxed),
            table_row_scan: self.table_row_scan.load(Ordering::Relaxed),
            table_task_num: self.table_task_num.load(Ordering::Relaxed),
            concurrency: self.concurrency,
            next_wait_index_scan: self.next_wait_index_scan.load(Ordering::Relaxed),
            next_wait_table_lookup_build: self.next_wait_table_lookup_build.load(Ordering::Relaxed),
            next_wait_table_lookup_resp: self.next_wait_table_lookup_resp.load(Ordering::Relaxed),
        }
    }
}

/// Go `IndexLookUpRunTimeStats` as registered.
#[derive(Clone, Debug, Default)]
pub struct IndexLookUpRunTimeStatsSnapshot {
    fetch_handle_total: i64,
    fetch_handle: i64,
    task_wait: i64,
    table_row_scan: i64,
    table_task_num: i64,
    concurrency: usize,
    next_wait_index_scan: i64,
    next_wait_table_lookup_build: i64,
    next_wait_table_lookup_resp: i64,
}

impl RuntimeStats for IndexLookUpRunTimeStatsSnapshot {
    /// Go `IndexLookUpRunTimeStats.String`.
    fn string(&self) -> String {
        let mut buf = String::new();
        if self.fetch_handle != 0 {
            buf.push_str(&format!(
                "index_task: {{total_time: {}, fetch_handle: {}, build: {}, wait: {}}}",
                fmt(self.fetch_handle_total),
                fmt(self.fetch_handle),
                fmt(self.fetch_handle_total - self.fetch_handle - self.task_wait),
                fmt(self.task_wait),
            ));
        }
        if self.table_row_scan != 0 {
            if !buf.is_empty() {
                buf.push(',');
            }
            buf.push_str(&format!(
                " table_task: {{total_time: {}, num: {}, concurrency: {}}}",
                fmt(self.table_row_scan),
                self.table_task_num,
                self.concurrency
            ));
        }
        if (self.next_wait_index_scan > 0
            || self.next_wait_table_lookup_build > 0
            || self.next_wait_table_lookup_resp > 0)
            && !buf.is_empty()
        {
            buf.push(',');
            buf.push_str(&format!(
                " next: {{wait_index: {}, wait_table_lookup_build: {}, wait_table_lookup_resp: {}}}",
                fmt(self.next_wait_index_scan),
                fmt(self.next_wait_table_lookup_build),
                fmt(self.next_wait_table_lookup_resp),
            ));
        }
        buf
    }

    /// Go `IndexLookUpRunTimeStats.Merge`.
    fn merge(&mut self, other: &dyn RuntimeStats) {
        let Some(tmp) = other
            .as_any()
            .downcast_ref::<IndexLookUpRunTimeStatsSnapshot>()
        else {
            return;
        };
        self.fetch_handle_total += tmp.fetch_handle_total;
        self.fetch_handle += tmp.fetch_handle;
        self.task_wait += tmp.task_wait;
        self.table_row_scan += tmp.table_row_scan;
        self.table_task_num += tmp.table_task_num;
        self.next_wait_index_scan += tmp.next_wait_index_scan;
        self.next_wait_table_lookup_build += tmp.next_wait_table_lookup_build;
        self.next_wait_table_lookup_resp += tmp.next_wait_table_lookup_resp;
    }

    fn clone_box(&self) -> Box<dyn RuntimeStats> {
        Box::new(self.clone())
    }

    fn tp(&self) -> i32 {
        TP_INDEX_LOOK_UP_RUN_TIME_STATS
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// What an index lookup records while it runs: Go `IndexLookUpExecutor`'s
/// `stats`, the index plan root's `indexScanBasicStats`, and the table plan
/// root the table readers register their basic stats on.
#[derive(Clone)]
pub struct IndexLookUpStatsHandle {
    /// Go `e.stats`.
    pub stats: Arc<IndexLookUpRunTimeStats>,
    /// Go `indexScanBasicStats`.
    pub index_scan_basic: Option<Arc<dyn ExecutorRuntimeStats>>,
    /// Go `RuntimeStatsColl`, for each table task's reader.
    pub source: Arc<dyn ExecutorRuntimeStatsSource>,
    /// Go `getTableRootPlanID()`.
    pub table_root_plan_id: i64,
    /// Go `e.ID()`.
    pub plan_id: i64,
    /// Whether the empty chunk that ends the index stream was recorded.
    pub index_end_recorded: bool,
}

impl std::fmt::Debug for IndexLookUpStatsHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IndexLookUpStatsHandle")
            .field("plan_id", &self.plan_id)
            .finish_non_exhaustive()
    }
}

impl IndexLookUpStatsHandle {
    /// One table task finished after `elapsed`, returning `rows`: Go's table
    /// worker's `executeTask` stats and the task's table reader, which Go
    /// builds per task (so a second task makes its basic stats `total_`).
    pub fn record_table_task(&self, started: Instant, rows: usize) {
        let elapsed = started.elapsed();
        self.stats.record_table_task(elapsed);
        if let Some(basic) = self.source.basic_runtime_stats(self.table_root_plan_id) {
            basic.record_open(Duration::ZERO);
            basic.record_next(elapsed, i64::try_from(rows).unwrap_or(i64::MAX));
            // The reader is drained to its empty chunk.
            basic.record_next(Duration::ZERO, 0);
            basic.record_close(Duration::ZERO);
        }
    }

    /// Go `RegisterStats(e.ID(), e.stats)` from `Close`; the next `Open`
    /// starts new stats (Go `initRuntimeStats`).
    pub fn register_and_reset(&mut self) {
        self.source
            .register_stats(self.plan_id, Box::new(self.stats.snapshot()));
        self.stats = Arc::new(IndexLookUpRunTimeStats::new(self.stats.concurrency));
        self.index_end_recorded = false;
    }
}

/// Go `IndexMergeRuntimeStat` while the merge runs.
#[derive(Debug, Default)]
pub struct IndexMergeRuntimeStat {
    index_merge_process: AtomicI64,
    fetch_idx_time: AtomicI64,
    wait_time: AtomicI64,
    fetch_row: AtomicI64,
    table_task_num: AtomicI64,
    concurrency: usize,
}

impl IndexMergeRuntimeStat {
    /// Go `&IndexMergeRuntimeStat{Concurrency: e.Ctx()...IndexLookupConcurrency()}`.
    #[must_use]
    pub fn new(concurrency: usize) -> Self {
        Self {
            concurrency,
            ..Self::default()
        }
    }

    /// Go partial workers' `FetchIdxTime`.
    pub fn add_fetch_idx_time(&self, elapsed: Duration) {
        self.fetch_idx_time
            .fetch_add(nanos(elapsed), Ordering::Relaxed);
    }

    /// Go process worker's `IndexMergeProcess`.
    pub fn add_index_merge_process(&self, elapsed: Duration) {
        self.index_merge_process
            .fetch_add(nanos(elapsed), Ordering::Relaxed);
    }

    /// Go table worker's `WaitTime`, `FetchRow` and `TableTaskNum`.
    pub fn record_table_task(&self, wait: Duration, fetch_row: Duration) {
        self.wait_time.fetch_add(nanos(wait), Ordering::Relaxed);
        self.fetch_row
            .fetch_add(nanos(fetch_row), Ordering::Relaxed);
        self.table_task_num.fetch_add(1, Ordering::Relaxed);
    }

    /// The stats as Close registers them.
    #[must_use]
    pub fn snapshot(&self) -> IndexMergeRuntimeStatSnapshot {
        IndexMergeRuntimeStatSnapshot {
            index_merge_process: self.index_merge_process.load(Ordering::Relaxed),
            fetch_idx_time: self.fetch_idx_time.load(Ordering::Relaxed),
            wait_time: self.wait_time.load(Ordering::Relaxed),
            fetch_row: self.fetch_row.load(Ordering::Relaxed),
            table_task_num: self.table_task_num.load(Ordering::Relaxed),
            concurrency: self.concurrency,
        }
    }
}

/// Go `IndexMergeRuntimeStat` as registered.
#[derive(Clone, Debug, Default)]
pub struct IndexMergeRuntimeStatSnapshot {
    index_merge_process: i64,
    fetch_idx_time: i64,
    wait_time: i64,
    fetch_row: i64,
    table_task_num: i64,
    concurrency: usize,
}

impl RuntimeStats for IndexMergeRuntimeStatSnapshot {
    /// Go `IndexMergeRuntimeStat.String` (Go `time.Duration` `%s`).
    fn string(&self) -> String {
        let mut buf = String::new();
        if self.fetch_idx_time != 0 {
            buf.push_str(&format!(
                "index_task:{{fetch_handle:{}",
                format_go_duration(self.fetch_idx_time)
            ));
            if self.index_merge_process != 0 {
                buf.push_str(&format!(
                    ", merge:{}",
                    format_go_duration(self.index_merge_process)
                ));
            }
            buf.push('}');
        }
        if self.fetch_row != 0 {
            if !buf.is_empty() {
                buf.push(',');
            }
            buf.push_str(&format!(
                " table_task:{{num:{}, concurrency:{}, fetch_row:{}, wait_time:{}}}",
                self.table_task_num,
                self.concurrency,
                format_go_duration(self.fetch_row),
                format_go_duration(self.wait_time),
            ));
        }
        buf
    }

    /// Go `IndexMergeRuntimeStat.Merge`, including its
    /// `e.WaitTime += e.WaitTime`.
    fn merge(&mut self, other: &dyn RuntimeStats) {
        let Some(tmp) = other
            .as_any()
            .downcast_ref::<IndexMergeRuntimeStatSnapshot>()
        else {
            return;
        };
        self.index_merge_process += tmp.index_merge_process;
        self.fetch_idx_time += tmp.fetch_idx_time;
        self.fetch_row += tmp.fetch_row;
        self.wait_time += self.wait_time;
        self.table_task_num += tmp.table_task_num;
    }

    fn clone_box(&self) -> Box<dyn RuntimeStats> {
        Box::new(self.clone())
    }

    fn tp(&self) -> i32 {
        TP_INDEX_MERGE_RUN_TIME_STATS
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// What an index merge records while it runs: Go `IndexMergeReaderExecutor`'s
/// `stats`, each partial plan root's basic stats and the table plan root's.
#[derive(Clone)]
pub struct IndexMergeStatsHandle {
    /// Go `e.stats`.
    pub stats: Arc<IndexMergeRuntimeStat>,
    /// Go partial workers' readers, in the order the reader drains them.
    pub partial_basic: Vec<Option<Arc<dyn ExecutorRuntimeStats>>>,
    /// Go `RuntimeStatsColl`, for each table task's reader.
    pub source: Arc<dyn ExecutorRuntimeStatsSource>,
    /// The table plan's root.
    pub table_root_plan_id: i64,
    /// Go `e.ID()`.
    pub plan_id: i64,
}

impl std::fmt::Debug for IndexMergeStatsHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IndexMergeStatsHandle")
            .field("plan_id", &self.plan_id)
            .finish_non_exhaustive()
    }
}

impl IndexMergeStatsHandle {
    /// One partial batch of `rows` handles read in `elapsed` (an empty batch
    /// is the partial's end).
    pub fn record_partial_batch(&self, partial: usize, elapsed: Duration, rows: usize) {
        self.stats.add_fetch_idx_time(elapsed);
        if let Some(Some(basic)) = self.partial_basic.get(partial) {
            basic.record_next(elapsed, i64::try_from(rows).unwrap_or(i64::MAX));
        }
    }

    /// One table task: Go `executeTask`'s `WaitTime`/`FetchRow` and its
    /// table reader's basic stats.
    pub fn record_table_task(&self, wait: Duration, started: Instant, rows: usize) {
        let elapsed = started.elapsed();
        self.stats.record_table_task(wait, elapsed);
        if let Some(basic) = self.source.basic_runtime_stats(self.table_root_plan_id) {
            basic.record_open(Duration::ZERO);
            basic.record_next(elapsed, i64::try_from(rows).unwrap_or(i64::MAX));
            basic.record_next(Duration::ZERO, 0);
            basic.record_close(Duration::ZERO);
        }
    }

    /// Go `RegisterStats(e.ID(), e.stats)` from `Close`; the next run starts
    /// new stats.
    pub fn register_and_reset(&mut self) {
        self.source
            .register_stats(self.plan_id, Box::new(self.stats.snapshot()));
        self.stats = Arc::new(IndexMergeRuntimeStat::new(self.stats.concurrency));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go's `index_task: {total_time: 495.6µs, fetch_handle: 491.2µs, build:
    /// 874ns, wait: 3.54µs}, table_task: {total_time: 546.9µs, num: 1,
    /// concurrency: 5}, next: {wait_index: 532.6µs, wait_table_lookup_build:
    /// 44µs, wait_table_lookup_resp: 492.4µs}`.
    #[test]
    fn index_lookup_string_matches_gos_keys() {
        let stats = IndexLookUpRunTimeStats::new(5);
        stats.record_index_task(
            Duration::from_nanos(491_200),
            Duration::from_nanos(495_614),
            Duration::from_nanos(3_540),
        );
        stats.record_table_task(Duration::from_nanos(546_900));
        stats.record_next_wait(
            Duration::from_nanos(532_600),
            Duration::from_micros(44),
            Duration::from_nanos(492_400),
        );
        assert_eq!(
            stats.snapshot().string(),
            "index_task: {total_time: 495.6µs, fetch_handle: 491.2µs, build: 874ns, wait: \
             3.54µs}, table_task: {total_time: 546.9µs, num: 1, concurrency: 5}, next: \
             {wait_index: 532.6µs, wait_table_lookup_build: 44µs, wait_table_lookup_resp: \
             492.4µs}"
        );
    }

    /// Go's `index_task:{fetch_handle:13.014543ms, merge:2.625µs},
    /// table_task:{num:2, concurrency:5, fetch_row:2.191792ms,
    /// wait_time:13.100375ms}`.
    #[test]
    fn index_merge_string_matches_gos_keys() {
        let stats = IndexMergeRuntimeStat::new(5);
        stats.add_fetch_idx_time(Duration::from_nanos(13_014_543));
        stats.add_index_merge_process(Duration::from_nanos(2_625));
        stats.record_table_task(
            Duration::from_nanos(13_100_375),
            Duration::from_nanos(1_000_000),
        );
        stats.record_table_task(Duration::ZERO, Duration::from_nanos(1_191_792));
        assert_eq!(
            stats.snapshot().string(),
            "index_task:{fetch_handle:13.014543ms, merge:2.625µs}, table_task:{num:2, \
             concurrency:5, fetch_row:2.191792ms, wait_time:13.100375ms}"
        );
    }
}
