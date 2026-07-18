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

//! Analyze-job status and progress metadata from `pkg/statistics/analyze_jobs.go`.
//!
//! This leaf owns the source's job labels, job-kind values, and concurrent
//! processed-row counter.  SQL persistence, scheduler state, failpoint
//! handling, and statistics-handle lifecycle remain future owners.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

/// Analyze job has been queued but has not started.
pub const ANALYZE_PENDING: &str = "pending";

/// Analyze job is currently running.
pub const ANALYZE_RUNNING: &str = "running";

/// Analyze job completed successfully.
pub const ANALYZE_FINISHED: &str = "finished";

/// Analyze job completed with an error.
pub const ANALYZE_FAILED: &str = "failed";

/// A processed-row delta large enough to be persisted by the source.
pub const MAX_DELTA: i64 = 10_000_000;

/// Minimum interval between persisted processed-row updates.
pub const DUMP_TIME_INTERVAL: Duration = Duration::from_secs(5);

/// Source analyze-job kind values (`iota + 1`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum JobType {
    /// Analyze one table or partition.
    TableAnalysis = 1,
    /// Merge statistics at global scope.
    GlobalStatsMerge = 2,
}

/// Thread-safe processed-row progress for one analyze job.
#[derive(Debug)]
pub struct AnalyzeProgress {
    last_dump_time: Mutex<SystemTime>,
    delta_count: AtomicI64,
}

impl Default for AnalyzeProgress {
    fn default() -> Self {
        Self {
            // Go's zero time is before every normal wall-clock timestamp;
            // UNIX_EPOCH provides the same first-update behavior in Rust.
            last_dump_time: Mutex::new(SystemTime::UNIX_EPOCH),
            delta_count: AtomicI64::new(0),
        }
    }
}

impl AnalyzeProgress {
    /// Adds rows using the current wall-clock time.
    pub fn update(&self, row_count: i64) -> i64 {
        self.update_at(row_count, SystemTime::now())
    }

    /// Adds rows at an explicit time, preserving the source update boundary.
    ///
    /// The explicit timestamp keeps source-backed tests deterministic while
    /// [`Self::update`] remains the production-shaped API.
    pub fn update_at(&self, row_count: i64, now: SystemTime) -> i64 {
        let new_count = self
            .delta_count
            .fetch_add(row_count, Ordering::SeqCst)
            .wrapping_add(row_count);
        let mut last_dump_time = self
            .last_dump_time
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let elapsed = now
            .duration_since(*last_dump_time)
            .unwrap_or(Duration::ZERO);
        if new_count > MAX_DELTA && elapsed > DUMP_TIME_INTERVAL {
            self.delta_count.store(0, Ordering::SeqCst);
            *last_dump_time = now;
            return new_count;
        }
        0
    }

    /// Returns rows accumulated since the last persisted update.
    #[must_use]
    pub fn get_delta_count(&self) -> i64 {
        self.delta_count.load(Ordering::SeqCst)
    }

    /// Sets the timestamp of the last persisted update.
    pub fn set_last_dump_time(&self, time: SystemTime) {
        let mut last_dump_time = self
            .last_dump_time
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *last_dump_time = time;
    }

    /// Returns the timestamp of the last persisted update.
    #[must_use]
    pub fn get_last_dump_time(&self) -> SystemTime {
        *self
            .last_dump_time
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Metadata describing one analyze job.
#[derive(Debug)]
pub struct AnalyzeJob {
    /// Wall-clock time at which the job started.
    pub start_time: SystemTime,
    /// Wall-clock time at which the job ended.
    pub end_time: SystemTime,
    /// Storage identifier assigned to this job, when persisted.
    pub id: Option<u64>,
    /// Database containing the analyzed table.
    pub db_name: String,
    /// Table being analyzed.
    pub table_name: String,
    /// Partition being analyzed, if any.
    pub partition_name: String,
    /// Human-readable analyze operation description.
    pub job_info: String,
    /// Explanation for the selected sample rate.
    pub sample_rate_reason: String,
    /// Concurrent processed-row progress.
    pub progress: AnalyzeProgress,
}

impl Default for AnalyzeJob {
    fn default() -> Self {
        Self {
            start_time: SystemTime::UNIX_EPOCH,
            end_time: SystemTime::UNIX_EPOCH,
            id: None,
            db_name: String::new(),
            table_name: String::new(),
            partition_name: String::new(),
            job_info: String::new(),
            sample_rate_reason: String::new(),
            progress: AnalyzeProgress::default(),
        }
    }
}
