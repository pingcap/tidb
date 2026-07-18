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

//! Source-backed tests for analyze-job status and progress metadata.

use std::time::{Duration, SystemTime};

use tidb_stats::{
    AnalyzeJob, AnalyzeProgress, JobType, ANALYZE_FAILED, ANALYZE_FINISHED, ANALYZE_PENDING,
    ANALYZE_RUNNING, DUMP_TIME_INTERVAL, MAX_DELTA,
};

#[test]
fn source_status_labels_and_job_kinds_match_go() {
    assert_eq!(ANALYZE_PENDING, "pending");
    assert_eq!(ANALYZE_RUNNING, "running");
    assert_eq!(ANALYZE_FINISHED, "finished");
    assert_eq!(ANALYZE_FAILED, "failed");
    assert_eq!(JobType::TableAnalysis as i32, 1);
    assert_eq!(JobType::GlobalStatsMerge as i32, 2);
    assert_eq!(MAX_DELTA, 10_000_000);
    assert_eq!(DUMP_TIME_INTERVAL, Duration::from_secs(5));
}

#[test]
fn source_progress_accumulates_until_the_dump_threshold() {
    let progress = AnalyzeProgress::default();
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    progress.set_last_dump_time(now - Duration::from_secs(10));

    assert_eq!(progress.update_at(100, now), 0);
    assert_eq!(progress.get_delta_count(), 100);
    assert_eq!(progress.get_last_dump_time(), now - Duration::from_secs(10));
}

#[test]
fn source_progress_dumps_and_resets_after_threshold_and_interval() {
    let progress = AnalyzeProgress::default();
    let last_dump_time = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let first_update = last_dump_time + DUMP_TIME_INTERVAL + Duration::from_secs(1);
    progress.set_last_dump_time(last_dump_time);

    const SMALL_COUNT: i64 = 100;
    const LARGE_COUNT: i64 = 15_000_000;
    assert_eq!(progress.update_at(SMALL_COUNT, first_update), 0);
    assert_eq!(
        progress.update_at(LARGE_COUNT, first_update),
        SMALL_COUNT + LARGE_COUNT
    );
    assert_eq!(progress.get_delta_count(), 0);
    assert_eq!(progress.get_last_dump_time(), first_update);

    let second_update = first_update + Duration::from_secs(1);
    assert_eq!(progress.update_at(LARGE_COUNT, second_update), 0);
    assert_eq!(progress.get_delta_count(), LARGE_COUNT);
    assert_eq!(progress.get_last_dump_time(), first_update);
}

#[test]
fn source_job_defaults_match_go_zero_values() {
    let job = AnalyzeJob::default();
    assert_eq!(job.start_time, SystemTime::UNIX_EPOCH);
    assert_eq!(job.end_time, SystemTime::UNIX_EPOCH);
    assert_eq!(job.id, None);
    assert!(job.db_name.is_empty());
    assert!(job.table_name.is_empty());
    assert!(job.partition_name.is_empty());
    assert!(job.job_info.is_empty());
    assert!(job.sample_rate_reason.is_empty());
    assert_eq!(job.progress.get_delta_count(), 0);
}
