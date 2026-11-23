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

//! Go `types.StatsAnalyze`'s analyze-job methods (`InsertAnalyzeJob`,
//! `StartAnalyzeJob`, `UpdateAnalyzeJobProgress`, `FinishAnalyzeJob`) over
//! the job Go's `statistics.AnalyzeJob` names, and `statistics.JobType`.

/// One row Go publishes in `mysql.analyze_jobs` before dispatching the
/// corresponding physical analyze task (Go `statistics.AnalyzeJob`'s
/// identity).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyzeJobSpec {
    /// Go `DBName`, stored in `table_schema`.
    pub schema: String,
    /// Go `TableName`, stored in `table_name`.
    pub table: String,
    /// Go `PartitionName`, empty for a nonpartition/global job.
    pub partition: String,
    /// Go `JobInfo`.
    pub job_info: String,
}

/// Go `statistics.JobType`: whether finishing a job also flushes its
/// residual processed-row count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnalyzeJobKind {
    /// Go `TableAnalysisJob`: a table, partition, or independent-index
    /// analysis.
    Table,
    /// Go `GlobalStatsMergeJob`: a partition-to-global statistics merge.
    GlobalStatsMerge,
}

/// Go `types.StatsAnalyze`'s analyze-job methods. Persistence errors are
/// logged by implementations and never change the result of `ANALYZE`.
pub trait AnalyzeJobLifecycle: Send + Sync {
    /// Go `InsertAnalyzeJob`: the job's generated ID when bookkeeping
    /// succeeds.
    fn insert(&self, spec: &AnalyzeJobSpec) -> Option<u64>;
    /// Go `StartAnalyzeJob`.
    fn start(&self, job_id: u64);
    /// Go `UpdateAnalyzeJobProgress`, with the processed-row delta its
    /// `job.Progress.Update` gives.
    fn update_progress(&self, job_id: u64, processed_rows: i64);
    /// Go `FinishAnalyzeJob`, flushing residual table-job progress.
    fn finish(&self, job_id: u64, processed_rows: i64, failure: Option<&str>, kind: AnalyzeJobKind);
}
