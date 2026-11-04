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

//! The `mysql.analyze_jobs` writers of Go `autoanalyze.go`: `InsertAnalyzeJob`,
//! `StartAnalyzeJob`, `UpdateAnalyzeJobProgress`, `FinishAnalyzeJob`,
//! `DeleteAnalyzeJobs` and the corrupted-job cleanups, plus the
//! `dataForAnalyzeStatusHelper` read.




/// One persisted `mysql.analyze_jobs` identifier.
pub type AnalyzeJobId = u64;

/// Pinned Go `BatchUpdateAnalyzeJobSQL`'s failure reason.
pub const CORRUPTED_ANALYZE_JOB_FAILURE: &str =
    "The TiDB Server has either shut down or the analyze query was terminated during the analyze job execution";

/// Storage-backed reader used by pinned Go `SHOW ANALYZE STATUS`.
pub trait AnalyzeStatusProvider: Send + Sync {
    /// Runs Go's fresh restricted read of the thirty newest persisted jobs.
    fn load_analyze_status(&self) -> Result<Vec<tidb_stats::AnalyzeStatusJob>, String>;

    /// Go `GlobalPDHelper.GetApproximateTableCountFromStorage`. Errors and a
    /// backend without PD both produce zero, as its caller ignores `hasPD`.
    fn approximate_table_count(
        &self,
        resource_group: &str,
        physical_id: i64,
        database: &str,
        table: &str,
        partition: &str,
    ) -> i64;
}
