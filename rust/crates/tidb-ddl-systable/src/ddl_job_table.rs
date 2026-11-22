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

//! A `mysql.tidb_ddl_job` row as the scheduler and the workers carry it:
//! the job (Go `job_meta`) and the scheduling columns. The table itself is
//! read and written through SQL (Go `systable.Manager`, `insertDDLJobs2Table`,
//! `updateDDLJob2Table`, `deleteDDLJob`).

use tidb_model::Job;

/// Go's delivered job object with the row's scheduling columns, which SQL
/// UPDATE / DELETE of the row preserve.
#[derive(Debug)]
pub struct ActiveDdlJob {
    /// Go `job_meta`.
    pub job: Job,
    /// Go `reorg` scheduling class.
    pub reorg: bool,
    /// Comma-separated involved schema IDs.
    pub schema_ids: String,
    /// Comma-separated involved table IDs.
    pub table_ids: String,
    /// Persisted action type column.
    pub type_: i64,
    /// Whether execution has started.
    pub processing: bool,
}

impl ActiveDdlJob {
    /// Go `worker.transitOneJobStep`: the delivered job is the worker's
    /// object; SQL UPDATE/DELETE preserve the stored scheduling columns.
    pub fn from_job(job: Job) -> Self {
        Self {
            reorg: job.may_need_reorg(),
            schema_ids: String::new(),
            table_ids: String::new(),
            type_: i64::from(job.type_.0),
            processing: true,
            job,
        }
    }

    /// The same row carrying `job` instead: Go's proxy job, which runs a
    /// multi-schema change's sub-job as if it were the job itself.
    pub fn with_job(&self, job: Job) -> Self {
        Self {
            job,
            reorg: self.reorg,
            schema_ids: self.schema_ids.clone(),
            table_ids: self.table_ids.clone(),
            type_: self.type_,
            processing: self.processing,
        }
    }
}
