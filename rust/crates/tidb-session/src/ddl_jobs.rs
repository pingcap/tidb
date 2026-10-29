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

//! Go's `DDLJobRetriever`: the running jobs and the meta history read from
//! the store, and `appendJobToChunk`, which renders them for both
//! `ADMIN SHOW DDL JOBS` and `information_schema.DDL_JOBS`.

use tidb_driver_error::DriverError;
use tidb_model::Job;

use crate::Session;

impl Session {

    /// Go `DDLJobRetriever.initial`: the running jobs (`GetAllDDLJobs`),
    /// then the meta history newest first (`GetLastHistoryDDLJobsIterator`),
    /// read from the session's store.
    ///
    /// Both readers run on a fresh system-session transaction
    /// (`GetSysSession` + `sessiontxn.NewTxn`), so the jobs are the latest
    /// ones whatever the statement reads at: the shared catalog, not this
    /// statement's transaction or `tidb_snapshot` image.
    pub(crate) fn all_ddl_jobs(&mut self) -> Result<(Vec<Job>, Vec<Job>), DriverError> {
        self.ddl_job_history_store
            .clone()
            .ok_or_else(|| DriverError::unsupported("this session has no store to read DDL jobs from"))?
            .all_ddl_jobs()
            .map_err(|message| DriverError::unsupported(message))
    }
}
