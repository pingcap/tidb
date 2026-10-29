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

//! Go `ddl_history.go`: the cluster's running and historical DDL jobs.

/// The cluster's DDL jobs, Go's `mysql.tidb_ddl_job` and meta
/// `DDLJobHistory`, where a front end keeps them in a shared store rather
/// than in its own catalog.
pub trait DdlJobHistoryStore: Send + Sync {
    /// Go `IterAllDDLJobs`' order: the running jobs (`GetAllDDLJobs`, by job
    /// ID), then the meta history newest first
    /// (`GetLastHistoryDDLJobsIterator`).
    fn all_ddl_jobs(&self) -> Result<(Vec<tidb_model::Job>, Vec<tidb_model::Job>), String>;
}
