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

//! Go's DDL job lifecycle as this synchronous owner keeps it.
//!
//! Go's executor submits each job (`doDDLJob2`), its worker runs it to a
//! terminal state, and `AddHistoryDDLJob` files it under its ID in the meta
//! history that `information_schema.DDL_JOBS` and `ADMIN SHOW DDL JOBS` read
//! back newest-first. Here each DDL submits its jobs to the statement's
//! [`DdlJobSink`] at the point it is admitted (the point a Go job reaches the
//! submitter), and the session finishes them when the statement completes:
//! [`done_schema_state`] and [`rolls_back`] are the terminal states Go's
//! workers leave.

use std::sync::{Arc, Mutex};

use tidb_model::{ActionType, SchemaState};

/// One job a DDL statement submitted (Go `doDDLJob2`'s job), with the
/// identity known when it was submitted.
#[derive(Clone, Debug)]
pub struct SubmittedDdlJob {
    /// Go `Job.Type`.
    pub action: ActionType,
    /// Go `Job.SchemaName`: the lower-case schema name.
    pub schema_name: String,
    /// Go `Job.TableName`: the lower-case table name, empty for a schema-
    /// or policy-level job.
    pub table_name: String,
    /// Go `Job.SchemaID`, when the schema exists at submission.
    pub schema_id: Option<i64>,
    /// Go `Job.TableID`, when the table exists at submission.
    pub table_id: Option<i64>,
    /// The sub-jobs of a multi-schema change, in Go's submission order.
    pub sub_jobs: Vec<ActionType>,
    /// The tables of a job over several (Go `FinishMultipleTableJob`'s
    /// `MultipleTableInfos`, as RENAME TABLES finishes), by ID.
    pub multiple_table_ids: Vec<i64>,
    /// Go `Job.RowCount`: the rows a reorganizing job processed.
    pub row_count: i64,
    /// The job's `Query` rewrite, Go's for INTERVAL partitioning sugar: the
    /// clause's original text and what replaces it in the statement.
    pub query_rewrite: Option<(String, String)>,
}

impl SubmittedDdlJob {
    /// A job on `schema_name`.`table_name` (an empty table name for a
    /// schema-level job).
    #[must_use]
    pub fn new(action: ActionType, schema_name: &str, table_name: &str) -> Self {
        use tidb_hack::GoToLower;
        Self {
            action,
            schema_name: schema_name.go_to_lower(),
            table_name: table_name.go_to_lower(),
            schema_id: None,
            table_id: None,
            sub_jobs: Vec::new(),
            multiple_table_ids: Vec::new(),
            row_count: 0,
            query_rewrite: None,
        }
    }

    /// The same job with the IDs it was submitted against.
    #[must_use]
    pub fn with_ids(mut self, schema_id: Option<i64>, table_id: Option<i64>) -> Self {
        self.schema_id = schema_id;
        self.table_id = table_id;
        self
    }

    /// A multi-schema change carrying `sub_jobs`.
    #[must_use]
    pub fn with_sub_jobs(mut self, sub_jobs: Vec<ActionType>) -> Self {
        self.sub_jobs = sub_jobs;
        self
    }

    /// A job finishing several tables, by ID.
    #[must_use]
    pub fn with_multiple_tables(mut self, table_ids: Vec<i64>) -> Self {
        self.multiple_table_ids = table_ids;
        self
    }
}

/// The jobs one statement submitted, shared by every statement context the
/// session builds for it.
#[derive(Clone, Debug, Default)]
pub struct DdlJobSink(Arc<Mutex<Vec<SubmittedDdlJob>>>);

impl DdlJobSink {
    /// Files a submitted job.
    pub fn submit(&self, job: SubmittedDdlJob) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(job);
    }

    /// Adds rows processed to the job submitted last (Go's reorg worker
    /// counting into `Job.RowCount`).
    pub fn add_row_count(&self, rows: i64) {
        if let Some(job) = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .last_mut()
        {
            job.row_count = job.row_count.saturating_add(rows);
        }
    }

    /// Rewrites the query of the job submitted last: `original` (a clause's
    /// source text) becomes `replacement`.
    pub fn rewrite_query(&self, original: String, replacement: String) {
        if let Some(job) = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .last_mut()
        {
            job.query_rewrite = Some((original, replacement));
        }
    }

    /// The jobs submitted since the last call, in submission order.
    #[must_use]
    pub fn take(&self) -> Vec<SubmittedDdlJob> {
        std::mem::take(
            &mut *self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

/// The schema state Go's worker leaves a job in when it finishes it done:
/// `none` for the jobs whose object is gone (`FinishTableJob(JobStateDone,
/// StateNone, ...)` / `FinishDBJob`), `public` for the rest.
#[must_use]
pub fn done_schema_state(action: ActionType) -> SchemaState {
    use ActionType as A;
    match action {
        A::ACTION_DROP_COLUMN
        | A::ACTION_DROP_CHECK_CONSTRAINT
        | A::ACTION_DROP_FOREIGN_KEY
        | A::ACTION_DROP_INDEX
        | A::ACTION_DROP_PRIMARY_KEY
        | A::ACTION_DROP_TABLE_PARTITION
        | A::ACTION_TRUNCATE_TABLE_PARTITION
        | A::ACTION_EXCHANGE_TABLE_PARTITION
        | A::ACTION_REORGANIZE_PARTITION
        | A::ACTION_ALTER_TABLE_PARTITIONING
        | A::ACTION_REMOVE_PARTITIONING
        | A::ACTION_DROP_PLACEMENT_POLICY
        | A::ACTION_DROP_RESOURCE_GROUP
        | A::ACTION_DROP_SCHEMA
        | A::ACTION_DROP_TABLE
        | A::ACTION_DROP_VIEW
        | A::ACTION_DROP_SEQUENCE
        | A::ACTION_UNLOCK_TABLE
        | A::ACTION_MULTI_SCHEMA_CHANGE => SchemaState::NONE,
        _ => SchemaState::PUBLIC,
    }
}

/// Whether Go's worker rolls a failed job back (`rollback done`) rather than
/// cancelling it: the jobs that reorganize data once they have started.
#[must_use]
pub fn rolls_back(action: ActionType) -> bool {
    use ActionType as A;
    matches!(
        action,
        A::ACTION_ADD_INDEX
            | A::ACTION_ADD_PRIMARY_KEY
            | A::ACTION_ADD_COLUMNAR_INDEX
            | A::ACTION_MODIFY_COLUMN
            | A::ACTION_REORGANIZE_PARTITION
            | A::ACTION_ALTER_TABLE_PARTITIONING
            | A::ACTION_REMOVE_PARTITIONING
            | A::ACTION_MULTI_SCHEMA_CHANGE
    )
}

/// Go's INTERVAL query rewrite (`rewritePartitionQueryString` and its two
/// ALTER siblings): the statement with the clause's original text replaced,
/// or the statement unchanged when the clause cannot be found in it.
#[must_use]
pub fn rewritten_query(query: &str, rewrite: Option<&(String, String)>) -> String {
    match rewrite {
        Some((original, replacement)) if !original.is_empty() => {
            match query.find(original.as_str()) {
                Some(start) => format!(
                    "{}{replacement}{}",
                    &query[..start],
                    &query[start + original.len()..]
                ),
                None => query.to_owned(),
            }
        }
        _ => query.to_owned(),
    }
}
