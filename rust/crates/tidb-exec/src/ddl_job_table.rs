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

//! The active DDL queue stored in `mysql.tidb_ddl_job`.
//!
//! This is the storage half of pinned Go `pkg/ddl/jobsubmit/submit.go`,
//! `pkg/ddl/job_scheduler.go`, and `pkg/ddl/job_worker.go`:
//!
//! * submission inserts all seven columns, with the Go-compatible encoded
//!   [`tidb_model::Job`] in `job_meta`;
//! * every worker step updates only `job_meta`;
//! * a new owner scans rows in `job_id` order and decodes the same envelope;
//! * terminal handling deletes the active row.
//!
//! Scheduling and action execution deliberately do not live here. Keeping the
//! table protocol independent is what lets every DDL action share the one Go
//! queue instead of growing an action-specific recovery record.

use std::fmt;

use tidb_datatype::Datum;
use tidb_model::Job;
use tidb_txnkv::transaction::OptimisticMutation;

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::mysql_system_tables::{
    scan_system_table_from_int_handle, scan_system_table_prefixed, SystemRow, SystemTableError,
    SystemTableView,
};
use crate::system_row_write::{
    delete_clustered_row, store_clustered_row, RowEncodeError, RowValues,
};

/// One decoded active DDL row together with the exact stored row values needed
/// by Go SQL `UPDATE`/`DELETE` semantics.
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
    values: RowValues,
}

/// The stored table definition and projection used by the active-job queue.
#[derive(Clone, Debug)]
pub struct DdlJobTable {
    table: Box<tidb_model::TableInfo>,
    view: SystemTableView,
}

/// A malformed or inaccessible active-job row.
#[derive(Debug)]
pub enum DdlJobTableError {
    /// The system table could not be located or decoded.
    Table(SystemTableError),
    /// A row could not be encoded according to its stored `TableInfo`.
    Row(RowEncodeError),
    /// `job_meta` is not a Go-compatible persisted job envelope.
    Job(serde_json::Error),
    /// A required column is SQL NULL.
    MissingColumn(&'static str),
}

impl fmt::Display for DdlJobTableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table(error) => write!(formatter, "{error}"),
            Self::Row(error) => write!(formatter, "{error}"),
            Self::Job(error) => write!(formatter, "DDL job metadata is invalid: {error}"),
            Self::MissingColumn(column) => {
                write!(formatter, "mysql.tidb_ddl_job.`{column}` is NULL")
            }
        }
    }
}

impl std::error::Error for DdlJobTableError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Table(error) => Some(error),
            Self::Row(error) => Some(error),
            Self::Job(error) => Some(error),
            Self::MissingColumn(_) => None,
        }
    }
}

impl From<SystemTableError> for DdlJobTableError {
    fn from(error: SystemTableError) -> Self {
        Self::Table(error)
    }
}

impl From<RowEncodeError> for DdlJobTableError {
    fn from(error: RowEncodeError) -> Self {
        Self::Row(error)
    }
}

impl From<serde_json::Error> for DdlJobTableError {
    fn from(error: serde_json::Error) -> Self {
        Self::Job(error)
    }
}

impl DdlJobTable {
    /// Locates the pinned active-job table in one loaded catalog.
    pub fn locate(catalog: &ClusterCatalog) -> Result<Self, DdlJobTableError> {
        let (_, table) = catalog.find_table("mysql", "tidb_ddl_job").ok_or_else(|| {
            SystemTableError::Missing {
                name: "mysql.tidb_ddl_job".to_owned(),
            }
        })?;
        let table = table.clone_like_go();
        let view = SystemTableView::project(
            "mysql.tidb_ddl_job",
            &table,
            &[
                "job_id",
                "reorg",
                "schema_ids",
                "table_ids",
                "job_meta",
                "type",
                "processing",
            ],
        );
        Ok(Self {
            table: Box::new(table),
            view,
        })
    }

    fn column_id(&self, name: &'static str) -> Result<i64, DdlJobTableError> {
        self.table
            .cols()
            .iter_deref()
            .find(|column| column.read().name.lowercase() == name)
            .map(|column| column.read().id)
            .ok_or_else(|| {
                DdlJobTableError::Table(SystemTableError::MissingColumn {
                    name: "mysql.tidb_ddl_job".to_owned(),
                    column: name.to_owned(),
                })
            })
    }

    fn value(
        &self,
        values: &mut RowValues,
        column: &'static str,
        datum: Datum,
    ) -> Result<(), DdlJobTableError> {
        values.insert(self.column_id(column)?, datum);
        Ok(())
    }

    /// Scans active jobs in the table's clustered `job_id` order, exactly as
    /// Go's scheduler query does.
    pub fn load<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
    ) -> Result<Vec<ActiveDdlJob>, DdlJobTableError> {
        self.load_from(snapshot, i64::MIN)
    }

    /// Go scheduler query's `job_id >= MinJobIDRefresher.GetCurrMinJobID()`
    /// lower bound.
    pub fn load_from<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        min_job_id: i64,
    ) -> Result<Vec<ActiveDdlJob>, DdlJobTableError> {
        let pairs = scan_system_table_from_int_handle(snapshot, &self.view, min_job_id)?;
        let mut jobs = Vec::with_capacity(pairs.len());
        for (key, value) in pairs {
            let row = SystemRow::parse(&self.view, &key, &value)?;
            if row.i64("job_id")?.unwrap_or_default() < min_job_id {
                continue;
            }
            let reorg = row.i64("reorg")?.unwrap_or_default() != 0;
            let schema_ids = row.text("schema_ids")?.unwrap_or_default();
            let table_ids = row.text("table_ids")?.unwrap_or_default();
            let job_meta = row
                .bytes("job_meta")?
                .ok_or(DdlJobTableError::MissingColumn("job_meta"))?;
            let type_ = row.i64("type")?.unwrap_or_default();
            let processing = row.i64("processing")?.unwrap_or_default() != 0;
            let values = row.into_values();
            let mut job = Job::default();
            job.decode(&job_meta)?;
            jobs.push(ActiveDdlJob {
                job,
                reorg,
                schema_ids,
                table_ids,
                type_,
                processing,
                values,
            });
        }
        Ok(jobs)
    }

    /// Pinned Go `systable.Manager.GetJobBytesByIDWithSe`.
    pub fn job_bytes_by_id<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        job_id: i64,
    ) -> Result<Option<Vec<u8>>, DdlJobTableError> {
        for (key, value) in scan_system_table_prefixed(snapshot, &self.view, &[Datum::Int(job_id)])?
        {
            let row = SystemRow::parse(&self.view, &key, &value)?;
            if row.i64("job_id")? == Some(job_id) {
                // Go chunk.Row.GetBytes returns an empty slice for SQL NULL;
                // GetJobBytesByIDWithSe itself still succeeds in that case.
                return Ok(Some(row.bytes("job_meta")?.unwrap_or_default()));
            }
        }
        Ok(None)
    }

    /// Pinned Go `systable.Manager.GetMinJobID`.
    pub fn min_job_id<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        previous_min_job_id: i64,
    ) -> Result<Option<i64>, DdlJobTableError> {
        let mut minimum = None;
        for (key, value) in
            scan_system_table_from_int_handle(snapshot, &self.view, previous_min_job_id)?
        {
            let row = SystemRow::parse(&self.view, &key, &value)?;
            if let Some(job_id) = row.i64("job_id")?.filter(|id| *id >= previous_min_job_id) {
                minimum = Some(minimum.map_or(job_id, |current: i64| current.min(job_id)));
            }
        }
        Ok(minimum)
    }

    /// Pinned Go `systable.Manager.HasFlashbackClusterJob` query used by
    /// `jobsubmit.SubmitBatch`.
    ///
    /// Admission depends only on the indexed job ID and action columns. It
    /// must not decode unrelated `job_meta` values as the scheduler's full
    /// active-job load does.
    pub fn has_flashback_cluster_job<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        min_job_id: i64,
    ) -> Result<bool, DdlJobTableError> {
        let mut found = false;
        for (key, value) in scan_system_table_from_int_handle(snapshot, &self.view, min_job_id)? {
            let row = SystemRow::parse(&self.view, &key, &value)?;
            if row.i64("job_id")?.unwrap_or_default() >= min_job_id
                && row.i64("type")?.unwrap_or_default()
                    == i64::from(tidb_model::ActionType::ACTION_FLASHBACK_CLUSTER.0)
            {
                found = true;
            }
        }
        Ok(found)
    }

    /// Appends pinned Go `insertDDLJobs2Table` for one already-ID-assigned
    /// job. The insert assertion prevents a duplicate job ID from replacing a
    /// different operation.
    pub fn append_insert(
        &self,
        job: &mut Job,
        reorg: bool,
        schema_ids: &str,
        table_ids: &str,
        processing: bool,
        mutations: &mut Vec<OptimisticMutation>,
    ) -> Result<(), DdlJobTableError> {
        let encoded = job.encode(true)?;
        let mut values = RowValues::new();
        self.value(&mut values, "job_id", Datum::Int(job.id))?;
        self.value(&mut values, "reorg", Datum::Int(i64::from(reorg)))?;
        self.value(
            &mut values,
            "schema_ids",
            Datum::Bytes(schema_ids.as_bytes().to_vec()),
        )?;
        self.value(
            &mut values,
            "table_ids",
            Datum::Bytes(table_ids.as_bytes().to_vec()),
        )?;
        self.value(&mut values, "job_meta", Datum::Bytes(encoded))?;
        self.value(&mut values, "type", Datum::Int(i64::from(job.type_.0)))?;
        self.value(&mut values, "processing", Datum::Int(i64::from(processing)))?;
        mutations.extend(store_clustered_row(&self.table, None, &values)?);
        Ok(())
    }

    /// Appends pinned Go `updateDDLJob2Table`: only `job_meta` changes and all
    /// scheduling columns retain their submitted values.
    pub fn append_update(
        &self,
        active: &mut ActiveDdlJob,
        update_raw_args: bool,
        mutations: &mut Vec<OptimisticMutation>,
    ) -> Result<(), DdlJobTableError> {
        let encoded = active.job.encode(update_raw_args)?;
        let mut values = active.values.clone();
        self.value(&mut values, "job_meta", Datum::Bytes(encoded))?;
        mutations.extend(store_clustered_row(
            &self.table,
            Some(&active.values),
            &values,
        )?);
        active.values = values;
        Ok(())
    }

    /// Appends pinned Go `deleteDDLJob` for the active row just loaded by the
    /// worker. Ordinary table deletion asserts that observed record exists.
    pub fn append_delete(
        &self,
        active: &ActiveDdlJob,
        mutations: &mut Vec<OptimisticMutation>,
    ) -> Result<(), DdlJobTableError> {
        mutations.extend(delete_clustered_row(&self.table, &active.values)?);
        Ok(())
    }

    /// Stored `TableInfo`, exposed for table-shape and mutation tests.
    #[must_use]
    pub fn table(&self) -> &tidb_model::TableInfo {
        &self.table
    }
}
