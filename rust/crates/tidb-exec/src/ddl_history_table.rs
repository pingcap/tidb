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

//! Go's two DDL-history writes performed by `AddHistoryDDLJob`.
//!
//! A terminal job is encoded once, then inserted into
//! `mysql.tidb_ddl_history` and into the `DDLJobHistory` meta hash in the same
//! transaction that removes it from `mysql.tidb_ddl_job`. The SQL-table half
//! is `INSERT IGNORE` and best-effort in Go; the caller owns the authoritative
//! meta-history write.

use std::fmt;

use chrono::{Datelike, Local, TimeZone, Timelike, Utc};
use tidb_datatype::{Datum, Time, TimeType};
use tidb_model::Job;
use tidb_txnkv::transaction::OptimisticMutation;

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::mysql_system_tables::{scan_system_table, SystemRow, SystemTableError, SystemTableView};
use crate::system_row_write::{store_clustered_row, RowEncodeError, RowValues};

/// The stored history-table definition.
#[derive(Clone, Debug)]
pub struct DdlHistoryTable {
    table: Box<tidb_model::TableInfo>,
    view: SystemTableView,
}

/// A malformed or inaccessible DDL-history write.
#[derive(Debug)]
pub enum DdlHistoryTableError {
    /// The system table could not be located.
    Table(SystemTableError),
    /// A row could not be encoded according to its stored `TableInfo`.
    Row(RowEncodeError),
    /// The job could not be encoded in Go's persisted JSON envelope.
    Job(serde_json::Error),
    /// The job start timestamp is outside the representable SQL DATETIME range.
    StartTime(u64),
    /// A required history column is SQL NULL.
    MissingColumn(&'static str),
}

impl fmt::Display for DdlHistoryTableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table(error) => write!(formatter, "{error}"),
            Self::Row(error) => write!(formatter, "{error}"),
            Self::Job(error) => write!(formatter, "DDL job metadata is invalid: {error}"),
            Self::StartTime(timestamp) => {
                write!(
                    formatter,
                    "DDL job start timestamp {timestamp} is out of range"
                )
            }
            Self::MissingColumn(column) => {
                write!(formatter, "mysql.tidb_ddl_history.`{column}` is NULL")
            }
        }
    }
}

impl std::error::Error for DdlHistoryTableError {}

impl From<SystemTableError> for DdlHistoryTableError {
    fn from(error: SystemTableError) -> Self {
        Self::Table(error)
    }
}

impl From<RowEncodeError> for DdlHistoryTableError {
    fn from(error: RowEncodeError) -> Self {
        Self::Row(error)
    }
}

impl From<serde_json::Error> for DdlHistoryTableError {
    fn from(error: serde_json::Error) -> Self {
        Self::Job(error)
    }
}

impl DdlHistoryTable {
    /// Locates `mysql.tidb_ddl_history` in one loaded catalog.
    pub fn locate(catalog: &ClusterCatalog) -> Result<Self, DdlHistoryTableError> {
        let (_, table) = catalog
            .find_table("mysql", "tidb_ddl_history")
            .ok_or_else(|| SystemTableError::Missing {
                name: "mysql.tidb_ddl_history".to_owned(),
            })?;
        let table = table.clone_like_go();
        let view = SystemTableView::project(
            "mysql.tidb_ddl_history",
            &table,
            &[
                "job_id",
                "job_meta",
                "db_name",
                "table_name",
                "schema_ids",
                "table_ids",
                "create_time",
            ],
        );
        Ok(Self {
            table: Box::new(table),
            view,
        })
    }

    fn column_id(&self, name: &'static str) -> Result<i64, DdlHistoryTableError> {
        self.table
            .cols()
            .iter_deref()
            .find(|column| column.read().name.lowercase() == name)
            .map(|column| column.read().id)
            .ok_or_else(|| {
                DdlHistoryTableError::Table(SystemTableError::MissingColumn {
                    name: self.view.name().to_owned(),
                    column: name.to_owned(),
                })
            })
    }

    fn value(
        &self,
        values: &mut RowValues,
        column: &'static str,
        datum: Datum,
    ) -> Result<(), DdlHistoryTableError> {
        values.insert(self.column_id(column)?, datum);
        Ok(())
    }

    /// Loads history jobs in clustered `job_id` order.
    pub fn load<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
    ) -> Result<Vec<Job>, DdlHistoryTableError> {
        scan_system_table(snapshot, &self.view)?
            .into_iter()
            .map(|(key, value)| {
                let row = SystemRow::parse(&self.view, &key, &value)?;
                let encoded = row
                    .bytes("job_meta")?
                    .ok_or(DdlHistoryTableError::MissingColumn("job_meta"))?;
                let mut job = Job::default();
                job.decode(&encoded)?;
                Ok(job)
            })
            .collect()
    }

    /// Appends Go's best-effort SQL `INSERT IGNORE` history write.
    pub fn append_insert_ignore<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        job: &Job,
        encoded: &[u8],
        mutations: &mut Vec<OptimisticMutation>,
    ) -> Result<(), DdlHistoryTableError> {
        let already_exists = scan_system_table(snapshot, &self.view)?
            .into_iter()
            .map(|(key, value)| {
                let row = SystemRow::parse(&self.view, &key, &value)?;
                Ok(row.i64("job_id")? == Some(job.id))
            })
            .collect::<Result<Vec<_>, SystemTableError>>()?
            .into_iter()
            .any(|matches| matches);
        if already_exists {
            return Ok(());
        }
        let create_time = tso_datetime(job.start_ts)?;
        let mut values = RowValues::new();
        self.value(&mut values, "job_id", Datum::Int(job.id))?;
        self.value(&mut values, "job_meta", Datum::Bytes(encoded.to_vec()))?;
        self.value(
            &mut values,
            "db_name",
            Datum::Bytes(job.schema_name.as_bytes().to_vec()),
        )?;
        self.value(
            &mut values,
            "table_name",
            Datum::Bytes(job.table_name.as_bytes().to_vec()),
        )?;
        self.value(
            &mut values,
            "schema_ids",
            Datum::Bytes(job.schema_id.to_string().into_bytes()),
        )?;
        self.value(
            &mut values,
            "table_ids",
            Datum::Bytes(job.table_id.to_string().into_bytes()),
        )?;
        self.value(&mut values, "create_time", Datum::Time(create_time))?;
        mutations.extend(store_clustered_row(&self.table, None, &values)?);
        Ok(())
    }
}

fn tso_datetime(timestamp: u64) -> Result<Time, DdlHistoryTableError> {
    let milliseconds =
        i64::try_from(timestamp >> 18).map_err(|_| DdlHistoryTableError::StartTime(timestamp))?;
    let utc = Utc
        .timestamp_millis_opt(milliseconds)
        .single()
        .ok_or(DdlHistoryTableError::StartTime(timestamp))?;
    let local = utc.with_timezone(&Local);
    Time::from_date_checked(
        local.year(),
        i32::try_from(local.month()).expect("month fits i32"),
        i32::try_from(local.day()).expect("day fits i32"),
        i32::try_from(local.hour()).expect("hour fits i32"),
        i32::try_from(local.minute()).expect("minute fits i32"),
        i32::try_from(local.second()).expect("second fits i32"),
        i32::try_from(local.nanosecond() / 1_000).expect("microsecond fits i32"),
        TimeType::DateTime,
        0,
    )
    .map_err(|_| DdlHistoryTableError::StartTime(timestamp))
}
