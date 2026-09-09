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

//! The refresh schedule rows stored in `mysql.tidb_mview_refresh_info`.
//!
//! This is the storage half of pinned Go `pkg/ddl/mview_worker.go`'s
//! `prewriteCreateMaterializedViewRefreshInfo` /
//! `upsertCreateMaterializedViewRefreshInfo` /
//! `deleteCreateMaterializedViewRefreshInfo` trio over the clustered
//! `MVIEW_ID` primary key:
//!
//! * the create worker's phase 1 prewrites
//!   `(MVIEW_ID, LAST_SUCCESS_READ_TSO, NULL, NULL)` through the
//!   `should_update = false` shape, whose `ON DUPLICATE KEY UPDATE` arm
//!   rewrites only the read TSO;
//! * the post-build upsert records the build's read TS, the refresh end
//!   seconds and the next deadline through the full four-column shape;
//! * the rollback path deletes the row (a missing row is a no-op).

use std::fmt;

use tidb_datatype::Datum;
use tidb_model::TableInfo;
use tidb_txnkv::transaction::OptimisticMutation;

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::mysql_system_tables::{
    scan_system_table_prefixed, SystemRow, SystemTableError, SystemTableView,
};
use crate::system_row_write::{
    delete_clustered_row, store_clustered_row, RowEncodeError, RowValues,
};

/// One decoded refresh-schedule row plus the exact stored row values needed
/// by Go SQL `UPDATE`/`DELETE` semantics.
#[derive(Debug)]
pub struct MviewRefreshInfoRow {
    /// Go `MVIEW_ID`.
    pub mview_id: i64,
    /// Go `LAST_SUCCESS_READ_TSO`; `None` is SQL NULL.
    pub last_success_read_tso: Option<u64>,
    /// Go `LAST_SUCCESS_REFRESH_END_UNIX_SECONDS`; `None` is SQL NULL.
    pub last_success_refresh_end_unix_seconds: Option<i64>,
    /// Go `NEXT_REFRESH_UNIX_SECONDS`; `None` is SQL NULL.
    pub next_refresh_unix_seconds: Option<i64>,
    values: RowValues,
}

/// A malformed or inaccessible refresh-schedule row.
#[derive(Debug)]
pub enum MviewRefreshInfoTableError {
    /// The system table could not be located or decoded.
    Table(SystemTableError),
    /// A row could not be encoded according to its stored `TableInfo`.
    Row(RowEncodeError),
}

impl fmt::Display for MviewRefreshInfoTableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table(error) => write!(formatter, "{error}"),
            Self::Row(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for MviewRefreshInfoTableError {}

impl From<SystemTableError> for MviewRefreshInfoTableError {
    fn from(error: SystemTableError) -> Self {
        Self::Table(error)
    }
}

impl From<RowEncodeError> for MviewRefreshInfoTableError {
    fn from(error: RowEncodeError) -> Self {
        Self::Row(error)
    }
}

/// The stored table definition and projection used by the refresh rows.
#[derive(Clone, Debug)]
pub struct MviewRefreshInfoTable {
    table: Box<TableInfo>,
    view: SystemTableView,
}

impl MviewRefreshInfoTable {
    /// Locates the pinned refresh-info table in one loaded catalog. Go
    /// converts a missing `mysql.tidb_mview_refresh_info` into
    /// `ErrInvalidDDLJob("create materialized view: required system table
    /// mysql.tidb_mview_refresh_info does not exist")`, which rolls the job
    /// back; the caller owns that translation.
    pub fn locate(catalog: &ClusterCatalog) -> Result<Self, MviewRefreshInfoTableError> {
        let (_, table) = catalog
            .find_table("mysql", "tidb_mview_refresh_info")
            .ok_or_else(|| SystemTableError::Missing {
                name: "mysql.tidb_mview_refresh_info".to_owned(),
            })?;
        let table = table.clone_like_go();
        let view = SystemTableView::project(
            "mysql.tidb_mview_refresh_info",
            &table,
            &[
                "mview_id",
                "last_success_read_tso",
                "last_success_refresh_end_unix_seconds",
                "next_refresh_unix_seconds",
            ],
        );
        Ok(Self {
            table: Box::new(table),
            view,
        })
    }

    fn column_id(&self, name: &'static str) -> Result<i64, MviewRefreshInfoTableError> {
        self.table
            .cols()
            .iter_deref()
            .find(|column| column.read().name.lowercase() == name)
            .map(|column| column.read().id)
            .ok_or_else(|| {
                MviewRefreshInfoTableError::Table(SystemTableError::MissingColumn {
                    name: "mysql.tidb_mview_refresh_info".to_owned(),
                    column: name.to_owned(),
                })
            })
    }

    /// Scans the one row a view's clustered `MVIEW_ID` addresses.
    pub fn find<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        mview_id: i64,
    ) -> Result<Option<MviewRefreshInfoRow>, MviewRefreshInfoTableError> {
        for (key, value) in
            scan_system_table_prefixed(snapshot, &self.view, &[Datum::Int(mview_id)])?
        {
            let row = SystemRow::parse(&self.view, &key, &value)?;
            if row.i64("mview_id")? == Some(mview_id) {
                return Ok(Some(MviewRefreshInfoRow {
                    mview_id,
                    last_success_read_tso: row.u64("last_success_read_tso")?,
                    last_success_refresh_end_unix_seconds: row
                        .i64("last_success_refresh_end_unix_seconds")?,
                    next_refresh_unix_seconds: row.i64("next_refresh_unix_seconds")?,
                    values: row.into_values(),
                }));
            }
        }
        Ok(None)
    }

    /// Appends Go `buildCreateMaterializedViewRefreshInfoUpsertSQL`. The
    /// `should_update = false` shape writes only the read TSO (its
    /// `ON DUPLICATE KEY UPDATE` arm rewrites the read TSO and clears
    /// nothing else), while the full shape records every column.
    pub fn append_upsert(
        &self,
        mview_id: i64,
        read_ts: u64,
        last_success: Option<i64>,
        next: Option<i64>,
        should_update: bool,
        existing: Option<&MviewRefreshInfoRow>,
        mutations: &mut Vec<OptimisticMutation>,
    ) -> Result<(), MviewRefreshInfoTableError> {
        let mut values = match existing {
            Some(row) => row.values.clone(),
            None => {
                let mut values = RowValues::new();
                values.insert(self.column_id("mview_id")?, Datum::Int(mview_id));
                // SQL NULL for the columns the insert omits, spelled
                // explicitly because the clustered-row encoder refuses a
                // public column with no stored value.
                for column in [
                    "last_success_read_tso",
                    "last_success_refresh_end_unix_seconds",
                    "next_refresh_unix_seconds",
                ] {
                    values.entry(self.column_id(column)?).or_insert(Datum::Null);
                }
                values
            }
        };
        values.insert(
            self.column_id("last_success_read_tso")?,
            Datum::UInt(read_ts),
        );
        values.insert(
            self.column_id("last_success_refresh_end_unix_seconds")?,
            last_success.map_or(Datum::Null, Datum::Int),
        );
        if should_update {
            values.insert(
                self.column_id("next_refresh_unix_seconds")?,
                next.map_or(Datum::Null, Datum::Int),
            );
        }
        mutations.extend(store_clustered_row(
            &self.table,
            existing.map(|row| &row.values),
            &values,
        )?);
        Ok(())
    }

    /// Appends Go `deleteCreateMaterializedViewRefreshInfo`'s clustered-row
    /// deletion; a missing row is a no-op exactly as Go's own SQL DELETE
    /// affects nothing.
    pub fn append_delete(
        &self,
        row: &MviewRefreshInfoRow,
        mutations: &mut Vec<OptimisticMutation>,
    ) -> Result<(), MviewRefreshInfoTableError> {
        mutations.extend(delete_clustered_row(&self.table, &row.values)?);
        Ok(())
    }

    /// Stored `TableInfo`, exposed for table-shape and mutation tests.
    #[must_use]
    pub fn table(&self) -> &TableInfo {
        &self.table
    }
}
