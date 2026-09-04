// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Inc. 2.0 (the "License");
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

//! The refresh-alert rows stored in `mysql.tidb_mview_refresh_alert`.
//!
//! This is the storage half of pinned Go `pkg/ddl/mview_worker.go`'s
//! `deleteCreateMaterializedViewRefreshAlert` /
//! `buildDeleteMViewRefreshAlertSQL`: the create rollback removes the view's
//! alert row, and a missing row is a no-op exactly as Go's SQL DELETE
//! affects nothing.

use std::fmt;

use tidb_datatype::Datum;
use tidb_model::TableInfo;
use tidb_txnkv::transaction::OptimisticMutation;

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::mysql_system_tables::{
    scan_system_table_prefixed, SystemRow, SystemTableError, SystemTableView,
};
use crate::system_row_write::{delete_clustered_row, RowEncodeError, RowValues};

/// A malformed or inaccessible refresh-alert row.
#[derive(Debug)]
pub enum MviewAlertTableError {
    /// The system table could not be located or decoded.
    Table(SystemTableError),
    /// A row could not be encoded according to its stored `TableInfo`.
    Row(RowEncodeError),
}

impl fmt::Display for MviewAlertTableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table(error) => write!(formatter, "{error}"),
            Self::Row(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for MviewAlertTableError {}

impl From<SystemTableError> for MviewAlertTableError {
    fn from(error: SystemTableError) -> Self {
        Self::Table(error)
    }
}

impl From<RowEncodeError> for MviewAlertTableError {
    fn from(error: RowEncodeError) -> Self {
        Self::Row(error)
    }
}

/// The stored table definition and projection used by the alert rows.
#[derive(Clone, Debug)]
pub struct MviewAlertTable {
    table: Box<TableInfo>,
    view: SystemTableView,
}

/// One decoded alert row plus the exact stored row values needed by Go SQL
/// `DELETE` semantics.
#[derive(Debug)]
pub struct MviewAlertRow {
    /// Go `MVIEW_ID`.
    pub mview_id: i64,
    values: RowValues,
}

impl MviewAlertTable {
    /// Locates the pinned refresh-alert table in one loaded catalog. Go
    /// swallows a missing system table in
    /// `deleteCreateMaterializedViewRefreshAlert`; the caller owns that
    /// translation.
    pub fn locate(catalog: &ClusterCatalog) -> Result<Self, MviewAlertTableError> {
        let (_, table) = catalog
            .find_table("mysql", "tidb_mview_refresh_alert")
            .ok_or_else(|| SystemTableError::Missing {
                name: "mysql.tidb_mview_refresh_alert".to_owned(),
            })?;
        let table = table.clone_like_go();
        let view =
            SystemTableView::project("mysql.tidb_mview_refresh_alert", &table, &["mview_id"]);
        Ok(Self {
            table: Box::new(table),
            view,
        })
    }

    /// Scans the one row a view's clustered `MVIEW_ID` addresses.
    pub fn find<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        mview_id: i64,
    ) -> Result<Option<MviewAlertRow>, MviewAlertTableError> {
        for (key, value) in
            scan_system_table_prefixed(snapshot, &self.view, &[Datum::Int(mview_id)])?
        {
            let row = SystemRow::parse(&self.view, &key, &value)?;
            if row.i64("mview_id")? == Some(mview_id) {
                return Ok(Some(MviewAlertRow {
                    mview_id,
                    values: row.into_values(),
                }));
            }
        }
        Ok(None)
    }

    /// Appends Go `buildDeleteMViewRefreshAlertSQL`'s clustered-row
    /// deletion; an absent row appends nothing.
    pub fn append_delete(
        &self,
        row: &MviewAlertRow,
        mutations: &mut Vec<OptimisticMutation>,
    ) -> Result<(), MviewAlertTableError> {
        mutations.extend(delete_clustered_row(&self.table, &row.values)?);
        Ok(())
    }
}
