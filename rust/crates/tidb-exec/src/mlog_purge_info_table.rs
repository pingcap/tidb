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

//! The purge schedule rows stored in `mysql.tidb_mlog_purge_info`.
//!
//! This is the storage half of pinned Go `pkg/ddl/mview_worker.go`'s
//! `upsertCreateMaterializedViewLogPurgeInfo` /
//! `deleteMaterializedViewLogPurgeInfo` pair: the create-log worker writes
//! one row per materialized view log carrying its next purge deadline, and
//! the rollback path deletes it. Go reaches both through SQL `INSERT
//! IGNORE` / `INSERT .. ON DUPLICATE KEY UPDATE` / `DELETE` on the clustered
//! `MLOG_ID` primary key, which is exactly the row-store contract below.
//!
//! The row's `NEXT_PURGE_UNIX_SECONDS` value comes from evaluating the log's
//! schedule expressions through the owner's session (Go's
//! `deriveCreateMaterializedViewLogNextUnixSeconds`); this module carries
//! only the persisted outcome.

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

/// One decoded purge-schedule row together with the exact stored row values
/// needed by Go SQL `UPDATE`/`DELETE` semantics.
#[derive(Debug)]
pub struct MlogPurgeInfoRow {
    /// Go `MLOG_ID`.
    pub mlog_id: i64,
    /// Go `NEXT_PURGE_UNIX_SECONDS`; `None` is SQL NULL.
    pub next_purge_unix_seconds: Option<i64>,
    /// Go `LAST_PURGED_TSO`; `None` is SQL NULL.
    pub last_purged_tso: Option<u64>,
    values: RowValues,
}

/// A malformed or inaccessible purge-schedule row.
#[derive(Debug)]
pub enum MlogPurgeInfoTableError {
    /// The system table could not be located or decoded.
    Table(SystemTableError),
    /// A row could not be encoded according to its stored `TableInfo`.
    Row(RowEncodeError),
}

impl fmt::Display for MlogPurgeInfoTableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table(error) => write!(formatter, "{error}"),
            Self::Row(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for MlogPurgeInfoTableError {}

impl From<SystemTableError> for MlogPurgeInfoTableError {
    fn from(error: SystemTableError) -> Self {
        Self::Table(error)
    }
}

impl From<RowEncodeError> for MlogPurgeInfoTableError {
    fn from(error: RowEncodeError) -> Self {
        Self::Row(error)
    }
}

/// The stored table definition and projection used by the purge queue.
#[derive(Clone, Debug)]
pub struct MlogPurgeInfoTable {
    table: Box<TableInfo>,
    view: SystemTableView,
}

impl MlogPurgeInfoTable {
    /// Locates the pinned purge-schedule table in one loaded catalog.
    ///
    /// Go converts a missing `mysql.tidb_mlog_purge_info` into
    /// `ErrInvalidDDLJob("create materialized view log: required system
    /// table mysql.tidb_mlog_purge_info does not exist")`, which rolls the
    /// job back; the caller owns that translation.
    pub fn locate(catalog: &ClusterCatalog) -> Result<Self, MlogPurgeInfoTableError> {
        let (_, table) = catalog
            .find_table("mysql", "tidb_mlog_purge_info")
            .ok_or_else(|| SystemTableError::Missing {
                name: "mysql.tidb_mlog_purge_info".to_owned(),
            })?;
        let table = table.clone_like_go();
        let view = SystemTableView::project(
            "mysql.tidb_mlog_purge_info",
            &table,
            &["mlog_id", "next_purge_unix_seconds", "last_purged_tso"],
        );
        Ok(Self {
            table: Box::new(table),
            view,
        })
    }

    fn column_id(&self, name: &'static str) -> Result<i64, MlogPurgeInfoTableError> {
        self.table
            .cols()
            .iter_deref()
            .find(|column| column.read().name.lowercase() == name)
            .map(|column| column.read().id)
            .ok_or_else(|| {
                MlogPurgeInfoTableError::Table(SystemTableError::MissingColumn {
                    name: "mysql.tidb_mlog_purge_info".to_owned(),
                    column: name.to_owned(),
                })
            })
    }

    /// Scans the one row a log's clustered `MLOG_ID` addresses.
    pub fn find<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        mlog_id: i64,
    ) -> Result<Option<MlogPurgeInfoRow>, MlogPurgeInfoTableError> {
        for (key, value) in
            scan_system_table_prefixed(snapshot, &self.view, &[Datum::Int(mlog_id)])?
        {
            let row = SystemRow::parse(&self.view, &key, &value)?;
            if row.i64("mlog_id")? == Some(mlog_id) {
                return Ok(Some(MlogPurgeInfoRow {
                    mlog_id,
                    next_purge_unix_seconds: row.i64("next_purge_unix_seconds")?,
                    last_purged_tso: row.u64("last_purged_tso")?,
                    values: row.into_values(),
                }));
            }
        }
        Ok(None)
    }

    /// Appends Go `buildCreateMaterializedViewLogPurgeInfoUpsertSQL`: the
    /// `should_update` form writes (or rewrites) the next-purge deadline,
    /// the `INSERT IGNORE` form records only the log ID and leaves an
    /// existing row untouched. A fresh log create never has a row yet, so
    /// the upsert's `ON DUPLICATE KEY UPDATE` arm cannot fire on this path.
    pub fn append_upsert(
        &self,
        mlog_id: i64,
        derived: MlogPurgeDerived,
        existing: Option<&MlogPurgeInfoRow>,
        mutations: &mut Vec<OptimisticMutation>,
    ) -> Result<(), MlogPurgeInfoTableError> {
        if !derived.should_update {
            // Go `INSERT IGNORE INTO .. (MLOG_ID) VALUES (..)`: an existing
            // row is left exactly as it is.
            if existing.is_some() {
                return Ok(());
            }
            let mut values = RowValues::new();
            self.insert_id(&mut values, mlog_id)?;
            self.insert_absent_columns(&mut values)?;
            mutations.extend(store_clustered_row(&self.table, None, &values)?);
            return Ok(());
        }
        let mut values = match existing {
            Some(row) => row.values.clone(),
            None => {
                let mut values = RowValues::new();
                self.insert_id(&mut values, mlog_id)?;
                self.insert_absent_columns(&mut values)?;
                values
            }
        };
        values.insert(
            self.column_id("next_purge_unix_seconds")?,
            derived.next_unix_seconds.map_or(Datum::Null, Datum::Int),
        );
        mutations.extend(store_clustered_row(
            &self.table,
            existing.map(|row| &row.values),
            &values,
        )?);
        Ok(())
    }

    /// Appends Go `deleteMaterializedViewLogPurgeInfo`'s clustered-row
    /// deletion. Go swallows a missing system table there and treats a
    /// missing ROW as a no-op, so an absent row appends nothing.
    pub fn append_delete(
        &self,
        row: &MlogPurgeInfoRow,
        mutations: &mut Vec<OptimisticMutation>,
    ) -> Result<(), MlogPurgeInfoTableError> {
        mutations.extend(delete_clustered_row(&self.table, &row.values)?);
        Ok(())
    }

    fn insert_id(
        &self,
        values: &mut RowValues,
        mlog_id: i64,
    ) -> Result<(), MlogPurgeInfoTableError> {
        values.insert(self.column_id("mlog_id")?, Datum::Int(mlog_id));
        Ok(())
    }

    /// SQL NULL for every non-key column the caller did not supply, spelled
    /// explicitly because the clustered-row encoder refuses a public column
    /// with no stored value.
    fn insert_absent_columns(&self, values: &mut RowValues) -> Result<(), MlogPurgeInfoTableError> {
        for column in ["next_purge_unix_seconds", "last_purged_tso"] {
            values.entry(self.column_id(column)?).or_insert(Datum::Null);
        }
        Ok(())
    }

    /// Stored `TableInfo`, exposed for table-shape and mutation tests.
    #[must_use]
    pub fn table(&self) -> &TableInfo {
        &self.table
    }
}

/// Go's `deriveCreateMaterializedViewLogNextUnixSeconds` outcome: the next
/// purge deadline the owner's session evaluated from the log's schedule,
/// and whether the persisted deadline should be overwritten.
///
/// `(None, true)` is Go's INSERT IGNORE path — no usable schedule (or a
/// schedule that evaluated to NULL) leaves the deadline SQL NULL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MlogPurgeDerived {
    /// The evaluated `NEXT_PURGE_UNIX_SECONDS`, when one applies.
    pub next_unix_seconds: Option<i64>,
    /// Go `shouldUpdate`.
    pub should_update: bool,
}

impl MlogPurgeDerived {
    /// The derivation for a log whose `PurgeStartWith`/`PurgeNext` are both
    /// empty: Go returns `(nil, true)` without touching the session.
    #[must_use]
    pub const fn unscheduled() -> Self {
        Self {
            next_unix_seconds: None,
            should_update: true,
        }
    }
}

impl MlogPurgeDerived {
    /// Go `deriveCreateMaterializedViewLogNextUnixSeconds`, evaluated the
    /// same way the DDL owner evaluates it — through SQL — but here via the
    /// driver's FROM-less SELECT over an empty catalog: the schedule
    /// expressions are constant for the log create's admitted grammar, so
    /// `SELECT NOW(6)` and `SELECT <expr>` carry the whole derivation,
    /// evaluated under the log's recorded SQL mode and schedule zone
    /// (Go's `setCreateMaterializedViewScheduleEvalSession`).
    ///
    /// Go's decision tree (`deriveCreateMaterializedScheduleNextUnixSeconds`):
    /// both expressions empty yields `(None, true)` with no evaluation; a
    /// START WITH in the future by more than ten seconds wins over NEXT;
    /// otherwise NEXT decides; any NULL evaluation logs and degrades to the
    /// `INSERT IGNORE` shape `(None, true)`.
    pub fn derive(
        log_meta: &tidb_model::MaterializedViewLogInfo,
        context: &tidb_executor::StmtContext,
    ) -> Result<Self, String> {
        use tidb_datatype::SessionTimeZone;

        let start_expr = log_meta.purge_start_with.trim();
        let next_expr = log_meta.purge_next.trim();
        if start_expr.is_empty() && next_expr.is_empty() {
            return Ok(Self::unscheduled());
        }

        // Go `setCreateMaterializedViewScheduleEvalSession` + `GetLocation`:
        // the expressions evaluate under the recorded SQL mode and schedule
        // zone.
        let zone = log_meta
            .purge_schedule_time_zone
            .get_location()
            .map_err(|error| format!("purge schedule zone: {error}"))?
            .read()
            .clone();
        let session_zone = match &zone {
            tidb_model::ResolvedTimeZone::Local => SessionTimeZone::Local,
            tidb_model::ResolvedTimeZone::Named(zone) => SessionTimeZone::Named(*zone),
            tidb_model::ResolvedTimeZone::Fixed { offset_seconds, .. } => SessionTimeZone::Fixed {
                name: log_meta.purge_schedule_time_zone.name.to_utf8_lossy_go(),
                offset_secs: i32::try_from(*offset_seconds)
                    .map_err(|_| "purge schedule zone offset overflows i32".to_owned())?,
            },
        };
        // Go's owner session carries a live clock, so `NOW(6)` reads the
        // tick's wall time through the lazy statement clock.
        let eval_context = context
            .clone()
            .with_lazy_clock(None, session_zone)
            .with_ddl_sql_mode(i64::try_from(log_meta.definition_sql_mode).unwrap_or_default());
        let empty_catalog = tidb_executor::Catalog::default();
        let evaluate = |expr: &str| -> Result<Option<ScheduleEvaluation>, String> {
            let sql = format!("SELECT {expr}");
            let (columns, rows) = tidb_executor::run_select_meta_in(
                &sql,
                &empty_catalog,
                tidb_executor::DEFAULT_DATABASE,
                &eval_context,
            )
            .map_err(|error| error.to_string())?;
            if columns.len() != 1 {
                return Err(format!(
                    "the schedule expression must evaluate to one column, got {}",
                    columns.len()
                ));
            }
            let Some(row) = rows.first() else {
                return Err("the schedule expression evaluated to no row".to_owned());
            };
            Ok(match &row[0] {
                tidb_datatype::Datum::Null => None,
                tidb_datatype::Datum::Time(time) => Some(ScheduleEvaluation {
                    time: *time,
                    is_datetime: columns[0].1.code() == tidb_datatype::FieldTypeCode::Datetime,
                }),
                other => {
                    return Err(format!(
                        "the schedule expression must return DATETIME/TIMESTAMP, got {other:?}"
                    ))
                }
            })
        };

        // Go `loadCreateMaterializedViewScheduleNow`.
        let now = evaluate("NOW(6)")?
            .ok_or("SELECT NOW(6) evaluated to NULL")?
            .time;

        let to_unix = |time: &ScheduleEvaluation| -> Result<i64, String> {
            let core = time.time.core_time();
            let unix = match &zone {
                tidb_model::ResolvedTimeZone::Local => core
                    .to_datetime(&chrono::Local)
                    .map(|datetime| datetime.timestamp()),
                tidb_model::ResolvedTimeZone::Named(zone) => {
                    core.to_datetime(zone).map(|datetime| datetime.timestamp())
                }
                tidb_model::ResolvedTimeZone::Fixed { offset_seconds, .. } => {
                    let offset =
                        chrono::FixedOffset::east_opt(i32::try_from(*offset_seconds).unwrap_or(0))
                            .ok_or("invalid schedule time zone offset")?;
                    core.to_datetime(&offset)
                        .map(|datetime| datetime.timestamp())
                }
            };
            unix.map_err(|error| error.to_string())
        };

        // Go: the near-now threshold is now + 10s in the schedule zone.
        let threshold = {
            let threshold_core = now.core_time().add_duration(10_000_000_000);
            tidb_datatype::Time::new(threshold_core, now.kind(), i64::from(now.fsp()))
                .map_err(|error| format!("near-now threshold: {error}"))?
        };

        if !start_expr.is_empty() {
            let Some(start_at) = evaluate(start_expr)? else {
                tidb_executor::ddl::mview_schedule_expr::log_create_materialized_view_log_next_unix_seconds_update_null(
                    "", "", "START WITH", start_expr, next_expr,
                );
                return Ok(Self {
                    next_unix_seconds: None,
                    should_update: true,
                });
            };
            let decide = |time: &ScheduleEvaluation| {
                to_unix(time).map(|next_unix_seconds| Self {
                    next_unix_seconds: Some(next_unix_seconds),
                    should_update: true,
                })
            };
            if next_expr.is_empty() {
                return decide(&start_at);
            }
            if start_at.time.compare(threshold) == std::cmp::Ordering::Less {
                let Some(next_at) = evaluate(next_expr)? else {
                    tidb_executor::ddl::mview_schedule_expr::log_create_materialized_view_log_next_unix_seconds_update_null(
                        "", "", "NEXT", start_expr, next_expr,
                    );
                    return Ok(Self {
                        next_unix_seconds: None,
                        should_update: true,
                    });
                };
                return decide(&next_at);
            }
            return decide(&start_at);
        }

        let Some(next_at) = evaluate(next_expr)? else {
            tidb_executor::ddl::mview_schedule_expr::log_create_materialized_view_log_next_unix_seconds_update_null(
                "", "", "NEXT", start_expr, next_expr,
            );
            return Ok(Self {
                next_unix_seconds: None,
                should_update: true,
            });
        };
        let core = next_at.time.core_time();
        let unix = match &zone {
            tidb_model::ResolvedTimeZone::Local => core
                .to_datetime(&chrono::Local)
                .map(|datetime| datetime.timestamp()),
            tidb_model::ResolvedTimeZone::Named(zone) => {
                core.to_datetime(zone).map(|datetime| datetime.timestamp())
            }
            tidb_model::ResolvedTimeZone::Fixed { offset_seconds, .. } => {
                let offset =
                    chrono::FixedOffset::east_opt(i32::try_from(*offset_seconds).unwrap_or(0))
                        .ok_or("invalid schedule time zone offset")?;
                core.to_datetime(&offset)
                    .map(|datetime| datetime.timestamp())
            }
        };
        unix.map(|next_unix_seconds| Self {
            next_unix_seconds: Some(next_unix_seconds),
            should_update: true,
        })
        .map_err(|error| error.to_string())
    }
}

/// One evaluated schedule expression: the native time plus whether the
/// expression's type was DATETIME (Go's `evalCreateMaterializedView
/// ScheduleExprToDatetime` converts to `TypeDatetime`; the persisted
/// deadline does not depend on that conversion, only on the value).
struct ScheduleEvaluation {
    time: tidb_datatype::Time,
    #[allow(dead_code)]
    is_datetime: bool,
}
