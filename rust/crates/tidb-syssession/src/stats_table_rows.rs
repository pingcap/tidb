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

//! The `mysql.stats_*`, `mysql.analyze_*` and `mysql.column_stats_usage` rows
//! the statistics handle reads and writes. Go issues those as restricted SQL
//! on a system session; this is the same work done row by row on the
//! session's snapshot, shared by every statistics package that writes.

use std::collections::BTreeMap;

use tidb_datatype::Datum;
use tidb_model::table_info::TableInfo;
use tidb_txnkv::transaction::BufferMutation;

use tidb_infoschema::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::mysql_system_tables::{
    scan_system_table, scan_system_table_index_prefixed, scan_system_table_prefixed, HandleLayout,
    SystemRow, SystemTableError, SystemTableView, SYSTEM_DB,
};
use crate::system_row_write::{
    delete_clustered_row, delete_row, insert_row, rewrite_rowid_row,
    store_clustered_row, RowEncodeError, RowValues,
};

/// Why one table's statistics could not be stored.
#[derive(Debug)]
pub enum StatsWriteError {
    /// A `mysql.stats_*` table the cluster does not have.
    MissingTable(String),
    /// The current rows could not be read.
    Read(SystemTableError),
    /// A row could not be encoded.
    Encode(RowEncodeError),
    /// A histogram bound could not be converted to the blob the column stores.
    Bound(String),
    /// A loaded CMSketch could not be encoded for `stats_histograms`.
    Cms(String),
    /// A predicate-column timestamp could not be parsed as Go TIMESTAMP(6).
    PredicateTime(String),
    /// The exact statistics version needed for a history row is no longer current.
    HistoricalMeta(String),
    /// A historical statistics JSON object could not be compressed.
    HistoricalData(String),
    /// Updating a statistics row's table ID would collide with an existing row.
    DuplicateIdentity(String),
}

impl std::fmt::Display for StatsWriteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingTable(name) => write!(formatter, "this cluster has no `{name}`"),
            Self::Read(error) => write!(formatter, "{error}"),
            Self::Encode(error) => write!(formatter, "{error}"),
            Self::Bound(detail) => write!(formatter, "a histogram bound did not convert: {detail}"),
            Self::Cms(detail) => write!(formatter, "a CMSketch did not encode: {detail}"),
            Self::PredicateTime(detail) => {
                write!(
                    formatter,
                    "a predicate-column timestamp did not parse: {detail}"
                )
            }
            Self::HistoricalMeta(detail) => formatter.write_str(detail),
            Self::HistoricalData(detail) => formatter.write_str(detail),
            Self::DuplicateIdentity(detail) => formatter.write_str(detail),
        }
    }
}

impl std::error::Error for StatsWriteError {}

impl From<SystemTableError> for StatsWriteError {
    fn from(error: SystemTableError) -> Self {
        Self::Read(error)
    }
}

impl From<RowEncodeError> for StatsWriteError {
    fn from(error: RowEncodeError) -> Self {
        Self::Encode(error)
    }
}

/// The mutations that make one table's `mysql.stats_*` rows equal `stats`.
#[derive(Debug, Default)]
pub struct StatsWritePlan {
    /// Every mutation, to be committed on the snapshot's own transaction.
    pub mutations: Vec<BufferMutation>,
    /// How many histograms the plan stores, for the statement's log line.
    pub histogram_count: usize,
    /// How many buckets, across every histogram.
    pub bucket_count: usize,
    /// How many TopN entries, across every histogram.
    pub topn_count: usize,
    /// A newer Analyze v2 snapshot already won the table. Pinned Go treats
    /// this as a successful no-op and does not replace any histogram rows.
    pub skipped_newer_snapshot: bool,
}

impl StatsWritePlan {
    /// Whether the plan writes nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }
}

/// One `mysql.stats_*` table's current rows for one analyzed table, addressed
/// by the LOGICAL identity that names a row rather than by a physical handle.
///
/// # Why the identity, and not the key
///
/// The four statistics tables have changed handle shape across TiDB versions:
/// what is `PRIMARY KEY (table_id, is_index, hist_id) CLUSTERED` on a recent
/// server was `UNIQUE INDEX tbl(...)` over a `_tidb_rowid` on an older one,
/// and `mysql.stats_top_n` has never had a clustered handle at all. A writer
/// that derived a record key from a row's values would work on exactly one of
/// those shapes and silently store unaddressable rows on the others -- so
/// this reads what is there and matches on the columns that MEAN "this
/// histogram's third bucket", which every version agrees on.
///
/// # Two replace strategies, because there are two handle shapes
///
/// A clustered row's key is derived from its identity, so rewriting it is a
/// put over the same key. A `_tidb_rowid` row's handle is arbitrary, and any
/// secondary index over a column this write moves would be left pointing at
/// the old value -- so the old row is deleted whole and a new one inserted
/// under a freshly allocated handle. That is deliberately more churn than an
/// in-place update, and it is the only version-independent way to be correct.
pub struct StatsRows<'table> {
    pub table: &'table TableInfo,
    pub identity: &'static [&'static str],
    pub clustered: bool,
    /// Identity -> (record key, stored values), drained as rows are matched.
    pub existing: BTreeMap<Vec<String>, (Vec<u8>, RowValues)>,
}

impl<'table> StatsRows<'table> {
    pub fn open_all<S: MetaSnapshot>(
        snapshot: &mut S,
        table: &'table TableInfo,
        identity: &'static [&'static str],
    ) -> Result<Self, StatsWriteError> {
        let clustered = !matches!(HandleLayout::of(table), HandleLayout::RowId);
        let mut existing = BTreeMap::new();
        for (key, values) in read_rows_with_keys(snapshot, table, None)? {
            existing.insert(identity_of(table, identity, &values)?, (key, values));
        }
        Ok(Self {
            table,
            identity,
            clustered,
            existing,
        })
    }

    pub fn open<S: MetaSnapshot>(
        snapshot: &mut S,
        table: &'table TableInfo,
        identity: &'static [&'static str],
        table_id: i64,
    ) -> Result<Self, StatsWriteError> {
        let clustered = !matches!(HandleLayout::of(table), HandleLayout::RowId);
        let mut existing = BTreeMap::new();
        for (key, values) in read_rows_with_keys(snapshot, table, Some(table_id))? {
            existing.insert(identity_of(table, identity, &values)?, (key, values));
        }
        Ok(Self {
            table,
            identity,
            clustered,
            existing,
        })
    }

    pub fn store_row(
        &mut self,
        catalog: &ClusterCatalog,
        values: &RowValues,
        plan: &mut StatsWritePlan,
        row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
    ) -> Result<(), StatsWriteError> {
        let identity = identity_of(self.table, self.identity, values)?;
        let previous = self.existing.remove(&identity);
        if self.clustered {
            plan.mutations.extend(store_clustered_row(
                self.table,
                previous.as_ref().map(|(_, stored)| stored),
                values,
            )?);
            return Ok(());
        }
        // A row that is already there keeps its handle: see
        // [`rewrite_rowid_row`] for why a delete-and-reinsert would collide
        // with itself on any index whose columns did not move.
        if let Some((key, stored)) = previous {
            plan.mutations
                .extend(rewrite_rowid_row(self.table, &key, &stored, values)?);
            return Ok(());
        }
        let row_id = allocate_row_id(catalog, self.table, row_ids)?;
        plan.mutations
            .extend(insert_row(self.table, row_id, values)?);
        Ok(())
    }

    pub fn existing_values(&self, identity: &[String]) -> Option<&RowValues> {
        self.existing.get(identity).map(|(_, values)| values)
    }

    pub fn retract_remaining(&mut self, plan: &mut StatsWritePlan) -> Result<(), StatsWriteError> {
        for (key, stored) in std::mem::take(&mut self.existing).into_values() {
            if self.clustered {
                plan.mutations
                    .extend(delete_clustered_row(self.table, &stored)?);
            } else {
                plan.mutations
                    .extend(delete_row(self.table, &key, &stored)?);
            }
        }
        Ok(())
    }

    pub fn retract_prefix(
        &mut self,
        prefix: &[String],
        plan: &mut StatsWritePlan,
    ) -> Result<(), StatsWriteError> {
        let identities = self
            .existing
            .keys()
            .filter(|identity| identity.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        for identity in identities {
            let (key, stored) = self
                .existing
                .remove(&identity)
                .expect("identity came from the existing map");
            if self.clustered {
                plan.mutations
                    .extend(delete_clustered_row(self.table, &stored)?);
            } else {
                plan.mutations
                    .extend(delete_row(self.table, &key, &stored)?);
            }
        }
        Ok(())
    }
}

/// The values of the columns that name one statistics row, rendered so that
/// two rows compare equal exactly when they mean the same thing.
pub fn identity_of(
    table: &TableInfo,
    identity: &'static [&'static str],
    values: &RowValues,
) -> Result<Vec<String>, StatsWriteError> {
    identity
        .iter()
        .map(|name| {
            let id = column_id(table, name)?;
            Ok(format!("{:?}", values.get(&id).unwrap_or(&Datum::Null)))
        })
        .collect()
}

pub fn locate<'catalog>(
    catalog: &'catalog ClusterCatalog,
    name: &str,
) -> Result<&'catalog TableInfo, StatsWriteError> {
    catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == SYSTEM_DB)
        .and_then(|database| {
            database
                .tables
                .iter()
                .find(|stored| stored.name.lowercase() == name)
        })
        .ok_or_else(|| StatsWriteError::MissingTable(format!("{SYSTEM_DB}.{name}")))
}

pub fn system_db_id(catalog: &ClusterCatalog) -> Result<i64, StatsWriteError> {
    catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == SYSTEM_DB)
        .map(|database| database.info.id)
        .ok_or_else(|| StatsWriteError::MissingTable(SYSTEM_DB.to_owned()))
}

pub fn column_id(table: &TableInfo, name: &str) -> Result<i64, StatsWriteError> {
    table
        .find_public_column_by_name(name)
        .map(|column| column.read().id)
        .ok_or_else(|| {
            StatsWriteError::MissingTable(format!("{SYSTEM_DB}.{}.{name}", table.name.original()))
        })
}

pub fn row_i64(table: &TableInfo, values: &RowValues, name: &str) -> Option<i64> {
    let id = column_id(table, name).ok()?;
    match values.get(&id)? {
        Datum::Int(value) => Some(*value),
        Datum::UInt(value) => i64::try_from(*value).ok(),
        _ => None,
    }
}

pub fn row_u64(table: &TableInfo, values: &RowValues, name: &str) -> Option<u64> {
    let id = column_id(table, name).ok()?;
    match values.get(&id)? {
        Datum::UInt(value) => Some(*value),
        Datum::Int(value) => u64::try_from(*value).ok(),
        _ => None,
    }
}

/// Sets one column, if the cluster's table has it.
///
/// A column a newer TiDB added -- `last_stats_histograms_version` -- is
/// simply absent on an older cluster, and a writer that insisted on it could
/// not store statistics there at all.
pub fn set(table: &TableInfo, values: &mut RowValues, name: &str, value: Datum) {
    if let Ok(id) = column_id(table, name) {
        values.insert(id, value);
    }
}

pub fn full_view(table: &TableInfo) -> SystemTableView {
    let names: Vec<String> = table
        .cols()
        .iter_deref()
        .map(|column| column.read().name.lowercase().to_owned())
        .collect();
    let borrowed: Vec<&str> = names.iter().map(String::as_str).collect();
    SystemTableView::project(
        &format!("{SYSTEM_DB}.{}", table.name.original()),
        table,
        &borrowed,
    )
}

/// The table's stored rows for one analyzed table, each with the record key
/// it lives under.
///
/// `table_id` narrows the scan by the clustered prefix when the table has
/// one, and filters afterwards when it does not -- which is exactly the
/// situation `mysql.stats_top_n` is always in and the older statistics
/// schemas are in too.
pub fn read_rows_with_keys<S: MetaSnapshot>(
    snapshot: &mut S,
    table: &TableInfo,
    table_id: Option<i64>,
) -> Result<Vec<(Vec<u8>, RowValues)>, StatsWriteError> {
    let view = full_view(table);
    let pairs = match table_id {
        // A `_tidb_rowid` table's record keys do not start with `table_id`,
        // so a record prefix names the whole table; Go reaches one table's
        // rows (`... WHERE table_id = %?`) through the `tbl` index instead.
        // Scanning the whole of `mysql.stats_top_n` per item made LOAD STATS
        // quadratic in the number of loaded items.
        Some(id)
            if matches!(HandleLayout::of(table), HandleLayout::RowId) && view.has_index("tbl") =>
        {
            scan_system_table_index_prefixed(snapshot, &view, "tbl", &[Datum::Int(id)])?
        }
        Some(id) => scan_system_table_prefixed(snapshot, &view, &[Datum::Int(id)])?,
        None => scan_system_table(snapshot, &view)?,
    };
    decode_rows_with_keys(table, table_id, &view, pairs)
}

/// Reads all rows in one named secondary-index order. Go's historical GC
/// forces `idx_create_time`; using that same ordered access path is what makes
/// each bounded delete remove the oldest rows first without a Rust-only sort.
pub fn read_rows_with_keys_from_index<S: MetaSnapshot>(
    snapshot: &mut S,
    table: &TableInfo,
    index_name: &str,
) -> Result<Vec<(Vec<u8>, RowValues)>, StatsWriteError> {
    let view = full_view(table);
    let pairs = scan_system_table_index_prefixed(snapshot, &view, index_name, &[])?;
    decode_rows_with_keys(table, None, &view, pairs)
}

pub fn decode_rows_with_keys(
    table: &TableInfo,
    table_id: Option<i64>,
    view: &SystemTableView,
    pairs: tidb_infoschema::cluster_catalog::MetaPairs,
) -> Result<Vec<(Vec<u8>, RowValues)>, StatsWriteError> {
    let mut rows = Vec::new();
    for (key, value) in pairs {
        let timezone = tidb_datatype::SessionTimeZone::utc();
        let parsed = SystemRow::parse_in_timezone(view, &key, &value, Some(&timezone))?;
        let mut values = RowValues::new();
        for column in table.cols().iter_deref() {
            let (id, name) = {
                let column = column.read();
                (column.id, column.name.lowercase().to_owned())
            };
            let stored = parsed.datum(&name)?.cloned().unwrap_or(Datum::Null);
            values.insert(id, stored);
        }
        if let Some(id) = table_id {
            if values.get(&column_id(table, "table_id")?) != Some(&Datum::Int(id)) {
                continue;
            }
        }
        rows.push((key, values));
    }
    Ok(rows)
}

/// Go's internal INSERT allocation for a `mysql.*` row: the table's own
/// allocator (`_tidb_rowid` for a heap table, AUTO_INCREMENT otherwise),
/// whose reservation commits in its own transaction.
pub fn allocate_row_id(
    catalog: &ClusterCatalog,
    table: &TableInfo,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<i64, StatsWriteError> {
    let ids = row_ids.ok_or_else(|| {
        StatsWriteError::Encode(RowEncodeError(format!(
            "{} has no row-ID allocator",
            table.name.original()
        )))
    })?;
    let db_id = system_db_id(catalog)?;
    let allocator = if matches!(HandleLayout::of(table), HandleLayout::RowId) {
        ids.row_allocator_for(db_id, table)
    } else {
        ids.allocator_for(db_id, table)
    };
    let id = allocator.alloc_next().map_err(|error| {
        StatsWriteError::Encode(RowEncodeError(format!(
            "{}'s row-ID allocator: {error}",
            table.name.original()
        )))
    })?;
    i64::try_from(id).map_err(|_| {
        StatsWriteError::Encode(RowEncodeError(format!(
            "{}'s row-ID allocator overflowed",
            table.name.original()
        )))
    })
}
