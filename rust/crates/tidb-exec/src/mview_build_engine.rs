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

//! The `CREATE MATERIALIZED VIEW` initial build executed against a
//! [`MetaSnapshot`](crate::cluster_catalog::MetaSnapshot).
//!
//! Go moves the base rows into the view in the create job's
//! `StateWriteReorganization` phase (`buildCreateMaterializedViewData`,
//! `pkg/ddl/mview_worker.go`) by executing the definition as an
//! `INSERT INTO`/`REPLACE INTO ... SELECT` (or `IMPORT INTO` on TiKV stores)
//! through a reorg session. This module is that data movement for the pure
//! tier: the definition `SELECT` runs over the base rows loaded from the
//! snapshot into a driver catalog, and each output row is encoded back
//! through the view's stored `TableInfo` into the same record + index
//! mutations `store_clustered_row`/`insert_row` produce, so the build lands
//! in the same transaction as the completion bookkeeping.
//!
//! Two Go behaviors are stated rather than re-implemented:
//!
//! * Go's `REPLACE INTO` would delete colliding rows before inserting; the
//!   initial build's destination is provably empty (the phase's own residual
//!   probe refuses otherwise), so plain `INSERT` semantics are identical.
//! * Go's import path (`IMPORT INTO`, TiKV stores) stays a real-store seam;
//!   the pure tier always takes Go's insert-select arm.

use tidb_codec::table_key::get_table_handle_key_range;
use tidb_executor::storage::{MemTableStorage, TableStorage};
use tidb_executor::{Catalog, KvColumn, KvTable, StmtContext};
use tidb_meta::{key, value};
use tidb_model::table_info::TableInfo;
use tidb_txnkv::transaction::OptimisticMutation;

use crate::cluster_catalog::MetaSnapshot;
use crate::cluster_ddl::{DdlAdmissionError, DdlPlanError};
use crate::mysql_system_tables::HandleLayout;
use crate::system_row_write::{insert_row, store_clustered_row, RowValues};

/// Go `mysql.ModeStrictTransTables` / `mysql.ModeStrictAllTables`: the two
/// bits whose presence decides a session's strictness. Carried locally
/// because this tier reads only the strictness out of the persisted
/// definition mode.
const GO_MODE_STRICT_TRANS_TABLES: i64 = 1 << 21;
/// See [`GO_MODE_STRICT_TRANS_TABLES`].
const GO_MODE_STRICT_ALL_TABLES: i64 = 1 << 22;

/// The outcome of one executed initial build.
#[derive(Clone, Debug)]
pub struct MviewBuildPlan {
    /// The snapshot the build read the base rows at — Go's
    /// `job.SnapshotVer` for the same phase. The pure tier reads its own
    /// transaction's snapshot, so this is the step's timestamp.
    pub read_ts: u64,
    /// How many view rows the build wrote (Go answers the same count from
    /// the import/insert's affected-rows summary).
    pub row_count: u64,
    /// The view's record + index mutations and, for a row-id view, the
    /// allocator watermark. Merged into the phase's transaction.
    pub mutations: Vec<OptimisticMutation>,
}

/// Executes Go's initial build (`buildCreateMaterializedViewData`) against
/// `snapshot` and returns the mutations that land the view rows.
///
/// The sequence mirrors the phase's Go order: the residual-build-rows probe
/// (`hasCreateMaterializedViewBuildRows` + the `ErrInvalidDDLJob` it raises),
/// the definition SELECT over every base table's stored rows, then one
/// encoded row write per output row.
pub fn derive_materialized_view_build<S: MetaSnapshot>(
    snapshot: &mut S,
    schema_name: &str,
    schema_id: i64,
    view: &TableInfo,
    bases: &[TableInfo],
    start_ts: u64,
) -> Result<MviewBuildPlan, DdlPlanError> {
    let Some(view_meta) = view.materialized_view.as_ref() else {
        return Err(DdlPlanError::Encode(
            "create materialized view: invalid select sql".to_owned(),
        ));
    };
    let sql_content = view_meta.read().sql_content.clone();
    if sql_content.is_empty() {
        return Err(DdlPlanError::Encode(
            "create materialized view: invalid select sql".to_owned(),
        ));
    }

    // Go probes `SELECT 1 FROM <view> LIMIT 1` when no live reorg context
    // exists — which is every tick on this tier — and refuses to build over
    // rows a crashed prior attempt left behind.
    let (view_start, view_end) = get_table_handle_key_range(view.id);
    let residual = snapshot
        .scan_range(&view_start, &view_end)
        .map_err(DdlPlanError::from)?;
    if !residual.is_empty() {
        return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
            tidb_error::tidb::errcode::ErrInvalidDDLJob,
            "create materialized view: detected residual build rows on retry",
        )));
    }

    // The definition runs over a world that holds exactly the base tables —
    // the same single-table admission the create enforced — with the rows
    // the snapshot already stores. A plain byte pre-load is enough: the
    // driver's SELECT decodes row values through the base's own columns.
    let mut catalog = Catalog::default();
    catalog.create_database(schema_name);
    for base in bases {
        let mut kv_columns: Vec<KvColumn> = Vec::new();
        let mut storage = MemTableStorage::new();
        for column in base.columns.iter_deref() {
            let column = column.read();
            kv_columns.push(KvColumn {
                name: column.name.original().to_owned(),
                id: column.id,
                field_type: column.field_type.clone(),
                column_info_version: column.version,
                default_value: None,
                origin_default: None,
                comment: column.comment.clone(),
                generated: None,
            });
        }
        let (start, end) = get_table_handle_key_range(base.id);
        for (key, value) in snapshot
            .scan_range(&start, &end)
            .map_err(DdlPlanError::from)?
        {
            storage
                .set(tidb_txnkv::Key::from_bytes(key), value)
                .map_err(|error| DdlPlanError::Encode(format!("base row pre-load: {error}")))?;
        }
        let mut kv_table = KvTable::with_storage(base.id, kv_columns, Box::new(storage));
        kv_table.set_name(base.name.original());
        // The record reader rebuilds the clustered key columns from the
        // record KEY (a PKIsHandle table stores its `id` in the key suffix,
        // not the row value), so the handle layout must be wired or every
        // read decodes the key column as NULL.
        if base.pk_is_handle {
            if let Some(offset) = base.columns.iter_deref().position(|column| {
                column.read().field_type.flags() & tidb_datatype::FieldTypeFlags::PRI_KEY != 0
            }) {
                kv_table.set_pk_handle_offset(offset);
            }
        } else if base.is_common_handle {
            kv_table.set_common_handle_offsets(common_handle_offsets(base)?);
            kv_table.set_common_handle_version(base.common_handle_version);
        }
        catalog
            .register_kv_in(schema_name, base.name.original(), kv_table)
            .map_err(|error| {
                let error = error.to_mysql_error();
                DdlPlanError::Admission(DdlAdmissionError::with_code(error.code, error.message))
            })?;
    }

    // The build session installs the definition's persisted SQL mode (Go
    // `MViewExecutionSessionVarsFromJob`); strictness is the one flag the
    // SELECT's expression evaluation reads here.
    let definition_sql_mode = view_meta
        .read()
        .definition_sql_mode
        .try_into()
        .unwrap_or_default();
    let strict =
        definition_sql_mode & (GO_MODE_STRICT_TRANS_TABLES | GO_MODE_STRICT_ALL_TABLES) != 0;
    let context = StmtContext::for_query()
        .with_strict(strict)
        .with_ddl_sql_mode(definition_sql_mode);
    let sql = format!("SELECT * FROM ({sql_content}) AS `tidb_mv_query`");
    let (_, rows) = tidb_executor::run_select_meta_in(&sql, &catalog, schema_name, &context)
        .map_err(|error| {
            DdlPlanError::Encode(format!("create materialized view build: {error}"))
        })?;

    // The view's columns are the SELECT's outputs in order (the create
    // stamped the derived field types onto them), so the mapping is
    // positional by construction.
    let view_columns: Vec<i64> = view
        .cols()
        .iter_deref()
        .map(|column| column.read().id)
        .collect();
    let mut mutations = Vec::new();
    let mut row_ids = rowid_view_allocator(snapshot, schema_id, view)?;
    for row in &rows {
        if row.len() != view_columns.len() {
            return Err(DdlPlanError::Encode(format!(
                "create materialized view build: definition produced {} columns but the view declares {}",
                row.len(),
                view_columns.len(),
            )));
        }
        let mut values = RowValues::new();
        for (offset, datum) in row.iter().enumerate() {
            values.insert(view_columns[offset], datum.clone());
        }
        match HandleLayout::of(view) {
            HandleLayout::RowId => {
                let row_id = row_ids.next_id();
                mutations.extend(
                    insert_row(view, row_id, &values)
                        .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
                );
            }
            HandleLayout::Int(_) | HandleLayout::Common(_) => {
                mutations.extend(
                    store_clustered_row(view, None, &values)
                        .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
                );
            }
        }
    }
    // A row-id view's allocator watermark advances to the last id this build
    // handed out (Go's transaction `Alloc` publishes the same end value).
    if let HandleLayout::RowId = HandleLayout::of(view) {
        if let Some(last_used) = row_ids.last_used {
            mutations.push(OptimisticMutation::meta_put(
                key::auto_table_id_kv_key(schema_id, view.id),
                value::encode_int_value(last_used),
            )?);
        }
    }

    Ok(MviewBuildPlan {
        read_ts: start_ts,
        row_count: u64::try_from(rows.len()).unwrap_or(u64::MAX),
        mutations,
    })
}

/// The clustered PRIMARY KEY's column offsets, the same derivation the
/// cluster session's table loader performs for a common-handle table.
fn common_handle_offsets(table: &TableInfo) -> Result<Vec<usize>, DdlPlanError> {
    let primary = table
        .indices
        .iter_deref()
        .find(|index| index.read().primary)
        .ok_or_else(|| {
            DdlPlanError::Encode(
                "create materialized view build: clustered base has no PRIMARY KEY index"
                    .to_owned(),
            )
        })?;
    let primary_columns = primary.read().columns.clone();
    let mut offsets = Vec::with_capacity(primary_columns.len());
    for column in primary_columns.iter_deref() {
        let name = column.read().name.lowercase().to_owned();
        let offset = table
            .columns
            .iter_deref()
            .position(|public| public.read().name.lowercase() == name.as_str())
            .ok_or_else(|| {
                DdlPlanError::Encode(format!(
                    "create materialized view build: clustered PRIMARY KEY covers non-public column {name}"
                ))
            })?;
        offsets.push(offset);
    }
    Ok(offsets)
}

/// The row-id allocator of a view whose rows live under `_tidb_rowid`.
///
/// Go's insert allocates from the table's cached allocator and publishes the
/// end of the batch at commit; reading the persisted watermark and handing
/// out the ids after it reaches the same end state.
struct RowIdAllocator {
    next: i64,
    last_used: Option<i64>,
}

impl RowIdAllocator {
    fn next_id(&mut self) -> i64 {
        let id = self.next;
        self.next = self.next.wrapping_add(1);
        self.last_used = Some(id);
        id
    }
}

fn rowid_view_allocator<S: MetaSnapshot>(
    snapshot: &mut S,
    schema_id: i64,
    view: &TableInfo,
) -> Result<RowIdAllocator, DdlPlanError> {
    if !matches!(HandleLayout::of(view), HandleLayout::RowId) {
        return Ok(RowIdAllocator {
            next: 0,
            last_used: None,
        });
    }
    // Go's `Inc` treats a missing allocator key as zero, so the first id is
    // 1; the view is provably empty (the residual probe ran first), so the
    // watermark alone decides the starting point.
    let stored = snapshot
        .get(&key::auto_table_id_kv_key(schema_id, view.id))
        .map_err(DdlPlanError::from)?;
    let current = stored
        .as_deref()
        .map(|bytes| {
            value::parse_int_value(bytes)
                .map_err(|error| DdlPlanError::Encode(format!("view row-id allocator: {error}")))
        })
        .transpose()?
        .unwrap_or(0);
    Ok(RowIdAllocator {
        next: current + 1,
        last_used: None,
    })
}
