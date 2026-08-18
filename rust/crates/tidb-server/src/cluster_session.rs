// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The wide-SQL session over cluster storage: the catalog comes from the
//! cluster loader, and every table it names reads and writes through a real
//! transaction instead of an in-process map.
//!
//! This is the join of the two halves the rewrite built separately. The
//! session driver (`tidb-session` over `tidb-executor`) owns SQL: parsing,
//! planning, expressions, joins, ordering, aggregation. The cluster loader and
//! the transaction coordinator own storage: schema at one snapshot, reads at
//! one `start_ts`, writes as one 2PC publication. Nothing in the driver knows
//! which side it is on -- it calls the row-level table API, which is why the
//! swap is one constructor argument.
//!
//! # What a table must be to run here
//!
//! Every loaded table is admitted whose *storage layout* this tier encodes and
//! decodes: a non-partitioned base table (not a view, not a sequence) whose
//! columns are public. A view, a sequence, a partitioned table, or a table
//! still mid-DDL is refused by name, and the refusal is kept and reported
//! rather than making the table silently disappear -- the same choice
//! [`configure_loaded_table`] makes for the bounded read path.
//!
//! [`configure_loaded_table`]: tidb_exec::cluster_catalog::configure_loaded_table

use std::sync::{Arc, Mutex};
use tidb_datatype::FieldTypeFlags;
use tidb_exec::cluster_catalog::ClusterCatalog;
use tidb_exec::cluster_stats_load::{ClusterStatsItem, ClusterTableStats};
use tidb_exec::stats_watch::{StatsSnapshot, TableStatsState};
use tidb_executor::access_cost::TableStatistics;
use tidb_executor::cluster_storage::ClusterTableStorage;
use tidb_executor::driver::Catalog;
use tidb_executor::kv_table::{KvColumn, KvIndex, KvTable, TableAutoId};
use tidb_executor::storage::TableStorage;
use tidb_model::{GoShared, SchemaState, TableInfo};
use tidb_planner::cardinality::row_count_estimator::{ColumnStats, IndexStats};
use tidb_session::{Session, SharedCatalog};

/// Go `mysql.PriKeyFlag`: what marks the column `PKIsHandle` points at.
const PRI_KEY_FLAG: u32 = 1 << 1;

/// Where a loaded table's auto-increment counter comes from.
///
/// The counter is NOT part of the stored `TableInfo` -- Go keeps it in meta
/// keys of its own -- so the loader cannot build one from what it reads, and
/// must be told. Whoever implements this also owns the allocator's LIFETIME:
/// the same table asked for twice must get clones of one allocator, or each
/// catalog rebuild would reserve a fresh range and leave a hole in the ids.
///
/// The cluster node implements it over the meta keys
/// (`tidb_exec::cluster_auto_id`); tests that only need the old in-process
/// behaviour use [`LocalTableAutoIds`].
pub trait TableAutoIds: std::fmt::Debug + Send + Sync {
    /// The live allocator for `table`, which is stored in database `db_id`.
    fn allocator_for(&self, db_id: i64, table: &TableInfo) -> TableAutoId;

    /// The live, distinct AUTO_RANDOM allocator for `table`.
    fn random_allocator_for(&self, db_id: i64, table: &TableInfo) -> TableAutoId;
}

/// Counters that live in this process and start at zero, one per table id.
///
/// This is what the tier did before the counter had a home, and it is correct
/// ONLY where nothing else writes the same rows -- tests and the smoke
/// binary. It is deliberately not the production default: against shared
/// cluster storage a counter that starts at zero re-issues ids that already
/// exist.
#[derive(Debug, Default)]
pub struct LocalTableAutoIds {
    allocators: Mutex<LocalTableAutoIdMap>,
}

type LocalTableAutoIdMap = std::collections::HashMap<(i64, bool), (i64, TableAutoId)>;

fn auto_id_step(table: &TableInfo) -> u64 {
    if table.auto_id_cache > 1 {
        table.auto_id_cache as u64
    } else {
        tidb_executor::kv_table::DEFAULT_AUTO_ID_STEP
    }
}

impl TableAutoIds for LocalTableAutoIds {
    fn allocator_for(&self, _db_id: i64, table: &TableInfo) -> TableAutoId {
        local_allocator_for(&self.allocators, table, false)
    }

    fn random_allocator_for(&self, _db_id: i64, table: &TableInfo) -> TableAutoId {
        local_allocator_for(&self.allocators, table, true)
    }
}

fn local_allocator_for(
    allocators: &Mutex<LocalTableAutoIdMap>,
    table: &TableInfo,
    random: bool,
) -> TableAutoId {
    let mut allocators = allocators.lock().expect("local auto id map poisoned");
    if let Some((cache, allocator)) = allocators.get(&(table.id, random)) {
        if *cache == table.auto_id_cache {
            return allocator.clone();
        }
        let allocator = allocator.with_step(auto_id_step(table));
        allocators.insert((table.id, random), (table.auto_id_cache, allocator.clone()));
        return allocator;
    }
    let allocator = TableAutoId::over(
        Arc::new(tidb_executor::kv_table::LocalAutoIdStore::new()),
        auto_id_step(table),
    );
    allocators.insert((table.id, random), (table.auto_id_cache, allocator.clone()));
    allocator
}

/// What [`cluster_table`] gives the table it builds as an auto-increment
/// counter.
///
/// Two shapes, because the loader has two callers that differ in KIND, not in
/// degree. A session catalog is built to RUN statements against, so every
/// table it holds must be able to allocate, and the database the counter's
/// meta key lives under is known -- that is [`AutoIdSource::In`]. The
/// `CREATE INDEX`/`DROP INDEX` backfill builds the same table only to WALK its
/// stored rows and write index entries; it inserts nothing, so it has no id to
/// allocate, and correspondingly no database id to name.
///
/// The absence is a variant rather than a zero `db_id` and a stub allocator
/// because a stub here would be the worst possible failure: a counter starting
/// at zero against shared cluster storage re-issues ids the table already
/// holds, silently, with no error to see. [`AutoIdSource::Unavailable`] cannot
/// do that -- asked for an id, it reports a counter it has no home for, which
/// is a loud wrong answer instead of a quiet one.
#[derive(Debug)]
pub enum AutoIdSource<'a> {
    /// A table in database `db_id`, whose counters `ids` owns.
    In {
        /// The database the table is stored in, which names its meta key.
        db_id: i64,
        /// The live counters, one per table.
        ids: &'a dyn TableAutoIds,
    },
    /// No counter, for a caller that reads and rewrites existing rows only.
    Unavailable,
}

impl AutoIdSource<'_> {
    /// The allocator for `table`, or one that refuses every request.
    fn allocator_for(&self, table: &TableInfo) -> TableAutoId {
        match self {
            AutoIdSource::In { db_id, ids } => ids.allocator_for(*db_id, table),
            AutoIdSource::Unavailable => TableAutoId::over(Arc::new(UnavailableAutoIdStore), 1),
        }
    }

    fn random_allocator_for(&self, table: &TableInfo) -> TableAutoId {
        match self {
            AutoIdSource::In { db_id, ids } => ids.random_allocator_for(*db_id, table),
            AutoIdSource::Unavailable => TableAutoId::over(Arc::new(UnavailableAutoIdStore), 1),
        }
    }
}

/// A counter home for a caller that has none, which answers every request with
/// the reason rather than a number.
///
/// It is an [`AutoIdStoreError`] and not a panic on purpose: the error already
/// means "this counter could not be reached", which is exactly true here, and
/// it travels the path a real unreachable meta key travels rather than taking
/// the process down.
///
/// [`AutoIdStoreError`]: tidb_executor::kv_table::AutoIdStoreError
#[derive(Debug)]
struct UnavailableAutoIdStore;

impl tidb_executor::kv_table::AutoIdStore for UnavailableAutoIdStore {
    fn reserve(
        &self,
        _step: u64,
        _unsigned: bool,
    ) -> Result<(u64, u64), tidb_executor::kv_table::AutoIdStoreError> {
        Err(unavailable_counter())
    }

    fn next_global(&self) -> Result<u64, tidb_executor::kv_table::AutoIdStoreError> {
        Err(tidb_executor::kv_table::AutoIdStoreError(
            "the table has no auto-id counter home".to_owned(),
        ))
    }

    fn rebase(
        &self,
        _required: u64,
        _unsigned: bool,
    ) -> Result<(), tidb_executor::kv_table::AutoIdStoreError> {
        Err(unavailable_counter())
    }

    fn force_rebase(
        &self,
        _required: u64,
        _unsigned: bool,
    ) -> Result<(), tidb_executor::kv_table::AutoIdStoreError> {
        Err(unavailable_counter())
    }

    fn reset(&self) -> Result<(), tidb_executor::kv_table::AutoIdStoreError> {
        Err(unavailable_counter())
    }
}

fn unavailable_counter() -> tidb_executor::kv_table::AutoIdStoreError {
    tidb_executor::kv_table::AutoIdStoreError(
        "this table was built to walk its stored rows, not to insert, so its \
         auto-increment counter was never given a home"
            .to_owned(),
    )
}

/// Why one loaded table is not part of the session's catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkippedTable {
    /// Fully qualified stored name, `schema.table`.
    pub name: String,
    /// Exact, self-contained explanation.
    pub reason: String,
}

/// One session's catalog over cluster storage, plus every table left out.
#[derive(Debug)]
pub struct ClusterSessionCatalog {
    /// The driver's catalog: databases and tables backed by `storage`.
    pub catalog: Catalog,
    /// Tables the cluster has that this tier cannot serve.
    pub skipped: Vec<SkippedTable>,
}

/// Builds a driver catalog whose every table reads and writes through
/// `storage`.
///
/// `storage` is cloned per table, and cloning a [`ClusterTableStorage`] clones
/// *handles*: all of them share the statement's snapshot and the session's
/// staged writes.
#[must_use]
pub fn cluster_session_catalog(
    loaded: &ClusterCatalog,
    storage: &ClusterTableStorage,
    stats: &StatsSnapshot,
    auto_ids: &dyn TableAutoIds,
) -> ClusterSessionCatalog {
    let mut catalog = Catalog::default();
    let mut skipped = Vec::new();
    for database in &loaded.databases {
        let schema = database.info.name.original().to_owned();
        catalog.register_database_with_id(&schema, database.info.id);
        for table in &database.tables {
            // A view has no storage half: it maps straight to the session
            // catalog's view entry, rebuilt from the published `TableInfo`
            // exactly as `build_view_table_info` wrote it (the enum ordinals
            // are `ast/model.go`'s).
            if let Some(view) = &table.view {
                let view = view.read();
                let mut columns = Vec::with_capacity(table.columns.len());
                for i in 0..table.columns.len() {
                    if let Some(column) = table.columns.get(i) {
                        let column = column.read();
                        columns.push((column.name.original().to_owned(), column.field_type.clone()));
                    }
                }
                let (definer_user, definer_host) = view.definer.as_ref().map_or_else(
                    || (String::new(), String::new()),
                    |definer| (definer.username.clone(), definer.hostname.clone()),
                );
                let view_def = tidb_executor::ViewDef {
                    name: table.name.original().to_owned(),
                    columns,
                    select_sql: view.select_stmt.clone(),
                    definer_user,
                    definer_host,
                    character_set_client: table.charset.clone(),
                    collation_connection: table.collate.clone(),
                    algorithm: match view.algorithm.0 {
                        1 => "MERGE",
                        2 => "TEMPTABLE",
                        _ => "UNDEFINED",
                    }
                    .to_owned(),
                    security: if view.security.0 == 1 { "INVOKER" } else { "DEFINER" }.to_owned(),
                    check_option: if view.check_option.0 == 0 { "LOCAL" } else { "CASCADED" }
                        .to_owned(),
                };
                if let Err(reason) = catalog.register_view_in(
                    database.info.name.original(),
                    table.name.original(),
                    view_def,
                ) {
                    skipped.push(SkippedTable {
                        name: format!("{schema}.{}", table.name.original()),
                        reason: format!("view registration failed: {reason:?}"),
                    });
                }
                continue;
            }
            let auto = AutoIdSource::In {
                db_id: database.info.id,
                ids: auto_ids,
            };
            match cluster_table(table, storage, &auto) {
                Ok(kv_table) => {
                    // A table the cluster reports as never analyzed
                    // (`TableStatsState::Pseudo`) or one this node has not
                    // loaded yet is left OUT of the map, which is exactly what
                    // makes the planner treat it as `statistics.PseudoTable`.
                    if let Some(loaded_stats) =
                        stats.get(&table.id).and_then(TableStatsState::loaded)
                    {
                        catalog.set_table_statistics(
                            table.id,
                            Arc::new(planner_statistics(loaded_stats, table)),
                        );
                    }
                    catalog
                        .register_kv_in(&schema, table.name.original(), kv_table)
                        .expect("the schema was created just above this loop");
                }
                Err(reason) => skipped.push(SkippedTable {
                    name: format!("{schema}.{}", table.name.original()),
                    reason,
                }),
            }
        }
    }
    ClusterSessionCatalog { catalog, skipped }
}

/// Opens a session on a catalog loaded from the cluster.
///
/// The catalog is this session's own: a cluster-backed table is bound to one
/// statement's snapshot, so it cannot be shared with peers the way the
/// in-process catalog is.
#[must_use]
pub fn session_with_cluster_storage(
    loaded: &ClusterCatalog,
    storage: &ClusterTableStorage,
    stats: &StatsSnapshot,
    auto_ids: &dyn TableAutoIds,
) -> (Session, Vec<SkippedTable>) {
    let ClusterSessionCatalog { catalog, skipped } =
        cluster_session_catalog(loaded, storage, stats, auto_ids);
    let shared: SharedCatalog = Arc::new(Mutex::new(catalog));
    (Session::with_catalog(shared), skipped)
}

/// Translates one stored `TableInfo` into a table over cluster storage.
///
/// Unlike the bounded node's `configure_loaded_column`, this one applies NO
/// charset/collation gate, and must not: the wide path keeps the stored
/// `FieldType` verbatim, so every collation-consuming site downstream --
/// `SortExec` (`collation_of_node`), `HashAggExec`'s group key (which is also
/// how `SELECT DISTINCT` is built), the hash-join key class, and the compare
/// builtins' `derived_collation` -- reads the column's REAL collation and
/// runs under it. Refusing a `utf8mb4_general_ci` table here would deny a
/// table this tier serves correctly. (The bounded node needs its gate because
/// its own comparator hardcodes `utf8mb4_bin`; the two tiers share no
/// execution code.)
pub(crate) fn cluster_table(
    table: &TableInfo,
    storage: &ClusterTableStorage,
    auto: &AutoIdSource<'_>,
) -> Result<KvTable, String> {
    if table.is_view() {
        return Err("it is a view".to_owned());
    }
    if table.is_sequence() {
        return Err("it is a sequence".to_owned());
    }
    if table.partition.is_some() {
        return Err("it is partitioned".to_owned());
    }
    if table.state != SchemaState::PUBLIC {
        return Err(format!(
            "its schema state is {} rather than public",
            table.state.0
        ));
    }
    let columns: Vec<GoShared<tidb_model::column::ColumnInfo>> =
        table.cols().iter_deref().collect();
    if columns.is_empty() {
        return Err("it has no public columns".to_owned());
    }
    // The row layout is keyed by column id, so a column's *offset* is only a
    // position in the tuple the driver builds; both must come from the same
    // public-column list, which is why the offsets below are indexes into it.
    //
    // A DEFAULT is stored on the cluster's `ColumnInfo` as a Go `any`, and the
    // driver's INSERT path reads it off `KvColumn` instead -- one fact in two
    // places. Dropping the second copy is NOT the safe side of that: a
    // nullable column whose DEFAULT this loader forgets stores NULL where
    // TiDB stores the default, silently, with no error to see. So the value
    // is materialised HERE, in the one call that builds the `KvColumn`, by
    // the same `system_row_write` rule the system-row writer uses; a default
    // that cannot be carried across verbatim (an expression, or
    // `CURRENT_TIMESTAMP`, whose instant is per-INSERT and not per-load)
    // refuses the whole table by name, the way a prefix index does below.
    let mut kv_columns: Vec<KvColumn> = Vec::with_capacity(columns.len());
    for column in &columns {
        let column = column.read();
        let name = column.name.original().to_owned();
        // A nil Go interface is "no DEFAULT was written", which is not the
        // same fact as a `DEFAULT NULL`, so it must become `None` here rather
        // than `Some(Null)`: only the first makes an omitted NOT NULL column
        // the 1364 it is in Go.
        let default_value = if column.default_value.is_nil() {
            None
        } else if let Some(fsp) = stored_clock_marker(&column) {
            // Go stores the WORD (`CURRENT_TIMESTAMP`, with the column's fsp
            // when it has one) and every INSERT evaluates it fresh. The
            // executor models exactly that as a computed default, so the
            // loader carries it instead of refusing the table -- refusal
            // would make every table a Go TiDB created with a clock default
            // unservable here.
            Some(
                tidb_executor::column_default::stored_clock_marker_default(
                    &column.field_type,
                    fsp.as_deref(),
                )
                .map_err(|error| format!("its column {name} has a default {error:?}"))?,
            )
        } else {
            // The loader refuses a computed default above, so what survives
            // is always a settled value.
            Some(tidb_executor::column_default::ColumnDefault::Value(
                tidb_exec::system_row_write::literal_default(&column, table.name.original())
                    .map_err(|error| format!("its column {name} has a default {error}"))?,
            ))
        };
        let origin_default_value = column.get_origin_default_value();
        let origin_default = if origin_default_value.is_nil() {
            None
        } else {
            Some(
                tidb_exec::system_row_write::origin_default(&column, table.name.original())
                    .map_err(|error| {
                        format!("its column {name} has an original default {error}")
                    })?,
            )
        };
        kv_columns.push(KvColumn {
            name,
            id: column.id,
            field_type: column.field_type.clone(),
            column_info_version: column.version,
            // The cluster catalog loader refuses a generated column outright
            // (`tidb_exec::cluster_catalog`), so a table that reaches here
            // never has one.
            generated: None,
            default_value,
            origin_default,
        });
    }
    let mut kv_table = KvTable::with_storage(table.id, kv_columns, storage.clone_box());
    kv_table.set_name(table.name.original());
    kv_table.set_cache_status(table.table_cache_status_type);
    // The AUTO_INCREMENT column and the counter that feeds it. Both halves
    // have to be here: marking the column without giving the counter a
    // cluster-wide home would allocate from a fresh in-process cell and
    // re-issue ids the table already holds, which is why this loader used to
    // refuse such a table outright rather than serve it half-wired.
    let auto_increment_offset = columns.iter().position(|column| {
        column
            .read()
            .field_type
            .has_flag(FieldTypeFlags::AUTO_INCREMENT)
    });
    if let Some(offset) = auto_increment_offset {
        kv_table.set_auto_increment_offset(offset);
    }
    // Go uses one row-id allocator for every table without a clustered
    // handle. This includes tables without an explicit AUTO_INCREMENT column:
    // their hidden `_tidb_rowid` must be allocated from the same
    // cluster-wide counter on every SQL node, or concurrent inserts can
    // overwrite one another's record keys.
    if auto_increment_offset.is_some() || (!table.pk_is_handle && !table.is_common_handle) {
        kv_table.set_auto_id(auto.allocator_for(table));
    }
    if table.pk_is_handle {
        let handle = columns
            .iter()
            .position(|column| column.read().field_type.flags() & PRI_KEY_FLAG != 0)
            .ok_or_else(|| {
                "it is marked PKIsHandle but has no public primary key column".to_owned()
            })?;
        kv_table.set_pk_handle_offset(handle);
    } else if table.is_common_handle {
        let handles = clustered_handle_offsets(table, &columns)?;
        kv_table.set_common_handle_offsets(handles);
    }
    if table.contains_auto_random_bits() {
        let offset = if table.pk_is_handle {
            kv_table
                .pk_handle_offset()
                .expect("an AUTO_RANDOM PKIsHandle table has a handle column")
        } else {
            *kv_table
                .common_handle_offsets()
                .first()
                .expect("an AUTO_RANDOM common handle has a first column")
        };
        kv_table.set_auto_random(tidb_executor::kv_table::AutoRandomSpec {
            offset,
            shard_bits: table.auto_random_bits,
            range_bits: if table.auto_random_range_bits == 0 {
                64
            } else {
                table.auto_random_range_bits
            },
            unsigned: table.is_auto_random_bit_col_unsigned(),
        });
        kv_table.set_auto_random_id(auto.random_allocator_for(table));
    }
    for index in table.indices.iter_deref() {
        let index = index.read();
        if index.state != SchemaState::PUBLIC {
            continue;
        }
        kv_table.add_index(kv_index(&index, &columns)?);
    }
    Ok(kv_table)
}

/// Translates one stored `IndexInfo` into the executor's `KvIndex`, against
/// the table's PUBLIC column list.
///
/// The offsets are resolved by NAME here even though `IndexColumn.Offset`
/// carries one, because the two count different things: Go's offset is a
/// position in `TableInfo.Columns`, and a `KvIndex`'s is a position in the
/// public columns this loader built. They coincide only while no column is
/// non-public, and a silent disagreement between them indexes the wrong
/// column.
///
/// It is one function rather than a loop inside [`cluster_table`] because the
/// `CREATE INDEX` backfill has to build the very same `KvIndex` for an index
/// the stored table does not carry yet -- and an index whose entries are
/// WRITTEN under one mapping and READ under another is the exact silent wrong
/// answer this tier keeps hunting.
pub(crate) fn kv_index(
    index: &tidb_model::index::IndexInfo,
    columns: &[GoShared<tidb_model::column::ColumnInfo>],
) -> Result<KvIndex, String> {
    // A prefix index (`KEY idx(s(4))`) stores each column value CUT to the
    // prefix. The IN-PROCESS engine now cuts on both sides of that -- see
    // `tidb_executor::index_prefix_cut` -- but this seam does not reach it:
    // the cluster write path encodes entries through
    // `tidb_codec::generate_index_key` and the cluster read path builds key
    // ranges in `tidb_distsql::request_builder`, neither of which cuts. An
    // index WRITTEN under one mapping and READ under another is the exact
    // silent wrong answer this tier keeps hunting, so the whole table is
    // refused and reported rather than half-supported.
    if index.has_prefix_index() {
        return Err(format!(
            "its index {} is a prefix index, whose entries are each column value cut to \
             the prefix length, which this node neither reads nor writes that way",
            index.name.original()
        ));
    }
    let mut offsets = Vec::with_capacity(index.columns.len());
    let mut prefix_lengths = Vec::with_capacity(index.columns.len());
    for column in index.columns.iter_deref() {
        let (name, original_name, length) = {
            let column = column.read();
            (
                column.name.lowercase().to_owned(),
                column.name.original().to_owned(),
                column.length,
            )
        };
        let offset = columns
            .iter()
            .position(|public| public.read().name.lowercase() == name.as_str())
            .ok_or_else(|| {
                format!(
                    "its index {} covers non-public column {}",
                    index.name.original(),
                    original_name
                )
            })?;
        offsets.push(offset);
        prefix_lengths.push(length);
    }
    Ok(KvIndex {
        id: index.id,
        name: index.name.original().to_owned(),
        comment: index.comment.clone(),
        unique: index.unique,
        column_offsets: offsets,
        prefix_lengths,
        visible: !index.invisible,
        global: index.global,
    })
}

/// Go `mysql.MaxUnsignedFlag` (`UnsignedFlag`), which the out-of-range
/// scaling in the estimator reads off a column.
const UNSIGNED_FLAG: u32 = 1 << 5;

/// Translates one table's loaded `mysql.stats_*` rows into the shape the
/// planner's estimator reads.
///
/// This is the ONE place the storage form and the estimation form meet: the
/// loader ([`tidb_exec::cluster_stats_load`]) owns how a histogram is stored,
/// [`tidb_planner::cardinality`] owns how it is read, and neither has to know
/// the other. A histogram whose `hist_id` names no current column or index --
/// a dropped one whose stats rows have not been GC'd -- is skipped, because
/// the estimator keys on the live schema.
fn planner_statistics(stats: &ClusterTableStats, table: &TableInfo) -> TableStatistics {
    let mut columns = std::collections::BTreeMap::new();
    for column in table.cols().iter_deref() {
        let (id, unsigned) = {
            let column = column.read();
            (column.id, column.field_type.flags() & UNSIGNED_FLAG != 0)
        };
        let Some(item) = stats.column(id).filter(stats_available) else {
            continue;
        };
        columns.insert(
            id,
            ColumnStats {
                histogram: item.histogram.clone(),
                topn: item.topn.clone(),
                cms: item.cms.clone(),
                stats_ver: item.stats_ver,
                unsigned,
            },
        );
    }
    let mut indexes = std::collections::BTreeMap::new();
    for index in table.indices.iter_deref() {
        let (id, num_columns, unique) = {
            let index = index.read();
            (index.id, index.columns.len(), index.unique)
        };
        let Some(item) = stats.index(id).filter(stats_available) else {
            continue;
        };
        indexes.insert(id, index_statistics(item, num_columns, unique));
    }
    // `TableStatistics::new` decides `pseudo` -- Go's `GetStatsTable` reaches
    // it both from an uninitialized histogram set and from a zero row count,
    // and that rule lives in one place for both tiers.
    TableStatistics::new(
        i64::try_from(stats.row_count).unwrap_or(i64::MAX),
        stats.modify_count,
        columns,
        indexes,
    )
}

/// Go `Column.StatsAvailable()` / `IsColumnAnalyzedOrSynthesized`: whether
/// this histogram was actually collected.
///
/// A `stats_histograms` row can exist with `stats_ver = 0` -- an ADD COLUMN
/// synthesizes one from the default value -- so the version alone is not the
/// test; Go also accepts a non-zero NDV or null count, which is that
/// synthesized case.
fn stats_available(item: &&ClusterStatsItem) -> bool {
    item.stats_ver > 0 || item.histogram.ndv > 0 || item.histogram.null_count > 0
}

/// One index histogram, with the two schema facts the estimator needs that
/// the stored row does not carry.
fn index_statistics(item: &ClusterStatsItem, num_columns: usize, unique: bool) -> IndexStats {
    IndexStats {
        histogram: item.histogram.clone(),
        topn: item.topn.clone(),
        cms: item.cms.clone(),
        stats_ver: item.stats_ver,
        num_columns,
        unique,
    }
}

/// The public-column offsets of a clustered composite handle, in key order.
fn clustered_handle_offsets(
    table: &TableInfo,
    columns: &[GoShared<tidb_model::column::ColumnInfo>],
) -> Result<Vec<usize>, String> {
    let primary = table
        .indices
        .iter_deref()
        .find(|index| index.read().primary)
        .ok_or_else(|| "it is marked clustered but has no PRIMARY KEY index".to_owned())?;
    let primary_columns = primary.read().columns.clone();
    let mut offsets = Vec::with_capacity(primary_columns.len());
    for column in primary_columns.iter_deref() {
        let (name, original_name) = {
            let column = column.read();
            (
                column.name.lowercase().to_owned(),
                column.name.original().to_owned(),
            )
        };
        let offset = columns
            .iter()
            .position(|public| public.read().name.lowercase() == name.as_str())
            .ok_or_else(|| {
                format!(
                    "its clustered PRIMARY KEY covers non-public column {}",
                    original_name
                )
            })?;
        offsets.push(offset);
    }
    Ok(offsets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_executor::cluster_storage::{ClusterSnapshot, MutationBuffer, SnapshotPairs};
    use tidb_executor::storage::StorageError;
    use tidb_model::column::{ColumnDefaultValue, ColumnInfo};
    use tidb_model::db::DBInfo;
    use tidb_model::index::{IndexColumn, IndexInfo};
    use tidb_model::GoAny;
    use tidb_session::StmtResult;
    use tidb_txnkv::Key;

    /// A snapshot with fixed contents: the cluster as this test's statements
    /// see it. Nothing a statement writes may appear in here, because a staged
    /// write is not a committed one.
    #[derive(Debug, Default)]
    struct FixedSnapshot {
        data: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    impl ClusterSnapshot for FixedSnapshot {
        fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.data.get(key.as_bytes()).cloned())
        }

        fn scan(
            &mut self,
            start: &Key,
            end: &Key,
            limit: Option<usize>,
        ) -> Result<SnapshotPairs, StorageError> {
            Ok(self
                .data
                .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
                .take(limit.unwrap_or(usize::MAX))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
    }

    fn column(id: i64, offset: i64, name: &str, primary: bool) -> ColumnInfo {
        let mut field_type = FieldType::new(FieldTypeCode::LongLong);
        if primary {
            field_type.add_flags(PRI_KEY_FLAG);
        }
        let mut column = ColumnInfo::new(id, name, field_type);
        column.offset = offset;
        column
    }

    /// One database `app` holding `t(id BIGINT PRIMARY KEY, v BIGINT)`, plus a
    /// table still mid-DDL that the session must refuse by name.
    fn loaded_catalog() -> ClusterCatalog {
        let base = TableInfo {
            id: 101,
            name: CiString::new("t"),
            columns: vec![column(1, 0, "id", true), column(2, 1, "v", false)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let pending = TableInfo {
            id: 102,
            name: CiString::new("t_pending"),
            columns: vec![column(1, 0, "id", false)].into(),
            state: SchemaState::NONE,
            ..TableInfo::default()
        };
        ClusterCatalog {
            schema_version: 7,
            databases: vec![tidb_exec::cluster_catalog::LoadedDatabase {
                info: DBInfo {
                    id: 5,
                    name: CiString::new("app"),
                    ..DBInfo::default()
                },
                tables: vec![base, pending],
            }],
        }
    }

    fn cluster_storage() -> (
        ClusterTableStorage,
        MutationBuffer,
        Arc<Mutex<FixedSnapshot>>,
    ) {
        let snapshot = Arc::new(Mutex::new(FixedSnapshot::default()));
        let buffer = MutationBuffer::new();
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&snapshot) as _;
        (
            ClusterTableStorage::new(buffer.clone(), handle),
            buffer,
            snapshot,
        )
    }

    #[test]
    fn loaded_column_ndv_reaches_grouped_cluster_plans() {
        let table = TableInfo {
            id: 130,
            name: CiString::new("order_line"),
            columns: vec![
                column(1, 0, "ol_o_id", true),
                column(2, 1, "ol_d_id", false),
                column(3, 2, "ol_amount", false),
            ]
            .into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let version = 42;
        let item = |id, ndv| {
            let mut item = ClusterStatsItem {
                id,
                is_index: false,
                stats_ver: 2,
                flag: 1,
                histogram: Default::default(),
                topn: None,
                cms: None,
            };
            item.histogram.id = id;
            item.histogram.ndv = ndv;
            item.histogram.last_update_version = version;
            item.histogram.append_bucket(
                tidb_datatype::Datum::Int(1),
                tidb_datatype::Datum::Int(3_000_065),
                3_000_065,
                1,
            );
            item
        };
        let loaded_stats = ClusterTableStats {
            table_id: table.id,
            version,
            modify_count: 0,
            row_count: 3_000_065,
            columns: vec![item(1, 3_000_065), item(2, 10), item(3, 3_000_065)],
            indexes: Vec::new(),
        };
        let translated = planner_statistics(&loaded_stats, &table);
        assert!(!translated.pseudo);
        assert_eq!(
            translated.columns.keys().copied().collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let snapshot =
            StatsSnapshot::from([(table.id, TableStatsState::Loaded(Arc::new(loaded_stats)))]);
        let (storage, _, _) = cluster_storage();
        let (mut session, skipped) = session_with_cluster_storage(
            &one_table_catalog(table),
            &storage,
            &snapshot,
            &LocalTableAutoIds::default(),
        );
        assert!(skipped.is_empty(), "{skipped:?}");
        {
            let catalog = session.shared_catalog();
            let catalog = catalog.lock().unwrap();
            let statistics = catalog
                .table_statistics(130)
                .expect("loaded statistics survive session construction");
            assert!(!statistics.pseudo);
            assert_eq!(statistics.columns.get(&2).unwrap().histogram.ndv, 10);
        }
        session.run("USE app").unwrap();
        let StmtResult::Rows(rows) = session
            .run(
                "EXPLAIN FORMAT='brief' SELECT ol_d_id, SUM(ol_amount) \
                 FROM order_line WHERE ol_o_id BETWEEN 1 AND 1775 GROUP BY ol_d_id",
            )
            .unwrap()
        else {
            panic!("expected EXPLAIN rows");
        };
        let text = |row: usize, column: usize| match &rows[row][column] {
            tidb_datatype::Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        };
        let grouped = rows
            .iter()
            .enumerate()
            .find(|(row, _)| text(*row, 0).contains("HashAgg"))
            .map(|(row, _)| row)
            .expect("the GROUP BY has a physical aggregation");
        assert_eq!(text(grouped, 1), "1.00", "{rows:#?}");
    }

    #[test]
    fn wide_sql_runs_on_cluster_storage_without_committing() {
        let (storage, buffer, snapshot) = cluster_storage();
        let (mut session, skipped) = session_with_cluster_storage(
            &loaded_catalog(),
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        // The unserved table is named in the refusal rather than silently
        // vanishing from the session's catalog.
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].name, "app.t_pending");
        assert_eq!(
            skipped[0].reason,
            "its schema state is 0 rather than public"
        );

        session.run("USE app").unwrap();
        assert!(matches!(
            session
                .run("INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
                .unwrap(),
            StmtResult::Affected(3)
        ));
        // The rows are staged, not committed: the cluster snapshot is
        // untouched and the session's buffer holds the bytes.
        assert!(snapshot.lock().unwrap().data.is_empty());
        assert_eq!(buffer.len(), 3);

        // The whole read path -- scan, WHERE, an expression over columns,
        // ORDER BY -- runs over the buffer-in-front-of-snapshot storage.
        let StmtResult::Rows(rows) = session
            .run("SELECT id, v + id * 2 FROM t WHERE v > 10 ORDER BY id DESC")
            .unwrap()
        else {
            panic!("expected rows");
        };
        let values: Vec<String> = rows
            .iter()
            .map(|row| format!("{:?}|{:?}", row[0], row[1]))
            .collect();
        assert_eq!(
            values,
            vec!["Int(3)|Int(36)".to_owned(), "Int(2)|Int(24)".to_owned()]
        );

        // A staged DELETE hides a staged row, and the cluster still sees none
        // of it.
        assert!(matches!(
            session.run("DELETE FROM t WHERE id = 2").unwrap(),
            StmtResult::Affected(1)
        ));
        let StmtResult::Rows(rows) = session.run("SELECT id FROM t ORDER BY id").unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows.len(), 2);
        assert!(snapshot.lock().unwrap().data.is_empty());
    }

    /// One database `app` holding `d(id BIGINT PRIMARY KEY, v BIGINT DEFAULT
    /// 7)`, as the loader receives it from the cluster: the DEFAULT lives on
    /// the stored `ColumnInfo`, not on anything this node builds by hand.
    fn default_catalog() -> ClusterCatalog {
        let mut v = column(2, 1, "v", false);
        v.default_value = ColumnDefaultValue::string_bytes(b"7".to_vec()).into();
        v.origin_default_value = ColumnDefaultValue::string_bytes(b"7".to_vec()).into();
        let table = TableInfo {
            id: 301,
            name: CiString::new("d"),
            columns: vec![column(1, 0, "id", true), v].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        ClusterCatalog {
            schema_version: 7,
            databases: vec![tidb_exec::cluster_catalog::LoadedDatabase {
                info: DBInfo {
                    id: 5,
                    name: CiString::new("app"),
                    ..DBInfo::default()
                },
                tables: vec![table],
            }],
        }
    }

    /// A column's DEFAULT is stored twice once a table is loaded: on the
    /// cluster's `ColumnInfo` (where `SHOW CREATE TABLE` and the system-row
    /// writer read it) and on the `KvColumn` the driver's INSERT path reads.
    /// The loader must carry it across, or an omitted column silently stores
    /// NULL where TiDB stores the default -- a wrong row, written without a
    /// word.
    #[test]
    fn a_cluster_columns_declared_default_reaches_the_row_an_insert_writes() {
        let (storage, _, _) = cluster_storage();
        let (mut session, skipped) = session_with_cluster_storage(
            &default_catalog(),
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        assert!(
            skipped.is_empty(),
            "a table with a literal DEFAULT is served"
        );
        session.run("USE app").unwrap();
        session.run("INSERT INTO d (id) VALUES (1)").unwrap();
        let StmtResult::Rows(rows) = session.run("SELECT v FROM d").unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows, vec![vec![tidb_datatype::Datum::Int(7)]]);
    }

    #[test]
    fn a_cluster_table_restores_its_persisted_cache_status() {
        let table = TableInfo {
            id: 302,
            name: CiString::new("cached"),
            columns: vec![column(1, 0, "id", true)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            table_cache_status_type: tidb_model::TableCacheStatusType::ENABLE,
            ..TableInfo::default()
        };
        let (storage, _, _) = cluster_storage();
        let loaded = cluster_table(&table, &storage, &AutoIdSource::Unavailable)
            .expect("the cached table is otherwise ordinary");
        assert!(loaded.is_cached());
    }

    /// A default whose value is not a literal -- `CURRENT_TIMESTAMP`, whose
    /// instant belongs to each INSERT and not to the moment the catalog was
    /// loaded -- refuses the table by name. Carrying it as the frozen load
    /// instant, or dropping it, would both write rows TiDB does not.
    #[test]
    fn a_cluster_column_whose_default_is_not_a_literal_refuses_the_table() {
        let catalog = default_catalog();
        let column = catalog.databases[0].tables[0]
            .columns
            .get(1)
            .expect("the fixture has its value column");
        let mut column = column.write();
        column.default_value =
            ColumnDefaultValue::string_bytes(b"CURRENT_TIMESTAMP".to_vec()).into();
        column.origin_default_value = GoAny::nil();
        drop(column);
        let (storage, _, _) = cluster_storage();
        let (_, skipped) = session_with_cluster_storage(
            &catalog,
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].name, "app.d");
        assert!(
            skipped[0].reason.starts_with("its column v has a default"),
            "{}",
            skipped[0].reason
        );
    }

    /// One database `app` holding `ci(id BIGINT PRIMARY KEY, c VARCHAR(32)
    /// COLLATE utf8mb4_general_ci)`, built the way the cluster loader builds
    /// it: the column's `FieldType` is the one decoded from the stored
    /// descriptor, collation name and all.
    fn ci_catalog() -> ClusterCatalog {
        // Decoded from the stored descriptor rather than built by hand, so
        // this exercises the same `From<JsonFieldType>` the catalog loader
        // uses -- including its duty to fill BOTH the collation name and the
        // cached `Collation` enum.
        let text: FieldType = serde_json::from_str(
            r#"{"Tp":15,"Flag":0,"Flen":32,"Decimal":0,"Charset":"utf8mb4",
                "Collate":"utf8mb4_general_ci","Elems":null,
                "ElemsIsBinaryLit":null,"Array":false}"#,
        )
        .unwrap();
        assert_eq!(text.collation(), tidb_datatype::Collation::Utf8Mb4GeneralCi);
        let mut c = ColumnInfo::new(2, "c", text);
        c.offset = 1;
        let table = TableInfo {
            id: 201,
            name: CiString::new("ci"),
            columns: vec![column(1, 0, "id", true), c].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        ClusterCatalog {
            schema_version: 7,
            databases: vec![tidb_exec::cluster_catalog::LoadedDatabase {
                info: DBInfo {
                    id: 5,
                    name: CiString::new("app"),
                    ..DBInfo::default()
                },
                tables: vec![table],
            }],
        }
    }

    /// A `utf8mb4_general_ci` column loaded FROM THE CLUSTER keeps its
    /// collation all the way into execution: `cluster_table` copies the
    /// stored `FieldType`, the planner's `Column` carries it, and
    /// `collation_of_node` reads the collation name back off that type. So
    /// `ORDER BY`, `SELECT DISTINCT` and `GROUP BY` all run case-insensitively
    /// on the convergence node, where a byte-ordered comparator would give
    /// `B, C, a, b` and four groups.
    ///
    /// (This is the tier the bounded node's `configured_string_is_binary`
    /// gate exists to protect: THAT node's `compare_prepared_rows` hardcodes
    /// `utf8mb4_bin`, so it refuses such a column at load. The wide path
    /// needs no gate because it never reaches that comparator.)
    #[test]
    fn a_case_insensitive_cluster_column_orders_groups_and_dedups_by_its_collation() {
        let (storage, _, _) = cluster_storage();
        let (mut session, skipped) = session_with_cluster_storage(
            &ci_catalog(),
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        assert!(skipped.is_empty(), "a _ci table is served, not refused");
        session.run("USE app").unwrap();
        session
            .run("INSERT INTO ci (id, c) VALUES (1,'a'),(2,'B'),(3,'b'),(4,'C')")
            .unwrap();

        let text = |result: Result<StmtResult, _>| -> Vec<String> {
            let StmtResult::Rows(rows) = result.unwrap_or_else(|_| panic!("expected rows")) else {
                panic!("expected rows");
            };
            rows.iter()
                .map(|row| {
                    row.iter()
                        .map(|d| match d {
                            tidb_datatype::Datum::Bytes(b) => {
                                String::from_utf8_lossy(b).into_owned()
                            }
                            tidb_datatype::Datum::String(s) => {
                                String::from_utf8_lossy(s.bytes()).into_owned()
                            }
                            other => format!("{other:?}"),
                        })
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect()
        };

        // ORDER BY: `_ci` folds case, and equal keys keep insertion order.
        assert_eq!(
            text(session.run("SELECT c FROM ci ORDER BY c")),
            vec!["a", "B", "b", "C"]
        );
        // DISTINCT: 'b' folds into 'B', so three rows, not four.
        assert_eq!(
            text(session.run("SELECT DISTINCT c FROM ci ORDER BY c")),
            vec!["a", "B", "C"]
        );
        // GROUP BY: the same identity gives the folded pair one group of 2.
        assert_eq!(
            text(session.run("SELECT c, COUNT(*) FROM ci GROUP BY c ORDER BY c")),
            vec!["a|Int(1)", "B|Int(2)", "C|Int(1)"]
        );
        // WHERE string equality and a self-join key derive the same collation.
        assert_eq!(
            text(session.run("SELECT COUNT(*) FROM ci WHERE c = 'B'")),
            vec!["Int(2)"]
        );
        assert_eq!(
            text(session.run("SELECT COUNT(*) FROM ci a, ci b WHERE a.c = b.c")),
            vec!["Int(6)"]
        );
    }

    #[test]
    fn a_snapshot_row_is_visible_and_shadowed_by_a_staged_write() {
        let (storage, _, snapshot) = cluster_storage();
        let (mut session, _) = session_with_cluster_storage(
            &loaded_catalog(),
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        session.run("USE app").unwrap();
        // Write one row through the driver, promote its staged bytes into the
        // snapshot, and start over: that is what a COMMIT leaves behind for the
        // next statement to read.
        session.run("INSERT INTO t (id, v) VALUES (9, 90)").unwrap();
        let staged = storage.buffer().staged();
        assert_eq!(staged.len(), 1);
        for (key, value) in staged {
            snapshot
                .lock()
                .unwrap()
                .data
                .insert(key.into_bytes(), value.expect("a row write, not a delete"));
        }
        storage.buffer().reset();

        let StmtResult::Rows(rows) = session.run("SELECT id, v FROM t").unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(format!("{rows:?}"), "[[Int(9), Int(90)]]");

        // The same row updated in this statement reads back the staged value,
        // while the committed bytes underneath stay as they were.
        session.run("UPDATE t SET v = 91 WHERE id = 9").unwrap();
        let StmtResult::Rows(rows) = session.run("SELECT v FROM t").unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(format!("{rows:?}"), "[[Int(91)]]");
        assert_eq!(snapshot.lock().unwrap().data.len(), 1);
    }

    /// An index on a column, as the cluster stores it. `prefix` is Go's
    /// `IndexColumn.Length`, which is `UnspecifiedLength` (-1) for a whole
    /// column and a byte count for a prefix index.
    fn index(id: i64, name: &str, column: &str, offset: i64, prefix: i64) -> IndexInfo {
        IndexInfo {
            id,
            name: CiString::new(name),
            columns: vec![IndexColumn {
                name: CiString::new(column),
                offset,
                length: prefix,
                ..IndexColumn::default()
            }]
            .into(),
            state: SchemaState::PUBLIC,
            ..IndexInfo::default()
        }
    }

    fn one_table_catalog(table: TableInfo) -> ClusterCatalog {
        ClusterCatalog {
            schema_version: 7,
            databases: vec![tidb_exec::cluster_catalog::LoadedDatabase {
                info: DBInfo {
                    id: 5,
                    name: CiString::new("app"),
                    ..DBInfo::default()
                },
                tables: vec![table],
            }],
        }
    }

    /// The table this loader used to refuse by name. It is served now: the
    /// counter it needs no longer starts at zero in this process, it comes
    /// from the node's registry over the cluster's own meta key.
    #[test]
    fn a_cluster_columns_auto_increment_is_served_with_a_counter_from_the_node() {
        let mut v = column(2, 1, "v", false);
        v.field_type
            .add_flags(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT | 1);
        let table = TableInfo {
            id: 401,
            name: CiString::new("ai"),
            columns: vec![column(1, 0, "id", true), v].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let (storage, _, _) = cluster_storage();
        let homes = LocalTableAutoIds::default();
        let built = cluster_session_catalog(
            &one_table_catalog(table.clone()),
            &storage,
            &StatsSnapshot::new(),
            &homes,
        );
        assert!(
            built.skipped.is_empty(),
            "the table is served now the counter has a home: {:?}",
            built.skipped
        );
        // The allocator handed to a rebuilt table is the SAME one, so the ids
        // it already reserved are not abandoned. This is the property that
        // makes a per-statement catalog rebuild invisible in the id sequence.
        let first = homes.allocator_for(1, &table);
        let second = homes.allocator_for(1, &table);
        assert!(first.same_allocator_as(&second));
    }

    #[test]
    fn auto_random_uses_a_distinct_stable_allocator() {
        let table = TableInfo {
            id: 402,
            name: CiString::new("ar"),
            columns: vec![column(1, 0, "id", true)].into(),
            pk_is_handle: true,
            auto_random_bits: 5,
            auto_random_range_bits: 64,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let homes = LocalTableAutoIds::default();
        let row = homes.allocator_for(1, &table);
        let random = homes.random_allocator_for(1, &table);
        let random_again = homes.random_allocator_for(1, &table);
        assert!(!row.same_allocator_as(&random));
        assert!(random.same_allocator_as(&random_again));
    }

    /// A table without a primary key uses the hidden `_tidb_rowid`. Separate
    /// SQL sessions on one node must draw those handles from one allocator,
    /// or concurrent inserts reuse the same record key and one row overwrites
    /// the other.
    #[test]
    fn non_clustered_tables_share_the_hidden_rowid_allocator() {
        let table = TableInfo {
            id: 403,
            name: CiString::new("no_pk"),
            columns: vec![column(1, 0, "v", false)].into(),
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let catalog = one_table_catalog(table);
        let (storage, buffer, _) = cluster_storage();
        let homes = LocalTableAutoIds::default();

        let (mut first, first_skipped) =
            session_with_cluster_storage(&catalog, &storage, &StatsSnapshot::new(), &homes);
        let (mut second, second_skipped) =
            session_with_cluster_storage(&catalog, &storage, &StatsSnapshot::new(), &homes);
        assert!(first_skipped.is_empty());
        assert!(second_skipped.is_empty());
        first.run("USE app").unwrap();
        second.run("USE app").unwrap();

        first.run("INSERT INTO no_pk (v) VALUES (10)").unwrap();
        second.run("INSERT INTO no_pk (v) VALUES (20)").unwrap();

        assert_eq!(buffer.len(), 2, "peer sessions must not overwrite rowids");
        let StmtResult::Rows(rows) = first.run("SELECT v FROM no_pk ORDER BY v").unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(format!("{rows:?}"), "[[Int(10)], [Int(20)]]");
    }

    /// The `CREATE INDEX`/`DROP INDEX` backfill walks the rows a table already
    /// has and never allocates, which is why it is given no counter -- and
    /// this is the proof rather than the assumption.
    ///
    /// The table is deliberately one WITH an `AUTO_INCREMENT` column, because
    /// that is the only case where a counter is installed at all. It is loaded
    /// twice from one storage, exactly as production does: once as a session's
    /// table, which inserts the rows, and once as
    /// [`AutoIdSource::Unavailable`], which is the shape
    /// `KvTableIndexBackfiller::stage` builds. Both index walks then succeed
    /// over a counter that answers every request with an error, so neither can
    /// have asked it for anything.
    ///
    /// The last assertion is what keeps the test from being vacuous: it shows
    /// the counter really does refuse, so the two passes above are evidence and
    /// not an allocator that quietly worked. Should `create_index` ever grow a
    /// path that allocates, this test fails loudly instead of that path reading
    /// a counter with no home.
    #[test]
    fn the_index_backfill_never_asks_the_counter_it_was_not_given() {
        let mut v = column(2, 1, "v", false);
        v.field_type
            .add_flags(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT | 1);
        let table = TableInfo {
            id: 402,
            name: CiString::new("ai"),
            columns: vec![column(1, 0, "id", true), v].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let (storage, buffer, _snapshot) = cluster_storage();

        // The rows the backfill will walk, inserted the way a session inserts
        // them -- through a real allocator, since an INSERT genuinely does
        // allocate.
        let (mut session, _) = session_with_cluster_storage(
            &one_table_catalog(table.clone()),
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        session.run("USE app").unwrap();
        session
            .run("INSERT INTO ai (id) VALUES (1), (2), (3)")
            .unwrap();
        assert_eq!(buffer.len(), 3, "three rows are staged for the walk");

        // Now the backfill's own view of the same table: no counter at all.
        let mut walked = cluster_table(&table, &storage, &AutoIdSource::Unavailable)
            .expect("the backfill builds this table");
        let columns: Vec<_> = table.cols().iter_deref().collect();
        let index = kv_index(&index(1, "vi", "v", 1, -1), &columns).expect("a full-value index");

        walked
            .create_index_with_context(index, &tidb_executor::StmtContext::default())
            .expect("a backfill that allocates nothing needs no allocator");
        assert!(
            walked
                .drop_index("vi", &tidb_datatype::SessionTimeZone::utc())
                .expect("the removal walk also allocates nothing"),
            "the index it just created is the one it removes"
        );

        // Not vacuous: the counter these two walks held really does refuse.
        assert!(
            walked.rebase_auto_increment(9).is_err(),
            "a counter with no home must report that, not hand out an id"
        );
    }

    /// A prefix index stores each value CUT to the prefix length. Nothing on
    /// this side of the seam cuts, so a range built from whole values is a
    /// SUBSET of what the index holds and matches nothing -- rows would go
    /// missing with no error. The table is refused, by name and with a reason,
    /// rather than answering wrongly.
    ///
    /// Before this refusal the index loaded as an ordinary full-value one and
    /// the planner picked it, so the query below returned zero rows.
    #[test]
    fn a_table_with_a_prefix_index_is_refused_by_name() {
        let (storage, _buffer, _snapshot) = cluster_storage();
        let table = TableInfo {
            id: 201,
            name: CiString::new("p"),
            columns: vec![column(1, 0, "id", true), column(2, 1, "s", false)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            indices: vec![index(1, "idx", "s", 1, 4)].into(),
            ..TableInfo::default()
        };
        let (mut session, skipped) = session_with_cluster_storage(
            &one_table_catalog(table),
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].name, "app.p");
        assert!(
            skipped[0]
                .reason
                .contains("its index idx is a prefix index"),
            "{}",
            skipped[0].reason
        );
        session.run("USE app").unwrap();
        // Refused loudly: the table is not there at all, rather than there and
        // answering with rows missing.
        assert!(session
            .run("SELECT id FROM p WHERE s = 'alphabet'")
            .is_err());
    }

    /// Go never chooses an INVISIBLE index for an access path (captured:
    /// `EXPLAIN` shows a full table scan, and `USE INDEX(inv)` is
    /// `[planner:1176]Key 'inv' doesn't exist in table 'iv'`). The index is
    /// still loaded and still maintained by writes -- only the planner is
    /// blind to it -- and the full table scan answers the query exactly.
    ///
    /// Before this, `IndexInfo.Invisible` was dropped on the floor and the
    /// cost-based chooser preferred the invisible index's cheap point range.
    #[test]
    fn an_invisible_index_is_loaded_but_never_chosen() {
        let table = TableInfo {
            id: 202,
            name: CiString::new("iv"),
            columns: vec![column(1, 0, "id", true), column(2, 1, "a", false)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            indices: vec![index(1, "inv", "a", 1, -1)].into(),
            ..TableInfo::default()
        };
        table
            .indices
            .get(0)
            .expect("the fixture has its index")
            .write()
            .comment = "cluster comment".to_owned();
        let invisible = table.clone_like_go();
        invisible
            .indices
            .get(0)
            .expect("the fixture has its index")
            .write()
            .invisible = true;

        for (info, expect_plan_index) in [(table, true), (invisible, false)] {
            // Each pass gets its own storage: the two catalogs describe the
            // same table id, so a shared buffer would collide on the handle.
            let (storage, _buffer, _snapshot) = cluster_storage();
            let (mut session, skipped) = session_with_cluster_storage(
                &one_table_catalog(info),
                &storage,
                &StatsSnapshot::new(),
                &LocalTableAutoIds::default(),
            );
            assert!(skipped.is_empty());
            session.run("USE app").unwrap();
            session
                .run("INSERT INTO iv (id, a) VALUES (1, 10), (2, 20), (3, 30)")
                .unwrap();
            // The answer is the same either way: with the index hidden the
            // full table scan still returns exactly the matching row.
            let StmtResult::Rows(rows) = session.run("SELECT id FROM iv WHERE a = 20").unwrap()
            else {
                panic!("expected rows");
            };
            assert_eq!(format!("{rows:?}"), "[[Int(2)]]");

            // The index is loaded and maintained either way; only the
            // planner's view of it differs.
            let catalog = session.shared_catalog();
            let catalog = catalog.lock().unwrap();
            let tidb_executor::driver::TableEntry::Kv(kv) = catalog
                .table_in("app", "iv")
                .expect("the table is in the catalog")
            else {
                panic!("a cluster table is a kv table");
            };
            assert_eq!(kv.indexes().len(), 1);
            assert_eq!(kv.indexes()[0].comment, "cluster comment");
            assert_eq!(kv.indexes()[0].visible, expect_plan_index);
            assert_eq!(kv.plan_indexes().count(), usize::from(expect_plan_index));
        }
    }
}

/// Whether a stored declared default is Go's clock marker on a temporal
/// column: the word `CURRENT_TIMESTAMP`, optionally with the column's
/// fractional precision (`CURRENT_TIMESTAMP(3)`).
fn stored_clock_marker(column: &tidb_model::ColumnInfo) -> Option<Option<String>> {
    if !matches!(
        column.field_type.code(),
        tidb_datatype::FieldTypeCode::Timestamp | tidb_datatype::FieldTypeCode::Datetime
    ) {
        return None;
    }
    let declared = column.get_default_value();
    let tidb_model::GoAnyView::String(bytes) = declared.view()? else {
        return None;
    };
    let text = String::from_utf8_lossy(bytes.as_bytes()).into_owned();
    let upper = text.to_ascii_uppercase();
    if upper == "CURRENT_TIMESTAMP" {
        return Some(None);
    }
    let fsp = upper
        .strip_prefix("CURRENT_TIMESTAMP(")?
        .strip_suffix(')')?
        .to_owned();
    fsp.chars().all(|c| c.is_ascii_digit()).then_some(())?;
    Some(Some(fsp))
}
