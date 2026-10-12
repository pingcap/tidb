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

//! Go `tables.TableFromMeta`: one stored `TableInfo` turned into the table
//! the executor reads and writes, over cluster storage, together with the
//! auto-id counters that belong to it.
//!
//! It lives beside the DDL planner rather than in the server so the planner
//! can build the same table the loader serves -- a partition reorganization
//! plans against the table's indexes and handle, which a metadata-only
//! stand-in does not carry.

use std::sync::{Arc, Mutex};
use tidb_datatype::FieldTypeFlags;
use tidb_executor::cluster_storage::ClusterTableStorage;
use tidb_executor::driver::SequenceDef;
use tidb_executor::kv_table::{KvColumn, KvForeignKey, KvIndex, KvTable, TableAutoId};
use tidb_executor::storage::TableStorage;
use tidb_model::{GoAnyView, GoShared, SchemaState, TableInfo};

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

    /// Hidden handles use RowIDAllocType, distinct when SepAutoInc applies.
    fn row_allocator_for(&self, db_id: i64, table: &TableInfo) -> TableAutoId {
        self.allocator_for(db_id, table)
    }

    /// The live, distinct AUTO_RANDOM allocator for `table`.
    fn random_allocator_for(&self, db_id: i64, table: &TableInfo) -> TableAutoId;

    /// The live allocator for a SEQUENCE stored in this database, so the
    /// sequence can be loaded into the catalog rather than skipped. Go keeps
    /// the counter in meta keys of its own (`SequenceValue`/`SequenceCycle`),
    /// exactly like an auto-increment counter, and keeps the allocator ALIVE
    /// for as long as the node -- one reserved cache batch belongs to whoever
    /// reserved it, so rebuilding it per catalog reload would throw away the
    /// ids it still held. The default of `None` is the tier that has no
    /// sequence counter at all.
    ///
    /// `table` is a stored `TableInfo` whose `sequence` field is set.
    fn sequence_allocator_for(&self, _db_id: i64, _table: &TableInfo) -> Option<SequenceDef> {
        None
    }
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

/// Which of a table's counters an allocator serves (Go
/// `autoid.AllocatorType`).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum AutoIdKind {
    /// `AutoIncrementType`.
    AutoIncrement,
    /// `RowIDAllocType`, distinct from AUTO_INCREMENT under `SepAutoInc`.
    RowId,
    /// `AutoRandomType`.
    AutoRandom,
}

type LocalTableAutoIdMap = std::collections::HashMap<(i64, AutoIdKind), (i64, TableAutoId)>;

fn auto_id_step(table: &TableInfo) -> u64 {
    if table.auto_id_cache > 0 {
        table.auto_id_cache as u64
    } else {
        tidb_executor::kv_table::DEFAULT_AUTO_ID_STEP
    }
}

impl TableAutoIds for LocalTableAutoIds {
    fn allocator_for(&self, _db_id: i64, table: &TableInfo) -> TableAutoId {
        local_allocator_for(&self.allocators, table, AutoIdKind::AutoIncrement)
    }

    fn row_allocator_for(&self, _db_id: i64, table: &TableInfo) -> TableAutoId {
        local_allocator_for(
            &self.allocators,
            table,
            if table.sep_auto_inc() {
                AutoIdKind::RowId
            } else {
                AutoIdKind::AutoIncrement
            },
        )
    }
    fn random_allocator_for(&self, _db_id: i64, table: &TableInfo) -> TableAutoId {
        local_allocator_for(&self.allocators, table, AutoIdKind::AutoRandom)
    }
}

fn local_allocator_for(
    allocators: &Mutex<LocalTableAutoIdMap>,
    table: &TableInfo,
    kind: AutoIdKind,
) -> TableAutoId {
    let mut allocators = allocators.lock().expect("local auto id map poisoned");
    if let Some((cache, allocator)) = allocators.get(&(table.id, kind)) {
        if *cache == table.auto_id_cache {
            return allocator.clone();
        }
        let allocator = allocator.with_step(if kind == AutoIdKind::RowId {
            tidb_executor::kv_table::DEFAULT_AUTO_ID_STEP
        } else {
            auto_id_step(table)
        });
        allocators.insert((table.id, kind), (table.auto_id_cache, allocator.clone()));
        return allocator;
    }
    let allocator = TableAutoId::over(
        Arc::new(tidb_executor::kv_table::LocalAutoIdStore::new()),
        if kind == AutoIdKind::RowId {
            tidb_executor::kv_table::DEFAULT_AUTO_ID_STEP
        } else {
            auto_id_step(table)
        },
    );
    allocators.insert((table.id, kind), (table.auto_id_cache, allocator.clone()));
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

    fn row_allocator_for(&self, table: &TableInfo) -> TableAutoId {
        match self {
            AutoIdSource::In { db_id, ids } => ids.row_allocator_for(*db_id, table),
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
pub fn cluster_table(
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
    // Go retains ColumnInfo.Hidden separately from column identity. The
    // executor's native layout keeps visible columns first; derive every
    // expression/index/handle offset from this one ordering, not stored
    // Go offsets. Stable partitioning preserves visible SQL column order.
    let (mut columns, hidden): (Vec<_>, Vec<_>) = table
        .cols()
        .iter_deref()
        .partition(|column| !column.read().hidden);
    let visible_count = columns.len();
    columns.extend(hidden);
    // A partitioned table is loadable when its partitioning is one this node
    // can rebuild from METADATA. `partition_spec_from_metadata` reads Go's
    // stored `PartitionInfo` -- the restored expression text, the columns,
    // and the definitions with their physical ids -- and returns the same
    // `PartitionSpec` a `CREATE TABLE` here would have built, so the read
    // path routes by the identical expression either way.
    //
    // Anything it cannot rebuild still skips, and the reason it gives says
    // WHICH kind and why, rather than the blanket "it is partitioned" this
    // replaced: RANGE and LIST keep their bounds folded, so loading them
    // means folding every stored bound text first.
    // Go `TableInfo.GetPartitionInfo`: metadata that is present but not
    // ENABLED is not partitioning. `ALTER TABLE ... REMOVE PARTITIONING`
    // leaves the record behind with `Enable` false, and Go then builds an
    // ordinary table -- routing rows by a spec Go ignores would send every
    // write to the wrong physical table.
    let partition_spec = match &table.partition {
        Some(partition) if partition.read().enable => {
            Some(partition_spec_for(&columns, &partition.read())?)
        }
        _ => None,
    };
    if table.state != SchemaState::PUBLIC {
        return Err(format!(
            "its schema state is {} rather than public",
            table.state.0
        ));
    }
    if columns.is_empty() {
        return Err("it has no public columns".to_owned());
    }
    // Go TableFromMeta rebuilds generated expressions against the complete
    // stored schema. The wide loader does not use the bounded reader's
    // admission rules, so it must retain this metadata itself.
    let generated_schema = columns
        .iter()
        .any(|column| column.read().is_generated())
        .then(|| {
            let names: Vec<String> = columns
                .iter()
                .map(|column| column.read().name.original().to_owned())
                .collect();
            let types: Vec<_> = columns
                .iter()
                .map(|column| column.read().field_type.clone())
                .collect();
            (names, types)
        });
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
    // refuses the whole table by name.
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
        } else if !column.default_is_expr
            && matches!(
                column.get_type(),
                tidb_datatype::FieldTypeCode::Date
                    | tidb_datatype::FieldTypeCode::Datetime
                    | tidb_datatype::FieldTypeCode::Timestamp
            )
            && matches!(
                column.default_value.view(),
                Some(GoAnyView::String(bytes))
                    if bytes.as_bytes().eq_ignore_ascii_case(b"CURRENT_DATE")
            )
        {
            // Go stores `DEFAULT (CURRENT_DATE)` as the word with
            // `DefaultIsExpr` false, like `CURRENT_TIMESTAMP`; every INSERT
            // evaluates it. Only a time column's word is the clock: Go's
            // `getColDefaultValue` reads it through `GetTimeValue` for
            // DATE/DATETIME/TIMESTAMP alone, and a string column's
            // `DEFAULT 'current_date'` is that text.
            let marker = tidb_ast::Expr::Func {
                name: "CURRENT_DATE".to_owned(),
                args: Vec::new(),
                origin_position: 0,
            };
            Some(
                tidb_executor::column_default::build(&marker, &column.field_type, |_| {
                    Err(tidb_executor::column_default::DefaultError::Unsupported(
                        "a clock marker never folds",
                    ))
                })
                .map_err(|error| format!("its column {name} has a default {error:?}"))?,
            )
        } else if column.default_is_expr {
            // Go stores the expression spelling with `default_is_expr`;
            // every INSERT re-runs it for omitted columns. Carry it as a
            // computed default like the clock markers — the literal
            // fallback refuses the whole table, which made every table with
            // an expression default unservable here (oracle m3-catalog:
            // c13(a json DEFAULT (json_array())) vanished right after its
            // CREATE).
            let Some(GoAnyView::String(bytes)) = column.default_value.view() else {
                return Err(format!(
                    "its column {name} has an expression default with no stored text"
                ));
            };
            let text = String::from_utf8_lossy(bytes.as_bytes()).into_owned();
            let parsed = tidb_parser::parse(&format!("SELECT {text}")).map_err(|error| {
                format!("its column {name} has an unparseable default {error:?}")
            })?;
            let tidb_ast::Stmt::Query(query) = parsed else {
                return Err(format!("its column {name} has a non-expression default"));
            };
            let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
                return Err(format!("its column {name} has a non-expression default"));
            };
            let Some(tidb_ast::SelectField::Expr { expr, .. }) = select.fields.first() else {
                return Err(format!("its column {name} has a non-expression default"));
            };
            let rewritten = tidb_expr::rewriter::rewrite_expr(expr).map_err(|error| {
                format!("its column {name} has an unresolvable default {error:?}")
            })?;
            Some(tidb_executor::column_default::ColumnDefault::Computed(
                Box::new(tidb_executor::column_default::ComputedDefault {
                    text,
                    kind: tidb_executor::column_default::ComputedDefaultKind::Expression,
                    expr: rewritten,
                    added_origin_safety: tidb_executor::column_default::AddedOriginSafety::Safe,
                }),
            ))
        } else {
            // The loader refuses a computed default above, so what survives
            // is always a settled value.
            Some(tidb_executor::column_default::ColumnDefault::Value(
                crate::system_row_write::literal_default(&column, table.name.original())
                    .map_err(|error| format!("its column {name} has a default {error}"))?,
            ))
        };
        let origin_default_value = column.get_origin_default_value();
        let origin_default = if origin_default_value.is_nil() {
            None
        } else {
            Some(
                crate::system_row_write::origin_default(&column, table.name.original()).map_err(
                    |error| format!("its column {name} has an original default {error}"),
                )?,
            )
        };
        let generated = if column.is_generated() {
            let expression =
                tidb_model::generated_expr::parse_expression(&column.generated_expr_string)
                    .map_err(|error| {
                        format!(
                            "its generated column {name} cannot be parsed: {}",
                            error.message
                        )
                    })?;
            let (names, types) = generated_schema
                .as_ref()
                .expect("generated schema collected");
            Some(
                tidb_executor::generated_column::build_generated_expr(
                    &expression,
                    column.generated_stored,
                    names,
                    types,
                    &tidb_datatype::SessionTimeZone::utc(),
                )
                .map_err(|error| {
                    format!("its generated column {name} cannot be built: {error:?}")
                })?,
            )
        } else {
            None
        };
        kv_columns.push(KvColumn {
            name,
            id: column.id,
            field_type: column.field_type.clone(),
            column_info_version: column.version,
            // Go `ColumnInfo.Comment`, which `SHOW CREATE TABLE` prints and
            // `information_schema.columns.column_comment` reports.
            comment: column.comment.clone(),
            generated,
            default_value,
            origin_default,
        });
    }
    let hidden = kv_columns.split_off(visible_count);
    let mut kv_table = KvTable::with_storage(table.id, kv_columns, storage.clone_box());
    kv_table.set_all_columns_public(
        table
            .columns
            .iter_deref()
            .all(|column| column.read().state == SchemaState::PUBLIC),
    );
    for column in hidden {
        kv_table.add_hidden_column(column);
    }
    kv_table.set_name(table.name.original());
    let constraints = table
        .constraints
        .iter_deref()
        .map(|constraint| constraint.read().clone())
        .collect::<Vec<_>>();
    kv_table
        .set_check_constraint_infos(constraints, &tidb_datatype::SessionTimeZone::utc(), b'\\')
        .map_err(|error| format!("its check constraints cannot be built: {error:?}"))?;
    kv_table.set_max_constraint_id(table.max_constraint_id);
    kv_table.set_max_column_id(table.max_column_id);
    kv_table.set_max_index_id(table.max_index_id);
    // Go `TableInfo.ForeignKeys` ride the loaded `TableInfo` into every
    // `tables.Table`: the planner's FK triggers (`physicalop/foreign_key.go`)
    // build their `FKCheck`/`FKCascade` EXPLAIN leaves from them and the FK
    // check execs resolve parents through them. Retain public legacy keys too:
    // metadata readers need them, while trigger planning and referred-key
    // lookup apply Go's version gate at their own boundaries.
    for fk in table.foreign_keys.iter_deref() {
        let fk = fk.read();
        if fk.state != SchemaState::PUBLIC {
            continue;
        }
        kv_table.add_foreign_key(KvForeignKey {
            version: fk.version,
            name: fk.name.original().to_owned(),
            cols: fk
                .cols
                .snapshot()
                .into_iter()
                .map(|column| column.original().to_owned())
                .collect(),
            ref_schema: fk.ref_schema.original().to_owned(),
            ref_table: fk.ref_table.original().to_owned(),
            ref_cols: fk
                .ref_cols
                .snapshot()
                .into_iter()
                .map(|column| column.original().to_owned())
                .collect(),
            on_delete: fk_action_from_stored(fk.on_delete),
            on_update: fk_action_from_stored(fk.on_update),
        });
    }
    // Go `TableInfo.Comment` reaches every reader of the loaded table:
    // `SHOW CREATE TABLE` prints it and
    // `information_schema.tables.table_comment` reports it. Dropping it
    // here made a stored comment invisible on the cluster path even though
    // both the DDL that wrote it and the printer that renders it were
    // right.
    kv_table.set_comment(table.comment.clone());
    // Go `TableInfo.PlacementPolicyRef`. A table a Go cluster wrote can name
    // a policy; dropping it here would mean this node stores the table back
    // WITHOUT the reference, erasing a policy binding Go put there -- the
    // same loss the partition-level reference and the partition comment each
    // had before they were carried through.
    kv_table.set_placement_policy(
        table
            .placement_policy_ref
            .as_ref()
            .map(|reference| reference.read().clone()),
    );
    // Go `TableInfo.AutoIDCache`. The allocator's step already comes from it
    // (`auto_id_step`); this is the value `SHOW CREATE TABLE` prints back.
    kv_table.set_recorded_auto_id_cache(table.auto_id_cache);
    // Go `TableInfo.Charset`/`Collate`: the table default `SHOW CREATE TABLE`
    // prints, what `information_schema.tables.table_collation` reports, and
    // the value each column's own charset is compared against when deciding
    // whether to print an explicit CHARACTER SET clause. Dropping it made a
    // table created as latin1 read back as utf8mb4 while its columns
    // correctly reported latin1 -- a definition that does not round-trip.
    kv_table.set_charset(tidb_executor::TableCharset {
        charset: tidb_datatype::Charset::from_name(&table.charset)
            .unwrap_or(tidb_executor::TableCharset::default().charset),
        collation: tidb_datatype::Collation::from_name(&table.collate)
            .unwrap_or(tidb_executor::TableCharset::default().collation),
    });
    kv_table.set_cache_status(table.table_cache_status_type);
    // Go `TableInfo.Lock`: a lock another TiDB took through its own lock job
    // is what this node's `CheckTableLock` must honor too.
    kv_table.set_lock(table.lock.as_ref().map(|lock| lock.read().clone()));
    kv_table.set_tiflash_replica(
        table
            .tiflash_replica
            .as_ref()
            .map(|replica| replica.read().clone()),
    );
    // Go `TableInfo.TempTableType`, which the meta store really does carry:
    // a GLOBAL temporary table is created by an ordinary DDL job and its
    // `TableInfo` is persisted like any other. Dropping it here would make
    // such a table read back as a permanent one -- `SHOW CREATE TABLE` would
    // lose both the `GLOBAL TEMPORARY` header and the `ON COMMIT DELETE ROWS`
    // clause, and every 8006 refusal keyed on the kind would stop firing.
    // (A LOCAL temporary table is in no meta store, so it can never arrive
    // through this loader.)
    kv_table.set_temp_table_type(table.temp_table_type);
    // Go `TableInfo.AutoIncID`, which a GLOBAL temporary table's in-memory
    // allocator starts from (`NewAllocatorFromTempTblInfo`); a permanent
    // table's counter lives in its cluster auto-id key instead.
    kv_table.set_auto_inc_id(table.auto_inc_id);
    // Go `TableInfo.ShardRowIDBits`, which `AllocHandleIDs` composes into
    // the high bits of every `_tidb_rowid`, and `PreSplitRegions`.
    kv_table.set_shard_row_id_bits(table.shard_row_id_bits);
    kv_table.set_pre_split_regions(table.pre_split_regions);
    // Go `TableInfo.Compression`, which `SHOW CREATE TABLE` prints.
    kv_table.set_compression(table.compression.clone());
    // Go `TableInfo.TableSplitPolicy` and each `IndexInfo.RegionSplitPolicy`,
    // which `SHOW CREATE TABLE` prints as `/*T![region_split] SPLIT ... */`.
    kv_table.set_table_split_policy(
        table
            .table_split_policy
            .as_ref()
            .map(|policy| policy.read().clone()),
    );
    for index in table.indices.iter_deref() {
        let index = index.read();
        if let Some(policy) = index.region_split_policy.as_ref() {
            kv_table.set_index_split_policy(index.id, policy.read().clone());
        }
    }
    // Go `TableInfo.TTLInfo`, which `SHOW CREATE TABLE` prints.
    kv_table.set_ttl_info(table.ttl_info.as_ref().map(|info| info.read().clone()));
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
    if table.sep_auto_inc() {
        kv_table.set_row_id(auto.row_allocator_for(table));
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
        let (handles, prefix_lengths) = clustered_handle_offsets(table, &columns)?;
        kv_table.set_common_handle_offsets(handles);
        kv_table.set_common_handle_prefix_lengths(prefix_lengths);
        kv_table.set_common_handle_version(table.common_handle_version);
    }
    kv_table.set_affinity(
        table
            .affinity
            .as_ref()
            .map(|affinity| affinity.read().clone()),
    );
    kv_table.set_engine_attribute(table.engine_attribute.clone());
    kv_table.set_storage_class((
        table.storage_class_tier.clone(),
        table.storage_class_transitions.snapshot(),
    ));
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
        // A clustered PRIMARY is the record handle, not a secondary `_i`
        // entry.  Go keeps the IndexInfo in its catalog so SHOW/metadata and
        // the planner can see the key, but its write path never maintains a
        // second index for it.  The entry is therefore loaded FLAGGED: the
        // write path skips a `clustered_primary` index (writing one beside the
        // clustered record corrupted tables shared with a Go TiDB node),
        // while multi-column join NDV estimation still reads its stats.
        let clustered_primary = (table.pk_is_handle || table.is_common_handle)
            && index.name.original().eq_ignore_ascii_case("PRIMARY");
        kv_table.add_index(kv_index(&index, &kv_table.columns)?, clustered_primary);
        // Go `IndexInfo.ConditionExprString`: a partial index's predicate,
        // built as `TableFromMeta` builds it, in the default context.
        if index.has_condition() {
            let condition = index.condition_expr().map_err(|error| {
                format!(
                    "its partial index {} has an unparsable condition: {}",
                    index.name, error.message
                )
            })?;
            kv_table
                .add_partial_index_condition(
                    index.id,
                    index.name.original(),
                    &condition,
                    &tidb_datatype::SessionTimeZone::utc(),
                    b'\\',
                )
                .map_err(|error| {
                    format!(
                        "its partial index {} cannot be built: {error:?}",
                        index.name
                    )
                })?;
        }
        // Go `model.IndexInfo.MVIndex`: DDL set the flag when exactly one key
        // part is an ARRAY-typed hidden column, and that column's
        // `Dependences` names the source it indexes
        // (`buildHiddenColumnInfoWithCheck`, `pkg/ddl/create_table.go`). The
        // pair is what lets a reader rewrite `(v MEMBER OF (source))` onto
        // this index's stored element keys; anything else about the shape is
        // recorded as absent rather than guessed.
        let array_part = index.columns.iter_deref().find_map(|part| {
            let part = part.read();
            let name = part.name.lowercase().to_owned();
            let position = columns
                .iter()
                .position(|column| column.read().name.lowercase() == name.as_str())?;
            let column = columns[position].read();
            let dependences = column.field_type.is_array().then(|| {
                let mut names = column.dependences.snapshot();
                names.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
                names
            })?;
            Some(dependences)
        });
        if let Some(dependences) = array_part {
            if dependences.len() == 1 {
                kv_table.set_mv_key_part_source(index.id, dependences[0].to_utf8_lossy_go());
            }
        }
    }
    if let Some(spec) = partition_spec {
        kv_table.set_partition(spec);
    }
    Ok(kv_table)
}

/// Rebuilds one stored `PartitionInfo` into the routing spec the read path
/// uses, or names why it cannot.
///
/// Go keeps the partition expression in `PartitionInfo.Expr` as restored TEXT
/// and rebuilds the evaluable form when it loads the table
/// (`PartitionExpr`); this is that rebuild. The column names and types come
/// from the table being loaded, so the expression binds to the same offsets
/// the scan decodes.
/// Go `ast.ReferOptionType` over the stored `FKInfo.On{Delete,Update}` int
/// (`pkg/parser/ast/model.go:242`): NoOption 0, Restrict 1, Cascade 2,
/// SetNull 3, NoAction 4, SetDefault 5.
fn fk_action_from_stored(action: i64) -> tidb_executor::kv_table::FkAction {
    use tidb_executor::kv_table::FkAction;
    match action {
        1 => FkAction::Restrict,
        2 => FkAction::Cascade,
        3 => FkAction::SetNull,
        4 => FkAction::NoAction,
        5 => FkAction::SetDefault,
        _ => FkAction::NoOption,
    }
}

fn partition_spec_for(
    table_columns: &[GoShared<tidb_model::column::ColumnInfo>],
    partition: &tidb_model::partition::PartitionInfo,
) -> Result<tidb_executor::partition_routing::PartitionSpec, String> {
    let names: Vec<String> = table_columns
        .iter()
        .map(|column| column.read().name.original().to_owned())
        .collect();
    let types: Vec<tidb_datatype::FieldType> = table_columns
        .iter()
        .map(|column| column.read().field_type.clone())
        .collect();
    let definitions: Vec<tidb_executor::ddl::StoredPartitionDefinition> = partition
        .definitions
        .snapshot()
        .into_iter()
        .map(|definition| tidb_executor::ddl::StoredPartitionDefinition {
            id: definition.id,
            name: definition.name.original().to_owned(),
            comment: definition.comment.clone(),
            less_than: definition.less_than.snapshot(),
            in_values: definition
                .in_values
                .snapshot()
                .into_iter()
                .map(|tuple| tuple.snapshot())
                .collect(),
            // Go's `PartitionDefinition.PlacementPolicyRef`. A definition a
            // Go cluster wrote can name a policy, and dropping it here would
            // mean this node stores the partition back WITHOUT the reference
            // -- the catalog would lose a policy binding that Go put there.
            placement_policy: definition
                .placement_policy_ref
                .as_ref()
                .map(|reference| reference.read().clone()),
            storage_class: (
                definition.storage_class_tier.clone(),
                definition.storage_class_transitions.snapshot(),
            ),
        })
        .collect();
    let columns: Vec<String> = partition
        .columns
        .snapshot()
        .into_iter()
        .map(|column| column.original().to_owned())
        .collect();
    let overlapping_dropping_partition_indices = (0..definitions.len())
        .map(|index| {
            usize::try_from(partition.get_overlapping_dropping_partition_idx(index as isize)).ok()
        })
        .collect::<Vec<_>>();
    tidb_executor::ddl::partition_spec_from_metadata(
        partition.partition_type,
        &partition.expr,
        &columns,
        // Go `PartitionInfo.IsEmptyColumns`: the clause was written as
        // `PARTITION BY KEY ()` and Go filled the columns in from the key.
        partition.is_empty_columns,
        &definitions,
        &overlapping_dropping_partition_indices,
        &names,
        &types,
    )
    .map_err(|error| format!("its partitioning cannot be rebuilt: {error}"))
}

/// Translates one stored `IndexInfo` into the executor's `KvIndex`, against
/// the loaded executor table's full native column list.
///
/// The offsets are resolved by NAME here even though `IndexColumn.Offset`
/// carries one, because the two count different things: Go's offset is a
/// position in `TableInfo.Columns`, and a `KvIndex`'s is a position in the
/// native columns this loader built, with visible columns before hidden
/// ones. Resolving against that same list also keeps backfill and normal
/// writes from indexing different row slots.
///
/// It is one function rather than a loop inside [`cluster_table`] because the
/// `CREATE INDEX` backfill has to build the very same `KvIndex` for an index
/// the stored table does not carry yet -- and an index whose entries are
/// WRITTEN under one mapping and READ under another is the exact silent wrong
/// answer this tier keeps hunting.
pub fn kv_index(
    index: &tidb_model::index::IndexInfo,
    columns: &[KvColumn],
) -> Result<KvIndex, String> {
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
            .position(|column| column.name.eq_ignore_ascii_case(&name))
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
        global_index_version: index.global_index_version,
        clustered_primary: false,
    })
}

/// The public-column offset and `IndexColumn.Length` of each clustered
/// primary key part, in key order. Go `AddRecord` truncates a prefix part
/// (`tablecodec.TruncateIndexValues`) before encoding the handle, so the
/// lengths decide the record key as much as the offsets do.
fn clustered_handle_offsets(
    table: &TableInfo,
    columns: &[GoShared<tidb_model::column::ColumnInfo>],
) -> Result<(Vec<usize>, Vec<i64>), String> {
    let primary = table
        .indices
        .iter_deref()
        .find(|index| index.read().primary)
        .ok_or_else(|| "it is marked clustered but has no PRIMARY KEY index".to_owned())?;
    let primary_columns = primary.read().columns.clone();
    let mut offsets = Vec::with_capacity(primary_columns.len());
    let mut prefix_lengths = Vec::with_capacity(primary_columns.len());
    for column in primary_columns.iter_deref() {
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
                    "its clustered PRIMARY KEY covers non-public column {}",
                    original_name
                )
            })?;
        offsets.push(offset);
        prefix_lengths.push(length);
    }
    Ok((offsets, prefix_lengths))
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
        // Go stores the marker WORD alone; the fsp lives on the column's own
        // decimal and is re-derived wherever the default is printed. The
        // rebuilt expression therefore has to carry that decimal: Go's
        // `getFuncCallDefaultValue` demands the WRITTEN fsp equal the
        // column's, so handing it a bare word for a `DATETIME(3)` column
        // fails a check that only ever applied to the written spelling --
        // and the loader then refused a table its own DDL had just
        // published, leaving it visible to CREATE (1050) and to nothing else
        // (1146).
        let decimal = column.field_type.decimal();
        return Some((decimal > 0).then(|| decimal.to_string()));
    }
    let fsp = upper
        .strip_prefix("CURRENT_TIMESTAMP(")?
        .strip_suffix(')')?
        .to_owned();
    fsp.chars().all(|c| c.is_ascii_digit()).then_some(())?;
    Some(Some(fsp))
}
