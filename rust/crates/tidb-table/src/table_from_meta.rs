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
use crate::sequence::SequenceDef;
use crate::kv_table::{KvColumn, KvForeignKey, KvIndex, KvTable, TableAutoId};
use tidb_kv::storage::TableStorage;
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
/// (`tidb_autoid::cluster_auto_id`); tests that only need the old in-process
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

    /// Go's infoschema builder recreates a table's allocators for an applied
    /// `ActionRebaseAutoID` (AutoIncrement and RowID) or
    /// `ActionRebaseAutoRandomBase` (AutoRandom) diff, so no range reserved
    /// before the rebase keeps handing out ids -- even when the rebase left
    /// the recorded base where it was (`FORCE AUTO_INCREMENT` to the same
    /// value twice). The default is the tier with no reserved ranges.
    fn rebuild_rebased_allocators(&self, _table_id: i64, _action: tidb_model::ActionType) {}
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
        tidb_autoid::DEFAULT_AUTO_ID_STEP
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
            tidb_autoid::DEFAULT_AUTO_ID_STEP
        } else {
            auto_id_step(table)
        });
        allocators.insert((table.id, kind), (table.auto_id_cache, allocator.clone()));
        return allocator;
    }
    let allocator = TableAutoId::over(
        Arc::new(tidb_autoid::LocalAutoIdStore::new()),
        if kind == AutoIdKind::RowId {
            tidb_autoid::DEFAULT_AUTO_ID_STEP
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
/// [`AutoIdStoreError`]: crate::kv_table::AutoIdStoreError
#[derive(Debug)]
struct UnavailableAutoIdStore;

impl tidb_autoid::AutoIdStore for UnavailableAutoIdStore {
    fn reserve(
        &self,
        _step: u64,
        _unsigned: bool,
    ) -> Result<(u64, u64), tidb_autoid::AutoIdStoreError> {
        Err(unavailable_counter())
    }

    fn next_global(&self) -> Result<u64, tidb_autoid::AutoIdStoreError> {
        Err(tidb_autoid::AutoIdStoreError(
            "the table has no auto-id counter home".to_owned(),
        ))
    }

    fn rebase(
        &self,
        _required: u64,
        _unsigned: bool,
    ) -> Result<(), tidb_autoid::AutoIdStoreError> {
        Err(unavailable_counter())
    }

    fn force_rebase(
        &self,
        _required: u64,
        _unsigned: bool,
    ) -> Result<(), tidb_autoid::AutoIdStoreError> {
        Err(unavailable_counter())
    }

    fn reset(&self) -> Result<(), tidb_autoid::AutoIdStoreError> {
        Err(unavailable_counter())
    }
}

fn unavailable_counter() -> tidb_autoid::AutoIdStoreError {
    tidb_autoid::AutoIdStoreError(
        "this table was built to walk its stored rows, not to insert, so its \
         auto-increment counter was never given a home"
            .to_owned(),
    )
}

/// Go `temptable.newTemporaryTableFromTableInfo`: a LOCAL temporary table's
/// `TableInfo` as the session's table, under the real table id the caller
/// drew from meta's global counter (`GenGlobalID`), public, over the
/// session's own row memory, with the in-memory allocator Go mocks for it
/// (`autoid.NewAllocatorFromTempTblInfo`).
pub fn new_temporary_table_from_table_info(
    mut table: TableInfo,
    table_id: i64,
) -> Result<KvTable, String> {
    table.id = table_id;
    table.state = SchemaState::PUBLIC;
    let mut kv_table = cluster_table(
        &table,
        &tidb_kv::storage::MemTableStorage::new(),
        &AutoIdSource::Unavailable,
    )?;
    kv_table.use_local_temporary_auto_id();
    Ok(kv_table)
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
    storage: &dyn TableStorage,
    auto: &AutoIdSource<'_>,
) -> Result<KvTable, String> {
    if table.is_view() {
        return Err("it is a view".to_owned());
    }
    if table.is_sequence() {
        return Err("it is a sequence".to_owned());
    }
    // Go `TableFromMeta` keeps `TableInfo.Cols()` in their stored order,
    // hidden expression-index columns included.
    let columns: Vec<_> = table.cols().iter_deref().collect();
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
            Some(loaded_partition_spec(&columns, &partition.read())?)
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
    //
    // Go builds every generated column of `tblInfo.Columns`, public or not,
    // so the schema is needed when ANY stored column is generated: a version
    // an `ADD COLUMN ... AS (...)` or a generated column's `DROP COLUMN`
    // publishes mid-job holds a non-public generated column (built by the
    // non-public loop below) while no public column is generated.
    let generated_schema = table
        .columns
        .iter_deref()
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
    // One `KvColumn` per stored column, public or not (the write path
    // carries the non-public ones).
    let build_column = |column: &tidb_model::ColumnInfo| -> Result<KvColumn, String> {
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
                crate::column_default::stored_clock_marker_default(
                    &name,
                    &column.field_type,
                    fsp.as_deref(),
                )
                .map_err(|error| format!("its column {name} has a default: {}", error.message))?,
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
                crate::column_default::build(&marker, &name, &column.field_type, |_| {
                    Err(tidb_terror::MysqlError::unknown(
                        "a clock marker never folds",
                    ))
                })
                .map_err(|error| format!("its column {name} has a default: {}", error.message))?,
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
            // A default that names a column loads, and fails when a row needs
            // it (Go answers 1054 at INSERT).
            let rewritten = match tidb_expr::rewriter::rewrite_expr(expr) {
                Ok(expression) => Ok(expression),
                Err(error @ tidb_exprctx::EvalError::UnknownColumnInClause(..)) => Err(error),
                Err(error) => {
                    return Err(format!(
                        "its column {name} has an unresolvable default {error:?}"
                    ))
                }
            };
            Some(crate::column_default::ColumnDefault::Computed(
                Box::new(crate::column_default::ComputedDefault {
                    text,
                    kind: crate::column_default::ComputedDefaultKind::Expression,
                    expr: rewritten,
                    added_origin_safety: crate::column_default::AddedOriginSafety::Safe,
                }),
            ))
        } else {
            // The loader refuses a computed default above, so what survives
            // is always a settled value.
            Some(crate::column_default::ColumnDefault::Value(
                crate::origin_default::literal_default(&column, table.name.original())
                    .map_err(|error| format!("its column {name} has a default {error}"))?,
            ))
        };
        let origin_default_value = column.get_origin_default_value();
        let origin_default = if origin_default_value.is_nil() {
            None
        } else {
            Some(
                crate::origin_default::origin_default(&column, table.name.original()).map_err(
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
                crate::generated_column::build_generated_expr(
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
        Ok(KvColumn {
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
        })
    };
    for column in &columns {
        kv_columns.push(build_column(&column.read())?);
    }
    let mut kv_table = KvTable::with_storage(table.id, kv_columns, storage.clone_box());
    kv_table.set_meta(table.clone());
    kv_table.set_all_columns_public(
        table
            .columns
            .iter_deref()
            .all(|column| column.read().state == SchemaState::PUBLIC),
    );
    // Go `TableFromMeta` keeps every column that is not `StateNone`: one a
    // DDL is adding, dropping or changing is written by its state, and a
    // public column a MODIFY COLUMN is checking carries its changing type.
    let public_ids: Vec<i64> = columns.iter().map(|column| column.read().id).collect();
    for column in table.columns.iter_deref() {
        let column = column.read();
        if column.state == SchemaState::PUBLIC {
            if let (Some(changing), Some(offset)) = (
                column.changing_field_type.as_ref(),
                public_ids.iter().position(|id| *id == column.id),
            ) {
                kv_table.set_changing_field_type(offset, changing.read().clone());
            }
            continue;
        }
        if column.state == SchemaState::NONE {
            continue;
        }
        // `DependencyColumnOffset` addresses `TableInfo.Columns`; the write
        // row addresses the public columns by this loader's order.
        let dependency = column.change_state_info.as_ref().and_then(|info| {
            let offset = usize::try_from(info.read().dependency_column_offset).ok()?;
            let id = table.columns.get(offset)?.read().id;
            public_ids.iter().position(|public| *public == id)
        });
        kv_table.add_nonpublic_column(crate::kv_table::NonPublicColumn {
            column: build_column(&column)?,
            state: column.state,
            dependency,
        });
    }
    kv_table.set_name(table.name.original());
    // Go `LoadCheckConstraint` -> `removeInvalidCheckConstraintsInfo`: a
    // CHECK naming a column that is no longer public (DROP COLUMN leaves it
    // in the stored metadata) is dropped lazily here.
    let constraints = table
        .constraints
        .iter_deref()
        .map(|constraint| constraint.read().clone())
        .filter(|constraint| {
            constraint.constraint_cols.iter().all(|name| {
                table.columns.iter_deref().any(|column| {
                    let column = column.read();
                    column.state == SchemaState::PUBLIC
                        && column.name.lowercase() == name.lowercase()
                })
            })
        })
        .collect::<Vec<_>>();
    kv_table
        .set_check_constraint_infos(constraints, &tidb_datatype::SessionTimeZone::utc(), b'\\')
        .map_err(|error| format!("its check constraints cannot be built: {error:?}"))?;
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
    }
    kv_table.set_new_table_global_indexes(
        table
            .partition
            .as_ref()
            .map(|partition| partition.read())
            .filter(|partition| partition.new_table_id != 0)
            .map(|partition| {
                let created = partition
                    .ddl_changed_index
                    .as_ref()
                    .map(|changed| {
                        changed
                            .read()
                            .iter()
                            .filter(|(_, is_new)| **is_new)
                            .map(|(id, _)| *id)
                            .collect()
                    })
                    .unwrap_or_default();
                (partition.new_table_id, created)
            }),
    );
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
        kv_table.set_auto_random(crate::kv_table::AutoRandomSpec {
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
        // Go `IndexColumn.UseChangingType`: the parts cast to their column's
        // changing type.
        kv_table.set_changing_index_parts(
            index.id,
            index
                .columns
                .iter_deref()
                .enumerate()
                .filter(|(_, part)| part.read().use_changing_type)
                .map(|(position, _)| position)
                .collect(),
        );
        // Go `TableFromMeta` keeps every index that is not `StateNone`
        // (`tables.go`): one a DDL is still adding or dropping is maintained
        // by DML according to its state, though no reader sees it. Skipping
        // it left a Go owner's ADD INDEX without this node's writes.
        if index.state == SchemaState::NONE {
            continue;
        }
        if index.state != SchemaState::PUBLIC {
            let clustered_primary = (table.pk_is_handle || table.is_common_handle) && index.primary;
            if clustered_primary {
                continue;
            }
            // A key part may address a column a DDL is adding or changing,
            // which the write row carries past the public columns.
            let write_columns: Vec<KvColumn> = kv_table
                .columns
                .iter()
                .cloned()
                .chain(
                    kv_table
                        .nonpublic_columns()
                        .iter()
                        .map(|column| column.column.clone()),
                )
                .collect();
            let Ok(nonpublic) = kv_index(&index, &write_columns) else {
                continue;
            };
            kv_table.add_nonpublic_index(nonpublic, index.state, index.backfill_state);
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
            continue;
        }
        // A clustered PRIMARY is the record handle, not a secondary `_i`
        // entry.  Go keeps the IndexInfo in its catalog so SHOW/metadata and
        // the planner can see the key, but its write path never maintains a
        // second index for it.  The entry is therefore loaded FLAGGED: the
        // write path skips a `clustered_primary` index (writing one beside the
        // clustered record corrupted tables shared with a Go TiDB node),
        // while multi-column join NDV estimation still reads its stats.
        let clustered_primary = (table.pk_is_handle || table.is_common_handle) && index.primary;
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
    if let Some(partition) = table.partition.as_ref().filter(|p| p.read().enable) {
        kv_table.set_partition_reorg(partition_double_write_for(&columns, &partition.read())?);
    }
    Ok(kv_table)
}

/// Go `newPartitionedTable`'s reorganization setup: during REORGANIZE
/// PARTITION, REMOVE PARTITIONING or ALTER TABLE ... PARTITION BY, which
/// partitions mirror their writes, where the copies go (`reorgPartitionExpr`
/// over the other definition set), and each side's index set. `None` when
/// no such DDL is running.
fn partition_double_write_for(
    table_columns: &[GoShared<tidb_model::column::ColumnInfo>],
    partition: &tidb_model::partition::PartitionInfo,
) -> Result<Option<crate::kv_table::PartitionDoubleWrite>, String> {
    use tidb_model::{ActionType, SchemaState};
    if !matches!(
        partition.ddl_action,
        ActionType::ACTION_REORGANIZE_PARTITION
            | ActionType::ACTION_REMOVE_PARTITIONING
            | ActionType::ACTION_ALTER_TABLE_PARTITIONING
    ) || partition.ddl_state == SchemaState::NONE
    {
        return Ok(None);
    }
    let adding = partition.adding_definitions.snapshot();
    let dropping = partition.dropping_definitions.snapshot();
    let changes_are_public = partition.ddl_state == SchemaState::DELETE_REORGANIZATION
        || partition.ddl_state == SchemaState::PUBLIC;
    // The routing scheme of the copies: the DDL's own type, expression and
    // columns for a new table (REMOVE PARTITIONING / PARTITION BY), the
    // table's for REORGANIZE PARTITION.
    let copies = |definitions: &[tidb_model::partition::PartitionDefinition]| {
        let mut info = partition.clone();
        info.definitions = tidb_goruntime::go_runtime::GoSharedSlice::from_vec(definitions.to_vec());
        if partition.new_table_id != 0 {
            info.partition_type = partition.ddl_type;
            info.expr = partition.ddl_expr.clone();
            info.columns = partition.ddl_columns.clone();
        }
        loaded_partition_spec(table_columns, &info)
    };
    let (spec, reorganize, double_write) = if changes_are_public {
        (
            copies(&dropping)?,
            adding.iter().map(|definition| definition.id).collect(),
            dropping.iter().map(|definition| definition.id).collect(),
        )
    } else {
        if adding.is_empty() {
            return Ok(None);
        }
        (
            copies(&adding)?,
            dropping.iter().map(|definition| definition.id).collect(),
            adding.iter().map(|definition| definition.id).collect(),
        )
    };
    // `DDLChangedIndex`: true = a new index (adding partitions only), false
    // = a replaced one (dropping partitions only).
    let changed = partition
        .ddl_changed_index
        .as_ref()
        .map(|changed| changed.read().clone())
        .unwrap_or_default();
    let mode = |adding_side: bool| crate::kv_table::PartitionIndexMode {
        // Untouched indexes: the side not yet (or no longer) public writes
        // its clone in a non-public state.
        state: match (adding_side, changes_are_public) {
            (true, false) => Some(partition.ddl_state),
            (false, true) => Some(SchemaState::WRITE_ONLY),
            _ => None,
        },
        excluded: changed
            .iter()
            .filter(|(_, is_new)| **is_new != adding_side)
            .map(|(id, _)| *id)
            .collect(),
        changed: changed
            .iter()
            .filter(|(_, is_new)| **is_new == adding_side)
            .map(|(id, _)| *id)
            .collect(),
    };
    let mut index_modes = std::collections::BTreeMap::new();
    for definition in &dropping {
        index_modes.insert(definition.id, mode(false));
    }
    for definition in &adding {
        index_modes.insert(definition.id, mode(true));
    }
    Ok(Some(crate::kv_table::PartitionDoubleWrite {
        spec,
        reorganize,
        double_write,
        delete_only: partition.ddl_state == SchemaState::DELETE_ONLY
            || partition.ddl_state == SchemaState::PUBLIC,
        index_modes,
    }))
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
fn fk_action_from_stored(action: i64) -> crate::kv_table::FkAction {
    use crate::kv_table::FkAction;
    match action {
        1 => FkAction::Restrict,
        2 => FkAction::Cascade,
        3 => FkAction::SetNull,
        4 => FkAction::NoAction,
        5 => FkAction::SetDefault,
        _ => FkAction::NoOption,
    }
}

pub fn partition_spec_for(
    table_columns: &[GoShared<tidb_model::column::ColumnInfo>],
    partition: &tidb_model::partition::PartitionInfo,
) -> Result<crate::partition_routing::PartitionSpec, tidb_terror::MysqlError> {
    let names: Vec<String> = table_columns
        .iter()
        .map(|column| column.read().name.original().to_owned())
        .collect();
    let types: Vec<tidb_datatype::FieldType> = table_columns
        .iter()
        .map(|column| column.read().field_type.clone())
        .collect();
    let definitions: Vec<crate::partition_metadata::StoredPartitionDefinition> = partition
        .definitions
        .snapshot()
        .into_iter()
        .map(|definition| crate::partition_metadata::StoredPartitionDefinition {
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
    crate::partition_metadata::partition_spec_from_metadata(
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
}

/// [`partition_spec_for`] as the loader reports a failure.
fn loaded_partition_spec(
    table_columns: &[GoShared<tidb_model::column::ColumnInfo>],
    partition: &tidb_model::partition::PartitionInfo,
) -> Result<crate::partition_routing::PartitionSpec, String> {
    partition_spec_for(table_columns, partition)
        .map_err(|error| format!("its partitioning cannot be rebuilt: {error}"))
}

/// Translates one stored `IndexInfo` into the executor's `KvIndex`, against
/// the loaded executor table's full native column list.
///
/// The offsets are resolved by NAME here even though `IndexColumn.Offset`
/// carries one, because the two count different things: Go's offset is a
/// position in `TableInfo.Columns`, and a `KvIndex`'s is a position in the
/// native columns this loader built from `TableInfo.Cols()`, the public
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
            .position(|column| tidb_hack::GoCaseCompare::go_lower_eq(column.name.as_str(), &name))
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
        primary: index.primary,
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
pub fn stored_clock_marker(column: &tidb_model::ColumnInfo) -> Option<Option<String>> {
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
