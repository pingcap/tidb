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

//! A table stored as real TiKV-format key/value bytes, plus the scan executor
//! that reads it back -- the storage-backed leg of the deployment ladder.
//!
//! Rows are written with the transcreated codecs: record keys through
//! `tidb_tablecodec::table_key::encode_row_key_with_handle` (`t{tid}_r{handle}`)
//! and row values through `tidb_tablecodec::encode_table_row` (the v2 row
//! format). [`TableScanExec`] iterates the table's record-key range and
//! decodes each pair back into chunk rows -- the same
//! encode -> store -> scan -> decode path a real TiKV-backed table takes
//! (Go `pkg/executor` table reader over `tablecodec`).
//!
//! The bytes live behind the [`TableStorage`] seam (`crate::storage`), whose
//! four operations are `kv.Retriever.Get`/`Iter` and `kv.Mutator.Set`/`Delete`
//! -- the same contract a TiKV snapshot implements -- so every read and write
//! in this file is written against the storage interface rather than against a
//! container. The backend this tier installs is [`MemTableStorage`], an
//! in-process `tidb-txnkv` `MemStorage`.
//!
//! NOT MODELLED (documented): the in-process backend has no MVCC versions,
//! timestamps, locks, regions, or coprocessor pushdown, so a scan reads the
//! latest write immediately. Installing a transaction-backed backend does not
//! touch the codec or the scan loop; `crate::storage` lists what such a
//! backend still needs.

pub mod auto_increment;
pub mod auto_random;
pub mod cache;
pub(crate) mod column_deps;
pub mod index_entries;
pub(crate) mod index_states;
pub mod nonpublic_columns;
pub mod partition_reorg;
pub use partition_reorg::{PartitionDoubleWrite, PartitionIndexMode};
use tidb_hack::GoCaseCompare;
pub mod row_decoder;
pub mod table_meta;

use tidb_autoid::{
    AutoIdError, AutoIdStoreError,
    LocalAutoIdStore, DEFAULT_AUTO_ID_STEP,
};

use tidb_autoid::AutoIdCall;
pub use auto_increment::{AutoIncrement, TableAutoId};
pub use auto_random::{AutoRandom, AutoRandomError, AutoRandomSpec};

pub use row_decoder::{
    DecodedRow, GeneratedColumnSelection, PreparedPointGetRowDecoder, RowDecoder,
};
pub use table_meta::{
    FkAction, KvColumn,
    KvForeignKey, KvIndex, NonPublicColumn, PreparedPointGetDecodeContext,
    RowDecodeContext, TableCharset, TableHandle,
};
pub use index_entries::index_key_bounds;
use table_meta::NonPublicIndex;

use tidb_kv::storage::{MemTableStorage, StorageError, StorageIterator, TableStorage};
use tidb_autoid::AutoIdAllocator;
use row_decoder::fill_handle_columns;
pub use row_decoder::row_checksum;
use std::collections::HashSet;
pub use table_meta::NOT_NULL_FLAG;
use tidb_tablecodec::table_key::{encode_row_key_with_handle, get_table_handle_key_range};
use tidb_datatype::{new_collation_enabled, Datum, FieldType, SessionTimeZone};
use tidb_expr::expression::Expression;

use index_entries::duplicate_value_text;
pub use index_entries::index_entry_handle;
pub use index_entries::IndexEntryForCheck;
use tidb_hack::GoToLower;
use tidb_tablecodec::encode_table_row;
use tidb_txnkv::{CommonHandle, Key};

/// Deduplicates index columns by table-column offset, preserving the first
/// occurrence and its pointer identity.
///
/// This is Go `tables.DedupIndexColumns`; an index condition can mention the
/// same column repeatedly, but downstream row extraction needs each offset at
/// most once and in its first-seen order.
#[must_use]
pub fn dedup_index_columns(
    columns: Vec<tidb_model::GoShared<tidb_model::IndexColumn>>,
) -> Vec<tidb_model::GoShared<tidb_model::IndexColumn>> {
    if columns.len() <= 1 {
        return columns;
    }

    let mut seen = HashSet::with_capacity(columns.len());
    columns
        .into_iter()
        .filter(|column| seen.insert(column.read().offset))
        .collect()
}

#[derive(Default)]
struct AstColumnPaths(Vec<Vec<String>>);

impl tidb_ast::Visitor for AstColumnPaths {
    fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
        if let Some(tidb_ast::Expr::Column(path)) = node.downcast_mut::<tidb_ast::Expr>() {
            self.0.push(path.clone());
            return true;
        }
        false
    }

    fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
        true
    }
}

/// Extracts table columns referenced by a partial-index predicate.
///
/// Go `tables.ExtractColumnsFromCondition` optionally expands a virtual
/// generated column to its base-column dependencies, then still returns the
/// virtual column itself. Stored generated columns remain ordinary columns.
pub fn extract_columns_from_index_condition(
    index: &tidb_model::IndexInfo,
    table: &tidb_model::TableInfo,
    include_virtual_generated_dependencies: bool,
) -> Result<Vec<tidb_model::GoShared<tidb_model::IndexColumn>>, String> {
    if index.condition_expr_string.is_empty() {
        return Ok(Vec::new());
    }
    let expression = index.condition_expr().map_err(|error| error.message)?;
    let expression = tidb_model::generated_expr::simple_resolve_name(expression, table)
        .map_err(|error| error.to_string())?;
    extract_columns_from_expression(expression, table, include_virtual_generated_dependencies)
}

fn extract_columns_from_expression(
    mut expression: tidb_ast::Expr,
    table: &tidb_model::TableInfo,
    include_virtual_generated_dependencies: bool,
) -> Result<Vec<tidb_model::GoShared<tidb_model::IndexColumn>>, String> {
    use tidb_ast::Visitable;

    let mut paths = AstColumnPaths::default();
    expression.accept(&mut paths);
    let mut columns = Vec::new();
    for path in paths.0 {
        let name = path
            .last()
            .ok_or_else(|| "partial-index condition contains an empty column path".to_owned())?;
        let lowercase_name = tidb_ast::CiString::new(name).lowercase().to_owned();
        let (offset, column) = table
            .columns
            .iter_deref()
            .enumerate()
            .find(|(_, column)| column.read().name.lowercase() == lowercase_name)
            .ok_or_else(|| format!("unknown column '{name}' in partial-index condition"))?;
        let column = column.read();
        if include_virtual_generated_dependencies && column.is_virtual_generated() {
            let generated =
                tidb_model::generated_expr::parse_expression(&column.generated_expr_string)
                    .map_err(|error| error.message)?;
            let generated = tidb_model::generated_expr::simple_resolve_name(generated, table)
                .map_err(|error| error.to_string())?;
            columns.extend(extract_columns_from_expression(
                generated,
                table,
                include_virtual_generated_dependencies,
            )?);
        }
        columns.push(tidb_model::GoShared::new(tidb_model::IndexColumn {
            name: column.name.clone(),
            offset: offset as i64,
            ..Default::default()
        }));
    }
    Ok(columns)
}

/// Options shared by table mutations and index writes.
///
/// Go stores an opaque `context.Context` handle in `commonMutateOpt`, and its
/// source test observes that the SAME handle survives every derived option.
/// Rust keeps its native [`crate::StmtContext`] behind `Rc` for that reason:
/// an update can derive add-record and create-index options without replacing
/// the statement object with a value snapshot.
#[derive(Clone, Default)]
struct CommonMutateOptions {
    context: Option<std::rc::Rc<dyn tidb_tblctx::MutateContext>>,
}

/// Go `table.AddRecordOpt`, in the fields exercised by
/// `pkg/table/table_test.go::TestOptions`.
#[derive(Clone, Default)]
pub struct AddRecordOptions {
    common: CommonMutateOptions,
    is_update: bool,
    generate_record_id: bool,
    reserve_auto_id: usize,
}

impl AddRecordOptions {
    /// Creates the Go zero-value option set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `WithCtx` for an add-record operation.
    #[must_use]
    pub fn with_context(mut self, context: std::rc::Rc<dyn tidb_tblctx::MutateContext>) -> Self {
        self.common.context = Some(context);
        self
    }

    /// Go `IsUpdate`: the add is the insert half of an update and therefore
    /// allocates a replacement row id when the table has no clustered handle.
    #[must_use]
    pub fn for_update(mut self) -> Self {
        self.is_update = true;
        self.generate_record_id = true;
        self
    }

    /// Go `WithReserveAutoIDHint`.
    #[must_use]
    pub fn with_reserve_auto_id_hint(mut self, count: usize) -> Self {
        self.reserve_auto_id = count;
        self
    }

    /// The statement context propagated by `WithCtx`.
    #[must_use]
    pub fn context(&self) -> Option<&std::rc::Rc<dyn tidb_tblctx::MutateContext>> {
        self.common.context.as_ref()
    }

    /// Whether this add belongs to an update.
    #[must_use]
    pub const fn is_update(&self) -> bool {
        self.is_update
    }

    /// Whether a new `_tidb_rowid` must be generated.
    #[must_use]
    pub const fn generate_record_id(&self) -> bool {
        self.generate_record_id
    }

    /// The auto-id batch size hinted by the caller.
    #[must_use]
    pub const fn reserve_auto_id(&self) -> usize {
        self.reserve_auto_id
    }

    /// Go `AddRecordOpt.GetCreateIdxOpt`.
    #[must_use]
    pub fn create_index_options(&self) -> CreateIndexOptions {
        CreateIndexOptions {
            common: self.common.clone(),
            ..CreateIndexOptions::default()
        }
    }
}

/// Go `table.UpdateRecordOpt`, including its conversions to the options used
/// by the delete/add and index halves of an update.
#[derive(Clone, Default)]
pub struct UpdateRecordOptions {
    common: CommonMutateOptions,
    skip_write_untouched_indices: bool,
}

impl UpdateRecordOptions {
    /// Creates the Go zero-value option set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `WithCtx` for an update-record operation.
    #[must_use]
    pub fn with_context(mut self, context: std::rc::Rc<dyn tidb_tblctx::MutateContext>) -> Self {
        self.common.context = Some(context);
        self
    }

    /// The statement context propagated by `WithCtx`.
    #[must_use]
    pub fn context(&self) -> Option<&std::rc::Rc<dyn tidb_tblctx::MutateContext>> {
        self.common.context.as_ref()
    }

    /// Whether unchanged index entries may be left alone.
    #[must_use]
    pub const fn skip_write_untouched_indices(&self) -> bool {
        self.skip_write_untouched_indices
    }

    /// Go `UpdateRecordOpt.GetAddRecordOpt`.
    #[must_use]
    pub fn add_record_options(&self) -> AddRecordOptions {
        AddRecordOptions {
            common: self.common.clone(),
            is_update: true,
            generate_record_id: true,
            reserve_auto_id: 0,
        }
    }

    /// Go `UpdateRecordOpt.GetAddRecordOptKeepRecordID`.
    #[must_use]
    pub fn add_record_options_keep_record_id(&self) -> AddRecordOptions {
        AddRecordOptions {
            common: self.common.clone(),
            is_update: true,
            generate_record_id: false,
            reserve_auto_id: 0,
        }
    }

    /// Go `UpdateRecordOpt.GetCreateIdxOpt`.
    #[must_use]
    pub fn create_index_options(&self) -> CreateIndexOptions {
        CreateIndexOptions {
            common: self.common.clone(),
            ..CreateIndexOptions::default()
        }
    }
}

/// Go `table.CreateIdxOpt`.
#[derive(Clone, Default)]
pub struct CreateIndexOptions {
    common: CommonMutateOptions,
    ignore_assertion: bool,
    from_backfill: bool,
}

impl CreateIndexOptions {
    /// Creates the Go zero-value option set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `WithCtx` for an index write.
    #[must_use]
    pub fn with_context(mut self, context: std::rc::Rc<dyn tidb_tblctx::MutateContext>) -> Self {
        self.common.context = Some(context);
        self
    }

    /// Go `WithIgnoreAssertion`.
    #[must_use]
    pub fn with_ignore_assertion(mut self) -> Self {
        self.ignore_assertion = true;
        self
    }

    /// Go `FromBackfill`.
    #[must_use]
    pub fn with_backfill_source(mut self) -> Self {
        self.from_backfill = true;
        self
    }

    /// The statement context propagated by `WithCtx`.
    #[must_use]
    pub fn context(&self) -> Option<&std::rc::Rc<dyn tidb_tblctx::MutateContext>> {
        self.common.context.as_ref()
    }

    /// Whether the index write skips transaction assertions.
    #[must_use]
    pub const fn ignore_assertion(&self) -> bool {
        self.ignore_assertion
    }

    /// Whether the index write comes from a DDL backfill worker.
    #[must_use]
    pub const fn from_backfill(&self) -> bool {
        self.from_backfill
    }
}

/// One enforced CHECK constraint compiled from persisted table metadata.
#[derive(Clone, Debug)]
struct KvCheckConstraint {
    name: String,
    expr: Expression,
    dependencies: Vec<String>,
    source: tidb_ast::Expr,
    build_zone: tidb_datatype::SessionTimeZone,
    build_like_default_escape: u8,
    zone_sensitive: bool,
    like_default_escape_sensitive: bool,
}

/// Go `CheckAndBuildIndexConditionString`'s restore flags
/// (`pkg/ddl/index.go`).
fn partial_index_condition_flags() -> tidb_parser_format::RestoreFlags {
    tidb_parser_format::RestoreFlags::STRING_SINGLE_QUOTES
        | tidb_parser_format::RestoreFlags::KEYWORD_LOWERCASE
        | tidb_parser_format::RestoreFlags::NAME_BACK_QUOTES
        | tidb_parser_format::RestoreFlags::SPACES_AROUND_BINARY_OPERATION
        | tidb_parser_format::RestoreFlags::WITHOUT_SCHEMA_NAME
        | tidb_parser_format::RestoreFlags::WITHOUT_TABLE_NAME
}

/// The lightweight evaluation context used by a partial-index predicate.
/// Index maintenance historically received only a session timezone rather
/// than the whole statement context; carrying the row and table columns here
/// preserves that narrow seam while still evaluating the compiled expression
/// through the same `tidb_expr` machinery as CHECK constraints.
struct IndexConditionContext<'a> {
    columns: &'a [KvColumn],
    row: &'a [Datum],
    zone: &'a SessionTimeZone,
}

impl tidb_exprctx::Columns for IndexConditionContext<'_> {
    fn get(&self, path: &[String]) -> Option<Datum> {
        let name = path.last()?;
        let offset = self
            .columns
            .iter()
            .position(|column| column.name.go_lower_eq(name))?;
        self.row.get(offset).cloned()
    }

    fn time_zone(&self) -> SessionTimeZone {
        self.zone.clone()
    }
}

/// One session transaction's state of a GLOBAL temporary table: Go
/// `TxnCtx.TemporaryTables[id]`, its rows and its auto-id allocator.
#[derive(Clone, Debug)]
pub struct TemporaryTableTxnData {
    store: Box<dyn TableStorage>,
    auto_id: AutoIdAllocator,
    row_id: Option<AutoIdAllocator>,
}

/// A table whose rows live as TiKV-format bytes in a sorted key/value map.
#[derive(Clone, Debug)]
pub struct KvTable {
    /// Go `table.Table.Meta()`: the `TableInfo` this table was loaded from
    /// (`TableFromMeta`), shared like Go's pointer and the one source of the
    /// table's scalar metadata; the fields below are what the loader derived
    /// from it. A table built from columns alone (a test fixture) carries
    /// its id here and what its setters record.
    meta: tidb_model::GoShared<tidb_model::TableInfo>,
    /// The table id (Go `TableInfo.ID`), the record-key prefix.
    pub(crate) table_id: i64,
    /// `(row count, average row length, data length, index length)` from Go
    /// `TableSizeStats.EstimateDataLength` for this catalog image.
    storage_statistics: (u64, u64, u64, u64),
    /// Per-physical-partition values used by Go
    /// `information_schema.PARTITIONS`. The logical table's aggregate stays
    /// in `storage_statistics`; these entries retain each partition's own
    /// row count and lengths.
    partition_storage_statistics: std::collections::BTreeMap<i64, (u64, u64, u64, u64)>,
    /// The columns, in schema order, VISIBLE ONES FIRST.
    ///
    /// The tail of this vector is the hidden columns an expression index was
    /// rewritten into (`hidden_columns` counts them). Keeping them contiguous
    /// at the end is what makes a visible offset and a physical offset the
    /// same number, so no call site has to translate between the two -- see
    /// [`crate::expression_index`] for why that matters.
    ///
    /// Behind an `Arc`, because this is METADATA and Go shares metadata by
    /// pointer: an `InfoSchema`'s `TableInfo` is immutable and every session
    /// holds the same one, with DDL building a NEW `TableInfo` rather than
    /// editing in place. Cloning a `KvTable` -- which the narrow tier does
    /// per transaction and the wide node per catalog rebuild -- was deep-
    /// copying every column's name and collation strings, and the clone was
    /// the single heaviest write-path frame under sysbench. Mutators go
    /// through [`KvTable::columns_mut`], whose `Arc::make_mut` is Go's
    /// build-a-new-TableInfo, one shared metadata copied only when a DDL
    /// actually changes it.
    pub(crate) columns: std::sync::Arc<Vec<KvColumn>>,
    /// How many of the TRAILING entries of `columns` are hidden (Go
    /// `ColumnInfo.Hidden`). Zero for every table with no expression index.
    hidden_columns: usize,
    /// Go `model.IndexInfo.MVIndex`, carried as the SOURCE column each
    /// multi-valued index's single ARRAY key part was built over
    /// (`ColumnInfo.Dependences`, which DDL's `buildHiddenColumnInfoWithCheck`
    /// fills from the indexed expression). Keyed by index id. Empty unless a
    /// loader or DDL recorded a multi-valued part; an entry is what lets the
    /// planner rewrite `(v MEMBER OF (source))` onto this index's stored
    /// element keys.
    mv_key_part_sources: std::collections::BTreeMap<i64, String>,
    /// The byte store, reached through the [`TableStorage`] seam (module
    /// doc), so a TiKV-backed backend replaces it without touching this file.
    pub(crate) store: Box<dyn TableStorage>,
    /// The statement budget a reader's own copy counts its scanned keys
    /// against (see [`Self::count_keys_read`]); `None` on catalog tables and
    /// on reads Go serves outside the coprocessor.
    keys_read: Option<tidb_kv::keys_read::KeysReadBudget>,
    /// Go `TableInfo.PKIsHandle`: the offset of the single integer primary-key
    /// column whose value IS the row handle, when the table has one.
    pub(crate) pk_handle_offset: Option<usize>,
    /// The table's indexes (Go `TableInfo.Indices`); `Arc`-shared like
    /// `columns`, for the same reason.
    pub(crate) indexes: std::sync::Arc<Vec<KvIndex>>,
    /// Indexes a DDL is still adding or dropping; written by state only.
    nonpublic_indexes: std::sync::Arc<Vec<NonPublicIndex>>,
    /// Columns a DDL is adding, dropping or changing. A write row carries
    /// their values after the public columns (offset `columns.len() + i`),
    /// which is where a non-public index's key parts address them.
    pub(crate) nonpublic_columns: std::sync::Arc<Vec<NonPublicColumn>>,
    /// Go `ColumnInfo.ChangingFieldType` of public columns, by offset: the
    /// type a MODIFY COLUMN that rewrites no rows is checking writes against.
    changing_field_types: std::collections::BTreeMap<usize, FieldType>,
    /// Go `IndexColumn.UseChangingType`, by index id: the key parts whose
    /// values are cast to their column's changing type.
    changing_index_parts: std::collections::BTreeMap<i64, Vec<usize>>,
    /// An ongoing partition reorganization, whose writes are mirrored.
    partition_reorg: Option<Box<PartitionDoubleWrite>>,
    /// Compiled `WHERE` predicates for partial indexes, keyed by the Go
    /// `IndexInfo.ID`. Keeping this sidecar separate from [`KvIndex`] avoids
    /// widening every source-shaped index fixture while making the predicate
    /// available to every index write and backfill path.
    partial_index_conditions: std::collections::BTreeMap<i64, KvCheckConstraint>,
    /// Go `IndexInfo.ConditionExprString` as the in-process DDL installed it
    /// (restored with the DDL session's connection charset, then through the
    /// meta JSON reload). A predicate without one prints its restored source.
    pub(crate) partial_index_condition_texts: std::collections::BTreeMap<i64, String>,
    /// The AUTO_INCREMENT column's offset, if the table has one.
    auto_increment_offset: Option<usize>,
    /// Go's auto-id allocator, shared across the copies a transaction stages
    /// so that a consumed id is never returned (see [`AutoIdAllocator`]).
    pub(crate) auto_id: AutoIdAllocator,
    pub(crate) row_id: Option<AutoIdAllocator>,
    /// The `AUTO_RANDOM` handle layout and its distinct TARID allocator.
    auto_random: Option<AutoRandomSpec>,
    pub(crate) auto_random_id: AutoIdAllocator,
    /// Go `TableInfo.IsCommonHandle`: the clustered primary key's column
    /// offsets, whose encoding IS the row handle. Empty when the table has no
    /// clustered common handle.
    pub(crate) common_handle_offsets: Vec<usize>,
    /// Go `IndexColumn.Length` for each common-handle column.
    pub(crate) common_handle_prefix_lengths: Vec<i64>,
    /// During ALTER TABLE ... PARTITION BY / REMOVE PARTITIONING: the
    /// table's new ID (`PartitionInfo.NewTableID`) and the global indexes the
    /// job created (`DDLChangedIndex` true), whose entries are keyed under it.
    new_table_global_indexes: Option<(i64, Vec<i64>)>,
    /// The cluster's persisted new-collation mode, captured when this table
    /// object is built.
    ///
    /// Go keeps the same fact in `TableCommon.encoder` and hands it to every
    /// `index` it constructs. It is deliberately table state rather than a
    /// global read at each key/value call: one entry must never combine a
    /// legacy key with a new-collation restored-data value, or vice versa.
    pub(crate) use_new_collation: bool,
    /// Whether every loaded column is public. Point-read planning must not
    /// bypass the ordinary reader while a column is in a DDL transition.
    all_columns_public: bool,
    /// Go `TableInfo.ForeignKeys`: the constraints this table DECLARES, i.e.
    /// the child side. The parent side is found by scanning the catalog for
    /// the tables whose foreign keys name this one, as Go's
    /// `ReferredFKInfo` index does.
    foreign_keys: Vec<KvForeignKey>,
    /// Go `TableCommon.writableConstraints`: enforced CHECK constraints not
    /// in delete-only/delete-reorganization state.
    check_constraints: std::sync::Arc<Vec<KvCheckConstraint>>,
    /// Go `TableInfo.Constraints`: the persisted CHECK metadata used by DDL,
    /// SHOW, CREATE LIKE, and dependency checks.
    check_constraint_infos: std::sync::Arc<Vec<tidb_model::table::ConstraintInfo>>,
    /// Go `TableInfo.Partition`: how the table's rows are spread over
    /// physical key prefixes, or `None` for an ordinary table whose rows all
    /// live under [`KvTable::table_id`].
    ///
    /// This is the ONLY branch partitioning adds to the write path: every
    /// record key goes through [`KvTable::record_physical_id`], which reads
    /// this field once and then has no cases left. See
    /// [`crate::partition_routing`].
    pub(crate) partition: Option<Box<crate::partition_routing::PartitionSpec>>,
    /// The physical ids a READ of this table may touch, when something has
    /// narrowed them: `PARTITION (p)` naming them explicitly, or pruning
    /// proving the rest cannot match. `None` is "every partition", which is
    /// also what an unpartitioned table always has.
    ///
    /// It lives on the table handle rather than beside the scan because
    /// EVERY read path -- the row cursor, the handle-range scan, the point
    /// get's partition probe -- already asks the table which ids its rows
    /// are under, so narrowing that one answer narrows all of them at once.
    /// A path that grew its own copy of the id list is exactly how a pruned
    /// read and an unpruned one come to disagree.
    ///
    /// Writes ignore it: a row's partition is a function of the ROW (Go's
    /// `locatePartition`), never of the statement's restriction.
    pub(crate) read_partitions: Option<Vec<i64>>,
}

/// Go PointGetExecutor keeps immutable table metadata and a separate snapshot.
/// A read owns only the backend handle, never a detached catalog/table image.
pub struct PointRead<'a> {
    table: &'a KvTable,
    store: Box<dyn TableStorage>,
}

/// Go point readers retain encoded values until Next decodes them. The batch
/// owns the storage result once; duplicate handles borrow the same bytes.
pub enum PointReadValues {
    Single(Option<Vec<u8>>),
    Batch {
        keys: Vec<Option<Key>>,
        values: std::collections::HashMap<Key, Vec<u8>>,
    },
}

/// Decoder metadata belongs to one reader's immutable table/statement image,
/// like Go NewRowDecoder. Mixed old/new rows initialize each path at most once.
pub struct PointRowDecoder {
    ordinary_columns: bool,
    v2: Option<(Vec<tidb_rowcodec::ColumnInfo>, Vec<i64>)>,
    general: Option<RowDecoder>,
}

impl PointRowDecoder {
    pub fn new(table: &KvTable) -> Self {
        Self {
            ordinary_columns: !table
                .columns
                .iter()
                .any(|column| column.generated.is_some()),
            v2: None,
            general: None,
        }
    }

    pub fn decode(
        &mut self,
        table: &KvTable,
        handle: &TableHandle,
        entry: &[u8],
        context: &RowDecodeContext,
    ) -> Result<Vec<Datum>, KvTableError> {
        if tidb_rowcodec::is_new_format(entry) && self.ordinary_columns {
            let (columns, handle_column_ids) = self.v2.get_or_insert_with(|| {
                let columns = table
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(offset, column)| tidb_rowcodec::ColumnInfo {
                        id: column.id,
                        is_pk_handle: table.pk_handle_offset == Some(offset),
                        virtual_generated: false,
                        field_type: column.field_type.clone(),
                    })
                    .collect();
                let ids = table
                    .pk_handle_offset
                    .into_iter()
                    .chain(
                        table
                            .common_handle_offsets
                            .iter()
                            .copied()
                            .zip(&table.common_handle_prefix_lengths)
                            .filter(|(_, length)| {
                                **length == tidb_datatype::UNSPECIFIED_LENGTH
                            })
                            .map(|(offset, _)| offset),
                    )
                    .filter_map(|offset| table.columns.get(offset).map(|column| column.id))
                    .collect();
                (columns, ids)
            });
            let codec_handle = match handle {
                TableHandle::Int(value) => tidb_rowcodec::Handle::Int(*value),
                TableHandle::Common(encoded) => {
                    let common = CommonHandle::new(encoded.clone())
                        .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                    let parts = table
                        .common_handle_offsets
                        .iter()
                        .enumerate()
                        .filter(|(position, _)| {
                            table.common_handle_prefix_lengths.get(*position).copied()
                                == Some(tidb_datatype::UNSPECIFIED_LENGTH)
                        })
                        .filter_map(|(index, _)| common.encoded_column(index).map(<[u8]>::to_vec))
                        .collect();
                    tidb_rowcodec::Handle::Common(parts)
                }
            };
            let default_datum = |index: usize| {
                table.columns[index]
                    .origin_default_value(context.type_flags(), context.zone())
                    .map_err(|error| error.to_string())
            };
            return tidb_rowcodec::decode_row_to_datums(
                entry,
                columns,
                &tidb_rowcodec::DecodeRowOptions {
                    handle_column_ids,
                    handle: Some(&codec_handle),
                    default_datum: Some(&default_datum),
                    timezone: Some(context.zone()),
                    ..tidb_rowcodec::DecodeRowOptions::default()
                },
            )
            .map(|row| row.values)
            .map_err(|error| KvTableError::Decode(format!("{error:?}")));
        }
        if self.general.is_none() {
            self.general = Some(RowDecoder::for_table_read(
                table.columns.clone(),
                table.pk_handle_offset,
                table.common_handle_offsets.clone(),
                table.common_handle_prefix_lengths.clone(),
                None,
                table.use_new_collation,
                context.clone(),
            )?);
        }
        Ok(self
            .general
            .as_ref()
            .expect("initialized decoder")
            .decode_and_eval(handle, entry)?
            .into_parts()
            .0)
    }
}

impl PointReadValues {
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        match self {
            Self::Single(value) => (index == 0).then_some(value.as_deref()).flatten(),
            Self::Batch { keys, values } => keys
                .get(index)?
                .as_ref()
                .and_then(|key| values.get(key))
                .map(Vec::as_slice),
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Single(value) => value.is_none(),
            Self::Batch { keys, .. } => keys.is_empty(),
        }
    }
}

impl PointRead<'_> {
    pub fn point_rpc_counts(&mut self) -> (u64, u64) {
        self.store.point_rpc_counts()
    }

    pub fn lookup_unique(
        &mut self,
        index_id: i64,
        values: &[Datum],
        zone: &SessionTimeZone,
    ) -> Result<Option<TableHandle>, KvTableError> {
        match self.table.unique_point_read(index_id, values, zone)? {
            Some(read) => read.get(self.store.as_mut()),
            None => Ok(None),
        }
    }

    pub fn get_prepared_point_row(
        &mut self,
        handle: &TableHandle,
        decoder: &PreparedPointGetRowDecoder,
        context: &PreparedPointGetDecodeContext,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        // The common sysbench point-get shape is an unpartitioned table. Avoid
        // allocating a one-element physical-id Vec for every row lookup; Go's
        // point reader carries the table id directly in that shape.
        let record = if self.table.read_partitions.is_none() && self.table.partition.is_none() {
            read_stored_record_one(self.store.as_mut(), self.table.table_id, handle)?
        } else {
            read_stored_record(
                self.store.as_mut(),
                self.table.record_physical_ids(),
                handle,
            )?
        };
        let Some((_, entry)) = record else {
            return Ok(None);
        };
        decoder.decode(handle, &entry, context).map(Some)
    }
}

fn read_stored_record_one(
    store: &mut dyn TableStorage,
    physical_id: i64,
    handle: &TableHandle,
) -> Result<Option<(Key, Vec<u8>)>, KvTableError> {
    let key = Key::from_bytes(encode_row_key_with_handle(
        physical_id,
        &handle.record_handle(),
    ));
    match store.get(&key) {
        Ok(entry) => Ok(Some((key, entry))),
        Err(StorageError::NotFound) => Ok(None),
        Err(error) => Err(KvTableError::from(error)),
    }
}

fn read_stored_record(
    store: &mut dyn TableStorage,
    physical_ids: Vec<i64>,
    handle: &TableHandle,
) -> Result<Option<(Key, Vec<u8>)>, KvTableError> {
    for id in physical_ids {
        let key = Key::from_bytes(encode_row_key_with_handle(id, &handle.record_handle()));
        match store.get(&key) {
            Ok(entry) => return Ok(Some((key, entry))),
            Err(StorageError::NotFound) => {}
            Err(error) => return Err(KvTableError::from(error)),
        }
    }
    Ok(None)
}

/// The conflicting handle and its original constraint diagnostic travel together.
/// Go's duplicate-check path retains both instead of reading the same key again.
#[derive(Debug)]
pub struct RowConflict {
    pub handle: TableHandle,
    pub error: KvTableError,
    /// Whether the candidate's own record key collided (Go's
    /// `toBeCheckedRow.handleKey`) rather than one of its unique keys; REPLACE
    /// in `LOAD DATA` counts the two removals differently.
    pub on_record_key: bool,
}

/// A reader's range iterator under a statement budget: each entry it moves
/// past is one key its scan processed, unistore's `tableScanExec` /
/// `indexScanExec` row count. Over a staged overlay only the snapshot's
/// entries count (see [`StorageIterator::snapshot_entries_consumed`]): Go's
/// `UnionScan` merges the transaction's own rows in TiDB, and the coprocessor
/// under it never processed them.
struct KeysReadIterator {
    inner: Box<dyn StorageIterator>,
    budget: tidb_kv::keys_read::KeysReadBudget,
    /// The snapshot entries already charged to `budget`.
    charged: u64,
}

impl KeysReadIterator {
    fn charge_snapshot_entries(&mut self, consumed: u64) -> Result<(), StorageError> {
        let fresh = consumed.saturating_sub(self.charged);
        self.charged = self.charged.max(consumed);
        if fresh > 0 {
            self.budget.add(fresh)?;
        }
        Ok(())
    }
}

impl StorageIterator for KeysReadIterator {
    fn valid(&self) -> bool {
        self.inner.valid()
    }

    fn key(&self) -> &Key {
        self.inner.key()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<(), StorageError> {
        let Some(consumed) = self.inner.snapshot_entries_consumed() else {
            self.budget.add(1)?;
            return self.inner.next();
        };
        // The entries up to the one being moved past, then -- once the range
        // is exhausted -- any the advance consumed behind staged tombstones.
        self.charge_snapshot_entries(consumed)?;
        self.inner.next()?;
        if !self.inner.valid() {
            if let Some(consumed) = self.inner.snapshot_entries_consumed() {
                self.charge_snapshot_entries(consumed)?;
            }
        }
        Ok(())
    }

    fn snapshot_entries_consumed(&self) -> Option<u64> {
        self.inner.snapshot_entries_consumed()
    }

    fn close(&mut self) {
        self.inner.close();
    }
}

mod error;
pub use error::KvTableError;
use tidb_kv::staged_writes::{StagedWrites, TxnStaging};

/// Go `ShardIDFormat.Compose` for a SIGNED `BIGINT` handle, which is what
/// `_tidb_rowid` always is.
///
/// `NewShardIDFormat` computes `incrementalBits = RowIDBitLength - shardBits`
/// and then subtracts one more for the sign bit, so with
/// `SHARD_ROW_ID_BITS = 15` the shard occupies the top 15 value bits and the
/// counter keeps the low 48 -- which is why the corpus reads a row's shard as
/// `_tidb_rowid >> 48`.
///
/// Zero shard bits leave the id alone: an unsharded table's handle is the
/// counter itself.
fn compose_row_id_shard(shard_bits: u64, shard: i64, id: i64) -> i64 {
    if shard_bits == 0 {
        return id;
    }
    let incremental_bits = ROW_ID_BIT_LENGTH - shard_bits - 1;
    ((shard & ((1 << shard_bits) - 1)) << incremental_bits) | id
}

/// Go `autoid.RowIDBitLength`.
const ROW_ID_BIT_LENGTH: u64 = 64;

impl KvTable {
    /// Builds an empty table over the in-process backend.
    #[must_use]
    /// The mutable view a DDL takes of the shared column metadata --
    /// Go's "build a new TableInfo": the first mutation after a share
    /// copies, an unshared table edits in place.
    /// Replaces the byte store of this table copy with `store`, keeping every
    /// other binding (columns, indexes, allocators) untouched.
    ///
    /// This is the session-rebinding seam for SHARED table templates: a
    /// factory builds each loaded table once per schema version over neutral
    /// storage, and every connection clones that template and swaps in its own
    /// [`TableStorage`] -- the snapshot slot and staged-write buffer its
    /// statements read and write through. Go reaches the same shape by giving
    /// each session pointers to one shared `table.Table`; this tier's tables
    /// own their store by value, so the rebinding is an explicit swap.
    pub fn replace_storage(&mut self, store: Box<dyn TableStorage>) {
        self.store = store;
    }

    /// See [`TableStorage::snapshot_reader`].
    pub fn snapshot_reader(&self) -> Option<Box<dyn TableStorage>> {
        self.store.snapshot_reader()
    }

    /// See [`TableStorage::mem_buffer_snapshot`].
    pub fn mem_buffer_snapshot(
        &self,
        ranges: &[(Key, Key)],
    ) -> Option<std::sync::Arc<tidb_kv::mem_buffer::MemBufferSnapshot>> {
        self.store.mem_buffer_snapshot(ranges)
    }

    pub fn columns_mut(&mut self) -> &mut Vec<KvColumn> {
        std::sync::Arc::make_mut(&mut self.columns)
    }

    /// Go `Meta().ID`: the table's id.
    #[must_use]
    pub const fn table_id(&self) -> i64 {
        self.table_id
    }

    /// Re-keys the table under another physical id (a partition's).
    pub fn set_table_id(&mut self, id: i64) {
        self.table_id = id;
    }

    /// The storage seam this table reads and writes through.
    #[must_use]
    pub fn store(&self) -> &dyn TableStorage {
        &*self.store
    }

    /// The storage seam, mutably (rebinding a table to a statement's store).
    pub fn store_mut(&mut self) -> &mut Box<dyn TableStorage> {
        &mut self.store
    }

    /// The shared column list.
    #[must_use]
    pub fn columns_arc(&self) -> &std::sync::Arc<Vec<KvColumn>> {
        &self.columns
    }

    /// The shared column list, for copy-on-write edits.
    pub fn columns_arc_mut(&mut self) -> &mut std::sync::Arc<Vec<KvColumn>> {
        &mut self.columns
    }

    /// The shared index list.
    #[must_use]
    pub fn indexes_arc(&self) -> &std::sync::Arc<Vec<KvIndex>> {
        &self.indexes
    }

    /// The shared index list, for copy-on-write edits.
    pub fn indexes_arc_mut(&mut self) -> &mut std::sync::Arc<Vec<KvIndex>> {
        &mut self.indexes
    }

    /// The shared non-public column list, for copy-on-write edits.
    pub fn nonpublic_columns_arc_mut(&mut self) -> &mut std::sync::Arc<Vec<NonPublicColumn>> {
        &mut self.nonpublic_columns
    }

    /// The handle column's offset, for edits.
    pub fn pk_handle_offset_mut(&mut self) -> &mut Option<usize> {
        &mut self.pk_handle_offset
    }

    /// The auto-increment allocator.
    #[must_use]
    pub fn auto_id(&self) -> &AutoIdAllocator {
        &self.auto_id
    }

    /// The auto-increment allocator, mutably.
    pub fn auto_id_mut(&mut self) -> &mut AutoIdAllocator {
        &mut self.auto_id
    }

    /// The hidden row-id allocator.
    #[must_use]
    pub fn row_id(&self) -> Option<&AutoIdAllocator> {
        self.row_id.as_ref()
    }

    /// The hidden row-id allocator, mutably.
    pub fn row_id_mut(&mut self) -> &mut Option<AutoIdAllocator> {
        &mut self.row_id
    }

    /// The auto-random allocator.
    #[must_use]
    pub fn auto_random_id(&self) -> &AutoIdAllocator {
        &self.auto_random_id
    }

    /// The auto-random allocator, mutably.
    pub fn auto_random_id_mut(&mut self) -> &mut AutoIdAllocator {
        &mut self.auto_random_id
    }

    /// Partial-index condition texts by index id.
    #[must_use]
    pub fn partial_index_condition_texts(&self) -> &std::collections::BTreeMap<i64, String> {
        &self.partial_index_condition_texts
    }

    /// Partial-index condition texts, mutably.
    pub fn partial_index_condition_texts_mut(
        &mut self,
    ) -> &mut std::collections::BTreeMap<i64, String> {
        &mut self.partial_index_condition_texts
    }

    /// The partitions a read is restricted to, if any.
    #[must_use]
    pub fn read_partitions(&self) -> Option<&[i64]> {
        self.read_partitions.as_deref()
    }

    /// The partition restriction, mutably.
    pub fn read_partitions_mut(&mut self) -> &mut Option<Vec<i64>> {
        &mut self.read_partitions
    }

    /// The partition spec, mutably.
    pub fn partition_mut(&mut self) -> &mut Option<Box<crate::partition_routing::PartitionSpec>> {
        &mut self.partition
    }

    /// The handle columns' offsets, mutably.
    pub fn common_handle_offsets_mut(&mut self) -> &mut Vec<usize> {
        &mut self.common_handle_offsets
    }

    /// The handle columns' prefix lengths, mutably.
    pub fn common_handle_prefix_lengths_mut(&mut self) -> &mut Vec<i64> {
        &mut self.common_handle_prefix_lengths
    }

    /// Whether the table uses the new-collation encoding, mutably.
    pub fn use_new_collation_mut(&mut self) -> &mut bool {
        &mut self.use_new_collation
    }

    /// All table columns in physical offset order, including hidden columns
    /// used by expression indexes, matching `TableInfo.Columns`.
    #[must_use]
    pub fn columns(&self) -> &[KvColumn] {
        &self.columns
    }

    pub fn indexes_mut(&mut self) -> &mut Vec<KvIndex> {
        std::sync::Arc::make_mut(&mut self.indexes)
    }

    pub fn new(table_id: i64, columns: Vec<KvColumn>) -> Self {
        KvTable::with_storage(table_id, columns, Box::new(MemTableStorage::new()))
    }

    /// Builds an empty table over the given backend.
    ///
    /// This is the one place a table's storage is chosen. A TiKV-backed tier
    /// hands every table a handle to the same transactional backend here; the
    /// table's own code is already written against the trait, so nothing else
    /// changes.
    #[must_use]
    pub fn with_storage(
        table_id: i64,
        columns: Vec<KvColumn>,
        store: Box<dyn TableStorage>,
    ) -> Self {
        Self::with_storage_and_collation(table_id, columns, store, new_collation_enabled())
    }

    /// Builds an empty table over the given backend with an already captured
    /// persisted collation mode.
    ///
    /// This mirrors Go `TableFromMetaWithCollate`: loaders and DDL backfills
    /// that already own the cluster/job setting pass it once, and every key
    /// and index value generated by the table uses that immutable copy.
    #[must_use]
    pub fn with_storage_and_collation(
        table_id: i64,
        columns: Vec<KvColumn>,
        store: Box<dyn TableStorage>,
        use_new_collation: bool,
    ) -> Self {
        KvTable {
            meta: tidb_model::GoShared::new(tidb_model::TableInfo {
                id: table_id,
                columns: columns
                    .iter()
                    .enumerate()
                    .map(|(offset, column)| fixture_column_info(column, offset, false))
                    .collect::<Vec<_>>()
                    .into(),
                ..tidb_model::TableInfo::default()
            }),
            table_id,
            storage_statistics: (0, 0, 0, 0),
            partition_storage_statistics: std::collections::BTreeMap::new(),
            columns: std::sync::Arc::new(columns),
            hidden_columns: 0,
            mv_key_part_sources: std::collections::BTreeMap::new(),
            store,
            keys_read: None,
            pk_handle_offset: None,
            indexes: std::sync::Arc::new(Vec::new()),
            nonpublic_indexes: std::sync::Arc::new(Vec::new()),
            nonpublic_columns: std::sync::Arc::new(Vec::new()),
            changing_field_types: std::collections::BTreeMap::new(),
            changing_index_parts: std::collections::BTreeMap::new(),
            partition_reorg: None,
            partial_index_conditions: std::collections::BTreeMap::new(),
            partial_index_condition_texts: std::collections::BTreeMap::new(),
            common_handle_offsets: Vec::new(),
            common_handle_prefix_lengths: Vec::new(),
            auto_increment_offset: None,
            auto_id: AutoIdAllocator::new(),
            row_id: None,
            auto_random: None,
            auto_random_id: AutoIdAllocator::new(),
            new_table_global_indexes: None,
            use_new_collation,
            all_columns_public: true,
            foreign_keys: Vec::new(),
            check_constraints: std::sync::Arc::new(Vec::new()),
            check_constraint_infos: std::sync::Arc::new(Vec::new()),
            partition: None,
            read_partitions: None,
        }
    }

    /// Publishes Go's four `information_schema.tables` statistics values.
    pub fn set_storage_statistics(&mut self, statistics: (u64, u64, u64, u64)) {
        self.storage_statistics = statistics;
    }

    /// Go `TableSizeStats.EstimateDataLength` for this catalog image.
    #[must_use]
    pub const fn storage_statistics(&self) -> (u64, u64, u64, u64) {
        self.storage_statistics
    }

    /// Publishes one physical partition's Go row/length estimates.
    pub fn set_partition_storage_statistics(
        &mut self,
        physical_id: i64,
        statistics: (u64, u64, u64, u64),
    ) {
        self.partition_storage_statistics
            .insert(physical_id, statistics);
    }

    /// Go `TableSizeStats.GetTableRows` plus
    /// `GetDataAndIndexLength` for one physical partition.
    #[must_use]
    pub fn partition_storage_statistics(&self, physical_id: i64) -> (u64, u64, u64, u64) {
        self.partition_storage_statistics
            .get(&physical_id)
            .copied()
            .unwrap_or_default()
    }

    /// Clears statement-derived information-schema size values.
    pub fn clear_storage_statistics(&mut self) {
        self.storage_statistics = (0, 0, 0, 0);
        self.partition_storage_statistics.clear();
    }

    /// Rebinds a freshly loaded table to a collation mode its outer plan
    /// already captured.
    ///
    /// The consuming form keeps this a construction step. The DDL backfill
    /// needs it because the shared catalog loader has already materialised all
    /// other table fields before the job-specific `UseNewCollate` is applied.
    #[must_use]
    pub fn with_new_collation_mode(mut self, use_new_collation: bool) -> Self {
        self.use_new_collation = use_new_collation;
        self
    }

    /// Whether persisted keys and expressions use the captured new-collation
    /// encoding mode (Go `table.Table.UseNewCollate`).
    #[must_use]
    pub const fn use_new_collation(&self) -> bool {
        self.use_new_collation
    }

    /// Carries the source catalog's column publication state into planning.
    pub fn set_all_columns_public(&mut self, public: bool) {
        self.all_columns_public = public;
    }

    /// Go isPointGetConvertableSchema's public-column prerequisite.
    pub const fn all_columns_public(&self) -> bool {
        self.all_columns_public
    }

    /// Go `session.HasDirtyContent(tid)`: whether the CURRENT transaction (or,
    /// in autocommit, the current statement) has staged a row write to this
    /// table. `staged` is that transaction's shared [`StagedWrites`] -- see
    /// the statement context's `staged_writes`.
    ///
    /// Its readers are the planner-side gate Go spells `tableHasDirtyContent`
    /// (`pkg/planner/core/util.go:156`), which decides whether a `UnionScan`
    /// sits above the reader; see the executor's `IndexRangeSourceExec`
    /// for the row ORDER that operator imposes.
    #[must_use]
    pub fn has_dirty_content(&self, staged: &StagedWrites) -> bool {
        staged.has_dirty_content(self.table_id)
    }

    /// Go `memBufSnap.Get(checkKey)`'s answer for one row handle: does the
    /// current transaction hold a staged write under this record key? A
    /// staged DELETE stages a tombstone and still answers true, exactly as
    /// Go's probe succeeds on the tombstone value.
    #[must_use]
    pub fn record_key_is_staged(
        &self,
        staged: &StagedWrites,
        physical_id: i64,
        handle: &TableHandle,
    ) -> bool {
        let key = encode_row_key_with_handle(physical_id, &handle.record_handle());
        staged.contains(self.table_id, &key)
    }

    /// Records how this table is partitioned (Go's `TableInfo.Partition`),
    /// which is what makes its record keys carry a partition id.
    pub fn set_partition(&mut self, partition: crate::partition_routing::PartitionSpec) {
        self.partition = Some(Box::new(partition));
    }

    /// The table's partitioning, or `None` for an ordinary table.
    #[must_use]
    pub fn partition(&self) -> Option<&crate::partition_routing::PartitionSpec> {
        self.partition.as_deref()
    }

    /// Go `TableInfo.PlacementPolicyRef`, by name.
    #[must_use]
    pub fn placement_policy(&self) -> Option<tidb_model::PolicyRefInfo> {
        self.meta.read().placement_policy_ref.as_ref().map(|policy| policy.read().clone())
    }


    /// How many rows each partition holds, in definition order, or an empty
    /// vector for an unpartitioned table.
    ///
    /// This is what Go reports as `information_schema.partitions.TABLE_ROWS`.
    /// It sees where a row was routed WITHOUT going through the read path, so
    /// it and `SELECT ... PARTITION (p)` pin the routing against real TiDB's
    /// captured answers independently -- a router and a selection that were
    /// wrong the same way would still disagree with one of the two.
    ///
    /// # Errors
    ///
    /// [`KvTableError::Storage`] when the backend refuses a scan.
    pub fn partition_row_counts(&mut self) -> Result<Vec<(String, usize)>, KvTableError> {
        let Some(partition) = self.partition.clone() else {
            return Ok(Vec::new());
        };
        let mut counts = Vec::with_capacity(partition.definitions.len());
        for definition in &partition.definitions {
            let (low, high) = get_table_handle_key_range(definition.id);
            let mut upper = high;
            upper.push(0);
            let mut iterator = self
                .store
                .iter(Some(&Key::from_bytes(low)), Some(&Key::from_bytes(upper)))
                .map_err(KvTableError::from)?;
            let mut rows = 0;
            while iterator.valid() {
                rows += 1;
                iterator.next().map_err(KvTableError::from)?;
            }
            iterator.close();
            counts.push((definition.name.clone(), rows));
        }
        Ok(counts)
    }

    /// The table's record-row count: the embedded-tier equivalent of the
    /// row count Go's session stats-delta flush maintains in
    /// `mysql.stats_meta`. A table written but never ANALYZEd plans from
    /// this real count with the pseudo distribution (Go
    /// `GetStatsTable` rule 3), so the planner needs the same number the
    /// flushed delta would have produced.
    pub fn stats_row_count(&mut self) -> Result<i64, KvTableError> {
        self.stats_row_count_for_physical_id(self.table_id)
    }

    /// Reads the record count for one physical table ID, including a single
    /// partition's keyspace. Stats-meta rows are keyed by these IDs.
    pub fn stats_row_count_for_physical_id(
        &mut self,
        physical_table_id: i64,
    ) -> Result<i64, KvTableError> {
        let (low, high) = get_table_handle_key_range(physical_table_id);
        let mut upper = high;
        upper.push(0);
        let mut iterator = self
            .store
            .iter(Some(&Key::from_bytes(low)), Some(&Key::from_bytes(upper)))
            .map_err(KvTableError::from)?;
        let mut rows = 0i64;
        while iterator.valid() {
            rows += 1;
            iterator.next().map_err(KvTableError::from)?;
        }
        iterator.close();
        Ok(rows)
    }

    /// The physical table id `row`'s record key is written under: the
    /// partition it routes into, or the table itself.
    ///
    /// This is the whole of "an unpartitioned table is a table with one
    /// partition whose id is its own" -- every write site calls this instead
    /// of reading `table_id`, so none of them carries a partitioning branch.
    pub fn record_physical_id(
        &self,
        row: &[Datum],
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Result<i64, KvTableError> {
        let Some(partition) = &self.partition else {
            return Ok(self.table_id);
        };
        partition
            .locate(row, &self.columns, ctx)
            .map_err(routing_error)
    }

    /// Encodes the selected row's lock identity without rereading storage.
    pub fn row_lock_key(
        &self,
        handle: &TableHandle,
        row: &[Datum],
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Result<Vec<u8>, KvTableError> {
        Ok(encode_row_key_with_handle(
            self.record_physical_id(row, ctx)?,
            &handle.record_handle(),
        ))
    }

    /// Verifies the partition selected for one INSERT row.
    ///
    /// Reads may narrow their physical key ranges, but an INSERT must still
    /// route the completed row normally and then reject it when that route is
    /// outside the statement's named set.  Keeping this beside
    /// [`Self::record_physical_id`] gives VALUES and INSERT...SELECT the same
    /// generated-column-aware decision.
    pub fn validate_insert_partitions(
        &self,
        row: &[Datum],
        selected: &[i64],
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Result<(), KvTableError> {
        if selected.contains(&self.record_physical_id(row, ctx)?) {
            Ok(())
        } else {
            Err(KvTableError::Mysql(
                tidb_terror::MysqlError::from_terror(
                    &crate::table_errors::ERR_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET
                        .generate_with_stack_by_args(&[]),
                ),
            ))
        }
    }

    /// Verifies both ends of one partition-qualified UPDATE.
    ///
    /// Go's `partitionTableWithGivenSets.UpdateRecord` rejects a row that was
    /// read outside the selection as well as one whose new values would move
    /// it outside.  The former is normally guaranteed by the read path; the
    /// latter is the write-boundary check that prevents a selected `p0` row
    /// from silently becoming a `p1` row.
    pub fn validate_update_partitions(
        &self,
        old_row: &[Datum],
        new_row: &[Datum],
        selected: &[i64],
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Result<(), KvTableError> {
        self.validate_insert_partitions(old_row, selected, ctx)?;
        self.validate_insert_partitions(new_row, selected, ctx)
    }

    /// The definition ordinal and physical table id each HANDLE routes to.
    ///
    /// This is Go `BatchPointGetPlan.getPartitionIdxs` before its caller
    /// removes handles outside the statement's explicit/pruned partition
    /// set. Keeping the route aligned with the handle list lets the executor
    /// issue one physical row read per handle instead of probing every
    /// partition.
    pub fn handle_partition_routes(
        &self,
        handles: &[TableHandle],
        zone: &SessionTimeZone,
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Vec<Option<(usize, i64)>> {
        if self.partition.is_none() {
            return vec![None; handles.len()];
        }
        handles
            .iter()
            .map(|handle| {
                let mut row = vec![Datum::Null; self.columns.len()];
                self.fill_handle_columns(&mut row, handle, zone).ok()?;
                self.row_partition_route(&row, ctx)
            })
            .collect()
    }

    /// Routes a complete or key-only source row to its readable partition.
    ///
    /// Go's common-handle batch point get uses `IndexValues`, not the encoded
    /// handle, because a collation sort key may not contain the original SQL
    /// value. Integer-handle callers can still build the row from the handle.
    pub fn row_partition_route(
        &self,
        row: &[Datum],
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Option<(usize, i64)> {
        let partition = self.partition.as_ref()?;
        let ordinal = partition.locate_ordinal(row, &self.columns, ctx).ok()?;
        let definition = partition.definitions.get(ordinal)?;
        self.record_physical_ids()
            .contains(&definition.id)
            .then_some((ordinal, definition.id))
    }

    /// Go `BatchPointGetPlan.AccessObject().Partitions`: the partitions a set
    /// of HANDLES routes into, named as declared and in DEFINITION order,
    /// deduplicated.
    ///
    /// Empty for an unpartitioned table, which is what makes the caller's
    /// annotation unconditional: `partition:` is printed exactly when there
    /// is something to print.
    ///
    /// Go builds one zero-valued row per handle, copies ONLY the handle
    /// column into it (`physical_batch_point_get.go:900`), and evaluates the
    /// partition expression over that -- so a partition expression reading a
    /// column the handle does not determine sees NULL, exactly as here. A
    /// handle that routes nowhere (`ErrNoPartitionForGivenValue`) is Go's
    /// `pIdx = -1`, which drops the handle from the plan altogether; it
    /// contributes no partition name either way, which is all this answers.
    ///
    /// # Recorded (`tests/integrationtest/r/planner/core/partition_pruner.result`)
    ///
    /// ```text
    /// create table t (a int primary key, b int, key (b))
    ///   partition by hash(a) (partition P0, partition p1, partition P2);
    /// explain format = 'brief' select * from t where a IN (1, 2);
    ///   Batch_Point_Get  2.00  root  table:t, partition:p1,P2  handle:[1 2], ...
    /// ```
    ///
    /// The names keep the case they were DECLARED in (`p1,P2`), not the case
    /// the statement wrote, and the order is the definition order rather than
    /// the handle order -- Go sorts the ordinals before naming them.
    #[must_use]
    pub fn handle_partition_names(
        &self,
        handles: &[TableHandle],
        zone: &SessionTimeZone,
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Vec<String> {
        let Some(partition) = &self.partition else {
            return Vec::new();
        };
        let mut ordinals: Vec<usize> = Vec::new();
        for (ordinal, _) in self
            .handle_partition_routes(handles, zone, ctx)
            .into_iter()
            .flatten()
        {
            if !ordinals.contains(&ordinal) {
                ordinals.push(ordinal);
            }
        }
        ordinals.sort_unstable();
        ordinals
            .into_iter()
            .filter_map(|ordinal| partition.definitions.get(ordinal))
            .map(|definition| definition.name.clone())
            .collect()
    }

    /// Every physical table id a READ of this table has to touch, ascending.
    ///
    /// A read that has only a HANDLE cannot route -- the partition is a
    /// function of the ROW -- so it probes these in turn. For an ordinary
    /// table that is the single-element list holding the table id, which is
    /// why the probe needs no unpartitioned special case. When something has
    /// narrowed the read (`PARTITION (p)`, pruning) it is that narrower list;
    /// see [`KvTable::read_partitions`].
    pub fn record_physical_ids(&self) -> Vec<i64> {
        if let Some(ids) = &self.read_partitions {
            return ids.clone();
        }
        self.partition
            .as_ref()
            .map_or_else(|| vec![self.table_id], |p| p.physical_ids())
    }

    /// The id a READ of this table looks its STATISTICS up under: Go's
    /// `DataSource.PhysicalTableID`, which
    /// `ds.StatisticTable = stats.GetStatsTable(ds.SCtx(), ds.TableInfo,
    /// ds.PhysicalTableID)` (`planner/core/stats.go`) passes straight
    /// through.
    ///
    /// A read narrowed to ONE physical table is that table -- an ordinary
    /// table, or a single partition under static pruning, where Go builds a
    /// DataSource per partition and `ANALYZE` stores a histogram per physical
    /// id without merging a logical one. A read spanning several is the
    /// logical table, which is the id dynamic pruning analyzes and stores its
    /// merged histogram under.
    ///
    /// Reading a partition's rows while asking the LOGICAL id for their
    /// distribution is why `analyze table tstring all columns` left
    /// `select * from tstring where a<='b'` printing `stats:pseudo` over a
    /// partition TiDB has real statistics for.
    pub fn stats_physical_id(&self) -> i64 {
        match self.record_physical_ids().as_slice() {
            [only] => *only,
            _ => self.table_id,
        }
    }

    /// Restricts every READ of this handle to `ids`, which must be physical
    /// partition ids of this table in ascending order.
    ///
    /// Narrowing is CUMULATIVE: a second call intersects with the first, so
    /// `SELECT ... FROM t PARTITION (p1) WHERE a < 10` reads neither more
    /// than `p1` nor more than the predicate admits, whichever order the two
    /// restrictions arrive in.
    pub fn restrict_read_to_partitions(&mut self, ids: &[i64]) {
        let narrowed = match &self.read_partitions {
            Some(existing) => existing
                .iter()
                .copied()
                .filter(|id| ids.contains(id))
                .collect(),
            None => ids.to_vec(),
        };
        self.read_partitions = Some(narrowed);
    }

    /// Go FindIndexByColumnsForForeignKey, also used by ALTER validation.
    /// The outer Option distinguishes a missing access path from an integer
    /// clustered record key, which needs no secondary index.
    pub fn foreign_key_lookup_index(&self, columns: &[usize]) -> Option<Option<i64>> {
        if columns.len() == 1 && self.pk_handle_offset == Some(columns[0]) {
            return Some(None);
        }
        self.indexes
            .iter()
            .find(|index| {
                index.column_offsets.starts_with(columns)
                    && columns.iter().enumerate().all(|(position, offset)| {
                        let length = index.prefix_length(position);
                        length == tidb_datatype::UNSPECIFIED_LENGTH
                            || length >= self.columns[*offset].field_type.flen()
                    })
                    && self.partial_index_safe_for_columns(index, columns)
            })
            .map(|index| Some(index.id))
    }

    /// The stored record for `handle` -- its key AND its bytes -- found by
    /// probing the partitions, or `None` when none holds it.
    ///
    /// The bytes come back with the key because every caller that wants the
    /// key wants either the row or nothing, and fetching them separately
    /// would double the round trips a point read costs -- which the access-
    /// path tests count.
    pub fn stored_record(
        &mut self,
        handle: &TableHandle,
    ) -> Result<Option<(Key, Vec<u8>)>, KvTableError> {
        if self.read_partitions.is_none() && self.partition.is_none() {
            return read_stored_record_one(self.store.as_mut(), self.table_id, handle);
        }
        let physical_ids = self.record_physical_ids();
        read_stored_record(self.store.as_mut(), physical_ids, handle)
    }

    /// The record key each of `handles` names, Go `EncodeRowKeyWithHandle(tID,
    /// handle)` with `tID` the routed partition (`planPhysIDs`) or the table.
    /// `None` when a partitioned table's handle is not routed to one
    /// partition, so no single key can be named before reading.
    pub fn record_lock_keys(
        &self,
        handles: &[TableHandle],
        physical_ids: Option<&[i64]>,
    ) -> Option<Vec<Key>> {
        match physical_ids {
            Some(ids) if ids.len() == handles.len() => Some(
                handles
                    .iter()
                    .zip(ids)
                    .map(|(handle, id)| {
                        Key::from_bytes(encode_row_key_with_handle(*id, &handle.record_handle()))
                    })
                    .collect(),
            ),
            Some(_) => None,
            None if self.read_partitions.is_none() && self.partition.is_none() => Some(
                handles
                    .iter()
                    .map(|handle| {
                        Key::from_bytes(encode_row_key_with_handle(
                            self.table_id,
                            &handle.record_handle(),
                        ))
                    })
                    .collect(),
            ),
            None => None,
        }
    }

    /// Go `PointGetExecutor.lockKeyBase` / `LockKeys` over this table's
    /// storage (see [`TableStorage::lock_keys_for_read`]).
    pub fn lock_keys_for_read(
        &mut self,
        keys: &[Key],
        wait: tidb_txnkv::transaction::LockWaitTime,
        only_if_exists: bool,
    ) -> Result<(), KvTableError> {
        if keys.is_empty() {
            return Ok(());
        }
        self.store
            .lock_keys_for_read(keys, wait, only_if_exists)
            .map_err(KvTableError::from)
    }

    /// Go `doLockKeys` over this table's storage (see
    /// [`TableStorage::do_lock_keys`]).
    pub fn do_lock_keys(
        &mut self,
        keys: &[Key],
        wait: tidb_txnkv::transaction::LockWaitTime,
    ) -> Result<(), KvTableError> {
        if keys.is_empty() {
            return Ok(());
        }
        self.store
            .do_lock_keys(keys, wait)
            .map_err(KvTableError::from)
    }

    /// [`KvTable::stored_record`] without the bytes.
    fn stored_record_key(&mut self, handle: &TableHandle) -> Result<Option<Key>, KvTableError> {
        Ok(self.stored_record(handle)?.map(|(key, _)| key))
    }

    /// One stored record per requested handle, in request order; `None`
    /// where the row is absent.
    ///
    /// Reads through one batched storage call across physical partitions -- Go's
    /// `BatchPointGetExec.initialize` fetching every handle with a single
    /// `BatchGet` -- instead of one point read per handle. A handle whose
    /// row is absent yields `None`, Go's point get that finds nothing.
    pub fn stored_records_batched(
        &mut self,
        handles: &[TableHandle],
        physical_ids: Option<&[i64]>,
    ) -> Result<PointReadValues, KvTableError> {
        let mut rows = vec![None; handles.len()];
        // The storage layer groups keys by region and schedules those reads
        // together. Splitting here by physical table would serialize otherwise
        // independent partitions. Keep the output position beside each key.
        let mut keys = Vec::with_capacity(handles.len());
        let mut positions = Vec::with_capacity(handles.len());
        let partition_ids = self.record_physical_ids();
        for (index, handle) in handles.iter().enumerate() {
            let record_handle = handle.record_handle();
            match physical_ids.and_then(|ids| ids.get(index).copied()) {
                Some(id) => {
                    keys.push(Key::from_bytes(encode_row_key_with_handle(
                        id,
                        &record_handle,
                    )));
                    positions.push(index);
                }
                None => {
                    // Unrouted handles retain stored_record's ascending
                    // candidate order; never use the logical table prefix.
                    for id in &partition_ids {
                        keys.push(Key::from_bytes(encode_row_key_with_handle(
                            *id,
                            &record_handle,
                        )));
                        positions.push(index);
                    }
                }
            }
        }
        if keys.is_empty() {
            return Ok(PointReadValues::Batch {
                keys: rows,
                values: Default::default(),
            });
        }
        let found = self.store.batch_get(&keys).map_err(KvTableError::from)?;
        for (index, key) in positions.into_iter().zip(keys) {
            // The first matching candidate answers an unrouted handle.
            if rows[index].is_none() {
                if found.contains_key(&key) {
                    rows[index] = Some(key);
                }
            }
        }
        Ok(PointReadValues::Batch {
            keys: rows,
            values: found,
        })
    }

    /// Records a foreign key this table declares.
    pub fn add_foreign_key(&mut self, foreign_key: KvForeignKey) {
        self.foreign_keys.push(foreign_key);
    }

    /// Go `TableInfo.MaxForeignKeyID`: the counter an UNNAMED constraint is
    /// named after (`fk_1`, `fk_2`, ...). It only ever rises, so dropping
    /// `fk_1` and adding another unnamed constraint yields `fk_2` rather than
    /// reusing the freed name.
    pub fn max_foreign_key_id(&self) -> i64 {
        self.meta.read().max_foreign_key_id
    }

    /// Removes the constraint with this name, reporting whether one went.
    ///
    /// Go `dropForeignKey` rebuilds `TableInfo.ForeignKeys` without it and
    /// leaves `MaxForeignKeyID` and the index the constraint relied on alone
    /// -- the index outlives the constraint and has to be dropped by name.
    pub fn drop_foreign_key(&mut self, name: &str) -> bool {
        let before = self.foreign_keys.len();
        self.foreign_keys.retain(|key| !key.name.go_lower_eq(name));
        self.foreign_keys.len() != before
    }

    /// The foreign keys this table declares (the child side).
    #[must_use]
    pub fn foreign_keys(&self) -> &[KvForeignKey] {
        &self.foreign_keys
    }

    /// Go `TableInfo.Charset`/`Collate`: the table's default character set
    /// and collation, which its unqualified string columns inherit.
    #[must_use]
    pub fn charset(&self) -> TableCharset {
        let meta = self.meta.read();
        TableCharset {
            charset: tidb_charset::Charset::from_name(&meta.charset)
                .unwrap_or(TableCharset::default().charset),
            collation: tidb_charset::Collation::from_name(&meta.collate)
                .unwrap_or(TableCharset::default().collation),
        }
    }

    pub fn row_id_allocator(&self) -> &AutoIdAllocator {
        self.row_id.as_ref().unwrap_or(&self.auto_id)
    }

    /// Installs Go's separate RowIDAllocType for a SepAutoInc table.
    pub fn set_row_id(&mut self, allocator: TableAutoId) {
        self.row_id = Some(allocator.0);
    }

    /// Go `TableInfo.AutoIDCache`: how many ids one reservation takes, zero
    /// when the table set none.
    #[must_use]
    pub fn auto_id_cache(&self) -> i64 {
        self.meta.read().auto_id_cache
    }

    /// Exchanges this table's row storage for `store`, returning the one it
    /// held.
    ///
    /// The ONE caller is the session's GLOBAL temporary table overlay. Go
    /// keeps such a table's `TableInfo` in the shared infoschema and its rows
    /// in the SESSION -- reads go through
    /// `temptable.TemporaryTableSnapshotInterceptor`, which answers an empty
    /// iterator for a global temporary table so the shared store is never
    /// read at all. This tier has one table object per name, so the swap is
    /// how a session gets its own rows under the shared schema; without it,
    /// two connections writing the same global temporary table would see each
    /// other's rows, which is the single thing the type exists to prevent.
    pub fn swap_storage(&mut self, store: Box<dyn TableStorage>) -> Box<dyn TableStorage> {
        std::mem::replace(&mut self.store, store)
    }

    /// Go `TempTableFromMeta`, the entry `TxnCtx.TemporaryTables` holds for a
    /// GLOBAL temporary table: empty rows and `NewAllocatorFromTempTblInfo`'s
    /// in-memory allocator, ONE for the auto-increment column and
    /// `_tidb_rowid` alike, rebased to `AutoIncID - 1` when CREATE set it.
    /// A transaction starts both afresh, which is why its first id is 1
    /// again.
    #[must_use]
    pub fn new_temporary_txn_data(&self) -> TemporaryTableTxnData {
        TemporaryTableTxnData {
            store: Box::new(MemTableStorage::new()),
            auto_id: AutoIdAllocator::temporary(self.auto_id.unsigned, self.meta.read().auto_inc_id),
            row_id: None,
        }
    }

    /// Gives a LOCAL temporary table the allocator Go's
    /// `temptable.CreateLocalTemporaryTable` builds for it from its
    /// `TableInfo`: in-memory and session-owned, one id at a time.
    pub fn use_local_temporary_auto_id(&mut self) {
        self.auto_id = AutoIdAllocator::temporary(self.auto_id.unsigned, self.meta.read().auto_inc_id);
        self.row_id = None;
    }

    /// Exchanges this table's rows and allocators for a session's
    /// transaction-scoped ones (see [`Self::new_temporary_txn_data`]),
    /// returning what it held. The session's GLOBAL temporary table overlay
    /// swaps them in for a statement and back out afterwards, as
    /// [`Self::swap_storage`] does for the rows alone.
    pub fn swap_temporary_txn_data(&mut self, data: TemporaryTableTxnData) -> TemporaryTableTxnData {
        TemporaryTableTxnData {
            store: std::mem::replace(&mut self.store, data.store),
            auto_id: std::mem::replace(&mut self.auto_id, data.auto_id),
            row_id: std::mem::replace(&mut self.row_id, data.row_id),
        }
    }

    /// Whether this table's row backend is rolled back by the cluster
    /// session's outer statement savepoint rather than by a catalog image.
    #[must_use]
    pub fn has_external_statement_rollback(&self) -> bool {
        self.store.has_external_statement_rollback()
    }

    /// Go snapshot runtime `CmdGet` and `CmdBatchGet` totals for this table's
    /// statement snapshot.
    pub fn point_rpc_counts(&mut self) -> (u64, u64) {
        self.store.point_rpc_counts()
    }

    /// Counts the keys this copy's range scans and handle lookups process
    /// against the statement's budget, as Go's coprocessor reads add each
    /// response's `ScanDetail.ProcessedKeys`. Reader executors install it on
    /// their own copy; point gets, foreign-key checks and DDL reads stay
    /// outside it, as they stay outside Go's coprocessor.
    pub fn count_keys_read(&mut self, budget: &tidb_kv::keys_read::KeysReadBudget) {
        // A union scan's mem reader walks the membuffer, which no
        // coprocessor processes.
        if !self.store.reads_processed_keys() {
            return;
        }
        self.keys_read = Some(budget.clone());
    }

    /// Opens one scan range, counting the keys it moves past when this copy
    /// carries a statement budget.
    pub fn scan_iterator(
        &mut self,
        low: &Key,
        high: &Key,
        descending: bool,
    ) -> Result<Box<dyn StorageIterator>, tidb_kv::storage::StorageError> {
        let iterator = if descending {
            self.store.iter_reverse(Some(high), Some(low))?
        } else {
            self.store.iter(Some(low), Some(high))?
        };
        Ok(match &self.keys_read {
            Some(budget) => Box::new(KeysReadIterator {
                inner: iterator,
                budget: budget.clone(),
                charged: 0,
            }),
            None => iterator,
        })
    }

    /// Adds the rows a handle lookup found, the keys Go's table-side
    /// coprocessor scan processes.
    pub fn note_keys_read(&self, keys: u64) -> Result<(), tidb_kv::storage::StorageError> {
        match &self.keys_read {
            Some(budget) if keys > 0 => budget.add(keys),
            _ => Ok(()),
        }
    }

    /// Installs point-reader options on the same backend that serves its keys.
    pub fn configure_snapshot_read(&self, ctx: &dyn tidb_tblctx::MutateContext, avg_row_bytes: f64) {
        self.store
            .set_snapshot_read_options(ctx.snapshot_read_options(avg_row_bytes));
    }

    /// Installs exactly `options` on the backend that serves this table's
    /// keys (Go `snapshot.SetOption` of a point reader's options).
    pub fn set_snapshot_read_options(&self, options: tidb_txnkv::SnapshotReadOptions) {
        self.store.set_snapshot_read_options(options);
    }

    pub fn point_reader(&self) -> PointRead<'_> {
        PointRead {
            table: self,
            store: self.store.clone(),
        }
    }


    /// Go `TableInfo.TempTableType`.
    #[must_use]
    pub fn temp_table_type(&self) -> tidb_model::TempTableType {
        self.meta.read().temp_table_type
    }

    /// Go `TableInfo.TableCacheStatusType`.
    #[must_use]
    pub fn cache_status(&self) -> tidb_model::TableCacheStatusType {
        self.meta.read().table_cache_status_type
    }

    /// Whether this table is temporary at all, in either scope -- Go's
    /// `tbl.Meta().TempTableType != model.TempTableNone`, which is the exact
    /// test every 8006 refusal makes.
    #[must_use]
    pub fn is_temporary(&self) -> bool {
        self.temp_table_type().0 != tidb_model::TempTableType::NONE.0
    }


    /// The table's `TTL` configuration, when it has one.
    #[must_use]
    pub fn ttl_info(&self) -> Option<tidb_model::TTLInfo> {
        self.meta.read().ttl_info.as_ref().map(|info| info.read().clone())
    }


    /// Go `TableInfo.ShardRowIDBits`.
    #[must_use]
    pub fn shard_row_id_bits(&self) -> u64 {
        self.meta.read().shard_row_id_bits
    }

    /// Go `verifyNoOverflowShardBits`: the next global row id must leave the
    /// new shard bits clear (`tables.OverflowShardBits`, sign bit reserved).
    pub fn verify_no_overflow_shard_bits(&self, bits: u64) -> Result<(), String> {
        if bits == 0 {
            return Ok(());
        }
        let next = self
            .row_id_allocator()
            .next_for_show()
            .map_err(|error| error.to_string())?;
        let mask = ((1u64 << bits) - 1) << (64 - bits - 1);
        if next & mask > 0 {
            return Err(format!(
                "shard_row_id_bits {bits} will cause next global auto ID {next} overflow"
            ));
        }
        Ok(())
    }


    /// Go `TableInfo.PreSplitRegions`.
    #[must_use]
    pub fn pre_split_regions(&self) -> u64 {
        self.meta.read().pre_split_regions
    }

    /// Go `TableInfo.UpdateTS`.
    #[must_use]
    pub fn update_ts(&self) -> u64 {
        self.meta.read().update_ts
    }



    /// The table-level comment served by metadata statements.
    #[must_use]
    pub fn comment(&self) -> String {
        self.meta.read().comment.clone()
    }

    /// The table's `COMPRESSION` setting. Go records whatever string the
    /// option carries and `SHOW CREATE TABLE` prints it only when non-empty.
    #[must_use]
    pub fn compression(&self) -> String {
        self.meta.read().compression.clone()
    }


    /// Marks the columns whose encoding is the clustered row handle, which Go
    /// records as `TableInfo.IsCommonHandle`.
    pub fn set_common_handle_offsets(&mut self, offsets: Vec<usize>) {
        self.common_handle_offsets = offsets;
        self.common_handle_prefix_lengths = vec![tidb_datatype::UNSPECIFIED_LENGTH; self.common_handle_offsets.len()];
    }

    /// Records Go `IndexColumn.Length` for each common-handle component.
    pub fn set_common_handle_prefix_lengths(&mut self, lengths: Vec<i64>) {
        assert_eq!(lengths.len(), self.common_handle_offsets.len());
        self.common_handle_prefix_lengths = lengths;
    }

    /// Sets Go `TableInfo.CommonHandleVersion` of a table built from
    /// columns.
    pub fn set_common_handle_version(&mut self, version: u16) {
        self.update_meta(|meta| meta.common_handle_version = version);
    }

    /// Go `TableInfo.CommonHandleVersion`.
    #[must_use]
    pub fn common_handle_version(&self) -> u16 {
        self.meta.read().common_handle_version
    }


    /// Go `TableInfo.TableSplitPolicy`.
    #[must_use]
    pub fn table_split_policy(&self) -> Option<tidb_model::RegionSplitPolicy> {
        self.meta.read().table_split_policy.as_ref().map(|policy| policy.read().clone())
    }

    /// Go `IndexInfo.RegionSplitPolicy` of the index `index_id`.
    #[must_use]
    pub fn index_split_policy(&self, index_id: i64) -> Option<tidb_model::RegionSplitPolicy> {
        self.meta
            .read()
            .indices
            .iter_deref()
            .find_map(|index| {
                let index = index.read();
                (index.id == index_id)
                    .then(|| index.region_split_policy.as_ref().map(|policy| policy.read().clone()))
                    .flatten()
            })
    }

    /// Sets Go `TableInfo.ExchangePartitionInfo`: (the other table's ID, the
    /// exchanged partition's ID).
    pub fn set_exchange_partition_info(&mut self, info: Option<(i64, i64)>) {
        self.update_meta(|meta| {
            meta.exchange_partition_info = info.map(|(table_id, def_id)| {
                tidb_model::GoShared::new(tidb_model::table::ExchangePartitionInfo {
                    exchange_partition_table_id: table_id,
                    exchange_partition_def_id: def_id,
                    ..Default::default()
                })
            });
        });
    }

    /// Go `TableInfo.ExchangePartitionInfo`: (the other table's ID, the
    /// exchanged partition's ID).
    #[must_use]
    pub fn exchange_partition_info(&self) -> Option<(i64, i64)> {
        self.meta.read().exchange_partition_info.as_ref().map(|info| {
            let info = info.read();
            (info.exchange_partition_table_id, info.exchange_partition_def_id)
        })
    }

    /// Sets the new table ID a PARTITION BY / REMOVE PARTITIONING gives the
    /// table, and the global indexes it created under that ID.
    pub fn set_new_table_global_indexes(&mut self, indexes: Option<(i64, Vec<i64>)>) {
        self.new_table_global_indexes = indexes;
    }

    /// Go `index.GenIndexKey`'s table ID for a global index: the table's,
    /// or the new one for an index a PARTITION BY / REMOVE PARTITIONING
    /// created.
    #[must_use]
    pub(crate) fn global_index_table_id(&self, index_id: i64) -> i64 {
        match &self.new_table_global_indexes {
            Some((new_table_id, indexes)) if indexes.contains(&index_id) => *new_table_id,
            _ => self.table_id,
        }
    }

    /// The physical table `row` routes into (Go `locatePartition`), the
    /// table itself when it is not partitioned.
    ///
    /// # Errors
    ///
    /// A row no partition accepts.
    pub fn locate_physical_id(
        &self,
        row: &[Datum],
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Result<i64, KvTableError> {
        self.record_physical_id(row, ctx)
    }

    /// Go `TableInfo.Affinity`.
    #[must_use]
    pub fn affinity(&self) -> Option<tidb_model::TableAffinityInfo> {
        self.meta.read().affinity.as_ref().map(|affinity| affinity.read().clone())
    }

    /// Whether Go `TableInfo.Affinity` is non-nil.
    #[must_use]
    pub fn has_affinity(&self) -> bool {
        self.meta.read().affinity.is_some()
    }

    /// Go `TableInfo.EngineAttribute`.
    #[must_use]
    pub fn engine_attribute(&self) -> String {
        self.meta.read().engine_attribute.clone()
    }

    /// Go `TableInfo.StorageClassString`.
    #[must_use]
    pub fn storage_class_string(&self) -> String {
        self.meta.read().storage_class_string()
    }

    /// Go `TableInfo.TiFlashReplica`.
    #[must_use]
    pub fn tiflash_replica(&self) -> Option<tidb_model::TiFlashReplicaInfo> {
        self.meta.read().tiflash_replica.as_ref().map(|replica| replica.read().clone())
    }

    /// The clustered primary key's column offsets, empty when there is none.
    #[must_use]
    pub fn common_handle_offsets(&self) -> &[usize] {
        &self.common_handle_offsets
    }

    /// Prefix lengths for the clustered primary-key columns.
    #[must_use]
    pub fn common_handle_prefix_lengths(&self) -> &[i64] {
        &self.common_handle_prefix_lengths
    }

    /// Encodes one complete clustered composite primary-key tuple as its
    /// record handle.
    ///
    /// Index-join probes use this path because part of the tuple may come
    /// from constants and the remainder from an outer row; routing the tuple
    /// through the table's collation mode and `CommonHandle` padding keeps it
    /// byte-identical to the handle created by an INSERT.
    pub fn common_handle_of_values(
        &self,
        values: &[Datum],
        zone: &SessionTimeZone,
    ) -> Result<TableHandle, KvTableError> {
        if self.common_handle_offsets.len() != values.len() {
            return Err(KvTableError::Encode(
                "a common handle probe does not cover every key column".to_owned(),
            ));
        }
        self.common_handle_from_values(values, zone)
    }

    /// Go `TableInfo.PKIsHandle` for one column: whether this offset IS the
    /// integer primary key that serves as the row handle, and so is
    /// addressable without an index of its own.
    #[must_use]
    pub fn is_clustered_handle_column(&self, offset: usize) -> bool {
        self.pk_handle_offset == Some(offset)
    }

    /// The column offsets the handle carries, which the row value omits
    /// (Go `CanSkip`: a PK handle column and a full-length common-handle
    /// column are both skipped from the encoded row).
    fn handle_column_offsets(&self) -> Vec<usize> {
        match self.pk_handle_offset {
            Some(offset) => vec![offset],
            None => self.common_handle_offsets.clone(),
        }
    }

    /// Builds one enforced, writable CHECK constraint from persisted metadata.
    pub fn add_check_constraint(
        &mut self,
        name: impl Into<String>,
        expression: &tidb_ast::Expr,
        zone: &tidb_datatype::SessionTimeZone,
    ) -> Result<(), KvTableError> {
        self.add_check_constraint_with_like_default_escape(name, expression, zone, b'\\')
    }

    /// Statement-aware form of [`Self::add_check_constraint`].
    pub(crate) fn add_check_constraint_with_like_default_escape(
        &mut self,
        name: impl Into<String>,
        expression: &tidb_ast::Expr,
        zone: &tidb_datatype::SessionTimeZone,
        like_default_escape: u8,
    ) -> Result<(), KvTableError> {
        let name = name.into();
        let compiled =
            self.compile_check_constraint(&name, expression, zone, like_default_escape)?;
        std::sync::Arc::make_mut(&mut self.check_constraints).push(compiled);
        Ok(())
    }

    /// Replaces Go `TableInfo.Constraints` and rebuilds the ordinary writable
    /// constraint list from that metadata.
    pub fn set_check_constraint_infos(
        &mut self,
        infos: Vec<tidb_model::table::ConstraintInfo>,
        zone: &tidb_datatype::SessionTimeZone,
        like_default_escape: u8,
    ) -> Result<(), KvTableError> {
        let mut compiled = Vec::new();
        for info in &infos {
            if !info.enforced
                || info.state == tidb_model::SchemaState::DELETE_ONLY
                || info.state == tidb_model::SchemaState::DELETE_REORGANIZATION
            {
                continue;
            }
            let expression = tidb_model::generated_expr::parse_expression(&info.expr_string)
                .map_err(|error| KvTableError::CheckConstraint {
                    name: info.name.original().to_owned(),
                    detail: format!("{error:?}"),
                    eval: None,
                })?;
            compiled.push(self.compile_check_constraint(
                info.name.original(),
                &expression,
                zone,
                like_default_escape,
            )?);
        }
        // Go's `TableInfo.MaxConstraintID` is an allocator high-water mark,
        // not the largest ID still present.  In particular, dropping the
        // newest constraint must not make its ID reusable.
        let max_id = infos.iter().map(|info| info.id).max().unwrap_or(0);
        self.update_meta(|meta| meta.max_constraint_id = meta.max_constraint_id.max(max_id));
        self.check_constraint_infos = std::sync::Arc::new(infos);
        self.check_constraints = std::sync::Arc::new(compiled);
        Ok(())
    }


    /// Go `TableInfo.Constraints`.
    #[must_use]
    pub fn check_constraint_infos(&self) -> &[tidb_model::table::ConstraintInfo] {
        &self.check_constraint_infos
    }


    fn compile_check_constraint(
        &self,
        name: &str,
        expression: &tidb_ast::Expr,
        zone: &tidb_datatype::SessionTimeZone,
        like_default_escape: u8,
    ) -> Result<KvCheckConstraint, KvTableError> {
        let names = self
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let types = self
            .columns
            .iter()
            .map(|column| column.field_type.clone())
            .collect::<Vec<_>>();
        let resolver = crate::generated_column::TableColumnResolver::with_like_default_escape(
            &names,
            &types,
            zone.clone(),
            like_default_escape,
        );
        let expr =
            tidb_expr::rewriter::rewrite_expr_resolved(expression, &resolver).map_err(|error| {
                KvTableError::CheckConstraint {
                    name: name.to_owned(),
                    detail: format!("{error:?}"),
                    eval: None,
                }
            })?;
        if let Some(missing) = resolver.missing_name() {
            return Err(KvTableError::CheckConstraint {
                name: name.to_owned(),
                detail: format!("unknown column '{missing}'"),
                eval: None,
            });
        }
        Ok(KvCheckConstraint {
            name: name.to_owned(),
            expr,
            dependencies: resolver.dependency_names(),
            source: expression.clone(),
            build_zone: zone.clone(),
            build_like_default_escape: like_default_escape,
            zone_sensitive: resolver.zone_was_read(),
            like_default_escape_sensitive: resolver.like_default_escape_was_read(),
        })
    }

    /// Go `CheckRowConstraintWithDatum` over this table's writable constraints.
    pub fn validate_check_constraints(
        &self,
        row: &[Datum],
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Result<(), KvTableError> {
        for constraint in self.check_constraints.iter() {
            let rebuilt;
            let expression = if (constraint.zone_sensitive
                && constraint.build_zone != ctx.time_zone())
                || (constraint.like_default_escape_sensitive
                    && constraint.build_like_default_escape != ctx.like_default_escape())
            {
                let names = self
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>();
                let types = self
                    .columns
                    .iter()
                    .map(|column| column.field_type.clone())
                    .collect::<Vec<_>>();
                let resolver =
                    crate::generated_column::TableColumnResolver::with_like_default_escape(
                        &names,
                        &types,
                        ctx.time_zone(),
                        ctx.like_default_escape(),
                    );
                rebuilt = tidb_expr::rewriter::rewrite_expr_resolved(&constraint.source, &resolver)
                    .map_err(|error| KvTableError::CheckConstraint {
                        name: constraint.name.clone(),
                        detail: format!("{error:?}"),
                        eval: None,
                    })?;
                &rebuilt
            } else {
                &constraint.expr
            };
            let value = crate::generated_column::eval_over_dependencies(
                expression,
                &constraint.dependencies,
                &*self.columns,
                row,
                ctx,
            )
            .map_err(|error| KvTableError::CheckConstraint {
                name: constraint.name.clone(),
                detail: format!("{error:?}"),
                eval: Some(error),
            })?;
            match tidb_expr::truthy_of(&value).map_err(|error| KvTableError::CheckConstraint {
                name: constraint.name.clone(),
                detail: format!("{error:?}"),
                eval: Some(error),
            })? {
                Some(false) => {
                    return Err(KvTableError::Mysql(
                        tidb_terror::MysqlError::from_terror(
                            &crate::table_errors::ERR_CHECK_CONSTRAINT_VIOLATED
                                .fast_generate_by_args(&[(constraint.name.clone()).into()]),
                        ),
                    ));
                }
                Some(true) | None => {}
            }
        }
        Ok(())
    }

    /// The handle a row's values produce.
    ///
    /// A table with no clustered handle gets Go's `_tidb_rowid`, and that
    /// counter is not a second one: `autoid.NewAllocatorsFromTblInfo` builds
    /// ONE `RowIDAllocType` allocator when `hasRowID || (hasAutoIncID &&
    /// !tblInfo.SepAutoInc())`, and unless `AUTO_ID_CACHE=1` splits them
    /// (`SepAutoInc`), the AUTO_INCREMENT column has no allocator of its own
    /// and draws from that same one. Captured through `gorun`: on `CREATE
    /// TABLE nc (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY NONCLUSTERED,
    /// v INT)` three single-row inserts give ids 1, 3, 5 with `_tidb_rowid`
    /// 2, 4, 6, and ONE three-row insert gives ids 1, 2, 3 with `_tidb_rowid`
    /// 4, 5, 6 -- the statement allocates every column id while building its
    /// rows and every handle while writing them, which is the order this
    /// tier's INSERT already has.
    ///
    /// The shared counter is also the whole of the domain end here: a
    /// non-clustered row consumes TWO ids, so such a table runs out one row
    /// before a clustered one does, and the `1467` it reports is the
    /// allocator's own rule rather than a second bound.
    pub fn handle_of_row(
        &mut self,
        row: &[Datum],
        zone: &SessionTimeZone,
        shard: i64,
    ) -> Result<TableHandle, KvTableError> {
        if !self.common_handle_offsets.is_empty() {
            let values: Vec<Datum> = self
                .common_handle_offsets
                .iter()
                .map(|offset| row.get(*offset).cloned().unwrap_or(Datum::Null))
                .collect();
            return self.common_handle_from_values(&values, zone);
        }
        match self.pk_handle_offset {
            Some(offset) => match row.get(offset) {
                Some(Datum::Int(value)) => Ok(TableHandle::Int(*value)),
                Some(Datum::UInt(value)) => Ok(TableHandle::Int(*value as i64)),
                Some(Datum::Null) | None => Err(KvTableError::Encode(
                    "the primary key column has no value".to_owned(),
                )),
                Some(other) => Err(KvTableError::Encode(format!(
                    "a handle primary key needs an integer value, got {other:?}"
                ))),
            },
            None => {
                // Go AllocHandle uses RowIDAllocType, independent of the
                // service-backed AUTO_INCREMENT authority for SepAutoInc.
                let handle = self
                    .row_id_allocator()
                    .alloc(1, 1)
                    .map_err(|error| match error {
                        AutoIdError::Store(detail) => KvTableError::Encode(detail.0),
                        _ => KvTableError::Mysql(tidb_terror::MysqlError::from_terror(
                            &tidb_autoid::autoid_errors::ERR_AUTOINC_READ_FAILED
                                .generate_with_stack_by_args(&[]),
                        )),
                    })?;
                // Go `AllocHandleIDs`: `if meta.ShardRowIDBits > 0 { base =
                // shardFmt.Compose(shard, base) }`. Composing here rather
                // than at the caller keeps the ONE place a heap table's
                // handle is minted responsible for its whole shape.
                Ok(TableHandle::Int(compose_row_id_shard(
                    self.shard_row_id_bits(),
                    shard,
                    handle as i64,
                )))
            }
        }
    }

    /// Builds the clustered handle named by primary-key values in primary-key
    /// order.
    ///
    /// Go's `BatchPointGetPlan.PrunePartitionsAndValues` takes the values of a
    /// row-valued `IN` over a common primary key through
    /// `EncodeUniqueIndexValuesForKey` and `kv.NewCommonHandle`.  Keeping the
    /// encoding beside [`Self::handle_of_row`] ensures a planned read and a
    /// write produce byte-identical record keys, including the short-key
    /// padding required by `kv.NewCommonHandle`.
    pub fn common_handle_from_values(
        &self,
        values: &[Datum],
        zone: &SessionTimeZone,
    ) -> Result<TableHandle, KvTableError> {
        if values.len() != self.common_handle_offsets.len() {
            return Err(KvTableError::Encode(format!(
                "a common handle needs {} values, got {}",
                self.common_handle_offsets.len(),
                values.len()
            )));
        }
        // Go `AddRecord` runs `tablecodec.TruncateIndexValues` over the
        // primary key before `codec.EncodeKey`: a prefix key column stores
        // only its leading part in the handle.
        let mut stored_values = values.to_vec();
        for (position, (value, offset)) in stored_values
            .iter_mut()
            .zip(self.common_handle_offsets.iter().copied())
            .enumerate()
        {
            let Some(column) = self.columns.get(offset) else {
                return Err(KvTableError::Encode(
                    "a common-handle column is outside the table schema".to_owned(),
                ));
            };
            if let Some(length) = self.common_handle_prefix_lengths.get(position) {
                tidb_tablecodec::cut_datum_by_prefix_len(value, *length, &column.field_type);
            }
        }
        let encoded = tidb_codec::Encoder::new(self.use_new_collation)
            .encode_key_in_timezone(zone, &stored_values)
            .map_err(|e| KvTableError::Encode(format!("{e:?}")))?;
        // Go `kv.NewCommonHandle` pads a short encoding out to nine bytes,
        // and so does the record-key codec this handle is about to be written
        // through. A DECIMAL primary key is the short case: `5` encodes to
        // four bytes.
        let padded = tidb_txnkv::CommonHandle::new(encoded)
            .map_err(|e| KvTableError::Encode(format!("{e:?}")))?;
        Ok(TableHandle::Common(padded.encoded().to_vec()))
    }

    /// The row value bytes, omitting lossless handle columns.
    pub fn encode_row_value(
        &self,
        row: &[Datum],
        zone: &SessionTimeZone,
    ) -> Result<Vec<u8>, KvTableError> {
        let mut ids = Vec::with_capacity(self.columns.len());
        let mut values = Vec::with_capacity(self.columns.len());
        for (offset, column) in self.columns.iter().enumerate() {
            // A common-handle sort key is not the original value when its
            // collation loses information. Go keeps exactly those handle
            // columns in the row bytes and rebuilds only lossless handles
            // from the record key.
            let handle_position = self
                .common_handle_offsets
                .iter()
                .position(|handle_offset| *handle_offset == offset);
            let full_common_handle = handle_position.is_some_and(|position| {
                self.common_handle_prefix_lengths.get(position).copied()
                    == Some(tidb_datatype::UNSPECIFIED_LENGTH)
            });
            let lossless_handle = self.pk_handle_offset == Some(offset)
                || (full_common_handle
                    && !column
                        .field_type
                        .need_restored_data_with_collation(self.use_new_collation));
            if lossless_handle || crate::generated_column::is_virtual(column) {
                continue;
            }
            ids.push(column.id);
            values.push(row.get(offset).cloned().unwrap_or(Datum::Null));
        }
        // Go `AddRecord` / `updateRecord`: a write row past the public
        // columns carries the writable non-public ones too.
        if row.len() > self.columns.len() {
            for (column, value) in self
                .nonpublic_columns
                .iter()
                .zip(&row[self.columns.len()..])
            {
                if column.is_writable() {
                    ids.push(column.column.id);
                    values.push(value.clone());
                }
            }
        }
        encode_table_row(Some(zone), &values, &ids, true, None)
            .map_err(|e| KvTableError::Encode(format!("{e:?}")))
    }

    /// Restores the handle columns into a decoded row, which Go does by
    /// reading `h.IntValue()` or `h.EncodedCol(i)` rather than the value.
    fn fill_handle_columns(
        &self,
        row: &mut [Datum],
        handle: &TableHandle,
        zone: &SessionTimeZone,
    ) -> Result<(), KvTableError> {
        fill_handle_columns(
            &self.columns,
            self.pk_handle_offset,
            &self.common_handle_offsets,
            &self.common_handle_prefix_lengths,
            row,
            handle,
            zone,
            self.use_new_collation,
        )
    }

    /// Empties the table, keeping its schema and indexes.
    ///
    /// Go implements TRUNCATE by replacing the table with a fresh one that has
    /// the same definition and a new id, which is why the rows, the index
    /// entries and the auto-increment counter all start over. Captured from
    /// TiDB: after truncating, the next auto-increment insert gets 1 again.
    pub fn truncate(&mut self) -> Result<(), AutoIdStoreError> {
        self.store.clear();
        self.auto_id.reset()?;
        if let Some(row_id) = &self.row_id {
            row_id.reset()?;
        }
        self.auto_random_id.reset()
    }

    /// Go `temptable.TruncateLocalTemporaryTable` retains metadata but gives
    /// the replacement a new identity, empty storage and memory allocators.
    pub fn recreate_local_temporary(&mut self, table_id: i64) {
        debug_assert_eq!(self.temp_table_type(), tidb_model::TempTableType::LOCAL);
        self.table_id = table_id;
        self.store = Box::new(MemTableStorage::new());
        self.auto_id = AutoIdAllocator::over(
            std::sync::Arc::new(LocalAutoIdStore::new()),
            match self.auto_id_cache() {
                0 => DEFAULT_AUTO_ID_STEP,
                cache => cache as u64,
            },
        );
        if self.row_id.is_some() {
            self.row_id = Some(AutoIdAllocator::new());
        }
        self.auto_random_id = AutoIdAllocator::new();
    }

    /// Sets the table's name, used to qualify a duplicate-key error.
    pub fn set_name(&mut self, name: &str) {
        self.update_meta(|meta| meta.name = tidb_ast::CiString::new(name));
    }

    /// Go `table.Table.Meta()`: the pointer every reader shares.
    #[must_use]
    pub fn meta(&self) -> tidb_model::GoShared<tidb_model::TableInfo> {
        self.meta.clone()
    }

    /// Records the `TableInfo` the loader built this table from.
    pub(crate) fn set_meta(&mut self, meta: tidb_model::TableInfo) {
        self.meta = tidb_model::GoShared::new(meta);
    }

    /// Changes this table's `TableInfo` copy-on-write: a cloned table (an
    /// older catalog image) keeps the pointer it had.
    pub(crate) fn update_meta(&mut self, change: impl FnOnce(&mut tidb_model::TableInfo)) {
        let mut meta = self.meta.read().clone();
        change(&mut meta);
        self.meta = tidb_model::GoShared::new(meta);
    }

    /// The table's name as created (Go `TableInfo.Name.O`), empty when the
    /// table was built without one.
    #[must_use]
    pub fn name(&self) -> String {
        self.meta.read().name.original().to_owned()
    }

    /// Go's key name in a duplicate-entry error: `table.index`.
    pub fn qualified_key(&self, index_name: &str) -> String {
        let meta = self.meta.read();
        match meta.name.original() {
            "" => index_name.to_owned(),
            name => format!("{name}.{index_name}"),
        }
    }

    /// Go `CleanupIndexExec`: deletes every entry of the public index named
    /// `index_name` whose row is no longer stored under its physical table,
    /// and answers how many it removed. Cleaning a clustered PRIMARY KEY
    /// removes nothing.
    ///
    /// `Ok(None)` is no public index of that name.
    pub fn cleanup_index(&mut self, index_name: &str) -> Result<Option<u64>, KvTableError> {
        let Some(index) = self
            .indexes
            .iter()
            .find(|index| index.name.go_lower_eq(index_name))
            .cloned()
        else {
            return Ok(None);
        };
        if index.clustered_primary {
            return Ok(Some(0));
        }
        let mut removed = 0_u64;
        for entry in self.index_entry_records_for_check(index.id)? {
            if read_stored_record_one(self.store.as_mut(), entry.physical_id, &entry.handle)?
                .is_some()
            {
                continue;
            }
            self.store
                .delete(Key::from_bytes(entry.key))
                .map_err(KvTableError::from)?;
            removed += 1;
        }
        Ok(Some(removed))
    }

    /// Removes the index's physical key ranges without evaluating rows. Go's
    /// delete-range worker has the same key ownership; this existing direct
    /// DDL path stages deletion in its transaction, pending durable GC jobs.
    pub fn drop_index(&mut self, name: &str) -> Result<bool, KvTableError> {
        let Some(position) = self
            .indexes
            .iter()
            .position(|index| index.name.go_lower_eq(name))
        else {
            return Ok(false);
        };
        let index = &self.indexes[position];
        let index_id = index.id;
        // DDL owns all physical partitions, even on a read-restricted view.
        let physical_ids = if index.global {
            vec![self.table_id]
        } else {
            self.partition
                .as_ref()
                .map_or_else(|| vec![self.table_id], |p| p.physical_ids())
        };
        let stage = self.store.staging();
        let result = (|| -> Result<(), KvTableError> {
            for physical_id in physical_ids {
                let (low, high) = crate::kv_table::index_key_bounds(physical_id, index_id);
                let mut iterator = self
                    .store
                    .iter(Some(&Key::from_bytes(low)), Some(&Key::from_bytes(high)))?;
                while iterator.valid() {
                    let key = iterator.key().clone();
                    iterator.next()?;
                    self.store.delete(key)?;
                }
                iterator.close();
            }
            Ok(())
        })();
        if let Err(error) = result {
            self.store.cleanup(stage);
            return Err(error);
        }
        self.store.release(stage);
        self.indexes_mut().remove(position);
        self.remove_partial_index_condition(index_id);
        Ok(true)
    }

    /// The next index ID, Go `TableInfo.MaxIndexID + 1`: IDs retired by
    /// earlier schema changes included.
    #[must_use]
    pub fn next_index_id(&self) -> i64 {
        self.meta.read().max_index_id + 1
    }

    /// The columns a user can name or see: everything but the hidden tail.
    ///
    /// A visible column's offset in this slice IS its physical offset, which
    /// is the whole point of keeping the hidden ones last.
    #[must_use]
    pub fn visible_columns(&self) -> &[KvColumn] {
        &self.columns[..self.visible_column_count()]
    }

    /// Columns carried by Go's logical `DataSource`, including hidden
    /// expression-index columns. Index-join key metadata addresses index
    /// offsets in this complete physical row layout.
    #[must_use]
    pub fn logical_columns(&self) -> &[KvColumn] {
        &self.columns
    }

    /// How many columns a user can name or see.
    #[must_use]
    pub fn visible_column_count(&self) -> usize {
        self.columns.len() - self.hidden_columns
    }

    /// Columns Go's logical `DataSource` allocates before appending its
    /// synthetic handle and commit-ts columns. Unlike user-facing wildcard
    /// expansion, this includes hidden expression-index columns.
    #[must_use]
    pub fn logical_data_source_column_count(&self) -> usize {
        self.columns.len()
    }

    /// Whether the column at `offset` is hidden.
    #[must_use]
    pub fn is_hidden(&self, offset: usize) -> bool {
        offset >= self.visible_column_count()
    }

    /// Appends a hidden column -- the one an expression index key part is
    /// rewritten into -- and returns its offset.
    ///
    /// It goes at the very end, so no existing offset moves and the tail
    /// invariant holds by construction.
    pub fn add_hidden_column(&mut self, column: KvColumn) -> usize {
        let offset = self.columns.len();
        self.record_fixture_column(&column, offset, true);
        self.columns_mut().push(column);
        self.hidden_columns += 1;
        self.columns.len() - 1
    }

    /// Adds a column at `position`, which is Go's ALTER TABLE ADD COLUMN.
    ///
    /// The column takes a fresh id, so rows written earlier simply do not
    /// carry it and read back its origin default. Index and handle offsets
    /// shift with the insertion, since they address columns by position.
    ///
    /// `position` is clamped to the VISIBLE width, so `ADD COLUMN` with no
    /// position lands before the hidden tail rather than after it. Captured
    /// from Go: after `alter table te add column z int` on a table with an
    /// expression index, `SHOW CREATE TABLE` prints `a`, `z` and
    /// `information_schema.columns` gives them ordinals 1 and 2.
    pub fn add_column(&mut self, position: usize, column: KvColumn) {
        let position = position.min(self.visible_column_count());
        self.record_fixture_column(&column, position, false);
        self.columns_mut().insert(position, column);
        let shift = |offset: &mut usize| {
            if *offset >= position {
                *offset += 1;
            }
        };
        if let Some(offset) = self.pk_handle_offset.as_mut() {
            shift(offset);
        }
        for offset in &mut self.common_handle_offsets {
            shift(offset);
        }
        if let Some(offset) = self.auto_increment_offset.as_mut() {
            shift(offset);
        }
        if let Some(spec) = self.auto_random.as_mut() {
            shift(&mut spec.offset);
        }
        for index in self.indexes_mut() {
            for offset in &mut index.column_offsets {
                shift(offset);
            }
        }
    }

    /// The next free column id, which Go allocates from `TableInfo.MaxColumnID`
    /// so a dropped id is never reused.
    #[must_use]
    pub fn next_column_id(&self) -> i64 {
        self.meta.read().max_column_id + 1
    }

    /// Removes the column at `offset`, shifting the offsets above it.
    ///
    /// The rows keep the dropped column's bytes, which are simply never read
    /// again because nothing lists that id -- Go likewise leaves the old row
    /// values in place until the table is rewritten.
    pub fn drop_column(&mut self, offset: usize) {
        if self.is_hidden(offset) {
            self.hidden_columns -= 1;
        }
        self.columns_mut().remove(offset);
        let shift = |value: &mut usize| {
            if *value > offset {
                *value -= 1;
            }
        };
        if let Some(value) = self.pk_handle_offset.as_mut() {
            shift(value);
        }
        for value in &mut self.common_handle_offsets {
            shift(value);
        }
        if let Some(value) = self.auto_increment_offset.as_mut() {
            shift(value);
        }
        if let Some(spec) = self.auto_random.as_mut() {
            shift(&mut spec.offset);
        }
        for index in self.indexes_mut() {
            for value in &mut index.column_offsets {
                shift(value);
            }
        }
    }

    /// The handles of the rows a candidate row would collide with: the
    /// clustered key first, then every unique index it duplicates.
    ///
    /// Go's `REPLACE` and `ON DUPLICATE KEY UPDATE` both start here
    /// (`addRecord` reports the conflicting handle rather than only the
    /// error), which is why a single REPLACE can delete more than one row --
    /// captured: replacing a row that duplicates one row's primary key and
    /// another row's unique key deletes BOTH.
    ///
    /// An UPDATE reports a conflict as Go's table layer does (`addIndices`,
    /// `getDuplicateError`): the key's values, a prefix column cut to its
    /// prefix.
    pub fn row_conflicts(
        &mut self,
        row: &[Datum],
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Result<Vec<RowConflict>, KvTableError> {
        self.row_conflicts_reported_as(row, None, ctx, DuplicateText::TableLayer)
    }

    /// An INSERT's conflicts, reported as Go's batch checker does
    /// (`getKeysNeedCheckOneRow`); its candidate handle key also includes an
    /// explicitly supplied heap ID.
    pub fn row_conflicts_with_row_id(
        &mut self,
        row: &[Datum],
        row_id: Option<i64>,
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Result<Vec<RowConflict>, KvTableError> {
        self.row_conflicts_reported_as(row, row_id, ctx, DuplicateText::BatchChecker)
    }

    fn row_conflicts_reported_as(
        &mut self,
        row: &[Datum],
        row_id: Option<i64>,
        ctx: &dyn tidb_exprctx::Columns,
        text: DuplicateText,
    ) -> Result<Vec<RowConflict>, KvTableError> {
        let zone = ctx.time_zone();
        let physical_id = self.record_physical_id(row, ctx)?;
        let mut found: Vec<RowConflict> = Vec::new();
        let clustered = self.pk_handle_offset.is_some() || !self.common_handle_offsets.is_empty();
        let candidate_handle = if clustered {
            Some(self.handle_of_row(row, &zone, 0)?)
        } else {
            row_id.map(TableHandle::Int)
        };
        if let Some(handle) = candidate_handle {
            if read_stored_record_one(self.store.as_mut(), physical_id, &handle)?.is_some() {
                // Go `getKeysNeedCheckOneRow`: a common handle reports its
                // columns' row values through `dataToStrings`, an integer
                // handle its value.
                let value = match (text, self.pk_handle_offset) {
                    (DuplicateText::TableLayer, _) if clustered => clustered_key_text(self, row),
                    (DuplicateText::BatchChecker, Some(offset)) => {
                        datum_text(row.get(offset).unwrap_or(&Datum::Null))
                    }
                    (DuplicateText::BatchChecker, None) if clustered => batch_check_key_text(
                        self.common_handle_offsets
                            .iter()
                            .map(|offset| row.get(*offset).unwrap_or(&Datum::Null)),
                    ),
                    _ => row_id.expect("explicit heap handle").to_string(),
                };
                found.push(RowConflict {
                    handle,
                    error: KvTableError::Mysql(tidb_terror::MysqlError::from_terror(
                        &tidb_kv::kv_errors::ERR_KEY_EXISTS.fast_generate_by_args(&[
                            value.into(),
                            (self.qualified_key("PRIMARY")).into(),
                        ]),
                    )),
                    on_record_key: true,
                });
            }
        }
        for index in self.indexes.as_ref().clone() {
            if !index.unique
                || index.clustered_primary
                || !self.index_condition_holds(&index, row, &zone)?
            {
                continue;
            }
            // A distinct entry's key does not carry the handle, so the
            // candidate handle below is only a placeholder for the key build.
            // A multi-valued index checks every element key (Go
            // `getKeysNeedCheckOneRow` iterates `GenIndexKVIter`).
            for entry in
                self.index_entry_keys(&index, row, &TableHandle::Int(0), physical_id, &zone)?
            {
                if !entry.distinct {
                    continue;
                }
                let value = match self.store.get(&Key::from_bytes(entry.key)) {
                    Ok(value) if !value.is_empty() => value,
                    Ok(_) | Err(StorageError::NotFound) => continue,
                    Err(error) => return Err(error.into()),
                };
                let handle = tidb_tablecodec::decode_handle_in_index_value(&value)
                    .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
                let handle = handle.ok_or_else(|| {
                    KvTableError::Decode("index value contains no handle".to_owned())
                })?;
                let handle = index_entries::convert_handle(&handle);
                if !found.iter().any(|conflict| conflict.handle == handle) {
                    found.push(RowConflict {
                        handle,
                        error: KvTableError::Mysql(tidb_terror::MysqlError::from_terror(
                            &tidb_kv::kv_errors::ERR_KEY_EXISTS.fast_generate_by_args(&[
                                // Both renderings read the key's values: Go's
                                // batch checker formats `FetchValues` after
                                // `GenIndexKVIter`'s `Next` cut them in place.
                                match text {
                                    DuplicateText::BatchChecker => {
                                        batch_check_key_text(self.index_values(&index, row).iter())
                                    }
                                    DuplicateText::TableLayer => {
                                        duplicate_value_text(&self.index_values(&index, row))
                                    }
                                }
                                .into(),
                                self.qualified_key(&index.name).into(),
                            ]),
                        )),
                        on_record_key: false,
                    });
                }
            }
        }
        Ok(found)
    }

    /// Go `getKeysNeedCheckOneRow`'s keys for one INSERT row: the record key
    /// of its handle (a clustered handle, or an explicitly supplied heap ID)
    /// and the key of every distinct unique-index entry it would file.
    fn insert_check_keys(
        &mut self,
        row: &[Datum],
        row_id: Option<i64>,
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Result<(Option<Key>, Vec<Key>), KvTableError> {
        let zone = ctx.time_zone();
        let physical_id = self.record_physical_id(row, ctx)?;
        let clustered = self.pk_handle_offset.is_some() || !self.common_handle_offsets.is_empty();
        let handle = if clustered {
            Some(self.handle_of_row(row, &zone, 0)?)
        } else {
            row_id.map(TableHandle::Int)
        };
        let handle_key = handle.map(|handle| {
            Key::from_bytes(encode_row_key_with_handle(
                physical_id,
                &handle.record_handle(),
            ))
        });
        let mut unique_keys = Vec::new();
        for index in self.indexes.as_ref().clone() {
            if !index.unique
                || index.clustered_primary
                || !self.index_condition_holds(&index, row, &zone)?
            {
                continue;
            }
            for entry in
                self.index_entry_keys(&index, row, &TableHandle::Int(0), physical_id, &zone)?
            {
                if entry.distinct {
                    unique_keys.push(Key::from_bytes(entry.key));
                }
            }
        }
        Ok((handle_key, unique_keys))
    }

    /// Go `prefetchUniqueIndices`: one `BatchGet` of every row's handle and
    /// unique keys, filling the snapshot cache the rows' own duplicate
    /// checks then read. Returns the entries found.
    pub fn prefetch_unique_indices(
        &mut self,
        rows: &[(&[Datum], Option<i64>)],
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Result<std::collections::HashMap<Key, Vec<u8>>, KvTableError> {
        let _region = tidb_tracing::start_thread_region_ex("prefetchUniqueIndices");
        let mut keys = Vec::new();
        for (row, row_id) in rows {
            // A row whose keys cannot be built (Go `getKeysNeedCheckOneRow`
            // marks an unroutable row `ignored`) is left to its own check,
            // which reports it; the prefetch only fills the cache.
            let Ok((handle_key, unique_keys)) = self.insert_check_keys(row, *row_id, ctx) else {
                continue;
            };
            keys.extend(handle_key);
            keys.extend(unique_keys);
        }
        // Go `tikvTxn.BatchGet`, the driver transaction's read.
        let _batch_get = tidb_tracing::start_thread_region_ex("tikvTxn.BatchGet");
        self.store.batch_get(&keys).map_err(KvTableError::from)
    }

    /// Go `prefetchConflictedOldRows`: one `BatchGet` of the rows the found
    /// unique keys point at, which an ON DUPLICATE KEY UPDATE or a REPLACE
    /// reads next.
    pub fn prefetch_conflicted_old_rows(
        &mut self,
        rows: &[(&[Datum], Option<i64>)],
        values: &std::collections::HashMap<Key, Vec<u8>>,
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Result<(), KvTableError> {
        let _region = tidb_tracing::start_thread_region_ex("prefetchConflictedOldRows");
        let mut keys = Vec::new();
        for (row, row_id) in rows {
            let Ok(physical_id) = self.record_physical_id(row, ctx) else {
                continue;
            };
            let Ok((_, unique_keys)) = self.insert_check_keys(row, *row_id, ctx) else {
                continue;
            };
            for key in unique_keys {
                let Some(value) = values.get(&key) else {
                    continue;
                };
                let handle = tidb_tablecodec::decode_handle_in_index_value(value)
                    .map_err(|e| KvTableError::Decode(format!("{e:?}")))?
                    .ok_or_else(|| {
                        KvTableError::Decode("index value contains no handle".to_owned())
                    })?;
                let handle = index_entries::convert_handle(&handle);
                keys.push(Key::from_bytes(encode_row_key_with_handle(
                    physical_id,
                    &handle.record_handle(),
                )));
            }
        }
        let _batch_get = tidb_tracing::start_thread_region_ex("tikvTxn.BatchGet");
        self.store.batch_get(&keys).map_err(KvTableError::from)?;
        Ok(())
    }

    /// Checks a batch of ordinary clustered INSERT record keys with one
    /// storage request. `None` means the table shape cannot use this fast
    /// path: a secondary unique index, a partitioned table, or a heap table
    /// still needs the row-by-row conflict machinery because it has more keys
    /// (or a handle allocated as a side effect) to resolve.
    ///
    /// The caller uses the `true` result only for a normal INSERT whose rows
    /// all have distinct, currently absent clustered keys. In that case the
    /// per-row `addRecord` existence probe would repeat this same check. A
    /// staged key from an earlier statement is included by `batch_get`, so an
    /// explicit transaction falls back rather than hiding a duplicate.
    pub fn all_clustered_insert_keys_absent(
        &mut self,
        rows: &[Vec<Datum>],
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Result<Option<bool>, KvTableError> {
        if rows.is_empty()
            || (self.pk_handle_offset.is_none() && self.common_handle_offsets.is_empty())
            || self.partition.is_some()
            || self.indexes.iter().any(|index| !index.clustered_primary)
            || !self.nonpublic_indexes.is_empty()
            || self.record_physical_ids().len() != 1
        {
            return Ok(None);
        }
        let zone = ctx.time_zone();
        let physical_id = self.record_physical_ids()[0];
        let mut seen = HashSet::with_capacity(rows.len());
        let mut keys = Vec::with_capacity(rows.len());
        for row in rows {
            let handle = self.handle_of_row(row, &zone, 0)?;
            let key = Key::from_bytes(encode_row_key_with_handle(
                physical_id,
                &handle.record_handle(),
            ));
            if !seen.insert(key.as_bytes().to_vec()) {
                // Two rows in one VALUES list would otherwise overwrite one
                // another if the per-row probe were skipped.
                return Ok(Some(false));
            }
            keys.push(key);
        }
        let found = self.store.batch_get(&keys).map_err(KvTableError::from)?;
        Ok(Some(found.is_empty()))
    }

    /// Adds an index, whose entries every later write maintains. A
    /// `clustered_primary` index is recorded for the planner but skipped by
    /// every entry write: its key IS the record handle.
    pub fn add_index(&mut self, index: KvIndex, clustered_primary: bool) {
        let mut index = index;
        if clustered_primary {
            index.clustered_primary = true;
        }
        self.record_mv_key_part_source(&index);
        self.record_fixture_index(&index);
        self.indexes_mut().push(index);
    }

    /// A table built from columns alone (a test fixture) records each column
    /// it gains in its `TableInfo`, as a Go fixture's `TableInfo` lists it. A
    /// table `TableFromMeta` loaded already holds every column there.
    fn record_fixture_column(&mut self, column: &KvColumn, offset: usize, hidden: bool) {
        if self.meta.read().cols().iter_deref().any(|info| info.read().id == column.id) {
            return;
        }
        let info = fixture_column_info(column, offset, hidden);
        self.update_meta(|meta| meta.columns.push_go(info));
    }

    /// [`Self::record_fixture_column`] for an index.
    fn record_fixture_index(&mut self, index: &KvIndex) {
        if self.meta.read().indices.iter_deref().any(|info| info.read().id == index.id) {
            return;
        }
        let info = tidb_model::IndexInfo {
            id: index.id,
            name: tidb_ast::CiString::new(index.name.clone()),
            columns: index
                .column_offsets
                .iter()
                .enumerate()
                .map(|(position, offset)| tidb_model::IndexColumn {
                    // A fixture may declare an index over columns it never added.
                    name: self
                        .columns
                        .get(*offset)
                        .map(|column| tidb_ast::CiString::new(column.name.clone()))
                        .unwrap_or_default(),
                    offset: *offset as i64,
                    length: index
                        .prefix_lengths
                        .get(position)
                        .copied()
                        .unwrap_or(tidb_datatype::UNSPECIFIED_LENGTH as i64),
                    ..tidb_model::IndexColumn::default()
                })
                .collect::<Vec<_>>()
                .into(),
            state: tidb_model::SchemaState::PUBLIC,
            unique: index.unique,
            primary: index.primary,
            mv_index: self.mv_key_part_source(index.id).is_some(),
            ..tidb_model::IndexInfo::default()
        };
        self.update_meta(|meta| meta.indices.push_go(info));
    }

    /// Adds an index a DDL is still adding or dropping, which only the write
    /// path maintains (by its state, as Go's `TableFromMeta` keeps every
    /// index that is not `StateNone`).
    pub fn add_nonpublic_index(
        &mut self,
        index: KvIndex,
        state: tidb_model::SchemaState,
        backfill_state: tidb_model::BackfillState,
    ) {
        self.record_mv_key_part_source(&index);
        std::sync::Arc::make_mut(&mut self.nonpublic_indexes).push(NonPublicIndex {
            index,
            state,
            backfill_state,
        });
    }

    /// Go `completeError`'s lookup: the name of the index whose key part is
    /// the column at `offset` -- the last such index, as Go's loop leaves it.
    pub fn func_index_name(&self, offset: usize) -> Option<&str> {
        self.indexes
            .iter()
            .rev()
            .find(|index| index.column_offsets.contains(&offset))
            .map(|index| index.name.as_str())
    }

    /// Records a multi-valued index's source column from its ARRAY key part's
    /// generation dependencies, which is what the cluster loader reads from
    /// `IndexInfo.MVIndex` and the hidden column's `Dependences`; a local
    /// index and a loaded one then look the same to the planner.
    pub fn record_mv_key_part_source(&mut self, index: &KvIndex) {
        let Some(generated) = index
            .column_offsets
            .iter()
            .filter_map(|offset| self.columns.get(*offset))
            .find(|column| column.field_type.is_array())
            .and_then(|column| column.generated.as_ref())
        else {
            return;
        };
        if let [source] = generated.dependencies.as_slice() {
            let source = source.go_to_lower();
            self.mv_key_part_sources.insert(index.id, source);
        }
    }

    /// The AUTO_INCREMENT column's offset in a row, if the table has one.
    #[must_use]
    pub fn auto_increment_column_offset(&self) -> Option<usize> {
        self.auto_increment_offset
    }

    /// Go `NextGlobalAutoID`, which SHOW CREATE TABLE prints: the shared
    /// counter's next id, including peer allocations.
    #[must_use]
    pub fn next_auto_increment_for_show(&self) -> Result<Option<i64>, AutoIdStoreError> {
        self.auto_increment_offset
            .map(|_| self.auto_id.next_for_show().map(|v| v as i64))
            .transpose()
    }

    /// Go `getAutoIncrementID` (`infoschema_reader.go:289-313`): this node's
    /// allocator `Base() + 1`, or 0 while it has drawn nothing -- what
    /// `information_schema.TABLES.AUTO_INCREMENT` and `SHOW TABLE STATUS`
    /// print. `SHOW CREATE TABLE` reads the shared counter instead
    /// ([`Self::next_auto_increment_for_show`]).
    #[must_use]
    pub fn allocated_auto_increment(&self) -> Option<i64> {
        self.auto_increment_offset
            .map(|_| self.auto_id.status_next() as i64)
    }

    /// The table's indexes: every one of them, which is what index
    /// maintenance, `SHOW INDEX`, and `information_schema` read.
    #[must_use]
    pub fn indexes(&self) -> &[KvIndex] {
        &self.indexes
    }

    /// Compiles and records a partial-index predicate beside the index
    /// metadata. The predicate is evaluated for every backfill and row write;
    /// a false/NULL result means that row has no entry in the partial index.
    /// Go `IndexInfo.ConditionExprString`: the partial-index predicate as
    /// SHOW CREATE TABLE prints it, restored with Go's index-condition flags
    /// (`RestoreStringSingleQuotes | RestoreKeyWordLowercase |
    /// RestoreNameBackQuotes | RestoreSpacesAroundBinaryOperation |
    /// RestoreWithoutSchemaName | RestoreWithoutTableName` in
    /// `pkg/ddl/index.go::CheckAndBuildIndexConditionString`). `None` when the
    /// index carries no predicate.
    #[must_use]
    pub fn partial_index_condition_string(&self, index_id: i64) -> Option<String> {
        if let Some(text) = self.partial_index_condition_texts.get(&index_id) {
            return Some(text.clone());
        }
        self.partial_index_conditions
            .get(&index_id)
            .map(|condition| {
                condition
                    .source
                    .restore_with_flags(partial_index_condition_flags())
            })
    }

    /// Go `IndexInfo.ConditionExprString`, compiled beside its index: the
    /// table's writes, backfills and readers honor a partial index through it.
    pub fn add_partial_index_condition(
        &mut self,
        index_id: i64,
        index_name: &str,
        expression: &tidb_ast::Expr,
        zone: &SessionTimeZone,
        like_default_escape: u8,
    ) -> Result<(), KvTableError> {
        let compiled = self.compile_check_constraint(
            &format!("index {index_name}"),
            expression,
            zone,
            like_default_escape,
        )?;
        self.partial_index_conditions.insert(index_id, compiled);
        Ok(())
    }

    /// Removes a predicate when an index backfill fails or the index is
    /// dropped. The sidecar must never outlive its `KvIndex` owner.
    pub(crate) fn remove_partial_index_condition(&mut self, index_id: i64) {
        self.partial_index_conditions.remove(&index_id);
        self.partial_index_condition_texts.remove(&index_id);
    }

    /// Whether a row belongs in an index's partial predicate. A NULL result
    /// follows Go's `EvalBool` rule for index conditions and is treated as
    /// false, so `WHERE b IS NOT NULL` excludes NULL rows.
    /// Go `IndexInfo.HasCondition`: whether `index_id` is a partial index.
    pub fn index_has_condition(&self, index_id: i64) -> bool {
        self.partial_index_conditions.contains_key(&index_id)
    }

    pub fn index_condition_holds(
        &self,
        index: &KvIndex,
        row: &[Datum],
        zone: &SessionTimeZone,
    ) -> Result<bool, KvTableError> {
        let Some(condition) = self.partial_index_conditions.get(&index.id) else {
            return Ok(true);
        };
        let context = IndexConditionContext {
            columns: &self.columns,
            row,
            zone,
        };
        let value = crate::generated_column::eval_over_dependencies(
            &condition.expr,
            &condition.dependencies,
            &*self.columns,
            row,
            &context,
        )
        .map_err(|error| KvTableError::CheckConstraint {
            name: condition.name.clone(),
            detail: format!("{error:?}"),
            eval: Some(error),
        })?;
        tidb_expr::truthy_of(&value)
            .map(|truthy| truthy.unwrap_or(false))
            .map_err(|error| KvTableError::CheckConstraint {
                name: condition.name.clone(),
                detail: format!("{error:?}"),
                eval: Some(error),
            })
    }

    /// Whether a partial index is safe to use as a foreign-key covering key.
    /// Go's FK validator accepts an `IS NOT NULL` predicate on every indexed
    /// column; a predicate on another column does not prove that the key row
    /// exists and therefore must not suppress the FK's auto-created support
    /// index. Non-partial indexes are safe by definition.
    pub fn partial_index_safe_for_columns(
        &self,
        index: &KvIndex,
        offsets: &[usize],
    ) -> bool {
        let Some(condition) = self.partial_index_conditions.get(&index.id) else {
            return true;
        };
        let mut not_null = HashSet::new();
        fn collect(expr: &tidb_ast::Expr, not_null: &mut HashSet<String>) -> bool {
            match expr {
                tidb_ast::Expr::Paren(inner) => collect(inner, not_null),
                tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, left, right) => {
                    collect(left, not_null) && collect(right, not_null)
                }
                tidb_ast::Expr::Is {
                    expr,
                    target: tidb_ast::IsTarget::Null,
                    not: true,
                } => match expr.as_ref() {
                    tidb_ast::Expr::Column(path) => path.last().is_some_and(|name| {
                        not_null.insert(name.go_to_lower());
                        true
                    }),
                    _ => false,
                },
                _ => false,
            }
        }
        if !collect(&condition.source, &mut not_null) {
            return false;
        }
        offsets.iter().all(|offset| {
            self.columns
                .get(*offset)
                .is_some_and(|column| not_null.contains(&column.name.go_to_lower()))
        })
    }

    /// The indexes a plan may read through, which is [`Self::indexes`] minus
    /// the invisible ones (Go's `IndexInfo.Invisible`). Every site that
    /// *chooses* an access path -- the cost-based index candidates and the
    /// unique-index point-get paths -- goes through here, so an index the
    /// planner must not pick is excluded once rather than at each chooser.
    /// The source column the multi-valued index `index_id`'s single ARRAY
    /// key part indexes, when one was recorded (`model.IndexInfo.MVIndex`
    /// plus that part's hidden column's `Dependences`). `None` for every
    /// ordinary index and every multi-valued index whose stored expression
    /// did not name exactly one column.
    #[must_use]
    pub fn mv_key_part_source(&self, index_id: i64) -> Option<&str> {
        self.mv_key_part_sources.get(&index_id).map(String::as_str)
    }

    /// Records the source column of a multi-valued index's ARRAY key part.
    /// Loaders and DDL call this once per multi-valued index they admit;
    /// recording nothing leaves the index unplannable, never misplannable.
    pub fn set_mv_key_part_source(&mut self, index_id: i64, source: String) {
        self.mv_key_part_sources.insert(index_id, source);
    }

    pub fn plan_indexes(&self) -> impl Iterator<Item = &KvIndex> {
        self.indexes.iter().filter(|index| index.visible)
    }

    /// The indexes Go's `tryPointGetPlan`/`tryBatchPointGetPlan` may pin: a
    /// planner-visible one that is not partial (`idxInfo.HasCondition()`),
    /// since a partial index holds only the rows its predicate admits and
    /// the fast plan has no residual to notice a row it never stored.
    pub fn point_get_indexes(&self) -> impl Iterator<Item = &KvIndex> {
        self.plan_indexes()
            .filter(|index| !self.index_has_condition(index.id))
    }

    /// Marks the column at `offset` as the table's handle column, which Go
    /// records as `TableInfo.PKIsHandle`.
    pub fn set_pk_handle_offset(&mut self, offset: usize) {
        self.pk_handle_offset = Some(offset);
        if !self.meta.read().pk_is_handle {
            self.update_meta(|meta| meta.pk_is_handle = true);
        }
    }

    /// The handle column's offset, if the table has one.
    #[must_use]
    pub fn pk_handle_offset(&self) -> Option<usize> {
        self.pk_handle_offset
    }

    /// Go `tblInfo.PKIsHandle && mysql.HasUnsignedFlag(tblInfo.GetPkColInfo()
    /// .GetFlag())`, which Go carries on a plan as `UnsignedHandle`.
    ///
    /// The handle BITS are the same either way; what differs is how they must
    /// be read back -- as a value to print, to sort by, or to range over. A
    /// handle above `i64::MAX` reads as a negative `i64`.
    #[must_use]
    pub fn unsigned_pk_handle(&self) -> bool {
        self.pk_handle_offset
            .and_then(|offset| self.columns.get(offset))
            .is_some_and(|column| column.field_type.is_unsigned())
    }

    /// The number of stored rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.store.key_count()
    }

    /// Whether the table has no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.store.key_count() == 0
    }

    /// Every raw key this table's backend currently holds, in key order:
    /// record keys and index keys together.
    ///
    /// This is the set a statement's *locks* would have to cover, which is
    /// what ports of Go's `TestDeleteLockKey` / `TestInsertLockUnchangedKeys`
    /// assert against. Go reads the same fact out of the membuffer
    /// (`pkg/kv/union_store.go`); this tier gives each table its own
    /// [`tidb_kv::storage::TableStorage`], so the whole key set is readable
    /// directly.
    pub fn stored_keys(&mut self) -> Result<Vec<Vec<u8>>, KvTableError> {
        // Every key of the table lies under one of its physical ids' `t{id}`
        // prefixes: the logical id (records, global indexes) and each
        // partition's. A store shared with other tables is read range by
        // range, as Go iterates a table prefix.
        let mut physical_ids = vec![self.table_id];
        physical_ids.extend(self.record_physical_ids());
        physical_ids.sort_unstable();
        physical_ids.dedup();
        let mut keys = Vec::new();
        for id in physical_ids {
            let start = Key::from_bytes(tidb_tablecodec::table_key::gen_table_prefix(id));
            let end = Key::from_bytes(tidb_tablecodec::table_key::gen_table_prefix(id.saturating_add(1)));
            let mut iterator = self
                .store
                .iter(Some(&start), Some(&end))
                .map_err(KvTableError::from)?;
            while iterator.valid() {
                keys.push(iterator.key().as_slice().to_vec());
                iterator.next().map_err(KvTableError::from)?;
            }
            iterator.close();
        }
        keys.sort_unstable();
        Ok(keys)
    }

    /// Inserts one row (a `Datum` per column, in schema order): encodes the
    /// record key from the next handle and the value through the v2 row format,
    /// exactly the bytes a TiKV-backed table would store.
    pub fn insert_row(
        &mut self,
        row: &[Datum],
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Result<TableHandle, KvTableError> {
        self.insert_row_with_row_id(row, None, 0, ctx)
    }

    /// [`Self::insert_row`] for a statement that WROTE `_tidb_rowid`.
    ///
    /// Go `adjustImplicitRowID`: a non-zero written value becomes the record
    /// handle, after `rebaseImplicitRowID` lifts the counter above it so a
    /// later automatic handle cannot collide with it. A NULL or a ZERO is not
    /// a handle -- it means "allocate one", which is the ordinary path -- and
    /// that is Go's own rule, not a convenience: `insert t (a, _tidb_rowid)
    /// values (1, 0)` gets the next id rather than handle zero.
    /// `shard` is Go's `GetRowIDShardGenerator().GetCurrentShard(n)`, which
    /// the caller reads because the generator is the SESSION's -- one shard
    /// serves a run of ids and both `SHARD_ROW_ID_BITS` and `AUTO_RANDOM`
    /// draw from it. Ignored when the table declares no shard bits.
    pub fn insert_row_with_row_id(
        &mut self,
        row: &[Datum],
        row_id: Option<i64>,
        shard: i64,
        ctx: &dyn tidb_exprctx::Columns,
    ) -> Result<TableHandle, KvTableError> {
        self.insert_row_in(row, row_id, shard, ctx, false, false, None, true)
    }

    /// [`Self::insert_row_with_row_id`] for the INSERT executor, which names
    /// the statement's duplicate-key mode. `lazy_dup_check` is Go
    /// `DupKeyCheckLazy` (`pkg/executor/insert.go`'s
    /// `optimizeDupKeyCheckForNormalInsert`, reached only by a NORMAL insert
    /// under a pessimistic transaction): existence is consulted in the
    /// statement's staged writes ONLY -- Go `GetLocal` -- and a miss stages
    /// the row presumed absent (`kv.SetPresumeKeyNotExists`), deferring the
    /// verdict to the commit's constraint check. The record key carries the
    /// mark; distinct secondary entries use the same policy. Each deferred
    /// key retains its own absence check and duplicate diagnostic.
    pub fn insert_row_with_row_id_checked(
        &mut self,
        row: &[Datum],
        row_id: Option<i64>,
        shard: i64,
        ctx: &dyn tidb_tblctx::MutateContext,
        lazy_dup_check: bool,
    ) -> Result<TableHandle, KvTableError> {
        // Go `AddRecord(..., table.WithCtx(ctx))`'s region; its index
        // writes take the caller's ctx (`GetCreateIdxOpt`), so they are its
        // siblings.
        let _add_record = tidb_tracing::start_thread_region("table.AddRecord");
        self.with_txn_staging(ctx, |table| {
            table.insert_row_in(
                row,
                row_id,
                shard,
                ctx,
                lazy_dup_check,
                false,
                Some(ctx),
                true,
            )
        })
    }

    /// Runs one row write with the statement's transaction staging attached
    /// to this table's store when the statement is part of a transaction
    /// (see [`tidb_kv::storage::TableStorage::attach_txn_staging`]).
    fn with_txn_staging<T>(
        &mut self,
        ctx: &dyn tidb_tblctx::MutateContext,
        write: impl FnOnce(&mut Self) -> Result<T, KvTableError>,
    ) -> Result<T, KvTableError> {
        let optimistic = ctx.optimistic_transaction();
        if !optimistic && !ctx.pessimistic_transaction() {
            return write(self);
        }
        self.store.attach_txn_staging(Some(TxnStaging {
            writes: ctx.staged_writes_handle(),
            table_id: self.table_id,
            optimistic,
        }));
        let result = write(self);
        self.store.attach_txn_staging(None);
        result
    }

    /// The bytes `key` holds in this table's store, if any: COMMIT's check
    /// of a key an INSERT presumed absent.
    #[must_use]
    pub fn stored_raw_value(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.store
            .get(&Key::from_bytes(key.to_vec()))
            .ok()
            .filter(|value| !value.is_empty())
    }

    /// [`Self::insert_row_with_row_id_checked`] for a normal clustered INSERT
    /// whose caller already proved every record key absent in one batch read.
    /// The batch proof helper refuses secondary-index tables; their ordinary
    /// insert path chooses record and index duplicate checking together.
    pub fn insert_row_with_row_id_checked_without_primary_duplicate_check(
        &mut self,
        row: &[Datum],
        row_id: Option<i64>,
        shard: i64,
        ctx: &dyn tidb_tblctx::MutateContext,
        lazy_dup_check: bool,
    ) -> Result<TableHandle, KvTableError> {
        let _add_record = tidb_tracing::start_thread_region("table.AddRecord");
        self.with_txn_staging(ctx, |table| {
            table.insert_row_in(
                row,
                row_id,
                shard,
                ctx,
                lazy_dup_check,
                true,
                Some(ctx),
                true,
            )
        })
    }

    /// Go `AddRecord` for a caller that already holds the row's record id
    /// (Go passes it as the row's trailing datum, `hasRecordID`): the handle
    /// is used as given, with no allocation and no rebase of the table's
    /// counter, which belongs to the statement that wrote `_tidb_rowid`
    /// (`adjustImplicitRowID`), not to the table. `None` derives a clustered
    /// table's handle from its key columns. `lazy_dup_check` is the
    /// statement's `DupKeyCheckLazy` option (Go
    /// `optimizeDupKeyCheckForNormalInsert`). The system-table writers
    /// (`tidb_syssession::system_row_write`) write through this.
    pub fn add_record_with_record_id(
        &mut self,
        row: &[Datum],
        record_id: Option<i64>,
        lazy_dup_check: bool,
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<TableHandle, KvTableError> {
        let _add_record = tidb_tracing::start_thread_region("table.AddRecord");
        self.with_txn_staging(ctx, |table| {
            table.insert_row_in(
                row,
                record_id,
                0,
                ctx,
                lazy_dup_check,
                false,
                Some(ctx),
                false,
            )
        })
    }

    /// The record handle a clustered table derives from `row` (Go
    /// `kv.BuildHandleFromDatumRow` over the primary key, or the integer
    /// primary key itself); `None` for a table whose rows live under an
    /// allocated `_tidb_rowid`.
    pub fn clustered_handle_of_row(
        &mut self,
        row: &[Datum],
        zone: &SessionTimeZone,
    ) -> Result<Option<TableHandle>, KvTableError> {
        if self.pk_handle_offset.is_none() && self.common_handle_offsets.is_empty() {
            return Ok(None);
        }
        self.handle_of_row(row, zone, 0).map(Some)
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_row_in(
        &mut self,
        row: &[Datum],
        row_id: Option<i64>,
        shard: i64,
        ctx: &dyn tidb_exprctx::Columns,
        lazy_dup_check: bool,
        skip_primary_duplicate_check: bool,
        stats_ctx: Option<&dyn tidb_tblctx::MutateContext>,
        rebase_written_id: bool,
    ) -> Result<TableHandle, KvTableError> {
        let zone = ctx.time_zone();
        // Go `addRecord`: a temporary table refuses a row once what it holds
        // (committed plus this transaction's) exceeds
        // `@@tidb_tmp_table_max_size`, before the row is written (1114).
        if let Some(statement) = stats_ctx.filter(|_| self.is_temporary()) {
            if i64::try_from(self.store.size_bytes()).unwrap_or(i64::MAX)
                > statement.tmp_table_max_size()
            {
                return Err(KvTableError::Mysql(
                    tidb_terror::MysqlError::from_terror(
                        &crate::table_errors::ERR_TEMP_TABLE_FULL
                            .generate_with_stack_by_args(&[self.name().into()]),
                    ),
                ));
            }
        }
        // Go AddRecord receives a row completed by the mutation executor.
        // Re-evaluation here would duplicate warnings and could replace its
        // converted/NULL-substituted generated values before index encoding.
        self.validate_check_constraints(row, ctx)?;
        self.check_data_for_modify_column(row, stats_ctx)?;
        // Go `AddRecord` writes `WritableCols`: past the public columns, the
        // ones a DDL is adding or changing.
        let write_row =
            self.insert_write_row(row, stats_ctx, tidb_exprctx::Columns::type_flags(ctx), &zone)?;
        let row = write_row.as_deref().unwrap_or(row);
        let value = self.encode_row_value(row, &zone)?;
        // Go `addRecord`: every record key is unique. A clustered key derives
        // it from visible columns; a heap table derives it from `_tidb_rowid`,
        // which `FORCE AUTO_INCREMENT` can intentionally rewind -- or which
        // the statement wrote itself.
        // Go `adjustImplicitRowID`: a written `_tidb_rowid` becomes the
        // record handle (rebasing the shared id counter), anything else asks
        // for an allocated one. See [`Self::insert_row_with_row_id`].
        let explicit_handle = match row_id {
            Some(0) => Some(TableHandle::Int(0)),
            Some(value) => {
                // Go's `adjustImplicitRowID` rebases with allocIDs=true, so an
                // ascending run of explicit row ids crosses to the counter's
                // home once per reserved window.
                if rebase_written_id {
                    self.row_id_allocator()
                        .rebase_allocating(value as u64)
                        .map_err(|error| {
                            KvTableError::from(tidb_kv::storage::StorageError::Backend(error.0))
                        })?;
                }
                Some(TableHandle::Int(value))
            }
            _ => None,
        };
        let handle = match explicit_handle {
            Some(handle) => handle,
            None => self.handle_of_row(row, &zone, shard)?,
        };
        let clustered = self.pk_handle_offset.is_some() || !self.common_handle_offsets.is_empty();
        let physical_id = self.record_physical_id(row, ctx)?;
        let key = Key::from_bytes(encode_row_key_with_handle(
            physical_id,
            &handle.record_handle(),
        ));
        let duplicate_value = if clustered {
            clustered_key_text(self, row)
        } else {
            match &handle {
                TableHandle::Int(value) => value.to_string(),
                TableHandle::Common(_) => unreachable!("a heap table has an integer handle"),
            }
        };
        let duplicate_key = self.qualified_key("PRIMARY");
        // Go `AddRecord`'s duplicate check, in both of its modes. In place it
        // reads the whole transaction (staged writes, then snapshot); lazily
        // it reads the STAGED WRITES ONLY (`GetLocal`) and a miss marks the
        // key presumed not exists so prewrite still rejects a real duplicate.
        // A locally staged tombstone is neither: the row was deleted inside
        // this transaction, and the reinsert overwrites it without any
        // presumption -- Go's own `len(v) == 0` arm.
        let set_presume = if lazy_dup_check || !skip_primary_duplicate_check {
            // Go addRecord checks t.RecordKey(recordID), not every partition
            // containing that handle. Hidden row IDs can repeat across physical
            // partitions, whose record keys are distinct.
            self.check_insert_key(&key, &duplicate_value, &duplicate_key, lazy_dup_check)?
        } else {
            false
        };
        let pessimistic = stats_ctx.is_some_and(|ctx| ctx.pessimistic_transaction());
        let check_in_prewrite =
            stats_ctx.is_some_and(|ctx| ctx.pessimistic_check_in_prewrite());
        self.write_index_entries(
            row,
            &handle,
            physical_id,
            &zone,
            lazy_dup_check,
            pessimistic,
            check_in_prewrite,
        )?;
        if let Some(stats_ctx) = stats_ctx {
            stats_ctx
                .staged_writes()
                .note(self.table_id, key.as_bytes());
        }
        self.store
            .set_with_constraint_check(
                key,
                value,
                if set_presume && !pessimistic {
                    tidb_txnkv::AssertionOp::AssertUnknown
                } else {
                    tidb_txnkv::AssertionOp::AssertNotExist
                },
                set_presume && pessimistic && check_in_prewrite,
            )
            .map_err(KvTableError::from)?;
        self.double_write_insert(
            physical_id,
            row,
            &handle,
            &zone,
            lazy_dup_check,
            pessimistic,
            check_in_prewrite,
            ctx,
        )?;
        if let Some(stats_ctx) = stats_ctx {
            stats_ctx.update_table_delta(physical_id, 1, 1);
        }
        Ok(handle)
    }

    /// The table's indexes, for `ADMIN CHECK` (see the executor's `admin_check`).
    ///
    /// The field itself stays private because every other reader reaches an
    /// index through a scan; the consistency check is the one caller that
    /// needs the whole list as metadata.
    #[must_use]
    pub fn index_list_for_check(&self) -> Vec<KvIndex> {
        self.indexes.as_ref().clone()
    }

    /// [`KvTable::index_entry_keys`] for the executor's `admin_check`: the entry
    /// keys one row's values encode to under `index`, each with its
    /// `distinct` flag.
    pub fn index_keys_for_check(
        &mut self,
        index: &KvIndex,
        row: &[Datum],
        handle: &TableHandle,
        physical_id: i64,
        zone: &SessionTimeZone,
    ) -> Result<Vec<(Vec<u8>, bool)>, KvTableError> {
        Ok(self
            .index_entry_keys(index, row, handle, physical_id, zone)?
            .into_iter()
            .map(|entry| (entry.key, entry.distinct))
            .collect())
    }

    /// Removes one stored key without touching anything else -- the only way
    /// to build the CORRUPT table `ADMIN CHECK` exists to find.
    ///
    /// Every ordinary write path keeps rows and index entries in step, so a
    /// test that only used SQL could never distinguish a real consistency
    /// check from one that returns OK. This is that distinguishing move, and
    /// it is deliberately the crudest possible one: a raw `Delete` against
    /// the storage seam, exactly what a lost region replica or a half-applied
    /// index backfill leaves behind.
    pub fn delete_raw_key_for_test(&mut self, key: &[u8]) -> Result<(), KvTableError> {
        self.store
            .delete(Key::from_bytes(key.to_vec()))
            .map_err(KvTableError::from)
    }

    /// Removes one row's record entry, leaving its index entries orphaned.
    /// See [`KvTable::delete_raw_key_for_test`].
    pub fn delete_record_for_test(&mut self, handle: &TableHandle) -> Result<(), KvTableError> {
        let Some(key) = self.stored_record_key(handle)? else {
            return Ok(());
        };
        self.delete_raw_key_for_test(key.as_bytes())
    }

    /// The row stored under `handle`, decoded, or `None` when absent -- the
    /// single read a point-get plan performs.
    pub fn get_row_by_handle_with_context(
        &mut self,
        handle: &TableHandle,
        context: &RowDecodeContext,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        self.read_row(handle, context)
    }

    /// One stored-record read decoded by the immutable projection retained on
    /// a prepared PointGet plan.
    pub fn get_prepared_point_row(
        &mut self,
        handle: &TableHandle,
        decoder: &PreparedPointGetRowDecoder,
        context: &PreparedPointGetDecodeContext,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        let Some((_, entry)) = self.stored_record(handle)? else {
            return Ok(None);
        };
        decoder.decode(handle, &entry, context).map(Some)
    }

    /// The row stored under `handle`, decoded, or `None` when absent.
    pub fn read_row(
        &mut self,
        handle: &TableHandle,
        context: &RowDecodeContext,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        let Some((_, entry)) = self.stored_record(handle)? else {
            return Ok(None);
        };
        Ok(Some(self.decode_row_entry(handle, &entry, context)?))
    }

    pub fn decode_row_entry(
        &self,
        handle: &TableHandle,
        entry: &[u8],
        context: &RowDecodeContext,
    ) -> Result<Vec<Datum>, KvTableError> {
        PointRowDecoder::new(self).decode(self, handle, entry, context)
    }

    /// Replaces the row stored under `handle` (Go's `UPDATE` writes the new
    /// row back under the same record key when the handle column did not
    /// change).
    pub fn update_row_with_context(
        &mut self,
        handle: &TableHandle,
        row: &[Datum],
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<(), KvTableError> {
        self.update_row_with_old_context(handle, None, row, ctx)
    }

    /// Replaces a row using the selected old row when available, avoiding a
    /// second point read. The statement owns decoding, transaction policy,
    /// staged writes and table statistics, as Go's table mutation context does.
    pub fn update_row_with_old_context(
        &mut self,
        handle: &TableHandle,
        old_row: Option<&[Datum]>,
        row: &[Datum],
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<(), KvTableError> {
        self.with_txn_staging(ctx, |table| table.update_row_in(handle, old_row, row, ctx))
    }

    fn update_row_in(
        &mut self,
        handle: &TableHandle,
        old_row: Option<&[Datum]>,
        row: &[Datum],
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<(), KvTableError> {
        let zone = ctx.session_zone();
        // The shared update owner has already completed generated values.
        self.validate_check_constraints(row, ctx)?;
        self.check_data_for_modify_column(row, Some(ctx))?;
        // Go reads `WritableCols` for an UPDATE: the old and new rows carry
        // the columns a DDL holds in an intermediate state.
        let write_rows = if self.nonpublic_columns.is_empty() {
            None
        } else {
            let old = match old_row {
                Some(old) => Some(old.to_vec()),
                None => self.read_row(handle, &RowDecodeContext::for_write(ctx))?,
            };
            match old {
                Some(old) => self.update_write_rows(handle, &old, row, ctx)?,
                None => None,
            }
        };
        let (old_row, row) = match &write_rows {
            Some((old, new)) => (Some(old.as_slice()), new.as_slice()),
            None => (old_row, row),
        };
        // Go `updateRecord` (`pkg/executor/write.go`): assigning a CHANGED value
        // to the AUTO_INCREMENT column REBASES the allocator, exactly as an
        // explicit value on INSERT does, so later rows land past the value the
        // UPDATE named. Without this an `UPDATE t SET id = 300` left the
        // counter where it was and the next allocations walked back over ids
        // the table had moved AHEAD of. An unchanged value touches no
        // allocator, as in Go: a writer that rewrites a row's other columns
        // (a system-table update) holds no counter at all. With no old row to
        // compare, the value is treated as changed; a rebase only moves the
        // counter up.
        //
        // The value travels as its 64-bit PATTERN, since Go's
        // `getAutoRecordID` hands `Rebase` an `int64` that `rebase4Unsigned`
        // reads back as `uint64` for an unsigned column.
        let changed = |offset: usize| {
            let record_id = |values: &[Datum]| {
                values
                    .get(offset)
                    .map(|value| auto_increment::get_auto_record_id(value, false))
            };
            old_row.is_none_or(|old| record_id(old) != record_id(row))
        };
        if let Some(offset) = self.auto_increment_offset.filter(|offset| changed(*offset)) {
            let assigned = row
                .get(offset)
                .map_or(0, |value| auto_increment::get_auto_record_id(value, false));
            // Same arm Go's insert arms use: the rebase reserves a window on
            // its one store crossing, matching `Rebase(..., true)`.
            self.auto_id
                .rebase_allocating_in(&AutoIdCall::statement(&ctx.statement_memory()), assigned)
                .map_err(|error| {
                    if let Err(stop) = ctx.statement_memory().check() {
                        return KvTableError::from(tidb_kv::storage::StorageError::Sql(
                            stop.to_mysql_error(),
                        ));
                    }
                    KvTableError::from(tidb_kv::storage::StorageError::Backend(error.0))
                })?;
        }
        // Go rebases the AUTO_RANDOM allocator under the same "changed" rule.
        if self.auto_random.is_none_or(|spec| changed(spec.offset)) {
            self.rebase_auto_random_from_row(row)?;
        }
        // A clustered primary key IS the row handle, and the record value omits
        // the handle columns entirely, so an UPDATE that assigns to the primary
        // key MOVES the row: it is stored under the handle its new values
        // produce, not the one it was scanned under. Go `tables.UpdateRecord`
        // takes exactly this path -- when the handle changes it removes the old
        // record and calls `AddRecord` for the new one.
        //
        // Deriving the destination handle for every update makes the move the
        // general case; an update that leaves the key alone is the same code
        // with `new_handle == handle`, so there is no "did the handle change"
        // branch to get wrong. A table with no clustered key keeps the handle
        // it was scanned under, because its handle is an allocated `_tidb_rowid`
        // that the row's values do not determine.
        let clustered = self.pk_handle_offset.is_some() || !self.common_handle_offsets.is_empty();
        let mut new_handle = if clustered {
            self.handle_of_row(row, &zone, 0)?
        } else {
            handle.clone()
        };
        let new_physical_id = self.record_physical_id(row, ctx)?;
        // The retained input row identifies its physical partition even when
        // another partition contains the same hidden row handle.
        let old_key = if let Some(old) = old_row {
            Some(Key::from_bytes(encode_row_key_with_handle(
                self.record_physical_id(old, ctx)?,
                &handle.record_handle(),
            )))
        } else {
            self.stored_record_key(handle)?
        };
        if !clustered
            && old_key
                .as_ref()
                .is_some_and(|key| tidb_tablecodec::table_key::decode_table_id(key.as_bytes()) != new_physical_id)
        {
            // Go partitionedTableUpdateRecord removes then AddRecords a heap
            // row crossing partitions, allocating a fresh hidden row ID.
            let shard = if self.shard_row_id_bits() > 0 {
                ctx.next_row_id_shard(1) as i64
            } else {
                0
            };
            new_handle = self.handle_of_row(row, &zone, shard)?;
        }
        // Go `partitionedTableUpdateRecord`'s reorganization half is decided
        // before the row's own write: a heap row whose reorganized copy
        // diverged moves to a fresh handle in both partitions.
        let old_physical_for_reorg = old_key.as_ref().map_or(self.table_id, |key| {
            tidb_tablecodec::table_key::decode_table_id(key.as_bytes())
        });
        let reorg_plan = if self.partition_reorg.is_some() {
            let owned_old;
            let old_for_reorg = match old_row {
                Some(old) => Some(old),
                None => {
                    owned_old = self.read_row(handle, &RowDecodeContext::for_write(ctx))?;
                    owned_old.as_deref()
                }
            };
            match old_for_reorg {
                Some(old) => {
                    let old: Vec<Datum> = old.to_vec();
                    self.plan_reorg_update(
                        old_physical_for_reorg,
                        new_physical_id,
                        handle,
                        &new_handle,
                        &old,
                        row,
                        ctx,
                    )?
                    .map(|plan| (plan, old.clone()))
                }
                None => None,
            }
        } else {
            None
        };
        if let Some(moved) =
            self.reorg_moved_handle(reorg_plan.as_ref().map(|(plan, _)| plan), handle)
        {
            new_handle = moved.clone();
        }
        let destination_key = Key::from_bytes(encode_row_key_with_handle(
            new_physical_id,
            &new_handle.record_handle(),
        ));
        let pessimistic = ctx.pessimistic_transaction();
        let check_in_prewrite = ctx.pessimistic_check_in_prewrite();
        // Go optimizeDupKeyCheckForUpdate keeps optimistic UPDATE and IGNORE
        // eager; pessimistic writes use the selected lock/prewrite boundary.
        let lazy_dup_check = pessimistic && !ctx.ignore_err();
        let set_presume = if old_key.as_ref() != Some(&destination_key) {
            let duplicate_value = if clustered {
                clustered_key_text(self, row)
            } else {
                match &new_handle {
                    TableHandle::Int(value) => value.to_string(),
                    TableHandle::Common(_) => unreachable!("heap table has an integer handle"),
                }
            };
            self.check_insert_key(
                &destination_key,
                &duplicate_value,
                &self.qualified_key("PRIMARY"),
                lazy_dup_check,
            )?
        } else {
            false
        };
        let old_physical_id = old_key.as_ref().map_or(self.table_id, |key| {
            tidb_tablecodec::table_key::decode_table_id(key.as_bytes())
        });
        // Go removes the old index entries and writes the new ones. They point
        // AT the handle, so a moved row needs them rewritten even when the
        // indexed values did not change.
        if !self.indexes.is_empty() || !self.nonpublic_indexes.is_empty() {
            let owned_old;
            let old = match old_row {
                Some(old) => Some(old),
                None => {
                    owned_old = self.read_row(handle, &RowDecodeContext::for_write(ctx))?;
                    owned_old.as_deref()
                }
            };
            // Go leaves a failed index rewrite to the statement's membuffer
            // stage (`StmtRollback`); this stage gives a store that is not
            // under one (a bare in-process store) the same rollback.
            let stage = self.store.staging();
            let rewritten = (|| -> Result<(), KvTableError> {
                match &old {
                    Some(old) if new_handle == *handle && old_physical_id == new_physical_id => {
                        // The row keeps its key, so every entry whose indexed
                        // columns and payload did not change would be rewritten to
                        // its own current bytes. Go skips those rewrites
                        // (`SkipWriteUntouchedIndices`, see
                        // `rewrite_changed_index_entries`); this port does too.
                        if let Err(error) = self.rewrite_changed_index_entries(
                            old,
                            row,
                            handle,
                            new_physical_id,
                            &zone,
                            lazy_dup_check,
                            pessimistic,
                            check_in_prewrite,
                            ctx.writes_untouched_indices(),
                        ) {
                            return Err(error);
                        }
                    }
                    Some(old) => {
                        self.delete_index_entries(old, handle, old_physical_id, &zone)?;
                        if let Err(error) = self.write_index_entries(
                            row,
                            &new_handle,
                            new_physical_id,
                            &zone,
                            lazy_dup_check,
                            pessimistic,
                            check_in_prewrite,
                        ) {
                            return Err(error);
                        }
                    }
                    None => {
                        self.write_index_entries(
                            row,
                            &new_handle,
                            new_physical_id,
                            &zone,
                            lazy_dup_check,
                            pessimistic,
                            check_in_prewrite,
                        )?;
                    }
                }
                Ok(())
            })();
            match rewritten {
                Ok(()) => self.store.release(stage),
                Err(error) => {
                    self.store.cleanup(stage);
                    return Err(error);
                }
            }
        }
        // The handle columns stay out of the value, as on insert.
        let value = self.encode_row_value(row, &zone)?;
        // An update can move the row TWICE over: onto a new handle, and --
        // when the update changes a partitioning column -- into a different
        // partition. Both are the same move, because both change the record
        // KEY, so the old key is removed whenever the new one differs from it
        // and there is no "did the partition change" branch.
        let key = Key::from_bytes(encode_row_key_with_handle(
            new_physical_id,
            &new_handle.record_handle(),
        ));
        let record_assertion = if old_key.as_ref().is_some_and(|old_key| *old_key != key) {
            tidb_txnkv::AssertionOp::AssertNotExist
        } else {
            tidb_txnkv::AssertionOp::AssertExist
        };
        if let Some(old_key) = old_key.filter(|old_key| *old_key != key) {
            ctx.staged_writes().note(self.table_id, old_key.as_bytes());
            self.store
                .delete_with_assertion(old_key, tidb_txnkv::AssertionOp::AssertExist)
                .map_err(KvTableError::from)?;
        }
        ctx.staged_writes().note(self.table_id, key.as_bytes());
        self.store
            .set_with_constraint_check(
                key,
                value,
                record_assertion,
                set_presume && pessimistic && check_in_prewrite,
            )
            .map_err(KvTableError::from)?;
        if let Some((plan, old)) = reorg_plan {
            self.apply_reorg_update(
                plan,
                handle,
                &old,
                row,
                lazy_dup_check,
                pessimistic,
                check_in_prewrite,
                ctx,
            )?;
        }
        if old_physical_id == new_physical_id {
            ctx.update_table_delta(old_physical_id, 0, 1);
        } else {
            ctx.update_table_delta(old_physical_id, -1, 1);
            ctx.update_table_delta(new_physical_id, 1, 1);
        }
        Ok(())
    }

    /// Removes the row stored under `handle` using the statement's write policy.
    pub fn delete_row_with_context(
        &mut self,
        handle: &TableHandle,
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<(), KvTableError> {
        self.with_txn_staging(ctx, |table| table.delete_row_in(handle, ctx))
    }

    fn delete_row_in(
        &mut self,
        handle: &TableHandle,
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<(), KvTableError> {
        let Some(key) = self.stored_record_key(handle)? else {
            return Ok(());
        };
        let physical_id = tidb_tablecodec::table_key::decode_table_id(key.as_bytes());
        let mut removed_row = None;
        if !self.indexes.is_empty()
            || !self.nonpublic_indexes.is_empty()
            || self.partition_reorg.is_some()
        {
            if let Some(row) = self.read_row(handle, &RowDecodeContext::for_write(ctx))? {
                // Go removes the `DeletableCols` row.
                let row = self.delete_write_row(handle, &row, ctx)?.unwrap_or(row);
                self.delete_index_entries(&row, handle, physical_id, &ctx.session_zone())?;
                removed_row = Some(row);
            }
        }
        ctx.staged_writes().note(self.table_id, key.as_bytes());
        self.store
            .delete_with_assertion(key, tidb_txnkv::AssertionOp::AssertExist)
            .map_err(KvTableError::from)?;
        if let Some(row) = removed_row {
            self.double_write_remove(physical_id, &row, handle, ctx)?;
        }
        ctx.update_table_delta(physical_id, -1, 1);
        Ok(())
    }

    /// Removes a row already fetched by this statement. Go `RemoveRecord`
    /// derives its record key and index entries from the retained input row;
    /// no second storage read is needed.
    pub fn delete_row_with_old_context(
        &mut self,
        handle: &TableHandle,
        old_row: &[Datum],
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<(), KvTableError> {
        self.with_txn_staging(ctx, |table| table.delete_old_row_in(handle, old_row, ctx))
    }

    fn delete_old_row_in(
        &mut self,
        handle: &TableHandle,
        old_row: &[Datum],
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<(), KvTableError> {
        let zone = ctx.session_zone();
        let physical_id = self.record_physical_id(old_row, ctx)?;
        let key = Key::from_bytes(encode_row_key_with_handle(
            physical_id,
            &handle.record_handle(),
        ));
        // Go removes the `DeletableCols` row.
        let write_row = self.delete_write_row(handle, old_row, ctx)?;
        let old_row = write_row.as_deref().unwrap_or(old_row);
        if !self.indexes.is_empty() || !self.nonpublic_indexes.is_empty() {
            self.delete_index_entries(old_row, handle, physical_id, &zone)?;
        }
        ctx.staged_writes().note(self.table_id, key.as_bytes());
        self.store
            .delete_with_assertion(key, tidb_txnkv::AssertionOp::AssertExist)
            .map_err(KvTableError::from)?;
        self.double_write_remove(physical_id, old_row, handle, ctx)?;
        ctx.update_table_delta(physical_id, -1, 1);
        Ok(())
    }
}

/// Which of Go's two renderings a duplicate entry takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DuplicateText {
    /// `batch_checker.go`'s `getKeysNeedCheckOneRow` (INSERT IGNORE, ON
    /// DUPLICATE KEY UPDATE, REPLACE): `dataToStrings` over a unique index's
    /// cut values, but over a common handle's uncut row values (only
    /// `buildHandleFromDatumRow`'s copies are cut).
    BatchChecker,
    /// The table layer's `addIndices` and `getDuplicateError` (UPDATE):
    /// `genIndexKeyStrs` over the key's values, cut by `TruncateIndexValues`.
    TableLayer,
}

/// Go `batch_checker.go`'s `dataToStrings`, joined as `GenKeyExistsErr`
/// joins it: a binary string, a BIT or a binary literal drops its trailing
/// `0x00` bytes (keeping one, binary strings only) and shows each
/// non-printable byte as `\xNN` (`util.FmtNonASCIIPrintableCharToHex`).
fn batch_check_key_text<'a>(values: impl Iterator<Item = &'a Datum>) -> String {
    values
        .map(|value| {
            let bytes: Option<(&[u8], bool)> = match value {
                Datum::Bytes(bytes) => Some((bytes, true)),
                Datum::String(text) if text.collation() == tidb_charset::Collation::Binary => {
                    Some((text.bytes(), true))
                }
                Datum::Bit(literal) | Datum::BinaryLiteral(literal) => {
                    Some((literal.as_bytes(), false))
                }
                _ => None,
            };
            let Some((mut bytes, trim_zeros)) = bytes else {
                return datum_text(value);
            };
            if trim_zeros {
                let kept = bytes.iter().rposition(|byte| *byte != 0).map_or(0, |last| last + 1);
                bytes = if kept == 0 { &[0] } else { &bytes[..kept] };
            }
            fmt_non_ascii_printable_char_to_hex(bytes)
        })
        .collect::<Vec<_>>()
        .join("-")
}

/// Go `util.FmtNonASCIIPrintableCharToHex(str, len(str), true)`.
fn fmt_non_ascii_printable_char_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        if (0x20..=0x7e).contains(&byte) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("\\x{byte:02X}"));
        }
    }
    out
}

/// The text a clustered-key duplicate reports: the key columns joined by `-`,
/// as Go's `ErrKeyExists` formats them. Every part goes through the one
/// [`datum_text`] renderer, so a composite key can never disagree with a
/// single-column one about how a value prints.
/// A partition route's failure as the write's error.
fn routing_error(error: crate::partition_routing::RoutingError) -> KvTableError {
    match error {
        crate::partition_routing::RoutingError::NoPartitionForValue(value) => {
            KvTableError::Mysql(tidb_terror::MysqlError::from_terror(
                &crate::table_errors::ERR_NO_PARTITION_FOR_GIVEN_VALUE
                    .generate_with_stack_by_args(&[value.into()]),
            ))
        }
        crate::partition_routing::RoutingError::ValueOverflowsBigint(value) => {
            KvTableError::Mysql(tidb_terror::MysqlError::from_terror(
                &tidb_datatype::types_errors::ERR_OVERFLOW.generate_with_stack(format!(
                    "constant {} overflows {}",
                    (value),
                    "bigint"
                )),
            ))
        }
        crate::partition_routing::RoutingError::Eval(error) => KvTableError::PartitionEval(error),
        crate::partition_routing::RoutingError::Conversion(error) => KvTableError::Decode(error),
    }
}

fn clustered_key_text(table: &KvTable, row: &[Datum]) -> String {
    // Go reports the values the handle holds, so a prefix key column shows
    // only its leading part (`TruncateIndexValues`).
    table
        .handle_column_offsets()
        .iter()
        .enumerate()
        .map(|(position, offset)| {
            let Some(mut value) = row.get(*offset).cloned() else {
                return String::new();
            };
            if table.pk_handle_offset.is_none() {
                if let (Some(length), Some(column)) = (
                    table.common_handle_prefix_lengths.get(position),
                    table.columns.get(*offset),
                ) {
                    tidb_tablecodec::cut_datum_by_prefix_len(&mut value, *length, &column.field_type);
                }
            }
            datum_text(&value)
        })
        .collect::<Vec<_>>()
        .join("-")
}

/// How Go reports a value the modified column cannot hold.
///
/// A string that is not a number going into a numeric column is
/// `ErrTruncatedWrongValue` naming the numeric domain (Go converts through
/// `StrToFloat`, hence "DOUBLE"); anything else -- a string too long for the
/// new width, an out-of-range number -- is `ErrTruncatedWrongValueForField`
/// naming the column.
pub fn convert_failure(column: &str, target: &FieldType, value: &Datum) -> KvTableError {
    let text = datum_text(value);
    let source_is_string = matches!(value, Datum::Bytes(_) | Datum::String(_));
    let numeric_target = matches!(
        target.eval_type(),
        tidb_datatype::EvalType::Int
            | tidb_datatype::EvalType::Real
            | tidb_datatype::EvalType::Decimal
    );
    if source_is_string && numeric_target {
        return KvTableError::Mysql(tidb_terror::MysqlError::from_terror(
            &tidb_datatype::types_errors::ERR_TRUNCATED_WRONG_VAL
                .generate_with_stack_by_args(&["DOUBLE".into(), text.into()]),
        ));
    }
    KvTableError::Mysql(tidb_terror::MysqlError::from_terror(
        &tidb_datatype::types_errors::ERR_TRUNCATED.generate_with_stack(format!(
            "Data truncated for column '{}', value is '{}'",
            (column.to_owned()),
            (text)
        )),
    ))
}

/// A datum as MySQL prints it inside an error message.
pub fn datum_text(value: &Datum) -> String {
    match value {
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        // Go formats a FLOAT with 32-bit precision, so the shortest text that
        // round-trips as an f32 -- not the f64 widening of it.
        Datum::Float32(value) => value.to_string(),
        Datum::Real(value) => value.to_string(),
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        Datum::Decimal(value) => value.to_string(),
        Datum::Time(value) => value.to_string(),
        Datum::Duration(value) => value.to_string(),
        Datum::Enum(value, _) => value.to_string(),
        Datum::Set(value, _) => value.to_string(),
        // Go's `Datum.ToString` reaches `BinaryLiteral.ToString()`, which is
        // `string(b)` -- the RAW BYTES. It is deliberately not the `String()`
        // form (`0x...`) that this type's `Display` renders.
        Datum::Bit(value) | Datum::BinaryLiteral(value) => {
            String::from_utf8_lossy(value.as_bytes()).into_owned()
        }
        Datum::Json(value) => value.to_string(),
        Datum::VectorFloat32(value) => value.to_string(),
        // Go's `KindNull` arm is the empty string; the remaining kinds are
        // its `default` error arm, which no stored column value reaches.
        Datum::Null => String::new(),
        other => format!("{other:?}"),
    }
}

/// The value a NULL takes when a column is rewritten to `TIMESTAMP NOT NULL`.
///
/// Go `updateColumnWorker.getRowRecord` substitutes the CURRENT TIMESTAMP
/// before it casts, guarded on exactly three things: the old value is NULL,
/// the new type is `mysql.TypeTimestamp`, and the new column carries
/// `NotNullFlag`. A `DATETIME NOT NULL` is deliberately not included, and
/// still refuses the NULL.
///
/// The clock is the STATEMENT's, so every row of one reorg takes the same
/// instant even across a second boundary -- Go reads it through
/// `GetTimeCurrentTimestamp(ctx.GetEvalCtx(), ...)` for the same reason.
pub fn null_timestamp_substitute(
    field_type: &tidb_datatype::FieldType,
    ctx: &dyn tidb_tblctx::MutateContext,
) -> Option<Datum> {
    if field_type.code() != tidb_datatype::FieldTypeCode::Timestamp
        || field_type.flags() & NOT_NULL_FLAG == 0
    {
        return None;
    }
    let now = crate::column_default::on_update_current_timestamp("", field_type).ok()?;
    crate::column_default::evaluate(
        &now,
        field_type,
        tidb_model::column::COLUMN_INFO_VERSION2,
        ctx.write_conversion_flags(),
        ctx,
        tidb_chunk::row::Row::empty(),
    )
    .ok()
}

/// The PRIMARY index whose columns are the common row handle.
///
/// TiDB exposes this metadata as an index for range building and statistics,
/// but physically reads its ranges from the table's `_r<common_handle>`
/// record keys. It is therefore a table path, not a secondary-index path.
pub fn common_handle_primary(table: &KvTable) -> Option<&KvIndex> {
    table
        .plan_indexes()
        .find(|index| index.primary && index.column_offsets == table.common_handle_offsets())
}

/// The clustered PRIMARY's index METADATA, whether or not the catalog stores
/// it as a [`KvIndex`].
///
/// Go's `TableInfo.Indices` always carries the clustered primary `IndexInfo`
/// (`Primary: true`), and `deriveCommonHandleTablePathStats` builds the table
/// path's ranges from it. This tier's `CREATE TABLE` stores no `KvIndex` for
/// a clustered key -- the record key itself enforces it, and a stored index
/// would be physically maintained as a duplicate -- so the same metadata is
/// synthesized here from the handle offsets. Prefix lengths are carried
/// alongside those offsets in the table metadata. Id `0` collides with
/// no stored index (the DDL allocates from 1), so a statistics lookup under
/// it misses and the estimate is the pseudo one -- exactly what Go computes
/// for an unanalyzed primary.
///
/// The tables that DO store a PRIMARY `KvIndex` (test scaffolding injects one
/// for statistics) keep using it, its id and its declared parts.
pub fn clustered_primary_metadata(table: &KvTable) -> Option<std::borrow::Cow<'_, KvIndex>> {
    if table.common_handle_offsets().is_empty() {
        return None;
    }
    if let Some(index) = common_handle_primary(table) {
        return Some(std::borrow::Cow::Borrowed(index));
    }
    Some(std::borrow::Cow::Owned(KvIndex {
        id: 0,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        column_offsets: table.common_handle_offsets().to_vec(),
        prefix_lengths: table.common_handle_prefix_lengths().to_vec(),
        visible: true,
        global: false,
        global_index_version: 0,
        clustered_primary: false,
        primary: true,
    }))
}

/// `hypo`'s indexes for `table` (`HypoIndexes::table_indexes`) resolved
/// against its columns, for the readers that work over the loaded table
/// (EXPLAIN's access objects, `SHOW CREATE TABLE`). An index naming a column
/// the table no longer has is left out.
#[must_use]
pub fn hypo_kv_indexes(
    hypo: &tidb_variable::hypo_index::HypoIndexes,
    database: &str,
    table: &KvTable,
) -> Vec<KvIndex> {
    hypo.table_indexes(database, &table.name())
        .into_iter()
        .filter_map(|index| crate::table_from_meta::kv_index(index, &table.columns).ok())
        .collect()
}

/// Go `model.ColumnInfo` for a column of a table built from columns alone.
fn fixture_column_info(column: &KvColumn, offset: usize, hidden: bool) -> tidb_model::ColumnInfo {
    tidb_model::ColumnInfo {
        id: column.id,
        name: tidb_ast::CiString::new(column.name.clone()),
        offset: offset as i64,
        field_type: column.field_type.clone(),
        state: tidb_model::SchemaState::PUBLIC,
        hidden,
        ..tidb_model::ColumnInfo::default()
    }
}
