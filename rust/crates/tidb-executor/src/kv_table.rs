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
//! `tidb_codec::table_key::encode_row_key_with_handle` (`t{tid}_r{handle}`)
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

mod auto_id;
mod auto_increment;
mod auto_random;
mod cache;
mod column_deps;
mod index_entries;
mod partition_maintenance;
mod row_decoder;
mod table_meta;
mod table_scan;

pub use column_deps::ColumnDependent;

pub use auto_id::{
    advance, calc_needed_batch_size, exceeds, AutoIdError, AutoIdStore, AutoIdStoreError,
    LocalAutoIdStore, DEFAULT_AUTO_ID_STEP,
};

pub use auto_increment::{AutoIncrement, TableAutoId};
pub use auto_random::{AutoRandom, AutoRandomError, AutoRandomSpec};

pub use row_decoder::{
    DecodedRow, GeneratedColumnSelection, PreparedPointGetRowDecoder, RowDecoder,
};
pub use table_meta::{
    index_ranges_estimated_memory_usage, intersect_index_ranges, FkAction, IndexRange, KvColumn,
    KvForeignKey, KvIndex, PreparedPointGetDecodeContext, RowDecodeContext, TableCharset,
    TableHandle,
};
pub(crate) use table_scan::insert_extra_handle;
pub use table_scan::{
    capture_decoded_column_ids, IndexRangeCursor, RemoteIndexHandleCursor, RemoteRowCursor,
    RowCursor, TableScanExec,
};

use crate::storage::{MemTableStorage, StorageError, TableStorage};
use auto_id::AutoIdAllocator;
use row_decoder::fill_handle_columns;
use std::collections::HashSet;
use table_meta::NOT_NULL_FLAG;
use tidb_codec::table_key::{encode_row_key_with_handle, get_table_handle_key_range};
use tidb_datatype::{new_collation_enabled, Datum, FieldType, SessionTimeZone};

use index_entries::duplicate_value_text;
pub(in crate::kv_table) use index_entries::index_entry_handle;
pub(crate) use index_entries::IndexEntryForCheck;
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
    context: Option<std::rc::Rc<crate::StmtContext>>,
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
    pub fn with_context(mut self, context: std::rc::Rc<crate::StmtContext>) -> Self {
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
    pub fn context(&self) -> Option<&std::rc::Rc<crate::StmtContext>> {
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
    pub fn with_context(mut self, context: std::rc::Rc<crate::StmtContext>) -> Self {
        self.common.context = Some(context);
        self
    }

    /// Go `SkipWriteUntouchedIndices`.
    #[must_use]
    pub fn with_skip_write_untouched_indices(mut self) -> Self {
        self.skip_write_untouched_indices = true;
        self
    }

    /// The statement context propagated by `WithCtx`.
    #[must_use]
    pub fn context(&self) -> Option<&std::rc::Rc<crate::StmtContext>> {
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
    pub fn with_context(mut self, context: std::rc::Rc<crate::StmtContext>) -> Self {
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
    pub fn context(&self) -> Option<&std::rc::Rc<crate::StmtContext>> {
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

/// A table whose rows live as TiKV-format bytes in a sorted key/value map.
#[derive(Clone, Debug)]
pub struct KvTable {
    /// The table id (Go `TableInfo.ID`), the record-key prefix.
    pub table_id: i64,
    /// The table's name, which a duplicate-key error qualifies its index with
    /// (`Duplicate entry 'a' for key 'm.code'`).
    pub name: String,
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
    pub columns: std::sync::Arc<Vec<KvColumn>>,
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
    store: Box<dyn TableStorage>,
    /// Go `TableInfo.PKIsHandle`: the offset of the single integer primary-key
    /// column whose value IS the row handle, when the table has one.
    pk_handle_offset: Option<usize>,
    /// The table's indexes (Go `TableInfo.Indices`); `Arc`-shared like
    /// `columns`, for the same reason.
    indexes: std::sync::Arc<Vec<KvIndex>>,
    /// The AUTO_INCREMENT column's offset, if the table has one.
    auto_increment_offset: Option<usize>,
    /// Go's auto-id allocator, shared across the copies a transaction stages
    /// so that a consumed id is never returned (see [`AutoIdAllocator`]).
    auto_id: AutoIdAllocator,
    /// The `AUTO_RANDOM` handle layout and its distinct TARID allocator.
    auto_random: Option<AutoRandomSpec>,
    auto_random_id: AutoIdAllocator,
    /// Go `TableInfo.IsCommonHandle`: the clustered primary key's column
    /// offsets, whose encoding IS the row handle. Empty when the table has no
    /// clustered common handle.
    common_handle_offsets: Vec<usize>,
    /// Go `TableInfo.Charset`/`Collate`: the table's default character set and
    /// collation, which its unqualified string columns inherit.
    charset: TableCharset,
    /// Go `TableInfo.Comment`, persisted by CREATE/ALTER TABLE and served by
    /// metadata statements.
    comment: String,
    /// Go `TableInfo.AutoIDCache`: how many ids one reservation takes. Zero
    /// is Go's "unset"; `SHOW CREATE TABLE` prints it only when set.
    auto_id_cache: i64,
    /// Go `TableInfo.TTLInfo`: the table's `TTL` configuration, when it set
    /// one. Only the metadata is kept -- there is no background job here to
    /// delete expired rows -- but it is what `SHOW CREATE TABLE` prints, and
    /// a definition has to round-trip through its own output.
    ttl_info: Option<tidb_model::TTLInfo>,
    /// Go `TableInfo.ShardRowIDBits`: how many HIGH bits of an allocated
    /// `_tidb_rowid` carry a shard, so concurrent inserts land in different
    /// regions instead of all at the end of one.
    ///
    /// Zero -- the default -- means an unsharded, monotonically increasing
    /// handle, which is what every table here had before.
    shard_row_id_bits: u64,
    /// Go `TableInfo.PreSplitRegions`: how many regions a sharded table is
    /// pre-split into at creation. This tier has one store and splits
    /// nothing, but the number is part of the definition and `SHOW CREATE
    /// TABLE` prints it back.
    pre_split_regions: u64,
    /// Go `TableInfo.TableCacheStatusType`. The synchronous local DDL has no
    /// externally visible switching phase, so tables are either enabled or
    /// disabled here; cluster-loaded metadata may still carry the source
    /// value verbatim.
    cache_status: tidb_model::TableCacheStatusType,
    /// Go `TableInfo.TempTableType` (`setTemporaryType`, `create_table.go`):
    /// whether this is an ordinary table, a GLOBAL temporary table, or a
    /// LOCAL one.
    ///
    /// It is metadata on the table object rather than a fact the session
    /// keeps beside it because EVERY reader of it -- `SHOW CREATE TABLE`'s
    /// header, the `CREATE BINDING` refusal (Go's 8006), the DDL guards that
    /// refuse a temporary table an option -- already has the table in hand
    /// and must not have to ask a second authority that can disagree.
    ///
    /// The rows a temporary table holds are NOT stored here: a global
    /// temporary table's `TableInfo` is shared by every session while its
    /// DATA is private to one (Go `pkg/table/temptable`), which is what
    /// `tidb-session`'s temporary-table overlay carries.
    temp_table_type: tidb_model::TempTableType,
    /// The cluster's persisted new-collation mode, captured when this table
    /// object is built.
    ///
    /// Go keeps the same fact in `TableCommon.encoder` and hands it to every
    /// `index` it constructs. It is deliberately table state rather than a
    /// global read at each key/value call: one entry must never combine a
    /// legacy key with a new-collation restored-data value, or vice versa.
    use_new_collation: bool,
    /// Go `TableInfo.ForeignKeys`: the constraints this table DECLARES, i.e.
    /// the child side. The parent side is found by scanning the catalog for
    /// the tables whose foreign keys name this one, as Go's
    /// `ReferredFKInfo` index does.
    foreign_keys: Vec<KvForeignKey>,
    /// Go `TableInfo.MaxForeignKeyID`: the counter an UNNAMED constraint is
    /// named after (`fk_1`, `fk_2`, ...). It only ever rises, so dropping
    /// `fk_1` and adding another unnamed constraint yields `fk_2` rather than
    /// reusing the freed name.
    max_foreign_key_id: i64,
    /// Go `TableInfo.Partition`: how the table's rows are spread over
    /// physical key prefixes, or `None` for an ordinary table whose rows all
    /// live under [`KvTable::table_id`].
    ///
    /// This is the ONLY branch partitioning adds to the write path: every
    /// record key goes through [`KvTable::record_physical_id`], which reads
    /// this field once and then has no cases left. See
    /// [`crate::partition_routing`].
    partition: Option<Box<crate::partition_routing::PartitionSpec>>,
    /// Go `TableInfo.PlacementPolicyRef`, reduced to the policy NAME.
    ///
    /// A table-level policy covers the whole table's key range, including
    /// every partition that does not name one of its own -- which is why Go
    /// does not copy it down onto the partitions.
    placement_policy: Option<tidb_model::PolicyRefInfo>,
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
    read_partitions: Option<Vec<i64>>,
    /// Go `session.HasDirtyContent(tid)` (`pkg/session/txn.go`): whether the
    /// transaction this table copy belongs to has STAGED a row write to it.
    ///
    /// Go answers the question by seeking the table's key prefix in the
    /// transaction's membuffer -- `it.Valid() && bytes.HasPrefix(it.Key(),
    /// seekKey)`. This tier stages a transaction as a private catalog COPY
    /// rather than a membuffer, so the staged keys are indistinguishable from
    /// the committed ones once written; the flag is what keeps the question
    /// answerable. It is set by the three row writes below, and cleared for
    /// every statement that does not continue an open transaction
    /// (`tidb_session`'s `execute_statement`) -- which is precisely when Go's
    /// membuffer is empty.
    ///
    /// Its ONE reader is the planner-side gate Go spells
    /// `tableHasDirtyContent` (`pkg/planner/core/util.go:156`), which decides
    /// whether a `UnionScan` sits above the reader; see
    /// [`crate::access_path::IndexRangeSourceExec`] for the row ORDER that
    /// operator imposes.
    dirty_content: bool,
}

/// A failure while encoding or decoding table bytes.
#[derive(Debug)]
pub enum KvTableError {
    /// A row failed to encode.
    Encode(String),
    /// Go `ErrDupKeyName` (1061).
    DuplicateKeyName(String),
    /// A generated column's expression could not be evaluated for a row.
    /// DDL admits only expressions this tier can build, so this is a
    /// value-domain failure (an overflow, say), not an unported form.
    Generation {
        /// The generated column.
        column: String,
        /// The evaluation failure.
        detail: String,
        /// The evaluation failure itself, when it carries a MySQL code the
        /// statement has to report (1365 for a zero divisor under
        /// `ERROR_FOR_DIVISION_BY_ZERO`).
        eval: Option<tidb_expr::EvalError>,
    },
    /// Go `ErrDupEntry` (1062): a row with this primary key already exists.
    DuplicateEntry {
        /// The rejected key value, as MySQL prints it.
        value: String,
        /// The violated key's name; Go names the clustered one PRIMARY.
        key: String,
    },
    /// Go `ErrTruncatedWrongValue` (1292): a stored value is not a valid
    /// number for the column's new numeric type.
    TruncatedIncorrectValue {
        /// The numeric domain Go names in the message.
        kind: &'static str,
        /// The value it could not read.
        value: String,
    },
    /// Go `ErrTruncatedWrongValueForField` (1265) with the value form: a
    /// stored value does not fit the column's new type.
    DataTruncatedValue {
        /// The column being modified.
        column: String,
        /// The value that does not fit.
        value: String,
    },
    /// Go's plain VECTOR conversion error. Vector dimension failures are not
    /// retitled as a generic truncation while a column is reorganized.
    Vector(String),
    /// Go `ErrTruncatedWrongValueForField` (1265) with the row form: a stored
    /// NULL is rejected by the column's new NOT NULL.
    DataTruncatedAtRow {
        /// The column being modified.
        column: String,
        /// The offending row's 1-based position.
        row: usize,
    },
    /// Go `table.ErrNoPartitionForGivenValue` (1526): the row's partition
    /// value falls outside every partition. HASH cannot produce it; RANGE and
    /// LIST can (captured: `insert into r2 values (20,0)` on a RANGE table
    /// with no `MAXVALUE` partition is `Table has no partition for value 20`).
    NoPartitionForValue(String),
    /// Go `table.ErrRowDoesNotMatchGivenPartitionSet` (1748): an
    /// `INSERT ... PARTITION (...)` row routed to a partition the statement
    /// did not name.
    RowDoesNotMatchGivenPartitionSet,
    /// Go `types.ErrOverflow` (1690) raised by `locateHashPartition`'s
    /// `ConvertTo(TypeLonglong)`: the row's partition value has no signed
    /// reading, so the write fails instead of routing by a clamped one.
    PartitionValueOverflowsBigint(String),
    /// Go `autoid.ErrAutoincReadFailed` (1467) raised while allocating the
    /// row's `_tidb_rowid`, which shares the AUTO_INCREMENT counter.
    AutoIdExhausted,
    /// A stored value failed to decode.
    Decode(String),
    /// The storage layer refused a read or write.
    Storage(String),
    /// TiDB `ErrOptOnCacheTable` (8242).
    CacheTableUnsupported(&'static str),
}

/// Carries a generation failure across the module boundary without losing the
/// MySQL code the evaluation error already knows.
fn generation_error(error: crate::generated_column::GenerationError) -> KvTableError {
    KvTableError::Generation {
        column: error.column,
        detail: error.detail,
        eval: error.eval,
    }
}

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

    pub fn columns_mut(&mut self) -> &mut Vec<KvColumn> {
        std::sync::Arc::make_mut(&mut self.columns)
    }

    fn indexes_mut(&mut self) -> &mut Vec<KvIndex> {
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
            auto_id_cache: 0,
            ttl_info: None,
            shard_row_id_bits: 0,
            pre_split_regions: 0,
            table_id,
            name: String::new(),
            columns: std::sync::Arc::new(columns),
            hidden_columns: 0,
            mv_key_part_sources: std::collections::BTreeMap::new(),
            store,
            pk_handle_offset: None,
            indexes: std::sync::Arc::new(Vec::new()),
            common_handle_offsets: Vec::new(),
            auto_increment_offset: None,
            auto_id: AutoIdAllocator::new(),
            auto_random: None,
            auto_random_id: AutoIdAllocator::new(),
            charset: TableCharset::default(),
            comment: String::new(),
            cache_status: tidb_model::TableCacheStatusType::DISABLE,
            temp_table_type: tidb_model::TempTableType::NONE,
            use_new_collation,
            foreign_keys: Vec::new(),
            max_foreign_key_id: 0,
            partition: None,
            placement_policy: None,
            read_partitions: None,
            dirty_content: false,
        }
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

    /// Go `session.HasDirtyContent(tid)`: whether the open transaction has
    /// staged a row write to this table. See the field's own doc.
    #[must_use]
    pub fn has_dirty_content(&self) -> bool {
        self.dirty_content
    }

    /// Forgets the staged writes: the state Go's membuffer is in at the start
    /// of a transaction. See [`crate::driver::Catalog::clear_dirty_content`].
    pub fn clear_dirty_content(&mut self) {
        self.dirty_content = false;
    }

    /// Builds the table `CREATE TABLE ... LIKE self` creates: Go
    /// `ddl.BuildTableInfoWithLike`.
    ///
    /// Go shallow-copies the whole `TableInfo` and then resets the few fields
    /// that identify the table rather than describe it. This builds the copy
    /// the other way round -- from an EMPTY table of the new id, copying only
    /// the structural fields across -- because the reset list is where the
    /// bugs live: a shallow copy that forgets one of them yields a table that
    /// silently shares its source's rows, row handles, or auto-increment
    /// counter. Starting empty means a field is inherited only if it is named
    /// here.
    ///
    /// Deliberately NOT inherited, each matching a line of Go's reset list:
    ///
    /// * the rows and their storage, which are not in `TableInfo` at all;
    /// * `auto_id` (Go `tblInfo.AutoIncID = 0`), the counter both the
    ///   AUTO_INCREMENT column and `_tidb_rowid` draw from, so the
    ///   copy's first row is handle 1 however far the source has run;
    /// * `foreign_keys` (Go `tblInfo.ForeignKeys = nil`), and with them
    ///   `max_foreign_key_id`;
    /// * `name` and `table_id`, which the caller supplies.
    ///
    /// The partitioning IS inherited, but `allocate_partition_id` must hand
    /// out ids of its own: a partition is a distinct PHYSICAL table, and two
    /// tables writing records under one physical id would interleave rows.
    #[must_use]
    pub fn create_like(
        &self,
        table_id: i64,
        allocate_partition_id: &mut dyn FnMut() -> i64,
    ) -> Self {
        let mut copy = KvTable::with_storage_and_collation(
            table_id,
            self.columns.as_ref().clone(),
            Box::new(MemTableStorage::new()),
            self.use_new_collation,
        );
        copy.hidden_columns = self.hidden_columns;
        copy.mv_key_part_sources = self.mv_key_part_sources.clone();
        copy.pk_handle_offset = self.pk_handle_offset;
        copy.indexes = self.indexes.clone();
        copy.common_handle_offsets = self.common_handle_offsets.clone();
        copy.auto_increment_offset = self.auto_increment_offset;
        copy.auto_random = self.auto_random;
        if let Some(spec) = copy.auto_random {
            copy.auto_random_id.set_unsigned(spec.unsigned);
        }
        copy.charset = self.charset;
        copy.comment = self.comment.clone();
        if let Some(partition) = self.partition() {
            let mut partition = partition.clone();
            for definition in &mut partition.definitions {
                definition.id = allocate_partition_id();
            }
            copy.set_partition(partition);
        }
        copy
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

    /// The table's partitioning for in-place edits.
    pub fn partition_mut(&mut self) -> Option<&mut crate::partition_routing::PartitionSpec> {
        self.partition.as_deref_mut()
    }

    /// Go `TableInfo.PlacementPolicyRef`, by name.
    #[must_use]
    pub fn placement_policy(&self) -> Option<&tidb_model::PolicyRefInfo> {
        self.placement_policy.as_ref()
    }

    /// Records the policy this table names.
    pub fn set_placement_policy(&mut self, policy: Option<tidb_model::PolicyRefInfo>) {
        self.placement_policy = policy;
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
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
            let mut rows = 0;
            while iterator.valid() {
                rows += 1;
                iterator
                    .next()
                    .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
            }
            iterator.close();
            counts.push((definition.name.clone(), rows));
        }
        Ok(counts)
    }

    /// The physical table id `row`'s record key is written under: the
    /// partition it routes into, or the table itself.
    ///
    /// This is the whole of "an unpartitioned table is a table with one
    /// partition whose id is its own" -- every write site calls this instead
    /// of reading `table_id`, so none of them carries a partitioning branch.
    fn record_physical_id(
        &self,
        row: &[Datum],
        ctx: &impl tidb_expr::Columns,
    ) -> Result<i64, KvTableError> {
        let Some(partition) = &self.partition else {
            return Ok(self.table_id);
        };
        partition
            .locate(row, &self.columns, ctx)
            .map_err(|error| match error {
                crate::partition_routing::RoutingError::NoPartitionForValue(value) => {
                    KvTableError::NoPartitionForValue(value)
                }
                crate::partition_routing::RoutingError::ValueOverflowsBigint(value) => {
                    KvTableError::PartitionValueOverflowsBigint(value)
                }
                crate::partition_routing::RoutingError::Eval(error) => {
                    KvTableError::Decode(format!("{error:?}"))
                }
                crate::partition_routing::RoutingError::Conversion(error) => {
                    KvTableError::Decode(error)
                }
            })
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
        ctx: &impl tidb_expr::Columns,
    ) -> Result<(), KvTableError> {
        if selected.contains(&self.record_physical_id(row, ctx)?) {
            Ok(())
        } else {
            Err(KvTableError::RowDoesNotMatchGivenPartitionSet)
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
        ctx: &impl tidb_expr::Columns,
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
    pub(crate) fn handle_partition_routes(
        &self,
        handles: &[TableHandle],
        zone: &SessionTimeZone,
        ctx: &impl tidb_expr::Columns,
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
    pub(crate) fn row_partition_route(
        &self,
        row: &[Datum],
        ctx: &impl tidb_expr::Columns,
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
        ctx: &impl tidb_expr::Columns,
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
    pub(crate) fn record_physical_ids(&self) -> Vec<i64> {
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
    pub(crate) fn stats_physical_id(&self) -> i64 {
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

    /// The stored record for `handle` -- its key AND its bytes -- found by
    /// probing the partitions, or `None` when none holds it.
    ///
    /// The bytes come back with the key because every caller that wants the
    /// key wants either the row or nothing, and fetching them separately
    /// would double the round trips a point read costs -- which the access-
    /// path tests count.
    fn stored_record(
        &mut self,
        handle: &TableHandle,
    ) -> Result<Option<(Key, Vec<u8>)>, KvTableError> {
        for id in self.record_physical_ids() {
            let key = Key::from_bytes(encode_row_key_with_handle(id, &handle.record_handle()));
            match self.store.get(&key) {
                Ok(entry) => return Ok(Some((key, entry))),
                Err(StorageError::NotFound) => {}
                Err(error) => return Err(KvTableError::Storage(format!("{error:?}"))),
            }
        }
        Ok(None)
    }

    /// [`KvTable::stored_record`] without the bytes.
    fn stored_record_key(&mut self, handle: &TableHandle) -> Result<Option<Key>, KvTableError> {
        Ok(self.stored_record(handle)?.map(|(key, _)| key))
    }

    /// One stored record per requested handle, in request order; `None`
    /// where the row is absent.
    ///
    /// Reads through ONE batched storage call per physical partition -- Go's
    /// `BatchPointGetExec.initialize` fetching every handle with a single
    /// `BatchGet` -- instead of one point read per handle. A handle whose
    /// row is absent yields `None`, Go's point get that finds nothing.
    pub(crate) fn stored_records_batched(
        &mut self,
        handles: &[TableHandle],
        physical_ids: Option<&[i64]>,
        context: &RowDecodeContext,
    ) -> Result<Vec<Option<Vec<Datum>>>, KvTableError> {
        let mut rows: Vec<Option<Vec<Datum>>> = vec![None; handles.len()];
        // Group the requested handles by the physical partition they name
        // (all under this table id for an unpartitioned table), so each
        // partition costs exactly one batched read.
        let mut grouped: std::collections::BTreeMap<i64, Vec<(usize, Key)>> =
            std::collections::BTreeMap::new();
        for (index, handle) in handles.iter().enumerate() {
            let id = physical_ids
                .and_then(|ids| ids.get(index).copied())
                .unwrap_or(self.table_id);
            grouped
                .entry(id)
                .or_default()
                .push((
                    index,
                    Key::from_bytes(encode_row_key_with_handle(id, &handle.record_handle())),
                ));
        }
        for (_id, keys) in grouped {
            let key_refs: Vec<Key> = keys.iter().map(|(_, key)| key.clone()).collect();
            let found = self
                .store
                .batch_get(&key_refs)
                .map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
            for (index, key) in keys {
                if let Some(entry) = found.get(&key) {
                    rows[index] =
                        Some(self.decode_row_entry(&handles[index], entry, context)?);
                }
            }
        }
        Ok(rows)
    }

    /// Records a foreign key this table declares.
    pub fn add_foreign_key(&mut self, foreign_key: KvForeignKey) {
        self.foreign_keys.push(foreign_key);
    }

    /// Go `allocateFKIndexID`: consumes the next constraint id.
    ///
    /// Kept separate from [`KvTable::add_foreign_key`] because Go consumes it
    /// BEFORE the constraint is validated against the table's rows, and the
    /// rollback that removes a rejected constraint does not give it back --
    /// captured, an `ADD FOREIGN KEY` that fails 1452 makes the NEXT unnamed
    /// constraint `fk_2` rather than `fk_1`.
    pub fn allocate_foreign_key_id(&mut self) -> i64 {
        self.max_foreign_key_id += 1;
        self.max_foreign_key_id
    }

    /// The name Go gives the NEXT unnamed constraint on this table:
    /// `fk_{MaxForeignKeyID+1}`.
    #[must_use]
    pub fn next_foreign_key_name(&self) -> String {
        format!("fk_{}", self.max_foreign_key_id + 1)
    }

    /// Removes the constraint with this name, reporting whether one went.
    ///
    /// Go `dropForeignKey` rebuilds `TableInfo.ForeignKeys` without it and
    /// leaves `MaxForeignKeyID` and the index the constraint relied on alone
    /// -- the index outlives the constraint and has to be dropped by name.
    pub fn drop_foreign_key(&mut self, name: &str) -> bool {
        let before = self.foreign_keys.len();
        self.foreign_keys
            .retain(|key| !key.name.eq_ignore_ascii_case(name));
        self.foreign_keys.len() != before
    }

    /// The foreign keys this table declares (the child side).
    #[must_use]
    pub fn foreign_keys(&self) -> &[KvForeignKey] {
        &self.foreign_keys
    }

    /// The same list, for the one caller that rewrites the column NAMES a
    /// constraint stores: Go `updateFKInfoWhenModifyColumn`.
    pub fn foreign_keys_mut(&mut self) -> &mut [KvForeignKey] {
        &mut self.foreign_keys
    }

    /// Sets the table's default character set and collation.
    pub fn set_charset(&mut self, charset: TableCharset) {
        self.charset = charset;
    }

    /// The table's default character set and collation.
    #[must_use]
    pub const fn charset(&self) -> TableCharset {
        self.charset
    }

    /// Records Go `TableInfo.AutoIDCache` without touching the allocator,
    /// for a loader that already gave the allocator its step.
    pub fn set_recorded_auto_id_cache(&mut self, cache: i64) {
        self.auto_id_cache = cache;
    }

    /// Go `TableInfo.AutoIDCache`, zero when the table set none.
    #[must_use]
    pub const fn auto_id_cache(&self) -> i64 {
        self.auto_id_cache
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

    /// Records Go `TableInfo.TempTableType` (`setTemporaryType`).
    pub fn set_temp_table_type(&mut self, kind: tidb_model::TempTableType) {
        self.temp_table_type = kind;
    }

    /// Go `TableInfo.TempTableType`.
    #[must_use]
    pub const fn temp_table_type(&self) -> tidb_model::TempTableType {
        self.temp_table_type
    }

    /// Whether this table is temporary at all, in either scope -- Go's
    /// `tbl.Meta().TempTableType != model.TempTableNone`, which is the exact
    /// test every 8006 refusal makes.
    #[must_use]
    pub const fn is_temporary(&self) -> bool {
        self.temp_table_type.0 != tidb_model::TempTableType::NONE.0
    }

    /// Records the table's `TTL` configuration (Go `TableInfo.TTLInfo`).
    pub fn set_ttl_info(&mut self, info: Option<tidb_model::TTLInfo>) {
        self.ttl_info = info;
    }

    /// The table's `TTL` configuration, when it has one.
    #[must_use]
    pub const fn ttl_info(&self) -> Option<&tidb_model::TTLInfo> {
        self.ttl_info.as_ref()
    }

    /// Records `SHARD_ROW_ID_BITS` (Go `TableInfo.ShardRowIDBits`).
    pub fn set_shard_row_id_bits(&mut self, bits: u64) {
        self.shard_row_id_bits = bits;
    }

    /// Go `TableInfo.ShardRowIDBits`.
    #[must_use]
    pub const fn shard_row_id_bits(&self) -> u64 {
        self.shard_row_id_bits
    }

    /// Records `PRE_SPLIT_REGIONS` (Go `TableInfo.PreSplitRegions`).
    pub fn set_pre_split_regions(&mut self, regions: u64) {
        self.pre_split_regions = regions;
    }

    /// Go `TableInfo.PreSplitRegions`.
    #[must_use]
    pub const fn pre_split_regions(&self) -> u64 {
        self.pre_split_regions
    }

    /// Replaces the table-level comment.
    pub fn set_comment(&mut self, comment: String) {
        self.comment = comment;
    }

    /// The table-level comment served by metadata statements.
    #[must_use]
    pub fn comment(&self) -> &str {
        &self.comment
    }

    /// Marks the columns whose encoding is the clustered row handle, which Go
    /// records as `TableInfo.IsCommonHandle`.
    pub fn set_common_handle_offsets(&mut self, offsets: Vec<usize>) {
        self.common_handle_offsets = offsets;
    }

    /// The clustered primary key's column offsets, empty when there is none.
    #[must_use]
    pub fn common_handle_offsets(&self) -> &[usize] {
        &self.common_handle_offsets
    }

    /// Encodes one complete clustered composite primary-key tuple as its
    /// record handle.
    ///
    /// Index-join probes use this path because part of the tuple may come
    /// from constants and the remainder from an outer row; routing the tuple
    /// through the table's collation mode and `CommonHandle` padding keeps it
    /// byte-identical to the handle created by an INSERT.
    pub(crate) fn common_handle_of_values(
        &self,
        values: &[Datum],
        zone: &SessionTimeZone,
    ) -> Result<TableHandle, KvTableError> {
        if self.common_handle_offsets.len() != values.len() {
            return Err(KvTableError::Encode(
                "a common handle probe does not cover every key column".to_owned(),
            ));
        }
        let encoded = tidb_codec::Encoder::new(self.use_new_collation)
            .encode_key_in_timezone(zone, values)
            .map_err(|e| KvTableError::Encode(format!("{e:?}")))?;
        let padded = tidb_txnkv::CommonHandle::new(encoded)
            .map_err(|e| KvTableError::Encode(format!("{e:?}")))?;
        Ok(TableHandle::Common(padded.encoded().to_vec()))
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

    /// Recomputes every generated column of `row` from the row itself
    /// (Go's `addRecord`/`updateRecord` calling `GenerateColumnValue`).
    ///
    /// Idempotent, so every writer may call it without coordinating with any
    /// other writer -- see [`crate::generated_column`].
    ///
    /// A row shorter than the table is WIDENED here rather than rejected: a
    /// statement builds its row over the VISIBLE columns, and the hidden
    /// columns an expression index added are exactly the ones whose value
    /// this call is what produces. Widening in the one place that fills them
    /// means no caller has to know the table has any.
    /// `ctx` is the writing statement's evaluation context: a generated
    /// expression obeys the statement's SQL mode exactly as any other
    /// expression of that statement does, so `b INT AS (100/a)` over `a = 0`
    /// fails the write under `ERROR_FOR_DIVISION_BY_ZERO` and stores NULL
    /// without it.
    pub fn materialize_generated(
        &self,
        row: &mut Vec<Datum>,
        ctx: &impl tidb_expr::Columns,
    ) -> Result<(), KvTableError> {
        if row.len() < self.columns.len() {
            row.resize(self.columns.len(), Datum::Null);
        }
        crate::generated_column::materialize(&self.columns, row, false, ctx)
            .map_err(generation_error)
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
    fn handle_of_row(
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
                // Go `AllocHandle`: `_tidb_rowid` comes off the SAME counter
                // the AUTO_INCREMENT column allocates from -- see this
                // method's doc for why one counter serves both.
                let handle = self.auto_id.alloc(1, 1).map_err(|error| match error {
                    AutoIdError::Store(detail) => KvTableError::Encode(detail.0),
                    _ => KvTableError::AutoIdExhausted,
                })?;
                // Go `AllocHandleIDs`: `if meta.ShardRowIDBits > 0 { base =
                // shardFmt.Compose(shard, base) }`. Composing here rather
                // than at the caller keeps the ONE place a heap table's
                // handle is minted responsible for its whole shape.
                Ok(TableHandle::Int(compose_row_id_shard(
                    self.shard_row_id_bits,
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
    pub(crate) fn common_handle_from_values(
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
        let encoded = tidb_codec::Encoder::new(self.use_new_collation)
            .encode_key_in_timezone(zone, values)
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
    fn encode_row_value(
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
            let lossless_handle = self.pk_handle_offset == Some(offset)
                || (self.common_handle_offsets.contains(&offset)
                    && !column
                        .field_type
                        .need_restored_data_with_collation(self.use_new_collation));
            if lossless_handle || crate::generated_column::is_virtual(column) {
                continue;
            }
            ids.push(column.id);
            values.push(row.get(offset).cloned().unwrap_or(Datum::Null));
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
        self.auto_random_id.reset()
    }

    /// Sets the table's name, used to qualify a duplicate-key error.
    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_owned();
    }

    /// Go's key name in a duplicate-entry error: `table.index`.
    fn qualified_key(&self, index_name: &str) -> String {
        if self.name.is_empty() {
            index_name.to_owned()
        } else {
            format!("{}.{}", self.name, index_name)
        }
    }

    /// Creates an index over the existing rows, which Go backfills as part of
    /// the DDL. A unique index whose existing rows already collide is
    /// rejected with the duplicate it found, leaving the table unchanged.
    pub fn create_index_with_context(
        &mut self,
        index: KvIndex,
        ctx: &crate::StmtContext,
    ) -> Result<(), KvTableError> {
        self.create_index_in(index, ctx, &RowDecodeContext::for_ddl(ctx))
    }

    /// Legacy index backfill retained for unmigrated server callers. Row
    /// decoding uses the exact former `DEFAULT_STATEMENT_FLAGS` behavior.
    pub fn create_index(
        &mut self,
        index: KvIndex,
        ctx: &impl tidb_expr::Columns,
    ) -> Result<(), KvTableError> {
        let zone = ctx.time_zone();
        self.create_index_in(index, ctx, &RowDecodeContext::legacy_default(&zone))
    }

    fn create_index_in(
        &mut self,
        index: KvIndex,
        ctx: &impl tidb_expr::Columns,
        decode_context: &RowDecodeContext,
    ) -> Result<(), KvTableError> {
        let zone = ctx.time_zone();
        if self
            .indexes
            .iter()
            .any(|existing| existing.name.eq_ignore_ascii_case(&index.name))
        {
            return Err(KvTableError::DuplicateKeyName(index.name.clone()));
        }
        // Go's index worker uses a full RowDecoder map. Every generated
        // column is therefore recomputed under the DDL statement context
        // before an index entry is persisted, including STORED columns whose
        // old bytes may predate this backfill.
        let rows = self.scan_rows_with_handles_recomputed(decode_context)?;
        let mut written = Vec::new();
        for (handle, row) in &rows {
            let physical_id = self.stored_physical_id(handle)?.unwrap_or(self.table_id);
            let (key, distinct) = self.index_key(&index, row, handle, physical_id, &zone)?;
            let key = Key::from_bytes(key);
            if distinct && self.store.get(&key).is_ok() {
                // Undo the entries this backfill already wrote, so a rejected
                // CREATE INDEX leaves no partial index behind.
                for key in written {
                    let _ = self.store.delete(key);
                }
                return Err(KvTableError::DuplicateEntry {
                    value: duplicate_value_text(&self.index_values(&index, row)),
                    key: self.qualified_key(&index.name),
                });
            }
            // The same entry value an INSERT writes, restored data and all --
            // a backfill that stored a simpler one would leave the index
            // holding two different formats for the same table.
            let value = self.index_entry_value(&index, row, handle, distinct, &zone)?;
            self.store
                .set(key.clone(), value)
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
            written.push(key);
        }
        self.indexes_mut().push(index);
        Ok(())
    }

    /// Drops an index and every entry it owns, reporting whether it existed.
    pub fn drop_index_with_context(
        &mut self,
        name: &str,
        ctx: &crate::StmtContext,
    ) -> Result<bool, KvTableError> {
        let zone = ctx.session_zone();
        let decode_context = RowDecodeContext::for_ddl(ctx);
        self.drop_index_in(name, &zone, &decode_context)
    }

    /// Legacy zone-only index removal retained for unmigrated server callers.
    /// Row decoding uses the exact former `DEFAULT_STATEMENT_FLAGS` behavior.
    pub fn drop_index(&mut self, name: &str, zone: &SessionTimeZone) -> Result<bool, KvTableError> {
        self.drop_index_in(name, zone, &RowDecodeContext::legacy_default(zone))
    }

    fn drop_index_in(
        &mut self,
        name: &str,
        zone: &SessionTimeZone,
        decode_context: &RowDecodeContext,
    ) -> Result<bool, KvTableError> {
        let Some(position) = self
            .indexes
            .iter()
            .position(|index| index.name.eq_ignore_ascii_case(name))
        else {
            return Ok(false);
        };
        let index = self.indexes_mut().remove(position);
        let rows = self.scan_rows_with_handles_recomputed(decode_context)?;
        for (handle, row) in &rows {
            let physical_id = self.stored_physical_id(handle)?.unwrap_or(self.table_id);
            let (key, _) = self.index_key(&index, row, handle, physical_id, zone)?;
            self.store
                .delete(Key::from_bytes(key))
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        }
        Ok(true)
    }

    /// The next free index id.
    #[must_use]
    pub fn next_index_id(&self) -> i64 {
        self.indexes.iter().map(|index| index.id).max().unwrap_or(0) + 1
    }

    /// The columns a user can name or see: everything but the hidden tail.
    ///
    /// A visible column's offset in this slice IS its physical offset, which
    /// is the whole point of keeping the hidden ones last.
    #[must_use]
    pub fn visible_columns(&self) -> &[KvColumn] {
        &self.columns[..self.visible_column_count()]
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
    pub(crate) fn logical_data_source_column_count(&self) -> usize {
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
        self.columns.iter().map(|c| c.id).max().unwrap_or(0) + 1
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
        for index in self.indexes_mut() {
            for value in &mut index.column_offsets {
                shift(value);
            }
        }
    }

    /// Rewrites a column's definition, converting the stored values into the
    /// new type, which is Go's ALTER TABLE MODIFY / CHANGE COLUMN.
    ///
    /// The column keeps its id, so index entries and handles stay valid; the
    /// rows are re-encoded because the value bytes carry the column's type.
    /// `new_position` moves the column, which Go's `FIRST`/`AFTER` clause does
    /// by reordering `TableInfo.Columns` while the ids stay put.
    ///
    /// A value the new type cannot hold aborts the whole statement and leaves
    /// the table untouched, as Go's `checkModifyColumnData` does before the
    /// schema change is applied.
    pub fn modify_column_with_context(
        &mut self,
        offset: usize,
        new_column: KvColumn,
        new_position: Option<usize>,
        ctx: &crate::StmtContext,
    ) -> Result<(), KvTableError> {
        let zone = ctx.session_zone();
        let substitute = null_timestamp_substitute(&new_column.field_type, ctx);
        self.modify_column_in(
            offset,
            new_column,
            new_position,
            &zone,
            &RowDecodeContext::for_ddl(ctx),
            substitute,
        )
    }

    /// Legacy zone-only column rewrite retained for unmigrated callers. Row
    /// decoding uses the exact former `DEFAULT_STATEMENT_FLAGS` behavior.
    pub fn modify_column(
        &mut self,
        offset: usize,
        new_column: KvColumn,
        new_position: Option<usize>,
        zone: &SessionTimeZone,
    ) -> Result<(), KvTableError> {
        // No statement context, so no statement clock: the NULL substitution
        // below needs one and this caller cannot supply it.
        self.modify_column_in(
            offset,
            new_column,
            new_position,
            zone,
            &RowDecodeContext::legacy_default(zone),
            None,
        )
    }

    fn modify_column_in(
        &mut self,
        offset: usize,
        new_column: KvColumn,
        new_position: Option<usize>,
        zone: &SessionTimeZone,
        decode_context: &RowDecodeContext,
        null_substitute: Option<Datum>,
    ) -> Result<(), KvTableError> {
        let target = new_column.field_type.clone();
        let not_null = target.flags() & NOT_NULL_FLAG != 0;
        // Keep the physical id from the record key rather than attempting to
        // route the converted value again. Go permits a non-partition column
        // rewrite without reorganizing partitions, and separately restricts
        // partition-column changes to definitions whose routing is stable.
        // In both cases the existing physical owner is the source of truth.
        let rows = self.scan_physical_rows_with_handles_with_context(decode_context)?;
        let mut converted_rows = Vec::with_capacity(rows.len());
        for (row_number, (physical_id, handle, row)) in rows.into_iter().enumerate() {
            let mut row = row;
            let mut value = row[offset].clone();
            // Go `updateColumnWorker.getRowRecord`: "convert null value to
            // timestamp should be substituted with current timestamp if
            // NOT_NULL flag is set". The substituted value then takes the
            // ordinary cast below, exactly as a stored one would.
            if let (true, Some(substitute)) = (value.is_null(), null_substitute.as_ref()) {
                value = substitute.clone();
            }
            // Go reports the offending row's 1-based position for a value the
            // new NOT NULL rejects, and the value itself for a bad conversion.
            if value.is_null() {
                if not_null {
                    return Err(KvTableError::DataTruncatedAtRow {
                        column: new_column.name.clone(),
                        row: row_number + 1,
                    });
                }
            } else {
                let converted = value
                    .convert_to(&target, tidb_datatype::STRICT_FLAGS)
                    .map_err(|error| {
                        if target.code() == tidb_datatype::FieldTypeCode::VectorFloat32 {
                            KvTableError::Vector(error.to_string())
                        } else {
                            convert_failure(&new_column.name, &target, &value)
                        }
                    })?;
                if converted.event.is_some() {
                    return Err(convert_failure(&new_column.name, &target, &value));
                }
                row[offset] = converted.value;
            }
            converted_rows.push((physical_id, handle, row));
        }

        self.columns_mut()[offset] = new_column;
        if let Some(partition) = &mut self.partition {
            partition.update_dependency_type(
                &self.columns[offset].name,
                &self.columns[offset].field_type,
            );
        }
        // Go `ddl.UpdateIndexCol` (`pkg/ddl/column.go`), run as part of the
        // same column change: a key part over this column keeps its declared
        // prefix only while the new type is still prefixable AND wider than
        // the prefix. Doing it here, before the entries below are rebuilt,
        // is what keeps the stored entries and the recorded lengths one
        // answer -- a caller that updated the metadata separately could
        // leave cut entries under a key part that no longer declares a cut.
        // Captured: `char(255) unique index idx(a(2))` modified to `float`
        // prints `UNIQUE KEY idx (a)`, and `char(250) idx(a(10))` modified to
        // `char(9)` does too.
        let field_type = self.columns[offset].field_type.clone();
        for index in self.indexes_mut() {
            for (position, at) in index.column_offsets.iter().enumerate() {
                if *at != offset {
                    continue;
                }
                let length = index.prefix_lengths[position];
                if !field_type.code().is_type_prefixable() || field_type.flen() <= length {
                    index.prefix_lengths[position] = crate::ddl::index_prefix::UNSPECIFIED_LENGTH;
                }
            }
        }
        if let Some(position) = new_position.filter(|position| *position != offset) {
            self.move_column(offset, position);
            for (_, _, row) in &mut converted_rows {
                let value = row.remove(offset);
                row.insert(position, value);
            }
        }

        // The value bytes encode the column's type, so every row is written
        // again; the handles and index ids are unchanged.
        self.store.clear();
        for (physical_id, handle, row) in &converted_rows {
            let value = self.encode_row_value(row, zone)?;
            let key = Key::from_bytes(encode_row_key_with_handle(
                *physical_id,
                &handle.record_handle(),
            ));
            self.write_index_entries(row, handle, *physical_id, zone)?;
            self.store
                .set(key, value)
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        }
        Ok(())
    }

    /// Moves the column at `from` to `to`, carrying every offset that
    /// addresses a column with it.
    fn move_column(&mut self, from: usize, to: usize) {
        let column = self.columns_mut().remove(from);
        self.columns_mut().insert(to, column);
        let shift = |offset: &mut usize| {
            *offset = if *offset == from {
                to
            } else if from < *offset && *offset <= to {
                *offset - 1
            } else if to <= *offset && *offset < from {
                *offset + 1
            } else {
                *offset
            };
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
        for index in self.indexes_mut() {
            for offset in &mut index.column_offsets {
                shift(offset);
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
    pub fn conflicting_handles(
        &mut self,
        row: &[Datum],
        ctx: &impl tidb_expr::Columns,
    ) -> Result<Vec<TableHandle>, KvTableError> {
        let zone = ctx.time_zone();
        let physical_id = self.record_physical_id(row, ctx)?;
        let mut found: Vec<TableHandle> = Vec::new();
        let clustered = self.pk_handle_offset.is_some() || !self.common_handle_offsets.is_empty();
        if clustered {
            let handle = self.handle_of_row(row, &zone, 0)?;
            if self.row_exists(&handle)? {
                found.push(handle);
            }
        }
        for index in self.indexes.as_ref().clone() {
            if !index.unique {
                continue;
            }
            // A distinct entry's key does not carry the handle, so the
            // candidate handle below is only a placeholder for the key build.
            let (key, distinct) =
                self.index_key(&index, row, &TableHandle::Int(0), physical_id, &zone)?;
            if !distinct {
                continue;
            }
            let Ok(value) = self.store.get(&Key::from_bytes(key)) else {
                continue;
            };
            let handle = tidb_tablecodec::decode_handle_in_index_value(&value)
                .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
            let handle = match handle {
                tidb_txnkv::Handle::Int(value) => TableHandle::Int(value.value()),
                tidb_txnkv::Handle::Common(common) => {
                    TableHandle::Common(common.encoded().to_vec())
                }
                tidb_txnkv::Handle::Partition(_) => {
                    return Err(KvTableError::Decode(
                        "a partitioned handle has no place in this tier".to_owned(),
                    ))
                }
            };
            if !found.contains(&handle) {
                found.push(handle);
            }
        }
        Ok(found)
    }

    /// The duplicate-entry error a conflicting row would raise, which names
    /// the key it collided on the way Go's `ErrDupEntry` does.
    pub fn duplicate_entry_error(
        &mut self,
        row: &[Datum],
        ctx: &impl tidb_expr::Columns,
    ) -> Result<KvTableError, KvTableError> {
        let zone = ctx.time_zone();
        let physical_id = self.record_physical_id(row, ctx)?;
        let clustered = self.pk_handle_offset.is_some() || !self.common_handle_offsets.is_empty();
        if clustered {
            let handle = self.handle_of_row(row, &zone, 0)?;
            if self.row_exists(&handle)? {
                return Ok(KvTableError::DuplicateEntry {
                    value: clustered_key_text(self, row),
                    key: self.qualified_key("PRIMARY"),
                });
            }
        }
        for index in self.indexes.as_ref().clone() {
            if !index.unique {
                continue;
            }
            let (key, distinct) =
                self.index_key(&index, row, &TableHandle::Int(0), physical_id, &zone)?;
            if distinct && self.store.get(&Key::from_bytes(key)).is_ok() {
                return Ok(KvTableError::DuplicateEntry {
                    value: duplicate_value_text(&self.index_values(&index, row)),
                    key: self.qualified_key(&index.name),
                });
            }
        }
        Err(KvTableError::Encode("no conflict to report".to_owned()))
    }

    /// Adds an index, whose entries every later write maintains.
    pub fn add_index(&mut self, index: KvIndex) {
        self.indexes_mut().push(index);
    }

    /// The next auto-increment value, which `SHOW TABLE STATUS` reports as
    /// `Auto_increment`. `None` when the table has no auto column at all,
    /// which is the NULL Go reports there.
    #[must_use]
    pub fn next_auto_increment(&self) -> Option<i64> {
        self.auto_increment_offset
            .map(|_| self.auto_id.next() as i64)
    }

    /// The table's indexes: every one of them, which is what index
    /// maintenance, `SHOW INDEX`, and `information_schema` read.
    #[must_use]
    pub fn indexes(&self) -> &[KvIndex] {
        &self.indexes
    }

    /// One index by name, for the metadata-only edits that change what an
    /// index is CALLED or whether the planner may see it, without touching a
    /// single entry it holds (`ALTER TABLE ... RENAME INDEX` / `ALTER INDEX
    /// ... {VISIBLE|INVISIBLE}`). Names match case-insensitively, as every
    /// other index lookup here and in Go does.
    pub fn index_mut_by_name(&mut self, name: &str) -> Option<&mut KvIndex> {
        self.indexes_mut()
            .iter_mut()
            .find(|index| index.name.eq_ignore_ascii_case(name))
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

    /// Marks the column at `offset` as the table's handle column, which Go
    /// records as `TableInfo.PKIsHandle`.
    pub fn set_pk_handle_offset(&mut self, offset: usize) {
        self.pk_handle_offset = Some(offset);
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
    /// [`crate::storage::TableStorage`], so the whole key set is readable
    /// directly.
    pub fn stored_keys(&mut self) -> Result<Vec<Vec<u8>>, KvTableError> {
        let mut iterator = self
            .store
            .iter(None, None)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        let mut keys = Vec::new();
        while iterator.valid() {
            keys.push(iterator.key().as_slice().to_vec());
            iterator
                .next()
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        }
        iterator.close();
        keys.sort_unstable();
        Ok(keys)
    }

    /// Inserts one row (a `Datum` per column, in schema order): encodes the
    /// record key from the next handle and the value through the v2 row format,
    /// exactly the bytes a TiKV-backed table would store.
    pub fn insert_row(
        &mut self,
        row: &[Datum],
        ctx: &impl tidb_expr::Columns,
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
        ctx: &impl tidb_expr::Columns,
    ) -> Result<TableHandle, KvTableError> {
        self.insert_row_in(row, row_id, shard, ctx, false)
    }

    /// [`Self::insert_row_with_row_id`] for the INSERT executor, which names
    /// the statement's duplicate-key mode. `lazy_dup_check` is Go
    /// `DupKeyCheckLazy` (`pkg/executor/insert.go`'s
    /// `optimizeDupKeyCheckForNormalInsert`, reached only by a NORMAL insert
    /// under a pessimistic transaction): existence is consulted in the
    /// statement's staged writes ONLY -- Go `GetLocal` -- and a miss stages
    /// the row presumed absent (`kv.SetPresumeKeyNotExists`), deferring the
    /// verdict to the commit's constraint check. The record key carries the
    /// mark; unique secondary entries keep their own eager check, matching
    /// nothing Go waives for them here.
    pub fn insert_row_with_row_id_checked(
        &mut self,
        row: &[Datum],
        row_id: Option<i64>,
        shard: i64,
        ctx: &impl tidb_expr::Columns,
        lazy_dup_check: bool,
    ) -> Result<TableHandle, KvTableError> {
        self.insert_row_in(row, row_id, shard, ctx, lazy_dup_check)
    }

    fn insert_row_in(
        &mut self,
        row: &[Datum],
        row_id: Option<i64>,
        shard: i64,
        ctx: &impl tidb_expr::Columns,
        lazy_dup_check: bool,
    ) -> Result<TableHandle, KvTableError> {
        let zone = ctx.time_zone();
        // The generated columns are recomputed HERE, at the one place every
        // row reaches, so the stored bytes, the row handle and the index
        // entries all see the same computed values and no caller can write a
        // row whose generated columns were never filled in.
        let mut owned;
        let row = if self.columns.iter().any(|c| c.generated.is_some()) {
            owned = row.to_vec();
            owned.resize(self.columns.len(), Datum::Null);
            self.materialize_generated(&mut owned, ctx)?;
            owned.as_slice()
        } else {
            row
        };
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
                self.auto_id
                    .rebase(value as u64)
                    .map_err(|error| KvTableError::Storage(error.0))?;
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
        // Go `AddRecord`'s duplicate check, in both of its modes. In place it
        // reads the whole transaction (staged writes, then snapshot); lazily
        // it reads the STAGED WRITES ONLY (`GetLocal`) and a miss marks the
        // key presumed not exists so prewrite still rejects a real duplicate.
        // A locally staged tombstone is neither: the row was deleted inside
        // this transaction, and the reinsert overwrites it without any
        // presumption -- Go's own `len(v) == 0` arm.
        let duplicated = if lazy_dup_check {
            match self.store.get_local(&key) {
                Ok(value) => !value.is_empty(),
                Err(StorageError::NotFound) => {
                    self.store.mark_presume_key_not_exists(&key);
                    false
                }
                Err(error) => return Err(KvTableError::Storage(format!("{error:?}"))),
            }
        } else {
            self.row_exists(&handle)?
        };
        if duplicated {
            return Err(KvTableError::DuplicateEntry {
                value: if clustered {
                    clustered_key_text(self, row)
                } else {
                    match &handle {
                        TableHandle::Int(value) => value.to_string(),
                        TableHandle::Common(_) => {
                            unreachable!("a heap table has an integer handle")
                        }
                    }
                },
                key: self.qualified_key("PRIMARY"),
            });
        }
        // Go writes the row first, then its index entries; a duplicate on a
        // unique index aborts the statement.
        self.write_index_entries(row, &handle, physical_id, &zone)?;
        self.store
            .set(key, value)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        self.dirty_content = true;
        Ok(handle)
    }

    /// The table's indexes, for `ADMIN CHECK` (see [`crate::admin_check`]).
    ///
    /// The field itself stays private because every other reader reaches an
    /// index through a scan; the consistency check is the one caller that
    /// needs the whole list as metadata.
    #[must_use]
    pub fn index_list_for_check(&self) -> Vec<KvIndex> {
        self.indexes.as_ref().clone()
    }

    /// [`KvTable::index_key`] for [`crate::admin_check`]: the entry key one
    /// row's values encode to under `index`.
    pub fn index_key_for_check(
        &mut self,
        index: &KvIndex,
        row: &[Datum],
        handle: &TableHandle,
        zone: &SessionTimeZone,
    ) -> Result<(Vec<u8>, bool), KvTableError> {
        let physical_id = self.stored_physical_id(handle)?.unwrap_or(self.table_id);
        self.index_key(index, row, handle, physical_id, zone)
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
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))
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

    /// The point row under one already-routed physical partition id.
    ///
    /// Go's partitioned `BatchPointGetExec` carries one physical id beside
    /// each handle. This is the matching storage boundary: one exact row-key
    /// lookup, without probing the table's remaining partitions.
    pub(crate) fn get_row_by_handle_in_physical_id_with_context(
        &mut self,
        handle: &TableHandle,
        physical_id: i64,
        context: &RowDecodeContext,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        let key = Key::from_bytes(encode_row_key_with_handle(
            physical_id,
            &handle.record_handle(),
        ));
        let entry = match self.store.get(&key) {
            Ok(entry) => entry,
            Err(StorageError::NotFound) => return Ok(None),
            Err(error) => return Err(KvTableError::Storage(format!("{error:?}"))),
        };
        Ok(Some(self.decode_row_entry(handle, &entry, context)?))
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

    /// Legacy zone-only point read retained for unmigrated DML/FK callers.
    /// Origin defaults use the exact former `DEFAULT_STATEMENT_FLAGS` behavior.
    pub fn get_row_by_handle(
        &mut self,
        handle: &TableHandle,
        zone: &SessionTimeZone,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        self.read_row(handle, &RowDecodeContext::legacy_default(zone))
    }

    /// The row stored under `handle`, decoded, or `None` when absent.
    fn read_row(
        &mut self,
        handle: &TableHandle,
        context: &RowDecodeContext,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        let Some((_, entry)) = self.stored_record(handle)? else {
            return Ok(None);
        };
        Ok(Some(self.decode_row_entry(handle, &entry, context)?))
    }

    fn decode_row_entry(
        &self,
        handle: &TableHandle,
        entry: &[u8],
        context: &RowDecodeContext,
    ) -> Result<Vec<Datum>, KvTableError> {
        // Row V2 already carries a positional column directory. For ordinary
        // stored columns, decode it directly instead of allocating the legacy
        // column-id map and rebuilding a full RowDecoder for every point read.
        // Generated columns still use RowDecoder so their expressions retain
        // the statement context and evaluation order.
        if tidb_codec::is_new_format(entry)
            && !self.columns.iter().any(|column| column.generated.is_some())
        {
            let columns = self
                .columns
                .iter()
                .enumerate()
                .map(|(offset, column)| tidb_codec::ColumnInfo {
                    id: column.id,
                    is_pk_handle: self.pk_handle_offset == Some(offset),
                    virtual_generated: false,
                    field_type: column.field_type.clone(),
                })
                .collect::<Vec<_>>();
            let handle_column_ids = self
                .pk_handle_offset
                .into_iter()
                .chain(self.common_handle_offsets.iter().copied())
                .filter_map(|offset| self.columns.get(offset).map(|column| column.id))
                .collect::<Vec<_>>();
            let codec_handle = match handle {
                TableHandle::Int(value) => tidb_codec::Handle::Int(*value),
                TableHandle::Common(encoded) => {
                    let common = CommonHandle::new(encoded.clone())
                        .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                    let parts = (0..self.common_handle_offsets.len())
                        .filter_map(|index| common.encoded_column(index).map(<[u8]>::to_vec))
                        .collect();
                    tidb_codec::Handle::Common(parts)
                }
            };
            let defaults = self
                .columns
                .iter()
                .map(|column| {
                    column
                        .origin_default_value(context.origin_default_flags(), context.zone())
                        .map_err(|error| KvTableError::Decode(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            return tidb_codec::decode_row_to_datums(
                entry,
                &columns,
                &tidb_codec::DecodeRowOptions {
                    handle_column_ids: &handle_column_ids,
                    handle: Some(&codec_handle),
                    defaults: Some(&defaults),
                    timezone: Some(context.zone()),
                    ..tidb_codec::DecodeRowOptions::default()
                },
            )
            .map(|row| row.values)
            .map_err(|error| KvTableError::Decode(format!("{error:?}")));
        }
        let decoder = RowDecoder::for_table_read(
            self.columns.as_ref().clone(),
            self.pk_handle_offset,
            self.common_handle_offsets.clone(),
            None,
            self.use_new_collation,
            context.clone(),
        )?;
        Ok(decoder.decode_and_eval(handle, entry)?.into_parts().0)
    }

    /// Whether a row is already stored under `handle`, in ANY partition.
    ///
    /// Asking across partitions rather than within one is not a widening: a
    /// clustered key includes every partitioning column (Go's 8264/1503
    /// checks, ported in [`crate::ddl::table_partition`]), so two rows
    /// sharing a handle route to the same partition anyway.
    fn row_exists(&mut self, handle: &TableHandle) -> Result<bool, KvTableError> {
        Ok(self.stored_record_key(handle)?.is_some())
    }

    /// Replaces the row stored under `handle` (Go's `UPDATE` writes the new
    /// row back under the same record key when the handle column did not
    /// change).
    pub fn update_row_with_context(
        &mut self,
        handle: &TableHandle,
        row: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<(), KvTableError> {
        self.update_row_in(handle, None, row, ctx, &RowDecodeContext::for_write(ctx))
    }

    /// Replaces a row when the caller already holds the selected row. Passing
    /// it avoids a second point read to discover the old record key and is
    /// equivalent to Go's update executor retaining its input chunk.
    pub fn update_row_with_old(
        &mut self,
        handle: &TableHandle,
        old_row: Option<&[Datum]>,
        row: &[Datum],
        ctx: &impl tidb_expr::Columns,
    ) -> Result<(), KvTableError> {
        let zone = ctx.time_zone();
        self.update_row_in(
            handle,
            old_row,
            row,
            ctx,
            &RowDecodeContext::legacy_default(&zone),
        )
    }

    /// Legacy row update retained for unmigrated DML/FK callers. Reading the
    /// old row uses the exact former `DEFAULT_STATEMENT_FLAGS` behavior.
    pub fn update_row(
        &mut self,
        handle: &TableHandle,
        row: &[Datum],
        ctx: &impl tidb_expr::Columns,
    ) -> Result<(), KvTableError> {
        let zone = ctx.time_zone();
        self.update_row_in(
            handle,
            None,
            row,
            ctx,
            &RowDecodeContext::legacy_default(&zone),
        )
    }

    fn update_row_in(
        &mut self,
        handle: &TableHandle,
        old_row: Option<&[Datum]>,
        row: &[Datum],
        ctx: &impl tidb_expr::Columns,
        decode_context: &RowDecodeContext,
    ) -> Result<(), KvTableError> {
        let zone = ctx.time_zone();
        // Recomputed, never carried over: an UPDATE that changes a dependency
        // must not leave a STORED generated column holding the value computed
        // from the old dependency.
        let mut owned;
        let row = if self.columns.iter().any(|c| c.generated.is_some()) {
            owned = row.to_vec();
            owned.resize(self.columns.len(), Datum::Null);
            self.materialize_generated(&mut owned, ctx)?;
            owned.as_slice()
        } else {
            row
        };
        // Go `updateRecord`: assigning to the AUTO_INCREMENT column REBASES the
        // allocator, exactly as an explicit value on INSERT does, so later rows
        // land past the value the UPDATE named. Without this an `UPDATE t SET
        // id = 300` left the counter where it was and the next allocations
        // walked back over ids the table had moved AHEAD of.
        //
        // Go guards this with "the column's value changed"; the guard is
        // redundant because a rebase only ever moves the counter UP and the
        // counter is already at or past every id the table stores (each one was
        // either allocated from it or rebased it on the way in). Rebasing
        // unconditionally therefore leaves no branch to get wrong -- and it is
        // why `UPDATE ... SET id = 0` correctly changes nothing.
        //
        // The value travels as its 64-bit PATTERN, since Go's
        // `getAutoRecordID` hands `Rebase` an `int64` that `rebase4Unsigned`
        // reads back as `uint64` for an unsigned column.
        if let Some(offset) = self.auto_increment_offset {
            let assigned = match row.get(offset) {
                Some(Datum::Int(value)) => *value as u64,
                Some(Datum::UInt(value)) => *value,
                _ => 0,
            };
            self.auto_id
                .rebase(assigned)
                .map_err(|error| KvTableError::Storage(error.0))?;
        }
        self.rebase_auto_random_from_row(row)?;
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
        let new_handle = if clustered {
            self.handle_of_row(row, &zone, 0)?
        } else {
            handle.clone()
        };
        // Moving onto an occupied handle is a primary-key duplicate, reported
        // exactly as `INSERT` reports one: `Duplicate entry '2' for key
        // 't.PRIMARY'`. Checked before anything is written, so the rejected
        // statement leaves the table untouched.
        if new_handle != *handle && self.row_exists(&new_handle)? {
            return Err(KvTableError::DuplicateEntry {
                value: clustered_key_text(self, row),
                key: self.qualified_key("PRIMARY"),
            });
        }
        let new_physical_id = self.record_physical_id(row, ctx)?;
        let old_key = if new_handle == *handle {
            if let Some(old) = old_row {
                let old_physical_id = self.record_physical_id(old, ctx)?;
                if old_physical_id == new_physical_id {
                    Some(Key::from_bytes(encode_row_key_with_handle(
                        old_physical_id,
                        &handle.record_handle(),
                    )))
                } else {
                    self.stored_record_key(handle)?
                }
            } else {
                self.stored_record_key(handle)?
            }
        } else {
            self.stored_record_key(handle)?
        };
        let old_physical_id = old_key.as_ref().map_or(self.table_id, |key| {
            tidb_codec::decode_table_id(key.as_bytes())
        });
        // Go removes the old index entries and writes the new ones. They point
        // AT the handle, so a moved row needs them rewritten even when the
        // indexed values did not change.
        if !self.indexes.is_empty() {
            let owned_old;
            let old = match old_row {
                Some(old) => Some(old),
                None => {
                    owned_old = self.read_row(handle, decode_context)?;
                    owned_old.as_deref()
                }
            };
            if let Some(old) = &old {
                self.delete_index_entries(old, handle, old_physical_id, &zone)?;
            }
            if let Err(error) = self.write_index_entries(row, &new_handle, new_physical_id, &zone) {
                // Restore the entries the failed update removed, so a rejected
                // statement leaves the index as it found it.
                if let Some(old) = &old {
                    self.write_index_entries(old, handle, old_physical_id, &zone)?;
                }
                return Err(error);
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
        if let Some(old_key) = old_key.filter(|old_key| *old_key != key) {
            self.store
                .delete(old_key)
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        }
        self.store
            .set(key, value)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        self.dirty_content = true;
        Ok(())
    }

    /// Removes the row stored under `handle`.
    pub fn delete_row_with_context(
        &mut self,
        handle: &TableHandle,
        ctx: &crate::StmtContext,
    ) -> Result<(), KvTableError> {
        let zone = ctx.session_zone();
        self.delete_row_in(handle, &zone, &RowDecodeContext::for_write(ctx))
    }

    /// Legacy zone-only row delete retained for unmigrated DML/FK callers.
    /// Reading the old row uses the exact former `DEFAULT_STATEMENT_FLAGS` behavior.
    pub fn delete_row(
        &mut self,
        handle: &TableHandle,
        zone: &SessionTimeZone,
    ) -> Result<(), KvTableError> {
        self.delete_row_in(handle, zone, &RowDecodeContext::legacy_default(zone))
    }

    /// [`KvTable::delete_row`] for a row THIS STATEMENT already fetched.
    ///
    /// Go `RemoveRecord` derives the record key from the row's own handle and
    /// removes index entries from the in-memory old row (`tables.DeleteRecord`)
    /// -- it never re-reads storage for either. The storage-backed form above
    /// reads because it was called WITHOUT the old row; the DELETE executor
    /// that just fetched its victims hands them here so one row costs one
    /// read.
    pub fn delete_row_with_old(
        &mut self,
        handle: &TableHandle,
        old_row: &[Datum],
        ctx: &impl tidb_expr::Columns,
    ) -> Result<(), KvTableError> {
        let zone = ctx.time_zone();
        let old_physical_id = self.record_physical_id(old_row, ctx)?;
        let key = Key::from_bytes(encode_row_key_with_handle(
            old_physical_id,
            &handle.record_handle(),
        ));
        if !self.indexes.is_empty() {
            self.delete_index_entries(old_row, handle, old_physical_id, &zone)?;
        }
        self.store
            .delete(key)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        self.dirty_content = true;
        Ok(())
    }

    fn delete_row_in(
        &mut self,
        handle: &TableHandle,
        zone: &SessionTimeZone,
        decode_context: &RowDecodeContext,
    ) -> Result<(), KvTableError> {
        let Some(key) = self.stored_record_key(handle)? else {
            return Ok(());
        };
        let physical_id = tidb_codec::decode_table_id(key.as_bytes());
        if !self.indexes.is_empty() {
            if let Some(row) = self.read_row(handle, decode_context)? {
                self.delete_index_entries(&row, handle, physical_id, zone)?;
            }
        }
        self.store
            .delete(key)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        self.dirty_content = true;
        Ok(())
    }
}

/// The text a clustered-key duplicate reports: the key columns joined by `-`,
/// as Go's `ErrKeyExists` formats them. Every part goes through the one
/// [`datum_text`] renderer, so a composite key can never disagree with a
/// single-column one about how a value prints.
fn clustered_key_text(table: &KvTable, row: &[Datum]) -> String {
    table
        .handle_column_offsets()
        .iter()
        .map(|offset| row.get(*offset).map_or_else(String::new, datum_text))
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
fn convert_failure(column: &str, target: &FieldType, value: &Datum) -> KvTableError {
    let text = datum_text(value);
    let source_is_string = matches!(value, Datum::Bytes(_) | Datum::String(_));
    let numeric_target = matches!(
        target.eval_type(),
        tidb_datatype::EvalType::Int
            | tidb_datatype::EvalType::Real
            | tidb_datatype::EvalType::Decimal
    );
    if source_is_string && numeric_target {
        return KvTableError::TruncatedIncorrectValue {
            kind: "DOUBLE",
            value: text,
        };
    }
    KvTableError::DataTruncatedValue {
        column: column.to_owned(),
        value: text,
    }
}

/// A datum as MySQL prints it inside an error message.
pub(in crate::kv_table) fn datum_text(value: &Datum) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{Executor, ExecutorMeta};
    use tidb_codec::table_key::{get_table_handle_key_range, RecordHandle};
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::schema::Schema;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn varstr() -> FieldType {
        FieldType::new(FieldTypeCode::VarString)
    }

    fn test_table() -> KvTable {
        KvTable::new(
            42,
            vec![
                KvColumn {
                    name: "a".to_owned(),
                    id: 1,
                    field_type: long(),
                    column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                    default_value: None,
                    // A column present at CREATE TABLE has no pre-existing rows.
                    origin_default: None,
                    comment: String::new(),
                    generated: None,
                },
                KvColumn {
                    name: "s".to_owned(),
                    id: 2,
                    field_type: varstr(),
                    column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                    default_value: None,
                    // A column present at CREATE TABLE has no pre-existing rows.
                    origin_default: None,
                    comment: String::new(),
                    generated: None,
                },
            ],
        )
    }

    #[test]
    fn test_options() {
        // Direct port of pkg/table/table_test.go::TestOptions. Rust builders
        // replace Go's variadic functional options, but the zero values and
        // every conversion retain the same observable fields.
        let add = AddRecordOptions::new();
        assert!(add.context().is_none());
        assert!(!add.is_update());
        assert!(!add.generate_record_id());
        assert_eq!(add.reserve_auto_id(), 0);
        let create = add.create_index_options();
        assert!(create.context().is_none());
        assert!(!create.ignore_assertion());
        assert!(!create.from_backfill());

        let context = std::rc::Rc::new(crate::StmtContext::default());
        let add = AddRecordOptions::new()
            .with_context(context.clone())
            .for_update()
            .with_reserve_auto_id_hint(12);
        assert!(add.context().is_some());
        assert!(add.is_update());
        assert!(add.generate_record_id());
        assert_eq!(add.reserve_auto_id(), 12);
        assert!(std::rc::Rc::ptr_eq(
            add.create_index_options().context().unwrap(),
            &context
        ));

        let update = UpdateRecordOptions::new();
        assert!(update.context().is_none());
        assert!(!update.skip_write_untouched_indices());
        let generated = update.add_record_options();
        assert!(generated.is_update());
        assert!(generated.generate_record_id());
        let kept = update.add_record_options_keep_record_id();
        assert!(kept.is_update());
        assert!(!kept.generate_record_id());
        assert!(update.create_index_options().context().is_none());

        let update = UpdateRecordOptions::new().with_context(context.clone());
        assert!(std::rc::Rc::ptr_eq(update.context().unwrap(), &context));
        assert!(std::rc::Rc::ptr_eq(
            update.add_record_options().context().unwrap(),
            &context
        ));
        assert!(std::rc::Rc::ptr_eq(
            update
                .add_record_options_keep_record_id()
                .context()
                .unwrap(),
            &context
        ));
        assert!(std::rc::Rc::ptr_eq(
            update.create_index_options().context().unwrap(),
            &context
        ));

        let create = CreateIndexOptions::new();
        assert!(create.context().is_none());
        assert!(!create.ignore_assertion());
        assert!(!create.from_backfill());
        let create = create
            .with_context(context.clone())
            .with_ignore_assertion()
            .with_backfill_source();
        assert!(std::rc::Rc::ptr_eq(create.context().unwrap(), &context));
        assert!(create.ignore_assertion());
        assert!(create.from_backfill());

        // The source context remains live through each of those same handles.
        context.append_warning_parts(1105, "source context marker");
        assert_eq!(create.context().unwrap().warning_count(), 1);
        assert_eq!(
            add.create_index_options()
                .context()
                .unwrap()
                .warning_count(),
            1
        );
        assert_eq!(
            update
                .add_record_options()
                .context()
                .unwrap()
                .warning_count(),
            1
        );
    }

    #[test]
    fn test_dedup_index_columns_4_test() {
        // Direct port of
        // pkg/table/tables/index_test.go::TestDedupIndexColumns4Test.
        let all_columns = (0..100)
            .map(|offset| {
                tidb_model::GoShared::new(tidb_model::IndexColumn {
                    name: tidb_ast::CiString::new(format!("c{offset}")),
                    offset,
                    ..Default::default()
                })
            })
            .collect::<Vec<_>>();
        let mut columns = all_columns.clone();
        for index in 0..100 {
            columns.push(all_columns[(index * 37) % all_columns.len()].clone());
        }

        let result = dedup_index_columns(columns);
        assert_eq!(result.len(), all_columns.len());
        for (actual, expected) in result.iter().zip(&all_columns) {
            assert!(actual.ptr_eq(expected));
        }
    }

    #[test]
    fn test_extract_columns_from_condition() {
        // Direct port of
        // pkg/table/tables/index_test.go::TestExtractColumnsFromCondition.
        let column = |name: &str, offset, generated: &str, stored| tidb_model::ColumnInfo {
            name: tidb_ast::CiString::new(name),
            offset,
            state: tidb_model::SchemaState::PUBLIC,
            generated_expr_string: generated.to_owned(),
            generated_stored: stored,
            ..Default::default()
        };
        let table = tidb_model::TableInfo {
            name: tidb_ast::CiString::new("test_table"),
            columns: vec![
                column("c1", 0, "", false),
                column("c2", 1, "", false),
                column("c3", 2, "c1 + c2", false),
                column("c4", 3, "c1 + c2", true),
            ]
            .into(),
            ..Default::default()
        };
        let cases = [
            (
                "c1 AND c2",
                vec![("c1", 0), ("c2", 1)],
                vec![("c1", 0), ("c2", 1)],
            ),
            ("c1 > 100", vec![("c1", 0)], vec![("c1", 0)]),
            (
                "c3 > 50",
                vec![("c3", 2)],
                vec![("c1", 0), ("c2", 1), ("c3", 2)],
            ),
            ("c4 > 50", vec![("c4", 3)], vec![("c4", 3)]),
        ];

        for (condition, expected, expected_with_virtual) in cases {
            let index = tidb_model::IndexInfo {
                condition_expr_string: condition.to_owned(),
                ..Default::default()
            };
            for (include_virtual, expected) in [
                (false, expected.as_slice()),
                (true, expected_with_virtual.as_slice()),
            ] {
                let actual =
                    extract_columns_from_index_condition(&index, &table, include_virtual).unwrap();
                let actual = actual
                    .iter()
                    .map(|column| {
                        let column = column.read();
                        (column.name.original().to_owned(), column.offset)
                    })
                    .collect::<Vec<_>>();
                let expected = expected
                    .iter()
                    .map(|(name, offset)| ((*name).to_owned(), *offset))
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected, "condition {condition}");
            }
        }
    }

    #[test]
    fn test_unique_index_multiple_null_entries() {
        // Direct port of
        // pkg/table/tables/tables_test.go::TestUniqueIndexMultipleNullEntries.
        // NULL-bearing UNIQUE keys are non-distinct and carry the row handle,
        // so two NULLs must create two entries without a duplicate error.
        let mut table = test_table();
        table.set_pk_handle_offset(0);
        table.add_index(KvIndex {
            id: 1,
            name: "s".to_owned(),
            comment: String::new(),
            unique: true,
            column_offsets: vec![1],
            prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH],
            visible: true,
            global: false,
        });

        table
            .insert_row(&[Datum::Int(1), Datum::Null], &tidb_expr::NoColumns)
            .unwrap();
        table
            .insert_row(&[Datum::Int(2), Datum::Null], &tidb_expr::NoColumns)
            .unwrap();
        assert_eq!(
            table
                .scan_rows_with_context(&RowDecodeContext::for_test_query_utc())
                .unwrap()
                .len(),
            2
        );
        assert_eq!(table.index_entries_for_check(1).unwrap().len(), 2);
    }

    #[test]
    fn test_table_from_meta_with_collate_uses_fixed_mode() {
        // Direct port of pkg/table/tables/tables_test.go::
        // TestTableFromMetaWithCollateUsesFixedMode. The table captures the
        // supplied persisted mode instead of consulting process-global state.
        for use_new_collation in [false, true] {
            let table = KvTable::with_storage_and_collation(
                1,
                vec![KvColumn {
                    name: "a".to_owned(),
                    id: 1,
                    field_type: varstr(),
                    column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                    default_value: None,
                    origin_default: None,
                    comment: String::new(),
                    generated: None,
                }],
                Box::new(MemTableStorage::new()),
                use_new_collation,
            );
            assert_eq!(table.use_new_collation(), use_new_collation);
        }
    }

    #[test]
    fn test_iter_records() {
        // Direct port of pkg/table/tables/tables_test.go::TestIterRecords.
        // Record iteration crosses the signed-handle ordering boundary and
        // preserves NULL in a non-handle column.
        let mut table = KvTable::new(
            42,
            ["a", "b"]
                .into_iter()
                .enumerate()
                .map(|(offset, name)| KvColumn {
                    name: name.to_owned(),
                    id: offset as i64 + 1,
                    field_type: long(),
                    column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                    default_value: None,
                    origin_default: None,
                    comment: String::new(),
                    generated: None,
                })
                .collect(),
        );
        table.set_pk_handle_offset(0);
        table
            .insert_row(&[Datum::Int(-1), Datum::Int(2)], &tidb_expr::NoColumns)
            .unwrap();
        table
            .insert_row(&[Datum::Int(2), Datum::Null], &tidb_expr::NoColumns)
            .unwrap();

        let rows = table
            .scan_rows_with_handles_with_context(&RowDecodeContext::for_test_query_utc())
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            (TableHandle::Int(-1), vec![Datum::Int(-1), Datum::Int(2)])
        );
        assert_eq!(
            rows[1],
            (TableHandle::Int(2), vec![Datum::Int(2), Datum::Null])
        );
    }

    /// The scan bound must cover the whole table and nothing beyond it: the
    /// codec's handle range is inclusive at the top while the iterator's upper
    /// bound is exclusive, so the largest handle must still be returned and a
    /// neighbouring table's rows must not be.
    /// The handle a scan reports must be the handle the codec wrote, so an
    /// UPDATE/DELETE addresses the row it read.
    #[test]
    fn scan_reports_the_handles_the_key_codec_wrote() {
        let mut t = test_table();
        let mut handles = Vec::new();
        for i in 0..3 {
            handles.push(
                t.insert_row(
                    &[Datum::Int(i * 10), Datum::Bytes(b"x".to_vec())],
                    &tidb_expr::NoColumns,
                )
                .unwrap(),
            );
        }
        let scanned: Vec<TableHandle> = t
            .scan_rows_with_handles_with_context(&RowDecodeContext::for_test_query_utc())
            .unwrap()
            .into_iter()
            .map(|(handle, _)| handle)
            .collect();
        assert_eq!(scanned, handles);

        // A row written under an explicit handle round-trips too.
        t.update_row_with_context(
            &handles[1],
            &[Datum::Int(99), Datum::Bytes(b"y".to_vec())],
            &crate::StmtContext::for_dml(false, false, false),
        )
        .unwrap();
        let rows = t
            .scan_rows_with_handles_with_context(&RowDecodeContext::for_test_query_utc())
            .unwrap();
        assert_eq!(rows.len(), 3, "update replaced in place, it did not append");
        assert_eq!(rows[1].0, handles[1]);
        assert_eq!(rows[1].1[0], Datum::Int(99));

        t.delete_row_with_context(
            &handles[0],
            &crate::StmtContext::for_dml(false, false, false),
        )
        .unwrap();
        let after: Vec<TableHandle> = t
            .scan_rows_with_handles_with_context(&RowDecodeContext::for_test_query_utc())
            .unwrap()
            .into_iter()
            .map(|(handle, _)| handle)
            .collect();
        assert_eq!(after, vec![handles[1].clone(), handles[2].clone()]);
    }

    #[test]
    fn scan_covers_the_whole_table_and_stops_at_its_range() {
        let mut t = test_table();
        for i in 0..3 {
            t.insert_row(
                &[Datum::Int(i), Datum::Bytes(b"x".to_vec())],
                &tidb_expr::NoColumns,
            )
            .unwrap();
        }
        // A row of the next table id, written into the same storage layout.
        let mut neighbour = KvTable::new(t.table_id + 1, t.columns.to_vec());
        neighbour
            .insert_row(
                &[Datum::Int(99), Datum::Bytes(b"y".to_vec())],
                &tidb_expr::NoColumns,
            )
            .unwrap();

        let rows = t
            .scan_rows_with_context(&RowDecodeContext::for_test_query_utc())
            .unwrap();
        assert_eq!(
            rows.len(),
            3,
            "every handle including the largest is scanned"
        );
        assert_eq!(rows[2][0], Datum::Int(2));
        assert_eq!(
            neighbour
                .scan_rows_with_context(&RowDecodeContext::for_test_query_utc())
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn insert_encodes_real_bytes_and_scan_decodes() {
        let mut t = test_table();
        let mut s1 = Datum::Null;
        s1.set_bytes(b"hello".to_vec());
        t.insert_row(&[Datum::Int(7), s1.clone()], &tidb_expr::NoColumns)
            .unwrap();
        t.insert_row(&[Datum::Int(8), Datum::Null], &tidb_expr::NoColumns)
            .unwrap();
        assert_eq!(t.len(), 2);

        let rows = t
            .scan_rows_with_context(&RowDecodeContext::for_test_query_utc())
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Datum::Int(7));
        assert_eq!(rows[1][0], Datum::Int(8));
        assert_eq!(rows[1][1], Datum::Null);
        // The string round-trips through the v2 row format.
        match &rows[0][1] {
            Datum::Bytes(b) => assert_eq!(b.as_slice(), b"hello"),
            Datum::String(s) => assert_eq!(s.bytes(), b"hello"),
            other => panic!("unexpected decoded string datum {other:?}"),
        }
    }

    #[test]
    fn table_scan_exec_emits_chunks() {
        let mut t = test_table();
        let mut s = Datum::Null;
        s.set_bytes(b"x".to_vec());
        t.insert_row(&[Datum::Int(1), s], &tidb_expr::NoColumns)
            .unwrap();

        let mut out_cols = Vec::new();
        for (i, ft) in [long(), varstr()].into_iter().enumerate() {
            let mut c = Column::new((i + 1) as i64, ft);
            c.index = i as i64;
            out_cols.push(c);
        }
        let mut scan = TableScanExec::new_with_context(
            ExecutorMeta::new(Schema::new(out_cols), 0, 4, 1024),
            t,
            RowDecodeContext::for_test_query_utc(),
            crate::remote_scan::PushdownStatementContext::default(),
        );
        scan.open().unwrap();
        let mut req = scan.new_chunk();
        scan.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 1);
        assert_eq!(req.get_row(0).get_int64(0), 1);
        assert_eq!(req.get_row(0).get_bytes(1), b"x");
        // EOF afterwards.
        scan.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 0);
    }

    #[test]
    fn record_keys_are_the_real_format() {
        // t{tid}_r + memcomparable handle: 19 bytes, 't' prefix.
        let key = encode_row_key_with_handle(42, &RecordHandle::Int(1));
        assert_eq!(key[0], b't');
        assert!(key.len() > 10);
        // Keys sort by handle within the table range.
        let k2 = encode_row_key_with_handle(42, &RecordHandle::Int(2));
        assert!(key < k2);
        let (low, high) = get_table_handle_key_range(42);
        assert!(low < key && key < high);
    }

    /// A clustered common handle whose codec encoding is SHORTER than nine
    /// bytes is zero-padded by the record-key codec (Go `kv.NewCommonHandle`,
    /// `tablecodec.EncodeRowKeyWithHandle`), so a write that files its index
    /// entries under the RAW encoding files them under a key the delete path
    /// -- which reads its handle back out of the record key -- can never
    /// name. `DECIMAL` is the shortest such encoding this tier can build:
    /// `5` is four bytes.
    #[test]
    fn a_short_common_handle_indexes_under_the_key_the_delete_path_computes() {
        let mut decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
        decimal_type.set_flen(4);
        decimal_type.set_decimal(0);
        let mut t = KvTable::new(
            77,
            vec![
                KvColumn {
                    name: "pk".to_owned(),
                    id: 1,
                    field_type: decimal_type,
                    column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                    default_value: None,
                    origin_default: None,
                    comment: String::new(),
                    generated: None,
                },
                KvColumn {
                    name: "v".to_owned(),
                    id: 2,
                    field_type: long(),
                    column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                    default_value: None,
                    origin_default: None,
                    comment: String::new(),
                    generated: None,
                },
            ],
        );
        t.set_common_handle_offsets(vec![0]);
        t.add_index(KvIndex {
            id: 1,
            name: "idx".to_owned(),
            comment: String::new(),
            unique: false,
            column_offsets: vec![1],
            prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH],
            visible: true,
            global: false,
        });
        let written = t
            .insert_row(
                &[
                    Datum::Decimal(tidb_datatype::Decimal::from_int(5)),
                    Datum::Int(42),
                ],
                &tidb_expr::NoColumns,
            )
            .unwrap();
        // The handle a WRITE derives from the row and the handle a READ
        // derives from the record key must be the same handle, or every
        // index entry the write filed is unreachable.
        let scanned = t
            .scan_rows_with_handles_with_context(&RowDecodeContext::for_test_query_utc())
            .unwrap();
        assert_eq!(scanned.len(), 1);
        assert_eq!(scanned[0].0, written, "the write and the read disagree");

        t.delete_row_with_context(
            &scanned[0].0,
            &crate::StmtContext::for_dml(false, false, false),
        )
        .unwrap();
        assert_eq!(
            t.stored_keys().unwrap(),
            Vec::<Vec<u8>>::new(),
            "the delete left orphaned index entries behind"
        );
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
fn null_timestamp_substitute(
    field_type: &tidb_datatype::FieldType,
    ctx: &crate::StmtContext,
) -> Option<Datum> {
    if field_type.code() != tidb_datatype::FieldTypeCode::Timestamp
        || field_type.flags() & NOT_NULL_FLAG == 0
    {
        return None;
    }
    let now = crate::column_default::on_update_current_timestamp(field_type).ok()?;
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
