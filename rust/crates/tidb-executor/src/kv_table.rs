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
mod index_entries;
mod table_meta;
mod table_scan;

pub use auto_id::{
    advance, exceeds, AutoIdError, AutoIdStore, AutoIdStoreError, LocalAutoIdStore,
    DEFAULT_AUTO_ID_STEP,
};

/// One table's live auto-increment allocator, held by whoever owns the
/// table's lifetime rather than by the table itself.
///
/// Cloning shares the reserved range and the counter's home, which is what
/// lets a node keep one allocator per table across catalog rebuilds and hand
/// each rebuilt [`KvTable`] a clone of it -- Go's arrangement, where the
/// allocator sits on the domain's table cache and the `TableInfo` carries no
/// counter at all.
#[derive(Clone, Debug)]
pub struct TableAutoId(auto_id::AutoIdAllocator);

impl TableAutoId {
    /// An allocator over `store`, reserving `step` ids at a time.
    #[must_use]
    pub fn over(store: Arc<dyn AutoIdStore>, step: u64) -> Self {
        TableAutoId(auto_id::AutoIdAllocator::over(store, step))
    }

    /// Whether both handles drive the same allocator, and so the same
    /// reserved range.
    ///
    /// The registry that hands these out must answer the same table with
    /// clones of one allocator; this is how that invariant is asserted rather
    /// than assumed.
    #[must_use]
    pub fn same_allocator_as(&self, other: &TableAutoId) -> bool {
        self.0.shares_cache_with(&other.0)
    }
}
pub use table_meta::{
    FkAction, IndexRange, KvColumn, KvForeignKey, KvIndex, TableCharset, TableHandle,
};
pub use table_scan::{
    capture_decoded_column_ids, IndexRangeCursor, RemoteRowCursor, RowCursor, TableScanExec,
};

use crate::storage::{MemTableStorage, StorageError, TableStorage};
use auto_id::AutoIdAllocator;
use std::collections::BTreeMap;
use std::sync::Arc;
use table_meta::NOT_NULL_FLAG;
use table_scan::fill_handle_columns;
use tidb_codec::table_key::{encode_row_key_with_handle, get_table_handle_key_range};
use tidb_datatype::{
    integer_signed_upper_bound, integer_unsigned_upper_bound, type_str, Datum, FieldType,
    FieldTypeCode, SessionTimeZone,
};

use index_entries::{duplicate_value_text, index_entry_handle};
use tidb_tablecodec::{
    decode_table_row_to_map, encode_handle_in_unique_index_value, encode_table_row,
};
use tidb_txnkv::Key;

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
    pub columns: Vec<KvColumn>,
    /// How many of the TRAILING entries of `columns` are hidden (Go
    /// `ColumnInfo.Hidden`). Zero for every table with no expression index.
    hidden_columns: usize,
    /// The byte store, reached through the [`TableStorage`] seam (module
    /// doc), so a TiKV-backed backend replaces it without touching this file.
    store: Box<dyn TableStorage>,
    /// Go `TableInfo.PKIsHandle`: the offset of the single integer primary-key
    /// column whose value IS the row handle, when the table has one.
    pk_handle_offset: Option<usize>,
    /// The table's indexes (Go `TableInfo.Indices`).
    indexes: Vec<KvIndex>,
    /// The AUTO_INCREMENT column's offset, if the table has one.
    auto_increment_offset: Option<usize>,
    /// Go's auto-id allocator, shared across the copies a transaction stages
    /// so that a consumed id is never returned (see [`AutoIdAllocator`]).
    auto_id: AutoIdAllocator,
    /// Go `TableInfo.IsCommonHandle`: the clustered primary key's column
    /// offsets, whose encoding IS the row handle. Empty when the table has no
    /// clustered common handle.
    common_handle_offsets: Vec<usize>,
    /// Go `TableInfo.Charset`/`Collate`: the table's default character set and
    /// collation, which its unqualified string columns inherit.
    charset: TableCharset,
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
    /// Go `autoid.ErrAutoincReadFailed` (1467) raised while allocating the
    /// row's `_tidb_rowid`, which shares the AUTO_INCREMENT counter.
    AutoIdExhausted,
    /// A stored value failed to decode.
    Decode(String),
    /// The storage layer refused a read or write.
    Storage(String),
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

/// Where a row's `AUTO_INCREMENT` value came from.
///
/// Go splits the same three ways and treats them differently, so collapsing
/// any pair loses a rule. `Given` is the arm that `continue`s before the retry
/// cursor (`insert_common.go:894-903`) and never touches `lastInsertID`;
/// `Reused` is the consume loop (`insert_common.go:909-921`), which likewise
/// leaves `lastInsertID` alone so a replay cannot move the value a client
/// already read; only `Allocated` -- the `AllocBatchAutoIncrementValue` arm --
/// sets it (`insert_common.go:936-938`).
///
/// All three still get RECORDED for the next attempt, which is the one rule
/// that is uniform, and so the recording is unconditional at the call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoIncrement {
    /// The table has no `AUTO_INCREMENT` column; nothing was placed.
    Absent,
    /// The row supplied its own non-zero id, carried as its 64-bit pattern.
    /// The counter was rebased past it.
    Given(u64),
    /// The id came back from the losing attempt's list rather than the counter.
    Reused(i64),
    /// The id was drawn from the counter.
    Allocated(i64),
}

impl AutoIncrement {
    /// The id placed in the row, or `None` when there was no column to place
    /// one in. Every placed id is recorded for the next attempt, whichever arm
    /// produced it.
    #[must_use]
    pub fn placed(self) -> Option<u64> {
        match self {
            Self::Absent => None,
            Self::Given(id) => Some(id),
            Self::Reused(id) | Self::Allocated(id) => Some(id.max(0) as u64),
        }
    }
}

impl KvTable {
    /// Builds an empty table over the in-process backend.
    #[must_use]
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
        KvTable {
            table_id,
            name: String::new(),
            columns,
            hidden_columns: 0,
            store,
            pk_handle_offset: None,
            indexes: Vec::new(),
            common_handle_offsets: Vec::new(),
            auto_increment_offset: None,
            auto_id: AutoIdAllocator::new(),
            charset: TableCharset::default(),
            foreign_keys: Vec::new(),
            max_foreign_key_id: 0,
            partition: None,
            read_partitions: None,
        }
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
        let mut copy = KvTable::new(table_id, self.columns.clone());
        copy.hidden_columns = self.hidden_columns;
        copy.pk_handle_offset = self.pk_handle_offset;
        copy.indexes = self.indexes.clone();
        copy.common_handle_offsets = self.common_handle_offsets.clone();
        copy.auto_increment_offset = self.auto_increment_offset;
        copy.charset = self.charset;
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
        let types: Vec<FieldType> = self.columns.iter().map(|c| c.field_type.clone()).collect();
        partition
            .locate(row, &types, ctx)
            .map_err(|error| match error {
                crate::partition_routing::RoutingError::NoPartitionForValue(value) => {
                    KvTableError::NoPartitionForValue(value)
                }
                crate::partition_routing::RoutingError::Eval(error) => {
                    KvTableError::Decode(format!("{error:?}"))
                }
            })
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

    /// Sets the table's default character set and collation.
    pub fn set_charset(&mut self, charset: TableCharset) {
        self.charset = charset;
    }

    /// The table's default character set and collation.
    #[must_use]
    pub const fn charset(&self) -> TableCharset {
        self.charset
    }

    /// Marks the AUTO_INCREMENT column.
    ///
    /// The column's signedness travels to the allocator here, because it is
    /// what decides the domain every later id is compared and counted in
    /// (Go's `isUnsigned`, taken from the same `ColumnInfo` flag).
    pub fn set_auto_increment_offset(&mut self, offset: usize) {
        self.auto_increment_offset = Some(offset);
        let unsigned = self
            .columns
            .get(offset)
            .is_some_and(|column| column.field_type.is_unsigned());
        self.auto_id.set_unsigned(unsigned);
    }

    /// Unmarks the AUTO_INCREMENT column, which `ALTER TABLE ... MODIFY
    /// COLUMN` does when the new definition drops the option and
    /// `@@tidb_allow_remove_auto_inc` allows it. The allocator itself stays,
    /// as Go's does: nothing hands out ids while no column claims them, and
    /// re-adding the option is refused anyway.
    pub fn clear_auto_increment_offset(&mut self) {
        self.auto_increment_offset = None;
    }

    /// Gives the table an allocator built elsewhere and kept alive across
    /// catalog rebuilds.
    ///
    /// This is the seam the cluster tier uses to give the counter the meta-key
    /// home Go gives it, and the sharing is as load-bearing as the home. Go's
    /// allocator lives on the domain's table cache, not on the `TableInfo`, so
    /// it outlives every schema reload; this tier rebuilds its `KvTable`s
    /// whenever the schema version or the stats snapshot moves, and an
    /// allocator rebuilt with them would throw away its reserved range and
    /// reserve a fresh one -- leaving a visible hole in the ids on a table
    /// nothing was wrong with. Handing in a [`TableAutoId`] the node already
    /// holds is what keeps the range across the rebuild.
    ///
    /// The column's signedness is re-derived rather than carried, since it is
    /// a fact about this table's column and
    /// [`set_auto_increment_offset`](Self::set_auto_increment_offset) may run
    /// on either side of this call.
    pub fn set_auto_id(&mut self, shared: TableAutoId) {
        self.auto_id = shared.0;
        if let Some(offset) = self.auto_increment_offset {
            self.set_auto_increment_offset(offset);
        }
    }

    /// The AUTO_INCREMENT column's offset, if any.
    #[must_use]
    pub fn auto_increment_offset(&self) -> Option<usize> {
        self.auto_increment_offset
    }

    /// Go's `AUTO_INCREMENT=n` table option: the first id the table hands out.
    ///
    /// Go seeds the allocator so the next id is `n`, so `AUTO_INCREMENT=100`
    /// at CREATE makes the first row land on 100. On an existing table
    /// (`ALTER TABLE ... AUTO_INCREMENT=n`) it is a Rebase, which only ever
    /// moves the counter UP -- naming a smaller number leaves it alone.
    ///
    /// `next_id` carries the option's 64-bit PATTERN (Go's
    /// `int64(opt.UintValue)`), and `rebase_to_next` reads it in the auto
    /// column's own domain -- Go's `adjustNewBaseToNextGlobalID`, which is
    /// why `ALTER TABLE ... AUTO_INCREMENT = 18446744073709551615` really does
    /// move a `BIGINT UNSIGNED` counter to the top of its range while the same
    /// number on a signed column is a negative base and moves nothing.
    /// CREATE does NOT share that domain-aware read; see
    /// `auto_increment_option`'s caller.
    pub fn rebase_auto_increment(&mut self, next_id: i64) -> Result<(), AutoIdStoreError> {
        self.auto_id.rebase_to_next(next_id as u64)
    }

    /// Go `adjustAutoIncrementDatum`: fills the auto-increment column.
    ///
    /// An omitted, NULL or zero value takes the next allocated id; an explicit
    /// non-zero value is kept and REBASES the allocator so later rows exceed
    /// it. Returns the id allocated for this row, which the statement reports
    /// as `LAST_INSERT_ID` for the first such row. Fails with Go's
    /// `ErrAutoincReadFailed` (1467) once the column's domain is exhausted.
    ///
    /// The explicit value is carried as its 64-bit PATTERN rather than as an
    /// `i64`, so a `BIGINT UNSIGNED` id above `i64::MAX` rebases the allocator
    /// in its own domain. Reading it as a signed integer made it negative, the
    /// rebase then ignored it, and the allocator went on to hand out ids the
    /// table already held (captured: `INSERT ... VALUES
    /// (18446744073709551615)` leaves the next insert with no id at all,
    /// `[autoid:1467]`, never with a duplicate).
    ///
    /// An explicit `0` always allocates here. `NO_AUTO_VALUE_ON_ZERO`, under
    /// which Go keeps the zero, is REFUSED by the INSERT path rather than
    /// silently ignored (`StmtContext::auto_increment_zero_is_explicit`).
    ///
    /// `reuse` is Go's `RetryInfo` arm: a statement being RUN AGAIN after a
    /// write conflict is handed the id its losing attempt already assigned to
    /// this row, so `LAST_INSERT_ID()` and the stored row still agree. It
    /// enters HERE rather than at the call site so the reused id goes through
    /// the same domain check and the same column-typed placement an allocated
    /// one does -- a reused id that no longer fits its column must fail the way
    /// a fresh one would, not slip past untested.
    ///
    /// It is a CLOSURE because the cursor it reads must advance only on the
    /// rows that actually take an id from it. Go's batch arm
    /// (`lazyAdjustAutoIncrementDatum`, `insert_common.go:894-903`) hits
    /// `continue` on an explicitly-supplied id and so never reaches the
    /// consume loop below it (`insert_common.go:909-921`); a call site that
    /// drew from the cursor unconditionally shifted every later row's id by
    /// one per explicit id in the batch. Measured against TiDB -- see the
    /// receipt on `RetryAutoIds`.
    pub fn apply_auto_increment(
        &mut self,
        row: &mut [Datum],
        step: (u64, u64),
        reuse: impl FnOnce() -> Option<u64>,
    ) -> Result<AutoIncrement, AutoIdError> {
        let Some(offset) = self.auto_increment_offset else {
            return Ok(AutoIncrement::Absent);
        };
        let current = match row.get(offset) {
            Some(Datum::Int(value)) => *value as u64,
            Some(Datum::UInt(value)) => *value,
            _ => 0,
        };
        if current != 0 {
            // Go rebases so the next allocation is past the explicit value,
            // and a value the counter is already past changes nothing.
            self.auto_id.rebase(current).map_err(AutoIdError::Store)?;
            return Ok(AutoIncrement::Given(current));
        }
        let (increment, step_offset) = auto_id::increment_and_offset(step.0, step.1);
        // A replay does not draw from the counter at all: the id it is
        // rewriting was already drawn, and drawing again is exactly the gap
        // that moves `LAST_INSERT_ID()` off the row it names.
        let (id, reused) = match reuse() {
            Some(id) => (id, true),
            None => (self.auto_id.alloc(increment, step_offset)?, false),
        };
        self.check_auto_increment_fits(offset, id)?;
        // The allocated id skips the per-column cast the written values went
        // through, so it is placed in the column's own domain here.
        row[offset] = if self.auto_id.unsigned {
            Datum::UInt(id)
        } else {
            Datum::Int(id as i64)
        };
        Ok(if reused {
            AutoIncrement::Reused(id as i64)
        } else {
            AutoIncrement::Allocated(id as i64)
        })
    }

    /// Go `setDatumAutoIDAndCast`: the id the allocator handed out is CAST to
    /// the column's own type before it is written, so a column narrower than
    /// `BIGINT` refuses the id that does not fit it.
    ///
    /// The allocator counts in the full 64-bit domain -- Go's `autoid`
    /// package knows only signedness, never the column's width -- so a
    /// `TINYINT AUTO_INCREMENT` sitting at `127` still gets `128` handed to
    /// it, and it is this cast that turns that into `[types:1690]constant 128
    /// overflows tinyint`. Without it the row is written with a value the
    /// column cannot hold. Captured across every width and both
    /// signednesses: the id AT the type's maximum is accepted (`127`, `255`,
    /// `32767`, `65535`, `8388607`, `16777215`, `2147483647`, `4294967295`)
    /// and the next one is refused.
    ///
    /// The bound is carried as a 64-bit PATTERN read in the column's domain,
    /// which is what keeps `BIGINT UNSIGNED` correct: its maximum is above
    /// `i64::MAX`, so a bound computed as a signed integer would truncate and
    /// refuse ids the column holds perfectly well. At `BIGINT` width the
    /// bound IS the domain end, so this check can never fire there and the
    /// allocator's own exhaustion rule (`1467`, one id earlier) stays the
    /// only limit -- the two rules do not overlap.
    fn check_auto_increment_fits(&self, offset: usize, allocated: u64) -> Result<(), AutoIdError> {
        let Some(column) = self.columns.get(offset) else {
            return Ok(());
        };
        let code = column.field_type.code();
        let unsigned = self.auto_id.unsigned;
        // Go allows AUTO_INCREMENT on FLOAT/DOUBLE too, whose cast is not an
        // integer range check; only the integer widths are bounded here.
        let limit = match code {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong => {
                if unsigned {
                    integer_unsigned_upper_bound(code)
                } else {
                    integer_signed_upper_bound(code) as u64
                }
            }
            _ => return Ok(()),
        };
        if exceeds(allocated, limit, unsigned) {
            let value = if unsigned {
                allocated.to_string()
            } else {
                (allocated as i64).to_string()
            };
            return Err(AutoIdError::OutOfRange {
                value,
                type_name: type_str(code).to_owned(),
            });
        }
        Ok(())
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
        crate::generated_column::materialize(
            &self.columns,
            |i| self.columns[i].name.clone(),
            row,
            false,
            ctx,
        )
        .map_err(generation_error)
    }

    /// Restores the generated columns a decoded row does not carry: the
    /// VIRTUAL ones, whose value was never written.
    ///
    /// The READ path evaluates at Go's query level, where a zero divisor is a
    /// warning and the column reads NULL -- `ERROR_FOR_DIVISION_BY_ZERO` is
    /// resolved from the SQL mode for `INSERT`/`UPDATE`/`DELETE` only
    /// (`errctx.ErrGroupDividedByZero`), and a `SELECT` is not one of those.
    /// A caller that needs the WRITE level -- an index backfill, whose value
    /// is about to be persisted -- passes its own context to
    /// [`Self::fill_virtual_columns_in`].
    fn fill_virtual_columns(&self, row: &mut [Datum]) -> Result<(), KvTableError> {
        self.fill_virtual_columns_in(row, &tidb_expr::NoColumns)
    }

    /// [`Self::fill_virtual_columns`] under a caller-chosen context.
    fn fill_virtual_columns_in(
        &self,
        row: &mut [Datum],
        ctx: &impl tidb_expr::Columns,
    ) -> Result<(), KvTableError> {
        crate::generated_column::materialize(
            &self.columns,
            |i| self.columns[i].name.clone(),
            row,
            true,
            ctx,
        )
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
    ) -> Result<TableHandle, KvTableError> {
        if !self.common_handle_offsets.is_empty() {
            let values: Vec<Datum> = self
                .common_handle_offsets
                .iter()
                .map(|offset| row.get(*offset).cloned().unwrap_or(Datum::Null))
                .collect();
            let encoded = tidb_codec::encode_key_in_timezone(zone, &values)
                .map_err(|e| KvTableError::Encode(format!("{e:?}")))?;
            return Ok(TableHandle::Common(encoded));
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
                Ok(TableHandle::Int(handle as i64))
            }
        }
    }

    /// The row value bytes, omitting the columns the handle already carries.
    fn encode_row_value(
        &self,
        row: &[Datum],
        zone: &SessionTimeZone,
    ) -> Result<Vec<u8>, KvTableError> {
        let skip = self.handle_column_offsets();
        let mut ids = Vec::with_capacity(self.columns.len());
        let mut values = Vec::with_capacity(self.columns.len());
        for (offset, column) in self.columns.iter().enumerate() {
            // A clustered handle column and a VIRTUAL generated column are
            // both columns whose value is not in the row bytes: one is
            // rebuilt from the key, the other from its expression. Same skip
            // list, because it is the same fact.
            if skip.contains(&offset) || crate::generated_column::is_virtual(column) {
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
        self.auto_id.reset()
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
    pub fn create_index(
        &mut self,
        index: KvIndex,
        ctx: &impl tidb_expr::Columns,
    ) -> Result<(), KvTableError> {
        let zone = ctx.time_zone();
        if self
            .indexes
            .iter()
            .any(|existing| existing.name.eq_ignore_ascii_case(&index.name))
        {
            return Err(KvTableError::DuplicateKeyName(index.name.clone()));
        }
        // Backfill from the rows that already exist. The scan filled the
        // virtual columns at the READ level; the entries about to be
        // PERSISTED are a write, so they are recomputed under the DDL's own
        // context -- which is what makes `ALTER TABLE t ADD INDEX ((100/a))`
        // over a row with `a = 0` fail with 1365 under
        // `ERROR_FOR_DIVISION_BY_ZERO` instead of quietly indexing a NULL.
        // Recomputation is idempotent, so this changes no value that the
        // read level already agreed on.
        let mut rows = self.scan_rows_with_handles(&zone)?;
        for (_, row) in &mut rows {
            self.fill_virtual_columns_in(row, ctx)?;
        }
        let mut written = Vec::new();
        for (handle, row) in &rows {
            let (key, distinct) = self.index_key(&index, row, handle, &zone)?;
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
            let value = if distinct {
                match handle {
                    TableHandle::Int(value) => encode_handle_in_unique_index_value(
                        &tidb_txnkv::IntHandle::new(*value).into(),
                        false,
                    ),
                    TableHandle::Common(bytes) => {
                        let common = tidb_txnkv::CommonHandle::new(bytes.clone())
                            .map_err(|e| KvTableError::Encode(format!("{e:?}")))?;
                        encode_handle_in_unique_index_value(&common.into(), false)
                    }
                }
            } else {
                vec![b'0']
            };
            self.store
                .set(key.clone(), value)
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
            written.push(key);
        }
        self.indexes.push(index);
        Ok(())
    }

    /// Drops an index and every entry it owns, reporting whether it existed.
    pub fn drop_index(&mut self, name: &str, zone: &SessionTimeZone) -> Result<bool, KvTableError> {
        let Some(position) = self
            .indexes
            .iter()
            .position(|index| index.name.eq_ignore_ascii_case(name))
        else {
            return Ok(false);
        };
        let index = self.indexes.remove(position);
        let rows = self.scan_rows_with_handles(zone)?;
        for (handle, row) in &rows {
            let (key, _) = self.index_key(&index, row, handle, zone)?;
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

    /// Whether the column at `offset` is hidden.
    #[must_use]
    pub fn is_hidden(&self, offset: usize) -> bool {
        offset >= self.visible_column_count()
    }

    /// Whether an expression index reads the column at `offset`.
    ///
    /// An expression index is stored as a HIDDEN generated column plus an
    /// index over it, so the index's dependence on a user column is that
    /// hidden column's dependence. Dropping or renaming the user column would
    /// leave the generation expression reading a column that is gone, which is
    /// why Go refuses both with `ErrDependentByFunctionalIndex` (3837).
    #[must_use]
    pub fn expression_index_depends_on(&self, offset: usize) -> bool {
        self.columns[self.visible_column_count()..]
            .iter()
            .filter_map(|column| column.generated.as_ref())
            .any(|generated| generated.dependencies.contains(&offset))
    }

    /// Appends a hidden column -- the one an expression index key part is
    /// rewritten into -- and returns its offset.
    ///
    /// It goes at the very end, so no existing offset moves and the tail
    /// invariant holds by construction.
    pub fn add_hidden_column(&mut self, column: KvColumn) -> usize {
        self.columns.push(column);
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
        self.columns.insert(position, column);
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
        for index in &mut self.indexes {
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
        self.columns.remove(offset);
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
        for index in &mut self.indexes {
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
    pub fn modify_column(
        &mut self,
        offset: usize,
        new_column: KvColumn,
        new_position: Option<usize>,
        zone: &SessionTimeZone,
    ) -> Result<(), KvTableError> {
        // Rewriting the rows here reads them back through the scan and writes
        // them under a key built from the table id, which for a partitioned
        // table would collapse every partition into one. Re-routing them
        // instead would need the statement's evaluation context, which this
        // path does not carry, and Go has its own rules for modifying a
        // partitioning column besides -- so this refuses rather than moving
        // rows it cannot place.
        if self.partition.is_some() {
            return Err(KvTableError::Storage(
                "MODIFY COLUMN on a partitioned table is not supported by this node".to_owned(),
            ));
        }
        let target = new_column.field_type.clone();
        let not_null = target.flags() & NOT_NULL_FLAG != 0;
        let rows = self.scan_rows_with_handles(zone)?;
        let mut converted_rows = Vec::with_capacity(rows.len());
        for (row_number, (handle, row)) in rows.into_iter().enumerate() {
            let mut row = row;
            let value = row[offset].clone();
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
                    .map_err(|_| convert_failure(&new_column.name, &target, &value))?;
                if converted.event.is_some() {
                    return Err(convert_failure(&new_column.name, &target, &value));
                }
                row[offset] = converted.value;
            }
            converted_rows.push((handle, row));
        }

        self.columns[offset] = new_column;
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
        for index in &mut self.indexes {
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
            for (_, row) in &mut converted_rows {
                let value = row.remove(offset);
                row.insert(position, value);
            }
        }

        // The value bytes encode the column's type, so every row is written
        // again; the handles and index ids are unchanged.
        self.store.clear();
        for (handle, row) in &converted_rows {
            let value = self.encode_row_value(row, zone)?;
            let key = Key::from_bytes(encode_row_key_with_handle(
                self.table_id,
                &handle.record_handle(),
            ));
            self.write_index_entries(row, handle, zone)?;
            self.store
                .set(key, value)
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        }
        Ok(())
    }

    /// Moves the column at `from` to `to`, carrying every offset that
    /// addresses a column with it.
    fn move_column(&mut self, from: usize, to: usize) {
        let column = self.columns.remove(from);
        self.columns.insert(to, column);
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
        for index in &mut self.indexes {
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
        zone: &SessionTimeZone,
    ) -> Result<Vec<TableHandle>, KvTableError> {
        let mut found: Vec<TableHandle> = Vec::new();
        let clustered = self.pk_handle_offset.is_some() || !self.common_handle_offsets.is_empty();
        if clustered {
            let handle = self.handle_of_row(row, zone)?;
            if self.row_exists(&handle)? {
                found.push(handle);
            }
        }
        for index in self.indexes.clone() {
            if !index.unique {
                continue;
            }
            // A distinct entry's key does not carry the handle, so the
            // candidate handle below is only a placeholder for the key build.
            let (key, distinct) = self.index_key(&index, row, &TableHandle::Int(0), zone)?;
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
        zone: &SessionTimeZone,
    ) -> Result<KvTableError, KvTableError> {
        let clustered = self.pk_handle_offset.is_some() || !self.common_handle_offsets.is_empty();
        if clustered {
            let handle = self.handle_of_row(row, zone)?;
            if self.row_exists(&handle)? {
                return Ok(KvTableError::DuplicateEntry {
                    value: clustered_key_text(self, row),
                    key: self.qualified_key("PRIMARY"),
                });
            }
        }
        for index in self.indexes.clone() {
            if !index.unique {
                continue;
            }
            let (key, distinct) = self.index_key(&index, row, &TableHandle::Int(0), zone)?;
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
        self.indexes.push(index);
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
        self.indexes
            .iter_mut()
            .find(|index| index.name.eq_ignore_ascii_case(name))
    }

    /// The indexes a plan may read through, which is [`Self::indexes`] minus
    /// the invisible ones (Go's `IndexInfo.Invisible`). Every site that
    /// *chooses* an access path -- the cost-based index candidates and the
    /// unique-index point-get paths -- goes through here, so an index the
    /// planner must not pick is excluded once rather than at each chooser.
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
        // Go `addRecord`: a clustered key IS the handle, so a repeat collides.
        let handle = self.handle_of_row(row, &zone)?;
        let clustered = self.pk_handle_offset.is_some() || !self.common_handle_offsets.is_empty();
        if clustered && self.row_exists(&handle)? {
            return Err(KvTableError::DuplicateEntry {
                value: clustered_key_text(self, row),
                key: self.qualified_key("PRIMARY"),
            });
        }
        let key = Key::from_bytes(encode_row_key_with_handle(
            self.record_physical_id(row, ctx)?,
            &handle.record_handle(),
        ));
        // Go writes the row first, then its index entries; a duplicate on a
        // unique index aborts the statement.
        self.write_index_entries(row, &handle, &zone)?;
        self.store
            .set(key, value)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        Ok(handle)
    }

    /// The table's indexes, for `ADMIN CHECK` (see [`crate::admin_check`]).
    ///
    /// The field itself stays private because every other reader reaches an
    /// index through a scan; the consistency check is the one caller that
    /// needs the whole list as metadata.
    #[must_use]
    pub fn index_list_for_check(&self) -> Vec<KvIndex> {
        self.indexes.clone()
    }

    /// [`KvTable::index_key`] for [`crate::admin_check`]: the entry key one
    /// row's values encode to under `index`.
    pub fn index_key_for_check(
        &self,
        index: &KvIndex,
        row: &[Datum],
        handle: &TableHandle,
        zone: &SessionTimeZone,
    ) -> Result<(Vec<u8>, bool), KvTableError> {
        self.index_key(index, row, handle, zone)
    }

    /// Every stored entry of one index, as `(entry key, the handle it names)`.
    ///
    /// This is the only sweep of a WHOLE index in the engine: an ordinary
    /// index read has datum bounds and goes through
    /// [`KvTable::index_range_cursor`]. `ADMIN CHECK` has none -- its subject
    /// is exactly the set of entries that exist.
    pub fn index_entries_for_check(
        &mut self,
        index_id: i64,
    ) -> Result<Vec<(Vec<u8>, TableHandle)>, KvTableError> {
        let Some(index) = self
            .indexes
            .iter()
            .find(|index| index.id == index_id)
            .cloned()
        else {
            return Err(KvTableError::Decode("no such index".to_owned()));
        };
        let common = !self.common_handle_offsets.is_empty();
        let (low, high) = crate::admin_check::index_key_bounds(self.table_id, index_id);
        let mut iterator = self
            .store
            .iter(Some(&Key::from_bytes(low)), Some(&Key::from_bytes(high)))
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        let mut entries = Vec::new();
        while iterator.valid() {
            let key = iterator.key().as_bytes().to_vec();
            let handle = index_entry_handle(&index, &key, iterator.value(), common)?;
            entries.push((key, handle));
            iterator
                .next()
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        }
        iterator.close();
        Ok(entries)
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
    pub fn get_row_by_handle(
        &mut self,
        handle: &TableHandle,
        zone: &SessionTimeZone,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        self.read_row(handle, zone)
    }

    /// The row stored under `handle`, decoded, or `None` when absent.
    fn read_row(
        &mut self,
        handle: &TableHandle,
        zone: &SessionTimeZone,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        let Some((_, entry)) = self.stored_record(handle)? else {
            return Ok(None);
        };
        let column_types: BTreeMap<i64, FieldType> = self
            .columns
            .iter()
            .map(|c| (c.id, c.field_type.clone()))
            .collect();
        let mut decoded = decode_table_row_to_map(&entry, &column_types, Some(zone))
            .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
        let mut row: Vec<Datum> = self
            .columns
            .iter()
            .map(|column| {
                decoded
                    .remove(&column.id)
                    .unwrap_or_else(|| column.origin_default_value())
            })
            .collect();
        // The handle columns are not in the value; Go reads them from the
        // handle itself.
        self.fill_handle_columns(&mut row, handle, zone)?;
        // Nor is a VIRTUAL generated column, whose value is its expression.
        self.fill_virtual_columns(&mut row)?;
        Ok(Some(row))
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
    pub fn update_row(
        &mut self,
        handle: &TableHandle,
        row: &[Datum],
        ctx: &impl tidb_expr::Columns,
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
            self.handle_of_row(row, &zone)?
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
        // Go removes the old index entries and writes the new ones. They point
        // AT the handle, so a moved row needs them rewritten even when the
        // indexed values did not change.
        if !self.indexes.is_empty() {
            let old = self.read_row(handle, &zone)?;
            if let Some(old) = &old {
                self.delete_index_entries(old, handle, &zone)?;
            }
            if let Err(error) = self.write_index_entries(row, &new_handle, &zone) {
                // Restore the entries the failed update removed, so a rejected
                // statement leaves the index as it found it.
                if let Some(old) = &old {
                    self.write_index_entries(old, handle, &zone)?;
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
        let old_key = self.stored_record_key(handle)?;
        let key = Key::from_bytes(encode_row_key_with_handle(
            self.record_physical_id(row, ctx)?,
            &new_handle.record_handle(),
        ));
        if let Some(old_key) = old_key.filter(|old_key| *old_key != key) {
            self.store
                .delete(old_key)
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        }
        self.store
            .set(key, value)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))
    }

    /// Removes the row stored under `handle`.
    pub fn delete_row(
        &mut self,
        handle: &TableHandle,
        zone: &SessionTimeZone,
    ) -> Result<(), KvTableError> {
        if !self.indexes.is_empty() {
            if let Some(row) = self.read_row(handle, zone)? {
                self.delete_index_entries(&row, handle, zone)?;
            }
        }
        let Some(key) = self.stored_record_key(handle)? else {
            return Ok(());
        };
        self.store
            .delete(key)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))
    }
}

/// The text a clustered-key duplicate reports: the key columns joined by `-`,
/// as Go's `ErrKeyExists` formats them.
fn clustered_key_text(table: &KvTable, row: &[Datum]) -> String {
    let offsets = table.handle_column_offsets();
    offsets
        .iter()
        .map(|offset| match row.get(*offset) {
            Some(Datum::Int(value)) => value.to_string(),
            Some(Datum::UInt(value)) => value.to_string(),
            Some(Datum::Bytes(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
            Some(Datum::String(text)) => String::from_utf8_lossy(text.bytes()).into_owned(),
            Some(other) => format!("{other:?}"),
            None => String::new(),
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
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        Datum::Real(value) => value.to_string(),
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
                    default_value: None,
                    // A column present at CREATE TABLE has no pre-existing rows.
                    origin_default: None,
                    generated: None,
                },
                KvColumn {
                    name: "s".to_owned(),
                    id: 2,
                    field_type: varstr(),
                    default_value: None,
                    // A column present at CREATE TABLE has no pre-existing rows.
                    origin_default: None,
                    generated: None,
                },
            ],
        )
    }

    /// The scan bound must cover the whole table and nothing beyond it: the
    /// codec's handle range is inclusive at the top while the iterator's upper
    /// bound is exclusive, so the largest handle must still be returned and a
    /// neighbouring table's rows must not be.
    /// The handle a scan reports must be the handle the codec wrote, so an
    /// UPDATE/DELETE addresses the row it read. Covers the sign flip the key
    /// codec applies (negative handles sort below positive ones).
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
            .scan_rows_with_handles(&tidb_datatype::SessionTimeZone::utc())
            .unwrap()
            .into_iter()
            .map(|(handle, _)| handle)
            .collect();
        assert_eq!(scanned, handles);

        // A row written under an explicit handle round-trips too.
        t.update_row(
            &handles[1],
            &[Datum::Int(99), Datum::Bytes(b"y".to_vec())],
            &tidb_expr::NoColumns,
        )
        .unwrap();
        let rows = t
            .scan_rows_with_handles(&tidb_datatype::SessionTimeZone::utc())
            .unwrap();
        assert_eq!(rows.len(), 3, "update replaced in place, it did not append");
        assert_eq!(rows[1].0, handles[1]);
        assert_eq!(rows[1].1[0], Datum::Int(99));

        t.delete_row(&handles[0], &tidb_datatype::SessionTimeZone::utc())
            .unwrap();
        let after: Vec<TableHandle> = t
            .scan_rows_with_handles(&tidb_datatype::SessionTimeZone::utc())
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
        let mut neighbour = KvTable::new(t.table_id + 1, t.columns.clone());
        neighbour
            .insert_row(
                &[Datum::Int(99), Datum::Bytes(b"y".to_vec())],
                &tidb_expr::NoColumns,
            )
            .unwrap();

        let rows = t.scan_rows(&tidb_datatype::SessionTimeZone::utc()).unwrap();
        assert_eq!(
            rows.len(),
            3,
            "every handle including the largest is scanned"
        );
        assert_eq!(rows[2][0], Datum::Int(2));
        assert_eq!(
            neighbour
                .scan_rows(&tidb_datatype::SessionTimeZone::utc())
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

        let rows = t.scan_rows(&tidb_datatype::SessionTimeZone::utc()).unwrap();
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
        let mut scan = TableScanExec::new(
            ExecutorMeta::new(Schema::new(out_cols), 0, 4, 1024),
            t,
            tidb_datatype::SessionTimeZone::utc(),
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

    /// A table whose only column is an AUTO_INCREMENT `BIGINT`, signed or not.
    fn auto_increment_table(unsigned: bool) -> KvTable {
        let mut table = KvTable::new(
            7,
            vec![KvColumn {
                name: "id".to_owned(),
                id: 1,
                field_type: long().with_unsigned(unsigned),
                default_value: None,
                origin_default: None,
                generated: None,
            }],
        );
        table.set_auto_increment_offset(0);
        table
    }

    /// An explicit id at the domain's end leaves the allocator with nothing to
    /// hand out, and it says so instead of wrapping or repeating.
    ///
    /// Captured from TiDB: `BIGINT` at `9223372036854775807` and `BIGINT
    /// UNSIGNED` at both `18446744073709551614` and `18446744073709551615` all
    /// answer the next insert with `[autoid:1467]`. The unsigned pair is here
    /// rather than in the session tests because a literal above `i64::MAX` is
    /// not yet expressible in this tier's SQL.
    #[test]
    fn the_allocator_refuses_at_the_end_of_the_columns_domain() {
        for (unsigned, explicit, pattern) in [
            (false, Datum::Int(i64::MAX), i64::MAX as u64),
            (true, Datum::UInt(u64::MAX), u64::MAX),
            (true, Datum::UInt(u64::MAX - 1), u64::MAX - 1),
        ] {
            let mut table = auto_increment_table(unsigned);
            let mut row = [explicit];
            assert_eq!(
                table.apply_auto_increment(&mut row, (1, 1), || None),
                Ok(AutoIncrement::Given(pattern))
            );
            let mut row = [Datum::Null];
            assert_eq!(
                table.apply_auto_increment(&mut row, (1, 1), || None),
                Err(AutoIdError::Exhausted),
                "unsigned={unsigned}"
            );
        }
    }

    /// An explicit UNSIGNED id above `i64::MAX` rebases the allocator in its
    /// own domain, and the id that follows it is the next unsigned integer --
    /// not the low id a signed reading of the counter would have re-issued on
    /// top of the row just written.
    #[test]
    fn an_unsigned_explicit_id_rebases_in_the_unsigned_domain() {
        let mut table = auto_increment_table(true);
        let mut row = [Datum::UInt(1 << 63)];
        assert_eq!(
            table.apply_auto_increment(&mut row, (1, 1), || None),
            Ok(AutoIncrement::Given(1 << 63))
        );
        let mut row = [Datum::Null];
        table
            .apply_auto_increment(&mut row, (1, 1), || None)
            .unwrap();
        assert_eq!(row[0], Datum::UInt((1 << 63) + 1));
    }

    /// The same explicit id on a SIGNED column is a value the counter is
    /// already past (Go's rebase only ever moves up), so allocation carries on
    /// from where it was.
    #[test]
    fn a_signed_explicit_id_below_the_counter_does_not_move_it() {
        let mut table = auto_increment_table(false);
        let mut row = [Datum::Int(-5)];
        assert_eq!(
            table.apply_auto_increment(&mut row, (1, 1), || None),
            Ok(AutoIncrement::Given(-5_i64 as u64))
        );
        let mut row = [Datum::Null];
        table
            .apply_auto_increment(&mut row, (1, 1), || None)
            .unwrap();
        assert_eq!(row[0], Datum::Int(1));
    }
}
