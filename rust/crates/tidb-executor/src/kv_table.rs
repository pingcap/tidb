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
//! The bytes live in a `tidb-txnkv` [`MemStorage`] and are read back through
//! the `Retriever`/`KvIterator` traits -- the same contract a TiKV snapshot
//! implements -- so the scan is written against the storage interface rather
//! than against a container.
//!
//! NOT MODELLED (documented): the storage behind those traits is in-process
//! and has no MVCC versions, timestamps, locks, regions, or coprocessor
//! pushdown, so a scan reads the latest write immediately. Replacing
//! [`MemStorage`] with a transaction-backed snapshot does not touch the codec
//! or the scan loop.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use std::collections::BTreeMap;
use tidb_chunk::chunk::Chunk;
use tidb_codec::table_key::{
    encode_index_seek_key, encode_row_key_with_handle, get_table_handle_key_range, RecordHandle,
    RECORD_ROW_KEY_LEN,
};
use tidb_datatype::{Datum, FieldType};
use tidb_expr::schema::Schema;
use tidb_tablecodec::{
    cut_index_key, decode_handle_in_index_value, decode_table_row_to_map,
    encode_handle_in_unique_index_value, encode_table_row,
};
use tidb_txnkv::{
    GetOptions, Getter, Key, KvIterator, MemStorage, MemStorageError, Mutator, Retriever,
};

/// Go `kv.Handle`: the row identifier a record key encodes.
///
/// An integer handle comes from a single-column integer primary key (or the
/// allocated `_tidb_rowid` when the table has none); a common handle is the
/// codec encoding of a clustered primary key's columns, which is what makes a
/// string or multi-column primary key clustered.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TableHandle {
    /// Go `kv.IntHandle`.
    Int(i64),
    /// Go `kv.CommonHandle`, holding the encoded key datums.
    Common(Vec<u8>),
}

impl TableHandle {
    /// The record-key component this handle contributes.
    fn record_handle(&self) -> RecordHandle {
        match self {
            TableHandle::Int(value) => RecordHandle::Int(*value),
            TableHandle::Common(bytes) => RecordHandle::Common(bytes.clone()),
        }
    }

    /// The integer value, for the callers that only support int handles.
    #[must_use]
    pub fn int_value(&self) -> Option<i64> {
        match self {
            TableHandle::Int(value) => Some(*value),
            TableHandle::Common(_) => None,
        }
    }
}

/// Go `ranger.Range`: one scanned interval of an index.
///
/// Both bounds are datum tuples over the index's leading columns, with a flag
/// for whether each end is excluded. Go's builder always produces bounds that
/// exclude NULL for an ordinary comparison -- a `<`/`<=` range starts at
/// `MinNotNull`, not at NULL -- which is why a NULL value never satisfies a
/// comparison.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexRange {
    /// Go `Range.LowVal`.
    pub low: Vec<Datum>,
    /// Go `Range.HighVal`.
    pub high: Vec<Datum>,
    /// Go `Range.LowExclude`.
    pub low_exclusive: bool,
    /// Go `Range.HighExclude`.
    pub high_exclusive: bool,
}

/// One index of a [`KvTable`]: Go `model.IndexInfo`, reduced to what an index
/// write and a uniqueness check need.
#[derive(Clone, Debug)]
pub struct KvIndex {
    /// The index id (Go `IndexInfo.ID`), the `_i` key component.
    pub id: i64,
    /// The index name, which a duplicate-key error reports.
    pub name: String,
    /// Go `IndexInfo.Unique`.
    pub unique: bool,
    /// The indexed columns' offsets in the row, in index order.
    pub column_offsets: Vec<usize>,
}

/// A column of a [`KvTable`]: name, column id, and type.
#[derive(Clone, Debug)]
pub struct KvColumn {
    /// The column name.
    pub name: String,
    /// The column id (Go `ColumnInfo.ID`), the key of the row-format entries.
    pub id: i64,
    /// The column type.
    pub field_type: FieldType,
    /// Go `ColumnInfo.DefaultValue`: the value an omitted column takes.
    /// `None` means no `DEFAULT` was written, which is not the same as a
    /// `DEFAULT NULL`.
    pub default_value: Option<Datum>,
    /// Go `ColumnInfo.OriginDefaultValue`: what a row written BEFORE this
    /// column existed reads back as. `ADD COLUMN ... DEFAULT 7` gives the
    /// existing rows 7, not NULL, and the row bytes are never rewritten --
    /// the value is filled in on read.
    pub origin_default: Option<Datum>,
}

/// A table whose rows live as TiKV-format bytes in a sorted key/value map.
#[derive(Clone, Debug)]
pub struct KvTable {
    /// The table id (Go `TableInfo.ID`), the record-key prefix.
    pub table_id: i64,
    /// The columns, in schema order.
    pub columns: Vec<KvColumn>,
    /// The byte store, read through the `Retriever` contract (module doc).
    store: MemStorage,
    /// The next integer row handle (Go `_tidb_rowid` allocation, simplified to
    /// a monotone counter; the real autoid allocator is a separate unit).
    next_handle: i64,
    /// Go `TableInfo.PKIsHandle`: the offset of the single integer primary-key
    /// column whose value IS the row handle, when the table has one.
    pk_handle_offset: Option<usize>,
    /// The table's indexes (Go `TableInfo.Indices`).
    indexes: Vec<KvIndex>,
    /// The AUTO_INCREMENT column's offset, if the table has one.
    auto_increment_offset: Option<usize>,
    /// Go's auto-id allocator base: the next value to hand out.
    next_auto_id: i64,
    /// Go `TableInfo.IsCommonHandle`: the clustered primary key's column
    /// offsets, whose encoding IS the row handle. Empty when the table has no
    /// clustered common handle.
    common_handle_offsets: Vec<usize>,
}

/// A failure while encoding or decoding table bytes.
#[derive(Debug)]
pub enum KvTableError {
    /// A row failed to encode.
    Encode(String),
    /// Go `ErrDupEntry` (1062): a row with this primary key already exists.
    DuplicateEntry {
        /// The rejected key value, as MySQL prints it.
        value: String,
        /// The violated key's name; Go names the clustered one PRIMARY.
        key: String,
    },
    /// A stored value failed to decode.
    Decode(String),
    /// The storage layer refused a read or write.
    Storage(String),
}

impl KvTable {
    /// Builds an empty table.
    #[must_use]
    pub fn new(table_id: i64, columns: Vec<KvColumn>) -> Self {
        KvTable {
            table_id,
            columns,
            store: MemStorage::new(),
            next_handle: 1,
            pk_handle_offset: None,
            indexes: Vec::new(),
            common_handle_offsets: Vec::new(),
            auto_increment_offset: None,
            next_auto_id: 1,
        }
    }

    /// Marks the AUTO_INCREMENT column.
    pub fn set_auto_increment_offset(&mut self, offset: usize) {
        self.auto_increment_offset = Some(offset);
    }

    /// The AUTO_INCREMENT column's offset, if any.
    #[must_use]
    pub fn auto_increment_offset(&self) -> Option<usize> {
        self.auto_increment_offset
    }

    /// Go `adjustAutoIncrementDatum`: fills the auto-increment column.
    ///
    /// An omitted, NULL or zero value takes the next allocated id; an explicit
    /// non-zero value is kept and REBASES the allocator so later rows exceed
    /// it. Returns the id allocated for this row, which the statement reports
    /// as `LAST_INSERT_ID` for the first such row.
    ///
    /// DEFERRED (documented): the `NO_AUTO_VALUE_ON_ZERO` sql_mode, under
    /// which Go keeps an explicit zero instead of allocating.
    pub fn apply_auto_increment(&mut self, row: &mut [Datum]) -> Option<i64> {
        let offset = self.auto_increment_offset?;
        let current = match row.get(offset) {
            Some(Datum::Int(value)) => *value,
            Some(Datum::UInt(value)) => *value as i64,
            _ => 0,
        };
        if current != 0 {
            // Go rebases so the next allocation is past the explicit value.
            if current >= self.next_auto_id {
                self.next_auto_id = current + 1;
            }
            return None;
        }
        let allocated = self.next_auto_id;
        self.next_auto_id += 1;
        row[offset] = Datum::Int(allocated);
        Some(allocated)
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

    /// The column offsets the handle carries, which the row value omits
    /// (Go `CanSkip`: a PK handle column and a full-length common-handle
    /// column are both skipped from the encoded row).
    fn handle_column_offsets(&self) -> Vec<usize> {
        match self.pk_handle_offset {
            Some(offset) => vec![offset],
            None => self.common_handle_offsets.clone(),
        }
    }

    /// The handle a row's values produce.
    fn handle_of_row(&mut self, row: &[Datum]) -> Result<TableHandle, KvTableError> {
        if !self.common_handle_offsets.is_empty() {
            let values: Vec<Datum> = self
                .common_handle_offsets
                .iter()
                .map(|offset| row.get(*offset).cloned().unwrap_or(Datum::Null))
                .collect();
            let encoded = tidb_codec::encode_key(&values)
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
                let handle = self.next_handle;
                self.next_handle += 1;
                Ok(TableHandle::Int(handle))
            }
        }
    }

    /// The row value bytes, omitting the columns the handle already carries.
    fn encode_row_value(&self, row: &[Datum]) -> Result<Vec<u8>, KvTableError> {
        let skip = self.handle_column_offsets();
        let mut ids = Vec::with_capacity(self.columns.len());
        let mut values = Vec::with_capacity(self.columns.len());
        for (offset, column) in self.columns.iter().enumerate() {
            if skip.contains(&offset) {
                continue;
            }
            ids.push(column.id);
            values.push(row.get(offset).cloned().unwrap_or(Datum::Null));
        }
        encode_table_row(None, &values, &ids, true, None)
            .map_err(|e| KvTableError::Encode(format!("{e:?}")))
    }

    /// Restores the handle columns into a decoded row, which Go does by
    /// reading `h.IntValue()` or `h.EncodedCol(i)` rather than the value.
    fn fill_handle_columns(
        &self,
        row: &mut [Datum],
        handle: &TableHandle,
    ) -> Result<(), KvTableError> {
        match handle {
            TableHandle::Int(value) => {
                if let Some(offset) = self.pk_handle_offset {
                    row[offset] = Datum::Int(*value);
                }
            }
            TableHandle::Common(bytes) => {
                let mut rest: &[u8] = bytes;
                for offset in &self.common_handle_offsets {
                    let (remaining, value) = tidb_codec::decode_one(rest)
                        .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
                    row[*offset] = value;
                    rest = remaining;
                }
            }
        }
        Ok(())
    }

    /// Adds a column at `position`, which is Go's ALTER TABLE ADD COLUMN.
    ///
    /// The column takes a fresh id, so rows written earlier simply do not
    /// carry it and read back its origin default. Index and handle offsets
    /// shift with the insertion, since they address columns by position.
    pub fn add_column(&mut self, position: usize, column: KvColumn) {
        let position = position.min(self.columns.len());
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

    /// Adds an index, whose entries every later write maintains.
    pub fn add_index(&mut self, index: KvIndex) {
        self.indexes.push(index);
    }

    /// The table's indexes.
    #[must_use]
    pub fn indexes(&self) -> &[KvIndex] {
        &self.indexes
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
        self.store.len()
    }

    /// Whether the table has no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Inserts one row (a `Datum` per column, in schema order): encodes the
    /// record key from the next handle and the value through the v2 row format,
    /// exactly the bytes a TiKV-backed table would store.
    pub fn insert_row(&mut self, row: &[Datum]) -> Result<TableHandle, KvTableError> {
        let value = self.encode_row_value(row)?;
        // Go `addRecord`: a clustered key IS the handle, so a repeat collides.
        let handle = self.handle_of_row(row)?;
        let clustered = self.pk_handle_offset.is_some() || !self.common_handle_offsets.is_empty();
        if clustered && self.row_exists(&handle)? {
            return Err(KvTableError::DuplicateEntry {
                value: clustered_key_text(self, row),
                key: "PRIMARY".to_owned(),
            });
        }
        let key = Key::from_bytes(encode_row_key_with_handle(
            self.table_id,
            &handle.record_handle(),
        ));
        // Go writes the row first, then its index entries; a duplicate on a
        // unique index aborts the statement.
        self.write_index_entries(row, &handle)?;
        self.store
            .set(key, value)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        Ok(handle)
    }

    /// Scans the table's record-key range in key order, decoding each value.
    /// Returns rows as `Datum`s in schema order (a missing column decodes
    /// NULL, and the handle columns come from the key).
    pub fn scan_rows(&mut self) -> Result<Vec<Vec<Datum>>, KvTableError> {
        Ok(self
            .scan_rows_with_handles()?
            .into_iter()
            .map(|(_, row)| row)
            .collect())
    }

    /// Like [`KvTable::scan_rows`], but each row carries the record handle its
    /// key encodes, which `UPDATE`/`DELETE` need to address the row again.
    pub fn scan_rows_with_handles(
        &mut self,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError> {
        let (low, high) = get_table_handle_key_range(self.table_id);
        let column_types: BTreeMap<i64, FieldType> = self
            .columns
            .iter()
            .map(|c| (c.id, c.field_type.clone()))
            .collect();
        // `get_table_handle_key_range` returns an inclusive upper bound, while
        // the iterator's is exclusive, so the scan runs to the key just past it.
        let mut upper = high;
        upper.push(0);
        let mut iterator = self
            .store
            .iter(Some(&Key::from_bytes(low)), Some(&Key::from_bytes(upper)))
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;

        let mut rows = Vec::new();
        while iterator.valid() {
            let handle = self.decode_record_handle(iterator.key().as_bytes())?;
            let mut decoded = decode_table_row_to_map(iterator.value(), &column_types, None)
                .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
            let mut row: Vec<Datum> = self
                .columns
                .iter()
                .map(|column| {
                    decoded
                        .remove(&column.id)
                        // A row written before this column existed reads back
                        // its origin default.
                        .unwrap_or_else(|| column.origin_default.clone().unwrap_or(Datum::Null))
                })
                .collect();
            self.fill_handle_columns(&mut row, &handle)?;
            rows.push((handle, row));
            iterator
                .next()
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        }
        iterator.close();
        Ok(rows)
    }

    /// Go `GenIndexKey`: the entry key for one index over `row`, plus Go's
    /// `distinct` flag.
    ///
    /// `distinct` is true only for a unique index whose indexed values are all
    /// non-NULL -- MySQL lets a unique index hold any number of NULLs, so a
    /// NULL-bearing entry is stored the non-distinct way (handle appended to
    /// the key) and never collides.
    fn index_key(
        &self,
        index: &KvIndex,
        row: &[Datum],
        handle: &TableHandle,
    ) -> Result<(Vec<u8>, bool), KvTableError> {
        let values: Vec<Datum> = index
            .column_offsets
            .iter()
            .map(|offset| row.get(*offset).cloned().unwrap_or(Datum::Null))
            .collect();
        let distinct = index.unique && !values.contains(&Datum::Null);
        let mut encoded =
            tidb_codec::encode_key(&values).map_err(|e| KvTableError::Encode(format!("{e:?}")))?;
        if !distinct {
            // Go appends the handle so non-distinct entries stay unique.
            match handle {
                TableHandle::Int(value) => encoded.extend_from_slice(
                    &tidb_codec::encode_key(&[Datum::Int(*value)])
                        .map_err(|e| KvTableError::Encode(format!("{e:?}")))?,
                ),
                TableHandle::Common(bytes) => encoded.extend_from_slice(bytes),
            }
        }
        Ok((
            encode_index_seek_key(self.table_id, index.id, &encoded),
            distinct,
        ))
    }

    /// Writes every index entry for `row`, rejecting a duplicate on a unique
    /// index as Go's `index.Create` does with `ErrKeyExists`.
    fn write_index_entries(
        &mut self,
        row: &[Datum],
        handle: &TableHandle,
    ) -> Result<(), KvTableError> {
        let indexes = self.indexes.clone();
        for index in &indexes {
            let (key, distinct) = self.index_key(index, row, handle)?;
            let key = Key::from_bytes(key);
            if distinct {
                if self.store.get(&key, GetOptions::default()).is_ok() {
                    return Err(KvTableError::DuplicateEntry {
                        value: duplicate_value_text(index, row),
                        key: index.name.clone(),
                    });
                }
                // A distinct entry carries the handle as its value, which is
                // what makes a unique-index lookup a point read.
                let value = match handle {
                    TableHandle::Int(value) => encode_handle_in_unique_index_value(
                        &tidb_txnkv::IntHandle::new(*value).into(),
                        false,
                    ),
                    // Go stores the encoded common handle as the entry value.
                    TableHandle::Common(bytes) => {
                        let common = tidb_txnkv::CommonHandle::new(bytes.clone())
                            .map_err(|e| KvTableError::Encode(format!("{e:?}")))?;
                        encode_handle_in_unique_index_value(&common.into(), false)
                    }
                };
                self.store
                    .set(key, value)
                    .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
            } else {
                // Go stores a single version byte for a non-distinct entry.
                self.store
                    .set(key, vec![b'0'])
                    .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
            }
        }
        Ok(())
    }

    /// Removes every index entry for `row`.
    fn delete_index_entries(
        &mut self,
        row: &[Datum],
        handle: &TableHandle,
    ) -> Result<(), KvTableError> {
        let indexes = self.indexes.clone();
        for index in &indexes {
            let (key, _) = self.index_key(index, row, handle)?;
            self.store
                .delete(Key::from_bytes(key))
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        }
        Ok(())
    }

    /// Looks a row handle up through a unique index, the point-get Go plans as
    /// `PointGetPlan` on a unique key. `None` when no entry matches.
    pub fn lookup_unique(
        &mut self,
        index_id: i64,
        values: &[Datum],
    ) -> Result<Option<TableHandle>, KvTableError> {
        let Some(index) = self
            .indexes
            .iter()
            .find(|index| index.id == index_id)
            .cloned()
        else {
            return Err(KvTableError::Decode("no such index".to_owned()));
        };
        if !index.unique || values.contains(&Datum::Null) {
            // Only a distinct entry stores the handle in its value.
            return Ok(None);
        }
        let encoded =
            tidb_codec::encode_key(values).map_err(|e| KvTableError::Encode(format!("{e:?}")))?;
        let key = Key::from_bytes(encode_index_seek_key(self.table_id, index.id, &encoded));
        match self.store.get(&key, GetOptions::default()) {
            Ok(entry) => {
                let handle = decode_handle_in_index_value(&entry.value)
                    .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
                Ok(Some(convert_handle(&handle)))
            }
            Err(MemStorageError::NotFound) => Ok(None),
            Err(error) => Err(KvTableError::Storage(format!("{error:?}"))),
        }
    }

    /// The row stored under `handle`, decoded, or `None` when absent -- the
    /// single read a point-get plan performs.
    pub fn get_row_by_handle(
        &mut self,
        handle: &TableHandle,
    ) -> Result<Option<Vec<Datum>>, KvTableError> {
        self.read_row(handle)
    }

    /// The row stored under `handle`, decoded, or `None` when absent.
    fn read_row(&mut self, handle: &TableHandle) -> Result<Option<Vec<Datum>>, KvTableError> {
        let key = Key::from_bytes(encode_row_key_with_handle(
            self.table_id,
            &handle.record_handle(),
        ));
        let entry = match self.store.get(&key, GetOptions::default()) {
            Ok(entry) => entry,
            Err(MemStorageError::NotFound) => return Ok(None),
            Err(error) => return Err(KvTableError::Storage(format!("{error:?}"))),
        };
        let column_types: BTreeMap<i64, FieldType> = self
            .columns
            .iter()
            .map(|c| (c.id, c.field_type.clone()))
            .collect();
        let mut decoded = decode_table_row_to_map(&entry.value, &column_types, None)
            .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
        let mut row: Vec<Datum> = self
            .columns
            .iter()
            .map(|column| {
                decoded
                    .remove(&column.id)
                    .unwrap_or_else(|| column.origin_default.clone().unwrap_or(Datum::Null))
            })
            .collect();
        // The handle columns are not in the value; Go reads them from the
        // handle itself.
        self.fill_handle_columns(&mut row, handle)?;
        Ok(Some(row))
    }

    /// The handles an index range covers, in index order.
    ///
    /// Go turns a range into a key interval in `IndexRangesToKVRanges`: the
    /// low key is the encoded low bound, advanced to its `PrefixNext` when the
    /// bound is excluded; the high key is the encoded high bound, advanced to
    /// its `PrefixNext` when the bound is INCLUDED, because the scan's upper
    /// end is exclusive. This walks that interval and reads the handle out of
    /// each entry.
    pub fn scan_index_range(
        &mut self,
        index_id: i64,
        range: &IndexRange,
    ) -> Result<Vec<TableHandle>, KvTableError> {
        let Some(index) = self
            .indexes
            .iter()
            .find(|index| index.id == index_id)
            .cloned()
        else {
            return Err(KvTableError::Decode("no such index".to_owned()));
        };
        let encode = |values: &[Datum]| -> Result<Vec<u8>, KvTableError> {
            tidb_codec::encode_key(values).map_err(|e| KvTableError::Encode(format!("{e:?}")))
        };
        let mut low = Key::from_bytes(encode_index_seek_key(
            self.table_id,
            index_id,
            &encode(&range.low)?,
        ));
        if range.low_exclusive {
            low = low.prefix_next();
        }
        let mut high = Key::from_bytes(encode_index_seek_key(
            self.table_id,
            index_id,
            &encode(&range.high)?,
        ));
        if !range.high_exclusive {
            high = high.prefix_next();
        }

        let mut iterator = self
            .store
            .iter(Some(&low), Some(&high))
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        let mut handles = Vec::new();
        while iterator.valid() {
            handles.push(index_entry_handle(
                &index,
                iterator.key().as_bytes(),
                iterator.value(),
                !self.common_handle_offsets.is_empty(),
            )?);
            iterator
                .next()
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        }
        iterator.close();
        Ok(handles)
    }

    /// The handle a record key encodes: the bytes after the record prefix,
    /// read as an integer or kept whole as a common handle.
    fn decode_record_handle(&self, key: &[u8]) -> Result<TableHandle, KvTableError> {
        if self.common_handle_offsets.is_empty() {
            return decode_int_handle(key).map(TableHandle::Int);
        }
        let bytes = key
            .get(RECORD_ROW_KEY_LEN - 8..)
            .ok_or_else(|| KvTableError::Decode("record key is too short".to_owned()))?;
        Ok(TableHandle::Common(bytes.to_vec()))
    }

    /// Whether a row is already stored under `handle`.
    fn row_exists(&mut self, handle: &TableHandle) -> Result<bool, KvTableError> {
        let key = Key::from_bytes(encode_row_key_with_handle(
            self.table_id,
            &handle.record_handle(),
        ));
        match self.store.get(&key, GetOptions::default()) {
            Ok(_) => Ok(true),
            Err(MemStorageError::NotFound) => Ok(false),
            Err(error) => Err(KvTableError::Storage(format!("{error:?}"))),
        }
    }

    /// Replaces the row stored under `handle` (Go's `UPDATE` writes the new
    /// row back under the same record key when the handle column did not
    /// change).
    pub fn update_row(&mut self, handle: &TableHandle, row: &[Datum]) -> Result<(), KvTableError> {
        // Go removes the old index entries and writes the new ones.
        if !self.indexes.is_empty() {
            if let Some(old) = self.read_row(handle)? {
                self.delete_index_entries(&old, handle)?;
            }
            if let Err(error) = self.write_index_entries(row, handle) {
                // Restore the entries the failed update removed, so a rejected
                // statement leaves the index as it found it.
                if let Some(old) = self.read_row(handle)? {
                    self.write_index_entries(&old, handle)?;
                }
                return Err(error);
            }
        }
        // The handle columns stay out of the value, as on insert.
        let value = self.encode_row_value(row)?;
        let key = Key::from_bytes(encode_row_key_with_handle(
            self.table_id,
            &handle.record_handle(),
        ));
        self.store
            .set(key, value)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))
    }

    /// Removes the row stored under `handle`.
    pub fn delete_row(&mut self, handle: &TableHandle) -> Result<(), KvTableError> {
        if !self.indexes.is_empty() {
            if let Some(row) = self.read_row(handle)? {
                self.delete_index_entries(&row, handle)?;
            }
        }
        let key = Key::from_bytes(encode_row_key_with_handle(
            self.table_id,
            &handle.record_handle(),
        ));
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

/// The row handle an index entry names.
///
/// A distinct entry keeps the handle in its value; a non-distinct entry keeps
/// it appended to its key, which is the same split Go's index reader makes.
fn convert_handle(handle: &tidb_txnkv::Handle) -> TableHandle {
    match handle.int_value() {
        Some(value) => TableHandle::Int(value),
        None => TableHandle::Common(handle.clone().encoded()),
    }
}

fn index_entry_handle(
    index: &KvIndex,
    key: &[u8],
    value: &[u8],
    common: bool,
) -> Result<TableHandle, KvTableError> {
    if index.unique {
        // Only a distinct entry stores the handle in the value; a unique index
        // holding NULLs writes non-distinct entries too, so fall through when
        // the value is the non-distinct marker.
        if value != *b"0" {
            let handle = decode_handle_in_index_value(value)
                .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
            return Ok(convert_handle(&handle));
        }
    }
    // The handle is appended to the key after the indexed values.
    let (_, rest) = cut_index_key(key, index.column_offsets.len())
        .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
    if common {
        return Ok(TableHandle::Common(rest.to_vec()));
    }
    let (_, handle) =
        tidb_codec::decode_one(rest).map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
    match handle {
        Datum::Int(value) => Ok(TableHandle::Int(value)),
        Datum::UInt(value) => Ok(TableHandle::Int(value as i64)),
        other => Err(KvTableError::Decode(format!(
            "an index key ended with {other:?} rather than a handle"
        ))),
    }
}

/// The value MySQL prints in a duplicate-key error: the indexed values joined
/// by `-`, as Go's `ErrKeyExists` formats them.
fn duplicate_value_text(index: &KvIndex, row: &[Datum]) -> String {
    index
        .column_offsets
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

/// The integer handle a record key encodes: the trailing big-endian-ordered
/// eight bytes `encode_row_key_with_handle` wrote.
fn decode_int_handle(key: &[u8]) -> Result<i64, KvTableError> {
    let tail: [u8; 8] = key
        .get(key.len().wrapping_sub(8)..)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| KvTableError::Decode("record key is too short for a handle".to_owned()))?;
    // The codec writes the handle sign-flipped so byte order matches numeric
    // order; `decode_int` is its inverse.
    Ok(i64::from_be_bytes(tail) ^ i64::MIN)
}

/// Scans a [`KvTable`]'s record range into chunks -- the storage-backed source
/// (Go's table reader over `tablecodec`, minus distsql/coprocessor).
pub struct TableScanExec {
    meta: ExecutorMeta,
    table: KvTable,
    emitted: bool,
}

impl TableScanExec {
    /// Builds a scan over `table`.
    #[must_use]
    pub fn new(meta: ExecutorMeta, table: KvTable) -> Self {
        TableScanExec {
            meta,
            table,
            emitted: false,
        }
    }
}

impl Executor for TableScanExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.emitted = false;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.emitted {
            return Ok(());
        }
        let rows = self
            .table
            .scan_rows()
            .map_err(|_| ExecError::Unsupported("table bytes failed to decode"))?;
        for row in &rows {
            for (c, value) in row.iter().enumerate() {
                req.append_datum(c, value);
            }
        }
        self.emitted = true;
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;

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
                },
                KvColumn {
                    name: "s".to_owned(),
                    id: 2,
                    field_type: varstr(),
                    default_value: None,
                    // A column present at CREATE TABLE has no pre-existing rows.
                    origin_default: None,
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
                t.insert_row(&[Datum::Int(i * 10), Datum::Bytes(b"x".to_vec())])
                    .unwrap(),
            );
        }
        let scanned: Vec<TableHandle> = t
            .scan_rows_with_handles()
            .unwrap()
            .into_iter()
            .map(|(handle, _)| handle)
            .collect();
        assert_eq!(scanned, handles);

        // A row written under an explicit handle round-trips too.
        t.update_row(&handles[1], &[Datum::Int(99), Datum::Bytes(b"y".to_vec())])
            .unwrap();
        let rows = t.scan_rows_with_handles().unwrap();
        assert_eq!(rows.len(), 3, "update replaced in place, it did not append");
        assert_eq!(rows[1].0, handles[1]);
        assert_eq!(rows[1].1[0], Datum::Int(99));

        t.delete_row(&handles[0]).unwrap();
        let after: Vec<TableHandle> = t
            .scan_rows_with_handles()
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
            t.insert_row(&[Datum::Int(i), Datum::Bytes(b"x".to_vec())])
                .unwrap();
        }
        // A row of the next table id, written into the same storage layout.
        let mut neighbour = KvTable::new(t.table_id + 1, t.columns.clone());
        neighbour
            .insert_row(&[Datum::Int(99), Datum::Bytes(b"y".to_vec())])
            .unwrap();

        let rows = t.scan_rows().unwrap();
        assert_eq!(
            rows.len(),
            3,
            "every handle including the largest is scanned"
        );
        assert_eq!(rows[2][0], Datum::Int(2));
        assert_eq!(neighbour.scan_rows().unwrap().len(), 1);
    }

    #[test]
    fn insert_encodes_real_bytes_and_scan_decodes() {
        let mut t = test_table();
        let mut s1 = Datum::Null;
        s1.set_bytes(b"hello".to_vec());
        t.insert_row(&[Datum::Int(7), s1.clone()]).unwrap();
        t.insert_row(&[Datum::Int(8), Datum::Null]).unwrap();
        assert_eq!(t.len(), 2);

        let rows = t.scan_rows().unwrap();
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
        t.insert_row(&[Datum::Int(1), s]).unwrap();

        let mut out_cols = Vec::new();
        for (i, ft) in [long(), varstr()].into_iter().enumerate() {
            let mut c = Column::new((i + 1) as i64, ft);
            c.index = i as i64;
            out_cols.push(c);
        }
        let mut scan = TableScanExec::new(ExecutorMeta::new(Schema::new(out_cols), 0, 4, 1024), t);
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
}
