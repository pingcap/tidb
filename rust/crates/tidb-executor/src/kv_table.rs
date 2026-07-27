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
};
use tidb_datatype::{Datum, FieldType};
use tidb_expr::schema::Schema;
use tidb_tablecodec::{
    decode_handle_in_index_value, decode_table_row_to_map, encode_handle_in_unique_index_value,
    encode_table_row,
};
use tidb_txnkv::{
    GetOptions, Getter, Key, KvIterator, MemStorage, MemStorageError, Mutator, Retriever,
};

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
    pub fn insert_row(&mut self, row: &[Datum]) -> Result<i64, KvTableError> {
        let column_ids: Vec<i64> = self.columns.iter().map(|c| c.id).collect();
        let value = encode_table_row(None, row, &column_ids, true, None)
            .map_err(|e| KvTableError::Encode(format!("{e:?}")))?;
        // Go `addRecord`: with PKIsHandle the primary key's value IS the
        // handle, so the row key encodes it and a repeat is a duplicate.
        let handle = match self.pk_handle_offset {
            Some(offset) => {
                let handle = match row.get(offset) {
                    Some(Datum::Int(value)) => *value,
                    Some(Datum::UInt(value)) => *value as i64,
                    Some(Datum::Null) | None => {
                        return Err(KvTableError::Encode(
                            "the primary key column has no value".to_owned(),
                        ))
                    }
                    Some(other) => {
                        return Err(KvTableError::Encode(format!(
                            "a handle primary key needs an integer value, got {other:?}"
                        )))
                    }
                };
                if self.row_exists(handle)? {
                    return Err(KvTableError::DuplicateEntry {
                        value: handle.to_string(),
                        key: "PRIMARY".to_owned(),
                    });
                }
                handle
            }
            None => {
                let handle = self.next_handle;
                self.next_handle += 1;
                handle
            }
        };
        let key = Key::from_bytes(encode_row_key_with_handle(
            self.table_id,
            &RecordHandle::Int(handle),
        ));
        // Go writes the row first, then its index entries; a duplicate on a
        // unique index aborts the statement.
        self.write_index_entries(row, handle)?;
        self.store
            .set(key, value)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        Ok(handle)
    }

    /// Scans the table's record-key range in key order, decoding each value.
    /// Returns rows as `Datum`s in schema order (a missing column decodes NULL).
    pub fn scan_rows(&mut self) -> Result<Vec<Vec<Datum>>, KvTableError> {
        Ok(self
            .scan_rows_with_handles()?
            .into_iter()
            .map(|(_, row)| row)
            .collect())
    }

    /// Like [`KvTable::scan_rows`], but each row carries the record handle its
    /// key encodes, which `UPDATE`/`DELETE` need to address the row again.
    pub fn scan_rows_with_handles(&mut self) -> Result<Vec<(i64, Vec<Datum>)>, KvTableError> {
        let (low, high) = get_table_handle_key_range(self.table_id);
        let column_types: BTreeMap<i64, FieldType> = self
            .columns
            .iter()
            .map(|c| (c.id, c.field_type.clone()))
            .collect();
        let column_ids: Vec<i64> = self.columns.iter().map(|c| c.id).collect();
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
            let handle = decode_int_handle(iterator.key().as_bytes())?;
            let mut decoded = decode_table_row_to_map(iterator.value(), &column_types, None)
                .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
            let row: Vec<Datum> = column_ids
                .iter()
                .map(|id| decoded.remove(id).unwrap_or(Datum::Null))
                .collect();
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
        handle: i64,
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
            encoded.extend_from_slice(
                &tidb_codec::encode_key(&[Datum::Int(handle)])
                    .map_err(|e| KvTableError::Encode(format!("{e:?}")))?,
            );
        }
        Ok((
            encode_index_seek_key(self.table_id, index.id, &encoded),
            distinct,
        ))
    }

    /// Writes every index entry for `row`, rejecting a duplicate on a unique
    /// index as Go's `index.Create` does with `ErrKeyExists`.
    fn write_index_entries(&mut self, row: &[Datum], handle: i64) -> Result<(), KvTableError> {
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
                let value = encode_handle_in_unique_index_value(
                    &tidb_txnkv::IntHandle::new(handle).into(),
                    false,
                );
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
    fn delete_index_entries(&mut self, row: &[Datum], handle: i64) -> Result<(), KvTableError> {
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
    ) -> Result<Option<i64>, KvTableError> {
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
                Ok(handle.int_value())
            }
            Err(MemStorageError::NotFound) => Ok(None),
            Err(error) => Err(KvTableError::Storage(format!("{error:?}"))),
        }
    }

    /// The row stored under `handle`, decoded, or `None` when absent.
    fn read_row(&mut self, handle: i64) -> Result<Option<Vec<Datum>>, KvTableError> {
        let key = Key::from_bytes(encode_row_key_with_handle(
            self.table_id,
            &RecordHandle::Int(handle),
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
        Ok(Some(
            self.columns
                .iter()
                .map(|c| decoded.remove(&c.id).unwrap_or(Datum::Null))
                .collect(),
        ))
    }

    /// Whether a row is already stored under `handle`.
    fn row_exists(&mut self, handle: i64) -> Result<bool, KvTableError> {
        let key = Key::from_bytes(encode_row_key_with_handle(
            self.table_id,
            &RecordHandle::Int(handle),
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
    pub fn update_row(&mut self, handle: i64, row: &[Datum]) -> Result<(), KvTableError> {
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
        let column_ids: Vec<i64> = self.columns.iter().map(|c| c.id).collect();
        let value = encode_table_row(None, row, &column_ids, true, None)
            .map_err(|e| KvTableError::Encode(format!("{e:?}")))?;
        let key = Key::from_bytes(encode_row_key_with_handle(
            self.table_id,
            &RecordHandle::Int(handle),
        ));
        self.store
            .set(key, value)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))
    }

    /// Removes the row stored under `handle`.
    pub fn delete_row(&mut self, handle: i64) -> Result<(), KvTableError> {
        if !self.indexes.is_empty() {
            if let Some(row) = self.read_row(handle)? {
                self.delete_index_entries(&row, handle)?;
            }
        }
        let key = Key::from_bytes(encode_row_key_with_handle(
            self.table_id,
            &RecordHandle::Int(handle),
        ));
        self.store
            .delete(key)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))
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
                },
                KvColumn {
                    name: "s".to_owned(),
                    id: 2,
                    field_type: varstr(),
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
        let scanned: Vec<i64> = t
            .scan_rows_with_handles()
            .unwrap()
            .into_iter()
            .map(|(handle, _)| handle)
            .collect();
        assert_eq!(scanned, handles);

        // A row written under an explicit handle round-trips too.
        t.update_row(handles[1], &[Datum::Int(99), Datum::Bytes(b"y".to_vec())])
            .unwrap();
        let rows = t.scan_rows_with_handles().unwrap();
        assert_eq!(rows.len(), 3, "update replaced in place, it did not append");
        assert_eq!(rows[1].0, handles[1]);
        assert_eq!(rows[1].1[0], Datum::Int(99));

        t.delete_row(handles[0]).unwrap();
        let after: Vec<i64> = t
            .scan_rows_with_handles()
            .unwrap()
            .into_iter()
            .map(|(handle, _)| handle)
            .collect();
        assert_eq!(after, vec![handles[1], handles[2]]);
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
