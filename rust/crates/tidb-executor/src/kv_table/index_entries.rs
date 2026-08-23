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

//! Index entries of a byte-backed table: the key one row produces for one
//! index, the entries a write adds and removes, and the point read a unique
//! index answers.
//!
//! Mirrors the index half of Go `pkg/tablecodec/tablecodec.go`
//! (`GenIndexKey`, `TruncateIndexValues`, `DecodeHandleInIndexValue`) as
//! `pkg/table/tables/index.go` drives it.
//!
//! Split out of [`super`] because it is one subject: every function here
//! turns row VALUES into an index KEY or reads a handle back out of one, and
//! the key-building half takes the session `time_zone` for the same reason
//! the row codec does -- Go's `GenIndexKey(enc, loc, ...)` converts a
//! `TIMESTAMP` key part to UTC before encoding it, so the entry a Shanghai
//! session files is the entry a UTC session seeks.

use tidb_codec::decode_table_id;
use tidb_codec::table_key::encode_index_seek_key;
use tidb_codec::Encoder;
use tidb_datatype::{is_bin_collation, Datum, SessionTimeZone};
use tidb_tablecodec::{
    cut_index_key, decode_handle_in_index_value, generate_index_value, index_kv_is_unique,
    IndexColumn as CodecIndexColumn, IndexInfo as CodecIndexInfo, TableColumn as CodecTableColumn,
    TableInfo as CodecTableInfo,
};
use tidb_txnkv::Key;

use crate::ddl::index_prefix::UNSPECIFIED_LENGTH;
use crate::storage::StorageError;

use super::{datum_text, KvIndex, KvTable, KvTableError, TableHandle};

pub(crate) struct IndexEntryForCheck {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) handle: TableHandle,
}

impl KvTable {
    /// Moves a stored raw value to a different key, constructing an index
    /// entry whose value still names the same row but whose indexed datum is
    /// wrong. Ordinary writes cannot create this corruption.
    pub fn move_raw_value_for_test(
        &mut self,
        old_key: &[u8],
        new_key: Vec<u8>,
    ) -> Result<(), KvTableError> {
        let old_key = tidb_txnkv::Key::from_bytes(old_key.to_vec());
        let value = self
            .store
            .get(&old_key)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        self.store
            .delete(old_key)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        self.store
            .set(tidb_txnkv::Key::from_bytes(new_key), value)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))
    }

    /// Every stored entry of one index, as `(entry key, the handle it names)`.
    pub fn index_entries_for_check(
        &mut self,
        index_id: i64,
    ) -> Result<Vec<(Vec<u8>, TableHandle)>, KvTableError> {
        Ok(self
            .index_entry_records_for_check(index_id)?
            .into_iter()
            .map(|entry| (entry.key, entry.handle))
            .collect())
    }

    pub(crate) fn index_entry_records_for_check(
        &mut self,
        index_id: i64,
    ) -> Result<Vec<IndexEntryForCheck>, KvTableError> {
        let Some(index) = self
            .indexes
            .iter()
            .find(|index| index.id == index_id)
            .cloned()
        else {
            return Err(KvTableError::Decode("no such index".to_owned()));
        };
        let common = !self.common_handle_offsets().is_empty();
        let mut entries = Vec::new();
        for physical_id in self.record_physical_ids() {
            let (low, high) = crate::admin_check::index_key_bounds(physical_id, index_id);
            let mut iterator = self
                .store
                .iter(Some(&Key::from_bytes(low)), Some(&Key::from_bytes(high)))
                .map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
            while iterator.valid() {
                let key = iterator.key().as_bytes().to_vec();
                let value = iterator.value().to_vec();
                let handle = index_entry_handle(&index, &key, &value, common)?;
                entries.push(IndexEntryForCheck { key, value, handle });
                iterator
                    .next()
                    .map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
            }
            iterator.close();
        }
        Ok(entries)
    }

    pub(crate) fn index_entry_values_for_check(
        &self,
        index: &KvIndex,
        key: &[u8],
        value: &[u8],
        zone: &SessionTimeZone,
    ) -> Result<Vec<Datum>, KvTableError> {
        self.codec_table_info()
            .decode_index_values_from_index(
                self.use_new_collation,
                Some(zone),
                &self.codec_index_info(index),
                key,
                value,
            )
            .map_err(|error| KvTableError::Decode(format!("{error:?}")))
    }

    pub(crate) fn index_values_for_check(&self, index: &KvIndex, row: &[Datum]) -> Vec<Datum> {
        self.index_values(index, row)
    }

    /// Swaps the stored values of two raw keys without changing either key.
    ///
    /// This constructs the unique-index corruption where both expected keys
    /// exist but each names the other row. Ordinary writes cannot create that
    /// state because index maintenance updates keys and values atomically.
    pub fn swap_raw_values_for_test(
        &mut self,
        left: &[u8],
        right: &[u8],
    ) -> Result<(), KvTableError> {
        let left_key = tidb_txnkv::Key::from_bytes(left.to_vec());
        let right_key = tidb_txnkv::Key::from_bytes(right.to_vec());
        let left_value = self
            .store
            .get(&left_key)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        let right_value = self
            .store
            .get(&right_key)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        self.store
            .set(left_key, right_value)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        self.store
            .set(right_key, left_value)
            .map_err(|e| KvTableError::Storage(format!("{e:?}")))
    }

    /// The values one index entry is built from: the indexed columns of
    /// `row`, each CUT to its key part's declared prefix (Go
    /// `tablecodec.TruncateIndexValues`, which `GenIndexKey` calls first).
    ///
    /// This is the only place the cut happens, which is what makes the key a
    /// write stores and the key a read seeks the same key by construction --
    /// and what makes a UNIQUE prefix index enforce uniqueness ON THE PREFIX,
    /// because two rows sharing it produce one key. Captured from real TiDB:
    /// with `unique key uidx(a(3))` holding `'abcdef'`, inserting `'abcxyz'`
    /// is rejected.
    pub(crate) fn index_values(&self, index: &KvIndex, row: &[Datum]) -> Vec<Datum> {
        index
            .column_offsets
            .iter()
            .enumerate()
            .map(|(position, offset)| {
                let mut value = row.get(*offset).cloned().unwrap_or(Datum::Null);
                if let Some(column) = self.columns.get(*offset) {
                    crate::index_prefix_cut::cut_index_value(
                        &mut value,
                        index.prefix_length(position),
                        &column.field_type,
                    );
                }
                value
            })
            .collect()
    }

    /// Go `GenIndexKey`: the entry key for one index over `row`, plus Go's
    /// `distinct` flag.
    ///
    /// `distinct` is true only for a unique index whose indexed values are all
    /// non-NULL -- MySQL lets a unique index hold any number of NULLs, so a
    /// NULL-bearing entry is stored the non-distinct way (handle appended to
    /// the key) and never collides.
    pub(crate) fn index_key(
        &self,
        index: &KvIndex,
        row: &[Datum],
        handle: &TableHandle,
        physical_id: i64,
        zone: &SessionTimeZone,
    ) -> Result<(Vec<u8>, bool), KvTableError> {
        let values = self.index_values(index, row);
        let distinct = index.unique && !values.contains(&Datum::Null);
        let encoder = Encoder::new(self.use_new_collation);
        let mut encoded = encoder
            .encode_key_in_timezone(zone, &values)
            .map_err(|e| KvTableError::Encode(format!("{e:?}")))?;
        if !distinct {
            // Go appends the handle so non-distinct entries stay unique.
            match handle {
                TableHandle::Int(value) => encoded.extend_from_slice(
                    &encoder
                        .encode_key(&[Datum::Int(*value)])
                        .map_err(|e| KvTableError::Encode(format!("{e:?}")))?,
                ),
                TableHandle::Common(bytes) => encoded.extend_from_slice(bytes),
            }
        }
        Ok((
            encode_index_seek_key(physical_id, index.id, &encoded),
            distinct,
        ))
    }

    /// The `tidb-tablecodec` projection of this table's metadata, which is
    /// what [`generate_index_value`] reads to decide an entry's FORMAT.
    ///
    /// Only the clustered PRIMARY KEY appears in `indices`: that list is read
    /// for exactly one purpose, `common_pk_restored_column_ids`, and the index
    /// being written is passed separately. This tier's own index list never
    /// holds a clustered primary key (`ddl::table_constraints` skips it, since
    /// its encoding IS the row key), so the entry has to be synthesised here.
    pub(in crate::kv_table) fn codec_table_info(&self) -> CodecTableInfo {
        let common_handle = !self.common_handle_offsets().is_empty();
        CodecTableInfo {
            columns: self
                .columns
                .iter()
                .enumerate()
                .map(|(offset, column)| CodecTableColumn {
                    id: column.id,
                    offset,
                    primary_key: column
                        .field_type
                        .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY),
                    field_type: column.field_type.clone(),
                    // A column type is only ever "changing" mid-way through an
                    // online MODIFY COLUMN, which this tier performs in one
                    // step; Go reads the same `nil` outside that window.
                    changing_field_type: None,
                })
                .collect(),
            indices: if common_handle {
                vec![CodecIndexInfo {
                    id: 0,
                    columns: self
                        .common_handle_offsets()
                        .iter()
                        .map(|offset| CodecIndexColumn {
                            offset: *offset,
                            // A clustered primary key may not declare a prefix
                            // (`ddl::index_prefix::clustered_prefix_unsupported`),
                            // so the whole column is always stored.
                            length: UNSPECIFIED_LENGTH,
                            use_changing_type: false,
                        })
                        .collect(),
                    unique: true,
                    global: false,
                    global_index_version: 0,
                    primary: true,
                }]
            } else {
                Vec::new()
            },
            pk_is_handle: self.pk_handle_offset().is_some(),
            is_common_handle: common_handle,
            // Go's DDL stamps every clustered common-handle table it creates
            // with version 1 (`pkg/ddl/create_table.go`), and version 0 exists
            // only for tables created before v5.0. Every table this tier holds
            // it created itself, so version 1 is the only reachable value.
            common_handle_version: u8::from(common_handle),
        }
    }

    /// The `tidb-tablecodec` view of one of this table's indexes.
    pub(in crate::kv_table) fn codec_index_info(&self, index: &KvIndex) -> CodecIndexInfo {
        CodecIndexInfo {
            id: index.id,
            columns: index
                .column_offsets
                .iter()
                .enumerate()
                .map(|(position, offset)| CodecIndexColumn {
                    offset: *offset,
                    length: index.prefix_length(position),
                    use_changing_type: false,
                })
                .collect(),
            unique: index.unique,
            // A global index only exists on a partitioned table's index that
            // spans partitions, which this tier does not yet build.
            global: false,
            global_index_version: 0,
            primary: index.name.eq_ignore_ascii_case("PRIMARY"),
        }
    }

    /// Go `tables.NeedRestoredData`: whether ANY of `index`'s key parts stores
    /// a column whose new-collation sort key loses the original bytes.
    fn index_needs_restored_data(&self, index: &KvIndex) -> bool {
        index.column_offsets.iter().any(|offset| {
            self.columns.get(*offset).is_some_and(|column| {
                column
                    .field_type
                    .need_restored_data_with_collation(self.use_new_collation)
            })
        })
    }

    /// Go `tables.TryGetHandleRestoredDataWrapper`: the clustered PRIMARY
    /// KEY's own restored data, which a version-1 common-handle table repeats
    /// in EVERY secondary index entry so an index-only read can rebuild the
    /// handle columns as well as the indexed ones.
    ///
    /// Go's `TryTruncateRestoredData` is absent because it cannot bite here: a
    /// clustered primary key may not declare a prefix, so the truncation
    /// target is always the whole column.
    fn handle_restored_data(&self, row: &[Datum]) -> Vec<Datum> {
        self.common_handle_offsets()
            .iter()
            .filter_map(|offset| {
                let column = self.columns.get(*offset)?;
                if !column
                    .field_type
                    .need_restored_data_with_collation(self.use_new_collation)
                {
                    return None;
                }
                let value = row.get(*offset).cloned().unwrap_or(Datum::Null);
                // Go `ConvertDatumToTailSpaceCount`: a bin collation's sort
                // key differs from the data only by the trailing spaces it
                // trimmed, so the COUNT restores it and the string would be a
                // second copy of the key.
                if is_bin_collation(column.field_type.collation_name()) {
                    return Some(Datum::Int(trailing_spaces(&value) as i64));
                }
                Some(value)
            })
            .collect()
    }

    /// Go `index.GenIndexValue` -> `tablecodec.GenIndexValuePortal`: the bytes
    /// one index entry stores.
    ///
    /// This exists because the entry value is not merely "the handle, or a
    /// marker": for any indexed column under a new collation the key is a
    /// LOSSY sort key, and the restored data in this value is the only place
    /// the original bytes survive. A writer that stores its own simpler value
    /// is self-consistent and still feeds a Go reader -- an index-only scan,
    /// `ADMIN CHECK INDEX`, a DDL backfill -- case-folded or space-stripped
    /// data.
    pub(in crate::kv_table) fn index_entry_value(
        &self,
        index: &KvIndex,
        row: &[Datum],
        handle: &TableHandle,
        distinct: bool,
        zone: &SessionTimeZone,
    ) -> Result<Vec<u8>, KvTableError> {
        let handle = match handle {
            TableHandle::Int(value) => tidb_txnkv::IntHandle::new(*value).into(),
            TableHandle::Common(bytes) => tidb_txnkv::CommonHandle::new(bytes.clone())
                .map_err(|e| KvTableError::Encode(format!("{e:?}")))?
                .into(),
        };
        generate_index_value(
            self.use_new_collation,
            Some(zone),
            &self.codec_table_info(),
            &self.codec_index_info(index),
            self.index_needs_restored_data(index),
            distinct,
            // Go's `untouched` marks an entry a pessimistic transaction
            // touched without changing; this tier writes only committed
            // entries.
            false,
            &self.index_values(index, row),
            &handle,
            // A partition id is only carried by a global index.
            0,
            &self.handle_restored_data(row),
        )
        .map_err(|e| KvTableError::Encode(format!("{e:?}")))
    }

    /// Writes every index entry for `row`, rejecting a duplicate on a unique
    /// index as Go's `index.Create` does with `ErrKeyExists`.
    pub(in crate::kv_table) fn write_index_entries(
        &mut self,
        row: &[Datum],
        handle: &TableHandle,
        physical_id: i64,
        zone: &SessionTimeZone,
    ) -> Result<(), KvTableError> {
        let indexes = self.indexes.clone();
        for index in indexes.iter() {
            let (key, distinct) = self.index_key(index, row, handle, physical_id, zone)?;
            let value = self.index_entry_value(index, row, handle, distinct, zone)?;
            let key = Key::from_bytes(key);
            if distinct && self.store.get(&key).is_ok() {
                return Err(KvTableError::DuplicateEntry {
                    value: duplicate_value_text(&self.index_values(index, row)),
                    key: self.qualified_key(&index.name),
                });
            }
            self.store
                .set(key, value)
                .map_err(|e| KvTableError::Storage(format!("{e:?}")))?;
        }
        Ok(())
    }

    /// Removes every index entry for `row`.
    pub(in crate::kv_table) fn delete_index_entries(
        &mut self,
        row: &[Datum],
        handle: &TableHandle,
        physical_id: i64,
        zone: &SessionTimeZone,
    ) -> Result<(), KvTableError> {
        let indexes = self.indexes.clone();
        for index in indexes.iter() {
            let (key, _) = self.index_key(index, row, handle, physical_id, zone)?;
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
        zone: &SessionTimeZone,
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
        // A prefix index's entry is filed under a CUT value, so finding one
        // does not prove the row matches: `'abcxyz'` and `'abcdef'` share the
        // key of `uidx(a(3))`. Go declines the plan outright
        // (`point_get_plan.go`'s `idxInfo.HasPrefixIndex()`); declining the
        // LOOKUP as well means no caller can reach a wrong row through this
        // door even if a future planner forgets the rule.
        if index.has_prefix() {
            return Ok(None);
        }
        let encoded = Encoder::new(self.use_new_collation)
            .encode_key_in_timezone(zone, values)
            .map_err(|e| KvTableError::Encode(format!("{e:?}")))?;
        for physical_id in self.record_physical_ids() {
            let key = Key::from_bytes(encode_index_seek_key(physical_id, index.id, &encoded));
            match self.store.get(&key) {
                Ok(entry) => {
                    let handle = decode_handle_in_index_value(&entry)
                        .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
                    return Ok(Some(convert_handle(&handle)));
                }
                Err(StorageError::NotFound) => {}
                Err(error) => return Err(KvTableError::Storage(format!("{error:?}"))),
            }
        }
        Ok(None)
    }

    pub(in crate::kv_table) fn stored_physical_id(
        &mut self,
        handle: &TableHandle,
    ) -> Result<Option<i64>, KvTableError> {
        Ok(self
            .stored_record_key(handle)?
            .map(|key| decode_table_id(key.as_bytes())))
    }
}

/// Go `stringutil.GetTailSpaceCount`: how many trailing spaces a bin
/// collation's sort key dropped.
fn trailing_spaces(value: &Datum) -> usize {
    value
        .as_raw_bytes()
        .map(|bytes| bytes.iter().rev().take_while(|byte| **byte == b' ').count())
        .unwrap_or(0)
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

pub(in crate::kv_table) fn index_entry_handle(
    index: &KvIndex,
    key: &[u8],
    value: &[u8],
    common: bool,
) -> Result<TableHandle, KvTableError> {
    // Only a DISTINCT entry stores the handle in the value, and the value's
    // own shape says whether it is one -- Go asks `tablecodec.IndexKVIsUnique`
    // rather than comparing against a marker byte, because a non-distinct
    // entry's value is not always that byte: a restored-data index stores a
    // rowcodec payload, and a version-1 common-handle table stores its three
    // version bytes.
    if index.unique && index_kv_is_unique(value) {
        let handle = decode_handle_in_index_value(value)
            .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
        return Ok(convert_handle(&handle));
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

/// The `Duplicate entry '...'` value MySQL prints, from the values the ENTRY
/// holds.
///
/// Go builds it the same way -- `tables.go`'s `TruncateIndexValues` then
/// `genIndexKeyStrs` -- so a unique prefix index reports the CUT value:
/// `'abc'`, not the `'abcxyz'` that was offered.
pub(in crate::kv_table) fn duplicate_value_text(values: &[Datum]) -> String {
    values.iter().map(datum_text).collect::<Vec<_>>().join("-")
}
