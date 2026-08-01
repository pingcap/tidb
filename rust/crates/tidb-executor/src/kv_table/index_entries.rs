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

use tidb_codec::table_key::encode_index_seek_key;
use tidb_datatype::{Datum, SessionTimeZone};
use tidb_tablecodec::{
    cut_index_key, decode_handle_in_index_value, encode_handle_in_unique_index_value,
};
use tidb_txnkv::Key;

use crate::storage::StorageError;

use super::{datum_text, KvIndex, KvTable, KvTableError, TableHandle};

impl KvTable {
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
        zone: &SessionTimeZone,
    ) -> Result<(Vec<u8>, bool), KvTableError> {
        let values = self.index_values(index, row);
        let distinct = index.unique && !values.contains(&Datum::Null);
        let mut encoded = tidb_codec::encode_key_in_timezone(zone, &values)
            .map_err(|e| KvTableError::Encode(format!("{e:?}")))?;
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
    pub(in crate::kv_table) fn write_index_entries(
        &mut self,
        row: &[Datum],
        handle: &TableHandle,
        zone: &SessionTimeZone,
    ) -> Result<(), KvTableError> {
        let indexes = self.indexes.clone();
        for index in &indexes {
            let (key, distinct) = self.index_key(index, row, handle, zone)?;
            let key = Key::from_bytes(key);
            if distinct {
                if self.store.get(&key).is_ok() {
                    return Err(KvTableError::DuplicateEntry {
                        value: duplicate_value_text(&self.index_values(index, row)),
                        key: self.qualified_key(&index.name),
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
    pub(in crate::kv_table) fn delete_index_entries(
        &mut self,
        row: &[Datum],
        handle: &TableHandle,
        zone: &SessionTimeZone,
    ) -> Result<(), KvTableError> {
        let indexes = self.indexes.clone();
        for index in &indexes {
            let (key, _) = self.index_key(index, row, handle, zone)?;
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
        let encoded = tidb_codec::encode_key_in_timezone(zone, values)
            .map_err(|e| KvTableError::Encode(format!("{e:?}")))?;
        let key = Key::from_bytes(encode_index_seek_key(self.table_id, index.id, &encoded));
        match self.store.get(&key) {
            Ok(entry) => {
                let handle = decode_handle_in_index_value(&entry)
                    .map_err(|e| KvTableError::Decode(format!("{e:?}")))?;
                Ok(Some(convert_handle(&handle)))
            }
            Err(StorageError::NotFound) => Ok(None),
            Err(error) => Err(KvTableError::Storage(format!("{error:?}"))),
        }
    }
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

/// The `Duplicate entry '...'` value MySQL prints, from the values the ENTRY
/// holds.
///
/// Go builds it the same way -- `tables.go`'s `TruncateIndexValues` then
/// `genIndexKeyStrs` -- so a unique prefix index reports the CUT value:
/// `'abc'`, not the `'abcxyz'` that was offered.
pub(in crate::kv_table) fn duplicate_value_text(values: &[Datum]) -> String {
    values.iter().map(datum_text).collect::<Vec<_>>().join("-")
}
