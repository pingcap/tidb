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

//! One batch of Go's transactional ADD INDEX reorganization, over this
//! table's storage bound to the batch's own transaction:
//! `addIndexTxnWorker.BackfillData` (`pkg/ddl/index.go`) writes the entries
//! of a range of rows into the index, and `mergeIndexWorker.BackfillData`
//! (`pkg/ddl/index_merge_tmp.go`) replays a range of the temporary index
//! into it. `getTableRange` (`pkg/ddl/reorg.go`) bounds the rows.
//!
//! The table is built with the index being added as an ordinary member, so
//! its keys are the index's own (Go's `FromBackfill` writes the real key).

use std::collections::HashMap;

use tidb_txnkv::Key;

use tidb_table::kv_table::index_entries::{convert_handle, duplicate_value_text};
use tidb_table::kv_table::row_decoder::RowDecoder;
use tidb_table::kv_table::{KvIndex, KvTable, KvTableError, RowDecodeContext, TableHandle};

/// Go `TempIndexKeyTypeMerge`: a double write made after the backfill.
const TEMP_INDEX_KEY_TYPE_MERGE: u8 = b'm';
/// Go `TempIndexKeyTypeDelete`: written in the delete-only state.
const TEMP_INDEX_KEY_TYPE_DELETE: u8 = b'd';

pub use crate::backfilling::ReorgBatch;

/// Go `kv.Key.Next`.
/// The decoder Go's `fetchRowColVals` decodes a batch's records with: every
/// column of the table's current shape.
pub(crate) fn reorg_row_decoder(
    table: &KvTable,
    context: &RowDecodeContext,
) -> Result<RowDecoder, KvTableError> {
    RowDecoder::for_table_read(
        table.columns.clone(),
        table.pk_handle_offset,
        table.common_handle_offsets.clone(),
        table.common_handle_prefix_lengths.clone(),
        None,
        table.use_new_collation,
        context.clone(),
    )
}

fn key_next(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0);
    next
}

/// Go's DDL backfill (`addIndexTxnWorker.BackfillData`, the column and temp-index workers) over a [`KvTable`].
pub trait KvTableBackfill {
    /// Go `getTableRange`: the first record key of `physical_id` and the key
    /// past its largest handle, at this storage's snapshot. `None` for an
    /// empty physical table.
    fn physical_record_range(
        &mut self,
        physical_id: i64,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, KvTableError>;

    /// Go `fetchRowColVals` over `[start, end)` (`iterateSnapshotKeys`): at
    /// most `batch_count` decoded rows, with the batch's next key and whether
    /// the range is exhausted (`getNextKey`).
    #[allow(clippy::type_complexity)]
    fn fetch_reorg_rows(
        &mut self,
        start: &[u8],
        end: &[u8],
        batch_count: usize,
        decode_context: &RowDecodeContext,
    ) -> Result<
        (
            Vec<(Vec<u8>, TableHandle, Vec<tidb_datatype::Datum>)>,
            ReorgBatch,
        ),
        KvTableError,
    >;

    /// Go `cleanUpIndexWorker.BackfillData` for one batch of a dropped
    /// partition `physical_id`'s rows in `[start, end)`: each row's entry in
    /// every global index of `index_ids` is deleted, unless a unique entry
    /// now names a row of another partition (one written after the drop).
    /// Go also locks a unique entry it deletes; the delete is this
    /// transaction's write of the key, which conflicts with a concurrent
    /// writer of it just the same.
    fn clean_up_global_index_batch(
        &mut self,
        index_ids: &[i64],
        physical_id: i64,
        start: &[u8],
        end: &[u8],
        batch_count: usize,
        decode_context: &RowDecodeContext,
    ) -> Result<ReorgBatch, KvTableError>;

    /// Go `addIndexTxnWorker.BackfillData` for one batch of `physical_id`'s
    /// rows in `[start, end)`: at most `batch_count` rows decoded
    /// (`fetchRowColVals`), the unique ones checked against what the index
    /// already holds (`batchCheckUniqueKey`: an entry naming the same row is
    /// skipped, another row's is a duplicate), and every other entry written.
    fn backfill_index_batch(
        &mut self,
        index_id: i64,
        physical_id: i64,
        start: &[u8],
        end: &[u8],
        batch_count: usize,
        decode_context: &RowDecodeContext,
    ) -> Result<ReorgBatch, KvTableError>;

    /// Go `mergeIndexWorker.BackfillData` for one batch of the temporary
    /// index range `[start, end)` of `index_id` (`fetchTempIndexVals`,
    /// `batchCheckTemporaryUniqueKey`): each surviving operation is replayed
    /// on the index key and the temporary key removed.
    fn merge_temp_index_batch(
        &mut self,
        index_id: i64,
        physical_id: i64,
        start: &[u8],
        end: &[u8],
        batch_count: usize,
    ) -> Result<ReorgBatch, KvTableError>;

    /// Go `tablecodec.EncodeRecordKey` of `handle` under `physical_id`.
    fn record_key_of(&self, physical_id: i64, handle: &TableHandle) -> Vec<u8>;

    /// Go `ExtractKeyExistsErrFromIndex`'s value text: the index key's
    /// values.
    fn duplicate_text_of_key(&self, index: &KvIndex, key: &[u8]) -> String;

    /// Go `updateColumnWorker.BackfillData`: one batch of a MODIFY COLUMN's
    /// row rewrite over `[start, end)`. Each row not yet carrying the
    /// changing column (DML writes it once the column exists) gets the old
    /// column's value cast into it (`getRowRecord`) and is written back. The
    /// changing column is the non-public column at `changing`; the old one
    /// the public column at `old_offset`.
    ///
    /// A cast failure is the job's error (`reformatErrors`); a warning is
    /// counted by code. `cast_context` is the job's reorganization context.
    #[allow(clippy::too_many_arguments)]
    fn update_column_batch(
        &mut self,
        old_offset: usize,
        changing: usize,
        start: &[u8],
        end: &[u8],
        batch_count: usize,
        decode_context: &RowDecodeContext,
        cast_context: &dyn tidb_table::context::MutateContext,
    ) -> Result<ReorgBatch, KvTableError>;
}

impl KvTableBackfill for KvTable {
    fn physical_record_range(
        &mut self,
        physical_id: i64,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, KvTableError> {
        let prefix = tidb_codec::gen_table_record_prefix(physical_id);
        let upper = Key::from_bytes(prefix.clone()).prefix_next();
        let start = {
            let mut iter = self
                .store
                .iter(Some(&Key::from_bytes(prefix.clone())), Some(&upper))?;
            let first = iter.valid().then(|| iter.key().as_bytes().to_vec());
            iter.close();
            first
        };
        let Some(start) = start else {
            return Ok(None);
        };
        let last = {
            let mut iter = self
                .store
                .iter_reverse(Some(&upper), Some(&Key::from_bytes(prefix)))?;
            let last = iter.valid().then(|| iter.key().as_bytes().to_vec());
            iter.close();
            last
        };
        let end = last.map_or_else(|| key_next(&start), |last| key_next(&last));
        Ok(Some((start, end)))
    }

    #[allow(clippy::type_complexity)]
    fn fetch_reorg_rows(
        &mut self,
        start: &[u8],
        end: &[u8],
        batch_count: usize,
        decode_context: &RowDecodeContext,
    ) -> Result<
        (
            Vec<(Vec<u8>, TableHandle, Vec<tidb_datatype::Datum>)>,
            ReorgBatch,
        ),
        KvTableError,
    > {
        let decoder = reorg_row_decoder(self, decode_context)?;
        let mut rows: Vec<(Vec<u8>, TableHandle, Vec<tidb_datatype::Datum>)> = Vec::new();
        let mut task_done = false;
        {
            let mut iter = self.store.iter(
                Some(&Key::from_bytes(start.to_vec())),
                Some(&Key::from_bytes(end.to_vec())),
            )?;
            loop {
                if !iter.valid() {
                    task_done = true;
                    break;
                }
                if rows.len() >= batch_count {
                    break;
                }
                let key = iter.key().as_bytes().to_vec();
                if key.as_slice() >= end {
                    task_done = true;
                    break;
                }
                let (handle, row) = decoder.decode_record(&key, iter.value())?;
                rows.push((key, handle, row));
                iter.next()?;
            }
            iter.close();
        }
        if rows.is_empty() {
            task_done = true;
        }
        let next_key = if task_done {
            end.to_vec()
        } else {
            key_next(&rows.last().expect("a full batch has rows").0)
        };
        let batch = ReorgBatch {
            next_key,
            done: task_done,
            ..ReorgBatch::default()
        };
        Ok((rows, batch))
    }

    fn clean_up_global_index_batch(
        &mut self,
        index_ids: &[i64],
        physical_id: i64,
        start: &[u8],
        end: &[u8],
        batch_count: usize,
        decode_context: &RowDecodeContext,
    ) -> Result<ReorgBatch, KvTableError> {
        let indexes = index_ids
            .iter()
            .map(|index_id| {
                self.indexes
                    .iter()
                    .find(|index| index.id == *index_id)
                    .cloned()
                    .ok_or_else(|| {
                        KvTableError::Decode(format!("index {index_id} is not on the table"))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let zone = decode_context.zone().clone();
        let (rows, mut batch) = self.fetch_reorg_rows(start, end, batch_count, decode_context)?;
        for (_, handle, row) in &rows {
            for index in &indexes {
                batch.scanned += 1;
                for entry in self.index_entry_keys(index, row, handle, physical_id, &zone)? {
                    let key = Key::from_bytes(entry.key.clone());
                    if entry.distinct {
                        match self.store.get(&key) {
                            Ok(value) if !value.is_empty() => {
                                let stored = tidb_tablecodec::decode_handle_in_index_value(&value)
                                    .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                                if let Some(tidb_txnkv::Handle::Partition(stored)) = &stored {
                                    if stored.partition_id() != physical_id {
                                        continue;
                                    }
                                }
                            }
                            Ok(_) | Err(tidb_kv::storage::StorageError::NotFound) => {}
                            Err(error) => return Err(error.into()),
                        }
                    }
                    self.store.delete(key)?;
                }
                batch.added += 1;
            }
        }
        Ok(batch)
    }

    fn backfill_index_batch(
        &mut self,
        index_id: i64,
        physical_id: i64,
        start: &[u8],
        end: &[u8],
        batch_count: usize,
        decode_context: &RowDecodeContext,
    ) -> Result<ReorgBatch, KvTableError> {
        let index = self
            .indexes
            .iter()
            .find(|index| index.id == index_id)
            .cloned()
            .ok_or_else(|| KvTableError::Decode(format!("index {index_id} is not on the table")))?;
        let zone = decode_context.zone().clone();
        let (rows, mut batch) = self.fetch_reorg_rows(start, end, batch_count, decode_context)?;
        // Go `batchCheckUniqueKey`: the batch's distinct keys read once; a key
        // the batch itself files counts for the rows after it.
        let mut filed: HashMap<Vec<u8>, TableHandle> = HashMap::new();
        for (row_key, handle, row) in &rows {
            batch.scanned += 1;
            let entries = self.index_entry_keys(&index, row, handle, physical_id, &zone)?;
            let mut skip = index.unique;
            let mut pending = Vec::with_capacity(entries.len());
            for entry in entries {
                if index.unique {
                    let existing = match filed.get(&entry.key) {
                        Some(owner) => Some(owner.clone()),
                        None => match self.store.get(&Key::from_bytes(entry.key.clone())) {
                            Ok(value) if !value.is_empty() => {
                                if entry.distinct {
                                    let stored =
                                        tidb_tablecodec::decode_handle_in_index_value(&value)
                                            .map_err(|error| {
                                                KvTableError::Decode(format!("{error:?}"))
                                            })?
                                            .ok_or_else(|| {
                                                KvTableError::Decode(
                                                    "index value contains no handle".to_owned(),
                                                )
                                            })?;
                                    Some(convert_handle(&stored))
                                } else {
                                    Some(handle.clone())
                                }
                            }
                            Ok(_) | Err(tidb_kv::storage::StorageError::NotFound) => None,
                            Err(error) => return Err(error.into()),
                        },
                    };
                    match existing {
                        Some(owner) if entry.distinct && owner != *handle => {
                            return Err(KvTableError::DuplicateEntry {
                                value: duplicate_value_text(&self.index_values(&index, row)),
                                key: self.qualified_key(&index.name),
                            });
                        }
                        Some(_) => {}
                        None => {
                            skip = false;
                            if entry.distinct {
                                filed.insert(entry.key.clone(), handle.clone());
                            }
                        }
                    }
                }
                pending.push(entry);
            }
            // An entry already filed for this row needs nothing; the DML
            // that filed it maintains it from here.
            if skip {
                continue;
            }
            batch.locks.push(row_key.clone());
            for entry in pending {
                let value = self.index_entry_value(
                    &index,
                    &entry.values,
                    row,
                    handle,
                    entry.distinct,
                    physical_id,
                    &zone,
                )?;
                self.store.set(Key::from_bytes(entry.key), value)?;
            }
            batch.added += 1;
        }
        Ok(batch)
    }

    fn merge_temp_index_batch(
        &mut self,
        index_id: i64,
        physical_id: i64,
        start: &[u8],
        end: &[u8],
        batch_count: usize,
    ) -> Result<ReorgBatch, KvTableError> {
        let index = self
            .indexes
            .iter()
            .find(|index| index.id == index_id)
            .cloned()
            .ok_or_else(|| KvTableError::Decode(format!("index {index_id} is not on the table")))?;
        struct TempRecord {
            temp_key: Vec<u8>,
            origin_key: Vec<u8>,
            value: Vec<u8>,
            handle: TableHandle,
            delete: bool,
            distinct: bool,
            skip: bool,
        }
        let mut records: Vec<TempRecord> = Vec::new();
        let mut last_key: Option<Vec<u8>> = None;
        let mut scanned = 0_u64;
        {
            let mut iter = self.store.iter(
                Some(&Key::from_bytes(start.to_vec())),
                Some(&Key::from_bytes(end.to_vec())),
            )?;
            while iter.valid() {
                let key = iter.key().as_bytes().to_vec();
                if key.as_slice() >= end || records.len() >= batch_count {
                    break;
                }
                let elements = tidb_tablecodec::decode_temp_index_value(iter.value())
                    .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                scanned += elements.len() as u64;
                let elements = tidb_tablecodec::filter_overwritten_temp_index_values(elements);
                let mut origin_key = key.clone();
                tidb_tablecodec::temp_index_key_to_index_key(&mut origin_key)
                    .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                for element in elements {
                    if element.key_version == TEMP_INDEX_KEY_TYPE_MERGE
                        || element.key_version == TEMP_INDEX_KEY_TYPE_DELETE
                    {
                        continue;
                    }
                    // Go `decodeTempIndexHandleFromIndexKV`.
                    let handle = match &element.handle {
                        Some(handle) => convert_handle(handle),
                        None => convert_handle(
                            &tidb_tablecodec::decode_index_handle(
                                &key,
                                &element.value,
                                index.column_offsets.len(),
                            )
                            .map_err(|error| KvTableError::Decode(format!("{error:?}")))?
                            .ok_or_else(|| {
                                KvTableError::Decode("temporary entry names no row".to_owned())
                            })?,
                        ),
                    };
                    let distinct =
                        !element.delete && tidb_tablecodec::index_kv_is_unique(&element.value);
                    records.push(TempRecord {
                        temp_key: key.clone(),
                        origin_key: origin_key.clone(),
                        value: if element.delete {
                            Vec::new()
                        } else {
                            element.value.clone()
                        },
                        handle,
                        delete: element.delete,
                        distinct,
                        skip: false,
                    });
                }
                last_key = Some(key);
                iter.next()?;
            }
            iter.close();
        }
        let done = records.is_empty();
        let next_key = if done {
            end.to_vec()
        } else {
            key_next(last_key.as_deref().expect("records came from a key"))
        };
        // Go `batchCheckTemporaryUniqueKey`.
        if index.unique {
            let mut seen: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
            for record in &mut records {
                let original = match seen.get(&record.origin_key) {
                    Some(value) => value.clone(),
                    None => match self.store.get(&Key::from_bytes(record.origin_key.clone())) {
                        Ok(value) if !value.is_empty() => Some(value),
                        Ok(_) | Err(tidb_kv::storage::StorageError::NotFound) => None,
                        Err(error) => return Err(error.into()),
                    },
                };
                match original {
                    Some(original) => {
                        // Go `checkTempIndexKey`.
                        if !record.delete {
                            if record.distinct && original != record.value {
                                return Err(KvTableError::DuplicateEntry {
                                    value: self.duplicate_text_of_key(&index, &record.origin_key),
                                    key: self.qualified_key(&index.name),
                                });
                            }
                            record.skip = true;
                        } else if tidb_tablecodec::index_kv_is_unique(&original) {
                            let in_value = tidb_tablecodec::decode_handle_in_index_value(&original)
                                .map_err(|error| KvTableError::Decode(format!("{error:?}")))?
                                .map(|handle| convert_handle(&handle));
                            if in_value.as_ref() == Some(&record.handle) {
                                // The delete matches: drop it from the check.
                                seen.insert(record.origin_key.clone(), None);
                            } else if let Some(owner) = in_value {
                                // Several changes hit the key: delete only when
                                // the row the index names is gone.
                                let row_key = self.record_key_of(physical_id, &owner);
                                let exists = match self.store.get(&Key::from_bytes(row_key)) {
                                    Ok(value) => !value.is_empty(),
                                    Err(tidb_kv::storage::StorageError::NotFound) => false,
                                    Err(error) => return Err(error.into()),
                                };
                                record.skip = exists;
                            }
                        }
                    }
                    None if record.distinct => {
                        seen.insert(record.origin_key.clone(), Some(record.value.clone()));
                    }
                    None => {}
                }
            }
        }
        let mut batch = ReorgBatch {
            next_key,
            done,
            scanned,
            ..ReorgBatch::default()
        };
        for record in records {
            if record.skip {
                continue;
            }
            let origin = Key::from_bytes(record.origin_key);
            if record.delete {
                self.store.delete(origin)?;
            } else {
                self.store.set(origin, record.value)?;
            }
            self.store.delete(Key::from_bytes(record.temp_key))?;
            batch.added += 1;
        }
        Ok(batch)
    }

    fn record_key_of(&self, physical_id: i64, handle: &TableHandle) -> Vec<u8> {
        let handle = match handle {
            TableHandle::Int(value) => tidb_codec::table_key::RecordHandle::Int(*value),
            TableHandle::Common(bytes) => {
                tidb_codec::table_key::RecordHandle::Common(bytes.clone())
            }
        };
        tidb_codec::table_key::encode_row_key_with_handle(physical_id, &handle)
    }

    fn duplicate_text_of_key(&self, index: &KvIndex, key: &[u8]) -> String {
        tidb_tablecodec::cut_index_key(key, index.column_offsets.len())
            .ok()
            .map(|(values, _)| {
                values
                    .iter()
                    .map(|encoded| {
                        tidb_codec::decode_one(encoded)
                            .map(|(_, datum)| tidb_table::kv_table::datum_text(&datum))
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>()
                    .join("-")
            })
            .unwrap_or_default()
    }

    #[allow(clippy::too_many_arguments)]
    fn update_column_batch(
        &mut self,
        old_offset: usize,
        changing: usize,
        start: &[u8],
        end: &[u8],
        batch_count: usize,
        decode_context: &RowDecodeContext,
        cast_context: &dyn tidb_table::context::MutateContext,
    ) -> Result<ReorgBatch, KvTableError> {
        let target = self
            .nonpublic_columns
            .get(changing)
            .ok_or_else(|| {
                KvTableError::Decode("the changing column is not on the table".to_owned())
            })?
            .column
            .clone();
        let old_name = self
            .columns
            .get(old_offset)
            .ok_or_else(|| {
                KvTableError::Decode("the modified column is not on the table".to_owned())
            })?
            .name
            .clone();
        let zone = decode_context.zone().clone();
        let decoder = reorg_row_decoder(self, decode_context)?;
        let types: std::collections::BTreeMap<i64, tidb_datatype::FieldType> = self
            .nonpublic_columns
            .iter()
            .map(|column| (column.column.id, column.column.field_type.clone()))
            .collect();
        let mut rows = Vec::new();
        let mut task_done = false;
        {
            let mut iter = self.store.iter(
                Some(&Key::from_bytes(start.to_vec())),
                Some(&Key::from_bytes(end.to_vec())),
            )?;
            loop {
                if !iter.valid() {
                    task_done = true;
                    break;
                }
                if rows.len() >= batch_count {
                    break;
                }
                let key = iter.key().as_bytes().to_vec();
                if key.as_slice() >= end {
                    task_done = true;
                    break;
                }
                rows.push((key, iter.value().to_vec()));
                iter.next()?;
            }
            iter.close();
        }
        if rows.is_empty() {
            task_done = true;
        }
        let next_key = if task_done {
            end.to_vec()
        } else {
            key_next(&rows.last().expect("a full batch has rows").0)
        };
        let mut batch = ReorgBatch {
            next_key,
            done: task_done,
            ..ReorgBatch::default()
        };
        // Go `null` -> NOT NULL TIMESTAMP: the current timestamp stands in.
        let substitute = (self.columns[old_offset].field_type.code()
            != tidb_datatype::FieldTypeCode::Timestamp)
            .then(|| tidb_table::kv_table::null_timestamp_substitute(&target.field_type, cast_context))
            .flatten();
        let flags = cast_context.reorg_default_conversion_flags();
        let statement_warnings = cast_context.take_warnings();
        let mut warnings: Vec<(u16, String, i64)> = Vec::new();
        let mut failure = None;
        for (key, value) in rows {
            batch.scanned += 1;
            let stored = tidb_tablecodec::decode_table_row_to_map(&value, &types, Some(&zone))
                .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
            // Go: "The column is already added by update or insert
            // statement, skip it."
            if stored.contains_key(&target.id) {
                continue;
            }
            let (handle, public_row) = decoder.decode_record(&key, &value)?;
            let mut old_value = public_row
                .get(old_offset)
                .cloned()
                .unwrap_or(tidb_datatype::Datum::Null);
            if let (true, Some(substitute)) = (old_value.is_null(), substitute.as_ref()) {
                old_value = substitute.clone();
            }
            let converted = match tidb_table::column::cast_table_value_with_flags(
                old_value.clone(),
                &target.field_type,
                &target.name,
                cast_context,
                flags,
                false,
            ) {
                Ok(converted) => converted,
                Err(error) => {
                    let reported = error.to_mysql_error();
                    failure = Some(KvTableError::ColumnCast(crate::reorg_errors::reformat_reorg_error(
                        reported, &old_name, &old_value,
                    )));
                    break;
                }
            };
            if let Some(warning) = cast_context.take_warnings().into_iter().next() {
                let warning = crate::reorg_errors::reformat_reorg_error(
                    tidb_error::mysql::MysqlError::sql_error(warning.code(), warning.message()),
                    &old_name,
                    &old_value,
                );
                match warnings
                    .iter_mut()
                    .find(|(seen, _, _)| *seen == warning.code)
                {
                    Some((_, _, count)) => *count += 1,
                    None => warnings.push((warning.code, warning.message, 1)),
                }
            }
            // The row as Go re-encodes it: every column it has, with the
            // changing column's value added.
            let mut write_row = public_row;
            for (position, column) in self.nonpublic_columns.iter().enumerate() {
                write_row.push(if position == changing {
                    converted.clone()
                } else {
                    stored
                        .get(&column.column.id)
                        .cloned()
                        .unwrap_or(tidb_datatype::Datum::Null)
                });
            }
            let encoded = self.encode_row_value(&write_row, &zone)?;
            let _ = handle;
            self.store.set(Key::from_bytes(key), encoded)?;
            batch.added += 1;
        }
        for warning in statement_warnings {
            cast_context.append_sql_warn(warning);
        }
        if let Some(failure) = failure {
            return Err(failure);
        }
        batch.warnings = warnings;
        Ok(batch)
    }
}


/// The error a failed backfill batch reports (Go `addIndexTxnWorker`): a
/// duplicate index name as `ErrDupKeyName`, every other row-write failure as
/// the table layer raises it.
#[must_use]
pub fn index_backfill_error(error: KvTableError) -> tidb_error::mysql::MysqlError {
    match error {
        KvTableError::DuplicateKeyName(name) => crate::errors::duplicate_key_name(name),
        other => other.into_mysql_error(),
    }
}
