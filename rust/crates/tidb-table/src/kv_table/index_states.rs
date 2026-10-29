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

//! DML maintenance of an index Go's DDL is still adding or dropping: the
//! state-dependent half of `pkg/table/tables/index.go` (`index.create`,
//! `index.Delete`, `GenTempIdxKeyByState`,
//! `FetchDuplicatedHandleForTempIndexKey`) and `IsIndexWritable`.
//!
//! While an ADD INDEX backfills with the merge method, DML writes go to the
//! TEMPORARY index (`BackfillStateRunning`) or to both the index and the
//! temporary index (`ReadyToMerge`/`Merging`), as history elements the
//! DDL's merge step replays. A node that skipped these indexes, or wrote the
//! real key in the running stage, would leave the finished index missing or
//! carrying rows the merge cannot reconcile.
//!
//! Go marks some of these writes `SetNeedLocked`; the cluster tier locks
//! every key a pessimistic statement staged, which covers them.

use tidb_datatype::{Datum, SessionTimeZone};
use tidb_tablecodec::{
    decode_handle_in_index_value, decode_temp_index_value, index_key_to_temp_index_key,
    temp_index_key_to_index_key, TempIndexValueElem,
};
use tidb_txnkv::{AssertionOp, Key};

use tidb_kv::storage::StorageError;

use super::index_entries::{convert_handle, duplicate_value_text};
use super::{KvTable, KvTableError, NonPublicIndex, TableHandle};

/// Go `tablecodec.TempIndexKeyTypeNone`: not a temporary-index write.
const TEMP_INDEX_KEY_TYPE_NONE: u8 = 0;
/// Go `tablecodec.TempIndexKeyTypeDelete`: written in the delete-only stage.
const TEMP_INDEX_KEY_TYPE_DELETE: u8 = b'd';
/// Go `tablecodec.TempIndexKeyTypeBackfill`: written in the backfill stage.
const TEMP_INDEX_KEY_TYPE_BACKFILL: u8 = b'b';
/// Go `tablecodec.TempIndexKeyTypeMerge`: written in the merge stage.
const TEMP_INDEX_KEY_TYPE_MERGE: u8 = b'm';

/// Go `GenTempIdxKeyByState`: the index key a DML write uses (`None` when
/// it writes only the temporary index), the temporary-index key it writes
/// too, and the history element's key version.
fn gen_temp_idx_key_by_state(
    index: &NonPublicIndex,
    index_key: Vec<u8>,
) -> Result<(Option<Vec<u8>>, Option<Vec<u8>>, u8), KvTableError> {
    let to_temp = |mut key: Vec<u8>| -> Result<Vec<u8>, KvTableError> {
        index_key_to_temp_index_key(&mut key)
            .map_err(|error| KvTableError::Encode(format!("{error:?}")))?;
        Ok(key)
    };
    match index.backfill_state {
        tidb_model::BackfillState::RUNNING => {
            let temp = to_temp(index_key)?;
            let version = if index.state == tidb_model::SchemaState::DELETE_ONLY {
                TEMP_INDEX_KEY_TYPE_DELETE
            } else {
                TEMP_INDEX_KEY_TYPE_BACKFILL
            };
            Ok((None, Some(temp), version))
        }
        tidb_model::BackfillState::READY_TO_MERGE | tidb_model::BackfillState::MERGING => {
            let temp = to_temp(index_key.clone())?;
            Ok((Some(index_key), Some(temp), TEMP_INDEX_KEY_TYPE_MERGE))
        }
        _ => Ok((Some(index_key), None, TEMP_INDEX_KEY_TYPE_NONE)),
    }
}

/// One encoded temporary-index history element appended to `previous`
/// (Go `TempIndexValueElem.Encode(previous)`).
fn encode_temp_value(
    previous: &[u8],
    element: &TempIndexValueElem,
) -> Result<Vec<u8>, KvTableError> {
    let mut output = previous.to_vec();
    element
        .encode(&mut output)
        .map_err(|error| KvTableError::Encode(format!("{error:?}")))?;
    Ok(output)
}

fn txn_handle(handle: &TableHandle) -> Result<tidb_txnkv::Handle, KvTableError> {
    Ok(match handle {
        TableHandle::Int(value) => tidb_txnkv::IntHandle::new(*value).into(),
        TableHandle::Common(bytes) => tidb_txnkv::CommonHandle::new(bytes.clone())
            .map_err(|error| KvTableError::Encode(format!("{error:?}")))?
            .into(),
    })
}

impl KvTable {
    /// Go `getKeyInTxn`: the value the transaction sees, `None` for absent
    /// (or deleted).
    fn get_key_in_txn(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, KvTableError> {
        match self.store.get(&Key::from_bytes(key.to_vec())) {
            Ok(value) if value.is_empty() => Ok(None),
            Ok(value) => Ok(Some(value)),
            Err(StorageError::NotFound) => Ok(None),
            Err(error) => Err(KvTableError::from(error)),
        }
    }

    /// Go `FetchDuplicatedHandleForTempIndexKey`: the handle a unique write
    /// would duplicate, judged from the temporary index's latest history
    /// element and the original index, plus the temporary key's raw value.
    fn fetch_duplicated_handle_for_temp_index_key(
        &mut self,
        temp_key: &[u8],
    ) -> Result<(Option<TableHandle>, Option<Vec<u8>>), KvTableError> {
        let mut origin_key = temp_key.to_vec();
        temp_index_key_to_index_key(&mut origin_key)
            .map_err(|error| KvTableError::Encode(format!("{error:?}")))?;
        let decode_error = |error| KvTableError::Encode(format!("{error:?}"));
        let Some(temp_raw) = self.get_key_in_txn(temp_key)? else {
            let Some(origin) = self.get_key_in_txn(&origin_key)? else {
                return Ok((None, None));
            };
            let handle = decode_handle_in_index_value(&origin).map_err(decode_error)?;
            return Ok((handle.as_ref().map(convert_handle), None));
        };
        let history = decode_temp_index_value(&temp_raw).map_err(decode_error)?;
        let Some(current) = history.last() else {
            return Ok((None, Some(temp_raw)));
        };
        if current.delete {
            let Some(origin) = self.get_key_in_txn(&origin_key)? else {
                return Ok((None, Some(temp_raw)));
            };
            let Some(origin_handle) =
                decode_handle_in_index_value(&origin).map_err(decode_error)?
            else {
                return Ok((None, Some(temp_raw)));
            };
            let deleted = current.handle.as_ref().map(convert_handle);
            if deleted.as_ref() == Some(&convert_handle(&origin_handle)) {
                // The key was deleted: not a duplicate.
                return Ok((None, Some(temp_raw)));
            }
            // Several modifications hit the key; the origin's row decides.
            let row_key = tidb_codec::encode_row_key(
                tidb_codec::decode_table_id(temp_key),
                &origin_handle.clone().encoded(),
            );
            if self.get_key_in_txn(&row_key)?.is_none() {
                return Ok((None, Some(temp_raw)));
            }
            return Ok((Some(convert_handle(&origin_handle)), Some(temp_raw)));
        }
        let handle = decode_handle_in_index_value(&current.value).map_err(decode_error)?;
        Ok((handle.as_ref().map(convert_handle), Some(temp_raw)))
    }

    /// Go `index.create` for a non-public index (never from backfill):
    /// assertions are skipped (`ignoreAssertion` for every non-public
    /// state), and a temporary key carries a history element.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_nonpublic_index_entries(
        &mut self,
        index: &NonPublicIndex,
        row: &[Datum],
        handle: &TableHandle,
        physical_id: i64,
        zone: &SessionTimeZone,
        lazy_dup_check: bool,
        pessimistic: bool,
        check_in_prewrite: bool,
    ) -> Result<(), KvTableError> {
        // Go `index.create`'s region (the statement's writes pass their ctx).
        let _create = tidb_tracing::start_thread_region("index.Create");
        let kv_index = &index.index;
        let duplicate_value = duplicate_value_text(&self.index_values(kv_index, row));
        for entry in self.index_entry_keys(kv_index, row, handle, physical_id, zone)? {
            let value = self.index_entry_value(
                kv_index,
                &entry.values,
                row,
                handle,
                entry.distinct,
                physical_id,
                zone,
            )?;
            let (key, temp_key, key_version) = gen_temp_idx_key_by_state(index, entry.key)?;
            // In the running stage the write's key IS the temporary key.
            let (key, temp_key, key_is_temp) = match key {
                Some(key) => (key, temp_key, false),
                None => (
                    temp_key.expect("a running stage writes a temp key"),
                    None,
                    true,
                ),
            };
            let has_temp_key = key_is_temp || temp_key.is_some();
            let element = |distinct: bool| TempIndexValueElem {
                value: value.clone(),
                handle: None,
                key_version,
                delete: false,
                distinct,
                global: false,
            };
            if !entry.distinct {
                let stored = if key_is_temp {
                    encode_temp_value(&[], &element(false))?
                } else {
                    value.clone()
                };
                self.store
                    .set(Key::from_bytes(key), stored)
                    .map_err(KvTableError::from)?;
                if let Some(temp_key) = temp_key {
                    self.store
                        .set(
                            Key::from_bytes(temp_key),
                            encode_temp_value(&[], &element(false))?,
                        )
                        .map_err(KvTableError::from)?;
                }
                continue;
            }
            if has_temp_key {
                // The memory buffer alone cannot answer for a temporary key:
                // a lazily checked value could overwrite its history.
                let probe = if key_is_temp {
                    key.clone()
                } else {
                    temp_key.clone().expect("a merge stage writes a temp key")
                };
                let (duplicate, previous) =
                    self.fetch_duplicated_handle_for_temp_index_key(&probe)?;
                if duplicate.is_some() {
                    return Err(KvTableError::Mysql(
                        tidb_error::mysql::MysqlError::from_terror(
                            &tidb_error::catalog::kv::ERR_KEY_EXISTS.fast_generate_by_args(&[
                                (duplicate_value.clone()).into(),
                                (self.qualified_key(&kv_index.name)).into(),
                            ]),
                        ),
                    ));
                }
                let previous = previous.unwrap_or_default();
                let stored = if key_is_temp {
                    encode_temp_value(&previous, &element(true))?
                } else {
                    value.clone()
                };
                self.store
                    .set(Key::from_bytes(key), stored)
                    .map_err(KvTableError::from)?;
                if let Some(temp_key) = temp_key {
                    self.store
                        .set(
                            Key::from_bytes(temp_key),
                            encode_temp_value(&previous, &element(true))?,
                        )
                        .map_err(KvTableError::from)?;
                }
                continue;
            }
            // `BackfillStateInapplicable`: the index key itself, checked like
            // a public unique entry but written without an assertion.
            let key = Key::from_bytes(key);
            let lazy_check = self.check_insert_key(
                &key,
                &duplicate_value,
                &self.qualified_key(&kv_index.name),
                lazy_dup_check,
            )?;
            self.store
                .set_with_constraint_check(
                    key,
                    value,
                    AssertionOp::AssertUnknown,
                    lazy_check && pessimistic && check_in_prewrite,
                )
                .map_err(KvTableError::from)?;
        }
        Ok(())
    }

    /// Go `index.Delete` for a non-public index: the index key is removed
    /// (a distinct key only when it still names this row while a backfill
    /// runs, #52914) and the temporary index records a delete element; no
    /// assertion, because a non-public entry need not exist.
    pub(crate) fn remove_nonpublic_index_entries(
        &mut self,
        index: &NonPublicIndex,
        row: &[Datum],
        handle: &TableHandle,
        physical_id: i64,
        zone: &SessionTimeZone,
    ) -> Result<(), KvTableError> {
        let kv_index = &index.index;
        let deleted_handle = txn_handle(handle)?;
        for entry in self.index_entry_keys(kv_index, row, handle, physical_id, zone)? {
            let (key, temp_key, key_version) = gen_temp_idx_key_by_state(index, entry.key)?;
            let previous = match (&temp_key, kv_index.unique) {
                (Some(temp_key), true) => self.get_key_in_txn(temp_key)?.unwrap_or_default(),
                _ => Vec::new(),
            };
            let element = TempIndexValueElem {
                value: Vec::new(),
                handle: Some(deleted_handle.clone()),
                key_version,
                delete: true,
                distinct: entry.distinct,
                global: false,
            };
            if let Some(key) = key {
                let mut ok_to_delete = true;
                if entry.distinct && index.backfill_state != tidb_model::BackfillState::INAPPLICABLE
                {
                    if let Some(origin) = self.get_key_in_txn(&key)? {
                        let origin_handle = decode_handle_in_index_value(&origin)
                            .map_err(|error| KvTableError::Encode(format!("{error:?}")))?;
                        if origin_handle.as_ref().map(convert_handle).as_ref() != Some(handle) {
                            // The key now belongs to an ingested row.
                            ok_to_delete = false;
                        }
                    }
                }
                if ok_to_delete {
                    self.store
                        .delete(Key::from_bytes(key))
                        .map_err(KvTableError::from)?;
                }
            }
            if let Some(temp_key) = temp_key {
                let base: &[u8] = if entry.distinct { &previous } else { &[] };
                self.store
                    .set(
                        Key::from_bytes(temp_key),
                        encode_temp_value(base, &element)?,
                    )
                    .map_err(KvTableError::from)?;
            }
        }
        Ok(())
    }

    /// Go `rebuildUpdateRecordIndices` over the non-public indexes: an index
    /// whose columns (and partial-index condition) did not change is
    /// untouched; otherwise every deletable index drops the old entry and
    /// every writable one files the new.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn rewrite_nonpublic_index_entries(
        &mut self,
        indexes: &[NonPublicIndex],
        old_row: &[Datum],
        new_row: &[Datum],
        handle: &TableHandle,
        physical_id: i64,
        zone: &SessionTimeZone,
        lazy_dup_check: bool,
        pessimistic: bool,
        check_in_prewrite: bool,
    ) -> Result<(), KvTableError> {
        for index in indexes {
            if index.index.clustered_primary {
                continue;
            }
            let old_holds = self.index_condition_holds(&index.index, old_row, zone)?;
            let new_holds = self.index_condition_holds(&index.index, new_row, zone)?;
            let untouched = old_holds == new_holds
                && index
                    .index
                    .column_offsets
                    .iter()
                    .all(|offset| old_row.get(*offset) == new_row.get(*offset));
            if untouched {
                continue;
            }
            if old_holds {
                self.remove_nonpublic_index_entries(index, old_row, handle, physical_id, zone)?;
            }
            if new_holds && index.is_writable() {
                self.create_nonpublic_index_entries(
                    index,
                    new_row,
                    handle,
                    physical_id,
                    zone,
                    lazy_dup_check,
                    pessimistic,
                    check_in_prewrite,
                )?;
            }
        }
        Ok(())
    }
}
