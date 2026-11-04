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

//! Go `pkg/table/tables/mutation_checker.go`: the row and index mutations one
//! `AddRecord` / `UpdateRecord` staged, read back from its memory-buffer
//! stage, must agree with the row it wrote (`tidb_enable_mutation_checker`).

use std::collections::HashMap;

use tidb_charset::Collation;
use tidb_datatype::{Datum, SessionTimeZone};
use tidb_kv::storage::{StagingHandle, TableStorage};
use tidb_tablecodec::table_key::{decode_index_id, decode_row_key, decode_table_id};
use tidb_tablecodec::{
    cut_datum_by_prefix_len, decode_column_value, decode_index_handle, decode_index_kv,
    decode_temp_index_value, is_record_key, temp_index_key_to_index_key,
    temp_index_value_is_untouched, HandleStatus, INDEX_ID_MASK,
};

use super::{Index, KvTableError, TableCommon};

/// Go `mutation`: one key the stage wrote.
#[derive(Debug, Clone, Default)]
struct Mutation {
    key: Vec<u8>,
    value: Vec<u8>,
    /// Only for index mutations: the key's index id, temp-index bit kept.
    index_id: i64,
}

impl TableCommon {
    /// Go `CheckDataConsistency`: whether the handles and indexed values the
    /// stage `sh` wrote match `row_to_insert` / `row_to_remove`.
    pub fn check_data_consistency(
        &self,
        store: &dyn TableStorage,
        zone: &SessionTimeZone,
        physical_table_id: i64,
        row_to_insert: Option<&[Datum]>,
        row_to_remove: Option<&[Datum]>,
        sh: StagingHandle,
    ) -> Result<(), KvTableError> {
        if self.meta().read().partition.is_some() {
            // Go: "TODO: Support check for partitions as well".
            return Ok(());
        }
        if sh.0 == 0 {
            // A backend that stages nothing.
            return Ok(());
        }
        let (index_mutations, row_insertion) =
            collect_table_mutations_from_buffer_stage(physical_table_id, store, sh)?;
        // Go `t.Indices()`: the public indexes and the ones a DDL is adding
        // or dropping.
        let index_id_to_info: HashMap<i64, &Index> = self
            .indexes()
            .iter()
            .chain(self.nonpublic_indexes.iter().map(|nonpublic| &nonpublic.index))
            .filter(|index| !(index.read().primary && self.meta().read().is_common_handle))
            .map(|index| (index.read().id, index))
            .collect();
        if !row_insertion.key.is_empty() {
            self.check_handle_consistency(&row_insertion, &index_mutations, &index_id_to_info)?;
        }
        self.check_index_keys(zone, row_to_insert, row_to_remove, &index_mutations, &index_id_to_info)
    }

    /// Go `checkHandleConsistency`: a PUT of an index implies the PUT of the
    /// row with the same handle. Deletions are not checked: their values are
    /// unknown.
    fn check_handle_consistency(
        &self,
        row_insertion: &Mutation,
        index_mutations: &[Mutation],
        index_id_to_info: &HashMap<i64, &Index>,
    ) -> Result<(), KvTableError> {
        let insertion_handle = decode_row_key(&row_insertion.key)
            .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
        for m in index_mutations {
            if m.value.is_empty() {
                continue;
            }
            let idx_id = m.index_id & INDEX_ID_MASK;
            let index = index_id_to_info
                .get(&idx_id)
                .ok_or_else(|| KvTableError::Decode("index not found".to_owned()))?;
            let columns_len = index.read().columns.len();
            let index_handle = if idx_id != m.index_id {
                // A temporary index's value carries its write history.
                if temp_index_value_is_untouched(&m.value) {
                    // Never committed to the storage.
                    continue;
                }
                let history = decode_temp_index_value(&m.value)
                    .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                let value = history.last().map(|current| current.value.clone()).unwrap_or_default();
                if value.is_empty() {
                    // A deletion.
                    continue;
                }
                let mut org_key = m.key.clone();
                temp_index_key_to_index_key(&mut org_key)
                    .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                decode_index_handle(&org_key, &value, columns_len)
            } else {
                decode_index_handle(&m.key, &m.value, columns_len)
            }
            .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
            let Some(index_handle) = index_handle else {
                continue;
            };
            // Go: the handle types can differ (issue 29520).
            let same_kind = index_handle.is_int() == insertion_handle.is_int();
            if same_kind && index_handle.encoded() != insertion_handle.encoded() {
                return Err(KvTableError::Mysql(tidb_terror::MysqlError::from_terror(
                    &crate::tables_errors::ERR_INCONSISTENT_HANDLE.gen_with_stack_by_args(&[
                        self.meta().read().name.original().to_owned().into(),
                        index.read().name.original().to_owned().into(),
                        format!("{index_handle}").into(),
                        format!("{insertion_handle}").into(),
                        format!("{m:?}").into(),
                        format!("{row_insertion:?}").into(),
                    ]),
                )));
            }
        }
        Ok(())
    }

    /// Go `checkIndexKeys`: every index mutation decodes to the values of the
    /// row it was written for -- an added entry to `row_to_insert`, a deleted
    /// one to `row_to_remove`.
    fn check_index_keys(
        &self,
        zone: &SessionTimeZone,
        row_to_insert: Option<&[Datum]>,
        row_to_remove: Option<&[Datum]>,
        index_mutations: &[Mutation],
        index_id_to_info: &HashMap<i64, &Index>,
    ) -> Result<(), KvTableError> {
        let use_new_collate = self.use_new_collation;
        for m in index_mutations {
            let idx_id = m.index_id & INDEX_ID_MASK;
            let index = index_id_to_info
                .get(&idx_id)
                .ok_or_else(|| KvTableError::Decode("index not found".to_owned()))?;
            let mut is_tmp_idx_val_and_deleted = false;
            let value = if idx_id != m.index_id {
                if temp_index_value_is_untouched(&m.value) {
                    continue;
                }
                let history = decode_temp_index_value(&m.value)
                    .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                let Some(current) = history.last() else {
                    continue;
                };
                is_tmp_idx_val_and_deleted = current.delete;
                current.value.clone()
            } else {
                m.value.clone()
            };
            // When the key alone cannot give back the original value.
            if value.is_empty() && self.index_needs_restored_data(index) {
                continue;
            }
            let row_col_infos = self.build_rowcodec_col_info_for_index_columns(index);
            let decoded_index_values = decode_index_kv(
                use_new_collate,
                &m.key,
                &value,
                index.read().columns.len(),
                HandleStatus::NotNeeded,
                &row_col_infos,
            )
            .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
            let mut index_data = Vec::with_capacity(decoded_index_values.len());
            for (position, encoded) in decoded_index_values.iter().enumerate() {
                let offset = index.column_offsets[position];
                let field_type = self.columns()[offset].read().field_type.array_type();
                index_data.push(
                    decode_column_value(encoded, &field_type, Some(zone))
                        .map_err(|error| KvTableError::Decode(format!("{error:?}")))?,
                );
            }
            // An add-index backfill state.
            let input = if value.is_empty() || is_tmp_idx_val_and_deleted {
                row_to_remove
            } else {
                row_to_insert
            };
            if let Some(input) = input {
                self.compare_index_data(use_new_collate, &index_data, input, index)?;
            }
        }
        Ok(())
    }

    /// Go `compareIndexData`: the decoded index data is a subset of `input`.
    fn compare_index_data(
        &self,
        use_new_collate: bool,
        index_data: &[Datum],
        input: &[Datum],
        index: &Index,
    ) -> Result<(), KvTableError> {
        let info = index.read();
        for (position, decoded) in index_data.iter().enumerate() {
            let offset = index.column_offsets[position];
            let column = self.columns()[offset].read();
            let length = info.columns.get(position).map_or(tidb_datatype::UNSPECIFIED_LENGTH, |column| column.read().length);
            let mut decoded_mutation_datum = decoded.clone();
            let Some(mut expected_datum) = input.get(offset).cloned() else {
                continue;
            };
            cut_datum_by_prefix_len(&mut expected_datum, length, &column.field_type);
            cut_datum_by_prefix_len(&mut decoded_mutation_datum, length, &column.field_type);
            let collator = get_collator_with_collate(use_new_collate, &decoded_mutation_datum);
            let comparison = compare_index_and_val(
                &expected_datum,
                &decoded_mutation_datum,
                collator,
                column.field_type.is_array() && matches!(expected_datum, Datum::Json(_)),
            )?;
            if comparison != std::cmp::Ordering::Equal {
                return Err(KvTableError::Mysql(tidb_terror::MysqlError::from_terror(
                    &crate::tables_errors::ERR_INCONSISTENT_INDEXED_VALUE
                        .gen_with_stack_by_args(&[
                            self.meta().read().name.original().to_owned().into(),
                            info.name.original().to_owned().into(),
                            column.name.original().to_owned().into(),
                            decoded_mutation_datum.string().into(),
                            expected_datum.string().into(),
                        ]),
                )));
            }
        }
        Ok(())
    }

    /// Go `BuildRowcodecColInfoForIndexColumns`.
    fn build_rowcodec_col_info_for_index_columns(&self, index: &Index) -> Vec<tidb_rowcodec::ColumnInfo> {
        let meta = self.meta();
        let pk_is_handle = meta.read().pk_is_handle;
        index
            .column_offsets
            .iter()
            .map(|offset| {
                let column = self.columns()[*offset].read();
                tidb_rowcodec::ColumnInfo {
                    id: column.id,
                    is_pk_handle: pk_is_handle
                        && tidb_mysql::has_pri_key_flag(column.field_type.flags() as usize),
                    virtual_generated: false,
                    field_type: column.field_type.clone(),
                }
            })
            .collect()
    }
}

/// Go `collectTableMutationsFromBufferStage`: the stage's index mutations
/// and its only row insertion (an empty mutation when there is none) for
/// the table `physical_table_id`.
fn collect_table_mutations_from_buffer_stage(
    physical_table_id: i64,
    store: &dyn TableStorage,
    sh: StagingHandle,
) -> Result<(Vec<Mutation>, Mutation), KvTableError> {
    let mut index_mutations = Vec::new();
    let mut row_insertion = Mutation::default();
    let mut err = None;
    store.inspect_stage(sh, &mut |key, _flags, data| {
        let key = key.as_bytes();
        // Only the current table.
        if decode_table_id(key) != physical_table_id {
            return;
        }
        let mut m = Mutation {
            key: key.to_vec(),
            value: data.to_vec(),
            index_id: 0,
        };
        if is_record_key(key) {
            if !data.is_empty() {
                if row_insertion.key.is_empty() {
                    row_insertion = m;
                } else {
                    err = Some(KvTableError::Decode(format!(
                        "multiple row mutations added/mutated, one = {row_insertion:?}, another = {m:?}"
                    )));
                }
            }
        } else {
            match decode_index_id(key) {
                Ok(index_id) => m.index_id = index_id,
                Err(error) => err = Some(KvTableError::Decode(format!("{error:?}"))),
            }
            index_mutations.push(m);
        }
    });
    match err {
        Some(error) => Err(error),
        None => Ok((index_mutations, row_insertion)),
    }
}

/// Go `collate.GetCollatorWithCollate(useNewCollate, d.Collation())`.
fn get_collator_with_collate(use_new_collate: bool, datum: &Datum) -> Collation {
    if !use_new_collate {
        return Collation::Binary;
    }
    match datum {
        Datum::String(value) => value.collation(),
        _ => Collation::Binary,
    }
}

/// Go `CompareIndexAndVal`: for a multi-valued index, whether the row's JSON
/// array holds the indexed value.
fn compare_index_and_val(
    row_val: &Datum,
    idx_val: &Datum,
    collator: Collation,
    cmp_mv_index: bool,
) -> Result<std::cmp::Ordering, KvTableError> {
    let compare = |left: &Datum, right: &Datum, collation| {
        left.compare(right, collation)
            .map_err(|error| KvTableError::Decode(format!("{error:?}")))
    };
    if cmp_mv_index {
        let Datum::Json(json) = row_val else {
            return compare(idx_val, row_val, collator);
        };
        let mut result = std::cmp::Ordering::Less;
        let json_error = |error| KvTableError::Decode(format!("{error:?}"));
        for element in 0..json.element_count().map_err(json_error)? {
            let Some(item) = json.array_get(element).map_err(json_error)? else {
                continue;
            };
            let json_datum = Datum::new_json(item);
            result = compare(&json_datum, idx_val, Collation::Binary)?;
            if result == std::cmp::Ordering::Equal {
                break;
            }
        }
        return Ok(result);
    }
    compare(idx_val, row_val, collator)
}
