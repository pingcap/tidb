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

//! Stored-row decoding shared by scans, point reads, DDL reorg, and ADMIN.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tidb_codec::table_key::RECORD_ROW_KEY_LEN;
use tidb_datatype::{new_collation_enabled, Datum, FieldType, SessionTimeZone};
use tidb_tablecodec::decode_table_row_to_map;
use tidb_txnkv::CommonHandle;

use super::table_meta::NOT_NULL_FLAG;
use super::{KvColumn, KvTableError, PreparedPointGetDecodeContext, RowDecodeContext, TableHandle};

/// Which generated columns a decoder evaluates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeneratedColumnSelection {
    /// Treat generated columns as ordinary stored columns.
    None,
    /// Evaluate only virtual columns omitted from stored row bytes.
    Virtual,
    /// Recompute every generated column, including stored columns.
    All,
}

/// One decoded row in both table-column order and column-id form.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedRow {
    values: Vec<Datum>,
    by_id: BTreeMap<i64, Datum>,
}

impl DecodedRow {
    /// Values in table-column order, including decoded defaults and generated values.
    #[must_use]
    pub fn values(&self) -> &[Datum] {
        &self.values
    }

    /// The decoded column-id map.
    #[must_use]
    pub fn by_id(&self) -> &BTreeMap<i64, Datum> {
        &self.by_id
    }

    /// Takes both representations.
    #[must_use]
    pub fn into_parts(self) -> (Vec<Datum>, BTreeMap<i64, Datum>) {
        (self.values, self.by_id)
    }
}

/// Everything needed to turn one record key/value pair into a table row.
#[derive(Clone)]
pub struct RowDecoder {
    /// The table schema this decoder reads, SHARED with the catalog's
    /// `KvTable` (`Arc`): building the decoder per execute must not deep-copy
    /// every column's name and collation strings again.
    columns: Arc<Vec<KvColumn>>,
    /// Columns for generated-column evaluation: the shared schema when any
    /// virtual generated column is decoded, with every non-generated entry's
    /// expression nulled; `None` when there is nothing to evaluate, so the
    /// common no-generated-columns case never copies the schema at all.
    evaluation_columns: Option<Arc<Vec<KvColumn>>>,
    column_types: BTreeMap<i64, FieldType>,
    decoded_offsets: BTreeSet<usize>,
    generated_offsets: BTreeSet<usize>,
    changing_dependencies: BTreeMap<usize, usize>,
    pk_handle_offset: Option<usize>,
    common_handle_offsets: Vec<usize>,
    use_new_collation: bool,
    keep: Option<Vec<usize>>,
    context: RowDecodeContext,
}

#[derive(Clone, Debug)]
struct PreparedPointGetColumn {
    column: KvColumn,
    from_handle: bool,
    common_handle_part: Option<usize>,
}

/// Immutable row-decoder metadata retained by a cached prepared PointGet.
///
/// It contains only the projected ordinary columns admitted by the point-plan
/// gate. Building the general [`RowDecoder`] on every execute would clone the
/// complete table schema and reconstruct its generated-column dependency
/// maps even though this path reads one handle and no expressions.
#[derive(Clone, Debug)]
pub struct PreparedPointGetRowDecoder {
    columns: Vec<PreparedPointGetColumn>,
    stored_column_types: BTreeMap<i64, FieldType>,
    common_handle_offsets: Vec<usize>,
    /// Whether the projection contains a column supplied by the clustered
    /// handle. Most YCSB projections do not select the key, so parsing the
    /// common-handle bytes for every row would be needless work.
    needs_handle: bool,
    /// Whether any projected column can be absent from an older row and needs
    /// an execute-time origin-default value.
    has_origin_defaults: bool,
    /// Row V2 metadata is immutable for the schema-versioned prepared plan.
    /// Keeping it here avoids cloning ten `ColumnInfo`s and handle IDs on each
    /// YCSB execute.
    v2_columns: Vec<tidb_codec::ColumnInfo>,
    v2_handle_column_ids: Vec<i64>,
}

impl PreparedPointGetRowDecoder {
    /// Compiles the schema-versioned projection stored on the point plan.
    pub fn new(
        columns: &[KvColumn],
        pk_handle_offset: usize,
        output_offsets: &[usize],
    ) -> Result<Self, KvTableError> {
        Self::new_with_handles(columns, Some(pk_handle_offset), &[], output_offsets)
    }

    /// Compiles a prepared point-get projection for either an integer or a
    /// common clustered handle.
    pub fn new_with_handles(
        columns: &[KvColumn],
        pk_handle_offset: Option<usize>,
        common_handle_offsets: &[usize],
        output_offsets: &[usize],
    ) -> Result<Self, KvTableError> {
        let common_handle_column_ids: Vec<i64> = common_handle_offsets
            .iter()
            .filter_map(|offset| columns.get(*offset).map(|column| column.id))
            .collect();
        let projected = output_offsets
            .iter()
            .map(|offset| {
                let column = columns.get(*offset).cloned().ok_or_else(|| {
                    KvTableError::Decode(
                        "prepared point-get projection is outside the table schema".to_owned(),
                    )
                })?;
                if column.generated.is_some() {
                    return Err(KvTableError::Decode(
                        "prepared point-get projection contains a generated column".to_owned(),
                    ));
                }
                Ok(PreparedPointGetColumn {
                    column,
                    from_handle: pk_handle_offset == Some(*offset),
                    common_handle_part: common_handle_offsets
                        .iter()
                        .position(|handle_offset| *handle_offset == *offset),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let stored_column_types = projected
            .iter()
            .filter(|column| !column.from_handle)
            .map(|column| (column.column.id, column.column.field_type.clone()))
            .collect();
        let v2_columns = projected
            .iter()
            .map(|output| tidb_codec::ColumnInfo {
                id: output.column.id,
                is_pk_handle: output.from_handle,
                virtual_generated: false,
                field_type: output.column.field_type.clone(),
            })
            .collect::<Vec<_>>();
        let v2_handle_column_ids = projected
            .iter()
            .find(|output| output.from_handle)
            .map(|output| output.column.id)
            .into_iter()
            .chain(common_handle_column_ids.iter().copied())
            .collect();
        let needs_handle = projected
            .iter()
            .any(|output| output.from_handle || output.common_handle_part.is_some());
        let has_origin_defaults = projected
            .iter()
            .any(|output| output.column.origin_default.is_some());
        Ok(Self {
            columns: projected,
            stored_column_types,
            common_handle_offsets: common_handle_offsets.to_vec(),
            needs_handle,
            has_origin_defaults,
            v2_columns,
            v2_handle_column_ids,
        })
    }

    pub(crate) fn decode(
        &self,
        handle: &TableHandle,
        value: &[u8],
        context: &PreparedPointGetDecodeContext,
    ) -> Result<Vec<Datum>, KvTableError> {
        // Row V2 stores a column directory and can decode directly into the
        // requested projection. This avoids the legacy BTreeMap allocation on
        // every prepared point read (the YCSB hot path).
        if tidb_codec::is_new_format(value) {
            let codec_handle = self.needs_handle.then(|| match handle {
                TableHandle::Int(handle_value) => Ok(tidb_codec::Handle::Int(*handle_value)),
                TableHandle::Common(encoded) => {
                    let common = CommonHandle::new(encoded.clone())
                        .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                    let parts = (0..self.common_handle_offsets.len())
                        .filter_map(|part| common.encoded_column(part).map(<[u8]>::to_vec))
                        .collect();
                    Ok(tidb_codec::Handle::Common(parts))
                }
            });
            let codec_handle = codec_handle.transpose()?;
            let defaults = self
                .has_origin_defaults
                .then(|| {
                    self.columns
                        .iter()
                        .map(|output| {
                            output
                                .column
                                .origin_default_value(
                                    context.origin_default_flags(),
                                    context.zone(),
                                )
                                .map_err(|error| KvTableError::Decode(error.to_string()))
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?;
            return tidb_codec::decode_row_to_datums(
                value,
                &self.v2_columns,
                &tidb_codec::DecodeRowOptions {
                    handle_column_ids: &self.v2_handle_column_ids,
                    handle: codec_handle.as_ref(),
                    defaults: defaults.as_deref(),
                    timezone: Some(context.zone()),
                    ..tidb_codec::DecodeRowOptions::default()
                },
            )
            .map(|row| row.values)
            .map_err(|error| KvTableError::Decode(format!("{error:?}")));
        }
        let decoded =
            decode_table_row_to_map(value, &self.stored_column_types, Some(context.zone()))
                .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
        self.columns
            .iter()
            .map(|output| {
                if output.from_handle {
                    let TableHandle::Int(value) = handle else {
                        return Err(KvTableError::Decode(
                            "prepared point-get plan requires an integer handle".to_owned(),
                        ));
                    };
                    return tidb_tablecodec::unflatten_datum(
                        Datum::Int(*value),
                        &output.column.field_type,
                        Some(context.zone()),
                    )
                    .map_err(|error| KvTableError::Decode(format!("{error:?}")));
                }
                if let Some(part) = output.common_handle_part {
                    let TableHandle::Common(encoded) = handle else {
                        return Err(KvTableError::Decode(
                            "prepared point-get plan requires a common handle".to_owned(),
                        ));
                    };
                    let common = CommonHandle::new(encoded.clone())
                        .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                    let Some(encoded) = common.encoded_column(part) else {
                        return Err(KvTableError::Decode(
                            "prepared point-get common handle is missing a column".to_owned(),
                        ));
                    };
                    let (remainder, value) = tidb_codec::decode_one_typed_in_timezone(
                        encoded,
                        &output.column.field_type,
                        Some(context.zone()),
                    )
                    .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                    if !remainder.is_empty() {
                        return Err(KvTableError::Decode(
                            "prepared point-get common handle has trailing bytes".to_owned(),
                        ));
                    }
                    return Ok(value);
                }
                if let Some(value) = decoded.get(&output.column.id) {
                    return Ok(value.clone());
                }
                if output.column.origin_default.is_none()
                    && output.column.field_type.flags() & NOT_NULL_FLAG != 0
                {
                    return Err(KvTableError::Decode("Miss column".to_owned()));
                }
                output
                    .column
                    .origin_default_value(context.origin_default_flags(), context.zone())
                    .map_err(|error| KvTableError::Decode(error.to_string()))
            })
            .collect()
    }
}

impl RowDecoder {
    /// Builds a full-schema decoder, equivalent to Go `NewRowDecoder` with
    /// `BuildFullDecodeColMap` when `generated` is [`GeneratedColumnSelection::All`].
    pub fn new(
        columns: Vec<KvColumn>,
        pk_handle_offset: Option<usize>,
        common_handle_offsets: Vec<usize>,
        generated: GeneratedColumnSelection,
        context: RowDecodeContext,
    ) -> Result<Self, KvTableError> {
        Self::build(
            Arc::new(columns),
            pk_handle_offset,
            common_handle_offsets,
            generated,
            None,
            new_collation_enabled(),
            context,
        )
    }

    /// Builds a decoder for a subset of table-column offsets. Dependencies
    /// of selected generated columns are included automatically.
    pub fn projected(
        columns: Vec<KvColumn>,
        pk_handle_offset: Option<usize>,
        common_handle_offsets: Vec<usize>,
        generated: GeneratedColumnSelection,
        offsets: &[usize],
        context: RowDecodeContext,
    ) -> Result<Self, KvTableError> {
        Self::build(
            Arc::new(columns),
            pk_handle_offset,
            common_handle_offsets,
            generated,
            Some(offsets),
            new_collation_enabled(),
            context,
        )
    }

    pub(crate) fn for_table_read(
        columns: Arc<Vec<KvColumn>>,
        pk_handle_offset: Option<usize>,
        common_handle_offsets: Vec<usize>,
        keep: Option<&[usize]>,
        use_new_collation: bool,
        context: RowDecodeContext,
    ) -> Result<Self, KvTableError> {
        Self::build(
            columns,
            pk_handle_offset,
            common_handle_offsets,
            GeneratedColumnSelection::Virtual,
            keep,
            use_new_collation,
            context,
        )
    }

    pub(crate) fn for_recomputed_read(
        columns: Arc<Vec<KvColumn>>,
        pk_handle_offset: Option<usize>,
        common_handle_offsets: Vec<usize>,
        use_new_collation: bool,
        context: RowDecodeContext,
    ) -> Result<Self, KvTableError> {
        Self::build(
            columns,
            pk_handle_offset,
            common_handle_offsets,
            GeneratedColumnSelection::All,
            None,
            use_new_collation,
            context,
        )
    }

    fn build(
        columns: Arc<Vec<KvColumn>>,
        pk_handle_offset: Option<usize>,
        common_handle_offsets: Vec<usize>,
        generated: GeneratedColumnSelection,
        keep: Option<&[usize]>,
        use_new_collation: bool,
        context: RowDecodeContext,
    ) -> Result<Self, KvTableError> {
        let width = columns.len();
        if pk_handle_offset.is_some_and(|offset| offset >= width)
            || common_handle_offsets.iter().any(|offset| *offset >= width)
        {
            return Err(KvTableError::Decode(
                "row decoder handle column is outside the table schema".to_owned(),
            ));
        }
        if pk_handle_offset.is_some() && !common_handle_offsets.is_empty() {
            return Err(KvTableError::Decode(
                "a table cannot have both integer and common handles".to_owned(),
            ));
        }

        let candidate_generated_offsets: BTreeSet<usize> = columns
            .iter()
            .enumerate()
            .filter_map(|(offset, column)| {
                let generation = column.generated.as_ref()?;
                match generated {
                    GeneratedColumnSelection::None => None,
                    GeneratedColumnSelection::Virtual if generation.stored => None,
                    GeneratedColumnSelection::Virtual | GeneratedColumnSelection::All => {
                        Some(offset)
                    }
                }
            })
            .collect();

        let mut decoded_offsets: BTreeSet<usize> = keep
            .map(|offsets| offsets.iter().copied().collect())
            .unwrap_or_else(|| (0..width).collect());
        if decoded_offsets.iter().any(|offset| *offset >= width) {
            return Err(KvTableError::Decode(
                "row decoder projection is outside the table schema".to_owned(),
            ));
        }
        loop {
            let before = decoded_offsets.len();
            for offset in decoded_offsets.clone() {
                if !candidate_generated_offsets.contains(&offset) {
                    continue;
                }
                let generated = columns[offset]
                    .generated
                    .as_ref()
                    .expect("generated offset has generation metadata");
                let dependencies =
                    crate::generated_column::dependency_offsets(&columns, &generated.dependencies)
                        .map_err(|name| {
                            KvTableError::Decode(format!(
                                "generated column `{}` reads missing column `{name}`",
                                columns[offset].name
                            ))
                        })?;
                decoded_offsets.extend(dependencies);
            }
            if decoded_offsets.len() == before {
                break;
            }
        }

        // A projected decoder evaluates only generated columns in its decode
        // map. Dependencies that are themselves generated join that map in
        // the fixed-point loop above; unrelated generated columns do not run
        // over a row full of NULL placeholders and cannot add warnings or
        // errors to a statement that never selected them.
        let generated_offsets = candidate_generated_offsets
            .intersection(&decoded_offsets)
            .copied()
            .collect::<BTreeSet<_>>();

        let column_types = decoded_offsets
            .iter()
            .map(|offset| (columns[*offset].id, columns[*offset].field_type.clone()))
            .collect();
        // Only a DECODED virtual generated column needs evaluation. When
        // there is none -- the common table -- `evaluation_columns` stays
        // `None` and no copy of the schema is made for it; when there is,
        // the copy nulls every non-evaluated expression exactly as before.
        let evaluation_columns = if generated_offsets.is_empty() {
            None
        } else {
            let mut evaluation_columns = (*columns).clone();
            for (offset, column) in evaluation_columns.iter_mut().enumerate() {
                if !generated_offsets.contains(&offset) {
                    column.generated = None;
                }
            }
            Some(Arc::new(evaluation_columns))
        };

        Ok(Self {
            columns,
            evaluation_columns,
            column_types,
            decoded_offsets,
            generated_offsets,
            changing_dependencies: BTreeMap::new(),
            pk_handle_offset,
            common_handle_offsets,
            use_new_collation,
            keep: keep.map(<[usize]>::to_vec),
            context,
        })
    }

    /// Marks `target_offset` as the changing form of `dependency_offset`.
    /// Missing target bytes are cast from the dependency during full decode;
    /// split-phase decode leaves the target for the DDL worker to set.
    pub fn with_changing_column(
        mut self,
        target_offset: usize,
        dependency_offset: usize,
    ) -> Result<Self, KvTableError> {
        if target_offset >= self.columns.len() || dependency_offset >= self.columns.len() {
            return Err(KvTableError::Decode(
                "changing-column dependency is outside the table schema".to_owned(),
            ));
        }
        for offset in [target_offset, dependency_offset] {
            self.decoded_offsets.insert(offset);
            self.column_types.insert(
                self.columns[offset].id,
                self.columns[offset].field_type.clone(),
            );
        }
        self.changing_dependencies
            .insert(target_offset, dependency_offset);
        Ok(self)
    }

    pub(crate) fn decoded_column_ids(&self) -> impl Iterator<Item = i64> + '_ {
        self.column_types.keys().copied()
    }

    pub(crate) fn record_handle(&self, key: &[u8]) -> Result<TableHandle, KvTableError> {
        if self.common_handle_offsets.is_empty() {
            return decode_int_handle(key).map(TableHandle::Int);
        }
        let bytes = key
            .get(RECORD_ROW_KEY_LEN - 8..)
            .ok_or_else(|| KvTableError::Decode("record key is too short".to_owned()))?;
        Ok(TableHandle::Common(bytes.to_vec()))
    }

    /// Decodes and evaluates one value with an already decoded handle.
    pub fn decode_and_eval(
        &self,
        handle: &TableHandle,
        value: &[u8],
    ) -> Result<DecodedRow, KvTableError> {
        let mut decoded = self.decode_existing_inner(handle, value, false)?;
        self.eval_remaining(&mut decoded)?;
        Ok(decoded)
    }

    /// First phase of DDL column reorg: decodes existing values and ordinary
    /// defaults, but leaves changing and generated columns unevaluated.
    pub fn decode_existing(
        &self,
        handle: &TableHandle,
        value: &[u8],
    ) -> Result<DecodedRow, KvTableError> {
        self.decode_existing_inner(handle, value, true)
    }

    fn decode_existing_inner(
        &self,
        handle: &TableHandle,
        value: &[u8],
        split_phase: bool,
    ) -> Result<DecodedRow, KvTableError> {
        let mut by_id =
            decode_table_row_to_map(value, &self.column_types, Some(self.context.zone()))
                .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
        let mut values = vec![Datum::Null; self.columns.len()];
        for offset in &self.decoded_offsets {
            let column = &self.columns[*offset];
            if let Some(value) = by_id.get(&column.id) {
                values[*offset] = value.clone();
            }
        }
        fill_handle_columns_if(
            &self.columns,
            self.pk_handle_offset,
            &self.common_handle_offsets,
            &mut values,
            handle,
            HandleDecodeContext {
                zone: self.context.zone(),
                use_new_collation: self.use_new_collation,
            },
            |offset| self.decoded_offsets.contains(&offset),
        )?;
        for offset in self.handle_offsets() {
            let column = &self.columns[offset];
            if self.decoded_offsets.contains(&offset)
                && !column
                    .field_type
                    .need_restored_data_with_collation(self.use_new_collation)
            {
                by_id
                    .entry(column.id)
                    .or_insert_with(|| values[offset].clone());
            }
        }

        for offset in &self.decoded_offsets {
            let column = &self.columns[*offset];
            if by_id.contains_key(&column.id) {
                continue;
            }
            if self.generated_offsets.contains(offset)
                || (split_phase && self.changing_dependencies.contains_key(offset))
            {
                continue;
            }
            let value = if let Some(dependency) = self.changing_dependencies.get(offset) {
                self.changing_value(*offset, *dependency, &by_id)?
            } else {
                self.origin_default(*offset)?
            };
            values[*offset] = value.clone();
            if split_phase {
                by_id.insert(column.id, value);
            }
        }
        Ok(DecodedRow { values, by_id })
    }

    fn handle_offsets(&self) -> impl Iterator<Item = usize> + '_ {
        self.pk_handle_offset
            .into_iter()
            .chain(self.common_handle_offsets.iter().copied())
    }

    fn origin_default(&self, offset: usize) -> Result<Datum, KvTableError> {
        let column = &self.columns[offset];
        if column.origin_default.is_none() && column.field_type.flags() & NOT_NULL_FLAG != 0 {
            return Err(KvTableError::Decode("Miss column".to_owned()));
        }
        column
            .origin_default_value(self.context.origin_default_flags(), self.context.zone())
            .map_err(|error| KvTableError::Decode(error.to_string()))
    }

    fn changing_value(
        &self,
        target_offset: usize,
        dependency_offset: usize,
        row: &BTreeMap<i64, Datum>,
    ) -> Result<Datum, KvTableError> {
        let dependency_id = self.columns[dependency_offset].id;
        let Some(source) = row.get(&dependency_id) else {
            return self.origin_default(target_offset);
        };
        crate::driver::cast_table_value(
            source.clone(),
            &self.columns[target_offset].field_type,
            &self.columns[target_offset].name,
            self.context.expression(),
            false,
        )
        .map_err(|error| KvTableError::Decode(format!("{error:?}")))
    }

    /// Updates both row representations before [`Self::eval_remaining`].
    pub fn set_column_value(
        &self,
        row: &mut DecodedRow,
        offset: usize,
        value: Datum,
    ) -> Result<(), KvTableError> {
        let Some(column) = self.columns.get(offset) else {
            return Err(KvTableError::Decode(
                "decoded-row update is outside the table schema".to_owned(),
            ));
        };
        row.values[offset] = value.clone();
        row.by_id.insert(column.id, value);
        Ok(())
    }

    /// Evaluates the configured generated columns left-to-right and adds
    /// their values to the column-id map.
    pub fn eval_remaining(&self, row: &mut DecodedRow) -> Result<(), KvTableError> {
        let Some(evaluation_columns) = &self.evaluation_columns else {
            // No decoded virtual generated column: every value in `row` is
            // already final, so there is nothing left to evaluate.
            return Ok(());
        };
        crate::generated_column::materialize_with_conversion_flags(
            evaluation_columns,
            &mut row.values,
            false,
            self.context.expression(),
            self.context
                .origin_default_flags()
                .with_ignore_truncate_err(true),
        )
        .map_err(|error| KvTableError::Generation {
            column: error.column,
            detail: error.detail,
            eval: error.eval,
        })?;
        for offset in &self.generated_offsets {
            row.by_id
                .insert(self.columns[*offset].id, row.values[*offset].clone());
        }
        Ok(())
    }

    pub(crate) fn decode_record(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<(TableHandle, Vec<Datum>), KvTableError> {
        let handle = self.record_handle(key)?;
        let decoded = self.decode_and_eval(&handle, value)?;
        let (mut values, _) = decoded.into_parts();
        if let Some(keep) = &self.keep {
            let projected = keep
                .iter()
                .map(|offset| std::mem::replace(&mut values[*offset], Datum::Null))
                .collect();
            return Ok((handle, projected));
        }
        Ok((handle, values))
    }
}

/// The integer handle at the end of an encoded record key.
impl PreparedPointGetRowDecoder {
    /// The handle at the tail of an encoded record key, for the row-range arm
    /// of a prepared point read (a clustered primary-key prefix).
    pub(crate) fn record_handle(&self, key: &[u8]) -> Result<TableHandle, KvTableError> {
        if self.common_handle_offsets.is_empty() {
            return decode_int_handle(key).map(TableHandle::Int);
        }
        let bytes = key
            .get(RECORD_ROW_KEY_LEN - 8..)
            .ok_or_else(|| KvTableError::Decode("record key is too short".to_owned()))?;
        Ok(TableHandle::Common(bytes.to_vec()))
    }
}

/// The integer handle at the end of an encoded record key.
pub(crate) fn decode_int_handle(key: &[u8]) -> Result<i64, KvTableError> {
    let tail: [u8; 8] = key
        .get(key.len().wrapping_sub(8)..)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| KvTableError::Decode("record key is too short for a handle".to_owned()))?;
    Ok(i64::from_be_bytes(tail) ^ i64::MIN)
}

/// Restores integer or common-handle columns into a row.
pub(crate) fn fill_handle_columns(
    columns: &[KvColumn],
    pk_handle_offset: Option<usize>,
    common_handle_offsets: &[usize],
    row: &mut [Datum],
    handle: &TableHandle,
    zone: &SessionTimeZone,
    use_new_collation: bool,
) -> Result<(), KvTableError> {
    fill_handle_columns_if(
        columns,
        pk_handle_offset,
        common_handle_offsets,
        row,
        handle,
        HandleDecodeContext {
            zone,
            use_new_collation,
        },
        |_| true,
    )
}

#[derive(Clone, Copy)]
struct HandleDecodeContext<'a> {
    zone: &'a SessionTimeZone,
    use_new_collation: bool,
}

fn fill_handle_columns_if(
    columns: &[KvColumn],
    pk_handle_offset: Option<usize>,
    common_handle_offsets: &[usize],
    row: &mut [Datum],
    handle: &TableHandle,
    context: HandleDecodeContext<'_>,
    selected: impl Fn(usize) -> bool,
) -> Result<(), KvTableError> {
    let unflatten = |offset: usize, value: Datum| -> Result<Datum, KvTableError> {
        tidb_tablecodec::unflatten_datum(value, &columns[offset].field_type, Some(context.zone))
            .map_err(|error| KvTableError::Decode(format!("{error:?}")))
    };
    match handle {
        TableHandle::Int(value) => {
            if let Some(offset) = pk_handle_offset {
                if !selected(offset) {
                    return Ok(());
                }
                let decoded = if columns[offset].field_type.is_unsigned() {
                    Datum::UInt(*value as u64)
                } else {
                    Datum::Int(*value)
                };
                row[offset] = unflatten(offset, decoded)?;
            }
        }
        TableHandle::Common(bytes) => {
            let mut rest: &[u8] = bytes;
            for offset in common_handle_offsets {
                let (remaining, value) = tidb_codec::decode_one(rest)
                    .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                rest = remaining;
                // Go's `DecodeHandleToDatumMap` deliberately skips a common-
                // handle column whose collation key loses information. The
                // original bytes are carried in the row value; decoding the
                // handle here would replace them with the sort key.
                if selected(*offset)
                    && !columns[*offset]
                        .field_type
                        .need_restored_data_with_collation(context.use_new_collation)
                {
                    row[*offset] = unflatten(*offset, value)?;
                }
            }
        }
    }
    Ok(())
}
