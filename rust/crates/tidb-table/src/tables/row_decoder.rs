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
use std::{borrow::Cow, sync::Arc};

use smallvec::SmallVec;

use tidb_chunk::chunk::Chunk;
use tidb_rowcodec::RawRowValue;
use tidb_tablecodec::table_key::RECORD_ROW_KEY_LEN;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, SessionTimeZone};
use tidb_tablecodec::decode_table_row_to_map;
use tidb_txnkv::CommonHandle;

use tidb_datatype::UNSPECIFIED_LENGTH;
use super::raw_row::common_handle_columns;
use super::table_meta::{NOT_NULL_FLAG, PreparedPointGetDecodeContext};
use super::{KvTableError, RowDecodeContext, TableHandle};

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
    pub values: Vec<Datum>,
    pub by_id: BTreeMap<i64, Datum>,
}

impl DecodedRow {
    /// Values in table-column order, including decoded defaults and generated values.
    pub fn values(&self) -> &[Datum] {
        &self.values
    }

    /// The decoded column-id map.
    pub fn by_id(&self) -> &BTreeMap<i64, Datum> {
        &self.by_id
    }

    /// Takes both representations.
    pub fn into_parts(self) -> (Vec<Datum>, BTreeMap<i64, Datum>) {
        (self.values, self.by_id)
    }
}

/// Everything needed to turn one record key/value pair into a table row.
#[derive(Clone)]
pub struct RowDecoder {
    /// The table schema this decoder reads, SHARED with the catalog's
    /// `TableCommon` (`Arc`): building the decoder per execute must not deep-copy
    /// every column's name and collation strings again.
    columns: Arc<Vec<crate::column::Column>>,
    /// Columns for generated-column evaluation: the shared schema when any
    /// virtual generated column is decoded, with every non-generated entry's
    /// expression nulled; `None` when there is nothing to evaluate, so the
    /// common no-generated-columns case never copies the schema at all.
    evaluation_columns: Option<Arc<Vec<crate::column::Column>>>,
    column_types: BTreeMap<i64, FieldType>,
    decoded_offsets: BTreeSet<usize>,
    generated_offsets: BTreeSet<usize>,
    changing_dependencies: BTreeMap<usize, usize>,
    pk_handle_offset: Option<usize>,
    common_handle_offsets: Vec<usize>,
    common_handle_prefix_lengths: Vec<i64>,
    use_new_collation: bool,
    keep: Option<Vec<ProjectedColumn>>,
    context: RowDecodeContext,
    /// Table/point readers use FillVirtualColumnValue; reorg uses RowDecoder.
    fill_virtual_values: bool,
    /// Row V2 metadata built once per cursor rather than once per row.
    v2_columns: Vec<tidb_rowcodec::ColumnInfo>,
    /// Column IDs supplied by the clustered handle in Row V2.
    v2_handle_column_ids: Vec<i64>,
    /// The common table-read shape can decode directly into a datum vector.
    /// Generated/default/DDL shapes retain the map-based compatibility path.
    v2_fast_path: bool,
}

/// The last use moves a decoded datum; earlier duplicate uses must clone it.
#[derive(Clone)]
struct ProjectedColumn {
    offset: usize,
    consume: bool,
}

fn projection_columns(keep: &[usize]) -> Vec<ProjectedColumn> {
    let mut seen = BTreeSet::new();
    let mut columns: Vec<_> = keep
        .iter()
        .rev()
        .map(|offset| ProjectedColumn {
            offset: *offset,
            consume: seen.insert(*offset),
        })
        .collect();
    columns.reverse();
    columns
}

#[derive(Clone, Debug)]
struct PreparedPointGetColumn {
    column: crate::column::Column,
    from_handle: bool,
    common_handle_part: Option<usize>,
    take_stored: bool,
}

#[derive(Clone, Copy, Debug)]
struct SingleHandleProjection {
    part: usize,
    integer: bool,
}

#[derive(Clone, Copy)]
enum PreparedHandleRef<'a> {
    Int(i64),
    Common(&'a [u8]),
    Indexed(&'a CommonHandle),
}

impl<'a> From<&'a TableHandle> for PreparedHandleRef<'a> {
    fn from(handle: &'a TableHandle) -> Self {
        match handle {
            TableHandle::Int(value) => Self::Int(*value),
            TableHandle::Common(bytes) => Self::Common(bytes),
        }
    }
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
    common_handle_parts: Vec<usize>,
    single_handle_projection: Option<SingleHandleProjection>,
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
    v2_columns: Vec<tidb_rowcodec::ColumnInfo>,
    v2_handle_column_ids: Vec<i64>,
    direct_projection: Option<DirectProjection>,
}

#[derive(Clone, Copy, Debug)]
enum DirectProjection {
    Strings,
    SignedInt,
    UnsignedInt,
}

impl PreparedPointGetRowDecoder {
    /// Compiles the schema-versioned projection stored on the point plan.
    pub fn new(
        columns: &[crate::column::Column],
        pk_handle_offset: usize,
        output_offsets: &[usize],
    ) -> Result<Self, KvTableError> {
        Self::new_with_handles(columns, Some(pk_handle_offset), &[], output_offsets)
    }

    /// Compiles a prepared point-get projection for either an integer or a
    /// common clustered handle.
    pub fn new_with_handles(
        columns: &[crate::column::Column],
        pk_handle_offset: Option<usize>,
        common_handle_offsets: &[usize],
        output_offsets: &[usize],
    ) -> Result<Self, KvTableError> {
        let prefix_lengths = vec![UNSPECIFIED_LENGTH; common_handle_offsets.len()];
        Self::new_with_handle_prefixes(
            columns,
            pk_handle_offset,
            common_handle_offsets,
            &prefix_lengths,
            output_offsets,
        )
    }

    /// Compiles a point-get decoder that preserves primary-key prefix values
    /// from row storage rather than replacing them with their truncated handle.
    pub fn new_with_handle_prefixes(
        columns: &[crate::column::Column],
        pk_handle_offset: Option<usize>,
        common_handle_offsets: &[usize],
        common_handle_prefix_lengths: &[i64],
        output_offsets: &[usize],
    ) -> Result<Self, KvTableError> {
        if common_handle_offsets.len() != common_handle_prefix_lengths.len() {
            return Err(KvTableError::Decode(
                "common-handle prefix metadata does not match its columns".to_owned(),
            ));
        }
        let common_handle_parts: Vec<usize> = common_handle_prefix_lengths
            .iter()
            .enumerate()
            .filter_map(|(part, length)| (*length == UNSPECIFIED_LENGTH).then_some(part))
            .collect();
        let common_handle_column_ids: Vec<i64> = common_handle_parts
            .iter()
            .filter_map(|part| columns.get(common_handle_offsets[*part]).map(|column| column.read().id))
            .collect();
        let mut projected = output_offsets
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
                    take_stored: false,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut seen = BTreeSet::new();
        for output in projected.iter_mut().rev() {
            if output.common_handle_part.is_some_and(|part| {
                common_handle_prefix_lengths[part] != UNSPECIFIED_LENGTH
            }) {
                output.common_handle_part = None;
            }
            if !output.from_handle && output.common_handle_part.is_none() {
                output.take_stored = seen.insert(output.column.read().id);
            }
        }
        let stored_column_types = projected
            .iter()
            .filter(|column| !column.from_handle)
            .map(|column| (column.column.read().id, column.column.read().field_type.clone()))
            .collect();
        let v2_columns: Vec<_> = projected
            .iter()
            .map(|output| tidb_rowcodec::ColumnInfo {
                id: output.column.read().id,
                is_pk_handle: output.from_handle,
                virtual_generated: false,
                field_type: output.column.read().field_type.clone(),
            })
            .collect::<Vec<_>>();
        let v2_handle_column_ids: Vec<_> = projected
            .iter()
            .find(|output| output.from_handle)
            .map(|output| output.column.read().id)
            .into_iter()
            .chain(common_handle_column_ids.iter().copied())
            .collect();
        let needs_handle = projected
            .iter()
            .any(|output| output.from_handle || output.common_handle_part.is_some());
        let has_origin_defaults = projected
            .iter()
            .any(|output| output.column.origin_default.is_some());
        let single_handle_projection = match projected.as_slice() {
            [output]
                if !output.from_handle
                    && common_handle_parts.len() == common_handle_offsets.len() =>
            {
                let field_type = &v2_columns[0].field_type;
                let id = v2_columns[0].id;
                output
                    .common_handle_part
                    .filter(|&part| {
                        !field_type.need_restored_data()
                            && v2_handle_column_ids.iter().position(|&column_id| column_id == id)
                                == Some(part)
                    })
                    .map(|part| SingleHandleProjection {
                        part,
                        integer: field_type.code().is_integer_type(),
                    })
            }
            _ => None,
        };
        let direct_projection = if needs_handle {
            None
        } else {
            match v2_columns.as_slice() {
                [column] if column.field_type.code() == FieldTypeCode::Year => {
                    Some(DirectProjection::SignedInt)
                }
                [column] if column.field_type.code().is_integer_type() => {
                    Some(if column.field_type.is_unsigned() {
                        DirectProjection::UnsignedInt
                    } else {
                        DirectProjection::SignedInt
                    })
                }
                columns if !columns.is_empty() && columns.iter().all(|column| {
                    let code = column.field_type.code();
                    code.is_string() && !code.is_type_unspecified()
                }) => Some(DirectProjection::Strings),
                _ => None,
            }
        };
        Ok(Self {
            columns: projected,
            stored_column_types,
            common_handle_parts,
            single_handle_projection,
            needs_handle,
            has_origin_defaults,
            v2_columns,
            v2_handle_column_ids,
            direct_projection,
        })
    }

    pub fn decode(
        &self,
        handle: &TableHandle,
        value: &[u8],
        context: &PreparedPointGetDecodeContext,
    ) -> Result<Vec<Datum>, KvTableError> {
        self.decode_row(handle.into(), Cow::Borrowed(value), context)
    }

    /// Projects an owned storage row and retains the backing for wide strings.
    pub fn decode_owned(
        &self,
        handle: &TableHandle,
        value: Vec<u8>,
        context: &PreparedPointGetDecodeContext,
    ) -> Result<Vec<Datum>, KvTableError> {
        self.decode_row(handle.into(), Cow::Owned(value), context)
    }

    pub(super) fn decode_storage_value(
        &self,
        handle: &TableHandle,
        value: Cow<'_, [u8]>,
        context: &PreparedPointGetDecodeContext,
    ) -> Result<Vec<Datum>, KvTableError> {
        self.decode_row(handle.into(), value, context)
    }

    pub(super) fn prefers_borrowed_storage(&self) -> bool {
        // Multi-column decoding can share one owned buffer and runs outside
        // the native storage read guard.
        self.columns.len() == 1
    }

    /// Whether the projection can write stored values into the output chunk.
    pub fn can_append_projected_row(&self) -> bool {
        self.direct_projection.is_some()
    }

    /// Writes a complete Row V2 projection, or falls back before writing to the chunk.
    pub fn try_append_v2_row(&self, value: &[u8], chunk: &mut Chunk) -> Result<bool, KvTableError> {
        let Some(projection) = self.direct_projection else {
            return Ok(false);
        };
        if !tidb_rowcodec::is_new_format(value) {
            return Ok(false);
        }
        self.append_v2_projection(projection, value, chunk).map_err(|error| {
            KvTableError::Decode(format!("{:?}", tidb_rowcodec::RowPackageError::from(error)))
        })
    }

    pub(super) fn append_storage_value(
        &self,
        handle: &TableHandle,
        value: Cow<'_, [u8]>,
        context: &PreparedPointGetDecodeContext,
        chunk: &mut Chunk,
    ) -> Result<(), KvTableError> {
        self.append_row(handle.into(), value, context, chunk)
    }

    pub(super) fn append_indexed_common(
        &self,
        handle: &CommonHandle,
        value: Cow<'_, [u8]>,
        context: &PreparedPointGetDecodeContext,
        chunk: &mut Chunk,
    ) -> Result<(), KvTableError> {
        self.append_row(PreparedHandleRef::Indexed(handle), value, context, chunk)
    }

    fn append_row(
        &self,
        handle: PreparedHandleRef<'_>,
        value: Cow<'_, [u8]>,
        context: &PreparedPointGetDecodeContext,
        chunk: &mut Chunk,
    ) -> Result<(), KvTableError> {
        if let Some(projection) = self.direct_projection {
            if tidb_rowcodec::is_new_format(&value)
                && self.append_v2_projection(projection, &value, chunk).map_err(|error| {
                    KvTableError::Decode(format!("{:?}", tidb_rowcodec::RowPackageError::from(error)))
                })?
            {
                return Ok(());
            }
        }
        for (column, value) in self.decode_row(handle, value, context)?.iter().enumerate() {
            chunk.append_datum(column, value);
        }
        Ok(())
    }

    fn append_v2_projection(
        &self,
        projection: DirectProjection,
        value: &[u8],
        chunk: &mut Chunk,
    ) -> Result<bool, tidb_rowcodec::RowDecodeError> {
        let (row, _) = tidb_rowcodec::RowDecoder::parse(value)?;
        if let [column] = self.v2_columns.as_slice() {
            match row.column(column.id)? {
                RawRowValue::NotNull { bytes, .. } => match projection {
                    DirectProjection::Strings => chunk.append_bytes(0, bytes),
                    DirectProjection::SignedInt => chunk.append_int64(0, tidb_rowcodec::decode_raw_int(bytes)?),
                    DirectProjection::UnsignedInt => chunk.append_uint64(0, tidb_rowcodec::decode_raw_uint(bytes)?),
                },
                RawRowValue::Missing if self.has_origin_defaults => return Ok(false),
                RawRowValue::Null | RawRowValue::Missing => chunk.append_null(0),
            }
            return Ok(true);
        }

        // Resolve the entire projection before writing any column so a missing
        // default or an invalid offset can leave the chunk untouched.
        let mut projected = SmallVec::<[Option<&[u8]>; 16]>::new();
        for column in &self.v2_columns {
            match row.column(column.id)? {
                RawRowValue::NotNull { bytes, .. } => projected.push(Some(bytes)),
                RawRowValue::Missing if self.has_origin_defaults => return Ok(false),
                RawRowValue::Null | RawRowValue::Missing => projected.push(None),
            }
        }
        for (column, value) in projected.into_iter().enumerate() {
            match value {
                Some(bytes) => chunk.append_bytes(column, bytes),
                None => chunk.append_null(column),
            }
        }
        Ok(true)
    }

    /// Projects a row using the common handle's existing key-part offsets.
    pub fn decode_indexed_common(
        &self,
        handle: &CommonHandle,
        value: Cow<'_, [u8]>,
        context: &PreparedPointGetDecodeContext,
    ) -> Result<Vec<Datum>, KvTableError> {
        self.decode_row(PreparedHandleRef::Indexed(handle), value, context)
    }

    fn decode_row(
        &self,
        handle: PreparedHandleRef<'_>,
        value: Cow<'_, [u8]>,
        context: &PreparedPointGetDecodeContext,
    ) -> Result<Vec<Datum>, KvTableError> {
        // A key-only v2 row has no stored columns to override the handle.
        if let (
            Some(SingleHandleProjection { part, integer }),
            [tidb_rowcodec::ROW_CODEC_VERSION, 0, 0, 0, 0, 0],
        ) = (self.single_handle_projection, value.as_ref())
        {
            let encoded_part = match handle {
                PreparedHandleRef::Indexed(handle) => handle.encoded_column(part),
                // Raw bytes require validation of every part, even when the
                // projection only needs one. The indexed handle has done this.
                PreparedHandleRef::Common(encoded) if encoded.len() <= usize::from(u16::MAX) => {
                    if integer {
                        if let Some(datum) = project_fixed_integer_handle(encoded, part) {
                            return Ok(vec![datum]);
                        }
                    }
                    find_common_handle_part(encoded, part)?
                }
                _ => None,
            };
            if let Some(encoded) = encoded_part {
                if let Ok((_, datum)) = tidb_codec::decode_one_typed_in_timezone(
                    encoded,
                    &self.v2_columns[0].field_type,
                    Some(context.zone()),
                ) {
                    return Ok(vec![datum]);
                }
            }
        }
        // Row V2 stores a column directory and can decode directly into the
        // requested projection. This avoids the legacy BTreeMap allocation on
        // every prepared point read (the YCSB hot path).
        if tidb_rowcodec::is_new_format(&value) {
            let mut int_handle = None;
            let mut common_parts = None;
            if self.needs_handle {
                match handle {
                    PreparedHandleRef::Int(value) => {
                        int_handle = Some(tidb_rowcodec::Handle::Int(value));
                    }
                    PreparedHandleRef::Common(encoded) => {
                        let parts = common_handle_columns(encoded)?;
                        common_parts = Some(
                            self.common_handle_parts
                                .iter()
                                .filter_map(|&part| parts.get(part).copied().flatten())
                                .collect::<SmallVec<[_; 16]>>(),
                        );
                    }
                    PreparedHandleRef::Indexed(handle) => {
                        common_parts = Some(
                            self.common_handle_parts
                                .iter()
                                .filter_map(|&part| handle.encoded_column(part))
                                .collect::<SmallVec<[_; 16]>>(),
                        );
                    }
                }
            }
            let default_datum = |index: usize| {
                self.columns[index]
                    .column
                    .origin_default_value(context.origin_default_flags(), context.zone())
                    .map_err(|error| error.to_string())
            };
            return tidb_rowcodec::decode_row_to_datums_with_refs(
                value,
                &self.v2_columns,
                &tidb_rowcodec::DecodeRowOptions {
                    handle_column_ids: &self.v2_handle_column_ids,
                    handle: int_handle.as_ref(),
                    default_datum: self.has_origin_defaults.then_some(
                        &default_datum as &(dyn Fn(usize) -> Result<Datum, String> + '_),
                    ),
                    timezone: Some(context.zone()),
                    ..tidb_rowcodec::DecodeRowOptions::default()
                },
                common_parts.as_deref(),
            )
            .map(|row| row.values)
            .map_err(|error| KvTableError::Decode(format!("{error:?}")));
        }
        let mut decoded = match value {
            Cow::Borrowed(bytes) => {
                decode_table_row_to_map(bytes, &self.stored_column_types, Some(context.zone()))
            }
            Cow::Owned(bytes) => {
                tidb_tablecodec::decode_owned_table_row_to_map(
                    bytes,
                    &self.stored_column_types,
                    Some(context.zone()),
                )
            }
        }
        .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
        let mut handle_columns = None;
        self.columns
            .iter()
            .map(|output| {
                if output.from_handle {
                    let PreparedHandleRef::Int(value) = handle else {
                        return Err(KvTableError::Decode(
                            "prepared point-get plan requires an integer handle".to_owned(),
                        ));
                    };
                    return tidb_tablecodec::unflatten_datum(
                        Datum::Int(value),
                        &output.column.read().field_type,
                        Some(context.zone()),
                    )
                    .map_err(|error| KvTableError::Decode(format!("{error:?}")));
                }
                if let Some(part) = output.common_handle_part {
                    let encoded = match handle {
                        PreparedHandleRef::Common(encoded) => {
                            let columns = match &handle_columns {
                                Some(columns) => columns,
                                None => handle_columns.insert(common_handle_columns(encoded)?),
                            };
                            columns.get(part).copied().flatten()
                        }
                        PreparedHandleRef::Indexed(handle) => handle.encoded_column(part),
                        PreparedHandleRef::Int(_) => {
                            return Err(KvTableError::Decode(
                                "prepared point-get plan requires a common handle".to_owned(),
                            ));
                        }
                    };
                    let Some(encoded) = encoded else {
                        return Err(KvTableError::Decode(
                            "prepared point-get common handle is missing a column".to_owned(),
                        ));
                    };
                    let (remainder, value) = tidb_codec::decode_one_typed_in_timezone(
                        encoded,
                        &output.column.read().field_type,
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
                if let Some(value) = decoded.get_mut(&output.column.read().id) {
                    return Ok(if output.take_stored {
                        std::mem::replace(value, Datum::Null)
                    } else {
                        value.clone()
                    });
                }
                if output.column.origin_default.is_none()
                    && output.column.read().field_type.flags() & NOT_NULL_FLAG != 0
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

fn find_common_handle_part(encoded: &[u8], part: usize) -> Result<Option<&[u8]>, KvTableError> {
    let mut index = 0;
    let mut selected = None;
    CommonHandle::scan_encoded_columns(encoded, |column| {
        if index == part {
            selected = Some(column);
        }
        index += 1;
    })
    .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
    Ok(selected)
}

fn project_fixed_integer_handle(encoded: &[u8], part: usize) -> Option<Datum> {
    use tidb_codec::{INT_FLAG, UINT_FLAG, decode_cmp_uint_to_int};

    // The general decoder uses wrapping 16-bit column endpoints.
    let (columns, tail) = encoded.as_chunks::<9>();
    if !tail.is_empty() || encoded.len() > usize::from(u16::MAX) {
        return None;
    }
    let &[flag, payload @ ..] = columns.get(part)?;
    if !columns
        .iter()
        .all(|column| matches!(column[0], INT_FLAG | UINT_FLAG))
    {
        return None;
    }
    let bits = u64::from_be_bytes(payload);
    Some(if flag == INT_FLAG {
        Datum::Int(decode_cmp_uint_to_int(bits))
    } else {
        Datum::UInt(bits)
    })
}

impl RowDecoder {
    /// Builds a full-schema decoder, equivalent to Go `NewRowDecoder` with
    /// `BuildFullDecodeColMap` when `generated` is [`GeneratedColumnSelection::All`].
    pub fn new(
        columns: Vec<crate::column::Column>,
        pk_handle_offset: Option<usize>,
        common_handle_offsets: Vec<usize>,
        generated: GeneratedColumnSelection,
        context: RowDecodeContext,
    ) -> Result<Self, KvTableError> {
        let common_handle_prefix_lengths =
            vec![UNSPECIFIED_LENGTH; common_handle_offsets.len()];
        Self::build(
            Arc::new(columns),
            pk_handle_offset,
            common_handle_offsets,
            common_handle_prefix_lengths,
            generated,
            None,
            context.expression().new_collation_enabled(),
            context,
        )
    }

    /// Builds a decoder for a subset of table-column offsets. Dependencies
    /// of selected generated columns are included automatically.
    pub fn projected(
        columns: Vec<crate::column::Column>,
        pk_handle_offset: Option<usize>,
        common_handle_offsets: Vec<usize>,
        generated: GeneratedColumnSelection,
        offsets: &[usize],
        context: RowDecodeContext,
    ) -> Result<Self, KvTableError> {
        let common_handle_prefix_lengths =
            vec![UNSPECIFIED_LENGTH; common_handle_offsets.len()];
        Self::build(
            Arc::new(columns),
            pk_handle_offset,
            common_handle_offsets,
            common_handle_prefix_lengths,
            generated,
            Some(offsets),
            context.expression().new_collation_enabled(),
            context,
        )
    }

    pub fn for_table_read(
        columns: Arc<Vec<crate::column::Column>>,
        pk_handle_offset: Option<usize>,
        common_handle_offsets: Vec<usize>,
        common_handle_prefix_lengths: Vec<i64>,
        keep: Option<&[usize]>,
        use_new_collation: bool,
        context: RowDecodeContext,
    ) -> Result<Self, KvTableError> {
        let mut decoder = Self::build(
            columns,
            pk_handle_offset,
            common_handle_offsets,
            common_handle_prefix_lengths,
            GeneratedColumnSelection::Virtual,
            keep,
            use_new_collation,
            context,
        )?;
        decoder.fill_virtual_values = true;
        Ok(decoder)
    }

    pub fn for_recomputed_read(
        columns: Arc<Vec<crate::column::Column>>,
        pk_handle_offset: Option<usize>,
        common_handle_offsets: Vec<usize>,
        common_handle_prefix_lengths: Vec<i64>,
        use_new_collation: bool,
        context: RowDecodeContext,
    ) -> Result<Self, KvTableError> {
        Self::build(
            columns,
            pk_handle_offset,
            common_handle_offsets,
            common_handle_prefix_lengths,
            GeneratedColumnSelection::All,
            None,
            use_new_collation,
            context,
        )
    }

    fn build(
        columns: Arc<Vec<crate::column::Column>>,
        pk_handle_offset: Option<usize>,
        common_handle_offsets: Vec<usize>,
        common_handle_prefix_lengths: Vec<i64>,
        generated: GeneratedColumnSelection,
        keep: Option<&[usize]>,
        use_new_collation: bool,
        context: RowDecodeContext,
    ) -> Result<Self, KvTableError> {
        let context = context.with_new_collation_enabled(use_new_collation);
        let width = columns.len();
        if common_handle_offsets.len() != common_handle_prefix_lengths.len()
            || pk_handle_offset.is_some_and(|offset| offset >= width)
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
                                columns[offset].read().name
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
            .map(|offset| (columns[*offset].read().id, columns[*offset].read().field_type.clone()))
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

        let v2_columns = columns
            .iter()
            .enumerate()
            .map(|(offset, column)| tidb_rowcodec::ColumnInfo {
                id: column.read().id,
                is_pk_handle: pk_handle_offset == Some(offset),
                virtual_generated: false,
                field_type: column.read().field_type.clone(),
            })
            .collect();
        let v2_handle_column_ids = pk_handle_offset
            .and_then(|offset| columns.get(offset).map(|column| column.read().id))
            .into_iter()
            .chain(
                common_handle_offsets
                    .iter()
                    .zip(&common_handle_prefix_lengths)
                    .filter(|(_, length)| **length == UNSPECIFIED_LENGTH)
                    .filter_map(|(offset, _)| columns.get(*offset).map(|column| column.read().id)),
            )
            .collect();
        let v2_fast_path = generated_offsets.is_empty()
            && columns.iter().all(|column| column.origin_default.is_none())
            // The typed rowcodec fast path uses the default (new-collation)
            // restored-data policy. Old-collation common handles must use the
            // map path so `fill_handle_columns_if` can materialize their
            // handle components, matching Go's mode-sensitive decoder.
            && (use_new_collation || common_handle_offsets.is_empty());

        Ok(Self {
            columns,
            evaluation_columns,
            column_types,
            decoded_offsets,
            generated_offsets,
            changing_dependencies: BTreeMap::new(),
            pk_handle_offset,
            common_handle_offsets,
            common_handle_prefix_lengths,
            use_new_collation,
            keep: keep.map(projection_columns),
            context,
            fill_virtual_values: false,
            v2_columns,
            v2_handle_column_ids,
            v2_fast_path,
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
                self.columns[offset].read().id,
                self.columns[offset].read().field_type.clone(),
            );
        }
        self.changing_dependencies
            .insert(target_offset, dependency_offset);
        self.v2_fast_path = false;
        Ok(self)
    }

    pub fn decoded_column_ids(&self) -> impl Iterator<Item = i64> + '_ {
        self.column_types.keys().copied()
    }

    /// Physical inputs needed before evaluating this decoder's projection.
    /// Dependency expansion is shared with byte-level row decoding.
    /// Every column the projection decodes, generated ones included: Go's
    /// `TableScan.Columns` after `ExpandVirtualColumn`.
    pub fn decoded_offsets(&self) -> Vec<usize> {
        self.decoded_offsets.iter().copied().collect()
    }

    pub fn physical_offsets(&self) -> Vec<usize> {
        self.decoded_offsets
            .iter()
            .copied()
            .filter(|offset| !self.generated_offsets.contains(offset))
            .collect()
    }

    /// Completes an already decoded coprocessor row, including virtual
    /// expressions and the caller's projection, just like a local record.
    pub fn project_remote_row(
        &self,
        offsets: &[usize],
        values: Vec<Datum>,
    ) -> Result<Vec<Datum>, KvTableError> {
        if offsets.len() != values.len() {
            return Err(KvTableError::Decode(
                "remote row does not match its physical projection".to_owned(),
            ));
        }
        let mut decoded = DecodedRow {
            values: vec![Datum::Null; self.columns.len()],
            by_id: BTreeMap::new(),
        };
        for (offset, value) in offsets.iter().zip(values) {
            self.set_column_value(&mut decoded, *offset, value)?;
        }
        self.eval_remaining(&mut decoded)?;
        Ok(self.project_values(decoded.values))
    }

    pub fn record_handle(&self, key: &[u8]) -> Result<TableHandle, KvTableError> {
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
            if let Some(value) = by_id.get(&column.read().id) {
                values[*offset] = value.clone();
            }
        }
        fill_handle_columns_if(
            &self.columns,
            self.pk_handle_offset,
            &self.common_handle_offsets,
            &self.common_handle_prefix_lengths,
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
                    .read().field_type
                    .need_restored_data_with_collation(self.use_new_collation)
            {
                by_id
                    .entry(column.read().id)
                    .or_insert_with(|| values[offset].clone());
            }
        }

        for offset in &self.decoded_offsets {
            let column = &self.columns[*offset];
            if by_id.contains_key(&column.read().id) {
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
                by_id.insert(column.read().id, value);
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
        if column.origin_default.is_none() && column.read().field_type.flags() & NOT_NULL_FLAG != 0 {
            return Err(KvTableError::Decode("Miss column".to_owned()));
        }
        column
            .origin_default_value(self.context.type_flags(), self.context.zone())
            .map_err(|error| KvTableError::Decode(error.to_string()))
    }

    fn changing_value(
        &self,
        target_offset: usize,
        dependency_offset: usize,
        row: &BTreeMap<i64, Datum>,
    ) -> Result<Datum, KvTableError> {
        let dependency_id = self.columns[dependency_offset].read().id;
        let Some(source) = row.get(&dependency_id) else {
            return self.origin_default(target_offset);
        };
        crate::column::cast_table_value_with_flags(
            source.clone(),
            &self.columns[target_offset].read().field_type,
            self.columns[target_offset].read().name.original(),
            self.context.expression(),
            self.context.type_flags(),
            false,
        )
        .map_err(|error| KvTableError::Mysql(error.to_mysql_error()))
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
        row.by_id.insert(column.read().id, value);
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
        crate::generated_column::materialize_with(
            evaluation_columns,
            &mut row.values,
            false,
            self.context.expression(),
            |column, value| {
                let cast = if self.fill_virtual_values {
                    crate::generated_column::cast_virtual_column_value(
                        value,
                        &column.read().field_type,
                        column.read().name.original(),
                        self.context.expression(),
                        self.context.type_flags(),
                    )
                } else {
                    crate::column::cast_table_value_with_flags(
                        value,
                        &column.read().field_type,
                        column.read().name.original(),
                        self.context.expression(),
                        self.context.type_flags(),
                        true,
                    )
                };
                cast.map_err(|error| KvTableError::Mysql(error.to_mysql_error()))
            },
        )?;
        for offset in &self.generated_offsets {
            row.by_id
                .insert(self.columns[*offset].read().id, row.values[*offset].clone());
        }
        Ok(())
    }

    pub fn decode_record(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<(TableHandle, Vec<Datum>), KvTableError> {
        let handle = self.record_handle(key)?;
        if self.v2_fast_path && tidb_rowcodec::is_new_format(value) {
            let codec_handle = match &handle {
                TableHandle::Int(value) => tidb_rowcodec::Handle::Int(*value),
                TableHandle::Common(encoded) => {
                    let common = tidb_txnkv::CommonHandle::new(encoded.clone())
                        .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                    tidb_rowcodec::Handle::Common(
                        self.common_handle_offsets
                            .iter()
                            .enumerate()
                            .filter(|(position, _)| {
                                self.common_handle_prefix_lengths.get(*position).copied()
                                    == Some(UNSPECIFIED_LENGTH)
                            })
                            .filter_map(|(part, _)| {
                                common.encoded_column(part).map(<[u8]>::to_vec)
                            })
                            .collect(),
                    )
                }
            };
            let values = tidb_rowcodec::decode_row_to_datums(
                value,
                &self.v2_columns,
                &tidb_rowcodec::DecodeRowOptions {
                    handle_column_ids: &self.v2_handle_column_ids,
                    handle: Some(&codec_handle),
                    timezone: Some(self.context.zone()),
                    ..tidb_rowcodec::DecodeRowOptions::default()
                },
            )
            .map_err(|error| KvTableError::Decode(format!("{error:?}")))?
            .values;
            return Ok((handle, self.project_values(values)));
        }
        let decoded = self.decode_and_eval(&handle, value)?;
        let (values, _) = decoded.into_parts();
        Ok((handle, self.project_values(values)))
    }

    fn project_values(&self, mut values: Vec<Datum>) -> Vec<Datum> {
        let Some(keep) = &self.keep else {
            return values;
        };
        keep.iter()
            .map(|column| {
                if column.consume {
                    std::mem::replace(&mut values[column.offset], Datum::Null)
                } else {
                    values[column.offset].clone()
                }
            })
            .collect()
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
pub fn fill_handle_columns(
    columns: &[crate::column::Column],
    pk_handle_offset: Option<usize>,
    common_handle_offsets: &[usize],
    common_handle_prefix_lengths: &[i64],
    row: &mut [Datum],
    handle: &TableHandle,
    zone: &SessionTimeZone,
    use_new_collation: bool,
) -> Result<(), KvTableError> {
    fill_handle_columns_if(
        columns,
        pk_handle_offset,
        common_handle_offsets,
        common_handle_prefix_lengths,
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
    columns: &[crate::column::Column],
    pk_handle_offset: Option<usize>,
    common_handle_offsets: &[usize],
    common_handle_prefix_lengths: &[i64],
    row: &mut [Datum],
    handle: &TableHandle,
    context: HandleDecodeContext<'_>,
    selected: impl Fn(usize) -> bool,
) -> Result<(), KvTableError> {
    let unflatten = |offset: usize, value: Datum| -> Result<Datum, KvTableError> {
        tidb_tablecodec::unflatten_datum(value, &columns[offset].read().field_type, Some(context.zone))
            .map_err(|error| KvTableError::Decode(format!("{error:?}")))
    };
    match handle {
        TableHandle::Int(value) => {
            if let Some(offset) = pk_handle_offset {
                if !selected(offset) {
                    return Ok(());
                }
                let decoded = if columns[offset].read().field_type.is_unsigned() {
                    Datum::UInt(*value as u64)
                } else {
                    Datum::Int(*value)
                };
                row[offset] = unflatten(offset, decoded)?;
            }
        }
        TableHandle::Common(bytes) => {
            let mut rest: &[u8] = bytes;
            for (position, offset) in common_handle_offsets.iter().enumerate() {
                let (remaining, value) = tidb_codec::decode_one(rest)
                    .map_err(|error| KvTableError::Decode(format!("{error:?}")))?;
                rest = remaining;
                // Go's `DecodeHandleToDatumMap` deliberately skips a common-
                // handle column whose collation key loses information. The
                // original bytes are carried in the row value; decoding the
                // handle here would replace them with the sort key.
                let prefix_length = common_handle_prefix_lengths
                    .get(position)
                    .copied()
                    .unwrap_or(UNSPECIFIED_LENGTH);
                if prefix_length == UNSPECIFIED_LENGTH
                    && selected(*offset)
                    && !columns[*offset]
                        .read().field_type
                        .need_restored_data_with_collation(context.use_new_collation)
                {
                    row[*offset] = unflatten(*offset, value)?;
                }
            }
        }
    }
    Ok(())
}

/// Go `fillRowChecksum` for one point-get row: `NULL` for a row in the old
/// format, otherwise the decimal text of `RowData.Checksum` over every table
/// column sorted by ID -- the stored values (`DecodeRowWithMapNew`), the
/// handle columns the value does not carry (`DecodeHandleToDatumMap`), and
/// each still-missing column's `GetColOriginDefaultValue`.
pub fn fill_row_checksum(
    table: &super::TableCommon,
    handle: &TableHandle,
    value: &[u8],
    context: &RowDecodeContext,
) -> Result<Datum, KvTableError> {
    if !tidb_rowcodec::is_new_format(value) {
        return Ok(Datum::Null);
    }
    let decode_error = |error: &dyn std::fmt::Debug| KvTableError::Decode(format!("{error:?}"));
    let zone = context.zone();
    let columns = table.public_columns.as_slice();
    let column_types = columns
        .iter()
        .map(|column| (column.read().id, column.read().field_type.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut datums = decode_table_row_to_map(value, &column_types, Some(zone))
        .map_err(|error| decode_error(&error))?;
    // Go: `PKIsHandle` names the integer handle column, `IsCommonHandle`
    // the primary index's columns in index order.
    let handle_column_ids = match table.pk_handle_offset() {
        Some(offset) => vec![columns[offset].read().id],
        None => table
            .common_handle_offsets()
            .iter()
            .map(|offset| columns[*offset].read().id)
            .collect(),
    };
    let kv_handle: tidb_txnkv::Handle = match handle {
        TableHandle::Int(value) => tidb_txnkv::IntHandle::new(*value).into(),
        TableHandle::Common(encoded) => CommonHandle::new(encoded.clone())
            .map_err(|error| decode_error(&error))?
            .into(),
    };
    tidb_tablecodec::decode_handle_to_datum_map(
        Some(&kv_handle),
        &handle_column_ids,
        &column_types,
        Some(zone),
        &mut datums,
    )
    .map_err(|error| decode_error(&error))?;
    let mut row = tidb_rowcodec::RowData::default();
    for column in columns {
        let datum = match datums.remove(&column.read().id) {
            Some(datum) => datum,
            None => column_origin_default_for_checksum(column, context)?,
        };
        row.columns.push(tidb_rowcodec::DatumColumn {
            id: column.read().id,
            field_type: column.read().field_type.clone(),
            datum,
        });
    }
    row.columns.sort_by_key(|column| column.id);
    let checksum = row
        .checksum(Some(zone))
        .map_err(|error| decode_error(&error))?;
    Ok(Datum::new_string_datum(checksum.to_string()))
}

/// Go `table.GetColOriginDefaultValue` with its `getColDefaultValueFromNil`
/// arms, under the statement's own strictness.
fn column_origin_default_for_checksum(
    column: &crate::column::Column,
    context: &RowDecodeContext,
) -> Result<Datum, KvTableError> {
    if column.origin_default.is_some() {
        return column
            .origin_default_value(context.type_flags(), context.zone())
            .map_err(|error| KvTableError::Decode(error.to_string()));
    }
    crate::column_default::get_col_default_value_from_nil(
        &column.read().field_type,
        column.read().name.original(),
        column.read().field_type.flags() & NOT_NULL_FLAG != 0,
        context.expression(),
    )
    .map_err(KvTableError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use tidb_charset::Collation;
    use tidb_datatype::FieldTypeCode;
    use tidb_model::{go_runtime::GoShared, ColumnInfo};

    #[test]
    fn prepared_point_decode_projects_fixed_integer_handle_parts() {
        let mut unsigned = FieldType::new(FieldTypeCode::LongLong);
        unsigned.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
        let columns = [FieldType::new(FieldTypeCode::LongLong), unsigned]
            .into_iter()
            .enumerate()
            .map(|(index, field_type)| {
                Column::new(GoShared::new(ColumnInfo {
                    id: (index + 1) as i64,
                    field_type,
                    ..ColumnInfo::default()
                }))
            })
            .collect::<Vec<_>>();
        let zone = SessionTimeZone::utc();
        let context = PreparedPointGetDecodeContext::for_query(false, zone.clone());
        let empty_row =
            tidb_tablecodec::encode_table_row(Some(&zone), &[], &[], true, None).unwrap();
        let key = [Datum::Int(i64::MIN), Datum::UInt(u64::MAX)];
        let encoded = tidb_codec::encode_key(&key).unwrap();
        for (part, expected) in key.into_iter().enumerate() {
            let decoder = PreparedPointGetRowDecoder::new_with_handles(
                &columns, None, &[0, 1], &[part],
            )
            .unwrap();
            let handle = TableHandle::Common(encoded.clone());
            assert_eq!(
                decoder.decode(&handle, &empty_row, &context).unwrap(),
                [expected]
            );

            let mut invalid = encoded.clone();
            invalid.extend([0xff; 9]);
            assert!(decoder
                .decode(&TableHandle::Common(invalid), &empty_row, &context)
                .is_err());
        }

        let decoder =
            PreparedPointGetRowDecoder::new_with_handles(&columns, None, &[0, 1], &[1]).unwrap();
        let mixed =
            tidb_codec::encode_key(&[Datum::Bytes(vec![0xfe; 8]), Datum::UInt(71)]).unwrap();
        assert_eq!(
            decoder
                .decode(&TableHandle::Common(mixed), &empty_row, &context)
                .unwrap(),
            [Datum::UInt(71)]
        );
        assert_eq!(
            decoder
                .decode(&TableHandle::Common(encoded[..9].to_vec()), &empty_row, &context)
                .unwrap(),
            [Datum::Null]
        );

        let stored = tidb_tablecodec::encode_table_row(
            Some(&zone),
            &[Datum::UInt(10)],
            &[2],
            true,
            None,
        )
        .unwrap();
        assert_eq!(
            decoder
                .decode_owned(&TableHandle::Common(encoded), stored, &context)
                .unwrap(),
            [Datum::UInt(10)]
        );
    }

    #[test]
    fn prepared_point_decode_checks_unprojected_parts_of_string_handle() {
        let columns = [FieldTypeCode::LongLong, FieldTypeCode::Varchar]
            .into_iter()
            .enumerate()
            .map(|(index, code)| {
                Column::new(GoShared::new(ColumnInfo {
                    id: (index + 1) as i64,
                    field_type: FieldType::new(code).with_collation(Collation::Binary),
                    ..ColumnInfo::default()
                }))
            })
            .collect::<Vec<_>>();
        let decoder =
            PreparedPointGetRowDecoder::new_with_handles(&columns, None, &[0, 1], &[1]).unwrap();
        let zone = SessionTimeZone::utc();
        let context = PreparedPointGetDecodeContext::for_query(false, zone.clone());
        let empty_row =
            tidb_tablecodec::encode_table_row(Some(&zone), &[], &[], true, None).unwrap();
        let expected = Datum::Bytes(vec![0xe0; 129]);
        let mut key = tidb_codec::encode_key(&[Datum::Int(1), expected.clone()]).unwrap();
        let indexed = CommonHandle::new(key.clone()).unwrap();
        assert_eq!(
            decoder.decode_indexed_common(&indexed, Cow::Borrowed(&empty_row), &context).unwrap(),
            [expected.clone()]
        );
        assert_eq!(
            decoder
                .decode(&TableHandle::Common(key.clone()), &empty_row, &context)
                .unwrap(),
            [expected]
        );
        key.push(0xff);
        assert!(decoder
            .decode(&TableHandle::Common(key), &empty_row, &context)
            .is_err());
    }

    #[test]
    fn prepared_point_read_uses_padded_indexed_common_handle() {
        use tidb_kv::storage::{MemTableStorage, TableStorage};

        let column = Column::new(GoShared::new(ColumnInfo {
            id: 1,
            field_type: FieldType::new(FieldTypeCode::NewDecimal),
            ..ColumnInfo::default()
        }));
        let decoder = PreparedPointGetRowDecoder::new_with_handles(
            std::slice::from_ref(&column), None, &[0], &[0],
        )
        .unwrap();
        let table = super::super::TableCommon::with_collation(21, vec![column], false);
        let expected = Datum::new_decimal(tidb_datatype::Decimal::from_signed_literal("5"));
        let encoded = tidb_codec::encode_key(std::slice::from_ref(&expected)).unwrap();
        assert!(encoded.len() < 9);
        let handle = CommonHandle::new(encoded.clone()).unwrap();
        let table_handle = TableHandle::Common(encoded);
        let key = tidb_tablecodec::table_key::encode_row_key_with_handle(
            21, &table_handle.record_handle(),
        );
        let zone = SessionTimeZone::utc();
        let context = PreparedPointGetDecodeContext::for_query(false, zone.clone());
        let row = tidb_tablecodec::encode_table_row(Some(&zone), &[], &[], true, None).unwrap();
        let mut store = MemTableStorage::new();
        store.set(tidb_txnkv::Key::from_bytes(key), row).unwrap();
        let (_, value) = table.stored_record(&mut store, &table_handle).unwrap().unwrap();
        assert_eq!(
            decoder.decode_indexed_common(&handle, Cow::Owned(value), &context).unwrap(),
            [expected]
        );
    }

    #[test]
    fn prepared_point_chunk_projection_preserves_defaults_and_decode_errors() {
        let field = FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Binary);
        let string = |bytes: &[u8]| Datum::new_collation_string_datum(bytes.to_vec(), Collation::Binary);
        let mut columns: Vec<_> = (1..=3)
            .map(|id| {
                Column::new(GoShared::new(ColumnInfo {
                    id,
                    field_type: field.clone(),
                    ..ColumnInfo::default()
                }))
            })
            .collect();
        columns[2].origin_default = Some(string(b"origin"));
        let decoder =
            PreparedPointGetRowDecoder::new_with_handles(&columns, None, &[], &[0, 1, 2]).unwrap();
        assert!(decoder.can_append_projected_row());
        let zone = SessionTimeZone::utc();
        let context = PreparedPointGetDecodeContext::for_query(false, zone.clone());
        let handle = TableHandle::Int(1);
        let fields = [field.clone(), field.clone(), field];
        let mut chunk = Chunk::new_chunk_with_capacity(&fields, 1);
        let stored = [string(b"\0\xff"), Datum::Null, string(b"")];
        for (count, v2) in [(3, true), (2, true), (3, false)] {
            let encoded = tidb_tablecodec::encode_table_row(
                Some(&zone), &stored[..count], &[1, 2, 3][..count], v2, None,
            )
            .unwrap();
            chunk.reset();
            decoder.append_storage_value(&handle, Cow::Borrowed(&encoded), &context, &mut chunk).unwrap();
            assert_eq!(chunk.num_rows(), 1);
            let mut expected = stored.clone();
            if count == 2 {
                expected[2] = string(b"origin");
            }
            assert_eq!(chunk.get_row(0).get_datum_row(&fields), expected);
        }
        let malformed = [
            tidb_rowcodec::ROW_CODEC_VERSION,
            tidb_rowcodec::ROW_FLAG_CHECKSUM,
            0, 0, 0, 0, 7,
        ];
        let expected = decoder.decode(&handle, &malformed, &context).unwrap_err();
        let error = decoder
            .append_storage_value(&handle, Cow::Borrowed(&malformed), &context, &mut chunk)
            .unwrap_err();
        assert_eq!(format!("{error:?}"), format!("{expected:?}"));
        assert_eq!(chunk.num_rows(), 1);
    }

    #[test]
    fn prepared_point_chunk_projects_signed_and_unsigned_integers() {
        let zone = SessionTimeZone::utc();
        let context = PreparedPointGetDecodeContext::for_query(false, zone.clone());
        for value in [Datum::Int(-129), Datum::UInt(u64::MAX)] {
            let mut field = FieldType::new(FieldTypeCode::LongLong);
            if matches!(&value, Datum::UInt(_)) {
                field.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
            }
            let column = Column::new(GoShared::new(ColumnInfo {
                id: 1, field_type: field.clone(), ..Default::default()
            }));
            let decoder =
                PreparedPointGetRowDecoder::new_with_handles(&[column], None, &[], &[0]).unwrap();
            assert!(decoder.can_append_projected_row());
            let encoded = tidb_tablecodec::encode_table_row(
                Some(&zone), std::slice::from_ref(&value), &[1], true, None,
            )
            .unwrap();
            let mut chunk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&field), 1);
            decoder
                .append_storage_value(&TableHandle::Int(1), Cow::Borrowed(&encoded), &context, &mut chunk)
                .unwrap();
            assert_eq!(chunk.get_row(0).get_datum(0, &field), value);
        }
    }

    #[test]
    fn prepared_point_decode_shares_bounded_storage_for_medium_strings() {
        let columns = (1..=3)
            .map(|id| {
                Column::new(GoShared::new(ColumnInfo {
                    id,
                    field_type: FieldType::new(FieldTypeCode::Varchar)
                        .with_collation(Collation::Binary),
                    ..ColumnInfo::default()
                }))
            })
            .collect::<Vec<_>>();
        let decoder =
            PreparedPointGetRowDecoder::new_with_handles(&columns, None, &[], &[0, 1, 2])
                .unwrap();
        let zone = SessionTimeZone::utc();
        let context = PreparedPointGetDecodeContext::for_query(false, zone.clone());
        let values = [64, 96, 16].map(|width| {
            Datum::new_collation_string_datum(vec![0xf0; width], Collation::Binary)
        });
        let encoded =
            tidb_tablecodec::encode_table_row(Some(&zone), &values, &[1, 2, 3], true, None)
                .unwrap();
        for oversized in [false, true] {
            let mut entry = encoded.clone();
            if oversized {
                entry.reserve_exact(4096);
            }
            let backing = entry.as_ptr().addr()..entry.as_ptr().addr() + entry.len();
            let row = decoder
                .decode_owned(&TableHandle::Int(1), entry, &context)
                .unwrap();
            assert_eq!(row, values);
            for (index, value) in row.iter().enumerate() {
                let bytes = value.as_raw_bytes().unwrap();
                assert_eq!(
                    backing.contains(&bytes.as_ptr().addr()),
                    !oversized && index < 2
                );
            }
        }
    }

    #[test]
    fn prepared_point_decode_retains_wide_stored_values() {
        let columns = [FieldTypeCode::LongLong, FieldTypeCode::LongBlob]
            .into_iter()
            .enumerate()
            .map(|(i, code)| {
                Column::new(GoShared::new(ColumnInfo {
                    id: (i + 1) as i64,
                    field_type: FieldType::new(code),
                    ..ColumnInfo::default()
                }))
            })
            .collect::<Vec<_>>();
        let decoder = PreparedPointGetRowDecoder::new(&columns, 0, &[1, 0, 1]).unwrap();
        let zone = SessionTimeZone::utc();
        let context = PreparedPointGetDecodeContext::for_query(false, zone.clone());
        let handle = TableHandle::Int(59);
        let datum = Datum::Bytes(vec![0xf0; 8192]);
        for v2 in [false, true] {
            let encoded = tidb_tablecodec::encode_table_row(
                Some(&zone),
                std::slice::from_ref(&datum),
                &[2],
                v2,
                None,
            )
            .unwrap();
            let backing = encoded.as_ptr().addr()..encoded.as_ptr().addr() + encoded.len();
            let expected = decoder.decode(&handle, &encoded, &context).unwrap();
            let decoded = decoder.decode_owned(&handle, encoded, &context).unwrap();
            assert_eq!(decoded, expected);
            assert_eq!(decoded[1], Datum::Int(59));
            for index in [0, 2] {
                let bytes = decoded[index].as_raw_bytes().unwrap();
                assert_eq!(bytes, datum.as_raw_bytes().unwrap());
                assert!(backing.contains(&bytes.as_ptr().addr()));
                assert!(bytes.as_ptr().addr() + bytes.len() <= backing.end);
            }
        }
    }
}

