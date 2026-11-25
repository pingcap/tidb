// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Schema projection for Go master buildAnalyzeSamplingPushdown.
use prost::encoding::{encode_varint, WireType};
use prost::Message;
use tidb_datatype::{CollationCollator, Datum};
use super::{AnalyzeError, AnalyzeOptions, AnalyzePlan};
use tidb_distsql::remote_scan::PushdownScanColumn;
use tidb_model::table_info::TableInfo;
use tidb_proto::{analyze_input, tipb};
use tidb_util::misc::{column_label_size, encode_column_labels};
use crate::dag_request::{begin_embedded_message, delimited_field_size, end_embedded_message};

pub(super) struct EncodedColumnRequest {
    pub(super) data: Vec<u8>,
    sample_rate: f64,
}

/// Encodes an ANALYZE request from its source columns and request builder.
///
/// Writes scalar metadata through a bounded buffer and streams borrowed labels
/// into their length-delimited TiPB fields.
/// The builder sets the supplied columns on `col_req.columns_info`. The
/// remaining request fields do not depend on the supplied vector's length.
#[inline]
pub fn encode_columns_request(
    columns: &[PushdownScanColumn],
    build: impl FnOnce(Vec<tipb::ColumnInfo>) -> tipb::AnalyzeReq,
) -> Result<Vec<u8>, AnalyzeError> {
    const MIN_STREAMED_LABELS: usize = 8;
    // The batch encoder amortizes the larger label sets.
    const MAX_SINGLE_COLUMN_LABELS: usize = 256;

    if let [column] = columns {
        use tidb_datatype::FieldTypeCode::{Enum, Set};
        if column.origin_default.is_none()
            && matches!(column.field_type.code(), Enum | Set)
            && column.field_type.with_elems_visible(|labels| {
                (1..=MAX_SINGLE_COLUMN_LABELS).contains(&labels.len())
            })
        {
            return encode_single_labeled_column(build(Vec::new()), column);
        }
    }

    let mut label_count = 0;
    let stream_labels = columns.iter().any(|column| {
        use tidb_datatype::FieldTypeCode::{Enum, Set};
        if matches!(column.field_type.code(), Enum | Set) {
            label_count += column.field_type.with_elems_visible(<[_]>::len);
        }
        label_count >= MIN_STREAMED_LABELS
    });
    if stream_labels {
        encode_columns_with_shared_labels(build(Vec::new()), columns)
    } else if let Some(default_lengths) = small_column_default_lengths(columns) {
        encode_small_columns(build(Vec::new()), columns, &default_lengths)
    } else if columns.len() >= 32
        || (!columns.is_empty() && columns.iter().all(is_scalar_column))
    {
        encode_columns_in_one_pass(build(Vec::new()), columns)
    } else {
        let mut fields = Vec::with_capacity(columns.len());
        for column in columns {
            let Some(descriptor) = crate::cop_reader::column_to_proto(column) else {
                return Err(invalid_analyze_column(column.id));
            };
            fields.push(crate::dag_request::column_into_pb(descriptor));
        }
        Ok(build(fields).encode_to_vec())
    }
}

#[inline]
fn analyze_column_descriptor(
    column: &PushdownScanColumn,
) -> Result<tidb_planner_core_operator_logicalop::tikv_scan_spec::ScanColumnInfo, AnalyzeError> {
    crate::cop_reader::column_to_proto(column).ok_or_else(|| invalid_analyze_column(column.id))
}

#[cold]
fn invalid_analyze_column(id: i64) -> AnalyzeError {
    AnalyzeError::unsupported(format!("ANALYZE cannot encode column {id}"))
}

fn missing_column_request() -> AnalyzeError {
    AnalyzeError::unsupported("missing ANALYZE column request".to_owned())
}

fn split_column_request(
    request: &mut tipb::AnalyzeReq,
) -> Result<(tipb::AnalyzeColumnsReq, tipb::AnalyzeColumnsReq), AnalyzeError> {
    let mut tail = request.col_req.take().ok_or_else(missing_column_request)?;
    let prefix = tipb::AnalyzeColumnsReq {
        bucket_size: tail.bucket_size.take(),
        sample_size: tail.sample_size.take(),
        sketch_size: tail.sketch_size.take(),
        columns_info: std::mem::take(&mut tail.columns_info),
        ..Default::default()
    };
    Ok((prefix, tail))
}

fn is_scalar_column(column: &PushdownScanColumn) -> bool {
    column.origin_default.is_none() && column.field_type.with_elems_visible(<[_]>::is_empty)
}

#[inline]
fn encode_columns_in_one_pass(
    mut request: tipb::AnalyzeReq,
    columns: &[PushdownScanColumn],
) -> Result<Vec<u8>, AnalyzeError> {
    let mut col_req = request.col_req.take().ok_or_else(missing_column_request)?;
    // Most fixed column descriptors occupy about 32 bytes on the wire.
    // Patch the enclosing length after writing the columns.
    let columns_length = col_req.encoded_len() + 32 * columns.len();
    let mut output = Vec::with_capacity(request.encoded_len() + delimited_field_size(columns_length));
    request.encode_raw(&mut output);
    let body = begin_embedded_message(6, columns_length, &mut output); // AnalyzeReq.col_req

    encode_column_request_prefix(&mut col_req, &mut output);

    let mut scratch = [0; SCALAR_COLUMN_WIRE_CAPACITY];
    for column in columns {
        if is_scalar_column(column) {
            let fields = crate::cop_reader::column_proto_scalars(
                column.id,
                &column.field_type,
                column.is_handle,
            )
            .ok_or_else(|| invalid_analyze_column(column.id))?;
            output.extend_from_slice(scalar_column_wire(fields, &mut scratch));
        } else {
            let fields = crate::dag_request::column_into_pb(analyze_column_descriptor(column)?);
            prost::encoding::message::encode(4, &fields, &mut output); // AnalyzeColumnsReq.columns_info
        }
    }
    col_req.encode_raw(&mut output);
    end_embedded_message(body, &mut output);
    Ok(output)
}

const SMALL_REQUEST_MAX_COLUMNS: usize = 8;
const MAX_INLINE_DEFAULT_BYTES: usize = 64;

fn small_column_default_lengths(
    columns: &[PushdownScanColumn],
) -> Option<[u8; SMALL_REQUEST_MAX_COLUMNS]> {
    if columns.is_empty() || columns.len() > SMALL_REQUEST_MAX_COLUMNS {
        return None;
    }
    let mut lengths = [0; SMALL_REQUEST_MAX_COLUMNS];
    for (column, length) in columns.iter().zip(&mut lengths) {
        if !column.field_type.with_elems_visible(<[_]>::is_empty) {
            return None;
        }
        if let Some(value) = &column.origin_default {
            let size = tidb_codec::estimate_value_size(value).ok()?;
            if size > MAX_INLINE_DEFAULT_BYTES {
                return None;
            }
            *length = size as u8;
        }
    }
    Some(lengths)
}

#[inline(never)]
fn encode_small_columns(
    mut request: tipb::AnalyzeReq,
    columns: &[PushdownScanColumn],
    default_lengths: &[u8; SMALL_REQUEST_MAX_COLUMNS],
) -> Result<Vec<u8>, AnalyzeError> {
    let mut col_req = request.col_req.take().ok_or_else(missing_column_request)?;
    // The common short request fits within this capacity and uses an
    // envelope length with the same varint width as the estimate.
    let columns_length = 48 + 64 * columns.len();
    let mut output = Vec::with_capacity(16 + delimited_field_size(columns_length));
    request.encode_raw(&mut output);
    let body = begin_embedded_message(6, columns_length, &mut output);
    encode_column_request_prefix(&mut col_req, &mut output);

    let mut scratch = [0; SCALAR_COLUMN_WIRE_CAPACITY];
    for (column, &default_length) in columns.iter().zip(default_lengths) {
        let fields = crate::cop_reader::column_proto_scalars(
            column.id,
            &column.field_type,
            column.is_handle,
        )
        .ok_or_else(|| invalid_analyze_column(column.id))?;
        let scalar = scalar_column_wire(fields, &mut scratch);
        if let Some(default) = &column.origin_default {
            encode_small_column_default(column.id, scalar, default, default_length, &mut output)?;
        } else {
            output.extend_from_slice(scalar);
        }
    }
    col_req.encode_raw(&mut output);
    end_embedded_message(body, &mut output);
    Ok(output)
}

fn encode_small_column_default(
    column_id: i64,
    scalar: &[u8],
    default: &Datum,
    default_length: u8,
    output: &mut Vec<u8>,
) -> Result<(), AnalyzeError> {
    // Exclude the scalar envelope and include the compact default's two-byte header.
    let scalar_body_length = scalar.len() - 2;
    let length = scalar_body_length + 2 + usize::from(default_length);
    let column_body = begin_embedded_message(4, length, output);
    let (head, tail) = scalar[2..].split_at(scalar_body_length - SCALAR_COLUMN_BOOL_FIELDS_SIZE);
    output.extend_from_slice(head);
    output.extend_from_slice(&[0x42, default_length]); // ColumnInfo.default_val
    tidb_codec::append_value_in_timezone(&chrono::Utc, output, default)
        .map_err(|_| invalid_analyze_column(column_id))?;
    output.extend_from_slice(tail);
    debug_assert_eq!(output.len() - column_body.1, length);
    Ok(())
}

fn encode_column_request_prefix(col_req: &mut tipb::AnalyzeColumnsReq, output: &mut Vec<u8>) {
    // Emit the fields that precede AnalyzeColumnsReq.columns_info first,
    // including any descriptors already supplied by the request builder.
    for (tag, field) in [
        (1, col_req.bucket_size.take()),
        (2, col_req.sample_size.take()),
        (3, col_req.sketch_size.take()),
    ] {
        if let Some(value) = field {
            prost::encoding::int64::encode(tag, &value, output);
        }
    }
    for field in std::mem::take(&mut col_req.columns_info) {
        prost::encoding::message::encode(4, &field, output);
    }
}

#[inline(never)]
fn encode_single_labeled_column(
    mut request: tipb::AnalyzeReq,
    column: &PushdownScanColumn,
) -> Result<Vec<u8>, AnalyzeError> {
    let mut col_req = request.col_req.take().ok_or_else(missing_column_request)?;
    let fields =
        crate::cop_reader::column_proto_scalars(column.id, &column.field_type, column.is_handle)
            .ok_or_else(|| invalid_analyze_column(column.id))?;
    let mut scratch = [0; SCALAR_COLUMN_WIRE_CAPACITY];
    let scalar = scalar_column_wire(fields, &mut scratch);
    let (head, tail) = scalar[2..].split_at(scalar.len() - 2 - SCALAR_COLUMN_BOOL_FIELDS_SIZE);

    // Hold one read of the shared label slice through sizing and writing.
    Ok(column.field_type.with_elems_visible(|labels| {
        let (label_length, short_labels_length) = column_label_size(labels);
        let length = head.len() + tail.len() + label_length;
        let request_length = col_req.encoded_len() + delimited_field_size(length);
        let mut output =
            Vec::with_capacity(request.encoded_len() + delimited_field_size(request_length));
        request.encode_raw(&mut output);
        let body = begin_embedded_message(6, request_length, &mut output); // AnalyzeReq.col_req
        encode_column_request_prefix(&mut col_req, &mut output);
        let column_body = begin_embedded_message(4, length, &mut output); // AnalyzeColumnsReq.columns_info
        output.extend_from_slice(head);
        encode_column_labels(labels, short_labels_length, &mut output);
        output.extend_from_slice(tail);
        end_embedded_message(column_body, &mut output);
        col_req.encode_raw(&mut output);
        end_embedded_message(body, &mut output);
        output
    }))
}

// ColumnInfo has six signed varint fields and two bool fields. Its scalar
// form always fits behind a one-byte message length.
const SCALAR_COLUMN_BOOL_FIELDS_SIZE: usize = 2 * 3;
const SCALAR_COLUMN_WIRE_CAPACITY: usize = 2 + 6 * (1 + 10) + SCALAR_COLUMN_BOOL_FIELDS_SIZE;

#[inline]
fn scalar_column_wire(
    fields: crate::cop_reader::ColumnProtoScalars,
    scratch: &mut [u8; SCALAR_COLUMN_WIRE_CAPACITY],
) -> &[u8] {
    let mut body = &mut scratch[2..];
    body = write_column_varint(body, 8, fields.column_id as u64);
    body = write_column_varint(body, 16, fields.tp as u64);
    body = write_column_varint(body, 24, fields.collation as u64);
    body = write_column_varint(body, 32, fields.column_len as u64);
    body = write_column_varint(body, 40, fields.decimal as u64);
    body = write_column_varint(body, 48, fields.flag as u64);
    body[..SCALAR_COLUMN_BOOL_FIELDS_SIZE].copy_from_slice(&[
        0xa8, 1, u8::from(fields.pk_handle), // ColumnInfo.pk_handle
        0xb0, 1, u8::from(fields.array), // ColumnInfo.array
    ]);
    let length = SCALAR_COLUMN_WIRE_CAPACITY - 2 - body.len() + SCALAR_COLUMN_BOOL_FIELDS_SIZE;
    scratch[0] = (4 << 3) | WireType::LengthDelimited as u8; // AnalyzeColumnsReq.columns_info
    scratch[1] = length as u8;
    &scratch[..2 + length]
}

#[inline]
fn write_column_varint(output: &mut [u8], key: u8, value: u64) -> &mut [u8] {
    if value < 128 {
        output[..2].copy_from_slice(&[key, value as u8]);
        &mut output[2..]
    } else if value < 16_384 {
        output[..3].copy_from_slice(&[key, value as u8 | 0x80, (value >> 7) as u8]);
        &mut output[3..]
    } else if value >= u64::MAX - 127 {
        // Negative values in -128..=-1 have eight sign-extension groups.
        output[..11].copy_from_slice(&[
            key, value as u8, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 1,
        ]);
        &mut output[11..]
    } else {
        output[0] = key;
        let mut rest = &mut output[1..];
        encode_varint(value, &mut rest);
        rest
    }
}

#[inline(never)]
fn encode_columns_with_shared_labels(
    mut request: tipb::AnalyzeReq,
    columns: &[PushdownScanColumn],
) -> Result<Vec<u8>, AnalyzeError> {
    let (prefix, tail) = split_column_request(&mut request)?;
    let columns = columns
        .iter()
        .map(|column| {
            let mut descriptor = analyze_column_descriptor(column)?;
            let labels = std::mem::take(&mut descriptor.elems);
            let fields = crate::dag_request::column_pb_fields(descriptor, Vec::new());
            let (label_length, short_labels_length) = labels.with_visible(column_label_size);
            let length = fields.encoded_len() + label_length;
            Ok((fields, labels, length, short_labels_length))
        })
        .collect::<Result<smallvec::SmallVec<[_; 1]>, AnalyzeError>>()?;

    let columns_length = columns.iter().fold(
        prefix.encoded_len() + tail.encoded_len(),
        |length, (_, _, column, _)| length + delimited_field_size(*column),
    );
    let mut output = Vec::with_capacity(request.encoded_len() + delimited_field_size(columns_length));
    request.encode_raw(&mut output);
    let request_length = begin_embedded_message(6, columns_length, &mut output); // AnalyzeReq.col_req
    prefix.encode_raw(&mut output);
    for (mut fields, labels, length, short_labels_length) in columns {
        let column_length = begin_embedded_message(4, length, &mut output); // AnalyzeColumnsReq.columns_info
        labels.with_visible(|labels| {
            if labels.is_empty() {
                fields.encode_raw(&mut output);
                return;
            }
            let trailer = tipb::ColumnInfo {
                default_val: fields.default_val.take(),
                pk_handle: fields.pk_handle.take(),
                array: fields.array.take(),
                ..Default::default()
            };
            fields.encode_raw(&mut output);
            encode_column_labels(labels, short_labels_length, &mut output);
            trailer.encode_raw(&mut output);
        });
        end_embedded_message(column_length, &mut output);
    }
    tail.encode_raw(&mut output);
    end_embedded_message(request_length, &mut output);
    Ok(output)
}

pub(super) struct SamplingSchema {
    pub columns: Vec<PushdownScanColumn>,
    pub handle_positions: Vec<usize>,
    pub common_handle: bool,
    /// The handle ranges the sampling request reads; see
    /// [`analyze_handle_ranges`].
    pub ranges: tidb_ranger::types::Ranges,
    primary_ids: Vec<i64>,
    primary_prefix_ids: Vec<i64>,
    primary_lengths: Vec<i64>,
}

impl SamplingSchema {
    pub fn new(table: &TableInfo, plan: &AnalyzePlan) -> Result<Self, AnalyzeError> {
        let integer_pk = table
            .pk_is_handle
            .then(|| table.get_pk_col_info())
            .flatten()
            .map(|column| column.read().id);
        let mut columns: Vec<_> = plan
            .columns()
            .iter()
            .map(|column| PushdownScanColumn {
                id: column.id,
                field_type: column.field_type.clone(),
                is_handle: integer_pk == Some(column.id),
                origin_default: Some(column.absent_value.clone()),
            })
            .collect();
        let mut primary_ids = Vec::new();
        let mut primary_prefix_ids = Vec::new();
        let mut primary_lengths = Vec::new();
        if table.is_common_handle {
            let primary = table.get_primary_key().ok_or_else(|| {
                AnalyzeError::unsupported("common handle has no primary index".to_owned())
            })?;
            for part in primary.read().columns.iter_deref() {
                let part = part.read();
                let source = table.cols().get(part.offset as usize).ok_or_else(|| {
                    AnalyzeError::unsupported("primary index offset is invalid".to_owned())
                })?;
                let source = source.read();
                primary_ids.push(source.id);
                primary_lengths.push(part.length);
                if part.length != tidb_datatype::UNSPECIFIED_LENGTH {
                    primary_prefix_ids.push(source.id);
                }
            }
        } else if let Some(id) = integer_pk {
            primary_ids.push(id);
        } else {
            columns.push(PushdownScanColumn {
                id: -1,
                field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                is_handle: true,
                origin_default: None,
            });
            primary_ids.push(-1);
        }
        let mut handle_positions = Vec::new();
        for id in &primary_ids {
            if !columns.iter().any(|column| column.id == *id) {
                let column = table
                    .cols()
                    .iter_deref()
                    .find(|column| column.read().id == *id)
                    .ok_or_else(|| {
                        AnalyzeError::unsupported("handle column is missing".to_owned())
                    })?;
                let column = column.read();
                columns.push(PushdownScanColumn {
                    id: *id,
                    field_type: column.field_type.clone(),
                    is_handle: integer_pk == Some(*id),
                    origin_default: Some(
                        super::origin_default(&column, table.name.original())
                            .map_err(|e| AnalyzeError::unsupported(e.to_string()))?,
                    ),
                });
            }
            handle_positions.push(columns.iter().position(|column| column.id == *id).unwrap());
        }
        Ok(Self {
            columns,
            handle_positions,
            common_handle: table.is_common_handle,
            ranges: analyze_handle_ranges(table),
            primary_ids,
            primary_prefix_ids,
            primary_lengths,
        })
    }

    pub fn statistics_slots(&self, plan: &AnalyzePlan) -> Vec<usize> {
        (0..plan.columns().len())
            .chain(
                plan.indexes()
                    .iter()
                    .enumerate()
                    .filter(|(_, index)| index.needs_own_slot())
                    .map(|(i, _)| self.columns.len() + i),
            )
            .collect()
    }

    pub fn decode_handle(
        &self,
        row: &mut Vec<tidb_datatype::Datum>,
        timezone: &tidb_datatype::SessionTimeZone,
    ) -> Result<tidb_txnkv::Handle, crate::analyze::AnalyzeError> {
        use tidb_datatype::Datum;
        use crate::analyze::AnalyzeError as Error;
        if row.len() != self.columns.len() {
            return Err(Error::Unsupported(
                "ANALYZE sample column count mismatch".to_owned(),
            ));
        }
        for (datum, column) in row.iter_mut().zip(&self.columns) {
            if !matches!(datum, Datum::Bytes(_)) {
                return Err(Error::Unsupported(
                    "ANALYZE sample is not encoded".to_owned(),
                ));
            }
            tidb_tablecodec::decode_column_value_in_place(
                datum,
                &column.field_type,
                Some(timezone),
            )
            .map_err(|e| Error::Encode(e.to_string()))?;
        }
        if !self.common_handle {
            return match row[self.handle_positions[0]] {
                Datum::Int(value) => Ok(tidb_txnkv::IntHandle::new(value).into()),
                Datum::UInt(value) => Ok(tidb_txnkv::IntHandle::new(value as i64).into()),
                _ => Err(Error::Unsupported(
                    "ANALYZE integer handle is not an integer".to_owned(),
                )),
            };
        }
        let mut values = Vec::with_capacity(self.handle_positions.len());
        for (key_position, position) in self.handle_positions.iter().copied().enumerate() {
            let column = &self.columns[position];
            let mut value = row[position].clone();
            tidb_tablecodec::truncate_index_value(
                &mut value,
                &tidb_tablecodec::IndexColumn {
                    offset: position,
                    length: self.primary_lengths[key_position],
                    use_changing_type: false,
                },
                &tidb_tablecodec::TableColumn {
                    id: column.id,
                    offset: position,
                    field_type: column.field_type.clone(),
                    primary_key: true,
                    changing_field_type: None,
                },
            )
            .map_err(|e| Error::Encode(e.to_string()))?;
            if let Some(collation) = crate::analyze::AnalyzedColumn::sampling_collation(
                &column.field_type,
                "primary key",
            )? {
                value = match value {
                    Datum::Bytes(bytes) => Datum::Bytes(collation.key(&bytes)),
                    Datum::String(string) => Datum::Bytes(collation.key(string.bytes())),
                    other => other,
                };
            }
            values.push(value);
        }
        let bytes = tidb_codec::encode_key(&values).map_err(|e| Error::Encode(e.to_string()))?;
        tidb_txnkv::CommonHandle::new(bytes)
            .map(Into::into)
            .map_err(|e| Error::Encode(e.to_string()))
    }

    pub fn project_collector(
        &self,
        plan: &AnalyzePlan,
        collector: analyze_input::RowSampleCollector,
    ) -> Result<
        tidb_stats::row_sample_collector::RowSampleCollectorProto<prost::bytes::Bytes>,
        AnalyzeError,
    > {
        let expected = self.columns.len() + plan.indexes().len();
        if collector.null_counts.len() != expected
            || collector.total_size.len() != expected
            || collector.fm_sketch.len() != expected
            || collector
                .samples
                .iter()
                .any(|sample| sample.row.len() != self.columns.len())
            || collector.count.unwrap_or_default() < 0
        {
            return Err(AnalyzeError::unsupported(
                "TiKV ANALYZE collector does not match requested schema".to_owned(),
            ));
        }
        let slots = self.statistics_slots(plan);
        Ok(tidb_stats::row_sample_collector::RowSampleCollectorProto {
            count: collector.count.unwrap_or_default(),
            null_counts: slots.iter().map(|i| collector.null_counts[*i]).collect(),
            total_sizes: slots.iter().map(|i| collector.total_size[*i]).collect(),
            fm_sketches: slots
                .iter()
                .map(|i| {
                    Some(tidb_stats::FmSketchProto {
                        mask: collector.fm_sketch[*i].mask.unwrap_or_default(),
                        hashset: collector.fm_sketch[*i].hashset.clone(),
                    })
                })
                .collect(),
            samples: collector
                .samples
                .into_iter()
                .map(|sample| tidb_stats::row_sample_collector::RowSampleProto {
                    row: sample.row,
                    weight: sample.weight.unwrap_or_default(),
                })
                .collect(),
        })
    }

    pub fn request(
        &self,
        plan: &AnalyzePlan,
        options: &AnalyzeOptions,
        rate: f64,
        flags: u64,
        offset: i64,
    ) -> Result<EncodedColumnRequest, AnalyzeError> {
        let sample_rate = if options.num_samples > 0 { 0.0 } else { rate };
        let data = encode_columns_request(&self.columns, |columns_info| tipb::AnalyzeReq {
            tp: Some(tipb::AnalyzeType::TypeFullSampling as i32),
            flags: Some(flags),
            time_zone_offset: Some(offset),
            col_req: Some(tipb::AnalyzeColumnsReq {
                bucket_size: Some(options.num_buckets as i64),
                sample_size: Some(options.num_samples as i64),
                sketch_size: Some(tidb_stats::MAX_SKETCH_SIZE as i64),
                sample_rate: Some(sample_rate),
                columns_info,
                primary_column_ids: if self.common_handle {
                    self.primary_ids.clone()
                } else {
                    Vec::new()
                },
                primary_prefix_column_ids: self.primary_prefix_ids.clone(),
                column_groups: plan
                    .indexes()
                    .iter()
                    .map(|index| tipb::AnalyzeColumnGroup {
                        column_offsets: index
                            .column_positions
                            .iter()
                            .map(|position| *position as i64)
                            .collect(),
                        prefix_lengths: Vec::new(),
                    })
                    .collect(),
                ..Default::default()
            }),
            ..Default::default()
        })?;
        Ok(EncodedColumnRequest {
            data,
            sample_rate,
        })
    }
}

/// Go `analyzeColumnsPushDown`'s full handle ranges, as `open` splits them
/// with `SplitRangesAcrossInt64Boundary(ranges, false, false,
/// !hasPkHist(e.handleCols))`: `FullIntRange` over an integer primary key --
/// one merged list, the unsigned half first -- `FullNotNullRange` over a
/// common handle, and the signed `FullIntRange` when `BuildHandleColsForAnalyze`
/// gives no handle columns (`_tidb_rowid`).
///
/// These are not the record prefix and its successor: TiKV's table scan reads
/// a range whose end is its start's `PrefixNext` as a point get of the start,
/// and no row key is the bare `t{id}_r`, so that range samples no rows.
fn analyze_handle_ranges(table: &TableInfo) -> tidb_ranger::types::Ranges {
    use tidb_ranger::points::{full_int_range, full_not_null_range};
    let unsigned_pk = table
        .pk_is_handle
        .then(|| table.get_pk_col_info())
        .flatten()
        .map(|column| column.read().field_type.is_unsigned());
    let ranges = match unsigned_pk {
        Some(unsigned) => full_int_range(unsigned),
        None if table.is_common_handle => full_not_null_range(),
        None => full_int_range(false),
    };
    if unsigned_pk.is_some() {
        crate::split_ranges_across_int64_boundary(ranges, false, false, false).0
    } else {
        ranges
    }
}

fn concurrency_for_stores(stores: u64) -> u64 {
    match stores {
        0..=5 => tidb_vardef::defaults::DEF_ANALYZE_DIST_SQL_SCAN_CONCURRENCY as u64,
        6..=10 => stores,
        11..=20 => stores.saturating_mul(2),
        21..=50 => stores.saturating_mul(3),
        _ => stores.saturating_mul(4),
    }
}

/// Go `adaptiveAnlayzeDistSQLConcurrency`: the store count comes from
/// `helper.TryGetPDHTTPClient` (the store's PD HTTP client, caller ID
/// `tidb-store-helper`) and `GetStores`.
pub(crate) fn adaptive_anlayze_dist_sql_concurrency(
    configured: i64,
    pd_http_client: Option<tikv_client::pd_http::Client>,
    timeout: std::time::Duration,
) -> u64 {
    if configured > 0 {
        return configured as u64;
    }
    let Some(pd_cli) = pd_http_client else {
        tidb_logutil::bg_logger().warn(
            "fail to TryGetPDHTTPClient",
            &[tidb_log::Field::new(
                "error",
                tidb_log::Value::Str("pd http client unavailable".to_owned()),
            )],
        );
        return concurrency_for_stores(0);
    };
    let pd_cli = pd_cli.with_caller_id("tidb-store-helper");
    let count = (|| -> Result<u64, String> {
        let stores_stat = tidb_store_driver_txn::cluster_http::block_on_pd_http(async {
            tokio::time::timeout(timeout, pd_cli.get_stores())
                .await
                .unwrap_or_else(|_| {
                    Err(tikv_client::Error::StringError(
                        "PD stores request timed out".to_owned(),
                    ))
                })
        })?;
        u64::try_from(stores_stat.count).map_err(|error| error.to_string())
    })();
    match count {
        Ok(count) => concurrency_for_stores(count),
        Err(error) => {
            tidb_logutil::bg_logger().warn(
                "fail to get stores info",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(error))],
            );
            concurrency_for_stores(0)
        }
    }
}

pub(crate) struct SamplingCancellation {
    done: crossbeam_channel::Sender<()>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl SamplingCancellation {
    pub(crate) fn start(
        killer: &tidb_sqlkiller::SqlKiller,
        cancellation: std::sync::Arc<tidb_txnkv::CancelHandle>,
    ) -> Result<Self, AnalyzeError> {
        let killed = killer.get_kill_event_chan();
        let (done, finished) = crossbeam_channel::bounded(1);
        let worker = std::thread::Builder::new()
            .name("analyze-cancel".to_owned())
            .spawn(tidb_cpuprofile::labels::inherit(move || {
                crossbeam_channel::select! {
                    recv(killed) -> _ => cancellation.cancel(),
                    recv(finished) -> _ => {},
                }
            }))
            .map_err(|e| AnalyzeError::unsupported(e.to_string()))?;
        Ok(Self {
            done,
            worker: Some(worker),
        })
    }
}

impl Drop for SamplingCancellation {
    fn drop(&mut self) {
        let _ = self.done.try_send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

pub(crate) fn analyze_table<S>(
    opener: &tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener<S>,
    table: &TableInfo,
    physical_id: i64,
    statement: &super::AnalyzeStatement,
    realtime_count: Option<i64>,
    snapshot: u64,
    selected_columns: Option<&std::collections::HashSet<i64>>,
    concurrency: u64,
    timeout: std::time::Duration,
    killer: &tidb_sqlkiller::SqlKiller,
    resource_group: &str,
    progress: &(dyn Fn(i64) + Sync),
    ndv_jobs: Option<(&dyn crate::real_tikv_analyze::AnalyzeJobLifecycle, &crate::real_tikv_analyze::AnalyzeJobSpec)>,
) -> Result<super::AnalyzeReport, AnalyzeError>
where
    S: tidb_store_driver_txn::transaction::StoreClient,
{
    let cancellation = std::sync::Arc::new(tidb_txnkv::CancelHandle::default());
    let _cancel_guard = SamplingCancellation::start(killer, cancellation.clone())?;
    let plan = super::cluster_analyze_plan(table, selected_columns, statement.enable_ddl_analyze_exec_opt)?
        .with_time_zone(statement.eval_context.session_zone());
    let virtuals =
        super::virtual_samples::VirtualSamples::new(table, &plan, &statement.eval_context)?;
    let schema = SamplingSchema::new(table, &plan)?;
    let options = &statement.options;
    let rate = options.sample_rate.unwrap_or_else(|| {
        tidb_stats::row_sample_collector::get_adjusted_sample_rate(realtime_count, None)
    });
    let request = schema.request(
        &plan,
        options,
        rate,
        statement.eval_context.push_down_flags(),
        statement.eval_context.session_zone().dag_zone().1,
    )?;
    // Go `AnalyzeColumnsExec.open`: `memTracker = memory.NewTracker(PlanID, -1)`
    // attached to the statement's tracker, which ANALYZE attached to
    // `GlobalAnalyzeMemoryTracker`; its bytes are released when the build ends.
    let mem_tracker = tidb_memory::Tracker::new(tidb_memory::LABEL_FOR_ANALYZE_MEMORY, -1);
    mem_tracker.attach_to(statement.eval_context.statement_memory().stmt_tracker());
    let _release = TrackerRelease(&mem_tracker);
    // Go `analyzeColumnsPushDown`: the special indexes (with a prefix or
    // virtual column) take their NDV from TiKV's index keys
    // (`handleNDVForSpecialIndexes`), beside the row sampling.
    let special_indexes = plan
        .indexes()
        .iter()
        .enumerate()
        .filter(|(_, index)| index_requires_ndv_scan(table, &plan, index))
        .collect::<Vec<_>>();
    let (collected, special) = std::thread::scope(|scope| {
        let special = scope.spawn(tidb_cpuprofile::labels::inherit(|| {
            handle_ndv_for_special_indexes(
                opener,
                table,
                physical_id,
                &special_indexes,
                statement,
                snapshot,
                concurrency,
                timeout,
                cancellation.clone(),
                resource_group,
                ndv_jobs,
            )
        }));
        let collected = collect_regions(
            opener,
            &schema,
            &plan,
            request,
            physical_id,
            snapshot,
            statement.analyze_snapshot,
            options,
            concurrency,
            statement.store_batch_size,
            timeout,
            cancellation.clone(),
            resource_group,
            progress,
            &mem_tracker,
            statement.sampling_stats_concurrency,
            killer,
        );
        let special = special
            .join()
            .unwrap_or_else(|_| Err(AnalyzeError::unsupported("analyze worker panic".to_owned())));
        (collected, special)
    });
    let mut collector = collected?;
    for (position, null_count, sketch) in special? {
        collector.replace_special_index_stats(plan.index_slot(position), null_count, sketch);
    }
    finish_samples(
        &virtuals,
        physical_id,
        statement,
        realtime_count,
        snapshot,
        &plan,
        &schema,
        collector,
    )
}

/// Go `handleNDVForSpecialIndexes`: `subIndexWorkerForNDV`s, at most
/// `samplingStatsConcurrency` of them, read each special index's NULL
/// count and FM sketch from its keys.
#[allow(clippy::too_many_arguments)]
fn handle_ndv_for_special_indexes<S>(
    opener: &tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener<S>,
    table: &TableInfo,
    physical_id: i64,
    special_indexes: &[(usize, &crate::analyze::AnalyzedIndex)],
    statement: &super::AnalyzeStatement,
    snapshot: u64,
    concurrency: u64,
    timeout: std::time::Duration,
    cancellation: std::sync::Arc<tidb_txnkv::CancelHandle>,
    resource_group: &str,
    ndv_jobs: Option<(&dyn crate::real_tikv_analyze::AnalyzeJobLifecycle, &crate::real_tikv_analyze::AnalyzeJobSpec)>,
) -> Result<Vec<(usize, i64, tidb_stats::FmSketch)>, AnalyzeError>
where
    S: tidb_store_driver_txn::transaction::StoreClient,
{
    if special_indexes.is_empty() {
        return Ok(Vec::new());
    }
    // Go `buildSubIndexJobForSpecialIndex`: one `analyze ndv for index` job
    // per special index, added before the workers start
    // (`AddNewAnalyzeJob`).
    let jobs = special_indexes
        .iter()
        .map(|(_, index)| {
            ndv_jobs.map(|(lifecycle, base)| {
                let name = table
                    .indices
                    .iter_deref()
                    .find_map(|info| {
                        let info = info.read();
                        (info.id == index.id).then(|| info.name.original().to_owned())
                    })
                    .unwrap_or_default();
                let auto_analyze = if statement.auto_analyze { "auto " } else { "" };
                crate::real_tikv_analyze::AnalyzeJobHandle::pending(
                    lifecycle,
                    &crate::real_tikv_analyze::AnalyzeJobSpec {
                        schema: base.schema.clone(),
                        table: base.table.clone(),
                        partition: base.partition.clone(),
                        job_info: format!("{auto_analyze}analyze ndv for index {name}"),
                    },
                )
            })
        })
        .collect::<Vec<_>>();
    let finished = (0..special_indexes.len()).map(|_| std::sync::atomic::AtomicBool::new(false)).collect::<Vec<_>>();
    let finish = |task: usize, failure: Option<&str>| {
        if let (Some(job), Some((lifecycle, _))) = (&jobs[task], ndv_jobs) {
            if !finished[task].swap(true, std::sync::atomic::Ordering::SeqCst) {
                job.finish(lifecycle, failure, crate::real_tikv_analyze::AnalyzeJobKind::Table);
            }
        }
    };
    let workers = statement.sampling_stats_concurrency.clamp(1, special_indexes.len());
    let (task_tx, task_rx) = crossbeam_channel::bounded::<usize>(special_indexes.len());
    for task in 0..special_indexes.len() {
        let _ = task_tx.send(task);
    }
    drop(task_tx);
    let results = std::sync::Mutex::new(Vec::with_capacity(special_indexes.len()));
    let failure = std::sync::Mutex::new(None);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let task_rx = task_rx.clone();
            let (results, failure, cancellation, jobs, finish) = (&results, &failure, cancellation.clone(), &jobs, &finish);
            scope.spawn(tidb_cpuprofile::labels::inherit(move || {
                // Go `subIndexWorkerForNDV`.
                for task in task_rx.iter() {
                    if let (Some(job), Some((lifecycle, _))) = (&jobs[task], ndv_jobs) {
                        job.start(lifecycle);
                    }
                    let (position, index) = special_indexes[task];
                    let primary = table.is_common_handle
                        && table.get_primary_key().is_some_and(|primary| primary.read().id == index.id);
                    // Go `buildSubIndexJobForSpecialIndex`.
                    let exec = crate::analyze_idx::AnalyzeIndexExec {
                        opener,
                        physical_id,
                        index_id: index.id,
                        num_columns: index.column_positions.len(),
                        is_common_handle_primary: primary,
                        analyze_pb: crate::analyze_idx::analyze_index_ndv_req(
                            index.column_positions.len(),
                            primary,
                            statement.eval_context.push_down_flags(),
                            statement.eval_context.session_zone().dag_zone().1,
                        ),
                        num_buckets: 1,
                        num_topn: 0,
                        concurrency,
                        snapshot,
                        analyze_snapshot: statement.analyze_snapshot,
                        timeout,
                        cancellation: cancellation.clone(),
                        resource_group,
                    };
                    match exec.analyze_index_ndv_push_down() {
                        Ok((null_count, sketch)) => {
                            finish(task, None);
                            results
                                .lock()
                                .expect("special index NDV results")
                                .push((position, null_count, sketch));
                        }
                        Err(error) => {
                            finish(task, Some(&error.to_string()));
                            failure.lock().expect("special index NDV failure").get_or_insert(error);
                            return;
                        }
                    }
                }
            }));
        }
    });
    if let Some(error) = failure.into_inner().expect("special index NDV failure") {
        // Go finishes the jobs no worker reached with the error.
        let failure = error.to_string();
        for task in 0..special_indexes.len() {
            finish(task, Some(&failure));
        }
        return Err(error);
    }
    Ok(results.into_inner().expect("special index NDV results"))
}

pub(super) fn finish_samples(
    virtuals: &super::virtual_samples::VirtualSamples,
    physical_id: i64,
    statement: &super::AnalyzeStatement,
    realtime_count: Option<i64>,
    snapshot: u64,
    plan: &AnalyzePlan,
    schema: &SamplingSchema,
    collector: tidb_stats::row_sample_collector::RowSampleCollector,
) -> Result<super::AnalyzeReport, AnalyzeError> {
    let run = crate::analyze::AnalyzeRun::start(plan, &statement.options, realtime_count)?;
    let timezone = statement.eval_context.session_zone();
    let analyzed = run.finish_collector(collector, statement.sampling_stats_concurrency, |row| {
        let handle = schema.decode_handle(row, &timezone)?;
        virtuals.materialize(row, &statement.eval_context)?;
        Ok(handle)
    })?;
    Ok(super::report_from_analyzed(analyzed, physical_id, snapshot))
}

/// Go analyzes NDV from stored index keys whenever row sampling cannot
/// reproduce the encoded key's distribution in TiKV.
pub(super) fn index_requires_ndv_scan(
    table: &TableInfo,
    plan: &AnalyzePlan,
    index: &crate::analyze::AnalyzedIndex,
) -> bool {
    index
        .prefix_lengths
        .iter()
        .any(|length| *length != tidb_datatype::UNSPECIFIED_LENGTH)
        || index.column_positions.iter().any(|position| {
            let id = plan.columns()[*position].id;
            table.cols().iter_deref().any(|column| {
                let column = column.read();
                column.id == id && column.is_virtual_generated()
            })
        })
}

pub(crate) fn read_policy(snapshot_enabled: bool, snapshot: u64) -> (u64, tidb_txnkv::IsolationLevel) {
    if snapshot_enabled {
        (snapshot, tidb_txnkv::IsolationLevel::Snapshot)
    } else {
        (u64::MAX, tidb_txnkv::IsolationLevel::ReadCommitted)
    }
}

/// Go `AnalyzeColumnsExec.buildResp`: store batching at the session's
/// `AnalyzeStoreBatchSize`, with merged responses accepted and the batch
/// tasks executed serially whenever batching is on.
fn set_analyze_store_batch(
    builder: &mut tidb_distsql::RequestBuilder,
    store_batch_size: i64,
) -> &mut tidb_distsql::RequestBuilder {
    let enable_store_batch = store_batch_size > 0;
    builder
        .set_store_batch_size(isize::try_from(store_batch_size).unwrap_or(isize::MAX))
        .set_allow_batch_task_data_merge(enable_store_batch)
        .set_execute_batch_tasks_serially(enable_store_batch)
}

/// Uses the process-owned region cache and lock recovery; only sampled
/// collector responses cross the TiKV boundary.
pub(super) fn collect_regions<S>(
    opener: &tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener<S>,
    schema: &SamplingSchema,
    plan: &AnalyzePlan,
    request: EncodedColumnRequest,
    physical_id: i64,
    snapshot: u64,
    snapshot_enabled: bool,
    options: &AnalyzeOptions,
    concurrency: u64,
    store_batch_size: i64,
    timeout: std::time::Duration,
    cancellation: std::sync::Arc<tidb_txnkv::CancelHandle>,
    resource_group: &str,
    progress: &(dyn Fn(i64) + Sync),
    mem_tracker: &std::sync::Arc<tidb_memory::Tracker>,
    sampling_stats_concurrency: usize,
    killer: &tidb_sqlkiller::SqlKiller,
) -> Result<tidb_stats::row_sample_collector::RowSampleCollector, AnalyzeError>
where
    S: tidb_store_driver_txn::transaction::StoreClient,
{
    use prost::Message;
    use tidb_stats::row_sample_collector::SamplePolicy;
    let error = |e: String| AnalyzeError::unsupported(e);
    let policy = SamplePolicy::new_row_sample_collector(options.num_samples, request.sample_rate)
        .ok_or_else(|| error("invalid ANALYZE sample policy".to_owned()))?;
    let transport = native_transport(opener, timeout).map_err(error)?;
    let (read_ts, isolation) = read_policy(snapshot_enabled, snapshot);
    let mut builder = tidb_distsql::RequestBuilder::new();
    builder
        // Go passes the session's `DistSQLContext`; ANALYZE's full handle
        // ranges hold no temporal bound, so its location cannot change them.
        .set_handle_ranges(None, physical_id, schema.common_handle, &schema.ranges)
        .set_analyze_request(request.data, isolation)
        .set_paging(false)
        .set_start_ts(read_ts)
        .set_concurrency(concurrency);
    set_analyze_store_batch(&mut builder, store_batch_size).set_resource_group_name(resource_group);
    let request = builder
        .build_transport_request(cancellation)
        .map_err(|e| error(format!("{e:?}")))?;
    let mut runtime = tidb_distsql::InjectedQueryRuntime::new(transport);
    let mut result = runtime
        .analyze(&request, true)
        .map_err(|e| error(e.to_string()))?;
    let outcome = build_sampling_collector(
        &mut result,
        schema,
        plan,
        policy,
        sampling_stats_concurrency,
        killer,
        progress,
        mem_tracker,
    );
    let _ = result.close();
    outcome
}

/// Go `buildSamplingStats`' merge half: `readDataAndSendTask` hands each
/// region's response to `samplingStatsConcurrency` `subMergeWorker`s, each
/// merging into its own collector, and the root collector merges theirs.
#[allow(clippy::too_many_arguments)]
fn build_sampling_collector<R: tidb_distsql::query_runtime::QueryResponse>(
    result: &mut tidb_distsql::query_runtime::QuerySelectResult<R>,
    schema: &SamplingSchema,
    plan: &AnalyzePlan,
    policy: tidb_stats::row_sample_collector::SamplePolicy,
    sampling_stats_concurrency: usize,
    killer: &tidb_sqlkiller::SqlKiller,
    progress: &(dyn Fn(i64) + Sync),
    mem_tracker: &std::sync::Arc<tidb_memory::Tracker>,
) -> Result<tidb_stats::row_sample_collector::RowSampleCollector, AnalyzeError> {
    use tidb_stats::row_sample_collector::RowSampleCollector;
    let concurrency = sampling_stats_concurrency.max(1);
    let (merge_task_tx, merge_task_rx) = crossbeam_channel::bounded::<Vec<u8>>(1);
    let (merge_result_tx, merge_result_rx) =
        crossbeam_channel::bounded::<Result<RowSampleCollector, AnalyzeError>>(1);
    // Go `taskCancel`: a failed merge worker stops the reader.
    let task_canceled = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        for index in 0..concurrency {
            let merge_task_rx = merge_task_rx.clone();
            let merge_result_tx = merge_result_tx.clone();
            let task_canceled = &task_canceled;
            scope.spawn(tidb_cpuprofile::labels::inherit(move || {
                let merged = crate::analyze::panic_recovery::recover_analyze_panic(|| {
                    sub_merge_worker(&merge_task_rx, schema, plan, policy, progress, mem_tracker, index)
                })
                .unwrap_or_else(|panic| Err(AnalyzeError::unsupported(panic.rendered_message().to_owned())));
                if merged.is_err() {
                    task_canceled.store(true, std::sync::atomic::Ordering::SeqCst);
                }
                let _ = merge_result_tx.send(merged);
            }));
        }
        drop(merge_task_rx);
        drop(merge_result_tx);
        // Merge the results from the workers.
        let root_merge = scope.spawn(tidb_cpuprofile::labels::inherit(|| {
            let mut root = RowSampleCollector::new(plan.slot_count(), policy);
            let mut err = None;
            for merged in merge_result_rx.iter() {
                match merged {
                    Ok(collector) => {
                        let old_root_size = root.go_mem_size();
                        let collector_size = collector.go_mem_size();
                        root.merge(collector);
                        mem_tracker.consume(root.go_mem_size() - old_root_size - collector_size);
                    }
                    Err(error) => err = Some(error),
                }
            }
            err.map_or(Ok(root), Err)
        }));
        let read = read_data_and_send_task(result, &merge_task_tx, &task_canceled, killer, mem_tracker);
        drop(merge_task_tx);
        let merged = root_merge
            .join()
            .unwrap_or_else(|_| Err(AnalyzeError::unsupported("analyze worker panic".to_owned())));
        match (read, merged) {
            (Ok(()), merged) => merged,
            (Err(read), Ok(_)) => Err(read),
            (Err(read), Err(merge)) => {
                if read.to_string() == merge.to_string() {
                    Err(read)
                } else {
                    Err(AnalyzeError::unsupported(format!("{read}\n{merge}")))
                }
            }
        }
    })
}

/// Go `readDataAndSendTask`.
fn read_data_and_send_task<R: tidb_distsql::query_runtime::QueryResponse>(
    result: &mut tidb_distsql::query_runtime::QuerySelectResult<R>,
    merge_task_tx: &crossbeam_channel::Sender<Vec<u8>>,
    task_canceled: &std::sync::atomic::AtomicBool,
    killer: &tidb_sqlkiller::SqlKiller,
    mem_tracker: &std::sync::Arc<tidb_memory::Tracker>,
) -> Result<(), AnalyzeError> {
    loop {
        if let Some(error) = killer.handle_signal() {
            return Err(AnalyzeError::Compute(crate::analyze::AnalyzeError::Execution(Box::new(
                crate::DriverError::Mysql(tidb_terror::MysqlError::from_terror(&error)),
            ))));
        }
        if task_canceled.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        let Some(data) = result.next_raw().map_err(|e| AnalyzeError::unsupported(e.to_string()))? else {
            return Ok(());
        };
        let data_size = data.capacity() as i64;
        mem_tracker.consume(data_size);
        if merge_task_tx.send(data).is_err() {
            mem_tracker.release(data_size);
            return Ok(());
        }
    }
}

/// Go `subMergeWorker`: merges the responses it takes into its own
/// collector, consuming each response's data and decoded size while it is
/// in flight, and the collector's growth.
fn sub_merge_worker(
    merge_task_rx: &crossbeam_channel::Receiver<Vec<u8>>,
    schema: &SamplingSchema,
    plan: &AnalyzePlan,
    policy: tidb_stats::row_sample_collector::SamplePolicy,
    progress: &(dyn Fn(i64) + Sync),
    mem_tracker: &std::sync::Arc<tidb_memory::Tracker>,
    _index: usize,
) -> Result<tidb_stats::row_sample_collector::RowSampleCollector, AnalyzeError> {
    use prost::Message;
    use tidb_stats::row_sample_collector::RowSampleCollector;
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/mockAnalyzeSamplingMergeWorkerPanic").is_some() {
        panic!("failpoint triggered");
    }
    let error = |e: String| AnalyzeError::unsupported(e);
    let mut ret_collector = RowSampleCollector::new(plan.slot_count(), policy);
    for data in merge_task_rx.iter() {
        let inflight_data_size = data.capacity() as i64;
        let response = if ret_collector.reservoir_is_full() {
            analyze_input::AnalyzeColumnsResp::decode(prost::bytes::Bytes::from(data))
        } else {
            analyze_input::AnalyzeColumnsResp::decode(data.as_slice())
        };
        let response = match response {
            Ok(response) => response,
            Err(e) => {
                mem_tracker.release(inflight_data_size);
                mem_tracker.release(ret_collector.go_mem_size());
                return Err(error(e.to_string()));
            }
        };
        let inflight_resp_size = response.encoded_len() as i64;
        mem_tracker.consume(inflight_resp_size);
        let release_inflight = || mem_tracker.release(inflight_data_size + inflight_resp_size);
        let Some(raw) = response.row_collector else {
            release_inflight();
            return Err(error("TiKV ANALYZE response has no row collector".to_owned()));
        };
        let projected = match schema.project_collector(plan, raw) {
            Ok(projected) => projected,
            Err(e) => {
                release_inflight();
                return Err(e);
            }
        };
        progress(projected.count);
        ret_collector.merge_owned_proto(projected, mem_tracker);
        release_inflight();
    }
    Ok(ret_collector)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_request_wire_matches_owned_protobuf_with_malformed_labels() {
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};

        let columns = [
            PushdownScanColumn {
                id: 12,
                field_type: FieldType::new(FieldTypeCode::Enum).with_elems([
                    "界".repeat(6000).into_bytes(),
                    Vec::new(),
                    b"embedded\0nul".to_vec(),
                    vec![0xe2, 0x82],
                    vec![0xac],
                    b"first".to_vec(),
                    b"second".to_vec(),
                    b"third".to_vec(),
                ]),
                is_handle: false,
                origin_default: Some(Datum::new_bytes_datum(vec![0xa4; 4096])),
            },
            PushdownScanColumn {
                id: 13,
                field_type: FieldType::new(FieldTypeCode::Enum).with_elems([vec![0xff; 70]]),
                is_handle: false,
                origin_default: None,
            },
            PushdownScanColumn {
                id: 14,
                field_type: FieldType::new(FieldTypeCode::Set).with_elems(
                    [vec![0xe2, 0x82], vec![0xac]]
                        .into_iter()
                        .chain((0..14).map(|i| format!("label\0{i}").into_bytes())),
                ),
                is_handle: false,
                origin_default: None,
            },
            PushdownScanColumn {
                id: -1,
                field_type: FieldType::new(FieldTypeCode::LongLong),
                is_handle: true,
                origin_default: None,
            },
        ];
        assert_columns_request_matches_generated(&columns);
        assert_columns_request_matches_generated(&columns[1..2]);
        assert_columns_request_matches_generated(&columns[2..3]);
    }

    #[test]
    fn wide_column_request_preserves_field_variants_and_repairs_the_envelope_length() {
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};

        let boundaries = [i64::MIN, -16_385, -129, -128, -1, 0, 127, 128, 16_383, 16_384, i64::MAX];
        let mut columns: Vec<_> = (0..64)
            .map(|index| PushdownScanColumn {
                id: boundaries[index % boundaries.len()],
                field_type: FieldType::new(FieldTypeCode::Varchar)
                    .with_flen(boundaries[(index + 3) % boundaries.len()])
                    .with_decimal(boundaries[(index + 5) % boundaries.len()])
                    .with_flags([0, 127, 128, 16_384][index % 4])
                    .with_array(index % 2 == 0),
                is_handle: false,
                origin_default: None,
            })
            .collect();
        columns[1].field_type = FieldType::new(FieldTypeCode::LongLong);
        columns[1].id = -1;
        columns[1].is_handle = true;
        columns[62].field_type = FieldType::new(FieldTypeCode::Enum).with_elems([vec![0xff]]);
        columns[63].origin_default = Some(Datum::new_bytes_datum(vec![0xa4; 20_000]));
        assert_columns_request_matches_generated(&columns);
    }

    #[test]
    fn short_column_request_preserves_default_values_and_presence() {
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};

        let defaults = [
            None,
            Some(Datum::Null),
            Some(Datum::Int(i64::MIN)),
            Some(Datum::UInt(u64::MAX)),
            Some(Datum::Real(-1.25)),
            Some(Datum::new_bytes_datum(Vec::new())),
            Some(Datum::new_bytes_datum(vec![0xa4; 62])),
            Some(Datum::new_bytes_datum(vec![0xa4; 63])),
        ];
        let columns: Vec<_> = defaults
            .into_iter()
            .enumerate()
            .map(|(index, default)| PushdownScanColumn {
                id: i64::MIN + index as i64,
                field_type: FieldType::new(FieldTypeCode::Blob)
                    .with_flen(-9000)
                    .with_decimal(-9000)
                    .with_flags(268_435_456)
                    .with_array(true),
                is_handle: index % 2 == 0,
                origin_default: default,
            })
            .collect();
        assert_columns_request_matches_generated(&columns[..7]);
        assert_columns_request_matches_generated(&columns);
        for column in &columns {
            assert_columns_request_matches_generated(std::slice::from_ref(column));
        }
    }

    fn assert_columns_request_matches_generated(columns: &[PushdownScanColumn]) {
        let template = tipb::AnalyzeReq {
            flags: Some(0),
            col_req: Some(tipb::AnalyzeColumnsReq {
                bucket_size: Some(64),
                columns_info: vec![tipb::ColumnInfo {
                    column_id: Some(7),
                    ..Default::default()
                }],
                primary_column_ids: vec![7, 12],
                sample_rate: Some(0.125),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut expected = template.clone();
        expected
            .col_req
            .as_mut()
            .unwrap()
            .columns_info
            .extend(columns.iter().map(|column| {
                crate::dag_request::column_into_pb(crate::cop_reader::column_to_proto(column).unwrap())
            }));
        let encoded = encode_columns_request(columns, |additional| {
            let mut request = template;
            request
                .col_req
                .as_mut()
                .unwrap()
                .columns_info
                .extend(additional);
            request
        }).unwrap();
        assert_eq!(encoded, expected.encode_to_vec());
        assert_eq!(tipb::AnalyzeReq::decode(encoded.as_slice()).unwrap(), expected);
    }

    /// Go `EncodeIndexKey` over `FullNotNullRange`/`NullRange` (single
    /// column) and `FullRange`: the high bound is `PrefixNext` of the
    /// `maxFlag` byte, `prefix\xfb`, never `PrefixNext(prefix)`.

    /// Go `buildResp`'s `SetStoreBatchSize(AnalyzeStoreBatchSize)`,
    /// `SetAllowBatchTaskDataMerge` and `SetExecuteBatchTasksSerially`, both
    /// true exactly when the size is positive.
    #[test]
    fn full_sampling_request_batches_by_store_like_go() {
        for (size, enabled) in [(4, true), (0, false)] {
            let mut builder = tidb_distsql::RequestBuilder::new();
            builder
                .set_analyze_request(Vec::new(), tidb_txnkv::IsolationLevel::ReadCommitted)
                .set_non_partitioned_key_ranges(vec![tidb_txnkv::KeyRange {
                    start_key: b"a".to_vec().into(),
                    end_key: b"b".to_vec().into(),
                }]);
            let metadata = super::set_analyze_store_batch(&mut builder, size)
                .build()
                .unwrap();
            assert_eq!(metadata.store_batch_size, size as isize);
            assert_eq!(metadata.allow_batch_task_data_merge, enabled);
            assert_eq!(metadata.execute_batch_tasks_serially, enabled);
        }
    }

    #[test]
    fn adaptive_concurrency_matches_go_store_count_boundaries() {
        for (stores, expected) in [
            (0, 4),
            (5, 4),
            (6, 6),
            (10, 10),
            (11, 22),
            (20, 40),
            (21, 63),
            (50, 150),
            (51, 204),
        ] {
            assert_eq!(super::concurrency_for_stores(stores), expected);
        }
    }

    #[test]
    fn kill_cancels_sampling_and_completed_guard_does_not_wait_for_kill() {
        let killer = tidb_sqlkiller::SqlKiller::default();
        let cancellation = std::sync::Arc::new(tidb_txnkv::CancelHandle::default());
        let guard = super::SamplingCancellation::start(&killer, cancellation.clone()).unwrap();
        killer.send_kill_signal(tidb_sqlkiller::KillSignal::QueryInterrupted);
        for _ in 0..100 {
            if cancellation.is_cancelled() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(cancellation.is_cancelled());
        drop(guard);
        killer.reset();
        let cancellation = std::sync::Arc::new(tidb_txnkv::CancelHandle::default());
        drop(super::SamplingCancellation::start(&killer, cancellation.clone()).unwrap());
        assert!(!cancellation.is_cancelled());
    }

    #[test]
    fn analyze_snapshot_controls_both_timestamp_and_isolation() {
        assert_eq!(
            super::read_policy(false, 123),
            (u64::MAX, tidb_txnkv::IsolationLevel::ReadCommitted)
        );
        assert_eq!(
            super::read_policy(true, 123),
            (123, tidb_txnkv::IsolationLevel::Snapshot)
        );
    }

    #[test]
    fn sample_decoding_restores_unsigned_integer_handle() {
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        let mut schema = SamplingSchema::new(
            &TableInfo::default(),
            &AnalyzePlan::new(Vec::new(), Vec::new(), "t").unwrap(),
        )
        .unwrap();
        schema.columns[0].field_type = FieldType::new(FieldTypeCode::LongLong)
            .with_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
        let mut row = vec![Datum::Bytes(
            tidb_codec::encode_value(&[Datum::UInt(u64::MAX)]).unwrap(),
        )];
        let handle = schema
            .decode_handle(&mut row, &tidb_datatype::SessionTimeZone::utc())
            .unwrap();
        assert_eq!(handle.encoded(), tidb_txnkv::IntHandle::new(-1).encoded());
        assert_eq!(row, vec![Datum::UInt(u64::MAX)]);
    }

    #[test]
    fn collector_projection_skips_handle_stats_and_preserves_index_stats() {
        let plan = AnalyzePlan::new(
            vec![crate::analyze::AnalyzedColumn {
                id: 7,
                name: "a".to_owned(),
                field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                absent_value: tidb_datatype::Datum::Null,
                collation: None,
            }],
            vec![crate::analyze::AnalyzedIndex {
                id: 3,
                column_positions: vec![0],
                prefix_lengths: vec![-1],
                single_column_unique: false,
            }],
            "t",
        )
        .unwrap();
        let schema = SamplingSchema::new(&TableInfo::default(), &plan).unwrap();
        let raw = analyze_input::RowSampleCollector {
            count: Some(100),
            null_counts: vec![2, 0, 3],
            total_size: vec![800, 999, 700],
            fm_sketch: vec![tipb::FmSketch::default(); 3],
            samples: vec![analyze_input::RowSample {
                row: vec![vec![0].into(), vec![8, 2].into()],
                weight: Some(91),
            }],
            ..Default::default()
        };
        let projected = schema.project_collector(&plan, raw.clone()).unwrap();
        assert_eq!(projected.count, 100);
        assert_eq!(projected.null_counts, vec![2]);
        assert_eq!(projected.total_sizes, vec![800]);
        assert_eq!(projected.samples[0].weight, 91);
        assert_eq!(projected.samples[0].row.len(), 2);
        let mut malformed = raw;
        malformed.fm_sketch.pop();
        assert!(schema.project_collector(&plan, malformed).is_err());
    }
}

/// Go's ANALYZE sends its sampling and index requests through the store's
/// coprocessor client (`copr.CopClient` over the `KVStore`): here the node's
/// client-rust store, which the opener carries.
pub(crate) fn native_transport<S: tidb_store_driver_txn::transaction::StoreClient>(
    opener: &tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener<S>,
    timeout: std::time::Duration,
) -> Result<
    tidb_copr::cop_paging::NativeCopTransport<dyn tidb_copr::cop_store::NativeCopStore>,
    String,
> {
    let store = opener
        .cop_store()
        .ok_or_else(|| "ANALYZE needs the node's store".to_owned())?;
    Ok(tidb_copr::cop_paging::NativeCopTransport::new(
        std::sync::Arc::clone(store),
        tidb_copr::cop_paging::CopRuntimeConfig {
            default_timeout: timeout,
            ..Default::default()
        },
    ))
}

/// Go `defer e.memTracker.Release(...)` / `Detach()`: an ANALYZE job's
/// tracker gives its bytes back and leaves the statement's tracker however
/// the build ends.
struct TrackerRelease<'a>(&'a std::sync::Arc<tidb_memory::Tracker>);

impl Drop for TrackerRelease<'_> {
    fn drop(&mut self) {
        self.0.release(self.0.bytes_consumed());
        self.0.detach();
    }
}
