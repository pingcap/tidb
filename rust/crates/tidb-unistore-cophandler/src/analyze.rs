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

//! Go cophandler/analyze.go FullSampling request execution.
use std::borrow::Cow;
use std::ops::Range;

use tidb_datatype::CollationCollator;
use super::*;
use tidb_datatype::Datum;
use tidb_stats::cmsketch::{hash_bytes, Murmur3Hasher128};
use tidb_stats::row_sample_collector::{HashedSlotValue, RowSampleCollector, SamplePolicy};

struct KeyedColumn {
    // A non-NULL encoding always includes a type flag. For a wide borrowed
    // collation key, this range contains just the compact datum header.
    bytes: Range<usize>,
    // Zero means the entire key is in `bytes`.
    borrowed_payload_len: u32,
    original_size: i64,
    same_encoding: bool,
}

impl KeyedColumn {
    fn key_and_payload<'a>(&self, encoded: &'a [u8], value: &'a Datum) -> (&'a [u8], &'a [u8]) {
        let payload = if self.borrowed_payload_len == 0 {
            &[][..]
        } else {
            &string_or_binary_bytes(value).expect("borrowed string key")
                [..self.borrowed_payload_len as usize]
        };
        (&encoded[self.bytes.clone()], payload)
    }
}

fn string_or_binary_bytes(value: &Datum) -> Option<&[u8]> {
    match value {
        Datum::Bytes(bytes) => Some(bytes),
        Datum::String(text) => Some(text.bytes()),
        _ => None,
    }
}

fn append_compact_datum(output: &mut Vec<u8>, bytes: &[u8]) {
    output.push(tidb_codec::COMPACT_BYTES_FLAG);
    tidb_codec::encode_compact_bytes(output, bytes);
}

fn hash_column_key(hash: &mut Murmur3Hasher128, prefix: &mut Vec<u8>, key: &[u8], payload: &[u8]) {
    if key.is_empty() {
        hash.write(&[tidb_codec::NIL_FLAG]);
    } else {
        prefix.clear();
        prefix.push(tidb_codec::COMPACT_BYTES_FLAG);
        tidb_codec::encode_varint(prefix, (key.len() + payload.len()) as i64);
        hash.write(prefix);
        hash.write(key);
        if !payload.is_empty() {
            hash.write(payload);
        }
    }
}

pub(super) fn handle(store: &dyn tidb_unistore_dbreader::DbReader, req: &coprocessor::Request) -> coprocessor::Response {
    match execute(store, req) {
        Ok(data) => coprocessor::Response {
            data: data.into(),
            ..Default::default()
        },
        Err(response) => response,
    }
}

fn execute(
    store: &dyn tidb_unistore_dbreader::DbReader,
    req: &coprocessor::Request,
) -> Result<Vec<u8>, coprocessor::Response> {
    if req.ranges.is_empty() {
        return Ok(Vec::new());
    }
    let request =
        tipb::AnalyzeReq::decode(req.data.as_ref()).map_err(|e| other_error(&e.to_string()))?;
    if request.tp() == tipb::AnalyzeType::TypeIndex {
        return execute_index(store, req, &request);
    }
    if request.tp() == tipb::AnalyzeType::TypeCommonHandle {
        return execute_index_ndv(store, req, &request);
    }
    if request.tp() != tipb::AnalyzeType::TypeFullSampling {
        return Err(other_error("unsupported ANALYZE type"));
    }
    let columns = request
        .col_req
        .as_ref()
        .ok_or_else(|| other_error("missing ANALYZE columns"))?;
    let timezone = TimeZoneSpec::FixedOffset(request.time_zone_offset())
        .resolve()
        .map_err(|e| other_error(&e))?;
    let types: std::collections::BTreeMap<_, _> = columns
        .columns_info
        .iter()
        .map(|c| (c.column_id(), field_type_from_pb_column(c)))
        .collect();
    let policy = SamplePolicy::new_row_sample_collector(columns.sample_size() as usize, columns.sample_rate())
        .ok_or_else(|| other_error("invalid sample policy"))?;
    let column_count = columns.columns_info.len();
    let mut collected_groups = Vec::new();
    let mut single_column_groups = Vec::new();
    for (group_index, group) in columns.column_groups.iter().enumerate() {
        let offsets = group.column_offsets.as_slice();
        let single_column = match offsets {
            [column] => usize::try_from(*column)
                .ok()
                .filter(|&column| column < column_count),
            _ => None,
        };
        if let Some(column) = single_column {
            single_column_groups.push((column_count + group_index, column));
        } else {
            collected_groups.push(offsets);
        }
    }
    let mut collector = RowSampleCollector::with_fm_sketch_size(
        column_count + collected_groups.len(),
        policy,
        columns.sketch_size() as usize,
    );
    let mut key_prefix = Vec::new();
    let mut encoded_keys = Vec::new();
    let mut keyed_columns = Vec::with_capacity(column_count);
    let mut slots = Vec::with_capacity(column_count + collected_groups.len());
    for range in &req.ranges {
        if range.start >= range.end {
            return Err(other_error("invalid ANALYZE range"));
        }
        let mut cursor = range.start.to_vec();
        loop {
            let pairs = store.scan(&tidb_unistore_dbreader::ScanReq {
                start_key: cursor,
                end_key: range.end.to_vec(),
                limit: 1024,
                version: req.start_ts,
                sample_step: 0,
                reverse: false,
            });
            if pairs.is_empty() {
                break;
            }
            let mut next = pairs.last().unwrap().key.clone();
            next.push(0);
            for pair in pairs {
                if let Some(error) = pair.error {
                    return Err(match *error {
                        tidb_unistore_kverrors::KvError::Locked(lock) => coprocessor::Response {
                            locked: Some(*lock),
                            ..Default::default()
                        },
                        error => other_error(&error.to_string()),
                    });
                }
                let handle = tidb_tablecodec::table_key::decode_row_key(&pair.key)
                    .map_err(|e| other_error(&e.to_string()))?;
                let common = match &handle {
                    RecordHandle::Common(bytes) => {
                        tidb_codec::decode(bytes, columns.primary_column_ids.len())
                            .map_err(|e| other_error(&e.to_string()))?
                    }
                    _ => Vec::new(),
                };
                let decoded =
                    tidb_tablecodec::decode_owned_table_row_to_map(pair.value, &types, Some(&timezone))
                        .map_err(|e| other_error(&e.to_string()))?;
                let mut values = Vec::with_capacity(column_count);
                for column in &columns.columns_info {
                    let value = if column.pk_handle() {
                        Cow::Owned(match handle {
                            RecordHandle::Int(value) => {
                                if column.flag() & 32 != 0 {
                                    Datum::UInt(value as u64)
                                } else {
                                    Datum::Int(value)
                                }
                            }
                            _ => return Err(other_error("invalid integer handle")),
                        })
                    } else if let Some(value) = decoded
                        .get(&column.column_id())
                        .filter(|value| !matches!(value, Datum::Null))
                    {
                        Cow::Borrowed(value)
                    } else if let Some(position) = columns
                        .primary_column_ids
                        .iter()
                        .position(|id| *id == column.column_id())
                        // Go `tryAppendHandleColumn`: a column whose key bytes
                        // are not its value is never read from the handle.
                        .filter(|_| !types[&column.column_id()].need_restored_data())
                    {
                        let value = common
                            .get(position)
                            .ok_or_else(|| other_error("missing common handle"))?;
                        Cow::Borrowed(value)
                    } else if decoded.contains_key(&column.column_id()) {
                        Cow::Borrowed(&Datum::Null)
                    } else if !column.default_val().is_empty() {
                        Cow::Owned(tidb_tablecodec::decode_column_value(
                            column.default_val(),
                            &types[&column.column_id()],
                            Some(&timezone),
                        )
                        .map_err(|e| other_error(&e.to_string()))?)
                    } else {
                        Cow::Borrowed(&Datum::Null)
                    };
                    values.push(value);
                }
                encoded_keys.clear();
                keyed_columns.clear();
                // Go `analyzeColumnsExec.Process`: the row decoded in the
                // request's zone goes back out through
                // `tablecodec.EncodeValue(sc.TimeZone(), ..)`, so a TIMESTAMP
                // sample is the stored UTC value again.
                for (value, column) in values.iter().zip(&columns.columns_info) {
                    let value = value.as_ref();
                    let start = encoded_keys.len();
                    let (original_size, same_encoding, borrowed_payload_len) = if value.is_null() {
                        (0, true, 0)
                    } else if let Some(bytes) = string_or_binary_bytes(value) {
                        let original_size = tidb_codec::estimate_value_size(value)
                            .map_err(|e| other_error(&e.to_string()))? as i64 - 1;
                        let key = types[&column.column_id()].collation().immutable_key(bytes);
                        let borrowed_payload_len = if key.len() >= 2048
                            && key.len() <= bytes.len()
                            && std::ptr::eq(key.as_ptr(), bytes.as_ptr())
                        {
                            u32::try_from(key.len()).unwrap_or(0)
                        } else {
                            0
                        };
                        if borrowed_payload_len != 0 {
                            // The binary key is the original value or its
                            // trimmed prefix. Keep its existing payload.
                            encoded_keys.push(tidb_codec::COMPACT_BYTES_FLAG);
                            tidb_codec::encode_varint(&mut encoded_keys, key.len() as i64);
                        } else {
                            append_compact_datum(&mut encoded_keys, &key);
                        }
                        (original_size, std::ptr::eq(bytes, key.as_ref()), borrowed_payload_len)
                    } else {
                        tidb_codec::append_value_refs_in_timezone(
                            &timezone,
                            &mut encoded_keys,
                            &[value],
                        )
                        .map_err(|e| other_error(&e.to_string()))?;
                        ((encoded_keys.len() - start) as i64 - 1, true, 0)
                    };
                    keyed_columns.push(KeyedColumn {
                        bytes: start..encoded_keys.len(),
                        borrowed_payload_len,
                        original_size,
                        same_encoding,
                    });
                }
                slots.clear();
                for (position, column) in keyed_columns.iter().enumerate() {
                    let key = &encoded_keys[column.bytes.clone()];
                    let hash = if key.is_empty() {
                        0
                    } else if key.len() <= 63 && column.borrowed_payload_len == 0 {
                        // The zig-zag encoded non-negative length fits in one byte.
                        let mut compact = [0; 65];
                        compact[0] = tidb_codec::COMPACT_BYTES_FLAG;
                        compact[1] = (key.len() * 2) as u8;
                        compact[2..2 + key.len()].copy_from_slice(key);
                        hash_bytes(&compact[..2 + key.len()]).h1
                    } else {
                        let (_, payload) = column.key_and_payload(&encoded_keys, &values[position]);
                        let mut hash = Murmur3Hasher128::default();
                        hash_column_key(&mut hash, &mut key_prefix, key, payload);
                        hash.finish().h1
                    };
                    slots.push(HashedSlotValue {
                        hash,
                        size: column.original_size,
                        is_null: key.is_empty(),
                    });
                }
                for offsets in &collected_groups {
                    let mut hash = Murmur3Hasher128::default();
                    let mut size = 0;
                    for &offset in *offsets {
                        let position = usize::try_from(offset)
                            .ok()
                            .filter(|&position| position < keyed_columns.len())
                            .ok_or_else(|| other_error("invalid ANALYZE column group"))?;
                        let column = &keyed_columns[position];
                        let (key, payload) = column.key_and_payload(&encoded_keys, &values[position]);
                        hash_column_key(&mut hash, &mut key_prefix, key, payload);
                        size += column.original_size;
                    }
                    slots.push(HashedSlotValue {
                        hash: hash.finish().h1,
                        size,
                        is_null: false,
                    });
                }
                collector.collect_hashed(&slots, || {
                    values
                        .iter()
                        .zip(&keyed_columns)
                        .map(|(value, column)| {
                            let (key, payload) = column.key_and_payload(&encoded_keys, value);
                            if key.is_empty() {
                                return Datum::Null;
                            }
                            if column.same_encoding {
                                if payload.is_empty() {
                                    return Datum::Bytes(key.to_vec());
                                }
                                let mut original = Vec::with_capacity(key.len() + payload.len());
                                original.extend_from_slice(key);
                                original.extend_from_slice(payload);
                                return Datum::Bytes(original);
                            }
                            let bytes = string_or_binary_bytes(value)
                                .expect("only byte strings have collation keys");
                            // The flag plus space for the longest varint prefix.
                            let mut original = Vec::with_capacity(bytes.len() + 11);
                            append_compact_datum(&mut original, bytes);
                            Datum::Bytes(original)
                        })
                        .collect()
                });
            }
            cursor = next;
            if cursor.as_slice() >= range.end.as_ref() {
                break;
            }
        }
    }
    drop(encoded_keys);
    drop(key_prefix);
    let mut result = collector.into_proto();
    // Go's RowSampleBuilder copies these facts from the column after scanning.
    for (slot, column) in single_column_groups {
        result.null_counts.insert(slot, result.null_counts[column]);
        result.total_sizes.insert(slot, result.total_sizes[column]);
        result
            .fm_sketches
            .insert(slot, result.fm_sketches[column].clone());
    }
    Ok(tipb::AnalyzeColumnsResp {
        row_collector: Some(tipb::RowSampleCollector {
            count: Some(result.count),
            null_counts: result.null_counts,
            total_size: result.total_sizes,
            fm_sketch: result
                .fm_sketches
                .into_iter()
                .map(|f| {
                    let f = f.unwrap_or_default();
                    tipb::FmSketch {
                        mask: Some(f.mask),
                        hashset: f.hashset,
                    }
                })
                .collect(),
            samples: result
                .samples
                .into_iter()
                .map(|s| tipb::RowSample {
                    row: s.row,
                    weight: Some(s.weight),
                })
                .collect(),
            ..Default::default()
        }),
        ..Default::default()
    }
    .encode_to_vec())
}

/// Go `handleAnalyzeIndexReq` with its `analyzeIndexProcessor`: every index
/// key in the ranges, its columns cut from the key, feeds a sorted histogram
/// of `bucket_size` buckets, the CMSketch (each column prefix), the FMSketch
/// and, for stats version 2, the runs of equal values from which the
/// `top_n_size` most frequent become the response's TopN (and leave the
/// CMSketch).
fn execute_index(
    store: &dyn tidb_unistore_dbreader::DbReader,
    req: &coprocessor::Request,
    request: &tipb::AnalyzeReq,
) -> Result<Vec<u8>, coprocessor::Response> {
    let index = request
        .idx_req
        .as_ref()
        .ok_or_else(|| other_error("missing ANALYZE index"))?;
    let stats_version = index.version.unwrap_or(1);
    let column_count = usize::try_from(index.num_columns()).map_err(|_| other_error("invalid index column count"))?;
    let mut builder = tidb_stats::SortedHistogramBuilder::new(
        index.bucket_size(),
        0,
        &tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Blob),
        stats_version as isize,
    );
    let topn_count = index.top_n_size.unwrap_or(0);
    let mut cms = match (index.cmsketch_depth, index.cmsketch_width) {
        (Some(depth), Some(width)) => Some(
            tidb_stats::CmsSketch::try_new(depth as u32, width as u32)
                .map_err(|_| other_error("invalid CMSketch dimensions"))?,
        ),
        _ => None,
    };
    let mut fm_sketch = tidb_stats::FmSketch::new(index.sketch_size() as usize);
    let mut topn_values: Vec<tidb_stats::TopNEntry> = Vec::new();
    let mut current: Option<tidb_stats::TopNEntry> = None;
    let mut row = Vec::new();
    for_each_key(store, req, |key| {
        let key = crate::mpp_exec::decode_index_keyspace_prefix(key).map_err(|error| other_error(&error))?;
        let (values, _) = tidb_tablecodec::cut_index_key_refs(&key, column_count)
            .map_err(|e| other_error(&e.to_string()))?;
        row.clear();
        for value in values {
            row.extend_from_slice(value);
            if let Some(cms) = cms.as_mut() {
                cms.insert_bytes(&row);
            }
        }
        let encoded = tidb_codec::encode_value(&[Datum::Bytes(row.clone())]).map_err(|e| other_error(&e.to_string()))?;
        tidb_stats::insert_encoded_value(&mut fm_sketch, &encoded);
        if stats_version >= 2 {
            match current.as_mut() {
                Some(run) if run.encoded == row => run.count += 1,
                _ => {
                    if let Some(run) = current.replace(tidb_stats::TopNEntry { encoded: row.clone(), count: 1 }) {
                        topn_values.push(run);
                    }
                }
            }
        }
        builder.iterate(Datum::Bytes(row.clone())).map_err(|e| other_error(&e.to_string()))
    })?;
    if stats_version >= 2 {
        topn_values.extend(current);
        tidb_stats::sort_topn_meta(&mut topn_values);
        topn_values.truncate(usize::try_from(topn_count).unwrap_or(0));
    }
    let cms = cms.map(|mut cms| {
        if stats_version >= 2 {
            for entry in &topn_values {
                cms.sub_hashed(tidb_stats::hash_bytes(&entry.encoded), entry.count);
            }
        }
        index_cmsketch_to_proto(&cms, topn_values)
    });
    Ok(tipb::AnalyzeIndexResp {
        hist: Some(histogram_to_proto(builder.histogram())),
        cms,
        collector: Some(tipb::SampleCollector {
            fm_sketch: Some(tipb::FmSketch { mask: Some(fm_sketch.mask()), hashset: fm_sketch.sorted_hashes() }),
            ..Default::default()
        }),
    }
    .encode_to_vec())
}

/// Go `CMSketchToProto` for index ANALYZE, which already owns the TopN keys.
fn index_cmsketch_to_proto(
    cms: &tidb_stats::CmsSketch,
    topn_values: Vec<tidb_stats::TopNEntry>,
) -> tipb::CmSketch {
    tipb::CmSketch {
        rows: cms
            .counter_rows()
            .map(|row| tipb::CmSketchRow { counters: row.to_vec() })
            .collect(),
        top_n: topn_values
            .into_iter()
            .map(|entry| tipb::CmSketchTopN {
                data: Some(entry.encoded),
                count: Some(entry.count),
            })
            .collect(),
        default_value: Some(cms.default_value()),
    }
}

/// Go `HistogramToProto`.
fn histogram_to_proto(histogram: &tidb_stats::Histogram) -> tipb::Histogram {
    let bytes = |datum: &Datum| match datum {
        Datum::Bytes(bytes) => bytes.clone(),
        _ => Vec::new(),
    };
    tipb::Histogram {
        ndv: Some(histogram.ndv),
        buckets: histogram
            .buckets
            .iter()
            .map(|bucket| tipb::Bucket {
                count: Some(bucket.count),
                lower_bound: Some(bytes(&bucket.lower_bound)),
                upper_bound: Some(bytes(&bucket.upper_bound)),
                repeats: Some(bucket.repeat),
                ndv: Some(bucket.ndv),
            })
            .collect(),
    }
}

/// Go `dbReader.Scan` over every request range at the request's start TS,
/// handing each key to `process` (Go's `ScanProcessor.Process`); a lock
/// becomes the response's `locked`.
fn for_each_key(
    store: &dyn tidb_unistore_dbreader::DbReader,
    req: &coprocessor::Request,
    mut process: impl FnMut(&[u8]) -> Result<(), coprocessor::Response>,
) -> Result<(), coprocessor::Response> {
    for range in &req.ranges {
        if range.start >= range.end {
            return Err(other_error("invalid ANALYZE range"));
        }
        let mut cursor = range.start.to_vec();
        loop {
            let pairs = store.scan(&tidb_unistore_dbreader::ScanReq {
                start_key: cursor,
                end_key: range.end.to_vec(),
                limit: 1024,
                version: req.start_ts,
                sample_step: 0,
                reverse: false,
            });
            if pairs.is_empty() {
                break;
            }
            let mut next = pairs.last().unwrap().key.clone();
            next.push(0);
            for pair in pairs {
                if let Some(error) = pair.error {
                    return Err(match *error {
                        tidb_unistore_kverrors::KvError::Locked(lock) => coprocessor::Response {
                            locked: Some(*lock),
                            ..Default::default()
                        },
                        error => other_error(&error.to_string()),
                    });
                }
                process(&pair.key)?;
            }
            cursor = next;
        }
    }
    Ok(())
}

// The common-handle NDV subtask uses one bucket and no TopN/CMS.
fn execute_index_ndv(
    store: &dyn tidb_unistore_dbreader::DbReader,
    req: &coprocessor::Request,
    request: &tipb::AnalyzeReq,
) -> Result<Vec<u8>, coprocessor::Response> {
    let index = request
        .idx_req
        .as_ref()
        .ok_or_else(|| other_error("missing ANALYZE index"))?;
    if index.bucket_size() != 1
        || index.top_n_size() != 0
        || index.cmsketch_depth() != 0
        || index.cmsketch_width() != 0
        || index.version() != 2
        || index.num_columns() <= 0
        || index.sketch_size() <= 0
    {
        return Err(other_error("unsupported index ANALYZE options"));
    }
    let mut sketch = tidb_stats::FmSketch::new(index.sketch_size() as usize);
    let mut count = 0;
    let mut lower = None;
    let mut upper = Vec::new();
    let mut repeats = 0;
    for range in &req.ranges {
        if range.start >= range.end {
            return Err(other_error("invalid ANALYZE range"));
        }
        let mut cursor = range.start.to_vec();
        loop {
            let pairs = store.scan(&tidb_unistore_dbreader::ScanReq {
                start_key: cursor,
                end_key: range.end.to_vec(),
                limit: 1024,
                version: req.start_ts,
                sample_step: 0,
                reverse: false,
            });
            if pairs.is_empty() {
                break;
            }
            let mut next = pairs.last().unwrap().key.clone();
            next.push(0);
            for pair in pairs {
                if let Some(error) = pair.error {
                    return Err(match *error {
                        tidb_unistore_kverrors::KvError::Locked(lock) => coprocessor::Response {
                            locked: Some(*lock),
                            ..Default::default()
                        },
                        error => other_error(&error.to_string()),
                    });
                }
                let RecordHandle::Common(bytes) =
                    tidb_tablecodec::table_key::decode_row_key(&pair.key)
                        .map_err(|e| other_error(&e.to_string()))?
                else {
                    return Err(other_error("invalid common handle"));
                };
                let mut remaining = bytes.as_slice();
                let mut key = Vec::new();
                for _ in 0..index.num_columns() {
                    let (part, rest) = tidb_codec::cut_one(remaining)
                        .map_err(|e| other_error(&e.to_string()))?;
                    key.extend_from_slice(part);
                    remaining = rest;
                }
                let encoded = tidb_codec::encode_value(&[Datum::Bytes(key.clone())])
                    .map_err(|e| other_error(&e.to_string()))?;
                tidb_stats::insert_encoded_value(&mut sketch, &encoded);
                count += 1;
                if lower.is_none() {
                    lower = Some(key.clone());
                }
                repeats = if key == upper { repeats + 1 } else { 1 };
                upper = key;
            }
            cursor = next;
        }
    }
    let buckets = lower
        .map(|lower| {
            vec![tipb::Bucket {
                count: Some(count),
                lower_bound: Some(lower),
                upper_bound: Some(upper),
                repeats: Some(repeats),
                ..Default::default()
            }]
        })
        .unwrap_or_default();
    Ok(tipb::AnalyzeIndexResp {
        hist: Some(tipb::Histogram {
            ndv: Some(sketch.ndv()),
            buckets,
        }),
        collector: Some(tipb::SampleCollector {
            fm_sketch: Some(tipb::FmSketch {
                mask: Some(sketch.mask()),
                hashset: sketch.sorted_hashes(),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
    .encode_to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_unistore_tikv::mvcc_store::MvccStore;
    #[test]
    fn index_ndv_reads_encoded_prefix_keys() {
        check_index_ndv(false, false);
        check_index_ndv(false, true);
        check_index_ndv(true, false);
    }

    #[test]
    fn index_cmsketch_proto_retains_key_storage_and_explicit_empty_fields() {
        let mut sketch = tidb_stats::CmsSketch::new(2, 0);
        sketch.set_default_value(u64::MAX);
        let key = vec![b'a'; 128];
        let key_ptr = key.as_ptr();
        let proto = index_cmsketch_to_proto(
            &sketch,
            vec![
                tidb_stats::TopNEntry {
                    encoded: key,
                    count: 0,
                },
                tidb_stats::TopNEntry {
                    encoded: Vec::new(),
                    count: 1,
                },
            ],
        );
        assert_eq!(proto.rows.len(), 2);
        assert!(proto.rows.iter().all(|row| row.counters.is_empty()));
        assert_eq!(proto.default_value, Some(u64::MAX));
        assert_eq!(proto.top_n[0].data.as_ref().unwrap().as_ptr(), key_ptr);
        assert_eq!(proto.top_n[0].count, Some(0));
        assert_eq!(proto.top_n[1].data, Some(Vec::new()));
        assert_eq!(proto.top_n[1].count, Some(1));
    }

    /// Go `handleAnalyzeIndexReq` for `AnalyzeIndexExec`'s full request:
    /// the histogram covers every key, the TopN holds the most frequent
    /// value and the CMSketch is sent with it.
    #[test]
    fn index_request_builds_histogram_topn_and_cmsketch() {
        use tidb_proto::{KvrpcMutation, KvrpcOp};
        let mut store = MvccStore::new();
        let prefix = tidb_tablecodec::table_key::encode_index_seek_key(42, 3, &[]);
        for (id, value) in [(1, "aa"), (2, "aa"), (3, "bb")] {
            let mut key = prefix.clone();
            key.extend(tidb_codec::encode_key(&[Datum::Bytes(value.as_bytes().to_vec()), Datum::Int(id)]).unwrap());
            store
                .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation { op: KvrpcOp::Put as i32, key: key.clone(), value: vec![0], ..Default::default() }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..Default::default()
                })
                .unwrap();
            store.commit(&[key], 10, 11).unwrap();
        }
        let mut end = prefix.clone();
        *end.last_mut().unwrap() += 1;
        let request = tipb::AnalyzeReq {
            tp: Some(tipb::AnalyzeType::TypeIndex as i32),
            idx_req: Some(tipb::AnalyzeIndexReq {
                num_columns: Some(1),
                bucket_size: Some(256),
                top_n_size: Some(1),
                cmsketch_depth: Some(5),
                cmsketch_width: Some(2048),
                sketch_size: Some(10000),
                version: Some(2),
                ..Default::default()
            }),
            ..Default::default()
        };
        let response = handle(
            &mut store,
            &coprocessor::Request {
                data: request.encode_to_vec().into(),
                start_ts: 20,
                ranges: vec![coprocessor::KeyRange { start: prefix.into(), end: end.into() }],
                ..Default::default()
            },
        );
        assert!(response.other_error.is_empty(), "{}", response.other_error);
        let response = tipb::AnalyzeIndexResp::decode(response.data.as_ref()).unwrap();
        let hist = response.hist.unwrap();
        assert_eq!(hist.ndv(), 2);
        assert_eq!(hist.buckets.last().unwrap().count(), 3);
        let cms = response.cms.unwrap();
        assert_eq!(cms.rows.len(), 5);
        assert_eq!(cms.top_n.len(), 1);
        assert_eq!(cms.top_n[0].count(), 2);
        assert_eq!(cms.top_n[0].data(), tidb_codec::encode_key(&[Datum::Bytes(b"aa".to_vec())]).unwrap());
    }

    #[test]
    fn full_sampling_preserves_interleaved_group_slots() {
        check_full_sampling_group_slots(tidb_charset::Collation::Utf8Mb4GeneralCi, -45, "Value É  ");
    }

    #[test]
    fn full_sampling_hashes_trimmed_wide_keys_and_keeps_original_bytes() {
        let value = format!("{}  ", "Value É".repeat(410));
        check_full_sampling_group_slots(tidb_charset::Collation::Utf8Mb4Bin, -46, &value);
    }

    fn check_full_sampling_group_slots(text: tidb_charset::Collation, collation_id: i32, value: &str) {
        use std::cell::RefCell;
        use tidb_unistore_dbreader::{DbReader, KvPair, ScanReq};

        struct Rows(RefCell<Vec<KvPair>>);
        impl DbReader for Rows {
            fn scan(&self, _request: &ScanReq) -> Vec<KvPair> {
                std::mem::take(&mut *self.0.borrow_mut())
            }
        }

        let key =
            |id| tidb_tablecodec::table_key::encode_row_key_with_handle(42, &RecordHandle::Int(id));
        let row = [
            Datum::new_collation_string_datum(value, text),
            Datum::Null,
        ];
        let reader = Rows(RefCell::new(vec![KvPair {
            key: key(7),
            value: tidb_tablecodec::encode_table_row(None, &row, &[2, 3], true, None).unwrap(),
            error: None,
            commit_ts: 0,
        }]));
        let columns_info = [(1, 8, -63, true), (2, 252, collation_id, false), (3, 252, -63, false)]
            .into_iter()
            .map(|(id, tp, collation, pk)| tipb::ColumnInfo {
                column_id: Some(id),
                tp: Some(tp),
                collation: Some(collation),
                pk_handle: Some(pk),
                ..Default::default()
            })
            .collect();
        let column_groups = [vec![2], vec![1, 0], vec![0], vec![2], vec![1]]
            .into_iter()
            .map(|column_offsets| tipb::AnalyzeColumnGroup {
                column_offsets,
                ..Default::default()
            })
            .collect();
        let request = tipb::AnalyzeReq {
            tp: Some(tipb::AnalyzeType::TypeFullSampling as i32),
            col_req: Some(tipb::AnalyzeColumnsReq {
                columns_info,
                column_groups,
                sample_rate: Some(1.0),
                sketch_size: Some(64),
                ..Default::default()
            }),
            ..Default::default()
        };
        let response = handle(
            &reader,
            &coprocessor::Request {
                data: request.encode_to_vec().into(),
                ranges: vec![coprocessor::KeyRange { start: key(0).into(), end: key(8).into() }],
                ..Default::default()
            },
        );
        assert!(response.other_error.is_empty(), "{}", response.other_error);
        let collector = tipb::AnalyzeColumnsResp::decode(response.data.as_ref())
            .unwrap()
            .row_collector
            .unwrap();
        assert_eq!(collector.count(), 1);
        assert_eq!(collector.samples.len(), 1);
        let original = tidb_codec::encode_value(&row[..1]).unwrap();
        assert_eq!(collector.samples[0].row[1], original);
        assert_eq!(collector.total_size[1], original.len() as i64 - 1);
        let key = tidb_codec::encode_value(&[Datum::Bytes(text.key(row[0].go_bytes()))]).unwrap();
        let slot = tidb_codec::encode_value(&[Datum::Bytes(key)]).unwrap();
        let hash = tidb_stats::cmsketch::hash_bytes(&slot).h1;
        assert_eq!(collector.fm_sketch[1].hashset, [hash]);
        assert_eq!(collector.null_counts, [0, 0, 1, 1, 0, 0, 1, 0]);
        assert_eq!(
            collector.total_size[4],
            collector.total_size[0] + collector.total_size[1]
        );
        for (slot, column) in [(3, 2), (5, 0), (6, 2), (7, 1)] {
            assert_eq!(collector.total_size[slot], collector.total_size[column]);
            assert_eq!(collector.fm_sketch[slot], collector.fm_sketch[column]);
        }
        let mut group = slot;
        let handle = tidb_codec::encode_value(&[Datum::Int(7)]).unwrap();
        group.extend(tidb_codec::encode_value(&[Datum::Bytes(handle)]).unwrap());
        assert_eq!(collector.fm_sketch[4].hashset, [tidb_stats::cmsketch::hash_bytes(&group).h1]);
    }

    fn check_index_ndv(common: bool, nulls: bool) {
        use tidb_proto::{KvrpcMutation, KvrpcOp};
        let mut store = MvccStore::new();
        let prefix = if common {
            tidb_tablecodec::table_key::gen_table_record_prefix(42)
        } else {
            tidb_tablecodec::table_key::encode_index_seek_key(42, 3, &[])
        };
        for (id, value) in [(1, "aa"), (2, "aa"), (3, "bb")] {
            let mut key = prefix.clone();
            key.extend(
                tidb_codec::encode_key(&[
                    if nulls {
                        Datum::Null
                    } else {
                        Datum::Bytes(value.as_bytes().to_vec())
                    },
                    Datum::Int(id),
                ])
                .unwrap(),
            );
            store
                .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value: vec![0],
                        ..Default::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..Default::default()
                })
                .unwrap();
            store.commit(&[key], 10, 11).unwrap();
        }
        let mut end = prefix.clone();
        *end.last_mut().unwrap() += 1;
        let request = tipb::AnalyzeReq {
            tp: Some(if common {
                tipb::AnalyzeType::TypeCommonHandle
            } else {
                tipb::AnalyzeType::TypeIndex
            } as i32),
            idx_req: Some(tipb::AnalyzeIndexReq {
                num_columns: Some(1),
                bucket_size: Some(1),
                sketch_size: Some(1000),
                version: Some(2),
                ..Default::default()
            }),
            ..Default::default()
        };
        let response = handle(
            &mut store,
            &coprocessor::Request {
                data: request.encode_to_vec().into(),
                start_ts: 20,
                ranges: vec![coprocessor::KeyRange { start: prefix.into(), end: end.into() }],
                ..Default::default()
            },
        );
        assert!(response.other_error.is_empty(), "{}", response.other_error);
        let response = tipb::AnalyzeIndexResp::decode(response.data.as_ref()).unwrap();
        let sketch = response.collector.unwrap().fm_sketch.unwrap();
        assert_eq!(
            tidb_stats::FmSketch::from_raw_parts(sketch.mask(), 1000, sketch.hashset).ndv(),
            if nulls { 1 } else { 2 }
        );
        assert_eq!(response.hist.unwrap().buckets.last().unwrap().count(), 3);
    }
}
