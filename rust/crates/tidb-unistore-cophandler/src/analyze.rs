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
use tidb_datatype::CollationCollator;
use super::*;
use tidb_datatype::Datum;
use tidb_stats::row_sample_collector::{RowSampleCollector, SamplePolicy, ScannedRow, SlotValue};

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
        tipb::AnalyzeReq::decode(req.data.as_slice()).map_err(|e| other_error(&e.to_string()))?;
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
    let mut collector = RowSampleCollector::with_memory_quota_and_fm_sketch_size(
        columns.columns_info.len() + columns.column_groups.len(),
        policy,
        tidb_stats::row_sample_collector::SampleMemoryQuota::unlimited(),
        columns.sketch_size() as usize,
    );
    for range in &req.ranges {
        if range.start >= range.end {
            return Err(other_error("invalid ANALYZE range"));
        }
        let mut cursor = range.start.clone();
        loop {
            let pairs = store.scan(&tidb_unistore_dbreader::ScanReq {
                start_key: cursor,
                end_key: range.end.clone(),
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
                    tidb_tablecodec::decode_table_row_to_map(&pair.value, &types, Some(&timezone))
                        .map_err(|e| other_error(&e.to_string()))?;
                let mut values = Vec::new();
                for column in &columns.columns_info {
                    let value = if column.pk_handle() {
                        match handle {
                            RecordHandle::Int(value) => {
                                if column.flag() & 32 != 0 {
                                    Datum::UInt(value as u64)
                                } else {
                                    Datum::Int(value)
                                }
                            }
                            _ => return Err(other_error("invalid integer handle")),
                        }
                    } else if let Some(value) = decoded
                        .get(&column.column_id())
                        .filter(|value| !matches!(value, Datum::Null))
                    {
                        value.clone()
                    } else if let Some(position) = columns
                        .primary_column_ids
                        .iter()
                        .position(|id| *id == column.column_id())
                        // Go `tryAppendHandleColumn`: a column whose key bytes
                        // are not its value is never read from the handle.
                        .filter(|_| !types[&column.column_id()].need_restored_data())
                    {
                        common
                            .get(position)
                            .cloned()
                            .ok_or_else(|| other_error("missing common handle"))?
                    } else if decoded.contains_key(&column.column_id()) {
                        Datum::Null
                    } else if !column.default_val().is_empty() {
                        tidb_tablecodec::decode_column_value(
                            column.default_val(),
                            &types[&column.column_id()],
                            Some(&timezone),
                        )
                        .map_err(|e| other_error(&e.to_string()))?
                    } else {
                        Datum::Null
                    };
                    values.push(value);
                }
                let mut sampled = Vec::new();
                let mut keyed = Vec::new();
                let mut sizes = Vec::new();
                // Go `analyzeColumnsExec.Process`: the row decoded in the
                // request's zone goes back out through
                // `tablecodec.EncodeValue(sc.TimeZone(), ..)`, so a TIMESTAMP
                // sample is the stored UTC value again.
                for (value, column) in values.iter().zip(&columns.columns_info) {
                    let encoded = tidb_codec::encode_value_in_timezone(
                        &timezone,
                        std::slice::from_ref(value),
                    )
                    .map_err(|e| other_error(&e.to_string()))?;
                    sizes.push(if value.is_null() {
                        0
                    } else {
                        encoded.len() as i64 - 1
                    });
                    sampled.push(if value.is_null() {
                        Datum::Null
                    } else {
                        Datum::Bytes(encoded)
                    });
                    let field = &types[&column.column_id()];
                    let value = match value {
                        Datum::Bytes(bytes) => Datum::Bytes(field.collation().key(bytes)),
                        Datum::String(string) => {
                            Datum::Bytes(field.collation().key(string.bytes()))
                        }
                        other => other.clone(),
                    };
                    keyed.push(if value.is_null() {
                        Datum::Null
                    } else {
                        Datum::Bytes(
                            tidb_codec::encode_value_in_timezone(&timezone, &[value])
                                .map_err(|e| other_error(&e.to_string()))?,
                        )
                    });
                }
                let mut encoded_slots = Vec::new();
                let mut slot_sizes = sizes.clone();
                let mut nulls: Vec<_> = values.iter().map(Datum::is_null).collect();
                for value in &keyed {
                    encoded_slots.push(
                        tidb_codec::encode_value(std::slice::from_ref(value))
                            .map_err(|e| other_error(&e.to_string()))?,
                    );
                }
                for group in &columns.column_groups {
                    let positions = group
                        .column_offsets
                        .iter()
                        .map(|p| {
                            usize::try_from(*p)
                                .ok()
                                .filter(|p| *p < keyed.len())
                                .ok_or_else(|| other_error("invalid ANALYZE column group"))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let datums: Vec<_> = positions.iter().map(|p| keyed[*p].clone()).collect();
                    encoded_slots.push(
                        tidb_codec::encode_value(&datums)
                            .map_err(|e| other_error(&e.to_string()))?,
                    );
                    slot_sizes.push(positions.iter().map(|p| sizes[*p]).sum());
                    nulls.push(positions.len() == 1 && values[positions[0]].is_null());
                }
                let slots: Vec<_> = encoded_slots
                    .iter()
                    .enumerate()
                    .map(|(i, encoded)| SlotValue {
                        encoded_value: encoded,
                        size: slot_sizes[i],
                        is_null: nulls[i],
                    })
                    .collect();
                collector
                    .collect(&ScannedRow {
                        columns: &sampled,
                        slots: &slots,
                    })
                    .map_err(|e| other_error(&e.to_string()))?;
            }
            cursor = next;
            if cursor >= range.end {
                break;
            }
        }
    }
    let result = collector.to_proto();
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
        let (values, _) = tidb_tablecodec::cut_index_key(&key, column_count).map_err(|e| other_error(&e.to_string()))?;
        row.clear();
        for value in &values {
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
        let mut topn = tidb_stats::TopN::new(topn_values.len());
        for entry in &topn_values {
            topn.append(&entry.encoded, entry.count);
        }
        let bytes = tidb_stats::encode_cmsketch_and_topn(Some(&cms), Some(&topn)).unwrap_or_default();
        tipb::CmSketch::decode(bytes.as_slice()).map_err(|e| other_error(&e.to_string()))
    });
    Ok(tipb::AnalyzeIndexResp {
        hist: Some(histogram_to_proto(builder.histogram())),
        cms: cms.transpose()?,
        collector: Some(tipb::SampleCollector {
            fm_sketch: Some(tipb::FmSketch { mask: Some(fm_sketch.mask()), hashset: fm_sketch.sorted_hashes() }),
            ..Default::default()
        }),
    }
    .encode_to_vec())
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
        let mut cursor = range.start.clone();
        loop {
            let pairs = store.scan(&tidb_unistore_dbreader::ScanReq {
                start_key: cursor,
                end_key: range.end.clone(),
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
        let mut cursor = range.start.clone();
        loop {
            let pairs = store.scan(&tidb_unistore_dbreader::ScanReq {
                start_key: cursor,
                end_key: range.end.clone(),
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
                ranges: vec![coprocessor::KeyRange { start: prefix, end }],
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
                ranges: vec![coprocessor::KeyRange { start: prefix, end }],
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
