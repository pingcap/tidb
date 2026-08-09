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

//! Legacy per-column collector behavior from `pkg/statistics/sample.go`.

use std::cmp::Ordering;

use tidb_datatype::{Collation, Datum, DatumValueError};
use tidb_txnkv::Handle;
use tidb_util::fastrand::{uint32_n, uint64_n};

use crate::cmsketch::sampled_topn_candidates;
use crate::go_stable_sort::go_stable_sort_by;
use crate::{
    decode_cmsketch_and_embedded_topn, encode_cmsketch_without_topn, fm_sketch_from_proto,
    fm_sketch_to_proto, hash_bytes, CmsSketch, CodecError, FmSketch, FmSketchProto,
    SortedHistogramBuilder, TopN, MAX_SAMPLE_VALUE_LENGTH,
};

#[derive(Clone, Debug)]
pub struct LegacySampleItem {
    pub value: Datum,
    pub handle: Option<Handle>,
    pub ordinal: isize,
}

/// Source `EmptySampleItemSize` on TiDB's supported 64-bit targets.
///
/// This is Go memory-accounting geometry, not Rust's native struct layout:
/// `types.Datum` is 72 bytes, a `kv.Handle` interface is 16 bytes, and an
/// `int` is 8 bytes.
pub const EMPTY_SAMPLE_ITEM_SIZE: i64 = 96;

/// Injectable form of the two `util/fastrand` draws used by Go's legacy
/// reservoir sampler.
pub trait LegacySampleRng {
    fn uint64_n(&mut self, upper: u64) -> u64;
    fn uint32_n(&mut self, upper: u32) -> u32;
}

struct GlobalLegacySampleRng;

impl LegacySampleRng for GlobalLegacySampleRng {
    fn uint64_n(&mut self, upper: u64) -> u64 {
        uint64_n(upper)
    }

    fn uint32_n(&mut self, upper: u32) -> u32 {
        uint32_n(upper)
    }
}

fn datum_get_bytes(value: &Datum) -> Vec<u8> {
    match value {
        Datum::Json(value) => value.value().to_vec(),
        Datum::VectorFloat32(value) => value.serialize(),
        value => value.go_bytes().to_vec(),
    }
}

/// Stable Go `sortSampleItems`, including its comparator-schedule-dependent
/// final error.
pub fn sort_legacy_sample_items(items: &mut [LegacySampleItem]) -> Result<(), DatumValueError> {
    let mut error = None;
    go_stable_sort_by(items, |left, right| {
        match left.value.compare(&right.value, Collation::Binary) {
            Ok(ordering) => {
                error = None;
                ordering
            }
            Err(found) => {
                error = Some(found);
                Ordering::Less
            }
        }
    });
    error.map_or(Ok(()), Err)
}

#[derive(Clone, Debug, Default)]
pub struct LegacySampleCollector {
    pub fm_sketch: Option<FmSketch>,
    pub cmsketch: Option<CmsSketch>,
    pub top_n: Option<TopN>,
    pub samples: Vec<LegacySampleItem>,
    pub seen_values: i64,
    pub null_count: i64,
    pub count: i64,
    pub max_sample_size: i64,
    pub total_size: i64,
    pub memory_size: i64,
    pub is_merger: bool,
}

impl LegacySampleCollector {
    /// Go `Destroy`, including assigning the sample slice to nil.
    pub fn destroy(&mut self) {
        self.fm_sketch = None;
        self.cmsketch = None;
        self.top_n = None;
        self.samples = Vec::new();
        self.seen_values = 0;
        self.null_count = 0;
        self.count = 0;
        self.max_sample_size = 0;
        self.total_size = 0;
        self.memory_size = 0;
        self.is_merger = false;
    }

    /// Offers one datum under Go's two-stream collector contract.
    ///
    /// `encode_value` is `codec.EncodeValue` for the FM sketch. CMS, size,
    /// retained samples, protobuf, and `CalcTotalSize` instead read the
    /// Datum's independent `GetBytes` backing payload.
    pub fn collect<E>(
        &mut self,
        value: Datum,
        encode_value: impl FnOnce(&Datum) -> Result<Vec<u8>, E>,
    ) -> Result<(), E> {
        let mut rng = GlobalLegacySampleRng;
        self.collect_with_rng(value, encode_value, &mut rng)
    }

    pub fn collect_with_rng<E>(
        &mut self,
        value: Datum,
        encode_value: impl FnOnce(&Datum) -> Result<Vec<u8>, E>,
        rng: &mut impl LegacySampleRng,
    ) -> Result<(), E> {
        if !self.is_merger {
            if value == Datum::Null {
                self.null_count = self.null_count.wrapping_add(1);
                return Ok(());
            }
            self.count = self.count.wrapping_add(1);
            let encoded_for_fm = encode_value(&value)?;
            self.fm_sketch
                .as_mut()
                .expect("SampleCollector.collect requires FMSketch")
                .insert_hash(hash_bytes(&encoded_for_fm).h1);
            let value_bytes = datum_get_bytes(&value);
            if let Some(cms) = &mut self.cmsketch {
                cms.insert_bytes(&value_bytes);
            }
            self.total_size = self.total_size.wrapping_add(value_bytes.len() as i64 - 1);
        }
        self.seen_values = self.seen_values.wrapping_add(1);
        if self.samples.len() < self.max_sample_size.max(0) as usize {
            self.samples.push(LegacySampleItem {
                value,
                handle: None,
                ordinal: 0,
            });
        } else {
            let should_add = (rng.uint64_n(self.seen_values as u64) as i64) < self.max_sample_size;
            if should_add {
                let index = rng.uint32_n(self.max_sample_size as u32) as usize;
                self.samples.remove(index);
                self.samples.push(LegacySampleItem {
                    value,
                    handle: None,
                    ordinal: 0,
                });
            }
        }
        Ok(())
    }

    /// Go `MergeSampleCollector`; incompatible CMS shapes are logged and the
    /// merge continues, so this pure port deliberately ignores that error.
    pub fn merge_with_encoder<E>(
        &mut self,
        source: &Self,
        mut encode_value: impl FnMut(&Datum) -> Result<Vec<u8>, E>,
        mut log_collect_error: impl FnMut(E),
    ) {
        self.null_count = self.null_count.wrapping_add(source.null_count);
        self.count = self.count.wrapping_add(source.count);
        self.total_size = self.total_size.wrapping_add(source.total_size);
        if let (Some(destination), Some(source)) = (&mut self.fm_sketch, &source.fm_sketch) {
            destination.merge(source);
        }
        if let (Some(destination), Some(source)) = (&mut self.cmsketch, &source.cmsketch) {
            let _ = destination.merge(source);
        }
        for item in &source.samples {
            if let Err(error) = self.collect(item.value.clone(), |value| encode_value(value)) {
                log_collect_error(error);
            }
        }
    }

    /// UTC convenience for callers without a statement time-zone adapter.
    pub fn merge(&mut self, source: &Self) {
        self.merge_with_encoder(
            source,
            |value| tidb_codec::encode_value(std::slice::from_ref(value)),
            |_| {},
        );
    }

    pub fn calculate_total_size(&mut self) {
        self.total_size = self.samples.iter().fold(0_i64, |total, item| {
            total.wrapping_add(datum_get_bytes(&item.value).len() as i64)
        });
    }

    /// Go `ExtractTopN` over already-decoded/re-encoded sample bytes.
    pub fn extract_topn<E>(
        &mut self,
        number: u32,
        normalize: impl FnMut(&[u8]) -> Result<Vec<u8>, E>,
    ) -> Result<(), E> {
        self.extract_topn_with_tie_stabilization(number, false, normalize)
    }

    /// Go `ExtractTopN` with explicit control of the source
    /// `StabilizeV1AnalyzeTopN` failpoint.
    pub fn extract_topn_with_tie_stabilization<E>(
        &mut self,
        number: u32,
        stabilize_equal_counts: bool,
        mut normalize: impl FnMut(&[u8]) -> Result<Vec<u8>, E>,
    ) -> Result<(), E> {
        if number == 0 {
            return Ok(());
        }
        let values: Vec<_> = self
            .samples
            .iter()
            .map(|sample| datum_get_bytes(&sample.value))
            .collect();
        let candidates = sampled_topn_candidates(&values, number, stabilize_equal_counts);
        // Go replaces c.TopN before processing the first candidate. Each
        // successful candidate then mutates CMS and TopN immediately, so a
        // later codec error exposes that prefix rather than rolling it back.
        self.top_n = Some(TopN::new(candidates.len()));
        for candidate in candidates {
            let hash = hash_bytes(&candidate.encoded);
            let count = self
                .cmsketch
                .as_ref()
                .expect("ExtractTopN requires CMSketch")
                .query_hashed(hash);
            let normalized = normalize(&candidate.encoded)?;
            self.cmsketch
                .as_mut()
                .expect("ExtractTopN requires CMSketch")
                .sub_hashed(hash, count);
            self.top_n
                .as_mut()
                .expect("TopN was initialized above")
                .append(&normalized, count);
        }
        self.top_n
            .as_mut()
            .expect("TopN was initialized above")
            .sort();
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LegacySampleCollectorProto {
    pub samples: Vec<Vec<u8>>,
    pub null_count: i64,
    pub count: i64,
    pub fm_sketch: Option<FmSketchProto>,
    pub cmsketch: Option<Vec<u8>>,
    pub total_size: Option<i64>,
}

pub fn legacy_sample_collector_to_proto(
    collector: &LegacySampleCollector,
) -> Result<LegacySampleCollectorProto, CodecError> {
    Ok(LegacySampleCollectorProto {
        samples: collector
            .samples
            .iter()
            .map(|item| datum_get_bytes(&item.value))
            .collect(),
        null_count: collector.null_count,
        count: collector.count,
        // Go `FMSketchToProto(nil)` still returns a non-nil empty message.
        fm_sketch: Some(fm_sketch_to_proto(collector.fm_sketch.as_ref())),
        cmsketch: collector
            .cmsketch
            .as_ref()
            .map(|cms| encode_cmsketch_without_topn(Some(cms)))
            .transpose()?
            .flatten(),
        total_size: Some(collector.total_size),
    })
}

pub fn legacy_sample_collector_from_proto(
    proto: &LegacySampleCollectorProto,
) -> Result<LegacySampleCollector, CodecError> {
    let (cmsketch, top_n) = match &proto.cmsketch {
        Some(bytes) => decode_cmsketch_and_embedded_topn(bytes)?,
        None => (None, None),
    };
    Ok(LegacySampleCollector {
        fm_sketch: fm_sketch_from_proto(proto.fm_sketch.as_ref()),
        cmsketch,
        top_n,
        samples: proto
            .samples
            .iter()
            .filter(|bytes| bytes.len() <= MAX_SAMPLE_VALUE_LENGTH)
            .map(|bytes| LegacySampleItem {
                value: Datum::Bytes(bytes.clone()),
                handle: None,
                ordinal: 0,
            })
            .collect(),
        null_count: proto.null_count,
        count: proto.count,
        total_size: proto.total_size.unwrap_or(0),
        ..LegacySampleCollector::default()
    })
}

/// One `RecordSet.Next` result consumed by Go `SampleBuilder`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LegacyRecordChunk {
    pub field_count: usize,
    pub rows: Vec<Vec<Datum>>,
}

/// Go `RowToDatums`: return one datum per result field, ignoring any extra
/// physical row values and panicking when a declared field is absent.
#[must_use]
pub fn legacy_row_to_datums(row: &[Datum], field_count: usize) -> Vec<Datum> {
    (0..field_count).map(|index| row[index].clone()).collect()
}

#[derive(Clone, Debug)]
pub struct LegacySampleBuilder {
    pub column_count: usize,
    pub max_sample_size: i64,
    pub max_fm_sketch_size: usize,
    pub cmsketch_depth: i32,
    pub cmsketch_width: i32,
    /// One entry per sampled column after an optional PK-handle column.
    pub collated_columns: Vec<bool>,
}

#[derive(Debug)]
pub enum LegacySampleBuilderError<E> {
    ZeroFields,
    PrimaryKey(DatumValueError),
    Encode(E),
}

impl<E: std::fmt::Display> std::fmt::Display for LegacySampleBuilderError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroFields => {
                formatter.write_str("collect column stats failed: record set has 0 field")
            }
            Self::PrimaryKey(error) => std::fmt::Display::fmt(error, formatter),
            Self::Encode(error) => std::fmt::Display::fmt(error, formatter),
        }
    }
}

impl<E> std::error::Error for LegacySampleBuilderError<E> where E: std::error::Error + 'static {}

impl LegacySampleBuilder {
    /// Go `CollectColumnStats` over dependency-free record chunks.
    /// `prepare` owns the non-NULL collation/table-codec substitution that Go
    /// performs before collection. `encode_value` is the later FM-sketch
    /// `codec.EncodeValue` call; it is deliberately separate from Datum
    /// `GetBytes` used by CMS, samples, and total size.
    pub fn collect_column_stats<I, E>(
        &self,
        chunks: I,
        mut primary_key_builder: Option<&mut SortedHistogramBuilder>,
        mut prepare: impl FnMut(usize, Datum, bool) -> Result<Datum, E>,
        mut encode_value: impl FnMut(&Datum) -> Result<Vec<u8>, E>,
    ) -> Result<Vec<LegacySampleCollector>, LegacySampleBuilderError<E>>
    where
        I: IntoIterator<Item = LegacyRecordChunk>,
    {
        let mut collectors: Vec<_> = (0..self.column_count)
            .map(|_| LegacySampleCollector {
                fm_sketch: Some(FmSketch::new(self.max_fm_sketch_size)),
                max_sample_size: self.max_sample_size,
                ..LegacySampleCollector::default()
            })
            .collect();
        if self.cmsketch_depth > 0 && self.cmsketch_width > 0 {
            for collector in &mut collectors {
                collector.cmsketch = Some(CmsSketch::new(
                    self.cmsketch_depth as u32,
                    self.cmsketch_width as u32,
                ));
            }
        }

        for chunk in chunks {
            if chunk.rows.is_empty() {
                return Ok(collectors);
            }
            if chunk.field_count == 0 {
                return Err(LegacySampleBuilderError::ZeroFields);
            }
            for row in chunk.rows {
                let mut datums = legacy_row_to_datums(&row, chunk.field_count);
                if let Some(builder) = primary_key_builder.as_deref_mut() {
                    builder
                        .iterate(datums[0].clone())
                        .map_err(LegacySampleBuilderError::PrimaryKey)?;
                    datums.remove(0);
                }
                for (index, datum) in datums.into_iter().enumerate() {
                    let collated = self.collated_columns[index];
                    // Go indexes Collators before testing NULL and invokes
                    // tablecodec only when both operands of its `&&` are
                    // true. Keep the callback behind that exact gate: it is
                    // allowed to fail and therefore cannot be a no-op call.
                    let datum = if collated && datum != Datum::Null {
                        prepare(index, datum, collated).map_err(LegacySampleBuilderError::Encode)?
                    } else {
                        datum
                    };
                    collectors[index]
                        .collect(datum, |value| encode_value(value))
                        .map_err(LegacySampleBuilderError::Encode)?;
                }
            }
        }
        Ok(collectors)
    }
}
