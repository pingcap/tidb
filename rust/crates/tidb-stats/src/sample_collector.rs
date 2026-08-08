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
use tidb_util::fastrand::{uint32_n, uint64_n};

use crate::{
    decode_cmsketch_and_embedded_topn, encode_cmsketch_without_topn, fm_sketch_from_proto,
    fm_sketch_to_proto, hash_bytes, CmsSketch, CodecError, FmSketch, FmSketchProto, TopN,
    MAX_SAMPLE_VALUE_LENGTH,
};

#[derive(Clone, Debug, PartialEq)]
pub struct LegacySampleItem {
    pub value: Datum,
    pub encoded: Vec<u8>,
    pub ordinal: i64,
}

/// Stable Go `sortSampleItems`, including its first comparison error.
pub fn sort_legacy_sample_items(items: &mut [LegacySampleItem]) -> Result<(), DatumValueError> {
    let mut error = None;
    items.sort_by(
        |left, right| match left.value.compare(&right.value, Collation::Binary) {
            Ok(ordering) => {
                error = None;
                ordering
            }
            Err(found) => {
                error = Some(found);
                Ordering::Less
            }
        },
    );
    error.map_or(Ok(()), Err)
}

#[derive(Clone, Debug)]
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

impl Default for LegacySampleCollector {
    fn default() -> Self {
        Self {
            fm_sketch: None,
            cmsketch: None,
            top_n: None,
            samples: Vec::new(),
            seen_values: 0,
            null_count: 0,
            count: 0,
            max_sample_size: 0,
            total_size: 0,
            memory_size: 0,
            is_merger: false,
        }
    }
}

impl LegacySampleCollector {
    /// Go `Destroy`, retaining allocated vector capacity while clearing refs.
    pub fn destroy(&mut self) {
        self.fm_sketch = None;
        self.cmsketch = None;
        self.top_n = None;
        self.samples.clear();
        self.seen_values = 0;
        self.null_count = 0;
        self.count = 0;
        self.max_sample_size = 0;
        self.total_size = 0;
        self.memory_size = 0;
        self.is_merger = false;
    }

    /// Offers an encoded datum under Go's reservoir algorithm.
    pub fn collect(&mut self, value: Datum, encoded: Vec<u8>) {
        if !self.is_merger {
            if value == Datum::Null {
                self.null_count = self.null_count.wrapping_add(1);
                return;
            }
            self.count = self.count.wrapping_add(1);
            if let Some(fm) = &mut self.fm_sketch {
                fm.insert_hash(hash_bytes(&encoded).h1);
            }
            if let Some(cms) = &mut self.cmsketch {
                cms.insert_bytes(&encoded);
            }
            self.total_size = self.total_size.wrapping_add(encoded.len() as i64 - 1);
        }
        self.seen_values = self.seen_values.wrapping_add(1);
        if self.samples.len() < self.max_sample_size.max(0) as usize {
            self.samples.push(LegacySampleItem {
                value,
                encoded,
                ordinal: 0,
            });
        } else if self.max_sample_size > 0
            && uint64_n(self.seen_values as u64) < self.max_sample_size as u64
        {
            let index = uint32_n(self.max_sample_size as u32) as usize;
            self.samples.remove(index);
            self.samples.push(LegacySampleItem {
                value,
                encoded,
                ordinal: 0,
            });
        }
    }

    /// Go `MergeSampleCollector`; incompatible CMS shapes are logged and the
    /// merge continues, so this pure port deliberately ignores that error.
    pub fn merge(&mut self, source: &Self) {
        self.null_count = self.null_count.wrapping_add(source.null_count);
        self.count = self.count.wrapping_add(source.count);
        self.total_size = self.total_size.wrapping_add(source.total_size);
        if let (Some(destination), Some(source)) = (&mut self.fm_sketch, &source.fm_sketch) {
            destination.merge(source);
        }
        if let Some(source) = &source.cmsketch {
            let destination = self
                .cmsketch
                .as_mut()
                .expect("CMS source cannot merge into nil destination");
            let _ = destination.merge(source);
        }
        for item in &source.samples {
            self.collect(item.value.clone(), item.encoded.clone());
        }
    }

    pub fn calculate_total_size(&mut self) {
        self.total_size = self.samples.iter().fold(0_i64, |total, item| {
            total.wrapping_add(item.encoded.len() as i64)
        });
    }

    /// Go `ExtractTopN` over already-decoded/re-encoded sample bytes.
    pub fn extract_topn<E>(
        &mut self,
        number: u32,
        mut normalize: impl FnMut(&[u8]) -> Result<Vec<u8>, E>,
    ) -> Result<(), E> {
        if number == 0 {
            return Ok(());
        }
        let mut frequencies = std::collections::HashMap::<Vec<u8>, u64>::new();
        for sample in &self.samples {
            *frequencies.entry(sample.encoded.clone()).or_default() += 1;
        }
        let mut candidates: Vec<_> = frequencies.into_iter().collect();
        candidates.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        candidates.truncate(number as usize);
        let cms = self
            .cmsketch
            .as_mut()
            .expect("ExtractTopN requires CMSketch");
        let mut top_n = TopN::new(candidates.len());
        for (encoded, _) in candidates {
            let hash = hash_bytes(&encoded);
            let count = cms.query_hashed(hash);
            let normalized = normalize(&encoded)?;
            cms.sub_hashed(hash, count);
            top_n.append(&normalized, count);
        }
        top_n.sort();
        self.top_n = Some(top_n);
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LegacySampleCollectorProto {
    pub samples: Vec<Vec<u8>>,
    pub null_count: i64,
    pub count: i64,
    pub fm_sketch: FmSketchProto,
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
            .map(|item| item.encoded.clone())
            .collect(),
        null_count: collector.null_count,
        count: collector.count,
        fm_sketch: fm_sketch_to_proto(collector.fm_sketch.as_ref()),
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
        fm_sketch: fm_sketch_from_proto(Some(&proto.fm_sketch)),
        cmsketch,
        top_n,
        samples: proto
            .samples
            .iter()
            .filter(|bytes| bytes.len() <= MAX_SAMPLE_VALUE_LENGTH)
            .map(|bytes| LegacySampleItem {
                value: Datum::Bytes(bytes.clone()),
                encoded: bytes.clone(),
                ordinal: 0,
            })
            .collect(),
        null_count: proto.null_count,
        count: proto.count,
        total_size: proto.total_size.unwrap_or(0),
        ..LegacySampleCollector::default()
    })
}
