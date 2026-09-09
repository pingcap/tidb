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

//! Go's stats-v2 independent index analyze stream.
//!
//! This is the statistics half of `analyzeIndexProcessor` and
//! `AnalyzeIndexExec.buildStatsFromResult`. The storage owner cuts the encoded
//! index columns from each ordered key and feeds them here. Keeping that
//! encoded boundary is essential for prefix and virtual-generated global
//! indexes: rebuilding their values from sampled table rows is not equivalent
//! to scanning the logical global-index keyspace.

use tidb_datatype::{Collation, Datum, DatumValueError};

use crate::histogram::{merge_histograms, TopNMergeEntry};
use crate::{
    hash_bytes, insert_encoded_value, merge_topn_and_update_cmsketch, sort_topn_meta, CmsSketch,
    FmSketch, Histogram, MergeError, SortedHistogramBuilder, TopN, TopNEntry, MAX_SKETCH_SIZE,
};

/// Go's default independent-index CMS dimensions from `defaultAnalyzeOptions`.
pub const DEFAULT_INDEX_CMS_DEPTH: u32 = 5;
pub const DEFAULT_INDEX_CMS_WIDTH: u32 = 2048;

/// The statistics produced by one complete ordered index-key stream.
#[derive(Clone, Debug)]
pub struct IndependentIndexStatistics {
    pub histogram: Histogram,
    pub topn: TopN,
    pub fm_sketch: FmSketch,
    pub count: i64,
}

/// One TiKV region's `AnalyzeIndexResp` payload before TiDB-side merging.
#[derive(Clone, Debug)]
pub struct IndependentIndexFragment {
    pub histogram: Histogram,
    pub topn: TopN,
    pub cms: CmsSketch,
    pub fm_sketch: FmSketch,
    pub null_count: i64,
}

/// Failure while reducing regional independent-index responses.
#[derive(Debug)]
pub enum IndependentIndexMergeError {
    Histogram(DatumValueError),
    Cms(MergeError),
}

impl std::fmt::Display for IndependentIndexMergeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Histogram(error) => std::fmt::Display::fmt(error, formatter),
            Self::Cms(error) => std::fmt::Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for IndependentIndexMergeError {}

/// Incrementally consumes one logical index's ordered encoded entries.
pub struct IndependentIndexAnalyze {
    column_count: usize,
    bucket_count: usize,
    topn_count: usize,
    histogram: SortedHistogramBuilder,
    cms: CmsSketch,
    fm_sketch: FmSketch,
    current_topn: Option<TopNEntry>,
    topn_values: Vec<TopNEntry>,
    null_count: i64,
}

impl IndependentIndexAnalyze {
    /// Opens the stats-v2 builder used by Go's independent index task.
    #[must_use]
    pub fn new(index_id: i64, column_count: usize, bucket_count: usize, topn_count: usize) -> Self {
        Self {
            column_count,
            bucket_count,
            topn_count,
            histogram: SortedHistogramBuilder::new(bucket_count as i64, index_id, 2),
            cms: CmsSketch::new(DEFAULT_INDEX_CMS_DEPTH, DEFAULT_INDEX_CMS_WIDTH),
            fm_sketch: FmSketch::new(MAX_SKETCH_SIZE),
            current_topn: None,
            topn_values: Vec::new(),
            null_count: 0,
        }
    }

    /// Adds the encoded index columns cut from one key.
    ///
    /// A one-column NULL belongs to Go's separate null range: it contributes
    /// only `Histogram.NullCount`. Multi-column tuples, including tuples with
    /// NULL parts, stay in the ordinary full range and have null count zero.
    pub fn push(&mut self, encoded_columns: &[Vec<u8>]) -> Result<(), DatumValueError> {
        assert_eq!(
            encoded_columns.len(),
            self.column_count,
            "independent index entry has the planned column count"
        );
        if self.column_count == 1 && encoded_columns[0].as_slice() == [tidb_codec::NIL_FLAG] {
            self.null_count = self.null_count.wrapping_add(1);
            return Ok(());
        }

        let total_length = encoded_columns.iter().map(Vec::len).sum();
        let mut tuple = Vec::with_capacity(total_length);
        for column in encoded_columns {
            tuple.extend_from_slice(column);
            // Go inserts every progressively longer index prefix into CMS.
            self.cms.insert_bytes(&tuple);
        }
        insert_encoded_value(&mut self.fm_sketch, &tuple);

        match self.current_topn.as_mut() {
            Some(current) if current.encoded == tuple => {
                current.count = current.count.wrapping_add(1);
            }
            Some(_) => {
                self.topn_values.push(
                    self.current_topn
                        .replace(TopNEntry {
                            encoded: tuple.clone(),
                            count: 1,
                        })
                        .expect("the current TopN run exists"),
                );
            }
            None => {
                self.current_topn = Some(TopNEntry {
                    encoded: tuple.clone(),
                    count: 1,
                });
            }
        }
        self.histogram.iterate(Datum::Bytes(tuple))
    }

    /// Finishes the region-local coprocessor response.
    #[must_use]
    pub fn finish_fragment(mut self) -> IndependentIndexFragment {
        if let Some(current) = self.current_topn.take() {
            self.topn_values.push(current);
        }
        sort_topn_meta(&mut self.topn_values);
        self.topn_values.truncate(self.topn_count);

        for entry in &self.topn_values {
            self.cms.sub_hashed(hash_bytes(&entry.encoded), entry.count);
        }

        let mut topn = TopN::new(self.topn_values.len());
        for entry in self.topn_values {
            topn.append(&entry.encoded, entry.count);
        }
        topn.sort();
        IndependentIndexFragment {
            histogram: self.histogram.histogram().clone(),
            topn,
            cms: self.cms,
            fm_sketch: self.fm_sketch,
            null_count: self.null_count,
        }
    }

    /// Convenience for a one-region response followed by TiDB-side merging.
    pub fn finish(self) -> Result<IndependentIndexStatistics, IndependentIndexMergeError> {
        let index_id = self.histogram.histogram().id;
        let bucket_count = self.bucket_count;
        let topn_count = self.topn_count;
        let fragment = self.finish_fragment();
        merge_independent_index_fragments(index_id, bucket_count, topn_count, [fragment])
    }
}

/// Go `updateIndexResult` plus `buildStatsFromResult`'s final normalization.
pub fn merge_independent_index_fragments(
    index_id: i64,
    bucket_count: usize,
    topn_count: usize,
    fragments: impl IntoIterator<Item = IndependentIndexFragment>,
) -> Result<IndependentIndexStatistics, IndependentIndexMergeError> {
    let mut histogram = Histogram {
        id: index_id,
        ..Histogram::default()
    };
    let mut cms = CmsSketch::new(DEFAULT_INDEX_CMS_DEPTH, DEFAULT_INDEX_CMS_WIDTH);
    let mut fm_sketch = FmSketch::new(MAX_SKETCH_SIZE);
    let mut topn = TopN::new(topn_count);
    let mut null_count = 0_i64;
    for fragment in fragments {
        histogram = merge_histograms(
            histogram,
            fragment.histogram,
            bucket_count,
            2,
            Collation::Binary,
        )
        .map_err(IndependentIndexMergeError::Histogram)?;
        cms.merge(&fragment.cms)
            .map_err(IndependentIndexMergeError::Cms)?;
        merge_topn_and_update_cmsketch(&mut topn, &fragment.topn, &mut cms, topn_count as u32);
        fm_sketch.merge(&fragment.fm_sketch);
        null_count = null_count.wrapping_add(fragment.null_count);
    }

    let mut remove = topn
        .entries()
        .iter()
        .map(|entry| TopNMergeEntry {
            value: Datum::Bytes(entry.encoded.clone()),
            count: entry.count,
        })
        .collect::<Vec<_>>();
    remove.sort_by(|left, right| match (&left.value, &right.value) {
        (Datum::Bytes(left), Datum::Bytes(right)) => left.cmp(right),
        _ => unreachable!("independent index TopN values are encoded bytes"),
    });

    histogram.null_count = null_count;
    if !remove.is_empty() {
        histogram
            .remove_values(&remove, Collation::Binary)
            .map_err(IndependentIndexMergeError::Histogram)?;
    }
    histogram.standardize_for_v2_analyze_index();
    cms.calc_default_value_for_analyze(histogram.ndv.max(0) as u64);
    let count = histogram
        .buckets
        .last()
        .map_or(0, |bucket| bucket.count)
        .wrapping_add(topn.total_count() as i64)
        .wrapping_add(null_count);
    Ok(IndependentIndexStatistics {
        histogram,
        topn,
        fm_sketch,
        count,
    })
}

#[cfg(test)]
mod tests {
    use tidb_codec::Encoder;

    use super::*;

    fn encoded(value: Datum) -> Vec<u8> {
        Encoder::new(false)
            .encode_key(&[value])
            .expect("datum encodes")
    }

    #[test]
    fn single_column_nulls_are_counted_outside_the_index_distribution() {
        let mut analyze = IndependentIndexAnalyze::new(7, 1, 8, 1);
        analyze.push(&[encoded(Datum::Null)]).unwrap();
        analyze.push(&[encoded(Datum::Int(1))]).unwrap();
        analyze.push(&[encoded(Datum::Int(1))]).unwrap();
        analyze.push(&[encoded(Datum::Int(2))]).unwrap();

        let built = analyze.finish().unwrap();
        assert_eq!(built.count, 4);
        assert_eq!(built.histogram.null_count, 1);
        assert_eq!(built.topn.num(), 1);
        assert_eq!(built.topn.entries()[0].count, 2);
        assert_eq!(built.fm_sketch.ndv(), 2);
        assert_eq!(built.histogram.buckets.last().unwrap().count, 1);
    }

    #[test]
    fn multi_column_null_tuple_remains_an_ordinary_value() {
        let mut analyze = IndependentIndexAnalyze::new(9, 2, 8, 0);
        analyze
            .push(&[encoded(Datum::Null), encoded(Datum::Int(1))])
            .unwrap();

        let built = analyze.finish().unwrap();
        assert_eq!(built.count, 1);
        assert_eq!(built.histogram.null_count, 0);
        assert_eq!(built.histogram.buckets.last().unwrap().count, 1);
        assert_eq!(built.fm_sketch.ndv(), 1);
    }

    #[test]
    fn regional_fragments_merge_before_final_topn_removal() {
        let one = encoded(Datum::Int(1));
        let two = encoded(Datum::Int(2));
        let mut left = IndependentIndexAnalyze::new(11, 1, 2, 1);
        left.push(std::slice::from_ref(&one)).unwrap();
        left.push(std::slice::from_ref(&two)).unwrap();
        let mut right = IndependentIndexAnalyze::new(11, 1, 2, 1);
        right.push(std::slice::from_ref(&two)).unwrap();
        right.push(std::slice::from_ref(&two)).unwrap();

        let built = merge_independent_index_fragments(
            11,
            2,
            1,
            [left.finish_fragment(), right.finish_fragment()],
        )
        .unwrap();

        assert_eq!(built.count, 4);
        assert_eq!(built.topn.entries()[0].encoded, two);
        // Go merges only each region's retained TopN metadata. The left
        // region's tied value `2` was spilled to CMS, so the final TopN keeps
        // the right region's exact count rather than reconstructing three.
        assert_eq!(built.topn.entries()[0].count, 2);
        assert_eq!(built.histogram.buckets.last().unwrap().count, 2);
        assert_eq!(built.histogram.ndv, 2);
    }
}
