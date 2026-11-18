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


//! Go `AnalyzeIndexExec`'s TiDB half: `updateIndexResult`, which merges each
//! store response of an `AnalyzeIndexReq` into the running histogram,
//! CMSketch, TopN and FMSketch, and the normalization that ends
//! `buildStatsFromResult`. The index keys are read and summarized by the
//! store's `analyzeIndexProcessor`; TiDB only merges what it sends back.

use tidb_charset::Collation;
use tidb_datatype::{Datum, DatumValueError};

use crate::histogram::{merge_histograms, TopNMergeEntry};
use crate::{merge_topn_and_update_cmsketch, CmsSketch, FmSketch, Histogram, MergeError, TopN, MAX_SKETCH_SIZE};

/// Go's default `AnalyzeOptCMSketchDepth` / `AnalyzeOptCMSketchWidth`.
pub const DEFAULT_INDEX_CMS_DEPTH: u32 = 5;
pub const DEFAULT_INDEX_CMS_WIDTH: u32 = 2048;

/// The statistics `buildStatsFromResult` returns for one index.
#[derive(Clone, Debug)]
pub struct AnalyzeIndexStatistics {
    pub histogram: Histogram,
    pub cms: Option<CmsSketch>,
    pub topn: Option<TopN>,
    pub fm_sketch: FmSketch,
}

/// Failure while merging the store responses.
#[derive(Debug)]
pub enum AnalyzeIndexMergeError {
    Histogram(DatumValueError),
    Cms(MergeError),
}

impl std::fmt::Display for AnalyzeIndexMergeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Histogram(error) => std::fmt::Display::fmt(error, formatter),
            Self::Cms(error) => std::fmt::Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for AnalyzeIndexMergeError {}

/// The state `buildStatsFromResult` threads through `updateIndexResult`.
pub struct AnalyzeIndexResult {
    histogram: Histogram,
    cms: Option<CmsSketch>,
    topn: Option<TopN>,
    fm_sketch: FmSketch,
    num_buckets: usize,
    num_topn: u32,
}

impl AnalyzeIndexResult {
    /// Go `buildStatsFromResult`'s start: an empty histogram, a CMSketch of
    /// the requested dimensions and a TopN only when `need_cms` (the
    /// distribution request, not the NULL-count one), and an FMSketch.
    #[must_use]
    pub fn new(need_cms: bool, cms_depth: u32, cms_width: u32, num_buckets: usize, num_topn: u32) -> Self {
        Self {
            histogram: Histogram::default(),
            cms: need_cms.then(|| CmsSketch::new(cms_depth, cms_width)),
            topn: need_cms.then(|| TopN::new(num_topn as usize)),
            fm_sketch: FmSketch::new(MAX_SKETCH_SIZE),
            num_buckets,
            num_topn,
        }
    }

    /// Go `updateIndexResult` for one response: its histogram
    /// (`HistogramFromProto`), its CMSketch and TopN
    /// (`CMSketchAndTopNFromProto`, `None` when the response has none) and
    /// its FMSketch. A response without a CMSketch leaves the sketch and TopN
    /// untouched, as Go's "nil CMS in response" warning does.
    pub fn update(
        &mut self,
        histogram: Histogram,
        cms_and_topn: Option<(Option<CmsSketch>, Option<TopN>)>,
        fm_sketch: Option<FmSketch>,
    ) -> Result<(), AnalyzeIndexMergeError> {
        let current = std::mem::take(&mut self.histogram);
        self.histogram = merge_histograms(current, histogram, self.num_buckets, 2, Collation::Binary)
            .map_err(AnalyzeIndexMergeError::Histogram)?;
        if let (Some(cms), Some(topn), Some((response_cms, response_topn))) =
            (self.cms.as_mut(), self.topn.as_mut(), cms_and_topn)
        {
            if let Some(response_cms) = response_cms {
                cms.merge(&response_cms).map_err(AnalyzeIndexMergeError::Cms)?;
            }
            let response_topn = response_topn.unwrap_or_else(|| TopN::new(0));
            merge_topn_and_update_cmsketch(topn, &response_topn, cms, self.num_topn);
        }
        if let Some(fm_sketch) = fm_sketch {
            self.fm_sketch.merge(&fm_sketch);
        }
        Ok(())
    }

    /// Go `buildStatsFromResult`'s end for stats version 2: the TopN values
    /// leave the histogram, which is then standardized, and the CMSketch's
    /// default value is derived from the histogram's NDV.
    pub fn finish(mut self) -> Result<AnalyzeIndexStatistics, AnalyzeIndexMergeError> {
        if let Some(topn) = self.topn.as_ref().filter(|topn| topn.total_count() > 0) {
            let mut remove = topn
                .entries()
                .iter()
                .map(|entry| TopNMergeEntry { value: Datum::Bytes(entry.encoded.clone()), count: entry.count })
                .collect::<Vec<_>>();
            remove.sort_by(|left, right| match (&left.value, &right.value) {
                (Datum::Bytes(left), Datum::Bytes(right)) => left.cmp(right),
                _ => unreachable!("index TopN values are encoded bytes"),
            });
            self.histogram
                .remove_values(&remove, Collation::Binary)
                .map_err(AnalyzeIndexMergeError::Histogram)?;
        }
        self.histogram.standardize_for_v2_analyze_index();
        if let Some(cms) = self.cms.as_mut() {
            cms.calc_default_value_for_analyze(self.histogram.ndv.max(0) as u64);
        }
        Ok(AnalyzeIndexStatistics {
            histogram: self.histogram,
            cms: self.cms,
            topn: self.topn,
            fm_sketch: self.fm_sketch,
        })
    }
}

#[cfg(test)]
mod tests {
    use tidb_codec::Encoder;
    use tidb_datatype::{FieldType, FieldTypeCode};

    use super::*;
    use crate::{hash_bytes, sort_topn_meta, SortedHistogramBuilder, TopNEntry};

    fn encoded(value: i64) -> Vec<u8> {
        Encoder::new(false).encode_key(&[Datum::Int(value)]).expect("datum encodes")
    }

    /// One store's `AnalyzeIndexResp` for a region holding `values` in key
    /// order, as Go unistore's `analyzeIndexProcessor` builds it.
    fn region(values: &[i64], num_buckets: i64, num_topn: usize) -> (Histogram, Option<(Option<CmsSketch>, Option<TopN>)>, Option<FmSketch>) {
        let mut builder = SortedHistogramBuilder::new(num_buckets, 0, &FieldType::new(FieldTypeCode::Blob), 2);
        let mut cms = CmsSketch::new(DEFAULT_INDEX_CMS_DEPTH, DEFAULT_INDEX_CMS_WIDTH);
        let mut fm_sketch = FmSketch::new(MAX_SKETCH_SIZE);
        let mut runs: Vec<TopNEntry> = Vec::new();
        for &value in values {
            let bytes = encoded(value);
            cms.insert_bytes(&bytes);
            crate::insert_encoded_value(&mut fm_sketch, &tidb_codec::encode_value(&[Datum::Bytes(bytes.clone())]).unwrap());
            match runs.last_mut() {
                Some(last) if last.encoded == bytes => last.count += 1,
                _ => runs.push(TopNEntry { encoded: bytes.clone(), count: 1 }),
            }
            builder.iterate(Datum::Bytes(bytes)).unwrap();
        }
        sort_topn_meta(&mut runs);
        runs.truncate(num_topn);
        let mut topn = TopN::new(runs.len());
        for entry in &runs {
            cms.sub_hashed(hash_bytes(&entry.encoded), entry.count);
            topn.append(&entry.encoded, entry.count);
        }
        topn.sort();
        (builder.histogram().clone(), Some((Some(cms), Some(topn))), Some(fm_sketch))
    }

    /// Go `updateIndexResult` merges only each region's retained TopN: the
    /// left region's tied value `2` was left in its CMSketch, so the final
    /// TopN keeps the right region's exact count rather than three.
    #[test]
    fn regional_responses_merge_before_final_topn_removal() {
        let mut result = AnalyzeIndexResult::new(true, DEFAULT_INDEX_CMS_DEPTH, DEFAULT_INDEX_CMS_WIDTH, 2, 1);
        for values in [&[1, 2][..], &[2, 2][..]] {
            let (histogram, cms_and_topn, fm_sketch) = region(values, 2, 1);
            result.update(histogram, cms_and_topn, fm_sketch).unwrap();
        }
        let built = result.finish().unwrap();
        let topn = built.topn.unwrap();
        assert_eq!(topn.entries()[0].encoded, encoded(2));
        assert_eq!(topn.entries()[0].count, 2);
        assert_eq!(built.histogram.buckets.last().unwrap().count, 2);
        assert_eq!(built.histogram.ndv, 2);
        assert_eq!(built.fm_sketch.ndv(), 2);
    }

    /// Go's NULL-count request: no CMSketch or TopN, and only the merged
    /// histogram's row count matters.
    #[test]
    fn null_range_result_keeps_only_the_histogram() {
        let mut result = AnalyzeIndexResult::new(false, 0, 0, 2, 0);
        let (histogram, _, _) = region(&[3, 3, 3], 2, 0);
        result.update(histogram, None, None).unwrap();
        let built = result.finish().unwrap();
        assert!(built.cms.is_none() && built.topn.is_none());
        assert_eq!(built.histogram.buckets.last().unwrap().count, 3);
    }
}
