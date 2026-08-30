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

//! Partition TopN merging from `pkg/statistics/handle/globalstats/topn.go`.

use std::collections::HashMap;
use std::fmt;

use chrono::TimeZone;
use tidb_datatype::{Collation, DatumValueError, FieldType};

use crate::cmsketch::{get_merged_topn_from_sorted_slice, CmsSketch, MergeError, TopN, TopNEntry};
use crate::histogram::{
    merge_partition_histograms, Histogram, HistogramMergeError, PartitionMergeOptions,
    TopNMergeEntry,
};
use crate::{merge_fm_sketch, DatumMapCache, FmSketch};

/// Which pinned Go global-statistics worker owns the merge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GlobalStatsMergeMode {
    /// `blockingMergePartitionStats2GlobalStats`.
    Blocking,
    /// `AsyncMergePartitionStats2GlobalStats`.
    Async,
}

/// One partition's payload for a single column or index.
#[derive(Clone, Debug)]
pub struct PartitionStatsItem {
    /// Partition histogram.
    pub histogram: Histogram,
    /// Partition CMSketch.
    pub cmsketch: Option<CmsSketch>,
    /// Partition TopN.
    pub topn: Option<TopN>,
    /// Partition FM sketch.
    pub fm_sketch: Option<FmSketch>,
}

/// The merged payload Go writes for one global column or index.
#[derive(Clone, Debug)]
pub struct GlobalStatsItem {
    /// Global histogram, absent only when no partition item was supplied.
    pub histogram: Option<Histogram>,
    /// Merged CMSketch.
    pub cmsketch: Option<CmsSketch>,
    /// Global TopN.
    pub topn: Option<TopN>,
}

/// A failure while an encoded TopN candidate is decoded or removed from a
/// partition histogram.
#[derive(Debug)]
pub enum GlobalStatsMergeError {
    /// The encoded TopN value could not be decoded with its column type.
    Decode(tidb_codec::CodecError),
    /// The decoded value could not be compared with histogram bounds.
    Datum(DatumValueError),
    /// Partition CMSketch dimensions differ.
    Cms(MergeError),
    /// Partition histograms could not be merged.
    Histogram(HistogramMergeError),
    /// Go cannot derive global NDV without any partition FM sketch.
    MissingFmSketch,
}

impl fmt::Display for GlobalStatsMergeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(error) => error.fmt(formatter),
            Self::Datum(error) => error.fmt(formatter),
            Self::Cms(error) => error.fmt(formatter),
            Self::Histogram(error) => error.fmt(formatter),
            Self::MissingFmSketch => formatter.write_str("partition FM sketch is missing"),
        }
    }
}

impl std::error::Error for GlobalStatsMergeError {}

impl From<tidb_codec::CodecError> for GlobalStatsMergeError {
    fn from(error: tidb_codec::CodecError) -> Self {
        Self::Decode(error)
    }
}

impl From<DatumValueError> for GlobalStatsMergeError {
    fn from(error: DatumValueError) -> Self {
        Self::Datum(error)
    }
}

impl From<MergeError> for GlobalStatsMergeError {
    fn from(error: MergeError) -> Self {
        Self::Cms(error)
    }
}

impl From<HistogramMergeError> for GlobalStatsMergeError {
    fn from(error: HistogramMergeError) -> Self {
        Self::Histogram(error)
    }
}

/// Go `MergePartTopN2GlobalTopN`.
///
/// Equal encoded values are combined across partition TopNs. On the first
/// occurrence of a candidate, every partition that does not contain it in
/// TopN contributes its histogram equality estimate, and that contribution is
/// removed from the histogram before the later global-histogram merge.
pub fn merge_partition_topn<TZ: TimeZone>(
    timezone: Option<&TZ>,
    analyze_version: i64,
    topns: &[Option<&TopN>],
    requested_topn: u32,
    mut histograms: Vec<Histogram>,
    field_type: &FieldType,
    is_index: bool,
) -> Result<(Option<TopN>, Vec<TopNEntry>, Vec<Histogram>), GlobalStatsMergeError> {
    if crate::cmsketch::check_empty_topns(topns) {
        return Ok((None, Vec::new(), histograms));
    }

    let mut counts = HashMap::<Vec<u8>, f64>::new();
    let mut datum_cache = DatumMapCache::new();
    for (partition_index, topn) in topns.iter().enumerate() {
        let Some(topn) = topn else {
            continue;
        };
        if topn.total_count() == 0 {
            continue;
        }
        for entry in topn.entries() {
            let existed = counts.contains_key(&entry.encoded);
            *counts.entry(entry.encoded.clone()).or_default() += entry.count as f64;
            if existed {
                continue;
            }

            for other_index in 0..topns.len() {
                if (other_index == partition_index && analyze_version >= 2)
                    || topns[other_index].is_some_and(|topn| topn.find(&entry.encoded).is_some())
                {
                    continue;
                }
                let datum = match datum_cache.get(&entry.encoded) {
                    Some(datum) => datum,
                    None => datum_cache.put_encoded(
                        &entry.encoded,
                        &entry.encoded,
                        field_type.code().mysql_type(),
                        is_index,
                        timezone,
                    )?,
                };
                let (count, _) = histograms[other_index].equal_row_count(
                    &datum,
                    is_index,
                    field_type.collation(),
                );
                if count != 0.0 {
                    *counts.entry(entry.encoded.clone()).or_default() += count;
                    histograms[other_index].binary_search_remove_value(
                        &datum,
                        count as i64,
                        field_type.collation(),
                    )?;
                }
            }
        }
    }

    if counts.is_empty() {
        return Ok((None, Vec::new(), histograms));
    }
    let ranked = counts
        .into_iter()
        .map(|(encoded, count)| TopNEntry {
            encoded,
            count: count as u64,
        })
        .collect();
    let (global, remainder) = get_merged_topn_from_sorted_slice(ranked, requested_topn);
    Ok((global, remainder, histograms))
}

/// Pinned Go's per-column/per-index merge after partition payload collection.
pub fn merge_partition_stats_item<TZ: TimeZone>(
    timezone: Option<&TZ>,
    analyze_version: i64,
    requested_topn: u32,
    expected_buckets: usize,
    total_count: i64,
    field_type: &FieldType,
    is_index: bool,
    mode: GlobalStatsMergeMode,
    partitions: Vec<PartitionStatsItem>,
) -> Result<GlobalStatsItem, GlobalStatsMergeError> {
    if partitions.is_empty() {
        return Ok(GlobalStatsItem {
            histogram: None,
            cmsketch: None,
            topn: None,
        });
    }

    let mut fm_sketch = None;
    for partition in &partitions {
        match (&mut fm_sketch, &partition.fm_sketch) {
            (None, Some(source)) => fm_sketch = Some(source.clone()),
            (Some(destination), Some(source)) => {
                merge_fm_sketch(Some(destination), Some(source));
            }
            _ => {}
        }
    }
    let global_ndv = fm_sketch
        .as_ref()
        .ok_or(GlobalStatsMergeError::MissingFmSketch)?
        .ndv()
        .min(total_count);

    let mut cmsketch = None;
    match mode {
        GlobalStatsMergeMode::Blocking => {
            cmsketch = partitions[0].cmsketch.clone();
            if let Some(destination) = &mut cmsketch {
                for partition in &partitions[1..] {
                    if let Some(source) = &partition.cmsketch {
                        destination.merge(source)?;
                    }
                }
            }
        }
        GlobalStatsMergeMode::Async => {
            for partition in &partitions {
                match (&mut cmsketch, &partition.cmsketch) {
                    (None, Some(source)) => cmsketch = Some(source.clone()),
                    (Some(destination), Some(source)) => destination.merge(source)?,
                    _ => {}
                }
            }
        }
    }

    let topns = partitions
        .iter()
        .map(|partition| partition.topn.as_ref())
        .collect::<Vec<_>>();
    let histograms = partitions
        .iter()
        .map(|partition| partition.histogram.clone())
        .collect();
    let (topn, remainder, histograms) = merge_partition_topn(
        timezone,
        analyze_version,
        &topns,
        requested_topn,
        histograms,
        field_type,
        is_index,
    )?;
    let mut datum_cache = DatumMapCache::new();
    let popped_topn = remainder
        .iter()
        .map(|entry| {
            let value = datum_cache.put_encoded(
                &entry.encoded,
                &entry.encoded,
                field_type.code().mysql_type(),
                is_index,
                timezone,
            )?;
            Ok(TopNMergeEntry {
                value,
                count: entry.count,
            })
        })
        .collect::<Result<Vec<_>, tidb_codec::CodecError>>()?;
    let mut histogram = merge_partition_histograms(
        &histograms,
        &popped_topn,
        PartitionMergeOptions {
            expected_buckets,
            is_index,
            analyze_version,
        },
        if is_index {
            Collation::Binary
        } else {
            field_type.collation()
        },
    )?;
    if let Some(histogram) = &mut histogram {
        histogram.ndv = global_ndv;
        for bucket in &mut histogram.buckets {
            bucket.ndv = 0;
        }
    }
    Ok(GlobalStatsItem {
        histogram,
        cmsketch,
        topn,
    })
}
