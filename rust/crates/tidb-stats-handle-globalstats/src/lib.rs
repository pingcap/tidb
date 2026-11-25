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

pub mod global_stats;

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};

use chrono::TimeZone;
use hashbrown::{HashMap, hash_map::Entry};
use tidb_datatype::{DatumValueError, FieldType};
use tidb_charset::Collation;
use tidb_mysql::errors::SqlError;
use tidb_sqlkiller::SqlKiller;

use tidb_stats::cmsketch::{get_merged_topn_from_sorted_slice, CmsSketch, MergeError, TopN, TopNEntry};
use tidb_stats::datum_map_cache::top_n_meta_to_datum;
use tidb_stats::histogram::{
    merge_partition_histograms, Histogram, HistogramMergeError, PartitionMergeOptions,
    TopNMergeEntry,
};
use tidb_stats::fm_sketch_ndv;
use tidb_stats::merge_fm_sketch;
use tidb_stats::FmSketch;

/// Go `MaxPartitionMergeBatchSize`, the largest partition batch assigned to
/// one concurrent TopN merge task.
pub const MAX_PARTITION_MERGE_BATCH_SIZE: usize = 256;

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
    /// The statement was killed while merging partition TopNs.
    Killed(SqlError),
    /// Concurrent TopN worker failures joined by pinned Go's coordinator.
    Concurrent(String),
    /// The encoded TopN value could not be decoded with its column type.
    Decode(tidb_codec::CodecError),
    /// The decoded value could not be compared with histogram bounds.
    Datum(DatumValueError),
    /// Partition CMSketch dimensions differ.
    Cms(MergeError),
    /// Partition histograms could not be merged.
    Histogram(HistogramMergeError),
}

impl fmt::Display for GlobalStatsMergeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Killed(error) => error.message.fmt(formatter),
            Self::Concurrent(error) => error.fmt(formatter),
            Self::Decode(error) => error.fmt(formatter),
            Self::Datum(error) => error.fmt(formatter),
            Self::Cms(error) => error.fmt(formatter),
            Self::Histogram(error) => error.fmt(formatter),
        }
    }
}

fn handle_kill_signal(killer: &SqlKiller) -> Result<(), GlobalStatsMergeError> {
    killer.handle_signal().map_or(Ok(()), |error| {
        Err(GlobalStatsMergeError::Killed(error.to_sql_error()))
    })
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
pub fn merge_part_top_n_2_global_top_n<TZ: TimeZone>(
    timezone: Option<&TZ>,
    analyze_version: i64,
    topns: &[Option<&TopN>],
    requested_topn: u32,
    mut histograms: Vec<Histogram>,
    field_type: &FieldType,
    is_index: bool,
    killer: &SqlKiller,
) -> Result<(Option<TopN>, Vec<TopNEntry>, Vec<Histogram>), GlobalStatsMergeError> {
    let mut counts = HashMap::<&[u8], f64>::new();
    let mysql_type = field_type.code().mysql_type();
    for (partition_index, topn) in topns.iter().enumerate() {
        handle_kill_signal(killer)?;
        let Some(topn) = topn else {
            continue;
        };
        if topn.total_count() == 0 {
            continue;
        }
        for entry in topn.entries() {
            let key = entry.encoded.as_slice();
            let total = match counts.entry(key) {
                Entry::Occupied(mut total) => {
                    *total.get_mut() += entry.count as f64;
                    continue;
                }
                Entry::Vacant(total) => total.insert(entry.count as f64),
            };

            // Only the first occurrence scans the partition histograms.
            let mut datum = None;
            for other_index in 0..topns.len() {
                handle_kill_signal(killer)?;
                if (other_index == partition_index && analyze_version >= 2)
                    || topns[other_index].is_some_and(|topn| topn.find(&entry.encoded).is_some())
                {
                    continue;
                }
                let datum = match &mut datum {
                    Some(datum) => datum,
                    slot => slot.insert(top_n_meta_to_datum(key, mysql_type, is_index, timezone)?),
                };
                let (count, _) = histograms[other_index].equal_row_count(
                    datum,
                    is_index,
                    field_type.collation(),
                );
                if count != 0.0 {
                    *total += count;
                    histograms[other_index].binary_search_remove_value(
                        datum,
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
            encoded: encoded.to_vec(),
            count: count as u64,
        })
        .collect();
    let (global, remainder) = get_merged_topn_from_sorted_slice(ranked, requested_topn);
    Ok((global, remainder, histograms))
}

/// Go `MergeGlobalStatsTopNByConcurrency`.
///
/// The caller supplies the already bounded batch size, just as the exported
/// Go worker boundary does.
pub fn merge_global_stats_top_n_by_concurrency<TZ: TimeZone + Sync>(
    timezone: Option<&TZ>,
    analyze_version: i64,
    topns: &[Option<&TopN>],
    requested_topn: u32,
    histograms: Vec<Histogram>,
    field_type: &FieldType,
    is_index: bool,
    merge_concurrency: usize,
    merge_batch_size: usize,
    killer: &SqlKiller,
) -> Result<(Option<TopN>, Vec<TopNEntry>, Vec<Histogram>), GlobalStatsMergeError> {
    let worker_count = merge_concurrency.min(topns.len());
    let batch_size = merge_batch_size;
    let next_partition = AtomicUsize::new(0);
    // A worker keeps the candidate's count while probing the histograms.
    let counts = Mutex::new(HashMap::<&[u8], Arc<Mutex<f64>>>::new());
    let histograms = histograms.into_iter().map(Mutex::new).collect::<Vec<_>>();
    let mysql_type = field_type.code().mysql_type();

    std::thread::scope(|scope| -> Result<(), GlobalStatsMergeError> {
        let mut workers = Vec::with_capacity(worker_count);
        let (response_sender, response_receiver) = channel();
        for _ in 0..worker_count {
            let response_sender = response_sender.clone();
            let next_partition = &next_partition;
            let counts = &counts;
            let histograms = &histograms;
            workers.push(scope.spawn(tidb_cpuprofile::labels::inherit(move || {
                let response = (|| -> Result<(), GlobalStatsMergeError> {
                    loop {
                        let start = next_partition.fetch_add(batch_size, Ordering::Relaxed);
                        if start >= topns.len() {
                            return Ok(());
                        }
                        let end = (start + batch_size).min(topns.len());
                        for partition_index in start..end {
                            handle_kill_signal(killer)?;
                            let Some(topn) = topns[partition_index] else {
                                continue;
                            };
                            if topn.total_count() == 0 {
                                continue;
                            }
                            for entry in topn.entries() {
                                let key = entry.encoded.as_slice();
                                let total = {
                                    let mut counts = counts
                                        .lock()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                                    match counts.entry(key) {
                                        Entry::Occupied(total) => {
                                            let mut count = total
                                                .get()
                                                .lock()
                                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                                            *count += entry.count as f64;
                                            continue;
                                        }
                                        Entry::Vacant(candidate) => {
                                            let total = Arc::new(Mutex::new(entry.count as f64));
                                            candidate.insert(Arc::clone(&total));
                                            total
                                        }
                                    }
                                };

                                let mut datum = None;
                                for other_index in 0..topns.len() {
                                    handle_kill_signal(killer)?;
                                    if (other_index == partition_index && analyze_version >= 2)
                                        || topns[other_index]
                                            .is_some_and(|topn| topn.find(&entry.encoded).is_some())
                                    {
                                        continue;
                                    }
                                    let datum = match &mut datum {
                                        Some(datum) => datum,
                                        slot => slot.insert(top_n_meta_to_datum(
                                            key, mysql_type, is_index, timezone,
                                        )?),
                                    };
                                    let mut histogram = histograms[other_index]
                                        .lock()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                                    let (count, _) = histogram.equal_row_count(
                                        datum,
                                        is_index,
                                        field_type.collation(),
                                    );
                                    if count == 0.0 {
                                        continue;
                                    }
                                    histogram.binary_search_remove_value(
                                        datum,
                                        count as i64,
                                        field_type.collation(),
                                    )?;
                                    drop(histogram);
                                    let mut total = total
                                        .lock()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                                    *total += count;
                                }
                            }
                        }
                    }
                })();
                let _ = response_sender.send(response);
            })));
        }
        drop(response_sender);
        let errors = response_receiver
            .into_iter()
            .filter_map(Result::err)
            .map(|error| error.to_string())
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().expect("global TopN worker panicked");
        }
        if !errors.is_empty() {
            return Err(GlobalStatsMergeError::Concurrent(errors.join(",")));
        }
        Ok(())
    })?;

    let counts = counts
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let ranked = counts
        .into_iter()
        .map(|(encoded, count)| {
            let total = count
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            TopNEntry {
                encoded: encoded.to_vec(),
                count: *total as u64,
            }
        })
        .collect();
    let histograms = histograms
        .into_iter()
        .map(|histogram| {
            histogram
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        })
        .collect();
    let (global, remainder) = get_merged_topn_from_sorted_slice(ranked, requested_topn);
    Ok((global, remainder, histograms))
}

/// Pinned Go's per-column/per-index merge after partition payload collection.
pub fn merge_partition_stats_item<TZ: TimeZone + Sync>(
    timezone: Option<&TZ>,
    analyze_version: i64,
    requested_topn: u32,
    expected_buckets: usize,
    total_count: i64,
    field_type: &FieldType,
    is_index: bool,
    merge_concurrency: usize,
    partitions: Vec<PartitionStatsItem>,
    killer: &SqlKiller,
) -> Result<GlobalStatsItem, GlobalStatsMergeError> {
    if partitions.is_empty() {
        return Ok(GlobalStatsItem {
            histogram: None,
            cmsketch: None,
            topn: None,
        });
    }

    let global_ndv = merge_partition_fm_sketches(
        total_count,
        partitions
            .iter()
            .map(|partition| partition.fm_sketch.as_ref()),
    );
    let cmsketch = merge_partition_cmsketches(
        partitions
            .iter()
            .map(|partition| partition.cmsketch.as_ref()),
    )?;
    let topns = partitions
        .iter()
        .map(|partition| partition.topn.clone())
        .collect();
    let histograms = partitions
        .into_iter()
        .map(|partition| partition.histogram)
        .collect();
    merge_partition_histogram_topn(
        timezone,
        analyze_version,
        requested_topn,
        expected_buckets,
        field_type,
        is_index,
        merge_concurrency,
        global_ndv,
        cmsketch,
        histograms,
        topns,
        killer,
    )
}

/// Pinned Go FM-worker reduction, including nil receiver NDV zero.
pub fn merge_partition_fm_sketches<'a>(
    total_count: i64,
    sketches: impl IntoIterator<Item = Option<&'a FmSketch>>,
) -> i64 {
    let mut fm_sketch = None;
    for sketch in sketches {
        match (&mut fm_sketch, sketch) {
            (None, Some(source)) => fm_sketch = Some(source.clone()),
            (Some(destination), Some(source)) => {
                merge_fm_sketch(Some(destination), Some(source));
            }
            _ => {}
        }
    }
    fm_sketch_ndv(fm_sketch.as_ref()).min(total_count)
}

/// Go's CMS-worker reduction (`AsyncMergePartitionStats2GlobalStats`): it
/// adopts the first non-nil sketch it receives.
pub fn merge_partition_cmsketches<'a>(
    sketches: impl IntoIterator<Item = Option<&'a CmsSketch>>,
) -> Result<Option<CmsSketch>, GlobalStatsMergeError> {
    let mut cmsketch = None;
    for source in sketches {
        match (&mut cmsketch, source) {
            (None, Some(source)) => cmsketch = Some(source.clone()),
            (Some(destination), Some(source)) => destination.merge(source)?,
            _ => {}
        }
    }
    Ok(cmsketch)
}

/// Pinned Go histogram-and-TopN worker after FM and CMS phases finish.
#[allow(clippy::too_many_arguments)]
pub fn merge_partition_histogram_topn<TZ: TimeZone + Sync>(
    timezone: Option<&TZ>,
    analyze_version: i64,
    requested_topn: u32,
    expected_buckets: usize,
    field_type: &FieldType,
    is_index: bool,
    merge_concurrency: usize,
    global_ndv: i64,
    cmsketch: Option<CmsSketch>,
    histograms: Vec<Histogram>,
    topns: Vec<Option<TopN>>,
    killer: &SqlKiller,
) -> Result<GlobalStatsItem, GlobalStatsMergeError> {
    if histograms.is_empty() {
        return Ok(GlobalStatsItem {
            histogram: None,
            cmsketch,
            topn: None,
        });
    }
    let topns = topns.iter().map(Option::as_ref).collect::<Vec<_>>();
    let (topn, remainder, histograms) = if tidb_stats::cmsketch::check_empty_topns(&topns) {
        (None, Vec::new(), histograms)
    } else if merge_concurrency < 2 {
        merge_part_top_n_2_global_top_n(
            timezone,
            analyze_version,
            &topns,
            requested_topn,
            histograms,
            field_type,
            is_index,
            killer,
        )?
    } else {
        let batch_size = (topns.len() / merge_concurrency).clamp(1, MAX_PARTITION_MERGE_BATCH_SIZE);
        merge_global_stats_top_n_by_concurrency(
            timezone,
            analyze_version,
            &topns,
            requested_topn,
            histograms,
            field_type,
            is_index,
            merge_concurrency,
            batch_size,
            killer,
        )?
    };
    let popped_topn = remainder
        .iter()
        .map(|entry| {
            let value = top_n_meta_to_datum(
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

/// Go globalstats `isLocalUnique`: a global index cannot prove that retained
/// statistics from different partitions describe disjoint value sets.
#[must_use]
pub fn is_local_unique(table: &tidb_model::TableInfo, is_index: bool, id: i64) -> bool {
    table.indices.iter_deref().any(|index| {
        let index = index.read();
        !index.global
            && tidb_stats::index::is_single_col_non_prefix_unique_index(&index)
            && if is_index {
                index.id == id
            } else {
                table
                    .columns
                    .get(index.columns.get(0).unwrap().read().offset as usize)
                    .is_some_and(|column| column.read().id == id)
            }
    })
}

/// Go `uniqueGlobalNDV`: absent/empty partition histograms contribute zero;
/// current rows bound old partition NDVs retained after deletes.
#[must_use]
pub fn unique_global_ndv<'a>(
    histograms: impl IntoIterator<Item = &'a Histogram>,
    count: i64,
) -> i64 {
    histograms
        .into_iter()
        .fold(0_i64, |ndv, hist| ndv.wrapping_add(hist.ndv))
        .min(count)
}
