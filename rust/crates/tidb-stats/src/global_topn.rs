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

//! Complete transcreation of Go `pkg/statistics/handle/globalstats/topn.go`
//! and the merge pipeline of `pkg/statistics/handle/globalstats/merge_worker.go`.
//!
//! Both source files are fully covered here: the sequential
//! `MergePartTopN2GlobalTopN`, the concurrent
//! `MergeGlobalStatsTopNByConcurrency`, the private `mergeGlobalStatsTopN`
//! dispatcher, `StatsWrapper`, `topnStatsMergeWorker` (`Run`/`Result`) and
//! `TopnStatsMergeResponse`. `TopnStatsMergeTask`, the other `merge_worker.go`
//! type, keeps its own module ([`crate::topn_merge_task`]) and is reused here.
//!
//! Faithful Rust adaptations, none changing observable behavior:
//! - Go's `gp.Pool` goroutines plus a buffered task channel and response
//!   channel become [`std::thread::scope`] workers draining a shared
//!   `Mutex<VecDeque<TopnStatsMergeTask>>` and appending to a shared response
//!   vector. Task hand-out order and the "return on first error, leaving the
//!   remaining tasks" behavior are preserved, and [`TopnStatsMergeWorker::run`]
//!   is directly callable so a single-threaded caller drives the same body.
//! - Go's `shardMutex []sync.Mutex` guarding `StatsWrapper.AllHg` becomes a
//!   borrowed `&[Mutex<&mut Histogram>]` slot array built by the concurrent
//!   entry point; the wrapper itself keeps plain `Vec<Histogram>` so the
//!   sequential path can take `&mut [Histogram]` exactly like Go's
//!   `[]*statistics.Histogram`.
//! - `hack.MutableString` map keys are `Vec<u8>`; the source's `float64`
//!   accumulation and its `uint64` truncation at ranking time are kept.
//!
//! Narrowings, named:
//! - `sessionctx.Context` is used by `mergeGlobalStatsTopN` only for
//!   `AnalyzePartitionMergeConcurrency` and the session `SQLKiller`, so
//!   [`merge_global_stats_topn`] takes `merge_concurrency` and a
//!   [`SqlKiller`] directly.
//! - Go reads the histogram element type from `hists[0].Tp.GetType()`. The
//!   ported [`Histogram`] carries no `types.FieldType`, so the MySQL type code
//!   arrives as [`TopNMergeOptions::value_type`].
//! - Go's `chunk.Compare` on histogram bounds is collation-free; the ported
//!   histogram comparisons take a collation, supplied as
//!   [`TopNMergeOptions::collation`].
//! - `SQLKiller::HandleSignal` returns a `terror` value from a crate this one
//!   does not depend on, so [`GlobalTopNMergeError::Killed`] carries its
//!   rendered message — the same text Go's concurrent path joins into its
//!   aggregate error.
//! - `MaxPartitionMergeBatchSize` is declared in the still-unported
//!   `global_stats.go`; only that constant is mirrored here as
//!   [`MAX_PARTITION_MERGE_BATCH_SIZE`].

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::TimeZone;
use tidb_codec::CodecError;
use tidb_datatype::{Collation, DatumValueError};
use tidb_util::sqlkiller::SqlKiller;

use crate::cmsketch::{
    check_empty_topns, find_topn, get_merged_topn_from_sorted_slice, TopN, TopNEntry,
};
use crate::datum_map_cache::DatumMapCache;
use crate::histogram::Histogram;
use crate::topn_merge_task::TopnStatsMergeTask;

/// Go `globalstats.MaxPartitionMergeBatchSize`.
pub const MAX_PARTITION_MERGE_BATCH_SIZE: usize = 256;

fn lock_unpoison<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The selected global TopN and values that did not fit in its requested size.
///
/// Go returns `(*TopN, []TopNMeta, []*Histogram, error)`; the histograms are
/// the caller's own slice, mutated in place, so they are not repeated here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GlobalTopNMerge {
    /// The selected values, sorted by encoded bytes like `TopN::sort`.
    pub top_n: TopN,
    /// Remaining values in source ranking order (count descending, then bytes).
    pub remainder: Vec<TopNEntry>,
}

/// Failure raised while merging partition TopN into a global TopN.
#[derive(Clone, Debug, PartialEq)]
pub enum GlobalTopNMergeError {
    /// Go: `killer.HandleSignal()` returned an error.
    Killed(String),
    /// Go: `datumMap.Put` failed to decode an encoded TopN value.
    Decode(CodecError),
    /// A histogram bound comparison failed. Go's `chunk.Compare` cannot fail.
    Compare(DatumValueError),
    /// Go: worker error strings joined with `","` by
    /// `MergeGlobalStatsTopNByConcurrency`.
    Workers(Vec<String>),
}

impl fmt::Display for GlobalTopNMergeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Killed(message) => formatter.write_str(message),
            Self::Decode(error) => error.fmt(formatter),
            Self::Compare(error) => error.fmt(formatter),
            Self::Workers(messages) => formatter.write_str(&messages.join(",")),
        }
    }
}

impl std::error::Error for GlobalTopNMergeError {}

impl From<CodecError> for GlobalTopNMergeError {
    fn from(error: CodecError) -> Self {
        Self::Decode(error)
    }
}

impl From<DatumValueError> for GlobalTopNMergeError {
    fn from(error: DatumValueError) -> Self {
        Self::Compare(error)
    }
}

/// The scalar arguments Go threads through `MergePartTopN2GlobalTopN` and
/// `topnStatsMergeWorker.Run`, plus the two named narrowings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopNMergeOptions {
    /// Go analyze version; `>= 2` lets a partition skip its own histogram.
    pub version: i64,
    /// Requested size of the global TopN. Has no default in the source.
    pub n: u32,
    /// Whether the merged statistics belong to an index.
    pub is_index: bool,
    /// MySQL type code of the histogram element (Go `hists[0].Tp.GetType()`).
    pub value_type: u8,
    /// Collation used for histogram bound comparisons.
    pub collation: Collation,
}

/// Go `StatsWrapper`: the partition histograms and TopN being merged.
#[derive(Debug, Default)]
pub struct StatsWrapper {
    /// Go `AllHg`.
    pub all_hg: Vec<Histogram>,
    /// Go `AllTopN`.
    pub all_topn: Vec<TopN>,
}

impl StatsWrapper {
    /// Go `NewStatsWrapper`.
    #[must_use]
    pub fn new(all_hg: Vec<Histogram>, all_topn: Vec<TopN>) -> Self {
        Self { all_hg, all_topn }
    }
}

/// Go `TopnStatsMergeResponse`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopnStatsMergeResponse {
    /// Go `Err`.
    pub err: Option<GlobalTopNMergeError>,
}

/// Go `topnStatsMergeWorker`.
///
/// The worker reads `statsWrapper.AllTopN` and merges counts into a shared
/// counter while mutating `statsWrapper.AllHg` under the per-histogram slot
/// locks. Every worker in a merge shares one instance, exactly as Go shares
/// the single `NewTopnStatsMergeWorker` value across its goroutines.
pub struct TopnStatsMergeWorker<'a> {
    killer: &'a SqlKiller,
    tasks: Mutex<VecDeque<TopnStatsMergeTask>>,
    responses: Mutex<Vec<TopnStatsMergeResponse>>,
    all_topn: &'a [TopN],
    all_hg: &'a [Mutex<&'a mut Histogram>],
    counter: Mutex<HashMap<Vec<u8>, f64>>,
}

impl<'a> TopnStatsMergeWorker<'a> {
    /// Go `NewTopnStatsMergeWorker`, with the task channel pre-filled.
    ///
    /// Go creates the buffered `taskCh`, starts the workers, then pushes every
    /// task and closes the channel. Since the task set is always fully known
    /// before any worker starts, the ported worker owns the queue directly.
    #[must_use]
    pub fn new(
        tasks: Vec<TopnStatsMergeTask>,
        all_topn: &'a [TopN],
        all_hg: &'a [Mutex<&'a mut Histogram>],
        killer: &'a SqlKiller,
    ) -> Self {
        Self {
            killer,
            tasks: Mutex::new(tasks.into()),
            responses: Mutex::new(Vec::new()),
            all_topn,
            all_hg,
            counter: Mutex::new(HashMap::new()),
        }
    }

    /// Go `(*topnStatsMergeWorker).Result`.
    #[must_use]
    pub fn result(&self) -> HashMap<Vec<u8>, f64> {
        lock_unpoison(&self.counter).clone()
    }

    /// Drains the responses Go reads off `respCh` after `wg.Wait()`.
    #[must_use]
    pub fn responses(&self) -> Vec<TopnStatsMergeResponse> {
        lock_unpoison(&self.responses).clone()
    }

    fn next_task(&self) -> Option<TopnStatsMergeTask> {
        lock_unpoison(&self.tasks).pop_front()
    }

    fn respond(&self, err: Option<GlobalTopNMergeError>) {
        lock_unpoison(&self.responses).push(TopnStatsMergeResponse { err });
    }

    fn check_signal(&self) -> Result<(), GlobalTopNMergeError> {
        match self.killer.handle_signal() {
            Some(error) => Err(GlobalTopNMergeError::Killed(error.to_string())),
            None => Ok(()),
        }
    }

    /// Go `(*topnStatsMergeWorker).Run`: consume tasks until the channel is
    /// drained, sending one response per task and returning early on error.
    pub fn run<TZ: TimeZone>(&self, timezone: Option<&TZ>, options: &TopNMergeOptions) {
        while let Some(task) = self.next_task() {
            match self.run_task(&task, timezone, options) {
                Ok(()) => self.respond(None),
                Err(error) => {
                    self.respond(Some(error));
                    return;
                }
            }
        }
    }

    fn run_task<TZ: TimeZone>(
        &self,
        task: &TopnStatsMergeTask,
        timezone: Option<&TZ>,
        options: &TopNMergeOptions,
    ) -> Result<(), GlobalTopNMergeError> {
        let start = task.start().max(0) as usize;
        let end = (task.end().max(0) as usize).min(self.all_topn.len());
        let part_num = self.all_topn.len();
        // Go allocates one datum cache per task, not per worker.
        let mut datum_map = DatumMapCache::new();

        for i in start..end {
            self.check_signal()?;
            let topn = &self.all_topn[i];
            if topn.total_count() == 0 {
                continue;
            }
            for entry in topn.resolved_entries() {
                let exists = {
                    let mut counter = lock_unpoison(&self.counter);
                    let exists = counter.contains_key(&entry.encoded);
                    *counter.entry(entry.encoded.clone()).or_insert(0.0) += entry.count as f64;
                    exists
                };
                if exists {
                    continue;
                }

                for j in 0..part_num {
                    self.check_signal()?;
                    if (j == i && options.version >= 2)
                        || find_topn(Some(&self.all_topn[j]), &entry.encoded).is_some()
                    {
                        continue;
                    }
                    let datum = match datum_map.get(&entry.encoded) {
                        Some(datum) => datum,
                        None => datum_map.put_encoded(
                            &entry.encoded,
                            &entry.encoded,
                            options.value_type,
                            options.is_index,
                            timezone,
                        )?,
                    };
                    let count = {
                        let mut hist = lock_unpoison(&self.all_hg[j]);
                        let (count, _) =
                            hist.equal_row_count(&datum, options.is_index, options.collation);
                        if count != 0.0 {
                            hist.binary_search_remove_value(
                                &datum,
                                count as i64,
                                options.collation,
                            )?;
                        }
                        count
                    };
                    if count != 0.0 {
                        *lock_unpoison(&self.counter)
                            .entry(entry.encoded.clone())
                            .or_insert(0.0) += count;
                    }
                }
            }
        }
        Ok(())
    }
}

/// Ranks an accumulated counter into a global TopN, Go's shared tail of
/// `MergePartTopN2GlobalTopN` and `MergeGlobalStatsTopNByConcurrency`.
fn rank_counter(counter: HashMap<Vec<u8>, f64>, n: u32) -> Option<GlobalTopNMerge> {
    if counter.is_empty() {
        return None;
    }
    let sorted: Vec<TopNEntry> = counter
        .into_iter()
        .map(|(encoded, count)| TopNEntry {
            encoded,
            count: count as u64,
        })
        .collect();
    let (top_n, remainder) = get_merged_topn_from_sorted_slice(sorted, n);
    Some(GlobalTopNMerge {
        top_n: top_n.unwrap_or_default(),
        remainder,
    })
}

/// Go `MergePartTopN2GlobalTopN`: merge partition-level TopN into one
/// global-level TopN, folding in matching histogram rows and removing them
/// from `hists` in place.
///
/// `None` is Go's `(nil, nil, hists, nil)` result for an empty counter.
pub fn merge_part_topn_2_global_topn<TZ: TimeZone>(
    timezone: Option<&TZ>,
    topns: &[TopN],
    hists: &mut [Histogram],
    options: &TopNMergeOptions,
    killer: &SqlKiller,
) -> Result<Option<GlobalTopNMerge>, GlobalTopNMergeError> {
    let part_num = topns.len();
    // Different TopN structures may hold the same value, we have to merge them.
    let mut counter: HashMap<Vec<u8>, f64> = HashMap::new();
    // datumMap caches the mapping from encoded bytes to the histogram-side
    // datum used to find the value in the histogram.
    let mut datum_map = DatumMapCache::new();
    let check = |killer: &SqlKiller| -> Result<(), GlobalTopNMergeError> {
        match killer.handle_signal() {
            Some(error) => Err(GlobalTopNMergeError::Killed(error.to_string())),
            None => Ok(()),
        }
    };

    for i in 0..part_num {
        check(killer)?;
        // Ignore the empty topN.
        if topns[i].total_count() == 0 {
            continue;
        }
        for entry in topns[i].resolved_entries() {
            let exists = counter.contains_key(&entry.encoded);
            *counter.entry(entry.encoded.clone()).or_insert(0.0) += entry.count as f64;
            if exists {
                // Already resolved against every histogram; go to the next value.
                continue;
            }

            // Check whether other partition-level statistics hold this value:
            // the TopN first, then the histogram.
            for j in 0..part_num {
                check(killer)?;
                if (j == i && options.version >= 2)
                    || find_topn(Some(&topns[j]), &entry.encoded).is_some()
                {
                    continue;
                }
                let datum = match datum_map.get(&entry.encoded) {
                    Some(datum) => datum,
                    None => datum_map.put_encoded(
                        &entry.encoded,
                        &entry.encoded,
                        options.value_type,
                        options.is_index,
                        timezone,
                    )?,
                };
                let (count, _) =
                    hists[j].equal_row_count(&datum, options.is_index, options.collation);
                if count != 0.0 {
                    *counter.entry(entry.encoded.clone()).or_insert(0.0) += count;
                    // Remove the value from the histogram it was taken from.
                    hists[j].binary_search_remove_value(&datum, count as i64, options.collation)?;
                }
            }
        }
    }

    Ok(rank_counter(counter, options.n))
}

/// Go `MergeGlobalStatsTopNByConcurrency`: split the partition TopN into
/// batches and merge them with `merge_concurrency` workers.
pub fn merge_global_stats_topn_by_concurrency<TZ>(
    merge_concurrency: usize,
    merge_batch_size: usize,
    wrapper: &mut StatsWrapper,
    timezone: Option<&TZ>,
    options: &TopNMergeOptions,
    killer: &SqlKiller,
) -> Result<Option<GlobalTopNMerge>, GlobalTopNMergeError>
where
    TZ: TimeZone + Sync,
    TZ::Offset: Sync,
{
    let mut merge_concurrency = merge_concurrency;
    if wrapper.all_topn.len() < merge_concurrency {
        merge_concurrency = wrapper.all_topn.len();
    }
    let batch = merge_batch_size.max(1);
    let mut tasks = Vec::new();
    let mut start = 0_usize;
    while start < wrapper.all_topn.len() {
        let end = (start + batch).min(wrapper.all_topn.len());
        tasks.push(TopnStatsMergeTask::new(start as isize, end as isize));
        start = end;
    }

    let StatsWrapper { all_hg, all_topn } = wrapper;
    let slots: Vec<Mutex<&mut Histogram>> = all_hg.iter_mut().map(Mutex::new).collect();
    let worker = TopnStatsMergeWorker::new(tasks, all_topn, &slots, killer);
    std::thread::scope(|scope| {
        for _ in 0..merge_concurrency {
            scope.spawn(|| worker.run(timezone, options));
        }
    });

    // handle Error
    let messages: Vec<String> = worker
        .responses()
        .into_iter()
        .filter_map(|response| response.err)
        .map(|error| error.to_string())
        .collect();
    if !messages.is_empty() {
        return Err(GlobalTopNMergeError::Workers(messages));
    }

    // fetch the response from each worker and merge them into global topn stats
    Ok(rank_counter(worker.result(), options.n))
}

/// Go `mergeGlobalStatsTopN`: the private dispatcher choosing between the
/// sequential and concurrent merge.
///
/// `merge_concurrency` is the named narrowing of
/// `sc.GetSessionVars().AnalyzePartitionMergeConcurrency`.
pub fn merge_global_stats_topn<TZ>(
    merge_concurrency: usize,
    wrapper: &mut StatsWrapper,
    timezone: Option<&TZ>,
    options: &TopNMergeOptions,
    killer: &SqlKiller,
) -> Result<Option<GlobalTopNMerge>, GlobalTopNMergeError>
where
    TZ: TimeZone + Sync,
    TZ::Offset: Sync,
{
    let borrowed: Vec<Option<&TopN>> = wrapper.all_topn.iter().map(Some).collect();
    if check_empty_topns(&borrowed) {
        return Ok(None);
    }
    drop(borrowed);

    // use original method if concurrency equals 1 or for version1
    if merge_concurrency < 2 {
        let StatsWrapper { all_hg, all_topn } = wrapper;
        return merge_part_topn_2_global_topn(timezone, all_topn, all_hg, options, killer);
    }
    let batch_size =
        (wrapper.all_topn.len() / merge_concurrency).clamp(1, MAX_PARTITION_MERGE_BATCH_SIZE);
    merge_global_stats_topn_by_concurrency(
        merge_concurrency,
        batch_size,
        wrapper,
        timezone,
        options,
        killer,
    )
}
