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

//! Go `pkg/util/stmtsummary/evicted.go`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tidb_datatype::{core_time_from_datetime, Datum, Time, TimeType};

use crate::statement_summary::{
    EvictedSink, StmtDigestKey, StmtSummaryByDigest, StmtSummaryByDigestElement,
    StmtSummaryByDigestMap, StmtSummaryStats,
};

/// Go's `time.Unix(seconds, 0)`, read back as a UTC instant.
///
/// Go renders the instant in the process-local zone; this crate keeps every
/// timestamp in UTC (as `statement_summary.rs` already does for `firstSeen` /
/// `lastSeen`), so the calendar fields differ from Go's by the local offset
/// while the underlying instant is identical.
fn unix_seconds(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).unwrap_or_else(|| DateTime::from_timestamp_nanos(0))
}

/// Go `types.NewTime(types.FromGoTime(time.Unix(seconds, 0)), mysql.TypeTimestamp, 0)`.
fn timestamp_datum(seconds: i64) -> Datum {
    let core = core_time_from_datetime(unix_seconds(seconds));
    // Go's `NewTime` cannot fail; fsp 0 is always a valid fsp here.
    let time = Time::new(core, TimeType::Timestamp, 0).unwrap_or_else(|error| {
        unreachable!("fsp 0 is always valid for a timestamp: {error:?}");
    });
    Datum::new_time(time)
}

/// Go `stmtSummaryByDigestEvicted`: the digests evicted from
/// `stmtSummaryByDigestMap`.
///
/// Go embeds a `sync.Mutex`; here the mutex lives outside, because
/// `stmtSummaryByDigestMap` already reaches this type through an
/// `Arc<Mutex<..>>` (see the [`EvictedSink`] implementation below).
#[derive(Debug, Default)]
pub struct StmtSummaryByDigestEvicted {
    /// Go `history`: evicted data recorded per interval. The latest interval is
    /// the back of the queue, matching Go's `container/list` usage.
    history: VecDeque<StmtSummaryByDigestEvictedElement>,
}

/// Go `stmtSummaryByDigestEvictedElement`: one interval's worth of evictions.
#[derive(Clone, Debug)]
pub struct StmtSummaryByDigestEvictedElement {
    /// Go `beginTime`: the begin time of the current interval.
    pub begin_time: i64,
    /// Go `endTime`: the end time of the current interval.
    pub end_time: i64,
    /// Go `count`: the number of digests evicted into this interval.
    pub count: i64,
    /// Go `otherSummary`: the summed-up information of the evicted elements.
    pub other_summary: StmtSummaryByDigestElement,
}

/// Go's `isMatch` / `isTooOld` / `isTooYoung` result of
/// `(*stmtSummaryByDigestEvictedElement).matchAndAdd`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatchResult {
    /// Go `isMatch`: the digest's interval fits inside this element's.
    Match,
    /// Go `isTooOld`: the digest ended at or before this element began.
    TooOld,
    /// Go `isTooYoung`: the digest reaches past this element's end.
    TooYoung,
}

impl StmtSummaryByDigestEvicted {
    /// Go `newStmtSummaryByDigestEvicted`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go's `ssbde.history.Len()`.
    #[must_use]
    pub fn history_len(&self) -> usize {
        self.history.len()
    }

    /// Go's read access to `ssbde.history`, oldest interval first.
    #[must_use]
    pub fn history(&self) -> &VecDeque<StmtSummaryByDigestEvictedElement> {
        &self.history
    }

    /// Go `(*stmtSummaryByDigestEvicted).AddEvicted`.
    ///
    /// Go passes `evictedKey`/`evictedValue` as pointers that the tests set to
    /// `nil`; a `None` key is Go's "refresh" mode (intervals are created but
    /// nothing is counted) and a `None` value returns immediately. Go's
    /// `evictedValue.history == nil` guard collapses into the empty-queue case,
    /// which already skips the loop. Go locks `evictedValue` itself; here the
    /// caller holds that lock and hands over a shared reference.
    pub fn add_evicted(
        &mut self,
        evicted_key: Option<&StmtDigestKey>,
        evicted_value: Option<&StmtSummaryByDigest>,
        history_size: usize,
    ) {
        let Some(evicted_value) = evicted_value else {
            return;
        };

        // Go's `h`: a cursor into `ssbde.history` that starts at the back and
        // is carried across every iteration of the outer loop. `None` is Go's
        // `nil`. When the trimming below removes the very element `h` points
        // at, Go detaches it (`container/list.Remove` clears its links), after
        // which `h.Prev()` is nil, `InsertAfter` on it is a no-op, and a match
        // would add into an unreachable element. All three are observationally
        // equal to dropping the cursor, so it becomes `None` here.
        let mut cursor: Option<usize> = self.history.len().checked_sub(1);

        for element in evicted_value.history.iter().rev() {
            let evicted_element = element.lock().unwrap();

            if self.history.is_empty() && history_size != 0 {
                // No record in `ssbde.history`, direct insert.
                let mut record = StmtSummaryByDigestEvictedElement::new(
                    evicted_element.begin_time,
                    evicted_element.end_time,
                );
                record.add_evicted(evicted_key, &evicted_element);
                self.history.push_front(record);
                cursor = Some(self.history.len() - 1);
            } else {
                // Look for a matching history interval.
                while let Some(index) = cursor {
                    let result = self.history[index].match_and_add(evicted_key, &evicted_element);
                    match result {
                        // Automatically added.
                        MatchResult::Match => break,
                        // Not matching: create a new record and insert it.
                        MatchResult::TooYoung => {
                            let mut record = StmtSummaryByDigestEvictedElement::new(
                                evicted_element.begin_time,
                                evicted_element.end_time,
                            );
                            record.add_evicted(evicted_key, &evicted_element);
                            self.history.insert(index + 1, record);
                            break;
                        }
                        MatchResult::TooOld => {
                            if index == 0 {
                                // The digest is older than every record in
                                // `ssbde.history`.
                                let mut record = StmtSummaryByDigestEvictedElement::new(
                                    evicted_element.begin_time,
                                    evicted_element.end_time,
                                );
                                record.add_evicted(evicted_key, &evicted_element);
                                self.history.push_front(record);
                                cursor = Some(index + 1);
                                break;
                            }
                            cursor = Some(index - 1);
                        }
                    }
                }
            }

            // Prevent exceeding the history size.
            while self.history.len() > history_size && !self.history.is_empty() {
                self.history.pop_front();
                cursor = match cursor {
                    Some(0) | None => None,
                    Some(index) => Some(index - 1),
                };
            }
        }
    }

    /// Go `(*stmtSummaryByDigestEvicted).Clear`.
    pub fn clear(&mut self) {
        self.history.clear();
    }

    /// Go `(*stmtSummaryByDigestEvicted).ToEvictedCountDatum`: converts the
    /// history to `evicted count` rows, newest interval first.
    ///
    /// Go skips rows whose `toEvictedCountDatum` returned `nil`; that helper
    /// always returns a row, so no row is skipped here.
    #[must_use]
    pub fn to_evicted_count_datum(&self) -> Vec<Vec<Datum>> {
        self.history
            .iter()
            .rev()
            .map(StmtSummaryByDigestEvictedElement::to_evicted_count_datum)
            .collect()
    }

    /// Go `(*stmtSummaryByDigestEvicted).collectHistorySummaries`.
    #[must_use]
    pub fn collect_history_summaries(
        &self,
        history_size: usize,
    ) -> Vec<&StmtSummaryByDigestEvictedElement> {
        self.history.iter().take(history_size).collect()
    }
}

/// Wires Go's `stmtSummaryByDigestMap.other` to the real rollup. The shared
/// `Arc` is what Go's single `*stmtSummaryByDigestEvicted` pointer provides:
/// the map mutates it through this implementation while its owner still reads
/// it.
impl EvictedSink for Arc<Mutex<StmtSummaryByDigestEvicted>> {
    fn add_evicted(
        &mut self,
        key: &StmtDigestKey,
        value: &Arc<Mutex<StmtSummaryByDigest>>,
        history_size: usize,
    ) {
        let value = value.lock().unwrap();
        self.lock()
            .unwrap()
            .add_evicted(Some(key), Some(&value), history_size);
    }

    fn clear(&mut self) {
        self.lock().unwrap().clear();
    }
}

impl StmtSummaryByDigestMap {
    /// Go `(*stmtSummaryByDigestMap).ToEvictedCountDatum`.
    ///
    /// Returns no rows when the map was built by
    /// [`StmtSummaryByDigestMap::with_sinks`] with a sink other than the
    /// `evicted.go` rollup.
    #[must_use]
    pub fn to_evicted_count_datum(&self) -> Vec<Vec<Datum>> {
        self.evicted().map_or_else(Vec::new, |evicted| {
            evicted.lock().unwrap().to_evicted_count_datum()
        })
    }
}

impl StmtSummaryByDigestEvictedElement {
    /// Go `newStmtSummaryByDigestEvictedElement`.
    #[must_use]
    pub fn new(begin_time: i64, end_time: i64) -> Self {
        Self {
            begin_time,
            end_time,
            count: 0,
            other_summary: StmtSummaryByDigestElement {
                begin_time,
                end_time,
                stats: StmtSummaryStats {
                    // Go `time.Duration(math.MaxInt64)`.
                    min_latency: Duration::from_nanos(i64::MAX as u64),
                    first_seen: unix_seconds(end_time),
                    ..StmtSummaryStats::default()
                },
            },
        }
    }

    /// Go `(*stmtSummaryByDigestEvictedElement).addEvicted`.
    pub fn add_evicted(
        &mut self,
        digest_key: Option<&StmtDigestKey>,
        digest_value: &StmtSummaryByDigestElement,
    ) {
        if digest_key.is_some() {
            self.count += 1;
            add_info(&mut self.other_summary, digest_value);
        }
    }

    /// Go `(*stmtSummaryByDigestEvictedElement).matchAndAdd`: compares the time
    /// interval of this element against `digest_value`, adding it on a match.
    ///
    /// Go's `seElement == nil || digestValue == nil` guard is unreachable here:
    /// both are references.
    pub fn match_and_add(
        &mut self,
        digest_key: Option<&StmtDigestKey>,
        digest_value: &StmtSummaryByDigestElement,
    ) -> MatchResult {
        let (s_begin_time, s_end_time) = (self.begin_time, self.end_time);
        let (e_begin_time, e_end_time) = (digest_value.begin_time, digest_value.end_time);
        if s_begin_time <= e_begin_time && e_end_time <= s_end_time {
            self.add_evicted(digest_key, digest_value);
            MatchResult::Match
        } else if e_end_time <= s_begin_time {
            MatchResult::TooOld
        } else {
            MatchResult::TooYoung
        }
    }

    /// Go `(*stmtSummaryByDigestEvictedElement).toEvictedCountDatum`.
    #[must_use]
    pub fn to_evicted_count_datum(&self) -> Vec<Datum> {
        vec![
            timestamp_datum(self.begin_time),
            timestamp_datum(self.end_time),
            Datum::new_int(self.count),
        ]
    }
}

/// Go `addInfo`: adds the information in `add_with` into `add_to`.
///
/// Go takes `addTo.Lock()`; the `&mut` receiver stands in for that lock.
#[allow(clippy::too_many_lines)]
pub fn add_info(add_to: &mut StmtSummaryByDigestElement, add_with: &StmtSummaryByDigestElement) {
    let (add_to, add_with) = (&mut add_to.stats, &add_with.stats);

    // user
    for user in &add_with.auth_users {
        add_to.auth_users.insert(user.clone());
    }

    // execCount and sumWarnings
    add_to.exec_count += add_with.exec_count;
    add_to.sum_warnings += add_with.sum_warnings;

    // latency
    add_to.sum_latency += add_with.sum_latency;
    if add_to.max_latency < add_with.max_latency {
        add_to.max_latency = add_with.max_latency;
    }
    if add_to.min_latency > add_with.min_latency {
        add_to.min_latency = add_with.min_latency;
    }
    add_to.sum_parse_latency += add_with.sum_parse_latency;
    if add_to.max_parse_latency < add_with.max_parse_latency {
        add_to.max_parse_latency = add_with.max_parse_latency;
    }
    add_to.sum_compile_latency += add_with.sum_compile_latency;
    if add_to.max_compile_latency < add_with.max_compile_latency {
        add_to.max_compile_latency = add_with.max_compile_latency;
    }

    // coprocessor
    add_to.sum_num_cop_tasks += add_with.sum_num_cop_tasks;
    if add_to.max_cop_process_time < add_with.max_cop_process_time {
        add_to.max_cop_process_time = add_with.max_cop_process_time;
        add_to.max_cop_process_address = add_with.max_cop_process_address.clone();
    }
    if add_to.max_cop_wait_time < add_with.max_cop_wait_time {
        add_to.max_cop_wait_time = add_with.max_cop_wait_time;
        add_to.max_cop_wait_address = add_with.max_cop_wait_address.clone();
    }

    // TiKV
    add_to.sum_process_time += add_with.sum_process_time;
    if add_to.max_process_time < add_with.max_process_time {
        add_to.max_process_time = add_with.max_process_time;
    }
    add_to.sum_wait_time += add_with.sum_wait_time;
    if add_to.max_wait_time < add_with.max_wait_time {
        add_to.max_wait_time = add_with.max_wait_time;
    }
    add_to.sum_backoff_time += add_with.sum_backoff_time;
    if add_to.max_backoff_time < add_with.max_backoff_time {
        add_to.max_backoff_time = add_with.max_backoff_time;
    }

    add_to.sum_total_keys += add_with.sum_total_keys;
    if add_to.max_total_keys < add_with.max_total_keys {
        add_to.max_total_keys = add_with.max_total_keys;
    }
    add_to.sum_processed_keys += add_with.sum_processed_keys;
    if add_to.max_processed_keys < add_with.max_processed_keys {
        add_to.max_processed_keys = add_with.max_processed_keys;
    }
    add_to.sum_rocksdb_delete_skipped_count += add_with.sum_rocksdb_delete_skipped_count;
    if add_to.max_rocksdb_delete_skipped_count < add_with.max_rocksdb_delete_skipped_count {
        add_to.max_rocksdb_delete_skipped_count = add_with.max_rocksdb_delete_skipped_count;
    }
    add_to.sum_rocksdb_key_skipped_count += add_with.sum_rocksdb_key_skipped_count;
    if add_to.max_rocksdb_key_skipped_count < add_with.max_rocksdb_key_skipped_count {
        add_to.max_rocksdb_key_skipped_count = add_with.max_rocksdb_key_skipped_count;
    }
    add_to.sum_rocksdb_block_cache_hit_count += add_with.sum_rocksdb_block_cache_hit_count;
    if add_to.max_rocksdb_block_cache_hit_count < add_with.max_rocksdb_block_cache_hit_count {
        add_to.max_rocksdb_block_cache_hit_count = add_with.max_rocksdb_block_cache_hit_count;
    }
    add_to.sum_rocksdb_block_read_count += add_with.sum_rocksdb_block_read_count;
    if add_to.max_rocksdb_block_read_count < add_with.max_rocksdb_block_read_count {
        add_to.max_rocksdb_block_read_count = add_with.max_rocksdb_block_read_count;
    }
    add_to.sum_rocksdb_block_read_byte += add_with.sum_rocksdb_block_read_byte;
    if add_to.max_rocksdb_block_read_byte < add_with.max_rocksdb_block_read_byte {
        add_to.max_rocksdb_block_read_byte = add_with.max_rocksdb_block_read_byte;
    }
    add_to.sum_ia_remote_read_segment_count += add_with.sum_ia_remote_read_segment_count;
    if add_to.max_ia_remote_read_segment_count < add_with.max_ia_remote_read_segment_count {
        add_to.max_ia_remote_read_segment_count = add_with.max_ia_remote_read_segment_count;
    }
    add_to.sum_ia_remote_read_segment_size += add_with.sum_ia_remote_read_segment_size;
    if add_to.max_ia_remote_read_segment_size < add_with.max_ia_remote_read_segment_size {
        add_to.max_ia_remote_read_segment_size = add_with.max_ia_remote_read_segment_size;
    }
    add_to.sum_ia_remote_read_segment_wait_time += add_with.sum_ia_remote_read_segment_wait_time;
    if add_to.max_ia_remote_read_segment_wait_time < add_with.max_ia_remote_read_segment_wait_time {
        add_to.max_ia_remote_read_segment_wait_time = add_with.max_ia_remote_read_segment_wait_time;
    }

    // txn
    add_to.commit_count += add_with.commit_count;
    add_to.sum_prewrite_time += add_with.sum_prewrite_time;
    if add_to.max_prewrite_time < add_with.max_prewrite_time {
        add_to.max_prewrite_time = add_with.max_prewrite_time;
    }
    add_to.sum_commit_time += add_with.sum_commit_time;
    if add_to.max_commit_time < add_with.max_commit_time {
        add_to.max_commit_time = add_with.max_commit_time;
    }
    add_to.sum_get_commit_ts_time += add_with.sum_get_commit_ts_time;
    if add_to.max_get_commit_ts_time < add_with.max_get_commit_ts_time {
        add_to.max_get_commit_ts_time = add_with.max_get_commit_ts_time;
    }
    add_to.sum_commit_backoff_time += add_with.sum_commit_backoff_time;
    if add_to.max_commit_backoff_time < add_with.max_commit_backoff_time {
        add_to.max_commit_backoff_time = add_with.max_commit_backoff_time;
    }
    add_to.sum_resolve_lock_time += add_with.sum_resolve_lock_time;
    if add_to.max_resolve_lock_time < add_with.max_resolve_lock_time {
        add_to.max_resolve_lock_time = add_with.max_resolve_lock_time;
    }
    add_to.sum_local_latch_time += add_with.sum_local_latch_time;
    if add_to.max_local_latch_time < add_with.max_local_latch_time {
        add_to.max_local_latch_time = add_with.max_local_latch_time;
    }
    add_to.sum_write_keys += add_with.sum_write_keys;
    if add_to.max_write_keys < add_with.max_write_keys {
        add_to.max_write_keys = add_with.max_write_keys;
    }
    add_to.sum_write_size += add_with.sum_write_size;
    if add_to.max_write_size < add_with.max_write_size {
        add_to.max_write_size = add_with.max_write_size;
    }
    add_to.sum_prewrite_region_num += add_with.sum_prewrite_region_num;
    if add_to.max_prewrite_region_num < add_with.max_prewrite_region_num {
        add_to.max_prewrite_region_num = add_with.max_prewrite_region_num;
    }
    add_to.sum_txn_retry += add_with.sum_txn_retry;
    if add_to.max_txn_retry < add_with.max_txn_retry {
        add_to.max_txn_retry = add_with.max_txn_retry;
    }
    add_to.sum_backoff_times += add_with.sum_backoff_times;
    for (backoff_type, backoff_value) in &add_with.backoff_types {
        *add_to
            .backoff_types
            .entry(backoff_type.clone())
            .or_insert(0) += *backoff_value;
    }

    // plan cache
    add_to.plan_cache_hits += add_with.plan_cache_hits;

    // other
    add_to.sum_affected_rows += add_with.sum_affected_rows;
    add_to.sum_mem += add_with.sum_mem;
    if add_to.max_mem < add_with.max_mem {
        add_to.max_mem = add_with.max_mem;
    }
    add_to.sum_mem_arbitration += add_with.sum_mem_arbitration;
    if add_to.max_mem_arbitration < add_with.max_mem_arbitration {
        add_to.max_mem_arbitration = add_with.max_mem_arbitration;
    }
    add_to.sum_disk += add_with.sum_disk;
    if add_to.max_disk < add_with.max_disk {
        add_to.max_disk = add_with.max_disk;
    }
    if add_to.first_seen > add_with.first_seen {
        add_to.first_seen = add_with.first_seen;
    }
    if add_to.last_seen < add_with.last_seen {
        add_to.last_seen = add_with.last_seen;
    }
    add_to.exec_retry_count += add_with.exec_retry_count;
    add_to.exec_retry_time += add_with.exec_retry_time;
    add_to.sum_kv_total += add_with.sum_kv_total;
    add_to.sum_pd_total += add_with.sum_pd_total;
    add_to.sum_backoff_total += add_with.sum_backoff_total;
    add_to.sum_write_sql_resp_total += add_with.sum_write_sql_resp_total;
    add_to.sum_tidb_cpu += add_with.sum_tidb_cpu;
    add_to.sum_tikv_cpu += add_with.sum_tikv_cpu;

    add_to.sum_errors += add_with.sum_errors;

    add_to.ru.merge(&add_with.ru);
    // `resourceGroupName` might not be inited, because this is an evicted item.
    add_to.resource_group_name = add_with.resource_group_name.clone();
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fmt::Write as _;

    use crate::statement_summary::tests::generate_any_exec_info;

    use super::*;

    /// Go's `time.Duration(n)`.
    fn ns(nanos: u64) -> Duration {
        Duration::from_nanos(nanos)
    }

    /// Go `newInduceSsbde`: fakes a `stmtSummaryByDigestElement`.
    fn new_induce_ssbde(begin_time: i64, end_time: i64) -> StmtSummaryByDigestElement {
        StmtSummaryByDigestElement {
            begin_time,
            end_time,
            stats: StmtSummaryStats {
                // Go `time.Duration.Round(1<<63-1, time.Nanosecond)`.
                min_latency: Duration::from_nanos(i64::MAX as u64),
                ..StmtSummaryStats::default()
            },
        }
    }

    /// Go `newInduceSsbd`: fakes a `stmtSummaryByDigest`.
    fn new_induce_ssbd(begin_time: i64, end_time: i64) -> StmtSummaryByDigest {
        let mut ssbd = StmtSummaryByDigest::default();
        push_history(&mut ssbd, begin_time, end_time);
        ssbd
    }

    /// Go's `value.history.PushBack(newInduceSsbde(begin, end))`.
    fn push_history(ssbd: &mut StmtSummaryByDigest, begin_time: i64, end_time: i64) {
        ssbd.history
            .push_back(Arc::new(Mutex::new(new_induce_ssbde(begin_time, end_time))));
    }

    /// Go `generateStmtSummaryByDigestKeyValue`.
    fn generate_stmt_summary_by_digest_key_value(
        schema: &str,
        begin_time: i64,
        end_time: i64,
    ) -> (StmtDigestKey, StmtSummaryByDigest) {
        let mut key = StmtDigestKey::new();
        key.init(schema, "", "", "", "", "");
        (key, new_induce_ssbd(begin_time, end_time))
    }

    /// Go `getAllEvicted`.
    fn get_all_evicted(ssbde: &StmtSummaryByDigestEvicted) -> String {
        let mut buf = String::new();
        for element in ssbde.history.iter().rev() {
            if !buf.is_empty() {
                buf.push_str(", ");
            }
            write!(buf, "{}", get_evicted(element)).unwrap();
        }
        buf
    }

    /// Go `getEvicted`.
    fn get_evicted(element: &StmtSummaryByDigestEvictedElement) -> String {
        format!(
            "{{begin: {}, end: {}, count: {}}}",
            element.begin_time, element.end_time, element.count
        )
    }

    /// Go's `expectedEvictedCount` row shape.
    fn evicted_count_row(begin_time: i64, end_time: i64, count: i64) -> Vec<Datum> {
        vec![
            timestamp_datum(begin_time),
            timestamp_datum(end_time),
            Datum::new_int(count),
        ]
    }

    /// Go `TestStmtSummaryByDigestEvicted`.
    #[test]
    fn test_stmt_summary_by_digest_evicted() {
        let stmt_evicted = StmtSummaryByDigestEvicted::new();
        assert_eq!(stmt_evicted.history_len(), 0);
    }

    /// Go `TestNewStmtSummaryByDigestEvictedElement`.
    #[test]
    fn test_new_stmt_summary_by_digest_evicted_element() {
        let now = chrono::Utc::now().timestamp();
        let end = now + 60;
        let element = StmtSummaryByDigestEvictedElement::new(now, end);
        assert_eq!(element.begin_time, now);
        assert_eq!(element.end_time, end);
        assert_eq!(element.count, 0);
    }

    /// Go `TestStmtSummaryByDigestEvictedElement`.
    #[test]
    fn test_stmt_summary_by_digest_evicted_element() {
        let mut record = StmtSummaryByDigestEvictedElement::new(0, 1);
        let (evicted_key, evicted_value) = generate_stmt_summary_by_digest_key_value("alpha", 0, 1);
        let digest_value = evicted_value
            .history
            .back()
            .unwrap()
            .lock()
            .unwrap()
            .clone();

        // Test poisoning with a NULL key. Go also passes a nil digest value in
        // the first call; a reference cannot be nil, so the zero-valued element
        // stands in and the nil key already makes the call a no-op.
        record.add_evicted(None, &digest_value);
        assert_eq!(get_evicted(&record), "{begin: 0, end: 1, count: 0}");
        record.add_evicted(None, &digest_value);
        assert_eq!(get_evicted(&record), "{begin: 0, end: 1, count: 0}");

        // Test adding an evicted key and evicted `stmtSummaryByDigestElement`.
        record.add_evicted(Some(&evicted_key), &digest_value);
        assert_eq!(get_evicted(&record), "{begin: 0, end: 1, count: 1}");

        // Test adding the same *kind* of values.
        record.add_evicted(Some(&evicted_key), &digest_value);
        assert_eq!(get_evicted(&record), "{begin: 0, end: 1, count: 2}");

        // Test adding a different *kind* of values.
        let (evicted_key, evicted_value) = generate_stmt_summary_by_digest_key_value("bravo", 0, 1);
        let digest_value = evicted_value
            .history
            .back()
            .unwrap()
            .lock()
            .unwrap()
            .clone();
        record.add_evicted(Some(&evicted_key), &digest_value);
        assert_eq!(get_evicted(&record), "{begin: 0, end: 1, count: 3}");
    }

    /// Go `TestSimpleStmtSummaryByDigestEvicted`.
    #[test]
    fn test_simple_stmt_summary_by_digest_evicted() {
        let mut ssbde = StmtSummaryByDigestEvicted::new();
        let (evicted_key, evicted_value) = generate_stmt_summary_by_digest_key_value("a", 1, 2);

        // Test NULL.
        ssbde.add_evicted(None, None, 10);
        assert_eq!(ssbde.history_len(), 0);
        ssbde.clear();
        // Passing a NULL key is used as a *refresh*.
        ssbde.add_evicted(None, Some(&evicted_value), 10);
        assert_eq!(ssbde.history_len(), 1);
        ssbde.clear();
        ssbde.add_evicted(Some(&evicted_key), None, 10);
        assert_eq!(ssbde.history_len(), 0);
        ssbde.clear();

        // Test a zero `historySize`.
        ssbde.add_evicted(Some(&evicted_key), Some(&evicted_value), 0);
        assert_eq!(ssbde.history_len(), 0);

        let mut ssbde = StmtSummaryByDigestEvicted::new();
        ssbde.add_evicted(Some(&evicted_key), Some(&evicted_value), 1);
        assert_eq!(get_all_evicted(&ssbde), "{begin: 1, end: 2, count: 1}");
        // Test inserting the same *kind* of digest.
        ssbde.add_evicted(Some(&evicted_key), Some(&evicted_value), 1);
        assert_eq!(get_all_evicted(&ssbde), "{begin: 1, end: 2, count: 2}");

        let (evicted_key, evicted_value) = generate_stmt_summary_by_digest_key_value("b", 1, 2);
        ssbde.add_evicted(Some(&evicted_key), Some(&evicted_value), 1);
        assert_eq!(get_all_evicted(&ssbde), "{begin: 1, end: 2, count: 3}");

        let (evicted_key, evicted_value) = generate_stmt_summary_by_digest_key_value("b", 5, 6);
        ssbde.add_evicted(Some(&evicted_key), Some(&evicted_value), 2);
        assert_eq!(
            get_all_evicted(&ssbde),
            "{begin: 5, end: 6, count: 1}, {begin: 1, end: 2, count: 3}"
        );

        let (evicted_key, evicted_value) = generate_stmt_summary_by_digest_key_value("b", 3, 4);
        ssbde.add_evicted(Some(&evicted_key), Some(&evicted_value), 3);
        assert_eq!(
            get_all_evicted(&ssbde),
            "{begin: 5, end: 6, count: 1}, {begin: 3, end: 4, count: 1}, {begin: 1, end: 2, count: 3}"
        );

        // Test an evicted element with a multi-time-range value.
        let mut ssbde = StmtSummaryByDigestEvicted::new();
        let (evicted_key, mut evicted_value) = generate_stmt_summary_by_digest_key_value("a", 1, 2);
        push_history(&mut evicted_value, 2, 3);
        push_history(&mut evicted_value, 5, 6);
        push_history(&mut evicted_value, 8, 9);
        ssbde.add_evicted(Some(&evicted_key), Some(&evicted_value), 3);
        assert_eq!(
            get_all_evicted(&ssbde),
            "{begin: 8, end: 9, count: 1}, {begin: 5, end: 6, count: 1}, {begin: 2, end: 3, count: 1}"
        );

        let mut evicted_key = StmtDigestKey::new();
        evicted_key.init("b", "", "", "", "", "");
        ssbde.add_evicted(Some(&evicted_key), Some(&evicted_value), 4);
        assert_eq!(
            get_all_evicted(&ssbde),
            "{begin: 8, end: 9, count: 2}, {begin: 5, end: 6, count: 2}, {begin: 2, end: 3, count: 2}, {begin: 1, end: 2, count: 1}"
        );

        let (evicted_key, mut evicted_value) = generate_stmt_summary_by_digest_key_value("c", 4, 5);
        push_history(&mut evicted_value, 5, 6);
        push_history(&mut evicted_value, 7, 8);
        ssbde.add_evicted(Some(&evicted_key), Some(&evicted_value), 4);
        assert_eq!(
            get_all_evicted(&ssbde),
            "{begin: 8, end: 9, count: 2}, {begin: 7, end: 8, count: 1}, {begin: 5, end: 6, count: 3}, {begin: 4, end: 5, count: 1}"
        );

        let (evicted_key, evicted_value) = generate_stmt_summary_by_digest_key_value("d", 7, 8);
        ssbde.add_evicted(Some(&evicted_key), Some(&evicted_value), 4);
        assert_eq!(
            get_all_evicted(&ssbde),
            "{begin: 8, end: 9, count: 2}, {begin: 7, end: 8, count: 2}, {begin: 5, end: 6, count: 3}, {begin: 4, end: 5, count: 1}"
        );

        // Test "too old".
        let (evicted_key, mut evicted_value) = generate_stmt_summary_by_digest_key_value("d", 0, 1);
        push_history(&mut evicted_value, 1, 2);
        push_history(&mut evicted_value, 2, 3);
        push_history(&mut evicted_value, 4, 5);
        ssbde.add_evicted(Some(&evicted_key), Some(&evicted_value), 4);
        assert_eq!(
            get_all_evicted(&ssbde),
            "{begin: 8, end: 9, count: 2}, {begin: 7, end: 8, count: 2}, {begin: 5, end: 6, count: 3}, {begin: 4, end: 5, count: 2}"
        );

        // Test "too young".
        let (evicted_key, mut evicted_value) = generate_stmt_summary_by_digest_key_value("d", 1, 2);
        push_history(&mut evicted_value, 9, 10);
        ssbde.add_evicted(Some(&evicted_key), Some(&evicted_value), 4);
        assert_eq!(
            get_all_evicted(&ssbde),
            "{begin: 9, end: 10, count: 1}, {begin: 8, end: 9, count: 2}, {begin: 7, end: 8, count: 2}, {begin: 5, end: 6, count: 3}"
        );
    }

    /// Go `TestMapToEvictedCountDatum`: `stmtSummaryByDigestMap.ToEvictedCountDatum`.
    ///
    /// Go's direct `ssMap.summaryMap.SetCapacity(1)` becomes `SetMaxStmtCount`,
    /// which is the only public path to the LRU's capacity here; nothing in
    /// `AddStatement` reads `optMaxStmtCount`, so the two agree.
    #[test]
    fn test_map_to_evicted_count_datum() {
        let ss_map = StmtSummaryByDigestMap::new();
        ss_map.clear();
        let now = chrono::Utc::now().timestamp();
        let mut interval = ss_map.refresh_interval();
        ss_map.set_begin_time_for_cur_interval(now + interval);

        // Set the summary map's capacity to 1.
        ss_map.set_max_stmt_count(1).unwrap();
        ss_map.clear();

        let mut sei0 = generate_any_exec_info();
        let mut sei1 = generate_any_exec_info();

        sei0.schema_name = "I'll occupy this cache! :(".to_owned();
        ss_map.add_statement(&sei0);
        let mut n = ss_map.begin_time_for_cur_interval();
        sei1.schema_name = "sorry, it's mine now. =)".to_owned();
        ss_map.add_statement(&sei1);

        assert_eq!(
            ss_map.to_evicted_count_datum()[0],
            evicted_count_row(n, n + interval, 1)
        );

        // Test multiple intervals.
        ss_map.clear();
        ss_map.set_refresh_interval(60);
        interval = ss_map.refresh_interval();
        ss_map.set_max_stmt_count(1).unwrap();
        ss_map.set_history_size(100);

        ss_map.set_begin_time_for_cur_interval(now + interval);
        // Insert one statement per interval.
        for _ in 0..50 {
            ss_map.add_statement(&generate_any_exec_info());
            ss_map.set_begin_time_for_cur_interval(
                ss_map.begin_time_for_cur_interval() + interval * 2,
            );
        }
        assert_eq!(ss_map.summary_map_size(), 1);
        let digest = ss_map.summary_map_values()[0].clone();
        assert_eq!(digest.lock().unwrap().history.len(), 50);

        ss_map.set_history_size(25);
        // Update the begin time.
        ss_map.set_begin_time_for_cur_interval(ss_map.begin_time_for_cur_interval() + interval * 2);
        let mut bandit_sei = generate_any_exec_info();
        bandit_sei.schema_name = "Kick you out >:(".to_owned();
        ss_map.add_statement(&bandit_sei);

        let evicted_count_datums = ss_map.to_evicted_count_datum();
        assert_eq!(evicted_count_datums.len(), 25);

        bandit_sei.schema_name = "Yet another kicker".to_owned();
        ss_map.add_statement(&bandit_sei);

        let evicted_count_datums = ss_map.to_evicted_count_datum();
        // Test a young digest.
        assert_eq!(evicted_count_datums.len(), 25);
        n = ss_map.begin_time_for_cur_interval();
        assert_eq!(
            evicted_count_datums[0],
            evicted_count_row(n, n + interval, 1)
        );
    }

    /// Go `TestEvictedCountDetailed`.
    #[test]
    fn test_evicted_count_detailed() {
        let ss_map = StmtSummaryByDigestMap::new();
        ss_map.clear();
        ss_map.set_refresh_interval(60);
        ss_map.set_history_size(100);
        let now = chrono::Utc::now().timestamp();
        let interval = 60_i64;
        ss_map.set_begin_time_for_cur_interval(now + interval);
        // Set the capacity to 1.
        ss_map.set_max_stmt_count(1).unwrap();

        // Test `stmtSummaryByDigest`'s history length.
        for i in 0..100 {
            if i == 0 {
                assert_eq!(ss_map.summary_map_size(), 0);
            } else {
                assert_eq!(ss_map.summary_map_size(), 1);
                let digest = ss_map.summary_map_values()[0].clone();
                assert_eq!(digest.lock().unwrap().history.len(), i);
            }
            ss_map.add_statement(&generate_any_exec_info());
            ss_map.set_begin_time_for_cur_interval(ss_map.begin_time_for_cur_interval() + interval);
        }
        ss_map.set_begin_time_for_cur_interval(ss_map.begin_time_for_cur_interval() - interval);

        let mut bandit_sei = generate_any_exec_info();
        bandit_sei.schema_name = "kick you out >:(".to_owned();
        ss_map.add_statement(&bandit_sei);
        let evicted_count_datums = ss_map.to_evicted_count_datum();
        let mut n = ss_map.begin_time_for_cur_interval();
        for evicted_count_datum in &evicted_count_datums {
            assert_eq!(*evicted_count_datum, evicted_count_row(n, n + 60, 1));
            n -= 60;
        }

        // Test more than one eviction in a single interval.
        bandit_sei.schema_name = "Yet another kicker".to_owned();
        let n = ss_map.begin_time_for_cur_interval();
        let expected_datum = evicted_count_row(n, n + 60, 2);
        ss_map.add_statement(&bandit_sei);
        let evicted_count_datums = ss_map.to_evicted_count_datum();
        assert_eq!(evicted_count_datums[0], expected_datum);

        ss_map.clear();
        let other = ss_map.evicted().unwrap();
        // Test poisoning with an empty-history digest value.
        other.lock().unwrap().add_evicted(
            Some(&StmtDigestKey::new()),
            Some(&StmtSummaryByDigest::default()),
            100,
        );
        assert_eq!(other.lock().unwrap().history_len(), 0);
    }

    /// Go `TestAddInfo`.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_add_info() {
        let now = chrono::Utc::now().timestamp();
        let mut add_to = StmtSummaryByDigestElement {
            begin_time: 0,
            end_time: 0,
            stats: StmtSummaryStats {
                // user
                auth_users: HashSet::from(["a".to_owned()]),

                // execCount and sumWarnings
                exec_count: 3,
                sum_warnings: 8,

                // latency
                sum_latency: ns(8),
                max_latency: ns(5),
                min_latency: ns(1),
                sum_parse_latency: ns(3),
                max_parse_latency: ns(2),
                sum_compile_latency: ns(3),
                max_compile_latency: ns(2),

                // coprocessor
                sum_num_cop_tasks: 4,
                max_cop_process_time: ns(4),
                max_cop_process_address: "19.19.8.10".to_owned(),
                max_cop_wait_time: ns(4),
                max_cop_wait_address: "19.19.8.10".to_owned(),

                // TiKV
                sum_process_time: ns(1),
                max_process_time: ns(1),
                sum_wait_time: ns(2),
                max_wait_time: ns(1),
                sum_backoff_time: ns(2),
                max_backoff_time: ns(2),

                sum_total_keys: 3,
                max_total_keys: 2,
                sum_processed_keys: 8,
                max_processed_keys: 4,
                sum_rocksdb_delete_skipped_count: 8,
                max_rocksdb_delete_skipped_count: 2,

                sum_rocksdb_key_skipped_count: 8,
                max_rocksdb_key_skipped_count: 3,
                sum_rocksdb_block_cache_hit_count: 8,
                max_rocksdb_block_cache_hit_count: 3,
                sum_rocksdb_block_read_count: 3,
                max_rocksdb_block_read_count: 3,
                sum_rocksdb_block_read_byte: 4,
                max_rocksdb_block_read_byte: 4,
                sum_ia_remote_read_segment_count: 8,
                max_ia_remote_read_segment_count: 3,

                // txn
                commit_count: 8,
                sum_prewrite_time: ns(3),
                max_prewrite_time: ns(3),
                sum_commit_time: ns(8),
                max_commit_time: ns(5),
                sum_get_commit_ts_time: ns(8),
                max_get_commit_ts_time: ns(8),
                sum_commit_backoff_time: 8,
                max_commit_backoff_time: 8,

                sum_resolve_lock_time: 8,
                max_resolve_lock_time: 8,
                sum_local_latch_time: ns(8),
                max_local_latch_time: ns(8),
                sum_write_keys: 8,
                max_write_keys: 8,
                sum_write_size: 8,
                max_write_size: 8,
                sum_prewrite_region_num: 8,
                max_prewrite_region_num: 8,
                sum_txn_retry: 8,
                max_txn_retry: 8,
                sum_backoff_times: 8,
                backoff_types: HashMap::new(),

                // plan cache
                plan_cache_hits: 8,

                // other
                sum_affected_rows: 8,
                sum_mem: 8,
                max_mem: 8,
                sum_mem_arbitration: 11.0,
                max_mem_arbitration: 11.0,
                sum_disk: 8,
                max_disk: 8,
                first_seen: unix_seconds(now - 10),
                last_seen: unix_seconds(now - 8),
                exec_retry_count: 8,
                exec_retry_time: ns(8),
                sum_kv_total: ns(2),
                sum_pd_total: ns(2),
                sum_backoff_total: ns(2),
                sum_write_sql_resp_total: ns(100),
                sum_errors: 8,
                ..StmtSummaryStats::default()
            },
        };

        let add_with = StmtSummaryByDigestElement {
            begin_time: 0,
            end_time: 0,
            stats: StmtSummaryStats {
                // user
                auth_users: HashSet::from(["a".to_owned(), "b".to_owned()]),

                // execCount and sumWarnings
                exec_count: 3,
                sum_warnings: 8,

                // latency
                sum_latency: ns(8),
                max_latency: ns(5),
                min_latency: ns(1),
                sum_parse_latency: ns(3),
                max_parse_latency: ns(2),
                sum_compile_latency: ns(3),
                max_compile_latency: ns(2),

                // coprocessor
                sum_num_cop_tasks: 4,
                max_cop_process_time: ns(15),
                max_cop_process_address: "1.14.5.14".to_owned(),
                max_cop_wait_time: ns(4),
                max_cop_wait_address: "19.19.8.10".to_owned(),

                // TiKV
                sum_process_time: ns(1),
                max_process_time: ns(1),
                sum_wait_time: ns(2),
                max_wait_time: ns(1),
                sum_backoff_time: ns(2),
                max_backoff_time: ns(2),

                sum_total_keys: 3,
                max_total_keys: 2,
                sum_processed_keys: 8,
                max_processed_keys: 4,
                sum_rocksdb_delete_skipped_count: 8,
                max_rocksdb_delete_skipped_count: 2,

                sum_rocksdb_key_skipped_count: 8,
                max_rocksdb_key_skipped_count: 3,
                sum_rocksdb_block_cache_hit_count: 8,
                max_rocksdb_block_cache_hit_count: 3,
                sum_rocksdb_block_read_count: 3,
                max_rocksdb_block_read_count: 3,
                sum_rocksdb_block_read_byte: 4,
                max_rocksdb_block_read_byte: 4,
                sum_ia_remote_read_segment_count: 8,
                max_ia_remote_read_segment_count: 5,

                // txn
                commit_count: 8,
                sum_prewrite_time: ns(3),
                max_prewrite_time: ns(3),
                sum_commit_time: ns(8),
                max_commit_time: ns(5),
                sum_get_commit_ts_time: ns(8),
                max_get_commit_ts_time: ns(8),
                sum_commit_backoff_time: 8,
                max_commit_backoff_time: 8,

                sum_resolve_lock_time: 8,
                max_resolve_lock_time: 8,
                sum_local_latch_time: ns(8),
                max_local_latch_time: ns(8),
                sum_write_keys: 8,
                max_write_keys: 8,
                sum_write_size: 8,
                max_write_size: 8,
                sum_prewrite_region_num: 8,
                max_prewrite_region_num: 8,
                sum_txn_retry: 8,
                max_txn_retry: 8,
                sum_backoff_times: 8,
                backoff_types: HashMap::new(),

                // plan cache
                plan_cache_hits: 8,

                // other
                sum_affected_rows: 8,
                sum_mem: 8,
                max_mem: 8,
                sum_disk: 8,
                max_disk: 8,
                sum_mem_arbitration: 13.0,
                max_mem_arbitration: 17.0,
                first_seen: unix_seconds(now - 20),
                last_seen: unix_seconds(now),
                exec_retry_count: 8,
                exec_retry_time: ns(8),
                sum_kv_total: ns(2),
                sum_pd_total: ns(2),
                sum_backoff_total: ns(2),
                sum_write_sql_resp_total: ns(100),
                sum_errors: 8,
                ..StmtSummaryStats::default()
            },
        };

        add_info(&mut add_to, &add_with);

        let expected_sum = StmtSummaryByDigestElement {
            begin_time: 0,
            end_time: 0,
            stats: StmtSummaryStats {
                // user
                auth_users: HashSet::from(["a".to_owned(), "b".to_owned()]),

                // execCount and sumWarnings
                exec_count: 6,
                sum_warnings: 16,

                // latency
                sum_latency: ns(16),
                max_latency: ns(5),
                min_latency: ns(1),
                sum_parse_latency: ns(6),
                max_parse_latency: ns(2),
                sum_compile_latency: ns(6),
                max_compile_latency: ns(2),

                // coprocessor
                sum_num_cop_tasks: 8,
                max_cop_process_time: ns(15),
                max_cop_process_address: "1.14.5.14".to_owned(),
                max_cop_wait_time: ns(4),
                max_cop_wait_address: "19.19.8.10".to_owned(),

                // TiKV
                sum_process_time: ns(2),
                max_process_time: ns(1),
                sum_wait_time: ns(4),
                max_wait_time: ns(1),
                sum_backoff_time: ns(4),
                max_backoff_time: ns(2),

                sum_total_keys: 6,
                max_total_keys: 2,
                sum_processed_keys: 16,
                max_processed_keys: 4,
                sum_rocksdb_delete_skipped_count: 16,
                max_rocksdb_delete_skipped_count: 2,

                sum_rocksdb_key_skipped_count: 16,
                max_rocksdb_key_skipped_count: 3,
                sum_rocksdb_block_cache_hit_count: 16,
                max_rocksdb_block_cache_hit_count: 3,
                sum_rocksdb_block_read_count: 6,
                max_rocksdb_block_read_count: 3,
                sum_rocksdb_block_read_byte: 8,
                max_rocksdb_block_read_byte: 4,
                sum_ia_remote_read_segment_count: 16,
                max_ia_remote_read_segment_count: 5,

                // txn
                commit_count: 16,
                sum_prewrite_time: ns(6),
                max_prewrite_time: ns(3),
                sum_commit_time: ns(16),
                max_commit_time: ns(5),
                sum_get_commit_ts_time: ns(16),
                max_get_commit_ts_time: ns(8),
                sum_commit_backoff_time: 16,
                max_commit_backoff_time: 8,

                sum_resolve_lock_time: 16,
                max_resolve_lock_time: 8,
                sum_local_latch_time: ns(16),
                max_local_latch_time: ns(8),
                sum_write_keys: 16,
                max_write_keys: 8,
                sum_write_size: 16,
                max_write_size: 8,
                sum_prewrite_region_num: 16,
                max_prewrite_region_num: 8,
                sum_txn_retry: 16,
                max_txn_retry: 8,
                sum_backoff_times: 16,
                backoff_types: HashMap::new(),

                // plan cache
                plan_cache_hits: 16,

                // other
                sum_affected_rows: 16,
                sum_mem: 16,
                max_mem: 8,
                sum_disk: 16,
                max_disk: 8,
                sum_mem_arbitration: 24.0,
                max_mem_arbitration: 17.0,
                first_seen: unix_seconds(now - 20),
                last_seen: unix_seconds(now),
                exec_retry_count: 16,
                exec_retry_time: ns(16),
                sum_kv_total: ns(4),
                sum_pd_total: ns(4),
                sum_backoff_total: ns(4),
                sum_write_sql_resp_total: ns(200),
                sum_errors: 16,
                ..StmtSummaryStats::default()
            },
        };

        assert_eq!(add_to, expected_sum);
    }
}
