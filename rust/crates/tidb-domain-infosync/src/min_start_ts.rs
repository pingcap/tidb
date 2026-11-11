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

//! Go `InfoSyncer.ReportMinStartTS` (`pkg/domain/infosync/info.go`), the
//! part that walks `sm.ShowProcessList()`.

use std::sync::Arc;

use tidb_sessmgr::processinfo::ProcessInfo;

/// Go `ReportMinStartTS`'s `for _, info := range pl` loop: the start
/// timestamps the process list holds back GC with.
///
/// A session whose statement waits on a queued DDL job (`StmtCtx.
/// IsDDLJobInQueue`) is skipped whole, cursors included. Every other session
/// contributes its `CurTxnStartTS` and the `StartTS` of every cursor its
/// `CursorTracker` holds open. Go folds each candidate into `minStartTS` when
/// it is above `startTSLowerLimit` and below the running minimum;
/// `tidb_txnkv::report_min_start_ts` applies that same fold to the list this
/// returns.
#[must_use]
pub fn process_list_min_start_ts_candidates(pl: &[Arc<ProcessInfo>]) -> Vec<u64> {
    let mut candidates = Vec::with_capacity(pl.len());
    for info in pl {
        if info.is_ddl_job_in_queue {
            // Ignore DDL sessions.
            continue;
        }
        candidates.push(info.cur_txn_start_ts);
        if let Some(tracker) = &info.cursor_tracker {
            tracker.range_cursor(|cursor| {
                candidates.push(cursor.state().start_ts);
                true
            });
        }
    }
    candidates
}

/// Go `infoschemaMinTS`.
pub trait InfoschemaMinTs {
    /// Go `GetAndResetRecentInfoSchemaTS`.
    fn get_and_reset_recent_info_schema_ts(&self, now: u64) -> u64;
}

/// Go `ReportMinStartTS`'s computation of `is.minStartTS`: the lower limit
/// of the start timestamps, so that an extremely old transaction does not
/// delay GC. `active` holds the start timestamps of the storage transactions
/// and cursors this process tracks beside its process list.
#[must_use]
pub fn min_start_ts(
    pl: &[Arc<ProcessInfo>],
    inner_session_start_ts_list: &[u64],
    mut active: Vec<u64>,
    current_ver: u64,
    gc_max_wait_time: u64,
    info_cache: &dyn InfoschemaMinTs,
    inner: &tidb_txnkv::InnerTxnStartTsBox,
) -> u64 {
    let now = tikv_client::oracle::get_time_from_ts(current_ver);
    active.extend(process_list_min_start_ts_candidates(pl));
    for inner_ts in inner_session_start_ts_list {
        let _ = tidb_txnkv::print_long_time_internal_txn(now, *inner_ts, false);
        active.push(*inner_ts);
    }
    active.push(info_cache.get_and_reset_recent_info_schema_ts(current_ver));
    tidb_txnkv::report_min_start_ts(current_ver, gc_max_wait_time, active, inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_session_cursor::{CursorTracker, State};

    fn process(cur_txn_start_ts: u64, ddl: bool, tracker: Option<CursorTracker>) -> Arc<ProcessInfo> {
        Arc::new(ProcessInfo {
            cur_txn_start_ts,
            is_ddl_job_in_queue: ddl,
            cursor_tracker: tracker,
            ..ProcessInfo::default()
        })
    }

    // Go `TestCursorWillBlockMinStartTS`
    // (`pkg/executor/staticrecordset/integration_test.go`): while a detached
    // cursor is open its start TS is the reported minimum even though the
    // session has moved on to a later transaction; once the cursor closes the
    // later transaction's start TS is.
    #[test]
    fn test_cursor_will_block_min_start_ts() {
        let now = tikv_client::oracle::compose_ts(1_700_000_000_000, 0);
        let initial_start_ts = now - (10_000 << 18);
        let second_start_ts = now - (5_000 << 18);
        let tracker = CursorTracker::new();
        let cursor = tracker.new_cursor(State {
            start_ts: initial_start_ts,
        });
        let pl = vec![process(second_start_ts, false, Some(tracker.clone()))];
        let inner = tidb_txnkv::InnerTxnStartTsBox::new();
        let report = |pl: &[Arc<ProcessInfo>]| {
            tidb_txnkv::report_min_start_ts(
                now,
                86_400,
                process_list_min_start_ts_candidates(pl),
                &inner,
            )
        };
        assert_eq!(report(&pl), initial_start_ts);
        cursor.close();
        assert_eq!(report(&pl), second_start_ts);
    }

    struct RecentMinTs(std::sync::atomic::AtomicU64);

    impl InfoschemaMinTs for RecentMinTs {
        fn get_and_reset_recent_info_schema_ts(&self, now: u64) -> u64 {
            self.0.swap(now, std::sync::atomic::Ordering::AcqRel)
        }
    }

    // Go `ReportMinStartTS`: `infoCache.GetAndResetRecentInfoSchemaTS(
    // currentVer.Ver)` hands back the TS the round before left (zero on the
    // first round, which is below the lower limit), so with nothing else
    // active a round reports the round before's current version.
    #[test]
    fn a_round_reports_the_recent_info_schema_ts_of_the_round_before_like_go() {
        let first = tikv_client::oracle::compose_ts(1_700_000_000_000, 7);
        let second = tikv_client::oracle::compose_ts(1_700_000_030_000, 9);
        let third = tikv_client::oracle::compose_ts(1_700_000_060_000, 0);
        let cache = RecentMinTs(std::sync::atomic::AtomicU64::new(0));
        let inner = tidb_txnkv::InnerTxnStartTsBox::new();
        let report = |version: u64, active: Vec<u64>| min_start_ts(&[], &[], active, version, 86_400, &cache, &inner);
        // `oracle.GoTimeToTS(now)`: the logical bits are gone.
        assert_eq!(report(first, Vec::new()), tikv_client::oracle::compose_ts(1_700_000_000_000, 0));
        assert_eq!(report(second, Vec::new()), first);
        // An older transaction is the minimum.
        let transaction = second - (1_000 << 18);
        assert_eq!(report(third, vec![transaction]), transaction);
        // A recent TS at or below the lower limit is not one.
        let late = tikv_client::oracle::compose_ts(1_700_000_060_000 + 86_400_000 + 1, 0);
        assert_eq!(report(late, Vec::new()), late);
    }

    // Go `TestDDLInsideTXNNotBlockMinStartTS`: a session waiting on its queued
    // DDL job is ignored, and so are the cursors it holds.
    #[test]
    fn test_ddl_session_and_its_cursors_do_not_block_min_start_ts() {
        let tracker = CursorTracker::new();
        tracker.new_cursor(State { start_ts: 5 });
        let pl = vec![
            process(10, true, Some(tracker)),
            process(30, false, None),
        ];
        assert_eq!(process_list_min_start_ts_candidates(&pl), vec![30]);
    }
}
