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
