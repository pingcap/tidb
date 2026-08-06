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

//! `pkg/executor/aggregate/agg_spill.go`'s `AggSpillDiskAction`: the
//! `ActionOnExceed` the SERIAL hash aggregation registers.
//!
//! # Which of Go's two spill mechanisms this is
//!
//! `agg_spill.go` holds two, and they are not variants of one design:
//!
//! * `parallelHashAggSpillHelper` + `ParallelAggSpillDiskAction` partition the
//!   PARTIAL RESULTS across 256 buckets, write each bucket's serialized
//!   partial results out, and restore them later by MERGING partial results
//!   (`restoreFromOneSpillFile` -> `MergePartialResult`). It needs the merge
//!   because several partial workers can each hold a partial result for the
//!   same group key.
//! * `AggSpillDiskAction` (this one) belongs to `unparallelExec`. It spills
//!   NOTHING of the aggregation state. It sets one flag, `inSpillMode`, and
//!   the serial loop then writes the UNPROCESSED INPUT ROWS of groups it has
//!   not opened yet back to disk (`spillUnprocessedData`), finishes and emits
//!   the groups it does hold, and re-reads the spilled rows as a fresh ROUND
//!   (`resetSpillMode` + `getNextChunk`). No partial result is ever
//!   serialized, and no two partial results for one key ever have to be
//!   merged: a group is opened in exactly one round and completed there.
//!
//! This tier's [`crate::hash_agg::HashAggExec`] is Go's `unparallelExec`, so
//! it is this arm that is ported -- the same choice the join's spill made when
//! it took Go's v1 container.
//!
//! # The soft limit, not the hard one
//!
//! Go registers this action with `FallbackOldAndSetNewActionForSoftLimit`
//! (`initForUnparallelExec`), so it fires at 80% of `tidb_mem_quota_query`
//! (`memory.SOFT_SCALE`), NOT at the quota itself. The cancellation still sits
//! on the hard-limit slot and still fires at 100% if spilling did not bring
//! the aggregation back under the quota. That is why an over-quota `GROUP BY`
//! can spill and finish while the 8175 path remains reachable.
//!
//! The shared `tidb-chunk` spill container now implements Go's optional
//! `aes128-ctr` file stack. The bounded server still has no top-level config
//! loader to call that container's process-wide config seam, so its startup
//! remains plaintext and rejects unsupported command-line options loudly.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::SeqCst};
use std::sync::Arc;

use tidb_util::memory::{ActionOnExceed, ArcAction, BaseOomAction, Tracker, DEF_SPILL_PRIORITY};

/// Go `maxSpillTimes`: how many times one aggregation may enter spill mode.
/// Past this the action stops spilling and falls through to the cancellation,
/// so a query that keeps overrunning fails rather than writing forever.
pub const MAX_SPILL_TIMES: u32 = 10;

/// Go `hasEnoughDataToSpill`: spill only once the aggregation itself holds at
/// least a FIFTH of the quota, so a query whose overrun comes from some other
/// operator does not make the aggregation open a spill file per chunk.
pub fn has_enough_data_to_spill(agg_tracker: &Tracker, triggered: &Tracker) -> bool {
    agg_tracker.bytes_consumed() >= triggered.get_bytes_limit() / 5
}

/// Go `AggSpillDiskAction`: raises the aggregation's `inSpillMode` flag.
pub struct AggSpillDiskAction {
    base: BaseOomAction,
    /// Go `HashAggExec.inSpillMode`, read by the serial loop at the next row.
    in_spill_mode: Arc<AtomicBool>,
    /// Go `AggSpillDiskAction.spillTimes`.
    spill_times: AtomicU32,
    /// Go `a.e.memTracker`: the aggregation's own tracker.
    agg_tracker: Arc<Tracker>,
}

impl AggSpillDiskAction {
    /// Builds the action and the flag the aggregation's loop polls.
    #[must_use]
    pub fn new(agg_tracker: &Arc<Tracker>) -> (Arc<AggSpillDiskAction>, Arc<AtomicBool>) {
        let in_spill_mode = Arc::new(AtomicBool::new(false));
        let action = Arc::new(AggSpillDiskAction {
            base: BaseOomAction::default(),
            in_spill_mode: Arc::clone(&in_spill_mode),
            spill_times: AtomicU32::new(0),
            agg_tracker: Arc::clone(agg_tracker),
        });
        (action, in_spill_mode)
    }

    /// How many times this action has entered spill mode (Go `spillTimes`).
    #[must_use]
    pub fn spill_times(&self) -> u32 {
        self.spill_times.load(SeqCst)
    }
}

impl ActionOnExceed for AggSpillDiskAction {
    /// Go `AggSpillDiskAction.Action`, condition for condition: not already in
    /// spill mode, enough data of our own to be worth a file, and under
    /// `maxSpillTimes`. Otherwise the fallback runs -- which for a `CANCEL`
    /// session ends in the 8175 cancellation.
    fn action(&self, t: &Arc<Tracker>) {
        if !self.in_spill_mode.load(SeqCst)
            && has_enough_data_to_spill(&self.agg_tracker, t)
            && self.spill_times.load(SeqCst) < MAX_SPILL_TIMES
        {
            self.spill_times.fetch_add(1, SeqCst);
            tracing::info!(
                spill_times = self.spill_times.load(SeqCst),
                consumed = t.bytes_consumed(),
                quota = t.get_bytes_limit(),
                "memory exceeds quota, set aggregate mode to spill-mode"
            );
            self.in_spill_mode.store(true, SeqCst);
            return;
        }
        if let Some(fallback) = self.get_fallback() {
            fallback.action(t);
        }
    }

    fn set_fallback(&self, a: Option<ArcAction>) {
        self.base.set_fallback(a);
    }

    fn get_fallback(&self) -> Option<ArcAction> {
        self.base.get_fallback()
    }

    /// Go `AggSpillDiskAction.GetPriority`.
    fn get_priority(&self) -> i64 {
        DEF_SPILL_PRIORITY
    }

    fn set_finished(&self) {
        self.base.set_finished();
    }

    fn is_finished(&self) -> bool {
        self.base.is_finished()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_util::memory::LABEL_FOR_SESSION;

    /// Go `hasEnoughDataToSpill` is a FIFTH of the limit, and it reads the
    /// aggregation's tracker while the limit comes from the tracker that
    /// overran -- a query can overrun on another operator's bytes.
    #[test]
    fn a_fifth_of_the_quota_is_the_bar() {
        let root = Tracker::new(LABEL_FOR_SESSION, 1000);
        let agg = Tracker::new(1, -1);
        agg.attach_to(&root);
        agg.consume(199);
        assert!(!has_enough_data_to_spill(&agg, &root));
        agg.consume(1);
        assert!(has_enough_data_to_spill(&agg, &root));
    }

    #[test]
    fn the_action_raises_spill_mode_only_once_and_at_most_ten_times() {
        let root = Tracker::new(LABEL_FOR_SESSION, 1000);
        let agg = Tracker::new(1, -1);
        agg.attach_to(&root);
        agg.consume(1000);
        let (action, flag) = AggSpillDiskAction::new(&agg);

        action.action(&root);
        assert!(flag.load(SeqCst));
        assert_eq!(action.spill_times(), 1);
        // Already in spill mode: the action does nothing until the round ends
        // and the aggregation lowers the flag.
        action.action(&root);
        assert_eq!(action.spill_times(), 1);

        for _ in 0..20 {
            flag.store(false, SeqCst);
            action.action(&root);
        }
        assert_eq!(action.spill_times(), MAX_SPILL_TIMES);
        assert!(
            !flag.load(SeqCst),
            "past maxSpillTimes the action must stop spilling"
        );
    }

    /// Too little data of our own: the action must hand over to the fallback
    /// (the cancellation), not swallow the overrun.
    #[test]
    fn too_little_data_falls_through_to_the_fallback() {
        struct Counting {
            base: BaseOomAction,
            hits: AtomicU32,
        }
        impl ActionOnExceed for Counting {
            fn action(&self, _t: &Arc<Tracker>) {
                self.hits.fetch_add(1, SeqCst);
            }
            fn set_fallback(&self, a: Option<ArcAction>) {
                self.base.set_fallback(a);
            }
            fn get_fallback(&self) -> Option<ArcAction> {
                self.base.get_fallback()
            }
            fn get_priority(&self) -> i64 {
                DEF_SPILL_PRIORITY
            }
            fn set_finished(&self) {
                self.base.set_finished();
            }
            fn is_finished(&self) -> bool {
                self.base.is_finished()
            }
        }

        let root = Tracker::new(LABEL_FOR_SESSION, 1000);
        let agg = Tracker::new(1, -1);
        agg.attach_to(&root);
        agg.consume(10);
        let (action, flag) = AggSpillDiskAction::new(&agg);
        let counting = Arc::new(Counting {
            base: BaseOomAction::default(),
            hits: AtomicU32::new(0),
        });
        action.set_fallback(Some(counting.clone() as ArcAction));
        action.action(&root);
        assert!(!flag.load(SeqCst), "10 bytes is not worth a spill file");
        assert_eq!(counting.hits.load(SeqCst), 1);
    }
}
