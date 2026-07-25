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

//! Transcreation of Go `pkg/util/context/plancache.go`.

use std::sync::{Arc, Mutex, Once};

use super::warn::WarnAppender;
use super::WarnErr;

/// The flag of plan cache (Go `PlanCacheType`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum PlanCacheType {
    /// No cache.
    #[default]
    DefaultNoCache,
    /// Session prepared plan cache.
    SessionPrepared,
    /// Session non-prepared plan cache.
    SessionNonPrepared,
}

/// The number of fields captured by [`PlanCacheTracker::save`], for
/// documentation parity with Go's five-value return.
pub const PLAN_CACHE_TRACKER_SAVED_FIELDS: usize = 5;

#[derive(Default)]
struct TrackerState {
    use_cache: bool,
    cache_type: PlanCacheType,
    plan_cache_unqualified: String,
    // Force the optimizer to use plan cache even if there is risky
    // optimization, see #49736.
    force_plan_cache: bool,
    always_warn_skip_cache: bool,
}

/// Thread-safe plan-cache decision tracker (Go `PlanCacheTracker`).
pub struct PlanCacheTracker {
    state: Mutex<TrackerState>,
    warn_handler: Arc<dyn WarnAppender + Send + Sync>,
}

impl PlanCacheTracker {
    /// Creates a new tracker (Go `NewPlanCacheTracker`).
    pub fn new(warn_handler: Arc<dyn WarnAppender + Send + Sync>) -> Self {
        PlanCacheTracker {
            state: Mutex::new(TrackerState::default()),
            warn_handler,
        }
    }

    /// Outputs the reason why this query can't hit the plan cache.
    pub fn warn_skip_plan_cache(&self, reason: &str) {
        let mut state = self.state.lock().unwrap();
        if state.cache_type == PlanCacheType::DefaultNoCache {
            return;
        }
        self.warn_skip_locked(&mut state, reason);
    }

    /// Sets to skip the plan cache.
    pub fn set_skip_plan_cache(&self, reason: &str) {
        let mut state = self.state.lock().unwrap();
        if !state.use_cache {
            return;
        }

        if state.force_plan_cache {
            self.warn_handler.append_warning(WarnErr::Message(format!(
                "force plan-cache: may use risky cached plan: {reason}"
            )));
            return;
        }

        state.use_cache = false;
        self.warn_skip_locked(&mut state, reason);
    }

    fn warn_skip_locked(&self, state: &mut TrackerState, reason: &str) {
        state.plan_cache_unqualified = reason.to_string();
        match state.cache_type {
            PlanCacheType::DefaultNoCache => {
                self.warn_handler
                    .append_warning(WarnErr::from("unknown cache type"));
            }
            PlanCacheType::SessionPrepared => {
                self.warn_handler.append_warning(WarnErr::Message(format!(
                    "skip prepared plan-cache: {reason}"
                )));
            }
            PlanCacheType::SessionNonPrepared => {
                if state.always_warn_skip_cache {
                    // The source uses the literal to avoid an import cycle on
                    // types.ExplainFormatPlanCache.
                    self.warn_handler.append_warning(WarnErr::Message(format!(
                        "skip non-prepared plan-cache: {reason}"
                    )));
                }
            }
        }
    }

    /// Sets whether to always warn when skipping the plan cache; by default
    /// `SessionNonPrepared` skips silently.
    pub fn set_always_warn_skip_cache(&self, always: bool) {
        self.state.lock().unwrap().always_warn_skip_cache = always;
    }

    /// Sets the cache type.
    pub fn set_cache_type(&self, cache_type: PlanCacheType) {
        self.state.lock().unwrap().cache_type = cache_type;
    }

    /// Sets whether to force plan cache despite risky optimizations.
    pub fn set_force_plan_cache(&self, force: bool) {
        self.state.lock().unwrap().force_plan_cache = force;
    }

    /// Sets to use the plan cache.
    pub fn enable_plan_cache(&self) {
        self.state.lock().unwrap().use_cache = true;
    }

    /// Captures the mutable planning-time state (Go `Save`'s five values).
    #[must_use]
    pub fn save(&self) -> (bool, PlanCacheType, String, bool, bool) {
        let state = self.state.lock().unwrap();
        (
            state.use_cache,
            state.cache_type,
            state.plan_cache_unqualified.clone(),
            state.force_plan_cache,
            state.always_warn_skip_cache,
        )
    }

    /// Restores the mutable planning-time state (Go `Restore`).
    pub fn restore(
        &self,
        use_cache: bool,
        cache_type: PlanCacheType,
        plan_cache_unqualified: String,
        force_plan_cache: bool,
        always_warn_skip_cache: bool,
    ) {
        let mut state = self.state.lock().unwrap();
        state.use_cache = use_cache;
        state.cache_type = cache_type;
        state.plan_cache_unqualified = plan_cache_unqualified;
        state.force_plan_cache = force_plan_cache;
        state.always_warn_skip_cache = always_warn_skip_cache;
    }

    /// Returns whether to use the plan cache.
    #[must_use]
    pub fn use_cache(&self) -> bool {
        self.state.lock().unwrap().use_cache
    }

    /// Returns the reason why the plan cache is unqualified.
    #[must_use]
    pub fn plan_cache_unqualified(&self) -> String {
        self.state.lock().unwrap().plan_cache_unqualified.clone()
    }
}

/// Handles range fallback: when there are too many ranges it falls back and
/// warns once (Go `RangeFallbackHandler`, thread-safe).
pub struct RangeFallbackHandler {
    plan_cache_tracker: Arc<PlanCacheTracker>,
    warn_handler: Arc<dyn WarnAppender + Send + Sync>,
    report_range_fallback_warning: Once,
}

impl RangeFallbackHandler {
    /// Creates a new handler (Go `NewRangeFallbackHandler`).
    pub fn new(
        plan_cache_tracker: Arc<PlanCacheTracker>,
        warn_handler: Arc<dyn WarnAppender + Send + Sync>,
    ) -> Self {
        RangeFallbackHandler {
            plan_cache_tracker,
            warn_handler,
            report_range_fallback_warning: Once::new(),
        }
    }

    /// Records the range fallback event: skips the plan cache (a fallback plan
    /// is probably suboptimal) and warns exactly once.
    pub fn record_range_fallback(&self, range_max_size: i64) {
        self.plan_cache_tracker
            .set_skip_plan_cache("in-list is too long");
        self.report_range_fallback_warning.call_once(|| {
            self.warn_handler.append_warning(WarnErr::Message(format!(
                "Memory capacity of {range_max_size} bytes for 'tidb_opt_range_max_size' \
                 exceeded when building ranges. Less accurate ranges such as full range are chosen"
            )));
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // Go ships no plancache test; these pin the observable warning/state
    // contract of PlanCacheTracker and RangeFallbackHandler.
    #[derive(Default)]
    struct Collector {
        warns: Mutex<Vec<(String, String)>>,
    }

    impl WarnAppender for Collector {
        fn append_warning(&self, err: WarnErr) {
            self.warns
                .lock()
                .unwrap()
                .push(("Warning".into(), err.to_string()));
        }
        fn append_note(&self, err: WarnErr) {
            self.warns
                .lock()
                .unwrap()
                .push(("Note".into(), err.to_string()));
        }
    }

    #[test]
    fn plan_cache_tracker_contract() {
        let collector = Arc::new(Collector::default());
        let tracker = PlanCacheTracker::new(collector.clone());

        // Skip before enabling: no state change, no warning.
        tracker.set_skip_plan_cache("early");
        assert!(!tracker.use_cache());
        assert!(collector.warns.lock().unwrap().is_empty());

        // Prepared cache: skipping records the reason and warns.
        tracker.enable_plan_cache();
        tracker.set_cache_type(PlanCacheType::SessionPrepared);
        assert!(tracker.use_cache());
        tracker.set_skip_plan_cache("has sub-queries");
        assert!(!tracker.use_cache());
        assert_eq!(tracker.plan_cache_unqualified(), "has sub-queries");
        assert_eq!(
            collector.warns.lock().unwrap().last().unwrap().1,
            "skip prepared plan-cache: has sub-queries"
        );

        // Force plan cache: skip request downgrades to a risk warning and the
        // cache stays on.
        tracker.enable_plan_cache();
        tracker.set_force_plan_cache(true);
        tracker.set_skip_plan_cache("risky");
        assert!(tracker.use_cache());
        assert_eq!(
            collector.warns.lock().unwrap().last().unwrap().1,
            "force plan-cache: may use risky cached plan: risky"
        );

        // Non-prepared cache warns only when always_warn_skip_cache is set.
        tracker.set_force_plan_cache(false);
        tracker.set_cache_type(PlanCacheType::SessionNonPrepared);
        let before = collector.warns.lock().unwrap().len();
        tracker.set_skip_plan_cache("np quiet");
        assert_eq!(collector.warns.lock().unwrap().len(), before);
        tracker.enable_plan_cache();
        tracker.set_always_warn_skip_cache(true);
        tracker.set_skip_plan_cache("np loud");
        assert_eq!(
            collector.warns.lock().unwrap().last().unwrap().1,
            "skip non-prepared plan-cache: np loud"
        );

        // WarnSkipPlanCache is a no-op for DefaultNoCache.
        tracker.set_cache_type(PlanCacheType::DefaultNoCache);
        let before = collector.warns.lock().unwrap().len();
        tracker.warn_skip_plan_cache("nope");
        assert_eq!(collector.warns.lock().unwrap().len(), before);

        // Save/restore round-trips the five fields.
        let saved = tracker.save();
        tracker.restore(true, PlanCacheType::SessionPrepared, "x".into(), true, true);
        assert!(tracker.use_cache());
        tracker.restore(saved.0, saved.1, saved.2.clone(), saved.3, saved.4);
        assert_eq!(tracker.plan_cache_unqualified(), saved.2);
    }

    #[test]
    fn range_fallback_warns_once() {
        let collector = Arc::new(Collector::default());
        let tracker = Arc::new(PlanCacheTracker::new(collector.clone()));
        tracker.enable_plan_cache();
        tracker.set_cache_type(PlanCacheType::SessionPrepared);
        let handler = RangeFallbackHandler::new(tracker.clone(), collector.clone());

        handler.record_range_fallback(1024);
        handler.record_range_fallback(1024);

        let warns = collector.warns.lock().unwrap();
        // One skip warning (second call is a no-op because use_cache is off)
        // plus exactly ONE fallback warning despite two records.
        let fallback: Vec<_> = warns
            .iter()
            .filter(|(_, m)| m.contains("tidb_opt_range_max_size"))
            .collect();
        assert_eq!(fallback.len(), 1);
        assert!(fallback[0].1.contains("1024 bytes"));
        assert!(!tracker.use_cache());
        assert_eq!(tracker.plan_cache_unqualified(), "in-list is too long");
    }
}
