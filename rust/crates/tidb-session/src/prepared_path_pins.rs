//! The prepared plan cache's REUSABLE half: per-statement access-path pins.
//!
//! [`crate::prepared_plan_cache`] ports the observable contract -- which
//! statements Go's prepared plan cache would admit, and whether an EXECUTE
//! would have hit. This module adds the one piece of actual reuse this tier
//! can honour without a reified plan object: the ACCESS-PATH SHAPE each join
//! leaf committed to on the statement's first execution.
//!
//! Go caches a whole physical plan and rebinds its ranges per execute. Here
//! every range, constant fold and residual split is still freshly derived
//! from the CURRENT parameters on every execution; what is pinned is only
//! which candidate WON the cost race originally. That is exactly the property
//! Go's cache exhibits in mixed workloads -- an execution whose literals
//! would flip the cost race (a wider date range that makes a secondary index
//! look selective under a bad estimate) replays the first winner instead of
//! flipping. Correctness never depends on the pin: candidates are built from
//! the same pushed conditions with the same residual handling, so forcing
//! one changes only cost, never the answer.
//!
//! # Lifecycle
//!
//! * `begin_prepared_path_pins` runs before the statement dispatches: it
//!   checks the same gates Go does (cache enabled, AST cacheable, no session
//!   bindings), builds the SAME key [`PreparedPlanKey`] uses, and either
//!   installs the stored pins (hit) or a fresh capture sink (miss).
//! * The sink fills during planning, from [`tidb_executor::StmtContext`].
//! * `finish_prepared_path_pins` runs after the statement: a SUCCESSFUL miss
//!   stores its captured map as the statement's pins; a failed or empty one
//!   stores nothing, so a statement that never planned a join leaf simply
//!   has no pins and behaves as before.
//!
//! # Keying and invalidation
//!
//! Entries are keyed by the prepared statement's SQL TEXT within this
//! session, and carry the full [`PreparedPlanKey`] (schema version, database,
//! sql_mode, time zone, push-down blacklist generation). Any of those moving
//! is a miss: the next execution replans freely and re-captures, which is
//! Go's invalidate-and-replan. The store is bounded; beyond the bound a new
//! entry evicts the oldest.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tidb_ast::Stmt;
use tidb_executor::PinnedLeafAccess;

use crate::prepared_plan_cache::PreparedPlanKey;
use crate::Session;

/// One prepared statement's committed access paths, keyed by the leaf name
/// the executor plans it under (stable across executions of one statement:
/// both come from the statement's own naming).
#[derive(Clone, Debug)]
pub(crate) struct PreparedPathPinEntry {
    pub(crate) key: PreparedPlanKey,
    pub(crate) pins: HashMap<String, PinnedLeafAccess>,
}

/// What one in-flight prepared execution carries into its statement context.
pub(crate) struct ActivePreparedPinState {
    /// Hit: replay these pins.
    pub(crate) apply: Option<Arc<HashMap<String, PinnedLeafAccess>>>,
    /// Miss: record this execution's winners here.
    pub(crate) capture: Option<Arc<Mutex<Option<HashMap<String, PinnedLeafAccess>>>>>,
    /// The key THIS execution planned against, stored with the captured map.
    pub(crate) key: PreparedPlanKey,
}

/// Bound on distinct pinned statements per session. Go bounds its cache by
/// memory; a count bound reaches the same "one connection cannot grow this
/// forever" answer with none of the accounting.
const MAX_PINNED_STATEMENTS: usize = 100;

impl Session {
    /// Opens the pin state for one prepared SELECT execution, or `None` when
    /// the statement must plan freely (not a query, cache disabled, bindings
    /// present, or no valid entry for the current key).
    pub(crate) fn begin_prepared_path_pins(
        &mut self,
        stmt: &mut Stmt,
        sql: &str,
    ) -> Option<ActivePreparedPinState> {
        if !matches!(stmt, Stmt::Query(_)) {
            return None;
        }
        if !self.prepared_plan_cache_enabled() {
            return None;
        }
        // A binding exists to change this statement's plan; letting a pin
        // outrank it would make CREATE BINDING a no-op. Go's cache refuses to
        // answer from the cache while baselines are being set up too.
        if self.has_session_bindings() {
            return None;
        }
        if super::prepared_plan_cache::stmt_cacheable(stmt).is_err() {
            return None;
        }
        let key = self.prepared_plan_key();
        let store = self.prepared_plan_pins.borrow_mut();
        let apply = match store.get(sql) {
            Some(entry) if entry.key == key => Some(Arc::new(entry.pins.clone())),
            _ => None,
        };
        if apply.is_some() {
            return Some(ActivePreparedPinState {
                apply,
                capture: None,
                key,
            });
        }
        // A stale entry (key moved) is replaced by what this run captures.
        Some(ActivePreparedPinState {
            apply: None,
            capture: Some(Arc::new(Mutex::new(Some(HashMap::new())))),
            key,
        })
    }

    /// Closes the pin state after the statement finished. Only a successful
    /// MISS with a non-empty capture stores anything; hits keep their entry
    /// untouched, and failures publish nothing.
    pub(crate) fn finish_prepared_path_pins(&self, sql: &str, outcome_ok: bool) {
        let state = self.active_prepared_pin.borrow_mut().take();
        let Some(state) = state else {
            return;
        };
        if !outcome_ok {
            return;
        }
        let Some(sink) = state.capture else {
            return;
        };
        let pins = sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(pins) = pins else {
            return;
        };
        if pins.is_empty() {
            // No join leaf planned under this statement: nothing to pin.
            return;
        }
        let mut store = self.prepared_plan_pins.borrow_mut();
        if !store.contains_key(sql) && store.len() >= MAX_PINNED_STATEMENTS {
            let oldest = store.keys().next().cloned();
            if let Some(oldest) = oldest {
                store.remove(&oldest);
            }
        }
        store.insert(
            sql.to_owned(),
            PreparedPathPinEntry {
                key: state.key,
                pins,
            },
        );
    }

    /// Whether a pin state is open for the statement now running.
    #[cfg(test)]
    pub(crate) fn active_prepared_pin_is_open(&self) -> bool {
        self.active_prepared_pin.borrow().is_some()
    }
}
