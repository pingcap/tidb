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

//! Go `kv_exec_count.go`: counting SQL executions in the kv dimension.
//!
//! The statement-owned handle is captured before executor admission and bound
//! to ordinary, coprocessor and MPP request dispatch. It preserves Go's admission
//! and per-RPC enable checks without introducing another transport wrapper.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use super::stmtstats::{new_sql_plan_digest, SqlPlanDigest, StatementStats};
use crate::topsql_state::top_sql_enabled;

/// Statement-owned reference captured during construction and initialized at
/// executor admission. Copies share the original counter; a new statement gets
/// a new reference even when the transaction and transport are reused.
#[derive(Clone, Debug, Default)]
pub struct KvExecCounterHandle(Arc<std::sync::OnceLock<Arc<KvExecCounter>>>);

impl PartialEq for KvExecCounterHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for KvExecCounterHandle {}

impl KvExecCounterHandle {
    /// Publishes Go's single execution counter at executor admission.
    pub fn initialize(&self, stats: &Arc<StatementStats>, sql: &[u8], plan: &[u8]) {
        self.0
            .get_or_init(|| stats.create_kv_exec_counter(sql, plan));
    }

    /// Runs the source interceptor's current-enable check before marking a target.
    pub fn mark(&self, target: &str) {
        if top_sql_enabled() {
            if let Some(counter) = self.0.get() {
                counter.mark(target);
            }
        }
    }
}

/// Go `KvExecCounter`: counts the number of SQL executions of the kv layer.
///
/// It calls `StatementStats::add_kv_exec_count` at the right time so that the
/// "SQL execution count of TiKV" semantic holds.
#[derive(Debug)]
pub struct KvExecCounter {
    stats: Arc<StatementStats>,
    /// Go's `marked map[string]struct{}` — a `HashSet<Target>`.
    marked: Mutex<HashSet<String>>,
    digest: SqlPlanDigest,
}

impl StatementStats {
    /// Go `StatementStats.CreateKvExecCounter`: creates an associated
    /// [`KvExecCounter`].
    ///
    /// The created counter can only be used during a single statement
    /// execution and cannot be reused.
    pub fn create_kv_exec_counter(
        self: &Arc<Self>,
        sql_digest: &[u8],
        plan_digest: &[u8],
    ) -> Arc<KvExecCounter> {
        Arc::new(KvExecCounter {
            stats: Arc::clone(self),
            digest: new_sql_plan_digest(sql_digest, plan_digest),
            marked: Mutex::new(HashSet::new()),
        })
    }
}

impl KvExecCounter {
    /// Go's `mark`: marks this target during the current execution of the
    /// statement. If the target is marked for the first time, the number of
    /// executions is increased. Thread-safe.
    pub fn mark(&self, target: &str) {
        let first_mark = self
            .marked
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(target.to_owned());
        if first_mark {
            self.stats.add_kv_exec_count(
                self.digest.sql_digest.as_bytes(),
                self.digest.plan_digest.as_bytes(),
                target,
                1,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::stmtstats::create_statement_stats;
    use super::super::test_support::{global_test_guard, reset_topsql_state, sql_plan_digest};
    use crate::topsql_state::enable_top_sql;

    // Go `TestKvExecCounter`.
    #[test]
    fn kv_exec_counter() {
        let _guard = global_test_guard();
        reset_topsql_state();
        enable_top_sql();
        let stats = create_statement_stats();
        let counter = super::KvExecCounterHandle::default();
        counter.initialize(&stats, b"SQL-1", b"");
        for _ in 0..10 {
            counter.mark("TIKV-1");
            counter.clone().mark("TIKV-2");
        }
        crate::topsql_state::disable_top_sql();
        counter.mark("TIKV-3");
        let inner = stats.lock();
        let data = &inner.data;
        assert!(data.contains_key(&sql_plan_digest("SQL-1", "")));
        let counts = &data[&sql_plan_digest("SQL-1", "")]
            .kv_stats_item
            .kv_exec_count;
        assert_eq!(counts.len(), 2);
        assert_eq!(counts["TIKV-1"], 1);
        assert_eq!(counts["TIKV-2"], 1);
        reset_topsql_state();
    }
}
