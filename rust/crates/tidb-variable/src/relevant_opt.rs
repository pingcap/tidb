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

//! Go `SessionVars.RecordRelevantOptVarsAndFixes`, `RelevantOptVars` and
//! `RelevantOptFixes` with `ResetRelevantOptVarsAndFixes`,
//! `RecordRelevantOptVar` and `RecordRelevantOptFix`: while a statement is
//! optimized for `EXPLAIN EXPLORE`'s plan generation, every optimizer
//! variable and fix control the optimizer consults is recorded.
//!
//! Go's planner reaches them through `sctx.GetSessionVars()` wherever it
//! reads the variable; this planner receives the variables' values as plain
//! data, so the record is kept for the planning thread: [`record_scope`]
//! starts it (Go `ResetRelevantOptVarsAndFixes(true)`) and its guard ends it
//! (`ResetRelevantOptVarsAndFixes(false)`). Optimization runs on the calling
//! thread, as Go's does on the session's goroutine.

use std::cell::RefCell;
use std::collections::BTreeSet;

#[derive(Default)]
struct Recorded {
    vars: BTreeSet<String>,
    fixes: BTreeSet<u64>,
}

thread_local! {
    static RECORDING: RefCell<Option<Recorded>> = const { RefCell::new(None) };
}

/// The recording [`record_scope`] started; dropping it stops recording.
pub struct RecordScope {
    previous: Option<Recorded>,
}

impl RecordScope {
    /// Go's `RelevantOptVars` keys sorted (`sort.Strings`) and
    /// `RelevantOptFixes` keys sorted, as `recordRelevantOptVarsAndFixes`
    /// answers them.
    #[must_use]
    pub fn recorded(&self) -> (Vec<String>, Vec<u64>) {
        RECORDING.with(|recording| {
            recording.borrow().as_ref().map_or_else(Default::default, |recorded| {
                (
                    recorded.vars.iter().cloned().collect(),
                    recorded.fixes.iter().copied().collect(),
                )
            })
        })
    }
}

impl Drop for RecordScope {
    fn drop(&mut self) {
        RECORDING.with(|recording| *recording.borrow_mut() = self.previous.take());
    }
}

/// Go `ResetRelevantOptVarsAndFixes(true)`: records from now until the
/// returned scope is dropped (`ResetRelevantOptVarsAndFixes(false)`).
#[must_use]
pub fn record_scope() -> RecordScope {
    RecordScope {
        previous: RECORDING.with(|recording| recording.borrow_mut().replace(Recorded::default())),
    }
}

/// Go `SessionVars.RecordRelevantOptVar`.
pub fn record_relevant_opt_var(var_name: &str) {
    RECORDING.with(|recording| {
        if let Some(recorded) = recording.borrow_mut().as_mut() {
            recorded.vars.insert(var_name.to_owned());
        }
    });
}

/// Go `SessionVars.RecordRelevantOptFix`.
pub fn record_relevant_opt_fix(fix_id: u64) {
    RECORDING.with(|recording| {
        if let Some(recorded) = recording.borrow_mut().as_mut() {
            recorded.fixes.insert(fix_id);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_only_inside_a_scope() {
        record_relevant_opt_var("outside");
        let scope = record_scope();
        record_relevant_opt_var("tidb_opt_b");
        record_relevant_opt_var("tidb_opt_a");
        record_relevant_opt_var("tidb_opt_a");
        record_relevant_opt_fix(52869);
        record_relevant_opt_fix(44855);
        assert_eq!(
            scope.recorded(),
            (vec!["tidb_opt_a".to_owned(), "tidb_opt_b".to_owned()], vec![44855, 52869])
        );
        drop(scope);
        record_relevant_opt_var("after");
        assert_eq!(record_scope().recorded(), (Vec::new(), Vec::new()));
    }
}
