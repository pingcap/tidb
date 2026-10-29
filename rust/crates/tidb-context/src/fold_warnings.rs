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

//! The constant fold's warnings on their way to the statement. Go's
//! `FoldConstant` evaluates with the statement's `BuildContext`, so a fold
//! warning lands in `StmtCtx.warnings` directly; this tier folds against a
//! sessionless context and hands the warnings over through this per-thread
//! buffer, which the statement drains (`StmtContext::drain_fold_warnings`).

/// Warnings the constant-fold evaluation raised, stashed for the caller that
/// owns the statement's warning buffer. The fold evaluates column-free
/// expressions against a sessionless context whose `append_warning` would
/// otherwise drop go's per-statement warnings (`ADDDATE('abc', INTERVAL 1
/// DAY)` warns `Incorrect datetime value: 'abc'` on an empty table).
thread_local! {
    static FOLD_WARNINGS: std::cell::RefCell<Vec<crate::SqlWarn>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Records one fold-time warning into this thread's stash.
pub fn record_fold_warning(code: u16, message: &str) {
    record_fold_sql_warn(crate::SqlWarn::with_code(
        crate::WarningLevel::Warning,
        code,
        message,
    ));
}

/// Records one fold-time warning, error and all, into this thread's stash.
pub fn record_fold_sql_warn(warning: crate::SqlWarn) {
    FOLD_WARNINGS.with(|warnings| warnings.borrow_mut().push(warning));
}

/// The current stash depth, so a failed fold attempt can roll its own
/// diagnostics back ([`truncate_fold_warnings`]).
#[must_use]
pub fn fold_warnings_len() -> usize {
    FOLD_WARNINGS.with(|warnings| warnings.borrow().len())
}

/// Rolls the stash back to `len`: a fold attempt whose evaluation FAILED
/// must leave no diagnostics behind -- go's foldConstant keeps the original
/// expression and the EXECUTION re-evaluates it, re-emitting each warning
/// exactly once. Keeping the failed attempt's warnings multiplied every
/// statement-error warning (fold + scalar exec + chunk exec).
pub fn truncate_fold_warnings(len: usize) {
    FOLD_WARNINGS.with(|warnings| warnings.borrow_mut().truncate(len));
}

/// Drains this thread's fold warnings. The statement driver calls this right
/// after planning, forwarding the pairs into the statement's warning buffer
/// so `SHOW WARNINGS` and the OK packet's count see them.
#[must_use]
pub fn take_fold_warnings() -> Vec<crate::SqlWarn> {
    let taken = FOLD_WARNINGS.with(std::cell::RefCell::take);
    taken
}
