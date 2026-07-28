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

//! The per-statement evaluation context, which is Go's `StatementContext`.

use std::cell::RefCell;
use std::rc::Rc;

use tidb_datatype::Datum;
use tidb_expr::{Columns, ErrorLevel};

/// Go `stmtctx.StatementContext`, in the part evaluation actually reads: the
/// warning buffer and the error levels that decide whether a tolerable
/// condition warns or fails the statement.
///
/// Go hands one `sctx` to every expression, and the buffer is mutated through
/// a shared reference; the handle here is cheap to clone for the same reason,
/// so every executor in a plan writes into the one buffer the statement
/// reports at the end.
///
/// DEFERRED (documented): the rest of `StatementContext` -- the other error
/// groups (truncation, bad NULL, no default), the statement-scoped clock, the
/// resource tracker and the runtime stats.
#[derive(Clone, Default)]
pub struct StmtContext {
    warnings: Rc<RefCell<Vec<(u16, String)>>>,
    division_by_zero: ErrorLevel,
}

impl StmtContext {
    /// A context for a query, where Go always warns on a zero divisor.
    #[must_use]
    pub fn for_query() -> Self {
        Self {
            warnings: Rc::default(),
            division_by_zero: ErrorLevel::Warn,
        }
    }

    /// A context for `INSERT`/`UPDATE`/`DELETE`, where Go resolves the level
    /// from the SQL mode: without `ERROR_FOR_DIVISION_BY_ZERO` the condition
    /// is ignored entirely, a non-strict mode warns, and the default strict
    /// mode fails the statement.
    #[must_use]
    pub fn for_dml(error_for_division_by_zero: bool, strict: bool) -> Self {
        let level = if !error_for_division_by_zero {
            ErrorLevel::Ignore
        } else if strict {
            ErrorLevel::Error
        } else {
            ErrorLevel::Warn
        };
        Self {
            warnings: Rc::default(),
            division_by_zero: level,
        }
    }

    /// The warnings evaluation recorded, in the order they were raised.
    #[must_use]
    pub fn take_warnings(&self) -> Vec<(u16, String)> {
        std::mem::take(&mut self.warnings.borrow_mut())
    }
}

impl Columns for StmtContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn division_by_zero_level(&self) -> ErrorLevel {
        self.division_by_zero
    }

    fn append_warning(&self, code: u16, message: &str) {
        self.warnings.borrow_mut().push((code, message.to_owned()));
    }
}
