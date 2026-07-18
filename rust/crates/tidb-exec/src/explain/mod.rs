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

//! EXPLAIN execution from pkg/executor/explain.go.
//!
//! The Go executor is a thin protocol adapter around two owners: the planner
//! renders rows and an optional child executor is drained for EXPLAIN ANALYZE.
//! This module keeps that ownership split. It does not invent SQL plans,
//! formats, or runtime counters: `ExplainExec` accepts the planner renderer
//! and analyze child through narrow executor-facing traits.

use std::any::Any;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};

/// Rows produced by an explain renderer before protocol conversion.
pub type ExplainRows = Vec<Vec<String>>;

/// Executor-owned EXPLAIN state.
///
/// Format selection, ID formatting, and result schemas belong to the planner
/// `Explain` owner. Keeping only `analyze` here prevents this executor seam
/// from becoming a second, incomplete EXPLAIN serializer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExplainOptions {
    /// Whether the statement requested runtime analysis.
    pub analyze: bool,
}

/// Error returned by an explain owner.
///
/// The payload is intentionally a string. Go's executor combines errors from
/// Next and Close using their rendered text; keeping that contract here avoids
/// converting an external executor's error vocabulary into a misleading
/// ExecError::Unsupported variant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExplainError {
    /// The child analyze executor failed while opening, draining, or closing.
    Analyze(String),
    /// The plan could not render its result rows.
    Render(String),
}

impl ExplainError {
    fn analyze(message: impl Into<String>) -> Self {
        Self::Analyze(message.into())
    }

    fn message(&self) -> &str {
        match self {
            Self::Analyze(message) | Self::Render(message) => message,
        }
    }
}

impl fmt::Display for ExplainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for ExplainError {}

/// A planner-owned result renderer.
pub trait ExplainPlan {
    /// Renders all rows for one explain request.
    ///
    /// The planner owns format validation, schema selection, tree formatting,
    /// runtime columns, and mutation of its cached Rows field.
    fn render_rows(&mut self) -> Result<ExplainRows, ExplainError>;

    /// Returns the target plan identity used by runtime-statistics owners.
    fn id(&self) -> i32;
}

/// Actionable foreign-key trigger view exposed by an analyze child.
///
/// The complete Rust FK executor will extend this trait with the source
/// `GetFKChecks` and `GetFKCascades` equivalents when those executor types are
/// translated. Returning a trait object, rather than a capability bit, keeps
/// the accessor usable without inventing those missing types.
pub trait ForeignKeyTrigger {
    /// Mirrors `WithForeignKeyTrigger.HasFKCascades`.
    fn has_fk_cascades(&self) -> bool;
}

/// A child executor consumed by EXPLAIN ANALYZE.
///
/// next returns the number of rows in the child batch. This is the one fact
/// the Go executor needs from a chunk: a zero-sized batch terminates the
/// drain, while a non-zero batch is discarded. Runtime row values, memory
/// trackers, and RU details remain owned by the external executor.
pub trait AnalyzeExecutor {
    /// Opens the child executor.
    fn open(&mut self) -> Result<(), String>;

    /// Fetches and discards one child batch, returning its row count.
    fn next(&mut self) -> Result<usize, String>;

    /// Closes the child executor.
    fn close(&mut self) -> Result<(), String>;

    /// Returns the child schema width for the no-delay DML handoff.
    fn schema_len(&self) -> usize;

    /// Returns the source `WithForeignKeyTrigger` view when implemented.
    fn foreign_key_trigger(&self) -> Option<&dyn ForeignKeyTrigger> {
        None
    }
}

/// A stateful executor matching Go's ExplainExec lifecycle.
pub struct ExplainExec {
    plan: Box<dyn ExplainPlan>,
    options: ExplainOptions,
    analyze_exec: Option<Box<dyn AnalyzeExecutor>>,
    executed: bool,
    rows: Option<ExplainRows>,
    cursor: usize,
}

impl ExplainExec {
    /// Creates an explain executor around a planner-owned renderer.
    pub fn new(
        plan: impl ExplainPlan + 'static,
        options: ExplainOptions,
        analyze_exec: Option<Box<dyn AnalyzeExecutor>>,
    ) -> Self {
        Self {
            plan: Box::new(plan),
            options,
            analyze_exec,
            executed: false,
            rows: None,
            cursor: 0,
        }
    }

    /// Returns the target plan ID used by runtime-statistics registration.
    #[must_use]
    pub fn target_plan_id(&self) -> i32 {
        self.plan.id()
    }

    /// Opens the analyze child when this is an EXPLAIN ANALYZE request.
    pub fn open(&mut self) -> Result<(), ExplainError> {
        if self.options.analyze {
            if let Some(child) = self.analyze_exec.as_mut() {
                open_child(child.as_mut())?;
            }
        }
        Ok(())
    }

    /// Clears buffered rows and closes an opened-but-not-executed child.
    pub fn close(&mut self) -> Result<(), ExplainError> {
        self.rows = None;
        if self.options.analyze && !self.executed {
            if let Some(child) = self.analyze_exec.as_mut() {
                return close_child(child.as_mut());
            }
        }
        Ok(())
    }

    /// Fetches up to capacity rendered rows.
    ///
    /// A zero capacity is a valid empty batch and does not advance the cursor.
    /// Production chunk adapters normally pass a positive capacity, but
    /// preserving this state transition avoids silently dropping a row.
    pub fn next(&mut self, capacity: usize) -> Result<ExplainRows, ExplainError> {
        if self.rows.is_none() {
            self.rows = Some(self.generate_explain_info()?);
        }
        let rows = self.rows.as_ref().expect("rows initialized above");
        if self.cursor >= rows.len() || capacity == 0 {
            return Ok(Vec::new());
        }
        let end = self.cursor.saturating_add(capacity).min(rows.len());
        let batch = rows[self.cursor..end].to_vec();
        self.cursor = end;
        Ok(batch)
    }

    /// Drains the analyze child once, then renders the plan rows.
    pub fn generate_explain_info(&mut self) -> Result<ExplainRows, ExplainError> {
        if self.options.analyze {
            self.execute_analyze_exec()?;
        }
        self.plan.render_rows()
    }

    /// Executes and closes the child, combining drain and close errors exactly
    /// like Go's deferred exec.Close path.
    pub fn execute_analyze_exec(&mut self) -> Result<(), ExplainError> {
        if !self.options.analyze || self.executed {
            return Ok(());
        }

        let Some(child) = self.analyze_exec.as_mut() else {
            return Ok(());
        };
        // Go marks the child executed before the first Next call, so a
        // failing/panicking drain is never retried by a later Next/Close.
        self.executed = true;

        let mut drain_error = None;
        loop {
            match catch_unwind(AssertUnwindSafe(|| child.next())) {
                Ok(Ok(row_count)) if row_count != 0 => continue,
                Ok(Ok(_)) => break,
                Ok(Err(error)) => {
                    drain_error = Some(ExplainError::analyze(error));
                    break;
                }
                Err(payload) => {
                    drain_error = Some(ExplainError::analyze(panic_message(payload)));
                    break;
                }
            }
        }

        let close_error = close_child(child.as_mut()).err();
        match (drain_error, close_error) {
            (Some(first), Some(second)) => Err(ExplainError::analyze(format!(
                "{}, {}",
                first.message(),
                second.message()
            ))),
            (Some(error), None) | (None, Some(error)) => Err(error),
            (None, None) => Ok(()),
        }
    }

    /// Borrows the retained child for no-delay DML execution and marks it executed.
    ///
    /// Only zero-column analyze executors (INSERT, UPDATE, or DELETE) are
    /// eligible, and the caller becomes responsible for driving and closing
    /// the returned child. Go returns an interface containing the same child
    /// pointer; safe Rust expresses that retained identity as a mutable borrow
    /// tied to this `ExplainExec` rather than transferring ownership out.
    pub fn get_analyze_exec_to_executed_no_delay(
        &mut self,
    ) -> Option<&mut (dyn AnalyzeExecutor + '_)> {
        if self.options.analyze && !self.executed {
            let eligible = self
                .analyze_exec
                .as_ref()
                .is_some_and(|child| child.schema_len() == 0);
            if eligible {
                self.executed = true;
                return match self.analyze_exec.as_mut() {
                    Some(child) => Some(child.as_mut()),
                    None => None,
                };
            }
        }
        None
    }

    /// Returns the analyze child's actionable foreign-key trigger view.
    #[must_use]
    pub fn get_analyze_exec_with_foreign_key_trigger(&self) -> Option<&dyn ForeignKeyTrigger> {
        if !self.options.analyze {
            return None;
        }
        self.analyze_exec
            .as_ref()
            .and_then(|child| child.foreign_key_trigger())
    }

    /// Returns whether the child has already been drained or handed off.
    #[must_use]
    pub const fn executed(&self) -> bool {
        self.executed
    }
}

fn open_child(child: &mut dyn AnalyzeExecutor) -> Result<(), ExplainError> {
    match catch_unwind(AssertUnwindSafe(|| child.open())) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(ExplainError::analyze(error)),
        Err(payload) => Err(ExplainError::analyze(panic_message(payload))),
    }
}

fn close_child(child: &mut dyn AnalyzeExecutor) -> Result<(), ExplainError> {
    match catch_unwind(AssertUnwindSafe(|| child.close())) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(ExplainError::analyze(error)),
        Err(payload) => Err(ExplainError::analyze(panic_message(payload))),
    }
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_owned();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "panic".to_owned()
}

/// Pure portion of Go's memory-debug scheduler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryDebugSchedule {
    /// Polling interval in seconds.
    pub interval_seconds: u64,
    /// Number of polls between informational log lines.
    pub print_mod: u32,
}

/// Selects the source polling interval and print modulus.
#[must_use]
pub const fn update_trigger_interval_by_heap_in_use(heap_in_use: u64) -> MemoryDebugSchedule {
    // Go's size.GB is 1 << 30. Keep the exact strict thresholds.
    if heap_in_use < 30 * (1 << 30) {
        MemoryDebugSchedule {
            interval_seconds: 5,
            print_mod: 6,
        }
    } else if heap_in_use < 40 * (1 << 30) {
        MemoryDebugSchedule {
            interval_seconds: 15,
            print_mod: 2,
        }
    } else {
        MemoryDebugSchedule {
            interval_seconds: 30,
            print_mod: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockPlan;

    impl ExplainPlan for MockPlan {
        fn render_rows(&mut self) -> Result<ExplainRows, ExplainError> {
            Ok(vec![vec!["first".to_owned()], vec!["second".to_owned()]])
        }

        fn id(&self) -> i32 {
            7
        }
    }

    #[derive(Debug)]
    struct MockAnalyze {
        schema_len: usize,
        batches: Vec<Result<usize, String>>,
        next_calls: usize,
        open_calls: usize,
        close_calls: usize,
        panic_open: bool,
        panic_next: bool,
        has_fk_trigger: bool,
        has_fk_cascades: bool,
        close_error: Option<String>,
    }

    impl MockAnalyze {
        fn empty(schema_len: usize) -> Self {
            Self {
                schema_len,
                batches: vec![Ok(0)],
                next_calls: 0,
                open_calls: 0,
                close_calls: 0,
                panic_open: false,
                panic_next: false,
                has_fk_trigger: false,
                has_fk_cascades: false,
                close_error: None,
            }
        }
    }

    impl ForeignKeyTrigger for MockAnalyze {
        fn has_fk_cascades(&self) -> bool {
            self.has_fk_cascades
        }
    }

    impl AnalyzeExecutor for MockAnalyze {
        fn open(&mut self) -> Result<(), String> {
            self.open_calls += 1;
            if self.panic_open {
                panic!("open panic");
            }
            Ok(())
        }

        fn next(&mut self) -> Result<usize, String> {
            self.next_calls += 1;
            if self.panic_next {
                panic!("next panic");
            }
            self.batches
                .get(self.next_calls - 1)
                .cloned()
                .unwrap_or(Ok(0))
        }

        fn close(&mut self) -> Result<(), String> {
            self.close_calls += 1;
            self.close_error.clone().map_or(Ok(()), Err)
        }

        fn schema_len(&self) -> usize {
            self.schema_len
        }

        fn foreign_key_trigger(&self) -> Option<&dyn ForeignKeyTrigger> {
            self.has_fk_trigger
                .then_some(self as &dyn ForeignKeyTrigger)
        }
    }

    #[test]
    fn analyze_lifecycle_drains_once_and_batches_rendered_rows() {
        let child = MockAnalyze {
            batches: vec![Ok(2), Ok(1), Ok(0)],
            ..MockAnalyze::empty(1)
        };
        let mut explain = ExplainExec::new(
            MockPlan,
            ExplainOptions { analyze: true },
            Some(Box::new(child)),
        );
        explain.open().expect("open");
        assert_eq!(explain.next(1).expect("first batch").len(), 1);
        assert_eq!(explain.next(2).expect("second batch").len(), 1);
        assert!(explain.next(1).expect("eof").is_empty());
        assert!(explain.executed());
        let child = explain.analyze_exec.as_ref().expect("child");
        // One open, three batches including the terminal empty batch, and one
        // deferred close are the source lifecycle.
        assert_eq!(child.schema_len(), 1);
    }

    #[test]
    fn open_panic_is_normalized_like_exec_open() {
        let child = MockAnalyze {
            panic_open: true,
            ..MockAnalyze::empty(1)
        };
        let mut explain = ExplainExec::new(
            MockPlan,
            ExplainOptions { analyze: true },
            Some(Box::new(child)),
        );
        assert_eq!(
            explain.open().expect_err("open panic"),
            ExplainError::Analyze("open panic".to_owned())
        );
    }

    #[test]
    fn analyze_error_and_panic_are_combined_with_close_error() {
        let child = MockAnalyze {
            batches: vec![Err("next error".to_owned())],
            close_error: Some("close error".to_owned()),
            ..MockAnalyze::empty(1)
        };
        let mut explain = ExplainExec::new(
            MockPlan,
            ExplainOptions { analyze: true },
            Some(Box::new(child)),
        );
        assert_eq!(
            explain.generate_explain_info().expect_err("next error"),
            ExplainError::Analyze("next error, close error".to_owned())
        );

        let child = MockAnalyze {
            panic_next: true,
            close_error: Some("close error".to_owned()),
            ..MockAnalyze::empty(1)
        };
        let mut explain = ExplainExec::new(
            MockPlan,
            ExplainOptions { analyze: true },
            Some(Box::new(child)),
        );
        assert_eq!(
            explain.generate_explain_info().expect_err("panic"),
            ExplainError::Analyze("next panic, close error".to_owned())
        );
    }

    #[test]
    fn close_without_next_closes_child_but_executed_child_is_not_closed_twice() {
        let child = MockAnalyze::empty(1);
        let mut explain = ExplainExec::new(
            MockPlan,
            ExplainOptions { analyze: true },
            Some(Box::new(child)),
        );
        explain.open().expect("open");
        explain.close().expect("close");
        let child = explain.analyze_exec.as_ref().expect("child");
        assert_eq!(child.schema_len(), 1);

        let child = MockAnalyze::empty(1);
        let mut explain = ExplainExec::new(
            MockPlan,
            ExplainOptions { analyze: true },
            Some(Box::new(child)),
        );
        explain.generate_explain_info().expect("drain");
        explain.close().expect("close after drain");
        assert!(explain.executed());
    }

    #[test]
    fn close_clears_rows_without_rewinding_source_cursor() {
        let mut explain = ExplainExec::new(MockPlan, ExplainOptions::default(), None);
        assert_eq!(explain.next(usize::MAX).expect("rows").len(), 2);
        explain.close().expect("close");
        assert!(explain
            .next(usize::MAX)
            .expect("still exhausted")
            .is_empty());
    }

    #[test]
    fn no_delay_borrows_retained_child_and_fk_hook_is_actionable() {
        let mut child = MockAnalyze::empty(1);
        child.has_fk_trigger = true;
        child.has_fk_cascades = true;
        let mut explain = ExplainExec::new(
            MockPlan,
            ExplainOptions { analyze: true },
            Some(Box::new(child)),
        );
        assert!(explain
            .get_analyze_exec_with_foreign_key_trigger()
            .expect("FK trigger")
            .has_fk_cascades());
        assert!(explain.get_analyze_exec_to_executed_no_delay().is_none());

        let mut child = MockAnalyze::empty(0);
        child.has_fk_trigger = true;
        child.has_fk_cascades = true;
        let mut explain = ExplainExec::new(
            MockPlan,
            ExplainOptions { analyze: true },
            Some(Box::new(child)),
        );
        {
            let handed_off = explain
                .get_analyze_exec_to_executed_no_delay()
                .expect("zero-column child");
            assert_eq!(handed_off.next().expect("next"), 0);
            handed_off.close().expect("close");
        }
        assert!(explain.executed());
        assert!(explain
            .get_analyze_exec_with_foreign_key_trigger()
            .expect("retained FK trigger")
            .has_fk_cascades());
    }

    #[test]
    fn memory_debug_thresholds_match_source_strict_boundaries() {
        let gb = 1_u64 << 30;
        assert_eq!(
            update_trigger_interval_by_heap_in_use(30 * gb - 1),
            MemoryDebugSchedule {
                interval_seconds: 5,
                print_mod: 6
            }
        );
        assert_eq!(
            update_trigger_interval_by_heap_in_use(30 * gb),
            MemoryDebugSchedule {
                interval_seconds: 15,
                print_mod: 2
            }
        );
        assert_eq!(
            update_trigger_interval_by_heap_in_use(40 * gb - 1),
            MemoryDebugSchedule {
                interval_seconds: 15,
                print_mod: 2
            }
        );
        assert_eq!(
            update_trigger_interval_by_heap_in_use(40 * gb),
            MemoryDebugSchedule {
                interval_seconds: 30,
                print_mod: 1
            }
        );
    }
}
