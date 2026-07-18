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

//! Exact dependency-closed assertions from
//! pkg/executor/explain_unit_test.go:135 TestExplainAnalyzeInvokeNextAndClose.
//!
//! The three RU/context subtests under that Go test remain independent,
//! untriaged inventory obligations until their runtime-statistics and static
//! recordset owners exist. They are deliberately not represented by unrelated
//! placeholder assertions here.

#![allow(non_snake_case)]

use std::cell::RefCell;
use std::rc::Rc;

use tidb_exec::explain::{
    AnalyzeExecutor, ExplainError, ExplainExec, ExplainOptions, ExplainPlan, ExplainRows,
};

struct Renderer;

impl ExplainPlan for Renderer {
    fn render_rows(&mut self) -> Result<ExplainRows, ExplainError> {
        Ok(Vec::new())
    }

    fn id(&self) -> i32 {
        0
    }
}

#[derive(Clone, Debug, Default)]
struct ChildState {
    close_calls: usize,
}

struct Child {
    state: Rc<RefCell<ChildState>>,
    panic_next: bool,
}

impl AnalyzeExecutor for Child {
    fn open(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn next(&mut self) -> Result<usize, String> {
        if self.panic_next {
            panic!("next panic");
        }
        Err("next error".to_owned())
    }

    fn close(&mut self) -> Result<(), String> {
        self.state.borrow_mut().close_calls += 1;
        Err("close error".to_owned())
    }

    fn schema_len(&self) -> usize {
        1
    }
}

fn make_child(panic_next: bool) -> (Rc<RefCell<ChildState>>, Child) {
    let state = Rc::new(RefCell::new(ChildState::default()));
    (Rc::clone(&state), Child { state, panic_next })
}

#[test]
fn TestExplainAnalyzeInvokeNextAndClose() {
    let (state, child) = make_child(false);
    let mut explain = ExplainExec::new(
        Renderer,
        ExplainOptions { analyze: true },
        Some(Box::new(child)),
    );
    assert_eq!(
        explain.generate_explain_info().expect_err("next error"),
        ExplainError::Analyze("next error, close error".to_owned())
    );
    assert_eq!(state.borrow().close_calls, 1);

    let (state, child) = make_child(true);
    let mut explain = ExplainExec::new(
        Renderer,
        ExplainOptions { analyze: true },
        Some(Box::new(child)),
    );
    assert_eq!(
        explain.generate_explain_info().expect_err("next panic"),
        ExplainError::Analyze("next panic, close error".to_owned())
    );
    assert_eq!(state.borrow().close_calls, 1);
}
