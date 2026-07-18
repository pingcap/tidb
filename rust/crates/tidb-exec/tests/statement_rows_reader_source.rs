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

use std::cell::Cell;
use std::collections::VecDeque;
use std::rc::Rc;

use tidb_exec::statement_rows_reader::{RowsPuller, StatementRowsReader};

struct ScriptedPuller {
    batches: VecDeque<Result<Vec<Vec<i64>>, String>>,
    close_error: Option<String>,
    close_counter: Option<Rc<Cell<usize>>>,
}

impl ScriptedPuller {
    fn new(batches: impl IntoIterator<Item = Result<Vec<Vec<i64>>, String>>) -> Self {
        Self {
            batches: batches.into_iter().collect(),
            close_error: None,
            close_counter: None,
        }
    }

    fn with_close_error(mut self, error: &str) -> Self {
        self.close_error = Some(error.to_owned());
        self
    }

    fn with_close_counter(mut self, counter: Rc<Cell<usize>>) -> Self {
        self.close_counter = Some(counter);
        self
    }
}

impl RowsPuller<i64> for ScriptedPuller {
    fn rows(&mut self) -> Result<Vec<Vec<i64>>, String> {
        self.batches.pop_front().unwrap_or_else(|| Ok(Vec::new()))
    }

    fn close(&mut self) -> Result<(), String> {
        if let Some(counter) = &self.close_counter {
            counter.set(counter.get() + 1);
        }
        self.close_error.clone().map_or(Ok(()), Err)
    }
}

fn flatten(rows: Vec<Vec<i64>>) -> Vec<i64> {
    rows.into_iter().flatten().collect()
}

#[test]
fn statement_summary_current_rows_read_to_eof() {
    // Mirrors TestStmtSummaryRetriverV2_TableStatementsSummary at
    // pkg/executor/stmtsummary_test.go:34.
    let mut reader = StatementRowsReader::simple(vec![vec![1], vec![2], vec![3]]);
    assert_eq!(flatten(reader.read(1024).unwrap()), vec![1, 2, 3]);
    assert!(reader.read(1024).unwrap().is_empty());
    assert_eq!(reader.buffered_len(), 0);
    assert!(!reader.has_puller());
}

#[test]
fn statement_summary_evicted_rows_preserve_partial_reads() {
    // Mirrors TestStmtSummaryRetriverV2_TableStatementsSummaryEvicted at
    // pkg/executor/stmtsummary_test.go:80.
    let mut reader = StatementRowsReader::simple(vec![vec![2, 2]]);
    assert_eq!(reader.read(1).unwrap(), vec![vec![2, 2]]);
    assert!(reader.read(1).unwrap().is_empty());
    assert_eq!(reader.buffered_len(), 0);
    assert!(reader.close().is_ok());
}

#[test]
fn statement_summary_history_reads_memory_then_disk_and_closes() {
    // Mirrors TestStmtSummaryRetriverV2_TableStatementsSummaryHistory at
    // pkg/executor/stmtsummary_test.go:127.
    let puller = ScriptedPuller::new([
        Ok(vec![vec![3], vec![4]]),
        Ok(vec![vec![5], vec![6], vec![7]]),
        Ok(Vec::new()),
    ]);
    let mut reader = StatementRowsReader::with_puller(vec![vec![1], vec![2]], Box::new(puller));

    let mut all = Vec::new();
    loop {
        let batch = reader.read(1024).unwrap();
        if batch.is_empty() {
            break;
        }
        all.extend(flatten(batch));
    }
    assert_eq!(all, vec![1, 2, 3, 4, 5, 6, 7]);
    assert!(!reader.has_puller());
    assert!(reader.close().is_ok());
}

#[test]
fn statement_rows_reader_propagates_pull_and_close_errors() {
    let pull_error = ScriptedPuller::new([Err("pull failed".to_owned())]);
    let mut reader = StatementRowsReader::with_puller(Vec::new(), Box::new(pull_error));
    assert_eq!(reader.read(1), Err("pull failed".to_owned()));
    assert!(reader.has_puller());

    let close_error = ScriptedPuller::new([Ok(Vec::new())]).with_close_error("close failed");
    let mut reader = StatementRowsReader::with_puller(Vec::new(), Box::new(close_error));
    assert_eq!(reader.read(1), Err("close failed".to_owned()));
    assert!(reader.has_puller());
    assert_eq!(reader.close(), Err("close failed".to_owned()));
    assert!(reader.has_puller());
}

#[test]
fn statement_rows_reader_close_delegation_and_eof_detachment() {
    let explicit_close_calls = Rc::new(Cell::new(0));
    let puller = ScriptedPuller::new([Ok(vec![vec![1]])])
        .with_close_counter(Rc::clone(&explicit_close_calls));
    let mut reader = StatementRowsReader::with_puller(Vec::new(), Box::new(puller));
    assert!(reader.close().is_ok());
    assert!(reader.close().is_ok());
    assert_eq!(explicit_close_calls.get(), 2);
    assert!(reader.has_puller());

    let eof_close_calls = Rc::new(Cell::new(0));
    let puller =
        ScriptedPuller::new([Ok(Vec::new())]).with_close_counter(Rc::clone(&eof_close_calls));
    let mut reader = StatementRowsReader::with_puller(Vec::new(), Box::new(puller));
    assert!(reader.read(1).unwrap().is_empty());
    assert_eq!(eof_close_calls.get(), 1);
    assert!(!reader.has_puller());
    assert!(reader.close().is_ok());
    assert_eq!(eof_close_calls.get(), 1);
}
