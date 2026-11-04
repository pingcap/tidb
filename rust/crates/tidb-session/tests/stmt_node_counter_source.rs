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

//! Go `executor.Compile`'s `CountStmtNode`: every compiled statement of a
//! non-internal session is counted under its label, wherever it came from.

use crate::support::testkit::TestKit;

fn count(label: &str) -> f64 {
    tidb_metrics::executor::STATEMENT_TOTAL
        .with_label_values(&["", "default", label])
        .get()
}

/// A session that never saw a wire counts its statements, and an EXECUTE
/// counts as the statement it prepared.
#[test]
fn compiled_statements_count_by_label_and_an_execute_by_its_prepared_statement() {
    let mut tk = TestKit::new();
    tk.must_exec("create table stmt_count_t (a int)");
    let (insert, select, execute, begin) = (count("Insert"), count("Select"), count("Execute"), count("Begin"));
    tk.must_exec("insert into stmt_count_t values (1)");
    tk.must_exec("prepare s from 'select a from stmt_count_t'");
    tk.must_query("execute s");
    tk.must_query("select a from stmt_count_t");
    tk.must_exec("begin");
    tk.must_exec("commit");
    assert_eq!(count("Insert") - insert, 1.0);
    assert_eq!(count("Select") - select, 2.0);
    assert_eq!(count("Execute") - execute, 0.0);
    assert_eq!(count("Begin") - begin, 1.0);
}
