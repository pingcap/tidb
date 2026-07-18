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

#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

fn selected_row(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    record.outcome == GoOutcome::Accepted
        && record.statement_count == 1
        && record.input.path == "tests/integrationtest/t/planner/core/issuetest/planner_issue.test"
        && record.input.start_line == 74
        && record.input.end_line == 74
        && record.input.sql.trim_end_matches(';').eq_ignore_ascii_case(
            "INSERT INTO v0 WITH ta2 AS (TABLE v0) TABLE ta2 FOR UPDATE OF ta2",
        )
}

#[test]
fn insert_with_table_for_update_static_go_row_matches() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| selected_row(record))
        .collect();
    assert_eq!(selected.len(), 1, "source-backed selector drifted");

    let record = selected[0];
    match tidb_parser::parse(&record.input.sql) {
        Ok(statement) => assert_eq!(
            statement.restore().as_bytes(),
            record.restores[0].as_slice(),
            "Rust restore drifted from Go for {}:{}",
            record.input.path,
            record.input.start_line
        ),
        Err(error) => panic!(
            "Rust rejected {}:{}: {error:?}",
            record.input.path, record.input.start_line
        ),
    }
}
