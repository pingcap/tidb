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

const SOURCE_PATH: &str = "tests/integrationtest/t/planner/core/issuetest/planner_issue.test";
const SOURCE_ROW: usize = 574;

#[test]
fn with_parenthesized_outer_query_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && record.input.path == SOURCE_PATH
                && record.input.start_line == SOURCE_ROW
        })
        .collect();
    assert_eq!(selected.len(), 1, "source-backed selector drifted");
    let record = selected[0];
    let statement = tidb_parser::parse(&record.input.sql)
        .unwrap_or_else(|error| panic!("{SOURCE_PATH}:{SOURCE_ROW} parse failed: {error:?}"));
    assert_eq!(
        statement.restore_bytes(),
        record.restores[0],
        "{SOURCE_PATH}:{SOURCE_ROW} restore mismatch"
    );
}
