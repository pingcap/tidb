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

const SOURCE_ROW: (&str, usize) = ("tests/integrationtest/t/planner/core/plan.test", 172);

#[test]
fn explain_parenthesized_values_source_row_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && (record.input.path.as_str(), record.input.start_line) == SOURCE_ROW
        })
        .collect();
    assert_eq!(selected.len(), 1, "source-backed selector drifted");

    let record = selected[0];
    let statement =
        tidb_parser::parse(&record.input.sql).expect("parse EXPLAIN parenthesized VALUES");
    assert_eq!(
        statement.restore().as_bytes(),
        record.restores[0].as_slice(),
        "{}:{}",
        record.input.path,
        record.input.start_line
    );
}
