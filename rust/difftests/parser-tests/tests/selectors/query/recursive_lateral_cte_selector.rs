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

const SOURCE_PATH: &str = "tests/integrationtest/t/planner/core/lateral_join.test";
const SOURCE_ROWS: &[usize] = &[46, 56];

#[test]
fn recursive_lateral_cte_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && record.input.path == SOURCE_PATH
                && SOURCE_ROWS.contains(&record.input.start_line)
        })
        .collect();
    assert_eq!(
        selected.len(),
        SOURCE_ROWS.len(),
        "source-backed selector drifted"
    );

    for record in selected {
        let statement = tidb_parser::parse(&record.input.sql).unwrap_or_else(|error| {
            panic!(
                "{}:{} parse failed: {error:?}",
                SOURCE_PATH, record.input.start_line
            )
        });
        assert_eq!(
            statement.restore().as_bytes(),
            record.restores[0].as_slice(),
            "{}:{} restore mismatch",
            SOURCE_PATH,
            record.input.start_line
        );
    }
}
