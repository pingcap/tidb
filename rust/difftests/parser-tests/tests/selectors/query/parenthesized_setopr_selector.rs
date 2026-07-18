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

const DB_PARTITION: &str = "tests/integrationtest/t/ddl/db_partition.test";
const PLANNER_ISSUE: &str = "tests/integrationtest/t/planner/core/issuetest/planner_issue.test";

fn selected_parenthesized_setopr(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    record.outcome == GoOutcome::Accepted
        && record.statement_count == 1
        && ((record.input.path == DB_PARTITION && matches!(record.input.start_line, 1072 | 1088))
            || (record.input.path == PLANNER_ISSUE && record.input.start_line == 773))
}

#[test]
fn parenthesized_setopr_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| selected_parenthesized_setopr(record))
        .collect();
    assert_eq!(selected.len(), 3, "source-backed selector drifted");

    let mut failures = Vec::new();
    for record in selected {
        match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {}
            Ok(statement) => failures.push(format!(
                "{}\n  go: {}\n rust: {}",
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => failures.push(format!("{}\n  parse error: {error:?}", record.input.sql)),
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
