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

const EXPAND_TEST: &str = "tests/integrationtest/t/executor/expand.test";
const ISSUES_TEST: &str = "tests/integrationtest/t/expression/issues.test";
const JSON_TEST: &str = "tests/integrationtest/t/expression/json.test";

fn selected_string_literal_alias(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    ((record.input.path == EXPAND_TEST
        && matches!(record.input.start_line, 100 | 131 | 278 | 288 | 303 | 312))
        || (record.input.path == ISSUES_TEST && record.input.start_line == 1840)
        || (record.input.path == JSON_TEST && record.input.start_line == 70))
        && record.outcome == GoOutcome::Accepted
        && record.statement_count == 1
}

#[test]
fn string_literal_alias_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| selected_string_literal_alias(record))
        .collect();
    assert_eq!(selected.len(), 8, "source-backed selector drifted");

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
