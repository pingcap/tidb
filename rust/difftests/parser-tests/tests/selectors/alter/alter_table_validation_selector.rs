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

//! Exact checked-oracle coverage for standalone ALTER TABLE validation rows.

use difftest::parser_oracle::{shared_golden, GoOutcome};

const ROWS: [(&str, usize); 2] = [
    ("tests/integrationtest/t/ddl/db_table.test", 44),
    ("tests/integrationtest/t/ddl/db_table.test", 46),
];

#[test]
fn alter_table_validation_lexical_rows_match_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && ROWS.contains(&(record.input.path.as_str(), record.input.start_line))
        })
        .collect();
    assert_eq!(selected.len(), ROWS.len(), "source-backed selector drifted");

    let mut failures = Vec::new();
    for record in selected {
        match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {}
            Ok(statement) => failures.push(format!(
                "{}:{}\n  go: {}\n  rust: {}",
                record.input.path,
                record.input.start_line,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => failures.push(format!(
                "{}:{}\n  parse error: {error:?}",
                record.input.path, record.input.start_line
            )),
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
