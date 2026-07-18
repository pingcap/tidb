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

/// Selects the two source-owned CTE-prefixed UPDATE binding rows from
/// `tests/integrationtest/t/bindinfo/temptable.test:51,67`.  The source path
/// and exact line set keep this ring from accidentally absorbing the broader
/// WITH/DML corpus or unrelated CREATE BINDING vectors.
fn selected_row(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    record.outcome == GoOutcome::Accepted
        && record.statement_count == 1
        && record.input.path == "tests/integrationtest/t/bindinfo/temptable.test"
        && matches!(record.input.start_line, 51 | 67)
        && record.input.start_line == record.input.end_line
        && record
            .input
            .sql
            .to_ascii_uppercase()
            .starts_with("CREATE GLOBAL BINDING FOR WITH ")
}

#[test]
fn create_binding_with_dml_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| selected_row(record))
        .collect();
    assert_eq!(selected.len(), 2, "source-backed selector drifted");

    let failures: Vec<_> = selected
        .into_iter()
        .filter_map(|record| match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {
                None
            }
            Ok(statement) => Some(format!(
                "{}:{}\n  go: {}\n rust: {}",
                record.input.path,
                record.input.start_line,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => Some(format!(
                "{}:{}\n  parse error: {error:?}",
                record.input.path, record.input.start_line
            )),
        })
        .collect();
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
