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

/// Selects the three contiguous Go `CREATE DEFINER ... VIEW` rows whose
/// parenthesized query terms are joined by an outer `UNION`. The source line
/// anchor is deliberately exact: broader `CREATE VIEW` selection belongs to
/// the identity/security and query-shape selectors, not this set-operation
/// ownership boundary.
fn is_parenthesized_definer_union(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    record.outcome == GoOutcome::Accepted
        && record.statement_count == 1
        && record.input.path == "tests/integrationtest/t/planner/core/casetest/integration.test"
        && matches!(record.input.start_line, 440 | 443 | 446)
        && record.input.start_line == record.input.end_line
        && record
            .input
            .sql
            .to_ascii_lowercase()
            .contains("create definer=")
        && record.input.sql.to_ascii_lowercase().contains(" union ")
}

#[test]
fn create_view_parenthesized_definer_union_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| is_parenthesized_definer_union(record))
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
