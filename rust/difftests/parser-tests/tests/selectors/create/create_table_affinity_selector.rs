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

//! Exact CREATE TABLE AFFINITY rows from TiDB's integration fixture.

use std::collections::BTreeSet;

use difftest::parser_oracle::{shared_golden, GoOutcome};

const AFFINITY_CREATE_FIXTURES: [(&str, usize); 9] = [
    ("tests/integrationtest/t/ddl/affinity.test", 3),
    ("tests/integrationtest/t/ddl/affinity.test", 5),
    ("tests/integrationtest/t/ddl/affinity.test", 8),
    ("tests/integrationtest/t/ddl/affinity.test", 10),
    ("tests/integrationtest/t/ddl/affinity.test", 12),
    ("tests/integrationtest/t/ddl/affinity.test", 28),
    ("tests/integrationtest/t/ddl/affinity.test", 30),
    ("tests/integrationtest/t/ddl/affinity.test", 68),
    ("tests/integrationtest/t/ddl/affinity.test", 120),
];

#[test]
fn creation_affinity_integration_rows_match_go_exactly() {
    let expected: BTreeSet<_> = AFFINITY_CREATE_FIXTURES.into_iter().collect();
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| expected.contains(&(record.input.path.as_str(), record.input.start_line)))
        .collect();
    assert_eq!(selected.len(), 9, "source-backed selector drifted");

    let mut failures = Vec::new();
    for record in selected {
        assert_eq!(record.outcome, GoOutcome::Accepted, "{}", record.input.sql);
        assert_eq!(record.statement_count, 1, "{}", record.input.sql);
        match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {}
            Ok(statement) => failures.push(format!(
                "{}:{}\n  sql: {}\n   go: {}\n rust: {}",
                record.input.path,
                record.input.start_line,
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => failures.push(format!(
                "{}:{}\n  sql: {}\n  parse error: {error:?}",
                record.input.path, record.input.start_line, record.input.sql
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
