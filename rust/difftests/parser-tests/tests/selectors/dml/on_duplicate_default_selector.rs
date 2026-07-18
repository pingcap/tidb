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

/// Go's `parseInsert` reads ON DUPLICATE assignments through
/// `parseAssignment`, whose RHS is `parseExprOrDefault`. These are the exact
/// bare-DEFAULT fixtures; `DEFAULT(column)` remains an ordinary expression,
/// and qualified INSERT SET targets are intentionally outside this seam.
fn is_on_duplicate_bare_default(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    matches!(
        (
            record.input.path.as_str(),
            record.input.start_line,
            record.input.end_line,
        ),
        ("tests/integrationtest/t/executor/write.test", 74, 74)
            | ("tests/integrationtest/t/executor/write.test", 95, 95)
            | ("tests/integrationtest/t/executor/write.test", 97, 97)
            | ("tests/integrationtest/t/executor/write.test", 99, 99)
            | ("tests/integrationtest/t/executor/write.test", 107, 107)
            | ("tests/integrationtest/t/executor/write.test", 109, 109)
            | ("tests/integrationtest/t/executor/write.test", 111, 111)
            | ("tests/integrationtest/t/executor/write.test", 113, 113)
    )
}

#[test]
fn on_duplicate_bare_default_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_on_duplicate_bare_default(record)
        })
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
