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

#![allow(missing_docs)]

//! Exact Go-oracle rows for the ordinary SHOW family leaves owned here.

use difftest::parser_oracle::{shared_golden, GoOutcome};

const SHOW_BUILTINS_ROW: (&str, usize) = ("tests/integrationtest/t/executor/show.test", 299);
const SHOW_FULL_TABLES_ROW: (&str, usize) = (
    "tests/integrationtest/t/planner/core/memtable_predicate_extractor.test",
    44,
);

#[test]
fn show_builtins_and_full_tables_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && matches!(
                    (record.input.path.as_str(), record.input.start_line),
                    SHOW_BUILTINS_ROW | SHOW_FULL_TABLES_ROW
                )
        })
        .collect();
    assert_eq!(rows.len(), 2, "source-backed selector drifted");

    let mut failures = Vec::new();
    for record in rows {
        match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {}
            Ok(statement) => failures.push(format!(
                "{}:{}\n  go: {}\n rust: {}",
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
