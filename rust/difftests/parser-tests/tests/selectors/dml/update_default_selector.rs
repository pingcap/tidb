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

/// Direct single-table UPDATE rows whose only new grammar is Go's
/// `parseExprOrDefault` bare-DEFAULT assignment. Generated-column and
/// `DEFAULT(column)` details remain typed expression/DDL work; multi-table
/// and derived-table UPDATE are deliberately excluded from this wave.
fn is_single_table_update_default(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    matches!(
        (
            record.input.path.as_str(),
            record.input.start_line,
            record.input.end_line,
        ),
        (
            "tests/integrationtest/t/ddl/default_as_expression.test",
            518,
            518
        ) | ("tests/integrationtest/t/executor/update.test", 514, 514)
            | ("tests/integrationtest/t/executor/update.test", 516, 516)
            | ("tests/integrationtest/t/executor/update.test", 518, 518)
            | ("tests/integrationtest/t/executor/update.test", 521, 521)
            | ("tests/integrationtest/t/executor/update.test", 528, 528)
            | ("tests/integrationtest/t/executor/update.test", 530, 530)
            | ("tests/integrationtest/t/executor/update.test", 532, 532)
            | ("tests/integrationtest/t/executor/update.test", 534, 534)
            | ("tests/integrationtest/t/executor/update.test", 536, 536)
            | ("tests/integrationtest/t/executor/update.test", 539, 539)
            | ("tests/integrationtest/t/executor/update.test", 542, 542)
            | ("tests/integrationtest/t/executor/update.test", 543, 543)
            | ("tests/integrationtest/t/executor/update.test", 546, 546)
            | ("tests/integrationtest/t/executor/update.test", 548, 548)
            | ("tests/integrationtest/t/executor/update.test", 552, 552)
            | (
                "tests/integrationtest/t/planner/core/integration.test",
                930,
                930
            )
    )
}

#[test]
fn single_table_update_default_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_single_table_update_default(record)
        })
        .collect();
    assert_eq!(selected.len(), 17, "source-backed selector drifted");

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
