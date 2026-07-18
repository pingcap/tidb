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
/// Go's `pkg/parser/dml_parser.go` dispatches an INSERT source beginning
/// with `WITH` through `parseWithStmt`, which attaches the clause to the
/// source ResultSetNode rather than to `ast.InsertStmt`. These are the seven
/// integration fixtures where `WITH` is directly that source (rather than a
/// nested derived query or literal text); keeping the source locations exact
/// prevents unrelated INSERT grammar from being attributed to this seam.
fn is_insert_with_source(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    matches!(
        (
            record.input.path.as_str(),
            record.input.start_line,
            record.input.end_line,
        ),
        ("tests/integrationtest/t/executor/admin.test", 223, 223)
            | ("tests/integrationtest/t/executor/sample.test", 128, 128)
            | ("tests/integrationtest/t/executor/sample.test", 129, 129)
            | ("tests/integrationtest/t/expression/json.test", 336, 341)
            | ("tests/integrationtest/t/expression/json.test", 342, 347)
            | ("tests/integrationtest/t/expression/misc.test", 320, 320)
            | ("tests/integrationtest/t/expression/misc.test", 337, 337)
    )
}

#[test]
fn insert_with_cte_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_insert_with_source(record)
        })
        .collect();
    assert_eq!(selected.len(), 7, "source-backed selector drifted");

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
