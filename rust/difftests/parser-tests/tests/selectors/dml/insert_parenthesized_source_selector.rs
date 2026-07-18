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

/// Go's `parseInsertStmt` owns a parenthesized result-set source after either
/// the target table or an explicit target-column list. These are every
/// Go-accepted static fixture at that typed SELECT boundary; scalar
/// subqueries inside VALUES/assignments are intentionally not selected.
fn is_parenthesized_insert_source(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    matches!(
        (
            record.input.path.as_str(),
            record.input.start_line,
            record.input.end_line,
        ),
        ("tests/integrationtest/t/executor/insert.test", 883, 883)
            | ("tests/integrationtest/t/executor/insert.test", 885, 885)
            | ("tests/integrationtest/t/executor/insert.test", 887, 887)
            | (
                "tests/integrationtest/t/executor/parallel_apply.test",
                90,
                90
            )
            | (
                "tests/integrationtest/t/executor/parallel_apply.test",
                110,
                110
            )
            | ("tests/integrationtest/t/expression/builtin.test", 271, 271)
            | ("tests/integrationtest/t/expression/builtin.test", 276, 276)
            | ("tests/integrationtest/t/expression/builtin.test", 311, 311)
            | ("tests/integrationtest/t/expression/issues.test", 1864, 1864)
            | ("tests/integrationtest/t/expression/issues.test", 1867, 1867)
    )
}

#[test]
fn parenthesized_insert_source_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_parenthesized_insert_source(record)
        })
        .collect();
    assert_eq!(selected.len(), 10, "source-backed selector drifted");

    for record in selected {
        let statement = tidb_parser::parse(&record.input.sql).expect("parse selected Go row");
        assert_eq!(
            statement.restore().as_bytes(),
            record.restores[0].as_slice(),
            "restore selected Go row: {}",
            record.input.sql
        );
    }
}
