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

const INDEX_LOOKUP_PUSHDOWN_TEST: &str =
    "tests/integrationtest/t/executor/index_lookup_pushdown.test";
const HINT_TEST: &str = "tests/integrationtest/t/planner/core/casetest/hint/hint.test";
const INTEGRATION_TEST: &str = "tests/integrationtest/t/planner/core/integration.test";
const PARTIAL_ORDER_TOPN_TEST: &str =
    "tests/integrationtest/t/planner/core/partial_order_topn.test";

const INDEX_LOOKUP_PUSHDOWN_LINES: &[usize] = &[50, 53, 60];
const HINT_LINES: &[usize] = &[
    32, 33, 34, 35, 38, 39, 42, 47, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 62, 63, 66, 67, 69, 71,
];
const INTEGRATION_LINES: &[usize] = &[2292];
const PARTIAL_ORDER_TOPN_LINES: &[usize] = &[
    237, 255, 256, 264, 271, 297, 298, 336, 342, 349, 358, 370, 371, 372, 381, 404, 407, 431, 435,
    483, 484, 487, 488,
];

fn has_selected_source_location(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    let line = record.input.start_line;
    match record.input.path.as_str() {
        INDEX_LOOKUP_PUSHDOWN_TEST => INDEX_LOOKUP_PUSHDOWN_LINES.contains(&line),
        HINT_TEST => HINT_LINES.contains(&line),
        INTEGRATION_TEST => INTEGRATION_LINES.contains(&line),
        PARTIAL_ORDER_TOPN_TEST => PARTIAL_ORDER_TOPN_LINES.contains(&line),
        _ => false,
    }
}

fn selected_index_level_hint(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    has_selected_source_location(record)
        && record.outcome == GoOutcome::Accepted
        && record.statement_count == 1
}

#[test]
fn index_level_hint_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| selected_index_level_hint(record))
        .collect();
    assert_eq!(selected.len(), 51, "source-backed selector drifted");

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
