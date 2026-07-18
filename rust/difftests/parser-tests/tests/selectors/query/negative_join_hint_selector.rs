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

use std::collections::BTreeSet;

use difftest::parser_oracle::{shared_golden, GoOutcome};

// Exact Go-accepted rows whose only missing grammar was one of Go's
// `parseTableLevelHint` negative join names: NO_HASH_JOIN, NO_MERGE_JOIN,
// NO_INDEX_JOIN, NO_INDEX_HASH_JOIN, or NO_INDEX_MERGE_JOIN. Nested LEADING
// trees and the malformed, Go-tolerated NO_HASH_JOIN comment are deliberately
// not selected because they require distinct parser contracts.
const NEGATIVE_JOIN_HINT_FIXTURES: [(&str, usize); 32] = [
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        44,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        45,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        46,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        47,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        48,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        49,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        50,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        51,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        52,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        53,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        54,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        55,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        56,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        57,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        67,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        68,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        69,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        70,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        71,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        72,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        73,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        74,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        85,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        86,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        87,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        88,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        89,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        90,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        93,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        94,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
        95,
    ),
    (
        "tests/integrationtest/t/planner/core/issuetest/planner_issue.test",
        608,
    ),
];

#[test]
fn negative_join_hint_static_go_rows_match() {
    let expected: BTreeSet<_> = NEGATIVE_JOIN_HINT_FIXTURES.into_iter().collect();
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| expected.contains(&(record.input.path.as_str(), record.input.start_line)))
        .collect();
    assert_eq!(selected.len(), expected.len(), "source fixture drifted");

    for record in selected {
        assert_eq!(record.outcome, GoOutcome::Accepted, "{}", record.input.sql);
        assert_eq!(record.statement_count, 1, "{}", record.input.sql);
        let statement = tidb_parser::parse(&record.input.sql)
            .unwrap_or_else(|error| panic!("{}: {error:?}", record.input.sql));
        assert_eq!(
            statement.restore().as_bytes(),
            record.restores[0].as_slice(),
            "{}",
            record.input.sql
        );
    }
}
