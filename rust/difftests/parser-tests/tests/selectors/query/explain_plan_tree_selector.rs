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

fn is_target(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    if record.outcome != GoOutcome::Accepted || record.statement_count != 1 {
        return false;
    }
    let sql = record.input.sql.trim_start();
    if !sql
        .get(.."explain format='plan_tree'".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("explain format='plan_tree'"))
    {
        return false;
    }
    matches!(
        (record.input.path.as_str(), record.input.start_line),
        (
            "tests/integrationtest/t/planner/core/casetest/predicate_simplification.test",
            345
        ) | (
            "tests/integrationtest/t/planner/core/casetest/rule/rule_join_reorder.test",
            529 | 530 | 533
        )
    )
}

#[test]
fn explain_plan_tree_source_rows_match_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records.iter().filter(|record| is_target(record)).collect();
    // These are the four exact source anchors for this EXPLAIN/LEADING family;
    // malformed hint behavior is covered by the direct source test.
    assert_eq!(selected.len(), 4, "EXPLAIN plan_tree selector drifted");

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

#[test]
fn explain_hint_source_row_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && (record.input.path.as_str(), record.input.start_line)
                    == ("tests/integrationtest/t/planner/core/plan.test", 4)
        })
        .collect();
    assert_eq!(selected.len(), 1, "EXPLAIN hint source selector drifted");

    let record = selected[0];
    let statement = tidb_parser::parse(&record.input.sql).expect("parse EXPLAIN hint source row");
    assert_eq!(
        statement.restore().as_bytes(),
        record.restores[0].as_slice(),
        "{}:{}",
        record.input.path,
        record.input.start_line
    );
}
