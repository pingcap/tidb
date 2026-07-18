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

// These are the 109 checked Go-oracle CREATE/ALTER rows whose only missing
// grammar was the direct BLOB/TEXT field-type family.  They were selected by
// replacing exactly those seven type spellings with the existing TEXT path,
// then retaining only rows whose transformed Rust restore exactly matched the
// transformed Go restore.  Pin locations, not a Rust self-filter, so later
// parser changes cannot silently shrink this source-backed obligation.
const LOB_FIELD_TYPE_FIXTURES: [(&str, usize); 110] = [
    ("tests/integrationtest/t/common_collation.test", 12),
    ("tests/integrationtest/t/ddl/column_modify.test", 162),
    ("tests/integrationtest/t/ddl/column_modify.test", 164),
    ("tests/integrationtest/t/ddl/column_modify.test", 168),
    ("tests/integrationtest/t/ddl/column_type_change.test", 67),
    ("tests/integrationtest/t/ddl/column_type_change.test", 148),
    ("tests/integrationtest/t/ddl/column_type_change.test", 170),
    ("tests/integrationtest/t/ddl/column_type_change.test", 192),
    ("tests/integrationtest/t/ddl/column_type_change.test", 214),
    ("tests/integrationtest/t/ddl/column_type_change.test", 242),
    ("tests/integrationtest/t/ddl/column_type_change.test", 265),
    ("tests/integrationtest/t/ddl/column_type_change.test", 287),
    ("tests/integrationtest/t/ddl/column_type_change.test", 312),
    ("tests/integrationtest/t/ddl/column_type_change.test", 336),
    ("tests/integrationtest/t/ddl/column_type_change.test", 364),
    ("tests/integrationtest/t/ddl/column_type_change.test", 392),
    ("tests/integrationtest/t/ddl/column_type_change.test", 414),
    ("tests/integrationtest/t/ddl/column_type_change.test", 440),
    ("tests/integrationtest/t/ddl/column_type_change.test", 465),
    ("tests/integrationtest/t/ddl/column_type_change.test", 483),
    ("tests/integrationtest/t/ddl/column_type_change.test", 694),
    ("tests/integrationtest/t/ddl/column_type_change.test", 695),
    ("tests/integrationtest/t/ddl/column_type_change.test", 696),
    ("tests/integrationtest/t/ddl/column_type_change.test", 697),
    ("tests/integrationtest/t/ddl/column_type_change.test", 698),
    ("tests/integrationtest/t/ddl/column_type_change.test", 699),
    ("tests/integrationtest/t/ddl/column_type_change.test", 700),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1123),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1124),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1125),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1126),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1127),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1527),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1528),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1529),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1530),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1531),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1532),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1533),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1534),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1535),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1536),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1885),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1939),
    ("tests/integrationtest/t/ddl/db.test", 374),
    ("tests/integrationtest/t/ddl/db_integration.test", 175),
    ("tests/integrationtest/t/ddl/default_as_expression.test", 51),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        144,
    ),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        240,
    ),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        279,
    ),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        301,
    ),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        333,
    ),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        364,
    ),
    ("tests/integrationtest/t/ddl/integration.test", 38),
    ("tests/integrationtest/t/executor/aggregate.test", 191),
    ("tests/integrationtest/t/executor/charset.test", 151),
    ("tests/integrationtest/t/executor/chunk_reuse.test", 3),
    ("tests/integrationtest/t/executor/chunk_reuse.test", 6),
    ("tests/integrationtest/t/executor/chunk_reuse.test", 91),
    ("tests/integrationtest/t/executor/chunk_reuse.test", 113),
    ("tests/integrationtest/t/executor/executor.test", 309),
    ("tests/integrationtest/t/executor/executor.test", 310),
    ("tests/integrationtest/t/executor/executor.test", 870),
    ("tests/integrationtest/t/executor/insert.test", 164),
    ("tests/integrationtest/t/executor/insert.test", 519),
    ("tests/integrationtest/t/executor/insert.test", 527),
    ("tests/integrationtest/t/executor/insert.test", 534),
    ("tests/integrationtest/t/executor/insert.test", 1824),
    ("tests/integrationtest/t/executor/jointest/join.test", 141),
    (
        "tests/integrationtest/t/executor/partition/issues.test",
        124,
    ),
    ("tests/integrationtest/t/executor/prepared.test", 62),
    ("tests/integrationtest/t/executor/prepared.test", 87),
    ("tests/integrationtest/t/executor/write.test", 265),
    ("tests/integrationtest/t/expression/builtin.test", 708),
    ("tests/integrationtest/t/expression/builtin.test", 794),
    ("tests/integrationtest/t/expression/builtin.test", 807),
    ("tests/integrationtest/t/expression/builtin.test", 1274),
    ("tests/integrationtest/t/expression/builtin.test", 1402),
    ("tests/integrationtest/t/expression/builtin.test", 1407),
    ("tests/integrationtest/t/expression/builtin.test", 1412),
    ("tests/integrationtest/t/expression/builtin.test", 1417),
    ("tests/integrationtest/t/expression/builtin.test", 1422),
    ("tests/integrationtest/t/expression/builtin.test", 1427),
    ("tests/integrationtest/t/expression/builtin.test", 1432),
    ("tests/integrationtest/t/expression/issues.test", 136),
    ("tests/integrationtest/t/expression/issues.test", 141),
    ("tests/integrationtest/t/expression/issues.test", 146),
    ("tests/integrationtest/t/expression/issues.test", 151),
    ("tests/integrationtest/t/expression/issues.test", 208),
    ("tests/integrationtest/t/expression/issues.test", 671),
    ("tests/integrationtest/t/expression/issues.test", 737),
    ("tests/integrationtest/t/expression/json.test", 308),
    ("tests/integrationtest/t/new_character_set.test", 46),
    ("tests/integrationtest/t/new_character_set.test", 53),
    ("tests/integrationtest/t/new_character_set.test", 62),
    (
        "tests/integrationtest/t/planner/core/casetest/predicate_simplification.test",
        39,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/predicate_simplification.test",
        319,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/rule/rule_derive_topn_from_window.test",
        37,
    ),
    ("tests/integrationtest/t/planner/core/indexjoin.test", 182),
    (
        "tests/integrationtest/t/planner/core/integration.test",
        1972,
    ),
    (
        "tests/integrationtest/t/planner/core/integration.test",
        2197,
    ),
    (
        "tests/integrationtest/t/planner/core/issuetest/planner_issue.test",
        494,
    ),
    ("tests/integrationtest/t/planner/core/plan.test", 88),
    ("tests/integrationtest/t/planner/core/plan.test", 148),
    (
        "tests/integrationtest/t/planner/core/point_get_plan.test",
        22,
    ),
    (
        "tests/integrationtest/t/planner/core/point_get_plan.test",
        23,
    ),
    ("tests/integrationtest/t/session/session.test", 52),
    ("tests/integrationtest/t/session/session.test", 53),
    ("tests/integrationtest/t/session/temporary_table.test", 61),
    ("tests/integrationtest/t/statistics/integration.test", 4),
];

// These six rows cross the LOB type seam but retain an unmodelled CREATE
// TABLE partition suffix.  Before this type translation they stopped at the
// missing field type; afterwards they reach the pre-existing table-option
// fallback and therefore become restore mismatches.  Keep the transition
// visible and distinct from the exact-match selector above: it is a
// partition-porting obligation, not a LOB success claim.
const LOB_PARTITION_BOUNDARY_FIXTURES: [(&str, usize); 6] = [
    (
        "tests/integrationtest/t/executor/partition/issues.test",
        347,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/partition/partition_pruner.test",
        272,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/partition/partition_pruner.test",
        280,
    ),
    ("tests/integrationtest/t/table/partition.test", 269),
    ("tests/integrationtest/t/table/partition.test", 275),
    ("tests/integrationtest/t/table/partition.test", 448),
];

#[test]
fn blob_and_text_family_static_go_rows_match() {
    let expected: BTreeSet<_> = LOB_FIELD_TYPE_FIXTURES.into_iter().collect();
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| expected.contains(&(record.input.path.as_str(), record.input.start_line)))
        .collect();
    assert_eq!(selected.len(), 110, "source-backed selector drifted");

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

#[test]
fn blob_type_partition_rows_match_go() {
    let expected: BTreeSet<_> = LOB_PARTITION_BOUNDARY_FIXTURES.into_iter().collect();
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| expected.contains(&(record.input.path.as_str(), record.input.start_line)))
        .collect();
    assert_eq!(selected.len(), 6, "source-backed boundary selector drifted");

    for record in selected {
        assert_eq!(record.outcome, GoOutcome::Accepted, "{}", record.input.sql);
        assert_eq!(record.statement_count, 1, "{}", record.input.sql);
        let statement =
            tidb_parser::parse(&record.input.sql).expect("LOB partition grammar should parse");
        assert_eq!(
            statement.restore().as_bytes(),
            record.restores[0].as_slice(),
            "LOB partition row drifted from Go restore: {}:{}",
            record.input.path,
            record.input.start_line
        );
    }
}
