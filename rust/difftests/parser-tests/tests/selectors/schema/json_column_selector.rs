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

// These are the 92 checked Go-oracle rows whose only formerly-missing
// grammar is a top-level JSON column type. The broader lexical population is
// deliberately not selected: it also contains unsupported partition/table
// options and other column grammar. Keeping the exact source locations makes
// this a stable source-backed contract rather than a self-confirming Rust
// parser filter.
const JSON_COLUMN_FIXTURES: [(&str, usize); 92] = [
    ("tests/integrationtest/t/collation_agg_func.test", 42),
    ("tests/integrationtest/t/ddl/column_modify.test", 157),
    ("tests/integrationtest/t/ddl/column_modify.test", 159),
    ("tests/integrationtest/t/ddl/column_modify.test", 170),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1192),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1223),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1253),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1283),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1314),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1350),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1380),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1410),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1436),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1462),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1488),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1514),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1540),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1566),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1602),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1638),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1669),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1700),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1731),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1762),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1806),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1807),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1812),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1813),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1818),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1819),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1824),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1825),
    ("tests/integrationtest/t/ddl/constraint.test", 320),
    ("tests/integrationtest/t/ddl/db_integration.test", 469),
    ("tests/integrationtest/t/ddl/db_integration.test", 495),
    ("tests/integrationtest/t/ddl/db_integration.test", 519),
    ("tests/integrationtest/t/ddl/db_rename.test", 18),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        145,
    ),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        241,
    ),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        302,
    ),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        334,
    ),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        365,
    ),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        586,
    ),
    ("tests/integrationtest/t/executor/aggregate.test", 153),
    ("tests/integrationtest/t/executor/aggregate.test", 198),
    ("tests/integrationtest/t/executor/aggregate.test", 239),
    ("tests/integrationtest/t/executor/aggregate.test", 582),
    ("tests/integrationtest/t/executor/executor.test", 849),
    ("tests/integrationtest/t/executor/executor.test", 868),
    ("tests/integrationtest/t/executor/executor.test", 874),
    ("tests/integrationtest/t/executor/executor.test", 2192),
    ("tests/integrationtest/t/executor/executor.test", 2203),
    ("tests/integrationtest/t/executor/explain.test", 38),
    ("tests/integrationtest/t/executor/foreign_key.test", 386),
    ("tests/integrationtest/t/executor/foreign_key.test", 389),
    ("tests/integrationtest/t/executor/issues.test", 377),
    ("tests/integrationtest/t/executor/issues.test", 529),
    ("tests/integrationtest/t/executor/union_scan.test", 86),
    ("tests/integrationtest/t/executor/update.test", 685),
    ("tests/integrationtest/t/executor/write.test", 282),
    ("tests/integrationtest/t/executor/write.test", 286),
    ("tests/integrationtest/t/expression/builtin.test", 294),
    ("tests/integrationtest/t/expression/builtin.test", 302),
    ("tests/integrationtest/t/expression/cast.test", 33),
    ("tests/integrationtest/t/expression/enum_set.test", 19),
    ("tests/integrationtest/t/expression/issues.test", 93),
    ("tests/integrationtest/t/expression/issues.test", 501),
    ("tests/integrationtest/t/expression/issues.test", 842),
    ("tests/integrationtest/t/expression/issues.test", 1524),
    ("tests/integrationtest/t/expression/json.test", 11),
    ("tests/integrationtest/t/expression/json.test", 25),
    ("tests/integrationtest/t/expression/json.test", 49),
    ("tests/integrationtest/t/expression/json.test", 115),
    ("tests/integrationtest/t/expression/json.test", 326),
    ("tests/integrationtest/t/expression/json.test", 334),
    ("tests/integrationtest/t/expression/json.test", 386),
    ("tests/integrationtest/t/expression/json.test", 410),
    ("tests/integrationtest/t/expression/json.test", 501),
    ("tests/integrationtest/t/expression/json.test", 587),
    ("tests/integrationtest/t/expression/json.test", 595),
    ("tests/integrationtest/t/expression/json.test", 604),
    ("tests/integrationtest/t/expression/json.test", 612),
    ("tests/integrationtest/t/expression/misc.test", 175),
    (
        "tests/integrationtest/t/expression/multi_valued_index.test",
        3,
    ),
    (
        "tests/integrationtest/t/expression/multi_valued_index.test",
        49,
    ),
    (
        "tests/integrationtest/t/expression/multi_valued_index.test",
        61,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/pushdown/push_down.test",
        17,
    ),
    ("tests/integrationtest/t/planner/core/indexjoin.test", 145),
    (
        "tests/integrationtest/t/planner/core/indexmerge_path.test",
        236,
    ),
    (
        "tests/integrationtest/t/planner/core/integration.test",
        1863,
    ),
    ("tests/integrationtest/t/planner/core/plan_cache.test", 274),
    (
        "tests/integrationtest/t/types/json_binary_functions.test",
        2,
    ),
];

// `parse_column_type` is shared by CREATE TABLE and ALTER TABLE's
// ADD/MODIFY/CHANGE productions. These 28 source rows are the complete
// checked-oracle ALTER slice that becomes restorable through that seam.
const JSON_ALTER_COLUMN_FIXTURES: [(&str, usize); 28] = [
    ("tests/integrationtest/t/ddl/column_type_change.test", 107),
    ("tests/integrationtest/t/ddl/column_type_change.test", 429),
    ("tests/integrationtest/t/ddl/column_type_change.test", 430),
    ("tests/integrationtest/t/ddl/column_type_change.test", 431),
    ("tests/integrationtest/t/ddl/column_type_change.test", 432),
    ("tests/integrationtest/t/ddl/column_type_change.test", 433),
    ("tests/integrationtest/t/ddl/column_type_change.test", 434),
    ("tests/integrationtest/t/ddl/column_type_change.test", 435),
    ("tests/integrationtest/t/ddl/column_type_change.test", 436),
    ("tests/integrationtest/t/ddl/column_type_change.test", 497),
    ("tests/integrationtest/t/ddl/column_type_change.test", 499),
    ("tests/integrationtest/t/ddl/column_type_change.test", 501),
    ("tests/integrationtest/t/ddl/column_type_change.test", 914),
    ("tests/integrationtest/t/ddl/column_type_change.test", 915),
    ("tests/integrationtest/t/ddl/column_type_change.test", 916),
    ("tests/integrationtest/t/ddl/column_type_change.test", 917),
    ("tests/integrationtest/t/ddl/column_type_change.test", 918),
    ("tests/integrationtest/t/ddl/column_type_change.test", 919),
    ("tests/integrationtest/t/ddl/column_type_change.test", 920),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1181),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1182),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1183),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1184),
    ("tests/integrationtest/t/ddl/column_type_change.test", 1185),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        280,
    ),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        538,
    ),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        540,
    ),
    (
        "tests/integrationtest/t/ddl/default_as_expression.test",
        574,
    ),
];

#[test]
fn json_column_lexical_one_statement_matches_go() {
    let expected: BTreeSet<_> = JSON_COLUMN_FIXTURES.into_iter().collect();
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| expected.contains(&(record.input.path.as_str(), record.input.start_line)))
        .collect();
    assert_eq!(selected.len(), 92, "source-backed selector drifted");

    let mut failures = Vec::new();
    for record in selected {
        assert_eq!(record.outcome, GoOutcome::Accepted, "{}", record.input.sql);
        assert_eq!(record.statement_count, 1, "{}", record.input.sql);
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

#[test]
fn json_alter_column_lexical_one_statement_matches_go() {
    let expected: BTreeSet<_> = JSON_ALTER_COLUMN_FIXTURES.into_iter().collect();
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| expected.contains(&(record.input.path.as_str(), record.input.start_line)))
        .collect();
    assert_eq!(selected.len(), 28, "source-backed selector drifted");

    let mut failures = Vec::new();
    for record in selected {
        assert_eq!(record.outcome, GoOutcome::Accepted, "{}", record.input.sql);
        assert_eq!(record.statement_count, 1, "{}", record.input.sql);
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
