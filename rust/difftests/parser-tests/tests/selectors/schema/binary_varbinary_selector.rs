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

const DIRECT_BINARY_ROWS: &[(&str, usize, usize)] = &[
    (
        "tests/integrationtest/t/collation_check_use_collation.test",
        89,
        89,
    ),
    (
        "tests/integrationtest/t/collation_check_use_collation.test",
        99,
        99,
    ),
    ("tests/integrationtest/t/common_collation.test", 17, 17),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        61,
        61,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        64,
        64,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        348,
        348,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        349,
        349,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        376,
        376,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        377,
        377,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        426,
        426,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        427,
        427,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        650,
        650,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        651,
        651,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        653,
        653,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        655,
        655,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        657,
        657,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        659,
        659,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        660,
        660,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        674,
        674,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        675,
        675,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        676,
        676,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        677,
        677,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        678,
        678,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        679,
        679,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        680,
        680,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1075,
        1075,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1076,
        1076,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1077,
        1077,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1078,
        1078,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1079,
        1079,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1091,
        1091,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1092,
        1092,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1093,
        1093,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1094,
        1094,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1095,
        1095,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1475,
        1475,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1476,
        1476,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1477,
        1477,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1478,
        1478,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1479,
        1479,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1480,
        1480,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1481,
        1481,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1482,
        1482,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1483,
        1483,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1484,
        1484,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1501,
        1501,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1502,
        1502,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1503,
        1503,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1504,
        1504,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1505,
        1505,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1506,
        1506,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1507,
        1507,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1508,
        1508,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1509,
        1509,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1510,
        1510,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1893,
        1893,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1909,
        1909,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1915,
        1915,
    ),
    (
        "tests/integrationtest/t/ddl/column_type_change.test",
        1904,
        1904,
    ),
    ("tests/integrationtest/t/ddl/db.test", 35, 35),
    ("tests/integrationtest/t/ddl/db.test", 38, 38),
    ("tests/integrationtest/t/ddl/db_integration.test", 102, 102),
    ("tests/integrationtest/t/ddl/db_integration.test", 108, 108),
    ("tests/integrationtest/t/ddl/db_integration.test", 114, 114),
    ("tests/integrationtest/t/ddl/db_integration.test", 120, 120),
    ("tests/integrationtest/t/executor/charset.test", 100, 100),
    ("tests/integrationtest/t/executor/charset.test", 21, 21),
    ("tests/integrationtest/t/executor/executor.test", 1487, 1487),
    ("tests/integrationtest/t/executor/insert.test", 152, 152),
    ("tests/integrationtest/t/executor/insert.test", 158, 158),
    ("tests/integrationtest/t/executor/insert.test", 564, 564),
    ("tests/integrationtest/t/executor/insert.test", 577, 577),
    ("tests/integrationtest/t/executor/insert.test", 1168, 1168),
    ("tests/integrationtest/t/executor/insert.test", 1173, 1173),
    ("tests/integrationtest/t/executor/insert.test", 1656, 1656),
    ("tests/integrationtest/t/executor/point_get.test", 100, 100),
    ("tests/integrationtest/t/executor/point_get.test", 128, 128),
    ("tests/integrationtest/t/executor/point_get.test", 156, 156),
    ("tests/integrationtest/t/executor/rowid.test", 35, 35),
    ("tests/integrationtest/t/explain_easy.test", 211, 211),
    ("tests/integrationtest/t/expression/builtin.test", 511, 511),
    ("tests/integrationtest/t/expression/builtin.test", 622, 622),
    ("tests/integrationtest/t/expression/builtin.test", 721, 721),
    ("tests/integrationtest/t/expression/builtin.test", 758, 758),
    ("tests/integrationtest/t/expression/builtin.test", 767, 767),
    ("tests/integrationtest/t/expression/builtin.test", 789, 789),
    ("tests/integrationtest/t/expression/builtin.test", 821, 821),
    ("tests/integrationtest/t/expression/builtin.test", 826, 826),
    ("tests/integrationtest/t/expression/builtin.test", 854, 854),
    ("tests/integrationtest/t/expression/builtin.test", 864, 864),
    ("tests/integrationtest/t/expression/builtin.test", 874, 874),
    ("tests/integrationtest/t/expression/builtin.test", 897, 897),
    ("tests/integrationtest/t/expression/builtin.test", 953, 953),
    (
        "tests/integrationtest/t/expression/builtin.test",
        1471,
        1471,
    ),
    (
        "tests/integrationtest/t/expression/charset_and_collation.test",
        412,
        412,
    ),
    (
        "tests/integrationtest/t/expression/charset_and_collation.test",
        417,
        417,
    ),
    (
        "tests/integrationtest/t/expression/charset_and_collation.test",
        3,
        3,
    ),
    (
        "tests/integrationtest/t/expression/constant_fold.test",
        3,
        3,
    ),
    ("tests/integrationtest/t/expression/issues.test", 77, 77),
    ("tests/integrationtest/t/expression/issues.test", 827, 827),
    ("tests/integrationtest/t/expression/issues.test", 1012, 1027),
    ("tests/integrationtest/t/expression/json.test", 297, 297),
    ("tests/integrationtest/t/expression/json.test", 398, 398),
    ("tests/integrationtest/t/expression/json.test", 507, 507),
    ("tests/integrationtest/t/expression/misc.test", 154, 154),
    ("tests/integrationtest/t/expression/uuid.test", 72, 72),
    ("tests/integrationtest/t/expression/uuid.test", 73, 73),
    ("tests/integrationtest/t/expression/uuid.test", 74, 74),
    ("tests/integrationtest/t/expression/uuid.test", 78, 80),
    (
        "tests/integrationtest/t/new_character_set_builtin.test",
        5,
        5,
    ),
    (
        "tests/integrationtest/t/new_character_set_builtin.test",
        31,
        31,
    ),
    (
        "tests/integrationtest/t/new_character_set_builtin.test",
        69,
        69,
    ),
    (
        "tests/integrationtest/t/new_character_set_builtin.test",
        78,
        78,
    ),
    (
        "tests/integrationtest/t/new_character_set_builtin.test",
        87,
        87,
    ),
    (
        "tests/integrationtest/t/new_character_set_builtin.test",
        96,
        96,
    ),
    (
        "tests/integrationtest/t/new_character_set_builtin.test",
        107,
        107,
    ),
    (
        "tests/integrationtest/t/new_character_set_builtin.test",
        124,
        124,
    ),
    (
        "tests/integrationtest/t/new_character_set_builtin.test",
        144,
        144,
    ),
    (
        "tests/integrationtest/t/new_character_set_builtin.test",
        164,
        164,
    ),
    (
        "tests/integrationtest/t/new_character_set_builtin.test",
        172,
        172,
    ),
    (
        "tests/integrationtest/t/new_character_set_builtin.test",
        246,
        246,
    ),
    (
        "tests/integrationtest/t/new_character_set_builtin.test",
        260,
        260,
    ),
    (
        "tests/integrationtest/t/new_character_set_builtin.test",
        269,
        269,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/expression_rewriter.test",
        21,
        21,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/predicate_simplification.test",
        47,
        62,
    ),
    (
        "tests/integrationtest/t/planner/core/indexmerge_path.test",
        413,
        413,
    ),
    (
        "tests/integrationtest/t/planner/core/integration.test",
        1272,
        1272,
    ),
    (
        "tests/integrationtest/t/planner/core/integration.test",
        1325,
        1325,
    ),
    (
        "tests/integrationtest/t/planner/core/plan_cache.test",
        1540,
        1546,
    ),
];

#[test]
fn direct_binary_and_varbinary_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            DIRECT_BINARY_ROWS.contains(&(
                record.input.path.as_str(),
                record.input.start_line,
                record.input.end_line,
            )) && record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
        })
        .collect();
    assert_eq!(selected.len(), 129, "source-backed selector drifted");

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
