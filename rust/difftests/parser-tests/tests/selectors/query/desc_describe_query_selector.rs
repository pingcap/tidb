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

// The complete checked Go-oracle population that exercises Go's
// DESC/DESCRIBE query-target routing through the existing Query EXPLAIN
// envelope. The nearby eight UPDATE/DELETE rows are deliberately excluded:
// they belong to the separate DML explain contract.
const DESC_DESCRIBE_QUERY_FIXTURES: [(&str, usize); 50] = [
    ("tests/integrationtest/t/ddl/db_partition.test", 1349),
    ("tests/integrationtest/t/executor/jointest/join.test", 1083),
    (
        "tests/integrationtest/t/executor/partition/issues.test",
        266,
    ),
    (
        "tests/integrationtest/t/executor/partition/issues.test",
        282,
    ),
    (
        "tests/integrationtest/t/executor/partition/issues.test",
        295,
    ),
    (
        "tests/integrationtest/t/executor/partition/issues.test",
        307,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        14,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        16,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        18,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        20,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        22,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        24,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        26,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        28,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        30,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        32,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        55,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        57,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        59,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        80,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        82,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        84,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        86,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        88,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        90,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        92,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        94,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        96,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        98,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        121,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        123,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        125,
    ),
    (
        "tests/integrationtest/t/explain_generate_column_substitute.test",
        143,
    ),
    ("tests/integrationtest/t/explain_indexmerge_stats.test", 43),
    ("tests/integrationtest/t/infoschema/infoschema.test", 281),
    (
        "tests/integrationtest/t/planner/core/partition_pruner.test",
        1022,
    ),
    (
        "tests/integrationtest/t/planner/core/partition_pruner.test",
        1023,
    ),
    (
        "tests/integrationtest/t/planner/core/partition_pruner.test",
        1024,
    ),
    (
        "tests/integrationtest/t/planner/core/partition_pruner.test",
        1033,
    ),
    (
        "tests/integrationtest/t/planner/core/partition_pruner.test",
        1034,
    ),
    (
        "tests/integrationtest/t/planner/core/partition_pruner.test",
        1035,
    ),
    ("tests/integrationtest/t/select.test", 187),
    ("tests/integrationtest/t/select.test", 188),
    ("tests/integrationtest/t/select.test", 190),
    ("tests/integrationtest/t/select.test", 198),
    ("tests/integrationtest/t/select.test", 199),
    ("tests/integrationtest/t/select.test", 200),
    ("tests/integrationtest/t/select.test", 203),
    ("tests/integrationtest/t/select.test", 204),
    ("tests/integrationtest/t/select.test", 205),
];

#[test]
fn desc_describe_query_static_go_rows_match() {
    let expected: BTreeSet<_> = DESC_DESCRIBE_QUERY_FIXTURES.into_iter().collect();
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
