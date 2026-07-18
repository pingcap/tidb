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

// The two queue leaders are the source anchors for this branch, but the
// source-shaped parser action also closes every accepted direct FIRST/LAST
// interval-bound row in the checked integration corpus. Keep all 25 anchors
// here so the global differential delta cannot hide unclaimed siblings.
const INTERVAL_PARTITION_ROWS: [(&str, usize); 25] = [
    ("tests/integrationtest/t/ddl/db_partition.test", 1557),
    ("tests/integrationtest/t/ddl/db_partition.test", 1558),
    ("tests/integrationtest/t/ddl/db_partition.test", 1575),
    ("tests/integrationtest/t/ddl/db_partition.test", 1580),
    ("tests/integrationtest/t/ddl/db_partition.test", 1582),
    ("tests/integrationtest/t/ddl/db_partition.test", 1584),
    ("tests/integrationtest/t/ddl/db_partition.test", 1588),
    ("tests/integrationtest/t/ddl/db_partition.test", 1589),
    ("tests/integrationtest/t/ddl/db_partition.test", 1596),
    ("tests/integrationtest/t/ddl/db_partition.test", 1598),
    ("tests/integrationtest/t/ddl/db_partition.test", 1600),
    ("tests/integrationtest/t/ddl/db_partition.test", 1602),
    ("tests/integrationtest/t/ddl/db_partition.test", 1642),
    ("tests/integrationtest/t/ddl/db_partition.test", 1645),
    ("tests/integrationtest/t/ddl/db_partition.test", 1654),
    ("tests/integrationtest/t/ddl/db_partition.test", 1656),
    ("tests/integrationtest/t/ddl/db_partition.test", 1661),
    ("tests/integrationtest/t/ddl/db_partition.test", 1663),
    ("tests/integrationtest/t/ddl/db_partition.test", 1667),
    ("tests/integrationtest/t/ddl/db_partition.test", 1669),
    ("tests/integrationtest/t/ddl/db_partition.test", 1673),
    ("tests/integrationtest/t/ddl/db_partition.test", 1675),
    ("tests/integrationtest/t/ddl/partition.test", 198),
    ("tests/integrationtest/t/ddl/partition.test", 206),
    ("tests/integrationtest/t/ddl/partition.test", 207),
];

#[test]
fn alter_interval_partition_source_rows_match_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && INTERVAL_PARTITION_ROWS
                    .contains(&(record.input.path.as_str(), record.input.start_line))
        })
        .collect();
    assert_eq!(
        selected.len(),
        INTERVAL_PARTITION_ROWS.len(),
        "source-backed selector drifted"
    );

    let mut failures = Vec::new();
    for record in selected {
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
