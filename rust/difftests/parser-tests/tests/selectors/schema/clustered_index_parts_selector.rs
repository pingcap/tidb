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

const FIXTURES: [(&str, usize); 6] = [
    ("tests/integrationtest/t/clustered_index.test", 8),
    ("tests/integrationtest/t/clustered_index.test", 14),
    ("tests/integrationtest/t/session/clustered_index.test", 4),
    ("tests/integrationtest/t/session/clustered_index.test", 11),
    ("tests/integrationtest/t/session/clustered_index.test", 22),
    ("tests/integrationtest/t/session/clustered_index.test", 27),
];

/// A coherent source family: Go's clustered-index fixtures use prefix parts
/// in table primary, unique, and ordinary indexes. Keep the exact rows as a
/// parser/restore gate so this shared AST boundary cannot regress back to a
/// plain list of column names.
#[test]
fn clustered_index_prefix_rows_match_go() {
    let records = shared_golden().expect("oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|record| FIXTURES.contains(&(record.input.path.as_str(), record.input.start_line)))
        .collect();
    assert_eq!(rows.len(), FIXTURES.len(), "source-backed selector drifted");

    let failures: Vec<_> = rows
        .into_iter()
        .filter_map(|record| {
            assert_eq!(record.outcome, GoOutcome::Accepted, "{}", record.input.sql);
            assert_eq!(record.statement_count, 1, "{}", record.input.sql);
            match tidb_parser::parse(&record.input.sql) {
                Ok(statement)
                    if statement.restore().as_bytes() == record.restores[0].as_slice() =>
                {
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
            }
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// This source statement combines a prefix key part with partition syntax.
/// The complete table shape now restores exactly, while the AST assertion
/// proves the prefix key was not discarded at the shared index boundary.
#[test]
fn clustered_key_part_with_partition_matches_go() {
    let records = shared_golden().expect("oracle");
    let record = records
        .iter()
        .find(|record| {
            (record.input.path.as_str(), record.input.start_line)
                == ("tests/integrationtest/t/cte.test", 9)
        })
        .expect("source fixture");
    assert_eq!(record.outcome, GoOutcome::Accepted);
    assert_eq!(record.statement_count, 1);
    let statement = tidb_parser::parse(&record.input.sql).expect("prefix key now parses");
    assert_eq!(
        statement.restore().as_bytes(),
        record.restores[0].as_slice()
    );
    let tidb_ast::Stmt::Ddl(ddl) = statement else {
        panic!("expected DDL envelope");
    };
    let tidb_ast::DdlStmt::CreateTable(table) = ddl.as_ref() else {
        panic!("expected CREATE TABLE");
    };
    let has_prefix = table.table_constraints.iter().any(|constraint| {
        matches!(
            constraint,
            tidb_ast::TableConstraint::Index(key)
                if matches!(
                    key.kind,
                    tidb_ast::IndexConstraintKind::Unique
                        | tidb_ast::IndexConstraintKind::UniqueKey
                        | tidb_ast::IndexConstraintKind::UniqueIndex
                ) && matches!(
                    key.parts.as_slice(),
                    [tidb_ast::IndexPart::Column { prefix_len: Some(3), .. }, ..]
                )
        )
    });
    assert!(
        has_prefix,
        "source prefix key must survive the AST boundary"
    );
}
