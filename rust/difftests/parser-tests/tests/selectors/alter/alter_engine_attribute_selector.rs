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

#![allow(missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

/// Integration rows whose root ALTER action is ENGINE_ATTRIBUTE. The
/// storage-class fixture intentionally joins this selector: its payload is
/// still the same parser-owned StringName branch, while STORAGE_CLASS rows
/// remain outside this leaf.
const ENGINE_ATTRIBUTE_ROWS: [(&str, usize); 18] = [
    ("tests/integrationtest/t/ddl/db_table.test", 89),
    ("tests/integrationtest/t/ddl/storage_class.test", 6),
    ("tests/integrationtest/t/ddl/storage_class.test", 15),
    ("tests/integrationtest/t/ddl/storage_class.test", 16),
    ("tests/integrationtest/t/ddl/storage_class.test", 28),
    ("tests/integrationtest/t/ddl/storage_class.test", 31),
    ("tests/integrationtest/t/ddl/storage_class.test", 33),
    ("tests/integrationtest/t/ddl/storage_class.test", 49),
    ("tests/integrationtest/t/ddl/storage_class.test", 65),
    ("tests/integrationtest/t/ddl/storage_class.test", 78),
    ("tests/integrationtest/t/ddl/storage_class.test", 86),
    ("tests/integrationtest/t/ddl/storage_class.test", 88),
    ("tests/integrationtest/t/ddl/storage_class.test", 117),
    ("tests/integrationtest/t/ddl/storage_class.test", 123),
    ("tests/integrationtest/t/ddl/storage_class.test", 129),
    ("tests/integrationtest/t/ddl/storage_class.test", 132),
    ("tests/integrationtest/t/ddl/storage_class.test", 142),
    ("tests/integrationtest/t/ddl/storage_class.test", 145),
];

#[test]
fn alter_engine_attribute_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && ENGINE_ATTRIBUTE_ROWS
                    .contains(&(record.input.path.as_str(), record.input.start_line))
        })
        .collect();
    assert_eq!(rows.len(), ENGINE_ATTRIBUTE_ROWS.len(), "selector drifted");

    let failures: Vec<_> = rows
        .into_iter()
        .filter_map(|record| match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {
                None
            }
            Ok(statement) => Some(format!(
                "{}:{}: {}\n  go: {}\n  rust: {}",
                record.input.path,
                record.input.start_line,
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => Some(format!(
                "{}:{}: {}\n  parse error: {error:?}",
                record.input.path, record.input.start_line, record.input.sql
            )),
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
