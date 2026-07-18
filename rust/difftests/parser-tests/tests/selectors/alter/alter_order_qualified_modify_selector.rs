// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

#![allow(missing_docs)]

//! Checked Go-oracle rows for ALTER TABLE ORDER BY and qualified MODIFY.

use difftest::parser_oracle::{shared_golden, GoOutcome};

const ROWS: [(&str, usize); 3] = [
    ("tests/integrationtest/t/ddl/db.test", 110),
    ("tests/integrationtest/t/ddl/db.test", 114),
    ("tests/integrationtest/t/ddl/db_integration.test", 256),
];

#[test]
fn alter_order_qualified_modify_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && ROWS.contains(&(record.input.path.as_str(), record.input.start_line))
        })
        .collect();
    assert_eq!(selected.len(), ROWS.len(), "source-backed selector drifted");

    let failures: Vec<_> = selected
        .into_iter()
        .filter_map(|record| match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {
                None
            }
            Ok(statement) => Some(format!(
                "{}:{}\n  go: {}\n  rust: {}",
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
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
