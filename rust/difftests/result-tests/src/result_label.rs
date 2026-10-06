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

//! The `gorun` oracle's byte-safe differential row label, shared by
//! `query_diff` and `table_diff`.
//!
//! This is a straight port of the label format the deleted `tidb-exec`
//! `Database` engine's `ResultSet::label` produced (see `git show
//! e8369b73e2^:rust/crates/tidb-exec/src/result.rs`) -- the corpus and its
//! golden files were captured against that format, and it never depended on
//! the dead engine's internals, only on `Datum`. The live engine
//! (`tidb-executor`/`tidb-session`) returns plain `Vec<Vec<Datum>>` rows, so
//! this reproduces the same rendering over that shape instead of importing
//! it from anywhere: `RS:<row>;<row>;...`, each row `c1|c2|...`.
#![allow(dead_code)]

use tidb_datatype::Datum;

const RESULT_BYTES_HEX_PREFIX: &str = "BYTES_HEX:";
const RESULT_TEXT_ESCAPE_PREFIX: &str = "TEXT:";

/// Renders `rows` in the `gorun` oracle's format. `ordered` must be `true`
/// exactly when the statement carried its own `ORDER BY` (rows are sorted
/// for comparison otherwise, matching the golden's own order-independence).
pub fn rows_label(rows: &[Vec<Datum>], ordered: bool) -> String {
    let mut rendered: Vec<String> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(result_cell_label)
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    if !ordered {
        rendered.sort();
    }
    format!("RS:{}", rendered.join(";"))
}

fn result_cell_label(value: &Datum) -> String {
    if value.is_null() {
        return "<nil>".to_owned();
    }
    let Some(bytes) = value.as_raw_bytes() else {
        return value
            .sql_string()
            .expect("non-string Datum SQL rendering is always valid UTF-8");
    };
    match std::str::from_utf8(bytes) {
        Ok(text) if text.contains(['\r', '\n']) => {
            format!("{RESULT_BYTES_HEX_PREFIX}{}", uppercase_hex(bytes))
        }
        Ok(text)
            if text.starts_with(RESULT_BYTES_HEX_PREFIX)
                || text.starts_with(RESULT_TEXT_ESCAPE_PREFIX) =>
        {
            format!("{RESULT_TEXT_ESCAPE_PREFIX}{text}")
        }
        Ok(text) => text.to_string(),
        Err(_) => format!("{RESULT_BYTES_HEX_PREFIX}{}", uppercase_hex(bytes)),
    }
}

fn uppercase_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(encoded, "{byte:02X}").expect("writing to String cannot fail");
    }
    encoded
}

/// Whether the parsed statement carries its own `ORDER BY` -- a `SELECT`'s
/// or a set operation's -- deciding whether [`rows_label`] may reorder rows
/// for comparison.
pub fn statement_is_ordered(stmt: &tidb_ast::Stmt) -> bool {
    let tidb_ast::Stmt::Query(query) = stmt else {
        return false;
    };
    match &**query {
        tidb_ast::QueryStmt::Select(select) => !select.order_by.is_empty(),
        tidb_ast::QueryStmt::SetOpr(set_opr) => !set_opr.order_by.is_empty(),
    }
}
