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

fn has_unquoted_keyword(sql: &str, keyword: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut quote = None;
    let mut index = 0usize;
    while index < bytes.len() {
        match quote {
            Some(_) if bytes[index] == b'\\' && index + 1 < bytes.len() => index += 2,
            Some(delimiter) if bytes[index] == delimiter => {
                if index + 1 < bytes.len() && bytes[index + 1] == delimiter {
                    index += 2;
                } else {
                    quote = None;
                    index += 1;
                }
            }
            Some(_) => index += 1,
            None if matches!(bytes[index], b'\'' | b'"' | b'`') => {
                quote = Some(bytes[index]);
                index += 1;
            }
            None if bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_' => {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                if sql[start..index].eq_ignore_ascii_case(keyword) {
                    return true;
                }
            }
            None => index += 1,
        }
    }
    false
}

/// Selects the direct Go `parseCreateTableStmt` GLOBAL TEMPORARY branch. The
/// exact leader keeps `CREATE GLOBAL BINDING` and every other CREATE command
/// out of this DDL grammar check.
fn is_global_temporary_table(sql: &str) -> bool {
    let sql = sql.trim_start();
    let leader = "create global temporary table";
    sql.get(..leader.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(leader))
        && sql
            .get(leader.len()..)
            .is_some_and(|rest| rest.starts_with(char::is_whitespace))
        // These are separate typed grammar payloads. The existing table
        // structure intentionally does not retain them, so including them
        // would assert a lossy restore rather than this global-temporary
        // declaration/policy slice.
        && !has_unquoted_keyword(sql, "AFFINITY")
        && !has_unquoted_keyword(sql, "PARTITION")
        && !has_unquoted_keyword(sql, "AUTO_RANDOM")
}

#[test]
fn create_global_temporary_table_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_global_temporary_table(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 42, "source-backed selector drifted");

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
