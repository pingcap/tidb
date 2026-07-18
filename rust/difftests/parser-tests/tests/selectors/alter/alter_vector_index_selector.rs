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
#[test]
fn alter_vector_index_static_go_rows_match() {
    let records = shared_golden().expect("oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|r| {
            let s = r.input.sql.trim_start().to_ascii_uppercase();
            r.outcome == GoOutcome::Accepted
                && r.statement_count == 1
                && s.starts_with("ALTER TABLE ")
                && s.contains(" ADD VECTOR INDEX")
        })
        .collect();
    assert_eq!(rows.len(), 2, "source-backed selector drifted");
    let failures: Vec<_> = rows
        .into_iter()
        .filter_map(|r| match tidb_parser::parse(&r.input.sql) {
            Ok(s) if s.restore().as_bytes() == r.restores[0].as_slice() => None,
            Ok(s) => Some(format!(
                "{}\n  go: {}\n rust: {}",
                r.input.sql,
                String::from_utf8_lossy(&r.restores[0]),
                s.restore()
            )),
            Err(e) => Some(format!("{}\n  parse error: {e:?}", r.input.sql)),
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
