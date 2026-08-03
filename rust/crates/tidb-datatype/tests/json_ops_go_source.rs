// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The BinaryJSON operations answered by a real TiDB, not by ourselves.
//!
//! Every expectation in `json_ops_go_fixture.txt` was captured by running the
//! matching SQL through a Go tidb-server built from this worktree:
//!
//! ```text
//! SELECT JSON_EXTRACT('<doc>', '<path>');
//! SELECT JSON_REMOVE / JSON_SET / JSON_INSERT / JSON_REPLACE (...);
//! SELECT JSON_MERGE_PATCH / JSON_MERGE_PRESERVE / JSON_CONTAINS / JSON_OVERLAPS (...);
//! ```
//!
//! A self-round-trip cannot find the defects this file exists for. Before it
//! was written, the whole `tidb-datatype` suite passed while `JSON_SET` on
//! `$[last-1]` silently dropped the value and `JSON_OVERLAPS` recursed one
//! level too deep — 20 of these 830 rows disagreed with Go.
//!
//! `ERR` is any error: Go's `gorun` reports the failure without its text, so
//! only the fact of failing is pinned here.

use std::path::PathBuf;

use tidb_datatype::{
    contains_binary_json, merge_binary_json, merge_patch_binary_json, overlaps_binary_json,
    parse_json_path_expr, BinaryJSON, JSONModifyType,
};

/// One captured answer, rendered the way the fixture writes it.
fn evaluate(operation: &str, arguments: &[&str]) -> String {
    let parse = |text: &str| BinaryJSON::parse(text).map_err(|_| ());
    let path = |text: &str| parse_json_path_expr(text).map_err(|_| ());
    let outcome: Result<String, ()> = (|| match operation {
        "extract" => Ok(parse(arguments[0])?
            .extract(&[path(arguments[1])?])
            .map_err(|_| ())?
            .map_or_else(|| "NULL".to_owned(), |value| value.to_string())),
        "remove" => Ok(parse(arguments[0])?
            .remove(&[path(arguments[1])?])
            .map_err(|_| ())?
            .to_string()),
        "set" | "insert" | "replace" => {
            let mode = match operation {
                "set" => JSONModifyType::Set,
                "insert" => JSONModifyType::Insert,
                _ => JSONModifyType::Replace,
            };
            Ok(parse(arguments[0])?
                .modify(&[path(arguments[1])?], &[parse(arguments[2])?], mode)
                .map_err(|_| ())?
                .to_string())
        }
        "merge_patch" => Ok(
            merge_patch_binary_json(&[parse(arguments[0])?, parse(arguments[1])?])
                .map_err(|_| ())?
                .map_or_else(|| "NULL".to_owned(), |value| value.to_string()),
        ),
        "merge_preserve" => Ok(
            merge_binary_json(&[parse(arguments[0])?, parse(arguments[1])?])
                .map_err(|_| ())?
                .to_string(),
        ),
        "contains" => Ok(i32::from(
            contains_binary_json(&parse(arguments[0])?, &parse(arguments[1])?).map_err(|_| ())?,
        )
        .to_string()),
        "overlaps" => Ok(i32::from(
            overlaps_binary_json(&parse(arguments[0])?, &parse(arguments[1])?).map_err(|_| ())?,
        )
        .to_string()),
        other => panic!("the fixture named an operation this test cannot run: {other}"),
    })();
    outcome.unwrap_or_else(|()| "ERR".to_owned())
}

#[test]
fn binary_json_operations_answer_exactly_what_go_answers() {
    let fixture = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/json_ops_go_fixture.txt"),
    )
    .expect("the captured Go answers");
    let mut checked = 0usize;
    let mut failures = Vec::new();
    for line in fixture.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let (call, expected) = line
            .split_once("\t=>\t")
            .unwrap_or_else(|| panic!("fixture row has no expectation: {line}"));
        let fields: Vec<&str> = call.split('\t').collect();
        let actual = evaluate(fields[0], &fields[1..]);
        if actual != expected {
            failures.push(format!("{call}\n  go   = {expected}\n  rust = {actual}"));
        }
        checked += 1;
    }
    assert_eq!(checked, 830, "the captured corpus lost rows");
    assert!(
        failures.is_empty(),
        "{} of {checked} operations disagree with Go:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
