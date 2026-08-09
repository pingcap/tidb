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

//! Executable Rust counterpart of `pkg/parser/differential_test.go`.
//!
//! The Go test only logs mismatches, so a divergent hand parser still exits
//! successfully. This counterpart replays every Go-reference-accepted row
//! against the checked Go restore oracle and fails on any parse or restore
//! difference.

use super::*;

const SOURCE: &str = include_str!("../../../../difftests/corpus/parser/go_test_differential.txt");
const GOLDEN: &str =
    include_str!("../../../../difftests/corpus/parser/go_test_differential.golden.txt");

fn source_rows() -> Vec<&'static str> {
    SOURCE
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with("##"))
        .collect()
}

fn golden_rows() -> Vec<String> {
    let mut rows = Vec::new();
    let mut current: Option<Vec<&str>> = None;
    for line in GOLDEN.split('\n') {
        if line.starts_with("#IDX ") {
            current = Some(Vec::new());
        } else if line == "#END" {
            rows.push(
                current
                    .take()
                    .expect("golden #END must close one row")
                    .join("\n"),
            );
        } else if let Some(lines) = current.as_mut() {
            lines.push(line);
        }
    }
    rows
}

/// `pkg/parser/differential_test.go::TestDifferential`.
#[test]
fn test_differential() {
    assert!(
        SOURCE.contains("## CREATE TABLE t (a GEOMETRY)"),
        "the one Go-reference-rejected row must remain explicit"
    );
    let sources = source_rows();
    let expected = golden_rows();
    assert_eq!(sources.len(), 670, "Go accepts 670 of its 671 source rows");
    assert_eq!(expected.len(), sources.len(), "source/golden row count");

    let mut failures = Vec::new();
    for (index, (sql, expected)) in sources.iter().zip(expected.iter()).enumerate() {
        match parse(sql) {
            Ok(statement) => {
                let actual = statement.restore();
                if &actual != expected {
                    failures.push(format!(
                        "#{index}: {sql}\n  Go:   {expected}\n  Rust: {actual}"
                    ));
                }
            }
            Err(error) => failures.push(format!(
                "#{index}: {sql}\n  Go:   {expected}\n  Rust: parse error at {}: {}",
                error.offset, error.message
            )),
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} Go differential rows diverged:\n{}",
        failures.len(),
        sources.len(),
        failures.join("\n")
    );
}
