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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Differential test: the Rust `tidb-lexer` token stream must match, token for
//! token (offset + engine-neutral label), the production Go scanner's output
//! captured in `corpus/golden.txt`.
//!
//! Regenerate the golden dump after changing the corpus:
//! ```sh
//! grep -v '^##' rust/difftests/corpus/statements.txt \
//!   | go run ./rust/difftests/godump > rust/difftests/corpus/golden.txt
//! ```

use std::fs;
use std::path::PathBuf;

use difftest::{difftest_root, parse_corpus, parse_golden};
use tidb_lexer::Lexer;

fn corpus_dir() -> PathBuf {
    difftest_root().join("corpus")
}

/// Produces the Rust lexer's `(offset, label)` stream for one statement.
fn rust_labels(sql: &str) -> Vec<(usize, String)> {
    Lexer::new(sql)
        .tokenize()
        .into_iter()
        .map(|t| (t.offset, t.label()))
        .collect()
}

fn run_corpus(statements_file: &str, golden_file: &str) {
    let dir = corpus_dir();
    let statements = parse_corpus(&fs::read_to_string(dir.join(statements_file)).unwrap());
    let golden = parse_golden(&fs::read_to_string(dir.join(golden_file)).unwrap());

    assert_eq!(
        statements.len(),
        golden.len(),
        "corpus/golden statement count mismatch: {} statements vs {} golden blocks \
         (regenerate golden.txt)",
        statements.len(),
        golden.len()
    );

    let mut failures = Vec::new();
    for (idx, sql) in statements.iter().enumerate() {
        let got = rust_labels(sql);
        let want = golden.get(&idx).cloned().unwrap_or_default();
        if got != want {
            failures.push(format!(
                "\n--- statement #{idx}: {sql}\n  expected (go): {want:?}\n  got   (rust): {got:?}"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} statements diverged from the Go scanner:{}",
        failures.len(),
        statements.len(),
        failures.join("")
    );
}

/// Curated corpus: one token class per statement, hand-checked.
#[test]
fn lexer_matches_go_scanner() {
    run_corpus("statements.txt", "golden.txt");
}

/// Broad corpus sampled from tests/integrationtest, to surface divergences the
/// curated set does not anticipate.
#[test]
fn lexer_matches_go_scanner_real_corpus() {
    run_corpus("real_statements.txt", "real_golden.txt");
}
