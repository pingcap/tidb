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

//! Replay every `corpus/table/` topic through a fresh live Session and
//! compare with recorded Go output. Each topic retains its own session state.
//! `ERR` goldens are counted as skipped; every `OK` and `RS:` result is asserted.
//! A divergence is evidence to investigate, not a reason to remove a fixture.
//!
//! Regenerate one topic's golden after changing it:
//! ```sh
//! grep -v '^##' rust/difftests/corpus/table/<topic>.txt \
//!   | go run ./rust/difftests/gorun 2>/dev/null | grep -E '^(RS:|OK|ERR)' \
//!   > rust/difftests/corpus/table/<topic>.golden.txt
//! ```

use difftest_result_tests::result_label;

use std::fs;
use std::path::PathBuf;

use difftest::{corpus_topics, difftest_root, parse_corpus, validate_executable_corpora};
use result_label::{rows_label, statement_is_ordered};
use tidb_executor::DriverError;
use tidb_session::{Session, StmtResult};

fn corpus_dir() -> PathBuf {
    difftest_root().join("corpus").join("table")
}

/// Runs one statement against a live [`Session`], returning its outcome
/// label.
fn run_stmt(session: &mut Session, sql: &str) -> Result<String, DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let ordered = statement_is_ordered(&stmt);
    match session.run(sql)? {
        StmtResult::Rows(rows) => Ok(rows_label(&rows, ordered)),
        StmtResult::Affected(_) | StmtResult::Done(_) => Ok("OK".to_owned()),
    }
}

#[test]
fn table_execution_matches_go_engine() {
    let root = difftest::parser_oracle::repo_root();
    validate_executable_corpora(&root).expect("executable corpus contract");
    let dir = corpus_dir();
    let mut failures = Vec::new();
    let mut matched = 0;
    let mut skipped = 0;
    let mut total = 0;

    for topic in corpus_topics(&dir) {
        let stmts = parse_corpus(&fs::read_to_string(dir.join(format!("{topic}.txt"))).unwrap());
        let golden: Vec<String> = fs::read_to_string(dir.join(format!("{topic}.golden.txt")))
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect();

        assert_eq!(
            stmts.len(),
            golden.len(),
            "corpus/table/{topic}: statement/golden count mismatch (regenerate {topic}.golden.txt)"
        );

        // Each topic is its own independent script against a fresh session.
        let mut session = Session::new();
        for (sql, want) in stmts.iter().zip(&golden) {
            total += 1;
            let got = run_stmt(&mut session, sql);
            if want == "ERR" {
                skipped += 1;
                continue; // out of the live engine's domain; state may diverge, so stop asserting
            }
            match got {
                Ok(g) if &g == want => matched += 1,
                Ok(g) => failures.push(format!(
                    "\n--- [{topic}] {sql}\n  go  : {want}\n  rust: {g}"
                )),
                Err(e) => failures.push(format!(
                    "\n--- [{topic}] {sql}\n  go  : {want}\n  rust: <error: {e:?}>"
                )),
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} in-domain statements diverged from real TiDB ({} skipped, {} total):{}",
        failures.len(),
        matched + failures.len(),
        skipped,
        total,
        failures.join("")
    );
}
