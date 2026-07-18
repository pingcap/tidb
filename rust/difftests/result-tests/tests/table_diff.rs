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

//! The result ring for real table access: a stateful `tidb_exec::Database` runs
//! an ordered script (`CREATE TABLE` / `INSERT` / `SELECT`) and must produce,
//! statement for statement, the same outcomes a real (mock-backed) TiDB session
//! does. The corpus is a directory of per-topic file pairs under
//! `corpus/table/` (`<topic>.txt` + `<topic>.golden.txt`); each topic is run
//! as an INDEPENDENT script against its own fresh `Database` (every topic so
//! far creates only its own uniquely-named table(s), never referencing
//! another topic's — this is what makes topics safely splittable in the
//! first place, and running each against a fresh `Database` keeps that
//! independence enforced rather than just assumed).
//!
//! Outcomes in the executor's domain (`OK` for side-effects, `RS:...` for
//! queries) are asserted; statements the Go side reports as `ERR` are skipped.
//!
//! Regenerate one topic's golden after changing it (gorun boots a fresh
//! mockstore per invocation, matching the fresh `Database` each topic runs
//! against here):
//! ```sh
//! grep -v '^##' rust/difftests/corpus/table/<topic>.txt \
//!   | go run ./rust/difftests/gorun 2>/dev/null | grep -E '^(RS:|OK|ERR)' \
//!   > rust/difftests/corpus/table/<topic>.golden.txt
//! ```
//! Table-execution goldens have shown real Go-engine nondeterminism before
//! (`GROUP_CONCAT(DISTINCT)`'s hash-map iteration order) — verify a NEW
//! topic's golden is stable across several repeated regenerations before
//! trusting it.
//!
//! Add a brand-new topic by creating a new `<topic>.txt` + regenerating its
//! `<topic>.golden.txt` the same way — never append to an existing topic's
//! file unless the addition genuinely continues that topic's own script.

use std::fs;
use std::path::PathBuf;

use difftest::{corpus_topics, difftest_root, parse_corpus, validate_executable_corpora};
use tidb_exec::{Database, Outcome};

fn corpus_dir() -> PathBuf {
    difftest_root().join("corpus").join("table")
}

/// Runs one statement against the Rust database, returning its outcome label.
fn run_stmt(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt = tidb_parser::parse(sql).map_err(|e| e.message)?;
    match db.run(&stmt).map_err(|e| format!("{e:?}"))? {
        Outcome::Done => Ok("OK".to_string()),
        Outcome::Rows(rs) => Ok(rs.label()),
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

        // Each topic is its own independent script against a fresh database.
        let mut db = Database::new();
        for (sql, want) in stmts.iter().zip(&golden) {
            total += 1;
            let got = run_stmt(&mut db, sql);
            if want == "ERR" {
                skipped += 1;
                continue; // out of the executor's domain; state may diverge, so stop asserting
            }
            match got {
                Ok(g) if &g == want => matched += 1,
                Ok(g) => failures.push(format!(
                    "\n--- [{topic}] {sql}\n  go  : {want}\n  rust: {g}"
                )),
                Err(e) => failures.push(format!(
                    "\n--- [{topic}] {sql}\n  go  : {want}\n  rust: <error: {e}>"
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
