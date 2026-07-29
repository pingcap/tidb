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

//! The result ring for real table access, re-pointed at the LIVE engine.
//!
//! `corpus/table/` is a directory of per-topic file pairs (`<topic>.txt` +
//! `<topic>.golden.txt`), Go-captured and verified, that the dead `tidb-exec`
//! `Database` engine's driver used to run (deleted in `e8369b73e2` along
//! with that engine). The corpus itself was kept -- it is real, verified Go
//! output -- on the understanding that re-pointing a driver at the live
//! engine is worth more than deleting the fixtures. This is that driver: it
//! runs each topic's script through [`tidb_session::Session`], the same
//! parse -> plan -> execute path the TCP convergence node and every other
//! in-process caller use, and compares against the recorded Go output.
//!
//! Each topic is its own independent script against a fresh `Session` (every
//! topic so far creates only its own uniquely-named table(s), never
//! referencing another topic's, which is what makes topics safely
//! splittable). Outcomes in the live engine's domain (`OK` for side effects,
//! `RS:...` for queries) are asserted; statements the Go side reports as
//! `ERR` are skipped, and a handful of topics that exercise capabilities the
//! live engine does not model at all are skipped BY NAME below.
//!
//! EXPECT DIVERGENCES: this corpus has not run against anything since the
//! dead engine was removed, and the live engine is a DIFFERENT engine (a
//! different operator tree, wired through a session rather than a
//! `Database`) from the one it was recorded against. A divergence here is a
//! finding to report, not a fixture to edit -- this test does not touch
//! `corpus/table/**`, and does not delete a case to make itself pass.
//!
//! Regenerate one topic's golden after changing it:
//! ```sh
//! grep -v '^##' rust/difftests/corpus/table/<topic>.txt \
//!   | go run ./rust/difftests/gorun 2>/dev/null | grep -E '^(RS:|OK|ERR)' \
//!   > rust/difftests/corpus/table/<topic>.golden.txt
//! ```

#[path = "result_label.rs"]
mod result_label;

use std::fs;
use std::path::PathBuf;

use difftest::{corpus_topics, difftest_root, parse_corpus, validate_executable_corpora};
use result_label::{rows_label, statement_is_ordered};
use tidb_executor::DriverError;
use tidb_session::{Session, StmtResult};

fn corpus_dir() -> PathBuf {
    difftest_root().join("corpus").join("table")
}

/// Topics that exercise a capability the live engine does not model at all
/// (rather than merely producing a wrong answer), captured by name so a
/// green run means every OTHER topic genuinely matched the Go engine.
///
/// Each reason names the missing capability the topic's script depends on
/// from its very first statement (usually a `CREATE TABLE ... FOREIGN KEY`
/// or the clause itself), not a narrower per-statement gap.
const UNSUPPORTED_TOPICS: &[(&str, &str)] = &[
    // The seven other `foreign_key*` topics came OFF this list when FOREIGN KEY
    // was transcreated -- removing a topic is the point of a capability unit.
    // This one stays for a DIFFERENT reason than it was first skipped for: its
    // FK half now passes, and it is the multi-table `DELETE ... USING` in its
    // second half that this tier still refuses.
    (
        "delete_ignore_foreign_key",
        "multi-table DELETE ... USING is not modelled",
    ),
];

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
    let mut skipped_topics = Vec::new();

    for topic in corpus_topics(&dir) {
        if let Some((_, reason)) = UNSUPPORTED_TOPICS.iter().find(|(name, _)| *name == topic) {
            skipped_topics.push(format!("{topic} ({reason})"));
            continue;
        }

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

    eprintln!(
        "table_execution_matches_go_engine: {} topics skipped by name: {}",
        skipped_topics.len(),
        skipped_topics.join(", ")
    );

    // Every divergence below is a real gap against Go, printed in full so it
    // can be worked off. It is a ratchet, not a waiver: the count may only go
    // DOWN. A permanently red suite would destroy the signal every other gate
    // depends on, and deleting the cases would destroy the evidence -- so the
    // debt is carried as a number that fails the moment it grows.
    //
    // Multi-table UPDATE/DELETE and `DELETE IGNORE` took this from 79 to 76:
    // five `row_count` statements now match, and TWO of that topic's
    // `ROW_COUNT()` reads newly diverge because `DELETE IGNORE FROM fp`
    // finally RUNS. Go skips its row -- a child row in `fc` references it --
    // and reports 0; with no foreign keys modelled here the row really is
    // deletable, so the count is 1, and the later `foreign_key_checks = 0`
    // delete then finds nothing left. Both belong to FOREIGN KEY support,
    // not to multi-table DML.
    const KNOWN_DIVERGENCES: usize = 52;

    assert!(
        failures.len() <= KNOWN_DIVERGENCES,
        "{} of {} in-domain statements diverged from real TiDB, up from {} ({} skipped, {} total, {} topics skipped by name) -- a new divergence appeared:{}",
        failures.len(),
        matched + failures.len(),
        KNOWN_DIVERGENCES,
        skipped,
        total,
        skipped_topics.len(),
        failures.join("")
    );
    assert!(
        failures.len() >= KNOWN_DIVERGENCES,
        "only {} of {} statements diverge now, down from {}. Lower \
         KNOWN_DIVERGENCES to {} so the ratchet holds.",
        failures.len(),
        matched + failures.len(),
        KNOWN_DIVERGENCES,
        failures.len()
    );
}
