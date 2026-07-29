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

//! The result ring at the query level, re-pointed at the LIVE engine.
//!
//! `tidb-executor`/`tidb-session` must produce the same result rows a real
//! (mock-backed) TiDB session does for a table-less `SELECT` or set
//! operation, captured in `corpus/query_golden.txt` / `corpus/query/*` by
//! the `gorun` tool. The driver that used to check this ran the dead
//! `tidb-exec` `Database` engine (deleted in `e8369b73e2`); this re-points
//! the same corpus at the live engine, through [`tidb_session::Session`] --
//! the same path the TCP convergence node and every other in-process caller
//! use.
//!
//! Results in the live engine's domain (`RS:...`) are asserted; statements
//! the Go side reports as `ERR` (needing a table, or out of scope) are
//! counted but not required.
//!
//! EXPECT DIVERGENCES: see `table_diff.rs`'s own note -- this corpus has not
//! run against anything since the dead engine was removed, and the live
//! engine is a different engine from the one it was recorded against.
//!
//! Regenerate the golden after changing the corpus (drops TiDB's stderr logs):
//! ```sh
//! grep -v '^##' rust/difftests/corpus/query_statements.txt \
//!   | go run ./rust/difftests/gorun 2>/dev/null | grep -E '^(RS:|ERR)' \
//!   > rust/difftests/corpus/query_golden.txt
//! ```

#[path = "result_label.rs"]
mod result_label;

use std::fs;
use std::path::PathBuf;

use difftest::{difftest_root, parse_corpus};
use result_label::{rows_label, statement_is_ordered};
use tidb_session::{Session, StmtResult};

fn corpus_dir() -> PathBuf {
    difftest_root().join("corpus")
}

/// Parses and executes a table-less `SELECT` against a fresh session,
/// returning its result label.
fn rust_run(sql: &str) -> Result<String, String> {
    let stmt = tidb_parser::parse(sql).map_err(|e| e.message)?;
    let ordered = statement_is_ordered(&stmt);
    let mut session = Session::new();
    match session.run(sql).map_err(|e| format!("{e:?}"))? {
        StmtResult::Rows(rows) => Ok(rows_label(&rows, ordered)),
        StmtResult::Affected(_) | StmtResult::Done(_) => {
            Err("statement produced no rows".to_owned())
        }
    }
}

/// Runs one statements-file/golden-file pair, appending divergences to
/// `failures` (tagged with `label` so a multi-topic failure names its file).
fn run_pair(
    label: &str,
    stmts_path: &PathBuf,
    golden_path: &PathBuf,
    failures: &mut Vec<String>,
) -> (usize, usize) {
    let stmts = parse_corpus(&fs::read_to_string(stmts_path).unwrap());
    let golden: Vec<String> = fs::read_to_string(golden_path)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect();

    assert_eq!(
        stmts.len(),
        golden.len(),
        "[{label}] corpus/golden count mismatch (regenerate the golden)"
    );

    let mut matched = 0;
    let mut skipped = 0;
    for (sql, want) in stmts.iter().zip(&golden) {
        if want == "ERR" {
            skipped += 1;
            continue;
        }
        match rust_run(sql) {
            Ok(got) if &got == want => matched += 1,
            Ok(got) => failures.push(format!(
                "\n--- [{label}] {sql}\n  go  : {want}\n  rust: {got}"
            )),
            Err(e) => failures.push(format!(
                "\n--- [{label}] {sql}\n  go  : {want}\n  rust: <error: {e}>"
            )),
        }
    }
    (matched, skipped)
}

#[test]
fn query_result_matches_go_engine() {
    let dir = corpus_dir();
    let mut failures = Vec::new();
    let mut matched = 0;
    let mut skipped = 0;

    // The legacy single pair.
    let (m, s) = run_pair(
        "query_statements",
        &dir.join("query_statements.txt"),
        &dir.join("query_golden.txt"),
        &mut failures,
    );
    matched += m;
    skipped += s;

    // Per-topic pairs under `corpus/query/` -- `<topic>.txt` +
    // `<topic>.golden.txt`, one topic per builtin family, so parallel
    // agents each own their own pair (the same splittable-topic pattern
    // `table_diff` established).
    let topic_dir = dir.join("query");
    if topic_dir.is_dir() {
        let mut topics: Vec<PathBuf> = fs::read_dir(&topic_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p.extension().is_some_and(|x| x == "txt")
                    && !p.to_string_lossy().ends_with(".golden.txt")
            })
            .collect();
        topics.sort();
        for stmts_path in topics {
            let topic = stmts_path
                .file_stem()
                .unwrap()
                .to_string_lossy()
                .to_string();
            let golden_path = topic_dir.join(format!("{topic}.golden.txt"));
            assert!(
                golden_path.exists(),
                "[{topic}] has no golden file (generate it with gorun)"
            );
            let (m, s) = run_pair(&topic, &stmts_path, &golden_path, &mut failures);
            matched += m;
            skipped += s;
        }
    }

    // Every divergence below is a real gap against Go, printed in full so it
    // can be worked off. It is a ratchet, not a waiver: the count may only go
    // DOWN. A permanently red suite would destroy the signal every other gate
    // depends on, and deleting the cases would destroy the evidence -- so the
    // debt is carried as a number that fails the moment it grows.
    const KNOWN_DIVERGENCES: usize = 0;

    if failures.len() > KNOWN_DIVERGENCES {
        panic!(
            "{} of {} in-domain queries diverged from the Go engine, up from {} \
             ({} skipped) -- a new divergence appeared:{}",
            failures.len(),
            matched + failures.len(),
            KNOWN_DIVERGENCES,
            skipped,
            failures.join("")
        );
    }
    assert!(
        failures.len() >= KNOWN_DIVERGENCES,
        "only {} of {} queries diverge now, down from {}. Lower KNOWN_DIVERGENCES \
         to {} so the ratchet holds.",
        failures.len(),
        matched + failures.len(),
        KNOWN_DIVERGENCES,
        failures.len()
    );
}
