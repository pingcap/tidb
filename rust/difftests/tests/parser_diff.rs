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

//! Differential test: `tidb-parser` must parse each corpus statement and
//! restore it to exactly the canonical SQL the Go AST produces
//! (`format.DefaultRestoreFlags`). The corpus is a directory of per-topic
//! file pairs under `corpus/parser/` (`<topic>.txt` + `<topic>.golden.txt`);
//! see `difftest::load_corpus_dir`.
//!
//! Regenerate one topic's golden after changing it:
//! ```sh
//! grep -v '^##' rust/difftests/corpus/parser/<topic>.txt \
//!   | go run ./rust/difftests/godump restore > rust/difftests/corpus/parser/<topic>.golden.txt
//! ```
//!
//! Add a brand-new topic by creating a new `<topic>.txt` + regenerating its
//! `<topic>.golden.txt` the same way — never append to an existing topic's
//! file unless the addition genuinely belongs to that topic.

use std::path::PathBuf;

use difftest::{difftest_root, load_corpus_dir, validate_executable_corpora};

fn corpus_dir() -> PathBuf {
    difftest_root().join("corpus").join("parser")
}

/// Parses the restore-golden format into one restored SQL string per statement
/// index (`!ERR` for statements the Go parser rejected). A restored string may
/// itself span multiple lines (e.g. a string literal with an embedded newline),
/// so all lines within one `#IDX`/`#END` block are joined. Splits on a plain
/// `'\n'`, NOT `str::lines()`: `lines()` treats a `"\r\n"` pair as a single
/// line terminator and strips BOTH characters, which silently eats a
/// legitimate trailing `\r` that's part of a restored string's own CONTENT
/// (not the file's line ending) whenever it happens to sit immediately
/// before the file's `\n` separator — confirmed via a real corpus statement,
/// `select ' \r\n  .col';`, which this bug made look like a genuine restore
/// mismatch (byte-identical when compared directly, without a golden-file
/// round trip) until traced here. This project's own golden files are always
/// LF-only at the STRUCTURAL level (`godump` prints via Go's `Fprintln` on
/// Unix), so a bare `'\n'` split is exactly correct here.
fn parse_restore_golden(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur: Option<Vec<String>> = None;
    for line in text.split('\n') {
        if line.starts_with("#IDX ") {
            cur = Some(Vec::new());
        } else if line == "#END" {
            if let Some(block) = cur.take() {
                out.push(block.join("\n"));
            }
        } else if let Some(block) = cur.as_mut() {
            block.push(line.to_string());
        }
    }
    out
}

/// Regression: `parse_restore_golden` used to split on `str::lines()`,
/// which treats a `"\r\n"` pair as ONE line terminator and strips both
/// characters — silently eating a legitimate trailing `\r` that's part of
/// a restored string's own CONTENT (not the file's line ending) whenever
/// it happens to sit immediately before the file's `\n` separator. Found
/// via a real corpus statement (`select ' \r\n  .col';`) that looked like
/// a genuine restore mismatch in diagnostic output — both sides
/// LOOKED identical when printed (a raw `\r` in a terminal just moves the
/// cursor, so a missing one is invisible) — until a direct, file-free
/// comparison proved the actual restore output was byte-identical, tracing
/// the false mismatch to this function.
#[test]
fn parse_restore_golden_preserves_embedded_cr_before_newline() {
    let golden = "#IDX 0\nSELECT _UTF8MB4' \r\n  .col'\n#END\n";
    assert_eq!(
        parse_restore_golden(golden),
        vec!["SELECT _UTF8MB4' \r\n  .col'".to_string()]
    );
}

#[test]
fn test_differential() {
    let root = difftest::parser_oracle::repo_root();
    validate_executable_corpora(&root).expect("executable corpus contract");
    let (statements, golden_text) = load_corpus_dir(&corpus_dir());
    let golden = parse_restore_golden(&golden_text);

    assert_eq!(
        statements.len(),
        golden.len(),
        "corpus/golden count mismatch in corpus/parser/ (regenerate the changed topic's golden)"
    );

    let mut failures = Vec::new();
    for (idx, sql) in statements.iter().enumerate() {
        let want = &golden[idx];
        // The curated corpus should contain only statements the Go parser
        // accepts; a golden `!ERR` means the corpus needs fixing.
        assert_ne!(
            want, "!ERR",
            "corpus statement #{idx} is rejected by the Go parser: {sql}"
        );

        match tidb_parser::parse(sql) {
            Ok(stmt) => {
                let got = stmt.restore();
                if &got != want {
                    failures.push(format!(
                        "\n--- #{idx}: {sql}\n  go  : {want}\n  rust: {got}"
                    ));
                }
            }
            Err(e) => failures.push(format!(
                "\n--- #{idx}: {sql}\n  go  : {want}\n  rust: <parse error at {}: {}>",
                e.offset, e.message
            )),
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} statements diverged from the Go parser:{}",
        failures.len(),
        statements.len(),
        failures.join("")
    );
}
