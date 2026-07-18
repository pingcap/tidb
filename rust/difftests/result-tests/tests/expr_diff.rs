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

//! The design's "result ring" at Phase-0 scale: `tidb-expr` must evaluate each
//! constant expression to the same value the production Go engine produces.
//! The corpus is a directory of per-topic file pairs under `corpus/expr/`
//! (`<topic>.txt` + `<topic>.golden.txt`); see `difftest::load_corpus_dir`.
//!
//! Every golden result `tidb-expr` covers (`INT:<v>` / `STR:<valid UTF-8>` /
//! `STR_HEX:<invalid UTF-8 bytes>` / `DEC:<v>` / `NULL`) is asserted;
//! out-of-domain results (`SKIP:<k>` — floats, decimal division, ... — and
//! `ERR`) are counted but not required, since `tidb-expr` does not cover them.
//!
//! Regenerate one topic's golden after changing it:
//! ```sh
//! grep -v '^##' rust/difftests/corpus/expr/<topic>.txt \
//!   | go run ./rust/difftests/goeval > rust/difftests/corpus/expr/<topic>.golden.txt
//! ```
//!
//! Add a brand-new topic by creating a new `<topic>.txt` + regenerating its
//! `<topic>.golden.txt` the same way — never append to an existing topic's
//! file unless the addition genuinely belongs to that topic.

use std::path::PathBuf;

use difftest::{difftest_root, load_corpus_dir, validate_executable_corpora};
use tidb_ast::{QueryStmt, SelectField, Stmt};

fn corpus_dir() -> PathBuf {
    difftest_root().join("corpus").join("expr")
}

/// Parses `expr` by wrapping it in `SELECT`, then returns its evaluated label.
fn rust_eval_label(expr: &str) -> Result<String, String> {
    let stmt = tidb_parser::parse(&format!("select {expr}")).map_err(|e| e.message)?;
    let Stmt::Query(query) = stmt else {
        return Err("not a query".to_string());
    };
    let QueryStmt::Select(sel) = *query else {
        return Err("not a select".to_string());
    };
    match sel.fields.first() {
        Some(SelectField::Expr { expr, .. }) => tidb_expr::eval(expr)
            .map(|v| v.label())
            .map_err(|e| format!("{e:?}")),
        _ => Err("no field expression".to_string()),
    }
}

#[test]
fn expr_eval_matches_go_engine() {
    let root = difftest::parser_oracle::repo_root();
    validate_executable_corpora(&root).expect("executable corpus contract");
    let (exprs, golden_text) = load_corpus_dir(&corpus_dir());
    let golden: Vec<String> = golden_text.lines().map(str::to_string).collect();

    assert_eq!(
        exprs.len(),
        golden.len(),
        "expr corpus/golden count mismatch in corpus/expr/ (regenerate the changed topic's golden)"
    );

    let mut failures = Vec::new();
    let mut matched = 0;
    let mut skipped = 0;
    for (expr, want) in exprs.iter().zip(&golden) {
        // Out-of-domain golden results are not required of tidb-expr yet.
        if want.starts_with("SKIP:") || want == "ERR" {
            skipped += 1;
            continue;
        }
        match rust_eval_label(expr) {
            Ok(got) if &got == want => matched += 1,
            Ok(got) => failures.push(format!("\n--- {expr}\n  go  : {want}\n  rust: {got}")),
            Err(e) => failures.push(format!(
                "\n--- {expr}\n  go  : {want}\n  rust: <error: {e}>"
            )),
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} in-domain expressions diverged from the Go engine ({} skipped):{}",
        failures.len(),
        matched + failures.len(),
        skipped,
        failures.join("")
    );
}
