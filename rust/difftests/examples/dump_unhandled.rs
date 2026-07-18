//! Prints every statement from `PARSER_COV_STMTS` whose golden entry isn't
//! `!ERR` but this project's parser fails on, one per line — a companion to
//! the `coverage_report` test (`difftest/tests/parser_diff.rs`), which only
//! reports the UNHANDLED bucket's total count, not its contents. Pipe the
//! output through `grep`/`sort`/etc. to sample and categorize the bucket by
//! hand, the same way the corpus's own mismatches are read.
//!
//! ```sh
//! PARSER_COV_STMTS=... PARSER_COV_GOLDEN=... \
//!   cargo run --example dump_unhandled -p difftest
//! ```

use std::fs;

use difftest::parse_corpus;

/// Splits on a plain `'\n'`, NOT `str::lines()` — see the identical function
/// in `difftest/tests/parser_diff.rs` for why (`lines()` silently eats a
/// content `\r` that sits right before the file's own `\n`).
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

fn main() {
    let stmts_path = std::env::var("PARSER_COV_STMTS").expect("PARSER_COV_STMTS");
    let golden_path = std::env::var("PARSER_COV_GOLDEN").expect("PARSER_COV_GOLDEN");
    let statements = parse_corpus(&fs::read_to_string(stmts_path).unwrap());
    let golden = parse_restore_golden(&fs::read_to_string(golden_path).unwrap());
    for (idx, sql) in statements.iter().enumerate() {
        if golden[idx] == "!ERR" {
            continue;
        }
        if tidb_parser::parse(sql).is_err() {
            println!("{sql}");
        }
    }
}
