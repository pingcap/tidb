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

//! Replay the checked Go parser oracle over every mysqltest parser input.
//!
//! This is intentionally an evidence test, not a false all-green parity
//! assertion: the seed Rust parser implements only a subset of TiDB grammar.
//! It fails if the static oracle no longer names the exact checked input
//! inventory, while reporting the complete Go/Rust outcome distribution for
//! the current parser. It never starts Go; `integration_parser_golden --write`
//! is the explicit regeneration operation.

use std::process::{Command, Output};

use difftest::parser_oracle::{shared_golden, GoOutcome};

#[derive(Debug, Default, Eq, PartialEq)]
struct Counts {
    go_accepted: usize,
    go_rejected: usize,
    go_restore_failure: usize,
    rust_matched: usize,
    rust_multi_statement_matched: usize,
    rust_parse_failure: usize,
    rust_restore_mismatch: usize,
    rust_accepted_go_rejected: usize,
    rust_accepted_go_restore_failure: usize,
}

// This is a deliberately reviewed coverage snapshot, replayed against the
// current parser dependency; it is not a claim of parser
// parity. A Rust grammar change must update it in the same change after
// inspecting the emitted categories; otherwise a dropped match or a changed
// rejection direction would pass unnoticed behind a still-valid Go oracle.
const EXPECTED_COUNTS: Counts = Counts {
    go_accepted: 51_498,
    go_rejected: 99,
    go_restore_failure: 1,
    // Every accepted statement and the one Go restore failure now has the
    // same Rust outcome. The remaining parse failures are exactly Go's
    // rejected inputs; there are no restore mismatches or false accepts.
    rust_matched: 51_489,
    rust_multi_statement_matched: 10,
    rust_parse_failure: 99,
    rust_restore_mismatch: 0,
    rust_accepted_go_rejected: 0,
    rust_accepted_go_restore_failure: 0,
};

fn run_difftest_check(binary: &str) -> Output {
    Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .current_dir(
            difftest::difftest_root()
                .parent()
                .expect("difftest lives below the Rust workspace"),
        )
        .args([
            "run", "-q", "-p", "difftest", "--bin", binary, "--", "--check",
        ])
        .output()
        .unwrap_or_else(|error| panic!("run difftest binary {binary}: {error}"))
}

#[test]
fn integration_parser_static_go_oracle_reports_rust_outcomes() {
    let source_inventory = run_difftest_check("integration_parser_inventory");
    assert!(
        source_inventory.status.success(),
        "source parser inventory is stale:\n{}",
        String::from_utf8_lossy(&source_inventory.stderr)
    );
    let output = run_difftest_check("integration_parser_golden");
    assert!(
        output.status.success(),
        "integration parser golden is stale or malformed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let records = shared_golden().expect("parse checked integration parser oracle");
    let mut counts = Counts::default();
    for record in records.iter() {
        match record.outcome {
            GoOutcome::Accepted => counts.go_accepted += 1,
            GoOutcome::Rejected => counts.go_rejected += 1,
            GoOutcome::RestoreFailure => counts.go_restore_failure += 1,
        }

        let replay = if record.outcome == GoOutcome::Accepted && record.statement_count != 1 {
            tidb_parser::parse_multi(&record.input.sql).map(|statements| {
                statements
                    .into_iter()
                    .map(|statement| statement.try_restore_bytes())
                    .collect::<Result<Vec<_>, _>>()
            })
        } else {
            tidb_parser::parse(&record.input.sql)
                .map(|statement| statement.try_restore_bytes().map(|restore| vec![restore]))
        };
        match replay {
            Err(_) => counts.rust_parse_failure += 1,
            Ok(Err(_)) if record.outcome == GoOutcome::RestoreFailure => {
                counts.rust_matched += 1;
            }
            Ok(Err(error)) => {
                eprintln!(
                    "Rust restore failed for Go-accepted input {}:{}: {error}\n{}",
                    record.input.path, record.input.start_line, record.input.sql
                );
                counts.rust_restore_mismatch += 1;
            }
            Ok(Ok(restores)) => match record.outcome {
                GoOutcome::Accepted if record.statement_count == 1 => {
                    if restores.len() == 1 && restores[0] == record.restores[0] {
                        counts.rust_matched += 1;
                    } else {
                        eprintln!(
                            "Rust restore mismatch at {}:{}\nSQL: {}\nGo: {:?}\nRust: {:?}",
                            record.input.path,
                            record.input.start_line,
                            record.input.sql,
                            record.restores,
                            restores
                        );
                        counts.rust_restore_mismatch += 1;
                    }
                }
                GoOutcome::Accepted => {
                    if restores == record.restores {
                        counts.rust_multi_statement_matched += 1;
                    } else {
                        eprintln!(
                            "Rust multi-restore mismatch at {}:{}\nSQL: {}\nGo: {:?}\nRust: {:?}",
                            record.input.path,
                            record.input.start_line,
                            record.input.sql,
                            record.restores,
                            restores
                        );
                        counts.rust_restore_mismatch += 1;
                    }
                }
                GoOutcome::Rejected => counts.rust_accepted_go_rejected += 1,
                GoOutcome::RestoreFailure => counts.rust_accepted_go_restore_failure += 1,
            },
        }
    }

    let total = records.len();
    assert_eq!(
        counts.go_accepted + counts.go_rejected + counts.go_restore_failure,
        total,
        "every static Go parser result must have an outcome"
    );
    assert_eq!(
        counts.rust_matched
            + counts.rust_multi_statement_matched
            + counts.rust_parse_failure
            + counts.rust_restore_mismatch
            + counts.rust_accepted_go_rejected
            + counts.rust_accepted_go_restore_failure,
        total,
        "every Rust replay must have exactly one outcome"
    );
    println!(
        "integration parser diff: total={total} go_accepted={} go_rejected={} go_restore_failure={} rust_matched={} rust_multi_statement_matched={} rust_parse_failure={} rust_restore_mismatch={} rust_accepted_go_rejected={} rust_accepted_go_restore_failure={}",
        counts.go_accepted,
        counts.go_rejected,
        counts.go_restore_failure,
        counts.rust_matched,
        counts.rust_multi_statement_matched,
        counts.rust_parse_failure,
        counts.rust_restore_mismatch,
        counts.rust_accepted_go_rejected,
        counts.rust_accepted_go_restore_failure,
    );
    assert_eq!(
        counts, EXPECTED_COUNTS,
        "Rust integration-parser outcome snapshot changed. Inspect the printed \n         Go/Rust categories and deliberately update EXPECTED_COUNTS with the parser change."
    );
}
