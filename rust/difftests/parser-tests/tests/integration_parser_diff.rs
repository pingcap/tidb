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
    // Direct Go source ports cover column CHECK, field-type aliases/modifiers
    // and binary-string normalization, CREATE TABLE AFFINITY, creation-side
    // SPLIT, inline bare-KEY primary spelling, SERIAL/AUTO_RANDOM, CREATE
    // USER REQUIRE/WITH payloads, and ALTER TABLE ATTRIBUTES.
    // `parseStringOptions` moves 20 rows to exact restore: 11 prior parser
    // failures and 9 prior restore mismatches, with no rejection-direction
    // change. Its source-addressed selector and field-type tests retain the
    // individual Go forms.
    // CREATE USER RESOURCE GROUP adds one exact row, partition ATTRIBUTES
    // adds six, SHOW TABLE NEXT_ROW_ID adds thirteen, ALTER INDEX visibility
    // adds twenty-two, terminal ALTER TABLE PARTITION BY adds forty-nine,
    // ALTER CHECK enforcement adds sixteen, SHOW TABLE STATUS adds three,
    // ALTER COLUMN SET/DROP DEFAULT adds twenty-five, SHOW STATUS adds three,
    // RENAME {KEY|INDEX} adds twenty-one, ADMIN SHOW DDL JOBS adds three,
    // DROP {CHECK|CONSTRAINT} adds twenty-one, DDL JOB QUERIES adds one,
    // DROP FOREIGN KEY adds sixteen, SHOW VARIABLES WHERE adds nine, SHOW
    // STATS_TOPN adds four, ADMIN DDL job control adds three, ALTER TABLE
    // AUTO_INCREMENT adds four, table-level COMMENT adds nine, and
    // SHARD_ROW_ID_BITS adds five selected records. The next wave adds ten
    // table-level placement-policy records, three STATS_LOCKED records, and
    // twenty-five accepted no-ON role-membership records (the direct R1
    // selectors cover five GRANT and three REVOKE rows). The current wave
    // adds six direct partition-placement rows, four ADMIN FLUSH PLAN_CACHE
    // rows, and two SHOW STATS_BUCKETS rows. The current wave adds fifty-three
    // table-level CACHE/NOCACHE rows, two ANALYZE INCREMENTAL rows, and two
    // SHOW OPEN TABLES rows. The current wave adds DROP PRIMARY KEY, ALTER
    // TTL/REMOVE TTL, AUTO_ID_CACHE/AUTO_RANDOM_BASE, dynamic REVOKE
    // privileges, and explicit CREATE DEFINER view forms. Additional
    // source-shaped composite option records close through the same shared
    // SetTableOptions envelope. These direct Go-source ports move exactly 59
    // more rows to restore equality; every other outcome category was reviewed
    // unchanged. The grouped ADD COLUMN literal-default source slice adds
    // thirteen exact rows, followed by sixteen RENAME COLUMN rows. The
    // latest source wave adds ENUM/SET binary members, bare CHECK time
    // functions, SET transaction snapshot syntax, joined UPDATE bare
    // DEFAULT assignments, SHOW CHARACTER SET/CHARSET, and dynamic
    // RESOURCE_GROUP privilege names; together these move twenty-nine
    // additional rows to exact restore without changing the
    // rejection-direction categories. SHOW ENGINES then closes one additional
    // identifier-dispatched SHOW row without changing any rejection category.
    // The current parallel rings close seven qualified CREATE TABLE column
    // rows, seven CREATE TABLE compatibility options, the adjacent ALTER
    // TABLE and EXPLAIN/LEADING slices, two partition actions, and three SET
    // restore mismatches without changing false accepts. The follow-up ring
    // closes the bare ADD PARTITION action and the CREATE TABLE MERGE UNION
    // option, then the DISCARD and interval-bound partition actions close
    // their full accepted integration families. The MERGE FIRST, SPLIT
    // MAXVALUE, and parenthesized set-operation rings then close four more
    // accepted rows, again without changing any rejection-direction category.
    // The validation, ENGINE_ATTRIBUTE, SHOW MASTER/PRIVILEGES, ADMIN CLEANUP,
    // ordinary SHOW, and DROP DATABASE rings close 26 additional accepted
    // rows; grouped ALTER ADD COLUMN, EXPLAIN VALUES, and REVOKE edge rows
    // close seven more without changing mismatch categories.
    // Decimal ValueExpr normalization closes one restore mismatch. The
    // NATIONAL/NCHAR family and quoted column COLLATE close two accepted
    // parser failures, EXPLAIN hint-name decoding closes one restore mismatch,
    // and table charset validation moves sixteen false accepts to rejection.
    // The follow-up expression ring validates CHAR/CONVERT USING charsets;
    // the charset-introducer ring rejects unsupported legacy introducers.
    // Strict DOUBLE arity and CREATE TABLE builtin-name token boundaries then
    // remove the remaining DDL false accepts without widening general name
    // parsing. The LIMIT, datetime-precision, and collation-validation rings
    // now close the final false accepts. Multi-statement replay is source-owned
    // through parse_multi; the reviewed aggregate is 51,488 exact single
    // restores, 10 exact multi-statement restores, 99 total Rust parse
    // failures, zero restore mismatches, and zero false accepts. The no-ID
    // executable T! comment and parenthesized WITH rings account for the
    // final two single-statement restores.
    rust_matched: 51_488,
    rust_multi_statement_matched: 10,
    rust_parse_failure: 99,
    rust_restore_mismatch: 0,
    rust_accepted_go_rejected: 0,
    rust_accepted_go_restore_failure: 1,
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
                    .map(|statement| statement.restore_bytes())
                    .collect::<Vec<_>>()
            })
        } else {
            tidb_parser::parse(&record.input.sql).map(|statement| vec![statement.restore_bytes()])
        };
        match replay {
            Err(_) => counts.rust_parse_failure += 1,
            Ok(restores) => match record.outcome {
                GoOutcome::Accepted if record.statement_count == 1 => {
                    if restores.len() == 1 && restores[0] == record.restores[0] {
                        counts.rust_matched += 1;
                    } else {
                        counts.rust_restore_mismatch += 1;
                    }
                }
                GoOutcome::Accepted => {
                    if restores == record.restores {
                        counts.rust_multi_statement_matched += 1;
                    } else {
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
