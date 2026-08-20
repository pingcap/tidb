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

//! `executor/jointest/join`'s 21-table comma join, at every prefix length.
//!
//! The statement is TiDB's own `TestMultiJoin` (`t/executor/jointest/join.test`,
//! recorded answer in `r/executor/jointest/join.result`): 21 two-row tables
//! listed with commas, every join equality written in `WHERE`. It is the
//! largest join in the whole integration suite and the reason
//! `driver::predicate_push_down` exists -- without pushdown each of the 20
//! join nodes is a CARTESIAN nested loop, so the cost doubles with every table
//! and the statement does not finish.
//!
//! # The control, and why it is exact
//!
//! The control is the same `FROM` with NO `WHERE` at all -- a plain cross
//! product, which no predicate rule has anything to reshape -- with the
//! equalities then applied in Rust to the rows that come back. It shares no
//! planner code with the statement under test, so it cannot fail the same way.
//!
//! The control used to be the same statement with every equality
//! parenthesized (`(a40)=b42`), on the reasoning that parentheses change no
//! meaning while `predicate_push_down::column_equality` matched only a bare
//! `Expr::Column`, so the parenthesized spelling got no pushdown. That control
//! was itself broken, and this test is what caught it:
//! `join_reorder::classify` DID see through the parentheses and classified the
//! conjunct as a join edge, which removed it from the residual `WHERE` as
//! join-executed, while `column_equality` refused to recognize it and the join
//! never installed it. The predicate ran NOWHERE, so the "control" returned
//! the unfiltered cross product -- 16 rows where 8 are correct. Both matchers
//! strip parentheses now, which is exactly why the control can no longer be a
//! parser artifact.
//!
//! Row equality is checked up to [`CONTROL_TABLES`] tables only, because the
//! control is the exponential plan: it is the thing being measured, and
//! running it at 21 tables costs the 7.4 seconds this test exists to prevent.
//!
//! # Why the deep stack
//!
//! Both tests run on `difftest::on_deep_stack` for the reason
//! `integration_diff::run_topic` states: a 21-table `FROM` is recursion over
//! INPUT SIZE, not over nesting the user wrote, and Go runs every statement on
//! a goroutine whose stack grows on demand. On a libtest thread's fixed 8MB a
//! debug build overflows at exactly 21 tables and the process aborts -- which
//! is the crash the survey originally reported for this topic, and it is
//! independent of how long the statement takes.
//!
//! # The budget assertion is load-bearing, measured
//!
//! Neutering the rule -- `if false && join.tp == ...` in `build_join`, run
//! and reverted -- takes the 21-table statement from 0.17s for this whole
//! file to 45.9s for the one test, and
//! [`twenty_one_tables_finish_within_budget_with_tidb_s_answer`] FAILS on the
//! budget. Its control, [`pushdown_returns_the_control_plan_s_rows`], still
//! passes under the same mutation, which is the point: the rows never
//! depended on the rule.
//!
//! # What this does NOT fix, named
//!
//! `executor/jointest/join` still does not replay to the end, and the cause
//! has MOVED rather than remained. It is now
//! `desc analyze select * from t t1, t t2, t t3, t t4, t t5, t t6` near the
//! end of the script -- a six-way self cross join with no `WHERE` at all,
//! over a table the script has just doubled eight times. That statement is
//! SUPPOSED to blow up: the script sets `tidb_mem_oom_action = 'CANCEL'` and
//! expects `--error 8175`, so Go cancels it on the memory quota. This tier
//! does not enforce a query memory quota on that path, so it runs the
//! cartesian until the OS kills the process. That is a memory-accounting
//! gap, not a join-planning one, and nothing in this file addresses it.

use std::time::{Duration, Instant};

use tidb_datatype::Datum;
use tidb_session::{Session, SharedCatalog};

/// The tables in the order `TestMultiJoin` lists them, which is the order the
/// left-deep join tree is built in.
const TABLES: &[&str] = &[
    "t35", "t40", "t14", "t42", "t15", "t7", "t64", "t19", "t9", "t8", "t57", "t37", "t44", "t38",
    "t18", "t62", "t4", "t48", "t31", "t16", "t12",
];

/// The statement's join equalities, as written.
const EQUALITIES: &[(&str, &str)] = &[
    ("b48", "a57"),
    ("a4", "b19"),
    ("a14", "b16"),
    ("b37", "a48"),
    ("a40", "b42"),
    ("a15", "b40"),
    ("a38", "b8"),
    ("b15", "a31"),
    ("b64", "a18"),
    ("b12", "a44"),
    ("b7", "a8"),
    ("b35", "a16"),
    ("a12", "b14"),
    ("a64", "b57"),
    ("b62", "a7"),
    ("a35", "b38"),
    ("b9", "a19"),
    ("a62", "b18"),
    ("b4", "a37"),
    ("b44", "a42"),
];

/// How many tables the un-pushed control is run over. The control's cost
/// doubles per table; 13 keeps the whole comparison well under a second even in
/// a debug build, while still crossing the point where the two plans have
/// visibly diverged (the control is already a thousand-fold more work there).
const CONTROL_TABLES: usize = 13;

/// The budget the full 21-table statement must finish in. Two orders of
/// far below the 7.4s the un-pushed plan took in RELEASE (and the 400s+ it
/// took in the debug survey, which is the build this test runs in by
/// default), and far above the ~5ms it takes with pushdown -- wide enough
/// that a loaded machine does not fail it, narrow enough that losing
/// pushdown cannot pass it by any margin.
const BUDGET: Duration = Duration::from_secs(5);

fn open() -> Session {
    let mut session = Session::with_catalog(SharedCatalog::default());
    session
        .run("create database if not exists many_table_join")
        .expect("create database");
    session
        .select_database("many_table_join")
        .expect("use database");
    for table in TABLES {
        let suffix = &table[1..];
        session
            .run(&format!(
                "create table {table}(a{suffix} int primary key, b{suffix} int, x{suffix} int)"
            ))
            .expect("create table");
        // The recorded fixture's two rows, unchanged.
        session
            .run(&format!("insert into {table} values(1,1,1)"))
            .expect("insert");
        session
            .run(&format!("insert into {table} values(7,7,7)"))
            .expect("insert");
    }
    session
}

/// The predicates over the first `count` tables, as `(left, right)` column
/// pairs plus the one literal test the fixture writes.
fn predicates(count: usize) -> (Vec<(String, String)>, bool) {
    let used = &TABLES[..count];
    let owns = |column: &str| used.contains(&&*format!("t{}", &column[1..]));
    let pairs = EQUALITIES
        .iter()
        .filter(|(left, right)| owns(left) && owns(right))
        .map(|(left, right)| ((*left).to_owned(), (*right).to_owned()))
        .collect();
    (pairs, used.contains(&"t31"))
}

/// The statement over the first `count` tables.
fn statement(count: usize) -> Option<String> {
    let used = &TABLES[..count];
    let columns: Vec<String> = used.iter().map(|t| format!("x{}", &t[1..])).collect();
    let (pairs, literal) = predicates(count);
    let mut written = Vec::new();
    if literal {
        written.push("a31=7".to_owned());
    }
    written.extend(pairs.iter().map(|(left, right)| format!("{left}={right}")));
    if written.is_empty() {
        return None;
    }
    Some(format!(
        "SELECT {} FROM {} WHERE {}",
        columns.join(","),
        used.join(","),
        written.join(" AND ")
    ))
}

/// The control rows: the same `FROM` with no `WHERE` at all, filtered here.
///
/// The cross product is selected with every predicate column alongside the
/// projected ones, so the filtering this function does needs nothing from the
/// engine beyond the rows themselves.
fn control_rows(session: &mut Session, count: usize) -> Vec<Vec<Datum>> {
    let used = &TABLES[..count];
    let mut columns: Vec<String> = used.iter().map(|t| format!("x{}", &t[1..])).collect();
    let projected = columns.len();
    let offset_of = |columns: &mut Vec<String>, name: &str| -> usize {
        if let Some(at) = columns.iter().position(|column| column == name) {
            return at;
        }
        columns.push(name.to_owned());
        columns.len() - 1
    };
    let (pairs, literal) = predicates(count);
    let tests: Vec<(usize, usize)> = pairs
        .iter()
        .map(|(left, right)| {
            (
                offset_of(&mut columns, left),
                offset_of(&mut columns, right),
            )
        })
        .collect();
    let literal_offset = literal.then(|| offset_of(&mut columns, "a31"));
    let sql = format!("SELECT {} FROM {}", columns.join(","), used.join(","));
    let mut kept: Vec<Vec<Datum>> = rows(session, &sql)
        .into_iter()
        .filter(|row| {
            tests.iter().all(|(left, right)| row[*left] == row[*right])
                && literal_offset.is_none_or(|at| row[at] == Datum::Int(7))
        })
        .map(|mut row| {
            row.truncate(projected);
            row
        })
        .collect();
    kept.sort_by_key(|row| format!("{row:?}"));
    kept
}

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<Datum>> {
    let tidb_session::StmtResult::Rows(mut rows) =
        session.run(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    else {
        panic!("{sql}: a SELECT answers with rows");
    };
    // The statement has no ORDER BY, so only the SET of rows is defined.
    rows.sort_by_key(|row| format!("{row:?}"));
    rows
}

/// Pushing a `WHERE` equality into the join must not move a single row.
#[test]
fn pushdown_returns_the_control_plan_s_rows() {
    difftest::on_deep_stack(pushdown_rows_on_this_stack);
}

fn pushdown_rows_on_this_stack() {
    let mut session = open();
    for count in 2..=CONTROL_TABLES {
        // The first few tables in the statement's own order share no
        // equality, so there is nothing to push and nothing to compare.
        let Some(sql) = statement(count) else {
            continue;
        };
        let pushed = rows(&mut session, &sql);
        let control = control_rows(&mut session, count);
        assert_eq!(
            pushed, control,
            "the row set moved at {count} tables:\n  statement: {sql}"
        );
    }
}

/// The full statement finishes, and answers what TiDB recorded for it.
///
/// `r/executor/jointest/join.result` records exactly one row, every cell `7`:
/// the `a31=7` conjunct pins one table to its second row and the equality
/// graph propagates that through all 21.
/// The per-table cost curve, which is the measurement the pushdown rule was
/// built from. Ignored: it is evidence, not a gate.
///
/// ```sh
/// cargo test --release -p difftest-result-tests --test many_table_join -- \
///     --ignored --nocapture scale_probe
/// ```
///
/// Before pushdown (release) the times double with every table added:
/// 136ms at 16, 1.44s at 19, 3.26s at 20, 7.42s at 21 -- a clean 2^k, which
/// is the cross product of 21 two-row tables and nothing to do with the order
/// the joins are taken in. After pushdown the curve is flat: ~4.9ms at 21.
#[test]
#[ignore]
fn scale_probe() {
    difftest::on_deep_stack(scale_probe_on_this_stack);
}

fn scale_probe_on_this_stack() {
    let mut session = open();
    for count in 2..=TABLES.len() {
        let Some(sql) = statement(count) else {
            continue;
        };
        let start = Instant::now();
        let _ = rows(&mut session, &sql);
        println!("k={count:2} {:?}", start.elapsed());
    }
}

#[test]
fn twenty_one_tables_finish_within_budget_with_tidb_s_answer() {
    difftest::on_deep_stack(twenty_one_tables_on_this_stack);
}

fn twenty_one_tables_on_this_stack() {
    let mut session = open();
    let sql = statement(TABLES.len()).expect("the full statement has predicates");
    let start = Instant::now();
    let answer = rows(&mut session, &sql);
    let elapsed = start.elapsed();
    assert_eq!(
        answer,
        vec![vec![Datum::Int(7); TABLES.len()]],
        "the 21-table join's recorded answer is one all-7 row"
    );
    assert!(
        elapsed < BUDGET,
        "the 21-table join took {elapsed:?}, over the {BUDGET:?} budget: the \
         `WHERE` equalities are no longer reaching the joins, so every join \
         node is back to a cartesian nested loop"
    );
}

/// A `WHERE` equality must be executed SOMEWHERE, whatever it is spelled like.
///
/// `join_reorder::classify` strips parentheses and
/// `predicate_push_down::column_equality` did not, so `(a40)=b14` was removed
/// from the residual `WHERE` as join-executed and then never installed on the
/// join: it ran nowhere and the statement returned its whole cross product.
/// The spellings here are all the same predicate, so they must all return the
/// same rows -- and, with two rows per table and `a` a primary key, that is 4
/// of the 8 the cross product holds.
#[test]
fn an_equality_is_executed_whatever_it_is_spelled_like() {
    let mut session = open();
    let expected = rows(
        &mut session,
        "SELECT x35,x40,x14 FROM t35,t40,t14 WHERE a40=b14",
    );
    assert_eq!(expected.len(), 4, "the fixture's own answer moved");
    for spelling in [
        "(a40)=b14",
        "a40=(b14)",
        "((a40))=((b14))",
        "b14=a40",
        "(b14)=a40",
        // The whole conjunct parenthesized, which `classify` and
        // `local_constant_equality` match on their outermost node.
        "(a40=b14)",
        "((a40=b14))",
        "((a40)=(b14))",
    ] {
        let sql = format!("SELECT x35,x40,x14 FROM t35,t40,t14 WHERE {spelling}");
        assert_eq!(rows(&mut session, &sql), expected, "{sql}");
    }
}
