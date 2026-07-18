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

//! Sequence execution tests (`CREATE`/`DROP SEQUENCE`,
//! `NEXTVAL`/`LASTVAL`/`SETVAL`) — every expected value here is copied
//! from a `gorun` probe recorded in task #128's investigation (see
//! `crate::sequence`'s module doc), none hand-derived.

use super::*;

/// The basic counter: `NEXTVAL` steps, `LASTVAL` echoes this session's
/// last `NEXTVAL`, `SETVAL` rebases (returning `NULL` once already
/// satisfied) — the exact statement script `gorun` was probed with.
#[test]
fn sequence_basic_counter() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "create sequence seq1"), "OK");
    assert_eq!(step(&mut db, "select nextval(seq1)"), "RS:1");
    assert_eq!(step(&mut db, "select nextval(seq1)"), "RS:2");
    assert_eq!(step(&mut db, "select nextval(seq1)"), "RS:3");
    assert_eq!(step(&mut db, "select lastval(seq1)"), "RS:3");
    assert_eq!(step(&mut db, "select setval(seq1, 100)"), "RS:100");
    assert_eq!(step(&mut db, "select nextval(seq1)"), "RS:101");
    // 50 is already behind the counter — NULL, and nothing moves.
    assert_eq!(step(&mut db, "select setval(seq1, 50)"), "RS:<nil>");
    // LASTVAL still echoes the last NEXTVAL, untouched by either SETVAL.
    assert_eq!(step(&mut db, "select lastval(seq1)"), "RS:101");
}

/// `LASTVAL` is `NULL` until this session's first `NEXTVAL` — and a
/// `SETVAL` alone does NOT seed it (both confirmed via `gorun`).
#[test]
fn sequence_lastval_session_scope() {
    let mut db = Database::new();
    step(&mut db, "create sequence seq2");
    assert_eq!(step(&mut db, "select lastval(seq2)"), "RS:<nil>");
    assert_eq!(step(&mut db, "select setval(seq2, 5)"), "RS:5");
    assert_eq!(step(&mut db, "select lastval(seq2)"), "RS:<nil>");
}

/// `INCREMENT BY`/`START WITH` drive the lattice; a decreasing sequence
/// with `NOCYCLE` (the default) errors on every `NEXTVAL` past its
/// minimum, not just the first (confirmed via `gorun`).
#[test]
fn sequence_increment_start_and_exhaustion() {
    let mut db = Database::new();
    step(&mut db, "create sequence seq3 increment by 5 start with 10");
    assert_eq!(step(&mut db, "select nextval(seq3)"), "RS:10");
    assert_eq!(step(&mut db, "select nextval(seq3)"), "RS:15");
    assert_eq!(step(&mut db, "select nextval(seq3)"), "RS:20");
    step(
        &mut db,
        "create sequence seq4 increment by -1 start with 5 minvalue 1 maxvalue 5",
    );
    for expect in ["RS:5", "RS:4", "RS:3", "RS:2", "RS:1"] {
        assert_eq!(step(&mut db, "select nextval(seq4)"), expect);
    }
    assert_eq!(
        step(&mut db, "select nextval(seq4)"),
        "Eval(Sequence(\"sequence has run out\"))"
    );
    assert_eq!(
        step(&mut db, "select nextval(seq4)"),
        "Eval(Sequence(\"sequence has run out\"))"
    );
}

/// `CYCLE` restarts at MINVALUE for a positive increment — NOT at
/// `START` (confirmed via `gorun` with `START` ≠ `MINVALUE`, and with a
/// max that isn't itself on the lattice).
#[test]
fn sequence_cycle_positive() {
    let mut db = Database::new();
    step(
        &mut db,
        "create sequence seq5 increment by 3 start with 2 minvalue 2 maxvalue 10 cycle",
    );
    for expect in ["RS:2", "RS:5", "RS:8", "RS:2", "RS:5", "RS:8", "RS:2"] {
        assert_eq!(step(&mut db, "select nextval(seq5)"), expect);
    }
    step(
        &mut db,
        "create sequence seq6 increment by 3 start with 2 minvalue 2 maxvalue 9 cycle",
    );
    for expect in ["RS:2", "RS:5", "RS:8", "RS:2", "RS:5"] {
        assert_eq!(step(&mut db, "select nextval(seq6)"), expect);
    }
    step(&mut db, "create sequence seq7 minvalue 3 maxvalue 8 cycle");
    for expect in ["RS:3", "RS:4", "RS:5", "RS:6", "RS:7", "RS:8", "RS:3"] {
        assert_eq!(step(&mut db, "select nextval(seq7)"), expect);
    }
}

/// `CYCLE` restarts at MAXVALUE for a NEGATIVE increment (confirmed via
/// `gorun`).
#[test]
fn sequence_cycle_negative() {
    let mut db = Database::new();
    step(
        &mut db,
        "create sequence seq8 increment by -3 start with 8 minvalue 2 maxvalue 8 cycle",
    );
    for expect in ["RS:8", "RS:5", "RS:2", "RS:8", "RS:5"] {
        assert_eq!(step(&mut db, "select nextval(seq8)"), expect);
    }
}

/// `SETVAL` applies NO range validation: rebasing past MAXVALUE succeeds
/// and returns the value — the sequence is simply exhausted afterward
/// (confirmed via `gorun`).
#[test]
fn sequence_setval_beyond_max() {
    let mut db = Database::new();
    step(&mut db, "create sequence seq11 minvalue 1 maxvalue 100");
    assert_eq!(step(&mut db, "select setval(seq11, 200)"), "RS:200");
    assert_eq!(step(&mut db, "select setval(seq11, -5)"), "RS:<nil>");
    assert_eq!(
        step(&mut db, "select nextval(seq11)"),
        "Eval(Sequence(\"sequence has run out\"))"
    );
}

/// The fresh-cache `SETVAL` artifact this module's faithful port exists
/// for (see `crate::sequence`'s module doc): on a DECREASING sequence
/// with no `NEXTVAL` yet, real TiDB's own uninitialized-cache
/// short-circuit answers `NULL` for any value ≥ 0 and changes NOTHING —
/// where a naive logical-counter model would rebase to 3 and answer 3,
/// making the following `NEXTVAL` 2 instead of the correct 10 (all
/// confirmed via `gorun`, task #128).
#[test]
fn sequence_setval_fresh_decreasing_artifact() {
    let mut db = Database::new();
    step(
        &mut db,
        "create sequence seq12 increment by -1 start with 10 minvalue 1 maxvalue 10",
    );
    assert_eq!(step(&mut db, "select setval(seq12, 3)"), "RS:<nil>");
    assert_eq!(step(&mut db, "select nextval(seq12)"), "RS:10");
    assert_eq!(step(&mut db, "select setval(seq12, 20)"), "RS:<nil>");
}

/// `SETVAL` below a fresh sequence's own start is already satisfied —
/// `NULL`, and the first `NEXTVAL` still answers `START` (both the
/// in-range-below-start and negative-value shapes confirmed via `gorun`).
#[test]
fn sequence_setval_fresh_below_start() {
    let mut db = Database::new();
    step(&mut db, "create sequence seq13 start with 1000");
    assert_eq!(step(&mut db, "select setval(seq13, 500)"), "RS:<nil>");
    assert_eq!(step(&mut db, "select nextval(seq13)"), "RS:1000");
    step(&mut db, "create sequence seq14 start with 1000");
    assert_eq!(step(&mut db, "select setval(seq14, -5)"), "RS:<nil>");
    assert_eq!(step(&mut db, "select nextval(seq14)"), "RS:1000");
}

/// Sequence allocation is NON-transactional: `ROLLBACK` does not undo a
/// `NEXTVAL` (confirmed via `gorun` — the whole reason
/// `Database::sequences` lives outside the snapshot).
#[test]
fn sequence_not_transactional() {
    let mut db = Database::new();
    step(&mut db, "create sequence seq1");
    step(&mut db, "begin");
    assert_eq!(step(&mut db, "select nextval(seq1)"), "RS:1");
    assert_eq!(step(&mut db, "select nextval(seq1)"), "RS:2");
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select nextval(seq1)"), "RS:3");
    assert_eq!(step(&mut db, "select lastval(seq1)"), "RS:3");
}

/// Option validation is an EXECUTION-time error (parse succeeds), same
/// as real TiDB (confirmed via `gorun`): a zero increment, an inverted
/// min/max pair, and a start outside [min, max] all fail at CREATE.
#[test]
fn sequence_create_validation() {
    let mut db = Database::new();
    assert_eq!(
        step(&mut db, "create sequence seq20 increment by 0"),
        "Unsupported(\"invalid sequence options\")"
    );
    assert_eq!(
        step(&mut db, "create sequence seq21 minvalue 10 maxvalue 5"),
        "Unsupported(\"invalid sequence options\")"
    );
    assert_eq!(
        step(
            &mut db,
            "create sequence seq22 start with 100 minvalue 1 maxvalue 50"
        ),
        "Unsupported(\"invalid sequence options\")"
    );
    // MAXVALUE exactly i64::MAX is also invalid (validateSequenceOptions'
    // own `MaxValue != math.MaxInt64` check, confirmed via `gorun`).
    assert_eq!(
        step(
            &mut db,
            "create sequence seq23 maxvalue 9223372036854775807"
        ),
        "Unsupported(\"invalid sequence options\")"
    );
}

/// Sequences share the table NAMESPACE but each DROP statement kind only
/// drops its own kind (all six collision/drop shapes confirmed via
/// `gorun` in one probe script).
#[test]
fn sequence_table_namespace() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "create table t1 (a int)"), "OK");
    assert_eq!(
        step(&mut db, "create sequence t1"),
        "Unsupported(\"table or sequence already exists\")"
    );
    assert_eq!(step(&mut db, "create sequence s1"), "OK");
    assert_eq!(
        step(&mut db, "create table s1 (a int)"),
        "Unsupported(\"table or sequence already exists\")"
    );
    assert_eq!(step(&mut db, "create sequence if not exists s1"), "OK");
    assert_eq!(
        step(&mut db, "create sequence s1"),
        "Unsupported(\"table or sequence already exists\")"
    );
    // DROP TABLE does not see sequences; DROP SEQUENCE does not see
    // tables.
    assert_eq!(step(&mut db, "drop table s1"), "UnknownTable(\"s1\")");
    assert_eq!(step(&mut db, "drop sequence t1"), "UnknownTable(\"t1\")");
    assert_eq!(step(&mut db, "drop sequence if exists nosuch"), "OK");
    assert_eq!(
        step(&mut db, "drop sequence nosuch"),
        "UnknownTable(\"nosuch\")"
    );
    // Drop, then the functions no longer resolve it.
    assert_eq!(step(&mut db, "create sequence s2"), "OK");
    assert_eq!(step(&mut db, "select nextval(s2)"), "RS:1");
    assert_eq!(step(&mut db, "drop sequence s2"), "OK");
    assert_eq!(
        step(&mut db, "select nextval(s2)"),
        "Eval(Sequence(\"unknown sequence\"))"
    );
    // Recreating the name starts fresh — including LASTVAL, which is
    // NULL again (real TiDB keys it by sequence ID, which changed).
    assert_eq!(step(&mut db, "create sequence s2"), "OK");
    assert_eq!(step(&mut db, "select lastval(s2)"), "RS:<nil>");
    assert_eq!(step(&mut db, "select nextval(s2)"), "RS:1");
}

/// `NEXT VALUE FOR seq` (the SQL-standard alternate syntax, task #121's
/// parse-time desugaring to `NEXTVAL(seq)`) executes identically.
#[test]
fn sequence_next_value_for_syntax() {
    let mut db = Database::new();
    step(&mut db, "create sequence seq1");
    assert_eq!(step(&mut db, "select next value for seq1"), "RS:1");
    assert_eq!(step(&mut db, "select nextval(seq1)"), "RS:2");
}

/// `ALTER SEQUENCE ... RESTART` resets the counter to `START` (bare) or
/// the given value; the intricate cache-window interaction of a later
/// `INCREMENT BY` change (the `1105` value, far past the naive next) is
/// reproduced faithfully by the ported allocator — every value here
/// copied from a `gorun` probe.
#[test]
fn sequence_alter_restart_and_increment() {
    let mut db = Database::new();
    step(&mut db, "create sequence s1 start with 5");
    assert_eq!(step(&mut db, "select nextval(s1)"), "RS:5");
    assert_eq!(step(&mut db, "select nextval(s1)"), "RS:6");
    assert_eq!(step(&mut db, "alter sequence s1 restart"), "OK");
    assert_eq!(step(&mut db, "select nextval(s1)"), "RS:5");
    assert_eq!(step(&mut db, "alter sequence s1 restart with 100"), "OK");
    assert_eq!(step(&mut db, "select nextval(s1)"), "RS:100");
    assert_eq!(step(&mut db, "select nextval(s1)"), "RS:101");
    // Meta change with the counter already advanced deep into the cache
    // batch: the next value seeks on the new lattice from the persistent
    // counter, landing at 1105 — the exact cache-window artifact real
    // TiDB exhibits (task #130's own recorded probe).
    assert_eq!(step(&mut db, "alter sequence s1 increment by 10"), "OK");
    assert_eq!(step(&mut db, "select nextval(s1)"), "RS:1105");
    // Unknown sequence: a real error, suppressed by `IF EXISTS`.
    assert_eq!(
        step(&mut db, "alter sequence nosuch restart"),
        "UnknownTable(\"nosuch\")"
    );
    assert_eq!(
        step(&mut db, "alter sequence if exists nosuch restart"),
        "OK"
    );
}

/// A single `ALTER` combining `RESTART WITH` and `INCREMENT BY`: the
/// restart value seeds the counter and the new increment drives the
/// lattice (restart-with value 2, increment 3, offset still the original
/// start 5 → 4, 5 — the wrapping-seek result, gorun-confirmed).
#[test]
fn sequence_alter_combined_restart_increment() {
    let mut db = Database::new();
    step(&mut db, "create sequence a1 start with 5");
    assert_eq!(
        step(&mut db, "alter sequence a1 restart with 2 increment by 3"),
        "OK"
    );
    assert_eq!(step(&mut db, "select nextval(a1)"), "RS:4");
    assert_eq!(step(&mut db, "select nextval(a1)"), "RS:5");
    // A bare `ALTER SEQUENCE name` (no options) is a parse error, so it
    // never reaches execution — asserted in `tidb-parser`'s own
    // `sequence_statements` test, not here.
}

/// `ALTER`'s option validation runs BEFORE committing, over the
/// would-be-final values, and rejects with the sequence UNCHANGED — an
/// execution-time error, same as `CREATE` (all gorun-confirmed).
#[test]
fn sequence_alter_validation() {
    let mut db = Database::new();
    step(&mut db, "create sequence c1 minvalue 1 maxvalue 3 cycle");
    assert_eq!(
        step(&mut db, "alter sequence c1 increment by 0"),
        "Unsupported(\"invalid sequence options\")"
    );
    assert_eq!(
        step(&mut db, "alter sequence c1 maxvalue 1"),
        "Unsupported(\"invalid sequence options\")"
    );
    // Rejected alters left the sequence usable and unchanged: it still
    // cycles 1..3.
    for expect in ["RS:1", "RS:2", "RS:3", "RS:1"] {
        assert_eq!(step(&mut db, "select nextval(c1)"), expect);
    }
    // ... and RESTART after cycling resets to START.
    assert_eq!(step(&mut db, "alter sequence c1 restart"), "OK");
    assert_eq!(step(&mut db, "select nextval(c1)"), "RS:1");
}
