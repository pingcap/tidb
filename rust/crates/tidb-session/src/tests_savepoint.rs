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

//! `SAVEPOINT` / `ROLLBACK TO [SAVEPOINT]` / `RELEASE SAVEPOINT`: named marks
//! inside one transaction, and the rules Go attaches to them.
//!
//! The Go mechanism is `TxnCtx.Savepoints` (`pkg/sessionctx/variable/
//! session.go`) over the membuffer checkpoint `pkg/kv/union_store.go`'s
//! `MemBuffer.Staging()` already gives statement-level rollback -- the same
//! primitive, held under a name for longer than one statement. The
//! statements are `pkg/executor/simple.go`'s `executeSavepoint` /
//! `executeReleaseSavepoint` and `executeRollback`'s savepoint arm.
//!
//! Every rule below is taken from real TiDB output: the
//! `savepoint`, `savepoint_autocommit`, `txn_savepoint1_source` (anchored on
//! `pkg/executor/test/txn/txn_test.go` `TestTxnSavepoint1`) and
//! `savepoint_big_txn_source` corpus topics in `rust/difftests/corpus/table/`,
//! which the result ring runs against this engine.
//!
//! SCOPE: these run on in-process `MemTableStorage`, so what they prove is the
//! session's catalog-image savepoint stack. The cluster path keeps its own
//! stack of `MutationBuffer` images and is pinned in `tidb_server`'s
//! `cluster_session_node` tests.

#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

fn ids(session: &mut Session) -> Vec<String> {
    rows(session, "SELECT id FROM sp ORDER BY id")
        .into_iter()
        .map(|row| row[0].clone())
        .collect()
}

fn session_with_table() -> Session {
    let mut session = Session::new();
    session.run("CREATE TABLE sp (id INT PRIMARY KEY)").unwrap();
    session
}

/// The core: `ROLLBACK TO` takes back exactly the writes made after the
/// savepoint, and leaves the transaction OPEN so a later statement joins it
/// and `COMMIT` publishes the survivors.
#[test]
fn rollback_to_a_savepoint_leaves_the_transaction_open_and_commit_publishes_the_survivors() {
    let mut session = session_with_table();
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO sp VALUES (1)").unwrap();
    session.run("SAVEPOINT s1").unwrap();
    session.run("INSERT INTO sp VALUES (2)").unwrap();
    session.run("ROLLBACK TO s1").unwrap();
    assert_eq!(ids(&mut session), ["1"]);
    // The transaction did not end: this insert belongs to it, not to a new
    // autocommit statement.
    assert!(session.in_transaction());
    session.run("INSERT INTO sp VALUES (3)").unwrap();
    session.run("COMMIT").unwrap();
    assert!(!session.in_transaction());
    // A fresh peer over the same catalog sees exactly what COMMIT published.
    let mut peer = Session::with_catalog(session.shared_catalog());
    assert_eq!(ids(&mut peer), ["1", "3"]);
}

/// `ROLLBACK TO` does not consume the savepoint: Go truncates the stack to
/// `[:idx+1]`, keeping the named one, so the same rollback repeats.
#[test]
fn a_savepoint_survives_its_own_rollback_and_can_be_rolled_back_to_again() {
    let mut session = session_with_table();
    session.run("BEGIN").unwrap();
    session.run("SAVEPOINT s1").unwrap();
    session.run("INSERT INTO sp VALUES (1)").unwrap();
    session.run("ROLLBACK TO s1").unwrap();
    session.run("INSERT INTO sp VALUES (2)").unwrap();
    session.run("ROLLBACK TO s1").unwrap();
    assert!(ids(&mut session).is_empty());
}

/// `ROLLBACK TO` drops every savepoint taken AFTER the one it names, so the
/// later name is gone.
#[test]
fn rolling_back_drops_the_savepoints_taken_after_it() {
    let mut session = session_with_table();
    session.run("BEGIN").unwrap();
    session.run("SAVEPOINT s1").unwrap();
    session.run("SAVEPOINT s2").unwrap();
    session.run("ROLLBACK TO s1").unwrap();
    assert!(matches!(
        session.run("ROLLBACK TO s2"),
        Err(DriverError::SavepointNotExists(name)) if name == "s2"
    ));
}

/// `RELEASE SAVEPOINT` drops the named savepoint and everything above it
/// without touching data (Go: `Savepoints[:i]`).
#[test]
fn release_drops_the_named_savepoint_and_its_successors_but_no_data() {
    let mut session = session_with_table();
    session.run("BEGIN").unwrap();
    session.run("SAVEPOINT s1").unwrap();
    session.run("INSERT INTO sp VALUES (1)").unwrap();
    session.run("SAVEPOINT s2").unwrap();
    session.run("RELEASE SAVEPOINT s1").unwrap();
    // The row written after s1 is untouched by the release.
    assert_eq!(ids(&mut session), ["1"]);
    for name in ["s1", "s2"] {
        assert!(matches!(
            session.run(&format!("ROLLBACK TO {name}")),
            Err(DriverError::SavepointNotExists(_))
        ));
    }
}

/// Releasing a MIDDLE savepoint takes the ones above it with it and leaves the
/// ones below reachable.
#[test]
fn releasing_a_middle_savepoint_keeps_the_ones_below_it() {
    let mut session = session_with_table();
    session.run("BEGIN").unwrap();
    session.run("SAVEPOINT s1").unwrap();
    session.run("INSERT INTO sp VALUES (1)").unwrap();
    session.run("SAVEPOINT s2").unwrap();
    session.run("SAVEPOINT s3").unwrap();
    session.run("RELEASE SAVEPOINT s2").unwrap();
    assert!(matches!(
        session.run("ROLLBACK TO s3"),
        Err(DriverError::SavepointNotExists(_))
    ));
    session.run("ROLLBACK TO s1").unwrap();
    assert!(ids(&mut session).is_empty());
}

/// Names match case-insensitively (Go lowercases in `AddSavepoint` and every
/// lookup), and the error text reports the spelling the statement used.
#[test]
fn savepoint_names_are_case_insensitive() {
    let mut session = session_with_table();
    session.run("BEGIN").unwrap();
    session.run("SAVEPOINT SP1").unwrap();
    session.run("INSERT INTO sp VALUES (1)").unwrap();
    session.run("ROLLBACK TO sp1").unwrap();
    assert!(ids(&mut session).is_empty());
    session.run("RELEASE SAVEPOINT sP1").unwrap();
    let error = session.run("ROLLBACK TO Sp1").unwrap_err();
    assert_eq!(
        error.to_mysql_error().message,
        "SAVEPOINT Sp1 does not exist"
    );
}

/// Redefining an existing name MOVES it to the end of the stack rather than
/// updating it in place (Go `AddSavepoint`: delete, then append). The
/// observable consequence is the one captured from real TiDB: rolling back to
/// the redefined name restores the LATER mark and no longer drops the
/// savepoint that was taken between the two definitions.
#[test]
fn redefining_a_savepoint_moves_it_to_the_end_of_the_stack() {
    let mut session = session_with_table();
    session.run("BEGIN").unwrap();
    session.run("SAVEPOINT a").unwrap();
    session.run("INSERT INTO sp VALUES (1)").unwrap();
    session.run("SAVEPOINT b").unwrap();
    session.run("INSERT INTO sp VALUES (2)").unwrap();
    session.run("SAVEPOINT a").unwrap();
    session.run("INSERT INTO sp VALUES (3)").unwrap();
    // `a` now marks the point after row 2, not the empty table.
    session.run("ROLLBACK TO a").unwrap();
    assert_eq!(ids(&mut session), ["1", "2"]);
    // `b` was taken BEFORE the redefinition, so it is below `a` and survived.
    session.run("ROLLBACK TO b").unwrap();
    assert_eq!(ids(&mut session), ["1"]);
}

/// `SAVEPOINT` in AUTOCOMMIT with no explicit transaction is a harmless
/// no-op that records nothing: Go returns `nil` before touching `TxnCtx`, so
/// the statement succeeds and the name is still unknown afterwards. The
/// autocommit half of the condition matters -- see
/// [`a_savepoint_with_autocommit_off_opens_the_transaction`].
#[test]
fn a_savepoint_outside_a_transaction_records_nothing() {
    let mut session = session_with_table();
    session.run("SAVEPOINT s1").unwrap();
    assert!(matches!(
        session.run("ROLLBACK TO s1"),
        Err(DriverError::SavepointNotExists(_))
    ));
    assert!(matches!(
        session.run("RELEASE SAVEPOINT s1"),
        Err(DriverError::SavepointNotExists(_))
    ));
}

/// An unknown name is Go's `ErrSavepointNotExists` -- 1305, SQLSTATE 42000,
/// "SAVEPOINT %s does not exist" -- for both statements that take a name.
#[test]
fn an_unknown_savepoint_is_error_1305() {
    let mut session = session_with_table();
    session.run("BEGIN").unwrap();
    for sql in ["ROLLBACK TO nosuch", "RELEASE SAVEPOINT nosuch"] {
        let reported = session.run(sql).unwrap_err().to_mysql_error();
        assert_eq!(reported.code, 1305);
        assert_eq!(&reported.state, b"42000");
        assert_eq!(reported.message, "SAVEPOINT nosuch does not exist");
    }
    // ROLLBACK TO with no transaction at all reports the same error, which is
    // Go's `!txn.Valid()` arm.
    session.run("ROLLBACK").unwrap();
    assert!(matches!(
        session.run("ROLLBACK TO nosuch"),
        Err(DriverError::SavepointNotExists(_))
    ));
}

/// A statement that fails rolls ITSELF back and leaves the savepoint stack
/// alone, so a `ROLLBACK TO` after it still restores the earlier point.
#[test]
fn rolling_back_after_a_failed_statement_still_restores_the_savepoint() {
    let mut session = session_with_table();
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO sp VALUES (1)").unwrap();
    session.run("SAVEPOINT s1").unwrap();
    session.run("INSERT INTO sp VALUES (2)").unwrap();
    // Duplicate primary key: the statement fails and stores nothing.
    session.run("INSERT INTO sp VALUES (3), (1)").unwrap_err();
    assert_eq!(ids(&mut session), ["1", "2"]);
    session.run("ROLLBACK TO s1").unwrap();
    assert_eq!(ids(&mut session), ["1"]);
}

/// AUTO_INCREMENT ids are allocated outside transaction semantics and are
/// never given back: rolling back to a savepoint undoes the ROWS, not the
/// allocator, exactly as a failed statement's rollback already does.
#[test]
fn rolling_back_to_a_savepoint_does_not_return_auto_increment_ids() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE spa (id INT AUTO_INCREMENT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO spa (v) VALUES (1)").unwrap();
    session.run("SAVEPOINT s1").unwrap();
    session.run("INSERT INTO spa (v) VALUES (2)").unwrap();
    session.run("ROLLBACK TO s1").unwrap();
    session.run("INSERT INTO spa (v) VALUES (3)").unwrap();
    session.run("COMMIT").unwrap();
    assert_eq!(
        row_text(session.run("SELECT id, v FROM spa ORDER BY id")),
        [
            vec!["1".to_owned(), "1".to_owned()],
            vec!["3".to_owned(), "3".to_owned()]
        ]
    );
}

/// A DDL statement implicitly commits the transaction, which ends it -- and
/// the savepoint stack goes with it, as Go's `TxnCtx` does.
#[test]
fn a_ddl_statements_implicit_commit_clears_the_savepoint_stack() {
    let mut session = session_with_table();
    session.run("BEGIN").unwrap();
    session.run("SAVEPOINT s1").unwrap();
    session.run("CREATE TABLE sp2 (id INT)").unwrap();
    assert!(matches!(
        session.run("ROLLBACK TO s1"),
        Err(DriverError::SavepointNotExists(_))
    ));
}

/// A plain `ROLLBACK` ends the transaction and takes the whole stack with it;
/// so does `COMMIT`.
#[test]
fn ending_the_transaction_clears_the_savepoint_stack() {
    for ending in ["ROLLBACK", "COMMIT"] {
        let mut session = session_with_table();
        session.run("BEGIN").unwrap();
        session.run("SAVEPOINT s1").unwrap();
        session.run(ending).unwrap();
        session.run("BEGIN").unwrap();
        assert!(
            matches!(
                session.run("ROLLBACK TO s1"),
                Err(DriverError::SavepointNotExists(_))
            ),
            "{ending} left a savepoint behind"
        );
    }
}

/// With autocommit OFF and no explicit `BEGIN`, `SAVEPOINT` is the statement
/// that OPENS the transaction: Go's no-op arm needs BOTH `!InTxn()` and
/// `IsAutocommit()`, and with autocommit off it falls through to
/// `e.Ctx().Txn(true)`, which activates the pending transaction before
/// `AddSavepoint`. Captured from Go (`rust/difftests/gorun`):
///
/// ```text
/// set autocommit = 0            OK
/// savepoint before_write        OK
/// insert into spac values (1)   OK
/// rollback to before_write      OK      <- the savepoint EXISTS
/// select id from spac           RS:     <- the row is gone
/// insert into spac values (2)   OK
/// commit                        OK
/// select id from spac           RS:2
/// set autocommit = 1
/// savepoint no_txn              OK      <- records nothing
/// rollback to no_txn            ERR     <- 1305
/// ```
#[test]
fn a_savepoint_with_autocommit_off_opens_the_transaction() {
    let mut session = Session::new();
    session.run("CREATE TABLE spac (id INT)").unwrap();
    session.run("SET autocommit = 0").unwrap();
    session.run("SAVEPOINT before_write").unwrap();
    session.run("INSERT INTO spac VALUES (1)").unwrap();
    session.run("ROLLBACK TO before_write").unwrap();
    assert!(rows(&mut session, "SELECT id FROM spac ORDER BY id").is_empty());
    session.run("INSERT INTO spac VALUES (2)").unwrap();
    session.run("COMMIT").unwrap();
    assert_eq!(
        row_text(session.run("SELECT id FROM spac ORDER BY id")),
        [vec!["2".to_owned()]]
    );

    // Back in autocommit the no-op arm applies again, and `ROLLBACK TO` is
    // Go's 1305.
    session.run("SET autocommit = 1").unwrap();
    session.run("SAVEPOINT no_txn").unwrap();
    let reported = session
        .run("ROLLBACK TO no_txn")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(reported.code, 1305);
    assert_eq!(&reported.state, b"42000");
    assert_eq!(reported.message, "SAVEPOINT no_txn does not exist");
    assert_eq!(
        row_text(session.run("SELECT id FROM spac ORDER BY id")),
        [vec!["2".to_owned()]]
    );
}
