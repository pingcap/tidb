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

//! Source-backed transaction lifecycle, rollback-image, savepoint,
//! autocommit, and isolation tests.

use super::*;

/// Structural port of `pkg/session/test/txn/txn_test.go:378 TestInTrans`.
///
/// TiDB exposes this state through `Session.Txn(true).Valid()`.  The seed
/// executor has no KV transaction handle, but `TransactionState::is_active`
/// is precisely its transaction-liveness boundary: it is true while an explicit or lazy
/// non-autocommit transaction can be rolled back, and every DDL statement
/// clears it through the same implicit-commit path as TiDB.  Keep the source
/// statement order intact so the status assertions cover the whole original
/// single-session control flow rather than only its convenient DML fragments.
#[test]
fn in_transaction_source_contract() {
    let mut db = Database::new();

    assert_eq!(step(&mut db, "drop table if exists in_trans_src"), "OK");
    assert_eq!(
        step(
            &mut db,
            "create table in_trans_src (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)",
        ),
        "OK"
    );
    assert_eq!(step(&mut db, "insert in_trans_src values ()"), "OK");
    assert_eq!(step(&mut db, "begin"), "OK");
    assert!(db.transaction.is_active());
    assert_eq!(step(&mut db, "insert in_trans_src values ()"), "OK");
    assert!(db.transaction.is_active());
    assert_eq!(step(&mut db, "drop table if exists in_trans_src"), "OK");
    assert!(!db.transaction.is_active());
    assert_eq!(
        step(
            &mut db,
            "create table in_trans_src (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)",
        ),
        "OK"
    );
    assert!(!db.transaction.is_active());
    assert_eq!(step(&mut db, "insert in_trans_src values ()"), "OK");
    assert!(!db.transaction.is_active());
    assert_eq!(step(&mut db, "commit"), "OK");
    assert_eq!(step(&mut db, "insert in_trans_src values ()"), "OK");

    assert_eq!(step(&mut db, "set autocommit=0"), "OK");
    assert_eq!(step(&mut db, "begin"), "OK");
    assert!(db.transaction.is_active());
    assert_eq!(step(&mut db, "insert in_trans_src values ()"), "OK");
    assert!(db.transaction.is_active());
    assert_eq!(step(&mut db, "commit"), "OK");
    assert!(!db.transaction.is_active());
    assert_eq!(step(&mut db, "insert in_trans_src values ()"), "OK");
    assert!(db.transaction.is_active());
    assert_eq!(step(&mut db, "commit"), "OK");
    assert!(!db.transaction.is_active());

    assert_eq!(step(&mut db, "set autocommit=1"), "OK");
    assert_eq!(step(&mut db, "drop table if exists in_trans_src"), "OK");
    assert_eq!(
        step(
            &mut db,
            "create table in_trans_src (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)",
        ),
        "OK"
    );
    assert_eq!(step(&mut db, "begin"), "OK");
    assert!(db.transaction.is_active());
    assert_eq!(step(&mut db, "insert in_trans_src values ()"), "OK");
    assert!(db.transaction.is_active());
    assert_eq!(step(&mut db, "rollback"), "OK");
    assert!(!db.transaction.is_active());
}

/// Structural, single-session port of
/// `pkg/session/test/txn/txn_test.go:131 TestTxnLazyInitialize`.
///
/// The source runs this contract in both optimistic and pessimistic TiKV
/// modes. This seed has one snapshot implementation, so it proves the common
/// lazy-start control flow once: table-free reads leave `autocommit = 0`
/// transaction-free; a base-table read, `BEGIN`, and DML establish a rollback
/// boundary. `@@tidb_current_ts`, `tidb_general_log`, `EXPLAIN`, and engine
/// selection are deliberately excluded because they require TiKV/session
/// protocol state rather than this executor's in-process catalog.
#[test]
fn transaction_lazy_initialization_source_contract() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "create table lazy_txn_src (id int)"), "OK");

    assert_eq!(step(&mut db, "set @@autocommit = 0"), "OK");
    assert!(!db.transaction.is_active());

    // Source: `select 1` does not force TiDB's lazy transaction context.
    assert_eq!(step(&mut db, "select 1"), "RS:1");
    assert!(!db.transaction.is_active());

    // Source: a table read does establish it, even before any mutation.
    assert_eq!(step(&mut db, "select * from lazy_txn_src"), "RS:");
    assert!(db.transaction.is_active());
    assert_eq!(step(&mut db, "rollback"), "OK");
    assert!(!db.transaction.is_active());

    assert_eq!(step(&mut db, "begin"), "OK");
    assert!(db.transaction.is_active());
    assert_eq!(step(&mut db, "rollback"), "OK");
    assert!(!db.transaction.is_active());

    assert_eq!(step(&mut db, "insert into lazy_txn_src values (1)"), "OK");
    assert!(db.transaction.is_active());
    assert_eq!(step(&mut db, "rollback"), "OK");
    assert_eq!(
        step(&mut db, "select * from lazy_txn_src order by id"),
        "RS:"
    );
}

/// Structural port of the executable single-session core of
/// `pkg/session/test/txn/txn_test.go:483 TestMemBufferCleanupMemoryLeak`.
///
/// TiDB's source repeatedly submits a two-row INSERT whose latter row
/// collides with a value already buffered by the transaction. Each failed
/// statement must clean up its own earlier mutation; otherwise the next
/// retry would collide on both rows and the transaction would accumulate
/// stale write state. The source's memory tracker/quota setup measures TiKV
/// allocation reclamation, which this in-memory seed intentionally has no
/// analogue for. The loop count and duplicate control flow below are copied
/// exactly; the final scan adds the catalog-level assertion that makes leaked
/// statement mutations observable in this executor.
#[test]
fn failed_duplicate_insert_cleans_up_each_statement_before_commit() {
    let mut db = Database::new();
    assert_eq!(
        step(&mut db, "create table txn_cleanup_src (a int primary key)"),
        "OK"
    );
    assert_eq!(step(&mut db, "begin"), "OK");
    assert_eq!(
        step(&mut db, "insert into txn_cleanup_src values (2)"),
        "OK"
    );

    for _ in 0..100 {
        assert_eq!(
            step(&mut db, "insert into txn_cleanup_src values (1), (2)"),
            "DuplicateKey"
        );
    }

    assert_eq!(step(&mut db, "commit"), "OK");
    assert_eq!(
        step(&mut db, "select a from txn_cleanup_src order by a"),
        "RS:2"
    );
}

/// Structural, single-session port of
/// `pkg/session/test/txn/txn_test.go:438 TestMemBufferSnapshotRead`.
///
/// The Go regression writes the complete `0..=100` relation into an open
/// transaction, then reads that buffered relation through an `INSERT ..
/// SELECT` self-join while the target is also the source. Its duplicate-key
/// action must see the statement's snapshot rather than partially-written
/// target rows: every final pair satisfies `a + b = 100`, both before and
/// after commit. The source's DistSQL/chunk-size knobs select TiKV execution
/// operators; this in-process executor has one deterministic scan path, so
/// they have no corresponding state here. The SQL data flow and transaction
/// boundary are retained verbatim.
#[test]
fn transaction_buffer_snapshot_insert_select_source_contract() {
    let mut db = Database::new();
    assert_eq!(
        step(
            &mut db,
            "create table txn_snapshot_src (a int primary key, b int, index i(b))",
        ),
        "OK"
    );
    assert_eq!(step(&mut db, "begin"), "OK");

    let rows = (0..=100)
        .map(|value| format!("({value}, {value})"))
        .collect::<Vec<_>>()
        .join(", ");
    assert_eq!(
        step(
            &mut db,
            &format!("insert into txn_snapshot_src values {rows}"),
        ),
        "OK"
    );

    assert_eq!(
        step(
            &mut db,
            "insert into txn_snapshot_src (select /*+ INL_JOIN(t1) */ 100 - t1.a as a, t1.b from txn_snapshot_src t1, (select a, b from txn_snapshot_src) t2 where t1.b = t2.b) on duplicate key update b = values(b)",
        ),
        "OK"
    );
    assert_eq!(
        step(
            &mut db,
            "select a, b from txn_snapshot_src where a + b != 100",
        ),
        "RS:"
    );
    assert_eq!(step(&mut db, "commit"), "OK");
    assert_eq!(
        step(
            &mut db,
            "select a, b from txn_snapshot_src where a + b != 100",
        ),
        "RS:"
    );
}

#[test]
fn transactions() {
    let mut db = Database::new();
    step(&mut db, "create table t (id int, v int)");
    step(&mut db, "insert into t values (1, 10)");
    // COMMIT/ROLLBACK with nothing pending are harmless no-ops.
    step(&mut db, "commit");
    step(&mut db, "rollback");

    // BEGIN snapshots the current catalog; ROLLBACK restores it.
    step(&mut db, "begin");
    step(&mut db, "insert into t values (2, 20)");
    assert_eq!(
        step(&mut db, "select id, v from t order by id"),
        "RS:1|10;2|20"
    );
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id, v from t order by id"), "RS:1|10");

    // COMMIT (or autocommit, outside any transaction) makes a change
    // permanent -- a LATER rollback has nothing to undo.
    step(&mut db, "start transaction");
    step(&mut db, "insert into t values (3, 30)");
    step(&mut db, "commit");
    assert_eq!(
        step(&mut db, "select id, v from t order by id"),
        "RS:1|10;3|30"
    );

    // DELETE and UPDATE both roll back too, not just INSERT.
    step(&mut db, "begin");
    step(&mut db, "delete from t where id = 3");
    step(&mut db, "update t set v = 999 where id = 1");
    assert_eq!(step(&mut db, "select id, v from t order by id"), "RS:1|999");
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id, v from t order by id"),
        "RS:1|10;3|30"
    );

    // A second BEGIN while already in a transaction implicitly
    // commits the pending one first (here, nothing was pending, so
    // the fresh snapshot is just the current state) -- the insert
    // AFTER the second BEGIN is what gets rolled back.
    step(&mut db, "begin");
    step(&mut db, "begin");
    step(&mut db, "insert into t values (4, 40)");
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id, v from t order by id"),
        "RS:1|10;3|30"
    );

    // DDL causes an implicit commit (a real MySQL rule, confirmed via
    // `gorun`): a pending INSERT survives a LATER rollback once a
    // CREATE TABLE has run in between.
    step(&mut db, "begin");
    step(&mut db, "insert into t values (5, 50)");
    step(&mut db, "create table u (id int)");
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id, v from t order by id"),
        "RS:1|10;3|30;5|50"
    );

    // ALTER TABLE is DDL too -- the added column survives a rollback
    // issued after it.
    step(&mut db, "begin");
    step(&mut db, "alter table t add column w int");
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id, v, w from t order by id"),
        "RS:1|10|<nil>;3|30|<nil>;5|50|<nil>"
    );

    // SAVEPOINT / ROLLBACK TO / RELEASE SAVEPOINT (confirmed via
    // `gorun`, every case verified in isolation first).
    let mut db2 = Database::new();
    step(&mut db2, "create table t (id int)");
    step(&mut db2, "begin");
    step(&mut db2, "insert into t values (1)");
    step(&mut db2, "savepoint sp1");
    step(&mut db2, "insert into t values (2)");
    assert_eq!(step(&mut db2, "select * from t"), "RS:1;2");
    // ROLLBACK TO restores the data but does NOT end the transaction
    // or remove the savepoint -- a repeated rollback to it succeeds
    // again, and further work (a fresh insert, then COMMIT) still
    // applies.
    step(&mut db2, "rollback to sp1");
    assert_eq!(step(&mut db2, "select * from t"), "RS:1");
    step(&mut db2, "rollback to sp1");
    assert_eq!(step(&mut db2, "select * from t"), "RS:1");
    step(&mut db2, "insert into t values (3)");
    step(&mut db2, "commit");
    assert_eq!(step(&mut db2, "select * from t"), "RS:1;3");

    // Rolling back to an EARLIER savepoint releases every savepoint
    // defined AFTER it.
    step(&mut db2, "begin");
    step(&mut db2, "savepoint a");
    step(&mut db2, "insert into t values (4)");
    step(&mut db2, "savepoint b");
    assert_eq!(step(&mut db2, "rollback to a"), "OK");
    assert!(step(&mut db2, "rollback to b").starts_with("UnknownSavepoint"));
    step(&mut db2, "rollback");

    // RELEASE SAVEPOINT removes the named savepoint AND every
    // savepoint defined after it, without touching data.
    step(&mut db2, "begin");
    step(&mut db2, "savepoint a");
    step(&mut db2, "savepoint b");
    step(&mut db2, "release savepoint a");
    assert!(step(&mut db2, "rollback to b").starts_with("UnknownSavepoint"));
    step(&mut db2, "rollback");

    // Redefining an EXISTING savepoint name moves it to the END of
    // the savepoint list (removing the old entry, not updating it in
    // place): rolling back to an EARLIER, untouched savepoint still
    // works even after a LATER one was redefined, but rolling back
    // to the redefined one FIRST then invalidates the earlier one
    // (since the redefinition moved it to after that point).
    step(&mut db2, "begin");
    step(&mut db2, "insert into t values (10)");
    step(&mut db2, "savepoint a");
    step(&mut db2, "insert into t values (11)");
    step(&mut db2, "savepoint b");
    step(&mut db2, "insert into t values (12)");
    step(&mut db2, "savepoint a");
    step(&mut db2, "insert into t values (13)");
    step(&mut db2, "rollback to b");
    // `ORDER BY` here isn't optional for a deterministic assertion:
    // confirmed via `gorun` that a plain, unordered `SELECT * FROM t`
    // does NOT return multi-digit `id`s in numeric (or insertion)
    // order at all once BOTH a single- and a double-digit value are
    // present (e.g. a bare `INSERT INTO t VALUES (1),(3),(10),(11)`
    // with NO transaction/savepoint involved already returns
    // `1;10;11;3`) — a genuine, pre-existing real-TiDB scan-order
    // quirk unrelated to savepoints, not a bug introduced here.
    assert_eq!(
        step(&mut db2, "select * from t order by id"),
        "RS:1;3;10;11"
    );
    step(&mut db2, "rollback");

    // A SAVEPOINT outside an explicit transaction is a harmless
    // no-op that records nothing -- ROLLBACK TO/RELEASE immediately
    // afterward still error as an unknown savepoint.
    assert_eq!(step(&mut db2, "savepoint outside"), "OK");
    assert!(step(&mut db2, "rollback to outside").starts_with("UnknownSavepoint"));
    assert!(step(&mut db2, "release savepoint outside").starts_with("UnknownSavepoint"));

    // Every DDL statement's own implicit commit clears all
    // savepoints too, same as it already clears the whole
    // transaction snapshot.
    step(&mut db2, "begin");
    step(&mut db2, "savepoint a");
    step(&mut db2, "create table u2 (id int)");
    assert!(step(&mut db2, "rollback to a").starts_with("UnknownSavepoint"));
    step(&mut db2, "commit");

    // Savepoint names match case-insensitively.
    step(&mut db2, "begin");
    step(&mut db2, "savepoint SP1");
    step(&mut db2, "insert into t values (99)");
    step(&mut db2, "rollback to sp1");
    assert_eq!(step(&mut db2, "select * from t"), "RS:1;3");
    step(&mut db2, "commit");

    // `SET autocommit = 0` opens an implicit transaction the moment a
    // DML statement needs one -- no explicit BEGIN required -- and it
    // persists across MULTIPLE statements until an explicit
    // COMMIT/ROLLBACK.
    let mut db3 = Database::new();
    step(&mut db3, "create table t (id int)");
    step(&mut db3, "set autocommit=0");
    step(&mut db3, "insert into t values (1)");
    step(&mut db3, "insert into t values (2)");
    step(&mut db3, "rollback");
    assert_eq!(step(&mut db3, "select * from t"), "RS:");
    // After an explicit COMMIT, a NEW implicit transaction opens for
    // the next statement (autocommit is still off).
    step(&mut db3, "insert into t values (1)");
    step(&mut db3, "commit");
    step(&mut db3, "insert into t values (2)");
    step(&mut db3, "rollback");
    assert_eq!(step(&mut db3, "select * from t"), "RS:1");
    // A DDL statement's own implicit commit is followed immediately
    // by a fresh implicit transaction, same as an explicit COMMIT --
    // rolling back afterward undoes only the INSERT, not the CREATE
    // TABLE itself (which already implicit-committed).
    step(&mut db3, "create table u (id int)");
    step(&mut db3, "insert into u values (9)");
    step(&mut db3, "rollback");
    assert_eq!(step(&mut db3, "select * from u"), "RS:");
    // Turning autocommit back ON implicitly commits whatever is
    // pending -- a LATER ROLLBACK is then a no-op.
    step(&mut db3, "insert into t values (2)");
    step(&mut db3, "set autocommit=1");
    step(&mut db3, "rollback");
    assert_eq!(step(&mut db3, "select * from t"), "RS:1;2");
    // A redundant `SET autocommit = 1` (already on) does NOT commit
    // an in-progress EXPLICIT transaction -- the transition itself is
    // what matters, not merely the resulting value.
    step(&mut db3, "begin");
    step(&mut db3, "insert into t values (3)");
    step(&mut db3, "set autocommit=1");
    step(&mut db3, "rollback");
    assert_eq!(step(&mut db3, "select * from t"), "RS:1;2");
    // ... but toggling autocommit off and back ON WHILE inside that
    // same explicit transaction DOES commit it (real MySQL does not
    // distinguish "implicit" from "explicit" here).
    step(&mut db3, "begin");
    step(&mut db3, "insert into t values (3)");
    step(&mut db3, "set autocommit=0");
    step(&mut db3, "set autocommit=1");
    step(&mut db3, "rollback");
    assert_eq!(step(&mut db3, "select * from t order by id"), "RS:1;2;3");
    // Every "off" value form MySQL accepts behaves identically: the
    // bare keyword OFF, and a numeric-looking quoted string '0'.
    step(&mut db3, "set autocommit=off");
    step(&mut db3, "insert into t values (5)");
    step(&mut db3, "rollback");
    assert_eq!(step(&mut db3, "select * from t order by id"), "RS:1;2;3");
    step(&mut db3, "set autocommit='0'");
    step(&mut db3, "insert into t values (5)");
    step(&mut db3, "rollback");
    assert_eq!(step(&mut db3, "select * from t order by id"), "RS:1;2;3");
    // Every "on" value form behaves identically too: an ordinary
    // TRUE literal, and the quoted string 'ON' -- once autocommit is
    // back on, a bare INSERT is immediately permanent again.
    step(&mut db3, "set autocommit=true");
    step(&mut db3, "insert into t values (5)");
    step(&mut db3, "rollback");
    assert_eq!(step(&mut db3, "select * from t order by id"), "RS:1;2;3;5");
    step(&mut db3, "set autocommit='ON'");
    step(&mut db3, "insert into t values (6)");
    step(&mut db3, "rollback");
    assert_eq!(
        step(&mut db3, "select * from t order by id"),
        "RS:1;2;3;5;6"
    );

    // `SET [SESSION] TRANSACTION ISOLATION LEVEL ...`/`READ WRITE` are
    // accepted no-ops -- this project's single-session, non-concurrent
    // model has no MVCC/concurrent transactions for an isolation
    // level to affect.
    assert_eq!(
        step(&mut db3, "set transaction isolation level read committed"),
        "OK"
    );
    assert_eq!(
        step(
            &mut db3,
            "set session transaction isolation level repeatable read"
        ),
        "OK"
    );
    assert_eq!(step(&mut db3, "set transaction read write"), "OK");
    // Unlike `READ WRITE`, `READ ONLY` is a genuine `ERR` -- confirmed
    // via `gorun` that real TiDB's own mockstore rejects it outright,
    // replicated faithfully rather than implementing an unenforceable
    // read-only mode.
    assert!(step(&mut db3, "set transaction read only").starts_with("Unsupported("));
    assert!(step(&mut db3, "set session transaction read only").starts_with("Unsupported("));
}

/// Go's `SimpleExec.executeSavepoint` calls `Ctx().Txn(true)` whenever
/// autocommit is off. Therefore SAVEPOINT, unlike a plain read-only statement,
/// is itself sufficient to start the lazy transaction and establish a rollback
/// boundary before the first write. This regression is the seed-model slice of
/// TiDB's `pkg/executor/test/txn/txn_test.go:TestTxnSavepoint0`.
#[test]
fn savepoint_starts_lazy_non_autocommit_transaction() {
    let mut db = Database::new();
    step(&mut db, "create table lazy_savepoint (id int)");

    step(&mut db, "set autocommit = 0");
    step(&mut db, "savepoint before_write");
    step(&mut db, "insert into lazy_savepoint values (1)");
    step(&mut db, "rollback to before_write");
    assert_eq!(
        step(&mut db, "select id from lazy_savepoint order by id"),
        "RS:"
    );

    // In contrast, a SAVEPOINT with autocommit on remains a harmless no-op;
    // neither spelling must leave a checkpoint after COMMIT clears the one
    // established above.
    step(&mut db, "commit");
    step(&mut db, "set autocommit = 1");
    step(&mut db, "savepoint no_transaction");
    assert!(step(&mut db, "rollback to no_transaction").starts_with("UnknownSavepoint"));
}

/// The seed executor has one in-memory snapshot model, not TiKV's distinct
/// optimistic/pessimistic transaction engines. It must nevertheless accept
/// both parsed modes and retain the normal rollback contract for each.
#[test]
fn transaction_begin_modes_use_snapshot_model() {
    let mut db = Database::new();
    step(&mut db, "create table tx_mode (id int primary key)");
    step(&mut db, "insert into tx_mode values (1)");

    step(&mut db, "begin optimistic");
    step(&mut db, "insert into tx_mode values (2)");
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from tx_mode order by id"), "RS:1");

    step(&mut db, "begin pessimistic");
    step(&mut db, "insert into tx_mode values (3)");
    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from tx_mode order by id"), "RS:1");
}

/// `READ WRITE` and `WITH CONSISTENT SNAPSHOT` restore to Go's default
/// transaction form, which the seed's single snapshot safely represents.
/// Read-only/stale/causal starts require semantics this executor does not
/// have, so they must fail before creating or replacing a transaction.
#[test]
fn start_transaction_options_keep_the_execution_boundary_honest() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table tx_start_options (id int primary key)",
    );

    assert_eq!(step(&mut db, "start transaction read write"), "OK");
    step(&mut db, "insert into tx_start_options values (1)");
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from tx_start_options order by id"),
        "RS:"
    );

    assert_eq!(
        step(&mut db, "start transaction with consistent snapshot"),
        "OK"
    );
    step(&mut db, "insert into tx_start_options values (2)");
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from tx_start_options order by id"),
        "RS:"
    );

    assert_eq!(
        step(&mut db, "start transaction read only"),
        "Unsupported(\"START TRANSACTION READ ONLY\")"
    );
    assert_eq!(
        step(
            &mut db,
            "start transaction read only as of timestamp '2015-09-21 00:07:01'",
        ),
        "Unsupported(\"START TRANSACTION READ ONLY AS OF TIMESTAMP\")"
    );
    assert_eq!(
        step(&mut db, "start transaction with causal consistency only"),
        "Unsupported(\"START TRANSACTION WITH CAUSAL CONSISTENCY ONLY\")"
    );

    // None of the rejected starts made a snapshot: this autocommit INSERT
    // remains permanent after the following rollback.
    step(&mut db, "insert into tx_start_options values (3)");
    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from tx_start_options order by id"),
        "RS:3"
    );
}

/// `@@tx_isolation`/`@@tx_isolation_one_shot` readback — see
/// `crate::transaction::TransactionState`'s own docs
/// for the full `gorun`-verified reasoning (value validation,
/// mid-transaction rejection, `@@GLOBAL` behavior).
#[test]
fn tx_isolation_readback() {
    let mut db = Database::new();
    // Real MySQL/TiDB defaults.
    assert_eq!(step(&mut db, "select @@tx_isolation"), "RS:REPEATABLE-READ");
    assert_eq!(
        step(&mut db, "select @@transaction_isolation"),
        "RS:REPEATABLE-READ"
    );
    assert_eq!(
        step(&mut db, "select @@session.tx_isolation"),
        "RS:REPEATABLE-READ"
    );
    assert_eq!(
        step(&mut db, "select @@global.tx_isolation"),
        "RS:REPEATABLE-READ"
    );
    assert_eq!(step(&mut db, "select @@tx_isolation_one_shot"), "RS:");

    // `SET SESSION TRANSACTION ISOLATION LEVEL ...` changes ONLY the
    // session-scoped readback, matching `autocommit`/`time_zone`'s own
    // established `@@GLOBAL` convention.
    step(
        &mut db,
        "set session transaction isolation level read committed",
    );
    assert_eq!(step(&mut db, "select @@tx_isolation"), "RS:READ-COMMITTED");
    assert_eq!(
        step(&mut db, "select @@transaction_isolation"),
        "RS:READ-COMMITTED"
    );
    assert_eq!(
        step(&mut db, "select @@global.tx_isolation"),
        "RS:REPEATABLE-READ"
    );
    // The SESSION-prefixed form does not touch the ONE-SHOT value.
    assert_eq!(step(&mut db, "select @@tx_isolation_one_shot"), "RS:");

    // Only READ-COMMITTED/REPEATABLE-READ are real, accepted values —
    // real TiDB's own MVCC storage engine never actually executes
    // SERIALIZABLE/READ-UNCOMMITTED, a permanent restriction (see
    // `TransactionState`'s own doc), not a scope cut. A bare
    // nonsense string is rejected the same way.
    assert!(
        step(&mut db, "set transaction isolation level serializable").starts_with("Unsupported(")
    );
    assert!(step(
        &mut db,
        "set session transaction isolation level serializable"
    )
    .starts_with("Unsupported("));
    assert!(
        step(&mut db, "set transaction isolation level read uncommitted")
            .starts_with("Unsupported(")
    );
    assert!(step(&mut db, "set tx_isolation = 'nonsense'").starts_with("Unsupported("));

    // The BARE (one-shot) form sets `@@tx_isolation_one_shot`, not
    // `@@tx_isolation`.
    step(&mut db, "set transaction isolation level repeatable read");
    assert_eq!(
        step(&mut db, "select @@tx_isolation_one_shot"),
        "RS:REPEATABLE-READ"
    );
    assert_eq!(step(&mut db, "select @@tx_isolation"), "RS:READ-COMMITTED");

    // Only the ONE-SHOT form is rejected mid-transaction (real TiDB's
    // own `ErrCantChangeTxCharacteristics`) — the SESSION-prefixed form
    // works fine there.
    step(&mut db, "begin");
    assert!(
        step(&mut db, "set transaction isolation level read committed").starts_with("Unsupported(")
    );
    assert_eq!(
        step(
            &mut db,
            "set session transaction isolation level read committed"
        ),
        "OK"
    );
    step(&mut db, "rollback");

    // A direct `SET tx_isolation = value` is equivalent to the
    // `SESSION`-prefixed sugar, and case-insensitive on input (readback
    // is always normalized to the dash-joined uppercase form).
    step(&mut db, "set tx_isolation = 'read-committed'");
    assert_eq!(step(&mut db, "select @@tx_isolation"), "RS:READ-COMMITTED");

    // Go declares `transaction_isolation` and `tx_isolation` as mutual
    // aliases.  Setting either name updates the one shared session value.
    step(
        &mut db,
        "set session transaction_isolation = 'repeatable-read'",
    );
    assert_eq!(
        step(&mut db, "select @@session.tx_isolation"),
        "RS:REPEATABLE-READ"
    );
    assert_eq!(
        step(&mut db, "select @@session.transaction_isolation"),
        "RS:REPEATABLE-READ"
    );
    assert_eq!(
        step(&mut db, "select @@global.transaction_isolation"),
        "RS:REPEATABLE-READ"
    );

    // `tx_isolation_one_shot` has no `@@GLOBAL` form at all.
    assert!(step(&mut db, "select @@global.tx_isolation_one_shot").starts_with("Eval("));
}

/// Rollback restores only transactional catalog data. Go keeps autocommit and
/// isolation settings in SessionVars outside the transaction mem-buffer
/// (pkg/session/test/txn/txn_test.go:40-127 and
/// pkg/sessionctx/variable/session.go:2796-2858), so extracting lifecycle
/// ownership must not accidentally snapshot those session fields.
#[test]
fn rollback_preserves_nontransactional_transaction_settings() {
    let mut db = Database::new();
    assert_eq!(
        step(
            &mut db,
            "create table txn_setting_boundary (id int primary key)",
        ),
        "OK"
    );
    assert_eq!(
        step(&mut db, "set transaction isolation level repeatable read",),
        "OK"
    );
    assert_eq!(step(&mut db, "set autocommit = 0"), "OK");
    assert_eq!(
        step(
            &mut db,
            "set session transaction isolation level read committed",
        ),
        "OK"
    );
    assert_eq!(
        step(&mut db, "insert into txn_setting_boundary values (1)"),
        "OK"
    );
    assert_eq!(step(&mut db, "rollback"), "OK");

    assert_eq!(
        step(&mut db, "select id from txn_setting_boundary order by id"),
        "RS:"
    );
    assert_eq!(
        step(
            &mut db,
            "select @@autocommit, @@tx_isolation, @@tx_isolation_one_shot",
        ),
        "RS:0|READ-COMMITTED|REPEATABLE-READ"
    );
}
