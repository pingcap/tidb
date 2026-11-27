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

use super::*;

#[test]
fn session_activation_preserves_assertion_level_until_transaction_end() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(129)).unwrap();
    for (attempt, (mode, level, later, fails)) in [
        ("OPTIMISTIC", "STRICT", "OFF", true),
        ("PESSIMISTIC", "FAST", "OFF", true),
        ("OPTIMISTIC", "OFF", "STRICT", false),
    ]
    .into_iter()
    .enumerate()
    {
        rows(
            &mut session,
            &format!("SET tidb_txn_assertion_level = '{level}'"),
        );
        session
            .node_conn()
            .control_transaction(&format!("BEGIN {mode}"))
            .unwrap();
        // A later SET must not change this transaction's commit assertion level.
        rows(
            &mut session,
            &format!("SET tidb_txn_assertion_level = '{later}'"),
        );
        session
            .node()
            .buffer
            .stage_owned_batch([(
                tidb_txnkv::Key::from_bytes(format!("activation_assertion_{attempt}").into_bytes()),
                Some(b"value".to_vec()),
                false,
                tidb_txnkv::AssertionOp::AssertExist,
            )])
            .unwrap();
        let result = session.node_conn().control_transaction("COMMIT");
        if fails {
            assert_eq!(
                result
                    .expect_err("missing key must fail its Exist assertion")
                    .code,
                8141
            );
        } else {
            result.unwrap();
        }
    }
}

#[test]
fn session_activation_controls_the_native_commit_protocol() {
    if crate::domain::isolate_process_globals() {
        return;
    }
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(130)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.activation_protocol (id INT PRIMARY KEY, v INT)",
    );
    rows(
        &mut session,
        "INSERT INTO test.activation_protocol VALUES (1, 10)",
    );
    for (asynchronous, one_pc) in [(false, false), (true, false), (false, true), (true, true)] {
        for path in [
            "insert",
            "prefetched",
            "fallback",
            "OPTIMISTIC",
            "PESSIMISTIC",
        ] {
            rows(
                &mut session,
                &format!(
                    "SET tidb_enable_async_commit = {}, tidb_enable_1pc = {}",
                    u8::from(asynchronous),
                    u8::from(one_pc)
                ),
            );
            let before = tikv_client::metrics::get_txn_commit_counter();
            match path {
                "insert" => {
                    session
                        .node_conn()
                        .execute_write("INSERT INTO test.activation_protocol VALUES (2, 20)")
                        .unwrap();
                }
                "prefetched" => {
                    rows(
                        &mut session,
                        "UPDATE test.activation_protocol SET v=v+1 WHERE id=1",
                    );
                }
                "fallback" => {
                    session
                        .node_conn()
                        .execute_write("UPDATE test.activation_protocol SET v=v+1 WHERE id=1")
                        .unwrap();
                }
                mode => {
                    session
                        .node_conn()
                        .control_transaction(&format!("BEGIN {mode}"))
                        .unwrap();
                    session
                        .node_conn()
                        .execute_write("UPDATE test.activation_protocol SET v=v+1 WHERE id=1")
                        .unwrap();
                    // SET cannot change the options of the active transaction.
                    rows(
                        &mut session,
                        &format!(
                            "SET tidb_enable_async_commit = {}, tidb_enable_1pc = {}",
                            u8::from(!asynchronous),
                            u8::from(!one_pc)
                        ),
                    );
                    session.node_conn().control_transaction("COMMIT").unwrap();
                }
            }
            let delta = tikv_client::metrics::get_txn_commit_counter().subtract(before);
            let expected = if one_pc {
                (0, 0, 1)
            } else if asynchronous {
                (0, 1, 0)
            } else {
                (1, 0, 0)
            };
            assert_eq!(
                (delta.two_pc, delta.async_commit, delta.one_pc),
                expected,
                "{path}: async={asynchronous}, 1pc={one_pc}"
            );
            if path == "insert" {
                session
                    .node_conn()
                    .execute_write("DELETE FROM test.activation_protocol WHERE id=2")
                    .unwrap();
            }
        }
    }
}

/// Go `CreateForeignKey` / `onCreateForeignKey` / `DropForeignKey` on the
/// production node; every answer is the Go oracle's for the same script. A
/// refused `checkForeignKeyConstrain` rolls the job back but keeps the id
/// `allocateFKIndexID` took, so the next anonymous key is `fk_3`, not `fk_2`.
/// This node refused both statements as unsupported.
#[test]
fn an_added_foreign_key_checks_rows_and_keeps_gos_ids_on_rollback() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(137)).unwrap();
    rows(&mut session, "SET @@foreign_key_checks = 1");
    rows(
        &mut session,
        "CREATE TABLE test.parent (id INT KEY, c INT, INDEX (c))",
    );
    rows(&mut session, "CREATE TABLE test.child (id INT KEY, cc INT)");
    rows(
        &mut session,
        "INSERT INTO test.parent VALUES (1, 1), (2, 2)",
    );
    rows(&mut session, "INSERT INTO test.child VALUES (1, 1), (2, 3)");
    let add = "ALTER TABLE test.child ADD FOREIGN KEY fk_2 (cc) REFERENCES test.parent (c)";
    let violation = "Cannot add or update a child row: a foreign key constraint fails \
                     (`test`.`child`, CONSTRAINT `fk_2` FOREIGN KEY (`cc`) REFERENCES \
                     `parent` (`c`))";
    let error = session.node_conn().execute_write(add).unwrap_err();
    assert_eq!((error.code, error.message.as_str()), (1452, violation));
    rows(&mut session, "DELETE FROM test.child WHERE id = 2");
    session.node_conn().execute_write(add).unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.child"));
    assert!(
        created[0][1].contains(
            "  KEY `fk_2` (`cc`),\n  \
             CONSTRAINT `fk_2` FOREIGN KEY (`cc`) REFERENCES `parent` (`c`)\n"
        ),
        "{}",
        created[0][1]
    );
    let error = session
        .node_conn()
        .execute_write("INSERT INTO test.child VALUES (3, 9)")
        .unwrap_err();
    assert_eq!((error.code, error.message.as_str()), (1452, violation));
    let error = session.node_conn().execute_write(add).unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1826, "Duplicate foreign key constraint name 'fk_2'")
    );
    let drop = "ALTER TABLE test.child DROP FOREIGN KEY fk_2";
    session.node_conn().execute_write(drop).unwrap();
    let error = session.node_conn().execute_write(drop).unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1091, "Can't DROP 'fk_2'; check that column/key exists")
    );
    session
        .node_conn()
        .execute_write("ALTER TABLE test.child ADD FOREIGN KEY (cc) REFERENCES test.parent (c)")
        .unwrap();
    let created = displayed(rows(&mut session, "SHOW CREATE TABLE test.child"));
    assert!(
        created[0][1].contains(
            "  KEY `fk_2` (`cc`),\n  \
             CONSTRAINT `fk_3` FOREIGN KEY (`cc`) REFERENCES `parent` (`c`)\n"
        ),
        "{}",
        created[0][1]
    );
    rows(&mut session, "ADMIN CHECK TABLE test.child");
}

#[test]
fn shared_session_serves_ranges_joins_and_locking_reads_without_table_selection() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(134)).unwrap();
    for name in ["shared_left", "shared_right", "shared_third"] {
        rows(
            &mut session,
            &format!("CREATE TABLE test.{name} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)"),
        );
    }
    rows(
        &mut session,
        "INSERT INTO test.shared_left VALUES (-7, 10), (0, 20), (1, 30), (42, 40)",
    );
    rows(
        &mut session,
        "INSERT INTO test.shared_right VALUES (-7, 100), (1, 300), (42, 400)",
    );
    rows(
        &mut session,
        "INSERT INTO test.shared_third VALUES (1, 3000)",
    );
    for (sql, expected) in [
        ("SELECT id FROM test.shared_left WHERE id != 0 AND v >= 20 ORDER BY id", vec![vec!["1"], vec!["42"]]),
        ("SELECT id FROM test.shared_left WHERE id < -9223372036854775808", vec![]),
        ("SELECT l.id, r.v FROM test.shared_left l JOIN test.shared_right r ON l.id=r.id ORDER BY r.v DESC LIMIT 1, 1", vec![vec!["1", "300"]]),
        ("SELECT l.id, r.v, t.v FROM test.shared_left l JOIN test.shared_right r USING (id) JOIN test.shared_third t USING (id)", vec![vec!["1", "300", "3000"]]),
        ("SELECT id FROM test.shared_left WHERE id=1 FOR UPDATE", vec![vec!["1"]]),
    ] {
        assert_eq!(displayed(rows(&mut session, sql)), expected, "{sql}");
    }
    session.node_conn().control_transaction("BEGIN").unwrap();
    rows(&mut session, "UPDATE test.shared_left SET v=99 WHERE id=1");
    rows(&mut session, "INSERT INTO test.shared_left VALUES (2, 88)");
    rows(&mut session, "DELETE FROM test.shared_left WHERE id=0");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id, v FROM test.shared_left WHERE id BETWEEN 0 AND 2 ORDER BY id"
        )),
        [["1", "99"], ["2", "88"]],
    );
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id, v FROM test.shared_left WHERE id BETWEEN 0 AND 2 ORDER BY id"
        )),
        [["0", "20"], ["1", "30"]],
    );
}

#[test]
fn cluster_account_locking_policy_survives_unrelated_account_writes() {
    let (stack, users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(904)).unwrap();
    session.node_conn().execute_write("CREATE USER 'locking_probe'@'%' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 7 COMMENT 'keep'").unwrap();
    session
        .node_conn()
        .execute_write("ALTER USER 'locking_probe'@'%' PASSWORD EXPIRE NEVER")
        .unwrap();
    let attributes = displayed(rows(
        &mut session,
        "SELECT User_attributes FROM mysql.user WHERE User='locking_probe'",
    ));
    let value: serde_json::Value = serde_json::from_str(&attributes[0][0]).unwrap();
    assert_eq!(value["Password_locking"]["failed_login_attempts"], 3);
    assert_eq!(value["Password_locking"]["password_lock_time_days"], 7);
    assert_eq!(value["metadata"]["comment"], "keep");
    session.node_conn().execute_write(r#"UPDATE mysql.user SET User_attributes='{"Password_locking":{"failed_login_attempts":3,"password_lock_time_days":7,"failed_login_count":2,"auto_account_locked":"Y","auto_locked_last_changed":"Sat Feb  3 04:05:06 UTC 2001"},"metadata":{"comment":"keep"},"additional_password":"preserve"}' WHERE User='locking_probe'"#).unwrap();
    session
        .node_conn()
        .execute_write("ALTER USER 'locking_probe'@'%' PASSWORD EXPIRE DEFAULT")
        .unwrap();
    let stored = users
        .accounts()
        .password_locking("locking_probe", "%")
        .unwrap();
    assert_eq!(stored.failed_login_count, 2);
    assert!(stored.auto_account_locked);
    assert_eq!(stored.auto_locked_last_changed, 981_173_106);
    session
        .node_conn()
        .execute_write("ALTER USER 'locking_probe'@'%' ACCOUNT UNLOCK")
        .unwrap();
    let reset = users
        .accounts()
        .password_locking("locking_probe", "%")
        .unwrap();
    assert_eq!(reset.failed_login_count, 0);
    assert!(!reset.auto_account_locked);
    assert!(reset.auto_locked_last_changed > stored.auto_locked_last_changed);
    session
        .node_conn()
        .execute_write(
            "ALTER USER 'locking_probe'@'%' FAILED_LOGIN_ATTEMPTS 0 PASSWORD_LOCK_TIME 0",
        )
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(
        &displayed(rows(
            &mut session,
            "SELECT User_attributes FROM mysql.user WHERE User='locking_probe'",
        ))[0][0],
    )
    .unwrap();
    assert!(value.get("Password_locking").is_none());
    assert_eq!(value["metadata"]["comment"], "keep");
    assert_eq!(value["additional_password"], "preserve");
}

#[test]
fn cluster_account_drop_persists_through_its_own_transaction() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(901)).unwrap();
    session
        .node_conn()
        .execute_write("CREATE USER 'drop_probe'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    session
        .node_conn()
        .execute_write("GRANT SELECT ON test.* TO 'drop_probe'@'%'")
        .unwrap();
    session
        .node_conn()
        .execute_write("DROP USER 'drop_probe'@'%'")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM mysql.user WHERE User='drop_probe'"
        )),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM mysql.db WHERE User='drop_probe'"
        )),
        [["0"]]
    );
    assert_eq!(
        session
            .node_conn()
            .execute_write("DROP USER 'drop_probe'@'%'")
            .unwrap_err()
            .code,
        1396
    );
    session
        .node_conn()
        .execute_write("CREATE USER 'drop_probe'@'%' IDENTIFIED BY 'newpw'")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT COUNT(*) FROM mysql.user WHERE User='drop_probe'"
        )),
        [["1"]]
    );
}

#[test]
fn a_held_record_lock_does_not_suppress_a_new_duplicate_assertion() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(125)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.held_assertion (id INT PRIMARY KEY, v INT)",
    );
    rows(
        &mut session,
        "INSERT INTO test.held_assertion VALUES (1,10),(2,20)",
    );
    for select in [
        "SELECT * FROM test.held_assertion WHERE id=1 FOR UPDATE",
        "SELECT * FROM test.held_assertion WHERE v=10 FOR UPDATE",
    ] {
        session
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .unwrap();
        assert_eq!(displayed(rows(&mut session, select)), [["1", "10"]]);
        let error = session
            .node_conn()
            .execute_write("INSERT INTO test.held_assertion VALUES (1,99)")
            .err();
        session.node_conn().control_transaction("ROLLBACK").unwrap();
        assert!(matches!(error, Some(ref error) if error.code == 1062),
            "an already-owned existing key must still reject the INSERT at statement end: {error:?}");
    }
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    assert!(rows(
        &mut session,
        "SELECT * FROM test.held_assertion WHERE id=3 FOR UPDATE"
    )
    .is_empty());
    rows(
        &mut session,
        "INSERT INTO test.held_assertion VALUES (3,30)",
    );
    rows(
        &mut session,
        "UPDATE test.held_assertion SET v=31 WHERE id=3",
    );
    session.node_conn().control_transaction("COMMIT").unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT * FROM test.held_assertion ORDER BY id"
        )),
        [["1", "10"], ["2", "20"], ["3", "31"]]
    );
}

/// Go `PessimisticSerializableTxnContextProvider` (`newProviderWithRequest`'s
/// SERIALIZABLE arm, reachable under `tidb_skip_isolation_level_check`):
/// every statement reads and locks at the start timestamp, and a lock that
/// meets a newer commit returns its write conflict instead of retrying. Go
/// master capture: `tidb-wt/tools/x05/go_serializable.jsonl`.
#[test]
fn a_serializable_pessimistic_transaction_reads_and_locks_at_its_start_timestamp() {
    let (stack, _users) = cop_backed_stack();
    let mut reader = stack.factory.open_session(session_context(127)).expect("reader");
    let mut writer = stack.factory.open_session(session_context(128)).expect("writer");
    for sql in [
        "CREATE TABLE test.ser (id INT PRIMARY KEY, v INT)",
        "INSERT INTO test.ser VALUES (1, 1), (2, 1)",
        "SET SESSION tidb_skip_isolation_level_check = 1",
        "SET SESSION transaction_isolation = 'SERIALIZABLE'",
    ] {
        reader.node_conn().execute_write(sql).expect("setup");
    }
    let conflict = |result: Result<(), tidb_store_driver_error::sql_query_error::SqlQueryError>| {
        let error = result.err().expect("the lock meets a newer commit");
        assert_eq!(error.code, 9007, "{}", error.message);
        assert!(error.message.starts_with("Write conflict, txnStartTS="), "{}", error.message);
    };
    let read = "SELECT v FROM test.ser WHERE id = 1";
    reader.node_conn().control_transaction("BEGIN PESSIMISTIC").expect("begin");
    assert_eq!(displayed(rows(&mut reader, read)), vec![vec!["1"]]);
    writer.node_conn().execute_write("UPDATE test.ser SET v = 2 WHERE id = 1").expect("peer");
    assert_eq!(displayed(rows(&mut reader, read)), vec![vec!["1"]]);
    conflict(reader.node_conn().execute("SELECT v FROM test.ser WHERE id = 1 FOR UPDATE").and_then(|mut result| {
        crate::domain::resultset_source::ResultSetSource::next_batch(result.source(), 8)
            .map(|_| ())
            .map_err(|error| tidb_store_driver_error::sql_query_error::SqlQueryError::new(error.code, *b"HY000", &error.message))
    }));
    conflict(reader.node_conn().execute_write("UPDATE test.ser SET v = v + 10 WHERE id = 1").map(|_| ()));
    assert_eq!(displayed(rows(&mut reader, read)), vec![vec!["1"]]);
    reader.node_conn().control_transaction("COMMIT").expect("commit");
    assert_eq!(displayed(rows(&mut reader, read)), vec![vec!["2"]]);

    let read = "SELECT v FROM test.ser WHERE id = 2";
    reader.node_conn().control_transaction("BEGIN PESSIMISTIC").expect("begin");
    assert_eq!(displayed(rows(&mut reader, read)), vec![vec!["1"]]);
    writer.node_conn().execute_write("UPDATE test.ser SET v = 5 WHERE id = 2").expect("peer");
    conflict(reader.node_conn().execute_write("UPDATE test.ser SET v = v + 10 WHERE id = 2").map(|_| ()));
    assert_eq!(displayed(rows(&mut reader, read)), vec![vec!["1"]]);
    reader.node_conn().control_transaction("COMMIT").expect("commit");
    assert_eq!(displayed(rows(&mut reader, read)), vec![vec!["5"]]);
}

/// Go `ExecStmt` under `tidb_low_resolution_tso` (`UseLowResolutionTSO`, a
/// user connection): a write statement and a SELECT that takes its lock
/// (`isSelectForUpdate`: inside a transaction) are refused with 1105; an
/// autocommit `FOR UPDATE` takes no lock and reads.
#[test]
fn low_resolution_tso_refuses_writes_and_locking_reads() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(126))
        .expect("session");
    for sql in [
        "CREATE TABLE test.lowres (a INT)",
        "SET tidb_low_resolution_tso = ON",
    ] {
        session.node_conn().execute_write(sql).expect("setup");
    }
    let refused = |session: &mut Session, sql: &str| {
        let error = session
            .node_conn()
            .execute_write(sql)
            .expect_err("refused");
        (error.code, error.message)
    };
    assert_eq!(
        refused(&mut session, "INSERT INTO test.lowres VALUES (1)"),
        (
            1105,
            "can not execute write statement when 'tidb_low_resolution_tso' is set".to_owned()
        )
    );
    assert_eq!(
        refused(&mut session, "CREATE TABLE test.lowres2 (a INT)").1,
        "can not execute write statement when 'tidb_low_resolution_tso' is set"
    );
    rows(&mut session, "SELECT * FROM test.lowres FOR UPDATE");
    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    let error = session
        .node_conn()
        .execute("SELECT * FROM test.lowres FOR UPDATE")
        .err()
        .expect("a locking read in a transaction is refused");
    assert_eq!(
        (error.code, error.message.as_str()),
        (
            1105,
            "can not execute select for update statement when 'tidb_low_resolution_tso' is set"
        )
    );
    rows(&mut session, "SELECT * FROM test.lowres");
    session
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback");
    // Go `newOracleFuture`: the transaction starts at the oracle's cached
    // low-resolution timestamp, so two transactions begun back to back share
    // it; an ordinary TSO never repeats. The cache is the oracle's last
    // timestamp, which every TSO fetch also advances (client-go `setLastTS`),
    // so a background fetch may land between one pair -- not between all.
    let start_ts = |session: &mut Session| {
        session.node_conn().control_transaction("BEGIN").expect("begin");
        rows(session, "SELECT * FROM test.lowres");
        let ts = displayed(rows(session, "SELECT @@tidb_current_ts"))[0][0].clone();
        session.node_conn().control_transaction("ROLLBACK").expect("rollback");
        ts
    };
    let pairs = |session: &mut Session| -> Vec<bool> {
        (0..5).map(|_| start_ts(session) == start_ts(session)).collect()
    };
    let low_resolution = pairs(&mut session);
    assert!(low_resolution.contains(&true), "{low_resolution:?}");
    session
        .node_conn()
        .execute_write("SET tidb_low_resolution_tso = OFF")
        .expect("off");
    let ordinary = pairs(&mut session);
    assert!(!ordinary.contains(&true), "{ordinary:?}");
    session
        .node_conn()
        .execute_write("INSERT INTO test.lowres VALUES (1)")
        .expect("a write runs again");
}

/// Go `TestLockUnchangedUniqueKeys` (`UniqueAndRowUnchanged`,
/// `UniqueAndRowChanged`): with `tidb_lock_unchanged_keys` ON a pessimistic
/// UPDATE (reading through `sk`, as Go's test forces, so no point get on
/// `uk` locks it) that leaves a unique key as it was locks that key
/// (`addUnchangedKeysForLockByRow`), so another transaction's INSERT of the
/// same key waits; OFF, it finds the duplicate at once.
#[test]
fn lock_unchanged_keys_locks_the_unique_keys_an_update_kept() {
    let (stack, _users) = cop_backed_stack();
    let mut writer = stack
        .factory
        .open_session(session_context(124))
        .expect("writer");
    let mut other = stack
        .factory
        .open_session(session_context(125))
        .expect("other");
    other
        .node_conn()
        .execute_write("SET innodb_lock_wait_timeout=1")
        .expect("bounded lock wait");
    for lock in [true, false] {
        for (case, update) in [
            ("unchanged", "UPDATE test.uk_lock FORCE INDEX(sk) SET v = 10 WHERE k = 1"),
            ("changed", "UPDATE test.uk_lock FORCE INDEX(sk) SET v = 11 WHERE k = 1"),
        ] {
            for sql in [
                format!("SET tidb_lock_unchanged_keys = {}", if lock { "ON" } else { "OFF" }),
                "DROP TABLE IF EXISTS test.uk_lock".to_owned(),
                "CREATE TABLE test.uk_lock (k INT, v INT, UNIQUE KEY uk(k), KEY sk(k))".to_owned(),
                "INSERT INTO test.uk_lock VALUES (1, 10)".to_owned(),
            ] {
                writer.node_conn().execute_write(&sql).expect("setup");
            }
            writer
                .node_conn()
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin writer");
            writer.node_conn().execute_write(update).expect("update");
            other
                .node_conn()
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin other");
            let code = other
                .node_conn()
                .execute_write("INSERT INTO test.uk_lock VALUES (1, 10)")
                .expect_err("the key exists")
                .code;
            let expected = if lock { 1205 } else { 1062 };
            assert_eq!(code, expected, "{case}, tidb_lock_unchanged_keys = {lock}");
            other
                .node_conn()
                .control_transaction("ROLLBACK")
                .expect("rollback other");
            writer
                .node_conn()
                .control_transaction("ROLLBACK")
                .expect("rollback writer");
        }
    }
}

#[test]
fn unchanged_updates_lock_only_matched_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut writer = stack
        .factory
        .open_session(session_context(122))
        .expect("writer");
    let mut reader = stack
        .factory
        .open_session(session_context(123))
        .expect("reader");
    rows(
        &mut writer,
        "CREATE TABLE test.unchanged_lock (id INT PRIMARY KEY, v INT)",
    );
    rows(
        &mut writer,
        "INSERT INTO test.unchanged_lock VALUES (1,20),(2,30)",
    );
    writer
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin writer");
    let write = writer
        .node_conn()
        .execute_write("UPDATE test.unchanged_lock SET v=v WHERE v=20")
        .expect("unchanged update")
        .expect("write result");
    assert_eq!(write.affected_rows, 0);
    reader
        .node_conn()
        .execute_write("SET innodb_lock_wait_timeout=1")
        .expect("bounded lock wait");
    reader
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin reader");
    // Go TestSelectForUpdateNoWait and TestInnodbLockWaitTimeout: range
    // locking must preserve NOWAIT, WAIT n and the session timeout.
    for (clause, code) in [("NOWAIT", 3572), ("WAIT 1", 1205), ("", 1205)] {
        let sql = format!("SELECT v FROM test.unchanged_lock WHERE v=20 FOR UPDATE {clause}");
        let result = reader.node_conn().execute(&sql);
        assert!(
            matches!(result, Err(ref error) if error.code == code),
            "locking range {clause} must report {code}; got {:?}",
            result.as_ref().err()
        );
    }
    let result = reader
        .node_conn()
        .execute_write("UPDATE test.unchanged_lock SET v=v+1 WHERE id=1");
    assert!(
        matches!(result, Err(ref error) if error.code == 1205),
        "the unchanged matched row must remain locked; error={:?}",
        result.as_ref().err()
    );
    let untouched = reader
        .node_conn()
        .execute_write("UPDATE test.unchanged_lock SET v=v+1 WHERE id=2")
        .expect("a rejected row is not locked")
        .expect("write result");
    assert_eq!(untouched.affected_rows, 1);
    writer
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("release writer");
    reader
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("release reader");
}

#[test]
fn global_binding_commands_commit_outside_the_user_transaction() {
    let (stack, _users) = cop_backed_stack();
    let mut writer = stack
        .factory
        .open_session(session_context(120))
        .expect("writer");
    let mut reader = stack
        .factory
        .open_session(session_context(121))
        .expect("reader");
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT original_sql, source FROM mysql.bind_info WHERE status='builtin'"
        )),
        [["builtin_pseudo_sql_for_bind_lock", "builtin"]],
        "bootstrap supplies the shared binding-writer lock row"
    );
    rows(
        &mut writer,
        "CREATE TABLE test.binding_command_visibility (id BIGINT PRIMARY KEY, v INT)",
    );
    rows(
        &mut writer,
        "INSERT INTO test.binding_command_visibility VALUES (1,20)",
    );
    writer
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin");
    writer
        .node_conn()
        .execute_write("UPDATE test.binding_command_visibility SET v=21 WHERE id=1")
        .expect("staged write");
    rows(&mut writer, "CREATE GLOBAL BINDING FOR SELECT v FROM test.binding_command_visibility WHERE v=20 USING SELECT v FROM test.binding_command_visibility USE INDEX() WHERE v=20");
    assert_eq!(
        displayed(rows(
            &mut writer,
            "SELECT v FROM test.binding_command_visibility WHERE id=1"
        )),
        [["21"]]
    );
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT v FROM test.binding_command_visibility WHERE id=1"
        )),
        [["20"]]
    );
    let count = "SELECT COUNT(*) FROM mysql.bind_info WHERE status='enabled'";
    assert_eq!(
        displayed(rows(&mut reader, count)),
        [["1"]],
        "binding committed while caller transaction remains open"
    );
    writer
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback user data");
    assert_eq!(
        displayed(rows(&mut reader, count)),
        [["1"]],
        "binding survives caller rollback"
    );
    assert_eq!(
        displayed(rows(
            &mut writer,
            "SELECT v FROM test.binding_command_visibility WHERE id=1"
        )),
        [["20"]]
    );
    for (command, expected) in [
        (
            "SET BINDING DISABLED FOR SELECT v FROM test.binding_command_visibility WHERE v=20",
            "disabled",
        ),
        (
            "SET BINDING ENABLED FOR SELECT v FROM test.binding_command_visibility WHERE v=20",
            "enabled",
        ),
        (
            "DROP GLOBAL BINDING FOR SELECT v FROM test.binding_command_visibility WHERE v=20",
            "deleted",
        ),
    ] {
        writer
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin user transaction");
        writer
            .node_conn()
            .execute_write("UPDATE test.binding_command_visibility SET v=21 WHERE id=1")
            .expect("staged user write");
        rows(&mut writer, command);
        let status = "SELECT status FROM mysql.bind_info WHERE status!='builtin'";
        assert_eq!(
            displayed(rows(&mut reader, status)),
            [[expected]],
            "binding command commits independently: {command}"
        );
        assert_eq!(
            displayed(rows(
                &mut writer,
                "SELECT v FROM test.binding_command_visibility WHERE id=1"
            )),
            [["21"]]
        );
        assert_eq!(
            displayed(rows(
                &mut reader,
                "SELECT v FROM test.binding_command_visibility WHERE id=1"
            )),
            [["20"]]
        );
        writer
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback user transaction");
        assert_eq!(
            displayed(rows(&mut reader, status)),
            [[expected]],
            "binding command survives caller rollback: {command}"
        );
    }
}

/// Go's notifier begins its transaction under
/// `kv.WithInternalSourceType(ctx, kv.InternalDDLNotifier)` and activates it
/// on its first statement, so its commit is labelled as its reads are. A
/// pooled session whose last work had another source used to start the
/// transaction with that one (a fresh session prewrote as `unknown`).
#[test]
fn an_internal_transaction_takes_its_callers_request_source() {
    let (stack, _users) =
        cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    stop_domain_ddl_notifier(&stack.factory);
    let mut session = stack
        .factory
        .open_session(session_context(87))
        .expect("session opens");
    rows(&mut session, "USE test");
    rows(&mut session, "CREATE TABLE internal_request_source (a INT)");
    let sent: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let recorded = Arc::clone(&sent);
    stack.rpc.set_send_hook(Some(Arc::new(move |request| {
        if let Some(request) =
            request.as_any().downcast_ref::<tikv_client::proto::kvrpcpb::PrewriteRequest>()
        {
            recorded.lock().unwrap().push(
                request
                    .context
                    .as_ref()
                    .map(|context| context.request_source.clone())
                    .unwrap_or_default(),
            );
        }
    })));
    let pool = stack.factory.advanced_sys_session_pool();
    tidb_stats_handle_util::call_with_sctx(
        pool.as_ref(),
        |sctx| {
            let run = |source: &str, sql: &str| {
                tidb_domain_core::sys_session::exec_sql(sctx, source, sql, &[])
                    .map(|_| ())
                    .map_err(tidb_domain_core::sys_session::stats_session_error)
            };
            // The session's previous work ran under another source.
            run(tidb_txnkv::INTERNAL_TXN_TTL, "SELECT 1")?;
            sctx.new_txn(tidb_txnkv::INTERNAL_DDL_NOTIFIER, true)
                .map_err(tidb_domain_core::sys_session::stats_session_error)?;
            run(
                tidb_txnkv::INTERNAL_DDL_NOTIFIER,
                "INSERT INTO test.internal_request_source VALUES (1)",
            )?;
            sctx.commit_txn().map_err(tidb_domain_core::sys_session::stats_session_error)
        },
        &[],
    )
    .expect("the internal transaction commits");
    stack.rpc.set_send_hook(None);
    let sent = sent.lock().unwrap().clone();
    assert!(sent.iter().any(|source| source == "internal_DDLNotifier"), "{sent:?}");
    assert!(
        !sent.iter().any(|source| source == "internal_TTL" || source == "unknown"),
        "{sent:?}"
    );
}

/// Go `newLockCtx` asks for existence metadata when assertions are enabled,
/// including fair locks that do not return the record value.
#[test]
fn session_activation_checks_existence_for_pessimistic_locks() {
    use tidb_txnkv::transaction::BufferMutation;

    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(904)).unwrap();
    for (id, (level, fair)) in [
        ("FAST", "ON"),
        ("STRICT", "ON"),
        ("OFF", "ON"),
        ("FAST", "OFF"),
        ("STRICT", "OFF"),
        ("OFF", "OFF"),
    ]
    .into_iter()
    .enumerate()
    {
        rows(
            &mut session,
            &format!("SET tidb_txn_assertion_level = '{level}'"),
        );
        rows(
            &mut session,
            &format!("SET tidb_pessimistic_txn_fair_locking = {fair}"),
        );
        let key = format!("assert-existence/{id}").into_bytes();
        for existing in [false, true] {
            session
                .node_conn()
                .control_transaction("BEGIN PESSIMISTIC")
                .unwrap();
            // Restricted SQL locks its planned keys without loading values,
            // then stages them. Both absent and existing records must survive
            // the native client's assertion check from the lock metadata.
            super::super::super::transactions::stage_pessimistic_statement(
                session.node().txn.explicit().as_deref().unwrap(),
                &session.node().buffer,
                session.lock_wait_timeout(),
                |_, _| {
                    let mutation = if existing {
                        BufferMutation::put_existing(key.clone(), b"updated".to_vec())
                    } else {
                        BufferMutation::set_with_flags(
                            key.clone(),
                            b"inserted".to_vec(),
                            true,
                            tidb_txnkv::AssertionOp::AssertNotExist,
                        )
                    }
                    .unwrap();
                    Ok(((), vec![mutation]))
                },
            )
            .unwrap();
            session.node_conn().control_transaction("COMMIT").unwrap();
        }
    }
}

/// A `DATETIME(n)`/`TIMESTAMP(n) DEFAULT CURRENT_TIMESTAMP(n)` column must
/// survive the round trip through this node's own catalog loader.
///
/// Go stores the marker WORD alone and re-derives the fsp from the column's
/// decimal wherever the default is printed. The loader used to rebuild the
/// written spelling as a bare word and then apply Go's admission-time
/// "written fsp must equal the column's" check to it, which no bare word can
/// satisfy for a column with an fsp. It therefore REFUSED a table its own
/// DDL had just published -- the worst shape a DDL can take, since CREATE
/// then reports 1050 while every read reports 1146.
#[test]
fn a_fractional_clock_default_survives_the_catalog_loader() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(23))
        .expect("session opens");

    rows(
        &mut session,
        "CREATE TABLE test.dt (o datetime(3) DEFAULT CURRENT_TIMESTAMP(3), v int)",
    );
    rows(
        &mut session,
        "CREATE TABLE test.ts (o timestamp(6) DEFAULT CURRENT_TIMESTAMP(6))",
    );

    // The table is READABLE, which is what the refusal used to break.
    let shown = displayed(rows(&mut session, "SHOW CREATE TABLE test.dt"));
    assert!(
        shown[0][1].contains("`o` datetime(3) DEFAULT CURRENT_TIMESTAMP(3)"),
        "{}",
        shown[0][1]
    );
    let shown = displayed(rows(&mut session, "SHOW CREATE TABLE test.ts"));
    assert!(
        shown[0][1].contains("`o` timestamp(6) DEFAULT CURRENT_TIMESTAMP(6)"),
        "{}",
        shown[0][1]
    );

    // And the marker still evaluates per row rather than storing the word.
    rows(&mut session, "INSERT INTO test.dt (v) VALUES (1)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT v, o IS NOT NULL FROM test.dt")),
        [["1", "1"]]
    );
}

#[test]
fn completed_empty_transaction_does_not_lose_the_next_autocommit_insert() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(66)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.txn_lifetime (id int primary key, v int)",
    );
    rows(&mut session, "INSERT INTO test.txn_lifetime VALUES (1, 10)");
    session.node_conn().control_transaction("BEGIN").unwrap();
    rows(&mut session, "SELECT v FROM test.txn_lifetime WHERE id = 1");
    session.node_conn().control_transaction("COMMIT").unwrap();
    rows(&mut session, "INSERT INTO test.txn_lifetime VALUES (2, 20)");

    rows(
        &mut session,
        "UPDATE test.txn_lifetime SET v = 0 WHERE id = 99",
    );
    rows(&mut session, "INSERT INTO test.txn_lifetime VALUES (3, 30)");
    for (end, id) in [("COMMIT", 4), ("ROLLBACK", 5)] {
        session
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .unwrap();
        rows(
            &mut session,
            "SELECT v FROM test.txn_lifetime WHERE id = 1 FOR UPDATE",
        );
        session.node_conn().control_transaction(end).unwrap();
        rows(
            &mut session,
            &format!("INSERT INTO test.txn_lifetime VALUES ({id}, {})", id * 10),
        );
    }
    session
        .node_conn()
        .execute_write("SET autocommit = 0")
        .unwrap();
    rows(&mut session, "SELECT v FROM test.txn_lifetime WHERE id = 1");
    session
        .node_conn()
        .execute_write("SET autocommit = 1")
        .unwrap();
    rows(&mut session, "INSERT INTO test.txn_lifetime VALUES (6, 60)");

    // Another connection proves the INSERT was persisted, not merely staged.
    let mut reader = stack.factory.open_session(session_context(67)).unwrap();
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT id, v FROM test.txn_lifetime ORDER BY id"
        )),
        [
            ["1", "10"],
            ["2", "20"],
            ["3", "30"],
            ["4", "40"],
            ["5", "50"],
            ["6", "60"]
        ]
    );
}

#[test]
fn failed_autocommit_update_does_not_lose_the_next_insert() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(68)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.failed_txn_lifetime (id int primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.failed_txn_lifetime VALUES (1, 10), (2, 20)",
    );
    let error = match session
        .node_conn()
        .execute("UPDATE test.failed_txn_lifetime SET id = 2 WHERE id = 1")
    {
        Err(error) => error,
        Ok(_) => panic!("the conflicting UPDATE must fail"),
    };
    assert_eq!(error.code, 1062);
    rows(
        &mut session,
        "INSERT INTO test.failed_txn_lifetime VALUES (3, 30)",
    );

    let mut reader = stack.factory.open_session(session_context(69)).unwrap();
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT id, v FROM test.failed_txn_lifetime ORDER BY id"
        )),
        [["1", "10"], ["2", "20"], ["3", "30"]]
    );
}

#[test]
fn failed_explicit_statement_retains_native_transaction_and_savepoints() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(670)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.statement_lifetime (id int primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.statement_lifetime VALUES (1, 10), (2, 20)",
    );
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    rows(
        &mut session,
        "UPDATE test.statement_lifetime SET v = 11 WHERE id = 1",
    );
    session
        .node_conn()
        .control_transaction("SAVEPOINT keep_first")
        .unwrap();
    rows(
        &mut session,
        "INSERT INTO test.statement_lifetime VALUES (3, 30)",
    );
    let error = match session
        .node_conn()
        .execute("UPDATE test.statement_lifetime SET id = 2 WHERE id = 1")
    {
        Err(error) => error,
        Ok(_) => panic!("the conflicting UPDATE must fail"),
    };
    assert_eq!(error.code, 1062);
    assert!(session.node().buffer.native_owner().is_some());
    session
        .node_conn()
        .control_transaction("ROLLBACK TO keep_first")
        .unwrap();
    session.node_conn().control_transaction("COMMIT").unwrap();
    rows(
        &mut session,
        "INSERT INTO test.statement_lifetime VALUES (4, 40)",
    );

    let mut reader = stack.factory.open_session(session_context(671)).unwrap();
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT id, v FROM test.statement_lifetime ORDER BY id"
        )),
        [["1", "11"], ["2", "20"], ["4", "40"]]
    );
}

/// Two explicit transactions racing an `UPDATE` of the same row both commit,
/// and the row carries BOTH increments -- Go's default `tidb_txn_mode =
/// 'pessimistic'` semantics, sysbench `oltp_read_write`'s exact shape.
///
/// The pessimistic wiring is what each half proves. The loser's `UPDATE`
/// blocks on the winner's row lock instead of proceeding on a stale
/// snapshot; when the lock releases, fair locking grants it WITH a conflict
/// and the statement is re-executed reading at the advanced `for_update_ts`
/// (Go `handlePessimisticDML` -> `UpdateForUpdateTS`), so its `v + 1`
/// computes from the winner's committed value. Before the wiring, `BEGIN`
/// held an optimistic transaction end to end: neither `UPDATE` blocked, both
/// computed from the same snapshot, and whichever `COMMIT` ran second failed
/// with 9007 -- this test then fails on the error, and would fail on
/// `v == 2` even if both were let through.
#[test]
fn racing_pessimistic_updates_both_commit_with_serial_effect() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(80))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.race_rw (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.race_rw VALUES (1, 0)");

    assert_eq!(
        first
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
        Some(true)
    );
    rows(&mut first, "UPDATE test.race_rw SET v = v + 1 WHERE id = 1");

    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(81))
                .expect("session opens");
            assert_eq!(
                second
                    .node_conn()
                    .control_transaction("BEGIN")
                    .expect("begin"),
                Some(true)
            );
            // Blocks on the first transaction's pessimistic row lock until
            // that transaction commits.
            rows(
                &mut second,
                "UPDATE test.race_rw SET v = v + 1 WHERE id = 1",
            );
            second
                .node_conn()
                .control_transaction("COMMIT")
                .expect("commit");
        });
        // Give the contender time to reach the lock wait, so the interesting
        // interleaving -- blocked UPDATE, then the winner's COMMIT -- is the
        // one exercised. The assertions hold under any interleaving.
        std::thread::sleep(std::time::Duration::from_millis(200));
        first
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after waiting the lock out");

    assert_eq!(
        displayed(rows(&mut first, "SELECT v FROM test.race_rw WHERE id = 1")),
        [["2"]],
        "both increments landed: the loser re-read the winner's commit"
    );
}

/// Go `handlePessimisticLockError` re-executes the statement under its one
/// `StmtCtx` (`ResetForRetry`); `ResetContextOfStmt` ran once, before the
/// first attempt, so what it promoted from the PRECEDING statement
/// (`PrevFoundInPlanCache`, read by `@@last_plan_from_cache`) is still there
/// for the replay. Go's `v + @@last_plan_from_cache` adds 1 on both attempts.
#[test]
fn a_lock_retry_keeps_the_statement_context_like_go() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(86))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.retry_ctx (k int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.retry_ctx VALUES (7, 10)");
    assert_eq!(
        first
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
        Some(true)
    );
    rows(&mut first, "UPDATE test.retry_ctx SET v = v + 1 WHERE v > 0");

    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(87))
                .expect("session opens");
            rows(
                &mut second,
                "PREPARE s FROM 'SELECT v FROM test.retry_ctx WHERE v > ?'",
            );
            rows(&mut second, "SET @a = 0");
            assert_eq!(
                second
                    .node_conn()
                    .control_transaction("BEGIN")
                    .expect("begin"),
                Some(true)
            );
            rows(&mut second, "EXECUTE s USING @a");
            // The statement before the UPDATE took its plan from the cache.
            rows(&mut second, "EXECUTE s USING @a");
            rows(
                &mut second,
                "UPDATE test.retry_ctx SET v = v + @@last_plan_from_cache WHERE v > 0",
            );
            second
                .node_conn()
                .control_transaction("COMMIT")
                .expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        first
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after waiting the lock out");

    assert_eq!(
        displayed(rows(&mut first, "SELECT v FROM test.retry_ctx WHERE k = 7")),
        [["12"]],
    );
}

/// A NON-locking read must not be answered from the pessimistic lock cache.
///
/// Go gates that cache on `e.lock`: `PointGetExecutor.get`
/// (`pkg/executor/point_get.go:671-684`) consults
/// `TxnCtx.GetKeyInPessimisticLockCache` only inside `if e.lock`, so a plain
/// `SELECT` falls through to the snapshot and reads at the transaction's own
/// `start_ts`. The cached row is the one the LOCK saw, at a `for_update_ts`
/// at or after `start_ts`; answering a plain read from it publishes a newer
/// row into a repeatable read.
///
/// The window is exactly the one pessimistic locking is built to tolerate: a
/// writer that commits after `BEGIN`, which the lock survives by advancing
/// its own `for_update_ts` rather than failing.
#[test]
fn a_plain_read_is_not_answered_from_the_pessimistic_lock_cache() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut reader = factory
        .open_session(session_context(84))
        .expect("session opens");
    rows(
        &mut reader,
        "CREATE TABLE test.lock_cache (id int primary key, v int)",
    );
    rows(&mut reader, "INSERT INTO test.lock_cache VALUES (1, 10)");

    assert_eq!(
        reader
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
        Some(true)
    );
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT v FROM test.lock_cache WHERE id = 1"
        )),
        [["10"]],
        "the transaction's snapshot is the row as of BEGIN"
    );

    let mut writer = factory
        .open_session(session_context(85))
        .expect("session opens");
    rows(
        &mut writer,
        "UPDATE test.lock_cache SET v = 99 WHERE id = 1",
    );

    // The LOCKING read may see the newer row -- it takes its own
    // `for_update_ts`, which is Go's behaviour too. This is what fills the
    // lock cache.
    let _ = reader
        .node_conn()
        .execute("SELECT v FROM test.lock_cache WHERE id = 1 FOR UPDATE");

    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT v FROM test.lock_cache WHERE id = 1"
        )),
        [["10"]],
        "the plain read that follows still reads at start_ts: the lock cache \
         belongs to locking reads only (`point_get.go:677`)"
    );
    reader
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback");
}

/// `BEGIN OPTIMISTIC` keeps Go's optimistic contract: neither `UPDATE`
/// blocks or locks, and the transaction that commits second fails at
/// `COMMIT` with the 9007 write conflict -- which is also the receipt that
/// [`racing_pessimistic_updates_both_commit_with_serial_effect`] tests the
/// WIRING and not some property both modes share: this test IS the
/// pre-wiring behavior, kept reachable by the keyword exactly as Go keeps
/// it.
#[test]
fn racing_optimistic_updates_still_conflict_at_commit() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(84))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.race_opt (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.race_opt VALUES (1, 0)");

    first
        .node_conn()
        .control_transaction("BEGIN OPTIMISTIC")
        .expect("begin");
    rows(
        &mut first,
        "UPDATE test.race_opt SET v = v + 1 WHERE id = 1",
    );

    // The contender's whole transaction runs and commits while the first is
    // still open: optimistically nothing blocks it.
    let mut second = factory
        .open_session(session_context(85))
        .expect("session opens");
    second
        .node_conn()
        .control_transaction("BEGIN OPTIMISTIC")
        .expect("begin");
    rows(
        &mut second,
        "UPDATE test.race_opt SET v = v + 1 WHERE id = 1",
    );
    second
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");

    // The first transaction's prewrite now finds the newer commit: 9007.
    let refused = first
        .node_conn()
        .control_transaction("COMMIT")
        .expect_err("an optimistic loser reports the conflict at COMMIT");
    assert_eq!(
        refused.code, 9007,
        "the loser's error keeps Go's write-conflict identity: {}",
        refused.message
    );
    assert_eq!(
        displayed(rows(
            &mut second,
            "SELECT v FROM test.race_opt WHERE id = 1"
        )),
        [["1"]],
        "only the winner's increment landed"
    );
}

/// `BEGIN` inside an open transaction implicitly COMMITS it -- Go's
/// documented `BEGIN` semantics -- and the staged writes are PUBLISHED, not
/// discarded. sysbench relies on this shape: an ignorable statement error
/// (1213) makes it abandon the transaction and simply issue the next
/// `BEGIN`. Before the fix the wrapper discarded the abandoned buffer, and
/// -- when a statistics republish had refreshed the catalog first -- the
/// implicit commit failed with a phantom 9007 "Write conflict" at `BEGIN`.
#[test]
fn begin_inside_a_transaction_implicitly_commits_it() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut session = factory
        .open_session(session_context(88))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.implicit_commit (id int primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.implicit_commit VALUES (1, 0)",
    );

    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    rows(
        &mut session,
        "UPDATE test.implicit_commit SET v = 7 WHERE id = 1",
    );
    // No COMMIT: the next BEGIN carries it implicitly.
    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("BEGIN with an open transaction implicitly commits, never conflicts");
    session
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");

    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT v FROM test.implicit_commit WHERE id = 1"
        )),
        [["7"]],
        "the abandoned transaction's write was committed, not discarded"
    );
}

/// Go's pessimistic point write folds its row read INTO its lock:
/// `PointGetExecutor.getAndLock` (`pkg/executor/point_get.go:549`) locks with
/// `InitReturnValues(1)` (line 614) and reads the row from the answer, cached
/// in `TxnCtx.SetPessimisticLockCache`. This session's EXECUTE path does the
/// fold: the classified row is locked WITH its value BEFORE the snapshot is
/// bound, so the statement's one read is answered from the lock response. The
/// assertions here are behavioral, not counters: the pre-lock must REALLY
/// hold the row (a contender blocks mid-transaction), the second update of
/// the same row must compute from its own staged write, and COMMIT persists.
/// Go `decideTxnMode` with `pessimistic-auto-commit = true` makes an
/// autocommit UPDATE's implicit transaction pessimistic, so
/// `tryUpdatePointPlan` plans its point source with `Lock` and
/// `PointGetExecutor.getAndLock` locks the row with the session's
/// `LockWaitTimeout` (`getLockWaitTime`): a row another transaction holds
/// makes the statement wait the lock out. Under the default (optimistic)
/// mode the same statement takes no lock and reads the committed row.
#[test]
fn a_pessimistic_autocommit_point_update_locks_from_its_plan() {
    use tidb_param::PreparedValue;

    struct ConfigGuard;
    impl Drop for ConfigGuard {
        fn drop(&mut self) {
            tidb_config::config_tree::config::update_global(|config| {
                config.pessimistic_txn.pessimistic_auto_commit =
                    tidb_config::config_tree::AtomicBool::new(false);
            });
        }
    }

    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut holder = factory
        .open_session(session_context(103))
        .expect("session opens");
    rows(
        &mut holder,
        "CREATE TABLE test.acpess (id int primary key, v int)",
    );
    rows(&mut holder, "INSERT INTO test.acpess VALUES (1, 10)");
    let mut writer = factory
        .open_session(session_context(104))
        .expect("session opens");
    writer
        .node_conn()
        .execute_write("SET SESSION innodb_lock_wait_timeout = 1")
        .expect("set lock wait");
    let statement = writer
        .node_conn().prepare_general("UPDATE test.acpess SET v = v + 1 WHERE id = ?")
        .expect("prepare");

    holder
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin holder");
    rows(
        &mut holder,
        "SELECT * FROM test.acpess WHERE id = 1 FOR UPDATE",
    );
    let _guard = ConfigGuard;
    tidb_config::config_tree::config::update_global(|config| {
        config.pessimistic_txn.pessimistic_auto_commit =
            tidb_config::config_tree::AtomicBool::new(true);
    });
    let error = match writer.node_conn().execute_general(&statement, &[PreparedValue::SignedLongLong(1)]) {
        Err(error) => error,
        Ok(_) => panic!("the held row makes the pessimistic autocommit UPDATE wait"),
    };
    assert_eq!(error.code, 1205, "{}", error.message);
    holder
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback holder");
    assert_eq!(
        displayed(rows(&mut holder, "SELECT v FROM test.acpess WHERE id = 1")),
        [["10"]]
    );
}

/// Go with `pessimistic-auto-commit = true`: an autocommit UPDATE / DELETE
/// over a row another transaction holds waits the lock out and fails with
/// 1205 (`ErrLockWaitTimeout`) through the text protocol, for a range plan
/// as for a point one; the row is unchanged.
#[test]
fn a_pessimistic_autocommit_write_times_out_on_a_held_row() {
    struct ConfigGuard;
    impl Drop for ConfigGuard {
        fn drop(&mut self) {
            tidb_config::config_tree::config::update_global(|config| {
                config.pessimistic_txn.pessimistic_auto_commit = tidb_config::config_tree::AtomicBool::new(false);
            });
        }
    }

    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut holder = factory.open_session(session_context(106)).expect("session opens");
    rows(&mut holder, "CREATE TABLE test.acwait (id int primary key, v int)");
    rows(&mut holder, "INSERT INTO test.acwait VALUES (1, 10), (2, 20)");
    let mut writer = factory.open_session(session_context(107)).expect("session opens");
    writer.node_conn().execute_write("SET SESSION innodb_lock_wait_timeout = 1").expect("set lock wait");

    holder.node_conn().control_transaction("BEGIN PESSIMISTIC").expect("begin holder");
    rows(&mut holder, "SELECT * FROM test.acwait WHERE id = 1 FOR UPDATE");
    let _guard = ConfigGuard;
    tidb_config::config_tree::config::update_global(|config| {
        config.pessimistic_txn.pessimistic_auto_commit = tidb_config::config_tree::AtomicBool::new(true);
    });
    for sql in [
        "UPDATE test.acwait SET v = v + 1 WHERE v = 10",
        "UPDATE test.acwait SET v = v + 1 WHERE id = 1",
        "DELETE FROM test.acwait WHERE v = 10",
        "DELETE FROM test.acwait WHERE id = 1",
    ] {
        match writer.node_conn().execute_write(sql) {
            Err(error) => assert_eq!(error.code, 1205, "{sql}: {}", error.message),
            Ok(outcome) => panic!("{sql}: the held row makes the statement wait, got {outcome:?}"),
        }
    }
    holder.node_conn().control_transaction("ROLLBACK").expect("rollback holder");
    assert_eq!(displayed(rows(&mut holder, "SELECT id, v FROM test.acwait ORDER BY id")), [["1", "10"], ["2", "20"]]);
}

/// Go with `pessimistic-auto-commit = true`: plain autocommit DML runs in a
/// pessimistic implicit transaction that locks what it writes (Go master
/// capture: `tidb-wt/tools/x05/autocommit_retry.py` with `pac.toml`, whose
/// setup is this sequence).
#[test]
fn pessimistic_autocommit_dml_writes_and_locks_its_rows() {
    struct ConfigGuard;
    impl Drop for ConfigGuard {
        fn drop(&mut self) {
            tidb_config::config_tree::config::update_global(|config| {
                config.pessimistic_txn.pessimistic_auto_commit = tidb_config::config_tree::AtomicBool::new(false);
            });
        }
    }
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(127)).expect("session");
    let _guard = ConfigGuard;
    tidb_config::config_tree::config::update_global(|config| {
        config.pessimistic_txn.pessimistic_auto_commit = tidb_config::config_tree::AtomicBool::new(true);
    });
    for sql in [
        "CREATE TABLE test.pac (id INT PRIMARY KEY, v INT)",
        "INSERT INTO test.pac VALUES (1, 1)",
        "UPDATE test.pac SET v = v + 10 WHERE id = 1",
        "DELETE FROM test.pac WHERE id = 2",
    ] {
        session.node_conn().execute_write(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    assert_eq!(displayed(rows(&mut session, "SELECT id, v FROM test.pac")), [["1", "11"]]);
}

/// Go with `pessimistic-auto-commit = true`: an autocommit UPDATE waiting on
/// a peer's lock retries, once the peer commits, in the SAME transaction
/// (`OnStmtRetry` advances its for-update timestamp), so its start timestamp
/// precedes the peer's commit. Go master capture:
/// `tidb-wt/tools/x05/go_autocommit_retry.json`.
#[test]
fn a_pessimistic_autocommit_lock_retry_keeps_its_transaction() {
    struct ConfigGuard;
    impl Drop for ConfigGuard {
        fn drop(&mut self) {
            tidb_config::config_tree::config::update_global(|config| {
                config.pessimistic_txn.pessimistic_auto_commit = tidb_config::config_tree::AtomicBool::new(false);
            });
        }
    }
    let last_txn_info = |session: &mut Session, key: &str| -> u64 {
        let info = displayed(rows(session, "SELECT @@tidb_last_txn_info"))[0][0].clone();
        let value: serde_json::Value = serde_json::from_str(&info).expect("txn info json");
        value[key].as_u64().expect("timestamp")
    };
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut holder = factory.open_session(session_context(110)).expect("session opens");
    rows(&mut holder, "CREATE TABLE test.acretry (id int primary key, v int)");
    rows(&mut holder, "INSERT INTO test.acretry VALUES (1, 1)");
    let _guard = ConfigGuard;
    tidb_config::config_tree::config::update_global(|config| {
        config.pessimistic_txn.pessimistic_auto_commit = tidb_config::config_tree::AtomicBool::new(true);
    });
    holder.node_conn().control_transaction("BEGIN PESSIMISTIC").expect("begin holder");
    rows(&mut holder, "UPDATE test.acretry SET v = v + 1 WHERE id = 1");
    let writer_start_ts = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut writer = factory.open_session(session_context(111)).expect("session opens");
            writer
                .node_conn()
                .execute_write("UPDATE test.acretry SET v = v + 10 WHERE id = 1")
                .expect("the update waits, then retries");
            last_txn_info(&mut writer, "start_ts")
        });
        std::thread::sleep(std::time::Duration::from_millis(300));
        holder.node_conn().control_transaction("COMMIT").expect("commit holder");
        contender.join().expect("contender joins")
    });
    let holder_commit_ts = last_txn_info(&mut holder, "commit_ts");
    assert!(writer_start_ts < holder_commit_ts, "{writer_start_ts} vs {holder_commit_ts}");
    assert_eq!(displayed(rows(&mut holder, "SELECT v FROM test.acretry")), [["12"]]);
}

/// Go with `pessimistic-auto-commit = true`: an autocommit INSERT whose
/// unique value another transaction is inserting waits on that key's lock
/// and, once the other commits, fails with 1062 (`ErrDupEntry`), as an
/// explicit pessimistic INSERT does.
#[test]
fn a_pessimistic_autocommit_insert_racing_a_unique_value_is_a_duplicate() {
    struct ConfigGuard;
    impl Drop for ConfigGuard {
        fn drop(&mut self) {
            tidb_config::config_tree::config::update_global(|config| {
                config.pessimistic_txn.pessimistic_auto_commit = tidb_config::config_tree::AtomicBool::new(false);
            });
        }
    }

    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut holder = factory.open_session(session_context(108)).expect("session opens");
    rows(&mut holder, "CREATE TABLE test.acuniq (id int primary key, u int, unique key uk (u))");
    let _guard = ConfigGuard;
    tidb_config::config_tree::config::update_global(|config| {
        config.pessimistic_txn.pessimistic_auto_commit = tidb_config::config_tree::AtomicBool::new(true);
    });
    holder.node_conn().control_transaction("BEGIN PESSIMISTIC").expect("begin holder");
    rows(&mut holder, "INSERT INTO test.acuniq VALUES (1, 5)");
    let outcome = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut writer = factory.open_session(session_context(109)).expect("session opens");
            let outcome = writer.node_conn().execute_write("INSERT INTO test.acuniq VALUES (2, 5)").map(|_| ());
            outcome
        });
        std::thread::sleep(std::time::Duration::from_millis(300));
        holder.node_conn().control_transaction("COMMIT").expect("commit holder");
        contender.join().expect("contender joins")
    });
    match outcome {
        Err(error) => assert_eq!(error.code, 1062, "{}", error.message),
        Ok(()) => panic!("the committed unique value makes the racing INSERT a duplicate"),
    }
    assert_eq!(displayed(rows(&mut holder, "SELECT id, u FROM test.acuniq")), [["1", "5"]]);
}

/// Go `LazyTxn.LockKeys` marks the transaction `LockWaiting` while its lock
/// request waits (`updateState(TxnLockAcquiring)`), including the lock a
/// point read takes inside `PointGetExecutor.Next`: `TIDB_TRX` shows it.
#[test]
fn a_point_lock_inside_execution_shows_the_transaction_lock_waiting() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut holder = factory
        .open_session(session_context(105))
        .expect("session opens");
    rows(
        &mut holder,
        "CREATE TABLE test.lockstate (id int primary key, v int)",
    );
    rows(&mut holder, "INSERT INTO test.lockstate VALUES (1, 1)");
    holder
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin holder");
    rows(
        &mut holder,
        "SELECT * FROM test.lockstate WHERE id = 1 FOR UPDATE",
    );
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let state = std::thread::scope(|scope| {
        let waiter = scope.spawn(|| {
            let mut writer = factory
                .open_session(session_context(106))
                .expect("session opens");
            writer
                .node_conn()
                .execute_write("SET SESSION innodb_lock_wait_timeout = 3")
                .expect("set lock wait");
            writer
                .node_conn()
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin writer");
            started_tx.send(()).expect("writer started");
            let result = writer
                .node_conn()
                .execute_write("UPDATE test.lockstate SET v = 2 WHERE id = 1");
            let _ = writer.node_conn().control_transaction("ROLLBACK");
            result.map(|_| ())
        });
        started_rx.recv().expect("writer starts");
        let mut observer = factory
            .open_session(session_context(107))
            .expect("session opens");
        let mut state = Vec::new();
        for _ in 0..40 {
            std::thread::sleep(std::time::Duration::from_millis(50));
            state = displayed(rows(
                &mut observer,
                "SELECT STATE FROM information_schema.TIDB_TRX WHERE SESSION_ID = 106",
            ));
            if state == [["LockWaiting"]] {
                break;
            }
        }
        holder
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("release holder");
        waiter
            .join()
            .expect("writer thread")
            .expect("the writer's lock is granted once the holder leaves");
        state
    });
    assert_eq!(state, [["LockWaiting"]]);
}

/// Go `tidb_last_query_info.for_update_ts` is `TxnCtx.GetForUpdateTS` as the
/// statement left it: a non-point pessimistic UPDATE fetches a fresh
/// for-update timestamp (`GetStmtForUpdateTS`) above the transaction's start.
#[test]
fn a_pessimistic_range_update_reports_its_advanced_for_update_ts() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(108))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.futs (id int primary key, v int)",
    );
    rows(&mut session, "INSERT INTO test.futs VALUES (1, 1), (2, 2)");
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin");
    session
        .node_conn()
        .execute_write("UPDATE test.futs SET v = v + 1 WHERE v > 0")
        .expect("update");
    let info = rows(&mut session, "SELECT @@tidb_last_query_info");
    let tidb_datatype::Datum::String(info) = &info[0][0] else {
        panic!("tidb_last_query_info is a string: {info:?}");
    };
    let info: serde_json::Value =
        serde_json::from_slice(info.bytes()).expect("tidb_last_query_info is JSON");
    let start_ts = info["start_ts"].as_u64().expect("start_ts");
    let for_update_ts = info["for_update_ts"].as_u64().expect("for_update_ts");
    assert!(
        for_update_ts > start_ts,
        "for_update_ts {for_update_ts} must be the statement's fresh timestamp above {start_ts}"
    );
    session
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback");
}

/// Go `AdviseOptimizeWithPlan`: a pessimistic statement whose plan reads
/// only through locking point gets (or an INSERT without SELECT) keeps the
/// transaction's for-update timestamp instead of fetching a new one --
/// repeatable read always (`notNeedGetLatestTSFromPD`), read committed under
/// `tidb_rc_write_check_ts` (`planSkipGetTsoFromPD`, reusing the latest
/// oracle timestamp). Each `kept` / advanced step was captured from Go.
#[test]
fn pessimistic_point_writes_keep_the_for_update_ts_as_go_advises() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(110))
        .expect("session opens");
    rows(&mut session, "CREATE TABLE test.adv (a int primary key, b int)");
    rows(&mut session, "INSERT INTO test.adv VALUES (1, 1), (2, 2)");
    // (start_ts, for_update_ts) of the statement before.
    let last_query_ts = |session: &mut Session| {
        let info = rows(session, "SELECT @@tidb_last_query_info");
        let tidb_datatype::Datum::String(info) = &info[0][0] else {
            panic!("tidb_last_query_info is a string: {info:?}");
        };
        let info: serde_json::Value =
            serde_json::from_slice(info.bytes()).expect("tidb_last_query_info is JSON");
        (
            info["start_ts"].as_u64().expect("start_ts"),
            info["for_update_ts"].as_u64().expect("for_update_ts"),
        )
    };
    let cases: [(&str, &str, &[(&str, bool)]); 3] = [
        (
            "REPEATABLE-READ",
            "OFF",
            &[
                ("SELECT * FROM test.adv WHERE a = 1 FOR UPDATE", true),
                ("INSERT INTO test.adv VALUES (3, 3)", true),
                ("UPDATE test.adv SET b = 9 WHERE b = 2", false),
                ("UPDATE test.adv SET b = 8 WHERE a = 1", true),
            ],
        ),
        (
            "READ-COMMITTED",
            "OFF",
            &[
                ("UPDATE test.adv SET b = 7 WHERE a = 1", false),
                ("INSERT INTO test.adv VALUES (4, 4)", true),
            ],
        ),
        (
            "READ-COMMITTED",
            "ON",
            &[
                ("UPDATE test.adv SET b = 6 WHERE a = 1", true),
                ("INSERT INTO test.adv VALUES (5, 5)", true),
                ("UPDATE test.adv SET b = 5 WHERE b = 2", false),
                ("SELECT * FROM test.adv WHERE a = 2 FOR UPDATE", true),
            ],
        ),
    ];
    for (isolation, rc_write_check_ts, statements) in cases {
        rows(
            &mut session,
            &format!(
                "SET transaction_isolation = '{isolation}', tidb_rc_write_check_ts = {rc_write_check_ts}"
            ),
        );
        session
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin");
        // The first statement is measured against the transaction's start.
        let mut previous = None;
        for (sql, kept) in statements {
            rows(&mut session, sql);
            let (start_ts, current) = last_query_ts(&mut session);
            let previous_ts = previous.unwrap_or(start_ts);
            assert_eq!(
                current == previous_ts,
                *kept,
                "{isolation} rc_write_check_ts={rc_write_check_ts}: {sql} moved for_update_ts {previous_ts} -> {current}"
            );
            previous = Some(current);
        }
        session
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
    }
}

/// Go's locking point reads lock inside `Next` exactly the keys their
/// isolation names (`PointGetExecutor.Next` / `getAndLock`,
/// `BatchPointGetExec.initialize`): repeatable read locks a unique index key
/// and the row it names, and every named key even when it is absent; read
/// committed locks only what exists. Each outcome was captured live against
/// the Go server on TiKV with a contender probing the keys.
#[test]
fn locking_point_reads_hold_the_keys_their_isolation_names() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut setup = factory
        .open_session(session_context(109))
        .expect("session opens");
    for sql in [
        "CREATE TABLE test.lku (id int primary key, k int, v int, unique key uk(k))",
        "CREATE TABLE test.lkt (id int primary key, v int)",
        "INSERT INTO test.lku VALUES (1, 10, 1), (2, 20, 2)",
        "INSERT INTO test.lkt VALUES (1, 1)",
    ] {
        rows(&mut setup, sql);
    }
    let cases: [(&str, &str, &str, bool); 5] = [
        (
            "REPEATABLE-READ",
            "UPDATE test.lku SET v = v + 1 WHERE k = 10",
            "SELECT * FROM test.lku WHERE k = 10 FOR UPDATE NOWAIT",
            true,
        ),
        (
            "REPEATABLE-READ",
            "UPDATE test.lku SET v = v + 1 WHERE k = 90",
            "INSERT INTO test.lku VALUES (9, 90, 9)",
            true,
        ),
        (
            "REPEATABLE-READ",
            "UPDATE test.lku SET v = v + 1 WHERE k IN (10, 90)",
            "INSERT INTO test.lku VALUES (9, 90, 9)",
            true,
        ),
        (
            "READ-COMMITTED",
            "UPDATE test.lku SET v = v + 1 WHERE k IN (10, 90)",
            "INSERT INTO test.lku VALUES (9, 90, 9)",
            false,
        ),
        (
            "READ-COMMITTED",
            "UPDATE test.lkt SET v = v + 1 WHERE id = 9",
            "INSERT INTO test.lkt VALUES (9, 9)",
            false,
        ),
    ];
    let mut writer = factory
        .open_session(session_context(110))
        .expect("session opens");
    let mut contender = factory
        .open_session(session_context(111))
        .expect("session opens");
    contender
        .node_conn()
        .execute_write("SET SESSION innodb_lock_wait_timeout = 1")
        .expect("set lock wait");
    for (isolation, statement, probe, held) in cases {
        writer
            .node_conn()
            .execute_write(&format!(
                "SET SESSION transaction_isolation = '{isolation}'"
            ))
            .expect("set isolation");
        writer
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin writer");
        writer
            .node_conn()
            .execute_write(statement)
            .expect("statement runs");
        contender
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin contender");
        let result = if probe.starts_with("SELECT") {
            contender.node_conn().execute(probe).map(|_| ())
        } else {
            contender.node_conn().execute_write(probe).map(|_| ())
        };
        let probed = match result {
            Ok(()) => false,
            Err(error) => {
                assert!(
                    matches!(error.code, 1205 | 3572),
                    "{probe}: {}",
                    error.message
                );
                true
            }
        };
        contender
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
        writer
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
        assert_eq!(probed, held, "{isolation}: {statement} / {probe}");
    }
}

/// Go `updateRecord` inside a transaction (no `SkipWriteUntouchedIndices`)
/// rewrites every untouched index entry into the memory buffer with the
/// untouched flag (`index.create(untouched = true)`), and `TiDBKVFilter`
/// keeps those entries out of the commit. Captured live against the Go
/// server on TiKV: `TIDB_TRX.MEM_BUFFER_KEYS` is 3 after a one-row UPDATE of
/// a non-indexed column of a table with two secondary indexes, 6 after the
/// second row, and the committed table still checks clean.
#[test]
fn an_update_in_a_transaction_stages_untouched_index_entries_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(112))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.untouched (id int primary key, k int, c int, v int, \
         unique key uk(k), key kc(c))",
    );
    rows(
        &mut session,
        "INSERT INTO test.untouched VALUES (1, 10, 100, 1), (2, 20, 200, 2)",
    );
    let keys = |session: &mut crate::Session| {
        displayed(rows(
            session,
            "SELECT MEM_BUFFER_KEYS FROM information_schema.TIDB_TRX \
             WHERE SESSION_ID = 112",
        ))
    };
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin");
    session
        .node_conn()
        .execute_write("UPDATE test.untouched SET v = v + 1 WHERE id = 1")
        .expect("update");
    assert_eq!(keys(&mut session), [["3"]]);
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT v FROM test.untouched USE INDEX(uk) WHERE k = 10",
        )),
        [["2"]]
    );
    session
        .node_conn()
        .execute_write("UPDATE test.untouched SET v = v + 1 WHERE v > 1")
        .expect("update");
    assert_eq!(keys(&mut session), [["6"]]);
    session
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");
    session
        .node_conn()
        .execute_write("ADMIN CHECK TABLE test.untouched")
        .expect("the untouched entries were not committed");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id, v FROM test.untouched USE INDEX(kc) WHERE c > 0 ORDER BY id",
        )),
        [["1", "3"], ["2", "3"]]
    );
}

#[test]
fn a_prepared_point_update_locks_its_row_before_reading_it() {
    use tidb_param::PreparedValue;

    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(90))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.fold (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.fold VALUES (1, 10)");

    assert_eq!(
        first
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
        Some(true)
    );
    let statement = first
        .node_conn().prepare_general("UPDATE test.fold SET v = v + 5 WHERE id = ?")
        .expect("prepare");
    let affected = match first
        .node_conn().execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
        .expect("execute")
    {
        crate::domain::session_api::GeneralExecuteOutcome::Write(outcome) => outcome.affected_rows,
        crate::domain::session_api::GeneralExecuteOutcome::Rows(_) => {
            panic!("an UPDATE answers OK, not with a result set")
        }
    };
    assert_eq!(affected, 1);

    // The lock was taken BEFORE the statement read anything, so a contender
    // on the same row must block until this transaction commits -- exactly
    // the interleaving
    // [`racing_pessimistic_updates_both_commit_with_serial_effect`] pins for
    // the post-run lock step, now exercised by the PRE-lock.
    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(91))
                .expect("session opens");
            assert_eq!(
                second
                    .node_conn()
                    .control_transaction("BEGIN")
                    .expect("begin"),
                Some(true)
            );
            rows(&mut second, "UPDATE test.fold SET v = v + 100 WHERE id = 1");
            second
                .node_conn()
                .control_transaction("COMMIT")
                .expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        first
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after waiting the prelock out");

    let mut after = factory
        .open_session(session_context(92))
        .expect("session opens");
    assert_eq!(
        displayed(rows(&mut after, "SELECT v FROM test.fold WHERE id = 1")),
        [["115"]],
        "+5 landed from the lock-carrying read, then +100 from the winner that re-read it"
    );
}

/// The same fold over the TEXT protocol: a client-side prepared driver
/// (Connector/J without `useServerPrepStmts`) sends the point `UPDATE` as
/// plain COM_QUERY, so the write-path classification must fire there too.
/// Behavioral assertion as above -- the row is locked BEFORE the statement
/// reads it, so a contender blocks mid-transaction.
#[test]
fn a_text_point_update_locks_its_row_before_reading_it() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(96))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.foldtxt (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.foldtxt VALUES (1, 10)");

    assert_eq!(
        first
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
        Some(true)
    );
    let affected = {
        let out = rows(&mut first, "UPDATE test.foldtxt SET v = v + 5 WHERE id = 1");
        out.len()
    };
    let _ = affected;

    // A contender on the same row must block until this transaction commits:
    // the lock predates the statement's own read.
    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(97))
                .expect("session opens");
            assert_eq!(
                second
                    .node_conn()
                    .control_transaction("BEGIN")
                    .expect("begin"),
                Some(true)
            );
            rows(
                &mut second,
                "UPDATE test.foldtxt SET v = v + 100 WHERE id = 1",
            );
            second
                .node_conn()
                .control_transaction("COMMIT")
                .expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        first
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after waiting the prelock out");

    let mut after = factory
        .open_session(session_context(98))
        .expect("session opens");
    assert_eq!(
        displayed(rows(&mut after, "SELECT v FROM test.foldtxt WHERE id = 1")),
        [["115"]],
        "+5 landed from the lock-carrying read, then +100 from the winner that re-read it"
    );
}

/// A strict pessimistic `UPDATE`/`DELETE` whose key constant fails Go's
/// fast-plan conversion (`getNameValuePairs` under the statement's type
/// context) has no point plan, so it locks nothing before its scan reports
/// the conversion error. Captured live against the Go server on TiKV: with
/// another transaction holding key 0, `UPDATE t SET v = 9 WHERE id = 'abc'`
/// fails at once with 1292 instead of waiting out the lock (1205).
///
/// The truncation is raised by the store evaluating the pushed Selection,
/// which answers it in `SelectResponse.error` under its own code (Go
/// unistore's `toPBError`, TiKV alike). The text is the evaluating engine's:
/// TiKV's cast names INTEGER (captured), while Go's unistore -- like this
/// store -- evaluates TiDB's `eq` over two doubles, whose string cast names
/// DOUBLE.
#[test]
fn a_strict_point_write_whose_key_does_not_convert_takes_no_prelock() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut holder = factory
        .open_session(session_context(99))
        .expect("session opens");
    rows(
        &mut holder,
        "CREATE TABLE test.noprelock (id int primary key, v int)",
    );
    rows(
        &mut holder,
        "INSERT INTO test.noprelock VALUES (0, 0), (1, 1)",
    );
    holder
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin holder");
    rows(
        &mut holder,
        "SELECT * FROM test.noprelock WHERE id = 0 FOR UPDATE",
    );

    let mut writer = factory
        .open_session(session_context(100))
        .expect("session opens");
    writer
        .node_conn()
        .execute_write("SET SESSION innodb_lock_wait_timeout = 1")
        .expect("set lock wait");
    for (sql, value) in [
        ("UPDATE test.noprelock SET v = 9 WHERE id = 'abc'", "abc"),
        ("UPDATE test.noprelock SET v = 9 WHERE id IN ('a', 'b')", "a"),
        ("DELETE FROM test.noprelock WHERE id = 'abc'", "abc"),
        ("DELETE FROM test.noprelock WHERE id IN ('a', 'b')", "a"),
    ] {
        writer
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin writer");
        let error = writer
            .node_conn()
            .execute_write(sql)
            .expect_err("the conversion fails the statement");
        assert_eq!(
            (error.code, error.message.as_str()),
            (
                1292,
                format!("Truncated incorrect DOUBLE value: '{value}'").as_str()
            ),
            "{sql} (1205 would be a wait on the held key 0)"
        );
        writer
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
    }
    holder
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback holder");
}

/// Go takes a pessimistic point lock inside execution (compile -> build the
/// executor -> `PointGetExecutor.Next` -> `getAndLock`), so a point write
/// that fails to compile never reaches a lock. Captured live against the Go
/// server on TiKV with another transaction holding key 1: each statement
/// fails at once with its compile error instead of waiting out the lock.
#[test]
fn a_point_write_that_fails_to_compile_takes_no_lock() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut holder = factory
        .open_session(session_context(101))
        .expect("session opens");
    rows(
        &mut holder,
        "CREATE TABLE test.compilelock (id int primary key, v int)",
    );
    rows(&mut holder, "INSERT INTO test.compilelock VALUES (1, 1)");
    holder
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin holder");
    rows(
        &mut holder,
        "SELECT * FROM test.compilelock WHERE id = 1 FOR UPDATE",
    );

    let mut writer = factory
        .open_session(session_context(102))
        .expect("session opens");
    writer
        .node_conn()
        .execute_write("SET SESSION innodb_lock_wait_timeout = 1")
        .expect("set lock wait");
    for (sql, message) in [
        (
            "UPDATE test.compilelock SET nosuch = 1 WHERE id = 1",
            "Unknown column 'nosuch' in 'field list'",
        ),
        (
            "DELETE FROM test.compilelock WHERE id = 1 AND nosuch = 1",
            "Unknown column 'nosuch' in 'where clause'",
        ),
    ] {
        writer
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin writer");
        let error = writer
            .node_conn()
            .execute_write(sql)
            .expect_err("the statement does not compile");
        assert_eq!(
            (error.code, error.message.as_str()),
            (1054, message),
            "{sql}"
        );
        writer
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
    }
    holder
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback holder");
}

/// Go's `SELECT ... FOR UPDATE` on one clustered handle-pinned row folds its
/// read INTO its lock (`TryFastPlan` -> `PointGetPlan(Lock=true)` ->
/// `getAndLock`). The text path classifies the same shape: the locking read's
/// row answers from the PessimisticLock response, and a contender on the row
/// blocks for the transaction's lifetime. The read-your-own-lock image is
/// also what a later statement in the same transaction must compute from.
#[test]
fn a_text_select_for_update_folds_its_read_into_its_lock() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(99))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.foldsel (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.foldsel VALUES (1, 10)");

    assert_eq!(
        first
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
        Some(true)
    );
    assert_eq!(
        displayed(rows(
            &mut first,
            "SELECT v FROM test.foldsel WHERE id = 1 FOR UPDATE"
        )),
        [["10"]],
        "the locking read answers its row"
    );

    // The row is already locked by the statement itself: a contender's update
    // waits for this transaction to end.
    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(100))
                .expect("session opens");
            assert_eq!(
                second
                    .node_conn()
                    .control_transaction("BEGIN")
                    .expect("begin"),
                Some(true)
            );
            rows(
                &mut second,
                "UPDATE test.foldsel SET v = v + 100 WHERE id = 1",
            );
            second
                .node_conn()
                .control_transaction("COMMIT")
                .expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        rows(&mut first, "UPDATE test.foldsel SET v = v + 5 WHERE id = 1");
        first
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after the folding reader commits");

    let mut after = factory
        .open_session(session_context(101))
        .expect("session opens");
    assert_eq!(
        displayed(rows(&mut after, "SELECT v FROM test.foldsel WHERE id = 1")),
        [["115"]],
        "+5 from the folded reader, then +100 from the winner that re-read it"
    );
}

/// Go's pessimistic `UPDATE`/`DELETE ... WHERE pk IN (...)` is a locking
/// `Batch_Point_Get` (`tryUpdatePointPlan`/`tryDeletePointPlan` set
/// `BatchPointGetPlan.Lock`), and `BatchPointGetExec` under repeatable read
/// locks every named key WITH its value before reading
/// (`batch_point_get.go:434-444`: `LockKeys` -> `InitReturnValues`), then
/// reads through the pessimistic-lock cache. One PessimisticLock RPC and no
/// BatchGet: counted at the store's RPC client, Go's
/// `UnistoreRPCClientSendHook`.
#[test]
fn a_pessimistic_batch_point_write_folds_its_read_into_one_lock() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut session = factory
        .open_session(session_context(102))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.foldbatch (id int primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.foldbatch VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
    );
    let table_id: i64 = displayed(rows(
        &mut session,
        "SELECT TIDB_TABLE_ID FROM information_schema.tables \
         WHERE table_schema = 'test' AND table_name = 'foldbatch'",
    ))[0][0]
        .parse()
        .expect("table id");
    // Only this table's row reads and locks count: the session's own system
    // reads (statistics, catalog) share the store.
    fn is_row_of(table_id: i64, key: &[u8]) -> bool {
        tidb_tablecodec::table_key::decode_record_key(logical_rpc_key(key)).is_ok_and(|(table, _)| table == table_id)
    }
    let sent: Arc<std::sync::Mutex<Vec<&'static str>>> = Arc::default();
    let recorded = Arc::clone(&sent);
    for (statement, after) in [
        (
            "UPDATE test.foldbatch SET v = v + 1 WHERE id IN (1, 2, 3)",
            [["1", "11"], ["2", "21"], ["3", "31"], ["4", "40"]].as_slice(),
        ),
        (
            "DELETE FROM test.foldbatch WHERE id IN (2, 3, 9)",
            [["1", "11"], ["4", "40"]].as_slice(),
        ),
    ] {
        session
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin");
        let recorded = Arc::clone(&recorded);
        stack.rpc.set_send_hook(Some(Arc::new(move |request| {
            use tikv_client::proto::kvrpcpb;
            let any = request.as_any();
            let touched = if let Some(lock) = any.downcast_ref::<kvrpcpb::PessimisticLockRequest>()
            {
                lock.mutations.iter().any(|m| is_row_of(table_id, &m.key))
            } else if let Some(get) = any.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                get.keys.iter().any(|key| is_row_of(table_id, key))
            } else if let Some(get) = any.downcast_ref::<kvrpcpb::GetRequest>() {
                is_row_of(table_id, &get.key)
            } else {
                false
            };
            if touched {
                recorded.lock().unwrap().push(request.label());
            }
        })));
        rows(&mut session, statement);
        stack.rpc.set_send_hook(None);
        let labels = std::mem::take(&mut *sent.lock().unwrap());
        let count = |label: &str| labels.iter().filter(|sent| **sent == label).count();
        assert_eq!(
            (
                count("kv_pessimistic_lock"),
                count("kv_batch_get"),
                count("kv_get")
            ),
            (1, 0, 0),
            "{statement}: {labels:?}"
        );
        session
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit");
        assert_eq!(
            displayed(rows(
                &mut session,
                "SELECT id, v FROM test.foldbatch ORDER BY id"
            )),
            after,
            "{statement}"
        );
    }
}

/// Go SelectLockExec locks the rows the range produces, including a LIMIT
/// winner. A contender must replay the range after that winner is deleted.
#[test]
fn a_locking_range_reselects_after_the_first_row_is_deleted() {
    locking_range_reselects("id", "ORDER BY id LIMIT 1", &["1"], &["2"]);
}

#[test]
fn a_locking_range_without_order_or_limit_still_locks() {
    locking_range_reselects("id", "", &["1", "2", "3"], &["2", "3"]);
}

#[test]
fn a_locking_range_projection_retains_its_composite_handle() {
    locking_range_reselects("id", "ORDER BY id", &["1", "2", "3"], &["2", "3"]);
}

#[test]
fn a_locking_aggregate_reselects_its_input_rows() {
    locking_range_reselects("SUM(id)", "", &["6"], &["5"]);
}

#[test]
fn locking_queries_retain_unprojected_record_handles() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(112))
        .expect("session opens");
    for (name, definition) in [
        ("lock_heap", "id int, v int"),
        ("lock_integer", "id int primary key, v int"),
        ("lock_common", "id int, v int, primary key(id,v) clustered"),
    ] {
        rows(
            &mut session,
            &format!("CREATE TABLE test.{name} ({definition})"),
        );
        rows(
            &mut session,
            &format!("INSERT INTO test.{name} VALUES (1,10)"),
        );
        session
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin");
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT v FROM test.{name} WHERE v>0 LIMIT 1 FOR UPDATE")
            )),
            [["10"]],
            "record handles must be available without appearing in the result"
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT * FROM test.{name} WHERE v>0 LIMIT 1 FOR UPDATE")
            )),
            [["1", "10"]],
            "hidden handles must not leak through wildcard projection"
        );
        session
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
    }
    // Go checkLockClauseTables / planner_issue_test's aliased OF targets:
    // the untargeted joined table must remain available to another locker.
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    for target in ["h", "test.lock_heap"] {
        assert_eq!(displayed(rows(&mut session, &format!(
            "SELECT h.v FROM test.lock_heap h JOIN test.lock_integer i ON h.id=i.id FOR UPDATE OF {target}"
        ))), [["10"]]);
    }
    let mut contender = stack.factory.open_session(session_context(115)).unwrap();
    contender
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut contender,
            "SELECT v FROM test.lock_integer WHERE id=1 FOR UPDATE NOWAIT"
        )),
        [["10"]]
    );
    assert!(
        matches!(contender.node_conn().execute("SELECT v FROM test.lock_heap WHERE v>0 FOR UPDATE NOWAIT"),
        Err(error) if error.code == 3572)
    );
    contender
        .node_conn()
        .control_transaction("ROLLBACK")
        .unwrap();
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert!(
        matches!(session.node_conn().execute("SELECT v FROM test.lock_heap FOR UPDATE OF missing"),
        Err(error) if error.code == 1109)
    );
}

/// A SELECT's FOR UPDATE locks its own rows, under the same keys a point
/// lock takes:
/// - Go `buildSubquery` pops the handle map a WHERE subquery leaves, so the
///   lock still sees the outer table's handles through an IN or `<> ANY`
///   semi join;
/// - a partitioned row's lock key names its partition, from the
///   `ExtraPhysTblID` storage fills in from the row's key (Go's TableScan
///   requests that column last);
/// - the subquery's own partitioned table carries that column too, and the
///   semi join drops it (`PhysicalLock.ResolveIndices` binds handles only).
#[test]
fn a_where_subquery_keeps_the_select_lock_on_its_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(993))
        .expect("session opens");
    for sql in [
        "CREATE TABLE test.sublock_t (id int primary key, v int)",
        "CREATE TABLE test.sublock_p (id int primary key, v int) PARTITION BY HASH(id) PARTITIONS 2",
        "CREATE TABLE test.sublock_u (id int primary key, v int) PARTITION BY HASH(id) PARTITIONS 2",
        "INSERT INTO test.sublock_t VALUES (1,10)",
        "INSERT INTO test.sublock_p VALUES (1,10)",
        "INSERT INTO test.sublock_u VALUES (1,10),(2,20)",
    ] {
        rows(&mut session, sql);
    }
    let mut contender = stack
        .factory
        .open_session(session_context(994))
        .expect("contender opens");
    for (locking, contended) in [
        (
            "SELECT id FROM test.sublock_t WHERE v IN (SELECT v FROM test.sublock_u) FOR UPDATE",
            "SELECT v FROM test.sublock_t WHERE id=1 FOR UPDATE NOWAIT",
        ),
        (
            "SELECT id FROM test.sublock_p WHERE v > 0 FOR UPDATE",
            "SELECT v FROM test.sublock_p WHERE id=1 FOR UPDATE NOWAIT",
        ),
        (
            "SELECT id FROM test.sublock_p WHERE v IN (SELECT v FROM test.sublock_u) FOR UPDATE",
            "SELECT v FROM test.sublock_p WHERE id=1 FOR UPDATE NOWAIT",
        ),
        (
            "SELECT id FROM test.sublock_t WHERE v <> ANY (SELECT v FROM test.sublock_u) FOR UPDATE",
            "SELECT v FROM test.sublock_t WHERE id=1 FOR UPDATE NOWAIT",
        ),
        (
            "SELECT id FROM test.sublock_p WHERE v <> ANY (SELECT v FROM test.sublock_u) FOR UPDATE",
            "SELECT v FROM test.sublock_p WHERE id=1 FOR UPDATE NOWAIT",
        ),
    ] {
        session
            .node_conn().control_transaction("BEGIN PESSIMISTIC")
            .expect("begin");
        assert_eq!(displayed(rows(&mut session, locking)), [["1"]], "{locking}");
        contender
            .node_conn().control_transaction("BEGIN PESSIMISTIC")
            .expect("contender begins");
        assert!(
            matches!(contender.node_conn().execute(contended), Err(error) if error.code == 3572),
            "{locking} left the row unlocked"
        );
        contender.node_conn().control_transaction("ROLLBACK").unwrap();
        session.node_conn().control_transaction("ROLLBACK").unwrap();
    }
}

#[test]
fn a_locking_range_sees_rows_committed_after_begin() {
    let (stack, _users) = cop_backed_stack();
    let mut reader = stack
        .factory
        .open_session(session_context(113))
        .expect("reader");
    let mut writer = stack
        .factory
        .open_session(session_context(114))
        .expect("writer");
    rows(
        &mut reader,
        "CREATE TABLE test.fresh_lock (id int primary key, v int)",
    );
    reader
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin");
    assert!(rows(&mut reader, "SELECT id FROM test.fresh_lock WHERE id>0").is_empty());
    rows(&mut writer, "INSERT INTO test.fresh_lock VALUES (1,10)");
    let locked = displayed(rows(
        &mut reader,
        "SELECT id FROM test.fresh_lock WHERE id>0 ORDER BY id LIMIT 1 FOR UPDATE",
    ));
    let plain = rows(&mut reader, "SELECT id FROM test.fresh_lock WHERE id>0");
    reader
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback");
    assert_eq!(
        locked,
        [["1"]],
        "locking reads use a fresh statement snapshot"
    );
    assert!(plain.is_empty(), "ordinary reads keep the BEGIN snapshot");
}

/// Go's repeatable-read BatchPointGet locks absent keys as well as rows.
/// Returning just the existing row must not permit an INSERT into another
/// key named by the same IN list while the locking transaction stays open.
#[test]
fn a_locking_batch_locks_missing_primary_keys() {
    use crate::domain::resultset_source::ResultSetSource;
    use tidb_param::PreparedValue;

    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    for (index, (isolation, change_default, implicit, prepared)) in [
        ("REPEATABLE-READ", false, false, false),
        ("READ-COMMITTED", false, false, false),
        ("REPEATABLE-READ", true, false, false),
        ("READ-COMMITTED", true, false, false),
        ("REPEATABLE-READ", false, true, false),
        ("REPEATABLE-READ", false, false, true),
    ]
    .into_iter()
    .enumerate()
    {
        let mut first = factory.open_session(session_context(115)).expect("reader");
        let table = format!("test.batch_missing_{index}");
        // The prepared case binds reordered, duplicated composite tuples.
        let definition = if prepared {
            "id INT, k INT, PRIMARY KEY(id,k) CLUSTERED"
        } else {
            "id INT PRIMARY KEY"
        };
        rows(&mut first, &format!("CREATE TABLE {table} ({definition})"));
        rows(
            &mut first,
            &format!(
                "INSERT INTO {table} VALUES ({})",
                if prepared { "1,10" } else { "1" }
            ),
        );
        rows(
            &mut first,
            &format!("SET SESSION transaction_isolation='{isolation}'"),
        );
        if implicit {
            rows(&mut first, "SET tidb_txn_mode='pessimistic'");
            rows(&mut first, "SET autocommit=0");
        } else {
            first
                .node_conn()
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin");
        }
        if change_default {
            let changed = if isolation == "REPEATABLE-READ" {
                "READ-COMMITTED"
            } else {
                "REPEATABLE-READ"
            };
            rows(
                &mut first,
                &format!("SET SESSION transaction_isolation='{changed}'"),
            );
        }
        let selected = if prepared {
            let statement = first
                .node_conn().prepare_general(&format!(
                    "SELECT id FROM {table} WHERE (k,id) IN ((?,?),(?,?),(?,?)) FOR UPDATE"
                ))
                .expect("prepare batch");
            let params = [10, 1, 20, 2, 20, 2].map(PreparedValue::SignedLongLong);
            let crate::domain::session_api::GeneralExecuteOutcome::Rows(mut result) = first
                .node_conn().execute_general(&statement, &params)
                .expect("execute batch")
            else {
                panic!("batch returns rows")
            };
            let source = result.source();
            let mut result = Vec::new();
            loop {
                let batch = source.next_batch(8).expect("batch");
                if batch.is_empty() {
                    break;
                }
                result.extend(batch);
            }
            source.finish().expect("finish");
            source.close().expect("close");
            result
        } else {
            rows(
                &mut first,
                &format!("SELECT id FROM {table} WHERE id IN (1,2) FOR UPDATE"),
            )
        };
        assert_eq!(displayed(selected), [["1"]], "case {index}");

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let blocked = std::thread::scope(|scope| {
            let contender = scope.spawn(|| {
                let mut second = factory.open_session(session_context(116)).expect("writer");
                started_tx.send(()).expect("writer started");
                rows(
                    &mut second,
                    &format!(
                        "INSERT INTO {table} VALUES ({})",
                        if prepared { "2,20" } else { "2" }
                    ),
                );
                finished_tx.send(()).expect("writer finished");
            });
            started_rx.recv().expect("writer starts");
            let blocked = matches!(
                finished_rx.recv_timeout(std::time::Duration::from_millis(200)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            );
            // Release before asserting so failure cannot strand the writer.
            first
                .node_conn()
                .control_transaction("ROLLBACK")
                .expect("release reader");
            contender.join().expect("writer finishes after release");
            blocked
        });
        assert_eq!(
            blocked,
            isolation == "REPEATABLE-READ",
            "case {index}: absent-key lock follows the transaction's isolation"
        );
        assert_eq!(
            displayed(rows(
                &mut first,
                &format!("SELECT id FROM {table} ORDER BY id")
            )),
            [["1"], ["2"]]
        );
        first
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("finish implicit read");
    }
}

/// The prelock joins the failed-statement release list: an EXECUTE that fails
/// AFTER its row was locked (a strict-mode cast error during the assignment)
/// must give the lock back AND drop its cached row image, so a contender can
/// take the row immediately (`OnPessimisticStmtEnd(isSuccessful=false)`).
#[test]
fn a_failed_prelocked_update_releases_its_row() {
    use tidb_param::PreparedValue;

    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(93))
        .expect("session opens");
    rows(
        &mut first,
        "CREATE TABLE test.foldfail (id int primary key, v int)",
    );
    rows(&mut first, "INSERT INTO test.foldfail VALUES (1, 10)");

    assert_eq!(
        first
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
        Some(true)
    );
    let statement = first
        .node_conn().prepare_general("UPDATE test.foldfail SET v = 'not-a-number' WHERE id = ?")
        .expect("prepare");
    // The execute outcome borrows the session for its row source, so the
    // error check happens inside a scope that ends before the rollback.
    let failed = {
        let outcome = first.node_conn().execute_general(&statement, &[PreparedValue::SignedLongLong(1)]);
        outcome.is_err()
    };
    if !failed {
        // A non-strict node coerces instead of failing; then there is no
        // post-lock failure to exercise and the release behavior above is
        // covered by the transaction-end path instead.
        first
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
        return;
    }
    // The statement failed after its prelock; the transaction stays open.
    assert_eq!(
        first
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback"),
        Some(false)
    );

    // A contender takes the released row immediately: had the prelock leaked,
    // this would block for the lock-wait timeout instead of completing.
    let mut second = factory
        .open_session(session_context(94))
        .expect("session opens");
    assert_eq!(
        second
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin"),
        Some(true)
    );
    rows(&mut second, "UPDATE test.foldfail SET v = 99 WHERE id = 1");
    second
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");

    let mut after = factory
        .open_session(session_context(95))
        .expect("session opens");
    assert_eq!(
        displayed(rows(&mut after, "SELECT v FROM test.foldfail WHERE id = 1")),
        [["99"]],
        "the failed statement left neither its lock nor its staged write behind"
    );
}

#[test]
fn login_durability_batch_locks_unlocks_and_checks_stale_cache() {
    use crate::domain::configured_user_store::AuthenticationFailure;
    let (stack, users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(689)).unwrap();
    rows(
        &mut session,
        "CREATE USER 'durable_lock'@'%' FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 1",
    );
    assert!(matches!(
        users.authenticate_native(
            "durable_lock",
            "127.0.0.1",
            b"01234567890123456789",
            b"wrong"
        ),
        Err(AuthenticationFailure::AutoLocked(_))
    ));
    let attributes = displayed(rows(
        &mut session,
        "SELECT user_attributes FROM mysql.user WHERE User='durable_lock' AND Host='%'",
    ))
    .remove(0)
    .remove(0);
    let attributes: serde_json::Value = serde_json::from_str(&attributes).unwrap();
    assert_eq!(attributes["Password_locking"]["auto_account_locked"], "Y");
    // Go `autolockAction`: the lock change reloads the privileges
    // (`NotifyUpdatePrivilege`).
    assert!(users.accounts().password_locking("durable_lock", "%").unwrap().auto_account_locked);
    // The loaded privileges go stale as Go's do: a row loaded unlocked by
    // FLUSH PRIVILEGES, then locked again by a write no reload follows.
    rows(&mut session, "UPDATE mysql.user SET user_attributes=JSON_MERGE_PATCH(user_attributes, '{\"Password_locking\": {\"auto_account_locked\": \"N\"}}') WHERE User='durable_lock' AND Host='%'");
    rows(&mut session, "FLUSH PRIVILEGES");
    rows(&mut session, "UPDATE mysql.user SET user_attributes=JSON_MERGE_PATCH(user_attributes, '{\"Password_locking\": {\"auto_account_locked\": \"Y\"}}') WHERE User='durable_lock' AND Host='%'");
    assert!(
        !users.accounts().password_locking("durable_lock", "%").unwrap().auto_account_locked,
        "the loaded privileges are stale"
    );
    assert!(
        matches!(
            users.authenticate_native("durable_lock", "127.0.0.1", b"01234567890123456789", b""),
            Err(AuthenticationFailure::AutoLocked(_))
        ),
        "successful password must read the authoritative locked row"
    );
    rows(&mut session, "UPDATE mysql.user SET user_attributes=JSON_MERGE_PATCH(user_attributes, '{\"Password_locking\": {\"failed_login_attempts\": 0}}') WHERE User='durable_lock' AND Host='%'");
    assert!(
        matches!(
            users.authenticate_native("durable_lock", "127.0.0.1", b"01234567890123456789", b""),
            Err(AuthenticationFailure::AutoLocked(_))
        ),
        "authoritative lock wins even if tracking was concurrently disabled"
    );
    rows(&mut session, "UPDATE mysql.user SET user_attributes=JSON_MERGE_PATCH(user_attributes, '{\"Password_locking\": {\"failed_login_attempts\": 1}}') WHERE User='durable_lock' AND Host='%'");
    rows(&mut session, "FLUSH PRIVILEGES");
    users.accounts().clock().advance(24 * 60 * 60 + 1);
    users
        .authenticate_native("durable_lock", "127.0.0.1", b"01234567890123456789", b"")
        .unwrap();
    let attributes = displayed(rows(
        &mut session,
        "SELECT user_attributes FROM mysql.user WHERE User='durable_lock' AND Host='%'",
    ))
    .remove(0)
    .remove(0);
    let attributes: serde_json::Value = serde_json::from_str(&attributes).unwrap();
    assert_eq!(attributes["Password_locking"]["auto_account_locked"], "N");
    assert_eq!(attributes["Password_locking"]["failed_login_count"], 0);
}

#[test]
fn deferred_uniqueness_batch_writes_skip_locks_and_check_at_commit() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(940)).unwrap();
    rows(&mut session, "CREATE DATABASE deferred_writes");
    rows(&mut session, "USE deferred_writes");
    rows(
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, u INT UNIQUE)",
    );
    rows(&mut session, "INSERT INTO t VALUES (1,10),(2,20)");
    rows(
        &mut session,
        "SET tidb_constraint_check_in_place_pessimistic=OFF",
    );
    rows(&mut session, "SET tidb_txn_assertion_level=OFF");
    for sql in [
        "INSERT INTO t VALUES (3,30)",
        "INSERT INTO t VALUES (1,30)",
        "INSERT INTO t VALUES (3,10)",
        "UPDATE t SET id=3,u=30 WHERE id=1",
        "INSERT INTO t VALUES (1,40) ON DUPLICATE KEY UPDATE id=3,u=30",
    ] {
        session
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .unwrap();
        let stage = session.node().buffer.checkpoint();
        session.node_conn().execute_write(sql).unwrap();
        let lock_keys = session.node().buffer.pessimistic_keys_since(stage);
        // New record/unique-index keys defer their absence check. UPDATE
        // retains the old record lock and deletion entries as Go does.
        let presumed = session.node().buffer.presume_not_exists_keys();
        assert!(!presumed.is_empty(), "{sql}");
        assert!(
            lock_keys.iter().all(|key| !presumed.contains(key)),
            "{sql}: deferred keys were locked"
        );
        session.node().buffer.release(stage);
        if sql == "INSERT INTO t VALUES (1,30)" || sql == "INSERT INTO t VALUES (3,10)" {
            let error = session
                .node_conn()
                .control_transaction("COMMIT")
                .unwrap_err();
            assert_eq!(error.code, 1062, "{sql}: {error:?}");
        } else {
            session.node_conn().control_transaction("ROLLBACK").unwrap();
        }
    }
    // Go unistore checks NotExist before Insert when assertions are enabled.
    rows(&mut session, "SET tidb_txn_assertion_level=STRICT");
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    rows(&mut session, "INSERT INTO t VALUES (1,30)");
    assert_eq!(
        session
            .node_conn()
            .control_transaction("COMMIT")
            .unwrap_err()
            .code,
        8141
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t ORDER BY id")),
        vec![vec!["1", "10"], vec!["2", "20"]]
    );
}

#[test]
fn deferred_uniqueness_batch_savepoint_follows_transaction_mode() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(941)).unwrap();
    rows(
        &mut session,
        "SET tidb_constraint_check_in_place_pessimistic=OFF",
    );
    // Autocommit SAVEPOINT is still a no-op; optimistic transactions work.
    session
        .node_conn()
        .control_transaction("SAVEPOINT empty")
        .unwrap();
    session
        .node_conn()
        .control_transaction("BEGIN OPTIMISTIC")
        .unwrap();
    session
        .node_conn()
        .control_transaction("SAVEPOINT allowed")
        .unwrap();
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    let error = session
        .node_conn()
        .control_transaction("SAVEPOINT refused")
        .unwrap_err();
    assert_eq!(error.code, 1105);
    assert_eq!(error.message, "savepoint is not supported in pessimistic transactions when in-place constraint check is disabled");
    assert!(session.in_transaction());
    session.node_conn().control_transaction("ROLLBACK").unwrap();
}

#[test]
fn deferred_uniqueness_batch_scope_tombstones_and_successful_commit() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(943)).unwrap();
    rows(&mut session, "CREATE DATABASE deferred_scope");
    rows(&mut session, "USE deferred_scope");
    rows(
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, u INT UNIQUE)",
    );
    rows(&mut session, "INSERT INTO t VALUES (1,10),(2,20)");
    rows(
        &mut session,
        "SET tidb_constraint_check_in_place_pessimistic=OFF",
    );
    session
        .node_conn()
        .control_transaction("BEGIN OPTIMISTIC")
        .unwrap();
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    session.set_restricted_sql(true);
    session.set_restricted_sql(false);
    session.set_connection_id(0);
    session.set_connection_id(943);
    rows(&mut session, "DELETE FROM t WHERE id=1");
    rows(&mut session, "INSERT INTO t VALUES (1,10)");
    assert!(
        session.node().buffer.presume_not_exists_keys().is_empty(),
        "local tombstones prove absence"
    );
    rows(&mut session, "INSERT INTO t VALUES (3,30),(4,NULL)");
    rows(&mut session, "UPDATE t SET u=31 WHERE id=3");
    rows(&mut session, "INSERT IGNORE INTO t VALUES (5,20)");
    rows(&mut session, "UPDATE IGNORE t SET u=20 WHERE id=3");
    assert!(session.in_transaction());
    rows(
        &mut session,
        "SET tidb_constraint_check_in_place_pessimistic=ON",
    );
    rows(&mut session, "INSERT INTO t VALUES (6,60)");
    session.node_conn().control_transaction("COMMIT").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t ORDER BY id")),
        vec![
            vec!["1", "10"],
            vec!["2", "20"],
            vec!["3", "31"],
            vec!["4", "NULL"],
            vec!["6", "60"]
        ]
    );
    rows(&mut session, "SET autocommit=0");
    rows(
        &mut session,
        "SET tidb_constraint_check_in_place_pessimistic=OFF",
    );
    assert!(session
        .node_conn()
        .control_transaction("SAVEPOINT implicit")
        .is_err());
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    rows(&mut session, "SET autocommit=1");
}

/// Go builds a fresh `TxnCtx` for every autocommit statement and at every
/// BEGIN / COMMIT / ROLLBACK, so the rows of an `ON COMMIT DELETE ROWS` table
/// live exactly as long as the transaction that wrote them. This node kept
/// them across autocommit statements.
#[test]
fn global_temporary_rows_end_with_their_transaction_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1007)).unwrap();
    rows(
        &mut session,
        "CREATE GLOBAL TEMPORARY TABLE test.g (a INT) ON COMMIT DELETE ROWS",
    );
    session
        .node_conn()
        .execute_write("INSERT INTO test.g VALUES (1)")
        .unwrap();
    assert!(rows(&mut session, "SELECT a FROM test.g").is_empty());
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO test.g VALUES (2)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a FROM test.g")),
        [["2"]]
    );
    session.node_conn().control_transaction("COMMIT").unwrap();
    assert!(rows(&mut session, "SELECT a FROM test.g").is_empty());
    // A BEGIN the query path runs opens the session's own transaction, which
    // the next statement continues into just the same.
    rows(&mut session, "BEGIN");
    session
        .node_conn()
        .execute_write("INSERT INTO test.g VALUES (3)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT a FROM test.g")),
        [["3"]]
    );
    rows(&mut session, "COMMIT");
    assert!(rows(&mut session, "SELECT a FROM test.g").is_empty());
}

/// Go's `kv.CommitHook` records the cluster transaction's own start and
/// commit timestamps as `@@tidb_last_txn_info`, so a stale read AS OF that
/// commit_ts sees the commit. This node reported timestamps from a local
/// counter that no transaction committed at.
#[test]
fn last_txn_info_reports_the_cluster_commit_as_go_does() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1012)).unwrap();
    rows(&mut session, "CREATE TABLE test.t1 (a INT)");
    session
        .node_conn()
        .execute_write("INSERT INTO test.t1 VALUES (1)")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO test.t1 VALUES (2)")
        .unwrap();
    rows(
        &mut session,
        "SET @c = json_extract(@@tidb_last_txn_info, '$.commit_ts')",
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM test.t1 AS OF TIMESTAMP @c"
        )),
        [["2"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM test.t1 AS OF TIMESTAMP cast(@c AS UNSIGNED) - 1"
        )),
        [["1"]]
    );
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO test.t1 VALUES (3)")
        .unwrap();
    session.node_conn().control_transaction("COMMIT").unwrap();
    let info = displayed(rows(
        &mut session,
        "SELECT json_extract(@@tidb_last_txn_info, '$.start_ts') < \
         json_extract(@@tidb_last_txn_info, '$.commit_ts'), \
         json_extract(@@tidb_last_txn_info, '$.txn_commit_mode')",
    ));
    assert_eq!(info[0][0], "1", "{info:?}");
    rows(
        &mut session,
        "SET @c = json_extract(@@tidb_last_txn_info, '$.commit_ts')",
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM test.t1 AS OF TIMESTAMP @c"
        )),
        [["3"]]
    );
}

/// Go `setLastTxnInfoBeforeTxnEnd` runs at the start of `CommitTxn` and
/// `RollbackTxn` and records the start-only struct only for a transaction
/// that was activated (`TxnCtx.StartTS != 0`); the commit hook then
/// overwrites it when a commit really ran. Captured on the Go oracle:
/// `USE`, `SET @x` and `PREPARE` leave `@@tidb_last_txn_info` alone (empty on
/// a fresh connection), an autocommit UPDATE matching no row leaves the
/// start-only record (`commit_ts` 0, no commit mode), and `BEGIN; ROLLBACK`
/// leaves the start-only record too. This node recorded a two-phase commit
/// at counter timestamps for every statement that produced no rows.
#[test]
fn last_txn_info_is_recorded_only_by_the_transaction_that_ended() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1014)).unwrap();
    let info = |session: &mut Session| {
        displayed(rows(session, "SELECT @@tidb_last_txn_info"))[0][0].clone()
    };
    rows(&mut session, "USE test");
    rows(&mut session, "SET @x = 1");
    rows(&mut session, "PREPARE p FROM 'SELECT 1'");
    assert_eq!(info(&mut session), "", "statements that ran no transaction");
    // A DDL does run one (`NewTxnInStmt`): a start-only record.
    rows(&mut session, "CREATE TABLE test.t_info (a INT PRIMARY KEY)");

    session
        .node_conn()
        .execute_write("INSERT INTO test.t_info VALUES (1)")
        .unwrap();
    let committed = info(&mut session);
    assert!(committed.contains("\"txn_commit_mode\":\"2pc\""), "{committed}");
    rows(&mut session, "USE test");
    rows(&mut session, "SET @x = 2");
    rows(&mut session, "PREPARE p2 FROM 'SELECT 1'");
    assert_eq!(info(&mut session), committed, "no transaction, no record");

    session
        .node_conn()
        .execute_write("UPDATE test.t_info SET a = a WHERE a = 99")
        .unwrap();
    let empty_update = info(&mut session);
    assert_ne!(empty_update, committed);
    assert!(
        empty_update.contains("\"commit_ts\":0,\"txn_commit_mode\":\"\""),
        "{empty_update}"
    );

    session.node_conn().control_transaction("BEGIN").unwrap();
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    let rolled_back = info(&mut session);
    assert_ne!(rolled_back, empty_update);
    assert!(
        rolled_back.contains("\"commit_ts\":0,\"txn_commit_mode\":\"\""),
        "{rolled_back}"
    );

    // `SELECT 1` reads no table and never activates a transaction; a table
    // read does, and its end leaves the start-only record.
    rows(&mut session, "SELECT 1");
    assert_eq!(info(&mut session), rolled_back);
    rows(&mut session, "SELECT * FROM test.t_info");
    let read = info(&mut session);
    assert_ne!(read, rolled_back);
    assert!(
        read.contains("\"commit_ts\":0,\"txn_commit_mode\":\"\""),
        "{read}"
    );
}

/// Go's DDL, account and ANALYZE statements run under a session transaction
/// opened by `NewTxnInStmt` at a fresh TSO, and the autocommit `CommitTxn`
/// that ends them records its start-only `@@tidb_last_txn_info`. Captured on
/// the Go oracle: CREATE TABLE, ALTER TABLE, CREATE USER, GRANT, REVOKE,
/// DROP USER and ANALYZE each leave a newer start with `commit_ts` 0; a
/// local `CREATE TEMPORARY TABLE` returns before `NewTxnInStmt` and leaves
/// the record alone.
#[test]
fn new_txn_in_stmt_statements_record_a_fresh_start_only_txn_info() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1017)).unwrap();
    let start = |session: &mut Session| -> u64 {
        displayed(rows(
            session,
            "SELECT IF(@@tidb_last_txn_info = '', 0, cast(json_unquote(json_extract(\
             @@tidb_last_txn_info, '$.start_ts')) AS UNSIGNED))",
        ))[0][0]
            .parse()
            .unwrap_or(0)
    };
    let mut last = start(&mut session);
    assert_eq!(last, 0, "a fresh session has no record");
    for statement in [
        "CREATE TABLE test.t_new_txn (a INT PRIMARY KEY)",
        "ALTER TABLE test.t_new_txn ADD COLUMN b INT",
        "CREATE USER 'new_txn_u'@'%' IDENTIFIED BY ''",
    ] {
        session.node_conn().execute_write(statement).unwrap();
        let now = start(&mut session);
        assert!(now > last, "{statement} left start {now} after {last}");
        let info = displayed(rows(&mut session, "SELECT @@tidb_last_txn_info"))[0][0].clone();
        assert!(
            info.contains("\"commit_ts\":0,\"txn_commit_mode\":\"\""),
            "{statement}: {info}"
        );
        last = now;
    }
    session
        .node_conn()
        .execute_write("CREATE TEMPORARY TABLE test.tt_new_txn (a INT)")
        .unwrap();
    assert_eq!(start(&mut session), last, "a local temporary table is not one");
}

/// Go `PrepareTxnCtx` enters the `autocommit = 0` transaction with
/// `EnterNewTxnBeforeStmt` (`activeNow = false`): it stays pending, spending
/// no timestamp, until a statement activates it. Captured on the Go oracle:
/// with `autocommit = 0`, `SELECT 1`, `SET @a`, `SHOW TABLES` and `EXPLAIN
/// SELECT 1` leave `@@tidb_current_ts` at 0, a row a peer commits after
/// `SELECT 1` is visible to the next table read, and a row a peer commits
/// after a table read is not. This node opened the store transaction at the
/// entry of every statement.
#[test]
fn autocommit_off_transaction_is_pending_until_a_statement_activates_it() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1015)).unwrap();
    let mut peer = stack.factory.open_session(session_context(1016)).unwrap();
    rows(&mut session, "CREATE TABLE test.t_pending (a INT PRIMARY KEY)");
    let active = |session: &mut Session| {
        displayed(rows(session, "SELECT @@tidb_current_ts > 0"))[0][0].clone()
    };
    rows(&mut session, "SET autocommit = 0");
    for statement in [
        "SELECT 1",
        "SET @a = 1",
        // Go `buildShow` refuses SHOW TABLES with no current database.
        "SHOW TABLES FROM test",
        "EXPLAIN SELECT 1",
        "SELECT now()",
    ] {
        rows(&mut session, statement);
        assert_eq!(active(&mut session), "0", "after {statement}");
    }
    peer.node_conn()
        .execute_write("INSERT INTO test.t_pending VALUES (21)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM test.t_pending")),
        [["1"]],
        "the transaction is still pending, so it reads what the peer committed"
    );
    assert_eq!(active(&mut session), "1");
    // Go's `GetSession` hook for `tidb_current_ts` returns `FormatUint`, a
    // string like every system variable's value.
    let current_ts = rows(&mut session, "SELECT @@tidb_current_ts");
    assert_eq!(current_ts[0][0].kind(), tidb_datatype::DatumKind::String);
    peer.node_conn()
        .execute_write("INSERT INTO test.t_pending VALUES (22)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM test.t_pending")),
        [["1"]],
        "the activated transaction keeps its snapshot"
    );
    session.node_conn().control_transaction("COMMIT").unwrap();
    assert_eq!(active(&mut session), "0");
    // A pending transaction that never activates commits and rolls back
    // without taking a timestamp.
    rows(&mut session, "SELECT 1");
    session.node_conn().control_transaction("COMMIT").unwrap();
    rows(&mut session, "SELECT 1");
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    // DML activates at build, SAVEPOINT activates.
    session
        .node_conn()
        .execute_write("INSERT INTO test.t_pending VALUES (23)")
        .unwrap();
    assert_eq!(active(&mut session), "1");
    session.node_conn().control_transaction("COMMIT").unwrap();
    rows(&mut session, "SELECT 1");
    session
        .node_conn()
        .control_transaction("SAVEPOINT s1")
        .unwrap();
    assert_eq!(active(&mut session), "1");
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM test.t_pending")),
        [["3"]]
    );
}

/// Go master `TestSessionFallsBackFromDroppedResourceGroup`'s transaction
/// case (#72794): a group dropped while a session's transaction is open is
/// gone from the LATEST schema (`infoCache.GetLatest()`), so the next
/// statement of that transaction moves the session to `default`, warns, and
/// the transaction commits. The commit-group assertion drives Go's
/// `TxnResourceGroupChecker` failpoint, which has no Rust site.
#[test]
fn a_transaction_moves_off_a_dropped_resource_group_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut root = stack.factory.open_session(session_context(1017)).unwrap();
    let mut tk = stack.factory.open_session(session_context(1018)).unwrap();
    rows(&mut root, "use test");
    rows(&mut root, "create table t (id int primary key)");
    rows(&mut root, "create resource group rg3 ru_per_sec=1000");
    rows(&mut tk, "use test");
    rows(&mut tk, "set resource group rg3");
    rows(&mut tk, "begin pessimistic");
    assert!(rows(&mut tk, "select * from t").is_empty());
    rows(&mut root, "drop resource group rg3");
    rows(&mut tk, "insert into t values (1)");
    assert_eq!(
        displayed(rows(&mut tk, "show warnings")),
        [["Warning", "1105", "resource group rg3 no longer exists, the session now uses resource group default"]]
    );
    assert_eq!(displayed(rows(&mut tk, "select current_resource_group()")), [["default"]]);
    rows(&mut tk, "commit");
    assert_eq!(displayed(rows(&mut root, "select id from t")), [["1"]]);
}

/// Go's `SET GLOBAL` writes `mysql.global_variables` through an internal
/// session (`SetGlobalSysVar`), so it neither commits the issuing session's
/// transaction nor clears `@@tidb_current_ts`: `set global
/// tidb_external_ts=@@tidb_current_ts` inside a transaction reads that
/// transaction's start timestamp. The production node committed first.
#[test]
fn set_global_inside_a_transaction_keeps_the_transaction_open() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1025)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE sg (a INT)")
        .unwrap();
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_external_ts = 19980613")
        .unwrap();
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO sg VALUES (1)")
        .unwrap();
    let start = displayed(rows(&mut session, "SELECT @@tidb_current_ts"));
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_external_ts = @@tidb_current_ts")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT @@tidb_current_ts")),
        start
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT @@tidb_external_ts")),
        start
    );
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert!(rows(&mut session, "SELECT * FROM sg").is_empty());
}

/// Go `HandleNonTransactionalDML` runs every job with `se.ExecuteStmt`, so an
/// auto-committed job publishes before the next one reads: the second job
/// sees the row the first moved and moves both. The node ran the jobs inside
/// the BATCH statement's own buffer, where the second job's index lookup met
/// the first job's unpublished row and failed, and the failure discarded
/// every job's writes.
#[test]
fn non_transactional_jobs_each_commit_before_the_next_reads() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1030)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t (id INT, v INT, KEY (id))")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE t2 (id INT, v INT, KEY (id))")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t2 VALUES (1, 1), (2, 2), (4, 4)")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "BATCH ON test.t.id LIMIT 1 UPDATE t JOIN t2 ON t.id = t2.id SET t2.id = t2.id + 1"
        )),
        [["3", "all succeeded"]]
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t2 ORDER BY v")),
        [["4", "1"], ["4", "2"], ["4", "4"]]
    );
}

/// Go `getColDefaultValue` evaluates the clock words through `GetTimeValue`
/// for DATE/DATETIME/TIMESTAMP columns alone. The catalog loader refused any
/// column whose literal default spelled `CURRENT_TIMESTAMP`, so a VARCHAR
/// declaring that text vanished right after its CREATE, and a VARCHAR's
/// `'current_date'` would have been evaluated as the clock.
#[test]
fn string_columns_keep_clock_words_as_their_text_default() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1031)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write(
            "CREATE TABLE t (id INT, a VARCHAR(20) DEFAULT 'cUrrent_tImestamp', \
             b VARCHAR(20) DEFAULT 'current_date')",
        )
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t (id) VALUES (1)")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT id, a, b FROM t")),
        [["1", "cUrrent_tImestamp", "current_date"]]
    );
}

/// Go's `TableSampleExecutor` samples `GetSnapshot(TxnCtx.StartTS)`: the open
/// transaction's own pending writes are invisible to `TABLESAMPLE REGIONS`
/// (`TestTableSampleUnionScanIgnorePendingKV`).
#[test]
fn table_sample_ignores_the_transactions_pending_writes() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1102)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("SET @@global.tidb_scatter_region = 'table'")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tsp (a INT PRIMARY KEY)")
        .unwrap();
    rows(
        &mut session,
        "SPLIT TABLE tsp BETWEEN (0) AND (40000) REGIONS 4",
    );
    session
        .node_conn()
        .execute_write("INSERT INTO tsp VALUES (1), (1000), (10002)")
        .unwrap();
    let sampled = |session: &mut Session| -> Vec<String> {
        displayed(rows(session, "SELECT * FROM tsp TABLESAMPLE REGIONS()"))
            .into_iter()
            .map(|row| row[0].clone())
            .collect()
    };
    assert_eq!(sampled(&mut session), ["1", "10002"]);
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO tsp VALUES (20006), (50000)")
        .unwrap();
    assert_eq!(sampled(&mut session), ["1", "10002"]);
    session
        .node_conn()
        .execute_write("DELETE FROM tsp WHERE a = 1")
        .unwrap();
    assert_eq!(sampled(&mut session), ["1", "10002"]);
    session.node_conn().control_transaction("COMMIT").unwrap();
    assert_eq!(sampled(&mut session), ["1000", "10002", "20006", "50000"]);
    // The splits are store metadata: a DDL rebuilds the catalog from the
    // stored schema (`TestTableSampleNotSupportedPlanWarning`) and must not
    // forget them.
    session
        .node_conn()
        .execute_write("CREATE TABLE tsr (a INT PRIMARY KEY, b INT)")
        .unwrap();
    rows(
        &mut session,
        "SPLIT TABLE tsr BETWEEN (0) AND (10000) REGIONS 5",
    );
    session
        .node_conn()
        .execute_write("INSERT INTO tsr VALUES (1000, 1), (1001, 1), (2100, 2), (4500, 3)")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE INDEX idx_0 ON tsr (b)")
        .unwrap();
    let rows_after_ddl: Vec<String> = displayed(rows(
        &mut session,
        "SELECT a FROM tsr TABLESAMPLE REGIONS() ORDER BY a",
    ))
    .into_iter()
    .map(|row| row[0].clone())
    .collect();
    assert_eq!(rows_after_ddl, ["1000", "2100", "4500"]);
}

/// Go `handlePessimisticSelectForUpdate` drains the rows and takes the locks
/// inside `ExecStmt.Exec`, so a lock that fails after the drain is an `Exec`
/// error: `runStmt` finishes the statement with it (`FinishExecuteStmt`), and
/// the summary counts one execution with one error. The port dropped the
/// drained record set without closing it, so the statement never finished.
#[test]
fn failed_select_for_update_lock_finishes_the_statement() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1131)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = 1")
        .unwrap();
    rows(&mut session, "CREATE TABLE lock_summary (id int, v int)");
    rows(&mut session, "INSERT INTO lock_summary VALUES (1, 10)");
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT v FROM lock_summary WHERE v > 0 FOR UPDATE"
        )),
        [["10"]]
    );
    let mut contender = stack.factory.open_session(session_context(1132)).unwrap();
    contender.node_conn().execute_write("USE test").unwrap();
    contender
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    assert!(matches!(
        contender.node_conn().execute("SELECT v FROM lock_summary WHERE v > 0 FOR UPDATE NOWAIT"),
        Err(error) if error.code == 3572
    ));
    contender
        .node_conn()
        .control_transaction("ROLLBACK")
        .unwrap();
    session.node_conn().control_transaction("ROLLBACK").unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT exec_count, sum_errors FROM information_schema.statements_summary \
             WHERE digest_text LIKE '%lock_summary%for update nowait'",
        )),
        [["1", "1"]]
    );
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = DEFAULT")
        .unwrap();
}

/// Go's fast point-get plan accepts a locking read and sets `Lock` only when
/// `PessimisticLockEligible` (autocommit off, or a transaction open): an
/// autocommit `SELECT ... FOR UPDATE` on a primary key is a plain `Point_Get`
/// with no `SelectLock` above it, as is the IN-list form's `Batch_Point_Get`.
#[test]
fn autocommit_for_update_point_gets_have_no_select_lock() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1113)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE tfu (id INT PRIMARY KEY, x INT)")
        .unwrap();
    let operators = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| {
                row[0]
                    .trim_start_matches(['├', '└', '─', '│', ' '])
                    .split('_')
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect()
    };
    assert_eq!(
        operators(
            &mut session,
            "EXPLAIN SELECT * FROM tfu WHERE id = 1 FOR UPDATE"
        ),
        ["Point"]
    );
    assert_eq!(
        operators(
            &mut session,
            "EXPLAIN SELECT * FROM tfu WHERE id IN (1, 2) FOR UPDATE"
        ),
        ["Batch"]
    );
    // A non-point read keeps its SelectLock (a no-op at execution).
    assert_eq!(
        operators(
            &mut session,
            "EXPLAIN SELECT * FROM tfu WHERE x = 1 FOR UPDATE"
        )[0],
        "SelectLock"
    );
}

/// Inside a transaction a locking SELECT over a point key plans as the point
/// plan itself carrying `lock` (Go `PointGetPlan.Lock` /
/// `BatchPointGetPlan.Lock`), not as a `SelectLock` above it; a partitioned
/// table's integer-handle pin takes the same fast plan, outside a transaction
/// too. Expected rows: Go 1.27 on this branch (`TestZZ` probes `US9`, `US19`).
#[test]
fn locking_point_reads_in_a_transaction_plan_as_point_get_with_lock() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1121)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    for ddl in [
        "CREATE TABLE tlk (a INT PRIMARY KEY, b INT, c INT, UNIQUE KEY(b), KEY(c))",
        "CREATE TABLE tlp (a INT PRIMARY KEY, b INT) PARTITION BY HASH(a) PARTITIONS 3",
        "INSERT INTO tlk VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4)",
        "INSERT INTO tlp VALUES (1,1),(2,2),(3,3)",
    ] {
        session.node_conn().execute_write(ddl).unwrap();
    }
    let plan = |session: &mut Session, sql: &str| -> Vec<String> {
        displayed(rows(session, &format!("EXPLAIN FORMAT = 'verbose' {sql}")))
            .into_iter()
            .map(|row| format!("{} {} {} {} {}", row[0], row[1], row[2], row[4], row[5]))
            .collect()
    };
    // Autocommit: no lock, and the partitioned pin is already a fast plan.
    assert_eq!(
        plan(&mut session, "SELECT * FROM tlp WHERE a = 1 FOR UPDATE"),
        ["Point_Get_1 1.00 0.00 table:tlp, partition:p1 handle:1"]
    );
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    assert_eq!(
        plan(&mut session, "SELECT * FROM tlk WHERE a = 1 FOR UPDATE"),
        ["Point_Get_1 1.00 0.00 table:tlk handle:1, lock"]
    );
    assert_eq!(
        plan(&mut session, "SELECT * FROM tlk WHERE b = 1 FOR UPDATE"),
        ["Point_Get_1 1.00 0.00 table:tlk, index:b(b) lock"]
    );
    assert_eq!(
        plan(&mut session, "SELECT * FROM tlk WHERE a IN (1, 2) FOR UPDATE"),
        ["Batch_Point_Get_1 2.00 0.00 table:tlk handle:[1 2], keep order:false, desc:false, lock"]
    );
    assert_eq!(
        plan(&mut session, "SELECT * FROM tlp WHERE a = 1 FOR UPDATE"),
        ["Point_Get_1 1.00 0.00 table:tlp, partition:p1 handle:1, lock"]
    );
    // The lock is taken: a second session cannot lock the row without waiting.
    assert_eq!(
        displayed(rows(&mut session, "SELECT b FROM tlk WHERE a = 1 FOR UPDATE")),
        [["1"]]
    );
    let mut contender = stack.factory.open_session(session_context(1122)).unwrap();
    contender.node_conn().execute_write("USE test").unwrap();
    contender
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    assert!(
        matches!(
            contender.node_conn().execute("SELECT b FROM tlk WHERE a = 1 FOR UPDATE NOWAIT"),
            Err(error) if error.code == 3572
        ),
        "the point lock must be held"
    );
    // A prepared statement caches the fast plan: its second EXECUTE, with a
    // different key, locks that key too.
    session
        .node_conn()
        .execute_write("PREPARE lockp FROM 'SELECT b FROM tlk WHERE a = ? FOR UPDATE'")
        .unwrap();
    session.node_conn().execute_write("SET @lk = 3").unwrap();
    for key in [3, 2] {
        session
            .node_conn()
            .execute_write(&format!("SET @lk = {key}"))
            .unwrap();
        assert_eq!(
            displayed(rows(&mut session, "EXECUTE lockp USING @lk")),
            [[key.to_string()]]
        );
        assert!(
            matches!(
                contender.node_conn().execute(&format!(
                    "SELECT b FROM tlk WHERE a = {key} FOR UPDATE NOWAIT"
                )),
                Err(error) if error.code == 3572
            ),
            "EXECUTE must lock key {key}"
        );
    }
    // The same holds for a prepared locking read that is no point read.
    session
        .node_conn()
        .execute_write("PREPARE lockq FROM 'SELECT b FROM tlk WHERE c = ? FOR UPDATE'")
        .unwrap();
    session.node_conn().execute_write("SET @lc = 4").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "EXECUTE lockq USING @lc")),
        [["4"]]
    );
    assert!(
        matches!(
            contender.node_conn().execute("SELECT b FROM tlk WHERE a = 4 FOR UPDATE NOWAIT"),
            Err(error) if error.code == 3572
        ),
        "a prepared locking index read must lock its row"
    );
    contender
        .node_conn()
        .control_transaction("ROLLBACK")
        .unwrap();
    session.node_conn().control_transaction("ROLLBACK").unwrap();
}

/// The binding maintenance loop commits its GC through a session on its own
/// thread, waiting out a conflicting lock. On a default 2 MB thread stack that
/// commit overflowed at the first GC tick (300 s) of the `imdbload` corpus
/// topic, whose region-retry nesting this test does not reproduce; the topic
/// is the reproducer, and this pins the GC path end to end on the worker.
#[test]
fn binding_gc_commits_on_the_maintenance_thread() {
    if crate::domain::isolate_process_globals() {
        return;
    }
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(131)).unwrap();
    rows(
        &mut session,
        "INSERT INTO mysql.bind_info (original_sql, bind_sql, default_db, status, create_time, \
         update_time, charset, collation, source) VALUES ('select 1', 'select 1', 'test', \
         'deleted', '2000-01-01 00:00:00', '2000-01-01 00:00:00', 'utf8mb4', 'utf8mb4_bin', \
         'manual')",
    );
    // A second transaction holds the row's lock, so the GC's prewrite meets
    // it and runs the client's lock-resolution path, where the overflow was.
    let mut holder = stack.factory.open_session(session_context(133)).unwrap();
    holder
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    rows(
        &mut holder,
        "SELECT * FROM mysql.bind_info WHERE status = 'deleted' FOR UPDATE",
    );
    let bindings = Arc::clone(stack.factory.binding_handle().expect("binding cache"));
    let long = Duration::from_secs(3600);
    let worker = tidb_bindinfo::cluster_binding_seam::BindingReloader::spawn(
        bindings,
        [long, Duration::from_millis(1), long],
        move || long,
    )
    .expect("binding worker");
    std::thread::sleep(Duration::from_millis(300));
    holder.node_conn().control_transaction("COMMIT").unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let left = rows(
            &mut session,
            "SELECT count(*) FROM mysql.bind_info WHERE status = 'deleted'",
        );
        if left == vec![vec![Datum::Int(0)]] {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the maintenance thread never collected the deleted binding: {left:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(worker);
}

/// Go rolls back a transaction whose COMMIT fails: the client sees the
/// error and nothing it staged is published, not even by the next
/// statement (captured from Go on this branch). The failure owner is the
/// store's prewrite (a presumed-absent key); a republish of the connection's
/// catalog while the transaction is open does not fail its commit, as Go's
/// schema and statistics reloads do not.
#[test]
fn a_failed_commit_publishes_nothing_later() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1901)).unwrap();
    rows(&mut session, "CREATE DATABASE failed_commit");
    rows(&mut session, "USE failed_commit");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    rows(&mut session, "INSERT INTO t VALUES (1, 10)");
    rows(&mut session, "SET tidb_txn_mode = 'optimistic'");
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO t VALUES (1, 99)")
        .unwrap();
    let error = session
        .node_conn()
        .control_transaction("COMMIT")
        .unwrap_err();
    assert_eq!(error.code, 1062, "{error:?}");
    session
        .node_conn()
        .execute_write("INSERT INTO t VALUES (2, 20)")
        .unwrap();

    session.node_conn().control_transaction("BEGIN").unwrap();
    rows(&mut session, "SELECT * FROM t WHERE id = 1 FOR UPDATE");
    session
        .node_conn()
        .execute_write("INSERT INTO t VALUES (3, 30)")
        .unwrap();
    {
        // A republish of the connection's catalog lands while the
        // transaction is open, as a schema or statistics reload does.
        let shared = session.shared_catalog();
        let mut catalog = shared.lock().unwrap();
        assert!(catalog.set_database_placement_policy("failed_commit", None));
    }
    session.node_conn().control_transaction("COMMIT").unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SELECT * FROM t ORDER BY id")),
        vec![vec!["1", "10"], vec!["2", "20"], vec!["3", "30"]]
    );
}

/// With table locks disabled Go's preprocessor still resolves every named
/// table under its own schema, then answers OK with warning 1235
/// (captured from Go on this branch).
#[test]
fn disabled_lock_tables_resolves_every_table_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1903)).unwrap();
    rows(&mut session, "CREATE DATABASE lock_disabled");
    rows(&mut session, "USE lock_disabled");
    rows(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)");
    session
        .node_conn()
        .execute_write("LOCK TABLES mysql.user READ, t WRITE")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "SHOW WARNINGS")),
        vec![vec![
            "Warning",
            "1235",
            "LOCK TABLES is not supported. To enable this experimental feature, set 'enable-table-lock' in the configuration file."
        ]]
    );
    let error = session
        .node_conn()
        .execute_write("LOCK TABLES t READ, nosuch READ")
        .unwrap_err();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1146, "Table 'lock_disabled.nosuch' doesn't exist")
    );
}

/// Go's pessimistic READ-COMMITTED provider reads every statement at its
/// own fresh timestamp (`getStmtTS`), so a peer's commit after BEGIN is
/// visible to the next statement; repeatable read keeps the start
/// timestamp.
#[test]
fn read_committed_statements_see_commits_made_after_begin() {
    let (stack, _users) = cop_backed_stack();
    let mut writer = stack.factory.open_session(session_context(1906)).unwrap();
    rows(&mut writer, "CREATE DATABASE rc_reads");
    rows(&mut writer, "USE rc_reads");
    rows(&mut writer, "CREATE TABLE t (id INT PRIMARY KEY)");
    rows(&mut writer, "INSERT INTO t VALUES (1)");
    for (isolation, visible) in [("REPEATABLE-READ", "1"), ("READ-COMMITTED", "2")] {
        let mut reader = stack.factory.open_session(session_context(1907)).unwrap();
        rows(&mut reader, "USE rc_reads");
        rows(
            &mut reader,
            &format!("SET transaction_isolation = '{isolation}'"),
        );
        reader
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .unwrap();
        assert_eq!(
            displayed(rows(&mut reader, "SELECT COUNT(*) FROM t")),
            vec![vec!["1"]]
        );
        writer
            .node_conn()
            .execute_write("INSERT INTO t VALUES (2)")
            .unwrap();
        assert_eq!(
            displayed(rows(&mut reader, "SELECT COUNT(*) FROM t")),
            vec![vec![visible]],
            "{isolation}"
        );
        reader.node_conn().control_transaction("ROLLBACK").unwrap();
        writer
            .node_conn()
            .execute_write("DELETE FROM t WHERE id = 2")
            .unwrap();
    }
}

/// Go's pessimistic DML reads at `GetStmtForUpdateTS`, a fresh timestamp
/// at statement start: an UPDATE or DELETE after BEGIN acts on rows a peer
/// committed meanwhile (captured from Go on this branch).
#[test]
fn pessimistic_dml_acts_on_rows_committed_after_begin() {
    let (stack, _users) = cop_backed_stack();
    let mut a = stack.factory.open_session(session_context(1911)).unwrap();
    let mut b = stack.factory.open_session(session_context(1912)).unwrap();
    rows(&mut a, "CREATE DATABASE dml_current");
    rows(&mut a, "USE dml_current");
    rows(&mut b, "USE dml_current");
    rows(&mut a, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    rows(&mut a, "INSERT INTO t VALUES (1, 0)");
    a.node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    assert_eq!(
        displayed(rows(&mut a, "SELECT * FROM t")),
        vec![vec!["1", "0"]]
    );
    b.node_conn()
        .execute_write("INSERT INTO t VALUES (2, 0)")
        .unwrap();
    let updated = a
        .node_conn()
        .execute_write("UPDATE t SET v = 9 WHERE id > 0")
        .unwrap()
        .unwrap();
    assert_eq!(updated.affected_rows, 2);
    assert_eq!(
        displayed(rows(&mut a, "SELECT * FROM t")),
        vec![vec!["1", "9"], vec!["2", "9"]]
    );
    a.node_conn().control_transaction("COMMIT").unwrap();

    a.node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    rows(&mut a, "SELECT * FROM t");
    b.node_conn()
        .execute_write("INSERT INTO t VALUES (3, 0)")
        .unwrap();
    let deleted = a
        .node_conn()
        .execute_write("DELETE FROM t WHERE v = 0")
        .unwrap()
        .unwrap();
    assert_eq!(deleted.affected_rows, 1);
    a.node_conn().control_transaction("COMMIT").unwrap();
    assert_eq!(
        displayed(rows(&mut a, "SELECT * FROM t")),
        vec![vec!["1", "9"], vec!["2", "9"]]
    );
}

/// LOCK TABLES, UNLOCK TABLES and ADMIN CLEANUP TABLE LOCK run as Go's
/// persisted `ActionLockTable` / `ActionUnlockTable` jobs: the session's
/// earlier locks are released one table per step, each new lock goes
/// none -> pre-lock -> public (one more READ holder skips the pre-lock), an
/// unlock that changes nothing still queues a job without a version, and an
/// unlock job records no query (captured from Go on this branch).
#[test]
fn table_locks_run_as_persisted_jobs_like_go() {
    let previous = tidb_config::config_tree::config::get_global_config();
    tidb_config::config_tree::config::update_global(|config| config.enable_table_lock = true);
    let (stack, _users) = cop_backed_stack();
    let mut first = stack.factory.open_session(session_context(1950)).unwrap();
    let mut second = stack.factory.open_session(session_context(1951)).unwrap();
    rows(&mut first, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut first, "USE test");
    rows(&mut second, "USE test");
    rows(&mut first, "create table t1 (a int)");
    rows(&mut first, "create table t2 (a int)");
    let version = |session: &mut _| -> i64 {
        displayed(rows(session, "admin show ddl"))[0][0]
            .parse()
            .unwrap()
    };
    // (job id, db|table|type|state|status, query) of the latest job.
    let job = |session: &mut _| -> (String, String, String) {
        let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
        let query = displayed(rows(
            session,
            &format!("admin show ddl job queries {}", row[0]),
        ))
        .remove(0)
        .remove(0);
        (
            row[0].clone(),
            [1, 2, 3, 4, 11].map(|column| row[column].clone()).join("|"),
            query,
        )
    };
    let step = |session: &mut _, sql: &str| -> (i64, String, String, String) {
        let before = version(session);
        rows(session, sql);
        let (id, kind, query) = job(session);
        (version(session) - before, id, kind, query)
    };
    for (on_second, sql, delta, expected, query) in [
        (
            false,
            "lock tables t1 write, t2 read",
            4,
            "test|t2|lock table|public|synced",
            "lock tables t1 write, t2 read",
        ),
        // t1 and t2 are released first, then t1 is locked.
        (
            false,
            "lock tables t1 read",
            4,
            "test|t1|lock table|public|synced",
            "lock tables t1 read",
        ),
        // One more READ holder: no pre-lock.
        (
            true,
            "lock tables t1 read",
            1,
            "test|t1|lock table|public|synced",
            "lock tables t1 read",
        ),
        (
            false,
            "unlock tables",
            1,
            "test|t1|unlock table|none|synced",
            "",
        ),
        (
            true,
            "unlock tables",
            1,
            "test|t1|unlock table|none|synced",
            "",
        ),
        (
            false,
            "lock tables t1 write",
            2,
            "test|t1|lock table|public|synced",
            "lock tables t1 write",
        ),
        (
            true,
            "admin cleanup table lock t1",
            1,
            "test|t1|unlock table|none|synced",
            "",
        ),
        // The cleanup already released t1: the job changes nothing.
        (
            false,
            "unlock tables",
            0,
            "test|t1|unlock table|none|synced",
            "",
        ),
        (
            false,
            "lock tables t2 write local",
            2,
            "test|t2|lock table|public|synced",
            "lock tables t2 write local",
        ),
        // t1 holds no lock; t2's goes.
        (
            true,
            "admin cleanup table lock t1, t2",
            1,
            "test|t2|unlock table|none|synced",
            "",
        ),
    ] {
        let session = if on_second { &mut second } else { &mut first };
        let (got_delta, _, kind, got_query) = step(session, sql);
        assert_eq!(
            (got_delta, kind.as_str(), got_query.as_str()),
            (delta, expected, query),
            "{sql}"
        );
    }
    // The cleanup did not touch this session's own list: its UNLOCK still
    // queues a job, and holding nothing, the next one queues none.
    let (delta, last, kind, _) = step(&mut first, "unlock tables");
    assert_eq!((delta, kind.as_str()), (0, "test|t2|unlock table|none|synced"));
    let (delta, id, _, _) = step(&mut first, "unlock tables");
    assert_eq!((delta, id), (0, last));
    // Another session's WRITE lock refuses before any job.
    step(&mut first, "lock tables t1 write");
    let (_, last, _, _) = step(&mut second, "select 1");
    let error = second
        .node_conn()
        .execute_write("lock tables t1 read")
        .unwrap_err();
    assert_eq!(error.code, 8020, "{}", error.message);
    assert_eq!(job(&mut second).0, last);
    rows(&mut first, "unlock tables");
    tidb_config::config_tree::config::store_global_config((*previous).clone());
}

/// The regions Go's executors and session open around the client spans: a
/// table reader's `TableReaderExecutor.Open` and the `table scan` event of
/// its every `Next`, an INSERT's event and its `table.AddRecord` /
/// `index.Create` per row (siblings: the index writes take the executor's
/// ctx), and COMMIT / ROLLBACK compiled and run as any statement --
/// `SimpleExec` under `handleNoDelayExecutor`, then `finishStmt`'s
/// `session.CommitTxn`, whose `doCommitWithRetry` runs only over a valid
/// transaction.
#[test]
fn trace_records_go_executor_and_transaction_regions() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(155)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.tr (id INT PRIMARY KEY, k INT, v INT, KEY ik (k))",
    );
    rows(
        &mut session,
        "INSERT INTO test.tr VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (5, 5, 6)",
    );
    let compile = [
        "trace",
        "  └─session.ExecuteStmt",
        "    ├─executor.Compile",
        "    │ ├─planner.Preprocess",
        "    │ └─planner.Optimize",
        "    └─session.runStmt",
    ];
    let tree = trace_tree(
        &mut session,
        "SELECT * FROM test.tr IGNORE INDEX (ik) WHERE v > 1",
    );
    assert_eq!(
        tree[5..9],
        [
            "  │ └─session.runStmt",
            "  │   └─TableReaderExecutor.Open",
            "  │     └─distsql.Select",
            "  │       ├─copr.buildCopTasks",
        ],
        "{tree:#?}"
    );
    let logged = displayed(rows(
        &mut session,
        "TRACE FORMAT = 'log' SELECT * FROM test.tr IGNORE INDEX (ik) WHERE v > 1",
    ));
    let scans = logged
        .iter()
        .filter(|row| row[1].starts_with("table scan "))
        .map(|row| (row[1].as_str(), row[3].as_str()))
        .collect::<Vec<_>>();
    assert_eq!(
        scans,
        [
            (
                "table scan table: tr, range: [[-inf,+inf]]",
                "*executor.TableReaderExecutor.Next"
            ),
            (
                "table scan table: tr, range: [[-inf,+inf]]",
                "*executor.TableReaderExecutor.Next"
            ),
        ]
    );

    let tree = trace_tree(
        &mut session,
        "INSERT INTO test.tr VALUES (7, 7, 7), (8, 8, 8)",
    );
    let mut expected = compile.to_vec();
    expected.extend([
        "      ├─executor.handleNoDelayExecutor",
        "      │ └─*executor.InsertExec.Next",
        "      │   ├─table.AddRecord",
        "      │   ├─index.Create",
        "      │   ├─table.AddRecord",
        "      │   └─index.Create",
        "      └─session.CommitTxn",
    ]);
    assert_eq!(tree[..expected.len()], expected[..], "{tree:#?}");
    let logged = displayed(rows(
        &mut session,
        "TRACE FORMAT = 'log' INSERT INTO test.tr VALUES (9, 9, 9)",
    ));
    assert!(
        logged
            .iter()
            .any(|row| row[1] == "insert 1 rows into table `tr`"
                && row[3] == "*executor.InsertExec.Next"),
        "{logged:#?}"
    );

    let simple = [
        "      ├─executor.handleNoDelayExecutor",
        "      │ └─*executor.SimpleExec.Next",
    ];
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    session
        .node_conn()
        .execute_write("UPDATE test.tr SET v = v + 1 WHERE id = 5")
        .unwrap();
    let mut expected = compile.to_vec();
    expected.extend(simple);
    expected.extend([
        "      └─session.CommitTxn",
        "        └─session.doCommitWithRetry",
        "          └─tikvTxn.Commit",
        "            ├─twoPhaseCommitter.prewriteMutations",
        "            │ └─regionRequest.SendReqCtx",
        "            ├─TiKVStore.getTimestampWithRetry",
        "            └─twoPhaseCommitter.commitMutations",
        "              └─regionRequest.SendReqCtx",
    ]);
    assert_eq!(trace_tree(&mut session, "COMMIT"), expected);
    assert!(!session.in_transaction());

    session.node_conn().control_transaction("BEGIN").unwrap();
    rows(&mut session, "SELECT * FROM test.tr WHERE id = 1");
    let mut expected = compile.to_vec();
    expected.extend(simple);
    expected.push("      └─session.CommitTxn");
    assert_eq!(trace_tree(&mut session, "ROLLBACK"), expected);
    assert!(!session.in_transaction());
}

/// Go plans a `SelectLock` over the source of a single-table pessimistic
/// UPDATE or DELETE (`buildUpdate` / `buildDelete`, `buildSelectLock`) and
/// over a `SELECT ... FOR UPDATE`, and `SelectLockExec.Next` locks the rows it
/// read when its child is drained: AFTER the chunk's `updateRecord`s, in the
/// `Next` call that returns no rows, not at the end of the statement. Each
/// tree was captured with `TRACE` from the Go server on TiKV in a
/// `BEGIN PESSIMISTIC`: the lock request sits under the second
/// `SelectLockExec.Next`, after every `updateRecord`; a point update locks in
/// its first `PointGetExecutor.Next` and reads EOF after its `updateRecord`.
#[test]
fn a_non_point_pessimistic_write_locks_inside_select_lock_next() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(2001))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.sllock (id int primary key, v int, w int, key iv(v))",
    );
    rows(
        &mut session,
        "INSERT INTO test.sllock VALUES (1, 2, 0), (2, 2, 0), (3, 5, 0), (4, 2, 0)",
    );
    let select = "*executor.SelectLockExec.Next";
    let update = "executor.updateRecord";
    let point = "*executor.PointGetExecutor.Next";
    let lock = "lock request";
    for (sql, expected) in [
        // Three rows changed.
        (
            "UPDATE test.sllock SET w = 1 WHERE v = 2",
            vec![select, update, update, update, select, lock],
        ),
        // Three rows matched and left as they were: the lock is still the
        // statement's `SelectLockExec` one, and nothing is left to lock.
        (
            "UPDATE test.sllock SET w = 0 WHERE v = 2",
            vec![select, update, update, update, select, lock],
        ),
        // A LIMIT sits above the lock only where Go cannot push it under.
        (
            "UPDATE test.sllock SET w = 1 WHERE v = 2 ORDER BY id LIMIT 2",
            vec![select, update, update, select, lock],
        ),
        ("UPDATE test.sllock SET w = 1 WHERE v = 99", vec![select]),
        ("DELETE FROM test.sllock WHERE v = 2", vec![select, select, lock]),
        (
            "SELECT * FROM test.sllock WHERE v = 2 FOR UPDATE",
            vec![select, select, lock],
        ),
        (
            "UPDATE test.sllock SET w = 1 WHERE id = 1",
            vec![point, lock, update, point],
        ),
    ] {
        session
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin");
        let tree = trace_tree(&mut session, sql);
        session
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
        assert_eq!(lock_markers(&tree), expected, "{sql}\n{tree:#?}");
    }
}

/// The rows a single-table pessimistic UPDATE, DELETE or locking read holds
/// when its transaction stays open are the rows `SelectLockExec` saw: below
/// the LIMIT where Go pushes it under the lock, every matched row of an
/// UPDATE that changed nothing, and none when nothing matched. Each case was
/// captured live from the Go server on TiKV, probing every id with
/// `FOR UPDATE NOWAIT` from a second transaction.
#[test]
fn non_point_pessimistic_statements_hold_the_rows_go_locks() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut setup = factory
        .open_session(session_context(2002))
        .expect("session opens");
    rows(
        &mut setup,
        "CREATE TABLE test.slheld (id int primary key, v int, w int, key iv(v))",
    );
    rows(
        &mut setup,
        "INSERT INTO test.slheld VALUES (1, 2, 0), (2, 2, 0), (3, 5, 0), (4, 2, 0)",
    );
    let mut writer = factory
        .open_session(session_context(2003))
        .expect("session opens");
    let mut contender = factory
        .open_session(session_context(2004))
        .expect("session opens");
    for (statement, held) in [
        (
            "UPDATE test.slheld SET w = 1 WHERE v = 2 ORDER BY id LIMIT 1",
            vec![1],
        ),
        ("UPDATE test.slheld SET w = 0 WHERE v = 2", vec![1, 2, 4]),
        (
            "DELETE FROM test.slheld WHERE v = 2 ORDER BY id DESC LIMIT 2",
            vec![2, 4],
        ),
        (
            "SELECT * FROM test.slheld WHERE v = 2 ORDER BY id LIMIT 1 FOR UPDATE",
            vec![1],
        ),
        ("UPDATE test.slheld SET w = 1 WHERE v = 99", vec![]),
    ] {
        writer
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin writer");
        if statement.starts_with("SELECT") {
            writer
                .node_conn()
                .execute(statement)
                .expect("statement runs");
        } else {
            writer
                .node_conn()
                .execute_write(statement)
                .expect("statement runs");
        }
        let mut probed = Vec::new();
        for id in 1..=4 {
            contender
                .node_conn()
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin contender");
            let refused = contender
                .node_conn()
                .execute(&format!(
                    "SELECT id FROM test.slheld WHERE id = {id} FOR UPDATE NOWAIT"
                ))
                .err();
            if let Some(error) = refused {
                assert_eq!(error.code, 3572, "{statement} id {id}: {}", error.message);
                probed.push(id);
            }
            contender
                .node_conn()
                .control_transaction("ROLLBACK")
                .expect("rollback");
        }
        writer
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
        assert_eq!(probed, held, "{statement}");
    }
}

/// Go's lock comes after the chunk's `updateRecord`s, so a row error preempts
/// a lock wait: with another transaction holding a matched row, the UPDATE
/// that violates a unique key or fails a cast reports that error at once,
/// and only the one that writes cleanly waits and times out with 1205.
/// Captured live from the Go server on TiKV.
#[test]
fn a_row_error_in_a_pessimistic_update_preempts_its_lock_wait() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut holder = factory
        .open_session(session_context(2005))
        .expect("session opens");
    rows(
        &mut holder,
        "CREATE TABLE test.slerr (id int primary key, uk int, w int, unique key uq(uk), key iw(w))",
    );
    rows(
        &mut holder,
        "INSERT INTO test.slerr VALUES (1, 10, 0), (2, 20, 0), (3, 30, 0)",
    );
    holder
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin holder");
    rows(
        &mut holder,
        "SELECT * FROM test.slerr WHERE id = 2 FOR UPDATE",
    );
    let mut contender = factory
        .open_session(session_context(2006))
        .expect("session opens");
    contender
        .node_conn()
        .execute_write("SET SESSION innodb_lock_wait_timeout = 1")
        .expect("set lock wait");
    for (statement, code) in [
        ("UPDATE test.slerr SET uk = 10 WHERE id > 1", 1062),
        ("UPDATE test.slerr SET w = 'x' WHERE id > 1", 1292),
        ("UPDATE test.slerr SET w = 1 WHERE id > 1", 1205),
    ] {
        contender
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin contender");
        let error = match contender.node_conn().execute_write(statement) {
            Err(error) => error,
            Ok(_) => panic!("{statement} must fail"),
        };
        contender
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
        assert_eq!(error.code, code, "{statement}: {}", error.message);
    }
    holder
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback");
}

/// A non-point UPDATE that waits on a row and finds it changed when the lock
/// is granted re-runs at the new `for_update_ts` (Go
/// `handlePessimisticLockError` -> `OnStmtRetry`): `SET w = w + 1` computes
/// from the committed 100, not the 0 it first read. Captured live from the Go
/// server on TiKV: the rows end 1, 101, 1 and the statement affects 3.
/// Go `ExecStmt.handlePessimisticLockError` re-runs `buildExecutor` over the
/// compiled `Plan` and `StmtCtx.ResetForRetry` truncates the statement's
/// warnings: a warning the compile raised (an inapplicable index hint) is
/// gone after a retry, since nothing compiles again. Captured from a Go
/// testkit probe of the same statements: the hint's 1176 without a
/// conflict, no warning after the retry.
#[test]
fn a_pessimistic_retry_does_not_compile_the_statement_again() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut session = factory
        .open_session(session_context(2013))
        .expect("session opens");
    let mut other = factory
        .open_session(session_context(2014))
        .expect("session opens");
    session.node_conn().execute_write("USE test").expect("use").expect("use");
    other.node_conn().execute_write("USE test").expect("use").expect("use");
    rows(&mut session, "CREATE TABLE rwarn (id int primary key, v int, key iv(v))");
    rows(&mut session, "INSERT INTO rwarn VALUES (1, 10)");
    for (statement, retry) in [
        ("UPDATE /*+ use_index(rwarn, nonexist) */ rwarn SET v = 12 WHERE id = 1", false),
        ("UPDATE /*+ use_index(rwarn, nonexist) */ rwarn SET v = 12 WHERE id = 1", true),
        ("SELECT /*+ use_index(rwarn, nonexist) */ * FROM rwarn WHERE id = 1 FOR UPDATE", false),
        ("SELECT /*+ use_index(rwarn, nonexist) */ * FROM rwarn WHERE id = 1 FOR UPDATE", true),
    ] {
        session
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin");
        if retry {
            other
                .node_conn()
                .execute_write("UPDATE rwarn SET v = v + 1 WHERE id = 1")
                .expect("conflicting write")
                .expect("write result");
        }
        if statement.starts_with("SELECT") {
            rows(&mut session, statement);
        } else {
            session
                .node_conn()
                .execute_write(statement)
                .expect("the update runs")
                .expect("write result");
        }
        let expected: &[[&str; 3]] = if retry {
            &[]
        } else {
            &[["Warning", "1176", "Key 'nonexist' doesn't exist in table 'rwarn'"]]
        };
        assert_eq!(displayed(rows(&mut session, "SHOW WARNINGS")), expected, "{statement} retry={retry}");
        session
            .node_conn()
            .control_transaction("ROLLBACK")
            .expect("rollback");
    }
}

#[test]
fn a_non_point_pessimistic_update_retries_after_a_conflict_found_by_its_lock() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut holder = factory
        .open_session(session_context(2007))
        .expect("session opens");
    rows(
        &mut holder,
        "CREATE TABLE test.slretry (id int primary key, v int, w int, key iv(v))",
    );
    rows(
        &mut holder,
        "INSERT INTO test.slretry VALUES (1, 2, 0), (2, 2, 0), (3, 5, 0), (4, 2, 0)",
    );
    holder
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin holder");
    rows(
        &mut holder,
        "SELECT * FROM test.slretry WHERE id = 2 FOR UPDATE",
    );
    let affected = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut contender = factory
                .open_session(session_context(2008))
                .expect("session opens");
            contender
                .node_conn()
                .execute_write("SET SESSION innodb_lock_wait_timeout = 10")
                .expect("set lock wait");
            contender
                .node_conn()
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin contender");
            let outcome = contender
                .node_conn()
                .execute_write("UPDATE test.slretry SET w = w + 1 WHERE v = 2")
                .expect("the update waits for the holder, then retries")
                .expect("write result");
            contender
                .node_conn()
                .control_transaction("COMMIT")
                .expect("commit");
            outcome.affected_rows
        });
        std::thread::sleep(std::time::Duration::from_millis(700));
        holder
            .node_conn()
            .execute_write("UPDATE test.slretry SET w = 100 WHERE id = 2")
            .expect("holder writes");
        holder
            .node_conn()
            .control_transaction("COMMIT")
            .expect("commit holder");
        contender.join().expect("contender thread")
    });
    assert_eq!(affected, 3);
    assert_eq!(
        displayed(rows(&mut holder, "SELECT id, w FROM test.slretry ORDER BY id")),
        [["1", "1"], ["2", "101"], ["3", "0"], ["4", "1"]]
    );
}

#[test]
fn a_step_session_statement_commits_with_its_step_as_go() {
    use tidb_ddl_build::session_pool::DdlSessionPool;
    use tidb_store_driver_txn::cluster_storage::MutationBuffer;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(184)).unwrap();
    for sql in [
        "create table test.st (a int primary key, b varchar(10))",
        "insert into test.st values (1, 'one')",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    let pool = crate::ddl_session_pool::NodeDdlSessionPool::default();
    pool.bind(&stack.factory);
    let domain = &stack.factory;
    let options = || tidb_domain_core::session_transaction::restricted_transaction_options(&domain.global_vars);
    let insert = "INSERT INTO test.st VALUES (%?, %?)";
    let select = "select a, b from test.st order by a";

    // Staged, then dropped: nothing is published.
    let start_ts = domain.transactions.fresh_timestamp().unwrap();
    let buffer = MutationBuffer::new();
    exec_in_step_txn(&pool, 
        start_ts,
        &buffer,
        insert,
        &[tidb_sqlescape::SqlArg::Signed(2), tidb_sqlescape::SqlArg::String(b"two")],
    )
    .unwrap();
    assert!(!buffer.is_empty());
    assert_eq!(displayed(rows(&mut session, select)), [["1", "one"]]);
    drop(buffer);
    assert_eq!(displayed(rows(&mut session, select)), [["1", "one"]]);

    // Staged, then committed with the step.
    let start_ts = domain.transactions.fresh_timestamp().unwrap();
    let buffer = MutationBuffer::new();
    exec_in_step_txn(&pool, 
        start_ts,
        &buffer,
        insert,
        &[tidb_sqlescape::SqlArg::Signed(2), tidb_sqlescape::SqlArg::String(b"two")],
    )
    .unwrap();
    domain
        .transactions
        .commit(&buffer, Some(start_ts), "", options())
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, select)),
        [["1", "one"], ["2", "two"]]
    );

    // A delete staged by the step session commits with the step.
    let start_ts = domain.transactions.fresh_timestamp().unwrap();
    let buffer = MutationBuffer::new();
    exec_in_step_txn(&pool, start_ts, &buffer, "DELETE FROM test.st WHERE a = 2", &[])
        .unwrap();
    assert!(!buffer.is_empty());
    domain
        .transactions
        .commit(&buffer, Some(start_ts), "", options())
        .unwrap();
    assert_eq!(displayed(rows(&mut session, select)), [["1", "one"]]);
    exec_in_step_txn(&pool, 
        domain.transactions.fresh_timestamp().unwrap(),
        &MutationBuffer::new(),
        "SELECT 1",
        &[],
    )
    .unwrap();
    let start_ts = domain.transactions.fresh_timestamp().unwrap();
    let buffer = MutationBuffer::new();
    exec_in_step_txn(&pool, 
        start_ts,
        &buffer,
        insert,
        &[tidb_sqlescape::SqlArg::Signed(2), tidb_sqlescape::SqlArg::String(b"two")],
    )
    .unwrap();
    domain
        .transactions
        .commit(&buffer, Some(start_ts), "", options())
        .unwrap();

    // A duplicate staged by the optimistic statement is refused when the
    // step commits it.
    let start_ts = domain.transactions.fresh_timestamp().unwrap();
    let buffer = MutationBuffer::new();
    let staged = exec_in_step_txn(&pool, 
        start_ts,
        &buffer,
        insert,
        &[tidb_sqlescape::SqlArg::Signed(1), tidb_sqlescape::SqlArg::String(b"dup")],
    );
    let committed = staged.map(|_| {
        domain
            .transactions
            .commit(&buffer, Some(start_ts), "", options())
    });
    let refused = match committed {
        Err(error) => error.code,
        Ok(Err(error)) => error.code,
        Ok(Ok(_)) => panic!("the duplicate committed"),
    };
    assert_eq!(refused, 1062);
    assert_eq!(
        displayed(rows(&mut session, select)),
        [["1", "one"], ["2", "two"]]
    );
}

/// Go `w.sess` is ONE session for the whole step: every statement the step
/// runs (`tidb_ddl::job_txn::StepSession::execute`) stages into the step's
/// buffer, and none of them loses an earlier one's rows -- a second session
/// would `BEGIN` by committing the leftover buffer, which the step refuses.
#[test]
fn a_step_sessions_statements_all_commit_with_the_step_as_go() {
    use tidb_store_driver_txn::cluster_storage::MutationBuffer;
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(186)).unwrap();
    session
        .node_conn()
        .execute_write("create table test.st2 (a int primary key)")
        .unwrap();
    let pool = Arc::new(crate::ddl_session_pool::NodeDdlSessionPool::default());
    pool.bind(&stack.factory);
    let domain = &stack.factory;
    let start_ts = domain.transactions.fresh_timestamp().unwrap();
    let buffer = MutationBuffer::new();
    {
        let step = tidb_ddl::job_txn::StepSession {
            pool: pool.clone(),
            buffer: buffer.clone(),
            sql: Default::default(),
            start_mode: tidb_ddl::StartMode::Normal,
            step_ctx: Default::default(),
        };
        for a in [1, 2, 3] {
            step.execute(
                start_ts,
                "INSERT INTO test.st2 VALUES (%?)",
                "test",
                &[tidb_sqlescape::SqlArg::Signed(a)],
            )
            .unwrap();
        }
    }
    domain
        .transactions
        .commit(
            &buffer,
            Some(start_ts),
            "",
            tidb_domain_core::session_transaction::restricted_transaction_options(&domain.global_vars),
        )
        .unwrap();
    assert_eq!(
        displayed(rows(&mut session, "select a from test.st2 order by a")),
        [["1"], ["2"], ["3"]]
    );
}

/// Go `jobScheduler.loadAndDeliverJobs`: a job runs on a worker of its own,
/// and `runningJobs.checkRunnable` keeps only jobs on the same schema
/// objects apart. An ALTER waiting on a reader's metadata lock does not hold
/// up a CREATE TABLE of another table; it finishes once the reader commits.
#[test]
fn a_job_waiting_on_metadata_lock_does_not_block_other_tables_ddl_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut reader = stack.factory.open_session(session_context(194)).unwrap();
    let mut probe = stack.factory.open_session(session_context(195)).unwrap();
    rows(&mut reader, "create table test.sc1 (a int)");
    rows(&mut reader, "begin");
    rows(&mut reader, "select * from test.sc1");
    let alter = std::thread::spawn({
        let factory = Arc::clone(&stack.factory);
        move || {
            let mut writer = factory.open_session(session_context(196)).unwrap();
            rows(&mut writer, "alter table test.sc1 add column b int");
        }
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while displayed(rows(&mut probe, "select count(*) from mysql.tidb_mdl_info")) != [["1"]] {
        assert!(
            std::time::Instant::now() < deadline,
            "the ALTER never waited on the reader"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let (created_tx, created_rx) = std::sync::mpsc::channel();
    let create = std::thread::spawn({
        let factory = Arc::clone(&stack.factory);
        move || {
            let mut other = factory.open_session(session_context(197)).unwrap();
            rows(&mut other, "create table test.sc2 (a int)");
            created_tx.send(()).unwrap();
        }
    });
    let created = created_rx.recv_timeout(Duration::from_secs(20));
    // The ALTER still waits on the reader.
    assert_eq!(
        displayed(rows(&mut probe, "select count(*) from mysql.tidb_mdl_info")),
        [["1"]]
    );
    // A second job on the same table waits behind the first, unstarted
    // (`runningJobs.checkRunnable`).
    let second = std::thread::spawn({
        let factory = Arc::clone(&stack.factory);
        move || {
            let mut writer = factory.open_session(session_context(198)).unwrap();
            rows(&mut writer, "alter table test.sc1 add column c int");
        }
    });
    let second_job = loop {
        let jobs = displayed(rows(
            &mut probe,
            "select job_id, json_extract(cast(job_meta as char), '$.schema_state') from mysql.tidb_ddl_job order by job_id",
        ));
        if jobs.len() == 2 {
            break jobs[1].clone();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the second ALTER never queued: {jobs:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    std::thread::sleep(Duration::from_millis(500));
    let second_state = displayed(rows(
        &mut probe,
        &format!(
            "select json_extract(cast(job_meta as char), '$.schema_state') from mysql.tidb_ddl_job where job_id = {}",
            second_job[0]
        ),
    ));
    assert_eq!(second_state, [["0"]], "the second job on the table started");
    rows(&mut reader, "commit");
    alter.join().unwrap();
    second.join().unwrap();
    assert!(
        created.is_ok(),
        "CREATE TABLE of another table waited for the blocked ALTER"
    );
    create.join().unwrap();
    assert_eq!(
        displayed(rows(
            &mut probe,
            "select column_name from information_schema.columns where table_schema = 'test' and table_name = 'sc1' order by ordinal_position"
        )),
        [["a"], ["b"], ["c"]]
    );
}

/// Go `session.GetAdvisoryLock`: each lock is an internal session holding
/// `INSERT INTO mysql.advisory_locks` in a pessimistic transaction, so the
/// lock is visible to every node's sessions; a second session sees it used
/// and cannot take it, and the lock goes with its holder's session.
#[test]
fn advisory_locks_are_internal_sessions_holding_a_row_lock_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut first = stack.factory.open_session(session_context(204)).unwrap();
    let mut second = stack.factory.open_session(session_context(205)).unwrap();
    assert_eq!(
        displayed(rows(&mut first, "select get_lock('cluster-lock', 0)")),
        [["1"]]
    );
    assert_eq!(
        displayed(rows(
            &mut second,
            "select is_used_lock('CLUSTER-LOCK'), get_lock('cluster-lock', 0)"
        )),
        [["1", "0"]]
    );
    // The lock is the row lock of `mysql.advisory_locks`' key.
    rows(&mut second, "begin pessimistic");
    let error = second
        .node_conn()
        .execute("select * from mysql.advisory_locks where lock_name = 'cluster-lock' for update nowait")
        .map(|_| ())
        .unwrap_err();
    assert_eq!(error.code, 3572, "{}", error.message);
    rows(&mut second, "rollback");
    drop(first);
    assert_eq!(
        displayed(rows(&mut second, "select get_lock('cluster-lock', 0)")),
        [["1"]]
    );
    assert_eq!(
        displayed(rows(&mut second, "select release_lock('cluster-lock')")),
        [["1"]]
    );
    assert_eq!(
        displayed(rows(&mut second, "select is_used_lock('cluster-lock')")),
        [["NULL"]]
    );
}

/// Go `PessimisticRRTxnContextProvider.getForUpdateTs`: a statement the plan
/// lets keep its timestamp (`optimizeForNotFetchingLatestTS`) reads at the
/// transaction's current for-update timestamp, which a `SELECT ... FOR
/// UPDATE` advanced past a peer's commit. The `INSERT ... ON DUPLICATE KEY
/// UPDATE` after it therefore sees the row the lock returned and updates it;
/// read at the start timestamp, it inserted and failed its commit's NotExist
/// assertion (what `FLUSH STATS_DELTA`'s `UpdateStatsMeta` hit against the
/// DDL statistics subscriber's row).
#[test]
fn a_kept_for_update_ts_is_the_transactions_not_its_start_as_go() {
    let (stack, _users) = cop_backed_stack();
    let mut s1 = stack.factory.open_session(session_context(301)).unwrap();
    let mut s2 = stack.factory.open_session(session_context(302)).unwrap();
    rows(&mut s1, "CREATE TABLE test.kept (id BIGINT PRIMARY KEY CLUSTERED, v INT)");
    rows(&mut s1, "BEGIN PESSIMISTIC");
    rows(&mut s1, "SELECT 1");
    rows(&mut s2, "INSERT INTO test.kept VALUES (1, 10)");
    assert_eq!(
        displayed(rows(&mut s1, "SELECT * FROM test.kept WHERE id IN (1) FOR UPDATE")),
        [["1", "10"]]
    );
    rows(
        &mut s1,
        "INSERT INTO test.kept VALUES (1, 20) ON DUPLICATE KEY UPDATE v = v + 1",
    );
    rows(&mut s1, "COMMIT");
    assert_eq!(displayed(rows(&mut s2, "SELECT * FROM test.kept")), [["1", "11"]]);
}

/// Go `doCommit`'s `CommitDetailCtxKey`: client-go fills the committing
/// statement's `CommitDetails` -- its write keys and the prewrite and commit
/// phases -- and the statement summary reports them. An explicit
/// transaction's details belong to its COMMIT, not to the writes before it.
#[test]
fn a_commit_reports_its_commit_details_in_the_statement_summary() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1191)).unwrap();
    session.node_conn().execute_write("USE test").unwrap();
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = 1")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE commit_details_auto (a int)")
        .unwrap();
    session
        .node_conn()
        .execute_write("CREATE TABLE commit_details_txn (a int)")
        .unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO commit_details_auto VALUES (1), (2), (3)")
        .unwrap();
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO commit_details_txn VALUES (1), (2)")
        .unwrap();
    session.node_conn().control_transaction("COMMIT").unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT max_write_keys, max_prewrite_time > 0, max_commit_time > 0 \
             FROM information_schema.statements_summary \
             WHERE digest_text LIKE 'insert into `commit_details_auto`%'",
        )),
        [["3", "1", "1"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT max_write_keys FROM information_schema.statements_summary \
             WHERE digest_text LIKE 'insert into `commit_details_txn`%'",
        )),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT max_write_keys, max_commit_time > 0 FROM information_schema.statements_summary \
             WHERE digest_text = 'commit'",
        )),
        [["2", "1"]]
    );
    session
        .node_conn()
        .execute_write("SET GLOBAL tidb_enable_stmt_summary = DEFAULT")
        .unwrap();
}

/// Go `TestHistoryReadInTxn` in executor_test.go: issue 34529's restricted
/// read at ts2 must restore the issuer's original snapshot/schema.
#[test]
fn test_history_read_in_txn() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(1792)).unwrap();
    for sql in [
        "USE test",
        "INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20060102-15:04:05 -0700', 'All versions after safe point can be accessed. (DO NOT EDIT)')",
        "CREATE TABLE his_t0 (id INT PRIMARY KEY, v INT)",
        "INSERT INTO his_t0 VALUES (1,10)",
        "BEGIN",
        "SELECT * FROM his_t0",
    ] {
        rows(&mut session, sql);
    }
    let ts1 = session.current_tso().value() as u64;
    rows(&mut session, "ROLLBACK");
    rows(&mut session, "CREATE TABLE his_t1 (id INT PRIMARY KEY, v INT)");
    rows(&mut session, "UPDATE his_t0 SET v=11");
    rows(&mut session, "BEGIN");
    rows(&mut session, "SELECT * FROM his_t0");
    let ts2 = session.current_tso().value() as u64;
    rows(&mut session, "ROLLBACK");
    rows(&mut session, "UPDATE his_t0 SET v=12");
    let options = [
        Arc::new(tidb_sqlexec::exec_option_use_cur_session) as tidb_sqlexec::OptionFuncAlias,
        tidb_sqlexec::exec_option_with_snapshot(ts2),
    ];
    for isolation in ["none", "optimistic", "REPEATABLE-READ", "READ-COMMITTED"] {
        for set_snapshot_before_txn in [false, true] {
            rows(&mut session, "ROLLBACK");
            rows(&mut session, "SET tidb_snapshot=''");
            if set_snapshot_before_txn || isolation == "none" {
                rows(&mut session, &format!("SET tidb_snapshot='{ts1}'"));
            }
            if isolation == "optimistic" {
                rows(&mut session, "BEGIN OPTIMISTIC");
            } else if isolation != "none" {
                rows(&mut session, &format!("SET tx_isolation='{isolation}'"));
                rows(&mut session, "BEGIN PESSIMISTIC");
            }
            if !set_snapshot_before_txn && isolation != "none" {
                rows(&mut session, &format!("SET tidb_snapshot='{ts1}'"));
            }
            // Go useCurrentSession restores SnapshotInfoschema beside
            // SnapshotTS. Inspect that retained schema directly.
            let schema = &session.snapshot_schema.as_ref().unwrap().1;
            assert!(schema.table_in("test", "his_t0").is_some());
            assert!(schema.table_in("test", "his_t1").is_none());
            let result = session
                .execute_restricted_sql(
                    &tidb_sqlexec::BackgroundContext,
                    &options,
                    "select * from his_t0 where id=1",
                    &[],
                )
                .unwrap();
            assert_eq!(displayed(result.0), [["1", "11"]]);
            assert_eq!(session.vars.snapshot_ts(), ts1);
            // Go useCurrentSession restores SnapshotInfoschema beside
            // SnapshotTS. Inspect that retained schema directly.
            let schema = &session.snapshot_schema.as_ref().unwrap().1;
            assert!(schema.table_in("test", "his_t0").is_some());
            assert!(schema.table_in("test", "his_t1").is_none());
            assert_eq!(
                displayed(rows(&mut session, "select * from his_t0 where id=1")),
                [["1", "10"]]
            );
            rows(&mut session, &format!("SET tidb_snapshot='{ts1}'"));
            assert!(session
                .execute_restricted_sql(
                    &tidb_sqlexec::BackgroundContext,
                    &options,
                    "select %?, %?",
                    &[1_i64.into()],
                )
                .is_err());
            assert_eq!(session.vars.snapshot_ts(), ts1);
            // Go useCurrentSession's clean clears the system-variable text
            // before restoring SnapshotTS, even when ParseWithParams fails.
            assert_eq!(session.vars.get_system(tidb_vardef::tidb_vars::TIDB_SNAPSHOT).unwrap(), "");
            assert!(session
                .execute_restricted_sql(
                    &tidb_sqlexec::BackgroundContext,
                    &options,
                    "select * from absent_r01",
                    &[]
                )
                .is_err());
            assert_eq!(session.vars.snapshot_ts(), ts1);
            assert_eq!(
                displayed(rows(&mut session, "select * from his_t0 where id=1")),
                [["1", "10"]]
            );
            rows(&mut session, "SET tidb_snapshot=''");
            assert!(session.snapshot_schema.is_none());
            assert!(rows(&mut session, "SELECT * FROM his_t1").is_empty());
            assert_eq!(
                displayed(rows(&mut session, "select * from his_t0 where id=1")),
                [["1", "12"]]
            );
        }
    }
}

/// Go `nontransactionaltest` `TestNonTransactionalWithCheckConstraint`: what
/// `checkConstraint` refuses -- a snapshot or pending `tx_read_ts` read, weak
/// consistency, a transaction, batch-dml, LIMIT / ORDER BY, the prepared
/// protocol, and a statement with no table to shard. A refused statement
/// leaves `tx_read_ts` for the next statement (Go master 1fdec7a1737).
#[test]
fn test_non_transactional_with_check_constraint() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(938)).unwrap();
    let err = |tk: &mut Session, sql: &str| -> (u16, String) {
        let error = tk.node_conn().execute(sql).err().unwrap_or_else(|| panic!("{sql} is refused"));
        (error.code, error.message)
    };
    let check_fn = |tk: &mut Session| {
        assert_eq!(displayed(rows(tk, "select count(*) from t")), [["100"]]);
        assert_eq!(displayed(rows(tk, "select count(*) from t1")), [["0"]]);
    };
    rows(&mut tk, "use test");
    rows(&mut tk, "drop table if exists t, t1");
    rows(&mut tk, "create table t(a int, b int, key(a))");
    rows(&mut tk, "create table t1(a int, b int, key(a))");
    // For mocked tikv, safe point is not initialized, we manually insert it
    // for snapshot to use.
    rows(
        &mut tk,
        "INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20060102-15:04:05.000 -0700', '')",
    );
    rows(&mut tk, "set @@tidb_max_chunk_size=35");
    rows(&mut tk, "set @a=now(6)");
    for i in 0..100 {
        rows(&mut tk, &format!("insert into t values ({i}, {})", i * 2));
    }
    let refused_dml = [
        "batch on a limit 10 insert into t1 select * from t",
        "batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b",
        "batch on a limit 10 delete from t",
    ];
    rows(&mut tk, "set @@tidb_snapshot=@a");
    for sql in refused_dml {
        err(&mut tk, sql);
    }
    rows(&mut tk, "set @@tidb_snapshot=''");
    check_fn(&mut tk);

    rows(&mut tk, "set transaction read only as of timestamp @a");
    let (_, message) = err(&mut tk, "batch on a limit 10 update t set b = b + 100");
    assert!(message.contains("tx_read_ts"), "{message}");
    assert_eq!(displayed(rows(&mut tk, "select count(*) from t")), [["0"]]);
    assert_eq!(displayed(rows(&mut tk, "select sum(b) from t")), [["9900"]]);

    rows(&mut tk, "set @@tidb_read_consistency=weak");
    for sql in refused_dml {
        err(&mut tk, sql);
    }
    rows(&mut tk, "set @@tidb_read_consistency=strict");
    check_fn(&mut tk);

    rows(&mut tk, "set autocommit=0");
    for sql in refused_dml {
        err(&mut tk, sql);
    }
    tk.node_conn().control_transaction("commit").unwrap();
    rows(&mut tk, "set autocommit=1");
    check_fn(&mut tk);

    tk.node_conn().control_transaction("begin").unwrap();
    for sql in refused_dml {
        err(&mut tk, sql);
    }
    tk.node_conn().control_transaction("commit").unwrap();
    check_fn(&mut tk);

    rows(&mut tk, "SET GLOBAL tidb_enable_batch_dml = 1");
    rows(&mut tk, "SET tidb_batch_insert = 1");
    rows(&mut tk, "SET tidb_dml_batch_size = 1");
    for sql in refused_dml {
        err(&mut tk, sql);
    }
    rows(&mut tk, "SET GLOBAL tidb_enable_batch_dml = 0");
    rows(&mut tk, "SET tidb_batch_insert = 0");
    rows(&mut tk, "SET tidb_dml_batch_size = 0");
    check_fn(&mut tk);

    for sql in [
        "batch on a limit 10 insert into t1 select * from t limit 10",
        "batch on a limit 10 insert into t1 select * from t limit 10 on duplicate key update t1.b=t.b",
        "batch on a limit 10 delete from t limit 10",
    ] {
        assert_eq!(err(&mut tk, sql).1, "Non-transactional statements don't support limit");
    }
    check_fn(&mut tk);

    for sql in [
        "batch on a limit 10 insert into t1 select * from t order by a",
        "batch on a limit 10 insert into t1 select * from t order by a on duplicate key update t1.b=t.b",
        "batch on a limit 10 delete from t order by a",
    ] {
        assert_eq!(err(&mut tk, sql).1, "Non-transactional statements don't support order by");
    }
    check_fn(&mut tk);

    for sql in [
        "prepare nt FROM 'batch limit 1 insert into t1 select * from t'",
        "prepare nt FROM 'batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b'",
        "prepare nt FROM 'batch limit 1 delete from t'",
    ] {
        assert_eq!(
            err(&mut tk, sql),
            (1295, "This command is not supported in the prepared statement protocol yet".to_owned())
        );
    }

    assert_eq!(err(&mut tk, "batch limit 1 insert into t select 1, 1").1, "table reference is nil");
    assert_eq!(
        err(&mut tk, "batch limit 1 insert into t select * from (select 1, 2) tmp").1,
        "Non-transactional DML, table name not found in join"
    );
}

/// Go `TestPanicOnStmtRollbackKilledTxn` (`pkg/session/test/txn/txn_test.go`):
/// a statement rollback reconciles the buffer's footprint with a kill
/// pending, keeps the kill reason, and leaves later writes under the quota.
#[test]
fn test_panic_on_stmt_rollback_killed_txn() {
    use tidb_sqlkiller::KillSignal;
    use tidb_txnkv::Key;
    let (stack, _users) = cop_backed_stack();
    for signal in [
        KillSignal::UnspecifiedKillSignal,
        KillSignal::QueryInterrupted,
        KillSignal::QueryMemoryExceeded,
    ] {
        let mut session = stack.factory.open_session(session_context(84)).expect("session opens");
        rows(&mut session, "use test");
        rows(&mut session, "drop table if exists t_stmt_rollback");
        rows(&mut session, "create table t_stmt_rollback(id int primary key, v mediumblob)");
        rows(&mut session, "begin pessimistic");
        rows(&mut session, "insert into t_stmt_rollback values(1, 'first statement')");
        let buf = session.node.as_ref().unwrap().txn.buffer().clone();
        let killer = session.sql_killer();
        let footprint = session.mem_db_footprint().expect("the footprint tracker");
        let mem = || i64::try_from(buf.memory_footprint()).unwrap();

        // This value belongs to the current statement, while the SQL insert above
        // has already been released into the transaction's previous statements.
        let savepoint = buf.checkpoint();
        let staged_key = Key::from(b"stmt-rollback-value".to_vec());
        buf.set(staged_key.clone(), b"discard me".to_vec()).unwrap();
        assert_eq!(mem(), footprint.bytes_consumed());

        // Flags-only writes grow the ART node arena without notifying the memory
        // tracker through the value arena. Cleanup must account for that positive
        // delta even when a kill signal is pending.
        for i in 0..1024 {
            let key = format!("stmt-rollback-flags-{i:04}-{}", "x".repeat(128));
            buf.mark_presume_key_not_exists(&Key::from(key.into_bytes()));
        }
        assert!(mem() > footprint.bytes_consumed(), "{} {}", mem(), footprint.bytes_consumed());
        if signal != KillSignal::UnspecifiedKillSignal {
            killer.send_kill_signal(signal);
        }
        // The session tracker's quota action must not fire on the cleanup.
        let session_tracker = Arc::clone(session.session_memory.session_tracker());
        let limit = session_tracker.get_bytes_limit();
        session_tracker.set_bytes_limit(1);
        session_tracker.set_action_on_exceed(Some(Arc::new(tidb_memory::PanicOnExceed::new(
            Arc::clone(&killer),
        ))));
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| buf.restore(savepoint)))
            .expect("the statement rollback does not panic");
        session_tracker.set_bytes_limit(limit);
        assert_eq!(killer.get_kill_signal(), signal);
        assert_eq!(mem(), footprint.bytes_consumed());
        assert_eq!(buf.get(&staged_key), None);

        // Reset only when starting the next command. Statement rollback must
        // preserve the kill reason and keep tracking the surviving transaction.
        killer.reset();
        let before = footprint.bytes_consumed();
        rows(&mut session, "insert into t_stmt_rollback values(2, repeat('x', 131072))");
        assert!(footprint.bytes_consumed() > before);
        assert_eq!(mem(), footprint.bytes_consumed());

        // A later write must still enforce the quota; cleanup must not leave the
        // memory hook disabled for the remainder of this explicit transaction.
        let savepoint = buf.checkpoint();
        session_tracker.set_bytes_limit(session_tracker.bytes_consumed() + 4096);
        session_tracker.set_action_on_exceed(Some(Arc::new(tidb_memory::PanicOnExceed::new(
            Arc::clone(&killer),
        ))));
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = buf.set(staged_key.clone(), vec![b'x'; 1 << 20]);
        }));
        r.expect_err("the write past the quota panics");
        assert_eq!(killer.get_kill_signal(), KillSignal::QueryMemoryExceeded);
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| buf.restore(savepoint)))
            .expect("the statement rollback does not panic");
        assert_eq!(mem(), footprint.bytes_consumed());
        session_tracker.set_bytes_limit(limit);
        session_tracker.set_action_on_exceed(None);
        killer.reset();
        rows(&mut session, "commit");
        assert_eq!(
            displayed(rows(&mut session, "select id, length(v) from t_stmt_rollback order by id")),
            [["1", "15"], ["2", "131072"]]
        );
    }
}

/// Go realtikv pessimistic_test.go's rows from 754bede38a0: an unchanged
/// REPLACE locks the physical row, in a partitioned table too, under READ
/// COMMITTED and REPEATABLE READ.
#[test]
fn test_unchanged_replace_locks_the_physical_row() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(5301)).unwrap();
    let mut tk2 = stack.factory.open_session(session_context(5302)).unwrap();
    rows(&mut tk, "use test");
    rows(&mut tk2, "use test");
    rows(&mut tk, "create table unchanged (id int)");
    for isolation in ["READ COMMITTED", "REPEATABLE READ"] {
        for partition in ["", " partition by range(id) (partition p0 values less than (1000), partition p1 values less than (maxvalue))"] {
            rows(&mut tk, "drop table unchanged");
            rows(&mut tk, &format!("create table unchanged (id bool zerofill primary key clustered){partition}"));
            rows(&mut tk, "insert into unchanged values (false), (true)");
            rows(&mut tk, &format!("set session transaction isolation level {isolation}"));
            rows(&mut tk, "begin pessimistic");
            rows(&mut tk, "replace into unchanged values (false)");
            assert_eq!(displayed(rows(&mut tk, "select row_count()")), [["1"]], "{isolation}{partition}");
            rows(&mut tk2, "begin pessimistic");
            let error = tk2
                .node_conn()
                .execute("select * from unchanged where id = 0 for update nowait")
                .map(|_| ())
                .unwrap_err();
            assert_eq!(error.code, 3572, "{isolation}{partition}: {}", error.message);
            rows(&mut tk2, "rollback");
            // Go's realtikv test then has an autocommit UPDATE wait out
            // innodb_lock_wait_timeout (1205); that answer is TiKV's, not
            // the embedded store's.
            rows(&mut tk, "rollback");
            rows(&mut tk2, "update unchanged set id = id + 19 where id = '' or id >= -1598653439");
            assert_eq!(displayed(rows(&mut tk2, "select id from unchanged order by id")), [["19"], ["20"]]);
        }
    }
}
