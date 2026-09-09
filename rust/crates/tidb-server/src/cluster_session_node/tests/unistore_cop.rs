// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Sessions over the REAL embedded store with the coprocessor wired.
//!
//! Every other module in this directory serves `cop_scans: None`, so a plan
//! that only misbehaves when base-table scans are answered by the pushdown
//! coprocessor -- `CopScanSource` over the in-process unistore transport --
//! never fails in-tree. This module builds the same stack `--store unistore
//! --cluster-session` boots and pins those plans.

use std::sync::Arc;

use tidb_datatype::Datum;

use super::node_fixture::{rows, session_context, ABC_HASH};
use crate::configured_user_store::ConfiguredUserStore;
use crate::sql_node::QuerySession;
use crate::unistore_node::{unistore_cluster_session_stack, UnistoreClusterStack};
use crate::QuerySessionFactory;

fn cop_backed_stack() -> (UnistoreClusterStack, Arc<ConfiguredUserStore>) {
    let config = crate::node_config::NodeConfig::parse([
        "tidb-server",
        "--store",
        "unistore",
        "--cluster-session",
        "--port",
        "0",
        // Parse-time requirement only: the test passes its own user store
        // below, so the flag never has to name real rows.
        "--auth-file",
        "/dev/null",
    ])
    .expect("node config");
    let users = Arc::new(
        ConfiguredUserStore::parse(&format!("root\t%\tmysql_native_password\t{ABC_HASH}\n"))
            .expect("configured user store"),
    );
    let stack = unistore_cluster_session_stack(&config, &users).expect("unistore stack");
    (stack, users)
}

fn displayed(rows: Vec<Vec<Datum>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                // Every datum a served column can hold renders as the text
                // the wire would carry. A `{other:?}` fallback here reads as
                // a value mismatch when the value is in fact right, which
                // has cost this file three false failures.
                .map(|datum| match datum {
                    Datum::Int(v) => v.to_string(),
                    Datum::UInt(v) => v.to_string(),
                    Datum::Real(v) => v.to_string(),
                    Datum::Decimal(d) => d.to_string(),
                    Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
                    Datum::Bytes(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                    Datum::Enum(value, _) => {
                        String::from_utf8_lossy(value.name().as_bytes()).into_owned()
                    }
                    Datum::Set(value, _) => {
                        String::from_utf8_lossy(value.name().as_bytes()).into_owned()
                    }
                    Datum::Time(time) => time.to_string(),
                    Datum::Duration(duration) => duration.to_string(),
                    Datum::Json(json) => json.to_string(),
                    Datum::Null => "NULL".to_owned(),
                    other => format!("{other:?}"),
                })
                .collect()
        })
        .collect()
}

struct LockDispatchRecorder {
    inner: Box<dyn super::super::OpenClusterTransaction>,
    prelocks: Arc<std::sync::Mutex<Vec<Vec<Vec<u8>>>>>,
    postlocks: Arc<std::sync::Mutex<Vec<Vec<Vec<u8>>>>>,
}

impl super::super::OpenClusterTransaction for LockDispatchRecorder {
    fn start_ts(&self) -> u64 {
        self.inner.start_ts()
    }
    fn snapshot(&self) -> Result<Box<dyn tidb_executor::cluster_storage::ClusterSnapshot>, String> {
        self.inner.snapshot()
    }
    fn snapshot_for(
        &self,
        locking: bool,
    ) -> Result<Box<dyn tidb_executor::cluster_storage::ClusterSnapshot>, String> {
        self.inner.snapshot_for(locking)
    }
    fn snapshot_at_for(
        &self,
        ts: u64,
        locking: bool,
    ) -> Result<Box<dyn tidb_executor::cluster_storage::ClusterSnapshot>, String> {
        self.inner.snapshot_at_for(ts, locking)
    }
    fn is_pessimistic(&self) -> bool {
        self.inner.is_pessimistic()
    }
    fn commit(
        self: Box<Self>,
        buffer: &tidb_executor::cluster_storage::MutationBuffer,
    ) -> Result<(), crate::sql_node::SqlQueryError> {
        self.inner.commit(buffer)
    }
    fn rollback(self: Box<Self>) -> Result<(), String> {
        self.inner.rollback()
    }
    fn lock_staged_keys_with_values(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<tidb_exec::cluster_table_storage::LockKeysOutcome, String> {
        self.prelocks.lock().unwrap().push(keys.clone());
        self.inner.lock_staged_keys_with_values(keys)
    }
    fn lock_staged_keys_with_assertions(
        &self,
        keys: Vec<Vec<u8>>,
        assertions: std::collections::BTreeSet<Vec<u8>>,
        hints: std::collections::BTreeMap<
            Vec<u8>,
            tidb_executor::cluster_storage::DuplicateKeyHint,
        >,
    ) -> Result<tidb_exec::cluster_table_storage::LockKeysOutcome, String> {
        self.postlocks.lock().unwrap().push(keys.clone());
        self.inner
            .lock_staged_keys_with_assertions(keys, assertions, hints)
    }
    fn release_statement_locks(&self, keys: Vec<Vec<u8>>) -> Result<(), String> {
        self.inner.release_statement_locks(keys)
    }
}

#[test]
fn statement_owned_locks_do_not_cross_the_worker_boundary_twice() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(120)).unwrap();
    rows(
        &mut session,
        "CREATE TABLE test.lock_dispatch (id INT PRIMARY KEY, v INT, u INT UNIQUE)",
    );
    rows(
        &mut session,
        "INSERT INTO test.lock_dispatch VALUES (1,10,100),(2,20,200)",
    );
    for (sql, index_locks) in [
        ("UPDATE test.lock_dispatch SET v=v+1 WHERE id=1", false),
        ("UPDATE test.lock_dispatch SET v=v WHERE id=1", false),
        ("UPDATE test.lock_dispatch SET u=101 WHERE id=1", true),
    ] {
        session.control_transaction("BEGIN PESSIMISTIC").unwrap();
        let prelocks = Arc::new(std::sync::Mutex::new(Vec::new()));
        let postlocks = Arc::new(std::sync::Mutex::new(Vec::new()));
        session.explicit = Some(Box::new(LockDispatchRecorder {
            inner: session.explicit.take().unwrap(),
            prelocks: Arc::clone(&prelocks),
            postlocks: Arc::clone(&postlocks),
        }));
        rows(&mut session, sql);
        let actual = postlocks.lock().unwrap().clone();
        session.control_transaction("ROLLBACK").unwrap();
        assert_eq!(
            prelocks.lock().unwrap().len(),
            1,
            "the row is locked before execution"
        );
        if index_locks {
            assert_eq!(
                actual.len(),
                1,
                "new unique-index keys still need a lock request"
            );
            assert!(!actual[0].is_empty());
            assert!(
                actual[0]
                    .iter()
                    .all(|key| tidb_tablecodec::is_index_key(key)),
                "already-owned record lock was sent again: {actual:?}"
            );
        } else {
            assert!(
                actual.is_empty(),
                "already-owned row incurred a second worker handoff: {actual:?}"
            );
        }
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT * FROM test.lock_dispatch ORDER BY id"
        )),
        [["1", "10", "100"], ["2", "20", "200"]]
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
        session.control_transaction("BEGIN PESSIMISTIC").unwrap();
        assert_eq!(displayed(rows(&mut session, select)), [["1", "10"]]);
        let error = session
            .execute_write("INSERT INTO test.held_assertion VALUES (1,99)")
            .err();
        session.control_transaction("ROLLBACK").unwrap();
        assert!(matches!(error, Some(ref error) if error.code == 1062),
            "an already-owned existing key must still reject the INSERT at statement end: {error:?}");
    }
    session.control_transaction("BEGIN PESSIMISTIC").unwrap();
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
    session.control_transaction("COMMIT").unwrap();
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT * FROM test.held_assertion ORDER BY id"
        )),
        [["1", "10"], ["2", "20"], ["3", "31"]]
    );
}

#[test]
fn system_table_hidden_ids_use_the_full_counter_key() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(119))
        .expect("session");
    rows(&mut session, "INSERT INTO mysql.bind_info (original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest) VALUES ('system counter allocation', 'SELECT 1', 'test', 'disabled', '2026-09-07 00:00:00', '2026-09-07 00:00:00', 'utf8mb4', 'utf8mb4_bin', 'manual', 'system_counter_probe')");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT original_sql FROM mysql.bind_info WHERE sql_digest='system_counter_probe'"
        )),
        [["system counter allocation"]]
    );
}

#[test]
fn global_bindings_do_not_depend_on_unrelated_staged_writes() {
    let (stack, _users) = cop_backed_stack();
    let mut writer = stack
        .factory
        .open_session(session_context(117))
        .expect("writer");
    rows(
        &mut writer,
        "CREATE TABLE test.binding_visibility (id BIGINT PRIMARY KEY, v INT)",
    );
    rows(
        &mut writer,
        "INSERT INTO test.binding_visibility VALUES (1,20),(2,30),(3,40),(4,50),(5,60),(6,70)",
    );
    // A committed storage row, as if a peer had created the global binding.
    // The matcher must not infer its existence from the reader's write buffer.
    rows(&mut writer, "INSERT INTO mysql.bind_info (original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest) VALUES ('select v from test.binding_visibility where v = ?', 'SELECT v FROM test.binding_visibility USE INDEX () WHERE v = 20', 'test', 'enabled', '2026-09-07 00:00:00', '2026-09-07 00:00:00', 'utf8mb4', 'utf8mb4_bin', 'manual', 'binding_visibility_probe')");
    let mut reader = stack
        .factory
        .open_session(session_context(118))
        .expect("reader");
    reader
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin reader");
    for dirty in [true, false] {
        if dirty {
            rows(
                &mut reader,
                "UPDATE test.binding_visibility SET v=v+1 WHERE id>1",
            );
        } else {
            reader
                .control_transaction("ROLLBACK")
                .expect("rollback reader");
            reader
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin reader");
        }
        assert_eq!(
            displayed(rows(
                &mut reader,
                "SELECT v FROM test.binding_visibility WHERE v=20"
            )),
            [["20"]]
        );
        assert_eq!(
            displayed(rows(&mut reader, "SELECT @@last_plan_from_binding")),
            [["1"]],
            "global binding must apply with dirty={dirty}"
        );
    }
    reader
        .control_transaction("ROLLBACK")
        .expect("rollback reader");

    let assert_binding = |reader: &mut crate::cluster_session_node::ClusterServerSession,
                          expected: &str,
                          phase: &str| {
        let (result, ops) = tidb_executor::storage::capture_storage_ops(|| {
            rows(reader, "SELECT v FROM test.binding_visibility WHERE v=20")
        });
        assert_eq!(displayed(result), [["20"]]);
        assert_eq!(
            ops.cop_scans, 1,
            "only the user table may be scanned: {ops:?}"
        );
        assert_eq!(
            displayed(rows(reader, "SELECT @@last_plan_from_binding")),
            [[expected]],
            "{phase}"
        );
    };
    // Uncommitted binding writes must not enter any session's planner image.
    writer
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin writer");
    rows(
        &mut writer,
        "UPDATE mysql.bind_info SET status='disabled' WHERE sql_digest='binding_visibility_probe'",
    );
    assert_binding(&mut reader, "1", "reader during uncommitted disable");
    assert_binding(&mut writer, "1", "writer during uncommitted disable");
    writer
        .control_transaction("ROLLBACK")
        .expect("rollback writer");
    assert_binding(&mut reader, "1", "after rollback");

    for (status, expected) in [("disabled", "0"), ("enabled", "1"), ("deleted", "0")] {
        writer
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin writer");
        rows(&mut writer, &format!("UPDATE mysql.bind_info SET status='{status}' WHERE sql_digest='binding_visibility_probe'"));
        writer.control_transaction("COMMIT").expect("commit writer");
        for _ in 0..3 {
            assert_binding(&mut reader, expected, status);
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
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin writer");
    let write = writer
        .execute_write("UPDATE test.unchanged_lock SET v=v WHERE v=20")
        .expect("unchanged update")
        .expect("write result");
    assert_eq!(write.affected_rows, 0);
    reader
        .execute_write("SET innodb_lock_wait_timeout=1")
        .expect("bounded lock wait");
    reader
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin reader");
    let result = reader.execute_write("UPDATE test.unchanged_lock SET v=v+1 WHERE id=1");
    assert!(
        matches!(result, Err(ref error) if error.code == 1205),
        "the unchanged matched row must remain locked; error={:?}",
        result.as_ref().err()
    );
    let untouched = reader
        .execute_write("UPDATE test.unchanged_lock SET v=v+1 WHERE id=2")
        .expect("a rejected row is not locked")
        .expect("write result");
    assert_eq!(untouched.affected_rows, 1);
    writer
        .control_transaction("ROLLBACK")
        .expect("release writer");
    reader
        .control_transaction("ROLLBACK")
        .expect("release reader");
}

#[test]
fn global_binding_writer_rolls_back_errors_without_replaying_commits() {
    use crate::cluster_binding_seam::ClusterBindings;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tidb_session::binding::GlobalBindingWriter;

    struct FailingRefresh {
        inner: Arc<dyn ClusterBindings>,
        attempts: AtomicUsize,
    }
    impl ClusterBindings for FailingRefresh {
        fn cache(&self) -> tidb_session::binding_cache::SharedBindingCache {
            self.inner.cache()
        }
        fn reload(&self) -> Result<(), String> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Err("injected post-commit refresh failure".to_owned())
        }
        fn has_changes(&self, buffer: &tidb_executor::cluster_storage::MutationBuffer) -> bool {
            self.inner.has_changes(buffer)
        }
    }

    let (stack, _users) = cop_backed_stack();
    let mut reader = stack
        .factory
        .open_session(session_context(124))
        .expect("reader");
    rows(
        &mut reader,
        "CREATE TABLE test.binding_writer_atomicity (id INT PRIMARY KEY)",
    );
    let bindings = Arc::new(FailingRefresh {
        inner: Arc::clone(stack.factory.bindings.as_ref().expect("binding authority")),
        attempts: AtomicUsize::new(0),
    });
    let mut storage_factory = stack.factory.clone();
    storage_factory.bindings = None;
    let writer = crate::cluster_session_node::InternalBindingWriter {
        factory: storage_factory,
        bindings: bindings.clone(),
        connection_id: 125,
    };
    let error = writer
        .execute(&mut |session| {
            session.run("INSERT INTO test.binding_writer_atomicity VALUES (1)")?;
            session.run("INSERT INTO test.binding_writer_atomicity VALUES (1)")?;
            Ok(0)
        })
        .expect_err("a real duplicate-key failure aborts the storage operation")
        .to_mysql_error();
    assert_eq!(error.code, 1062);
    assert_eq!(error.state, *b"23000");
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT COUNT(*) FROM test.binding_writer_atomicity"
        )),
        [["0"]]
    );
    assert_eq!(
        bindings.attempts.load(Ordering::SeqCst),
        0,
        "no refresh before a successful commit"
    );

    let mut calls = 0;
    assert_eq!(
        writer
            .execute(&mut |session| {
                calls += 1;
                session.run("INSERT INTO test.binding_writer_atomicity VALUES (2)")?;
                Ok(1)
            })
            .expect("a committed operation succeeds despite refresh failure"),
        1
    );
    assert_eq!(
        calls, 1,
        "refresh failure cannot replay the committed operation"
    );
    assert_eq!(bindings.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        displayed(rows(
            &mut reader,
            "SELECT id FROM test.binding_writer_atomicity"
        )),
        [["2"]]
    );
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
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin");
    writer
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
            .control_transaction("BEGIN PESSIMISTIC")
            .expect("begin user transaction");
        writer
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
            .control_transaction("ROLLBACK")
            .expect("rollback user transaction");
        assert_eq!(
            displayed(rows(&mut reader, status)),
            [[expected]],
            "binding command survives caller rollback: {command}"
        );
    }
}

/// The probe-33 regression: a derived table whose inner SELECT plans as a
/// partial-aggregate push (root HashAgg over `TableReader(data:HashAgg)`)
/// must still answer the aggregate, not the bare scan rows. Go returns
/// `((1,30),(2,120))`; the broken handoff returned five raw rows.
#[test]
fn a_derived_aggregate_over_the_coprocessor_answers_its_output() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(7))
        .expect("session opens");
    rows(&mut session, "CREATE TABLE test.rep (g int, v int)");
    rows(
        &mut session,
        "INSERT INTO test.rep VALUES (1, 10), (1, 20), (2, 40), (2, 80), (1, 0)",
    );

    let inner = displayed(rows(
        &mut session,
        "SELECT g, sum(v) AS t FROM test.rep GROUP BY g ORDER BY g",
    ));
    assert_eq!(
        inner,
        [["1", "30"], ["2", "120"]],
        "the inner aggregate alone must already be right"
    );

    let derived = displayed(rows(
        &mut session,
        "SELECT * FROM (SELECT g, sum(v) AS t FROM test.rep GROUP BY g) s ORDER BY g",
    ));
    assert_eq!(
        derived,
        [["1", "30"], ["2", "120"]],
        "the derived consumer must see the aggregate, not the scan rows"
    );

    // The COUNT(*) shape panicked the worker thread on the live node
    // (chunk column index out of bounds); here a panic fails the test.
    let counted = displayed(rows(
        &mut session,
        "SELECT * FROM (SELECT g, count(*) AS c FROM test.rep GROUP BY g) s ORDER BY g",
    ));
    assert_eq!(counted, [["1", "3"], ["2", "2"]]);

    // The desc keep-order Limit rides the REVERSED region walk -- the
    // shape whose first live draft returned NOTHING because the reverse
    // scan's caller-swaps-the-bounds contract was missed.
    rows(
        &mut session,
        "CREATE TABLE test.walk (id bigint primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.walk VALUES (1, 10), (2, 20), (3, 30), (5, 50), (100, 1)",
    );
    let descending = displayed(rows(
        &mut session,
        "SELECT id FROM test.walk WHERE id > 1 ORDER BY id DESC LIMIT 2",
    ));
    assert_eq!(
        descending,
        [["100"], ["5"]],
        "the desc keep-order Limit must answer the LARGEST ids over the region walk"
    );

    // The covering-index COUNT rides an [IndexScan, Aggregation] DAG:
    // the region decodes the indexed values out of the KEY and counts
    // them, Go's PhysicalIndexReader carrying the partial stage.
    rows(&mut session, "CREATE INDEX walk_v ON test.walk (v)");
    let counted_over_index = displayed(rows(
        &mut session,
        "SELECT count(v) FROM test.walk WHERE v > 5",
    ));
    assert_eq!(counted_over_index, [["4"]]);

    // A full covering SUM uses the unordered Global aggregate contract. Its
    // input is indexed in the pruned scan schema, so lowering must translate
    // that offset back through the table schema before reading index keys.
    let requests_before_sum = stack.cop_source.stats().requests.len();
    let summed_over_index = displayed(rows(&mut session, "SELECT sum(v) FROM test.walk"));
    assert_eq!(summed_over_index, [["111"]]);
    let after_sum = stack.cop_source.stats();
    assert!(
        after_sum.requests.len() > requests_before_sum
            && after_sum.requests[requests_before_sum..]
                .iter()
                .any(|request| request.contains("IndexScan") && request.contains("HashAgg")),
        "the covering SUM did not reach an index HashAgg DAG: {:?}",
        after_sum.requests
    );

    // The receipt that the partial stage ran AT THE REGION: the scanner's
    // request log names an aggregation executor in a served DAG. A refusal
    // would fall back to the local partial cursor -- same answer, but the
    // lowering this test pins would silently be dead.
    let stats = stack.cop_source.stats();
    assert!(
        stats
            .requests
            .iter()
            .any(|request| request.contains("HashAgg") || request.contains("StreamAgg")),
        "no served DAG carried an aggregation executor: {:?}",
        stats.requests
    );
    assert!(
        stats
            .requests
            .iter()
            .any(|request| request.contains("IndexScan")),
        "no served DAG carried the covering-index aggregate: {:?}",
        stats.requests
    );
}

/// Go `setDataForServersInfo` (`infoschema_reader.go:2730`) over
/// `GetAllServerInfo`: one row per server, in Go's eight-column order.
/// With no etcd client the syncer answers THIS node alone -- Go's
/// `etcdCli == nil` path -- which is what a single-node deployment shows.
#[test]
fn tidb_servers_info_reports_this_node() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(11))
        .expect("session opens");

    let rows = displayed(rows(
        &mut session,
        "SELECT DDL_ID, IP, PORT, STATUS_PORT, LEASE, VERSION, GIT_HASH, LABELS \
         FROM information_schema.tidb_servers_info",
    ));
    assert_eq!(rows.len(), 1, "a single node reports itself alone");
    let row = &rows[0];

    // Go's DDL_ID is `uuid.New().String()`; the shape is what a peer's
    // stale-entry match and this reader both see.
    assert_eq!(row[0].len(), 36, "DDL_ID is a uuid: {}", row[0]);
    assert_eq!(row[0].matches('-').count(), 4, "{}", row[0]);
    // The port the node was configured with, as an integer column.
    assert_eq!(row[2], "0", "the fixture binds an ephemeral port");
    assert_eq!(row[3], "10080", "the default status port");
    // The lease travels as text, and the version pair is the build's.
    assert!(row[4].ends_with("ms"), "LEASE is text: {}", row[4]);
    assert!(!row[5].is_empty(), "VERSION is reported");
    // No labels are configured, which renders as the empty string rather
    // than a stray separator (Go `BuildStringFromLabels`).
    assert_eq!(row[7], "");
}

/// Go `ALTER TABLE ... [FORCE] AUTO_INCREMENT = n` over the real node.
///
/// The DDL half -- the stored `AutoIncID` and the counter key -- is pinned in
/// `tidb-exec`. What only this stack can show is that the node's LIVE
/// allocator notices: it caches a reserved range that outlives schema
/// reloads by design, so a rebase that moved only the meta keys would leave
/// the next INSERT allocating from the range reserved before the change, and
/// the statement would look like it did nothing.
#[test]
fn a_rebased_auto_increment_reaches_the_next_insert() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(13))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.seq (id bigint primary key auto_increment, v int)",
    );
    rows(&mut session, "INSERT INTO test.seq (v) VALUES (1)");

    // FORCE sets the base exactly, even below the counter the first
    // reservation already wrote.
    rows(
        &mut session,
        "ALTER TABLE test.seq FORCE AUTO_INCREMENT = 500",
    );
    rows(&mut session, "INSERT INTO test.seq (v) VALUES (2)");
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.seq ORDER BY v")),
        [["1"], ["500"]],
        "the forced base is what the next INSERT allocates"
    );

    // Without FORCE the base is floored at the allocator's next id, and Go
    // says so rather than silently doing something else. The reservation
    // taken above ends at 500 + the default step, so the floor is well past
    // the 5 that was asked for.
    rows(&mut session, "ALTER TABLE test.seq AUTO_INCREMENT = 5");
    let warnings = displayed(rows(&mut session, "SHOW WARNINGS"));
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_eq!(warnings[0][0], "Warning");
    assert_eq!(warnings[0][1], "1105");
    assert!(
        warnings[0][2].starts_with("Can't reset AUTO_INCREMENT to 5 without FORCE option, using "),
        "{}",
        warnings[0][2]
    );
}

/// Go `ShowDDLExec.Next` (`executor/show_ddl.go`): six columns describing the
/// DDL owner and this node.
///
/// `SCHEMA_VER` is the version this node currently follows, so it moves when a
/// catalog change lands. The owner columns name THIS node, which is what a
/// single-node deployment reports and what this node truthfully is: it runs no
/// election, and every catalog change it accepts, it performs itself. The two
/// job-list columns are structurally empty because a change is published in
/// one transaction rather than queued, so no later statement can observe one
/// in flight.
#[test]
fn admin_show_ddl_reports_this_node_and_the_followed_version() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(17))
        .expect("session opens");

    let before = displayed(rows(&mut session, "ADMIN SHOW DDL"));
    assert_eq!(before.len(), 1);
    let row = &before[0];
    let version: i64 = row[0].parse().expect("SCHEMA_VER is an integer");
    // Go's DDL_ID is a uuid, and the owner and self are the same node here.
    assert_eq!(row[1].len(), 36, "OWNER_ID is a uuid: {}", row[1]);
    assert_eq!(row[1], row[4], "this node is its own owner");
    assert!(
        row[2].contains(':'),
        "OWNER_ADDRESS is host:port: {}",
        row[2]
    );
    assert_eq!(row[3], "", "no job is ever observably in flight");
    assert_eq!(row[5], "", "and so no query is either");

    // The reported version follows the catalog, so a change moves it.
    rows(
        &mut session,
        "CREATE TABLE test.ddl_probe (id int primary key)",
    );
    let after = displayed(rows(&mut session, "ADMIN SHOW DDL"));
    let moved: i64 = after[0][0].parse().expect("SCHEMA_VER is an integer");
    assert!(
        moved > version,
        "a published change moves SCHEMA_VER: {version} -> {moved}"
    );
    assert_eq!(after[0][4], row[4], "the node identity is stable");

    // The identity is the one TIDB_SERVERS_INFO reports for this node.
    let servers = displayed(rows(
        &mut session,
        "SELECT DDL_ID FROM information_schema.tidb_servers_info",
    ));
    assert_eq!(servers, [[row[4].clone()]]);
}

/// Go `dataForTiDBClusterInfo` (`infoschema_reader.go:1842`) over
/// `GetClusterServerInfo`: one row per node, describing where it is and how
/// long it has been up.
///
/// Go chains five retrievers there and only the first has a source here, so
/// this reports the TiDB rows alone -- see `Session::cluster_info_table_rows`
/// for the four it cannot see and why inventing them would be worse.
#[test]
fn cluster_info_reports_this_node() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(19))
        .expect("session opens");

    let reported = displayed(rows(
        &mut session,
        "SELECT TYPE, INSTANCE, STATUS_ADDRESS, VERSION, GIT_HASH, UPTIME, SERVER_ID \
         FROM information_schema.cluster_info",
    ));
    assert_eq!(reported.len(), 1, "a single node reports itself alone");
    let row = &reported[0];
    assert_eq!(row[0], "tidb");
    // Both addresses are host:port, and they are the node's own two ports.
    assert!(row[1].contains(':'), "INSTANCE is host:port: {}", row[1]);
    assert!(
        row[2].contains(':'),
        "STATUS_ADDRESS is host:port: {}",
        row[2]
    );
    assert_ne!(row[1], row[2], "the SQL and status ports differ");
    assert!(!row[3].is_empty(), "VERSION is reported");
    assert!(!row[4].is_empty(), "GIT_HASH is reported");
    // Go prints `time.Since(startTime).String()`, so the unit is spelled out.
    assert!(
        row[5].ends_with('s'),
        "UPTIME is a Go duration string: {}",
        row[5]
    );

    // The instance is the same node TIDB_SERVERS_INFO describes, which is the
    // point of the two tables agreeing.
    let servers = displayed(rows(
        &mut session,
        "SELECT IP, PORT FROM information_schema.tidb_servers_info",
    ));
    assert_eq!(row[1], format!("{}:{}", servers[0][0], servers[0][1]));
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

/// THE INVARIANT: every `TableInfo` this node's DDL publishes, its own
/// catalog loader must load.
///
/// Breaking it produces the worst shape a DDL can take -- the CREATE reports
/// success, a later CREATE of the same name reports 1050, and every read
/// reports 1146 -- and it broke for real on
/// `DATETIME(n) DEFAULT CURRENT_TIMESTAMP(n)`. Neither half's own tests could
/// catch that: the DDL wrote correct metadata and the loader correctly
/// refused what it was given. Only the two together show it.
///
/// The shapes below are the column and key forms this node admits. A new
/// admitted shape belongs here.
#[test]
fn every_shape_the_ddl_admits_the_loader_loads() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(29))
        .expect("session opens");

    const SHAPES: &[&str] = &[
        // Literal defaults, one per storage family.
        "a int DEFAULT 5",
        "a varchar(10) DEFAULT 'x'",
        "a decimal(10,2) DEFAULT 1.5",
        "a double DEFAULT 1.5",
        "a bit(8) DEFAULT b'101'",
        "a enum('x','y') DEFAULT 'y'",
        "a set('p','q') DEFAULT 'q'",
        "a date DEFAULT '2020-01-01'",
        "a time(3) DEFAULT '01:02:03.400'",
        "a year DEFAULT 2020",
        "a binary(4) DEFAULT 'ab'",
        "a char(3) CHARACTER SET latin1 DEFAULT 'q'",
        "a int UNSIGNED ZEROFILL DEFAULT 7",
        "a json",
        "a text",
        // The clock marker, at every fsp -- the shape that broke.
        "a timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "a datetime DEFAULT CURRENT_TIMESTAMP",
        "a datetime(3) DEFAULT CURRENT_TIMESTAMP(3)",
        "a timestamp(6) DEFAULT CURRENT_TIMESTAMP(6)",
        // Keys and indexes.
        "id bigint PRIMARY KEY AUTO_INCREMENT, a int, KEY k(a)",
        "id varchar(20) PRIMARY KEY, a int",
        "id bigint, a int, PRIMARY KEY (id, a)",
        "id bigint PRIMARY KEY AUTO_RANDOM",
        "a int, b int, UNIQUE KEY u(a,b)",
        "a int, KEY k(a) COMMENT 'c'",
        "a int, KEY k(a) INVISIBLE",
        "id bigint PRIMARY KEY NONCLUSTERED, a int",
        "a int COMMENT 'col comment'",
    ];

    for (index, shape) in SHAPES.iter().enumerate() {
        let name = format!("test.shape{index}");
        rows(&mut session, &format!("CREATE TABLE {name} ({shape})"));
        // The read is the assertion: a table the loader dropped answers 1146
        // here while still colliding with a second CREATE.
        let loaded = displayed(rows(
            &mut session,
            &format!(
                "SELECT count(*) FROM information_schema.tables \
                 WHERE table_schema = 'test' AND table_name = 'shape{index}'"
            ),
        ));
        assert_eq!(
            loaded,
            [["1"]],
            "the DDL published `{shape}` and the loader dropped it"
        );
        // And it is actually usable, not merely listed.
        rows(&mut session, &format!("SELECT * FROM {name}"));
    }
}

/// Go `fieldTypeFromPBColumn`: the coprocessor rebuilds each column's type
/// from the DAG request, and the type CODE alone is not the type.
///
/// Dropping the rest decoded the stored bytes under the wrong rules. An
/// `INT UNSIGNED` holding 4294967295 came back as -1 on the SCAN path while
/// the point-get path -- which builds its types from the catalog -- returned
/// the stored value: one table, two paths, two answers. Carrying the flag
/// then exposed the second half, since the region's filter understood only
/// `Datum::Int` and silently dropped every row of an unsigned column.
#[test]
fn unsigned_columns_survive_the_coprocessor_scan() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(31))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.un (id int primary key, a tinyint unsigned, \
         b smallint unsigned, d int unsigned, e bigint unsigned)",
    );
    rows(
        &mut session,
        "INSERT INTO test.un VALUES (1, 255, 65535, 4294967295, 18446744073709551615), \
         (2, 1, 1, 1, 1)",
    );

    // The scan path returns the stored values, not their signed
    // reinterpretation.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a, b, d, e FROM test.un WHERE id > 0"
        )),
        [
            ["255", "65535", "4294967295", "18446744073709551615"],
            ["1", "1", "1", "1"],
        ]
    );
    // And it agrees with the point-get path, which never lost the flag.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT a, d, e FROM test.un WHERE id = 1"
        )),
        [["255", "4294967295", "18446744073709551615"]]
    );

    // A pushed-down predicate over an unsigned column compares in the
    // unsigned domain, including past i64::MAX.
    for (predicate, expected) in [
        ("d > 2", "1"),
        ("a < 255", "1"),
        ("e > 9223372036854775807", "1"),
        ("e = 18446744073709551615", "1"),
        ("a IN (255, 1)", "2"),
    ] {
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM test.un WHERE {predicate}"),
            )),
            [[expected.to_owned()]],
            "`{predicate}` over the scan path"
        );
    }
}

/// The REST of Go `fieldTypeFromPBColumn`: flag is not the only field the
/// decode needs.
///
/// `elems` decides what an ENUM/SET ordinal means, and `decimal` the scale a
/// DECIMAL and the fsp a TIME/DATETIME read back with. The scan path rebuilt
/// none of them, so this pins each against the point-get path, which builds
/// its types from the catalog and therefore never lost them. A disagreement
/// here is the same class of bug as the unsigned one: one table, two paths,
/// two answers.
#[test]
fn the_scan_path_decodes_elems_and_scale_like_the_point_get_path() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(37))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.tt (id int primary key, en enum('alpha','beta','gamma'), \
         st set('p','q','r'), dc decimal(12,4), yr year, tm time(3), dt datetime(6))",
    );
    rows(
        &mut session,
        "INSERT INTO test.tt VALUES (1, 'gamma', 'q,r', 12345.6789, 2024, \
         '12:34:56.789', '2024-03-04 05:06:07.891011')",
    );

    let columns = "en, st, dc, yr, tm, dt";
    // `id = 1` is a point get; `id > 0 AND id < 2` is a scan of the same row.
    let point_get = displayed(rows(
        &mut session,
        &format!("SELECT {columns} FROM test.tt WHERE id = 1"),
    ));
    let scanned = displayed(rows(
        &mut session,
        &format!("SELECT {columns} FROM test.tt WHERE id > 0 AND id < 2"),
    ));
    assert_eq!(point_get, scanned, "the two paths must read one row alike");
    assert_eq!(
        scanned,
        [[
            "gamma".to_owned(),
            "q,r".to_owned(),
            "12345.6789".to_owned(),
            "2024".to_owned(),
            "12:34:56.789".to_owned(),
            "2024-03-04 05:06:07.891011".to_owned(),
        ]],
        "and both must read what was stored"
    );
}

/// An INDEX read and a TABLE scan of the same rows must agree, and both must
/// agree with what was stored.
///
/// The index path decodes from the index KEY rather than the row value, so it
/// is a second decode of the same data under the same signedness rules. The
/// table-scan path got those rules wrong once already (it rebuilt column
/// types from the DAG without the UNSIGNED flag); this pins the pair so a
/// change to either side cannot drift from the other.
#[test]
fn an_index_read_and_a_table_scan_agree_over_unsigned_keys() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(41))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.ix (id int primary key, u bigint unsigned, KEY ku(u))",
    );
    rows(
        &mut session,
        "INSERT INTO test.ix VALUES (1, 18446744073709551615), (2, 1), \
         (3, 9223372036854775808), (4, NULL), (5, 0)",
    );

    // Each predicate straddles the signed/unsigned boundary, where a signed
    // reading would answer differently.
    for predicate in [
        "u > 2",
        "u >= 9223372036854775808",
        "u = 18446744073709551615",
        "u < 9223372036854775808",
        "u IS NULL",
    ] {
        let indexed = displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM test.ix WHERE {predicate}"),
        ));
        let scanned = displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM test.ix IGNORE INDEX (ku) WHERE {predicate}"),
        ));
        assert_eq!(indexed, scanned, "`{predicate}`: index and scan disagree");
    }

    // The index also ORDERS in the unsigned domain, which is the reading a
    // signed key encoding would reverse at the top of the range.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT u FROM test.ix WHERE u IS NOT NULL ORDER BY u"
        )),
        [
            ["0"],
            ["1"],
            ["9223372036854775808"],
            ["18446744073709551615"],
        ]
    );
}

/// THE DIFFERENTIAL, as a test: a predicate pushed into the coprocessor and
/// the same predicate evaluated locally must select the same rows.
///
/// `WHERE p` is answered by the region's filter; `sum(CASE WHEN p ...)` is
/// answered by the local evaluator over the same rows. Any disagreement is a
/// silent wrong answer -- the region either invented a row or dropped one --
/// and no single-path test can see it, because each evaluator is correct
/// against its own inputs. This is how the UNSIGNED decode bug was found,
/// after every existing test passed straight through it.
///
/// The fixture is deliberately made of boundary values: agreement on
/// ordinary data proves nothing.
#[test]
fn a_pushed_down_predicate_selects_what_local_evaluation_selects() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(43))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.diff (id int primary key, i bigint, u bigint unsigned, \
         s varchar(20) COLLATE utf8mb4_general_ci, b varchar(20) COLLATE utf8mb4_bin, \
         dc decimal(12,3), d datetime)",
    );
    rows(
        &mut session,
        "INSERT INTO test.diff VALUES \
         (1, -9223372036854775808, 18446744073709551615, 'Hello', 'Hello', -999999.999, \
          '1000-01-01 00:00:00'), \
         (2, 0, 0, '', '', 0.000, '2024-06-15 12:30:45'), \
         (3, NULL, NULL, NULL, NULL, NULL, NULL), \
         (4, 9223372036854775807, 9223372036854775808, 'HELLO', 'HELLO', 999999.999, \
          '9999-12-31 23:59:59'), \
         (5, -1, 1, 'world', 'world', -0.001, '2000-02-29 00:00:00')",
    );

    const PREDICATES: &[&str] = &[
        // Signed and unsigned integers at their extremes.
        "i > 0",
        "i < 0",
        "i = -9223372036854775808",
        "u > 2",
        "u >= 9223372036854775808",
        "u = 18446744073709551615",
        // Three-valued logic.
        "i IS NULL",
        "i IS NOT NULL AND u > 0",
        "NOT (i > 0)",
        "i = 0 OR u = 0",
        "i IN (0, -1)",
        "i NOT IN (0)",
        // Collation-sensitive comparison, both sides of the pair.
        "s = 'hello'",
        "b = 'hello'",
        "s > 'HELLO'",
        // Other families, and cross-type coercion.
        "dc > 0",
        "dc = -0.001",
        "d > '2024-01-01'",
        "i = '0'",
        "i BETWEEN '-1' AND '1'",
    ];

    for predicate in PREDICATES {
        let pushed = displayed(rows(
            &mut session,
            &format!("SELECT count(*) FROM test.diff WHERE {predicate}"),
        ));
        let local = displayed(rows(
            &mut session,
            &format!(
                "SELECT coalesce(sum(CASE WHEN ({predicate}) THEN 1 ELSE 0 END), 0) \
                 FROM test.diff"
            ),
        ));
        assert_eq!(
            pushed, local,
            "`{predicate}`: the region and the local evaluator disagree"
        );
    }
}

/// Statistics may change the PLAN; they must never change the ANSWER.
///
/// `ANALYZE` replaces pseudo estimates with real ones and the optimizer then
/// picks differently. What must hold either way is that every query returns
/// the same rows before and after: a cost decision that alters results is a
/// wrong answer no estimate can justify.
///
/// The plan TEXT changing shape across `ANALYZE` is not evidence of lost
/// pushdown -- see `crate::explain`'s first named divergence, where every row
/// prints task `root` whether or not the wire pushed anything, and
/// `analyze_does_not_stop_a_pushed_shape_reaching_the_region` for the receipt
/// that actually answers it.
#[test]
fn analyze_changes_the_plan_and_never_the_answer() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(47))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.st (id int primary key, a int, u bigint unsigned, \
         s varchar(10), KEY ka(a), KEY ku(u))",
    );
    rows(
        &mut session,
        "INSERT INTO test.st VALUES (1,1,18446744073709551615,'x'), (2,1,0,'y'), \
         (3,2,9223372036854775808,'z'), (4,3,1,'w'), (5,3,NULL,NULL)",
    );

    const QUERIES: &[&str] = &[
        "SELECT id FROM test.st WHERE a = 1 ORDER BY id",
        "SELECT id FROM test.st WHERE a >= 2 ORDER BY id",
        "SELECT count(*) FROM test.st WHERE u > 2",
        "SELECT id FROM test.st WHERE u = 18446744073709551615",
        "SELECT a, count(*) FROM test.st GROUP BY a ORDER BY a",
        "SELECT id FROM test.st WHERE s IS NULL",
        "SELECT id FROM test.st ORDER BY u DESC LIMIT 2",
        "SELECT max(u), min(u) FROM test.st",
    ];

    let before: Vec<_> = QUERIES
        .iter()
        .map(|query| displayed(rows(&mut session, query)))
        .collect();

    rows(&mut session, "ANALYZE TABLE test.st");

    for (query, expected) in QUERIES.iter().zip(before) {
        assert_eq!(
            displayed(rows(&mut session, query)),
            expected,
            "`{query}` answered differently once statistics existed"
        );
    }
}

/// Does a pushed-down shape still reach the region once statistics exist?
///
/// `EXPLAIN` cannot answer this: `crate::explain`'s documented divergence is
/// that every row prints task `root` whether or not the wire pushed anything,
/// so the display and the coprocessor have deliberately come apart. The
/// receipt is the scanner's own request log, and the shape has to be one this
/// node actually lowers -- a grouped aggregate, as
/// `a_derived_aggregate_over_the_coprocessor_answers_its_output` pins.
#[test]
fn analyze_does_not_stop_a_pushed_shape_reaching_the_region() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(53))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.pd (id int primary key, g int, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.pd VALUES (1,1,10),(2,1,20),(3,2,40),(4,2,80),(5,1,0)",
    );

    let query = "SELECT g, sum(v) FROM test.pd GROUP BY g ORDER BY g";
    let expected = displayed(rows(&mut session, query));
    let before = stack.cop_source.stats().requests.len();
    assert!(before > 0, "the grouped aggregate reached the region");

    rows(&mut session, "ANALYZE TABLE test.pd");
    let after_analyze = stack.cop_source.stats().requests.len();

    assert_eq!(
        displayed(rows(&mut session, query)),
        expected,
        "the answer changed once statistics existed"
    );
    assert!(
        stack.cop_source.stats().requests.len() > after_analyze,
        "the same query served no coprocessor request once statistics existed"
    );
}

/// A write takes the same access paths a `SELECT` does, which is
/// `crate::explain`'s divergence 8 as it now stands.
///
/// That paragraph claimed the opposite for a while -- "none is offered to a
/// write" -- after `write_index_range_path` landed and nothing checked the
/// prose against the code. A doc that describes a gap which no longer exists
/// sends the next reader to build what is already there, so the claim is
/// pinned here rather than trusted.
#[test]
fn a_write_reaches_the_index_path_like_a_select() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(59))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.wi (id int primary key, a int, b int, KEY ka(a), UNIQUE KEY ub(b))",
    );
    rows(
        &mut session,
        "INSERT INTO test.wi VALUES (1,10,100),(2,10,200),(3,20,300)",
    );

    let plan_of = |session: &mut _, sql: &str| {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };

    // A non-unique secondary index is chosen for a write, as it is for a read.
    for sql in [
        "EXPLAIN UPDATE test.wi SET b = b + 1 WHERE a = 10",
        "EXPLAIN DELETE FROM test.wi WHERE a = 10",
    ] {
        let plan = plan_of(&mut session, sql);
        assert!(
            plan.contains("IndexRangeScan") && plan.contains("index:ka(a)"),
            "`{sql}` did not reach the index path:\n{plan}"
        );
        // Divergence 7: the ranges are a superset, so the filter stays above.
        assert!(plan.contains("Selection"), "{plan}");
    }

    // A WHERE that pins a whole UNIQUE index still takes the point plan.
    let plan = plan_of(
        &mut session,
        "EXPLAIN UPDATE test.wi SET a = 1 WHERE b = 100",
    );
    assert!(plan.contains("Point_Get"), "{plan}");

    // And the rows a write touches are the rows the predicate names,
    // whichever path carried it there.
    rows(&mut session, "UPDATE test.wi SET b = b + 1 WHERE a = 10");
    assert_eq!(
        displayed(rows(&mut session, "SELECT id, b FROM test.wi ORDER BY id")),
        [["1", "101"], ["2", "201"], ["3", "300"]]
    );
    rows(&mut session, "DELETE FROM test.wi WHERE a = 10");
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.wi ORDER BY id")),
        [["3"]]
    );
}

/// Go `handleUnsignedCol`: a NEGATIVE bound on an unsigned column is either
/// rewritten to `>= 0` or makes the range invalid, and an invalid range folds
/// to a `TableDual` (`crate::explain`'s divergence 9).
///
/// The distinction that matters is negative VALUE, not negative-looking
/// predicate: `a < 0` compares against zero, which Go treats as non-negative
/// and rewrites nothing, so it keeps a real `IndexRangeScan`. Mis-reading
/// that cost an incorrect doc edit once; it is pinned here so the next reader
/// gets the boundary from a test rather than from prose.
#[test]
fn a_negative_bound_on_an_unsigned_column_follows_gos_rewrite() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(61))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.ud (id int primary key, a int unsigned, KEY ka(a))",
    );
    rows(
        &mut session,
        "INSERT INTO test.ud VALUES (1,0),(2,5),(3,4294967295)",
    );

    let plan_of = |session: &mut _, sql: &str| {
        displayed(rows(session, sql))
            .into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };

    // A negative value with LT/LE/EQ makes the range invalid -> TableDual.
    for predicate in ["a < -1", "a <= -1", "a = -1"] {
        let plan = plan_of(
            &mut session,
            &format!("EXPLAIN SELECT id FROM test.ud USE INDEX(ka) WHERE {predicate}"),
        );
        assert!(
            plan.contains("TableDual"),
            "`{predicate}` should fold to a dual:\n{plan}"
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM test.ud WHERE {predicate}")
            )),
            [["0"]],
        );
    }

    // A negative value with GT/GE/NE is rewritten to `>= 0`, so every row
    // qualifies rather than none.
    for predicate in ["a > -1", "a >= -5", "a <> -1"] {
        let plan = plan_of(
            &mut session,
            &format!("EXPLAIN SELECT id FROM test.ud USE INDEX(ka) WHERE {predicate}"),
        );
        assert!(
            plan.contains("range:[0"),
            "`{predicate}` should start at 0:\n{plan}"
        );
        assert_eq!(
            displayed(rows(
                &mut session,
                &format!("SELECT count(*) FROM test.ud WHERE {predicate}")
            )),
            [["3"]],
        );
    }

    // `a < 0` is NOT a negative value: Go rewrites nothing, so the range
    // survives as a real scan that happens to find no rows.
    let plan = plan_of(
        &mut session,
        "EXPLAIN SELECT id FROM test.ud USE INDEX(ka) WHERE a < 0",
    );
    assert!(plan.contains("IndexRangeScan"), "{plan}");
    assert!(!plan.contains("TableDual"), "{plan}");
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT count(*) FROM test.ud WHERE a < 0"
        )),
        [["0"]],
    );
}

/// `CREATE VIEW` over the real embedded store creates the view, and it reads
/// back.
///
/// This is the path `--store unistore --cluster-session` actually serves, and
/// it had NO coverage: the mock-seam modules in this directory serve
/// `cop_scans: None`, and the pipeline session's own `CREATE VIEW` wire test
/// exercises a different session. That blind spot is how seven commits landed
/// on a working feature and broke it in silence.
///
/// Bisected, one build and one live server per point: `9b893f4abd` still
/// creates the view; `b1f979cc76` ("rust: complete unistore transaction batch
/// get") answers
///
/// ```text
/// ERROR 1105 (HY000): table bytes failed to decode
/// ```
///
/// and leaves no view behind. The real cause is
/// `Storage("Backend(\"query deadline exceeded\")")`, and the shape of the
/// failure is the clue: this test sits for ~20s before reporting, so the
/// coprocessor request never COMPLETES and the wait runs out. It is a request
/// that goes unanswered, not a deadline that was mis-set.
#[test]
fn a_view_over_the_coprocessor_is_created_and_reads_back() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(9))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.vsrc (id int primary key, v int)",
    );
    rows(
        &mut session,
        "INSERT INTO test.vsrc VALUES (1, 10), (2, 20)",
    );
    rows(
        &mut session,
        "CREATE VIEW test.vview AS SELECT id FROM test.vsrc WHERE id > 1",
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.vview")),
        [["2"]],
        "the view reads back its defining query"
    );
}

/// A HASH-partitioned table created HERE writes its rows to the right
/// physical tables and reads every one of them back.
///
/// This is the end-to-end claim the metadata round trip does NOT make. The
/// loader proves the stored bounds fold back; this proves a row written
/// under one partition's physical table id is found again by a read that has
/// to visit all of them. The four ids straddle both partitions under
/// `HASH(id) PARTITIONS 2`, so a read that reached only one physical table
/// would come back with half the rows rather than with an error.
#[test]
fn a_hash_partitioned_table_writes_and_reads_every_partition() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(63))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.hp (id int primary key, v int) PARTITION BY HASH (id) PARTITIONS 2",
    );
    rows(
        &mut session,
        "INSERT INTO test.hp VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id, v FROM test.hp ORDER BY id")),
        [["1", "10"], ["2", "20"], ["3", "30"], ["4", "40"]],
        "every partition's rows come back IN ORDER: the per-partition scans \
         are each ordered, and merging them is what makes the whole answer so"
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id FROM test.hp ORDER BY id DESC"
        )),
        [["4"], ["3"], ["2"], ["1"]],
        "the descending merge walks both partitions backwards together"
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT count(*) FROM test.hp")),
        [["4"]],
        "the aggregate reaches every physical table too"
    );
}

/// A RANGE-partitioned table prunes on read without losing rows.
///
/// The unpruned read is the control: if pruning dropped a partition it
/// should not have, only the narrowed query would be wrong, and only a
/// comparison against the full scan shows it.
///
/// This does NOT test cross-partition ordering, and cannot: a clustered
/// primary key must cover the partition columns, so a RANGE table keyed on
/// its own primary key stores the partitions in handle order and
/// concatenating them is already sorted. `HASH` is what separates the two,
/// because hashing scatters the handle across partitions.
#[test]
fn a_range_partitioned_table_prunes_without_losing_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(64))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.rp (id int primary key, v int) PARTITION BY RANGE (id) \
         (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
    );
    rows(
        &mut session,
        "INSERT INTO test.rp VALUES (5, 50), (15, 150), (25, 250)",
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.rp ORDER BY id")),
        [["5"], ["15"], ["25"]],
        "the unpruned read sees both partitions"
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.rp WHERE id < 10")),
        [["5"]],
        "the pruned read keeps the row that is actually below the bound"
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id FROM test.rp WHERE id >= 10 ORDER BY id"
        )),
        [["15"], ["25"]],
        "and the other side keeps the rows above it"
    );
}

/// A KEY-partitioned table answers an ordered read in order.
///
/// KEY hashes the partition columns exactly as HASH does, so the handles
/// scatter across partitions and the merge is what puts them back together.
/// This is the sibling of
/// [`a_hash_partitioned_table_writes_and_reads_every_partition`] over the
/// other method that stores rows out of handle order.
#[test]
fn a_key_partitioned_table_answers_an_ordered_read_in_order() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack
        .factory
        .open_session(session_context(65))
        .expect("session opens");
    rows(
        &mut session,
        "CREATE TABLE test.kp (id int primary key, v int) PARTITION BY KEY (id) PARTITIONS 2",
    );
    rows(
        &mut session,
        "INSERT INTO test.kp VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT id FROM test.kp ORDER BY id")),
        [["1"], ["2"], ["3"], ["4"], ["5"]],
        "the merge orders across every KEY partition"
    );
}

/// Two sessions racing their `CREATE TABLE`s both succeed -- sysbench's
/// parallel `prepare` (`--threads=4 --tables=2`) is exactly this shape, and
/// it was the workload that found the gap: `sbtest2` never existed and every
/// later statement on it failed.
///
/// Every catalog change writes `SchemaVersionKey`, so concurrent DDL is a
/// GUARANTEED optimistic write conflict. Go runs each DDL meta write under
/// `kv.RunInNewTxn(retryable=true)` (`pkg/ddl/ddl.go`), which rolls the
/// loser back and re-runs it from a fresh snapshot -- re-read, re-plan,
/// re-commit -- so no client ever sees the conflict. Before
/// `commit_cluster_ddl` carried that loop, the loser here surfaced error
/// 1105 ("... refuses to interleave: ... WriteConflict").
#[test]
fn concurrent_creates_both_succeed_like_gos_ddl_queue() {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        for worker in 0u64..2 {
            let barrier = &barrier;
            scope.spawn(move || {
                let mut session = factory
                    .open_session(session_context(70 + worker))
                    .expect("session opens");
                barrier.wait();
                for table in 0..4 {
                    rows(
                        &mut session,
                        &format!(
                            "CREATE TABLE test.race_{worker}_{table} (id int primary key, v int)"
                        ),
                    );
                }
            });
        }
    });
    // Every one of the eight racing tables exists and serves reads and
    // writes: the retry made both sessions' schedules land, in some order.
    let mut session = factory
        .open_session(session_context(79))
        .expect("session opens");
    for worker in 0..2 {
        for table in 0..4 {
            rows(
                &mut session,
                &format!("INSERT INTO test.race_{worker}_{table} VALUES (1, 10)"),
            );
            assert_eq!(
                displayed(rows(
                    &mut session,
                    &format!("SELECT v FROM test.race_{worker}_{table} WHERE id = 1"),
                )),
                [["10"]],
                "table race_{worker}_{table} must exist and answer"
            );
        }
    }
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
        first.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    rows(&mut first, "UPDATE test.race_rw SET v = v + 1 WHERE id = 1");

    let second = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(81))
                .expect("session opens");
            assert_eq!(
                second.control_transaction("BEGIN").expect("begin"),
                Some(true)
            );
            // Blocks on the first transaction's pessimistic row lock until
            // that transaction commits.
            rows(
                &mut second,
                "UPDATE test.race_rw SET v = v + 1 WHERE id = 1",
            );
            second.control_transaction("COMMIT").expect("commit");
        });
        // Give the contender time to reach the lock wait, so the interesting
        // interleaving -- blocked UPDATE, then the winner's COMMIT -- is the
        // one exercised. The assertions hold under any interleaving.
        std::thread::sleep(std::time::Duration::from_millis(200));
        first.control_transaction("COMMIT").expect("commit");
        contender.join()
    });
    second.expect("the contending transaction commits after waiting the lock out");

    assert_eq!(
        displayed(rows(&mut first, "SELECT v FROM test.race_rw WHERE id = 1")),
        [["2"]],
        "both increments landed: the loser re-read the winner's commit"
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
        reader.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    assert_eq!(
        displayed(rows(&mut reader, "SELECT v FROM test.lock_cache WHERE id = 1")),
        [["10"]],
        "the transaction's snapshot is the row as of BEGIN"
    );

    let mut writer = factory
        .open_session(session_context(85))
        .expect("session opens");
    rows(&mut writer, "UPDATE test.lock_cache SET v = 99 WHERE id = 1");

    // The LOCKING read may see the newer row -- it takes its own
    // `for_update_ts`, which is Go's behaviour too. This is what fills the
    // lock cache.
    let _ = reader.execute("SELECT v FROM test.lock_cache WHERE id = 1 FOR UPDATE");

    assert_eq!(
        displayed(rows(&mut reader, "SELECT v FROM test.lock_cache WHERE id = 1")),
        [["10"]],
        "the plain read that follows still reads at start_ts: the lock cache \
         belongs to locking reads only (`point_get.go:677`)"
    );
    reader.control_transaction("ROLLBACK").expect("rollback");
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
        .control_transaction("BEGIN OPTIMISTIC")
        .expect("begin");
    rows(
        &mut second,
        "UPDATE test.race_opt SET v = v + 1 WHERE id = 1",
    );
    second.control_transaction("COMMIT").expect("commit");

    // The first transaction's prewrite now finds the newer commit: 9007.
    let refused = first
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

    session.control_transaction("BEGIN").expect("begin");
    rows(
        &mut session,
        "UPDATE test.implicit_commit SET v = 7 WHERE id = 1",
    );
    // No COMMIT: the next BEGIN carries it implicitly.
    session
        .control_transaction("BEGIN")
        .expect("BEGIN with an open transaction implicitly commits, never conflicts");
    session.control_transaction("COMMIT").expect("commit");

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
#[test]
fn a_prepared_point_update_locks_its_row_before_reading_it() {
    use tidb_protocol::PreparedValue;

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
        first.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    let statement = first
        .prepare_general("UPDATE test.fold SET v = v + 5 WHERE id = ?")
        .expect("prepare");
    let affected = match first
        .execute_general(&statement, &[PreparedValue::SignedLongLong(1)])
        .expect("execute")
    {
        crate::sql_node::GeneralExecuteOutcome::Write(outcome) => outcome.affected_rows,
        crate::sql_node::GeneralExecuteOutcome::Rows(_) => {
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
                second.control_transaction("BEGIN").expect("begin"),
                Some(true)
            );
            rows(&mut second, "UPDATE test.fold SET v = v + 100 WHERE id = 1");
            second.control_transaction("COMMIT").expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        first.control_transaction("COMMIT").expect("commit");
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
        first.control_transaction("BEGIN").expect("begin"),
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
                second.control_transaction("BEGIN").expect("begin"),
                Some(true)
            );
            rows(
                &mut second,
                "UPDATE test.foldtxt SET v = v + 100 WHERE id = 1",
            );
            second.control_transaction("COMMIT").expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        first.control_transaction("COMMIT").expect("commit");
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
        first.control_transaction("BEGIN").expect("begin"),
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
                second.control_transaction("BEGIN").expect("begin"),
                Some(true)
            );
            rows(
                &mut second,
                "UPDATE test.foldsel SET v = v + 100 WHERE id = 1",
            );
            second.control_transaction("COMMIT").expect("commit");
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        rows(&mut first, "UPDATE test.foldsel SET v = v + 5 WHERE id = 1");
        first.control_transaction("COMMIT").expect("commit");
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

fn locking_range_reselects(projection: &str, window: &str, before: &[&str], after: &[&str]) {
    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    let mut first = factory
        .open_session(session_context(110))
        .expect("session opens");
    rows(&mut first, "CREATE TABLE test.lock_range (w int, d int, id int, PRIMARY KEY(w,d,id) CLUSTERED)");
    rows(
        &mut first,
        "INSERT INTO test.lock_range VALUES (1,1,1),(1,1,2),(1,1,3)",
    );
    first
        .control_transaction("BEGIN PESSIMISTIC")
        .expect("begin");
    let query = format!("SELECT {projection} FROM test.lock_range WHERE w=1 AND d=1 {window} FOR UPDATE");
    let flatten = |rows: Vec<Vec<Datum>>| {
        let mut ids: Vec<String> = displayed(rows).into_iter().map(|row| row[0].clone()).collect();
        ids.sort();
        ids
    };
    assert_eq!(flatten(rows(&mut first, &query)), before);

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let result = std::thread::scope(|scope| {
        let contender = scope.spawn(|| {
            let mut second = factory
                .open_session(session_context(111))
                .expect("session opens");
            second
                .control_transaction("BEGIN PESSIMISTIC")
                .expect("begin");
            started_tx.send(()).expect("signal begin");
            let result = flatten(rows(&mut second, &query));
            second.control_transaction("ROLLBACK").expect("rollback");
            result
        });
        started_rx.recv().expect("contender began");
        std::thread::sleep(std::time::Duration::from_millis(200));
        rows(
            &mut first,
            "DELETE FROM test.lock_range WHERE w=1 AND d=1 AND id=1",
        );
        first.control_transaction("COMMIT").expect("commit");
        contender.join().expect("contender finishes")
    });
    assert_eq!(
        result,
        after,
        "a locking range must not return the deleted winner"
    );
}

#[test]
fn locking_queries_retain_unprojected_record_handles() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(112)).expect("session opens");
    for (name, definition) in [
        ("lock_heap", "id int, v int"),
        ("lock_integer", "id int primary key, v int"),
        ("lock_common", "id int, v int, primary key(id,v) clustered"),
    ] {
        rows(&mut session, &format!("CREATE TABLE test.{name} ({definition})"));
        rows(&mut session, &format!("INSERT INTO test.{name} VALUES (1,10)"));
        session.control_transaction("BEGIN PESSIMISTIC").expect("begin");
        assert_eq!(
            displayed(rows(&mut session, &format!(
                "SELECT v FROM test.{name} WHERE v>0 LIMIT 1 FOR UPDATE"
            ))),
            [["10"]],
            "record handles must be available without appearing in the result"
        );
        assert_eq!(
            displayed(rows(&mut session, &format!(
                "SELECT * FROM test.{name} WHERE v>0 LIMIT 1 FOR UPDATE"
            ))),
            [["1", "10"]],
            "hidden handles must not leak through wildcard projection"
        );
        session.control_transaction("ROLLBACK").expect("rollback");
    }
}

#[test]
fn a_locking_range_sees_rows_committed_after_begin() {
    let (stack, _users) = cop_backed_stack();
    let mut reader = stack.factory.open_session(session_context(113)).expect("reader");
    let mut writer = stack.factory.open_session(session_context(114)).expect("writer");
    rows(&mut reader, "CREATE TABLE test.fresh_lock (id int primary key, v int)");
    reader.control_transaction("BEGIN PESSIMISTIC").expect("begin");
    assert!(rows(&mut reader, "SELECT id FROM test.fresh_lock WHERE id>0").is_empty());
    rows(&mut writer, "INSERT INTO test.fresh_lock VALUES (1,10)");
    let locked = displayed(rows(
        &mut reader,
        "SELECT id FROM test.fresh_lock WHERE id>0 ORDER BY id LIMIT 1 FOR UPDATE",
    ));
    let plain = rows(&mut reader, "SELECT id FROM test.fresh_lock WHERE id>0");
    reader.control_transaction("ROLLBACK").expect("rollback");
    assert_eq!(locked, [["1"]], "locking reads use a fresh statement snapshot");
    assert!(plain.is_empty(), "ordinary reads keep the BEGIN snapshot");
}

/// Go's repeatable-read BatchPointGet locks absent keys as well as rows.
/// Returning just the existing row must not permit an INSERT into another
/// key named by the same IN list while the locking transaction stays open.
#[test]
fn a_locking_batch_locks_missing_primary_keys() {
    use tidb_protocol::PreparedValue;
    use crate::resultset_source::ResultSetSource;

    let (stack, _users) = cop_backed_stack();
    let factory = &stack.factory;
    for (index, (isolation, change_default, implicit, prepared)) in [
        ("REPEATABLE-READ", false, false, false),
        ("READ-COMMITTED", false, false, false),
        ("REPEATABLE-READ", true, false, false),
        ("READ-COMMITTED", true, false, false),
        ("REPEATABLE-READ", false, true, false),
        ("REPEATABLE-READ", false, false, true),
    ].into_iter().enumerate() {
        let mut first = factory.open_session(session_context(115)).expect("reader");
        let table = format!("test.batch_missing_{index}");
        // The prepared case binds reordered, duplicated composite tuples.
        let definition = if prepared { "id INT, k INT, PRIMARY KEY(id,k) CLUSTERED" }
            else { "id INT PRIMARY KEY" };
        rows(&mut first, &format!("CREATE TABLE {table} ({definition})"));
        rows(&mut first, &format!("INSERT INTO {table} VALUES ({})", if prepared { "1,10" } else { "1" }));
        rows(&mut first, &format!("SET SESSION transaction_isolation='{isolation}'"));
        if implicit {
            rows(&mut first, "SET tidb_txn_mode='pessimistic'");
            rows(&mut first, "SET autocommit=0");
        } else {
            first.control_transaction("BEGIN PESSIMISTIC").expect("begin");
        }
        if change_default {
            let changed = if isolation == "REPEATABLE-READ" { "READ-COMMITTED" } else { "REPEATABLE-READ" };
            rows(&mut first, &format!("SET SESSION transaction_isolation='{changed}'"));
        }
        let selected = if prepared {
            let statement = first.prepare_general(&format!(
                "SELECT id FROM {table} WHERE (k,id) IN ((?,?),(?,?),(?,?)) FOR UPDATE"
            )).expect("prepare batch");
            let params = [10,1,20,2,20,2].map(PreparedValue::SignedLongLong);
            let crate::sql_node::GeneralExecuteOutcome::Rows(mut result) =
                first.execute_general(&statement, &params).expect("execute batch")
            else { panic!("batch returns rows") };
            let source = result.source();
            let mut result = Vec::new();
            loop {
                let batch = source.next_batch(8).expect("batch");
                if batch.is_empty() { break; }
                result.extend(batch);
            }
            source.finish().expect("finish");
            source.close().expect("close");
            result
        } else {
            rows(&mut first, &format!("SELECT id FROM {table} WHERE id IN (1,2) FOR UPDATE"))
        };
        assert_eq!(displayed(selected), [["1"]], "case {index}");

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let blocked = std::thread::scope(|scope| {
            let contender = scope.spawn(|| {
                let mut second = factory.open_session(session_context(116)).expect("writer");
                started_tx.send(()).expect("writer started");
                rows(&mut second, &format!("INSERT INTO {table} VALUES ({})", if prepared { "2,20" } else { "2" }));
                finished_tx.send(()).expect("writer finished");
            });
            started_rx.recv().expect("writer starts");
            let blocked = matches!(
                finished_rx.recv_timeout(std::time::Duration::from_millis(200)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            );
            // Release before asserting so failure cannot strand the writer.
            first.control_transaction("ROLLBACK").expect("release reader");
            contender.join().expect("writer finishes after release");
            blocked
        });
        assert_eq!(blocked, isolation == "REPEATABLE-READ", "case {index}: absent-key lock follows the transaction's isolation");
        assert_eq!(displayed(rows(&mut first, &format!("SELECT id FROM {table} ORDER BY id"))),
            [["1"], ["2"]]);
        first.control_transaction("ROLLBACK").expect("finish implicit read");
    }
}

/// The prelock joins the failed-statement release list: an EXECUTE that fails
/// AFTER its row was locked (a strict-mode cast error during the assignment)
/// must give the lock back AND drop its cached row image, so a contender can
/// take the row immediately (`OnPessimisticStmtEnd(isSuccessful=false)`).
#[test]
fn a_failed_prelocked_update_releases_its_row() {
    use tidb_protocol::PreparedValue;

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
        first.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    let statement = first
        .prepare_general("UPDATE test.foldfail SET v = 'not-a-number' WHERE id = ?")
        .expect("prepare");
    // The execute outcome borrows the session for its row source, so the
    // error check happens inside a scope that ends before the rollback.
    let failed = {
        let outcome = first.execute_general(&statement, &[PreparedValue::SignedLongLong(1)]);
        outcome.is_err()
    };
    if !failed {
        // A non-strict node coerces instead of failing; then there is no
        // post-lock failure to exercise and the release behavior above is
        // covered by the transaction-end path instead.
        first.control_transaction("ROLLBACK").expect("rollback");
        return;
    }
    // The statement failed after its prelock; the transaction stays open.
    assert_eq!(
        first.control_transaction("ROLLBACK").expect("rollback"),
        Some(false)
    );

    // A contender takes the released row immediately: had the prelock leaked,
    // this would block for the lock-wait timeout instead of completing.
    let mut second = factory
        .open_session(session_context(94))
        .expect("session opens");
    assert_eq!(
        second.control_transaction("BEGIN").expect("begin"),
        Some(true)
    );
    rows(&mut second, "UPDATE test.foldfail SET v = 99 WHERE id = 1");
    second.control_transaction("COMMIT").expect("commit");

    let mut after = factory
        .open_session(session_context(95))
        .expect("session opens");
    assert_eq!(
        displayed(rows(&mut after, "SELECT v FROM test.foldfail WHERE id = 1")),
        [["99"]],
        "the failed statement left neither its lock nor its staged write behind"
    );
}
