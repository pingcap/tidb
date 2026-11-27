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

//! Go master `pkg/ddl/tests/metadatalock/mdl_test.go`'s materialized view
//! log tests: a write to a base table holds the metadata lock of its
//! implicit MLog (7aa6d52f16d).

use std::sync::mpsc;
use std::time::Duration;

use super::*;

/// A DDL statement run on its own thread; the session comes back with the
/// answer.
struct BackgroundDdl {
    done: mpsc::Receiver<String>,
    thread: std::thread::JoinHandle<Session>,
}

impl BackgroundDdl {
    fn start(mut session: Session, sql: String) -> Self {
        let (done_tx, done) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let _ = done_tx.send(answer(&mut session, &sql));
            session
        });
        Self { done, thread }
    }

    /// The DDL is still waiting after `wait`.
    fn assert_blocked(&self, wait: Duration, what: &str) {
        if let Ok(answer) = self.done.recv_timeout(wait) {
            panic!("{what}: the DDL finished early: {answer}");
        }
    }

    /// The DDL's answer, then its session.
    fn finish(self, what: &str) -> (String, Session) {
        let answer = self
            .done
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|_| panic!("{what}: the DDL did not finish"));
        (answer, self.thread.join().unwrap())
    }
}

/// Signals once, when `matches` accepts a job reaching Go
/// `beforeWaitSchemaSynced`.
fn signal_before_wait_schema_synced(
    matches: impl Fn(&tidb_model::Job, i64) -> bool + Send + Sync + 'static,
) -> (tidb_failpoint::Guard, mpsc::Receiver<()>) {
    let (signal_tx, signal) = mpsc::channel();
    let signal_tx = std::sync::Mutex::new(Some(signal_tx));
    let guard = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced", move |arg| {
        let (job, schema_version) = arg.downcast_mut::<(tidb_model::Job, i64)>().unwrap();
        if matches(job, *schema_version) {
            if let Some(signal_tx) = signal_tx.lock().unwrap().take() {
                let _ = signal_tx.send(());
            }
        }
    });
    (guard, signal)
}

fn rows_of(session: &mut Session, sql: &str) -> String {
    let rows = answer(session, sql);
    assert!(rows.starts_with("RS:"), "{sql}: {rows}");
    rows
}

/// Go `TestMDLCreateMaterializedViewLogNewTxnWriteMLog`: CREATE
/// MATERIALIZED VIEW LOG waits for a running writer, and a later
/// transaction writes the log. Then, for each write a transaction runs
/// (also from a cached plan), ALTER ... ADD COLUMN on the base table's log
/// waits until the writer ends, a new view taken after it includes every old
/// write, and later writes fill the new log column. A transaction that read
/// the base table before the ADD COLUMN, or began before the log existed,
/// still logs its write in the log's current schema.
#[test]
fn test_mdl_create_materialized_view_log_new_txn_write_m_log() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    let mut tk_ddl = stack.factory.open_session(session_context(2201)).unwrap();
    must_exec_all(&mut tk, &["use test", "set tidb_mview_enable = on"]);
    must_exec_all(&mut tk_ddl, &["set tidb_mview_enable = on"]);
    must_exec_all(&mut tk, &["create table t(a int)", "begin", "insert into t values (1)"]);

    {
        let (_guard, mdl_wait) = signal_before_wait_schema_synced(|job, schema_version| {
            job.type_ == tidb_model::ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG && schema_version != 0
        });
        let ddl = BackgroundDdl::start(tk_ddl, "create materialized view log on test.t (a)".to_owned());
        mdl_wait
            .recv_timeout(Duration::from_secs(10))
            .expect("CREATE MATERIALIZED VIEW LOG enters schema sync");
        ddl.assert_blocked(Duration::ZERO, "create materialized view log should be blocked by running transaction");
        must_exec_all(&mut tk, &["commit"]);
        let (done, session) = ddl.finish("create materialized view log should finish after transaction commit");
        assert_eq!(done, "OK");
        tk_ddl = session;
    }

    must_exec_all(&mut tk, &["begin", "insert into t values (2)", "commit"]);
    assert_eq!(answer(&mut tk, "select a from t order by a"), r#"RS:[["1"],["2"]]"#);
    assert_eq!(
        answer(&mut tk, "select a, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t` order by a"),
        r#"RS:[["2","I","1"]]"#
    );

    must_exec_all(&mut tk_ddl, &["use test"]);
    let cases = [
        ("add_column_insert", "insert into %s values (2, 20)", "commit", false),
        ("add_column_update", "update %s set b = 20 where id = 1", "commit", false),
        ("add_column_delete", "delete from %s where id = 1", "commit", false),
        ("add_column_rollback", "update %s set b = 20 where id = 1", "rollback", false),
        ("add_column_plan_cache", "", "commit", true),
    ];
    for (name, write, finish, prepared) in cases {
        let table_name = format!("t_mlog_{name}");
        let mlog_name = format!("$mlog${table_name}");
        let mv_name = format!("mv_mlog_{name}");
        must_exec_all(&mut tk, &[
            format!("create table {table_name}(id int primary key, b int not null)").as_str(),
            format!("insert into {table_name} values (1, 10)").as_str(),
            format!("create materialized view log on {table_name} (id)").as_str(),
        ]);
        if prepared {
            must_exec_all(&mut tk, &[
                "set tidb_enable_prepared_plan_cache = 1",
                format!("prepare mlog_update_stmt from 'update {table_name} set b = ? where id = 1'").as_str(),
                "begin pessimistic",
                "set @b = 10",
                "execute mlog_update_stmt using @b",
                "execute mlog_update_stmt using @b",
            ]);
            assert_eq!(answer(&mut tk, "select @@last_plan_from_cache"), r#"RS:[["1"]]"#, "{name}");
            must_exec_all(&mut tk, &["commit"]);
        }

        must_exec_all(&mut tk, &["begin pessimistic"]);
        if prepared {
            must_exec_all(&mut tk, &["set @b = 20", "execute mlog_update_stmt using @b"]);
            assert_eq!(answer(&mut tk, "select @@last_plan_from_cache"), r#"RS:[["1"]]"#, "{name}");
        } else {
            must_exec_all(&mut tk, &[write.replace("%s", &table_name).as_str()]);
        }

        let (guard, ddl_started) = {
            let mlog_name = mlog_name.clone();
            signal_before_wait_schema_synced(move |job, _| {
                job.type_ == tidb_model::ActionType::ACTION_ADD_COLUMN
                    && job.table_name.to_string() == mlog_name
                    && job.schema_state == tidb_model::SchemaState::DELETE_ONLY
            })
        };
        let ddl = BackgroundDdl::start(tk_ddl, format!("alter materialized view log on {table_name} add column (b)"));
        ddl_started
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|_| panic!("{name}: MLog ADD COLUMN did not start"));
        ddl.assert_blocked(Duration::from_millis(200), &format!("{name}: MLog ADD COLUMN must wait for the base-table writer"));
        must_exec_all(&mut tk, &[finish]);
        let (done, session) = ddl.finish(&format!("{name}: MLog ADD COLUMN did not finish after the writer ended"));
        assert_eq!(done, "OK", "{name}");
        tk_ddl = session;
        drop(guard);

        // "A new MV must include every old writer before it starts consuming
        // changes to the newly tracked column from the MLog."
        let base_query = format!("select id, sum(b), count(1) from {table_name} group by id order by id");
        let mv_query = format!("select id, s, cnt from {mv_name} order by id");
        must_exec_all(&mut tk_ddl, &[format!(
            "create materialized view {mv_name} (id, s, cnt) refresh fast as select id, sum(b), count(1) from {table_name} group by id"
        ).as_str()]);
        assert_eq!(rows_of(&mut tk_ddl, &base_query), rows_of(&mut tk_ddl, &mv_query), "{name}");
        if prepared {
            must_exec_all(&mut tk, &["set @b = 30", "execute mlog_update_stmt using @b"]);
            assert_eq!(answer(&mut tk, "select @@last_plan_from_cache"), r#"RS:[["0"]]"#, "{name}");
        } else {
            must_exec_all(&mut tk, &[format!("update {table_name} set b = b + 1").as_str()]);
        }
        must_exec_all(&mut tk, &[format!("insert into {table_name} values (3, 30)").as_str()]);
        assert_eq!(
            answer(&mut tk_ddl, &format!("select b from `{mlog_name}` where id = 3")),
            r#"RS:[["30"]]"#,
            "{name}"
        );
        must_exec_all(&mut tk_ddl, &[format!("refresh materialized view {mv_name} fast").as_str()]);
        assert_eq!(rows_of(&mut tk_ddl, &base_query), rows_of(&mut tk_ddl, &mv_query), "{name}");
        // Go's deferred rollback.
        must_exec_all(&mut tk, &["rollback"]);
    }

    // transaction_before_add_column
    must_exec_all(&mut tk, &[
        "create table t_mlog_old_schema(id int primary key, b int not null)",
        "insert into t_mlog_old_schema values (1, 10)",
        "create materialized view log on t_mlog_old_schema (id)",
        "begin pessimistic",
    ]);
    // "Reading the base table must not lock its MLog. The subsequent write
    // must still acquire the MLog dependency when the base MDL is held."
    assert_eq!(answer(&mut tk, "select b from t_mlog_old_schema"), r#"RS:[["10"]]"#);
    must_exec_all(&mut tk_ddl, &["alter materialized view log on t_mlog_old_schema add column (b)"]);
    must_exec_all(&mut tk, &["update t_mlog_old_schema set b = 20 where id = 1", "commit"]);
    assert_eq!(
        answer(&mut tk_ddl, "select b, `_MLOG$_OLD_NEW` from `$mlog$t_mlog_old_schema` order by `_MLOG$_OLD_NEW`"),
        r#"RS:[["10","-1"],["20","1"]]"#
    );
    must_exec_all(&mut tk, &["rollback"]);

    // transaction_before_create_mlog
    must_exec_all(&mut tk, &[
        "create table t_mlog_new_schema(id int primary key, b int not null)",
        "insert into t_mlog_new_schema values (1, 10)",
        "begin pessimistic",
    ]);
    must_exec_all(&mut tk_ddl, &["create materialized view log on t_mlog_new_schema (id, b)"]);
    must_exec_all(&mut tk, &["update t_mlog_new_schema set b = 20 where id = 1", "commit"]);
    assert_eq!(
        answer(&mut tk_ddl, "select b, `_MLOG$_OLD_NEW` from `$mlog$t_mlog_new_schema` order by `_MLOG$_OLD_NEW`"),
        r#"RS:[["10","-1"],["20","1"]]"#
    );
    must_exec_all(&mut tk, &["rollback"]);
}
