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

//! Go `tests/realtikvtest/addindextest*` over a running cluster: the node's
//! domain boots in-process against the PD that `TIDB_REALTIKV_PD` names (Go
//! `realtikvtest.CreateMockStoreAndSetup` with `-with-real-tikv`), so the
//! tests can drive failpoints. Without the variable the tests skip, as Go's
//! realtikv packages run only in their own CI job. Like Go's package, they
//! run one at a time (`cargo nextest run -j 1 ... -E 'test(realtikv_)'`): they
//! share the cluster and its databases.

use tidb_server::ClusterServerSession;
use tidb_session::domain::cluster_session_node::DomainSessions as _;
use tidb_session::domain::resultset_source::ResultSetSource as _;
use tidb_session::domain::session_api::SessionContext;
use tidb_session::Session;

/// Go `realtikvtest.CreateMockStoreAndSetup`: the domain over the cluster,
/// `None` when no cluster is configured.
struct RealTiKV {
    stack: tidb_server::ClusterStack,
    next_connection: std::sync::atomic::AtomicU64,
}

impl RealTiKV {
    fn setup() -> Option<Self> {
        let pd = std::env::var("TIDB_REALTIKV_PD").ok()?;
        let temp_dir = std::env::temp_dir().join(format!("tidb-realtikv-{}", std::process::id()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let config = tidb_server::parse_node_config([
            "tidb-server".to_owned(),
            "--store".to_owned(),
            "tikv".to_owned(),
            "--path".to_owned(),
            pd,
            "-P".to_owned(),
            "0".to_owned(),
            "--socket".to_owned(),
            String::new(),
            "--load-privileges".to_owned(),
            "--temp-dir".to_owned(),
            temp_dir.display().to_string(),
        ])
        .expect("the realtikv node config");
        let (stack, authority) = tidb_server::realtikv_test_stack(&config).expect("the realtikv domain");
        // The process ends with the test; Go's store stays open to the end too.
        std::mem::forget(authority);
        Some(Self { stack, next_connection: std::sync::atomic::AtomicU64::new(7000) })
    }

    /// Go `testkit.NewTestKit(t, store)`: a root connection.
    fn new_tk(&self) -> Tk {
        let identity = self
            .stack
            .users
            .authenticate_native("root", "127.0.0.1", &[0; 20], &[])
            .expect("root with no password");
        let connection_id = self.next_connection.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let peer: std::net::SocketAddr = "127.0.0.1:4000".parse().unwrap();
        let session = self
            .stack
            .factory
            .open_session(SessionContext {
                connection_id,
                peer_addr: peer.into(),
                identity,
                client_capability: 0,
                tls_state: None,
                compression_algorithm: tidb_mysql::consts::CompressionNone,
                compression_level: 0,
                cancellation: Default::default(),
                close: Default::default(),
            })
            .expect("a session");
        Tk { session }
    }
}

/// Go `testkit.TestKit` on one realtikv connection.
struct Tk {
    session: Session,
}

impl Tk {
    fn exec(&mut self, sql: &str) -> Result<Vec<Vec<String>>, String> {
        self.exec_coded(sql).map_err(|(_, message)| message)
    }

    /// The statement's rows, or its error's code and message.
    fn exec_coded(&mut self, sql: &str) -> Result<Vec<Vec<String>>, (u16, String)> {
        // The wire's path: Go `ParseSQL`, then `ExecuteStmt`.
        let stmt = self
            .session
            .parse_at_statement_boundary(sql)
            .map_err(|error| (1064, error.to_string()))?;
        let conn: ClusterServerSession<'_> = self.session.node_conn();
        let Some(mut result) = conn.execute_stmt(sql, &stmt).map_err(|error| (error.code, error.message))? else {
            return Ok(Vec::new());
        };
        let source = result.source();
        let mut rows = Vec::new();
        loop {
            let batch = source.next_batch(64).map_err(|error| (error.code, error.message))?;
            if batch.is_empty() {
                break;
            }
            rows.extend(batch.into_iter().map(|row| {
                row.iter()
                    .map(|datum| datum.sql_string().unwrap_or_else(|_| "<nil>".to_owned()))
                    .collect()
            }));
        }
        source.finish().map_err(|error| (error.code, error.message))?;
        Ok(rows)
    }

    fn must_exec(&mut self, sql: &str) {
        if let Err(error) = self.exec(sql) {
            panic!("{sql}: {error}");
        }
    }

    fn must_query(&mut self, sql: &str) -> Vec<Vec<String>> {
        self.exec(sql).unwrap_or_else(|error| panic!("{sql}: {error}"))
    }

    fn must_get_err_code(&mut self, sql: &str, code: u16) {
        match self.exec_coded(sql) {
            Ok(_) => panic!("{sql}: expected error {code}"),
            Err((got, message)) => assert_eq!(got, code, "{sql}: {message}"),
        }
    }

    fn exec_to_err(&mut self, sql: &str) -> String {
        match self.exec(sql) {
            Ok(_) => panic!("{sql}: expected an error"),
            Err(error) => error,
        }
    }
}

macro_rules! realtikv {
    () => {
        match RealTiKV::setup() {
            Some(store) => store,
            None => {
                eprintln!("TIDB_REALTIKV_PD is unset: realtikv test skipped");
                return;
            }
        }
    };
}

/// Go `addindextest1.TestAddIndexForCurrentTimestampColumn`.
#[test]
fn test_add_index_for_current_timestamp_column() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task = on;");
    tk.must_exec("create table t (a timestamp default current_timestamp);");
    tk.must_exec("insert into t values ();");
    tk.must_exec("alter table t add index idx(a);");
    tk.must_exec("admin check table t;");
}

/// Go `addindextest1.TestAddUKErrorMessage`.
#[test]
fn test_add_uk_error_message() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task = on;");
    tk.must_exec("create table t (a int primary key, b int);");
    tk.must_exec("insert into t values (5, 1), (10005, 1), (20005, 1), (30005, 1);");
    tk.must_exec("split table t between (1) and (100001) regions 10;");
    let error = tk.exec_to_err("alter table t add unique index uk(b);");
    assert!(error.contains("Duplicate entry '1' for key 't.uk'"), "{error}");
}

/// Go `addindextest1.TestAddIndexInvalidDistTaskVariableSetting`.
#[test]
fn test_add_index_invalid_dist_task_variable_setting() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=off;");
    tk.must_exec("set global tidb_enable_dist_task = on;");
    tk.must_exec("create table t (a int);");
    // errno.ErrUnsupportedDDLOperation
    tk.must_get_err_code("alter table t add index idx(a);", 8200);
    tk.must_get_err_code("alter table t add column b int, add index idx(a);", 8200);
    tk.must_exec("alter table t add column b int, add column c int;");
    tk.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set global tidb_enable_dist_task = off;");
}

/// Go `addindextest1.TestAddIndexDistCancel`, its cancel-is-timely half: an
/// ADMIN CANCEL reaches a DXF add index blocked in a region job.
#[test]
fn test_add_index_dist_cancel_is_timely() {
    let store = realtikv!();
    let mut tk = store.new_tk();
    tk.must_exec("drop database if exists addindexlit;");
    tk.must_exec("create database addindexlit;");
    tk.must_exec("use addindexlit;");
    tk.must_exec("create table t (a int, b int);");
    tk.must_exec("insert into t values (1, 1), (2, 2), (3, 3);");
    let mut tk2 = store.new_tk();
    tk2.must_exec("use addindexlit;");
    tk2.must_exec("set global tidb_ddl_enable_fast_reorg=on;");
    tk.must_exec("set @@global.tidb_enable_dist_task = 1;");
    let (enter_tx, enter) = std::sync::mpsc::sync_channel::<()>(1);
    let _hook = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/beforeExecuteRegionJob",
        move |arg| {
            let ctx = arg.downcast_mut::<tidb_gocontext::Context>().unwrap();
            let _ = enter_tx.try_send(());
            // Wait up to 30 seconds or until the job's context is cancelled.
            let _ = ctx.sleep(std::time::Duration::from_secs(30));
        },
    );
    std::thread::scope(|scope| {
        let ddl = scope.spawn(|| tk2.exec("alter table t add index idx_ba(b, a);"));
        enter.recv().expect("a region job started");
        let job_id = tk.must_query("admin show ddl jobs 1;")[0][0].clone();
        let now = std::time::Instant::now();
        tk.must_exec(&format!("admin cancel ddl jobs {job_id}"));
        let error = ddl.join().unwrap().expect_err("the cancelled ADD INDEX fails");
        assert!(error.contains("Cancelled DDL job"), "{error}");
        // cancel should be timely
        assert!(now.elapsed().as_secs_f64() < 20.0, "{:?}", now.elapsed());
    });
}
