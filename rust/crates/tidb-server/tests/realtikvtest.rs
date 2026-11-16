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

//! Go `tests/realtikvtest`: the node's domain boots in-process against the
//! PD that `TIDB_REALTIKV_PD` names (Go `CreateMockStoreAndSetup` with
//! `-with-real-tikv`), so the tests can drive failpoints. Without the
//! variable the tests skip, as Go's realtikv packages run only in their own CI
//! job. Like Go's packages, they run one at a time (`cargo nextest run -j 1
//! ... -E 'test(realtikv_)'`): they share the cluster and its databases.

use tidb_server::ClusterServerSession;
use tidb_session::domain::cluster_session_node::DomainSessions as _;
use tidb_session::domain::resultset_source::ResultSetSource as _;
use tidb_session::domain::session_api::SessionContext;
use tidb_session::Session;

/// Go `realtikvtest.CreateMockStoreAndSetup`: the domain over the cluster,
/// `None` when no cluster is configured.
pub(crate) struct RealTiKV {
    stack: tidb_server::ClusterStack,
    /// Go `store`, the domain's TiKV storage.
    tikv_store: tidb_txnkv::driver::tikv_store::TikvStore,
    pd: String,
    next_connection: std::sync::atomic::AtomicU64,
}

impl RealTiKV {
    pub(crate) fn setup() -> Option<Self> {
        let pd = std::env::var("TIDB_REALTIKV_PD").ok()?;
        // set it to 5 seconds for testing lock resolve.
        tikv_client::MANAGED_LOCK_TTL.store(5000, std::sync::atomic::Ordering::SeqCst);
        tikv_client::PREWRITE_MAX_BACKOFF.store(500, std::sync::atomic::Ordering::SeqCst);
        let temp_dir = std::env::temp_dir().join(format!("tidb-realtikv-{}", std::process::id()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let config = tidb_server::parse_node_config([
            "tidb-server".to_owned(),
            "--store".to_owned(),
            "tikv".to_owned(),
            "--path".to_owned(),
            pd.clone(),
            "-P".to_owned(),
            "0".to_owned(),
            "--socket".to_owned(),
            String::new(),
            "--load-privileges".to_owned(),
            "--temp-dir".to_owned(),
            temp_dir.display().to_string(),
            // Go `vardef.SetSchemaLease(500 * time.Millisecond)`.
            "--lease".to_owned(),
            "500ms".to_owned(),
        ])
        .expect("the realtikv node config");
        let (stack, authority) = tidb_server::realtikv_test_stack(&config).expect("the realtikv domain");
        let tikv_store = authority.tikv_store().clone();
        // The process ends with the test; Go's store stays open to the end too.
        std::mem::forget(authority);
        let store = Self { stack, tikv_store, pd, next_connection: std::sync::atomic::AtomicU64::new(7000) };
        let mut tk = store.new_tk();
        // set it to default value.
        tk.must_exec("set global innodb_lock_wait_timeout = 50");
        tk.must_exec("use test");
        // Go's cleanup unless `retainData`: no DXF task, no DDL job, and no
        // table or view in `test` survives from an earlier test.
        tk.must_exec("delete from mysql.tidb_global_task;");
        tk.must_exec("delete from mysql.tidb_background_subtask;");
        tk.must_exec("delete from mysql.tidb_ddl_job;");
        let tables: Vec<String> = tk
            .must_query("show full tables where table_type = 'BASE TABLE';")
            .into_iter()
            .map(|row| format!("`{}`", row[0]))
            .collect();
        for table in &tables {
            tk.must_exec(&format!("alter table {table} nocache"));
        }
        if !tables.is_empty() {
            tk.must_exec(&format!("drop table {}", tables.join(",")));
        }
        for row in tk.must_query("show full tables where table_type = 'VIEW';") {
            tk.must_exec(&format!("drop view `{}`", row[0]));
        }
        Some(store)
    }

    /// Go `tables/testutil.CheckIndexKVCount`'s count: the index's entries
    /// in table `test.table_name`, read from a snapshot of the store.
    pub(crate) fn index_kv_count(&self, tk: &mut Tk, table_name: &str, index_name: &str) -> usize {
        let table_id: i64 = tk.must_query(&format!(
            "select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = '{table_name}'"
        ))[0][0]
            .parse()
            .unwrap();
        let index_id: i64 = tk.must_query(&format!(
            "select index_id from information_schema.tidb_indexes where table_schema = 'test' and table_name = '{table_name}' and key_name = '{index_name}'"
        ))[0][0]
            .parse()
            .unwrap();
        let prefix = tidb_tablecodec::table_key::encode_table_index_prefix(table_id, index_id);
        let end = tidb_tablecodec::table_key::encode_table_index_prefix(table_id, index_id + 1);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let client = tikv_client::TransactionClient::new(vec![self.pd.clone()]).await.unwrap();
            let ts = client.current_timestamp().await.unwrap();
            let mut snapshot = client.snapshot(ts, tikv_client::TransactionOptions::new_optimistic().read_only());
            snapshot.scan(prefix..end, u32::MAX).await.unwrap().count()
        })
    }

    /// Go `store`: the domain's TiKV storage.
    pub(crate) fn tikv_store(&self) -> &tidb_txnkv::driver::tikv_store::TikvStore {
        &self.tikv_store
    }

    /// The DDL's system sessions (Go tests reach them through the domain).
    pub(crate) fn ddl_session_pool(&self) -> &std::sync::Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool> {
        &self.stack.ddl_session_pool
    }

    /// Go `testkit.NewTestKit(t, store)`: a root connection.
    pub(crate) fn new_tk(&self) -> Tk {
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
pub(crate) struct Tk {
    session: Session,
}

impl Tk {
    pub(crate) fn exec(&mut self, sql: &str) -> Result<Vec<Vec<String>>, String> {
        self.exec_coded(sql).map_err(|(_, message)| message)
    }

    /// The statement's rows, or its error's code and Go `err.Error()` text
    /// (`[class:code]message`). Like Go's testkit, SQL without a statement
    /// runs nothing.
    pub(crate) fn exec_coded(&mut self, sql: &str) -> Result<Vec<Vec<String>>, (u16, String)> {
        if sql.trim().is_empty() {
            return Ok(Vec::new());
        }
        // The wire's path: Go `ParseSQL`, then `ExecuteStmt`.
        let stmt = self
            .session
            .parse_at_statement_boundary(sql)
            .map_err(|error| (1064, error.to_string()))?;
        let conn: ClusterServerSession<'_> = self.session.node_conn();
        let Some(mut result) = conn.execute_stmt(sql, &stmt).map_err(|error| (error.code, error.error_string()))? else {
            return Ok(Vec::new());
        };
        let source = result.source();
        let mut rows = Vec::new();
        loop {
            let batch = source.next_batch(64).map_err(|error| (error.code, error.error_string()))?;
            if batch.is_empty() {
                break;
            }
            rows.extend(batch.into_iter().map(|row| {
                row.iter()
                    .map(|datum| datum.sql_string().unwrap_or_else(|_| "<nil>".to_owned()))
                    .collect()
            }));
        }
        source.finish().map_err(|error| (error.code, error.error_string()))?;
        Ok(rows)
    }

    pub(crate) fn must_exec(&mut self, sql: &str) {
        if let Err(error) = self.exec(sql) {
            panic!("{sql}: {error}");
        }
    }

    pub(crate) fn must_query(&mut self, sql: &str) -> Vec<Vec<String>> {
        self.exec(sql).unwrap_or_else(|error| panic!("{sql}: {error}"))
    }

    pub(crate) fn must_get_err_code(&mut self, sql: &str, code: u16) {
        match self.exec_coded(sql) {
            Ok(_) => panic!("{sql}: expected error {code}"),
            Err((got, message)) => assert_eq!(got, code, "{sql}: {message}"),
        }
    }

    /// Go `tk.Session().AffectedRows()`.
    pub(crate) fn affected_rows(&mut self) -> u64 {
        self.session.affected_rows()
    }

    pub(crate) fn exec_to_err(&mut self, sql: &str) -> String {
        match self.exec(sql) {
            Ok(_) => panic!("{sql}: expected an error"),
            Err(error) => error,
        }
    }
}

/// Go `realtikvtest.CreateMockStoreAndSetup`, or skip the test.
macro_rules! realtikv {
    () => {
        match crate::realtikvtest::RealTiKV::setup() {
            Some(store) => store,
            None => {
                eprintln!("TIDB_REALTIKV_PD is unset: realtikv test skipped");
                return;
            }
        }
    };
}
