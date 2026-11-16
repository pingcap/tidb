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
    authority: tidb_store_driver::real_tikv_read::ProductionReadProcessAuthority,
    /// Go `store`, the domain's TiKV storage.
    tikv_store: tidb_txnkv::driver::tikv_store::TikvStore,
    pd: String,
    next_connection: std::sync::atomic::AtomicU64,
}

impl RealTiKV {
    pub(crate) fn setup() -> Option<Self> {
        let pd = std::env::var("TIDB_REALTIKV_PD").ok()?;
        // Go `realtikvtest.TestMain`: enable client-go failpoints.
        tikv_client::tikv::enable_failpoints();
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
        let store = Self { stack, authority, tikv_store, pd, next_connection: std::sync::atomic::AtomicU64::new(7000) };
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

    /// Go `dom.InfoSyncer().GetSessionManager()` for concurrent transaction observation.
    pub(crate) fn processes(&self) -> tidb_sessmgr::ProcessRegistry {
        self.stack.factory.processes()
    }

    /// The DDL's system sessions (Go tests reach them through the domain).
    pub(crate) fn ddl_session_pool(&self) -> &std::sync::Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool> {
        &self.stack.ddl_session_pool
    }

    /// Go `gcworker.RunResolveLocks` using this domain's shared storage authority.
    pub(crate) fn run_resolve_locks(&self, safe_point: u64) {
        let deps = tidb_gcworker::node::gc_worker_deps(&self.stack.factory, &self.tikv_store,
            std::sync::Arc::new(self.authority.transaction_opener()));
        tidb_gcworker::run_resolve_locks(deps, safe_point, "gc-worker-test-batch-resolve-locks", 1).unwrap();
    }

    /// Go `startTiDBServer` / `openDB`: a real TCP connection over this domain.
    pub(crate) fn new_tcp(&self, multi_statements: bool) -> (TcpTk, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let factory = self.stack.factory.clone();
        let users = self.stack.users.clone();
        let worker = std::thread::spawn(move || {
            let (stream, peer) = listener.accept().unwrap();
            let _ = tidb_server::serve_mysql_connection(stream, peer, Default::default(),
                factory.as_ref(), users.as_ref(), &std::sync::Arc::new(tidb_server::ConnectionTracker::default()),
                tidb_protocol::DEFAULT_MAX_ALLOWED_PACKET);
        });
        (TcpTk::connect(address, multi_statements), worker)
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

impl Drop for RealTiKV {
    /// Go `CreateMockStoreAndDomainAndSetup` cleanup: close the domain,
    /// release the owner/server leases, then close the store.
    fn drop(&mut self) {
        for worker in self.stack.workers.drain(..) {
            drop(worker);
        }
        self.stack.stop_resource_groups();
        // Go `ddl.CloseOwnerManager(store)`: revoke the DDL owner's etcd
        // lease now, or the next test's node waits out its TTL before it
        // can own DDL.
        if let Some(owner) = self.stack.factory.ddl.owner_manager() {
            owner.close();
        }
        for lease in self.stack.leases.drain(..) {
            drop(lease);
        }
        if let Err(error) = self.authority.shutdown() {
            // Do not panic a second time while a failing test unwinds.
            eprintln!("realtikv store shutdown failed: {error:?}");
        }
        tikv_client::PREWRITE_MAX_BACKOFF.store(20000, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Go `testkit.TestKit` on one realtikv connection.
pub(crate) struct Tk {
    session: Session,
}

impl Tk {
    /// Go `testkit.TestKit.Session`.
    pub(crate) fn session(&mut self) -> &mut Session {
        &mut self.session
    }

    /// Go `session.PrepareStmt` (the binary protocol's prepared handle).
    pub(crate) fn prepare(&mut self, sql: &str) -> tidb_session::domain::session_api::PreparedGeneral {
        self.session.node_conn().prepare_general(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"))
    }

    /// Go `session.ExecutePreparedStmt` and `testkit.ResultSetToResult`.
    pub(crate) fn execute_prepared(
        &mut self,
        statement: &tidb_session::domain::session_api::PreparedGeneral,
        values: &[tidb_param::PreparedValue],
    ) -> Result<Vec<Vec<String>>, String> {
        use tidb_session::domain::session_api::GeneralExecuteOutcome;
        let outcome = self.session.node_conn().execute_general(statement, values)
            .map_err(|error| error.error_string())?;
        let GeneralExecuteOutcome::Rows(mut result) = outcome else { return Ok(Vec::new()); };
        let source = result.source();
        let mut rows = Vec::new();
        loop {
            let batch = source.next_batch(64).map_err(|error| error.error_string())?;
            if batch.is_empty() { break; }
            rows.extend(batch.into_iter().map(|row| row.iter().map(|datum|
                datum.sql_string().unwrap_or_else(|_| "<nil>".to_owned())).collect()));
        }
        source.finish().map_err(|error| error.error_string())?;
        Ok(rows)
    }

    /// Go `testkit.TestKit.MustQuery` and `testkit.Result.Check(testkit.Rows(...))`.
    pub(crate) fn check(&mut self, sql: &str, expected: &[&str]) {
        // Go Result.Check compares fmt.Sprintf("%s", row), whose cells are joined by spaces.
        let actual: Vec<String> = self.must_query(sql).iter().map(|row| row.join(" ")).collect();
        assert_eq!(actual, expected, "{sql}");
    }

    /// Go `testkit.Result.Sort().Check(testkit.Rows(...))`.
    pub(crate) fn check_sorted(&mut self, sql: &str, expected: &[&str]) {
        let mut actual = self.must_query(sql);
        actual.sort();
        let actual: Vec<String> = actual.iter().map(|row| row.join(" ")).collect();
        assert_eq!(actual, expected, "{sql}");
    }

    /// Go `testkit.TestKit.MustContainErrMsg`.
    pub(crate) fn must_contain_err_msg(&mut self, sql: &str, message: &str) {
        let error = self.exec_to_err(sql);
        assert!(error.contains(message), "{sql}: expected {message:?}, got {error:?}");
    }

    /// Go `testkit.TestKit.MustHavePlan`.
    pub(crate) fn must_have_plan(&mut self, sql: &str, plan: &str) {
        let rows = self.must_query(&format!("explain {sql}"));
        assert!(rows.iter().any(|row| row[0].contains(plan)), "{sql}: expected {plan}, got {rows:?}");
    }

    pub(crate) fn exec(&mut self, sql: &str) -> Result<Vec<Vec<String>>, String> {
        self.exec_coded(sql).map_err(|(_, message)| message)
    }

    /// Go `testkit.TestKit.ExecWithContext` with `interceptor.WithRPCInterceptor`.
    pub(crate) fn exec_with_interceptor(
        &mut self, interceptor: tikv_client::RpcInterceptorHandle, sql: &str,
    ) -> Result<Vec<Vec<String>>, String> {
        // Go `tk.ExecWithContext(interceptor.WithRPCInterceptor(ctx, ...))`.
        let context = tikv_client::with_rpc_interceptor(&tikv_client::trace::TraceContext::new(), interceptor);
        tikv_client::trace::with_trace_context_sync(context, || self.exec(sql))
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

/// Go `openDB`: the test's MySQL connection, retaining its raw TCP stream.
pub(crate) struct TcpTk {
    pub stream: std::net::TcpStream,
    reader: tidb_protocol::PacketReader<std::net::TcpStream>,
    pub connection_id: u64,
}

impl TcpTk {
    /// Go `mysql.RegisterDialContext` / `openDB`: authenticate root without a password.
    fn connect(address: std::net::SocketAddr, multi_statements: bool) -> Self {
        let stream = std::net::TcpStream::connect(address).unwrap();
        stream.set_read_timeout(Some(std::time::Duration::from_secs(10))).unwrap();
        let mut reader = tidb_protocol::PacketReader::new(stream.try_clone().unwrap());
        let handshake = reader.read_packet().unwrap();
        let version_end = handshake[1..].iter().position(|b| *b == 0).unwrap() + 1;
        let connection_id = u32::from_le_bytes(handshake[version_end+1..version_end+5].try_into().unwrap()) as u64;
        let mut tk = Self { stream, reader, connection_id };
        let capabilities: u32 = (1 << 9) | (1 << 15) | (1 << 19) | (1 << 17) | if multi_statements { 1 << 16 } else { 0 };
        let mut auth = capabilities.to_le_bytes().to_vec();
        auth.extend_from_slice(&(tidb_protocol::DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
        auth.push(46);
        auth.extend_from_slice(&[0; 23]);
        auth.extend_from_slice(b"root\0");
        auth.push(0);
        auth.extend_from_slice(b"mysql_native_password\0");
        tk.write_packet(1, &auth);
        tk.reader.set_sequence(2);
        let reply = tk.reader.read_packet().unwrap();
        assert_eq!(reply[0], 0, "authentication: {reply:?}");
        tk.must_exec("use test");
        tk
    }

    /// Go mysql driver's packet writer.
    fn write_packet(&mut self, sequence: u8, payload: &[u8]) {
        let mut writer = tidb_protocol::PacketWriter::with_sequence(&mut self.stream, sequence);
        writer.write_packet(payload).unwrap();
        writer.flush().unwrap();
    }

    /// Go `mustExec` for setup statements that return an OK packet.
    pub fn must_exec(&mut self, sql: &str) {
        self.send_query(sql);
        let response = self.reader.read_packet().unwrap();
        assert_eq!(response[0], 0, "{sql}: {response:?}");
    }

    /// Go mysql driver's COM_QUERY.
    pub fn send_query(&mut self, sql: &str) {
        let mut command = vec![tidb_server::command::COM_QUERY];
        command.extend_from_slice(sql.as_bytes());
        self.write_packet(0, &command);
        self.reader.set_sequence(1);
    }

    /// Go `sql.Conn.PrepareContext`: read all parameter and column metadata.
    pub fn prepare(&mut self, sql: &str) -> u32 {
        let mut command = vec![tidb_server::command::COM_STMT_PREPARE];
        command.extend_from_slice(sql.as_bytes());
        self.write_packet(0, &command);
        self.reader.set_sequence(1);
        let response = self.reader.read_packet().unwrap();
        assert_eq!(response[0], 0, "prepare {sql}: {response:?}");
        let id = u32::from_le_bytes(response[1..5].try_into().unwrap());
        let columns = u16::from_le_bytes(response[5..7].try_into().unwrap());
        let parameters = u16::from_le_bytes(response[7..9].try_into().unwrap());
        for count in [parameters, columns] {
            for _ in 0..count { self.reader.read_packet().unwrap(); }
            if count > 0 { assert_eq!(self.reader.read_packet().unwrap()[0], 0xfe); }
        }
        id
    }

    /// Go `Stmt.ExecContext` / `Stmt.QueryContext` for a statement with no parameters.
    pub fn send_prepared(&mut self, id: u32) {
        let mut command = vec![tidb_server::command::COM_STMT_EXECUTE];
        command.extend_from_slice(&id.to_le_bytes());
        command.push(0);
        command.extend_from_slice(&1_u32.to_le_bytes());
        self.write_packet(0, &command);
        self.reader.set_sequence(1);
    }

    /// Go `executeBlockingSQL`: the blocked request must fail when the raw socket closes.
    pub fn await_disconnect_error(&mut self) {
        match self.reader.read_packet() {
            Err(_) => {},
            Ok(reply) => assert_eq!(reply.first(), Some(&0xff), "blocked request unexpectedly succeeded: {reply:?}"),
        }
    }
}

impl Drop for TcpTk {
    /// Go `rawConn.Close`.
    fn drop(&mut self) { let _ = self.stream.shutdown(std::net::Shutdown::Both); }
}
