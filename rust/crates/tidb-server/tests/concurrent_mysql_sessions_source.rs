// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Condvar, Mutex};
use std::time::{Duration, Instant};

use sha1::{Digest, Sha1};
use tidb_datatype::{Datum, FieldTypeCode};
use tidb_protocol::{
    ColumnInfo, PacketReader, PacketWriter, BINARY_DEFAULT_COLLATION_ID, COM_PING, COM_QUERY,
    COM_QUIT, DEFAULT_MAX_ALLOWED_PACKET,
};
use tidb_server::{
    ActiveQueryCancellation, ConcurrentSqlNode, ConfiguredUserStore, ConnectionCancellation,
    NodeConfig, QueryCancellationLease, QueryResult, QuerySession, QuerySessionFactory,
    ResultSetSource, SessionContext, SqlQueryError,
};
use tidb_util::globalconn::{parse_conn_id, Gcid};

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

struct Session;

impl QuerySession for Session {
    fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        unreachable!("these sessions only log in; Go's login issues no statement")
    }

    fn set_collation(&mut self, collation_id: u8) -> Result<(), SqlQueryError> {
        // Go TiDBDriver.OpenCtx installs the handshake collation (46,
        // utf8mb4_bin) directly, before commands.
        assert_eq!(collation_id, 46);
        Ok(())
    }
}

struct BarrierFactory {
    barrier: Arc<Barrier>,
    contexts: Mutex<Vec<SessionContext>>,
    opening: AtomicUsize,
    max_opening: AtomicUsize,
}

impl QuerySessionFactory for BarrierFactory {
    type Session = Session;

    fn open_session(&self, context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        self.contexts.lock().unwrap().push(context);
        let opening = self.opening.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_opening.fetch_max(opening, Ordering::AcqRel);
        self.barrier.wait();
        self.opening.fetch_sub(1, Ordering::AcqRel);
        Ok(Session)
    }
}

fn config() -> NodeConfig {
    config_with_token_limit("1000")
}

fn config_with_token_limit(limit: &str) -> NodeConfig {
    NodeConfig::parse([
        "tidb-server",
        "--path",
        "127.0.0.1:2379",
        "--auth-file",
        "/tmp/campaign21-users.tsv",
        "--max-connections",
        "3",
        "--port",
        "0",
        "--token-limit",
        limit,
    ])
    .unwrap()
}

fn users() -> ConfiguredUserStore {
    ConfiguredUserStore::parse(
        "alice\t%\tmysql_native_password\t*14E65567ABDB5135D0CFD9A70B3032C179A49EE7\n",
    )
    .unwrap()
}

fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) {
    let mut writer = PacketWriter::with_sequence(stream, sequence);
    writer.write_packet(payload).unwrap();
    writer.flush().unwrap();
}

fn handshake_fields(initial: &[u8]) -> (u32, [u8; 20]) {
    assert_eq!(initial[0], 10);
    let version_end = initial[1..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| offset + 1)
        .unwrap();
    let connection = version_end + 1;
    let connection_id = u32::from_le_bytes(initial[connection..connection + 4].try_into().unwrap());
    let first = connection + 4;
    let second = first + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10;
    let mut salt = [0; 20];
    salt[..8].copy_from_slice(&initial[first..first + 8]);
    salt[8..].copy_from_slice(&initial[second..second + 12]);
    (connection_id, salt)
}

fn native_response(password: &[u8], salt: &[u8]) -> [u8; 20] {
    let stage_one = Sha1::digest(password);
    let stage_two = Sha1::digest(stage_one);
    let mut challenge = Sha1::new();
    challenge.update(salt);
    challenge.update(stage_two);
    let challenge = challenge.finalize();
    let mut response = [0; 20];
    for index in 0..response.len() {
        response[index] = stage_one[index] ^ challenge[index];
    }
    response
}

fn authenticate_ping_quit(address: std::net::SocketAddr) -> u32 {
    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    reader.set_sequence(0);
    let (connection_id, salt) = handshake_fields(&reader.read_packet().unwrap());
    let capabilities = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH
        | CLIENT_CONNECT_ATTRS
        | CLIENT_DEPRECATE_EOF;
    let auth = native_response(b"secret", &salt);
    let mut response = Vec::new();
    response.extend_from_slice(&capabilities.to_le_bytes());
    response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    response.push(46);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(b"alice\0");
    response.push(20);
    response.extend_from_slice(&auth);
    response.extend_from_slice(b"mysql_native_password\0");
    response.push(0);
    write_packet(&mut client, 1, &response);
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    write_packet(&mut client, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    write_packet(&mut client, 0, &[COM_QUIT]);
    connection_id
}

fn authenticate(address: std::net::SocketAddr) -> (TcpStream, PacketReader<TcpStream>) {
    let mut client = TcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    reader.set_sequence(0);
    let (_, salt) = handshake_fields(&reader.read_packet().unwrap());
    let capabilities = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH
        | CLIENT_CONNECT_ATTRS
        | CLIENT_DEPRECATE_EOF;
    let auth = native_response(b"secret", &salt);
    let mut response = Vec::new();
    response.extend_from_slice(&capabilities.to_le_bytes());
    response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    response.push(46);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(b"alice\0");
    response.push(20);
    response.extend_from_slice(&auth);
    response.extend_from_slice(b"mysql_native_password\0");
    response.push(0);
    write_packet(&mut client, 1, &response);
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    (client, reader)
}

struct BlockingQueryState {
    entered: AtomicBool,
    cancelled: Mutex<bool>,
    wake: Condvar,
}

struct BlockingCancellation {
    state: Arc<BlockingQueryState>,
}

impl ActiveQueryCancellation for BlockingCancellation {
    fn cancel(&self) {
        *self.state.cancelled.lock().unwrap() = true;
        self.state.wake.notify_all();
    }
}

struct BlockingResultSet {
    state: Arc<BlockingQueryState>,
    _cancellation_lease: QueryCancellationLease,
}

impl ResultSetSource for BlockingResultSet {
    fn next_batch(
        &mut self,
        _max_rows: usize,
    ) -> Result<Vec<Vec<Datum>>, tidb_executor::MysqlError> {
        self.state.entered.store(true, Ordering::Release);
        let mut cancelled = self.state.cancelled.lock().unwrap();
        while !*cancelled {
            cancelled = self.state.wake.wait(cancelled).unwrap();
        }
        Err("query cancelled by connection shutdown".into())
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, tidb_executor::MysqlError> {
        Ok(vec![ColumnInfo {
            schema: "campaign21".to_owned(),
            table: "rows".to_owned(),
            org_table: "rows".to_owned(),
            name: "id".to_owned(),
            org_name: "id".to_owned(),
            column_length: 20,
            charset: BINARY_DEFAULT_COLLATION_ID,
            flag: 0x0003,
            decimal: 0,
            type_code: FieldTypeCode::LongLong.mysql_type(),
            default_value: None,
        }])
    }

    fn finish(&mut self) -> Result<(), tidb_executor::MysqlError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), tidb_executor::MysqlError> {
        Ok(())
    }
}

struct BlockingQuerySession {
    cancellation: ConnectionCancellation,
    state: Arc<BlockingQueryState>,
}

impl QuerySession for BlockingQuerySession {
    fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        let cancellation = Arc::new(BlockingCancellation {
            state: Arc::clone(&self.state),
        });
        let lease = self.cancellation.install(cancellation);
        Ok(QueryResult::new(Box::new(BlockingResultSet {
            state: Arc::clone(&self.state),
            _cancellation_lease: lease,
        })))
    }
}

struct BlockingQueryFactory {
    state: Arc<BlockingQueryState>,
}

#[test]
fn command_token_is_shared_across_sockets_and_held_through_result_streaming() {
    for max_connections in [0, 3] {
        command_token_streaming_case(max_connections);
    }
}

fn command_token_streaming_case(max_connections: usize) {
    let state = Arc::new(BlockingQueryState {
        entered: AtomicBool::new(false),
        cancelled: Mutex::new(false),
        wake: Condvar::new(),
    });
    let factory = Arc::new(BlockingQueryFactory {
        state: Arc::clone(&state),
    });
    let mut config = config_with_token_limit("1");
    config.max_connections = max_connections;
    let node = ConcurrentSqlNode::bind(&config, factory, users().into()).unwrap();
    let address = node.local_addr().unwrap();
    let shutdown = node.shutdown_handle();
    let server = std::thread::spawn(move || node.run());
    let (mut first, first_reader) = authenticate(address);
    write_packet(
        &mut first,
        0,
        &[&[COM_QUERY][..], b"select blocked"].concat(),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while !state.entered.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "first result stream did not start"
        );
        std::thread::yield_now();
    }

    // An independent server gets its own permit even while this one is busy.
    let other =
        ConcurrentSqlNode::bind(&config, Arc::new(CommandFailureFactory), users().into()).unwrap();
    let other_address = other.local_addr().unwrap();
    let other_shutdown = other.shutdown_handle();
    let other_server = std::thread::spawn(move || other.run());
    let (mut other_client, mut other_reader) = authenticate(other_address);
    other_client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write_packet(&mut other_client, 0, &[COM_PING]);
    other_reader.set_sequence(1);
    let independent_response = other_reader.read_packet();

    // Handshake is outside command admission even while the only token is held.
    let (mut second, mut reader) = authenticate(address);
    second
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    write_packet(&mut second, 0, &[COM_PING]);
    reader.set_sequence(1);
    let early = reader.read_packet();

    // Release before any assertions so the unchanged implementation also drains.
    *state.cancelled.lock().unwrap() = true;
    state.wake.notify_all();
    drop(first_reader);
    drop(first);
    write_packet(&mut other_client, 0, &[COM_QUIT]);
    drop(other_reader);
    drop(other_client);
    other_shutdown.shutdown();
    other_server.join().unwrap().unwrap();
    second
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let response = match early.as_ref() {
        Ok(packet) => packet.clone(),
        Err(_) => reader.read_packet().unwrap(),
    };
    write_packet(&mut second, 0, &[COM_QUIT]);
    drop(reader);
    drop(second);
    shutdown.shutdown();
    server.join().unwrap().unwrap();
    assert_eq!(
        response[0], 0,
        "PING succeeds after the query releases its token"
    );
    assert!(early.is_err(), "PING bypassed the server's token limit");
    assert_eq!(
        independent_response.unwrap()[0],
        0,
        "independent server shared the busy server's permits"
    );
}

struct CommandFailureFactory;

struct CommandFailureSession;

impl QuerySessionFactory for CommandFailureFactory {
    type Session = CommandFailureSession;

    fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        Ok(CommandFailureSession)
    }
}

impl QuerySession for CommandFailureSession {
    fn execute<'a>(&'a mut self, sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        assert_ne!(sql, "panic", "injected command panic");
        Err(SqlQueryError::unknown("injected command error"))
    }
}

#[test]
fn command_token_releases_on_error_no_response_quit_protocol_failure_and_panic() {
    let node = ConcurrentSqlNode::bind(
        &config_with_token_limit("1"),
        Arc::new(CommandFailureFactory),
        users().into(),
    )
    .unwrap();
    let address = node.local_addr().unwrap();
    let tracker = node.tracker();
    let server = std::thread::spawn(move || node.serve_connections(4));
    let (mut first, mut reader) = authenticate(address);
    first
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    for command in [
        [&[COM_QUERY][..], b"error"].concat(),
        [&[tidb_protocol::COM_STMT_PREPARE][..], b"select ?"].concat(),
        // Unknown prepared handle: dispatch returns a protocol ERR.
        vec![tidb_protocol::COM_STMT_EXECUTE, 123, 0, 0, 0, 0, 1, 0, 0, 0],
        vec![0xfe],
    ] {
        write_packet(&mut first, 0, &command);
        reader.set_sequence(1);
        assert_eq!(reader.read_packet().unwrap()[0], 0xff);
    }
    // COM_STMT_CLOSE has no response, but still must return its command token.
    write_packet(
        &mut first,
        0,
        &[tidb_protocol::COM_STMT_CLOSE, 123, 0, 0, 0],
    );
    write_packet(&mut first, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    write_packet(&mut first, 0, &[COM_QUIT]);
    drop(reader);
    drop(first);

    for command in [
        vec![tidb_protocol::COM_SET_OPTION],
        [&[COM_QUERY][..], b"panic"].concat(),
    ] {
        let (mut client, mut reader) = authenticate(address);
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        write_packet(&mut client, 0, &command);
        reader.set_sequence(1);
        // Go attempts an ERR before closing a panicked connection. This test
        // checks retirement and permit release; panic_recovery_source checks
        // the error response across commands and negotiated transports.
        match reader.read_packet() {
            Ok(packet) => {
                assert_eq!(packet[0], 0xff);
                assert!(matches!(
                    reader.read_packet(),
                    Err(tidb_protocol::PacketError::EndOfStream)
                ));
            }
            Err(error) => assert!(matches!(error, tidb_protocol::PacketError::EndOfStream)),
        }
    }
    let (mut last, mut reader) = authenticate(address);
    last.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    write_packet(&mut last, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    write_packet(&mut last, 0, &[COM_QUIT]);
    server.join().unwrap().unwrap();
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.completed(), 4);
}

impl QuerySessionFactory for BlockingQueryFactory {
    type Session = BlockingQuerySession;

    fn open_session(&self, context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        Ok(BlockingQuerySession {
            cancellation: context.cancellation,
            state: Arc::clone(&self.state),
        })
    }
}

#[test]
fn fixed_workers_hold_three_authenticated_sessions_concurrently_and_drain_all() {
    // pkg/server/server.go:549-625 startNetworkListener
    // pkg/server/server.go:753-855 onConn
    // pkg/server/tests/commontest/tidb_test.go:3206 TestConnectionWillNotLeak
    // pkg/server/tests/commontest/tidb_test.go:3295 TestConnectionCount
    let config = config();
    let barrier = Arc::new(Barrier::new(4));
    let factory = Arc::new(BarrierFactory {
        barrier: Arc::clone(&barrier),
        contexts: Mutex::new(Vec::new()),
        opening: AtomicUsize::new(0),
        max_opening: AtomicUsize::new(0),
    });
    let node = ConcurrentSqlNode::bind(&config, Arc::clone(&factory), Arc::new(users())).unwrap();
    let address = node.local_addr().unwrap();
    let tracker = node.tracker();
    let server = std::thread::spawn(move || node.serve_connections(3).unwrap());

    let clients = (0..3)
        .map(|_| std::thread::spawn(move || authenticate_ping_quit(address)))
        .collect::<Vec<_>>();
    barrier.wait();
    let mut client_ids = clients
        .into_iter()
        .map(|client| client.join().unwrap())
        .collect::<Vec<_>>();
    server.join().unwrap();

    client_ids.sort_unstable();
    let expected_ids = [1, 2, 3].map(|local_conn_id| {
        Gcid {
            server_id: 1,
            local_conn_id,
            is_64bits: false,
        }
        .to_conn_id()
    });
    assert_eq!(
        client_ids,
        expected_ids.map(|id| u32::try_from(id).unwrap())
    );
    for id in &client_ids {
        let (gcid, truncated) = parse_conn_id(u64::from(*id)).unwrap();
        assert!(!truncated);
        assert_eq!(gcid.server_id, 1);
        assert!(!gcid.is_64bits);
    }
    let mut session_ids = factory
        .contexts
        .lock()
        .unwrap()
        .iter()
        .map(|context| context.connection_id)
        .collect::<Vec<_>>();
    session_ids.sort_unstable();
    assert_eq!(session_ids, expected_ids);
    assert_eq!(factory.max_opening.load(Ordering::Acquire), 3);
    assert_eq!(tracker.max_active(), 3);
    assert!(tracker.max_active() <= config.max_connections);
    assert_eq!(tracker.accepted(), 3);
    assert_eq!(tracker.completed(), 3);
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.failed(), 0);
}

#[test]
fn shutdown_stops_acceptance_and_forces_a_stalled_connection_after_grace() {
    // pkg/server/server_test.go:238 TestServerShutdownFlags
    // pkg/server/tests/commontest/tidb_test.go:1098 TestGracefulShutdown
    let mut config = config();
    config.max_connections = 1;
    let factory = Arc::new(BarrierFactory {
        barrier: Arc::new(Barrier::new(1)),
        contexts: Mutex::new(Vec::new()),
        opening: AtomicUsize::new(0),
        max_opening: AtomicUsize::new(0),
    });
    let node = ConcurrentSqlNode::bind(&config, factory, Arc::new(users()))
        .unwrap()
        .with_shutdown_grace(Duration::from_millis(20));
    let address = node.local_addr().unwrap();
    let tracker = node.tracker();
    let shutdown = node.shutdown_handle();
    let server = std::thread::spawn(move || node.run().unwrap());

    let client = TcpStream::connect(address).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while tracker.active() != 1 {
        assert!(Instant::now() < deadline, "connection was not admitted");
        std::thread::sleep(Duration::from_millis(1));
    }
    shutdown.shutdown();
    server.join().unwrap();
    drop(client);

    assert_eq!(tracker.accepted(), 1);
    assert_eq!(tracker.completed(), 1);
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.failed(), 0);
}

#[test]
fn forced_shutdown_cancels_an_inflight_com_query_before_joining_worker() {
    // pkg/server/server_test.go:238 TestServerShutdownFlags
    // pkg/server/tests/commontest/tidb_test.go:1098 TestGracefulShutdown
    let mut config = config_with_token_limit("1");
    config.max_connections = 2;
    let state = Arc::new(BlockingQueryState {
        entered: AtomicBool::new(false),
        cancelled: Mutex::new(false),
        wake: Condvar::new(),
    });
    let factory = Arc::new(BlockingQueryFactory {
        state: Arc::clone(&state),
    });
    let node = ConcurrentSqlNode::bind(&config, factory, Arc::new(users()))
        .unwrap()
        .with_shutdown_grace(Duration::from_millis(20));
    let address = node.local_addr().unwrap();
    let tracker = node.tracker();
    let shutdown = node.shutdown_handle();
    let server = std::thread::spawn(move || node.run().unwrap());

    let (mut client, _reader) = authenticate(address);
    let mut query = vec![COM_QUERY];
    query.extend_from_slice(b"SELECT id FROM campaign21.rows");
    write_packet(&mut client, 0, &query);
    let deadline = Instant::now() + Duration::from_secs(2);
    while !state.entered.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "COM_QUERY did not enter result pulling"
        );
        std::thread::sleep(Duration::from_millis(1));
    }

    // A second connection waits on the same command owner. Cancelling its
    // running peer must release admission so the shutdown barrier can retire
    // both connections.
    let (mut waiting, mut waiting_reader) = authenticate(address);
    waiting
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    write_packet(&mut waiting, 0, &[COM_PING]);
    waiting_reader.set_sequence(1);
    let early = waiting_reader.read_packet();
    shutdown.shutdown();
    server.join().unwrap();
    assert!(early.is_err(), "waiting command bypassed admission");
    assert!(*state.cancelled.lock().unwrap());
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.accepted(), 2);
    assert_eq!(tracker.completed(), 2);
    assert_eq!(tracker.failed(), 0);
}

#[test]
fn command_dispatch_exports_success_and_error_counters() {
    let factory = Arc::new(BarrierFactory {
        barrier: Arc::new(Barrier::new(1)),
        contexts: Mutex::new(Vec::new()),
        opening: AtomicUsize::new(0),
        max_opening: AtomicUsize::new(0),
    });
    let node = ConcurrentSqlNode::bind(&config(), factory, Arc::new(users())).unwrap();
    let address = node.local_addr().unwrap();
    let tracker = node.tracker();
    let shutdown = node.shutdown_handle();
    let server = std::thread::spawn(move || node.run().unwrap());
    let (mut client, mut reader) = authenticate(address);
    write_packet(&mut client, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    write_packet(&mut client, 0, &[127]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0xff);
    write_packet(&mut client, 0, &[COM_QUIT]);
    let deadline = Instant::now() + Duration::from_secs(3);
    while tracker.completed() == 0 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    shutdown.shutdown();
    server.join().unwrap();
    let text = prometheus::TextEncoder::new()
        .encode_to_string(&prometheus::gather())
        .unwrap();
    assert!(
        text.contains("tidb_server_handle_query_duration_seconds_count{"),
        "completed commands must observe duration: {text}"
    );
    for (command, result) in [("Ping", "OK"), ("127", "Error")] {
        assert!(
            text.lines()
                .any(|line| line.starts_with("tidb_server_query_total{")
                    && line.contains(&format!("type=\"{command}\""))
                    && line.contains(&format!("result=\"{result}\""))
                    && line.contains("resource_group=\"default\"")
                    && line.rsplit_once(' ').unwrap().1.parse::<f64>().unwrap() >= 1.0),
            "missing completed {command}/{result} command metric: {text}"
        );
    }
}

struct MetricsSession {
    group: &'static str,
}
struct MetricsFactory;
impl QuerySessionFactory for MetricsFactory {
    type Session = MetricsSession;
    fn open_session(&self, _: SessionContext) -> Result<MetricsSession, SqlQueryError> {
        Ok(MetricsSession {
            group: "metrics-before",
        })
    }
}
impl QuerySession for MetricsSession {
    fn control_transaction(&mut self, sql: &str) -> Result<Option<bool>, SqlQueryError> {
        Ok((sql == "BEGIN").then_some(true))
    }
    fn metrics_resource_group(&self) -> &str {
        self.group
    }
    fn metrics_statement_resource_group(&self) -> &str {
        "metrics-statement-hint"
    }
    fn parse_statement(&mut self, sql: &str) -> Result<Option<tidb_ast::Stmt>, SqlQueryError> {
        tidb_parser::parse(sql)
            .map(Some)
            .map_err(|error| SqlQueryError::unknown(format!("{error:?}")))
    }
    fn execute_write(
        &mut self,
        sql: &str,
    ) -> Result<Option<tidb_server::WriteOutcome>, SqlQueryError> {
        if sql.starts_with("SET") {
            self.group = "metrics-after";
        } else if !sql.starts_with("CREATE") {
            return Ok(None);
        }
        Ok(Some(tidb_server::WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        }))
    }
    fn execute<'a>(&'a mut self, _: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        Err(SqlQueryError::unknown("metrics fixture execution error"))
    }
}

#[test]
fn query_metrics_use_post_dispatch_group_and_exclude_text_ddl() {
    let node =
        ConcurrentSqlNode::bind(&config(), Arc::new(MetricsFactory), Arc::new(users())).unwrap();
    let address = node.local_addr().unwrap();
    let tracker = node.tracker();
    let shutdown = node.shutdown_handle();
    let server = std::thread::spawn(move || node.run().unwrap());
    let (mut client, mut reader) = authenticate(address);
    for (sql, expected) in [
        ("SET @x=1", 0),
        ("CREATE TABLE t(id INT)", 0),
        ("SELECT 1", 0xff),
    ] {
        let mut payload = vec![COM_QUERY];
        payload.extend_from_slice(sql.as_bytes());
        write_packet(&mut client, 0, &payload);
        reader.set_sequence(1);
        assert_eq!(reader.read_packet().unwrap()[0], expected);
    }
    write_packet(&mut client, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    write_packet(&mut client, 0, &[COM_QUIT]);
    let deadline = Instant::now() + Duration::from_secs(3);
    while tracker.completed() == 0 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    shutdown.shutdown();
    server.join().unwrap();
    let text = prometheus::TextEncoder::new()
        .encode_to_string(&prometheus::gather())
        .unwrap();
    for result in ["OK", "Error"] {
        let line = text
            .lines()
            .find(|line| {
                line.starts_with("tidb_server_query_total{")
                    && line.contains("type=\"Query\"")
                    && line.contains("resource_group=\"metrics-after\"")
                    && line.contains(&format!("result=\"{result}\""))
            })
            .expect("post-dispatch group counter");
        assert!(line.ends_with(" 1"), "DDL must not contribute: {line}");
    }
    assert!(
        text.lines().any(|line| line
            .starts_with("tidb_server_handle_query_duration_seconds_count{")
            && line.contains("sql_type=\"Select\"")
            && line.contains("resource_group=\"metrics-statement-hint\"")
            && line.ends_with(" 3")),
        "{text}"
    );
}

#[test]
fn prepared_execute_exports_cached_statement_label() {
    let node =
        ConcurrentSqlNode::bind(&config(), Arc::new(MetricsFactory), Arc::new(users())).unwrap();
    let address = node.local_addr().unwrap();
    let tracker = node.tracker();
    let shutdown = node.shutdown_handle();
    let server = std::thread::spawn(move || node.run().unwrap());
    let (mut client, mut reader) = authenticate(address);
    let mut prepare = vec![22];
    prepare.extend_from_slice(b"BEGIN");
    write_packet(&mut client, 0, &prepare);
    reader.set_sequence(1);
    let response = reader.read_packet().unwrap();
    assert_eq!(response[0], 0);
    let statement_id = &response[1..5];
    let mut execute = vec![23];
    execute.extend_from_slice(statement_id);
    execute.push(0);
    execute.extend_from_slice(&1_u32.to_le_bytes());
    write_packet(&mut client, 0, &execute);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    write_packet(&mut client, 0, &[COM_QUIT]);
    let deadline = Instant::now() + Duration::from_secs(3);
    while tracker.completed() == 0 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    shutdown.shutdown();
    server.join().unwrap();
    let text = prometheus::TextEncoder::new()
        .encode_to_string(&prometheus::gather())
        .unwrap();
    assert!(
        text.lines()
            .any(|line| line.starts_with("tidb_server_query_total{")
                && line.contains("type=\"StmtExecute\"")
                && line.contains("result=\"OK\"")
                && line.ends_with(" 1")),
        "{text}"
    );
    assert!(
        text.lines().any(|line| line
            .starts_with("tidb_server_handle_query_duration_seconds_count{")
            && line.contains("sql_type=\"Begin\"")
            && line.ends_with(" 2")),
        "{text}"
    );
}
