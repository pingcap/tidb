// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

//! Go `pkg/server/conn.go` `Run`'s deferred `recover()`: a statement that
//! panics ends ITS connection, and the server lives to serve the next one.
//! Recovery writes through the retained PacketIO before transport/session cleanup.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tidb_datatype::{Datum, FieldTypeCode};

use sha1::{Digest, Sha1};
use tidb_protocol::{
    ColumnInfo, CompressionAlgorithm, PacketIoReader, PacketIoWriter, PacketReader, PacketWriter,
    COM_PING, COM_QUERY, COM_QUIT, COM_STMT_PREPARE, DEFAULT_MAX_ALLOWED_PACKET,
};
use tidb_server::{
    ConcurrentSqlNode, ConfiguredUserStore, NodeConfig, PreparedGeneral, QueryResult, QuerySession,
    QuerySessionFactory, ResultSetSource, SessionContext, SqlQueryError, WriteOutcome,
};

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

struct PanickingSession {
    context: SessionContext,
    retired: Arc<Mutex<Vec<bool>>>,
}

impl Drop for PanickingSession {
    fn drop(&mut self) {
        self.retired
            .lock()
            .unwrap()
            .push(self.context.close.is_closed());
    }
}

impl QuerySession for PanickingSession {
    fn execute<'a>(&'a mut self, sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        // Setup must finish before the test injects a command-loop failure.
        if sql.starts_with("SET NAMES ") {
            return Err(SqlQueryError::unknown("fixture does not execute setup SQL"));
        }
        if sql == "stream" {
            return Ok(QueryResult::new(Box::new(PanickingRows { pulls: 0 })));
        }
        if sql == "closed" {
            self.context.close.request();
        }
        panic!("injected command panic")
    }

    fn prepare_general(&mut self, _: &str) -> Result<PreparedGeneral, SqlQueryError> {
        panic!("injected command panic")
    }

    fn local_infile_path(&mut self, sql: &str) -> Result<Option<String>, SqlQueryError> {
        Ok((sql == "infile").then(|| "fixture.data".into()))
    }

    fn execute_local_infile(
        &mut self,
        _: &str,
        data: &[u8],
    ) -> Result<WriteOutcome, SqlQueryError> {
        assert_eq!(data, b"fixture contents");
        panic!("injected command panic")
    }
}

struct PanickingRows {
    pulls: usize,
}

impl ResultSetSource for PanickingRows {
    fn next_batch(&mut self, _: usize) -> Result<Vec<Vec<Datum>>, tidb_executor::MysqlError> {
        self.pulls += 1;
        if self.pulls == 3 {
            panic!("injected command panic");
        }
        Ok(vec![vec![Datum::Int(7)]])
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, tidb_executor::MysqlError> {
        Ok(vec![ColumnInfo {
            schema: String::new(),
            table: String::new(),
            org_table: String::new(),
            name: "value".into(),
            org_name: "value".into(),
            column_length: 20,
            charset: 63,
            flag: 0,
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

#[derive(Default)]
struct PanickingFactory {
    retired: Arc<Mutex<Vec<bool>>>,
    contexts: Mutex<Vec<SessionContext>>,
}

impl QuerySessionFactory for PanickingFactory {
    type Session = PanickingSession;

    fn open_session(&self, context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        // Deliberately retain raw close handles outside the command owner. EOF
        // must come from ordered transport cleanup, not the last socket Drop.
        self.contexts.lock().unwrap().push(context.clone());
        Ok(PanickingSession {
            context,
            retired: self.retired.clone(),
        })
    }
}

fn config() -> NodeConfig {
    NodeConfig::parse([
        "tidb-server",
        "--path",
        "127.0.0.1:2379",
        "--read-table",
        "campaign21",
        "rows",
        "42",
        "1",
        "id:1:clustered-pk",
        "--auth-file",
        "/tmp/campaign21-users.tsv",
        "--max-connections",
        "2",
        "--port",
        "0",
        "--token-limit",
        "1",
    ])
    .unwrap()
}

fn users() -> ConfiguredUserStore {
    ConfiguredUserStore::parse(
        "alice\t%\tmysql_native_password\t*14E65567ABDB5135D0CFD9A70B3032C179A49EE7\n",
    )
    .unwrap()
}

fn write_packet(stream: &mut impl Write, sequence: u8, payload: &[u8]) {
    let mut writer = PacketWriter::with_sequence(stream, sequence);
    writer.write_packet(payload).unwrap();
    writer.flush().unwrap();
}

fn handshake_fields(initial: &[u8]) -> [u8; 20] {
    assert_eq!(initial[0], 10);
    let version_end = initial[1..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| offset + 1)
        .unwrap();
    let first = version_end + 1 + 4;
    let second = first + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10;
    let mut salt = [0; 20];
    salt[..8].copy_from_slice(&initial[first..first + 8]);
    salt[8..].copy_from_slice(&initial[second..second + 12]);
    salt
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

trait Wire: Read + Write {}
impl<T: Read + Write> Wire for T {}

struct TestTls {
    directory: std::path::PathBuf,
    client: Arc<rustls::ClientConfig>,
}

impl TestTls {
    fn new(config: &mut NodeConfig) -> Self {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let directory = std::env::temp_dir().join(format!(
            "tidb-recovery-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let cert = directory.join("cert.pem");
        let key = directory.join("key.pem");
        std::fs::write(&cert, certified.cert.pem()).unwrap();
        std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();
        config.ssl_cert = Some(cert);
        config.ssl_key = Some(key);
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certified.cert.der().clone()).unwrap();
        let client = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        Self {
            directory,
            client: Arc::new(client),
        }
    }
}

impl Drop for TestTls {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

fn authenticate(
    address: std::net::SocketAddr,
    algorithm: CompressionAlgorithm,
    tls: Option<&TestTls>,
) -> Box<dyn Wire> {
    let mut client = TcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let salt = handshake_fields(&PacketReader::new(&mut client).read_packet().unwrap());
    let compression = match algorithm {
        CompressionAlgorithm::None => 0,
        CompressionAlgorithm::Zlib => 1 << 5,
        CompressionAlgorithm::Zstd => 1 << 26,
    };
    let capabilities = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH
        | CLIENT_CONNECT_ATTRS
        | CLIENT_DEPRECATE_EOF
        | (1 << 7)
        | compression
        | if tls.is_some() { 1 << 11 } else { 0 };
    let mut response = Vec::new();
    response.extend_from_slice(&capabilities.to_le_bytes());
    response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    response.push(46);
    response.extend_from_slice(&[0; 23]);
    let mut sequence = 1;
    let mut client: Box<dyn Wire> = if let Some(tls) = tls {
        write_packet(&mut client, sequence, &response);
        sequence += 1;
        let session =
            rustls::ClientConnection::new(tls.client.clone(), "localhost".try_into().unwrap())
                .unwrap();
        Box::new(rustls::StreamOwned::new(session, client))
    } else {
        Box::new(client)
    };
    response.extend_from_slice(b"alice\0");
    response.push(20);
    response.extend_from_slice(&native_response(b"secret", &salt));
    response.extend_from_slice(b"mysql_native_password\0");
    response.push(0);
    if algorithm == CompressionAlgorithm::Zstd {
        response.push(3);
    }
    write_packet(&mut client, sequence, &response);
    let mut reader = PacketReader::new(&mut client);
    reader.set_sequence(sequence + 1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    client
}

fn command(client: &mut Box<dyn Wire>, algorithm: CompressionAlgorithm, payload: &[u8]) -> u8 {
    let mut writer = PacketIoWriter::new(client, algorithm).unwrap();
    writer.write_packet(payload).unwrap();
    writer.flush().unwrap();
    writer.compressed_sequence().unwrap_or(0)
}

#[test]
fn panic_recovery_writes_error_before_close_and_server_keeps_serving() {
    panic_matrix("panic", COM_QUERY);
}

#[test]
fn panic_recovery_preserves_partial_reply_sequence_and_buffered_bytes() {
    panic_matrix("stream", COM_QUERY);
}

#[test]
fn panic_recovery_covers_prepared_dispatch() {
    panic_matrix("SELECT 1", COM_STMT_PREPARE);
}

#[test]
fn panic_recovery_preserves_sequence_after_local_infile() {
    panic_matrix("infile", COM_QUERY);
}

#[test]
fn panic_recovery_retires_connection_when_error_write_fails() {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let factory = Arc::new(PanickingFactory::default());
    let server_factory = factory.clone();
    let worker = std::thread::spawn(move || {
        let (socket, peer) = listener.accept().unwrap();
        tidb_server::serve_mysql_connection(
            socket,
            peer,
            tidb_server::ConnectionCancellation::default(),
            &*server_factory,
            &users(),
            &Arc::new(tidb_server::ConnectionTracker::default()),
            DEFAULT_MAX_ALLOWED_PACKET,
        )
    });
    let mut client = authenticate(address, CompressionAlgorithm::None, None);
    command(&mut client, CompressionAlgorithm::None, b"\x03closed");
    let mut reader = PacketReader::new(&mut client);
    reader.set_sequence(1);
    let response = reader.read_packet();
    let result = worker.join().unwrap();
    assert!(matches!(
        response,
        Err(tidb_protocol::PacketError::EndOfStream)
    ));
    match result {
        Err(tidb_server::MysqlConnectionError::Panicked(message)) => {
            assert_eq!(message, "injected command panic");
        }
        other => panic!("failed recovery write must preserve the original panic: {other:?}"),
    }
    assert_eq!(*factory.retired.lock().unwrap(), [true]);
}

fn panic_matrix(sql: &str, command: u8) {
    for tls in [false, true] {
        for algorithm in [
            CompressionAlgorithm::None,
            CompressionAlgorithm::Zlib,
            CompressionAlgorithm::Zstd,
        ] {
            panic_case(sql, command, algorithm, tls);
        }
    }
}

fn panic_case(sql: &str, opcode: u8, algorithm: CompressionAlgorithm, tls_enabled: bool) {
    let mut config = config();
    config.auto_tls = false;
    let tls = tls_enabled.then(|| TestTls::new(&mut config));
    let factory = Arc::new(PanickingFactory::default());
    let node = ConcurrentSqlNode::bind(&config, factory.clone(), users().into()).unwrap();
    let address = node.local_addr().unwrap();
    let tracker = node.tracker();
    let server = std::thread::spawn(move || node.serve_connections(2));

    let mut first = authenticate(address, algorithm, tls.as_ref());
    // A preceding successful command must not leave its response sequence in
    // the recovery writer. Every new command starts its own packet exchange.
    let compressed = command(&mut first, algorithm, &[COM_PING]);
    let mut reader = PacketIoReader::new(&mut first, algorithm).unwrap();
    reader.set_sequence(1);
    reader.set_compressed_sequence(compressed);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    drop(reader);
    let mut compressed = command(
        &mut first,
        algorithm,
        &[&[opcode][..], sql.as_bytes()].concat(),
    );
    let mut sequence = 1;
    if sql == "infile" {
        let mut reader = PacketIoReader::new(&mut first, algorithm).unwrap();
        reader.set_sequence(sequence);
        reader.set_compressed_sequence(compressed);
        assert_eq!(reader.read_packet().unwrap(), b"\xfbfixture.data");
        sequence = reader.sequence();
        compressed = reader.compressed_sequence().unwrap_or(0);
        drop(reader);
        let mut writer = PacketIoWriter::new(&mut first, algorithm).unwrap();
        writer.set_sequence(sequence);
        writer.set_compressed_sequence(compressed);
        writer.write_packet(b"fixture contents").unwrap();
        writer.write_packet(b"").unwrap();
        writer.flush().unwrap();
        sequence = writer.sequence();
        compressed = writer.compressed_sequence().unwrap_or(0);
    }
    let mut reader = PacketIoReader::new(&mut first, algorithm).unwrap();
    reader.set_sequence(sequence);
    reader.set_compressed_sequence(compressed);
    let mut packets = Vec::new();
    let error_packet = loop {
        match reader.read_packet() {
            Ok(packet) if packet.first() == Some(&0xff) => break Ok(packet),
            Ok(packet) => packets.push(packet),
            Err(error) => break Err(error),
        }
    };
    let closed = reader.read_packet();
    drop(reader);
    drop(first);

    // The same owner must return the token on unwind and keep serving peers.
    let mut second = authenticate(address, algorithm, tls.as_ref());
    let compressed = command(&mut second, algorithm, &[COM_PING]);
    let mut reader = PacketIoReader::new(&mut second, algorithm).unwrap();
    reader.set_sequence(1);
    reader.set_compressed_sequence(compressed);
    let ping = reader.read_packet();
    drop(reader);
    command(&mut second, algorithm, &[COM_QUIT]);
    server.join().unwrap().unwrap();

    let packet = error_packet.expect("Go attempts ERR before closing a panicked command");
    assert_eq!(
        &packet[..9],
        &[0xff, 0x51, 0x04, b'#', b'H', b'Y', b'0', b'0', b'0']
    );
    assert_eq!(&packet[9..], b"injected command panic");
    match closed {
        Err(tidb_protocol::PacketError::EndOfStream) => {}
        Err(tidb_protocol::PacketError::Io(error)) if tls_enabled => {
            // Closing the underlying transport, as Go closeConn does, need
            // not include a TLS close_notify. Rustls reports that EOF here.
            assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        }
        other => panic!("connection must close after recovery: {other:?}"),
    }
    if sql == "stream" {
        assert_eq!(
            packets.len(),
            3,
            "column count, column and first row precede ERR"
        );
        assert_eq!(packets[0], vec![1]);
        assert_eq!(packets[2], vec![1, b'7']);
    } else {
        assert!(packets.is_empty());
    }
    assert_eq!(ping.unwrap()[0], 0);
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.accepted(), 2);
    assert_eq!(tracker.failed(), 1);
    assert_eq!(
        *factory.retired.lock().unwrap(),
        [true, true],
        "transport closes before session retirement"
    );
}
