// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};

use sha1::{Digest, Sha1};
use tidb_protocol::{PacketReader, PacketWriter, COM_PING, COM_QUIT, DEFAULT_MAX_ALLOWED_PACKET};
use tidb_server::{
    ConcurrentSqlNode, ConfiguredUserStore, NodeConfig, QueryResult, QuerySession,
    QuerySessionFactory, SessionContext, SqlQueryError,
};

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

struct Session;

impl QuerySession for Session {
    fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        panic!("concurrency proof uses authenticated PING and QUIT only")
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
    NodeConfig::parse([
        "tidb-server",
        "--path",
        "127.0.0.1:2379",
        "--database",
        "campaign21",
        "--table",
        "rows",
        "--table-id",
        "42",
        "--column",
        "id:1:clustered-pk",
        "--auth-file",
        "/tmp/campaign21-users.tsv",
        "--max-connections",
        "3",
        "--port",
        "0",
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
    assert_eq!(client_ids, [1, 2, 3]);
    let mut session_ids = factory
        .contexts
        .lock()
        .unwrap()
        .iter()
        .map(|context| context.connection_id)
        .collect::<Vec<_>>();
    session_ids.sort_unstable();
    assert_eq!(session_ids, [1, 2, 3]);
    assert_eq!(factory.max_opening.load(Ordering::Acquire), 3);
    assert_eq!(tracker.max_active(), 3);
    assert!(tracker.max_active() <= config.max_connections);
    assert_eq!(tracker.accepted(), 3);
    assert_eq!(tracker.completed(), 3);
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.failed(), 0);
}
