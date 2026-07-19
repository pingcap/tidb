// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use std::collections::VecDeque;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use sha1::{Digest, Sha1};
use tidb_datatype::Datum;
use tidb_protocol::{
    ColumnInfo, PacketReader, PacketWriter, COM_PING, COM_QUERY, COM_QUIT,
    DEFAULT_MAX_ALLOWED_PACKET, TYPE_LONGLONG,
};
use tidb_server::{
    serve_mysql_connection, AuthSwitchRequest, ConfiguredUserStore, ConnectionCancellation,
    ConnectionExit, ConnectionTracker, QueryResult, QuerySession, QuerySessionFactory,
    ResultSetSource, SessionContext, SqlQueryError,
};

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;
const NATIVE_PLUGIN: &str = "mysql_native_password";
const ALICE_STAGE_TWO: &str = "*14E65567ABDB5135D0CFD9A70B3032C179A49EE7";

fn users() -> ConfiguredUserStore {
    ConfiguredUserStore::parse(&format!("alice\t%\t{NATIVE_PLUGIN}\t{ALICE_STAGE_TWO}\n")).unwrap()
}

fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) {
    let mut writer = PacketWriter::with_sequence(stream, sequence);
    writer.write_packet(payload).unwrap();
    writer.flush().unwrap();
}

fn handshake_salt(initial: &[u8]) -> [u8; 20] {
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

fn handshake_response(user: &str, plugin: &str, auth: &[u8]) -> Vec<u8> {
    let capabilities = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH
        | CLIENT_CONNECT_ATTRS
        | CLIENT_DEPRECATE_EOF;
    let mut response = Vec::new();
    response.extend_from_slice(&capabilities.to_le_bytes());
    response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    response.push(46);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(user.as_bytes());
    response.push(0);
    response.push(u8::try_from(auth.len()).unwrap());
    response.extend_from_slice(auth);
    response.extend_from_slice(plugin.as_bytes());
    response.push(0);
    response.push(0);
    response
}

struct Rows {
    rows: VecDeque<Vec<Datum>>,
}

impl ResultSetSource for Rows {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
        Ok((0..max_rows).map_while(|_| self.rows.pop_front()).collect())
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
        Ok(vec![ColumnInfo {
            schema: "campaign21".to_owned(),
            table: "rows".to_owned(),
            org_table: "rows".to_owned(),
            name: "id".to_owned(),
            org_name: "id".to_owned(),
            column_length: 20,
            charset: 63,
            flag: 0x0003,
            decimal: 0,
            type_code: TYPE_LONGLONG,
            default_value: None,
        }])
    }

    fn finish(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        Ok(())
    }
}

struct Session;

impl QuerySession for Session {
    fn execute<'a>(&'a mut self, sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        assert_eq!(sql, "select id from campaign21.rows");
        Ok(QueryResult::new(Box::new(Rows {
            rows: [vec![Datum::Int(42)]].into(),
        })))
    }
}

#[derive(Default)]
struct RecordingFactory {
    contexts: Mutex<Vec<SessionContext>>,
}

impl QuerySessionFactory for RecordingFactory {
    type Session = Session;

    fn open_session(&self, context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        self.contexts.lock().unwrap().push(context);
        Ok(Session)
    }
}

#[test]
fn native_nonroot_auth_query_ping_quit_publishes_canonical_session_identity() {
    // pkg/server/conn_test.go:595 TestIssue1768
    // pkg/server/conn_test.go:789 TestDispatchClientProtocol41
    // pkg/server/conn_test.go:2479 TestCloseConn
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let factory = Arc::new(RecordingFactory::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker_factory = Arc::clone(&factory);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            worker_factory.as_ref(),
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    reader.set_sequence(0);
    let salt = handshake_salt(&reader.read_packet().unwrap());
    write_packet(
        &mut client,
        1,
        &handshake_response("alice", NATIVE_PLUGIN, &native_response(b"secret", &salt)),
    );
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    write_packet(
        &mut client,
        0,
        &[COM_QUERY]
            .into_iter()
            .chain(b"select id from campaign21.rows".iter().copied())
            .collect::<Vec<_>>(),
    );
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap(), [1]);
    assert_ne!(reader.read_packet().unwrap()[0], 0xff);
    assert_eq!(reader.read_packet().unwrap(), b"\x0242");
    assert_eq!(reader.read_packet().unwrap()[0], 0xfe);

    write_packet(&mut client, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    write_packet(&mut client, 0, &[COM_QUIT]);

    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::Quit);
    assert_eq!(report.queries, 1);
    let contexts = factory.contexts.lock().unwrap();
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0].connection_id, report.connection_id);
    assert_eq!(contexts[0].identity.username(), "alice");
    assert_eq!(contexts[0].identity.host(), "%");
    assert_eq!(contexts[0].identity.auth_plugin(), NATIVE_PLUGIN);
    assert_eq!(tracker.accepted(), 1);
    assert_eq!(tracker.completed(), 1);
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.failed(), 0);
}

#[test]
fn nonnative_client_plugin_runs_real_auth_switch_packet_sequence() {
    // pkg/server/conn_test.go:1532 TestHandleAuthPlugin
    // pkg/server/conn_test.go:1927 TestAuthPlugin2
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let factory = Arc::new(RecordingFactory::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker_factory = Arc::clone(&factory);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            worker_factory.as_ref(),
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    reader.set_sequence(0);
    let salt = handshake_salt(&reader.read_packet().unwrap());
    write_packet(
        &mut client,
        1,
        &handshake_response("alice", "caching_sha2_password", b"ignored"),
    );
    reader.set_sequence(2);
    let switch = AuthSwitchRequest::parse_payload(&reader.read_packet().unwrap()).unwrap();
    assert_eq!(switch.client_plugin, NATIVE_PLUGIN);
    assert_eq!(switch.auth_data, salt);
    write_packet(&mut client, 3, &native_response(b"secret", &salt));
    reader.set_sequence(4);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    write_packet(&mut client, 0, &[COM_QUIT]);

    assert_eq!(worker.join().unwrap().exit, ConnectionExit::Quit);
    let contexts = factory.contexts.lock().unwrap();
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0].identity.username(), "alice");
    assert_eq!(contexts[0].identity.host(), "%");
    assert_eq!(tracker.completed(), 1);
    assert_eq!(tracker.active(), 0);
}

fn rejected_packet(user: &str, password: Option<&[u8]>) -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let factory = Arc::new(RecordingFactory::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker_factory = Arc::clone(&factory);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            worker_factory.as_ref(),
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    reader.set_sequence(0);
    let salt = handshake_salt(&reader.read_packet().unwrap());
    let auth = password
        .map(|password| native_response(password, &salt).to_vec())
        .unwrap_or_default();
    write_packet(
        &mut client,
        1,
        &handshake_response(user, NATIVE_PLUGIN, &auth),
    );
    reader.set_sequence(2);
    let error = reader.read_packet().unwrap();
    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::AuthenticationRejected);
    assert!(factory.contexts.lock().unwrap().is_empty());
    assert_eq!(tracker.accepted(), 1);
    assert_eq!(tracker.completed(), 1);
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.failed(), 0);
    error
}

#[test]
fn wrong_unknown_and_empty_root_credentials_share_identical_1045_and_cleanup() {
    // pkg/server/conn_test.go:1879 TestChangeUserAuth
    // pkg/server/conn_test.go:2315 TestAuthSha
    let wrong = rejected_packet("alice", Some(b"wrong"));
    let unknown = rejected_packet("unknown", Some(b"secret"));
    let empty_root = rejected_packet("root", None);
    assert_eq!(wrong, unknown);
    assert_eq!(wrong, empty_root);
    assert_eq!(wrong[0], 0xff);
    assert_eq!(u16::from_le_bytes([wrong[1], wrong[2]]), 1045);
    assert_eq!(&wrong[3..9], b"#28000");
    assert_eq!(&wrong[9..], b"Access denied");
}
