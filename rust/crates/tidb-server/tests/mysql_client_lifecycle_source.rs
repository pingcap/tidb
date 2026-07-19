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
    ColumnInfo, PacketReader, PacketWriter, COM_INIT_DB, COM_PING, COM_QUERY, COM_QUIT,
    DEFAULT_MAX_ALLOWED_PACKET, TYPE_LONGLONG,
};
use tidb_server::{
    serve_mysql_connection, ConfiguredUserStore, ConnectionExit, ConnectionTracker, QueryResult,
    QuerySession, QuerySessionFactory, ResultSetSource, SessionContext, SqlQueryError,
};

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

#[derive(Default)]
struct Lifecycle {
    finished: usize,
    closed: usize,
}

struct Rows {
    rows: VecDeque<Vec<Datum>>,
    lifecycle: Arc<Mutex<Lifecycle>>,
}

impl ResultSetSource for Rows {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
        Ok((0..max_rows).map_while(|_| self.rows.pop_front()).collect())
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
        Ok(vec![
            ColumnInfo {
                schema: "campaign20".to_owned(),
                table: "rows".to_owned(),
                org_table: "rows".to_owned(),
                name: "amount".to_owned(),
                org_name: "balance".to_owned(),
                column_length: 20,
                charset: 63,
                flag: 0x0001,
                decimal: 0,
                type_code: TYPE_LONGLONG,
                default_value: None,
            },
            ColumnInfo {
                schema: "campaign20".to_owned(),
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
            },
        ])
    }

    fn finish(&mut self) -> Result<(), String> {
        self.lifecycle.lock().unwrap().finished += 1;
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        self.lifecycle.lock().unwrap().closed += 1;
        Ok(())
    }
}

struct Session {
    queries: Arc<Mutex<Vec<String>>>,
    lifecycle: Arc<Mutex<Lifecycle>>,
}

impl QuerySession for Session {
    fn execute<'a>(&'a mut self, sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        self.queries.lock().unwrap().push(sql.to_owned());
        Ok(QueryResult::new(Box::new(Rows {
            rows: [
                vec![Datum::Int(-11), Datum::Int(7)],
                vec![Datum::Int(25), Datum::Int(8)],
            ]
            .into(),
            lifecycle: Arc::clone(&self.lifecycle),
        })))
    }
}

struct Factory {
    queries: Arc<Mutex<Vec<String>>>,
    lifecycle: Arc<Mutex<Lifecycle>>,
}

impl QuerySessionFactory for Factory {
    type Session = Session;

    fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        Ok(Session {
            queries: Arc::clone(&self.queries),
            lifecycle: Arc::clone(&self.lifecycle),
        })
    }
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

fn authenticate(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    user: &str,
    password: &[u8],
) {
    reader.set_sequence(0);
    let initial = reader.read_packet().unwrap();
    let salt = handshake_salt(&initial);
    let version_end = initial[1..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| offset + 1)
        .unwrap();
    assert_eq!(initial[version_end + 16], 46);

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
    let auth = native_response(password, &salt);
    response.push(u8::try_from(auth.len()).unwrap());
    response.extend_from_slice(&auth);
    response.extend_from_slice(b"mysql_native_password\0");
    response.push(0); // zero connection attributes
    write_packet(client, 1, &response);
}

fn read_length_encoded_string<'a>(packet: &mut &'a [u8]) -> &'a [u8] {
    let length = usize::from(packet[0]);
    assert!(length < 0xfb, "test metadata uses one-byte lengths");
    *packet = &packet[1..];
    let (value, remaining) = packet.split_at(length);
    *packet = remaining;
    value
}

fn assert_column_packet(packet: &[u8], name: &[u8], org_name: &[u8], flags: u16) {
    let mut remaining = packet;
    assert_eq!(read_length_encoded_string(&mut remaining), b"def");
    assert_eq!(read_length_encoded_string(&mut remaining), b"campaign20");
    assert_eq!(read_length_encoded_string(&mut remaining), b"rows");
    assert_eq!(read_length_encoded_string(&mut remaining), b"rows");
    assert_eq!(read_length_encoded_string(&mut remaining), name);
    assert_eq!(read_length_encoded_string(&mut remaining), org_name);
    assert_eq!(remaining[0], 0x0c);
    assert_eq!(u16::from_le_bytes([remaining[1], remaining[2]]), 63);
    assert_eq!(remaining[7], TYPE_LONGLONG);
    assert_eq!(u16::from_le_bytes([remaining[8], remaining[9]]), flags);
}

#[test]
fn real_tcp_connection_runs_handshake_query_ping_quit_and_exact_cleanup() {
    // pkg/server/conn_test.go:789 TestDispatchClientProtocol41
    // pkg/server/conn_test.go:909 TestQueryEndWithZero
    // pkg/server/conn_test.go:2479 TestCloseConn
    // pkg/server/conn_test.go:2518 TestConnAddMetrics
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let queries = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(Mutex::new(Lifecycle::default()));
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_queries = Arc::clone(&queries);
    let worker_lifecycle = Arc::clone(&lifecycle);
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let factory = Factory {
            queries: worker_queries,
            lifecycle: worker_lifecycle,
        };
        serve_mysql_connection(
            stream,
            peer_addr,
            &factory,
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader, "alice", b"secret");
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    write_packet(&mut client, 0, &[COM_INIT_DB, b'x']);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0xff);

    let mut query = vec![COM_QUERY];
    query.extend_from_slice(b"select balance as amount, id from campaign20.rows\0\0");
    write_packet(&mut client, 0, &query);
    reader.set_sequence(1);
    let payloads = (0..6)
        .map(|_| reader.read_packet().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(payloads[0].as_slice(), [2]);
    assert_column_packet(&payloads[1], b"amount", b"balance", 0x0001);
    assert_column_packet(&payloads[2], b"id", b"id", 0x0003);
    assert_eq!(payloads[3].as_slice(), b"\x03-11\x017");
    assert_eq!(payloads[4].as_slice(), b"\x0225\x018");
    assert_eq!(payloads[5][0], 0xfe);

    write_packet(&mut client, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    write_packet(&mut client, 0, &[COM_QUIT]);

    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::Quit);
    assert_eq!(report.queries, 1);
    assert_eq!(
        queries.lock().unwrap().as_slice(),
        ["select balance as amount, id from campaign20.rows\0"]
    );
    let lifecycle = lifecycle.lock().unwrap();
    assert_eq!(lifecycle.finished, 1);
    assert_eq!(lifecycle.closed, 1);
    assert_eq!(tracker.accepted(), 1);
    assert_eq!(tracker.completed(), 1);
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.failed(), 0);
}

struct RejectingSession;

impl QuerySession for RejectingSession {
    fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        Err(SqlQueryError::new(1142, *b"42000", "read denied"))
    }
}

struct RejectingFactory;

impl QuerySessionFactory for RejectingFactory {
    type Session = RejectingSession;

    fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        Ok(RejectingSession)
    }
}

#[test]
fn query_error_is_written_as_err_and_connection_remains_command_aligned() {
    // pkg/server/conn_test.go:789 TestDispatchClientProtocol41
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            &RejectingFactory,
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader, "alice", b"secret");
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    write_packet(&mut client, 0, &[COM_QUERY, b'x']);
    reader.set_sequence(1);
    let error = reader.read_packet().unwrap();
    assert_eq!(error[0], 0xff);
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1142);
    assert_eq!(&error[4..9], b"42000");
    assert!(error.ends_with(b"read denied"));

    write_packet(&mut client, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    write_packet(&mut client, 0, &[COM_QUIT]);

    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::Quit);
    assert_eq!(report.queries, 0);
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.completed(), 1);
    assert_eq!(tracker.failed(), 0);
}
