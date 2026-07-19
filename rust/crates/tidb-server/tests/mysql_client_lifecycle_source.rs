// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use std::collections::VecDeque;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use tidb_datatype::Datum;
use tidb_protocol::{
    ColumnInfo, PacketReader, PacketWriter, COM_INIT_DB, COM_PING, COM_QUERY, COM_QUIT,
    DEFAULT_MAX_ALLOWED_PACKET, TYPE_LONGLONG,
};
use tidb_server::{
    serve_mysql_connection, ConnectionExit, ConnectionTracker, ResultSetSource, SerialQueryEngine,
    SerialQueryResult, SqlQueryError,
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
        Ok(vec![ColumnInfo {
            schema: "campaign19".to_owned(),
            table: "rows".to_owned(),
            org_table: "rows".to_owned(),
            name: "id".to_owned(),
            org_name: "id".to_owned(),
            column_length: 20,
            charset: 63,
            flag: 0,
            decimal: 0,
            type_code: TYPE_LONGLONG,
            default_value: None,
        }])
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

struct Engine {
    queries: Arc<Mutex<Vec<String>>>,
    lifecycle: Arc<Mutex<Lifecycle>>,
}

impl SerialQueryEngine for Engine {
    fn execute<'a>(&'a mut self, sql: &str) -> Result<SerialQueryResult<'a>, SqlQueryError> {
        self.queries.lock().unwrap().push(sql.to_owned());
        Ok(SerialQueryResult::new(Box::new(Rows {
            rows: [vec![Datum::Int(7)], vec![Datum::Int(8)]].into(),
            lifecycle: Arc::clone(&self.lifecycle),
        })))
    }
}

fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) {
    let mut writer = PacketWriter::with_sequence(stream, sequence);
    writer.write_packet(payload).unwrap();
    writer.flush().unwrap();
}

fn authenticate(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    user: &str,
    auth: &[u8],
) {
    reader.set_sequence(0);
    let initial = reader.read_packet().unwrap();
    assert_eq!(initial[0], 10);
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
    response.push(u8::try_from(auth.len()).unwrap());
    response.extend_from_slice(auth);
    response.extend_from_slice(b"mysql_native_password\0");
    response.push(0); // zero connection attributes
    write_packet(client, 1, &response);
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
        let (stream, _) = listener.accept().unwrap();
        let mut engine = Engine {
            queries: worker_queries,
            lifecycle: worker_lifecycle,
        };
        serve_mysql_connection(
            stream,
            &mut engine,
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader, "root", &[]);
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0);

    write_packet(&mut client, 0, &[COM_INIT_DB, b'x']);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0xff);

    let mut query = vec![COM_QUERY];
    query.extend_from_slice(b"select id from campaign19.rows\0\0");
    write_packet(&mut client, 0, &query);
    reader.set_sequence(1);
    let payloads = (0..5)
        .map(|_| reader.read_packet().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(payloads[0].as_slice(), [1]);
    assert!(payloads[1].windows(2).any(|part| part == b"id"));
    assert_eq!(payloads[2].as_slice(), b"\x017");
    assert_eq!(payloads[3].as_slice(), b"\x018");
    assert_eq!(payloads[4][0], 0xfe);

    write_packet(&mut client, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    write_packet(&mut client, 0, &[COM_QUIT]);

    let report = worker.join().unwrap();
    assert_eq!(report.exit, ConnectionExit::Quit);
    assert_eq!(report.queries, 1);
    assert_eq!(
        queries.lock().unwrap().as_slice(),
        ["select id from campaign19.rows\0"]
    );
    let lifecycle = lifecycle.lock().unwrap();
    assert_eq!(lifecycle.finished, 1);
    assert_eq!(lifecycle.closed, 1);
    assert_eq!(tracker.accepted(), 1);
    assert_eq!(tracker.completed(), 1);
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.failed(), 0);
}

fn assert_auth_rejected(user: &str, auth: &[u8]) {
    let user = user.to_owned();
    let auth = auth.to_vec();
    // pkg/server/conn_test.go:72 TestMatchIdentityWithVariantsStarter
    // pkg/server/conn_test.go:1532 TestHandleAuthPlugin
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut engine = Engine {
            queries: Arc::new(Mutex::new(Vec::new())),
            lifecycle: Arc::new(Mutex::new(Lifecycle::default())),
        };
        serve_mysql_connection(
            stream,
            &mut engine,
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });
    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader, &user, &auth);
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0xff);
    assert_eq!(
        worker.join().unwrap().exit,
        ConnectionExit::AuthenticationRejected
    );
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.accepted(), 1);
    assert_eq!(tracker.completed(), 1);
}

#[test]
fn nonempty_or_nonroot_auth_is_rejected_without_leaking_connection_count() {
    assert_auth_rejected("other", &[]);
    assert_auth_rejected("root", b"not-empty");
}

struct RejectingEngine;

impl SerialQueryEngine for RejectingEngine {
    fn execute<'a>(&'a mut self, _sql: &str) -> Result<SerialQueryResult<'a>, SqlQueryError> {
        Err(SqlQueryError::new(1142, *b"42000", "read denied"))
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
        let (stream, _) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            &mut RejectingEngine,
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader, "root", &[]);
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
