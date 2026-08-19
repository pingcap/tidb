// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

//! Go `pkg/server/conn.go` `Run`'s deferred `recover()`: a statement that
//! panics ends ITS connection, and the server lives to serve the next one.
//! Before the recovery landed, one panicking query unwound through its
//! worker thread and the whole node's run reported a terminated worker.

use std::io::Read;
use std::net::TcpStream;

use sha1::{Digest, Sha1};
use tidb_protocol::{
    PacketReader, PacketWriter, COM_PING, COM_QUERY, COM_QUIT, DEFAULT_MAX_ALLOWED_PACKET,
};
use tidb_server::{
    ConcurrentSqlNode, ConfiguredUserStore, NodeConfig, QueryResult, QuerySession,
    QuerySessionFactory, SessionContext, SqlQueryError,
};

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

struct PanickingSession;

impl QuerySession for PanickingSession {
    fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        panic!("index out of bounds: the len is 0 but the index is 0")
    }
}

struct PanickingFactory;

impl QuerySessionFactory for PanickingFactory {
    type Session = PanickingSession;

    fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        Ok(PanickingSession)
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

fn authenticate(address: std::net::SocketAddr) -> (TcpStream, PacketReader<TcpStream>) {
    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    reader.set_sequence(0);
    let salt = handshake_fields(&reader.read_packet().unwrap());
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

#[test]
fn a_panicking_query_closes_its_connection_and_the_server_keeps_serving() {
    let node =
        ConcurrentSqlNode::bind(&config(), std::sync::Arc::new(PanickingFactory), users().into())
            .unwrap();
    let address = node.local_addr().unwrap();
    let tracker = node.tracker();
    let server = std::thread::spawn(move || node.serve_connections(2));

    // Connection one: the query panics; the recovery closes only this
    // connection, so the read observes end of stream, not a hung socket.
    let (mut first, _reader) = authenticate(address);
    write_packet(&mut first, 0, &[&[COM_QUERY][..], b"select 1".as_ref()].concat());
    let mut probe = [0u8; 1];
    let read = first.read(&mut probe).unwrap_or(0);
    assert_eq!(read, 0, "the panicked connection must close, not answer");

    // Connection two: the same worker pool serves a healthy lifecycle.
    let (mut second, mut reader) = authenticate(address);
    write_packet(&mut second, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    write_packet(&mut second, 0, &[COM_QUIT]);

    // The node drains cleanly: no worker terminated, no panic joined.
    server.join().unwrap().unwrap();
    assert_eq!(tracker.active(), 0);
    assert_eq!(tracker.accepted(), 2);
    assert_eq!(tracker.failed(), 1, "the panicked lifecycle counts failed");
}
