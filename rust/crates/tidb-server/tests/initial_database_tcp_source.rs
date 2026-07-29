// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! `mysql -D <db>` over a real socket.
//!
//! The client here reproduces the one libmysqlclient behavior that made this
//! path fail, captured from a real `mysql` 9.5 client against a listener that
//! advertised each capability set in turn: the client sets
//! `CLIENT_CONNECT_WITH_DB` in its response flags *unconditionally*, and
//! writes the database field only when the **server** advertised the same
//! bit. A server that omits the bit therefore receives a response whose flags
//! promise a field the packet does not carry, and the database, auth plugin
//! and connection attributes are all read one field early.
//!
//! Captured capability flags, same client, same listener:
//! `-D mydb` sends `8da2bf19` (bit 3 set), no `-D` and `-D ""` both send
//! `85a2bf19` (bit 3 clear).
//!
//! Captured response body with the bit advertised (`mysql -u root -D mydb`),
//! abbreviated:
//!
//! ```text
//! 8da2bf19 00000001 08 <23 zeros> "root\0" 00 "mydb\0" "caching_sha2_password\0" 76 ...attrs
//! ```
//!
//! and without it -- same capability flags, `"mydb\0"` simply absent:
//!
//! ```text
//! 8da2bf19 00000001 08 <23 zeros> "root\0" 00 "caching_sha2_password\0" 76 ...attrs
//! ```

#![allow(missing_docs)]

use std::collections::HashSet;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

use sha1::{Digest, Sha1};
use tidb_datatype::Datum;
use tidb_protocol::{
    ColumnInfo, PacketReader, PacketWriter, COM_INIT_DB, COM_QUERY, COM_QUIT,
    DEFAULT_MAX_ALLOWED_PACKET, TYPE_LONGLONG,
};
use tidb_server::{
    serve_mysql_connection, ConfiguredUserStore, ConnectionCancellation, ConnectionTracker,
    QueryResult, QuerySession, QuerySessionFactory, ResultSetSource, SessionContext,
    SqlQueryError,
};

const CLIENT_CONNECT_WITH_DB: u32 = 1 << 3;
const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

/// The one table the session serves, reachable unqualified only while its own
/// schema is selected.
const SCHEMA: &str = "campaign31";

#[derive(Default)]
struct Rows {
    sent: bool,
}

impl ResultSetSource for Rows {
    fn next_batch(&mut self, _max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
        if self.sent {
            return Ok(Vec::new());
        }
        self.sent = true;
        Ok(vec![vec![Datum::Int(7)]])
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
        Ok(vec![ColumnInfo {
            schema: SCHEMA.to_owned(),
            table: "rows".to_owned(),
            org_table: "rows".to_owned(),
            name: "id".to_owned(),
            org_name: "id".to_owned(),
            column_length: 20,
            charset: 63,
            flag: 0x0001,
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

/// A session that resolves an unqualified table name against its current
/// schema, so a connection that "succeeded" without actually selecting the
/// schema still fails the query.
struct Session {
    databases: HashSet<String>,
    current: String,
}

impl QuerySession for Session {
    fn execute<'a>(&'a mut self, sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        let qualified = format!("SELECT id FROM {SCHEMA}.rows");
        if sql == qualified || (sql == "SELECT id FROM rows" && self.current == SCHEMA) {
            return Ok(QueryResult::new(Box::new(Rows::default())));
        }
        Err(SqlQueryError::new(
            1146,
            *b"42S02",
            format!("Table '{}.rows' doesn't exist", self.current),
        ))
    }

    fn select_database(&mut self, name: &str) -> Result<(), SqlQueryError> {
        if !self.databases.contains(name) {
            return Err(SqlQueryError::new(
                1049,
                *b"42000",
                format!("Unknown database '{name}'"),
            ));
        }
        self.current = name.to_owned();
        Ok(())
    }
}

struct Factory;

impl QuerySessionFactory for Factory {
    type Session = Session;

    fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        Ok(Session {
            databases: [SCHEMA.to_owned(), "other31".to_owned()].into_iter().collect(),
            current: String::new(),
        })
    }
}

fn users() -> ConfiguredUserStore {
    ConfiguredUserStore::parse(
        "alice\t%\tmysql_native_password\t*14E65567ABDB5135D0CFD9A70B3032C179A49EE7\n",
    )
    .unwrap()
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

struct InitialHandshakeFields {
    salt: [u8; 20],
    capability: u32,
}

fn read_initial_handshake(initial: &[u8]) -> InitialHandshakeFields {
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
    let low = u16::from_le_bytes([initial[first + 9], initial[first + 10]]);
    let high = u16::from_le_bytes([initial[first + 14], initial[first + 15]]);
    InitialHandshakeFields {
        salt,
        capability: u32::from(low) | (u32::from(high) << 16),
    }
}

/// What the client asks for as its initial schema.
#[derive(Clone, Copy)]
enum InitialDatabase {
    /// No `-D`: a real client clears `CLIENT_CONNECT_WITH_DB` and writes no
    /// field (captured flags `85a2bf19`).
    None,
    /// `-D name`: a real client sets the bit unconditionally (captured flags
    /// `8da2bf19`) and writes the field only when the server advertised it.
    Named(&'static str),
    /// The bit set with a zero-length name. A real `mysql -D ""` degrades to
    /// [`InitialDatabase::None`], but the packet is legal and must not be
    /// read as a schema selection.
    EmptyField,
}

/// Writes the response a real libmysqlclient writes, always with connection
/// attributes alongside -- the combination where a framing error hides.
fn write_client_response(
    client: &mut TcpStream,
    server_capability: u32,
    salt: &[u8],
    user: &str,
    password: &[u8],
    database: InitialDatabase,
) {
    let mut capabilities = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH
        | CLIENT_CONNECT_ATTRS
        | CLIENT_DEPRECATE_EOF;
    if !matches!(database, InitialDatabase::None) {
        capabilities |= CLIENT_CONNECT_WITH_DB;
    }
    let mut response = Vec::new();
    response.extend_from_slice(&capabilities.to_le_bytes());
    response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    response.push(46);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(user.as_bytes());
    response.push(0);
    let auth = native_response(password, salt);
    response.push(u8::try_from(auth.len()).unwrap());
    response.extend_from_slice(&auth);
    if server_capability & CLIENT_CONNECT_WITH_DB != 0 {
        match database {
            InitialDatabase::None => {}
            InitialDatabase::Named(name) => {
                response.extend_from_slice(name.as_bytes());
                response.push(0);
            }
            InitialDatabase::EmptyField => response.push(0),
        }
    }
    response.extend_from_slice(b"mysql_native_password\0");
    let mut attrs = Vec::new();
    for (key, value) in [("_client_name", "libmysql"), ("_os", "macos15.7")] {
        attrs.push(u8::try_from(key.len()).unwrap());
        attrs.extend_from_slice(key.as_bytes());
        attrs.push(u8::try_from(value.len()).unwrap());
        attrs.extend_from_slice(value.as_bytes());
    }
    response.push(u8::try_from(attrs.len()).unwrap());
    response.extend_from_slice(&attrs);
    let mut writer = PacketWriter::with_sequence(client, 1);
    writer.write_packet(&response).unwrap();
    writer.flush().unwrap();
}

/// Serves exactly one connection on a fresh loopback port.
fn serve_one() -> (TcpStream, PacketReader<TcpStream>, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (stream, peer) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer,
            ConnectionCancellation::default(),
            &Factory,
            &users(),
            &Arc::new(ConnectionTracker::default()),
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap();
    });
    let client = TcpStream::connect(address).unwrap();
    // A hang here is a protocol mistake, not a slow server; surface it.
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(20)))
        .unwrap();
    let reader = PacketReader::new(client.try_clone().unwrap());
    (client, reader, server)
}

/// Connects with `database` and returns the packet answering the handshake.
fn connect(database: InitialDatabase) -> (TcpStream, PacketReader<TcpStream>, std::thread::JoinHandle<()>, Vec<u8>)
{
    let (mut client, mut reader, server) = serve_one();
    reader.set_sequence(0);
    let initial = reader.read_packet().unwrap();
    let fields = read_initial_handshake(&initial);
    write_client_response(
        &mut client,
        fields.capability,
        &fields.salt,
        "alice",
        b"secret",
        database,
    );
    // The server answers the handshake response at sequence 2.
    reader.set_sequence(2);
    let answer = reader.read_packet().unwrap();
    (client, reader, server, answer)
}

fn query(client: &mut TcpStream, reader: &mut PacketReader<TcpStream>, sql: &str) -> Vec<Vec<u8>> {
    let mut payload = vec![COM_QUERY];
    payload.extend_from_slice(sql.as_bytes());
    let mut writer = PacketWriter::with_sequence(&mut *client, 0);
    writer.write_packet(&payload).unwrap();
    writer.flush().unwrap();
    reader.set_sequence(1);
    let first = reader.read_packet().unwrap();
    if first[0] == 0xff {
        return vec![first];
    }
    let mut packets = vec![first];
    loop {
        let packet = reader.read_packet().unwrap();
        let done = packet[0] == 0xfe && packet.len() < 9;
        packets.push(packet);
        if done {
            break;
        }
    }
    packets
}

fn quit(client: &mut TcpStream, server: std::thread::JoinHandle<()>) {
    let mut writer = PacketWriter::with_sequence(client, 0);
    writer.write_packet(&[COM_QUIT]).unwrap();
    writer.flush().unwrap();
    server.join().unwrap();
}

fn error_code(packet: &[u8]) -> u16 {
    assert_eq!(packet[0], 0xff, "expected an error packet");
    u16::from_le_bytes([packet[1], packet[2]])
}

#[test]
fn connecting_with_an_initial_database_selects_that_schema() {
    let (mut client, mut reader, server, answer) = connect(InitialDatabase::Named(SCHEMA));
    assert_eq!(
        answer[0], 0x00,
        "the handshake must be answered with an OK packet, got {answer:02x?}"
    );
    // A handshake that succeeds but silently ignores the database is still
    // wrong, so the proof is an unqualified read of a table in that schema.
    let packets = query(&mut client, &mut reader, "SELECT id FROM rows");
    assert_ne!(
        packets[0][0], 0xff,
        "unqualified read failed: {:02x?}",
        packets[0]
    );
    assert_eq!(packets[0], vec![1], "expected a one-column result set");
    quit(&mut client, server);
}

#[test]
fn connecting_without_an_initial_database_still_works() {
    let (mut client, mut reader, server, answer) = connect(InitialDatabase::None);
    assert_eq!(answer[0], 0x00);
    // No schema is selected, so only the qualified name resolves.
    let packets = query(&mut client, &mut reader, "SELECT id FROM rows");
    assert_eq!(error_code(&packets[0]), 1146);
    let packets = query(
        &mut client,
        &mut reader,
        &format!("SELECT id FROM {SCHEMA}.rows"),
    );
    assert_eq!(packets[0], vec![1]);
    quit(&mut client, server);
}

#[test]
fn an_empty_initial_database_is_not_a_schema_selection() {
    let (mut client, _reader, server, answer) = connect(InitialDatabase::EmptyField);
    assert_eq!(answer[0], 0x00, "got {answer:02x?}");
    quit(&mut client, server);
}

#[test]
fn a_nonexistent_initial_database_is_unknown_database_not_access_denied() {
    let (client, _reader, server, answer) = connect(InitialDatabase::Named("campaign31_missing"));
    assert_eq!(
        error_code(&answer),
        1049,
        "a missing schema must not be reported as access denied"
    );
    let message = String::from_utf8_lossy(&answer[9..]).into_owned();
    assert_eq!(message, "Unknown database 'campaign31_missing'");
    let _ = client.try_clone();
    server.join().unwrap();
}

#[test]
fn a_database_name_needing_escaping_is_passed_through_verbatim() {
    // Go renders `use `+"`"+db+"`" and so has to care about backquotes; the
    // name travels as a plain string here, so a name that would need quoting
    // reaches the session exactly as the client wrote it.
    let (client, _reader, server, answer) = connect(InitialDatabase::Named("weird`name"));
    assert_eq!(error_code(&answer), 1049);
    let message = String::from_utf8_lossy(&answer[9..]).into_owned();
    assert_eq!(message, "Unknown database 'weird`name'");
    let _ = client.try_clone();
    server.join().unwrap();
}

#[test]
fn com_init_db_switches_the_selected_schema() {
    let (mut client, mut reader, server, answer) = connect(InitialDatabase::None);
    assert_eq!(answer[0], 0x00);

    let mut payload = vec![COM_INIT_DB];
    payload.extend_from_slice(SCHEMA.as_bytes());
    let mut writer = PacketWriter::with_sequence(&mut client, 0);
    writer.write_packet(&payload).unwrap();
    writer.flush().unwrap();
    reader.set_sequence(1);
    let answer = reader.read_packet().unwrap();
    assert_eq!(
        answer[0], 0x00,
        "`USE {SCHEMA}` must be accepted, got {answer:02x?}"
    );

    let packets = query(&mut client, &mut reader, "SELECT id FROM rows");
    assert_eq!(packets[0], vec![1]);

    let mut payload = vec![COM_INIT_DB];
    payload.extend_from_slice(b"campaign31_missing");
    let mut writer = PacketWriter::with_sequence(&mut client, 0);
    writer.write_packet(&payload).unwrap();
    writer.flush().unwrap();
    reader.set_sequence(1);
    let answer = reader.read_packet().unwrap();
    assert_eq!(error_code(&answer), 1049);

    quit(&mut client, server);
}
