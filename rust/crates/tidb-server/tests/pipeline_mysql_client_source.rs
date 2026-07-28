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

//! End-to-end over TCP: a raw MySQL-protocol client runs CREATE TABLE,
//! INSERT, and SELECT against the NEW pipeline session
//! (`PipelineSessionFactory` -> `tidb_session::Session` -> real TiKV-format
//! bytes) through the real handshake/auth/COM_QUERY wire path.
//!
//! Writes and DDL answer with a real MySQL OK packet carrying `affected_rows`
//! (through the `QuerySession::execute_write` hook), exactly as a stock client
//! expects; queries answer with a streamed text result set.

use sha1::{Digest, Sha1};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use tidb_protocol::{PacketReader, PacketWriter, COM_QUERY, DEFAULT_MAX_ALLOWED_PACKET};
use tidb_server::{
    serve_mysql_connection, ConfiguredUserStore, ConnectionCancellation, ConnectionTracker,
    PipelineSessionFactory,
};

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

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

fn authenticate(client: &mut TcpStream, reader: &mut PacketReader<TcpStream>) {
    reader.set_sequence(0);
    let initial = reader.read_packet().unwrap();
    let salt = handshake_salt(&initial);
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
    response.extend_from_slice(b"alice");
    response.push(0);
    let auth = native_response(b"secret", &salt);
    response.push(u8::try_from(auth.len()).unwrap());
    response.extend_from_slice(&auth);
    response.extend_from_slice(b"mysql_native_password\0");
    response.push(0);
    write_packet(client, 1, &response);
    reader.set_sequence(2);
    assert_eq!(reader.read_packet().unwrap()[0], 0, "auth OK");
}

/// Reads a protocol length-encoded string, including the multi-byte length
/// prefixes a value longer than 250 bytes uses -- `SHOW CREATE TABLE` and
/// `sql_mode` both produce those.
fn read_length_encoded_string(packet: &mut &[u8]) -> Vec<u8> {
    let first = packet[0];
    let (length, header) = match first {
        0xfb => (0, 1), // NULL, which reads as an empty value here.
        0xfc => (
            usize::from(u16::from_le_bytes([packet[1], packet[2]])),
            3,
        ),
        0xfd => (
            u32::from_le_bytes([packet[1], packet[2], packet[3], 0]) as usize,
            4,
        ),
        0xfe => (
            usize::try_from(u64::from_le_bytes(
                packet[1..9].try_into().expect("eight length bytes"),
            ))
            .expect("a length that fits this platform"),
            9,
        ),
        other => (usize::from(other), 1),
    };
    *packet = &packet[header..];
    let (value, remaining) = packet.split_at(length);
    *packet = remaining;
    value.to_vec()
}

/// Sends one COM_QUERY expected to answer with an OK packet (a write or DDL)
/// and returns its `affected_rows`.
fn run_write(client: &mut TcpStream, reader: &mut PacketReader<TcpStream>, sql: &str) -> u64 {
    let mut command = vec![COM_QUERY];
    command.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &command);
    reader.set_sequence(1);
    let packet = reader.read_packet().unwrap();
    assert_eq!(packet[0], 0x00, "a write answers with an OK packet: {packet:?}");
    // OK payload: header 0x00, length-encoded affected_rows, ...
    let affected = packet[1];
    assert!(affected < 0xfb, "test writes report small counts");
    u64::from(affected)
}

/// Sends one write and returns the OK packet's `last_insert_id`, the field a
/// client reads a generated key from.
fn run_write_insert_id(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    sql: &str,
) -> u64 {
    let mut command = vec![COM_QUERY];
    command.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &command);
    reader.set_sequence(1);
    let packet = reader.read_packet().unwrap();
    assert_eq!(packet[0], 0x00, "a write answers with an OK packet: {packet:?}");
    // OK payload: header, length-encoded affected_rows, then length-encoded
    // last_insert_id.
    assert!(packet[1] < 0xfb, "test writes report small counts");
    assert!(packet[2] < 0xfb, "test ids are small");
    u64::from(packet[2])
}

/// Sends one transaction-control COM_QUERY and returns whether the OK
/// packet's status flags advertise `SERVER_STATUS_IN_TRANS` (0x0001).
fn run_transaction_control(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    sql: &str,
) -> bool {
    let mut command = vec![COM_QUERY];
    command.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &command);
    reader.set_sequence(1);
    let packet = reader.read_packet().unwrap();
    assert_eq!(
        packet[0], 0x00,
        "transaction control answers with an OK packet: {packet:?}"
    );
    // OK payload: header, affected_rows, last_insert_id, then status flags.
    let status = u16::from_le_bytes([packet[3], packet[4]]);
    status & 0x0001 != 0
}

/// Sends one COM_QUERY and reads its (deprecate-EOF) text result set:
/// column-count, column definitions, rows, terminal OK-with-0xFE-header.
/// Returns each row's columns as strings.
fn run_query(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    sql: &str,
) -> Vec<Vec<String>> {
    let mut command = vec![COM_QUERY];
    command.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &command);
    reader.set_sequence(1);

    let first = reader.read_packet().unwrap();
    assert_ne!(first[0], 0xff, "query errored: {first:?}");
    let column_count = usize::from(first[0]);
    assert!(column_count > 0, "the pipeline answers with result sets");
    for _ in 0..column_count {
        let _column_definition = reader.read_packet().unwrap();
    }
    let mut rows = Vec::new();
    loop {
        let packet = reader.read_packet().unwrap();
        if packet[0] == 0xfe && packet.len() < 9 + 4 {
            break; // terminal OK with EOF header (deprecate-EOF mode)
        }
        let mut remaining = packet.as_slice();
        let mut row = Vec::new();
        for _ in 0..column_count {
            row.push(String::from_utf8(read_length_encoded_string(&mut remaining)).unwrap());
        }
        rows.push(row);
    }
    rows
}

/// The deployment-ladder proof: a raw MySQL client speaks the real wire
/// protocol to the new engine and reads its own data back.
#[test]
fn mysql_client_runs_the_pipeline_end_to_end() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &PipelineSessionFactory::default(),
            &users(),
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader);

    // DDL and DML answer with real OK packets, as a stock client expects.
    assert_eq!(
        run_write(&mut client, &mut reader, "CREATE TABLE t (a BIGINT, b BIGINT)"),
        0
    );
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)"
        ),
        3
    );
    // The query reads the rows back through real TiKV-format bytes.
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            "SELECT a, b FROM t WHERE a > 1 ORDER BY b DESC"
        ),
        vec![
            vec!["3".to_owned(), "30".to_owned()],
            vec!["2".to_owned(), "20".to_owned()],
        ]
    );

    // The statements a stock MySQL client sends when it connects: SET NAMES
    // answers with an OK packet, and the server properties read back as rows.
    assert_eq!(run_write(&mut client, &mut reader, "SET NAMES utf8mb4"), 0);
    assert_eq!(
        run_write(&mut client, &mut reader, "SET autocommit = 1, sql_mode = 'ANSI_QUOTES'"),
        0
    );
    let comment = run_query(&mut client, &mut reader, "SELECT @@version_comment");
    assert!(
        comment[0][0].starts_with("TiDB Server (Apache License 2.0)"),
        "{comment:?}"
    );
    assert_eq!(
        run_query(&mut client, &mut reader, "SELECT @@sql_mode"),
        vec![vec!["ANSI_QUOTES".to_owned()]]
    );

    // A generated key comes back in the OK packet, which is how a client
    // reads it, and LAST_INSERT_ID() reports the same value.
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "CREATE TABLE gen (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)"
        ),
        0
    );
    assert_eq!(
        run_write_insert_id(&mut client, &mut reader, "INSERT INTO gen (v) VALUES (1)"),
        1
    );
    assert_eq!(
        run_write_insert_id(&mut client, &mut reader, "INSERT INTO gen (v) VALUES (2), (3)"),
        2,
        "a multi-row insert reports its first id"
    );
    assert_eq!(
        run_query(&mut client, &mut reader, "SELECT LAST_INSERT_ID()"),
        vec![vec!["2".to_owned()]]
    );

    // A transaction over the wire: BEGIN/COMMIT answer with OK packets whose
    // status advertises SERVER_STATUS_IN_TRANS, and the staged write is
    // visible to the transaction itself and survives the commit.
    assert!(
        run_transaction_control(&mut client, &mut reader, "BEGIN"),
        "BEGIN advertises SERVER_STATUS_IN_TRANS"
    );
    assert_eq!(
        run_write(&mut client, &mut reader, "INSERT INTO t VALUES (4, 40)"),
        1
    );
    assert_eq!(
        run_query(&mut client, &mut reader, "SELECT a FROM t WHERE a = 4"),
        vec![vec!["4".to_owned()]]
    );
    assert!(
        !run_transaction_control(&mut client, &mut reader, "COMMIT"),
        "COMMIT clears SERVER_STATUS_IN_TRANS"
    );
    assert_eq!(
        run_query(&mut client, &mut reader, "SELECT a FROM t WHERE a = 4"),
        vec![vec!["4".to_owned()]]
    );

    // ROLLBACK discards the staged write.
    run_transaction_control(&mut client, &mut reader, "BEGIN");
    run_write(&mut client, &mut reader, "INSERT INTO t VALUES (5, 50)");
    run_transaction_control(&mut client, &mut reader, "ROLLBACK");
    assert_eq!(
        run_query(&mut client, &mut reader, "SELECT a FROM t WHERE a = 5"),
        Vec::<Vec<String>>::new()
    );

    // The metadata statements a client and its tooling send: each answers
    // with a real result set over the wire, the same way `Session::run`
    // answers it in process. These went through a separate classification
    // on this path, which used to reject them.
    assert_eq!(
        run_query(&mut client, &mut reader, "SHOW VARIABLES LIKE 'autocommit'").len(),
        1
    );
    let created = run_query(&mut client, &mut reader, "SHOW CREATE TABLE t");
    assert!(created[0][1].starts_with("CREATE TABLE `t`"), "{created:?}");
    let columns = run_query(&mut client, &mut reader, "SHOW COLUMNS FROM t");
    assert_eq!(columns.len(), 2, "{columns:?}");
    let index = run_query(&mut client, &mut reader, "SHOW INDEX FROM gen");
    assert_eq!(index[0][2], "PRIMARY", "{index:?}");
    assert_eq!(
        run_query(&mut client, &mut reader, "SHOW TABLES")
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec!["gen".to_owned(), "t".to_owned()]
    );
    assert!(run_query(&mut client, &mut reader, "SHOW WARNINGS").is_empty());
    // USE answers with an OK packet, as a client expects when it switches
    // schema.
    assert_eq!(run_write(&mut client, &mut reader, "USE test"), 0);

    // COM_QUIT ends the connection cleanly.
    write_packet(&mut client, 0, &[0x01]);
    drop(client);
    worker.join().unwrap();
}
