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
use tidb_protocol::{
    PacketReader, PacketWriter, COM_QUERY, COM_STMT_CLOSE, COM_STMT_EXECUTE, COM_STMT_FETCH,
    COM_STMT_PREPARE, COM_STMT_RESET, DEFAULT_MAX_ALLOWED_PACKET,
};
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
        // 0xfb is the protocol's NULL marker. `read_text_value` distinguishes
        // it; a caller that reaches here for a NULL reads an empty value.
        0xfb => (0, 1),
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

/// Reads one value of a TEXT result-set row, where NULL is its own 0xfb
/// marker rather than a zero-length string.
fn read_text_value(packet: &mut &[u8]) -> String {
    if packet[0] == 0xfb {
        *packet = &packet[1..];
        return "NULL".to_owned();
    }
    String::from_utf8_lossy(&read_length_encoded_string(packet)).into_owned()
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
            row.push(read_text_value(&mut remaining));
        }
        rows.push(row);
    }
    rows
}

/// Sends COM_STMT_PREPARE and returns `(statement_id, parameter_count,
/// column_count)` from the prepare-OK packet, consuming the parameter and
/// column definitions that follow it.
fn prepare_statement(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    sql: &str,
) -> (u32, u16, u16) {
    let mut command = vec![COM_STMT_PREPARE];
    command.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &command);
    reader.set_sequence(1);
    let response = reader.read_packet().unwrap();
    assert_eq!(response[0], 0x00, "prepare errored: {response:?}");
    let statement_id = u32::from_le_bytes(response[1..5].try_into().unwrap());
    let column_count = u16::from_le_bytes(response[5..7].try_into().unwrap());
    let parameter_count = u16::from_le_bytes(response[7..9].try_into().unwrap());
    for _ in 0..parameter_count {
        reader.read_packet().unwrap();
    }
    if parameter_count > 0 {
        // EOF terminating the parameter definitions, when the server sends it.
    }
    for _ in 0..column_count {
        reader.read_packet().unwrap();
    }
    (statement_id, parameter_count, column_count)
}

/// The column type byte a protocol-41 column definition carries, which a
/// binary row's own encoding depends on.
fn column_definition_type(packet: &[u8]) -> u8 {
    let mut remaining = packet;
    // catalog, schema, table, org_table, name, org_name.
    for _ in 0..6 {
        read_length_encoded_string(&mut remaining);
    }
    // The fixed-length block: 0x0c, charset(2), column_length(4), type(1).
    remaining[1 + 2 + 4]
}

/// Reads one value of a binary result-set row, whose encoding is fixed by the
/// column's own type rather than always length-encoded.
fn read_binary_value(packet: &mut &[u8], column_type: u8) -> String {
    match column_type {
        // MYSQL_TYPE_LONGLONG.
        8 => {
            let value = i64::from_le_bytes(packet[..8].try_into().unwrap());
            *packet = &packet[8..];
            value.to_string()
        }
        // MYSQL_TYPE_LONG / INT24.
        3 | 9 => {
            let value = i32::from_le_bytes(packet[..4].try_into().unwrap());
            *packet = &packet[4..];
            value.to_string()
        }
        // MYSQL_TYPE_SHORT / YEAR.
        2 | 13 => {
            let value = i16::from_le_bytes(packet[..2].try_into().unwrap());
            *packet = &packet[2..];
            value.to_string()
        }
        // MYSQL_TYPE_TINY.
        1 => {
            let value = packet[0] as i8;
            *packet = &packet[1..];
            value.to_string()
        }
        // MYSQL_TYPE_DOUBLE.
        5 => {
            let value = f64::from_le_bytes(packet[..8].try_into().unwrap());
            *packet = &packet[8..];
            value.to_string()
        }
        // Everything else -- strings, decimals -- is length-encoded.
        _ => String::from_utf8_lossy(&read_length_encoded_string(packet)).into_owned(),
    }
}

/// Sends COM_STMT_EXECUTE with signed-BIGINT parameters and reads the binary
/// result set back as strings.
fn execute_statement(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    statement_id: u32,
    parameters: &[i64],
) -> Vec<Vec<String>> {
    let mut command = vec![COM_STMT_EXECUTE];
    command.extend_from_slice(&statement_id.to_le_bytes());
    command.push(0); // no cursor
    command.extend_from_slice(&1u32.to_le_bytes()); // iteration count
    if !parameters.is_empty() {
        let null_bitmap_len = parameters.len().div_ceil(8);
        command.extend(std::iter::repeat_n(0u8, null_bitmap_len));
        command.push(1); // a new parameter-type vector follows
        for _ in parameters {
            command.push(8); // MYSQL_TYPE_LONGLONG
            command.push(0); // signed
        }
        for value in parameters {
            command.extend_from_slice(&value.to_le_bytes());
        }
    }
    write_packet(client, 0, &command);
    reader.set_sequence(1);

    let first = reader.read_packet().unwrap();
    assert_ne!(first[0], 0xff, "execute errored: {first:?}");
    let column_count = usize::from(first[0]);
    let mut column_types = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let definition = reader.read_packet().unwrap();
        column_types.push(column_definition_type(&definition));
    }
    let mut rows = Vec::new();
    loop {
        let packet = reader.read_packet().unwrap();
        if packet[0] == 0xfe && packet.len() < 9 + 4 {
            break;
        }
        // A binary row is: 0x00 header, a NULL bitmap offset by two bits,
        // then each value packed by its own column type.
        let null_bitmap_len = (column_count + 7 + 2) / 8;
        let null_bitmap = &packet[1..1 + null_bitmap_len];
        let mut remaining = &packet[1 + null_bitmap_len..];
        let mut row = Vec::new();
        for (index, column_type) in column_types.iter().enumerate() {
            let bit = index + 2;
            if null_bitmap[bit / 8] & (1 << (bit % 8)) != 0 {
                row.push("NULL".to_owned());
                continue;
            }
            row.push(read_binary_value(&mut remaining, *column_type));
        }
        rows.push(row);
    }
    rows
}

/// Sends COM_STMT_EXECUTE with explicitly typed parameters -- the shapes a
/// real driver binds -- and reads the binary result set back.
fn execute_statement_typed(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    statement_id: u32,
    parameters: &[(u8, u8, Vec<u8>)],
    null_flags: &[bool],
) -> Vec<Vec<String>> {
    let mut command = vec![COM_STMT_EXECUTE];
    command.extend_from_slice(&statement_id.to_le_bytes());
    command.push(0);
    command.extend_from_slice(&1u32.to_le_bytes());
    let null_bitmap_len = parameters.len().div_ceil(8);
    let mut bitmap = vec![0u8; null_bitmap_len];
    for (index, is_null) in null_flags.iter().enumerate() {
        if *is_null {
            bitmap[index / 8] |= 1 << (index % 8);
        }
    }
    command.extend_from_slice(&bitmap);
    command.push(1);
    for (type_code, flag, _) in parameters {
        command.push(*type_code);
        command.push(*flag);
    }
    for (index, (_, _, value_bytes)) in parameters.iter().enumerate() {
        // A NULL parameter carries no bytes at all.
        if !null_flags[index] {
            command.extend_from_slice(value_bytes);
        }
    }
    write_packet(client, 0, &command);
    reader.set_sequence(1);

    let first = reader.read_packet().unwrap();
    assert_ne!(first[0], 0xff, "execute errored: {first:?}");
    // A write answers with one OK packet and no result set at all, which is
    // what an INSERT prepared this way returns.
    if first[0] == 0x00 {
        return Vec::new();
    }
    let column_count = usize::from(first[0]);
    let mut column_types = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let definition = reader.read_packet().unwrap();
        column_types.push(column_definition_type(&definition));
    }
    let mut rows = Vec::new();
    loop {
        let packet = reader.read_packet().unwrap();
        if packet[0] == 0xfe && packet.len() < 9 + 4 {
            break;
        }
        let null_bitmap_len = (column_count + 7 + 2) / 8;
        let null_bitmap = &packet[1..1 + null_bitmap_len];
        let mut remaining = &packet[1 + null_bitmap_len..];
        let mut row = Vec::new();
        for (index, column_type) in column_types.iter().enumerate() {
            let bit = index + 2;
            if null_bitmap[bit / 8] & (1 << (bit % 8)) != 0 {
                row.push("NULL".to_owned());
                continue;
            }
            row.push(read_binary_value(&mut remaining, *column_type));
        }
        rows.push(row);
    }
    rows
}

/// Sends a cursor-mode COM_STMT_EXECUTE (CURSOR_TYPE_READ_ONLY) and returns
/// the status flags of the metadata-terminating EOF/OK, consuming the column
/// definitions. In cursor mode the execute answers with NO rows.
fn execute_with_cursor(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    statement_id: u32,
    parameters: &[i64],
) -> (usize, u16) {
    let mut command = vec![COM_STMT_EXECUTE];
    command.extend_from_slice(&statement_id.to_le_bytes());
    command.push(0x01); // CURSOR_TYPE_READ_ONLY
    command.extend_from_slice(&1u32.to_le_bytes());
    if !parameters.is_empty() {
        let null_bitmap_len = parameters.len().div_ceil(8);
        command.extend(std::iter::repeat_n(0u8, null_bitmap_len));
        command.push(1);
        for _ in parameters {
            command.push(8);
            command.push(0);
        }
        for value in parameters {
            command.extend_from_slice(&value.to_le_bytes());
        }
    }
    write_packet(client, 0, &command);
    reader.set_sequence(1);
    let first = reader.read_packet().unwrap();
    assert_ne!(first[0], 0xff, "cursor execute errored: {first:?}");
    let column_count = usize::from(first[0]);
    for _ in 0..column_count {
        reader.read_packet().unwrap();
    }
    // The terminator carries the cursor bit; in deprecate-EOF mode it is an
    // OK-as-EOF (0xfe header) whose status sits after two length-encoded
    // ints; the status of a legacy EOF sits at bytes 3..5.
    let terminator = reader.read_packet().unwrap();
    assert_eq!(terminator[0], 0xfe, "metadata terminator: {terminator:?}");
    let status = terminator_status(&terminator);
    (column_count, status)
}

/// The status flags of a 0xfe-headed terminator, in both its legacy-EOF and
/// its OK-as-EOF (deprecate-EOF) encodings.
fn terminator_status(packet: &[u8]) -> u16 {
    if packet.len() == 5 {
        return u16::from_le_bytes([packet[3], packet[4]]);
    }
    // OK-as-EOF: header, affected-rows lenenc, last-insert-id lenenc, status.
    let mut remaining = &packet[1..];
    let _ = read_length_encoded_string_int(&mut remaining);
    let _ = read_length_encoded_string_int(&mut remaining);
    u16::from_le_bytes([remaining[0], remaining[1]])
}

/// Reads one length-encoded integer (the small forms the tests produce).
fn read_length_encoded_string_int(packet: &mut &[u8]) -> u64 {
    let first = packet[0];
    match first {
        0xfc => {
            let value = u64::from(u16::from_le_bytes([packet[1], packet[2]]));
            *packet = &packet[3..];
            value
        }
        other => {
            *packet = &packet[1..];
            u64::from(other)
        }
    }
}

/// Sends COM_STMT_FETCH and returns the decoded rows plus the terminating
/// status flags.
fn fetch_rows(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    statement_id: u32,
    fetch_size: u32,
    column_types: &[u8],
) -> (Vec<Vec<String>>, u16) {
    let mut command = vec![COM_STMT_FETCH];
    command.extend_from_slice(&statement_id.to_le_bytes());
    command.extend_from_slice(&fetch_size.to_le_bytes());
    write_packet(client, 0, &command);
    reader.set_sequence(1);
    let mut rows = Vec::new();
    loop {
        let packet = reader.read_packet().unwrap();
        assert_ne!(packet[0], 0xff, "fetch errored: {packet:?}");
        if packet[0] == 0xfe && packet.len() < 9 + 4 {
            return (rows, terminator_status(&packet));
        }
        let column_count = column_types.len();
        let null_bitmap_len = (column_count + 7 + 2) / 8;
        let null_bitmap = &packet[1..1 + null_bitmap_len];
        let mut remaining = &packet[1 + null_bitmap_len..];
        let mut row = Vec::new();
        for (index, column_type) in column_types.iter().enumerate() {
            let bit = index + 2;
            if null_bitmap[bit / 8] & (1 << (bit % 8)) != 0 {
                row.push("NULL".to_owned());
                continue;
            }
            row.push(read_binary_value(&mut remaining, *column_type));
        }
        rows.push(row);
    }
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

    // The binary protocol: PREPARE reports the marker count, EXECUTE binds
    // the values and answers with a real binary result set. This is how a
    // JDBC or Go-driver client runs a parameterized statement.
    let (statement_id, parameter_count, _columns) =
        prepare_statement(&mut client, &mut reader, "SELECT a, b FROM t WHERE a = ?");
    assert_eq!(parameter_count, 1);
    assert_eq!(
        execute_statement(&mut client, &mut reader, statement_id, &[2]),
        vec![vec!["2".to_owned(), "20".to_owned()]]
    );
    // The same statement runs again with a different value, which is the
    // point of preparing it.
    assert_eq!(
        execute_statement(&mut client, &mut reader, statement_id, &[3]),
        vec![vec!["3".to_owned(), "30".to_owned()]]
    );
    // The parameter families a real driver binds: a NULL, an unsigned value,
    // a DOUBLE and a DECIMAL each reach the engine as their own datum.
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "CREATE TABLE p (id BIGINT, note VARCHAR(20), amount DECIMAL(10,2), ratio DOUBLE)"
        ),
        0
    );
    let (insert_id, insert_params, _) = prepare_statement(
        &mut client,
        &mut reader,
        "INSERT INTO p (id, note, amount, ratio) VALUES (?, ?, ?, ?)",
    );
    assert_eq!(insert_params, 4);
    let decimal_digits = b"12.34";
    let mut decimal_bytes = vec![decimal_digits.len() as u8];
    decimal_bytes.extend_from_slice(decimal_digits);
    execute_statement_typed(
        &mut client,
        &mut reader,
        insert_id,
        &[
            (0x08, 0x80, u64::from(7u32).to_le_bytes().to_vec()), // unsigned BIGINT
            (0x0f, 0, vec![0]),                                   // NULL VARCHAR
            (0xf6, 0, decimal_bytes),                             // DECIMAL digits
            (0x05, 0, 1.5_f64.to_bits().to_le_bytes().to_vec()),  // DOUBLE
        ],
        &[false, true, false, false],
    );
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            "SELECT id, note, amount, ratio FROM p"
        ),
        vec![vec![
            "7".to_owned(),
            "NULL".to_owned(),
            "12.34".to_owned(),
            "1.5".to_owned()
        ]]
    );

    // A temporal parameter: the driver sends the packed date-time and the
    // engine stores what Go renders from those same bytes.
    assert_eq!(
        run_write(&mut client, &mut reader, "CREATE TABLE d (t VARCHAR(30))"),
        0
    );
    let (date_id, date_params, _) =
        prepare_statement(&mut client, &mut reader, "INSERT INTO d (t) VALUES (?)");
    assert_eq!(date_params, 1);
    let mut datetime_payload = vec![7];
    datetime_payload.extend_from_slice(&2020_u16.to_le_bytes());
    datetime_payload.extend_from_slice(&[3, 5, 6, 7, 8]);
    execute_statement_typed(
        &mut client,
        &mut reader,
        date_id,
        &[(0x0c, 0, datetime_payload)], // MYSQL_TYPE_DATETIME
        &[false],
    );
    assert_eq!(
        run_query(&mut client, &mut reader, "SELECT t FROM d"),
        vec![vec!["2020-03-05 06:07:08".to_owned()]]
    );

    // COM_STMT_RESET returns a statement to its post-prepare state and
    // answers with an OK packet; the statement still executes afterwards,
    // which is the point of resetting rather than closing.
    let mut reset = vec![COM_STMT_RESET];
    reset.extend_from_slice(&statement_id.to_le_bytes());
    write_packet(&mut client, 0, &reset);
    reader.set_sequence(1);
    let reset_response = reader.read_packet().unwrap();
    assert_eq!(reset_response[0], 0x00, "reset answers OK: {reset_response:?}");
    assert_eq!(
        execute_statement(&mut client, &mut reader, statement_id, &[2]),
        vec![vec!["2".to_owned(), "20".to_owned()]]
    );
    // An unknown handle is the same error the execute path reports.
    let mut bad_reset = vec![COM_STMT_RESET];
    bad_reset.extend_from_slice(&9999_u32.to_le_bytes());
    write_packet(&mut client, 0, &bad_reset);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0xff);

    let mut close = vec![COM_STMT_CLOSE];
    close.extend_from_slice(&statement_id.to_le_bytes());
    write_packet(&mut client, 0, &close);

    // The identity the handshake matched reaches the session: CURRENT_USER()
    // reports the matched grant identity and USER() the host the client
    // connected from. This plumbing did not exist -- the factory discarded
    // the session context.
    let identities = run_query(
        &mut client,
        &mut reader,
        "SELECT CURRENT_USER(), USER(), SESSION_USER()",
    );
    // The configured grant is `alice@%`, and the client connects from
    // loopback -- so the matched identity keeps the grant's host pattern
    // while the login identity carries the real peer address.
    assert_eq!(identities[0][0], "alice@%", "current user: {identities:?}");
    assert_eq!(
        identities[0][1], "alice@127.0.0.1",
        "login user: {identities:?}"
    );
    assert_eq!(identities[0][2], identities[0][1], "SESSION_USER is USER");

    // A read-only server-side cursor, which a useCursorFetch JDBC client
    // drives: the execute answers with only the column definitions and a
    // status advertising the cursor; the rows arrive in fetch-sized batches,
    // and the final batch's status drops the cursor bit and sets
    // SERVER_STATUS_LAST_ROW_SEND.
    let (cursor_stmt, _, _) =
        prepare_statement(&mut client, &mut reader, "SELECT a, b FROM t ORDER BY a");
    let (cursor_columns, execute_status) =
        execute_with_cursor(&mut client, &mut reader, cursor_stmt, &[]);
    assert_eq!(cursor_columns, 2);
    assert_ne!(execute_status & 0x0040, 0, "cursor advertised: {execute_status:#06x}");
    // t currently holds rows (1,10),(2,20),(3,30),(4,40) from the earlier
    // stages of this test; fetch two at a time.
    let types = [0x08u8, 0x08];
    let (batch1, status1) = fetch_rows(&mut client, &mut reader, cursor_stmt, 2, &types);
    assert_eq!(
        batch1,
        vec![
            vec!["1".to_owned(), "10".to_owned()],
            vec!["2".to_owned(), "20".to_owned()]
        ]
    );
    assert_ne!(status1 & 0x0040, 0, "cursor still open: {status1:#06x}");
    assert_eq!(status1 & 0x0080, 0, "not the last batch: {status1:#06x}");
    let (batch2, status2) = fetch_rows(&mut client, &mut reader, cursor_stmt, 10, &types);
    assert_eq!(batch2.len(), 2, "{batch2:?}");
    assert_eq!(batch2[1], vec!["4".to_owned(), "40".to_owned()]);
    assert_eq!(status2 & 0x0040, 0, "cursor closed: {status2:#06x}");
    assert_ne!(status2 & 0x0080, 0, "last row sent: {status2:#06x}");
    // A fetch after exhaustion is Go's ErrSpCursorNotOpen.
    let mut dead_fetch = vec![COM_STMT_FETCH];
    dead_fetch.extend_from_slice(&cursor_stmt.to_le_bytes());
    dead_fetch.extend_from_slice(&5u32.to_le_bytes());
    write_packet(&mut client, 0, &dead_fetch);
    reader.set_sequence(1);
    let dead = reader.read_packet().unwrap();
    assert_eq!(dead[0], 0xff, "fetch on a closed cursor errors: {dead:?}");

    // The features the last integration landed, each proven through the
    // socket rather than only in process -- this seam has hidden three
    // working-in-process features before.
    // SHOW STATUS answers rows.
    let status = run_query(&mut client, &mut reader, "SHOW STATUS LIKE 'Compression'");
    assert_eq!(status.len(), 1, "{status:?}");
    assert_eq!(status[0][0], "Compression");
    // INSERT ... SET writes through the same OK path.
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "CREATE TABLE ws (a BIGINT PRIMARY KEY, b BIGINT DEFAULT 7)"
        ),
        0
    );
    assert_eq!(
        run_write(&mut client, &mut reader, "INSERT INTO ws SET a = 1"),
        1
    );
    assert_eq!(
        run_query(&mut client, &mut reader, "SELECT a, b FROM ws"),
        vec![vec!["1".to_owned(), "7".to_owned()]]
    );
    // WITH ROLLUP and a multi-argument, inner-ordered GROUP_CONCAT.
    run_write(
        &mut client,
        &mut reader,
        "CREATE TABLE wr (g BIGINT, v VARCHAR(10), n BIGINT)",
    );
    run_write(
        &mut client,
        &mut reader,
        "INSERT INTO wr VALUES (1,'b',2),(1,'a',1),(2,'c',3)",
    );
    let rollup = run_query(
        &mut client,
        &mut reader,
        "SELECT g, SUM(n) FROM wr GROUP BY g WITH ROLLUP ORDER BY g, SUM(n)",
    );
    assert_eq!(
        rollup,
        vec![
            vec!["NULL".to_owned(), "6".to_owned()],
            vec!["1".to_owned(), "3".to_owned()],
            vec!["2".to_owned(), "3".to_owned()],
        ],
        "rollup over the wire"
    );
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            "SELECT GROUP_CONCAT(v, n ORDER BY n SEPARATOR '|') FROM wr"
        ),
        vec![vec!["a1|b2|c3".to_owned()]]
    );
    // RETURNING parses and is ignored, answering a plain OK like Go.
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "INSERT INTO ws (a) VALUES (2) RETURNING a"
        ),
        1
    );

    // COM_QUIT ends the connection cleanly.
    write_packet(&mut client, 0, &[0x01]);
    drop(client);
    worker.join().unwrap();
}
