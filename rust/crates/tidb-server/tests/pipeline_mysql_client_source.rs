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
//! INSERT, and SELECT against a node over an embedded store (its session
//! factory -> `tidb_session::Session` -> the store) through the real
//! handshake/auth/COM_QUERY wire path.
//!
//! Writes and DDL answer with a real MySQL OK packet carrying `affected_rows`
//! (through the `QuerySession::execute_write` hook), exactly as a stock client
//! expects; queries answer with a streamed text result set.

use sha1::{Digest, Sha1};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use tidb_protocol::PacketReader;
use tidb_protocol::PacketWriter;
use tidb_server::command::COM_PING;
use tidb_server::command::COM_QUERY;
use tidb_server::command::COM_STMT_CLOSE;
use tidb_server::command::COM_STMT_EXECUTE;
use tidb_server::command::COM_STMT_FETCH;
use tidb_server::command::COM_STMT_PREPARE;
use tidb_server::command::COM_STMT_RESET;
use tidb_protocol::DEFAULT_MAX_ALLOWED_PACKET;
use tidb_server::{
    serve_mysql_connection, ConfiguredUserStore, ConnectionCancellation, ConnectionExit,
    ConnectionTracker,
};
use tidb_session::domain::mock_store::MockStoreDomain;
use tidb_globalconn::parse_conn_id;

/// Go's server over a mock store whose node authenticates against, and keeps
/// its accounts in, `store`: connections are served from its session factory.
fn node_over(store: &ConfiguredUserStore) -> std::sync::Arc<MockStoreDomain> {
    MockStoreDomain::create_with_users(std::sync::Arc::new(store.clone()))
        .expect("boot the server's store")
}

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_FOUND_ROWS: u32 = 1 << 1;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

fn users() -> ConfiguredUserStore {
    // The account and DDL statements below are privileged on a Go TiDB, and
    // now here too: `alice`'s `mysql.user` row holds every global privilege
    // (`GRANT ALL ON *.* TO alice WITH GRANT OPTION`).
    let alice = tidb_privilege::loaded::LoadedUser {
        privileges: tidb_privilege::cluster_privilege_load::USER_PRIVILEGE_COLUMNS
            .iter()
            .map(|(_, name)| *name)
            .collect(),
        ..tidb_privilege::loaded::LoadedUser::new(
            "alice",
            "%",
            "mysql_native_password",
            "*14E65567ABDB5135D0CFD9A70B3032C179A49EE7",
        )
    };
    ConfiguredUserStore::from_accounts(tidb_privilege::load::registry_from_user_rows([alice]))
}

fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) {
    let mut writer = PacketWriter::with_sequence(stream, sequence);
    writer.write_packet(payload).unwrap();
    writer.flush().unwrap();
}

fn assert_error_packet(packet: &[u8], code: u16, state: &[u8; 5]) {
    assert_eq!(packet.first(), Some(&0xff), "ERR packet: {packet:?}");
    assert_eq!(
        u16::from_le_bytes([packet[1], packet[2]]),
        code,
        "ERR code: {packet:?}"
    );
    assert_eq!(&packet[3..4], b"#", "SQLSTATE marker: {packet:?}");
    assert_eq!(&packet[4..9], state, "SQLSTATE: {packet:?}");
}

fn assert_error_packet_exact(packet: &[u8], code: u16, state: &[u8; 5], message: &str) {
    assert_error_packet(packet, code, state);
    assert_eq!(&packet[9..], message.as_bytes(), "ERR message: {packet:?}");
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

fn handshake_capabilities(initial: &[u8]) -> u32 {
    assert_eq!(initial[0], 10);
    let version_end = initial[1..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| offset + 1)
        .unwrap();
    let lower_offset = version_end + 1 + 4 + 8 + 1;
    let upper_offset = lower_offset + 2 + 1 + 2;
    u32::from(u16::from_le_bytes([
        initial[lower_offset],
        initial[lower_offset + 1],
    ])) | (u32::from(u16::from_le_bytes([
        initial[upper_offset],
        initial[upper_offset + 1],
    ])) << 16)
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
    authenticate_with_capabilities(client, reader, 0);
}

fn authenticate_with_capabilities(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    additional_capabilities: u32,
) {
    let reply = handshake_reply(client, reader, additional_capabilities, 46);
    assert_eq!(reply[0], 0, "auth OK");
    // This handshake carries NO initial database, which is what a client
    // started without `-D` sends. Go leaves `CurrentDB` empty in that state
    // (`preprocess.go handleTableName` raises `ErrNoDB` for every unqualified
    // name), so a schema has to be chosen before the unqualified statements
    // below can run -- exactly as a real client must. This used to be
    // unnecessary only because the node defaulted the session into `test`
    // itself.
    let mut use_test = vec![COM_QUERY];
    use_test.extend_from_slice(b"USE test");
    write_packet(client, 0, &use_test);
    reader.set_sequence(1);
    assert_eq!(
        reader.read_packet().unwrap()[0],
        0,
        "USE test answers with an OK packet"
    );
}

/// Sends alice's HandshakeResponse41 carrying `collation` and returns the
/// server's reply to it: the OK packet, or the ERR that refused the login.
fn handshake_reply(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    additional_capabilities: u32,
    collation: u8,
) -> Vec<u8> {
    handshake_reply_as(client, reader, b"alice", additional_capabilities, collation)
}

/// [`handshake_reply`] for `user`, whose password is also `secret`.
fn handshake_reply_as(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    user: &[u8],
    additional_capabilities: u32,
    collation: u8,
) -> Vec<u8> {
    reader.set_sequence(0);
    let initial = reader.read_packet().unwrap();
    assert_eq!(
        handshake_capabilities(&initial) & additional_capabilities,
        additional_capabilities,
        "the server must advertise every requested optional capability"
    );
    let salt = handshake_salt(&initial);
    let capabilities = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH
        | CLIENT_CONNECT_ATTRS
        | CLIENT_DEPRECATE_EOF
        | additional_capabilities;
    let mut response = Vec::new();
    response.extend_from_slice(&capabilities.to_le_bytes());
    response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    response.push(collation);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(user);
    response.push(0);
    let auth = native_response(b"secret", &salt);
    response.push(u8::try_from(auth.len()).unwrap());
    response.extend_from_slice(&auth);
    response.extend_from_slice(b"mysql_native_password\0");
    response.push(0);
    write_packet(client, 1, &response);
    reader.set_sequence(2);
    reader.read_packet().unwrap()
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

/// Sends one small COM_QUERY write and returns its affected-row count and the
/// length-encoded info string from the OK packet.
fn run_write_with_info(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    sql: &str,
) -> (u64, String) {
    let mut command = vec![COM_QUERY];
    command.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &command);
    reader.set_sequence(1);
    let packet = reader.read_packet().unwrap();
    assert_eq!(packet[0], 0x00, "a write answers with an OK packet: {packet:?}");
    assert!(packet[1] < 0xfb, "test writes report small counts");
    assert!(packet[2] < 0xfb, "test writes report small ids");
    // header | affected | last_insert_id | status(2) | warnings(2) |
    // length-encoded info.
    let info = match packet.get(7).copied() {
        None => Vec::new(),
        Some(info_len) => packet
            .get(8..8 + usize::from(info_len))
            .expect("complete length-encoded OK info")
            .to_vec(),
    };
    (
        u64::from(packet[1]),
        String::from_utf8(info).expect("Go's update info is UTF-8"),
    )
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
        // Go dumpLengthEncodedInt uses 0xfe for a row cell only at
        // lengths >= 2^24. An OK-as-EOF may carry DML's info string and
        // exceed 13 bytes; its size is not the legacy EOF packet size.
        if packet[0] == 0xfe && packet.len() < (1 << 24) {
            break;
        }
        assert_ne!(packet[0], 0xff, "query {sql} failed while streaming: {packet:?}");
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

/// Sends COM_STMT_EXECUTE and classifies every packet of the answer in the
/// order it arrives, so a test can pin the SEQUENCE a client frames against
/// rather than only the decoded rows.
///
/// The tags are the client's own reading of each packet's first byte in
/// context: `header:<n>` is the column-count packet that opens a result set,
/// `ok` is an OK packet answering with no result set at all, `error` is an
/// error packet, `column` a definition, `row` a binary row, and `terminator`
/// the EOF/OK that closes the set.
fn execute_statement_packet_tags(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    statement_id: u32,
    parameters: &[i64],
) -> Vec<String> {
    let mut command = vec![COM_STMT_EXECUTE];
    command.extend_from_slice(&statement_id.to_le_bytes());
    command.push(0); // no cursor
    command.extend_from_slice(&1u32.to_le_bytes()); // iteration count
    if !parameters.is_empty() {
        command.extend(std::iter::repeat_n(0u8, parameters.len().div_ceil(8)));
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
    match first[0] {
        0xff => return vec!["error".to_owned()],
        0x00 => return vec!["ok".to_owned()],
        _ => {}
    }
    let column_count = usize::from(first[0]);
    let mut tags = vec![format!("header:{column_count}")];
    for _ in 0..column_count {
        reader.read_packet().unwrap();
        tags.push("column".to_owned());
    }
    loop {
        let packet = reader.read_packet().unwrap();
        // An EOF-shaped packet this short is the terminator; a binary row
        // begins with 0x00 and carries the row's own bytes.
        if packet[0] == 0xfe && packet.len() < 9 + 4 {
            tags.push("terminator".to_owned());
            return tags;
        }
        tags.push("row".to_owned());
    }
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
        // The store's account table and the factory's are ONE table: an
        // authenticated identity must be a row the session's `SHOW GRANTS`
        // can find.
        let store = users();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            node_over(&store).factory().as_ref(),
            &store,
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
    // An EXPLICIT auto value reaches the wire too -- Go's OK packet reports
    // `StmtCtx.LastInsertID` when the statement allocated and
    // `StmtCtx.InsertID`, the given value, otherwise. Captured from TiDB:
    // after `INSERT INTO g VALUES (50, 2)` the OK packet carries 50 while
    // `LAST_INSERT_ID()`, which only ever follows an ALLOCATED id, does not
    // move. Both are asserted here so the two can never diverge unseen.
    assert_eq!(
        run_write_insert_id(
            &mut client,
            &mut reader,
            "INSERT INTO gen (id, v) VALUES (50, 4)"
        ),
        50,
        "the OK packet reports the explicit auto value"
    );
    assert_eq!(
        run_query(&mut client, &mut reader, "SELECT LAST_INSERT_ID()"),
        vec![vec!["2".to_owned()]],
        "LAST_INSERT_ID() does not follow an explicit value"
    );
    // A statement that allocates nothing at all reports 0 on the wire and
    // still leaves the function where it was.
    assert_eq!(
        run_write_insert_id(&mut client, &mut reader, "UPDATE gen SET v = 5 WHERE id = 50"),
        0
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
    // The session is still under the ANSI_QUOTES set above, and Go escapes
    // SHOW CREATE identifiers by it (`stringutil.Escape`).
    assert!(created[0][1].starts_with("CREATE TABLE \"t\""), "{created:?}");
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

    // A view over the wire: created, read through, and listed beside the base
    // tables with its kind -- the whole life a client sees.
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "CREATE VIEW vwire AS SELECT a, b FROM t WHERE a >= 2"
        ),
        0
    );
    assert_eq!(
        run_query(&mut client, &mut reader, "SELECT b FROM vwire ORDER BY a"),
        vec![vec!["20".to_owned()], vec!["30".to_owned()], vec![
            "40".to_owned()
        ]]
    );
    let created_view = run_query(&mut client, &mut reader, "SHOW CREATE VIEW vwire");
    // Go `fetchShowCreateTable4View` escapes the definer, the view name and
    // its columns by the session's ANSI_QUOTES; the SELECT is the stored
    // restored text.
    assert_eq!(
        created_view[0][1],
        "CREATE ALGORITHM=UNDEFINED DEFINER=\"alice\"@\"%\" SQL SECURITY DEFINER VIEW \"vwire\" \
         (\"a\", \"b\") AS SELECT `a` AS `a`,`b` AS `b` FROM `test`.`t` WHERE `a`>=2",
        "{created_view:?}"
    );
    assert!(
        run_query(&mut client, &mut reader, "SHOW TABLES")
            .into_iter()
            .any(|row| row[0] == "vwire"),
        "SHOW TABLES lists the view"
    );
    assert!(
        run_query(&mut client, &mut reader, "SHOW FULL TABLES")
            .into_iter()
            .any(|row| row[0] == "vwire" && row[1] == "VIEW"),
        "SHOW FULL TABLES reports the view's kind"
    );
    assert_eq!(run_write(&mut client, &mut reader, "DROP VIEW vwire"), 0);

    // A ranking window function computes over the wire: RANK over distinct
    // values numbers the rows, and the outer ORDER BY runs after the window.
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            "SELECT a, RANK() OVER (ORDER BY b) FROM t ORDER BY a DESC"
        ),
        vec![
            vec!["4".to_owned(), "4".to_owned()],
            vec!["3".to_owned(), "3".to_owned()],
            vec!["2".to_owned(), "2".to_owned()],
            vec!["1".to_owned(), "1".to_owned()],
        ]
    );

    // A composite interval unit: the digit runs assign right-to-left, so
    // '1:30' HOUR_MINUTE adds one hour and thirty minutes.
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            "SELECT DATE_ADD('2024-01-31 10:20:30', INTERVAL '1:30' HOUR_MINUTE)"
        ),
        vec![vec!["2024-01-31 11:50:30".to_owned()]]
    );

    // EXPLAIN of a write describes without executing: the plan row comes
    // back and the table keeps its row count.
    let explained = run_query(
        &mut client,
        &mut reader,
        "EXPLAIN INSERT INTO t VALUES (9, 90)",
    );
    assert_eq!(explained[0][0], "Insert_1", "{explained:?}");
    assert_eq!(
        run_query(&mut client, &mut reader, "SELECT COUNT(*) FROM t"),
        vec![vec!["4".to_owned()]],
        "EXPLAIN INSERT must not insert"
    );

    // JSON evaluated as values over the wire: extraction operators, TiDB's
    // plain-byte key ordering, and the real JSON error code for a bad path.
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            r#"SELECT JSON_UNQUOTE(JSON_EXTRACT(JSON_OBJECT('b', 1, 'aa', 2), '$.aa')), JSON_TYPE('1.0')"#
        ),
        vec![vec!["2".to_owned(), "DOUBLE".to_owned()]]
    );

    // The privilege registry answers over the wire, and the grants it
    // reports are the ones the statements below are checked against.
    let grants = run_query(&mut client, &mut reader, "SHOW GRANTS");
    assert_eq!(
        grants[0][0],
        "GRANT ALL PRIVILEGES ON *.* TO 'alice'@'%' WITH GRANT OPTION",
        "{grants:?}"
    );
    // A fresh account reports USAGE, as Go's mysql.user does -- and reading
    // ANOTHER account's grants is itself privileged (Go: SELECT on `mysql`),
    // which this caller holds.
    assert_eq!(
        run_write(&mut client, &mut reader, "CREATE USER 'wireusage'@'%'"),
        0
    );
    let other = run_query(&mut client, &mut reader, "SHOW GRANTS FOR 'wireusage'@'%'");
    assert_eq!(
        other[0][0],
        "GRANT USAGE ON *.* TO 'wireusage'@'%'",
        "{other:?}"
    );

    // A JSON column round-trips through the real cell (type 245) with the
    // canonical key order, and a mutation function edits it in SQL.
    assert_eq!(
        run_write(&mut client, &mut reader, "CREATE TABLE jt (j JSON)"),
        0
    );
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            r#"INSERT INTO jt VALUES ('{"b":2,"a":1}')"#
        ),
        1
    );
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            r#"SELECT JSON_SET(j, '$.c', 3) FROM jt"#
        ),
        vec![vec![r#"{"a": 1, "b": 2, "c": 3}"#.to_owned()]]
    );

    // A window aggregate with a RANGE value bound and a correlated HAVING
    // comparison, both computed over the grouped rows.
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            "SELECT a, SUM(b) OVER (ORDER BY a RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) \
             FROM t ORDER BY a"
        ),
        vec![
            vec!["1".to_owned(), "10".to_owned()],
            vec!["2".to_owned(), "30".to_owned()],
            vec!["3".to_owned(), "50".to_owned()],
            vec!["4".to_owned(), "70".to_owned()],
        ]
    );

    // EXPLAIN ANALYZE really executes and reports the actual row count in
    // the 9-column shape, actRows exact for the full-scan read.
    let analyzed = run_query(
        &mut client,
        &mut reader,
        "EXPLAIN ANALYZE SELECT a FROM t WHERE a > 1",
    );
    assert_eq!(analyzed[0].len(), 9, "{analyzed:?}");
    let scan_row = analyzed
        .iter()
        .find(|row| row[0].contains("TableFullScan"))
        .expect("scan operator present");
    assert_eq!(scan_row[2], "4", "actRows counts the real scanned rows");

    // Roles over the wire: create, grant, activate, and read the active set.
    assert_eq!(run_write(&mut client, &mut reader, "CREATE ROLE wirerole"), 0);
    // Go grant executors have an empty schema and answer with an OK packet.
    assert_eq!(
        run_write(&mut client, &mut reader, "GRANT wirerole TO 'alice'@'%'"),
        0
    );
    assert_eq!(run_write(&mut client, &mut reader, "SET ROLE wirerole"), 0);
    assert_eq!(
        run_query(&mut client, &mut reader, "SELECT CURRENT_ROLE()"),
        vec![vec!["`wirerole`@`%`".to_owned()]]
    );

    // Charset resolution over the wire: a VARBINARY column reports the binary
    // charset while VARCHAR reports the session default, and BINARY(3) pads.
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "CREATE TABLE cs (v VARCHAR(4), b BINARY(3))"
        ),
        0
    );
    assert_eq!(
        run_write(&mut client, &mut reader, "INSERT INTO cs VALUES ('ab', 'ab')"),
        1
    );
    assert_eq!(
        run_query(&mut client, &mut reader, "SELECT HEX(b), LENGTH(b) FROM cs"),
        vec![vec!["616200".to_owned(), "3".to_owned()]]
    );
    let columns = run_query(&mut client, &mut reader, "SHOW FULL COLUMNS FROM cs");
    assert_eq!(columns[0][2], "utf8mb4_bin", "{columns:?}");
    assert_eq!(columns[1][2], "NULL", "binary column has no collation");

    // The processlist virtual table sees this very connection.
    let processes = run_query(
        &mut client,
        &mut reader,
        "SELECT ID, COMMAND FROM information_schema.PROCESSLIST",
    );
    assert_eq!(processes.len(), 1, "{processes:?}");
    assert_ne!(processes[0][0], "0", "a live connection id");

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
    // Go saves new types before splitting values, so both the first failure
    // and a later type-reuse execute report pkg/param's errno 8051.
    let mut unknown_type = vec![COM_STMT_EXECUTE];
    unknown_type.extend_from_slice(&statement_id.to_le_bytes());
    unknown_type.extend_from_slice(&[0, 1, 0, 0, 0, 0, 1, 0x9f, 0]);
    write_packet(&mut client, 0, &unknown_type);
    reader.set_sequence(1);
    assert_error_packet(&reader.read_packet().unwrap(), 8051, b"HY000");

    let mut reuse_unknown_type = vec![COM_STMT_EXECUTE];
    reuse_unknown_type.extend_from_slice(&statement_id.to_le_bytes());
    reuse_unknown_type.extend_from_slice(&[0, 1, 0, 0, 0, 0, 0]);
    write_packet(&mut client, 0, &reuse_unknown_type);
    reader.set_sequence(1);
    assert_error_packet(&reader.read_packet().unwrap(), 8051, b"HY000");

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

    // Binary strings use @@character_set_client, so GBK bytes are decoded
    // before storage and read back as UTF-8 after restoring the charset.
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "SET @@character_set_client = 'gbk'"
        ),
        0
    );
    assert_eq!(
        run_write(&mut client, &mut reader, "CREATE TABLE g (v VARCHAR(20))"),
        0
    );
    let (gbk_id, gbk_params, _) =
        prepare_statement(&mut client, &mut reader, "INSERT INTO g VALUES (?)");
    assert_eq!(gbk_params, 1);
    execute_statement_typed(
        &mut client,
        &mut reader,
        gbk_id,
        &[(0x0f, 0, vec![4, 0xb2, 0xe2, 0xca, 0xd4])],
        &[false],
    );
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "SET @@character_set_client = 'utf8mb4'"
        ),
        0
    );
    assert_eq!(
        run_query(&mut client, &mut reader, "SELECT v FROM g"),
        vec![vec!["测试".to_owned()]]
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

    // CONNECTION_ID() must reach the session the same way the identity did:
    // the factory once discarded `SessionContext` entirely, which would leave
    // this NULL instead of the accepted connection's id.
    let connection_id = run_query(&mut client, &mut reader, "SELECT CONNECTION_ID()");
    let id: u64 = connection_id[0][0]
        .parse()
        .unwrap_or_else(|_| panic!("CONNECTION_ID() should be numeric: {connection_id:?}"));
    assert!(id > 0, "CONNECTION_ID() should be positive: {id}");

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

    // The third integration's features, through the socket.
    // EXPLAIN answers the five-column plan table.
    // `t` has no primary key, so its plan is honestly a full scan; `gen`
    // has one, so its equality is a point get.
    let plan = run_query(&mut client, &mut reader, "EXPLAIN SELECT a FROM t WHERE a = 1");
    assert!(
        plan.iter().any(|row| row[0].contains("TableFullScan")),
        "explain over the wire: {plan:?}"
    );
    assert_eq!(plan[0].len(), 5, "{plan:?}");
    let point = run_query(
        &mut client,
        &mut reader,
        "EXPLAIN SELECT v FROM gen WHERE id = 1",
    );
    assert!(
        point.iter().any(|row| row[0].contains("Point_Get")),
        "point get over the wire: {point:?}"
    );
    // GROUPING() distinguishes the rollup subtotal.
    run_write(
        &mut client,
        &mut reader,
        "CREATE TABLE wg (g BIGINT, n BIGINT)",
    );
    run_write(&mut client, &mut reader, "INSERT INTO wg VALUES (1,1),(2,2)");
    let grouped = run_query(
        &mut client,
        &mut reader,
        "SELECT g, GROUPING(g), SUM(n) FROM wg GROUP BY g WITH ROLLUP ORDER BY GROUPING(g), g",
    );
    assert_eq!(
        grouped,
        vec![
            vec!["1".to_owned(), "0".to_owned(), "1".to_owned()],
            vec!["2".to_owned(), "0".to_owned(), "2".to_owned()],
            vec!["NULL".to_owned(), "1".to_owned(), "3".to_owned()],
        ],
        "grouping over the wire"
    );
    // A DATETIME column compared with a date string, in the time domain.
    run_write(
        &mut client,
        &mut reader,
        "CREATE TABLE wt (id BIGINT, created DATETIME)",
    );
    run_write(
        &mut client,
        &mut reader,
        "INSERT INTO wt VALUES (1,'2024-12-30 23:59:59'),(2,'2025-01-02 00:00:00')",
    );
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            "SELECT id FROM wt WHERE created <= '2024-12-31'"
        ),
        vec![vec!["1".to_owned()]]
    );
    // Two of the newly unblocked builtins.
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            "SELECT SUBSTRING_INDEX('a.b.c', '.', 2), DATE_FORMAT('2024-06-15', '%Y/%m')"
        ),
        vec![vec!["a.b".to_owned(), "2024/06".to_owned()]]
    );

    // The fourth integration's features, through the socket.
    // Interval arithmetic in projection and WHERE.
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            "SELECT DATE_ADD('2024-01-31 10:20:30', INTERVAL 1 MONTH)"
        ),
        vec![vec!["2024-02-29 10:20:30".to_owned()]],
        "month-end clamp over the wire"
    );
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            "SELECT id FROM wt WHERE created >= DATE_SUB('2025-01-01', INTERVAL 1 DAY)"
        ),
        vec![vec!["2".to_owned()]]
    );
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            "SELECT EXTRACT(YEAR FROM created), TIMESTAMPDIFF(DAY, '2024-01-31', '2024-03-01')              FROM wt WHERE id = 1"
        ),
        vec![vec!["2024".to_owned(), "30".to_owned()]]
    );
    // A correlated scalar subquery in the SELECT list.
    run_write(
        &mut client,
        &mut reader,
        "CREATE TABLE co (id BIGINT PRIMARY KEY, v BIGINT)",
    );
    run_write(
        &mut client,
        &mut reader,
        "INSERT INTO co VALUES (1, 10), (2, 20)",
    );
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            "SELECT t.a, (SELECT COUNT(*) FROM co WHERE co.id = t.a) FROM t ORDER BY t.a LIMIT 2"
        ),
        vec![
            vec!["1".to_owned(), "1".to_owned()],
            vec!["2".to_owned(), "1".to_owned()],
        ]
    );
    // The introspection tables a JDBC driver reads.
    let kcu = run_query(
        &mut client,
        &mut reader,
        "SELECT column_name FROM information_schema.key_column_usage          WHERE table_name = 'gen' AND constraint_name = 'PRIMARY'",
    );
    assert_eq!(kcu, vec![vec!["id".to_owned()]], "key_column_usage over the wire");

    // The fifth integration's query features, through the socket.
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            "SELECT COUNT(DISTINCT g, n) FROM wr"
        ),
        vec![vec!["3".to_owned()]],
        "distinct tuple count over the wire"
    );
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            "SELECT a FROM t WHERE a > ALL (SELECT id FROM co WHERE co.id < t.a) ORDER BY a LIMIT 1"
        ),
        vec![vec!["1".to_owned()]],
        "correlated ALL over the wire (empty inner set is vacuously true)"
    );

    // COM_QUIT ends the connection cleanly.
    write_packet(&mut client, 0, &[0x01]);
    drop(client);
    worker.join().unwrap();
}

/// Go advertises `CLIENT_FOUND_ROWS` and stores the negotiated bit on the
/// session. An UPDATE then reports every successfully matched row, including
/// rows already holding the assigned value; an unchanged duplicate-key UPDATE
/// similarly reports one instead of zero.
#[test]
fn client_found_rows_reports_matched_updates_over_the_mysql_wire() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let store = users();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            node_over(&store).factory().as_ref(),
            &store,
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate_with_capabilities(&mut client, &mut reader, CLIENT_FOUND_ROWS);

    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "CREATE TABLE found_rows_t (id BIGINT PRIMARY KEY, v BIGINT)"
        ),
        0
    );
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "INSERT INTO found_rows_t VALUES (1, 10), (2, 20)"
        ),
        2
    );
    let (affected, info) = run_write_with_info(
        &mut client,
        &mut reader,
        "UPDATE found_rows_t SET v = 10",
    );
    assert_eq!(affected, 2, "one changed row plus one unchanged matched row");
    assert_eq!(info, "Rows matched: 2  Changed: 1  Warnings: 0");
    let (_, set_info) = run_write_with_info(&mut client, &mut reader, "SET sql_mode = ''");
    assert!(set_info.is_empty(), "a later SET must not reuse UPDATE info");
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "INSERT INTO found_rows_t VALUES (1, 10) ON DUPLICATE KEY UPDATE v = VALUES(v)"
        ),
        1,
        "an unchanged duplicate-key update reports its matched row"
    );

    write_packet(&mut client, 0, &[0x01]);
    drop(client);
    assert_eq!(worker.join().unwrap().exit, ConnectionExit::Quit);
}

/// Source: `pkg/planner/core/tests/rewriter.TestVariableRewritter`.
///
/// Go's expression rewriter rejects an explicitly invalid system-variable
/// scope while planning both a text query and a prepared statement. The typed
/// 1238/1193 must survive the session adapter and the connection error writer;
/// a failed PREPARE must not install a statement or count as a success.
#[test]
fn system_variable_scope_errors_reach_text_and_prepare_wire() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let store = users();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            node_over(&store).factory().as_ref(),
            &store,
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader);

    let cases = [
        (
            "SELECT @@session.ddl_slow_threshold",
            1238,
            "Variable 'ddl_slow_threshold' is a GLOBAL variable",
        ),
        (
            "SELECT @@session.tidb_redact_log",
            1193,
            "Unknown system variable 'tidb_redact_log'",
        ),
    ];
    for command_code in [COM_QUERY, COM_STMT_PREPARE] {
        for (sql, code, message) in cases {
            let mut command = vec![command_code];
            command.extend_from_slice(sql.as_bytes());
            write_packet(&mut client, 0, &command);
            reader.set_sequence(1);
            assert_error_packet_exact(
                &reader.read_packet().unwrap(),
                code,
                b"HY000",
                message,
            );
        }
    }

    // A semantic query error is command-local: the event loop remains usable.
    write_packet(&mut client, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0x00, "COM_PING OK");

    write_packet(&mut client, 0, &[0x01]);
    drop(client);
    let report = worker.join().unwrap();
    // Three: the `USE test` every connection now sends to choose a schema (a
    // handshake carrying none leaves `CurrentDB` empty, as in Go), plus the
    // two this test issues.
    assert_eq!(report.commands.text_query_commands, 3);
    assert_eq!(report.commands.stmt_prepare_commands, 2);
    assert_eq!(report.commands.stmt_prepare_successes, 0);
    assert_eq!(report.exit, ConnectionExit::Quit);
}

/// Go `TiDBDriver.OpenCtx` seeds the connection charset variables from the
/// handshake collation id with `session.SetCollation`: a direct write, with
/// no statement and no validation. Collation 8 (`latin1_swedish_ci`) is not
/// one the new collation framework supports, so Go stores `latin1` for the
/// client and results charsets and substitutes `utf8mb4_bin` for the
/// collation, whose hook then makes `character_set_connection` `utf8mb4`.
/// The substitution belongs to `SetCollation` alone; no `SET NAMES`
/// statement produces this state.
///
/// An id outside Go's collation table is Go's `GetCharsetInfoByID` error,
/// which `OpenCtx` returns, so the login is refused with it.
#[test]
fn the_handshake_collation_seeds_the_charset_variables_as_go_set_collation() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let store = users();
        let node = node_over(&store);
        let factory = node.factory().as_ref();
        let mut reports = Vec::new();
        for _ in 0..2 {
            let (stream, peer_addr) = listener.accept().unwrap();
            reports.push(
                serve_mysql_connection(
                    stream,
                    peer_addr,
                    ConnectionCancellation::default(),
                    factory,
                    &store,
                    &worker_tracker,
                    DEFAULT_MAX_ALLOWED_PACKET,
                )
                .unwrap(),
            );
        }
        reports
    });

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    assert_eq!(
        handshake_reply(&mut client, &mut reader, 0, 8)[0],
        0,
        "auth OK"
    );
    assert_eq!(
        run_query(
            &mut client,
            &mut reader,
            "SELECT @@character_set_client, @@character_set_connection, \
             @@character_set_results, @@collation_connection",
        ),
        vec![vec![
            "latin1".to_owned(),
            "utf8mb4".to_owned(),
            "latin1".to_owned(),
            "utf8mb4_bin".to_owned(),
        ]]
    );
    write_packet(&mut client, 0, &[0x01]);
    drop(client);

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    assert_error_packet_exact(
        &handshake_reply(&mut client, &mut reader, 0, 0),
        1105,
        b"HY000",
        "Unknown collation id 0",
    );
    drop(client);

    let reports = worker.join().unwrap();
    // The login itself is not a statement: only the one SELECT counts.
    assert_eq!(reports[0].commands.text_query_commands, 1);
    assert_eq!(reports[0].exit, ConnectionExit::Quit);
    assert_eq!(reports[1].exit, ConnectionExit::SessionRejected);
}

/// Cursor materialization is part of the executing statement's memory
/// budget. With temporary storage disabled, exceeding that budget is 8175
/// and the statement must not retain a half-built cursor.
#[test]
fn cursor_materialization_without_tmp_storage_is_8175_and_never_opens_cursor() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let store = users();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            node_over(&store).factory().as_ref(),
            &store,
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader);

    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "CREATE TABLE cursor_quota (a BIGINT, b BIGINT)"
        ),
        0
    );
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "INSERT INTO cursor_quota VALUES (1,10),(2,20),(3,30),(4,40)"
        ),
        4
    );
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "SET @@global.tidb_mem_oom_action = 'CANCEL'"
        ),
        0
    );
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "SET @@global.tidb_enable_tmp_storage_on_oom = 0"
        ),
        0
    );
    let (statement_id, parameter_count, column_count) = prepare_statement(
        &mut client,
        &mut reader,
        "SELECT a, b FROM cursor_quota",
    );
    assert_eq!((parameter_count, column_count), (0, 2));
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            // Above the read's queued coprocessor response (Go's
            // `copIteratorWorker.checkRespOOM` charges it, and at a quota of
            // 1 Go cancels the read itself, gorun) but below the cursor's
            // materialized rows.
            "SET @@tidb_mem_quota_query = 300"
        ),
        0
    );

    let mut execute = vec![COM_STMT_EXECUTE];
    execute.extend_from_slice(&statement_id.to_le_bytes());
    execute.push(0x01); // CURSOR_TYPE_READ_ONLY
    execute.extend_from_slice(&1_u32.to_le_bytes());
    write_packet(&mut client, 0, &execute);
    reader.set_sequence(1);
    let execute_error = reader.read_packet().unwrap();
    assert_error_packet(&execute_error, 8175, b"HY000");

    let mut fetch = vec![COM_STMT_FETCH];
    fetch.extend_from_slice(&statement_id.to_le_bytes());
    fetch.extend_from_slice(&1_u32.to_le_bytes());
    write_packet(&mut client, 0, &fetch);
    reader.set_sequence(1);
    let fetch_error = reader.read_packet().unwrap();
    assert_error_packet(&fetch_error, 1326, b"24000");

    // The same statement and quota succeed when the one policy difference is
    // restored: cursor spill runs before CANCEL, and FETCH returns the exact
    // rows through the binary protocol.
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            "SET @@global.tidb_enable_tmp_storage_on_oom = 1"
        ),
        0
    );
    let (columns, execute_status) =
        execute_with_cursor(&mut client, &mut reader, statement_id, &[]);
    assert_eq!(columns, 2);
    assert_ne!(execute_status & 0x0040, 0);
    let (rows, fetch_status) = fetch_rows(
        &mut client,
        &mut reader,
        statement_id,
        4,
        &[0x08, 0x08],
    );
    assert_eq!(
        rows,
        vec![
            vec!["1".to_owned(), "10".to_owned()],
            vec!["2".to_owned(), "20".to_owned()],
            vec!["3".to_owned(), "30".to_owned()],
            vec!["4".to_owned(), "40".to_owned()],
        ]
    );
    assert_eq!(fetch_status & 0x0040, 0);
    assert_ne!(fetch_status & 0x0080, 0);

    write_packet(&mut client, 0, &[0x01]);
    drop(client);
    assert_eq!(worker.join().unwrap().exit, ConnectionExit::Quit);
}


/// `SHOW PROCESSLIST` over the wire: the connection sees ITSELF in the list a
/// real server keeps, with the identity the handshake authenticated and the
/// statement it is running -- the SHOW.
///
/// Captured from TiDB (testkit session): columns
/// `Id User Host db Command Time State Info`, one row per live connection,
/// with `Info` the running statement and `State` `autocommit`.
///
/// `KILL <unknown id>` is captured as a silent no-op that answers OK: Go's
/// `executeKillStmt` reaches `KillWithNormalCloseMsg`, which ignores an id it
/// does not hold. (1094 `Unknown thread id` is EXPLAIN FOR CONNECTION's
/// error, not KILL's.)
#[test]
fn mysql_client_reads_the_process_list_and_kills_by_id() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        // The store's account table and the factory's are ONE table: an
        // authenticated identity must be a row the session's `SHOW GRANTS`
        // can find.
        let store = users();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            node_over(&store).factory().as_ref(),
            &store,
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader);

    let rows = run_query(&mut client, &mut reader, "SHOW PROCESSLIST");
    assert_eq!(rows.len(), 1, "one live connection: {rows:?}");
    let row = &rows[0];
    assert!(
        row[0].parse::<u64>().unwrap() > 0,
        "the server assigned a connection id: {row:?}"
    );
    assert_eq!(row[1], "alice");
    assert!(row[2].starts_with("127.0.0.1:"), "client address: {row:?}");
    assert_eq!(row[3], "test");
    assert_eq!(row[4], "Query");
    assert_eq!(row[6], "autocommit");
    assert_eq!(row[7], "SHOW PROCESSLIST");
    let connection_id: u64 = row[0].parse().unwrap();
    let (gcid, truncated) = parse_conn_id(connection_id).unwrap();
    assert!(!truncated);
    assert_eq!(gcid.server_id, 1);
    assert!(!gcid.is_64bits);

    // FULL differs only in that Info is not truncated.
    let full = run_query(&mut client, &mut reader, "SHOW FULL PROCESSLIST");
    assert_eq!(full[0][7], "SHOW FULL PROCESSLIST");

    // An id this server does not hold: OK, nothing killed, connection usable.
    assert_eq!(run_write(&mut client, &mut reader, "KILL 999999"), 0);
    assert_eq!(
        run_write(&mut client, &mut reader, "KILL CONNECTION 999999"),
        0
    );
    // Killing one's OWN running query is legal and leaves the connection
    // usable, as it is in Go.
    assert_eq!(
        run_write(
            &mut client,
            &mut reader,
            &format!("KILL QUERY {connection_id}")
        ),
        0
    );
    assert_eq!(
        run_query(&mut client, &mut reader, "SELECT 1"),
        vec![vec!["1".to_owned()]]
    );

    // COM_QUIT ends the connection cleanly, and the row leaves the list with
    // it.
    write_packet(&mut client, 0, &[0x01]);
    drop(client);
    worker.join().unwrap();
}


/// sysbench's `simple_ranges` over the binary protocol, whose packet SEQUENCE
/// is what a client frames its next command against.
///
/// A MySQL client reads a `COM_STMT_EXECUTE` answer against the column count
/// the PREPARE reported: a prepare that says zero columns and an execute that
/// then sends a result set leave the client one result set behind, and its
/// next command fails with `2014 Commands out of sync`. That is a framing
/// contract between two packets sent seconds apart, so both ends of it are
/// asserted here rather than only the rows: the prepare-OK's column count,
/// the count the execute's own header repeats, and the exact packet run --
/// one header, one definition per column, one row packet per row, one
/// terminator, nothing after it.
///
/// The statement is sysbench `oltp_read_only`'s, over sysbench's own table
/// shape and inside its transaction, because the shape is what decides the
/// access path underneath.
#[test]
fn prepared_handle_range_frames_its_binary_result_set() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let store = users();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            node_over(&store).factory().as_ref(),
            &store,
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader);
    run_write(
        &mut client,
        &mut reader,
        "CREATE TABLE sbtest1 (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL DEFAULT 0, \
         c CHAR(120) NOT NULL DEFAULT '', pad CHAR(60) NOT NULL DEFAULT '', INDEX k_1(k))",
    );
    for block in 0..10 {
        let values: Vec<String> = (0..100)
            .map(|offset| {
                let id = block * 100 + offset + 1;
                format!("({id},{id},'c{id}','p{id}')")
            })
            .collect();
        run_write(
            &mut client,
            &mut reader,
            &format!("INSERT INTO sbtest1 VALUES {}", values.join(",")),
        );
    }
    // sysbench prepares every statement of the transaction up front, and the
    // marker count is the only thing it binds later. A prepare plans with the
    // markers bound to NULL, which is the shape that first met the empty
    // handle range.
    let (point, point_parameters, point_columns) =
        prepare_statement(&mut client, &mut reader, "SELECT c FROM sbtest1 WHERE id = ?");
    let (simple, simple_parameters, simple_columns) = prepare_statement(
        &mut client,
        &mut reader,
        "SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?",
    );
    let (sum, _, sum_columns) = prepare_statement(
        &mut client,
        &mut reader,
        "SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?",
    );
    assert_eq!((point_parameters, point_columns), (1, 1));
    assert_eq!(
        (simple_parameters, simple_columns),
        (2, 1),
        "the range select reports its one result column at PREPARE; zero here \
         is what a client answers with `Commands out of sync` at EXECUTE"
    );
    assert_eq!(sum_columns, 1);

    run_transaction_control(&mut client, &mut reader, "BEGIN");
    for _ in 0..10 {
        assert_eq!(
            execute_statement(&mut client, &mut reader, point, &[500]),
            vec![vec!["c500".to_owned()]]
        );
    }

    // The sequence, packet by packet: a header repeating the count the
    // PREPARE promised, that many definitions, one row packet per row, and
    // one terminator with nothing behind it.
    let mut expected = vec!["header:1".to_owned()];
    expected.push("column".to_owned());
    expected.extend(std::iter::repeat_n("row".to_owned(), 100));
    expected.push("terminator".to_owned());
    assert_eq!(
        execute_statement_packet_tags(&mut client, &mut reader, simple, &[100, 199]),
        expected
    );
    // The same connection keeps working afterwards, which is the property
    // `Commands out of sync` denies.
    assert_eq!(
        execute_statement(&mut client, &mut reader, sum, &[100, 199]),
        vec![vec!["14950".to_owned()]]
    );
    assert_eq!(
        execute_statement(&mut client, &mut reader, simple, &[100, 199])[0],
        vec!["c100".to_owned()]
    );
    // The SECOND execute of the same statement must read the range its OWN
    // parameters imply. A range accepted by the scan and then kept there would
    // answer this from the first execute's bounds, silently and without any
    // framing error to announce it.
    assert_eq!(
        execute_statement(&mut client, &mut reader, simple, &[1, 3]),
        vec![
            vec!["c1".to_owned()],
            vec!["c2".to_owned()],
            vec!["c3".to_owned()]
        ]
    );
    run_transaction_control(&mut client, &mut reader, "COMMIT");

    // A range that admits nothing is still a one-column result set with no
    // rows -- never an error, and never a bare OK.
    assert_eq!(
        execute_statement_packet_tags(&mut client, &mut reader, simple, &[199, 100]),
        vec![
            "header:1".to_owned(),
            "column".to_owned(),
            "terminator".to_owned()
        ]
    );

    write_packet(&mut client, 0, &[0x01]);
    drop(client);
    worker.join().unwrap();
}

/// Two connections on one server: the second sees BOTH rows in the process
/// list, and `KILL <id>` ends the first one -- Go's `KILL` closes the target
/// session's connection.
///
/// Deterministic: the kill shuts the target's socket down, so the thread
/// serving it returns without the target's client having to send anything.
#[test]
fn kill_connection_ends_the_targeted_peer() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    // Both connections are served by ONE factory: that is what gives them one
    // process list, as one TiDB instance has one session manager.
    let store = Arc::new(users());
    let node = node_over(&store);
    let factory = Arc::clone(node.factory());
    let acceptor = std::thread::spawn(move || {
        let mut workers = Vec::new();
        for _ in 0..2 {
            let (stream, peer_addr) = listener.accept().unwrap();
            let tracker = Arc::clone(&tracker);
            let factory = Arc::clone(&factory);
            let store = Arc::clone(&store);
            workers.push(std::thread::spawn(move || {
                serve_mysql_connection(
                    stream,
                    peer_addr,
                    ConnectionCancellation::default(),
                    factory.as_ref(),
                    store.as_ref(),
                    &tracker,
                    DEFAULT_MAX_ALLOWED_PACKET,
                )
                .unwrap()
            }));
        }
        workers
    });

    // The victim connects first and runs one statement, so it is registered
    // and idle between commands when the kill arrives.
    let mut victim = TcpStream::connect(address).unwrap();
    let victim_read = victim.try_clone().unwrap();
    let mut victim_reader = PacketReader::new(victim_read);
    authenticate(&mut victim, &mut victim_reader);
    assert_eq!(
        run_query(&mut victim, &mut victim_reader, "SELECT 1"),
        vec![vec!["1".to_owned()]]
    );

    let mut killer = TcpStream::connect(address).unwrap();
    let killer_read = killer.try_clone().unwrap();
    let mut killer_reader = PacketReader::new(killer_read);
    authenticate(&mut killer, &mut killer_reader);

    let rows = run_query(&mut killer, &mut killer_reader, "SHOW PROCESSLIST");
    assert_eq!(rows.len(), 2, "both connections are listed: {rows:?}");
    // The killer is the row running this SHOW; the idle victim is `Sleep`
    // with a NULL Info, which is how Go reports a connection between
    // commands.
    let victim_row = rows
        .iter()
        .find(|row| row[4] == "Sleep")
        .unwrap_or_else(|| panic!("an idle peer row: {rows:?}"));
    assert_eq!(victim_row[7], "NULL");
    let victim_id: u64 = victim_row[0].parse().unwrap();

    assert_eq!(
        run_write(&mut killer, &mut killer_reader, &format!("KILL {victim_id}")),
        0
    );

    // The killer ends normally; the victim's own serving thread reports that
    // a KILL ended it.
    write_packet(&mut killer, 0, &[0x01]);
    drop(killer);
    let workers = acceptor.join().unwrap();
    let exits: Vec<ConnectionExit> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().exit)
        .collect();
    assert!(
        exits.contains(&ConnectionExit::Killed),
        "the killed connection reports its kill: {exits:?}"
    );
    assert!(exits.contains(&ConnectionExit::Quit), "{exits:?}");
    drop(victim);
}

/// Sends COM_STMT_EXECUTE for a parameterless statement and returns the OK
/// packet's `SERVER_STATUS_IN_TRANS` bit, the way [`run_transaction_control`]
/// does for the text protocol.
fn execute_transaction_control(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    statement_id: u32,
) -> bool {
    let mut command = vec![COM_STMT_EXECUTE];
    command.extend_from_slice(&statement_id.to_le_bytes());
    command.push(0); // CURSOR_TYPE_NO_CURSOR
    command.extend_from_slice(&1u32.to_le_bytes()); // iteration count
    write_packet(client, 0, &command);
    reader.set_sequence(1);
    let packet = reader.read_packet().unwrap();
    assert_eq!(
        packet[0], 0x00,
        "prepared transaction control answers with an OK packet: {packet:?}"
    );
    u16::from_le_bytes([packet[3], packet[4]]) & 0x0001 != 0
}

/// A prepared `BEGIN`/`ROLLBACK` is the text protocol's own transaction
/// control: the same OK packet, carrying the same `SERVER_STATUS_IN_TRANS`
/// flag, and the same effect on the connection's transaction.
///
/// The flag is the client-visible half, and it is not cosmetic: a driver that
/// reads it back as clear after a prepared `BEGIN` believes it is in
/// autocommit and will not send a `COMMIT` at all.
#[test]
fn prepared_transaction_control_answers_like_the_text_protocol() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let store = users();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            node_over(&store).factory().as_ref(),
            &store,
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader);
    assert_eq!(
        run_write(&mut client, &mut reader, "CREATE TABLE t (a BIGINT)"),
        0
    );

    // PREPARE reports transaction control as it is: no markers to bind and no
    // result columns for a client to frame its EXECUTE answer against.
    let (begin_id, begin_params, begin_columns) =
        prepare_statement(&mut client, &mut reader, "BEGIN");
    assert_eq!((begin_params, begin_columns), (0, 0));
    let (rollback_id, _, _) = prepare_statement(&mut client, &mut reader, "ROLLBACK");

    assert!(
        execute_transaction_control(&mut client, &mut reader, begin_id),
        "a prepared BEGIN must report SERVER_STATUS_IN_TRANS"
    );
    assert_eq!(
        run_write(&mut client, &mut reader, "INSERT INTO t VALUES (1)"),
        1
    );
    assert!(
        !execute_transaction_control(&mut client, &mut reader, rollback_id),
        "a prepared ROLLBACK must clear SERVER_STATUS_IN_TRANS"
    );
    // And it really discarded: the row the transaction staged is gone.
    assert_eq!(
        run_query(&mut client, &mut reader, "SELECT COUNT(*) FROM t"),
        vec![vec!["0".to_owned()]]
    );

    write_packet(&mut client, 0, &[0x01]);
    drop(client);
    assert_eq!(worker.join().unwrap().exit, ConnectionExit::Quit);
}

/// Sends one COM_QUERY expected to answer with an OK packet and returns the
/// packet's warning count -- the field Go fills from `ctx.WarningCount()` in
/// `writeOkWith`.
///
/// OK payload: header, length-encoded affected_rows, length-encoded
/// last_insert_id, status flags, warning count.
fn run_write_warning_count(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    sql: &str,
) -> u16 {
    let mut command = vec![COM_QUERY];
    command.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &command);
    reader.set_sequence(1);
    let packet = reader.read_packet().unwrap();
    assert_eq!(
        packet[0], 0x00,
        "a write answers with an OK packet: {packet:?}"
    );
    assert!(packet[1] < 0xfb, "test writes report small counts");
    assert!(packet[2] < 0xfb, "test ids are small");
    u16::from_le_bytes([packet[5], packet[6]])
}

/// Sends one COM_QUERY result set and returns `(rows, terminal warning
/// count)`. Under deprecate-EOF the terminal packet is an OK packet with an
/// EOF header, so its warning count sits where an OK packet's does.
fn run_query_warning_count(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    sql: &str,
) -> (Vec<Vec<String>>, u16) {
    let mut command = vec![COM_QUERY];
    command.extend_from_slice(sql.as_bytes());
    write_packet(client, 0, &command);
    reader.set_sequence(1);

    let first = reader.read_packet().unwrap();
    assert_ne!(first[0], 0xff, "query errored: {first:?}");
    let column_count = usize::from(first[0]);
    for _ in 0..column_count {
        reader.read_packet().unwrap();
    }
    let mut rows = Vec::new();
    loop {
        let packet = reader.read_packet().unwrap();
        if packet[0] == 0xfe && packet.len() < 9 + 4 {
            return (rows, u16::from_le_bytes([packet[5], packet[6]]));
        }
        let mut remaining = packet.as_slice();
        let mut row = Vec::new();
        for _ in 0..column_count {
            row.push(read_text_value(&mut remaining));
        }
        rows.push(row);
    }
}

/// The OK/EOF packet's warning count is the ONLY way a driver learns that a
/// statement warned: `SHOW WARNINGS` is a separate query a client has to know
/// to send, while JDBC's `SQLWarning` chain, the `mysql` client's
/// `1 warning` line, and every connector's `warning_count` all come from this
/// field.
///
/// Three things are pinned here, and a `SHOW WARNINGS` assertion can prove
/// none of them:
///
///  * a statement that warns reports its count (Go `writeOkWith` reading
///    `ctx.WarningCount()`);
///  * `SHOW WARNINGS` reports the warning as a ROW and a count of ZERO, which
///    is Go's `StatementContext.WarningCount` returning 0 while
///    `InShowWarning` is set -- the count deliberately does NOT inherit the
///    buffer the rows do;
///  * the NEXT statement reports 0. A count read from the wrong buffer, or
///    after that buffer reset, is wrong for the FOLLOWING statement rather
///    than obviously broken, so only the sequence pins it.
///
/// `SET @@group_concat_max_len=1` is Go's `checkUInt64SystemVar` clamping to
/// the minimum and warning 1292 about the value as written. It also answers
/// through the front end's own `SET` door rather than `Session::run`, which is
/// exactly where a per-statement count goes stale.
#[test]
fn the_ok_packet_reports_the_statements_warning_count() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let store = users();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            node_over(&store).factory().as_ref(),
            &store,
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader);

    // A statement with no warnings reports none.
    assert_eq!(
        run_write_warning_count(&mut client, &mut reader, "CREATE TABLE t (a BIGINT)"),
        0
    );
    // A statement with one warning reports it on the wire.
    assert_eq!(
        run_write_warning_count(&mut client, &mut reader, "SET @@group_concat_max_len=1"),
        1,
        "the OK packet must carry the warning count, or no driver ever sees it"
    );
    // The buffer still holds it -- the channel this bug hid behind.
    let (rows, show_warnings_count) =
        run_query_warning_count(&mut client, &mut reader, "SHOW WARNINGS");
    assert_eq!(
        rows,
        vec![vec![
            "Warning".to_owned(),
            "1292".to_owned(),
            "Truncated incorrect group_concat_max_len value: '1'".to_owned(),
        ]]
    );
    assert_eq!(
        show_warnings_count, 0,
        "Go reports 0 while InShowWarning is set, however many rows it returns"
    );
    // And the next statement starts over. This is the ordering the bug hides
    // in: a count taken from the previous statement's buffer looks plausible.
    let (rows, count) = run_query_warning_count(&mut client, &mut reader, "SELECT 1");
    assert_eq!(rows, vec![vec!["1".to_owned()]]);
    assert_eq!(count, 0, "a statement that did not warn reports 0");
    // Two warning statements in a row report their OWN counts, not a running
    // total: the second is 1, not 2.
    assert_eq!(
        run_write_warning_count(&mut client, &mut reader, "SET @@group_concat_max_len=1"),
        1
    );
    assert_eq!(
        run_write_warning_count(&mut client, &mut reader, "SET @@group_concat_max_len=1"),
        1,
        "the buffer resets per statement, so the count does not accumulate"
    );
    // Two warnings in ONE statement report 2, so the field carries a count and
    // not a flag. Captured from TiDB over a mock-backed session:
    // `set @@group_concat_max_len=1, @@tidb_session_alias='abc  '` ->
    // `WarningCount() == 2`.
    assert_eq!(
        run_write_warning_count(
            &mut client,
            &mut reader,
            "SET @@group_concat_max_len=1, @@tidb_session_alias='abc  '",
        ),
        2
    );

    write_packet(&mut client, 0, &[0x01]);
    drop(client);
    assert_eq!(worker.join().unwrap().exit, ConnectionExit::Quit);
}

#[test]
fn session_plan_cache_binary_close_uses_session_retention_policy() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let store = users();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            node_over(&store).factory().as_ref(),
            &store,
            &Arc::new(ConnectionTracker::default()),
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });
    let mut client = TcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(15)))
        .unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader);
    run_write(
        &mut client,
        &mut reader,
        "CREATE TABLE close_cache (id INT PRIMARY KEY, v INT)",
    );
    run_write(
        &mut client,
        &mut reader,
        "INSERT INTO close_cache VALUES (1,10),(2,20)",
    );
    for (setting, expected_hit) in [("OFF", "0"), ("ON", "1")] {
        run_write(
            &mut client,
            &mut reader,
            &format!("SET tidb_ignore_prepared_cache_close_stmt={setting}"),
        );
        run_write(&mut client, &mut reader, "ADMIN FLUSH SESSION PLAN_CACHE");
        let (first, _, _) = prepare_statement(
            &mut client,
            &mut reader,
            "SELECT v FROM close_cache WHERE id>=?",
        );
        let (second, _, _) = prepare_statement(
            &mut client,
            &mut reader,
            "SELECT v FROM close_cache WHERE id>=?",
        );
        for (id, hit) in [(first, "0"), (second, "1")] {
            assert_eq!(
                execute_statement(&mut client, &mut reader, id, &[2]),
                vec![vec!["20".to_owned()]]
            );
            assert_eq!(
                run_query(&mut client, &mut reader, "SELECT @@last_plan_from_cache"),
                vec![vec![hit.to_owned()]]
            );
        }
        let mut close = vec![COM_STMT_CLOSE];
        close.extend_from_slice(&first.to_le_bytes());
        write_packet(&mut client, 0, &close);
        assert_eq!(
            execute_statement(&mut client, &mut reader, second, &[1]),
            vec![vec!["10".to_owned()], vec!["20".to_owned()]]
        );
        assert_eq!(
            run_query(&mut client, &mut reader, "SELECT @@last_plan_from_cache"),
            vec![vec![expected_hit.to_owned()]]
        );
    }
    write_packet(&mut client, 0, &[0x01]);
    drop(client);
    worker.join().unwrap();
}

#[test]
fn session_migration_moves_text_and_binary_prepared_handles_over_mysql() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let worker = std::thread::spawn(move || {
        let store = users();
        let node = node_over(&store);
        let factory = node.factory().as_ref();
        for _ in 0..2 {
            let (stream, peer) = listener.accept().unwrap();
            serve_mysql_connection(
                stream,
                peer,
                ConnectionCancellation::default(),
                factory,
                &store,
                &Arc::new(ConnectionTracker::default()),
                DEFAULT_MAX_ALLOWED_PACKET,
            )
            .unwrap();
        }
    });
    let mut source = TcpStream::connect(address).unwrap();
    source
        .set_read_timeout(Some(std::time::Duration::from_secs(15)))
        .unwrap();
    let mut reader = PacketReader::new(source.try_clone().unwrap());
    authenticate(&mut source, &mut reader);
    run_write(&mut source, &mut reader, "SET @migration_value=42");
    run_write(
        &mut source,
        &mut reader,
        "PREPARE text_stmt FROM 'SELECT ?'",
    );
    let (id, _, _) = prepare_statement(&mut source, &mut reader, "SELECT ?");
    assert_eq!(id, 2, "SQL and binary handles share the same allocator");
    assert_eq!(
        execute_statement(&mut source, &mut reader, id, &[42]),
        vec![vec!["42".to_owned()]]
    );
    run_write(&mut source, &mut reader, "PREPARE migration_export FROM 'SHOW SESSION_STATES'");
    let state = run_query(&mut source, &mut reader, "EXECUTE migration_export")[0][0].clone();
    let json: serde_json::Value = serde_json::from_str(&state).unwrap();
    assert_eq!(json["prepared-stmts"][id.to_string()]["types"], "CAA=");
    source.shutdown(std::net::Shutdown::Both).unwrap();
    drop(reader);
    let mut target = TcpStream::connect(address).unwrap();
    target
        .set_read_timeout(Some(std::time::Duration::from_secs(15)))
        .unwrap();
    let mut reader = PacketReader::new(target.try_clone().unwrap());
    authenticate(&mut target, &mut reader);
    run_write(
        &mut target,
        &mut reader,
        &format!(
            "SET SESSION_STATES '{}'",
            state.replace('\\', "\\\\").replace('\'', "''")
        ),
    );
    assert_eq!(
        run_query(
            &mut target,
            &mut reader,
            "EXECUTE text_stmt USING @migration_value"
        ),
        vec![vec!["42".to_owned()]]
    );
    // Go restores remembered parameter types, so the destination can execute
    // without the client resending a type vector.
    let mut execute = vec![COM_STMT_EXECUTE];
    execute.extend_from_slice(&id.to_le_bytes());
    execute.push(0);
    execute.extend_from_slice(&1u32.to_le_bytes());
    execute.extend_from_slice(&[0, 0]); // non-NULL, reuse types
    execute.extend_from_slice(&44i64.to_le_bytes());
    write_packet(&mut target, 0, &execute);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap(), vec![1]);
    reader.read_packet().unwrap(); // column definition
    let row = reader.read_packet().unwrap();
    assert_eq!(i64::from_le_bytes(row[2..10].try_into().unwrap()), 44);
    assert_eq!(reader.read_packet().unwrap()[0], 0xfe);
    assert_eq!(
        execute_statement(&mut target, &mut reader, id, &[43]),
        vec![vec!["43".to_owned()]]
    );
    let (next, _, _) = prepare_statement(&mut target, &mut reader, "SELECT 1");
    assert_eq!(next, 4);
    let (export, _, _) = prepare_statement(&mut target, &mut reader, "SHOW SESSION_STATES");
    let exported = execute_statement(&mut target, &mut reader, export, &[]);
    let exported: serde_json::Value = serde_json::from_str(&exported[0][0]).unwrap();
    assert_eq!(exported["prepared-stmts"][id.to_string()]["types"], "CAA=");
    let mut long_data = vec![tidb_server::command::COM_STMT_SEND_LONG_DATA];
    long_data.extend_from_slice(&id.to_le_bytes());
    long_data.extend_from_slice(&0u16.to_le_bytes());
    write_packet(&mut target, 0, &long_data);
    let mut show = vec![COM_QUERY];
    show.extend_from_slice(b"SHOW SESSION_STATES");
    write_packet(&mut target, 0, &show);
    reader.set_sequence(1);
    assert_error_packet_exact(
        &reader.read_packet().unwrap(),
        8146,
        b"HY000",
        "cannot migrate the current session: prepared statements have bound params",
    );
    let mut reset = vec![COM_STMT_RESET];
    reset.extend_from_slice(&id.to_le_bytes());
    write_packet(&mut target, 0, &reset);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    execute_with_cursor(&mut target, &mut reader, id, &[45]);
    write_packet(&mut target, 0, &show);
    reader.set_sequence(1);
    assert_error_packet_exact(&reader.read_packet().unwrap(), 8146, b"HY000", "cannot migrate the current session: prepared statements have unfetched rows");
    target.shutdown(std::net::Shutdown::Both).unwrap();
    worker.join().unwrap();
}

/// Source: `tidbResultSet.Columns()` (`ConvertColumnInfo` over
/// `recordSet.Fields()`) and `dumpTextRow`.
///
/// A table column's definition names its schema, visible and original table
/// and original column, so a DOUBLE(7,4) column of a table prints at full
/// precision; a computed column (IFNULL keeps DOUBLE(7,4)) has no table and an
/// empty `org_name`, and prints at its fixed decimals.
#[test]
fn query_column_definitions_carry_result_field_names() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let store = users();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            node_over(&store).factory().as_ref(),
            &store,
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader);
    run_write(
        &mut client,
        &mut reader,
        "CREATE TABLE fields_t (id INT PRIMARY KEY, d DOUBLE(7,4))",
    );
    run_write(
        &mut client,
        &mut reader,
        "INSERT INTO fields_t VALUES (1, 123.45)",
    );

    let mut command = vec![COM_QUERY];
    command.extend_from_slice(b"SELECT d, IFNULL(d, 0) AS e FROM fields_t AS x");
    write_packet(&mut client, 0, &command);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap(), [2]);
    let definitions = (0..2)
        .map(|_| {
            let packet = reader.read_packet().unwrap();
            let mut remaining = packet.as_slice();
            // catalog, schema, table, org_table, name, org_name.
            (0..6)
                .map(|_| String::from_utf8(read_length_encoded_string(&mut remaining)).unwrap())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        definitions,
        [
            ["def", "test", "x", "fields_t", "d", "d"],
            ["def", "", "", "", "e", ""],
        ]
    );
    let packet = reader.read_packet().unwrap();
    let mut remaining = packet.as_slice();
    assert_eq!(
        [
            read_text_value(&mut remaining),
            read_text_value(&mut remaining)
        ],
        ["123.45", "123.4500"]
    );
    let terminal = reader.read_packet().unwrap();
    assert_eq!(terminal[0], 0xfe);

    write_packet(&mut client, 0, &[0x01]);
    drop(client);
    assert_eq!(worker.join().unwrap().exit, ConnectionExit::Quit);
}

/// Sends one COM_QUERY of raw bytes and returns the result's column names and
/// cells exactly as the wire carries them.
fn run_query_bytes(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    sql: &[u8],
) -> (Vec<Vec<u8>>, Vec<Vec<Vec<u8>>>) {
    let mut command = vec![COM_QUERY];
    command.extend_from_slice(sql);
    write_packet(client, 0, &command);
    reader.set_sequence(1);
    let first = reader.read_packet().unwrap();
    assert_ne!(first[0], 0xff, "query errored: {first:?}");
    let column_count = usize::from(first[0]);
    let names = (0..column_count)
        .map(|_| {
            let packet = reader.read_packet().unwrap();
            let mut remaining = packet.as_slice();
            // catalog, schema, table, org_table, then name.
            for _ in 0..4 {
                read_length_encoded_string(&mut remaining);
            }
            read_length_encoded_string(&mut remaining)
        })
        .collect();
    let mut rows = Vec::new();
    loop {
        let packet = reader.read_packet().unwrap();
        if packet[0] == 0xfe && packet.len() < 9 + 4 {
            break;
        }
        let mut remaining = packet.as_slice();
        rows.push(
            (0..column_count)
                .map(|_| read_length_encoded_string(&mut remaining))
                .collect(),
        );
    }
    (names, rows)
}

/// Go scans a latin1/binary/utf8mb4 client's COM_QUERY bytes raw and its
/// `ResultEncoder` passes latin1 and binary results through unchanged
/// (`encodingLatin1.Transform` returns its source). Captured against TiDB with
/// the stock `mysql` client:
///
/// * `--default-character-set=latin1` (handshake collation 8: client and
///   results latin1, connection utf8mb4): a UTF-8 terminal's `└─é` is
///   `E29494E29480C3A9` and reads back as the same bytes, a raw `0xE9` is
///   `?` (`3F`), the column of `HEX('\xE9')` is `HEX(0xe9)`, and EXPLAIN's
///   tree glyphs go out as their UTF-8 bytes, not `??`;
/// * after `SET NAMES latin1`, `HEX('\xE9')` is `E9`;
/// * `--default-character-set=gbk` (collation 28): `HEX('\xC4\xE3')` is
///   `C4E3`, and the cell reads back as the same GBK bytes;
/// * `--default-character-set=binary` (collation 63): `HEX('\xE9')` is `E9`.
#[test]
fn client_charset_bytes_reach_storage_and_results_like_go() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let worker = std::thread::spawn(move || {
        let store = users();
        let node = node_over(&store);
        let factory = node.factory().as_ref();
        let mut reports = Vec::new();
        for _ in 0..3 {
            let (stream, peer_addr) = listener.accept().unwrap();
            reports.push(
                serve_mysql_connection(
                    stream,
                    peer_addr,
                    ConnectionCancellation::default(),
                    factory,
                    &store,
                    &worker_tracker,
                    DEFAULT_MAX_ALLOWED_PACKET,
                )
                .unwrap(),
            );
        }
        reports
    });
    let glyphs = "\u{2514}\u{2500}\u{e9}".as_bytes();

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    assert_eq!(
        handshake_reply(&mut client, &mut reader, 0, 8)[0],
        0,
        "auth OK"
    );
    let (names, rows) = run_query_bytes(
        &mut client,
        &mut reader,
        &[b"SELECT HEX('", glyphs, b"'), HEX('\xe9'), '", glyphs, b"'"].concat(),
    );
    assert_eq!(
        names,
        [
            [b"HEX('", glyphs, b"')"].concat(),
            b"HEX(0xe9)".to_vec(),
            glyphs.to_vec(),
        ]
    );
    assert_eq!(
        rows,
        [[
            b"E29494E29480C3A9".to_vec(),
            b"3F".to_vec(),
            glyphs.to_vec()
        ]]
    );
    run_write(&mut client, &mut reader, "USE test");
    run_write(
        &mut client,
        &mut reader,
        "CREATE TABLE glyphs (id INT PRIMARY KEY, v INT)",
    );
    let (_, plan) = run_query_bytes(
        &mut client,
        &mut reader,
        b"EXPLAIN SELECT v FROM glyphs WHERE id < 3",
    );
    assert!(
        plan.iter()
            .any(|row| row[0].starts_with("\u{2514}\u{2500}".as_bytes())),
        "{plan:?}"
    );
    run_write(&mut client, &mut reader, "SET NAMES latin1");
    let (_, rows) = run_query_bytes(&mut client, &mut reader, b"SELECT HEX('\xe9'), '\xe9'");
    assert_eq!(rows, [[b"E9".to_vec(), b"\xe9".to_vec()]]);
    write_packet(&mut client, 0, &[0x01]);
    drop(client);

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    assert_eq!(
        handshake_reply(&mut client, &mut reader, 0, 28)[0],
        0,
        "auth OK"
    );
    let (_, rows) = run_query_bytes(
        &mut client,
        &mut reader,
        b"SELECT HEX('\xc4\xe3'), '\xc4\xe3'",
    );
    assert_eq!(rows, [[b"C4E3".to_vec(), b"\xc4\xe3".to_vec()]]);
    write_packet(&mut client, 0, &[0x01]);
    drop(client);

    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    assert_eq!(
        handshake_reply(&mut client, &mut reader, 0, 63)[0],
        0,
        "auth OK"
    );
    let (_, rows) = run_query_bytes(&mut client, &mut reader, b"SELECT HEX('\xe9'), '\xe9'");
    assert_eq!(rows, [[b"E9".to_vec(), b"\xe9".to_vec()]]);
    write_packet(&mut client, 0, &[0x01]);
    drop(client);

    let reports = worker.join().unwrap();
    assert!(reports
        .iter()
        .all(|report| report.exit == ConnectionExit::Quit));
}

/// The cursors the node's sessions track, as Go `ReportMinStartTS` walks
/// them through `ProcessInfo.CursorTracker`.
fn tracked_cursor_start_ts(processes: &tidb_sessmgr::ProcessRegistry) -> Vec<u64> {
    let mut start_ts = Vec::new();
    for process in tidb_sessmgr::processinfo::SessionManager::show_process_list(processes) {
        if let Some(tracker) = &process.cursor_tracker {
            tracker.range_cursor(|cursor| {
                start_ts.push(cursor.state().start_ts);
                true
            });
        }
    }
    start_ts
}

/// [`tracked_cursor_start_ts`] once the previous command has finished: this
/// server writes every packet straight to the socket, so the client can read
/// a FETCH's last EOF before the command drops the exhausted cursor (Go
/// buffers the EOF and flushes after `stmt.Reset()`). A round trip orders the
/// read after it.
fn settled_cursor_start_ts(
    client: &mut TcpStream,
    reader: &mut PacketReader<TcpStream>,
    processes: &tidb_sessmgr::ProcessRegistry,
) -> Vec<u64> {
    run_query(client, reader, "SELECT 1");
    tracked_cursor_start_ts(processes)
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

/// Go `tidb_enable_lazy_cursor_fetch`: a cursor-mode execute of a detachable
/// read-only autocommit query detaches its record set (`executeWithLazyCursor`
/// -> `execStmtResult.TryDetach`), the session tracks a cursor at the
/// statement's start TS until FETCH drains it, and the rows come from the
/// statement's snapshot even after another statement ran on the session.
///
/// The wire values are Go's, captured with a raw-protocol client over
/// `servertestkit` (zzprobe_o09 `TestLazyCursorWire`, lazy ON): execute
/// status `0x0042`; `select id from t1` FETCH 2 -> `[1,2]` `0x0042`, FETCH
/// 10 -> `[3,4,5]` `0x0082`; an `INSERT` of `(6,60)` between the execute and
/// the FETCH still reads `[1,2,3,4,5]`; `limit 3` -> `[1,2,3]`; inside
/// `BEGIN` the execute status is `0x0043` and the FETCH `0x0083`. The cursor
/// bookkeeping is Go's `TestCursorWillBeClosed` / `TestDetachError`
/// (`pkg/executor/staticrecordset/integration_test.go`).
/// Go's `clientConn.writeChunks` draws the result chunk from the session's
/// allocator (`rs.NewChunk(sessVars.GetChunkAllocator())`), whose columns a
/// later statement's write loop reuses at their grown capacity, and a covering
/// `IndexReader` returns its `OutputColumns` with no projection chunk above
/// it (`dagReq.OutputOffsets`). Go reads the target's operator memory through
/// the target session's current statement (`p.SCtx()`). Captured from a Go
/// tidb-server over the wire.
#[test]
fn explain_for_connection_charges_reused_chunks_like_go() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let store = users();
    let node = node_over(&store);
    let worker_node = Arc::clone(&node);
    std::thread::spawn(move || {
        for _ in 0..2 {
            let (stream, peer_addr) = listener.accept().unwrap();
            let node = Arc::clone(&worker_node);
            let store = store.clone();
            let tracker = Arc::clone(&tracker);
            std::thread::spawn(move || {
                let _ = serve_mysql_connection(
                    stream,
                    peer_addr,
                    ConnectionCancellation::default(),
                    node.factory().as_ref(),
                    &store,
                    &tracker,
                    DEFAULT_MAX_ALLOWED_PACKET,
                );
            });
        }
    });
    let connect = || {
        let mut client = TcpStream::connect(address).unwrap();
        let mut reader = PacketReader::new(client.try_clone().unwrap());
        authenticate(&mut client, &mut reader);
        (client, reader)
    };
    let (mut target, mut target_reader) = connect();
    let (mut other, mut other_reader) = connect();
    run_write(&mut target, &mut target_reader, "CREATE TABLE t (id INT PRIMARY KEY, uk INT, b INT, UNIQUE KEY(uk))");
    run_write(&mut target, &mut target_reader, "INSERT INTO t VALUES (1,10,0),(2,20,0),(3,30,0)");
    let id = run_query(&mut target, &mut target_reader, "SELECT connection_id()")[0][0].clone();
    let root_memory = |client: &mut TcpStream, reader: &mut PacketReader<TcpStream>| {
        let plan = run_query(client, reader, &format!("EXPLAIN FOR CONNECTION {id}"));
        (plan[0][0].clone(), plan[0][7].clone())
    };
    for (warm, memory) in [
        ("SELECT 1", "889 Bytes"),
        ("SELECT id FROM t", "889 Bytes"),
        ("UPDATE t SET b = b + 1 WHERE b > 100", "1.11 KB"),
    ] {
        run_write(&mut target, &mut target_reader, "UPDATE t SET b = 1 WHERE id = 3");
        if warm.starts_with("SELECT") {
            run_query(&mut target, &mut target_reader, warm);
        } else {
            run_write(&mut target, &mut target_reader, warm);
        }
        run_write(&mut target, &mut target_reader, "UPDATE t SET b = b + 1 WHERE id = 1");
        assert_eq!(root_memory(&mut other, &mut other_reader), ("Update_3".to_owned(), memory.to_owned()), "{warm}");
    }
    // The connection explaining itself reads its own current statement's
    // trackers, which hold no `Update_3`.
    assert_eq!(root_memory(&mut target, &mut target_reader), ("Update_3".to_owned(), "N/A".to_owned()));
}

#[test]
fn lazy_cursor_fetch_detaches_and_tracks_the_cursor_like_go() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let store = users();
    let node = node_over(&store);
    let processes = node.factory().processes();
    let worker_node = Arc::clone(&node);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            worker_node.factory().as_ref(),
            &store,
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });

    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader);

    run_write(&mut client, &mut reader, "CREATE TABLE t1 (id BIGINT PRIMARY KEY, v BIGINT)");
    run_write(
        &mut client,
        &mut reader,
        "INSERT INTO t1 VALUES (1,10),(2,20),(3,30),(4,40),(5,50)",
    );
    let types = [0x08u8];
    let ids = |rows: Vec<Vec<String>>| rows.into_iter().map(|row| row[0].clone()).collect::<Vec<_>>();

    // Lazy cursor fetch OFF (Go's default): the eager cursor tracks nothing.
    let (statement, _, _) = prepare_statement(&mut client, &mut reader, "SELECT id FROM t1");
    let (_, status) = execute_with_cursor(&mut client, &mut reader, statement, &[]);
    assert_eq!(status, 0x0042);
    assert_eq!(tracked_cursor_start_ts(&processes), Vec::<u64>::new());
    let (rows, status) = fetch_rows(&mut client, &mut reader, statement, 10, &types);
    assert_eq!((ids(rows), status), (strings(&["1", "2", "3", "4", "5"]), 0x0082));

    run_write(&mut client, &mut reader, "SET tidb_enable_lazy_cursor_fetch = ON");

    // A detached table reader: one cursor at the statement's start TS.
    let (_, status) = execute_with_cursor(&mut client, &mut reader, statement, &[]);
    assert_eq!(status, 0x0042);
    let cursors = tracked_cursor_start_ts(&processes);
    assert_eq!(cursors.len(), 1, "{cursors:?}");
    assert_ne!(cursors[0], 0);
    // The cursor holds back the reported minimum start TS.
    let pl = tidb_sessmgr::processinfo::SessionManager::show_process_list(&processes);
    assert!(tidb_domain_infosync::min_start_ts::process_list_min_start_ts_candidates(&pl)
        .contains(&cursors[0]));
    let (rows, status) = fetch_rows(&mut client, &mut reader, statement, 2, &types);
    assert_eq!((ids(rows), status), (strings(&["1", "2"]), 0x0042));
    // The session runs another statement; the cursor still reads its own
    // snapshot.
    assert_eq!(run_write(&mut client, &mut reader, "INSERT INTO t1 VALUES (6,60)"), 1);
    assert_eq!(tracked_cursor_start_ts(&processes), cursors);
    let (rows, status) = fetch_rows(&mut client, &mut reader, statement, 10, &types);
    assert_eq!((ids(rows), status), (strings(&["3", "4", "5"]), 0x0082));
    // Reaching the end resets the statement, which closes the cursor.
    assert_eq!(settled_cursor_start_ts(&mut client, &mut reader, &processes), Vec::<u64>::new());
    run_write(&mut client, &mut reader, "DELETE FROM t1 WHERE id = 6");

    // Go `TestStaticRecordSetWithTxn`: a statement between the execute and
    // the first FETCH does not reach the detached reader's snapshot.
    let (_, status) = execute_with_cursor(&mut client, &mut reader, statement, &[]);
    assert_eq!(status, 0x0042);
    assert_eq!(run_write(&mut client, &mut reader, "INSERT INTO t1 VALUES (6,60)"), 1);
    let (rows, status) = fetch_rows(&mut client, &mut reader, statement, 10, &types);
    assert_eq!((ids(rows), status), (strings(&["1", "2", "3", "4", "5"]), 0x0082));
    run_write(&mut client, &mut reader, "DELETE FROM t1 WHERE id = 6");

    // Rows spanning several chunks keep coming from the snapshot while
    // other statements change the table between FETCHes.
    run_write(&mut client, &mut reader, "CREATE TABLE big (id BIGINT PRIMARY KEY)");
    let values = (1..=3000).map(|id| format!("({id})")).collect::<Vec<_>>().join(",");
    // (`run_transaction_control` only checks for the OK packet: these counts
    // do not fit `run_write`'s one-byte decoding.)
    run_transaction_control(&mut client, &mut reader, &format!("INSERT INTO big VALUES {values}"));
    let (big, _, _) = prepare_statement(&mut client, &mut reader, "SELECT id FROM big");
    execute_with_cursor(&mut client, &mut reader, big, &[]);
    assert_eq!(tracked_cursor_start_ts(&processes).len(), 1);
    let (rows, _) = fetch_rows(&mut client, &mut reader, big, 10, &types);
    assert_eq!(ids(rows), (1..=10).map(|id| id.to_string()).collect::<Vec<_>>());
    run_transaction_control(&mut client, &mut reader, "DELETE FROM big WHERE id > 5");
    assert_eq!(run_write(&mut client, &mut reader, "INSERT INTO big VALUES (5000)"), 1);
    // Go caps one FETCH at 1024 rows (`parse.StmtFetchCmd`'s `maxFetchSize`).
    let mut rest = Vec::new();
    let mut statuses = Vec::new();
    while statuses.last().is_none_or(|status| status & 0x0080 == 0) {
        let (rows, status) = fetch_rows(&mut client, &mut reader, big, 5000, &types);
        rest.extend(ids(rows));
        statuses.push(status);
    }
    assert_eq!(statuses, [0x0042, 0x0042, 0x0082]);
    assert_eq!(rest, (11..=3000).map(|id| id.to_string()).collect::<Vec<_>>());
    assert_eq!(settled_cursor_start_ts(&mut client, &mut reader, &processes), Vec::<u64>::new());

    // A projection and a parameterized selection detach too.
    let (projection, _, _) = prepare_statement(&mut client, &mut reader, "SELECT id + v FROM t1");
    execute_with_cursor(&mut client, &mut reader, projection, &[]);
    assert_eq!(tracked_cursor_start_ts(&processes).len(), 1);
    let (rows, status) = fetch_rows(&mut client, &mut reader, projection, 10, &types);
    assert_eq!((ids(rows), status), (strings(&["11", "22", "33", "44", "55"]), 0x0082));
    let (selection, _, _) =
        prepare_statement(&mut client, &mut reader, "SELECT id FROM t1 WHERE v >= ?");
    execute_with_cursor(&mut client, &mut reader, selection, &[30]);
    assert_eq!(tracked_cursor_start_ts(&processes).len(), 1);
    let (rows, status) = fetch_rows(&mut client, &mut reader, selection, 10, &types);
    assert_eq!((ids(rows), status), (strings(&["3", "4", "5"]), 0x0082));
    assert_eq!(settled_cursor_start_ts(&mut client, &mut reader, &processes), Vec::<u64>::new());

    // `CONNECTION_ID()` folds into the plan as Go's `NewFunction` builds it
    // (`plus(1, test.t1.id)` in a Go testkit), so its projection detaches.
    let (folded, _, _) = prepare_statement(&mut client, &mut reader, "SELECT connection_id() + id FROM t1");
    execute_with_cursor(&mut client, &mut reader, folded, &[]);
    assert_eq!(tracked_cursor_start_ts(&processes).len(), 1);
    fetch_rows(&mut client, &mut reader, folded, 10, &types);
    assert_eq!(settled_cursor_start_ts(&mut client, &mut reader, &processes), Vec::<u64>::new());

    // A root Limit does not detach (Go `BaseExecutorV2.Detach`), and neither
    // does a projection reading an optional property (`FOUND_ROWS()` reads
    // the session variables): both take the eager cursor.
    for sql in ["SELECT id FROM t1 LIMIT 3", "SELECT found_rows() + id FROM t1"] {
        let (eager, _, _) = prepare_statement(&mut client, &mut reader, sql);
        let (_, status) = execute_with_cursor(&mut client, &mut reader, eager, &[]);
        assert_eq!(status, 0x0042, "{sql}");
        assert_eq!(tracked_cursor_start_ts(&processes), Vec::<u64>::new(), "{sql}");
        fetch_rows(&mut client, &mut reader, eager, 10, &types);
    }

    // Inside an explicit transaction nothing detaches (Go
    // `IsAutoCommitTxn`).
    run_write(&mut client, &mut reader, "BEGIN");
    let (_, status) = execute_with_cursor(&mut client, &mut reader, statement, &[]);
    assert_eq!(status, 0x0043);
    assert_eq!(tracked_cursor_start_ts(&processes), Vec::<u64>::new());
    let (rows, status) = fetch_rows(&mut client, &mut reader, statement, 10, &types);
    assert_eq!((ids(rows).len(), status), (5, 0x0083));
    run_write(&mut client, &mut reader, "COMMIT");

    // An open lazy cursor closes with its statement.
    execute_with_cursor(&mut client, &mut reader, statement, &[]);
    assert_eq!(tracked_cursor_start_ts(&processes).len(), 1);
    let mut close = vec![COM_STMT_CLOSE];
    close.extend_from_slice(&statement.to_le_bytes());
    write_packet(&mut client, 0, &close);
    run_query(&mut client, &mut reader, "SELECT 1");
    assert_eq!(tracked_cursor_start_ts(&processes), Vec::<u64>::new());

    write_packet(&mut client, 0, &[0x01]);
    drop(client);
    assert_eq!(worker.join().unwrap().exit, ConnectionExit::Quit);
}

/// Go `execStmtResult.TryDetach` finishes a lazily fetched statement with
/// `finishStmt` alone and abandons its record set, whose `Close`
/// (`FinishExecuteStmt`) never runs: the statement leaves no statement
/// summary entry, while the same statement run as a plain query does (Go
/// testkit probe over `TryDetach`).
#[test]
fn a_detached_cursor_statement_is_not_summarized_like_go() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker_tracker = Arc::clone(&tracker);
    let store = users();
    let node = node_over(&store);
    let worker_node = Arc::clone(&node);
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            worker_node.factory().as_ref(),
            &store,
            &worker_tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
        .unwrap()
    });
    let mut client = TcpStream::connect(address).unwrap();
    let read_side = client.try_clone().unwrap();
    let mut reader = PacketReader::new(read_side);
    authenticate(&mut client, &mut reader);
    run_write(&mut client, &mut reader, "CREATE TABLE lazy_summary (id BIGINT PRIMARY KEY)");
    run_write(&mut client, &mut reader, "INSERT INTO lazy_summary VALUES (1),(2),(3)");
    run_write(&mut client, &mut reader, "SET tidb_enable_lazy_cursor_fetch = ON");
    let summarized = |client: &mut TcpStream, reader: &mut PacketReader<TcpStream>, table: &str| {
        run_query(
            client,
            reader,
            &format!(
                "SELECT COUNT(*) FROM information_schema.statements_summary \
                 WHERE digest_text = 'select `id` from `{table}` where `id` > ?'"
            ),
        )[0][0]
            .clone()
    };
    let types = [0x08u8];
    let (statement, _, _) =
        prepare_statement(&mut client, &mut reader, "SELECT id FROM lazy_summary WHERE id > 0");
    let (_, status) = execute_with_cursor(&mut client, &mut reader, statement, &[]);
    assert_eq!(status, 0x0042);
    let (rows, status) = fetch_rows(&mut client, &mut reader, statement, 10, &types);
    assert_eq!((rows.len(), status), (3, 0x0082));
    assert_eq!(summarized(&mut client, &mut reader, "lazy_summary"), "0");
    run_query(&mut client, &mut reader, "SELECT id FROM lazy_summary WHERE id > 0");
    assert_eq!(summarized(&mut client, &mut reader, "lazy_summary"), "1");
    write_packet(&mut client, 0, &[0x01]);
    drop(client);
    assert_eq!(worker.join().unwrap().exit, ConnectionExit::Quit);
}

/// Go `StreamAggExec.OpenSelf` takes its child chunk from the connection's
/// chunk pool when it opens, after its `DefaultVal`, so it gets the column a
/// previous statement's result grew. Captured from a Go tidb-server over the
/// wire (199 rows): the StreamAgg charges 388 Bytes on a fresh connection and
/// 2.16 KB after a scan returned 189 rows.
#[test]
fn a_stream_agg_takes_its_child_chunk_from_the_pool_when_it_opens_like_go() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let store = users();
    let node = node_over(&store);
    let worker_node = Arc::clone(&node);
    std::thread::spawn(move || {
        for _ in 0..3 {
            let (stream, peer_addr) = listener.accept().unwrap();
            let node = Arc::clone(&worker_node);
            let store = store.clone();
            let tracker = Arc::clone(&tracker);
            std::thread::spawn(move || {
                let _ = serve_mysql_connection(
                    stream,
                    peer_addr,
                    ConnectionCancellation::default(),
                    node.factory().as_ref(),
                    &store,
                    &tracker,
                    DEFAULT_MAX_ALLOWED_PACKET,
                );
            });
        }
    });
    let connect = || {
        let mut client = TcpStream::connect(address).unwrap();
        let mut reader = PacketReader::new(client.try_clone().unwrap());
        authenticate(&mut client, &mut reader);
        (client, reader)
    };
    let stream_agg_memory = |client: &mut TcpStream, reader: &mut PacketReader<TcpStream>, from: &str| {
        let plan = run_query(client, reader, &format!("EXPLAIN ANALYZE SELECT /*+ stream_agg() */ count(*) FROM {from}"));
        plan.iter()
            .find(|row| row[0].starts_with("StreamAgg"))
            .unwrap_or_else(|| panic!("{plan:?}"))[7]
            .clone()
    };
    let (mut fresh, mut fresh_reader) = connect();
    run_write(&mut fresh, &mut fresh_reader, "CREATE TABLE i (a INT PRIMARY KEY, b INT, KEY (b))");
    let values = (1..=199).map(|x| format!("({x}, {x})")).collect::<Vec<_>>().join(", ");
    run_write(&mut fresh, &mut fresh_reader, &format!("INSERT INTO i VALUES {values}"));
    // Go `LimitExec.open` takes its child chunk from the pool after its
    // children opened, so the StreamAgg above it, opened later, gets the
    // grown column (1.02 KB fresh, 2.16 KB after the scan).
    let limited = "(SELECT b FROM i WHERE b > 0 LIMIT 100) t";
    for (from, fresh_memory) in [("i", "388 Bytes"), (limited, "1.02 KB")] {
        let (mut scanned, mut scanned_reader) = connect();
        assert_eq!(stream_agg_memory(&mut scanned, &mut scanned_reader, from), fresh_memory, "{from}");
        assert_eq!(run_query(&mut scanned, &mut scanned_reader, "SELECT * FROM i WHERE b > 10").len(), 189);
        assert_eq!(stream_agg_memory(&mut scanned, &mut scanned_reader, from), "2.16 KB", "{from}");
    }
}

/// Go's EXPLAIN ANALYZE writes its result through a chunk the connection
/// takes from its allocator after `ExplainExec.Open` and before `Next` runs
/// the analyzed plan, and a lookup's index worker, table tasks (a pushed
/// partial aggregate's included) and a root `LimitExec` take theirs from the
/// same allocator, so every statement's charges follow the columns earlier
/// statements on the connection returned to it. Captured from a Go
/// tidb-server over one connection, and (for this client, which sends no
/// session setup queries first) a Go testkit statement loop installing an
/// allocator per statement as `clientConn` does.
#[test]
fn a_connections_statements_reuse_each_others_chunks_like_go() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let store = users();
    let node = node_over(&store);
    std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let _ = serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            node.factory().as_ref(),
            &store,
            &tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        );
    });
    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader);
    run_write(&mut client, &mut reader, "CREATE TABLE ch (a VARCHAR(10) PRIMARY KEY CLUSTERED, b INT, c INT, KEY(b))");
    run_write(
        &mut client,
        &mut reader,
        "INSERT INTO ch VALUES ('x',1,1),('y',2,2),('z',3,3),('w',4,4),('v',5,5),('u',6,6)",
    );
    run_write(&mut client, &mut reader, "CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT, KEY (b))");
    let values = (1..=100).map(|x| format!("({x}, {x}, {})", x % 5)).collect::<Vec<_>>().join(", ");
    run_write(&mut client, &mut reader, &format!("INSERT INTO t VALUES {values}"));
    for (sql, operator, memory) in [
        ("SELECT * FROM ch USE INDEX(b) WHERE b > 1", "IndexLookUp_7", "3.18 KB"),
        ("SELECT * FROM ch USE INDEX(b) WHERE b > 1 ORDER BY b", "IndexLookUp_14", "3.54 KB"),
        ("SELECT count(c) FROM t USE INDEX(b) WHERE b > 1", "HashAgg_13", "13.5 KB"),
        ("SELECT count(c) FROM t USE INDEX(b) WHERE b > 1", "HashAgg_13", "21.3 KB"),
        ("SELECT * FROM t WHERE c > 2 LIMIT 3", "Limit_8", "N/A"),
        ("SELECT b, c FROM t USE INDEX(b) WHERE b > 1", "IndexLookUp_7", "14.1 KB"),
        ("SELECT * FROM t WHERE c > 2 LIMIT 3", "Limit_8", "N/A"),
        ("SELECT * FROM ch USE INDEX(b) WHERE b > 1 ORDER BY b", "IndexLookUp_14", "12.0 KB"),
    ] {
        let plan = run_query(&mut client, &mut reader, &format!("EXPLAIN ANALYZE {sql}"));
        let row = plan
            .iter()
            .find(|row| row[0].trim_start_matches(|c: char| !c.is_ascii_alphabetic()) == operator)
            .unwrap_or_else(|| panic!("{sql}: no {operator} in {plan:?}"));
        // A reader's charge carries its responses' execution details, whose
        // timings vary in length; the LIMIT reads matter for what they leave
        // in the pool.
        assert_eq!(row[7], memory, "{sql}: {plan:?}");
    }
}

/// Go's `ExplainExec.Next` appends its rows into the chunk the connection
/// took from its allocator, so the string columns that chunk grows return to
/// the pool and a later statement's string columns take them. Captured from
/// a Go statement loop that installs an allocator per statement and writes
/// each result through `rs.NewChunk(alloc)`, as `clientConn` does.
#[test]
fn an_explain_result_is_written_through_its_pooled_chunk_like_go() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let store = users();
    let node = node_over(&store);
    std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let _ = serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            node.factory().as_ref(),
            &store,
            &tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        );
    });
    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader);
    run_write(&mut client, &mut reader, "CREATE TABLE t (a INT, b VARCHAR(20), KEY(a))");
    run_write(&mut client, &mut reader, "INSERT INTO t VALUES (1,'x'),(2,'yy'),(2,'zzz'),(3,'w')");
    for (sql, operator, memory) in [
        ("SELECT * FROM t USE INDEX(a) WHERE a > 0", "IndexLookUp_7", "2.10 KB"),
        ("SELECT * FROM t USE INDEX(a) WHERE a > 0", "IndexLookUp_7", "9.97 KB"),
        ("SELECT /*+ hash_agg() */ b, count(a) FROM t GROUP BY b", "HashAgg_9", "32.8 KB"),
        ("SELECT /*+ hash_agg() */ b, count(a) FROM t GROUP BY b", "HashAgg_9", "33.4 KB"),
        ("SELECT /*+ hash_agg() */ a, group_concat(b) FROM t GROUP BY a", "HashAgg_5", "33.9 KB"),
        ("SELECT * FROM t USE INDEX(a) WHERE a > 0", "IndexLookUp_7", "2.72 KB"),
    ] {
        let plan = run_query(&mut client, &mut reader, &format!("EXPLAIN ANALYZE {sql}"));
        let row = plan
            .iter()
            .find(|row| row[0].trim_start_matches(|c: char| !c.is_ascii_alphabetic()) == operator)
            .unwrap_or_else(|| panic!("{sql}: no {operator} in {plan:?}"));
        assert_eq!(row[7], memory, "{sql}: {plan:?}");
    }
}

/// Go's `writeChunks` writes every record set through `rs.NewChunk(alloc)`:
/// a plain EXPLAIN (whose `ExplainExec` builds no executors) and a SHOW draw
/// their write chunk from the connection's allocator too, so the string
/// columns they grow reach later statements. Run against the server binary,
/// whose connection serves the routed session; captured from a Go statement
/// loop that installs an allocator per statement, as `clientConn` does.
#[test]
fn a_plain_explain_and_a_show_write_through_pooled_chunks_like_go() {
    let free_port = || TcpListener::bind(("127.0.0.1", 0)).unwrap().local_addr().unwrap().port();
    let (port, status_port) = (free_port(), free_port());
    let base = std::env::temp_dir().join(format!("tidb-server-pooled-write-{}", std::process::id()));
    let users = base.with_extension("users");
    // `root`, the bootstrapped account, with the harness password `secret`.
    std::fs::write(&users, "root\t%\tmysql_native_password\t*14E65567ABDB5135D0CFD9A70B3032C179A49EE7\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&users, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let _child = Child(
        std::process::Command::new(env!("CARGO_BIN_EXE_tidb-server"))
            .args(["--store", "unistore", "--path", base.to_str().unwrap()])
            .args(["--host", "127.0.0.1", "-P", &port.to_string(), "--socket", ""])
            .args(["--status-host", "127.0.0.1", "--status", &status_port.to_string()])
            .args(["--auth-file", users.to_str().unwrap()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("start tidb-server"),
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
    let mut client = loop {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(client) => break client,
            Err(_) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Err(error) => panic!("tidb-server did not listen: {error}"),
        }
    };
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    // The flags a pymysql client sends, whose text queries the node splits
    // into statements as Go's `handleQuery` does.
    let multi = (1 << 16) | (1 << 17);
    assert_eq!(handshake_reply_as(&mut client, &mut reader, b"root", multi, 46)[0], 0, "auth OK");
    for sql in [
        "SET AUTOCOMMIT = 1",
        "CREATE DATABASE w",
        "USE w",
        "CREATE TABLE t (a INT, b VARCHAR(20), KEY(a))",
        "INSERT INTO t VALUES (1,'x'),(2,'yy'),(2,'zzz'),(3,'w')",
    ] {
        run_write(&mut client, &mut reader, sql);
    }
    let memory = |client: &mut TcpStream, reader: &mut PacketReader<TcpStream>, sql: &str, operator: &str| {
        let plan = run_query(client, reader, &format!("EXPLAIN ANALYZE {sql}"));
        plan.iter()
            .find(|row| row[0].trim_start_matches(|c: char| !c.is_ascii_alphabetic()) == operator)
            .unwrap_or_else(|| panic!("{sql}: no {operator} in {plan:?}"))[7]
            .clone()
    };
    run_query(&mut client, &mut reader, "EXPLAIN SELECT /*+ hash_agg() */ a, group_concat(b) FROM t GROUP BY a");
    let sql = "SELECT /*+ hash_agg() */ a, group_concat(b) FROM t GROUP BY a";
    assert_eq!(memory(&mut client, &mut reader, sql, "HashAgg_5"), "16.5 KB");
    run_query(&mut client, &mut reader, "EXPLAIN FORMAT='brief' SELECT * FROM t WHERE a > 1 ORDER BY b");
    let sql = "SELECT /*+ hash_agg() */ b, count(a) FROM t GROUP BY b";
    assert_eq!(memory(&mut client, &mut reader, sql, "HashAgg_9"), "16.9 KB");
    run_query(&mut client, &mut reader, "SHOW COLUMNS FROM t");
    let sql = "SELECT * FROM t USE INDEX(a) WHERE a > 0";
    assert_eq!(memory(&mut client, &mut reader, sql, "IndexLookUp_7"), "2.10 KB");
    // Go zzprobe/c02.TestC02Materialized: analyzed DML runs in
    // handleNoDelayExecutor BEFORE the Explain record set takes its chunk.
    // One projection worker fixes the empty-output swap's allocation order.
    run_write(&mut client, &mut reader, "SET tidb_executor_concurrency=1");
    assert_eq!(memory(&mut client, &mut reader, "UPDATE t SET b='changed' WHERE a=1", "Update_4"), "1.36 KB");
    let sql = "SELECT /*+ hash_agg() */ b, count(a) FROM t GROUP BY b";
    assert_eq!(memory(&mut client, &mut reader, sql, "HashAgg_9"), "1.76 KB");
    assert_eq!(memory(&mut client, &mut reader, "INSERT INTO t SELECT a+10,b FROM t", "Insert_1"), "1.58 KB");
    assert_eq!(memory(&mut client, &mut reader, sql, "HashAgg_9"), "2.38 KB");
    run_query(&mut client, &mut reader, "TRACE SELECT * FROM t");
    assert_eq!(memory(&mut client, &mut reader, sql, "HashAgg_9"), "1.76 KB");
    let _ = std::fs::remove_file(&users);
    let _ = std::fs::remove_dir_all(&base);
}

/// Go `Handle` starts empty and loads each user at first use
/// (`ensureActiveUser`) until the first event's `UpdateAll`, and
/// `user_connections.go` counts each account's live connections against its
/// own `MAX_USER_CONNECTIONS`, else the global `max_user_connections`
/// (`checkUserConnectionCount` at login, `increaseUserConnectionsCount` once
/// logged in, `decreaseUserConnectionCount` at the end). Captured from a Go
/// tidb-server on a fresh unistore store, the same statements over pymysql:
/// a `mysql.user` row written after startup logs in with no FLUSH, a second
/// `u1` and a second `root` under the global limit are refused with 1203.
#[test]
fn a_login_loads_its_user_and_counts_against_max_user_connections_like_go() {
    let free_port = || TcpListener::bind(("127.0.0.1", 0)).unwrap().local_addr().unwrap().port();
    let (port, status_port) = (free_port(), free_port());
    let base = std::env::temp_dir().join(format!("tidb-server-user-connections-{}", std::process::id()));
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    // The accounts are the store's own, as Go's are: the bootstrap's
    // passwordless `root@%`.
    let _child = Child(
        std::process::Command::new(env!("CARGO_BIN_EXE_tidb-server"))
            .args(["--store", "unistore", "--path", base.to_str().unwrap()])
            .args(["--host", "127.0.0.1", "-P", &port.to_string(), "--socket", ""])
            .args(["--status-host", "127.0.0.1", "--status", &status_port.to_string()])
            .arg("--load-privileges")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("start tidb-server"),
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
    let connect = || loop {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(client) => break client,
            Err(_) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Err(error) => panic!("tidb-server did not listen: {error}"),
        }
    };
    // The login's answer: `OK`, or the error packet's code and message.
    let login = |user: &str| {
        let mut client = connect();
        let mut reader = PacketReader::new(client.try_clone().unwrap());
        reader.set_sequence(0);
        let salt = handshake_salt(&reader.read_packet().unwrap());
        let mut response = Vec::new();
        let capabilities = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
        response.extend_from_slice(&capabilities.to_le_bytes());
        response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
        response.push(46);
        response.extend_from_slice(&[0; 23]);
        response.extend_from_slice(user.as_bytes());
        response.push(0);
        // `root` is passwordless; every other account's password is `secret`.
        let auth = if user == "root" { Vec::new() } else { native_response(b"secret", &salt).to_vec() };
        response.push(u8::try_from(auth.len()).unwrap());
        response.extend_from_slice(&auth);
        response.extend_from_slice(b"mysql_native_password\0");
        write_packet(&mut client, 1, &response);
        reader.set_sequence(2);
        let reply = reader.read_packet().unwrap();
        let answer = if reply[0] == 0xff {
            format!(
                "{} {}",
                u16::from_le_bytes([reply[1], reply[2]]),
                String::from_utf8_lossy(&reply[9..])
            )
        } else {
            "OK".to_owned()
        };
        (client, reader, answer)
    };
    let (mut root, mut root_reader, answer) = login("root");
    assert_eq!(answer, "OK");
    run_write(
        &mut root,
        &mut root_reader,
        "INSERT INTO mysql.user (Host, User, authentication_string, plugin) \
         VALUES ('%', 'raw', '*14E65567ABDB5135D0CFD9A70B3032C179A49EE7', 'mysql_native_password')",
    );
    assert_eq!(login("raw").2, "OK", "a user written after startup loads at its login");
    run_write(&mut root, &mut root_reader, "CREATE USER u1 IDENTIFIED BY 'secret' WITH MAX_USER_CONNECTIONS 1");
    let first = login("u1");
    assert_eq!(first.2, "OK");
    assert_eq!(login("u1").2, "1203 User u1@% has exceeded the 'max_user_connections' resource");
    drop(first);
    let again = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let answer = login("u1").2;
        if answer == "OK" {
            break;
        }
        assert!(std::time::Instant::now() < again, "a closed connection releases its count: {answer}");
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    run_write(&mut root, &mut root_reader, "SET GLOBAL max_user_connections = 1");
    assert_eq!(login("root").2, "1203 User root@% has exceeded the 'max_user_connections' resource");
    run_write(&mut root, &mut root_reader, "SET GLOBAL max_user_connections = 0");
    assert_eq!(login("root").2, "OK");
    let _ = std::fs::remove_dir_all(&base);
}

/// Go `pkg/extension/event_listener_test.go:TestExtensionStmtEvents`, through TCP.
#[test]
fn test_extension_stmt_events() {
    use std::sync::Mutex;
    use tidb_extension::{ConnEventTp, StmtEventTp};
    #[derive(Clone, Debug)]
    struct Record {
        tp: StmtEventTp,
        text: String,
        normalized: String,
        digest: bool,
        params: Vec<tidb_datatype::Datum>,
        tables: Vec<(String, String)>,
        affected: u64,
        node: bool,
        execute: bool,
        prepared: bool,
        error: Option<String>,
        alias: String,
        user: tidb_parser::auth::UserIdentity,
        connection_id: u64,
    }
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            tidb_extension::reset();
        }
    }
    let _reset = Reset;
    tidb_extension::reset();
    let records = Arc::new(Mutex::new(Vec::<Record>::new()));
    let connections = Arc::new(Mutex::new(Vec::new()));
    let factory_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let captured = records.clone();
    let conn = connections.clone();
    let factories = factory_calls.clone();
    tidb_extension::register(
        "test",
        vec![tidb_extension::with_session_handler_factory(Arc::new(
            move || {
                factories.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let captured = captured.clone();
                let conn = conn.clone();
                Some(tidb_extension::SessionHandler {
                    on_connection_event: Some(Arc::new(move |tp, info| {
                        conn.lock().unwrap().push((tp, info.clone()))
                    })),
                    on_stmt_event: Some(Arc::new(move |tp, info| {
                        captured.lock().unwrap().push(Record {
                            tp,
                            text: info.original_text().into(),
                            normalized: info.sql_digest().0.into(),
                            digest: {
                                let digest: Option<&tidb_parser::Digest> = info.sql_digest().1;
                                digest.is_some()
                            },
                            params: info.prepared_params().to_vec(),
                            tables: info
                                .related_tables()
                                .iter()
                                .map(|t| (t.db.clone(), t.table.clone()))
                                .collect(),
                            affected: info.affected_rows(),
                            node: info.stmt_node().is_some(),
                            execute: info.execute_stmt_node().is_some(),
                            prepared: info.execute_prepared_stmt().is_some(),
                            error: info.get_error().map(str::to_owned),
                            alias: info.session_alias().into(),
                            user: info.user().unwrap().clone(),
                            connection_id: info.connection_info().unwrap().connection_id,
                        });
                    })),
                })
            },
        ))],
    )
    .unwrap();
    tidb_extension::setup().unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let worker = std::thread::spawn(move || {
        let store = users();
        let node = node_over(&store);
        let tracker = Arc::new(ConnectionTracker::default());
        // One accepted and one rejected connection, each gets a new handler.
        for _ in 0..2 {
            let (stream, peer) = listener.accept().unwrap();
            serve_mysql_connection(
                stream,
                peer,
                ConnectionCancellation::default(),
                node.factory().as_ref(),
                &store,
                &tracker,
                DEFAULT_MAX_ALLOWED_PACKET,
            )
            .unwrap();
        }
    });
    let mut client = TcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(60)))
        .unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate_with_capabilities(&mut client, &mut reader, 1 << 16);
    for sql in [
        "create table t1(a int,b int)",
        "create table t2(id int primary key)",
        "create database test2",
        "create table test2.t1(c int,d int)",
        "set @a=1",
        "set @b=2",
    ] {
        run_write(&mut client, &mut reader, sql);
    }
    let (p1, _, _) = prepare_statement(&mut client, &mut reader, "select ?");
    let (p2, _, _) = prepare_statement(&mut client, &mut reader, "select a, b from t1 left join test2.t1 as t2 on t2.c = t1.a where t1.a = 3 and t1.b = ? and t2.d = ?");
    run_write(&mut client, &mut reader, "create table tnoexist(n int)");
    let (p3, _, _) =
        prepare_statement(&mut client, &mut reader, "select * from tnoexist where n=?");
    run_write(&mut client, &mut reader, "drop table tnoexist");
    let (p4, _, _) = prepare_statement(&mut client, &mut reader, "insert into t2 values(?)");
    // Ping waits for the callback after the preceding response has been flushed.
    let sync = |client: &mut TcpStream, reader: &mut PacketReader<TcpStream>| {
        write_packet(client, 0, &[COM_PING]);
        reader.set_sequence(1);
        assert_eq!(reader.read_packet().unwrap()[0], 0);
    };
    sync(&mut client, &mut reader);
    records.lock().unwrap().clear();
    let check = |text: &str,
                 normalized: &str,
                 tables: &[(&str, &str)],
                 affected: u64,
                 error: bool,
                 execute: bool,
                 prepared: bool,
                 params: &[i64],
                 alias: &str| {
        let mut locked = records.lock().unwrap();
        assert_eq!(locked.len(), 1, "{text}: {locked:?}");
        let record = locked.remove(0);
        assert_eq!(record.text, text);
        assert_eq!(record.normalized, normalized);
        assert_eq!(
            record.tp,
            if error {
                StmtEventTp::Error
            } else {
                StmtEventTp::Success
            }
        );
        assert_eq!(record.error.is_some(), error);
        assert_eq!(record.affected, affected);
        assert_eq!(record.execute, execute);
        assert_eq!(record.prepared, prepared);
        assert_eq!(record.node, text != "invalid sql");
        assert_eq!(record.digest, text != "invalid sql");
        assert_eq!(record.alias, alias);
        assert_eq!(record.user.username, "alice");
        assert_eq!(record.user.auth_hostname, "%");
        assert_ne!(record.connection_id, 0);
        let mut actual = record.tables.clone();
        actual.sort();
        let mut expected: Vec<_> = tables
            .iter()
            .map(|(d, t)| (d.to_string(), t.to_string()))
            .collect();
        expected.sort();
        assert_eq!(actual, expected, "{text}");
        assert_eq!(
            record
                .params
                .iter()
                .map(|v| v.as_int().unwrap())
                .collect::<Vec<_>>(),
            params
        );
        record
    };
    run_query(&mut client, &mut reader, "select 1");
    sync(&mut client, &mut reader);
    check("select 1", "select ?", &[], 0, false, false, false, &[], "");
    write_packet(&mut client, 0, b"\x03invalid sql");
    reader.set_sequence(1);
    assert_error_packet(&reader.read_packet().unwrap(), 1064, b"42000");
    sync(&mut client, &mut reader);
    check(
        "invalid sql",
        "invalid sql",
        &[],
        0,
        true,
        false,
        false,
        &[],
        "",
    );
    execute_statement(&mut client, &mut reader, p1, &[7]);
    sync(&mut client, &mut reader);
    check("select ?", "select ?", &[], 0, false, true, true, &[7], "");
    let sql = "select a, b from t1 left join test2.t1 as t2 on t2.c = t1.a where t1.a = 3 and t1.b = ? and t2.d = ?";
    execute_statement(&mut client, &mut reader, p2, &[3, 4]);
    sync(&mut client, &mut reader);
    check(sql, "select `a` , `b` from `t1` left join `test2` . `t1` as `t2` on `t2` . `c` = `t1` . `a` where `t1` . `a` = ? and `t1` . `b` = ? and `t2` . `d` = ?", &[("test","t1"),("test2","t1")], 0, false, true, true, &[3,4], "");
    run_query(
        &mut client,
        &mut reader,
        "select a, b from t1 where a > 1 and b < 2",
    );
    sync(&mut client, &mut reader);
    check(
        "select a, b from t1 where a > 1 and b < 2",
        "select `a` , `b` from `t1` where `a` > ? and `b` < ?",
        &[("test", "t1")],
        0,
        false,
        false,
        false,
        &[],
        "",
    );
    assert_eq!(
        execute_statement_packet_tags(&mut client, &mut reader, p3, &[5]),
        vec!["error"]
    );
    sync(&mut client, &mut reader);
    let error = check(
        "select * from tnoexist where n=?",
        "select * from `tnoexist` where `n` = ?",
        &[("test", "tnoexist")],
        0,
        true,
        true,
        true,
        &[5],
        "",
    )
    .error
    .unwrap();
    assert_eq!(error, "select * from tnoexist where n=? [arguments: 5]: [planner:8113]Schema change caused error: [schema:1146]Table 'test.tnoexist' doesn't exist");
    run_write(&mut client, &mut reader, "insert into t2 values(1)");
    sync(&mut client, &mut reader);
    check(
        "insert into t2 values(1)",
        "insert into `t2` values ( ? )",
        &[("test", "t2")],
        1,
        false,
        false,
        false,
        &[],
        "",
    );
    write_packet(&mut client, 0, b"\x03insert into t2 values(1)");
    reader.set_sequence(1);
    assert_error_packet(&reader.read_packet().unwrap(), 1062, b"23000");
    sync(&mut client, &mut reader);
    check(
        "insert into t2 values(1)",
        "insert into `t2` values ( ? )",
        &[("test", "t2")],
        0,
        true,
        false,
        false,
        &[],
        "",
    );
    let prep = "prepare s from 'select * from t1 where a=1 and b>? and b<?'";
    run_write(&mut client, &mut reader, prep);
    sync(&mut client, &mut reader);
    check(
        prep,
        "prepare `s` from ?",
        &[],
        0,
        false,
        false,
        false,
        &[],
        "",
    );
    run_query(&mut client, &mut reader, "execute s using @a, @b");
    sync(&mut client, &mut reader);
    check(
        "select * from t1 where a=1 and b>? and b<?",
        "select * from `t1` where `a` = ? and `b` > ? and `b` < ?",
        &[("test", "t1")],
        0,
        false,
        true,
        true,
        &[1, 2],
        "",
    );
    write_packet(&mut client, 0, b"\x03execute sn using @a, @b");
    reader.set_sequence(1);
    assert_error_packet(&reader.read_packet().unwrap(), 8111, b"HY000");
    sync(&mut client, &mut reader);
    check(
        "execute sn using @a, @b",
        "execute `sn` using @a , @b",
        &[],
        0,
        true,
        true,
        false,
        &[1, 2],
        "",
    );
    // Binary writes and errors use the prepared text and original arguments.
    assert_eq!(
        execute_statement_packet_tags(&mut client, &mut reader, p4, &[3]),
        vec!["ok"]
    );
    sync(&mut client, &mut reader);
    check(
        "insert into t2 values(?)",
        "insert into `t2` values ( ? )",
        &[("test", "t2")],
        1,
        false,
        true,
        true,
        &[3],
        "",
    );
    assert_eq!(
        execute_statement_packet_tags(&mut client, &mut reader, p4, &[3]),
        vec!["error"]
    );
    sync(&mut client, &mut reader);
    assert_eq!(check("insert into t2 values(?)", "insert into `t2` values ( ? )", &[("test","t2")], 0, true, true, true, &[3], "").error.as_deref(),Some("insert into t2 values(?) [arguments: 3]: [kv:1062]Duplicate entry '3' for key 't2.PRIMARY'"));
    run_write(
        &mut client,
        &mut reader,
        "insert into t1 values(1, 10), (2, 20)",
    );
    sync(&mut client, &mut reader);
    check(
        "insert into t1 values(1, 10), (2, 20)",
        "insert into `t1` values ( ... )",
        &[("test", "t1")],
        2,
        false,
        false,
        false,
        &[],
        "",
    );
    write_packet(&mut client, 0, b"\x03select 1;select * from t1 where a > 1");
    reader.set_sequence(1);
    for _ in 0..2 {
        let packet = reader.read_packet().unwrap();
        assert_ne!(packet[0], 0xff);
        let columns = usize::from(packet[0]);
        for _ in 0..columns {
            reader.read_packet().unwrap();
        }
        loop {
            let packet = reader.read_packet().unwrap();
            if packet[0] == 0xfe && packet.len() < 13 {
                break;
            }
        }
    }
    sync(&mut client, &mut reader);
    {
        let mut records = records.lock().unwrap();
        assert_eq!(records.len(), 2);
        for (record, text, normalized, tables) in [
            (&records[0], "select 1;", "select ?", vec![]),
            (
                &records[1],
                "select * from t1 where a > 1",
                "select * from `t1` where `a` > ?",
                vec![("test".to_owned(), "t1".to_owned())],
            ),
        ] {
            assert_eq!(record.text, text);
            assert_eq!(record.normalized, normalized);
            assert_eq!(record.tables, tables);
            assert!(record.error.is_none());
            assert!(record.params.is_empty());
            assert_eq!(record.affected, 0);
        }
        records.clear();
    }
    for (sql, normalized, tables) in [
        (
            "create database db1",
            "create database `db1`",
            vec![("db1", "")],
        ),
        ("kill query 1", "kill query ?", vec![]),
        (
            "create placement policy p1 followers=1",
            "create placement policy `p1` followers = ?",
            vec![],
        ),
    ] {
        run_write(&mut client, &mut reader, sql);
        sync(&mut client, &mut reader);
        check(sql, normalized, &tables, 0, false, false, false, &[], "");
    }
    for (db, failed) in [("db1", false), ("noexistdb", true)] {
        let mut packet = vec![2];
        packet.extend_from_slice(db.as_bytes());
        write_packet(&mut client, 0, &packet);
        reader.set_sequence(1);
        let reply = reader.read_packet().unwrap();
        assert_eq!(reply[0] == 0xff, failed);
        sync(&mut client, &mut reader);
        check(
            &format!("use `{db}`"),
            &format!("use `{db}`"),
            &[(db, "")],
            0,
            failed,
            false,
            false,
            &[],
            "",
        );
    }
    run_write(
        &mut client,
        &mut reader,
        "set @@tidb_session_alias='alias123'",
    );
    sync(&mut client, &mut reader);
    check(
        "set @@tidb_session_alias='alias123'",
        "set @@tidb_session_alias = ?",
        &[],
        0,
        false,
        false,
        false,
        &[],
        "alias123",
    );
    run_query(&mut client, &mut reader, "select 123");
    sync(&mut client, &mut reader);
    check(
        "select 123",
        "select ?",
        &[],
        0,
        false,
        false,
        false,
        &[],
        "alias123",
    );
    run_write(&mut client, &mut reader, "set @@tidb_session_alias=''");
    sync(&mut client, &mut reader);
    check(
        "set @@tidb_session_alias=''",
        "set @@tidb_session_alias = ?",
        &[],
        0,
        false,
        false,
        false,
        &[],
        "",
    );
    run_query(&mut client, &mut reader, "select 123");
    sync(&mut client, &mut reader);
    check(
        "select 123",
        "select ?",
        &[],
        0,
        false,
        false,
        false,
        &[],
        "",
    );
    run_write(
        &mut client,
        &mut reader,
        "set @@tidb_session_alias='alias123'",
    );
    sync(&mut client, &mut reader);
    records.lock().unwrap().clear();
    // ConnReset opens a fresh session, preserves DB, and clears user variables and alias.
    write_packet(&mut client, 0, &[0x1f]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0);
    assert_eq!(
        run_query(&mut client, &mut reader, "select database(), @a"),
        vec![vec!["db1", "NULL"]]
    );
    sync(&mut client, &mut reader);
    assert_eq!(records.lock().unwrap().last().unwrap().alias, "");
    write_packet(&mut client, 0, &[1]);
    drop(reader);
    drop(client);
    let mut rejected = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(rejected.try_clone().unwrap());
    let reply = handshake_reply_as(&mut rejected, &mut reader, b"no_such_user", 0, 46);
    assert_error_packet(&reply, 1045, b"28000");
    drop(reader);
    drop(rejected);
    worker.join().unwrap();
    let connections = connections.lock().unwrap();
    assert_eq!(
        connections.iter().map(|(tp, _)| *tp).collect::<Vec<_>>(),
        [
            ConnEventTp::Connected,
            ConnEventTp::HandshakeAccepted,
            ConnEventTp::Reset,
            ConnEventTp::Disconnected,
            ConnEventTp::Connected,
            ConnEventTp::HandshakeRejected,
            ConnEventTp::Disconnected
        ]
    );
    assert_eq!(factory_calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(connections[1].1.connection_info.connection_type, "TCP");
    assert_eq!(
        connections[1].1.connection_info.server_port,
        i64::from(address.port())
    );
    assert_eq!(
        connections[1].1.connection_info.auth_method,
        "mysql_native_password"
    );
    assert!(connections[5]
        .1
        .error
        .as_ref()
        .unwrap()
        .contains("Access denied"));
}

/// Go `TestAuthPlugin` / `clientConn.authSwitchRequest` and `conn.AuthConn`, over TCP.
#[test]
fn test_auth_plugin_tcp_exchange() {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            tidb_extension::reset();
        }
    }
    let _reset = Reset;
    tidb_extension::reset();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let called = calls.clone();
    tidb_extension::register(
        "test",
        vec![tidb_extension::with_custom_auth_plugins(vec![
            tidb_extension::AuthPlugin {
                name: "authentication_test_plugin".into(),
                required_client_side_plugin: "mysql_native_password".into(),
                authenticate_user: Some(Arc::new(move |mut request| {
                    called.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    assert_eq!(request.salt.len(), 20);
                    assert!(request.conn_state.is_none());
                    assert_eq!(
                        request.stored_auth_string,
                        if request.user == "empty" {
                            ""
                        } else {
                            "encodedpassword"
                        }
                    );
                    if request.input_auth_string != b"1" {
                        return Err("private callback diagnostic".into());
                    }
                    let conn = request.auth_conn.as_mut().expect("real auth I/O");
                    conn.write_auth_more_data(b"challenge")?;
                    conn.flush()?;
                    if conn.read_packet()? != b"answer" {
                        return Err("wrong response".into());
                    }
                    Ok(())
                })),
                generate_auth_string: Some(Arc::new(|_| ("encodedpassword".into(), true))),
                validate_auth_string: Some(Arc::new(|hash| {
                    hash.is_empty() || hash == "encodedpassword"
                })),
                ..Default::default()
            },
        ])],
    )
    .unwrap();
    tidb_extension::setup().unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let worker = std::thread::spawn(move || {
        let store =
            ConfiguredUserStore::from_accounts(tidb_privilege::load::registry_from_user_rows([
                tidb_privilege::loaded::LoadedUser::new(
                    "u2",
                    "%",
                    "authentication_test_plugin",
                    "encodedpassword",
                ),
                tidb_privilege::loaded::LoadedUser::new(
                    "empty",
                    "%",
                    "authentication_test_plugin",
                    "",
                ),
            ]));
        let node = node_over(&store);
        let tracker = Arc::new(ConnectionTracker::default());
        let mut exits = Vec::new();
        for _ in 0..3 {
            let (stream, peer) = listener.accept().unwrap();
            exits.push(
                serve_mysql_connection(
                    stream,
                    peer,
                    ConnectionCancellation::default(),
                    node.factory().as_ref(),
                    &store,
                    &tracker,
                    DEFAULT_MAX_ALLOWED_PACKET,
                )
                .unwrap()
                .exit,
            );
        }
        exits
    });
    for (user, credential, accepted) in [
        ("u2", b"1".as_slice(), true),
        ("u2", b"2".as_slice(), false),
        ("empty", b"1".as_slice(), true),
    ] {
        let mut client = TcpStream::connect(address).unwrap();
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(60)))
            .unwrap();
        let mut reader = PacketReader::new(client.try_clone().unwrap());
        let switch = handshake_reply_as(&mut client, &mut reader, user.as_bytes(), 0, 46);
        assert_eq!(switch[0], 0xfe);
        assert!(switch[1..].starts_with(b"mysql_native_password\0"));
        write_packet(&mut client, 3, credential);
        reader.set_sequence(4);
        let reply = reader.read_packet().unwrap();
        if accepted {
            assert_eq!(reply, b"\x01challenge");
            write_packet(&mut client, 5, b"answer");
            reader.set_sequence(6);
            assert_eq!(reader.read_packet().unwrap()[0], 0);
            assert_eq!(
                run_query(&mut client, &mut reader, "select 1"),
                vec![vec!["1"]]
            );
            write_packet(&mut client, 0, &[1]);
        } else {
            assert_error_packet(&reply, 1045, b"28000");
            assert!(!String::from_utf8_lossy(&reply).contains("private callback diagnostic"));
        }
    }
    assert_eq!(
        worker.join().unwrap(),
        [
            ConnectionExit::Quit,
            ConnectionExit::AuthenticationRejected,
            ConnectionExit::Quit
        ]
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
}

/// Go `pkg/server/tests/commontest.TestExtensionConnEvent`.
#[test]
fn test_extension_conn_event() {
    use tidb_extension::ConnEventTp;
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            tidb_extension::reset();
        }
    }
    let _reset = Reset;
    tidb_extension::reset();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = events.clone();
    tidb_extension::register(
        "test",
        vec![tidb_extension::with_session_handler_factory(Arc::new(
            move || {
                let captured = captured.clone();
                Some(tidb_extension::SessionHandler {
                    on_connection_event: Some(Arc::new(move |tp, info| {
                        captured.lock().unwrap().push((tp, info.clone()))
                    })),
                    ..Default::default()
                })
            },
        ))],
    )
    .unwrap();
    tidb_extension::setup().unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let worker = std::thread::spawn(move || {
        let store = users();
        let node = node_over(&store);
        let tracker = Arc::new(ConnectionTracker::default());
        for _ in 0..2 {
            let (stream, peer) = listener.accept().unwrap();
            serve_mysql_connection(
                stream,
                peer,
                ConnectionCancellation::default(),
                node.factory().as_ref(),
                &store,
                &tracker,
                DEFAULT_MAX_ALLOWED_PACKET,
            )
            .unwrap();
        }
    });
    let handshake = |client: &mut TcpStream, reader: &mut PacketReader<TcpStream>, user: &str| {
        reader.set_sequence(0);
        let initial = reader.read_packet().unwrap();
        let salt = handshake_salt(&initial);
        let caps = CLIENT_PROTOCOL_41
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH
            | CLIENT_DEPRECATE_EOF
            | (1 << 3);
        let mut response = Vec::new();
        response.extend_from_slice(&caps.to_le_bytes());
        response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
        response.push(46);
        response.extend_from_slice(&[0; 23]);
        response.extend_from_slice(user.as_bytes());
        response.push(0);
        let auth = if user == "alice" {
            native_response(b"secret", &salt).to_vec()
        } else {
            Vec::new()
        };
        response.push(auth.len() as u8);
        response.extend_from_slice(&auth);
        response.extend_from_slice(b"test\0mysql_native_password\0");
        write_packet(client, 1, &response);
        reader.set_sequence(2);
        reader.read_packet().unwrap()
    };
    let mut client = TcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(60)))
        .unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    assert_eq!(handshake(&mut client, &mut reader, "alice")[0], 0);
    for sql in [
        "create role r1@'%'",
        "grant r1 to alice",
        "set role all",
        "set @@tidb_session_alias='alias123'",
    ] {
        run_write(&mut client, &mut reader, sql);
    }
    write_packet(&mut client, 0, &[1]);
    drop(client);
    drop(reader);
    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    assert_error_packet(
        &handshake(&mut client, &mut reader, "noexist"),
        1045,
        b"28000",
    );
    drop(client);
    drop(reader);
    worker.join().unwrap();
    let events = events.lock().unwrap();
    assert_eq!(
        events.iter().map(|(tp, _)| *tp).collect::<Vec<_>>(),
        [
            ConnEventTp::Connected,
            ConnEventTp::HandshakeAccepted,
            ConnEventTp::Disconnected,
            ConnEventTp::Connected,
            ConnEventTp::HandshakeRejected,
            ConnEventTp::Disconnected
        ]
    );
    for index in [0, 3] {
        let before = &events[index].1;
        assert_eq!(before.connection_info.client_ip, "127.0.0.1");
        assert_eq!(before.connection_info.server_ip, "127.0.0.1");
        assert!(before.connection_info.user.is_empty());
        assert!(before.connection_info.db.is_empty());
        assert_eq!(
            before.connection_info.server_port,
            i64::from(address.port())
        );
        assert_ne!(
            before.connection_info.client_port,
            address.port().to_string()
        );
        assert_ne!(before.connection_info.connection_id, 0);
        assert!(before.active_roles.is_empty());
        assert!(before.error.is_none());
        assert!(before.session_alias.is_empty());
        let mut expected = before.connection_info.clone();
        expected.user = if index == 0 { "alice" } else { "noexist" }.into();
        expected.db = "test".into();
        assert_eq!(events[index + 1].1.connection_info, expected);
        assert!(events[index + 1].1.active_roles.is_empty());
        assert_eq!(events[index + 2].1.connection_info, expected);
        assert!(events[index + 2].1.error.is_none());
    }
    assert!(events[1].1.error.is_none());
    assert!(events[1].1.session_alias.is_empty());
    assert_eq!(
        events[2].1.active_roles,
        vec![tidb_parser::auth::RoleIdentity {
            username: "r1".into(),
            hostname: "%".into()
        }]
    );
    assert_eq!(events[2].1.session_alias, "alias123");
    assert_eq!(
        events[4].1.error.as_deref(),
        Some("[server:1045]Access denied for user 'noexist'@'127.0.0.1' (using password: NO)")
    );
    assert!(events[5].1.session_alias.is_empty());
}
