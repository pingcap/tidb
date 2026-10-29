// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(missing_docs)]

use sha1::{Digest, Sha1};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tidb_protocol::CompressionAlgorithm;
use tidb_protocol::PacketIoReader;
use tidb_protocol::PacketIoWriter;
use tidb_protocol::PacketReader;
use tidb_protocol::PacketWriter;
use tidb_server::command::COM_PING;
use tidb_server::command::COM_QUERY;
use tidb_server::command::COM_QUIT;
use tidb_server::command::COM_REFRESH;
use tidb_protocol::DEFAULT_MAX_ALLOWED_PACKET;
use tidb_server::handshake::CLIENT_ZSTD_COMPRESSION_ALGORITHM;
use tidb_server::{
    serve_mysql_connection, ConfiguredUserStore, ConnectionCancellation, ConnectionReport,
    ConnectionTracker, MysqlConnectionError,
};
use tidb_session::domain::mock_store::MockStoreDomain;

/// Go's server over a mock store whose node authenticates against, and keeps
/// its accounts in, `store`: connections are served from its session factory.
fn node_over(store: &ConfiguredUserStore) -> std::sync::Arc<MockStoreDomain> {
    MockStoreDomain::create_with_users(std::sync::Arc::new(store.clone()))
        .expect("boot the server's store")
}

const CLIENT_COMPRESS: u32 = 1 << 5;
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

fn start_server() -> (
    SocketAddr,
    JoinHandle<Result<ConnectionReport, MysqlConnectionError>>,
) {
    start_server_with(users())
}

/// [`start_server`] whose node keeps its accounts in `store`.
fn start_server_with(
    store: ConfiguredUserStore,
) -> (
    SocketAddr,
    JoinHandle<Result<ConnectionReport, MysqlConnectionError>>,
) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            node_over(&store).factory().as_ref(),
            &store,
            &tracker,
            DEFAULT_MAX_ALLOWED_PACKET,
        )
    });
    (address, worker)
}

fn handshake_salt_and_capabilities(initial: &[u8]) -> ([u8; 20], u32) {
    assert_eq!(initial[0], 10);
    let version_end = initial[1..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| offset + 1)
        .unwrap();
    let first_salt = version_end + 1 + 4;
    let lower_capability = first_salt + 8 + 1;
    let upper_capability = lower_capability + 2 + 1 + 2;
    let second_salt = upper_capability + 2 + 1 + 10;
    let mut salt = [0; 20];
    salt[..8].copy_from_slice(&initial[first_salt..first_salt + 8]);
    salt[8..].copy_from_slice(&initial[second_salt..second_salt + 12]);
    let low = u16::from_le_bytes(
        initial[lower_capability..lower_capability + 2]
            .try_into()
            .unwrap(),
    );
    let high = u16::from_le_bytes(
        initial[upper_capability..upper_capability + 2]
            .try_into()
            .unwrap(),
    );
    (salt, u32::from(low) | (u32::from(high) << 16))
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

fn write_raw_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) {
    try_write_raw_packet(stream, sequence, payload).expect("the packet is written");
}

/// The same write, but reporting a failure instead of panicking.
///
/// A connection the SERVER has already closed fails the write with
/// `BrokenPipe` rather than letting it through to a failing read, and
/// which side notices first is a TCP timing detail. A test asserting that
/// a connection closed must accept either.
fn try_write_raw_packet(
    stream: &mut TcpStream,
    sequence: u8,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut writer = PacketWriter::with_sequence(stream, sequence);
    writer
        .write_packet(payload)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    writer
        .flush()
        .map_err(|error| std::io::Error::other(error.to_string()))
}

fn authenticate(
    client: &mut TcpStream,
    raw_reader: &mut PacketReader<TcpStream>,
    compression_capability: u32,
    zstd_level: u8,
) -> u32 {
    raw_reader.set_sequence(0);
    let initial = raw_reader.read_packet().unwrap();
    let (salt, server_capabilities) = handshake_salt_and_capabilities(&initial);
    let capabilities = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH
        | CLIENT_CONNECT_ATTRS
        | CLIENT_DEPRECATE_EOF
        | compression_capability;
    let mut response = Vec::new();
    response.extend_from_slice(&capabilities.to_le_bytes());
    response.extend_from_slice(&(DEFAULT_MAX_ALLOWED_PACKET as u32).to_le_bytes());
    response.push(46);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(b"alice\0");
    let auth = native_response(b"secret", &salt);
    response.push(u8::try_from(auth.len()).unwrap());
    response.extend_from_slice(&auth);
    response.extend_from_slice(b"mysql_native_password\0");
    response.push(0);
    if compression_capability & CLIENT_ZSTD_COMPRESSION_ALGORITHM != 0 {
        response.push(zstd_level);
    }
    write_raw_packet(client, 1, &response);
    raw_reader.set_sequence(2);
    assert_eq!(raw_reader.read_packet().unwrap()[0], 0, "auth OK");
    server_capabilities
}

fn run_compressed_ping_pair(algorithm: CompressionAlgorithm, capability: u32) {
    let (address, worker) = start_server();
    let mut client = TcpStream::connect(address).unwrap();
    let mut raw_reader = PacketReader::new(client.try_clone().unwrap());
    let server_capabilities = authenticate(&mut client, &mut raw_reader, capability, 3);
    if server_capabilities & capability == 0 {
        write_raw_packet(&mut client, 0, &[COM_QUIT]);
        let report = worker.join().unwrap().unwrap();
        assert_eq!(report.queries, 0);
        panic!("server did not advertise negotiated compression capability 0x{capability:08x}");
    }

    let mut writer = PacketIoWriter::new(client.try_clone().unwrap(), algorithm).unwrap();
    writer.set_zstd_level(3);
    let mut reader = PacketIoReader::new(client.try_clone().unwrap(), algorithm).unwrap();
    for _ in 0..2 {
        writer.set_sequence(0);
        writer.set_compressed_sequence(0);
        writer.write_packet(&[COM_PING]).unwrap();
        writer.flush().unwrap();
        reader.set_sequence(1);
        reader.set_compressed_sequence(writer.compressed_sequence().unwrap());
        let response = reader.read_packet().unwrap();
        assert_eq!(response[0], 0, "compressed COM_PING OK");
    }

    let mut query = vec![COM_QUERY];
    query.extend_from_slice(b"SELECT 1");
    writer.set_sequence(0);
    writer.set_compressed_sequence(0);
    writer.write_packet(&query).unwrap();
    writer.flush().unwrap();
    reader.set_sequence(1);
    reader.set_compressed_sequence(writer.compressed_sequence().unwrap());
    assert_eq!(reader.read_packet().unwrap(), [1], "one result column");
    let definition = reader.read_packet().unwrap();
    assert!(!definition.is_empty(), "column definition");
    assert_eq!(reader.read_packet().unwrap(), [1, b'1'], "one text row");
    assert_eq!(reader.read_packet().unwrap()[0], 0xfe, "result terminator");

    writer.set_sequence(0);
    writer.set_compressed_sequence(0);
    writer.write_packet(&[COM_QUIT]).unwrap();
    writer.flush().unwrap();
    let report = worker.join().unwrap().unwrap();
    assert_eq!(report.queries, 1);
}

#[test]
fn live_commands_reset_negotiated_zlib_sequence_for_each_command() {
    run_compressed_ping_pair(CompressionAlgorithm::Zlib, CLIENT_COMPRESS);
}

#[test]
fn live_commands_reset_negotiated_zstd_sequence_for_each_command() {
    run_compressed_ping_pair(
        CompressionAlgorithm::Zstd,
        CLIENT_ZSTD_COMPRESSION_ALGORITHM,
    );
}

#[test]
fn live_commands_prefer_zlib_when_both_compression_bits_are_set() {
    run_compressed_ping_pair(
        CompressionAlgorithm::Zlib,
        CLIENT_COMPRESS | CLIENT_ZSTD_COMPRESSION_ALGORITHM,
    );
}

#[test]
fn com_refresh_privileges_keeps_go_two_ok_response_shape() {
    // `FLUSH PRIVILEGES` needs RELOAD (Go `buildFlush`).
    let store = users();
    store.accounts().grant("alice", "%", tidb_mysql::ReloadPriv.0);
    let (address, worker) = start_server_with(store);
    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader, 0, 0);

    // Go's `handleRefresh(0x01)` runs `FLUSH PRIVILEGES` through
    // `handleQuery` (which writes one OK) and then writes its own OK as the
    // command response. Clients therefore see two consecutive OK packets;
    // consuming only one leaves the second packet queued for the next
    // command and desynchronizes the connection.
    write_raw_packet(&mut client, 0, &[COM_REFRESH, 0x01]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0, "flush query OK");
    assert_eq!(reader.read_packet().unwrap()[0], 0, "refresh command OK");

    // Other refresh targets are Go's successful no-op and emit only the
    // command-level OK.
    write_raw_packet(&mut client, 0, &[COM_REFRESH, 0x02]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0, "refresh no-op OK");

    write_raw_packet(&mut client, 0, &[COM_QUIT]);
    let report = worker.join().unwrap().unwrap();
    assert_eq!(report.queries, 1, "FLUSH PRIVILEGES runs one SQL query");
}

/// Go's `handleRefresh(0x01)` returns `handleQuery`'s error: without RELOAD
/// the `FLUSH PRIVILEGES` it runs is refused (1227), and that ERR is the
/// command's only response.
#[test]
fn com_refresh_privileges_without_reload_is_refused_like_go() {
    let (address, worker) = start_server();
    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader, 0, 0);

    write_raw_packet(&mut client, 0, &[COM_REFRESH, 0x01]);
    reader.set_sequence(1);
    let packet = reader.read_packet().unwrap();
    assert_eq!(packet[0], 0xff, "ERR packet: {packet:?}");
    assert_eq!(u16::from_le_bytes([packet[1], packet[2]]), 1227);

    write_raw_packet(&mut client, 0, &[COM_QUIT]);
    worker.join().unwrap().unwrap();
}

#[test]
fn session_wait_timeout_closes_the_next_idle_command_read() {
    let (address, worker) = start_server();
    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader, 0, 0);

    let mut command = vec![COM_QUERY];
    command.extend_from_slice(b"SET @@wait_timeout = 1");
    write_raw_packet(&mut client, 0, &command);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0, "SET OK");

    std::thread::sleep(Duration::from_millis(1_300));
    // The close can surface on either side of the exchange: the write of
    // this PING may already hit a closed socket, or it may land and the
    // read that follows return nothing. Both are the timeout doing its
    // job, so both count.
    let write_failed = try_write_raw_packet(&mut client, 0, &[COM_PING]).is_err();
    reader.set_sequence(1);
    let timed_out = write_failed || reader.read_packet().is_err();
    if !timed_out {
        write_raw_packet(&mut client, 0, &[COM_QUIT]);
    }
    let _ = worker.join().unwrap();
    assert!(
        timed_out,
        "the session wait_timeout must close an idle command read"
    );
}

#[test]
fn zero_wait_timeout_clears_the_previous_command_deadline() {
    let (address, worker) = start_server();
    let mut client = TcpStream::connect(address).unwrap();
    let mut reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut reader, 0, 0);

    for sql in ["SET @@wait_timeout = 1", "SET @@wait_timeout = 0"] {
        let mut command = vec![COM_QUERY];
        command.extend_from_slice(sql.as_bytes());
        write_raw_packet(&mut client, 0, &command);
        reader.set_sequence(1);
        assert_eq!(reader.read_packet().unwrap()[0], 0, "SET OK");
    }

    std::thread::sleep(Duration::from_millis(1_300));
    write_raw_packet(&mut client, 0, &[COM_PING]);
    reader.set_sequence(1);
    assert_eq!(reader.read_packet().unwrap()[0], 0, "COM_PING remains live");
    write_raw_packet(&mut client, 0, &[COM_QUIT]);
    assert!(worker.join().unwrap().is_ok());
}

/// Go `compressionStats` over the wire: the session records the codec the
/// handshake negotiated (`SetCompressionAlgorithm`) and the zstd level the
/// client asked for (`SetCompressionLevel`); zlib reports its default level.
fn compression_statuses(algorithm: CompressionAlgorithm, capability: u32) -> Vec<(String, String)> {
    let (address, worker) = start_server();
    let mut client = TcpStream::connect(address).unwrap();
    let mut raw_reader = PacketReader::new(client.try_clone().unwrap());
    authenticate(&mut client, &mut raw_reader, capability, 3);
    let mut writer = PacketIoWriter::new(client.try_clone().unwrap(), algorithm).unwrap();
    writer.set_zstd_level(3);
    let mut reader = PacketIoReader::new(client.try_clone().unwrap(), algorithm).unwrap();

    let mut query = vec![COM_QUERY];
    query.extend_from_slice(b"SHOW STATUS LIKE 'Compression%'");
    writer.set_sequence(0);
    writer.set_compressed_sequence(0);
    writer.write_packet(&query).unwrap();
    writer.flush().unwrap();
    reader.set_sequence(1);
    reader.set_compressed_sequence(writer.compressed_sequence().unwrap());
    assert_eq!(reader.read_packet().unwrap(), [2], "two result columns");
    reader.read_packet().unwrap();
    reader.read_packet().unwrap();
    let mut rows = Vec::new();
    loop {
        let packet = reader.read_packet().unwrap();
        if packet[0] == 0xfe {
            break;
        }
        // Two length-encoded strings, each shorter than 251 bytes.
        let name_len = usize::from(packet[0]);
        let name = String::from_utf8(packet[1..=name_len].to_vec()).unwrap();
        let value_len = usize::from(packet[name_len + 1]);
        let value =
            String::from_utf8(packet[name_len + 2..name_len + 2 + value_len].to_vec()).unwrap();
        rows.push((name, value));
    }

    writer.set_sequence(0);
    writer.set_compressed_sequence(0);
    writer.write_packet(&[COM_QUIT]).unwrap();
    writer.flush().unwrap();
    worker.join().unwrap().unwrap();
    rows
}

#[test]
fn a_compressed_connection_reports_its_compression_like_go() {
    let row = |name: &str, value: &str| (name.to_owned(), value.to_owned());
    assert_eq!(
        compression_statuses(CompressionAlgorithm::Zlib, CLIENT_COMPRESS),
        [
            row("Compression", "ON"),
            row("Compression_algorithm", "zlib"),
            row("Compression_level", "6"),
        ]
    );
    assert_eq!(
        compression_statuses(CompressionAlgorithm::Zstd, CLIENT_ZSTD_COMPRESSION_ALGORITHM),
        [
            row("Compression", "ON"),
            row("Compression_algorithm", "zstd"),
            row("Compression_level", "3"),
        ]
    );
}
