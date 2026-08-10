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
use tidb_protocol::{
    CompressionAlgorithm, PacketIoReader, PacketIoWriter, PacketReader, PacketWriter, COM_PING,
    COM_QUERY, COM_QUIT, DEFAULT_MAX_ALLOWED_PACKET,
};
use tidb_server::handshake::CLIENT_ZSTD_COMPRESSION_ALGORITHM;
use tidb_server::{
    serve_mysql_connection, ConfiguredUserStore, ConnectionCancellation, ConnectionReport,
    ConnectionTracker, MysqlConnectionError, PipelineSessionFactory,
};

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
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let tracker = Arc::new(ConnectionTracker::default());
    let worker = std::thread::spawn(move || {
        let (stream, peer_addr) = listener.accept().unwrap();
        let store = users();
        serve_mysql_connection(
            stream,
            peer_addr,
            ConnectionCancellation::default(),
            &PipelineSessionFactory::with_accounts(store.accounts()),
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
    let mut writer = PacketWriter::with_sequence(stream, sequence);
    writer.write_packet(payload).unwrap();
    writer.flush().unwrap();
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
        writer.write_packet(&[COM_PING]).unwrap();
        writer.flush().unwrap();
        reader.set_sequence(1);
        reader.set_compressed_sequence(writer.compressed_sequence().unwrap());
        let response = reader.read_packet().unwrap();
        assert_eq!(response[0], 0, "compressed COM_PING OK");
        writer.set_compressed_sequence(reader.compressed_sequence().unwrap());
    }

    let mut query = vec![COM_QUERY];
    query.extend_from_slice(b"SELECT 1");
    writer.set_sequence(0);
    writer.write_packet(&query).unwrap();
    writer.flush().unwrap();
    reader.set_sequence(1);
    reader.set_compressed_sequence(writer.compressed_sequence().unwrap());
    assert_eq!(reader.read_packet().unwrap(), [1], "one result column");
    let definition = reader.read_packet().unwrap();
    assert!(!definition.is_empty(), "column definition");
    assert_eq!(reader.read_packet().unwrap(), [1, b'1'], "one text row");
    assert_eq!(reader.read_packet().unwrap()[0], 0xfe, "result terminator");
    writer.set_compressed_sequence(reader.compressed_sequence().unwrap());

    writer.set_sequence(0);
    writer.write_packet(&[COM_QUIT]).unwrap();
    writer.flush().unwrap();
    let report = worker.join().unwrap().unwrap();
    assert_eq!(report.queries, 1);
}

#[test]
fn live_commands_use_negotiated_zlib_and_one_outer_sequence() {
    run_compressed_ping_pair(CompressionAlgorithm::Zlib, CLIENT_COMPRESS);
}

#[test]
fn live_commands_use_negotiated_zstd_and_one_outer_sequence() {
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
    write_raw_packet(&mut client, 0, &[COM_PING]);
    reader.set_sequence(1);
    let timed_out = reader.read_packet().is_err();
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
