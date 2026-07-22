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

#![allow(missing_docs)]

use std::collections::HashMap;
use std::io::Cursor;

use tidb_exec::Cluster;
use tidb_protocol::{
    CompressedReader, CompressionAlgorithm, PacketError, PacketIoReader, PacketIoWriter,
    ResultSetOptions,
};
use tidb_server::handshake::CLIENT_ZSTD_COMPRESSION_ALGORITHM;
use tidb_server::{
    AuthHandshakeRequest, CommandIoOutcome, CompressedCommandIo, Connection, HandshakeResponse41,
    NegotiatedCompression, CLIENT_COMPRESS,
};

fn handshake_request(capability: u32, zstd_level: i32) -> AuthHandshakeRequest {
    AuthHandshakeRequest {
        response: HandshakeResponse41 {
            attrs: HashMap::new(),
            user: "root".to_owned(),
            db_name: String::new(),
            auth_plugin: "mysql_native_password".to_owned(),
            auth: Vec::new(),
            zstd_level,
            capability,
            collation: 45,
        },
        negotiated_capability: capability,
        raw_packet: Vec::new(),
        server_auth_plugin: "mysql_native_password".to_owned(),
    }
}

fn encoded_command(algorithm: CompressionAlgorithm, zstd_level: i32, command: &[u8]) -> Vec<u8> {
    let mut writer = PacketIoWriter::new(Vec::new(), algorithm).expect("command writer");
    writer.set_zstd_level(zstd_level);
    writer.write_packet(command).expect("command packet");
    writer.flush().expect("command flush");
    writer.into_inner()
}

fn response_payloads(algorithm: CompressionAlgorithm, response: &[u8]) -> Vec<Vec<u8>> {
    let mut reader =
        PacketIoReader::new(Cursor::new(response), algorithm).expect("response reader");
    reader.set_sequence(1);
    let mut payloads = Vec::new();
    loop {
        match reader.read_packet() {
            Ok(payload) => payloads.push(payload),
            Err(PacketError::EndOfStream) => break,
            Err(error) => panic!("response packet: {error}"),
        }
    }
    payloads
}

fn first_inner_sequence(algorithm: CompressionAlgorithm, response: &[u8]) -> u8 {
    assert_eq!(response[3], 0, "first compressed envelope sequence");
    let mut reader =
        CompressedReader::new(Cursor::new(response), algorithm).expect("compressed reader");
    let mut header = [0; 4];
    let mut position = 0;
    while position < header.len() {
        position += reader
            .read_bytes(&mut header[position..])
            .expect("inner packet header");
    }
    header[3]
}

#[test]
fn handshake_compression_state_preserves_source_selection_and_level() {
    let none = NegotiatedCompression::from_handshake(&handshake_request(0, 0));
    assert_eq!(none.algorithm(), CompressionAlgorithm::None);
    assert_eq!(none.zstd_level(), 0);

    let zstd = NegotiatedCompression::from_handshake(&handshake_request(
        CLIENT_ZSTD_COMPRESSION_ALGORITHM,
        7,
    ));
    assert_eq!(zstd.algorithm(), CompressionAlgorithm::Zstd);
    assert_eq!(zstd.zstd_level(), 7);

    let both = NegotiatedCompression::from_handshake(&handshake_request(
        CLIENT_COMPRESS | CLIENT_ZSTD_COMPRESSION_ALGORITHM,
        11,
    ));
    assert_eq!(both.algorithm(), CompressionAlgorithm::Zlib);
    assert_eq!(both.zstd_level(), 11);
}

#[test]
fn zlib_query_crosses_packet_io_dispatch_and_response_flush() {
    let request = handshake_request(CLIENT_COMPRESS, 0);
    let input = encoded_command(
        CompressionAlgorithm::Zlib,
        0,
        &[
            tidb_protocol::COM_QUERY,
            b's',
            b'e',
            b'l',
            b'e',
            b'c',
            b't',
            b' ',
            b'7',
        ],
    );
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    let mut io = CompressedCommandIo::from_handshake(Cursor::new(input), Vec::new(), &request)
        .expect("negotiated command I/O");

    assert_eq!(
        io.dispatch_next(&mut connection, ResultSetOptions::default())
            .expect("query dispatch"),
        CommandIoOutcome::ResponseWritten(5)
    );
    assert_eq!(connection.request().request.original_sql, "select 7");

    assert_eq!(
        first_inner_sequence(CompressionAlgorithm::Zlib, io.writer_ref()),
        1
    );
    let payloads = response_payloads(CompressionAlgorithm::Zlib, io.writer_ref());
    assert_eq!(payloads.len(), 5);
    assert_eq!(payloads[0], vec![0x01]);
    assert_eq!(payloads[3], vec![0x01, b'7']);
}

#[test]
fn zstd_ping_uses_requested_level_and_sequence_one_response() {
    let request = handshake_request(CLIENT_ZSTD_COMPRESSION_ALGORITHM, 1);
    let input = encoded_command(CompressionAlgorithm::Zstd, 1, &[tidb_protocol::COM_PING]);
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    let mut io = CompressedCommandIo::from_handshake(Cursor::new(input), Vec::new(), &request)
        .expect("negotiated command I/O");

    assert_eq!(io.compression().zstd_level(), 1);
    assert_eq!(
        io.dispatch_next(
            &mut connection,
            ResultSetOptions {
                status_flags: 2,
                warnings: 3,
                ..ResultSetOptions::default()
            },
        )
        .expect("ping dispatch"),
        CommandIoOutcome::ResponseWritten(1)
    );
    assert_eq!(
        first_inner_sequence(CompressionAlgorithm::Zstd, io.writer_ref()),
        1
    );
    assert_eq!(
        response_payloads(CompressionAlgorithm::Zstd, io.writer_ref()),
        vec![vec![0x00, 0x00, 0x00, 0x02, 0x00, 0x03, 0x00]]
    );
}

#[test]
fn uncompressed_quit_closes_without_writing_or_flushing_an_envelope() {
    let request = handshake_request(0, 0);
    let input = encoded_command(CompressionAlgorithm::None, 0, &[tidb_protocol::COM_QUIT]);
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    let mut io = CompressedCommandIo::from_handshake(Cursor::new(input), Vec::new(), &request)
        .expect("uncompressed command I/O");

    assert_eq!(
        io.dispatch_next(&mut connection, ResultSetOptions::default())
            .expect("quit dispatch"),
        CommandIoOutcome::Quit
    );
    assert!(connection.is_closed());
    assert!(io.writer_ref().is_empty());
}
