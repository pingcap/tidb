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

use std::io::Cursor;

use tidb_exec::Cluster;
use tidb_protocol::{ColumnInfo, PacketReader, PacketWriter, ResultSetOptions};
use tidb_server::{Connection, DispatchError, FramedResponse};

fn frame_command(code: u8, payload: &[u8]) -> Vec<u8> {
    let mut command = Vec::with_capacity(payload.len() + 1);
    command.push(code);
    command.extend_from_slice(payload);
    let mut framed = Vec::new();
    let mut writer = PacketWriter::new(&mut framed);
    writer.write_packet(&command).expect("frame command");
    writer.flush().expect("flush command");
    framed
}

fn response_payloads(framed: &[u8]) -> Vec<Vec<u8>> {
    let mut reader = PacketReader::new(Cursor::new(framed));
    reader.set_sequence(1);
    let mut payloads = Vec::new();
    loop {
        let cursor = reader.get_ref();
        if cursor.position() == cursor.get_ref().len() as u64 {
            break;
        }
        payloads.push(reader.read_packet().expect("response packet"));
    }
    payloads
}

fn response_sequences(framed: &[u8]) -> Vec<u8> {
    let mut sequences = Vec::new();
    let mut offset = 0;
    while offset < framed.len() {
        let payload_len = usize::from(framed[offset])
            | (usize::from(framed[offset + 1]) << 8)
            | (usize::from(framed[offset + 2]) << 16);
        sequences.push(framed[offset + 3]);
        offset += 4 + payload_len;
    }
    assert_eq!(offset, framed.len(), "response has a partial packet header");
    sequences
}

fn query_column() -> ColumnInfo {
    ColumnInfo {
        schema: String::new(),
        table: String::new(),
        org_table: String::new(),
        name: "value".to_owned(),
        org_name: "value".to_owned(),
        column_length: 20,
        charset: tidb_protocol::DEFAULT_COLLATION_ID,
        flag: 0,
        decimal: 0,
        type_code: tidb_protocol::TYPE_LONGLONG,
        default_value: None,
    }
}

#[test]
fn framed_query_returns_source_ordered_metadata_rows_and_eof() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    let response = connection
        .dispatch_framed(
            &frame_command(tidb_protocol::COM_QUERY, b"select 7"),
            &[query_column()],
            ResultSetOptions {
                status_flags: 2,
                warnings: 3,
                ..ResultSetOptions::default()
            },
        )
        .expect("query response");
    let FramedResponse::Packets(framed) = response else {
        panic!("query must return packets");
    };
    assert_eq!(response_sequences(&framed), vec![1, 2, 3, 4, 5]);

    let payloads = response_payloads(&framed);
    assert_eq!(payloads.len(), 5);
    assert_eq!(payloads[0], vec![0x01]);
    assert_eq!(payloads[1][..4], [0x03, b'd', b'e', b'f']);
    // COM_QUERY warnings come from the status published by the executed
    // statement, not caller-supplied response options.
    assert_eq!(payloads[2], vec![0xfe, 0x00, 0x00, 0x02, 0x00]);
    assert_eq!(payloads[3], vec![0x01, b'7']);
    assert_eq!(payloads[4], vec![0xfe, 0x00, 0x00, 0x02, 0x00]);
    assert_eq!(connection.request().request.original_sql, "select 7");
}

#[test]
fn framed_ping_returns_one_ok_packet_at_server_sequence_one() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    let response = connection
        .dispatch_framed(
            &frame_command(tidb_protocol::COM_PING, &[]),
            &[],
            ResultSetOptions {
                status_flags: 2,
                warnings: 3,
                ..ResultSetOptions::default()
            },
        )
        .expect("ping response");
    let FramedResponse::Packets(framed) = response else {
        panic!("ping must return packets");
    };
    assert_eq!(response_sequences(&framed), vec![1]);
    assert_eq!(
        response_payloads(&framed),
        vec![vec![0x00, 0x00, 0x00, 0x02, 0x00, 0x03, 0x00]]
    );
}

#[test]
fn framed_quit_closes_without_response_and_rejects_later_commands() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    assert_eq!(
        connection.dispatch_framed(
            &frame_command(tidb_protocol::COM_QUIT, &[]),
            &[],
            ResultSetOptions::default(),
        ),
        Ok(FramedResponse::Quit)
    );
    assert!(connection.is_closed());
    assert_eq!(
        connection.dispatch_framed(
            &frame_command(tidb_protocol::COM_PING, &[]),
            &[],
            ResultSetOptions::default(),
        ),
        Err(DispatchError::ConnectionClosed)
    );
}

#[test]
fn framed_dispatch_rejects_malformed_unsupported_and_invalid_commands() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    let options = ResultSetOptions::default();
    assert!(matches!(
        connection.dispatch_framed(&[], &[], options),
        Err(DispatchError::MalformedCommand(_))
    ));

    let bad_sequence = vec![0x01, 0x00, 0x00, 0x07, tidb_protocol::COM_PING];
    assert!(matches!(
        connection.dispatch_framed(&bad_sequence, &[], options),
        Err(DispatchError::MalformedCommand(_))
    ));

    let unsupported = frame_command(tidb_protocol::COM_INIT_DB, b"test");
    assert_eq!(
        connection.dispatch_framed(&unsupported, &[], options),
        Err(DispatchError::UnsupportedCommand(
            tidb_protocol::COM_INIT_DB
        ))
    );

    let invalid_utf8 = frame_command(tidb_protocol::COM_QUERY, &[0xff]);
    assert_eq!(
        connection.dispatch_framed(&invalid_utf8, &[], options),
        Err(DispatchError::InvalidQueryUtf8)
    );

    let mut trailing = frame_command(tidb_protocol::COM_PING, &[]);
    trailing.extend_from_slice(&frame_command(tidb_protocol::COM_PING, &[]));
    assert!(matches!(
        connection.dispatch_framed(&trailing, &[], options),
        Err(DispatchError::MalformedCommand(_))
    ));
}
