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

//! Live Session status publication into MySQL OK and EOF response frames.

use std::io::Cursor;

use tidb_exec::Cluster;
use tidb_protocol::{ColumnInfo, PacketReader, ResultSetOptions, COM_QUERY, TYPE_LONGLONG};
use tidb_server::{Connection, FramedResponse};

fn frame_query(sql: &[u8]) -> Vec<u8> {
    let mut payload = vec![COM_QUERY];
    payload.extend_from_slice(sql);
    let mut framed = Vec::with_capacity(payload.len() + 4);
    let length = payload.len() as u32;
    framed.extend_from_slice(&length.to_le_bytes()[..3]);
    framed.push(0);
    framed.extend_from_slice(&payload);
    framed
}

fn response_payloads(framed: &[u8]) -> Vec<Vec<u8>> {
    let mut reader = PacketReader::new(Cursor::new(framed));
    reader.set_sequence(1);
    let mut payloads = Vec::new();
    while reader.get_ref().position() < reader.get_ref().get_ref().len() as u64 {
        payloads.push(reader.read_packet().expect("framed response packet"));
    }
    payloads
}

fn integer_column() -> ColumnInfo {
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
        type_code: TYPE_LONGLONG,
        default_value: None,
    }
}

#[test]
fn query_eof_uses_published_warning_count_not_caller_metadata() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    let response = connection
        .dispatch_framed(
            &frame_query(b"select 7"),
            &[integer_column()],
            ResultSetOptions {
                status_flags: 2,
                warnings: 9,
                ..ResultSetOptions::default()
            },
        )
        .expect("query response");
    let FramedResponse::Packets(framed) = response else {
        panic!("query must return packets")
    };
    let payloads = response_payloads(&framed);

    assert_eq!(payloads[2], vec![0xfe, 0x00, 0x00, 0x02, 0x00]);
    assert_eq!(payloads[4], vec![0xfe, 0x00, 0x00, 0x02, 0x00]);
}

#[test]
fn query_preserves_status_and_negotiated_eof_capabilities() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    let response = connection
        .dispatch_framed(
            &frame_query(b"select 7"),
            &[integer_column()],
            ResultSetOptions {
                status_flags: 0x1234,
                warnings: 9,
                deprecate_eof: true,
                protocol_41: true,
            },
        )
        .expect("deprecate-EOF query response");
    let FramedResponse::Packets(framed) = response else {
        panic!("query must return packets")
    };
    let payloads = response_payloads(&framed);

    assert_eq!(payloads.len(), 4, "deprecate-EOF omits metadata EOF");
    assert_eq!(payloads[3], vec![0xfe, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00]);

    let response = connection
        .dispatch_framed(
            &frame_query(b"select 8"),
            &[integer_column()],
            ResultSetOptions {
                status_flags: 0x5678,
                warnings: 9,
                deprecate_eof: false,
                protocol_41: false,
            },
        )
        .expect("legacy query response");
    let FramedResponse::Packets(framed) = response else {
        panic!("query must return packets")
    };
    let payloads = response_payloads(&framed);

    assert_eq!(payloads.len(), 5);
    assert_eq!(payloads[2], vec![0xfe]);
    assert_eq!(payloads[4], vec![0xfe]);
}

#[test]
fn done_statement_ok_uses_published_affected_rows() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    connection
        .dispatch_framed(
            &frame_query(b"create table status_publish (id int)"),
            &[],
            ResultSetOptions::default(),
        )
        .expect("create response");
    let response = connection
        .dispatch_framed(
            &frame_query(b"insert into status_publish values (7)"),
            &[],
            ResultSetOptions {
                status_flags: 2,
                warnings: 9,
                ..ResultSetOptions::default()
            },
        )
        .expect("insert response");
    let FramedResponse::Packets(framed) = response else {
        panic!("insert must return packets")
    };

    assert_eq!(
        response_payloads(&framed),
        vec![vec![0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00]]
    );
}

#[test]
fn noop_warn_mode_publishes_ordered_session_warnings_and_packet_counts() {
    const MESSAGE: &str = "function READ ONLY has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions";

    let cluster = Cluster::new();
    let mut session = cluster.session();
    session
        .execute_sql("set tidb_enable_noop_functions = warn")
        .expect("enable WARN mode");
    assert!(session.statement_status().previous().warnings.is_empty());

    session
        .execute_sql("set tx_read_only = 1, tx_read_only = 0, transaction_read_only = 1")
        .expect("accepted no-op assignments");
    let published = session.statement_status().previous();
    assert_eq!(published.warnings.len(), 2);
    assert_eq!(published.warnings[0].message, MESSAGE);
    assert_eq!(published.warnings[1].message, MESSAGE);

    session
        .execute_sql("set transaction_read_only = 0")
        .expect("next statement resets warnings");
    assert!(session.statement_status().previous().warnings.is_empty());

    let mut connection = Connection::new(&cluster);
    connection
        .dispatch_framed(
            &frame_query(b"set tidb_enable_noop_functions = warn"),
            &[],
            ResultSetOptions {
                status_flags: 2,
                warnings: 9,
                ..ResultSetOptions::default()
            },
        )
        .expect("framed WARN mode");
    let response = connection
        .dispatch_framed(
            &frame_query(b"set tx_read_only = 1, tx_read_only = 0, transaction_read_only = 1"),
            &[],
            ResultSetOptions {
                status_flags: 2,
                warnings: 9,
                ..ResultSetOptions::default()
            },
        )
        .expect("framed warning-producing assignments");
    let FramedResponse::Packets(framed) = response else {
        panic!("SET must return packets")
    };
    assert_eq!(
        response_payloads(&framed),
        vec![vec![0x00, 0x00, 0x00, 0x02, 0x00, 0x02, 0x00]]
    );

    let response = connection
        .dispatch_framed(
            &frame_query(b"set transaction_read_only = 0"),
            &[],
            ResultSetOptions {
                status_flags: 2,
                warnings: 9,
                ..ResultSetOptions::default()
            },
        )
        .expect("framed warning reset");
    let FramedResponse::Packets(framed) = response else {
        panic!("SET must return packets")
    };
    assert_eq!(
        response_payloads(&framed),
        vec![vec![0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]]
    );
}
