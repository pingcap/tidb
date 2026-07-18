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
use tidb_protocol::{
    PacketReader, PacketWriter, ResultSetOptions, BINARY_FLAG, COM_QUERY, TYPE_LONGLONG,
};
use tidb_server::{Connection, DispatchError, FramedResponse};

fn frame_query(sql: &str) -> Vec<u8> {
    let mut payload = vec![COM_QUERY];
    payload.extend_from_slice(sql.as_bytes());
    let mut framed = Vec::new();
    let mut writer = PacketWriter::new(&mut framed);
    writer.write_packet(&payload).expect("frame query");
    writer.flush().expect("flush query");
    framed
}

fn response_payloads(framed: &[u8]) -> Vec<Vec<u8>> {
    let mut reader = PacketReader::new(Cursor::new(framed));
    reader.set_sequence(1);
    let mut payloads = Vec::new();
    while reader.get_ref().position() < framed.len() as u64 {
        payloads.push(reader.read_packet().expect("response packet"));
    }
    payloads
}

fn decoded_column_wire(payload: &[u8]) -> (u16, u32, u8, u16, u8) {
    let mut offset = 0;
    for _ in 0..6 {
        let length = usize::from(payload[offset]);
        assert!(length < 0xfb, "test column strings use one-byte lengths");
        offset += 1 + length;
    }
    assert_eq!(payload[offset], 0x0c);
    offset += 1;
    let charset = u16::from_le_bytes([payload[offset], payload[offset + 1]]);
    offset += 2;
    let column_length = u32::from_le_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]);
    offset += 4;
    let type_code = payload[offset];
    offset += 1;
    let flags = u16::from_le_bytes([payload[offset], payload[offset + 1]]);
    offset += 2;
    let decimal = payload[offset];
    (charset, column_length, type_code, flags, decimal)
}

fn decoded_column_strings(payload: &[u8]) -> Vec<&[u8]> {
    let mut offset = 0;
    (0..6)
        .map(|_| {
            let length = usize::from(payload[offset]);
            assert!(length < 0xfb, "test column strings use one-byte lengths");
            offset += 1;
            let end = offset + length;
            let value = &payload[offset..end];
            offset = end;
            value
        })
        .collect()
}

#[test]
fn tableless_query_derives_columns_and_encodes_rows_end_to_end() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    let response = connection
        .dispatch_framed_auto(
            &frame_query("SELECT 7, 'hello' AS greeting"),
            ResultSetOptions::default(),
        )
        .expect("automatic result response");
    let FramedResponse::Packets(framed) = response else {
        panic!("query must return packets");
    };
    let payloads = response_payloads(&framed);
    assert_eq!(payloads[0], vec![0x02]);
    assert_eq!(&payloads[1][..4], &[0x03, b'd', b'e', b'f']);
    assert_eq!(&payloads[2][..4], &[0x03, b'd', b'e', b'f']);
    assert_eq!(
        payloads[4],
        vec![0x01, b'7', 0x05, b'h', b'e', b'l', b'l', b'o']
    );
    assert_eq!(
        connection.request().request.original_sql,
        "SELECT 7, 'hello' AS greeting"
    );
}

#[test]
fn tableless_count_crosses_com_query_with_automatic_metadata() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    let response = connection
        .dispatch_framed_auto(
            &frame_query("SELECT COUNT(*) AS counted"),
            ResultSetOptions::default(),
        )
        .expect("automatic COUNT response");
    let FramedResponse::Packets(framed) = response else {
        panic!("COUNT query must return packets");
    };
    let payloads = response_payloads(&framed);
    assert_eq!(payloads[0], vec![0x01]);
    assert!(payloads[1]
        .windows(b"counted".len())
        .any(|window| window == b"counted"));
    assert_eq!(
        decoded_column_wire(&payloads[1]),
        (
            63,
            21,
            TYPE_LONGLONG,
            BINARY_FLAG | tidb_exec::NOT_NULL_FLAG,
            0,
        )
    );
    assert_eq!(payloads[3], vec![0x01, b'1']);
    assert_eq!(
        connection.request().request.original_sql,
        "SELECT COUNT(*) AS counted"
    );
}

#[test]
fn catalog_count_column_crosses_com_query_with_bound_fixed_metadata() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    for sql in [
        "CREATE TABLE count_input (a INT, b INT)",
        "INSERT INTO count_input VALUES (1, 10), (NULL, 20), (2, 30)",
    ] {
        connection
            .dispatch_framed(&frame_query(sql), &[], ResultSetOptions::default())
            .expect("COUNT(column) setup statement");
    }

    let response = connection
        .dispatch_framed_auto(
            &frame_query("SELECT COUNT(t.a) AS c FROM count_input AS t"),
            ResultSetOptions::default(),
        )
        .expect("bound catalog COUNT(column) response");
    let FramedResponse::Packets(framed) = response else {
        panic!("COUNT(column) query must return packets");
    };
    let payloads = response_payloads(&framed);
    assert_eq!(payloads[0], vec![0x01]);
    assert_eq!(
        decoded_column_strings(&payloads[1]),
        [b"def".as_slice(), b"", b"", b"", b"c", b""]
    );
    assert_eq!(
        decoded_column_wire(&payloads[1]),
        (
            63,
            21,
            TYPE_LONGLONG,
            BINARY_FLAG | tidb_exec::NOT_NULL_FLAG,
            0,
        )
    );
    assert_eq!(payloads[3], vec![0x01, b'2']);

    connection
        .dispatch_framed(
            &frame_query("CREATE TABLE empty_count_input (a INT)"),
            &[],
            ResultSetOptions::default(),
        )
        .expect("empty COUNT(column) setup statement");
    let response = connection
        .dispatch_framed_auto(
            &frame_query("SELECT COUNT(e.a) AS c FROM empty_count_input AS e"),
            ResultSetOptions::default(),
        )
        .expect("empty catalog COUNT(column) response");
    let FramedResponse::Packets(framed) = response else {
        panic!("empty COUNT(column) query must return packets");
    };
    assert_eq!(response_payloads(&framed)[3], vec![0x01, b'0']);
}

#[test]
fn catalog_count_column_auto_response_fails_closed_around_bounded_shape() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    for sql in [
        "CREATE TABLE count_guard (a INT, b INT)",
        "CREATE TABLE count_guard_other (a INT)",
    ] {
        connection
            .dispatch_framed(&frame_query(sql), &[], ResultSetOptions::default())
            .expect("COUNT(column) guard setup statement");
    }

    for sql in [
        "SELECT COUNT(*) AS c FROM count_guard AS t",
        "SELECT COUNT(DISTINCT t.a) AS c FROM count_guard AS t",
        "SELECT COUNT(t.a) FROM count_guard AS t",
        "SELECT COUNT(a) AS c FROM count_guard AS t",
        "SELECT COUNT(x.a) AS c FROM count_guard AS t",
        "SELECT COUNT(t.missing) AS c FROM count_guard AS t",
        "SELECT COUNT(t.a) AS c, t.b FROM count_guard AS t",
        "SELECT COUNT(t.a + 1) AS c FROM count_guard AS t",
        "SELECT SUM(t.a) AS c FROM count_guard AS t",
        "SELECT COUNT(t.a) AS c FROM count_guard AS t JOIN count_guard_other AS u ON t.a = u.a",
        "SELECT COUNT(t.a) AS c FROM count_guard AS t WHERE t.a > 0",
        "SELECT COUNT(t.a) AS c FROM count_guard AS t GROUP BY t.b",
        "SELECT COUNT(t.a) AS c FROM count_guard AS t HAVING c > 0",
        "SELECT COUNT(t.a) OVER () AS c FROM count_guard AS t",
        "SELECT COUNT(t.a) AS c FROM count_guard AS t ORDER BY c",
        "SELECT COUNT(t.a) AS c FROM count_guard AS t LIMIT 1",
        "SELECT COUNT(t.a) AS c FROM otherdb.count_guard AS t",
        "SELECT COUNT(t.a + (@count_wire_leak := 1)) AS c FROM count_guard AS t",
    ] {
        let error = connection
            .dispatch_framed_auto(&frame_query(sql), ResultSetOptions::default())
            .expect_err("shape outside bounded COUNT(column) must fail closed");
        assert!(
            matches!(error, DispatchError::AutomaticResultMetadata(_)),
            "unexpected error for {sql}: {error:?}"
        );
    }
}

#[test]
fn automatic_metadata_rejects_schema_dependent_query_without_guessing() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    let error = connection
        .dispatch_framed_auto(
            &frame_query("SELECT value FROM t"),
            ResultSetOptions::default(),
        )
        .expect_err("FROM must require catalog metadata");
    assert!(matches!(error, DispatchError::AutomaticResultMetadata(_)));
}

#[test]
fn catalog_query_derives_declared_columns_and_rows_end_to_end() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    for sql in [
        "CREATE TABLE users (id INT, name VARCHAR(8))",
        "INSERT INTO users VALUES (7, 'hello')",
    ] {
        connection
            .dispatch_framed(&frame_query(sql), &[], ResultSetOptions::default())
            .expect("catalog setup statement");
    }

    let response = connection
        .dispatch_framed_auto(
            &frame_query("SELECT u.name AS display_name, u.id FROM users AS u"),
            ResultSetOptions::default(),
        )
        .expect("catalog-backed automatic result response");
    let FramedResponse::Packets(framed) = response else {
        panic!("query must return packets");
    };
    let payloads = response_payloads(&framed);
    assert_eq!(payloads[0], vec![0x02]);
    assert!(payloads[1]
        .windows(12)
        .any(|window| window == b"display_name"));
    assert!(payloads[2].windows(2).any(|window| window == b"id"));
    assert!(payloads
        .iter()
        .any(|payload| payload.windows(5).any(|window| window == b"hello")));
}

#[test]
fn catalog_join_query_derives_both_declared_schemas_end_to_end() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    for sql in [
        "CREATE TABLE users (id INT, name VARCHAR(8))",
        "CREATE TABLE orders (user_id INT, total INT)",
        "INSERT INTO users VALUES (7, 'hello')",
        "INSERT INTO orders VALUES (7, 42)",
    ] {
        connection
            .dispatch_framed(&frame_query(sql), &[], ResultSetOptions::default())
            .expect("catalog join setup statement");
    }

    let response = connection
        .dispatch_framed_auto(
            &frame_query(
                "SELECT u.name AS display_name, o.total FROM users AS u JOIN orders AS o ON u.id = o.user_id",
            ),
            ResultSetOptions::default(),
        )
        .expect("catalog-backed automatic join response");
    let FramedResponse::Packets(framed) = response else {
        panic!("query must return packets");
    };
    let payloads = response_payloads(&framed);
    assert_eq!(payloads[0], vec![0x02]);
    assert!(payloads[1]
        .windows(12)
        .any(|window| window == b"display_name"));
    assert!(payloads[2].windows(5).any(|window| window == b"total"));
    assert!(payloads
        .iter()
        .any(|payload| payload.windows(5).any(|window| window == b"hello")));
    assert!(payloads
        .iter()
        .any(|payload| payload.windows(2).any(|window| window == b"42")));
}

#[test]
fn automatic_metadata_uses_planner_using_output_and_coalesced_rows() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    for sql in [
        "CREATE TABLE users (id INT, name VARCHAR(8))",
        "CREATE TABLE orders (id INT, total INT)",
        "INSERT INTO users VALUES (7, 'hello')",
        "INSERT INTO orders VALUES (7, 42)",
    ] {
        connection
            .dispatch_framed(&frame_query(sql), &[], ResultSetOptions::default())
            .expect("catalog setup statement");
    }
    let response = connection
        .dispatch_framed_auto(
            &frame_query("SELECT * FROM users u JOIN orders o USING (id)"),
            ResultSetOptions::default(),
        )
        .expect("USING output metadata and rows");
    let FramedResponse::Packets(framed) = response else {
        panic!("query must return packets");
    };
    let payloads = response_payloads(&framed);
    assert_eq!(payloads[0], vec![0x03]);
    assert!(payloads.iter().any(|payload| {
        payload
            == &[
                0x01, b'7', 0x05, b'h', b'e', b'l', b'l', b'o', 0x02, b'4', b'2',
            ]
    }));
}

#[test]
fn automatic_metadata_uses_nullable_left_output_and_null_extended_rows() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    for sql in [
        "CREATE TABLE users (id INT)",
        "CREATE TABLE orders (id INT)",
        "INSERT INTO users VALUES (7)",
    ] {
        connection
            .dispatch_framed(&frame_query(sql), &[], ResultSetOptions::default())
            .expect("catalog setup statement");
    }
    let response = connection
        .dispatch_framed_auto(
            &frame_query("SELECT * FROM users u LEFT JOIN orders o ON u.id = o.id"),
            ResultSetOptions::default(),
        )
        .expect("LEFT output metadata and null extension");
    let FramedResponse::Packets(framed) = response else {
        panic!("query must return packets");
    };
    let payloads = response_payloads(&framed);
    assert_eq!(payloads[0], vec![0x02]);
    assert!(payloads
        .iter()
        .any(|payload| payload == &[0x01, b'7', 0xfb]));
}

#[test]
fn automatic_metadata_projects_left_join_columns_without_row_width_drift() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    for sql in [
        "CREATE TABLE users (id INT)",
        "CREATE TABLE orders (id INT)",
        "INSERT INTO users VALUES (7)",
    ] {
        connection
            .dispatch_framed(&frame_query(sql), &[], ResultSetOptions::default())
            .expect("catalog setup statement");
    }
    let response = connection
        .dispatch_framed_auto(
            &frame_query(
                "SELECT u.id AS user_id, o.id AS order_id FROM users u LEFT JOIN orders o ON u.id = o.id",
            ),
            ResultSetOptions::default(),
        )
        .expect("LEFT direct-column projection metadata and rows");
    let FramedResponse::Packets(framed) = response else {
        panic!("query must return packets");
    };
    let payloads = response_payloads(&framed);
    assert_eq!(payloads[0], vec![0x02]);
    assert!(payloads
        .iter()
        .any(|payload| payload == &[0x01, b'7', 0xfb]));
}

#[test]
fn automatic_protocol_projects_qualified_right_using_column() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    for sql in [
        "CREATE TABLE users (id INT)",
        "CREATE TABLE orders (id INT NOT NULL)",
        "INSERT INTO users VALUES (7), (8)",
        "INSERT INTO orders VALUES (7)",
    ] {
        connection
            .dispatch_framed(&frame_query(sql), &[], ResultSetOptions::default())
            .expect("catalog setup statement");
    }
    let response = connection
        .dispatch_framed_auto(
            &frame_query(
                "SELECT o.id AS order_id FROM users u LEFT JOIN orders o USING (id) ORDER BY u.id",
            ),
            ResultSetOptions::default(),
        )
        .expect("qualified right USING column metadata and rows");
    let FramedResponse::Packets(framed) = response else {
        panic!("query must return packets");
    };
    let payloads = response_payloads(&framed);
    assert_eq!(payloads[0], vec![0x01]);

    let metadata = decoded_column_strings(&payloads[1]);
    assert_eq!(metadata[0], b"def");
    assert_eq!(metadata[1], b"");
    assert_eq!(metadata[2], b"o");
    assert_eq!(metadata[3], b"orders");
    assert_eq!(metadata[4], b"order_id");
    assert_eq!(metadata[5], b"id");
    assert_eq!(
        decoded_column_wire(&payloads[1]).3 & tidb_exec::NOT_NULL_FLAG,
        0
    );

    assert_eq!(payloads[3], vec![0x01, b'7']);
    assert_eq!(payloads[4], vec![0xfb]);
}

#[test]
fn automatic_protocol_frames_right_and_natural_join_rows() {
    let cluster = Cluster::new();
    let mut connection = Connection::new(&cluster);
    for sql in [
        "CREATE TABLE join_left (z INT, id INT, left_only INT)",
        "CREATE TABLE join_outer (id INT, z INT, right_only INT)",
        "INSERT INTO join_left VALUES (10, 1, 100)",
        "INSERT INTO join_outer VALUES (1, 10, 200), (2, 20, 300)",
    ] {
        connection
            .dispatch_framed(&frame_query(sql), &[], ResultSetOptions::default())
            .expect("catalog setup statement");
    }

    for sql in [
        "SELECT * FROM join_left l RIGHT JOIN join_outer r USING (z, id)",
        "SELECT * FROM join_left l NATURAL RIGHT JOIN join_outer r",
    ] {
        let response = connection
            .dispatch_framed_auto(&frame_query(sql), ResultSetOptions::default())
            .expect("automatic RIGHT/NATURAL response");
        let FramedResponse::Packets(framed) = response else {
            panic!("query must return packets")
        };
        let payloads = response_payloads(&framed);
        assert_eq!(payloads[0], vec![0x04], "SQL: {sql}");
        assert_eq!(decoded_column_strings(&payloads[1])[4], b"id");
        assert_eq!(decoded_column_strings(&payloads[2])[4], b"z");
        assert_eq!(decoded_column_strings(&payloads[3])[4], b"right_only");
        assert_eq!(decoded_column_strings(&payloads[4])[4], b"left_only");
        assert!(payloads.iter().any(|payload| {
            payload
                == &[
                    0x01, b'1', 0x02, b'1', b'0', 0x03, b'2', b'0', b'0', 0x03, b'1', b'0', b'0',
                ]
        }));
        assert!(payloads.iter().any(|payload| {
            payload == &[0x01, b'2', 0x02, b'2', b'0', 0x03, b'3', b'0', b'0', 0xfb]
        }));
    }
}

#[test]
fn framed_query_command_stays_at_sequence_zero_before_dispatch() {
    let framed = frame_query("SELECT 1");
    let mut reader = PacketReader::new(Cursor::new(framed));
    assert_eq!(reader.sequence(), 0);
    assert_eq!(reader.read_packet().expect("command packet")[0], COM_QUERY);
    assert_eq!(reader.sequence(), 1);
}
