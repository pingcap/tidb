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

//! The first connected transport-to-session read-only path.
//!
//! This is intentionally source-shaped rather than a claim that the server is
//! complete: it proves packet framing, COM_QUERY extraction, SQL parsing,
//! shared-session catalog execution, and DistSQL request metadata in one local
//! seam. Authentication, compression, command routing, result encoding, and
//! TiKV RPC remain explicit follow-up contracts.

use super::*;

fn frame_query(sql: &str) -> Vec<u8> {
    let mut framed = Vec::new();
    let mut writer = tidb_protocol::PacketWriter::new(&mut framed);
    let mut payload = vec![0x03];
    payload.extend_from_slice(sql.as_bytes());
    writer.write_packet(&payload).expect("frame query");
    writer.flush().expect("flush query");
    framed
}

fn run_query(
    session: &mut Session,
    request: &mut tidb_distsql::DistSqlContext,
    sql: &str,
) -> Result<Outcome, ExecError> {
    session.execute_framed_query(&frame_query(sql), request)
}

#[test]
fn framed_com_query_reaches_shared_read_only_session() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    let mut request = tidb_distsql::DistSqlContext::new();

    assert_eq!(
        run_query(
            &mut session,
            &mut request,
            "create table framed_read (id int primary key, value int)",
        ),
        Ok(Outcome::Done)
    );
    assert_eq!(
        request.request.original_sql,
        "create table framed_read (id int primary key, value int)"
    );
    assert_eq!(
        run_query(
            &mut session,
            &mut request,
            "insert into framed_read values (1, 7)",
        ),
        Ok(Outcome::Done)
    );
    assert_eq!(
        run_query(
            &mut session,
            &mut request,
            "select value from framed_read where id = 1",
        ),
        Ok(Outcome::Rows(ResultSet {
            rows: vec![vec![tidb_datatype::Datum::Int(7)]],
            ordered: false,
        }))
    );
}

#[test]
fn malformed_or_non_query_frames_fail_before_catalog_mutation() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    let mut request = tidb_distsql::DistSqlContext::new();
    assert_eq!(
        run_query(
            &mut session,
            &mut request,
            "create table framed_gate (id int)"
        ),
        Ok(Outcome::Done)
    );
    let version_before = cluster.catalog_version();

    let mut ping = Vec::new();
    let mut writer = tidb_protocol::PacketWriter::new(&mut ping);
    writer.write_packet(&[0x0e]).expect("frame ping");
    writer.flush().expect("flush ping");
    assert_eq!(
        session.execute_framed_query(&ping, &mut request),
        Err(ExecError::Unsupported("COM_QUERY"))
    );
    assert_eq!(cluster.catalog_version(), version_before);

    let mut trailing_packet = frame_query("create table must_not_commit (id int)");
    trailing_packet.extend_from_slice(&frame_query("select 1"));
    assert!(matches!(
        session.execute_framed_query(&trailing_packet, &mut request),
        Err(ExecError::Protocol(_))
    ));
    assert_eq!(cluster.catalog_version(), version_before);

    // A truncated header is rejected by the packet reader before a command
    // byte can be interpreted or request metadata can be changed.
    assert!(matches!(
        session.execute_framed_query(&[0x03], &mut request),
        Err(ExecError::Protocol(_))
    ));
    assert_eq!(cluster.catalog_version(), version_before);
    assert_eq!(
        session.execute_sql("select id from framed_gate"),
        Ok(Outcome::Rows(ResultSet {
            rows: Vec::new(),
            ordered: false,
        }))
    );
}

#[test]
fn invalid_query_payload_does_not_replace_request_sql() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    let mut request = tidb_distsql::DistSqlContext::new();
    request.request.original_sql = "previous query".to_string();

    let mut framed = Vec::new();
    let mut writer = tidb_protocol::PacketWriter::new(&mut framed);
    writer
        .write_packet(&[0x03, 0xff])
        .expect("frame invalid utf8");
    writer.flush().expect("flush invalid utf8");
    assert!(matches!(
        session.execute_framed_query(&framed, &mut request),
        Err(ExecError::Protocol(_))
    ));
    assert_eq!(request.request.original_sql, "previous query");
    assert_eq!(cluster.catalog_version(), 0);
}

#[test]
fn framed_query_text_rows_encode_after_session_execution() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    let mut request = tidb_distsql::DistSqlContext::new();

    let encoded = session
        .execute_framed_query_text_rows(&frame_query("select 7"), &mut request)
        .expect("encode row packet");
    let mut reader = tidb_protocol::PacketReader::new(std::io::Cursor::new(encoded));
    assert_eq!(reader.read_packet().expect("row packet"), vec![0x01, b'7']);
    assert_eq!(request.request.original_sql, "select 7");

    let encoded_null = session
        .execute_framed_query_text_rows(&frame_query("select null"), &mut request)
        .expect("encode null row packet");
    let mut null_reader = tidb_protocol::PacketReader::new(std::io::Cursor::new(encoded_null));
    assert_eq!(
        null_reader.read_packet().expect("null row packet"),
        vec![0xfb]
    );

    assert!(session
        .execute_framed_query_text_rows(
            &frame_query("insert into missing values (1)"),
            &mut request
        )
        .is_err());
}

#[test]
fn framed_query_text_result_set_encodes_metadata_rows_and_terminal_eof() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    let mut request = tidb_distsql::DistSqlContext::new();
    let columns = [tidb_protocol::ColumnInfo {
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
    }];
    let encoded = session
        .execute_framed_query_text_result_set(
            &frame_query("select 7"),
            &mut request,
            &columns,
            tidb_protocol::ResultSetOptions {
                status_flags: 2,
                ..tidb_protocol::ResultSetOptions::default()
            },
        )
        .expect("encode complete result set");

    let mut reader = tidb_protocol::PacketReader::new(std::io::Cursor::new(encoded));
    assert_eq!(reader.read_packet().expect("column count"), vec![0x01]);
    let metadata = reader.read_packet().expect("column metadata");
    assert_eq!(metadata[0], 0x03); // length-encoded "def" catalog
    assert_eq!(
        reader.read_packet().expect("metadata EOF"),
        vec![0xfe, 0x00, 0x00, 0x02, 0x00]
    );
    assert_eq!(reader.read_packet().expect("text row"), vec![0x01, b'7']);
    assert_eq!(
        reader.read_packet().expect("terminal EOF"),
        vec![0xfe, 0x00, 0x00, 0x02, 0x00]
    );
    assert_eq!(request.request.original_sql, "select 7");
}

#[test]
fn framed_query_text_result_set_uses_source_typed_scalar_formatting() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    let mut request = tidb_distsql::DistSqlContext::new();
    let columns = [
        tidb_protocol::ColumnInfo {
            schema: String::new(),
            table: String::new(),
            org_table: String::new(),
            name: "decimal_value".to_owned(),
            org_name: "decimal_value".to_owned(),
            column_length: 20,
            charset: tidb_protocol::DEFAULT_COLLATION_ID,
            flag: 0,
            decimal: 2,
            type_code: tidb_protocol::TYPE_NEW_DECIMAL,
            default_value: None,
        },
        tidb_protocol::ColumnInfo {
            schema: String::new(),
            table: String::new(),
            org_table: String::new(),
            name: "real_value".to_owned(),
            org_name: "real_value".to_owned(),
            column_length: 20,
            charset: tidb_protocol::DEFAULT_COLLATION_ID,
            flag: 0,
            decimal: tidb_protocol::NOT_FIXED_DECIMAL,
            type_code: tidb_protocol::TYPE_DOUBLE,
            default_value: None,
        },
        tidb_protocol::ColumnInfo {
            schema: String::new(),
            table: String::new(),
            org_table: String::new(),
            name: "bytes_value".to_owned(),
            org_name: "bytes_value".to_owned(),
            column_length: 20,
            charset: tidb_protocol::BINARY_DEFAULT_COLLATION_ID,
            flag: tidb_protocol::BINARY_FLAG,
            decimal: tidb_protocol::NOT_FIXED_DECIMAL,
            type_code: tidb_protocol::TYPE_VAR_STRING,
            default_value: None,
        },
    ];
    let encoded = session
        .execute_framed_query_text_result_set(
            &frame_query("select 1.20, 1.2e0, 'raw'"),
            &mut request,
            &columns,
            tidb_protocol::ResultSetOptions::default(),
        )
        .expect("typed scalar result set");
    let mut reader = tidb_protocol::PacketReader::new(std::io::Cursor::new(encoded));
    assert_eq!(reader.read_packet().expect("column count"), vec![0x03]);
    for _ in &columns {
        reader.read_packet().expect("column metadata");
    }
    reader.read_packet().expect("metadata EOF");
    assert_eq!(
        reader.read_packet().expect("typed row"),
        vec![0x04, b'1', b'.', b'2', b'0', 0x03, b'1', b'.', b'2', 0x03, b'r', b'a', b'w']
    );
}

#[test]
fn framed_query_text_result_set_encodes_ok_for_done_statements() {
    let cluster = Cluster::new();
    let mut session = cluster.session();
    let mut request = tidb_distsql::DistSqlContext::new();
    assert_eq!(
        run_query(
            &mut session,
            &mut request,
            "create table framed_ok (id int)",
        ),
        Ok(Outcome::Done)
    );

    let encoded = session
        .execute_framed_query_text_result_set(
            &frame_query("insert into framed_ok values (1)"),
            &mut request,
            &[],
            tidb_protocol::ResultSetOptions {
                status_flags: 2,
                ..tidb_protocol::ResultSetOptions::default()
            },
        )
        .expect("encode OK packet");
    let mut reader = tidb_protocol::PacketReader::new(std::io::Cursor::new(encoded));
    assert_eq!(
        reader.read_packet().expect("OK packet"),
        vec![0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00]
    );
    assert_eq!(
        request.request.original_sql,
        "insert into framed_ok values (1)"
    );
}
