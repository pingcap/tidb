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

use tidb_exec::{ExecError, RenderedExecError, StatementKind, StatementStatus};
use tidb_protocol::{PacketReader, ERR_HEADER};
use tidb_server::{
    frame_execution_error_response, frame_rendered_error_response, Connection, DispatchError,
    FramedResponse,
};

fn response_payload(framed: &[u8]) -> Vec<u8> {
    let mut reader = PacketReader::new(Cursor::new(framed));
    reader.set_sequence(1);
    let payload = reader.read_packet().expect("ERR response packet");
    assert_eq!(reader.get_ref().position(), framed.len() as u64);
    payload
}

#[test]
fn execution_error_response_matches_write_error_protocol_41_order() {
    // Source: pkg/server/conn.go:1725-1768. The server receives the
    // source-rendered message from session/error context; it does not derive
    // one from ExecError here.
    let error = ExecError::UnknownColumn("missing_name".to_owned());
    let response = frame_execution_error_response(&error, [b'm', 0xff, b's', b'g'], true)
        .expect("frame execution error");
    let FramedResponse::Packets(framed) = response else {
        panic!("execution error must produce one packet response");
    };

    assert_eq!(framed[3], 1, "server response starts at sequence one");
    let payload = response_payload(&framed);
    assert_eq!(
        payload,
        [ERR_HEADER, 0x1e, 0x04, b'#', b'4', b'2', b'S', b'2', b'2', b'm', 0xff, b's', b'g']
    );
}

#[test]
fn execution_error_response_omits_state_for_legacy_client_without_losing_bytes() {
    let error = ExecError::Parse {
        message: "near FROM".to_owned(),
        offset: 7,
    };
    let response = frame_execution_error_response(&error, [b'?', 0x00, 0x80], false)
        .expect("frame legacy execution error");
    let FramedResponse::Packets(framed) = response else {
        panic!("execution error must produce one packet response");
    };

    assert_eq!(framed[..4], [6, 0, 0, 1]);
    assert_eq!(
        response_payload(&framed),
        [ERR_HEADER, 0x28, 0x04, b'?', 0x00, 0x80]
    );
}

#[test]
fn rendered_error_response_consumes_attached_status_without_wire_guessing() {
    // Source: pkg/server/conn.go:1338-1345 and 1725-1768.  The statement
    // context owns rendering and status; the connection writer emits only
    // the ERR fields and leaves warnings/info for their own response paths.
    let mut status = StatementStatus::default();
    status.begin_statement(StatementKind::Dml);
    status.warn("retained warning");
    let published = status.finish_statement();
    let rendered = RenderedExecError::with_status(
        &ExecError::UnknownTable("app.orders".to_owned()),
        [b'T', 0xff, b'!'],
        &published,
    );
    assert_eq!(rendered.status(), Some(&published));

    let response =
        frame_rendered_error_response(&rendered, true).expect("frame attached rendered error");
    let FramedResponse::Packets(framed) = response else {
        panic!("execution error must produce one packet response");
    };
    assert_eq!(
        response_payload(&framed),
        [ERR_HEADER, 0x7a, 0x04, b'#', b'4', b'2', b'S', b'0', b'2', b'T', 0xff, b'!',]
    );
}

#[test]
fn connection_error_frame_uses_failed_session_status_and_source_bytes() {
    // Source: pkg/server/conn.go:1338-1345,1725-1768. The connection first
    // dispatches the statement (publishing ExecSuccess=false), then the
    // caller supplies the already-rendered bytes for the ERR writer.
    let mut connection = Connection::new(&tidb_exec::Cluster::new());
    let error = match connection.dispatch(&[
        tidb_protocol::COM_QUERY,
        b'S',
        b'E',
        b'L',
        b'E',
        b'C',
        b'T',
        b' ',
        b'*',
    ]) {
        Err(DispatchError::Execution(error)) => error,
        other => panic!("expected execution error, got {other:?}"),
    };
    let response = connection
        .frame_execution_error(&error, [b'x', 0xff, b'!'], true)
        .expect("frame connection error");
    let FramedResponse::Packets(framed) = response else {
        panic!("execution error must produce one packet response");
    };
    let payload = response_payload(&framed);
    assert_eq!(payload[0], ERR_HEADER);
    assert_eq!(&payload[3..9], b"#HY000");
    assert_eq!(&payload[9..], &[b'x', 0xff, b'!']);
}
