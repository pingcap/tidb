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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(missing_docs)]

use tidb_protocol::resultset_stream::{
    ResultSetStream, ResultSetStreamError, ResultSetStreamState,
};
use tidb_protocol::{ColumnInfo, ResultSetOptions, TYPE_VAR_STRING};

fn column() -> ColumnInfo {
    ColumnInfo {
        schema: "test".to_owned(),
        table: "t".to_owned(),
        org_table: "t".to_owned(),
        name: "a".to_owned(),
        org_name: "a".to_owned(),
        column_length: 32,
        charset: 46,
        flag: 0,
        decimal: 0,
        type_code: TYPE_VAR_STRING,
        default_value: None,
    }
}

#[test]
fn incremental_sequence_matches_write_chunks_without_row_buffering() {
    let mut stream = ResultSetStream::new(
        vec![column()],
        ResultSetOptions {
            status_flags: 2,
            ..ResultSetOptions::default()
        },
    );
    assert_eq!(stream.state(), ResultSetStreamState::Initial);
    let metadata = stream.metadata_packets().unwrap();
    assert_eq!(metadata.len(), 3);
    assert_eq!(metadata[0], vec![1]);
    assert_eq!(metadata[2], vec![0xfe, 0, 0, 2, 0]);
    assert_eq!(
        stream.row_packet(&[Some(b"one".to_vec())]).unwrap(),
        b"\x03one".to_vec()
    );
    assert_eq!(stream.row_packet(&[None]).unwrap(), vec![0xfb]);
    assert_eq!(stream.row_count(), 2);
    assert_eq!(stream.finish_packet().unwrap(), metadata[2]);
    assert_eq!(stream.state(), ResultSetStreamState::Finished);
}

#[test]
fn deprecate_eof_skips_metadata_eof_but_keeps_ok_shaped_terminal() {
    let mut stream = ResultSetStream::new(
        vec![column()],
        ResultSetOptions {
            status_flags: 2,
            deprecate_eof: true,
            ..ResultSetOptions::default()
        },
    );
    assert_eq!(stream.metadata_packets().unwrap().len(), 2);
    assert_eq!(
        stream.finish_packet().unwrap(),
        vec![0xfe, 0, 0, 2, 0, 0, 0]
    );
}

#[test]
fn lifecycle_and_row_width_are_checked_at_the_incremental_boundary() {
    let mut stream = ResultSetStream::new(vec![column()], ResultSetOptions::default());
    assert!(matches!(
        stream.row_packet(&[Some(b"x".to_vec())]),
        Err(ResultSetStreamError::InvalidState { .. })
    ));
    stream.metadata_packets().unwrap();
    assert_eq!(
        stream.row_packet(&[]),
        Err(ResultSetStreamError::RowColumnCount {
            row: 0,
            expected: 1,
            actual: 0,
        })
    );
    stream.finish_packet().unwrap();
    assert!(matches!(
        stream.finish_packet(),
        Err(ResultSetStreamError::InvalidState { .. })
    ));
}
