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

use std::collections::VecDeque;

use tidb_datatype::Datum;
use tidb_protocol::{ColumnInfo, ResultSetOptions, TYPE_LONG};
use tidb_server::connection_resultset::write_connection_result_set_to_sink;
use tidb_server::resultset_source::ResultSetSource;
use tidb_server::resultset_writer::{write_result_set, ResultSetSink, SinkWriteError};

#[derive(Default)]
struct Source {
    events: VecDeque<Result<Vec<Vec<Datum>>, String>>,
    log: Vec<&'static str>,
    columns_calls: usize,
    finish_error: Option<String>,
    close_calls: usize,
}

impl ResultSetSource for Source {
    fn next_batch(&mut self, _: usize) -> Result<Vec<Vec<Datum>>, String> {
        self.log.push("next");
        self.events.pop_front().unwrap_or(Ok(Vec::new()))
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
        self.log.push("columns");
        self.columns_calls += 1;
        Ok(vec![column()])
    }

    fn finish(&mut self) -> Result<(), String> {
        self.log.push("finish");
        self.finish_error.clone().map_or(Ok(()), Err)
    }

    fn close(&mut self) -> Result<(), String> {
        self.log.push("close");
        self.close_calls += 1;
        Ok(())
    }
}

#[derive(Default)]
struct Sink {
    payloads: Vec<Vec<u8>>,
    fail_at: Option<usize>,
}

#[derive(Default)]
struct CountingSink {
    packets: usize,
}

impl ResultSetSink for CountingSink {
    fn write_payload(&mut self, _: &[u8]) -> Result<(), SinkWriteError> {
        self.packets += 1;
        Ok(())
    }

    fn packets_written(&self) -> usize {
        self.packets
    }
}

impl ResultSetSink for Sink {
    fn write_payload(&mut self, payload: &[u8]) -> Result<(), SinkWriteError> {
        if self.fail_at == Some(self.payloads.len()) {
            return Err(SinkWriteError {
                message: "write failed".to_owned(),
                bytes_escaped: true,
            });
        }
        self.payloads.push(payload.to_vec());
        Ok(())
    }

    fn packets_written(&self) -> usize {
        self.payloads.len()
    }
}

fn column() -> ColumnInfo {
    ColumnInfo {
        schema: String::new(),
        table: String::new(),
        org_table: String::new(),
        name: "a".to_owned(),
        org_name: String::new(),
        column_length: 11,
        charset: 63,
        flag: 0,
        decimal: 0,
        type_code: TYPE_LONG,
        default_value: None,
    }
}

#[test]
fn first_next_error_is_retryable_and_never_reads_columns_or_writes() {
    let mut source = Source {
        events: [Err("first next failed".to_owned())].into(),
        ..Source::default()
    };
    let mut sink = Sink::default();
    let error =
        write_result_set(&mut source, &mut sink, ResultSetOptions::default(), 32).unwrap_err();
    assert!(error.retryable);
    assert!(!error.bytes_escaped);
    assert_eq!(source.columns_calls, 0);
    assert!(sink.payloads.is_empty());
    assert_eq!(source.log, ["next"]);
}

#[test]
fn empty_first_next_still_emits_metadata_then_finishes_and_emits_eof() {
    let mut source = Source::default();
    let mut sink = Sink::default();
    let outcome =
        write_result_set(&mut source, &mut sink, ResultSetOptions::default(), 32).unwrap();
    assert_eq!(source.log, ["next", "columns", "finish"]);
    assert_eq!(outcome.rows_written, 0);
    assert_eq!(sink.payloads.len(), 4);
    assert_eq!(sink.payloads.first().unwrap(), &[1]);
    assert_eq!(sink.payloads.last().unwrap()[0], 0xfe);
}

#[test]
fn second_next_error_is_nonretryable_after_metadata_and_rows_escape() {
    let mut source = Source {
        events: [
            Ok(vec![vec![Datum::Int(7)]]),
            Err("second next failed".to_owned()),
        ]
        .into(),
        ..Source::default()
    };
    let mut sink = Sink::default();
    let error =
        write_result_set(&mut source, &mut sink, ResultSetOptions::default(), 1).unwrap_err();
    assert!(!error.retryable);
    assert!(error.bytes_escaped);
    assert_eq!(source.log, ["next", "columns", "next"]);
    assert!(sink
        .payloads
        .iter()
        .any(|payload| payload.as_slice() == b"\x017"));
}

#[test]
fn connection_finishes_before_close_on_early_error_without_double_finish() {
    let mut source = Source {
        events: [Err("first next failed".to_owned())].into(),
        ..Source::default()
    };
    let mut sink = Sink::default();
    let error = write_connection_result_set_to_sink(
        &mut source,
        &mut sink,
        ResultSetOptions::default(),
        32,
    )
    .unwrap_err();
    assert!(error.retryable);
    assert_eq!(source.log, ["next", "finish", "close"]);
    assert_eq!(source.close_calls, 1);

    let mut source = Source {
        finish_error: Some("finish failed".to_owned()),
        ..Source::default()
    };
    let mut sink = Sink::default();
    let error = write_connection_result_set_to_sink(
        &mut source,
        &mut sink,
        ResultSetOptions::default(),
        32,
    )
    .unwrap_err();
    assert_eq!(error.message, "finish failed");
    assert_eq!(source.log, ["next", "columns", "finish", "close"]);
    assert_eq!(source.close_calls, 1);
}

#[test]
fn first_pulled_row_format_error_is_nonretryable_and_connection_is_cleaned_up() {
    let mut source = Source {
        events: [Ok(vec![vec![Datum::MinNotNull]])].into(),
        ..Source::default()
    };
    let mut sink = Sink::default();
    let error = write_connection_result_set_to_sink(
        &mut source,
        &mut sink,
        ResultSetOptions::default(),
        32,
    )
    .unwrap_err();
    assert_eq!(error.message, "cannot render MinNotNull as a SQL row");
    assert!(!error.retryable);
    assert!(error.bytes_escaped);
    assert_eq!(source.log, ["next", "columns", "finish", "close"]);
    assert_eq!(source.close_calls, 1);
}

#[test]
fn caller_owned_sink_receives_packets_without_a_response_vec() {
    let mut source = Source {
        events: [Ok(vec![vec![Datum::Int(7)]]), Ok(Vec::new())].into(),
        ..Source::default()
    };
    let mut sink = CountingSink::default();
    let outcome =
        write_connection_result_set_to_sink(&mut source, &mut sink, ResultSetOptions::default(), 1)
            .unwrap();
    assert_eq!(outcome.rows_written, 1);
    assert_eq!(outcome.packets_written, sink.packets);
    assert_eq!(source.log, ["next", "columns", "next", "finish", "close"]);
    assert_eq!(source.close_calls, 1);
}

#[test]
fn finish_error_suppresses_terminal_eof_and_write_failure_is_nonretryable() {
    let mut source = Source {
        finish_error: Some("finish failed".to_owned()),
        ..Source::default()
    };
    let mut sink = Sink::default();
    let error =
        write_result_set(&mut source, &mut sink, ResultSetOptions::default(), 32).unwrap_err();
    assert_eq!(error.message, "finish failed");
    assert!(!error.retryable);
    // The third packet is the metadata terminator and is itself an EOF packet.
    // A successful empty result has a fourth, terminal EOF packet; Finish
    // failure must stop before that packet is emitted.
    assert_eq!(sink.payloads.len(), 3);
    assert_eq!(sink.payloads.last().unwrap()[0], 0xfe);

    let mut source = Source::default();
    let mut sink = Sink {
        fail_at: Some(0),
        ..Sink::default()
    };
    let error =
        write_result_set(&mut source, &mut sink, ResultSetOptions::default(), 32).unwrap_err();
    assert!(!error.retryable);
    assert!(error.bytes_escaped);
}

/// A `NULL` cell is the one-byte `0xfb` sentinel in a text-protocol row, per
/// MySQL's `ProtocolText::ResultsetRow`. It is distinct from the empty string,
/// which is a zero-length-encoded string (`0x00`), so a client can tell them
/// apart.
#[test]
fn a_null_cell_is_the_text_protocol_0xfb_sentinel() {
    let mut source = Source {
        events: [Ok(vec![
            vec![Datum::Null],
            vec![Datum::new_bytes(Vec::new())],
            vec![Datum::new_int(7)],
        ])]
        .into(),
        ..Source::default()
    };
    let mut sink = Sink::default();
    let outcome = write_result_set(&mut source, &mut sink, ResultSetOptions::default(), 8)
        .expect("rows write");
    assert_eq!(outcome.rows_written, 3);

    // Column count, one column definition, EOF, then the three row packets.
    let rows = &sink.payloads[sink.payloads.len() - 4..sink.payloads.len() - 1];
    assert_eq!(rows[0], vec![0xfb], "NULL is the bare 0xfb sentinel");
    assert_eq!(rows[1], vec![0x00], "an empty string is a zero-length string");
    assert_eq!(rows[2], vec![0x01, b'7']);
}
