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

// The leaf is intentionally tested by path until the crate root assigns its
// public module/re-export.  This keeps the workstream isolated: the source
// and its source-shaped tests can land without racing another agent editing
// `tidb-distsql/src/lib.rs`.
#[path = "../src/select_iter.rs"]
mod select_iter;

use std::cell::Cell;
use std::collections::VecDeque;
use std::rc::Rc;

use select_iter::{
    unsupported_chunk, unsupported_next_raw, unsupported_sorted_heap, unsupported_tikv_transport,
    SelectResultError, SelectResultRow, SelectResultSource, SerialSelectResults,
    UnsupportedCapability,
};

struct Source {
    rows: VecDeque<SelectResultRow<i32>>,
    next_error: Option<SelectResultError>,
    close_error: Option<SelectResultError>,
    closed: Rc<Cell<bool>>,
}

impl Source {
    fn rows(channel_index: usize, rows: impl IntoIterator<Item = i32>) -> Self {
        Self {
            rows: rows
                .into_iter()
                .map(|row| SelectResultRow::new(channel_index, row))
                .collect(),
            next_error: None,
            close_error: None,
            closed: Rc::new(Cell::new(false)),
        }
    }

    fn empty() -> Self {
        Self::rows(0, [])
    }

    fn with_next_error(mut self, message: &'static str) -> Self {
        self.next_error = Some(SelectResultError::source(message));
        self
    }

    fn with_close_error(mut self, message: &'static str) -> Self {
        self.close_error = Some(SelectResultError::source(message));
        self
    }
}

impl SelectResultSource for Source {
    type Row = i32;

    fn next_row(&mut self) -> Result<Option<SelectResultRow<Self::Row>>, SelectResultError> {
        if let Some(error) = self.next_error.take() {
            return Err(error);
        }
        Ok(self.rows.pop_front())
    }

    fn close(&mut self) -> Result<(), SelectResultError> {
        self.closed.set(true);
        self.close_error.take().map_or(Ok(()), Err)
    }
}

#[test]
fn go_select_result_iter_serially_composes_rows_and_skips_empty_sources() {
    // Structural port of `pkg/distsql/select_result_test.go:288 TestSelectResultIter`:
    // the final channel is consumed after all intermediate channels, while an
    // empty response contributes no sentinel row to the output.
    let mut serial = SerialSelectResults::new([
        Source::empty(),
        Source::rows(0, [1, 2]),
        Source::empty(),
        Source::rows(2, [3]),
    ]);

    assert_eq!(serial.current_source(), 0);
    assert_eq!(serial.next_row().unwrap(), Some(SelectResultRow::new(0, 1)));
    assert_eq!(serial.next_row().unwrap(), Some(SelectResultRow::new(0, 2)));
    assert_eq!(serial.current_source(), 1);
    assert_eq!(serial.next_row().unwrap(), Some(SelectResultRow::new(2, 3)));
    // As in Go's `serialSelectResults`, the current source advances only when
    // its next call observes the source's empty result.
    assert_eq!(serial.current_source(), 3);
    assert!(!serial.is_drained());
    assert_eq!(serial.next_row().unwrap(), None);
    assert_eq!(serial.current_source(), 4);
    assert!(serial.is_drained());
    assert_eq!(serial.next_row().unwrap(), None);
}

#[test]
fn go_select_result_iter_propagates_next_error_without_skipping_source() {
    let source = Source::rows(1, [7]).with_next_error("decode failed");
    let mut serial = SerialSelectResults::new([Source::empty(), source]);

    assert_eq!(
        serial.next_row().unwrap_err(),
        SelectResultError::source("decode failed")
    );
    assert_eq!(serial.current_source(), 1);
    // The source error was consumed, so the source remains current and its row
    // is still returned.  Serial composition must not silently discard it.
    assert_eq!(serial.next_row().unwrap(), Some(SelectResultRow::new(1, 7)));
}

#[test]
fn go_serial_select_results_close_calls_every_source_and_returns_last_error() {
    let first = Source::rows(0, []).with_close_error("first close error");
    let first_closed = Rc::clone(&first.closed);
    let second = Source::rows(1, []).with_close_error("last close error");
    let second_closed = Rc::clone(&second.closed);
    let third = Source::rows(2, []);
    let third_closed = Rc::clone(&third.closed);
    let mut serial = SerialSelectResults::new([first, second, third]);

    assert_eq!(
        serial.close().unwrap_err(),
        SelectResultError::source("last close error")
    );
    assert!(first_closed.get());
    assert!(second_closed.get());
    assert!(third_closed.get());
}

#[test]
fn unsupported_result_capabilities_are_explicit() {
    assert_eq!(
        SelectResultError::Cancelled.to_string(),
        "query cancelled by caller"
    );
    assert_eq!(
        SelectResultRow::new(4, "row").map(str::len),
        SelectResultRow::new(4, 3)
    );
    assert_eq!(
        unsupported_next_raw(),
        SelectResultError::Unsupported(UnsupportedCapability::NextRaw)
    );
    assert_eq!(
        unsupported_chunk(),
        SelectResultError::Unsupported(UnsupportedCapability::Chunk)
    );
    assert_eq!(
        unsupported_tikv_transport(),
        SelectResultError::Unsupported(UnsupportedCapability::TiKvTransport)
    );
    assert_eq!(
        unsupported_sorted_heap(),
        SelectResultError::Unsupported(UnsupportedCapability::SortedHeap)
    );
    assert!(unsupported_next_raw().to_string().contains("NextRaw"));
    assert!(unsupported_chunk().to_string().contains("chunk decoding"));
    assert!(unsupported_tikv_transport()
        .to_string()
        .contains("TiKV transport"));
    assert!(unsupported_sorted_heap()
        .to_string()
        .contains("sorted result heap"));
}
