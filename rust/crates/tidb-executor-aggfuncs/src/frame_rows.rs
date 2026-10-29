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

//! The frame a window function's state reads: rows that may span several
//! child chunks (Go's `[]chunk.Row` partition), with one chunk as the simple
//! case.

use std::ops::Range;

use tidb_chunk::{chunk::Chunk, row::Row};
use tidb_driver_error::ExecError;

/// Frame access independent of whether rows span one or several child chunks.
/// Aggregate evaluators can bind typed column readers once per contiguous range.
pub trait FrameRows {
    fn num_rows(&self) -> usize;

    /// Go subtracts frame offsets as uint64. A backward offset visits the
    /// remaining rows before its wrapped shift reaches the first invalid row.
    fn sliding_end(&self, start: usize, end: usize) -> usize {
        if end < start {
            self.num_rows()
        } else {
            end
        }
    }

    fn check_sliding_end(&self, start: usize, end: usize) -> Result<(), ExecError> {
        if end < start {
            let len = self.num_rows();
            Err(ExecError::internal(format!(
                "runtime error: index out of range [{len}] with length {len}"
            )))
        } else {
            Ok(())
        }
    }

    fn get_row(&self, index: usize) -> Row<'_>;
    fn visit_chunks(
        &self,
        range: Range<usize>,
        visit: impl FnMut(&Chunk, Range<usize>) -> Result<(), ExecError>,
    ) -> Result<(), ExecError>;
}

impl FrameRows for Chunk {
    fn num_rows(&self) -> usize {
        self.num_rows()
    }
    fn get_row(&self, index: usize) -> Row<'_> {
        self.get_row(index)
    }
    fn visit_chunks(
        &self,
        range: Range<usize>,
        mut visit: impl FnMut(&Chunk, Range<usize>) -> Result<(), ExecError>,
    ) -> Result<(), ExecError> {
        if range.is_empty() {
            Ok(())
        } else {
            visit(self, range)
        }
    }
}
