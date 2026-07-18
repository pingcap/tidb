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
//
//! Dependency-closed statement-summary rows reader from `pkg/executor/stmtsummary.go`.
//!
//! The Go reader combines materialized rows with an optional puller.  It only pulls
//! when its buffer is empty, drains bounded prefixes, and closes/detaches the puller
//! after an empty pull.  Statement-summary storage, table selection, privileges,
//! session context, Datum rendering, and SQL execution remain outside this module.

/// A source of statement-summary rows.
pub trait RowsPuller<T> {
    /// Fetch the next materialized batch.  An empty batch is end-of-stream.
    fn rows(&mut self) -> Result<Vec<Vec<T>>, String>;

    /// Close the source and release external resources.
    fn close(&mut self) -> Result<(), String>;
}

/// Buffered rows plus an optional puller, matching TiDB's `rowsReader` state.
pub struct StatementRowsReader<T> {
    puller: Option<Box<dyn RowsPuller<T>>>,
    rows: Vec<Vec<T>>,
}

impl<T> StatementRowsReader<T> {
    /// Construct a reader backed only by already materialized rows.
    pub fn simple(rows: Vec<Vec<T>>) -> Self {
        Self { puller: None, rows }
    }

    /// Construct a reader with initial rows followed by a pull-based source.
    pub fn with_puller(rows: Vec<Vec<T>>, puller: Box<dyn RowsPuller<T>>) -> Self {
        Self {
            puller: Some(puller),
            rows,
        }
    }

    /// Return at most `max_count` rows, pulling only when the buffer is empty.
    pub fn read(&mut self, max_count: usize) -> Result<Vec<Vec<T>>, String> {
        self.pull()?;
        if max_count >= self.rows.len() {
            return Ok(std::mem::take(&mut self.rows));
        }
        Ok(self.rows.drain(..max_count).collect())
    }

    /// Delegate close to the active puller.  The Go implementation intentionally
    /// leaves the puller attached, so repeated calls delegate repeatedly.
    pub fn close(&mut self) -> Result<(), String> {
        if let Some(puller) = self.puller.as_mut() {
            puller.close()
        } else {
            Ok(())
        }
    }

    /// Number of rows currently buffered in memory.
    pub fn buffered_len(&self) -> usize {
        self.rows.len()
    }

    /// Whether a puller is still attached.
    pub fn has_puller(&self) -> bool {
        self.puller.is_some()
    }

    fn pull(&mut self) -> Result<(), String> {
        if !self.rows.is_empty() || self.puller.is_none() {
            return Ok(());
        }

        let rows = {
            let puller = self.puller.as_mut().expect("puller checked above");
            puller.rows()?
        };
        if !rows.is_empty() {
            self.rows = rows;
            return Ok(());
        }

        // Empty Rows closes and detaches only after Close succeeds.  A close error
        // leaves the puller attached so callers can observe/retry it.
        self.puller
            .as_mut()
            .expect("puller remains attached after Rows")
            .close()?;
        self.puller = None;
        Ok(())
    }
}
