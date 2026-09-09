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

//! Go `markChildrenUsedCols` / `baseJoiner`: output selection never changes
//! the input layout used by hash keys and residual expressions.

use tidb_chunk::{chunk::Chunk, row::Row};
use tidb_datatype::Datum;
use tidb_expr::schema::Schema;

use super::JoinKind;
use crate::ExecError;

#[derive(Clone)]
pub(super) struct JoinOutput {
    left: Vec<usize>,
    right: Vec<usize>,
    left_width: usize,
    left_default: Vec<Datum>,
    right_default: Vec<Datum>,
}

impl JoinOutput {
    pub(super) fn all(kind: JoinKind, left: usize, right: usize) -> Self {
        Self {
            left: (0..left).collect(),
            right: if matches!(kind, JoinKind::Inner | JoinKind::Left | JoinKind::Right) {
                (0..right).collect()
            } else {
                Vec::new()
            },
            left_width: left,
            left_default: vec![Datum::Null; left],
            right_default: vec![Datum::Null; right],
        }
    }

    pub(super) fn resolved(
        schema: &Schema,
        kind: JoinKind,
        left_width: usize,
        right_width: usize,
    ) -> Result<Self, ExecError> {
        let mut output = Self::all(kind, left_width, right_width);
        output.left.clear();
        output.right.clear();
        let columns = if kind == JoinKind::LeftOuterSemi {
            &schema.columns[..schema.columns.len().saturating_sub(1)]
        } else {
            &schema.columns
        };
        for column in columns {
            let index = usize::try_from(column.index)
                .map_err(|_| ExecError::internal("join output column is unresolved"))?;
            if index < left_width {
                output.left.push(index);
            } else if index < left_width + right_width {
                output.right.push(index - left_width);
            } else {
                return Err(ExecError::internal(
                    "join output column is outside children",
                ));
            }
        }
        Ok(output)
    }

    pub(super) fn width(&self) -> usize {
        self.left.len() + self.right.len()
    }

    pub(super) fn set_default(&mut self, left: bool, values: &[Datum]) -> Result<(), ExecError> {
        let target = if left {
            &mut self.left_default
        } else {
            &mut self.right_default
        };
        if !values.is_empty() {
            if target.len() != values.len() {
                return Err(ExecError::internal("join default row width mismatch"));
            }
            target.clone_from_slice(values);
        }
        Ok(())
    }

    pub(super) fn padded(&self, outer_left: bool, outer: &[Datum]) -> Vec<Datum> {
        if outer_left {
            outer.iter().chain(&self.right_default).cloned().collect()
        } else {
            self.left_default.iter().chain(outer).cloned().collect()
        }
    }

    fn side(&self, left: bool) -> (usize, &[usize]) {
        if left {
            (0, &self.left)
        } else {
            (self.left.len(), &self.right)
        }
    }

    fn finish(req: &mut Chunk, rows: usize) {
        req.set_num_virtual_rows(req.num_virtual_rows() + rows);
    }

    pub(super) fn datums(&self, req: &mut Chunk, joined: &[Datum]) {
        for (dest, source) in self
            .left
            .iter()
            .copied()
            .chain(self.right.iter().map(|index| self.left_width + index))
            .enumerate()
        {
            req.append_datum(dest, &joined[source]);
        }
        Self::finish(req, 1);
    }

    pub(super) fn datum_side(&self, req: &mut Chunk, left: bool, row: &[Datum]) {
        let (start, columns) = self.side(left);
        for (dest, source) in columns.iter().enumerate() {
            req.append_datum(start + dest, &row[*source]);
        }
    }

    pub(super) fn chunk_side(&self, req: &mut Chunk, left: bool, row: Row<'_>) {
        let (start, columns) = self.side(left);
        req.append_partial_row_by_col_idxs(start, row, Some(columns));
    }

    pub(super) fn datum_pair(
        &self,
        req: &mut Chunk,
        outer_left: bool,
        outer: &[Datum],
        inner: &[Datum],
    ) {
        self.datum_side(req, outer_left, outer);
        self.datum_side(req, !outer_left, inner);
        Self::finish(req, 1);
    }

    pub(super) fn datum_chunk(
        &self,
        req: &mut Chunk,
        outer_left: bool,
        outer: &[Datum],
        inner: Row<'_>,
    ) {
        self.datum_side(req, outer_left, outer);
        self.chunk_side(req, !outer_left, inner);
        Self::finish(req, 1);
    }

    pub(super) fn chunk_datum(
        &self,
        req: &mut Chunk,
        outer_left: bool,
        outer: Row<'_>,
        inner: &[Datum],
    ) {
        self.chunk_side(req, outer_left, outer);
        self.datum_side(req, !outer_left, inner);
        Self::finish(req, 1);
    }

    pub(super) fn chunks(&self, req: &mut Chunk, probe_left: bool, probe: Row<'_>, build: Row<'_>) {
        self.chunk_side(req, probe_left, probe);
        self.chunk_side(req, !probe_left, build);
        Self::finish(req, 1);
    }

    pub(super) fn preserved(&self, req: &mut Chunk, row: Row<'_>) {
        self.chunk_side(req, true, row);
        Self::finish(req, 1);
    }

    pub(super) fn unmatched(
        &self,
        req: &mut Chunk,
        probe_left: bool,
        probe: Row<'_>,
        _build_width: usize,
    ) {
        self.chunk_side(req, probe_left, probe);
        let (start, columns) = self.side(!probe_left);
        let defaults = if probe_left {
            &self.right_default
        } else {
            &self.left_default
        };
        for (dest, source) in columns.iter().enumerate() {
            req.append_datum(start + dest, &defaults[*source]);
        }
        Self::finish(req, 1);
    }

    pub(super) fn side_range(&self, req: &mut Chunk, left: bool, input: &Chunk) {
        let (start, columns) = self.side(left);
        for (dest, source) in columns.iter().enumerate() {
            req.append_column_range_from(start + dest, input, *source, 0, input.num_rows());
        }
    }

    pub(super) fn key_range(&self, req: &mut Chunk, left: bool, input: &Chunk, key: usize) {
        let (start, columns) = self.side(left);
        for dest in start..start + columns.len() {
            req.append_column_range_from(dest, input, key, 0, input.num_rows());
        }
    }

    pub(super) fn finish_range(&self, req: &mut Chunk, rows: usize) {
        Self::finish(req, rows);
    }
}
