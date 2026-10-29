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

//! Merge-sort of the per-group results of a grouped-range ordered read.
//!
//! Go `matchProperty` accepts an access path whose ranges each pin some index
//! columns to one constant (`a = 3 AND b IN (1, 3, 5) ORDER BY c`) by reading
//! every group of ranges as its own request -- each group is already in the
//! requested order -- and merging the group results through
//! `distsql.NewSortedSelectResults`. This operator is that merge: one child
//! per range group, a heap of each child's current row, the same sift order as
//! Go's `container/heap` so equal keys come out in Go's order.

use std::cmp::Ordering;

use crate::sort::{eval_sort_key, less_by_items, validate_by_items, SortByItem};
use crate::topn_chunk_heap::go_heap;
use crate::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::schema::Schema;
use tidb_exprctx::Columns;

/// Go `sortedSelectResults.heap` element: `chunk.RowPtr` plus the evaluated
/// key of that row.
struct HeapRow {
    child: usize,
    row: usize,
    key: Vec<Datum>,
}

/// Go `distsql.sortedSelectResults` over already-built group readers.
pub struct RangeGroupMergeExec<C: Columns> {
    meta: ExecutorMeta,
    by_items: Vec<SortByItem>,
    children: Vec<Box<dyn Executor>>,
    ctx: C,
    /// Go `cachedChunks`: each group's current chunk.
    cached: Vec<Option<Chunk>>,
    heap: Vec<HeapRow>,
}

impl<C> RangeGroupMergeExec<C>
where
    C: Columns + Clone + Send + Sync + 'static,
{
    /// Builds the merge. `by_items` must be direct columns of the children's
    /// (shared) output schema, as Go's `buildKeyColumns` requires.
    pub fn new(
        meta: ExecutorMeta,
        by_items: Vec<SortByItem>,
        children: Vec<Box<dyn Executor>>,
        ctx: C,
    ) -> Result<Self, ExecError> {
        validate_by_items(&by_items)?;
        let cached = children.iter().map(|_| None).collect();
        Ok(Self {
            meta,
            by_items,
            children,
            ctx,
            cached,
            heap: Vec::new(),
        })
    }

    fn row_key(&self, child: usize, row: usize) -> Result<Vec<Datum>, ExecError> {
        let chunk = self.cached[child]
            .as_ref()
            .expect("a child's chunk exists before its rows are keyed");
        eval_sort_key(&self.by_items, &self.ctx, chunk.get_row(row))
    }

    /// Go `heap.Push`.
    fn push(&mut self, element: HeapRow) -> Result<(), ExecError> {
        let by_items = &self.by_items;
        let mut error = None;
        self.heap.push(element);
        let last = self.heap.len() - 1;
        go_heap::up(&mut self.heap, last, &mut |left, right| {
            ordering_is_less(less_by_items(by_items, &left.key, &right.key), &mut error)
        });
        error.map_or(Ok(()), Err)
    }

    /// Go `heap.Pop`.
    fn pop(&mut self) -> Result<Option<HeapRow>, ExecError> {
        let by_items = &self.by_items;
        let mut error = None;
        let head = go_heap::pop(&mut self.heap, &mut |left, right| {
            ordering_is_less(less_by_items(by_items, &left.key, &right.key), &mut error)
        });
        error.map_or(Ok(head), Err)
    }

    /// Go `updateCachedChunk`: fetch the child's next chunk and, when it has
    /// rows, push its first row.
    fn update_cached_chunk(&mut self, child: usize) -> Result<(), ExecError> {
        let chunk = self.cached[child]
            .as_mut()
            .expect("a child's chunk exists before it is fetched");
        self.children[child].next(chunk)?;
        if chunk.num_rows() == 0 {
            return Ok(());
        }
        let key = self.row_key(child, 0)?;
        self.push(HeapRow { child, row: 0, key })
    }
}

fn ordering_is_less(result: Result<Ordering, ExecError>, error: &mut Option<ExecError>) -> bool {
    match result {
        Ok(ordering) => ordering == Ordering::Less,
        Err(failure) => {
            if error.is_none() {
                *error = Some(failure);
            }
            false
        }
    }
}

impl<C> Executor for RangeGroupMergeExec<C>
where
    C: Columns + Clone + Send + Sync + 'static,
{
    fn open(&mut self) -> Result<(), ExecError> {
        self.heap.clear();
        for chunk in &mut self.cached {
            *chunk = None;
        }
        for child in &mut self.children {
            child.open()?;
        }
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        for (child, chunk) in self.children.iter().zip(&mut self.cached) {
            if chunk.is_none() {
                *chunk = Some(child.new_chunk());
            }
        }
        if self.heap.is_empty() {
            for child in 0..self.children.len() {
                self.update_cached_chunk(child)?;
            }
        }
        let batch = req.required_rows().min(self.meta.max_chunk_size());
        while req.num_rows() < batch {
            let Some(head) = self.pop()? else {
                break;
            };
            let chunk = self.cached[head.child]
                .as_ref()
                .expect("a popped row's chunk is cached");
            req.append_row(chunk.get_row(head.row));
            if head.row + 1 >= chunk.num_rows() {
                self.update_cached_chunk(head.child)?;
            } else {
                let row = head.row + 1;
                let key = self.row_key(head.child, row)?;
                self.push(HeapRow {
                    child: head.child,
                    row,
                    key,
                })?;
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.heap.clear();
        for chunk in &mut self.cached {
            *chunk = None;
        }
        let mut first_error = None;
        for child in &mut self.children {
            if let Err(error) = child.close() {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}
