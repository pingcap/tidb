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

//! Go NestedLoopApplyExec: reopen a built inner executor after rebinding
//! correlated datum cells; tuple semantics belong to the shared Joiner.
use crate::apply_cache::ApplyCache;
use crate::joiner::{row_based_filter, Joiner, NAAJType};
use crate::{ExecError, Executor, ExecutorMeta, StatementMemory, StmtContext};
use std::sync::Arc;
use tidb_chunk::{
    chunk::Chunk,
    iterator::{Iterator4List, LendingIterator, ListIteratorPosition},
    list::List,
};
use tidb_datatype::{Datum, FieldType};
use tidb_expr::{column::CorrelatedColumn, expression::Expression, schema::Schema};
use tidb_util::memory::Tracker;

struct InnerRows {
    list: List,
}
impl Drop for InnerRows {
    fn drop(&mut self) {
        self.list.clear();
    }
}

/// The serial source executor. It owns only execution-local bindings and rows.
pub struct NestedLoopApplyExec {
    meta: ExecutorMeta,
    outer: Box<dyn Executor>,
    inner: Box<dyn Executor>,
    outer_filter: Vec<Expression>,
    inner_filter: Vec<Expression>,
    outer_schema: Vec<CorrelatedColumn>,
    joiner: Box<dyn Joiner>,
    preserve_outer: bool,
    context: StmtContext,
    memory: StatementMemory,
    tracker: Arc<Tracker>,
    cache_enabled: bool,
    cache: Option<ApplyCache<InnerRows>>,
    inner_rows: Option<Arc<InnerRows>>,
    inner_position: Option<ListIteratorPosition>,
    outer_chunk: Chunk,
    inner_chunk: Chunk,
    outer_selected: Vec<bool>,
    inner_selected: Vec<bool>,
    outer_cursor: usize,
    active_outer: Option<usize>,
    has_match: bool,
    has_null: bool,
    done: bool,
}
impl NestedLoopApplyExec {
    pub fn new(
        meta: ExecutorMeta,
        outer: Box<dyn Executor>,
        inner: Box<dyn Executor>,
        outer_filter: Vec<Expression>,
        inner_filter: Vec<Expression>,
        outer_schema: Vec<CorrelatedColumn>,
        joiner: Box<dyn Joiner>,
        preserve_outer: bool,
        cache_enabled: bool,
        context: StmtContext,
    ) -> Self {
        let memory = context.statement_memory();
        let tracker = memory.operator_tracker(meta.id());
        let outer_chunk = outer.new_chunk();
        let inner_chunk = inner.new_chunk();
        Self {
            meta,
            outer,
            inner,
            outer_filter,
            inner_filter,
            outer_schema,
            joiner,
            preserve_outer,
            context,
            memory,
            tracker,
            cache_enabled,
            cache: None,
            inner_rows: None,
            inner_position: None,
            outer_chunk,
            inner_chunk,
            outer_selected: vec![],
            inner_selected: vec![],
            outer_cursor: 0,
            active_outer: None,
            has_match: false,
            has_null: false,
            done: false,
        }
    }
    fn account(&self) -> Result<(), ExecError> {
        let rows = self
            .inner_rows
            .as_ref()
            .map_or(0, |rows| rows.list.mem_tracker().bytes_consumed());
        self.tracker.replace_bytes_used(
            self.outer_chunk.memory_usage()
                + self.inner_chunk.memory_usage()
                + rows
                + self.cache.as_ref().map_or(0, ApplyCache::memory_consumed),
        );
        self.memory.check()
    }
    fn load_inner(&mut self, index: usize) -> Result<(), ExecError> {
        let row = self.outer_chunk.get_row(index);
        let mut key_values = Vec::with_capacity(self.outer_schema.len());
        for col in &self.outer_schema {
            let ty = col
                .column
                .ret_type
                .as_ref()
                .ok_or_else(|| ExecError::internal("Apply correlation has no type"))?;
            let value = row.get_datum(col.column.index as usize, ty);
            col.data.set(value.clone());
            key_values.push(value);
        }
        let key = if self.cache.is_some() {
            Some(
                tidb_codec::encode_key_in_timezone(&self.context.session_zone(), &key_values)
                    .map_err(|e| {
                        ExecError::internal(format!("cannot encode Apply cache key: {e}"))
                    })?,
            )
        } else {
            None
        };
        if let (Some(cache), Some(key)) = (&self.cache, &key) {
            if let Some(rows) = cache.get(key) {
                self.inner_rows = Some(rows);
                return self.account();
            }
        }
        let mut rows = if self.cache.is_none() {
            self.inner_rows
                .take()
                .and_then(|rows| Arc::try_unwrap(rows).ok())
                .unwrap_or_else(|| InnerRows {
                    list: List::new(
                        self.inner.ret_field_types(),
                        self.meta.init_cap(),
                        self.meta.max_chunk_size(),
                    ),
                })
        } else {
            InnerRows {
                list: List::new(
                    self.inner.ret_field_types(),
                    self.meta.init_cap(),
                    self.meta.max_chunk_size(),
                ),
            }
        };
        rows.list.reset();
        let result: Result<(), ExecError> = (|| {
            self.inner.open()?;
            loop {
                self.inner.next(&mut self.inner_chunk)?;
                if self.inner_chunk.num_rows() == 0 {
                    return Ok(());
                }
                row_based_filter(
                    &self.context,
                    &self.inner_filter,
                    &self.inner_chunk,
                    &mut self.inner_selected,
                    None,
                )?;
                for row in 0..self.inner_chunk.num_rows() {
                    if self.inner_selected[row] {
                        rows.list.append_row(self.inner_chunk.get_row(row));
                    }
                }
                self.tracker.replace_bytes_used(
                    self.outer_chunk.memory_usage()
                        + self.inner_chunk.memory_usage()
                        + rows.list.mem_tracker().bytes_consumed()
                        + self.cache.as_ref().map_or(0, ApplyCache::memory_consumed),
                );
                self.memory.check()?;
            }
        })();
        if let Err(error) = self.inner.close() {
            eprintln!("Apply inner close: {error:?}");
        }
        result?;
        let rows = Arc::new(rows);
        if let (Some(cache), Some(key)) = (&self.cache, key) {
            cache.set_shared(key, rows.clone(), rows.list.mem_tracker().bytes_consumed());
        }
        self.inner_rows = Some(rows);
        self.account()
    }
    fn select_outer(&mut self, req: &mut Chunk) -> Result<bool, ExecError> {
        loop {
            if self.outer_cursor >= self.outer_chunk.num_rows() {
                let previous_cursor = self.outer_cursor;
                self.outer.next(&mut self.outer_chunk)?;
                if self.outer_chunk.num_rows() == 0 {
                    self.done = true;
                    return Ok(false);
                }
                row_based_filter(
                    &self.context,
                    &self.outer_filter,
                    &self.outer_chunk,
                    &mut self.outer_selected,
                    None,
                )?;
                if previous_cursor == 0
                    && self.outer_chunk.num_rows() == 1
                    && self.outer_selected[0]
                    && self.outer.agg_tree_input_empty()
                {
                    self.outer_selected[0] = false;
                }
                self.outer_cursor = 0;
                self.account()?;
            }
            let index = self.outer_cursor;
            self.outer_cursor += 1;
            if self.outer_selected[index] {
                self.active_outer = Some(index);
                return Ok(true);
            }
            if self.preserve_outer {
                self.joiner
                    .on_miss_match(false, self.outer_chunk.get_row(index), req);
                if req.is_full() {
                    return Ok(false);
                }
            }
        }
    }
}
impl Executor for NestedLoopApplyExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.outer.open()?;
        self.outer_chunk = self.outer.new_chunk();
        self.inner_chunk = self.inner.new_chunk();
        self.outer_cursor = 0;
        self.active_outer = None;
        self.inner_rows = None;
        self.inner_position = None;
        self.done = false;
        self.has_match = false;
        self.has_null = false;
        self.cache = self
            .cache_enabled
            .then(|| ApplyCache::new(self.context.apply_cache_capacity()));
        self.account()
    }
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        while !req.is_full() && !self.done {
            if self.active_outer.is_none() {
                if !self.select_outer(req)? {
                    return Ok(());
                }
                self.has_match = false;
                self.has_null = false;
                self.inner_position = None;
                self.load_inner(self.active_outer.unwrap())?;
            }
            let index = self.active_outer.unwrap();
            let rows = self.inner_rows.as_ref().expect("loaded inner rows");
            let mut iterator = match self.inner_position {
                Some(position) => {
                    LendingIterator::List(Iterator4List::resume(&rows.list, position))
                }
                None => {
                    let mut iterator = LendingIterator::list(&rows.list);
                    iterator.begin();
                    iterator
                }
            };
            if iterator.current().is_some() {
                let (matched, has_null) = self.joiner.try_to_match_inners(
                    self.outer_chunk.get_row(index),
                    &mut iterator,
                    req,
                    NAAJType::Unknown,
                )?;
                self.has_match |= matched;
                self.has_null |= has_null;
            }
            let exhausted = iterator.current().is_none();
            let LendingIterator::List(iterator) = iterator else {
                unreachable!()
            };
            self.inner_position = Some(iterator.position());
            if exhausted {
                // A full output chunk must not receive the mismatch row as well.
                // Retain the exhausted cursor and finish this outer row on the next call.
                if !self.has_match && req.is_full() {
                    return Ok(());
                }
                if !self.has_match {
                    self.joiner
                        .on_miss_match(self.has_null, self.outer_chunk.get_row(index), req);
                }
                self.active_outer = None;
                self.inner_position = None;
            }
        }
        Ok(())
    }
    fn close(&mut self) -> Result<(), ExecError> {
        self.inner_rows = None;
        self.cache = None;
        self.active_outer = None;
        self.inner_position = None;
        self.outer_chunk = Chunk::default();
        self.inner_chunk = Chunk::default();
        self.outer_selected.clear();
        self.inner_selected.clear();
        self.tracker.replace_bytes_used(0);
        self.outer.close()
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
