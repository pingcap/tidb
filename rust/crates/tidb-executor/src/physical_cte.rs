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

//! Go `CTEExec`, `cteProducer`, and `CTETableReaderExec`.
//!
//! A statement owns one producer per physical CTE storage ID. Every CTE
//! reader shares that producer and its complete result table; the recursive
//! plan reads the producer's current delta through a shared CTE-table reader.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::schema::Schema;

use crate::cte_storage::CteStorage;
use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::StmtContext;

pub(crate) type SharedCteStorage = Arc<Mutex<CteStorage>>;
pub(crate) type SharedCteProducer = Arc<Mutex<CteProducer>>;

/// Go `CTETableReaderExec`: scan the current recursive delta, restarting from
/// chunk zero whenever the producer advances the iteration marker.
pub(crate) struct CteTableReaderExec {
    meta: ExecutorMeta,
    iter_in: SharedCteStorage,
    chunk_index: usize,
    current_iteration: usize,
}

impl CteTableReaderExec {
    pub(crate) fn new(meta: ExecutorMeta, iter_in: SharedCteStorage) -> Self {
        Self {
            meta,
            iter_in,
            chunk_index: 0,
            current_iteration: 0,
        }
    }

    fn reset(&mut self) {
        self.chunk_index = 0;
        self.current_iteration = 0;
    }
}

impl Executor for CteTableReaderExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.reset();
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let iteration = self.iter_in.lock().unwrap().iter();
        if self.current_iteration != iteration {
            if self.current_iteration > iteration {
                return Err(ExecError::internal(format!(
                    "invalid iteration for CTETableReaderExec (current: {}, storage: {})",
                    self.current_iteration, iteration
                )));
            }
            self.chunk_index = 0;
            self.current_iteration = iteration;
        }
        let chunk = {
            let storage = self.iter_in.lock().unwrap();
            if self.chunk_index >= storage.num_chunks() {
                return Ok(());
            }
            let chunk = storage.get_chunk(self.chunk_index)?.copy_construct_sel();
            chunk
        };
        *req = chunk;
        self.chunk_index += 1;
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.reset();
        Ok(())
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

/// Shared Go `cteProducer` state. This is deliberately separate from each
/// root `CteExec`: multiple readers have independent result cursors but drive
/// one materialization.
pub(crate) struct CteProducer {
    seed: Box<dyn Executor>,
    recursive: Option<Box<dyn Executor>>,
    result: SharedCteStorage,
    iter_in: SharedCteStorage,
    iter_out: Option<CteStorage>,
    executor_opened: bool,
    open_error: Option<ExecError>,
    result_error: Option<ExecError>,
    is_distinct: bool,
    seen: HashSet<Vec<u8>>,
    current_iteration: usize,
    has_limit: bool,
    limit_end: u64,
    context: StmtContext,
    output_types: Vec<FieldType>,
    max_chunk_size: usize,
}

impl CteProducer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        seed: Box<dyn Executor>,
        recursive: Option<Box<dyn Executor>>,
        result: SharedCteStorage,
        iter_in: SharedCteStorage,
        is_distinct: bool,
        has_limit: bool,
        limit_end: u64,
        context: StmtContext,
        output_types: Vec<FieldType>,
        max_chunk_size: usize,
    ) -> Self {
        Self {
            seed,
            recursive,
            result,
            iter_in,
            iter_out: None,
            executor_opened: false,
            open_error: None,
            result_error: None,
            is_distinct,
            seen: HashSet::new(),
            current_iteration: 0,
            has_limit,
            limit_end,
            context,
            output_types,
            max_chunk_size,
        }
    }

    fn has_result(&self) -> bool {
        self.result.lock().unwrap().done()
    }

    fn open(&mut self) -> Result<(), ExecError> {
        if let Some(error) = &self.open_error {
            return Err(error.clone());
        }
        if self.has_result() || self.executor_opened {
            return Ok(());
        }
        let result: Result<(), ExecError> = (|| {
            self.seed.open()?;
            if let Some(recursive) = self.recursive.as_mut() {
                recursive.open()?;
                let mut iter_out = CteStorage::new(
                    recursive.ret_field_types().to_vec(),
                    self.max_chunk_size,
                    self.context.statement_memory(),
                );
                iter_out.open_and_ref()?;
                self.iter_out = Some(iter_out);
            }
            self.seen.clear();
            Ok(())
        })();
        self.executor_opened = true;
        if let Err(error) = &result {
            self.open_error = Some(error.clone());
        }
        result
    }

    fn close(&mut self) -> Result<(), ExecError> {
        if !self.executor_opened {
            return Ok(());
        }
        let mut first_error = self.seed.close().err();
        if let Some(recursive) = self.recursive.as_mut() {
            if let Err(error) = recursive.close() {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        if let Some(mut iter_out) = self.iter_out.take() {
            if let Err(error) = iter_out.deref_and_close() {
                first_error.get_or_insert(error);
            }
        }
        self.executor_opened = false;
        if !self.has_result() {
            if let Err(error) = self.result.lock().unwrap().reopen() {
                first_error.get_or_insert(error);
            }
            if let Err(error) = self.iter_in.lock().unwrap().reopen() {
                first_error.get_or_insert(error);
            }
            self.result_error = None;
            self.open_error = None;
            self.current_iteration = 0;
            self.seen.clear();
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn limit_done(&self, storage: &CteStorage) -> bool {
        self.has_limit && storage.num_rows() as u64 >= self.limit_end
    }

    fn row_key(&self, row: &[Datum]) -> Result<Vec<u8>, ExecError> {
        if row.len() != self.output_types.len() {
            return Err(ExecError::internal("CTE DISTINCT row width mismatch"));
        }
        let mut key = Vec::new();
        let timezone = self.context.session_zone();
        let mut part = Vec::new();
        for (value, field_type) in row.iter().zip(&self.output_types) {
            part.clear();
            tidb_codec::append_hash_group_key_in_timezone(&timezone, value, field_type, &mut part)
                .map_err(|error| ExecError::internal(error.to_string()))?;
            key.extend_from_slice(&(part.len() as u64).to_le_bytes());
            key.extend_from_slice(&part);
        }
        Ok(key)
    }

    fn deduplicate(&mut self, chunk: &Chunk) -> Result<Chunk, ExecError> {
        if !self.is_distinct {
            return Ok(chunk.copy_construct_sel());
        }
        let mut result = Chunk::new_with_capacity(&self.output_types, chunk.num_rows());
        for row_index in 0..chunk.num_rows() {
            let values = chunk.get_row(row_index).get_datum_row(&self.output_types);
            if self.seen.insert(self.row_key(&values)?) {
                if self.output_types.is_empty() {
                    result.set_num_virtual_rows(result.num_rows() + 1);
                } else {
                    for (column, value) in values.iter().enumerate() {
                        result.append_datum(column, value);
                    }
                }
            }
        }
        Ok(result)
    }

    fn compute_seed(&mut self) -> Result<(), ExecError> {
        self.current_iteration = 0;
        self.iter_in.lock().unwrap().set_iter(0);
        loop {
            if self.limit_done(&self.iter_in.lock().unwrap()) {
                break;
            }
            let mut chunk = self.seed.new_chunk();
            self.seed.next(&mut chunk)?;
            if chunk.num_rows() == 0 {
                break;
            }
            let admitted = self.deduplicate(&chunk)?;
            self.iter_in
                .lock()
                .unwrap()
                .add_chunk(admitted.copy_construct_sel())?;
            self.result.lock().unwrap().add_chunk(admitted)?;
        }
        self.current_iteration += 1;
        self.iter_in
            .lock()
            .unwrap()
            .set_iter(self.current_iteration);
        Ok(())
    }

    fn finish_iteration(&mut self) -> Result<(), ExecError> {
        let chunks = {
            let output = self
                .iter_out
                .as_ref()
                .ok_or_else(|| ExecError::internal("recursive CTE output storage is absent"))?;
            (0..output.num_chunks())
                .map(|index| Ok(output.get_chunk(index)?.copy_construct_sel()))
                .collect::<Result<Vec<_>, ExecError>>()?
        };

        if self.is_distinct {
            let mut admitted = Vec::with_capacity(chunks.len());
            for chunk in chunks {
                let chunk = self.deduplicate(&chunk)?;
                self.result
                    .lock()
                    .unwrap()
                    .add_chunk(chunk.copy_construct_sel())?;
                admitted.push(chunk);
            }
            self.iter_in.lock().unwrap().reopen()?;
            for chunk in admitted {
                self.iter_in.lock().unwrap().add_chunk(chunk)?;
            }
        } else {
            for chunk in &chunks {
                self.result
                    .lock()
                    .unwrap()
                    .add_chunk(chunk.copy_construct_sel())?;
            }
            self.iter_in.lock().unwrap().reopen()?;
            self.iter_in.lock().unwrap().swap_data(
                self.iter_out
                    .as_mut()
                    .ok_or_else(|| ExecError::internal("recursive CTE output storage is absent"))?,
            )?;
        }
        self.iter_out
            .as_mut()
            .ok_or_else(|| ExecError::internal("recursive CTE output storage is absent"))?
            .reopen()?;
        Ok(())
    }

    fn compute_recursive(&mut self) -> Result<(), ExecError> {
        if self.recursive.is_none() || self.iter_in.lock().unwrap().num_chunks() == 0 {
            return Ok(());
        }
        if self.current_iteration > self.context.cte_max_recursion_depth() as usize {
            return Err(ExecError::CteMaxRecursionDepth(
                self.current_iteration as u64,
            ));
        }
        if self.limit_done(&self.result.lock().unwrap()) {
            return Ok(());
        }

        loop {
            let mut chunk = self
                .recursive
                .as_ref()
                .ok_or_else(|| ExecError::internal("recursive CTE executor is absent"))?
                .new_chunk();
            self.recursive
                .as_mut()
                .ok_or_else(|| ExecError::internal("recursive CTE executor is absent"))?
                .next(&mut chunk)?;
            if chunk.num_rows() != 0 {
                self.iter_out
                    .as_mut()
                    .ok_or_else(|| ExecError::internal("recursive CTE output storage is absent"))?
                    .add_chunk(chunk)?;
                continue;
            }

            self.finish_iteration()?;
            if self.limit_done(&self.result.lock().unwrap())
                || self.iter_in.lock().unwrap().num_chunks() == 0
            {
                break;
            }
            self.current_iteration += 1;
            self.iter_in
                .lock()
                .unwrap()
                .set_iter(self.current_iteration);
            if self.current_iteration > self.context.cte_max_recursion_depth() as usize {
                return Err(ExecError::CteMaxRecursionDepth(
                    self.current_iteration as u64,
                ));
            }
            let recursive = self
                .recursive
                .as_mut()
                .ok_or_else(|| ExecError::internal("recursive CTE executor is absent"))?;
            recursive.close()?;
            recursive.open()?;
        }
        Ok(())
    }

    fn generate_result(&mut self) -> Result<(), ExecError> {
        if let Some(error) = &self.result_error {
            return Err(error.clone());
        }
        // Go computeSeedPart/computeRecursivePart turn panics into the error
        // cached by genCTEResult, before releasing shared materialization.
        let result = crate::sort_util::recover_worker_panic(|| {
            self.compute_seed().and_then(|()| self.compute_recursive())
        });
        match result {
            Ok(()) => {
                self.result.lock().unwrap().set_done();
                Ok(())
            }
            Err(error) => {
                self.result.lock().unwrap().set_error(format!("{error:?}"));
                self.result_error = Some(error.clone());
                Err(error)
            }
        }
    }
}

/// Go `CTEExec`: one reader cursor over the producer's shared complete result.
pub(crate) struct CteExec {
    meta: ExecutorMeta,
    producer: SharedCteProducer,
    chunk_index: usize,
    cursor: u64,
    met_first_batch: bool,
    has_limit: bool,
    limit_begin: u64,
    limit_end: u64,
}

impl CteExec {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        meta: ExecutorMeta,
        producer: SharedCteProducer,
        has_limit: bool,
        limit_begin: u64,
        limit_end: u64,
    ) -> Self {
        Self {
            meta,
            producer,
            chunk_index: 0,
            cursor: 0,
            met_first_batch: false,
            has_limit,
            limit_begin,
            limit_end,
        }
    }

    fn reset(&mut self) {
        self.chunk_index = 0;
        self.cursor = 0;
        self.met_first_batch = false;
    }

    fn next_unlimited(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        let chunk = {
            let producer = self.producer.lock().unwrap();
            let result = producer.result.lock().unwrap();
            if self.chunk_index >= result.num_chunks() {
                return Ok(());
            }
            let chunk = result.get_chunk(self.chunk_index)?.copy_construct_sel();
            chunk
        };
        *req = chunk;
        self.chunk_index += 1;
        Ok(())
    }

    fn next_limited(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if !self.met_first_batch {
            loop {
                let chunk = {
                    let producer = self.producer.lock().unwrap();
                    let result = producer.result.lock().unwrap();
                    if self.chunk_index >= result.num_chunks() {
                        return Ok(());
                    }
                    let chunk = result.get_chunk(self.chunk_index)?.copy_construct_sel();
                    chunk
                };
                self.chunk_index += 1;
                let rows = chunk.num_rows() as u64;
                let new_cursor = self.cursor.saturating_add(rows);
                if new_cursor >= self.limit_begin {
                    self.met_first_batch = true;
                    let begin = self.limit_begin.saturating_sub(self.cursor).min(rows);
                    let end = if new_cursor > self.limit_end {
                        self.limit_end.saturating_sub(self.cursor).min(rows)
                    } else {
                        rows
                    };
                    self.cursor = self.cursor.saturating_add(end);
                    if begin != end {
                        req.append_range_from(&chunk, begin as usize, end as usize);
                        return Ok(());
                    }
                    break;
                }
                self.cursor = new_cursor;
            }
        }

        let chunk = {
            let producer = self.producer.lock().unwrap();
            let result = producer.result.lock().unwrap();
            if self.chunk_index >= result.num_chunks() || self.cursor >= self.limit_end {
                return Ok(());
            }
            let chunk = result.get_chunk(self.chunk_index)?.copy_construct_sel();
            chunk
        };
        self.chunk_index += 1;
        let rows = (chunk.num_rows() as u64).min(self.limit_end - self.cursor);
        if rows == chunk.num_rows() as u64 {
            *req = chunk;
        } else {
            req.append_range_from(&chunk, 0, rows as usize);
        }
        self.cursor += rows;
        Ok(())
    }
}

impl Executor for CteExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.reset();
        self.producer.lock().unwrap().open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        {
            let mut producer = self.producer.lock().unwrap();
            if !producer.has_result() {
                if !producer.executor_opened {
                    producer.open()?;
                }
                producer.generate_result()?;
            }
        }
        if self.has_limit {
            self.next_limited(req)
        } else {
            self.next_unlimited(req)
        }
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.producer.lock().unwrap().close()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::RowCount;

    struct CteInput {
        meta: ExecutorMeta,
        panic_on_next: bool,
        calls: RowCount,
    }

    impl Executor for CteInput {
        fn open(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn close(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            self.calls.set(self.calls.get() + 1);
            assert!(!self.panic_on_next, "CTE input panic");
            req.reset();
            req.set_num_virtual_rows(usize::from(self.calls.get() == 1));
            Ok(())
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

    /// Go computeSeedPart/computeRecursivePart recover and genCTEResult
    /// caches the error. Other readers and Close still acquire the same lock.
    #[test]
    fn cte_input_panic_is_cached_and_does_not_poison_shared_producer() {
        for recursive_panic in [false, true] {
            let context = StmtContext::for_query();
            let meta = ExecutorMeta::new(Schema::new(vec![]), 1, 32, 1024);
            let storage = || {
                let mut storage = CteStorage::new(vec![], 1024, context.statement_memory());
                storage.open_and_ref().unwrap();
                Arc::new(Mutex::new(storage))
            };
            let failed_calls = RowCount::default();
            let input = |panic_on_next| {
                Box::new(CteInput {
                    meta: meta.clone(),
                    panic_on_next,
                    calls: if panic_on_next {
                        failed_calls.clone()
                    } else {
                        RowCount::default()
                    },
                }) as Box<dyn Executor>
            };
            let producer = Arc::new(Mutex::new(CteProducer::new(
                input(!recursive_panic),
                recursive_panic.then(|| input(true)),
                storage(),
                storage(),
                false,
                false,
                0,
                context.clone(),
                vec![],
                1024,
            )));
            let mut reader = CteExec::new(meta.clone(), producer.clone(), false, 0, 0);
            reader.open().unwrap();
            let mut output = reader.new_chunk();
            let first = crate::sort_util::recover_worker_panic(|| reader.next(&mut output));
            assert!(
                matches!(first, Err(ExecError::Internal(ref message)) if message.contains("CTE input panic"))
            );
            assert!(
                !producer.is_poisoned(),
                "a recovered child panic must not poison CTE ownership"
            );
            let mut peer = CteExec::new(meta, producer.clone(), false, 0, 0);
            peer.open().unwrap();
            let second = peer.next(&mut output);
            assert!(
                matches!(second, Err(ExecError::Internal(ref message)) if message.contains("CTE input panic"))
            );
            assert_eq!(
                failed_calls.get(),
                1,
                "another reader sees the cached error"
            );
            reader.close().unwrap();
            peer.close().unwrap();
            assert!(!producer.lock().unwrap().executor_opened);
        }
    }
}
