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

//! Go fetchAndBuildHashTableImpl: bounded input admission, disjoint worker
//! partitions, then merged-table linking. The coordinator owns the build child;
//! only owned chunks cross queues. Build workers finish before the table can
//! be probed or released; ready linking tasks share the executor CPU pool.
//! A spill request drains admitted chunks before the
//! coordinator borrows every worker's tables for parallel partition writes.

use crossbeam_channel::{bounded, select_biased};
use tidb_chunk::{chunk::Chunk, chunk_in_disk::DataInDiskByChunks};
use tidb_datatype::FieldType;

use super::spill::HashJoinSpill;
use super::HashJoinV2Exec;
use crate::hash_table_v2::SharedHashTableSlots;
use crate::join_row_table::RowTableSegment;
use crate::row_table_builder::{BuildContext, RowTableBuildError};
use crate::sort_util::recover_worker_panic;
use crate::{ExecError, Executor, StatementMemory};

enum BuildInput<'a> {
    Child(&'a mut dyn Executor),
    Restored {
        files: Vec<&'a mut DataInDiskByChunks>,
        fields: &'a [FieldType],
    },
}

fn build_error(error: RowTableBuildError) -> ExecError {
    match error {
        RowTableBuildError::Expression(error) => ExecError::Eval(error),
        RowTableBuildError::Killed(error) => ExecError::Killed(error),
        other => ExecError::internal(other.to_string()),
    }
}

impl HashJoinV2Exec {
    /// Consumes an already-open build child through native Next(chunk), then
    /// waits for both build barriers. Does not open/close the child: Go's parent
    /// executor owns those calls. The caller must provide spill orchestration
    /// before selecting this pipeline for SQL with temporary storage enabled.
    pub fn fetch_and_build_hash_table(
        &mut self,
        source: &mut dyn Executor,
        context: &BuildContext<'_>,
        memory: &StatementMemory,
    ) -> Result<usize, ExecError> {
        self.fetch_and_build_hash_table_impl(BuildInput::Child(source), context, memory, None)
    }

    /// The spill-aware build uses the same workers and input pool. Its OOM
    /// action only requests a fetcher barrier; IO runs after admitted chunks
    /// have returned, before any row-table storage can be freed.
    pub fn fetch_and_build_hash_table_with_spill(
        &mut self,
        source: &mut dyn Executor,
        context: &BuildContext<'_>,
        memory: &StatementMemory,
        spill: &mut HashJoinSpill,
    ) -> Result<usize, ExecError> {
        spill.action.set_can_spill(true);
        self.fetch_and_build_hash_table_impl(
            BuildInput::Child(source),
            context,
            memory,
            Some(spill),
        )
    }

    pub fn fetch_and_build_restored_hash_table(
        &mut self,
        files: Vec<&mut DataInDiskByChunks>,
        fields: &[FieldType],
        context: &BuildContext<'_>,
        memory: &StatementMemory,
        spill: &mut HashJoinSpill,
    ) -> Result<usize, ExecError> {
        spill.action.set_can_spill(true);
        self.fetch_and_build_hash_table_impl(
            BuildInput::Restored { files, fields },
            context,
            memory,
            Some(spill),
        )
    }

    fn fetch_and_build_hash_table_impl(
        &mut self,
        source: BuildInput<'_>,
        context: &BuildContext<'_>,
        memory: &StatementMemory,
        mut spill: Option<&mut HashJoinSpill>,
    ) -> Result<usize, ExecError> {
        self.release_build_memory();
        self.hash_table_context
            .memory_tracker
            .attach_to(memory.stmt_tracker());
        self.begin_build(context.meta.null_map_length);
        let result = recover_worker_panic(|| {
            self.split_build_chunks(source, context, memory, spill.as_deref_mut())?;
            memory.check()?;
            let count = if let Some(spill) = spill.as_deref_mut() {
                self.hash_table_context
                    .merge_row_tables_with_spill(self.ctx.partition_number, spill)?
            } else {
                self.hash_table_context
                    .merge_row_tables_to_hash_table(self.ctx.partition_number)
            };
            memory.check()?;
            self.link_build_tasks(count, memory)?;
            Ok(count)
        });
        if result.is_err() {
            if let Some(spill) = spill {
                spill.action.set_can_spill(false);
            }
            self.release_build_memory();
        }
        result
    }

    /// Release all build buffers and their accounting together, after workers
    /// have joined. The tracker remains attached until the owning executor closes.
    pub fn release_build_memory(&mut self) {
        for worker in &mut self.build_workers {
            worker.builder = None;
        }
        for table in self.hash_table_context.row_tables.iter_mut().flatten() {
            *table = None;
        }
        for table in &mut self.hash_table_context.hash_table.tables {
            *table = None;
        }
        let tracker = &self.hash_table_context.memory_tracker;
        tracker.consume(-tracker.bytes_consumed());
    }

    fn split_build_chunks(
        &mut self,
        source: BuildInput<'_>,
        context: &BuildContext<'_>,
        memory: &StatementMemory,
        mut spill: Option<&mut HashJoinSpill>,
    ) -> Result<(), ExecError> {
        let concurrency = self.ctx.concurrency;
        let (mut child, restored_files, fields) = match source {
            BuildInput::Child(source) => (Some(source), Vec::new(), &[][..]),
            BuildInput::Restored { files, fields } => {
                assert_eq!(files.len(), concurrency);
                (None, files, fields)
            }
        };
        let restored = child.is_none();
        let mut remaining_chunks: usize = restored_files.iter().map(|file| file.num_chunks()).sum();
        let mut restored_files = restored_files.into_iter();
        let tracker = &self.hash_table_context.memory_tracker;
        let killed = memory.sql_killer().get_kill_event_chan();
        // A lock is held once per chunk by its own worker, never per row. The
        // fetcher takes these only at the zero-in-flight spill barrier.
        let states: Vec<_> = self
            .build_workers
            .iter_mut()
            .zip(&mut self.hash_table_context.row_tables)
            .map(std::sync::Mutex::new)
            .collect();
        std::thread::scope(|scope| {
            let (input_tx, input_rx) = bounded(1);
            // At most N workers, one queued chunk and one fetcher-owned chunk.
            let (resource_tx, resource_rx) = bounded(concurrency + 2);
            let (error_tx, error_rx) = bounded(concurrency);
            let (close, close_rx) = bounded::<()>(0);
            let mut close = Some(close);
            let mut handles = Vec::with_capacity(concurrency);
            for state in &states {
                let mut disk = restored_files.next();
                let (input, resources, errors, close, killed) = (
                    input_rx.clone(),
                    resource_tx.clone(),
                    error_tx.clone(),
                    close_rx.clone(),
                    killed.clone(),
                );
                handles.push(scope.spawn(move || {
                    let result = recover_worker_panic(|| {
                        let mut build_context = BuildContext::new(context.meta, context.partition, context.key_serializer);
                        build_context.build_filter = context.build_filter;
                        build_context.memory_tracker = Some(tracker);
                        build_context.sql_killer = Some(memory.sql_killer());
                        let mut restored_index = 0;
                        let mut restored_chunk = disk.as_ref().map(|_| Chunk::new_with_capacity(fields, super::spill::SPILL_CHUNK_SIZE));
                        loop {
                            if disk.as_ref().is_some_and(|disk| restored_index == disk.num_chunks()) { return Ok(()); }
                            let mut chunk = select_biased! {
                                recv(close) -> _ => return Ok(()),
                                recv(killed) -> _ => { memory.check()?; return Ok(()); },
                                recv(input) -> chunk => match chunk { Ok(chunk) => chunk, Err(_) => return Ok(()) },
                            };
                            memory.check()?;
                            let build_chunk = if let Some(disk) = disk.as_mut() {
                                let restored_chunk = restored_chunk.as_mut().expect("worker restore buffer");
                                disk.fill_chunk(restored_index, restored_chunk)
                                    .map_err(|error| ExecError::SpillFailed(error.to_string()))?;
                                restored_index += 1;
                                &*restored_chunk
                            } else { &chunk };
                            {
                                let mut state = state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                                let (worker, row_tables) = &mut *state;
                                if restored {
                                    worker.process_one_restored_chunk(build_chunk, &mut build_context, row_tables)
                                } else {
                                    worker.process_one_chunk(build_chunk, &mut build_context, row_tables)
                                }.map_err(build_error)?;
                            }
                            memory.check()?;
                            chunk.reset();
                            select_biased! {
                                recv(close) -> _ => return Ok(()),
                                recv(killed) -> _ => { memory.check()?; return Ok(()); },
                                send(resources, chunk) -> result => if result.is_err() { return Ok(()); },
                            }
                        }
                    });
                    if let Err(error) = &result { let _ = errors.send(error.clone()); }
                    result
                }));
            }
            drop(input_rx);
            drop(resource_tx);
            drop(error_tx);
            let mut spare = Vec::new();
            let mut in_flight = 0;
            let workers_stopped = std::cell::Cell::new(false);
            let receive_resource = || -> Result<Option<tidb_chunk::chunk::Chunk>, ExecError> {
                select_biased! {
                    recv(error_rx) -> error => match error { Ok(error) => Err(error), Err(_) => Ok(resource_rx.try_recv().ok()) },
                    recv(killed) -> _ => { memory.check()?; Err(ExecError::internal("build killed without SQL signal")) },
                    recv(resource_rx) -> chunk => Ok(chunk.ok()),
                }
            };
            macro_rules! returned_chunk {
                () => {
                    match receive_resource()? {
                        Some(chunk) => chunk,
                        None => {
                            workers_stopped.set(true);
                            return Ok(());
                        }
                    }
                };
            }
            let produced = recover_worker_panic(|| {
                // Allocate only the bounded resource pool, never the whole input.
                for _ in 0..concurrency + 2 {
                    spare.push(match child.as_ref() {
                        Some(child) => child.new_chunk(),
                        // Go sends nil admission tokens during restore. Each
                        // worker owns one actual disk-read buffer above.
                        None => Chunk::new_empty(&[]),
                    });
                }
                loop {
                    memory.check()?;
                    if let Some(spill) = spill.as_deref_mut() {
                        if spill.action.is_spill_needed() {
                            while in_flight > 0 {
                                spare.push(returned_chunk!());
                                in_flight -= 1;
                            }
                            let mut locked: Vec<_> = states
                                .iter()
                                .map(|state| {
                                    state
                                        .lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                                })
                                .collect();
                            let mut tables: Vec<_> = locked
                                .iter_mut()
                                .map(|state| state.1.as_mut_slice())
                                .collect();
                            spill.spill_rows(&mut tables, None, false)?;
                        }
                    }
                    let mut chunk = match spare.pop() {
                        Some(chunk) => chunk,
                        None => {
                            let chunk = returned_chunk!();
                            in_flight -= 1;
                            chunk
                        }
                    };
                    if let Some(child) = child.as_mut() {
                        child.next(&mut chunk)?;
                        if chunk.num_rows() == 0 {
                            break;
                        }
                    } else {
                        if remaining_chunks == 0 {
                            break;
                        }
                        remaining_chunks -= 1;
                    }
                    select_biased! {
                        recv(error_rx) -> error => match error {
                            Ok(error) => return Err(error),
                            Err(_) => { workers_stopped.set(true); return Ok(()); }
                        },
                        recv(killed) -> _ => { memory.check()?; return Ok(()); },
                        send(input_tx, chunk) -> sent => if sent.is_err() { workers_stopped.set(true); return Ok(()); },
                    }
                    in_flight += 1;
                }
                // Go waits for admitted chunks before spilling the final rows
                // of already-spilled partitions. Retain all other partitions.
                while in_flight > 0 {
                    spare.push(returned_chunk!());
                    in_flight -= 1;
                }
                if let Some(spill) = spill.as_deref_mut() {
                    if spill.spilled_partitions().iter().any(|&spilled| spilled) {
                        let mut locked: Vec<_> = states
                            .iter()
                            .map(|state| {
                                state
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                            })
                            .collect();
                        let mut tables: Vec<_> = locked
                            .iter_mut()
                            .map(|state| state.1.as_mut_slice())
                            .collect();
                        spill.spill_rows(&mut tables, None, true)?;
                    }
                }
                Ok(())
            });
            drop(input_tx);
            if produced.is_err() || workers_stopped.get() {
                drop(close.take());
            }
            let mut result = produced;
            for handle in handles {
                let completed = handle.join().expect("build worker catches its panics");
                if result.is_ok() {
                    result = completed;
                }
            }
            // A disconnected input/resource queue can win selection as a
            // worker publishes its failure. The worker result, not transport
            // closure, owns the error. Join first, as Go waitJobDone does.
            if result.is_ok() && workers_stopped.get() {
                result = memory.check().and_then(|()| {
                    Err(ExecError::internal(
                        "build workers stopped without an error",
                    ))
                });
            }
            drop(close);
            result
        })
    }

    fn link_build_tasks(
        &mut self,
        segment_count: usize,
        memory: &StatementMemory,
    ) -> Result<(), ExecError> {
        struct Task {
            partition: usize,
            slots: SharedHashTableSlots,
            segments: Vec<RowTableSegment>,
            atomic: bool,
        }
        let tasks = self.create_tasks(segment_count);
        let total_lengths: Vec<_> = self
            .hash_table_context
            .hash_table
            .tables
            .iter()
            .map(|table| table.as_ref().unwrap().row_data.segments.len())
            .collect();
        let mut partitions: Vec<_> = self
            .hash_table_context
            .hash_table
            .tables
            .iter_mut()
            .map(|table| {
                let table = table.as_mut().unwrap();
                (
                    table.shared_slots(),
                    std::mem::take(&mut table.row_data.segments).into_iter(),
                )
            })
            .collect();
        let concurrency = self.ctx.concurrency;
        let tag_helper = self.hash_table_context.tag_helper;
        let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        // Go's round-robin segment tasks are all ready after the merge barrier.
        // Move each disjoint range to its task, sharing only the atomic buckets.
        // Row buffers keep their allocation and synthetic addresses. Completed
        // ranges return in task order before any probe can read the table.
        let ready = tasks.into_iter().map(|task| {
            let (slots, remaining) = &mut partitions[task.partition_idx];
            let mut task = Task {
                partition: task.partition_idx,
                slots: slots.clone(),
                segments: remaining
                    .by_ref()
                    .take(task.seg_end_idx - task.seg_start_idx)
                    .collect(),
                atomic: task.seg_start_idx != 0
                    || task.seg_end_idx != total_lengths[task.partition_idx],
            };
            let stopped = std::sync::Arc::clone(&stopped);
            let memory = memory.clone();
            move || {
                use std::sync::atomic::Ordering;
                let result = recover_worker_panic(|| {
                    if stopped.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    memory.check()?;
                    task.slots.as_slots().build_segments_with_mode(
                        &mut task.segments,
                        &tag_helper,
                        task.atomic,
                    );
                    memory.check()
                });
                if result.is_err() {
                    stopped.store(true, Ordering::Release);
                }
                (task.partition, task.segments, result)
            }
        });
        // Join and restore every owned range, including after the first error.
        let mut result = Ok(());
        for (partition, segments, outcome) in crate::worker_pool::map(ready, concurrency) {
            self.hash_table_context.hash_table.tables[partition]
                .as_mut()
                .unwrap()
                .row_data
                .segments
                .extend(segments);
            result = result.and(outcome);
        }
        result
    }
}
