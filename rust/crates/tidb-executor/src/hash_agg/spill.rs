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

//! The ROUND machinery of `pkg/executor/aggregate`'s `unparallelExec`: reading
//! the child and then the previous round's spilled rows (`getNextChunk`),
//! deferring the rows of groups this round will not open
//! (`spillUnprocessedData`), and rolling to the next round (`resetSpillMode`).
//!
//! This is a CHILD module of [`super`] rather than a sibling so it can reach
//! the executor's private state, which is the state a round mutates.

use super::*;

const PARALLEL_INT_AGG_WORKERS: usize = 5;
// Keep the bounded input window large enough that the five short-lived worker
// threads amortize their setup over a meaningful scan batch.  Each q13 chunk
// is only 1K rows, so 40 chunks forced dozens of thread cohorts for 1.5M rows.
const PARALLEL_INT_AGG_CHUNKS_PER_WINDOW: usize = 256;

impl<C: HashAggContext> HashAggExec<C> {
    /// Bytes this aggregation has written to spill files (Go's `diskTracker`).
    #[must_use]
    pub fn bytes_in_disk(&self) -> i64 {
        self.disk_tracker.bytes_consumed()
    }

    /// How many times the aggregation entered spill mode; zero means it never
    /// spilled. For tests and diagnostics (Go `IsSpillTriggeredForTest`).
    #[must_use]
    pub fn spill_times(&self) -> u32 {
        self.spill_action
            .as_ref()
            .map_or(0, |action| action.spill_times())
    }

    /// Go `getNextChunk`: the child first, and once it is drained, the rows
    /// this aggregation deferred to disk in the PREVIOUS round.
    pub(super) fn get_next_chunk(&mut self) -> Result<(), ExecError> {
        self.child_chunk.reset();
        if !self.is_child_drained {
            self.child.next(&mut self.child_chunk)?;
            if self.child_chunk.num_rows() != 0 {
                self.child_returned_empty = false;
                return Ok(());
            }
            self.is_child_drained = true;
        }
        if self.offset_of_spilled_chks < self.num_of_spilled_chks {
            let in_disk = self
                .data_in_disk
                .as_mut()
                .expect("a spilled chunk count implies a spill file");
            self.child_chunk = in_disk
                .get_chunk(self.offset_of_spilled_chks)
                .map_err(spill_error)?;
            self.offset_of_spilled_chks += 1;
        }
        Ok(())
    }

    /// Go `spillUnprocessedData`: the rows of groups this round will not open.
    ///
    /// SIMPLIFICATION (unobservable): Go has a fast path that writes the whole
    /// child chunk when EVERY one of its rows is unprocessed, and otherwise
    /// fills `tmpChkForSpill` to capacity. This always goes through the
    /// temporary chunk, so a spill file's chunk BOUNDARIES can differ from
    /// Go's -- the rows, and their order, cannot, and nothing reads a spill
    /// file except the next round's sequential re-read.
    pub(super) fn spill_unprocessed_data(
        &mut self,
        chunk: &Chunk,
        sel: &[usize],
    ) -> Result<(), ExecError> {
        let full = self.child.max_chunk_size();
        for &row in sel {
            self.tmp_chk_for_spill.append_row(chunk.get_row(row));
            if self.tmp_chk_for_spill.num_rows() >= full {
                self.flush_tmp_spill_chunk()?;
            }
        }
        Ok(())
    }

    /// Writes the pending spill chunk out, if it holds anything.
    fn flush_tmp_spill_chunk(&mut self) -> Result<(), ExecError> {
        if self.tmp_chk_for_spill.num_rows() == 0 {
            return Ok(());
        }
        let field_types = self.child.ret_field_types().to_vec();
        let in_disk = match &mut self.data_in_disk {
            Some(in_disk) => in_disk,
            None => {
                let in_disk = DataInDiskByChunks::new(field_types, "", self.memory.spill_storage());
                in_disk.disk_tracker().attach_to(&self.disk_tracker);
                self.data_in_disk.insert(in_disk)
            }
        };
        in_disk.add(&self.tmp_chk_for_spill).map_err(spill_error)?;
        self.tmp_chk_for_spill.reset();
        Ok(())
    }

    /// Go `resetSpillMode`: drop the round's groups, decide whether another
    /// round is needed, and lower the spill flag.
    pub(super) fn reset_spill_mode(&mut self) {
        self.cursor = 0;
        self.groups.clear();
        self.ordered.clear();
        self.group_count = 0;
        self.prepared = false;
        // No NEW rows were deferred while this round ran, so every row has now
        // been aggregated: the aggregation is done.
        let spilled = self.data_in_disk.as_ref().map_or(0, |d| d.num_chunks());
        self.executed = self.num_of_spilled_chks == spilled;
        self.num_of_spilled_chks = spilled;
        self.tracker.replace_bytes_used(0);
        self.in_spill_mode.store(false, SeqCst);
    }

    /// Go `execute`: fold rows into groups until the input (child, then the
    /// previous round's spill file) is exhausted.
    pub(super) fn execute(&mut self) -> Result<(), ExecError> {
        let result = self.execute_impl();
        // Go's `defer`: whatever happened, the pending spill chunk is written.
        if result.is_ok() {
            self.flush_tmp_spill_chunk()?;
        }
        result
    }

    fn execute_impl(&mut self) -> Result<(), ExecError> {
        // The q13-shaped integer fast path keeps its historical priority;
        // the general partial/final worker pipeline takes every other
        // eligible aggregation, and the round machinery below is the serial
        // implementation.
        if let Some((group_column, group_unsigned, specs)) = self.parallel_int_agg_specs() {
            return self.execute_parallel_int_agg(group_column, group_unsigned, &specs);
        }
        if self.pipeline_mode {
            return C::run_parallel_pipeline_bridge(self)
                .expect("pipeline mode implies a context-provided bridge");
        }
        loop {
            let before = self.child_chunk.memory_usage();
            self.get_next_chunk()?;
            self.tracker
                .consume(self.child_chunk.memory_usage() - before);
            let rows = self.child_chunk.num_rows();
            if rows == 0 {
                return Ok(());
            }

            // The chunk moves out of `self` for the row loop, so folding a row
            // in can take `&mut self` while the row is borrowed from it.
            let chunk = std::mem::take(&mut self.child_chunk);
            let folded = self.fold_chunk(&chunk, rows);
            self.child_chunk = chunk;
            let sel = folded?;

            if !sel.is_empty() {
                let chunk = std::mem::take(&mut self.child_chunk);
                let spilled = self.spill_unprocessed_data(&chunk, &sel);
                self.child_chunk = chunk;
                spilled?;
            }
            // Where the spill action fires (soft limit) and, if spilling did
            // not save it, where the statement stops with 8175 (hard limit).
            self.memory.check()?;
        }
    }

    /// Runs the q13-shaped integer hash aggregate in bounded partial-worker
    /// windows. The main thread remains the child fetcher and final merger;
    /// workers only own chunks and typed integer state, so no non-`Send`
    /// executor or session context crosses the thread boundary.
    fn execute_parallel_int_agg(
        &mut self,
        group_column: usize,
        group_unsigned: bool,
        specs: &[ParallelIntAggSpec],
    ) -> Result<(), ExecError> {
        if let [spec @ (ParallelIntAggSpec::Count(_) | ParallelIntAggSpec::FinalCount { .. })] =
            specs
        {
            return self.execute_parallel_int_count_agg(group_column, group_unsigned, spec);
        }
        let mut global: ParallelIntMap<ParallelIntGroup> = ParallelIntMap::default();
        let mut next_sequence = 0usize;
        let mut child_drained = false;

        while !child_drained {
            let mut batch = Vec::with_capacity(PARALLEL_INT_AGG_CHUNKS_PER_WINDOW);
            for _ in 0..PARALLEL_INT_AGG_CHUNKS_PER_WINDOW {
                let before = self.child_chunk.memory_usage();
                self.child_chunk.reset();
                self.child.next(&mut self.child_chunk)?;
                self.tracker
                    .consume(self.child_chunk.memory_usage() - before);
                let rows = self.child_chunk.num_rows();
                if rows == 0 {
                    self.is_child_drained = true;
                    child_drained = true;
                    break;
                }
                self.child_returned_empty = false;
                // Keep a schema-compatible request chunk installed while the
                // fetched batch is owned by workers. Leaving the field as a
                // zero-value `Chunk` would make the next child call (and the
                // HashJoin output swap beneath it) lose all columns.
                let replacement = self.child.new_chunk();
                let chunk = std::mem::replace(&mut self.child_chunk, replacement);
                batch.push((chunk, next_sequence));
                next_sequence += rows;
            }
            if batch.is_empty() {
                break;
            }

            let workers = batch.len().min(PARALLEL_INT_AGG_WORKERS);
            let mut partitions: Vec<Vec<(Chunk, usize)>> =
                (0..workers).map(|_| Vec::new()).collect();
            for (index, chunk) in batch.into_iter().enumerate() {
                partitions[index % workers].push(chunk);
            }
            let partial_maps = std::thread::scope(|scope| {
                let handles = partitions.into_iter().map(|chunks| {
                    scope.spawn(move || {
                        Self::parallel_int_agg_chunk(chunks, group_column, group_unsigned, specs)
                    })
                });
                handles
                    .map(|handle| {
                        handle
                            .join()
                            .expect("parallel hash aggregate worker panicked")
                    })
                    .collect::<Vec<_>>()
            });
            for partial in partial_maps {
                let partial = partial?;
                for (key, incoming) in partial {
                    match global.entry(key) {
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            slot.insert(incoming);
                        }
                        std::collections::hash_map::Entry::Occupied(mut slot) => {
                            let current = slot.get_mut();
                            let incoming_is_first = incoming.first_seq < current.first_seq;
                            if incoming_is_first {
                                current.first_seq = incoming.first_seq;
                            }
                            for (index, count) in incoming.counts.into_iter().enumerate() {
                                current.counts[index] = current.counts[index].wrapping_add(count);
                            }
                            for (index, sum) in incoming.decimal_sums.into_iter().enumerate() {
                                if let Some(sum) = sum {
                                    current.decimal_sums[index] =
                                        Some(match current.decimal_sums[index].take() {
                                            Some(existing) => existing.add(sum),
                                            None => sum,
                                        });
                                }
                            }
                            for (index, value) in incoming.first_rows.into_iter().enumerate() {
                                if incoming_is_first || current.first_rows[index].is_none() {
                                    if let Some(value) = value {
                                        current.first_rows[index] = Some(value);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            self.parallel_agg_windows += 1;
            self.memory.check()?;
        }
        self.finish_parallel_int_agg(global, specs);
        self.executed = true;
        Ok(())
    }

    /// COUNT-only specialization of the bounded integer aggregate. Keeping
    /// the accumulator inline in the map value avoids one heap allocation per
    /// local group; q13's inner aggregation opens 150k groups in several
    /// worker maps before the final merge.
    fn execute_parallel_int_count_agg(
        &mut self,
        group_column: usize,
        group_unsigned: bool,
        spec: &ParallelIntAggSpec,
    ) -> Result<(), ExecError> {
        let mut global: ParallelIntMap<ParallelIntCountGroup> = ParallelIntMap::default();
        let mut next_sequence = 0usize;
        let mut child_drained = false;

        while !child_drained {
            let mut batch = Vec::with_capacity(PARALLEL_INT_AGG_CHUNKS_PER_WINDOW);
            for _ in 0..PARALLEL_INT_AGG_CHUNKS_PER_WINDOW {
                let before = self.child_chunk.memory_usage();
                self.child_chunk.reset();
                self.child.next(&mut self.child_chunk)?;
                self.tracker
                    .consume(self.child_chunk.memory_usage() - before);
                let rows = self.child_chunk.num_rows();
                if rows == 0 {
                    self.is_child_drained = true;
                    child_drained = true;
                    break;
                }
                self.child_returned_empty = false;
                let replacement = self.child.new_chunk();
                let chunk = std::mem::replace(&mut self.child_chunk, replacement);
                batch.push((chunk, next_sequence));
                next_sequence += rows;
            }
            if batch.is_empty() {
                break;
            }

            let workers = batch.len().min(PARALLEL_INT_AGG_WORKERS);
            let mut partitions: Vec<Vec<(Chunk, usize)>> =
                (0..workers).map(|_| Vec::new()).collect();
            for (index, chunk) in batch.into_iter().enumerate() {
                partitions[index % workers].push(chunk);
            }
            let partial_maps = std::thread::scope(|scope| {
                let handles = partitions.into_iter().map(|chunks| {
                    scope.spawn(move || {
                        Self::parallel_int_count_chunk(chunks, group_column, group_unsigned, spec)
                    })
                });
                handles
                    .map(|handle| {
                        handle
                            .join()
                            .expect("parallel COUNT aggregate worker panicked")
                    })
                    .collect::<Vec<_>>()
            });
            for partial in partial_maps {
                for (key, incoming) in partial {
                    match global.entry(key) {
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            slot.insert(incoming);
                        }
                        std::collections::hash_map::Entry::Occupied(mut slot) => {
                            let current = slot.get_mut();
                            current.first_seq = current.first_seq.min(incoming.first_seq);
                            current.count = current.count.wrapping_add(incoming.count);
                        }
                    }
                }
            }
            self.parallel_agg_windows += 1;
            self.memory.check()?;
        }
        self.finish_parallel_int_count_agg(global);
        self.executed = true;
        Ok(())
    }
}

/// The bytes one newly opened group costs the tracker: Go's
/// `getPartialResults` charges `len(groupKey)`, the map entry
/// (`partialResultMap.Set`) and each function's `AllocPartialResult`.
///
/// DIVERGENCE (named): Go's per-function `AllocPartialResult` sizes are the Go
/// struct sizes of each `partialResult4*`; this charges the Rust state's size,
/// which is the memory THIS process holds. The exact byte at which a given
/// quota is crossed therefore differs from Go's, the same way the write path's
/// accounting does -- what has to hold is that the number grows with group
/// cardinality, which is the quantity spilling exists to bound.
pub(super) fn new_group_bytes(key_len: usize, num_funcs: usize) -> i64 {
    let per_group = key_len
        // The map entry: the key is stored a second time, with its bucket slot.
        + size_of::<usize>()
        + num_funcs * size_of::<AggState>();
    i64::try_from(per_group).unwrap_or(i64::MAX)
}

fn spill_error(error: tidb_chunk::chunk_in_disk::DiskError) -> ExecError {
    ExecError::SpillFailed(error.to_string())
}
