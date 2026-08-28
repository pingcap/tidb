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
        self.spill_action.as_ref().map_or_else(
            || {
                self.parallel_spill_action
                    .as_ref()
                    .map_or(0, |action| action.spill_times())
            },
            |action| action.spill_times(),
        )
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
}

/// Go `AllocPartialResult`'s fixed allocation delta for one aggregate. These
/// are the 64-bit Go struct sizes used by `pkg/executor/aggfuncs`; retained
/// variable data is charged separately by `UpdatePartialResult`.
fn go_partial_result_bytes(func: &AggFunc) -> usize {
    let base = match &func.kind {
        AggKind::Count => 8,
        AggKind::FinalCount => 16,
        AggKind::Sum => 48,
        AggKind::FirstRow | AggKind::Min | AggKind::Max => 48,
        AggKind::Avg => 48,
        AggKind::GroupConcat { .. } => 40,
        AggKind::Bit(_) => 8,
        AggKind::Variance { .. } => 24,
        AggKind::JsonArrayAgg { .. } => 48,
        AggKind::JsonObjectAgg { .. } => 24,
        AggKind::ApproxCountDistinct => 48,
        AggKind::ApproxPercentile(_) => 24,
    };
    // Go's DISTINCT implementations embed one value-set header in the
    // partial result; entries themselves arrive through UpdatePartialResult.
    base + usize::from(func.distinct) * 24
}

/// The per-entry share of Go's 312-byte `MemAwareMap[string, any]` bucket,
/// whose bucket holds eight entries. Go charges the whole bucket at each map
/// growth; distributing it keeps the same total while this Rust HashMap has
/// no bucket-allocation callback.
const GO_MAP_ENTRY_BYTES: usize = 312 / 8;

/// Go serial `getPartialResults` memory delta for a newly opened group.
pub(super) fn new_group_bytes(key_len: usize, funcs: &[AggFunc]) -> i64 {
    let per_group =
        key_len + GO_MAP_ENTRY_BYTES + funcs.iter().map(go_partial_result_bytes).sum::<usize>();
    i64::try_from(per_group).unwrap_or(i64::MAX)
}

/// Go parallel `getPartialResultsOfEachRow` additionally charges the
/// `[]PartialResult` pointer array (`partialResultNumInRow * 8`).
pub(super) fn parallel_new_group_bytes(key_len: usize, funcs: &[AggFunc]) -> i64 {
    new_group_bytes(key_len, funcs)
        .saturating_add(i64::try_from(funcs.len() * 8).unwrap_or(i64::MAX))
}

fn spill_error(error: tidb_chunk::chunk_in_disk::DiskError) -> ExecError {
    ExecError::SpillFailed(error.to_string())
}
