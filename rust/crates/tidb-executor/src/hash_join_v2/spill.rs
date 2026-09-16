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

//! Go hash_join_spill.go and hash_join_spill_helper.go: flag-only OOM action,
//! quiescent partition selection, chunk-backed spill files and restore stack.
//! The build coordinator owns mutation; workers get disjoint lanes. Probe
//! workers route deferred rows here; the executor owns recursive restore rounds.

use std::sync::{Arc, Condvar, Mutex};

use tidb_chunk::{chunk::Chunk, chunk_in_disk::DataInDiskByChunks};
use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_util::memory::{ActionOnExceed, ArcAction, BaseOomAction, Tracker, DEF_SPILL_PRIORITY};

use crate::join_row_table::{RowTable, RowTableSegment};
use crate::sort_util::recover_worker_panic;
use crate::{ExecError, StatementMemory};

/// Go spillChunkSize; both disk sides use native columnar batches.
pub const SPILL_CHUNK_SIZE: usize = 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
enum SpillStatus {
    NotSpilled,
    NeedSpill,
    InSpilling,
}

struct Control {
    status: SpillStatus,
    can_spill: bool,
    consumed: i64,
    limit: i64,
}

/// The action never performs IO while a row builder owns a chunk. It requests
/// the fetcher's barrier, or delegates to the previous executor/cancel action.
pub struct HashJoinSpillAction {
    base: BaseOomAction,
    control: Mutex<Control>,
    ready: Condvar,
    tracker: Arc<Tracker>,
}

impl HashJoinSpillAction {
    fn new(tracker: Arc<Tracker>) -> Self {
        Self {
            base: BaseOomAction::default(),
            control: Mutex::new(Control {
                status: SpillStatus::NotSpilled,
                can_spill: false,
                consumed: 0,
                limit: 0,
            }),
            ready: Condvar::new(),
            tracker,
        }
    }

    /// Fetcher admission stops when the quota action requests a barrier.
    pub fn is_spill_needed(&self) -> bool {
        self.control.lock().unwrap().status == SpillStatus::NeedSpill
    }

    /// Go disables spilling after table merge and reenables it for restore.
    pub fn set_can_spill(&self, enabled: bool) {
        self.control.lock().unwrap().can_spill = enabled;
    }

    fn finish_spill(&self) {
        self.control.lock().unwrap().status = SpillStatus::NotSpilled;
        self.ready.notify_all();
    }
}

impl ActionOnExceed for HashJoinSpillAction {
    fn action(&self, tracker: &Arc<Tracker>) {
        let mut control = self.control.lock().unwrap();
        while control.status == SpillStatus::InSpilling {
            control = self.ready.wait(control).unwrap();
        }
        let enough = self.tracker.bytes_consumed() >= tracker.get_bytes_limit() / 20;
        let exceeds = tracker.check_exceed();
        if exceeds && control.status == SpillStatus::NotSpilled && enough && control.can_spill {
            control.status = SpillStatus::NeedSpill;
            control.consumed = tracker.bytes_consumed();
            control.limit = tracker.get_bytes_limit();
            return;
        }
        let fallback = exceeds && (!enough || !control.can_spill);
        drop(control);
        if fallback {
            if let Some(action) = self.get_fallback() {
                action.action(tracker);
            }
        }
    }
    fn set_fallback(&self, action: Option<ArcAction>) {
        self.base.set_fallback(action);
    }
    fn get_fallback(&self) -> Option<ArcAction> {
        self.base.get_fallback()
    }
    fn get_priority(&self) -> i64 {
        DEF_SPILL_PRIORITY
    }
    fn set_finished(&self) {
        self.base.set_finished();
    }
    fn is_finished(&self) -> bool {
        self.base.is_finished()
    }
}

/// A worker's files for one partition; the paired probe store is created with
/// the build store even when that worker contributes zero build rows.
pub struct PartitionFiles {
    /// Hash, valid-key byte and packed build-row bytes.
    pub build: DataInDiskByChunks,
    /// Hash, serialized key and original probe columns.
    pub probe: DataInDiskByChunks,
}

struct SpillLane {
    files: Vec<Option<PartitionFiles>>,
    chunk: Chunk,
    valid_keys: Vec<u8>,
}

/// One partition popped in Go's LIFO restore order. Dropping it closes/removes
/// both sides' files, including after errors or an early executor Close.
pub struct RestorePartition {
    /// Paired stores in build-worker order.
    pub files: Vec<PartitionFiles>,
    /// Original spill is round one; recursive repartitioning increments it.
    pub round: usize,
}

/// Build spill ownership, reusable worker buffers and pending restore files.
pub struct HashJoinSpill {
    /// Registered statement-quota action; it never owns row tables or files.
    pub action: Arc<HashJoinSpillAction>,
    memory: StatementMemory,
    tracker: Arc<Tracker>,
    disk_tracker: Arc<Tracker>,
    build_types: Vec<FieldType>,
    probe_types: Vec<FieldType>,
    concurrency: usize,
    lanes: Vec<Arc<Mutex<SpillLane>>>,
    spilled: Vec<bool>,
    stack: Vec<RestorePartition>,
    valid_rows: u64,
}

impl HashJoinSpill {
    /// Prepare metadata and trackers; allocate lanes only when spilling begins.
    pub fn new(
        concurrency: usize,
        partitions: usize,
        probe_types: &[FieldType],
        tracker: Arc<Tracker>,
        memory: StatementMemory,
        label: i64,
    ) -> Self {
        let build_types = vec![
            FieldType::new(FieldTypeCode::LongLong).with_flags(FieldTypeFlags::UNSIGNED),
            FieldType::new(FieldTypeCode::Bit),
            FieldType::new(FieldTypeCode::Bit),
        ];
        let mut spilled_probe_types = build_types[..2].to_vec();
        spilled_probe_types.extend_from_slice(probe_types);
        Self {
            action: Arc::new(HashJoinSpillAction::new(Arc::clone(&tracker))),
            disk_tracker: memory.operator_disk_tracker(label),
            memory,
            tracker,
            concurrency,
            lanes: Vec::new(),
            build_types,
            probe_types: spilled_probe_types,
            spilled: vec![false; partitions],
            stack: Vec::new(),
            valid_rows: 0,
        }
    }

    /// Matches OpenSelf's registration boundary. A single partition cannot
    /// recursively repartition, so Go leaves the previous action in place.
    pub fn register(&self) {
        if self.memory.tmp_storage_on_oom() && self.spilled.len() > 1 {
            self.memory
                .session_tracker()
                .fallback_old_and_set_new_action(self.action.clone());
        }
    }

    /// Current-round partition routing mask.
    pub fn spilled_partitions(&self) -> &[bool] {
        &self.spilled
    }
    /// Native probe-spill layout: saved hash, serialized key, original columns.
    pub fn probe_field_types(&self) -> &[FieldType] {
        &self.probe_types
    }
    pub fn build_field_types(&self) -> &[FieldType] {
        &self.build_types
    }
    /// Valid build keys written in this round, excluding NULL/filtered rows.
    pub fn valid_spilled_rows(&self) -> u64 {
        self.valid_rows
    }
    /// Bytes held by current-round build stores.
    pub fn build_spill_bytes(&self) -> i64 {
        self.lanes
            .iter()
            .map(|lane| {
                lane.lock()
                    .unwrap()
                    .files
                    .iter()
                    .flatten()
                    .map(|files| files.build.total_bytes_in_disk())
                    .sum::<i64>()
            })
            .sum()
    }
    /// Bytes held by current-round probe stores.
    pub fn probe_spill_bytes(&self) -> i64 {
        self.lanes
            .iter()
            .map(|lane| {
                lane.lock()
                    .unwrap()
                    .files
                    .iter()
                    .flatten()
                    .map(|files| files.probe.total_bytes_in_disk())
                    .sum::<i64>()
            })
            .sum()
    }

    fn choose_partitions(&self, usage: &[i64], remaining_only: bool) -> Vec<usize> {
        let mut selected: Vec<_> = self
            .spilled
            .iter()
            .enumerate()
            .filter_map(|(id, &spilled)| spilled.then_some(id))
            .collect();
        if remaining_only {
            return selected;
        }
        let limit = self.action.control.lock().unwrap().limit;
        let mut remaining =
            self.tracker.bytes_consumed() - selected.iter().map(|&id| usage[id]).sum::<i64>();
        if (remaining as f64) <= limit as f64 * 0.5 {
            return selected;
        }
        let mut candidates: Vec<_> = self
            .spilled
            .iter()
            .enumerate()
            .filter_map(|(id, &spilled)| (!spilled).then_some(id))
            .collect();
        candidates.sort_by_key(|&id| std::cmp::Reverse(usage[id]));
        for id in candidates {
            selected.push(id);
            remaining -= usage[id];
            if (remaining as f64) <= limit as f64 * 0.5 {
                break;
            }
        }
        selected
    }

    /// Called only after all admitted chunks return. Each worker's tables and
    /// disk lane are exclusively borrowed during the parallel write barrier.
    /// Bucket bytes, when supplied, were precharged before choosing partitions.
    pub fn spill_rows(
        &mut self,
        tables: &mut [&mut [Option<RowTable>]],
        hash_bytes: Option<&[i64]>,
        remaining_only: bool,
    ) -> Result<(), ExecError> {
        self.action.control.lock().unwrap().status = SpillStatus::InSpilling;
        let result = recover_worker_panic(|| {
            self.memory.check()?;
            // Go init/initTmpSpillBuildSideChunks are lazy: an in-memory join
            // must not allocate disk lanes or their spill chunks.
            if self.lanes.is_empty() {
                self.lanes = (0..self.concurrency)
                    .map(|_| {
                        Arc::new(Mutex::new(SpillLane {
                            files: (0..self.spilled.len()).map(|_| None).collect(),
                            chunk: Chunk::new(
                                &self.build_types,
                                SPILL_CHUNK_SIZE,
                                SPILL_CHUNK_SIZE,
                            ),
                            valid_keys: Vec::new(),
                        }))
                    })
                    .collect();
            }
            let usage: Vec<_> = (0..self.spilled.len())
                .map(|id| {
                    tables
                        .iter()
                        .filter_map(|lane| lane[id].as_ref())
                        .map(RowTable::get_total_memory_usage)
                        .sum::<i64>()
                        + hash_bytes.map_or(0, |bytes| bytes[id])
                })
                .collect();
            let selected = self.choose_partitions(&usage, remaining_only);
            for &id in &selected {
                self.spilled[id] = true;
            }
            let released: i64 = selected.iter().map(|&id| usage[id]).sum();
            let (consumed, quota) = {
                let control = self.action.control.lock().unwrap();
                (control.consumed, control.limit)
            };
            tracing::info!(consumed, quota, "memory exceeds quota, spill to disk now.");
            let memory = self.memory.clone();
            let disk_tracker = Arc::clone(&self.disk_tracker);
            let build_types = self.build_types.clone();
            let probe_types = self.probe_types.clone();
            let spill_lanes =
                crate::worker_pool::LanePool::new("hash-join-spill", self.concurrency);

            // Take only the selected row-table options out of the caller's
            // slices. The spill lanes themselves stay in `self` and retain
            // their disk handles and reusable chunks between barriers.
            let mut jobs = Vec::with_capacity(tables.len());
            for (worker, worker_tables) in tables.iter_mut().enumerate() {
                let owned_tables = selected
                    .iter()
                    .map(|&id| (id, worker_tables[id].take()))
                    .collect::<Vec<_>>();
                jobs.push((worker, owned_tables));
            }

            let (result_tx, result_rx) = std::sync::mpsc::sync_channel(tables.len());
            for (worker, mut owned_tables) in jobs {
                let lane = Arc::clone(&self.lanes[worker]);
                let selected = selected.clone();
                let memory = memory.clone();
                let disk_tracker = Arc::clone(&disk_tracker);
                let build_types = build_types.clone();
                let probe_types = probe_types.clone();
                let result_tx = result_tx.clone();
                spill_lanes
                    .submit(move || {
                        let outcome = recover_worker_panic(|| {
                            let mut valid = 0;
                            let mut lane =
                                lane.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                            let SpillLane {
                                files: lane_files,
                                chunk: lane_chunk,
                                valid_keys: lane_valid_keys,
                            } = &mut *lane;
                            for &id in &selected {
                                memory.check()?;
                                let mut files = lane_files[id].take().unwrap_or_else(|| {
                                    let build = DataInDiskByChunks::new(
                                        build_types.clone(),
                                        "hash-join-v2-build-",
                                        memory.spill_storage(),
                                    );
                                    build.disk_tracker().attach_to(&disk_tracker);
                                    let probe = DataInDiskByChunks::new(
                                        probe_types.clone(),
                                        "hash-join-v2-probe-",
                                        memory.spill_storage(),
                                    );
                                    probe.disk_tracker().attach_to(&disk_tracker);
                                    PartitionFiles { build, probe }
                                });
                                if let Some((_, table)) = owned_tables
                                    .iter_mut()
                                    .find(|(partition, _)| *partition == id)
                                {
                                    if let Some(table) = table.take() {
                                        valid += spill_segments(
                                            &mut files.build,
                                            lane_chunk,
                                            lane_valid_keys,
                                            &table.segments,
                                            &memory,
                                        )?;
                                    }
                                }
                                lane_files[id] = Some(files);
                            }
                            Ok(valid)
                        });
                        let _ = result_tx.send((worker, owned_tables, outcome));
                    })
                    .map_err(|_| ExecError::internal("hash-join spill worker pool stopped"))?;
            }
            drop(result_tx);

            // Every lane reports back, even when an earlier lane failed. This
            // restores any row table left untouched by a failed spill job
            // before propagating the first error.
            let mut error = None;
            let mut valid_rows = 0;
            for (worker, owned_tables, outcome) in result_rx {
                for (partition, table) in owned_tables {
                    tables[worker][partition] = table;
                }
                match outcome {
                    Ok(rows) => valid_rows += rows,
                    Err(failure) => {
                        error.get_or_insert(failure);
                    }
                }
            }
            if let Some(error) = error {
                return Err(error);
            }
            self.valid_rows += valid_rows;
            self.tracker.consume(-released);
            self.memory.check()
        });
        self.action.finish_spill();
        result
    }

    /// Append one worker's probe batch to its already-created partition store.
    pub fn spill_probe_chunk(
        &self,
        worker: usize,
        partition: usize,
        chunk: &Chunk,
    ) -> Result<(), ExecError> {
        self.memory.check()?;
        self.lanes[worker]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .files[partition]
            .as_mut()
            .expect("build spill creates paired probe file")
            .probe
            .add(chunk)
            .map_err(|error| ExecError::SpillFailed(error.to_string()))
    }

    /// Move each partition's lane files onto Go's stack in ascending partition
    /// order, then reset only current-round state for recursive spilling.
    pub fn prepare_for_restoring(
        &mut self,
        last_round: usize,
        max_round: usize,
    ) -> Result<(), ExecError> {
        self.memory.check()?;
        if last_round + 1 > max_round {
            return Err(ExecError::internal("Exceed max spill round"));
        }
        for id in 0..self.spilled.len() {
            if !self.spilled[id] {
                continue;
            }
            let files: Vec<_> = self
                .lanes
                .iter()
                .filter_map(|lane| {
                    lane.lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .files[id]
                        .take()
                })
                .collect();
            if !files.is_empty() {
                self.stack.push(RestorePartition {
                    files,
                    round: last_round + 1,
                });
            }
        }
        self.spilled.fill(false);
        self.valid_rows = 0;
        Ok(())
    }
    /// Transfer ownership of the most recently stacked restore partition.
    pub fn pop_restore(&mut self) -> Option<RestorePartition> {
        self.stack.pop()
    }

    /// Mark the fallback link finished and release/remove every owned spill.
    pub fn close(&mut self) {
        self.action.set_can_spill(false);
        self.action.set_finished();
        self.lanes.clear();
        self.stack.clear();
        self.disk_tracker.detach();
    }
}

impl Drop for HashJoinSpill {
    fn drop(&mut self) {
        self.close();
    }
}

fn spill_segments(
    disk: &mut DataInDiskByChunks,
    chunk: &mut Chunk,
    valid: &mut Vec<u8>,
    segments: &[RowTableSegment],
    memory: &StatementMemory,
) -> Result<u64, ExecError> {
    chunk.reset();
    let mut valid_count = 0;
    for segment in segments {
        valid.resize(segment.get_row_num(), 0);
        valid.fill(0);
        for &index in &segment.valid_join_key_pos {
            valid[index] = 1;
        }
        valid_count += segment.valid_key_count();
        for row in 0..segment.get_row_num() {
            if chunk.is_full() {
                memory.check()?;
                disk.add(chunk)
                    .map_err(|error| ExecError::SpillFailed(error.to_string()))?;
                chunk.reset();
            }
            chunk.append_uint64(0, segment.hash_values[row]);
            chunk.append_bytes(1, &valid[row..row + 1]);
            chunk.append_bytes(2, segment.get_row_bytes(row));
        }
    }
    if chunk.num_rows() > 0 {
        memory.check()?;
        disk.add(chunk)
            .map_err(|error| ExecError::SpillFailed(error.to_string()))?;
        chunk.reset();
    }
    Ok(valid_count)
}
