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

//! Tests for [`tidb_exec::hash_join_v2`], the port of
//! `pkg/executor/join/hash_join_v2.go`.
//!
//! Go-source join matrices and native build/probe transport fixtures. Component
//! tests cover the source's task geometry, row layout and bucket chains; the
//! inner/outer matrices use the expression evaluator and worker queues. Native
//! child-executor fixtures exercise build admission, cancellation and cleanup.
//! This is not full SQL/spill coverage of the upstream join package.

use tidb_chunk::chunk::Chunk;
use tidb_codec::{JoinKeyColumns, SerializeMode};

use tidb_datatype::{FieldType, FieldTypeCode};

use tidb_exec::base_join_probe::{is_key_matched, new_join_probe, BaseJoinProbe, ProbeContext};
use tidb_exec::hash_join_v2::probe_worker::{ProbeWorkerEvent, ProbeWorkerV2};
use tidb_exec::hash_join_v2::{
    new_join_build_worker_v2, AntiLeftOuterSemiJoinProbe, AntiSemiJoinProbe, BuildTask,
    HashJoinCtxV2, HashJoinV2Exec, HashTableContext, InnerJoinProbe, LeftOuterSemiJoinProbe,
    OuterJoinProbe, ProbeV2, SemiJoinProbe, LABEL_FOR_HASH_TABLE_IN_HASH_JOIN_V2,
};
use tidb_exec::hash_table_v2::{get_hash_table_length_by_row_len, get_hash_table_memory_usage};
use tidb_exec::join_row_table::{RowLayoutMeta, RowTableSegment};
use tidb_exec::join_table_meta::{ColumnType, JoinTableMeta, KeyMode};
use tidb_exec::row_table_builder::{get_partition_mask_offset, BuildContext, PartitionInfo};
use tidb_executor::joiner::JoinType;
use tidb_executor::{ExecError, Executor, ExecutorMeta, OomAction, StatementMemory};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// Go mockDataSource. Open/Close belong to the parent; Next owns the child
// exclusively and may run on the join's build coordinator.
#[derive(Default)]
struct ProbeSourceObservations {
    calls: usize,
    allocations: usize,
    requested_rows: Vec<usize>,
}

struct BuildSource {
    meta: ExecutorMeta,
    chunks: std::collections::VecDeque<Chunk>,
    owner: std::thread::ThreadId,
    allocations: std::cell::Cell<usize>,
    observations: Option<std::sync::Arc<std::sync::Mutex<ProbeSourceObservations>>>,
    calls: usize,
    fail_at: Option<usize>,
    panic_at: Option<usize>,
    requested_rows: Vec<usize>,
    lifecycle: Option<SourceLifecycle>,
    first_fetch: Option<(
        crossbeam_channel::Sender<()>,
        crossbeam_channel::Receiver<()>,
    )>,
    first_required_rows: Option<usize>,
}

struct SourceLifecycle {
    side: &'static str,
    events: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    chunks: Vec<Chunk>,
    memory: StatementMemory,
    fail_open: bool,
    fail_close: bool,
}

impl BuildSource {
    fn new(chunks: Vec<Chunk>) -> Self {
        let schema = tidb_expr::schema::Schema::new(vec![tidb_expr::column::Column::new(
            1,
            FieldType::new(FieldTypeCode::LongLong),
        )]);
        Self {
            meta: ExecutorMeta::new(schema, 1, 32, 1024),
            chunks: chunks.into(),
            owner: std::thread::current().id(),
            allocations: std::cell::Cell::new(0),
            observations: None,
            calls: 0,
            fail_at: None,
            panic_at: None,
            requested_rows: Vec::new(),
            lifecycle: None,
            first_fetch: None,
            first_required_rows: None,
        }
    }
}

impl Executor for BuildSource {
    fn open(&mut self) -> Result<(), ExecError> {
        if let Some(lifecycle) = &self.lifecycle {
            assert_eq!(self.owner, std::thread::current().id());
            lifecycle
                .events
                .lock()
                .unwrap()
                .push(format!("{}:open", lifecycle.side));
            if lifecycle.fail_open {
                return Err(ExecError::internal(format!("{} open", lifecycle.side)));
            }
            self.calls = 0;
            self.chunks = lifecycle.chunks.clone().into();
        }
        Ok(())
    }
    fn close(&mut self) -> Result<(), ExecError> {
        if let Some(lifecycle) = &self.lifecycle {
            assert_eq!(self.owner, std::thread::current().id());
            assert_eq!(
                lifecycle.memory.stmt_tracker().bytes_consumed(),
                0,
                "table released before child Close"
            );
            lifecycle
                .events
                .lock()
                .unwrap()
                .push(format!("{}:close", lifecycle.side));
            if lifecycle.fail_close {
                return Err(ExecError::internal(format!("{} close", lifecycle.side)));
            }
        }
        Ok(())
    }
    fn next(&mut self, output: &mut Chunk) -> Result<(), ExecError> {
        self.calls += 1;
        if let Some(observations) = &self.observations {
            let mut observations = observations.lock().unwrap();
            observations.calls += 1;
            observations.requested_rows.push(output.required_rows());
        }
        if self.calls == 1 {
            if let Some(required) = self.first_required_rows {
                assert_eq!(output.required_rows(), required);
            }
            if let Some((entered, peer)) = &self.first_fetch {
                entered.send(()).unwrap();
                // Go fetchProbeSideChunks enters the probe child before
                // wait4BuildSide. This bounds a deadlock, not query speed.
                peer.recv_timeout(std::time::Duration::from_secs(5))
                    .map_err(|_| ExecError::internal("build/probe first fetch did not overlap"))?;
            }
        }
        if let Some(lifecycle) = &self.lifecycle {
            lifecycle
                .events
                .lock()
                .unwrap()
                .push(format!("{}:next", lifecycle.side));
        }
        self.requested_rows.push(output.required_rows());
        assert_ne!(self.panic_at, Some(self.calls), "build source panic");
        if self.fail_at == Some(self.calls) {
            return Err(ExecError::Eval(
                tidb_expr::EvalError::ParamIndexExceedParamCounts,
            ));
        }
        output.reset();
        if let Some(mut input) = self.chunks.pop_front() {
            output.swap_columns(&mut input);
        }
        Ok(())
    }
    fn schema(&self) -> &tidb_expr::schema::Schema {
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
        self.allocations.set(self.allocations.get() + 1);
        if let Some(observations) = &self.observations {
            observations.lock().unwrap().allocations += 1;
        }
        self.meta.new_chunk()
    }
}

// Go fetchAndBuildHashTableImpl / createTasks / buildHashTable: duplicate keys,
// selected rows and skew exercise whole-partition and shared-bucket linking.
#[test]
fn native_build_partition_spill_source() {
    use std::sync::Arc;
    use tidb_exec::hash_join_v2::spill::HashJoinSpill;
    use tidb_util::memory::ActionOnExceed;
    use tidb_util::spill_storage::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};
    for concurrency in [1, 2, 4, 5] {
        for mode in 0..6 {
            let path = std::env::temp_dir().join(format!(
                "tidb-v2-build-spill-{}-{concurrency}-{mode}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let storage = Arc::new(
                SpillStorage::open(SpillStorageSpec {
                    path: path.clone(),
                    quota_bytes: if mode == 3 { 1 } else { -1 },
                    encryption: if mode == 1 {
                        SpillEncryptionMethod::Aes128Ctr
                    } else {
                        SpillEncryptionMethod::Plaintext
                    },
                })
                .unwrap(),
            );
            let memory = StatementMemory::new(128 * 1024, OomAction::Cancel, 1)
                .with_spill_storage(storage.clone())
                .with_tmp_storage_on_oom(mode != 5);
            let ctx = HashJoinCtxV2::new(concurrency, JoinType::RightOuter, true);
            let mut exec = HashJoinV2Exec::new(ctx, &[0], &[false]);
            exec.ctx.need_scan_row_table_after_probe_done = true;
            let meta = JoinTableMeta::new(
                &[0],
                &[ColumnType::Int],
                &[ColumnType::Int],
                &[ColumnType::Int],
                None,
                Some(&[0]),
                true,
            );
            let layout = RowLayoutMeta::from_join_table_meta(&meta, vec![Some(8)]);
            let keys = key_columns(FieldTypeCode::LongLong);
            let context = BuildContext::new(&layout, PartitionInfo::new(concurrency), &keys);
            let chunks: Vec<_> = (0..64)
                .map(|batch| {
                    let mut chunk =
                        Chunk::new_with_capacity(&[FieldType::new(FieldTypeCode::LongLong)], 128);
                    for row in 0..128 {
                        if row % 19 == 0 {
                            chunk.append_null(0);
                        } else {
                            chunk.append_int64(0, if mode == 1 { 42 } else { batch * 128 + row });
                        }
                    }
                    chunk.set_sel(Some((0..128).rev().filter(|row| row % 5 != 0).collect()));
                    chunk
                })
                .collect();
            let mut source = BuildSource::new(chunks.clone());
            source.fail_at = (mode == 2).then_some(if concurrency == 1 { 1 } else { 48 });
            if mode == 4 {
                memory
                    .sql_killer()
                    .send_kill_signal(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
            }
            let mut spill = HashJoinSpill::new(
                concurrency,
                exec.ctx.partition_number,
                source.ret_field_types(),
                exec.hash_table_context.memory_tracker.clone(),
                memory.clone(),
                9,
            );
            spill.register();
            let result = exec.fetch_and_build_hash_table_with_spill(
                &mut source,
                &context,
                &memory,
                &mut spill,
            );
            if mode == 4 {
                assert!(matches!(result, Err(ExecError::Killed(ref error)) if error.code == 1317));
            } else if mode == 2 {
                assert!(matches!(
                    result,
                    Err(ExecError::Eval(
                        tidb_expr::EvalError::ParamIndexExceedParamCounts
                    ))
                ));
            } else if concurrency == 1 || mode == 5 {
                assert!(
                    matches!(
                        result,
                        Err(ExecError::MemoryExceedForQuery { .. }) | Err(ExecError::Killed(_))
                    ),
                    "concurrency={concurrency} mode={mode}: {result:?}"
                );
            } else if mode == 3 {
                assert!(
                    matches!(result, Err(ExecError::SpillFailed(_))),
                    "{result:?}"
                );
            } else {
                result.unwrap();
                assert!(spill.spilled_partitions().iter().any(|&spilled| spilled));
                assert!(spill.build_spill_bytes() > 0);
                assert_eq!(spill.probe_spill_bytes(), 0);
                assert_eq!(source.allocations.get(), concurrency + 2);
                let mut actual = Vec::new();
                let collect = |exec: &HashJoinV2Exec| {
                    let mut rows = Vec::new();
                    for table in exec.hash_table_context.hash_table.tables.iter().flatten() {
                        for segment in &table.row_data.segments {
                            for row in 0..segment.get_row_num() {
                                let mut bytes = segment.get_row_bytes(row).to_vec();
                                bytes[..8].fill(0); // bucket linkage is rebuilt after restore
                                rows.push((
                                    segment.hash_values[row],
                                    segment.valid_join_key_pos.contains(&row),
                                    bytes,
                                ));
                            }
                        }
                    }
                    rows
                };
                actual.extend(collect(&exec));
                let expected_spilled_valid = spill.valid_spilled_rows();
                let spilled_ids: Vec<_> = spill
                    .spilled_partitions()
                    .iter()
                    .enumerate()
                    .filter_map(|(id, &spilled)| spilled.then_some(id))
                    .collect();
                std::thread::scope(|scope| {
                    let mut workers = Vec::new();
                    for worker in 0..concurrency {
                        let spill = &spill;
                        let spilled_ids = &spilled_ids;
                        let shift = exec.ctx.partition_mask_offset;
                        workers.push(scope.spawn(move || {
                            let types = vec![
                                FieldType::new(FieldTypeCode::LongLong),
                                FieldType::new(FieldTypeCode::Bit),
                                FieldType::new(FieldTypeCode::LongLong),
                            ];
                            let mut chunk = Chunk::new_with_capacity(&types, 1);
                            for &id in spilled_ids {
                                chunk.reset();
                                chunk.append_uint64(0, (id as u64) << shift);
                                chunk.append_bytes(1, &(worker as u64).to_ne_bytes());
                                chunk.append_int64(2, worker as i64);
                                spill.spill_probe_chunk(worker, id, &chunk).unwrap();
                            }
                        }));
                    }
                    for worker in workers {
                        worker.join().unwrap();
                    }
                });
                assert!(spill.probe_spill_bytes() > 0);
                spill.prepare_for_restoring(0, 3).unwrap();
                assert!(spill.spilled_partitions().iter().all(|&spilled| !spilled));
                let mut valid_rows = 0;
                let mut probe_rows = 0;
                let mut last_partition = exec.ctx.partition_number;
                while let Some(mut partition) = spill.pop_restore() {
                    assert_eq!(partition.round, 1);
                    let mut observed_partition = None;
                    for (worker, files) in partition.files.iter_mut().enumerate() {
                        assert_eq!(files.probe.num_chunks(), 1);
                        let probe = files.probe.get_chunk(0).unwrap();
                        assert_eq!(probe.num_rows(), 1);
                        let probe = probe.get_row(0);
                        assert_eq!(probe.get_int64(2), worker as i64);
                        assert_eq!(&*probe.get_bytes(1), &(worker as u64).to_ne_bytes());
                        probe_rows += 1;
                        for index in 0..files.build.num_chunks() {
                            let chunk = files.build.get_chunk(index).unwrap();
                            assert!(
                                chunk.num_rows()
                                    <= tidb_exec::hash_join_v2::spill::SPILL_CHUNK_SIZE
                            );
                            for row in 0..chunk.num_rows() {
                                let row = chunk.get_row(row);
                                let hash = row.get_uint64(0);
                                let valid = row.get_bytes(1)[0] == 1;
                                valid_rows += u64::from(valid);
                                if valid {
                                    observed_partition =
                                        Some((hash >> exec.ctx.partition_mask_offset) as usize);
                                }
                                let mut bytes = row.get_bytes(2).to_vec();
                                bytes[..8].fill(0);
                                actual.push((hash, valid, bytes));
                            }
                        }
                    }
                    if let Some(id) = observed_partition {
                        assert!(id < last_partition);
                        last_partition = id;
                    }
                }
                assert_eq!(valid_rows, expected_spilled_valid);
                assert_eq!(probe_rows, spilled_ids.len() * concurrency);
                let baseline_memory = StatementMemory::new(-1, OomAction::Cancel, 1);
                let mut baseline = HashJoinV2Exec::new(exec.ctx.clone(), &[0], &[false]);
                baseline
                    .fetch_and_build_hash_table(
                        &mut BuildSource::new(chunks),
                        &context,
                        &baseline_memory,
                    )
                    .unwrap();
                let mut expected = collect(&baseline);
                actual.sort();
                expected.sort();
                assert_eq!(actual, expected);
                baseline.release_build_memory();
                // Go refuses unbounded respill rather than looping forever.
                assert!(
                    matches!(spill.prepare_for_restoring(3, 3), Err(ExecError::Internal(ref error)) if error == "Exceed max spill round")
                );
            }
            spill.close();
            assert!(spill.action.is_finished());
            exec.release_build_memory();
            assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
            assert_eq!(storage.global_tracker().bytes_consumed(), 0);
            assert_eq!(std::fs::read_dir(&path).unwrap().count(), 0);
            std::fs::remove_dir(&path).unwrap();
        }
    }
}

/// Go inner/outer/semi spill suites: run native Open/Next/Close through quota
/// spilling and restore, comparing against the same source without spilling.
#[test]
fn native_hash_join_recursive_restore_source() {
    use std::sync::Arc;
    use tidb_exec::hash_join_v2::executor::{HashJoinV2Executor, HashJoinV2Plan};
    use tidb_expr::{
        column::Column,
        expression::{Constant, Expression, ScalarFunction},
        schema::Schema,
        NoColumns,
    };
    use tidb_util::spill_storage::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};
    for concurrency in [2, 4, 5] {
        for kind in [
            JoinType::Inner,
            JoinType::LeftOuter,
            JoinType::RightOuter,
            JoinType::SemiJoin,
            JoinType::AntiSemiJoin,
            JoinType::LeftOuterSemiJoin,
            JoinType::AntiLeftOuterSemiJoin,
        ] {
            for right_build in [false, true] {
                for residual in [false, true] {
                    let outer_semi = matches!(
                        kind,
                        JoinType::LeftOuterSemiJoin | JoinType::AntiLeftOuterSemiJoin
                    );
                    if outer_semi && !right_build {
                        continue;
                    }
                    let semi = !matches!(
                        kind,
                        JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter
                    );
                    let int = FieldType::new(FieldTypeCode::LongLong);
                    let fields = vec![int.clone(), int.clone()];
                    let source = |side: usize| {
                        let chunks = (0..64)
                            .map(|batch| {
                                let mut chunk = Chunk::new_with_capacity(&fields, 128);
                                for row in 0..128 {
                                    let value = batch * 128 + row;
                                    if value % 19 == 0 {
                                        chunk.append_null(0);
                                    } else {
                                        chunk.append_int64(0, (value / 2 + side * 1024) as i64);
                                    }
                                    chunk.append_int64(1, value as i64);
                                }
                                chunk.set_sel(Some(
                                    (0..128).rev().filter(|row| row % 5 != 0).collect(),
                                ));
                                chunk
                            })
                            .collect();
                        let mut source = BuildSource::new(chunks);
                        source.meta = ExecutorMeta::new(
                            Schema::new(
                                fields
                                    .iter()
                                    .enumerate()
                                    .map(|(index, field)| Column::new(index as i64, field.clone()))
                                    .collect(),
                            ),
                            1,
                            128,
                            128,
                        );
                        Box::new(source) as Box<dyn Executor>
                    };
                    let width = if outer_semi {
                        3
                    } else if semi {
                        2
                    } else {
                        4
                    };
                    let make = |memory: StatementMemory| {
                        HashJoinV2Executor::new(
                            ExecutorMeta::new(
                                Schema::new(
                                    (0..width)
                                        .map(|index| Column::new(index, int.clone()))
                                        .collect(),
                                ),
                                2,
                                32,
                                128,
                            ),
                            HashJoinV2Plan {
                                concurrency,
                                join_type: kind,
                                right_as_build_side: right_build,
                                build_key_indices: vec![0],
                                probe_key_indices: vec![0],
                                build_key_types: vec![int.clone()],
                                probe_key_types: vec![int.clone()],
                                l_used: vec![0, 1],
                                r_used: if semi { vec![] } else { vec![0, 1] },
                                l_used_in_other_condition: if residual { vec![1] } else { vec![] },
                                r_used_in_other_condition: vec![],
                                build_filter: vec![],
                                probe_filter: if residual {
                                    let mut column = Column::new(1, int.clone());
                                    column.index = 1;
                                    vec![Expression::ScalarFunction(ScalarFunction::new(
                                        tidb_ast::CiString::new("lt"),
                                        int.clone(),
                                        vec![
                                            Expression::Column(column),
                                            Expression::Constant(Constant::new(
                                                tidb_datatype::Datum::Int(4000),
                                                int.clone(),
                                            )),
                                        ],
                                    ))]
                                } else {
                                    vec![]
                                },
                                other_condition: if residual {
                                    let mut column = Column::new(1, int.clone());
                                    column.index = 1;
                                    vec![Expression::ScalarFunction(ScalarFunction::new(
                                        tidb_ast::CiString::new("lt"),
                                        int.clone(),
                                        vec![
                                            Expression::Column(column),
                                            Expression::Constant(Constant::new(
                                                tidb_datatype::Datum::Int(4000),
                                                int.clone(),
                                            )),
                                        ],
                                    ))]
                                } else {
                                    vec![]
                                },
                                vectorized: true,
                            },
                            source(0),
                            source(1),
                            NoColumns,
                            memory,
                        )
                    };
                    let collect = |exec: &mut HashJoinV2Executor<NoColumns>| -> Result<Vec<Vec<Option<i64>>>, ExecError> {
                    exec.open()?;
                    let mut output = exec.new_chunk();
                    let mut rows = Vec::new();
                    loop {
                        exec.next(&mut output)?;
                        if output.num_rows() == 0 { break; }
                        rows.extend((0..output.num_rows()).map(|index| {
                            let row = output.get_row(index);
                            (0..width as usize).map(|col| (!row.is_null(col)).then(|| row.get_int64(col))).collect::<Vec<_>>()
                        }));
                    }
                    exec.close()?;
                    rows.sort();
                    Ok(rows)
                };
                    let baseline_memory = StatementMemory::new(-1, OomAction::Cancel, 1);
                    let expected = collect(&mut make(baseline_memory.clone())).unwrap();
                    assert_eq!(baseline_memory.stmt_tracker().bytes_consumed(), 0);
                    let path = std::env::temp_dir().join(format!(
                        "tidb-v2-restore-{}-{concurrency}-{kind:?}-{right_build}-{}",
                        std::process::id(),
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos()
                    ));
                    let storage = Arc::new(
                        SpillStorage::open(SpillStorageSpec {
                            path: path.clone(),
                            quota_bytes: -1,
                            encryption: if concurrency == 5 {
                                SpillEncryptionMethod::Aes128Ctr
                            } else {
                                SpillEncryptionMethod::Plaintext
                            },
                        })
                        .unwrap(),
                    );
                    let memory = StatementMemory::new(192 * 1024, OomAction::Cancel, 1)
                        .with_spill_storage(storage.clone())
                        .with_tmp_storage_on_oom(true);
                    let mut exec = make(memory.clone());
                    let result = collect(&mut exec);
                    // Cleanup is also required on an assertion failure.
                    exec.close().unwrap();
                    assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
                    assert_eq!(storage.global_tracker().bytes_consumed(), 0);
                    assert!(
                        storage.global_tracker().max_consumed() > 0,
                        "fixture must spill"
                    );
                    assert_eq!(std::fs::read_dir(&path).unwrap().count(), 0);
                    std::fs::remove_dir(&path).unwrap();
                    let actual = result.unwrap_or_else(|error| {
                        panic!(
                        "{kind:?} concurrency={concurrency} right_build={right_build}: {error:?}"
                    )
                    });
                    assert!(actual == expected,
                    "{kind:?} concurrency={concurrency} right_build={right_build} residual={residual}: rows {}/{}, first missing {:?}, extra {:?}",
                    actual.len(), expected.len(), expected.iter().find(|row| actual.binary_search(row).is_err()),
                    actual.iter().find(|row| expected.binary_search(row).is_err()));
                }
            }
        }
    }
}

/// Go testInnerJoinSpillCase3: row tables fit, but projected bucket arrays
/// cross the quota. Selection must precede bucket allocation and preserve
/// empty bucket tables for the spilled partitions.
#[test]
fn native_hash_bucket_allocation_spill_source() {
    use tidb_exec::hash_join_v2::spill::HashJoinSpill;
    use tidb_util::memory::ActionOnExceed;
    let memory = StatementMemory::new(-1, OomAction::Cancel, 1);
    let mut exec = HashJoinV2Exec::new(HashJoinCtxV2::new(4, JoinType::Inner, true), &[0], &[true]);
    let tracker = exec.hash_table_context.memory_tracker.clone();
    tracker.attach_to(memory.stmt_tracker());
    let mut spill = HashJoinSpill::new(
        4,
        4,
        &[FieldType::new(FieldTypeCode::LongLong)],
        tracker.clone(),
        memory.clone(),
        9,
    );
    spill.register();
    spill.action.set_can_spill(true);
    let layout = one_int_key_layout();
    let keys = key_columns(FieldTypeCode::LongLong);
    let mut context = BuildContext::new(&layout, PartitionInfo::new(4), &keys);
    context.memory_tracker = Some(&tracker);
    let input = probe_chunk(&(0..4096).collect::<Vec<_>>());
    exec.begin_build(layout.null_map_length);
    exec.append_build_chunk(0, &input, &mut context).unwrap();
    let limit = tracker.bytes_consumed() + 1;
    memory.session_tracker().set_bytes_limit(limit);
    assert!(!spill.action.is_spill_needed());
    exec.hash_table_context
        .merge_row_tables_with_spill(4, &mut spill)
        .unwrap();
    let mask = spill.spilled_partitions().to_vec();
    let count = mask.iter().filter(|&&spilled| spilled).count();
    assert!(
        count > 0 && count < 4,
        "largest partitions suffice; do not spill everything"
    );
    assert!(tracker.bytes_consumed() <= limit / 2 + 4 * 256);
    let resident: u64 = exec
        .hash_table_context
        .hash_table
        .tables
        .iter()
        .flatten()
        .map(|table| table.row_data.row_count())
        .sum();
    assert_eq!(resident + spill.valid_spilled_rows(), 4096);
    for (id, &spilled) in mask.iter().enumerate() {
        if spilled {
            assert_eq!(
                exec.hash_table_context.hash_table.tables[id]
                    .as_ref()
                    .unwrap()
                    .row_data
                    .row_count(),
                0
            );
        }
    }
    // After merge, another overrun must fall back to cancellation, not ask
    // for a spill of table state that the probe workers already share.
    tracker.consume(limit);
    assert!(memory.check().is_err());
    assert!(!spill.action.is_spill_needed());
    tracker.consume(-limit);
    exec.release_build_memory();
    spill.close();
    assert!(spill.action.is_finished());
    assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
}

#[test]
fn native_build_stage_consumes_child_chunks_and_links_all_rows_source() {
    let layout = one_int_key_layout();
    let keys = key_columns(FieldTypeCode::LongLong);
    for concurrency in [1, 4, 5] {
        for skew in [false, true] {
            let mut chunks = Vec::new();
            let mut expected = std::collections::BTreeMap::<i64, usize>::new();
            for batch in 0..24 {
                let values: Vec<i64> = (0..64)
                    .map(|row| if skew { 42 } else { (batch * 64 + row) % 11 })
                    .collect();
                let mut chunk = probe_chunk(&values);
                let selected: Vec<_> = (0..64).step_by(2).rev().collect();
                for &index in &selected {
                    *expected.entry(values[index]).or_default() += 1;
                }
                chunk.set_sel(Some(selected));
                chunks.push(chunk);
            }
            let mut source = BuildSource::new(chunks);
            let ctx = HashJoinCtxV2::new(concurrency, JoinType::Inner, true);
            let mut exec = HashJoinV2Exec::new(ctx, &[0], &[true]);
            let context = BuildContext::new(&layout, PartitionInfo::new(concurrency), &keys);
            let memory = StatementMemory::new(-1, OomAction::Cancel, 1);
            let segments = exec
                .fetch_and_build_hash_table(&mut source, &context, &memory)
                .unwrap();
            assert!(segments > 0);
            assert_eq!(source.calls, 25);
            assert_eq!(source.allocations.get(), concurrency + 2);
            assert!(exec
                .hash_table_context
                .row_tables
                .iter()
                .flatten()
                .all(Option::is_none));
            let probe_keys: Vec<_> = expected.keys().copied().collect();
            let matches = matches_for(&exec, &layout, &probe_keys);
            for (&key, rows) in probe_keys.iter().zip(matches) {
                assert_eq!(rows, vec![key; expected[&key]]);
            }
            assert!(memory.bytes_consumed() > 0);
            assert_eq!(
                memory.bytes_consumed(),
                exec.hash_table_context.memory_tracker.bytes_consumed()
            );
            exec.release_build_memory();
            assert_eq!(memory.bytes_consumed(), 0);
        }
    }
}

// Go TestHashJoinRandomFail and killedDuringBuild / memory-quota paths, using
// deterministic failures so the original error and post-join cleanup are exact.
#[test]
fn native_build_stage_errors_join_workers_and_release_memory_source() {
    use tidb_util::sqlkiller::KillSignal;
    let layout = one_int_key_layout();
    let keys = key_columns(FieldTypeCode::LongLong);
    struct FilterContext<'a> {
        mode: usize,
        memory: &'a StatementMemory,
    }
    impl tidb_expr::Columns for FilterContext<'_> {
        fn get(&self, _: &[String]) -> Option<tidb_datatype::Datum> {
            None
        }
        fn param_value(&self, _: usize) -> Result<tidb_datatype::Datum, tidb_expr::EvalError> {
            assert_ne!(self.mode, 2, "build worker panic");
            if self.mode == 3 {
                self.memory
                    .sql_killer()
                    .send_kill_signal(KillSignal::QueryInterrupted);
            }
            if self.mode == 6 {
                return Err(tidb_expr::EvalError::ParamIndexExceedParamCounts);
            }
            Ok(tidb_datatype::Datum::Int(1))
        }
    }
    for mode in 0..7 {
        let chunks: Vec<_> = (0..32).map(|_| probe_chunk(&[1, 2, 3, 4])).collect();
        let mut source = BuildSource::new(chunks);
        source.fail_at = (mode == 0).then_some(2);
        source.panic_at = (mode == 1).then_some(2);
        let memory = StatementMemory::new(if mode == 4 { 64 } else { -1 }, OomAction::Cancel, 7);
        if mode == 5 {
            memory
                .sql_killer()
                .send_kill_signal(KillSignal::QueryInterrupted);
        }
        let mut parameter = tidb_expr::constant::Constant::new(
            tidb_datatype::Datum::Null,
            FieldType::new(FieldTypeCode::LongLong),
        );
        parameter.param_marker = Some(tidb_expr::constant::ParamMarker { order: 0 });
        let filter = tidb_exec::base_join_probe::JoinFilter::new(
            FilterContext {
                mode,
                memory: &memory,
            },
            vec![tidb_expr::expression::Expression::Constant(parameter)],
            true,
        );
        let mut context = BuildContext::new(&layout, PartitionInfo::new(4), &keys);
        if matches!(mode, 2 | 3 | 6) {
            context.build_filter = Some(&filter);
        }
        let mut ctx = HashJoinCtxV2::new(4, JoinType::Inner, true);
        ctx.has_build_filter = context.build_filter.is_some();
        let mut exec = HashJoinV2Exec::new(ctx, &[0], &[true]);
        let error = exec
            .fetch_and_build_hash_table(&mut source, &context, &memory)
            .unwrap_err();
        match mode {
            0 | 6 => assert!(matches!(
                error,
                ExecError::Eval(tidb_expr::EvalError::ParamIndexExceedParamCounts)
            )),
            1 | 2 => assert!(
                matches!(error, ExecError::Internal(ref message) if message.contains("build source panic") || message.contains("build worker panic"))
            ),
            3 | 5 => assert!(matches!(error, ExecError::Killed(ref error) if error.code == 1317)),
            4 => assert!(
                matches!(error, ExecError::MemoryExceedForQuery { conn_id: 7 })
                    || matches!(error, ExecError::Killed(ref error) if error.code == 8175)
            ),
            _ => unreachable!(),
        }
        assert_eq!(memory.bytes_consumed(), 0);
        assert_eq!(exec.hash_table_context.memory_tracker.bytes_consumed(), 0);
        assert!(exec
            .build_workers
            .iter()
            .all(|worker| worker.builder.is_none()));
        assert!(exec
            .hash_table_context
            .row_tables
            .iter()
            .flatten()
            .all(Option::is_none));
        assert!(exec
            .hash_table_context
            .hash_table
            .tables
            .iter()
            .all(Option::is_none));
    }
}

// Go runJoinWorker / scanRowTableAfterProbeDone, through the native bounded
// transport. Only assertions clone completed results; production recycles them.
fn run_join_worker(
    probe: &mut dyn ProbeV2,
    chunks: Vec<Chunk>,
    new_result_chunk: &dyn Fn() -> Chunk,
) -> Result<Vec<Chunk>, ExecError> {
    run_worker_stage(probe, chunks, new_result_chunk(), false)
}

/// Go `(*ProbeWorkerV2).scanRowTableAfterProbeDone`.
///
/// Go runs this on `Concurrency` workers after every probe worker has
/// finished, each scanning the slice of the row table
/// `commonInitForScanRowTable` gave it. That barrier is what makes the
/// used flags final, and it is preserved here by the call order.
///
/// # Errors
///
/// Propagates what Go stores in `joinResult.err`.
fn scan_row_table_after_probe_done(
    probe: &mut dyn ProbeV2,
    new_result_chunk: &dyn Fn() -> Chunk,
) -> Result<Vec<Chunk>, ExecError> {
    run_worker_stage(probe, Vec::new(), new_result_chunk(), true)
}

fn run_worker_stage(
    probe: &mut dyn ProbeV2,
    chunks: Vec<Chunk>,
    output: Chunk,
    scan: bool,
) -> Result<Vec<Chunk>, ExecError> {
    let (events_tx, events) = crossbeam_channel::bounded(2);
    let (close, close_rx) = crossbeam_channel::bounded(0);
    let (scan_ready, scan_rx) = crossbeam_channel::bounded(0);
    let (worker, ports) = ProbeWorkerV2::new(0, output, events_tx, close_rx, scan_rx);
    let memory = StatementMemory::new(-1, OomAction::Cancel, 1);
    std::thread::scope(|scope| {
        let consumer = scope.spawn(move || {
            let mut results = Vec::new();
            let mut inputs = chunks.into_iter();
            let mut input_tx = Some(ports.input);
            let mut scan_ready = Some(scan_ready);
            let mut pending_capacity = Vec::new();
            let submit = |chunk: Chunk,
                          pending: &mut Vec<usize>,
                          sender: &crossbeam_channel::Sender<Chunk>| {
                *pending = (0..chunk.num_cols())
                    .map(|i| chunk.column(i).data_capacity())
                    .collect();
                sender.send(chunk).unwrap();
            };
            if let Some(chunk) = inputs.next() {
                submit(chunk, &mut pending_capacity, input_tx.as_ref().unwrap());
            } else {
                input_tx.take();
            }
            loop {
                // A timeout unwinds this consumer, dropping Close and releasing
                // the worker even if a transport regression blocks a queue.
                match events
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .expect("worker progress")
                {
                    ProbeWorkerEvent::Input { chunk, .. } => {
                        assert_eq!(chunk.num_rows(), 0);
                        assert_eq!(
                            (0..chunk.num_cols())
                                .map(|i| chunk.column(i).data_capacity())
                                .collect::<Vec<_>>(),
                            pending_capacity
                        );
                        if let Some(chunk) = inputs.next() {
                            submit(chunk, &mut pending_capacity, input_tx.as_ref().unwrap());
                        } else {
                            input_tx.take();
                        }
                    }
                    ProbeWorkerEvent::Output { mut chunk, .. } => {
                        assert!(chunk.num_rows() <= chunk.required_rows());
                        results.push(chunk.clone());
                        chunk.reset();
                        // The last partial result may outlive the worker.
                        let _ = ports.recycle.send(chunk);
                    }
                    ProbeWorkerEvent::ProbeDone { scan_required, .. } => {
                        if scan && scan_required {
                            drop(scan_ready.take());
                        } else {
                            drop(close);
                            return Ok(results);
                        }
                    }
                    ProbeWorkerEvent::Done { .. } => {
                        drop(close);
                        return Ok(results);
                    }
                    ProbeWorkerEvent::FetcherDone | ProbeWorkerEvent::FetcherError { .. } => {
                        panic!("a directly driven worker has no fetcher")
                    }
                    ProbeWorkerEvent::Error { error, .. } => {
                        drop(close);
                        return Err(error);
                    }
                }
            }
        });
        worker.run(probe, &memory);
        consumer.join().unwrap()
    })
}

/// A row layout with one inlined fixed-width integer key and no null map,
/// matching the fixture `base_join_probe_source.rs` already uses.
fn one_int_key_layout() -> RowLayoutMeta {
    RowLayoutMeta {
        null_map_length: 0,
        col_offset_in_null_map: 0,
        row_columns_order: vec![0],
        columns_size: vec![Some(8)],
        is_join_keys_inlined: true,
        is_join_keys_fixed_length: true,
        join_keys_length: 8,
        fake_key_byte: vec![0_u8; 8],
        key_mode: KeyMode::OneInt64,
    }
}

fn preserved_int_key_layout() -> RowLayoutMeta {
    let meta = JoinTableMeta::new(
        &[0],
        &[ColumnType::Int],
        &[ColumnType::Int],
        &[ColumnType::Int],
        None,
        Some(&[0]),
        true,
    );
    RowLayoutMeta::from_join_table_meta(&meta, vec![Some(8)])
}

fn build_chunk(keys: &[i64]) -> Chunk {
    probe_chunk(keys)
}

fn probe_chunk(keys: &[i64]) -> Chunk {
    let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
    let mut chunk = Chunk::new(&fields, keys.len().max(1), 1024);
    for &key in keys {
        chunk.append_int64(0, key);
    }
    chunk
}

fn empty_blob_probe_chunk() -> Chunk {
    let fields = vec![FieldType::new(FieldTypeCode::Blob)];
    let mut chunk = Chunk::new(&fields, 1, 1024);
    chunk.append_bytes(0, b"");
    chunk
}

fn type_null_key_layout() -> RowLayoutMeta {
    let meta = JoinTableMeta::new(
        &[0],
        &[ColumnType::Null],
        &[ColumnType::Null],
        &[ColumnType::BinaryString],
        None,
        Some(&[0]),
        true,
    );
    RowLayoutMeta::from_join_table_meta(&meta, vec![None])
}

fn type_null_build_chunk() -> Chunk {
    let mut chunk = Chunk::new_with_capacity(&[FieldType::new(FieldTypeCode::Null)], 1);
    chunk.append_null(0);
    chunk
}

/// Drives the incremental build stages over the test's predetermined worker shares.
fn built_exec(
    concurrency: usize,
    join_type: JoinType,
    layout: &RowLayoutMeta,
    chunks_per_worker: &[Vec<Chunk>],
) -> (HashJoinV2Exec, usize) {
    let ctx = HashJoinCtxV2::new(concurrency, join_type, true);
    let mut exec = HashJoinV2Exec::new(ctx, &[0], &[true]);
    let partition = PartitionInfo::new(exec.ctx.partition_number);
    let serializer = key_columns(FieldTypeCode::LongLong);
    let mut build_context = BuildContext::new(layout, partition, &serializer);
    exec.begin_build(layout.null_map_length);
    for (worker_id, chunks) in chunks_per_worker.iter().enumerate() {
        for chunk in chunks {
            exec.append_build_chunk(worker_id, chunk, &mut build_context)
                .expect("build side");
            assert_eq!(
                exec.hash_table_context.memory_tracker.bytes_consumed(),
                build_context.consumed_memory
            );
        }
    }
    let total = exec.finish_build();
    (exec, total)
}

/// Every build key that the finished hash table matches for `key`, found the
/// way a per-join-type probe finds them: bucket head from
/// `SetChunkForProbe`, then [`BaseJoinProbe::next_matched_row`] down the
/// chain with [`is_key_matched`] at each link.
fn matches_for(exec: &HashJoinV2Exec, layout: &RowLayoutMeta, keys: &[i64]) -> Vec<Vec<i64>> {
    let hash_table = &exec.hash_table_context.hash_table;
    let ctx = ProbeContext {
        spill: None,
        hash_table,
        meta: layout,
        column_count_needed_for_other_condition: 0,
        total_column_number: 1,
        tag_helper: exec.hash_table_context.tag_helper,
        partition_number: exec.ctx.partition_number,
        partition_mask_offset: exec.ctx.partition_mask_offset,
        has_other_condition: false,
        right_as_build_side: true,
        l_used: vec![0],
        r_used: vec![0],
        l_used_in_other_condition: Vec::new(),
        r_used_in_other_condition: Vec::new(),
        concurrency: exec.ctx.concurrency,
        max_chunk_size: 1024,
    };
    let mut probe = new_join_probe(&ctx, 0, JoinType::Inner, vec![0], &[false], true);
    let chunk = probe_chunk(keys);
    probe
        .set_chunk_for_probe(&ctx, chunk, None, &(key_columns(FieldTypeCode::LongLong)))
        .expect("probe chunk prepared");

    (0..keys.len())
        .map(|logical_row| {
            let mut matched = Vec::new();
            let hash_value = probe.matched_rows_hash_value()[logical_row];
            let serialized = probe.serialized_keys()[logical_row].to_vec();
            let mut current = probe.matched_rows_headers()[logical_row];
            while current != 0 {
                let row = tidb_exec::base_join_probe::BuildRowSource::row_bytes(
                    hash_table,
                    tidb_exec::hash_table_v2::row_address_of(&ctx.tag_helper, current),
                );
                if is_key_matched(layout.key_mode, &serialized, row, layout) {
                    let mut bytes = [0_u8; 8];
                    bytes.copy_from_slice(&layout.get_key_bytes(row)[..8]);
                    matched.push(i64::from_le_bytes(bytes));
                }
                current = BaseJoinProbe::next_matched_row(row, &ctx.tag_helper, hash_value);
            }
            matched.sort_unstable();
            matched
        })
        .collect()
}

// ---------------------------------------------------------------------------
// SetupPartitionInfo (`hash_join_v2.go:298`, `:306`, `:313`)
// ---------------------------------------------------------------------------

#[test]
fn partition_info_is_a_capped_power_of_two_with_a_matching_mask_offset() {
    // genHashJoinPartitionNumber doubles until it reaches the hint, capped at
    // 16, so the partition number is always a power of two.
    for (concurrency, expected) in [
        (1_usize, 1_usize),
        (2, 2),
        (3, 4),
        (5, 8),
        (16, 16),
        (64, 16),
    ] {
        let ctx = HashJoinCtxV2::new(concurrency, JoinType::Inner, true);
        assert_eq!(ctx.partition_number, expected, "concurrency {concurrency}");
        assert!(ctx.partition_number.is_power_of_two());
        // getPartitionMaskOffset: the top log2(partitionNumber) bits of the
        // hash value select the partition.
        assert_eq!(
            ctx.partition_mask_offset,
            64 - ctx.partition_number.trailing_zeros() as usize
        );
        assert_eq!(
            ctx.partition_mask_offset,
            get_partition_mask_offset(ctx.partition_number)
        );
    }
}

// ---------------------------------------------------------------------------
// initMaxSpillRound (`hash_join_v2.go:636`)
// ---------------------------------------------------------------------------

#[test]
fn max_spill_round_is_the_rounds_needed_to_pass_1024_partitions() {
    // log(1024)/log(partitionNumber), truncated: with 2 partitions it takes
    // 10 rounds of re-partitioning to exceed 1024, with 16 it takes 2.
    for (partition_number, expected) in [(2_usize, 10_usize), (4, 5), (8, 3), (16, 2)] {
        let mut ctx = HashJoinCtxV2::new(1, JoinType::Inner, true);
        ctx.partition_number = partition_number;
        ctx.init_max_spill_round();
        assert_eq!(
            ctx.max_spill_round, expected,
            "{partition_number} partitions"
        );
    }

    // Above 1024 partitions one round already suffices.
    let mut ctx = HashJoinCtxV2::new(1, JoinType::Inner, true);
    ctx.partition_number = 2048;
    ctx.init_max_spill_round();
    assert_eq!(ctx.max_spill_round, 1);
}

// ---------------------------------------------------------------------------
// canSkipProbeIfHashTableIsEmpty / shouldLimitProbeFetchSize
// (`hash_join_v2.go:753`, `:763`)
// ---------------------------------------------------------------------------

#[test]
fn probe_can_be_skipped_only_when_an_empty_build_side_produces_no_rows() {
    let skip = |join_type, right_as_build_side| {
        HashJoinCtxV2::new(1, join_type, right_as_build_side)
            .can_skip_probe_if_hash_table_is_empty()
    };
    // Inner: no build row, no output row, either way round.
    assert!(skip(JoinType::Inner, true));
    assert!(skip(JoinType::Inner, false));
    // Outer joins: skippable only when the *outer* side is the build side,
    // because then there is nothing to null-extend.
    assert!(!skip(JoinType::LeftOuter, true));
    assert!(skip(JoinType::LeftOuter, false));
    assert!(skip(JoinType::RightOuter, true));
    assert!(!skip(JoinType::RightOuter, false));
    // Semi: skippable when the existence side is the build side.
    assert!(skip(JoinType::SemiJoin, true));
    assert!(!skip(JoinType::SemiJoin, false));
    // Anti semi still emits every probe row, so it can never be skipped.
    assert!(!skip(JoinType::AntiSemiJoin, true));
    assert!(!skip(JoinType::AntiSemiJoin, false));
}

#[test]
fn probe_fetch_size_is_limited_only_for_the_outer_side_of_an_outer_join() {
    let limit = |join_type, right_as_build_side| {
        HashJoinCtxV2::new(1, join_type, right_as_build_side).should_limit_probe_fetch_size()
    };
    assert!(limit(JoinType::LeftOuter, true));
    assert!(!limit(JoinType::LeftOuter, false));
    assert!(limit(JoinType::RightOuter, false));
    assert!(!limit(JoinType::RightOuter, true));
    assert!(!limit(JoinType::Inner, true));
    assert!(!limit(JoinType::SemiJoin, true));
}

// ---------------------------------------------------------------------------
// hashTableContext (`hash_join_v2.go:70`)
// ---------------------------------------------------------------------------

#[test]
fn appending_a_row_segment_skips_empty_segments_and_creates_the_row_table_lazily() {
    let mut context = HashTableContext::new(2, 2);
    assert!(context.get_segments_in_row_table(0, 0).is_empty());

    // appendRowSegment returns early on an empty segment, so no row table is
    // created for it.
    context.append_row_segment(0, 0, RowTableSegment::new());
    assert!(context.row_tables[0][0].is_none());

    let mut segment = RowTableSegment::new();
    segment.hash_values.push(7);
    segment.row_start_offset.push(0);
    segment.raw_data.extend_from_slice(&[0_u8; 8]);
    segment.finalize();
    context.append_row_segment(1, 1, segment);
    assert_eq!(context.get_segments_in_row_table(1, 1).len(), 1);
    assert!(context.get_all_segments_memory_usage_in_row_table() > 0);

    context.clear_segments_in_row_table(1, 1);
    assert!(context.get_segments_in_row_table(1, 1).is_empty());
}

#[test]
fn merging_row_tables_concatenates_every_worker_share_and_consumes_memory() {
    let layout = one_int_key_layout();
    // Two workers, each with two chunks; every chunk contributes one segment
    // per non-empty partition.
    let chunks_per_worker = vec![
        vec![build_chunk(&[1, 2, 3]), build_chunk(&[4, 5])],
        vec![build_chunk(&[6, 7]), build_chunk(&[8])],
    ];
    let (exec, total_segment_cnt) = built_exec(2, JoinType::Inner, &layout, &chunks_per_worker);

    // Every per-worker row table is drained by the merge, which is Go's
    // clearAllSegmentsInRowTable.
    assert_eq!(
        exec.hash_table_context
            .get_all_segments_memory_usage_in_row_table(),
        0
    );
    // Segments land in the sub tables instead.
    let in_sub_tables: usize = (0..exec.ctx.partition_number)
        .map(|part| {
            exec.hash_table_context
                .hash_table
                .sub_table(part)
                .row_data
                .segments
                .len()
        })
        .sum();
    assert_eq!(in_sub_tables, total_segment_cnt);
    assert_eq!(exec.hash_table_context.hash_table.total_row_count(), 8);

    // tryToSpill's unconditional pre-consume: the bucket arrays of every
    // partition, charged to the hash-table tracker.
    let expected: i64 = (0..exec.ctx.partition_number)
        .map(|part| {
            let valid = exec
                .hash_table_context
                .hash_table
                .sub_table(part)
                .row_data
                .valid_key_count();
            get_hash_table_memory_usage(get_hash_table_length_by_row_len(valid))
        })
        .sum();
    // The tracker also carries the row-table charge the builder made, so the
    // bucket total is a lower bound on it, and an exact match against what
    // the sub tables now report.
    assert!(exec.hash_table_context.memory_tracker.bytes_consumed() > expected);
    assert!(exec.hash_table_context.get_all_memory_usage_in_hash_table() >= expected);
    assert!(expected > 0);
    assert_eq!(
        exec.hash_table_context.memory_tracker.label(),
        LABEL_FOR_HASH_TABLE_IN_HASH_JOIN_V2
    );
}

#[test]
fn resetting_the_hash_table_context_for_restore_gives_back_the_bucket_memory() {
    let layout = one_int_key_layout();
    let (mut exec, _) = built_exec(1, JoinType::Inner, &layout, &[vec![build_chunk(&[1, 2])]]);
    let before = exec.hash_table_context.memory_tracker.bytes_consumed();
    assert!(before > 0);
    let in_hash_table = exec.hash_table_context.get_all_memory_usage_in_hash_table();
    assert!(in_hash_table > 0);
    HashJoinCtxV2::reset_hash_table_context_for_restore(&mut exec.hash_table_context);
    // Go gives back exactly what the sub tables hold, not the whole tracker:
    // the row-table charge was already released by clearAllSegmentsInRowTable.
    assert_eq!(
        exec.hash_table_context.memory_tracker.bytes_consumed(),
        before - in_hash_table
    );
    assert_eq!(
        exec.hash_table_context.get_all_memory_usage_in_hash_table(),
        0
    );
}

// ---------------------------------------------------------------------------
// checkBalance / createTasks (`hash_join_v2.go:1197`, `:1215`)
// ---------------------------------------------------------------------------

#[test]
fn balanced_partitions_become_one_whole_partition_task_each() {
    let layout = one_int_key_layout();
    // Concurrency 1 => 1 partition, so concurrency == partitionNumber and
    // every segment is in that one partition: perfectly balanced.
    let chunks = vec![vec![build_chunk(&[1, 2]), build_chunk(&[3, 4])]];
    let (exec, total) = built_exec(1, JoinType::Inner, &layout, &chunks);
    assert!(exec.check_balance(total));
    assert_eq!(
        exec.create_tasks(total),
        vec![BuildTask {
            partition_idx: 0,
            seg_start_idx: 0,
            seg_end_idx: total,
        }]
    );
}

#[test]
fn unbalanced_partitions_are_sliced_round_robin_and_cover_every_segment_once() {
    let layout = one_int_key_layout();
    // Concurrency 4 => 4 partitions. checkBalance requires
    // concurrency == partitionNumber, which holds, but the per-partition
    // segment counts come out uneven, so the round-robin path runs.
    let chunks: Vec<Vec<Chunk>> = (0..4)
        .map(|worker| {
            (0..3)
                .map(|chunk| build_chunk(&[(worker * 10 + chunk) as i64, 100 + worker as i64]))
                .collect()
        })
        .collect();
    let (exec, total) = built_exec(4, JoinType::Inner, &layout, &chunks);
    let tasks = exec.create_tasks(total);

    // Every segment of every partition is covered exactly once, by a
    // contiguous run of tasks, whichever branch createTasks took.
    for part in 0..exec.ctx.partition_number {
        let expected = exec
            .hash_table_context
            .hash_table
            .sub_table(part)
            .row_data
            .segments
            .len();
        let mut next = 0_usize;
        for task in tasks.iter().filter(|task| task.partition_idx == part) {
            assert_eq!(task.seg_start_idx, next, "partition {part} has a gap");
            assert!(task.seg_end_idx > task.seg_start_idx);
            next = task.seg_end_idx;
        }
        assert_eq!(next, expected, "partition {part} not fully covered");
    }

    // The round-robin ordering: consecutive tasks never repeat a partition
    // while another partition still has work, which is the property Go's
    // comment states.
    if !exec.check_balance(total) {
        for pair in tasks.windows(2) {
            if pair[0].partition_idx == pair[1].partition_idx {
                assert_eq!(
                    tasks
                        .iter()
                        .filter(|t| t.partition_idx != pair[0].partition_idx)
                        .count(),
                    0,
                    "a partition repeated while others still had segments"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// End-to-end: fetchAndBuildHashTableImpl then probe
// ---------------------------------------------------------------------------

#[test]
fn build_then_probe_finds_every_matching_build_row_and_only_those() {
    let layout = one_int_key_layout();
    // Duplicate keys, keys present on one worker only, and keys absent
    // entirely -- the three cases every join type branches on.
    let mut selected = build_chunk(&[99, 2, 1, 2, 888]);
    selected.set_sel(Some(vec![2, 1, 3]));
    let chunks_per_worker = vec![
        vec![selected, build_chunk(&[3])],
        vec![build_chunk(&[2, 4]), build_chunk(&[1])],
    ];
    let (exec, _) = built_exec(2, JoinType::Inner, &layout, &chunks_per_worker);

    let probe_keys = [1_i64, 2, 3, 4, 99];
    let matches = matches_for(&exec, &layout, &probe_keys);

    assert_eq!(matches[0], vec![1, 1], "key 1 is built twice");
    assert_eq!(matches[1], vec![2, 2, 2], "key 2 is built three times");
    assert_eq!(matches[2], vec![3]);
    assert_eq!(matches[3], vec![4]);
    assert!(matches[4].is_empty(), "key 99 is not on the build side");
}

#[test]
fn build_then_probe_agrees_with_each_join_type_match_rule() {
    let layout = one_int_key_layout();
    let chunks_per_worker = vec![vec![build_chunk(&[10, 20, 20, 30])]];
    let (exec, _) = built_exec(1, JoinType::Inner, &layout, &chunks_per_worker);

    let probe_keys = [10_i64, 20, 40];
    let matches = matches_for(&exec, &layout, &probe_keys);

    // Inner (right as build side): one output row per matched build row.
    let inner_rows: usize = matches.iter().map(Vec::len).sum();
    assert_eq!(inner_rows, 3, "10 matches once, 20 twice, 40 never");

    // Left outer with the right side as build: every probe row survives,
    // null-extended when it has no match.
    let left_outer_rows: usize = matches.iter().map(|m| m.len().max(1)).sum();
    assert_eq!(left_outer_rows, 4);

    // Semi: one row per probe row that has at least one match.
    let semi_rows = matches.iter().filter(|m| !m.is_empty()).count();
    assert_eq!(semi_rows, 2);

    // Anti semi: the complement.
    let anti_semi_rows = matches.iter().filter(|m| m.is_empty()).count();
    assert_eq!(anti_semi_rows, 1);

    // Left outer semi: one row per probe row, carrying the existence flag.
    assert_eq!(matches.len(), probe_keys.len());
}

#[test]
fn inner_probe_driver_emits_joined_rows_through_the_v2_worker_boundary() {
    let layout = one_int_key_layout();
    let chunks_per_worker = vec![vec![build_chunk(&[1, 2, 2, 3])]];
    let (exec, _) = built_exec(1, JoinType::Inner, &layout, &chunks_per_worker);

    let probe_context = ProbeContext {
        spill: None,
        hash_table: &exec.hash_table_context.hash_table,
        meta: &layout,
        column_count_needed_for_other_condition: 0,
        total_column_number: 1,
        tag_helper: exec.hash_table_context.tag_helper,
        partition_number: exec.ctx.partition_number,
        partition_mask_offset: exec.ctx.partition_mask_offset,
        has_other_condition: false,
        right_as_build_side: true,
        l_used: vec![0],
        r_used: vec![0],
        l_used_in_other_condition: Vec::new(),
        r_used_in_other_condition: Vec::new(),
        concurrency: exec.ctx.concurrency,
        max_chunk_size: 1024,
    };
    let serializer = key_columns(FieldTypeCode::LongLong);
    let mut probe = InnerJoinProbe::new(
        probe_context,
        0,
        vec![0],
        &[false],
        true,
        serializer,
        None,
        None,
    );
    let output_fields = vec![
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong),
    ];
    let results = run_join_worker(
        &mut probe,
        vec![probe_chunk(&[1]), probe_chunk(&[2]), probe_chunk(&[2, 99])],
        &|| Chunk::new(&output_fields, 2, 2),
    )
    .expect("inner probe");
    assert_eq!(
        results.iter().map(Chunk::num_rows).collect::<Vec<_>>(),
        vec![2, 2, 1]
    );

    let mut pairs: Vec<(i64, i64)> = results
        .iter()
        .flat_map(|chunk| {
            (0..chunk.num_rows()).map(|row| {
                let row = chunk.get_row(row);
                (row.get_int64(0), row.get_int64(1))
            })
        })
        .collect();
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(1, 1), (2, 2), (2, 2), (2, 2), (2, 2)]);
}

#[test]
fn right_build_semi_and_anti_probes_emit_each_preserved_row_once() {
    let layout = one_int_key_layout();
    let (exec, _) = built_exec(
        1,
        JoinType::Inner,
        &layout,
        &[vec![build_chunk(&[10, 20, 20, 30])]],
    );
    let probe_context = || ProbeContext {
        spill: None,
        hash_table: &exec.hash_table_context.hash_table,
        meta: &layout,
        column_count_needed_for_other_condition: 0,
        total_column_number: 1,
        tag_helper: exec.hash_table_context.tag_helper,
        partition_number: exec.ctx.partition_number,
        partition_mask_offset: exec.ctx.partition_mask_offset,
        has_other_condition: false,
        right_as_build_side: true,
        l_used: vec![0],
        r_used: Vec::new(),
        l_used_in_other_condition: Vec::new(),
        r_used_in_other_condition: Vec::new(),
        concurrency: exec.ctx.concurrency,
        max_chunk_size: 1024,
    };
    let serializer = key_columns(FieldTypeCode::LongLong);
    let output_fields = [FieldType::new(FieldTypeCode::LongLong)];
    let mut semi = SemiJoinProbe::new(
        probe_context(),
        0,
        vec![0],
        &[false],
        true,
        serializer.clone(),
        None,
        None,
    );
    let semi_results = run_join_worker(&mut semi, vec![probe_chunk(&[10, 20, 40])], &|| {
        Chunk::new(&output_fields, 2, 2)
    })
    .expect("semi probe");
    let semi_rows: Vec<i64> = semi_results
        .iter()
        .flat_map(|chunk| (0..chunk.num_rows()).map(|row| chunk.get_row(row).get_int64(0)))
        .collect();
    assert_eq!(semi_rows, vec![10, 20]);

    let mut anti = AntiSemiJoinProbe::new(
        probe_context(),
        0,
        vec![0],
        &[false],
        true,
        serializer.clone(),
        None,
        None,
    );
    let anti_results = run_join_worker(&mut anti, vec![probe_chunk(&[10, 20, 40])], &|| {
        Chunk::new(&output_fields, 2, 2)
    })
    .expect("anti-semi probe");
    let anti_rows: Vec<i64> = anti_results
        .iter()
        .flat_map(|chunk| (0..chunk.num_rows()).map(|row| chunk.get_row(row).get_int64(0)))
        .collect();
    assert_eq!(anti_rows, vec![40]);

    let outer_fields = [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong),
    ];
    let mut left_outer = LeftOuterSemiJoinProbe::new(
        probe_context(),
        0,
        vec![0],
        &[false],
        true,
        serializer.clone(),
        None,
        None,
    );
    let outer_results = run_join_worker(&mut left_outer, vec![probe_chunk(&[10, 20, 40])], &|| {
        Chunk::new(&outer_fields, 2, 2)
    })
    .expect("left outer semi probe");
    let outer_rows: Vec<(i64, i64)> = outer_results
        .iter()
        .flat_map(|chunk| {
            (0..chunk.num_rows()).map(|row| {
                let row = chunk.get_row(row);
                (row.get_int64(0), row.get_int64(1))
            })
        })
        .collect();
    assert_eq!(outer_rows, vec![(10, 1), (20, 1), (40, 0)]);
    // Go TestLeftOuterSemiJoinBuildResultFastPath: all rows fit in an empty
    // output, and input has no selection, so the columns are copied in bulk.
    let mut whole = LeftOuterSemiJoinProbe::new(
        probe_context(),
        0,
        vec![0],
        &[false],
        true,
        serializer.clone(),
        None,
        None,
    );
    let chunks = run_join_worker(&mut whole, vec![probe_chunk(&[10, 20, 40])], &|| {
        Chunk::new(&outer_fields, 128, 128)
    })
    .unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(
        (0..3)
            .map(|row| {
                let row = chunks[0].get_row(row);
                (row.get_int64(0), row.get_int64(1))
            })
            .collect::<Vec<_>>(),
        outer_rows
    );

    let mut anti_outer = AntiLeftOuterSemiJoinProbe::new(
        probe_context(),
        0,
        vec![0],
        &[false],
        true,
        serializer.clone(),
        None,
        None,
    );
    let anti_outer_results =
        run_join_worker(&mut anti_outer, vec![probe_chunk(&[10, 20, 40])], &|| {
            Chunk::new(&outer_fields, 2, 2)
        })
        .expect("anti-left outer semi probe");
    let anti_outer_rows: Vec<(i64, i64)> = anti_outer_results
        .iter()
        .flat_map(|chunk| {
            (0..chunk.num_rows()).map(|row| {
                let row = chunk.get_row(row);
                (row.get_int64(0), row.get_int64(1))
            })
        })
        .collect();
    assert_eq!(anti_outer_rows, vec![(10, 0), (20, 0), (40, 1)]);
}

// Go waitJoinWorkers must finish every prober before scanRowTableAfterProbeDone.
// One fast worker has no input; a deliberately later worker matches all build
// rows. Premature scanning would emit false NULL-extended right-side rows.
#[test]
fn outer_join_worker_scan_waits_for_all_probers_source() {
    use crossbeam_channel::{bounded, RecvTimeoutError};
    use std::time::Duration;
    let layout = preserved_int_key_layout();
    let (exec, _) = built_exec(
        2,
        JoinType::RightOuter,
        &layout,
        &[vec![build_chunk(&[1, 2])], vec![build_chunk(&[3, 4])]],
    );
    let fields = vec![FieldType::new(FieldTypeCode::LongLong); 2];
    let memory = StatementMemory::new(-1, OomAction::Cancel, 1);
    std::thread::scope(|scope| {
        let (events_tx, events) = bounded(3);
        let (close, close_rx) = bounded(0);
        let (scan_ready, scan_rx) = bounded(0);
        let mut scan_ready = Some(scan_ready);
        let mut inputs = Vec::new();
        let mut resources = Vec::new();
        for worker_id in 0..2 {
            let (worker, ports) = ProbeWorkerV2::new(
                worker_id,
                Chunk::new(&fields, 2, 2),
                events_tx.clone(),
                close_rx.clone(),
                scan_rx.clone(),
            );
            inputs.push(Some(ports.input));
            resources.push(ports.recycle);
            let (exec, layout, memory) = (&exec, &layout, &memory);
            scope.spawn(move || {
                let ctx = ProbeContext {
                    spill: None,
                    hash_table: &exec.hash_table_context.hash_table,
                    meta: layout,
                    column_count_needed_for_other_condition: 0,
                    total_column_number: 1,
                    tag_helper: exec.hash_table_context.tag_helper,
                    partition_number: exec.ctx.partition_number,
                    partition_mask_offset: exec.ctx.partition_mask_offset,
                    has_other_condition: false,
                    right_as_build_side: true,
                    l_used: vec![0],
                    r_used: vec![0],
                    l_used_in_other_condition: vec![],
                    r_used_in_other_condition: vec![],
                    concurrency: 2,
                    max_chunk_size: 2,
                };
                let mut probe = OuterJoinProbe::new(
                    ctx,
                    worker_id,
                    JoinType::RightOuter,
                    vec![0],
                    &[false],
                    true,
                    key_columns(FieldTypeCode::LongLong),
                    None,
                    None,
                );
                worker.run(&mut probe, memory);
            });
        }
        drop(events_tx);
        drop(inputs[0].take());
        let mut probe_done = [false; 2];
        let mut done = [false; 2];
        let mut rows = Vec::new();
        while done.iter().any(|done| !done) {
            match events
                .recv_timeout(Duration::from_secs(10))
                .expect("worker progress")
            {
                ProbeWorkerEvent::ProbeDone {
                    worker_id,
                    scan_required,
                } => {
                    assert!(scan_required);
                    probe_done[worker_id] = true;
                    if worker_id == 0 {
                        assert!(
                            matches!(
                                events.recv_timeout(Duration::from_millis(30)),
                                Err(RecvTimeoutError::Timeout)
                            ),
                            "scan preceded the probe barrier"
                        );
                        inputs[1]
                            .as_ref()
                            .unwrap()
                            .send(probe_chunk(&[1, 2, 3, 4]))
                            .unwrap();
                        drop(inputs[1].take());
                    }
                    if probe_done.iter().all(|done| *done) {
                        drop(scan_ready.take());
                    }
                }
                ProbeWorkerEvent::Output {
                    worker_id,
                    mut chunk,
                } => {
                    for row in 0..chunk.num_rows() {
                        let row = chunk.get_row(row);
                        assert!(!row.is_null(0), "a later worker matched this preserved row");
                        rows.push((row.get_int64(0), row.get_int64(1)));
                    }
                    chunk.reset();
                    let _ = resources[worker_id].send(chunk);
                }
                ProbeWorkerEvent::Input { chunk, .. } => assert_eq!(chunk.num_rows(), 0),
                ProbeWorkerEvent::Done { worker_id, .. } => {
                    assert!(probe_done.iter().all(|done| *done));
                    done[worker_id] = true;
                }
                ProbeWorkerEvent::FetcherDone | ProbeWorkerEvent::FetcherError { .. } => {
                    panic!("a directly driven worker has no fetcher")
                }
                ProbeWorkerEvent::Error { error, .. } => panic!("{error:?}"),
            }
        }
        drop(close);
        rows.sort_unstable();
        assert_eq!(rows, vec![(1, 1), (2, 2), (3, 3), (4, 4)]);
    });
}

// Go's Close / handleProbeWorkerPanic / SQLKiller paths and
// TestHashJoinRandomFail's slow-worker coverage, at deterministic queue phases.
#[test]
fn join_worker_close_and_kill_release_queue_waits_source() {
    use crossbeam_channel::bounded;
    use std::time::Duration;
    use tidb_util::sqlkiller::KillSignal;
    let layout = preserved_int_key_layout();
    let (exec, _) = built_exec(
        1,
        JoinType::RightOuter,
        &layout,
        &[vec![build_chunk(&[1, 1, 1])]],
    );
    let fields = vec![FieldType::new(FieldTypeCode::LongLong); 2];
    // Input wait, output-resource wait, full event queue, and scan barrier.
    for phase in 0..4 {
        for kill in [false, true] {
            let memory = StatementMemory::new(-1, OomAction::Cancel, 1);
            std::thread::scope(|scope| {
                let (events_tx, events) = bounded(if phase == 2 { 0 } else { 2 });
                let (close, close_rx) = bounded(0);
                let mut close = Some(close);
                let (_scan_ready, scan_rx) = bounded(0);
                let (finished_tx, finished) = bounded(1);
                let (worker, ports) =
                    ProbeWorkerV2::new(0, Chunk::new(&fields, 1, 1), events_tx, close_rx, scan_rx);
                let (exec, layout, memory) = (&exec, &layout, &memory);
                scope.spawn(move || {
                    let ctx = ProbeContext {
                        spill: None,
                        hash_table: &exec.hash_table_context.hash_table,
                        meta: layout,
                        column_count_needed_for_other_condition: 0,
                        total_column_number: 1,
                        tag_helper: exec.hash_table_context.tag_helper,
                        partition_number: exec.ctx.partition_number,
                        partition_mask_offset: exec.ctx.partition_mask_offset,
                        has_other_condition: false,
                        right_as_build_side: true,
                        l_used: vec![0],
                        r_used: vec![0],
                        l_used_in_other_condition: vec![],
                        r_used_in_other_condition: vec![],
                        concurrency: 1,
                        max_chunk_size: 1,
                    };
                    let mut probe = OuterJoinProbe::new(
                        ctx,
                        0,
                        JoinType::RightOuter,
                        vec![0],
                        &[false],
                        true,
                        key_columns(FieldTypeCode::LongLong),
                        None,
                        None,
                    );
                    worker.run(&mut probe, memory);
                    finished_tx.send(()).unwrap();
                });
                let mut input = Some(ports.input);
                if phase == 1 || phase == 2 {
                    input.as_ref().unwrap().send(probe_chunk(&[1])).unwrap();
                }
                if phase == 1 {
                    assert!(matches!(
                        events.recv_timeout(Duration::from_secs(10)).unwrap(),
                        ProbeWorkerEvent::Output { .. }
                    ));
                    // Deliberately do not return the output allocation.
                }
                if phase == 3 {
                    drop(input.take());
                    assert!(matches!(
                        events.recv_timeout(Duration::from_secs(10)).unwrap(),
                        ProbeWorkerEvent::ProbeDone {
                            scan_required: true,
                            ..
                        }
                    ));
                }
                if kill {
                    memory
                        .sql_killer()
                        .send_kill_signal(KillSignal::QueryInterrupted);
                    loop {
                        match events.recv_timeout(Duration::from_secs(10)).unwrap() {
                            ProbeWorkerEvent::Error { error, .. } => {
                                assert!(matches!(error, ExecError::Killed(_)));
                                break;
                            }
                            ProbeWorkerEvent::Output { .. } => {}
                            _ => panic!("worker advanced past a blocked stage"),
                        }
                    }
                } else {
                    drop(close.take());
                }
                finished
                    .recv_timeout(Duration::from_secs(10))
                    .expect("worker exits after close/kill");
                drop(close);
            });
        }
    }
}

#[test]
fn left_build_anti_semi_scans_unmatched_nullable_build_rows() {
    // Source: pkg/executor/join/anti_semi_join_probe_test.go and the
    // TypeNull regression from commit febee17ec7. A NULL build key is not a
    // hash match for the empty non-NULL probe key, and left-build anti-semi
    // must emit it from the post-probe row-table scan.
    let layout = type_null_key_layout();
    let ctx = {
        let mut ctx = HashJoinCtxV2::new(1, JoinType::AntiSemiJoin, false);
        ctx.need_scan_row_table_after_probe_done = true;
        ctx
    };
    let mut exec = HashJoinV2Exec::new(ctx, &[0], &[false]);
    let partition = PartitionInfo::new(exec.ctx.partition_number);
    let serializer = key_columns(FieldTypeCode::Null);
    let mut build_context = BuildContext::new(&layout, partition, &serializer);
    exec.begin_build(layout.null_map_length);
    exec.append_build_chunk(0, &type_null_build_chunk(), &mut build_context)
        .expect("nullable build side");
    exec.finish_build();

    let probe_context = ProbeContext {
        spill: None,
        hash_table: &exec.hash_table_context.hash_table,
        meta: &layout,
        column_count_needed_for_other_condition: 0,
        total_column_number: 1,
        tag_helper: exec.hash_table_context.tag_helper,
        partition_number: exec.ctx.partition_number,
        partition_mask_offset: exec.ctx.partition_mask_offset,
        has_other_condition: false,
        right_as_build_side: false,
        l_used: vec![0],
        r_used: Vec::new(),
        l_used_in_other_condition: Vec::new(),
        r_used_in_other_condition: Vec::new(),
        concurrency: exec.ctx.concurrency,
        max_chunk_size: 1024,
    };
    let serializer = key_columns(FieldTypeCode::Blob);
    let mut probe = AntiSemiJoinProbe::new(
        probe_context,
        0,
        vec![0],
        &[false],
        false,
        serializer,
        None,
        None,
    );
    assert!(probe.need_scan_row_table());
    let output_fields = [FieldType::new(FieldTypeCode::Null)];
    let probe_results = run_join_worker(&mut probe, vec![empty_blob_probe_chunk()], &|| {
        Chunk::new(&output_fields, 1, 1)
    })
    .expect("left-build anti-semi probe");
    assert!(probe_results.is_empty());

    let scan_results =
        scan_row_table_after_probe_done(&mut probe, &|| Chunk::new(&output_fields, 1, 1))
            .expect("left-build anti-semi row-table scan");
    let rows: Vec<Option<i64>> = scan_results
        .iter()
        .flat_map(|chunk| {
            (0..chunk.num_rows()).map(|row| {
                let row = chunk.get_row(row);
                (!row.is_null(0)).then(|| row.get_int64(0))
            })
        })
        .collect();
    assert_eq!(rows, vec![None]);
}

#[test]
fn probe_preserved_outer_join_emits_null_extension_across_output_chunks() {
    let layout = one_int_key_layout();
    let (exec, _) = built_exec(
        1,
        JoinType::LeftOuter,
        &layout,
        &[vec![build_chunk(&[1, 1, 3])]],
    );
    let probe_context = ProbeContext {
        spill: None,
        hash_table: &exec.hash_table_context.hash_table,
        meta: &layout,
        column_count_needed_for_other_condition: 0,
        total_column_number: 1,
        tag_helper: exec.hash_table_context.tag_helper,
        partition_number: exec.ctx.partition_number,
        partition_mask_offset: exec.ctx.partition_mask_offset,
        has_other_condition: false,
        right_as_build_side: true,
        l_used: vec![0],
        r_used: vec![0],
        l_used_in_other_condition: Vec::new(),
        r_used_in_other_condition: Vec::new(),
        concurrency: exec.ctx.concurrency,
        max_chunk_size: 1024,
    };
    let serializer = key_columns(FieldTypeCode::LongLong);
    let mut probe = OuterJoinProbe::new(
        probe_context,
        0,
        JoinType::LeftOuter,
        vec![0],
        &[false],
        true,
        serializer.clone(),
        None,
        None,
    );
    let output_fields = [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong),
    ];
    let results = run_join_worker(&mut probe, vec![probe_chunk(&[1, 2, 4, 5, 6, 7])], &|| {
        Chunk::new(&output_fields, 2, 2)
    })
    .expect("outer probe");
    assert!(
        results.iter().all(|chunk| chunk.num_rows() <= 2),
        "unmatched rows must obey RequiredRows"
    );
    let rows: Vec<(i64, Option<i64>)> = results
        .iter()
        .flat_map(|chunk| {
            (0..chunk.num_rows()).map(|row| {
                let row = chunk.get_row(row);
                (
                    row.get_int64(0),
                    (!row.is_null(1)).then(|| row.get_int64(1)),
                )
            })
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            (1, Some(1)),
            (1, Some(1)),
            (2, None),
            (4, None),
            (5, None),
            (6, None),
            (7, None)
        ]
    );
}

#[test]
fn probe_preserved_right_outer_join_places_null_build_columns_first() {
    let layout = one_int_key_layout();
    let (exec, _) = built_exec(
        1,
        JoinType::RightOuter,
        &layout,
        &[vec![build_chunk(&[1, 1, 3])]],
    );
    let probe_context = ProbeContext {
        spill: None,
        hash_table: &exec.hash_table_context.hash_table,
        meta: &layout,
        column_count_needed_for_other_condition: 0,
        total_column_number: 1,
        tag_helper: exec.hash_table_context.tag_helper,
        partition_number: exec.ctx.partition_number,
        partition_mask_offset: exec.ctx.partition_mask_offset,
        has_other_condition: false,
        right_as_build_side: false,
        l_used: vec![0],
        r_used: vec![0],
        l_used_in_other_condition: Vec::new(),
        r_used_in_other_condition: Vec::new(),
        concurrency: exec.ctx.concurrency,
        max_chunk_size: 1024,
    };
    let serializer = key_columns(FieldTypeCode::LongLong);
    let mut probe = OuterJoinProbe::new(
        probe_context,
        0,
        JoinType::RightOuter,
        vec![0],
        &[false],
        false,
        serializer.clone(),
        None,
        None,
    );
    let output_fields = [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong),
    ];
    let results = run_join_worker(&mut probe, vec![probe_chunk(&[1, 2])], &|| {
        Chunk::new(&output_fields, 2, 2)
    })
    .expect("right outer probe");
    let rows: Vec<(Option<i64>, i64)> = results
        .iter()
        .flat_map(|chunk| {
            (0..chunk.num_rows()).map(|row| {
                let row = chunk.get_row(row);
                (
                    (!row.is_null(0)).then(|| row.get_int64(0)),
                    row.get_int64(1),
                )
            })
        })
        .collect();
    assert_eq!(rows, vec![(Some(1), 1), (Some(1), 1), (None, 2)]);
}

#[test]
fn a_build_side_with_no_rows_leaves_every_partition_hash_table_empty() {
    let layout = one_int_key_layout();
    let (exec, total) = built_exec(2, JoinType::Inner, &layout, &[Vec::new(), Vec::new()]);
    assert_eq!(total, 0);
    assert!(exec.hash_table_context.hash_table.is_hash_table_empty());
    // Which is exactly the condition canSkipProbeIfHashTableIsEmpty guards.
    assert!(exec.ctx.can_skip_probe_if_hash_table_is_empty());
    assert!(matches_for(&exec, &layout, &[1, 2])
        .iter()
        .all(Vec::is_empty));
}

// ---------------------------------------------------------------------------
// NewJoinBuildWorkerV2 (`hash_join_v2.go:588`)
// ---------------------------------------------------------------------------

#[test]
fn a_build_worker_has_a_nullable_key_when_any_key_column_is_nullable() {
    // Go: hasNullableKey is true as soon as one key column lacks NOT NULL.
    let worker = new_join_build_worker_v2(0, vec![0, 2], &[true, false, true]);
    assert!(!worker.has_nullable_key, "both key columns are NOT NULL");

    let worker = new_join_build_worker_v2(1, vec![0, 2], &[true, true, false]);
    assert!(worker.has_nullable_key);
    assert_eq!(worker.worker_id, 1);
    assert!(worker.builder.is_none(), "the builder is created later");
}

/// Go TestSemiJoinProbeOtherCondition, TestAntiSemiJoinProbeBasic and
/// TestLeftOuterSemiJoinProbeOtherCondition: both build sides, duplicate
/// keys, selected chunks, IN-derived NULL and ordinary NULL, partial output.
#[test]
fn semi_family_chunk_residual_matrix_source() {
    for kind in [
        JoinType::SemiJoin,
        JoinType::AntiSemiJoin,
        JoinType::LeftOuterSemiJoin,
        JoinType::AntiLeftOuterSemiJoin,
    ] {
        let outer = matches!(
            kind,
            JoinType::LeftOuterSemiJoin | JoinType::AntiLeftOuterSemiJoin
        );
        for right_build in [true, false] {
            if outer && !right_build {
                continue;
            } // Go supports outer-semi right build only.
            for residual in 0..4 {
                for filtered in [false, true] {
                    for empty_output in [false, true] {
                        for vectorized in [false, true] {
                            check_semi_family(
                                kind,
                                right_build,
                                residual,
                                filtered,
                                empty_output,
                                vectorized,
                            );
                        }
                    }
                }
            }
        }
    }
}

fn check_semi_family(
    kind: JoinType,
    right_build: bool,
    residual: usize,
    filtered: bool,
    empty_output: bool,
    vectorized: bool,
) {
    use tidb_ast::CiString;
    use tidb_datatype::Datum;
    use tidb_exec::{base_join_probe::JoinFilter, hash_join_v2::JoinOtherCondition};
    use tidb_expr::{
        column::Column,
        expression::{Constant, Expression, ScalarFunction},
        NoColumns,
    };
    let int = FieldType::new(FieldTypeCode::LongLong);
    let types = vec![int.clone(); 2];
    let all_types = vec![int.clone(); 4];
    let column = |index: i64, in_operand| {
        let mut col = Column::new(index + 1, int.clone());
        col.index = index;
        col.in_operand = in_operand;
        Expression::Column(col)
    };
    let constant = |value| Expression::Constant(Constant::new(Datum::Int(value), int.clone()));
    let function = |name, args| {
        Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), int.clone(), args))
    };
    let left_values: Vec<_> = [
        (Some(10001), Some(5)),
        (Some(10001), None),
        (Some(2), Some(1)),
        (Some(10003), Some(9)),
        (None, Some(5)),
        (Some(4), Some(4)),
    ]
    .into_iter()
    .cycle()
    .take(80)
    .collect();
    let right_values: Vec<_> = [
        (Some(10001), Some(2)),
        (Some(10001), Some(8)),
        (Some(10001), None),
        (Some(2), Some(1)),
        (Some(10003), None),
        (Some(5), Some(2)),
    ]
    .into_iter()
    .cycle()
    .take(91)
    .collect();
    let make_input = |values: &[(Option<i64>, Option<i64>)]| {
        let mut chunk = Chunk::new_with_capacity(&types, values.len());
        for &(key, cmp) in values {
            for (index, value) in [key, cmp].into_iter().enumerate() {
                if let Some(value) = value {
                    chunk.append_int64(index, value);
                } else {
                    chunk.append_null(index);
                }
            }
        }
        chunk.set_sel(Some(
            (0..values.len()).rev().filter(|row| row % 5 != 0).collect(),
        ));
        chunk
    };
    let left = make_input(&left_values);
    let right = make_input(&right_values);
    let outer = matches!(
        kind,
        JoinType::LeftOuterSemiJoin | JoinType::AntiLeftOuterSemiJoin
    );
    let anti = matches!(
        kind,
        JoinType::AntiSemiJoin | JoinType::AntiLeftOuterSemiJoin
    );
    let used = if empty_output { vec![] } else { vec![0, 1] };
    let build_used = if right_build { vec![] } else { used.clone() };
    let build_input = if right_build { &right } else { &left };
    let probe_input = if right_build { &left } else { &right };
    let build_filter = filtered && !right_build;
    let meta = JoinTableMeta::new(
        &[0],
        &[ColumnType::Int; 2],
        &[ColumnType::Int],
        &[ColumnType::Int],
        Some(&[1]),
        Some(&build_used),
        !right_build,
    );
    let layout =
        RowLayoutMeta::from_join_table_meta(&meta, vec![Some(8); meta.row_columns_order.len()]);
    let mut ctx = HashJoinCtxV2::new(4, kind, right_build);
    ctx.has_other_condition = residual != 0;
    ctx.has_build_filter = build_filter;
    ctx.need_scan_row_table_after_probe_done = !right_build;
    let mut exec = HashJoinV2Exec::new(ctx, &[0], &[false; 2]);
    let keys = key_columns(FieldTypeCode::LongLong);
    let filter = JoinFilter::new(
        NoColumns,
        vec![function("gt", vec![column(0, false), constant(10000)])],
        vectorized,
    );
    let mut context = BuildContext::new(&layout, PartitionInfo::new(4), &keys);
    context.build_filter = build_filter.then_some(&filter);
    let mut source = BuildSource::new(vec![build_input.clone()]);
    let memory = StatementMemory::new(-1, OomAction::Cancel, 1);
    exec.fetch_and_build_hash_table(&mut source, &context, &memory)
        .unwrap();
    let predicate = match residual {
        0 => None,
        1 => Some(function("gt", vec![column(1, false), column(3, false)])),
        2 => Some(function("eq", vec![column(1, true), column(3, true)])),
        3 => Some(function("getparam", vec![constant(-1)])),
        _ => unreachable!(),
    };
    let make_probe = |worker| -> Box<dyn ProbeV2 + '_> {
        let ctx = ProbeContext {
            spill: None,
            hash_table: &exec.hash_table_context.hash_table,
            meta: &layout,
            column_count_needed_for_other_condition: meta.column_count_needed_for_other_condition,
            total_column_number: 2,
            tag_helper: exec.hash_table_context.tag_helper,
            partition_number: exec.ctx.partition_number,
            partition_mask_offset: exec.ctx.partition_mask_offset,
            has_other_condition: residual != 0,
            right_as_build_side: right_build,
            l_used: used.clone(),
            r_used: vec![],
            l_used_in_other_condition: vec![1],
            r_used_in_other_condition: vec![1],
            concurrency: 4,
            max_chunk_size: 7,
        };
        let condition = predicate.as_ref().map(|predicate| {
            JoinOtherCondition::new(
                NoColumns,
                vec![predicate.clone()],
                &all_types,
                7,
                vectorized,
            )
        });
        let filter = (filtered && right_build).then_some(&filter);
        match kind {
            JoinType::SemiJoin => Box::new(SemiJoinProbe::new(
                ctx,
                worker,
                vec![0],
                &[true],
                right_build,
                keys.clone(),
                filter,
                condition,
            )),
            JoinType::AntiSemiJoin => Box::new(AntiSemiJoinProbe::new(
                ctx,
                worker,
                vec![0],
                &[true],
                right_build,
                keys.clone(),
                filter,
                condition,
            )),
            JoinType::LeftOuterSemiJoin => Box::new(LeftOuterSemiJoinProbe::new(
                ctx,
                worker,
                vec![0],
                &[true],
                right_build,
                keys.clone(),
                filter,
                condition,
            )),
            JoinType::AntiLeftOuterSemiJoin => Box::new(AntiLeftOuterSemiJoinProbe::new(
                ctx,
                worker,
                vec![0],
                &[true],
                right_build,
                keys.clone(),
                filter,
                condition,
            )),
            _ => unreachable!(),
        }
    };
    let fields = vec![int; used.len() + usize::from(outer)];
    let output = || Chunk::new(&fields, 3, 3);
    let mut probe = make_probe(0);
    let result = run_join_worker(
        probe.as_mut(),
        vec![probe_input.clone(), probe_input.clone()],
        &output,
    );
    if residual == 3 {
        assert!(matches!(
            result,
            Err(ExecError::Eval(
                tidb_expr::EvalError::ParamIndexExceedParamCounts
            ))
        ));
        return;
    }
    let mut chunks = result.unwrap();
    if !right_build {
        // Resetting one worker must not clear marks owned by the shared table.
        probe.reset_probe();
        for worker in 0..4 {
            let mut scanner = make_probe(worker);
            chunks.extend(scan_row_table_after_probe_done(scanner.as_mut(), &output).unwrap());
        }
    }
    let mut expected = Vec::new();
    for &row in left.sel().unwrap() {
        let (key, cmp) = left_values[row];
        let mut matched = false;
        let mut null = false;
        for &right_row in right.sel().unwrap() {
            let (right_key, right_cmp) = right_values[right_row];
            if key.is_none()
                || key != right_key
                || (filtered && !key.is_some_and(|key| key > 10000))
            {
                continue;
            }
            match residual {
                0 => matched = true,
                1 => matched |= cmp.zip(right_cmp).is_some_and(|(a, b)| a > b),
                2 => {
                    if let Some((a, b)) = cmp.zip(right_cmp) {
                        matched |= a == b;
                    } else {
                        null = true;
                    }
                }
                _ => unreachable!(),
            }
        }
        if outer || if anti { !(matched || null) } else { matched } {
            let mut result = if empty_output { vec![] } else { vec![key, cmp] };
            if outer {
                result.push(if matched {
                    Some(i64::from(!anti))
                } else if null {
                    None
                } else {
                    Some(i64::from(anti))
                });
            }
            expected.push(result.clone());
            if right_build {
                expected.push(result);
            } // two identical probe chunks
        }
    }
    let mut actual: Vec<Vec<Option<i64>>> = chunks
        .iter()
        .flat_map(|chunk| {
            assert!(chunk.num_rows() <= 3);
            (0..chunk.num_rows()).map(|row| {
                (0..fields.len())
                    .map(|col| {
                        let row = chunk.get_row(row);
                        (!row.is_null(col)).then(|| row.get_int64(col))
                    })
                    .collect()
            })
        })
        .collect();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected, "{kind:?} right_build={right_build} residual={residual} filtered={filtered} empty={empty_output} vectorized={vectorized}");
    assert_probe_canceled(make_probe(0).as_mut(), probe_input.clone(), &output);
}

// Go killedDuringProbe: cancellation must be observed inside Probe/Scan,
// before the worker transport can publish a partially constructed result.
fn assert_probe_canceled(probe: &mut dyn ProbeV2, input: Chunk, new_output: &dyn Fn() -> Chunk) {
    let killer = tidb_util::sqlkiller::SqlKiller::default();
    killer.send_kill_signal(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
    probe.set_chunk_for_probe(input).unwrap();
    let mut output = new_output();
    assert!(matches!(probe.probe(&mut output, &killer),
        Err(tidb_exec::base_join_probe::ProbeError::Killed(ref error)) if error.code == 1317));
    assert!(
        !output.is_incomplete_chunk(),
        "Go defer restores the output shape on error"
    );
    if probe.need_scan_row_table() {
        output.reset();
        probe.init_for_scan_row_table();
        assert!(matches!(probe.scan_row_table(&mut output, &killer),
            Err(tidb_exec::base_join_probe::ProbeError::Killed(ref error)) if error.code == 1317));
    }
}

/// Go HashJoinV2Exec.Next/Close, probe fetcher, and waitJoinWorkers: native
/// child batches, result swaps, global scan barrier, typed failures, and Close.
#[test]
fn native_hash_join_executor_lifecycle_source() {
    use tidb_datatype::Datum;
    use tidb_exec::hash_join_v2::executor::{HashJoinV2Executor, HashJoinV2Plan};
    use tidb_expr::{
        column::Column,
        expression::{Constant, Expression, ScalarFunction},
        schema::Schema,
        NoColumns,
    };
    use tidb_util::sqlkiller::KillSignal;
    fn statement_context_can_cross_worker_lanes<
        C: tidb_expr::Columns + Clone + Send + Sync + 'static,
    >() {
    }
    statement_context_can_cross_worker_lanes::<tidb_executor::StmtContext>();
    for concurrency in [1, 4, 5] {
        for kind in [
            JoinType::Inner,
            JoinType::LeftOuter,
            JoinType::RightOuter,
            JoinType::SemiJoin,
            JoinType::AntiSemiJoin,
            JoinType::LeftOuterSemiJoin,
            JoinType::AntiLeftOuterSemiJoin,
        ] {
            for right_build in [false, true] {
                let outer_semi = matches!(
                    kind,
                    JoinType::LeftOuterSemiJoin | JoinType::AntiLeftOuterSemiJoin
                );
                if outer_semi && !right_build {
                    continue;
                }
                for mode in 0..17 {
                    let memory =
                        StatementMemory::new(if mode == 9 { 1 } else { -1 }, OomAction::Cancel, 1);
                    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
                    let int = FieldType::new(FieldTypeCode::LongLong);
                    let make_chunk = |values: &[Option<i64>]| {
                        let mut chunk = Chunk::new_with_capacity(&[int.clone()], values.len() + 1);
                        for &value in values {
                            match value {
                                Some(value) => chunk.append_int64(0, value),
                                None => chunk.append_null(0),
                            }
                        }
                        chunk.append_int64(0, 99);
                        chunk.set_sel(Some((0..values.len()).rev().collect()));
                        chunk
                    };
                    let left: &[Option<i64>] = if mode == 14 && right_build {
                        &[]
                    } else {
                        &[Some(1), Some(2), Some(2), Some(9), None]
                    };
                    let right: &[Option<i64>] = if mode == 14 && !right_build {
                        &[]
                    } else {
                        &[Some(1), Some(2), Some(3), None]
                    };
                    let (left_entered, left_wait) = crossbeam_channel::bounded(1);
                    let (right_entered, right_wait) = crossbeam_channel::bounded(1);
                    let source = |side, is_build: bool, chunks: Vec<Chunk>| {
                        let mut source = BuildSource::new(vec![]);
                        if mode == 12 {
                            source.first_fetch = Some(if side == "left" {
                                (left_entered.clone(), right_wait.clone())
                            } else {
                                (right_entered.clone(), left_wait.clone())
                            });
                            if !is_build {
                                source.first_required_rows = Some(
                                    if matches!(
                                        (kind, right_build),
                                        (JoinType::LeftOuter, true) | (JoinType::RightOuter, false)
                                    ) {
                                        2
                                    } else {
                                        1024
                                    },
                                );
                            }
                        }
                        source.fail_at = ((mode == 4 && is_build)
                            || (matches!(mode, 5 | 13) && !is_build))
                            .then_some(1);
                        source.panic_at =
                            ((mode == 15 && is_build) || (mode == 16 && !is_build)).then_some(1);
                        source.lifecycle = Some(SourceLifecycle {
                            side,
                            events: events.clone(),
                            chunks: if mode == 13 && is_build {
                                vec![]
                            } else {
                                chunks
                            },
                            memory: memory.clone(),
                            fail_open: (mode == 6 && side == "left")
                                || (mode == 7 && side == "right"),
                            fail_close: mode == 8,
                        });
                        Box::new(source) as Box<dyn Executor>
                    };
                    let semi = !matches!(
                        kind,
                        JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter
                    );
                    let width = if semi && !outer_semi { 1 } else { 2 };
                    let meta = ExecutorMeta::new(
                        Schema::new(
                            (0..width)
                                .map(|index| Column::new(index, int.clone()))
                                .collect(),
                        ),
                        9,
                        3,
                        3,
                    );
                    let column = |index| {
                        let mut col = Column::new(index, int.clone());
                        col.index = index;
                        Expression::Column(col)
                    };
                    let constant =
                        |value| Expression::Constant(Constant::new(Datum::Int(value), int.clone()));
                    let function = |name, args| {
                        Expression::ScalarFunction(ScalarFunction::new(
                            tidb_ast::CiString::new(name),
                            int.clone(),
                            args,
                        ))
                    };
                    let mut plan = HashJoinV2Plan {
                        concurrency,
                        join_type: kind,
                        right_as_build_side: right_build,
                        build_key_indices: vec![0],
                        probe_key_indices: vec![0],
                        build_key_types: vec![int.clone()],
                        probe_key_types: vec![int.clone()],
                        l_used: vec![0],
                        r_used: if semi { vec![] } else { vec![0] },
                        l_used_in_other_condition: vec![],
                        r_used_in_other_condition: vec![],
                        build_filter: vec![],
                        probe_filter: vec![],
                        other_condition: vec![],
                        vectorized: true,
                    };
                    if mode == 10 {
                        plan.other_condition = vec![function("getparam", vec![constant(-1)])];
                    } else if mode == 11 {
                        plan.l_used_in_other_condition = vec![0];
                        plan.other_condition = vec![function("gt", vec![column(0), constant(1)])];
                        let filter = vec![function("lt", vec![column(0), constant(9)])];
                        if right_build {
                            plan.probe_filter = filter;
                        } else {
                            plan.build_filter = filter;
                        }
                        plan.vectorized = false;
                    }
                    let mut exec: Box<dyn Executor> = Box::new(HashJoinV2Executor::new(
                        meta,
                        plan,
                        source(
                            "left",
                            !right_build,
                            vec![make_chunk(&left), make_chunk(&left)],
                        ),
                        source("right", right_build, vec![make_chunk(&right)]),
                        NoColumns,
                        memory.clone(),
                    ));
                    let opened = exec.open();
                    if mode == 6 || mode == 7 {
                        assert!(
                            matches!(opened, Err(ExecError::Internal(ref msg)) if msg.contains("open"))
                        );
                    } else {
                        opened.unwrap();
                        assert_eq!(
                            &*events.lock().unwrap(),
                            &["left:open", "right:open"],
                            "Open must not fetch input"
                        );
                    }
                    if mode == 3 {
                        memory
                            .sql_killer()
                            .send_kill_signal(KillSignal::QueryInterrupted);
                    }
                    let mut output = exec.new_chunk();
                    output.set_required_rows(2, 3);
                    let mut actual = Vec::new();
                    let result = if opened_is_error(mode) {
                        Ok(())
                    } else {
                        loop {
                            if let Err(error) = exec.next(&mut output) {
                                break Err(error);
                            }
                            if output.num_rows() == 0 {
                                break Ok(());
                            }
                            assert!(output.num_rows() <= 3);
                            for col in 0..width as usize {
                                assert_eq!(output.column(col).rows(), output.num_rows(), "{kind:?} right_build={right_build} concurrency={concurrency} mode={mode} column={col}");
                            }
                            actual.extend((0..output.num_rows()).map(|row| {
                                (0..width as usize)
                                    .map(|col| {
                                        let row = output.get_row(row);
                                        (!row.is_null(col)).then(|| row.get_int64(col))
                                    })
                                    .collect::<Vec<_>>()
                            }));
                            if mode == 1 || mode == 2 {
                                break Ok(());
                            }
                        }
                    };
                    match mode {
                        3 => assert!(
                            matches!(result, Err(ExecError::Killed(ref e)) if e.code == 1317)
                        ),
                        4 | 5 | 10 | 13 => assert!(matches!(
                            result,
                            Err(ExecError::Eval(
                                tidb_expr::EvalError::ParamIndexExceedParamCounts
                            ))
                        )),
                        9 => assert!(matches!(result, Err(ExecError::MemoryExceedForQuery { .. }) | Err(ExecError::Killed(_)))
                            || matches!(&result, Err(ExecError::Internal(error)) if concurrency > 1 && error == "Exceed max spill round"),
                            "{kind:?} right_build={right_build} concurrency={concurrency}: {result:?}"),
                        15 | 16 => assert!(matches!(result, Err(ExecError::Internal(ref message)) if message.contains("build source panic"))),
                        _ => result.unwrap(),
                    }
                    if matches!(mode, 0 | 8 | 11 | 12 | 14) {
                        let mut expected = Vec::new();
                        let mut right_matched = vec![false; right.len()];
                        for &left in left.iter().cycle().take(left.len() * 2) {
                            let mut matched = false;
                            for (index, &right) in right.iter().enumerate() {
                                if left.is_some()
                                    && left == right
                                    && (mode != 11 || left.is_some_and(|v| v > 1 && v < 9))
                                {
                                    matched = true;
                                    right_matched[index] = true;
                                    if !semi {
                                        expected.push(vec![left, right]);
                                    }
                                }
                            }
                            match kind {
                                JoinType::LeftOuter if !matched => expected.push(vec![left, None]),
                                JoinType::SemiJoin if matched => expected.push(vec![left]),
                                JoinType::AntiSemiJoin if !matched => expected.push(vec![left]),
                                JoinType::LeftOuterSemiJoin => {
                                    expected.push(vec![left, Some(i64::from(matched))])
                                }
                                JoinType::AntiLeftOuterSemiJoin => {
                                    expected.push(vec![left, Some(i64::from(!matched))])
                                }
                                _ => {}
                            }
                        }
                        if kind == JoinType::RightOuter {
                            for (index, &right) in right.iter().enumerate() {
                                if !right_matched[index] {
                                    expected.push(vec![None, right]);
                                }
                            }
                        }
                        actual.sort();
                        expected.sort();
                        assert_eq!(actual, expected, "{kind:?} right_build={right_build} concurrency={concurrency} mode={mode}");
                        // EOF must neither refetch nor restart worker lanes.
                        let event_count = events.lock().unwrap().len();
                        exec.next(&mut output).unwrap();
                        assert_eq!(output.num_rows(), 0);
                        assert_eq!(events.lock().unwrap().len(), event_count);
                    }
                    if mode == 2 {
                        drop(exec);
                    } else {
                        let closed = exec.close();
                        if mode == 8 {
                            assert!(
                                matches!(closed, Err(ExecError::Internal(ref msg)) if msg == "left close")
                            );
                        } else {
                            closed.unwrap();
                        }
                        exec.close().unwrap();
                        if mode == 0 {
                            // Reopen must allocate a fresh table and marks, not reuse a drained stage.
                            exec.open().unwrap();
                            let mut reopened = Vec::new();
                            loop {
                                exec.next(&mut output).unwrap();
                                if output.num_rows() == 0 {
                                    break;
                                }
                                reopened.extend((0..output.num_rows()).map(|row| {
                                    (0..width as usize)
                                        .map(|col| {
                                            let row = output.get_row(row);
                                            (!row.is_null(col)).then(|| row.get_int64(col))
                                        })
                                        .collect::<Vec<_>>()
                                }));
                            }
                            reopened.sort();
                            assert_eq!(reopened, actual);
                            exec.close().unwrap();
                        }
                        drop(exec);
                    }
                    assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
                    let events = events.lock().unwrap();
                    assert_eq!(&events[events.len() - 2..], &["left:close", "right:close"]);
                    assert_eq!(
                        events
                            .iter()
                            .filter(|event| event.as_str() == "left:close")
                            .count(),
                        if mode == 0 { 2 } else { 1 }
                    );
                }
            }
        }
    }
    fn opened_is_error(mode: usize) -> bool {
        mode == 6 || mode == 7
    }
}

/// Go probe fetcher and worker barrier, independently of the owning facade.
#[test]
fn native_probe_stage_next_close_and_error_source() {
    use std::sync::Arc;
    use std::time::Duration;
    use tidb_exec::hash_join_v2::probe_stage::ProbeStage;
    use tidb_util::sqlkiller::KillSignal;
    for concurrency in [1, 4, 5] {
        for kind in [JoinType::Inner, JoinType::LeftOuter, JoinType::RightOuter] {
            for mode in 0..9 {
                let memory = StatementMemory::new(-1, OomAction::Cancel, 1);
                let (stop, stopped) = std::sync::mpsc::channel();
                let deadline_memory = memory.clone();
                let watchdog = std::thread::spawn(move || {
                    if matches!(
                        stopped.recv_timeout(Duration::from_secs(10)),
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                    ) {
                        deadline_memory
                            .sql_killer()
                            .send_kill_signal(KillSignal::QueryInterrupted);
                    }
                });
                let layout = if kind == JoinType::RightOuter {
                    preserved_int_key_layout()
                } else {
                    one_int_key_layout()
                };
                let keys = key_columns(FieldTypeCode::LongLong);
                let mut ctx = HashJoinCtxV2::new(concurrency, kind, true);
                ctx.need_scan_row_table_after_probe_done = kind == JoinType::RightOuter;
                let mut exec = HashJoinV2Exec::new(ctx, &[0], &[true]);
                let build_chunks = if mode == 7 {
                    vec![]
                } else {
                    vec![probe_chunk(&[1, 2, 3, 4, 5])]
                };
                let mut build = BuildSource::new(build_chunks);
                let context = BuildContext::new(&layout, PartitionInfo::new(concurrency), &keys);
                exec.fetch_and_build_hash_table(&mut build, &context, &memory)
                    .unwrap();
                let fixture = Arc::new((exec, layout));
                let mut source = BuildSource::new(
                    (0..13)
                        .map(|batch| {
                            let mut chunk = probe_chunk(&[2, 2, 4, 9]);
                            if batch % 2 == 1 {
                                chunk.set_sel(Some(vec![2, 1]));
                            }
                            chunk
                        })
                        .collect(),
                );
                source.fail_at = (mode == 1).then_some(3);
                source.panic_at = (mode == 2).then_some(3);
                let output_meta = ExecutorMeta::new(
                    tidb_expr::schema::Schema::new(
                        (0..2)
                            .map(|index| {
                                tidb_expr::column::Column::new(
                                    index,
                                    FieldType::new(FieldTypeCode::LongLong),
                                )
                            })
                            .collect(),
                    ),
                    9,
                    3,
                    3,
                );
                let lane_fixture = Arc::clone(&fixture);
                let observations =
                    Arc::new(std::sync::Mutex::new(ProbeSourceObservations::default()));
                source.observations = Some(Arc::clone(&observations));
                let mut source = Some(Box::new(source) as Box<dyn Executor>);
                let mut stage = ProbeStage::new(
                    &mut source,
                    &output_meta,
                    &fixture.0,
                    memory.clone(),
                    move |worker_id, worker, memory| {
                        assert!(!(mode == 3 && worker_id == 0), "probe factory panic");
                        let (exec, layout) = &*lane_fixture;
                        let ctx = ProbeContext {
                            spill: None,
                            hash_table: &exec.hash_table_context.hash_table,
                            meta: layout,
                            column_count_needed_for_other_condition: 0,
                            total_column_number: 1,
                            tag_helper: exec.hash_table_context.tag_helper,
                            partition_number: exec.ctx.partition_number,
                            partition_mask_offset: exec.ctx.partition_mask_offset,
                            has_other_condition: false,
                            right_as_build_side: true,
                            l_used: vec![0],
                            r_used: vec![0],
                            l_used_in_other_condition: vec![],
                            r_used_in_other_condition: vec![],
                            concurrency,
                            max_chunk_size: 3,
                        };
                        let filter = tidb_exec::base_join_probe::JoinFilter::new(
                            tidb_expr::NoColumns,
                            vec![tidb_expr::expression::Expression::ScalarFunction(
                                tidb_expr::expression::ScalarFunction::new(
                                    tidb_ast::CiString::new("getparam"),
                                    FieldType::new(FieldTypeCode::LongLong),
                                    vec![tidb_expr::expression::Expression::Constant(
                                        tidb_expr::constant::Constant::new(
                                            tidb_datatype::Datum::Int(-1),
                                            FieldType::new(FieldTypeCode::LongLong),
                                        ),
                                    )],
                                ),
                            )],
                            true,
                        );
                        let filter = (mode == 8).then_some(&filter);
                        let mut probe: Box<dyn ProbeV2> = if kind == JoinType::Inner {
                            Box::new(InnerJoinProbe::new(
                                ctx,
                                worker_id,
                                vec![0],
                                &[false],
                                true,
                                key_columns(FieldTypeCode::LongLong),
                                filter,
                                None,
                            ))
                        } else {
                            Box::new(OuterJoinProbe::new(
                                ctx,
                                worker_id,
                                kind,
                                vec![0],
                                &[false],
                                true,
                                key_columns(FieldTypeCode::LongLong),
                                filter,
                                None,
                            ))
                        };
                        worker.run(probe.as_mut(), memory);
                    },
                )
                .unwrap();
                assert_eq!(
                    observations.lock().unwrap().allocations,
                    if mode == 7 && kind != JoinType::LeftOuter {
                        0
                    } else {
                        concurrency
                    }
                );
                if mode == 4 {
                    memory
                        .sql_killer()
                        .send_kill_signal(KillSignal::QueryInterrupted);
                }
                let mut output = output_meta.new_chunk();
                output.set_required_rows(2, 3);
                let mut rows = Vec::new();
                let result = if mode == 6 {
                    Ok(())
                } else {
                    loop {
                        if let Err(error) = stage.next(&mut output) {
                            break Err(error);
                        }
                        if output.num_rows() == 0 {
                            break Ok(());
                        }
                        rows.extend((0..output.num_rows()).map(|row| {
                            let row = output.get_row(row);
                            (0..2)
                                .map(|col| (!row.is_null(col)).then(|| row.get_int64(col)))
                                .collect::<Vec<_>>()
                        }));
                        if mode == 5 {
                            break Ok(());
                        }
                    }
                };
                // Join the fetcher before reading shared observations. Mode 6
                // still exercises Drop without an explicit Close.
                let mut stage = Some(stage);
                if mode == 6 {
                    drop(stage.take());
                } else {
                    stage.as_mut().unwrap().close().unwrap();
                }
                let source = observations.lock().unwrap();
                match mode {
                    0 => {
                        result.unwrap();
                        let mut expected = Vec::new();
                        for batch in 0..13 {
                            expected.extend([vec![Some(2), Some(2)], vec![Some(4), Some(4)]]);
                            if batch % 2 == 0 {
                                expected.push(vec![Some(2), Some(2)]);
                            }
                            if kind == JoinType::LeftOuter && batch % 2 == 0 {
                                expected.push(vec![Some(9), None]);
                            }
                        }
                        if kind == JoinType::RightOuter {
                            expected.extend([
                                vec![None, Some(1)],
                                vec![None, Some(3)],
                                vec![None, Some(5)],
                            ]);
                        }
                        rows.sort();
                        expected.sort();
                        assert_eq!(rows, expected);
                        assert_eq!(source.calls, 14);
                        assert!(stage.as_ref().unwrap().is_finished());
                        if kind == JoinType::LeftOuter {
                            assert!(source.requested_rows.iter().all(|&rows| rows == 2));
                        }
                    }
                    1 | 8 => assert!(matches!(
                        result,
                        Err(ExecError::Eval(
                            tidb_expr::EvalError::ParamIndexExceedParamCounts
                        ))
                    )),
                    2 | 3 => assert!(
                        matches!(result, Err(ExecError::Internal(ref message)) if message.contains("panic")),
                        "mode={mode} concurrency={concurrency} kind={kind:?}: {result:?}"
                    ),
                    4 => assert!(
                        matches!(result, Err(ExecError::Killed(ref error)) if error.code == 1317)
                    ),
                    5 => {
                        result.unwrap();
                        assert!(source.calls < 14);
                    }
                    6 => assert_eq!(source.calls, 0),
                    7 => {
                        result.unwrap();
                        if kind == JoinType::LeftOuter {
                            let mut expected = Vec::new();
                            for batch in 0..13 {
                                for key in if batch % 2 == 0 {
                                    vec![2, 2, 4, 9]
                                } else {
                                    vec![4, 2]
                                } {
                                    expected.push(vec![Some(key), None]);
                                }
                            }
                            rows.sort();
                            expected.sort();
                            assert_eq!(rows, expected);
                            assert_eq!(source.calls, 14);
                        } else {
                            assert_eq!(source.calls, 0);
                            assert!(rows.is_empty());
                        }
                    }
                    _ => unreachable!(),
                }
                if mode != 6 {
                    stage.as_mut().unwrap().close().unwrap();
                    stage.as_mut().unwrap().close().unwrap();
                    assert!(stage.as_ref().unwrap().is_finished());
                }
                drop(stage);
                let (mut exec, _) = Arc::try_unwrap(fixture)
                    .ok()
                    .expect("Close joined every worker and dropped the factory");
                exec.release_build_memory();
                assert_eq!(memory.bytes_consumed(), 0);
                let _ = stop.send(());
                watchdog.join().unwrap();
            }
        }
    }
}
fn key_columns(code: FieldTypeCode) -> JoinKeyColumns {
    JoinKeyColumns {
        indices: vec![0],
        types: vec![FieldType::new(code)],
        modes: vec![SerializeMode::Normal],
    }
}

/// Go TestInnerJoinProbeOtherCondition: predicate-only columns, partial output,
/// empty projections and unequal input widths in both build orientations.
#[test]
fn inner_join_probe_other_condition_source() {
    for filtered in [false, true] {
        check_join_other_condition(JoinType::Inner, true, filtered);
    }
}

/// Go TestLeftOuterJoinProbeBasic/OtherCondition and right-outer counterparts.
#[test]
fn outer_join_probe_both_build_sides_source() {
    for join_type in [JoinType::LeftOuter, JoinType::RightOuter] {
        for residual in [false, true] {
            for filtered in [false, true] {
                check_join_other_condition(join_type, residual, filtered);
            }
        }
    }
}

fn check_join_other_condition(join_type: JoinType, residual: bool, filtered: bool) {
    use tidb_ast::CiString;
    use tidb_datatype::Datum;
    use tidb_exec::hash_join_v2::JoinOtherCondition;
    use tidb_expr::{
        column::Column,
        expression::{Expression, ScalarFunction},
        NoColumns,
    };
    let int = FieldType::new(FieldTypeCode::LongLong);
    let string = FieldType::new(FieldTypeCode::VarString);
    let left_types = vec![
        int.clone(),
        int.clone(),
        string.clone(),
        int.clone(),
        string.clone(),
    ];
    let right_types: Vec<_> = left_types.iter().chain(&left_types).cloned().collect();
    let all_types: Vec<_> = left_types.iter().chain(&right_types).cloned().collect();
    let make_input =
        |types: &[FieldType], side: &str, values: &[(Option<i64>, Option<i64>)], cmp: usize| {
            let mut chunk = Chunk::new_with_capacity(types, values.len());
            for (row, &(key, comparison)) in values.iter().enumerate() {
                for (column, field) in types.iter().enumerate() {
                    if column == 0 {
                        if let Some(key) = key {
                            chunk.append_int64(column, key);
                        } else {
                            chunk.append_null(column);
                        }
                    } else if column == cmp {
                        if let Some(value) = comparison {
                            chunk.append_int64(column, value);
                        } else {
                            chunk.append_null(column);
                        }
                    } else if field.code() == FieldTypeCode::VarString {
                        chunk.append_string(column, format!("{side}{row}c{column}").as_bytes());
                    } else {
                        chunk.append_int64(column, row as i64);
                    }
                }
            }
            chunk.set_sel(Some((0..values.len()).rev().collect()));
            chunk
        };
    let left_values: Vec<_> = [
        (Some(10001), Some(5)),
        (Some(1), Some(9)),
        (Some(2), Some(1)),
        (None, None),
    ]
    .into_iter()
    .cycle()
    .take(200)
    .collect();
    let right_values: Vec<_> = [
        (Some(10001), Some(2)),
        (Some(1), Some(8)),
        (Some(3), Some(1)),
        (None, None),
    ]
    .into_iter()
    .cycle()
    .take(200)
    .collect();
    let left = make_input(&left_types, "l", &left_values, 1);
    let right = make_input(&right_types, "r", &right_values, 3);
    let make_column = |index| {
        let mut column = Column::new(index as i64, int.clone());
        column.index = index as i64;
        Expression::Column(column)
    };
    let predicate = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("gt"),
        FieldType::new(FieldTypeCode::Tiny),
        vec![make_column(1), make_column(left_types.len() + 3)],
    ));
    // Go createSimpleFilter: column 0 > 10000, on the preserved outer
    // side (left for inner). Rejected preserved rows must still be emitted.
    let side_filter = tidb_exec::base_join_probe::JoinFilter::new(
        NoColumns,
        vec![Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("gt"),
            int.clone(),
            vec![
                make_column(0),
                Expression::Constant(tidb_expr::constant::Constant::new(
                    Datum::Int(10000),
                    int.clone(),
                )),
            ],
        ))],
        true,
    );
    let filter_right = join_type == JoinType::RightOuter;
    let mut expected = Vec::new();
    let mut right_matched = vec![false; right_values.len()];
    let left_result = |row: usize, value: Option<i64>, right_key: Option<i64>| {
        vec![
            value.map_or(Datum::Null, Datum::Int),
            Datum::new_string(format!("l{row}c2")),
            Datum::new_string(format!("l{row}c4")),
            right_key.map_or(Datum::Null, Datum::Int),
        ]
    };
    for (row, &(left_key, left_cmp)) in left_values.iter().enumerate() {
        let mut matched = false;
        for (right_row, &(right_key, right_cmp)) in right_values.iter().enumerate() {
            if left_key.is_some()
                && left_key == right_key
                && (!filtered
                    || if filter_right { right_key } else { left_key }
                        .is_some_and(|key| key > 10000))
                && (!residual
                    || left_cmp
                        .zip(right_cmp)
                        .is_some_and(|(left, right)| left > right))
            {
                matched = true;
                right_matched[right_row] = true;
                expected.push(left_result(row, left_cmp, right_key));
            }
        }
        if join_type == JoinType::LeftOuter && !matched {
            expected.push(left_result(row, left_cmp, None));
        }
    }
    if join_type == JoinType::RightOuter {
        for (row, &(key, _)) in right_values.iter().enumerate() {
            if !right_matched[row] {
                expected.push(vec![
                    Datum::Null,
                    Datum::Null,
                    Datum::Null,
                    key.map_or(Datum::Null, Datum::Int),
                ]);
            }
        }
    }
    fn result_key(row: &[Datum]) -> (Option<i64>, Option<&[u8]>, Option<i64>) {
        let int = |value: &Datum| match value {
            Datum::Int(value) => Some(*value),
            Datum::Null => None,
            _ => unreachable!(),
        };
        let string = match &row[1] {
            Datum::String(value) => Some(value.bytes()),
            Datum::Null => None,
            _ => unreachable!(),
        };
        (int(&row[0]), string, int(&row[3]))
    }
    let canonicalize =
        |rows: &mut Vec<Vec<Datum>>| rows.sort_by(|a, b| result_key(a).cmp(&result_key(b)));
    canonicalize(&mut expected);
    for right_as_build in [false, true] {
        let build_filtered = filtered && right_as_build == filter_right;
        let probe_filtered = filtered && !build_filtered;
        let outer_side_build = matches!(
            (join_type, right_as_build),
            (JoinType::LeftOuter, false) | (JoinType::RightOuter, true)
        );
        for empty_output in [false, true] {
            let l_used = if empty_output { vec![] } else { vec![1, 2, 4] };
            let r_used = if empty_output { vec![] } else { vec![0] };
            let (build_input, probe_input, build_types, build_used, condition_col) =
                if right_as_build {
                    (&right, &left, &right_types, &r_used, 3)
                } else {
                    (&left, &right, &left_types, &l_used, 1)
                };
            let categories: Vec<_> = build_types
                .iter()
                .map(|field| {
                    if field.code() == FieldTypeCode::VarString {
                        ColumnType::String
                    } else {
                        ColumnType::Int
                    }
                })
                .collect();
            let meta = JoinTableMeta::new(
                &[0],
                &categories,
                &[ColumnType::Int],
                &[ColumnType::Int],
                Some(&[condition_col]),
                Some(build_used),
                outer_side_build,
            );
            let sizes = meta
                .row_columns_order
                .iter()
                .map(|&index| {
                    usize::try_from(tidb_chunk::column::get_fixed_len(&build_types[index])).ok()
                })
                .collect();
            let layout = RowLayoutMeta::from_join_table_meta(&meta, sizes);
            let mut ctx = HashJoinCtxV2::new(4, join_type, right_as_build);
            ctx.has_build_filter = build_filtered;
            ctx.has_other_condition = residual;
            ctx.need_scan_row_table_after_probe_done = outer_side_build;
            let mut exec = HashJoinV2Exec::new(ctx, &[0], &vec![false; build_types.len()]);
            let keys = key_columns(FieldTypeCode::LongLong);
            let mut build = BuildContext::new(
                &layout,
                PartitionInfo::new(exec.ctx.partition_number),
                &keys,
            );
            build.build_filter = build_filtered.then_some(&side_filter);
            let mut source = BuildSource::new(vec![build_input.clone()]);
            let memory = StatementMemory::new(-1, OomAction::Cancel, 1);
            exec.fetch_and_build_hash_table(&mut source, &build, &memory)
                .unwrap();
            let output_types: Vec<_> = l_used
                .iter()
                .map(|&col| left_types[col].clone())
                .chain(r_used.iter().map(|&col| right_types[col].clone()))
                .collect();
            let probe_ctx = || ProbeContext {
                spill: None,
                hash_table: &exec.hash_table_context.hash_table,
                meta: &layout,
                column_count_needed_for_other_condition: meta
                    .column_count_needed_for_other_condition,
                total_column_number: build_types.len(),
                tag_helper: exec.hash_table_context.tag_helper,
                partition_number: exec.ctx.partition_number,
                partition_mask_offset: exec.ctx.partition_mask_offset,
                has_other_condition: residual,
                right_as_build_side: right_as_build,
                l_used: l_used.clone(),
                r_used: r_used.clone(),
                l_used_in_other_condition: vec![1],
                r_used_in_other_condition: vec![3],
                concurrency: 4,
                max_chunk_size: 128,
            };
            let make_condition = || {
                residual.then(|| {
                    JoinOtherCondition::new(
                        NoColumns,
                        vec![predicate.clone()],
                        &all_types,
                        128,
                        true,
                    )
                })
            };
            let make_probe =
                |worker, condition: Option<JoinOtherCondition<'static>>| -> Box<dyn ProbeV2 + '_> {
                    if join_type == JoinType::Inner {
                        Box::new(InnerJoinProbe::new(
                            probe_ctx(),
                            worker,
                            vec![0],
                            &[true],
                            right_as_build,
                            keys.clone(),
                            probe_filtered.then_some(&side_filter),
                            condition,
                        ))
                    } else {
                        Box::new(OuterJoinProbe::new(
                            probe_ctx(),
                            worker,
                            join_type,
                            vec![0],
                            &[true],
                            right_as_build,
                            keys.clone(),
                            probe_filtered.then_some(&side_filter),
                            condition,
                        ))
                    }
                };
            let mut probe = make_probe(0, make_condition());
            let mut outputs = run_join_worker(probe.as_mut(), vec![probe_input.clone()], &|| {
                Chunk::new(&output_types, 128, 128)
            })
            .unwrap();
            if probe.need_scan_row_table() {
                // Every worker scans its disjoint range after the shared probe barrier.
                for worker in 0..4 {
                    let mut scanner = make_probe(worker, make_condition());
                    outputs.extend(
                        scan_row_table_after_probe_done(scanner.as_mut(), &|| {
                            Chunk::new(&output_types, 128, 128)
                        })
                        .unwrap(),
                    );
                }
            }
            assert!(outputs
                .iter()
                .all(|chunk| chunk.num_rows() <= chunk.required_rows()));
            assert_eq!(
                outputs.iter().map(Chunk::num_rows).sum::<usize>(),
                expected.len()
            );
            if !empty_output {
                let mut rows: Vec<_> = outputs
                    .iter()
                    .flat_map(|chunk| {
                        (0..chunk.num_rows())
                            .map(|row| chunk.get_row(row).get_datum_row(&output_types))
                    })
                    .collect();
                canonicalize(&mut rows);
                assert_eq!(rows, expected);
            }

            assert_probe_canceled(
                make_probe(0, make_condition()).as_mut(),
                probe_input.clone(),
                &|| Chunk::new(&output_types, 128, 128),
            );
            if residual {
                // An evaluator failure is returned intact before any candidate rows
                // are copied into the caller's result chunk.
                let failure = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("getparam"),
                    int.clone(),
                    vec![Expression::Constant(tidb_expr::constant::Constant::new(
                        Datum::Int(-1),
                        int.clone(),
                    ))],
                ));
                let condition = JoinOtherCondition::new(
                    NoColumns,
                    vec![failure.clone()],
                    &all_types,
                    128,
                    true,
                );
                let mut failing_probe = make_probe(0, Some(condition));
                failing_probe
                    .set_chunk_for_probe(probe_input.clone())
                    .unwrap();
                let mut output = Chunk::new(&output_types, 128, 128);
                assert_eq!(
                    failing_probe.probe(&mut output, &tidb_util::sqlkiller::SqlKiller::default()),
                    Err(tidb_exec::base_join_probe::ProbeError::Expression(
                        tidb_expr::EvalError::ParamIndexExceedParamCounts
                    ))
                );
                assert_eq!(output.num_rows(), 0);
                let condition =
                    JoinOtherCondition::new(NoColumns, vec![failure], &all_types, 128, true);
                let mut failing_worker = make_probe(0, Some(condition));
                assert!(matches!(
                    run_join_worker(failing_worker.as_mut(), vec![probe_input.clone()], &|| {
                        Chunk::new(&output_types, 128, 128)
                    }),
                    Err(ExecError::Eval(
                        tidb_expr::EvalError::ParamIndexExceedParamCounts
                    ))
                ));
            }
        }
    }
}

/// Go TestInnerJoinProbeAllJoinKeys: all 18 key categories, composed keys,
/// nullable/non-nullable inputs and both build orientations. Deterministic
/// rows make duplicate, NULL and selected-row expectations independent of hashing.
#[test]
fn inner_join_probe_all_join_keys_source() {
    use tidb_chunk::column::get_fixed_len;
    use tidb_datatype::{
        BinaryJSON, BinaryJSONValue, BinaryLiteral, Collation, CoreTime, Datum, Decimal,
        FieldTypeFlags, MySqlDuration, MysqlEnum, MysqlSet, Time, TimeType,
    };
    let mut cases: Vec<(FieldType, ColumnType, Datum, Datum)> = Vec::new();
    for code in [FieldTypeCode::Tiny, FieldTypeCode::LongLong] {
        cases.push((
            FieldType::new(code),
            ColumnType::Int,
            Datum::Int(-2),
            Datum::Int(5),
        ));
    }
    cases.push((
        FieldType::new(FieldTypeCode::LongLong).with_flags(FieldTypeFlags::UNSIGNED),
        ColumnType::UnsignedInt,
        Datum::UInt(u64::MAX),
        Datum::UInt(5),
    ));
    cases.push((
        FieldType::new(FieldTypeCode::Year),
        ColumnType::Year,
        Datum::Int(2020),
        Datum::Int(2021),
    ));
    cases.push((
        FieldType::new(FieldTypeCode::Duration),
        ColumnType::Duration,
        Datum::new_duration(MySqlDuration::from_nanoseconds(-1000000000, 0).unwrap()),
        Datum::new_duration(MySqlDuration::from_nanoseconds(2000000000, 0).unwrap()),
    ));
    for as_int in [false, true] {
        let mut field = FieldType::new(FieldTypeCode::Enum).with_elems(["A", "B"]);
        if as_int {
            field.add_flags(FieldTypeFlags::ENUM_SET_AS_INT);
        }
        cases.push((
            field,
            if as_int {
                ColumnType::EnumInt
            } else {
                ColumnType::Enum
            },
            Datum::new_enum(MysqlEnum::new("A", 1), Collation::Binary),
            Datum::new_enum(MysqlEnum::new("B", 2), Collation::Binary),
        ));
    }
    cases.push((
        FieldType::new(FieldTypeCode::Set).with_elems(["A", "B"]),
        ColumnType::Set,
        Datum::new_set(MysqlSet::new("A", 1), Collation::Binary),
        Datum::new_set(MysqlSet::new("B", 2), Collation::Binary),
    ));
    cases.push((
        FieldType::new(FieldTypeCode::Bit),
        ColumnType::Bit,
        Datum::new_mysql_bit(BinaryLiteral::from(vec![1])),
        Datum::new_mysql_bit(BinaryLiteral::from(vec![2])),
    ));
    cases.push((
        FieldType::new(FieldTypeCode::Json),
        ColumnType::Json,
        Datum::new_json(
            BinaryJSON::from_typed_value(&BinaryJSONValue::String("A".into())).unwrap(),
        ),
        Datum::new_json(
            BinaryJSON::from_typed_value(&BinaryJSONValue::String("B".into())).unwrap(),
        ),
    ));
    cases.push((
        FieldType::new(FieldTypeCode::Float),
        ColumnType::Float,
        Datum::Float32(-0.0),
        Datum::Float32(1.25),
    ));
    cases.push((
        FieldType::new(FieldTypeCode::Double),
        ColumnType::Float,
        Datum::Real(-0.0),
        Datum::Real(1.25),
    ));
    cases.push((
        FieldType::new(FieldTypeCode::VarString).with_collation(Collation::Utf8Mb4GeneralCi),
        ColumnType::String,
        Datum::new_string("A "),
        Datum::new_string("B"),
    ));
    let time_value = |kind, day| {
        Datum::new_time(Time::new(CoreTime::from_date(2026, 9, day, 0, 0, 0, 0), kind, 0).unwrap())
    };
    cases.push((
        FieldType::new(FieldTypeCode::Datetime),
        ColumnType::DateTime,
        time_value(TimeType::DateTime, 1),
        time_value(TimeType::DateTime, 2),
    ));
    cases.push((
        FieldType::new(FieldTypeCode::NewDecimal),
        ColumnType::Decimal,
        Datum::new_decimal(Decimal::from_signed_literal("1.20")),
        Datum::new_decimal(Decimal::from_signed_literal("2.50")),
    ));
    cases.push((
        FieldType::new(FieldTypeCode::Timestamp),
        ColumnType::DateTime,
        time_value(TimeType::Timestamp, 1),
        time_value(TimeType::Timestamp, 2),
    ));
    cases.push((
        FieldType::new(FieldTypeCode::Date),
        ColumnType::DateTime,
        time_value(TimeType::Date, 1),
        time_value(TimeType::Date, 2),
    ));
    cases.push((
        FieldType::new(FieldTypeCode::Blob).with_collation(Collation::Binary),
        ColumnType::BinaryString,
        Datum::new_bytes(vec![0xff, 0]),
        Datum::new_bytes(vec![0xfe, 0]),
    ));
    assert_eq!(cases.len(), 18);

    let mut fields: Vec<_> = cases.iter().map(|case| case.0.clone()).collect();
    fields.push(FieldType::new(FieldTypeCode::LongLong));
    let mut categories: Vec<_> = cases.iter().map(|case| case.1).collect();
    categories.push(ColumnType::Int);
    let id = cases.len();
    let mut key_sets: Vec<Vec<usize>> = (0..id).map(|index| vec![index]).collect();
    key_sets.extend([vec![1, 2], vec![12, 1], vec![1, 12], (0..id).collect()]);
    for nullable in [false, true] {
        let mut input = Chunk::new_with_capacity(&fields, 4);
        for row in 0..4 {
            for (column, (_, _, a, b)) in cases.iter().enumerate() {
                if row == 3 {
                    input.append_null(column);
                } else if row == 2 && column == 10 {
                    input.append_float32(column, 0.0);
                } else if row == 2 && column == 11 {
                    input.append_float64(column, 0.0);
                } else {
                    input.append_datum(column, if row == 1 { b } else { a });
                }
            }
            input.append_int64(id, row as i64);
        }
        input.set_sel(Some(if nullable {
            vec![2, 1, 0, 3]
        } else {
            vec![2, 1, 0]
        }));
        for indices in &key_sets {
            let key_types: Vec<_> = indices.iter().map(|&index| categories[index]).collect();
            let meta = JoinTableMeta::new(
                indices,
                &categories,
                &key_types,
                &key_types,
                None,
                Some(&[id]),
                false,
            );
            let sizes = meta
                .row_columns_order
                .iter()
                .map(|&index| usize::try_from(get_fixed_len(&fields[index])).ok())
                .collect();
            let layout = RowLayoutMeta::from_join_table_meta(&meta, sizes);
            let columns = JoinKeyColumns {
                indices: indices.clone(),
                types: indices.iter().map(|&index| fields[index].clone()).collect(),
                modes: meta.serialize_modes.clone(),
            };
            // Compare native bytes to the established codec for every key shape,
            // then test actual build/probe results against explicit row IDs.
            let rows: Vec<_> = (0..input.num_rows())
                .map(|row| input.get_row(row).get_datum_row(&fields))
                .collect();
            let (expected, _) =
                tidb_codec::serialize_keys(&rows, indices, &columns.types, &columns.modes, None)
                    .unwrap();
            let mut keys = tidb_codec::SerializedJoinKeys::default();
            let mut nulls = Vec::new();
            columns
                .serialize(&input, input.sel().unwrap(), None, &mut nulls, &mut keys)
                .unwrap();
            assert_eq!(
                keys.iter().collect::<Vec<_>>(),
                expected.iter().map(Vec::as_slice).collect::<Vec<_>>(),
                "{indices:?}"
            );
            let retained = keys.memory_usage();
            for mode in [
                SerializeMode::Normal,
                SerializeMode::NeedSignFlag,
                SerializeMode::KeepVarColumnLength,
            ] {
                let mut framed = columns.clone();
                framed.modes.fill(mode);
                let (reference, _) =
                    tidb_codec::serialize_keys(&rows, indices, &framed.types, &framed.modes, None)
                        .unwrap();
                let mut framed_keys = tidb_codec::SerializedJoinKeys::default();
                framed
                    .serialize(
                        &input,
                        input.sel().unwrap(),
                        None,
                        &mut nulls,
                        &mut framed_keys,
                    )
                    .unwrap();
                assert_eq!(
                    framed_keys.iter().collect::<Vec<_>>(),
                    reference.iter().map(Vec::as_slice).collect::<Vec<_>>(),
                    "keys={indices:?}, mode={mode:?}"
                );
            }
            columns
                .serialize(
                    &input,
                    &input.sel().unwrap()[..1],
                    None,
                    &mut nulls,
                    &mut keys,
                )
                .unwrap();
            assert_eq!(keys.memory_usage(), retained);
            columns
                .serialize(&input, input.sel().unwrap(), None, &mut nulls, &mut keys)
                .unwrap();

            for right_as_build in [false, true] {
                let ctx = HashJoinCtxV2::new(4, JoinType::Inner, right_as_build);
                let nullable_keys = vec![nullable; indices.len()];
                let mut exec = HashJoinV2Exec::new(ctx, indices, &vec![!nullable; fields.len()]);
                let mut build = BuildContext::new(
                    &layout,
                    PartitionInfo::new(exec.ctx.partition_number),
                    &columns,
                );
                exec.begin_build(layout.null_map_length);
                exec.append_build_chunk(0, &input, &mut build).unwrap();
                exec.finish_build();
                let probe_ctx = ProbeContext {
                    spill: None,
                    hash_table: &exec.hash_table_context.hash_table,
                    meta: &layout,
                    column_count_needed_for_other_condition: 0,
                    total_column_number: fields.len(),
                    tag_helper: exec.hash_table_context.tag_helper,
                    partition_number: exec.ctx.partition_number,
                    partition_mask_offset: exec.ctx.partition_mask_offset,
                    has_other_condition: false,
                    right_as_build_side: right_as_build,
                    l_used: vec![id],
                    r_used: vec![id],
                    l_used_in_other_condition: vec![],
                    r_used_in_other_condition: vec![],
                    concurrency: 4,
                    max_chunk_size: 2,
                };
                let mut probe = InnerJoinProbe::new(
                    probe_ctx,
                    0,
                    indices.clone(),
                    &nullable_keys,
                    right_as_build,
                    columns.clone(),
                    None,
                    None,
                );
                let outputs = run_join_worker(&mut probe, vec![input.clone()], &|| {
                    Chunk::new(&[fields[id].clone(), fields[id].clone()], 2, 2)
                })
                .unwrap();
                let mut pairs: Vec<_> = outputs
                    .iter()
                    .flat_map(|chunk| {
                        (0..chunk.num_rows()).map(|row| {
                            let row = chunk.get_row(row);
                            (row.get_int64(0), row.get_int64(1))
                        })
                    })
                    .collect();
                pairs.sort_unstable();
                assert_eq!(
                    pairs,
                    vec![(0, 0), (0, 2), (1, 1), (2, 0), (2, 2)],
                    "keys={indices:?}, nullable={nullable}, right_build={right_as_build}"
                );
                // The owning executor derives row layout and serializer modes
                // from native field types instead of the fixture categories.
                use tidb_exec::hash_join_v2::executor::{HashJoinV2Executor, HashJoinV2Plan};
                let source = || {
                    let mut source = BuildSource::new(vec![input.clone()]);
                    source.meta = ExecutorMeta::new(
                        tidb_expr::schema::Schema::new(
                            fields
                                .iter()
                                .enumerate()
                                .map(|(index, field)| {
                                    tidb_expr::column::Column::new(index as i64, field.clone())
                                })
                                .collect(),
                        ),
                        1,
                        2,
                        2,
                    );
                    Box::new(source) as Box<dyn Executor>
                };
                let plan = HashJoinV2Plan {
                    concurrency: 4,
                    join_type: JoinType::Inner,
                    right_as_build_side: right_as_build,
                    build_key_indices: indices.clone(),
                    probe_key_indices: indices.clone(),
                    build_key_types: columns.types.clone(),
                    probe_key_types: columns.types.clone(),
                    l_used: vec![id],
                    r_used: vec![id],
                    l_used_in_other_condition: vec![],
                    r_used_in_other_condition: vec![],
                    build_filter: vec![],
                    probe_filter: vec![],
                    other_condition: vec![],
                    vectorized: true,
                };
                let output_meta = ExecutorMeta::new(
                    tidb_expr::schema::Schema::new(
                        (0..2)
                            .map(|index| tidb_expr::column::Column::new(index, fields[id].clone()))
                            .collect(),
                    ),
                    2,
                    2,
                    2,
                );
                let memory = StatementMemory::new(-1, OomAction::Cancel, 1);
                let mut native = HashJoinV2Executor::new(
                    output_meta,
                    plan,
                    source(),
                    source(),
                    tidb_expr::NoColumns,
                    memory.clone(),
                );
                native.open().unwrap();
                let mut output = native.new_chunk();
                let mut native_pairs = Vec::new();
                loop {
                    native.next(&mut output).unwrap();
                    if output.num_rows() == 0 {
                        break;
                    }
                    native_pairs.extend((0..output.num_rows()).map(|row| {
                        let row = output.get_row(row);
                        (row.get_int64(0), row.get_int64(1))
                    }));
                }
                native.close().unwrap();
                assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
                native_pairs.sort_unstable();
                assert_eq!(
                    native_pairs, pairs,
                    "native field metadata keys={indices:?} right_build={right_as_build}"
                );
            }
        }
    }
}
