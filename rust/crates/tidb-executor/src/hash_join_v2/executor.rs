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

//! Owning Open/Next/Close lifecycle for native V2 build, probe and recursive
//! spill restore. SQL builder selection remains separate integration work.

use std::sync::{Arc, Mutex};

use tidb_chunk::{chunk::Chunk, column::get_fixed_len};
use tidb_codec::JoinKeyColumns;
use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::{expression::Expression, schema::Schema, Columns};

use super::probe_stage::ProbeStage;
use super::spill::{HashJoinSpill, PartitionFiles, SPILL_CHUNK_SIZE};
use super::{
    AntiLeftOuterSemiJoinProbe, AntiSemiJoinProbe, HashJoinCtxV2, HashJoinV2Exec, InnerJoinProbe,
    JoinOtherCondition, LeftOuterSemiJoinProbe, OuterJoinProbe, ProbeV2, SemiJoinProbe,
};
use crate::base_join_probe::{JoinFilter, ProbeContext};
use crate::join_row_table::RowLayoutMeta;
use crate::join_table_meta::{ColumnType, JoinTableMeta};
use crate::joiner::JoinType;
use crate::row_table_builder::{BuildContext, PartitionInfo};
use crate::sort_util::recover_worker_panic;
use crate::{ExecError, Executor, ExecutorMeta, StatementMemory};

/// Go builder's resolved child indices, comparison types, projection and ON
/// conditions. Key types include the equality expression's collation and flags;
/// they must not be replaced by the unadjusted child types.
pub struct HashJoinV2Plan {
    pub concurrency: usize,
    pub join_type: JoinType,
    pub right_as_build_side: bool,
    pub build_key_indices: Vec<usize>,
    pub probe_key_indices: Vec<usize>,
    pub build_key_types: Vec<FieldType>,
    pub probe_key_types: Vec<FieldType>,
    pub l_used: Vec<usize>,
    pub r_used: Vec<usize>,
    pub l_used_in_other_condition: Vec<usize>,
    pub r_used_in_other_condition: Vec<usize>,
    pub build_filter: Vec<Expression>,
    pub probe_filter: Vec<Expression>,
    pub other_condition: Vec<Expression>,
    pub vectorized: bool,
}

struct Runtime {
    build: HashJoinV2Exec,
    table_meta: JoinTableMeta,
    layout: RowLayoutMeta,
    build_keys: JoinKeyColumns,
    probe_keys: JoinKeyColumns,
    joined_types: Vec<FieldType>,
    spill: HashJoinSpill,
    restored: Vec<Mutex<PartitionFiles>>,
    round: usize,
}

/// The build coordinator exclusively owns the build child while the caller
/// fetches the first probe chunk. The probe stage moves the probe child into a
/// Go-equivalent fetcher after that first chunk and returns it before Close;
/// probe workers share only the completed table.
pub struct HashJoinV2Executor<C> {
    meta: ExecutorMeta,
    plan: Arc<HashJoinV2Plan>,
    children: [Option<Box<dyn Executor>>; 2],
    context: C,
    memory: StatementMemory,
    runtime: Option<Arc<Runtime>>,
    probe: Option<ProbeStage>,
    children_open: bool,
    finished: bool,
}

impl<C> HashJoinV2Executor<C> {
    pub fn new(
        meta: ExecutorMeta,
        plan: HashJoinV2Plan,
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        context: C,
        memory: StatementMemory,
    ) -> Self {
        Self {
            meta,
            plan: Arc::new(plan),
            children: [Some(left), Some(right)],
            context,
            memory,
            runtime: None,
            probe: None,
            children_open: false,
            finished: false,
        }
    }

    fn close_owned(&mut self) -> Result<(), ExecError> {
        self.finished = true;
        let probe_index = usize::from(!self.plan.right_as_build_side);
        let mut first_error = None;
        if let Some(mut stage) = self.probe.take() {
            if let Err(error) = stage.close() {
                first_error = Some(error);
            }
            if let Some(source) = stage.take_source() {
                debug_assert!(self.children[probe_index].is_none());
                self.children[probe_index] = Some(source);
            }
        }
        if let Some(runtime) = self.runtime.take() {
            // The only shared owners are the workers, all joined above.
            let mut runtime = Arc::try_unwrap(runtime)
                .unwrap_or_else(|_| panic!("V2 worker retained build state after Close"));
            runtime.build.release_build_memory();
            runtime.restored.clear();
            runtime.spill.close();
            runtime.build.hash_table_context.memory_tracker.detach();
        }
        if std::mem::take(&mut self.children_open) {
            // BaseExecutor.Close visits both children and returns the first
            // error, including after a partially successful Open.
            for child in self.children.iter_mut().flatten() {
                if let Err(error) = child.close() {
                    first_error.get_or_insert(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn open_self(&mut self) -> Result<(), ExecError> {
        let plan = &self.plan;
        let right_build = plan.right_as_build_side;
        if plan.concurrency == 0 || plan.build_key_indices.is_empty() {
            return Err(ExecError::internal("V2 requires workers and equality keys"));
        }
        if matches!(
            plan.join_type,
            JoinType::LeftOuterSemiJoin | JoinType::AntiLeftOuterSemiJoin
        ) && !right_build
        {
            return Err(ExecError::internal(
                "Go outer-semi V2 requires a right build child",
            ));
        }
        if plan.build_key_indices.len() != plan.probe_key_indices.len()
            || plan.build_key_indices.len() != plan.build_key_types.len()
            || plan.build_key_indices.len() != plan.probe_key_types.len()
        {
            return Err(ExecError::internal(
                "V2 key indices and comparison types differ",
            ));
        }
        let build_types = self.children[usize::from(right_build)]
            .as_ref()
            .expect("build child missing before Open")
            .ret_field_types();
        let categories = field_categories(build_types)?;
        let build_key_types = field_categories(&plan.build_key_types)?;
        let probe_key_types = field_categories(&plan.probe_key_types)?;
        let (used, other) = if right_build {
            (&plan.r_used, &plan.r_used_in_other_condition)
        } else {
            (&plan.l_used, &plan.l_used_in_other_condition)
        };
        let need_scan = match plan.join_type {
            JoinType::Inner | JoinType::LeftOuterSemiJoin | JoinType::AntiLeftOuterSemiJoin => {
                false
            }
            JoinType::LeftOuter | JoinType::SemiJoin | JoinType::AntiSemiJoin => !right_build,
            JoinType::RightOuter => right_build,
        };
        let mut table_meta = JoinTableMeta::new(
            &plan.build_key_indices,
            &categories,
            &build_key_types,
            &probe_key_types,
            (!plan.other_condition.is_empty()).then_some(other.as_slice()),
            Some(used),
            need_scan,
        );
        let widths: Vec<_> = table_meta
            .row_columns_order
            .iter()
            .map(|&column| usize::try_from(get_fixed_len(&build_types[column])).ok())
            .collect();
        // Native Float has four-byte row storage but an eight-byte encoded key.
        // Use the chunk ABI, not the abstract category's width, for saved rows.
        table_meta.is_fixed_length = widths.iter().all(Option::is_some);
        table_meta.row_length = if table_meta.is_fixed_length {
            widths.iter().flatten().sum()
        } else {
            0
        };
        let layout = RowLayoutMeta::from_join_table_meta(&table_meta, widths);
        let keys = |indices: &Vec<usize>, types: &Vec<FieldType>| JoinKeyColumns {
            indices: indices.clone(),
            types: types.clone(),
            modes: table_meta.serialize_modes.clone(),
        };
        let build_keys = keys(&plan.build_key_indices, &plan.build_key_types);
        let probe_keys = keys(&plan.probe_key_indices, &plan.probe_key_types);
        let not_null: Vec<_> = build_types
            .iter()
            .map(|field| field.flags() & FieldTypeFlags::NOT_NULL != 0)
            .collect();
        let mut ctx = HashJoinCtxV2::new(plan.concurrency, plan.join_type, right_build);
        ctx.has_build_filter = !plan.build_filter.is_empty();
        ctx.has_other_condition = !plan.other_condition.is_empty();
        ctx.need_scan_row_table_after_probe_done = need_scan;
        let mut build = HashJoinV2Exec::new(ctx, &plan.build_key_indices, &not_null);
        if self.memory.tmp_storage_on_oom() && build.ctx.partition_number > 1 {
            build.init_max_spill_round();
        }
        build
            .hash_table_context
            .memory_tracker
            .attach_to(self.memory.stmt_tracker());
        let spill = HashJoinSpill::new(
            plan.concurrency,
            build.ctx.partition_number,
            self.children[usize::from(!right_build)]
                .as_ref()
                .expect("probe child missing before Open")
                .ret_field_types(),
            build.hash_table_context.memory_tracker.clone(),
            self.memory.clone(),
            self.meta.id(),
        );
        spill.register();
        self.runtime = Some(Arc::new(Runtime {
            spill,
            restored: Vec::new(),
            round: 0,
            build,
            table_meta,
            layout,
            build_keys,
            probe_keys,
            joined_types: self
                .children
                .iter()
                .filter_map(|child| child.as_ref())
                .flat_map(|child| child.ret_field_types().iter().cloned())
                .collect(),
        }));
        self.finished = false;
        Ok(())
    }
}

impl<C: Columns + Clone + Send + Sync + 'static> HashJoinV2Executor<C> {
    fn prepare(&mut self, required_rows: usize) -> Result<(), ExecError> {
        let plan = &self.plan;
        let build_index = usize::from(plan.right_as_build_side);
        let runtime = Arc::get_mut(self.runtime.as_mut().expect("opened V2 runtime"))
            .expect("build starts before sharing the table");
        let filter = (!plan.build_filter.is_empty()).then(|| {
            JoinFilter::new(
                self.context.clone(),
                plan.build_filter.clone(),
                plan.vectorized,
            )
        });
        let mut build_context = BuildContext::new(
            &runtime.layout,
            PartitionInfo::new(plan.concurrency),
            &runtime.build_keys,
        );
        build_context.build_filter = filter.as_ref();
        let first_probe = if runtime.restored.is_empty() {
            let [left, right] = &mut self.children;
            let (build, probe) = if plan.right_as_build_side {
                (
                    right
                        .as_deref_mut()
                        .expect("build child missing before prepare"),
                    left.as_deref_mut()
                        .expect("probe child missing before prepare"),
                )
            } else {
                (
                    left.as_deref_mut()
                        .expect("build child missing before prepare"),
                    right
                        .as_deref_mut()
                        .expect("probe child missing before prepare"),
                )
            };
            let mut first_probe = probe.new_chunk();
            if ProbeStage::should_limit_fetch_size(plan.join_type, plan.right_as_build_side) {
                first_probe.set_required_rows(required_rows as isize, probe.max_chunk_size());
            }
            let memory = &self.memory;
            // Go starts fetchAndBuildHashTable independently, and its probe
            // fetcher calls Next BEFORE wait4BuildSide. Keep exactly that one
            // chunk (including EOF), and never probe an unfinished table.
            std::thread::scope(|scope| -> Result<(), ExecError> {
                let builder = std::thread::Builder::new()
                    .name("hash-join-build".into())
                    .spawn_scoped(scope, || {
                        recover_worker_panic(|| {
                            runtime.build.fetch_and_build_hash_table_with_spill(
                                build,
                                &build_context,
                                memory,
                                &mut runtime.spill,
                            )
                        })
                    })
                    .map_err(|error| {
                        ExecError::internal(format!("start hash join build: {error}"))
                    })?;
                let probe_result = recover_worker_panic(|| probe.next(&mut first_probe));
                let build_result = builder.join().map_err(|panic| {
                    ExecError::internal(format!("hash join build panic: {panic:?}"))
                });
                // Fetch errors precede wait4BuildSide in Go. Join ownership
                // even on failure before returning either error to the parent.
                probe_result?;
                build_result??;
                memory.check()
            })?;
            Some(first_probe)
        } else {
            let fields = runtime.spill.build_field_types().to_vec();
            let files = runtime
                .restored
                .iter_mut()
                .map(|lane| &mut lane.get_mut().unwrap().build)
                .collect();
            runtime.build.fetch_and_build_restored_hash_table(
                files,
                &fields,
                &build_context,
                &self.memory,
                &mut runtime.spill,
            )?;
            None
        };

        let runtime = self.runtime.as_ref().expect("built V2 runtime");
        let shared = Arc::clone(runtime);
        let plan = Arc::clone(plan);
        let context = self.context.clone();
        let max_chunk_size = self.meta.max_chunk_size();
        self.probe = Some(ProbeStage::new_with_spill(
            &mut self.children[1 - build_index],
            &self.meta,
            &runtime.build,
            self.memory.clone(),
            runtime.spill.spilled_partitions().iter().any(|&part| part),
            !runtime.restored.is_empty(),
            first_probe,
            move |worker_id, worker, memory| {
                let runtime = &*shared;
                let build = &runtime.build;
                let ctx = ProbeContext {
                    spill: Some(&runtime.spill),
                    hash_table: &build.hash_table_context.hash_table,
                    meta: &runtime.layout,
                    column_count_needed_for_other_condition: runtime
                        .table_meta
                        .column_count_needed_for_other_condition,
                    total_column_number: runtime.table_meta.total_column_number,
                    tag_helper: build.hash_table_context.tag_helper,
                    partition_number: build.ctx.partition_number,
                    partition_mask_offset: build.ctx.partition_mask_offset,
                    has_other_condition: build.ctx.has_other_condition,
                    right_as_build_side: plan.right_as_build_side,
                    l_used: plan.l_used.clone(),
                    r_used: plan.r_used.clone(),
                    l_used_in_other_condition: plan.l_used_in_other_condition.clone(),
                    r_used_in_other_condition: plan.r_used_in_other_condition.clone(),
                    concurrency: plan.concurrency,
                    max_chunk_size,
                };
                let filter = (!plan.probe_filter.is_empty()).then(|| {
                    JoinFilter::new(context.clone(), plan.probe_filter.clone(), plan.vectorized)
                });
                let condition = (!plan.other_condition.is_empty()).then(|| {
                    JoinOtherCondition::new(
                        context.clone(),
                        plan.other_condition.clone(),
                        &runtime.joined_types,
                        max_chunk_size,
                        plan.vectorized,
                    )
                });
                let nullable: Vec<_> = plan
                    .probe_key_types
                    .iter()
                    .map(|field| field.flags() & FieldTypeFlags::NOT_NULL == 0)
                    .collect();
                macro_rules! probe {
                    ($kind:ident) => {
                        Box::new($kind::new(
                            ctx,
                            worker_id,
                            plan.probe_key_indices.clone(),
                            &nullable,
                            plan.right_as_build_side,
                            runtime.probe_keys.clone(),
                            filter.as_ref(),
                            condition,
                        ))
                    };
                }
                let mut probe: Box<dyn ProbeV2> = match plan.join_type {
                    JoinType::Inner => probe!(InnerJoinProbe),
                    JoinType::LeftOuter | JoinType::RightOuter => Box::new(OuterJoinProbe::new(
                        ctx,
                        worker_id,
                        plan.join_type,
                        plan.probe_key_indices.clone(),
                        &nullable,
                        plan.right_as_build_side,
                        runtime.probe_keys.clone(),
                        filter.as_ref(),
                        condition,
                    )),
                    JoinType::SemiJoin => probe!(SemiJoinProbe),
                    JoinType::AntiSemiJoin => probe!(AntiSemiJoinProbe),
                    JoinType::LeftOuterSemiJoin => probe!(LeftOuterSemiJoinProbe),
                    JoinType::AntiLeftOuterSemiJoin => probe!(AntiLeftOuterSemiJoinProbe),
                };
                if runtime.restored.is_empty() {
                    worker.run(probe.as_mut(), memory);
                } else {
                    let mut lane = runtime.restored[worker_id].lock().unwrap();
                    let chunk = Chunk::new_with_capacity(
                        runtime.spill.probe_field_types(),
                        SPILL_CHUNK_SIZE,
                    );
                    worker.run_restored(probe.as_mut(), memory, &mut lane.probe, chunk);
                }
            },
        )?);
        Ok(())
    }

    fn next_restore_partition(&mut self) -> Result<bool, ExecError> {
        if let Some(mut stage) = self.probe.take() {
            let close_result = stage.close();
            if let Some(source) = stage.take_source() {
                let probe_index = usize::from(!self.plan.right_as_build_side);
                debug_assert!(self.children[probe_index].is_none());
                self.children[probe_index] = Some(source);
            }
            close_result?;
        }
        let runtime = Arc::get_mut(self.runtime.as_mut().expect("opened V2 runtime"))
            .expect("round workers joined before restore");
        runtime.build.release_build_memory();
        runtime.restored.clear();
        runtime
            .spill
            .prepare_for_restoring(runtime.round, runtime.build.ctx.max_spill_round)?;
        let Some(partition) = runtime.spill.pop_restore() else {
            return Ok(false);
        };
        runtime.round = partition.round;
        runtime.restored = partition.files.into_iter().map(Mutex::new).collect();
        Ok(true)
    }
}

impl<C: Columns + Clone + Send + Sync + 'static> Executor for HashJoinV2Executor<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.close_owned()?;
        self.children_open = true;
        for child in self.children.iter_mut().flatten() {
            child.open()?;
        }
        recover_worker_panic(|| self.open_self())
    }

    fn next(&mut self, output: &mut Chunk) -> Result<(), ExecError> {
        output.reset();
        if self.finished {
            return Ok(());
        }
        if self.runtime.is_none() {
            return Err(ExecError::internal("V2 Next before Open"));
        }
        let result = recover_worker_panic(|| {
            self.memory.check()?;
            loop {
                if self.probe.is_none() {
                    self.prepare(output.required_rows())?;
                }
                self.probe
                    .as_mut()
                    .expect("prepared V2 probe")
                    .next(output)?;
                if output.num_rows() != 0 {
                    return Ok(());
                }
                if !self.next_restore_partition()? {
                    self.finished = true;
                    return Ok(());
                }
            }
        });
        if result.is_err() {
            self.finished = true;
            if let Some(mut stage) = self.probe.take() {
                let _ = stage.close();
                if let Some(source) = stage.take_source() {
                    let probe_index = usize::from(!self.plan.right_as_build_side);
                    debug_assert!(self.children[probe_index].is_none());
                    self.children[probe_index] = Some(source);
                }
            }
        }
        result
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.close_owned()
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

impl<C> Drop for HashJoinV2Executor<C> {
    fn drop(&mut self) {
        let _ = self.close_owned();
    }
}

fn field_categories(fields: &[FieldType]) -> Result<Vec<ColumnType>, ExecError> {
    fields
        .iter()
        .map(|field| {
            Ok(match field.code() {
                FieldTypeCode::Tiny
                | FieldTypeCode::Short
                | FieldTypeCode::Int24
                | FieldTypeCode::Long
                | FieldTypeCode::LongLong => {
                    if field.flags() & FieldTypeFlags::UNSIGNED != 0 {
                        ColumnType::UnsignedInt
                    } else {
                        ColumnType::Int
                    }
                }
                FieldTypeCode::Year => ColumnType::Year,
                FieldTypeCode::Duration => ColumnType::Duration,
                FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
                    ColumnType::DateTime
                }
                FieldTypeCode::Float | FieldTypeCode::Double => ColumnType::Float,
                FieldTypeCode::Varchar
                | FieldTypeCode::VarString
                | FieldTypeCode::String
                | FieldTypeCode::Blob
                | FieldTypeCode::TinyBlob
                | FieldTypeCode::MediumBlob
                | FieldTypeCode::LongBlob => {
                    if field.runtime_collator().can_use_raw_mem_as_key() {
                        ColumnType::BinaryString
                    } else {
                        ColumnType::String
                    }
                }
                FieldTypeCode::NewDecimal => ColumnType::Decimal,
                FieldTypeCode::Enum => {
                    if field.flags() & FieldTypeFlags::ENUM_SET_AS_INT != 0 {
                        ColumnType::EnumInt
                    } else {
                        ColumnType::Enum
                    }
                }
                FieldTypeCode::Set => ColumnType::Set,
                FieldTypeCode::Json => ColumnType::Json,
                FieldTypeCode::Bit => ColumnType::Bit,
                FieldTypeCode::Null => ColumnType::Null,
                other => {
                    return Err(ExecError::unsupported(format!(
                        "V2 native row metadata for {other:?}"
                    )))
                }
            })
        })
        .collect()
}
