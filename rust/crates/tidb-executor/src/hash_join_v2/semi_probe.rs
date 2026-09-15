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

//! Go base_semi_join.go and the semi, anti-semi, and outer-semi probes.
//! Shared chunk/queue state; join kind changes only which verdict is emitted.

use std::collections::VecDeque;

use super::*;
use tidb_chunk::chunk_util::copy_rows;

// Go baseSemiJoin.maxMatchedRowNum: round-robin visits bound each probe row's
// contribution, not the total number of candidates it may eventually examine.
const MAX_MATCHED_ROW_NUM: usize = 4;

struct BaseSemiJoin<'a> {
    base: BaseJoinProbe,
    ctx: ProbeContext<'a>,
    keys: JoinKeyColumns,
    filter: Option<ProbeFilter<'a>>,
    condition: Option<JoinOtherCondition<'a>>,
    left_build: bool,
    anti: bool,
    outer_semi: bool,
    matched: Vec<bool>,
    null_rows: Vec<bool>,
    nulls: Vec<bool>,
    unfinished: VecDeque<usize>,
    offsets: Vec<usize>,
    row_iter: Option<RowIter<'a>>,
}

impl<'a> BaseSemiJoin<'a> {
    fn new(
        ctx: ProbeContext<'a>,
        worker: usize,
        join_type: JoinType,
        key_index: Vec<usize>,
        nullable: &[bool],
        right_build: bool,
        keys: JoinKeyColumns,
        filter: Option<ProbeFilter<'a>>,
        condition: Option<JoinOtherCondition<'a>>,
    ) -> Self {
        let outer_semi = matches!(
            join_type,
            JoinType::LeftOuterSemiJoin | JoinType::AntiLeftOuterSemiJoin
        );
        assert!(
            !outer_semi || right_build,
            "outer-semi requires right build, as in Go"
        );
        assert_eq!(ctx.has_other_condition, condition.is_some());
        Self {
            base: new_join_probe(&ctx, worker, join_type, key_index, nullable, right_build),
            ctx,
            keys,
            filter,
            condition,
            left_build: !right_build,
            outer_semi,
            anti: matches!(
                join_type,
                JoinType::AntiSemiJoin | JoinType::AntiLeftOuterSemiJoin
            ),
            matched: Vec::new(),
            null_rows: Vec::new(),
            nulls: Vec::new(),
            unfinished: VecDeque::new(),
            offsets: Vec::new(),
            row_iter: None,
        }
    }

    fn set_chunk_for_probe(&mut self, chunk: Chunk) -> Result<(), ProbeError> {
        self.base
            .set_chunk_for_probe(&self.ctx, chunk, self.filter, &self.keys)?;
        self.reset_chunk_state();
        Ok(())
    }

    fn set_restored_chunk_for_probe(&mut self, chunk: Chunk) -> Result<(), ProbeError> {
        self.base.set_restored_chunk_for_probe(&self.ctx, chunk)?;
        self.reset_chunk_state();
        Ok(())
    }

    fn reset_chunk_state(&mut self) {
        self.matched.clear();
        self.null_rows.clear();
        if !self.left_build {
            self.matched.resize(self.base.chunk_rows(), false);
            self.null_rows.resize(self.base.chunk_rows(), false);
        }
        self.unfinished.clear();
        if self.condition.is_some() {
            self.unfinished.extend(
                self.base
                    .matched_rows_headers()
                    .iter()
                    .enumerate()
                    .filter_map(|(row, &header)| (header != 0).then_some(row)),
            );
        }
    }

    fn is_current_chunk_probe_done(&self) -> bool {
        self.unfinished.is_empty() && self.base.is_current_chunk_probe_done()
    }

    fn probe(&mut self, output: &mut Chunk, killer: &SqlKiller) -> Result<(), ProbeError> {
        if output.is_full() {
            return Ok(());
        }
        self.base.prepare_for_probe(&self.ctx, output);
        if self.condition.is_some() {
            if !self.unfinished.is_empty() {
                self.produce_result(killer)?;
                self.base.set_current_probe_row(if self.left_build {
                    self.base.chunk_rows()
                } else {
                    0
                });
            } else if self.left_build {
                self.base.set_current_probe_row(self.base.chunk_rows());
            }
            if self.unfinished.is_empty() && !self.left_build {
                self.emit_probe_rows(output);
            }
        } else if self.left_build {
            self.mark_build_rows(killer)?;
        } else {
            self.probe_right_build(output, killer)?;
        }
        Ok(())
    }

    fn produce_result(&mut self, killer: &SqlKiller) -> Result<(), ProbeError> {
        let condition = self.condition.as_mut().expect("residual evaluator");
        condition.chunk.reset();
        let mut remaining = condition.chunk.capacity();
        while remaining > 0 {
            let Some(row) = self.unfinished.pop_front() else {
                break;
            };
            if !self.left_build && self.matched[row] {
                continue;
            }
            self.base.set_current_probe_row(row);
            let hash = self.base.matched_rows_hash_value()[row];
            let partition = crate::row_table_builder::generate_partition_index(
                hash,
                self.ctx.partition_mask_offset,
            ) as usize;
            let mut header = self.base.matched_rows_headers()[row];
            while header != 0
                && remaining > 0
                && self.base.matched_rows_for_current_probe_row() < MAX_MATCHED_ROW_NUM
            {
                let address = crate::hash_table_v2::row_address_of(&self.ctx.tag_helper, header);
                let build_row = self
                    .ctx
                    .hash_table
                    .row_bytes_in_partition(partition, address);
                if !self.left_build || !self.ctx.hash_table.is_build_row_matched(address) {
                    if is_key_matched(
                        self.ctx.meta.key_mode,
                        &self.base.serialized_keys()[row],
                        build_row,
                        self.ctx.meta,
                    ) {
                        self.base.append_build_row_to_cached_build_rows_v1(
                            &self.ctx,
                            self.ctx.hash_table,
                            row,
                            address,
                            &mut condition.chunk,
                            0,
                            true,
                        );
                        self.base.record_matched_row_for_current_probe_row();
                        remaining -= 1;
                    } else {
                        self.base.record_probe_collision();
                    }
                }
                header = BaseJoinProbe::next_matched_row(build_row, &self.ctx.tag_helper, hash);
            }
            self.base.set_matched_rows_header(row, header);
            self.base.finish_lookup_current_probe_row();
            if header != 0 {
                self.unfinished.push_back(row);
            }
        }
        check_probe_killed(killer)?;
        self.base
            .finish_current_lookup_loop(&self.ctx, self.ctx.hash_table, &mut condition.chunk);
        if condition.chunk.num_rows() == 0 {
            return Ok(());
        }
        let (selected, nulls) = (condition.evaluate)(
            &condition.chunk,
            std::mem::take(self.base.selected_mut()),
            std::mem::take(&mut self.nulls),
            self.anti || self.outer_semi,
        )?;
        for (index, info) in self.base.row_index_infos().iter().enumerate() {
            let matched = selected[index] || (self.anti && !self.outer_semi && nulls[index]);
            if self.left_build {
                if matched {
                    self.ctx
                        .hash_table
                        .mark_build_row_matched(info.build_row_start);
                }
            } else {
                self.matched[info.probe_row_index] |= matched;
                self.null_rows[info.probe_row_index] |= nulls[index];
            }
        }
        *self.base.selected_mut() = selected;
        self.nulls = nulls;
        Ok(())
    }

    // Go left-build no-other-condition path: every matching build row needs a
    // mark, including duplicates. Output is deferred until the global barrier.
    fn mark_build_rows(&mut self, killer: &SqlKiller) -> Result<(), ProbeError> {
        let mut loop_count = 0;
        while !self.base.is_current_chunk_probe_done() {
            let row = self.base.current_probe_row();
            let hash = self.base.matched_rows_hash_value()[row];
            let partition = crate::row_table_builder::generate_partition_index(
                hash,
                self.ctx.partition_mask_offset,
            ) as usize;
            let mut header = self.base.matched_rows_headers()[row];
            while header != 0 {
                let address = crate::hash_table_v2::row_address_of(&self.ctx.tag_helper, header);
                let build_row = self
                    .ctx
                    .hash_table
                    .row_bytes_in_partition(partition, address);
                if !self.ctx.hash_table.is_build_row_matched(address) {
                    if is_key_matched(
                        self.ctx.meta.key_mode,
                        &self.base.serialized_keys()[row],
                        build_row,
                        self.ctx.meta,
                    ) {
                        self.ctx.hash_table.mark_build_row_matched(address);
                    } else {
                        self.base.record_probe_collision();
                    }
                }
                header = BaseJoinProbe::next_matched_row(build_row, &self.ctx.tag_helper, hash);
                loop_count += 1;
                if loop_count % 2000 == 0 {
                    check_probe_killed(killer)?;
                }
            }
            self.base.set_matched_rows_header(row, 0);
            self.base.set_current_probe_row(row + 1);
            loop_count += 1;
            if loop_count % 2000 == 0 {
                check_probe_killed(killer)?;
            }
        }
        check_probe_killed(killer)
    }

    fn probe_right_build(
        &mut self,
        output: &mut Chunk,
        killer: &SqlKiller,
    ) -> Result<(), ProbeError> {
        let mut remaining = output.required_rows() - output.num_rows();
        let start = self.base.current_probe_row();
        self.offsets.clear();
        while remaining > 0 && !self.base.is_current_chunk_probe_done() {
            let row = self.base.current_probe_row();
            let hash = self.base.matched_rows_hash_value()[row];
            let partition = crate::row_table_builder::generate_partition_index(
                hash,
                self.ctx.partition_mask_offset,
            ) as usize;
            let mut header = self.base.matched_rows_headers()[row];
            while header != 0 {
                let address = crate::hash_table_v2::row_address_of(&self.ctx.tag_helper, header);
                let build_row = self
                    .ctx
                    .hash_table
                    .row_bytes_in_partition(partition, address);
                if is_key_matched(
                    self.ctx.meta.key_mode,
                    &self.base.serialized_keys()[row],
                    build_row,
                    self.ctx.meta,
                ) {
                    self.matched[row] = true;
                    break;
                }
                self.base.record_probe_collision();
                header = BaseJoinProbe::next_matched_row(build_row, &self.ctx.tag_helper, hash);
            }
            self.base.set_matched_rows_header(row, 0);
            if !self.base.is_spilled(row) && (self.outer_semi || self.matched[row] != self.anti) {
                self.offsets.push(self.base.used_rows()[row]);
                remaining -= 1;
            }
            self.base.set_current_probe_row(row + 1);
        }
        check_probe_killed(killer)?;
        self.copy_probe_rows(output, start);
        Ok(())
    }

    fn emit_probe_rows(&mut self, output: &mut Chunk) {
        let mut remaining = output.required_rows() - output.num_rows();
        let start = self.base.current_probe_row();
        self.offsets.clear();
        while remaining > 0 && !self.base.is_current_chunk_probe_done() {
            let row = self.base.current_probe_row();
            if !self.base.is_spilled(row) && (self.outer_semi || self.matched[row] != self.anti) {
                self.offsets.push(self.base.used_rows()[row]);
                remaining -= 1;
            }
            self.base.set_current_probe_row(row + 1);
        }
        self.copy_probe_rows(output, start);
    }

    fn copy_probe_rows(&self, output: &mut Chunk, start: usize) {
        let before = output.num_rows();
        let input = self.base.current_chunk().expect("probe input");
        let copy_whole_columns = self.outer_semi
            && self.base.spilled_indices().is_empty()
            && start == 0
            && self.base.current_probe_row() == self.base.chunk_rows()
            && input.sel().is_none()
            && before == 0;
        for (index, &column) in self.ctx.l_used.iter().enumerate() {
            if copy_whole_columns {
                // Go buildResult's CopyConstruct into an empty result. Keep
                // the destination allocation while copying the whole column.
                output.append_column_range_from(index, input, column, 0, self.base.chunk_rows());
            } else {
                copy_rows(
                    &mut output.column_mut(index),
                    &input.column(column),
                    &self.offsets,
                );
            }
        }
        if self.outer_semi {
            let flag = self.ctx.l_used.len();
            for row in start..self.base.current_probe_row() {
                if self.base.is_spilled(row) {
                    continue;
                }
                if self.matched[row] {
                    output.append_int64(flag, i64::from(!self.anti));
                } else if self.null_rows[row] {
                    output.append_null(flag);
                } else {
                    output.append_int64(flag, i64::from(self.anti));
                }
            }
        }
        output.set_num_virtual_rows(before + self.offsets.len());
    }

    fn init_for_scan_row_table(&mut self) {
        assert!(self.left_build, "right-build semi family does not scan");
        self.row_iter = Some(common_init_for_scan_row_table(
            self.ctx.hash_table,
            self.base.work_id(),
            self.ctx.concurrency,
        ));
    }

    fn is_scan_row_table_done(&self) -> bool {
        self.row_iter.as_ref().expect("scan before init").is_end()
    }

    fn scan_row_table(&mut self, output: &mut Chunk, killer: &SqlKiller) -> Result<(), ProbeError> {
        if output.is_full() {
            return Ok(());
        }
        self.base.prepare_for_probe(&self.ctx, output);
        let mut remaining = output.required_rows() - output.num_rows();
        let rows = self.row_iter.as_mut().expect("scan before init");
        while remaining > 0 && !rows.is_end() {
            let address = rows.get_value();
            if self.ctx.hash_table.is_build_row_matched(address) != self.anti {
                self.base.append_build_row_to_cached_build_rows_v1(
                    &self.ctx,
                    self.ctx.hash_table,
                    0,
                    address,
                    output,
                    0,
                    false,
                );
                remaining -= 1;
            }
            rows.next();
        }
        check_probe_killed(killer)?;
        if self.base.next_cached_build_row_index() > 0 {
            self.base
                .batch_construct_build_rows(&self.ctx, self.ctx.hash_table, output, 0, false);
        }
        Ok(())
    }

    fn reset_probe(&mut self) {
        self.row_iter = None;
        self.unfinished.clear();
        // Go ResetProbe resets this worker, not the shared table's used flags.
        self.base.reset_probe(&self.ctx);
    }
}

macro_rules! semi_probe {
    ($name:ident, $kind:ident) => {
        #[doc = concat!("Go ", stringify!($kind), " over shared baseSemiJoin state.")]
        pub struct $name<'a> {
            inner: BaseSemiJoin<'a>,
        }
        impl<'a> $name<'a> {
            #[allow(clippy::too_many_arguments)]
            pub fn new(
                ctx: ProbeContext<'a>,
                worker: usize,
                key_index: Vec<usize>,
                nullable: &[bool],
                right_build: bool,
                keys: JoinKeyColumns,
                filter: Option<ProbeFilter<'a>>,
                condition: Option<JoinOtherCondition<'a>>,
            ) -> Self {
                Self {
                    inner: BaseSemiJoin::new(
                        ctx,
                        worker,
                        JoinType::$kind,
                        key_index,
                        nullable,
                        right_build,
                        keys,
                        filter,
                        condition,
                    ),
                }
            }
        }
        impl ProbeV2 for $name<'_> {
            fn set_restored_chunk_for_probe(&mut self, chunk: Chunk) -> Result<(), ProbeError> {
                self.inner.set_restored_chunk_for_probe(chunk)
            }
            fn spill_remaining_probe_chunks(&mut self) -> Result<(), ProbeError> {
                self.inner
                    .base
                    .spill_remaining_probe_chunks(&self.inner.ctx)
            }
            fn take_probe_chunk(&mut self) -> Option<Chunk> {
                self.inner.base.take_probe_chunk()
            }
            fn set_chunk_for_probe(&mut self, chunk: Chunk) -> Result<(), ProbeError> {
                self.inner.set_chunk_for_probe(chunk)
            }
            fn is_current_chunk_probe_done(&self) -> bool {
                self.inner.is_current_chunk_probe_done()
            }
            fn probe(&mut self, output: &mut Chunk, killer: &SqlKiller) -> Result<(), ProbeError> {
                self.inner.probe(output, killer)
            }
            fn need_scan_row_table(&self) -> bool {
                self.inner.left_build
            }
            fn init_for_scan_row_table(&mut self) {
                self.inner.init_for_scan_row_table()
            }
            fn is_scan_row_table_done(&self) -> bool {
                self.inner.is_scan_row_table_done()
            }
            fn scan_row_table(
                &mut self,
                output: &mut Chunk,
                killer: &SqlKiller,
            ) -> Result<(), ProbeError> {
                self.inner.scan_row_table(output, killer)
            }
            fn reset_probe(&mut self) {
                self.inner.reset_probe()
            }
            fn reset_probe_collision(&mut self) {
                self.inner.base.reset_probe_collision()
            }
            fn get_probe_collision(&self) -> u64 {
                self.inner.base.get_probe_collision()
            }
        }
    };
}

semi_probe!(SemiJoinProbe, SemiJoin);
semi_probe!(AntiSemiJoinProbe, AntiSemiJoin);
semi_probe!(LeftOuterSemiJoinProbe, LeftOuterSemiJoin);
semi_probe!(AntiLeftOuterSemiJoinProbe, AntiLeftOuterSemiJoin);
