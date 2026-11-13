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

//! Go `pkg/executor/admin.go`'s `CheckIndexRangeExec`: `ADMIN CHECK INDEX
//! t idx (begin, end), ...` reads the index's own entries over the whole
//! index (`ranger.FullRange()`, kept in order) and answers those whose handle
//! falls in one of the ranges.

use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_distsql::select_response::SelectResponseIter;
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_expr::schema::Schema;
use tidb_model::{ColumnInfo, GoShared, IndexInfo, TableInfo};
use tidb_proto::tipb;

use crate::range_group_merge::SelectResult as _;

/// Go `CheckIndexRangeExec`.
pub(crate) struct CheckIndexRangeExec {
    meta: ExecutorMeta,
    ctx: crate::StmtContext,
    /// Go `dctx.Client`.
    client: Arc<dyn tidb_txnkv::Client>,
    /// Go `table`.
    table: TableInfo,
    /// Go `index`.
    index: IndexInfo,
    /// Go `handleRanges`.
    handle_ranges: Vec<tidb_ast::AdminCheckHandleRange>,
    /// Go `srcChunk`.
    src_chunk: Option<Chunk>,
    /// Go `result`.
    result: Option<SelectResponseIter>,
    /// Go `cols`.
    cols: Vec<GoShared<ColumnInfo>>,
    /// Go `txn.StartTS()`.
    start_ts: u64,
}

impl CheckIndexRangeExec {
    /// Go `executorBuilder.buildCheckIndexRange`.
    pub(crate) fn new(
        meta: ExecutorMeta,
        ctx: crate::StmtContext,
        client: Arc<dyn tidb_txnkv::Client>,
        table: TableInfo,
        index: IndexInfo,
        handle_ranges: Vec<tidb_ast::AdminCheckHandleRange>,
        start_ts: u64,
    ) -> Self {
        Self { meta, ctx, client, table, index, handle_ranges, src_chunk: None, result: None, cols: Vec::new(), start_ts }
    }

    /// Go `buildDAGPB`.
    fn build_dag_pb(
        &self,
        statement: &tidb_distsql::remote_scan::PushdownStatementContext,
    ) -> Result<tipb::DagRequest, ExecError> {
        let (time_zone_name, time_zone_offset) = statement.time_zone.dag_zone();
        let output_offsets = (0..self.meta.schema().columns.len())
            .map(|i| u32::try_from(i).expect("an output offset fits u32"))
            .collect();
        let mut exec_pb = self.construct_index_scan_pb();
        if let Some(idx_scan) = exec_pb.idx_scan.as_mut() {
            tidb_table::tables::scan_from_infos::set_pb_columns_default_value(
                &statement.time_zone,
                &mut idx_scan.columns,
                &self.cols,
                self.table.name.original(),
            )
            .map_err(|error| ExecError::Internal(error.into()))?;
        }
        // Go `distsql.SetEncodeType`.
        let (encode_type, chunk_memory_layout) = crate::dag_request::encode_type_pb(tidb_distsql::EncodeType::Chunk);
        Ok(tipb::DagRequest {
            time_zone_name: Some(time_zone_name),
            time_zone_offset: Some(time_zone_offset),
            flags: Some(statement.push_down_flags),
            output_offsets,
            executors: vec![exec_pb],
            encode_type: Some(encode_type as i32),
            chunk_memory_layout,
            ..tipb::DagRequest::default()
        })
    }

    /// Go `constructIndexScanPB`.
    fn construct_index_scan_pb(&self) -> tipb::Executor {
        let idx_exec = tipb::IndexScan {
            table_id: Some(self.table.id),
            index_id: Some(self.index.id),
            columns: tidb_util::misc::columns_to_proto(&self.cols, self.table.pk_is_handle, true, false),
            ..tipb::IndexScan::default()
        };
        tipb::Executor {
            idx_scan: Some(idx_exec),
            ..crate::dag_request::go_executor(tipb::ExecType::TypeIndexScan)
        }
    }
}

impl Executor for CheckIndexRangeExec {
    /// Go `Open`.
    fn open(&mut self) -> Result<(), ExecError> {
        let t_cols = self.table.cols();
        self.cols.clear();
        for ic in self.index.columns.iter_deref() {
            let offset = usize::try_from(ic.read().offset).unwrap_or(usize::MAX);
            let col = t_cols
                .get(offset)
                .ok_or_else(|| ExecError::Internal("an index column is not a public table column".into()))?;
            self.cols.push(col);
        }
        let col_type_for_handle = self
            .meta
            .schema()
            .columns
            .get(self.cols.len())
            .and_then(|column| column.ret_type.clone())
            .ok_or_else(|| ExecError::Internal("the check index schema has no handle column".into()))?;
        // Go's literal `ColumnInfo{ID, Name, FieldType}`.
        let mut handle = ColumnInfo::default();
        handle.id = tidb_model::column::EXTRA_HANDLE_ID;
        handle.name = ColumnInfo::new_extra_handle_col_info().name;
        handle.field_type = col_type_for_handle;
        self.cols.push(GoShared::new(handle));

        self.src_chunk = Some(self.meta.new_chunk());
        let statement = crate::remote_scan::pushdown_statement_context(&self.ctx);
        let dag = self.build_dag_pb(&statement)?;
        let key_ranges = tidb_distsql::index_ranges_to_kv_ranges(
            Some(&statement.time_zone),
            &[self.table.id],
            self.index.id,
            &tidb_ranger::points::full_range(),
        )
        .map_err(|error| ExecError::Internal(format!("{error:?}").into()))?
        .into_iter()
        .flatten()
        .collect();
        let result = crate::cop_reader::open_table_read(
            self.client.as_ref(),
            &statement,
            crate::cop_reader::TableRead {
                store_type: tidb_kv::store_type::StoreType::TiKv,
                dag,
                key_ranges,
                key_range_hints: Vec::new(),
                partition_id_and_ranges: Vec::new(),
                batch_cop: false,
                schema_version: 0,
                connection_id: tidb_exprctx::Columns::connection_id(&self.ctx).unwrap_or_default(),
                connection_alias: self.ctx.ddl_session_alias().to_owned(),
                start_ts: self.start_ts,
                keep_order: true,
                desc: false,
                paging: statement.paging,
                field_types: self.meta.ret_field_types().to_vec(),
                cop_plan_ids: Vec::new(),
                root_plan_id: isize::try_from(self.meta.id()).unwrap_or(0),
                mem_tracker: None,
                intermediate_output_types: Vec::new(),
            },
        )
        .map_err(|error| ExecError::Internal(error.into()))?;
        self.result = Some(result);
        Ok(())
    }

    /// Go `Next`.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let handle_idx = self.meta.schema().columns.len() - 1;
        let (Some(result), Some(src_chunk)) = (self.result.as_mut(), self.src_chunk.as_mut()) else {
            return Ok(());
        };
        loop {
            result.next(src_chunk)?;
            if src_chunk.num_rows() == 0 {
                return Ok(());
            }
            for i in 0..src_chunk.num_rows() {
                let row = src_chunk.get_row(i);
                let handle = row.get_int64(handle_idx);
                if self.handle_ranges.iter().any(|hr| handle >= hr.begin && handle < hr.end) {
                    req.append_row(row);
                }
            }
            if req.num_rows() > 0 {
                return Ok(());
            }
        }
    }

    /// Go `Close`.
    fn close(&mut self) -> Result<(), ExecError> {
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
