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

//! Go `pkg/store/mockstore/unistore/cophandler/mpp.go`: `mppExecBuilder`,
//! which turns a DAG's executor tree into the `mppExec` chain
//! (`mpp_exec.rs`) the coprocessor runs.
//!
//! # Narrowings, by name
//!
//! * `extractKVRanges` validates and orders the request ranges; the store
//!   reader carries no region bounds to clip them to.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use tidb_datatype::FieldType;
use tidb_proto::coprocessor;
use tidb_proto::tipb;
use tidb_unistore_dbreader::DbReader;

use crate::eval_context::RequestEvalContext;
use crate::mpp_exec::{
    AggExec, BaseMppExec, ExecDetail, ExpandExec, IndexLookUpExec, IndexScanExec, LimitExec, MppExec, ProjExec,
    RangeCounts, SelExec, SharedPaging, TableRowDecoder, TableScanExec, TopNExec,
};
use crate::{convert_expr_with_context, field_type_from_pb_column, RegionAggregator, SimpleExpr, TopNSpec};

/// Go `ErrExecutorNotSupportedMsg`.
pub(crate) const ERR_EXECUTOR_NOT_SUPPORTED_MSG: &str = "executor not supported: ";

/// Go `mppExecBuilder`.
pub(crate) struct MppExecBuilder<'a> {
    pub(crate) sctx: Arc<RequestEvalContext>,
    pub(crate) db_reader: &'a dyn DbReader,
    pub(crate) key_ranges: Vec<coprocessor::KeyRange>,
    pub(crate) start_ts: u64,
    pub(crate) counts: Option<Rc<RefCell<RangeCounts>>>,
    pub(crate) paging: Option<SharedPaging>,
    pub(crate) paging_size: u64,
    /// Go `mppCtx`: set when the DAG runs as an MPP task.
    pub(crate) mpp_ctx: Option<Arc<crate::mpp_task::MppCtx>>,
    /// Go `dagReq.OutputOffsets`, which an exchange sender sends.
    pub(crate) output_offsets: Vec<u32>,
    /// Go `inBuildIndexLookUp`.
    pub(crate) in_build_index_look_up: bool,
}

/// Go `buildMPPTableScanWithReader`, over the builder's state it reads.
fn build_mpp_table_scan_with_reader<'a>(
    sctx: &Arc<RequestEvalContext>,
    start_ts: u64,
    counts: Option<Rc<RefCell<RangeCounts>>>,
    paging: Option<SharedPaging>,
    pb: &tipb::TableScan,
    reader: Box<dyn DbReader + 'a>,
    read_ranges: &[coprocessor::KeyRange],
) -> Result<TableScanExec<'a>, String> {
    let ranges = extract_kv_ranges(read_ranges, pb.desc())?;
    let field_types = pb.columns.iter().map(field_type_from_pb_column).collect();
    let decoder = TableRowDecoder::new(&pb.columns, &pb.primary_column_ids, sctx.zone.clone());
    Ok(TableScanExec::new(
        Arc::clone(sctx),
        field_types,
        ranges,
        start_ts,
        reader,
        counts,
        decoder,
        pb.desc(),
        paging,
    ))
}

/// Go's `%v` rendering of a `[]byte`: space-separated decimals in brackets.
fn go_byte_slice(bytes: &[u8]) -> String {
    let rendered = bytes
        .iter()
        .map(u8::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    format!("[{rendered}]")
}

/// Go `extractKVRanges` (`cop_handler.go`): every range must be ordered, and
/// a descending scan walks them last to first.
pub(crate) fn extract_kv_ranges(
    key_ranges: &[coprocessor::KeyRange],
    desc_scan: bool,
) -> Result<Vec<coprocessor::KeyRange>, String> {
    let mut kv_ranges = Vec::with_capacity(key_ranges.len());
    for range in key_ranges {
        if range.start >= range.end {
            return Err(format!(
                "invalid range, start should be smaller than end: {} {}",
                go_byte_slice(&range.start),
                go_byte_slice(&range.end)
            ));
        }
        kv_ranges.push(range.clone());
    }
    if desc_scan {
        kv_ranges.reverse();
    }
    Ok(kv_ranges)
}

/// The executor body Go dereferences without a check.
fn body<'b, T>(body: Option<&'b T>, name: &str) -> Result<&'b T, String> {
    body.ok_or_else(|| format!("executor missing {name} body"))
}

impl<'a> MppExecBuilder<'a> {
    fn base(&self, children: Vec<Box<dyn MppExec<'a> + 'a>>, field_types: Vec<FieldType>) -> BaseMppExec<'a> {
        BaseMppExec {
            sctx: Arc::clone(&self.sctx),
            children,
            field_types,
            exec_summary: ExecDetail::default(),
        }
    }

    /// The request's session context over `child`'s output schema, which
    /// Go's builders hand `convertToExprs` / `PBToExpr`.
    fn child_ctx(&self, child: &dyn MppExec<'a>) -> Arc<RequestEvalContext> {
        self.sctx.with_column_types(child.get_field_types().to_vec())
    }

    /// Go `buildMPPTableScan`.
    fn build_mpp_table_scan(&mut self, pb: &tipb::TableScan) -> Result<TableScanExec<'a>, String> {
        build_mpp_table_scan_with_reader(
            &self.sctx,
            self.start_ts,
            self.counts.clone(),
            self.paging.clone(),
            pb,
            Box::new(self.db_reader),
            &self.key_ranges,
        )
    }

    /// Go `buildIndexLookUp`.
    fn build_index_look_up(&mut self, pb: &tipb::IndexLookUp) -> Result<IndexLookUpExec<'a>, String> {
        self.in_build_index_look_up = true;
        let built = self.build_index_look_up_inner(pb);
        self.in_build_index_look_up = false;
        built
    }

    fn build_index_look_up_inner(&mut self, pb: &tipb::IndexLookUp) -> Result<IndexLookUpExec<'a>, String> {
        if pb.children.len() != 2 {
            return Err("IndexLookUp should have two children".to_owned());
        }
        if pb.children[1].tp() != tipb::ExecType::TypeTableScan {
            return Err("The second child of IndexLookUp should be TableScan".to_owned());
        }
        if self.paging_size > 0 {
            return Err("paging not supported in index lookup push down".to_owned());
        }

        let tbl_scan_pb = body(pb.children[1].tbl_scan.as_ref(), "tbl_scan")?.clone();
        let index_scan_child = self.build_mpp_executor(&pb.children[0])?;
        let field_types = tbl_scan_pb
            .columns
            .iter()
            .map(field_type_from_pb_column)
            .collect::<Vec<_>>();
        // a mock child that only used to provide base and metrics info
        let mock_child = Box::new(self.base(Vec::new(), field_types.clone()));
        let db_reader = self.db_reader;
        let (sctx, start_ts) = (Arc::clone(&self.sctx), self.start_ts);
        let (counts, paging) = (self.counts.clone(), self.paging.clone());
        Ok(IndexLookUpExec {
            base: self.base(vec![index_scan_child, mock_child], field_types),
            index_handle_offsets: pb.index_handle_offsets.clone(),
            table_id: tbl_scan_pb.table_id(),
            is_common_handle: !tbl_scan_pb.primary_column_ids.is_empty(),
            extra_reader_provider: db_reader.extra_db_reader_provider(),
            build_table_scan: Box::new(move |reader, ranges| {
                build_mpp_table_scan_with_reader(
                    &sctx,
                    start_ts,
                    counts.clone(),
                    paging.clone(),
                    &tbl_scan_pb,
                    reader,
                    ranges,
                )
            }),
            index_chunks: Vec::new(),
        })
    }

    /// Go `buildMPPPartitionTableScan`: an ascending scan without range
    /// counts or paging.
    fn build_mpp_partition_table_scan(
        &mut self,
        pb: &tipb::PartitionTableScan,
    ) -> Result<TableScanExec<'a>, String> {
        let ranges = extract_kv_ranges(&self.key_ranges, false)?;
        let field_types = pb.columns.iter().map(field_type_from_pb_column).collect();
        let decoder = TableRowDecoder::new(&pb.columns, &pb.primary_column_ids, self.sctx.zone.clone());
        Ok(TableScanExec::new(
            Arc::clone(&self.sctx),
            field_types,
            ranges,
            self.start_ts,
            Box::new(self.db_reader),
            None,
            decoder,
            false,
            None,
        ))
    }

    /// Go `buildIdxScan`: the index columns, the handle columns behind them
    /// (decoded by `DecodeIndexKV`), and an optional physical table id.
    fn build_idx_scan(&mut self, pb: &tipb::IndexScan) -> Result<IndexScanExec<'a>, String> {
        const EXTRA_HANDLE_ID: i64 = -1;
        const EXTRA_PHYSICAL_TABLE_ID: i64 = -3;
        let ranges = extract_kv_ranges(&self.key_ranges, pb.desc())?;
        let mut field_types = pb
            .columns
            .iter()
            .map(field_type_from_pb_column)
            .collect::<Vec<_>>();
        let col_infos = pb
            .columns
            .iter()
            .zip(&field_types)
            .filter(|(column, _)| column.column_id() != EXTRA_HANDLE_ID)
            .map(|(column, field_type)| tidb_rowcodec::ColumnInfo {
                id: column.column_id(),
                is_pk_handle: column.pk_handle(),
                virtual_generated: false,
                field_type: field_type.clone(),
            })
            .collect::<Vec<_>>();
        // `numIdxCols` counts only index-key columns. The trailing handle
        // columns are materialized separately by DecodeIndexKV.
        let has_phys_tbl_id = pb
            .columns
            .last()
            .is_some_and(|column| column.column_id() == EXTRA_PHYSICAL_TABLE_ID);
        let mut num_idx_cols = pb.columns.len() - usize::from(has_phys_tbl_id);
        let hdl_status = if !pb.primary_column_ids.is_empty() {
            num_idx_cols = num_idx_cols.saturating_sub(pb.primary_column_ids.len());
            tidb_tablecodec::HandleStatus::Default
        } else if let Some(handle) = num_idx_cols
            .checked_sub(1)
            .and_then(|offset| pb.columns.get(offset))
            .filter(|column| column.pk_handle() || column.column_id() == EXTRA_HANDLE_ID)
        {
            num_idx_cols -= 1;
            if field_type_from_pb_column(handle).is_unsigned() {
                tidb_tablecodec::HandleStatus::Unsigned
            } else {
                tidb_tablecodec::HandleStatus::Default
            }
        } else {
            tidb_tablecodec::HandleStatus::NotNeeded
        };
        // For index scan in index lookup, if the table has common handle, we need to
        // return the common handle key to indexLookUpExec
        let common_handle_key_idx =
            (self.in_build_index_look_up && !pb.primary_column_ids.is_empty()).then(|| {
                field_types.push(FieldType::new(tidb_datatype::FieldTypeCode::Varchar));
                field_types.len() - 1
            });
        Ok(IndexScanExec::new(
            Arc::clone(&self.sctx),
            field_types,
            self.start_ts,
            ranges,
            pb.desc(),
            self.db_reader,
            self.counts.clone(),
            col_infos,
            num_idx_cols,
            hdl_status,
            has_phys_tbl_id,
            common_handle_key_idx,
            self.sctx.zone.clone(),
            self.paging.clone(),
        ))
    }

    /// Go `buildLimit`.
    fn build_limit(&mut self, pb: &tipb::Limit) -> Result<LimitExec<'a>, String> {
        let child = self.build_mpp_executor(body(pb.child.as_deref(), "limit child")?)?;
        let field_types = child.get_field_types().to_vec();
        Ok(LimitExec {
            base: self.base(vec![child], field_types),
            limit: pb.limit(),
        })
    }

    /// Go `buildExpand`.
    fn build_expand(&mut self, pb: &tipb::Expand) -> Result<ExpandExec<'a>, String> {
        let child = self.build_mpp_executor(body(pb.child.as_deref(), "expand child")?)?;
        let ctx = self.child_ctx(child.as_ref());
        let child_field_types = child.get_field_types().to_vec();
        // convert the grouping sets.
        let mut tidb_gss = Vec::with_capacity(pb.grouping_sets.len());
        for gs in &pb.grouping_sets {
            let mut tidb_gs = Vec::with_capacity(gs.grouping_exprs.len());
            for grouping_exprs in &gs.grouping_exprs {
                let tidb_grouping_exprs = grouping_exprs
                    .grouping_expr
                    .iter()
                    .map(|pb_expr| convert_expr_with_context(pb_expr, &ctx))
                    .collect::<Result<Vec<_>, _>>()?;
                tidb_gs.push(tidb_grouping_exprs);
            }
            tidb_gss.push(tidb_gs);
        }
        let mut in_grouping_set_map = std::collections::HashSet::new();
        for gs in &tidb_gss {
            // for every grouping set, collect column offsets under this grouping set.
            for grouping_exprs in gs {
                for grouping_expr in grouping_exprs {
                    let SimpleExpr::Column(index) = grouping_expr else {
                        return Err("grouping set expr is not column ref".to_owned());
                    };
                    in_grouping_set_map.insert(*index);
                }
            }
        }
        // change the field types return from children tobe nullable.
        let mut mutated_field_types: Vec<FieldType> = child_field_types
            .iter()
            .enumerate()
            .map(|(offset, f)| {
                let mut cf = f.clone();
                if in_grouping_set_map.contains(&offset) {
                    // remove the not null flag, make it nullable.
                    cf.del_flag(tidb_datatype::FieldTypeFlags::NOT_NULL);
                }
                cf
            })
            .collect();
        // adding groupingID uint64|not-null as last one field types.
        let mut grouping_id_field_type = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        grouping_id_field_type.set_flag(tidb_datatype::FieldTypeFlags::NOT_NULL | tidb_datatype::FieldTypeFlags::UNSIGNED);
        mutated_field_types.push(grouping_id_field_type);
        Ok(ExpandExec {
            base: self.base(vec![child], mutated_field_types),
            last_num: 0,
            last_chunk: None,
            grouping_sets: tidb_gss,
            grouping_set_offset_map: Vec::new(),
            grouping_set_scope: std::collections::HashSet::new(),
        })
    }

    /// Go `buildTopN`.
    fn build_top_n(&mut self, pb: &tipb::TopN) -> Result<TopNExec<'a>, String> {
        let child = self.build_mpp_executor(body(pb.child.as_deref(), "topN child")?)?;
        let spec = TopNSpec::build(pb, &self.child_ctx(child.as_ref()))?;
        let field_types = child.get_field_types().to_vec();
        // When using paging protocol, if paging size < topN limit, the topN
        // exec degenerates to do nothing.
        let dummy = self.paging.is_some() && self.paging_size < pb.limit();
        Ok(TopNExec {
            base: self.base(vec![child], field_types),
            topn: pb.limit(),
            idx: 0,
            spec,
            rows: Vec::new(),
            dummy,
        })
    }

    /// Go `buildMPPProj`.
    fn build_mpp_proj(&mut self, proj: &tipb::Projection) -> Result<ProjExec<'a>, String> {
        let ch_exec = self.build_mpp_executor(body(proj.child.as_deref(), "projection child")?)?;
        let mut exprs = Vec::with_capacity(proj.exprs.len());
        let mut field_types = Vec::with_capacity(proj.exprs.len());
        for pb_expr in &proj.exprs {
            let expr = tidb_expr::distsql_builtin::pb_to_expr_in(
                pb_expr,
                ch_exec.get_field_types(),
                &self.sctx.zone,
            )?;
            // Go `expr.GetType(ctx)`.
            let field_type = match expr.static_type() {
                Some(field_type) => field_type.clone(),
                None => tidb_expr::distsql_builtin::pb_type_to_field_type(
                    &pb_expr.field_type.clone().unwrap_or_default(),
                ),
            };
            exprs.push(expr);
            field_types.push(field_type);
        }
        Ok(ProjExec {
            base: self.base(vec![ch_exec], field_types),
            exprs,
        })
    }

    /// Go `buildMPPSel`.
    fn build_mpp_sel(&mut self, sel: &tipb::Selection) -> Result<SelExec<'a>, String> {
        let ch_exec = self.build_mpp_executor(body(sel.child.as_deref(), "selection child")?)?;
        let ctx = self.child_ctx(ch_exec.as_ref());
        let conditions = sel
            .conditions
            .iter()
            .map(|pb_expr| convert_expr_with_context(pb_expr, &ctx))
            .collect::<Result<Vec<_>, _>>()?;
        let field_types = ch_exec.get_field_types().to_vec();
        Ok(SelExec {
            base: self.base(vec![ch_exec], field_types),
            conditions,
        })
    }

    /// Go `buildMPPAgg`: the aggregate results' types, then the group keys'.
    fn build_mpp_agg(&mut self, agg: &tipb::Aggregation) -> Result<AggExec<'a>, String> {
        let ch_exec = self.build_mpp_executor(body(agg.child.as_deref(), "aggregation child")?)?;
        let aggregator = RegionAggregator::build(agg, &self.child_ctx(ch_exec.as_ref()))?;
        let field_types = agg
            .agg_func
            .iter()
            .chain(&agg.group_by)
            .map(|expr| {
                tidb_expr::distsql_builtin::pb_type_to_field_type(
                    &expr.field_type.clone().unwrap_or_default(),
                )
            })
            .collect();
        Ok(AggExec {
            base: self.base(vec![ch_exec], field_types),
            aggregator: Some(aggregator),
            agg_exprs_len: agg.agg_func.len(),
            processed: false,
        })
    }

    fn mpp_ctx(&self) -> Result<Arc<crate::mpp_task::MppCtx>, String> {
        self.mpp_ctx.clone().ok_or_else(|| "an exchange runs only in an MPP task".to_owned())
    }

    /// Go `buildMPPExchangeSender`.
    fn build_mpp_exchange_sender(&mut self, pb: &tipb::ExchangeSender) -> Result<crate::mpp_exec::ExchSenderExec<'a>, String> {
        let child = self.build_mpp_executor(body(pb.child.as_deref(), "child")?)?;
        let field_types = child.get_field_types().to_vec();
        let mpp_ctx = self.mpp_ctx()?;

        let mut hash_key_offsets = Vec::new();
        let mut hash_key_types = Vec::new();
        if pb.tp() == tipb::ExchangeType::Hash {
            // remove the limitation of len(pb.PartitionKeys) == 1
            for partition_key in &pb.partition_keys {
                if partition_key.tp() != tipb::ExprType::ColumnRef {
                    return Err("Hash key must be column type".to_owned());
                }
                let (_, offset) = tidb_codec::decode_int(partition_key.val.as_deref().unwrap_or_default())
                    .map_err(|err| format!("{err:?}"))?;
                let offset = usize::try_from(offset).map_err(|_| "Hash key must be column type".to_owned())?;
                hash_key_offsets.push(offset);
                hash_key_types.push(field_types[offset].clone());
            }
        }

        let mut tunnels = Vec::with_capacity(pb.encoded_task_meta.len());
        for task_meta in &pb.encoded_task_meta {
            use prost::Message;
            let target_task = tidb_proto::mpp::TaskMeta::decode(task_meta.as_slice()).map_err(|err| err.to_string())?;
            let tunnel = Arc::new(crate::mpp_task::ExchangerTunnel::new(mpp_ctx.task_handler.meta.clone(), target_task));
            tunnels.push(Arc::clone(&tunnel));
            mpp_ctx.task_handler.register_tunnel(tunnel)?;
        }
        Ok(crate::mpp_exec::ExchSenderExec {
            base: self.base(vec![child], field_types),
            tunnels,
            output_offsets: self.output_offsets.clone(),
            exchange_tp: pb.tp(),
            hash_key_offsets,
            hash_key_types,
        })
    }

    /// Go `buildMPPExchangeReceiver`.
    fn build_mpp_exchange_receiver(&mut self, pb: &tipb::ExchangeReceiver) -> Result<crate::mpp_exec::ExchRecvExec<'a>, String> {
        let field_types = pb.field_types.iter().map(tidb_expr::distsql_builtin::pb_type_to_field_type).collect();
        Ok(crate::mpp_exec::ExchRecvExec {
            base: self.base(Vec::new(), field_types),
            exchange_receiver: pb.clone(),
            mpp_ctx: self.mpp_ctx()?,
            chk: None,
            inited: false,
        })
    }

    /// Go `buildMPPJoin`.
    fn build_mpp_join(&mut self, pb: &tipb::Join, children: &[tipb::Executor]) -> Result<crate::mpp_exec::JoinExec<'a>, String> {
        let [left, right] = children else {
            return Err("a join has two children".to_owned());
        };
        let left_ch = self.build_mpp_executor(left)?;
        let right_ch = self.build_mpp_executor(right)?;
        let mut left_types = left_ch.get_field_types().to_vec();
        let mut right_types = right_ch.get_field_types().to_vec();
        let mut default_inner = Vec::new();
        if pb.join_type() == tipb::JoinType::TypeLeftOuterJoin {
            for tp in &mut right_types {
                tp.del_flag(tidb_datatype::FieldTypeFlags::NOT_NULL);
            }
            default_inner = vec![tidb_datatype::Datum::Null; right_types.len()];
        } else if pb.join_type() == tipb::JoinType::TypeRightOuterJoin {
            for tp in &mut left_types {
                tp.del_flag(tidb_datatype::FieldTypeFlags::NOT_NULL);
            }
            default_inner = vec![tidb_datatype::Datum::Null; left_types.len()];
        }
        let column_index = |key: Option<&tipb::Expr>| -> Result<usize, String> {
            let key = key.ok_or("a join has no key")?;
            if key.tp() != tipb::ExprType::ColumnRef {
                return Err("a join key must be a column".to_owned());
            }
            let (_, index) = tidb_codec::decode_int(key.val.as_deref().unwrap_or_default()).map_err(|err| format!("{err:?}"))?;
            usize::try_from(index).map_err(|_| "a join key must be a column".to_owned())
        };
        let left_key = column_index(pb.left_join_keys.first())?;
        let right_key = column_index(pb.right_join_keys.first())?;
        let inner_idx = usize::try_from(pb.inner_idx()).map_err(|_| "a join's inner index".to_owned())?;
        let (build_key, probe_key, build_key_tp, probe_key_tp) = if inner_idx == 1 {
            (right_key, left_key, right_types[right_key].clone(), left_types[left_key].clone())
        } else {
            (left_key, right_key, left_types[left_key].clone(), right_types[right_key].clone())
        };
        let mut com_key_tp = tidb_datatype::agg_field_type(&[probe_key_tp, build_key_tp]);
        if com_key_tp.code() == tidb_datatype::FieldTypeCode::NewDecimal {
            com_key_tp.set_flen(tidb_mysql::MaxDecimalWidth as i64);
            com_key_tp.set_decimal(tidb_mysql::MaxDecimalScale as i64);
        }
        // because the field type is immutable, so this kind of appending is safe.
        let field_types = left_types.into_iter().chain(right_types).collect();
        Ok(crate::mpp_exec::JoinExec {
            base: self.base(vec![left_ch, right_ch], field_types),
            join_type: pb.join_type(),
            hash_map: std::collections::HashMap::new(),
            build_key,
            probe_key,
            build_side_idx: inner_idx,
            idx: 0,
            reserved_rows: Vec::new(),
            default_inner,
            inited: false,
            com_key_tp,
        })
    }

    /// Go `buildMPPExecutor`.
    pub(crate) fn build_mpp_executor(
        &mut self,
        exec: &tipb::Executor,
    ) -> Result<Box<dyn MppExec<'a> + 'a>, String> {
        let tp = exec.tp();
        Ok(match tp {
            tipb::ExecType::TypeTableScan => {
                Box::new(self.build_mpp_table_scan(body(exec.tbl_scan.as_ref(), "tbl_scan")?)?)
            }
            tipb::ExecType::TypeAggregation | tipb::ExecType::TypeStreamAgg => Box::new(
                self.build_mpp_agg(body(exec.aggregation.as_deref(), "aggregation")?)?,
            ),
            tipb::ExecType::TypeProjection => Box::new(
                self.build_mpp_proj(body(exec.projection.as_deref(), "projection")?)?,
            ),
            tipb::ExecType::TypeSelection => {
                Box::new(self.build_mpp_sel(body(exec.selection.as_deref(), "selection")?)?)
            }
            tipb::ExecType::TypeIndexScan => {
                Box::new(self.build_idx_scan(body(exec.idx_scan.as_ref(), "idx_scan")?)?)
            }
            tipb::ExecType::TypeLimit => {
                Box::new(self.build_limit(body(exec.limit.as_deref(), "limit")?)?)
            }
            tipb::ExecType::TypeTopN => {
                Box::new(self.build_top_n(body(exec.top_n.as_deref(), "topN")?)?)
            }
            tipb::ExecType::TypePartitionTableScan => Box::new(self.build_mpp_partition_table_scan(
                body(exec.partition_table_scan.as_ref(), "partition_table_scan")?,
            )?),
            tipb::ExecType::TypeExchangeSender => Box::new(
                self.build_mpp_exchange_sender(body(exec.exchange_sender.as_deref(), "exchange_sender")?)?,
            ),
            tipb::ExecType::TypeExchangeReceiver => Box::new(
                self.build_mpp_exchange_receiver(body(exec.exchange_receiver.as_ref(), "exchange_receiver")?)?,
            ),
            tipb::ExecType::TypeJoin => {
                let join = body(exec.join.as_ref(), "join")?;
                Box::new(self.build_mpp_join(join, &join.children)?)
            }
            tipb::ExecType::TypeExpand => Box::new(self.build_expand(body(exec.expand.as_deref(), "expand")?)?),
            tipb::ExecType::TypeIndexLookUp => {
                Box::new(self.build_index_look_up(body(exec.index_lookup.as_ref(), "index_lookup")?)?)
            }
            _ => return Err(format!("{ERR_EXECUTOR_NOT_SUPPORTED_MSG}{tp:?}")),
        })
    }
}
