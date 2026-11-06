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
//! * `buildIndexLookUp`, `buildMPPJoin`, `buildMPPExchangeSender`,
//!   `buildMPPExchangeReceiver` and `buildExpand` refuse by name: they need
//!   region-located extra readers or an MPP task (`HandleMPPDAGReq`).

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use tidb_datatype::FieldType;
use tidb_proto::coprocessor;
use tidb_proto::tipb;
use tidb_unistore_dbreader::DbReader;

use crate::eval_context::RequestEvalContext;
use crate::mpp_exec::{
    AggExec, BaseMppExec, ExecDetail, IndexScanExec, LimitExec, MppExec, ProjExec, RangeCounts,
    SelExec, SharedPaging, TableRowDecoder, TableScanExec, TopNExec,
};
use crate::{convert_expr_with_context, field_type_from_pb_column, RegionAggregator, TopNSpec};

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

    /// Go `buildMPPTableScan` / `buildMPPTableScanWithReader`.
    fn build_mpp_table_scan(&mut self, pb: &tipb::TableScan) -> Result<TableScanExec<'a>, String> {
        let ranges = extract_kv_ranges(&self.key_ranges, pb.desc())?;
        let field_types = pb.columns.iter().map(field_type_from_pb_column).collect();
        let decoder = TableRowDecoder::new(&pb.columns, &pb.primary_column_ids, self.sctx.zone.clone());
        Ok(TableScanExec::new(
            Arc::clone(&self.sctx),
            field_types,
            ranges,
            self.start_ts,
            self.db_reader,
            self.counts.clone(),
            decoder,
            pb.desc(),
            self.paging.clone(),
        ))
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
            self.db_reader,
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
        let field_types = pb
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
            processed: false,
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
            tipb::ExecType::TypeIndexLookUp
            | tipb::ExecType::TypeJoin
            | tipb::ExecType::TypeExchangeSender
            | tipb::ExecType::TypeExchangeReceiver
            | tipb::ExecType::TypeExpand => {
                return Err(format!("the {tp:?} executor is a later course of this port"))
            }
            _ => return Err(format!("{ERR_EXECUTOR_NOT_SUPPORTED_MSG}{tp:?}")),
        })
    }
}
