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

//! TiKV physical scan to tipb DAG request lowering.
//!
//! This is the bounded list-based branch of Go `ConstructDAGReq` plus
//! `PhysicalTableScan.ToPB`, `PhysicalIndexScan.ToPB`, and the bounded TiKV
//! list form of `PhysicalSelection.ToPB`. It accepts exactly one already-built
//! TiKV scan plus an optional resolved Selection and constructs no ranges,
//! reader, region task, or RPC transport.

use std::{error::Error, fmt};

use tidb_distsql::{system_endian, EncodeType as DistSqlEncodeType, SystemEndian};
use tidb_planner::{
    physical::{PhysicalIndexScan, PhysicalTableScan},
    logical::tikv_scan_spec::{
        check_cover_index, ScanColumnInfo, TiKvIndexScanSpec, TiKvTableScanSpec,
        UnsupportedScanFeature,
    },
};
use tidb_proto::tipb::{
    Aggregation, ChunkMemoryLayout, ColumnInfo, DagRequest, EncodeType, Endian, EngineType,
    ExecType, Executor, Expr, IndexScan, Limit, Selection, TableScan,
};

/// Go's default `div_precision_increment`; the field is omitted at this value.
pub const DEFAULT_DIV_PRECISION_INCREMENT: u32 = 4;

/// Request-scoped values read by Go's `ConstructDAGReq`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DagRequestContext {
    /// Resolved location name, including the actual system location name.
    pub time_zone_name: String,
    /// Location offset in seconds for the statement timestamp.
    pub time_zone_offset: i64,
    /// `StatementContext.PushDownFlags()`.
    pub push_down_flags: u64,
    /// Whether a runtime-statistics collector is attached.
    pub collect_execution_summaries: bool,
    /// Session division scale increment.
    pub div_precision_increment: u32,
    /// Result encoding selected by the existing alignment-gated policy.
    pub encode_type: DistSqlEncodeType,
}

impl DagRequestContext {
    /// Creates the source defaults around one resolved timezone and flag set.
    #[must_use]
    pub fn new(
        time_zone_name: impl Into<String>,
        time_zone_offset: i64,
        push_down_flags: u64,
        encode_type: DistSqlEncodeType,
    ) -> Self {
        Self {
            time_zone_name: time_zone_name.into(),
            time_zone_offset,
            push_down_flags,
            collect_execution_summaries: false,
            div_precision_increment: DEFAULT_DIV_PRECISION_INCREMENT,
            encode_type,
        }
    }
}

/// The two physical scan variants admitted by this list-based TiKV closure.
#[derive(Clone, Copy, Debug)]
pub enum TiKvScanPlan<'a> {
    /// Ordinary table scan.
    Table(&'a PhysicalTableScan),
    /// Ordinary index scan.
    Index(&'a PhysicalIndexScan),
}

/// Why a physical scan cannot enter the bounded TiKV DAG request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DagRequestBuildError {
    /// This closure requires one source scan and no parent executors.
    PlanCount {
        /// Number of plans supplied by the caller.
        actual: usize,
    },
    /// The table scan has not received schema-resolved ToPB metadata.
    MissingTablePushdown,
    /// The live index task has not received schema-resolved ToPB metadata.
    MissingIndexPushdown,
    /// A scan feature belongs to a deliberately external planner/store owner.
    UnsupportedScanFeature(UnsupportedScanFeature),
    /// A requested final projection offset is outside the scan output.
    OutputOffsetOutOfRange {
        /// Invalid caller-supplied offset.
        offset: u32,
        /// Number of columns emitted by the scan.
        width: usize,
    },
    /// A metadata-only Selection cannot enter the executable TiKV DAG.
    EmptySelection,
    /// A metadata-only Aggregation cannot enter the executable TiKV DAG.
    EmptyAggregation,
}

impl fmt::Display for DagRequestBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PlanCount { actual } => {
                write!(f, "TiKV scan DAG requires exactly one plan, got {actual}")
            }
            Self::MissingTablePushdown => {
                f.write_str("physical table scan lacks pre-resolved TiKV pushdown metadata")
            }
            Self::MissingIndexPushdown => {
                f.write_str("physical index scan lacks pre-resolved TiKV pushdown metadata")
            }
            Self::UnsupportedScanFeature(feature) => {
                write!(f, "unsupported TiKV scan feature: {feature:?}")
            }
            Self::OutputOffsetOutOfRange { offset, width } => {
                write!(
                    f,
                    "TiKV output offset {offset} is outside scan width {width}"
                )
            }
            Self::EmptySelection => {
                f.write_str("TiKV Selection requires at least one resolved condition")
            }
            Self::EmptyAggregation => {
                f.write_str("TiKV Aggregation requires at least one resolved function")
            }
        }
    }
}

impl Error for DagRequestBuildError {}

/// Ports the TiKV list branch of Go `ConstructDAGReq` for one physical scan.
pub fn construct_dag_req(
    context: &DagRequestContext,
    plans: &[TiKvScanPlan<'_>],
) -> Result<DagRequest, DagRequestBuildError> {
    construct_dag_req_inner(
        context,
        plans,
        SelectionSource::None,
        None,
        None,
        None,
        false,
        None,
    )
}

/// Go's `PhysicalLimit.ToPB` in the TiKV list form: the coprocessor stops
/// after `limit` rows, so the rows past the cap never leave the region.
///
/// The cap is `offset + count`, because the coprocessor has no offset of its
/// own -- the client skips the offset in the rows it receives, which is what
/// Go's pushed `Limit` does too.
#[must_use]
pub fn limit_to_pb(limit: u64) -> Executor {
    Executor {
        tp: Some(ExecType::TypeLimit as i32),
        tbl_scan: None,
        idx_scan: None,
        selection: None,
        aggregation: None,
        top_n: None,
        limit: Some(Box::new(Limit {
            limit: Some(limit),
            ..Limit::default()
        })),
        executor_id: Some(String::new()),
        parent_idx: None,
        exchange_sender: None,
        ..Default::default()
    }
}

/// Where a DAG's Selection conditions come from: the planner's pushed
/// conditions, already converted to TiPB (Go `ExprToPB`).
#[derive(Clone, Copy, Debug)]
enum SelectionSource<'a> {
    /// No Selection executor at all.
    None,
    /// Already-lowered conditions, in `WHERE` order.
    Conditions(&'a [Expr]),
}

/// Go's TiKV list DAG for one scan, the pushed conditions the caller already
/// lowered to TiPB and an optional coprocessor row cap. An empty `conditions` builds no Selection executor.
pub fn construct_capped_read_only_dag_req_with_conditions(
    context: &DagRequestContext,
    scan: TiKvScanPlan<'_>,
    conditions: &[Expr],
    limit: Option<u64>,
    output_offsets: &[u32],
) -> Result<DagRequest, DagRequestBuildError> {
    construct_dag_req_inner(
        context,
        &[scan],
        if conditions.is_empty() {
            SelectionSource::None
        } else {
            SelectionSource::Conditions(conditions)
        },
        Some(output_offsets),
        None,
        None,
        false,
        limit,
    )
}

/// A TiKV list DAG with a global partial aggregation above the complete
/// Selection. Aggregate functions are already resolved TiPB expressions; an
/// empty slice is rejected rather than silently changing the output schema.
pub fn construct_aggregate_read_only_dag_req_with_conditions(
    context: &DagRequestContext,
    scan: TiKvScanPlan<'_>,
    conditions: &[Expr],
    aggregate_functions: &[Expr],
    streamed: bool,
    output_offsets: &[u32],
) -> Result<DagRequest, DagRequestBuildError> {
    construct_dag_req_inner(
        context,
        &[scan],
        if conditions.is_empty() {
            SelectionSource::None
        } else {
            SelectionSource::Conditions(conditions)
        },
        Some(output_offsets),
        Some(aggregate_functions),
        None,
        streamed,
        None,
    )
}

/// A TiKV list DAG with a grouped partial aggregation above the complete
/// Selection. TiKV's output schema is aggregate functions first, followed by
/// the group-key expressions in source order.
pub fn construct_grouped_aggregate_read_only_dag_req_with_conditions(
    context: &DagRequestContext,
    scan: TiKvScanPlan<'_>,
    conditions: &[Expr],
    aggregate_functions: &[Expr],
    group_by: &[Expr],
    streamed: bool,
    output_offsets: &[u32],
) -> Result<DagRequest, DagRequestBuildError> {
    construct_dag_req_inner(
        context,
        &[scan],
        if conditions.is_empty() {
            SelectionSource::None
        } else {
            SelectionSource::Conditions(conditions)
        },
        Some(output_offsets),
        Some(aggregate_functions),
        Some(group_by),
        streamed,
        None,
    )
}

fn aggregation_to_executor(aggregation: Aggregation) -> Executor {
    let streamed = aggregation.streamed == Some(true);
    Executor {
        tp: Some(if streamed {
            ExecType::TypeStreamAgg
        } else {
            ExecType::TypeAggregation
        } as i32),
        tbl_scan: None,
        idx_scan: None,
        selection: None,
        aggregation: Some(Box::new(aggregation)),
        top_n: None,
        limit: None,
        executor_id: Some(String::new()),
        parent_idx: None,
        exchange_sender: None,
        ..Default::default()
    }
}

fn construct_dag_req_inner(
    context: &DagRequestContext,
    plans: &[TiKvScanPlan<'_>],
    selection: SelectionSource<'_>,
    requested_output_offsets: Option<&[u32]>,
    aggregate_functions: Option<&[Expr]>,
    aggregate_group_by: Option<&[Expr]>,
    aggregate_streamed: bool,
    limit: Option<u64>,
) -> Result<DagRequest, DagRequestBuildError> {
    construct_dag_req_assembled(
        context,
        plans,
        selection,
        requested_output_offsets,
        aggregate_functions,
        aggregate_group_by,
        aggregate_streamed,
        limit,
        None,
    )
}

fn construct_dag_req_assembled(
    context: &DagRequestContext,
    plans: &[TiKvScanPlan<'_>],
    selection: SelectionSource<'_>,
    requested_output_offsets: Option<&[u32]>,
    aggregate_functions: Option<&[Expr]>,
    aggregate_group_by: Option<&[Expr]>,
    aggregate_streamed: bool,
    limit: Option<u64>,
    aggregation: Option<(Aggregation, usize)>,
) -> Result<DagRequest, DagRequestBuildError> {
    let [plan] = plans else {
        return Err(DagRequestBuildError::PlanCount {
            actual: plans.len(),
        });
    };

    let (scan_executor, scan_columns) = match plan {
        TiKvScanPlan::Table(plan) => {
            let spec = plan
                .pushdown()
                .ok_or(DagRequestBuildError::MissingTablePushdown)?;
            (table_scan_to_pb(spec)?, spec.columns.as_slice())
        }
        TiKvScanPlan::Index(plan) => (
            index_scan_to_pb(plan)?,
            plan.pushdown()
                .ok_or(DagRequestBuildError::MissingIndexPushdown)?
                .columns
                .as_slice(),
        ),
    };
    if aggregate_functions.is_some_and(<[Expr]>::is_empty)
        && aggregate_group_by.is_none_or(<[Expr]>::is_empty)
    {
        return Err(DagRequestBuildError::EmptyAggregation);
    }
    let output_width = match &aggregation {
        // The aggregation replaces the scan row: the reader addresses the
        // aggregate results and group keys, never the scanned columns.
        Some((_, width)) => *width,
        None => aggregate_functions.map_or(scan_columns.len(), |functions| {
            functions.len() + aggregate_group_by.map_or(0, <[Expr]>::len)
        }),
    };
    let output_offsets = match requested_output_offsets {
        Some(offsets) => {
            validate_output_offsets(offsets, output_width)?;
            offsets.to_vec()
        }
        None => (0..output_width)
            .map(|offset| {
                u32::try_from(offset).expect("executor output width fits TiKV u32 offsets")
            })
            .collect(),
    };
    let mut executors = vec![scan_executor];
    match selection {
        SelectionSource::None => {}
        SelectionSource::Conditions(conditions) => {
            executors.push(selection_executor(conditions.to_vec())?);
        }
    }
    if let Some((aggregation, _)) = aggregation {
        executors.push(aggregation_to_executor(aggregation));
    } else if let Some(functions) = aggregate_functions {
        executors.push(aggregation_to_pb(
            functions,
            aggregate_group_by.unwrap_or_default(),
            aggregate_streamed,
        )?);
    }
    if let Some(limit) = limit {
        executors.push(limit_to_pb(limit));
    }
    let (encode_type, chunk_memory_layout) = match context.encode_type {
        DistSqlEncodeType::Default => (EncodeType::TypeDefault, None),
        DistSqlEncodeType::Chunk => {
            let endian = match system_endian() {
                SystemEndian::Little => Endian::LittleEndian,
                SystemEndian::Big => Endian::BigEndian,
            };
            (
                EncodeType::TypeChunk,
                Some(ChunkMemoryLayout {
                    endian: Some(endian as i32),
                }),
            )
        }
    };

    Ok(DagRequest {
        executors,
        time_zone_offset: Some(context.time_zone_offset),
        flags: Some(context.push_down_flags),
        output_offsets,
        encode_type: Some(encode_type as i32),
        time_zone_name: Some(context.time_zone_name.clone()),
        collect_execution_summaries: context.collect_execution_summaries.then_some(true),
        chunk_memory_layout,
        div_precision_increment: (context.div_precision_increment
            != DEFAULT_DIV_PRECISION_INCREMENT)
            .then_some(context.div_precision_increment),
        root_executor: None,
        ..Default::default()
    })
}

fn validate_output_offsets(offsets: &[u32], width: usize) -> Result<(), DagRequestBuildError> {
    for &offset in offsets {
        if usize::try_from(offset).expect("u32 fits usize") >= width {
            return Err(DagRequestBuildError::OutputOffsetOutOfRange { offset, width });
        }
    }
    Ok(())
}

fn selection_executor(conditions: Vec<Expr>) -> Result<Executor, DagRequestBuildError> {
    if conditions.is_empty() {
        return Err(DagRequestBuildError::EmptySelection);
    }
    Ok(Executor {
        tp: Some(ExecType::TypeSelection as i32),
        tbl_scan: None,
        idx_scan: None,
        selection: Some(Box::new(Selection {
            conditions,
            ..Default::default()
        })),
        aggregation: None,
        top_n: None,
        limit: None,
        // PhysicalSelection.ToPB always takes the address of executorID; it
        // remains empty for TiKV's list form.
        executor_id: Some(String::new()),
        parent_idx: None,
        exchange_sender: None,
        ..Default::default()
    })
}

fn aggregation_to_pb(
    functions: &[Expr],
    group_by: &[Expr],
    streamed: bool,
) -> Result<Executor, DagRequestBuildError> {
    if functions.is_empty() && group_by.is_empty() {
        return Err(DagRequestBuildError::EmptyAggregation);
    }
    Ok(Executor {
        tp: Some(if streamed {
            ExecType::TypeStreamAgg
        } else {
            ExecType::TypeAggregation
        } as i32),
        tbl_scan: None,
        idx_scan: None,
        selection: None,
        aggregation: Some(Box::new(Aggregation {
            group_by: group_by.to_vec(),
            agg_func: functions.to_vec(),
            // Go's PhysicalHashAgg/PhysicalStreamAgg list-form protobuf does
            // not set Aggregation.streamed; the executor type is the mode
            // discriminator in this list-form request.
            streamed: None,
            ..Default::default()
        })),
        top_n: None,
        limit: None,
        executor_id: Some(String::new()),
        parent_idx: None,
        exchange_sender: None,
        ..Default::default()
    })
}

fn table_scan_to_pb(spec: &TiKvTableScanSpec) -> Result<Executor, DagRequestBuildError> {
    if let Some(feature) = spec.unsupported {
        return Err(DagRequestBuildError::UnsupportedScanFeature(feature));
    }
    if spec.columns.iter().any(|column| column.array) {
        return Err(DagRequestBuildError::UnsupportedScanFeature(
            UnsupportedScanFeature::MultiValuedIndex,
        ));
    }
    Ok(Executor {
        tp: Some(ExecType::TypeTableScan as i32),
        tbl_scan: Some(TableScan {
            table_id: Some(spec.table_id),
            columns: spec.columns.iter().map(column_to_pb).collect(),
            desc: Some(spec.desc),
            primary_column_ids: spec.primary_column_ids.clone(),
            next_read_engine: Some(EngineType::Local as i32),
            primary_prefix_column_ids: spec.primary_prefix_column_ids.clone(),
            keep_order: Some(spec.keep_order),
            is_fast_scan: Some(false),
            max_wait_time_ms: Some(0),
            ..Default::default()
        }),
        idx_scan: None,
        selection: None,
        aggregation: None,
        top_n: None,
        limit: None,
        // PhysicalTableScan.ToPB takes the address of its initially empty ID
        // even for TiKV, so field 10 is present with an empty string.
        executor_id: Some(String::new()),
        parent_idx: None,
        exchange_sender: None,
        ..Default::default()
    })
}

fn index_scan_to_pb(plan: &PhysicalIndexScan) -> Result<Executor, DagRequestBuildError> {
    let spec = plan
        .pushdown()
        .ok_or(DagRequestBuildError::MissingIndexPushdown)?;
    if let Some(feature) = spec.unsupported {
        return Err(DagRequestBuildError::UnsupportedScanFeature(feature));
    }
    if spec.columns.iter().any(|column| column.array) {
        return Err(DagRequestBuildError::UnsupportedScanFeature(
            UnsupportedScanFeature::MultiValuedIndex,
        ));
    }
    Ok(Executor {
        tp: Some(ExecType::TypeIndexScan as i32),
        tbl_scan: None,
        idx_scan: Some(index_payload_to_pb(spec, plan)),
        selection: None,
        aggregation: None,
        top_n: None,
        limit: None,
        executor_id: None,
        parent_idx: None,
        exchange_sender: None,
        ..Default::default()
    })
}

fn index_payload_to_pb(spec: &TiKvIndexScanSpec, plan: &PhysicalIndexScan) -> IndexScan {
    IndexScan {
        table_id: Some(spec.table_id),
        index_id: Some(spec.index_id),
        columns: spec.columns.iter().map(column_to_pb).collect(),
        desc: Some(spec.desc),
        unique: Some(check_cover_index(
            spec.declared_unique,
            spec.index_column_count,
            plan.ranges(),
        )),
        primary_column_ids: spec.primary_column_ids.clone(),
        ..Default::default()
    }
}

/// Go `util.ColumnToProto`'s `tipb.ColumnInfo` for one scan descriptor.
pub fn column_to_pb(column: &ScanColumnInfo) -> ColumnInfo {
    ColumnInfo {
        column_id: Some(column.column_id),
        tp: Some(column.tp),
        collation: Some(column.collation),
        column_len: Some(column.column_len),
        decimal: Some(column.decimal),
        flag: Some(column.flag),
        elems: column.elems.clone(),
        default_val: column.default_val.clone(),
        pk_handle: Some(column.pk_handle),
        array: Some(column.array),
    }
}
