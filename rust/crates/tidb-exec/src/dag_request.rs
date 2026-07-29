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

use tidb_ast::BinaryOp;
use tidb_distsql::{system_endian, EncodeType as DistSqlEncodeType, SystemEndian};
use tidb_expr::pb_comparison::{
    signed_bigint_comparison_to_pb, PbComparisonError, SignedBigIntPbOperand,
};
use tidb_planner::{
    physical_index_scan::PhysicalIndexScanPlan,
    physical_selection::{ComparisonOp, ComparisonOperand, PhysicalSelectionPlan},
    physical_table_scan::PhysicalTableScanPlan,
    scan_pushdown::{
        check_cover_index, ScanColumnInfo, TiKvIndexScanSpec, TiKvTableScanSpec,
        UnsupportedScanFeature,
    },
};
use tidb_proto::tipb::{
    ChunkMemoryLayout, ColumnInfo, DagRequest, EncodeType, Endian, EngineType, ExecType, Executor,
    IndexScan, Limit, Selection, TableScan,
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
    Table(&'a PhysicalTableScanPlan),
    /// Ordinary index scan.
    Index(&'a PhysicalIndexScanPlan),
}

/// Why a physical scan cannot enter the bounded TiKV DAG request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DagRequestBuildError {
    /// This closure requires one source scan and no parent executors.
    PlanCount {
        /// Number of plans supplied by the caller.
        actual: usize,
    },
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
    /// A Selection condition refers outside the scan output.
    ConditionInputOffsetOutOfRange {
        /// Invalid resolved input offset.
        offset: u32,
        /// Number of columns emitted by the scan.
        width: usize,
    },
    /// Catalog field flags cannot be represented by TiPB's unsigned field.
    InvalidColumnFlags {
        /// Resolved scan-input offset.
        offset: u32,
        /// Invalid signed scan metadata.
        flags: i32,
    },
    /// A metadata-only Selection cannot enter the executable TiKV DAG.
    EmptySelection,
    /// The bounded expression owner rejected a condition.
    Expression(PbComparisonError),
}

impl fmt::Display for DagRequestBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PlanCount { actual } => {
                write!(f, "TiKV scan DAG requires exactly one plan, got {actual}")
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
            Self::ConditionInputOffsetOutOfRange { offset, width } => write!(
                f,
                "TiKV Selection input offset {offset} is outside scan width {width}"
            ),
            Self::InvalidColumnFlags { offset, flags } => write!(
                f,
                "TiKV Selection input offset {offset} has invalid column flags {flags}"
            ),
            Self::EmptySelection => {
                f.write_str("TiKV Selection requires at least one resolved condition")
            }
            Self::Expression(error) => write!(f, "cannot lower TiKV Selection expression: {error}"),
        }
    }
}

impl Error for DagRequestBuildError {}

impl From<PbComparisonError> for DagRequestBuildError {
    fn from(error: PbComparisonError) -> Self {
        Self::Expression(error)
    }
}

/// Ports the TiKV list branch of Go `ConstructDAGReq` for one physical scan.
pub fn construct_dag_req(
    context: &DagRequestContext,
    plans: &[TiKvScanPlan<'_>],
) -> Result<DagRequest, DagRequestBuildError> {
    construct_dag_req_inner(context, plans, None, None, None)
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
        limit: Some(Limit { limit: Some(limit) }),
        executor_id: Some(String::new()),
        parent_idx: None,
    }
}

/// Ports Go's TiKV list DAG for one scan and an optional physical Selection.
///
/// `output_offsets` is the final reader projection, not the scan width. This
/// lets a predicate-only scan input participate in Selection without leaking
/// into the MySQL result row. Duplicate offsets remain valid, matching SQL
/// projections such as `SELECT a, a`.
pub fn construct_read_only_dag_req(
    context: &DagRequestContext,
    scan: TiKvScanPlan<'_>,
    selection: Option<&PhysicalSelectionPlan>,
    output_offsets: &[u32],
) -> Result<DagRequest, DagRequestBuildError> {
    construct_dag_req_inner(context, &[scan], selection, Some(output_offsets), None)
}

/// [`construct_read_only_dag_req`] with a coprocessor-side row cap appended
/// above the Selection, which is the executor list Go builds for a `LIMIT`
/// pushed into a TiKV reader.
pub fn construct_capped_read_only_dag_req(
    context: &DagRequestContext,
    scan: TiKvScanPlan<'_>,
    selection: Option<&PhysicalSelectionPlan>,
    limit: Option<u64>,
    output_offsets: &[u32],
) -> Result<DagRequest, DagRequestBuildError> {
    construct_dag_req_inner(context, &[scan], selection, Some(output_offsets), limit)
}

fn construct_dag_req_inner(
    context: &DagRequestContext,
    plans: &[TiKvScanPlan<'_>],
    selection: Option<&PhysicalSelectionPlan>,
    requested_output_offsets: Option<&[u32]>,
    limit: Option<u64>,
) -> Result<DagRequest, DagRequestBuildError> {
    let [plan] = plans else {
        return Err(DagRequestBuildError::PlanCount {
            actual: plans.len(),
        });
    };

    let (scan_executor, scan_columns) = match plan {
        TiKvScanPlan::Table(plan) => (
            table_scan_to_pb(plan.pushdown())?,
            plan.pushdown().columns.as_slice(),
        ),
        TiKvScanPlan::Index(plan) => (
            index_scan_to_pb(plan)?,
            plan.pushdown()
                .ok_or(DagRequestBuildError::MissingIndexPushdown)?
                .columns
                .as_slice(),
        ),
    };
    let output_offsets = match requested_output_offsets {
        Some(offsets) => {
            validate_output_offsets(offsets, scan_columns.len())?;
            offsets.to_vec()
        }
        None => (0..scan_columns.len())
            .map(|offset| {
                u32::try_from(offset).expect("executor output width fits TiKV u32 offsets")
            })
            .collect(),
    };
    let mut executors = vec![scan_executor];
    if let Some(selection) = selection {
        executors.push(selection_to_pb(selection, scan_columns)?);
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

fn selection_to_pb(
    plan: &PhysicalSelectionPlan,
    scan_columns: &[ScanColumnInfo],
) -> Result<Executor, DagRequestBuildError> {
    if plan.conditions().is_empty() {
        return Err(DagRequestBuildError::EmptySelection);
    }
    let conditions = plan
        .conditions()
        .iter()
        .map(|condition| {
            let left = comparison_operand_to_pb(condition.lhs(), scan_columns)?;
            let right = comparison_operand_to_pb(condition.rhs(), scan_columns)?;
            signed_bigint_comparison_to_pb(comparison_op(condition.op()), left, right)
                .map_err(DagRequestBuildError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Executor {
        tp: Some(ExecType::TypeSelection as i32),
        tbl_scan: None,
        idx_scan: None,
        selection: Some(Selection { conditions }),
        limit: None,
        // PhysicalSelection.ToPB always takes the address of executorID; it
        // remains empty for TiKV's list form.
        executor_id: Some(String::new()),
        parent_idx: None,
    })
}

const fn comparison_op(operator: ComparisonOp) -> BinaryOp {
    match operator {
        ComparisonOp::Lt => BinaryOp::Lt,
        ComparisonOp::Le => BinaryOp::Le,
        ComparisonOp::Gt => BinaryOp::Gt,
        ComparisonOp::Ge => BinaryOp::Ge,
        ComparisonOp::Eq => BinaryOp::Eq,
        ComparisonOp::Ne => BinaryOp::Ne,
    }
}

fn comparison_operand_to_pb(
    operand: ComparisonOperand,
    scan_columns: &[ScanColumnInfo],
) -> Result<SignedBigIntPbOperand, DagRequestBuildError> {
    match operand {
        ComparisonOperand::Int(value) => Ok(SignedBigIntPbOperand::Literal(value)),
        ComparisonOperand::InputOffset(offset) => {
            let column = scan_columns.get(offset as usize).ok_or(
                DagRequestBuildError::ConditionInputOffsetOutOfRange {
                    offset,
                    width: scan_columns.len(),
                },
            )?;
            let flags = u32::try_from(column.flag).map_err(|_| {
                DagRequestBuildError::InvalidColumnFlags {
                    offset,
                    flags: column.flag,
                }
            })?;
            Ok(SignedBigIntPbOperand::Column {
                offset: offset as usize,
                flags,
            })
        }
    }
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
        }),
        idx_scan: None,
        selection: None,
        limit: None,
        // PhysicalTableScan.ToPB takes the address of its initially empty ID
        // even for TiKV, so field 10 is present with an empty string.
        executor_id: Some(String::new()),
        parent_idx: None,
    })
}

fn index_scan_to_pb(plan: &PhysicalIndexScanPlan) -> Result<Executor, DagRequestBuildError> {
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
        limit: None,
        executor_id: None,
        parent_idx: None,
    })
}

fn index_payload_to_pb(spec: &TiKvIndexScanSpec, plan: &PhysicalIndexScanPlan) -> IndexScan {
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
    }
}

fn column_to_pb(column: &ScanColumnInfo) -> ColumnInfo {
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
