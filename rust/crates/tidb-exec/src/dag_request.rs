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
//! `PhysicalTableScan.ToPB` and `PhysicalIndexScan.ToPB`. It accepts exactly
//! one already-built TiKV scan and constructs no ranges, reader, region task,
//! or RPC transport.

use std::{error::Error, fmt};

use tidb_distsql::{system_endian, EncodeType as DistSqlEncodeType, SystemEndian};
use tidb_planner::{
    physical_index_scan::PhysicalIndexScanPlan,
    physical_table_scan::PhysicalTableScanPlan,
    scan_pushdown::{
        check_cover_index, ScanColumnInfo, TiKvIndexScanSpec, TiKvTableScanSpec,
        UnsupportedScanFeature,
    },
};
use tidb_proto::tipb::{
    ChunkMemoryLayout, ColumnInfo, DagRequest, EncodeType, Endian, EngineType, ExecType, Executor,
    IndexScan, TableScan,
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
        }
    }
}

impl Error for DagRequestBuildError {}

/// Ports the TiKV list branch of Go `ConstructDAGReq` for one physical scan.
pub fn construct_dag_req(
    context: &DagRequestContext,
    plans: &[TiKvScanPlan<'_>],
) -> Result<DagRequest, DagRequestBuildError> {
    let [plan] = plans else {
        return Err(DagRequestBuildError::PlanCount {
            actual: plans.len(),
        });
    };

    let executor = match plan {
        TiKvScanPlan::Table(plan) => table_scan_to_pb(plan.pushdown())?,
        TiKvScanPlan::Index(plan) => index_scan_to_pb(plan)?,
    };
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
        executors: vec![executor],
        time_zone_offset: Some(context.time_zone_offset),
        flags: Some(context.push_down_flags),
        encode_type: Some(encode_type as i32),
        time_zone_name: Some(context.time_zone_name.clone()),
        collect_execution_summaries: context.collect_execution_summaries.then_some(true),
        chunk_memory_layout,
        div_precision_increment: (context.div_precision_increment
            != DEFAULT_DIV_PRECISION_INCREMENT)
            .then_some(context.div_precision_increment),
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
        }),
        idx_scan: None,
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
