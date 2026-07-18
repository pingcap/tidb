// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

/// Partition pruning mode used by TiDB auto analyze.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartitionPruneMode {
    Static,
    Dynamic,
}

/// Source-shaped index metadata needed by job creation and validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexMeta {
    pub id: i64,
    pub name: String,
    pub public: bool,
    pub columnar: bool,
    pub special_global: bool,
}

/// Source-shaped partition metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionMeta {
    pub id: i64,
    pub name: String,
}

/// Source-shaped table metadata exposed by the injected InfoSchema port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableMeta {
    pub id: i64,
    pub schema_name: String,
    pub table_name: String,
    pub indexes: Vec<IndexMeta>,
    pub partitions: Vec<PartitionMeta>,
}

/// Statistics fields consumed by the Go factory.
#[derive(Clone, Debug, PartialEq)]
pub struct TableStats {
    pub physical_id: i64,
    pub eligible: bool,
    pub analyzed: bool,
    pub realtime_count: i64,
    pub modify_count: i64,
    pub analyze_row_count: i64,
    pub column_count: usize,
    pub last_analyze_timestamp_nanos: i64,
    pub analyze_version: i32,
    pub present_index_stats: BTreeSet<i64>,
    pub analyzed_index_markers: BTreeSet<i64>,
}

/// A partition ID/name and its statistics.
#[derive(Clone, Debug, PartialEq)]
pub struct PartitionStats {
    pub partition: PartitionMeta,
    pub stats: TableStats,
}

/// Common ranking inputs carried by every concrete job.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct JobIndicators {
    pub change_percentage: f64,
    pub table_size: f64,
    pub last_analysis_duration_nanos: i64,
}

/// Exact parameter value accepted by the injected SQL port.
#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Identifier(String),
    IdentifierList(Vec<String>),
}

/// SQL template plus ordered placeholder values.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlStatement {
    pub sql: String,
    pub params: Vec<SqlValue>,
}

/// Dynamic partition-index membership keyed by index ID.
pub type PartitionIndexMap = BTreeMap<i64, Vec<i64>>;

/// Runtime errors are deliberately port-owned strings; this layer does not
/// invent TiDB session or storage error identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeError(pub String);

pub type RuntimeResult<T> = Result<T, RuntimeError>;
