// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeSet;

use super::jobs::AnalysisJobRuntime;
use super::model::{RuntimeResult, SqlStatement, TableMeta, TableStats};
use crate::PriorityHeapItem;

/// Session variables read by the Go factory and DDL handler.
pub trait SessionPort {
    fn analyze_version(&self) -> i32;
    fn auto_analyze_ratio(&self) -> f64;
    fn auto_analyze_enabled(&self) -> bool;
    fn dynamic_partition_pruning(&self) -> bool;
}

/// InfoSchema lookups used during validation and DDL recreation.
pub trait InfoSchemaPort {
    fn table_by_id(&self, physical_or_table_id: i64) -> Option<TableMeta>;
}

/// Statistics cache and lock-table lookups.
pub trait StatisticsPort {
    fn stats_by_id(&self, physical_id: i64) -> Option<TableStats>;
    fn locked_table_ids(&self) -> RuntimeResult<BTreeSet<i64>>;
    fn update_after_analyze(&mut self, physical_id: i64) -> RuntimeResult<()>;
}

/// SQL execution/query seam. Concrete TiDB sessions remain external.
pub trait SqlPort {
    fn query_optional_f64(&mut self, statement: &SqlStatement) -> RuntimeResult<Option<f64>>;
    fn query_optional_i64(&mut self, statement: &SqlStatement) -> RuntimeResult<Option<i64>>;
    fn execute(&mut self, statement: &SqlStatement) -> RuntimeResult<()>;
}

/// Clock/TSO projection used by factory duration calculations.
pub trait ClockPort {
    fn now_timestamp_nanos(&self) -> i64;
}

/// Success/failure hook publication used by concrete jobs.
pub trait JobHookPort {
    fn success(&mut self, job: &AnalysisJobRuntime);
    fn failure(&mut self, job: &AnalysisJobRuntime, need_retry: bool);
}

/// Narrow queue mutation seam consumed by DDL handling.
pub trait QueueMutationPort {
    fn is_initialized(&self) -> bool;
    fn remove(&self, table_id: i64) -> RuntimeResult<()>;
    fn upsert(&self, job: PriorityHeapItem, locked: &BTreeSet<i64>) -> RuntimeResult<()>;
}
