// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

mod dynamic_partitioned;
mod non_partitioned;
mod static_partitioned;

pub use dynamic_partitioned::{flatten_partition_names, get_partition_sql, DynamicPartitionedJob};
pub use non_partitioned::NonPartitionedJob;
pub use static_partitioned::StaticPartitionedJob;

use super::calculator::calculate_weight;
use super::interval::{
    average_analysis_duration, last_failed_analysis_duration, JUST_FAILED, NO_RECORD,
};
use super::model::{JobIndicators, RuntimeResult};
use super::ports::{InfoSchemaPort, JobHookPort, SqlPort, StatisticsPort};
use crate::{AnalysisIndicators, AnalysisJobKind, PriorityHeapItem};

pub const TABLE_NOT_EXIST: &str = "table does not exist";
pub const SCHEMA_NOT_EXIST: &str = "schema does not exist";
pub const NOT_PARTITIONED_TABLE: &str = "table is not a partitioned table";
pub const PARTITION_NOT_EXIST: &str = "partition does not exist";
pub const DEFAULT_FAILED_ANALYSIS_WAIT_NANOS: i64 = 30 * 60 * 1_000_000_000;

/// One of the three concrete source job families.
#[derive(Clone, Debug, PartialEq)]
pub enum AnalysisJobRuntime {
    NonPartitioned(NonPartitionedJob),
    DynamicPartitioned(DynamicPartitionedJob),
    StaticPartitioned(StaticPartitionedJob),
}

impl AnalysisJobRuntime {
    #[must_use]
    pub fn table_id(&self) -> i64 {
        match self {
            Self::NonPartitioned(job) => job.table_id,
            Self::DynamicPartitioned(job) => job.global_table_id,
            Self::StaticPartitioned(job) => job.partition_id,
        }
    }

    #[must_use]
    pub fn has_new_index(&self) -> bool {
        match self {
            Self::NonPartitioned(job) => !job.index_ids.is_empty(),
            Self::DynamicPartitioned(job) => !job.partition_index_ids.is_empty(),
            Self::StaticPartitioned(job) => !job.index_ids.is_empty(),
        }
    }

    #[must_use]
    pub fn indicators(&self) -> JobIndicators {
        match self {
            Self::NonPartitioned(job) => job.indicators,
            Self::DynamicPartitioned(job) => job.indicators,
            Self::StaticPartitioned(job) => job.indicators,
        }
    }

    pub fn set_weight(&mut self, weight: f64) {
        match self {
            Self::NonPartitioned(job) => job.weight = weight,
            Self::DynamicPartitioned(job) => job.weight = weight,
            Self::StaticPartitioned(job) => job.weight = weight,
        }
    }

    #[must_use]
    pub fn weight(&self) -> f64 {
        match self {
            Self::NonPartitioned(job) => job.weight,
            Self::DynamicPartitioned(job) => job.weight,
            Self::StaticPartitioned(job) => job.weight,
        }
    }

    pub fn calculate_and_set_weight(&mut self) {
        self.set_weight(calculate_weight(self.indicators(), self.has_new_index()));
    }

    #[must_use]
    pub fn heap_item(&self) -> PriorityHeapItem {
        let indicators = self.indicators();
        let metadata = AnalysisIndicators {
            change_percentage: indicators.change_percentage,
            table_size: indicators.table_size,
            last_analysis_duration_nanos: indicators.last_analysis_duration_nanos,
        };
        match self {
            Self::NonPartitioned(job) => PriorityHeapItem::new(job.table_id, job.weight)
                .with_job_metadata(AnalysisJobKind::NonPartitioned, metadata),
            Self::DynamicPartitioned(job) => PriorityHeapItem::new(job.global_table_id, job.weight)
                .with_job_metadata(AnalysisJobKind::DynamicPartitioned, metadata),
            Self::StaticPartitioned(job) => PriorityHeapItem::new_static_partition(
                job.global_table_id,
                job.partition_id,
                job.weight,
            )
            .with_job_metadata(AnalysisJobKind::StaticPartitioned, metadata),
        }
    }

    pub fn validate_and_prepare(
        &mut self,
        info: &impl InfoSchemaPort,
        sql: &mut impl SqlPort,
        hooks: &mut impl JobHookPort,
    ) -> RuntimeResult<(bool, String)> {
        let result = match self {
            Self::NonPartitioned(job) => job.validate_and_prepare(info, sql),
            Self::DynamicPartitioned(job) => job.validate_and_prepare(info, sql),
            Self::StaticPartitioned(job) => job.validate_and_prepare(info, sql),
        }?;
        if !result.0 {
            hooks.failure(
                self,
                result.1 != TABLE_NOT_EXIST
                    && result.1 != SCHEMA_NOT_EXIST
                    && result.1 != NOT_PARTITIONED_TABLE
                    && result.1 != PARTITION_NOT_EXIST,
            );
        }
        Ok(result)
    }

    pub fn analyze(
        &self,
        sql: &mut impl SqlPort,
        stats: &mut impl StatisticsPort,
        hooks: &mut impl JobHookPort,
        partition_batch_size: usize,
    ) -> RuntimeResult<()> {
        let execution_result = match self {
            Self::NonPartitioned(job) => job.analyze(sql),
            Self::DynamicPartitioned(job) => job.analyze(sql, partition_batch_size),
            Self::StaticPartitioned(job) => job.analyze(sql),
        };
        let result = execution_result.and_then(|()| stats.update_after_analyze(self.table_id()));
        match result {
            Ok(()) => {
                hooks.success(self);
                Ok(())
            }
            Err(error) => {
                hooks.failure(self, true);
                Err(error)
            }
        }
    }
}

pub(super) fn valid_to_analyze(
    sql: &mut impl SqlPort,
    schema: &str,
    table: &str,
    partitions: &[String],
) -> RuntimeResult<(bool, String)> {
    let failed = match last_failed_analysis_duration(sql, schema, table, partitions) {
        Ok(value) => value,
        Err(error) => {
            return Ok((
                false,
                format!("fail to get last failed analysis duration: {}", error.0),
            ))
        }
    };
    let average = match average_analysis_duration(sql, schema, table, partitions) {
        Ok(value) => value,
        Err(error) => {
            return Ok((
                false,
                format!("fail to get average analysis duration: {}", error.0),
            ))
        }
    };
    if failed == JUST_FAILED {
        return Ok((false, "last analysis just failed".to_owned()));
    }
    if failed != NO_RECORD && average == NO_RECORD && failed < DEFAULT_FAILED_ANALYSIS_WAIT_NANOS {
        return Ok((
            false,
            "last failed analysis duration is less than 30m".to_owned(),
        ));
    }
    if failed != NO_RECORD && failed < 2 * average {
        return Ok((
            false,
            "last failed analysis duration is less than 2 times the average analysis duration"
                .to_owned(),
        ));
    }
    Ok((true, String::new()))
}
