// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use super::jobs::{
    AnalysisJobRuntime, DynamicPartitionedJob, NonPartitionedJob, StaticPartitionedJob,
};
use super::model::{JobIndicators, PartitionIndexMap, PartitionStats, TableMeta, TableStats};
use super::ports::{ClockPort, SessionPort};

/// Daily UTC-minute window used by the source factory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AutoAnalysisTimeWindow {
    pub start: Option<u16>,
    pub end: Option<u16>,
}

impl AutoAnalysisTimeWindow {
    #[must_use]
    pub const fn new(start: Option<u16>, end: Option<u16>) -> Self {
        Self { start, end }
    }

    #[must_use]
    pub const fn contains(self, current: u16) -> bool {
        let (Some(start), Some(end)) = (self.start, self.end) else {
            return false;
        };
        if end >= start {
            current >= start && current <= end
        } else {
            current >= start || current <= end
        }
    }

    #[must_use]
    pub const fn is_within_time_window(self, current: u16) -> bool {
        self.contains(current)
    }

    #[must_use]
    pub const fn start(self) -> Option<u16> {
        self.start
    }

    #[must_use]
    pub const fn end(self) -> Option<u16> {
        self.end
    }
}

/// Concrete source job factory over injected session and clock state.
pub struct AnalysisJobFactory<'a, S, C> {
    session: &'a S,
    clock: &'a C,
}

impl<'a, S: SessionPort, C: ClockPort> AnalysisJobFactory<'a, S, C> {
    pub const fn new(session: &'a S, clock: &'a C) -> Self {
        Self { session, clock }
    }

    #[must_use]
    pub fn change_percentage(&self, stats: &TableStats) -> f64 {
        if !stats.analyzed {
            return 1.0;
        }
        let ratio = self.session.auto_analyze_ratio();
        if ratio == 0.0 {
            return 0.0;
        }
        let denominator = if stats.analyze_row_count > 0 {
            stats.analyze_row_count
        } else {
            stats.realtime_count
        };
        let change = stats.modify_count as f64 / denominator as f64;
        if change > ratio {
            change
        } else {
            0.0
        }
    }

    #[must_use]
    pub fn table_size(stats: &TableStats) -> f64 {
        assert_ne!(stats.column_count, 0, "column count must not be zero");
        stats.realtime_count as f64 * stats.column_count as f64
    }

    #[must_use]
    pub fn last_analyze_duration(&self, stats: &TableStats) -> i64 {
        if stats.analyzed {
            self.clock.now_timestamp_nanos() - stats.last_analyze_timestamp_nanos
        } else {
            30 * 60 * 1_000_000_000
        }
    }

    #[must_use]
    pub fn indexes_needing_analyze(table: &TableMeta, stats: &TableStats) -> BTreeSet<i64> {
        if !stats.analyzed {
            return BTreeSet::new();
        }
        table
            .indexes
            .iter()
            .filter(|index| {
                index.public
                    && !index.columnar
                    && !stats.present_index_stats.contains(&index.id)
                    && !stats.analyzed_index_markers.contains(&index.id)
            })
            .map(|index| index.id)
            .collect()
    }

    #[must_use]
    pub fn create_non_partitioned(
        &self,
        table: &TableMeta,
        stats: &TableStats,
    ) -> Option<AnalysisJobRuntime> {
        if !stats.eligible {
            return None;
        }
        let change = self.change_percentage(stats);
        let indexes = Self::indexes_needing_analyze(table, stats);
        if change == 0.0 && indexes.is_empty() {
            return None;
        }
        Some(AnalysisJobRuntime::NonPartitioned(NonPartitionedJob {
            table_id: table.id,
            index_ids: indexes,
            table_stats_version: self.session.analyze_version(),
            need_version_rewrite_warning: stats.analyze_version != self.session.analyze_version(),
            indicators: JobIndicators {
                change_percentage: change,
                table_size: Self::table_size(stats),
                last_analysis_duration_nanos: self.last_analyze_duration(stats),
            },
            weight: 0.0,
            schema_name: String::new(),
            table_name: String::new(),
            index_names: Vec::new(),
        }))
    }

    #[must_use]
    pub fn create_static_partition(
        &self,
        table: &TableMeta,
        partition_id: i64,
        stats: &TableStats,
    ) -> Option<AnalysisJobRuntime> {
        if !stats.eligible {
            return None;
        }
        let change = self.change_percentage(stats);
        let indexes = Self::indexes_needing_analyze(table, stats);
        if change == 0.0 && indexes.is_empty() {
            return None;
        }
        Some(AnalysisJobRuntime::StaticPartitioned(
            StaticPartitionedJob {
                global_table_id: table.id,
                partition_id,
                index_ids: indexes,
                table_stats_version: self.session.analyze_version(),
                need_version_rewrite_warning: stats.analyze_version
                    != self.session.analyze_version(),
                indicators: JobIndicators {
                    change_percentage: change,
                    table_size: Self::table_size(stats),
                    last_analysis_duration_nanos: self.last_analyze_duration(stats),
                },
                weight: 0.0,
                schema_name: String::new(),
                table_name: String::new(),
                partition_name: String::new(),
                index_names: Vec::new(),
            },
        ))
    }

    #[must_use]
    pub fn partition_indicators(
        &self,
        global: &TableStats,
        partitions: &[PartitionStats],
    ) -> (JobIndicators, BTreeSet<i64>) {
        let selected: Vec<_> = partitions
            .iter()
            .filter_map(|partition| {
                let change = self.change_percentage(&partition.stats);
                (change != 0.0).then_some((partition, change))
            })
            .collect();
        if selected.is_empty() {
            return (JobIndicators::default(), BTreeSet::new());
        }
        let count = selected.len() as f64;
        let indicators = JobIndicators {
            change_percentage: selected.iter().map(|(_, change)| *change).sum::<f64>() / count,
            table_size: selected
                .iter()
                .map(|(partition, _)| {
                    partition.stats.realtime_count as f64 * global.column_count as f64
                })
                .sum::<f64>()
                / count,
            last_analysis_duration_nanos: selected
                .iter()
                .map(|(partition, _)| self.last_analyze_duration(&partition.stats))
                .sum::<i64>()
                / selected.len() as i64,
        };
        (
            indicators,
            selected
                .into_iter()
                .map(|(partition, _)| partition.partition.id)
                .collect(),
        )
    }

    #[must_use]
    pub fn partition_indexes_needing_analyze(
        table: &TableMeta,
        partitions: &[PartitionStats],
    ) -> PartitionIndexMap {
        let mut result = BTreeMap::new();
        for index in &table.indexes {
            if !index.public || index.columnar || index.special_global {
                continue;
            }
            let ids: Vec<_> = partitions
                .iter()
                .filter(|partition| {
                    !partition.stats.present_index_stats.contains(&index.id)
                        && !partition.stats.analyzed_index_markers.contains(&index.id)
                })
                .map(|partition| partition.partition.id)
                .collect();
            if !ids.is_empty() {
                result.insert(index.id, ids);
            }
        }
        result
    }

    #[must_use]
    pub fn create_dynamic_partitioned(
        &self,
        table: &TableMeta,
        global: &TableStats,
        partitions: &[PartitionStats],
    ) -> Option<AnalysisJobRuntime> {
        if !global.eligible {
            return None;
        }
        let (indicators, partition_ids) = self.partition_indicators(global, partitions);
        let partition_index_ids = Self::partition_indexes_needing_analyze(table, partitions);
        if partition_ids.is_empty() && partition_index_ids.is_empty() {
            return None;
        }
        let versions_match = global.analyze_version == self.session.analyze_version()
            && partitions
                .iter()
                .all(|partition| partition.stats.analyze_version == self.session.analyze_version());
        Some(AnalysisJobRuntime::DynamicPartitioned(
            DynamicPartitionedJob {
                global_table_id: table.id,
                partition_ids,
                partition_index_ids,
                table_stats_version: self.session.analyze_version(),
                need_version_rewrite_warning: !versions_match,
                indicators,
                weight: 0.0,
                schema_name: String::new(),
                table_name: String::new(),
                partition_names: Vec::new(),
                partition_index_names: BTreeMap::new(),
            },
        ))
    }
}
