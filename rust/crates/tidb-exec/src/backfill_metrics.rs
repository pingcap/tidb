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

//! Go `pkg/ddl/backfill_metrics.go` (87 production lines): the pure
//! branching logic that decides which table ID a backfill-progress metric is
//! keyed and cleaned up on. This file is a SEED of the much larger `pkg/ddl`
//! package, ported to completion for its own small unit.
//!
//! Every symbol is ported EXCEPT the two thinnest wrappers, named below.
//!
//! # Narrowings
//!
//! - `// boundary:` `pkg/metrics` (Prometheus counter/gauge registration,
//!   collection and `DDLClearBackfillMetrics` cleanup) is not ported to this
//!   crate -- it is a large, separate Prometheus-integration unit with no
//!   `pkg/ddl`-owned branching logic of its own. Go's `getBackfillTotalByTableID`
//!   and `getBackfillProgressByTableID` are one-line delegations to it
//!   (`metrics.GetBackfillTotalByTableID`/`GetBackfillProgressByTableID`) and
//!   are DROPPED rather than ported as no-ops; [`labels`] carries just the
//!   bare label strings this file's own logic (`backfill_progress_label`,
//!   `is_partition_reorg_backfill_metric_label`) needs, copied by value from
//!   their Go source (`pkg/metrics/ddl.go`).
//! - `// boundary:` Go `reorgInfo` (`pkg/ddl/reorg.go`) is a large struct
//!   (`StartKey`, `EndKey`, `jobCtx`, `elements`, ...) far outside this file's
//!   unit; only three of its fields are read here. [`BackfillReorgInfo`]
//!   narrows it to exactly those three -- `Job` (Go's anonymously embedded
//!   `*model.Job`) and `PhysicalTableID`, plus the promoted `Type` field as
//!   the [`BackfillReorgInfo::action_type`] accessor below.
//!
//! # Test coverage and labeling
//!
//! Source: `pkg/ddl/backfill_metrics_test.go`, 3 Go test functions (in Go
//! package `ddl`, i.e. an internal test file reaching the unexported
//! functions directly). All three exercise, and mostly ASSERT ON, the
//! dropped `pkg/metrics` Prometheus registration/cleanup machinery
//! (`metrics.GetBackfillLabelsForTest`, `metrics.DDLClearBackfillMetrics`,
//! real `prometheus.Collector` iteration) rather than this file's own
//! branching logic:
//!
//! - `TestBackfillMetricsCleanupByTableID` and `TestBackfillMetricsIdempotentCleanup`
//!   are ENTIRELY about that dropped subsystem end to end (register a
//!   metric, clear it by table ID, assert the Prometheus vectors reflect
//!   it) and are NOT ported.
//! - `TestBackfillMetricsCleanupPartitionedTable` mixes the same dropped
//!   registration/cleanup assertions with two nested subtests that DO
//!   exercise this file's own `backfillMetricsTableID`:
//!   `partition-reorg-rate-uses-logical-table-id`'s two `backfillMetricsTableID`
//!   assertions, and the whole of `metric-table-id-selection-audit` (a
//!   pure, table-driven `backfillMetricsTableID` test with no Prometheus
//!   dependency at all). Both are ported byte-exact below as
//!   `partition_reorg_rate_uses_logical_table_id` and
//!   `metric_table_id_selection_audit`; the Prometheus-registration/cleanup
//!   parts of the same Go test are not.

use tidb_model::action_type::ActionType;
use tidb_model::Job;

/// Go `metrics.LblXxx` string constants (`pkg/metrics/ddl.go`) that this
/// file's own logic reads. See the module boundary note: the metric-vector
/// machinery those labels key into is not ported here.
pub mod labels {
    /// Go `metrics.LblAddIndex`.
    pub const ADD_INDEX: &str = "add_index";
    /// Go `metrics.LblAddIndexMerge`.
    pub const ADD_INDEX_MERGE: &str = "add_index_merge_tmp";
    /// Go `metrics.LblModifyColumn`.
    pub const MODIFY_COLUMN: &str = "modify_column";
    /// Go `metrics.LblReorgPartition`.
    pub const REORG_PARTITION: &str = "reorganize_partition";
    /// Go `metrics.LblCleanupIdxRate`.
    pub const CLEANUP_IDX_RATE: &str = "cleanup_idx_rate";
    /// Go `metrics.LblReorgPartitionRate`.
    pub const REORG_PARTITION_RATE: &str = "reorg_partition_rate";
}

/// Go `reorgInfo`, narrowed to the three fields this file reads. See the
/// module boundary note.
#[derive(Debug, Clone, Default)]
pub struct BackfillReorgInfo {
    /// Go's anonymously embedded `*model.Job`, `None` mirroring a nil
    /// pointer.
    pub job: Option<Job>,
    /// Go `reorgInfo.PhysicalTableID`.
    pub physical_table_id: i64,
}

impl BackfillReorgInfo {
    /// Go's promoted `rInfo.Type` field access: `reorgInfo` embeds
    /// `*model.Job` anonymously, so reading `rInfo.Type` really reads
    /// `rInfo.Job.Type` and nil-pointer-dereferences exactly like this
    /// panics when `job` is `None`. Every real caller constructs a
    /// `reorgInfo` with a `Job` already attached (`getReorgInfo`), so this is
    /// not a new failure mode introduced by the port.
    fn action_type(&self) -> ActionType {
        self.job
            .as_ref()
            .expect("BackfillReorgInfo.job is None: Go's promoted `rInfo.Type` would nil-pointer-dereference here too")
            .type_
    }
}

/// Go `backfillProgressLabel`: the metric type label for one backfill job.
#[must_use]
pub fn backfill_progress_label(job_type: ActionType, merging_tmp_idx: bool) -> &'static str {
    match job_type {
        ActionType::ACTION_ADD_INDEX | ActionType::ACTION_ADD_PRIMARY_KEY => {
            if merging_tmp_idx {
                labels::ADD_INDEX_MERGE
            } else {
                labels::ADD_INDEX
            }
        }
        ActionType::ACTION_MODIFY_COLUMN => labels::MODIFY_COLUMN,
        ActionType::ACTION_REORGANIZE_PARTITION
        | ActionType::ACTION_ALTER_TABLE_PARTITIONING
        | ActionType::ACTION_REMOVE_PARTITIONING => labels::REORG_PARTITION,
        _ => "",
    }
}

/// Go `backfillMetricsTableID`: the table ID a backfill-progress metric for
/// `label` is keyed (and later cleaned up) on. `r_info: None` mirrors Go's
/// nil `*reorgInfo`.
#[must_use]
pub fn backfill_metrics_table_id(r_info: Option<&BackfillReorgInfo>, label: &str) -> i64 {
    let Some(r_info) = r_info else {
        return 0;
    };
    if !is_partition_reorg_ddl(r_info.action_type()) {
        // Cleanup index rate metrics for partition DDLs (DROP/TRUNCATE PARTITION) must
        // use the logical table ID, because the old partition physical IDs are removed
        // from Partition.Definitions after the DDL completes, so metrics keyed by them
        // can never be cleaned up by DDLClearBackfillMetrics.
        if label == labels::CLEANUP_IDX_RATE
            && is_partition_drop_or_truncate_ddl(r_info.action_type())
        {
            if let Some(job) = &r_info.job {
                return job.table_id;
            }
        }
        return r_info.physical_table_id;
    }
    let Some(job) = &r_info.job else {
        return r_info.physical_table_id;
    };
    if is_partition_reorg_backfill_metric_label(label) {
        return job.table_id;
    }
    r_info.physical_table_id
}

/// Go `isPartitionReorgDDL`.
#[must_use]
pub fn is_partition_reorg_ddl(tp: ActionType) -> bool {
    tp == ActionType::ACTION_REORGANIZE_PARTITION
        || tp == ActionType::ACTION_ALTER_TABLE_PARTITIONING
        || tp == ActionType::ACTION_REMOVE_PARTITIONING
}

/// Go `isPartitionDropOrTruncateDDL`.
#[must_use]
pub fn is_partition_drop_or_truncate_ddl(tp: ActionType) -> bool {
    tp == ActionType::ACTION_DROP_TABLE_PARTITION
        || tp == ActionType::ACTION_TRUNCATE_TABLE_PARTITION
}

/// Go `isPartitionReorgBackfillMetricLabel`.
#[must_use]
pub fn is_partition_reorg_backfill_metric_label(label: &str) -> bool {
    label == labels::REORG_PARTITION || label.starts_with(labels::REORG_PARTITION_RATE)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `tidb_model::Job` carries private synchronization fields (`mu`,
    /// `args`) alongside its public ones, so it cannot be built with a
    /// struct literal + `..Default::default()` from outside the crate; this
    /// mutates the two public fields these tests need onto a
    /// `Job::default()` instead.
    fn job_with(action_type: ActionType, table_id: i64) -> Job {
        let mut job = Job::default();
        job.type_ = action_type;
        job.table_id = table_id;
        job
    }

    /// Source: `TestBackfillMetricsCleanupPartitionedTable`'s
    /// `partition-reorg-rate-uses-logical-table-id` subtest -- just its two
    /// `backfillMetricsTableID` assertions; the rest of that subtest
    /// registers and clears real Prometheus metrics, which is out of scope
    /// (see the module boundary note).
    #[test]
    fn partition_reorg_rate_uses_logical_table_id() {
        const REORG_TABLE_ID: i64 = 200;
        const DROPPING_PART_ID: i64 = 201;

        let info = BackfillReorgInfo {
            job: Some(job_with(
                ActionType::ACTION_REORGANIZE_PARTITION,
                REORG_TABLE_ID,
            )),
            physical_table_id: DROPPING_PART_ID,
        };
        assert_eq!(
            backfill_metrics_table_id(Some(&info), labels::REORG_PARTITION_RATE),
            REORG_TABLE_ID
        );
        assert_eq!(
            backfill_metrics_table_id(
                Some(&info),
                &format!("{}-conflict", labels::REORG_PARTITION_RATE)
            ),
            REORG_TABLE_ID
        );
    }

    /// Source: `TestBackfillMetricsCleanupPartitionedTable`'s
    /// `metric-table-id-selection-audit` subtest, ported byte-exact: it is a
    /// pure `backfillMetricsTableID` table test with no Prometheus
    /// dependency.
    #[test]
    fn metric_table_id_selection_audit() {
        const LOGICAL_TABLE_ID: i64 = 300;
        const PHYSICAL_TABLE_ID: i64 = 301;

        let cases: Vec<(&str, ActionType, String, i64)> = vec![
            (
                "reorg-partition-progress",
                ActionType::ACTION_REORGANIZE_PARTITION,
                labels::REORG_PARTITION.to_owned(),
                LOGICAL_TABLE_ID,
            ),
            (
                "reorg-partition-rate",
                ActionType::ACTION_REORGANIZE_PARTITION,
                labels::REORG_PARTITION_RATE.to_owned(),
                LOGICAL_TABLE_ID,
            ),
            (
                "reorg-partition-rate-conflict",
                ActionType::ACTION_REORGANIZE_PARTITION,
                format!("{}-conflict", labels::REORG_PARTITION_RATE),
                LOGICAL_TABLE_ID,
            ),
            (
                "alter-partitioning-rate",
                ActionType::ACTION_ALTER_TABLE_PARTITIONING,
                labels::REORG_PARTITION_RATE.to_owned(),
                LOGICAL_TABLE_ID,
            ),
            (
                "remove-partitioning-progress",
                ActionType::ACTION_REMOVE_PARTITIONING,
                labels::REORG_PARTITION.to_owned(),
                LOGICAL_TABLE_ID,
            ),
            (
                "add-index-rate-keeps-physical-id",
                ActionType::ACTION_ADD_INDEX,
                "add_idx_rate".to_owned(),
                PHYSICAL_TABLE_ID,
            ),
            (
                "add-index-progress-keeps-physical-id",
                ActionType::ACTION_ADD_INDEX,
                labels::ADD_INDEX.to_owned(),
                PHYSICAL_TABLE_ID,
            ),
            (
                "merge-temp-rate-keeps-physical-id",
                ActionType::ACTION_ADD_INDEX,
                "merge_tmp_idx_rate".to_owned(),
                PHYSICAL_TABLE_ID,
            ),
            (
                "cleanup-index-rate-keeps-physical-id-for-non-partition-ddl",
                ActionType::ACTION_ADD_INDEX,
                labels::CLEANUP_IDX_RATE.to_owned(),
                PHYSICAL_TABLE_ID,
            ),
            (
                "cleanup-index-rate-uses-logical-id-for-drop-partition",
                ActionType::ACTION_DROP_TABLE_PARTITION,
                labels::CLEANUP_IDX_RATE.to_owned(),
                LOGICAL_TABLE_ID,
            ),
            (
                "cleanup-index-rate-uses-logical-id-for-truncate-partition",
                ActionType::ACTION_TRUNCATE_TABLE_PARTITION,
                labels::CLEANUP_IDX_RATE.to_owned(),
                LOGICAL_TABLE_ID,
            ),
            (
                "modify-column-rate-keeps-physical-id",
                ActionType::ACTION_MODIFY_COLUMN,
                "update_col_rate".to_owned(),
                PHYSICAL_TABLE_ID,
            ),
        ];

        for (name, action_type, label, expect_table) in cases {
            let info = BackfillReorgInfo {
                job: Some(job_with(action_type, LOGICAL_TABLE_ID)),
                physical_table_id: PHYSICAL_TABLE_ID,
            };
            assert_eq!(
                backfill_metrics_table_id(Some(&info), &label),
                expect_table,
                "case: {name}"
            );
        }
    }

    /// Not from the Go test file: `backfillMetricsTableID(nil, ...)` is
    /// reachable in this port's own type (`Option<&BackfillReorgInfo>`) even
    /// though no ported Go test drives it.
    #[test]
    fn nil_reorg_info_is_handled() {
        assert_eq!(backfill_metrics_table_id(None, labels::ADD_INDEX), 0);
    }

    /// A present `BackfillReorgInfo` with `job: None` panics, matching Go:
    /// `reorgInfo` embeds `*model.Job` anonymously, so `rInfo.Type` reads
    /// `rInfo.Job.Type` and nil-pointer-dereferences the same way. Every real
    /// caller attaches a `Job` before calling this (`getReorgInfo`), so this
    /// case is untested upstream and this port does not paper over the
    /// crash — see the doc comment on `BackfillReorgInfo::action_type`.
    #[test]
    #[should_panic(expected = "BackfillReorgInfo.job is None")]
    fn nil_job_panics_like_gos_nil_pointer_dereference() {
        let info = BackfillReorgInfo {
            job: None,
            physical_table_id: 42,
        };
        let _ = backfill_metrics_table_id(Some(&info), labels::REORG_PARTITION);
    }

    /// Not from the Go test file: `backfillProgressLabel` has no dedicated Go
    /// test of its own (only `backfillMetricsTableID` is tested in
    /// `backfill_metrics_test.go`); this pins its label-mapping switch
    /// directly against `pkg/ddl/backfill_metrics.go`'s source.
    #[test]
    fn backfill_progress_label_cases() {
        assert_eq!(
            backfill_progress_label(ActionType::ACTION_ADD_INDEX, false),
            labels::ADD_INDEX
        );
        assert_eq!(
            backfill_progress_label(ActionType::ACTION_ADD_INDEX, true),
            labels::ADD_INDEX_MERGE
        );
        assert_eq!(
            backfill_progress_label(ActionType::ACTION_ADD_PRIMARY_KEY, true),
            labels::ADD_INDEX_MERGE
        );
        assert_eq!(
            backfill_progress_label(ActionType::ACTION_MODIFY_COLUMN, false),
            labels::MODIFY_COLUMN
        );
        assert_eq!(
            backfill_progress_label(ActionType::ACTION_REORGANIZE_PARTITION, false),
            labels::REORG_PARTITION
        );
        assert_eq!(
            backfill_progress_label(ActionType::ACTION_ALTER_TABLE_PARTITIONING, false),
            labels::REORG_PARTITION
        );
        assert_eq!(
            backfill_progress_label(ActionType::ACTION_REMOVE_PARTITIONING, false),
            labels::REORG_PARTITION
        );
        assert_eq!(
            backfill_progress_label(ActionType::ACTION_DROP_TABLE_PARTITION, false),
            ""
        );
    }
}
