// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/backfilling_dist_scheduler.go`.
use tidb_dxf::*;

/// Go `CalculateRegionBatch`.
pub fn calculate_region_batch(
    total_region_cnt: usize,
    node_cnt: usize,
    use_local_disk: bool,
) -> usize {
    if let Some(value) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockRegionBatch") {
        return value.int() as usize;
    }
    let nodes = if use_local_disk {
        node_cnt.min(3)
    } else {
        node_cnt
    };
    let average = total_region_cnt.div_ceil(nodes);
    if use_local_disk {
        average.max(100).min(total_region_cnt)
    } else {
        average.min(4000)
    }
}
/// Go `skipMergeSort`: the read step's files overlap little enough for the
/// ingest step to read them directly.
#[must_use]
pub fn skip_merge_sort(stats: &[tidb_ingestor_simplesst::MultipleFilesStat], concurrency: i64) -> bool {
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/forceMergeSort").is_some() {
        return false;
    }
    tidb_ingestor_simplesst::get_max_overlapping_total(stats)
        <= tidb_ingestor_simplesst::get_adjusted_merge_sort_overlap_threshold(concurrency)
}
/// Go `LitBackfillScheduler`'s task-invariant mode.
#[derive(Default)]
pub struct LitBackfillScheduler {
    /// Go `GlobalSort`.
    pub global_sort: bool,
    /// Go `MergeTempIndex`.
    pub merge_temp_index: bool,
}
impl LitBackfillScheduler {
    /// Go `LitBackfillScheduler.GetNextStep`.
    pub fn get_next_step(&self, task: &TaskBase) -> Step {
        match task.step {
            STEP_INIT if self.merge_temp_index => BACKFILL_STEP_MERGE_TEMP_INDEX,
            STEP_INIT => BACKFILL_STEP_READ_INDEX,
            BACKFILL_STEP_READ_INDEX if self.global_sort => BACKFILL_STEP_MERGE_SORT,
            BACKFILL_STEP_MERGE_SORT => BACKFILL_STEP_WRITE_AND_INGEST,
            _ => STEP_DONE,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    /// Go `TestCalculateRegionBatch`.
    #[test]
    fn test_calculate_region_batch() {
        for (regions, nodes, local, expected) in [
            (100, 8, false, 13),
            (2, 8, false, 1),
            (8, 8, false, 1),
            (100, 8, true, 100),
            (2, 8, true, 2),
            (24, 8, true, 24),
            (1000, 8, true, 334),
            (1000, 2, true, 500),
            (200, 3, true, 100),
        ] {
            assert_eq!(calculate_region_batch(regions, nodes, local), expected);
        }
    }
    /// Go `TestGetNextStep`.
    #[test]
    fn test_get_next_step() {
        for (global_sort, merge_temp_index, expected) in [
            (false, false, vec![BACKFILL_STEP_READ_INDEX, STEP_DONE]),
            (
                true,
                false,
                vec![
                    BACKFILL_STEP_READ_INDEX,
                    BACKFILL_STEP_MERGE_SORT,
                    BACKFILL_STEP_WRITE_AND_INGEST,
                ],
            ),
            (false, true, vec![BACKFILL_STEP_MERGE_TEMP_INDEX, STEP_DONE]),
        ] {
            let scheduler = LitBackfillScheduler {
                global_sort,
                merge_temp_index,
            };
            let mut task = TaskBase {
                step: STEP_INIT,
                ..Default::default()
            };
            for next in expected {
                assert_eq!(scheduler.get_next_step(&task), next);
                task.step = next;
            }
        }
    }
}
