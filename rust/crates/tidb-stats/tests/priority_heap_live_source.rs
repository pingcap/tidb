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

//! Dependency-closed lifecycle tests for Go `queue_test.go`.

use std::sync::Arc;
use std::thread;

use tidb_stats::priority_heap::{DmlJobChange, LiveAnalysisQueue};
use tidb_stats::{AnalysisIndicators, AnalysisJobKind, PriorityHeapError, PriorityHeapItem};

fn job(table_id: i64, weight: f64) -> PriorityHeapItem {
    PriorityHeapItem::new(table_id, weight).with_job_metadata(
        AnalysisJobKind::NonPartitioned,
        AnalysisIndicators {
            change_percentage: weight / 100.0,
            table_size: 10.0,
            last_analysis_duration_nanos: 1,
        },
    )
}

#[test]
fn apis_before_initialize_share_the_source_gate() {
    let queue = LiveAnalysisQueue::new();
    assert_eq!(queue.is_empty(), Err(PriorityHeapError::NotInitialized));
    assert_eq!(queue.pop(), Err(PriorityHeapError::NotInitialized));
    assert_eq!(queue.peek(), Err(PriorityHeapError::NotInitialized));
    assert_eq!(queue.len(), Err(PriorityHeapError::NotInitialized));
    assert!(queue.snapshot().is_err());
    assert!(queue.running_jobs().is_empty());
    assert!(!queue.is_initialized());
}

#[test]
fn initialize_pop_close_and_reinitialize_preserve_live_identity() {
    let queue = LiveAnalysisQueue::new();
    queue
        .initialize([job(1, 10.0), job(2, 20.0)], [], 7)
        .unwrap();
    assert!(queue.is_initialized());
    assert!(!queue.is_empty().unwrap());
    // Double initialization is a no-op rather than replacing live state.
    queue.initialize([job(3, 30.0)], [], 8).unwrap();
    assert_eq!(queue.len().unwrap(), 2);
    assert_eq!(queue.pop().unwrap().table_id, 2);
    assert_eq!(queue.pop().unwrap().table_id, 1);
    assert!(queue.is_empty().unwrap());
    assert_eq!(queue.snapshot().unwrap().running_jobs, [1, 2]);

    queue.close();
    assert!(!queue.is_initialized());
    queue.close();
    queue.initialize([job(3, 30.0)], [], 9).unwrap();
    assert_eq!(queue.peek().unwrap().table_id, 3);
}

#[test]
fn dml_changes_running_retry_and_completion_follow_source_transitions() {
    let queue = LiveAnalysisQueue::new();
    queue
        .initialize([job(1, 20.0), job(2, 10.0)], [], 1)
        .unwrap();
    let running = queue.pop().unwrap();
    assert_eq!(running.table_id, 1);

    queue
        .process_dml_changes([job(1, 60.0), job(2, 50.0)], [], 2)
        .unwrap();
    let snapshot = queue.snapshot().unwrap();
    assert_eq!(snapshot.running_jobs, [1]);
    assert_eq!(snapshot.must_retry_jobs, [1]);
    assert_eq!(queue.peek().unwrap().table_id, 2);

    queue.complete(running, false);
    queue.requeue_must_retry_jobs().unwrap();
    assert_eq!(queue.peek().unwrap().table_id, 1);

    let failed = queue.pop().unwrap();
    queue.complete(failed, true);
    assert_eq!(queue.snapshot().unwrap().must_retry_jobs, [1]);
    queue.requeue_must_retry_jobs().unwrap();
    assert_eq!(queue.peek().unwrap().table_id, 1);
}

#[test]
fn whole_table_and_partition_locks_delete_and_restore_jobs() {
    let queue = LiveAnalysisQueue::new();
    queue
        .initialize(
            [
                job(10, 30.0).with_job_metadata(
                    AnalysisJobKind::DynamicPartitioned,
                    AnalysisIndicators::default(),
                ),
                job(11, 20.0).with_job_metadata(
                    AnalysisJobKind::StaticPartitioned,
                    AnalysisIndicators::default(),
                ),
            ],
            [],
            1,
        )
        .unwrap();
    queue.process_dml_changes([], [10, 11], 2).unwrap();
    assert_eq!(queue.len().unwrap(), 0);

    queue
        .process_dml_changes(
            [
                job(10, 31.0).with_job_metadata(
                    AnalysisJobKind::DynamicPartitioned,
                    AnalysisIndicators::default(),
                ),
                job(11, 21.0).with_job_metadata(
                    AnalysisJobKind::StaticPartitioned,
                    AnalysisIndicators::default(),
                ),
            ],
            [],
            3,
        )
        .unwrap();
    assert_eq!(queue.len().unwrap(), 2);
}

#[test]
fn static_partition_identity_uses_partition_key_and_parent_lock() {
    let queue = LiveAnalysisQueue::new();
    let partition = PriorityHeapItem::new_static_partition(10, 11, 20.0);
    queue.initialize([partition], [], 1).unwrap();
    assert_eq!(queue.peek().unwrap().table_id, 11);
    assert_eq!(queue.peek().unwrap().global_table_id, 10);

    queue.process_dml_changes([], [10], 2).unwrap();
    assert!(queue.is_empty().unwrap());
    queue.process_dml_changes([partition], [], 3).unwrap();
    assert_eq!(queue.peek().unwrap().table_id, 11);
}

#[test]
fn dynamic_job_identity_is_not_the_locked_partition_identity() {
    let queue = LiveAnalysisQueue::new();
    let dynamic = job(10, 20.0).with_job_metadata(
        AnalysisJobKind::DynamicPartitioned,
        AnalysisIndicators::default(),
    );
    queue.initialize([dynamic], [], 1).unwrap();

    // A dynamic job is keyed by its global table. Locking one physical
    // partition filters that partition during external job reconstruction;
    // it must not delete the global job merely because the IDs differ.
    queue.process_dml_changes([], [11], 2).unwrap();
    assert_eq!(queue.peek().unwrap().table_id, 10);
}

#[test]
fn dml_versions_are_strictly_newer_and_never_move_backwards() {
    let queue = LiveAnalysisQueue::new();
    queue.initialize([job(1, 1.0)], [], 10).unwrap();
    queue
        .process_versioned_dml_changes(
            [
                DmlJobChange {
                    job: job(1, 99.0),
                    version: 10,
                },
                DmlJobChange {
                    job: job(2, 20.0),
                    version: 11,
                },
            ],
            [],
            11,
        )
        .unwrap();
    assert_eq!(queue.snapshot().unwrap().last_dml_update_version, 11);
    assert_eq!(queue.peek().unwrap().table_id, 2);
    assert_eq!(
        queue
            .snapshot()
            .unwrap()
            .current_jobs
            .iter()
            .find(|job| job.table_id == 1)
            .unwrap()
            .weight,
        1.0
    );
    queue.process_dml_changes([], [], 9).unwrap();
    assert_eq!(queue.snapshot().unwrap().last_dml_update_version, 11);
}

#[test]
fn retry_pass_preserves_a_still_running_identity() {
    let queue = LiveAnalysisQueue::new();
    queue.initialize([job(1, 1.0)], [], 1).unwrap();
    let running = queue.pop().unwrap();
    queue.process_dml_changes([job(1, 2.0)], [], 2).unwrap();
    queue.requeue_must_retry_jobs().unwrap();
    assert_eq!(queue.snapshot().unwrap().must_retry_jobs, [1]);
    queue.complete(running, false);
    queue.requeue_must_retry_jobs().unwrap();
    assert_eq!(queue.peek().unwrap().weight, 2.0);
}

#[test]
fn refresh_updates_weight_and_removes_deleted_tables() {
    let queue = LiveAnalysisQueue::new();
    queue.initialize([job(1, 1.0), job(2, 2.0)], [], 1).unwrap();
    queue.refresh_jobs([job(2, 9.0)], [2]).unwrap();
    assert_eq!(queue.len().unwrap(), 1);
    let refreshed = queue.peek().unwrap();
    assert_eq!(refreshed.table_id, 2);
    assert_eq!(refreshed.weight, 9.0);
    assert_ne!(refreshed.indicators.last_analysis_duration_nanos, 0);
}

#[test]
fn close_is_concurrent_idempotent_and_never_deadlocks() {
    let queue = Arc::new(LiveAnalysisQueue::new());
    queue.initialize([job(1, 1.0)], [], 1).unwrap();
    let threads: Vec<_> = (0..20)
        .map(|_| {
            let queue = Arc::clone(&queue);
            thread::spawn(move || queue.close())
        })
        .collect();
    for thread in threads {
        thread.join().expect("close must not panic");
    }
    assert!(!queue.is_initialized());
}

#[test]
fn concurrent_initialize_close_and_background_operations_are_serialized() {
    let queue = Arc::new(LiveAnalysisQueue::new());
    let initialize = {
        let queue = Arc::clone(&queue);
        thread::spawn(move || {
            for version in 0..20 {
                queue
                    .initialize([job(1, version as f64)], [], version)
                    .unwrap();
            }
        })
    };
    let close = {
        let queue = Arc::clone(&queue);
        thread::spawn(move || {
            for _ in 0..20 {
                queue.close();
            }
        })
    };
    initialize.join().expect("initialize must not panic");
    close.join().expect("close must not panic");
    queue.close();
    assert!(!queue.is_initialized());
}

#[test]
fn close_serializes_with_real_maintenance_operations() {
    let queue = Arc::new(LiveAnalysisQueue::new());
    queue.initialize([job(1, 1.0)], [], 1).unwrap();
    let maintenance = {
        let queue = Arc::clone(&queue);
        thread::spawn(move || {
            for version in 2..100 {
                let _ = queue.process_dml_changes([job(1, version as f64)], [], version);
                let _ = queue.refresh_jobs([job(1, version as f64)], [1]);
                let _ = queue.requeue_must_retry_jobs();
                let _ = queue.snapshot();
            }
        })
    };
    let close = {
        let queue = Arc::clone(&queue);
        thread::spawn(move || queue.close())
    };
    maintenance.join().expect("maintenance must not panic");
    close.join().expect("close must not panic");
    queue.close();
    assert!(!queue.is_initialized());
}

#[test]
fn panic_recovery_resets_queue_for_reinitialization() {
    let queue = LiveAnalysisQueue::new();
    queue.initialize([job(1, 1.0)], [], 1).unwrap();
    assert!(!queue.run_with_recovery(|| panic!("injected queue worker panic")));
    assert!(!queue.is_initialized());
    queue.initialize([job(2, 2.0)], [], 2).unwrap();
    assert_eq!(queue.peek().unwrap().table_id, 2);
}
