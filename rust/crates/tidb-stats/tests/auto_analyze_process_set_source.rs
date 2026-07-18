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

//! Source-backed tests for auto-analyze process-ID tracking.

use std::sync::Arc;

use tidb_stats::AutoAnalyzeProcessSet;

#[test]
fn source_process_set_tracks_untracks_and_contains_ids() {
    let processes = AutoAnalyzeProcessSet::new();
    assert!(processes.all().is_empty());
    assert!(!processes.contains(7));

    processes.tracker(7);
    processes.tracker(11);
    processes.tracker(7);
    assert!(processes.contains(7));
    assert!(processes.contains(11));
    let mut all = processes.all();
    all.sort_unstable();
    assert_eq!(all, vec![7, 11]);

    processes.untracker(7);
    processes.untracker(7);
    assert!(!processes.contains(7));
    assert_eq!(processes.all(), vec![11]);
}

#[test]
fn source_process_set_supports_concurrent_snapshots() {
    let processes = Arc::new(AutoAnalyzeProcessSet::new());
    let mut workers = Vec::new();
    for id in 0..8 {
        let processes = Arc::clone(&processes);
        workers.push(std::thread::spawn(move || {
            processes.tracker(id);
            assert!(processes.contains(id));
        }));
    }
    for worker in workers {
        worker.join().expect("tracker worker panicked");
    }

    let mut all = processes.all();
    all.sort_unstable();
    assert_eq!(all, (0..8).collect::<Vec<_>>());
}
