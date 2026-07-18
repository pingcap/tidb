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

//! Source-backed tests for stable lock-status diagnostics.

use tidb_stats::{
    generate_stable_skipped_partitions_message, generate_stable_skipped_tables_message,
};

#[test]
fn source_skipped_tables_message_matches_go_cases() {
    let no_skips: Vec<&str> = vec![];
    assert_eq!(
        generate_stable_skipped_tables_message(3, &no_skips, "locking", "locked"),
        ""
    );
    assert_eq!(
        generate_stable_skipped_tables_message(1, &["t1"], "locking", "locked"),
        "skip locking locked table: t1"
    );
    assert_eq!(
        generate_stable_skipped_tables_message(4, &["t1", "t2", "t3"], "locking", "locked"),
        "skip locking locked tables: t1, t2, t3, other tables locked successfully"
    );
    assert_eq!(
        generate_stable_skipped_tables_message(4, &["t1", "t2", "t3", "t4"], "locking", "locked"),
        "skip locking locked tables: t1, t2, t3, t4"
    );
    assert_eq!(
        generate_stable_skipped_tables_message(
            4,
            &["t1", "t2", "t3", "t4"],
            "unlocking",
            "unlocked"
        ),
        "skip unlocking unlocked tables: t1, t2, t3, t4"
    );
}

#[test]
fn source_skipped_partitions_message_matches_go_cases() {
    let no_skips: Vec<&str> = vec![];
    let all_ids = [1_i64, 2, 3, 4];
    assert_eq!(
        generate_stable_skipped_partitions_message(
            &[1, 2, 3],
            "test.t",
            &no_skips,
            "locking",
            "locked"
        ),
        ""
    );
    assert_eq!(
        generate_stable_skipped_partitions_message(&[1], "test.t", &["t1"], "locking", "locked"),
        "skip locking locked partition of table test.t: t1"
    );
    assert_eq!(
        generate_stable_skipped_partitions_message(&all_ids, "test.t", &["t1", "t2", "t3"], "locking", "locked"),
        "skip locking locked partitions of table test.t: t1, t2, t3, other partitions locked successfully"
    );
    assert_eq!(
        generate_stable_skipped_partitions_message(
            &all_ids,
            "test.t",
            &["t1", "t2", "t3", "t4"],
            "locking",
            "locked"
        ),
        "skip locking locked partitions of table test.t: t1, t2, t3, t4"
    );
    assert_eq!(
        generate_stable_skipped_partitions_message(
            &all_ids,
            "test.t",
            &["t1", "t2", "t3", "t4"],
            "unlocking",
            "unlocked"
        ),
        "skip unlocking unlocked partitions of table test.t: t1, t2, t3, t4"
    );
}

#[test]
fn source_skipped_names_are_sorted_before_formatting() {
    assert_eq!(
        generate_stable_skipped_tables_message(2, &["t2", "t1"], "locking", "locked"),
        "skip locking locked tables: t1, t2"
    );
    assert_eq!(
        generate_stable_skipped_partitions_message(
            &[2, 1],
            "test.t",
            &["p2", "p1"],
            "locking",
            "locked"
        ),
        "skip locking locked partitions of table test.t: p1, p2"
    );
}
