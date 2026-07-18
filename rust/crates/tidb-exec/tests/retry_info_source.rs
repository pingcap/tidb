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

//! Source-shaped tests for `variable.RetryInfo`'s queue boundary.

use tidb_exec::retry_info::RetryInfo;

#[test]
fn auto_increment_and_random_values_are_consumed_in_source_order() {
    // Source: pkg/sessionctx/variable/session.go:135-152, 171-177.
    let mut retry = RetryInfo::default();
    retry.add_auto_increment_id(1000);
    retry.add_auto_increment_id(1001);
    retry.add_auto_random_id(-7);
    retry.add_auto_random_id(8);

    assert_eq!(retry.next_auto_increment_id(), Some(1000));
    assert_eq!(retry.next_auto_increment_id(), Some(1001));
    assert_eq!(retry.next_auto_increment_id(), None);
    assert_eq!(retry.next_auto_random_id(), Some(-7));
    assert_eq!(retry.next_auto_random_id(), Some(8));
    assert_eq!(retry.next_auto_random_id(), None);
}

#[test]
fn reset_offset_replays_values_without_allocating_new_ones() {
    // Source: pkg/sessionctx/variable/session.go:129-133, 160-162.
    let mut retry = RetryInfo::default();
    retry.add_auto_increment_id(42);
    retry.add_auto_random_id(84);
    assert_eq!(retry.next_auto_increment_id(), Some(42));
    assert_eq!(retry.next_auto_random_id(), Some(84));

    retry.reset_offset();
    assert_eq!(retry.next_auto_increment_id(), Some(42));
    assert_eq!(retry.next_auto_random_id(), Some(84));
    assert_eq!(retry.next_auto_increment_id(), None);
    assert_eq!(retry.next_auto_random_id(), None);
}

#[test]
fn clean_clears_queues_and_dropped_statement_ids_but_preserves_lifecycle_fields() {
    // Source: pkg/sessionctx/variable/session.go:110-127, 164-169.
    let mut retry = RetryInfo::default();
    retry.retrying = true;
    retry.dropped_prepared_stmt_ids = vec![11, 12];
    retry.last_rc_read_ts = 99;
    retry.add_auto_increment_id(1);
    retry.add_auto_random_id(2);
    assert_eq!(retry.next_auto_increment_id(), Some(1));
    assert_eq!(retry.next_auto_random_id(), Some(2));

    retry.clean();
    assert_eq!(retry.next_auto_increment_id(), None);
    assert_eq!(retry.next_auto_random_id(), None);
    assert!(retry.dropped_prepared_stmt_ids.is_empty());
    assert!(retry.retrying);
    assert_eq!(retry.last_rc_read_ts, 99);
}

#[test]
fn empty_queues_are_safe_and_default_metadata_is_zeroed() {
    // Source: pkg/sessionctx/variable/session.go:171-177.
    let mut retry = RetryInfo::default();
    assert!(!retry.retrying);
    assert_eq!(retry.last_rc_read_ts, 0);
    assert_eq!(retry.next_auto_increment_id(), None);
    assert_eq!(retry.next_auto_random_id(), None);
}
