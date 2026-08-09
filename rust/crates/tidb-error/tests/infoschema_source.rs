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

//! Source translation of `pkg/errno/infoschema_test.go`.

use tidb_error::tidb::infoschema::{
    flush_stats, global_stats, host_stats, increment_error, increment_warning, user_stats,
};

#[test]
fn test_copy_safety() {
    flush_stats();

    increment_error(123, "user", "host");
    increment_error(321, "user2", "host2");
    increment_warning(123, "user", "host");
    increment_warning(999, "user", "host");
    increment_warning(222, "u", "h");

    let global_copy = global_stats();
    let user_copy = user_stats();
    let host_copy = host_stats();

    increment_error(123, "user", "host");
    increment_error(999, "user2", "host2");
    increment_error(123, "user3", "host");
    increment_warning(123, "user", "host");
    increment_warning(222, "u", "h");
    increment_warning(222, "a", "b");
    increment_warning(333, "c", "d");

    let global = global_stats();
    assert_eq!(global[&123].error_count, 3);
    assert_eq!(global_copy[&123].error_count, 1);

    let users = user_stats();
    assert_eq!(users.len(), 6);
    assert_eq!(user_copy.len(), 3);
    assert_eq!(users["user"][&123].error_count, 2);
    assert_eq!(users["user"][&123].warning_count, 2);
    assert_eq!(user_copy["user"][&123].error_count, 1);
    assert_eq!(user_copy["user"][&123].warning_count, 1);

    assert!(!user_copy.contains_key("user3"));
    assert!(users.contains_key("user3"));
    assert!(!user_copy.contains_key("a"));
    assert!(users.contains_key("a"));

    let hosts = host_stats();
    assert_eq!(hosts.len(), 5);
    assert_eq!(host_copy.len(), 3);

    increment_error(123, "user3", "newhost");
    let hosts = host_stats();
    assert_eq!(hosts.len(), 6);
    assert_eq!(host_copy.len(), 3);

    assert!(!host_copy.contains_key("newhost"));
    assert!(hosts.contains_key("newhost"));
    assert!(!host_copy.contains_key("b"));
    assert!(hosts.contains_key("b"));

    flush_stats();
}
