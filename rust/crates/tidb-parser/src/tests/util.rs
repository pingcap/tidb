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

//! Transcreation of `pkg/parser/ast/util_test.go`.

use super::*;

fn read_only(sql: &str) -> bool {
    parse(sql).expect("parse").is_read_only(true)
}

#[test]
fn test_cacheable() {
    for sql in [
        "DELETE FROM t",
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a = 1",
    ] {
        assert!(!read_only(sql), "{sql}");
    }
    for sql in ["DO 1", "SHOW TABLES", "SELECT 1", "EXPLAIN SELECT 1"] {
        assert!(read_only(sql), "{sql}");
    }
    assert!(read_only("EXPLAIN INSERT INTO t VALUES (1)"));
    assert!(!read_only("EXPLAIN ANALYZE INSERT INTO t VALUES (1)"));
    assert!(read_only("EXPLAIN ANALYZE SELECT 1"));
}

#[test]
fn test_union_read_only() {
    for sql in [
        "SELECT 1 UNION SELECT 2",
        "SELECT 1 UNION SELECT 2 UNION SELECT 3",
    ] {
        assert!(read_only(sql), "{sql}");
    }
    for sql in [
        "SELECT 1 UNION SELECT 2 FOR UPDATE",
        "SELECT 1 UNION SELECT 2 FOR SHARE NOWAIT",
        "SELECT 1 FOR UPDATE UNION SELECT 2",
    ] {
        assert!(!read_only(sql), "{sql}");
    }
}
