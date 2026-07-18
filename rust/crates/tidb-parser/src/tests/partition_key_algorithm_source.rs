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

//! Direct source rows from Go's `TestPartitionKeyAlgorithm`.
//!
//! The parser owns the `ALGORITHM` value and its range validation.  Partition
//! execution and key hashing remain outside this syntax-only fixture.

use super::*;

#[test]
fn partition_key_algorithm_source_rows() {
    assert_eq!(
        r("CREATE TABLE t (c1 integer,c2 integer) PARTITION BY LINEAR KEY ALGORITHM = 1 (c1,c2) PARTITIONS 4"),
        "CREATE TABLE `t` (`c1` INT,`c2` INT) PARTITION BY LINEAR KEY ALGORITHM = 1 (`c1`,`c2`) PARTITIONS 4",
    );

    for sql in [
        "CREATE TABLE t (c1 integer,c2 integer) PARTITION BY LINEAR KEY ALGORITHM = -1 (c1,c2) PARTITIONS 4",
        "CREATE TABLE t (c1 integer,c2 integer) PARTITION BY LINEAR KEY ALGORITHM = 0 (c1,c2) PARTITIONS 4",
        "CREATE TABLE t (c1 integer,c2 integer) PARTITION BY LINEAR KEY ALGORITHM = 3 (c1,c2) PARTITIONS 4",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}
