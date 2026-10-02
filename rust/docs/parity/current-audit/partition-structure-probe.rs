// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Diagnostic only: record partition data before and after accepted DDL.
//! Successful execution does not establish parity or durable DDL acceptance.

use tidb_session::Session;

fn main() {
    let mut session = Session::new();
    for sql in [
        "CREATE TABLE review_partition (a INT) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        "INSERT INTO review_partition VALUES (1),(11)",
        "SELECT * FROM review_partition ORDER BY a",
        "ALTER TABLE review_partition PARTITION BY HASH(a) PARTITIONS 2",
        "SHOW CREATE TABLE review_partition",
        "SELECT * FROM review_partition ORDER BY a",
        "INSERT INTO review_partition VALUES (2),(12)",
        "SELECT * FROM review_partition ORDER BY a",
        "CREATE TABLE review_plain (a INT)",
        "INSERT INTO review_plain VALUES (1),(11)",
        "ALTER TABLE review_plain PARTITION BY HASH(a) PARTITIONS 2",
        "SELECT * FROM review_plain ORDER BY a",
    ] {
        println!("{sql}\n  {:?}", session.run(sql));
    }
}
