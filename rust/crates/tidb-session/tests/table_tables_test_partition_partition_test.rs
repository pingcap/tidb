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

//! Go `pkg/table/tables/test/partition/partition_test.go` (`TestPointGetKeyPartitioning`, `TestPruningOverflow`), on the mock store through the session, as Go's testkit
//! runs them.

#![allow(unused_mut, unused_variables, unused_imports)]

use crate::support::testkit::*;

/// Go `TestPointGetKeyPartitioning`.
#[test]
fn test_point_get_key_partitioning() {
    let mut tk = TestKit::new();
    tk.must_exec(r#"use test"#);
    tk.must_exec(r#"CREATE TABLE t (a VARCHAR(30) NOT NULL, b VARCHAR(45) NOT NULL, c VARCHAR(45) NOT NULL, PRIMARY KEY (b, a)) PARTITION BY KEY(b) PARTITIONS 5"#);
    tk.must_exec(r#"INSERT INTO t VALUES ('Aa', 'Ab', 'Ac'), ('Ba', 'Bb', 'Bc')"#);
    tk.must_query(r#"SELECT * FROM t WHERE b = 'Ab'"#)
        .check(["Aa Ab Ac"]);
}

/// Go `TestPruningOverflow`.
#[test]
fn test_pruning_overflow() {
    let mut tk = TestKit::new();
    tk.must_exec(r#"use test"#);
    tk.must_exec("CREATE TABLE t (a int NOT NULL, b bigint NOT NULL,PRIMARY KEY (a,b)) PARTITION BY HASH ((a*b))PARTITIONS 13");
    tk.must_exec(r#"insert into t values(0, 3522101843073676459)"#);
    tk.must_query(r#"SELECT a, b FROM t WHERE a IN (0,14158354938390,0) AND b IN (3522101843073676459,-2846203247576845955,838395691793635638)"#).check(["0 3522101843073676459"]);
}
