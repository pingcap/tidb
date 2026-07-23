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

//! Stable workload for `pkg/parser/bench_test.go::BenchmarkHandParser`.

use std::hint::black_box;

#[test]
fn benchmark_hand_parser() {
    for sql in [
        "SELECT a FROM t WHERE a = 1",
        "INSERT INTO t (a, b, c) VALUES (1, 2, 3)",
        "UPDATE t SET a = 1 WHERE b = 2",
        "DELETE FROM t WHERE a = 1",
        "SELECT a, b, c FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.a > 1 AND t2.b < 10 ORDER BY t1.a LIMIT 100",
        "SELECT a FROM t WHERE a = 1 AND b = 2 AND c = 3",
        "SELECT a FROM t WHERE id = 1",
    ] {
        for _ in 0..1_000 {
            black_box(tidb_parser::parse(sql).unwrap());
        }
    }
}
