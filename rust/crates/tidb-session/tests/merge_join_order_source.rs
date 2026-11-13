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

//! Go `PhysicalMergeJoin` and the order it demands of its children.

use crate::support::testkit::TestKit;

/// Go's merge join asks each child for its join keys' order with the key
/// `Column`s themselves (`GetJoinKeys`, `NewPhysicalProperty`), so a
/// partitioned child's keep-order IndexLookUp merges its partitions by a
/// by-item its index scan's schema holds (`buildIndexLookUpReader` finds it by
/// column ID). The port named the keys by unique id alone, which left the
/// by-item without a column ID and refused the reader. Go's plan and rows
/// (gorun).
#[test]
fn a_merge_join_over_a_partitioned_keep_order_lookup_like_go() {
    let mut tk = TestKit::new();
    tk.must_exec("create table trange(a int, b int, key(a)) partition by range(a) (partition p0 values less than(300), partition p1 values less than(500), partition p2 values less than(1100))");
    tk.must_exec("create table tregular1(a int, b int, key(a))");
    tk.must_exec("insert into trange values (50,1),(300,2),(550,3),(700,4)");
    tk.must_exec("insert into tregular1 values (50,1),(300,2),(550,3),(700,4)");
    let sql = "select /*+ merge_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and trange.a in (300, 50, 550)";
    tk.must_query(&format!("explain format='brief' {sql}")).check_at(&[0, 2, 3, 4], [
        "MergeJoin root  inner join, left key:test.trange.a, right key:test.tregular1.a",
        "├─Projection(Build) root  test.tregular1.a, test.tregular1.b",
        "│ └─IndexLookUp root  ",
        "│   ├─Selection(Build) cop[tikv]  not(isnull(test.tregular1.a))",
        "│   │ └─IndexRangeScan cop[tikv] table:tregular1, index:a(a) range:[50,50], [300,300], [550,550], keep order:true, stats:pseudo",
        "│   └─TableRowIDScan(Probe) cop[tikv] table:tregular1 keep order:false, stats:pseudo",
        "└─Projection(Probe) root  test.trange.a, test.trange.b",
        "  └─IndexLookUp root partition:all ",
        "    ├─Selection(Build) cop[tikv]  not(isnull(test.trange.a))",
        "    │ └─IndexRangeScan cop[tikv] table:trange, index:a(a) range:[50,50], [300,300], [550,550], keep order:true, stats:pseudo",
        "    └─TableRowIDScan(Probe) cop[tikv] table:trange keep order:false, stats:pseudo",
    ]);
    tk.must_query(sql).check(["50 1 50 1", "300 2 300 2", "550 3 550 3"]);
}
