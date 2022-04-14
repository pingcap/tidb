// Copyright 2022 PingCAP, Inc.
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

package core_test

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func TestGByStr2Int1(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set session tidb_enable_groupby_str2int=1")
	tk.MustExec("CREATE TABLE t (a varchar(10), b int, c int, d text, e char(20))")
	tk.MustQuery("explain format=brief SELECT d, b, max(c) FROM t WHERE d in ('x', 'y', 'z') GROUP BY d,b").Check(testkit.Rows(
		"Projection 24.00 root  case(eq(Column#8, 0), x, eq(Column#8, 1), y, eq(Column#8, 2), z)->Column#9, test.t.b, Column#7",
		"└─HashAgg 24.00 root  group by:Column#10, test.t.b, funcs:max(Column#11)->Column#7, funcs:firstrow(test.t.b)->test.t.b, funcs:firstrow(Column#10)->Column#8",
		"  └─TableReader 24.00 root  data:HashAgg",
		"    └─HashAgg 24.00 cop[tikv]  group by:case(eq(test.t.d, \"x\"), 0, eq(test.t.d, \"y\"), 1, eq(test.t.d, \"z\"), 2), test.t.b, funcs:max(test.t.c)->Column#11",
		"      └─Selection 30.00 cop[tikv]  in(test.t.d, \"x\", \"y\", \"z\")",
		"        └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	tk.MustQuery("explain format=brief SELECT a, b, max(c) FROM t WHERE a in ('x', 'y', 'z') GROUP BY a,b").Check(testkit.Rows(
		"Projection 24.00 root  case(eq(Column#8, 0), x, eq(Column#8, 1), y, eq(Column#8, 2), z)->Column#9, test.t.b, Column#7",
		"└─HashAgg 24.00 root  group by:Column#10, test.t.b, funcs:max(Column#11)->Column#7, funcs:firstrow(Column#10)->Column#8, funcs:firstrow(test.t.b)->test.t.b",
		"  └─TableReader 24.00 root  data:HashAgg",
		"    └─HashAgg 24.00 cop[tikv]  group by:case(eq(test.t.a, \"x\"), 0, eq(test.t.a, \"y\"), 1, eq(test.t.a, \"z\"), 2), test.t.b, funcs:max(test.t.c)->Column#11",
		"      └─Selection 30.00 cop[tikv]  in(test.t.a, \"x\", \"y\", \"z\")",
		"        └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	tk.MustQuery("explain format=brief SELECT b,e, max(c) FROM t WHERE e in ('x', 'y', 'z') GROUP BY b,e").Check(testkit.Rows(
		"Projection 24.00 root  test.t.b, case(eq(Column#8, 0), x, eq(Column#8, 1), y, eq(Column#8, 2), z)->Column#9, Column#7",
		"└─HashAgg 24.00 root  group by:Column#10, test.t.b, funcs:max(Column#11)->Column#7, funcs:firstrow(test.t.b)->test.t.b, funcs:firstrow(Column#10)->Column#8",
		"  └─TableReader 24.00 root  data:HashAgg",
		"    └─HashAgg 24.00 cop[tikv]  group by:case(eq(test.t.e, \"x\"), 0, eq(test.t.e, \"y\"), 1, eq(test.t.e, \"z\"), 2), test.t.b, funcs:max(test.t.c)->Column#11",
		"      └─Selection 30.00 cop[tikv]  in(test.t.e, \"x\", \"y\", \"z\")",
		"        └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	tk.MustQuery("explain format=brief SELECT b,e, max(c) FROM t WHERE e in ('x', 'y') or e='z' GROUP BY b,e").Check(testkit.Rows(
		"Projection 24.00 root  test.t.b, case(eq(Column#8, 0), x, eq(Column#8, 1), y, eq(Column#8, 2), z)->Column#9, Column#7",
		"└─HashAgg 24.00 root  group by:Column#10, test.t.b, funcs:max(Column#11)->Column#7, funcs:firstrow(test.t.b)->test.t.b, funcs:firstrow(Column#10)->Column#8",
		"  └─TableReader 24.00 root  data:HashAgg",
		"    └─HashAgg 24.00 cop[tikv]  group by:case(eq(test.t.e, \"x\"), 0, eq(test.t.e, \"y\"), 1, eq(test.t.e, \"z\"), 2), test.t.b, funcs:max(test.t.c)->Column#11",
		"      └─Selection 30.00 cop[tikv]  or(in(test.t.e, \"x\", \"y\"), eq(test.t.e, \"z\"))",
		"        └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	// group key with scalar function is not supported yet
	tk.MustQuery("explain format=brief SELECT concat(d,'pp'), b, max(c) FROM t WHERE concat(d,'pp') in ('x', 'y', 'z') GROUP BY concat(d,'pp'),b").Check(testkit.Rows(
		"Projection 6400.00 root  concat(test.t.d, pp)->Column#8, test.t.b, Column#7",
		"└─HashAgg 6400.00 root  group by:Column#19, Column#20, funcs:max(Column#16)->Column#7, funcs:firstrow(Column#17)->test.t.b, funcs:firstrow(Column#18)->test.t.d",
		"  └─Projection 8000.00 root  test.t.c, test.t.b, test.t.d, concat(test.t.d, pp)->Column#19, test.t.b",
		"    └─TableReader 8000.00 root  data:Selection",
		"      └─Selection 8000.00 cop[tikv]  in(concat(test.t.d, \"pp\"), \"x\", \"y\", \"z\")",
		"        └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))
}

func TestGByStr2Int2(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set session tidb_enable_groupby_str2int=1")
	tk.MustExec("CREATE TABLE t (a int,b int,c int, d text)")
	tk.MustExec("insert into t values(1,1,1,'x')")
	tk.MustQuery(" SELECT d, b, max(c) FROM t WHERE d in ('x', 'y', 'z') GROUP BY d,b").Check(testkit.Rows(
		"x 1 1"))
	tk.MustExec("insert into t values(1,1,2,'x')")
	tk.MustQuery(" SELECT d, b, max(c) FROM t WHERE d in ('x', 'y', 'z') GROUP BY d,b").Check(testkit.Rows(
		"x 1 2"))
	tk.MustExec("insert into t values(1,8,9,'y')")
	tk.MustExec("insert into t values(1,5,6,'y')")
	tk.MustExec("insert into t values(1,1,2,'p')")
	tk.MustQuery(" SELECT d, b, max(c) FROM t WHERE d in ('x', 'y', 'z') GROUP BY d,b ORDER BY d,b").Check(testkit.Rows(
		"x 1 2", "y 5 6", "y 8 9"))
}

func TestGByStr2Int3(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set session tidb_enable_groupby_str2int=1")
	tk.MustExec("CREATE TABLE t (a varchar(10), b int, c int, d text, e char(20))")
	tk.MustExec("insert into t values('xx',1,1,'x','y')")
	tk.MustQuery(" SELECT d, b, max(c) FROM t WHERE d in ('x', 'y', 'z') GROUP BY d,b").Check(testkit.Rows(
		"x 1 1"))
	tk.MustQuery(" SELECT a, b, max(c) FROM t WHERE a in ('x', 'xx', 'z') GROUP BY a,b").Check(testkit.Rows(
		"xx 1 1"))
	tk.MustQuery(" SELECT e, b, max(c) FROM t WHERE e in ('x', 'y', 'z') GROUP BY e,b").Check(testkit.Rows(
		"y 1 1"))
	tk.MustExec("insert into t values('xx',1,3,'x','y')")
	tk.MustExec("insert into t values('xx',1,2,'x1','y')")
	tk.MustExec("insert into t values('xx',2,4,'x1','y')")
	tk.MustExec("insert into t values('xx',2,5,'x','y1')")
	tk.MustExec("insert into t values('xx',2,6,'z','y1')")
	tk.MustExec("insert into t values('yy',1,3,'x','y3')")
	tk.MustExec("insert into t values('yy',1,2,'y','y1')")
	tk.MustExec("insert into t values('xx',2,4,'y','y2')")
	tk.MustExec("insert into t values('zz',2,5,'z','y3')")
	tk.MustExec("insert into t values('zz',2,6,'z','y3')")
	tk.MustQuery(" SELECT d, b, sum(c) FROM t WHERE d in ('x', 'y', 'z') GROUP BY d,b ORDER BY d,b").Check(testkit.Rows(
		"x 1 7]\n[x 2 5]\n[y 1 2]\n[y 2 4]\n[z 2 17"))
	tk.MustQuery(" SELECT a, b, sum(c) FROM t WHERE a in ('x', 'xx', 'z') GROUP BY a,b ORDER BY a,b").Check(testkit.Rows(
		"xx 1 6]\n[xx 2 19"))
	tk.MustQuery(" SELECT e, b, sum(c) FROM t WHERE e in ('y1', 'y', 'y3') GROUP BY e,b ORDER BY e,b").Check(testkit.Rows(
		"y 1 6]\n[y 2 4]\n[y1 1 2]\n[y1 2 11]\n[y3 1 3]\n[y3 2 11"))
}
