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

package issuetest

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// It's a case for Columns in tableScan and indexScan with double reader
func TestIssue43461(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, index b(b), index b_c(b, c)) partition by hash(a) partitions 4;")
	tk.MustExec("analyze table t")

	stmt, err := parser.New().ParseOneStmt("select * from t use index(b) where b > 1 order by b limit 1", "", "")
	require.NoError(t, err)

	p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, domain.InfoSchema())
	require.NoError(t, err)
	require.NotNil(t, p)

	var idxLookUpPlan *core.PhysicalIndexLookUpReader
	var ok bool

	for {
		idxLookUpPlan, ok = p.(*core.PhysicalIndexLookUpReader)
		if ok {
			break
		}
		p = p.(base.PhysicalPlan).Children()[0]
	}
	require.True(t, ok)

	is := idxLookUpPlan.IndexPlans[0].(*core.PhysicalIndexScan)
	ts := idxLookUpPlan.TablePlans[0].(*core.PhysicalTableScan)

	require.NotEqual(t, is.Columns, ts.Columns)
}

func Test53726(t *testing.T) {
	// test for RemoveUnnecessaryFirstRow
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t7(c int); ")
	tk.MustExec("insert into t7 values (575932053), (-258025139);")
	tk.MustQuery("select distinct cast(c as decimal), cast(c as signed) from t7").
		Sort().Check(testkit.Rows("-258025139 -258025139", "575932053 575932053"))
	tk.MustQuery("explain select distinct cast(c as decimal), cast(c as signed) from t7").
		Check(testkit.Rows(
			"HashAgg_8 8000.00 root  group by:Column#7, Column#8, funcs:firstrow(Column#7)->Column#3, funcs:firstrow(Column#8)->Column#4",
			"└─TableReader_9 8000.00 root  data:HashAgg_4",
			"  └─HashAgg_4 8000.00 cop[tikv]  group by:cast(test.t7.c, bigint(22) BINARY), cast(test.t7.c, decimal(10,0) BINARY), ",
			"    └─TableFullScan_7 10000.00 cop[tikv] table:t7 keep order:false, stats:pseudo"))

	tk.MustExec("analyze table t7 all columns")
	tk.MustQuery("select distinct cast(c as decimal), cast(c as signed) from t7").
		Sort().
		Check(testkit.Rows("-258025139 -258025139", "575932053 575932053"))
	tk.MustQuery("explain select distinct cast(c as decimal), cast(c as signed) from t7").
		Check(testkit.Rows(
			"HashAgg_6 2.00 root  group by:Column#11, Column#12, funcs:firstrow(Column#11)->Column#3, funcs:firstrow(Column#12)->Column#4",
			"└─Projection_12 2.00 root  cast(test.t7.c, decimal(10,0) BINARY)->Column#11, cast(test.t7.c, bigint(22) BINARY)->Column#12",
			"  └─TableReader_11 2.00 root  data:TableFullScan_10",
			"    └─TableFullScan_10 2.00 cop[tikv] table:t7 keep order:false"))
}

func TestIssue54449(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE p ( groupid bigint(20) DEFAULT NULL, KEY k1 (groupid));")
	tk.MustExec(`CREATE TABLE g (groupid bigint(20) DEFAULT NULL,parentid bigint(20) NOT NULL,KEY k1 (parentid),KEY k2 (groupid,parentid));`)
	tk.MustExec(`set tidb_opt_enable_hash_join=off;`)
	tk.MustQuery(`explain WITH RECURSIVE w(gid) AS (
  SELECT
    groupId
  FROM
    p
  UNION
  SELECT
    g.groupId
  FROM
    g
    JOIN w ON g.parentId = w.gid
)
SELECT
  1
FROM
  g
WHERE
  g.groupId IN (
    SELECT
      gid
    FROM
      w
  );`).Check(testkit.Rows(
		"Projection_54 9990.00 root  1->Column#17",
		"└─IndexJoin_59 9990.00 root  inner join, inner:IndexReader_58, outer key:test.p.groupid, inner key:test.g.groupid, equal cond:eq(test.p.groupid, test.g.groupid)",
		"├─HashAgg_75(Build) 12800.00 root  group by:test.p.groupid, funcs:firstrow(test.p.groupid)->test.p.groupid",
		"│ └─Selection_72 12800.00 root  not(isnull(test.p.groupid))",
		"│   └─CTEFullScan_73 16000.00 root CTE:w data:CTE_0",
		"└─IndexReader_58(Probe) 9990.00 root  index:Selection_57",
		"└─Selection_57 9990.00 cop[tikv]  not(isnull(test.g.groupid))",
		"└─IndexRangeScan_56 10000.00 cop[tikv] table:g, index:k2(groupid, parentid) range: decided by [eq(test.g.groupid, test.p.groupid)], keep order:false, stats:pseudo",
		"CTE_0 16000.00 root  Recursive CTE",
		"├─IndexReader_24(Seed Part) 10000.00 root  index:IndexFullScan_23",
		"│ └─IndexFullScan_23 10000.00 cop[tikv] table:p, index:k1(groupid) keep order:false, stats:pseudo",
		"└─IndexHashJoin_34(Recursive Part) 10000.00 root  inner join, inner:IndexLookUp_31, outer key:test.p.groupid, inner key:test.g.parentid, equal cond:eq(test.p.groupid, test.g.parentid)",
		"  ├─Selection_51(Build) 8000.00 root  not(isnull(test.p.groupid))",
		"  │ └─CTETable_52 10000.00 root  Scan on CTE_0",
		"  └─IndexLookUp_31(Probe) 10000.00 root  ",
		"    ├─IndexRangeScan_29(Build) 10000.00 cop[tikv] table:g, index:k1(parentid) range: decided by [eq(test.g.parentid, test.p.groupid)], keep order:false, stats:pseudo",
		"    └─TableRowIDScan_30(Probe) 10000.00 cop[tikv] table:g keep order:false, stats:pseudo]"))
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
}
