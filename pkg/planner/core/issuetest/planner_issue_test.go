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
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func instanceCachedPlanForPrepared(t *testing.T, tk *testkit.TestKit, name string) base.Plan {
	prepared, err := tk.Session().GetSessionVars().GetPreparedStmtByName(name)
	require.NoError(t, err)
	cacheKey, _, cacheable, reason, err := core.NewPreparedPlanCacheKey(tk.Session(), prepared.(*core.PlanCacheStmt))
	require.NoError(t, err)
	require.True(t, cacheable, reason)
	raw, ok := domain.GetDomain(tk.Session()).GetInstancePlanCache().Get(cacheKey, nil)
	require.True(t, ok)
	return raw.(*core.PlanCacheValue).Plan
}

func sessionCachedPlanForPrepared(t *testing.T, tk *testkit.TestKit, name string) base.Plan {
	prepared, err := tk.Session().GetSessionVars().GetPreparedStmtByName(name)
	require.NoError(t, err)
	cacheKey, _, cacheable, reason, err := core.NewPreparedPlanCacheKey(tk.Session(), prepared.(*core.PlanCacheStmt))
	require.NoError(t, err)
	require.True(t, cacheable, reason)
	raw, ok := tk.Session().GetSessionPlanCache().Get(cacheKey, nil)
	require.True(t, ok)
	return raw.(*core.PlanCacheValue).Plan
}

func dmlExpressionGraphFingerprint(t testing.TB, plan base.Plan) string {
	fieldType := func(ft *types.FieldType) string {
		if ft == nil {
			return "<nil>"
		}
		return fmt.Sprintf("tp=%d/cs=%s/co=%s/fl=%d/len=%d/dec=%d", ft.GetType(), ft.GetCharset(),
			ft.GetCollate(), ft.GetFlag(), ft.GetFlen(), ft.GetDecimal())
	}
	var expressionFingerprint func(expression.Expression) string
	expressionFingerprint = func(expr expression.Expression) string {
		switch e := expr.(type) {
		case nil:
			return "<nil>"
		case *expression.Constant:
			value, err := e.Value.ToString()
			require.NoError(t, err)
			marker := "nil"
			if e.ParamMarker != nil {
				marker = "present"
			}
			return fmt.Sprintf("constant@%p{%s,kind=%d,value=%q,bytes=%x,marker=%s,deferred=%s}",
				e, fieldType(e.RetType), e.Value.Kind(), value, e.Value.GetBytes(), marker,
				expressionFingerprint(e.DeferredExpr))
		case *expression.Column:
			return fmt.Sprintf("column@%p{%s,id=%d,uid=%d,index=%d,name=%q,hidden=%t,prefix=%t,in=%t,virtual=%s}",
				e, fieldType(e.RetType), e.ID, e.UniqueID, e.Index, e.OrigName, e.IsHidden, e.IsPrefix,
				e.InOperand, expressionFingerprint(e.VirtualExpr))
		case *expression.ScalarFunction:
			args := e.GetArgs()
			parts := make([]string, len(args))
			for i := range args {
				parts[i] = expressionFingerprint(args[i])
			}
			argsBacking := "<nil>"
			if args != nil {
				argsBacking = "<empty>"
				if len(args) > 0 {
					argsBacking = fmt.Sprintf("%p", &args[0])
				}
			}
			return fmt.Sprintf("scalar@%p{name=%s,%s,args=nil:%t/%d/%d/back=%s:%s}", e, e.FuncName.L,
				fieldType(e.RetType), args == nil, len(args), cap(args), argsBacking, strings.Join(parts, ","))
		default:
			return fmt.Sprintf("expr@%p{%T,type=%s}", expr, expr, fieldType(expr.GetType(nil)))
		}
	}
	assignment := func(a *expression.Assignment) string {
		if a == nil {
			return "<nil-assignment>"
		}
		return fmt.Sprintf("assignment@%p{name=%s,col=%s,expr=%s,lazy=%v}", a, a.ColName.O,
			expressionFingerprint(a.Col), expressionFingerprint(a.Expr), a.LazyErr)
	}
	var b strings.Builder
	switch p := plan.(type) {
	case *core.Insert:
		listsBacking := "<nil>"
		if p.Lists != nil {
			listsBacking = "<empty>"
			if len(p.Lists) > 0 {
				listsBacking = fmt.Sprintf("%p", &p.Lists[0])
			}
		}
		fmt.Fprintf(&b, "insert@%p{lists=nil:%t/%d/%d/back=%s", p, p.Lists == nil, len(p.Lists),
			cap(p.Lists), listsBacking)
		for _, list := range p.Lists {
			listBacking := "<nil>"
			if list != nil {
				listBacking = "<empty>"
				if len(list) > 0 {
					listBacking = fmt.Sprintf("%p", &list[0])
				}
			}
			fmt.Fprintf(&b, ",list=nil:%t/%d/%d/back=%s", list == nil, len(list), cap(list), listBacking)
			for _, expr := range list {
				fmt.Fprintf(&b, ",%s", expressionFingerprint(expr))
			}
		}
		onDuplicateBacking := "<nil>"
		if p.OnDuplicate != nil {
			onDuplicateBacking = "<empty>"
			if len(p.OnDuplicate) > 0 {
				onDuplicateBacking = fmt.Sprintf("%p", &p.OnDuplicate[0])
			}
		}
		fmt.Fprintf(&b, ",ondup=nil:%t/%d/%d/back=%s", p.OnDuplicate == nil, len(p.OnDuplicate),
			cap(p.OnDuplicate), onDuplicateBacking)
		for _, a := range p.OnDuplicate {
			fmt.Fprintf(&b, ",%s", assignment(a))
		}
		b.WriteString("}")
	case *core.Update:
		orderedBacking := "<nil>"
		if p.OrderedList != nil {
			orderedBacking = "<empty>"
			if len(p.OrderedList) > 0 {
				orderedBacking = fmt.Sprintf("%p", &p.OrderedList[0])
			}
		}
		fmt.Fprintf(&b, "update@%p{ordered=nil:%t/%d/%d/back=%s", p, p.OrderedList == nil,
			len(p.OrderedList), cap(p.OrderedList), orderedBacking)
		for _, a := range p.OrderedList {
			fmt.Fprintf(&b, ",%s", assignment(a))
		}
		b.WriteString("}")
	default:
		require.Failf(t, "unsupported DML plan", "%T", plan)
	}
	return b.String()
}

func assertNonFKCachedUpdateAccessPath(t *testing.T, name string, plan base.Plan) {
	update, ok := plan.(*core.Update)
	require.True(t, ok, "%s cached plan must be Update, got %T", name, plan)
	switch name {
	case "phase_c_non_fk_point":
		point, ok := update.SelectPlan.(*core.PointGetPlan)
		require.True(t, ok, "%s must cache a primary-key PointGet, got %T", name, update.SelectPlan)
		require.Nil(t, point.IndexInfo, "primary-key PointGet must use the table handle")
		require.NotNil(t, point.Handle, "primary-key PointGet must carry its resolved table handle")
		require.NotNil(t, point.HandleConstant, "primary-key PointGet must retain its handle parameter")
		require.NotNil(t, point.HandleConstant.ParamMarker, "primary-key locator must come from the prepared parameter")
		require.Equal(t, int64(1), point.Handle.IntValue(), "primary-key locator must retain the published handle")
		require.Equal(t, "phase_c_non_fk", point.TblInfo.Name.L)
	case "phase_c_non_fk_unique":
		point, ok := update.SelectPlan.(*core.PointGetPlan)
		require.True(t, ok, "%s must cache a unique-index PointGet, got %T", name, update.SelectPlan)
		require.NotNil(t, point.IndexInfo, "unique PointGet must carry the selected index")
		require.Equal(t, "uk", point.IndexInfo.Name.L)
		require.Len(t, point.IndexConstants, 1, "unique PointGet must retain its index parameter")
		require.NotNil(t, point.IndexConstants[0].ParamMarker, "unique-index locator must come from the prepared parameter")
		require.Len(t, point.IndexValues, 1, "unique PointGet must resolve one index value")
		require.Equal(t, int64(22), point.IndexValues[0].GetInt64(), "unique-index locator must retain the published value")
		require.Equal(t, "phase_c_non_fk", point.TblInfo.Name.L)
	case "phase_c_non_fk_batch":
		batch, ok := update.SelectPlan.(*core.BatchPointGetPlan)
		require.True(t, ok, "%s must cache a primary-handle BatchPointGet, got %T", name, update.SelectPlan)
		require.Nil(t, batch.IndexInfo, "primary-key BatchPointGet must use table handles")
		require.Len(t, batch.HandleParams, 2, "BatchPointGet must retain both handle parameters")
		for i, param := range batch.HandleParams {
			require.NotNil(t, param.ParamMarker, "BatchPointGet locator %d must come from a prepared parameter", i)
		}
		require.Len(t, batch.Handles, 2, "BatchPointGet must resolve both table handles")
		handles := []int64{batch.Handles[0].IntValue(), batch.Handles[1].IntValue()}
		require.ElementsMatch(t, []int64{1, 2}, handles, "BatchPointGet must retain both published table handles")
		require.Equal(t, "phase_c_non_fk", batch.TblInfo.Name.L)
	default:
		require.Failf(t, "unknown non-FK UPDATE access-path case", "%s", name)
	}
}

// It's a case for Columns in tableScan and indexScan with double reader
func TestIssue43461(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, index b(b), index b_c(b, c)) partition by hash(a) partitions 4;")
	tk.MustExec("analyze table t")

	stmt, err := parser.New().ParseOneStmt("select * from t use index(b) where b > 1 order by b limit 1", "", "")
	require.NoError(t, err)

	nodeW := resolve.NewNodeW(stmt)
	p, _, err := planner.Optimize(context.TODO(), tk.Session(), nodeW, domain.InfoSchema())
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

func TestIssue54535(t *testing.T) {
	// test for tidb_enable_inl_join_inner_multi_pattern system variable
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set session tidb_enable_inl_join_inner_multi_pattern='ON'")
	tk.MustExec("create table ta(a1 int, a2 int, a3 int, index idx_a(a1))")
	tk.MustExec("create table tb(b1 int, b2 int, b3 int, index idx_b(b1))")
	tk.MustExec("analyze table ta")
	tk.MustExec("analyze table tb")

	tk.MustQuery("explain SELECT /*+ inl_join(tmp) */ * FROM ta, (SELECT b1, COUNT(b3) AS cnt FROM tb GROUP BY b1, b2) as tmp where ta.a1 = tmp.b1").
		Check(testkit.Rows(
			"Projection_9 9990.00 root  test.ta.a1, test.ta.a2, test.ta.a3, test.tb.b1, Column#9",
			"└─IndexJoin_18 9990.00 root  inner join, inner:HashAgg_16, outer key:test.ta.a1, inner key:test.tb.b1, equal cond:eq(test.ta.a1, test.tb.b1)",
			"  ├─TableReader_45(Build) 9990.00 root  data:Selection_44",
			"  │ └─Selection_44 9990.00 cop[tikv]  not(isnull(test.ta.a1))",
			"  │   └─TableFullScan_43 10000.00 cop[tikv] table:ta keep order:false, stats:pseudo",
			"  └─HashAgg_16(Probe) 9990.00 root  group by:test.tb.b1, test.tb.b2, funcs:count(Column#11)->Column#9, funcs:firstrow(test.tb.b1)->test.tb.b1",
			"    └─IndexLookUp_17 9990.00 root  ",
			"      ├─Selection_14(Build) 9990.00 cop[tikv]  not(isnull(test.tb.b1))",
			"      │ └─IndexRangeScan_12 10000.00 cop[tikv] table:tb, index:idx_b(b1) range: decided by [eq(test.tb.b1, test.ta.a1)], keep order:false, stats:pseudo",
			"      └─HashAgg_15(Probe) 9990.00 cop[tikv]  group by:test.tb.b1, test.tb.b2, funcs:count(test.tb.b3)->Column#11",
			"        └─TableRowIDScan_13 9990.00 cop[tikv] table:tb keep order:false, stats:pseudo"))
	// test for issues/55169
	tk.MustExec("create table t1(col_1 int, index idx_1(col_1));")
	tk.MustExec("create table t2(col_1 int, col_2 int, index idx_2(col_1));")
	tk.MustQuery("select /*+ inl_join(tmp) */ * from t1 inner join (select col_1, group_concat(col_2) from t2 group by col_1) tmp on t1.col_1 = tmp.col_1;").Check(testkit.Rows())
	tk.MustQuery("select /*+ inl_join(tmp) */ * from t1 inner join (select col_1, group_concat(distinct col_2 order by col_2) from t2 group by col_1) tmp on t1.col_1 = tmp.col_1;").Check(testkit.Rows())
}

func TestIssue53175(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a int)`)
	tk.MustExec(`set @@sql_mode = default`)
	tk.MustQuery(`select @@sql_mode REGEXP 'ONLY_FULL_GROUP_BY'`).Check(testkit.Rows("1"))
	tk.MustContainErrMsg(`select * from t group by null`, "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.a' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	tk.MustExec(`create view v as select * from t group by null`)
	tk.MustContainErrMsg(`select * from v`, "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.a' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	tk.MustExec(`set @@sql_mode = ''`)
	tk.MustQuery(`select * from t group by null`)
	tk.MustQuery(`select * from v`)
}

func TestIssue58476(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("CREATE TABLE t3 (id int PRIMARY KEY,c1 varchar(256),c2 varchar(256) GENERATED ALWAYS AS (concat(c1, c1)) VIRTUAL,KEY (id));")
	tk.MustExec("insert into t3(id, c1) values (50, 'c');")
	tk.MustQuery("SELECT /*+ USE_INDEX_MERGE(`t3`)*/ id FROM `t3` WHERE c2 BETWEEN 'a' AND 'b' GROUP BY id HAVING id < 100 or id > 0;").Check(testkit.Rows())
	tk.MustQuery("explain format='brief' SELECT /*+ USE_INDEX_MERGE(`t3`)*/ id FROM `t3` WHERE c2 BETWEEN 'a' AND 'b' GROUP BY id HAVING id < 100 or id > 0;").
		Check(testkit.Rows(
			`Projection 249.75 root  test.t3.id`,
			`└─Selection 249.75 root  ge(test.t3.c2, "a"), le(test.t3.c2, "b")`,
			`  └─Projection 9990.00 root  test.t3.id, test.t3.c2`,
			`    └─IndexMerge 9990.00 root  type: union`,
			`      ├─IndexRangeScan(Build) 3323.33 cop[tikv] table:t3, index:id(id) range:[-inf,100), keep order:false, stats:pseudo`,
			`      ├─TableRangeScan(Build) 3333.33 cop[tikv] table:t3 range:(0,+inf], keep order:false, stats:pseudo`,
			`      └─TableRowIDScan(Probe) 9990.00 cop[tikv] table:t3 keep order:false, stats:pseudo`))
}

func TestIssues57583(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1(id int, v1 int, v2 int, v3 int);")
	tk.MustExec(" create table t2(id int, v1 int, v2 int, v3 int);")
	tk.MustQuery("explain select t1.id from t1 join t2 on t1.v1 = t2.v2 intersect select t1.id from t1 join t2 on t1.v1 = t2.v2;").Check(testkit.Rows(
		"HashJoin_15 6393.60 root  semi join, equal:[nulleq(test.t1.id, test.t1.id)]",
		"├─HashJoin_26(Build) 12487.50 root  inner join, equal:[eq(test.t1.v1, test.t2.v2)]",
		"│ ├─TableReader_33(Build) 9990.00 root  data:Selection_32",
		"│ │ └─Selection_32 9990.00 cop[tikv]  not(isnull(test.t2.v2))",
		"│ │   └─TableFullScan_31 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"│ └─TableReader_30(Probe) 9990.00 root  data:Selection_29",
		"│   └─Selection_29 9990.00 cop[tikv]  not(isnull(test.t1.v1))",
		"│     └─TableFullScan_28 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
		"└─HashAgg_16(Probe) 7992.00 root  group by:test.t1.id, funcs:firstrow(test.t1.id)->test.t1.id",
		"  └─HashJoin_17 12487.50 root  inner join, equal:[eq(test.t1.v1, test.t2.v2)]",
		"    ├─TableReader_24(Build) 9990.00 root  data:Selection_23",
		"    │ └─Selection_23 9990.00 cop[tikv]  not(isnull(test.t2.v2))",
		"    │   └─TableFullScan_22 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"    └─TableReader_21(Probe) 9990.00 root  data:Selection_20",
		"      └─Selection_20 9990.00 cop[tikv]  not(isnull(test.t1.v1))",
		"        └─TableFullScan_19 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
}

func TestIssue59643(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustQuery(`explain format='brief' SELECT
    base.c1,
    base.c2,
    base2.c1 AS base2_c1,
    base2.c3
FROM
    (SELECT distinct 1 AS c1, 'Alice' AS c2 UNION SELECT NULL AS c1, 'Bob' AS c2) AS base
INNER JOIN
    (SELECT 1 AS c1, 100 AS c3 UNION SELECT NULL AS c1, NULL AS c3) AS base2
ON base.c1 <=> base2.c1;
`).Check(testkit.Rows(
		"HashJoin 2.00 root  inner join, equal:[nulleq(Column#5, Column#11)]",
		"├─HashAgg(Build) 2.00 root  group by:Column#5, Column#6, funcs:firstrow(Column#5)->Column#5, funcs:firstrow(Column#6)->Column#6",
		"│ └─Union 2.00 root  ",
		"│   ├─HashAgg 1.00 root  group by:1, funcs:firstrow(1)->Column#1, funcs:firstrow(\"Alice\")->Column#2",
		"│   │ └─TableDual 1.00 root  rows:1",
		"│   └─Projection 1.00 root  <nil>->Column#5, Bob->Column#6",
		"│     └─TableDual 1.00 root  rows:1",
		"└─HashAgg(Probe) 2.00 root  group by:Column#11, Column#12, funcs:firstrow(Column#11)->Column#11, funcs:firstrow(Column#12)->Column#12",
		"  └─Union 2.00 root  ",
		"    ├─Projection 1.00 root  1->Column#11, 100->Column#12",
		"    │ └─TableDual 1.00 root  rows:1",
		"    └─Projection 1.00 root  <nil>->Column#11, <nil>->Column#12",
		"      └─TableDual 1.00 root  rows:1"))
	tk.MustQuery(`SELECT
    base.c1,
    base.c2,
    base2.c1 AS base2_c1,
    base2.c3
FROM
    (SELECT distinct 1 AS c1, 'Alice' AS c2 UNION SELECT NULL AS c1, 'Bob' AS c2) AS base
INNER JOIN
    (SELECT 1 AS c1, 100 AS c3 UNION SELECT NULL AS c1, NULL AS c3) AS base2
ON base.c1 <=> base2.c1;`).Sort().Check(testkit.Rows(
		"1 Alice 1 100",
		"<nil> Bob <nil> <nil>"))
	tk.MustQuery(`SELECT
    base.c1,
    base.c2,
    base2.c1 AS base2_c1,
    base2.c3
FROM
    (SELECT 1 AS c1, 'Alice' AS c2 UNION SELECT NULL AS c1, 'Bob' AS c2) AS base
INNER JOIN
    (SELECT 1 AS c1, 100 AS c3 UNION SELECT NULL AS c1, NULL AS c3) AS base2
ON base.c1 <=> base2.c1;`).Sort().Check(testkit.Rows(
		"1 Alice 1 100",
		"<nil> Bob <nil> <nil>"))
}

func TestIssue59902(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1(a int primary key, b int);")
	tk.MustExec("create table t2(a int, b int, key idx(a));")
	tk.MustExec(`INSERT INTO t1 (a, b) VALUES (1, 100), (2, 200), (3, 300);`)
	tk.MustExec(`INSERT INTO t2 (a, b) VALUES (1, 10), (1, 20), (2, 30), (4, 40);`)
	tk.MustExec("set tidb_enable_inl_join_inner_multi_pattern=on;")
	tk.MustQuery("explain format='brief' select t1.b,(select count(*) from t2 where t2.a=t1.a) as a from t1 where t1.a=1;").
		Check(testkit.Rows(
			"Projection 8.00 root  test.t1.b, ifnull(Column#9, 0)->Column#9",
			"└─HashJoin 8.00 root  CARTESIAN left outer join",
			"  ├─Point_Get(Build) 1.00 root table:t1 handle:1",
			"  └─StreamAgg(Probe) 8.00 root  group by:test.t2.a, funcs:count(Column#11)->Column#9",
			"    └─IndexReader 8.00 root  index:StreamAgg",
			"      └─StreamAgg 8.00 cop[tikv]  group by:test.t2.a, funcs:count(1)->Column#11",
			"        └─IndexRangeScan 10.00 cop[tikv] table:t2, index:idx(a) range:[1,1], keep order:true, stats:pseudo"))
	tk.MustQuery("select t1.b,(select count(*) from t2 where t2.a=t1.a) as a from t1 where t1.a=1;").Check(testkit.Rows("100 2"))
}

func TestIssue61118(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test;")
	tk1.MustExec("set global tidb_enable_instance_plan_cache = 1;")
	tk1.MustExec("create table t(a timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), b int, c int, primary key(a), unique key(b,c));")
	tk1.MustExec("insert into t(b,c) value (1,1);")
	tk1.MustExec("prepare stmt from 'update t set a = NOW(6) where b = ? and c = ?';")
	tk1.MustExec("set @a = 1;")
	tk1.MustExec("execute stmt using @a, @a;")
	tk1.MustExec("set time_zone='+1:00';")
	tk1.MustExec("execute stmt using @a, @a;")
	tk1.MustExec("execute stmt using @a, @a;")
	tk1.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test;")
	tk2.MustExec("prepare stmt from 'update t set a = NOW(6) where b = ? and c = ?';")
	tk2.MustExec("set @a = 1;")
	tk2.MustExec("execute stmt using @a, @a;")
	tk2.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk2.MustExec("admin check table t;")
}

func TestJoinReorderWithAddSelection(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec(`create table t0(vkey integer, c3 varchar(0));`)
	tk.MustExec(`create table t1(vkey integer, c10 integer);`)
	tk.MustExec(`create table t2(c12 integer, c13 integer, c14 varchar(0), c15 double);`)
	tk.MustExec(`create table t3(vkey varchar(0), c20 integer);`)
	tk.MustQuery(`explain format=brief select 0 from t2 join(t3 join t0 a on 0) left join(t1 b left join t1 c on 0) on(c20 = b.vkey) on(c13 = a.vkey) join(select c14 d from(t2 join t3 on c12 = vkey)) e on(c3 = d) where nullif(c15, case when(c.c10) then 0 end);`).Check(testkit.Rows(
		`Projection 0.00 root  0->Column#26`,
		`└─HashJoin 0.00 root  inner join, equal:[eq(Column#27, Column#28)]`,
		`  ├─HashJoin(Build) 0.00 root  inner join, equal:[eq(test.t0.c3, test.t2.c14)]`,
		`  │ ├─Selection(Build) 0.00 root  if(eq(test.t2.c15, cast(case(test.t1.c10, 0), double BINARY)), NULL, test.t2.c15)`,
		`  │ │ └─HashJoin 0.00 root  left outer join, equal:[eq(test.t3.c20, test.t1.vkey)]`,
		`  │ │   ├─HashJoin(Build) 0.00 root  inner join, equal:[eq(test.t0.vkey, test.t2.c13)]`,
		`  │ │   │ ├─TableDual(Build) 0.00 root  rows:0`,
		`  │ │   │ └─TableReader(Probe) 9990.00 root  data:Selection`,
		`  │ │   │   └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.c13))`,
		`  │ │   │     └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo`,
		`  │ │   └─HashJoin(Probe) 9990.00 root  CARTESIAN left outer join`,
		`  │ │     ├─TableDual(Build) 0.00 root  rows:0`,
		`  │ │     └─TableReader(Probe) 9990.00 root  data:Selection`,
		`  │ │       └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.vkey))`,
		`  │ │         └─TableFullScan 10000.00 cop[tikv] table:b keep order:false, stats:pseudo`,
		`  │ └─Projection(Probe) 9990.00 root  test.t2.c14, cast(test.t2.c12, double BINARY)->Column#27`,
		`  │   └─TableReader 9990.00 root  data:Selection`,
		`  │     └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.c14))`,
		`  │       └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo`,
		`  └─Projection(Probe) 10000.00 root  cast(test.t3.vkey, double BINARY)->Column#28`,
		`    └─TableReader 10000.00 root  data:TableFullScan`,
		`      └─TableFullScan 10000.00 cop[tikv] table:t3 keep order:false, stats:pseudo`))
}

func TestIssue66339(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t(a int, key(a))")
	tk.MustExec("set @a=1")
	tk.MustHavePlan("select a from t where a=@a", "IndexRangeScan")
	tk.MustExec("set @A=1")
	tk.MustHavePlan("select a from t where a=@A", "IndexRangeScan")
}

func TestUserVarAggregateRewriteInScalarSubquery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select (select sum(1))").Check(testkit.Rows("1"))
	tk.MustQuery("select (select @a:=sum(1))").Check(testkit.Rows("1"))
}

func TestOnlyFullGroupCantFeelUnaryConstant(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int);")
	tk.MustQuery("select a,min(a) from t where a=-1;").Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("select a,min(a) from t where -1=a;").Check(testkit.Rows("<nil> <nil>"))
}

func TestIssue64645(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t(a int, b int, c int)")
	tk.MustExec("create table tt(a int, index idx(a))")
	tk.MustExec("prepare stmt from 'select * from t where b > (select a from tt where tt.a = t.a and t.b  >= ? and t.b <= ?)'")
	tk.MustExec("set @a=1, @b=100")
	tk.MustExec("execute stmt using @a, @b")
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).MultiCheckContain([]string{"IndexRangeScan"})
}

func TestIssue67534PointUpdateAssignmentCasting(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@sql_mode = default")

	// issue-63455-point-update-negative-to-unsigned
	{
		tk.MustExec("drop table if exists foo")
		tk.MustExec("create table foo (id int primary key, bing bigint unsigned default null)")
		tk.MustExec("insert into foo values (1, 1), (2, null)")

		// With a subquery in assignment, the planner won't use the fast point-update path.
		// It should follow the normal cast behavior and fail in strict SQL mode.
		tk.MustGetErrCode("update foo set bing = (select -1) where id = 2", errno.ErrWarnDataOutOfRange)

		// Fast point-update path should behave the same; before the fix for #63455 it wrapped -1 to MAX_UINT64 without error.
		tk.MustGetErrCode("update foo set bing = -1 where id = 2", errno.ErrWarnDataOutOfRange)
	}

	// issue-67534-point-update-unsigned-decimal-negative
	{
		tk.MustExec("drop table if exists foo")
		tk.MustExec("create table foo (id int primary key, bing decimal(10, 0) unsigned default null)")
		tk.MustExec("insert into foo values (1, 1), (2, null)")

		// With a subquery in assignment, the planner won't use the fast point-update path.
		// It should follow the normal cast behavior and fail in strict SQL mode.
		tk.MustGetErrCode("update foo set bing = (select -1) where id = 2", errno.ErrWarnDataOutOfRange)

		// Fast point-update path should behave the same; before the fix for #67534 it might wrap -1 using CAST semantics.
		tk.MustGetErrCode("update foo set bing = -1 where id = 2", errno.ErrWarnDataOutOfRange)
	}

	// issue-67534-point-update-unsigned-real-negative
	{
		tk.MustExec("drop table if exists foo")
		tk.MustExec("create table foo (id int primary key, bing double unsigned default null)")
		tk.MustExec("insert into foo values (1, 1), (2, null)")

		// With a subquery in assignment, the planner won't use the fast point-update path.
		// It should follow the normal cast behavior and fail in strict SQL mode.
		tk.MustGetErrCode("update foo set bing = (select -1) where id = 2", errno.ErrWarnDataOutOfRange)

		// Fast point-update path should behave the same; before the fix for #67534 it might wrap -1 using CAST semantics.
		tk.MustGetErrCode("update foo set bing = -1 where id = 2", errno.ErrWarnDataOutOfRange)
	}

	// issue-67534-point-update-set-int-assignment
	{
		tk.MustExec("drop table if exists foo")
		tk.MustExec("create table foo (id int primary key, bing set('1','2','3') default null)")
		tk.MustExec("insert into foo values (1, null), (2, null)")

		// With a subquery in assignment, the planner won't use the fast point-update path.
		tk.MustExec("update foo set bing = (select 3) where id = 1")

		// Fast point-update path should have the same enum/set-as-int behavior as the normal UPDATE path.
		tk.MustExec("update foo set bing = 3 where id = 2")

		tk.MustQuery("select id, bing + 0, bing from foo order by id").Check(testkit.Rows(
			"1 3 1,2",
			"2 3 1,2",
		))
	}
}

func TestFix69990PointUpdateCacheConversionOracle(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_plan_cache_dml_param_type_match = ON")
	tk.MustExec("create table conversion_oracle (id int primary key, i tinyint, b bigint, d decimal(10,2))")
	tk.MustExec("insert into conversion_oracle values (1, 0, 0, 0)")

	type outcome struct {
		err      string
		warnings [][]any
		row      [][]any
	}
	run := func(cacheEnabled bool, sqlMode, assignment, warmValue, testValue string) (outcome, bool) {
		tk.MustExec("set tidb_enable_prepared_plan_cache = 1")
		tk.MustExec("set @@sql_mode = ?", sqlMode)
		tk.MustExec("prepare conversion_stmt from 'update conversion_oracle set " + assignment + " where id=1'")
		if !cacheEnabled {
			tk.MustExec("set tidb_enable_prepared_plan_cache = 0")
		}
		if cacheEnabled {
			tk.MustExec("set @v = ?", warmValue)
			tk.MustExec("execute conversion_stmt using @v")
			tk.MustExec("update conversion_oracle set i=0, b=0, d=0 where id=1")
		}
		if testValue == "<null>" {
			tk.MustExec("set @v = null")
		} else {
			tk.MustExec("set @v = ?", testValue)
		}
		err := tk.ExecToErr("execute conversion_stmt using @v")
		foundInPlanCache := tk.Session().GetSessionVars().FoundInPlanCache
		warnings := tk.MustQuery("show warnings").Rows()
		filteredWarnings := warnings[:0]
		for _, warning := range warnings {
			if len(warning) == 3 && warning[1] == "1105" && warning[2] == "skip prepared plan-cache: plan cache is disabled" {
				continue
			}
			filteredWarnings = append(filteredWarnings, warning)
		}
		result := outcome{
			warnings: filteredWarnings,
			row:      tk.MustQuery("select i, b, d from conversion_oracle where id=1").Rows(),
		}
		if err != nil {
			result.err = err.Error()
		}
		tk.MustExec("deallocate prepare conversion_stmt")
		return result, foundInPlanCache
	}

	cases := []struct {
		name       string
		sqlMode    string
		assignment string
		warmValue  string
		testValue  string
	}{
		{name: "strict invalid tinyint", sqlMode: "STRICT_TRANS_TABLES", assignment: "i=?", warmValue: "1", testValue: "x"},
		{name: "strict tinyint overflow", sqlMode: "STRICT_TRANS_TABLES", assignment: "i=?", warmValue: "1", testValue: "999"},
		{name: "non-strict tinyint overflow", sqlMode: "", assignment: "i=?", warmValue: "1", testValue: "999"},
		{name: "non-strict bigint overflow", sqlMode: "", assignment: "b=?", warmValue: "1", testValue: "9223372036854775808"},
		{name: "non-strict decimal rounding", sqlMode: "", assignment: "d=?", warmValue: "1.00", testValue: "1.239"},
		{name: "sql null", sqlMode: "STRICT_TRANS_TABLES", assignment: "i=?", warmValue: "1", testValue: "<null>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tk.MustExec("update conversion_oracle set i=0, b=0, d=0 where id=1")
			replan, _ := run(false, tc.sqlMode, tc.assignment, tc.warmValue, tc.testValue)
			tk.MustExec("update conversion_oracle set i=0, b=0, d=0 where id=1")
			cached, foundInPlanCache := run(true, tc.sqlMode, tc.assignment, tc.warmValue, tc.testValue)
			require.True(t, foundInPlanCache, "conversion must execute through the target-matched cached plan")
			require.Equal(t, replan, cached)
		})
	}
}

func TestFix69990PhaseBUpdateInsertBindingCacheOracle(t *testing.T) {
	ctx := context.WithValue(context.Background(), core.PlanCacheKeyEnableInstancePlanCache{}, true)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExecWithContext(ctx, "use test")
	tk.MustExecWithContext(ctx, "set tidb_enable_plan_cache_dml_param_type_match=ON")
	tk.MustExecWithContext(ctx, "create table phase_b(id int primary key, grp int, v bigint, s varchar(20))")
	tk.MustExecWithContext(ctx, "insert into phase_b values(1,10,0,'a'),(2,10,0,'a')")

	// Scan UPDATE: write markers use target matching while the WHERE marker
	// remains strict. Rotating v from integer to string still hits the plan.
	tk.MustExecWithContext(ctx, "prepare update_scan from 'update phase_b set v=?, s=? where grp=?'")
	tk.MustExecWithContext(ctx, "set @v=11, @s='first', @grp=10")
	tk.MustExecWithContext(ctx, "execute update_scan using @v,@s,@grp")
	tk.MustExecWithContext(ctx, "set @v='22', @s='second'")
	tk.MustExecWithContext(ctx, "execute update_scan using @v,@s,@grp")
	tk.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQueryWithContext(ctx, "select id,v,s from phase_b order by id").Check(testkit.Rows("1 22 second", "2 22 second"))

	// A marker inside a complex write expression structurally fails closed, so
	// changing its source type must use a different strict cache variant.
	tk.MustExecWithContext(ctx, "prepare update_complex from 'update phase_b set v=?+1 where grp=?'")
	tk.MustExecWithContext(ctx, "set @v=30")
	tk.MustExecWithContext(ctx, "execute update_complex using @v,@grp")
	tk.MustExecWithContext(ctx, "set @v='40'")
	tk.MustExecWithContext(ctx, "execute update_complex using @v,@grp")
	tk.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("0"))

	// Literal/DEFAULT assignments still participate in duplicate-target
	// detection. The duplicate makes the whole statement strict, so rotating
	// the direct marker's source type must miss rather than reuse a target-type
	// cache variant.
	tk.MustExecWithContext(ctx, "prepare update_duplicate from 'update phase_b set v=1, v=? where grp=?'")
	tk.MustExecWithContext(ctx, "set @v=50")
	tk.MustExecWithContext(ctx, "execute update_duplicate using @v,@grp")
	tk.MustExecWithContext(ctx, "set @v='60'")
	tk.MustExecWithContext(ctx, "execute update_duplicate using @v,@grp")
	tk.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("0"))

	// Base-table index modifiers are outside the direct-write structural
	// allowlist and therefore keep every marker strict.
	tk.MustExecWithContext(ctx, "prepare update_index_hint from 'update phase_b use index(primary) set v=? where grp=?'")
	tk.MustExecWithContext(ctx, "set @v=70")
	tk.MustExecWithContext(ctx, "execute update_index_hint using @v,@grp")
	tk.MustExecWithContext(ctx, "set @v='80'")
	tk.MustExecWithContext(ctx, "execute update_index_hint using @v,@grp")
	tk.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("0"))

	// Multi-row INSERT and ON DUPLICATE share global marker order. Every safe
	// direct-write parameter, including varchar targets, ignores its source type
	// after target conversion. Rotate both numeric and varchar sources while
	// reusing the same cache variant.
	insertSQL := "insert into phase_b(id,grp,v,s) values (?,?,?,?),(?,?,?,?) on duplicate key update v=?"
	tk.MustExecWithContext(ctx, "prepare insert_values from '"+insertSQL+"'")
	tk.MustExecWithContext(ctx, "set @id1=3,@g1=20,@v1=31,@s1='x',@id2=4,@g2=20,@v2=41,@s2='y',@dup=51")
	tk.MustExecWithContext(ctx, "execute insert_values using @id1,@g1,@v1,@s1,@id2,@g2,@v2,@s2,@dup")
	tk.MustExecWithContext(ctx, "set @id1='3',@g1='21',@v1='32',@s1=111,@id2='5',@g2='21',@v2='42',@s2=222,@dup='52'")
	tk.MustExecWithContext(ctx, "execute insert_values using @id1,@g1,@v1,@s1,@id2,@g2,@v2,@s2,@dup")
	tk.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQueryWithContext(ctx, "select id,grp,v,s from phase_b where id>=3 order by id").Check(testkit.Rows(
		"3 20 52 x", "4 20 41 y", "5 21 42 222"))

	// A second session owns another prepared statement/snapshot but hits the
	// same Instance entry with its current raw values.
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExecWithContext(ctx, "use test")
	tk2.MustExecWithContext(ctx, "set tidb_enable_plan_cache_dml_param_type_match=ON")
	tk2.MustExecWithContext(ctx, "prepare insert_values from '"+insertSQL+"'")
	tk2.MustExecWithContext(ctx, "set @id1='3',@g1='22',@v1='33',@s1='m',@id2='6',@g2='22',@v2='43',@s2='n',@dup='53'")
	tk2.MustExecWithContext(ctx, "execute insert_values using @id1,@g1,@v1,@s1,@id2,@g2,@v2,@s2,@dup")
	tk2.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk2.MustQueryWithContext(ctx, "select id,grp,v,s from phase_b where id in (3,6) order by id").Check(testkit.Rows(
		"3 20 53 x", "6 22 43 n"))
}

// TestFix69990ParamMatchMaskRefreshAfterSchemaChange verifies that schema
// re-preprocess refreshes the statement-scoped matching modes. Recreating the
// target as a partitioned table changes the classifier result from direct-write
// matching to fail-closed strict matching.
func TestFix69990ParamMatchMaskRefreshAfterSchemaChange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_prepared_plan_cache=1")
	tk.MustExec("set tidb_enable_plan_cache_dml_param_type_match=ON")
	tk.MustExec("set tidb_partition_prune_mode='dynamic'")
	tk.MustExec("create table mask_refresh(id int primary key, v bigint)")
	tk.MustExec("insert into mask_refresh values(1,0)")
	tk.MustExec("prepare mask_refresh_stmt from 'update mask_refresh set v=? where id=?'")

	// The ordinary base table classifies the assignment marker as direct-write.
	tk.MustExec("set @v=1,@id=1")
	tk.MustExec("execute mask_refresh_stmt using @v,@id")
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustExec("set @v='2'")
	tk.MustExec("execute mask_refresh_stmt using @v,@id")
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)

	// DROP/CREATE changes the table binding and classifier eligibility. The first
	// execution misses because the old schema entry is fenced; after refresh the
	// assignment is strict, so rotating its source type must miss again.
	tk.MustExec("drop table mask_refresh")
	tk.MustExec("create table mask_refresh(id int primary key, v bigint) partition by hash(id) partitions 2")
	tk.MustExec("insert into mask_refresh values(1,0)")
	tk.MustExec("set @v=3")
	tk.MustExec("execute mask_refresh_stmt using @v,@id")
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustExec("set @v='4'")
	tk.MustExec("execute mask_refresh_stmt using @v,@id")
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery("select v from mask_refresh where id=1").Check(testkit.Rows("4"))
}

func TestFix69990PhaseBCacheOffOnOracle(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_plan_cache_dml_param_type_match=ON")
	tk.MustExec("create table phase_b_oracle(id int primary key, grp int, v bigint, d decimal(10,2))")

	type outcome struct {
		updateAffected uint64
		insertAffected uint64
		warnings       [][]any
		rows           [][]any
	}
	run := func(cacheEnabled bool) outcome {
		tk.MustExec("truncate table phase_b_oracle")
		tk.MustExec("insert into phase_b_oracle values(1,10,0,0),(2,10,0,0)")
		tk.MustExec("set tidb_enable_prepared_plan_cache=1")
		tk.MustExec("prepare phase_b_update_oracle from 'update phase_b_oracle set v=?, d=? where grp=?'")
		tk.MustExec("prepare phase_b_insert_oracle from 'insert into phase_b_oracle(id,grp,v,d) values(?,?,?,?),(?,?,?,?) on duplicate key update v=?'")
		if !cacheEnabled {
			tk.MustExec("set tidb_enable_prepared_plan_cache=0")
		}
		if cacheEnabled {
			tk.MustExec("set @v=1,@d=1.00,@grp=10")
			tk.MustExec("execute phase_b_update_oracle using @v,@d,@grp")
			tk.MustExec("set @id1=3,@g1=20,@v1=3,@d1=3.00,@id2=4,@g2=20,@v2=4,@d2=4.00,@dup=5")
			tk.MustExec("execute phase_b_insert_oracle using @id1,@g1,@v1,@d1,@id2,@g2,@v2,@d2,@dup")
			tk.MustExec("truncate table phase_b_oracle")
			tk.MustExec("insert into phase_b_oracle values(1,10,0,0),(2,10,0,0)")
		}
		tk.MustExec("set @v='22',@d='2.239',@grp=10")
		tk.MustExec("execute phase_b_update_oracle using @v,@d,@grp")
		result := outcome{updateAffected: tk.Session().GetSessionVars().StmtCtx.AffectedRows()}
		tk.MustExec("set @id1='3',@g1='20',@v1='31',@d1='3.239',@id2='4',@g2='20',@v2='41',@d2='4.239',@dup='51'")
		tk.MustExec("execute phase_b_insert_oracle using @id1,@g1,@v1,@d1,@id2,@g2,@v2,@d2,@dup")
		result.insertAffected = tk.Session().GetSessionVars().StmtCtx.AffectedRows()
		warnings := tk.MustQuery("show warnings").Rows()
		for _, warning := range warnings {
			if len(warning) == 3 && warning[1] == "1105" && warning[2] == "skip prepared plan-cache: plan cache is disabled" {
				continue
			}
			result.warnings = append(result.warnings, warning)
		}
		result.rows = tk.MustQuery("select id,grp,v,d from phase_b_oracle order by id").Rows()
		tk.MustExec("deallocate prepare phase_b_update_oracle")
		tk.MustExec("deallocate prepare phase_b_insert_oracle")
		return result
	}

	replan := run(false)
	cached := run(true)
	require.Equal(t, replan, cached)
}

func TestFix69990PreservesPrepareTimeUncacheableLifecycle(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_plan_cache_dml_param_type_match=ON")
	tk.MustExec("create table phase_b_uncacheable(id int primary key, v int, s varchar(20))")
	requireWarningContains := func(fragment string) {
		warnings := tk.MustQuery("show warnings").Rows()
		require.Condition(t, func() bool {
			for _, warning := range warnings {
				if len(warning) == 3 && strings.Contains(fmt.Sprint(warning[2]), fragment) {
					return true
				}
			}
			return false
		}, "warnings %v do not contain %q", warnings, fragment)
	}

	// The existing PREPARE-time cache switch remains part of StmtCacheable.
	// Enabling the cache later must neither retroactively enable target matching
	// nor publish an entry.
	tk.MustExec("set tidb_enable_prepared_plan_cache=0")
	tk.MustExec("prepare phase_b_disabled from 'insert into phase_b_uncacheable(id,v) values (?,?)'")
	prepared, err := tk.Session().GetSessionVars().GetPreparedStmtByName("phase_b_disabled")
	require.NoError(t, err)
	require.False(t, prepared.(*core.PlanCacheStmt).StmtCacheable)
	require.Equal(t, "plan cache is disabled", prepared.(*core.PlanCacheStmt).UncacheableReason)
	tk.MustExec("set tidb_enable_prepared_plan_cache=1")
	tk.MustExec("set @id=1,@v='11'")
	tk.MustExec("execute phase_b_disabled using @id,@v")
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	requireWarningContains("plan cache is disabled")
	tk.MustExec("set @id=2,@v='12'")
	tk.MustExec("execute phase_b_disabled using @id,@v")
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery("select * from phase_b_uncacheable order by id").Check(testkit.Rows("1 11 <nil>", "2 12 <nil>"))

	// Stable existing checker rejection also remains legacy with Fix ON for
	// both UPDATE and INSERT; repeated executions cannot become cache hits.
	for _, tc := range []struct {
		name string
		sql  string
		set  string
		use  string
	}{
		{name: "update", sql: "update phase_b_uncacheable set v=? where @phase_b_guard is null", set: "set @p='21',@phase_b_guard=null", use: "@p"},
		{name: "insert", sql: "insert into phase_b_uncacheable(id,v,s) values (?,?,@phase_b_guard)", set: "set @id=3,@p='22',@phase_b_guard='legacy'", use: "@id,@p"},
	} {
		tk.MustExec("prepare phase_b_uncacheable_stmt from '" + tc.sql + "'")
		prepared, err = tk.Session().GetSessionVars().GetPreparedStmtByName("phase_b_uncacheable_stmt")
		require.NoError(t, err)
		stmt := prepared.(*core.PlanCacheStmt)
		require.False(t, stmt.StmtCacheable, tc.name)
		require.Contains(t, stmt.UncacheableReason, "user-defined variables", tc.name)
		tk.MustExec(tc.set)
		tk.MustExec("execute phase_b_uncacheable_stmt using " + tc.use)
		require.False(t, tk.Session().GetSessionVars().FoundInPlanCache, tc.name)
		requireWarningContains("user-defined variables")
		if tc.name == "insert" {
			tk.MustExec("delete from phase_b_uncacheable where id=3")
		}
		tk.MustExec("execute phase_b_uncacheable_stmt using " + tc.use)
		require.False(t, tk.Session().GetSessionVars().FoundInPlanCache, tc.name)
		tk.MustExec("deallocate prepare phase_b_uncacheable_stmt")
	}
}

func TestFix69990JSONBitCrossSourceHitAndRecovery(t *testing.T) {
	for _, instance := range []bool{false, true} {
		cacheName := map[bool]string{false: "session", true: "instance"}[instance]
		for _, tc := range []struct {
			name            string
			column          string
			allowedSet      string
			blockedSet      string
			blockedMayError bool
		}{
			{name: "json", column: "j", allowedSet: `set @v='{"a":1}'`, blockedSet: "set @v=1", blockedMayError: true},
			{name: "bit", column: "b", allowedSet: "set @v=null", blockedSet: "set @v=1"},
		} {
			t.Run(cacheName+"/"+tc.name, func(t *testing.T) {
				store, dom := testkit.CreateMockStoreAndDomain(t)
				ctx := context.Background()
				if instance {
					ctx = context.WithValue(ctx, core.PlanCacheKeyEnableInstancePlanCache{}, true)
				}
				tk := testkit.NewTestKit(t, store)
				tk.MustExecWithContext(ctx, "use test")
				tk.MustExecWithContext(ctx, "set tidb_enable_plan_cache_dml_param_type_match=ON")
				tk.MustExecWithContext(ctx, "set tidb_enable_prepared_plan_cache=1")
				tk.MustExecWithContext(ctx, "create table phase_c_source(id int primary key, j json, b bit(8))")
				tk.MustExecWithContext(ctx, "insert into phase_c_source values(1,null,null)")
				tk.MustExecWithContext(ctx, "prepare phase_c_source_stmt from 'update phase_c_source set "+tc.column+"=? where id=1'")
				cacheSize := func() int64 {
					if instance {
						return dom.GetInstancePlanCache().Size()
					}
					return int64(tk.Session().GetSessionPlanCache().Size())
				}
				execute := func() error {
					_, err := tk.ExecWithContext(ctx, "execute phase_c_source_stmt using @v")
					return err
				}

				tk.MustExecWithContext(ctx, tc.allowedSet)
				require.NoError(t, execute())
				require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
				afterPublish := cacheSize()
				require.Greater(t, afterPublish, int64(0))
				require.NoError(t, execute())
				require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
				if tc.name == "json" {
					tk.MustExecWithContext(ctx, "set @v='not-json'")
					require.Error(t, execute())
					require.True(t, tk.Session().GetSessionVars().FoundInPlanCache,
						"an allowed nonbinary string source must reach cached executor conversion")
					require.Equal(t, afterPublish, cacheSize())
					tk.MustExecWithContext(ctx, tc.allowedSet)
					require.NoError(t, execute())
					require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
				}

				tk.MustExecWithContext(ctx, tc.blockedSet)
				err := execute()
				if !tc.blockedMayError {
					require.NoError(t, err)
				}
				require.True(t, tk.Session().GetSessionVars().FoundInPlanCache,
					"cross-source direct write must reuse the target-typed plan")
				require.Equal(t, afterPublish, cacheSize(), "cross-source hit must not create a cache variant")
				for _, warning := range tk.MustQueryWithContext(ctx, "show warnings").Rows() {
					require.NotContains(t, fmt.Sprint(warning), "skip prepared plan-cache")
				}

				// Cross-source execution does not delete or mutate the normalized entry.
				tk.MustExecWithContext(ctx, tc.allowedSet)
				require.NoError(t, execute())
				require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
				require.Equal(t, afterPublish, cacheSize())
			})
		}
	}
}

func TestFix69990ConcurrentInstanceCrossSourceIsolation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	ctx := context.WithValue(context.Background(), core.PlanCacheKeyEnableInstancePlanCache{}, true)
	newSession := func() *testkit.TestKit {
		tk := testkit.NewTestKit(t, store)
		tk.MustExecWithContext(ctx, "use test")
		tk.MustExecWithContext(ctx, "set tidb_enable_plan_cache_dml_param_type_match=ON")
		tk.MustExecWithContext(ctx, "set tidb_enable_prepared_plan_cache=1")
		return tk
	}
	allowed, crossSource := newSession(), newSession()
	allowed.MustExecWithContext(ctx, "create table phase_c_concurrent(id int primary key, j json)")
	allowed.MustExecWithContext(ctx, "insert into phase_c_concurrent values(1,null),(2,null)")
	for _, tk := range []*testkit.TestKit{allowed, crossSource} {
		tk.MustExecWithContext(ctx, "prepare phase_c_concurrent_stmt from 'update phase_c_concurrent set j=? where id=?'")
	}

	allowed.MustExecWithContext(ctx, `set @j='{"warm":1}',@id=1`)
	allowed.MustExecWithContext(ctx, "execute phase_c_concurrent_stmt using @j,@id")
	require.False(t, allowed.Session().GetSessionVars().FoundInPlanCache)
	cacheSize := dom.GetInstancePlanCache().Size()
	require.Greater(t, cacheSize, int64(0))
	crossSource.MustExecWithContext(ctx, `set @j='{"warm":2}',@id=2`)
	crossSource.MustExecWithContext(ctx, "execute phase_c_concurrent_stmt using @j,@id")
	require.True(t, crossSource.Session().GetSessionVars().FoundInPlanCache,
		"the second session must prove both sessions share the warmed Instance value")

	type result struct {
		allowed bool
		err     error
		hit     bool
	}
	for iteration := 0; iteration < 8; iteration++ {
		allowed.MustExecWithContext(ctx, "set @j=?,@id=1", fmt.Sprintf(`{"allowed":%d}`, iteration))
		crossSource.MustExecWithContext(ctx, "set @j=?,@id=2", iteration)
		start := make(chan struct{})
		results := make(chan result, 2)
		for _, execution := range []struct {
			tk      *testkit.TestKit
			allowed bool
		}{{allowed, true}, {crossSource, false}} {
			execution := execution
			go func() {
				<-start
				_, err := execution.tk.ExecWithContext(ctx, "execute phase_c_concurrent_stmt using @j,@id")
				results <- result{allowed: execution.allowed, err: err, hit: execution.tk.Session().GetSessionVars().FoundInPlanCache}
			}()
		}
		close(start)
		for range 2 {
			out := <-results
			if out.allowed {
				require.NoError(t, out.err)
				require.True(t, out.hit)
			} else {
				require.NoError(t, out.err)
				require.True(t, out.hit)
			}
		}
		require.Equal(t, cacheSize, dom.GetInstancePlanCache().Size(), "cross-source hits must not publish another entry")
	}
}

func TestFix69990NonFKInstanceAssignmentGraphIsolation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	ctx := context.WithValue(context.Background(), core.PlanCacheKeyEnableInstancePlanCache{}, true)
	newSession := func() *testkit.TestKit {
		tk := testkit.NewTestKit(t, store)
		tk.MustExecWithContext(ctx, "use test")
		tk.MustExecWithContext(ctx, "set tidb_enable_plan_cache_dml_param_type_match=ON")
		tk.MustExecWithContext(ctx, "set tidb_enable_prepared_plan_cache=1")
		return tk
	}
	tk1, tk2 := newSession(), newSession()
	tk1.MustExecWithContext(ctx, "create table phase_c_non_fk(id int primary key,uk int unique,a int,b decimal(10,2),c varchar(20))")
	tk1.MustExecWithContext(ctx, "insert into phase_c_non_fk values(1,11,0,0,'a'),(2,22,0,0,'b')")
	statements := []struct {
		name string
		sql  string
	}{
		{"phase_c_non_fk_point", "update phase_c_non_fk set a=?,b=? where id=?"},
		{"phase_c_non_fk_unique", "update phase_c_non_fk set a=? where uk=?"},
		{"phase_c_non_fk_batch", "update phase_c_non_fk set a=? where id in (?,?)"},
		{"phase_c_non_fk_values", "insert into phase_c_non_fk values(?,?,?,?,?),(?,?,?,?,?)"},
		{"phase_c_non_fk_set", "insert into phase_c_non_fk set id=?,uk=?,a=?,b=?,c=?"},
		{"phase_c_non_fk_dup", "insert into phase_c_non_fk(id,uk,a,b,c) values(?,?,?,?,?) on duplicate key update a=?,b=?"},
	}
	for _, tk := range []*testkit.TestKit{tk1, tk2} {
		for _, statement := range statements {
			tk.MustExecWithContext(ctx, "prepare "+statement.name+" from '"+statement.sql+"'")
		}
	}
	type execution struct {
		name       string
		publishSet string
		hitSet     string
		execute    string
	}
	executions := []execution{
		{"phase_c_non_fk_point", "set @a=1,@b=1.25,@id=1", "set @a='2',@b='2.50',@id=1", "execute phase_c_non_fk_point using @a,@b,@id"},
		{"phase_c_non_fk_unique", "set @a=3,@uk=22", "set @a='4',@uk=22", "execute phase_c_non_fk_unique using @a,@uk"},
		{"phase_c_non_fk_batch", "set @a=5,@id1=1,@id2=2", "set @a='6',@id1=1,@id2=2", "execute phase_c_non_fk_batch using @a,@id1,@id2"},
		{"phase_c_non_fk_values", "set @id1=3,@uk1=33,@a1=7,@b1=7.25,@c1='c',@id2=4,@uk2=44,@a2=8,@b2=8.25,@c2='d'", "set @id1=5,@uk1=55,@a1='9',@b1='9.25',@c1='e',@id2=6,@uk2=66,@a2='10',@b2='10.25',@c2='f'", "execute phase_c_non_fk_values using @id1,@uk1,@a1,@b1,@c1,@id2,@uk2,@a2,@b2,@c2"},
		{"phase_c_non_fk_set", "set @id=7,@uk=77,@a=11,@b=11.25,@c='g'", "set @id=8,@uk=88,@a='12',@b='12.25',@c='h'", "execute phase_c_non_fk_set using @id,@uk,@a,@b,@c"},
		{"phase_c_non_fk_dup", "set @id=1,@uk=11,@a=13,@b=13.25,@c='x',@da=14,@db=14.25", "set @id=1,@uk=11,@a='15',@b='15.25',@c='y',@da='16',@db='16.25'", "execute phase_c_non_fk_dup using @id,@uk,@a,@b,@c,@da,@db"},
	}
	fingerprints := make(map[string]string, len(executions))
	cachedPlans := make(map[string]base.Plan, len(executions))
	for _, execution := range executions {
		tk1.MustExecWithContext(ctx, execution.publishSet)
		tk1.MustExecWithContext(ctx, execution.execute)
		require.False(t, tk1.Session().GetSessionVars().FoundInPlanCache)
		cached := instanceCachedPlanForPrepared(t, tk1, execution.name)
		if strings.HasPrefix(execution.name, "phase_c_non_fk_point") || execution.name == "phase_c_non_fk_unique" || execution.name == "phase_c_non_fk_batch" {
			assertNonFKCachedUpdateAccessPath(t, execution.name, cached)
		}
		cachedPlans[execution.name] = cached
		fingerprints[execution.name] = dmlExpressionGraphFingerprint(t, cached)

		tk2.MustExecWithContext(ctx, execution.hitSet)
		tk2.MustExecWithContext(ctx, execution.execute)
		require.True(t, tk2.Session().GetSessionVars().FoundInPlanCache)
		require.Equal(t, fingerprints[execution.name], dmlExpressionGraphFingerprint(t, cached))
	}
	for iteration := 0; iteration < 4; iteration++ {
		tk1.MustExecWithContext(ctx, "set @a=?,@b=?,@id=1", 20+iteration, fmt.Sprintf("%d.25", 20+iteration))
		tk2.MustExecWithContext(ctx, "set @a=?,@b=?,@id=2", fmt.Sprint(30+iteration), fmt.Sprintf("%d.25", 30+iteration))
		start := make(chan struct{})
		errs := make(chan error, 2)
		for _, tk := range []*testkit.TestKit{tk1, tk2} {
			tk := tk
			go func() {
				<-start
				_, err := tk.ExecWithContext(ctx, "execute phase_c_non_fk_point using @a,@b,@id")
				errs <- err
			}()
		}
		close(start)
		require.NoError(t, <-errs)
		require.NoError(t, <-errs)
		require.True(t, tk1.Session().GetSessionVars().FoundInPlanCache)
		require.True(t, tk2.Session().GetSessionVars().FoundInPlanCache)
		require.Equal(t, fingerprints["phase_c_non_fk_point"], dmlExpressionGraphFingerprint(t, cachedPlans["phase_c_non_fk_point"]))
	}

	// Session cache executes its cached plan directly rather than cloning it.
	// Repeat every assignment/list shape and prove real hits leave the cached
	// graph byte-for-byte identical as source types and values rotate.
	sessionCtx := context.Background()
	tkSession := testkit.NewTestKit(t, store)
	tkSession.MustExecWithContext(sessionCtx, "use test")
	tkSession.MustExecWithContext(sessionCtx, "set tidb_enable_plan_cache_dml_param_type_match=ON")
	tkSession.MustExecWithContext(sessionCtx, "set tidb_enable_prepared_plan_cache=1")
	tkSession.MustExecWithContext(sessionCtx, "create table phase_c_non_fk_session like phase_c_non_fk")
	tkSession.MustExecWithContext(sessionCtx, "insert into phase_c_non_fk_session(id,uk,a,b,c) values(1,11,0,0,'a'),(2,22,0,0,'b')")
	for _, statement := range statements {
		sql := strings.Replace(statement.sql, "phase_c_non_fk", "phase_c_non_fk_session", 1)
		tkSession.MustExecWithContext(sessionCtx, "prepare "+statement.name+" from '"+sql+"'")
	}
	for _, execution := range executions {
		tkSession.MustExecWithContext(sessionCtx, execution.publishSet)
		tkSession.MustExecWithContext(sessionCtx, execution.execute)
		require.False(t, tkSession.Session().GetSessionVars().FoundInPlanCache)
		cached := sessionCachedPlanForPrepared(t, tkSession, execution.name)
		fingerprint := dmlExpressionGraphFingerprint(t, cached)
		tkSession.MustExecWithContext(sessionCtx, execution.hitSet)
		tkSession.MustExecWithContext(sessionCtx, execution.execute)
		require.True(t, tkSession.Session().GetSessionVars().FoundInPlanCache)
		require.Equal(t, fingerprint, dmlExpressionGraphFingerprint(t, cached))
	}

	// Fix identity changes must not consume or mutate the Fix-ON entry.  Turning
	// the fix back on recovers the same immutable cached assignment graph.
	tk2.MustExecWithContext(ctx, "set tidb_enable_plan_cache_dml_param_type_match=OFF")
	tk2.MustExecWithContext(ctx, "set @a=17,@b=17.25,@id=1")
	tk2.MustExecWithContext(ctx, "execute phase_c_non_fk_point using @a,@b,@id")
	require.False(t, tk2.Session().GetSessionVars().FoundInPlanCache)
	require.Equal(t, fingerprints["phase_c_non_fk_point"], dmlExpressionGraphFingerprint(t, cachedPlans["phase_c_non_fk_point"]))
	tk2.MustExecWithContext(ctx, "set tidb_enable_plan_cache_dml_param_type_match=ON")
	tk2.MustExecWithContext(ctx, "execute phase_c_non_fk_point using @a,@b,@id")
	require.False(t, tk2.Session().GetSessionVars().FoundInPlanCache)
	tk2.MustExecWithContext(ctx, "set @a=171")
	tk2.MustExecWithContext(ctx, "execute phase_c_non_fk_point using @a,@b,@id")
	require.True(t, tk2.Session().GetSessionVars().FoundInPlanCache)

	// A DDL revision fences the old plan.  The old cached graph remains
	// immutable while the rebuilt revision misses once and then hits.
	tk1.MustExecWithContext(ctx, "alter table phase_c_non_fk add column note int")
	tk2.MustExecWithContext(ctx, "set @a=18,@b=18.25,@id=1")
	tk2.MustExecWithContext(ctx, "execute phase_c_non_fk_point using @a,@b,@id")
	require.False(t, tk2.Session().GetSessionVars().FoundInPlanCache)
	require.Equal(t, fingerprints["phase_c_non_fk_point"], dmlExpressionGraphFingerprint(t, cachedPlans["phase_c_non_fk_point"]))
	tk2.MustExecWithContext(ctx, "set @a=19")
	tk2.MustExecWithContext(ctx, "execute phase_c_non_fk_point using @a,@b,@id")
	require.True(t, tk2.Session().GetSessionVars().FoundInPlanCache)
	require.Greater(t, dom.GetInstancePlanCache().MemUsage(), int64(0))
}

func TestFix69990PointUpdateUnifiedAssignmentConversion(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_plan_cache_dml_param_type_match = ON")
	tk.MustExec("create table unified_conversion (id int primary key, u int unsigned, d decimal(10,2), e enum('a','b'), s set('a','b'), v varchar(8))")
	tk.MustExec("insert into unified_conversion values (1, 0, 0, 'a', 'a', ''), (2, 0, 0, 'a', 'a', '')")

	type outcome struct {
		err      string
		warnings [][]any
		row      [][]any
	}
	run := func(sqlMode, assignment, predicate, id string) outcome {
		tk.MustExec("set @@sql_mode = ?", sqlMode)
		tk.MustExec("update unified_conversion set u=0, d=0, e='a', s='a', v='' where id in (1, 2)")
		err := tk.ExecToErr("update unified_conversion set " + assignment + " where " + predicate)
		result := outcome{
			warnings: tk.MustQuery("show warnings").Rows(),
			row:      tk.MustQuery("select u, d, e+0, s+0, v from unified_conversion where id=" + id).Rows(),
		}
		if err != nil {
			result.err = err.Error()
		}
		return result
	}

	testCases := []struct {
		name       string
		sqlMode    string
		assignment string
	}{
		{name: "unsigned negative", sqlMode: "", assignment: "u=-1"},
		{name: "decimal expression", sqlMode: "", assignment: "d=1.239+0"},
		{name: "explicit cast", sqlMode: "", assignment: "d=cast('2.345' as decimal(10,3))"},
		{name: "enum integer", sqlMode: "STRICT_TRANS_TABLES", assignment: "e=2"},
		{name: "set integer", sqlMode: "STRICT_TRANS_TABLES", assignment: "s=3"},
		{name: "string conversion", sqlMode: "STRICT_TRANS_TABLES", assignment: "v=123"},
		{name: "null", sqlMode: "", assignment: "d=null"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			point := run(tc.sqlMode, tc.assignment, "id=1", "1")
			logical := run(tc.sqlMode, tc.assignment, "id>1", "2")
			require.Equal(t, logical, point)
		})
	}
}
