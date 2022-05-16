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

package funcdep_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/hint"
	"github.com/stretchr/testify/require"
)

func testGetIS(t *testing.T, ctx sessionctx.Context) infoschema.InfoSchema {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	require.NoError(t, err)
	return dom.InfoSchema()
}

func TestFDSet_ExtractFD(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	par := parser.New()
	par.SetParserConfig(parser.ParserConfig{EnableWindowFunction: true, EnableStrictDoubleTypeCheck: true})

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_new_only_full_group_by_check = 'on';")
	tk.MustExec("create table t1(a int key, b int, c int, unique(b,c))")
	tk.MustExec("create table t2(m int key, n int, p int, unique(m,n))")
	tk.MustExec("create table x1(a int not null primary key, b int not null, c int default null, d int not null, unique key I_b_c (b,c), unique key I_b_d (b,d))")
	tk.MustExec("create table x2(a int not null primary key, b int not null, c int default null, d int not null, unique key I_b_c (b,c), unique key I_b_d (b,d))")

	tests := []struct {
		sql  string
		best string
		fd   string
	}{
		{
			sql:  "select a from t1",
			best: "DataScan(t1)->Projection",
			fd:   "{(1)-->(2,3), (2,3)~~>(1)} >>> {}",
		},
		{
			sql:  "select a,b from t1",
			best: "DataScan(t1)->Projection",
			fd:   "{(1)-->(2,3), (2,3)~~>(1)} >>> {(1)-->(2)}",
		},
		{
			sql:  "select a,c,b+1 from t1",
			best: "DataScan(t1)->Projection",
			// 4 is the extended column from (b+1) determined by b, also determined by a.
			// since b is also projected in the b+1, so 2 is kept.
			fd: "{(1)-->(2,3), (2,3)~~>(1)} >>> {(1)-->(2,3), (2,3)~~>(1), (2)-->(4)}",
		},
		{
			sql:  "select a,b+1,c+b from t1",
			best: "DataScan(t1)->Projection",
			// 4, 5 is the extended column from (b+1),(c+b) determined by b,c, also determined by a.
			fd: "{(1)-->(2,3), (2,3)~~>(1)} >>> {(1)-->(2,3), (2,3)~~>(1), (2)-->(4), (2,3)-->(5)}",
		},
		{
			sql:  "select a,a+b,1 from t1",
			best: "DataScan(t1)->Projection",
			// 4 is the extended column from (b+1) determined by b, also determined by a.
			fd: "{(1)-->(2,3), (2,3)~~>(1)} >>> {(1)-->(2,4), ()-->(5)}",
		},
		{
			sql: "select b+1, sum(a) from t1 group by(b)",
			// The final ones are b -> (b+1), b -> sum(a)
			best: "DataScan(t1)->Aggr(sum(test.t1.a),firstrow(test.t1.b))->Projection",
			fd:   "{(1)-->(2,3), (2,3)~~>(1)} >>> {(1)-->(2,3), (2,3)~~>(1), (2)-->(4)} >>> {(2)-->(4,5)}",
		},
		{
			sql: "select b+1, b, sum(a) from t1 group by(b)",
			// The final ones are b -> (b+1), b -> sum(a)
			best: "DataScan(t1)->Aggr(sum(test.t1.a),firstrow(test.t1.b))->Projection",
			fd:   "{(1)-->(2,3), (2,3)~~>(1)} >>> {(1)-->(2,3), (2,3)~~>(1), (2)-->(4)} >>> {(2)-->(4,5)}",
		},
		// test for table x1 and x2
		{
			sql:  "select a from x1 group by a,b,c",
			best: "DataScan(x1)->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(1)-->(2,3), (2,3)~~>(1)}",
		},
		{
			sql:  "select b from x1 group by b",
			best: "DataScan(x1)->Aggr(firstrow(test.x1.b))->Projection",
			// b --> b is natural existed, so it won't exist in fd.
			fd: "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {}",
		},
		{
			sql:  "select b as e from x1 group by b",
			best: "DataScan(x1)->Aggr(firstrow(test.x1.b))->Projection",
			// b --> b is naturally existed, so it won't exist in fd.
			fd: "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {}",
		},
		{
			sql:  "select b+c from x1 group by b+c",
			best: "DataScan(x1)->Aggr(firstrow(test.x1.b),firstrow(test.x1.c))->Projection",
			// avoid allocating unique ID 7 from fd temporarily, and substituted by unique ID 5
			// attention:
			// b+c is an expr assigned with new plan ID when building upper-layer projection.
			// when extracting FD after build phase is done, we should be able to recognize a+b in lower-layer group by item with the same unique ID.
			// that's why we introduce session variable MapHashCode2UniqueID4ExtendedCol in.
			fd: "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3), (2,3)-->(5)} >>> {(2,3)-->(5)}",
		},
		{
			sql:  "select b+c, min(a) from x1 group by b+c, b-c",
			best: "DataScan(x1)->Aggr(min(test.x1.a),firstrow(test.x1.b),firstrow(test.x1.c))->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3), (2,3)-->(6,7), (6,7)-->(5)} >>> {(2,3)-->(6,7), (6,7)-->(5)}",
		},
		{
			sql:  "select b+c, min(a) from x1 group by b, c",
			best: "DataScan(x1)->Aggr(min(test.x1.a),firstrow(test.x1.b),firstrow(test.x1.c))->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3), (2,3)-->(5)} >>> {(2,3)-->(5,6)}",
		},
		{
			sql:  "select b+c from x1 group by b,c",
			best: "DataScan(x1)->Aggr(firstrow(test.x1.b),firstrow(test.x1.c))->Projection",
			// b --> b is naturally existed, so it won't exist in fd.
			fd: "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(2,3)-->(5)}",
		},
		{
			sql:  "select case b when 1 then c when 2 then d else d end from x1 group by b,c,d",
			best: "DataScan(x1)->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(2,3)~~>(4), (2,4)-->(3,5)}",
		},
		{
			// scalar sub query will be substituted with constant datum.
			sql:  "select c > (select b from x1) from x1 group by c",
			best: "DataScan(x1)->Aggr(firstrow(test.x1.c))->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(3)-->(15)}",
		},
		{
			sql:  "select exists (select * from x1) from x1 group by d",
			best: "DataScan(x1)->Aggr(firstrow(1))->Projection",
			// 14 is added in the logicAgg pruning process cause all the columns of agg has been pruned.
			fd: "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {()-->(13)}",
		},
		{
			sql:  "select c is null from x1 group by c",
			best: "DataScan(x1)->Aggr(firstrow(test.x1.c))->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(3)-->(5)}",
		},
		{
			sql:  "select c is true from x1 group by c",
			best: "DataScan(x1)->Aggr(firstrow(test.x1.c))->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(3)-->(5)}",
		},
		{
			sql: "select (c+b)*d from x1 group by c,b,d",
			// agg elimination.
			best: "DataScan(x1)->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(2,3)~~>(4), (2,4)-->(3,5)}",
		},
		{
			sql:  "select b in (c,d) from x1 group by b,c,d",
			best: "DataScan(x1)->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(2,3)~~>(4), (2,4)-->(3,5)}",
		},
		{
			sql:  "select b like '%a' from x1 group by b",
			best: "DataScan(x1)->Aggr(firstrow(test.x1.b))->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(2)-->(5)}",
		},
		// test functional dependency on primary key
		{
			sql: "select * from x1 group by a",
			// agg eliminated by primary key.
			best: "DataScan(x1)->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)}",
		},
		// test functional dependency on unique key with not null
		{
			sql:  "select * from x1 group by b,d",
			best: "DataScan(x1)->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)}",
		},
		// test functional dependency derived from keys in where condition
		{
			sql:  "select * from x1 where c = d group by b, c",
			best: "DataScan(x1)->Aggr(firstrow(test.x1.a),firstrow(test.x1.b),firstrow(test.x1.c),firstrow(test.x1.d))->Projection",
			// c = d derives:
			// 1: c and d are not null, make lax FD (2,3)~~>(1,4) to be strict one.
			// 2: c and d are equivalent.
			fd: "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(1)-->(2-4), (2,3)-->(1,4), (2,4)-->(1,3), (3,4)==(3,4)} >>> {(1)-->(2-4), (2,3)-->(1,4), (2,4)-->(1,3), (3,4)==(3,4)}",
		},
	}

	ctx := context.TODO()
	is := testGetIS(t, tk.Session())
	for i, tt := range tests {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt.sql)
		stmt, err := par.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)
		tk.Session().GetSessionVars().PlanID = 0
		tk.Session().GetSessionVars().PlanColumnID = 0
		err = plannercore.Preprocess(tk.Session(), stmt, plannercore.WithPreprocessorReturn(&plannercore.PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err)
		tk.Session().PrepareTSFuture(ctx)
		builder, _ := plannercore.NewPlanBuilder().Init(tk.Session(), is, &hint.BlockHintProcessor{})
		// extract FD to every OP
		p, err := builder.Build(ctx, stmt)
		require.NoError(t, err)
		p, err = plannercore.LogicalOptimizeTest(ctx, builder.GetOptFlag(), p.(plannercore.LogicalPlan))
		require.NoError(t, err)
		require.Equal(t, tt.best, plannercore.ToString(p), comment)
		// extract FD to every OP
		p.(plannercore.LogicalPlan).ExtractFD()
		require.Equal(t, tt.fd, plannercore.FDToString(p.(plannercore.LogicalPlan)), comment)
	}
}

func TestFDSet_ExtractFDForApply(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	par := parser.New()
	par.SetParserConfig(parser.ParserConfig{EnableWindowFunction: true, EnableStrictDoubleTypeCheck: true})

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_new_only_full_group_by_check = 'on';")
	tk.MustExec("CREATE TABLE X (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)")
	tk.MustExec("CREATE UNIQUE INDEX uni ON X (b, c)")
	tk.MustExec("CREATE TABLE Y (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))")

	tests := []struct {
		sql  string
		best string
		fd   string
	}{
		{
			sql: "select * from X where exists (select * from Y where m=a limit 1)",
			// For this Apply, it's essentially a semi join, for every `a` in X, do the inner loop once.
			//   +- datasource(x)
			//   +- limit
			//     +- datasource(Y)
			best: "Apply{DataScan(X)->DataScan(Y)->Limit}->Projection",
			// Since semi join will keep the **all** rows of the outer table, it's FD can be derived.
			fd: "{(1)-->(2-5), (2,3)~~>(1,4,5)} >>> {(1)-->(2-5), (2,3)~~>(1,4,5)}",
		},
		{
			sql: "select a, b from X where exists (select * from Y where m=a limit 1)",
			// For this Apply, it's essentially a semi join, for every `a` in X, do the inner loop once.
			//   +- datasource(x)
			//   +- limit
			//     +- datasource(Y)
			best: "Apply{DataScan(X)->DataScan(Y)->Limit}->Projection", // semi join
			// Since semi join will keep the **part** rows of the outer table, it's FD can be derived.
			fd: "{(1)-->(2-5), (2,3)~~>(1,4,5)} >>> {(1)-->(2)}",
		},
		{
			// Limit will refuse to de-correlate apply to join while this won't.
			sql:  "select * from X where exists (select * from Y where m=a and p=1)",
			best: "Join{DataScan(X)->DataScan(Y)}(test.x.a,test.y.m)->Projection", // semi join
			fd:   "{(1)-->(2-5), (2,3)~~>(1,4,5)} >>> {(1)-->(2-5), (2,3)~~>(1,4,5)}",
		},
		{
			sql:  "select * from X where exists (select * from Y where m=a and p=q)",
			best: "Join{DataScan(X)->DataScan(Y)}(test.x.a,test.y.m)->Projection", // semi join
			fd:   "{(1)-->(2-5), (2,3)~~>(1,4,5)} >>> {(1)-->(2-5), (2,3)~~>(1,4,5)}",
		},
		{
			sql:  "select * from X where exists (select * from Y where m=a and b=1)",
			best: "Join{DataScan(X)->DataScan(Y)}(test.x.a,test.y.m)->Projection", // semi join
			// b=1 is semi join's left condition which should be conserved.
			fd: "{(1)-->(2-5), (2,3)~~>(1,4,5)} >>> {(1)-->(2-5), (2,3)~~>(1,4,5)}",
		},
		{
			sql:  "select * from (select b,c,d,e from X) X1 where exists (select * from Y where p=q and n=1) ",
			best: "Dual->Projection",
			fd:   "{}",
		},
		{
			sql:  "select * from (select b,c,d,e from X) X1 where exists (select * from Y where p=b and n=1) ",
			best: "Join{DataScan(X)->DataScan(Y)}(test.x.b,test.y.p)->Projection", // semi join
			fd:   "{(2,3)~~>(4,5)} >>> {(2,3)~~>(4,5)}",
		},
		{
			sql:  "select * from X where exists (select m, p, q from Y where n=a and p=1)",
			best: "Join{DataScan(X)->DataScan(Y)}(test.x.a,test.y.n)->Projection",
			// p=1 is semi join's right condition which should **NOT** be conserved.
			fd: "{(1)-->(2-5), (2,3)~~>(1,4,5)} >>> {(1)-->(2-5), (2,3)~~>(1,4,5)}",
		},
	}

	ctx := context.TODO()
	is := testGetIS(t, tk.Session())
	for i, tt := range tests {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt.sql)
		stmt, err := par.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)
		tk.Session().GetSessionVars().PlanID = 0
		tk.Session().GetSessionVars().PlanColumnID = 0
		err = plannercore.Preprocess(tk.Session(), stmt, plannercore.WithPreprocessorReturn(&plannercore.PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err, comment)
		tk.Session().PrepareTSFuture(ctx)
		builder, _ := plannercore.NewPlanBuilder().Init(tk.Session(), is, &hint.BlockHintProcessor{})
		// extract FD to every OP
		p, err := builder.Build(ctx, stmt)
		require.NoError(t, err, comment)
		p, err = plannercore.LogicalOptimizeTest(ctx, builder.GetOptFlag(), p.(plannercore.LogicalPlan))
		require.NoError(t, err, comment)
		require.Equal(t, tt.best, plannercore.ToString(p), comment)
		// extract FD to every OP
		p.(plannercore.LogicalPlan).ExtractFD()
		require.Equal(t, tt.fd, plannercore.FDToString(p.(plannercore.LogicalPlan)), comment)
	}
}

func TestFDSet_MakeOuterJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	par := parser.New()
	par.SetParserConfig(parser.ParserConfig{EnableWindowFunction: true, EnableStrictDoubleTypeCheck: true})

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_new_only_full_group_by_check = 'on';")
	tk.MustExec("CREATE TABLE X (a INT PRIMARY KEY, b INT, c INT, d INT, e INT)")
	tk.MustExec("CREATE UNIQUE INDEX uni ON X (b, c)")
	tk.MustExec("CREATE TABLE Y (m INT, n INT, p INT, q INT, PRIMARY KEY (m, n))")

	tests := []struct {
		sql  string
		best string
		fd   string
	}{
		{
			sql:  "select * from X left outer join (select *, p+q from Y) Y1 ON true",
			best: "Join{DataScan(X)->DataScan(Y)->Projection}->Projection",
			fd:   "{(1)-->(2-5), (2,3)~~>(1,4,5), (6,7)-->(8,9,11), (8,9)-->(11), (1,6,7)-->(2-5,8,9,11)} >>> {(1)-->(2-5), (2,3)~~>(1,4,5), (6,7)-->(8,9,11), (8,9)-->(11), (1,6,7)-->(2-5,8,9,11)}",
		},
	}

	ctx := context.TODO()
	is := testGetIS(t, tk.Session())
	for i, tt := range tests {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt.sql)
		stmt, err := par.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)
		tk.Session().GetSessionVars().PlanID = 0
		tk.Session().GetSessionVars().PlanColumnID = 0
		err = plannercore.Preprocess(tk.Session(), stmt, plannercore.WithPreprocessorReturn(&plannercore.PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err, comment)
		tk.Session().PrepareTSFuture(ctx)
		builder, _ := plannercore.NewPlanBuilder().Init(tk.Session(), is, &hint.BlockHintProcessor{})
		// extract FD to every OP
		p, err := builder.Build(ctx, stmt)
		require.NoError(t, err, comment)
		p, err = plannercore.LogicalOptimizeTest(ctx, builder.GetOptFlag(), p.(plannercore.LogicalPlan))
		require.NoError(t, err, comment)
		require.Equal(t, tt.best, plannercore.ToString(p), comment)
		// extract FD to every OP
		p.(plannercore.LogicalPlan).ExtractFD()
		require.Equal(t, tt.fd, plannercore.FDToString(p.(plannercore.LogicalPlan)), comment)
	}
}
