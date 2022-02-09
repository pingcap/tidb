package functional_dependency_test

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
	"github.com/stretchr/testify/assert"
)

func testGetIS(ass *assert.Assertions, ctx sessionctx.Context) infoschema.InfoSchema {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	ass.Nil(err)
	return dom.InfoSchema()
}

func TestFDSet_ExtractFD(t *testing.T) {
	t.Parallel()
	ass := assert.New(t)

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	par := parser.New()
	par.SetParserConfig(parser.ParserConfig{EnableWindowFunction: true, EnableStrictDoubleTypeCheck: true})

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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
			// since b is projected out, b --> b+1 and b ~~> sum(a) is eliminated.
			best: "DataScan(t1)->Aggr(sum(test.t1.a),firstrow(test.t1.b))->Projection",
			fd:   "{(1)-->(2,3), (2,3)~~>(1)} >>> {(2)~~>(4)} >>> {(2)~~>(4), (2)-->(5)}",
		},
		{
			sql:  "select b+1, b, sum(a) from t1 group by(b)",
			best: "DataScan(t1)->Aggr(sum(test.t1.a),firstrow(test.t1.b))->Projection",
			fd:   "{(1)-->(2,3), (2,3)~~>(1)} >>> {(2)~~>(4)} >>> {(2)~~>(4), (2)-->(5)}",
		},
		// test for table x1 and x2
		{
			sql:  "select a from x1 group by a,b,c",
			best: "DataScan(x1)->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {}",
		},
		{
			sql:  "select b from x1 group by b",
			best: "DataScan(x1)->Aggr(firstrow(test.x1.b))->Projection",
			// b --> b is natural existed, so it won't exist in fd.
			fd: "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {} >>> {}",
		},
		{
			sql:  "select b as e from x1 group by b",
			best: "DataScan(x1)->Aggr(firstrow(test.x1.b))->Projection",
			// b --> b is naturally existed, so it won't exist in fd.
			fd: "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {} >>> {}",
		},
		{
			sql:  "select b+c from x1 group by b+c",
			best: "DataScan(x1)->Aggr(firstrow(test.x1.b),firstrow(test.x1.c))->Projection",
			// avoid allocating unique ID 7 from fd temporarily, and substituted by unique ID 5
			// attention:
			// b+c is an expr assigned with new plan ID when building upper-layer projection.
			// when extracting FD after build phase is done, we should be able to recognize a+b in lower-layer group by item with the same unique ID.
			// that's why we introduce session variable MapHashCode2UniqueID4ExtendedCol in.
			fd: "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(2,3)-->(5), (5)~~>(2,3)} >>> {(2,3)-->(5), (5)~~>(2,3)}",
		},
		{
			sql:  "select b+c, min(a) from x1 group by b+c, b-c",
			best: "DataScan(x1)->Aggr(min(test.x1.a),firstrow(test.x1.b),firstrow(test.x1.c))->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(2,3)-->(6,8), (6,8)~~>(2,3,5)} >>> {(2,3)-->(6)}",
		},
		{
			sql:  "select b+c, min(a) from x1 group by b, c",
			best: "DataScan(x1)->Aggr(min(test.x1.a),firstrow(test.x1.b),firstrow(test.x1.c))->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(2,3)~~>(5)} >>> {(2,3)~~>(5), (2,3)-->(6)}",
		},
		{
			sql:  "select b+c from x1 group by b,c",
			best: "DataScan(x1)->Aggr(firstrow(test.x1.b),firstrow(test.x1.c))->Projection",
			// b --> b is naturally existed, so it won't exist in fd.
			fd: "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {} >>> {(2,3)-->(5)}",
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
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {} >>> {(3)-->(15)}",
		},
		{
			sql:  "select exists (select * from x1) from x1 group by d",
			best: "DataScan(x1)->Aggr(firstrow(1))->Projection",
			// 14 is added in the logicAgg pruning process cause all the columns of agg has been pruned.
			fd: "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {(4)~~>(14)} >>> {()-->(13)}",
		},
		{
			sql:  "select c is null from x1 group by c",
			best: "DataScan(x1)->Aggr(firstrow(test.x1.c))->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {} >>> {(3)-->(5)}",
		},
		{
			sql:  "select c is true from x1 group by c",
			best: "DataScan(x1)->Aggr(firstrow(test.x1.c))->Projection",
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {} >>> {(3)-->(5)}",
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
			fd:   "{(1)-->(2-4), (2,3)~~>(1,4), (2,4)-->(1,3)} >>> {} >>> {(2)-->(5)}",
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
			fd: "{(1)-->(2-4), (2,3)-->(1,4), (2,4)-->(1,3), (3,4)==(3,4)} >>> {(1)-->(2-4), (2,3)-->(1,4), (2,4)-->(1,3), (3,4)==(3,4)} >>> {(1)-->(2-4), (2,3)-->(1,4), (2,4)-->(1,3), (3,4)==(3,4)}",
		},
	}

	ctx := context.TODO()
	is := testGetIS(ass, tk.Session())
	for i, tt := range tests {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt.sql)
		stmt, err := par.ParseOneStmt(tt.sql, "", "")
		ass.Nil(err, comment)
		tk.Session().GetSessionVars().PlanID = 0
		tk.Session().GetSessionVars().PlanColumnID = 0
		err = plannercore.Preprocess(tk.Session(), stmt, plannercore.WithPreprocessorReturn(&plannercore.PreprocessorReturn{InfoSchema: is}))
		ass.Nil(err)
		tk.Session().PrepareTSFuture(ctx)
		builder, _ := plannercore.NewPlanBuilder().Init(tk.Session(), is, &hint.BlockHintProcessor{})
		// extract FD to every OP
		p, err := builder.Build(ctx, stmt)
		ass.Nil(err)
		p, err = plannercore.LogicalOptimizeTest(ctx, builder.GetOptFlag(), p.(plannercore.LogicalPlan))
		ass.Nil(err)
		ass.Equal(tt.best, plannercore.ToString(p), comment)
		// extract FD to every OP
		p.(plannercore.LogicalPlan).ExtractFD()
		ass.Equal(tt.fd, plannercore.FDToString(p.(plannercore.LogicalPlan)), comment)
	}
}
