// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testPlanSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testPlanSuite struct {
	*parser.Parser
}

func (s *testPlanSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
}

func mockResolve(node ast.Node) error {
	indices := []*model.IndexInfo{
		{
			Name: model.NewCIStr("c_d_e"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("c"),
					Length: types.UnspecifiedLength,
				},
				{
					Name:   model.NewCIStr("d"),
					Length: types.UnspecifiedLength,
				},
				{
					Name:   model.NewCIStr("e"),
					Length: types.UnspecifiedLength,
				},
			},
		},
	}
	pkColumn := &model.ColumnInfo{
		State: model.StatePublic,
		Name:  model.NewCIStr("a"),
	}
	col0 := &model.ColumnInfo{
		State: model.StatePublic,
		Name:  model.NewCIStr("b"),
	}
	col1 := &model.ColumnInfo{
		State: model.StatePublic,
		Name:  model.NewCIStr("c"),
	}
	col2 := &model.ColumnInfo{
		State: model.StatePublic,
		Name:  model.NewCIStr("d"),
	}
	col3 := &model.ColumnInfo{
		State: model.StatePublic,
		Name:  model.NewCIStr("e"),
	}
	pkColumn.Flag = mysql.PriKeyFlag
	table := &model.TableInfo{
		Columns:    []*model.ColumnInfo{pkColumn, col0, col1, col2, col3},
		Indices:    indices,
		Name:       model.NewCIStr("t"),
		PKIsHandle: true,
	}
	is := infoschema.MockInfoSchema([]*model.TableInfo{table})
	ctx := mock.NewContext()
	variable.BindSessionVars(ctx)
	return MockResolveName(node, is, "test", ctx)
}

func (s *testPlanSuite) TestPredicatePushDown(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql   string
		first string
		best  string
	}{
		{
			sql:   "select count(*) from t a, t b where a.a = b.a",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Aggr->Projection",
			best:  "Join{DataScan(t)->DataScan(t)}->Aggr->Projection",
		},
		{
			sql:   "select a from (select a from t where d = 0) k where k.a = 5",
			first: "DataScan(t)->Selection->Projection->Selection->Projection",
			best:  "DataScan(t)->Selection->Projection->Projection",
		},
		{
			sql:   "select a from (select 1+2 as a from t where d = 0) k where k.a = 5",
			first: "DataScan(t)->Selection->Projection->Selection->Projection",
			best:  "DataScan(t)->Selection->Projection->Projection",
		},
		{
			sql:   "select a from (select d as a from t where d = 0) k where k.a = 5",
			first: "DataScan(t)->Selection->Projection->Selection->Projection",
			best:  "DataScan(t)->Selection->Projection->Projection",
		},
		{
			sql:   "select * from t ta, t tb where (ta.d, ta.a) = (tb.b, tb.c)",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
			best:  "Join{DataScan(t)->DataScan(t)}->Projection",
		},
		{
			sql:   "select * from t ta join t tb on ta.d = tb.d and ta.d > 1 where tb.a = 0",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
			best:  "Join{DataScan(t)->Selection->DataScan(t)->Selection}->Projection",
		},
		{
			sql:   "select * from t ta join t tb on ta.d = tb.d where ta.d > 1 and tb.a = 0",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
			best:  "Join{DataScan(t)->Selection->DataScan(t)->Selection}->Projection",
		},
		{
			sql:   "select * from t ta left outer join t tb on ta.d = tb.d and ta.d > 1 where tb.a = 0",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
			best:  "Join{DataScan(t)->Selection->DataScan(t)->Selection}->Projection",
		},
		{
			sql:   "select * from t ta right outer join t tb on ta.d = tb.d and ta.a > 1 where tb.a = 0",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
			best:  "Join{DataScan(t)->Selection->DataScan(t)->Selection}->Projection",
		},
		{
			sql:   "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where ta.d = 0",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
			best:  "Join{DataScan(t)->Selection->DataScan(t)}->Projection",
		},
		{
			sql:   "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where tb.d = 0",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
			best:  "Join{DataScan(t)->Selection->DataScan(t)->Selection}->Projection",
		},
		{
			sql:   "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where tb.c is not null and tb.c = 0 and ifnull(tb.d, 1)",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
			best:  "Join{DataScan(t)->Selection->DataScan(t)->Selection}->Projection",
		},
		{
			sql:   "select * from t ta left outer join t tb on ta.a = tb.a left outer join t tc on tb.b = tc.b where tc.c > 0",
			first: "Join{Join{DataScan(t)->DataScan(t)}->DataScan(t)}->Selection->Projection",
			best:  "Join{Join{DataScan(t)->DataScan(t)}->DataScan(t)->Selection}->Projection",
		},
		{
			sql:   "select * from t ta left outer join t tb on ta.a = tb.a left outer join t tc on tc.b = ta.b where tb.c > 0",
			first: "Join{Join{DataScan(t)->DataScan(t)}->DataScan(t)}->Selection->Projection",
			best:  "Join{Join{DataScan(t)->DataScan(t)->Selection}->DataScan(t)}->Projection",
		},
		{
			sql:   "select * from t as ta left outer join (t as tb left join t as tc on tc.b = tb.b) on tb.a = ta.a where tc.c > 0",
			first: "Join{DataScan(t)->Join{DataScan(t)->DataScan(t)}}->Selection->Projection",
			best:  "Join{DataScan(t)->Join{DataScan(t)->DataScan(t)->Selection}}->Projection",
		},
		{
			sql:   "select * from ( t as ta left outer join t as tb on ta.a = tb.a) join ( t as tc left join t as td on tc.b = td.b) on ta.c = td.c where tb.c = 2 and td.a = 1",
			first: "Join{Join{DataScan(t)->DataScan(t)}->Join{DataScan(t)->DataScan(t)}}->Selection->Projection",
			best:  "Join{Join{DataScan(t)->DataScan(t)->Selection}->Join{DataScan(t)->DataScan(t)->Selection}}->Projection",
		},
		{
			sql:   "select * from t ta left outer join (t tb left outer join t tc on tc.b = tb.b) on tb.a = ta.a and tc.c = ta.c where tc.d > 0 or ta.d > 0",
			first: "Join{DataScan(t)->Join{DataScan(t)->DataScan(t)}}->Selection->Projection",
			best:  "Join{DataScan(t)->Join{DataScan(t)->DataScan(t)}}->Selection->Projection",
		},
		{
			sql:   "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where ifnull(tb.d, null) or tb.d is null",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
			best:  "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
		},
		{
			sql:   "select a, d from (select * from t union all select * from t union all select * from t) z where a < 10",
			first: "UnionAll{DataScan(t)->Projection->DataScan(t)->Projection->DataScan(t)->Projection}->Selection->Projection",
			best:  "UnionAll{DataScan(t)->Selection->Projection->DataScan(t)->Selection->Projection->DataScan(t)->Selection->Projection}->Projection",
		},
		{
			sql:   "select (select count(*) from t where t.a = k.a) from t k",
			first: "DataScan(t)->Apply(DataScan(t)->Selection->Aggr->Projection->MaxOneRow)->Projection",
			best:  "DataScan(t)->Apply(DataScan(t)->Selection->Aggr->Projection->MaxOneRow)->Projection",
		},
		{
			sql:   "select a from t where exists(select 1 from t as x where x.a < t.a)",
			first: "Join{DataScan(t)->DataScan(t)}->Projection",
			best:  "Join{DataScan(t)->DataScan(t)}->Projection",
		},
		{
			sql:   "select a from t where exists(select 1 from t as x where x.a = t.a and t.a < 1 and x.a < 1)",
			first: "Join{DataScan(t)->DataScan(t)}->Projection",
			best:  "Join{DataScan(t)->Selection->DataScan(t)->Selection}->Projection",
		},
		{
			sql:   "select a from t where exists(select 1 from t as x where x.a = t.a and x.a < 1) and a < 1",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
			best:  "Join{DataScan(t)->Selection->DataScan(t)->Selection}->Projection",
		},
		{
			sql:   "select a from t where exists(select 1 from t as x where x.a = t.a) and exists(select 1 from t as x where x.a = t.a)",
			first: "Join{Join{DataScan(t)->DataScan(t)}->DataScan(t)}->Projection",
			best:  "Join{Join{DataScan(t)->DataScan(t)}->DataScan(t)}->Projection",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		err = mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mock.NewContext(),
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)
		c.Assert(ToString(lp), Equals, ca.first, Commentf("for %s", ca.sql))

		_, lp, err = lp.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		_, err = lp.PruneColumnsAndResolveIndices(lp.GetSchema())
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, ca.best, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestJoinReOrder(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select * from t t1, t t2, t t3, t t4, t t5, t t6 where t1.a = t2.b and t2.a = t3.b and t3.c = t4.a and t4.d = t2.c and t5.d = t6.d",
			best: "LeftHashJoin{LeftHashJoin{LeftHashJoin{LeftHashJoin{Table(t)->Table(t)}(t1.a,t2.b)->Table(t)}(t2.a,t3.b)->Table(t)}(t3.c,t4.a)(t2.c,t4.d)->LeftHashJoin{Table(t)->Table(t)}(t5.d,t6.d)}->Projection",
		},
		{
			sql:  "select * from t t1, t t2, t t3, t t4, t t5, t t6, t t7, t t8 where t1.a = t8.a",
			best: "LeftHashJoin{LeftHashJoin{LeftHashJoin{LeftHashJoin{Table(t)->Table(t)}(t1.a,t8.a)->Table(t)}->LeftHashJoin{Table(t)->Table(t)}}->LeftHashJoin{LeftHashJoin{Table(t)->Table(t)}->Table(t)}}->Projection",
		},
		{
			sql:  "select * from t t1, t t2, t t3, t t4, t t5 where t1.a = t5.a and t5.a = t4.a and t4.a = t3.a and t3.a = t2.a and t2.a = t1.a and t1.a = t3.a and t2.a = t4.a and t5.b < 8",
			best: "LeftHashJoin{LeftHashJoin{LeftHashJoin{RightHashJoin{Table(t)->Selection->Table(t)}(t5.a,t1.a)->Table(t)}(t1.a,t2.a)->Table(t)}(t2.a,t3.a)(t1.a,t3.a)->Table(t)}(t5.a,t4.a)(t3.a,t4.a)(t2.a,t4.a)->Projection",
		},
		{
			sql:  "select * from t t1, t t2, t t3, t t4, t t5 where t1.a = t5.a and t5.a = t4.a and t4.a = t3.a and t3.a = t2.a and t2.a = t1.a and t1.a = t3.a and t2.a = t4.a and t3.b = 1 and t4.a = 1",
			best: "LeftHashJoin{LeftHashJoin{LeftHashJoin{LeftHashJoin{Table(t)->Selection->Table(t)}(t3.a,t4.a)->Table(t)}(t4.a,t5.a)->Table(t)}(t5.a,t1.a)(t3.a,t1.a)->Table(t)}(t3.a,t2.a)(t1.a,t2.a)(t4.a,t2.a)->Projection",
		},
		{
			sql:  "select * from t o where o.b in (select t3.c from t t1, t t2, t t3 where t1.a = t3.a and t2.a = t3.a and t2.a = o.a)",
			best: "Table(t)->Apply(LeftHashJoin{RightHashJoin{Table(t)->Selection->Table(t)}(t2.a,t3.a)->Table(t)}(t3.a,t1.a)->Projection)->Selection->Projection",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		err = mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mock.NewContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)

		_, lp, err = lp.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		_, err = lp.PruneColumnsAndResolveIndices(lp.GetSchema())
		c.Assert(err, IsNil)
		_, res, _, err := lp.convert2PhysicalPlan(nil)
		c.Assert(err, IsNil)
		p = res.p.PushLimit(nil)
		c.Assert(ToString(p), Equals, ca.best, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestCBO(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select * from t t1 where 1 = 0",
			best: "Dummy->Projection",
		},
		{
			sql:  "select count(*) from t t1 having 1 = 0",
			best: "Dummy->Aggr->Selection->Projection",
		},
		{
			sql:  "select * from t a where a.c = 1 order by a.d limit 2",
			best: "Index(t.c_d_e)[[1,1]]->Projection",
		},
		{
			sql:  "select * from t a where 1 = a.c and a.d > 1 order by a.d desc limit 2",
			best: "Index(t.c_d_e)[(1 1,1 +inf]]->Projection",
		},
		{
			sql:  "select * from t a where a.c < 10000 order by a.a limit 2",
			best: "Table(t)->Selection->Limit->Projection",
		},
		{
			sql:  "select * from (select * from t) a left outer join (select * from t) b on 1 order by a.c",
			best: "LeftHashJoin{Index(t.c_d_e)[[<nil>,+inf]]->Projection->Table(t)->Projection}->Projection",
		},
		{
			sql:  "select * from (select * from t) a left outer join (select * from t) b on 1 order by b.c",
			best: "LeftHashJoin{Table(t)->Projection->Table(t)->Projection}->Projection->Sort",
		},
		{
			sql:  "select * from (select * from t) a right outer join (select * from t) b on 1 order by a.c",
			best: "RightHashJoin{Table(t)->Projection->Table(t)->Projection}->Projection->Sort",
		},
		{
			sql:  "select * from (select * from t) a right outer join (select * from t) b on 1 order by b.c",
			best: "RightHashJoin{Table(t)->Projection->Index(t.c_d_e)[[<nil>,+inf]]->Projection}->Projection",
		},
		{
			sql:  "select * from t a where exists(select * from t b where a.a = b.a) and a.c = 1 order by a.d limit 3",
			best: "SemiJoin{Index(t.c_d_e)[[1,1]]->Table(t)}->Limit->Projection",
		},
		{
			sql:  "select exists(select * from t b where a.a = b.a and b.c = 1) from t a order by a.c limit 3",
			best: "SemiJoinWithAux{Index(t.c_d_e)[[<nil>,+inf]]->Index(t.c_d_e)[[1,1]]}->Projection->Trim",
		},
		{
			sql:  "select * from (select t.a from t union select t.d from t where t.c = 1 union select t.c from t) k order by a limit 1",
			best: "UnionAll{Table(t)->Projection->Index(t.c_d_e)[[1,1]]->Projection->Index(t.c_d_e)[[<nil>,+inf]]->Projection}->Distinct->Limit->Projection",
		},
		{
			sql:  "select * from (select t.a from t union select t.d from t union select t.c from t) k order by a limit 1",
			best: "UnionAll{Table(t)->Projection->Table(t)->Projection->Table(t)->Projection}->Distinct->Projection->Sort + Limit(1) + Offset(0)",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		err = mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mock.NewContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)

		_, lp, err = lp.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		_, err = lp.PruneColumnsAndResolveIndices(lp.GetSchema())
		c.Assert(err, IsNil)
		_, res, _, err := lp.convert2PhysicalPlan(nil)
		c.Assert(err, IsNil)
		p = res.p.PushLimit(nil)
		c.Assert(ToString(p), Equals, ca.best, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestRefine(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select a from t where c is not null",
			best: "Index(t.c_d_e)[[-inf,+inf]]->Projection",
		},
		{
			sql:  "select a from t where c >= 4",
			best: "Index(t.c_d_e)[[4,+inf]]->Projection",
		},
		{
			sql:  "select a from t where c <= 4",
			best: "Index(t.c_d_e)[[-inf,4]]->Projection",
		},
		{
			sql:  "select a from t where c = 4 and d = 5 and e = 6",
			best: "Index(t.c_d_e)[[4 5 6,4 5 6]]->Projection",
		},
		{
			sql:  "select a from t where d = 4 and c = 5",
			best: "Index(t.c_d_e)[[5 4,5 4]]->Projection",
		},
		{
			sql:  "select a from t where c = 4 and e < 5",
			best: "Index(t.c_d_e)[[4,4]]->Selection->Projection",
		},
		{
			sql:  "select a from t where c = 4 and d <= 5 and d > 3",
			best: "Index(t.c_d_e)[(4 3,4 5]]->Projection",
		},
		{
			sql:  "select a from t where d <= 5 and d > 3",
			best: "Table(t)->Selection->Projection",
		},
		{
			sql:  "select a from t where c <= 5 and c >= 3 and d = 1",
			best: "Index(t.c_d_e)[[3,5]]->Selection->Projection",
		},
		{
			sql:  "select a from t where c = 1 or c = 2 or c = 3",
			best: "Index(t.c_d_e)[[1,1] [2,2] [3,3]]->Projection",
		},
		{
			sql:  "select b from t where c = 1 or c = 2 or c = 3 or c = 4 or c = 5",
			best: "Table(t)->Selection->Projection",
		},
		{
			sql:  "select a from t where c = 5",
			best: "Index(t.c_d_e)[[5,5]]->Projection",
		},
		{
			sql:  "select a from t where c = 5 and b = 1",
			best: "Index(t.c_d_e)[[5,5]]->Selection->Projection",
		},
		{
			sql:  "select a from t where c in (1)",
			best: "Index(t.c_d_e)[[1,1]]->Projection",
		},
		{
			sql:  "select a from t where c in (1) and d > 3",
			best: "Index(t.c_d_e)[[1,1]]->Selection->Projection",
		},
		{
			sql:  "select a from t where c in (1, 2, 3)",
			best: "Index(t.c_d_e)[[1,1] [2,2] [3,3]]->Projection",
		},
		{
			sql:  "select a from t where d in (1, 2, 3)",
			best: "Table(t)->Selection->Projection",
		},
		{
			sql:  "select a from t where c not in (1)",
			best: "Table(t)->Selection->Projection",
		},
		{
			sql:  "select a from t where c like ''",
			best: "Index(t.c_d_e)[[,]]->Projection",
		},
		{
			sql:  "select a from t where c like 'abc'",
			best: "Index(t.c_d_e)[[abc,abc]]->Projection",
		},
		{
			sql:  "select a from t where c not like 'abc'",
			best: "Table(t)->Selection->Projection",
		},
		{
			sql:  "select a from t where not (c like 'abc' or c like 'abd')",
			best: "Table(t)->Selection->Projection",
		},
		{
			sql:  "select a from t where c like '_abc'",
			best: "Table(t)->Selection->Projection",
		},
		{
			sql:  "select a from t where c like 'abc%'",
			best: "Index(t.c_d_e)[[abc,abd)]->Projection",
		},
		{
			sql:  "select a from t where c like 'abc_'",
			best: "Index(t.c_d_e)[(abc,abd)]->Selection->Projection",
		},
		{
			sql:  "select a from t where c like 'abc%af'",
			best: "Index(t.c_d_e)[[abc,abd)]->Selection->Projection",
		},
		{
			sql:  `select a from t where c like 'abc\\_' escape ''`,
			best: "Index(t.c_d_e)[[abc_,abc_]]->Projection",
		},
		{
			sql:  `select a from t where c like 'abc\\_'`,
			best: "Index(t.c_d_e)[[abc_,abc_]]->Projection",
		},
		{
			sql:  `select a from t where c like 'abc\\\\_'`,
			best: "Index(t.c_d_e)[(abc\\,abc])]->Selection->Projection",
		},
		{
			sql:  `select a from t where c like 'abc\\_%'`,
			best: "Index(t.c_d_e)[[abc_,abc`)]->Projection",
		},
		{
			sql:  `select a from t where c like 'abc=_%' escape '='`,
			best: "Index(t.c_d_e)[[abc_,abc`)]->Projection",
		},
		{
			sql:  `select a from t where c like 'abc\\__'`,
			best: "Index(t.c_d_e)[(abc_,abc`)]->Selection->Projection",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		err = mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mock.NewContext(),
		}
		p := builder.build(stmt).(LogicalPlan)
		c.Assert(builder.err, IsNil)

		_, p, err = p.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		_, err = p.PruneColumnsAndResolveIndices(p.GetSchema())
		c.Assert(err, IsNil)
		_, res, _, err := p.convert2PhysicalPlan(nil)
		c.Assert(err, IsNil)
		np := res.p.PushLimit(nil)
		c.Assert(ToString(np), Equals, ca.best, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestColumnPruning(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql string
		ans map[string][]string
	}{
		{
			sql: "select count(*) from t group by a",
			ans: map[string][]string{
				"TableScan_1": {"a"},
			},
		},
		{
			sql: "select count(*) from t",
			ans: map[string][]string{
				"TableScan_1": {},
			},
		},
		{
			sql: "select count(*) from t a join t b where a.a < 1",
			ans: map[string][]string{
				"TableScan_1": {"a"},
				"TableScan_2": {},
			},
		},
		{
			sql: "select count(*) from t a join t b on a.a = b.d",
			ans: map[string][]string{
				"TableScan_1": {"a"},
				"TableScan_2": {"d"},
			},
		},
		{
			sql: "select count(*) from t a join t b on a.a = b.d order by sum(a.d)",
			ans: map[string][]string{
				"TableScan_1": {"a", "d"},
				"TableScan_2": {"d"},
			},
		},
		{
			sql: "select count(b.a) from t a join t b on a.a = b.d group by b.b order by sum(a.d)",
			ans: map[string][]string{
				"TableScan_1": {"a", "d"},
				"TableScan_2": {"a", "b", "d"},
			},
		},
		{
			sql: "select * from (select count(b.a) from t a join t b on a.a = b.d group by b.b having sum(a.d) < 0) tt",
			ans: map[string][]string{
				"TableScan_1": {"a", "d"},
				"TableScan_2": {"a", "b", "d"},
			},
		},
		{
			sql: "select (select count(a) from t where b = k.a) from t k",
			ans: map[string][]string{
				"TableScan_1": {"a"},
				"TableScan_2": {"a", "b"},
			},
		},
		{
			sql: "select exists (select count(*) from t where b = k.a) from t k",
			ans: map[string][]string{
				"TableScan_1": {"a"},
				"TableScan_2": {"b"},
			},
		},
		{
			sql: "select b = (select count(*) from t where b = k.a) from t k",
			ans: map[string][]string{
				"TableScan_1": {"a", "b"},
				"TableScan_2": {"b"},
			},
		},
		{
			sql: "select exists (select count(a) from t where b = k.a) from t k",
			ans: map[string][]string{
				"TableScan_1": {"a"},
				"TableScan_2": {"b"},
			},
		},
		{
			sql: "select a as c1, b as c2 from t order by 1, c1 + c2 + c",
			ans: map[string][]string{
				"TableScan_1": {"a", "b", "c"},
			},
		},
		{
			sql: "select a from t where b < any (select c from t)",
			ans: map[string][]string{
				"TableScan_1": {"a", "b"},
				"TableScan_2": {"c"},
			},
		},
		{
			sql: "select a from t where (b,a) != all (select c,d from t)",
			ans: map[string][]string{
				"TableScan_1": {"a", "b"},
				"TableScan_2": {"c", "d"},
			},
		},
		{
			sql: "select a from t where (b,a) in (select c,d from t)",
			ans: map[string][]string{
				"TableScan_1": {"a", "b"},
				"TableScan_2": {"c", "d"},
			},
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		err = mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			colMapper: make(map[*ast.ColumnNameExpr]int),
			allocator: new(idAllocator),
			ctx:       mock.NewContext(),
		}
		p := builder.build(stmt).(LogicalPlan)
		c.Assert(builder.err, IsNil, comment)

		_, p, err = p.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		_, err = p.PruneColumnsAndResolveIndices(p.GetSchema())
		c.Assert(err, IsNil)
		check(p, c, ca.ans, comment)
	}
}

func (s *testPlanSuite) TestAllocID(c *C) {
	pA := &DataSource{baseLogicalPlan: newBaseLogicalPlan(Ts, new(idAllocator))}

	pB := &DataSource{baseLogicalPlan: newBaseLogicalPlan(Ts, new(idAllocator))}

	pA.initID()
	pB.initID()
	c.Assert(pA.id, Equals, pB.id)
}

func (s *testPlanSuite) TestNewRangeBuilder(c *C) {
	defer testleak.AfterTest(c)()
	rb := &rangeBuilder{}

	cases := []struct {
		exprStr   string
		resultStr string
	}{
		{
			exprStr:   "a = 1",
			resultStr: "[[1 1]]",
		},
		{
			exprStr:   "1 = a",
			resultStr: "[[1 1]]",
		},
		{
			exprStr:   "a != 1",
			resultStr: "[[-inf 1) (1 +inf]]",
		},
		{
			exprStr:   "1 != a",
			resultStr: "[[-inf 1) (1 +inf]]",
		},
		{
			exprStr:   "a > 1",
			resultStr: "[(1 +inf]]",
		},
		{
			exprStr:   "1 < a",
			resultStr: "[(1 +inf]]",
		},
		{
			exprStr:   "a >= 1",
			resultStr: "[[1 +inf]]",
		},
		{
			exprStr:   "1 <= a",
			resultStr: "[[1 +inf]]",
		},
		{
			exprStr:   "a < 1",
			resultStr: "[[-inf 1)]",
		},
		{
			exprStr:   "1 > a",
			resultStr: "[[-inf 1)]",
		},
		{
			exprStr:   "a <= 1",
			resultStr: "[[-inf 1]]",
		},
		{
			exprStr:   "1 >= a",
			resultStr: "[[-inf 1]]",
		},
		{
			exprStr:   "(a)",
			resultStr: "[[-inf 0) (0 +inf]]",
		},
		{
			exprStr:   "a in (1, 3, NULL, 2)",
			resultStr: "[[<nil> <nil>] [1 1] [2 2] [3 3]]",
		},
		{
			exprStr:   `a IN (8,8,81,45)`,
			resultStr: `[[8 8] [45 45] [81 81]]`,
		},
		{
			exprStr:   "a between 1 and 2",
			resultStr: "[[1 2]]",
		},
		{
			exprStr:   "a not between 1 and 2",
			resultStr: "[[-inf 1) (2 +inf]]",
		},
		{
			exprStr:   "a not between null and 0",
			resultStr: "[(0 +inf]]",
		},
		{
			exprStr:   "a between 2 and 1",
			resultStr: "[]",
		},
		{
			exprStr:   "a not between 2 and 1",
			resultStr: "[[-inf +inf]]",
		},
		{
			exprStr:   "a IS NULL",
			resultStr: "[[<nil> <nil>]]",
		},
		{
			exprStr:   "a IS NOT NULL",
			resultStr: "[[-inf +inf]]",
		},
		{
			exprStr:   "a IS TRUE",
			resultStr: "[[-inf 0) (0 +inf]]",
		},
		{
			exprStr:   "a IS NOT TRUE",
			resultStr: "[[<nil> <nil>] [0 0]]",
		},
		{
			exprStr:   "a IS FALSE",
			resultStr: "[[0 0]]",
		},
		{
			exprStr:   "a IS NOT FALSE",
			resultStr: "[[<nil> 0) (0 +inf]]",
		},
		{
			exprStr:   "a LIKE 'abc%'",
			resultStr: "[[abc abd)]",
		},
		{
			exprStr:   "a LIKE 'abc_'",
			resultStr: "[(abc abd)]",
		},
		{
			exprStr:   "a LIKE 'abc'",
			resultStr: "[[abc abc]]",
		},
		{
			exprStr:   `a LIKE "ab\_c"`,
			resultStr: "[[ab_c ab_c]]",
		},
		{
			exprStr:   "a LIKE '%'",
			resultStr: "[[-inf +inf]]",
		},
		{
			exprStr:   `a LIKE '\%a'`,
			resultStr: `[[%a %a]]`,
		},
		{
			exprStr:   `a LIKE "\\"`,
			resultStr: `[[\ \]]`,
		},
		{
			exprStr:   `a LIKE "\\\\a%"`,
			resultStr: `[[\a \b)]`,
		},
		{
			exprStr:   `0.4`,
			resultStr: `[]`,
		},
		{
			exprStr:   `a > NULL`,
			resultStr: `[]`,
		},
	}

	for _, ca := range cases {
		sql := "select 1 from t where " + ca.exprStr
		stmts, err := s.Parse(sql, "", "")
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, ca.exprStr))
		stmt := stmts[0].(*ast.SelectStmt)

		err = mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{allocator: new(idAllocator), ctx: mock.NewContext()}
		p := builder.build(stmt)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, ca.exprStr))
		var selection *Selection
		for _, child := range p.GetChildren() {
			plan, ok := child.(*Selection)
			if ok {
				selection = plan
				break
			}
		}
		c.Assert(selection, NotNil, Commentf("expr:%v", ca.exprStr))
		result := fullRange
		for _, cond := range selection.Conditions {
			result = rb.intersection(result, rb.newBuild(cond))
		}
		c.Assert(rb.err, IsNil)
		got := fmt.Sprintf("%v", result)
		c.Assert(got, Equals, ca.resultStr, Commentf("different for expr %s", ca.exprStr))
	}
}

func (s *testPlanSuite) TestTableScanWithOrder(c *C) {
	defer testleak.AfterTest(c)()
	// Sort result by scanning PKHandle column.
	sql := "select * from t order by a limit 1;"
	stmt, err := s.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	ast.SetFlag(stmt)

	err = mockResolve(stmt)
	c.Assert(err, IsNil)

	builder := &planBuilder{
		allocator: new(idAllocator),
		ctx:       mock.NewContext(),
		colMapper: make(map[*ast.ColumnNameExpr]int),
	}
	p := builder.build(stmt)
	c.Assert(builder.err, IsNil)
	logic, ok := p.(LogicalPlan)
	c.Assert(ok, IsTrue)
	// Get physical plan.
	_, pp, _, err := logic.convert2PhysicalPlan(nil)
	c.Assert(err, IsNil)
	// Limit->Projection->PhysicalTableScan
	// Get PhysicalTableScan plan.
	cpp, ok := pp.p.GetChildByIndex(0).GetChildByIndex(0).(*PhysicalTableScan)
	c.Assert(cpp, NotNil)
	c.Assert(ok, IsTrue)
	// Make sure KeepOrder is true.
	c.Assert(cpp.KeepOrder, IsTrue)
}

func (s *testPlanSuite) TestConstantFolding(c *C) {
	defer testleak.AfterTest(c)()

	cases := []struct {
		exprStr   string
		resultStr string
	}{
		{
			exprStr:   "a < 1 + 2",
			resultStr: "lt(test.t.a, 3)",
		},
		{
			exprStr:   "a < greatest(1, 2)",
			resultStr: "lt(test.t.a, 2)",
		},
		{
			exprStr:   "a <  1 + 2 + 3 + b",
			resultStr: "lt(test.t.a, plus(6, test.t.b))",
		},
		{
			exprStr: "a = CASE 1+2 " +
				"WHEN 3 THEN 'a' " +
				"WHEN 1 THEN 'b' " +
				"END;",
			resultStr: "eq(test.t.a, a)",
		},
		{
			exprStr:   "a in (hex(12), 'a', '9')",
			resultStr: "in(test.t.a, C, a, 9)",
		},
		{
			exprStr:   "'string' is not null",
			resultStr: "1",
		},
		{
			exprStr:   "'string' is null",
			resultStr: "0",
		},
		{
			exprStr:   "a = !(1+1)",
			resultStr: "eq(test.t.a, 0)",
		},
		{
			exprStr:   "a = rand()",
			resultStr: "eq(test.t.a, rand())",
		},
		{
			exprStr:   "a = version()",
			resultStr: "eq(test.t.a, version())",
		},
	}

	for _, ca := range cases {
		sql := "select 1 from t where " + ca.exprStr
		stmts, err := s.Parse(sql, "", "")
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, ca.exprStr))
		stmt := stmts[0].(*ast.SelectStmt)

		err = mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{allocator: new(idAllocator), ctx: mock.NewContext()}
		p := builder.build(stmt)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, ca.exprStr))

		selection := p.GetChildByIndex(0).(*Selection)
		c.Assert(selection, NotNil, Commentf("expr:%v", ca.exprStr))

		c.Assert(expression.ComposeCNFCondition(selection.Conditions).String(), Equals, ca.resultStr, Commentf("different for expr %s", ca.exprStr))
	}
}

func (s *testPlanSuite) TestCoveringIndex(c *C) {
	cases := []struct {
		columnNames []string
		indexNames  []string
		indexLens   []int
		isCovering  bool
	}{
		{[]string{"a"}, []string{"a"}, []int{-1}, true},
		{[]string{"a"}, []string{"a", "b"}, []int{-1, -1}, true},
		{[]string{"a", "b"}, []string{"b", "a"}, []int{-1, -1}, true},
		{[]string{"a", "b"}, []string{"b", "c"}, []int{-1, -1}, false},
		{[]string{"a", "b"}, []string{"a", "b"}, []int{50, -1}, false},
		{[]string{"a", "b"}, []string{"a", "c"}, []int{-1, -1}, false},
		{[]string{"id", "a"}, []string{"a", "b"}, []int{-1, -1}, true},
	}
	for _, ca := range cases {
		var columns []*model.ColumnInfo
		var pkIsHandle bool
		for _, cn := range ca.columnNames {
			col := &model.ColumnInfo{Name: model.NewCIStr(cn)}
			if cn == "id" {
				pkIsHandle = true
				col.Flag = mysql.PriKeyFlag
			}
			columns = append(columns, col)
		}
		var indexCols []*model.IndexColumn
		for i := range ca.indexNames {
			icn := ca.indexNames[i]
			icl := ca.indexLens[i]
			indexCols = append(indexCols, &model.IndexColumn{Name: model.NewCIStr(icn), Length: icl})
		}
		covering := isCoveringIndex(columns, indexCols, pkIsHandle)
		c.Assert(covering, Equals, ca.isCovering)
	}
}

func check(p Plan, c *C, ans map[string][]string, comment CommentInterface) {
	switch p.(type) {
	case *PhysicalTableScan:
		colList, ok := ans[p.GetID()]
		c.Assert(ok, IsTrue, comment)
		for i, colName := range colList {
			c.Assert(colName, Equals, p.GetSchema()[i].ColName.L, comment)
		}
	}
	for _, child := range p.GetChildren() {
		check(child, c, ans, comment)
	}
}
