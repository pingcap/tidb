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
	"sort"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

var _ = Suite(&testPlanSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testPlanSuite struct {
	*parser.Parser
}

func (s *testPlanSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
}

func newLongType() types.FieldType {
	return *(types.NewFieldType(mysql.TypeLong))
}

func newStringType() types.FieldType {
	ft := types.NewFieldType(mysql.TypeVarchar)
	ft.Charset, ft.Collate = types.DefaultCharsetForType(mysql.TypeVarchar)
	return *ft
}

func mockResolve(node ast.Node) error {
	indices := []*model.IndexInfo{
		{
			Name: model.NewCIStr("c_d_e"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("c"),
					Length: types.UnspecifiedLength,
					Offset: 1,
				},
				{
					Name:   model.NewCIStr("d"),
					Length: types.UnspecifiedLength,
					Offset: 2,
				},
				{
					Name:   model.NewCIStr("e"),
					Length: types.UnspecifiedLength,
					Offset: 3,
				},
			},
			State: model.StatePublic,
		},
		{
			Name: model.NewCIStr("e"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("e"),
					Length: types.UnspecifiedLength,
				},
			},
			State: model.StateWriteOnly,
		},
		{
			Name: model.NewCIStr("f"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("f"),
					Length: types.UnspecifiedLength,
					Offset: 1,
				},
			},
			State: model.StatePublic,
		},
		{
			Name: model.NewCIStr("g"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("g"),
					Length: types.UnspecifiedLength,
					Offset: 1,
				},
			},
			State: model.StatePublic,
		},
		{
			Name: model.NewCIStr("f_g"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("f"),
					Length: types.UnspecifiedLength,
					Offset: 1,
				},
				{
					Name:   model.NewCIStr("g"),
					Length: types.UnspecifiedLength,
					Offset: 2,
				},
			},
			State: model.StatePublic,
		},
		{
			Name: model.NewCIStr("c_d_e_str"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("c_str"),
					Length: types.UnspecifiedLength,
					Offset: 5,
				},
				{
					Name:   model.NewCIStr("d_str"),
					Length: types.UnspecifiedLength,
					Offset: 6,
				},
				{
					Name:   model.NewCIStr("e_str"),
					Length: types.UnspecifiedLength,
					Offset: 7,
				},
			},
			State: model.StatePublic,
		},
	}
	pkColumn := &model.ColumnInfo{
		State:     model.StatePublic,
		Name:      model.NewCIStr("a"),
		FieldType: newLongType(),
		ID:        1,
	}
	col0 := &model.ColumnInfo{
		State:     model.StatePublic,
		Name:      model.NewCIStr("b"),
		FieldType: newLongType(),
		ID:        2,
	}
	col1 := &model.ColumnInfo{
		State:     model.StatePublic,
		Name:      model.NewCIStr("c"),
		FieldType: newLongType(),
		ID:        3,
	}
	col2 := &model.ColumnInfo{
		State:     model.StatePublic,
		Name:      model.NewCIStr("d"),
		FieldType: newLongType(),
		ID:        4,
	}
	col3 := &model.ColumnInfo{
		State:     model.StatePublic,
		Name:      model.NewCIStr("e"),
		FieldType: newLongType(),
		ID:        5,
	}
	colStr1 := &model.ColumnInfo{
		State:     model.StatePublic,
		Name:      model.NewCIStr("c_str"),
		FieldType: newStringType(),
		ID:        6,
	}
	colStr2 := &model.ColumnInfo{
		State:     model.StatePublic,
		Name:      model.NewCIStr("d_str"),
		FieldType: newStringType(),
		ID:        7,
	}
	colStr3 := &model.ColumnInfo{
		State:     model.StatePublic,
		Name:      model.NewCIStr("e_str"),
		FieldType: newStringType(),
		ID:        8,
	}
	col4 := &model.ColumnInfo{
		State:     model.StatePublic,
		Name:      model.NewCIStr("f"),
		FieldType: newLongType(),
		ID:        9,
	}
	col5 := &model.ColumnInfo{
		State:     model.StatePublic,
		Name:      model.NewCIStr("g"),
		FieldType: newLongType(),
		ID:        10,
	}

	pkColumn.Flag = mysql.PriKeyFlag
	table := &model.TableInfo{
		Columns:    []*model.ColumnInfo{pkColumn, col0, col1, col2, col3, colStr1, colStr2, colStr3, col4, col5},
		Indices:    indices,
		Name:       model.NewCIStr("t"),
		PKIsHandle: true,
	}
	is := infoschema.MockInfoSchema([]*model.TableInfo{table})
	ctx := mock.NewContext()
	variable.BindSessionVars(ctx)
	err := MockResolveName(node, is, "test", ctx)
	if err != nil {
		return err
	}
	return InferType(node)
}

func supportExpr(exprType tipb.ExprType) bool {
	switch exprType {
	// data type
	case tipb.ExprType_Null, tipb.ExprType_Int64, tipb.ExprType_Uint64,
		tipb.ExprType_Float32, tipb.ExprType_Float64, tipb.ExprType_String,
		tipb.ExprType_Bytes, tipb.ExprType_MysqlDuration, tipb.ExprType_MysqlDecimal,
		tipb.ExprType_MysqlTime, tipb.ExprType_ColumnRef:
		return true
	// logic operators
	case tipb.ExprType_And, tipb.ExprType_Or, tipb.ExprType_Not, tipb.ExprType_Xor:
		return true
	// compare operators
	case tipb.ExprType_LT, tipb.ExprType_LE, tipb.ExprType_EQ, tipb.ExprType_NE,
		tipb.ExprType_GE, tipb.ExprType_GT, tipb.ExprType_NullEQ,
		tipb.ExprType_In, tipb.ExprType_ValueList, tipb.ExprType_Like:
		return true
	// arithmetic operators
	case tipb.ExprType_Plus, tipb.ExprType_Div, tipb.ExprType_Minus,
		tipb.ExprType_Mul, tipb.ExprType_IntDiv, tipb.ExprType_Mod:
		return true
	// aggregate functions
	case tipb.ExprType_Count, tipb.ExprType_First, tipb.ExprType_Sum,
		tipb.ExprType_Avg, tipb.ExprType_Max, tipb.ExprType_Min:
		return true
	// bitwise operators
	case tipb.ExprType_BitAnd, tipb.ExprType_BitOr, tipb.ExprType_BitXor, tipb.ExprType_BitNeg:
		return true
	// control functions
	case tipb.ExprType_Case, tipb.ExprType_If:
		return true
	// other functions
	case tipb.ExprType_Coalesce:
		return true
	case kv.ReqSubTypeDesc:
		return true
	default:
		return false
	}
}

type mockClient struct {
}

func (c *mockClient) Send(_ *kv.Request) kv.Response {
	return nil
}

func (c *mockClient) SupportRequestType(reqType, subType int64) bool {
	switch reqType {
	case kv.ReqTypeSelect, kv.ReqTypeIndex:
		switch subType {
		case kv.ReqSubTypeGroupBy, kv.ReqSubTypeBasic, kv.ReqSubTypeTopN:
			return true
		default:
			return supportExpr(tipb.ExprType(subType))
		}
	}
	return false
}

type mockStore struct {
	client *mockClient
}

func (m *mockStore) GetClient() kv.Client {
	return m.client
}

func (m *mockStore) Begin() (kv.Transaction, error) {
	return nil, nil
}

func (m *mockStore) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
	return nil, nil
}

func (m *mockStore) Close() error {
	return nil
}

func (m *mockStore) UUID() string {
	return "mock"
}

func (m *mockStore) CurrentVersion() (kv.Version, error) {
	return kv.Version{}, nil
}

func mockContext() context.Context {
	ctx := mock.NewContext()
	ctx.Store = &mockStore{
		client: &mockClient{},
	}
	return ctx
}

func (s *testPlanSuite) TestPushDownAggregation(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql       string
		best      string
		aggFuns   string
		aggFields string
		gbyItems  string
	}{
		{
			sql:       "select count(*) from t",
			best:      "Table(t)->HashAgg->Projection",
			aggFuns:   "[count(1)]",
			aggFields: "[blob bigint(21)]",
			gbyItems:  "[]",
		},
		{
			sql:       "select sum(b) from t group by c",
			best:      "Table(t)->HashAgg->Projection",
			aggFuns:   "[sum(test.t.b)]",
			aggFields: "[blob decimal]",
			gbyItems:  "[test.t.c]",
		},
		{
			sql:       "select max(b + c), min(case when b then 1 else 2 end) from t group by d + e, a",
			best:      "Table(t)->HashAgg->Projection",
			aggFuns:   "[max(plus(test.t.b, test.t.c)) min(case(test.t.b, 1, 2))]",
			aggFields: "[blob bigint bigint]",
			gbyItems:  "[plus(test.t.d, test.t.e) test.t.a]",
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
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)

		_, lp, err = lp.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		_, err = lp.PruneColumnsAndResolveIndices(lp.GetSchema())
		c.Assert(err, IsNil)
		info, err := lp.convert2PhysicalPlan(&requiredProperty{})
		c.Assert(err, IsNil)
		c.Assert(ToString(info.p), Equals, ca.best, Commentf("for %s", ca.sql))
		p = info.p
		for {
			var ts *physicalTableSource
			switch x := p.(type) {
			case *PhysicalTableScan:
				ts = &x.physicalTableSource
			case *PhysicalIndexScan:
				ts = &x.physicalTableSource
			}
			if ts != nil {
				c.Assert(fmt.Sprintf("%s", ts.aggFuncs), Equals, ca.aggFuns, Commentf("for %s", ca.sql))
				c.Assert(fmt.Sprintf("%s", ts.gbyItems), Equals, ca.gbyItems, Commentf("for %s", ca.sql))
				c.Assert(fmt.Sprintf("%s", ts.AggFields), Equals, ca.aggFields, Commentf("for %s", ca.sql))
				break
			}
			p = p.GetChildByIndex(0)
		}
	}
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
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Aggr(count(1))->Projection",
			best:  "Join{DataScan(t)->DataScan(t)}->Aggr(count(1))->Projection",
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
			sql:   "select * from t t1, t t2 where t1.a = t2.b and t2.b > 0 and t1.a = t1.c and t1.d like 'abc' and t2.d = t1.d",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
			best:  "Join{DataScan(t)->Selection->DataScan(t)->Selection}->Projection",
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
			first: "DataScan(t)->Apply(DataScan(t)->Selection->Aggr(count(1))->Projection->MaxOneRow)->Projection",
			best:  "DataScan(t)->Apply(DataScan(t)->Selection->Aggr(count(1))->Projection->MaxOneRow)->Projection",
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
		{
			sql:   "select * from (select a, b, sum(c) as s from t group by a, b) k where k.a > k.b * 2 + 1",
			first: "DataScan(t)->Aggr(firstrow(test.t.a),firstrow(test.t.b),sum(test.t.c))->Projection->Selection->Projection",
			best:  "DataScan(t)->Selection->Aggr(firstrow(test.t.a),firstrow(test.t.b),sum(test.t.c))->Projection->Projection",
		},
		{
			sql:   "select * from (select a, b, sum(c) as s from t group by a, b) k where k.a > 1 and k.b > 2",
			first: "DataScan(t)->Aggr(firstrow(test.t.a),firstrow(test.t.b),sum(test.t.c))->Projection->Selection->Projection",
			best:  "DataScan(t)->Selection->Aggr(firstrow(test.t.a),firstrow(test.t.b),sum(test.t.c))->Projection->Projection",
		},
		{
			sql:   "select * from (select k.a, sum(k.s) as ss from (select a, sum(b) as s from t group by a) k group by k.a) l where l.a > 2",
			first: "DataScan(t)->Aggr(firstrow(test.t.a),sum(test.t.b))->Projection->Aggr(firstrow(k.a),sum(k.s))->Projection->Selection->Projection",
			best:  "DataScan(t)->Selection->Aggr(firstrow(test.t.a),sum(test.t.b))->Projection->Aggr(firstrow(k.a),sum(k.s))->Projection->Projection",
		},
		{
			sql:   "select * from (select a, sum(b) as s from t group by a) k where a > s",
			first: "DataScan(t)->Aggr(firstrow(test.t.a),sum(test.t.b))->Projection->Selection->Projection",
			best:  "DataScan(t)->Aggr(firstrow(test.t.a),sum(test.t.b))->Selection->Projection->Projection",
		},
		{
			sql:   "select * from (select a, sum(b) as s from t group by a + 1) k where a > 1",
			first: "DataScan(t)->Aggr(firstrow(test.t.a),sum(test.t.b))->Projection->Selection->Projection",
			best:  "DataScan(t)->Aggr(firstrow(test.t.a),sum(test.t.b))->Selection->Projection->Projection",
		},
		{
			sql:   "select * from (select a, sum(b) as s from t group by a having 1 = 0) k where a > 1",
			first: "DataScan(t)->Aggr(firstrow(test.t.a),sum(test.t.b))->Projection->Selection->Selection->Projection",
			best:  "DataScan(t)->Selection->Aggr(firstrow(test.t.a),sum(test.t.b))->Selection->Projection->Projection",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

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
			best: "LeftHashJoin{RightHashJoin{RightHashJoin{Table(t)->Selection->Table(t)}->LeftHashJoin{Table(t)->Table(t)}}->Table(t)}->Projection",
		},
		{
			sql:  "select * from t o where o.b in (select t3.c from t t1, t t2, t t3 where t1.a = t3.a and t2.a = t3.a and t2.a = o.a)",
			best: "Table(t)->Apply(LeftHashJoin{RightHashJoin{Table(t)->Selection->Table(t)}(t2.a,t3.a)->Table(t)}(t3.a,t1.a)->Projection)->Selection->Projection",
		},
		{
			sql:  "select * from t o where o.b in (select t3.c from t t1, t t2, t t3 where t1.a = t3.a and t2.a = t3.a and t2.a = o.a and t1.a = 1)",
			best: "Table(t)->Apply(LeftHashJoin{LeftHashJoin{Table(t)->Table(t)}->Table(t)->Selection}->Projection)->Selection->Projection",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

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
		info, err := lp.convert2PhysicalPlan(&requiredProperty{})
		c.Assert(err, IsNil)
		c.Assert(ToString(info.p), Equals, ca.best, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestLogicalPlan(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select sum(t.a), sum(t.a+1), sum(t.a), count(t.a), sum(t.a) + count(t.a) from t",
			best: "DataScan(t)->Aggr(sum(test.t.a),sum(plus(test.t.a, 1)),count(test.t.a))->Projection",
		},
		{
			sql:  "select sum(t.a + t.b), sum(t.a + t.c), sum(t.a + t.b), count(t.a) from t having sum(t.a + t.b) > 0 order by sum(t.a + t.c)",
			best: "DataScan(t)->Aggr(sum(plus(test.t.a, test.t.b)),sum(plus(test.t.a, test.t.c)),count(test.t.a))->Selection->Projection->Sort->Trim",
		},
		{
			sql:  "select sum(a.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(t)->Aggr(sum(a.a),firstrow(a.c))->DataScan(t)}->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(t)->DataScan(t)->Aggr(sum(b.a),firstrow(b.c))}->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(b.a), a.a from t a, t b where a.c = b.c",
			best: "Join{DataScan(t)->DataScan(t)->Aggr(sum(b.a),firstrow(b.c))}->Aggr(sum(join_agg_0),firstrow(a.a))->Projection",
		},
		{
			sql:  "select sum(a.a), b.a from t a, t b where a.c = b.c",
			best: "Join{DataScan(t)->Aggr(sum(a.a),firstrow(a.c))->DataScan(t)}->Aggr(sum(join_agg_0),firstrow(b.a))->Projection",
		},
		{
			sql:  "select sum(a.a), sum(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(t)->DataScan(t)}->Aggr(sum(a.a),sum(b.a))->Projection",
		},
		{
			sql:  "select sum(a.a), max(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(t)->Aggr(sum(a.a),firstrow(a.c))->DataScan(t)}->Aggr(sum(join_agg_0),max(b.a))->Projection",
		},
		{
			sql:  "select max(a.a), sum(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(t)->DataScan(t)->Aggr(sum(b.a),firstrow(b.c))}->Aggr(max(a.a),sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(a.a) from t a, t b, t c where a.c = b.c and b.c = c.c",
			best: "Join{Join{DataScan(t)->Aggr(sum(a.a),firstrow(a.c))->DataScan(t)}->Aggr(sum(join_agg_0),firstrow(b.c))->DataScan(t)}->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(b.a) from t a left join t b on a.c = b.c",
			best: "Join{DataScan(t)->DataScan(t)->Aggr(sum(b.a),firstrow(b.c))}->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(a.a) from t a left join t b on a.c = b.c",
			best: "Join{DataScan(t)->Aggr(sum(a.a),firstrow(a.c))->DataScan(t)}->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(a.a) from t a right join t b on a.c = b.c",
			best: "Join{DataScan(t)->Aggr(sum(a.a),firstrow(a.c))->DataScan(t)}->Aggr(sum(join_agg_0))->Projection",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

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
		solver := &aggPushDownSolver{
			ctx:   builder.ctx,
			alloc: builder.allocator,
		}
		solver.aggPushDown(lp)
		_, err = lp.PruneColumnsAndResolveIndices(lp.GetSchema())
		c.Assert(err, IsNil)
		c.Assert(ToString(lp), Equals, ca.best, Commentf("for %s", ca.sql))
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
			best: "Index(t.c_d_e)[[1,1] [2,2] [3,3] [4,4] [5,5]]->Projection",
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
			sql:  "select a from t where not a",
			best: "Table(t)->Selection->Projection",
		},
		{
			sql:  "select a from t where c in (1)",
			best: "Index(t.c_d_e)[[1,1]]->Projection",
		},
		{
			sql:  "select a from t where c in (1) and d > 3",
			best: "Index(t.c_d_e)[(1 3,1 +inf]]->Projection",
		},
		{
			sql:  "select a from t where c in (1, 2, 3) and (d > 3 and d < 4 or d > 5 and d < 6)",
			best: "Index(t.c_d_e)[(1 3,1 4) (1 5,1 6) (2 3,2 4) (2 5,2 6) (3 3,3 4) (3 5,3 6)]->Projection",
		},
		{
			sql:  "select a from t where c in (1, 2, 3)",
			best: "Index(t.c_d_e)[[1,1] [2,2] [3,3]]->Projection",
		},
		{
			sql:  "select a from t where c in (1, 2, 3) and d in (1,2) and e = 1",
			best: "Index(t.c_d_e)[[1 1 1,1 1 1] [1 2 1,1 2 1] [2 1 1,2 1 1] [2 2 1,2 2 1] [3 1 1,3 1 1] [3 2 1,3 2 1]]->Projection",
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
			sql:  "select a from t where c_str like ''",
			best: "Index(t.c_d_e_str)[[,]]->Projection",
		},
		{
			sql:  "select a from t where c_str like 'abc'",
			best: "Index(t.c_d_e_str)[[abc,abc]]->Projection",
		},
		{
			sql:  "select a from t where c_str not like 'abc'",
			best: "Table(t)->Selection->Projection",
		},
		{
			sql:  "select a from t where not (c_str like 'abc' or c_str like 'abd')",
			best: "Table(t)->Selection->Projection",
		},
		{
			sql:  "select a from t where c_str like '_abc'",
			best: "Table(t)->Selection->Projection",
		},
		{
			sql:  "select a from t where c_str like 'abc%'",
			best: "Index(t.c_d_e_str)[[abc,abd)]->Projection",
		},
		{
			sql:  "select a from t where c_str like 'abc_'",
			best: "Index(t.c_d_e_str)[(abc,abd)]->Selection->Projection",
		},
		{
			sql:  "select a from t where c_str like 'abc%af'",
			best: "Index(t.c_d_e_str)[[abc,abd)]->Selection->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc\\_' escape ''`,
			best: "Index(t.c_d_e_str)[[abc_,abc_]]->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc\\_'`,
			best: "Index(t.c_d_e_str)[[abc_,abc_]]->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc\\\\_'`,
			best: "Index(t.c_d_e_str)[(abc\\,abc])]->Selection->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc\\_%'`,
			best: "Index(t.c_d_e_str)[[abc_,abc`)]->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc=_%' escape '='`,
			best: "Index(t.c_d_e_str)[[abc_,abc`)]->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc\\__'`,
			best: "Index(t.c_d_e_str)[(abc_,abc`)]->Selection->Projection",
		},
		{
			// Check that 123 is converted to string '123'. index can be used.
			sql:  `select a from t where c_str like 123`,
			best: "Index(t.c_d_e_str)[[123,123]]->Projection",
		},
		{
			// c is not string type, added cast to string during InferType, no index can be used.
			sql:  `select a from t where c like '1'`,
			best: "Table(t)->Selection->Projection",
		},
		{
			sql:  `select a from t where c = 1.1 and d > 3`,
			best: "Index(t.c_d_e)[]->Projection",
		},
		{
			sql:  `select a from t where c = 1.9 and d > 3`,
			best: "Index(t.c_d_e)[]->Projection",
		},
		{
			sql:  `select a from t where c < 1.1`,
			best: "Index(t.c_d_e)[[-inf,1]]->Projection",
		},
		{
			sql:  `select a from t where c <= 1.9`,
			best: "Index(t.c_d_e)[[-inf,2)]->Projection",
		},
		{
			sql:  `select a from t where c >= 1.1`,
			best: "Index(t.c_d_e)[(1,+inf]]->Projection",
		},
		{
			sql:  `select a from t where c > 1.9`,
			best: "Index(t.c_d_e)[[2,+inf]]->Projection",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

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
		info, err := p.convert2PhysicalPlan(&requiredProperty{})
		c.Assert(err, IsNil)
		jsonPlan, _ := info.p.MarshalJSON()
		c.Assert(ToString(info.p), Equals, ca.best, Commentf("for %s, %s", ca.sql, string(jsonPlan)))
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
		checkDataSourceCols(p, c, ca.ans, comment)
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
		sql := "select * from t where " + ca.exprStr
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, ca.exprStr))
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
			result = rb.intersection(result, rb.build(cond))
		}
		c.Assert(rb.err, IsNil)
		got := fmt.Sprintf("%v", result)
		c.Assert(got, Equals, ca.resultStr, Commentf("different for expr %s", ca.exprStr))
	}
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
			resultStr: "in(test.t.a, C, 0, 9)",
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

func checkDataSourceCols(p Plan, c *C, ans map[string][]string, comment CommentInterface) {
	switch p.(type) {
	case *PhysicalTableScan:
		colList, ok := ans[p.GetID()]
		c.Assert(ok, IsTrue, comment)
		for i, colName := range colList {
			c.Assert(colName, Equals, p.GetSchema()[i].ColName.L, comment)
		}
	}
	for _, child := range p.GetChildren() {
		checkDataSourceCols(child, c, ans, comment)
	}
}

func (s *testPlanSuite) TestConstantPropagation(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql   string
		after string
	}{
		{
			sql:   "a = b and b = c and c = d and d = 1",
			after: "eq(test.t.a, 1), eq(test.t.b, 1), eq(test.t.c, 1), eq(test.t.d, 1)",
		},
		{
			sql:   "a = b and b = 1 and a = null and c = d and c > 2 and c != 4 and d != 5",
			after: "<nil>, eq(test.t.a, 1), eq(test.t.b, 1), eq(test.t.c, test.t.d), gt(test.t.c, 2), gt(test.t.d, 2), ne(test.t.c, 4), ne(test.t.c, 5), ne(test.t.d, 4), ne(test.t.d, 5)",
		},
		{
			sql:   "a = b and b > 0 and a = c",
			after: "eq(test.t.a, test.t.b), eq(test.t.a, test.t.c), gt(test.t.a, 0), gt(test.t.b, 0), gt(test.t.c, 0)",
		},
		{
			sql:   "a = b and b = c and c LIKE 'abc%'",
			after: "eq(test.t.a, test.t.b), eq(test.t.b, test.t.c), like(cast(test.t.c), abc%, 92)",
		},
		{
			sql:   "a = b and a > 2 and b > 3 and a < 1 and b < 2",
			after: "eq(test.t.a, test.t.b), gt(test.t.a, 2), gt(test.t.a, 3), gt(test.t.b, 2), gt(test.t.b, 3), lt(test.t.a, 1), lt(test.t.a, 2), lt(test.t.b, 1), lt(test.t.b, 2)",
		},
		{
			sql:   "a = null and cast(null as SIGNED) is null",
			after: "eq(test.t.a, <nil>), isnull(cast(<nil>))",
		},
	}
	for _, ca := range cases {
		sql := "select * from t where " + ca.sql
		comment := Commentf("for %s", sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)
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
		var (
			sel    *Selection
			ok     bool
			result []string
		)
		v := lp
		for {
			if sel, ok = v.(*Selection); ok {
				break
			}
			v = v.GetChildByIndex(0).(LogicalPlan)
		}
		for _, v := range sel.Conditions {
			result = append(result, v.String())
		}
		sort.Strings(result)
		c.Assert(strings.Join(result, ", "), Equals, ca.after, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestValidate(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql string
		err *terror.Error
	}{
		{
			sql: "select date_format((1,2), '%H');",
			err: ErrOneColumn,
		},
		{
			sql: "select cast((1,2) as date)",
			err: ErrOneColumn,
		},
		{
			sql: "select (1,2) between (3,4) and (5,6)",
			err: ErrOneColumn,
		},
		{
			sql: "select (1,2) rlike '1'",
			err: ErrOneColumn,
		},
		{
			sql: "select (1,2) like '1'",
			err: ErrOneColumn,
		},
		{
			sql: "select case(1,2) when(1,2) then true end",
			err: ErrOneColumn,
		},
		{
			sql: "select (1,2) in ((3,4),(5,6))",
			err: nil,
		},
		{
			sql: "select (1,2) in ((3,4),5)",
			err: ErrSameColumns,
		},
		{
			sql: "select (1,2) is true",
			err: ErrOneColumn,
		},
		{
			sql: "select (1,2) is null",
			err: ErrOneColumn,
		},
		{
			sql: "select (+(1,2))=(1,2)",
			err: nil,
		},
		{
			sql: "select (-(1,2))=(1,2)",
			err: ErrOneColumn,
		},
		{
			sql: "select (1,2)||(1,2)",
			err: ErrOneColumn,
		},
		{
			sql: "select (1,2) < (3,4)",
			err: nil,
		},
		{
			sql: "select (1,2) < 3",
			err: ErrSameColumns,
		},
		{
			sql: "select 1, * from t",
			err: ErrInvalidWildCard,
		},
		{
			sql: "select *, 1 from t",
			err: nil,
		},
		{
			sql: "select 1, t.* from t",
			err: nil,
		},
	}
	for _, ca := range cases {
		sql := ca.sql
		comment := Commentf("for %s", sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)
		err = mockResolve(stmt)
		c.Assert(err, IsNil)
		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mock.NewContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
		}
		builder.build(stmt)
		if ca.err == nil {
			c.Assert(builder.err, IsNil, comment)
		} else {
			c.Assert(ca.err.Equal(builder.err), IsTrue, comment)
		}
	}
}
