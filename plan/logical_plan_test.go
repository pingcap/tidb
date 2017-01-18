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
	"github.com/pingcap/tidb/context"
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

func mockResolve(node ast.Node) (infoschema.InfoSchema, error) {
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
			State:  model.StatePublic,
			Unique: true,
		},
		{
			Name: model.NewCIStr("e"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("e"),
					Length: types.UnspecifiedLength,
				},
			},
			State:  model.StateWriteOnly,
			Unique: true,
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
			State:  model.StatePublic,
			Unique: true,
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
			State:  model.StatePublic,
			Unique: true,
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
			State:  model.StatePublic,
			Unique: true,
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
	// Column 'b', 'c', 'd', 'f', 'g' is not null.
	col0.Flag = mysql.NotNullFlag
	col1.Flag = mysql.NotNullFlag
	col2.Flag = mysql.NotNullFlag
	col4.Flag = mysql.NotNullFlag
	col5.Flag = mysql.NotNullFlag
	table := &model.TableInfo{
		Columns:    []*model.ColumnInfo{pkColumn, col0, col1, col2, col3, colStr1, colStr2, colStr3, col4, col5},
		Indices:    indices,
		Name:       model.NewCIStr("t"),
		PKIsHandle: true,
	}
	is := infoschema.MockInfoSchema([]*model.TableInfo{table})
	ctx := mockContext()
	err := MockResolveName(node, is, "test", ctx)
	if err != nil {
		return nil, err
	}
	return is, InferType(ctx.GetSessionVars().StmtCtx, node)
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
	case tipb.ExprType_Case, tipb.ExprType_If, tipb.ExprType_IfNull, tipb.ExprType_NullIf:
		return true
	// other functions
	case tipb.ExprType_Coalesce, tipb.ExprType_IsNull:
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

func (s *testPlanSuite) TestPredicatePushDown(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql   string
		first string
		best  string
	}{
		{
			sql:   "select count(*) from t a, t b where a.a = b.a",
			first: "Join{DataScan(a)->DataScan(b)}->Selection->Aggr(count(1))->Projection",
			best:  "Join{DataScan(a)->DataScan(b)}(a.a,b.a)->Aggr(count(1))->Projection",
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
			first: "Join{DataScan(ta)->DataScan(tb)}->Selection->Projection",
			best:  "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.b)(ta.a,tb.c)->Projection",
		},
		{
			sql:   "select * from t t1, t t2 where t1.a = t2.b and t2.b > 0 and t1.a = t1.c and t1.d like 'abc' and t2.d = t1.d",
			first: "Join{DataScan(t1)->DataScan(t2)}->Selection->Projection",
			best:  "Join{DataScan(t2)->Selection->DataScan(t1)->Selection}(t2.b,t1.a)(t2.d,t1.d)->Projection",
		},
		{
			sql:   "select * from t ta join t tb on ta.d = tb.d and ta.d > 1 where tb.a = 0",
			first: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Selection->Projection",
			best:  "Join{DataScan(ta)->Selection->DataScan(tb)->Selection}(ta.d,tb.d)->Projection",
		},
		{
			sql:   "select * from t ta join t tb on ta.d = tb.d where ta.d > 1 and tb.a = 0",
			first: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Selection->Projection",
			best:  "Join{DataScan(ta)->Selection->DataScan(tb)->Selection}(ta.d,tb.d)->Projection",
		},
		{
			sql:   "select * from t ta left outer join t tb on ta.d = tb.d and ta.d > 1 where tb.a = 0",
			first: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Selection->Projection",
			best:  "Join{DataScan(ta)->Selection->DataScan(tb)->Selection}(ta.d,tb.d)->Projection",
		},
		{
			sql:   "select * from t ta right outer join t tb on ta.d = tb.d and ta.a > 1 where tb.a = 0",
			first: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Selection->Projection",
			best:  "Join{DataScan(ta)->Selection->DataScan(tb)->Selection}(ta.d,tb.d)->Projection",
		},
		{
			sql:   "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where ta.d = 0",
			first: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Selection->Projection",
			best:  "Join{DataScan(ta)->Selection->DataScan(tb)}(ta.d,tb.d)->Projection",
		},
		{
			sql:   "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where tb.d = 0",
			first: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Selection->Projection",
			best:  "Join{DataScan(ta)->Selection->DataScan(tb)->Selection}->Projection",
		},
		{
			sql:   "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where tb.c is not null and tb.c = 0 and ifnull(tb.d, 1)",
			first: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Selection->Projection",
			best:  "Join{DataScan(ta)->Selection->DataScan(tb)->Selection}(ta.d,tb.d)->Projection",
		},
		{
			sql:   "select * from t ta left outer join t tb on ta.a = tb.a left outer join t tc on tb.b = tc.b where tc.c > 0",
			first: "Join{Join{DataScan(ta)->DataScan(tb)}(ta.a,tb.a)->DataScan(tc)}(tb.b,tc.b)->Selection->Projection",
			best:  "Join{Join{DataScan(ta)->DataScan(tb)}(ta.a,tb.a)->DataScan(tc)->Selection}(tb.b,tc.b)->Projection",
		},
		{
			sql:   "select * from t ta left outer join t tb on ta.a = tb.a left outer join t tc on tc.b = ta.b where tb.c > 0",
			first: "Join{Join{DataScan(ta)->DataScan(tb)}(ta.a,tb.a)->DataScan(tc)}(ta.b,tc.b)->Selection->Projection",
			best:  "Join{Join{DataScan(ta)->DataScan(tb)->Selection}(ta.a,tb.a)->DataScan(tc)}(ta.b,tc.b)->Projection",
		},
		{
			sql:   "select * from t as ta left outer join (t as tb left join t as tc on tc.b = tb.b) on tb.a = ta.a where tc.c > 0",
			first: "Join{DataScan(ta)->Join{DataScan(tb)->DataScan(tc)}(tb.b,tc.b)}(ta.a,tb.a)->Selection->Projection",
			best:  "Join{DataScan(ta)->Join{DataScan(tb)->DataScan(tc)->Selection}(tb.b,tc.b)}(ta.a,tb.a)->Projection",
		},
		{
			sql:   "select * from ( t as ta left outer join t as tb on ta.a = tb.a) join ( t as tc left join t as td on tc.b = td.b) on ta.c = td.c where tb.c = 2 and td.a = 1",
			first: "Join{Join{DataScan(ta)->DataScan(tb)}(ta.a,tb.a)->Join{DataScan(tc)->DataScan(td)}(tc.b,td.b)}(ta.c,td.c)->Selection->Projection",
			best:  "Join{Join{DataScan(ta)->DataScan(tb)->Selection}(ta.a,tb.a)->Join{DataScan(tc)->DataScan(td)->Selection}(tc.b,td.b)}(ta.c,td.c)->Projection",
		},
		{
			sql:   "select * from t ta left outer join (t tb left outer join t tc on tc.b = tb.b) on tb.a = ta.a and tc.c = ta.c where tc.d > 0 or ta.d > 0",
			first: "Join{DataScan(ta)->Join{DataScan(tb)->DataScan(tc)}(tb.b,tc.b)}(ta.a,tb.a)(ta.c,tc.c)->Selection->Projection",
			best:  "Join{DataScan(ta)->Join{DataScan(tb)->DataScan(tc)}(tb.b,tc.b)}(ta.a,tb.a)(ta.c,tc.c)->Selection->Projection",
		},
		{
			sql:   "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where ifnull(tb.d, null) or tb.d is null",
			first: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Selection->Projection",
			best:  "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Selection->Projection",
		},
		{
			sql:   "select a, d from (select * from t union all select * from t union all select * from t) z where a < 10",
			first: "UnionAll{DataScan(t)->Projection->DataScan(t)->Projection->DataScan(t)->Projection}->Selection->Projection",
			best:  "UnionAll{DataScan(t)->Selection->Projection->DataScan(t)->Selection->Projection->DataScan(t)->Selection->Projection}->Projection",
		},
		{
			sql:   "select (select count(*) from t where t.a = k.a) from t k",
			first: "Apply{DataScan(k)->DataScan(t)->Selection->Aggr(count(1))->Projection->MaxOneRow}->Projection",
			best:  "Apply{DataScan(k)->DataScan(t)->Selection->Aggr(count(1))->Projection->MaxOneRow}->Projection",
		},
		{
			sql:   "select a from t where exists(select 1 from t as x where x.a < t.a)",
			first: "Join{DataScan(t)->DataScan(x)}->Projection",
			best:  "Join{DataScan(t)->DataScan(x)}->Projection",
		},
		{
			sql:   "select a from t where exists(select 1 from t as x where x.a = t.a and t.a < 1 and x.a < 1)",
			first: "Join{DataScan(t)->DataScan(x)}(test.t.a,x.a)->Projection",
			best:  "Join{DataScan(t)->Selection->DataScan(x)->Selection}(test.t.a,x.a)->Projection",
		},
		{
			sql:   "select a from t where exists(select 1 from t as x where x.a = t.a and x.a < 1) and a < 1",
			first: "Join{DataScan(t)->DataScan(x)}(test.t.a,x.a)->Selection->Projection",
			best:  "Join{DataScan(t)->Selection->DataScan(x)->Selection}(test.t.a,x.a)->Projection",
		},
		{
			sql:   "select a from t where exists(select 1 from t as x where x.a = t.a) and exists(select 1 from t as x where x.a = t.a)",
			first: "Join{Join{DataScan(t)->DataScan(x)}(test.t.a,x.a)->DataScan(x)}(test.t.a,x.a)->Projection",
			best:  "Join{Join{DataScan(t)->DataScan(x)}(test.t.a,x.a)->DataScan(x)}(test.t.a,x.a)->Projection",
		},
		{
			sql:   "select * from (select a, b, sum(c) as s from t group by a, b) k where k.a > k.b * 2 + 1",
			first: "DataScan(t)->Aggr(sum(test.t.c),firstrow(test.t.a),firstrow(test.t.b))->Projection->Selection->Projection",
			best:  "DataScan(t)->Selection->Aggr(sum(test.t.c),firstrow(test.t.a),firstrow(test.t.b))->Projection->Projection",
		},
		{
			sql:   "select * from (select a, b, sum(c) as s from t group by a, b) k where k.a > 1 and k.b > 2",
			first: "DataScan(t)->Aggr(sum(test.t.c),firstrow(test.t.a),firstrow(test.t.b))->Projection->Selection->Projection",
			best:  "DataScan(t)->Selection->Aggr(sum(test.t.c),firstrow(test.t.a),firstrow(test.t.b))->Projection->Projection",
		},
		{
			sql:   "select * from (select k.a, sum(k.s) as ss from (select a, sum(b) as s from t group by a) k group by k.a) l where l.a > 2",
			first: "DataScan(t)->Aggr(sum(test.t.b),firstrow(test.t.a))->Projection->Aggr(sum(k.s),firstrow(k.a))->Projection->Selection->Projection",
			best:  "DataScan(t)->Selection->Aggr(sum(test.t.b),firstrow(test.t.a))->Projection->Aggr(sum(k.s),firstrow(k.a))->Projection->Projection",
		},
		{
			sql:   "select * from (select a, sum(b) as s from t group by a) k where a > s",
			first: "DataScan(t)->Aggr(sum(test.t.b),firstrow(test.t.a))->Projection->Selection->Projection",
			best:  "DataScan(t)->Aggr(sum(test.t.b),firstrow(test.t.a))->Selection->Projection->Projection",
		},
		{
			sql:   "select * from (select a, sum(b) as s from t group by a + 1) k where a > 1",
			first: "DataScan(t)->Aggr(sum(test.t.b),firstrow(test.t.a))->Projection->Selection->Projection",
			best:  "DataScan(t)->Aggr(sum(test.t.b),firstrow(test.t.a))->Selection->Projection->Projection",
		},
		{
			sql:   "select * from (select a, sum(b) as s from t group by a having 1 = 0) k where a > 1",
			first: "DataScan(t)->Aggr(sum(test.t.b),firstrow(test.t.a))->Projection->Selection->Selection->Projection",
			best:  "DataScan(t)->Selection->Aggr(sum(test.t.b),firstrow(test.t.a))->Selection->Projection->Projection",
		},
		{
			sql:   "select a, count(a) cnt from t group by a having cnt < 1",
			first: "DataScan(t)->Aggr(count(test.t.a),firstrow(test.t.a))->Projection->Selection",
			best:  "DataScan(t)->Aggr(count(test.t.a),firstrow(test.t.a))->Selection->Projection",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := mockResolve(stmt)
		c.Assert(err, IsNil, comment)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			is:        is,
			colMapper: make(map[*ast.ColumnNameExpr]int),
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil, comment)
		lp := p.(LogicalPlan)
		lp.PruneColumns(lp.GetSchema().Columns)
		c.Assert(ToString(lp), Equals, ca.first, Commentf("for %s", ca.sql))
		_, lp, err = lp.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		lp.ResolveIndicesAndCorCols()
		c.Assert(err, IsNil)
		c.Assert(ToString(lp), Equals, ca.best, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestPlanBuilder(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql  string
		plan string
	}{
		{
			// This will be resolved as in sub query.
			sql:  "select * from t where 10 in (select b from t s where s.a = t.a)",
			plan: "Apply{DataScan(t)->DataScan(s)->Selection->Projection}->Selection->Projection",
		},
		{
			sql:  "select count(c) ,(select b from t s where s.a = t.a) from t",
			plan: "Apply{DataScan(t)->Aggr(count(test.t.c),firstrow(test.t.a))->DataScan(s)->Selection->Projection->MaxOneRow}->Projection",
		},
		{
			sql:  "select count(c) ,(select count(s.b) from t s where s.a = t.a) from t",
			plan: "Apply{DataScan(t)->Aggr(count(test.t.c),firstrow(test.t.a))->DataScan(s)->Selection->Aggr(count(s.b))->Projection->MaxOneRow}->Projection",
		},
		{
			// This will be resolved as in sub query.
			sql:  "select * from t where 10 in (((select b from t s where s.a = t.a)))",
			plan: "Apply{DataScan(t)->DataScan(s)->Selection->Projection}->Selection->Projection",
		},
		{
			// This will be resolved as in function.
			sql:  "select * from t where 10 in (((select b from t s where s.a = t.a)), 10)",
			plan: "Apply{DataScan(t)->DataScan(s)->Selection->Projection->MaxOneRow}->Selection->Projection",
		},
		{
			sql:  "select * from t where exists (select s.a from t s having sum(s.a) = t.a )",
			plan: "Join{DataScan(t)->DataScan(s)->Aggr(sum(s.a))->Projection}(test.t.a,sel_agg_1)->Projection",
		},
		{
			sql:  "select * from t for update",
			plan: "DataScan(t)->Lock->Projection",
		},
		{
			sql:  "update t set t.a = t.a * 1.5 where t.a >= 1000 order by t.a desc limit 10",
			plan: "DataScan(t)->Selection->Sort->Limit->*plan.Update",
		},
		{
			sql:  "delete from t where t.a >= 1000 order by t.a desc limit 10",
			plan: "DataScan(t)->Selection->Sort->Limit->*plan.Delete",
		},
		{
			sql:  "explain select * from t union all select * from t limit 1, 1",
			plan: "UnionAll{Table(t)->Table(t)->Limit}->*plan.Explain",
		},
		{
			sql:  "insert into t select * from t",
			plan: "DataScan(t)->Projection->*plan.Insert",
		},
		{
			sql:  "show columns from t where `Key` = 'pri' like 't*'",
			plan: "*plan.Show->Selection",
		},
		{
			sql:  "do sleep(5)",
			plan: "*plan.TableDual->Projection",
		},
		{
			sql:  "select substr(\"abc\", 1)",
			plan: "*plan.TableDual->Projection",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		p := builder.build(stmt)
		if lp, ok := p.(LogicalPlan); ok {
			lp.PruneColumns(p.GetSchema().Columns)
		}
		c.Assert(builder.err, IsNil)
		c.Assert(ToString(p), Equals, ca.plan, Commentf("for %s", ca.sql))
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
			best: "Join{Join{Join{Join{DataScan(t1)->DataScan(t2)}(t1.a,t2.b)->DataScan(t3)}(t2.a,t3.b)->DataScan(t4)}(t3.c,t4.a)(t2.c,t4.d)->Join{DataScan(t5)->DataScan(t6)}(t5.d,t6.d)}->Projection",
		},
		{
			sql:  "select * from t t1, t t2, t t3, t t4, t t5, t t6, t t7, t t8 where t1.a = t8.a",
			best: "Join{Join{Join{Join{DataScan(t1)->DataScan(t8)}(t1.a,t8.a)->DataScan(t2)}->Join{DataScan(t3)->DataScan(t4)}}->Join{Join{DataScan(t5)->DataScan(t6)}->DataScan(t7)}}->Projection",
		},
		{
			sql:  "select * from t t1, t t2, t t3, t t4, t t5 where t1.a = t5.a and t5.a = t4.a and t4.a = t3.a and t3.a = t2.a and t2.a = t1.a and t1.a = t3.a and t2.a = t4.a and t5.b < 8",
			best: "Join{Join{Join{Join{DataScan(t5)->Selection->DataScan(t1)}(t5.a,t1.a)->DataScan(t2)}(t1.a,t2.a)->DataScan(t3)}(t2.a,t3.a)(t1.a,t3.a)->DataScan(t4)}(t5.a,t4.a)(t3.a,t4.a)(t2.a,t4.a)->Projection",
		},
		{
			sql:  "select * from t t1, t t2, t t3, t t4, t t5 where t1.a = t5.a and t5.a = t4.a and t4.a = t3.a and t3.a = t2.a and t2.a = t1.a and t1.a = t3.a and t2.a = t4.a and t3.b = 1 and t4.a = 1",
			best: "Join{Join{Join{Join{DataScan(t3)->Selection->DataScan(t4)->Selection}->DataScan(t5)->Selection}->DataScan(t1)->Selection}->DataScan(t2)->Selection}->Projection",
		},
		{
			sql:  "select * from t o where o.b in (select t3.c from t t1, t t2, t t3 where t1.a = t3.a and t2.a = t3.a and t2.a = o.a)",
			best: "Apply{DataScan(o)->Join{Join{DataScan(t2)->Selection->DataScan(t3)}(t2.a,t3.a)->DataScan(t1)}(t3.a,t1.a)->Projection}->Selection->Projection",
		},
		{
			sql:  "select * from t o where o.b in (select t3.c from t t1, t t2, t t3 where t1.a = t3.a and t2.a = t3.a and t2.a = o.a and t1.a = 1)",
			best: "Apply{DataScan(o)->Join{Join{DataScan(t1)->Selection->DataScan(t3)->Selection}->DataScan(t2)->Selection}->Projection}->Selection->Projection",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)

		_, lp, err = lp.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		lp.PruneColumns(lp.GetSchema().Columns)
		lp.ResolveIndicesAndCorCols()
		c.Assert(err, IsNil)
		c.Assert(ToString(lp), Equals, ca.best, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestAggPushDown(c *C) {
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
			best: "Join{DataScan(a)->Aggr(sum(a.a),firstrow(a.c))->DataScan(b)}(a.c,b.c)->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->DataScan(b)->Aggr(sum(b.a),firstrow(b.c))}(a.c,b.c)->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(b.a), a.a from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->DataScan(b)->Aggr(sum(b.a),firstrow(b.c))}(a.c,b.c)->Aggr(sum(join_agg_0),firstrow(a.a))->Projection",
		},
		{
			sql:  "select sum(a.a), b.a from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->Aggr(sum(a.a),firstrow(a.c))->DataScan(b)}(a.c,b.c)->Aggr(sum(join_agg_0),firstrow(b.a))->Projection",
		},
		{
			sql:  "select sum(a.a), sum(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->DataScan(b)}(a.c,b.c)->Aggr(sum(a.a),sum(b.a))->Projection",
		},
		{
			sql:  "select sum(a.a), max(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->Aggr(sum(a.a),firstrow(a.c))->DataScan(b)}(a.c,b.c)->Aggr(sum(join_agg_0),max(b.a))->Projection",
		},
		{
			sql:  "select max(a.a), sum(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->DataScan(b)->Aggr(sum(b.a),firstrow(b.c))}(a.c,b.c)->Aggr(max(a.a),sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(a.a) from t a, t b, t c where a.c = b.c and b.c = c.c",
			best: "Join{Join{DataScan(a)->Aggr(sum(a.a),firstrow(a.c))->DataScan(b)}(a.c,b.c)->Aggr(sum(join_agg_0),firstrow(b.c))->DataScan(c)}(b.c,c.c)->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(b.a) from t a left join t b on a.c = b.c",
			best: "Join{DataScan(a)->DataScan(b)->Aggr(sum(b.a),firstrow(b.c))}(a.c,b.c)->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(a.a) from t a left join t b on a.c = b.c",
			best: "Join{DataScan(a)->Aggr(sum(a.a),firstrow(a.c))->DataScan(b)}(a.c,b.c)->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(a.a) from t a right join t b on a.c = b.c",
			best: "Join{DataScan(a)->Aggr(sum(a.a),firstrow(a.c))->DataScan(b)}(a.c,b.c)->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(a) from (select * from t) x",
			best: "DataScan(t)->Aggr(sum(test.t.a))->Projection",
		},
		{
			sql:  "select sum(c1) from (select c c1, d c2 from t a union all select a c1, b c2 from t b union all select b c1, e c2 from t c) x group by c2",
			best: "UnionAll{DataScan(a)->Aggr(sum(a.c),firstrow(a.d))->DataScan(b)->Aggr(sum(b.a),firstrow(b.b))->DataScan(c)->Aggr(sum(c.b),firstrow(c.e))}->Aggr(sum(join_agg_0))->Projection",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
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
		lp.PruneColumns(lp.GetSchema().Columns)
		lp.ResolveIndicesAndCorCols()
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
			best: "Table(t)->Projection",
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
			best: "Index(t.c_d_e)[[4,4]]->Projection",
		},
		{
			sql:  "select a from t where c = 4 and d <= 5 and d > 3",
			best: "Index(t.c_d_e)[(4 3,4 5]]->Projection",
		},
		{
			sql:  "select a from t where d <= 5 and d > 3",
			best: "Table(t)->Projection",
		},
		{
			sql:  "select a from t where c between 1 and 2",
			best: "Index(t.c_d_e)[[1,2]]->Projection",
		},
		{
			sql:  "select a from t where c not between 1 and 2",
			best: "Index(t.c_d_e)[[-inf,1) (2,+inf]]->Projection",
		},
		{
			sql:  "select a from t where c <= 5 and c >= 3 and d = 1",
			best: "Index(t.c_d_e)[[3,5]]->Projection",
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
			best: "Index(t.c_d_e)[[5,5]]->Projection",
		},
		{
			sql:  "select a from t where not a",
			best: "Table(t)->Projection",
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
			best: "Table(t)->Projection",
		},
		{
			sql:  "select a from t where c not in (1)",
			best: "Table(t)->Projection",
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
			best: "Table(t)->Projection",
		},
		{
			sql:  "select a from t where not (c_str like 'abc' or c_str like 'abd')",
			best: "Table(t)->Projection",
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

		is, err := mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			is:        is,
		}
		p := builder.build(stmt).(LogicalPlan)
		c.Assert(builder.err, IsNil)

		_, p, err = p.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		p.PruneColumns(p.GetSchema().Columns)
		p.ResolveIndicesAndCorCols()
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

		is, err := mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			colMapper: make(map[*ast.ColumnNameExpr]int),
			allocator: new(idAllocator),
			ctx:       mockContext(),
			is:        is,
		}
		p := builder.build(stmt).(LogicalPlan)
		c.Assert(builder.err, IsNil, comment)

		_, p, err = p.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		p.PruneColumns(p.GetSchema().Columns)
		p.ResolveIndicesAndCorCols()
		c.Assert(err, IsNil)
		checkDataSourceCols(p, c, ca.ans, comment)
	}
}

func (s *testPlanSuite) TestAllocID(c *C) {
	pA := &DataSource{baseLogicalPlan: newBaseLogicalPlan(Tbl, new(idAllocator))}
	pB := &DataSource{baseLogicalPlan: newBaseLogicalPlan(Tbl, new(idAllocator))}
	ctx := mockContext()
	pA.initIDAndContext(ctx)
	pB.initIDAndContext(ctx)
	c.Assert(pA.id, Equals, pB.id)
}

func (s *testPlanSuite) TestRangeBuilder(c *C) {
	defer testleak.AfterTest(c)()
	rb := &rangeBuilder{sc: new(variable.StatementContext)}

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
		is, err := mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			is:        is,
		}
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
			result = rb.intersection(result, rb.build(pushDownNot(cond, false)))
		}
		c.Assert(rb.err, IsNil)
		got := fmt.Sprintf("%v", result)
		c.Assert(got, Equals, ca.resultStr, Commentf("different for expr %s", ca.exprStr))
	}
}

func checkDataSourceCols(p Plan, c *C, ans map[string][]string, comment CommentInterface) {
	switch p.(type) {
	case *PhysicalTableScan:
		colList, ok := ans[p.GetID()]
		c.Assert(ok, IsTrue, comment)
		for i, colName := range colList {
			c.Assert(colName, Equals, p.GetSchema().Columns[i].ColName.L, comment)
		}
	}
	for _, child := range p.GetChildren() {
		checkDataSourceCols(child, c, ans, comment)
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
			err: ErrOperandColumns,
		},
		{
			sql: "select cast((1,2) as date)",
			err: ErrOperandColumns,
		},
		{
			sql: "select (1,2) between (3,4) and (5,6)",
			err: ErrOperandColumns,
		},
		{
			sql: "select (1,2) rlike '1'",
			err: ErrOperandColumns,
		},
		{
			sql: "select (1,2) like '1'",
			err: ErrOperandColumns,
		},
		{
			sql: "select case(1,2) when(1,2) then true end",
			err: ErrOperandColumns,
		},
		{
			sql: "select (1,2) in ((3,4),(5,6))",
			err: nil,
		},
		{
			sql: "select row(1,(2,3)) in (select a,b from t)",
			err: ErrOperandColumns,
		},
		{
			sql: "select row(1,2) in (select a,b from t)",
			err: nil,
		},
		{
			sql: "select (1,2) in ((3,4),5)",
			err: ErrOperandColumns,
		},
		{
			sql: "select (1,2) is true",
			err: ErrOperandColumns,
		},
		{
			sql: "select (1,2) is null",
			err: ErrOperandColumns,
		},
		{
			sql: "select (+(1,2))=(1,2)",
			err: nil,
		},
		{
			sql: "select (-(1,2))=(1,2)",
			err: ErrOperandColumns,
		},
		{
			sql: "select (1,2)||(1,2)",
			err: ErrOperandColumns,
		},
		{
			sql: "select (1,2) < (3,4)",
			err: nil,
		},
		{
			sql: "select (1,2) < 3",
			err: ErrOperandColumns,
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
		is, err := mockResolve(stmt)
		c.Assert(err, IsNil)
		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		builder.build(stmt)
		if ca.err == nil {
			c.Assert(builder.err, IsNil, comment)
		} else {
			c.Assert(ca.err.Equal(builder.err), IsTrue, comment)
		}
	}
}

func checkUniqueKeys(p Plan, c *C, ans map[string][][]string, comment CommentInterface) {
	keyList, ok := ans[p.GetID()]
	c.Assert(ok, IsTrue, comment)
	c.Assert(len(keyList), Equals, len(p.GetSchema().Keys), comment)
	for i, key := range keyList {
		c.Assert(len(key), Equals, len(p.GetSchema().Keys[i]), comment)
		for j, colName := range key {
			c.Assert(colName, Equals, p.GetSchema().Keys[i][j].String(), comment)
		}
	}
	for _, child := range p.GetChildren() {
		checkUniqueKeys(child, c, ans, comment)
	}
}

func (s *testPlanSuite) TestUniqueKeyInfo(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql string
		ans map[string][][]string
	}{
		{
			sql: "select a, sum(e) from t group by a",
			ans: map[string][][]string{
				"TableScan_1":   {{"test.t.a"}},
				"Aggregation_2": {{"test.t.a"}, {"test.t.a"}},
				"Projection_3":  {{"a"}, {"a"}},
			},
		},
		{
			sql: "select a, sum(f) from t group by a",
			ans: map[string][][]string{
				"TableScan_1":   {{"test.t.f"}, {"test.t.a"}},
				"Aggregation_2": {{"test.t.a"}, {"test.t.a"}},
				"Projection_3":  {{"a"}, {"a"}},
			},
		},
		{
			sql: "select c, d, e, sum(a) from t group by c, d, e",
			ans: map[string][][]string{
				"TableScan_1":   {{"test.t.a"}},
				"Aggregation_2": nil,
				"Projection_3":  nil,
			},
		},
		{
			sql: "select f, g, sum(a) from t group by f, g",
			ans: map[string][][]string{
				"TableScan_1":   {{"test.t.f"}, {"test.t.g"}, {"test.t.f", "test.t.g"}, {"test.t.a"}},
				"Aggregation_2": {{"test.t.f"}, {"test.t.g"}, {"test.t.f", "test.t.g"}, {"test.t.f"}},
				"Projection_3":  {{"f"}, {"g"}, {"f", "g"}, {"f"}},
			},
		},
		{
			sql: "select * from t t1 join t t2 on t1.a = t2.e",
			ans: map[string][][]string{
				"TableScan_1":  {{"t1.f"}, {"t1.g"}, {"t1.f", "t1.g"}, {"t1.a"}},
				"TableScan_2":  {{"t2.f"}, {"t2.g"}, {"t2.f", "t2.g"}, {"t2.a"}},
				"Join_3":       {{"t2.f"}, {"t2.g"}, {"t2.f", "t2.g"}, {"t2.a"}},
				"Projection_4": {{"t2.f"}, {"t2.g"}, {"t2.f", "t2.g"}, {"t2.a"}},
			},
		},
		{
			sql: "select f from t having sum(a) > 0",
			ans: map[string][][]string{
				"TableScan_1":   {{"test.t.f"}, {"test.t.a"}},
				"Aggregation_2": {{"test.t.f"}},
				"Selection_6":   {{"test.t.f"}},
				"Projection_3":  {{"f"}},
				"Trim_5":        {{"f"}},
			},
		},
		{
			sql: "select * from t t1 left join t t2 on t1.a = t2.a",
			ans: map[string][][]string{
				"TableScan_1":  {{"t1.f"}, {"t1.g"}, {"t1.f", "t1.g"}, {"t1.a"}},
				"TableScan_2":  {{"t2.f"}, {"t2.g"}, {"t2.f", "t2.g"}, {"t2.a"}},
				"Join_3":       {{"t1.f"}, {"t1.g"}, {"t1.f", "t1.g"}, {"t1.a"}},
				"Projection_4": {{"t1.f"}, {"t1.g"}, {"t1.f", "t1.g"}, {"t1.a"}},
			},
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			colMapper: make(map[*ast.ColumnNameExpr]int),
			allocator: new(idAllocator),
			ctx:       mockContext(),
			is:        is,
		}
		p := builder.build(stmt).(LogicalPlan)
		c.Assert(builder.err, IsNil, comment)

		_, p, err = p.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		p.PruneColumns(p.GetSchema().Columns)
		p.ResolveIndicesAndCorCols()
		p.buildKeyInfo()
		c.Assert(err, IsNil)
		checkUniqueKeys(p, c, ca.ans, comment)
	}
}
