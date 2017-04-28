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
	"sort"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
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

func MockResolve(node ast.Node) (infoschema.InfoSchema, error) {
	indices := []*model.IndexInfo{
		{
			Name: model.NewCIStr("c_d_e"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("c"),
					Length: types.UnspecifiedLength,
					Offset: 2,
				},
				{
					Name:   model.NewCIStr("d"),
					Length: types.UnspecifiedLength,
					Offset: 3,
				},
				{
					Name:   model.NewCIStr("e"),
					Length: types.UnspecifiedLength,
					Offset: 4,
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
					Offset: 4,
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
					Offset: 8,
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
					Offset: 9,
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
					Offset: 8,
				},
				{
					Name:   model.NewCIStr("g"),
					Length: types.UnspecifiedLength,
					Offset: 9,
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
		{
			Name: model.NewCIStr("e_d_c_str_prefix"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("e_str"),
					Length: types.UnspecifiedLength,
					Offset: 7,
				},
				{
					Name:   model.NewCIStr("d_str"),
					Length: types.UnspecifiedLength,
					Offset: 6,
				},
				{
					Name:   model.NewCIStr("c_str"),
					Length: 10,
					Offset: 5,
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

func (c *mockClient) Send(ctx goctx.Context, _ *kv.Request) kv.Response {
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

// BeginWithStartTS begins with startTS.
func (m *mockStore) BeginWithStartTS(startTS uint64) (kv.Transaction, error) {
	return m.Begin()
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
	do := &domain.Domain{}
	do.CreateStatsHandle(ctx)
	sessionctx.BindDomain(ctx, do)
	return ctx
}

func mockStatsTable(tbl *model.TableInfo, rowCount int64) *statistics.Table {
	statsTbl := &statistics.Table{
		TableID: tbl.ID,
		Count:   rowCount,
		Columns: make(map[int64]*statistics.Column, len(tbl.Columns)),
		Indices: make(map[int64]*statistics.Index, len(tbl.Indices)),
	}
	return statsTbl
}

// mockStatsHistogram will create a statistics.Histogram, of which the data is uniform distribution.
func mockStatsHistogram(id int64, values []types.Datum, repeat int64) *statistics.Histogram {
	ndv := len(values)
	histogram := &statistics.Histogram{
		ID:      id,
		NDV:     int64(ndv),
		Buckets: make([]statistics.Bucket, ndv),
	}
	for i := 0; i < ndv; i++ {
		histogram.Buckets[i].Repeats = repeat
		histogram.Buckets[i].Count = repeat * int64(i+1)
		histogram.Buckets[i].Value = values[i]
	}
	return histogram
}

func (s *testPlanSuite) TestPredicatePushDown(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql   string
		first string
		best  string
	}{
		{
			sql:  "select count(*) from t a, t b where a.a = b.a",
			best: "Join{DataScan(a)->DataScan(b)}(a.a,b.a)->Aggr(count(1))->Projection",
		},
		{
			sql:  "select a from (select a from t where d = 0) k where k.a = 5",
			best: "DataScan(t)->Selection->Projection->Projection",
		},
		{
			sql:  "select a from (select 1+2 as a from t where d = 0) k where k.a = 5",
			best: "DataScan(t)->Selection->Projection->Projection",
		},
		{
			sql:  "select a from (select d as a from t where d = 0) k where k.a = 5",
			best: "DataScan(t)->Selection->Projection->Projection",
		},
		{
			sql:  "select * from t ta, t tb where (ta.d, ta.a) = (tb.b, tb.c)",
			best: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.b)(ta.a,tb.c)->Projection",
		},
		{
			sql:  "select * from t t1, t t2 where t1.a = t2.b and t2.b > 0 and t1.a = t1.c and t1.d like 'abc' and t2.d = t1.d",
			best: "Join{DataScan(t2)->Selection->DataScan(t1)->Selection}(t2.b,t1.a)(t2.d,t1.d)->Projection",
		},
		{
			sql:  "select * from t ta join t tb on ta.d = tb.d and ta.d > 1 where tb.a = 0",
			best: "Join{DataScan(ta)->Selection->DataScan(tb)->Selection}(ta.d,tb.d)->Projection",
		},
		{
			sql:  "select * from t ta join t tb on ta.d = tb.d where ta.d > 1 and tb.a = 0",
			best: "Join{DataScan(ta)->Selection->DataScan(tb)->Selection}(ta.d,tb.d)->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.d = tb.d and ta.d > 1 where tb.a = 0",
			best: "Join{DataScan(ta)->Selection->DataScan(tb)->Selection}(ta.d,tb.d)->Projection",
		},
		{
			sql:  "select * from t ta right outer join t tb on ta.d = tb.d and ta.a > 1 where tb.a = 0",
			best: "Join{DataScan(ta)->Selection->DataScan(tb)->Selection}(ta.d,tb.d)->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where ta.d = 0",
			best: "Join{DataScan(ta)->Selection->DataScan(tb)}(ta.d,tb.d)->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where tb.d = 0",
			best: "Join{DataScan(ta)->Selection->DataScan(tb)->Selection}->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where tb.c is not null and tb.c = 0 and ifnull(tb.d, 1)",
			best: "Join{DataScan(ta)->Selection->DataScan(tb)->Selection}(ta.d,tb.d)->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.a = tb.a left outer join t tc on tb.b = tc.b where tc.c > 0",
			best: "Join{Join{DataScan(ta)->DataScan(tb)}(ta.a,tb.a)->DataScan(tc)->Selection}(tb.b,tc.b)->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.a = tb.a left outer join t tc on tc.b = ta.b where tb.c > 0",
			best: "Join{Join{DataScan(ta)->DataScan(tb)->Selection}(ta.a,tb.a)->DataScan(tc)}(ta.b,tc.b)->Projection",
		},
		{
			sql:  "select * from t as ta left outer join (t as tb left join t as tc on tc.b = tb.b) on tb.a = ta.a where tc.c > 0",
			best: "Join{DataScan(ta)->Join{DataScan(tb)->DataScan(tc)->Selection}(tb.b,tc.b)}(ta.a,tb.a)->Projection",
		},
		{
			sql:  "select * from ( t as ta left outer join t as tb on ta.a = tb.a) join ( t as tc left join t as td on tc.b = td.b) on ta.c = td.c where tb.c = 2 and td.a = 1",
			best: "Join{Join{DataScan(ta)->DataScan(tb)->Selection}(ta.a,tb.a)->Join{DataScan(tc)->DataScan(td)->Selection}(tc.b,td.b)}(ta.c,td.c)->Projection",
		},
		{
			sql:  "select * from t ta left outer join (t tb left outer join t tc on tc.b = tb.b) on tb.a = ta.a and tc.c = ta.c where tc.d > 0 or ta.d > 0",
			best: "Join{DataScan(ta)->Join{DataScan(tb)->DataScan(tc)}(tb.b,tc.b)}(ta.a,tb.a)(ta.c,tc.c)->Selection->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where ifnull(tb.d, null) or tb.d is null",
			best: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Selection->Projection",
		},
		{
			sql:  "select a, d from (select * from t union all select * from t union all select * from t) z where a < 10",
			best: "UnionAll{DataScan(t)->Selection->Projection->DataScan(t)->Selection->Projection->DataScan(t)->Selection->Projection}->Projection",
		},
		{
			sql:  "select (select count(*) from t where t.a = k.a) from t k",
			best: "Apply{DataScan(k)->DataScan(t)->Selection->Aggr(count(1))->Projection->MaxOneRow}->Projection",
		},
		{
			sql:  "select a from t where exists(select 1 from t as x where x.a < t.a)",
			best: "Join{DataScan(t)->DataScan(x)}->Projection",
		},
		{
			sql:  "select a from t where exists(select 1 from t as x where x.a = t.a and t.a < 1 and x.a < 1)",
			best: "Join{DataScan(t)->Selection->DataScan(x)->Selection}(test.t.a,x.a)->Projection",
		},
		{
			sql:  "select a from t where exists(select 1 from t as x where x.a = t.a and x.a < 1) and a < 1",
			best: "Join{DataScan(t)->Selection->DataScan(x)->Selection}(test.t.a,x.a)->Projection",
		},
		{
			sql:  "select a from t where exists(select 1 from t as x where x.a = t.a) and exists(select 1 from t as x where x.a = t.a)",
			best: "Join{Join{DataScan(t)->DataScan(x)}(test.t.a,x.a)->DataScan(x)}(test.t.a,x.a)->Projection",
		},
		{
			sql:  "select * from (select a, b, sum(c) as s from t group by a, b) k where k.a > k.b * 2 + 1",
			best: "DataScan(t)->Selection->Aggr(sum(test.t.c),firstrow(test.t.a),firstrow(test.t.b))->Projection->Projection",
		},
		{
			sql:  "select * from (select a, b, sum(c) as s from t group by a, b) k where k.a > 1 and k.b > 2",
			best: "DataScan(t)->Selection->Aggr(sum(test.t.c),firstrow(test.t.a),firstrow(test.t.b))->Projection->Projection",
		},
		{
			sql:  "select * from (select k.a, sum(k.s) as ss from (select a, sum(b) as s from t group by a) k group by k.a) l where l.a > 2",
			best: "DataScan(t)->Selection->Aggr(sum(test.t.b),firstrow(test.t.a))->Projection->Aggr(sum(k.s),firstrow(k.a))->Projection->Projection",
		},
		{
			sql:  "select * from (select a, sum(b) as s from t group by a) k where a > s",
			best: "DataScan(t)->Aggr(sum(test.t.b),firstrow(test.t.a))->Selection->Projection->Projection",
		},
		{
			sql:  "select * from (select a, sum(b) as s from t group by a + 1) k where a > 1",
			best: "DataScan(t)->Aggr(sum(test.t.b),firstrow(test.t.a))->Selection->Projection->Projection",
		},
		{
			sql:  "select * from (select a, sum(b) as s from t group by a having 1 = 0) k where a > 1",
			best: "DataScan(t)->Selection->Aggr(sum(test.t.b),firstrow(test.t.a))->Selection->Projection->Projection",
		},
		{
			sql:  "select a, count(a) cnt from t group by a having cnt < 1",
			best: "DataScan(t)->Aggr(count(test.t.a),firstrow(test.t.a))->Selection->Projection",
		},
	}
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil, comment)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			is:        is,
			colMapper: make(map[*ast.ColumnNameExpr]int),
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil, comment)
		c.Assert(builder.optFlag&flagPredicatePushDown, Greater, uint64(0))
		p, err = logicalOptimize(flagPredicatePushDown|flagDecorrelate|flagPrunColumns, p.(LogicalPlan), builder.ctx, builder.allocator)
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, ca.best, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestPlanBuilder(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql  string
		plan string
	}{
		{
			// This will be resolved as in sub query.
			sql:  "select * from t where 10 in (select b from t s where s.a = t.a)",
			plan: "Join{DataScan(t)->DataScan(s)}(test.t.a,s.a)->Projection",
		},
		{
			sql:  "select count(c) ,(select b from t s where s.a = t.a) from t",
			plan: "Join{DataScan(t)->Aggr(count(test.t.c),firstrow(test.t.a))->DataScan(s)}(test.t.a,s.a)->Projection->Projection",
		},
		{
			sql:  "select count(c) ,(select count(s.b) from t s where s.a = t.a) from t",
			plan: "Join{DataScan(t)->Aggr(count(test.t.c),firstrow(test.t.a))->DataScan(s)}(test.t.a,s.a)->Aggr(firstrow(aggregation_2_col_0),firstrow(test.t.a),count(s.b))->Projection->Projection",
		},
		{
			// Semi-join with agg cannot decorrelate.
			sql:  "select t.c in (select count(s.b) from t s where s.a = t.a) from t",
			plan: "Apply{DataScan(t)->DataScan(s)->Selection->Aggr(count(s.b))}->Projection",
		},
		{
			// Theta-join with agg cannot decorrelate.
			sql:  "select (select count(s.b) k from t s where s.a = t.a having k != 0) from t",
			plan: "Apply{DataScan(t)->DataScan(s)->Selection->Aggr(count(s.b))}->Projection->Projection",
		},
		{
			// Relation without keys cannot decorrelate.
			sql:  "select (select count(s.b) k from t s where s.a = t1.a) from t t1, t t2",
			plan: "Apply{Join{DataScan(t1)->DataScan(t2)}->DataScan(s)->Selection->Aggr(count(s.b))}->Projection->Projection",
		},
		{
			// Aggregate function like count(1) cannot decorrelate.
			sql:  "select (select count(1) k from t s where s.a = t.a having k != 0) from t",
			plan: "Apply{DataScan(t)->DataScan(s)->Selection->Aggr(count(1))}->Projection->Projection",
		},
		{
			sql:  "select a from t where a in (select a from t s group by t.b)",
			plan: "Join{DataScan(t)->DataScan(s)->Aggr(firstrow(s.a))->Projection}(test.t.a,a)->Projection",
		},
		{
			// This will be resolved as in sub query.
			sql:  "select * from t where 10 in (((select b from t s where s.a = t.a)))",
			plan: "Join{DataScan(t)->DataScan(s)}(test.t.a,s.a)->Projection",
		},
		{
			// This will be resolved as in function.
			sql:  "select * from t where 10 in (((select b from t s where s.a = t.a)), 10)",
			plan: "Join{DataScan(t)->DataScan(s)}(test.t.a,s.a)->Projection->Selection->Projection",
		},
		{
			sql:  "select * from t where exists (select s.a from t s having sum(s.a) = t.a )",
			plan: "Join{DataScan(t)->DataScan(s)->Aggr(sum(s.a))->Projection}(test.t.a,sel_agg_1)->Projection",
		},
		{
			// Test Nested sub query.
			sql:  "select * from t where exists (select s.a from t s where s.c in (select c from t as k where k.d = s.d) having sum(s.a) = t.a )",
			plan: "Join{DataScan(t)->Join{DataScan(s)->DataScan(k)}(s.d,k.d)(s.c,k.c)->Aggr(sum(s.a))->Projection}(test.t.a,sel_agg_1)->Projection",
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
			plan: "Dual->Projection",
		},
		{
			sql:  "select substr(\"abc\", 1)",
			plan: "Dual->Projection",
		},
		{
			sql:  "analyze table t, t",
			plan: "*plan.Analyze",
		},
	}
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		p := builder.build(stmt)
		if lp, ok := p.(LogicalPlan); ok {
			p, err = logicalOptimize(flagBuildKeyInfo|flagDecorrelate|flagPrunColumns, lp.(LogicalPlan), builder.ctx, builder.allocator)
		}
		c.Assert(builder.err, IsNil)
		c.Assert(ToString(p), Equals, ca.plan, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestJoinReOrder(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
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
			best: "Apply{DataScan(o)->Join{Join{DataScan(t2)->Selection->DataScan(t3)}(t2.a,t3.a)->DataScan(t1)}(t3.a,t1.a)->Projection}->Projection",
		},
		{
			sql:  "select * from t o where o.b in (select t3.c from t t1, t t2, t t3 where t1.a = t3.a and t2.a = t3.a and t2.a = o.a and t1.a = 1)",
			best: "Apply{DataScan(o)->Join{Join{DataScan(t1)->Selection->DataScan(t3)->Selection}->DataScan(t2)->Selection}->Projection}->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := MockResolve(stmt)
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
		p, err = logicalOptimize(flagPredicatePushDown, lp.(LogicalPlan), builder.ctx, builder.allocator)
		c.Assert(err, IsNil)
		c.Assert(ToString(lp), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestAggPushDown(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select sum(t.a), sum(t.a+1), sum(t.a), count(t.a), sum(t.a) + count(t.a) from t",
			best: "DataScan(t)->Aggr(sum(test.t.a),sum(plus(test.t.a, 1)),count(test.t.a))->Projection",
		},
		{
			sql:  "select sum(t.a + t.b), sum(t.a + t.c), sum(t.a + t.b), count(t.a) from t having sum(t.a + t.b) > 0 order by sum(t.a + t.c)",
			best: "DataScan(t)->Aggr(sum(plus(test.t.a, test.t.b)),sum(plus(test.t.a, test.t.c)),count(test.t.a))->Selection->Projection->Sort->Projection",
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
			best: "Join{Join{DataScan(a)->DataScan(b)}(a.c,b.c)->DataScan(c)}(b.c,c.c)->Aggr(sum(a.a))->Projection",
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
		{
			sql:  "select max(a.b), max(b.b) from t a join t b on a.c = b.c group by a.a",
			best: "Join{DataScan(a)->DataScan(b)->Aggr(max(b.b),firstrow(b.c))}(a.c,b.c)->Projection->Projection",
		},
		{
			sql:  "select max(a.b), max(b.b) from t a join t b on a.a = b.a group by a.c",
			best: "Join{DataScan(a)->DataScan(b)}(a.a,b.a)->Aggr(max(a.b),max(b.b))->Projection",
		},
		{
			sql:  "select max(c.b) from (select * from t a union all select * from t b) c group by c.a",
			best: "UnionAll{DataScan(a)->Projection->Projection->DataScan(b)->Projection->Projection}->Aggr(max(join_agg_0))->Projection",
		},
		{
			sql:  "select max(a.c) from t a join t b on a.a=b.a and a.b=b.b group by a.b",
			best: "Join{DataScan(a)->DataScan(b)}(a.a,b.a)(a.b,b.b)->Aggr(max(a.c))->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := MockResolve(stmt)
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
		p, err = logicalOptimize(flagBuildKeyInfo|flagPredicatePushDown|flagPrunColumns|flagAggregationOptimize, lp.(LogicalPlan), builder.ctx, builder.allocator)
		lp.ResolveIndicesAndCorCols()
		c.Assert(err, IsNil)
		c.Assert(ToString(lp), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestRefine(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
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
			best: "Index(t.c_d_e)[(4 3 +inf,4 5 +inf]]->Projection",
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
			best: "Index(t.c_d_e)[[-inf <nil>,1 <nil>) (2 +inf,+inf +inf]]->Projection",
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
			best: "Index(t.c_d_e)[(1 3 +inf,1 +inf +inf]]->Projection",
		},
		{
			sql:  "select a from t where c in (1, 2, 3) and (d > 3 and d < 4 or d > 5 and d < 6)",
			best: "Index(t.c_d_e)[(1 3 +inf,1 4 <nil>) (1 5 +inf,1 6 <nil>) (2 3 +inf,2 4 <nil>) (2 5 +inf,2 6 <nil>) (3 3 +inf,3 4 <nil>) (3 5 +inf,3 6 <nil>)]->Projection",
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
			sql:  "select a from t use index(c_d_e) where c != 1",
			best: "Index(t.c_d_e)[[-inf <nil>,1 <nil>) (1 +inf,+inf +inf]]->Projection",
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
			best: "Index(t.c_d_e_str)[[abc <nil>,abd <nil>)]->Projection",
		},
		{
			sql:  "select a from t where c_str like 'abc_'",
			best: "Index(t.c_d_e_str)[(abc +inf,abd <nil>)]->Selection->Projection",
		},
		{
			sql:  "select a from t where c_str like 'abc%af'",
			best: "Index(t.c_d_e_str)[[abc <nil>,abd <nil>)]->Selection->Projection",
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
			best: "Index(t.c_d_e_str)[(abc\\ +inf,abc] <nil>)]->Selection->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc\\_%'`,
			best: "Index(t.c_d_e_str)[[abc_ <nil>,abc` <nil>)]->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc=_%' escape '='`,
			best: "Index(t.c_d_e_str)[[abc_ <nil>,abc` <nil>)]->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc\\__'`,
			best: "Index(t.c_d_e_str)[(abc_ +inf,abc` <nil>)]->Selection->Projection",
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
			best: "Index(t.c_d_e)[[-inf <nil>,2 <nil>)]->Projection",
		},
		{
			sql:  `select a from t where c >= 1.1`,
			best: "Index(t.c_d_e)[(1 +inf,+inf +inf]]->Projection",
		},
		{
			sql:  `select a from t where c > 1.9`,
			best: "Index(t.c_d_e)[[2,+inf]]->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			is:        is,
		}
		p := builder.build(stmt).(LogicalPlan)
		c.Assert(builder.err, IsNil)
		p, err = logicalOptimize(flagPredicatePushDown|flagPrunColumns, p.(LogicalPlan), builder.ctx, builder.allocator)
		info, err := p.convert2PhysicalPlan(&requiredProperty{})
		c.Assert(err, IsNil)
		jsonPlan, _ := info.p.MarshalJSON()
		c.Assert(ToString(info.p), Equals, tt.best, Commentf("for %s, %s", tt.sql, string(jsonPlan)))
	}
}

func (s *testPlanSuite) TestColumnPruning(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
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
				"TableScan_3": {"a", "b"},
			},
		},
		{
			sql: "select exists (select count(*) from t where b = k.a) from t k",
			ans: map[string][]string{
				"TableScan_1": {},
			},
		},
		{
			sql: "select b = (select count(*) from t where b = k.a) from t k",
			ans: map[string][]string{
				"TableScan_1": {"a", "b"},
				"TableScan_3": {"b"},
			},
		},
		{
			sql: "select exists (select count(a) from t where b = k.a group by b) from t k",
			ans: map[string][]string{
				"TableScan_1": {"a"},
				"TableScan_3": {"b"},
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
				"TableScan_3": {"c"},
			},
		},
		{
			sql: "select a from t where (b,a) != all (select c,d from t)",
			ans: map[string][]string{
				"TableScan_1": {"a", "b"},
				"TableScan_3": {"c", "d"},
			},
		},
		{
			sql: "select a from t where (b,a) in (select c,d from t)",
			ans: map[string][]string{
				"TableScan_1": {"a", "b"},
				"TableScan_3": {"c", "d"},
			},
		},
		{
			sql: "select a from t where a in (select a from t s group by t.b)",
			ans: map[string][]string{
				"TableScan_1": {"a"},
				"TableScan_3": {"a"},
			},
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil, comment)

		builder := &planBuilder{
			colMapper: make(map[*ast.ColumnNameExpr]int),
			allocator: new(idAllocator),
			ctx:       mockContext(),
			is:        is,
		}
		p := builder.build(stmt).(LogicalPlan)
		c.Assert(builder.err, IsNil, comment)

		p, err = logicalOptimize(flagPredicatePushDown|flagPrunColumns, p.(LogicalPlan), builder.ctx, builder.allocator)
		c.Assert(err, IsNil)
		checkDataSourceCols(p, c, tt.ans, comment)
	}
}

func (s *testPlanSuite) TestAllocID(c *C) {
	ctx := mockContext()
	pA := DataSource{}.init(new(idAllocator), ctx)
	pB := DataSource{}.init(new(idAllocator), ctx)
	c.Assert(pA.id, Equals, pB.id)
}

func checkDataSourceCols(p Plan, c *C, ans map[string][]string, comment CommentInterface) {
	switch p.(type) {
	case *DataSource:
		colList, ok := ans[p.ID()]
		c.Assert(ok, IsTrue, comment)
		for i, colName := range colList {
			c.Assert(colName, Equals, p.Schema().Columns[i].ColName.L, comment)
		}
	}
	for _, child := range p.Children() {
		checkDataSourceCols(child, c, ans, comment)
	}
}

func (s *testPlanSuite) TestValidate(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
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
	for _, tt := range tests {
		sql := tt.sql
		comment := Commentf("for %s", sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)
		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)
		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		builder.build(stmt)
		if tt.err == nil {
			c.Assert(builder.err, IsNil, comment)
		} else {
			c.Assert(tt.err.Equal(builder.err), IsTrue, comment)
		}
	}
}

func checkUniqueKeys(p Plan, c *C, ans map[string][][]string, sql string) {
	keyList, ok := ans[p.ID()]
	c.Assert(ok, IsTrue, Commentf("for %s, %v not found", sql, p.ID()))
	c.Assert(len(p.Schema().Keys), Equals, len(keyList), Commentf("for %s, %v, the number of key doesn't match, the schema is %s", sql, p.ID(), p.Schema()))
	for i, key := range keyList {
		c.Assert(len(key), Equals, len(p.Schema().Keys[i]), Commentf("for %s, %v %v, the number of column doesn't match", sql, p.ID(), key))
		for j, colName := range key {
			c.Assert(colName, Equals, p.Schema().Keys[i][j].String(), Commentf("for %s, %v %v, column dosen't match", sql, p.ID(), key))
		}
	}
	for _, child := range p.Children() {
		checkUniqueKeys(child, c, ans, sql)
	}
}

func (s *testPlanSuite) TestUniqueKeyInfo(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql string
		ans map[string][][]string
	}{
		{
			sql: "select a, sum(e) from t group by b",
			ans: map[string][][]string{
				"TableScan_1":   {{"test.t.a"}},
				"Aggregation_2": {{"test.t.a"}},
				"Projection_3":  {{"a"}},
			},
		},
		{
			sql: "select a, b, sum(f) from t group by b",
			ans: map[string][][]string{
				"TableScan_1":   {{"test.t.f"}, {"test.t.a"}},
				"Aggregation_2": {{"test.t.a"}, {"test.t.b"}},
				"Projection_3":  {{"a"}, {"b"}},
			},
		},
		{
			sql: "select c, d, e, sum(a) from t group by c, d, e",
			ans: map[string][][]string{
				"TableScan_1":   {{"test.t.a"}},
				"Aggregation_2": {{"test.t.c", "test.t.d", "test.t.e"}},
				"Projection_3":  {{"c", "d", "e"}},
			},
		},
		{
			sql: "select f, g, sum(a) from t",
			ans: map[string][][]string{
				"TableScan_1":   {{"test.t.f"}, {"test.t.g"}, {"test.t.f", "test.t.g"}, {"test.t.a"}},
				"Aggregation_2": {{"test.t.f"}, {"test.t.g"}, {"test.t.f", "test.t.g"}},
				"Projection_3":  {{"f"}, {"g"}, {"f", "g"}},
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
				"Projection_5":  {{"f"}},
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
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			colMapper: make(map[*ast.ColumnNameExpr]int),
			allocator: new(idAllocator),
			ctx:       mockContext(),
			is:        is,
		}
		p := builder.build(stmt).(LogicalPlan)
		c.Assert(builder.err, IsNil, comment)

		p, err = logicalOptimize(flagPredicatePushDown|flagPrunColumns|flagBuildKeyInfo, p.(LogicalPlan), builder.ctx, builder.allocator)
		checkUniqueKeys(p, c, tt.ans, tt.sql)
	}
}

func (s *testPlanSuite) TestAggPrune(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select a, count(b) from t group by a",
			best: "DataScan(t)->Projection->Projection",
		},
		{
			sql:  "select sum(b) from t group by c, d, e",
			best: "DataScan(t)->Aggr(sum(test.t.b))->Projection",
		},
		{
			sql:  "select t1.a, count(t2.b) from t t1, t t2 where t1.a = t2.a group by t1.a",
			best: "Join{DataScan(t1)->DataScan(t2)}(t1.a,t2.a)->Projection->Projection",
		},
		{
			sql:  "select tt.a, sum(tt.b) from (select a, b from t) tt group by tt.a",
			best: "DataScan(t)->Projection->Projection->Projection",
		},
		{
			sql:  "select count(1) from (select count(1), a as b from t group by a) tt group by b",
			best: "DataScan(t)->Projection->Projection->Projection->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			is:        is,
		}
		p := builder.build(stmt).(LogicalPlan)
		c.Assert(builder.err, IsNil)
		p, err = logicalOptimize(flagPredicatePushDown|flagPrunColumns|flagBuildKeyInfo|flagAggregationOptimize, p.(LogicalPlan), builder.ctx, builder.allocator)
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestVisitInfo(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql string
		ans []visitInfo
	}{
		{
			sql: "insert into t values (1)",
			ans: []visitInfo{
				{mysql.InsertPriv, "test", "t", ""},
			},
		},
		{
			sql: "delete from t where a = 1",
			ans: []visitInfo{
				{mysql.DeletePriv, "test", "t", ""},
				{mysql.SelectPriv, "test", "t", ""},
			},
		},
		{
			sql: "delete from a1 using t as a1 inner join t as a2 where a1.a = a2.a",
			ans: []visitInfo{
				{mysql.DeletePriv, "test", "t", ""},
				{mysql.SelectPriv, "test", "t", ""},
			},
		},
		{
			sql: "update t set a = 7 where a = 1",
			ans: []visitInfo{
				{mysql.UpdatePriv, "test", "t", ""},
				{mysql.SelectPriv, "test", "t", ""},
			},
		},
		{
			sql: "update t, (select * from t) a1 set t.a = a1.a;",
			ans: []visitInfo{
				{mysql.UpdatePriv, "test", "t", ""},
				{mysql.SelectPriv, "test", "t", ""},
			},
		},
		{
			sql: "select a, sum(e) from t group by a",
			ans: []visitInfo{
				{mysql.SelectPriv, "test", "t", ""},
			},
		},
		{
			sql: "truncate table t",
			ans: []visitInfo{
				{mysql.DeletePriv, "test", "t", ""},
			},
		},
		{
			sql: "drop table t",
			ans: []visitInfo{
				{mysql.DropPriv, "test", "t", ""},
			},
		},
		{
			sql: "create table t (a int)",
			ans: []visitInfo{
				{mysql.CreatePriv, "test", "t", ""},
			},
		},
		{
			sql: "create table t1 like t",
			ans: []visitInfo{
				{mysql.CreatePriv, "test", "t1", ""},
				{mysql.SelectPriv, "test", "t", ""},
			},
		},
		{
			sql: "create database test",
			ans: []visitInfo{
				{mysql.CreatePriv, "test", "", ""},
			},
		},
		{
			sql: "drop database test",
			ans: []visitInfo{
				{mysql.DropPriv, "test", "", ""},
			},
		},
		{
			sql: "create index t_1 on t (a)",
			ans: []visitInfo{
				{mysql.IndexPriv, "test", "t", ""},
			},
		},
		{
			sql: "drop index e on t",
			ans: []visitInfo{
				{mysql.IndexPriv, "test", "t", ""},
			},
		},
		{
			sql: `create user 'test'@'%' identified by '123456'`,
			ans: []visitInfo{
				{mysql.CreateUserPriv, "", "", ""},
			},
		},
		{
			sql: `drop user 'test'@'%'`,
			ans: []visitInfo{
				{mysql.CreateUserPriv, "", "", ""},
			},
		},
		{
			sql: `grant all privileges on test.* to 'test'@'%'`,
			ans: []visitInfo{
				{mysql.SelectPriv, "test", "", ""},
				{mysql.InsertPriv, "test", "", ""},
				{mysql.UpdatePriv, "test", "", ""},
				{mysql.DeletePriv, "test", "", ""},
				{mysql.CreatePriv, "test", "", ""},
				{mysql.DropPriv, "test", "", ""},
				{mysql.GrantPriv, "test", "", ""},
				{mysql.AlterPriv, "test", "", ""},
				{mysql.ExecutePriv, "test", "", ""},
				{mysql.IndexPriv, "test", "", ""},
			},
		},
		{
			sql: `grant select on test.ttt to 'test'@'%'`,
			ans: []visitInfo{
				{mysql.SelectPriv, "test", "ttt", ""},
				{mysql.GrantPriv, "test", "ttt", ""},
			},
		},
		{
			sql: `revoke all privileges on *.* from 'test'@'%'`,
			ans: []visitInfo{
				{mysql.SuperPriv, "", "", ""},
			},
		},
		{
			sql: `set password for 'root'@'%' = 'xxxxx'`,
			ans: []visitInfo{
				{mysql.SuperPriv, "", "", ""},
			},
		},
	}

	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			colMapper: make(map[*ast.ColumnNameExpr]int),
			allocator: new(idAllocator),
			ctx:       mockContext(),
			is:        is,
		}
		builder.build(stmt)
		c.Assert(builder.err, IsNil, comment)

		checkVisitInfo(c, builder.visitInfo, tt.ans, comment)
	}
}

type visitInfoArray []visitInfo

func (v visitInfoArray) Len() int {
	return len(v)
}

func (v visitInfoArray) Less(i, j int) bool {
	if v[i].privilege < v[j].privilege {
		return true
	}
	if v[i].db < v[j].db {
		return true
	}
	if v[i].table < v[j].table {
		return true
	}
	if v[i].column < v[j].column {
		return true
	}

	return false
}

func (v visitInfoArray) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}

func unique(v []visitInfo) []visitInfo {
	repeat := 0
	for i := 1; i < len(v); i++ {
		if v[i] == v[i-1] {
			repeat++
		} else {
			v[i-repeat] = v[i]
		}
	}
	return v[:len(v)-repeat]
}

func checkVisitInfo(c *C, v1, v2 []visitInfo, comment CommentInterface) {
	sort.Sort(visitInfoArray(v1))
	sort.Sort(visitInfoArray(v2))
	v1 = unique(v1)
	v2 = unique(v2)

	c.Assert(len(v1), Equals, len(v2), comment)
	for i := 0; i < len(v1); i++ {
		c.Assert(v1[i], Equals, v2[i], comment)
	}
}

func (s *testPlanSuite) TestTopNPushDown(c *C) {
	UseDAGPlanBuilder = true
	defer func() {
		testleak.AfterTest(c)()
		UseDAGPlanBuilder = false
	}()
	tests := []struct {
		sql  string
		best string
	}{
		// Test TopN + Selection.
		{
			sql:  "select * from t where a < 1 order by b limit 5",
			best: "DataScan(t)->Sort + Limit(5) + Offset(0)->Projection",
		},
		// Test Limit + Selection.
		{
			sql:  "select * from t where a < 1 limit 5",
			best: "DataScan(t)->Limit->Projection",
		},
		// Test Limit + Agg + Proj .
		{
			sql:  "select a, count(b) from t group by b limit 5",
			best: "DataScan(t)->Aggr(count(test.t.b),firstrow(test.t.a))->Limit->Projection",
		},
		// Test TopN + Agg + Proj .
		{
			sql:  "select a, count(b) from t group by b order by c limit 5",
			best: "DataScan(t)->Aggr(count(test.t.b),firstrow(test.t.a),firstrow(test.t.c))->Sort + Limit(5) + Offset(0)->Projection->Projection",
		},
		// Test TopN + Join + Proj.
		{
			sql:  "select * from t, t s order by t.a limit 5",
			best: "Join{DataScan(t)->DataScan(s)}->Sort + Limit(5) + Offset(0)->Projection",
		},
		// Test Limit + Join + Proj.
		{
			sql:  "select * from t, t s limit 5",
			best: "Join{DataScan(t)->DataScan(s)}->Limit->Projection",
		},
		// Test TopN + Left Join + Proj.
		{
			sql:  "select * from t left outer join t s on t.a = s.a order by t.a limit 5",
			best: "Join{DataScan(t)->Sort + Limit(5) + Offset(0)->DataScan(s)}(test.t.a,s.a)->Sort + Limit(5) + Offset(0)->Projection",
		},
		// Test TopN + Left Join + Proj.
		{
			sql:  "select * from t left outer join t s on t.a = s.a order by t.a limit 5, 5",
			best: "Join{DataScan(t)->Sort + Limit(10) + Offset(0)->DataScan(s)}(test.t.a,s.a)->Sort + Limit(5) + Offset(5)->Projection",
		},
		// Test Limit + Left Join + Proj.
		{
			sql:  "select * from t left outer join t s on t.a = s.a limit 5",
			best: "Join{DataScan(t)->Limit->DataScan(s)}(test.t.a,s.a)->Limit->Projection",
		},
		// Test Limit + Left Join Apply + Proj.
		{
			sql:  "select (select s.a from t s where t.a = s.a) from t limit 5",
			best: "Join{DataScan(t)->Limit->DataScan(s)}(test.t.a,s.a)->Limit->Projection->Projection",
		},
		// Test TopN + Left Join Apply + Proj.
		{
			sql:  "select (select s.a from t s where t.a = s.a) from t order by t.a limit 5",
			best: "Join{DataScan(t)->Sort + Limit(5) + Offset(0)->DataScan(s)}(test.t.a,s.a)->Sort + Limit(5) + Offset(0)->Projection->Projection->Projection",
		},
		// Test TopN + Left Semi Join Apply + Proj.
		{
			sql:  "select exists (select s.a from t s where t.a = s.a) from t order by t.a limit 5",
			best: "Join{DataScan(t)->Sort + Limit(5) + Offset(0)->DataScan(s)}(test.t.a,s.a)->Sort + Limit(5) + Offset(0)->Projection->Projection",
		},
		// Test TopN + Semi Join Apply + Proj.
		{
			sql:  "select * from t where exists (select s.a from t s where t.a = s.a) order by t.a limit 5",
			best: "Join{DataScan(t)->DataScan(s)}(test.t.a,s.a)->Sort + Limit(5) + Offset(0)->Projection",
		},
		// Test TopN + Right Join + Proj.
		{
			sql:  "select * from t right outer join t s on t.a = s.a order by s.a limit 5",
			best: "Join{DataScan(t)->DataScan(s)->Sort + Limit(5) + Offset(0)}(test.t.a,s.a)->Sort + Limit(5) + Offset(0)->Projection",
		},
		// Test Limit + Right Join + Proj.
		{
			sql:  "select * from t right outer join t s on t.a = s.a order by s.a,t.b limit 5",
			best: "Join{DataScan(t)->DataScan(s)}(test.t.a,s.a)->Sort + Limit(5) + Offset(0)->Projection",
		},
		// Test TopN + UA + Proj.
		{
			sql:  "select * from t union all (select * from t s) order by a,b limit 5",
			best: "UnionAll{DataScan(t)->Sort + Limit(5) + Offset(0)->Projection->DataScan(s)->Sort + Limit(5) + Offset(0)->Projection}->Sort + Limit(5) + Offset(0)",
		},
		// Test TopN + UA + Proj.
		{
			sql:  "select * from t union all (select * from t s) order by a,b limit 5, 5",
			best: "UnionAll{DataScan(t)->Sort + Limit(10) + Offset(0)->Projection->DataScan(s)->Sort + Limit(10) + Offset(0)->Projection}->Sort + Limit(5) + Offset(5)",
		},
		// Test Limit + UA + Proj + Sort.
		{
			sql:  "select * from t union all (select * from t s order by a) limit 5",
			best: "UnionAll{DataScan(t)->Limit->Projection->DataScan(s)->Sort + Limit(5) + Offset(0)->Projection->Projection}->Limit",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			is:        is,
			colMapper: make(map[*ast.ColumnNameExpr]int),
		}
		p := builder.build(stmt).(LogicalPlan)
		c.Assert(builder.err, IsNil)
		p, err = logicalOptimize(builder.optFlag, p.(LogicalPlan), builder.ctx, builder.allocator)
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, comment)
	}
}
