// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

// AggregateFuncExtractor visits Expr tree.
// It converts ColunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type AggregateFuncExtractor struct {
	inAggregateFuncExpr bool
	// AggFuncs is the collected AggregateFuncExprs.
	AggFuncs []*ast.AggregateFuncExpr
}

// Enter implements Visitor interface.
func (a *AggregateFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggregateFuncExpr = true
	case *ast.SelectStmt, *ast.UnionStmt:
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (a *AggregateFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggregateFuncExpr = false
		a.AggFuncs = append(a.AggFuncs, v)
	}
	return n, true
}

func MockTable() *model.TableInfo {
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
	return table
}

func MockResolve(node ast.Node) (infoschema.InfoSchema, error) {
	is := infoschema.MockInfoSchema([]*model.TableInfo{MockTable()})
	ctx := mockContext()
	err := MockResolveName(node, is, "test", ctx)
	if err != nil {
		return nil, err
	}
	return is, expression.InferType(ctx.GetSessionVars().StmtCtx, node)
}

func newLongType() types.FieldType {
	return *(types.NewFieldType(mysql.TypeLong))
}

func newStringType() types.FieldType {
	ft := types.NewFieldType(mysql.TypeVarchar)
	ft.Charset, ft.Collate = types.DefaultCharsetForType(mysql.TypeVarchar)
	return *ft
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
		histogram.Buckets[i].UpperBound = values[i]
	}
	return histogram
}

type mockStore struct {
	client *mockClient
}

func (m *mockStore) GetClient() kv.Client {
	return m.client
}

func (m *mockStore) GetOracle() oracle.Oracle {
	return nil
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

type mockClient struct {
}

func (c *mockClient) Send(ctx goctx.Context, _ *kv.Request) kv.Response {
	return nil
}

func (c *mockClient) IsRequestTypeSupported(reqType, subType int64) bool {
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

type VisitInfoArray []visitInfo

func (v VisitInfoArray) Len() int {
	return len(v)
}

func (v VisitInfoArray) Less(i, j int) bool {
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

func (v VisitInfoArray) Swap(i, j int) {
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
