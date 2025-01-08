// Copyright 2024 PingCAP, Inc.
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

package infoschema

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema/internal"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	cacheCnt    int
	testActions []testAction
	statusHook  *mockStatusHook

	ctx context.Context
	r   autoid.Requirement
	is  infoschemaV2
	t   *testing.T
}

type cacheOp int

const (
	opCacheMiss cacheOp = iota
	opCacheHit
	opCacheEvict
)

type testOp int

const (
	opCreateSchema testOp = iota
	opCreateTable
	opTableByID
	opTableByName
)

type testAction struct {
	op   testOp
	args []any
}

func TestInfoschemaCache(t *testing.T) {
	tests := []testCase{
		{
			cacheCnt: 3,
			testActions: []testAction{
				{op: opCreateSchema, args: []any{1, "db1"}},

				{op: opCreateTable, args: []any{1, "db1", 1, "t1", 1}},
				{op: opCreateTable, args: []any{1, "db1", 2, "t2", 2}},
				{op: opCreateTable, args: []any{1, "db1", 3, "t3", 3}},

				{op: opCreateTable, args: []any{1, "db1", 4, "t4", 4, opCacheEvict}},

				{op: opTableByID, args: []any{1, 4, opCacheMiss, opCacheEvict}},
				{op: opTableByID, args: []any{2, 4, opCacheMiss, opCacheEvict}},
				{op: opTableByID, args: []any{3, 4, opCacheMiss, opCacheEvict}},
				{op: opTableByID, args: []any{4, 4, opCacheMiss, opCacheEvict}},

				{op: opTableByID, args: []any{2, 4, opCacheHit}},
				{op: opTableByID, args: []any{3, 4, opCacheHit}},
				{op: opTableByID, args: []any{4, 4, opCacheHit}},
			},
		},
		{
			cacheCnt: 3,
			testActions: []testAction{
				{op: opCreateSchema, args: []any{1, "db1"}},
				{op: opCreateTable, args: []any{1, "db1", 1, "t1", 1}},
				{op: opCreateTable, args: []any{1, "db1", 2, "t2", 2}},
				{op: opCreateTable, args: []any{1, "db1", 3, "t3", 3}},

				{op: opCreateTable, args: []any{1, "db1", 4, "t4", 4, opCacheEvict}},

				{op: opTableByName, args: []any{"db1", "t1", 4, opCacheMiss, opCacheEvict}},
				{op: opTableByName, args: []any{"db1", "t2", 4, opCacheMiss, opCacheEvict}},
				{op: opTableByName, args: []any{"db1", "t3", 4, opCacheMiss, opCacheEvict}},
				{op: opTableByName, args: []any{"db1", "t4", 4, opCacheMiss, opCacheEvict}},

				{op: opTableByName, args: []any{"db1", "t2", 4, opCacheHit}},
				{op: opTableByName, args: []any{"db1", "t3", 4, opCacheHit}},
				{op: opTableByName, args: []any{"db1", "t4", 4, opCacheHit}},
			},
		},
	}

	for _, tc := range tests {
		tc.run(t)
	}
}

type mockStatusHook struct {
	mock.Mock
}

func (m *mockStatusHook) onHit() {
	m.Called()
}
func (m *mockStatusHook) onMiss() {
	m.Called()
}
func (m *mockStatusHook) onEvict() {
	m.Called()
}
func (m *mockStatusHook) onUpdateSize(size uint64) {
	m.Called()
}
func (m *mockStatusHook) onUpdateLimit(limit uint64) {
	m.Called()
}

func mockTableSize(r autoid.Requirement, t *testing.T) uint64 {
	tblInfo := internal.MockTableInfo(t, r.Store(), "t")
	item := entry[tableCacheKey, table.Table]{
		key:   tableCacheKey{1, 1},
		value: internal.MockTable(t, r.Store(), tblInfo),
	}
	return item.Size()
}

func (tc *testCase) run(t *testing.T) {
	tc.ctx = WithRefillOption(context.Background(), true)
	tc.statusHook = &mockStatusHook{}
	tc.r = internal.CreateAutoIDRequirement(t)
	defer func() {
		tc.r.Store().Close()
	}()

	data := NewData()
	data.tableCache.SetStatusHook(tc.statusHook)
	tc.statusHook.On("onUpdateSize").Return()

	tableSize := mockTableSize(tc.r, t)
	tc.statusHook.On("onUpdateLimit").Return().Once()
	data.tableCache.SetCapacity(uint64(tc.cacheCnt) * tableSize)

	tc.is = NewInfoSchemaV2(tc.r, nil, data)
	tc.t = t

	for _, action := range tc.testActions {
		tc.runAction(action)
	}

	tc.statusHook.AssertExpectations(tc.t)
}

func (tc *testCase) runAction(action testAction) {
	switch action.op {
	case opCreateSchema:
		tc.createSchema(action.args)
	case opCreateTable:
		tc.createTable(action.args)
	case opTableByID:
		tc.tableByID(action.args)
	case opTableByName:
		tc.tableByName(action.args)
	}
}

// args: dbID, dbName
func (tc *testCase) createSchema(args []any) {
	dbInfo := &model.DBInfo{
		ID:    int64(args[0].(int)),
		Name:  ast.NewCIStr(args[1].(string)),
		State: model.StatePublic,
	}
	dbInfo.Deprecated.Tables = []*model.TableInfo{}
	tc.is.Data.addDB(dbInfo.ID, dbInfo)
	internal.AddDB(tc.t, tc.r.Store(), dbInfo)
}

// args: dbID, dbName, tblID, tblName, schemaVersion, cacheOp
func (tc *testCase) createTable(args []any) {
	colInfo := &model.ColumnInfo{
		ID:        1,
		Name:      ast.NewCIStr("a"),
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     model.StatePublic,
	}

	tblInfo := &model.TableInfo{
		DBID:    int64(args[0].(int)),
		ID:      int64(args[2].(int)),
		Name:    ast.NewCIStr(args[3].(string)),
		Columns: []*model.ColumnInfo{colInfo},
		State:   model.StatePublic,
	}

	for i := 5; i < len(args); i++ {
		switch args[i].(cacheOp) {
		case opCacheEvict:
			tc.statusHook.On("onEvict").Return().Once()
		}
	}
	tc.is.Data.add(tableItem{ast.NewCIStr(args[1].(string)), tblInfo.DBID, tblInfo.Name, tblInfo.ID, int64(args[4].(int)), false}, internal.MockTable(tc.t, tc.r.Store(), tblInfo))
	internal.AddTable(tc.t, tc.r.Store(), tblInfo.DBID, tblInfo)
}

// args: tblID, version, cacheOp, cacheOp, ...
func (tc *testCase) tableByID(args []any) {
	tc.is.base().schemaMetaVersion = int64(args[1].(int))
	ver, err := tc.r.Store().CurrentVersion(kv.GlobalTxnScope)
	require.NoError(tc.t, err)
	tc.is.ts = ver.Ver

	for i := 2; i < len(args); i++ {
		switch args[i].(cacheOp) {
		case opCacheMiss:
			tc.statusHook.On("onMiss").Return().Once()
		case opCacheHit:
			tc.statusHook.On("onHit").Return().Once()
		case opCacheEvict:
			tc.statusHook.On("onEvict").Return().Once()
		}
	}

	tc.is.TableByID(tc.ctx, int64(args[0].(int)))
}

// args: dbName, tblName, version, cacheOp, cacheOp, ...
func (tc *testCase) tableByName(args []any) {
	tc.is.base().schemaMetaVersion = int64(args[2].(int))
	ver, err := tc.r.Store().CurrentVersion(kv.GlobalTxnScope)
	require.NoError(tc.t, err)
	tc.is.ts = ver.Ver

	for i := 3; i < len(args); i++ {
		switch args[i].(cacheOp) {
		case opCacheMiss:
			tc.statusHook.On("onMiss").Return().Once()
		case opCacheHit:
			tc.statusHook.On("onHit").Return().Once()
		case opCacheEvict:
			tc.statusHook.On("onEvict").Return().Once()
		}
	}

	tc.is.TableByName(tc.ctx, ast.NewCIStr(args[0].(string)), ast.NewCIStr(args[1].(string)))
}
