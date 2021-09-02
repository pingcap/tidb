// Copyright 2021 PingCAP, Inc.
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

package temptable

import (
	"context"
	"testing"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testbridge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testbridge.WorkaroundGoCheckFlags()
	goleak.VerifyTestMain(m)
}

type TemporaryTableManagerSuite struct {
	suite.Suite
	sctx sessionctx.Context
	mgr  *TemporaryTableManager
}

func TestTemporaryTableManagerSuit(t *testing.T) {
	suite.Run(t, new(TemporaryTableManagerSuite))
}

func (s *TemporaryTableManagerSuite) SetupTest() {
	store, err := mockstore.NewMockStore()
	assert.Nil(s.T(), err)

	sctx := mock.NewContext()
	sctx.Store = store

	s.sctx = sctx
	s.mgr = GetTemporaryTableManager(sctx)
}

func (s *TemporaryTableManagerSuite) TearDownTest() {
	assert.Nil(s.T(), s.sctx.GetStore().Close())
}

func (s *TemporaryTableManagerSuite) TestAddLocalTemporaryTable() {
	assert := assert.New(s.T())

	tbl1 := newMockTable(1, "t1")
	tbl2 := newMockTable(2, "t2")

	assert.Nil(s.sctx.GetSessionVars().LocalTemporaryTables)
	assert.Nil(s.sctx.GetSessionVars().TemporaryTableData)

	// insert t1
	err := s.mgr.AddLocalTemporaryTable(model.NewCIStr("db1"), tbl1)
	assert.Nil(err)
	assert.NotNil(s.sctx.GetSessionVars().LocalTemporaryTables)
	assert.NotNil(s.sctx.GetSessionVars().TemporaryTableData)

	got, exists := s.sctx.GetSessionVars().LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByID(1)
	assert.True(exists)
	assert.Equal(got, tbl1)

	// insert t2 with data
	err = s.mgr.AddLocalTemporaryTable(model.NewCIStr("db1"), tbl2)
	assert.Nil(err)
	assert.NotNil(s.sctx.GetSessionVars().LocalTemporaryTables)
	assert.NotNil(s.sctx.GetSessionVars().TemporaryTableData)

	k := tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(1))
	err = s.sctx.GetSessionVars().TemporaryTableData.SetTableKey(1, k, []byte("v1"))
	assert.Nil(err)

	got, exists = s.sctx.GetSessionVars().LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByID(2)
	assert.True(exists)
	assert.Equal(got, tbl2)

	val, err := s.sctx.GetSessionVars().TemporaryTableData.Get(context.Background(), k)
	assert.Nil(err)
	assert.Equal([]byte("v1"), val)

	// insert dup table
	tbl1x := newMockTable(3, "t1")
	err = s.mgr.AddLocalTemporaryTable(model.NewCIStr("db1"), tbl1x)
	assert.True(infoschema.ErrTableExists.Equal(err))
	got, exists = s.sctx.GetSessionVars().LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByID(3)
	assert.False(exists)
	assert.Nil(got)
	assert.NotNil(s.sctx.GetSessionVars().LocalTemporaryTables)
	assert.NotNil(s.sctx.GetSessionVars().TemporaryTableData)

	// insert should be success for same table name in different db
	err = s.mgr.AddLocalTemporaryTable(model.NewCIStr("db2"), tbl1x)
	assert.Nil(err)
	got, exists = s.sctx.GetSessionVars().LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByID(3)
	assert.True(exists)
	assert.Equal(got, tbl1x)

	// tbl1 still exist
	got, exists = s.sctx.GetSessionVars().LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByID(1)
	assert.True(exists)
	assert.Equal(got, tbl1)
}

func (s *TemporaryTableManagerSuite) TestRemoveLocalTemporaryTable() {
	assert := assert.New(s.T())

	// remove when empty
	err := s.mgr.RemoveLocalTemporaryTable(ast.Ident{Schema: model.NewCIStr("db1"), Name: model.NewCIStr("t1")})
	assert.True(infoschema.ErrTableNotExists.Equal(err))

	// add one table
	tbl1 := newMockTable(1, "t1")
	err = s.mgr.AddLocalTemporaryTable(model.NewCIStr("db1"), tbl1)
	assert.Nil(err)
	k := tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(1))
	err = s.sctx.GetSessionVars().TemporaryTableData.SetTableKey(1, k, []byte("v1"))
	assert.Nil(err)

	// remove failed when table not found
	err = s.mgr.RemoveLocalTemporaryTable(ast.Ident{Schema: model.NewCIStr("db1"), Name: model.NewCIStr("t2")})
	assert.True(infoschema.ErrTableNotExists.Equal(err))

	// remove failed when table not found (same table name in different db)
	err = s.mgr.RemoveLocalTemporaryTable(ast.Ident{Schema: model.NewCIStr("db2"), Name: model.NewCIStr("t1")})
	assert.True(infoschema.ErrTableNotExists.Equal(err))

	// check failed remove should have no effects
	got, exists := s.sctx.GetSessionVars().LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByID(1)
	assert.True(exists)
	assert.Equal(got, tbl1)
	val, err := s.sctx.GetSessionVars().TemporaryTableData.Get(context.Background(), k)
	assert.Nil(err)
	assert.Equal([]byte("v1"), val)

	// remove success
	err = s.mgr.RemoveLocalTemporaryTable(ast.Ident{Schema: model.NewCIStr("db1"), Name: model.NewCIStr("t1")})
	assert.Nil(err)
	got, exists = s.sctx.GetSessionVars().LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByID(1)
	assert.Nil(got)
	assert.False(exists)
	val, err = s.sctx.GetSessionVars().TemporaryTableData.Get(context.Background(), k)
	assert.Nil(err)
	assert.Equal([]byte{}, val)
}

func (s *TemporaryTableManagerSuite) TestReplaceAndTruncateLocalTemporaryTable() {
	assert := assert.New(s.T())

	// truncate when empty
	err := s.mgr.ReplaceAndTruncateLocalTemporaryTable(
		ast.Ident{Schema: model.NewCIStr("db1"), Name: model.NewCIStr("t1")},
		newMockTable(1, "t1"),
	)
	assert.True(infoschema.ErrTableNotExists.Equal(err))
	assert.Nil(s.sctx.GetSessionVars().LocalTemporaryTables)
	assert.Nil(s.sctx.GetSessionVars().TemporaryTableData)

	// add one table
	tbl1 := newMockTable(1, "t1")
	err = s.mgr.AddLocalTemporaryTable(model.NewCIStr("db1"), tbl1)
	assert.Nil(err)
	k := tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(1))
	err = s.sctx.GetSessionVars().TemporaryTableData.SetTableKey(1, k, []byte("v1"))
	assert.Nil(err)

	// truncate failed for table not exist
	err = s.mgr.ReplaceAndTruncateLocalTemporaryTable(
		ast.Ident{Schema: model.NewCIStr("db1"), Name: model.NewCIStr("t2")},
		newMockTable(2, "t2"),
	)
	assert.True(infoschema.ErrTableNotExists.Equal(err))

	// truncate failed for table not exist (same table name in different db)
	err = s.mgr.ReplaceAndTruncateLocalTemporaryTable(
		ast.Ident{Schema: model.NewCIStr("db2"), Name: model.NewCIStr("t1")},
		newMockTable(2, "t1"),
	)
	assert.True(infoschema.ErrTableNotExists.Equal(err))

	// truncate failed for same table id
	err = s.mgr.ReplaceAndTruncateLocalTemporaryTable(
		ast.Ident{Schema: model.NewCIStr("db1"), Name: model.NewCIStr("t1")},
		newMockTable(1, "t1"),
	)
	assert.NotNil(err)

	// truncate failed for not same table name
	err = s.mgr.ReplaceAndTruncateLocalTemporaryTable(
		ast.Ident{Schema: model.NewCIStr("db1"), Name: model.NewCIStr("t1")},
		newMockTable(2, "t2"),
	)
	assert.NotNil(err)

	// check failed should have no effects
	got, exists := s.sctx.GetSessionVars().LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByID(1)
	assert.True(exists)
	assert.Equal(got, tbl1)
	val, err := s.sctx.GetSessionVars().TemporaryTableData.Get(context.Background(), k)
	assert.Nil(err)
	assert.Equal([]byte("v1"), val)

	// truncate success
	tb1x := newMockTable(2, "t1")
	err = s.mgr.ReplaceAndTruncateLocalTemporaryTable(
		ast.Ident{Schema: model.NewCIStr("db1"), Name: model.NewCIStr("t1")},
		tb1x,
	)
	assert.Nil(err)
	got, exists = s.sctx.GetSessionVars().LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByID(1)
	assert.False(exists)
	assert.Nil(got)
	got, exists = s.sctx.GetSessionVars().LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db1"), model.NewCIStr("t1"))
	assert.True(exists)
	assert.Equal(got, tb1x)
	val, err = s.sctx.GetSessionVars().TemporaryTableData.Get(context.Background(), k)
	assert.Nil(err)
	assert.Equal([]byte{}, val)
}

func newMockTable(tblID int64, tblName string) table.Table {
	c1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("c1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeLonglong)}
	c2 := &model.ColumnInfo{ID: 2, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeVarchar)}

	tblInfo := &model.TableInfo{ID: tblID, Name: model.NewCIStr(tblName), Columns: []*model.ColumnInfo{c1, c2}, PKIsHandle: true}
	return tables.MockTableFromMeta(tblInfo)
}
