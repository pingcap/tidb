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

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
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

type TemporaryTableDDLSuite struct {
	suite.Suite
	sctx sessionctx.Context
	ddl  *temporaryTableDDL
}

func TestTemporaryTableDDLSuit(t *testing.T) {
	suite.Run(t, new(TemporaryTableDDLSuite))
}

func (s *TemporaryTableDDLSuite) SetupTest() {
	store, err := mockstore.NewMockStore()
	assert.Nil(s.T(), err)

	sctx := mock.NewContext()
	sctx.Store = store

	s.sctx = sctx
	s.ddl = GetTemporaryTableDDL(sctx).(*temporaryTableDDL)
}

func (s *TemporaryTableDDLSuite) TearDownTest() {
	assert.Nil(s.T(), s.sctx.GetStore().Close())
}

func (s *TemporaryTableDDLSuite) TestAddLocalTemporaryTable() {
	assert := assert.New(s.T())
	sessVars := s.sctx.GetSessionVars()

	tbl1 := newMockTable("t1")
	tbl2 := newMockTable("t2")

	assert.Nil(sessVars.LocalTemporaryTables)
	assert.Nil(sessVars.TemporaryTableData)

	// insert t1
	err := s.ddl.CreateLocalTemporaryTable(model.NewCIStr("db1"), tbl1)
	assert.Nil(err)
	assert.NotNil(sessVars.LocalTemporaryTables)
	assert.NotNil(sessVars.TemporaryTableData)
	assert.Equal(int64(1), tbl1.ID)
	got, exists := sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db1"), model.NewCIStr("t1"))
	assert.True(exists)
	assert.Equal(got.Meta(), tbl1)

	// insert t2 with data
	err = s.ddl.CreateLocalTemporaryTable(model.NewCIStr("db1"), tbl2)
	assert.Nil(err)
	assert.Equal(int64(2), tbl2.ID)
	got, exists = sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db1"), model.NewCIStr("t2"))
	assert.True(exists)
	assert.Equal(got.Meta(), tbl2)

	// should success to set a key for a table
	k := tablecodec.EncodeRowKeyWithHandle(tbl1.ID, kv.IntHandle(1))
	err = sessVars.TemporaryTableData.SetTableKey(tbl1.ID, k, []byte("v1"))
	assert.Nil(err)

	val, err := sessVars.TemporaryTableData.Get(context.Background(), k)
	assert.Nil(err)
	assert.Equal([]byte("v1"), val)

	// insert dup table
	tbl1x := newMockTable("t1")
	err = s.ddl.CreateLocalTemporaryTable(model.NewCIStr("db1"), tbl1x)
	assert.True(infoschema.ErrTableExists.Equal(err))
	got, exists = sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db1"), model.NewCIStr("t1"))
	assert.True(exists)
	assert.Equal(got.Meta(), tbl1)

	// insert should be success for same table name in different db
	err = s.ddl.CreateLocalTemporaryTable(model.NewCIStr("db2"), tbl1x)
	assert.Nil(err)
	got, exists = sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db2"), model.NewCIStr("t1"))
	assert.Equal(int64(4), got.Meta().ID)
	assert.True(exists)
	assert.Equal(got.Meta(), tbl1x)

	// tbl1 still exist
	got, exists = sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db1"), model.NewCIStr("t1"))
	assert.True(exists)
	assert.Equal(got.Meta(), tbl1)
}

func (s *TemporaryTableDDLSuite) TestRemoveLocalTemporaryTable() {
	assert := assert.New(s.T())
	sessVars := s.sctx.GetSessionVars()

	// remove when empty
	err := s.ddl.DropLocalTemporaryTable(model.NewCIStr("db1"), model.NewCIStr("t1"))
	assert.True(infoschema.ErrTableNotExists.Equal(err))

	// add one table
	tbl1 := newMockTable("t1")
	err = s.ddl.CreateLocalTemporaryTable(model.NewCIStr("db1"), tbl1)
	assert.Nil(err)
	assert.Equal(int64(1), tbl1.ID)
	k := tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(1))
	err = sessVars.TemporaryTableData.SetTableKey(tbl1.ID, k, []byte("v1"))
	assert.Nil(err)

	// remove failed when table not found
	err = s.ddl.DropLocalTemporaryTable(model.NewCIStr("db1"), model.NewCIStr("t2"))
	assert.True(infoschema.ErrTableNotExists.Equal(err))

	// remove failed when table not found (same table name in different db)
	err = s.ddl.DropLocalTemporaryTable(model.NewCIStr("db2"), model.NewCIStr("t1"))
	assert.True(infoschema.ErrTableNotExists.Equal(err))

	// check failed remove should have no effects
	got, exists := sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByID(tbl1.ID)
	assert.True(exists)
	assert.Equal(got.Meta(), tbl1)
	val, err := sessVars.TemporaryTableData.Get(context.Background(), k)
	assert.Nil(err)
	assert.Equal([]byte("v1"), val)

	// remove success
	err = s.ddl.DropLocalTemporaryTable(model.NewCIStr("db1"), model.NewCIStr("t1"))
	assert.Nil(err)
	got, exists = sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db1"), model.NewCIStr("t1"))
	assert.Nil(got)
	assert.False(exists)
	val, err = sessVars.TemporaryTableData.Get(context.Background(), k)
	assert.Nil(err)
	assert.Equal([]byte{}, val)
}

func (s *TemporaryTableDDLSuite) TestTruncateLocalTemporaryTable() {
	assert := assert.New(s.T())
	sessVars := s.sctx.GetSessionVars()

	// truncate when empty
	err := s.ddl.TruncateLocalTemporaryTable(model.NewCIStr("db1"), model.NewCIStr("t1"))
	assert.True(infoschema.ErrTableNotExists.Equal(err))
	assert.Nil(sessVars.LocalTemporaryTables)
	assert.Nil(sessVars.TemporaryTableData)

	// add one table
	tbl1 := newMockTable("t1")
	err = s.ddl.CreateLocalTemporaryTable(model.NewCIStr("db1"), tbl1)
	assert.Equal(int64(1), tbl1.ID)
	assert.Nil(err)
	k := tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(1))
	err = sessVars.TemporaryTableData.SetTableKey(1, k, []byte("v1"))
	assert.Nil(err)

	// truncate failed for table not exist
	err = s.ddl.TruncateLocalTemporaryTable(model.NewCIStr("db1"), model.NewCIStr("t2"))
	assert.True(infoschema.ErrTableNotExists.Equal(err))
	err = s.ddl.TruncateLocalTemporaryTable(model.NewCIStr("db2"), model.NewCIStr("t1"))
	assert.True(infoschema.ErrTableNotExists.Equal(err))

	// check failed should have no effects
	got, exists := sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db1"), model.NewCIStr("t1"))
	assert.True(exists)
	assert.Equal(got.Meta(), tbl1)
	val, err := sessVars.TemporaryTableData.Get(context.Background(), k)
	assert.Nil(err)
	assert.Equal([]byte("v1"), val)

	// insert a new tbl
	tbl2 := newMockTable("t2")
	err = s.ddl.CreateLocalTemporaryTable(model.NewCIStr("db1"), tbl2)
	assert.Equal(int64(2), tbl2.ID)
	assert.Nil(err)
	k2 := tablecodec.EncodeRowKeyWithHandle(2, kv.IntHandle(1))
	err = sessVars.TemporaryTableData.SetTableKey(2, k2, []byte("v2"))
	assert.Nil(err)

	// truncate success
	err = s.ddl.TruncateLocalTemporaryTable(model.NewCIStr("db1"), model.NewCIStr("t1"))
	assert.Nil(err)
	got, exists = sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db1"), model.NewCIStr("t1"))
	assert.True(exists)
	assert.NotEqual(got.Meta(), tbl1)
	assert.Equal(int64(3), got.Meta().ID)
	val, err = sessVars.TemporaryTableData.Get(context.Background(), k)
	assert.Nil(err)
	assert.Equal([]byte{}, val)

	// truncate just effect its own data
	val, err = sessVars.TemporaryTableData.Get(context.Background(), k2)
	assert.Nil(err)
	assert.Equal([]byte("v2"), val)
}

func newMockTable(tblName string) *model.TableInfo {
	c1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("c1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeLonglong)}
	c2 := &model.ColumnInfo{ID: 2, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeVarchar)}

	tblInfo := &model.TableInfo{Name: model.NewCIStr(tblName), Columns: []*model.ColumnInfo{c1, c2}, PKIsHandle: true}
	return tblInfo
}
