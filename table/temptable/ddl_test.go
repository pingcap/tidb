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

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func createTestSuite(t *testing.T) (sessionctx.Context, *temporaryTableDDL, func()) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)

	sctx := mock.NewContext()
	sctx.Store = store
	ddl := GetTemporaryTableDDL(sctx).(*temporaryTableDDL)
	clean := func() {
		require.NoError(t, store.Close())
	}

	return sctx, ddl, clean
}

func TestAddLocalTemporaryTable(t *testing.T) {
	sctx, ddl, clean := createTestSuite(t)
	defer clean()

	sessVars := sctx.GetSessionVars()

	db1 := newMockSchema("db1")
	db2 := newMockSchema("db2")
	tbl1 := newMockTable("t1")
	tbl2 := newMockTable("t2")

	require.Nil(t, sessVars.LocalTemporaryTables)
	require.Nil(t, sessVars.TemporaryTableData)

	// insert t1
	err := ddl.CreateLocalTemporaryTable(db1, tbl1)
	require.NoError(t, err)
	require.NotNil(t, sessVars.LocalTemporaryTables)
	require.NotNil(t, sessVars.TemporaryTableData)
	require.Equal(t, int64(1), tbl1.ID)
	got, exists := sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db1"), model.NewCIStr("t1"))
	require.True(t, exists)
	require.Equal(t, got.Meta(), tbl1)

	// insert t2 with data
	err = ddl.CreateLocalTemporaryTable(db1, tbl2)
	require.NoError(t, err)
	require.Equal(t, int64(2), tbl2.ID)
	got, exists = sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db1"), model.NewCIStr("t2"))
	require.True(t, exists)
	require.Equal(t, got.Meta(), tbl2)

	// should success to set a key for a table
	k := tablecodec.EncodeRowKeyWithHandle(tbl1.ID, kv.IntHandle(1))
	err = sessVars.TemporaryTableData.SetTableKey(tbl1.ID, k, []byte("v1"))
	require.NoError(t, err)

	val, err := sessVars.TemporaryTableData.Get(context.Background(), k)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)

	// insert dup table
	tbl1x := newMockTable("t1")
	err = ddl.CreateLocalTemporaryTable(db1, tbl1x)
	require.True(t, infoschema.ErrTableExists.Equal(err))
	got, exists = sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db1"), model.NewCIStr("t1"))
	require.True(t, exists)
	require.Equal(t, got.Meta(), tbl1)

	// insert should be success for same table name in different db
	err = ddl.CreateLocalTemporaryTable(db2, tbl1x)
	require.NoError(t, err)
	got, exists = sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db2"), model.NewCIStr("t1"))
	require.Equal(t, int64(4), got.Meta().ID)
	require.True(t, exists)
	require.Equal(t, got.Meta(), tbl1x)

	// tbl1 still exist
	got, exists = sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db1"), model.NewCIStr("t1"))
	require.True(t, exists)
	require.Equal(t, got.Meta(), tbl1)
}

func TestRemoveLocalTemporaryTable(t *testing.T) {
	sctx, ddl, clean := createTestSuite(t)
	defer clean()

	sessVars := sctx.GetSessionVars()
	db1 := newMockSchema("db1")

	// remove when empty
	err := ddl.DropLocalTemporaryTable(model.NewCIStr("db1"), model.NewCIStr("t1"))
	require.True(t, infoschema.ErrTableNotExists.Equal(err))

	// add one table
	tbl1 := newMockTable("t1")
	err = ddl.CreateLocalTemporaryTable(db1, tbl1)
	require.NoError(t, err)
	require.Equal(t, int64(1), tbl1.ID)
	k := tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(1))
	err = sessVars.TemporaryTableData.SetTableKey(tbl1.ID, k, []byte("v1"))
	require.NoError(t, err)

	// remove failed when table not found
	err = ddl.DropLocalTemporaryTable(model.NewCIStr("db1"), model.NewCIStr("t2"))
	require.True(t, infoschema.ErrTableNotExists.Equal(err))

	// remove failed when table not found (same table name in different db)
	err = ddl.DropLocalTemporaryTable(model.NewCIStr("db2"), model.NewCIStr("t1"))
	require.True(t, infoschema.ErrTableNotExists.Equal(err))

	// check failed remove should have no effects
	got, exists := sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByID(tbl1.ID)
	require.True(t, exists)
	require.Equal(t, got.Meta(), tbl1)
	val, err := sessVars.TemporaryTableData.Get(context.Background(), k)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)

	// remove success
	err = ddl.DropLocalTemporaryTable(model.NewCIStr("db1"), model.NewCIStr("t1"))
	require.NoError(t, err)
	got, exists = sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db1"), model.NewCIStr("t1"))
	require.Nil(t, got)
	require.False(t, exists)
	val, err = sessVars.TemporaryTableData.Get(context.Background(), k)
	require.NoError(t, err)
	require.Equal(t, []byte{}, val)
}

func TestTruncateLocalTemporaryTable(t *testing.T) {
	sctx, ddl, clean := createTestSuite(t)
	defer clean()

	sessVars := sctx.GetSessionVars()
	db1 := newMockSchema("db1")

	// truncate when empty
	err := ddl.TruncateLocalTemporaryTable(model.NewCIStr("db1"), model.NewCIStr("t1"))
	require.True(t, infoschema.ErrTableNotExists.Equal(err))
	require.Nil(t, sessVars.LocalTemporaryTables)
	require.Nil(t, sessVars.TemporaryTableData)

	// add one table
	tbl1 := newMockTable("t1")
	err = ddl.CreateLocalTemporaryTable(db1, tbl1)
	require.Equal(t, int64(1), tbl1.ID)
	require.NoError(t, err)
	k := tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(1))
	err = sessVars.TemporaryTableData.SetTableKey(1, k, []byte("v1"))
	require.NoError(t, err)

	// truncate failed for table not exist
	err = ddl.TruncateLocalTemporaryTable(model.NewCIStr("db1"), model.NewCIStr("t2"))
	require.True(t, infoschema.ErrTableNotExists.Equal(err))
	err = ddl.TruncateLocalTemporaryTable(model.NewCIStr("db2"), model.NewCIStr("t1"))
	require.True(t, infoschema.ErrTableNotExists.Equal(err))

	// check failed should have no effects
	got, exists := sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db1"), model.NewCIStr("t1"))
	require.True(t, exists)
	require.Equal(t, got.Meta(), tbl1)
	val, err := sessVars.TemporaryTableData.Get(context.Background(), k)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)

	// insert a new tbl
	tbl2 := newMockTable("t2")
	err = ddl.CreateLocalTemporaryTable(db1, tbl2)
	require.Equal(t, int64(2), tbl2.ID)
	require.NoError(t, err)
	k2 := tablecodec.EncodeRowKeyWithHandle(2, kv.IntHandle(1))
	err = sessVars.TemporaryTableData.SetTableKey(2, k2, []byte("v2"))
	require.NoError(t, err)

	// truncate success
	err = ddl.TruncateLocalTemporaryTable(model.NewCIStr("db1"), model.NewCIStr("t1"))
	require.NoError(t, err)
	got, exists = sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables).TableByName(model.NewCIStr("db1"), model.NewCIStr("t1"))
	require.True(t, exists)
	require.NotEqual(t, got.Meta(), tbl1)
	require.Equal(t, int64(3), got.Meta().ID)
	val, err = sessVars.TemporaryTableData.Get(context.Background(), k)
	require.NoError(t, err)
	require.Equal(t, []byte{}, val)

	// truncate just effect its own data
	val, err = sessVars.TemporaryTableData.Get(context.Background(), k2)
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), val)
}

func newMockTable(tblName string) *model.TableInfo {
	c1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("c1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeLonglong)}
	c2 := &model.ColumnInfo{ID: 2, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeVarchar)}

	tblInfo := &model.TableInfo{Name: model.NewCIStr(tblName), Columns: []*model.ColumnInfo{c1, c2}, PKIsHandle: true}
	return tblInfo
}

func newMockSchema(schemaName string) *model.DBInfo {
	return &model.DBInfo{ID: 10, Name: model.NewCIStr(schemaName), State: model.StatePublic}
}
