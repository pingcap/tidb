// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package ddl

import (
	"testing"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestInfoSchemaLowerCaseTableNames(t *testing.T) {
	t.Parallel()

	dbName := model.NewCIStr("DBName")
	lowerDBName := model.NewCIStr("dbname")
	tableName := model.NewCIStr("TableName")
	lowerTableName := model.NewCIStr("tablename")
	dbInfo := &model.DBInfo{Name: dbName}
	tableInfo := &model.TableInfo{Name: tableName}

	// case-sensitive

	is := newInMemoryInfoSchema(0)
	is.putSchema(dbInfo)
	got := is.SchemaByName(dbName)
	require.NotNil(t, got)
	got = is.SchemaByName(lowerDBName)
	require.Nil(t, got)

	err := is.putTable(lowerDBName, tableInfo)
	require.True(t, infoschema.ErrDatabaseNotExists.Equal(err))
	err = is.putTable(dbName, tableInfo)
	require.NoError(t, err)
	got2, err := is.TableByName(dbName, tableName)
	require.NoError(t, err)
	require.NotNil(t, got2)
	got2, err = is.TableByName(lowerTableName, tableName)
	require.True(t, infoschema.ErrDatabaseNotExists.Equal(err))
	require.Nil(t, got2)
	got2, err = is.TableByName(dbName, lowerTableName)
	require.True(t, infoschema.ErrTableNotExists.Equal(err))
	require.Nil(t, got2)

	// compare-insensitive

	is = newInMemoryInfoSchema(2)
	is.putSchema(dbInfo)
	got = is.SchemaByName(dbName)
	require.NotNil(t, got)
	got = is.SchemaByName(lowerDBName)
	require.NotNil(t, got)
	require.Equal(t, dbName, got.Name)

	err = is.putTable(lowerDBName, tableInfo)
	require.NoError(t, err)
	got2, err = is.TableByName(dbName, tableName)
	require.NoError(t, err)
	require.NotNil(t, got2)
	got2, err = is.TableByName(dbName, lowerTableName)
	require.NoError(t, err)
	require.NotNil(t, got2)
	require.Equal(t, tableName, got2.Name)
}

func TestInfoSchemaDeleteTables(t *testing.T) {
	t.Parallel()

	is := newInMemoryInfoSchema(0)
	dbName1 := model.NewCIStr("DBName1")
	dbName2 := model.NewCIStr("DBName2")
	tableName1 := model.NewCIStr("TableName1")
	tableName2 := model.NewCIStr("TableName2")
	dbInfo1 := &model.DBInfo{Name: dbName1}
	dbInfo2 := &model.DBInfo{Name: dbName2}
	tableInfo1 := &model.TableInfo{Name: tableName1}
	tableInfo2 := &model.TableInfo{Name: tableName2}

	is.putSchema(dbInfo1)
	err := is.putTable(dbName1, tableInfo1)
	require.NoError(t, err)
	err = is.putTable(dbName1, tableInfo2)
	require.NoError(t, err)

	// db2 not created
	ok := is.deleteSchema(dbName2)
	require.False(t, ok)
	err = is.putTable(dbName2, tableInfo1)
	require.True(t, infoschema.ErrDatabaseNotExists.Equal(err))
	err = is.deleteTable(dbName2, tableName1)
	require.True(t, infoschema.ErrDatabaseNotExists.Equal(err))

	is.putSchema(dbInfo2)
	err = is.putTable(dbName2, tableInfo1)
	require.NoError(t, err)

	err = is.deleteTable(dbName2, tableName2)
	require.True(t, infoschema.ErrTableNotExists.Equal(err))
	err = is.deleteTable(dbName2, tableName1)
	require.NoError(t, err)

	// delete db will remove its tables
	ok = is.deleteSchema(dbName1)
	require.True(t, ok)
	_, err = is.TableByName(dbName1, tableName1)
	require.True(t, infoschema.ErrDatabaseNotExists.Equal(err))
}

func TestInMemoryDDL(t *testing.T) {
	t.Parallel()

	sctx := mock.NewContext()
	p := parser.New()

	testDDL := NewInMemoryDDL(2)
	dbInfo := testDDL.SchemaByName(model.NewCIStr("test"))
	require.Nil(t, dbInfo)

	err := testDDL.CreateSchema(sctx, &ast.CreateDatabaseStmt{Name: model.NewCIStr("test")})
	require.NoError(t, err)
	dbInfo = testDDL.SchemaByName(model.NewCIStr("test"))
	require.Equal(t, "test", dbInfo.Name.L)

	sql := "CREATE TABLE test.t1 (c INT PRIMARY KEY)"
	createAST, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = testDDL.CreateTable(sctx, createAST.(*ast.CreateTableStmt))
	require.NoError(t, err)

	tbl, err := testDDL.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	require.Len(t, tbl.Columns, 1)
	require.Equal(t, "c", tbl.Columns[0].Name.O)

	err = testDDL.DropTable(sctx, &ast.DropTableStmt{
		Tables: []*ast.TableName{{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t1")}},
	})
	require.NoError(t, err)
	_, err = testDDL.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.True(t, infoschema.ErrTableNotExists.Equal(err))

	err = testDDL.DropSchema(sctx, &ast.DropDatabaseStmt{Name: model.NewCIStr("test")})
	require.NoError(t, err)
}
