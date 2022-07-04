// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package ddl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
)

func TestInfoStoreLowerCaseTableNames(t *testing.T) {
	dbName := model.NewCIStr("DBName")
	lowerDBName := model.NewCIStr("dbname")
	tableName := model.NewCIStr("TableName")
	lowerTableName := model.NewCIStr("tablename")
	dbInfo := &model.DBInfo{Name: dbName}
	tableInfo := &model.TableInfo{Name: tableName}

	// case-sensitive

	is := newInfoStore(0)
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

	is = newInfoStore(2)
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

func TestInfoStoreDeleteTables(t *testing.T) {
	is := newInfoStore(0)
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

func mustCreateDatabase(t *testing.T, ddl SchemaTracker, sctx sessionctx.Context, dbName string) {
	dbInfo := ddl.SchemaByName(model.NewCIStr(dbName))
	require.Nil(t, dbInfo)

	err := ddl.CreateSchema(sctx, &ast.CreateDatabaseStmt{Name: model.NewCIStr(dbName)})
	require.NoError(t, err)
	dbInfo = ddl.SchemaByName(model.NewCIStr(dbName))
	require.Equal(t, dbName, dbInfo.Name.O)
}

func mustCreateTable(t *testing.T, ddl SchemaTracker, sctx sessionctx.Context, sql string) {
	p := parser.New()

	a, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	createAST := a.(*ast.CreateTableStmt)
	tbl, _ := ddl.TableByName(createAST.Table.Schema, createAST.Table.Name)
	require.Nil(t, tbl)

	err = ddl.CreateTable(sctx, createAST)
	require.NoError(t, err)
	tbl, err = ddl.TableByName(createAST.Table.Schema, createAST.Table.Name)
	require.NoError(t, err)
	require.NotNil(t, tbl)
}

func TestTrackerCreateDropTable(t *testing.T) {
	sctx := mock.NewContext()

	testDDL := NewSchemaTracker(2)
	mustCreateDatabase(t, testDDL, sctx, "test")

	sql := "CREATE TABLE test.t1 (c INT PRIMARY KEY)"
	mustCreateTable(t, testDDL, sctx, sql)

	err := testDDL.DropTable(sctx, &ast.DropTableStmt{
		Tables: []*ast.TableName{{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t1")}},
	})
	require.NoError(t, err)
	_, err = testDDL.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.True(t, infoschema.ErrTableNotExists.Equal(err))

	err = testDDL.DropSchema(sctx, &ast.DropDatabaseStmt{Name: model.NewCIStr("test")})
	require.NoError(t, err)
}

func TestTrackRenameTable(t *testing.T) {
	sctx := mock.NewContext()
	p := parser.New()

	testDDL := NewSchemaTracker(2)
	mustCreateDatabase(t, testDDL, sctx, "test")

	sql := "CREATE TABLE test.t1 (c INT PRIMARY KEY)"
	mustCreateTable(t, testDDL, sctx, sql)

	sql = "RENAME TABLE test.t1 TO test.t2"
	renameAST, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = testDDL.RenameTable(sctx, renameAST.(*ast.RenameTableStmt))
	require.NoError(t, err)

	tbl, err := testDDL.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	require.Equal(t, "t2", tbl.Name.L)

	sql = "CREATE TABLE test.t3 (c INT PRIMARY KEY)"
	mustCreateTable(t, testDDL, sctx, sql)

	sql = "RENAME TABLE test.t2 TO test.t1, test.t3 TO test.t2"
	renameAST, err = p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = testDDL.RenameTable(sctx, renameAST.(*ast.RenameTableStmt))
	require.NoError(t, err)

	// now have t1 and t2
	tbl, err = testDDL.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	require.Equal(t, "t2", tbl.Name.L)
	tbl, err = testDDL.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	require.Equal(t, "t1", tbl.Name.L)

	sql = "RENAME TABLE test.t1 TO test.t3, test.t2 TO test.t3"
	renameAST, err = p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = testDDL.RenameTable(sctx, renameAST.(*ast.RenameTableStmt))
	require.True(t, infoschema.ErrTableExists.Equal(err))
}

func TestTrackIndex(t *testing.T) {
	sctx := mock.NewContext()
	p := parser.New()

	testDDL := NewSchemaTracker(2)
	mustCreateDatabase(t, testDDL, sctx, "test")

	sql := "CREATE TABLE test.t1 (c INT)"
	mustCreateTable(t, testDDL, sctx, sql)

	sql = "CREATE UNIQUE INDEX uk_c ON test.t1 (c)"
	createAST, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = testDDL.CreateIndex(sctx, createAST.(*ast.CreateIndexStmt))
	require.NoError(t, err)

	tbl, err := testDDL.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	require.Len(t, tbl.Indices, 1)
	require.Equal(t, "uk_c", tbl.Indices[0].Name.O)

	sql = "DROP INDEX uk_c ON test.t1"
	dropAST, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = testDDL.DropIndex(sctx, dropAST.(*ast.DropIndexStmt))
	require.NoError(t, err)

	tbl, err = testDDL.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	require.Len(t, tbl.Indices, 0)
}

func TestAlterTableChangeColumn(t *testing.T) {
	ctx := context.Background()
	sctx := mock.NewContext()
	p := parser.New()

	testDDL := NewSchemaTracker(2)
	mustCreateDatabase(t, testDDL, sctx, "test")

	sql := "CREATE TABLE test.t1 (a INT)"
	mustCreateTable(t, testDDL, sctx, sql)

	sql = "ALTER TABLE test.t1 ADD b TEXT(100)"
	alterAST, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = testDDL.AlterTable(ctx, sctx, alterAST.(*ast.AlterTableStmt))
	require.NoError(t, err)

	tblInfo, err := testDDL.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)

	sql = "ALTER TABLE test.t1 CHANGE COLUMN b b TEXT(10000000)"
	alterAST, err = p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	err = testDDL.AlterTable(ctx, sctx, alterAST.(*ast.AlterTableStmt))
	require.NoError(t, err)

	tblInfo, err = testDDL.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)

	// CHANGE COLUMN b b TEXT(10000000) will generate LONGTEXT
	require.Equal(t, "longtext", tblInfo.Columns[1].GetTypeDesc())
}
