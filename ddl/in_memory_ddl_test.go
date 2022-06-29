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
