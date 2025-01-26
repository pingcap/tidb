// Copyright 2019 PingCAP, Inc.
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

package ddl_test

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func getClonedTableInfoFromDomain(dbName string, tableName string, dom *domain.Domain) (*model.TableInfo, error) {
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr(tableName))
	if err != nil {
		return nil, err
	}
	return tbl.Meta().Clone(), nil
}

func TestCreateTableWithModeInfo(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	de := domain.DDLExecutor()
	tk := testkit.NewTestKit(t, store)

	ctx := testkit.NewTestKit(t, store).Session()

	// init test
	tk.MustExec("use test")
	tk.MustExec("create table t1(id int)")

	// get cloned table info for creating new table t1_restore
	tblInfo, err := getClonedTableInfoFromDomain("test", "t1", domain)
	require.NoError(t, err)

	// For testing create table as ModeRestore
	tblInfo.Name = ast.NewCIStr("t1_restore")
	tblInfo.TableMode = model.TableModeRestore
	err = de.CreateTableWithInfo(tk.Session(), ast.NewCIStr("test"), tblInfo, nil, ddl.WithOnExist(ddl.OnExistIgnore))
	require.NoError(t, err)
	dbInfo, ok := domain.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	checkTableModeTest(t, store, dbInfo, tblInfo, model.TableModeRestore)

	// For testing select is not allowed when table is in ModeImport
	// TODO(xiaoyuan): currently the error is very ugly like:
	// [schema:8020]Table 't1_restore' was locked in %!s(MISSING) by %!v(MISSING)
	// this is because infoschema.ErrTableModeRestore is based on mysql.ErrTableLocked,
	// how to make it look nice? or shoule I define a new mysql error?
	tk.MustGetErrCode("select * from t1_restore", 8020)

	// For testing insert is not allowed when table is in ModeImport
	tk.MustGetErrCode("insert into t1_restore values(1)", 8020)

	// TODO(xiaoyuan): extract as function and reuse the code
	// For testing AlterTable from ModeRestore to ModeImport is not allowed
	args := &model.AlterTableModeArgs{
		TableMode: model.TableModeImport,
		SchemaID:  dbInfo.ID,
		TableID:   tblInfo.ID,
	}
	job := &model.Job{
		Version:    model.JobVersion2,
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAlterTableMode,
		BinlogInfo: &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{
				Database: dbInfo.Name.L,
				Table:    tblInfo.Name.L,
			},
		},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = de.(ddl.ExecutorForTest).DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true))
	// TODO(xiaoyuan): consider use different error code for transition error and not-accessible error
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	require.True(t, ok)
	sqlErr := terror.ToSQLError(tErr)
	require.Equal(t, 8020, int(sqlErr.Code))

	// For testing AlterTableMode from ModeRestore to ModeNormal
	args = &model.AlterTableModeArgs{
		TableMode: model.TableModeNormal,
		SchemaID:  dbInfo.ID,
		TableID:   tblInfo.ID,
	}
	job = &model.Job{
		Version:    model.JobVersion2,
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAlterTableMode,
		BinlogInfo: &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{
				Database: dbInfo.Name.L,
				Table:    tblInfo.Name.L,
			},
		},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = de.(ddl.ExecutorForTest).DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true))
	testCheckTableState(t, store, dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)
	checkTableModeTest(t, store, dbInfo, tblInfo, model.TableModeNormal)

	// For testing AlterTableMode from ModeNormal to ModeRestore
	args = &model.AlterTableModeArgs{
		TableMode: model.TableModeImport,
		SchemaID:  dbInfo.ID,
		TableID:   tblInfo.ID,
	}
	job = &model.Job{
		Version:    model.JobVersion2,
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAlterTableMode,
		BinlogInfo: &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{
				Database: dbInfo.Name.L,
				Table:    tblInfo.Name.L,
			},
		},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = de.(ddl.ExecutorForTest).DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true))
	testCheckTableState(t, store, dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)
	checkTableModeTest(t, store, dbInfo, tblInfo, model.TableModeImport)

	// TODO: batch create tables with ModeRestore test
}
