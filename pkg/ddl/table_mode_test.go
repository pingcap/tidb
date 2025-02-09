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
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
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

func setTableModeTest(ctx sessionctx.Context, t *testing.T, store kv.Storage, de ddl.ExecutorForTest, dbInfo *model.DBInfo, tblInfo *model.TableInfo, mode model.TableModeState) error {
	args := &model.AlterTableModeArgs{
		TableMode: mode,
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
	err := de.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true))

	if err == nil {
		testCheckTableState(t, store, dbInfo, tblInfo, model.StatePublic)
		testCheckJobDone(t, store, job.ID, true)
		checkTableModeTest(t, store, dbInfo, tblInfo, mode)
	}

	return err
}

// TODO(xiaoyuan): consider use different error code for transition error and not-accessible error
func checkErrorCode(t *testing.T, err error, expected int) {
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	require.True(t, ok)
	sqlErr := terror.ToSQLError(tErr)
	require.Equal(t, expected, int(sqlErr.Code))
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
	tk.MustGetErrCode("select * from t1_restore", errno.ErrProtectedTableMode)

	// For testing insert is not allowed when table is in ModeImport
	tk.MustGetErrCode("insert into t1_restore values(1)", errno.ErrProtectedTableMode)

	// For testing accessing table metadata is allowed when table is in ModeRestore
	tk.MustExec("show create table t1_restore")
	tk.MustExec("describe t1_restore")

	// For testing AlterTable ModeRestore -> ModeImport is not allowed
	err = setTableModeTest(ctx, t, store, de.(ddl.ExecutorForTest), dbInfo, tblInfo, model.TableModeImport)
	checkErrorCode(t, err, errno.ErrInvalidTableModeConversion)

	// For testing AlterTableMode ModeRestore -> ModeNormal
	err = setTableModeTest(ctx, t, store, de.(ddl.ExecutorForTest), dbInfo, tblInfo, model.TableModeNormal)
	require.NoError(t, err)

	// For testing AlterTableMode ModeNormal -> ModeRestore
	err = setTableModeTest(ctx, t, store, de.(ddl.ExecutorForTest), dbInfo, tblInfo, model.TableModeRestore)
	require.NoError(t, err)

	// For testing batch create tables with info
	var tblInfo1, tblInfo2, tblInfo3 *model.TableInfo
	tblInfo1, err = getClonedTableInfoFromDomain("test", "t1", domain)
	tblInfo1.Name = ast.NewCIStr("t1_1")
	tblInfo1.TableMode = model.TableModeNormal
	tblInfo2, err = getClonedTableInfoFromDomain("test", "t1", domain)
	tblInfo2.Name = ast.NewCIStr("t1_2")
	tblInfo2.TableMode = model.TableModeImport
	tblInfo3, err = getClonedTableInfoFromDomain("test", "t1", domain)
	tblInfo3.Name = ast.NewCIStr("t1_3")
	tblInfo3.TableMode = model.TableModeRestore
	err = de.BatchCreateTableWithInfo(
		ctx,
		ast.NewCIStr("test"),
		[]*model.TableInfo{tblInfo1, tblInfo2, tblInfo3},
		ddl.WithOnExist(ddl.OnExistIgnore),
	)
	checkTableModeTest(t, store, dbInfo, tblInfo1, model.TableModeNormal)
	checkTableModeTest(t, store, dbInfo, tblInfo2, model.TableModeImport)
	checkTableModeTest(t, store, dbInfo, tblInfo3, model.TableModeRestore)
}
