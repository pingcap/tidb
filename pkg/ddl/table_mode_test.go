// Copyright 2025 PingCAP, Inc.
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
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func getClonedTableInfoFromDomain(
	t *testing.T,
	dbName string,
	tableName string,
	dom *domain.Domain,
) *model.TableInfo {
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr(tableName))
	require.NoError(t, err)
	return tbl.Meta().Clone()
}

func setTableModeTest(
	ctx sessionctx.Context,
	t *testing.T,
	store kv.Storage,
	de ddl.ExecutorForTest,
	dbInfo *model.DBInfo,
	tblInfo *model.TableInfo,
	mode model.TableModeState,
) error {
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
		v := getSchemaVer(t, ctx)
		checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})

		testCheckTableState(t, store, dbInfo, tblInfo, model.StatePublic)
		testCheckJobDone(t, store, job.ID, true)
		checkTableModeTest(t, store, dbInfo, tblInfo, mode)
	}

	return err
}

func checkTableModeTest(t *testing.T, store kv.Storage, dbInfo *model.DBInfo, tblInfo *model.TableInfo, mode model.TableModeState) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		tt := meta.NewMutator(txn)
		info, err := tt.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, mode, info.TableMode)
		return nil
	})
	require.NoError(t, err)
}

func checkErrorCode(t *testing.T, err error, expected int) {
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	require.True(t, ok)
	sqlErr := terror.ToSQLError(tErr)
	require.Equal(t, expected, int(sqlErr.Code))
}

func TestTableModeBasic(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	de := domain.DDLExecutor()
	tk := testkit.NewTestKit(t, store)

	ctx := testkit.NewTestKit(t, store).Session()

	// init test
	tk.MustExec("use test")
	tk.MustExec("create table t1(id int)")

	// get cloned table info for creating new table t1_restore
	tblInfo := getClonedTableInfoFromDomain(t, "test", "t1", domain)

	// For testing create table as ModeRestore
	tblInfo.Name = ast.NewCIStr("t1_restore")
	tblInfo.TableMode = model.TableModeRestore
	err := de.CreateTableWithInfo(tk.Session(), ast.NewCIStr("test"), tblInfo, nil, ddl.WithOnExist(ddl.OnExistIgnore))
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
	checkErrorCode(t, err, errno.ErrInvalidTableModeSet)

	// For testing AlterTableMode ModeRestore -> ModeNormal
	err = setTableModeTest(ctx, t, store, de.(ddl.ExecutorForTest), dbInfo, tblInfo, model.TableModeNormal)
	require.NoError(t, err)

	// For testing AlterTableMode ModeNormal -> ModeRestore
	err = setTableModeTest(ctx, t, store, de.(ddl.ExecutorForTest), dbInfo, tblInfo, model.TableModeRestore)
	require.NoError(t, err)

	// For testing batch create tables with info
	var tblInfo1, tblInfo2, tblInfo3 *model.TableInfo
	tblInfo1 = getClonedTableInfoFromDomain(t, "test", "t1", domain)
	tblInfo1.Name = ast.NewCIStr("t1_1")
	tblInfo1.TableMode = model.TableModeNormal
	tblInfo2 = getClonedTableInfoFromDomain(t, "test", "t1", domain)
	tblInfo2.Name = ast.NewCIStr("t1_2")
	tblInfo2.TableMode = model.TableModeImport
	tblInfo3 = getClonedTableInfoFromDomain(t, "test", "t1", domain)
	tblInfo3.Name = ast.NewCIStr("t1_3")
	tblInfo3.TableMode = model.TableModeRestore
	err = de.BatchCreateTableWithInfo(
		ctx,
		ast.NewCIStr("test"),
		[]*model.TableInfo{tblInfo1, tblInfo2, tblInfo3},
		ddl.WithOnExist(ddl.OnExistIgnore),
	)
	require.NoError(t, err)
	checkTableModeTest(t, store, dbInfo, tblInfo1, model.TableModeNormal)
	checkTableModeTest(t, store, dbInfo, tblInfo2, model.TableModeImport)
	checkTableModeTest(t, store, dbInfo, tblInfo3, model.TableModeRestore)
}

func TestTableModeConcurrent(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	de := domain.DDLExecutor()
	tk := testkit.NewTestKit(t, store)
	ctx := testkit.NewTestKit(t, store).Session()

	tk.MustExec("use test")
	tk.MustExec("create table t1(id int)")
	dbInfo, ok := domain.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)

	// Concurrency test1: concurrently alter t1 to ModeImport, expecting one success, one failure.
	t1Infos := []*model.TableInfo{
		getClonedTableInfoFromDomain(t, "test", "t1", domain),
		getClonedTableInfoFromDomain(t, "test", "t1", domain),
	}
	var wg sync.WaitGroup
	wg.Add(len(t1Infos))
	errs := make(chan error, len(t1Infos))
	for _, info := range t1Infos {
		go func(info *model.TableInfo) {
			defer wg.Done()
			errs <- setTableModeTest(ctx, t, store, de.(ddl.ExecutorForTest), dbInfo, info, model.TableModeImport)
		}(info)
	}
	wg.Wait()
	close(errs)
	var successCount int
	var failedErr error
	for e := range errs {
		if e == nil {
			successCount++
		} else {
			failedErr = e
		}
	}
	require.Equal(t, 1, successCount)
	require.NotNil(t, failedErr)
	checkErrorCode(t, failedErr, errno.ErrInvalidTableModeSet)
	checkTableModeTest(t, store, dbInfo, t1Infos[0], model.TableModeImport)

	// Concurrency test2: concurrently alter t1 to ModeNormal, expecting both success.
	t1NormalInfos := []*model.TableInfo{
		getClonedTableInfoFromDomain(t, "test", "t1", domain),
		getClonedTableInfoFromDomain(t, "test", "t1", domain),
	}
	var wg2 sync.WaitGroup
	wg2.Add(len(t1NormalInfos))
	errs2 := make(chan error, len(t1NormalInfos))
	for _, info := range t1NormalInfos {
		go func(info *model.TableInfo) {
			defer wg2.Done()
			errs2 <- setTableModeTest(ctx, t, store, de.(ddl.ExecutorForTest), dbInfo, info, model.TableModeNormal)
		}(info)
	}
	wg2.Wait()
	close(errs2)
	for e := range errs2 {
		require.NoError(t, e)
	}
	checkTableModeTest(t, store, dbInfo, t1NormalInfos[0], model.TableModeNormal)

	// Concurrency test3: concurrently alter t1 to ModeRestore and ModeImport, expecting one success, one failure.
	modes := []model.TableModeState{
		model.TableModeRestore,
		model.TableModeImport,
	}
	clones := make([]*model.TableInfo, len(modes))
	for i := range modes {
		clones[i] = getClonedTableInfoFromDomain(t, "test", "t1", domain)
	}
	var wg3 sync.WaitGroup
	wg3.Add(len(modes))
	errs3 := make(chan error, len(modes))
	for i, mode := range modes {
		go func(clone *model.TableInfo, m model.TableModeState) {
			defer wg3.Done()
			errs3 <- setTableModeTest(ctx, t, store, de.(ddl.ExecutorForTest), dbInfo, clone, m)
		}(clones[i], mode)
	}
	wg3.Wait()
	close(errs3)
	var successCount3 int
	var failedErr3 error
	for e := range errs3 {
		if e == nil {
			successCount3++
		} else {
			failedErr3 = e
		}
	}
	require.Equal(t, 1, successCount3)
	require.NotNil(t, failedErr3)
	checkErrorCode(t, failedErr3, errno.ErrInvalidTableModeSet)
}
