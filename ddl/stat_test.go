// Copyright 2015 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
<<<<<<< HEAD
=======
	"github.com/pingcap/tidb/ddl/internal/callback"
	"github.com/pingcap/tidb/kv"
>>>>>>> b7330bdc15f (*: fix bug that table name in 'admin show ddl jobs' is missing for ongoing drop table operation (#42904))
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestDDLStatsInfo(t *testing.T) {
	store, domain, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	defer clean()
	d := domain.DDL()

	dbInfo, err := testSchemaInfo(store, "test_stat")
	require.NoError(t, err)
	testCreateSchema(t, testkit.NewTestKit(t, store).Session(), d, dbInfo)
	tblInfo, err := testTableInfo(store, "t", 2)
	require.NoError(t, err)
	ctx := testkit.NewTestKit(t, store).Session()
	testCreateTable(t, ctx, d, dbInfo, tblInfo)

	m := testGetTable(t, domain, tblInfo.ID)
	// insert t values (1, 1), (2, 2), (3, 3)
	_, err = m.AddRecord(ctx, types.MakeDatums(1, 1))
	require.NoError(t, err)
	_, err = m.AddRecord(ctx, types.MakeDatums(2, 2))
	require.NoError(t, err)
	_, err = m.AddRecord(ctx, types.MakeDatums(3, 3))
	require.NoError(t, err)
	require.NoError(t, ctx.CommitTxn(context.Background()))

	job := buildCreateIdxJob(dbInfo, tblInfo, true, "idx", "c1")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum"))
	}()

	done := make(chan error, 1)
	go func() {
		ctx.SetValue(sessionctx.QueryString, "skip")
		done <- d.DoDDLJob(ctx, job)
	}()

	exit := false
	// a copy of ddl.ddlJobReorgHandle
	ddlJobReorgHandle := "ddl_job_reorg_handle"
	for !exit {
		select {
		case err := <-done:
			require.NoError(t, err)
			exit = true
		case wg := <-ddl.TestCheckWorkerNumCh:
			varMap, err := d.Stats(nil)
			wg.Done()
			require.NoError(t, err)
			require.Equal(t, varMap[ddlJobReorgHandle], "1")
		}
	}
}

func buildCreateIdxJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, unique bool, indexName string, colName string) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args: []interface{}{unique, model.NewCIStr(indexName),
			[]*ast.IndexPartSpecification{{
				Column: &ast.ColumnName{Name: model.NewCIStr(colName)},
				Length: types.UnspecifiedLength}}},
	}
}

func TestIssue42268(t *testing.T) {
	// issue 42268 missing table name in 'admin show ddl' result during drop table
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_0")
	tk.MustExec("create table t_0 (c1 int, c2 int)")

	tbl := external.GetTableByName(t, tk, "test", "t_0")
	require.NotNil(t, tbl)
	require.Equal(t, 2, len(tbl.Cols()))

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	hook := &callback.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if tbl.Meta().ID != job.TableID {
			return
		}
		switch job.SchemaState {
		case model.StateNone:
		case model.StateDeleteOnly, model.StateWriteOnly, model.StateWriteReorganization:
			rs := tk1.MustQuery("admin show ddl jobs")
			tblName := fmt.Sprintf("%s", rs.Rows()[0][2])
			require.Equal(t, tblName, "t_0")
		}
	}
	dom.DDL().SetHook(hook)

	tk.MustExec("drop table t_0")
}
