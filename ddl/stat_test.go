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

package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
<<<<<<< HEAD
	"github.com/pingcap/tidb/sessionctx"
=======
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/internal/callback"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
>>>>>>> b7330bdc15f (*: fix bug that table name in 'admin show ddl jobs' is missing for ongoing drop table operation (#42904))
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestDDLStatsInfo(t *testing.T) {
	store := createMockStore(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Stop())
	}()

	dbInfo, err := testSchemaInfo(d, "test_stat")
	require.NoError(t, err)
	testCreateSchema(t, testNewContext(d), d, dbInfo)
	tblInfo, err := testTableInfo(d, "t", 2)
	require.NoError(t, err)
	ctx := testNewContext(d)
	testCreateTable(t, ctx, d, dbInfo, tblInfo)

	m := testGetTable(t, d, dbInfo.ID, tblInfo.ID)
	// insert t values (1, 1), (2, 2), (3, 3)
	_, err = m.AddRecord(ctx, types.MakeDatums(1, 1))
	require.NoError(t, err)
	_, err = m.AddRecord(ctx, types.MakeDatums(2, 2))
	require.NoError(t, err)
	_, err = m.AddRecord(ctx, types.MakeDatums(3, 3))
	require.NoError(t, err)
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	job := buildCreateIdxJob(dbInfo, tblInfo, true, "idx", "c1")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum"))
	}()

	done := make(chan error, 1)
	go func() {
		ctx.SetValue(sessionctx.QueryString, "skip")
		done <- d.doDDLJob(ctx, job)
	}()

	exit := false
	for !exit {
		select {
		case err := <-done:
			require.NoError(t, err)
			exit = true
		case wg := <-TestCheckWorkerNumCh:
			varMap, err := d.Stats(nil)
			wg.Done()
			require.NoError(t, err)
			require.Equal(t, varMap[ddlJobReorgHandle], "1")
		}
	}
}
<<<<<<< HEAD
=======

func TestGetDDLInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	sess := tk.Session()
	tk.MustExec("begin")
	txn, err := sess.Txn(true)
	require.NoError(t, err)

	dbInfo2 := &model.DBInfo{
		ID:    2,
		Name:  model.NewCIStr("b"),
		State: model.StateNone,
	}
	job := &model.Job{
		ID:       1,
		SchemaID: dbInfo2.ID,
		Type:     model.ActionCreateSchema,
		RowCount: 0,
	}
	job1 := &model.Job{
		ID:       2,
		SchemaID: dbInfo2.ID,
		Type:     model.ActionAddIndex,
		RowCount: 0,
	}

	err = addDDLJobs(sess, txn, job)
	require.NoError(t, err)

	info, err := ddl.GetDDLInfo(sess)
	require.NoError(t, err)
	require.Len(t, info.Jobs, 1)
	require.Equal(t, job, info.Jobs[0])
	require.Nil(t, info.ReorgHandle)

	// two jobs
	err = addDDLJobs(sess, txn, job1)
	require.NoError(t, err)

	info, err = ddl.GetDDLInfo(sess)
	require.NoError(t, err)
	require.Len(t, info.Jobs, 2)
	require.Equal(t, job, info.Jobs[0])
	require.Equal(t, job1, info.Jobs[1])
	require.Nil(t, info.ReorgHandle)

	tk.MustExec("rollback")
}

func addDDLJobs(sess session.Session, txn kv.Transaction, job *model.Job) error {
	b, err := job.Encode(true)
	if err != nil {
		return err
	}
	_, err = sess.Execute(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), fmt.Sprintf("insert into mysql.tidb_ddl_job(job_id, reorg, schema_ids, table_ids, job_meta, type, processing) values (%d, %t, %s, %s, %s, %d, %t)",
		job.ID, job.MayNeedReorg(), strconv.Quote(strconv.FormatInt(job.SchemaID, 10)), strconv.Quote(strconv.FormatInt(job.TableID, 10)), wrapKey2String(b), job.Type, false))
	return err
}

func wrapKey2String(key []byte) string {
	if len(key) == 0 {
		return "''"
	}
	return fmt.Sprintf("0x%x", key)
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
		ReorgMeta: &model.DDLReorgMeta{ // Add index job must have this field.
			SQLMode:       mysql.SQLMode(0),
			Warnings:      make(map[errors.ErrorID]*terror.Error),
			WarningsCount: make(map[errors.ErrorID]int64),
		},
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
>>>>>>> b7330bdc15f (*: fix bug that table name in 'admin show ddl jobs' is missing for ongoing drop table operation (#42904))
