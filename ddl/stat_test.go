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
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestDDLStatsInfo(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	d := domain.DDL()

	tk := testkit.NewTestKit(t, store)
	ctx := tk.Session()
	dbInfo, err := testSchemaInfo(store, "test_stat")
	require.NoError(t, err)
	testCreateSchema(t, ctx, d, dbInfo)
	tblInfo, err := testTableInfo(store, "t", 2)
	require.NoError(t, err)
	testCreateTable(t, ctx, d, dbInfo, tblInfo)
	err = sessiontxn.NewTxn(context.Background(), ctx)
	require.NoError(t, err)

	m := testGetTable(t, domain, tblInfo.ID)
	// insert t values (1, 1), (2, 2), (3, 3)
	_, err = m.AddRecord(ctx, types.MakeDatums(1, 1))
	require.NoError(t, err)
	_, err = m.AddRecord(ctx, types.MakeDatums(2, 2))
	require.NoError(t, err)
	_, err = m.AddRecord(ctx, types.MakeDatums(3, 3))
	require.NoError(t, err)
	ctx.StmtCommit(context.Background())
	require.NoError(t, ctx.CommitTxn(context.Background()))

	job := buildCreateIdxJob(dbInfo, tblInfo, true, "idx", "c1")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/checkBackfillWorkerNum"))
	}()

	ctx = testkit.NewTestKit(t, store).Session()
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
			key, err := hex.DecodeString(varMap[ddlJobReorgHandle].(string))
			require.NoError(t, err)
			_, h, err := tablecodec.DecodeRecordKey(key)
			require.NoError(t, err)
			require.Equal(t, h.IntValue(), int64(1))
		}
	}
}

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
