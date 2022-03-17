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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type testDDLSerialSuiteToVerify struct {
	suite.Suite
}

func TestDDLSerialSuite(t *testing.T) {
	suite.Run(t, new(testDDLSerialSuiteToVerify))
}

const testLease = 5 * time.Millisecond

func (s *testDDLSerialSuiteToVerify) SetupSuite() {
	SetWaitTimeWhenErrorOccurred(time.Microsecond)
}

func TestCheckOwner(t *testing.T) {
	store := createMockStore(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	d1, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d1.Stop())
	}()
	time.Sleep(testLease)
	testCheckOwner(t, d1, true)

	require.Equal(t, d1.GetLease(), testLease)
}

func TestNotifyDDLJob(t *testing.T) {
	store := createMockStore(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	getFirstNotificationAfterStartDDL := func(d *ddl) {
		select {
		case <-d.workers[addIdxWorker].ddlJobCh:
		default:
			// The notification may be received by the worker.
		}
		select {
		case <-d.workers[generalWorker].ddlJobCh:
		default:
			// The notification may be received by the worker.
		}
	}

	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Stop())
	}()
	getFirstNotificationAfterStartDDL(d)
	// Ensure that the notification is not handled in workers `start` function.
	d.cancel()
	for _, worker := range d.workers {
		worker.close()
	}

	job := &model.Job{
		SchemaID:   1,
		TableID:    2,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{},
	}
	// Test the notification mechanism of the owner and the server receiving the DDL request on the same TiDB.
	// This DDL request is a general DDL job.
	d.asyncNotifyWorker(job)
	select {
	case <-d.workers[generalWorker].ddlJobCh:
	default:
		require.FailNow(t, "do not get the general job notification")
	}
	// Test the notification mechanism of the owner and the server receiving the DDL request on the same TiDB.
	// This DDL request is a add index DDL job.
	job.Type = model.ActionAddIndex
	d.asyncNotifyWorker(job)
	select {
	case <-d.workers[addIdxWorker].ddlJobCh:
	default:
		require.FailNow(t, "do not get the add index job notification")
	}

	// Test the notification mechanism that the owner and the server receiving the DDL request are not on the same TiDB.
	// And the etcd client is nil.
	d1, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d1.Stop())
	}()
	getFirstNotificationAfterStartDDL(d1)
	// Ensure that the notification is not handled by worker's "start".
	d1.cancel()
	for _, worker := range d1.workers {
		worker.close()
	}
	d1.ownerManager.RetireOwner()
	d1.asyncNotifyWorker(job)
	job.Type = model.ActionCreateTable
	d1.asyncNotifyWorker(job)
	testCheckOwner(t, d1, false)
	select {
	case <-d1.workers[addIdxWorker].ddlJobCh:
		require.FailNow(t, "should not get the add index job notification")
	case <-d1.workers[generalWorker].ddlJobCh:
		require.FailNow(t, "should not get the general job notification")
	default:
	}
}

// TestRunWorker tests no job is handled when the value of RunWorker is false.
func (s *testDDLSerialSuiteToVerify) TestRunWorker() {
	store := createMockStore(s.T())
	defer func() {
		require.NoError(s.T(), store.Close())
	}()

	RunWorker = false
	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	testCheckOwner(s.T(), d, false)
	defer func() {
		require.NoError(s.T(), d.Stop())
	}()

	// Make sure the DDL worker is nil.
	worker := d.generalWorker()
	require.Nil(s.T(), worker)
	// Make sure the DDL job can be done and exit that goroutine.
	RunWorker = true
	d1, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	testCheckOwner(s.T(), d1, true)
	defer func() {
		err := d1.Stop()
		require.NoError(s.T(), err)
	}()
	worker = d1.generalWorker()
	require.NotNil(s.T(), worker)
}

func TestSchemaError(t *testing.T) {
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
	ctx := testNewContext(d)

	doDDLJobErr(t, 1, 0, model.ActionCreateSchema, []interface{}{1}, ctx, d)
}

func TestTableError(t *testing.T) {
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
	ctx := testNewContext(d)

	// Schema ID is wrong, so dropping table is failed.
	doDDLJobErr(t, -1, 1, model.ActionDropTable, nil, ctx, d)
	// Table ID is wrong, so dropping table is failed.
	dbInfo, err := testSchemaInfo(d, "test_ddl")
	require.NoError(t, err)
	testCreateSchema(t, testNewContext(d), d, dbInfo)
	job := doDDLJobErr(t, dbInfo.ID, -1, model.ActionDropTable, nil, ctx, d)

	// Table ID or schema ID is wrong, so getting table is failed.
	tblInfo, err := testTableInfo(d, "t", 3)
	require.NoError(t, err)
	testCreateTable(t, ctx, d, dbInfo, tblInfo)
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		job.SchemaID = -1
		job.TableID = -1
		m := meta.NewMeta(txn)
		_, err1 := getTableInfoAndCancelFaultJob(m, job, job.SchemaID)
		require.Error(t, err1)
		job.SchemaID = dbInfo.ID
		_, err1 = getTableInfoAndCancelFaultJob(m, job, job.SchemaID)
		require.Error(t, err1)
		return nil
	})
	require.NoError(t, err)

	// Args is wrong, so creating table is failed.
	doDDLJobErr(t, 1, 1, model.ActionCreateTable, []interface{}{1}, ctx, d)
	// Schema ID is wrong, so creating table is failed.
	doDDLJobErr(t, -1, tblInfo.ID, model.ActionCreateTable, []interface{}{tblInfo}, ctx, d)
	// Table exists, so creating table is failed.
	tblInfo.ID++
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionCreateTable, []interface{}{tblInfo}, ctx, d)

}

func TestViewError(t *testing.T) {
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
	ctx := testNewContext(d)
	dbInfo, err := testSchemaInfo(d, "test_ddl")
	require.NoError(t, err)
	testCreateSchema(t, testNewContext(d), d, dbInfo)

	// Table ID or schema ID is wrong, so getting table is failed.
	tblInfo := testViewInfo(t, d, "t", 3)
	testCreateView(t, ctx, d, dbInfo, tblInfo)

	// Args is wrong, so creating view is failed.
	doDDLJobErr(t, 1, 1, model.ActionCreateView, []interface{}{1}, ctx, d)
	// Schema ID is wrong and orReplace is false, so creating view is failed.
	doDDLJobErr(t, -1, tblInfo.ID, model.ActionCreateView, []interface{}{tblInfo, false}, ctx, d)
	// View exists and orReplace is false, so creating view is failed.
	tblInfo.ID++
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionCreateView, []interface{}{tblInfo, false}, ctx, d)

}

func TestInvalidDDLJob(t *testing.T) {
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
	ctx := testNewContext(d)

	job := &model.Job{
		SchemaID:   0,
		TableID:    0,
		Type:       model.ActionNone,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = d.doDDLJob(ctx, job)
	require.Equal(t, err.Error(), "[ddl:8204]invalid ddl job type: none")
}

func TestForeignKeyError(t *testing.T) {
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
	ctx := testNewContext(d)

	doDDLJobErr(t, -1, 1, model.ActionAddForeignKey, nil, ctx, d)
	doDDLJobErr(t, -1, 1, model.ActionDropForeignKey, nil, ctx, d)

	dbInfo, err := testSchemaInfo(d, "test_ddl")
	require.NoError(t, err)
	tblInfo, err := testTableInfo(d, "t", 3)
	require.NoError(t, err)
	testCreateSchema(t, ctx, d, dbInfo)
	testCreateTable(t, ctx, d, dbInfo, tblInfo)
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionDropForeignKey, []interface{}{model.NewCIStr("c1_foreign_key")}, ctx, d)
}

func TestIndexError(t *testing.T) {
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
	ctx := testNewContext(d)

	// Schema ID is wrong.
	doDDLJobErr(t, -1, 1, model.ActionAddIndex, nil, ctx, d)
	doDDLJobErr(t, -1, 1, model.ActionDropIndex, nil, ctx, d)

	dbInfo, err := testSchemaInfo(d, "test_ddl")
	require.NoError(t, err)
	tblInfo, err := testTableInfo(d, "t", 3)
	require.NoError(t, err)
	testCreateSchema(t, ctx, d, dbInfo)
	testCreateTable(t, ctx, d, dbInfo, tblInfo)

	// for adding index
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, []interface{}{1}, ctx, d)
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("t"), 1,
			[]*ast.IndexPartSpecification{{Column: &ast.ColumnName{Name: model.NewCIStr("c")}, Length: 256}}}, ctx, d)
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("c1_index"), 1,
			[]*ast.IndexPartSpecification{{Column: &ast.ColumnName{Name: model.NewCIStr("c")}, Length: 256}}}, ctx, d)
	testCreateIndex(t, ctx, d, dbInfo, tblInfo, false, "c1_index", "c1")
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("c1_index"), 1,
			[]*ast.IndexPartSpecification{{Column: &ast.ColumnName{Name: model.NewCIStr("c1")}, Length: 256}}}, ctx, d)

	// for dropping index
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, []interface{}{1}, ctx, d)
	testDropIndex(t, ctx, d, dbInfo, tblInfo, "c1_index")
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, []interface{}{model.NewCIStr("c1_index")}, ctx, d)
}

func TestColumnError(t *testing.T) {
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
	ctx := testNewContext(d)

	dbInfo, err := testSchemaInfo(d, "test_ddl")
	require.NoError(t, err)
	tblInfo, err := testTableInfo(d, "t", 3)
	require.NoError(t, err)
	testCreateSchema(t, ctx, d, dbInfo)
	testCreateTable(t, ctx, d, dbInfo, tblInfo)
	col := &model.ColumnInfo{
		Name:         model.NewCIStr("c4"),
		Offset:       len(tblInfo.Columns),
		DefaultValue: 0,
	}
	col.ID = allocateColumnID(tblInfo)
	col.FieldType = *types.NewFieldType(mysql.TypeLong)
	pos := &ast.ColumnPosition{Tp: ast.ColumnPositionAfter, RelativeColumn: &ast.ColumnName{Name: model.NewCIStr("c5")}}

	cols := &[]*model.ColumnInfo{col}
	positions := &[]*ast.ColumnPosition{pos}

	// for adding column
	doDDLJobErr(t, -1, tblInfo.ID, model.ActionAddColumn, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(t, dbInfo.ID, -1, model.ActionAddColumn, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, []interface{}{0}, ctx, d)
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, []interface{}{col, pos, 0}, ctx, d)

	// for dropping column
	doDDLJobErr(t, -1, tblInfo.ID, model.ActionDropColumn, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(t, dbInfo.ID, -1, model.ActionDropColumn, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionDropColumn, []interface{}{0}, ctx, d)
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionDropColumn, []interface{}{model.NewCIStr("c5")}, ctx, d)

	// for adding columns
	doDDLJobErr(t, -1, tblInfo.ID, model.ActionAddColumns, []interface{}{cols, positions, 0}, ctx, d)
	doDDLJobErr(t, dbInfo.ID, -1, model.ActionAddColumns, []interface{}{cols, positions, 0}, ctx, d)
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionAddColumns, []interface{}{0}, ctx, d)
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionAddColumns, []interface{}{cols, positions, 0}, ctx, d)

	// for dropping columns
	doDDLJobErr(t, -1, tblInfo.ID, model.ActionDropColumns, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(t, dbInfo.ID, -1, model.ActionDropColumns, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionDropColumns, []interface{}{0}, ctx, d)
	doDDLJobErr(t, dbInfo.ID, tblInfo.ID, model.ActionDropColumns, []interface{}{[]model.CIStr{model.NewCIStr("c5"), model.NewCIStr("c6")}, make([]bool, 2)}, ctx, d)
}

func (s *testDDLSerialSuiteToVerify) TestAddBatchJobError() {
	store := createMockStore(s.T())
	defer func() {
		require.NoError(s.T(), store.Close())
	}()
	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	defer func() {
		require.NoError(s.T(), d.Stop())
	}()
	ctx := testNewContext(d)
	require.Nil(s.T(), failpoint.Enable("github.com/pingcap/tidb/ddl/mockAddBatchDDLJobsErr", `return(true)`))
	// Test the job runner should not hang forever.
	job := &model.Job{SchemaID: 1, TableID: 1}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = d.doDDLJob(ctx, job)
	require.Error(s.T(), err)
	require.Equal(s.T(), err.Error(), "mockAddBatchDDLJobsErr")
	require.Nil(s.T(), failpoint.Disable("github.com/pingcap/tidb/ddl/mockAddBatchDDLJobsErr"))
}

func testCheckOwner(t *testing.T, d *ddl, expectedVal bool) {
	require.Equal(t, d.isOwner(), expectedVal)
}

func testCheckJobDone(t *testing.T, d *ddl, job *model.Job, isAdd bool) {
	require.NoError(t, kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		historyJob, err := m.GetHistoryDDLJob(job.ID)
		require.NoError(t, err)
		checkHistoryJob(t, historyJob)
		if isAdd {
			require.Equal(t, historyJob.SchemaState, model.StatePublic)
		} else {
			require.Equal(t, historyJob.SchemaState, model.StateNone)
		}

		return nil
	}))
}

func testCheckJobCancelled(t *testing.T, d *ddl, job *model.Job, state *model.SchemaState) {
	require.NoError(t, kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		historyJob, err := m.GetHistoryDDLJob(job.ID)
		require.NoError(t, err)
		require.True(t, historyJob.IsCancelled() || historyJob.IsRollbackDone(), "history job %s", historyJob)
		if state != nil {
			require.Equal(t, historyJob.SchemaState, *state)
		}
		return nil
	}))
}

func doDDLJobErrWithSchemaState(ctx sessionctx.Context, d *ddl, t *testing.T, schemaID, tableID int64, tp model.ActionType,
	args []interface{}, state *model.SchemaState) *model.Job {
	job := &model.Job{
		SchemaID:   schemaID,
		TableID:    tableID,
		Type:       tp,
		Args:       args,
		BinlogInfo: &model.HistoryInfo{},
	}
	// TODO: check error detail
	ctx.SetValue(sessionctx.QueryString, "skip")
	require.Error(t, d.doDDLJob(ctx, job))
	testCheckJobCancelled(t, d, job, state)

	return job
}

func doDDLJobSuccess(ctx sessionctx.Context, d *ddl, t *testing.T, schemaID, tableID int64, tp model.ActionType,
	args []interface{}) {
	job := &model.Job{
		SchemaID:   schemaID,
		TableID:    tableID,
		Type:       tp,
		Args:       args,
		BinlogInfo: &model.HistoryInfo{},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)
}

func doDDLJobErr(t *testing.T, schemaID, tableID int64, tp model.ActionType, args []interface{}, ctx sessionctx.Context, d *ddl) *model.Job {
	return doDDLJobErrWithSchemaState(ctx, d, t, schemaID, tableID, tp, args, nil)
}

func checkCancelState(txn kv.Transaction, job *model.Job, test *testCancelJob) error {
	var checkErr error
	addIndexFirstReorg := (test.act == model.ActionAddIndex || test.act == model.ActionAddPrimaryKey) &&
		job.SchemaState == model.StateWriteReorganization && job.SnapshotVer == 0
	// If the action is adding index and the state is writing reorganization, it wants to test the case of cancelling the job when backfilling indexes.
	// When the job satisfies this case of addIndexFirstReorg, the worker hasn't started to backfill indexes.
	if test.cancelState == job.SchemaState && !addIndexFirstReorg && !job.IsRollingback() {
		errs, err := admin.CancelJobs(txn, test.jobIDs)
		if err != nil {
			checkErr = errors.Trace(err)
			return checkErr
		}
		// It only tests cancel one DDL job.
		if !terror.ErrorEqual(errs[0], test.cancelRetErrs[0]) {
			checkErr = errors.Trace(errs[0])
			return checkErr
		}
	}
	return checkErr
}

type testCancelJob struct {
	jobIDs        []int64
	cancelRetErrs []error          // cancelRetErrs is the first return value of CancelJobs.
	act           model.ActionType // act is the job action.
	cancelState   model.SchemaState
}

func buildCancelJobTests(firstID int64) []testCancelJob {
	noErrs := []error{nil}
	tests := []testCancelJob{
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 1}, cancelRetErrs: noErrs, cancelState: model.StateDeleteOnly},
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 2}, cancelRetErrs: noErrs, cancelState: model.StateWriteOnly},
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 3}, cancelRetErrs: noErrs, cancelState: model.StateWriteReorganization},
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 4}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 4)}, cancelState: model.StatePublic},

		// Test cancel drop index job , see TestCancelDropIndex.
		{act: model.ActionAddColumn, jobIDs: []int64{firstID + 5}, cancelRetErrs: noErrs, cancelState: model.StateDeleteOnly},
		{act: model.ActionAddColumn, jobIDs: []int64{firstID + 6}, cancelRetErrs: noErrs, cancelState: model.StateWriteOnly},
		{act: model.ActionAddColumn, jobIDs: []int64{firstID + 7}, cancelRetErrs: noErrs, cancelState: model.StateWriteReorganization},
		{act: model.ActionAddColumn, jobIDs: []int64{firstID + 8}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 8)}, cancelState: model.StatePublic},

		// Test create table, watch out, table id will alloc a globalID.
		{act: model.ActionCreateTable, jobIDs: []int64{firstID + 10}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		// Test create database, watch out, database id will alloc a globalID.
		{act: model.ActionCreateSchema, jobIDs: []int64{firstID + 12}, cancelRetErrs: noErrs, cancelState: model.StateNone},

		{act: model.ActionDropColumn, jobIDs: []int64{firstID + 13}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 13)}, cancelState: model.StateDeleteOnly},
		{act: model.ActionDropColumn, jobIDs: []int64{firstID + 14}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 14)}, cancelState: model.StateWriteOnly},
		{act: model.ActionDropColumn, jobIDs: []int64{firstID + 15}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 15)}, cancelState: model.StateWriteReorganization},
		{act: model.ActionRebaseAutoID, jobIDs: []int64{firstID + 16}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionShardRowID, jobIDs: []int64{firstID + 17}, cancelRetErrs: noErrs, cancelState: model.StateNone},

		{act: model.ActionModifyColumn, jobIDs: []int64{firstID + 18}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionModifyColumn, jobIDs: []int64{firstID + 19}, cancelRetErrs: noErrs, cancelState: model.StateDeleteOnly},

		{act: model.ActionAddForeignKey, jobIDs: []int64{firstID + 20}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionAddForeignKey, jobIDs: []int64{firstID + 21}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 21)}, cancelState: model.StatePublic},
		{act: model.ActionDropForeignKey, jobIDs: []int64{firstID + 22}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionDropForeignKey, jobIDs: []int64{firstID + 23}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 23)}, cancelState: model.StatePublic},

		{act: model.ActionRenameTable, jobIDs: []int64{firstID + 24}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionRenameTable, jobIDs: []int64{firstID + 25}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 25)}, cancelState: model.StatePublic},

		{act: model.ActionModifyTableCharsetAndCollate, jobIDs: []int64{firstID + 26}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionModifyTableCharsetAndCollate, jobIDs: []int64{firstID + 27}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 27)}, cancelState: model.StatePublic},
		{act: model.ActionTruncateTablePartition, jobIDs: []int64{firstID + 28}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionTruncateTablePartition, jobIDs: []int64{firstID + 29}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 29)}, cancelState: model.StatePublic},
		{act: model.ActionModifySchemaCharsetAndCollate, jobIDs: []int64{firstID + 31}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionModifySchemaCharsetAndCollate, jobIDs: []int64{firstID + 32}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 32)}, cancelState: model.StatePublic},

		{act: model.ActionAddPrimaryKey, jobIDs: []int64{firstID + 33}, cancelRetErrs: noErrs, cancelState: model.StateDeleteOnly},
		{act: model.ActionAddPrimaryKey, jobIDs: []int64{firstID + 34}, cancelRetErrs: noErrs, cancelState: model.StateWriteOnly},
		{act: model.ActionAddPrimaryKey, jobIDs: []int64{firstID + 35}, cancelRetErrs: noErrs, cancelState: model.StateWriteReorganization},
		{act: model.ActionAddPrimaryKey, jobIDs: []int64{firstID + 36}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 36)}, cancelState: model.StatePublic},
		{act: model.ActionDropPrimaryKey, jobIDs: []int64{firstID + 37}, cancelRetErrs: noErrs, cancelState: model.StateWriteOnly},
		{act: model.ActionDropPrimaryKey, jobIDs: []int64{firstID + 38}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 38)}, cancelState: model.StateDeleteOnly},

		{act: model.ActionAddColumns, jobIDs: []int64{firstID + 39}, cancelRetErrs: noErrs, cancelState: model.StateDeleteOnly},
		{act: model.ActionAddColumns, jobIDs: []int64{firstID + 40}, cancelRetErrs: noErrs, cancelState: model.StateWriteOnly},
		{act: model.ActionAddColumns, jobIDs: []int64{firstID + 41}, cancelRetErrs: noErrs, cancelState: model.StateWriteReorganization},
		{act: model.ActionAddColumns, jobIDs: []int64{firstID + 42}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 42)}, cancelState: model.StatePublic},

		{act: model.ActionDropColumns, jobIDs: []int64{firstID + 43}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 43)}, cancelState: model.StateDeleteOnly},
		{act: model.ActionDropColumns, jobIDs: []int64{firstID + 44}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 44)}, cancelState: model.StateWriteOnly},
		{act: model.ActionDropColumns, jobIDs: []int64{firstID + 45}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 45)}, cancelState: model.StateWriteReorganization},

		{act: model.ActionAlterIndexVisibility, jobIDs: []int64{firstID + 47}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionAlterIndexVisibility, jobIDs: []int64{firstID + 48}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 48)}, cancelState: model.StatePublic},

		{act: model.ActionExchangeTablePartition, jobIDs: []int64{firstID + 54}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionExchangeTablePartition, jobIDs: []int64{firstID + 55}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 55)}, cancelState: model.StatePublic},

		{act: model.ActionAddTablePartition, jobIDs: []int64{firstID + 60}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionAddTablePartition, jobIDs: []int64{firstID + 61}, cancelRetErrs: noErrs, cancelState: model.StateReplicaOnly},
		{act: model.ActionAddTablePartition, jobIDs: []int64{firstID + 62}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob}, cancelState: model.StatePublic},

		// modify column has two different types, normal-type and reorg-type. The latter has 5 states and it can be cancelled except the public state.
		{act: model.ActionModifyColumn, jobIDs: []int64{firstID + 65}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionModifyColumn, jobIDs: []int64{firstID + 66}, cancelRetErrs: noErrs, cancelState: model.StateDeleteOnly},
		{act: model.ActionModifyColumn, jobIDs: []int64{firstID + 67}, cancelRetErrs: noErrs, cancelState: model.StateWriteOnly},
		{act: model.ActionModifyColumn, jobIDs: []int64{firstID + 68}, cancelRetErrs: noErrs, cancelState: model.StateWriteReorganization},
		{act: model.ActionModifyColumn, jobIDs: []int64{firstID + 69}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob}, cancelState: model.StatePublic},

		// for drop indexes
		{act: model.ActionDropIndexes, jobIDs: []int64{firstID + 72}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 72)}, cancelState: model.StateWriteOnly},
		{act: model.ActionDropIndexes, jobIDs: []int64{firstID + 73}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 73)}, cancelState: model.StateDeleteOnly},
		{act: model.ActionDropIndexes, jobIDs: []int64{firstID + 74}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 74)}, cancelState: model.StateWriteReorganization},

		// for alter db placement
		{act: model.ActionModifySchemaDefaultPlacement, jobIDs: []int64{firstID + 75}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionModifySchemaDefaultPlacement, jobIDs: []int64{firstID + 76}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 76)}, cancelState: model.StatePublic},
	}

	return tests
}

func (s *testDDLSerialSuiteToVerify) checkDropIdx(t *testing.T, d *ddl, schemaID int64, tableID int64, idxName string, success bool) {
	checkIdxExist(t, d, schemaID, tableID, idxName, !success)
}

func (s *testDDLSerialSuiteToVerify) checkAddIdx(t *testing.T, d *ddl, schemaID int64, tableID int64, idxName string, success bool) {
	checkIdxExist(t, d, schemaID, tableID, idxName, success)
}

func checkIdxExist(t *testing.T, d *ddl, schemaID int64, tableID int64, idxName string, expectedExist bool) {
	changedTable := testGetTable(t, d, schemaID, tableID)
	var found bool
	for _, idxInfo := range changedTable.Meta().Indices {
		if idxInfo.Name.O == idxName {
			found = true
			break
		}
	}
	require.Equal(t, found, expectedExist)
}

func (s *testDDLSerialSuiteToVerify) checkAddColumns(d *ddl, schemaID int64, tableID int64, colNames []string, success bool) {
	changedTable := testGetTable(s.T(), d, schemaID, tableID)
	found := !checkColumnsNotFound(changedTable, colNames)
	require.Equal(s.T(), found, success)
}

func (s *testDDLSerialSuiteToVerify) checkCancelDropColumns(d *ddl, schemaID int64, tableID int64, colNames []string, success bool) {
	changedTable := testGetTable(s.T(), d, schemaID, tableID)
	notFound := checkColumnsNotFound(changedTable, colNames)
	require.Equal(s.T(), notFound, success)
}

func checkColumnsNotFound(t table.Table, colNames []string) bool {
	notFound := true
	for _, colName := range colNames {
		for _, colInfo := range t.Meta().Columns {
			if colInfo.Name.O == colName {
				notFound = false
			}
		}
	}
	return notFound
}

func checkIdxVisibility(changedTable table.Table, idxName string, expected bool) bool {
	for _, idxInfo := range changedTable.Meta().Indices {
		if idxInfo.Name.O == idxName && idxInfo.Invisible == expected {
			return true
		}
	}
	return false
}

func (s *testDDLSerialSuiteToVerify) TestCancelJob() {
	store := createMockStore(s.T())
	defer func() {
		require.NoError(s.T(), store.Close())
	}()
	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	defer func() {
		require.NoError(s.T(), d.Stop())
	}()
	dbInfo, err := testSchemaInfo(d, "test_cancel_job")
	require.NoError(s.T(), err)
	testCreateSchema(s.T(), testNewContext(d), d, dbInfo)
	// create a partition table.
	partitionTblInfo := testTableInfoWithPartition(s.T(), d, "t_partition", 5)
	// Skip using sessPool. Make sure adding primary key can be successful.
	partitionTblInfo.Columns[0].Flag |= mysql.NotNullFlag
	// create table t (c1 int, c2 int, c3 int, c4 int, c5 int);
	tblInfo, err := testTableInfo(d, "t", 5)
	require.NoError(s.T(), err)
	ctx := testNewContext(d)
	err = ctx.NewTxn(context.Background())
	require.NoError(s.T(), err)
	err = ctx.GetSessionVars().SetSystemVar("tidb_enable_exchange_partition", "1")
	require.NoError(s.T(), err)
	defer func() {
		err := ctx.GetSessionVars().SetSystemVar("tidb_enable_exchange_partition", "0")
		require.NoError(s.T(), err)
	}()
	testCreateTable(s.T(), ctx, d, dbInfo, partitionTblInfo)
	tableAutoID := int64(100)
	shardRowIDBits := uint64(5)
	tblInfo.AutoIncID = tableAutoID
	tblInfo.ShardRowIDBits = shardRowIDBits
	job := testCreateTable(s.T(), ctx, d, dbInfo, tblInfo)
	// insert t values (1, 2, 3, 4, 5);
	originTable := testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	row := types.MakeDatums(1, 2, 3, 4, 5)
	_, err = originTable.AddRecord(ctx, row)
	require.NoError(s.T(), err)
	txn, err := ctx.Txn(true)
	require.NoError(s.T(), err)
	err = txn.Commit(context.Background())
	require.NoError(s.T(), err)

	tc := &TestDDLCallback{}
	// set up hook
	firstJobID := job.ID
	tests := buildCancelJobTests(firstJobID)
	var checkErr error
	var mu sync.Mutex
	var test *testCancelJob
	updateTest := func(t *testCancelJob) {
		mu.Lock()
		test = t
		mu.Unlock()
	}
	hookCancelFunc := func(job *model.Job) {
		if job.State == model.JobStateSynced || job.State == model.JobStateCancelled || job.State == model.JobStateCancelling {
			return
		}
		// This hook only valid for the related test job.
		// This is use to avoid parallel test fail.
		mu.Lock()
		if len(test.jobIDs) > 0 && test.jobIDs[0] != job.ID {
			mu.Unlock()
			return
		}
		mu.Unlock()
		if checkErr != nil {
			return
		}

		hookCtx := mock.NewContext()
		hookCtx.Store = store
		err1 := hookCtx.NewTxn(context.Background())
		if err1 != nil {
			checkErr = errors.Trace(err1)
			return
		}
		txn, err1 = hookCtx.Txn(true)
		if err1 != nil {
			checkErr = errors.Trace(err1)
			return
		}
		mu.Lock()
		checkErr = checkCancelState(txn, job, test)
		mu.Unlock()
		if checkErr != nil {
			return
		}
		err1 = txn.Commit(context.Background())
		if err1 != nil {
			checkErr = errors.Trace(err1)
			return
		}
	}
	tc.onJobUpdated = hookCancelFunc
	tc.onJobRunBefore = hookCancelFunc
	d.SetHook(tc)

	// for adding index
	updateTest(&tests[0])
	idxOrigName := "idx"
	validArgs := []interface{}{false, model.NewCIStr(idxOrigName),
		[]*ast.IndexPartSpecification{{
			Column: &ast.ColumnName{Name: model.NewCIStr("c1")},
			Length: -1,
		}}, nil}

	// When the job satisfies this test case, the option will be rollback, so the job's schema state is none.
	cancelState := model.StateNone
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, model.ActionAddIndex, validArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	s.checkAddIdx(s.T(), d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[1])
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, model.ActionAddIndex, validArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	s.checkAddIdx(s.T(), d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[2])
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, model.ActionAddIndex, validArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	s.checkAddIdx(s.T(), d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[3])
	testCreateIndex(s.T(), ctx, d, dbInfo, tblInfo, false, "idx", "c2")
	require.NoError(s.T(), checkErr)
	txn, err = ctx.Txn(true)
	require.NoError(s.T(), err)
	require.Nil(s.T(), txn.Commit(context.Background()))
	s.checkAddIdx(s.T(), d, dbInfo.ID, tblInfo.ID, idxOrigName, true)

	// for add column
	updateTest(&tests[4])
	addingColName := "colA"
	newColumnDef := &ast.ColumnDef{
		Name:    &ast.ColumnName{Name: model.NewCIStr(addingColName)},
		Tp:      &types.FieldType{Tp: mysql.TypeLonglong},
		Options: []*ast.ColumnOption{},
	}
	chs, coll := charset.GetDefaultCharsetAndCollate()
	col, _, err := buildColumnAndConstraint(ctx, 2, newColumnDef, nil, chs, coll)
	require.NoError(s.T(), err)

	addColumnArgs := []interface{}{col, &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, 0}
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, model.ActionAddColumn, addColumnArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	s.checkAddColumns(d, dbInfo.ID, tblInfo.ID, []string{addingColName}, false)

	updateTest(&tests[5])
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, model.ActionAddColumn, addColumnArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	s.checkAddColumns(d, dbInfo.ID, tblInfo.ID, []string{addingColName}, false)

	updateTest(&tests[6])
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, model.ActionAddColumn, addColumnArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	s.checkAddColumns(d, dbInfo.ID, tblInfo.ID, []string{addingColName}, false)

	updateTest(&tests[7])
	testAddColumn(s.T(), ctx, d, dbInfo, tblInfo, addColumnArgs)
	require.NoError(s.T(), checkErr)
	s.checkAddColumns(d, dbInfo.ID, tblInfo.ID, []string{addingColName}, true)

	// for create table
	tblInfo1, err := testTableInfo(d, "t1", 2)
	require.NoError(s.T(), err)
	updateTest(&tests[8])
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo1.ID, model.ActionCreateTable, []interface{}{tblInfo1}, &cancelState)
	require.NoError(s.T(), checkErr)
	testCheckTableState(s.T(), d, dbInfo, tblInfo1, model.StateNone)

	// for create database
	dbInfo1, err := testSchemaInfo(d, "test_cancel_job1")
	require.NoError(s.T(), err)
	updateTest(&tests[9])
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo1.ID, 0, model.ActionCreateSchema, []interface{}{dbInfo1}, &cancelState)
	require.NoError(s.T(), checkErr)
	testCheckSchemaState(s.T(), d, dbInfo1, model.StateNone)

	// for drop column.
	updateTest(&tests[10])
	dropColName := "c3"
	s.checkCancelDropColumns(d, dbInfo.ID, tblInfo.ID, []string{dropColName}, false)
	testDropColumn(s.T(), ctx, d, dbInfo, tblInfo, dropColName, false)
	require.NoError(s.T(), checkErr)
	s.checkCancelDropColumns(d, dbInfo.ID, tblInfo.ID, []string{dropColName}, true)

	updateTest(&tests[11])
	dropColName = "c4"
	s.checkCancelDropColumns(d, dbInfo.ID, tblInfo.ID, []string{dropColName}, false)
	testDropColumn(s.T(), ctx, d, dbInfo, tblInfo, dropColName, false)
	require.NoError(s.T(), checkErr)
	s.checkCancelDropColumns(d, dbInfo.ID, tblInfo.ID, []string{dropColName}, true)

	updateTest(&tests[12])
	dropColName = "c5"
	s.checkCancelDropColumns(d, dbInfo.ID, tblInfo.ID, []string{dropColName}, false)
	testDropColumn(s.T(), ctx, d, dbInfo, tblInfo, dropColName, false)
	require.NoError(s.T(), checkErr)
	s.checkCancelDropColumns(d, dbInfo.ID, tblInfo.ID, []string{dropColName}, true)

	// cancel rebase auto id
	updateTest(&tests[13])
	rebaseIDArgs := []interface{}{int64(200)}
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, model.ActionRebaseAutoID, rebaseIDArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	changedTable := testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	require.Equal(s.T(), changedTable.Meta().AutoIncID, tableAutoID)

	// cancel shard bits
	updateTest(&tests[14])
	shardRowIDArgs := []interface{}{uint64(7)}
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, model.ActionShardRowID, shardRowIDArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	changedTable = testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	require.Equal(s.T(), changedTable.Meta().ShardRowIDBits, shardRowIDBits)

	// modify none-state column
	col.DefaultValue = "1"
	updateTest(&tests[15])
	modifyColumnArgs := []interface{}{col, col.Name, &ast.ColumnPosition{}, byte(0), uint64(0)}
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, test.act, modifyColumnArgs, &test.cancelState)
	require.NoError(s.T(), checkErr)
	changedTable = testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	changedCol := model.FindColumnInfo(changedTable.Meta().Columns, col.Name.L)
	require.Nil(s.T(), changedCol.DefaultValue)

	// modify delete-only-state column,
	col.FieldType.Tp = mysql.TypeTiny
	col.FieldType.Flen--
	updateTest(&tests[16])
	modifyColumnArgs = []interface{}{col, col.Name, &ast.ColumnPosition{}, byte(0), uint64(0)}
	cancelState = model.StateNone
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, test.act, modifyColumnArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	changedTable = testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	changedCol = model.FindColumnInfo(changedTable.Meta().Columns, col.Name.L)
	require.Equal(s.T(), changedCol.FieldType.Tp, mysql.TypeLonglong)
	require.Equal(s.T(), changedCol.FieldType.Flen, col.FieldType.Flen+1)
	col.FieldType.Flen++

	// Test add foreign key failed cause by canceled.
	updateTest(&tests[17])
	addForeignKeyArgs := []interface{}{model.FKInfo{Name: model.NewCIStr("fk1")}}
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, test.act, addForeignKeyArgs, &test.cancelState)
	require.NoError(s.T(), checkErr)
	changedTable = testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	require.Equal(s.T(), len(changedTable.Meta().ForeignKeys), 0)

	// Test add foreign key successful.
	updateTest(&tests[18])
	doDDLJobSuccess(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, test.act, addForeignKeyArgs)
	require.NoError(s.T(), checkErr)
	changedTable = testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	require.Equal(s.T(), len(changedTable.Meta().ForeignKeys), 1)
	require.Equal(s.T(), changedTable.Meta().ForeignKeys[0].Name, addForeignKeyArgs[0].(model.FKInfo).Name)

	// Test drop foreign key failed cause by canceled.
	updateTest(&tests[19])
	dropForeignKeyArgs := []interface{}{addForeignKeyArgs[0].(model.FKInfo).Name}
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, test.act, dropForeignKeyArgs, &test.cancelState)
	require.NoError(s.T(), checkErr)
	changedTable = testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	require.Equal(s.T(), len(changedTable.Meta().ForeignKeys), 1)
	require.Equal(s.T(), changedTable.Meta().ForeignKeys[0].Name, dropForeignKeyArgs[0].(model.CIStr))

	// Test drop foreign key successful.
	updateTest(&tests[20])
	doDDLJobSuccess(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, test.act, dropForeignKeyArgs)
	require.NoError(s.T(), checkErr)
	changedTable = testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	require.Equal(s.T(), len(changedTable.Meta().ForeignKeys), 0)

	// test rename table failed caused by canceled.
	test = &tests[21]
	renameTableArgs := []interface{}{dbInfo.ID, model.NewCIStr("t2"), dbInfo.Name}
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, test.act, renameTableArgs, &test.cancelState)
	require.NoError(s.T(), checkErr)
	changedTable = testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	require.Equal(s.T(), changedTable.Meta().Name.L, "t")

	// test rename table successful.
	test = &tests[22]
	doDDLJobSuccess(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, test.act, renameTableArgs)
	require.NoError(s.T(), checkErr)
	changedTable = testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	require.Equal(s.T(), changedTable.Meta().Name.L, "t2")

	// test modify table charset failed caused by canceled.
	test = &tests[23]
	modifyTableCharsetArgs := []interface{}{"utf8mb4", "utf8mb4_bin"}
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, test.act, modifyTableCharsetArgs, &test.cancelState)
	require.NoError(s.T(), checkErr)
	changedTable = testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	require.Equal(s.T(), changedTable.Meta().Charset, "utf8")
	require.Equal(s.T(), changedTable.Meta().Collate, "utf8_bin")

	// test modify table charset successfully.
	test = &tests[24]
	doDDLJobSuccess(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, test.act, modifyTableCharsetArgs)
	require.NoError(s.T(), checkErr)
	changedTable = testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	require.Equal(s.T(), changedTable.Meta().Charset, "utf8mb4")
	require.Equal(s.T(), changedTable.Meta().Collate, "utf8mb4_bin")

	// test truncate table partition failed caused by canceled.
	test = &tests[25]
	truncateTblPartitionArgs := []interface{}{[]int64{partitionTblInfo.Partition.Definitions[0].ID}}
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, partitionTblInfo.ID, test.act, truncateTblPartitionArgs, &test.cancelState)
	require.NoError(s.T(), checkErr)
	changedTable = testGetTable(s.T(), d, dbInfo.ID, partitionTblInfo.ID)
	require.True(s.T(), changedTable.Meta().Partition.Definitions[0].ID == partitionTblInfo.Partition.Definitions[0].ID)

	// test truncate table partition charset successfully.
	test = &tests[26]
	doDDLJobSuccess(ctx, d, s.T(), dbInfo.ID, partitionTblInfo.ID, test.act, truncateTblPartitionArgs)
	require.NoError(s.T(), checkErr)
	changedTable = testGetTable(s.T(), d, dbInfo.ID, partitionTblInfo.ID)
	require.False(s.T(), changedTable.Meta().Partition.Definitions[0].ID == partitionTblInfo.Partition.Definitions[0].ID)

	// test modify schema charset failed caused by canceled.
	test = &tests[27]
	charsetAndCollate := []interface{}{"utf8mb4", "utf8mb4_bin"}
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, test.act, charsetAndCollate, &test.cancelState)
	require.NoError(s.T(), checkErr)
	dbInfo, err = testGetSchemaInfoWithError(d, dbInfo.ID)
	require.NoError(s.T(), err)
	require.Equal(s.T(), dbInfo.Charset, "")
	require.Equal(s.T(), dbInfo.Collate, "")

	// test modify table charset successfully.
	test = &tests[28]
	doDDLJobSuccess(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, test.act, charsetAndCollate)
	require.NoError(s.T(), checkErr)
	dbInfo, err = testGetSchemaInfoWithError(d, dbInfo.ID)
	require.NoError(s.T(), err)
	require.Equal(s.T(), dbInfo.Charset, "utf8mb4")
	require.Equal(s.T(), dbInfo.Collate, "utf8mb4_bin")

	// for adding primary key
	tblInfo = changedTable.Meta()
	updateTest(&tests[29])
	idxOrigName = "primary"
	validArgs = []interface{}{false, model.NewCIStr(idxOrigName),
		[]*ast.IndexPartSpecification{{
			Column: &ast.ColumnName{Name: model.NewCIStr("c1")},
			Length: -1,
		}}, nil}
	cancelState = model.StateNone
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, model.ActionAddPrimaryKey, validArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	s.checkAddIdx(s.T(), d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[30])
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, model.ActionAddPrimaryKey, validArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	s.checkAddIdx(s.T(), d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[31])
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, model.ActionAddPrimaryKey, validArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	s.checkAddIdx(s.T(), d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[32])
	testCreatePrimaryKey(s.T(), ctx, d, dbInfo, tblInfo, "c1")
	require.NoError(s.T(), checkErr)
	txn, err = ctx.Txn(true)
	require.NoError(s.T(), err)
	require.Nil(s.T(), txn.Commit(context.Background()))
	s.checkAddIdx(s.T(), d, dbInfo.ID, tblInfo.ID, idxOrigName, true)

	// for dropping primary key
	updateTest(&tests[33])
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, model.ActionDropPrimaryKey, validArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	s.checkDropIdx(s.T(), d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[34])
	testDropIndex(s.T(), ctx, d, dbInfo, tblInfo, idxOrigName)
	require.NoError(s.T(), checkErr)
	s.checkDropIdx(s.T(), d, dbInfo.ID, tblInfo.ID, idxOrigName, true)

	// for add columns
	updateTest(&tests[35])
	addingColNames := []string{"colA", "colB", "colC", "colD", "colE", "colF"}
	cols := make([]*table.Column, len(addingColNames))
	for i, addingColName := range addingColNames {
		newColumnDef := &ast.ColumnDef{
			Name:    &ast.ColumnName{Name: model.NewCIStr(addingColName)},
			Tp:      &types.FieldType{Tp: mysql.TypeLonglong},
			Options: []*ast.ColumnOption{},
		}
		col, _, err := buildColumnAndConstraint(ctx, 0, newColumnDef, nil, mysql.DefaultCharset, "")
		require.NoError(s.T(), err)
		cols[i] = col
	}
	offsets := make([]int, len(cols))
	positions := make([]*ast.ColumnPosition, len(cols))
	for i := range positions {
		positions[i] = &ast.ColumnPosition{Tp: ast.ColumnPositionNone}
	}
	ifNotExists := make([]bool, len(cols))

	addColumnArgs = []interface{}{cols, positions, offsets, ifNotExists}
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, model.ActionAddColumns, addColumnArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	s.checkAddColumns(d, dbInfo.ID, tblInfo.ID, addingColNames, false)

	updateTest(&tests[36])
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, model.ActionAddColumns, addColumnArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	s.checkAddColumns(d, dbInfo.ID, tblInfo.ID, addingColNames, false)

	updateTest(&tests[37])
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, model.ActionAddColumns, addColumnArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	s.checkAddColumns(d, dbInfo.ID, tblInfo.ID, addingColNames, false)

	updateTest(&tests[38])
	testAddColumns(s.T(), ctx, d, dbInfo, tblInfo, addColumnArgs)
	require.NoError(s.T(), checkErr)
	s.checkAddColumns(d, dbInfo.ID, tblInfo.ID, addingColNames, true)

	// for drop columns
	updateTest(&tests[39])
	dropColNames := []string{"colA", "colB"}
	s.checkCancelDropColumns(d, dbInfo.ID, tblInfo.ID, dropColNames, false)
	testDropColumns(s.T(), ctx, d, dbInfo, tblInfo, dropColNames, false)
	require.NoError(s.T(), checkErr)
	s.checkCancelDropColumns(d, dbInfo.ID, tblInfo.ID, dropColNames, true)

	updateTest(&tests[40])
	dropColNames = []string{"colC", "colD"}
	s.checkCancelDropColumns(d, dbInfo.ID, tblInfo.ID, dropColNames, false)
	testDropColumns(s.T(), ctx, d, dbInfo, tblInfo, dropColNames, false)
	require.NoError(s.T(), checkErr)
	s.checkCancelDropColumns(d, dbInfo.ID, tblInfo.ID, dropColNames, true)

	updateTest(&tests[41])
	dropColNames = []string{"colE", "colF"}
	s.checkCancelDropColumns(d, dbInfo.ID, tblInfo.ID, dropColNames, false)
	testDropColumns(s.T(), ctx, d, dbInfo, tblInfo, dropColNames, false)
	require.NoError(s.T(), checkErr)
	s.checkCancelDropColumns(d, dbInfo.ID, tblInfo.ID, dropColNames, true)

	// test alter index visibility failed caused by canceled.
	indexName := "idx_c3"
	testCreateIndex(s.T(), ctx, d, dbInfo, tblInfo, false, indexName, "c3")
	require.NoError(s.T(), checkErr)
	txn, err = ctx.Txn(true)
	require.NoError(s.T(), err)
	require.Nil(s.T(), txn.Commit(context.Background()))
	s.checkAddIdx(s.T(), d, dbInfo.ID, tblInfo.ID, indexName, true)

	updateTest(&tests[42])
	alterIndexVisibility := []interface{}{model.NewCIStr(indexName), true}
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, test.act, alterIndexVisibility, &test.cancelState)
	require.NoError(s.T(), checkErr)
	changedTable = testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	require.True(s.T(), checkIdxVisibility(changedTable, indexName, false))

	// cancel alter index visibility successfully
	updateTest(&tests[43])
	alterIndexVisibility = []interface{}{model.NewCIStr(indexName), true}
	doDDLJobSuccess(ctx, d, s.T(), dbInfo.ID, tblInfo.ID, test.act, alterIndexVisibility)
	require.NoError(s.T(), checkErr)
	changedTable = testGetTable(s.T(), d, dbInfo.ID, tblInfo.ID)
	require.True(s.T(), checkIdxVisibility(changedTable, indexName, true))

	// test exchange partition failed caused by canceled
	pt := testTableInfoWithPartition(s.T(), d, "pt", 5)
	nt, err := testTableInfo(d, "nt", 5)
	require.NoError(s.T(), err)
	testCreateTable(s.T(), ctx, d, dbInfo, pt)
	testCreateTable(s.T(), ctx, d, dbInfo, nt)

	updateTest(&tests[44])
	defID := pt.Partition.Definitions[0].ID
	exchangeTablePartition := []interface{}{defID, dbInfo.ID, pt.ID, "p0", true}
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, nt.ID, test.act, exchangeTablePartition, &test.cancelState)
	require.NoError(s.T(), checkErr)
	changedNtTable := testGetTable(s.T(), d, dbInfo.ID, nt.ID)
	changedPtTable := testGetTable(s.T(), d, dbInfo.ID, pt.ID)
	require.True(s.T(), changedNtTable.Meta().ID == nt.ID)
	require.True(s.T(), changedPtTable.Meta().Partition.Definitions[0].ID == pt.Partition.Definitions[0].ID)

	// cancel exchange partition successfully
	updateTest(&tests[45])
	doDDLJobSuccess(ctx, d, s.T(), dbInfo.ID, nt.ID, test.act, exchangeTablePartition)
	require.NoError(s.T(), checkErr)
	changedNtTable = testGetTable(s.T(), d, dbInfo.ID, pt.Partition.Definitions[0].ID)
	changedPtTable = testGetTable(s.T(), d, dbInfo.ID, pt.ID)
	require.False(s.T(), changedNtTable.Meta().ID == nt.ID)
	require.True(s.T(), changedPtTable.Meta().Partition.Definitions[0].ID == nt.ID)

	// Cancel add table partition.
	baseTableInfo := testTableInfoWithPartitionLessThan(s.T(), d, "empty_table", 5, "1000")
	testCreateTable(s.T(), ctx, d, dbInfo, baseTableInfo)

	cancelState = model.StateNone
	updateTest(&tests[46])
	addedPartInfo := testAddedNewTablePartitionInfo(s.T(), d, baseTableInfo, "p1", "maxvalue")
	addPartitionArgs := []interface{}{addedPartInfo}
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, baseTableInfo.ID, test.act, addPartitionArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	baseTable := testGetTable(s.T(), d, dbInfo.ID, baseTableInfo.ID)
	require.Equal(s.T(), len(baseTable.Meta().Partition.Definitions), 1)

	updateTest(&tests[47])
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, baseTableInfo.ID, test.act, addPartitionArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	baseTable = testGetTable(s.T(), d, dbInfo.ID, baseTableInfo.ID)
	require.Equal(s.T(), len(baseTable.Meta().Partition.Definitions), 1)

	updateTest(&tests[48])
	doDDLJobSuccess(ctx, d, s.T(), dbInfo.ID, baseTableInfo.ID, test.act, addPartitionArgs)
	require.NoError(s.T(), checkErr)
	baseTable = testGetTable(s.T(), d, dbInfo.ID, baseTableInfo.ID)
	require.Equal(s.T(), len(baseTable.Meta().Partition.Definitions), 2)
	require.Equal(s.T(), baseTable.Meta().Partition.Definitions[1].ID, addedPartInfo.Definitions[0].ID)
	require.Equal(s.T(), baseTable.Meta().Partition.Definitions[1].LessThan[0], addedPartInfo.Definitions[0].LessThan[0])

	// Cancel modify column which should reorg the data.
	require.Nil(s.T(), failpoint.Enable("github.com/pingcap/tidb/ddl/skipMockContextDoExec", `return(true)`))
	baseTableInfo = testTableInfoWith2IndexOnFirstColumn(s.T(), d, "modify-table", 2)
	// This will cost 2 global id, one for table id, the other for the job id.
	testCreateTable(s.T(), ctx, d, dbInfo, baseTableInfo)

	cancelState = model.StateNone
	newCol := baseTableInfo.Columns[0].Clone()
	// change type from long to tinyint.
	newCol.FieldType = *types.NewFieldType(mysql.TypeTiny)
	// change from null to not null
	newCol.FieldType.Flag |= mysql.NotNullFlag
	newCol.FieldType.Flen = 2

	originColName := baseTableInfo.Columns[0].Name
	pos := &ast.ColumnPosition{Tp: ast.ColumnPositionNone}

	updateTest(&tests[49])
	modifyColumnArgs = []interface{}{&newCol, originColName, pos, mysql.TypeNull, 0}
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, baseTableInfo.ID, test.act, modifyColumnArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	baseTable = testGetTable(s.T(), d, dbInfo.ID, baseTableInfo.ID)
	require.Equal(s.T(), baseTable.Meta().Columns[0].FieldType.Tp, mysql.TypeLong)
	require.Equal(s.T(), mysql.HasNotNullFlag(baseTable.Meta().Columns[0].FieldType.Flag), false)

	updateTest(&tests[50])
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, baseTableInfo.ID, test.act, modifyColumnArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	baseTable = testGetTable(s.T(), d, dbInfo.ID, baseTableInfo.ID)
	require.Equal(s.T(), baseTable.Meta().Columns[0].FieldType.Tp, mysql.TypeLong)
	require.Equal(s.T(), baseTable.Meta().Columns[0].FieldType.Flag&mysql.NotNullFlag, uint(0))

	updateTest(&tests[51])
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, baseTableInfo.ID, test.act, modifyColumnArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	baseTable = testGetTable(s.T(), d, dbInfo.ID, baseTableInfo.ID)
	require.Equal(s.T(), baseTable.Meta().Columns[0].FieldType.Tp, mysql.TypeLong)
	require.Equal(s.T(), baseTable.Meta().Columns[0].FieldType.Flag&mysql.NotNullFlag, uint(0))

	updateTest(&tests[52])
	doDDLJobErrWithSchemaState(ctx, d, s.T(), dbInfo.ID, baseTableInfo.ID, test.act, modifyColumnArgs, &cancelState)
	require.NoError(s.T(), checkErr)
	baseTable = testGetTable(s.T(), d, dbInfo.ID, baseTableInfo.ID)
	require.Equal(s.T(), baseTable.Meta().Columns[0].FieldType.Tp, mysql.TypeLong)
	require.Equal(s.T(), baseTable.Meta().Columns[0].FieldType.Flag&mysql.NotNullFlag, uint(0))

	updateTest(&tests[53])
	doDDLJobSuccess(ctx, d, s.T(), dbInfo.ID, baseTableInfo.ID, test.act, modifyColumnArgs)
	require.NoError(s.T(), checkErr)
	baseTable = testGetTable(s.T(), d, dbInfo.ID, baseTableInfo.ID)
	require.Equal(s.T(), baseTable.Meta().Columns[0].FieldType.Tp, mysql.TypeTiny)
	require.Equal(s.T(), baseTable.Meta().Columns[0].FieldType.Flag&mysql.NotNullFlag, uint(1))
	require.Nil(s.T(), failpoint.Disable("github.com/pingcap/tidb/ddl/skipMockContextDoExec"))

	// for drop indexes
	updateTest(&tests[54])
	ifExists := make([]bool, 2)
	idxNames := []model.CIStr{model.NewCIStr("i1"), model.NewCIStr("i2")}
	dropIndexesArgs := []interface{}{idxNames, ifExists}
	tableInfo := createTestTableForDropIndexes(s.T(), ctx, d, dbInfo, "test-drop-indexes", 6)
	doDDLJobSuccess(ctx, d, s.T(), dbInfo.ID, tableInfo.ID, test.act, dropIndexesArgs)
	s.checkDropIndexes(d, dbInfo.ID, tableInfo.ID, idxNames, true)

	updateTest(&tests[55])
	idxNames = []model.CIStr{model.NewCIStr("i3"), model.NewCIStr("i4")}
	dropIndexesArgs = []interface{}{idxNames, ifExists}
	doDDLJobSuccess(ctx, d, s.T(), dbInfo.ID, tableInfo.ID, test.act, dropIndexesArgs)
	s.checkDropIndexes(d, dbInfo.ID, tableInfo.ID, idxNames, true)

	updateTest(&tests[56])
	idxNames = []model.CIStr{model.NewCIStr("i5"), model.NewCIStr("i6")}
	dropIndexesArgs = []interface{}{idxNames, ifExists}
	doDDLJobSuccess(ctx, d, s.T(), dbInfo.ID, tableInfo.ID, test.act, dropIndexesArgs)
	s.checkDropIndexes(d, dbInfo.ID, tableInfo.ID, idxNames, true)
}

func TestIgnorableSpec(t *testing.T) {
	specs := []ast.AlterTableType{
		ast.AlterTableOption,
		ast.AlterTableAddColumns,
		ast.AlterTableAddConstraint,
		ast.AlterTableDropColumn,
		ast.AlterTableDropPrimaryKey,
		ast.AlterTableDropIndex,
		ast.AlterTableDropForeignKey,
		ast.AlterTableModifyColumn,
		ast.AlterTableChangeColumn,
		ast.AlterTableRenameTable,
		ast.AlterTableAlterColumn,
	}
	for _, spec := range specs {
		require.False(t, isIgnorableSpec(spec))
	}

	ignorableSpecs := []ast.AlterTableType{
		ast.AlterTableLock,
		ast.AlterTableAlgorithm,
	}
	for _, spec := range ignorableSpecs {
		require.True(t, isIgnorableSpec(spec))
	}
}

func TestBuildJobDependence(t *testing.T) {
	store := createMockStore(t)
	defer func() {
		require.NoError(t, store.Close())
	}()
	// Add some non-add-index jobs.
	job1 := &model.Job{ID: 1, TableID: 1, Type: model.ActionAddColumn}
	job2 := &model.Job{ID: 2, TableID: 1, Type: model.ActionCreateTable}
	job3 := &model.Job{ID: 3, TableID: 2, Type: model.ActionDropColumn}
	job6 := &model.Job{ID: 6, TableID: 1, Type: model.ActionDropTable}
	job7 := &model.Job{ID: 7, TableID: 2, Type: model.ActionModifyColumn}
	job9 := &model.Job{ID: 9, SchemaID: 111, Type: model.ActionDropSchema}
	job11 := &model.Job{ID: 11, TableID: 2, Type: model.ActionRenameTable, Args: []interface{}{int64(111), "old db name"}}
	err := kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		require.NoError(t, m.EnQueueDDLJob(job1))
		require.NoError(t, m.EnQueueDDLJob(job2))
		require.NoError(t, m.EnQueueDDLJob(job3))
		require.NoError(t, m.EnQueueDDLJob(job6))
		require.NoError(t, m.EnQueueDDLJob(job7))
		require.NoError(t, m.EnQueueDDLJob(job9))
		require.NoError(t, m.EnQueueDDLJob(job11))
		return nil
	})
	require.NoError(t, err)
	job4 := &model.Job{ID: 4, TableID: 1, Type: model.ActionAddIndex}
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := buildJobDependence(m, job4)
		require.NoError(t, err)
		require.Equal(t, job4.DependencyID, int64(2))
		return nil
	})
	require.NoError(t, err)
	job5 := &model.Job{ID: 5, TableID: 2, Type: model.ActionAddIndex}
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := buildJobDependence(m, job5)
		require.NoError(t, err)
		require.Equal(t, job5.DependencyID, int64(3))
		return nil
	})
	require.NoError(t, err)
	job8 := &model.Job{ID: 8, TableID: 3, Type: model.ActionAddIndex}
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := buildJobDependence(m, job8)
		require.NoError(t, err)
		require.Equal(t, job8.DependencyID, int64(0))
		return nil
	})
	require.NoError(t, err)
	job10 := &model.Job{ID: 10, SchemaID: 111, TableID: 3, Type: model.ActionAddIndex}
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := buildJobDependence(m, job10)
		require.NoError(t, err)
		require.Equal(t, job10.DependencyID, int64(9))
		return nil
	})
	require.NoError(t, err)
	job12 := &model.Job{ID: 12, SchemaID: 112, TableID: 2, Type: model.ActionAddIndex}
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := buildJobDependence(m, job12)
		require.NoError(t, err)
		require.Equal(t, job12.DependencyID, int64(11))
		return nil
	})
	require.NoError(t, err)
}

func addDDLJob(t *testing.T, d *ddl, job *model.Job) {
	task := &limitJobTask{job, make(chan error)}
	d.limitJobCh <- task
	err := <-task.err
	require.NoError(t, err)
}

func TestParallelDDL(t *testing.T) {
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
	ctx := testNewContext(d)
	err = ctx.NewTxn(context.Background())
	require.NoError(t, err)
	/*
		build structure:
			DBs -> {
			 db1: test_parallel_ddl_1
			 db2: test_parallel_ddl_2
			}
			Tables -> {
			 db1.t1 (c1 int, c2 int)
			 db1.t2 (c1 int primary key, c2 int, c3 int)
			 db2.t3 (c1 int, c2 int, c3 int, c4 int)
			}
			Data -> {
			 t1: (10, 10), (20, 20)
			 t2: (1, 1, 1), (2, 2, 2), (3, 3, 3)
			 t3: (11, 22, 33, 44)
			}
	*/
	// create database test_parallel_ddl_1;
	dbInfo1, err := testSchemaInfo(d, "test_parallel_ddl_1")
	require.NoError(t, err)
	testCreateSchema(t, ctx, d, dbInfo1)
	// create table t1 (c1 int, c2 int);
	tblInfo1, err := testTableInfo(d, "t1", 2)
	require.NoError(t, err)
	testCreateTable(t, ctx, d, dbInfo1, tblInfo1)
	// insert t1 values (10, 10), (20, 20)
	tbl1 := testGetTable(t, d, dbInfo1.ID, tblInfo1.ID)
	_, err = tbl1.AddRecord(ctx, types.MakeDatums(1, 1))
	require.NoError(t, err)
	_, err = tbl1.AddRecord(ctx, types.MakeDatums(2, 2))
	require.NoError(t, err)
	// create table t2 (c1 int primary key, c2 int, c3 int);
	tblInfo2, err := testTableInfo(d, "t2", 3)
	require.NoError(t, err)
	tblInfo2.Columns[0].Flag = mysql.PriKeyFlag | mysql.NotNullFlag
	tblInfo2.PKIsHandle = true
	testCreateTable(t, ctx, d, dbInfo1, tblInfo2)
	// insert t2 values (1, 1), (2, 2), (3, 3)
	tbl2 := testGetTable(t, d, dbInfo1.ID, tblInfo2.ID)
	_, err = tbl2.AddRecord(ctx, types.MakeDatums(1, 1, 1))
	require.NoError(t, err)
	_, err = tbl2.AddRecord(ctx, types.MakeDatums(2, 2, 2))
	require.NoError(t, err)
	_, err = tbl2.AddRecord(ctx, types.MakeDatums(3, 3, 3))
	require.NoError(t, err)
	// create database test_parallel_ddl_2;
	dbInfo2, err := testSchemaInfo(d, "test_parallel_ddl_2")
	require.NoError(t, err)
	testCreateSchema(t, ctx, d, dbInfo2)
	// create table t3 (c1 int, c2 int, c3 int, c4 int);
	tblInfo3, err := testTableInfo(d, "t3", 4)
	require.NoError(t, err)
	testCreateTable(t, ctx, d, dbInfo2, tblInfo3)
	// insert t3 values (11, 22, 33, 44)
	tbl3 := testGetTable(t, d, dbInfo2.ID, tblInfo3.ID)
	_, err = tbl3.AddRecord(ctx, types.MakeDatums(11, 22, 33, 44))
	require.NoError(t, err)

	// set hook to execute jobs after all jobs are in queue.
	jobCnt := int64(11)
	tc := &TestDDLCallback{}
	once := sync.Once{}
	var checkErr error
	tc.onJobRunBefore = func(job *model.Job) {
		// TODO: extract a unified function for other tests.
		once.Do(func() {
			qLen1 := int64(0)
			qLen2 := int64(0)
			for {
				checkErr = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
					m := meta.NewMeta(txn)
					qLen1, err = m.DDLJobQueueLen()
					if err != nil {
						return err
					}
					qLen2, err = m.DDLJobQueueLen(meta.AddIndexJobListKey)
					if err != nil {
						return err
					}
					return nil
				})
				if checkErr != nil {
					break
				}
				if qLen1+qLen2 == jobCnt {
					if qLen2 != 5 {
						checkErr = errors.Errorf("add index jobs cnt %v != 6", qLen2)
					}
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
	d.SetHook(tc)

	/*
		prepare jobs:
		/	job no.	/	database no.	/	table no.	/	action type	 /
		/     1		/	 	1			/		1		/	add index	 /
		/     2		/	 	1			/		1		/	add column	 /
		/     3		/	 	1			/		1		/	add index	 /
		/     4		/	 	1			/		2		/	drop column	 /
		/     5		/	 	1			/		1		/	drop index 	 /
		/     6		/	 	1			/		2		/	add index	 /
		/     7		/	 	2			/		3		/	drop column	 /
		/     8		/	 	2			/		3		/	rebase autoID/
		/     9		/	 	1			/		1		/	add index	 /
		/     10	/	 	2			/		null   	/	drop schema  /
		/     11	/	 	2			/		2		/	add index	 /
	*/
	job1 := buildCreateIdxJob(dbInfo1, tblInfo1, false, "db1_idx1", "c1")
	addDDLJob(t, d, job1)
	job2 := buildCreateColumnJob(dbInfo1, tblInfo1, "c3", &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, nil)
	addDDLJob(t, d, job2)
	job3 := buildCreateIdxJob(dbInfo1, tblInfo1, false, "db1_idx2", "c3")
	addDDLJob(t, d, job3)
	job4 := buildDropColumnJob(dbInfo1, tblInfo2, "c3")
	addDDLJob(t, d, job4)
	job5 := buildDropIdxJob(dbInfo1, tblInfo1, "db1_idx1")
	addDDLJob(t, d, job5)
	job6 := buildCreateIdxJob(dbInfo1, tblInfo2, false, "db2_idx1", "c2")
	addDDLJob(t, d, job6)
	job7 := buildDropColumnJob(dbInfo2, tblInfo3, "c4")
	addDDLJob(t, d, job7)
	job8 := buildRebaseAutoIDJobJob(dbInfo2, tblInfo3, 1024)
	addDDLJob(t, d, job8)
	job9 := buildCreateIdxJob(dbInfo1, tblInfo1, false, "db1_idx3", "c2")
	addDDLJob(t, d, job9)
	job10 := buildDropSchemaJob(dbInfo2)
	addDDLJob(t, d, job10)
	job11 := buildCreateIdxJob(dbInfo2, tblInfo3, false, "db3_idx1", "c2")
	addDDLJob(t, d, job11)
	// TODO: add rename table job

	// check results.
	isChecked := false
	for !isChecked {
		err := kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
			m := meta.NewMeta(txn)
			lastJob, err := m.GetHistoryDDLJob(job11.ID)
			require.NoError(t, err)
			// all jobs are finished.
			if lastJob != nil {
				finishedJobs, err := m.GetAllHistoryDDLJobs()
				require.NoError(t, err)
				// get the last 12 jobs completed.
				finishedJobs = finishedJobs[len(finishedJobs)-11:]
				// check some jobs are ordered because of the dependence.
				require.Equal(t, finishedJobs[0].ID, job1.ID, "%v", finishedJobs)
				require.Equal(t, finishedJobs[1].ID, job2.ID, "%v", finishedJobs)
				require.Equal(t, finishedJobs[2].ID, job3.ID, "%v", finishedJobs)
				require.Equal(t, finishedJobs[4].ID, job5.ID, "%v", finishedJobs)
				require.Equal(t, finishedJobs[10].ID, job11.ID, "%v", finishedJobs)
				// check the jobs are ordered in the backfill-job queue or general-job queue.
				backfillJobID := int64(0)
				generalJobID := int64(0)
				for _, job := range finishedJobs {
					// check jobs' order.
					if mayNeedReorg(job) {
						require.Greater(t, job.ID, backfillJobID)
						backfillJobID = job.ID
					} else {
						require.Greater(t, job.ID, generalJobID)
						generalJobID = job.ID
					}
					// check jobs' state.
					if job.ID == lastJob.ID {
						require.Equal(t, job.State, model.JobStateCancelled, "job: %v", job)
					} else {
						require.Equal(t, job.State, model.JobStateSynced, "job: %v", job)
					}
				}

				isChecked = true
			}
			return nil
		})
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}

	require.NoError(t, checkErr)
	tc = &TestDDLCallback{}
	d.SetHook(tc)
}

func TestDDLPackageExecuteSQL(t *testing.T) {
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
	testCheckOwner(t, d, true)
	defer func() {
		require.NoError(t, d.Stop())
	}()
	worker := d.generalWorker()
	require.NotNil(t, worker)

	// In test environment, worker.ctxPool will be nil, and get will return mock.Context.
	// We just test that can use it to call sqlexec.SQLExecutor.Execute.
	sess, err := worker.sessPool.get()
	require.NoError(t, err)
	defer worker.sessPool.put(sess)
	se := sess.(sqlexec.SQLExecutor)
	_, _ = se.Execute(context.Background(), "create table t(a int);")
}

func (s *testDDLSerialSuiteToVerify) checkDropIndexes(d *ddl, schemaID int64, tableID int64, idxNames []model.CIStr, success bool) {
	for _, idxName := range idxNames {
		checkIdxExist(s.T(), d, schemaID, tableID, idxName.O, !success)
	}
}
