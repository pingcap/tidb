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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"golang.org/x/net/context"
)

var _ = Suite(&testDDLSuite{})

type testDDLSuite struct{}

const testLease = 5 * time.Millisecond

func (s *testDDLSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	// set ReorgWaitTimeout to small value, make test to be faster.
	ReorgWaitTimeout = 50 * time.Millisecond
}

func (s *testDDLSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testDDLSuite) TestCheckOwner(c *C) {
	store := testCreateStore(c, "test_owner")
	defer store.Close()

	d1 := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d1.Stop()
	time.Sleep(testLease)
	testCheckOwner(c, d1, true)

	d1.SetLease(context.Background(), 1*time.Second)
	d1.SetLease(context.Background(), 2*time.Second)
	c.Assert(d1.GetLease(), Equals, 2*time.Second)
}

// TestRunWorker tests no job is handled when the value of RunWorker is false.
func (s *testDDLSuite) TestRunWorker(c *C) {
	store := testCreateStore(c, "test_run_worker")
	defer store.Close()

	RunWorker = false
	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	testCheckOwner(c, d, false)
	defer d.Stop()
	ctx := testNewContext(d)

	dbInfo := testSchemaInfo(c, d, "test")
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}

	exitCh := make(chan struct{})
	go func(ch chan struct{}) {
		err := d.doDDLJob(ctx, job)
		c.Assert(err, IsNil)
		close(ch)
	}(exitCh)
	// Make sure the DDL job is in the DDL job queue.
	// The reason for doing it twice is to eliminate the operation in the start function.
	<-d.ddlJobCh
	<-d.ddlJobCh
	// Make sure the DDL job doesn't be handled.
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		job, err := d.getFirstDDLJob(t)
		c.Assert(err, IsNil)
		c.Assert(job, NotNil)
		c.Assert(job.SchemaID, Equals, dbInfo.ID, Commentf("job %s", job))
		return nil
	})
	// Make sure the DDL job can be done and exit that goroutine.
	RunWorker = true
	d1 := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	testCheckOwner(c, d1, true)
	defer d1.Stop()
	asyncNotify(d1.ddlJobCh)
	<-exitCh
}

func (s *testDDLSuite) TestCleanJobs(c *C) {
	defer testleak.AfterTest(c)()
	store := testCreateStore(c, "test_clean_jobs")
	defer store.Close()
	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)

	ctx := testNewContext(d)
	dbInfo := testSchemaInfo(c, d, "test")
	testCreateSchema(c, ctx, d, dbInfo)
	tblInfo := testTableInfo(c, d, "t", 2)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)

	var failedJobIDs []int64
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
	}
	idxColNames := []*ast.IndexColName{{
		Column: &ast.ColumnName{Name: model.NewCIStr("c1")},
		Length: types.UnspecifiedLength}}
	// Add some adding index jobs to AddIndexJobList.
	backfillAddIndexJob := func(jobArgs []interface{}) {
		kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
			var err error
			t := meta.NewMeta(txn)
			t.SetJobListKey(meta.AddIndexJobListKey)
			job.ID, err = t.GenGlobalID()
			c.Assert(err, IsNil)
			failedJobIDs = append(failedJobIDs, job.ID)
			job.Args = jobArgs
			err = t.EnQueueDDLJob(job)
			c.Assert(err, IsNil)
			return nil
		})
	}

	// Add a StateNone job.
	indexName := model.NewCIStr("idx_none")
	args := []interface{}{false, indexName, idxColNames, nil}
	backfillAddIndexJob(args)
	// Add a StateDeleteOnly job.
	indexName = model.NewCIStr("idx_delete_only")
	args = []interface{}{false, indexName, idxColNames, nil}
	backfillAddIndexJob(args)
	changeJobState := func() {
		kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
			t := meta.NewMeta(txn)
			t.SetJobListKey(meta.AddIndexJobListKey)
			lastJobID := int64(len(failedJobIDs) - 1)
			job, err1 := t.GetDDLJob(lastJobID)
			c.Assert(err1, IsNil)
			_, err1 = d.runDDLJob(t, job)
			c.Assert(err1, IsNil)
			_, err1 = updateSchemaVersion(t, job)
			c.Assert(err1, IsNil)
			err1 = t.UpdateDDLJob(lastJobID, job, true)
			c.Assert(err1, IsNil)
			return nil
		})
		err := d.callHookOnChanged(nil)
		c.Assert(err, IsNil)
	}
	changeJobState()
	// Add a StateWriteReorganization job.
	indexName = model.NewCIStr("idx_write_reorg")
	args = []interface{}{false, indexName, idxColNames, nil}
	backfillAddIndexJob(args)
	changeJobState() // convert to delete only
	changeJobState() // convert to write only
	changeJobState() // convert to write reorg

	err := d.Stop()
	c.Assert(err, IsNil)
	// Make sure shouldCleanJobs is ture.
	d = testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()

	// Make sure all DDL jobs are done.
	for {
		var isAllJobDone bool
		kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
			t := meta.NewMeta(txn)
			len, err := t.DDLJobQueueLen()
			c.Assert(err, IsNil)
			t.SetJobListKey(meta.AddIndexJobListKey)
			addIndexLen, err := t.DDLJobQueueLen()
			c.Assert(err, IsNil)
			if len == 0 && addIndexLen == 0 {
				isAllJobDone = true
			}
			return nil
		})
		if isAllJobDone {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// Check that the jobs in add index list are finished.
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		for i, id := range failedJobIDs {
			historyJob, err := t.GetHistoryDDLJob(id)
			c.Assert(err, IsNil)
			c.Assert(historyJob, NotNil, Commentf("job %v", historyJob))
			if i == 0 {
				c.Assert(historyJob.State, Equals, model.JobStateCancelled)
			} else {
				c.Assert(historyJob.State, Equals, model.JobStateRollbackDone)
			}
		}
		return nil
	})
}

func (s *testDDLSuite) TestSchemaError(c *C) {
	store := testCreateStore(c, "test_schema_error")
	defer store.Close()

	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)

	doDDLJobErr(c, 1, 0, model.ActionCreateSchema, []interface{}{1}, ctx, d)
}

func (s *testDDLSuite) TestTableError(c *C) {
	store := testCreateStore(c, "test_table_error")
	defer store.Close()

	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)

	// Schema ID is wrong, so dropping table is failed.
	doDDLJobErr(c, -1, 1, model.ActionDropTable, nil, ctx, d)
	// Table ID is wrong, so dropping table is failed.
	dbInfo := testSchemaInfo(c, d, "test")
	testCreateSchema(c, testNewContext(d), d, dbInfo)
	job := doDDLJobErr(c, dbInfo.ID, -1, model.ActionDropTable, nil, ctx, d)

	// Table ID or schema ID is wrong, so getting table is failed.
	tblInfo := testTableInfo(c, d, "t", 3)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)
	err := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		job.SchemaID = -1
		job.TableID = -1
		t := meta.NewMeta(txn)
		_, err1 := getTableInfo(t, job, job.SchemaID)
		c.Assert(err1, NotNil)
		job.SchemaID = dbInfo.ID
		_, err1 = getTableInfo(t, job, job.SchemaID)
		c.Assert(err1, NotNil)
		return nil
	})
	c.Assert(err, IsNil)

	// Args is wrong, so creating table is failed.
	doDDLJobErr(c, 1, 1, model.ActionCreateTable, []interface{}{1}, ctx, d)
	// Schema ID is wrong, so creating table is failed.
	doDDLJobErr(c, -1, tblInfo.ID, model.ActionCreateTable, []interface{}{tblInfo}, ctx, d)
	// Table exists, so creating table is failed.
	tblInfo.ID = tblInfo.ID + 1
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionCreateTable, []interface{}{tblInfo}, ctx, d)

}

func (s *testDDLSuite) TestInvalidDDLJob(c *C) {
	store := testCreateStore(c, "test_invalid_ddl_job_type_error")
	defer store.Close()
	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)

	job := &model.Job{
		SchemaID:   0,
		TableID:    0,
		Type:       model.ActionNone,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err.Error(), Equals, "[ddl:3]invalid ddl job type: none")
}

func (s *testDDLSuite) TestForeignKeyError(c *C) {
	store := testCreateStore(c, "test_foreign_key_error")
	defer store.Close()

	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)

	doDDLJobErr(c, -1, 1, model.ActionAddForeignKey, nil, ctx, d)
	doDDLJobErr(c, -1, 1, model.ActionDropForeignKey, nil, ctx, d)

	dbInfo := testSchemaInfo(c, d, "test")
	tblInfo := testTableInfo(c, d, "t", 3)
	testCreateSchema(c, ctx, d, dbInfo)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropForeignKey, []interface{}{model.NewCIStr("c1_foreign_key")}, ctx, d)
}

func (s *testDDLSuite) TestIndexError(c *C) {
	store := testCreateStore(c, "test_index_error")
	defer store.Close()

	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)

	// Schema ID is wrong.
	doDDLJobErr(c, -1, 1, model.ActionAddIndex, nil, ctx, d)
	doDDLJobErr(c, -1, 1, model.ActionDropIndex, nil, ctx, d)

	dbInfo := testSchemaInfo(c, d, "test")
	tblInfo := testTableInfo(c, d, "t", 3)
	testCreateSchema(c, ctx, d, dbInfo)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)

	// for adding index
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, []interface{}{1}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("t"), 1,
			[]*ast.IndexColName{{Column: &ast.ColumnName{Name: model.NewCIStr("c")}, Length: 256}}}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("c1_index"), 1,
			[]*ast.IndexColName{{Column: &ast.ColumnName{Name: model.NewCIStr("c")}, Length: 256}}}, ctx, d)
	testCreateIndex(c, ctx, d, dbInfo, tblInfo, false, "c1_index", "c1")
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("c1_index"), 1,
			[]*ast.IndexColName{{Column: &ast.ColumnName{Name: model.NewCIStr("c1")}, Length: 256}}}, ctx, d)

	// for dropping index
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, []interface{}{1}, ctx, d)
	testDropIndex(c, ctx, d, dbInfo, tblInfo, "c1_index")
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, []interface{}{model.NewCIStr("c1_index")}, ctx, d)
}

func (s *testDDLSuite) TestColumnError(c *C) {
	store := testCreateStore(c, "test_column_error")
	defer store.Close()
	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)

	dbInfo := testSchemaInfo(c, d, "test")
	tblInfo := testTableInfo(c, d, "t", 3)
	testCreateSchema(c, ctx, d, dbInfo)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)
	col := &model.ColumnInfo{
		Name:         model.NewCIStr("c4"),
		Offset:       len(tblInfo.Columns),
		DefaultValue: 0,
	}
	col.ID = allocateColumnID(tblInfo)
	col.FieldType = *types.NewFieldType(mysql.TypeLong)
	pos := &ast.ColumnPosition{Tp: ast.ColumnPositionAfter, RelativeColumn: &ast.ColumnName{Name: model.NewCIStr("c5")}}

	// for adding column
	doDDLJobErr(c, -1, tblInfo.ID, model.ActionAddColumn, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, -1, model.ActionAddColumn, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, []interface{}{0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, []interface{}{col, pos, 0}, ctx, d)

	// for dropping column
	doDDLJobErr(c, -1, tblInfo.ID, model.ActionDropColumn, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, -1, model.ActionDropColumn, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropColumn, []interface{}{0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropColumn, []interface{}{model.NewCIStr("c5")}, ctx, d)
}

func testCheckOwner(c *C, d *ddl, isOwner bool) {
	c.Assert(d.isOwner(), Equals, isOwner)
}

func testCheckJobDone(c *C, d *ddl, job *model.Job, isAdd bool) {
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err := t.GetHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		checkHistoryJob(c, historyJob)
		if isAdd {
			c.Assert(historyJob.SchemaState, Equals, model.StatePublic)
		} else {
			c.Assert(historyJob.SchemaState, Equals, model.StateNone)
		}

		return nil
	})
}

func testCheckJobCancelled(c *C, d *ddl, job *model.Job, state *model.SchemaState) {
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err := t.GetHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		c.Assert(historyJob.IsCancelled() || historyJob.IsRollbackDone(), IsTrue, Commentf("histroy job %s", historyJob))
		if state != nil {
			c.Assert(historyJob.SchemaState, Equals, *state)
		}
		return nil
	})
}

func doDDLJobErrWithSchemaState(ctx sessionctx.Context, d *ddl, c *C, schemaID, tableID int64, tp model.ActionType,
	args []interface{}, state *model.SchemaState) *model.Job {
	job := &model.Job{
		SchemaID:   schemaID,
		TableID:    tableID,
		Type:       tp,
		Args:       args,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	// TODO: Add the detail error check.
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job, state)

	return job
}

func doDDLJobErr(c *C, schemaID, tableID int64, tp model.ActionType, args []interface{},
	ctx sessionctx.Context, d *ddl) *model.Job {
	return doDDLJobErrWithSchemaState(ctx, d, c, schemaID, tableID, tp, args, nil)
}

func checkCancelState(txn kv.Transaction, job *model.Job, test *testCancelJob) error {
	var checkErr error
	if test.cancelState == job.SchemaState {
		if job.SchemaState == model.StateNone && job.State != model.JobStateDone {
			// If the schema state is none, we only test the job is finished.
		} else {
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
	}
	return checkErr
}

type testCancelJob struct {
	act           model.ActionType // act is the job action.
	jobIDs        []int64
	cancelRetErrs []error // cancelRetErrs is the first return value of CancelJobs.
	cancelState   model.SchemaState
	ddlRetErr     error
}

func buildCancelJobTests(firstID int64) []testCancelJob {
	err := errCancelledDDLJob
	errs := []error{err}
	noErrs := []error{nil}
	tests := []testCancelJob{
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 1}, cancelRetErrs: noErrs, cancelState: model.StateDeleteOnly, ddlRetErr: err},
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 2}, cancelRetErrs: noErrs, cancelState: model.StateWriteOnly, ddlRetErr: err},
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 3}, cancelRetErrs: noErrs, cancelState: model.StateWriteReorganization, ddlRetErr: err},
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 4}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenByArgs(firstID + 4)}, cancelState: model.StatePublic, ddlRetErr: err},

		// TODO: after fix drop index and create table rollback bug, the below test cases maybe need to change.
		{act: model.ActionDropIndex, jobIDs: []int64{firstID + 5}, cancelRetErrs: errs, cancelState: model.StateWriteOnly, ddlRetErr: err},
		{act: model.ActionDropIndex, jobIDs: []int64{firstID + 6}, cancelRetErrs: errs, cancelState: model.StateDeleteOnly, ddlRetErr: err},
		{act: model.ActionDropIndex, jobIDs: []int64{firstID + 7}, cancelRetErrs: errs, cancelState: model.StateDeleteReorganization, ddlRetErr: err},
		{act: model.ActionDropIndex, jobIDs: []int64{firstID + 8}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenByArgs(firstID + 8)}, cancelState: model.StateNone, ddlRetErr: err},

		// TODO: add create table back after we fix the cancel bug.
		//{act: model.ActionCreateTable, jobIDs: []int64{firstID + 9}, cancelRetErrs: noErrs, cancelState: model.StatePublic, ddlRetErr: err},
		{act: model.ActionAddColumn, jobIDs: []int64{firstID + 9}, cancelRetErrs: noErrs, cancelState: model.StateDeleteOnly, ddlRetErr: err},
		{act: model.ActionAddColumn, jobIDs: []int64{firstID + 10}, cancelRetErrs: noErrs, cancelState: model.StateWriteOnly, ddlRetErr: err},
		{act: model.ActionAddColumn, jobIDs: []int64{firstID + 11}, cancelRetErrs: noErrs, cancelState: model.StateWriteReorganization, ddlRetErr: err},
		{act: model.ActionAddColumn, jobIDs: []int64{firstID + 12}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenByArgs(firstID + 12)}, cancelState: model.StatePublic, ddlRetErr: err},
	}

	return tests
}

func (s *testDDLSuite) checkAddIdx(c *C, d *ddl, schemaID int64, tableID int64, idxName string, success bool) {
	changedTable := testGetTable(c, d, schemaID, tableID)
	var found bool
	for _, idxInfo := range changedTable.Meta().Indices {
		if idxInfo.Name.O == idxName {
			found = true
			break
		}
	}
	c.Assert(found, Equals, success)
}
func (s *testDDLSuite) checkAddColumn(c *C, d *ddl, schemaID int64, tableID int64, colName string, success bool) {
	changedTable := testGetTable(c, d, schemaID, tableID)
	var found bool
	for _, colInfo := range changedTable.Meta().Columns {
		if colInfo.Name.O == colName {
			found = true
			break
		}
	}
	c.Assert(found, Equals, success)
}

func (s *testDDLSuite) TestCancelJob(c *C) {
	store := testCreateStore(c, "test_cancel_job")
	defer store.Close()
	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	dbInfo := testSchemaInfo(c, d, "test_cancel_job")
	testCreateSchema(c, testNewContext(d), d, dbInfo)

	// create table t (c1 int, c2 int);
	tblInfo := testTableInfo(c, d, "t", 2)
	ctx := testNewContext(d)
	err := ctx.NewTxn()
	c.Assert(err, IsNil)
	job := testCreateTable(c, ctx, d, dbInfo, tblInfo)
	// insert t values (1, 2);
	originTable := testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	row := types.MakeDatums(1, 2)
	_, err = originTable.AddRecord(ctx, row, false)
	c.Assert(err, IsNil)
	err = ctx.Txn(true).Commit(context.Background())
	c.Assert(err, IsNil)

	tc := &TestDDLCallback{}
	// set up hook
	firstJobID := job.ID
	tests := buildCancelJobTests(firstJobID)
	var checkErr error
	var test *testCancelJob
	tc.onJobUpdated = func(job *model.Job) {
		if job.State == model.JobStateSynced || job.State == model.JobStateCancelled || job.State == model.JobStateCancelling {
			return
		}

		if checkErr != nil {
			return
		}
		hookCtx := mock.NewContext()
		hookCtx.Store = store
		var err1 error
		err1 = hookCtx.NewTxn()
		if err1 != nil {
			checkErr = errors.Trace(err1)
			return
		}
		checkErr = checkCancelState(hookCtx.Txn(true), job, test)
		if checkErr != nil {
			return
		}
		checkCancelState(hookCtx.Txn(true), job, test)
		err1 = hookCtx.Txn(true).Commit(context.Background())
		if err1 != nil {
			checkErr = errors.Trace(err1)
			return
		}
	}
	d.SetHook(tc)

	// for adding index
	test = &tests[0]
	idxOrigName := "idx"
	validArgs := []interface{}{false, model.NewCIStr(idxOrigName),
		[]*ast.IndexColName{{
			Column: &ast.ColumnName{Name: model.NewCIStr("c1")},
			Length: -1,
		}}, nil}
	// When the job satisfies this test case, the option will be rollback, so the job's schema state is none.
	cancelState := model.StateNone
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	test = &tests[1]
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	test = &tests[2]
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	test = &tests[3]
	testCreateIndex(c, ctx, d, dbInfo, tblInfo, false, "idx", "c2")
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	c.Assert(ctx.Txn(true).Commit(context.Background()), IsNil)
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, true)

	// for dropping index
	idxName := []interface{}{model.NewCIStr("idx")}
	test = &tests[4]
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, idxName, &test.cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	test = &tests[5]
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, idxName, &test.cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	test = &tests[6]
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, idxName, &test.cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	test = &tests[7]
	testDropIndex(c, ctx, d, dbInfo, tblInfo, "idx")
	c.Check(errors.ErrorStack(checkErr), Equals, "")

	// for add column
	test = &tests[8]
	addingColName := "colA"
	newColumnDef := &ast.ColumnDef{
		Name:    &ast.ColumnName{Name: model.NewCIStr(addingColName)},
		Tp:      &types.FieldType{Tp: mysql.TypeLonglong},
		Options: []*ast.ColumnOption{},
	}
	col, _, err := buildColumnAndConstraint(ctx, 2, newColumnDef)
	addColumnArgs := []interface{}{col, &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, 0}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, addColumnArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumn(c, d, dbInfo.ID, tblInfo.ID, addingColName, false)
	test = &tests[9]
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, addColumnArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumn(c, d, dbInfo.ID, tblInfo.ID, addingColName, false)
	test = &tests[10]
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, addColumnArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumn(c, d, dbInfo.ID, tblInfo.ID, addingColName, false)
	test = &tests[11]
	testAddColumn(c, ctx, d, dbInfo, tblInfo, addColumnArgs)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumn(c, d, dbInfo.ID, tblInfo.ID, addingColName, true)
}

func (s *testDDLSuite) TestIgnorableSpec(c *C) {
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
		c.Assert(isIgnorableSpec(spec), IsFalse)
	}

	ignorableSpecs := []ast.AlterTableType{
		ast.AlterTableLock,
		ast.AlterTableAlgorithm,
	}
	for _, spec := range ignorableSpecs {
		c.Assert(isIgnorableSpec(spec), IsTrue)
	}
}

func (s *testDDLSuite) TestBuildJobDependence(c *C) {
	defer testleak.AfterTest(c)()
	store := testCreateStore(c, "test_set_job_relation")
	defer store.Close()

	job1 := &model.Job{ID: 1, TableID: 1}
	job2 := &model.Job{ID: 2, TableID: 1}
	job3 := &model.Job{ID: 3, TableID: 2}
	job6 := &model.Job{ID: 6, TableID: 1}
	job7 := &model.Job{ID: 7, TableID: 2}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := t.EnQueueDDLJob(job1)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job2)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job3)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job6)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job7)
		c.Assert(err, IsNil)
		return nil
	})
	job4 := &model.Job{ID: 4, TableID: 1}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job4)
		c.Assert(err, IsNil)
		c.Assert(job4.DependencyID, Equals, int64(2))
		return nil
	})
	job5 := &model.Job{ID: 5, TableID: 2}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job5)
		c.Assert(err, IsNil)
		c.Assert(job5.DependencyID, Equals, int64(3))
		return nil
	})
	job8 := &model.Job{ID: 8, TableID: 3}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job8)
		c.Assert(err, IsNil)
		c.Assert(job8.DependencyID, Equals, int64(0))
		return nil
	})
}
