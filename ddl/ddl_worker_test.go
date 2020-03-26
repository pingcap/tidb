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
	"context"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/sqlexec"
)

var _ = Suite(&testDDLSuite{})
var _ = Suite(&testDDLSerialSuite{})

type testDDLSuite struct{}
type testDDLSerialSuite struct{}

const testLease = 5 * time.Millisecond

func (s *testDDLSerialSuite) SetUpSuite(c *C) {
	SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)

	// We hope that this test is serially executed. So put it here.
	s.testRunWorker(c)
}

func (s *testDDLSuite) TearDownSuite(c *C) {
}

func (s *testDDLSuite) TestCheckOwner(c *C) {
	store := testCreateStore(c, "test_owner")
	defer store.Close()

	d1 := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	defer d1.Stop()
	time.Sleep(testLease)
	testCheckOwner(c, d1, true)

	c.Assert(d1.GetLease(), Equals, testLease)
}

// testRunWorker tests no job is handled when the value of RunWorker is false.
func (s *testDDLSerialSuite) testRunWorker(c *C) {
	store := testCreateStore(c, "test_run_worker")
	defer store.Close()

	RunWorker = false
	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	testCheckOwner(c, d, false)
	defer d.Stop()

	// Make sure the DDL worker is nil.
	worker := d.generalWorker()
	c.Assert(worker, IsNil)
	// Make sure the DDL job can be done and exit that goroutine.
	RunWorker = true
	d1 := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	testCheckOwner(c, d1, true)
	defer d1.Stop()
	worker = d1.generalWorker()
	c.Assert(worker, NotNil)
}

func (s *testDDLSuite) TestSchemaError(c *C) {
	store := testCreateStore(c, "test_schema_error")
	defer store.Close()

	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	defer d.Stop()
	ctx := testNewContext(d)

	doDDLJobErr(c, 1, 0, model.ActionCreateSchema, []interface{}{1}, ctx, d)
}

func (s *testDDLSuite) TestTableError(c *C) {
	store := testCreateStore(c, "test_table_error")
	defer store.Close()

	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
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
		_, err1 := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
		c.Assert(err1, NotNil)
		job.SchemaID = dbInfo.ID
		_, err1 = getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
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

func (s *testDDLSuite) TestViewError(c *C) {
	store := testCreateStore(c, "test_view_error")
	defer store.Close()

	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	defer d.Stop()
	ctx := testNewContext(d)
	dbInfo := testSchemaInfo(c, d, "test")
	testCreateSchema(c, testNewContext(d), d, dbInfo)

	// Table ID or schema ID is wrong, so getting table is failed.
	tblInfo := testViewInfo(c, d, "t", 3)
	testCreateView(c, ctx, d, dbInfo, tblInfo)

	// Args is wrong, so creating view is failed.
	doDDLJobErr(c, 1, 1, model.ActionCreateView, []interface{}{1}, ctx, d)
	// Schema ID is wrong and orReplace is false, so creating view is failed.
	doDDLJobErr(c, -1, tblInfo.ID, model.ActionCreateView, []interface{}{tblInfo, false}, ctx, d)
	// View exists and orReplace is false, so creating view is failed.
	tblInfo.ID = tblInfo.ID + 1
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionCreateView, []interface{}{tblInfo, false}, ctx, d)

}

func (s *testDDLSuite) TestInvalidDDLJob(c *C) {
	store := testCreateStore(c, "test_invalid_ddl_job_type_error")
	defer store.Close()
	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
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
	c.Assert(err.Error(), Equals, "[ddl:8204]invalid ddl job type: none")
}

func (s *testDDLSuite) TestForeignKeyError(c *C) {
	store := testCreateStore(c, "test_foreign_key_error")
	defer store.Close()

	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
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

	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
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
			[]*ast.IndexPartSpecification{{Column: &ast.ColumnName{Name: model.NewCIStr("c")}, Length: 256}}}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("c1_index"), 1,
			[]*ast.IndexPartSpecification{{Column: &ast.ColumnName{Name: model.NewCIStr("c")}, Length: 256}}}, ctx, d)
	testCreateIndex(c, ctx, d, dbInfo, tblInfo, false, "c1_index", "c1")
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("c1_index"), 1,
			[]*ast.IndexPartSpecification{{Column: &ast.ColumnName{Name: model.NewCIStr("c1")}, Length: 256}}}, ctx, d)

	// for dropping index
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, []interface{}{1}, ctx, d)
	testDropIndex(c, ctx, d, dbInfo, tblInfo, "c1_index")
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, []interface{}{model.NewCIStr("c1_index")}, ctx, d)
}

func (s *testDDLSuite) TestColumnError(c *C) {
	store := testCreateStore(c, "test_column_error")
	defer store.Close()
	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
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

	cols := &[]*model.ColumnInfo{col}
	positions := &[]*ast.ColumnPosition{pos}

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

	// for adding columns
	doDDLJobErr(c, -1, tblInfo.ID, model.ActionAddColumns, []interface{}{cols, positions, 0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, -1, model.ActionAddColumns, []interface{}{cols, positions, 0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddColumns, []interface{}{0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddColumns, []interface{}{cols, positions, 0}, ctx, d)

	// for dropping columns
	doDDLJobErr(c, -1, tblInfo.ID, model.ActionDropColumns, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, -1, model.ActionDropColumns, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropColumns, []interface{}{0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropColumns, []interface{}{[]model.CIStr{model.NewCIStr("c5"), model.NewCIStr("c6")}}, ctx, d)
}

func testCheckOwner(c *C, d *ddl, expectedVal bool) {
	c.Assert(d.isOwner(), Equals, expectedVal)
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
		c.Assert(historyJob.IsCancelled() || historyJob.IsRollbackDone(), IsTrue, Commentf("history job %s", historyJob))
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
	c.Assert(err, NotNil, Commentf("err:%v", err))
	testCheckJobCancelled(c, d, job, state)

	return job
}

func doDDLJobSuccess(ctx sessionctx.Context, d *ddl, c *C, schemaID, tableID int64, tp model.ActionType,
	args []interface{}) {
	job := &model.Job{
		SchemaID:   schemaID,
		TableID:    tableID,
		Type:       tp,
		Args:       args,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
}

func doDDLJobErr(c *C, schemaID, tableID int64, tp model.ActionType, args []interface{},
	ctx sessionctx.Context, d *ddl) *model.Job {
	return doDDLJobErrWithSchemaState(ctx, d, c, schemaID, tableID, tp, args, nil)
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
		{act: model.ActionAddForeignKey, jobIDs: []int64{firstID + 19}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionAddForeignKey, jobIDs: []int64{firstID + 20}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 20)}, cancelState: model.StatePublic},
		{act: model.ActionDropForeignKey, jobIDs: []int64{firstID + 21}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionDropForeignKey, jobIDs: []int64{firstID + 22}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 22)}, cancelState: model.StatePublic},

		{act: model.ActionRenameTable, jobIDs: []int64{firstID + 23}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionRenameTable, jobIDs: []int64{firstID + 24}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 24)}, cancelState: model.StatePublic},

		{act: model.ActionModifyTableCharsetAndCollate, jobIDs: []int64{firstID + 25}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionModifyTableCharsetAndCollate, jobIDs: []int64{firstID + 26}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 26)}, cancelState: model.StatePublic},
		{act: model.ActionTruncateTablePartition, jobIDs: []int64{firstID + 27}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionTruncateTablePartition, jobIDs: []int64{firstID + 28}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 28)}, cancelState: model.StatePublic},
		{act: model.ActionModifySchemaCharsetAndCollate, jobIDs: []int64{firstID + 30}, cancelRetErrs: noErrs, cancelState: model.StateNone},
		{act: model.ActionModifySchemaCharsetAndCollate, jobIDs: []int64{firstID + 31}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 31)}, cancelState: model.StatePublic},

		{act: model.ActionAddPrimaryKey, jobIDs: []int64{firstID + 32}, cancelRetErrs: noErrs, cancelState: model.StateDeleteOnly},
		{act: model.ActionAddPrimaryKey, jobIDs: []int64{firstID + 33}, cancelRetErrs: noErrs, cancelState: model.StateWriteOnly},
		{act: model.ActionAddPrimaryKey, jobIDs: []int64{firstID + 34}, cancelRetErrs: noErrs, cancelState: model.StateWriteReorganization},
		{act: model.ActionAddPrimaryKey, jobIDs: []int64{firstID + 35}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 35)}, cancelState: model.StatePublic},
		{act: model.ActionDropPrimaryKey, jobIDs: []int64{firstID + 36}, cancelRetErrs: noErrs, cancelState: model.StateWriteOnly},
		{act: model.ActionDropPrimaryKey, jobIDs: []int64{firstID + 37}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 37)}, cancelState: model.StateDeleteOnly},

		{act: model.ActionAddColumns, jobIDs: []int64{firstID + 38}, cancelRetErrs: noErrs, cancelState: model.StateDeleteOnly},
		{act: model.ActionAddColumns, jobIDs: []int64{firstID + 39}, cancelRetErrs: noErrs, cancelState: model.StateWriteOnly},
		{act: model.ActionAddColumns, jobIDs: []int64{firstID + 40}, cancelRetErrs: noErrs, cancelState: model.StateWriteReorganization},
		{act: model.ActionAddColumns, jobIDs: []int64{firstID + 41}, cancelRetErrs: []error{admin.ErrCancelFinishedDDLJob.GenWithStackByArgs(firstID + 41)}, cancelState: model.StatePublic},

		{act: model.ActionDropColumns, jobIDs: []int64{firstID + 42}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 42)}, cancelState: model.StateDeleteOnly},
		{act: model.ActionDropColumns, jobIDs: []int64{firstID + 43}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 43)}, cancelState: model.StateWriteOnly},
		{act: model.ActionDropColumns, jobIDs: []int64{firstID + 44}, cancelRetErrs: []error{admin.ErrCannotCancelDDLJob.GenWithStackByArgs(firstID + 44)}, cancelState: model.StateWriteReorganization},
	}

	return tests
}

func (s *testDDLSuite) checkDropIdx(c *C, d *ddl, schemaID int64, tableID int64, idxName string, success bool) {
	checkIdxExist(c, d, schemaID, tableID, idxName, !success)
}

func (s *testDDLSuite) checkAddIdx(c *C, d *ddl, schemaID int64, tableID int64, idxName string, success bool) {
	checkIdxExist(c, d, schemaID, tableID, idxName, success)
}

func checkIdxExist(c *C, d *ddl, schemaID int64, tableID int64, idxName string, expectedExist bool) {
	changedTable := testGetTable(c, d, schemaID, tableID)
	var found bool
	for _, idxInfo := range changedTable.Meta().Indices {
		if idxInfo.Name.O == idxName {
			found = true
			break
		}
	}
	c.Assert(found, Equals, expectedExist)
}

func (s *testDDLSuite) checkAddColumns(c *C, d *ddl, schemaID int64, tableID int64, colNames []string, success bool) {
	changedTable := testGetTable(c, d, schemaID, tableID)
	var found bool
	for _, colName := range colNames {
		for _, colInfo := range changedTable.Meta().Columns {
			if colInfo.Name.O == colName {
				found = true
				break
			}
		}
		c.Assert(found, Equals, success)
	}
}

func (s *testDDLSuite) checkCancelDropColumns(c *C, d *ddl, schemaID int64, tableID int64, colNames []string, success bool) {
	changedTable := testGetTable(c, d, schemaID, tableID)
	notFound := checkColumnsNotFound(changedTable, colNames)
	c.Assert(notFound, Equals, success)
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

func (s *testDDLSuite) TestCancelJob(c *C) {
	store := testCreateStore(c, "test_cancel_job")
	defer store.Close()
	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	defer d.Stop()
	dbInfo := testSchemaInfo(c, d, "test_cancel_job")
	testCreateSchema(c, testNewContext(d), d, dbInfo)
	// create a partition table.
	partitionTblInfo := testTableInfoWithPartition(c, d, "t_partition", 5)
	// Skip using sessPool. Make sure adding primary key can be successful.
	partitionTblInfo.Columns[0].Flag |= mysql.NotNullFlag
	// create table t (c1 int, c2 int, c3 int, c4 int, c5 int);
	tblInfo := testTableInfo(c, d, "t", 5)
	ctx := testNewContext(d)
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	testCreateTable(c, ctx, d, dbInfo, partitionTblInfo)
	tableAutoID := int64(100)
	shardRowIDBits := uint64(5)
	tblInfo.AutoIncID = tableAutoID
	tblInfo.ShardRowIDBits = shardRowIDBits
	job := testCreateTable(c, ctx, d, dbInfo, tblInfo)
	// insert t values (1, 2, 3, 4, 5);
	originTable := testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	row := types.MakeDatums(1, 2, 3, 4, 5)
	_, err = originTable.AddRecord(ctx, row)
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

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
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[1])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[2])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[3])
	testCreateIndex(c, ctx, d, dbInfo, tblInfo, false, "idx", "c2")
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, true)

	// for add column
	updateTest(&tests[4])
	addingColName := "colA"
	newColumnDef := &ast.ColumnDef{
		Name:    &ast.ColumnName{Name: model.NewCIStr(addingColName)},
		Tp:      &types.FieldType{Tp: mysql.TypeLonglong},
		Options: []*ast.ColumnOption{},
	}
	col, _, err := buildColumnAndConstraint(ctx, 2, newColumnDef, nil, mysql.DefaultCharset, "", mysql.DefaultCharset, "")
	c.Assert(err, IsNil)

	addColumnArgs := []interface{}{col, &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, 0}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, addColumnArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumns(c, d, dbInfo.ID, tblInfo.ID, []string{addingColName}, false)

	updateTest(&tests[5])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, addColumnArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumns(c, d, dbInfo.ID, tblInfo.ID, []string{addingColName}, false)

	updateTest(&tests[6])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, addColumnArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumns(c, d, dbInfo.ID, tblInfo.ID, []string{addingColName}, false)

	updateTest(&tests[7])
	testAddColumn(c, ctx, d, dbInfo, tblInfo, addColumnArgs)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumns(c, d, dbInfo.ID, tblInfo.ID, []string{addingColName}, true)

	// for create table
	tblInfo1 := testTableInfo(c, d, "t1", 2)
	updateTest(&tests[8])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo1.ID, model.ActionCreateTable, []interface{}{tblInfo1}, &cancelState)
	c.Check(checkErr, IsNil)
	testCheckTableState(c, d, dbInfo, tblInfo1, model.StateNone)

	// for create database
	dbInfo1 := testSchemaInfo(c, d, "test_cancel_job1")
	updateTest(&tests[9])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo1.ID, 0, model.ActionCreateSchema, []interface{}{dbInfo1}, &cancelState)
	c.Check(checkErr, IsNil)
	testCheckSchemaState(c, d, dbInfo1, model.StateNone)

	// for drop column.
	updateTest(&tests[10])
	dropColName := "c3"
	s.checkCancelDropColumns(c, d, dbInfo.ID, tblInfo.ID, []string{dropColName}, false)
	testDropColumn(c, ctx, d, dbInfo, tblInfo, dropColName, false)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkCancelDropColumns(c, d, dbInfo.ID, tblInfo.ID, []string{dropColName}, true)

	updateTest(&tests[11])
	dropColName = "c4"
	s.checkCancelDropColumns(c, d, dbInfo.ID, tblInfo.ID, []string{dropColName}, false)
	testDropColumn(c, ctx, d, dbInfo, tblInfo, dropColName, false)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkCancelDropColumns(c, d, dbInfo.ID, tblInfo.ID, []string{dropColName}, true)

	updateTest(&tests[12])
	dropColName = "c5"
	s.checkCancelDropColumns(c, d, dbInfo.ID, tblInfo.ID, []string{dropColName}, false)
	testDropColumn(c, ctx, d, dbInfo, tblInfo, dropColName, false)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkCancelDropColumns(c, d, dbInfo.ID, tblInfo.ID, []string{dropColName}, true)

	// cancel rebase auto id
	updateTest(&tests[13])
	rebaseIDArgs := []interface{}{int64(200)}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionRebaseAutoID, rebaseIDArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	changedTable := testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedTable.Meta().AutoIncID, Equals, tableAutoID)

	// cancel shard bits
	updateTest(&tests[14])
	shardRowIDArgs := []interface{}{uint64(7)}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionShardRowID, shardRowIDArgs, &cancelState)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedTable.Meta().ShardRowIDBits, Equals, shardRowIDBits)

	// modify column
	col.DefaultValue = "1"
	updateTest(&tests[15])
	modifyColumnArgs := []interface{}{col, col.Name, &ast.ColumnPosition{}, byte(0)}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, modifyColumnArgs, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	changedCol := model.FindColumnInfo(changedTable.Meta().Columns, col.Name.L)
	c.Assert(changedCol.DefaultValue, IsNil)

	// Test add foreign key failed cause by canceled.
	updateTest(&tests[16])
	addForeignKeyArgs := []interface{}{model.FKInfo{Name: model.NewCIStr("fk1")}}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, addForeignKeyArgs, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(len(changedTable.Meta().ForeignKeys), Equals, 0)

	// Test add foreign key successful.
	updateTest(&tests[17])
	doDDLJobSuccess(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, addForeignKeyArgs)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(len(changedTable.Meta().ForeignKeys), Equals, 1)
	c.Assert(changedTable.Meta().ForeignKeys[0].Name, Equals, addForeignKeyArgs[0].(model.FKInfo).Name)

	// Test drop foreign key failed cause by canceled.
	updateTest(&tests[18])
	dropForeignKeyArgs := []interface{}{addForeignKeyArgs[0].(model.FKInfo).Name}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, dropForeignKeyArgs, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(len(changedTable.Meta().ForeignKeys), Equals, 1)
	c.Assert(changedTable.Meta().ForeignKeys[0].Name, Equals, dropForeignKeyArgs[0].(model.CIStr))

	// Test drop foreign key successful.
	updateTest(&tests[19])
	doDDLJobSuccess(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, dropForeignKeyArgs)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(len(changedTable.Meta().ForeignKeys), Equals, 0)

	// test rename table failed caused by canceled.
	test = &tests[20]
	renameTableArgs := []interface{}{dbInfo.ID, model.NewCIStr("t2")}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, renameTableArgs, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedTable.Meta().Name.L, Equals, "t")

	// test rename table successful.
	test = &tests[21]
	doDDLJobSuccess(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, renameTableArgs)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedTable.Meta().Name.L, Equals, "t2")

	// test modify table charset failed caused by canceled.
	test = &tests[22]
	modifyTableCharsetArgs := []interface{}{"utf8mb4", "utf8mb4_bin"}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, modifyTableCharsetArgs, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedTable.Meta().Charset, Equals, "utf8")
	c.Assert(changedTable.Meta().Collate, Equals, "utf8_bin")

	// test modify table charset successfully.
	test = &tests[23]
	doDDLJobSuccess(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, modifyTableCharsetArgs)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedTable.Meta().Charset, Equals, "utf8mb4")
	c.Assert(changedTable.Meta().Collate, Equals, "utf8mb4_bin")

	// test truncate table partition failed caused by canceled.
	test = &tests[24]
	truncateTblPartitionArgs := []interface{}{partitionTblInfo.Partition.Definitions[0].ID}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, partitionTblInfo.ID, test.act, truncateTblPartitionArgs, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, partitionTblInfo.ID)
	c.Assert(changedTable.Meta().Partition.Definitions[0].ID == partitionTblInfo.Partition.Definitions[0].ID, IsTrue)

	// test truncate table partition charset successfully.
	test = &tests[25]
	doDDLJobSuccess(ctx, d, c, dbInfo.ID, partitionTblInfo.ID, test.act, truncateTblPartitionArgs)
	c.Check(checkErr, IsNil)
	changedTable = testGetTable(c, d, dbInfo.ID, partitionTblInfo.ID)
	c.Assert(changedTable.Meta().Partition.Definitions[0].ID == partitionTblInfo.Partition.Definitions[0].ID, IsFalse)

	// test modify schema charset failed caused by canceled.
	test = &tests[26]
	charsetAndCollate := []interface{}{"utf8mb4", "utf8mb4_bin"}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, charsetAndCollate, &test.cancelState)
	c.Check(checkErr, IsNil)
	dbInfo, err = testGetSchemaInfoWithError(d, dbInfo.ID)
	c.Assert(err, IsNil)
	c.Assert(dbInfo.Charset, Equals, "")
	c.Assert(dbInfo.Collate, Equals, "")

	// test modify table charset successfully.
	test = &tests[27]
	doDDLJobSuccess(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, charsetAndCollate)
	c.Check(checkErr, IsNil)
	dbInfo, err = testGetSchemaInfoWithError(d, dbInfo.ID)
	c.Assert(err, IsNil)
	c.Assert(dbInfo.Charset, Equals, "utf8mb4")
	c.Assert(dbInfo.Collate, Equals, "utf8mb4_bin")

	// for adding primary key
	tblInfo = changedTable.Meta()
	updateTest(&tests[28])
	idxOrigName = "primary"
	validArgs = []interface{}{false, model.NewCIStr(idxOrigName),
		[]*ast.IndexPartSpecification{{
			Column: &ast.ColumnName{Name: model.NewCIStr("c1")},
			Length: -1,
		}}, nil}
	cancelState = model.StateNone
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddPrimaryKey, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[29])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddPrimaryKey, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[30])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddPrimaryKey, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[31])
	testCreatePrimaryKey(c, ctx, d, dbInfo, tblInfo, "c1")
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, true)

	// for dropping primary key
	updateTest(&tests[32])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionDropPrimaryKey, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkDropIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	updateTest(&tests[33])
	testDropIndex(c, ctx, d, dbInfo, tblInfo, idxOrigName)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkDropIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, true)

	// for add columns
	updateTest(&tests[34])
	addingColNames := []string{"colA", "colB", "colC", "colD", "colE", "colF"}
	var cols []*table.Column
	for _, addingColName := range addingColNames {
		newColumnDef := &ast.ColumnDef{
			Name:    &ast.ColumnName{Name: model.NewCIStr(addingColName)},
			Tp:      &types.FieldType{Tp: mysql.TypeLonglong},
			Options: []*ast.ColumnOption{},
		}
		col, _, err := buildColumnAndConstraint(ctx, 0, newColumnDef, nil, mysql.DefaultCharset, "", mysql.DefaultCharset, "")
		c.Assert(err, IsNil)
		cols = append(cols, col)
	}
	offsets := make([]int, len(cols))
	positions := make([]*ast.ColumnPosition, len(cols))
	for i := range positions {
		positions[i] = &ast.ColumnPosition{Tp: ast.ColumnPositionNone}
	}

	addColumnArgs = []interface{}{cols, positions, offsets}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddColumns, addColumnArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumns(c, d, dbInfo.ID, tblInfo.ID, addingColNames, false)

	updateTest(&tests[35])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddColumns, addColumnArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumns(c, d, dbInfo.ID, tblInfo.ID, addingColNames, false)

	updateTest(&tests[36])
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddColumns, addColumnArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumns(c, d, dbInfo.ID, tblInfo.ID, addingColNames, false)

	updateTest(&tests[37])
	testAddColumns(c, ctx, d, dbInfo, tblInfo, addColumnArgs)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddColumns(c, d, dbInfo.ID, tblInfo.ID, addingColNames, true)

	// for drop columns
	updateTest(&tests[38])
	dropColNames := []string{"colA", "colB"}
	s.checkCancelDropColumns(c, d, dbInfo.ID, tblInfo.ID, dropColNames, false)
	testDropColumns(c, ctx, d, dbInfo, tblInfo, dropColNames, false)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkCancelDropColumns(c, d, dbInfo.ID, tblInfo.ID, dropColNames, true)

	updateTest(&tests[39])
	dropColNames = []string{"colC", "colD"}
	s.checkCancelDropColumns(c, d, dbInfo.ID, tblInfo.ID, dropColNames, false)
	testDropColumns(c, ctx, d, dbInfo, tblInfo, dropColNames, false)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkCancelDropColumns(c, d, dbInfo.ID, tblInfo.ID, dropColNames, true)

	updateTest(&tests[40])
	dropColNames = []string{"colE", "colF"}
	s.checkCancelDropColumns(c, d, dbInfo.ID, tblInfo.ID, dropColNames, false)
	testDropColumns(c, ctx, d, dbInfo, tblInfo, dropColNames, false)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkCancelDropColumns(c, d, dbInfo.ID, tblInfo.ID, dropColNames, true)
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
	store := testCreateStore(c, "test_set_job_relation")
	defer store.Close()

	// Add some non-add-index jobs.
	job1 := &model.Job{ID: 1, TableID: 1, Type: model.ActionAddColumn}
	job2 := &model.Job{ID: 2, TableID: 1, Type: model.ActionCreateTable}
	job3 := &model.Job{ID: 3, TableID: 2, Type: model.ActionDropColumn}
	job6 := &model.Job{ID: 6, TableID: 1, Type: model.ActionDropTable}
	job7 := &model.Job{ID: 7, TableID: 2, Type: model.ActionModifyColumn}
	job9 := &model.Job{ID: 9, SchemaID: 111, Type: model.ActionDropSchema}
	job11 := &model.Job{ID: 11, TableID: 2, Type: model.ActionRenameTable, Args: []interface{}{int64(111), "old db name"}}
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
		err = t.EnQueueDDLJob(job9)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job11)
		c.Assert(err, IsNil)
		return nil
	})
	job4 := &model.Job{ID: 4, TableID: 1, Type: model.ActionAddIndex}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job4)
		c.Assert(err, IsNil)
		c.Assert(job4.DependencyID, Equals, int64(2))
		return nil
	})
	job5 := &model.Job{ID: 5, TableID: 2, Type: model.ActionAddIndex}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job5)
		c.Assert(err, IsNil)
		c.Assert(job5.DependencyID, Equals, int64(3))
		return nil
	})
	job8 := &model.Job{ID: 8, TableID: 3, Type: model.ActionAddIndex}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job8)
		c.Assert(err, IsNil)
		c.Assert(job8.DependencyID, Equals, int64(0))
		return nil
	})
	job10 := &model.Job{ID: 10, SchemaID: 111, TableID: 3, Type: model.ActionAddIndex}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job10)
		c.Assert(err, IsNil)
		c.Assert(job10.DependencyID, Equals, int64(9))
		return nil
	})
	job12 := &model.Job{ID: 12, SchemaID: 112, TableID: 2, Type: model.ActionAddIndex}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job12)
		c.Assert(err, IsNil)
		c.Assert(job12.DependencyID, Equals, int64(11))
		return nil
	})
}

func addDDLJob(c *C, d *ddl, job *model.Job) {
	task := &limitJobTask{job, make(chan error)}
	d.limitJobCh <- task
	err := <-task.err
	c.Assert(err, IsNil)
}

func (s *testDDLSuite) TestParallelDDL(c *C) {
	store := testCreateStore(c, "test_parallel_ddl")
	defer store.Close()
	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	defer d.Stop()
	ctx := testNewContext(d)
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
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
	dbInfo1 := testSchemaInfo(c, d, "test_parallel_ddl_1")
	testCreateSchema(c, ctx, d, dbInfo1)
	// create table t1 (c1 int, c2 int);
	tblInfo1 := testTableInfo(c, d, "t1", 2)
	testCreateTable(c, ctx, d, dbInfo1, tblInfo1)
	// insert t1 values (10, 10), (20, 20)
	tbl1 := testGetTable(c, d, dbInfo1.ID, tblInfo1.ID)
	_, err = tbl1.AddRecord(ctx, types.MakeDatums(1, 1))
	c.Assert(err, IsNil)
	_, err = tbl1.AddRecord(ctx, types.MakeDatums(2, 2))
	c.Assert(err, IsNil)
	// create table t2 (c1 int primary key, c2 int, c3 int);
	tblInfo2 := testTableInfo(c, d, "t2", 3)
	tblInfo2.Columns[0].Flag = mysql.PriKeyFlag | mysql.NotNullFlag
	tblInfo2.PKIsHandle = true
	testCreateTable(c, ctx, d, dbInfo1, tblInfo2)
	// insert t2 values (1, 1), (2, 2), (3, 3)
	tbl2 := testGetTable(c, d, dbInfo1.ID, tblInfo2.ID)
	_, err = tbl2.AddRecord(ctx, types.MakeDatums(1, 1, 1))
	c.Assert(err, IsNil)
	_, err = tbl2.AddRecord(ctx, types.MakeDatums(2, 2, 2))
	c.Assert(err, IsNil)
	_, err = tbl2.AddRecord(ctx, types.MakeDatums(3, 3, 3))
	c.Assert(err, IsNil)
	// create database test_parallel_ddl_2;
	dbInfo2 := testSchemaInfo(c, d, "test_parallel_ddl_2")
	testCreateSchema(c, ctx, d, dbInfo2)
	// create table t3 (c1 int, c2 int, c3 int, c4 int);
	tblInfo3 := testTableInfo(c, d, "t3", 4)
	testCreateTable(c, ctx, d, dbInfo2, tblInfo3)
	// insert t3 values (11, 22, 33, 44)
	tbl3 := testGetTable(c, d, dbInfo2.ID, tblInfo3.ID)
	_, err = tbl3.AddRecord(ctx, types.MakeDatums(11, 22, 33, 44))
	c.Assert(err, IsNil)

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
				checkErr = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
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
						checkErr = errors.Errorf("add index jobs cnt %v != 5", qLen2)
					}
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
	d.SetHook(tc)
	c.Assert(checkErr, IsNil)

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
	addDDLJob(c, d, job1)
	job2 := buildCreateColumnJob(dbInfo1, tblInfo1, "c3", &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, nil)
	addDDLJob(c, d, job2)
	job3 := buildCreateIdxJob(dbInfo1, tblInfo1, false, "db1_idx2", "c3")
	addDDLJob(c, d, job3)
	job4 := buildDropColumnJob(dbInfo1, tblInfo2, "c3")
	addDDLJob(c, d, job4)
	job5 := buildDropIdxJob(dbInfo1, tblInfo1, "db1_idx1")
	addDDLJob(c, d, job5)
	job6 := buildCreateIdxJob(dbInfo1, tblInfo2, false, "db2_idx1", "c2")
	addDDLJob(c, d, job6)
	job7 := buildDropColumnJob(dbInfo2, tblInfo3, "c4")
	addDDLJob(c, d, job7)
	job8 := buildRebaseAutoIDJobJob(dbInfo2, tblInfo3, 1024)
	addDDLJob(c, d, job8)
	job9 := buildCreateIdxJob(dbInfo1, tblInfo1, false, "db1_idx3", "c2")
	addDDLJob(c, d, job9)
	job10 := buildDropSchemaJob(dbInfo2)
	addDDLJob(c, d, job10)
	job11 := buildCreateIdxJob(dbInfo2, tblInfo3, false, "db3_idx1", "c2")
	addDDLJob(c, d, job11)
	// TODO: add rename table job

	// check results.
	isChecked := false
	for !isChecked {
		kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
			m := meta.NewMeta(txn)
			lastJob, err := m.GetHistoryDDLJob(job11.ID)
			c.Assert(err, IsNil)
			// all jobs are finished.
			if lastJob != nil {
				finishedJobs, err := m.GetAllHistoryDDLJobs()
				c.Assert(err, IsNil)
				// get the last 11 jobs completed.
				finishedJobs = finishedJobs[len(finishedJobs)-11:]
				// check some jobs are ordered because of the dependence.
				c.Assert(finishedJobs[0].ID, Equals, job1.ID)
				c.Assert(finishedJobs[1].ID, Equals, job2.ID)
				c.Assert(finishedJobs[2].ID, Equals, job3.ID)
				c.Assert(finishedJobs[4].ID, Equals, job5.ID)
				c.Assert(finishedJobs[10].ID, Equals, job11.ID)
				// check the jobs are ordered in the adding-index-job queue or general-job queue.
				addIdxJobID := int64(0)
				generalJobID := int64(0)
				for _, job := range finishedJobs {
					// check jobs' order.
					if job.Type == model.ActionAddIndex {
						c.Assert(job.ID, Greater, addIdxJobID)
						addIdxJobID = job.ID
					} else {
						c.Assert(job.ID, Greater, generalJobID)
						generalJobID = job.ID
					}
					// check jobs' state.
					if job.ID == lastJob.ID {
						c.Assert(job.State, Equals, model.JobStateCancelled, Commentf("job: %v", job))
					} else {
						c.Assert(job.State, Equals, model.JobStateSynced, Commentf("job: %v", job))
					}
				}

				isChecked = true
			}
			return nil
		})
		time.Sleep(10 * time.Millisecond)
	}

	tc = &TestDDLCallback{}
	d.SetHook(tc)
}

func (s *testDDLSuite) TestDDLPackageExecuteSQL(c *C) {
	store := testCreateStore(c, "test_run_sql")
	defer store.Close()

	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	testCheckOwner(c, d, true)
	defer d.Stop()
	worker := d.generalWorker()
	c.Assert(worker, NotNil)

	// In test environment, worker.ctxPool will be nil, and get will return mock.Context.
	// We just test that can use it to call sqlexec.SQLExecutor.Execute.
	sess, err := worker.sessPool.get()
	c.Assert(err, IsNil)
	defer worker.sessPool.put(sess)
	se := sess.(sqlexec.SQLExecutor)
	_, _ = se.Execute(context.Background(), "create table t(a int);")
}
