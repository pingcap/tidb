// Copyright 2016 PingCAP, Inc.
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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type testColumnChangeSuiteToVerify struct {
	suite.Suite
	store  kv.Storage
	dbInfo *model.DBInfo
}

func TestColumnChangeSuite(t *testing.T) {
	_, err := infosync.GlobalInfoSyncerInit(context.Background(), "t", func() uint64 { return 1 }, nil, true)
	require.NoError(t, err)

	suite.Run(t, new(testColumnChangeSuiteToVerify))
}

func (s *testColumnChangeSuiteToVerify) SetupSuite() {
	SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	s.store = createMockStore(s.T())
	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	defer func() {
		err := d.Stop()
		require.NoError(s.T(), err)
	}()
	s.dbInfo, err = testSchemaInfo(d, "test_index_change")
	require.NoError(s.T(), err)
	testCreateSchema(s.T(), testNewContext(d), d, s.dbInfo)
}

func (s *testColumnChangeSuiteToVerify) TearDownSuite() {
	s.store.Close()
}

func (s *testColumnChangeSuiteToVerify) TestColumnChange() {
	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	defer func() {
		err := d.Stop()
		require.NoError(s.T(), err)
	}()
	// create table t (c1 int, c2 int);
	tblInfo, err := testTableInfo(d, "t", 2)
	require.NoError(s.T(), err)
	ctx := testNewContext(d)
	err = ctx.NewTxn(context.Background())
	require.NoError(s.T(), err)
	testCreateTable(s.T(), ctx, d, s.dbInfo, tblInfo)
	// insert t values (1, 2);
	originTable := testGetTable(s.T(), d, s.dbInfo.ID, tblInfo.ID)
	row := types.MakeDatums(1, 2)
	h, err := originTable.AddRecord(ctx, row)
	require.NoError(s.T(), err)
	txn, err := ctx.Txn(true)
	require.NoError(s.T(), err)
	err = txn.Commit(context.Background())
	require.NoError(s.T(), err)

	var mu sync.Mutex
	tc := &TestDDLCallback{}
	// set up hook
	prevState := model.StateNone
	var (
		deleteOnlyTable table.Table
		writeOnlyTable  table.Table
		publicTable     table.Table
	)
	var checkErr error
	tc.onJobUpdated = func(job *model.Job) {
		if job.SchemaState == prevState {
			return
		}
		hookCtx := mock.NewContext()
		hookCtx.Store = s.store
		prevState = job.SchemaState
		err := hookCtx.NewTxn(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
		}
		switch job.SchemaState {
		case model.StateDeleteOnly:
			deleteOnlyTable, err = getCurrentTable(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
		case model.StateWriteOnly:
			writeOnlyTable, err = getCurrentTable(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			err = s.checkAddWriteOnly(hookCtx, d, deleteOnlyTable, writeOnlyTable, h)
			if err != nil {
				checkErr = errors.Trace(err)
			}
		case model.StatePublic:
			mu.Lock()
			publicTable, err = getCurrentTable(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			err = s.checkAddPublic(hookCtx, d, writeOnlyTable, publicTable)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			mu.Unlock()
		}
		txn, err := hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
		}
		err = txn.Commit(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
		}
	}
	d.SetHook(tc)
	defaultValue := int64(3)
	job := testCreateColumn(s.T(), ctx, d, s.dbInfo, tblInfo, "c3", &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, defaultValue)
	require.NoError(s.T(), checkErr)
	testCheckJobDone(s.T(), d, job, true)
	mu.Lock()
	tb := publicTable
	mu.Unlock()
	s.testColumnDrop(ctx, d, tb)
	s.testAddColumnNoDefault(ctx, d, tblInfo)
}

func (s *testColumnChangeSuiteToVerify) TestModifyAutoRandColumnWithMetaKeyChanged() {
	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	defer func() {
		err := d.Stop()
		require.NoError(s.T(), err)
	}()

	ids, err := d.genGlobalIDs(1)
	tableID := ids[0]
	require.NoError(s.T(), err)
	colInfo := &model.ColumnInfo{
		Name:      model.NewCIStr("a"),
		Offset:    0,
		State:     model.StatePublic,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
	}
	tblInfo := &model.TableInfo{
		ID:             tableID,
		Name:           model.NewCIStr("auto_random_table_name"),
		Columns:        []*model.ColumnInfo{colInfo},
		AutoRandomBits: 5,
	}
	colInfo.ID = allocateColumnID(tblInfo)
	ctx := testNewContext(d)
	testCreateTable(s.T(), ctx, d, s.dbInfo, tblInfo)

	tc := &TestDDLCallback{}
	var errCount int32 = 3
	var genAutoRandErr error
	tc.onJobRunBefore = func(job *model.Job) {
		if atomic.LoadInt32(&errCount) > 0 && job.Type == model.ActionModifyColumn {
			atomic.AddInt32(&errCount, -1)
			genAutoRandErr = kv.RunInNewTxn(context.Background(), s.store, false, func(ctx context.Context, txn kv.Transaction) error {
				t := meta.NewMeta(txn)
				_, err1 := t.GetAutoIDAccessors(s.dbInfo.ID, tableID).RandomID().Inc(1)
				return err1
			})
		}
	}
	d.SetHook(tc)
	const newAutoRandomBits uint64 = 10
	job := &model.Job{
		SchemaID:   s.dbInfo.ID,
		TableID:    tblInfo.ID,
		SchemaName: s.dbInfo.Name.L,
		Type:       model.ActionModifyColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{colInfo, colInfo.Name, ast.ColumnPosition{}, 0, newAutoRandomBits},
	}
	err = d.doDDLJob(ctx, job)
	require.NoError(s.T(), err)
	require.True(s.T(), errCount == 0)
	require.Nil(s.T(), genAutoRandErr)
	testCheckJobDone(s.T(), d, job, true)
	var newTbInfo *model.TableInfo
	err = kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err error
		newTbInfo, err = t.GetTable(s.dbInfo.ID, tableID)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	})
	require.NoError(s.T(), err)
	require.Equal(s.T(), newTbInfo.AutoRandomBits, newAutoRandomBits)
}

func (s *testColumnChangeSuiteToVerify) testAddColumnNoDefault(ctx sessionctx.Context, d *ddl, tblInfo *model.TableInfo) {
	tc := &TestDDLCallback{}
	// set up hook
	prevState := model.StateNone
	var checkErr error
	var writeOnlyTable table.Table
	tc.onJobUpdated = func(job *model.Job) {
		if job.SchemaState == prevState {
			return
		}
		hookCtx := mock.NewContext()
		hookCtx.Store = s.store
		prevState = job.SchemaState
		err := hookCtx.NewTxn(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
		}
		switch job.SchemaState {
		case model.StateWriteOnly:
			writeOnlyTable, err = getCurrentTable(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
		case model.StatePublic:
			_, err = getCurrentTable(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			_, err = writeOnlyTable.AddRecord(hookCtx, types.MakeDatums(10, 10))
			if err != nil {
				checkErr = errors.Trace(err)
			}
		}
		txn, err := hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
		}
		err = txn.Commit(context.TODO())
		if err != nil {
			checkErr = errors.Trace(err)
		}
	}
	d.SetHook(tc)
	job := testCreateColumn(s.T(), ctx, d, s.dbInfo, tblInfo, "c3", &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, nil)
	require.NoError(s.T(), checkErr)
	testCheckJobDone(s.T(), d, job, true)
}

func (s *testColumnChangeSuiteToVerify) testColumnDrop(ctx sessionctx.Context, d *ddl, tbl table.Table) {
	dropCol := tbl.Cols()[2]
	tc := &TestDDLCallback{}
	// set up hook
	prevState := model.StateNone
	var checkErr error
	tc.onJobUpdated = func(job *model.Job) {
		if job.SchemaState == prevState {
			return
		}
		prevState = job.SchemaState
		currentTbl, err := getCurrentTable(d, s.dbInfo.ID, tbl.Meta().ID)
		if err != nil {
			checkErr = errors.Trace(err)
		}
		for _, col := range currentTbl.Cols() {
			if col.ID == dropCol.ID {
				checkErr = errors.Errorf("column is not dropped")
			}
		}
	}
	d.SetHook(tc)
	require.NoError(s.T(), checkErr)
	testDropColumn(s.T(), ctx, d, s.dbInfo, tbl.Meta(), dropCol.Name.L, false)
}

func seek(t table.PhysicalTable, ctx sessionctx.Context, h kv.Handle) (kv.Handle, bool, error) {
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, false, err
	}
	recordPrefix := t.RecordPrefix()
	seekKey := tablecodec.EncodeRowKeyWithHandle(t.GetPhysicalID(), h)
	iter, err := txn.Iter(seekKey, recordPrefix.PrefixNext())
	if err != nil {
		return nil, false, err
	}
	if !iter.Valid() || !iter.Key().HasPrefix(recordPrefix) {
		// No more records in the table, skip to the end.
		return nil, false, nil
	}
	handle, err := tablecodec.DecodeRowKey(iter.Key())
	if err != nil {
		return nil, false, err
	}
	return handle, true, nil
}

func (s *testColumnChangeSuiteToVerify) checkAddWriteOnly(ctx sessionctx.Context, d *ddl, deleteOnlyTable, writeOnlyTable table.Table, h kv.Handle) error {
	// WriteOnlyTable: insert t values (2, 3)
	err := ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	_, err = writeOnlyTable.AddRecord(ctx, types.MakeDatums(2, 3))
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	err = checkResult(ctx, writeOnlyTable, writeOnlyTable.WritableCols(),
		testutil.RowsWithSep(" ", "1 2 <nil>", "2 3 3"))
	if err != nil {
		return errors.Trace(err)
	}
	// This test is for RowWithCols when column state is StateWriteOnly.
	row, err := tables.RowWithCols(writeOnlyTable, ctx, h, writeOnlyTable.WritableCols())
	if err != nil {
		return errors.Trace(err)
	}
	got := fmt.Sprintf("%v", row)
	expect := fmt.Sprintf("%v", []types.Datum{types.NewDatum(1), types.NewDatum(2), types.NewDatum(nil)})
	if got != expect {
		return errors.Errorf("expect %v, got %v", expect, got)
	}
	// DeleteOnlyTable: select * from t
	err = checkResult(ctx, deleteOnlyTable, deleteOnlyTable.WritableCols(), testutil.RowsWithSep(" ", "1 2", "2 3"))
	if err != nil {
		return errors.Trace(err)
	}
	// WriteOnlyTable: update t set c1 = 2 where c1 = 1
	h, _, err = seek(writeOnlyTable.(table.PhysicalTable), ctx, kv.IntHandle(0))
	if err != nil {
		return errors.Trace(err)
	}
	err = writeOnlyTable.UpdateRecord(context.Background(), ctx, h, types.MakeDatums(1, 2, 3), types.MakeDatums(2, 2, 3), touchedSlice(writeOnlyTable))
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	// After we update the first row, its default value is also set.
	err = checkResult(ctx, writeOnlyTable, writeOnlyTable.WritableCols(), testutil.RowsWithSep(" ", "2 2 3", "2 3 3"))
	if err != nil {
		return errors.Trace(err)
	}
	// DeleteOnlyTable: delete from t where c2 = 2
	err = deleteOnlyTable.RemoveRecord(ctx, h, types.MakeDatums(2, 2))
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	// After delete table has deleted the first row, check the WriteOnly table records.
	err = checkResult(ctx, writeOnlyTable, writeOnlyTable.WritableCols(), testutil.RowsWithSep(" ", "2 3 3"))
	return errors.Trace(err)
}

func touchedSlice(t table.Table) []bool {
	touched := make([]bool, 0, len(t.WritableCols()))
	for range t.WritableCols() {
		touched = append(touched, true)
	}
	return touched
}

func (s *testColumnChangeSuiteToVerify) checkAddPublic(sctx sessionctx.Context, d *ddl, writeOnlyTable, publicTable table.Table) error {
	ctx := context.TODO()
	// publicTable Insert t values (4, 4, 4)
	err := sctx.NewTxn(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	h, err := publicTable.AddRecord(sctx, types.MakeDatums(4, 4, 4))
	if err != nil {
		return errors.Trace(err)
	}
	err = sctx.NewTxn(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// writeOnlyTable update t set c1 = 3 where c1 = 4
	oldRow, err := tables.RowWithCols(writeOnlyTable, sctx, h, writeOnlyTable.WritableCols())
	if err != nil {
		return errors.Trace(err)
	}
	if len(oldRow) != 3 {
		return errors.Errorf("%v", oldRow)
	}
	newRow := types.MakeDatums(3, 4, oldRow[2].GetValue())
	err = writeOnlyTable.UpdateRecord(context.Background(), sctx, h, oldRow, newRow, touchedSlice(writeOnlyTable))
	if err != nil {
		return errors.Trace(err)
	}
	err = sctx.NewTxn(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// publicTable select * from t, make sure the new c3 value 4 is not overwritten to default value 3.
	err = checkResult(sctx, publicTable, publicTable.WritableCols(), testutil.RowsWithSep(" ", "2 3 3", "3 4 4"))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func getCurrentTable(d *ddl, schemaID, tableID int64) (table.Table, error) {
	var tblInfo *model.TableInfo
	err := kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err error
		tblInfo, err = t.GetTable(schemaID, tableID)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	alloc := autoid.NewAllocator(d.store, schemaID, tblInfo.ID, false, autoid.RowIDAllocType)
	tbl, err := table.TableFromMeta(autoid.NewAllocators(alloc), tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tbl, err
}

func checkResult(ctx sessionctx.Context, t table.Table, cols []*table.Column, rows [][]interface{}) error {
	var gotRows [][]interface{}
	err := tables.IterRecords(t, ctx, cols, func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
		gotRows = append(gotRows, datumsToInterfaces(data))
		return true, nil
	})
	if err != nil {
		return err
	}
	got := fmt.Sprintf("%v", gotRows)
	expect := fmt.Sprintf("%v", rows)
	if got != expect {
		return errors.Errorf("expect %v, got %v", expect, got)
	}
	return nil
}

func datumsToInterfaces(datums []types.Datum) []interface{} {
	ifs := make([]interface{}, 0, len(datums))
	for _, d := range datums {
		ifs = append(ifs, d.GetValue())
	}
	return ifs
}
