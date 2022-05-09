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

package ddl_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestColumnAdd(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	tk := testkit.NewTestKit(t, store)
	internal := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 int, c2 int);")
	tk.MustExec("insert t values (1, 2);")

	d := dom.DDL()
	tc := &ddl.TestDDLCallback{Do: dom}

	ct := testNewContext(store)
	// set up hook
	var (
		deleteOnlyTable table.Table
		writeOnlyTable  table.Table
		publicTable     table.Table
		dropCol         *table.Column
	)
	first := true
	var jobID int64
	tc.OnJobUpdatedExported = func(job *model.Job) {
		jobID = job.ID
		require.NoError(t, dom.Reload())
		tbl, exist := dom.InfoSchema().TableByID(job.TableID)
		require.True(t, exist)
		switch job.SchemaState {
		case model.StateDeleteOnly:
			deleteOnlyTable = tbl
		case model.StateWriteOnly:
			writeOnlyTable = tbl
			require.NoError(t, checkAddWriteOnly(ct, deleteOnlyTable, writeOnlyTable, kv.IntHandle(1)))
		case model.StatePublic:
			if first {
				first = false
			} else {
				return
			}
			publicTable = tbl
			require.NoError(t, checkAddPublic(ct, writeOnlyTable, publicTable))
		}
	}
	d.SetHook(tc)
	tk.MustExec("alter table t add column c3 int default 3")
	tb := publicTable
	v := getSchemaVer(t, tk.Session())
	checkHistoryJobArgs(t, tk.Session(), jobID, &historyJobArgs{ver: v, tbl: tb.Meta()})

	// Drop column.
	tc.OnJobRunBeforeExported = func(job *model.Job) {
		if dropCol == nil {
			tbl := external.GetTableByName(t, internal, "test", "t")
			dropCol = tbl.VisibleCols()[2]
		}
	}
	tc.OnJobUpdatedExported = func(job *model.Job) {
		jobID = job.ID
		tbl := external.GetTableByName(t, internal, "test", "t")
		if job.SchemaState != model.StatePublic {
			for _, col := range tbl.Cols() {
				require.NotEqualf(t, col.ID, dropCol.ID, "column is not dropped")
			}
		}
	}
	d.SetHook(tc)
	tk.MustExec("alter table t drop column c3")
	v = getSchemaVer(t, tk.Session())
	// Don't check column, so it's ok to use tb.
	checkHistoryJobArgs(t, tk.Session(), jobID, &historyJobArgs{ver: v, tbl: tb.Meta()})

	// Add column not default.
	first = true
	tc.OnJobUpdatedExported = func(job *model.Job) {
		jobID = job.ID
		tbl, exist := dom.InfoSchema().TableByID(job.TableID)
		require.True(t, exist)
		switch job.SchemaState {
		case model.StateWriteOnly:
			writeOnlyTable = tbl
		case model.StatePublic:
			if first {
				first = false
			} else {
				return
			}
			sess := testNewContext(store)
			err := sessiontxn.NewTxn(context.Background(), sess)
			require.NoError(t, err)
			_, err = writeOnlyTable.AddRecord(sess, types.MakeDatums(10, 10))
			require.NoError(t, err)
		}
	}
	d.SetHook(tc)
	tk.MustExec("alter table t add column c3 int")
	testCheckJobDone(t, store, jobID, true)
}

func TestModifyAutoRandColumnWithMetaKeyChanged(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a bigint primary key clustered AUTO_RANDOM(5));")

	d := dom.DDL()
	tc := &ddl.TestDDLCallback{Do: dom}

	var errCount int32 = 3
	var genAutoRandErr error
	var dbID int64
	var tID int64
	var jobID int64
	tc.OnJobRunBeforeExported = func(job *model.Job) {
		jobID = job.ID
		dbID = job.SchemaID
		tID = job.TableID
		if atomic.LoadInt32(&errCount) > 0 && job.Type == model.ActionModifyColumn {
			atomic.AddInt32(&errCount, -1)
			genAutoRandErr = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
				t := meta.NewMeta(txn)
				_, err1 := t.GetAutoIDAccessors(dbID, tID).RandomID().Inc(1)
				return err1
			})
		}
	}
	d.SetHook(tc)

	tk.MustExec("alter table t modify column a bigint AUTO_RANDOM(10)")
	require.True(t, errCount == 0)
	require.Nil(t, genAutoRandErr)
	const newAutoRandomBits uint64 = 10
	testCheckJobDone(t, store, jobID, true)
	var newTbInfo *model.TableInfo
	err := kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err error
		newTbInfo, err = t.GetTable(dbID, tID)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, newTbInfo.AutoRandomBits, newAutoRandomBits)
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

func checkAddWriteOnly(ctx sessionctx.Context, deleteOnlyTable, writeOnlyTable table.Table, h kv.Handle) error {
	// WriteOnlyTable: insert t values (2, 3)
	err := sessiontxn.NewTxn(context.Background(), ctx)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = writeOnlyTable.AddRecord(ctx, types.MakeDatums(2, 3))
	if err != nil {
		return errors.Trace(err)
	}
	err = sessiontxn.NewTxn(context.Background(), ctx)
	if err != nil {
		return errors.Trace(err)
	}
	err = checkResult(ctx, writeOnlyTable, writeOnlyTable.WritableCols(), [][]string{
		{"1", "2", "<nil>"},
		{"2", "3", "3"},
	})
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
	err = checkResult(ctx, deleteOnlyTable, deleteOnlyTable.WritableCols(), [][]string{
		{"1", "2"},
		{"2", "3"},
	})
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
	err = sessiontxn.NewTxn(context.Background(), ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// After we update the first row, its default value is also set.
	err = checkResult(ctx, writeOnlyTable, writeOnlyTable.WritableCols(), [][]string{
		{"2", "2", "3"},
		{"2", "3", "3"},
	})
	if err != nil {
		return errors.Trace(err)
	}
	// DeleteOnlyTable: delete from t where c2 = 2
	err = deleteOnlyTable.RemoveRecord(ctx, h, types.MakeDatums(2, 2))
	if err != nil {
		return errors.Trace(err)
	}
	err = sessiontxn.NewTxn(context.Background(), ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// After delete table has deleted the first row, check the WriteOnly table records.
	err = checkResult(ctx, writeOnlyTable, writeOnlyTable.WritableCols(), [][]string{
		{"2", "3", "3"},
	})
	return errors.Trace(err)
}

func touchedSlice(t table.Table) []bool {
	touched := make([]bool, 0, len(t.WritableCols()))
	for range t.WritableCols() {
		touched = append(touched, true)
	}
	return touched
}

func checkAddPublic(sctx sessionctx.Context, writeOnlyTable, publicTable table.Table) error {
	ctx := context.TODO()
	// publicTable Insert t values (4, 4, 4)
	err := sessiontxn.NewTxn(ctx, sctx)
	if err != nil {
		return errors.Trace(err)
	}
	h, err := publicTable.AddRecord(sctx, types.MakeDatums(4, 4, 4))
	if err != nil {
		return errors.Trace(err)
	}
	err = sessiontxn.NewTxn(ctx, sctx)
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
	err = sessiontxn.NewTxn(ctx, sctx)
	if err != nil {
		return errors.Trace(err)
	}
	// publicTable select * from t, make sure the new c3 value 4 is not overwritten to default value 3.
	err = checkResult(sctx, publicTable, publicTable.WritableCols(), [][]string{
		{"2", "3", "3"},
		{"3", "4", "4"},
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func checkResult(ctx sessionctx.Context, t table.Table, cols []*table.Column, rows [][]string) error {
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

type historyJobArgs struct {
	ver    int64
	db     *model.DBInfo
	tbl    *model.TableInfo
	tblIDs map[int64]struct{}
}

func getSchemaVer(t *testing.T, ctx sessionctx.Context) int64 {
	err := sessiontxn.NewTxn(context.Background(), ctx)
	require.NoError(t, err)
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	ver, err := m.GetSchemaVersion()
	require.NoError(t, err)
	return ver
}

func checkEqualTable(t *testing.T, t1, t2 *model.TableInfo) {
	require.Equal(t, t1.ID, t2.ID)
	require.Equal(t, t1.Name, t2.Name)
	require.Equal(t, t1.Charset, t2.Charset)
	require.Equal(t, t1.Collate, t2.Collate)
	require.Equal(t, t1.PKIsHandle, t2.PKIsHandle)
	require.Equal(t, t1.Comment, t2.Comment)
	require.Equal(t, t1.AutoIncID, t2.AutoIncID)
}

func checkHistoryJobArgs(t *testing.T, ctx sessionctx.Context, id int64, args *historyJobArgs) {
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	tran := meta.NewMeta(txn)
	historyJob, err := tran.GetHistoryDDLJob(id)
	require.NoError(t, err)
	require.Greater(t, historyJob.BinlogInfo.FinishedTS, uint64(0))

	if args.tbl != nil {
		require.Equal(t, historyJob.BinlogInfo.SchemaVersion, args.ver)
		checkEqualTable(t, historyJob.BinlogInfo.TableInfo, args.tbl)
		return
	}

	// for handling schema job
	require.Equal(t, historyJob.BinlogInfo.SchemaVersion, args.ver)
	require.Equal(t, historyJob.BinlogInfo.DBInfo, args.db)
	// only for creating schema job
	if args.db != nil && len(args.tblIDs) == 0 {
		return
	}
}

func testCheckJobDone(t *testing.T, store kv.Storage, jobID int64, isAdd bool) {
	require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		historyJob, err := m.GetHistoryDDLJob(jobID)
		require.NoError(t, err)
		require.Equal(t, historyJob.State, model.JobStateSynced)
		if isAdd {
			require.Equal(t, historyJob.SchemaState, model.StatePublic)
		} else {
			require.Equal(t, historyJob.SchemaState, model.StateNone)
		}

		return nil
	}))
}

func testNewContext(store kv.Storage) sessionctx.Context {
	ctx := mock.NewContext()
	ctx.Store = store
	return ctx
}
