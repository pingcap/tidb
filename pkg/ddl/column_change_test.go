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
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestColumnAdd(t *testing.T) {
	store := testkit.CreateMockStore(t)
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	tk := testkit.NewTestKit(t, store)
	se := tk.Session()
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 int, c2 int);")
	tk.MustExec("insert t values (1, 2);")

	var (
		deleteOnlyTable table.Table
		writeOnlyTable  table.Table
		publicTable     table.Table
		dropCol         *table.Column
		jobID           atomic.Int64
	)

	// Add column with default value.
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		if job.State == model.JobStateSynced {
			return
		}
		jobID.Store(job.ID)
		tbl := external.GetTableByName(t, tk, "test", "t")
		switch job.SchemaState {
		case model.StateDeleteOnly:
			deleteOnlyTable = tbl
		case model.StateWriteOnly:
			writeOnlyTable = tbl
			require.NoError(t, checkAddWriteOnly(se, deleteOnlyTable, writeOnlyTable, kv.IntHandle(1)))
		case model.StatePublic:
			publicTable = tbl
			require.NoError(t, checkAddPublic(se, writeOnlyTable, publicTable))
		}
	})
	tk.MustExec("alter table t add column c3 int default 3")
	checkJobWithHistory(t, se, jobID.Load(), nil, publicTable.Meta())

	// Drop column.
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if dropCol == nil {
			tbl := external.GetTableByName(t, tk, "test", "t")
			dropCol = tbl.VisibleCols()[2]
		}
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		if job.NotStarted() {
			return
		}
		jobID.Store(job.ID)
		tbl := external.GetTableByName(t, tk, "test", "t")
		if job.SchemaState != model.StatePublic {
			for _, col := range tbl.Cols() {
				require.NotEqualf(t, col.ID, dropCol.ID, "column is not dropped")
			}
		}
	})
	tk.MustExec("alter table t drop column c3")
	// checkJobWithHistory doesn't check column, so it's ok to use previous one.
	checkJobWithHistory(t, se, jobID.Load(), nil, publicTable.Meta())

	// Add column with no default value set.
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		if job.State == model.JobStateSynced {
			return
		}
		jobID.Store(job.ID)
		tbl := external.GetTableByName(t, tk, "test", "t")
		switch job.SchemaState {
		case model.StateWriteOnly:
			writeOnlyTable = tbl
		case model.StatePublic:
			txn, err := newTxn(se)
			require.NoError(t, err)
			_, err = writeOnlyTable.AddRecord(se.GetTableCtx(), txn, types.MakeDatums(10, 10))
			require.NoError(t, err)
		}
	})
	tk.MustExec("alter table t add column c3 int")
	testCheckJobDone(t, store, jobID.Load(), true)
}

func TestModifyAutoRandColumnWithMetaKeyChanged(t *testing.T) {
	store := testkit.CreateMockStore(t)
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a bigint primary key clustered AUTO_RANDOM(5));")

	var errCount int32 = 3
	var genAutoRandErr error
	var dbID int64
	var tID int64
	var jobID int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		jobID = job.ID
		dbID = job.SchemaID
		tID = job.TableID
		if atomic.LoadInt32(&errCount) > 0 && job.Type == model.ActionModifyColumn {
			atomic.AddInt32(&errCount, -1)
			ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBackfillDDLPrefix+ddl.DDLBackfillers[model.ActionModifyColumn])
			genAutoRandErr = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
				t := meta.NewMutator(txn)
				_, err1 := t.GetAutoIDAccessors(dbID, tID).RandomID().Inc(1)
				return err1
			})
		}
	})

	tk.MustExec("alter table t modify column a bigint AUTO_RANDOM(10)")
	require.True(t, errCount == 0)
	require.Nil(t, genAutoRandErr)
	const newAutoRandomBits uint64 = 10
	testCheckJobDone(t, store, jobID, true)
	var newTbInfo *model.TableInfo
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
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
	txn, err := newTxn(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = writeOnlyTable.AddRecord(ctx.GetTableCtx(), txn, types.MakeDatums(2, 3))
	if err != nil {
		return errors.Trace(err)
	}
	err = txn.Commit(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	txn, err = newTxn(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	err = checkResult(ctx, writeOnlyTable, writeOnlyTable.WritableCols(), [][]string{
		{"1", "2", "3"},
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
	expect := fmt.Sprintf("%v", []types.Datum{types.NewDatum(1), types.NewDatum(2), types.NewDatum(3)})
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
	err = writeOnlyTable.UpdateRecord(ctx.GetTableCtx(), txn, h, types.MakeDatums(1, 2, 3), types.MakeDatums(2, 2, 3), touchedSlice(writeOnlyTable))
	if err != nil {
		return errors.Trace(err)
	}
	err = txn.Commit(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	txn, err = newTxn(ctx)
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
	err = deleteOnlyTable.RemoveRecord(ctx.GetTableCtx(), txn, h, types.MakeDatums(2, 2))
	if err != nil {
		return errors.Trace(err)
	}
	err = txn.Commit(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	_, err = newTxn(ctx)
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
	// publicTable Insert t values (4, 4, 4)
	txn, err := newTxn(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	h, err := publicTable.AddRecord(sctx.GetTableCtx(), txn, types.MakeDatums(4, 4, 4))
	if err != nil {
		return errors.Trace(err)
	}
	err = txn.Commit(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	txn, err = newTxn(sctx)
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
	err = writeOnlyTable.UpdateRecord(sctx.GetTableCtx(), txn, h, oldRow, newRow, touchedSlice(writeOnlyTable))
	if err != nil {
		return errors.Trace(err)
	}
	err = txn.Commit(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	_, err = newTxn(sctx)
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
	var gotRows [][]any
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

func datumsToInterfaces(datums []types.Datum) []any {
	ifs := make([]any, 0, len(datums))
	for _, d := range datums {
		ifs = append(ifs, d.GetValue())
	}
	return ifs
}

func getSchemaVer(t *testing.T, ctx sessionctx.Context) int64 {
	txn, err := newTxn(ctx)
	require.NoError(t, err)
	m := meta.NewMutator(txn)
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

// checkJobWithHistory checks the history job info with the expected one.
func checkJobWithHistory(t *testing.T, ctx sessionctx.Context, id int64, dbInfo *model.DBInfo, tblInfo *model.TableInfo) {
	ver := getSchemaVer(t, ctx)

	historyJob, err := ddl.GetHistoryJobByID(ctx, id)
	require.NoError(t, err)
	require.Greater(t, historyJob.BinlogInfo.FinishedTS, uint64(0))
	require.Equal(t, historyJob.BinlogInfo.SchemaVersion, ver)

	if tblInfo != nil {
		checkEqualTable(t, historyJob.BinlogInfo.TableInfo, tblInfo)
	}

	if dbInfo != nil {
		require.Equal(t, historyJob.BinlogInfo.DBInfo, dbInfo)
	}
}

func testCheckJobDone(t *testing.T, store kv.Storage, jobID int64, isAdd bool) {
	sess := testkit.NewTestKit(t, store).Session()
	historyJob, err := ddl.GetHistoryJobByID(sess, jobID)
	require.NoError(t, err)
	require.Equal(t, historyJob.State, model.JobStateSynced)
	if isAdd {
		if historyJob.Type == model.ActionMultiSchemaChange {
			for _, sub := range historyJob.MultiSchemaInfo.SubJobs {
				require.Equal(t, sub.SchemaState, model.StatePublic)
			}
		} else {
			require.Equal(t, historyJob.SchemaState, model.StatePublic)
		}
	} else {
		require.Equal(t, historyJob.SchemaState, model.StateNone)
	}
}

func TestIssue40135(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("CREATE TABLE t40135 ( a tinyint DEFAULT NULL, b varchar(32) DEFAULT 'md') PARTITION BY HASH (a) PARTITIONS 2")
	one := true
	var checkErr error
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if one {
			one = false
			_, checkErr = tk1.Exec("alter table t40135 change column a aNew SMALLINT NULL DEFAULT '-14996'")
		}
	})
	tk.MustExec("alter table t40135 modify column a MEDIUMINT NULL DEFAULT '6243108' FIRST")

	require.ErrorContains(t, checkErr, "[ddl:3855]Column 'a' has a partitioning function dependency and cannot be dropped or renamed")
}

func newTxn(ctx sessionctx.Context) (kv.Transaction, error) {
	err := sessiontxn.NewTxn(context.Background(), ctx)
	if err != nil {
		return nil, err
	}
	return ctx.Txn(true)
}
