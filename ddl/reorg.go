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
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// reorgCtx is for reorganization.
type reorgCtx struct {
	// doneCh is used to notify.
	// If the reorganization job is done, we will use this channel to notify outer.
	// TODO: Now we use goroutine to simulate reorganization jobs, later we may
	// use a persistent job list.
	doneCh chan error
	// rowCount is used to simulate a job's row count.
	rowCount int64
	// notifyCancelReorgJob is used to notify the backfilling goroutine if the DDL job is cancelled.
	// 0: job is not canceled.
	// 1: job is canceled.
	notifyCancelReorgJob int32
	// doneHandle is used to simulate the handle that has been processed.
	doneHandle atomic.Value // nullableHandle
}

// nullableHandle can store <nil> handle.
// Storing a nil object to atomic.Value can lead to panic. This is a workaround.
type nullableHandle struct {
	handle kv.Handle
}

// toString is used in log to avoid nil dereference panic.
func toString(handle kv.Handle) string {
	if handle == nil {
		return "<nil>"
	}
	return handle.String()
}

// newContext gets a context. It is only used for adding column in reorganization state.
func newContext(store kv.Storage) sessionctx.Context {
	c := mock.NewContext()
	c.Store = store
	c.GetSessionVars().SetStatusFlag(mysql.ServerStatusAutocommit, false)
	c.GetSessionVars().StmtCtx.TimeZone = time.UTC
	return c
}

const defaultWaitReorgTimeout = 10 * time.Second

// ReorgWaitTimeout is the timeout that wait ddl in write reorganization stage.
var ReorgWaitTimeout = 5 * time.Second

func (rc *reorgCtx) notifyReorgCancel() {
	atomic.StoreInt32(&rc.notifyCancelReorgJob, 1)
}

func (rc *reorgCtx) cleanNotifyReorgCancel() {
	atomic.StoreInt32(&rc.notifyCancelReorgJob, 0)
}

func (rc *reorgCtx) isReorgCanceled() bool {
	return atomic.LoadInt32(&rc.notifyCancelReorgJob) == 1
}

func (rc *reorgCtx) setRowCount(count int64) {
	atomic.StoreInt64(&rc.rowCount, count)
}

func (rc *reorgCtx) setNextHandle(doneHandle kv.Handle) {
	rc.doneHandle.Store(nullableHandle{handle: doneHandle})
}

func (rc *reorgCtx) increaseRowCount(count int64) {
	atomic.AddInt64(&rc.rowCount, count)
}

func (rc *reorgCtx) getRowCountAndHandle() (int64, kv.Handle) {
	row := atomic.LoadInt64(&rc.rowCount)
	h, _ := (rc.doneHandle.Load()).(nullableHandle)
	return row, h.handle
}

func (rc *reorgCtx) clean() {
	rc.setRowCount(0)
	rc.setNextHandle(nil)
	rc.doneCh = nil
}

func (w *worker) runReorgJob(t *meta.Meta, reorgInfo *reorgInfo, tblInfo *model.TableInfo, lease time.Duration, f func() error) error {
	job := reorgInfo.Job
	if w.reorgCtx.doneCh == nil {
		// start a reorganization job
		w.wg.Add(1)
		w.reorgCtx.doneCh = make(chan error, 1)
		// initial reorgCtx
		w.reorgCtx.setRowCount(job.GetRowCount())
		w.reorgCtx.setNextHandle(reorgInfo.StartHandle)
		go func() {
			defer w.wg.Done()
			w.reorgCtx.doneCh <- f()
		}()
	}

	waitTimeout := defaultWaitReorgTimeout
	// if lease is 0, we are using a local storage,
	// and we can wait the reorganization to be done here.
	// if lease > 0, we don't need to wait here because
	// we should update some job's progress context and try checking again,
	// so we use a very little timeout here.
	if lease > 0 {
		waitTimeout = ReorgWaitTimeout
	}

	// wait reorganization job done or timeout
	select {
	case err := <-w.reorgCtx.doneCh:
		rowCount, _ := w.reorgCtx.getRowCountAndHandle()
		logutil.BgLogger().Info("[ddl] run reorg job done", zap.Int64("handled rows", rowCount))
		// Update a job's RowCount.
		job.SetRowCount(rowCount)
		if err == nil {
			metrics.AddIndexProgress.Set(100)
		}
		w.reorgCtx.clean()
		return errors.Trace(err)
	case <-w.ctx.Done():
		logutil.BgLogger().Info("[ddl] run reorg job quit")
		w.reorgCtx.setNextHandle(nil)
		w.reorgCtx.setRowCount(0)
		// We return errWaitReorgTimeout here too, so that outer loop will break.
		return errWaitReorgTimeout
	case <-time.After(waitTimeout):
		rowCount, doneHandle := w.reorgCtx.getRowCountAndHandle()
		// Update a job's RowCount.
		job.SetRowCount(rowCount)
		updateAddIndexProgress(w, tblInfo, rowCount)
		// Update a reorgInfo's handle.
		err := t.UpdateDDLReorgStartHandle(job, doneHandle)
		logutil.BgLogger().Info("[ddl] run reorg job wait timeout", zap.Duration("waitTime", waitTimeout),
			zap.Int64("totalAddedRowCount", rowCount), zap.String("doneHandle", toString(doneHandle)), zap.Error(err))
		// If timeout, we will return, check the owner and retry to wait job done again.
		return errWaitReorgTimeout
	}
}

func updateAddIndexProgress(w *worker, tblInfo *model.TableInfo, addedRowCount int64) {
	if tblInfo == nil || addedRowCount == 0 {
		return
	}
	totalCount := getTableTotalCount(w, tblInfo)
	progress := float64(0)
	if totalCount > 0 {
		progress = float64(addedRowCount) / float64(totalCount)
	} else {
		progress = 1
	}
	if progress > 1 {
		progress = 1
	}
	metrics.AddIndexProgress.Set(progress * 100)
}

func getTableTotalCount(w *worker, tblInfo *model.TableInfo) int64 {
	var ctx sessionctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		return statistics.PseudoRowCount
	}
	defer w.sessPool.put(ctx)

	executor, ok := ctx.(sqlexec.RestrictedSQLExecutor)
	// `mock.Context` is used in tests, which doesn't implement RestrictedSQLExecutor
	if !ok {
		return statistics.PseudoRowCount
	}
	sql := fmt.Sprintf("select table_rows from information_schema.tables where tidb_table_id=%v;", tblInfo.ID)
	rows, _, err := executor.ExecRestrictedSQL(sql)
	if err != nil {
		return statistics.PseudoRowCount
	}
	if len(rows) != 1 {
		return statistics.PseudoRowCount
	}
	return rows[0].GetInt64(0)
}

func (w *worker) isReorgRunnable(d *ddlCtx) error {
	if isChanClosed(w.ctx.Done()) {
		// Worker is closed. So it can't do the reorganizational job.
		return errInvalidWorker.GenWithStack("worker is closed")
	}

	if w.reorgCtx.isReorgCanceled() {
		// Job is cancelled. So it can't be done.
		return errCancelledDDLJob
	}

	if !d.isOwner() {
		// If it's not the owner, we will try later, so here just returns an error.
		logutil.BgLogger().Info("[ddl] DDL worker is not the DDL owner", zap.String("ID", d.uuid))
		return errors.Trace(errNotOwner)
	}
	return nil
}

type reorgInfo struct {
	*model.Job

	// StartHandle is the first handle of the adding indices table.
	StartHandle kv.Handle
	// EndHandle is the last handle of the adding indices table.
	EndHandle kv.Handle
	d         *ddlCtx
	first     bool
	// PhysicalTableID is used for partitioned table.
	// DDL reorganize for a partitioned table will handle partitions one by one,
	// PhysicalTableID is used to trace the current partition we are handling.
	// If the table is not partitioned, PhysicalTableID would be TableID.
	PhysicalTableID int64
}

func (r *reorgInfo) String() string {
	return "StartHandle:" + toString(r.StartHandle) + "," +
		"EndHandle:" + toString(r.EndHandle) + "," +
		"first:" + strconv.FormatBool(r.first) + "," +
		"PhysicalTableID:" + strconv.FormatInt(r.PhysicalTableID, 10)
}

func constructDescTableScanPB(physicalTableID int64, tblInfo *model.TableInfo, handleCols []*model.ColumnInfo) *tipb.Executor {
	tblScan := tables.BuildTableScanFromInfos(tblInfo, handleCols)
	tblScan.TableId = physicalTableID
	tblScan.Desc = true
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}
}

func constructLimitPB(count uint64) *tipb.Executor {
	limitExec := &tipb.Limit{
		Limit: count,
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec}
}

func buildDescTableScanDAG(ctx sessionctx.Context, tbl table.PhysicalTable, handleCols []*model.ColumnInfo, limit uint64) (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	_, timeZoneOffset := time.Now().In(time.UTC).Zone()
	dagReq.TimeZoneOffset = int64(timeZoneOffset)
	for i := range handleCols {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	dagReq.Flags |= model.FlagInSelectStmt

	tblScanExec := constructDescTableScanPB(tbl.GetPhysicalID(), tbl.Meta(), handleCols)
	dagReq.Executors = append(dagReq.Executors, tblScanExec)
	dagReq.Executors = append(dagReq.Executors, constructLimitPB(limit))
	distsql.SetEncodeType(ctx, dagReq)
	return dagReq, nil
}

func getColumnsTypes(columns []*model.ColumnInfo) []*types.FieldType {
	colTypes := make([]*types.FieldType, 0, len(columns))
	for _, col := range columns {
		colTypes = append(colTypes, &col.FieldType)
	}
	return colTypes
}

// buildDescTableScan builds a desc table scan upon tblInfo.
func (dc *ddlCtx) buildDescTableScan(ctx context.Context, startTS uint64, tbl table.PhysicalTable,
	handleCols []*model.ColumnInfo, limit uint64) (distsql.SelectResult, error) {
	sctx := newContext(dc.store)
	dagPB, err := buildDescTableScanDAG(sctx, tbl, handleCols, limit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ranges := ranger.FullIntRange(false)
	var builder distsql.RequestBuilder
	builder.SetTableRanges(tbl.GetPhysicalID(), ranges, nil).
		SetDAGRequest(dagPB).
		SetStartTS(startTS).
		SetKeepOrder(true).
		SetConcurrency(1).SetDesc(true)

	builder.Request.NotFillCache = true
	builder.Request.Priority = kv.PriorityLow

	kvReq, err := builder.Build()
	if err != nil {
		return nil, errors.Trace(err)
	}

	result, err := distsql.Select(ctx, sctx, kvReq, getColumnsTypes(handleCols), statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.Fetch(ctx)
	return result, nil
}

// GetTableMaxHandle gets the max handle of a PhysicalTable.
func (dc *ddlCtx) GetTableMaxHandle(startTS uint64, tbl table.PhysicalTable) (maxHandle kv.Handle, emptyTable bool, err error) {
	var handleCols []*model.ColumnInfo
	tblInfo := tbl.Meta()
	switch {
	case tblInfo.PKIsHandle:
		for _, col := range tbl.Meta().Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				handleCols = []*model.ColumnInfo{col}
				break
			}
		}
	case tblInfo.IsCommonHandle:
		pkIdx := tables.FindPrimaryIndex(tblInfo)
		cols := tblInfo.Cols()
		for _, idxCol := range pkIdx.Columns {
			handleCols = append(handleCols, cols[idxCol.Offset])
		}
	default:
		handleCols = []*model.ColumnInfo{model.NewExtraHandleColInfo()}
	}

	ctx := context.Background()
	// build a desc scan of tblInfo, which limit is 1, we can use it to retrieve the last handle of the table.
	result, err := dc.buildDescTableScan(ctx, startTS, tbl, handleCols, 1)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	defer terror.Call(result.Close)

	chk := chunk.New(getColumnsTypes(handleCols), 1, 1)
	err = result.Next(ctx, chk)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	if chk.NumRows() == 0 {
		// empty table
		return nil, true, nil
	}
	sessCtx := newContext(dc.store)
	row := chk.GetRow(0)
	if tblInfo.IsCommonHandle {
		maxHandle, err = buildHandleFromChunkRow(sessCtx.GetSessionVars().StmtCtx, row, handleCols)
		return maxHandle, false, err
	}
	return kv.IntHandle(row.GetInt64(0)), false, nil
}

func buildHandleFromChunkRow(sctx *stmtctx.StatementContext, row chunk.Row, cols []*model.ColumnInfo) (kv.Handle, error) {
	fieldTypes := make([]*types.FieldType, 0, len(cols))
	for _, col := range cols {
		fieldTypes = append(fieldTypes, &col.FieldType)
	}
	datumRow := row.GetDatumRow(fieldTypes)

	var handleBytes []byte
	handleBytes, err := codec.EncodeKey(sctx, nil, datumRow...)
	if err != nil {
		return nil, err
	}
	return kv.NewCommonHandle(handleBytes)
}

// getTableRange gets the start and end handle of a table (or partition).
func getTableRange(d *ddlCtx, tbl table.PhysicalTable, snapshotVer uint64, priority int) (startHandle, endHandle kv.Handle, err error) {
	// Get the start handle of this partition.
	err = iterateSnapshotRows(d.store, priority, tbl, snapshotVer, nil, nil, true,
		func(h kv.Handle, rowKey kv.Key, rawRecord []byte) (bool, error) {
			startHandle = h
			return false, nil
		})
	if err != nil {
		return startHandle, endHandle, errors.Trace(err)
	}
	var emptyTable bool
	endHandle, emptyTable, err = d.GetTableMaxHandle(snapshotVer, tbl)
	if err != nil {
		return startHandle, endHandle, errors.Trace(err)
	}
	if emptyTable || endHandle.Compare(startHandle) < 0 {
		logutil.BgLogger().Info("[ddl] get table range, endHandle < startHandle", zap.String("table", fmt.Sprintf("%v", tbl.Meta())),
			zap.Int64("table/partition ID", tbl.GetPhysicalID()), zap.String("endHandle", toString(endHandle)), zap.String("startHandle", toString(startHandle)))
		endHandle = startHandle
	}
	return
}

func getValidCurrentVersion(store kv.Storage) (ver kv.Version, err error) {
	ver, err = store.CurrentVersion()
	if err != nil {
		return ver, errors.Trace(err)
	} else if ver.Ver <= 0 {
		return ver, errInvalidStoreVer.GenWithStack("invalid storage current version %d", ver.Ver)
	}
	return ver, nil
}

func getReorgInfo(d *ddlCtx, t *meta.Meta, job *model.Job, tbl table.Table) (*reorgInfo, error) {
	var (
		start kv.Handle
		end   kv.Handle
		pid   int64
		info  reorgInfo
	)

	if job.SnapshotVer == 0 {
		info.first = true
		// get the current version for reorganization if we don't have
		ver, err := getValidCurrentVersion(d.store)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tblInfo := tbl.Meta()
		pid = tblInfo.ID
		var tb table.PhysicalTable
		if pi := tblInfo.GetPartitionInfo(); pi != nil {
			pid = pi.Definitions[0].ID
			tb = tbl.(table.PartitionedTable).GetPartition(pid)
		} else {
			tb = tbl.(table.PhysicalTable)
		}
		start, end, err = getTableRange(d, tb, ver.Ver, job.Priority)
		if err != nil {
			return nil, errors.Trace(err)
		}
		logutil.BgLogger().Info("[ddl] job get table range",
			zap.Int64("jobID", job.ID), zap.Int64("physicalTableID", pid),
			zap.String("startHandle", toString(start)), zap.String("endHandle", toString(end)))

		failpoint.Inject("errorUpdateReorgHandle", func() (*reorgInfo, error) {
			return &info, errors.New("occur an error when update reorg handle")
		})
		err = t.UpdateDDLReorgHandle(job, start, end, pid)
		if err != nil {
			return &info, errors.Trace(err)
		}
		// Update info should after data persistent.
		job.SnapshotVer = ver.Ver
	} else {
		var err error
		start, end, pid, err = t.GetDDLReorgHandle(job, tbl.Meta().IsCommonHandle)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	info.Job = job
	info.d = d
	info.StartHandle = start
	info.EndHandle = end
	info.PhysicalTableID = pid

	return &info, nil
}

func getReorgInfoFromPartitions(d *ddlCtx, t *meta.Meta, job *model.Job, tbl table.Table, partitionIDs []int64) (*reorgInfo, error) {
	var (
		start kv.Handle
		end   kv.Handle
		pid   int64
		info  reorgInfo
	)
	if job.SnapshotVer == 0 {
		info.first = true
		// get the current version for reorganization if we don't have
		ver, err := getValidCurrentVersion(d.store)
		if err != nil {
			return nil, errors.Trace(err)
		}
		pid = partitionIDs[0]
		logutil.BgLogger().Info("ddddddd ", zap.Int64("pid", pid))
		var tb table.PhysicalTable
		tb = tbl.(table.PartitionedTable).GetPartition(pid)
		logutil.BgLogger().Info("ddddddd ", zap.Int64("tblID", tbl.Meta().ID), zap.Bool("tbNotNill", tb != nil))
		start, end, err = getTableRange(d, tb, ver.Ver, job.Priority)
		if err != nil {
			return nil, errors.Trace(err)
		}
		logutil.BgLogger().Info("[ddl] job get table range",
			zap.Int64("jobID", job.ID), zap.Int64("physicalTableID", pid),
			zap.String("startHandle", toString(start)), zap.String("endHandle", toString(end)))

		err = t.UpdateDDLReorgHandle(job, start, end, pid)
		if err != nil {
			return &info, errors.Trace(err)
		}
		// Update info should after data persistent.
		job.SnapshotVer = ver.Ver
	} else {
		var err error
		start, end, pid, err = t.GetDDLReorgHandle(job, tbl.Meta().IsCommonHandle)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	info.Job = job
	info.d = d
	info.StartHandle = start
	info.EndHandle = end
	info.PhysicalTableID = pid

	return &info, nil
}

func (r *reorgInfo) UpdateReorgMeta(txn kv.Transaction, startHandle, endHandle kv.Handle, physicalTableID int64) error {
	if startHandle == nil && endHandle == nil {
		return nil
	}
	t := meta.NewMeta(txn)
	return errors.Trace(t.UpdateDDLReorgHandle(r.Job, startHandle, endHandle, physicalTableID))
}
