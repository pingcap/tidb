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
	"math"
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
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
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
	doneHandle int64
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

func (rc *reorgCtx) setNextHandle(doneHandle int64) {
	atomic.StoreInt64(&rc.doneHandle, doneHandle)
}

func (rc *reorgCtx) increaseRowCount(count int64) {
	atomic.AddInt64(&rc.rowCount, count)
}

func (rc *reorgCtx) getRowCountAndHandle() (int64, int64) {
	row := atomic.LoadInt64(&rc.rowCount)
	handle := atomic.LoadInt64(&rc.doneHandle)
	return row, handle
}

func (rc *reorgCtx) clean() {
	rc.setRowCount(0)
	rc.setNextHandle(0)
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
	case <-w.quitCh:
		logutil.BgLogger().Info("[ddl] run reorg job quit")
		w.reorgCtx.setNextHandle(0)
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
			zap.Int64("totalAddedRowCount", rowCount), zap.Int64("doneHandle", doneHandle), zap.Error(err))
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
	if isChanClosed(w.quitCh) {
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
	StartHandle int64
	// EndHandle is the last handle of the adding indices table.
	EndHandle int64
	d         *ddlCtx
	first     bool
	// PhysicalTableID is used for partitioned table.
	// DDL reorganize for a partitioned table will handle partitions one by one,
	// PhysicalTableID is used to trace the current partition we are handling.
	// If the table is not partitioned, PhysicalTableID would be TableID.
	PhysicalTableID int64
}

func (r *reorgInfo) String() string {
	return "StartHandle:" + strconv.FormatInt(r.StartHandle, 10) + "," +
		"EndHandle:" + strconv.FormatInt(r.EndHandle, 10) + "," +
		"first:" + strconv.FormatBool(r.first) + "," +
		"PhysicalTableID:" + strconv.FormatInt(r.PhysicalTableID, 10)
}

func constructDescTableScanPB(physicalTableID int64, pbColumnInfos []*tipb.ColumnInfo) *tipb.Executor {
	tblScan := &tipb.TableScan{
		TableId: physicalTableID,
		Columns: pbColumnInfos,
		Desc:    true,
	}

	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}
}

func constructLimitPB(count uint64) *tipb.Executor {
	limitExec := &tipb.Limit{
		Limit: count,
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec}
}

func buildDescTableScanDAG(ctx sessionctx.Context, tbl table.PhysicalTable, columns []*model.ColumnInfo, limit uint64) (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	_, timeZoneOffset := time.Now().In(time.UTC).Zone()
	dagReq.TimeZoneOffset = int64(timeZoneOffset)
	for i := range columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	dagReq.Flags |= model.FlagInSelectStmt

	pbColumnInfos := util.ColumnsToProto(columns, tbl.Meta().PKIsHandle)
	tblScanExec := constructDescTableScanPB(tbl.GetPhysicalID(), pbColumnInfos)
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
func (dc *ddlCtx) buildDescTableScan(ctx context.Context, startTS uint64, tbl table.PhysicalTable, columns []*model.ColumnInfo, limit uint64) (distsql.SelectResult, error) {
	sctx := newContext(dc.store)
	dagPB, err := buildDescTableScanDAG(sctx, tbl, columns, limit)
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

	result, err := distsql.Select(ctx, sctx, kvReq, getColumnsTypes(columns), statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.Fetch(ctx)
	return result, nil
}

// GetTableMaxRowID gets the last row id of the table partition.
func (dc *ddlCtx) GetTableMaxRowID(startTS uint64, tbl table.PhysicalTable) (maxRowID int64, emptyTable bool, err error) {
	maxRowID = int64(math.MaxInt64)
	var columns []*model.ColumnInfo
	if tbl.Meta().PKIsHandle {
		for _, col := range tbl.Meta().Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				columns = []*model.ColumnInfo{col}
				break
			}
		}
	} else {
		columns = []*model.ColumnInfo{model.NewExtraHandleColInfo()}
	}

	ctx := context.Background()
	// build a desc scan of tblInfo, which limit is 1, we can use it to retrieve the last handle of the table.
	result, err := dc.buildDescTableScan(ctx, startTS, tbl, columns, 1)
	if err != nil {
		return maxRowID, false, errors.Trace(err)
	}
	defer terror.Call(result.Close)

	chk := chunk.New(getColumnsTypes(columns), 1, 1)
	err = result.Next(ctx, chk)
	if err != nil {
		return maxRowID, false, errors.Trace(err)
	}

	if chk.NumRows() == 0 {
		// empty table
		return maxRowID, true, nil
	}
	row := chk.GetRow(0)
	maxRowID = row.GetInt64(0)
	return maxRowID, false, nil
}

// getTableRange gets the start and end handle of a table (or partition).
func getTableRange(d *ddlCtx, tbl table.PhysicalTable, snapshotVer uint64, priority int) (startHandle, endHandle int64, err error) {
	startHandle = math.MinInt64
	endHandle = math.MaxInt64
	// Get the start handle of this partition.
	err = iterateSnapshotRows(d.store, priority, tbl, snapshotVer, math.MinInt64, math.MaxInt64, true,
		func(h int64, rowKey kv.Key, rawRecord []byte) (bool, error) {
			startHandle = h
			return false, nil
		})
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	var emptyTable bool
	// Get the end handle of this partition.
	endHandle, emptyTable, err = d.GetTableMaxRowID(snapshotVer, tbl)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	if endHandle < startHandle || emptyTable {
		logutil.BgLogger().Info("[ddl] get table range, endHandle < startHandle", zap.String("table", fmt.Sprintf("%v", tbl.Meta())),
			zap.Int64("partitionID", tbl.GetPhysicalID()), zap.Int64("endHandle", endHandle), zap.Int64("startHandle", startHandle))
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
		start int64
		end   int64
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
		logutil.BgLogger().Info("[ddl] job get table range", zap.Int64("jobID", job.ID), zap.Int64("physicalTableID", pid), zap.Int64("startHandle", start), zap.Int64("endHandle", end))

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
		start, end, pid, err = t.GetDDLReorgHandle(job)
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

func (r *reorgInfo) UpdateReorgMeta(txn kv.Transaction, startHandle, endHandle, physicalTableID int64) error {
	t := meta.NewMeta(txn)
	return errors.Trace(t.UpdateDDLReorgHandle(r.Job, startHandle, endHandle, physicalTableID))
}
