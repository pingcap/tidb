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
	"math"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
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

func (w *worker) runReorgJob(t *meta.Meta, reorgInfo *reorgInfo, lease time.Duration, f func() error) error {
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
		log.Infof("[ddl] run reorg job done, handled %d rows", rowCount)
		// Update a job's RowCount.
		job.SetRowCount(rowCount)
		w.reorgCtx.clean()
		return errors.WithStack(err)
	case <-w.quitCh:
		log.Info("[ddl] run reorg job quit")
		w.reorgCtx.setNextHandle(0)
		w.reorgCtx.setRowCount(0)
		// We return errWaitReorgTimeout here too, so that outer loop will break.
		return errWaitReorgTimeout
	case <-time.After(waitTimeout):
		rowCount, doneHandle := w.reorgCtx.getRowCountAndHandle()
		// Update a job's RowCount.
		job.SetRowCount(rowCount)
		// Update a reorgInfo's handle.
		err := t.UpdateDDLReorgHandle(job, doneHandle)
		log.Infof("[ddl] run reorg job wait timeout %v, handled %d rows, current done handle %d, err %v", waitTimeout, rowCount, doneHandle, err)
		// If timeout, we will return, check the owner and retry to wait job done again.
		return errWaitReorgTimeout
	}
}

func (w *worker) isReorgRunnable(d *ddlCtx) error {
	if isChanClosed(w.quitCh) {
		// Worker is closed. So it can't do the reorganizational job.
		return errInvalidWorker.Gen("worker is closed")
	}

	if w.reorgCtx.isReorgCanceled() {
		// Job is cancelled. So it can't be done.
		return errCancelledDDLJob
	}

	if !d.isOwner() {
		// If it's not the owner, we will try later, so here just returns an error.
		log.Infof("[ddl] the %s not the job owner", d.uuid)
		return errors.WithStack(errNotOwner)
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
}

func constructDescTableScanPB(tblInfo *model.TableInfo, pbColumnInfos []*tipb.ColumnInfo) *tipb.Executor {
	tblScan := &tipb.TableScan{
		TableId: tblInfo.ID,
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

func buildDescTableScanDAG(startTS uint64, tblInfo *model.TableInfo, columns []*model.ColumnInfo, limit uint64) (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.StartTs = startTS
	_, timeZoneOffset := time.Now().In(time.UTC).Zone()
	dagReq.TimeZoneOffset = int64(timeZoneOffset)
	for i := range columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	dagReq.Flags |= model.FlagInSelectStmt

	pbColumnInfos := model.ColumnsToProto(columns, tblInfo.PKIsHandle)
	tblScanExec := constructDescTableScanPB(tblInfo, pbColumnInfos)
	dagReq.Executors = append(dagReq.Executors, tblScanExec)
	dagReq.Executors = append(dagReq.Executors, constructLimitPB(limit))
	return dagReq, nil
}

func getColumnsTypes(columns []*model.ColumnInfo) []*types.FieldType {
	colTypes := make([]*types.FieldType, 0, len(columns))
	for _, col := range columns {
		colTypes = append(colTypes, &col.FieldType)
	}
	return colTypes
}

// builds a desc table scan upon tblInfo.
func (d *ddlCtx) buildDescTableScan(ctx context.Context, startTS uint64, tblInfo *model.TableInfo, columns []*model.ColumnInfo, limit uint64) (distsql.SelectResult, error) {
	dagPB, err := buildDescTableScanDAG(startTS, tblInfo, columns, limit)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ranges := ranger.FullIntRange(false)
	var builder distsql.RequestBuilder
	builder.SetTableRanges(tblInfo.ID, ranges, nil).
		SetDAGRequest(dagPB).
		SetKeepOrder(true).
		SetConcurrency(1).SetDesc(true)

	builder.Request.NotFillCache = true
	builder.Request.Priority = kv.PriorityLow
	builder.Request.IsolationLevel = kv.SI

	kvReq, err := builder.Build()
	sctx := newContext(d.store)
	result, err := distsql.Select(ctx, sctx, kvReq, getColumnsTypes(columns), statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	result.Fetch(ctx)
	return result, nil
}

// GetTableMaxRowID gets the last row id of the table.
func (d *ddlCtx) GetTableMaxRowID(startTS uint64, tblInfo *model.TableInfo) (maxRowID int64, emptyTable bool, err error) {
	maxRowID = int64(math.MaxInt64)
	var columns []*model.ColumnInfo
	if tblInfo.PKIsHandle {
		for _, col := range tblInfo.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				columns = []*model.ColumnInfo{col}
				break
			}
		}
	} else {
		columns = []*model.ColumnInfo{model.NewExtraHandleColInfo()}
	}

	ctx := context.Background()
	// build a desc scan of tblInfo, which limit is 1, we can use it to retrive the last handle of the table.
	result, err := d.buildDescTableScan(ctx, startTS, tblInfo, columns, 1)
	if err != nil {
		return maxRowID, false, errors.WithStack(err)
	}
	defer terror.Call(result.Close)

	chk := chunk.NewChunkWithCapacity(getColumnsTypes(columns), 1)
	err = result.Next(ctx, chk)
	if err != nil {
		return maxRowID, false, errors.WithStack(err)
	}

	if chk.NumRows() == 0 {
		// empty table
		return maxRowID, true, nil
	}
	row := chk.GetRow(0)
	maxRowID = row.GetInt64(0)
	return maxRowID, false, nil
}

var gofailOnceGuard bool

func getReorgInfo(d *ddlCtx, t *meta.Meta, job *model.Job, tbl table.Table) (*reorgInfo, error) {
	var err error

	info := &reorgInfo{
		Job: job,
		d:   d,
		// init start handle is math.MinInt64
		StartHandle: math.MinInt64,
		// init end handle is math.MaxInt64
		EndHandle: math.MaxInt64,
		first:     job.SnapshotVer == 0,
	}

	if info.first {
		// get the current version for reorganization if we don't have
		var ver kv.Version
		ver, err = d.store.CurrentVersion()
		if err != nil {
			return nil, errors.WithStack(err)
		} else if ver.Ver <= 0 {
			return nil, errInvalidStoreVer.Gen("invalid storage current version %d", ver.Ver)
		}

		// Get the first handle of this table.
		err = iterateSnapshotRows(d.store, tbl, ver.Ver, math.MinInt64,
			func(h int64, rowKey kv.Key, rawRecord []byte) (bool, error) {
				info.StartHandle = h
				return false, nil
			})
		if err != nil {
			return info, errors.WithStack(err)
		}

		// gofail: var errorUpdateReorgHandle bool
		// if errorUpdateReorgHandle && !gofailOnceGuard {
		//  // only return error once.
		//	gofailOnceGuard = true
		// 	return info, errors.New("occur an error when update reorg handle.")
		// }
		err = t.UpdateDDLReorgHandle(job, info.StartHandle)
		if err != nil {
			return info, errors.WithStack(err)
		}

		job.SnapshotVer = ver.Ver
	} else {
		info.StartHandle, err = t.GetDDLReorgHandle(job)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	// tbl set to nil is only in the tests.
	if tbl == nil {
		return info, errors.WithStack(err)
	}

	// init the reorg meta info of job.
	if job.ReorgMeta == nil {
		reorgMeta := model.NewDDLReorgMeta()
		// Gets the real table end handle, the new added row after the index being writable,
		// has no needs to backfill.
		endHandle, emptyTable, err1 := d.GetTableMaxRowID(job.SnapshotVer, tbl.Meta())
		if err1 != nil {
			return info, errors.WithStack(err1)
		}

		if endHandle < info.StartHandle || emptyTable {
			endHandle = info.StartHandle
		}

		reorgMeta.EndHandle = endHandle
		log.Infof("[ddl] job %v get table startHandle:%v, endHandle:%v", job.ID, info.StartHandle, reorgMeta.EndHandle)
		job.ReorgMeta = reorgMeta
	}
	info.EndHandle = job.ReorgMeta.EndHandle
	return info, errors.WithStack(err)
}

func (r *reorgInfo) UpdateHandle(txn kv.Transaction, handle int64) error {
	t := meta.NewMeta(txn)
	return errors.WithStack(t.UpdateDDLReorgHandle(r.Job, handle))
}
