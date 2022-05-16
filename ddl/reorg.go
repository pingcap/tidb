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
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/dbterror"
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

	doneKey atomic.Value // nullable kv.Key

	// element is used to record the current element in the reorg process, it can be
	// accessed by reorg-worker and daemon-worker concurrently.
	element atomic.Value

	// warnings is used to store the warnings when doing the reorg job under
	// a certain SQL Mode.
	mu struct {
		sync.Mutex
		warnings      map[errors.ErrorID]*terror.Error
		warningsCount map[errors.ErrorID]int64
	}
}

// nullableKey can store <nil> kv.Key.
// Storing a nil object to atomic.Value can lead to panic. This is a workaround.
type nullableKey struct {
	key kv.Key
}

// newContext gets a context. It is only used for adding column in reorganization state.
func newContext(store kv.Storage) sessionctx.Context {
	c := mock.NewContext()
	c.Store = store
	c.GetSessionVars().SetStatusFlag(mysql.ServerStatusAutocommit, false)

	tz := *time.UTC
	c.GetSessionVars().TimeZone = &tz
	c.GetSessionVars().StmtCtx.TimeZone = &tz
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

func (rc *reorgCtx) setNextKey(doneKey kv.Key) {
	rc.doneKey.Store(nullableKey{key: doneKey})
}

func (rc *reorgCtx) setCurrentElement(element *meta.Element) {
	rc.element.Store(element)
}

func (rc *reorgCtx) mergeWarnings(warnings map[errors.ErrorID]*terror.Error, warningsCount map[errors.ErrorID]int64) {
	if len(warnings) == 0 || len(warningsCount) == 0 {
		return
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.mu.warnings, rc.mu.warningsCount = mergeWarningsAndWarningsCount(warnings, rc.mu.warnings, warningsCount, rc.mu.warningsCount)
}

func (rc *reorgCtx) resetWarnings() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.mu.warnings = make(map[errors.ErrorID]*terror.Error)
	rc.mu.warningsCount = make(map[errors.ErrorID]int64)
}

func (rc *reorgCtx) increaseRowCount(count int64) {
	atomic.AddInt64(&rc.rowCount, count)
}

func (rc *reorgCtx) getRowCountAndKey() (int64, kv.Key, *meta.Element) {
	row := atomic.LoadInt64(&rc.rowCount)
	h, _ := (rc.doneKey.Load()).(nullableKey)
	element, _ := (rc.element.Load()).(*meta.Element)
	return row, h.key, element
}

func (rc *reorgCtx) clean() {
	rc.setRowCount(0)
	rc.setNextKey(nil)
	rc.resetWarnings()
	rc.doneCh = nil
}

// runReorgJob is used as a portal to do the reorganization work.
// eg:
// 1: add index
// 2: alter column type
// 3: clean global index
//
// ddl goroutine >---------+
//   ^                     |
//   |                     |
//   |                     |
//   |                     | <---(doneCh)--- f()
// HandleDDLQueue(...)     | <---(regular timeout)
//   |                     | <---(ctx done)
//   |                     |
//   |                     |
// A more ddl round  <-----+
//
// How can we cancel reorg job?
//
// The background reorg is continuously running except for several factors, for instances, ddl owner change,
// logic error (kv duplicate when insert index / cast error when alter column), ctx done, and cancel signal.
//
// When `admin cancel ddl jobs xxx` takes effect, we will give this kind of reorg ddl one more round.
// because we should pull the result from doneCh out, otherwise, the reorg worker will hang on `f()` logic,
// which is a kind of goroutine leak.
//
// That's why we couldn't set the job to rollingback state directly in `convertJob2RollbackJob`, which is a
// cancelling portal for admin cancel action.
//
// In other words, the cancelling signal is informed from the bottom up, we set the atomic cancel variable
// in the cancelling portal to notify the lower worker goroutine, and fetch the cancel error from them in
// the additional ddl round.
//
// After that, we can make sure that the worker goroutine is correctly shut down.
func (w *worker) runReorgJob(t *meta.Meta, reorgInfo *reorgInfo, tblInfo *model.TableInfo, lease time.Duration, f func() error) error {
	job := reorgInfo.Job
	// This is for tests compatible, because most of the early tests try to build the reorg job manually
	// without reorg meta info, which will cause nil pointer in here.
	if job.ReorgMeta == nil {
		job.ReorgMeta = &model.DDLReorgMeta{
			SQLMode:       mysql.ModeNone,
			Warnings:      make(map[errors.ErrorID]*terror.Error),
			WarningsCount: make(map[errors.ErrorID]int64),
			Location:      &model.TimeZoneLocation{Name: time.UTC.String(), Offset: 0},
		}
	}
	if w.reorgCtx.doneCh == nil {
		// Since reorg job will be interrupted for polling the cancel action outside. we don't need to wait for 2.5s
		// for the later entrances.
		// lease = 0 means it's in an integration test. In this case we don't delay so the test won't run too slowly.
		if lease > 0 {
			delayForAsyncCommit()
		}
		// start a reorganization job
		w.wg.Add(1)
		w.reorgCtx.doneCh = make(chan error, 1)
		// initial reorgCtx
		w.reorgCtx.setRowCount(job.GetRowCount())
		w.reorgCtx.setNextKey(reorgInfo.StartKey)
		w.reorgCtx.setCurrentElement(reorgInfo.currElement)
		w.reorgCtx.mu.warnings = make(map[errors.ErrorID]*terror.Error)
		w.reorgCtx.mu.warningsCount = make(map[errors.ErrorID]int64)
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
		// Since job is cancelled，we don't care about its partial counts.
		if w.reorgCtx.isReorgCanceled() || terror.ErrorEqual(err, dbterror.ErrCancelledDDLJob) {
			w.reorgCtx.clean()
			return dbterror.ErrCancelledDDLJob
		}
		rowCount, _, _ := w.reorgCtx.getRowCountAndKey()
		logutil.BgLogger().Info("[ddl] run reorg job done", zap.Int64("handled rows", rowCount))
		// Update a job's RowCount.
		job.SetRowCount(rowCount)

		// Update a job's warnings.
		w.mergeWarningsIntoJob(job)

		w.reorgCtx.clean()
		// For other errors, even err is not nil here, we still wait the partial counts to be collected.
		// since in the next round, the startKey is brand new which is stored by last time.
		if err != nil {
			return errors.Trace(err)
		}

		switch reorgInfo.Type {
		case model.ActionAddIndex, model.ActionAddPrimaryKey:
			metrics.GetBackfillProgressByLabel(metrics.LblAddIndex).Set(100)
		case model.ActionModifyColumn:
			metrics.GetBackfillProgressByLabel(metrics.LblModifyColumn).Set(100)
		}
		if err1 := t.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
			logutil.BgLogger().Warn("[ddl] run reorg job done, removeDDLReorgHandle failed", zap.Error(err1))
			return errors.Trace(err1)
		}
	case <-w.ctx.Done():
		logutil.BgLogger().Info("[ddl] run reorg job quit")
		w.reorgCtx.setNextKey(nil)
		w.reorgCtx.setRowCount(0)
		w.reorgCtx.resetWarnings()
		// We return dbterror.ErrWaitReorgTimeout here too, so that outer loop will break.
		return dbterror.ErrWaitReorgTimeout
	case <-time.After(waitTimeout):
		rowCount, doneKey, currentElement := w.reorgCtx.getRowCountAndKey()
		// Update a job's RowCount.
		job.SetRowCount(rowCount)
		updateBackfillProgress(w, reorgInfo, tblInfo, rowCount)

		// Update a job's warnings.
		w.mergeWarningsIntoJob(job)

		w.reorgCtx.resetWarnings()

		// Update a reorgInfo's handle.
		// Since daemon-worker is triggered by timer to store the info half-way.
		// you should keep these infos is read-only (like job) / atomic (like doneKey & element) / concurrent safe.
		err := t.UpdateDDLReorgStartHandle(job, currentElement, doneKey)

		logutil.BgLogger().Info("[ddl] run reorg job wait timeout",
			zap.Duration("waitTime", waitTimeout),
			zap.ByteString("elementType", currentElement.TypeKey),
			zap.Int64("elementID", currentElement.ID),
			zap.Int64("totalAddedRowCount", rowCount),
			zap.String("doneKey", tryDecodeToHandleString(doneKey)),
			zap.Error(err))
		// If timeout, we will return, check the owner and retry to wait job done again.
		return dbterror.ErrWaitReorgTimeout
	}
	return nil
}

func (w *worker) mergeWarningsIntoJob(job *model.Job) {
	w.reorgCtx.mu.Lock()
	partWarnings := w.reorgCtx.mu.warnings
	partWarningsCount := w.reorgCtx.mu.warningsCount
	job.SetWarnings(mergeWarningsAndWarningsCount(partWarnings, job.ReorgMeta.Warnings, partWarningsCount, job.ReorgMeta.WarningsCount))
	w.reorgCtx.mu.Unlock()
}

func updateBackfillProgress(w *worker, reorgInfo *reorgInfo, tblInfo *model.TableInfo,
	addedRowCount int64) {
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
	switch reorgInfo.Type {
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		metrics.GetBackfillProgressByLabel(metrics.LblAddIndex).Set(progress * 100)
	case model.ActionModifyColumn:
		metrics.GetBackfillProgressByLabel(metrics.LblModifyColumn).Set(progress * 100)
	}
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
	sql := "select table_rows from information_schema.tables where tidb_table_id=%?;"
	rows, _, err := executor.ExecRestrictedSQL(w.ddlJobCtx, nil, sql, tblInfo.ID)
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
		return dbterror.ErrInvalidWorker.GenWithStack("worker is closed")
	}

	if w.reorgCtx.isReorgCanceled() {
		// Job is cancelled. So it can't be done.
		return dbterror.ErrCancelledDDLJob
	}

	if !d.isOwner() {
		// If it's not the owner, we will try later, so here just returns an error.
		logutil.BgLogger().Info("[ddl] DDL worker is not the DDL owner", zap.String("ID", d.uuid))
		return errors.Trace(dbterror.ErrNotOwner)
	}
	return nil
}

type reorgInfo struct {
	*model.Job

	StartKey kv.Key
	EndKey   kv.Key
	d        *ddlCtx
	first    bool
	// PhysicalTableID is used for partitioned table.
	// DDL reorganize for a partitioned table will handle partitions one by one,
	// PhysicalTableID is used to trace the current partition we are handling.
	// If the table is not partitioned, PhysicalTableID would be TableID.
	PhysicalTableID int64
	elements        []*meta.Element
	currElement     *meta.Element
}

func (r *reorgInfo) String() string {
	return "CurrElementType:" + string(r.currElement.TypeKey) + "," +
		"CurrElementID:" + strconv.FormatInt(r.currElement.ID, 10) + "," +
		"StartHandle:" + tryDecodeToHandleString(r.StartKey) + "," +
		"EndHandle:" + tryDecodeToHandleString(r.EndKey) + "," +
		"First:" + strconv.FormatBool(r.first) + "," +
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
func (dc *ddlCtx) buildDescTableScan(ctx *JobContext, startTS uint64, tbl table.PhysicalTable,
	handleCols []*model.ColumnInfo, limit uint64) (distsql.SelectResult, error) {
	sctx := newContext(dc.store)
	dagPB, err := buildDescTableScanDAG(sctx, tbl, handleCols, limit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var b distsql.RequestBuilder
	var builder *distsql.RequestBuilder
	var ranges []*ranger.Range
	if tbl.Meta().IsCommonHandle {
		ranges = ranger.FullNotNullRange()
	} else {
		ranges = ranger.FullIntRange(false)
	}
	builder = b.SetHandleRanges(sctx.GetSessionVars().StmtCtx, tbl.GetPhysicalID(), tbl.Meta().IsCommonHandle, ranges, nil)
	builder.SetDAGRequest(dagPB).
		SetStartTS(startTS).
		SetKeepOrder(true).
		SetConcurrency(1).SetDesc(true)

	builder.Request.ResourceGroupTagger = ctx.getResourceGroupTaggerForTopSQL()
	builder.Request.NotFillCache = true
	builder.Request.Priority = kv.PriorityLow

	kvReq, err := builder.Build()
	if err != nil {
		return nil, errors.Trace(err)
	}

	result, err := distsql.Select(ctx.ddlJobCtx, sctx, kvReq, getColumnsTypes(handleCols), statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return result, nil
}

// GetTableMaxHandle gets the max handle of a PhysicalTable.
func (dc *ddlCtx) GetTableMaxHandle(ctx *JobContext, startTS uint64, tbl table.PhysicalTable) (maxHandle kv.Handle, emptyTable bool, err error) {
	var handleCols []*model.ColumnInfo
	var pkIdx *model.IndexInfo
	tblInfo := tbl.Meta()
	switch {
	case tblInfo.PKIsHandle:
		for _, col := range tbl.Meta().Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				handleCols = []*model.ColumnInfo{col}
				break
			}
		}
	case tblInfo.IsCommonHandle:
		pkIdx = tables.FindPrimaryIndex(tblInfo)
		cols := tblInfo.Cols()
		for _, idxCol := range pkIdx.Columns {
			handleCols = append(handleCols, cols[idxCol.Offset])
		}
	default:
		handleCols = []*model.ColumnInfo{model.NewExtraHandleColInfo()}
	}

	// build a desc scan of tblInfo, which limit is 1, we can use it to retrieve the last handle of the table.
	result, err := dc.buildDescTableScan(ctx, startTS, tbl, handleCols, 1)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	defer terror.Call(result.Close)

	chk := chunk.New(getColumnsTypes(handleCols), 1, 1)
	err = result.Next(ctx.ddlJobCtx, chk)
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
		maxHandle, err = buildCommonHandleFromChunkRow(sessCtx.GetSessionVars().StmtCtx, tblInfo, pkIdx, handleCols, row)
		return maxHandle, false, err
	}
	return kv.IntHandle(row.GetInt64(0)), false, nil
}

func buildCommonHandleFromChunkRow(sctx *stmtctx.StatementContext, tblInfo *model.TableInfo, idxInfo *model.IndexInfo,
	cols []*model.ColumnInfo, row chunk.Row) (kv.Handle, error) {
	fieldTypes := make([]*types.FieldType, 0, len(cols))
	for _, col := range cols {
		fieldTypes = append(fieldTypes, &col.FieldType)
	}
	datumRow := row.GetDatumRow(fieldTypes)
	tablecodec.TruncateIndexValues(tblInfo, idxInfo, datumRow)

	var handleBytes []byte
	handleBytes, err := codec.EncodeKey(sctx, nil, datumRow...)
	if err != nil {
		return nil, err
	}
	return kv.NewCommonHandle(handleBytes)
}

// getTableRange gets the start and end handle of a table (or partition).
func getTableRange(ctx *JobContext, d *ddlCtx, tbl table.PhysicalTable, snapshotVer uint64, priority int) (startHandleKey, endHandleKey kv.Key, err error) {
	// Get the start handle of this partition.
	err = iterateSnapshotRows(ctx, d.store, priority, tbl, snapshotVer, nil, nil,
		func(h kv.Handle, rowKey kv.Key, rawRecord []byte) (bool, error) {
			startHandleKey = rowKey
			return false, nil
		})
	if err != nil {
		return startHandleKey, endHandleKey, errors.Trace(err)
	}
	maxHandle, isEmptyTable, err := d.GetTableMaxHandle(ctx, snapshotVer, tbl)
	if err != nil {
		return startHandleKey, nil, errors.Trace(err)
	}
	if maxHandle != nil {
		endHandleKey = tablecodec.EncodeRecordKey(tbl.RecordPrefix(), maxHandle)
	}
	if isEmptyTable || endHandleKey.Cmp(startHandleKey) < 0 {
		logutil.BgLogger().Info("[ddl] get table range, endHandle < startHandle", zap.String("table", fmt.Sprintf("%v", tbl.Meta())),
			zap.Int64("table/partition ID", tbl.GetPhysicalID()),
			zap.String("endHandle", tryDecodeToHandleString(endHandleKey)),
			zap.String("startHandle", tryDecodeToHandleString(startHandleKey)))
		endHandleKey = startHandleKey
	}
	return
}

func getValidCurrentVersion(store kv.Storage) (ver kv.Version, err error) {
	ver, err = store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return ver, errors.Trace(err)
	} else if ver.Ver <= 0 {
		return ver, dbterror.ErrInvalidStoreVer.GenWithStack("invalid storage current version %d", ver.Ver)
	}
	return ver, nil
}

func getReorgInfo(ctx *JobContext, d *ddlCtx, t *meta.Meta, job *model.Job, tbl table.Table, elements []*meta.Element) (*reorgInfo, error) {
	var (
		element *meta.Element
		start   kv.Key
		end     kv.Key
		pid     int64
		info    reorgInfo
	)

	if job.SnapshotVer == 0 {
		// For the case of the old TiDB version(do not exist the element information) is upgraded to the new TiDB version.
		// Third step, we need to remove the element information to make sure we can save the reorganized information to storage.
		failpoint.Inject("MockGetIndexRecordErr", func(val failpoint.Value) {
			if val.(string) == "addIdxNotOwnerErr" && atomic.CompareAndSwapUint32(&mockNotOwnerErrOnce, 3, 4) {
				if err := t.RemoveReorgElement(job); err != nil {
					failpoint.Return(nil, errors.Trace(err))
				}
				info.first = true
				failpoint.Return(&info, nil)
			}
		})

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
		start, end, err = getTableRange(ctx, d, tb, ver.Ver, job.Priority)
		if err != nil {
			return nil, errors.Trace(err)
		}
		logutil.BgLogger().Info("[ddl] job get table range",
			zap.Int64("jobID", job.ID), zap.Int64("physicalTableID", pid),
			zap.String("startHandle", tryDecodeToHandleString(start)),
			zap.String("endHandle", tryDecodeToHandleString(end)))

		failpoint.Inject("errorUpdateReorgHandle", func() (*reorgInfo, error) {
			return &info, errors.New("occur an error when update reorg handle")
		})
		err = t.UpdateDDLReorgHandle(job, start, end, pid, elements[0])
		if err != nil {
			return &info, errors.Trace(err)
		}
		// Update info should after data persistent.
		job.SnapshotVer = ver.Ver
		element = elements[0]
	} else {
		failpoint.Inject("MockGetIndexRecordErr", func(val failpoint.Value) {
			// For the case of the old TiDB version(do not exist the element information) is upgraded to the new TiDB version.
			// Second step, we need to remove the element information to make sure we can get the error of "ErrDDLReorgElementNotExist".
			// However, since "txn.Reset()" will be called later, the reorganized information cannot be saved to storage.
			if val.(string) == "addIdxNotOwnerErr" && atomic.CompareAndSwapUint32(&mockNotOwnerErrOnce, 2, 3) {
				if err := t.RemoveReorgElement(job); err != nil {
					failpoint.Return(nil, errors.Trace(err))
				}
			}
		})

		var err error
		element, start, end, pid, err = t.GetDDLReorgHandle(job)
		if err != nil {
			// If the reorg element doesn't exist, this reorg info should be saved by the older TiDB versions.
			// It's compatible with the older TiDB versions.
			// We'll try to remove it in the next major TiDB version.
			if meta.ErrDDLReorgElementNotExist.Equal(err) {
				job.SnapshotVer = 0
				logutil.BgLogger().Warn("[ddl] get reorg info, the element does not exist", zap.String("job", job.String()))
			}
			return &info, errors.Trace(err)
		}
	}
	info.Job = job
	info.d = d
	info.StartKey = start
	info.EndKey = end
	info.PhysicalTableID = pid
	info.currElement = element
	info.elements = elements

	return &info, nil
}

func getReorgInfoFromPartitions(ctx *JobContext, d *ddlCtx, t *meta.Meta, job *model.Job, tbl table.Table, partitionIDs []int64, elements []*meta.Element) (*reorgInfo, error) {
	var (
		element *meta.Element
		start   kv.Key
		end     kv.Key
		pid     int64
		info    reorgInfo
	)
	if job.SnapshotVer == 0 {
		info.first = true
		// get the current version for reorganization if we don't have
		ver, err := getValidCurrentVersion(d.store)
		if err != nil {
			return nil, errors.Trace(err)
		}
		pid = partitionIDs[0]
		tb := tbl.(table.PartitionedTable).GetPartition(pid)
		start, end, err = getTableRange(ctx, d, tb, ver.Ver, job.Priority)
		if err != nil {
			return nil, errors.Trace(err)
		}
		logutil.BgLogger().Info("[ddl] job get table range",
			zap.Int64("jobID", job.ID), zap.Int64("physicalTableID", pid),
			zap.String("startHandle", tryDecodeToHandleString(start)),
			zap.String("endHandle", tryDecodeToHandleString(end)))

		err = t.UpdateDDLReorgHandle(job, start, end, pid, elements[0])
		if err != nil {
			return &info, errors.Trace(err)
		}
		// Update info should after data persistent.
		job.SnapshotVer = ver.Ver
		element = elements[0]
	} else {
		var err error
		element, start, end, pid, err = t.GetDDLReorgHandle(job)
		if err != nil {
			// If the reorg element doesn't exist, this reorg info should be saved by the older TiDB versions.
			// It's compatible with the older TiDB versions.
			// We'll try to remove it in the next major TiDB version.
			if meta.ErrDDLReorgElementNotExist.Equal(err) {
				job.SnapshotVer = 0
				logutil.BgLogger().Warn("[ddl] get reorg info, the element does not exist", zap.String("job", job.String()))
			}
			return &info, errors.Trace(err)
		}
	}
	info.Job = job
	info.d = d
	info.StartKey = start
	info.EndKey = end
	info.PhysicalTableID = pid
	info.currElement = element
	info.elements = elements

	return &info, nil
}

func (r *reorgInfo) UpdateReorgMeta(startKey kv.Key) error {
	if startKey == nil && r.EndKey == nil {
		return nil
	}

	err := kv.RunInNewTxn(context.Background(), r.d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		return errors.Trace(t.UpdateDDLReorgHandle(r.Job, startKey, r.EndKey, r.PhysicalTableID, r.currElement))
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
