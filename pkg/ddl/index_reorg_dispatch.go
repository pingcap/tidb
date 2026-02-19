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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

func initForReorgIndexes(w *worker, job *model.Job, idxInfos []*model.IndexInfo) error {
	if len(idxInfos) == 0 {
		return nil
	}
	reorgTp, err := pickBackfillType(job)
	if err != nil {
		return err
	}
	// Partial Index is not supported without fast reorg.
	for _, indexInfo := range idxInfos {
		if (reorgTp == model.ReorgTypeTxn || reorgTp == model.ReorgTypeTxnMerge) && indexInfo.HasCondition() {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs("add partial index without fast reorg is not supported")
		}
	}
	loadCloudStorageURI(w, job)
	if reorgTp.NeedMergeProcess() {
		// Increase telemetryAddIndexIngestUsage
		telemetryAddIndexIngestUsage.Inc()
		for _, indexInfo := range idxInfos {
			indexInfo.BackfillState = model.BackfillStateRunning
		}
	}
	return nil
}


func checkIfTableReorgWorkCanSkip(
	store kv.Storage,
	sessCtx sessionctx.Context,
	tbl table.Table,
	job *model.Job,
) bool {
	if job.SnapshotVer != 0 {
		// Reorg work has begun.
		return false
	}
	txn, err := sessCtx.Txn(false)
	validTxn := err == nil && txn != nil && txn.Valid()
	intest.Assert(validTxn)
	if !validTxn {
		logutil.DDLLogger().Warn("check if table is empty failed", zap.Error(err))
		return false
	}
	startTS := txn.StartTS()
	ctx := NewReorgContext()
	ctx.resourceGroupName = job.ReorgMeta.ResourceGroupName
	ctx.attachTopProfilingInfo(job.Query)
	if isEmpty, err := checkIfTableIsEmpty(ctx, store, tbl, startTS); err != nil || !isEmpty {
		return false
	}
	return true
}

// CheckImportIntoTableIsEmpty check import into table is empty or not.
func CheckImportIntoTableIsEmpty(
	store kv.Storage,
	sessCtx sessionctx.Context,
	tbl table.Table,
) (bool, error) {
	failpoint.Inject("checkImportIntoTableIsEmpty", func(_val failpoint.Value) {
		if val, ok := _val.(string); ok {
			switch val {
			case "error":
				failpoint.Return(false, errors.New("check is empty get error"))
			case "notEmpty":
				failpoint.Return(false, nil)
			}
		}
	})
	txn, err := sessCtx.Txn(true)
	if err != nil {
		return false, err
	}
	validTxn := txn != nil && txn.Valid()
	if !validTxn {
		return false, errors.New("check if table is empty failed")
	}
	startTS := txn.StartTS()
	return checkIfTableIsEmpty(NewReorgContext(), store, tbl, startTS)
}

func checkIfTableIsEmpty(
	ctx *ReorgContext,
	store kv.Storage,
	tbl table.Table,
	startTS uint64,
) (bool, error) {
	if pTbl, ok := tbl.(table.PartitionedTable); ok {
		for _, pid := range pTbl.GetAllPartitionIDs() {
			pTbl := pTbl.GetPartition(pid)
			if isEmpty, err := checkIfPhysicalTableIsEmpty(ctx, store, pTbl, startTS); err != nil || !isEmpty {
				return false, err
			}
		}
		return true, nil
	}
	//nolint:forcetypeassert
	plainTbl := tbl.(table.PhysicalTable)
	return checkIfPhysicalTableIsEmpty(ctx, store, plainTbl, startTS)
}

func checkIfPhysicalTableIsEmpty(
	ctx *ReorgContext,
	store kv.Storage,
	tbl table.PhysicalTable,
	startTS uint64,
) (bool, error) {
	hasRecord, err := existsTableRow(ctx, store, tbl, startTS)
	intest.Assert(err == nil)
	if err != nil {
		logutil.DDLLogger().Warn("check if table is empty failed", zap.Error(err))
		return false, err
	}
	return !hasRecord, nil
}

func checkIfTempIndexReorgWorkCanSkip(
	store kv.Storage,
	sessCtx sessionctx.Context,
	tbl table.Table,
	allIndexInfos []*model.IndexInfo,
	job *model.Job,
) bool {
	failpoint.Inject("skipReorgWorkForTempIndex", func(val failpoint.Value) {
		if v, ok := val.(bool); ok {
			failpoint.Return(v)
		}
	})
	if job.SnapshotVer != 0 {
		// Reorg work has begun.
		return false
	}
	txn, err := sessCtx.Txn(false)
	validTxn := err == nil && txn != nil && txn.Valid()
	intest.Assert(validTxn)
	if !validTxn {
		logutil.DDLLogger().Warn("check if temp index is empty failed", zap.Error(err))
		return false
	}
	startTS := txn.StartTS()
	ctx := NewReorgContext()
	ctx.resourceGroupName = job.ReorgMeta.ResourceGroupName
	ctx.attachTopProfilingInfo(job.Query)
	firstIdxID := allIndexInfos[0].ID
	lastIdxID := allIndexInfos[len(allIndexInfos)-1].ID
	var globalIdxIDs []int64
	for _, idxInfo := range allIndexInfos {
		if idxInfo.Global {
			globalIdxIDs = append(globalIdxIDs, idxInfo.ID)
		}
	}
	return checkIfTempIndexIsEmpty(ctx, store, tbl, firstIdxID, lastIdxID, globalIdxIDs, startTS)
}

func checkIfTempIndexIsEmpty(
	ctx *ReorgContext,
	store kv.Storage,
	tbl table.Table,
	firstIdxID, lastIdxID int64,
	globalIdxIDs []int64,
	startTS uint64,
) bool {
	tblMetaID := tbl.Meta().ID
	if pTbl, ok := tbl.(table.PartitionedTable); ok {
		for _, pid := range pTbl.GetAllPartitionIDs() {
			if !checkIfTempIndexIsEmptyForPhysicalTable(ctx, store, pid, firstIdxID, lastIdxID, startTS) {
				return false
			}
		}
		for _, globalIdxID := range globalIdxIDs {
			if !checkIfTempIndexIsEmptyForPhysicalTable(ctx, store, tblMetaID, globalIdxID, globalIdxID, startTS) {
				return false
			}
		}
		return true
	}
	return checkIfTempIndexIsEmptyForPhysicalTable(ctx, store, tblMetaID, firstIdxID, lastIdxID, startTS)
}

func checkIfTempIndexIsEmptyForPhysicalTable(
	ctx *ReorgContext,
	store kv.Storage,
	pid int64,
	firstIdxID, lastIdxID int64,
	startTS uint64,
) bool {
	start, end := encodeTempIndexRange(pid, firstIdxID, lastIdxID)
	foundKey := false
	idxPrefix := tablecodec.GenTableIndexPrefix(pid)
	err := iterateSnapshotKeys(ctx, store, kv.PriorityLow, idxPrefix, startTS, start, end,
		func(_ kv.Handle, _ kv.Key, _ []byte) (more bool, err error) {
			foundKey = true
			return false, nil
		})
	intest.Assert(err == nil)
	if err != nil {
		logutil.DDLLogger().Info("check if temp index is empty failed", zap.Error(err))
		return false
	}
	return !foundKey
}

// pickBackfillType determines which backfill process will be used. The result is
// both stored in job.ReorgMeta.ReorgTp and returned.
func pickBackfillType(job *model.Job) (model.ReorgType, error) {
	if job.ReorgMeta.ReorgTp != model.ReorgTypeNone {
		// The backfill task has been started.
		// Don't change the backfill type.
		return job.ReorgMeta.ReorgTp, nil
	}
	if !job.ReorgMeta.IsFastReorg {
		job.ReorgMeta.ReorgTp = model.ReorgTypeTxn
		return model.ReorgTypeTxn, nil
	}
	if ingest.LitInitialized {
		if job.ReorgMeta.UseCloudStorage {
			job.ReorgMeta.ReorgTp = model.ReorgTypeIngest
			return model.ReorgTypeIngest, nil
		}
		if err := ingest.LitDiskRoot.PreCheckUsage(); err != nil {
			logutil.DDLIngestLogger().Info("ingest backfill is not available", zap.Error(err))
			return model.ReorgTypeNone, err
		}
		job.ReorgMeta.ReorgTp = model.ReorgTypeIngest
		return model.ReorgTypeIngest, nil
	}
	// The lightning environment is unavailable, but we can still use the txn-merge backfill.
	logutil.DDLLogger().Info("fallback to txn-merge backfill process",
		zap.Bool("lightning env initialized", ingest.LitInitialized))
	job.ReorgMeta.ReorgTp = model.ReorgTypeTxnMerge
	return model.ReorgTypeTxnMerge, nil
}

func loadCloudStorageURI(w *worker, job *model.Job) {
	jc := w.jobContext(job.ID, job.ReorgMeta)
	jc.cloudStorageURI = handle.GetCloudStorageURI(w.workCtx, w.store)
	job.ReorgMeta.UseCloudStorage = len(jc.cloudStorageURI) > 0 && job.ReorgMeta.IsDistReorg
	failpoint.InjectCall("afterLoadCloudStorageURI", job)
}

func doReorgWorkForCreateIndex(
	w *worker,
	jobCtx *jobContext,
	job *model.Job,
	tbl table.Table,
	allIndexInfos []*model.IndexInfo,
) (done bool, ver int64, err error) {
	var reorgTp model.ReorgType
	reorgTp, err = pickBackfillType(job)
	if err != nil {
		return false, ver, err
	}
	if !reorgTp.NeedMergeProcess() {
		skipReorg := checkIfTableReorgWorkCanSkip(w.store, w.sess.Session(), tbl, job)
		if skipReorg {
			logutil.DDLLogger().Info("table is empty, skipping reorg work",
				zap.Int64("jobID", job.ID),
				zap.String("table", tbl.Meta().Name.O))
			return true, ver, nil
		}
		return runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
	}
	switch allIndexInfos[0].BackfillState {
	case model.BackfillStateRunning:
		skipReorg := checkIfTableReorgWorkCanSkip(w.store, w.sess.Session(), tbl, job)
		if !skipReorg {
			logutil.DDLLogger().Info("index backfill state running",
				zap.Int64("job ID", job.ID), zap.String("table", tbl.Meta().Name.O),
				zap.Bool("ingest mode", reorgTp == model.ReorgTypeIngest),
				zap.String("index", allIndexInfos[0].Name.O))
			switch reorgTp {
			case model.ReorgTypeIngest:
				if job.ReorgMeta.IsDistReorg {
					done, ver, err = runIngestReorgJobDist(w, jobCtx, job, tbl, allIndexInfos)
				} else {
					done, ver, err = runIngestReorgJob(w, jobCtx, job, tbl, allIndexInfos)
				}
			case model.ReorgTypeTxnMerge:
				done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
			}
			if err != nil || !done {
				return false, ver, errors.Trace(err)
			}
		} else {
			failpoint.InjectCall("afterCheckTableReorgCanSkip")
			logutil.DDLLogger().Info("table is empty, skipping reorg work",
				zap.Int64("jobID", job.ID),
				zap.String("table", tbl.Meta().Name.O))
		}
		for _, indexInfo := range allIndexInfos {
			indexInfo.BackfillState = model.BackfillStateReadyToMerge
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tbl.Meta(), true)
		failpoint.InjectCall("afterBackfillStateRunningDone", job)
		return false, ver, errors.Trace(err)
	case model.BackfillStateReadyToMerge:
		failpoint.InjectCall("beforeBackfillMerge")
		logutil.DDLLogger().Info("index backfill state ready to merge",
			zap.Int64("job ID", job.ID),
			zap.String("table", tbl.Meta().Name.O),
			zap.String("index", allIndexInfos[0].Name.O))
		for _, indexInfo := range allIndexInfos {
			indexInfo.BackfillState = model.BackfillStateMerging
		}
		job.SnapshotVer = 0 // Reset the snapshot version for merge index reorg.
		ver, err = updateVersionAndTableInfo(jobCtx, job, tbl.Meta(), true)
		return false, ver, errors.Trace(err)
	case model.BackfillStateMerging:
		skipReorg := checkIfTempIndexReorgWorkCanSkip(w.store, w.sess.Session(), tbl, allIndexInfos, job)
		if !skipReorg {
			done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, true)
			if !done {
				return false, ver, err
			}
		} else {
			failpoint.InjectCall("afterCheckTempIndexReorgCanSkip")
			logutil.DDLLogger().Info("temp index is empty, skipping reorg work",
				zap.Int64("jobID", job.ID),
				zap.String("table", tbl.Meta().Name.O))
		}
		for _, indexInfo := range allIndexInfos {
			indexInfo.BackfillState = model.BackfillStateInapplicable // Prevent double-write on this index.
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tbl.Meta(), true)
		return true, ver, errors.Trace(err)
	default:
		return false, 0, dbterror.ErrInvalidDDLState.GenWithStackByArgs("backfill", allIndexInfos[0].BackfillState)
	}
}

func runIngestReorgJobDist(w *worker, jobCtx *jobContext, job *model.Job,
	tbl table.Table, allIndexInfos []*model.IndexInfo) (done bool, ver int64, err error) {
	done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
	if err != nil {
		return false, ver, errors.Trace(err)
	}

	if !done {
		return false, ver, nil
	}

	return true, ver, nil
}

func runIngestReorgJob(w *worker, jobCtx *jobContext, job *model.Job,
	tbl table.Table, allIndexInfos []*model.IndexInfo) (done bool, ver int64, err error) {
	done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
	if err != nil {
		if kv.ErrKeyExists.Equal(err) {
			logutil.DDLLogger().Warn("import index duplicate key, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), allIndexInfos, err)
		} else if !isRetryableJobError(err, job.ErrorCount) {
			logutil.DDLLogger().Warn("run reorg job failed, convert job to rollback",
				zap.String("job", job.String()), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), allIndexInfos, err)
		} else {
			logutil.DDLLogger().Warn("run add index ingest job error", zap.Error(err))
		}
		return false, ver, errors.Trace(err)
	}
	failpoint.InjectCall("afterRunIngestReorgJob", job, done)
	return done, ver, nil
}

func isRetryableJobError(err error, jobErrCnt int64) bool {
	if jobErrCnt+1 >= vardef.GetDDLErrorCountLimit() {
		return false
	}
	return isRetryableError(err)
}

func isRetryableError(err error) bool {
	errMsg := err.Error()
	for _, m := range dbterror.ReorgRetryableErrMsgs {
		if strings.Contains(errMsg, m) {
			return true
		}
	}
	originErr := errors.Cause(err)
	if tErr, ok := originErr.(*terror.Error); ok {
		sqlErr := terror.ToSQLError(tErr)
		_, ok := dbterror.ReorgRetryableErrCodes[sqlErr.Code]
		return ok
	}
	// For the unknown errors, we should retry.
	return true
}

func runReorgJobAndHandleErr(
	w *worker,
	jobCtx *jobContext,
	job *model.Job,
	tbl table.Table,
	allIndexInfos []*model.IndexInfo,
	mergingTmpIdx bool,
) (done bool, ver int64, err error) {
	elements := make([]*meta.Element, 0, len(allIndexInfos))
	for _, indexInfo := range allIndexInfos {
		elements = append(elements, &meta.Element{ID: indexInfo.ID, TypeKey: meta.IndexElementKey})
	}

	failpoint.InjectCall("beforeRunReorgJobAndHandleErr", allIndexInfos)

	sctx, err1 := w.sessPool.Get()
	if err1 != nil {
		err = err1
		return
	}
	defer w.sessPool.Put(sctx)
	rh := newReorgHandler(sess.NewSession(sctx))
	dbInfo, err := jobCtx.metaMut.GetDatabase(job.SchemaID)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	reorgInfo, err := getReorgInfo(jobCtx.oldDDLCtx.jobContext(job.ID, job.ReorgMeta), jobCtx, rh, job, dbInfo, tbl, elements, mergingTmpIdx)
	if err != nil || reorgInfo == nil || reorgInfo.first {
		// If we run reorg firstly, we should update the job snapshot version
		// and then run the reorg next time.
		return false, ver, errors.Trace(err)
	}
	err = overwriteReorgInfoFromGlobalCheckpoint(w, rh.s, job, reorgInfo)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	err = w.runReorgJob(jobCtx, reorgInfo, tbl.Meta(), func() (addIndexErr error) {
		defer util.Recover(metrics.LabelDDL, "onCreateIndex",
			func() {
				addIndexErr = dbterror.ErrCancelledDDLJob.GenWithStack("add table `%v` index `%v` panic", tbl.Meta().Name, allIndexInfos[0].Name)
			}, false)
		return w.addTableIndex(jobCtx, tbl, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			return false, ver, nil
		}
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return false, ver, nil
		}
		// TODO(tangenta): get duplicate column and match index.
		err = ingest.TryConvertToKeyExistsErr(err, allIndexInfos[0], tbl.Meta())
		if !isRetryableJobError(err, job.ErrorCount) {
			logutil.DDLLogger().Warn("run add index job failed, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), allIndexInfos, err)
			if err1 := rh.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
				logutil.DDLLogger().Warn("run add index job failed, convert job to rollback, RemoveDDLReorgHandle failed", zap.Stringer("job", job), zap.Error(err1))
			}
		}
		return false, ver, errors.Trace(err)
	}

	failpoint.InjectCall("afterRunReorgJobAndHandleErr")
	return true, ver, nil
}
