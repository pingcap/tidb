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
	"bytes"
	"context"
	"encoding/hex"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)
func overwriteReorgInfoFromGlobalCheckpoint(w *worker, sess *sess.Session, job *model.Job, reorgInfo *reorgInfo) error {
	if job.ReorgMeta.ReorgTp != model.ReorgTypeIngest {
		// Only used for the ingest mode job.
		return nil
	}
	if reorgInfo.mergingTmpIdx {
		// Merging the temporary index uses txn mode, so we don't need to consider the checkpoint.
		return nil
	}
	if job.ReorgMeta.IsDistReorg {
		// The global checkpoint is not used in distributed tasks.
		return nil
	}
	if w.getReorgCtx(job.ID) != nil {
		// We only overwrite from checkpoint when the job runs for the first time on this TiDB instance.
		return nil
	}
	start, pid, err := getImportedKeyFromCheckpoint(sess, job)
	if err != nil {
		return errors.Trace(err)
	}
	if pid != reorgInfo.PhysicalTableID {
		// Current physical ID does not match checkpoint physical ID.
		// Don't overwrite reorgInfo.StartKey.
		return nil
	}
	if len(start) > 0 {
		reorgInfo.StartKey = start
	}
	return nil
}

func extractElemIDs(r *reorgInfo) []int64 {
	elemIDs := make([]int64, 0, len(r.elements))
	for _, elem := range r.elements {
		if !bytes.Equal(elem.TypeKey, meta.IndexElementKey) {
			continue
		}
		elemIDs = append(elemIDs, elem.ID)
	}
	return elemIDs
}

func (w *worker) mergeWarningsIntoJob(job *model.Job) {
	rc := w.getReorgCtx(job.ID)
	rc.mu.Lock()
	partWarnings := rc.mu.warnings
	partWarningsCount := rc.mu.warningsCount
	rc.mu.Unlock()
	warnings, warningsCount := job.GetWarnings()
	warnings, warningsCount = mergeWarningsAndWarningsCount(partWarnings, warnings, partWarningsCount, warningsCount)
	job.SetWarnings(warnings, warningsCount)
}

func updateBackfillProgress(w *worker, reorgInfo *reorgInfo, tblInfo *model.TableInfo,
	addedRowCount int64) {
	if tblInfo == nil {
		return
	}
	progress := float64(0)
	if addedRowCount != 0 {
		totalCount := getTableTotalCount(w, tblInfo)
		if totalCount > 0 {
			progress = float64(addedRowCount) / float64(totalCount)
		} else {
			progress = 0
		}
		if progress > 1 {
			progress = 1
		}
		// Prevent progress regression by keeping track of the maximum progress.
		rc := w.getReorgCtx(reorgInfo.ID)
		if rc != nil {
			progress = rc.setMaxProgress(progress)
		}
		logutil.DDLLogger().Debug("update progress",
			zap.Float64("progress", progress),
			zap.Int64("addedRowCount", addedRowCount),
			zap.Int64("totalCount", totalCount))
	}
	switch reorgInfo.Type {
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		var label string
		if reorgInfo.mergingTmpIdx {
			label = metrics.LblAddIndexMerge
		} else {
			label = metrics.LblAddIndex
		}
		idxNames := ""
		args, err := model.GetModifyIndexArgs(reorgInfo.Job)
		if err != nil {
			logutil.DDLLogger().Error("Fail to get ModifyIndexArgs", zap.Error(err))
		} else {
			idxNames = getIdxNamesFromArgs(args)
		}
		metrics.GetBackfillProgressByLabel(label, reorgInfo.SchemaName, tblInfo.Name.String(), idxNames).Set(progress * 100)
	case model.ActionModifyColumn:
		colName := ""
		args, err := model.GetModifyColumnArgs(reorgInfo.Job)
		if err != nil {
			logutil.DDLLogger().Error("Fail to get ModifyColumnArgs", zap.Error(err))
		} else {
			colName = args.OldColumnName.O
		}
		metrics.GetBackfillProgressByLabel(metrics.LblModifyColumn, reorgInfo.SchemaName, tblInfo.Name.String(), colName).Set(progress * 100)
	case model.ActionReorganizePartition, model.ActionRemovePartitioning,
		model.ActionAlterTablePartitioning:
		metrics.GetBackfillProgressByLabel(metrics.LblReorgPartition, reorgInfo.SchemaName, tblInfo.Name.String(), "").Set(progress * 100)
	}
}

func getTableTotalCount(w *worker, tblInfo *model.TableInfo) int64 {
	var ctx sessionctx.Context
	ctx, err := w.sessPool.Get()
	if err != nil {
		return statistics.PseudoRowCount
	}
	defer w.sessPool.Put(ctx)
	executor := ctx.GetRestrictedSQLExecutor()
	var rows []chunk.Row
	if tblInfo.Partition != nil && len(tblInfo.Partition.DroppingDefinitions) > 0 {
		// if Reorganize Partition, only select number of rows from the selected partitions!
		defs := tblInfo.Partition.DroppingDefinitions
		partIDs := make([]string, 0, len(defs))
		for _, def := range defs {
			partIDs = append(partIDs, strconv.FormatInt(def.ID, 10))
		}
		sql := "select sum(table_rows) from information_schema.partitions where tidb_partition_id in (%?);"
		rows, _, err = executor.ExecRestrictedSQL(w.workCtx, nil, sql, strings.Join(partIDs, ","))
	} else {
		sql := "select table_rows from information_schema.tables where tidb_table_id=%?;"
		rows, _, err = executor.ExecRestrictedSQL(w.workCtx, nil, sql, tblInfo.ID)
	}
	if err != nil {
		return statistics.PseudoRowCount
	}
	if len(rows) != 1 {
		return statistics.PseudoRowCount
	}
	return rows[0].GetInt64(0)
}

func (dc *ddlCtx) isReorgRunnable(ctx context.Context, isDistReorg bool) error {
	if dc.ctx.Err() != nil {
		// Worker is closed. So it can't do the reorganization.
		return dbterror.ErrInvalidWorker.GenWithStack("worker is closed")
	}

	if ctx.Err() != nil {
		return context.Cause(ctx)
	}

	// If isDistReorg is true, we needn't check if it is owner.
	if isDistReorg {
		return nil
	}
	if !dc.isOwner() {
		// If it's not the owner, we will try later, so here just returns an error.
		logutil.DDLLogger().Info("DDL is not the DDL owner", zap.String("ID", dc.uuid))
		return errors.Trace(dbterror.ErrNotOwner)
	}
	return nil
}

type reorgInfo struct {
	*model.Job

	StartKey      kv.Key
	EndKey        kv.Key
	jobCtx        *jobContext
	first         bool
	mergingTmpIdx bool
	// PhysicalTableID is used for partitioned table.
	// DDL reorganize for a partitioned table will handle partitions one by one,
	// PhysicalTableID is used to trace the current partition we are handling.
	// If the table is not partitioned, PhysicalTableID would be TableID.
	PhysicalTableID int64
	dbInfo          *model.DBInfo
	elements        []*meta.Element
	currElement     *meta.Element
}

func (r *reorgInfo) NewJobContext() *ReorgContext {
	return r.jobCtx.oldDDLCtx.jobContext(r.Job.ID, r.Job.ReorgMeta)
}

func (r *reorgInfo) String() string {
	var isEnabled bool
	if ingest.LitInitialized {
		isEnabled = r.ReorgMeta != nil && r.ReorgMeta.IsFastReorg
	}
	return "CurrElementType:" + string(r.currElement.TypeKey) + "," +
		"CurrElementID:" + strconv.FormatInt(r.currElement.ID, 10) + "," +
		"StartKey:" + hex.EncodeToString(r.StartKey) + "," +
		"EndKey:" + hex.EncodeToString(r.EndKey) + "," +
		"First:" + strconv.FormatBool(r.first) + "," +
		"PhysicalTableID:" + strconv.FormatInt(r.PhysicalTableID, 10) + "," +
		"Ingest mode:" + strconv.FormatBool(isEnabled)
}

// UpdateConfigFromSysTbl updates the reorg config from system table.
func (r *reorgInfo) UpdateConfigFromSysTbl(ctx context.Context) {
	latestJob, err := r.jobCtx.sysTblMgr.GetJobByID(ctx, r.ID)
	if err != nil {
		logutil.DDLLogger().Warn("failed to get latest job from system table",
			zap.Int64("jobID", r.ID), zap.Error(err))
		return
	}
	if latestJob.State == model.JobStateRunning && latestJob.IsAlterable() {
		r.ReorgMeta.SetConcurrency(latestJob.ReorgMeta.GetConcurrency())
		r.ReorgMeta.SetBatchSize(latestJob.ReorgMeta.GetBatchSize())
		r.ReorgMeta.SetMaxWriteSpeed(latestJob.ReorgMeta.GetMaxWriteSpeed())
	}
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

func getReorgInfo(ctx *ReorgContext, jobCtx *jobContext, rh *reorgHandler, job *model.Job, dbInfo *model.DBInfo,
	tbl table.Table, elements []*meta.Element, mergingTmpIdx bool) (*reorgInfo, error) {
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
				if err := rh.RemoveReorgElementFailPoint(job); err != nil {
					failpoint.Return(nil, errors.Trace(err))
				}
				info.first = true
				failpoint.Return(&info, nil)
			}
		})

		info.first = true
		delayForAsyncCommit()
		ver, err := getValidCurrentVersion(jobCtx.store)
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
		if mergingTmpIdx {
			for _, element := range elements {
				if !bytes.Equal(element.TypeKey, meta.IndexElementKey) {
					continue
				}
				// If has a global index in elements, need start process at `tblInfo.ID`
				// because there are some temporary global indexes prefixed with table ID.
				idxInfo := model.FindIndexInfoByID(tblInfo.Indices, element.ID)
				if idxInfo.Global {
					pid = tblInfo.ID
				}
			}
			start, end = encodeTempIndexRange(pid, elements[0].ID, elements[len(elements)-1].ID)
		} else {
			start, end, err = getTableRange(ctx, jobCtx.store, tb, ver.Ver, job.Priority)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		logutil.DDLLogger().Info("job get table range",
			zap.Int64("jobID", job.ID), zap.Int64("physicalTableID", pid),
			zap.String("startKey", hex.EncodeToString(start)),
			zap.String("endKey", hex.EncodeToString(end)))

		failpoint.Inject("errorUpdateReorgHandle", func() {
			failpoint.Return(&info, errors.New("occur an error when update reorg handle"))
		})
		err = rh.InitDDLReorgHandle(job, start, end, pid, elements[0])
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
				if err := rh.RemoveReorgElementFailPoint(job); err != nil {
					failpoint.Return(nil, errors.Trace(err))
				}
			}
		})

		var err error
		element, start, end, pid, err = rh.GetDDLReorgHandle(job)
		if err != nil {
			// If the reorg element doesn't exist, this reorg info should be saved by the older TiDB versions.
			// It's compatible with the older TiDB versions.
			// We'll try to remove it in the next major TiDB version.
			if meta.ErrDDLReorgElementNotExist.Equal(err) {
				job.SnapshotVer = 0
				logutil.DDLLogger().Warn("get reorg info, the element does not exist", zap.Stringer("job", job))
				if job.IsCancelling() {
					return nil, nil
				}
			}
			return &info, errors.Trace(err)
		}
	}
	info.Job = job
	info.jobCtx = jobCtx
	info.StartKey = start
	info.EndKey = end
	info.PhysicalTableID = pid
	info.currElement = element
	info.elements = elements
	info.mergingTmpIdx = mergingTmpIdx
	info.dbInfo = dbInfo

	return &info, nil
}

