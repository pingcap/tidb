// Copyright 2022 PingCAP, Inc.
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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func (d *ddl) MultiSchemaChange(ctx sessionctx.Context, ti ast.Ident) error {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}
	job := &model.Job{
		SchemaID:        schema.ID,
		TableID:         t.Meta().ID,
		SchemaName:      schema.Name.L,
		Type:            model.ActionMultiSchemaChange,
		BinlogInfo:      &model.HistoryInfo{},
		Args:            nil,
		MultiSchemaInfo: ctx.GetSessionVars().StmtCtx.MultiSchemaInfo,
	}
	err = checkMultiSchemaInfo(ctx.GetSessionVars().StmtCtx.MultiSchemaInfo, t)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.GetSessionVars().StmtCtx.MultiSchemaInfo = nil
	err = d.doDDLJob(ctx, job)
	return d.callHookOnChanged(err)
}

func onMultiSchemaChange(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	if job.MultiSchemaInfo.Revertible {
		// Handle the rolling back job.
		if job.IsRollingback() {
			// Rollback/cancel the sub-jobs in reverse order.
			for i := len(job.MultiSchemaInfo.SubJobs) - 1; i >= 0; i-- {
				sub := job.MultiSchemaInfo.SubJobs[i]
				if isFinished(sub) {
					continue
				}
				proxyJob := cloneFromSubJob(job, sub)
				ver, err = w.runDDLJob(d, t, proxyJob)
				mergeBackToSubJob(proxyJob, sub)
				if i == 0 && isFinished(sub) {
					job.State = model.JobStateRollbackDone
				}
				return ver, err
			}
			// The last rollback/cancelling sub-job is done.
			job.State = model.JobStateRollbackDone
			return ver, nil
		}

		// The sub-jobs are normally running.
		// Run the first executable sub-job.
		for i, sub := range job.MultiSchemaInfo.SubJobs {
			if !sub.Revertible || isFinished(sub) {
				// Skip the sub jobs which related schema states
				// are in the last revertible point.
				// If a sub job is finished here, it should be a noop job.
				continue
			}
			proxyJob := cloneFromSubJob(job, sub)
			ver, err = w.runDDLJob(d, t, proxyJob)
			mergeBackToSubJob(proxyJob, sub)
			handleRevertibleException(job, sub, i, proxyJob.Error)
			return ver, err
		}
		// All the sub-jobs are non-revertible.
		job.MultiSchemaInfo.Revertible = false
		// Step the sub-jobs to the non-revertible states all at once.
		for _, sub := range job.MultiSchemaInfo.SubJobs {
			if isFinished(sub) {
				continue
			}
			proxyJob := cloneFromSubJob(job, sub)
			ver, err = w.runDDLJob(d, t, proxyJob)
			mergeBackToSubJob(proxyJob, sub)
		}
		return ver, err
	}
	// Run the rest non-revertible sub-jobs one by one.
	for _, sub := range job.MultiSchemaInfo.SubJobs {
		if isFinished(sub) {
			continue
		}
		proxyJob := cloneFromSubJob(job, sub)
		ver, err = w.runDDLJob(d, t, proxyJob)
		mergeBackToSubJob(proxyJob, sub)
		return ver, err
	}
	job.State = model.JobStateDone
	return ver, err
}

func isFinished(job *model.SubJob) bool {
	return job.State == model.JobStateDone ||
		job.State == model.JobStateRollbackDone ||
		job.State == model.JobStateCancelled
}

func cloneFromSubJob(job *model.Job, sub *model.SubJob) *model.Job {
	return &model.Job{
		ID:              job.ID,
		Type:            sub.Type,
		SchemaID:        job.SchemaID,
		TableID:         job.TableID,
		SchemaName:      job.SchemaName,
		State:           sub.State,
		Warning:         sub.Warning,
		Error:           nil,
		ErrorCount:      0,
		RowCount:        0,
		Mu:              sync.Mutex{},
		CtxVars:         sub.CtxVars,
		Args:            sub.Args,
		RawArgs:         sub.RawArgs,
		SchemaState:     sub.SchemaState,
		SnapshotVer:     sub.SnapshotVer,
		RealStartTS:     job.RealStartTS,
		StartTS:         job.StartTS,
		DependencyID:    job.DependencyID,
		Query:           job.Query,
		BinlogInfo:      job.BinlogInfo,
		Version:         job.Version,
		ReorgMeta:       job.ReorgMeta,
		MultiSchemaInfo: &model.MultiSchemaInfo{Revertible: sub.Revertible},
		Priority:        job.Priority,
		SeqNum:          job.SeqNum,
	}
}

func mergeBackToSubJob(job *model.Job, sub *model.SubJob) {
	sub.Revertible = job.MultiSchemaInfo.Revertible
	sub.SchemaState = job.SchemaState
	sub.SnapshotVer = job.SnapshotVer
	sub.Args = job.Args
	sub.State = job.State
	sub.Warning = job.Warning
}

func handleRevertibleException(job *model.Job, subJob *model.SubJob, idx int, err *terror.Error) {
	if !isAbnormal(subJob) {
		return
	}
	job.State = model.JobStateRollingback
	job.Error = err
	// Flush the cancelling state and cancelled state to sub-jobs.
	for _, sub := range job.MultiSchemaInfo.SubJobs {
		switch sub.State {
		case model.JobStateRunning:
			sub.State = model.JobStateCancelling
		case model.JobStateNone:
			sub.State = model.JobStateCancelled
		}
	}
}

func isAbnormal(job *model.SubJob) bool {
	return job.State == model.JobStateCancelling ||
		job.State == model.JobStateCancelled ||
		job.State == model.JobStateRollingback ||
		job.State == model.JobStateRollbackDone
}

func appendToSubJobs(m *model.MultiSchemaInfo, job *model.Job) error {
	err := fillMultiSchemaInfo(m, job)
	if err != nil {
		return err
	}
	m.SubJobs = append(m.SubJobs, &model.SubJob{
		Type:        job.Type,
		Args:        job.Args,
		RawArgs:     job.RawArgs,
		SchemaState: job.SchemaState,
		SnapshotVer: job.SnapshotVer,
		Revertible:  true,
		CtxVars:     job.CtxVars,
	})
	return nil
}

func fillMultiSchemaInfo(info *model.MultiSchemaInfo, job *model.Job) (err error) {
	switch job.Type {
	case model.ActionAddColumn:
		col := job.Args[0].(*table.Column)
		pos := job.Args[1].(*ast.ColumnPosition)
		info.AddColumns = append(info.AddColumns, col.Name)
		if pos != nil && pos.Tp == ast.ColumnPositionAfter {
			info.RelativeColumns = append(info.RelativeColumns, pos.RelativeColumn.Name)
		}
	case model.ActionDropColumn:
		colName := job.Args[0].(model.CIStr)
		info.DropColumns = append(info.DropColumns, colName)
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		indexName := job.Args[0].(model.CIStr)
		info.DropIndexes = append(info.DropIndexes, indexName)
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		indexName := job.Args[1].(model.CIStr)
		indexPartSpecifications := job.Args[2].([]*ast.IndexPartSpecification)
		info.AddIndexes = append(info.AddIndexes, indexName)
		for _, indexPartSpecification := range indexPartSpecifications {
			info.RelativeColumns = append(info.RelativeColumns, indexPartSpecification.Column.Name)
		}
	case model.ActionRenameIndex:
		from := job.Args[0].(model.CIStr)
		to := job.Args[1].(model.CIStr)
		info.AddIndexes = append(info.AddIndexes, to)
		info.DropIndexes = append(info.DropIndexes, from)
	case model.ActionModifyColumn:
		newCol := *job.Args[0].(**model.ColumnInfo)
		oldColName := job.Args[1].(model.CIStr)
		pos := job.Args[2].(*ast.ColumnPosition)
		if newCol.Name.L != oldColName.L {
			info.AddColumns = append(info.AddColumns, newCol.Name)
			info.DropColumns = append(info.DropColumns, oldColName)
		} else {
			info.RelativeColumns = append(info.RelativeColumns, newCol.Name)
		}
		if pos != nil && pos.Tp == ast.ColumnPositionAfter {
			info.RelativeColumns = append(info.RelativeColumns, pos.RelativeColumn.Name)
		}
	default:
		return dbterror.ErrRunMultiSchemaChanges
	}
	return nil
}

func checkOperateSameColumn(info *model.MultiSchemaInfo) error {
	modifyCols := make(map[string]struct{})
	modifyIdx := make(map[string]struct{})

	checkColumns := func(colNames []model.CIStr, addToModifyCols bool) error {
		for _, colName := range colNames {
			name := colName.L
			if _, ok := modifyCols[name]; ok {
				return dbterror.ErrOperateSameColumn.GenWithStackByArgs(name)
			}
			if addToModifyCols {
				modifyCols[name] = struct{}{}
			}
		}
		return nil
	}

	checkIndexes := func(idxNames []model.CIStr, addToModifyIdx bool) error {
		for _, idxName := range idxNames {
			name := idxName.L
			if _, ok := modifyIdx[name]; ok {
				return dbterror.ErrOperateSameColumn.GenWithStackByArgs(name)
			}
			if addToModifyIdx {
				modifyIdx[name] = struct{}{}
			}
		}
		return nil
	}

	if err := checkColumns(info.AddColumns, true); err != nil {
		return err
	}
	if err := checkColumns(info.DropColumns, true); err != nil {
		return err
	}
	if err := checkIndexes(info.AddIndexes, true); err != nil {
		return err
	}
	if err := checkIndexes(info.DropIndexes, true); err != nil {
		return err
	}

	return checkColumns(info.RelativeColumns, false)
}

func checkMultiSchemaInfo(info *model.MultiSchemaInfo, t table.Table) error {
	err := checkOperateSameColumn(info)
	if err != nil {
		return err
	}

	err = checkVisibleColumnCnt(t, len(info.AddColumns), len(info.DropColumns))
	if err != nil {
		return err
	}

	return checkAddColumnTooManyColumns(len(t.Cols()) + len(info.AddColumns) - len(info.DropColumns))
}

func appendMultiChangeWarningsToOwnerCtx(ctx sessionctx.Context, job *model.Job) {
	if job.MultiSchemaInfo == nil || job.Type != model.ActionMultiSchemaChange {
		return
	}
	for _, sub := range job.MultiSchemaInfo.SubJobs {
		if sub.Warning != nil {
			ctx.GetSessionVars().StmtCtx.AppendNote(sub.Warning)
		}
	}
}

// rollingBackMultiSchemaChange updates a multi-schema change job
// from cancelling state to rollingback state.
func rollingBackMultiSchemaChange(job *model.Job) error {
	if !job.MultiSchemaInfo.Revertible {
		// Cannot rolling back because the jobs are non-revertible.
		// Resume the job state to running.
		job.State = model.JobStateRunning
		return nil
	}
	// Mark all the jobs to cancelling.
	for _, sub := range job.MultiSchemaInfo.SubJobs {
		switch sub.State {
		case model.JobStateRunning:
			sub.State = model.JobStateCancelling
		case model.JobStateNone:
			sub.State = model.JobStateCancelled
		}
	}
	job.State = model.JobStateRollingback
	return dbterror.ErrCancelledDDLJob
}

func multiSchemaChangeOnCreateIndexPending(t *meta.Meta, job *model.Job,
	tblInfo *model.TableInfo) (done bool, ver int64, err error) {
	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		// For multi-schema change, we don't public the index immediately.
		// Mark the job non-revertible and wait for other sub-jobs to finish.
		job.MarkNonRevertible()
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, false)
		return true, ver, err
	}
	return false, 0, nil
}

func multiSchemaChangeOnCreateIndexFinish(t *meta.Meta, job *model.Job,
	tblInfo *model.TableInfo, indexInfo *model.IndexInfo) (done bool, ver int64, err error) {
	if job.State != model.JobStateCancelling && job.MultiSchemaInfo != nil && !job.MultiSchemaInfo.Revertible {
		// The multi-schema change steps to a non-revertible state.
		// Public the index and finish the sub-job.
		indexInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
		if err != nil {
			return true, ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		return true, ver, nil
	}
	return false, 0, nil
}

func multiSchemaChangeOnCreateIndexCancelling(err error, t *meta.Meta, job *model.Job,
	tblInfo *model.TableInfo, indexInfo *model.IndexInfo) (done bool, ver int64, err1 error) {
	if job.State == model.JobStateCancelling && meta.ErrDDLReorgElementNotExist.Equal(err) && job.MultiSchemaInfo != nil {
		// For multi-schema change, the absence of the reorg element means the sub-job has finished the reorg.
		logutil.BgLogger().Warn("[ddl] run add index job failed, convert job to rollback",
			zap.String("job", job.String()), zap.Error(err))

		ver, err1 = convertAddIdxJob2RollbackJob(t, job, tblInfo, indexInfo, dbterror.ErrCancelledDDLJob)
		return true, ver, err1
	}
	return false, 0, err
}
