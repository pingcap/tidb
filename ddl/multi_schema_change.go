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

	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/dbterror"
)

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
		CtxVars:         nil,
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
	case model.ActionDropIndex:
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

	if err := checkColumns(info.RelativeColumns, false); err != nil {
		return err
	}
	return nil
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

	err = checkAddColumnTooManyColumns(len(t.Cols()) + len(info.AddColumns) - len(info.DropColumns))
	if err != nil {
		return err
	}
	return nil
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
