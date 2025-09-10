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
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
)

func onMultiSchemaChange(w *worker, jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	jobCtx.inInnerRunOneJobStep = true
	defer func() {
		jobCtx.inInnerRunOneJobStep = false
	}()
	metaMut := jobCtx.metaMut
	if job.MultiSchemaInfo.Revertible {
		// Handle the rolling back job.
		if job.IsRollingback() {
			// Rollback/cancel the sub-jobs in reverse order.
			for i := len(job.MultiSchemaInfo.SubJobs) - 1; i >= 0; i-- {
				sub := job.MultiSchemaInfo.SubJobs[i]
				if sub.IsFinished() {
					continue
				}
				proxyJob := sub.ToProxyJob(job, i)
				ver, _, err = w.runOneJobStep(jobCtx, &proxyJob)
				err = handleRollbackException(err, proxyJob.Error)
				if err != nil {
					return ver, err
				}
				sub.FromProxyJob(&proxyJob, ver)
				return ver, nil
			}
			// The last rollback/cancelling sub-job is done.
			job.State = model.JobStateRollbackDone
			return ver, nil
		}

		// The sub-jobs are normally running.
		// Run the first executable sub-job.
		for i, sub := range job.MultiSchemaInfo.SubJobs {
			if !sub.Revertible || sub.IsFinished() {
				// Skip the sub-jobs which related schema states
				// are in the last revertible point.
				// If a sub job is finished here, it should be a noop job.
				continue
			}
			proxyJob := sub.ToProxyJob(job, i)
			ver, _, err = w.runOneJobStep(jobCtx, &proxyJob)
			sub.FromProxyJob(&proxyJob, ver)
			handleRevertibleException(job, sub, proxyJob.Error)
			return ver, err
		}

		// Save table info and sub-jobs for rolling back.
		var tblInfo *model.TableInfo
		tblInfo, err = metaMut.GetTable(job.SchemaID, job.TableID)
		if err != nil {
			return ver, err
		}
		var schemaVersionGenerated = false
		subJobs := make([]model.SubJob, len(job.MultiSchemaInfo.SubJobs))
		// Step the sub-jobs to the non-revertible states all at once.
		// We only generate 1 schema version for these sub-job.
		actionTypes := make([]model.ActionType, 0, len(job.MultiSchemaInfo.SubJobs))
		for i, sub := range job.MultiSchemaInfo.SubJobs {
			if sub.IsFinished() {
				continue
			}
			subJobs[i] = *sub
			proxyJob := sub.ToProxyJob(job, i)
			if schemaVersionGenerated {
				proxyJob.MultiSchemaInfo.SkipVersion = true
			}
			proxyJobVer, _, err := w.runOneJobStep(jobCtx, &proxyJob)
			if !schemaVersionGenerated && proxyJobVer != 0 {
				schemaVersionGenerated = true
				ver = proxyJobVer
			}
			sub.FromProxyJob(&proxyJob, proxyJobVer)
			if err != nil || proxyJob.Error != nil {
				for j := i - 1; j >= 0; j-- {
					// TODO if some sub-job is finished, this will empty them
					// also some sub-job cannot be rollback completely, maybe keep them?
					job.MultiSchemaInfo.SubJobs[j] = &subJobs[j]
				}
				handleRevertibleException(job, sub, proxyJob.Error)
				// The TableInfo and sub-jobs should be restored
				// because some schema changes update the transaction aggressively.
				// TODO this error handling cannot handle below case:
				// suppose the job is for "alter table t auto_increment = 100, add column c int".
				// if we fail on "add column c int", the allocator is rebased to 100
				// which cannot be rollback, but it's table-info.AutoIncID is rollback by below call.
				// TODO we should also change schema diff of 'ver' if len(actionTypes) > 1.
				return updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
			}
			actionTypes = append(actionTypes, sub.Type)
		}
		if len(actionTypes) > 1 {
			// only single table schema changes can be put into a multi-schema-change
			// job except AddForeignKey which is handled separately in the first loop.
			// so this diff is enough, but it wound be better to accumulate all the diffs,
			// and then merge them into a single diff.
			if err = metaMut.SetSchemaDiff(&model.SchemaDiff{
				Version:        ver,
				Type:           job.Type,
				TableID:        job.TableID,
				SchemaID:       job.SchemaID,
				SubActionTypes: actionTypes,
			}); err != nil {
				return ver, err
			}
		}
		// All the sub-jobs are non-revertible.
		job.MarkNonRevertible()
		return ver, err
	}
	// Run the rest non-revertible sub-jobs one by one.
	for i, sub := range job.MultiSchemaInfo.SubJobs {
		if sub.IsFinished() {
			continue
		}
		proxyJob := sub.ToProxyJob(job, i)
		ver, _, err = w.runOneJobStep(jobCtx, &proxyJob)
		sub.FromProxyJob(&proxyJob, ver)
		return ver, err
	}
	return finishMultiSchemaJob(job, metaMut)
}

func handleRevertibleException(job *model.Job, subJob *model.SubJob, err *terror.Error) {
	if subJob.IsNormal() {
		return
	}
	job.State = model.JobStateRollingback
	job.Error = err
	// Flush the cancelling state and cancelled state to sub-jobs.
	for _, sub := range job.MultiSchemaInfo.SubJobs {
		switch sub.State {
		case model.JobStateRunning:
			sub.State = model.JobStateCancelling
		case model.JobStateNone, model.JobStateQueueing:
			sub.State = model.JobStateCancelled
		}
	}
}

func handleRollbackException(runJobErr error, proxyJobErr *terror.Error) error {
	if runJobErr != nil {
		// The physical errors are not recoverable during rolling back.
		// We keep retrying it.
		return runJobErr
	}
	if proxyJobErr != nil {
		if proxyJobErr.Equal(dbterror.ErrCancelledDDLJob) {
			// A cancelled DDL error is normal during rolling back.
			return nil
		}
		return proxyJobErr
	}
	return nil
}

func appendToSubJobs(m *model.MultiSchemaInfo, jobW *JobWrapper) error {
	err := fillMultiSchemaInfo(m, jobW)
	if err != nil {
		return err
	}
	var reorgTp model.ReorgType
	if jobW.ReorgMeta != nil {
		reorgTp = jobW.ReorgMeta.ReorgTp
	}
	m.SubJobs = append(m.SubJobs, &model.SubJob{
		Type:        jobW.Type,
		JobArgs:     jobW.JobArgs,
		RawArgs:     jobW.RawArgs,
		SchemaState: jobW.SchemaState,
		SnapshotVer: jobW.SnapshotVer,
		Revertible:  true,
		CtxVars:     jobW.CtxVars,
		ReorgTp:     reorgTp,
	})
	return nil
}

func fillMultiSchemaInfo(info *model.MultiSchemaInfo, job *JobWrapper) error {
	switch job.Type {
	case model.ActionAddColumn:
		args := job.JobArgs.(*model.TableColumnArgs)
		col, pos := args.Col, args.Pos
		info.AddColumns = append(info.AddColumns, col.Name)
		for colName := range col.Dependences {
			info.RelativeColumns = append(info.RelativeColumns, ast.CIStr{L: colName, O: colName})
		}
		if pos != nil && pos.Tp == ast.ColumnPositionAfter {
			info.PositionColumns = append(info.PositionColumns, pos.RelativeColumn.Name)
		}
	case model.ActionDropColumn:
		colName := job.JobArgs.(*model.TableColumnArgs).Col.Name
		info.DropColumns = append(info.DropColumns, colName)
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		args := job.JobArgs.(*model.ModifyIndexArgs)
		info.DropIndexes = append(info.DropIndexes, args.IndexArgs[0].IndexName)
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		args := job.JobArgs.(*model.ModifyIndexArgs)
		// This job has not been merged, len(args) should be one.
		intest.Assert(len(args.IndexArgs) == 1, "len(args.IndexArgs) != 1")
		indexArg := args.IndexArgs[0]
		info.AddIndexes = append(info.AddIndexes, indexArg.IndexName)
		for _, indexPartSpecification := range indexArg.IndexPartSpecifications {
			info.RelativeColumns = append(info.RelativeColumns, indexPartSpecification.Column.Name)
		}
		for _, c := range indexArg.HiddenCols {
			for depColName := range c.Dependences {
				info.RelativeColumns = append(info.RelativeColumns, ast.NewCIStr(depColName))
			}
		}
	case model.ActionRenameIndex:
		args := job.JobArgs.(*model.ModifyIndexArgs)
		from, to := args.GetRenameIndexes()
		info.AddIndexes = append(info.AddIndexes, from)
		info.DropIndexes = append(info.DropIndexes, to)
	case model.ActionModifyColumn:
		args := job.JobArgs.(*model.ModifyColumnArgs)
		newCol, oldColName, pos := args.Column, args.OldColumnName, args.Position
		if newCol.Name.L != oldColName.L {
			info.AddColumns = append(info.AddColumns, newCol.Name)
			info.DropColumns = append(info.DropColumns, oldColName)
		} else {
			info.ModifyColumns = append(info.ModifyColumns, newCol.Name)
		}
		if pos != nil && pos.Tp == ast.ColumnPositionAfter {
			info.PositionColumns = append(info.PositionColumns, pos.RelativeColumn.Name)
		}
	case model.ActionSetDefaultValue:
		args := job.JobArgs.(*model.SetDefaultValueArgs)
		col := args.Col
		info.ModifyColumns = append(info.ModifyColumns, col.Name)
	case model.ActionAlterIndexVisibility:
		idxName := job.JobArgs.(*model.AlterIndexVisibilityArgs).IndexName
		info.AlterIndexes = append(info.AlterIndexes, idxName)
	case model.ActionRebaseAutoID, model.ActionModifyTableComment, model.ActionModifyTableCharsetAndCollate:
	case model.ActionAddForeignKey:
		fkInfo := job.JobArgs.(*model.AddForeignKeyArgs).FkInfo
		info.AddForeignKeys = append(info.AddForeignKeys, model.AddForeignKeyInfo{
			Name: fkInfo.Name,
			Cols: fkInfo.Cols,
		})
	case model.ActionDropForeignKey:
		// there is nothing to verify for `DROP FOREIGN KEY`
	default:
		return dbterror.ErrRunMultiSchemaChanges.FastGenByArgs(job.Type.String())
	}
	return nil
}

func checkOperateSameColAndIdx(info *model.MultiSchemaInfo) error {
	modifyCols := make(map[string]struct{})
	modifyIdx := make(map[string]struct{})

	checkColumns := func(colNames []ast.CIStr, addToModifyCols bool) error {
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

	checkIndexes := func(idxNames []ast.CIStr, addToModifyIdx bool) error {
		for _, idxName := range idxNames {
			name := idxName.L
			if _, ok := modifyIdx[name]; ok {
				return dbterror.ErrOperateSameIndex.GenWithStackByArgs(name)
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
	if err := checkColumns(info.PositionColumns, false); err != nil {
		return err
	}
	if err := checkColumns(info.ModifyColumns, true); err != nil {
		return err
	}
	if err := checkColumns(info.RelativeColumns, false); err != nil {
		return err
	}

	if err := checkIndexes(info.AddIndexes, true); err != nil {
		return err
	}
	if err := checkIndexes(info.DropIndexes, true); err != nil {
		return err
	}
	return checkIndexes(info.AlterIndexes, true)
}

func mergeAddIndex(info *model.MultiSchemaInfo) {
	var mergedSubJob *model.SubJob
	var mergeCnt int
	for _, subJob := range info.SubJobs {
		if subJob.Type == model.ActionAddForeignKey {
			// Foreign key requires the order of adding indexes is unchanged.
			return
		}
		if subJob.Type == model.ActionAddIndex {
			mergeCnt++
			if mergedSubJob == nil {
				mergedSubJob = subJob.Clone()
				mergedSubJob.RawArgs = nil
			}
		}
	}

	if mergeCnt <= 1 {
		// No multiple add index jobs in this multi-schema change.
		return
	}

	newSubJobs := make([]*model.SubJob, 0, len(info.SubJobs))
	newAddIndexesArgs := &model.ModifyIndexArgs{OpType: model.OpAddIndex}

	for _, subJob := range info.SubJobs {
		if subJob.Type == model.ActionAddIndex {
			args := subJob.JobArgs.(*model.ModifyIndexArgs)
			newAddIndexesArgs.IndexArgs = append(newAddIndexesArgs.IndexArgs, args.IndexArgs...)
		} else {
			newSubJobs = append(newSubJobs, subJob)
		}
	}

	// place the merged add index job at the end of the sub-jobs.
	mergedSubJob.JobArgs = newAddIndexesArgs
	newSubJobs = append(newSubJobs, mergedSubJob)
	info.SubJobs = newSubJobs
}

func checkOperateDropIndexUseByForeignKey(info *model.MultiSchemaInfo, t table.Table) error {
	var remainIndexes, droppingIndexes []*model.IndexInfo
	tbInfo := t.Meta()
	for _, idx := range tbInfo.Indices {
		dropping := false
		for _, name := range info.DropIndexes {
			if name.L == idx.Name.L {
				dropping = true
				break
			}
		}
		if dropping {
			droppingIndexes = append(droppingIndexes, idx)
		} else {
			remainIndexes = append(remainIndexes, idx)
		}
	}

	for _, fk := range info.AddForeignKeys {
		if droppingIdx := model.FindIndexByColumns(tbInfo, droppingIndexes, fk.Cols...); droppingIdx != nil && model.FindIndexByColumns(tbInfo, remainIndexes, fk.Cols...) == nil {
			return dbterror.ErrDropIndexNeededInForeignKey.GenWithStackByArgs(droppingIdx.Name)
		}
	}
	return nil
}

func checkMultiSchemaInfo(info *model.MultiSchemaInfo, t table.Table) error {
	err := checkOperateSameColAndIdx(info)
	if err != nil {
		return err
	}

	err = checkVisibleColumnCnt(t, len(info.AddColumns), len(info.DropColumns))
	if err != nil {
		return err
	}

	err = checkOperateDropIndexUseByForeignKey(info, t)
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
		case model.JobStateNone, model.JobStateQueueing:
			sub.State = model.JobStateCancelled
		}
	}
	job.State = model.JobStateRollingback
	return dbterror.ErrCancelledDDLJob
}

func finishMultiSchemaJob(job *model.Job, t *meta.Mutator) (ver int64, err error) {
	for _, sub := range job.MultiSchemaInfo.SubJobs {
		if ver < sub.SchemaVer {
			ver = sub.SchemaVer
		}
	}
	tblInfo, err := t.GetTable(job.SchemaID, job.TableID)
	if err != nil {
		return 0, err
	}
	job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
	return 0, err
}
