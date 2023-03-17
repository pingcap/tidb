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
	"github.com/pingcap/errors"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/dbterror"
)

func (d *ddl) MultiSchemaChange(ctx sessionctx.Context, ti ast.Ident) error {
	if len(ctx.GetSessionVars().StmtCtx.MultiSchemaInfo.SubJobs) == 0 {
		return nil
	}
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}
	tzName, tzOffset := ddlutil.GetTimeZone(ctx)
	job := &model.Job{
		SchemaID:        schema.ID,
		TableID:         t.Meta().ID,
		SchemaName:      schema.Name.L,
		TableName:       t.Meta().Name.L,
		Type:            model.ActionMultiSchemaChange,
		BinlogInfo:      &model.HistoryInfo{},
		Args:            nil,
		MultiSchemaInfo: ctx.GetSessionVars().StmtCtx.MultiSchemaInfo,
		ReorgMeta: &model.DDLReorgMeta{
			SQLMode:       ctx.GetSessionVars().SQLMode,
			Warnings:      make(map[errors.ErrorID]*terror.Error),
			WarningsCount: make(map[errors.ErrorID]int64),
			Location:      &model.TimeZoneLocation{Name: tzName, Offset: tzOffset},
		},
	}
	err = checkMultiSchemaInfo(ctx.GetSessionVars().StmtCtx.MultiSchemaInfo, t)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.GetSessionVars().StmtCtx.MultiSchemaInfo = nil
	err = d.DoDDLJob(ctx, job)
	return d.callHookOnChanged(job, err)
}

func onMultiSchemaChange(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	if job.MultiSchemaInfo.Revertible {
		// Handle the rolling back job.
		if job.IsRollingback() {
			// Rollback/cancel the sub-jobs in reverse order.
			for i := len(job.MultiSchemaInfo.SubJobs) - 1; i >= 0; i-- {
				sub := job.MultiSchemaInfo.SubJobs[i]
				if sub.IsFinished() {
					continue
				}
				proxyJob := sub.ToProxyJob(job)
				ver, err = w.runDDLJob(d, t, &proxyJob)
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
		for _, sub := range job.MultiSchemaInfo.SubJobs {
			if !sub.Revertible || sub.IsFinished() {
				// Skip the sub-jobs which related schema states
				// are in the last revertible point.
				// If a sub job is finished here, it should be a noop job.
				continue
			}
			proxyJob := sub.ToProxyJob(job)
			ver, err = w.runDDLJob(d, t, &proxyJob)
			sub.FromProxyJob(&proxyJob, ver)
			handleRevertibleException(job, sub, proxyJob.Error)
			return ver, err
		}

		// Save table info and sub-jobs for rolling back.
		var tblInfo *model.TableInfo
		tblInfo, err = t.GetTable(job.SchemaID, job.TableID)
		if err != nil {
			return ver, err
		}
		var schemaVersionGenerated = false
		subJobs := make([]model.SubJob, len(job.MultiSchemaInfo.SubJobs))
		// Step the sub-jobs to the non-revertible states all at once.
		// We only generate 1 schema version for these sub-job.
		for i, sub := range job.MultiSchemaInfo.SubJobs {
			if sub.IsFinished() {
				continue
			}
			subJobs[i] = *sub
			proxyJob := sub.ToProxyJob(job)
			if schemaVersionGenerated {
				proxyJob.MultiSchemaInfo.SkipVersion = true
			}
			proxyJobVer, err := w.runDDLJob(d, t, &proxyJob)
			if !schemaVersionGenerated && proxyJobVer != 0 {
				schemaVersionGenerated = true
				ver = proxyJobVer
			}
			sub.FromProxyJob(&proxyJob, proxyJobVer)
			if err != nil || proxyJob.Error != nil {
				for j := i - 1; j >= 0; j-- {
					job.MultiSchemaInfo.SubJobs[j] = &subJobs[j]
				}
				handleRevertibleException(job, sub, proxyJob.Error)
				// The TableInfo and sub-jobs should be restored
				// because some schema changes update the transaction aggressively.
				return updateVersionAndTableInfo(d, t, job, tblInfo, true)
			}
		}
		// All the sub-jobs are non-revertible.
		job.MarkNonRevertible()
		return ver, err
	}
	// Run the rest non-revertible sub-jobs one by one.
	for _, sub := range job.MultiSchemaInfo.SubJobs {
		if sub.IsFinished() {
			continue
		}
		proxyJob := sub.ToProxyJob(job)
		ver, err = w.runDDLJob(d, t, &proxyJob)
		sub.FromProxyJob(&proxyJob, ver)
		return ver, err
	}
	return finishMultiSchemaJob(job, t)
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

func appendToSubJobs(m *model.MultiSchemaInfo, job *model.Job) error {
	err := fillMultiSchemaInfo(m, job)
	if err != nil {
		return err
	}
	var reorgTp model.ReorgType
	if job.ReorgMeta != nil {
		reorgTp = job.ReorgMeta.ReorgTp
	}
	m.SubJobs = append(m.SubJobs, &model.SubJob{
		Type:        job.Type,
		Args:        job.Args,
		RawArgs:     job.RawArgs,
		SchemaState: job.SchemaState,
		SnapshotVer: job.SnapshotVer,
		Revertible:  true,
		CtxVars:     job.CtxVars,
		ReorgTp:     reorgTp,
	})
	return nil
}

func fillMultiSchemaInfo(info *model.MultiSchemaInfo, job *model.Job) (err error) {
	switch job.Type {
	case model.ActionAddColumn:
		col := job.Args[0].(*table.Column)
		pos := job.Args[1].(*ast.ColumnPosition)
		info.AddColumns = append(info.AddColumns, col.Name)
		for colName := range col.Dependences {
			info.RelativeColumns = append(info.RelativeColumns, model.CIStr{L: colName, O: colName})
		}
		if pos != nil && pos.Tp == ast.ColumnPositionAfter {
			info.PositionColumns = append(info.PositionColumns, pos.RelativeColumn.Name)
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
		if hiddenCols, ok := job.Args[4].([]*model.ColumnInfo); ok {
			for _, c := range hiddenCols {
				for depColName := range c.Dependences {
					info.RelativeColumns = append(info.RelativeColumns, model.NewCIStr(depColName))
				}
			}
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
			info.ModifyColumns = append(info.ModifyColumns, newCol.Name)
		}
		if pos != nil && pos.Tp == ast.ColumnPositionAfter {
			info.PositionColumns = append(info.PositionColumns, pos.RelativeColumn.Name)
		}
	case model.ActionSetDefaultValue:
		col := job.Args[0].(*table.Column)
		info.ModifyColumns = append(info.ModifyColumns, col.Name)
	case model.ActionAlterIndexVisibility:
		idxName := job.Args[0].(model.CIStr)
		info.AlterIndexes = append(info.AlterIndexes, idxName)
	case model.ActionRebaseAutoID, model.ActionModifyTableComment, model.ActionModifyTableCharsetAndCollate:
	case model.ActionAddForeignKey:
		fkInfo := job.Args[0].(*model.FKInfo)
		info.AddForeignKeys = append(info.AddForeignKeys, model.AddForeignKeyInfo{
			Name: fkInfo.Name,
			Cols: fkInfo.Cols,
		})
	default:
		return dbterror.ErrRunMultiSchemaChanges.FastGenByArgs(job.Type.String())
	}
	return nil
}

func checkOperateSameColAndIdx(info *model.MultiSchemaInfo) error {
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

func finishMultiSchemaJob(job *model.Job, t *meta.Meta) (ver int64, err error) {
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
