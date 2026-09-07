// Copyright 2026 PingCAP, Inc.
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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"go.uber.org/zap"
)

const materializedViewInfoDeleteBatchSize = 1000

func (w *worker) onCreateMaterializedViewLog(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetCreateMaterializedViewLogArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	mlogTableInfo := args.TableInfo
	if mlogTableInfo == nil || mlogTableInfo.MaterializedViewLog == nil {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view log: invalid job args")
	}
	if job.IsRollingback() {
		return w.rollbackCreateMaterializedViewLog(jobCtx, job, mlogTableInfo)
	}
	baseTableID := mlogTableInfo.MaterializedViewLog.BaseTableID
	if baseTableID == 0 {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view log: invalid base table id")
	}
	baseTblInfo, err := getTableInfo(jobCtx.metaMut, baseTableID, job.SchemaID)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}
	if !isValidMaterializedViewLogBaseTable(strings.ToLower(job.SchemaName), baseTblInfo) {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrWrongObject.GenWithStackByArgs(job.SchemaName, baseTblInfo.Name, "BASE TABLE")
	}
	if baseTblInfo.GetPartitionInfo() != nil {
		job.State = model.JobStateCancelled
		return ver, errUnsupportedMaterializedViewOnPartitionTable("CREATE MATERIALIZED VIEW LOG")
	}
	if baseTblInfo.State != model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrInvalidDDLState.GenWithStack("table %s is not in public, but %s", baseTblInfo.Name, baseTblInfo.State)
	}
	if baseTblInfo.MaterializedViewBase != nil && baseTblInfo.MaterializedViewBase.MLogID != 0 {
		job.State = model.JobStateCancelled
		return ver, infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: ast.NewCIStr(job.SchemaName), Name: mlogTableInfo.Name})
	}

	mlogTableInfo.State = model.StateNone
	mlogTableInfo, err = createTable(w, jobCtx, job, jobCtx.getAutoIDRequirement(), &model.CreateTableArgs{TableInfo: mlogTableInfo, FKCheck: false})
	if err != nil {
		return ver, errors.Trace(err)
	}
	if baseTblInfo.MaterializedViewBase == nil {
		baseTblInfo.MaterializedViewBase = &model.MaterializedViewBaseInfo{}
	}
	baseTblInfo.MaterializedViewBase.MLogID = mlogTableInfo.ID
	if err = updateTable(jobCtx.metaMut, job.SchemaID, baseTblInfo, true); err != nil {
		return ver, errors.Trace(err)
	}
	if err = w.upsertCreateMaterializedViewLogPurgeInfo(jobCtx, job.SchemaName, mlogTableInfo); err != nil {
		if dbterror.ErrInvalidDDLJob.Equal(err) {
			job.State = model.JobStateRollingback
		}
		return ver, errors.Trace(err)
	}
	ver, err = updateSchemaVersion(jobCtx, job, schemaIDAndTableInfo{schemaID: job.SchemaID, tblInfo: baseTblInfo})
	if err != nil {
		return ver, errors.Trace(err)
	}
	if err = asyncNotifyEvent(jobCtx, notifier.NewCreateTableEvent(mlogTableInfo), job, noSubJob, w.sess); err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishMultipleTableJob(model.JobStateDone, model.StatePublic, ver, []*model.TableInfo{baseTblInfo, mlogTableInfo})
	return ver, nil
}

func (w *worker) rollbackCreateMaterializedViewLog(jobCtx *jobContext, job *model.Job, mlogTableInfo *model.TableInfo) (ver int64, _ error) {
	actualTblInfo, err := getTableInfo(jobCtx.metaMut, job.TableID, job.SchemaID)
	if err != nil && !infoschema.ErrDatabaseNotExists.Equal(err) && !infoschema.ErrTableNotExists.Equal(err) {
		return ver, errors.Trace(err)
	}

	droppingTblInfo := mlogTableInfo
	if actualTblInfo != nil {
		droppingTblInfo = actualTblInfo
	}
	extraInfos, err := updateMaterializedViewBaseInfoOnDrop(jobCtx, job, droppingTblInfo)
	if err != nil {
		return ver, errors.Trace(err)
	}
	for _, extra := range extraInfos {
		if err := updateTable(jobCtx.metaMut, extra.schemaID, extra.tblInfo, true); err != nil {
			return ver, errors.Trace(err)
		}
	}
	if actualTblInfo != nil {
		if err := jobCtx.metaMut.DropTableOrView(job.SchemaID, job.TableID); err != nil {
			return ver, errors.Trace(err)
		}
		if err := jobCtx.metaMut.GetAutoIDAccessors(job.SchemaID, job.TableID).Del(); err != nil {
			return ver, errors.Trace(err)
		}
	}
	if err := w.deleteMaterializedViewLogPurgeInfo(jobCtx, job.TableID); err != nil {
		return ver, errors.Trace(err)
	}

	job.State = model.JobStateRollbackDone
	job.SchemaState = model.StateNone
	ver, err = updateSchemaVersion(jobCtx, job, extraInfos...)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return ver, nil
}

func onCreateMaterializedViewBaseCheck(metaMut *meta.Mutator, schemaID, baseTableID int64, schemaName string) (*model.TableInfo, error) {
	baseTblInfo, err := getTableInfo(metaMut, baseTableID, schemaID)
	if err != nil {
		return nil, err
	}
	if baseTblInfo.IsView() || baseTblInfo.IsSequence() || baseTblInfo.TempTableType != model.TempTableNone {
		return nil, dbterror.ErrWrongObject.GenWithStackByArgs(schemaName, baseTblInfo.Name, "BASE TABLE")
	}
	if baseTblInfo.GetPartitionInfo() != nil {
		return nil, errUnsupportedMaterializedViewOnPartitionTable("CREATE MATERIALIZED VIEW")
	}
	if baseTblInfo.State != model.StatePublic {
		return nil, dbterror.ErrInvalidDDLState.GenWithStackByArgs("table", baseTblInfo.State)
	}
	if baseTblInfo.MaterializedViewBase == nil || baseTblInfo.MaterializedViewBase.MLogID == 0 {
		return nil, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: base table has no materialized view log")
	}
	mlogTableInfo, err := getTableInfo(metaMut, baseTblInfo.MaterializedViewBase.MLogID, schemaID)
	if err != nil {
		return nil, err
	}
	if mlogTableInfo.MaterializedViewLog == nil || mlogTableInfo.MaterializedViewLog.BaseTableID != baseTblInfo.ID {
		return nil, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: invalid materialized view log metadata")
	}
	if mlogTableInfo.State != model.StatePublic {
		return nil, dbterror.ErrInvalidDDLState.GenWithStackByArgs("table", mlogTableInfo.State)
	}
	return baseTblInfo, nil
}

func isCreateMaterializedViewBaseCheckCancelledErr(err error) bool {
	return infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err) ||
		dbterror.ErrInvalidDDLJob.Equal(err) || dbterror.ErrWrongObject.Equal(err) ||
		dbterror.ErrInvalidDDLState.Equal(err) || dbterror.ErrGeneralUnsupportedDDL.Equal(err)
}

func (w *worker) onCreateMaterializedView(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetCreateMaterializedViewArgs(job)
	if err != nil || args == nil {
		job.State = model.JobStateCancelled
		if err == nil {
			err = dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: invalid job args")
		}
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	mviewTableInfo := args.TableInfo
	if mviewTableInfo == nil || mviewTableInfo.MaterializedView == nil || len(mviewTableInfo.MaterializedView.BaseTableIDs) == 0 {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: invalid job args")
	}
	baseTableIDs := mviewTableInfo.MaterializedView.BaseTableIDs
	seenBaseTableIDs := make(map[int64]struct{}, len(baseTableIDs))
	for _, id := range baseTableIDs {
		if id == 0 {
			job.State = model.JobStateCancelled
			return ver, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: invalid base table id")
		}
		if _, ok := seenBaseTableIDs[id]; ok {
			job.State = model.JobStateCancelled
			return ver, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: duplicate base table id")
		}
		seenBaseTableIDs[id] = struct{}{}
	}
	if job.IsRollingback() {
		return w.rollbackCreateMaterializedView(jobCtx, job, mviewTableInfo)
	}

	switch job.SchemaState {
	case model.StateNone:
		for _, id := range baseTableIDs {
			if _, err := onCreateMaterializedViewBaseCheck(jobCtx.metaMut, job.SchemaID, id, job.SchemaName); err != nil {
				if isCreateMaterializedViewBaseCheckCancelledErr(err) {
					job.State = model.JobStateCancelled
				}
				return ver, errors.Trace(err)
			}
		}
		mviewTableInfo.State = model.StateNone
		mviewTableInfo, err = createTable(w, jobCtx, job, jobCtx.getAutoIDRequirement(), &model.CreateTableArgs{TableInfo: mviewTableInfo, FKCheck: false})
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.TableID = mviewTableInfo.ID
		extraInfos, err := updateMaterializedViewBaseInfoOnCreate(jobCtx, job, mviewTableInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		ver, err = updateSchemaVersion(jobCtx, job, extraInfos...)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if err = asyncNotifyEvent(jobCtx, notifier.NewCreateTableEvent(mviewTableInfo), job, noSubJob, w.sess); err != nil {
			return ver, errors.Trace(err)
		}
		if err = w.prewriteCreateMaterializedViewRefreshInfo(jobCtx, mviewTableInfo.ID); err != nil {
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteReorganization
		job.State = model.JobStateRunning
		return ver, nil

	case model.StateWriteReorganization:
		if w.getReorgCtx(job.ID) == nil {
			hasRows, checkErr := w.hasCreateMaterializedViewBuildRows(jobCtx.stepCtx, job.SchemaName, mviewTableInfo.Name.O)
			if checkErr != nil {
				job.State = model.JobStateRollingback
				return ver, errors.Trace(checkErr)
			}
			if hasRows {
				job.State = model.JobStateRollingback
				return ver, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: detected residual build rows on retry")
			}
		}
		reorg := &reorgInfo{Job: job, jobCtx: jobCtx}
		storeName := ""
		if jobCtx.store != nil {
			storeName = jobCtx.store.Name()
		}
		err = w.runReorgJob(jobCtx, reorg, mviewTableInfo, func() error {
			return w.buildCreateMaterializedViewData(jobCtx.stepCtx, storeName, job, mviewTableInfo)
		})
		if err != nil {
			if dbterror.ErrPausedDDLJob.Equal(err) || isCreateMaterializedViewPausedErr(jobCtx, err) || dbterror.ErrWaitReorgTimeout.Equal(err) {
				return ver, nil
			}
			if isCreateMaterializedViewCancelledErr(jobCtx, err) {
				job.State = model.JobStateRollingback
				return ver, nil
			}
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}
		failpoint.Inject("mockCreateMaterializedViewPostBuildRetryableErr", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(ver, dbterror.ErrWaitReorgTimeout)
			}
		})
		if job.SnapshotVer == 0 {
			job.State = model.JobStateRollingback
			return ver, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: invalid build read tso")
		}
		if err = w.upsertCreateMaterializedViewRefreshInfo(jobCtx, job.SchemaName, mviewTableInfo, job.SnapshotVer, job.SQLMode); err != nil {
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}
		failpoint.InjectCall("afterCreateMaterializedViewSuccessRefreshInfoUpsert")
		failpoint.Inject("mockCreateMaterializedViewPostBuildAfterRefreshInfoUpsertRetryableErr", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(ver, dbterror.ErrWaitReorgTimeout)
			}
		})
		mviewTableInfo.MaterializedView.InitBuildState = model.MViewInitBuildReady
		if err = updateTable(jobCtx.metaMut, job.SchemaID, mviewTableInfo, true); err != nil {
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}
		ver, err = updateSchemaVersion(jobCtx, job)
		if err != nil {
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}
		finished := make([]*model.TableInfo, 0, len(baseTableIDs)+1)
		for _, id := range baseTableIDs {
			base, getErr := getTableInfo(jobCtx.metaMut, id, job.SchemaID)
			if getErr != nil {
				return ver, errors.Trace(getErr)
			}
			finished = append(finished, base)
		}
		finished = append(finished, mviewTableInfo)
		job.FinishMultipleTableJob(model.JobStateDone, model.StatePublic, ver, finished)
		return ver, nil
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStack("invalid create materialized view schema state %s", job.SchemaState)
	}
}

func isCreateMaterializedViewCancelledErr(jobCtx *jobContext, err error) bool {
	if dbterror.ErrCancelledDDLJob.Equal(err) {
		return true
	}
	return errors.Cause(err) == context.Canceled && jobCtx.stepCtx != nil && dbterror.ErrCancelledDDLJob.Equal(context.Cause(jobCtx.stepCtx))
}

func isCreateMaterializedViewPausedErr(jobCtx *jobContext, err error) bool {
	if dbterror.ErrPausedDDLJob.Equal(err) {
		return true
	}
	return errors.Cause(err) == context.Canceled && jobCtx.stepCtx != nil && dbterror.ErrPausedDDLJob.Equal(context.Cause(jobCtx.stepCtx))
}

func (w *worker) rollbackCreateMaterializedView(jobCtx *jobContext, job *model.Job, mviewTableInfo *model.TableInfo) (ver int64, _ error) {
	droppingTblInfo := mviewTableInfo
	actualTblInfo, err := getTableInfo(jobCtx.metaMut, job.TableID, job.SchemaID)
	if err == nil {
		droppingTblInfo = actualTblInfo
	} else if !infoschema.ErrDatabaseNotExists.Equal(err) && !infoschema.ErrTableNotExists.Equal(err) {
		return ver, errors.Trace(err)
	}

	extraInfos, err := updateMaterializedViewBaseInfoOnDrop(jobCtx, job, droppingTblInfo)
	if err != nil {
		return ver, errors.Trace(err)
	}
	for _, extra := range extraInfos {
		if err := updateTable(jobCtx.metaMut, extra.schemaID, extra.tblInfo, true); err != nil {
			return ver, errors.Trace(err)
		}
	}
	if actualTblInfo != nil {
		if err := jobCtx.metaMut.DropTableOrView(job.SchemaID, job.TableID); err != nil {
			return ver, errors.Trace(err)
		}
		if err := jobCtx.metaMut.GetAutoIDAccessors(job.SchemaID, job.TableID).Del(); err != nil {
			return ver, errors.Trace(err)
		}
	}
	if err := w.deleteCreateMaterializedViewRefreshInfo(jobCtx, job.TableID); err != nil {
		return ver, errors.Trace(err)
	}
	if err := w.deleteCreateMaterializedViewRefreshAlert(jobCtx, job.TableID); err != nil {
		logutil.DDLLogger().Warn("create materialized view rollback: failed to delete refresh alert", zap.String("schemaName", job.SchemaName), zap.String("tableName", mviewTableInfo.Name.O), zap.Int64("mviewID", job.TableID), zap.Error(err))
	}
	job.State = model.JobStateRollbackDone
	job.SchemaState = model.StateNone
	ver, err = updateSchemaVersion(jobCtx, job, extraInfos...)
	if err != nil {
		return ver, errors.Trace(err)
	}
	var mlogTableIDs []int64
	if args, ok := jobCtx.jobArgs.(*model.CreateMaterializedViewArgs); ok && args != nil {
		mlogTableIDs = args.MLogTableIDs
	}
	job.FillArgs(&model.CreateMaterializedViewArgs{TableInfo: mviewTableInfo, MLogTableIDs: mlogTableIDs})
	return ver, nil
}

func updateMaterializedViewBaseInfoOnCreate(jobCtx *jobContext, job *model.Job, createdTable *model.TableInfo) ([]schemaIDAndTableInfo, error) {
	var baseTableIDs []int64
	var mlogTableIDs []int64
	var apply func(*model.TableInfo) error
	switch {
	case createdTable.MaterializedView != nil:
		if len(createdTable.MaterializedView.BaseTableIDs) == 0 {
			job.State = model.JobStateCancelled
			return nil, errors.New("materialized view must reference at least one base table")
		}
		baseTableIDs = createdTable.MaterializedView.BaseTableIDs
		if args, ok := jobCtx.jobArgs.(*model.CreateMaterializedViewArgs); ok && args != nil {
			mlogTableIDs = args.MLogTableIDs
		}
		apply = func(base *model.TableInfo) error {
			if base.MaterializedViewBase == nil {
				base.MaterializedViewBase = &model.MaterializedViewBaseInfo{}
			}
			for _, id := range base.MaterializedViewBase.MViewIDs {
				if id == createdTable.ID {
					return nil
				}
			}
			base.MaterializedViewBase.MViewIDs = append(base.MaterializedViewBase.MViewIDs, createdTable.ID)
			return nil
		}
	case createdTable.MaterializedViewLog != nil:
		baseTableIDs = []int64{createdTable.MaterializedViewLog.BaseTableID}
		apply = func(base *model.TableInfo) error {
			if base.MaterializedViewBase == nil {
				base.MaterializedViewBase = &model.MaterializedViewBaseInfo{}
			}
			if base.MaterializedViewBase.MLogID != 0 && base.MaterializedViewBase.MLogID != createdTable.ID {
				return errors.Errorf("base table %s already has a materialized view log", base.Name.O)
			}
			base.MaterializedViewBase.MLogID = createdTable.ID
			return nil
		}
	default:
		return nil, nil
	}
	extraInfos := make([]schemaIDAndTableInfo, 0, len(baseTableIDs)+len(mlogTableIDs))
	processed := make(map[int64]struct{}, len(baseTableIDs))
	for _, baseID := range baseTableIDs {
		if baseID == 0 {
			job.State = model.JobStateCancelled
			return nil, errors.New("materialized view base table id is invalid")
		}
		if _, ok := processed[baseID]; ok {
			continue
		}
		processed[baseID] = struct{}{}
		base, err := getTableInfo(jobCtx.metaMut, baseID, job.SchemaID)
		if err != nil {
			job.State = model.JobStateCancelled
			return nil, errors.Trace(err)
		}
		if err := apply(base); err != nil {
			job.State = model.JobStateCancelled
			return nil, errors.Trace(err)
		}
		if err := updateTable(jobCtx.metaMut, job.SchemaID, base, true); err != nil {
			job.State = model.JobStateCancelled
			return nil, errors.Trace(err)
		}
		extraInfos = append(extraInfos, schemaIDAndTableInfo{schemaID: job.SchemaID, tblInfo: base})
	}
	processedMLogs := make(map[int64]struct{}, len(mlogTableIDs))
	for _, mlogID := range mlogTableIDs {
		if mlogID == 0 {
			job.State = model.JobStateCancelled
			return nil, errors.New("materialized view log id is invalid")
		}
		if _, ok := processedMLogs[mlogID]; ok {
			continue
		}
		processedMLogs[mlogID] = struct{}{}
		mlog, err := getTableInfo(jobCtx.metaMut, mlogID, job.SchemaID)
		if err != nil {
			job.State = model.JobStateCancelled
			return nil, errors.Trace(err)
		}
		if mlog.MaterializedViewLog == nil {
			job.State = model.JobStateCancelled
			return nil, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: invalid materialized view log")
		}
		isBaseTable := false
		for _, baseID := range baseTableIDs {
			if mlog.MaterializedViewLog.BaseTableID == baseID {
				isBaseTable = true
				break
			}
		}
		if !isBaseTable {
			job.State = model.JobStateCancelled
			return nil, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: materialized view log does not belong to a base table")
		}
		dependent := false
		for _, mviewID := range mlog.MaterializedViewLog.DependentMViewIDs {
			if mviewID == createdTable.ID {
				dependent = true
				break
			}
		}
		if dependent {
			continue
		}
		mlog.MaterializedViewLog.DependentMViewIDs = append(mlog.MaterializedViewLog.DependentMViewIDs, createdTable.ID)
		if err := updateTable(jobCtx.metaMut, job.SchemaID, mlog, true); err != nil {
			job.State = model.JobStateCancelled
			return nil, errors.Trace(err)
		}
		extraInfos = append(extraInfos, schemaIDAndTableInfo{schemaID: job.SchemaID, tblInfo: mlog})
	}
	return extraInfos, nil
}

func buildCreateMaterializedViewImportSQL(schemaName string, mviewTableInfo *model.TableInfo, threadCnt int, diskQuota string) (string, error) {
	if mviewTableInfo.MaterializedView == nil || mviewTableInfo.MaterializedView.SQLContent == "" {
		return "", dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: invalid select sql")
	}
	prefix := sqlescape.MustEscapeSQL("IMPORT INTO %n.%n FROM ", schemaName, mviewTableInfo.Name.O)
	return prefix + "(" + mviewTableInfo.MaterializedView.SQLContent + ") WITH " + strings.Join(BuildMViewImportIntoOptions(threadCnt, diskQuota), ", "), nil
}

func getCreateMaterializedViewBuildReadTS(ctx context.Context, ddlSess *sess.Session) (uint64, error) {
	rows, err := ddlSess.Execute(ctx, "SELECT COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(@@tidb_last_query_info, '$.start_ts')) AS UNSIGNED), CAST(0 AS UNSIGNED))", "create-materialized-view-build-read-ts")
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(rows) == 0 {
		return 0, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: cannot fetch build read tso")
	}
	readTS := rows[0].GetUint64(0)
	if readTS == 0 {
		return 0, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: invalid build read tso")
	}
	return readTS, nil
}

func getCreateMaterializedViewTxnStartTS(ddlSess *sess.Session) (uint64, error) {
	if startTS := ddlSess.GetSessionVars().TxnCtx.StartTS; startTS != 0 {
		return startTS, nil
	}
	txn, err := ddlSess.Txn()
	if err != nil {
		return 0, errors.Trace(err)
	}
	if txn.StartTS() == 0 {
		return 0, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: invalid init refresh tso")
	}
	return txn.StartTS(), nil
}

func buildCreateMaterializedViewInsertSQL(schemaName string, mviewTableInfo *model.TableInfo) (string, error) {
	if mviewTableInfo.MaterializedView == nil || mviewTableInfo.MaterializedView.SQLContent == "" {
		return "", dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: invalid select sql")
	}
	return sqlescape.MustEscapeSQL("REPLACE INTO %n.%n ", schemaName, mviewTableInfo.Name.O) + mviewTableInfo.MaterializedView.SQLContent, nil
}

func (w *worker) hasCreateMaterializedViewBuildRows(ctx context.Context, schemaName, mvTableName string) (bool, error) {
	if ctx == nil {
		ctx = w.workCtx
	}
	vars := w.sess.GetSessionVars()
	original := vars.InMViewMaintenance
	vars.InMViewMaintenance = true
	defer func() { vars.InMViewMaintenance = original }()
	rows, err := w.sess.Execute(ctx, sqlescape.MustEscapeSQL("SELECT 1 FROM %n.%n LIMIT 1", schemaName, mvTableName), "create-materialized-view-check-build-rows")
	if err != nil {
		return false, errors.Trace(err)
	}
	return len(rows) > 0, nil
}

func initCreateMaterializedViewBuildSession(sessCtx sessionctx.Context, job *model.Job, mviewTableInfo *model.TableInfo, currentDB string) (func(), error) {
	if job == nil || job.ReorgMeta == nil {
		return nil, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: missing reorg metadata")
	}
	sessVars := sessCtx.GetSessionVars() //nolint:forbidigo
	restore := restoreSessCtx(sessCtx)
	originalMaintenance := sessVars.InMViewMaintenance
	originalDB := sessVars.CurrentDB
	if err := initSessCtx(sessCtx, job.ReorgMeta); err != nil {
		restore(sessCtx)
		return nil, errors.Trace(err)
	}
	target, err := MViewExecutionSessionVarsFromJob(job, sessVars)
	if err != nil {
		restore(sessCtx)
		return nil, errors.Trace(err)
	}
	restoreExecution, err := ApplyMViewExecutionSessionVars(sessVars, target)
	if err != nil {
		restore(sessCtx)
		return nil, errors.Trace(err)
	}
	if mviewTableInfo != nil && mviewTableInfo.MaterializedView != nil {
		sessVars.DivPrecisionIncrement = mviewTableInfo.MaterializedView.DefinitionDivPrecisionIncrement
	}
	sessVars.CurrentDB = currentDB
	sessVars.InMViewMaintenance = true
	failpoint.InjectCall("createMaterializedViewBuildMaintainMemQuotaApplied", sessVars.MemQuotaQuery)
	failpoint.InjectCall(
		"createMaterializedViewBuildTiFlashSessionVarsApplied",
		sessVars.TiFlashMaxThreads,
		sessVars.TiFlashFineGrainedShuffleStreamCount,
		sessVars.TiFlashFineGrainedShuffleBatchSize,
	)
	failpoint.InjectCall(
		"createMaterializedViewBuildTiFlashSpillSessionVarsApplied",
		sessVars.TiFlashMaxBytesBeforeExternalJoin,
		sessVars.TiFlashMaxBytesBeforeExternalGroupBy,
		sessVars.TiFlashMaxBytesBeforeExternalSort,
		sessVars.TiFlashMaxQueryMemoryPerNode,
		sessVars.TiFlashQuerySpillRatio,
	)
	failpoint.InjectCall(
		"createMaterializedViewBuildImportSessionVarsApplied",
		sessVars.MViewMaintainImportThreads,
		sessVars.MViewMaintainImportDiskQuota,
	)
	return func() {
		restoreExecution()
		restore(sessCtx)
		sessVars.InMViewMaintenance = originalMaintenance
		sessVars.CurrentDB = originalDB
	}, nil
}

func (w *worker) setCreateMaterializedViewBuildReadTSInReorgCtx(jobID int64, readTS uint64) error {
	rc := w.getReorgCtx(jobID)
	if rc == nil {
		return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: reorg context missing")
	}
	rc.setSnapshotVer(readTS)
	return nil
}

func (w *worker) buildCreateMaterializedViewDataByImport(ctx context.Context, job *model.Job, mviewTableInfo *model.TableInfo) error {
	sessCtx, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	restore, err := initCreateMaterializedViewBuildSession(sessCtx, job, mviewTableInfo, job.SchemaName)
	if err != nil {
		w.sessPool.Put(sessCtx)
		return errors.Trace(err)
	}
	defer func() { restore(); w.sessPool.Put(sessCtx) }()
	ddlSess := sess.NewSession(sessCtx)
	sessVars := sessCtx.GetSessionVars() //nolint:forbidigo
	buildSQL, err := buildCreateMaterializedViewImportSQL(job.SchemaName, mviewTableInfo, sessVars.MViewMaintainImportThreads, sessVars.MViewMaintainImportDiskQuota)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = ddlSess.Execute(ctx, buildSQL, "create-materialized-view-build-import"); err != nil {
		return errors.Trace(err)
	}
	readTS, err := getCreateMaterializedViewBuildReadTS(ctx, ddlSess)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(w.setCreateMaterializedViewBuildReadTSInReorgCtx(job.ID, readTS))
}

func (w *worker) buildCreateMaterializedViewDataByInsert(ctx context.Context, job *model.Job, mviewTableInfo *model.TableInfo) error {
	sessCtx, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	restore, err := initCreateMaterializedViewBuildSession(sessCtx, job, mviewTableInfo, job.SchemaName)
	if err != nil {
		w.sessPool.Put(sessCtx)
		return errors.Trace(err)
	}
	defer func() { restore(); w.sessPool.Put(sessCtx) }()
	buildSQL, err := buildCreateMaterializedViewInsertSQL(job.SchemaName, mviewTableInfo)
	if err != nil {
		return errors.Trace(err)
	}
	ddlSess := sess.NewSession(sessCtx)
	if _, err = ddlSess.Execute(ctx, buildSQL, "create-materialized-view-build-insert"); err != nil {
		return errors.Trace(err)
	}
	readTS, err := getCreateMaterializedViewBuildReadTS(ctx, ddlSess)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(w.setCreateMaterializedViewBuildReadTSInReorgCtx(job.ID, readTS))
}

func (w *worker) buildCreateMaterializedViewData(ctx context.Context, storeName string, job *model.Job, mviewTableInfo *model.TableInfo) error {
	if ctx == nil {
		ctx = w.workCtx
	}
	failpoint.Inject("pauseCreateMaterializedViewBuild", func() {})
	failpoint.Inject("mockCreateMaterializedViewBuildErr", func(val failpoint.Value) {
		if msg, ok := val.(string); ok && msg == "context-canceled" {
			failpoint.Return(context.Canceled)
		}
		failpoint.Return(errors.New("mock create materialized view build error"))
	})
	method := "insert-into"
	if storeName == "TiKV" {
		method = "import-into"
	}
	logutil.DDLLogger().Info("create materialized view: choose init build method", zap.Int64("jobID", job.ID), zap.String("schema", job.SchemaName), zap.String("mview", mviewTableInfo.Name.O), zap.String("storeName", storeName), zap.String("method", method))
	if storeName == "TiKV" {
		return w.buildCreateMaterializedViewDataByImport(ctx, job, mviewTableInfo)
	}
	return w.buildCreateMaterializedViewDataByInsert(ctx, job, mviewTableInfo)
}

func (w *worker) prewriteCreateMaterializedViewRefreshInfo(jobCtx *jobContext, mviewID int64) error {
	ctx := jobCtx.stepCtx
	if ctx == nil {
		ctx = w.workCtx
	}
	sessCtx, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.Put(sessCtx)
	ddlSess := sess.NewSession(sessCtx)
	if err = ddlSess.Begin(ctx); err != nil {
		return errors.Trace(err)
	}
	committed := false
	defer func() {
		if !committed {
			ddlSess.Rollback()
		}
	}()
	if err = warmupCreateMaterializedViewRefreshInfoTxn(ctx, ddlSess, mviewID); err != nil {
		return errors.Trace(err)
	}
	startTS, err := getCreateMaterializedViewTxnStartTS(ddlSess)
	if err != nil {
		return errors.Trace(err)
	}
	if err = execCreateMaterializedViewRefreshInfoUpsert(ctx, ddlSess, mviewID, startTS, nil, nil, false); err != nil {
		return errors.Trace(err)
	}
	if err = ddlSess.Commit(ctx); err != nil {
		return errors.Trace(err)
	}
	committed = true
	return nil
}

func warmupCreateMaterializedViewRefreshInfoTxn(ctx context.Context, ddlSess *sess.Session, mviewID int64) error {
	warmupSQL := sqlescape.MustEscapeSQL(
		"SELECT 1 FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? LIMIT 1",
		mviewID,
	)
	_, err := ddlSess.Execute(ctx, warmupSQL, "mview-refresh-info-prewrite-warmup")
	return errors.Trace(convertCreateMaterializedViewRefreshInfoTableNotExistsErr(err))
}

func (w *worker) upsertCreateMaterializedViewRefreshInfo(jobCtx *jobContext, mviewSchemaName string, mviewTableInfo *model.TableInfo, readTS uint64, sqlMode mysql.SQLMode) error {
	if mviewTableInfo == nil || mviewTableInfo.MaterializedView == nil {
		return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: invalid materialized view metadata")
	}
	ctx := jobCtx.stepCtx
	if ctx == nil {
		ctx = w.workCtx
	}
	evalSessCtx, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.Put(evalSessCtx)
	evalSess := sess.NewSession(evalSessCtx)
	scheduleTimeZone, err := mviewTableInfo.MaterializedView.RefreshScheduleTimeZone.GetLocation()
	if err != nil {
		return errors.Trace(err)
	}
	restore := setCreateMaterializedViewScheduleEvalSession(evalSessCtx, sqlMode, scheduleTimeZone)
	defer restore()
	next, shouldUpdate, err := deriveCreateMaterializedViewNextUnixSeconds(ctx, evalSess, mviewSchemaName, mviewTableInfo.Name.O, mviewTableInfo.MaterializedView)
	if err != nil {
		return errors.Trace(err)
	}
	lastSuccess := time.Now().Unix()
	return errors.Trace(execCreateMaterializedViewRefreshInfoUpsert(ctx, w.sess, mviewTableInfo.ID, readTS, &lastSuccess, next, shouldUpdate))
}

func (w *worker) upsertCreateMaterializedViewLogPurgeInfo(jobCtx *jobContext, mlogSchemaName string, mlogTableInfo *model.TableInfo) error {
	if mlogTableInfo == nil || mlogTableInfo.MaterializedViewLog == nil {
		return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view log: invalid materialized view log metadata")
	}
	ctx := jobCtx.stepCtx
	if ctx == nil {
		ctx = w.workCtx
	}
	evalSessCtx, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.Put(evalSessCtx)
	evalSess := sess.NewSession(evalSessCtx)
	info := mlogTableInfo.MaterializedViewLog
	tz, err := info.PurgeScheduleTimeZone.GetLocation()
	if err != nil {
		return errors.Trace(err)
	}
	restore := setCreateMaterializedViewScheduleEvalSession(evalSessCtx, info.DefinitionSQLMode, tz)
	defer restore()
	next, shouldUpdate, err := deriveCreateMaterializedViewLogNextUnixSeconds(ctx, evalSess, mlogSchemaName, mlogTableInfo.Name.O, info)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(execCreateMaterializedViewLogPurgeInfoUpsert(ctx, w.sess, mlogTableInfo.ID, next, shouldUpdate))
}

func buildCreateMaterializedViewLogPurgeInfoUpsertSQL(mlogID int64, nextPurgeUnixSeconds *int64, shouldUpdate bool) string {
	if shouldUpdate {
		var next any
		if nextPurgeUnixSeconds != nil {
			next = *nextPurgeUnixSeconds
		}
		return sqlescape.MustEscapeSQL(`INSERT INTO mysql.tidb_mlog_purge_info (MLOG_ID, NEXT_PURGE_UNIX_SECONDS)
VALUES (%?, %?) ON DUPLICATE KEY UPDATE NEXT_PURGE_UNIX_SECONDS = VALUES(NEXT_PURGE_UNIX_SECONDS)`, mlogID, next)
	}
	return sqlescape.MustEscapeSQL("INSERT IGNORE INTO mysql.tidb_mlog_purge_info (MLOG_ID) VALUES (%?)", mlogID)
}

func execCreateMaterializedViewLogPurgeInfoUpsert(ctx context.Context, ddlSess *sess.Session, mlogID int64, next *int64, shouldUpdate bool) error {
	_, err := ddlSess.Execute(ctx, buildCreateMaterializedViewLogPurgeInfoUpsertSQL(mlogID, next, shouldUpdate), "mlog-purge-info-upsert")
	failpoint.Inject("mockInsertMLogPurgeTableNotExists", func(val failpoint.Value) {
		if val.(bool) {
			err = infoschema.ErrTableNotExists.GenWithStackByArgs("mysql", "tidb_mlog_purge_info")
		}
	})
	return errors.Trace(convertCreateMaterializedViewLogPurgeInfoTableNotExistsErr(err))
}

func (w *worker) deleteMaterializedViewLogPurgeInfo(jobCtx *jobContext, mlogID int64) error {
	return w.deleteMaterializedViewLogPurgeInfos(jobCtx, []int64{mlogID})
}

func (w *worker) deleteMaterializedViewLogPurgeInfos(jobCtx *jobContext, mlogIDs []int64) error {
	if len(mlogIDs) == 0 {
		return nil
	}
	ctx := jobCtx.stepCtx
	if ctx == nil {
		ctx = w.workCtx
	}
	for start := 0; start < len(mlogIDs); start += materializedViewInfoDeleteBatchSize {
		end := min(start+materializedViewInfoDeleteBatchSize, len(mlogIDs))
		batch := mlogIDs[start:end]
		args := make([]any, len(batch))
		for i, id := range batch {
			args[i] = id
		}
		/* #nosec G202: only the placeholder count is dynamic; IDs are escaped by sqlescape. */
		_, err := w.sess.Execute(ctx,
			sqlescape.MustEscapeSQL("DELETE FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID IN ("+strings.Repeat("%?,", len(batch)-1)+"%?)", args...),
			"mlog-purge-info-delete")
		failpoint.Inject("mockDeleteMaterializedViewLogPurgeInfoTableNotExists", func(val failpoint.Value) {
			if val.(bool) {
				err = infoschema.ErrTableNotExists.GenWithStackByArgs("mysql", "tidb_mlog_purge_info")
			}
		})
		failpoint.Inject("mockDeleteMaterializedViewLogPurgeInfoErr", func(val failpoint.Value) {
			err = errors.New(val.(string))
		})
		if infoschema.ErrTableNotExists.Equal(err) {
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func execCreateMaterializedViewRefreshInfoUpsert(ctx context.Context, ddlSess *sess.Session, mviewID int64, readTS uint64, lastSuccess, next *int64, shouldUpdate bool) error {
	_, err := ddlSess.Execute(ctx, buildCreateMaterializedViewRefreshInfoUpsertSQL(mviewID, readTS, lastSuccess, next, shouldUpdate), "mview-refresh-info-upsert")
	failpoint.Inject("mockUpsertCreateMaterializedViewRefreshInfoTableNotExists", func(val failpoint.Value) {
		if val.(bool) {
			err = infoschema.ErrTableNotExists.GenWithStackByArgs("mysql", "tidb_mview_refresh_info")
		}
	})
	return errors.Trace(convertCreateMaterializedViewRefreshInfoTableNotExistsErr(err))
}

func convertCreateMaterializedViewRefreshInfoTableNotExistsErr(err error) error {
	if infoschema.ErrTableNotExists.Equal(err) {
		return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: required system table mysql.tidb_mview_refresh_info does not exist")
	}
	return err
}

func convertCreateMaterializedViewLogPurgeInfoTableNotExistsErr(err error) error {
	if infoschema.ErrTableNotExists.Equal(err) {
		return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view log: required system table mysql.tidb_mlog_purge_info does not exist")
	}
	return errors.Trace(err)
}

func buildCreateMaterializedViewRefreshInfoUpsertSQL(mviewID int64, readTS uint64, lastSuccess, next *int64, shouldUpdate bool) string {
	var last any
	if lastSuccess != nil {
		last = *lastSuccess
	}
	if shouldUpdate {
		var nextArg any
		if next != nil {
			nextArg = *next
		}
		return sqlescape.MustEscapeSQL(`INSERT INTO mysql.tidb_mview_refresh_info (MVIEW_ID, LAST_SUCCESS_READ_TSO, LAST_SUCCESS_REFRESH_END_UNIX_SECONDS, NEXT_REFRESH_UNIX_SECONDS)
VALUES (%?, %?, %?, %?) ON DUPLICATE KEY UPDATE LAST_SUCCESS_READ_TSO = VALUES(LAST_SUCCESS_READ_TSO), LAST_SUCCESS_REFRESH_END_UNIX_SECONDS = VALUES(LAST_SUCCESS_REFRESH_END_UNIX_SECONDS), NEXT_REFRESH_UNIX_SECONDS = VALUES(NEXT_REFRESH_UNIX_SECONDS)`, mviewID, readTS, last, nextArg)
	}
	return sqlescape.MustEscapeSQL(`INSERT INTO mysql.tidb_mview_refresh_info (MVIEW_ID, LAST_SUCCESS_READ_TSO, LAST_SUCCESS_REFRESH_END_UNIX_SECONDS)
VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE LAST_SUCCESS_READ_TSO = VALUES(LAST_SUCCESS_READ_TSO), LAST_SUCCESS_REFRESH_END_UNIX_SECONDS = VALUES(LAST_SUCCESS_REFRESH_END_UNIX_SECONDS)`, mviewID, readTS, last)
}

func (w *worker) deleteCreateMaterializedViewRefreshInfo(jobCtx *jobContext, mviewID int64) error {
	return w.deleteCreateMaterializedViewRefreshInfos(jobCtx, []int64{mviewID})
}

func (w *worker) deleteCreateMaterializedViewRefreshInfos(jobCtx *jobContext, mviewIDs []int64) error {
	if len(mviewIDs) == 0 {
		return nil
	}
	ctx := jobCtx.stepCtx
	if ctx == nil {
		ctx = w.workCtx
	}
	for start := 0; start < len(mviewIDs); start += materializedViewInfoDeleteBatchSize {
		end := min(start+materializedViewInfoDeleteBatchSize, len(mviewIDs))
		batch := mviewIDs[start:end]
		args := make([]any, len(batch))
		for i, id := range batch {
			args[i] = id
		}
		/* #nosec G202: only the placeholder count is dynamic; IDs are escaped by sqlescape. */
		_, err := w.sess.Execute(ctx,
			sqlescape.MustEscapeSQL("DELETE FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN ("+strings.Repeat("%?,", len(batch)-1)+"%?)", args...),
			"mview-refresh-info-delete")
		failpoint.Inject("mockDeleteCreateMaterializedViewRefreshInfoTableNotExists", func(val failpoint.Value) {
			if val.(bool) {
				err = infoschema.ErrTableNotExists.GenWithStackByArgs("mysql", "tidb_mview_refresh_info")
			}
		})
		failpoint.Inject("mockDeleteCreateMaterializedViewRefreshInfoErr", func(val failpoint.Value) {
			err = errors.New(val.(string))
		})
		if infoschema.ErrTableNotExists.Equal(err) {
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (w *worker) deleteCreateMaterializedViewRefreshAlert(jobCtx *jobContext, mviewID int64) error {
	return w.deleteCreateMaterializedViewRefreshAlerts(jobCtx, []int64{mviewID})
}

func (w *worker) deleteCreateMaterializedViewRefreshAlerts(jobCtx *jobContext, mviewIDs []int64) error {
	if len(mviewIDs) == 0 {
		return nil
	}
	ctx := jobCtx.stepCtx
	if ctx == nil {
		ctx = w.workCtx
	}
	for start := 0; start < len(mviewIDs); start += materializedViewInfoDeleteBatchSize {
		end := min(start+materializedViewInfoDeleteBatchSize, len(mviewIDs))
		batch := mviewIDs[start:end]
		args := make([]any, len(batch))
		for i, id := range batch {
			args[i] = id
		}
		var err error
		failpoint.Inject("mockDeleteCreateMaterializedViewRefreshAlertErr", func(val failpoint.Value) {
			err = errors.New(val.(string))
		})
		if err == nil {
			/* #nosec G202: only the placeholder count is dynamic; IDs are escaped by sqlescape. */
			_, err = w.sess.Execute(ctx,
				sqlescape.MustEscapeSQL("DELETE FROM mysql.tidb_mview_refresh_alert WHERE MVIEW_ID IN ("+strings.Repeat("%?,", len(batch)-1)+"%?)", args...),
				"mview-refresh-alert-delete")
		}
		if infoschema.ErrTableNotExists.Equal(err) {
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func hasMaterializedViewDependsOnBaseTable(baseTableInfo *model.TableInfo) bool {
	return baseTableInfo.MaterializedViewBase != nil && len(baseTableInfo.MaterializedViewBase.MViewIDs) > 0
}

func hasMaterializedViewDependsOnMaterializedViewLog(mlogTableInfo *model.TableInfo) bool {
	return mlogTableInfo.MaterializedViewLog != nil && len(mlogTableInfo.MaterializedViewLog.DependentMViewIDs) > 0
}

func hasMaterializedViewID(ids []int64, mviewID int64) bool {
	for _, id := range ids {
		if id == mviewID {
			return true
		}
	}
	return false
}

func removeMaterializedViewID(ids []int64, mviewID int64) ([]int64, bool) {
	removed := false
	filtered := ids[:0]
	for _, id := range ids {
		if id == mviewID {
			removed = true
			continue
		}
		filtered = append(filtered, id)
	}
	return filtered, removed
}

func errDropMaterializedViewLogDependent(schemaName, baseTableName string) error {
	return errors.Errorf("cannot drop materialized view log on %s.%s: dependent materialized views exist", schemaName, baseTableName)
}

func checkDropMaterializedViewLogHasNoDependentMVs(jobCtx *jobContext, job *model.Job, droppingTable *model.TableInfo) error {
	if droppingTable.MaterializedViewLog == nil {
		return nil
	}
	if !hasMaterializedViewDependsOnMaterializedViewLog(droppingTable) {
		return nil
	}
	baseTableID := droppingTable.MaterializedViewLog.BaseTableID
	baseTblInfo, err := getTableInfo(jobCtx.metaMut, baseTableID, job.SchemaID)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err) {
			return nil
		}
		return errors.Trace(err)
	}
	job.State = model.JobStateCancelled
	return errDropMaterializedViewLogDependent(job.SchemaName, baseTblInfo.Name.O)
}

func updateMaterializedViewBaseInfoOnDrop(jobCtx *jobContext, job *model.Job, droppingTable *model.TableInfo) ([]schemaIDAndTableInfo, error) {
	var baseTableIDs []int64
	var apply func(*model.TableInfo)
	switch {
	case droppingTable.MaterializedView != nil:
		if len(droppingTable.MaterializedView.BaseTableIDs) == 0 {
			logutil.DDLLogger().Warn(
				"materialized view has no base tables in metadata, skip dependency cleanup when dropping",
				zap.Int64("mviewID", droppingTable.ID),
			)
			return nil, nil
		}
		baseTableIDs = droppingTable.MaterializedView.BaseTableIDs
		apply = func(base *model.TableInfo) {
			if base.MaterializedViewBase == nil {
				return
			}
			newIDs := base.MaterializedViewBase.MViewIDs[:0]
			for _, id := range base.MaterializedViewBase.MViewIDs {
				if id != job.TableID {
					newIDs = append(newIDs, id)
				}
			}
			base.MaterializedViewBase.MViewIDs = newIDs
			if base.MaterializedViewBase.MLogID == 0 && len(newIDs) == 0 {
				base.MaterializedViewBase = nil
			}
		}
	case droppingTable.MaterializedViewLog != nil:
		baseTableIDs = []int64{droppingTable.MaterializedViewLog.BaseTableID}
		apply = func(base *model.TableInfo) {
			if base.MaterializedViewBase == nil {
				return
			}
			if base.MaterializedViewBase.MLogID == job.TableID {
				base.MaterializedViewBase.MLogID = 0
			}
			if base.MaterializedViewBase.MLogID == 0 && len(base.MaterializedViewBase.MViewIDs) == 0 {
				base.MaterializedViewBase = nil
			}
		}
	default:
		return nil, nil
	}
	extraInfos := make([]schemaIDAndTableInfo, 0, len(baseTableIDs))
	processed := make(map[int64]struct{}, len(baseTableIDs))
	for _, baseID := range baseTableIDs {
		if _, ok := processed[baseID]; ok {
			continue
		}
		processed[baseID] = struct{}{}
		base, err := jobCtx.metaMut.GetTable(job.SchemaID, baseID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if base == nil {
			continue
		}
		var mlogID int64
		if droppingTable.MaterializedView != nil && base.MaterializedViewBase != nil {
			mlogID = base.MaterializedViewBase.MLogID
		}
		apply(base)
		extraInfos = append(extraInfos, schemaIDAndTableInfo{schemaID: job.SchemaID, tblInfo: base})
		if mlogID == 0 {
			continue
		}
		mlog, err := jobCtx.metaMut.GetTable(job.SchemaID, mlogID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if mlog == nil || mlog.MaterializedViewLog == nil {
			// The executor rejects this corrupted metadata before submitting the job.
			// Do not leave an already-started DROP job retrying in delete-only state.
			logutil.DDLLogger().Error(
				"drop materialized view: materialized view log is missing or invalid during dependency cleanup",
				zap.Int64("mviewID", job.TableID),
				zap.Int64("baseTableID", baseID),
				zap.Int64("mlogID", mlogID),
			)
			continue
		}
		if mlog.MaterializedViewLog.BaseTableID != baseID {
			// See the missing-MLog branch above. This is a permanent metadata error,
			// not a retryable DDL failure after the DROP job has started.
			logutil.DDLLogger().Error(
				"drop materialized view: materialized view log belongs to a different base table during dependency cleanup",
				zap.Int64("mviewID", job.TableID),
				zap.Int64("baseTableID", baseID),
				zap.Int64("mlogID", mlogID),
				zap.Int64("mlogBaseTableID", mlog.MaterializedViewLog.BaseTableID),
			)
			continue
		}
		if !hasMaterializedViewID(mlog.MaterializedViewLog.DependentMViewIDs, job.TableID) {
			continue
		}
		var removed bool
		mlog.MaterializedViewLog.DependentMViewIDs, removed = removeMaterializedViewID(mlog.MaterializedViewLog.DependentMViewIDs, job.TableID)
		if removed {
			extraInfos = append(extraInfos, schemaIDAndTableInfo{schemaID: job.SchemaID, tblInfo: mlog})
		}
	}
	return extraInfos, nil
}
