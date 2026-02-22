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
	"os"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

const (
	// MaxCommentLength is exported for testing.
	MaxCommentLength = 1024
)

var telemetryAddIndexIngestUsage = metrics.TelemetryAddIndexIngestCnt

// DefaultCumulativeTimeout is the default cumulative timeout for analyze operation.
// exported for testing.
var DefaultCumulativeTimeout = 1 * time.Minute

// DefaultAnalyzeCheckInterval is the interval for checking analyze status.
// exported for testing.
var DefaultAnalyzeCheckInterval = 10 * time.Second

// AddIndexColumnFlag aligns the column flags of columns in TableInfo to IndexInfo.
func AddIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	if indexInfo.Primary {
		for _, col := range indexInfo.Columns {
			tblInfo.Columns[col.Offset].AddFlag(mysql.PriKeyFlag)
		}
		return
	}

	col := indexInfo.Columns[0]
	if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[col.Offset].AddFlag(mysql.UniqueKeyFlag)
	} else {
		tblInfo.Columns[col.Offset].AddFlag(mysql.MultipleKeyFlag)
	}
}

// DropIndexColumnFlag drops the column flag of columns in TableInfo according to the IndexInfo.
func DropIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	if indexInfo.Primary {
		for _, col := range indexInfo.Columns {
			tblInfo.Columns[col.Offset].DelFlag(mysql.PriKeyFlag)
		}
	} else if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[indexInfo.Columns[0].Offset].DelFlag(mysql.UniqueKeyFlag)
	} else {
		tblInfo.Columns[indexInfo.Columns[0].Offset].DelFlag(mysql.MultipleKeyFlag)
	}

	col := indexInfo.Columns[0]
	// other index may still cover this col
	for _, index := range tblInfo.Indices {
		if index.Name.L == indexInfo.Name.L {
			continue
		}

		if index.Columns[0].Name.L != col.Name.L {
			continue
		}

		AddIndexColumnFlag(tblInfo, index)
	}
}

// ValidateRenameIndex checks if index name is ok to be renamed.
func ValidateRenameIndex(from, to ast.CIStr, tbl *model.TableInfo) (ignore bool, err error) {
	if fromIdx := tbl.FindIndexByName(from.L); fromIdx == nil {
		return false, errors.Trace(infoschema.ErrKeyNotExists.GenWithStackByArgs(from.O, tbl.Name))
	}
	// Take case-sensitivity into account, if `FromKey` and  `ToKey` are the same, nothing need to be changed
	if from.O == to.O {
		return true, nil
	}
	// If spec.FromKey.L == spec.ToKey.L, we operate on the same index(case-insensitive) and change its name (case-sensitive)
	// e.g: from `inDex` to `IndEX`. Otherwise, we try to rename an index to another different index which already exists,
	// that's illegal by rule.
	if toIdx := tbl.FindIndexByName(to.L); toIdx != nil && from.L != to.L {
		return false, errors.Trace(infoschema.ErrKeyNameDuplicate.GenWithStackByArgs(toIdx.Name.O))
	}
	return false, nil
}

func onRenameIndex(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	tblInfo, from, to, err := checkRenameIndex(jobCtx.metaMut, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Rename Index"))
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		// Store the mark and enter the next DDL handling loop.
		return updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, false)
	}

	renameIndexes(tblInfo, from, to)
	renameHiddenColumns(tblInfo, from, to)

	if ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func validateAlterIndexVisibility(ctx sessionctx.Context, indexName ast.CIStr, invisible bool, tbl *model.TableInfo) (bool, error) {
	var idx *model.IndexInfo
	if idx = tbl.FindIndexByName(indexName.L); idx == nil || idx.State != model.StatePublic {
		return false, errors.Trace(infoschema.ErrKeyNotExists.GenWithStackByArgs(indexName.O, tbl.Name))
	}
	if ctx == nil || ctx.GetSessionVars() == nil || ctx.GetSessionVars().StmtCtx.MultiSchemaInfo == nil {
		// Early return.
		if idx.Invisible == invisible {
			return true, nil
		}
	}
	if invisible && idx.IsColumnarIndex() {
		return false, dbterror.ErrUnsupportedIndexType.FastGen("INVISIBLE can not be used in %s INDEX", idx.Tp)
	}
	return false, nil
}

func onAlterIndexVisibility(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	tblInfo, from, invisible, err := checkAlterIndexVisibility(jobCtx.metaMut, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, false)
	}

	setIndexVisibility(tblInfo, from, invisible)
	if ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func setIndexVisibility(tblInfo *model.TableInfo, name ast.CIStr, invisible bool) {
	for _, idx := range tblInfo.Indices {
		if idx.Name.L == name.L || idx.GetChangingOriginName() == name.O {
			idx.Invisible = invisible
		}
	}
}

func getNullColInfos(tblInfo *model.TableInfo, cols []*model.IndexColumn) []*model.ColumnInfo {
	nullCols := make([]*model.ColumnInfo, 0, len(cols))
	for _, colName := range cols {
		col := model.FindColumnInfo(tblInfo.Columns, colName.Name.L)
		if !mysql.HasNotNullFlag(col.GetFlag()) || mysql.HasPreventNullInsertFlag(col.GetFlag()) {
			nullCols = append(nullCols, col)
		}
	}
	return nullCols
}

func checkPrimaryKeyNotNull(jobCtx *jobContext, w *worker, job *model.Job,
	tblInfo *model.TableInfo, indexInfo *model.IndexInfo) (warnings []string, err error) {
	if !indexInfo.Primary {
		return nil, nil
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(jobCtx.metaMut, job)
	if err != nil {
		return nil, err
	}
	nullCols := getNullColInfos(tblInfo, indexInfo.Columns)
	if len(nullCols) == 0 {
		return nil, nil
	}

	err = modifyColsFromNull2NotNull(
		jobCtx.stepCtx,
		w,
		dbInfo,
		tblInfo,
		nullCols,
		&model.ColumnInfo{Name: ast.NewCIStr("")},
		false,
	)
	if err == nil {
		return nil, nil
	}
	_, err = convertAddIdxJob2RollbackJob(jobCtx, job, tblInfo, []*model.IndexInfo{indexInfo}, err)
	// TODO: Support non-strict mode.
	// warnings = append(warnings, ErrWarnDataTruncated.GenWithStackByArgs(oldCol.Name.L, 0).Error())
	return nil, err
}

// moveAndUpdateHiddenColumnsToPublic updates the hidden columns to public, and
// moves the hidden columns to proper offsets, so that Table.Columns' states meet the assumption of
// [public, public, ..., public, non-public, non-public, ..., non-public].
func moveAndUpdateHiddenColumnsToPublic(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) {
	hiddenColOffset := make(map[int]struct{}, 0)
	for _, col := range idxInfo.Columns {
		if tblInfo.Columns[col.Offset].Hidden {
			hiddenColOffset[col.Offset] = struct{}{}
		}
	}
	if len(hiddenColOffset) == 0 {
		return
	}
	// Find the first non-public column.
	firstNonPublicPos := len(tblInfo.Columns) - 1
	for i, c := range tblInfo.Columns {
		if c.State != model.StatePublic {
			firstNonPublicPos = i
			break
		}
	}
	for _, col := range idxInfo.Columns {
		tblInfo.Columns[col.Offset].State = model.StatePublic
		if _, needMove := hiddenColOffset[col.Offset]; needMove {
			tblInfo.MoveColumnInfo(col.Offset, firstNonPublicPos)
		}
	}
}

func checkAndBuildIndexInfo(
	job *model.Job, tblInfo *model.TableInfo,
	columnarIndexType model.ColumnarIndexType, isPK bool, args *model.IndexArg,
) (*model.IndexInfo, error) {
	var err error
	indexInfo := tblInfo.FindIndexByName(args.IndexName.L)
	if indexInfo != nil {
		if indexInfo.State == model.StatePublic {
			err = dbterror.ErrDupKeyName.GenWithStack("index already exist %s", args.IndexName)
			if isPK {
				err = infoschema.ErrMultiplePriKey
			}
			return nil, err
		}
		return indexInfo, nil
	}

	for _, hiddenCol := range args.HiddenCols {
		columnInfo := model.FindColumnInfo(tblInfo.Columns, hiddenCol.Name.L)
		if columnInfo != nil && columnInfo.State == model.StatePublic {
			// We already have a column with the same column name.
			// TODO: refine the error message
			return nil, infoschema.ErrColumnExists.GenWithStackByArgs(hiddenCol.Name)
		}
	}

	if len(args.HiddenCols) > 0 {
		for _, hiddenCol := range args.HiddenCols {
			InitAndAddColumnToTable(tblInfo, hiddenCol)
		}
	}
	if err = checkAddColumnTooManyColumns(len(tblInfo.Columns)); err != nil {
		return nil, errors.Trace(err)
	}
	indexInfo, err = BuildIndexInfo(
		nil,
		tblInfo,
		args.IndexName,
		isPK,
		args.Unique,
		columnarIndexType,
		args.IndexPartSpecifications,
		args.IndexOption,
		model.StateNone,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if isPK {
		if _, err = CheckPKOnGeneratedColumn(tblInfo, args.IndexPartSpecifications); err != nil {
			return nil, err
		}
	}
	indexInfo.ID = AllocateIndexID(tblInfo)
	tblInfo.Indices = append(tblInfo.Indices, indexInfo)
	if err = checkTooManyIndexes(tblInfo.Indices); err != nil {
		return nil, errors.Trace(err)
	}
	// Here we need do this check before set state to `DeleteOnly`,
	// because if hidden columns has been set to `DeleteOnly`,
	// the `DeleteOnly` columns are missing when we do this check.
	if err := checkInvisibleIndexOnPK(tblInfo); err != nil {
		return nil, err
	}
	logutil.DDLLogger().Info("[ddl] run add index job", zap.String("job", job.String()), zap.Reflect("indexInfo", indexInfo))
	return indexInfo, nil
}

func (w *worker) onCreateIndex(jobCtx *jobContext, job *model.Job, isPK bool) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropIndex(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// Handle normal job.
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Create Index"))
	}

	args, err := model.GetModifyIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	allIndexInfos := make([]*model.IndexInfo, 0, len(args.IndexArgs))
	for _, arg := range args.IndexArgs {
		indexInfo, err := checkAndBuildIndexInfo(job, tblInfo, model.ColumnarIndexTypeNA, job.Type == model.ActionAddPrimaryKey, arg)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// The condition in the index option is not marshaled, so we need to set it here.
		if len(arg.ConditionString) > 0 {
			indexInfo.ConditionExprString = arg.ConditionString
			// As we've updated the `ConditionExprString`, we need to rebuild the AffectColumn.
			indexInfo.AffectColumn, err = buildAffectColumn(indexInfo, tblInfo)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
		}
		allIndexInfos = append(allIndexInfos, indexInfo)
	}

	originalState := allIndexInfos[0].State

SwitchIndexState:
	switch allIndexInfos[0].State {
	case model.StateNone:
		// none -> delete only
		err = initForReorgIndexes(w, job, allIndexInfos)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
		err = preSplitIndexRegions(jobCtx.stepCtx, w.sess.Context, jobCtx.store, tblInfo, allIndexInfos, job.ReorgMeta, args)
		if err != nil {
			if !isRetryableJobError(err, job.ErrorCount) {
				job.State = model.JobStateCancelled
			}
			return ver, err
		}
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateDeleteOnly
			moveAndUpdateHiddenColumnsToPublic(tblInfo, indexInfo)
		}
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != model.StateDeleteOnly)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> write only
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateWriteOnly
			_, err = checkPrimaryKeyNotNull(jobCtx, w, job, tblInfo, indexInfo)
			if err != nil {
				break SwitchIndexState
			}
		}

		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateWriteOnly)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateWriteReorganization
			_, err = checkPrimaryKeyNotNull(jobCtx, w, job, tblInfo, indexInfo)
			if err != nil {
				break SwitchIndexState
			}
		}

		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateWriteReorganization)
		if err != nil {
			return ver, err
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		// reorganization -> public
		tbl, err := getTable(jobCtx.getAutoIDRequirement(), schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		switch job.ReorgMeta.AnalyzeState {
		case model.AnalyzeStateNone:
			// reorg the index data.
			var done bool
			done, ver, err = doReorgWorkForCreateIndex(w, jobCtx, job, tbl, allIndexInfos)
			if !done {
				return ver, err
			}
			// For multi-schema change, analyze is done by parent job.
			if job.MultiSchemaInfo == nil && checkNeedAnalyze(job, tblInfo) {
				job.ReorgMeta.AnalyzeState = model.AnalyzeStateRunning
			} else {
				job.ReorgMeta.AnalyzeState = model.AnalyzeStateSkipped
				checkAndMarkNonRevertible(job)
			}
		case model.AnalyzeStateRunning:
			intest.Assert(job.MultiSchemaInfo == nil, "multi schema change shouldn't reach here")
			w.startAnalyzeAndWait(job, tblInfo)
		case model.AnalyzeStateDone, model.AnalyzeStateSkipped, model.AnalyzeStateTimeout, model.AnalyzeStateFailed:
			// Set column index flag.
			for _, indexInfo := range allIndexInfos {
				AddIndexColumnFlag(tblInfo, indexInfo)
				if isPK {
					if err = UpdateColsNull2NotNull(tblInfo, indexInfo); err != nil {
						return ver, errors.Trace(err)
					}
				}
				indexInfo.State = model.StatePublic
			}

			// Inject the failpoint to prevent the progress of index creation.
			failpoint.Inject("create-index-stuck-before-public", func(v failpoint.Value) {
				if sigFile, ok := v.(string); ok {
					for {
						time.Sleep(1 * time.Second)
						if _, err := os.Stat(sigFile); err != nil {
							if os.IsNotExist(err) {
								continue
							}
							failpoint.Return(ver, errors.Trace(err))
						}
						break
					}
				}
			})

			ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StatePublic)
			if err != nil {
				return ver, errors.Trace(err)
			}

			a := &model.ModifyIndexArgs{
				PartitionIDs: getPartitionIDs(tbl.Meta()),
				OpType:       model.OpAddIndex,
			}
			for _, indexInfo := range allIndexInfos {
				a.IndexArgs = append(a.IndexArgs, &model.IndexArg{
					IndexID:  indexInfo.ID,
					IfExist:  false,
					IsGlobal: indexInfo.Global,
				})
			}
			job.FillFinishedArgs(a)

			analyzed := job.ReorgMeta.AnalyzeState == model.AnalyzeStateDone
			addIndexEvent := notifier.NewAddIndexEvent(tblInfo, allIndexInfos, analyzed)
			err2 := asyncNotifyEvent(jobCtx, addIndexEvent, job, noSubJob, w.sess)
			if err2 != nil {
				return ver, errors.Trace(err2)
			}

			// Finish this job.
			job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
			logutil.DDLLogger().Info("run add index job done",
				zap.String("charset", job.Charset),
				zap.String("collation", job.Collate))
		}
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", allIndexInfos[0].State)
	}

	return ver, errors.Trace(err)
}

