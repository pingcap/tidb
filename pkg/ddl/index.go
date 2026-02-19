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
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	lightningmetric "github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
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

func (w *worker) addPhysicalTableIndex(
	ctx context.Context,
	t table.PhysicalTable,
	reorgInfo *reorgInfo,
) error {
	if reorgInfo.mergingTmpIdx {
		logutil.DDLLogger().Info("start to merge temp index", zap.Stringer("job", reorgInfo.Job), zap.Stringer("reorgInfo", reorgInfo))
		return w.writePhysicalTableRecord(ctx, w.sessPool, t, typeAddIndexMergeTmpWorker, reorgInfo)
	}
	logutil.DDLLogger().Info("start to add table index", zap.Stringer("job", reorgInfo.Job), zap.Stringer("reorgInfo", reorgInfo))
	m := metrics.RegisterLightningCommonMetricsForDDL(reorgInfo.ID)
	ctx = lightningmetric.WithCommonMetric(ctx, m)
	defer func() {
		metrics.UnregisterLightningCommonMetricsForDDL(reorgInfo.ID, m)
	}()
	return w.writePhysicalTableRecord(ctx, w.sessPool, t, typeAddIndexWorker, reorgInfo)
}

// addTableIndex handles the add index reorganization state for a table.
func (w *worker) addTableIndex(
	jobCtx *jobContext,
	t table.Table,
	reorgInfo *reorgInfo,
) error {
	ctx := jobCtx.stepCtx
	if reorgInfo.ReorgMeta.IsDistReorg && reorgInfo.ReorgMeta.ReorgTp == model.ReorgTypeIngest {
		err := w.executeDistTask(jobCtx, t, reorgInfo)
		if err != nil {
			return err
		}
		if reorgInfo.ReorgMeta.UseCloudStorage {
			// When adding unique index by global sort, it detects duplicate keys in each step.
			// A duplicate key must be detected before, so we can skip the check bellow.
			return nil
		}
		if reorgInfo.mergingTmpIdx {
			// Merging temp index checks the duplicate keys in subtask executors.
			return nil
		}
		return checkDuplicateForUniqueIndex(ctx, t, reorgInfo, w.store)
	}

	var err error
	if tbl, ok := t.(table.PartitionedTable); ok {
		var finish, ok bool
		for !finish {
			var p table.PhysicalTable
			if tbl.Meta().ID == reorgInfo.PhysicalTableID {
				p, ok = t.(table.PhysicalTable) // global index
				if !ok {
					return fmt.Errorf("unexpected error, can't cast %T to table.PhysicalTable", t)
				}
			} else {
				p = tbl.GetPartition(reorgInfo.PhysicalTableID)
				if p == nil {
					return dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, t.Meta().ID)
				}
			}
			err = w.addPhysicalTableIndex(ctx, p, reorgInfo)
			if err != nil {
				break
			}

			finish, err = updateReorgInfo(w.sessPool, tbl, reorgInfo)
			if err != nil {
				return errors.Trace(err)
			}
			failpoint.InjectCall("afterUpdatePartitionReorgInfo", reorgInfo.Job)
			// Every time we finish a partition, we update the progress of the job.
			if rc := w.getReorgCtx(reorgInfo.Job.ID); rc != nil {
				reorgInfo.Job.SetRowCount(rc.getRowCount())
			}
		}
	} else {
		//nolint:forcetypeassert
		phyTbl := t.(table.PhysicalTable)
		err = w.addPhysicalTableIndex(ctx, phyTbl, reorgInfo)
	}
	return errors.Trace(err)
}

func checkDuplicateForUniqueIndex(ctx context.Context, t table.Table, reorgInfo *reorgInfo, store kv.Storage) (err error) {
	var (
		backendCtx ingest.BackendCtx
		cfg        *local.BackendConfig
		backend    *local.Backend
	)
	defer func() {
		if backendCtx != nil {
			backendCtx.Close()
		}
		if backend != nil {
			backend.Close()
		}
	}()
	for _, elem := range reorgInfo.elements {
		indexInfo := model.FindIndexInfoByID(t.Meta().Indices, elem.ID)
		if indexInfo == nil {
			return errors.New("unexpected error, can't find index info")
		}
		if indexInfo.Unique {
			ctx := tidblogutil.WithCategory(ctx, "ddl-ingest")
			if backendCtx == nil {
				if config.GetGlobalConfig().Store == config.StoreTypeTiKV {
					cfg, backend, err = ingest.CreateLocalBackend(ctx, store, reorgInfo.Job, true, true, 0)
					if err != nil {
						return errors.Trace(err)
					}
				}
				backendCtx, err = ingest.NewBackendCtxBuilder(ctx, store, reorgInfo.Job).
					ForDuplicateCheck().
					Build(cfg, backend)
				if err != nil {
					return err
				}
			}
			err = backendCtx.CollectRemoteDuplicateRows(indexInfo.ID, t)
			failpoint.Inject("mockCheckDuplicateForUniqueIndexError", func(_ failpoint.Value) {
				err = context.DeadlineExceeded
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// TaskKeyBuilder is used to build task key for the backfill job.
type TaskKeyBuilder struct {
	multiSchemaSeq int32
	mergeTempIdx   bool
}

// NewTaskKeyBuilder creates a new TaskKeyBuilder.
func NewTaskKeyBuilder() *TaskKeyBuilder {
	return &TaskKeyBuilder{multiSchemaSeq: -1}
}

// SetMergeTempIndex sets whether to merge the temporary index.
func (b *TaskKeyBuilder) SetMergeTempIndex(flag bool) *TaskKeyBuilder {
	b.mergeTempIdx = flag
	return b
}

// SetMultiSchema sets the multi-schema change information.
func (b *TaskKeyBuilder) SetMultiSchema(info *model.MultiSchemaInfo) *TaskKeyBuilder {
	if info != nil {
		b.multiSchemaSeq = info.Seq
	}
	return b
}

// Build builds the task key for the backfill job.
func (b *TaskKeyBuilder) Build(jobID int64) string {
	labels := make([]string, 0, 8)
	if kerneltype.IsNextGen() {
		labels = append(labels, keyspace.GetKeyspaceNameBySettings())
	}
	labels = append(labels, "ddl", proto.Backfill.String(), strconv.FormatInt(jobID, 10))
	if b.multiSchemaSeq >= 0 {
		labels = append(labels, strconv.Itoa(int(b.multiSchemaSeq)))
	}
	if b.mergeTempIdx {
		labels = append(labels, "merge")
	}
	return strings.Join(labels, "/")
}

// TaskKey generates a task key for the backfill job.
func TaskKey(jobID int64, mergeTempIdx bool) string {
	labels := make([]string, 0, 8)
	if kerneltype.IsNextGen() {
		ks := keyspace.GetKeyspaceNameBySettings()
		labels = append(labels, ks)
	}
	labels = append(labels, "ddl", proto.Backfill.String(), strconv.FormatInt(jobID, 10))
	if mergeTempIdx {
		labels = append(labels, "merge")
	}
	return strings.Join(labels, "/")
}

// changingIndex is used to store the index that need to be changed during modifying column.
type changingIndex struct {
	IndexInfo *model.IndexInfo
	// Column offset in idxInfo.Columns.
	Offset int
	// When the modifying column is contained in the index, a temp index is created.
	// isTemp indicates whether the indexInfo is a temp index created by a previous modify column job.
	isTemp bool
}

// FindRelatedIndexesToChange finds the indexes that covering the given column.
// The normal one will be overwritten by the temp one.
func FindRelatedIndexesToChange(tblInfo *model.TableInfo, colName ast.CIStr) []changingIndex {
	// In multi-schema change jobs that contains several "modify column" sub-jobs, there may be temp indexes for another temp index.
	// To prevent reorganizing too many indexes, we should create the temp indexes that are really necessary.
	var normalIdxInfos, tempIdxInfos []changingIndex
	for _, idxInfo := range tblInfo.Indices {
		if pos := findIdxCol(idxInfo, colName); pos != -1 {
			isTemp := isTempIndex(idxInfo, tblInfo)
			r := changingIndex{IndexInfo: idxInfo, Offset: pos, isTemp: isTemp}
			if isTemp {
				tempIdxInfos = append(tempIdxInfos, r)
			} else {
				normalIdxInfos = append(normalIdxInfos, r)
			}
		}
	}
	// Overwrite if the index has the corresponding temp index. For example,
	// we try to find the indexes that contain the column `b` and there are two indexes, `i(a, b)` and `$i($a, b)`.
	// Note that the symbol `$` means temporary. The index `$i($a, b)` is temporarily created by the previous "modify a" statement.
	// In this case, we would create a temporary index like $$i($a, $b), so the latter should be chosen.
	result := normalIdxInfos
	for _, tmpIdx := range tempIdxInfos {
		origName := tmpIdx.IndexInfo.GetChangingOriginName()
		for i, normIdx := range normalIdxInfos {
			if normIdx.IndexInfo.Name.O == origName {
				result[i] = tmpIdx
			}
		}
	}
	return result
}

// isColumnarIndexColumn checks if any index contains the given column is a columnar index.
func isColumnarIndexColumn(tblInfo *model.TableInfo, col *model.ColumnInfo) bool {
	indexesToChange := FindRelatedIndexesToChange(tblInfo, col.Name)
	for _, idx := range indexesToChange {
		if idx.IndexInfo.IsColumnarIndex() {
			return true
		}
	}
	return false
}

// isTempIndex checks whether the index is a temp index created by modify column.
// There are two types of temp index:
// 1. The index contains a temp column that is newly added, indicated by ChangeStateInfo
// 2. The index contains a old column changing its type in place, indicated by UsingChangingType
func isTempIndex(idxInfo *model.IndexInfo, tblInfo *model.TableInfo) bool {
	for _, idxCol := range idxInfo.Columns {
		if idxCol.UseChangingType || tblInfo.Columns[idxCol.Offset].ChangeStateInfo != nil {
			return true
		}
	}
	return false
}

func findIdxCol(idxInfo *model.IndexInfo, colName ast.CIStr) int {
	for offset, idxCol := range idxInfo.Columns {
		if idxCol.Name.L == colName.L {
			return offset
		}
	}
	return -1
}

func renameIndexes(tblInfo *model.TableInfo, from, to ast.CIStr) {
	for _, idx := range tblInfo.Indices {
		if idx.Name.L == from.L {
			idx.Name = to
		} else if isTempIndex(idx, tblInfo) &&
			(idx.GetChangingOriginName() == from.O ||
				idx.GetRemovingOriginName() == from.O) {
			idx.Name.L = strings.Replace(idx.Name.L, from.L, to.L, 1)
			idx.Name.O = strings.Replace(idx.Name.O, from.O, to.O, 1)
		}
		for _, col := range idx.Columns {
			originalCol := tblInfo.Columns[col.Offset]
			if originalCol.Hidden && getExpressionIndexOriginName(col.Name) == from.O {
				col.Name.L = strings.Replace(col.Name.L, from.L, to.L, 1)
				col.Name.O = strings.Replace(col.Name.O, from.O, to.O, 1)
			}
		}
	}
}

func renameHiddenColumns(tblInfo *model.TableInfo, from, to ast.CIStr) {
	for _, col := range tblInfo.Columns {
		if col.Hidden && getExpressionIndexOriginName(col.Name) == from.O {
			col.Name.L = strings.Replace(col.Name.L, from.L, to.L, 1)
			col.Name.O = strings.Replace(col.Name.O, from.O, to.O, 1)
		}
	}
}

// CheckAndBuildIndexConditionString validates whether the given expression is compatible with
// the table schema and returns a string representation of the expression.
func CheckAndBuildIndexConditionString(tblInfo *model.TableInfo, indexConditionExpr ast.ExprNode) (string, error) {
	if indexConditionExpr == nil {
		return "", nil
	}

	// Be careful, in `CREATE TABLE` statement, the `tblInfo.Partition` is always nil here. We have to
	// check it in `buildTablePartitionInfo` again.
	if tblInfo.Partition != nil {
		return "", dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
			"partial index on partitioned table is not supported")
	}

	// check partial index condition expression
	err := checkIndexCondition(tblInfo, indexConditionExpr)
	if err != nil {
		return "", errors.Trace(err)
	}

	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation | format.RestoreWithoutSchemaName | format.RestoreWithoutTableName
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
	sb.Reset()
	err = indexConditionExpr.Restore(restoreCtx)
	if err != nil {
		return "", errors.Trace(err)
	}

	return sb.String(), nil
}

func checkIndexCondition(tblInfo *model.TableInfo, indexCondition ast.ExprNode) error {
	// Only the following expressions are supported:
	// 1. column IS NULL
	// 2. column IS NOT NULL
	// 3. column = / != / > / < / >= / <= const
	// The column must be a visible column in the table, and the const must be a literal value with
	// the same type as the column.
	// The column must **NOT** be a generated column. We can loosen this restriction in the future.
	//
	// TODO: support more expressions in the future.
	if indexCondition == nil {
		return nil
	}

	switch cond := indexCondition.(type) {
	case *ast.IsNullExpr:
		// `IS NULL` and `IS NOT NULL` are both in this branch.
		columnName, ok := cond.Expr.(*ast.ColumnNameExpr)
		if !ok {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				"partial index condition must include a column name in the IS NULL expression")
		}
		columnInfo := model.FindColumnInfo(tblInfo.Columns, columnName.Name.Name.L)
		if columnInfo == nil {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("column name %s referenced in partial index condition is not found in table",
					columnName.Name.Name.L))
		}
		if columnInfo.IsGenerated() {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("generated column %s cannot be used in partial index condition", columnName.Name.Name.L))
		}

		return nil
	case *ast.BinaryOperationExpr:
		if cond.Op != opcode.EQ && cond.Op != opcode.NE && cond.Op != opcode.GT &&
			cond.Op != opcode.LT && cond.Op != opcode.GE && cond.Op != opcode.LE {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("binary operation %s is not supported", cond.Op.String()))
		}

		var columnName *ast.ColumnNameExpr
		var anotherSide ast.ExprNode
		columnName, ok := cond.L.(*ast.ColumnNameExpr)
		if !ok {
			// maybe the right side is a column name
			columnName, ok = cond.R.(*ast.ColumnNameExpr)
			if !ok {
				return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
					"partial index condition must include a column name in the binary operation")
			}

			anotherSide = cond.L
		} else {
			anotherSide = cond.R
		}
		columnInfo := model.FindColumnInfo(tblInfo.Columns, columnName.Name.Name.L)
		if columnInfo == nil {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("column name `%s` referenced in partial index condition is not found in table",
					columnName.Name.Name.L))
		}
		if columnInfo.IsGenerated() {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("generated column %s cannot be used in partial index condition", columnName.Name.Name.L))
		}

		// The another side must be a literal value, and it must have the same type as the column.
		constantExpr, ok := anotherSide.(ast.ValueExpr)
		if !ok {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				"partial index condition must include a literal value on the other side of the binary operation")
		}
		// Reference `types.DefaultTypeForValue`, they are all possible types for literal values.
		// However, this switch-case still includes more types than the ones we have in that function
		// to avoid breaking in the future.
		//
		// Accept tiny type conversion as the type of the literal value is too limited. We shouldn't
		// force the user to use such a limited range of types.
		//
		// It'll allow precision / length difference in most of the cases.
		switch constantExpr.GetType().GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong,
			mysql.TypeInt24, mysql.TypeBit, mysql.TypeYear:
			// the target column must be an integer type or enum or set
			if columnInfo.GetType() != mysql.TypeTiny &&
				columnInfo.GetType() != mysql.TypeShort &&
				columnInfo.GetType() != mysql.TypeLong &&
				columnInfo.GetType() != mysql.TypeLonglong &&
				columnInfo.GetType() != mysql.TypeInt24 &&
				columnInfo.GetType() != mysql.TypeBit &&
				columnInfo.GetType() != mysql.TypeYear &&
				columnInfo.GetType() != mysql.TypeEnum &&
				columnInfo.GetType() != mysql.TypeSet {
				return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
					fmt.Sprintf("the type %s of the column `%s` in partial index condition is not compatible with the literal value type %s",
						columnInfo.FieldType.String(), columnName.Name.Name.L, constantExpr.GetType().String()))
			}
			return nil
		case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
			// the target column must be either a float or double type
			// TODO: consider whether need to support decimal type in this branch
			if columnInfo.GetType() != mysql.TypeFloat &&
				columnInfo.GetType() != mysql.TypeDouble &&
				columnInfo.GetType() != mysql.TypeNewDecimal {
				return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
					fmt.Sprintf("the type %s of the column `%s` in partial index condition is not compatible with the literal value type %s",
						columnInfo.FieldType.String(), columnName.Name.Name.L, constantExpr.GetType().String()))
			}
			return nil
		case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
			mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
			if types.IsString(columnInfo.GetType()) {
				// check the collation of the column and the literal value
				if columnInfo.FieldType.GetCharset() != constantExpr.GetType().GetCharset() {
					return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
						fmt.Sprintf("the charset %s of the column `%s` in partial index condition is not compatible with the literal value charset %s",
							columnInfo.FieldType.GetCharset(), columnName.Name.Name.L, constantExpr.GetType().GetCharset()))
				}

				return nil
			}

			// Allow to compare a datetime type column with a string literal, because we don't have a datetime literal.
			// This branch will allow users to use datetime columns in index condition.
			if columnInfo.GetType() == mysql.TypeTimestamp ||
				columnInfo.GetType() == mysql.TypeDate ||
				columnInfo.GetType() == mysql.TypeDuration ||
				columnInfo.GetType() == mysql.TypeNewDate ||
				columnInfo.GetType() == mysql.TypeDatetime {
				return nil
			}

			// ENUM and SET are also allowed for string literal.
			if columnInfo.GetType() == mysql.TypeEnum || columnInfo.GetType() == mysql.TypeSet {
				return nil
			}

			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("the type %s of the column `%s` in partial index condition is not compatible with the literal value type %s",
					columnInfo.FieldType.String(), columnName.Name.Name.L, constantExpr.GetType().String()))
		case mysql.TypeNull:
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				"= NULL is not supported in partial index condition because it is always false")
		case mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDuration, mysql.TypeNewDate,
			mysql.TypeDatetime, mysql.TypeJSON, mysql.TypeEnum, mysql.TypeSet:
			// The `DATE '2025-07-28'` is actually a `cast` function, so they are also not supported yet.
			intest.Assert(false, "should never generate literal values of these types")

			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("the type %s of the literal value in partial index condition is not supported",
					constantExpr.GetType().String()))
		default:
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("the type %s of the literal value in partial index condition is not supported",
					constantExpr.GetType().String()))
		}
	default:
		return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
			"the kind of partial index condition is not supported")
	}
}

func buildAffectColumn(idxInfo *model.IndexInfo, tblInfo *model.TableInfo) ([]*model.IndexColumn, error) {
	ectx := exprstatic.NewExprContext()

	// Build affect column for partial index.
	if idxInfo.HasCondition() {
		cols, err := tables.ExtractColumnsFromCondition(ectx, idxInfo, tblInfo, true)
		if err != nil {
			return nil, err
		}
		return tables.DedupIndexColumns(cols), nil
	}

	return nil, nil
}

// buildIndexConditionChecker builds an expression for evaluating the index condition based on
// the given columns.
func buildIndexConditionChecker(copCtx copr.CopContext, tblInfo *model.TableInfo, idxInfo *model.IndexInfo) (func(row chunk.Row) (bool, error), error) {
	schema, names := copCtx.GetBase().GetSchemaAndNames()

	exprCtx := copCtx.GetBase().ExprCtx
	expr, err := expression.ParseSimpleExpr(exprCtx, idxInfo.ConditionExprString, expression.WithInputSchemaAndNames(schema, names, tblInfo))
	if err != nil {
		return nil, err
	}

	return func(row chunk.Row) (bool, error) {
		datum, isNull, err := expr.EvalInt(exprCtx.GetEvalCtx(), row)
		if err != nil {
			return false, err
		}
		// If the result is NULL, it usually means the original column itself is NULL.
		// In this case, we should refuse to consider the index for partial index condition.
		return datum > 0 && !isNull, nil
	}, nil
}
