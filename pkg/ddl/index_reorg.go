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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	lightningmetric "github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

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

