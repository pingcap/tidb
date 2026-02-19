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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

func getNextPartitionInfo(reorg *reorgInfo, t table.PartitionedTable, currPhysicalTableID int64) (pid int64, startKey, endKey kv.Key, err error) {
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return 0, nil, nil, nil
	}

	// This will be used in multiple different scenarios/ALTER TABLE:
	// ADD INDEX - no change in partitions, just use pi.Definitions (1)
	// REORGANIZE PARTITION - copy data from partitions to be dropped (2)
	// REORGANIZE PARTITION - (re)create indexes on partitions to be added (3)
	// REORGANIZE PARTITION - Update new Global indexes with data from non-touched partitions (4)
	// (i.e. pi.Definitions - pi.DroppingDefinitions)
	// MODIFY COLUMN - no change in partitions, just use pi.Definitions (5)
	if bytes.Equal(reorg.currElement.TypeKey, meta.IndexElementKey) {
		// case 1, 3 or 4
		if len(pi.AddingDefinitions) == 0 {
			// case 1
			// Simply AddIndex, without any partitions added or dropped!
			if reorg.mergingTmpIdx && currPhysicalTableID == t.Meta().ID {
				// If the current Physical id is the table id,
				// 1. All indexes are global index, the next Physical id should be the first partition id.
				// 2. Not all indexes are global index, return 0.
				allGlobal := true
				for _, element := range reorg.elements {
					if !bytes.Equal(element.TypeKey, meta.IndexElementKey) {
						allGlobal = false
						break
					}
					idxInfo := model.FindIndexInfoByID(t.Meta().Indices, element.ID)
					if !idxInfo.Global {
						allGlobal = false
						break
					}
				}
				if allGlobal {
					pid = 0
				} else {
					pid = pi.Definitions[0].ID
				}
			} else {
				pid, err = findNextPartitionID(currPhysicalTableID, pi.Definitions)
			}
		} else {
			// case 3 (or if not found AddingDefinitions; 4)
			// check if recreating Global Index (during Reorg Partition)
			pid, err = findNextPartitionID(currPhysicalTableID, pi.AddingDefinitions)
			if err != nil {
				// case 4
				// Not a partition in the AddingDefinitions, so it must be an existing
				// non-touched partition, i.e. recreating Global Index for the non-touched partitions
				pid, err = findNextNonTouchedPartitionID(currPhysicalTableID, pi)
			}
		}
	} else if len(pi.DroppingDefinitions) == 0 {
		// case 5
		pid, err = findNextPartitionID(currPhysicalTableID, pi.Definitions)
	} else {
		// case 2
		pid, err = findNextPartitionID(currPhysicalTableID, pi.DroppingDefinitions)
	}
	if err != nil {
		// Fatal error, should not run here.
		logutil.DDLLogger().Error("find next partition ID failed", zap.Reflect("table", t), zap.Error(err))
		return 0, nil, nil, errors.Trace(err)
	}
	if pid == 0 {
		// Next partition does not exist, all the job done.
		return 0, nil, nil, nil
	}

	failpoint.Inject("mockUpdateCachedSafePoint", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			ts := oracle.GoTimeToTS(time.Now())
			//nolint:forcetypeassert
			s := reorg.jobCtx.store.(tikv.Storage)
			s.UpdateTxnSafePointCache(ts, time.Now())
			time.Sleep(time.Second * 3)
		}
	})

	if reorg.mergingTmpIdx {
		elements := reorg.elements
		firstElemTempID := tablecodec.TempIndexPrefix | elements[0].ID
		lastElemTempID := tablecodec.TempIndexPrefix | elements[len(elements)-1].ID
		startKey = tablecodec.EncodeIndexSeekKey(pid, firstElemTempID, nil)
		endKey = tablecodec.EncodeIndexSeekKey(pid, lastElemTempID, []byte{255})
	} else {
		currentVer, err := getValidCurrentVersion(reorg.jobCtx.store)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
		startKey, endKey, err = getTableRange(reorg.NewJobContext(), reorg.jobCtx.store, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
	}
	return pid, startKey, endKey, nil
}

// updateReorgInfo will find the next partition according to current reorgInfo.
// If no more partitions, or table t is not a partitioned table, returns true to
// indicate that the reorganize work is finished.
func updateReorgInfo(sessPool *sess.Pool, t table.PartitionedTable, reorg *reorgInfo) (bool, error) {
	pid, startKey, endKey, err := getNextPartitionInfo(reorg, t, reorg.PhysicalTableID)
	if err != nil {
		return false, errors.Trace(err)
	}
	if pid == 0 {
		// Next partition does not exist, all the job done.
		return true, nil
	}
	reorg.PhysicalTableID, reorg.StartKey, reorg.EndKey = pid, startKey, endKey

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = reorg.UpdateReorgMeta(reorg.StartKey, sessPool)
	logutil.DDLLogger().Info("job update reorgInfo",
		zap.Int64("jobID", reorg.Job.ID),
		zap.Stringer("element", reorg.currElement),
		zap.Int64("partitionTableID", pid),
		zap.String("startKey", hex.EncodeToString(reorg.StartKey)),
		zap.String("endKey", hex.EncodeToString(reorg.EndKey)), zap.Error(err))
	return false, errors.Trace(err)
}

// findNextPartitionID finds the next partition ID in the PartitionDefinition array.
// Returns 0 if current partition is already the last one.
func findNextPartitionID(currentPartition int64, defs []model.PartitionDefinition) (int64, error) {
	for i, def := range defs {
		if currentPartition == def.ID {
			if i == len(defs)-1 {
				return 0, nil
			}
			return defs[i+1].ID, nil
		}
	}
	return 0, errors.Errorf("partition id not found %d", currentPartition)
}

func findNextNonTouchedPartitionID(currPartitionID int64, pi *model.PartitionInfo) (int64, error) {
	pid, err := findNextPartitionID(currPartitionID, pi.Definitions)
	if err != nil {
		return 0, err
	}
	if pid == 0 {
		return 0, nil
	}
	for _, notFoundErr := findNextPartitionID(pid, pi.DroppingDefinitions); notFoundErr == nil; {
		// This can be optimized, but it is not frequently called, so keeping as-is
		pid, err = findNextPartitionID(pid, pi.Definitions)
		if pid == 0 {
			break
		}
	}
	return pid, err
}

// AllocateIndexID allocates an index ID from TableInfo.
func AllocateIndexID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxIndexID++
	return tblInfo.MaxIndexID
}

func getIndexInfoByNameAndColumn(oldTableInfo *model.TableInfo, newOne *model.IndexInfo) *model.IndexInfo {
	for _, oldOne := range oldTableInfo.Indices {
		if newOne.Name.L == oldOne.Name.L && indexColumnSliceEqual(newOne.Columns, oldOne.Columns) {
			return oldOne
		}
	}
	return nil
}

func indexColumnSliceEqual(a, b []*model.IndexColumn) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		logutil.DDLLogger().Warn("admin repair table : index's columns length equal to 0")
		return true
	}
	// Accelerate the compare by eliminate index bound check.
	b = b[:len(a)]
	for i, v := range a {
		if v.Name.L != b[i].Name.L {
			return false
		}
	}
	return true
}

type cleanUpIndexWorker struct {
	baseIndexWorker
}

func newCleanUpIndexWorker(id int, t table.PhysicalTable, decodeColMap map[int64]decoder.Column, reorgInfo *reorgInfo, jc *ReorgContext) (*cleanUpIndexWorker, error) {
	bCtx, err := newBackfillCtx(id, reorgInfo, reorgInfo.SchemaName, t, jc, metrics.LblCleanupIdxRate, false)
	if err != nil {
		return nil, err
	}

	indexes := make([]table.Index, 0, len(t.Indices()))
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)
	for _, index := range t.Indices() {
		if index.Meta().IsColumnarIndex() {
			continue
		}
		if index.Meta().Global {
			indexes = append(indexes, index)
		}
	}
	return &cleanUpIndexWorker{
		baseIndexWorker: baseIndexWorker{
			backfillCtx: bCtx,
			indexes:     indexes,
			rowDecoder:  rowDecoder,
			defaultVals: make([]types.Datum, len(t.WritableCols())),
			rowMap:      make(map[int64]types.Datum, len(decodeColMap)),
		},
	}, nil
}

func (w *cleanUpIndexWorker) BackfillData(_ context.Context, handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			panic("panic test")
		}
	})

	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceAndTaskType(context.Background(), w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)
	errInTxn = kv.RunInNewTxn(ctx, w.ddlCtx.store, true, func(_ context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		updateTxnEntrySizeLimitIfNeeded(txn)
		txn.SetOption(kv.Priority, handleRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(handleRange.getJobID()); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}
		txn.SetOption(kv.ResourceGroupName, w.jobContext.resourceGroupName)

		idxRecords, nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		txn.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)

		lockCtx := new(kv.LockCtx)
		evalCtx := w.exprCtx.GetEvalCtx()
		loc, ec := evalCtx.Location(), evalCtx.ErrCtx()
		n := len(w.indexes)
		globalIndexKeys := make([]kv.Key, 0, len(idxRecords))
		allKeys := make([]kv.Key, 0, len(idxRecords))
		for i, idxRecord := range idxRecords {
			key, distinct, err := w.indexes[i%n].GenIndexKey(ec, loc, idxRecord.vals, idxRecord.handle, nil)
			if err != nil {
				return errors.Trace(err)
			}
			if distinct {
				globalIndexKeys = append(globalIndexKeys, key)
			}
			allKeys = append(allKeys, key)
		}

		var found map[string][]byte
		found, err = kv.BatchGetValue(ctx, txn, globalIndexKeys)
		if err != nil {
			return errors.Trace(err)
		}

		for i, idxRecord := range idxRecords {
			taskCtx.scanCount++
			if val, ok := found[string(allKeys[i])]; ok {
				// Only delete if it is from the partition it was read from.
				handle, errPart := tablecodec.DecodeHandleInIndexValue(val)
				if errPart != nil {
					return errors.Trace(errPart)
				}
				if partHandle, ok := handle.(kv.PartitionHandle); ok {
					if partHandle.PartitionID != handleRange.physicalTable.GetPhysicalID() {
						continue
					}
				}
				// Lock the global index entry, to prevent deleting something that is concurrently added.
				err = txn.LockKeys(ctx, lockCtx, allKeys[i])
				if err != nil {
					return errors.Trace(err)
				}
			}
			// we fetch records row by row, so records will belong to
			// index[0], index[1] ... index[n-1], index[0], index[1] ...
			// respectively. So indexes[i%n] is the index of idxRecords[i].
			err = w.indexes[i%n].Delete(w.tblCtx, txn, idxRecord.vals, idxRecord.handle)
			if err != nil {
				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}
		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "cleanUpIndexBackfillDataInTxn", 3000)
	failpoint.Inject("mockDMLExecution", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && MockDMLExecution != nil {
			MockDMLExecution()
		}
	})

	return
}

// cleanupPhysicalTableIndex handles the drop partition reorganization state for a non-partitioned table or a partition.
func (w *worker) cleanupPhysicalTableIndex(t table.PhysicalTable, reorgInfo *reorgInfo) error {
	logutil.DDLLogger().Info("start to clean up index", zap.Stringer("job", reorgInfo.Job), zap.Stringer("reorgInfo", reorgInfo))
	return w.writePhysicalTableRecord(w.workCtx, w.sessPool, t, typeCleanUpIndexWorker, reorgInfo)
}

// cleanupGlobalIndex handles the drop partition reorganization state to clean up index entries of partitions.
func (w *worker) cleanupGlobalIndexes(tbl table.PartitionedTable, partitionIDs []int64, reorgInfo *reorgInfo) error {
	var err error
	var finish bool
	for !finish {
		p := tbl.GetPartition(reorgInfo.PhysicalTableID)
		if p == nil {
			return dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, tbl.Meta().ID)
		}
		err = w.cleanupPhysicalTableIndex(p, reorgInfo)
		if err != nil {
			break
		}
		finish, err = w.updateReorgInfoForPartitions(tbl, reorgInfo, partitionIDs)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return errors.Trace(err)
}

// updateReorgInfoForPartitions will find the next partition in partitionIDs according to current reorgInfo.
// If no more partitions, or table t is not a partitioned table, returns true to
// indicate that the reorganize work is finished.
func (w *worker) updateReorgInfoForPartitions(t table.PartitionedTable, reorg *reorgInfo, partitionIDs []int64) (bool, error) {
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return true, nil
	}

	var pid int64
	for i, pi := range partitionIDs {
		if pi == reorg.PhysicalTableID {
			if i == len(partitionIDs)-1 {
				return true, nil
			}
			pid = partitionIDs[i+1]
			break
		}
	}

	currentVer, err := getValidCurrentVersion(reorg.jobCtx.store)
	if err != nil {
		return false, errors.Trace(err)
	}
	start, end, err := getTableRange(reorg.NewJobContext(), reorg.jobCtx.store, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
	if err != nil {
		return false, errors.Trace(err)
	}
	reorg.StartKey, reorg.EndKey, reorg.PhysicalTableID = start, end, pid

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = reorg.UpdateReorgMeta(reorg.StartKey, w.sessPool)
	logutil.DDLLogger().Info("job update reorg info", zap.Int64("jobID", reorg.Job.ID),
		zap.Stringer("element", reorg.currElement),
		zap.Int64("partition table ID", pid), zap.String("start key", hex.EncodeToString(start)),
		zap.String("end key", hex.EncodeToString(end)), zap.Error(err))
	return false, errors.Trace(err)
}

