// Copyright 2017 PingCAP, Inc.
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
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"go.uber.org/zap"
)

const (
	insertDeleteRangeSQLPrefix = `INSERT IGNORE INTO mysql.gc_delete_range VALUES `
	insertDeleteRangeSQLValue  = `(%?, %?, %?, %?, %?)`
	insertDeleteRangeSQL       = insertDeleteRangeSQLPrefix + insertDeleteRangeSQLValue

	delBatchSize = 65536
	delBackLog   = 128
)

var (
	// batchInsertDeleteRangeSize is the maximum size for each batch insert statement in the delete-range.
	batchInsertDeleteRangeSize = 256
)

type delRangeManager interface {
	// addDelRangeJob add a DDL job into gc_delete_range table.
	addDelRangeJob(ctx context.Context, job *model.Job) error
	// removeFromGCDeleteRange removes the deleting table job from gc_delete_range table by jobID and tableID.
	// It's use for recover the table that was mistakenly deleted.
	removeFromGCDeleteRange(ctx context.Context, jobID int64, tableID []int64) error
	start()
	clear()
}

type delRange struct {
	store      kv.Storage
	sessPool   *sessionPool
	emulatorCh chan struct{}
	keys       []kv.Key
	quitCh     chan struct{}

	wait         sync.WaitGroup // wait is only used when storeSupport is false.
	storeSupport bool
}

// newDelRangeManager returns a delRangeManager.
func newDelRangeManager(store kv.Storage, sessPool *sessionPool) delRangeManager {
	dr := &delRange{
		store:        store,
		sessPool:     sessPool,
		storeSupport: store.SupportDeleteRange(),
		quitCh:       make(chan struct{}),
	}
	if !dr.storeSupport {
		dr.emulatorCh = make(chan struct{}, delBackLog)
		dr.keys = make([]kv.Key, 0, delBatchSize)
	}
	return dr
}

// addDelRangeJob implements delRangeManager interface.
func (dr *delRange) addDelRangeJob(ctx context.Context, job *model.Job) error {
	sctx, err := dr.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer dr.sessPool.put(sctx)

	err = insertJobIntoDeleteRangeTable(ctx, sctx, job)
	if err != nil {
		logutil.BgLogger().Error("[ddl] add job into delete-range table failed", zap.Int64("jobID", job.ID), zap.String("jobType", job.Type.String()), zap.Error(err))
		return errors.Trace(err)
	}
	if !dr.storeSupport {
		dr.emulatorCh <- struct{}{}
	}
	logutil.BgLogger().Info("[ddl] add job into delete-range table", zap.Int64("jobID", job.ID), zap.String("jobType", job.Type.String()))
	return nil
}

// removeFromGCDeleteRange implements delRangeManager interface.
func (dr *delRange) removeFromGCDeleteRange(ctx context.Context, jobID int64, tableIDs []int64) error {
	sctx, err := dr.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer dr.sessPool.put(sctx)
	err = util.RemoveMultiFromGCDeleteRange(ctx, sctx, jobID, tableIDs)
	return errors.Trace(err)
}

// start implements delRangeManager interface.
func (dr *delRange) start() {
	if !dr.storeSupport {
		dr.wait.Add(1)
		go dr.startEmulator()
	}
}

// clear implements delRangeManager interface.
func (dr *delRange) clear() {
	logutil.BgLogger().Info("[ddl] closing delRange")
	close(dr.quitCh)
	dr.wait.Wait()
}

// startEmulator is only used for those storage engines which don't support
// delete-range. The emulator fetches records from gc_delete_range table and
// deletes all keys in each DelRangeTask.
func (dr *delRange) startEmulator() {
	defer dr.wait.Done()
	logutil.BgLogger().Info("[ddl] start delRange emulator")
	for {
		select {
		case <-dr.emulatorCh:
		case <-dr.quitCh:
			return
		}
		if util.IsEmulatorGCEnable() {
			err := dr.doDelRangeWork()
			terror.Log(errors.Trace(err))
		}
	}
}

func (dr *delRange) doDelRangeWork() error {
	ctx, err := dr.sessPool.get()
	if err != nil {
		logutil.BgLogger().Error("[ddl] delRange emulator get session failed", zap.Error(err))
		return errors.Trace(err)
	}
	defer dr.sessPool.put(ctx)

	ranges, err := util.LoadDeleteRanges(ctx, math.MaxInt64)
	if err != nil {
		logutil.BgLogger().Error("[ddl] delRange emulator load tasks failed", zap.Error(err))
		return errors.Trace(err)
	}

	for _, r := range ranges {
		if err := dr.doTask(ctx, r); err != nil {
			logutil.BgLogger().Error("[ddl] delRange emulator do task failed", zap.Error(err))
			return errors.Trace(err)
		}
	}
	return nil
}

func (dr *delRange) doTask(ctx sessionctx.Context, r util.DelRangeTask) error {
	var oldStartKey, newStartKey kv.Key
	oldStartKey = r.StartKey
	for {
		finish := true
		dr.keys = dr.keys[:0]
		err := kv.RunInNewTxn(context.Background(), dr.store, false, func(ctx context.Context, txn kv.Transaction) error {
			if topsqlstate.TopSQLEnabled() {
				// Only when TiDB run without PD(use unistore as storage for test) will run into here, so just set a mock internal resource tagger.
				txn.SetOption(kv.ResourceGroupTagger, util.GetInternalResourceGroupTaggerForTopSQL())
			}
			iter, err := txn.Iter(oldStartKey, r.EndKey)
			if err != nil {
				return errors.Trace(err)
			}
			defer iter.Close()

			txn.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
			for i := 0; i < delBatchSize; i++ {
				if !iter.Valid() {
					break
				}
				finish = false
				dr.keys = append(dr.keys, iter.Key().Clone())
				newStartKey = iter.Key().Next()

				if err := iter.Next(); err != nil {
					return errors.Trace(err)
				}
			}

			for _, key := range dr.keys {
				err := txn.Delete(key)
				if err != nil && !kv.ErrNotExist.Equal(err) {
					return errors.Trace(err)
				}
			}
			return nil
		})
		if err != nil {
			return errors.Trace(err)
		}
		if finish {
			if err := util.CompleteDeleteRange(ctx, r); err != nil {
				logutil.BgLogger().Error("[ddl] delRange emulator complete task failed", zap.Error(err))
				return errors.Trace(err)
			}
			startKey, endKey := r.Range()
			logutil.BgLogger().Info("[ddl] delRange emulator complete task",
				zap.Int64("jobID", r.JobID),
				zap.Int64("elementID", r.ElementID),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey))
			break
		}
		if err := util.UpdateDeleteRange(ctx, r, newStartKey, oldStartKey); err != nil {
			logutil.BgLogger().Error("[ddl] delRange emulator update task failed", zap.Error(err))
		}
		oldStartKey = newStartKey
	}
	return nil
}

type elementIDAlloc struct {
	id int64
}

func (ea *elementIDAlloc) alloc() int64 {
	ea.id++
	return ea.id
}

// insertJobIntoDeleteRangeTable parses the job into delete-range arguments,
// and inserts a new record into gc_delete_range table. The primary key is
// (job ID, element ID), so we ignore key conflict error.
func insertJobIntoDeleteRangeTable(ctx context.Context, sctx sessionctx.Context, job *model.Job) error {
	now, err := getNowTSO(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	var ea elementIDAlloc

	s := sctx.(sqlexec.SQLExecutor)
	switch job.Type {
	case model.ActionDropSchema:
		var tableIDs []int64
		if err := job.DecodeArgs(&tableIDs); err != nil {
			return errors.Trace(err)
		}
		for i := 0; i < len(tableIDs); i += batchInsertDeleteRangeSize {
			batchEnd := len(tableIDs)
			if batchEnd > i+batchInsertDeleteRangeSize {
				batchEnd = i + batchInsertDeleteRangeSize
			}
			if err := doBatchInsert(ctx, s, job.ID, tableIDs[i:batchEnd], now, &ea); err != nil {
				return errors.Trace(err)
			}
		}
	case model.ActionDropTable, model.ActionTruncateTable:
		tableID := job.TableID
		// The startKey here is for compatibility with previous versions, old version did not endKey so don't have to deal with.
		var startKey kv.Key
		var physicalTableIDs []int64
		var ruleIDs []string
		if err := job.DecodeArgs(&startKey, &physicalTableIDs, &ruleIDs); err != nil {
			return errors.Trace(err)
		}
		if len(physicalTableIDs) > 0 {
			for _, pid := range physicalTableIDs {
				startKey = tablecodec.EncodeTablePrefix(pid)
				endKey := tablecodec.EncodeTablePrefix(pid + 1)
				if err := doInsert(ctx, s, job.ID, ea.alloc(), startKey, endKey, now, fmt.Sprintf("partition ID is %d", pid)); err != nil {
					return errors.Trace(err)
				}
			}
			return nil
		}
		startKey = tablecodec.EncodeTablePrefix(tableID)
		endKey := tablecodec.EncodeTablePrefix(tableID + 1)
		return doInsert(ctx, s, job.ID, ea.alloc(), startKey, endKey, now, fmt.Sprintf("table ID is %d", tableID))
	case model.ActionDropTablePartition, model.ActionTruncateTablePartition:
		var physicalTableIDs []int64
		if err := job.DecodeArgs(&physicalTableIDs); err != nil {
			return errors.Trace(err)
		}
		for _, physicalTableID := range physicalTableIDs {
			startKey := tablecodec.EncodeTablePrefix(physicalTableID)
			endKey := tablecodec.EncodeTablePrefix(physicalTableID + 1)
			if err := doInsert(ctx, s, job.ID, ea.alloc(), startKey, endKey, now, fmt.Sprintf("partition table ID is %d", physicalTableID)); err != nil {
				return errors.Trace(err)
			}
		}
	// ActionAddIndex, ActionAddPrimaryKey needs do it, because it needs to be rolled back when it's canceled.
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		tableID := job.TableID
		var indexID int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexID, &partitionIDs); err != nil {
			return errors.Trace(err)
		}
		if len(partitionIDs) > 0 {
			for _, pid := range partitionIDs {
				startKey := tablecodec.EncodeTableIndexPrefix(pid, indexID)
				endKey := tablecodec.EncodeTableIndexPrefix(pid, indexID+1)
				if err := doInsert(ctx, s, job.ID, ea.alloc(), startKey, endKey, now, fmt.Sprintf("partition table ID is %d", pid)); err != nil {
					return errors.Trace(err)
				}
			}
		} else {
			startKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
			endKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID+1)
			return doInsert(ctx, s, job.ID, ea.alloc(), startKey, endKey, now, fmt.Sprintf("table ID is %d", tableID))
		}
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		tableID := job.TableID
		var indexName interface{}
		var indexID int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexName, &indexID, &partitionIDs); err != nil {
			return errors.Trace(err)
		}
		if len(partitionIDs) > 0 {
			for _, pid := range partitionIDs {
				startKey := tablecodec.EncodeTableIndexPrefix(pid, indexID)
				endKey := tablecodec.EncodeTableIndexPrefix(pid, indexID+1)
				if err := doInsert(ctx, s, job.ID, ea.alloc(), startKey, endKey, now, fmt.Sprintf("partition table ID is %d", pid)); err != nil {
					return errors.Trace(err)
				}
			}
		} else {
			startKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
			endKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID+1)
			return doInsert(ctx, s, job.ID, ea.alloc(), startKey, endKey, now, fmt.Sprintf("index ID is %d", indexID))
		}
	case model.ActionDropIndexes:
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&[]model.CIStr{}, &[]bool{}, &indexIDs, &partitionIDs); err != nil {
			return errors.Trace(err)
		}
		// Remove data in TiKV.
		if len(indexIDs) == 0 {
			return nil
		}
		if len(partitionIDs) == 0 {
			return doBatchDeleteIndiceRange(ctx, s, job.ID, job.TableID, indexIDs, now, &ea)
		}
		for _, pID := range partitionIDs {
			if err := doBatchDeleteIndiceRange(ctx, s, job.ID, pID, indexIDs, now, &ea); err != nil {
				return errors.Trace(err)
			}
		}
	case model.ActionDropColumn:
		var colName model.CIStr
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&colName, &indexIDs, &partitionIDs); err != nil {
			return errors.Trace(err)
		}
		if len(indexIDs) > 0 {
			if len(partitionIDs) > 0 {
				for _, pid := range partitionIDs {
					if err := doBatchDeleteIndiceRange(ctx, s, job.ID, pid, indexIDs, now, &ea); err != nil {
						return errors.Trace(err)
					}
				}
			} else {
				return doBatchDeleteIndiceRange(ctx, s, job.ID, job.TableID, indexIDs, now, &ea)
			}
		}
	case model.ActionDropColumns:
		var colNames []model.CIStr
		var ifExists []bool
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&colNames, &ifExists, &indexIDs, &partitionIDs); err != nil {
			return errors.Trace(err)
		}
		if len(indexIDs) > 0 {
			if len(partitionIDs) > 0 {
				for _, pid := range partitionIDs {
					if err := doBatchDeleteIndiceRange(ctx, s, job.ID, pid, indexIDs, now, &ea); err != nil {
						return errors.Trace(err)
					}
				}
			} else {
				return doBatchDeleteIndiceRange(ctx, s, job.ID, job.TableID, indexIDs, now, &ea)
			}
		}
	case model.ActionModifyColumn:
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexIDs, &partitionIDs); err != nil {
			return errors.Trace(err)
		}
		if len(indexIDs) == 0 {
			return nil
		}
		if len(partitionIDs) == 0 {
			return doBatchDeleteIndiceRange(ctx, s, job.ID, job.TableID, indexIDs, now, &ea)
		}
		for _, pid := range partitionIDs {
			if err := doBatchDeleteIndiceRange(ctx, s, job.ID, pid, indexIDs, now, &ea); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func doBatchDeleteIndiceRange(ctx context.Context, s sqlexec.SQLExecutor, jobID, tableID int64, indexIDs []int64, ts uint64, ea *elementIDAlloc) error {
	logutil.BgLogger().Info("[ddl] batch insert into delete-range indices", zap.Int64("jobID", jobID), zap.Int64("tableID", tableID), zap.Int64s("indexIDs", indexIDs))
	paramsList := make([]interface{}, 0, len(indexIDs)*5)
	var buf strings.Builder
	buf.WriteString(insertDeleteRangeSQLPrefix)
	for i, indexID := range indexIDs {
		startKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
		endKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID+1)
		startKeyEncoded := hex.EncodeToString(startKey)
		endKeyEncoded := hex.EncodeToString(endKey)
		buf.WriteString(insertDeleteRangeSQLValue)
		if i != len(indexIDs)-1 {
			buf.WriteString(",")
		}
		paramsList = append(paramsList, jobID, ea.alloc(), startKeyEncoded, endKeyEncoded, ts)
	}
	_, err := s.ExecuteInternal(ctx, buf.String(), paramsList...)
	return errors.Trace(err)
}

func doInsert(ctx context.Context, s sqlexec.SQLExecutor, jobID, elementID int64, startKey, endKey kv.Key, ts uint64, comment string) error {
	logutil.BgLogger().Info("[ddl] insert into delete-range table", zap.Int64("jobID", jobID), zap.Int64("elementID", elementID), zap.String("comment", comment))
	startKeyEncoded := hex.EncodeToString(startKey)
	endKeyEncoded := hex.EncodeToString(endKey)
	// set session disk full opt
	// TODO ddl txn func including an session pool txn, there may be a problem?
	s.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	_, err := s.ExecuteInternal(ctx, insertDeleteRangeSQL, jobID, elementID, startKeyEncoded, endKeyEncoded, ts)
	// clear session disk full opt
	s.ClearDiskFullOpt()
	return errors.Trace(err)
}

func doBatchInsert(ctx context.Context, s sqlexec.SQLExecutor, jobID int64, tableIDs []int64, ts uint64, ea *elementIDAlloc) error {
	logutil.BgLogger().Info("[ddl] batch insert into delete-range table", zap.Int64("jobID", jobID), zap.Int64s("tableIDs", tableIDs))
	var buf strings.Builder
	buf.WriteString(insertDeleteRangeSQLPrefix)
	paramsList := make([]interface{}, 0, len(tableIDs)*5)
	for i, tableID := range tableIDs {
		startKey := tablecodec.EncodeTablePrefix(tableID)
		endKey := tablecodec.EncodeTablePrefix(tableID + 1)
		startKeyEncoded := hex.EncodeToString(startKey)
		endKeyEncoded := hex.EncodeToString(endKey)
		buf.WriteString(insertDeleteRangeSQLValue)
		if i != len(tableIDs)-1 {
			buf.WriteString(",")
		}
		paramsList = append(paramsList, jobID, ea.alloc(), startKeyEncoded, endKeyEncoded, ts)
	}
	// set session disk full opt
	s.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	_, err := s.ExecuteInternal(ctx, buf.String(), paramsList...)
	// clear session disk full opt
	s.ClearDiskFullOpt()
	return errors.Trace(err)
}

// getNowTS gets the current timestamp, in TSO.
func getNowTSO(ctx sessionctx.Context) (uint64, error) {
	currVer, err := ctx.GetStore().CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return currVer.Ver, nil
}
