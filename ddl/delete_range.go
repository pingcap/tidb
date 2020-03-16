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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	insertDeleteRangeSQL = `INSERT IGNORE INTO mysql.gc_delete_range VALUES ("%d", "%d", "%s", "%s", "%d")`

	delBatchSize = 65536
	delBackLog   = 128
)

// enableEmulatorGC means whether to enable emulator GC. The default is enable.
// In some unit tests, we want to stop emulator GC, then wen can set enableEmulatorGC to 0.
var emulatorGCEnable = int32(1)

type delRangeManager interface {
	// addDelRangeJob add a DDL job into gc_delete_range table.
	addDelRangeJob(job *model.Job) error
	// removeFromGCDeleteRange removes the deleting table job from gc_delete_range table by jobID and tableID.
	// It's use for recover the table that was mistakenly deleted.
	removeFromGCDeleteRange(jobID int64, tableID []int64) error
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
func (dr *delRange) addDelRangeJob(job *model.Job) error {
	ctx, err := dr.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer dr.sessPool.put(ctx)

	err = insertJobIntoDeleteRangeTable(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}
	if !dr.storeSupport {
		dr.emulatorCh <- struct{}{}
	}
	logutil.BgLogger().Info("[ddl] add job into delete-range table", zap.Int64("jobID", job.ID), zap.String("jobType", job.Type.String()))
	return nil
}

// removeFromGCDeleteRange implements delRangeManager interface.
func (dr *delRange) removeFromGCDeleteRange(jobID int64, tableIDs []int64) error {
	ctx, err := dr.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer dr.sessPool.put(ctx)
	err = util.RemoveMultiFromGCDeleteRange(ctx, jobID, tableIDs)
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
		if IsEmulatorGCEnable() {
			err := dr.doDelRangeWork()
			logutil.LogErrStack(err)
		}
	}
}

// EmulatorGCEnable enables emulator gc. It exports for testing.
func EmulatorGCEnable() {
	atomic.StoreInt32(&emulatorGCEnable, 1)
}

// EmulatorGCDisable disables emulator gc. It exports for testing.
func EmulatorGCDisable() {
	atomic.StoreInt32(&emulatorGCEnable, 0)
}

// IsEmulatorGCEnable indicates whether emulator GC enabled. It exports for testing.
func IsEmulatorGCEnable() bool {
	return atomic.LoadInt32(&emulatorGCEnable) == 1
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
		err := kv.RunInNewTxn(dr.store, false, func(txn kv.Transaction) error {
			iter, err := txn.Iter(oldStartKey, r.EndKey)
			if err != nil {
				return errors.Trace(err)
			}
			defer iter.Close()

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
			logutil.BgLogger().Info("[ddl] delRange emulator complete task", zap.Int64("jobID", r.JobID), zap.Int64("elementID", r.ElementID))
			break
		}
		if err := util.UpdateDeleteRange(ctx, r, newStartKey, oldStartKey); err != nil {
			logutil.BgLogger().Error("[ddl] delRange emulator update task failed", zap.Error(err))
		}
		oldStartKey = newStartKey
	}
	return nil
}

// insertJobIntoDeleteRangeTable parses the job into delete-range arguments,
// and inserts a new record into gc_delete_range table. The primary key is
// job ID, so we ignore key conflict error.
func insertJobIntoDeleteRangeTable(ctx sessionctx.Context, job *model.Job) error {
	now, err := getNowTSO(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	s := ctx.(sqlexec.SQLExecutor)
	switch job.Type {
	case model.ActionDropSchema:
		var tableIDs []int64
		if err := job.DecodeArgs(&tableIDs); err != nil {
			return errors.Trace(err)
		}
		for _, tableID := range tableIDs {
			startKey := tablecodec.EncodeTablePrefix(tableID)
			endKey := tablecodec.EncodeTablePrefix(tableID + 1)
			if err := doInsert(s, job.ID, tableID, startKey, endKey, now); err != nil {
				return errors.Trace(err)
			}
		}
	case model.ActionDropTable, model.ActionTruncateTable:
		tableID := job.TableID
		// The startKey here is for compatibility with previous versions, old version did not endKey so don't have to deal with.
		var startKey kv.Key
		var physicalTableIDs []int64
		if err := job.DecodeArgs(startKey, &physicalTableIDs); err != nil {
			return errors.Trace(err)
		}
		if len(physicalTableIDs) > 0 {
			for _, pid := range physicalTableIDs {
				startKey = tablecodec.EncodeTablePrefix(pid)
				endKey := tablecodec.EncodeTablePrefix(pid + 1)
				if err := doInsert(s, job.ID, pid, startKey, endKey, now); err != nil {
					return errors.Trace(err)
				}
			}
			return nil
		}
		startKey = tablecodec.EncodeTablePrefix(tableID)
		endKey := tablecodec.EncodeTablePrefix(tableID + 1)
		return doInsert(s, job.ID, tableID, startKey, endKey, now)
	case model.ActionDropTablePartition, model.ActionTruncateTablePartition:
		var physicalTableID int64
		if err := job.DecodeArgs(&physicalTableID); err != nil {
			return errors.Trace(err)
		}
		startKey := tablecodec.EncodeTablePrefix(physicalTableID)
		endKey := tablecodec.EncodeTablePrefix(physicalTableID + 1)
		return doInsert(s, job.ID, physicalTableID, startKey, endKey, now)
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
				if err := doInsert(s, job.ID, indexID, startKey, endKey, now); err != nil {
					return errors.Trace(err)
				}
			}
		} else {
			startKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
			endKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID+1)
			return doInsert(s, job.ID, indexID, startKey, endKey, now)
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
				if err := doInsert(s, job.ID, indexID, startKey, endKey, now); err != nil {
					return errors.Trace(err)
				}
			}
		} else {
			startKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
			endKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID+1)
			return doInsert(s, job.ID, indexID, startKey, endKey, now)
		}
	}
	return nil
}

func doInsert(s sqlexec.SQLExecutor, jobID int64, elementID int64, startKey, endKey kv.Key, ts uint64) error {
	logutil.BgLogger().Info("[ddl] insert into delete-range table", zap.Int64("jobID", jobID), zap.Int64("elementID", elementID))
	startKeyEncoded := hex.EncodeToString(startKey)
	endKeyEncoded := hex.EncodeToString(endKey)
	sql := fmt.Sprintf(insertDeleteRangeSQL, jobID, elementID, startKeyEncoded, endKeyEncoded, ts)
	_, err := s.Execute(context.Background(), sql)
	return errors.Trace(err)
}

// getNowTS gets the current timestamp, in TSO.
func getNowTSO(ctx sessionctx.Context) (uint64, error) {
	currVer, err := ctx.GetStore().CurrentVersion()
	if err != nil {
		return 0, errors.Trace(err)
	}
	return currVer.Ver, nil
}
