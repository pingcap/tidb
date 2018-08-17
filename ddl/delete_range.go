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
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/sqlexec"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

const (
	insertDeleteRangeSQL = `INSERT IGNORE INTO mysql.gc_delete_range VALUES ("%d", "%d", "%s", "%s", "%d")`

	delBatchSize = 65536
	delBackLog   = 128
)

type delRangeManager interface {
	// addDelRangeJob add a DDL job into gc_delete_range table.
	addDelRangeJob(job *model.Job) error
	start()
	clear()
}

type delRange struct {
	store        kv.Storage
	ctxPool      *pools.ResourcePool
	storeSupport bool
	emulatorCh   chan struct{}
	keys         []kv.Key
	quitCh       chan struct{}

	wait sync.WaitGroup // wait is only used when storeSupport is false.
}

// newDelRangeManager returns a delRangeManager.
func newDelRangeManager(store kv.Storage, ctxPool *pools.ResourcePool) delRangeManager {
	dr := &delRange{
		store:        store,
		ctxPool:      ctxPool,
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
	resource, err := dr.ctxPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer dr.ctxPool.Put(resource)
	ctx := resource.(sessionctx.Context)
	ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusAutocommit, true)
	ctx.GetSessionVars().InRestrictedSQL = true

	err = insertJobIntoDeleteRangeTable(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}
	if !dr.storeSupport {
		dr.emulatorCh <- struct{}{}
	}
	log.Infof("[ddl] add job (%d,%s) into delete-range table", job.ID, job.Type.String())
	return nil
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
	log.Infof("[ddl] closing delRange session pool")
	close(dr.quitCh)
	dr.wait.Wait()
	dr.ctxPool.Close()
}

// startEmulator is only used for those storage engines which don't support
// delete-range. The emulator fetches records from gc_delete_range table and
// deletes all keys in each DelRangeTask.
func (dr *delRange) startEmulator() {
	defer dr.wait.Done()
	log.Infof("[ddl] start delRange emulator")
	for {
		select {
		case <-dr.emulatorCh:
		case <-dr.quitCh:
			return
		}
		err := dr.doDelRangeWork()
		terror.Log(errors.Trace(err))
	}
}

func (dr *delRange) doDelRangeWork() error {
	resource, err := dr.ctxPool.Get()
	if err != nil {
		log.Errorf("[ddl] delRange emulator get session fail: %s", err)
		return errors.Trace(err)
	}
	defer dr.ctxPool.Put(resource)
	ctx := resource.(sessionctx.Context)
	ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusAutocommit, true)
	ctx.GetSessionVars().InRestrictedSQL = true

	ranges, err := util.LoadDeleteRanges(ctx, math.MaxInt64)
	if err != nil {
		log.Errorf("[dd] delRange emulator load tasks fail: %s", err)
		return errors.Trace(err)
	}

	for _, r := range ranges {
		if err := dr.doTask(ctx, r); err != nil {
			log.Errorf("[ddl] delRange emulator do task fail: %s", err)
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
			iter, err := txn.Seek(oldStartKey)
			if err != nil {
				return errors.Trace(err)
			}
			defer iter.Close()

			for i := 0; i < delBatchSize; i++ {
				if !iter.Valid() {
					break
				}
				finish = bytes.Compare(iter.Key(), r.EndKey) >= 0
				if finish {
					break
				}
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
				log.Errorf("[ddl] delRange emulator complete task fail: %s", err)
				return errors.Trace(err)
			}
			log.Infof("[ddl] delRange emulator complete task: (%d, %d)", r.JobID, r.ElementID)
			break
		}
		if err := util.UpdateDeleteRange(ctx, r, newStartKey, oldStartKey); err != nil {
			log.Errorf("[ddl] delRange emulator update task fail: %s", err)
		}
		oldStartKey = newStartKey
	}
	return nil
}

// insertJobIntoDeleteRangeTable parses the job into delete-range arguments,
// and inserts a new record into gc_delete_range table. The primary key is
// job ID, so we ignore key conflict error.
func insertJobIntoDeleteRangeTable(ctx sessionctx.Context, job *model.Job) error {
	now, err := getNowTS(ctx)
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
	case model.ActionDropTablePartition:
		var physicalTableID int64
		if err := job.DecodeArgs(&physicalTableID); err != nil {
			return errors.Trace(err)
		}
		startKey := tablecodec.EncodeTablePrefix(physicalTableID)
		endKey := tablecodec.EncodeTablePrefix(physicalTableID + 1)
		return doInsert(s, job.ID, physicalTableID, startKey, endKey, now)
	// ActionAddIndex needs do it, because it needs to be rolled back when it's canceled.
	case model.ActionAddIndex:
		tableID := job.TableID
		var indexID int64
		if err := job.DecodeArgs(&indexID); err != nil {
			return errors.Trace(err)
		}
		startKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
		endKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID+1)
		return doInsert(s, job.ID, indexID, startKey, endKey, now)
	case model.ActionDropIndex:
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

func doInsert(s sqlexec.SQLExecutor, jobID int64, elementID int64, startKey, endKey kv.Key, ts int64) error {
	log.Infof("[ddl] insert into delete-range table with key: (%d,%d)", jobID, elementID)
	startKeyEncoded := hex.EncodeToString(startKey)
	endKeyEncoded := hex.EncodeToString(endKey)
	sql := fmt.Sprintf(insertDeleteRangeSQL, jobID, elementID, startKeyEncoded, endKeyEncoded, ts)
	_, err := s.Execute(context.Background(), sql)
	return errors.Trace(err)
}

// getNowTS gets the current timestamp, in second.
func getNowTS(ctx sessionctx.Context) (int64, error) {
	currVer, err := ctx.GetStore().CurrentVersion()
	if err != nil {
		return 0, errors.Trace(err)
	}
	physical := oracle.ExtractPhysical(currVer.Ver)
	return physical / 1e3, nil
}
