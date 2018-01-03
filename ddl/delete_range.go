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

	"github.com/juju/errors"
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/sqlexec"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
)

const (
	insertDeleteRangeSQL = `INSERT IGNORE INTO mysql.gc_delete_range VALUES ("%d", "%d", "%s", "%s", "%d")`

	delBatchSize int = 65536
	delBackLog       = 128
)

type delRangeManager interface {
	// addDelRangeJob add a DDL job into gc_delete_range table.
	addDelRangeJob(job *model.Job) error
	start()
	clear()
}

type delRange struct {
	d            *ddl
	ctxPool      *pools.ResourcePool
	storeSupport bool
	emulatorCh   chan struct{}
	keys         []kv.Key
}

// newDelRangeManager returns a delRangeManager.
func newDelRangeManager(d *ddl, ctxPool *pools.ResourcePool, supportDelRange bool) delRangeManager {
	dr := &delRange{
		d:            d,
		ctxPool:      ctxPool,
		storeSupport: supportDelRange,
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
	ctx := resource.(context.Context)
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
		dr.d.wait.Add(1)
		go dr.startEmulator()
	}
}

// clear implements delRangeManager interface.
func (dr *delRange) clear() {
	log.Infof("[ddl] closing delRange session pool")
	dr.ctxPool.Close()
}

// startEmulator is only used for those storage engines which don't support
// delete-range. The emulator fetches records from gc_delete_range table and
// deletes all keys in each DelRangeTask.
func (dr *delRange) startEmulator() {
	defer dr.d.wait.Done()
	log.Infof("[ddl] start delRange emulator")
	for {
		select {
		case <-dr.emulatorCh:
		case <-dr.d.quitCh:
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
	ctx := resource.(context.Context)
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

func (dr *delRange) doTask(ctx context.Context, r util.DelRangeTask) error {
	var oldStartKey, newStartKey kv.Key
	oldStartKey = r.StartKey
	for {
		finish := true
		dr.keys = dr.keys[:0]
		err := kv.RunInNewTxn(dr.d.store, false, func(txn kv.Transaction) error {
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
		} else {
			if err := util.UpdateDeleteRange(ctx, r, newStartKey, oldStartKey); err != nil {
				log.Errorf("[ddl] delRange emulator update task fail: %s", err)
			}
			oldStartKey = newStartKey
		}
	}
	return nil
}

// insertJobIntoDeleteRangeTable parses the job into delete-range arguments,
// and inserts a new record into gc_delete_range table. The primary key is
// job ID, so we ignore key conflict error.
func insertJobIntoDeleteRangeTable(ctx context.Context, job *model.Job) error {
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
		startKey := tablecodec.EncodeTablePrefix(tableID)
		endKey := tablecodec.EncodeTablePrefix(tableID + 1)
		return doInsert(s, job.ID, tableID, startKey, endKey, now)
	case model.ActionDropIndex:
		tableID := job.TableID
		var indexName interface{}
		var indexID int64
		if err := job.DecodeArgs(&indexName, &indexID); err != nil {
			return errors.Trace(err)
		}
		startKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
		endKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID+1)
		return doInsert(s, job.ID, indexID, startKey, endKey, now)
	}
	return nil
}

func doInsert(s sqlexec.SQLExecutor, jobID int64, elementID int64, startKey, endKey kv.Key, ts int64) error {
	log.Infof("[ddl] insert into delete-range table with key: (%d,%d)", jobID, elementID)
	startKeyEncoded := hex.EncodeToString(startKey)
	endKeyEncoded := hex.EncodeToString(endKey)
	sql := fmt.Sprintf(insertDeleteRangeSQL, jobID, elementID, startKeyEncoded, endKeyEncoded, ts)
	_, err := s.Execute(goctx.Background(), sql)
	return errors.Trace(err)
}

// getNowTS gets the current timestamp, in second.
func getNowTS(ctx context.Context) (int64, error) {
	currVer, err := ctx.GetStore().CurrentVersion()
	if err != nil {
		return 0, errors.Trace(err)
	}
	physical := oracle.ExtractPhysical(currVer.Ver)
	return physical / 1e3, nil
}
