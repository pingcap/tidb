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
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	insertDeleteRangeSQL   = `INSERT IGNORE INTO mysql.gc_delete_range VALUES ("%d", "%d", "%s", "%s", "%d")`
	loadDeleteRangeSQL     = `SELECT job_id, element_id, start_key, end_key FROM mysql.gc_delete_range WHERE ts < %v ORDER BY ts`
	completeDeleteRangeSQL = `DELETE FROM mysql.gc_delete_range WHERE job_id = %d AND element_id = %d`
	loadSafePointSQL       = `SELECT variable_value FROM mysql.tidb WHERE variable_name = "tikv_gc_safe_point" FOR UPDATE`

	delBatchSize = 65536
	delBackLog   = 128
)

type delRangeManager interface {
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

	err = insertBgJobIntoDeleteRangeTable(ctx, job)
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
		go dr.startEmulator()
	}
}

// clear implements delRangeManager interface.
func (dr *delRange) clear() {
	log.Infof("[ddl] closing delRange session pool")
	dr.ctxPool.Close()
}

func (dr *delRange) startEmulator() {
	log.Infof("[ddl] start delRange emulator")
	dr.d.wait.Add(1)
	defer dr.d.wait.Done()
	for {
		select {
		case <-dr.emulatorCh:
		case <-dr.d.quitCh:
			return
		}
		dr.doDelRangeWork()
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

	ranges, err := LoadDeleteRanges(ctx, math.MaxInt64)
	if err != nil {
		log.Errorf("[dd] delRange emulator load tasks fail: %s", err)
		return errors.Trace(err)
	}

	for _, r := range ranges {
		if err := dr.doTask(r); err != nil {
			log.Errorf("[ddl] delRange emulator do task fail: %s", err)
			return errors.Trace(err)
		}
		if err := CompleteDeleteRange(ctx, r); err != nil {
			log.Errorf("[ddl] delRange emulator complete task fail: %s", err)
			return errors.Trace(err)
		}
		log.Infof("[ddl] delRange emulator complete task: (%d, %d)", r.jobID, r.elementID)
	}
	return nil
}

func (dr *delRange) doTask(r DelRangeTask) error {
	for {
		finish := true
		dr.keys = dr.keys[:0]
		err := kv.RunInNewTxn(dr.d.store, false, func(txn kv.Transaction) error {
			iter, err := txn.Seek(r.startKey)
			if err != nil {
				return errors.Trace(err)
			}
			defer iter.Close()

			for i := 0; i < delBatchSize; i++ {
				if iter.Valid() {
					finish = bytes.Compare(iter.Key(), r.endKey) >= 0
					if !finish {
						dr.keys = append(dr.keys, iter.Key().Clone())
						if err := iter.Next(); err != nil {
							return errors.Trace(err)
						}
						continue
					}
				}
				break
			}
			for _, key := range dr.keys {
				err := txn.Delete(key)
				if err != nil && !terror.ErrorEqual(err, kv.ErrNotExist) {
					return errors.Trace(err)
				}
			}
			return nil
		})
		if err != nil {
			return errors.Trace(err)
		}
		if finish {
			break
		}
	}
	return nil
}

// insertBgJobIntoDeleteRangeTable parses the job into delete-range arguments,
// and inserts a new record into gc_delete_range table. The primary key is
// job ID, so we ignore key conflict error.
func insertBgJobIntoDeleteRangeTable(ctx context.Context, job *model.Job) error {
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
			if err := doInsert(s, job.ID, tableID, startKey, endKey, time.Now().Unix()); err != nil {
				return errors.Trace(err)
			}
		}
	case model.ActionDropTable, model.ActionTruncateTable:
		tableID := job.TableID
		startKey := tablecodec.EncodeTablePrefix(tableID)
		endKey := tablecodec.EncodeTablePrefix(tableID + 1)
		return doInsert(s, job.ID, tableID, startKey, endKey, time.Now().Unix())
	case model.ActionDropIndex:
		tableID := job.TableID
		var indexName interface{}
		var indexID int64
		if err := job.DecodeArgs(&indexName, &indexID); err != nil {
			return errors.Trace(err)
		}
		startKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
		endKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID+1)
		return doInsert(s, job.ID, indexID, startKey, endKey, time.Now().Unix())
	}
	return nil
}

func doInsert(s sqlexec.SQLExecutor, jobID int64, elementID int64, startKey, endKey kv.Key, ts int64) error {
	log.Infof("[ddl] insert into delete-range table with key: (%d,%d)", jobID, elementID)
	startKeyEncoded := hex.EncodeToString(startKey)
	endKeyEncoded := hex.EncodeToString(endKey)
	sql := fmt.Sprintf(insertDeleteRangeSQL, jobID, elementID, startKeyEncoded, endKeyEncoded, ts)
	_, err := s.Execute(sql)
	return errors.Trace(err)
}

// DelRangeTask is for run delete-range command in gc_worker.
type DelRangeTask struct {
	jobID, elementID int64
	startKey, endKey []byte
}

// Range returns the range [start, end) to delete.
func (t DelRangeTask) Range() ([]byte, []byte) {
	return t.startKey, t.endKey
}

// LoadDeleteRanges loads delete range tasks from gc_delete_range table.
func LoadDeleteRanges(ctx context.Context, safePoint uint64) (ranges []DelRangeTask, _ error) {
	sql := fmt.Sprintf(loadDeleteRangeSQL, safePoint)
	rss, err := ctx.(sqlexec.SQLExecutor).Execute(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rs := rss[0]
	for {
		row, err := rs.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			break
		}
		startKey, err := hex.DecodeString(row.Data[2].GetString())
		if err != nil {
			return nil, errors.Trace(err)
		}
		endKey, err := hex.DecodeString(row.Data[3].GetString())
		if err != nil {
			return nil, errors.Trace(err)
		}
		ranges = append(ranges, DelRangeTask{
			jobID:     row.Data[0].GetInt64(),
			elementID: row.Data[1].GetInt64(),
			startKey:  startKey,
			endKey:    endKey,
		})
	}
	return ranges, nil
}

// CompleteDeleteRange deletes a record from gc_delete_range table.
// NOTE: This function WILL NOT start and run in a new transaction internally.
func CompleteDeleteRange(ctx context.Context, dr DelRangeTask) error {
	sql := fmt.Sprintf(completeDeleteRangeSQL, dr.jobID, dr.elementID)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(sql)
	return errors.Trace(err)
}

// LoadPendingBgJobsIntoDeleteTable loads all pending DDL backgroud jobs
// into table `gc_delete_range` so that gc worker can process them.
// NOTE: This function WILL NOT start and run in a new transaction internally.
func LoadPendingBgJobsIntoDeleteTable(ctx context.Context) (err error) {
	log.Infof("[ddl] loading pending backgroud DDL jobs")
	var met = meta.NewMeta(ctx.Txn())
	for {
		var job *model.Job
		job, err = met.DeQueueBgJob()
		if err != nil || job == nil {
			break
		}
		err = insertBgJobIntoDeleteRangeTable(ctx, job)
		if err != nil {
			break
		}
	}
	return errors.Trace(err)
}

type mockDelRange struct {
}

// newMockDelRangeManager creates a mock delRangeManager only used for test.
func newMockDelRangeManager() delRangeManager {
	return &mockDelRange{}
}

// addDelRangeJob implements delRangeManager interface.
func (dr *mockDelRange) addDelRangeJob(job *model.Job) error {
	return nil
}

// start implements delRangeManager interface.
func (dr *mockDelRange) start() {
	return
}

// clear implements delRangeManager interface.
func (dr *mockDelRange) clear() {
	return
}
