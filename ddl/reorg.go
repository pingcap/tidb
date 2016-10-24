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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/terror"
)

var _ context.Context = &reorgContext{}

// reorgContext implements context.Context interface for reorganization use.
type reorgContext struct {
	store kv.Storage
	m     map[fmt.Stringer]interface{}
	txn   kv.Transaction
}

func (c *reorgContext) GetTxn(forceNew bool) (kv.Transaction, error) {
	if forceNew {
		if c.txn != nil {
			if err := c.txn.Commit(); err != nil {
				return nil, errors.Trace(err)
			}
			c.txn = nil
		}
	}

	if c.txn != nil {
		return c.txn, nil
	}

	txn, err := c.store.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}

	c.txn = txn
	return c.txn, nil
}

func (c *reorgContext) finishTxn(rollback bool) error {
	if c.txn == nil {
		return nil
	}

	var err error
	if rollback {
		err = c.txn.Rollback()
	} else {
		err = c.txn.Commit()
	}

	c.txn = nil

	return errors.Trace(err)
}

func (c *reorgContext) RollbackTxn() error {
	return c.finishTxn(true)
}

func (c *reorgContext) CommitTxn() error {
	return c.finishTxn(false)
}

func (c *reorgContext) GetClient() kv.Client {
	return c.store.GetClient()
}

func (c *reorgContext) SetValue(key fmt.Stringer, value interface{}) {
	c.m[key] = value
}

func (c *reorgContext) Value(key fmt.Stringer) interface{} {
	return c.m[key]
}

func (c *reorgContext) ClearValue(key fmt.Stringer) {
	delete(c.m, key)
}

func (d *ddl) newReorgContext() context.Context {
	c := &reorgContext{
		store: d.store,
		m:     make(map[fmt.Stringer]interface{}),
	}

	return c
}

const waitReorgTimeout = 10 * time.Second

func (d *ddl) runReorgJob(f func() error) error {
	if d.reorgDoneCh == nil {
		// start a reorganization job
		d.wait.Add(1)
		d.reorgDoneCh = make(chan error, 1)
		go func() {
			defer d.wait.Done()
			d.reorgDoneCh <- f()
		}()
	}

	waitTimeout := waitReorgTimeout
	// if d.lease is 0, we are using a local storage,
	// and we can wait the reorganization to be done here.
	// if d.lease > 0, we don't need to wait here because
	// we will wait 2 * lease outer and try checking again,
	// so we use a very little timeout here.
	if d.lease > 0 {
		waitTimeout = 1 * time.Millisecond
	}

	// wait reorganization job done or timeout
	select {
	case err := <-d.reorgDoneCh:
		log.Info("[ddl] run reorg job done")
		d.reorgDoneCh = nil
		return errors.Trace(err)
	case <-d.quitCh:
		log.Info("[ddl] run reorg job ddl quit")
		// we return errWaitReorgTimeout here too, so that outer loop will break.
		return errWaitReorgTimeout
	case <-time.After(waitTimeout):
		log.Infof("[ddl] run reorg job wait timeout :%v", waitTimeout)
		// if timeout, we will return, check the owner and retry to wait job done again.
		return errWaitReorgTimeout
	}
}

func (d *ddl) isReorgRunnable(txn kv.Transaction, flag JobType) error {
	if d.isClosed() {
		// worker is closed, can't run reorganization.
		return errors.Trace(errInvalidWorker.Gen("worker is closed"))
	}

	t := meta.NewMeta(txn)
	owner, err := d.getJobOwner(t, flag)
	if err != nil {
		return errors.Trace(err)
	}
	if owner == nil || owner.OwnerID != d.uuid {
		// if no owner, we will try later, so here just return error.
		// or another server is owner, return error too.
		log.Infof("[ddl] %s job, self id %s owner %s, txnTS:%d", flag, d.uuid, owner, txn.StartTS())
		return errors.Trace(errNotOwner)
	}

	return nil
}

// delKeysWithPrefix deletes keys with prefix key in a limited number. If limit < 0, deletes all keys.
func (d *ddl) delKeysWithPrefix(prefix kv.Key, jobType JobType, job *model.Job, limit int) (int, error) {
	batch := limit
	if batch == 0 {
		return 0, nil
	} else if batch < 0 {
		batch = defaultBatchSize
	}
	delAll := limit < 0

	var count int
	total := job.GetRowCount()
	for {
		startTS := time.Now()
		keys := make([]kv.Key, 0, batch)
		err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
			if err1 := d.isReorgRunnable(txn, jobType); err1 != nil {
				return errors.Trace(err1)
			}

			iter, err := txn.Seek(prefix)
			if err != nil {
				return errors.Trace(err)
			}
			defer iter.Close()

			for i := 0; i < batch; i++ {
				if iter.Valid() && iter.Key().HasPrefix(prefix) {
					keys = append(keys, iter.Key().Clone())
					err = iter.Next()
					if err != nil {
						return errors.Trace(err)
					}
				} else {
					break
				}
			}

			for _, key := range keys {
				err := txn.Delete(key)
				// must skip ErrNotExist
				// if key doesn't exist, skip this error.
				if err != nil && !terror.ErrorEqual(err, kv.ErrNotExist) {
					return errors.Trace(err)
				}
			}

			count += len(keys)
			total += int64(len(keys))
			return nil
		})
		sub := time.Since(startTS).Seconds()
		if err != nil {
			log.Warnf("[ddl] deleted %v keys with prefix %q failed, take time %v", total, prefix, sub)
			return 0, errors.Trace(err)
		}

		job.SetRowCount(total)
		batchHandleDataHistogram.WithLabelValues(batchDelData).Observe(sub)
		log.Infof("[ddl] deleted %v keys with prefix %q take time %v", total, prefix, sub)

		// delete keys number less than batch, return.
		if len(keys) < batch {
			break
		}
		if !delAll {
			break
		}
	}

	return count, nil
}

// addFinishInfo adds schema version and information (schema or table information) that are used for binlog.
// dbInfo is added in the following operations: create database, drop database.
// And the other operations add tblInfo.
func addFinishInfo(job *model.Job, ver int64, dbInfo *model.DBInfo, tblInfo *model.TableInfo) {
	if dbInfo == nil {
		job.Args = []interface{}{ver, tblInfo}
		return
	}
	job.Args = []interface{}{ver, dbInfo}
}

type reorgInfo struct {
	*model.Job
	Handle int64
	d      *ddl
	first  bool
}

func (d *ddl) getReorgInfo(t *meta.Meta, job *model.Job) (*reorgInfo, error) {
	var err error

	info := &reorgInfo{
		Job:   job,
		d:     d,
		first: job.SnapshotVer == 0,
	}

	if info.first {
		// get the current version for reorganization if we don't have
		var ver kv.Version
		ver, err = d.store.CurrentVersion()
		if err != nil {
			return nil, errors.Trace(err)
		} else if ver.Ver <= 0 {
			return nil, errInvalidStoreVer.Gen("invalid storage current version %d", ver.Ver)
		}

		job.SnapshotVer = ver.Ver
	} else {
		info.Handle, err = t.GetDDLReorgHandle(job)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if info.Handle > 0 {
		// we have already handled this handle, so use next
		info.Handle++
	}

	return info, errors.Trace(err)
}

func (r *reorgInfo) UpdateHandle(txn kv.Transaction, handle int64) error {
	t := meta.NewMeta(txn)
	return errors.Trace(t.UpdateDDLReorgHandle(r.Job, handle))
}
