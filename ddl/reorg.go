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
	"math"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/mock"
	log "github.com/sirupsen/logrus"
)

// reorgCtx is for reorganization.
type reorgCtx struct {
	// doneCh is used to notify.
	// If the reorganization job is done, we will use this channel to notify outer.
	// TODO: Now we use goroutine to simulate reorganization jobs, later we may
	// use a persistent job list.
	doneCh chan error
	// rowCount is used to simulate a job's row count.
	rowCount int64
	// notifyCancelReorgJob is used to notify the backfilling goroutine if the DDL job is cancelled.
	// 0: job is not canceled.
	// 1: job is canceled.
	notifyCancelReorgJob int32
	// doneHandle is used to simulate the handle that has been processed.
	doneHandle int64
}

// newContext gets a context. It is only used for adding column in reorganization state.
func (d *ddl) newContext() sessionctx.Context {
	c := mock.NewContext()
	c.Store = d.store
	c.GetSessionVars().SetStatusFlag(mysql.ServerStatusAutocommit, false)
	c.GetSessionVars().StmtCtx.TimeZone = time.UTC
	return c
}

const defaultWaitReorgTimeout = 10 * time.Second

// ReorgWaitTimeout is the timeout that wait ddl in write reorganization stage.
var ReorgWaitTimeout = 1 * time.Second

func (rc *reorgCtx) notifyReorgCancel() {
	atomic.StoreInt32(&rc.notifyCancelReorgJob, 1)
}

func (rc *reorgCtx) cleanNotifyReorgCancel() {
	atomic.StoreInt32(&rc.notifyCancelReorgJob, 0)
}

func (rc *reorgCtx) isReorgCanceled() bool {
	return atomic.LoadInt32(&rc.notifyCancelReorgJob) == 1
}

func (rc *reorgCtx) setRowCount(count int64) {
	atomic.StoreInt64(&rc.rowCount, count)
}

func (rc *reorgCtx) setNextHandle(doneHandle int64) {
	atomic.StoreInt64(&rc.doneHandle, doneHandle)
}

func (rc *reorgCtx) increaseRowCount(count int64) {
	atomic.AddInt64(&rc.rowCount, count)
}

func (rc *reorgCtx) getRowCountAndHandle() (int64, int64) {
	row := atomic.LoadInt64(&rc.rowCount)
	handle := atomic.LoadInt64(&rc.doneHandle)
	return row, handle
}

func (rc *reorgCtx) clean() {
	rc.setRowCount(0)
	rc.setNextHandle(0)
	rc.doneCh = nil
}

func (d *ddl) runReorgJob(t *meta.Meta, reorgInfo *reorgInfo, f func() error) error {
	job := reorgInfo.Job
	if d.reorgCtx.doneCh == nil {
		// start a reorganization job
		d.wait.Add(1)
		d.reorgCtx.doneCh = make(chan error, 1)
		// initial reorgCtx
		d.reorgCtx.setRowCount(job.GetRowCount())
		d.reorgCtx.setNextHandle(reorgInfo.Handle)
		go func() {
			defer d.wait.Done()
			d.reorgCtx.doneCh <- f()
		}()
	}

	waitTimeout := defaultWaitReorgTimeout
	// if d.lease is 0, we are using a local storage,
	// and we can wait the reorganization to be done here.
	// if d.lease > 0, we don't need to wait here because
	// we should update some job's progress context and try checking again,
	// so we use a very little timeout here.
	if d.lease > 0 {
		waitTimeout = ReorgWaitTimeout
	}

	// wait reorganization job done or timeout
	select {
	case err := <-d.reorgCtx.doneCh:
		rowCount, _ := d.reorgCtx.getRowCountAndHandle()
		log.Infof("[ddl] run reorg job done, handled %d rows", rowCount)
		// Update a job's RowCount.
		job.SetRowCount(rowCount)
		d.reorgCtx.clean()
		return errors.Trace(err)
	case <-d.quitCh:
		log.Info("[ddl] run reorg job ddl quit")
		d.reorgCtx.setNextHandle(0)
		d.reorgCtx.setRowCount(0)
		// We return errWaitReorgTimeout here too, so that outer loop will break.
		return errWaitReorgTimeout
	case <-time.After(waitTimeout):
		rowCount, doneHandle := d.reorgCtx.getRowCountAndHandle()
		// Update a job's RowCount.
		job.SetRowCount(rowCount)
		// Update a reorgInfo's handle.
		err := t.UpdateDDLReorgHandle(job, doneHandle)
		log.Infof("[ddl] run reorg job wait timeout %v, handled %d rows, current done handle %d, err %v", waitTimeout, rowCount, doneHandle, err)
		// If timeout, we will return, check the owner and retry to wait job done again.
		return errWaitReorgTimeout
	}
}

func (d *ddl) isReorgRunnable() error {
	if d.isClosed() {
		// Worker is closed. So it can't do the reorganizational job.
		return errInvalidWorker.Gen("worker is closed")
	}

	if d.reorgCtx.isReorgCanceled() {
		// Job is cancelled. So it can't be done.
		return errCancelledDDLJob
	}

	if !d.isOwner() {
		// If it's not the owner, we will try later, so here just returns an error.
		log.Infof("[ddl] the %s not the job owner", d.uuid)
		return errors.Trace(errNotOwner)
	}
	return nil
}

type reorgInfo struct {
	*model.Job
	Handle int64
	d      *ddl
	first  bool
}

var gofailOnceGuard bool

func (d *ddl) getReorgInfo(t *meta.Meta, job *model.Job, tbl table.Table) (*reorgInfo, error) {
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

		// Get the first handle of this table.
		err = iterateSnapshotRows(d.store, tbl, ver.Ver, math.MinInt64,
			func(h int64, rowKey kv.Key, rawRecord []byte) (bool, error) {
				info.Handle = h
				return false, nil
			})
		if err != nil {
			return info, errors.Trace(err)
		}
		// gofail: var errorUpdateReorgHandle bool
		// if errorUpdateReorgHandle && !gofailOnceGuard {
		//  // only return error once.
		//	gofailOnceGuard = true
		// 	return info, errors.New("occur an error when update reorg handle.")
		// }
		err = t.UpdateDDLReorgHandle(job, info.Handle)
		if err != nil {
			return info, errors.Trace(err)
		}

		job.SnapshotVer = ver.Ver
	} else {
		info.Handle, err = t.GetDDLReorgHandle(job)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return info, errors.Trace(err)
}

func (r *reorgInfo) UpdateHandle(txn kv.Transaction, handle int64) error {
	t := meta.NewMeta(txn)
	return errors.Trace(t.UpdateDDLReorgHandle(r.Job, handle))
}
