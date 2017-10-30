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
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/mock"
)

// newContext gets a context. It is only used for adding column in reorganization state.
func (d *ddl) newContext() context.Context {
	c := mock.NewContext()
	c.Store = d.store
	c.GetSessionVars().SetStatusFlag(mysql.ServerStatusAutocommit, false)
	return c
}

const waitReorgTimeout = 10 * time.Second

func (d *ddl) setReorgRowCount(count int64) {
	atomic.StoreInt64(&d.reorgRowCount, count)
}

func (d *ddl) getReorgRowCount() int64 {
	return atomic.LoadInt64(&d.reorgRowCount)
}

func (d *ddl) runReorgJob(job *model.Job, f func() error) error {
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
		waitTimeout = 50 * time.Millisecond
	}

	// wait reorganization job done or timeout
	select {
	case err := <-d.reorgDoneCh:
		log.Info("[ddl] run reorg job done")
		d.reorgDoneCh = nil
		// Update a job's RowCount.
		job.SetRowCount(d.getReorgRowCount())
		d.setReorgRowCount(0)
		return errors.Trace(err)
	case <-d.quitCh:
		log.Info("[ddl] run reorg job ddl quit")
		d.setReorgRowCount(0)
		// We return errWaitReorgTimeout here too, so that outer loop will break.
		return errWaitReorgTimeout
	case <-time.After(waitTimeout):
		log.Infof("[ddl] run reorg job wait timeout %v", waitTimeout)
		// Update a job's RowCount.
		job.SetRowCount(d.getReorgRowCount())
		// If timeout, we will return, check the owner and retry to wait job done again.
		return errWaitReorgTimeout
	}
}

func (d *ddl) isReorgRunnable() error {
	if d.isClosed() {
		// Worker is closed. So it can't do the reorganizational job.
		return errInvalidWorker.Gen("worker is closed")
	}

	select {
	case <-d.notifyCancelReorgJob:
		// Job is cancelled. So it can't be done.
		return errCancelledDDLJob
	default:
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

	return info, errors.Trace(err)
}

func (r *reorgInfo) UpdateHandle(txn kv.Transaction, handle int64) error {
	t := meta.NewMeta(txn)
	return errors.Trace(t.UpdateDDLReorgHandle(r.Job, handle))
}
