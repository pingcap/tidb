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
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	goctx "golang.org/x/net/context"
)

// ChangeOwnerInNewWay is used for testing.
var ChangeOwnerInNewWay = false

const (
	ddlOwnerKey               = "/tidb/ddl/owner"
	bgOwnerKey                = "/tidb/ddl/bg/owner"
	newSessionDefaultRetryCnt = 3
)

// worker represents the structure which is used for electing owner.
type worker struct {
	ddlOwner    int32
	bgOwner     int32
	ddlID       string
	etcdClient  *clientv3.Client
	etcdSession *concurrency.Session
	cancel      goctx.CancelFunc
}

func (w *worker) isOwner() bool {
	return atomic.LoadInt32(&w.ddlOwner) == 1
}

func (w *worker) setOwner(isOwner bool) {
	if isOwner {
		atomic.StoreInt32(&w.ddlOwner, 1)
	} else {
		atomic.StoreInt32(&w.ddlOwner, 0)
	}
}

func (w *worker) isBgOwner() bool {
	return atomic.LoadInt32(&w.bgOwner) == 1
}

func (w *worker) setBgOwner(isOwner bool) {
	if isOwner {
		atomic.StoreInt32(&w.bgOwner, 1)
	} else {
		atomic.StoreInt32(&w.bgOwner, 0)
	}
}

func (w *worker) newSession(ctx goctx.Context, retryCnt int) error {
	var err error
	for i := 0; i < retryCnt; i++ {
		w.etcdSession, err = concurrency.NewSession(w.etcdClient, concurrency.WithContext(ctx))
		if err != nil {
			log.Warnf("[ddl] failed to new session, err %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		break
	}
	return errors.Trace(err)
}

func (d *ddl) campaignOwners(ctx goctx.Context) error {
	err := d.worker.newSession(ctx, newSessionDefaultRetryCnt)
	if err != nil {
		return errors.Trace(err)
	}

	d.wait.Add(2)
	ddlCtx, _ := goctx.WithCancel(ctx)
	go d.campaignLoop(ddlCtx, ddlOwnerKey)

	bgCtx, _ := goctx.WithCancel(ctx)
	go d.campaignLoop(bgCtx, bgOwnerKey)
	return nil
}

func (d *ddl) campaignLoop(ctx goctx.Context, key string) {
	defer d.wait.Done()
	worker := d.worker
	for {
		select {
		case <-worker.etcdSession.Done():
			// TODO: Create session again?
			log.Warnf("etcd session is done.")
		case <-ctx.Done():
			return
		default:
		}

		elec := concurrency.NewElection(worker.etcdSession, key)
		err := elec.Campaign(ctx, worker.ddlID)
		if err != nil {
			log.Infof("[ddl] worker %s failed to campaign, err %v", worker.ddlID, err)
			continue
		}

		// Get owner information.
		resp, err := elec.Leader(ctx)
		if err != nil {
			// If no leader elected currently, it returns ErrElectionNoLeader.
			log.Infof("[ddl] failed to get leader, err %v", err)
			continue
		}
		leader := string(resp.Kvs[0].Value)
		log.Info("[ddl] %s worker is %s, owner is %v", key, worker.ddlID, leader)
		if leader == worker.ddlID {
			worker.setOwnerVal(key, true)
		} else {
			log.Warnf("[ddl] worker %s isn't the owner", worker.ddlID)
			continue
		}

		// TODO: Use content instead of quitCh.
		worker.watchOwner(ctx, string(resp.Kvs[0].Key))
		worker.setOwnerVal(key, false)
		d.hookMu.Lock()
		d.hook.OnWatched(ctx)
		d.hookMu.Unlock()
	}
}

func (w *worker) setOwnerVal(key string, val bool) {
	if key == ddlOwnerKey {
		w.setOwner(val)
	} else {
		w.setBgOwner(val)
	}
}

func (w *worker) watchOwner(ctx goctx.Context, key string) {
	log.Debugf("[ddl] worker %s watch owner key %v", w.ddlID, key)
	watchCh := w.etcdClient.Watch(ctx, key)
	for {
		select {
		case resp := <-watchCh:
			if resp.Canceled {
				log.Infof("[ddl] worker %s watch owner key %v failed, no owner",
					w.ddlID, key)
				return
			}

			for _, ev := range resp.Events {
				if ev.Type == mvccpb.DELETE {
					log.Infof("[ddl] worker %s watch owner key %v failed, owner is deleted", w.ddlID, key)
					return
				}
			}
		case <-w.etcdSession.Done():
			return
		case <-ctx.Done():
			return
		}
	}
}
