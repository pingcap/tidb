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
	"github.com/ngaut/log"
	goctx "golang.org/x/net/context"
)

// NewOwnerChange is used for testing.
var NewOwnerChange = false

const ddlOwnerKey = "/tidb/ddl/owner"
const bgOwnerKey = "/tidb/ddl/bg/owner"

// campaignTimeout is variable for testing.
var campaignTimeout = 5 * time.Second

type worker struct {
	ddlOwner    int32
	bgOwner     int32
	ddlID       string
	quitCh      chan struct{}
	etcdClient  *clientv3.Client
	etcdSession *concurrency.Session
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

func (w *worker) newSession() {
	for {
		session, err := concurrency.NewSession(w.etcdClient)
		if err != nil {
			log.Warnf("[ddl] failed to new session, err %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		w.etcdSession = session
		break
	}
}

func (d *ddl) campaignOwners() {
	d.worker.newSession()
	go d.campaignLoop(ddlOwnerKey)
	d.campaignLoop(bgOwnerKey)
}

func (d *ddl) campaignLoop(key string) {
	defer d.wait.Done()
	worker := d.worker
	for {
		if d.isClosed() {
			return
		}

		elec := concurrency.NewElection(worker.etcdSession, key)
		ctx, cancel := goctx.WithTimeout(goctx.Background(), campaignTimeout)
		defer cancel()
		err := elec.Campaign(ctx, worker.ddlID)
		if err != nil {
			log.Infof("[ddl] worker %s failed to campaign, err %v", worker.ddlID, err)
		}

		// Get owner information.
		resp, err := elec.Leader(goctx.Background())
		if err != nil {
			// If no leader elected currently, it returns ErrElectionNoLeader.
			log.Infof("[ddl] failed to get leader, err %v", err)
			continue
		}
		if len(resp.Kvs) < 1 {
			log.Warnf("[ddl] worker %s watch owner key is empty")
		}
		leader := string(resp.Kvs[0].Value)
		log.Infof("[ddl] %s worker is %s, owner is %v", key, worker.ddlID, leader)
		if leader == worker.ddlID {
			worker.setOwnerVal(key, true)
		}

		// TODO: Use content instead of quitCh.
		worker.watchOwner(d.quitCh, string(resp.Kvs[0].Key))
		worker.setOwnerVal(key, false)
		d.hookMu.Lock()
		d.hook.OnWatched()
		d.hookMu.Unlock()

		select {
		case <-worker.etcdSession.Done():
			// TODO: Create session again?
		default:
		}
	}
}

func (w *worker) setOwnerVal(key string, val bool) {
	if key == ddlOwnerKey {
		w.setOwner(val)
	} else {
		w.setBgOwner(val)
	}
}

func (w *worker) watchOwner(quitCh chan struct{}, key string) {
	log.Debugf("[ddl] worker %s watch owner key %v", w.ddlID, key)
	watchCh := w.etcdClient.Watch(goctx.Background(), key)
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
		case <-quitCh:
			return
		}
	}
}
