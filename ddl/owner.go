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
	"golang.org/x/net/context"
	goctx "golang.org/x/net/context"
)

const ddlOwnerKey = "/tidb/ddl/owner"
const bgOwnerKey = "/tidb/ddl/bg/owner"

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
		elec := concurrency.NewElection(worker.etcdSession, key)
		ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
		defer cancel()
		err := elec.Campaign(ctx, worker.ddlID)
		if err != nil {
			log.Warnf("[ddl] failed to campaign, err %v", err)
		}
		_, err = elec.Leader(goctx.Background())
		if err != nil {
			// If no leader elected currently, it returns ErrElectionNoLeader.
			log.Warnf("[ddl] failed to get leader, err %v", err)
			continue
		}
		worker.watchOwner(key)

		select {
		case <-d.quitCh:
			break
		case <-worker.etcdSession.Done():
			// TODO: Do something.
		default:
		}
	}
}

func (w *worker) watchOwner(key string) {
	watchCh := w.etcdClient.Watch(goctx.Background(), key)

	for {
		select {
		case resp := <-watchCh:
			if resp.Canceled {
				log.Infof("[ddl] watch owner failed, no owner")
				return
			}

			for _, ev := range resp.Events {
				if ev.Type == mvccpb.DELETE {
					log.Infof("[ddl] owner is deleted")
				}
			}
		}
	}
}

//func (w *worker) sessionLoop() {
//	for {
//		select {
//		case <-w.etcdSession.Done():
//			w.newSession()
//		case <-quitCh:
//			return
//		}
//	}
//}
