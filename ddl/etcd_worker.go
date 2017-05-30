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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	goctx "golang.org/x/net/context"
)

// EtcdWorker is an interface for DDL access the etcd.
type EtcdWorker interface {
	// ID returns the ID of DDL.
	ID() string
	// IsOwner returns whether the worker is the DDL owner.
	IsOwner() bool
	// SetOwner sets whether the worker is the DDL owner.
	SetOwner(isOwner bool)
	// IsOwner returns whether the worker is the background owner.
	IsBgOwner() bool
	// SetOwner sets whether the worker is the background owner.
	SetBgOwner(isOwner bool)
	// CampaignOwners campaigns the DDL owner and the background owner.
	CampaignOwners(ctx goctx.Context, wg *sync.WaitGroup) error

	SchemaSyncer

	// Cancel cancels this etcd worker campaign.
	Cancel()
}

// ChangeOwnerInNewWay is used for controlling the way of changing owner.
// TODO: Remove it.
var ChangeOwnerInNewWay = true

const (
	ddlOwnerKey               = "/tidb/ddl/owner"
	bgOwnerKey                = "/tidb/ddl/bg/owner"
	newSessionDefaultRetryCnt = 3
)

// worker represents the structure which is used for electing owner.
type worker struct {
	*schemaVersionSyncer
	ddlOwner    int32
	bgOwner     int32
	ddlID       string // id is the ID of DDL.
	etcdSession *concurrency.Session
	cancel      goctx.CancelFunc
}

// NewEtcdWorker creates a new EtcdWorker.
func NewEtcdWorker(etcdCli *clientv3.Client, id string, cancel goctx.CancelFunc) EtcdWorker {
	return &worker{
		schemaVersionSyncer: &schemaVersionSyncer{
			etcdCli:           etcdCli,
			selfSchemaVerPath: fmt.Sprintf("%s/%s", ddlAllSchemaVersions, id),
		},
		ddlID:  id,
		cancel: cancel,
	}
}

// ID implements EtcdWorker.ID interface.
func (w *worker) ID() string {
	return w.ddlID
}

// IsOwner implements EtcdWorker.IsOwner interface.
func (w *worker) IsOwner() bool {
	return atomic.LoadInt32(&w.ddlOwner) == 1
}

// SetOwner implements EtcdWorker.SetOwner interface.
func (w *worker) SetOwner(isOwner bool) {
	if isOwner {
		atomic.StoreInt32(&w.ddlOwner, 1)
	} else {
		atomic.StoreInt32(&w.ddlOwner, 0)
	}
}

// Cancel implements EtcdWorker.Cancel interface.
func (w *worker) Cancel() {
	w.cancel()
}

// IsBgOwner implements EtcdWorker.IsBgOwner interface.
func (w *worker) IsBgOwner() bool {
	return atomic.LoadInt32(&w.bgOwner) == 1
}

// SetBgOwner implements EtcdWorker.SetBgOwner interface.
func (w *worker) SetBgOwner(isOwner bool) {
	if isOwner {
		atomic.StoreInt32(&w.bgOwner, 1)
	} else {
		atomic.StoreInt32(&w.bgOwner, 0)
	}
}

func (w *worker) newSession(ctx goctx.Context, retryCnt int) error {
	var err error
	for i := 0; i < retryCnt; i++ {
		w.etcdSession, err = concurrency.NewSession(w.etcdCli, concurrency.WithContext(ctx))
		if err != nil {
			log.Warnf("[ddl] failed to new session, err %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		break
	}
	return errors.Trace(err)
}

// CampaignOwners implements EtcdWorker.CampaignOwners interface.
func (w *worker) CampaignOwners(ctx goctx.Context, wg *sync.WaitGroup) error {
	err := w.newSession(ctx, newSessionDefaultRetryCnt)
	if err != nil {
		return errors.Trace(err)
	}

	wg.Add(2)
	ddlCtx, _ := goctx.WithCancel(ctx)
	go w.campaignLoop(ddlCtx, ddlOwnerKey, wg)

	bgCtx, _ := goctx.WithCancel(ctx)
	go w.campaignLoop(bgCtx, bgOwnerKey, wg)
	return nil
}

func (w *worker) campaignLoop(ctx goctx.Context, key string, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-w.etcdSession.Done():
			// TODO: Create session again?
			log.Warnf("[ddl] etcd session is done.")
		case <-ctx.Done():
			return
		default:
		}

		elec := concurrency.NewElection(w.etcdSession, key)
		err := elec.Campaign(ctx, w.ddlID)
		if err != nil {
			log.Infof("[ddl] worker %s failed to campaign, err %v", w.ddlID, err)
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
		log.Info("[ddl] %s worker is %s, owner is %v", key, w.ddlID, leader)
		if leader == w.ddlID {
			w.setOwnerVal(key, true)
		} else {
			log.Warnf("[ddl] worker %s isn't the owner", w.ddlID)
			continue
		}

		w.watchOwner(ctx, string(resp.Kvs[0].Key))
		w.setOwnerVal(key, false)
	}
}

func (w *worker) setOwnerVal(key string, val bool) {
	if key == ddlOwnerKey {
		w.SetOwner(val)
	} else {
		w.SetBgOwner(val)
	}
}

func (w *worker) watchOwner(ctx goctx.Context, key string) {
	log.Debugf("[ddl] worker %s watch owner key %v", w.ddlID, key)
	watchCh := w.etcdCli.Watch(ctx, key)
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
