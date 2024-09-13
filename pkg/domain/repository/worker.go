// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package repository

import (
	"context"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(resource pools.Resource)
}

// Worker is the main struct for repository.
type Worker struct {
	etcdClient   *clientv3.Client
	exit         chan struct{}
	sesspool     sessionPool
	cancel       context.CancelFunc
	getGlobalVar func(string) (string, error)
	newOwner     func(string, string) owner.Manager
	owner        owner.Manager
	wg           *util.WaitGroupEnhancedWrapper
}

// NewWorker creates a new repository worker.
func NewWorker(etcdClient *clientv3.Client,
	getGlobalVar func(string) (string, error),
	newOwner func(string, string) owner.Manager,
	sesspool sessionPool, exit chan struct{}) *Worker {
	w := &Worker{
		etcdClient:   etcdClient,
		exit:         exit,
		getGlobalVar: getGlobalVar,
		sesspool:     sesspool,
		newOwner:     newOwner,
		wg:           util.NewWaitGroupEnhancedWrapper("repository", exit, false),
	}
	return w
}

func (w *Worker) startSnapshot(ctx context.Context) func() {
	return func() {
		ticker := time.NewTicker(time.Minute)
		for {
			select {
			case <-w.exit:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				// snapshot thread
			}
		}
	}
}

func (w *Worker) startSample(ctx context.Context) func() {
	return func() {
		ticker := time.NewTicker(time.Minute)
		for {
			select {
			case <-w.exit:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				// sample thread
			}
		}
	}
}

func (w *Worker) getSessionWithRetry() pools.Resource {
	for {
		_sessctx, err := w.sesspool.Get()
		if err != nil {
			logutil.BgLogger().Warn("can not init session for repository")
			time.Sleep(time.Second)
			continue
		}
		return _sessctx
	}
}

func (w *Worker) start(ctx context.Context) func() {
	// TODO: add another txn type
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	return func() {
		w.owner = w.newOwner(ownerKey, promptKey)
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-w.exit:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if w.owner.IsOwner() {
					// create table if not exist
					if err := w.createAllTables(ctx); err != nil {
						logutil.BgLogger().Error("can't create workload repository tables", zap.Error(err), zap.Stack("stack"))
					}
				}
				// check if table exists
				if w.checkTablesExists(ctx) {
					w.wg.RunWithRecover(w.startSample(ctx), func(err interface{}) {
						logutil.BgLogger().Info("sample panic", zap.Any("err", err), zap.Stack("stack"))
					}, "sample")
					w.wg.RunWithRecover(w.startSnapshot(ctx), func(err interface{}) {
						logutil.BgLogger().Info("snapshot panic", zap.Any("err", err), zap.Stack("stack"))
					}, "snapshot")
					w.wg.RunWithRecover(w.startHouseKeeper(ctx), func(err interface{}) {
						logutil.BgLogger().Info("housekeeper panic", zap.Any("err", err), zap.Stack("stack"))
					}, "housekeeper")
					return
				}
			}
		}
	}
}

// Start will start the worker.
func (w *Worker) Start(ctx context.Context) {
	if w.cancel != nil {
		return
	}

	nctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.wg.RunWithRecover(w.start(nctx), func(err interface{}) {
		logutil.BgLogger().Info("prestart panic", zap.Any("err", err), zap.Stack("stack"))
	}, "prestart")
}

// Stop will stop the worker.
func (w *Worker) Stop() {
	if w.owner != nil {
		w.owner.Cancel()
		w.owner = nil
	}
	if w.cancel != nil {
		w.cancel()
		w.cancel = nil
	}
	w.wg.Wait()
}
