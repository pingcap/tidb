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

package util

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/metrics"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Watcher is responsible for watching the etcd path related operations.
type Watcher interface {
	// WatchChan returns the chan for watching etcd path.
	WatchChan() clientv3.WatchChan
	// Watch watches the etcd path.
	Watch(ctx context.Context, etcdCli *clientv3.Client, path string)
	// Rewatch rewatches the etcd path.
	Rewatch(ctx context.Context, etcdCli *clientv3.Client, path string)
}

type watcher struct {
	wCh clientv3.WatchChan
	sync.RWMutex
}

// NewWatcher creates a new watcher.
func NewWatcher() Watcher {
	return &watcher{}
}

// WatchChan implements SyncerWatch.WatchChan interface.
func (w *watcher) WatchChan() clientv3.WatchChan {
	w.RLock()
	defer w.RUnlock()
	return w.wCh
}

// Watch implements SyncerWatch.Watch interface.
func (w *watcher) Watch(ctx context.Context, etcdCli *clientv3.Client, path string) {
	w.Lock()
	w.wCh = etcdCli.Watch(ctx, path)
	w.Unlock()
}

// Rewatch implements SyncerWatch.Rewatch interface.
func (w *watcher) Rewatch(ctx context.Context, etcdCli *clientv3.Client, path string) {
	startTime := time.Now()
	// Make sure the wCh doesn't receive the information of 'close' before we finish the rewatch.
	w.Lock()
	w.wCh = nil
	w.Unlock()

	go func() {
		defer func() {
			metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerRewatch, metrics.RetLabel(nil)).Observe(time.Since(startTime).Seconds())
		}()
		wCh := etcdCli.Watch(ctx, path)

		w.Lock()
		w.wCh = wCh
		w.Unlock()
		logutil.DDLLogger().Info("syncer rewatch global info finished")
	}()
}
