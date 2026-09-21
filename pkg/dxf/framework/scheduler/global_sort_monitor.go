// Copyright 2026 PingCAP, Inc.
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

package scheduler

import (
	"context"
	"errors"
	"net/url"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ingestor/globalsort/residual"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/zap"
)

type globalSortURIResolver func(context.Context, kv.Storage) string

type globalSortStoreFactory func(context.Context, string) (storeapi.Storage, error)

func newGlobalSortStore(ctx context.Context, uri string) (storeapi.Storage, error) {
	backend, err := objstore.ParseBackend(uri, nil)
	if err != nil {
		return nil, err
	}
	return objstore.NewWithDefaultOpt(ctx, backend)
}

func (sm *Manager) requestGlobalSortMonitor() {
	if kerneltype.IsClassic() {
		return
	}

	sm.globalSortMonitorMu.Lock()
	defer sm.globalSortMonitorMu.Unlock()
	if sm.globalSortMonitorStopping || sm.ctx.Err() != nil || sm.globalSortMonitorRunning {
		return
	}
	sm.globalSortMonitorRunning = true
	failpoint.InjectCall("beforeGlobalSortResidualMonitorRun")
	sm.wg.Run(func() {
		defer func() {
			sm.globalSortMonitorMu.Lock()
			sm.globalSortMonitorRunning = false
			sm.globalSortMonitorMu.Unlock()
		}()
		failpoint.InjectCall("globalSortResidualMonitorWorker")
		sm.monitorGlobalSort()
	})
}

func (sm *Manager) monitorGlobalSort() {
	if sm.ctx.Err() != nil {
		return
	}

	tasks, err := sm.taskMgr.GetAllTasks(sm.ctx)
	if err != nil {
		if !isGlobalSortMonitorCancellation(err) {
			sm.logger.Warn("global sort residual monitor failed to get all tasks", zap.Error(err))
		}
		return
	}
	if len(tasks) > 0 {
		metrics.GlobalSortResidualDataSize.Set(0)
		return
	}
	if sm.ctx.Err() != nil {
		return
	}

	storageURI := sm.globalSortURIResolver(sm.ctx, sm.store)
	logStorageURI := globalSortStorageLogURI(storageURI)
	var scan residual.Stats
	if storageURI != "" {
		storage, err := sm.globalSortStoreFactory(sm.ctx, storageURI)
		if err != nil {
			if !isGlobalSortMonitorCancellation(err) {
				sm.logger.Warn("global sort residual monitor failed to create storage",
					zap.String("storage-uri", logStorageURI))
			}
			return
		}
		defer storage.Close()

		scan, err = residual.Scan(sm.ctx, storage)
		if err != nil {
			if !isGlobalSortMonitorCancellation(err) {
				sm.logger.Warn("global sort residual monitor failed to scan storage",
					zap.String("storage-uri", logStorageURI))
			}
			return
		}
	}
	if sm.ctx.Err() != nil {
		return
	}

	tasks, err = sm.taskMgr.GetAllTasks(sm.ctx)
	if err != nil {
		if !isGlobalSortMonitorCancellation(err) {
			sm.logger.Warn("global sort residual monitor failed to get all tasks", zap.Error(err))
		}
		return
	}
	if len(tasks) > 0 {
		metrics.GlobalSortResidualDataSize.Set(0)
		sm.logger.Info("global sort residual monitor discarded scan because tasks appeared",
			zap.String("storage-uri", logStorageURI),
			zap.Int("task-count", len(tasks)))
		return
	}
	if sm.ctx.Err() != nil {
		return
	}

	metrics.GlobalSortResidualDataSize.Set(float64(scan.SizeBytes))
	sm.logger.Info("global sort residual monitor success",
		zap.String("storage-uri", logStorageURI),
		zap.Int64("residual-size-bytes", scan.SizeBytes),
		zap.Int64("residual-object-count", scan.ObjectCount),
		zap.Strings("sample-prefixes", scan.SamplePrefixes),
		zap.Bool("sample-prefixes-omitted", scan.SamplePrefixesOmitted))
}

func isGlobalSortMonitorCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func globalSortStorageLogURI(storageURI string) string {
	if _, err := url.Parse(storageURI); err != nil {
		return "<invalid>"
	}
	return ast.RedactURL(storageURI)
}
