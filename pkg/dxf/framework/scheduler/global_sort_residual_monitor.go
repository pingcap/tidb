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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/zap"
)

type globalSortStorageURIResolver func(context.Context, kv.Storage) string

type globalSortStoreFactory func(context.Context, string) (storeapi.Storage, error)

func newGlobalSortStore(ctx context.Context, uri string) (storeapi.Storage, error) {
	backend, err := objstore.ParseBackend(uri, nil)
	if err != nil {
		return nil, err
	}
	return objstore.NewWithDefaultOpt(ctx, backend)
}

func (sm *Manager) requestGlobalSortResidualMonitor() {
	if kerneltype.IsClassic() {
		return
	}

	sm.globalSortResidualMu.Lock()
	defer sm.globalSortResidualMu.Unlock()
	if sm.globalSortResidualStopping || sm.ctx.Err() != nil || sm.globalSortResidualRunning {
		return
	}
	sm.globalSortResidualRunning = true
	failpoint.InjectCall("beforeGlobalSortResidualMonitorRun")
	sm.wg.Run(func() {
		defer func() {
			sm.globalSortResidualMu.Lock()
			sm.globalSortResidualRunning = false
			sm.globalSortResidualMu.Unlock()
		}()
		failpoint.InjectCall("globalSortResidualMonitorWorker")
		sm.monitorGlobalSortResidual()
	})
}

func (sm *Manager) monitorGlobalSortResidual() {
	if sm.ctx.Err() != nil {
		return
	}

	tasks, err := sm.taskMgr.GetAllTasks(sm.ctx)
	if err != nil {
		if !isGlobalSortResidualCancellation(err) {
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

	storageURI := sm.globalSortStorageURIResolver(sm.ctx, sm.store)
	logStorageURI := globalSortStorageLogURI(storageURI)
	var scan globalSortResidualScan
	if storageURI != "" {
		storage, err := sm.globalSortStoreFactory(sm.ctx, storageURI)
		if err != nil {
			if !isGlobalSortResidualCancellation(err) {
				sm.logger.Warn("global sort residual monitor failed to create storage",
					zap.String("storage-uri", logStorageURI))
			}
			return
		}
		defer storage.Close()

		scan, err = scanGlobalSortResidual(sm.ctx, storage)
		if err != nil {
			if !isGlobalSortResidualCancellation(err) {
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
		if !isGlobalSortResidualCancellation(err) {
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

	metrics.GlobalSortResidualDataSize.Set(float64(scan.sizeBytes))
	sm.logger.Info("global sort residual monitor success",
		zap.String("storage-uri", logStorageURI),
		zap.Int64("residual-size-bytes", scan.sizeBytes),
		zap.Int64("residual-object-count", scan.objectCount),
		zap.Strings("sample-prefixes", scan.samplePrefixes),
		zap.Bool("sample-prefixes-omitted", scan.samplePrefixesOmitted))
}

func isGlobalSortResidualCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func globalSortStorageLogURI(storageURI string) string {
	if _, err := url.Parse(storageURI); err != nil {
		return "<invalid>"
	}
	return ast.RedactURL(storageURI)
}
