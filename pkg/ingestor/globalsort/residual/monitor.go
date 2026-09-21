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

package residual

import (
	"context"
	"errors"
	"net/url"
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/zap"
)

// Config configures a residual Monitor.
type Config struct {
	Enabled    bool
	TaskCount  func(context.Context) (int, error)
	StorageURI func(context.Context) string
	Logger     *zap.Logger
}

type storeFactory func(context.Context, string) (storeapi.Storage, error)

// Monitor scans residual global-sort objects and publishes their size.
type Monitor struct {
	ctx          context.Context
	cfg          Config
	storeFactory storeFactory
	wg           sync.WaitGroup

	mu      sync.Mutex
	running bool
	stopped bool
}

// NewMonitor creates a residual Monitor.
func NewMonitor(ctx context.Context, cfg Config) *Monitor {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.TaskCount == nil {
		cfg.TaskCount = func(context.Context) (int, error) { return 0, nil }
	}
	if cfg.StorageURI == nil {
		cfg.StorageURI = func(context.Context) string { return "" }
	}
	return &Monitor{
		ctx:          ctx,
		cfg:          cfg,
		storeFactory: newStore,
	}
}

func newStore(ctx context.Context, uri string) (storeapi.Storage, error) {
	backend, err := objstore.ParseBackend(uri, nil)
	if err != nil {
		return nil, err
	}
	return objstore.NewWithDefaultOpt(ctx, backend)
}

// Request starts at most one residual scan.
func (m *Monitor) Request() {
	m.mu.Lock()
	if !m.cfg.Enabled || m.stopped || m.ctx.Err() != nil || m.running {
		m.mu.Unlock()
		return
	}
	m.running = true
	m.wg.Add(1)
	m.mu.Unlock()
	failpoint.InjectCall("beforeGlobalSortResidualMonitorRun")
	go func() {
		defer func() {
			m.mu.Lock()
			m.running = false
			m.mu.Unlock()
			m.wg.Done()
		}()
		failpoint.InjectCall("globalSortResidualMonitorWorker")
		m.run()
	}()
}

// Stop prevents new scans, waits for an admitted scan, and resets the residual gauge.
func (m *Monitor) Stop() {
	m.mu.Lock()
	m.stopped = true
	m.mu.Unlock()
	m.wg.Wait()
	metrics.GlobalSortResidualDataSize.Set(0)
}

func (m *Monitor) run() {
	if m.ctx.Err() != nil {
		return
	}

	taskCount, err := m.cfg.TaskCount(m.ctx)
	if err != nil {
		if !isCancellation(err) {
			m.cfg.Logger.Warn("global sort residual monitor failed to get all tasks", zap.Error(err))
		}
		return
	}
	if taskCount > 0 {
		metrics.GlobalSortResidualDataSize.Set(0)
		return
	}
	if m.ctx.Err() != nil {
		return
	}

	storageURI := m.cfg.StorageURI(m.ctx)
	logStorageURI := storageLogURI(storageURI)
	var scan Stats
	if storageURI != "" {
		storage, err := m.storeFactory(m.ctx, storageURI)
		if err != nil {
			if !isCancellation(err) {
				m.cfg.Logger.Warn("global sort residual monitor failed to create storage",
					zap.String("storage-uri", logStorageURI))
			}
			return
		}
		defer storage.Close()

		scan, err = Scan(m.ctx, storage)
		if err != nil {
			if !isCancellation(err) {
				m.cfg.Logger.Warn("global sort residual monitor failed to scan storage",
					zap.String("storage-uri", logStorageURI))
			}
			return
		}
	}
	if m.ctx.Err() != nil {
		return
	}

	taskCount, err = m.cfg.TaskCount(m.ctx)
	if err != nil {
		if !isCancellation(err) {
			m.cfg.Logger.Warn("global sort residual monitor failed to get all tasks", zap.Error(err))
		}
		return
	}
	if taskCount > 0 {
		metrics.GlobalSortResidualDataSize.Set(0)
		m.cfg.Logger.Info("global sort residual monitor discarded scan because tasks appeared",
			zap.String("storage-uri", logStorageURI),
			zap.Int("task-count", taskCount))
		return
	}
	if m.ctx.Err() != nil {
		return
	}

	metrics.GlobalSortResidualDataSize.Set(float64(scan.SizeBytes))
	m.cfg.Logger.Info("global sort residual monitor success",
		zap.String("storage-uri", logStorageURI),
		zap.Int64("residual-size-bytes", scan.SizeBytes),
		zap.Int64("residual-object-count", scan.ObjectCount),
		zap.Strings("sample-prefixes", scan.SamplePrefixes),
		zap.Bool("sample-prefixes-omitted", scan.SamplePrefixesOmitted))
}

func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func storageLogURI(storageURI string) string {
	if _, err := url.Parse(storageURI); err != nil {
		return "<invalid>"
	}
	return ast.RedactURL(storageURI)
}
